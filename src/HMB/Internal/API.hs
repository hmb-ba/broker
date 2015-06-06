-- |
-- Module      : HMB.Internal.API
-- Copyright   : (c) Marc Juchli, Lorenz Wolf 2015
-- License     : BSD-style
--
-- Maintainer  :
-- Stability   : WIP
-- Portability : GHC
--
-- This module handles incoming requests. First of all it parses the
-- received bytes as RequestMessage and proceeds an appropriate action
-- depending on the delivered API key. After performing the action, an
-- according ResponseMessage is generated and provided to the Network Layer
-- for sending back to the client.
--
-- -- > import ...
--
module HMB.Internal.API
( handleRequest
, runApiHandler
) where

import HMB.Internal.Types
import qualified HMB.Internal.Log as Log
import Kafka.Protocol

import Network.Socket
import qualified Network.Socket.ByteString.Lazy as S
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Lazy.Char8 as C

import Control.Concurrent
import Control.Concurrent.Chan
import Control.Monad

import Data.Binary.Get
import Data.Binary.Put

import Control.Exception
import Prelude hiding (catch)
import System.IO.Error


-------------------------------
--API Handler Thread
------------------------------
-- | Api handler
--
runApiHandler :: RequestChan -> ResponseChan -> IO()
runApiHandler rqChan rsChan = do
  s <- Log.new
  handlerLoop s rqChan rsChan

handlerLoop :: Log.LogState -> RequestChan -> ResponseChan -> IO ()
handlerLoop s rqChan rsChan = do
  (conn, req) <- readChan rqChan
  case readRequest req of
    Left (bs, bo, e) -> handleHandlerError conn $ ParseRequestError e
    Right (bs, bo, rm) -> do
      res <- handleRequest rm s
      case res  of
        Left e -> handleHandlerError conn e
        Right bs -> writeToResChan conn rsChan (bs)
  handlerLoop s rqChan rsChan

readRequest :: BL.ByteString -> Either (BL.ByteString, ByteOffset, String) (BL.ByteString, ByteOffset, RequestMessage)
readRequest a = runGetOrFail requestMessageParser a

writeToResChan :: (Socket, SockAddr) -> ResponseChan -> BL.ByteString -> IO()
writeToResChan conn chan res = writeChan chan (conn, res)

handleHandlerError :: (Socket, SockAddr) -> HandleError -> IO()
handleHandlerError (s, a) (ParseRequestError e) = do
    putStrLn $ (show "[ParseError]: ") ++ e
    --sClose s
    putStrLn $ "***Host " ++ (show a) ++ " disconnected ***"
handleHandlerError (s, a) (PrWriteLogError o e) = do
    putStrLn $ show "[WriteLogError on offset " ++ show o ++ "]: " ++ show e
handleHandlerError (s, a) UnknownRqError = do
    putStrLn $ show "[UnknownRqError]"
handleHandlerError (s, a) e = do
  putStrLn $ show e
  S.sendAll s $ C.pack $ show e -- TODO: Send BS to Response Channel
  sClose s
  putStrLn $ "***Host " ++ (show a) ++ "disconnected ***"


handleRequest :: RequestMessage -> Log.LogState -> IO (Either HandleError BL.ByteString)
handleRequest rm s = do
   handle <- case rqApiKey rm of
    0  -> handleProduceRequest (rqRequest rm) s
    1  -> handleFetchRequest (rqRequest rm)
    --2  -> Right $ putStrLn "OffsetRequest"
    3  -> handleMetadataRequest (rqRequest rm)
    --8  -> Right $ putStrLn "OffsetCommitRequest"
    --9  -> Right $ putStrLn "OffsetFetchRequest"
    --10 -> Right $ putStrLn "ConsumerMetadataRequest"
    _  -> return $ Left UnknownRqError
   return handle

-----------------
-- Handle ProduceRequest
-----------------
handleProduceRequest :: Request -> Log.LogState -> IO (Either HandleError BL.ByteString)
handleProduceRequest req s = do
  --mapM Log.appendLog (Log.logData req)

  w <- tryIOError( mapM Log.append [
                    (s, BC.unpack(rqTopicName x), fromIntegral(rqPrPartitionNumber y), rqPrMessageSet y )
                    | x <- rqPrTopics req, y <- partitions x
                          ]
          )
  case w of
      Left e -> return $ Left $ PrWriteLogError 0 "todo"
      Right r -> return $ Right $ buildPrResponseMessage packProduceResponse

  return $ Right C.empty

packProduceResponse :: ResponseMessage
packProduceResponse =
  let error = RsPrPayload 0 0 0
  in
  let response = ProduceResponse
        (RsTopic
          (fromIntegral $ BL.length $ C.pack "topicHardCoded")
          (BC.pack "topicHardCoded")
          (fromIntegral $ length [error])
          [error]
        )
        in
  let responseMessage = ResponseMessage 0 1 [response]
  in
  responseMessage

-----------------
-- Handle FetchRequest
-----------------

packPartitionsToFtRsPayload :: TopicName -> Partition -> IO RsPayload
packPartitionsToFtRsPayload t p = do
    log <- Log.readLog (BC.unpack $ t, fromIntegral $ rqFtPartitionNumber p) $ fromIntegral $ rqFtFetchOffset p
    return $ RsFtPayload
        0
        0
        0
        (fromIntegral $ BL.length $ foldl (\acc ms -> BL.append acc (runPut $ buildMessageSet ms)) BL.empty log)
        log

packLogToFtRs :: RqTopic -> IO Response
packLogToFtRs t = do
    rss <- (mapM (packPartitionsToFtRsPayload $ rqTopicName $ t) $ partitions t)
    return $ FetchResponse
        (rqTopicNameLen t)
        (rqTopicName t )
        (numPartitions t )
        rss

handleFetchRequest :: Request -> IO (Either HandleError BL.ByteString)
handleFetchRequest req = do
  w <- tryIOError(liftM (ResponseMessage 0 1) $ mapM packLogToFtRs (rqFtTopics req))
  case w of
    Left e -> return $ Left $ FtReadLogError 0 $ show e
    Right rsms -> return $ Right $ runPut $ buildFtRsMessage rsms


-----------------
-- Handle MetadataRequest
-----------------
packMdRsPayloadTopic :: String -> IO RsPayload
packMdRsPayloadTopic t = return $ RsMdPayloadTopic 0 (fromIntegral $ length t) (BC.pack t) 0 [] --TODO: Partition Metadata

packMdRs :: IO Response
packMdRs = do
  ts <- Log.getTopics
  tss <- mapM packMdRsPayloadTopic ts
  return $ MetadataResponse
            1
            ([RsMdPayloadBroker 0 (fromIntegral $ BL.length $ C.pack "localhost") (BC.pack "localhost") 4343]) --single broker solution
            (fromIntegral $ length tss)
            tss

handleMetadataRequest :: Request -> IO (Either HandleError BL.ByteString)
handleMetadataRequest req = do
  res <- packMdRs
  return $ Right $ runPut $ buildMdRsMessage $ ResponseMessage 0 1 [res]

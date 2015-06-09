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


import Control.Exception
import Control.Concurrent
import Control.Concurrent.Chan
import Control.Monad

import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Lazy.Char8 as C
import Data.Binary.Get
import Data.Binary.Put

import HMB.Internal.Types
import qualified HMB.Internal.LogManager as LogManager
import qualified HMB.Internal.Log as Log
import qualified HMB.Internal.Index as Index

import Kafka.Protocol

import Network.Socket
import Network.Info
import qualified Network.Socket.ByteString.Lazy as S

import Prelude hiding (catch)

import System.IO.Error


-------------------------------
--API Handler Thread
------------------------------
-- | Api handler
--
runApiHandler :: RequestChan -> ResponseChan -> IO()
runApiHandler rqChan rsChan = do
  s <- LogManager.new
  handlerLoop s rqChan rsChan

handlerLoop :: LogManager.State -> RequestChan -> ResponseChan -> IO ()
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


handleRequest :: RequestMessage -> LogManager.State -> IO (Either HandleError BL.ByteString)
handleRequest rm s = do
   handle <- case rqApiKey rm of
    0  -> handleProduceRequest (rm) s
    1  -> handleFetchRequest (rqRequest rm)
    --2  -> Right $ putStrLn "OffsetRequest"
    3  -> handleMetadataRequest (rm)
    --8  -> Right $ putStrLn "OffsetCommitRequest"
    --9  -> Right $ putStrLn "OffsetFetchRequest"
    --10 -> Right $ putStrLn "ConsumerMetadataRequest"
    _  -> return $ Left UnknownRqError
   return handle

-----------------
-- Handle ProduceRequest
-----------------
handleProduceRequest :: RequestMessage -> LogManager.State -> IO (Either HandleError BL.ByteString)
handleProduceRequest rm s = do
  --mapM Log.appendLog (Log.logData req)
  let req = rqRequest rm

  w <- tryIOError( mapM LogManager.append [
                    (s, BC.unpack(rqToName x), fromIntegral(rqPrPartitionNumber y), rqPrMessageSet y )
                    | x <- rqPrTopics req, y <- rqToPartitions x
                          ]
          )
  case w of
      Left e -> return $ Left $ PrWriteLogError 0 "todo"
      Right r -> return $ Right $ runPut $ buildPrResponseMessage $ packProduceResponse $ rm

packProduceResponse :: RequestMessage -> ResponseMessage
packProduceResponse rm = ResponseMessage resLen (rqCorrelationId rm) response
  where
    resLen = (fromIntegral (BL.length $ runPut $ buildProduceResponse $ response) + 4 )
    response = ProduceResponse (fromIntegral $ length topics) topics
    topics = (map topic (rqPrTopics $ rqRequest $ rm))
    topic t = RsTopic (fromIntegral $ BC.length $ rqToName t)
                      (rqToName t)
                      (fromIntegral $ length [error])
                      [error]
    error = RsPrPayload 0 0 0

-----------------
-- Handle FetchRequest
-----------------

packPartitionsToFtRsPayload :: TopicName -> Partition -> IO RsPayload
packPartitionsToFtRsPayload t p = do
    log <- LogManager.readLog (BC.unpack $ t, fromIntegral $ rqFtPartitionNumber p) $ fromIntegral $ rqFtFetchOffset p
    return $ RsFtPayload
        0
        0
        0
        (fromIntegral $ BL.length $ foldl (\acc ms -> BL.append acc (runPut $ buildMessageSet ms)) BL.empty log)
        log

packLogToFtRs :: RqTopic -> IO Response
packLogToFtRs t = do
    rss <- (mapM (packPartitionsToFtRsPayload $ rqToName $ t) $ rqToPartitions t)
    return $ FetchResponse 1 [(RsTopic
        (rqToNameLen t)
        (rqToName t )
        (rqToNumPartitions t )
        rss)]

handleFetchRequest :: Request -> IO (Either HandleError BL.ByteString)
handleFetchRequest req = do
  w <- tryIOError(liftM (ResponseMessage 0 0) $ packLogToFtRs (head $ rqFtTopics req))
  case w of
    Left e -> return $ Left $ FtReadLogError 0 $ show e
    Right rsms -> return $ Right $ runPut $ buildFtRsMessage rsms


-----------------
-- Handle MetadataRequest
-----------------

packMdRsPartitionMetadata :: RsMdPartitionMetadata
packMdRsPartitionMetadata = RsMdPartitionMetadata 0 0 0 1 [0] 1 [0]

packMdRsPayloadTopic :: String -> IO RsPayload
packMdRsPayloadTopic t = return $ RsMdPayloadTopic 0 (fromIntegral $ length t) (BC.pack t) 1 [packMdRsPartitionMetadata]

packMdRs :: IO Response
packMdRs = do
  ts <- Log.getTopicNames
  tss <- mapM packMdRsPayloadTopic ts
  (NetworkInterface name ipv4 ipv6 mac) <- getHostIp
  return $ MetadataResponse
            1
            ([RsMdPayloadBroker 0 (fromIntegral $ BL.length $ C.pack $ show ipv4) (BC.pack $ show ipv4) 4343]) --single broker solution
            (fromIntegral $ length tss)
            tss

getHostIp :: IO NetworkInterface
getHostIp = do
  ns <- getNetworkInterfaces
  return $ head $ filter (\(NetworkInterface name ipv4 ipv6 mac) -> name == "eth0") ns


handleMetadataRequest :: RequestMessage -> IO (Either HandleError BL.ByteString)
handleMetadataRequest rm = do
  res <- packMdRs
  return $ Right $ runPut $ buildMdRsMessage $ ResponseMessage (fromIntegral(BL.length $ runPut $ buildMdRs res) + 4) (rqCorrelationId rm) res



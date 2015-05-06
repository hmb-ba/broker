module HMB.Internal.Handler 
( handleRequest
, initHandler
, listenLoop
) where 

import Network.Socket 
import qualified Network.Socket.ByteString.Lazy as SBL
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Char8 as BC

import Data.Int
import System.IO 
import System.Environment
import Control.Concurrent
import Control.Monad

import Kafka.Protocol
import Data.Binary.Get

import HMB.Internal.Log
import qualified Data.ByteString.Lazy.Char8 as C

import Control.Exception
import Prelude hiding (catch)

import Control.Monad.Error
import System.IO.Error

import Control.Applicative
---------------
-- error types
---------------

data ErrorCode =
        NoError
      | Unknown
      | OffsetOutOfRange
      | InvalidMessage
      | UnknownTopicOrPartition
      | InvalidMessageSize
      | LeaderNotAvailable
      | NotLeaderForPartition
      | RequestTimedOut
      | BrokerNotAvailable
      | ReplicaNotAvailable
      | MessageSizeTooLarge
      | StaleControllerEpochCode
      | OffsetMetadataTooLargeCode
      | OffsetsLoadInProgressCode
      | ConsumerCoordinatorNotAvailableCode
      | NotCoordinatorForConsumerCodeA
        deriving (Show, Enum)

data SocketError =
        SocketRecvError String
      | SocketSendError String
        deriving Show

data HandleError =
        ParseRequestError String
      | PrWriteLogError Int String
      | PrPackError String
      | FtReadLogError Int String
        deriving Show


---------------

readRequest :: BL.ByteString -> Either (BL.ByteString, ByteOffset, String) (BL.ByteString, ByteOffset, RequestMessage)
readRequest a = runGetOrFail requestMessageParser a

initHandler :: IO Socket
initHandler = do
  sock <- socket AF_INET Stream 0
  setSocketOption sock ReuseAddr 1
  bindSocket sock (SockAddrInet 4343 iNADDR_ANY)
  listen sock 2
  return sock

listenLoop :: Socket -> IO()
listenLoop sock =  do
  conn <- accept sock
  forkIO $ forever $ do
    r <- recvFromSock conn
    case (r) of
      Left e -> handleSocketError conn e
      Right i -> do 
        print i
        handle <- handleRequest conn i
        case handle of
          Left e -> handleHandlerError conn e
          Right bs -> do 
            r <- try(sendResponse conn bs) :: IO (Either SomeException ())
            case r of 
              Left e -> handleSocketError conn $ SocketSendError $ show e
              Right r -> return r
  listenLoop sock

handleSocketError :: (Socket, SockAddr) -> SocketError -> IO()
handleSocketError (sock, sockaddr) e = do
  putStrLn $ show e
  sClose sock

handleHandlerError :: (Socket, SockAddr) -> HandleError -> IO()
handleHandlerError (s, a) (ParseRequestError e) = do
    putStrLn $ (show "[ParseError]: ") ++ e
    sClose s
handleHandlerError (s, a) (PrWriteLogError o e) = do
    putStrLn $ show "[WriteLogError on offset " ++ show o ++ "]: " ++ show e
handleHandlerError (s, a) e = do
  putStrLn $ show e
  SBL.sendAll s $ C.pack $ show e
  sClose s

recvFromSock :: (Socket, SockAddr) -> IO (Either SocketError BL.ByteString)
recvFromSock (sock, sockaddr) =  do 
  respLen <- try (SBL.recv sock (4 :: Int64)) :: IO (Either SomeException BL.ByteString)
  case respLen of
    Left e -> return $ Left $ SocketRecvError $ show e 
    Right rl -> do
      response <- try (SBL.recv sock $ getLength $ rl) :: IO (Either SomeException BL.ByteString)
      case response of
        Left e -> return $ Left $ SocketRecvError $ show e
        Right bs -> return $ Right bs
  where 
    getLength = runGet $ fromIntegral <$> getWord32be

sendResponse :: (Socket, SockAddr) -> BL.ByteString -> IO()
sendResponse (socket, addr) responsemessage = SBL.sendAll socket $ responsemessage

handleRequest :: (Socket, SockAddr) -> BL.ByteString -> IO (Either HandleError BL.ByteString)
handleRequest (sock, sockaddr) input = do
  case readRequest input of
    Left (bs, bo, e) -> return $ Left $ ParseRequestError e
    Right (bs, bo, rm) -> do
        handle <- case rqApiKey rm of
          0  -> handleProduceRequest (rqRequest rm)
          1  -> handleFetchRequest (rqRequest rm) sock
          --2  -> Right $ putStrLn "OffsetRequest"
          3  -> handleMetadataRequest (rqRequest rm) sock 
          --8  -> Right $ putStrLn "OffsetCommitRequest"
          --9  -> Right $ putStrLn "OffsetFetchRequest"
          --10 -> Right $ putStrLn "ConsumerMetadataRequest"
          _  -> return $ Right BL.empty
        return handle

-----------------
-- ProduceRequest
-----------------

handleProduceRequest :: Request ->  IO (Either HandleError BL.ByteString)
handleProduceRequest req = do
  w <- tryIOError( mapM writeLog [ 
                    (BC.unpack(rqTopicName x), fromIntegral(rqPrPartitionNumber y), rqPrMessageSet y ) 
                    | x <- rqPrTopics req, y <- partitions x 
                          ]
          )
  case w of
      Left e -> return $ Left $ PrWriteLogError 0 "todo"
      Right r -> return $ Right $ buildPrResponseMessage packProduceResponse


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
-- FetchRequest
-----------------

packPartitionsToFtRsPayload :: TopicName -> Partition -> IO RsPayload
packPartitionsToFtRsPayload t p = do
    log <- readLog (BC.unpack $ t, fromIntegral $ rqFtPartitionNumber p, fromIntegral $ rqFtFetchOffset p)
    return $ RsFtPayload
        0
        0
        0
        (fromIntegral $ BL.length $ foldl (\acc ms -> BL.append acc (buildMessageSet ms)) BL.empty log)
        log

packLogToFtRs :: RqTopic -> IO Response
packLogToFtRs t = do
    rss <- (mapM (packPartitionsToFtRsPayload $ rqTopicName $ t) $ partitions t)
    return $ FetchResponse
        (rqTopicNameLen t)
        (rqTopicName t )
        (numPartitions t )
        rss

handleFetchRequest :: Request -> Socket -> IO (Either HandleError BL.ByteString)
handleFetchRequest req sock = do
  w <- tryIOError(liftM (ResponseMessage 0 1) $ mapM packLogToFtRs (rqFtTopics req))
  case w of 
    Left e -> return $ Left $ FtReadLogError 0 "todo"
    Right rsms -> return $ Right $ buildFtRsMessage rsms


-----------------
-- MetadataRequest
-----------------
packMdRsPayloadTopic :: String -> IO RsPayload
packMdRsPayloadTopic t = return $ RsMdPayloadTopic 0 (fromIntegral $ length t) (BC.pack t) 0 [] --TODO: Partition Metadata 

packMdRs :: IO Response
packMdRs = do 
  ts <- getTopicNames
  tss <- mapM packMdRsPayloadTopic ts 
  return $ MetadataResponse 
            1
            ([RsMdPayloadBroker 0 (fromIntegral $ BL.length $ C.pack "localhost") (BC.pack "localhost") 4343]) --single broker solution
            (fromIntegral $ length tss) 
            tss 

handleMetadataRequest :: Request -> Socket -> IO (Either HandleError BL.ByteString)
handleMetadataRequest req sock = do 
  res <- packMdRs 
  return $ Right $ buildMdRsMessage $ ResponseMessage 0 1 [res]



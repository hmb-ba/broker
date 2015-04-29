module HMB.Internal.Handler 
( handleRequest
, initHandler
, listenLoop
) where 

import Network.Socket 
import qualified Network.Socket.ByteString.Lazy as SBL
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Char8 as BS

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
import System.IO.Unsafe

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
        case handleRequest conn i of
          Left e -> handleHandleError conn e
          Right bs -> do 
                      res <- bs
                      sendResponse conn res
  listenLoop sock

handleSocketError :: (Socket, SockAddr) -> SocketError -> IO()
handleSocketError (sock, sockaddr) e = do
  putStrLn $ show e
  SBL.sendAll sock $ C.pack $ show e
  sClose sock

handleHandleError :: (Socket, SockAddr) -> HandleError -> IO()
handleHandleError (sock, sockaddr) e = do
  putStrLn $ show e
  SBL.sendAll sock $ C.pack $ show e
  sClose sock
  -- central point to create response for each type of handle error
  -- e case of 
  -- | SomeError -> ... 

recvFromSock :: (Socket, SockAddr) -> IO (Either SocketError BL.ByteString)
recvFromSock (sock, sockaddr) =
  do r <- try (SBL.recv sock 4096) :: IO (Either SomeException BL.ByteString)
     case r of
       Left e -> return $ Left $ SocketRecvError $ show e
       Right bs -> return $ Right bs

sendResponse :: (Socket, SockAddr) -> BL.ByteString -> IO()
sendResponse (socket, addr) responsemessage = SBL.sendAll socket $ responsemessage

handleRequest :: (Socket, SockAddr) -> BL.ByteString -> Either HandleError (IO BL.ByteString)
handleRequest (sock, sockaddr) input = do
  case readRequest input of
    Left (bs, bo, e) -> Left $ ParseRequestError e
    Right (bs, bo, rm) -> do
        x <- case rqApiKey rm of
          0  -> handleProduceRequest (rqRequest rm)
          1  -> handleFetchRequest (rqRequest rm) sock
          --2  -> Right $ putStrLn "OffsetRequest"
          --3  -> Right $ putStrLn "MetadataRequest"
          --8  -> Right $ putStrLn "OffsetCommitRequest"
          --9  -> Right $ putStrLn "OffsetFetchRequest"
          --10 -> Right $ putStrLn "ConsumerMetadataRequest"
          --_  -> Right $ putStrLn "Unknown ApiKey"
        return x

-----------------
-- ProduceRequest
-----------------

handleProduceRequest :: Request ->  Either HandleError (IO BL.ByteString)
handleProduceRequest req = do
  let w = unsafePerformIO $ tryIOError( mapM writeLog [ 
                    (BS.unpack(topicName x), fromIntegral(rqPrPartitionNumber y), rqPrMessageSet y ) 
                    | x <- rqPrTopics req, y <- partitions x 
                          ]
          )
  case w of
      Left e -> Left $ PrWriteLogError 0 "hoi"
      Right r -> return $ return $ buildPrResponseMessage packProduceResponse


packProduceResponse :: ResponseMessage 
packProduceResponse = 
  let error = RsPrError 0 0 0 
  in
  let response = ProduceResponse 
        (fromIntegral $ BS.length $ BS.pack "topicHardCoded")
        (BS.pack "topicHardCoded")
        (fromIntegral $ length [error])
        [error]
  in
  let responseMessage = ResponseMessage 0 1 [response]
  in
  responseMessage 

-----------------
-- FetchRequest
-----------------

packPartitionsToFtRsPayload :: TopicName -> Partition -> IO RsFtPayload
packPartitionsToFtRsPayload t p = do
    log <- readLog (BS.unpack $ t, fromIntegral $ rqFtPartitionNumber p, fromIntegral $ rqFtFetchOffset p)
    return $ RsFtPayload
        0
        0
        0
        (fromIntegral $ BL.length $ foldl (\acc ms -> BL.append acc (buildMessageSet ms)) BL.empty log)
        log

packLogToFtRs :: Topic -> IO Response
packLogToFtRs t = do
    rss <- (mapM (packPartitionsToFtRsPayload $ topicName t) $ partitions t)
    return $ FetchResponse
        (topicNameLen t)
        (topicName t)
        (numPartitions t )
        rss

handleFetchRequest :: Request -> Socket -> Either HandleError (IO BL.ByteString)
handleFetchRequest req sock = do
  let rsms = liftM (ResponseMessage 0 1) $ mapM packLogToFtRs (rqFtTopics req)
  let msg = liftM buildFtRsMessage rsms
  Right msg


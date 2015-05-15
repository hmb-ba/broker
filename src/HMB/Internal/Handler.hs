module HMB.Internal.Handler 
( handleRequest
, initSocket
, initReqChan
, initResChan
, runAcceptor
, runApiHandler
, runResponder
) where 

import Network.Socket 
import qualified Network.Socket.ByteString.Lazy as SBL
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Char8 as BC

import Data.Int
import System.IO 
import System.Environment
import Control.Concurrent
import Control.Concurrent.Chan
import Control.Monad
import Kafka.Protocol
import Data.Binary.Get
import qualified Data.ByteString.Lazy.Char8 as C
import Control.Exception
import Prelude hiding (catch)
import Control.Monad.Error
import System.IO.Error
import Control.Applicative

import HMB.Internal.Log

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
        PrWriteLogError Int String
      | PrPackError String
      | ParseRequestError String 
      | FtReadLogError Int String
      | SendResponseError String
      | UnknownRqError
        deriving Show

---------------------------
--Initialize Socket 
---------------------------
initSocket :: IO Socket
initSocket = do
  sock <- socket AF_INET Stream 0
  setSocketOption sock ReuseAddr 1
  bindSocket sock (SockAddrInet 4343 iNADDR_ANY)
  listen sock 2
  return sock

--------------------------
-- Initialize Request Channel
---------------------------
type ChanMessage = ((Socket, SockAddr), BL.ByteString)
type RequestChan = Chan ChanMessage

initReqChan :: IO RequestChan
initReqChan = newChan

--------------------------
-- Initialize Response Channel
---------------------------
type ResponseChan = Chan ChanMessage

initResChan :: IO ResponseChan
initResChan = newChan

-----------------------------
-- Acceptor Thread
----------------------------
runAcceptor :: Socket -> RequestChan -> IO()
runAcceptor sock chan =  do
  (conSock, sockAddr) <- accept sock
  putStrLn $ "***Host " ++ (show sockAddr) ++ " connected***"
  forkIO $ runConnection (conSock, sockAddr) chan True
  runAcceptor sock chan

-----------------------------
--Connection Processor Thread
----------------------------
runConnection :: (Socket, SockAddr) -> RequestChan -> Bool -> IO()
runConnection conn chan True = do 
  r <- recvFromSock conn
  case (r) of
    Left e -> do 
      handleSocketError conn  e
      runConnection conn chan False
    Right input -> do 
      writeToReqChan conn chan input
      --putStrLn "***Request Received***"

      runConnection conn chan True
runConnection conn chan False = return () --TODO: Better solution for breaking out the loop? 

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

writeToReqChan :: (Socket, SockAddr) -> RequestChan -> BL.ByteString -> IO()
writeToReqChan conn chan req = writeChan chan (conn, req)

handleSocketError :: (Socket, SockAddr) -> SocketError -> IO()
handleSocketError (sock, sockaddr) e = do
  return ()
  --sClose sock

-----------------------
-- Response Processor Thread
-----------------------
runResponder :: ResponseChan -> IO() 
runResponder chan = do 
  (conn, res) <- readChan chan 
  res <- sendResponse conn res 
  case res of 
    Left e -> handleSocketError conn $ SocketSendError $ show e ---TODO: What to do with responses when client disconnected?
    Right io -> return io
  runResponder chan 

sendResponse :: (Socket, SockAddr) -> BL.ByteString -> IO(Either SomeException ())
sendResponse (socket, addr) responsemessage = try(SBL.sendAll socket $ responsemessage) :: IO (Either SomeException ())

-------------------------------
--API Handler Thread
------------------------------
runApiHandler :: RequestChan -> ResponseChan -> IO()
runApiHandler rqChan rsChan = do
  (conn, req) <- readChan rqChan
  case readRequest req of
    Left (bs, bo, e) -> handleHandlerError conn $ ParseRequestError e
    Right (bs, bo, rm) -> do
      res <- handleRequest rm
      case res  of
        Left e -> handleHandlerError conn e
        Right bs -> writeToResChan conn rsChan bs
  runApiHandler rqChan rsChan

readRequest :: BL.ByteString -> Either (BL.ByteString, ByteOffset, String) (BL.ByteString, ByteOffset, RequestMessage)
readRequest a = runGetOrFail requestMessageParser a

writeToResChan :: (Socket, SockAddr) -> ResponseChan -> BL.ByteString -> IO()
writeToResChan conn chan res = writeChan chan (conn, res)

handleHandlerError :: (Socket, SockAddr) -> HandleError -> IO()
handleHandlerError (s, a) (ParseRequestError e) = do
    putStrLn $ (show "[ParseError]: ") ++ e
    sClose s
    putStrLn $ "***Host " ++ (show a) ++ " disconnected ***"
handleHandlerError (s, a) (PrWriteLogError o e) = do
    putStrLn $ show "[WriteLogError on offset " ++ show o ++ "]: " ++ show e
handleHandlerError (s, a) UnknownRqError = do
    putStrLn $ show "[UnknownRqError]"
handleHandlerError (s, a) e = do
  putStrLn $ show e
  SBL.sendAll s $ C.pack $ show e
  sClose s
  putStrLn $ "***Host " ++ (show a) ++ "disconnected ***"


handleRequest :: RequestMessage -> IO (Either HandleError BL.ByteString)
handleRequest rm = do
   handle <- case rqApiKey rm of
    0  -> handleProduceRequest (rqRequest rm)
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
handleProduceRequest :: Request ->  IO (Either HandleError BL.ByteString)
handleProduceRequest req = do
  bo <- getLastBaseOffset (BC.pack("topicX"), fromIntegral 0)
  print bo
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
-- Handle FetchRequest
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

handleFetchRequest :: Request -> IO (Either HandleError BL.ByteString)
handleFetchRequest req = do
  w <- tryIOError(liftM (ResponseMessage 0 1) $ mapM packLogToFtRs (rqFtTopics req))
  case w of 
    Left e -> return $ Left $ FtReadLogError 0 "todo"
    Right rsms -> return $ Right $ buildFtRsMessage rsms


-----------------
-- Handle MetadataRequest
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

handleMetadataRequest :: Request -> IO (Either HandleError BL.ByteString)
handleMetadataRequest req = do 
  res <- packMdRs 
  return $ Right $ buildMdRsMessage $ ResponseMessage 0 1 [res]

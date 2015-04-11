module HMB.Internal.Handler (
    initHandler, 
    listenLoop
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

import HMB.Internal.Log

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
  readReqFromSock conn
  listenLoop sock

readReqFromSock :: (Socket, SockAddr) -> IO()
readReqFromSock (sock, sockaddr) = do
  input <- SBL.recv sock 4096
  let i  = input
  requestMessage <- readRequest i
  case rqApiKey requestMessage of
    0  -> handleProduceRequest (rqRequest requestMessage) sock
    1  -> handleFetchRequest (rqRequest requestMessage) sock
    2  -> putStrLn "OffsetRequest"
    3  -> putStrLn "MetadataRequest"
    8  -> putStrLn "OffsetCommitRequest"
    9  -> putStrLn "OffsetFetchRequest"
    10 -> putStrLn "ConsumerMetadataRequest"
    _  -> putStrLn "Unknown ApiKey"
  print requestMessage
  return ()


-----------------
-- ProduceRequest
-----------------

handleProduceRequest :: Request -> Socket -> IO()
handleProduceRequest req sock = do
  mapM writeLog [ 
      (BS.unpack(topicName x), fromIntegral(rqPrPartitionNumber y), rqPrMessageSet y ) 
      | x <- rqPrTopics req, y <- partitions x 
    ]
  sendProduceResponse sock packProduceResponse 

-- TODO dynamic function
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

sendProduceResponse :: Socket -> ResponseMessage -> IO()
sendProduceResponse socket responsemessage = do
  let msg = buildPrResponseMessage responsemessage
  SBL.sendAll socket msg

-----------------
-- FetchRequest
-----------------

requestLog :: Request -> IO [Log]
requestLog req = mapM readLog [
      (BS.unpack(topicName x), fromIntegral(rqFtPartitionNumber y))
      | x <- rqFtTopics req, y <- partitions x 
    ]

packMsToFtRsPayload :: MessageSet -> RsFtPayload
packMsToFtRsPayload ms = RsFtPayload 0 0 0 (fromIntegral $ len ms) ms

packLogToFtRsPayload :: Log -> [RsFtPayload]
packLogToFtRsPayload log = map packMsToFtRsPayload log

packLogToFtRs :: Log -> Response
packLogToFtRs log = FetchResponse
        0
        (BS.pack("testtopic"))
        0
        (packLogToFtRsPayload log)

handleFetchRequest :: Request -> Socket -> IO()
handleFetchRequest req sock = do
  logs <- requestLog req
  let rs = map packLogToFtRs logs
  let msg = buildFtRsMessage (ResponseMessage 0 0 rs)
  SBL.sendAll sock msg
  print "send resp"


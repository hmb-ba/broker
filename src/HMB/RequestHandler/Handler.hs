module HMB.RequestHandler.Handler (
    initHandler, 
    listenLoop
) where 

import Network.Socket 
import qualified Network.Socket.ByteString.Lazy as SockBL
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Char8 as BS

import System.IO 
import System.Environment
import Control.Concurrent
import Control.Monad

import HMB.Common
import HMB.Network
import HMB.Log.Writer

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
  input <- SockBL.recv sock 4096
  let i  = input
  requestMessage <- readRequest i
  case reqApiKey requestMessage of
    0  -> handleProduceRequest (request requestMessage) sock
    1  -> putStrLn "FetchRequest"
    2  -> putStrLn "OffsetRequest"
    3  -> putStrLn "MetadataRequest"
    8  -> putStrLn "OffsetCommitRequest"
    9  -> putStrLn "OffsetFetchRequest"
    10 -> putStrLn "ConsumerMetadataRequest"
    _  -> putStrLn "Unknown ApiKey"
  print requestMessage
  return ()

handleProduceRequest :: Request -> Socket -> IO()
handleProduceRequest req sock = do
  mapM writeLog [ (BS.unpack(topicName x), fromIntegral(partitionNumber y), messageSet y ) | x <- reqTopics req, y <- partitions x ]
  
  sendProduceResponse sock packProduceResponse 
  --return()

packProduceResponse :: ResponseMessage 
packProduceResponse = 
  let error = Error {
      errPartitionNumber = 0 
    , errCode = 0
    , errOffset = 0
  }
  in
  let response = ProduceResponse {
      resTopicName = BS.pack "topicHardCoded"
    , resTopicNameLen = fromIntegral $ BS.length $ BS.pack "topicHardCoded"
    , resNumErrors = 1
    , resErrors = [error]
  }
  in
  let responseMessage = ResponseMessage {
      resCorrelationId = 0 
    , resNumResponses = 1 
    , responses = [response]
  }
  in 
  responseMessage 

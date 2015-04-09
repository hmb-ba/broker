module HMB.Internal.Handler (
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
  input <- SockBL.recv sock 4096
  let i  = input
  requestMessage <- readRequest i
  case rqApiKey requestMessage of
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
  mapM writeLog [ (BS.unpack(rqPrTopicName x), fromIntegral(rqPrPartitionNumber y), rqPrMessageSet y ) | x <- rqPrTopics req, y <- rqPrPartitions x ]
  sendProduceResponse sock packProduceResponse 

-- TODO dynamic function
packProduceResponse :: ResponseMessage 
packProduceResponse = 
  let error = RsPrError 0 0 0 
  in
  let response = ProduceResponse 
        BS.pack "topicHardCoded"
        fromIntegral $ BS.length $ BS.pack "topicHardCoded"
        1
        [error]
  in
  let responseMessage = ResponseMessage 0 1 [response]
  in 
  responseMessage 

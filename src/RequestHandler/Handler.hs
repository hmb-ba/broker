module RequestHandler.Handler (
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

import Common.Types
import Network.Types
import Network.Parser
import Log.Writer

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
  readStream conn
  listenLoop sock

readStream :: (Socket, SockAddr) -> IO()
readStream (sock, sockaddr) = do
  input <- SockBL.recv sock 4096
  let i  = input
  requestMessage <- readRequest i
  case apiKey requestMessage of
    0  ->  handleProduceRequest $ request requestMessage
    1  -> putStrLn "FetchRequest"
    2  -> putStrLn "OffsetRequest"
    3  -> putStrLn "MetadataRequest"
    8  -> putStrLn "OffsetCommitRequest"
    9  -> putStrLn "OffsetFetchRequest"
    10 -> putStrLn "ConsumerMetadataRequest"
    _  -> putStrLn "Unknown ApiKey"
  print requestMessage
  return ()

handleProduceRequest :: Request -> IO()
handleProduceRequest req = do
  mapM writeLog [ (BS.unpack(topicName x), fromIntegral(partitionNumber y), messageSet y ) | x <- topics req, y <- partitions x ]
  return()
 


module RequestHandler.Handler (
  initHandler
) where 

import Network.Socket 
import Network.Types
import qualified Network.Socket.ByteString.Lazy as SockBL
import System.IO 
import System.Environment
import Control.Concurrent
import Network.Parser

initHandler :: IO()
initHandler = do 
  sock <- socket AF_INET Stream 0 
  setSocketOption sock ReuseAddr 1 
  bindSocket sock (SockAddrInet 4343 iNADDR_ANY)
  listen sock  2 
  forkIO (listenLoop sock)
  return()
  --listenLoop sock 

listenLoop :: Socket -> IO()
listenLoop sock =  do 
  print "A"
  conn <- accept sock 
  print "Aa"
  --forkIO (readStream conn)
  readStream conn
  listenLoop sock 

readStream :: (Socket, SockAddr) -> IO() 
readStream (sock, sockaddr) = do 
  print "B"
  input <- SockBL.recv sock 4096 
  let request = input
  requestMessage <- readRequest request
  print requestMessage 
  return () 



module Main (
  main
) where

import Log.Parser
import Log.Writer
import Protocol.Parser
import Protocol.Writer
import Network.Socket
import System.IO
import System.Environment
import qualified Data.ByteString.Lazy as BL

main = do
  parseLogData
  parseNetworkData
  putStrLn "done"

parseLogData = do 
  file <- getArgs
  log <- parseLog $ head file
  print log
  writeLog "myfile" 0 0 log

parseNetworkData = do
  request <- parseData $ "data/payload"
  print request 

  --write request to socket (debug with: nc -l 4242 | xxd)
  sock <- socket AF_INET Stream  defaultProtocol 
  setSocketOption sock ReuseAddr 1 
  connect sock (SockAddrInet 4242 iNADDR_ANY)
  hdl <- socketToHandle sock WriteMode 
  writeProtocol hdl request

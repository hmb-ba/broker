module Main (
  main
) where

import Network.Socket
import System.IO
import System.Environment
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Lazy.Char8 as BC
import Control.Monad
import Control.Concurrent.Async
import Control.Concurrent
import Control.Monad.Fix
--HMB
import Kafka.Protocol
import HMB.Internal.Log
import HMB.Internal.Handler

main = do
  --parseLogData

  (sock, chan) <- initHandler
  
  listenLoop (sock, chan)
  putStrLn "loop"
   --mapM_ wait[t1]
  forkIO $ fix $ \loop -> do
    msg <- readChan chan
    res <- handleRequest msg
    case res of 
      Left e -> putStrLn $ "[HandleError] " ++ show e
      Right r -> putStrLn $ BC.unpack r 
    loop
  putStrLn "exit"
  
  

  --sendNetworkData

--parseLogData = do
  --file <- getArgs
  --log <- parseLog $ head file
  --print log
  --writeLog "myfile" 0 0 log


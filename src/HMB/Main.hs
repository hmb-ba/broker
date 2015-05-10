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
  done <- newEmptyMVar 

  sock <- initSocket
  chan <- initReqChan

  forkIO $ runAcceptor sock chan
  putStrLn "***Acceptor Thread started to work"

    --forkIO $ fix $ \loop -> do
  forkIO $ runApiHandler chan
  putStrLn "***API Worker Thread Started to work"
  --  loop
  --  putMVar done ()

  takeMVar done  

  putStrLn "exit"
  
  

  --sendNetworkData

--parseLogData = do
  --file <- getArgs
  --log <- parseLog $ head file
  --print log
  --writeLog "myfile" 0 0 log


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
  rqChan <- initReqChan
  rsChan <- initResChan

  forkIO $ runAcceptor sock rqChan
  putStrLn "***Acceptor Thread started***"
  
  forkIO $ runResponder rsChan 
  putStrLn "***Responder Thread started***"

    --forkIO $ fix $ \loop -> do
  forkIO $ runApiHandler rqChan rsChan
  putStrLn "***API Worker Thread started"
  --  loop
  --  putMVar done ()

  --takeMVar done  
  forever $ threadDelay 100000000  --TODO: Managed Threads and wait for all Threads to be finished

  putStrLn "exit"




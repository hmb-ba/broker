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
  rsChan <- initResChan

  forkIO $ runAcceptor sock chan
  putStrLn "***Acceptor Thread started***"
  
  forkIO $ runResponser rsChan 
  putStrLn "***Responser Thread started***"

    --forkIO $ fix $ \loop -> do
  forkIO $ runApiHandler chan rsChan
  putStrLn "***API Worker Thread started"
  --  loop
  --  putMVar done ()

  takeMVar done  
  --threadDelay 100000000

  putStrLn "exit"



module Main (
  main
) where

import HMB.Internal.Types
import HMB.Internal.Network
import HMB.Internal.API

import Control.Monad
import Control.Concurrent.Async
import Control.Concurrent

main = do

  sock <- initSock
  rqChan <- initRqChan
  rsChan <- initRsChan

  withAsync (runAcceptor sock rqChan) $ \a1 -> do 
    putStrLn "***Acceptor Thread started***"
    withAsync (runResponder rsChan) $ \a2 -> do 
      putStrLn "***Responder Thread started***"
      withAsync (runApiHandler rqChan rsChan) $ \a3 -> do 
        putStrLn "***API Worker Thread started"
        page1 <- wait a1
        page2 <- wait a2
        page3 <- wait a3
        putStrLn "exit"

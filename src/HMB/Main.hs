module Main (
  main
) where

import Control.Monad
import Control.Concurrent.Async
import Control.Concurrent
import HMB.Internal.Handler

main = do

  sock <- initSocket
  rqChan <- initReqChan
  rsChan <- initResChan

  runAcceptor sock rqChan 
--  withAsync (runAcceptor sock rqChan) $ \a1 -> do 
--    putStrLn "***Acceptor Thread started***"
--    withAsync (runResponder rsChan) $ \a2 -> do 
--      putStrLn "***Responder Thread started***"
--      withAsync (runApiHandler rqChan rsChan) $ \a3 -> do 
--        putStrLn "***API Worker Thread started"
--        page1 <- wait a1
--        page2 <- wait a2
--        page3 <- wait a3
--        putStrLn "exit"

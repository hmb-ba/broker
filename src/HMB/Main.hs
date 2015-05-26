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
  done <- newEmptyMVar 

  sock <- initSocket
  rqChan <- initReqChan
  rsChan <- initResChan

  forkIO $ runAcceptor sock rqChan
  putStrLn "***Acceptor Thread started***"

  forkIO $ runResponder rsChan 
  putStrLn "***Responder Thread started***"

  forkIO $ runApiHandler rqChan rsChan
  putStrLn "***API Worker Thread started"

  forever $ threadDelay 100000000  --TODO: Managed Threads and wait for all Threads to be finished
  -- FIXME (meiersi): yes please ;-) you can use 'withAsync' from the 'async'
  -- package. It is not perfect, as explained in this lenghty thread
  --
  --   http://www.reddit.com/r/haskell/comments/36tjca/neil_mitchells_haskell_blog_handling_controlc_in/
  --
  -- but it should get you to a reasonable place.
  -- Ideally, you expose your server as a service as described in the service
  -- pattern and start it using a 'withHandle :: (Handle -> IO a) -> IO a'
  -- function that guarantees proper resource cleanup on exit of the inner
  -- function.

  putStrLn "exit"




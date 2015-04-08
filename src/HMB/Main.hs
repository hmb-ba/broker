module Main (
  main
) where

import Network.Socket
import System.IO
import System.Environment
import qualified Data.ByteString.Lazy as BL
import Control.Monad
import Control.Concurrent.Async 

--HMB
import Kafka.Protocol
import HMB.Internal.Log
import HMB.Internal.Handler

main = do
  --parseLogData
  
  sock <- initHandler
  forever $ do
    listenLoop sock
    putStrLn "loop"
   --mapM_ wait[t1]
  putStrLn "exit"

  --sendNetworkData

--parseLogData = do 
  --file <- getArgs
  --log <- parseLog $ head file
  --print log
  --writeLog "myfile" 0 0 log


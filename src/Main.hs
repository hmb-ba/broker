module Main (
  main
) where

import Log.Parser
import Log.Writer
import Network.Parser
import Network.Writer
import Network.Socket
import RequestHandler.Handler
import System.IO
import System.Environment
import qualified Data.ByteString.Lazy as BL
import RequestHandler.Handler
import Control.Monad
import Control.Concurrent.Async 

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


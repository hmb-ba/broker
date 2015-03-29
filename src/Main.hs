module Main (
  main
) where

import Log.Parser
import Log.Writer
import Protocol.Parser
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

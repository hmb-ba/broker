module Main (
  main
) where

import Log.Parser
import Log.Writer
import System.Environment

main = do
  file <- getArgs
  log <- parseLog $ head file
  print log
  writeLog "myfile" 0 0 log
  putStrLn "done"

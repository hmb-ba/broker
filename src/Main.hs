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
  writeLogEntry "myfile" 0 $ head log
  putStrLn "done"

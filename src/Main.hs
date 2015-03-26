module Main (
  main
) where

import Log.Parser
import System.Environment

main = do
  file <- getArgs
  log <- parseLog $ head file
  print log
  putStrLn "done"

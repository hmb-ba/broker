module HMB.Internal.Log.Parser
( parseLog ) where

import HMB.Common
import Data.Binary.Get
import qualified Data.ByteString.Lazy as BL

getLog :: Get Log
getLog = do
  empty <- isEmpty
  if empty
      then return []
      else do messageSet <- messageSetParser
              messageSets <- getLog
              return (messageSet:messageSets)

parseLog :: String -> IO Log
parseLog a = do
  input <- BL.readFile a
  return (runGet getLog input)


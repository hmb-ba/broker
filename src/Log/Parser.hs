module Log.Parser
( parseLog ) where

import Common.Types
import Common.Parser
import Log.Types
import Data.Binary.Get
import qualified Data.ByteString.Lazy as BL

getMessageSets :: Get Log
getMessageSets = do
  empty <- isEmpty
  if empty
      then return []
      else do messageSet <- messageSetParser
              messageSets <- getMessageSets
              return (messageSet:messageSets)

parseLog :: String -> IO Log
parseLog a = do
  input <- BL.readFile a
  return (runGet getMessageSets input)


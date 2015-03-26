module Log.Parser
( parseLog ) where

import Log.Types
import Data.Binary.Get
import qualified Data.ByteString.Lazy as BL

entryParser :: Get LogEntry
entryParser = do
  offset <- getWord64be
  len    <- getWord32be
  crc    <- getWord32be
  magic  <- getWord8
  attr   <- getWord8
  p      <- payloadParser
  return $! LogEntry offset len crc magic attr p

payloadParser :: Get Payload
payloadParser = do
  keylen <- getWord32be
  paylen <- getWord32be
  payload <- getByteString $ fromIntegral paylen
  return $! Payload keylen paylen payload

getEntries :: Get Log
getEntries = do
  empty <- isEmpty
  if empty
      then return []
      else do entry <- entryParser
              entries <- getEntries
              return (entry:entries)

parseLog :: String -> IO Log
parseLog a = do
  input <- BL.readFile a
  return (runGet getEntries input)


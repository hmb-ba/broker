-- |
-- Module      : HMB.Internal.Index
-- Copyright   : (c) Marc Juchli, Lorenz Wolf 2015
-- License     : BSD-style
--
-- Maintainer  :
-- Stability   : WIP
-- Portability : GHC
--
-- This module encapsulates Index functionality.

module HMB.Internal.Index
  ( new
  , append
  , IndexState(..)
  ) where

import Kafka.Protocol

import Data.Word
import Data.List hiding (find)
import qualified Data.Map.Lazy as Map
import Data.Maybe
import Text.Printf

import Control.Concurrent.MVar

import System.IO


-- dupl
type TopicStr = String
type PartitionNr = Int
--
type OffsetPosition = (RelativeOffset, FileOffset)
type RelativeOffset = Word32
type FileOffset = Word32

type Indices = Map.Map (TopicStr, PartitionNr) [OffsetPosition]
newtype IndexState = IndexState (MVar Indices)

-- | Creates a new and empty IndexState. The index is represented as a Map
-- whereas the key is a tuple of topic and partition.
new :: IO IndexState
new = do
  m <- newMVar Map.empty
  return (IndexState m)

-- | Appends an OffsetPosition to memory and eventually writes to disk. The
-- index will be kept in memory as long as the broker is running.
append :: (IndexState, TopicStr, PartitionNr, Log) -> IO ()
append (IndexState m, t, p, ms) = do
  indices <- takeMVar m
  let bo = 0 -- todo: directory state
  let old = find (t, p) indices
  let path = getPath (logFolder t p) (indexFile bo)
  fs <- getFileSize path
  let new = pack (fromIntegral (offset (head ms)) - bo, fs)
  let newIndex = old ++ [new]
  let newIndices = Map.insert (t, p) newIndex indices
  -- 4. write to disk
  putMVar m indices

-- | Find a list of OffsetPosition within the map of Indices. If nothing is
-- found, return an empty List
-- TODO: make generic
find :: (TopicStr, PartitionNr) -> Indices -> [OffsetPosition]
find (t, p) indices = fromMaybe [] (Map.lookup (t, p) indices)

getFileSize :: String -> IO Integer
getFileSize path = do
    size <- withFile path ReadMode (\hdl -> hFileSize hdl)
    return size

pack :: (Int, Integer) -> OffsetPosition
pack (o, fs) = (fromIntegral o, fromIntegral fs)

--buildOffsetPosition :: OffsetPosition -> Put
--buildOffsetPosition (o, p) = do
--    putWord32be o
--    putWord32be p
--
--encodeIndex = runPutOrFail buildOffsetPosition
--
--
--getLastOffsetPosition' :: BL.ByteString -> OffsetPosition
--getLastOffsetPosition' bs =
--  case decodeIndex bs of
--    Left (bs, bo, e) -> (0,0)
--    Right (bs, bo, ops) -> lastIndex ops


---------DUPLICATE----------
logFolder :: TopicStr -> PartitionNr -> String
logFolder t p = "log/" ++ t ++ "_" ++ show p

leadingZero :: Int -> String
leadingZero = printf "%020d"

logFile :: Int -> String
logFile o = leadingZero o ++ ".log"

indexFile :: Int -> String
indexFile o = leadingZero o ++ ".index"

getPath :: String -> String -> String
getPath folder file = folder ++ "/" ++ file

-- |
-- Module      : HMB.Internal.Log
-- Copyright   : (c) Marc Juchli, Lorenz Wolf 2015
-- License     : BSD-style
--
-- Maintainer  :
-- Stability   : WIP
-- Portability : GHC
--
-- This module encapsulates actions to the log on the filesystem.
-- Fundamental functions are appending MessageSet's to Log or reading from
-- it.

module HMB.Internal.Log
  ( getTopicNames
  , new
  , find
  , size
  , sizeRange
  , append
  , lookup
  , getTopics
  , lastOffset
  , continueOffset
  , getBaseOffset
  , LogState(..)
  ) where

import Control.Concurrent.MVar

import Data.List hiding (find, lookup)
import qualified Data.Map.Lazy as Map
import qualified Data.ByteString.Lazy as BL
import Data.Binary.Get
import Data.Binary.Put
import Data.Maybe
import Data.Word
import Data.Int

import qualified HMB.Internal.LogConfig as L

import Kafka.Protocol

import Prelude hiding (lookup)

import System.Directory
import System.IO.MMap
import System.IO



----------------------------------------------------------
-- Log Writer
----------------------------------------------------------

type OffsetPosition = (RelativeOffset, FileOffset)
type RelativeOffset = Word32
type FileOffset = Word32
type BaseOffset = Int

type Logs = Map.Map (L.TopicStr, L.PartitionNr) Log

-- FIXME (SM): I think it should be such that each partition has its own MVar,
-- as one can easily write and read concurrently to different partitions.
-- Ideally, you can structure the read-path such that it does not need to take
-- a lock. For the write case you could also structure the code such that
-- there is exactly one writer-thread for each partition. In this case, you'd
-- only need bounded channels for sending messages to these threads. One
-- should then be able to arrange it such that a message flows through the
-- system without contention on any lock.
newtype LogState = LogState (MVar Logs)

getTopics :: IO [String]
getTopics = getDirectoryContents "log/"

-- | Creates a new and empty log state. The log is represented as a Map
-- where the key is a tuple of topic and partition.
new :: IO LogState
new = do
  m <- newMVar Map.empty
  return (LogState m)

-- | Returns the effective (built) size of a log
size :: Log -> Int64
size = BL.length . runPut . buildMessageSets

-- | Determines the size of a log between a given range of offset
sizeRange :: Maybe Offset -> Maybe Offset -> Log -> Int64
sizeRange Nothing Nothing log = size log
sizeRange Nothing (Just to) log = size $ filter (\x -> msOffset x <= to) log
sizeRange (Just from) Nothing log = size $ filter (\x -> msOffset x >= from) log
sizeRange (Just from) (Just to) log = size $ filter (\x -> msOffset x >=from && msOffset x <= to) log

-- | Find a Log within the map of Logs. If nothing is found, return an empty
-- List
find :: (L.TopicStr, L.PartitionNr) -> Logs -> Log
find (t, p) logs = fromMaybe [] (Map.lookup (t, p) logs)

-- | Controls the number of messages accumulated in each topic (partition)
-- before the data is flushed to disk and made available to consumers.
isFlushInterval :: Log -> Bool
isFlushInterval log = 500 <= length log

-- | Synchronize collected log with disk, but only if the flush interval is
-- reached.
append :: (L.TopicStr, L.PartitionNr) -> Logs -> IO Logs
append (t, p) logs = do
  let log = find (t, p) logs
  let logToSync = if (msOffset $ head log) == 0 then log else tail log
  if isFlushInterval logToSync
      then do
          write (t, p, logToSync)
          let keepLast = [last logToSync]
          return (Map.insert (t, p) keepLast logs)
      else return logs

-- | Effectively write log to disk in append mode
write :: (L.TopicStr, Int, Log) -> IO ()
write (t, p, ms) = do
  let bo = 0 -- PERFORMANCE
  --bo <- getBaseOffset (t, p) Nothing -- TODO: directory state
  let logPath = L.getPath (L.logFolder t p) (L.logFile bo)
  let bs = runPut $ buildMessageSets ms
  withFile logPath AppendMode $ \hdl -> BL.hPut hdl bs


offsetFromFileName :: String -> Int
offsetFromFileName = read . reverse . snd . splitAt 4 . reverse

isLogFile :: String -> Bool
isLogFile x = ".log" `isInfixOf` x

isDirectory :: String -> Bool
isDirectory x = x `elem` [".", ".."]

filterRootDir :: [String] -> [String]
filterRootDir = filter (\x -> not $ isDirectory x)

maxOffset' :: [Int] -> Int
maxOffset' [] = 0
maxOffset' [x] = x
maxOffset' xs = maximum xs

nextSmaller :: [Int] -> Offset -> Int
nextSmaller [] _ = 0
nextSmaller [x] _ = x
nextSmaller xs x = last $ filter (<(fromIntegral x)) $ sort xs

getBaseOffsets :: (L.TopicStr, Int) -> IO [BaseOffset]
getBaseOffsets (t, p) = do
  dirs <- getDirectoryContents $ L.logFolder t p
  return $ map (offsetFromFileName) (filter (isLogFile) (filterRootDir dirs))

-- | Returns the base offset for a tuple of topic and partition,
-- provided by a request message. If the second argument remains Nothing,
-- the highest number of available log/index files will be return. Otherwise,
-- the base offset, in whose related log file the provided offset is stored,
-- is returned.
getBaseOffset :: (L.TopicStr, Int) -> Maybe Offset -> IO BaseOffset
getBaseOffset (t, p) o = do
  bos <- getBaseOffsets (t, p)
  case o of
      Nothing -> return $ maxOffset' bos
      Just o -> return $ nextSmaller bos o


lastOffset :: Log -> Maybe Offset
lastOffset [] = Nothing
lastOffset xs = Just $ (msOffset . last) xs

assignOffset :: Offset -> MessageSet -> MessageSet
assignOffset o ms = MessageSet o (msLen ms) (msMessage ms)

-- | Increment offset over every provided messageset based on a given offset
-- (typically last log offset)
continueOffset :: Offset -> Log -> [MessageSet]
continueOffset o [] = []
continueOffset o (m:ms) = assignOffset o m : continueOffset (o + 1) ms


-------------------------------------------------------
-- Read
-------------------------------------------------------

getLog :: Get Log
getLog = do
  empty <- isEmpty
  if empty
      then return []
      else do messageSet <- messageSetParser
              messageSets <- getLog
              return (messageSet:messageSets)


getFileSize :: String -> IO Integer
getFileSize path = do
  size <- withFile path ReadMode (\hdl -> hFileSize hdl)
  return size

decodeLog :: Get Log
decodeLog = do
  empty <- isEmpty
  if empty
    then return []
      else do ms <- messageSetParser
              mss <- decodeLog
              return $ ms : mss

filterByOffset :: Offset -> Log -> Log
filterByOffset to = filter (\x -> msOffset x >= fromIntegral to)

lookup :: (L.TopicStr, Int) -> BaseOffset -> OffsetPosition -> Offset -> IO Log
lookup (t, p) bo (_, phy) o = do
  let path = L.getPath (L.logFolder t p) (L.logFile bo)
  fs <- getFileSize path
  --print phy
  bs <- mmapFileByteStringLazy path $ Just (fromIntegral phy, (fromIntegral (fs) - fromIntegral phy))
  let log = runGet decodeLog bs
  return $ filterByOffset o log


-- | TopicNames for Metadata Request
getTopicNames :: IO [String]
getTopicNames = do
  dirs <- (getDirectoryContents "log/")
  return (map topicFromFileName $ filterRootDir dirs)

topicFromFileName :: [Char] -> [Char]
topicFromFileName = reverse . snd . splitAt 2 . reverse


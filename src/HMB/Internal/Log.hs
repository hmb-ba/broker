-- |
-- Module      : HMB.Internal.API
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

--
-- -- > import ...
--
module HMB.Internal.Log
( readLog
, getTopicNames

, new
, HMB.Internal.Log.insert
, LogState(..)
) where

import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Lazy.Char8 as BCL
import Data.Binary.Get
import Data.Binary.Put
import Data.Maybe
import qualified Data.Map.Lazy as Map
import Data.Word
import Data.List hiding (find)
import Data.Int

import Text.Printf

import System.Directory
import System.IO.MMap
import System.IO

import qualified Control.Monad.Trans.Resource as R
import Control.Concurrent.MVar
import Control.Conditional
import Control.Monad
import Control.Applicative

import Kafka.Protocol


----------------------------------------------------------
-- Log Writer (old)
----------------------------------------------------------

type MessageInput = (TopicStr, PartitionStr, Log)
type PartitionStr = Int

--writeLog :: MessageInput -> IO()
--writeLog (topicName, partitionNumber, log) = do
--  createDirectoryIfMissing False $ logFolder topicName partitionNumber
--  let filePath = getPath (logFolder topicName partitionNumber) (logFile 0)
--  ifM (doesFileExist filePath)
--      (appendToLog filePath (topicName,partitionNumber, log))
--      (newLog filePath (topicName,partitionNumber, log))



maxOffset :: [Offset] -> Offset
maxOffset [] = 0
maxOffset [x] = x
maxOffset (x:xs) = max x (maxOffset xs)


--newLog :: String -> MessageInput -> IO()
--newLog filepath (t, p, log) = do
--  let l = buildLog 0 log
--  BL.writeFile filepath l
--  return ()


getMaxOffsetOfLog :: MessageInput -> IO Offset
getMaxOffsetOfLog (t, p, _) = do
  log <- readLogFromBeginning (t,p) --TODO: optimieren, dass nich gesamter log gelesen werden muss
  return (maxOffset $ [ offset x | x <- log ])

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

readLogFromBeginning :: (String, Int) -> IO Log
readLogFromBeginning (t, p) = parseLog $
    getPath (logFolder t p) (logFile 0)

readLog' :: (String, Int, Int) -> IO Log
readLog' (t, p, o) = do
  log <- readLogFromBeginning (t,p)
  return ([ x | x <- log, fromIntegral(offset x) >= o])

getTopicNames :: IO [String]
getTopicNames = (getDirectoryContents "log/")


----------------------------------------------------------
-- Log Writer
----------------------------------------------------------

type TopicStr = String
type PartitionNr = Int

type LogSegment = (FilemessageSet, OffsetIndex)
type FilemessageSet = [MessageSet]
type OffsetIndex = [OffsetPosition]
type OffsetPosition = (RelativeOffset, FileOffset)
type RelativeOffset = Word32
type FileOffset = Word32
type BaseOffset = Int

type Logs = Map.Map (TopicStr, PartitionNr) [MessageSet]
newtype LogState = LogState (MVar Logs)

new :: IO LogState
new = do
  m <- newMVar Map.empty
  return (LogState m)

insert :: (LogState, TopicStr, PartitionNr, [MessageSet]) -> IO ()
insert (LogState m, t, p, ms) = do
  logs <- takeMVar m
  let log = find (t, p) logs
  let llo = fromMaybe 0 (lastOffset log)
  let newLog = log ++ continueOffset (nextOffset llo) ms
  let logToSync = Map.insert (t, p) newLog logs
  syncedLogs <- sync (t, p) logToSync
  putMVar m syncedLogs

logSize :: Log -> Int64
logSize = BL.length . runPut . buildMessageSets

find :: (TopicStr, PartitionNr) -> Logs -> [MessageSet]
find (t, p) logs = fromMaybe [] (Map.lookup (t, p) logs)

-- | Controls the number of messages accumulated in each topic (partition)
-- before the data is flushed to disk and made available to consumers.
isFlushInterval :: Log -> Bool
isFlushInterval log = 500 <= (length log)

sync :: (TopicStr, PartitionNr) -> Logs -> IO (Logs)
sync (t, p) logs = do
  let log = find (t, p) logs
  let logToSync = if (offset $ head log) == 0 then log else tail log
  --putStrLn $ "size of log: " ++ show (length logToSync)
  case isFlushInterval logToSync of
      False -> return logs
      True -> do
        appendLog (t, p, logToSync)
        let keepLast = (last logToSync) : []
        return (Map.insert (t, p) keepLast logs)

appendLog :: (TopicStr, Int, [MessageSet]) -> IO (Log)
appendLog (t, p, ms) = do
  --putStrLn "appendlog now"
  --bo <- getBaseOffset (t, p) Nothing -- todo: directory state
  let bo = 0
  let logPath = getPath (logFolder t p) (logFile bo)
  let bs = runPut $ buildMessageSets ms
  BL.appendFile logPath bs
  --withFile logPath WriteMode $ \hdl -> do
  --    BL.hPut hdl bs
  return $ (last ms) : []


-- | The byte interval at which we add an entry to the offset index. When
-- executing a fetch request the server must do a linear scan for up to this
-- many bytes to find the correct position in the log to begin and end the
-- fetch. So setting this value to be larger will mean larger index files (and a
-- bit more memory usage) but less scanning. However the server will never add
-- more than one index entry per log append (even if more than
-- log.index.interval worth of messages are appended).
isIndexInterval :: Log -> Bool
isIndexInterval log = 4096 < (logSize log)

----------------------------------------------------------


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

lastIndex :: [OffsetPosition] -> OffsetPosition
lastIndex [] = (0,0)
lastIndex xs = last xs


----------------------------------------------------------


offsetFromFileName :: [Char] -> Int
offsetFromFileName = read . reverse . snd . splitAt 4 . reverse

isLogFile :: [Char] -> Bool
isLogFile x = ".log" `isInfixOf` x

isDirectory :: [Char] -> Bool
isDirectory x = elem x [".", ".."]

filterRootDir :: [String] -> [String]
filterRootDir d = filter (\x -> not $ isDirectory x) d

getLogFolder :: (TopicStr, Int) -> String
getLogFolder (t, p) = "log/" ++ t ++ "_" ++ show p

maxOffset' :: [Int] -> Int
maxOffset' [] = 0
maxOffset' [x] = x
maxOffset' xs = maximum xs

nextSmaller :: [Int] -> Offset -> Int
nextSmaller [] _ = 0
nextSmaller [x] _ = x
nextSmaller xs x = last $ filter (<(fromIntegral x)) $ sort xs

getBaseOffsets :: (TopicStr, Int) -> IO [BaseOffset]
getBaseOffsets (t, p) = do
  dirs <- getDirectoryContents $ getLogFolder (t, p)
  return $ map (offsetFromFileName) (filter (isLogFile) (filterRootDir dirs))

-- | Returns the base offset for a tuple of topic and partition,
-- provided by a request message. If the second argument remains Nothing,
-- the highest number of available log/index files will be return. Otherwise,
-- the base offset, in whose related log file the provided offset is stored,
-- is returned.
getBaseOffset :: (TopicStr, Int) -> Maybe Offset -> IO BaseOffset
getBaseOffset (t, p) o = do
  bos <- getBaseOffsets (t, p)
  case o of
      Nothing -> return $ maxOffset' bos
      Just o -> return $ nextSmaller bos o


-------------------------------------------------------


-- decode as long as physical position != 0 which means last index has passed
decodeIndexEntry :: Get [OffsetPosition]
decodeIndexEntry = do
  empty <- isEmpty
  if empty
    then return []
    else do rel  <- getWord32be
            phys <- getWord32be
            case phys of
              0 -> return $ (rel, phys)  : []
              _ -> do
                    e <- decodeIndexEntry
                    return $ (rel, phys) : e

decodeIndex :: BL.ByteString -> Either (BL.ByteString, ByteOffset, String) (BL.ByteString, ByteOffset, [OffsetPosition])
decodeIndex = runGetOrFail decodeIndexEntry

getLastOffsetPosition :: (TopicStr, Int) -> BaseOffset -> IO OffsetPosition
-- get offset of last index entry
-- 1. open file to bs
-- 2. parse as long as not 0
-- 3. read offset/physical from last element
getLastOffsetPosition (t, p) bo = do
  let path = getPath (getLogFolder (t, p)) (indexFile bo)
  -- check if file exists
  bs <- mmapFileByteStringLazy path Nothing
  case decodeIndex bs of
    Left (bs, bo, e)   -> do
        print e
        return $ (0,0) --todo: error handling
    Right (bs, bo, ops) -> return $ lastIndex ops

getLastOffsetPosition' :: BL.ByteString -> OffsetPosition
getLastOffsetPosition' bs =
  case decodeIndex bs of
    Left (bs, bo, e) -> (0,0)
    Right (bs, bo, ops) -> lastIndex ops


-------------------------------------------------------


getFileSize :: String -> IO Integer
getFileSize path = do
  size <- withFile path ReadMode (\hdl -> hFileSize hdl)
--  hdl <- openFile path ReadMode
--  size <- hFileSize hdl
--  print size
  return size

lastOffset :: Log -> Maybe Offset
lastOffset [] = Nothing
lastOffset xs = Just $ (offset . last) xs

getLastLogOffset :: (TopicStr, Int) -> BaseOffset -> OffsetPosition -> IO Offset
-- find last Offset in the log, start search from given offsetposition
-- 1. get file Size for end of file position
-- 2. open log file from start position given by offsetPosition to eof position
-- 3. parse log and get highest offset
getLastLogOffset (t, p) bo (rel, phys) = do
  let path = getPath (logFolder t p) (logFile bo)
  -- check if file exists
  fs <- getFileSize path
  --print $ "physical start: " ++ (show $ fromIntegral phys)
  --print $ "filesize: " ++ show fs

  -- FIXME (meiersi): this will leak resources! I suggest to read up on
  -- 'ResourceT' to handle that properly
  -- <https://hackage.haskell.org/package/resourcet>.
--  bs <- mmapFileByteStringLazy path $ Nothing -- Just (fromIntegral phys, (fromIntegral (fs) - fromIntegral phys))

  hdl <- openFile path ReadMode
  bs <- BL.hGetContents hdl

  case lastOffset $ runGet getLog bs of
    Nothing -> return 0
    Just lo -> return lo

getLastLogOffset' :: BL.ByteString -> Maybe Offset
getLastLogOffset' bs = lastOffset $ runGet getLog bs

-------------------------------------------------------


assignOffset :: Offset -> MessageSet -> MessageSet
assignOffset o ms = MessageSet o (len ms) (message ms)

-- increment offset over every provided messageset based on a given offset (typically last log offset)
continueOffset :: Offset -> [MessageSet] -> [MessageSet]
continueOffset o [] = []
continueOffset o (m:ms) = assignOffset o m : continueOffset (o + 1) ms

-------------------------------------------------------



nextOffset :: Offset -> Offset
nextOffset o = o + 1

withinIndexInterval :: Integer -> Bool
withinIndexInterval 0 = False
withinIndexInterval fs = 0 == (fs `mod` 100)

appendIndex :: String -> OffsetPosition -> IO ()
appendIndex path op = do
  let bs = runPut $ buildOffsetPosition op
  BL.appendFile path bs

buildOffsetPosition :: OffsetPosition -> Put
buildOffsetPosition (o, p) = do
    putWord32be o
    putWord32be p


---------------
-- ResourceT
-- ------------



-------------------------------------------------------
-- Read
-------------------------------------------------------

readLog :: (TopicStr, Int) -> Offset -> IO [MessageSet]
readLog tp o = do
  bos <- getBaseOffsets tp
  let bo = getBaseOffsetFor bos o
  op <- indexLookup tp bo o
  print op
  log <- getLogFrom tp bo op
  return $ filterMessageSetsFor log o

indexLookup :: (TopicStr, Int) -> BaseOffset -> Offset -> IO OffsetPosition
---locate the offset/location pair for the greatest offset less than or equal
-- to the target offset.
indexLookup (t, p) bo to = do
  let path = getPath (getLogFolder (t, p)) (indexFile bo)
  bs <- mmapFileByteStringLazy path Nothing
  case decodeIndex bs of
    Left (bs, byo, e)   -> do
        print e
        return $ (0,0) --todo: error handling
    Right (bs, byo, ops) -> do
      print ops
      return $ getOffsetPositionFor ops bo to

getOffsetPositionFor :: [OffsetPosition] -> BaseOffset -> Offset -> OffsetPosition
-- get greatest offsetPosition from list that is less than or equal target offset
getOffsetPositionFor [] bo to = (0, 0)
getOffsetPositionFor [x] bo to = x
getOffsetPositionFor (x:xs) bo to
       | targetOffset <= absoluteIndexOffset = (0,0)
       | absoluteIndexOffset <= targetOffset && targetOffset < nextAbsoluteIndexOffset = x
       | otherwise = getOffsetPositionFor xs bo to
  where  nextAbsoluteIndexOffset = ((fromIntegral $ fst $ head $ xs) + bo)
         absoluteIndexOffset = (fromIntegral $ fst $ x) + bo
         targetOffset = fromIntegral $ to

getBaseOffsetFor :: [BaseOffset] -> Offset -> BaseOffset
-- get greatest baseOffset from list that is less than or equal target offset
getBaseOffsetFor [] to = 0
getBaseOffsetFor [x] to = x
getBaseOffsetFor (x:xs) to = if (x <= fromIntegral to && fromIntegral to < head xs) then x else getBaseOffsetFor xs to

-- searchLogFor
-- Search forward the log file  for the position of the last offset that is greater than or equal to the target offset
-- and return its physical position

getLogFrom :: (TopicStr, Int) -> BaseOffset -> OffsetPosition -> IO [MessageSet]
-- ParseLog starting from given physical Position.
getLogFrom (t, p) bo (_, phy) = do
  let path = getPath (logFolder t p) (logFile bo)
  fs <- getFileSize path
  --print phy
  bs <- mmapFileByteStringLazy path $ Just (fromIntegral phy, (fromIntegral (fs) - fromIntegral phy))
  return $ runGet decodeLog bs

decodeLog :: Get [MessageSet]
decodeLog = do
  empty <- isEmpty
  if empty
    then return []
      else do ms <- messageSetParser
              mss <- decodeLog
              return $ ms : mss

filterMessageSetsFor :: [MessageSet] -> Offset -> [MessageSet]
filterMessageSetsFor ms to = filter (\x -> offset x >= fromIntegral to) ms



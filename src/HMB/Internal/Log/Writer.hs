module HMB.Internal.Log.Writer
( writeLog
, readLog
, getTopicNames
, getLastBaseOffset
, getLastOffsetPosition
, getLastLogOffset
, continueOffset
, appendLog
) where

import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Char8 as BC

import Data.Binary.Put
import System.Directory
import Control.Conditional
import Control.Monad
import Kafka.Protocol

import Data.Binary.Get
import Data.Word
import Control.Applicative

import System.IO.MMap
import System.IO
--import System.Posix.Types --for file size 
import qualified System.Posix.Files as Files
import Data.List
--import qualified Data.Text as Text -- for isInfixOf
import Text.Printf


type MessageInput = (TopicStr, PartitionStr, Log)
type PartitionStr = Int


buildLog :: Offset -> Log -> BL.ByteString
buildLog o [] = BL.empty
buildLog o (x:xs) =
  (BL.append (buildLogEntry x o) (buildLog (o + 1) xs))

writeLog :: MessageInput -> IO()
writeLog (topicName, partitionNumber, log) = do
  createDirectoryIfMissing False $ logFolder topicName partitionNumber
  let filePath = getPath (logFolder topicName partitionNumber) (logFile 0)
  ifM (doesFileExist filePath) 
      (appendToLog filePath (topicName,partitionNumber, log)) 
      (newLog filePath (topicName,partitionNumber, log))

--writeLogOrFail :: MessageInput -> IO(Either String ())
--writeLogOrFail input = do
-- r <- tryIOError(writeLog input)
-- case r of 
--    Left e -> return $ Left $ show e 
--    Right io -> return $ Right io 

appendToLog :: String -> MessageInput -> IO() 
appendToLog filepath (t, p, log)  = do 
  o <- getMaxOffsetOfLog (t, p, log)
  print o  --TODO: is needed for preventing file lock ...
  let l =  buildLog (o + 1) log
  BL.appendFile filepath l
  return ()

newLog :: String -> MessageInput -> IO()
newLog filepath (t, p, log) = do 
  let l = buildLog 0 log
  BL.writeFile filepath l
  return ()

maxOffset :: [Offset] -> Offset 
maxOffset [] = 0 
maxOffset [x] = x
maxOffset (x:xs) = max x (maxOffset xs)

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

--------------

type TopicStr = String 
type PartitionNr = Int

type LogSegment = (FilemessageSet, OffsetIndex)
type FilemessageSet = [MessageSet]
type OffsetIndex = [OffsetPosition]
type OffsetPosition = (RelativeOffset, FileOffset)
type RelativeOffset = Word32
type FileOffset = Word32
type BaseOffset = Int --Word64


logFolder :: TopicStr -> PartitionNr -> String
logFolder t p = "log/" ++ t ++ "_" ++ show p

leadingZero :: Int -> String
leadingZero = printf "%020d"

logFile :: Int -> String
logFile o = (leadingZero o) ++ ".log"

indexFile :: Int -> String
indexFile o = (leadingZero o) ++ ".index"

getPath :: String -> String -> String
getPath folder file = folder ++ "/" ++ file

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

-- the highest number of available log/index files
-- 1. list directory (log folder)
-- 2. determine the offset (int) from containing files (we filter only .log files but could be .index as well)
-- 3. return the max offset
getLastBaseOffset :: (TopicStr, Int) -> IO BaseOffset
getLastBaseOffset (t, p) = do 
  bos <- getBaseOffsets (t, p)
  return $ maximum bos

getBaseOffsets :: (TopicStr, Int) -> IO [BaseOffset]
getBaseOffsets (t, p) = do 
  dirs <- getDirectoryContents $ getLogFolder (t, p)
  return $ map (offsetFromFileName) (filter (isLogFile) (filterRootDir dirs))


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
    Right (bs, bo, ops) -> return $ last ops

-------------------------------------------------------
getFileSize :: String -> IO Integer
getFileSize path = do
    hdl <- openFile path ReadMode 
    size <- hFileSize hdl 
    hClose hdl 
    return size

lastOffset :: Log -> Offset
lastOffset = offset . last

getLastLogOffset :: (TopicStr, Int) -> BaseOffset -> OffsetPosition -> IO Offset
-- find last Offset in the log, start search from given offsetposition 
-- 1. get file Size for end of file position 
-- 2. open log file from start position given by offsetPosition to eof position 
-- 3. parse log and get highest offset
getLastLogOffset (t, p) bo (rel, phys) = do
  let path = getPath (logFolder t p) (logFile bo)
  -- check if file exists
  eof <- getFileSize path
  print $ "physical start: " ++ (show $ fromIntegral phys)
  print $ "filesize: " ++ show eof
  bs <- mmapFileByteStringLazy path Nothing -- $ Just (fromIntegral phys, fromIntegral eof)
  return $ lastOffset $ runGet getLog bs

-------------------------------------------------------


assignOffset :: Offset -> MessageSet -> MessageSet
assignOffset o ms = MessageSet o (len ms) (message ms)

continueOffset :: Offset -> [MessageSet] -> [MessageSet]
continueOffset o [] = []
continueOffset o (m:ms) = assignOffset o m : continueOffset (o + 1) ms

-------------------------------------------------------

appendLog :: (TopicStr, Int) -> BaseOffset -> [MessageSet] -> IO()
appendLog (t, p) bo ms = do
  let path = getPath (logFolder t p) (logFile bo)
  let bs = buildMessageSets ms
  print bs
  BL.appendFile path bs

readLog :: (TopicStr, Int) -> Offset -> IO [MessageSet]
readLog tp o = do
  bos <- getBaseOffsets tp 
  let bo = getBaseOffsetFor bos o
  op <- indexLookup tp bo o 
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
    Right (bs, byo, ops) -> return $ getOffsetPositionFor ops bo to 

getOffsetPositionFor :: [OffsetPosition] -> BaseOffset -> Offset -> OffsetPosition 
-- get greatest offsetPosition from list that is les than or equal target offset 
getOffsetPositionFor [] bo to = (0, 0)
getOffsetPositionFor [x] bo to = x
getOffsetPositionFor (x:xs) bo to = 
                      if ((fromIntegral $ fst $ x) + bo) <= fromIntegral to 
                      && fromIntegral to < ((fromIntegral $ fst $ head $ xs) + bo) 
                        then x 
                        else getOffsetPositionFor xs bo to 

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
getLogFrom (t, p) bo (ro, phy) = do 
  let path = getPath (logFolder t p) (logFile bo)
  fs <- getFileSize path 
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


--decodeLogFrom :: Offset -> Get MessageSet
--decodeLogFrom to = do
--  empty <- isEmpty
--  if empty
--    then return $ MessageSet 0 0 (Message 0 (Payload 0 0 0 0 BC.empty)) --TOOD: possible to work with Maybe MessageSet?
--    else do ms <- messageSetParser
--            if (offset ms) >= to 
--              then return ms
--              else decodeLogFrom to 

--getRelativeOffset :: BaseOffset -> Offset -> RelativeOffset
--getRelativeOffset bo o = fromIntegral o - fromIntegral bo
--
----appendSegment :: [MessageSet] -> 
----appendSegment = do
--    --appendIndex
--    --appendLog
--
--assignOffset :: [MessageSet] -> Offset -> [MessageSet]
--assignOffset ms o = ms
--
--
--buildIndexPath :: BaseOffset -> (TopicName, Int) -> String
--buildIndexPath bo (t, p) = buildPath bo (t, p) ".index"
--
--buildLogPath :: BaseOffset -> (TopicName, Int) -> String
--buildLogPath bo (t, p) = buildPath bo (t, p) ".log"
--
--buildPath :: BaseOffset -> (TopicName, Int) -> String -> String
--buildPath bo (t, p) ending = (show t) ++ "_" ++ (show p) ++ "/" ++ (show bo) ++ ending
--
--appendLog :: (TopicName, Int) -> [MessageSet] -> IO()
--appendLog (t, p) ms = do 
--  bo <- getLastBaseOffset (t,p)
--  let path = (buildLogPath bo (t, p))
--  op <- getLastOffsetPosition bo
--  lo <- getLastLogOffset bo (t,p) op
--  BL.appendFile 
--    path $ 
--    buildMessageSets $ assignOffset (ms) $ lo
--
--    --TODO Chaining with "." does not compile. Chaining with "$" does not work
--    --yet because of IO return Types 
--    --
--    --buildMessageSets 
--    -- . assignOffset (ms) 
--    -- . getLastLogOffset bo 
--    -- . getLastOffsetPosition bo
--    -- . getLastBaseOffset (t, p) 
--
--
----TODO: Does not compile yet
----appendIndex :: (TopicName, Partition) -> IO()
----appendIndex (t, p) = do
----  BL.appendFile buildIndexPath (t, p) $ buildOffsetPosition 
----    ((getRelativeOffset 
----        getLastBaseOffset  
----        $ getLastLogOffset(getLastBaseOffset(t,p)) 
----          . getLastOffsetPosition 
----          . getLastBaseOffset (t, p) 
----    ),
----    getFileSize buildIndexPath (t, p))
--
--buildOffsetPosition :: OffsetPosition -> BL.ByteString
--buildOffsetPosition (o, p) = runPut $ do 
--  putWord32be o
--  putWord32be p 
----readLastIndexEntry :: (TopicStr, PartitionStr) ->  IO IndexEntry
----readLastIndexEntry (topic, partition) = do 
----  let indexPath = getPath (logFolder topic partition) (indexFile 0) 
----  ex <- doesFileExist indexPath
----  if not ex 
----    then (newIndex indexPath)
----    else (newIndex indexPath)
----  return ((0,0))
--
----newIndex :: String ->  IO IndexEntry
----newIndex filepath = do 
----  let e = buildIndexEntry (0,0)
-- -- BL.writeFile filepath e
--  --return (0,0)
--


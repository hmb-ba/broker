module HMB.Internal.Log.Writer
( writeLog, readLog, getTopicNames ) where

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

type TopicStr = String 
type PartitionStr = Int

type MessageInput = (TopicStr, PartitionStr, Log)

logFolder :: TopicStr -> PartitionStr -> String
logFolder t p = "log/" ++ t ++ "_" ++ show p

logFile :: (Integral i) => i -> String
logFile o = (show $ fromIntegral o) ++ ".log"

getPath :: String -> String -> String
getPath folder file = folder ++ "/" ++ file

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

-- todo: move to reader
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

readLog :: (String, Int, Int) -> IO Log
readLog (t, p, o) = do 
  log <- readLogFromBeginning (t,p)
  return ([ x | x <- log, fromIntegral(offset x) >= o])

getTopicNames :: IO [String]
getTopicNames = (getDirectoryContents "log/")


--------------


type LogSegment = (FilemessageSet, OffsetIndex)
type FilemessageSet = [MessageSet]
type OffsetIndex = [OffsetPosition]
type OffsetPosition = (RelativeOffset, PhysicalPosition)
type RelativeOffset = Word32
type PhysicalPosition = Word32
type FileName = String

--g = appendIndex 

f = appendLog getLastSegment (getLastOffset $ (getLastSegment . getLastOffsetPosition . getLastOffset))

getLastSegment :: (Topic, Partition) -> FileName
-- get active segment

getLastOffsetPosition :: FileName -> OffsetPosition
-- get offset of last index entry

getLastOffset :: PhysicalPosition -> Offset

appendIndex :: (IndexFile, OffsetPosition) -> IO()

appendLog :: (LogFile, Offset) -> [MessageSet] -> IO()

--readLastIndexEntry :: (TopicStr, PartitionStr) ->  IO IndexEntry
--readLastIndexEntry (topic, partition) = do 
--  let indexPath = getPath (logFolder topic partition) (indexFile 0) 
--  ex <- doesFileExist indexPath
--  if not ex 
--    then (newIndex indexPath)
--    else (newIndex indexPath)
--  return ((0,0))

newIndex :: String ->  IO IndexEntry
newIndex filepath = do 
  let e = buildIndexEntry (0,0)
  BL.writeFile filepath e
  return (0,0)

buildIndexEntry :: IndexEntry -> BL.ByteString
buildIndexEntry (o, p) = runPut $ do 
  putWord32be o
  putWord32be p 

indexFile :: (Integral i) => i -> String 
indexFile o = (show $ fromIntegral o) ++ ".index"


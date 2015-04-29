module HMB.Internal.Log.Writer
( writeLog, readLog ) where

import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BL
import Data.Binary.Put
import System.Directory
import Control.Conditional
import Control.Monad
import Kafka.Protocol

import HMB.Internal.Types

import Data.Binary.Get

type TopicStr = String --TODO: better name (ambigious)
type PartitionStr = Int  -- TODO: better name (ambigious)

type MessageInput = (TopicStr, PartitionStr, Log)

logFolder :: TopicStr -> PartitionStr -> String
logFolder t p = t ++ "_" ++ show p

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
  putStrLn "here"
  createDirectoryIfMissing False $ logFolder topicName partitionNumber
  let filePath = getPath (logFolder topicName partitionNumber) (logFile 0)
  ifM (doesFileExist filePath) 
      (appendToLog filePath (topicName,partitionNumber, log)) 
      (newLog filePath (topicName,partitionNumber, log))

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

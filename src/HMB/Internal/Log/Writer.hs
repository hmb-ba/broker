module HMB.Internal.Log.Writer
( writeLog ) where

import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BL
import Data.Binary.Put
import System.Directory
import Control.Conditional

import Kafka.Protocol.Types
import Kafka.Protocol.Serializer

type TopicStr = String --TODO: better name (ambigious)
type PartitionStr = Int  -- TODO: better name (ambigious)

type MessageInput = (TopicStr, PartitionStr, Log)

logFolder :: TopicStr -> PartitionStr -> String
logFolder t p = t ++ "_" ++ show p

logFile :: (Integral i) => i -> String
logFile o = (show $ fromIntegral o) ++ ".log"

getPath :: String -> String -> String
getPath folder file = folder ++ "/" ++ file

buildLog :: Log -> BL.ByteString
buildLog [] = BL.empty
buildLog (x:xs) = 
  BL.append (buildMessageSet x) (buildLog xs) 

writeLog :: MessageInput -> IO() 
writeLog (topicName, partitionNumber, log) = do
  createDirectoryIfMissing False $ logFolder topicName partitionNumber
  let filePath = getPath (logFolder topicName partitionNumber) (logFile 0)
  ifM (doesFileExist filePath) 
    (BL.appendFile filePath $ buildLog log)
    (BL.writeFile filePath $ buildLog log)


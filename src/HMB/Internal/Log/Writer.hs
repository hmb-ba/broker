module HMB.Internal.Log.Writer
( writeLog ) where

import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BL
import Data.Binary.Put
import HMB.Common
import System.Directory
import Control.Conditional

type Topic = String
type Partition = Int

type MessageInput = (Topic, Partition, Log)

isTopic :: Topic -> Bool
isTopic s = True

isPartition :: Partition -> Bool
isPartition p = True

logFolder :: Topic -> Partition -> String
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


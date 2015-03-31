module Network.Parser
(readRequest) where 

import Common.Types
import Common.Parser
import Network.Types
import Data.Binary.Get 
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BL 

partitionParser :: Get Partition
partitionParser = do 
  partitionNumber <- getWord32be
  messageSetSize <- getWord32be
  --messageSet <- runGet getMessageSets $ getByteString $ fromIntegral messageSetSize
  messageSet <- messageSetParser
  return $! Partition partitionNumber messageSetSize messageSet

getPartitions :: Int -> Get [Partition]
getPartitions i = do
  if (i < 1)
    then return []
    else do partition <- partitionParser
            partitions <- getPartitions $ i-1
            return (partition:partitions)

topicParser :: Get Topic 
topicParser = do 
  topicNameLen <- getWord16be
  topicName <- getByteString $ fromIntegral topicNameLen
  numPartitions <- getWord32be
  partitions <- getPartitions $ fromIntegral numPartitions
  return $! Topic topicNameLen topicName numPartitions partitions

getTopics :: Int -> Get [Topic]
getTopics i = do 
  if (i < 1)
    then return []
    else do topic <- topicParser
            topics <- getTopics $ i-1
            return (topic:topics)

produceRequestParser :: Get Request
produceRequestParser = do 
  requiredAcks <- getWord16be
  timeout <- getWord32be 
  numTopics <- getWord32be
  topics <- getTopics $ fromIntegral numTopics
  return $! ProduceRequest requiredAcks timeout numTopics topics

requestMessageParser :: Get RequestMessage 
requestMessageParser = do 
  requestSize <- getWord32be
  apiKey <- getWord16be
  apiVersion <- getWord16be 
  correlationId <- getWord32be 
  clientIdLen <- getWord16be 
  clientId <- getByteString $ fromIntegral clientIdLen
  --if apiKey =  0 
   -- then
  request <- produceRequestParser
   -- else 
   --   request <- getRemainingLazyByteStringa
  return $! RequestMessage requestSize apiKey apiVersion correlationId clientIdLen clientId $ request

readRequest :: String -> IO RequestMessage
readRequest a = do 
  input <- BL.readFile a 
  return (runGet requestMessageParser input)


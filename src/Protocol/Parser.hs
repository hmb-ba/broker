module Protocol.Parser
(parseData) where 

import Protocol.Types
import Data.Binary.Get 
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BL 

messageParser :: Get Message 
messageParser = do 
  crc <- getWord32be
  magic <- getWord8
  attributes <- getWord8 
  key <- getWord32be 
  payloadLen <- getWord32be 
  payloadData <- getByteString $ fromIntegral payloadLen
  return $! Message crc magic attributes key payloadData

messageSetParser :: Get MessageSet 
messageSetParser = do 
  offset <- getWord64be
  length <- getWord32be 
  message <- messageParser
  return $! MessageSet offset length message

partitionParser :: Get Partition
partitionParser = do 
  partition <- getWord32be
  messageSetSize <- getWord32be
  messageSet <- messageSetParser
  return $! Partition partition messageSetSize messageSet

getPartitions :: Int -> Get [Partition]
getPartitions i = do
  if (i < 1)
    then return []
    else do partition <- partitionParser
            partitions <- getPartitions $ i-1
            return (partition:partitions)

topicsParser :: Get Topic 
topicsParser = do 
  topicLen <- getWord16be
  topicName <- getByteString $ fromIntegral topicLen
  numPartitions <- getWord32be
  partitions <- getPartitions $ fromIntegral numPartitions
  return $! Topic topicName partitions

getTopics :: Int -> Get [Topic]
getTopics i = do 
  if (i < 1)
    then return []
    else do topic <- topicsParser
            topics <- getTopics $ i-1
            return (topic:topics)

produceRequestParser :: Get ProduceRequest
produceRequestParser = do 
  requiredAcks <- getWord16be
  timeout <- getWord32be 
  numTopics <- getWord32be
  topics <- getTopics $ fromIntegral numTopics
  return $! ProduceRequest requiredAcks timeout topics

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
  return $! RequestMessage requestSize apiKey apiVersion correlationId    clientId $ Request request

parseData :: String -> IO RequestMessage
parseData a = do 
  input <- BL.readFile a 
  return (runGet requestMessageParser input)
  
--main:: IO()
--main = do 
--  input <- BL.readFile "payload"
--  let t = runGet requestMessageParser input
--  print t

import Data.Word 
import Data.Binary.Get 
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BL 
data RequestMessage = RequestMessage 
  { requestSize     :: !Word32
  , apiKey :: !Word16
  , apiVersion :: !Word16
  , correlationId :: !Word32
  , clientIdLen :: !Word16
  , clientId :: BS.ByteString
  , request :: Request 
  } deriving (Show)

data Request = Request ProduceRequest deriving (Show)

data ProduceRequest = ProduceRequest
  { requiredAcks :: !Word16
  , timeout :: !Word32 
  , topics :: [Topic]
  } deriving (Show)

data Topic = Topic 
  { topicLen :: !Word16
  , topicName :: BS.ByteString
  , numPartitions :: !Word32
  , partitions :: [Partition]
  } deriving (Show)

data Partition = Partition 
  { partition :: !Word32
  , messageSetSize :: !Word32
  , messageSet :: MessageSet
} deriving (Show)

data MessageSet = MessageSet 
  { offset :: !Word64
  , messageSize :: !Word32
  , message :: Message 
  } deriving (Show) 

data Message = Message 
  { crc :: !Word32
  , magicByte :: !Word8
  , attributes :: !Word8
  , key :: !Word32
  --,  :: !Word16
  --, key :: BS.ByteString
  , value :: BS.ByteString
  } deriving (Show)

messageParser :: Get Message 
messageParser = do 
  crc <- getWord32be
  magicByte <- getWord8
  attributes <- getWord8 
  key <- getWord32be 
  valueLen <- getWord32be 
  value <- getByteString $ fromIntegral valueLen
  return $! Message crc magicByte attributes key value

messageSetParser :: Get MessageSet 
messageSetParser = do 
  offset <- getWord64be
  messageSize <- getWord32be 
  message <- messageParser
  return $! MessageSet offset messageSize message

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
  return $! Topic topicLen topicName numPartitions partitions

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
  return $! RequestMessage requestSize apiKey apiVersion correlationId clientIdLen clientId $ Request request

main :: IO()
main = do 
  input <- BL.readFile "payload"
  let t = runGet requestMessageParser input
  print t

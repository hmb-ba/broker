module Protocol.Types
( RequestMessage (..)
, RequestSize
, ApiKey
, ApiVersion
, CorrelationId 
, ClientId 
, Request (..)
--, ProduceRequest
, RequiredAcks
, Timeout
, NumTopics
, Topic (..)
, TopicName
, TopicNameLen
, NumPartitions
, Partition (..)
, PartitionNumber
, MessageSet (..)
, MessageSetSize
, Message (..)
, Offset
, Crc
, Len
, Magic
, Attributes
, Key
, PayloadLen
, PayloadData 
)where 

import Data.Word
import qualified Data.ByteString as BS

type RequestSize = Word32
type ApiKey = Word16 
type ApiVersion = Word16 
type CorrelationId = Word32
type ClientId = BS.ByteString
type ClientIdLen = Word16

data RequestMessage = RequestMessage 
  { requestSize     :: !RequestSize
  , apiKey :: !ApiKey
  , apiVersion :: !ApiVersion
  , correlationId :: !CorrelationId
  , clientIdLen :: !ClientIdLen
  , clientId :: !ClientId
  , request :: Request 
} deriving (Show)

--data Request = ProduceRequest deriving (Show)

type RequiredAcks = Word16 
type Timeout = Word32 
type NumTopics = Word32

data Request = ProduceRequest
  { requiredAcks :: !RequiredAcks
  , timeout :: !Timeout 
  , numTopics :: !NumTopics
  , topics :: [Topic]
  }
  | MetainfoRequest 
  { topicNames :: [TopicName] } deriving (Show)

type TopicName = BS.ByteString
type TopicNameLen = Word16
type NumPartitions = Word32

data Topic = Topic 
  { topicNameLen :: !TopicNameLen
  , topicName :: !TopicName
  , numPartitions :: !NumPartitions
  , partitions :: [Partition]
} deriving (Show)

type PartitionNumber = Word32
type MessageSetSize = Word32

data Partition = Partition 
  { partitionNumber :: !PartitionNumber
  , messageSetSize :: !MessageSetSize
  , messageSet :: MessageSet
} deriving (Show)

type Offset = Word64
type Len = Word32

data MessageSet = MessageSet 
  { offset :: !Offset
  , len :: !Len
  , message :: Message 
} deriving (Show) 

type Crc = Word32
type Magic = Word8
type Attributes = Word8 
type Key = Word32 
type PayloadLen =  Word32
type PayloadData = BS.ByteString

data Message = Message 
  { crc :: !Crc
  , magic :: !Magic
  , attributes :: !Attributes
  , key :: !Key
  --,  :: !Word16
  --, key :: BS.ByteString
  , payloadLen :: !PayloadLen
  , payloadData :: !PayloadData
  } deriving (Show)






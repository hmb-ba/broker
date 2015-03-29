module Protocol.Types
( RequestMessage (..)
, RequestSize
, ApiKey
, ApiVersion
, CorrelationId 
, ClientId 
, Request (..)
, ProduceRequest (..)
, RequiredAcks
, Timeout
, Topic (..)
, TopicName
, Partition (..)
, PartitionNumber
, MessageSet (..)
, MessageSetSize
, Message (..)
, Offset
, Crc
, Magic
, Attributes
, Key
, PayloadData 
)where 

import Data.Word
import qualified Data.ByteString as BS

type RequestSize = Word32
type ApiKey = Word16 
type ApiVersion = Word16 
type CorrelationId = Word32
type ClientId = BS.ByteString

data RequestMessage = RequestMessage 
  { requestSize     :: !RequestSize
  , apiKey :: !ApiKey
  , apiVersion :: !ApiVersion
  , correlationId :: !CorrelationId
  , clientId :: !ClientId
  , request :: Request 
} deriving (Show)

data Request = Request ProduceRequest deriving (Show)

type RequiredAcks = Word16 
type Timeout = Word32 

data ProduceRequest = ProduceRequest
  { requiredAcks :: !RequiredAcks
  , timeout :: !Timeout 
  , topics :: [Topic]
} deriving (Show)

type TopicName = BS.ByteString

data Topic = Topic 
  { topicName :: !TopicName
  , partitions :: [Partition]
} deriving (Show)

type PartitionNumber = Word32
type MessageSetSize = Word32

data Partition = Partition 
  { partition :: !PartitionNumber
  , messageSetSize :: !MessageSetSize
  , messageSet :: MessageSet
} deriving (Show)

type Offset = Word64
type Length = Word32

data MessageSet = MessageSet 
  { offset :: !Offset
  , length :: !Length
  , message :: Message 
} deriving (Show) 

type Crc = Word32
type Magic = Word8
type Attributes = Word8 
type Key = Word32 
type PayloadData = BS.ByteString

data Message = Message 
  { crc :: !Crc
  , magic :: !Magic
  , attributes :: !Attributes
  , key :: !Key
  --,  :: !Word16
  --, key :: BS.ByteString
  , payloadData :: !PayloadData
  } deriving (Show)






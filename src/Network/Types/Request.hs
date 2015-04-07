module Network.Types.Request
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
)where

import Data.Word
import qualified Data.ByteString as BS
import Common.Types

type RequestSize = Word32
type ApiKey = Word16
type ApiVersion = Word16
type CorrelationId = Word32
type ClientId = BS.ByteString
type ClientIdLen = Word16

type RequiredAcks = Word16
type Timeout = Word32
type NumTopics = Word32

type TopicName = BS.ByteString
type TopicNameLen = Word16
type NumPartitions = Word32

type PartitionNumber = Word32
type MessageSetSize = Word32

--type Offset = Word64
--type Len = Word32

data RequestMessage = RequestMessage
  { requestSize     :: !RequestSize
  , apiKey          :: !ApiKey
  , apiVersion      :: !ApiVersion
  , correlationId   :: !CorrelationId
  , clientIdLen     :: !ClientIdLen
  , clientId        :: !ClientId
  , request         :: Request
  } deriving (Show)

data Request = ProduceRequest
  { requiredAcks    :: !RequiredAcks
  , timeout         :: !Timeout
  , numTopics       :: !NumTopics
  , topics          :: [Topic]
  }
  | MetainfoRequesta
  { topicNames      :: [TopicName] }
  deriving (Show)

data Topic = Topic
  { topicNameLen    :: !TopicNameLen
  , topicName       :: !TopicName
  , numPartitions   :: !NumPartitions
  , partitions      :: [Partition]
  } deriving (Show)

data Partition = Partition
  { partitionNumber :: !PartitionNumber
  , messageSetSize  :: !MessageSetSize
  , messageSet      :: [MessageSet]
  } deriving (Show)



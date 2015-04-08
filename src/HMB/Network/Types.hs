module HMB.Network.Types
( RequestMessage (..)
, RequestSize
, ApiKey
, ApiVersion
, CorrelationId
, ClientId
, Request (..)
, RequiredAcks
, Timeout
--, NumTopics
, Topic (..)
, TopicName
, TopicNameLen
--, NumPartitions
, Partition (..)
, PartitionNumber
, Response (..)
, ResponseMessage (..)
, ProResPayload (..)
, FetResPayload
) where

import Data.Word
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BL
import HMB.Common


type ListLength = Word32

type RequestSize = Word32
type ApiKey = Word16
type ApiVersion = Word16
type CorrelationId = Word32
type ClientId = BS.ByteString
type ClientIdLen = Word16
type RequiredAcks = Word16
type Timeout = Word32
--type NumTopics = Word32
type TopicName = BS.ByteString
type TopicNameLen = Word16
--type NumPartitions = Word32
type PartitionNumber = Word32
type MessageSetSize = Word32

type ErrorCode = Word16
--type NumResponses = Word32
--type NumErrors = Word32
type HightwaterMarkOffset = Word64

------------
-- Resquest
------------

data RequestMessage = RequestMessage
  { reqSize     :: !RequestSize
  , reqApiKey          :: !ApiKey
  , reqApiVersion      :: !ApiVersion
  , reqCorrelationId   :: !CorrelationId
  , reqClientIdLen     :: !ClientIdLen
  , reqClientId        :: !ClientId
  , request            :: Request
  } deriving (Show)

data Request = ProduceRequest
  { reqRequiredAcks    :: !RequiredAcks
  , reqTimeout         :: !Timeout
  , reqNumTopics       :: !ListLength
  , reqTopics          :: [Topic]
  }
  | MetadataRequest
  { reqTopicNames      :: [TopicName] }
  deriving (Show)

data Topic = Topic
  { topicNameLen    :: !TopicNameLen
  , topicName       :: !TopicName
  , numPartitions   :: !ListLength
  , partitions      :: [Partition]
  } deriving (Show)

data Partition = Partition
  { partitionNumber :: !PartitionNumber
  , messageSetSize  :: !MessageSetSize
  , messageSet      :: [MessageSet]
  } deriving (Show)


------------
-- Response
------------

data ResponseMessage = ResponseMessage
  { resCorrelationId   :: !CorrelationId
  , resNumResponses    :: !ListLength
  , responses        :: [Response]
  } deriving (Show)

data Response = ProduceResponse
  { proTopicNameLen    :: !TopicNameLen
  , proTopicName       :: !TopicName 
  , proNumErrors       :: !ListLength
  , proErrors          :: [ProResPayload]
  }
  | MetadataResponse 
  { resTopicNameLen    :: !TopicNameLen
  , resTopicName       :: !TopicName 
  } 
  | FetchResponse 
  { fetNumFetchs       :: !ListLength
  , fetFetchs          :: [Fetch]
  } deriving (Show) 

data ProResPayload = ProResPayload
  { proPartitionNumber :: !PartitionNumber
  , proErrCode       :: !ErrorCode
  , proErrOffset          :: !Offset
  } deriving (Show)

data Fetch = Fetch
  { fetTopicNameLen     :: !TopicNameLen
  , fetNumsPayloads     :: !ListLength
  , fetPayloads         :: [FetResPayload]
  } deriving (Show) 

data FetResPayload = FetResPayload 
  { fetPartitionNumber  :: !PartitionNumber
  , fetErrorCode        :: !ErrorCode 
  , fetHighwaterMarkOffset :: !HightwaterMarkOffset
  , fetMessageSetSize   :: !MessageSetSize
  , fetMessageSet       :: !MessageSet 
  } deriving (Show)


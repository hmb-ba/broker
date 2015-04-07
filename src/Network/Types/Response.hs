module Network.Types.Response
( Response (..)
, ResponseMessage (..)
, Error (..)
)where

import Data.Word
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BL
--import Common.Types

type CorrelationId = Word32
type TopicNameLen = Word16
type TopicName = BS.ByteString

type PartitionNumber = Word32
type ErrorCode = Word16
type Offset = Word64 
type NumResponses = Word32
type NumErrors = Word32

data ResponseMessage = ResponseMessage
  { correlationId   :: !CorrelationId
  , numResponses    :: !NumResponses
  , responses        :: [Response]
  } deriving (Show)

data Response = ProduceResponse
  { topicNameLen    :: !TopicNameLen
  , topicName       :: !TopicName 
  , numErrors       :: !NumErrors
  , errors          :: [Error]
  }
  | MetadataResponse 
  { topicNameLen    :: !TopicNameLen
  , topicName       :: !TopicName 
  } deriving (Show) 

data Error = Error 
  { partitionNumber :: !PartitionNumber
  , errorCode       :: !ErrorCode
  , offset          :: !Offset
  } deriving (Show)


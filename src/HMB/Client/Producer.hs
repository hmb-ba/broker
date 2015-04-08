module HMB.Client.Producer
(  packRequest
 , sendRequest
 , readProduceResponse
 , InputMessage (..)
)
where 

import Network.Socket

import HMB.Common
import HMB.Protocol

import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BL

import Data.Digest.CRC32
import qualified Network.Socket.ByteString.Lazy as SBL

import Data.Binary.Get 

data InputMessage = InputMessage
  { inputClientId        :: !ClientId
  , inputTopicName       :: !TopicName
  , inputPartitionNumber :: !PartitionNumber
  , inputData            :: !BS.ByteString
}

packRequest :: InputMessage -> RequestMessage
packRequest iM = 
  let payload = Payload {
      keylen = 0
    , magic = 0
    , attr= 0
    , payloadLen = fromIntegral $ BS.length $ inputData iM 
    , payloadData = inputData iM
  }
  in
  let message = Message { 
      crc = crc32 $ buildPayload payload
    , payload = payload
  }
  in
  let messageSet = MessageSet {
      offset = 0
    , len = fromIntegral $ BL.length $ buildMessage message
    , message = message
  }
  in
  let partition = Partition {
      partitionNumber = inputPartitionNumber iM
    , messageSetSize = fromIntegral $ BL.length $ buildMessageSet messageSet
    , messageSet = [messageSet]
  }
  in
  let topic = Topic {
      topicNameLen = fromIntegral $ BS.length $ inputTopicName iM
    , topicName = inputTopicName iM
    , numPartitions = fromIntegral $ length [partition]
    , partitions = [partition]
  }
  in
  let request = ProduceRequest {
      reqRequiredAcks = 0
    , reqTimeout = 1500
    , reqNumTopics = fromIntegral $ length [topic]
    , reqTopics = [topic]
  }
  in
  let requestMessage = RequestMessage {
      reqSize = fromIntegral $ (BL.length $ buildProduceRequestMessage request )
          + 2 -- reqApiKey
          + 2 -- reqApiVersion
          + 4 -- correlationId 
          + 2 -- clientIdLen
          + (fromIntegral $ BS.length $ inputClientId iM) --clientId
    , reqApiKey = 0
    , reqApiVersion = 0
    , reqCorrelationId = 0
    , reqClientIdLen = fromIntegral $ BS.length $ inputClientId iM
    , reqClientId = inputClientId iM
    , request = request
  }
  in
  requestMessage

sendRequest :: Socket -> RequestMessage -> IO() 
sendRequest socket requestMessage = do 
  let msg = buildRequestMessage requestMessage
  SBL.sendAll socket msg

readProduceResponse :: BL.ByteString -> IO ResponseMessage
readProduceResponse a =  do 
  return (runGet produceResponseMessageParser a)


module Network.Writer.Request 
(  writeRequest 
 , buildProduceRequestMessage
 , buildRequestMessage
) where 

--import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BL
import Network.Socket
import qualified Network.Socket.ByteString.Lazy as SBL
import Data.Binary.Put
import Common.Writer
import Common.Types
import Network.Types.Request
import System.IO

buildMessageSets :: [MessageSet] -> BL.ByteString
buildMessageSets [] = BL.empty
buildMessageSets (x:xs) = BL.append (buildMessageSet x) (buildMessageSets xs)

buildPartition :: Partition -> BL.ByteString
buildPartition e = runPut $ do 
  putWord32be $ partitionNumber e
  putWord32be $ messageSetSize e
  putLazyByteString $ buildMessageSets $  messageSet e

buildPartitions :: [Partition] -> BL.ByteString
buildPartitions [] = BL.empty
buildPartitions (x:xs) = BL.append (buildPartition x) (buildPartitions xs) 

buildTopic :: Topic -> BL.ByteString 
buildTopic e = runPut $  do 
  putWord16be $ topicNameLen e 
  putByteString $ topicName e
  putWord32be $ numPartitions e 
  putLazyByteString $ buildPartitions $ partitions e 

buildTopics :: [Topic] -> BL.ByteString
buildTopics [] = BL.empty 
buildTopics (x:xs) = BL.append (buildTopic x) (buildTopics xs)

buildProduceRequestMessage :: Request -> BL.ByteString
buildProduceRequestMessage e = runPut $ do 
  putWord16be $ requiredAcks e
  putWord32be $ timeout e 
  putWord32be $ numTopics e 
  putLazyByteString $ buildTopics $ topics e

buildRequestMessage :: RequestMessage -> BL.ByteString
buildRequestMessage e = runPut $ do 
  putWord32be $ requestSize e
  putWord16be $ apiKey e 
  putWord16be $ apiVersion e 
  putWord32be $ correlationId e 
  putWord16be $ clientIdLen e 
  putByteString $ clientId e 
  putLazyByteString $ buildProduceRequestMessage $ request e

writeRequest :: Socket -> RequestMessage -> IO() --TODO: better name
--writeLog topics partitions messageset  = do 
writeRequest socket requestMessage = do 
  let msg = buildRequestMessage requestMessage
  SBL.sendAll socket msg
 -- return ()





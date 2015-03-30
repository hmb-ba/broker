module Protocol.Writer 
( writeProtocol ) where 

--import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BL
import Data.Binary.Put
import Protocol.Types
--import Network.Socket
import System.IO

buildMessage :: Message -> BL.ByteString
buildMessage e = runPut $ do 
  putWord32be $ crc e 
  putWord8 $ magic e 
  putWord8 $ attributes e 
  putWord32be $ key e
  putWord32be $ payloadLen e
  putByteString $ payloadData e 

buildMessageSet :: MessageSet -> BL.ByteString
buildMessageSet e = runPut $ do 
  putWord64be $ offset e
  putWord32be $ len e
  putLazyByteString $ buildMessage $ message e

buildPartition :: Partition -> BL.ByteString
buildPartition e = runPut $ do 
  putWord32be $ partitionNumber e
  putWord32be $ messageSetSize e
  putLazyByteString $ buildMessageSet $  messageSet e

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

writeProtocol :: Handle -> RequestMessage -> IO()
--writeLog topics partitions messageset  = do 
writeProtocol handle requestMessage = do 
  BL.hPut handle (buildRequestMessage requestMessage)




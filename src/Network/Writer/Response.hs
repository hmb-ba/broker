module Network.Writer.Response
( sendProduceResponse 
) where 

import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BL
import qualified Network.Socket.ByteString.Lazy as SBL
import Network.Socket
import Data.Binary.Put

--HMB
import Network.Types.Response

buildError :: Error -> BL.ByteString 
buildError e = runPut $ do 
  putWord32be $ partitionNumber e
  putWord16be $ errorCode e 
  putWord64be $ offset e 

buildErrors :: [Error] -> BL.ByteString
buildErrors [] = BL.empty
buildErrors (x:xs) = BL.append (buildError x) (buildErrors xs)

buildProduceResponse :: Response -> BL.ByteString
buildProduceResponse e = runPut $ do 
  putWord16be $ topicNameLen e 
  putByteString $ topicName e
  putWord32be $ numErrors e 
  putLazyByteString $ buildErrors $ errors e

buildProduceResponses :: [Response] -> BL.ByteString
buildProduceResponses [] = BL.empty 
buildProduceResponses (x:xs) = BL.append (buildProduceResponse x) (buildProduceResponses xs)

buildProduceResponseMessage :: ResponseMessage -> BL.ByteString
buildProduceResponseMessage e = runPut $ do 
  putWord32be $ correlationId e 
  putWord32be $ numResponses e 
  putLazyByteString $ buildProduceResponses $ responses e 

sendProduceResponse :: Socket -> ResponseMessage -> IO() 
sendProduceResponse socket responsemessage = do 
  let msg = buildProduceResponseMessage responsemessage
  SBL.sendAll socket msg

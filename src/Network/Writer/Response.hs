module Network.Writer.Response
( sendResponse 
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

buildResponse :: Response -> BL.ByteString
buildResponse e = runPut $ do 
  putWord16be $ topicNameLen e 
  putByteString $ topicName e
  putLazyByteString $ buildErrors $ errors e

buildResponses :: [Response] -> BL.ByteString
buildResponses [] = BL.empty 
buildResponses (x:xs) = BL.append (buildResponse x) (buildResponses xs)

buildResponseMessage :: ResponseMessage -> BL.ByteString
buildResponseMessage e = runPut $ do 
  putWord32be $ correlationId e 
  putLazyByteString $ buildResponses $ responses e 

sendResponse :: Socket -> ResponseMessage -> IO() 
sendResponse socket responsemessage = do 
  let msg = buildResponseMessage responsemessage
  SBL.sendAll socket msg 

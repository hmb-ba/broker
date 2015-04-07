module HMB.Common.Writer 
(
buildMessageSet,
buildMessage
) 
where

import qualified Data.ByteString.Lazy as BL
import Data.Binary.Put
import HMB.Common.Types

buildMessageSet :: MessageSet -> BL.ByteString
buildMessageSet e = runPut $ do 
  putWord64be $ offset e
  putWord32be $ len e
  putLazyByteString $ buildMessage $ message e

buildMessage :: Message -> BL.ByteString
buildMessage e = runPut $ do 
  putWord32be $ crc e
  putWord8    $ magic e
  putWord8    $ attr e
  putWord32be $ keylen $ payload e
  putWord32be $ payloadLen $ payload e
  putByteString $ payloadData $ payload e


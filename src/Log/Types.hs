module Log.Types
( LogEntry (..)
, Payload (..)
, Log
) where

import Data.Word
import qualified Data.ByteString as BS

data Payload = Payload
  { keylen      :: !Word32
  --todo: key
  , payloadLen  :: !Word32
  , payloadData :: BS.ByteString
  } deriving (Show)

data LogEntry = LogEntry
  { offset  :: !Word64
  , len     :: !Word32
  , crc     :: !Word32
  , magic   :: !Word8
  , attr    :: !Word8
  , payload :: Payload
  } deriving (Show)

type Log = [LogEntry]



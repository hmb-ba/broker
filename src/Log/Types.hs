module Log.Types
( LogEntry (..)
, Payload (..)
, Log
, PayloadData
, Offset
, Length
, Crc
, Magic
, Attribute
, KeyLength
, PayloadLength
) where

import Data.Word
import qualified Data.ByteString as BS

type PayloadData = BS.ByteString
type Offset = Word64
type Length = Word32
type Crc = Word32
type Magic = Word8
type Attribute = Word8
type KeyLength = Word32
type PayloadLength = Word32

data Payload = Payload
  { keylen      :: !KeyLength
  --todo: key
  , payloadLen  :: !PayloadLength
  , payloadData :: !PayloadData
  } deriving (Show)

data LogEntry = LogEntry
  { offset  :: !Offset
  , len     :: !Length
  , crc     :: !Crc
  , magic   :: !Magic
  , attr    :: !Attribute
  , payload :: Payload
  } deriving (Show)

type Log = [LogEntry]



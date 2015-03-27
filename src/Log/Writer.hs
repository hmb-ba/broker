module Log.Writer
( writeLog ) where
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BL
import Data.Binary.Put
import Log.Types

type Topic = String
type Partition = Int

isTopic :: Topic -> Bool
isTopic s = True

isPartition :: Partition -> Bool
isPartition p = True

logFolder :: Topic -> Partition -> String
logFolder t p = t ++ "_" ++ show p

logFile :: (Integral i) => i -> String
logFile o = (show $ fromIntegral o) ++ ".log"

getPath :: String -> String -> String
getPath folder file = folder ++ "/" ++ file

buildLogEntry :: LogEntry -> BL.ByteString
buildLogEntry e = runPut $ do 
  putWord64be $ offset e
  putWord32be $ len e
  putWord32be $ crc e
  putWord8    $ magic e
  putWord8    $ attr e
  putWord32be $ keylen $ payload e
  putWord32be $ payloadLen $ payload e
  putByteString $ payloadData $ payload e

buildLog :: Log -> BL.ByteString
buildLog [] = BL.empty
buildLog (x:xs) = 
  BL.append (buildLogEntry x) (buildLog xs)

writeLog :: Topic -> Partition -> Int -> Log -> IO ()
writeLog topic partition fileOffset log = do
  let path = getPath (logFolder topic partition) (logFile fileOffset)
  BL.writeFile path $ buildLog log


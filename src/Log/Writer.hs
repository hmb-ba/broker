module Log.Writer
( writeLogEntry ) where
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

logFile :: Offset -> String
logFile o = (show $ fromIntegral o) ++ ".log"

getPath :: String -> String -> String
getPath folder file = folder ++ file

writeLogEntry :: String -> Int -> LogEntry -> IO ()
writeLogEntry topic partition logEntry = do
  let path = getPath (logFolder topic partition) (logFile $ offset logEntry)
  BS.writeFile path $ payloadData $ payload logEntry

--writeLog :: Put Log

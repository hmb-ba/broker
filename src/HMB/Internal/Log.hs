-- FIXME (meiersi): more comment would be helpful :-)
module HMB.Internal.Log
(-- writeLog
 readLog
, getTopicNames
, getLastBaseOffset
, getLastOffsetPosition
, getLastLogOffset
, continueOffset
, appendLog
, appendLogSafe
, logData
) where

import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Lazy.Char8 as BCL

import Data.Binary.Put
import System.Directory
import Control.Conditional
import Control.Monad
import Kafka.Protocol

import Data.Binary.Get
import Data.Binary.Put 

import Data.Word
import Control.Applicative

import System.IO.MMap
import System.IO
--import System.Posix.Types --for file size 
--import qualified System.Posix.Files as Files
import Data.List
--import qualified Data.Text as Text -- for isInfixOf
import Text.Printf

import qualified Control.Monad.Trans.Resource as R

----------------------------------------------------------
-- Log Writer (old)
----------------------------------------------------------

type MessageInput = (TopicStr, PartitionStr, Log)
type PartitionStr = Int


--buildLog :: Offset -> Log -> BL.ByteString
--buildLog o [] = BL.empty
--buildLog o (x:xs) =
--  (BL.append (buildLogEntry x o) (buildLog (o + 1) xs))

--writeLog :: MessageInput -> IO()
--writeLog (topicName, partitionNumber, log) = do
--  createDirectoryIfMissing False $ logFolder topicName partitionNumber
--  let filePath = getPath (logFolder topicName partitionNumber) (logFile 0)
--  ifM (doesFileExist filePath) 
--      (appendToLog filePath (topicName,partitionNumber, log)) 
--      (newLog filePath (topicName,partitionNumber, log))

--writeLogOrFail :: MessageInput -> IO(Either String ())
--writeLogOrFail input = do
-- r <- tryIOError(writeLog input)
-- case r of 
--    Left e -> return $ Left $ show e 
--    Right io -> return $ Right io 

maxOffset :: [Offset] -> Offset
maxOffset [] = 0
maxOffset [x] = x
maxOffset (x:xs) = max x (maxOffset xs)


--appendToLog :: String -> MessageInput -> IO() 
--appendToLog filepath (t, p, log)  = do 
--  o <- getMaxOffsetOfLog (t, p, log)
--  print o  --TODO: is needed for preventing file lock ...
--  let l =  buildLog (o + 1) log
--  BL.appendFile filepath l
--  return ()

--newLog :: String -> MessageInput -> IO()
--newLog filepath (t, p, log) = do 
--  let l = buildLog 0 log
--  BL.writeFile filepath l
--  return ()


getMaxOffsetOfLog :: MessageInput -> IO Offset
getMaxOffsetOfLog (t, p, _) = do 
  log <- readLogFromBeginning (t,p) --TODO: optimieren, dass nich gesamter log gelesen werden muss 
  return (maxOffset $ [ offset x | x <- log ])

getLog :: Get Log
getLog = do
  empty <- isEmpty
  if empty
      then return []
      else do messageSet <- messageSetParser
              messageSets <- getLog
              return (messageSet:messageSets)

parseLog :: String -> IO Log
parseLog a = do
  input <- BL.readFile a
  return (runGet getLog input)

readLogFromBeginning :: (String, Int) -> IO Log
readLogFromBeginning (t, p) = parseLog $ 
    getPath (logFolder t p) (logFile 0)

readLog' :: (String, Int, Int) -> IO Log
readLog' (t, p, o) = do 
  log <- readLogFromBeginning (t,p)
  return ([ x | x <- log, fromIntegral(offset x) >= o])

getTopicNames :: IO [String]
getTopicNames = (getDirectoryContents "log/")



----------------------------------------------------------
-- Log Writer
----------------------------------------------------------

type TopicStr = String 
type PartitionNr = Int

type LogSegment = (FilemessageSet, OffsetIndex)
type FilemessageSet = [MessageSet]
type OffsetIndex = [OffsetPosition]
type OffsetPosition = (RelativeOffset, FileOffset)
type RelativeOffset = Word32
type FileOffset = Word32
type BaseOffset = Int --Word64


logFolder :: TopicStr -> PartitionNr -> String
logFolder t p = "log/" ++ t ++ "_" ++ show p

leadingZero :: Int -> String
leadingZero = printf "%020d"

logFile :: Int -> String
logFile o = (leadingZero o) ++ ".log"

indexFile :: Int -> String
indexFile o = (leadingZero o) ++ ".index"

getPath :: String -> String -> String
getPath folder file = folder ++ "/" ++ file

lastIndex :: [OffsetPosition] -> OffsetPosition
lastIndex [] = (0,0)
lastIndex xs = last xs


----------------------------------------------------------


offsetFromFileName :: [Char] -> Int
offsetFromFileName = read . reverse . snd . splitAt 4 . reverse

isLogFile :: [Char] -> Bool
isLogFile x = ".log" `isInfixOf` x

isDirectory :: [Char] -> Bool
isDirectory x = elem x [".", ".."]

filterRootDir :: [String] -> [String]
filterRootDir d = filter (\x -> not $ isDirectory x) d

getLogFolder :: (TopicStr, Int) -> String
getLogFolder (t, p) = "log/" ++ t ++ "_" ++ show p

maxOffset' :: [Int] -> Int
maxOffset' [] = 0
maxOffset' [x] = x
maxOffset' xs = maximum xs

-- the highest number of available log/index files
-- 1. list directory (log folder)
-- 2. determine the offset (int) from containing files (we filter only .log files but could be .index as well)
-- 3. return the max offset
getLastBaseOffset :: (TopicStr, Int) -> IO BaseOffset
getLastBaseOffset (t, p) = do 
  bos <- getBaseOffsets (t, p)
  return $ maxOffset' bos

getBaseOffsets :: (TopicStr, Int) -> IO [BaseOffset]
getBaseOffsets (t, p) = do 
  dirs <- getDirectoryContents $ getLogFolder (t, p)
  return $ map (offsetFromFileName) (filter (isLogFile) (filterRootDir dirs))


-------------------------------------------------------


-- decode as long as physical position != 0 which means last index has passed
decodeIndexEntry :: Get [OffsetPosition]
decodeIndexEntry = do
  empty <- isEmpty
  if empty
    then return []
    else do rel  <- getWord32be
            phys <- getWord32be
            case phys of
              0 -> return $ (rel, phys)  : []
              _ -> do 
                    e <- decodeIndexEntry
                    return $ (rel, phys) : e

decodeIndex :: BL.ByteString -> Either (BL.ByteString, ByteOffset, String) (BL.ByteString, ByteOffset, [OffsetPosition])
decodeIndex = runGetOrFail decodeIndexEntry

getLastOffsetPosition :: (TopicStr, Int) -> BaseOffset -> IO OffsetPosition
-- get offset of last index entry
-- 1. open file to bs
-- 2. parse as long as not 0
-- 3. read offset/physical from last element
getLastOffsetPosition (t, p) bo = do 
  let path = getPath (getLogFolder (t, p)) (indexFile bo)
  -- check if file exists
  bs <- mmapFileByteStringLazy path Nothing
  case decodeIndex bs of
    Left (bs, bo, e)   -> do
        print e
        return $ (0,0) --todo: error handling
    Right (bs, bo, ops) -> return $ lastIndex ops

getLastOffsetPosition' :: BL.ByteString -> OffsetPosition
getLastOffsetPosition' bs = 
  case decodeIndex bs of
    Left (bs, bo, e) -> (0,0)
    Right (bs, bo, ops) -> lastIndex ops


-------------------------------------------------------


getFileSize :: String -> IO Integer
getFileSize path = do
  size <- withFile path ReadMode (\hdl -> hFileSize hdl)
--  hdl <- openFile path ReadMode
--  size <- hFileSize hdl
--  print size
  return size

lastOffset :: Log -> Maybe Offset
lastOffset [] = Nothing
lastOffset xs = Just $ (offset . last) xs

getLastLogOffset :: (TopicStr, Int) -> BaseOffset -> OffsetPosition -> IO Offset
-- find last Offset in the log, start search from given offsetposition 
-- 1. get file Size for end of file position 
-- 2. open log file from start position given by offsetPosition to eof position 
-- 3. parse log and get highest offset
getLastLogOffset (t, p) bo (rel, phys) = do
  let path = getPath (logFolder t p) (logFile bo)
  -- check if file exists
  fs <- getFileSize path
  --print $ "physical start: " ++ (show $ fromIntegral phys)
  --print $ "filesize: " ++ show fs

  -- FIXME (meiersi): this will leak resources! I suggest to read up on
  -- 'ResourceT' to handle that properly
  -- <https://hackage.haskell.org/package/resourcet>.
--  bs <- mmapFileByteStringLazy path $ Nothing -- Just (fromIntegral phys, (fromIntegral (fs) - fromIntegral phys))
  
  hdl <- openFile path ReadMode
  bs <- BL.hGetContents hdl

  case lastOffset $ runGet getLog bs of
    Nothing -> return 0
    Just lo -> return lo

getLastLogOffset' :: BL.ByteString -> Maybe Offset
getLastLogOffset' bs = lastOffset $ runGet getLog bs

-------------------------------------------------------


assignOffset :: Offset -> MessageSet -> MessageSet
assignOffset o ms = MessageSet o (len ms) (message ms)

-- increment offset over every provided messageset based on a given offset (typically last log offset)
continueOffset :: Offset -> [MessageSet] -> [MessageSet]
continueOffset o [] = []
continueOffset o (m:ms) = assignOffset o m : continueOffset (o + 1) ms

-------------------------------------------------------


partitionsOfTopic :: RqTopic -> (TopicStr, [Partition])
partitionsOfTopic t = (topic, ps)
  where
    topic = BC.unpack $ rqTopicName t
    ps = partitions t

setsOfPartitions :: (TopicStr, [Partition]) -> [(TopicStr, Int, [MessageSet])]
setsOfPartitions (tn, ps) = map (partitionSet tn) ps

partitionSet :: TopicStr -> Partition -> (TopicStr, Int, [MessageSet])
partitionSet tn p = (tn, pn, ms)
  where
    pn = fromIntegral $ rqPrPartitionNumber p
    ms = rqPrMessageSet p

logData :: Request -> [(TopicStr, Int, [MessageSet])]
logData r = concat $ map (setsOfPartitions . partitionsOfTopic) (rqPrTopics r)

nextOffset :: Offset -> Offset
nextOffset o = o + 1

appendLog :: (TopicStr, Int, [MessageSet]) -> IO()
appendLog (t, p, ms) = do 
  -- (1)
  bo  <- getLastBaseOffset (t, p)
  let logPath = getPath (logFolder t p) (logFile bo)
  let indexPath = getPath (logFolder t p) (indexFile bo)

  someindexreturn <- withFile indexPath ReadWriteMode $ \ihdl -> do

    -- (2)
      ibs <- BL.hGetContents ihdl
      let lop = getLastOffsetPosition' ibs
      let phys = fromIntegral $ snd lop
      --print $ "last index: " ++ (show lop)


      somelogreturn <- withFile logPath ReadWriteMode $ \lhdl -> do
            fs <- hFileSize lhdl
            --print $ "filesize log:" ++ (show fs)
            --bs <- mmapFileByteStringLazy path $ Just (fromIntegral phys, (fromIntegral (fs) - fromIntegral phys))

            lbs <- case fs of
              0 -> return BL.empty
              _ -> do
                  --hSeek lhdl AbsoluteSeek phys
                  lbs <- BL.hGet lhdl (fromInteger fs)
                  return lbs
            --print $ "log bytes: " ++ (show lbs)

            -- (3)
            case getLastLogOffset' lbs of
                Nothing -> do
                      --print "last log offset: empty"
                      let bs = runPut $ buildMessageSets $ continueOffset (nextOffset 0) ms
                      BL.hPut lhdl bs
                      return (fs, 0)
                Just llo -> do
                      --print $ "last log offset: " ++ (show llo)
                      let bs = runPut $ buildMessageSets $ continueOffset (nextOffset llo) ms
                      hSeek lhdl SeekFromEnd 0
                      BL.hPut lhdl bs
                      return (fs, llo)

      let lfs = fst somelogreturn
      let llo = snd somelogreturn
      case withinIndexInterval lfs of
          False -> return ()
          True  -> do
            let ro = fromIntegral(nextOffset llo) - bo -- calculate relativeOffset for Index
            let op = (fromIntegral ro, fromIntegral lfs)
            BL.hPut ihdl $ runPut $ buildOffsetPosition op
      
  return ()

withinIndexInterval :: Integer -> Bool
withinIndexInterval 0 = False
withinIndexInterval fs = 0 == (fs `mod` 100)

appendIndex :: String -> OffsetPosition -> IO ()
appendIndex path op = do 
  let bs = runPut $ buildOffsetPosition op
  BL.appendFile path bs 

buildOffsetPosition :: OffsetPosition -> Put
buildOffsetPosition (o, p) = do 
    putWord32be o
    putWord32be p

-------------------------------------------------------
-- Read
-------------------------------------------------------

readLog :: (TopicStr, Int) -> Offset -> IO [MessageSet]
readLog tp o = do
  bos <- getBaseOffsets tp 
  let bo = getBaseOffsetFor bos o
  op <- indexLookup tp bo o 
  print op
  log <- getLogFrom tp bo op
  return $ filterMessageSetsFor log o 

indexLookup :: (TopicStr, Int) -> BaseOffset -> Offset -> IO OffsetPosition 
---locate the offset/location pair for the greatest offset less than or equal
-- to the target offset.
indexLookup (t, p) bo to = do 
  let path = getPath (getLogFolder (t, p)) (indexFile bo)
  bs <- mmapFileByteStringLazy path Nothing 
  case decodeIndex bs of
    Left (bs, byo, e)   -> do
        print e
        return $ (0,0) --todo: error handling
    Right (bs, byo, ops) -> do 
      print ops 
      return $ getOffsetPositionFor ops bo to 

getOffsetPositionFor :: [OffsetPosition] -> BaseOffset -> Offset -> OffsetPosition 
-- get greatest offsetPosition from list that is less than or equal target offset 
getOffsetPositionFor [] bo to = (0, 0)
getOffsetPositionFor [x] bo to = x
getOffsetPositionFor (x:xs) bo to
       | targetOffset <= absoluteIndexOffset = (0,0) 
       | absoluteIndexOffset <= targetOffset && targetOffset < nextAbsoluteIndexOffset = x 
       | otherwise = getOffsetPositionFor xs bo to 
  where  nextAbsoluteIndexOffset = ((fromIntegral $ fst $ head $ xs) + bo)
         absoluteIndexOffset = (fromIntegral $ fst $ x) + bo
         targetOffset = fromIntegral $ to 

getBaseOffsetFor :: [BaseOffset] -> Offset -> BaseOffset
-- get greatest baseOffset from list that is less than or equal target offset 
getBaseOffsetFor [] to = 0
getBaseOffsetFor [x] to = x
getBaseOffsetFor (x:xs) to = if (x <= fromIntegral to && fromIntegral to < head xs) then x else getBaseOffsetFor xs to

-- searchLogFor 
-- Search forward the log file  for the position of the last offset that is greater than or equal to the target offset
-- and return its physical position

getLogFrom :: (TopicStr, Int) -> BaseOffset -> OffsetPosition -> IO [MessageSet]
-- ParseLog starting from given physical Position. 
getLogFrom (t, p) bo (_, phy) = do 
  let path = getPath (logFolder t p) (logFile bo)
  fs <- getFileSize path
  --print phy
  bs <- mmapFileByteStringLazy path $ Just (fromIntegral phy, (fromIntegral (fs) - fromIntegral phy))
  return $ runGet decodeLog bs 

decodeLog :: Get [MessageSet]
decodeLog = do
  empty <- isEmpty
  if empty
    then return []
      else do ms <- messageSetParser
              mss <- decodeLog 
              return $ ms : mss

filterMessageSetsFor :: [MessageSet] -> Offset -> [MessageSet]
filterMessageSetsFor ms to = filter (\x -> offset x >= fromIntegral to) ms 

---------------
-- ResourceT
-- ------------
appendLogSafe :: (TopicStr, Int, [MessageSet]) -> IO ()
appendLogSafe (t, p, ms) = do
  bo <- getLastBaseOffset (t, p)
  R.runResourceT $ runAppend bo (t, p, ms)

runAppend :: BaseOffset -> (TopicStr, Int, [MessageSet]) -> R.ResourceT IO ()
runAppend bo (t, p, ms) = do
  let logPath = getPath (logFolder t p) (logFile 0)
  let indexPath = getPath (logFolder t p) (indexFile 0)
  (key, iresource) <- R.allocate (allocateFile indexPath) free
  -- Register some Action with resources 
  let lop = getLastOffsetPosition' iresource
  let phys = fromIntegral $ snd lop


  (ikey, lresource) <- R.allocate (allocateFile logPath) free
  -- Register some Action with resources 

  let fs = toInteger $ BL.length lresource
  let llo = case getLastLogOffset' lresource of
              Nothing -> 0
              Just llo -> llo

  --print $ "last log offset: " ++ (show llo)
  let bs = runPut $ buildMessageSets $ continueOffset (nextOffset llo) ms
  R.register $ BL.appendFile logPath bs

  R.release ikey

  case withinIndexInterval fs of
      False -> return ()
      True  -> do
        let ro = fromIntegral(nextOffset llo) - bo -- calculate relativeOffset for Index
        let op = (fromIntegral ro, fromIntegral fs)
        k <- R.register $ BL.appendFile indexPath (runPut $ buildOffsetPosition op)
        return ()

  R.release key

allocateFile :: String -> IO BL.ByteString
allocateFile path = BL.readFile path -- mmapFileByteStringLazy path Nothing

free :: BL.ByteString -> IO() 
free bs = return () --TODO: free Space

--decodeLogFrom :: Offset -> Get MessageSet
--decodeLogFrom to = do
--  empty <- isEmpty
--  if empty
--    then return $ MessageSet 0 0 (Message 0 (Payload 0 0 0 0 BC.empty)) --TOOD: possible to work with Maybe MessageSet?
--    else do ms <- messageSetParser
--            if (offset ms) >= to 
--              then return ms
--              else decodeLogFrom to 

--getRelativeOffset :: BaseOffset -> Offset -> RelativeOffset
--getRelativeOffset bo o = fromIntegral o - fromIntegral bo
--
----appendSegment :: [MessageSet] -> 
----appendSegment = do
--    --appendIndex
--    --appendLog
--
--assignOffset :: [MessageSet] -> Offset -> [MessageSet]
--assignOffset ms o = ms
--
--
--buildIndexPath :: BaseOffset -> (TopicName, Int) -> String
--buildIndexPath bo (t, p) = buildPath bo (t, p) ".index"
--
--buildLogPath :: BaseOffset -> (TopicName, Int) -> String
--buildLogPath bo (t, p) = buildPath bo (t, p) ".log"
--
--buildPath :: BaseOffset -> (TopicName, Int) -> String -> String
--buildPath bo (t, p) ending = (show t) ++ "_" ++ (show p) ++ "/" ++ (show bo) ++ ending
--
--appendLog :: (TopicName, Int) -> [MessageSet] -> IO()
--appendLog (t, p) ms = do 
--  bo <- getLastBaseOffset (t,p)
--  let path = (buildLogPath bo (t, p))
--  op <- getLastOffsetPosition bo
--  lo <- getLastLogOffset bo (t,p) op
--  BL.appendFile 
--    path $ 
--    buildMessageSets $ assignOffset (ms) $ lo
--
--    --TODO Chaining with "." does not compile. Chaining with "$" does not work
--    --yet because of IO return Types 
--    --
--    --buildMessageSets 
--    -- . assignOffset (ms) 
--    -- . getLastLogOffset bo 
--    -- . getLastOffsetPosition bo
--    -- . getLastBaseOffset (t, p) 
--
----readLastIndexEntry :: (TopicStr, PartitionStr) ->  IO IndexEntry
----readLastIndexEntry (topic, partition) = do 
----  let indexPath = getPath (logFolder topic partition) (indexFile 0) 
----  ex <- doesFileExist indexPath
----  if not ex 
----    then (newIndex indexPath)
----    else (newIndex indexPath)
----  return ((0,0))

----TODO: Does not compile yet
----appendIndex :: (TopicName, Partition) -> IO()
----appendIndex (t, p) = do
----  BL.appendFile buildIndexPath (t, p) $ buildOffsetPosition 
----    ((getRelativeOffset 
----        getLastBaseOffset  
----        $ getLastLogOffset(getLastBaseOffset(t,p)) 
----          . getLastOffsetPosition 
----          . getLastBaseOffset (t, p) 
----    ),
----    getFileSize buildIndexPath (t, p))
--
----
----newIndex :: String ->  IO IndexEntry
----newIndex filepath = do 
----  let e = buildIndexEntry (0,0)
-- -- BL.writeFile filepath e
--  --return (0,0)
--

-- |
-- Module      : HMB.Internal.LogManager
-- Copyright   : (c) Marc Juchli, Lorenz Wolf 2015
-- License     : BSD-style
--
-- Maintainer  :
-- Stability   : WIP
-- Portability : GHC
-- 
-- This module acts as an inteface to any component that uses functionalities of
-- the log. 

module HMB.Internal.LogManager
  ( new
  , append
  , readLog
  , State
  ) where

import Control.Concurrent.MVar

import Data.Maybe
import qualified Data.Map.Lazy as Map

import qualified HMB.Internal.Log as Log
import qualified HMB.Internal.Index as Index
import qualified HMB.Internal.LogConfig as L

import Kafka.Protocol


type State = (Log.LogState, Index.IndexState)

-- | Initialize new Log and Index state
new :: IO (Log.LogState, Index.IndexState)
new = do
  ls <- Log.new
  is <- Index.new
  return (ls, is)

-- | Appends a Log (set of MessageSet) to memory and eventually writes to disk.
append :: (State, L.TopicStr, L.PartitionNr, Log) -> IO ()
append ((Log.LogState ls, Index.IndexState is), t, p, ms) = do
  logs <- takeMVar ls
  let log = Log.find (t, p) logs
  let recvLog = Log.continueOffset (fromMaybe (-1) (Log.lastOffset log) + 1) ms
  let newLog = log ++ recvLog

  indices <- takeMVar is
  let index = Index.find (t, p) indices
  let bo = 0 -- PERFORMANCE
  --bo <- Log.getBaseOffset (t, p) Nothing
  let lastIndexedOffset = fromIntegral (fst $ Index.lastOrNull index) + (fromIntegral bo)
  if Index.isInterval (Log.sizeRange (Just lastIndexedOffset) Nothing newLog)
     then do
        syncedIndices <- Index.append indices (t, p) newLog (Log.size newLog)
        putMVar is syncedIndices
     else do
        putMVar is indices
  let newLogs = Map.insert (t, p) newLog logs
  syncedLogs <- Log.append (t, p) newLogs
  putMVar ls syncedLogs

-- | Reads as set of MessageSes from log (directly from disk)
readLog :: (Index.IndexState, L.TopicStr, L.PartitionNr) -> Offset -> IO Log
readLog (Index.IndexState is, t, p) o = do
  bo <- Log.getBaseOffset (t, p) (Just o)
  indices <- takeMVar is
  let index = Index.find (t, p) indices
  op <- Index.lookup index bo o
  putMVar is indices
  log <- Log.lookup (t, p) bo op o
  return log

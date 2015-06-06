-- |
-- Module      : HMB.Internal.LogManager
-- Copyright   : (c) Marc Juchli, Lorenz Wolf 2015
-- License     : BSD-style
--
-- Maintainer  :
-- Stability   : WIP
-- Portability : GHC
--

module HMB.Internal.LogManager
  ( new
  , append
  , State
  ) where

import Kafka.Protocol
import qualified HMB.Internal.Log as Log
import qualified HMB.Internal.Index as Index

import Control.Concurrent.MVar

import Data.Maybe
import qualified Data.Map.Lazy as Map


-- dupl
type TopicStr = String
type PartitionNr = Int
--
type State = (Log.LogState, Index.IndexState)

new :: IO (Log.LogState, Index.IndexState)
new = do
  ls <- Log.new
  is <- Index.new
  return (ls, is)

-- | Appends a Log (set of MessageSet) to memory and eventually writes to disk.
append :: (State, TopicStr, PartitionNr, Log) -> IO ()
append ((Log.LogState ls, Index.IndexState is), t, p, ms) = do
  --Log.append (ls, t, p, ms)
  logs <- takeMVar ls
  let log = Log.find (t, p) logs
  let llo = fromMaybe 0 (Log.lastOffset log)
  let newLog = log ++ Log.continueOffset (llo + 1) ms
  let newLogs = Map.insert (t, p) newLog logs
  syncedLogs <- Log.append (t, p) newLogs
  putMVar ls syncedLogs




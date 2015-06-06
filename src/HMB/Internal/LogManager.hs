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
  , State
  ) where

import Kafka.Protocol
import qualified HMB.Internal.Log as Log
import qualified HMB.Internal.Index as Index

type State = (Log.LogState, Index.IndexState)

new :: IO (LogState, IndexState)
new = do
  ls <- Log.new
  is <- Index.new
  return (ls, is)

append :: (State, TopicStr, PartitionNr, Log) -> IO ()
append ((LogState ls, IndexState is), t, p, ms) = do
  Log.append (ls, t, p, ms)


module Main (
  main
) where

import Control.Monad
import Control.Concurrent

import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as C
import Data.IP
import Data.Word

import Kafka.Client

import Network.Socket
import qualified Network.Socket.ByteString.Lazy as SBL

import System.Entropy
import System.IO

startTest :: Int -> (b) -> b
startTest 1 f = f
startTest n f = startTest (n-1) f

main = do
  -----------------
  -- Init Socket with user input
  -----------------
  sock <- socket AF_INET Stream defaultProtocol
  setSocketOption sock ReuseAddr 1
  setSocketOption sock SendBuffer 131072
  --setSocketOption sock Linger 5
  let ip = toHostAddress (read "127.0.0.1" :: IPv4)
  --let ip = toHostAddress (read "152.96.193.212" :: IPv4)
  --let ip = toHostAddress (read "152.96.195.4" :: IPv4)
  connect sock (SockAddrInet 4343 ip)

  -- FIXME (meiersi): I'd also recommend making the length of this string a
  -- command-line parameter to simplify tests.
  -- FIXME (meiersi): also consider whether you can create criterion
  -- microbenchmarks for all relevant parts of the message processing code
  -- path. This will help you pinpoint performance problems and direct your
  -- optimization efforts.
  print "Number of bytes: "
  x <- getLine
  print "Baching factor: "
  y <- getLine
  print "Number of repeats: "
  z <- getLine
  let numberOfBytes = read x :: Int
  let batchSize = read y :: Int
  let numberOfRepeats = read z :: Int

  randBytes <- getEntropy numberOfBytes

  let topicA = stringToTopic "performance-0"
  let topicB = stringToTopic "performance-1"
  let clientId = stringToClientId "benchmark-producer"
  let bytes = [randBytes | x <- [1..batchSize]]

  let head = Head 0 0 clientId
  let req = Produce head [ ToTopic topicA [ ToPart 0 bytes]]

  replicateM_ numberOfRepeats (sendRequest sock $ req)
  putStrLn "done produce"
  threadDelay 10000000
  return ()


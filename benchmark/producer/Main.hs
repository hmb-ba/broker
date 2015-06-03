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
  let ip = toHostAddress (read "127.0.0.1" :: IPv4)
  --let ip = toHostAddress (read "152.96.195.205" :: IPv4)
  --let ip = toHostAddress (read "152.96.195.4" :: IPv4)
  connect sock (SockAddrInet 4343 ip)

  -------------------------
  -- Send / Receive Loop
  -------------------------
  -- FIXME (meiersi): I'd also recommend making the length of this string a
  -- command-line parameter to simplify tests.
  -- FIXME (meiersi): also consider whether you can create criterion
  -- microbenchmarks for all relevant parts of the message processing code
  -- path. This will help you pinpoint performance problems and direct your
  -- optimization efforts.
  randBytes <- getEntropy 100

  let topicA = stringToTopic "performance-0"
  let topicB = stringToTopic "performance-1"
  let clientId = stringToClientId "benchmark-producer"
  let bytes = [randBytes | x <- [1..10]]

  let head = Head 0 0 clientId
  let prod = Produce head [ ToTopic topicA [ ToPart 0 bytes, ToPart 1 bytes], ToTopic topicB [ToPart 0 bytes] ]

  let req = pack prod

  replicateM_ 1000000 (sendRequest sock $ req)
  putStrLn "done produce"
  return ()


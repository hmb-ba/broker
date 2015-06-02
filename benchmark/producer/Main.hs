module Main (
  main
) where

-- FIXME (meiersi): consider to sort alphabetically and introduce an empty
-- line whenever the prefix before the first '.' changes. It is a simple rule
-- and improves readability of the dependency graph.
import Kafka.Client
import Network.Socket
import System.IO
import Control.Monad
import Data.IP
import Data.Word
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as C
import Control.Concurrent
import qualified Network.Socket.ByteString.Lazy as SBL
import System.Entropy

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
  -- FIXME (meiersi): consider using a randomly generated string to avoid
  -- artifacts due to accidental regularity.
  -- FIXME (meiersi): I'd also recommend making the length of this string a
  -- command-line parameter to simplify tests.
  -- FIXME (meiersi): also consider whether you can create criterion
  -- microbenchmarks for all relevant parts of the message processing code
  -- path. This will help you pinpoint performance problems and direct your
  -- optimization efforts.

  randBytes <- getEntropy 100
  let req = packPrRqMessage (C.pack "client", toTopic $  TopicS "performance", 0,  [randBytes | x <- [1..10]])
  --let req = packPrRqMessage (C.pack "client", C.pack "performance", 0, [randBytes])
  --print req
  --replicateM_ 1000 (sendRequest sock $ req)
  replicateM_ 1000000 (sendRequest sock $ req)
  putStrLn "done produce"
  return ()

    --------------------
    -- Receive Response
    --------------------
--    input <- SBL.recv sock 4096
--    let response = decodePrResponse input
--    print response

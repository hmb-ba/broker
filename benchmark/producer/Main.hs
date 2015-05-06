module Main (
  main
) where

import Kafka.Client
import Network.Socket
import System.IO
import Control.Monad
import Data.IP
import Data.Word
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as C

import qualified Network.Socket.ByteString.Lazy as SBL

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
  connect sock (SockAddrInet 4343 ip)

  -------------------------
  -- Send / Receive Loop
  -------------------------
  return $ map show $ replicate 1000000 (packPrRqMessage ("clientX", "topicX", 0, "100bytes"))
  return ()
    
    --------------------
    -- Receive Response
    --------------------
--    input <- SBL.recv sock 4096
--    let response = decodePrResponse input
--    print response 

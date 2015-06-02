-- |
-- Module      : HMB.Internal.Network
-- Copyright   : (c) Marc Juchli, Lorenz Wolf 2015
-- License     : BSD-style
--
-- Maintainer  :
-- Stability   : WIP
-- Portability : GHC
--
-- This modules encapsulate action on the network. It initiates Socket
-- connections and receive bytes from client. It chunks the received bytes
-- into single requests and provide it to the API Layer.
--
-- > import Network.Socket
-- > import Network.Socket.ByteString.Lazy
--
module HMB.Internal.Network
(
  initSock,
  initRqChan,
  initRsChan,
  runAcceptor,
  runResponder
) where

import HMB.Internal.Types

import Data.Int
import Data.Binary.Get

import Control.Applicative
import Control.Concurrent
import Control.Concurrent.Chan
import Control.Exception

import qualified Data.ByteString.Lazy as B

import Network.Socket
import qualified Network.Socket.ByteString.Lazy as S

--------------------------
--Initialize Socket
---------------------------
initSock :: IO Socket
initSock = do
  sock <- socket AF_INET Stream 0
  setSocketOption sock ReuseAddr 1
  bindSocket sock (SockAddrInet 4343 iNADDR_ANY)
  listen sock 2
  return sock

--------------------------
-- Initialize Request Channel
---------------------------
initRqChan :: IO RequestChan
initRqChan = newChan

--------------------------
-- Initialize Response Channel
---------------------------
initRsChan :: IO ResponseChan
initRsChan = newChan

-----------------------------
-- Acceptor Thread
----------------------------
runAcceptor :: Socket -> RequestChan -> IO ()
runAcceptor sock chan =  do
  (conSock, sockAddr) <- accept sock
  putStrLn $ "***Host " ++ (show sockAddr) ++ " connected***"
  forkIO $ runConnection (conSock, sockAddr) chan True
  runAcceptor sock chan

-----------------------------
--Connection Processor Thread
----------------------------
runConnection :: (Socket, SockAddr) -> RequestChan -> Bool -> IO ()
runConnection conn chan True = do
  r <- recvFromSock conn
  case (r) of
    Left e -> do
      handleSocketError conn  e
      runConnection conn chan False
    Right input -> do
      writeToReqChan conn chan input
      runConnection conn chan True
runConnection conn chan False = return () --TODO: Better solution for breaking out the loop?

recvFromSock :: (Socket, SockAddr) -> IO (Either SocketError B.ByteString)
recvFromSock (sock, sockaddr) =  do
  -- FIXME (meiersi): it is very bad stayle to indiscriminately catch
  -- 'SomeException'! It leads to losing asynchronous exceptions like
  -- 'ThreadKilled' or 'UserInterrupt'. You should just catch exactly the
  -- exceptions that you want to handle.
  respLen <- try (S.recv sock (4 :: Int64)) :: IO (Either SomeException B.ByteString)
  case respLen of
    Left e -> return $ Left $ SocketRecvError $ show e
    Right rl -> do
      let parsedLength = getLength $ rl
      case parsedLength of
        Left (b, bo, e) -> return $ Left $ SocketRecvError $ show e
        Right (b, bo, l) ->  do
          req <- recvExactly sock l  -- TODO: Socket Error handling :: IO (Either SomeException BS.ByteString)
          return $! Right req
   where
      getLength = runGetOrFail $ fromIntegral <$> getWord32be

-- Because Socket.Recv: may return fewer bytes than specified
recvExactly :: Socket -> Int64 -> IO B.ByteString
recvExactly sock size = B.concat . reverse <$> loop [] 0
  where
    loop chunks bytesRead
        | bytesRead >= size = return chunks
        | otherwise = do
            chunk <- S.recv sock (size - bytesRead)
            if B.null chunk
              then return chunks
              else loop (chunk:chunks) $! bytesRead + B.length chunk

-- FIXME (meiersi): why so skimpy on spaces? 'IO()' is NOT idiomatic Haskell ;-)
writeToReqChan :: (Socket, SockAddr) -> RequestChan -> B.ByteString -> IO()
writeToReqChan conn chan req = writeChan chan (conn, req)

handleSocketError :: (Socket, SockAddr) -> SocketError -> IO()
handleSocketError (sock, sockaddr) (SocketRecvError e) = putStrLn $ "[Socket Receive Error] " ++ e
handleSocketError (sock, sockaddr) (SocketSendError e) = putStrLn $ "[Socket Send Error] " ++ e
  --sClose sock

-----------------------
-- Response Processor Thread
-----------------------
runResponder :: ResponseChan -> IO()
runResponder chan = do
  (conn, res) <- readChan chan
  res <- sendResponse conn res
  case res of
    Left e -> handleSocketError conn $ SocketSendError $ show e ---TODO: What to do with responses when client disconnected?
    Right io -> return io
  runResponder chan

sendResponse :: (Socket, SockAddr) -> B.ByteString -> IO(Either SomeException ())
sendResponse (socket, addr) responsemessage = try(S.sendAll socket $ responsemessage) :: IO (Either SomeException ())



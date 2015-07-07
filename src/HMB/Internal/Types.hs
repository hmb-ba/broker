module HMB.Internal.Types
  -- FIXME (SM): explicit exports should always be used. It makes the code
  -- more readable and helps in removing unneeded code.
where 

import Network.Socket 
import qualified Data.ByteString.Lazy as B
import Control.Concurrent.Chan

--------------
--Channels
-------------

-- FIXME (SM): I think one should really use bounded channels for the
-- communicatoin. Otherwise, it is too easy for the broker to go out-of-memory
-- under load, which is the worst possible behaviour. It should be easy or at
-- least possible to bound the maximal amount of memory that the broker is
-- going to use.
type ChanMessage = ((Socket, SockAddr), B.ByteString)
type RequestChan = Chan ChanMessage
type ResponseChan = Chan ChanMessage

---------------
-- error types
---------------

data ErrorCode =
        NoError
      | Unknown
      | OffsetOutOfRange
      | InvalidMessage
      | UnknownTopicOrPartition
      | InvalidMessageSize
      | LeaderNotAvailable
      | NotLeaderForPartition
      | RequestTimedOut
      | BrokerNotAvailable
      | ReplicaNotAvailable
      | MessageSizeTooLarge
      | StaleControllerEpochCode
      | OffsetMetadataTooLargeCode
      | OffsetsLoadInProgressCode
      | ConsumerCoordinatorNotAvailableCode
      | NotCoordinatorForConsumerCodeA
        deriving (Show, Enum)

data SocketError =
        SocketRecvError String
      | SocketSendError String
        deriving Show

data HandleError =
        PrWriteLogError Int String
      | PrPackError String
      | ParseRequestError String
      | FtReadLogError Int String
      | SendResponseError String
      | UnknownRqError
        deriving Show


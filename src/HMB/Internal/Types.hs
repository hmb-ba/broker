module HMB.Internal.Types
where 

import Network.Socket 
import qualified Data.ByteString.Lazy as B
import Control.Concurrent.Chan

--------------
--Channels
-------------
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


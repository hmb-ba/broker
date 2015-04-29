module HMB.Internal.Types

where
  
data HandleError =
        ParseRequestError String
      | PrError String
      | FtError String
        deriving Show


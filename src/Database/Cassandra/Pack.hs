{-| A Collection of utilities for binary packing values into Bytestring |-}

module Database.Cassandra.Pack
    ( packLong
    ) where

-------------------------------------------------------------------------------
import qualified Data.Binary.Put            as BN
import           Data.ByteString.Lazy.Char8 (ByteString)
-------------------------------------------------------------------------------


-------------------------------------------------------------------------------
-- | Pack any integral value into LongType
packLong :: Integral a => a -> ByteString
packLong = BN.runPut . BN.putWord64be . fromIntegral


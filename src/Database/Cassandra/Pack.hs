{-# LANGUAGE FlexibleInstances    #-}
{-# LANGUAGE OverlappingInstances #-}
{-# LANGUAGE RankNTypes           #-}

{-| A Collection of utilities for binary packing values into Bytestring |-}

module Database.Cassandra.Pack
    ( CasType (..)
    , TAscii (..)
    , TBytes (..)
    , TCounter (..)
    , TInt (..)
    , TInt32 (..)
    , TUtf8 (..)
    , TUUID (..)
    , TLong (..)
    , Exclusive (..)
    , Single (..)
    ) where

-------------------------------------------------------------------------------
import           Control.Applicative
import           Data.Binary
import           Data.Binary.Get
import           Data.Binary.Put
import qualified Data.ByteString.Char8      as B
import           Data.ByteString.Lazy.Char8 (ByteString)
import qualified Data.ByteString.Lazy.Char8 as LB
import           Data.Char
import           Data.Int
import           Data.List
import           Data.Text                  (Text)
import qualified Data.Text                  as T
import qualified Data.Text.Encoding         as T
import qualified Data.Text.Lazy             as LT
import qualified Data.Text.Lazy.Encoding    as LT

import           Data.Time
-------------------------------------------------------------------------------



-------------------------------------------------------------------------------
newtype TAscii = TAscii { getAscii :: ByteString } deriving (Eq,Show,Read,Ord)
newtype TBytes = TBytes { getTBytes :: ByteString } deriving (Eq,Show,Read,Ord)
newtype TCounter = TCounter { getCounter :: ByteString } deriving (Eq,Show,Read,Ord)
newtype TInt32 = TInt32 { getInt32 :: Int32 } deriving (Eq,Show,Read,Ord)
newtype TInt = TInt { getInt :: Integer } deriving (Eq,Show,Read,Ord)
newtype TUUID = TUUID { getUUID :: ByteString } deriving (Eq,Show,Read,Ord)
newtype TLong = TLong { getLong :: Integer } deriving (Eq,Show,Read,Ord)
newtype TUtf8 = TUtf8 { getUtf8 :: Text } deriving (Eq,Show,Read,Ord)


-------------------------------------------------------------------------------
-- | This typeclass defines and maps to haskell types that Cassandra
-- natively knows about and uses in sorting and potentially validating
-- column key values.
--
-- All column keys are eventually sent to and received from Cassandra
-- in binary form. This typeclass allows us to map some Haskell type
-- definitions to their binary representation. The correct binary
-- serialization is handled for you behind the scenes.
--
-- For simplest cases, just use one of the string-like instances, e.g.
-- 'ByteString', 'String' or 'Text'. Please keep in mind that these
-- are just mapped to untyped BytesType.
--
-- Remember that for special column types, such as 'TLong', to have
-- any effect, your ColumnFamily must have been created with that
-- comparator or validator. Otherwise you're just encoding/decoding
-- integer values without any Cassandra support for sorting or
-- correctness.
--
-- The Python library pycassa has a pretty good tutorial to learn more.
--
-- Tuple instances support fixed ComponentType columns. Example:
--
-- > insert "testCF" "row1" [packCol ((TLong 124, TAscii "Hello"), "some content")]
class CasType a where
    encodeCas :: a -> ByteString
    decodeCas :: ByteString -> a


instance CasType B.ByteString where
    encodeCas = fromStrict
    decodeCas = toStrict


instance CasType String where
    encodeCas = LB.pack
    decodeCas = LB.unpack


instance CasType LT.Text where
    encodeCas = encodeCas . LT.encodeUtf8
    decodeCas =  LT.decodeUtf8


instance CasType T.Text where
    encodeCas = encodeCas . LT.fromChunks . return
    decodeCas = T.concat . LT.toChunks . decodeCas


instance CasType LB.ByteString where
    encodeCas = id
    decodeCas = id


instance CasType TAscii where
    encodeCas = getAscii
    decodeCas = TAscii


instance CasType TBytes where
    encodeCas = getTBytes
    decodeCas = TBytes


instance CasType TCounter where
    encodeCas = getCounter
    decodeCas = TCounter


-------------------------------------------------------------------------------
-- | Pack as a 4 byte number
instance CasType TInt32 where
    encodeCas = runPut . putWord32be . fromIntegral . getInt32
    decodeCas = TInt32 . fromIntegral . runGet getWord32be


-------------------------------------------------------------------------------
-- | Pack as an 8 byte number - same as 'TLong'
instance CasType TInt where
    encodeCas = runPut . putWord64be . fromIntegral . getInt
    decodeCas = TInt . fromIntegral . runGet getWord64be


-------------------------------------------------------------------------------
-- | Pack as an 8 byte unsigned number; negative signs are lost.
instance CasType TLong where
    encodeCas = runPut . putWord64be . fromIntegral . getLong
    decodeCas = TLong . fromIntegral . runGet getWord64be


-------------------------------------------------------------------------------
-- | Encode and decode as Utf8 'Text'
instance CasType TUtf8 where
    encodeCas = LB.fromChunks . return . T.encodeUtf8 . getUtf8
    decodeCas = TUtf8 . T.decodeUtf8 . B.concat . LB.toChunks


-------------------------------------------------------------------------------
-- | Use the 'Single' wrapper when querying only with the first of a
-- two or more field CompositeType.
instance (CasType a) => CasType (Single a) where
    encodeCas a = runPut $ do
        putSegment a end

    decodeCas bs = flip runGet bs $ Single <$> getSegment

-------------------------------------------------------------------------------
-- | Composite types - see Cassandra or pycassa docs to understand
instance (CasType a, CasType b) => CasType (a,b) where
    encodeCas (a, b) = runPut $ do
        putSegment a sep
        putSegment b end

    decodeCas bs = flip runGet bs $ (,)
        <$> getSegment
        <*> getSegment


instance (CasType a, CasType b, CasType c) => CasType (a,b,c) where
    encodeCas (a, b, c) = runPut $ do
        putSegment a sep
        putSegment b sep
        putSegment c end

    decodeCas bs = flip runGet bs $ (,,)
        <$> getSegment
        <*> getSegment
        <*> getSegment


instance (CasType a, CasType b, CasType c, CasType d) => CasType (a,b,c,d) where
    encodeCas (a, b, c, d) = runPut $ do
        putSegment a sep
        putSegment b sep
        putSegment c sep
        putSegment d end

    decodeCas bs = flip runGet bs $ (,,,)
        <$> getSegment
        <*> getSegment
        <*> getSegment
        <*> getSegment


instance (CasType a, CasType b) => CasType (a, Exclusive b) where
    encodeCas (a, Exclusive b) = runPut $ do
        putSegment a sep
        putSegment b exc

    decodeCas bs = flip runGet bs $ (,)
        <$> getSegment
        <*> (Exclusive <$> getSegment)


instance (CasType a, CasType b, CasType c) => CasType (a, b, Exclusive c) where
    encodeCas (a, b, Exclusive c) = runPut $ do
        putSegment a sep
        putSegment b sep
        putSegment c exc

    decodeCas bs = flip runGet bs $ (,,)
        <$> getSegment
        <*> getSegment
        <*> (Exclusive <$> getSegment)


instance (CasType a, CasType b, CasType c, CasType d) => CasType (a, b, c, Exclusive d) where
    encodeCas (a, b, c, Exclusive d) = runPut $ do
        putSegment a sep
        putSegment b sep
        putSegment c sep
        putSegment d exc

    decodeCas bs = flip runGet bs $ (,,,)
        <$> getSegment
        <*> getSegment
        <*> getSegment
        <*> (Exclusive <$> getSegment)


instance CasType a => CasType [a] where
    encodeCas as = runPut $ do
        mapM (flip putSegment sep) $ init as
        putSegment (last as) end


-------------------------------------------------------------------------------
-- | Exclusive tag for composite column. You may tag the end of a
-- composite range with this to make the range exclusive. See pycassa
-- documentation for more information.
newtype Exclusive a = Exclusive a deriving (Eq,Show,Read,Ord)


-------------------------------------------------------------------------------
-- | Use the Single wrapper when you want to refer only to the first
-- coolumn of a CompositeType column.
newtype Single a = Single a deriving (Eq,Show,Read,Ord)


-- | composite columns are a pain
-- need to write 2 byte length, n byte body, 1 byte separator
--
-- from pycassa:
-- The composite format for each component is:
--     <len>   <value>   <eoc>
--   2 bytes | ? bytes | 1 byte


-------------------------------------------------------------------------------
putBytes b = do
    putLen b
    putByteString b


-------------------------------------------------------------------------------
getBytes' = getLen >>= getBytes


-------------------------------------------------------------------------------
getLen :: Get Int
getLen = fromIntegral `fmap` getWord16be


-------------------------------------------------------------------------------
putLen :: B.ByteString -> Put
putLen b = putWord16be . fromIntegral $ (B.length b)



-------------------------------------------------------------------------------
toStrict = B.concat . LB.toChunks
fromStrict = LB.fromChunks . return


-------------------------------------------------------------------------------
getSegment :: CasType a => Get a
getSegment = do
    a <- (decodeCas . fromStrict) <$> getBytes'
    getWord8                    -- discard separator character
    return a


-------------------------------------------------------------------------------
putSegment a f = do
    putBytes . toStrict $ encodeCas a
    f

-- | When end point is exclusive
exc = putWord8 . fromIntegral $ ord '\xff'

-- | Regular (inclusive) end point
end = putWord8 . fromIntegral $ ord '\x01'

-- | Separator between composite parts
sep = putWord8 . fromIntegral $ ord '\x00'


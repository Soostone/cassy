{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE RankNTypes        #-}
{-# LANGUAGE OverlappingInstances        #-}

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
import           Data.Time
-------------------------------------------------------------------------------


-------------------------------------------------------------------------------
-- | Additinal cases will be added here in the future
data CasColumn
    = CBytes TBytes
    -- | CLong Integer
    -- | CInt Integer
    -- | CInt32 Int32
    -- | CUtf T.Text
    -- | CAscii B.ByteString

    -- | CUUID ByteString
    -- | CDouble Double
    -- | CDate Day
    -- | CBool Bool
    -- | CComposite [CasColumn]


packCol (CBytes x) = encodeCas x
-- packCol (CBytes x) = encodeCas . TBytes $ LB.fromChunks . return $ x
-- packCol (CLong x) = encodeCas $ TLong x
-- packCol (CInt x) = encodeCas $ TInt x
-- packCol (CInt32 x) = encodeCas $ TInt32 x
-- packCol (CUtf x) = encodeCas $ TUtf8 x
-- packCol (CAscii x) = encodeCas $ TAscii $ LB.fromChunks . return $ x


-------------------------------------------------------------------------------
unpackCol :: CasType a => (a -> CasColumn) -> ByteString -> CasColumn
unpackCol f = f . decodeCas


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
class CasType a where
    encodeCas :: a -> ByteString
    decodeCas :: ByteString -> a


instance CasType TAscii where
    encodeCas = getAscii
    decodeCas = TAscii


instance CasType TBytes where
    encodeCas = getTBytes
    decodeCas = TBytes


instance CasType TCounter where
    encodeCas = getCounter
    decodeCas = TCounter


instance CasType TInt32 where
    encodeCas = runPut . putWord32be . fromIntegral . getInt32
    decodeCas = TInt32 . fromIntegral . runGet getWord32be


instance CasType TInt where
    encodeCas = runPut . putWord64be . fromIntegral . getInt
    decodeCas = TInt . fromIntegral . runGet getWord64be


instance CasType TLong where
    encodeCas = runPut . putWord64be . fromIntegral . getLong
    decodeCas = TLong . fromIntegral . runGet getWord64be


instance CasType TUtf8 where
    encodeCas = LB.fromChunks . return . T.encodeUtf8 . getUtf8
    decodeCas = TUtf8 . T.decodeUtf8 . B.concat . LB.toChunks


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


newtype Exclusive a = Exclusive a deriving (Eq,Show,Read,Ord)


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


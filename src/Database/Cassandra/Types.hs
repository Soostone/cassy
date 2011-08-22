{-# LANGUAGE TypeSynonymInstances #-}

module Database.Cassandra.Types where

import           Control.Monad
import           Data.ByteString.Lazy (ByteString)
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy.Char8 as LB
import           Data.Int (Int32, Int64)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Text.Lazy as LT
import qualified Data.Text.Lazy.Encoding as LT
import           Data.Time
import           Data.Time.Clock.POSIX

import qualified Database.Cassandra.Thrift.Cassandra_Types as C


type ColumnFamily = String


-- | A column selector/filter statement for queries.
--
-- Remember that SuperColumns are always fully deserialized, so we don't offer
-- a way to filter columns within a 'SuperColumn'.
data Selector = 
    All
  -- ^ Return everything in 'Row'
  | ColNames [ByteString]
  -- ^ Return specific columns or super-columns depending on the 'ColumnFamily'
  | Range (Maybe ByteString) (Maybe ByteString) Order Int32
  -- ^ Return a range of columns or super-columns
  deriving (Show)


------------------------------------------------------------------------------
-- | Order in a range query
data Order = Regular | Reversed
  deriving (Show)

renderOrd Regular = False
renderOrd Reversed = True


------------------------------------------------------------------------------
-- | Content Types
--
------------------------------------------------------------------------------

type Row = [Column]


data Column = 
    SuperColumn (ByteString, [Column])
  | Column {
      colKey :: ByteString
    , colVal :: ByteString
    , colTS :: Maybe Int64
    }
  deriving (Eq,Show,Read,Ord)


castColumn :: C.ColumnOrSuperColumn -> Either CassandraException Column
castColumn x | Just c <- C.f_ColumnOrSuperColumn_column x = castCol c
castColumn x | Just c <- C.f_ColumnOrSuperColumn_super_column x = castSuperCol c
castColumn _ = Left $ ConversionException "Unsupported ColumnOrSuperColumn type"


castCol :: C.Column -> Either CassandraException Column
castCol c 
  | Just nm <- C.f_Column_name c
  , Just val <- C.f_Column_value c
  , Just ts <- C.f_Column_timestamp c
  = Right $ Column nm val (Just ts)
castCol _ = Left $ ConversionException "Can't parse Column"


castSuperCol :: C.SuperColumn -> Either CassandraException Column
castSuperCol c 
  | Just nm <- C.f_SuperColumn_name c
  , Just cols <- C.f_SuperColumn_columns c
  , Right cols' <- mapM castCol cols
  = Right $ SuperColumn (nm, cols')
castSuperCol _ = Left $ ConversionException "Can't parse SuperColumn"


data CassandraException = 
    NotFoundException
  | InvalidRequestException String
  | UnavailableException
  | TimedOutException
  | AuthenticationException String
  | AuthorizationException String
  | SchemaDisagreementException
  | ConversionException String
  deriving (Eq,Show,Read,Ord)


-- | Cassandra is VERY sensitive to its timestamp values.. as a convention,
-- timestamps are always in microseconds
getTime :: IO Int64
getTime = do
  t <- getPOSIXTime
  return . fromIntegral . floor $ t * 1000000


------------------------------------------------------------------------------
-- | Cassandra keys need to be 'ByteString's
class (Ord a) => BS a where
  bs :: a -> ByteString

instance BS String where
  bs = LB.pack 

instance BS ByteString where
  bs = id 
 
instance BS B.ByteString where
  bs = LB.fromChunks . return 

instance BS T.Text where
  bs = bs . T.encodeUtf8

instance BS LT.Text where
  bs = LT.encodeUtf8

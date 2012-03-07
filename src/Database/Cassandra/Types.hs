{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE PatternGuards, NamedFieldPuns, RecordWildCards #-}

module Database.Cassandra.Types where

import           Control.Monad
import           Control.Exception
import           Data.Generics
import           Data.ByteString.Lazy (ByteString)
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy.Char8 as LB
import           Data.Int (Int32, Int64)
import           Data.Time
import           Data.Time.Clock.POSIX

import qualified Database.Cassandra.Thrift.Cassandra_Types as C


-- | A 'Key' range selector to use with 'getMulti'.
data KeySelector = 
    Keys [Key]
  -- ^ Just a list of keys to get
  | KeyRange KeyRangeType Key Key Int32
  -- ^ A range of keys to get. Remember that RandomPartitioner ranges may not
  -- mean much as keys are randomly assigned to nodes.
  deriving (Show)


-- | Encodes the Key vs. Token options in the thrift API. 
--
-- 'InclusiveRange' ranges are just plain intuitive range queries.
-- 'WrapAround' ranges are also inclusive, but they wrap around the ring.
data KeyRangeType = InclusiveRange | WrapAround
  deriving (Show)


mkKeyRange (KeyRange ty st end cnt) = case ty of
  InclusiveRange -> C.KeyRange (Just st) (Just end) Nothing Nothing (Just cnt)
  WrapAround -> C.KeyRange Nothing Nothing (Just $ LB.unpack st) (Just $ LB.unpack end) (Just cnt)

-- | A column selector/filter statement for queries.
--
-- Remember that SuperColumns are always fully deserialized, so we don't offer
-- a way to filter columns within a 'SuperColumn'.
data Selector = 
    All
  -- ^ Return everything in 'Row'
  | ColNames [ColumnName]
  -- ^ Return specific columns or super-columns depending on the 'ColumnFamily'
  | SupNames ColumnName [ColumnName]
  -- ^ When deleting specific columns in a super column
  | Range (Maybe ColumnName) (Maybe ColumnName) Order Int32
  -- ^ Return a range of columns or super-columns
  deriving (Show)


mkPredicate :: Selector -> C.SlicePredicate
mkPredicate s = 
  let
    allRange = C.SliceRange (Just "") (Just "") (Just False) (Just 100)
  in case s of
    All -> C.SlicePredicate Nothing (Just allRange)
    ColNames ks -> C.SlicePredicate (Just ks) Nothing
    Range st end ord cnt -> 
      let
        st' = st `mplus` Just ""
        end' = end `mplus` Just ""
      in C.SlicePredicate Nothing 
          (Just (C.SliceRange st' end' (Just $ renderOrd ord) (Just cnt)))



------------------------------------------------------------------------------
-- | Order in a range query
data Order = Regular | Reversed
  deriving (Show)

renderOrd Regular = False
renderOrd Reversed = True


type ColumnFamily = String


type Key = ByteString


type ColumnName = ByteString


type Value = ByteString


------------------------------------------------------------------------------
-- | A Column is either a single key-value pair or a SuperColumn with an
-- arbitrary number of key-value pairs
data Column = 
    SuperColumn ColumnName [Column]
  | Column {
      colKey :: ColumnName
    , colVal :: Value
    , colTS :: Maybe Int64
    -- ^ Last update timestamp; will be overridden during write/update ops
    , colTTL :: Maybe Int32
    -- ^ A TTL after which Cassandra will erase the column
    }
  deriving (Eq,Show,Read,Ord)


------------------------------------------------------------------------------
-- | A full row is simply a sequence of columns
type Row = [Column]


------------------------------------------------------------------------------
-- | A short-hand for creating key-value 'Column' values
col :: ByteString -> ByteString -> Column
col k v = Column k v Nothing Nothing


mkThriftCol :: Column -> IO C.Column
mkThriftCol Column{..} = do
  now <- getTime
  return $ C.Column (Just colKey) (Just colVal) (Just now) colTTL


castColumn :: C.ColumnOrSuperColumn -> Either CassandraException Column
castColumn x | Just c <- C.f_ColumnOrSuperColumn_column x = castCol c
             | Just c <- C.f_ColumnOrSuperColumn_super_column x = castSuperCol c
castColumn _ = 
  Left $ ConversionException "castColumn: Unsupported/unexpected ColumnOrSuperColumn type"


castCol :: C.Column -> Either CassandraException Column
castCol c 
  | Just nm <- C.f_Column_name c
  , Just val <- C.f_Column_value c
  , Just ts <- C.f_Column_timestamp c
  , ttl <- C.f_Column_ttl c
  = Right $ Column nm val (Just ts) ttl
castCol _ = Left $ ConversionException "Can't parse Column"


castSuperCol :: C.SuperColumn -> Either CassandraException Column
castSuperCol c 
  | Just nm <- C.f_SuperColumn_name c
  , Just cols <- C.f_SuperColumn_columns c
  , Right cols' <- mapM castCol cols
  = Right $ SuperColumn nm cols'
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
  | OperationNotSupported String
  deriving (Eq,Show,Read,Ord,Data,Typeable)


instance Exception CassandraException


------------------------------------------------------------------------------
-- | Cassandra is VERY sensitive to its timestamp values. As a convention,
-- timestamps are always in microseconds
getTime :: IO Int64
getTime = do
  t <- getPOSIXTime
  return . fromIntegral . floor $ t * 1000000


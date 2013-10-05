{-# LANGUAGE DeriveDataTypeable        #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleInstances         #-}
{-# LANGUAGE NamedFieldPuns            #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE PatternGuards             #-}
{-# LANGUAGE RecordWildCards           #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE TypeSynonymInstances      #-}

module Database.Cassandra.Types where

-------------------------------------------------------------------------------
import           Control.Exception
import           Control.Exception.Lifted                  as E
import           Control.Monad
import qualified Data.ByteString.Char8                     as B
import           Data.ByteString.Lazy                      (ByteString)
import qualified Data.ByteString.Lazy.Char8                as LB
import           Data.Default
import           Data.Generics
import           Data.Int                                  (Int32, Int64)
import           Data.List
import           Data.Text                                 (Text)
import qualified Data.Text                                 as T
import qualified Data.Text.Encoding                        as T
import qualified Data.Text.Lazy                            as LT
import qualified Data.Text.Lazy.Encoding                   as LT
import           Data.Time
import           Data.Time.Clock.POSIX
import qualified Database.Cassandra.Thrift.Cassandra_Types as C
-------------------------------------------------------------------------------
import           Database.Cassandra.Pack
-------------------------------------------------------------------------------


------------------------------------------------------------------------------
-- | Possible outcomes of a modify operation
data ModifyOperation a =
    Update a
  | Delete
  | DoNothing
  deriving (Eq,Show,Ord,Read)


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


-------------------------------------------------------------------------------
-- | A column selector/filter statement for queries.
--
-- Remember that SuperColumns are always fully deserialized, so we don't offer
-- a way to filter columns within a 'SuperColumn'.
--
-- Column names and ranges are specified by any type that can be
-- packed into a Cassandra column using the 'CasType' typeclass.
data Selector =
    All
  -- ^ Return everything in 'Row'
  | forall a. CasType a => ColNames [a]
  -- ^ Return specific columns or super-columns depending on the 'ColumnFamily'
  | forall a b. (CasType a, CasType b) => SupNames a [b]
  -- ^ When deleting specific columns in a super column
  | forall a b. (CasType a, CasType b) => Range {
      rangeStart :: Maybe a
    , rangeEnd   :: Maybe b
    , rangeOrder :: Order
    , rangeLimit :: Int32
    }
  -- ^ Return a range of columns or super-columns.

-------------------------------------------------------------------------------
-- | A default starting point for range 'Selector'. Use this so you
-- don't run into ambiguous type variables when using Nothing.
--
-- > range = Range (Nothing :: Maybe ByteString) (Nothing :: Maybe ByteString) Regular 1024
range = Range (Nothing :: Maybe ByteString) (Nothing :: Maybe ByteString) Regular 1024


boundless :: Maybe ByteString
boundless = Nothing



instance Default Selector where
    def = All

instance Show Selector where
    show All = "All"
    show (ColNames cns) = concat
        ["ColNames: ", intercalate ", " $ map showCas cns]
    show (SupNames cn cns) = concat
        ["SuperCol: ", showCas cn, "; Cols: ", intercalate ", " (map showCas cns)]
    show (Range a b order i) = concat
        [ "Range from ", maybe "Nothing" showCas a, " to ", maybe "Nothing" showCas b
        , " order ", show order, " max ", show i, " items." ]


-------------------------------------------------------------------------------
showCas :: CasType a => a -> String
showCas t = LB.unpack . encodeCas $ t


-------------------------------------------------------------------------------
mkPredicate :: Selector -> C.SlicePredicate
mkPredicate s =
  let
    allRange = C.SliceRange (Just "") (Just "") (Just False) (Just 50000)
  in case s of
    All -> C.SlicePredicate Nothing (Just allRange)
    ColNames ks -> C.SlicePredicate (Just (map encodeCas ks)) Nothing
    Range st end ord cnt ->
      let
        st' = fmap encodeCas st `mplus` Just ""
        end' = fmap encodeCas end `mplus` Just ""
      in C.SlicePredicate Nothing
          (Just (C.SliceRange st' end' (Just $ renderOrd ord) (Just cnt)))



------------------------------------------------------------------------------
-- | Order in a range query
data Order = Regular | Reversed
  deriving (Show)


-------------------------------------------------------------------------------
renderOrd Regular = False
renderOrd Reversed = True


-------------------------------------------------------------------------------
reverseOrder Regular = Reversed
reverseOrder _ = Regular


type ColumnFamily = String


type Key = ByteString
type RowKey = Key


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
    , colTS  :: Maybe Int64
    -- ^ Last update timestamp; will be overridden during write/update ops
    , colTTL :: Maybe Int32
    -- ^ A TTL after which Cassandra will erase the column
    }
  deriving (Eq,Show,Read,Ord)


------------------------------------------------------------------------------
-- | A full row is simply a sequence of columns
type Row = [Column]


------------------------------------------------------------------------------
-- | A short-hand for creating key-value 'Column' values. This is
-- pretty low level; you probably want to use 'packCol'.
col :: ByteString -> ByteString -> Column
col k v = Column k v Nothing Nothing


mkThriftCol :: Column -> IO C.Column
mkThriftCol Column{..} = do
  now <- getTime
  return $ C.Column (Just colKey) (Just colVal) (Just now) colTTL
mkThriftCol _ = error "mkThriftCol can only process regular columns."


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


-- | Exception handler that returns @True@ for errors that may be
-- resolved after a retry. So they are good candidates for 'retrying'
-- queries.
casRetryH :: Monad m => E.Handler m Bool
casRetryH = E.Handler $ \ e -> return $
    case e of
      UnavailableException{} -> True
      TimedOutException{} -> True
      SchemaDisagreementException{} -> True
      _ -> False


-- | 'IOException's should be retried
networkRetryH :: Monad m => E.Handler m Bool
networkRetryH = E.Handler $ \ (_ :: IOException) -> return True


------------------------------------------------------------------------------
-- | Cassandra is VERY sensitive to its timestamp values. As a convention,
-- timestamps are always in microseconds
getTime :: IO Int64
getTime = do
  t <- getPOSIXTime
  return . fromIntegral . floor $ t * 1000000


                               ----------------
                               -- Pagination --
                               ----------------


-------------------------------------------------------------------------------
-- | Describes the result of a single pagination action
data PageResult m a
  = PDone { pCache :: [a] }
  -- ^ Done, this is all I have.
  | PMore { pCache :: [a], pMore :: m (PageResult m a) }
  -- ^ Here's a batch and there is more when you call the action.




-------------------------------------------------------------------------------
pIsDry x = pIsDone x && null (pCache x)

-------------------------------------------------------------------------------
pIsDone PDone{} = True
pIsDone _ = False


-------------------------------------------------------------------------------
pHasMore PMore{} = True
pHasMore _ = False


-------------------------------------------------------------------------------
instance Monad m => Functor (PageResult m) where
    fmap f (PDone as) = PDone (fmap f as)
    fmap f (PMore as m) = PMore (fmap f as) m'
      where
        m' = liftM (fmap f) m



                             --------------------
                             -- CKey Typeclass --
                             --------------------

------------------------------------------------------------------------------
-- | A typeclass to enable using any string-like type for row and column keys
class CKey a where
  toColKey    :: a -> ByteString
  fromColKey  :: ByteString -> Either String a


-------------------------------------------------------------------------------
-- | Raise an error if conversion fails
fromColKey' :: CKey a => ByteString -> a
fromColKey' = either error id . fromColKey


-------------------------------------------------------------------------------
-- | For easy composite keys, just serialize your data type to a list
-- of bytestrings, we'll concat them and turn them into column keys.
instance CKey [B.ByteString] where
    toColKey xs = LB.intercalate ":" $ map toColKey xs
    fromColKey str = mapM fromColKey $ LB.split ':' str


instance CKey String where
    toColKey = LB.pack
    fromColKey = return . LB.unpack


instance CKey LT.Text where
    toColKey = LT.encodeUtf8
    fromColKey = return `fmap` LT.decodeUtf8


instance CKey T.Text where
    toColKey = toColKey . LT.fromChunks . return
    fromColKey = fmap (T.concat . LT.toChunks) . fromColKey


instance CKey B.ByteString where
    toColKey = LB.fromChunks . return
    fromColKey = fmap (B.concat . LB.toChunks) . fromColKey


instance CKey ByteString where
    toColKey = id
    fromColKey = return



{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE NamedFieldPuns             #-}
{-# LANGUAGE NoMonomorphismRestriction  #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE PatternGuards              #-}
{-# LANGUAGE RecordWildCards            #-}


module Database.Cassandra.Basic

    (

    -- * Connection
      CPool
    , Server
    , defServer
    , defServers
    , KeySpace
    , createCassandraPool

    -- * MonadCassandra Typeclass
    , MonadCassandra (..)
    , Cas (..)
    , runCas
    , mapCassandra

    -- * Cassandra Operations
    , getCol
    , get
    , getMulti
    , insert
    , delete

    -- * Filtering
    , Selector(..)
    , range
    , boundless
    , Order(..)
    , reverseOrder
    , KeySelector(..)
    , KeyRangeType(..)

    -- * Exceptions
    , CassandraException(..)

    -- * Utility
    , getTime
    , throwing
    , wrapException

    -- * Basic Types
    , ColumnFamily
    , Key
    , ColumnName
    , Value
    , Column(..)
    , col
    , packCol
    , unpackCol
    , packKey
    , Row
    , ConsistencyLevel(..)

    -- * Helpers
    , CKey (..)
    , fromColKey'

    -- * Working with column types
    , CasType (..)
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
import           Control.Concurrent.Async
import           Control.Exception
import           Control.Monad
import           Control.Monad.Reader
import           Data.ByteString.Lazy                       (ByteString)
import           Data.Map                                   (Map)
import qualified Data.Map                                   as M
import           Data.Maybe                                 (mapMaybe)
import           Data.Traversable                           (Traversable)
import qualified Database.Cassandra.Thrift.Cassandra_Client as C
import           Database.Cassandra.Thrift.Cassandra_Types 
                                                             (ConsistencyLevel (..))
import qualified Database.Cassandra.Thrift.Cassandra_Types  as T
import           Prelude                                    hiding (catch)
-------------------------------------------------------------------------------
import           Database.Cassandra.Pack
import           Database.Cassandra.Pool
import           Database.Cassandra.Types
-------------------------------------------------------------------------------


-- test = do
--   pool <- createCassandraPool [("127.0.0.1", 9160)] 3 300 "Keyspace1"
--   withResource pool $ \ Cassandra{..} -> do
--     let cp = T.ColumnParent (Just "CF1") Nothing
--     let sr = Just $ T.SliceRange (Just "") (Just "") (Just False) (Just 100)
--     let ks = Just ["eben"]
--     let sp = T.SlicePredicate Nothing sr
--     C.get_slice (cProto, cProto) "darak" cp sp ONE
--   flip runCas pool $ do
--     get "CF1" "CF1" All ONE
--     getCol "CF1" "darak" "eben" ONE
--     insert "CF1" "test1" ONE [col "col1" "val1", col "col2" "val2"]
--     get  "CF1" "CF1" All ONE >>= liftIO . print
--     get  "CF1" "not here" All ONE >>= liftIO . print
--     delete  "CF1" "CF1" (ColNames ["col2"]) ONE
--     get  "CF1" "CF1" (Range Nothing Nothing Reversed 1) ONE >>= liftIO . putStrLn . show


-------------------------------------------------------------------------------
-- | All Cassy operations are designed to run inside 'MonadCassandra'
-- context.
--
-- We provide a default concrete 'Cas' datatype, but you can simply
-- make your own application monads an instance of 'MonadCassandra'
-- for conveniently using all operations of this package.
--
-- Please keep in mind that all Cassandra operations may raise
-- 'CassandraException's at any point in time.
class (MonadIO m) => MonadCassandra m where
    getCassandraPool :: m CPool


-------------------------------------------------------------------------------
-- | Run a list of cassandra computations in parallel using the async library
mapCassandra :: (Traversable t, MonadCassandra m) => t (Cas b) -> m (t b)
mapCassandra ms = do
    cp <- getCassandraPool
    let f m = runCas cp m
    liftIO $ mapConcurrently f ms


-------------------------------------------------------------------------------
withCassandraPool :: MonadCassandra m => (Cassandra -> IO b) -> m b
withCassandraPool f = do
  p <- getCassandraPool
  liftIO $ withResource p f


-------------------------------------------------------------------------------
type Cas a = ReaderT CPool IO a


-------------------------------------------------------------------------------
-- | Main running function when using the ad-hoc Cas monad. Just write
-- your cassandra actions within the 'Cas' monad and supply them with
-- a 'CPool' to execute.
runCas :: CPool -> Cas a -> IO a
runCas = flip runReaderT


-------------------------------------------------------------------------------
instance (MonadIO m) => MonadCassandra (ReaderT CPool m) where
    getCassandraPool = ask


------------------------------------------------------------------------------
-- | Get a single key-column value.
getCol
  :: (MonadCassandra m, CasType k)
  => ColumnFamily
  -> ByteString
  -- ^ Row key
  -> k
  -- ^ Column/SuperColumn key; see 'CasType' for what it can be. Use
  -- ByteString in the simple case.
  -> ConsistencyLevel
  -- ^ Read quorum
  -> m (Maybe Column)
getCol cf k cn cl = do
    res <- get cf k (ColNames [encodeCas cn]) cl
    case res of
      [] -> return Nothing
      x:_ -> return $ Just x


------------------------------------------------------------------------------
-- | An arbitrary get operation - slice with 'Selector'
get
  :: (MonadCassandra m)
  => ColumnFamily
  -- ^ in ColumnFamily
  -> ByteString
  -- ^ Row key to get
  -> Selector
  -- ^ Slice columns with selector
  -> ConsistencyLevel
  -> m [Column]
get cf k s cl = withCassandraPool $ \ Cassandra{..} -> do
  res <- wrapException $ C.get_slice (cProto, cProto) k cp (mkPredicate s) cl
  throwing . return $ mapM castColumn res
  where
    cp = T.ColumnParent (Just cf) Nothing


------------------------------------------------------------------------------
-- | Do multiple 'get's in one DB hit
getMulti
  :: (MonadCassandra m)
  => ColumnFamily
  -> KeySelector
  -- ^ A selection of rows to fetch in one hit
  -> Selector
  -- ^ Subject to column selector conditions
  -> ConsistencyLevel
  -> m (Map ByteString Row)
  -- ^ A Map from Row keys to 'Row's is returned
getMulti cf ks s cl = withCassandraPool $ \ Cassandra{..} -> do
  case ks of
    Keys xs -> do
      res <- wrapException $ C.multiget_slice (cProto, cProto) xs cp (mkPredicate s) cl
      return $ M.mapMaybe f res
    KeyRange {} -> do
      res <- wrapException $
        C.get_range_slices (cProto, cProto) cp (mkPredicate s) (mkKeyRange ks) cl
      return $ collectKeySlices res
  where
    collectKeySlices :: [T.KeySlice] -> Map ByteString Row
    collectKeySlices xs = M.fromList $ mapMaybe collectKeySlice xs

    collectKeySlice (T.KeySlice (Just k) (Just xs)) =
      case mapM castColumn xs of
        Left _ -> Nothing
        Right xs' -> Just (k, xs')
    collectKeySlice _ = Nothing

    cp = T.ColumnParent (Just cf) Nothing
    f xs =
      case mapM castColumn xs of
        Left _ -> Nothing
        Right xs' -> Just xs'


------------------------------------------------------------------------------
-- | Insert an entire row into the db.
--
-- This will do as many round-trips as necessary to insert the full
-- row. Please keep in mind that each column and each column of each
-- super-column is sent to the server one by one.
--
-- > insert "testCF" "row1" ONE [packCol ("column key", "some column content")]
insert
  :: (MonadCassandra m)
  => ColumnFamily
  -> ByteString
  -- ^ Row key
  -> ConsistencyLevel
  -> [Column]
  -- ^ best way to make these columns is through "packCol"
  -> m ()
insert cf k cl row = withCassandraPool $ \ Cassandra{..} -> do
  let insCol cp c = do
        c' <- mkThriftCol c
        wrapException $ C.insert (cProto, cProto) k cp c' cl
  forM_ row $ \ c -> do
    case c of
      Column{} -> do
        let cp = T.ColumnParent (Just cf) Nothing
        insCol cp c
      SuperColumn cn cols -> do
        let cp = T.ColumnParent (Just cf) (Just cn)
        mapM_ (insCol cp) cols


-------------------------------------------------------------------------------
-- | Pack key-value pair into 'Column' form ready to be written to Cassandra
packCol :: CasType k => (k, ByteString) -> Column
packCol (k, v) = col (packKey k) v


-------------------------------------------------------------------------------
-- | Unpack a Cassandra 'Column' into a more convenient (k,v) form
unpackCol :: CasType k => Column -> (k, Value)
unpackCol (Column k v _ _) = (decodeCas k, v)
unpackCol _ = error "unpackcol unimplemented for SuperColumn types"


-------------------------------------------------------------------------------
-- | Pack a column key into binary, ready for submission to Cassandra
packKey :: CasType a => a -> ByteString
packKey = encodeCas

------------------------------------------------------------------------------
-- | Delete an entire row, specific columns or a specific sub-set of columns
-- within a SuperColumn.
delete
  ::  (MonadCassandra m)
  => ColumnFamily
  -- ^ In 'ColumnFamily'
  -> Key
  -- ^ Key to be deleted
  -> Selector
  -- ^ Columns to be deleted
  -> ConsistencyLevel
  -> m ()
delete cf k s cl = withCassandraPool $ \ Cassandra {..} -> do
  now <- getTime
  wrapException $ case s of
    All -> C.remove (cProto, cProto) k cpAll now cl
    ColNames cs -> forM_ cs $ \c -> do
      C.remove (cProto, cProto) k (cpCol c) now cl
    SupNames sn cs -> forM_ cs $ \c -> do
      C.remove (cProto, cProto) k (cpSCol sn c) now cl
    Range{} -> error "delete: Range delete not implemented"
  where
    -- wipe out the entire row
    cpAll = T.ColumnPath (Just cf) Nothing Nothing

    -- just a single column
    cpCol name = T.ColumnPath (Just cf) Nothing (Just (encodeCas name))

    -- scope column by supercol
    cpSCol sc name = T.ColumnPath (Just cf) (Just (encodeCas sc)) (Just (encodeCas name))



------------------------------------------------------------------------------
-- | Wrap exceptions of the underlying thrift library into the
-- exception types defined here.
wrapException :: IO a -> IO a
wrapException a = f
    where
      f = a
        `catch` (\ (T.NotFoundException) -> throw NotFoundException)
        `catch` (\ (T.InvalidRequestException e) ->
                  throw . InvalidRequestException $ maybe "" id e)
        `catch` (\ T.UnavailableException -> throw UnavailableException)
        `catch` (\ T.TimedOutException -> throw TimedOutException)
        `catch` (\ (T.AuthenticationException e) ->
                  throw . AuthenticationException $ maybe "" id e)
        `catch` (\ (T.AuthorizationException e) ->
                  throw . AuthorizationException $ maybe "" id e)
        `catch` (\ T.SchemaDisagreementException -> throw SchemaDisagreementException)


-------------------------------------------------------------------------------
-- | Make exceptions implicit.
throwing :: IO (Either CassandraException a) -> IO a
throwing f = do
  res <- f
  case res of
    Left e -> throw e
    Right a -> return a

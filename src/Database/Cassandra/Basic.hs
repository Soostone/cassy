{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternGuards, NamedFieldPuns, RecordWildCards #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE FlexibleContexts #-}


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

    -- * Cassandra Operations
    , getCol
    , get
    , getMulti
    , insert
    , delete

    -- * Filtering
    , Selector(..)
    , Order(..)
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
    , Row
    , ConsistencyLevel(..)

    -- * Helpers
    , CKey (..)
    , packLong
    ) where


-------------------------------------------------------------------------------
import           Control.Applicative
import           Control.Exception
import           Control.Monad
import           Control.Monad.IO.Class
import           Control.Monad.Trans.Reader
import           Data.ByteString.Lazy                       (ByteString)
import           Data.Map                                   (Map)
import qualified Data.Map                                   as M
import           Data.Maybe                                 (mapMaybe)
import qualified Database.Cassandra.Thrift.Cassandra_Client as C
import           Database.Cassandra.Thrift.Cassandra_Types  (ConsistencyLevel (..))
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
withCassandraPool :: MonadCassandra m => (Cassandra -> IO b) -> m b
withCassandraPool f = do
  p <- getCassandraPool
  liftIO $ withResource p f


-------------------------------------------------------------------------------
newtype Cas a = Cas { unCas :: ReaderT CPool IO a }
    deriving (Functor,Applicative,Monad,MonadIO)


-------------------------------------------------------------------------------
-- | Main running function when using the ad-hoc Cas monad. Just write
-- your cassandra actions within the 'Cas' monad and supply them with
-- a 'CPool' to execute.
runCas :: Cas a -> CPool -> IO a
runCas f p = runReaderT (unCas f) p 


-------------------------------------------------------------------------------
instance MonadCassandra Cas where
    getCassandraPool = Cas ask


------------------------------------------------------------------------------
-- | Get a single key-column value.
getCol 
  :: (MonadCassandra m)
  => ColumnFamily 
  -> Key 
  -- ^ Row key
  -> ColumnName
  -- ^ Column/SuperColumn name
  -> ConsistencyLevel 
  -- ^ Read quorum
  -> m (Maybe Column)
getCol cf k cn cl = do
    res <- get cf k (ColNames [cn]) cl
    case res of
      [] -> return Nothing
      x:_ -> return $ Just x


------------------------------------------------------------------------------
-- | An arbitrary get operation - slice with 'Selector'
get 
  :: (MonadCassandra m)
  => ColumnFamily 
  -- ^ in ColumnFamily
  -> Key 
  -- ^ Row key to get
  -> Selector 
  -- ^ Slice columns with selector
  -> ConsistencyLevel 
  -> m Row
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
insert
  :: (MonadCassandra m)
  => ColumnFamily
  -> Key
  -> ConsistencyLevel
  -> Row
  -> m ()
insert cf k cl row = withCassandraPool $ \ Cassandra{..} -> do
  let insCol cp c = do
        c' <- mkThriftCol c
        C.insert (cProto, cProto) k cp c' cl
  forM_ row $ \ c -> do
    case c of
      Column{} -> do
        let cp = T.ColumnParent (Just cf) Nothing
        insCol cp c
      SuperColumn cn cols -> do
        let cp = T.ColumnParent (Just cf) (Just cn)
        mapM_ (insCol cp) cols
    

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
    Range _ _ _ _ -> error "delete: Range delete not implemented"
  where
    -- wipe out the entire row
    cpAll = T.ColumnPath (Just cf) Nothing Nothing

    -- just a single column
    cpCol name = T.ColumnPath (Just cf) Nothing (Just name)

    -- scope column by supercol
    cpSCol sc name = T.ColumnPath (Just cf) (Just sc) (Just name)
  


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
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternGuards, NamedFieldPuns, RecordWildCards #-}


module Database.Cassandra.Basic 

(
  -- * Basic Types
    ColumnFamily(..)
  , Key(..)
  , ColumnName(..)
  , Value(..)
  , Column(..)
  , col
  , Row(..)
  , ConsistencyLevel(..)

  -- * Filtering
  , Selector(..)
  , Order(..)
  , KeySelector(..)
  , KeyRangeType(..)

  -- * Exceptions
  , CassandraException(..)

  -- * Connection
  , CPool
  , Server(..)
  , defServer
  , defServers
  , KeySpace(..)
  , createCassandraPool

  -- * Cassandra Operations
  , getOne
  , get
  , getMulti
  , insert
  , remove

  -- * Utility
  , getTime
) where


import           Control.Exception
import           Control.Monad
import           Data.ByteString.Lazy (ByteString)
import qualified Database.Cassandra.Thrift.Cassandra_Client as C
import qualified Database.Cassandra.Thrift.Cassandra_Types as T
import           Database.Cassandra.Thrift.Cassandra_Types 
                  (ConsistencyLevel(..))
import           Data.Map (Map)
import qualified Data.Map as M
import           Data.Maybe (mapMaybe)
import           Network
import           Prelude hiding (catch)

import           Database.Cassandra.Pool
import           Database.Cassandra.Types


test = do
  pool <- createCassandraPool [("127.0.0.1", PortNumber 9160)] 3 300 "Keyspace1"
  withPool pool $ \ Cassandra{..} -> do
    let cp = T.ColumnParent (Just "CF1") Nothing
    let sr = Just $ T.SliceRange (Just "") (Just "") (Just False) (Just 100)
    let ks = Just ["eben"]
    let sp = T.SlicePredicate Nothing sr
    C.get_slice (cProto, cProto) "darak" cp sp ONE
  get pool "darak" "CF1" All ONE
  getOne pool "CF1" "darak" "eben" ONE
  insert pool "CF1" "test1" ONE [col "col1" "val1", col "col2" "val2"] 
  get pool "test1" "CF1" All ONE >>= putStrLn . show
  remove pool "test1" "CF1" (ColNames ["col2"]) ONE
  get pool "test1" "CF1" (Range Nothing Nothing Reversed 1) ONE >>= putStrLn . show


------------------------------------------------------------------------------
-- | Get a single key-column value
getOne 
  :: CPool
  -> ColumnFamily 
  -> Key 
  -- ^ Row key
  -> ColumnName
  -- ^ Column/SuperColumn name
  -> ConsistencyLevel 
  -- ^ Read quorum
  -> IO (Either CassandraException Column)
getOne p cf k cn cl = do
  c <- get p cf k (ColNames [cn]) cl
  case c of
    Left e -> return $ Left e
    Right [] -> return $ Left NotFoundException
    Right (x:_) -> return $ Right x


------------------------------------------------------------------------------
-- | An arbitrary get operation - slice with 'Selector'
get 
  :: CPool
  -> ColumnFamily 
  -- ^ in ColumnFamily
  -> Key 
  -- ^ Row key to get
  -> Selector 
  -- ^ Slice columns with selector
  -> ConsistencyLevel 
  -> IO (Either CassandraException Row)
get p cf k s cl = withPool p $ \ Cassandra{..} -> do
  res <- wrapException $ C.get_slice (cProto, cProto) k cp (mkPredicate s) cl
  case res of
    Left e -> return $ Left e
    Right xs -> return $ do
      cs <- mapM castColumn xs
      case cs of
        [] -> Left NotFoundException
        _ -> Right $ cs
  where
    cp = T.ColumnParent (Just cf) Nothing


------------------------------------------------------------------------------
-- | Do multiple 'get's in one DB hit
getMulti 
  :: CPool
  -> ColumnFamily 
  -> KeySelector
  -- ^ A selection of rows to fetch in one hit
  -> Selector 
  -- ^ Subject to column selector conditions
  -> ConsistencyLevel 
  -> IO (Either CassandraException (Map ByteString Row))
  -- ^ A Map from Row keys to 'Row's is returned
getMulti p cf ks s cl = withPool p $ \ Cassandra{..} -> do
  case ks of
    Keys xs -> do
      res <- wrapException $ C.multiget_slice (cProto, cProto) xs cp (mkPredicate s) cl
      case res of
        Left e -> return $ Left e
        Right m -> return . Right $ M.mapMaybe f m
    KeyRange {} -> do
      res <- wrapException $ 
        C.get_range_slices (cProto, cProto) cp (mkPredicate s) (mkKeyRange ks) cl
      case res of
        Left e -> return $ Left e
        Right res' -> return . Right $ collectKeySlices res'
  where
    collectKeySlices :: [T.KeySlice] -> Map ByteString Row
    collectKeySlices ks = M.fromList $ mapMaybe collectKeySlice ks
      where 
        f (k, Just x) = True
        f _ = False
        g (k, Just x) = (k,x)


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
-- This will do as many round-trips as necessary to insert the full row.
insert
  :: CPool
  -> ColumnFamily
  -> Key
  -> ConsistencyLevel
  -> Row
  -> IO (Either CassandraException ())
insert p cf k cl row = withPool p $ \ Cassandra{..} -> do
  let iOne c = do
                c' <- mkThriftCol c
                wrapException $ C.insert (cProto, cProto) k cp c' cl 
  res <- sequenceE $ map iOne row
  return $ res >> return ()
  where
    cp = T.ColumnParent (Just cf) Nothing
    

sequenceE :: [IO (Either a b)] -> IO (Either a [b])
sequenceE [] = return (return [])
sequenceE (a:as) = do
  r1 <- a
  rr <- sequenceE as
  return $ liftM2 (:) r1 rr


------------------------------------------------------------------------------
-- | Remove an entire row, specific columns or a specific sub-set of columns
-- within a SuperColumn.
remove 
  ::  CPool
  -> ColumnFamily
  -- ^ In 'ColumnFamily'
  -> Key
  -- ^ Key to be deleted
  -> Selector
  -- ^ Columns to be deleted
  -> ConsistencyLevel
  -> IO (Either CassandraException ())
remove p cf k s cl = withPool p $ \ Cassandra {..} -> do
  now <- getTime
  wrapException $ case s of
    All -> C.remove (cProto, cProto) k cpAll now cl
    ColNames cs -> forM_ cs $ \c -> do
      C.remove (cProto, cProto) k (cpCol c) now cl
    SupNames sn cs -> forM_ cs $ \c -> do
      C.remove (cProto, cProto) k (cpSCol sn c) now cl
  where
    -- wipe out the entire row
    cpAll = T.ColumnPath (Just cf) Nothing Nothing

    -- just a single column
    cpCol name = T.ColumnPath (Just cf) Nothing (Just name)

    -- scope column by supercol
    cpSCol sc name = T.ColumnPath (Just cf) (Just sc) (Just name)
  


------------------------------------------------------------------------------
-- | Wrap exceptions into an explicit type
wrapException :: IO a -> IO (Either CassandraException a)
wrapException a = 
  (a >>= return . Right)
  `catch` (\(T.NotFoundException) -> return $ Left NotFoundException)
  `catch` (\(T.InvalidRequestException e) -> 
            return . Left . InvalidRequestException $ maybe "" id e)
  `catch` (\T.UnavailableException -> return $ Left UnavailableException)
  `catch` (\T.TimedOutException -> return $ Left TimedOutException)
  `catch` (\(T.AuthenticationException e) -> 
            return . Left . AuthenticationException $ maybe "" id e)
  `catch` (\(T.AuthorizationException e) -> 
            return . Left . AuthorizationException $ maybe "" id e)
  `catch` (\T.SchemaDisagreementException -> return $ Left SchemaDisagreementException)

{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternGuards, NamedFieldPuns, RecordWildCards #-}


module Database.Cassandra.ColumnFamily 

(
  -- * Basic Types
    ColumnFamily(..)
  , Key(..)
  , ColumnName(..)
  , Value(..)
  , Column(..)
  , col
  , Row(..)

  -- * Filtering
  , Selector(..)
  , Order(..)

  -- * Exceptions
  , CassandraException(..)

  -- * Connection
  , CPool
  , Server(..)
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
  getOne pool "darak" "eben" "CF1" ONE
  insert pool "test1" [col "col1" "val1", col "col2" "val2"] "CF1" ONE
  get pool "test1" "CF1" All ONE >>= putStrLn . show
  remove pool "test1" "CF1" (ColNames ["col2"]) ONE
  get pool "test1" "CF1" All ONE >>= putStrLn . show


------------------------------------------------------------------------------
-- | Get a single key-column value
getOne 
  ::  CPool
  -> Key 
  -- ^ Row key
  -> ColumnName
  -- ^ Column/SuperColumn name
  -> ColumnFamily 
  -> ConsistencyLevel 
  -> IO (Either CassandraException Column)
getOne p k col cf cl = do
  c <- get p k cf (ColNames [col]) cl
  case c of
    Left e -> return $ Left e
    Right [] -> return $ Left NotFoundException
    Right (x:_) -> return $ Right x


------------------------------------------------------------------------------
-- | An arbitrary get operation - slice with 'Selector'
get 
  ::  CPool
  -> Key 
  -> ColumnFamily 
  -> Selector 
  -> ConsistencyLevel 
  -> IO (Either CassandraException Row)
get p k cf s cl = withPool p $ \ Cassandra{..} -> do
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
  ::  CPool
  -> [Key] 
  -> ColumnFamily 
  -> Selector 
  -> ConsistencyLevel 
  -> IO (Either CassandraException (Map ByteString Row))
getMulti p k cf s cl = withPool p $ \ Cassandra{..} -> do
  res <- wrapException $ C.multiget_slice (cProto, cProto) k cp (mkPredicate s) cl
  case res of
    Left e -> return $ Left e
    Right m -> return . Right $ M.mapMaybe f m
  where
    cp = T.ColumnParent (Just cf) Nothing
    f xs = 
      let
        cs = mapM castColumn xs
      in case cs of
        Left _ -> Nothing
        Right [] -> Nothing
        Right xs' -> Just xs'


------------------------------------------------------------------------------
-- | Insert an entire row into the db.
--
-- This will do as many round-trips as necessary to insert the full row.
insert
  :: CPool
  -> Key
  -> Row
  -> ColumnFamily
  -> ConsistencyLevel
  -> IO (Either CassandraException ())
insert p k row cf cl = withPool p $ \ Cassandra{..} -> do
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
  -> Key
  -- ^ Key to be deleted
  -> ColumnFamily
  -- ^ In 'ColumnFamily'
  -> Selector
  -- ^ Columns to be deleted
  -> ConsistencyLevel
  -> IO (Either CassandraException ())
remove p k cf s cl = withPool p $ \ Cassandra {..} -> do
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

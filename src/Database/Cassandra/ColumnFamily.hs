{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternGuards, NamedFieldPuns, RecordWildCards #-}


module Database.Cassandra.ColumnFamily where

import           Control.Exception
import           Data.ByteString (ByteString)
import qualified Database.Cassandra.Thrift.Cassandra_Client as C
import qualified Database.Cassandra.Thrift.Cassandra_Types as T
import           Database.Cassandra.Thrift.Cassandra_Types 
                  (ConsistencyLevel(..))
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
  get pool ("darak" :: String) "CF1" All ONE
  getOne pool ("darak" :: String) ("eben" :: String) "CF1" ONE


------------------------------------------------------------------------------
-- | Get a single key-column value
getOne 
  :: (BS key) 
  => Pool Cassandra Server
  -> key 
  -- ^ Row key
  -> key
  -- ^ Column/SuperColumn key
  -> ColumnFamily 
  -> ConsistencyLevel 
  -> IO (Either CassandraException Column)
getOne p k col cf cl = do
  c <- get p k cf (ColNames [bs col]) cl
  case c of
    Left e -> return $ Left e
    Right [] -> return $ Left NotFoundException
    Right (x:_) -> return $ Right x


------------------------------------------------------------------------------
-- | An arbitrary get operation - slice with 'Selector'
get 
  :: (BS key) 
  => Pool Cassandra Server
  -> key 
  -> ColumnFamily 
  -> Selector 
  -> ConsistencyLevel 
  -> IO (Either CassandraException Row)
get p k cf s cl = withPool p $ \ Cassandra{..} -> do
  res <- wrapException $ C.get_slice (cProto, cProto) k' cp sp cl
  case res of
    Left e -> return $ Left e
    Right xs -> return $ do
      cs <- mapM castColumn xs
      case cs of
        [] -> Left NotFoundException
        _ -> Right $ cs
  where
    k' = bs k
    cp = T.ColumnParent (Just cf) Nothing
    sp = case s of
      All -> T.SlicePredicate Nothing (Just allRange)
      ColNames ks -> T.SlicePredicate (Just ks) Nothing
      Range st end ord cnt -> 
        T.SlicePredicate Nothing 
          (Just (T.SliceRange st end (Just $ renderOrd ord) (Just cnt)))
    allRange = T.SliceRange (Just "") (Just "") (Just False) (Just 100)


getMulti 
  :: (BS key) 
  => Pool Cassandra Server
  -> [key] 
  -> ColumnFamily 
  -> Selector 
  -> ConsistencyLevel 
  -> IO (Either CassandraException [Row])
getMulti = undefined


insert
  :: Pool Cassandra Server
  -> k
  -> Row
  -> ColumnFamily
  -> ConsistencyLevel
  -> Maybe Int
  -> IO (Either CassandraException Integer)
insert p k cs cf cl ttl = withPool p $ \ Cassandra{..} -> do
  undefined


remove 
  :: (BS k) 
  => Pool Cassandra Server
  -> k
  -> Selector
  -> ColumnFamily
  -> ConsistencyLevel
  -> IO (Either CassandraException Integer)
remove = undefined


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

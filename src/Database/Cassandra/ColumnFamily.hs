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


get 
  :: (BS k) 
  => Pool Cassandra Server
  -> k 
  -> ColumnFamily 
  -> Selector 
  -> ConsistencyLevel 
  -> IO (Either CassandraException [Column])
get p k cf s cl = undefined
  where
    k' = bs k


getMulti 
  :: (BS k) 
  => Pool Cassandra Server
  -> [k] 
  -> ColumnFamily 
  -> Selector 
  -> ConsistencyLevel 
  -> IO (Either CassandraException [Row])
getMulti = undefined


insert
  :: Pool Cassandra Server
  -> k
  -> [Column]
  -> ColumnFamily
  -> ConsistencyLevel
  -> Maybe Int
  -> IO (Either CassandraException Integer)
insert = undefined


remove 
  :: (BS k) 
  => Pool Cassandra Server
  -> k
  -> Selector
  -> ColumnFamily
  -> ConsistencyLevel
  -> IO (Either CassandraException Integer)
remove = undefined


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

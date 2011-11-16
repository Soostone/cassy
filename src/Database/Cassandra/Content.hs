{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternGuards, NamedFieldPuns, RecordWildCards #-}

{-|
    A higher level module for working with Cassandra.

    Serialization and de-serialization of Column values are taken care of
    automatically.

-}

module Database.Cassandra.Content 
( 

  -- * Necessary Types
    Content(..)
  , ModifyOperation(..)

  -- * Cassandra Operations
  , modify
  , modify_

) where

import           Control.Exception
import           Control.Monad
import           Data.ByteString.Lazy.Char8 (ByteString)
import qualified Data.ByteString.Lazy.Char8 as LB
import qualified Data.ByteString.Char8 as B
import qualified Database.Cassandra.Thrift.Cassandra_Client as C
import qualified Database.Cassandra.Thrift.Cassandra_Types as T
import           Database.Cassandra.Thrift.Cassandra_Types 
                  (ConsistencyLevel(..))
import           Data.Map (Map)
import qualified Data.Map as M
import           Network
import           Prelude hiding (catch)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Text.Lazy as LT
import qualified Data.Text.Lazy.Encoding as LT

import           Database.Cassandra.Basic
import           Database.Cassandra.Pool
import           Database.Cassandra.Types


-------------------------------------------------------------------------------
---- Content Typeclass
-------------------------------------------------------------------------------


------------------------------------------------------------------------------
-- | A typeclass for serializing any record to Cassandra
class Content a where
  toBS    :: a -> ByteString
  fromBS  :: ByteString -> Maybe a


instance Content String where
    toBS = LB.pack
    fromBS = Just . LB.unpack

instance Content LT.Text where
    toBS = LT.encodeUtf8
    fromBS = Just . LT.decodeUtf8

instance Content T.Text where
    toBS = toBS . LT.fromChunks . return
    fromBS = fmap (T.concat . LT.toChunks) . fromBS

instance Content B.ByteString where
    toBS = LB.fromChunks . return
    fromBS = fmap (B.concat . LB.toChunks) . fromBS

instance Content ByteString where
    toBS = id
    fromBS = Just . id 


------------------------------------------------------------------------------
-- | Possible outcomes of a modify operation 
data ModifyOperation a = 
    Update a
  | Delete
  | DoNothing
  deriving (Eq,Show,Ord,Read)


------------------------------------------------------------------------------
-- | A modify function that will fetch a specific column, apply modification
-- function on it and save results back to Cassandra.
--
-- A 'b' side value is returned for computational convenience.
--
-- This method may throw a 'CassandraException' for all exceptions other than
-- 'NotFoundException'.
modify
  :: (Content k, Content a)
  => CPool
  -> ColumnFamily
  -> k
  -> ColumnName
  -> ConsistencyLevel
  -- ^ Read quorum
  -> ConsistencyLevel
  -- ^ Write quorum
  -> (Maybe a -> IO (ModifyOperation a, b))
  -- ^ Modification function. Called with 'Just' the value if present,
  -- 'Nothing' otherwise.
  -> IO (ModifyOperation a, b)
modify cp cf k cn rcl wcl f = 
  let
    k' = toBS k
    execF prev = do
      (fres, b) <- f prev
      case fres of
        ares@(Update a) -> do
          insert cp cf k' wcl [col cn (toBS a)]
          return (ares, b)
        ares@(Delete) -> do
          remove cp cf k' (ColNames [cn]) wcl
          return (ares, b)
        ares@(DoNothing) -> return (ares, b)
  in do
    res <- getOne cp cf k' cn rcl
    case res of
      Left NotFoundException -> execF Nothing
      Left e -> throw e
      Right Column{..} -> execF (fromBS colVal)
      Right SuperColumn{..} -> throw $ 
        OperationNotSupported "modify not implemented for SuperColumn"


------------------------------------------------------------------------------
-- | Same as 'modify' but does not offer a side value.
--
-- This method may throw a 'CassandraException' for all exceptions other than
-- 'NotFoundException'.
modify_
  :: (Content k, Content a)
  => CPool
  -> ColumnFamily
  -> k
  -> ColumnName
  -> ConsistencyLevel
  -- ^ Read quorum
  -> ConsistencyLevel
  -- ^ Write quorum
  -> (Maybe a -> IO (ModifyOperation a))
  -- ^ Modification function. Called with 'Just' the value if present,
  -- 'Nothing' otherwise.
  -> IO (ModifyOperation a)
modify_ cp cf k cn rcl wcl f = 
  let
    f' prev = do
      op <- f prev
      return (op, ())
  in do
  (op, _) <- modify cp cf k cn rcl wcl f'
  return op

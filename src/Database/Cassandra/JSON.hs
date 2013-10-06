{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleInstances         #-}
{-# LANGUAGE NamedFieldPuns            #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE PatternGuards             #-}
{-# LANGUAGE RecordWildCards           #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE TypeSynonymInstances      #-}

{-|
    A higher level module for working with Cassandra.


    All row and column keys are standardized to be of strict types.
    Row keys are Text, while Column keys are ByteString. This might change
    in the future and we may revert to entirely ByteString keys.


    Serialization and de-serialization of Column values are taken care of
    automatically using the ToJSON and FromJSON typeclasses.

-}

module Database.Cassandra.JSON
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
    , Cas
    , runCas
    , transCas
    , mapCassandra

    -- * Cassandra Operations
    , get
    , get_
    , getCol
    , getMulti
    , insertCol
    , insertColTTL
    , modify
    , modify_
    , delete

    -- * Necessary Types
    , RowKey
    , ColumnName
    , ModifyOperation (..)
    , ColumnFamily
    , ConsistencyLevel (..)
    , CassandraException (..)

    -- * Filtering
    , Selector (..)
    , range
    , boundless
    , Order(..)
    , reverseOrder
    , KeySelector (..)
    , KeyRangeType (..)

    -- * Helpers
    , CKey (..)
    , fromColKey'

    -- * Cassandra Column Key Types
    , module Database.Cassandra.Pack

    ) where

-------------------------------------------------------------------------------
import           Control.Exception
import           Control.Monad
import           Data.Aeson                 as A
import           Data.Aeson.Parser          (value)
import qualified Data.Attoparsec            as Atto (IResult (..), parse)
import qualified Data.ByteString.Char8      as B
import           Data.ByteString.Lazy.Char8 (ByteString)
import qualified Data.ByteString.Lazy.Char8 as LB
import           Data.Int                   (Int32)
import           Data.Map                   (Map)
import qualified Data.Map                   as M
import           Prelude                    hiding (catch)
-------------------------------------------------------------------------------
import           Database.Cassandra.Basic   hiding (KeySelector (..), delete,
                                             get, getCol, getMulti)
import qualified Database.Cassandra.Basic   as CB
import           Database.Cassandra.Pack
-------------------------------------------------------------------------------



-------------------------------------------------------------------------------
-- | Convert regular column to a key-value pair
col2val :: (FromJSON a, CasType k) => Column -> (k, a)
col2val c = f $ unpackCol c
    where
      f (k, val) = (k, maybe err id $ unMarshallJSON' val)
      err = error "Value can't be parsed from JSON."
col2val _ = error "col2val is not implemented for SuperColumns"



------------------------------------------------------------------------------
-- | Possible outcomes of a modify operation
data ModifyOperation a =
    Update a
  | Delete
  | DoNothing
  deriving (Eq,Show,Ord,Read)


-------------------------------------------------------------------------------
type RowKey = ByteString


------------------------------------------------------------------------------
-- | A modify function that will fetch a specific column, apply modification
-- function on it and save results back to Cassandra.
--
-- A 'b' side value is returned for computational convenience.
--
-- This is intended to be a workhorse function, in that you should be
-- able to do all kinds of relatively straightforward operations just
-- using this function.
--
-- This method may throw a 'CassandraException' for all exceptions other than
-- 'NotFoundException'.
modify
  :: (MonadCassandra m, ToJSON a, FromJSON a, CasType k)
  => ColumnFamily
  -> RowKey
  -> k
  -- ^ Column name; anything in CasType
  -> ConsistencyLevel
  -- ^ Read quorum
  -> ConsistencyLevel
  -- ^ Write quorum
  -> (Maybe a -> m (ModifyOperation a, b))
  -- ^ Modification function. Called with 'Just' the value if present,
  -- 'Nothing' otherwise.
  -> m b
  -- ^ Return the decided 'ModifyOperation' and its execution outcome
modify cf k cn rcl wcl f =
  let
    cn' = encodeCas cn
    execF prev = do
      (fres, b) <- f prev
      case fres of
        Update a  -> insert cf k wcl [col cn' (marshallJSON' a)]
        Delete    -> CB.delete cf k (ColNames [cn']) wcl
        DoNothing -> return ()
      return b
  in do
    res <- CB.getCol cf k cn' rcl
    case res of
      Nothing              -> execF Nothing
      Just Column{..}      -> execF (unMarshallJSON' colVal)
      Just SuperColumn{..} -> throw $
        OperationNotSupported "modify not implemented for SuperColumn"


------------------------------------------------------------------------------
-- | Same as 'modify' but does not offer a side value.
--
-- This method may throw a 'CassandraException' for all exceptions other than
-- 'NotFoundException'.
modify_
  :: (MonadCassandra m, ToJSON a, FromJSON a, CasType k)
  => ColumnFamily
  -> RowKey
  -> k
  -- ^ Column name; anything in CasType
  -> ConsistencyLevel
  -- ^ Read quorum
  -> ConsistencyLevel
  -- ^ Write quorum
  -> (Maybe a -> m (ModifyOperation a))
  -- ^ Modification function. Called with 'Just' the value if present,
  -- 'Nothing' otherwise.
  -> m ()
modify_ cf k cn rcl wcl f =
  let
    f' prev = do
      op <- f prev
      return (op, ())
  in do
      modify cf k cn rcl wcl f'
      return ()


-------------------------------------------------------------------------------
-- Simple insertion function making use of typeclasses
insertCol
    :: (MonadCassandra m, ToJSON a, CasType k)
    => ColumnFamily
    -> RowKey
    -> k
    -- ^ Column name. See 'CasType' for what you can use here.
    -> ConsistencyLevel
    -> a -- ^ Content
    -> m ()
insertCol cf rk cn cl a =
    insert cf rk cl [packCol (cn, marshallJSON' a)]



-------------------------------------------------------------------------------
-- Simple insertion function making use of typeclasses
insertColTTL
    :: (MonadCassandra m, ToJSON a, CasType k)
    => ColumnFamily
    -> RowKey
    -> k
    -- ^ Column name. See 'CasType' for what you can use here.
    -> ConsistencyLevel
    -> a
    -- ^ Content
    -> Int32
    -- ^ TTL for this column
    -> m ()
insertColTTL cf rk cn cl a ttl = insert cf rk cl [column]
    where
      column = Column (packKey cn) (marshallJSON' a) Nothing (Just ttl)


------------------------------------------------------------------------------
-- | An arbitrary get operation - slice with 'Selector'.
--
-- Internally based on Basic.get. Table is assumed to be a regular
-- ColumnFamily and contents of returned columns are cast into the
-- target type.
get
    :: (MonadCassandra m, FromJSON a, CasType k)
    => ColumnFamily
    -> RowKey
    -> Selector
    -- ^ A slice selector
    -> ConsistencyLevel
    -> m [(k, a)]
    -- ^ List of key-value pairs. See 'CasType' for what key types you can use.
get cf k s cl = do
  res <- CB.get cf k s cl
  return $ map col2val res


-------------------------------------------------------------------------------
-- | A version of 'get' that discards the column names for the common
-- scenario. Useful because you would otherwise be forced to manually
-- supply type signatures to get rid of the 'CasType' ambiguity.
get_
    :: (MonadCassandra m, FromJSON a)
    => ColumnFamily
    -> RowKey
    -> Selector
    -- ^ A slice selector
    -> ConsistencyLevel
    -> m [a]
get_ cf k s cl = do
    (res :: [(LB.ByteString, a)]) <- get cf k s cl
    return $ map snd res


-------------------------------------------------------------------------------
data KeySelector
    = Keys [RowKey]
    | KeyRange KeyRangeType RowKey RowKey Int32


-------------------------------------------------------------------------------
ksToBasicKS :: KeySelector -> CB.KeySelector
ksToBasicKS (Keys k) = CB.Keys $ map toColKey k
ksToBasicKS (KeyRange ty fr to i) = CB.KeyRange ty (toColKey fr) (toColKey to) i


-------------------------------------------------------------------------------
-- | Get a slice of columns from multiple rows at once. Note that
-- since we are auto-serializing from JSON, all the columns must be of
-- the same data type.
getMulti
    :: (MonadCassandra m, FromJSON a)
    => ColumnFamily
    -> KeySelector
    -> Selector
    -> ConsistencyLevel
    -> m (Map RowKey [(ColumnName, a)])
getMulti cf ks s cl = do
  res <- CB.getMulti cf (ksToBasicKS ks) s cl
  return . M.fromList . map conv . M.toList $ res
  where
    conv (k, row) = (k, map col2val row)


-------------------------------------------------------------------------------
-- | Get a single column from a single row
getCol
    :: (MonadCassandra m, FromJSON a, CasType k)
    => ColumnFamily
    -> RowKey
    -> k
    -- ^ Column name; anything in 'CasType'
    -> ConsistencyLevel
    -> m (Maybe a)
getCol cf rk ck cl = do
    res <- CB.getCol cf rk (encodeCas ck) cl
    case res of
      Nothing -> return Nothing
      Just res' -> do
          let (_ :: ByteString, x) = col2val res'
          return $ Just x


------------------------------------------------------------------------------
-- | Same as the 'delete' in the 'Cassandra.Basic' module, except that
-- it throws an exception rather than returning an explicit Either
-- value.
delete
  :: (MonadCassandra m)
  =>ColumnFamily
  -- ^ In 'ColumnFamily'
  -> RowKey
  -- ^ Key to be deleted
  -> Selector
  -- ^ Columns to be deleted
  -> ConsistencyLevel
  -> m ()
delete cf k s cl = CB.delete cf k s cl


------------------------------------------------------------------------------
-- | Lazy 'marshallJSON'
marshallJSON' :: ToJSON a => a -> ByteString
marshallJSON' = LB.fromChunks . return . marshallJSON


------------------------------------------------------------------------------
-- | Encode JSON
marshallJSON :: ToJSON a => a -> B.ByteString
marshallJSON = B.concat . LB.toChunks . A.encode


------------------------------------------------------------------------------
-- | Lazy 'unMarshallJSON'
unMarshallJSON' :: FromJSON a => ByteString -> Maybe a
unMarshallJSON' = unMarshallJSON . B.concat . LB.toChunks


------------------------------------------------------------------------------
-- | Decode JSON
unMarshallJSON :: FromJSON a => B.ByteString -> Maybe a
unMarshallJSON = pJson
  where
    pJson bs = val
      where
        js = Atto.parse value bs
        val = case js of
          Atto.Done _ r -> case fromJSON r of
            Error e -> error $ "JSON err: " ++ show e
            Success a -> a
          _ -> Nothing

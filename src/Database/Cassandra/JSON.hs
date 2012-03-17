{-# LANGUAGE PatternGuards, 
             NamedFieldPuns, 
             OverloadedStrings,
             TypeSynonymInstances, 
             ScopedTypeVariables,
             FlexibleInstances,
             ExistentialQuantification,
             RecordWildCards #-}

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
    , Server (..)
    , defServer
    , defServers
    , KeySpace (..)
    , createCassandraPool

    -- * MonadCassandra Typeclass
    , MonadCassandra (..)
    , Cas (..)
    , runCas

    -- * Cassandra Operations
    , get
    , getCol  
    , getMulti
    , insertCol
    , modify
    , modify_
    , delete

    -- * Necessary Types
    , RowKey
    , ColumnName
    , ModifyOperation (..)
    , ColumnFamily (..)
    , ConsistencyLevel (..)
    , CassandraException (..)
    , Selector (..)
    , KeySelector (..)
    , KeyRangeType (..)
    
    -- * Helpers
    , CKey (..)
    , packLong
    
    ) where

-------------------------------------------------------------------------------
import           Control.Exception
import           Control.Monad
import           Data.Aeson                 as A
import           Data.Aeson.Parser          (value)
import qualified Data.Attoparsec            as Atto (IResult(..), parse)
import qualified Data.ByteString.Char8      as B
import           Data.ByteString.Lazy.Char8 (ByteString)
import qualified Data.ByteString.Lazy.Char8 as LB
import           Data.Int                   (Int32, Int64)
import           Data.Map                   (Map)
import qualified Data.Map                   as M
import qualified Data.Text                  as T
import           Data.Text                  (Text)
import qualified Data.Text.Encoding         as T
import qualified Data.Text.Lazy             as LT
import qualified Data.Text.Lazy.Encoding    as LT
import           Network
import           Prelude                    hiding (catch)
-------------------------------------------------------------------------------
import           Database.Cassandra.Basic   hiding (get, getCol, delete, KeySelector (..), 
                                                    getMulti)
import qualified Database.Cassandra.Basic   as CB
import           Database.Cassandra.Pool
import           Database.Cassandra.Types   hiding (KeySelector (..), ColumnName)
import           Database.Cassandra.Pack
-------------------------------------------------------------------------------



-------------------------------------------------------------------------------
-- | Convert regular column to a key-value pair
col2val :: (FromJSON a) => Column -> (ColumnName, a)
col2val (Column nm val _ _) =  (fromBS nm, maybe err id $ unMarshallJSON' val)
    where err = error "Value can't be parsed from JSON."
col2val _ = error "col2val is not implemented for SuperColumns"



------------------------------------------------------------------------------
-- | Possible outcomes of a modify operation 
data ModifyOperation a = 
    Update a
  | Delete
  | DoNothing
  deriving (Eq,Show,Ord,Read)


-------------------------------------------------------------------------------
type RowKey = Text


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
  :: (MonadCassandra m, ToJSON a, FromJSON a)
  => ColumnFamily
  -> RowKey
  -> ColumnName
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
    k' = toBS k
    cn' = toBS cn
    execF prev = do
      (fres, b) <- f prev
      case fres of
        (Update a) ->
          insert cf k' wcl [col cn' (marshallJSON' a)]
        (Delete) ->
          CB.delete cf k' (ColNames [cn']) wcl
        (DoNothing) -> return ()
      return b
  in do
    res <- CB.getCol cf k' cn' rcl
    case res of
      Nothing -> execF Nothing
      Just Column{..} -> execF (unMarshallJSON' colVal)
      Just SuperColumn{..} -> throw $ 
        OperationNotSupported "modify not implemented for SuperColumn"


------------------------------------------------------------------------------
-- | Same as 'modify' but does not offer a side value.
--
-- This method may throw a 'CassandraException' for all exceptions other than
-- 'NotFoundException'.
modify_
  :: (MonadCassandra m, ToJSON a, FromJSON a)
  => ColumnFamily
  -> RowKey
  -> ColumnName
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
    :: (MonadCassandra m, ToJSON a)
    => ColumnFamily 
    -> RowKey
    -> ColumnName
    -> ConsistencyLevel
    -> a -- ^ Content
    -> m ()
insertCol cf k cn cl a = 
    insert cf (toBS k) cl [col (toBS cn) (marshallJSON' a)]


------------------------------------------------------------------------------
-- | An arbitrary get operation - slice with 'Selector'.
--
-- Internally based on Basic.get. Table is assumed to be a regular
-- ColumnFamily and contents of returned columns are cast into the
-- target type.
get
    :: (MonadCassandra m, FromJSON a)
    => ColumnFamily
    -> RowKey
    -> Selector
    -> ConsistencyLevel
    -> m [(ColumnName, a)]
get cf k s cl = do
  res <- CB.get cf (toBS k) s cl
  return $ map col2val res


-------------------------------------------------------------------------------
data KeySelector 
    = Keys [RowKey]
    | KeyRange KeyRangeType RowKey RowKey Int32


-------------------------------------------------------------------------------
ksToBasicKS (Keys k) = CB.Keys $ map toBS k
ksToBasicKS (KeyRange ty fr to i) = CB.KeyRange ty (toBS fr) (toBS to) i


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
    conv (k, row) = (fromBS k, map col2val row)
  

-------------------------------------------------------------------------------
-- | Get a single column from a single row
getCol
    :: (MonadCassandra m, FromJSON a)
    => ColumnFamily
    -> RowKey
    -> ColumnName
    -> ConsistencyLevel
    -> m (Maybe a)
getCol cf rk ck cl = do
    res <- CB.getCol cf (toBS rk) (toBS ck) cl 
    case res of
      Nothing -> return Nothing
      Just res' -> do
          let (_, x) = col2val res'
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
delete cf k s cl = CB.delete cf (toBS k) s cl


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

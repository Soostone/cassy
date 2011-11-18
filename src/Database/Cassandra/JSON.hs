{-# LANGUAGE PatternGuards, 
             NamedFieldPuns, 
             OverloadedStrings,
             TypeSynonymInstances, 
             RecordWildCards #-}

{-|
    A higher level module for working with Cassandra.
    
    Row and Column keys can be any string-like type implementing the
    CKey typeclass. You can add your own types by defining new instances

    Serialization and de-serialization of Column values are taken care of
    automatically using the ToJSON and FromJSON typeclasses.
    
    Also, this module currently attempts to reduce verbosity by
    throwing errors instead of returning Either types as in the
    'Database.Cassandra.Basic' module.

-}

module Database.Cassandra.JSON 
( 
  
  -- * Connection
    CPool
  , Server(..)
  , defServer
  , defServers
  , KeySpace(..)
  , createCassandraPool


  -- * Necessary Types
  , CKey(..)
  , ModifyOperation(..)
  , ColumnFamily(..)
  , ConsistencyLevel(..)

  -- * Cassandra Operations
  , insertCol
  , modify
  , modify_

) where

import           Control.Exception
import           Control.Monad
import           Data.Aeson as A
import qualified Data.Attoparsec                            as Atto (Result(..), parse)
import qualified Data.ByteString.Char8                      as B
import           Data.ByteString.Lazy.Char8                 (ByteString)
import qualified Data.ByteString.Lazy.Char8                 as LB
import           Data.Map                                   (Map)
import qualified Data.Map                                   as M
import           Network
import           Prelude                                    hiding (catch)
import qualified Data.Text                                  as T
import qualified Data.Text.Encoding                         as T
import qualified Data.Text.Lazy                             as LT
import qualified Data.Text.Lazy.Encoding                    as LT

import           Database.Cassandra.Basic
import qualified Database.Cassandra.Basic as CB
import           Database.Cassandra.Pool
import           Database.Cassandra.Types


-------------------------------------------------------------------------------
---- CKey Typeclass
-------------------------------------------------------------------------------


------------------------------------------------------------------------------
-- | A typeclass to enable using any string-like type for row and column keys
class CKey a where
  toBS    :: a -> ByteString

instance CKey String where
    toBS = LB.pack

instance CKey LT.Text where
    toBS = LT.encodeUtf8

instance CKey T.Text where
    toBS = toBS . LT.fromChunks . return

instance CKey B.ByteString where
    toBS = LB.fromChunks . return

instance CKey ByteString where
    toBS = id



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
-- This is intended to be a workhorse function, in that you should be
-- able to do all kinds of relatively straightforward operations just
-- using this function.
--
-- This method may throw a 'CassandraException' for all exceptions other than
-- 'NotFoundException'.
modify
  :: (CKey k, ToJSON a, FromJSON a)
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
  -> IO b
  -- ^ Return the decided 'ModifyOperation' and its execution outcome
modify cp cf k cn rcl wcl f = 
  let
    k' = toBS k
    execF prev = do
      (fres, b) <- f prev
      dbres <- case fres of
        (Update a) ->
          insert cp cf k' wcl [col cn (marshallJSON' a)]
        (Delete) ->
          remove cp cf k' (ColNames [cn]) wcl
        (DoNothing) -> return $ Right ()
      case dbres of
        Left e -> throw e -- Modify op returned error; throw it
        Right _ -> return b
  in do
    res <- getOne cp cf k' cn rcl
    case res of
      Left NotFoundException -> execF Nothing
      Left e -> throw e
      Right Column{..} -> execF (unMarshallJSON' colVal)
      Right SuperColumn{..} -> throw $ 
        OperationNotSupported "modify not implemented for SuperColumn"


------------------------------------------------------------------------------
-- | Same as 'modify' but does not offer a side value.
--
-- This method may throw a 'CassandraException' for all exceptions other than
-- 'NotFoundException'.
modify_
  :: (CKey k, ToJSON a, FromJSON a)
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
  -> IO ()
modify_ cp cf k cn rcl wcl f = 
  let
    f' prev = do
      op <- f prev
      return (op, ())
  in do
  modify cp cf k cn rcl wcl f'
  return ()


-------------------------------------------------------------------------------
-- Simple insertion function making use of typeclasses
insertCol
    :: (CKey k, CKey columnName, ToJSON a)
    => CPool -> ColumnFamily 
    -> k -- ^ Row Key
    -> columnName
    -> ConsistencyLevel
    -> a -- ^ Content
    -> IO (Either CassandraException ())
insertCol cp cf k cn cl a = insert cp cf (toBS k) cl [col (toBS cn) (marshallJSON' a)]


------------------------------------------------------------------------------
-- | An arbitrary get operation - slice with 'Selector'.
--
-- Internally based on Basic.get. Table is assumed to be a regular
-- ColumnFamily and contents of returned columns are cast into the
-- target type.
get
    :: (CKey k, FromJSON a)
    => CPool -> ColumnFamily
    -> k
    -> Selector
   -> ConsistencyLevel
    -> IO [(ColumnName, Maybe a)]
get cp cf k s cl = 
  let conv (Column nm val _ _) = (nm, unMarshallJSON' val)
  in do
    res <- throwing $ CB.get cp cf (toBS k) s cl
    return $ map conv res
  
    

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
        js = Atto.parse json bs
        val = case js of
          Atto.Done _ r -> case fromJSON r of
            Error e -> error $ "JSON err: " ++ show e
            Success a -> a
          _ -> Nothing

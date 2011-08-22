{-# LANGUAGE PatternGuards, NamedFieldPuns, RecordWildCards #-}


module Database.Cassandra.Pool where



import Control.Applicative ((<$>))
import Control.Concurrent.STM
import Control.Exception (SomeException, catch, onException)
import Control.Monad (forM_, forever, join, liftM2, unless, when)
import Control.Monad.IO.Class (liftIO)
import Data.ByteString (ByteString)
import Data.List (partition)
import Data.Time.Clock (NominalDiffTime, UTCTime, diffUTCTime, getCurrentTime)
import Prelude hiding (catch)
import System.Mem.Weak (addFinalizer)
import System.IO (hClose, Handle(..))


import qualified Database.Cassandra.Thrift.Cassandra_Client as C
import Thrift.Transport
import Thrift.Transport.Handle
import Thrift.Transport.Framed
import Thrift.Protocol.Binary
import Network


type Server = (HostName, PortID)


data Cassandra = Cassandra {
    cHandle :: Handle
  , cFramed :: FramedTransport Handle
  , cProto :: BinaryProtocol (FramedTransport Handle)
}


createCassandraPool 
  :: [Server]
  -> Int
  -> NominalDiffTime
  -> String
  -> IO (Pool Cassandra Server)
createCassandraPool servers n maxIdle ks = createPool cr dest n maxIdle servers
  where
    cr :: Server -> IO Cassandra
    cr (host, p) = do
      h <- hOpen (host, p)
      ft <- openFramedTransport h
      let p = BinaryProtocol ft
      C.set_keyspace (p,p) ks
      return $ Cassandra h ft p
    dest h = hClose $ cHandle h


newtype Pool a s = Pool { stripes :: TVar (Ring (Stripe a s)) }


createPool cr dest n maxIdle servers = do
  when (maxIdle < 0.5) $
    modError "pool " $ "invalid idle time " ++ show maxIdle
  when (n < 1) $
    modError "pool " $ "invalid maximum resource count " ++ show n
  stripes' <- mapM (createStripe cr dest n maxIdle) servers
  -- reaperId <- forkIO $ reaper destroy idleTime localPools
  -- addFinalizer p $ killThread reaperId
  tv <- atomically $ newTVar (mkRing stripes')
  return $ Pool tv



withPool :: Pool a s -> (a -> IO b) -> IO b
withPool Pool{..} f = do
  Ring{..} <- atomically $ do
    r <- readTVar stripes
    writeTVar stripes $ next r
    return r
  withStripe current f


data Ring a = Ring {
    current :: !a
  , used :: [a]
  , upcoming :: [a]
  }


mkRing [] = error "Can't make a ring from empty list"
mkRing (a:as) = Ring a [] as


next :: Ring a -> Ring a
next Ring{..} 
  | (n:rest) <- upcoming
  = Ring n (current : used) rest
next Ring{..} 
  | (n:rest) <- reverse (current : used)
  = Ring n [] rest


data Stripe a s = Stripe {
    idle :: TVar [Connection a]
  -- ^ FIFO buffer of idle connections
  , inUse :: TVar Int
  -- ^ Set of in-use connections
  , server :: s
  -- ^ Server this strip is connected to
  , create :: s -> IO a
  -- ^ Create action
  , destroy :: (a -> IO ())
  -- ^ Destroy action
  , cxns :: Int
  -- ^ Max connections
  , ttl :: NominalDiffTime
  -- ^ TTL for each connection
  }


createStripe 
  :: (s -> IO a)
  -> (a -> IO ())
  -> Int
  -> NominalDiffTime
  -> s
  -> IO (Stripe a s)
createStripe cr dest n maxIdle s = atomically $ do
  idles <- newTVar []
  used <- newTVar 0
  return $ Stripe {
    idle = idles
  , inUse = used
  , server = s
  , create = cr
  , destroy = dest
  , cxns = n
  , ttl = maxIdle
  }


withStripe :: Stripe a s -> (a -> IO b) -> IO b
withStripe Stripe{..} f = do
  res <- join . atomically $ do
    cs <- readTVar idle
    case cs of
      (Connection{..}:rest) -> writeTVar idle rest >> return (return cxn)
      [] -> do
        used <- readTVar inUse
        when (used == cxns) retry
        writeTVar inUse $! used + 1
        return $ create server 
          `onException` atomically (modifyTVar_ inUse (subtract 1))
  ret <- f res `onException` (destroy res `onException` return ())
  now <- getCurrentTime
  atomically $ modifyTVar_ idle (Connection res now : ) 
  return ret



data Connection a = Connection {
    cxn :: a
  , lastUse :: UTCTime
  }



modifyTVar_ :: TVar a -> (a -> a) -> STM ()
modifyTVar_ v f = readTVar v >>= \a -> writeTVar v $! f a


modError :: String -> String -> a
modError func msg =
    error $ "Data.Pool." ++ func ++ ": " ++ msg



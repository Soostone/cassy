{-# LANGUAGE NamedFieldPuns  #-}
{-# LANGUAGE PackageImports  #-}
{-# LANGUAGE PatternGuards   #-}
{-# LANGUAGE RecordWildCards #-}

module Database.Cassandra.Pool
    ( CPool
    , Server
    , defServer
    , defServers
    , KeySpace
    , Cassandra (..)
    , createCassandraPool
    , withResource
    ) where

-------------------------------------------------------------------------------
import           Control.Applicative                        ((<$>))
import           Control.Arrow
import           Control.Concurrent
import           Control.Concurrent.STM
import           Control.Exception                          (SomeException, handle, onException)
import           Control.Monad                              (forM_, forever, join, liftM2, unless, when)
import           Data.ByteString                            (ByteString)
import           Data.List                                  (find, partition)
import           Data.Maybe
import           Data.Pool
import           Data.Time.Clock                            (NominalDiffTime, UTCTime, diffUTCTime, getCurrentTime)
import qualified Database.Cassandra.Thrift.Cassandra_Client as C
import qualified Database.Cassandra.Thrift.Cassandra_Types  as C
import           Network
import           Prelude                                    hiding (catch)
import           System.IO                                  (Handle(..), hClose)
import           System.Mem.Weak                            (addFinalizer)
import           Thrift.Protocol.Binary
import           Thrift.Transport
import           Thrift.Transport.Framed
import           Thrift.Transport.Handle
-------------------------------------------------------------------------------


------------------------------------------------------------------------------
-- | A round-robin pool of cassandra connections
type CPool = Pool Cassandra


-------------------------------------------------------------------------------
-- | A (ServerName, Port) tuple
type Server = (HostName, Int)


-- | A localhost server with default configuration
defServer :: Server
defServer = ("127.0.0.1", 9160)


-- | A single localhost server with default configuration
defServers :: [Server]
defServers = [defServer]


-------------------------------------------------------------------------------
type KeySpace = String


-------------------------------------------------------------------------------
data Cassandra = Cassandra {
    cHandle :: Handle
  , cFramed :: FramedTransport Handle
  , cProto :: BinaryProtocol (FramedTransport Handle)
}



-- | Create a pool of connections to a cluster of Cassandra boxes
--
-- Each box in the cluster will get up to n connections. The pool will send
-- queries in round-robin fashion to balance load on each box in the cluster.
createCassandraPool
  :: [Server]
  -- ^ List of servers to connect to
  -> Int
  -- ^ Number of stripes to maintain
  -> Int
  -- ^ Max connections per stripe
  -> NominalDiffTime
  -- ^ Kill each connection after this many seconds
  -> KeySpace
  -- ^ Each pool operates on a single KeySpace
  -> IO CPool
createCassandraPool servers numStripes perStripe maxIdle ks = do
    sring <- newTVarIO $ mkRing servers
    pool <- createPool (cr sring) dest numStripes maxIdle perStripe
    forkIO (serverDiscoveryThread sring ks pool)
    return pool
  where
    cr :: ServerRing -> IO Cassandra
    cr sring = handle (handler sring) $ do
      (host, p) <- atomically $ do
        ring@Ring{..} <- readTVar sring
        writeTVar sring (next ring)
        return current

      h <- hOpen (host, PortNumber (fromIntegral p))
      ft <- openFramedTransport h
      let p = BinaryProtocol ft
      C.set_keyspace (p,p) ks

      return $ Cassandra h ft p

    handler :: ServerRing -> SomeException -> IO Cassandra
    handler sring e = do
      modifyServers sring removeCurrent
      cr sring

    dest h = hClose $ cHandle h


-------------------------------------------------------------------------------
modifyServers :: TVar (Ring a) -> (Ring a -> Ring a) -> IO ()
modifyServers sring f = atomically $ do
    ring@Ring{..} <- readTVar sring
    writeTVar sring $ f ring
    return ()


-------------------------------------------------------------------------------
serverDiscoveryThread :: TVar (Ring Server)
                      -> String
                      -> Pool Cassandra
                      -> IO b
serverDiscoveryThread sring ks pool = forever $ do
    threadDelay 5000000
    withResource pool (updateServers sring ks)


-------------------------------------------------------------------------------
updateServers :: TVar (Ring Server) -> String -> Cassandra -> IO ()
updateServers sring ks (Cassandra _ _ p) = do
    ranges <- C.describe_ring (p,p) ks
    let hosts = concat $ catMaybes $ map C.f_TokenRange_endpoints ranges
        servers = map (\e -> first (const e) defServer) hosts
    putStrLn $ "Cassy: Discovered new servers: " ++ show servers
    modifyServers sring (addNewServers servers)


-------------------------------------------------------------------------------
type ServerRing = TVar (Ring Server)


-------------------------------------------------------------------------------
data Ring a = Ring {
    current :: !a
  , used :: [a]
  , upcoming :: [a]
  }


-------------------------------------------------------------------------------
mkRing [] = error "Can't make a ring from empty list"
mkRing (a:as) = Ring a [] as


-------------------------------------------------------------------------------
next :: Ring a -> Ring a
next Ring{..}
  | (n:rest) <- upcoming
  = Ring n (current : used) rest
next Ring{..}
  | (n:rest) <- reverse (current : used)
  = Ring n [] rest

-------------------------------------------------------------------------------
removeCurrent :: Ring a -> Ring a
removeCurrent Ring{..}
  | (n:rest) <- upcoming
  = Ring n used rest
removeCurrent Ring{..}
  | (n:rest) <- reverse used
  = Ring n [] rest

-------------------------------------------------------------------------------
addNewServers :: [Server] -> Ring Server -> Ring Server
addNewServers servers Ring{..} = Ring current used (new++upcoming)
  where
    new = filter (not . existing) servers
    existing s = isJust (find (\a -> fst s == fst a) used)
              || isJust (find (\a -> fst s == fst a) upcoming)
              || (fst s == fst current)



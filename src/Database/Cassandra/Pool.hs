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

------------------------------------------------------------------------------
import           Control.Applicative                        ((<$>))
import           Control.Arrow
import           Control.Concurrent
import           Control.Concurrent.STM
import           Control.Exception                          (SomeException, handle, onException)
import           Control.Monad                              (forM_, forever, join, liftM2, unless, when)
import           Data.ByteString                            (ByteString)
import           Data.List                                  (find, nub, partition)
import           Data.Maybe
import           Data.Pool
import           Data.Set                                   (Set)
import qualified Data.Set                                   as S
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
------------------------------------------------------------------------------


------------------------------------------------------------------------------
-- | A round-robin pool of cassandra connections
type CPool = Pool Cassandra


------------------------------------------------------------------------------
-- | A (ServerName, Port) tuple
type Server = (HostName, Int)


-- | A localhost server with default configuration
defServer :: Server
defServer = ("127.0.0.1", 9160)


-- | A single localhost server with default configuration
defServers :: [Server]
defServers = [defServer]


------------------------------------------------------------------------------
type KeySpace = String


------------------------------------------------------------------------------
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
    cr sring = do
      s@(host, p) <- atomically $ do
        ring@Ring{..} <- readTVar sring
        writeTVar sring (next ring)
        return current

      handle (handler sring s) $ do
        h <- hOpen (host, PortNumber (fromIntegral p))
        ft <- openFramedTransport h
        let p = BinaryProtocol ft
        C.set_keyspace (p,p) ks
        return $ Cassandra h ft p

    handler :: ServerRing -> Server -> SomeException -> IO Cassandra
    handler sring server e = do
      modifyServers sring (removeServer server)
      cr sring

    dest h = hClose $ cHandle h


------------------------------------------------------------------------------
modifyServers :: TVar (Ring a) -> (Ring a -> Ring a) -> IO ()
modifyServers sring f = atomically $ do
    ring@Ring{..} <- readTVar sring
    writeTVar sring $ f ring
    return ()


------------------------------------------------------------------------------
serverDiscoveryThread :: TVar (Ring Server)
                      -> String
                      -> Pool Cassandra
                      -> IO b
serverDiscoveryThread sring ks pool = forever $ do
    withResource pool (updateServers sring ks)
    threadDelay 60000000


------------------------------------------------------------------------------
updateServers :: TVar (Ring Server) -> String -> Cassandra -> IO ()
updateServers sring ks (Cassandra _ _ p) = do
    ranges <- C.describe_ring (p,p) ks
    let hosts = concat $ catMaybes $ map C.f_TokenRange_endpoints ranges
        servers = nub $ map (\e -> first (const e) defServer) hosts
    -- putStrLn $ "Cassy: Discovered new servers: " ++ show servers
    modifyServers sring (addNewServers servers)


------------------------------------------------------------------------------
type ServerRing = TVar (Ring Server)


------------------------------------------------------------------------------
data Ring a = Ring {
    allItems :: Set a
  , current :: !a
  , used :: [a]
  , upcoming :: [a]
  }


------------------------------------------------------------------------------
mkRing [] = error "Can't make a ring from empty list"
mkRing (a:as) = Ring S.empty a [] as


------------------------------------------------------------------------------
next :: Ring a -> Ring a
next Ring{..}
  | (n:rest) <- upcoming
  = Ring allItems n (current : used) rest
next Ring{..}
  | (n:rest) <- reverse (current : used)
  = Ring allItems n [] rest


------------------------------------------------------------------------------
removeServer :: Ord a => a -> Ring a -> Ring a
removeServer s r@Ring{..}
  | s `S.member` allItems = Ring (S.delete s allItems) current' used' upcoming'
  | otherwise             = r
  where
    used' = filter (/=s) used
    (current':upcoming') = filter (/=s) (current:upcoming)


------------------------------------------------------------------------------
addNewServers :: [Server] -> Ring Server -> Ring Server
addNewServers servers Ring{..} = Ring all' current' used' (new ++ upcoming')
  where
    all' = S.fromList servers
    new = S.toList $ all' S.\\ allItems
    used' = filter (`S.member` all') used
    (current':upcoming') = filter (`S.member` all') (current:upcoming)



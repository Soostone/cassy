{-# LANGUAGE PatternGuards, NamedFieldPuns, RecordWildCards #-}
{-# LANGUAGE PackageImports #-}

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
import Control.Applicative ((<$>))
import Control.Concurrent.STM
import Control.Exception (SomeException, catch, onException)
import Control.Monad (forM_, forever, join, liftM2, unless, when)
import Data.ByteString (ByteString)
import Data.List (partition)
import "resource-pool" Data.Pool
import Data.Time.Clock (NominalDiffTime, UTCTime, diffUTCTime, getCurrentTime)
import qualified Database.Cassandra.Thrift.Cassandra_Client as C
import Network
import Prelude hiding (catch)
import System.IO (hClose, Handle(..))
import System.Mem.Weak (addFinalizer)
import Thrift.Protocol.Binary
import Thrift.Transport
import Thrift.Transport.Framed
import Thrift.Transport.Handle
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
    createPool (cr sring) dest numStripes maxIdle perStripe
  where
    cr :: ServerRing -> IO Cassandra
    cr sring = do
      server <- atomically $ do
        ring@Ring{..} <- readTVar sring
        writeTVar sring $ next ring
        return current
      crCon server
        
    crCon :: Server -> IO Cassandra
    crCon (host, p) = do
      h <- hOpen (host, PortNumber (fromIntegral p))
      ft <- openFramedTransport h
      let p = BinaryProtocol ft
      C.set_keyspace (p,p) ks
      return $ Cassandra h ft p
    dest h = hClose $ cHandle h



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


module Database.Cassandra.Types where

import           Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy.Char8 as LB
import           Data.Int (Int32)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import           Data.Time


type ColumnFamily = ByteString


-- | A column selector/filter statement for queries.
--
-- Remember that SuperColumns are always fully deserialized, so we don't offer
-- a way to filter columns within a 'SuperColumn'
data Selector = 
    All
  -- ^ Return everything in Row
  | ColNames [ByteString]
  -- ^ Return specific columns or super-columns.
  | Range (Maybe ByteString) (Maybe ByteString) Order Int32
  -- ^ Return a range of columns or super-columns.


data Order = Regular | Reversed


data Column = 
    SuperColumn {
      scName :: ByteString
    , scColumns :: [Column]
    }
  | Column {
      colKey :: ByteString
    , colVal :: ByteString
    , colTS :: UTCTime
    }
  deriving (Eq,Show,Read,Ord)


data Row = Row {
	  rKey :: ByteString
	, rColumns :: [Column]
} deriving (Eq,Show,Read,Ord)


data CassandraException = 
    NotFoundException
  | InvalidRequestException String
  | UnavailableException
  | TimedOutException
  | AuthenticationException String
  | AuthorizationException String
  | SchemaDisagreementException
  deriving (Eq,Show,Read,Ord)



------------------------------------------------------------------------------
-- | Cassandra keys need to be 'ByteString's
class (Ord a) => BS a where
  bs :: a -> ByteString

instance BS String where
  bs = B.pack 

instance BS ByteString where
  bs = id 
 
instance BS LB.ByteString where
  bs = B.concat . LB.toChunks

instance BS T.Text where
  bs = T.encodeUtf8

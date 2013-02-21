{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE RecordWildCards            #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE StandaloneDeriving         #-}
{-# LANGUAGE TemplateHaskell            #-}

module Main where


-------------------------------------------------------------------------------
import           Control.Applicative
import qualified Data.ByteString.Char8                      as B
import qualified Data.ByteString.Lazy.Char8                 as LB
import           Data.DeriveTH
import qualified Data.Map                                   as M
import qualified Data.Text                                  as T
import qualified Database.Cassandra.Thrift.Cassandra_Client as C
import           Database.Cassandra.Thrift.Cassandra_Types 
                                                             (ConsistencyLevel (..))
import           Database.Cassandra.Thrift.Cassandra_Types  as T
import           System.IO.Unsafe
import           Test.Framework                             (defaultMain,
                                                             testGroup)
import           Test.Framework.Providers.HUnit
import           Test.Framework.Providers.QuickCheck2       (testProperty)
import           Test.HUnit
import           Test.QuickCheck
import           Test.QuickCheck.Property

-------------------------------------------------------------------------------
import           Database.Cassandra.Basic
import           Database.Cassandra.Pack
import           Database.Cassandra.Pool
-------------------------------------------------------------------------------



main = do
    pool <- mkTestConn
    defaultMain $ tests pool


tests pool = [testGroup "packTests" (packTests pool)]


packTests pool =
    [ testProperty "cas type marshalling long" prop_casTypeLong
    , testProperty "cas type marshalling ascii" prop_casTypeAscii
    , testProperty "cas type marshalling ascii" prop_casTypeInt32
    , testProperty "cas type marshalling composite" prop_casTypeComp
    , testCase "cas live test - composite get/set" test_composite_col
    , testCase "cas live - set comp + single col slice" test_composite_slice
    -- , testProperty "cas live test read/write QC" (prop_composite_col_readWrite pool)
    ]


deriving instance Arbitrary TAscii
deriving instance Arbitrary TBytes
deriving instance Arbitrary TCounter
deriving instance Arbitrary TInt32
deriving instance Arbitrary TInt
deriving instance Arbitrary TUUID
deriving instance Arbitrary TLong
deriving instance Arbitrary TUtf8
deriving instance Arbitrary a => Arbitrary (Exclusive a)


instance Arbitrary T.Text where
    arbitrary = T.pack <$> arbitrary


instance Arbitrary LB.ByteString where
    arbitrary = LB.pack <$> arbitrary


prop_casTypeAscii :: TAscii -> Bool
prop_casTypeAscii a = (decodeCas . encodeCas) a == a


prop_casTypeLong :: TLong -> Property
prop_casTypeLong a@(TLong n) = n >= 0 ==> (decodeCas . encodeCas) a == a


prop_casTypeInt32 :: TInt32 -> Bool
prop_casTypeInt32 a = (decodeCas . encodeCas) a == a



prop_casTypeComp :: (TAscii, TBytes, TInt32, TUtf8) -> Property
prop_casTypeComp a = whenFail err $ a == a'
    where
      a' = (decodeCas . encodeCas) a
      err = print $ "Decoded back into: " ++ show a'



prop_casTypeExcComp :: (TAscii, TBytes, TInt32, Exclusive TUtf8) -> Property
prop_casTypeExcComp a = whenFail err $ a == a'
    where
      a' = (decodeCas . encodeCas) a
      err = print $ "Decoded back into: " ++ show a'




newKS = KsDef {
          f_KsDef_name = Just "testing"
        , f_KsDef_strategy_class = Just "org.apache.cassandra.locator.NetworkTopologyStrategy"
        , f_KsDef_strategy_options = Just (M.fromList [("datacenter1","1")])
        , f_KsDef_replication_factor = Nothing
        , f_KsDef_cf_defs = Nothing
        , f_KsDef_durable_writes = Just True
        }


mkTestConn = createCassandraPool [("127.0.0.1", 9160)] 2 2 300 "testing"


-------------------------------------------------------------------------------
test_composite_col = do
    pool <- mkTestConn
    res <- runCas pool $ do
        insert "testing" "row1" ONE [packCol content]
        getCol "testing" "row1" (packKey key) ONE
    assertEqual "composite get-set" (Just content) (fmap unpackCol res)
    where
      key = (TLong 125, TBytes "oklahoma")
      content = (key, "asdf")


-------------------------------------------------------------------------------
test_composite_slice = do
    pool <- mkTestConn
    xs <- runCas pool $ do
        insert "testing" "row2" ONE [packCol (key, content)]
        get "testing" "row2" slice ONE
    let (res :: [((TLong, TBytes), LB.ByteString)]) = map unpackCol xs

    assertEqual "composite single col slice" content (snd . head $ res)
    where
      key = (TLong 125, TBytes "oklahoma")
      content = "asdf"
      slice = Range (Just (Exclusive (Single (TLong 125))))
                    (Just (Single (TLong 125)))
                    -- (Just (Single (TLong 127)))
                    Regular
                    100
      -- slice = Range (Just (TLong 125, TBytes "")) (Just (TLong 125, TBytes "zzzzz")) Regular 100


-------------------------------------------------------------------------------
-- | test quick-check generated pairs for composite column
prop_composite_col_readWrite ::  CPool -> ((TLong, TBytes), LB.ByteString) -> Property
prop_composite_col_readWrite pool content@(k@(TLong i, _),v) = i >= 0 ==>
  unsafePerformIO $ do
    res <- runCas pool $ do
      insert "testing" "row" ONE [packCol content]
      getCol "testing" "row" (packKey k) ONE
    return $ (Just content) == fmap unpackCol res





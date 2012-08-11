{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE StandaloneDeriving         #-}
{-# LANGUAGE TemplateHaskell            #-}

module Main where


-------------------------------------------------------------------------------
import           Control.Applicative
import qualified Data.ByteString.Char8                as B
import qualified Data.ByteString.Lazy.Char8           as LB
import           Data.DeriveTH
import qualified Data.Text                            as T
import           Test.Framework                       (defaultMain, testGroup)
import           Test.Framework.Providers.HUnit
import           Test.Framework.Providers.QuickCheck2 (testProperty)
import           Test.HUnit
import           Test.QuickCheck
import           Test.QuickCheck.Property
-------------------------------------------------------------------------------
import           Database.Cassandra.Pack
-------------------------------------------------------------------------------



main = defaultMain tests


tests = [testGroup "packTests" packTests]


packTests =
    [ testProperty "cas type marshalling long" prop_casTypeLong
    , testProperty "cas type marshalling ascii" prop_casTypeAscii
    , testProperty "cas type marshalling ascii" prop_casTypeInt32
    , testProperty "cas type marshalling composite" prop_casTypeComp
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

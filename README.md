
# Cassy - High Level Cassandra Client for Haskell


## Introduction

The intent is to develop a high quality, high level driver similar to
pycassa.

There are currently two modules, one for basic/lower level (but still
much more pleasant than thrift) operation, and one for some
experimental JSON serialization support.


## Examples

### Database.Cassandra.Basic Usage

This module offers straightforward functionality that is still much
more pleasant than using Thrift directly.
    
    import Database.Cassandra.Basic

    test :: IO ()
    test = do
      pool <- createCassandraPool defServers 3 300 "TestKeySpace"
      insert pool "testCF" "key1" QUORUM [col "col1" "value1"]
      getCol pool "testCF" "key1" "col1" QUORUM


### Database.Cassandra.JSON Usage

This module does two things in addition to basic functionality:

- Row and column keys are polymorphic so that you can use any
  type that is a member of the CKey typeclass. By default, we provide
  instances for String, ByteString and Text.
  
- Values are automatically marshalled to/from JSON.

Example usage:

      import Database.Cassandra.JSON
      import Data.Aeson

      type Name = String
      type Age = Int
      data Person = Person Name Age

      -- Define JSON serialization for our data structure

      instance ToJSON Person where
          toJSON (Person nm age) = toJSON (nm,age)

      instance FromJSON Person where
          parseJSON v = do
              (nm, age) <- parseJSON v
              return $ Person nm age


      test :: Person -> IO ()
      test p@(Person nm age) = do
        pool <- createCassandraPool defServers 3 300 "TestKeySpace"

        -- I can use any string-like key and don't have to explicitly
        -- convert person to ByteString.
        insertCol pool "testCF" "people" nm QUORUM p

        -- Automatically de-serialized back to a datatype
        res <- getCol pool "testCF" "people" nm QUORUM
        case res of
            Just (Person nm age) -> return age
            Nothing -> error "Oh NO!!!"

      
## TODOs

* Add support for counters and batch mutators
* Add support for composite columns
* Add support for database/admin operations

## Contributions

Would love to get contributions, bug reports, suggestions, feedback
from the community.

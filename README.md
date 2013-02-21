
# Cassy - High Level Cassandra Client for Haskell


## Introduction

The intent is to develop a high quality, high level driver similar to
pycassa.

There are currently two modules, one for basic/lower level (but still
much more pleasant than thrift) operation, and one for some
experimental JSON serialization support.

## API Documentation

Please see the Haddocks hosted on HackageDB:

http://hackage.haskell.org/package/cassy


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

## Release Notes

### Version 0.4

This version packs a fairly large changeset. It will almost definitely
break your code, although the fix/adjustment is likely to be minor.


* Added `Database.Cassandra.Marshall` that is now intended to be the
  primary module to be used in all Cassandra operations. This module
  supercedes and replaces the `Database.Cassandra.JSON` high level
  module. Building on top of the Basic module, Marshall allows user to
  pick the serialization methodology for each of the operations. We
  provide out of box support for JSON, cereal, safecopy and plain old
  show/read.
* Vastly enhanced the Database.Cassandra.Pack module to represent
  types that Cassandra can sort and validate.
* Added CasType typeclass that offers `encodeCas` and `decodeCas`
  methods that handle conversions to/from the binary ByteString
  representation.
* Composite columns are now supported through tuples. Just pick two or
  more CasType instances and put them in a tuple to automatically
  trigger composite column conversion. Keep in mind that your
  ColumnFamily schema must be configured right or else you'll get
  runtime exceptions.
* Added a bunch of newtype wrappers to directly map to types Cassandra
  knows. These include `TAscii`, `TBytes`, `TInt`, `TUtf8` and some
  others.
* A new `TTimeStamp` type makes it easier to have timestamps as
  Long-encoded columns.
* Changed several methods in Basic and JSON modules to expect CasType
  column key values instead of concrete ByteString.
* Added the useful `packCol` and `unpackCol` functions to smoothly
  handle column key type conversions when working with the Basic
  module.
* There is now a `Retry` module for automatically retrying queries
  flexibly. Just pass any of your cassy operations to the retrying
  combinator and it will be retried in case of server-related failure.
* There is now simple support for pagination of columns in wide rows,
  CPS-style. See the `paginate` function in
  `Database.Cassandra.Marshall`.
* Made the Cas monad a simple type synonym for ReaderT CPool.
* Added the `get_` metho to `JSON` to make it easier to discard key
  names and just get the column contents.
* Numerous fixes and minor tweaks.


### Version 0.3

* Added MonadCassandra typeclass, which is now used by *all*
  operations by default.
* Added a default Cas moand that instantiates MonadCassandra for
  convenience. 
* All Basic module ops now return results directly instead of an
  Either wrapper. Each operation may raise a CassandraException.
* Connection pooling now builds on top of the resource-pool library to
  initiate connections to multiple servers in round-robin fashion.
* Basic.insert now knows how to insert a SuperColumn.


## TODOs

* Add support for counters and batch mutators
* Add support for database/admin operations

## Contributions

Would love to get contributions, bug reports, suggestions, feedback
from the community.


# Cassy - High Level Cassandra Client for Haskell


## Introduction

The intent is to develop a high quality, high level driver similar to
pycassa.

There are currently two modules, one for basic/lower level (but still
much more pleasant than thrift) operation, and one for some
experimental JSON serialization support.


## Examples

### Basic Usage
    
    import Database.Cassandra.Basic

    test :: IO ()
    test = do
      pool <- createCassandraPool defServers 3 300 "TestKeySpace"
      insert pool "testCF" "key1" QUORUM [col "col1" "value1"]
      getOne pool "testCF" "key1" "col1" QUORUM
      
      
## TODOs

* Add support for counters and batch mutators
* Add support for composite columns
* Add support for database/admin operations

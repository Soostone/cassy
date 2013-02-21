{-# LANGUAGE RecordWildCards #-}
-----------------------------------------------------------------------------
-- |
-- Module      :  Database.Cassandra.Retry
-- Copyright   :  Ozgun Ataman <ozataman@gmail.com>
-- License     :  BSD3
--
-- Maintainer  :  Ozgun Ataman
-- Stability   :  experimental
--
-- Utilites for working with Cassandra
----------------------------------------------------------------------------


module Database.Cassandra.Retry where


-------------------------------------------------------------------------------
import           Control.Concurrent
import           Control.Monad.CatchIO
import           Control.Monad.Trans
import           Data.Default
-------------------------------------------------------------------------------


-- | Settings for retry behavior. Just using 'def' for default values.
-- should work in most cases.
data RetrySettings = RetrySettings {
      numRetries :: Int
    -- ^ Number of retries. Defaults to 5.
    , backoff    :: Bool
    -- ^ Whether to implement exponential backoff in retries. Defaults
    -- to True.
    , baseDelay  :: Int
    -- ^ The base delay in miliseconds. Defaults to 50. Without
    -- 'backoff', this is the delay. With 'backoff', this base delay
    -- will grow by powers of 2 on each subsequent retry.
    }


instance Default RetrySettings where
    def = RetrySettings 5 True 50


-- | A flexible action retrying combinator.
retrying :: (Functor m, Exception e, MonadCatchIO m)
         => RetrySettings
         -- ^ For default settings, just use 'def'
         -> (e -> Bool)
         -- ^ Should a given exception be retried?
         -> m b
         -- ^ Action to perform
         -> m b
retrying RetrySettings{..} h f = go 0
    where
      delay = baseDelay * 1000
      backoffRetry n = liftIO (threadDelay (2^n * delay)) >> go (n+1)
      flatRetry n = liftIO (threadDelay delay) >> go (n+1)
      go n = do
        res <- try f
        case res of
          Right a -> return a
          Left e ->
            case h e of
              True ->
                case n >= numRetries of
                  True -> throw e
                  False -> if backoff then backoffRetry n else flatRetry n
              False -> throw e




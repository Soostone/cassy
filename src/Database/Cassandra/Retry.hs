{-# LANGUAGE DeriveDataTypeable  #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
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
import           Control.Exception     (SomeException)
import           Control.Monad.CatchIO
import           Control.Monad.Trans
import           Data.Default
import           Data.Generics
import           Prelude               hiding (catch)
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


-- | Retry ALL exceptions that may be raised. To be used with caution.
retryAll :: MonadCatchIO m
         => RetrySettings
         -> m a
         -> m a
retryAll set f = retrying set [h] f
    where
      h = Handler $ \ (e :: SomeException) -> return True


-- | A flexible action retrying combinator.
retrying :: forall m a. MonadCatchIO m
         => RetrySettings
         -- ^ For default settings, just use 'def'
         -> [Handler m Bool]
         -- ^ Should a given exception be retried? Action will be
         -- retried if this returns True.
         -> m a
         -- ^ Action to perform
         -> m a
retrying RetrySettings{..} hs f = go 0
    where
      delay = baseDelay * 1000
      backoffRetry n = liftIO (threadDelay (2^n * delay)) >> go (n+1)
      flatRetry n = liftIO (threadDelay delay) >> go (n+1)


      -- | Convert a (e -> m Bool) handler into (e -> m a) so it can
      -- be wired into the 'catches' combinator.
      transHandler :: Int -> Handler m Bool -> Handler m a
      transHandler n (Handler h) = Handler $ \ e -> do
          chk <- h e
          case chk of
            True ->
              case n >= numRetries of
                True -> throw e
                False -> if backoff then backoffRetry n else flatRetry n
            False -> throw e

      -- handle :: forall e. Exception e => Handler m Bool -> Int -> e -> m a
      -- handle (Handler h) n e = do
      --     chk <- h e
      --     case chk of
      --       True ->
      --         case n >= numRetries of
      --           True -> throw e
      --           False -> if backoff then backoffRetry n else flatRetry n
      --       False -> throw e

      go n = f `catches` map (transHandler n) hs



                              ------------------
                              -- Simple Tests --
                              ------------------



-- data TestException = TestException deriving (Show, Typeable)
-- data AnotherException = AnotherException deriving (Show, Typeable)

-- instance Exception TestException
-- instance Exception AnotherException


-- test = retrying def [h1,h2] f
--     where
--       f = putStrLn "Running action" >> throw AnotherException
--       h1 = Handler $ \ (e :: TestException) -> return False
--       h2 = Handler $ \ (e :: AnotherException) -> return True



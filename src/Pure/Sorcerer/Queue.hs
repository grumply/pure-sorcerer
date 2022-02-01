module Pure.Sorcerer.Queue 
  ( newQueue
  , withEmptyQueue
  , isEmptyQueue
  , arrive
  , arriveMany
  , collect
  , Queue
  ) where

import Control.Concurrent
import Control.Monad (join,void)
import qualified Data.List as List (reverse)

--------------------------------------------------------------------------------
-- Modified version of Neil Mitchell's `The Flavor of MVar` Queue from:
-- http://neilmitchell.blogspot.com/2012/06/flavours-of-mvar_04.html
-- Modified for amortized O(1) read and O(1) write.
data Queue a = Queue (MVar (Either [a] (MVar [a])))

{-# INLINE newQueue #-}
newQueue :: IO (Queue a)
newQueue = Queue <$> newMVar (Left [])

{-# INLINE withEmptyQueue #-}
withEmptyQueue :: Queue a -> IO () -> IO Bool
withEmptyQueue (Queue q_) io =
  withMVar q_ $ \case
    Left [] -> io >> pure True
    Right b -> do
      ma <- tryReadMVar b
      case ma of
        Nothing -> io >> pure True
        Just a  -> pure False
    _ -> pure False

{-# INLINE isEmptyQueue #-}
isEmptyQueue :: Queue a -> IO Bool
isEmptyQueue = flip withEmptyQueue (pure ())

{-# INLINE arrive #-}
arrive :: Queue a -> a -> IO ()
arrive (Queue q_) x =
  modifyMVar_ q_ $ \case
    Left xs -> return (Left (x:xs))
    Right b -> do 
      putMVar b [x]
      return (Left [])

{-# INLINE arriveMany #-}
arriveMany :: Queue a -> [a] -> IO ()
arriveMany (Queue q_) as =
  modifyMVar_ q_ $ \case
    Left xs -> return (Left (List.reverse as ++ xs))
    Right b -> do
      putMVar b as
      return (Left [])

{-# INLINE collect #-}
collect :: Queue a -> IO [a]
collect (Queue q_) = do
  join $ modifyMVar q_ $ \case
    Right b -> do
      mxs <- tryTakeMVar b
      case mxs of
        Nothing -> return (Right b,takeMVar b)
        Just xs -> return (Left [],return $ List.reverse xs)
    Left [] -> do 
      b <- newEmptyMVar
      return (Right b,takeMVar b)
    Left xs -> 
      return (Left [],return $ List.reverse xs)
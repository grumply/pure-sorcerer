{-# language ImplicitParams #-}
module Pure.Sorcerer.Sorcery 
  ( listenStream, listenStreams
  , subscribeStream, subscribeStreams
  , unsubscribeStream, unsubscribeStreams
  , unlisten
  ) where

import Pure.Sorcerer.Dispatcher
import Pure.Sorcerer.Streamable

import Pure.Elm.Component hiding (Left,Right,Listener)

import Control.Concurrent
import Control.Monad
import Data.Foldable
import Data.Typeable
import Data.Unique
import Unsafe.Coerce

listenStream :: (Typeable ev, Ord (Stream ev)) => Stream ev -> (ev -> IO ()) -> IO (StreamListener ev)
listenStream s f = do
  u <- newUnique
  addStreamListener (Left u) s (traverse_ g)
  where
    g = \case
      Write ev -> f (unsafeCoerce ev)
      Transact ev _ -> f (unsafeCoerce ev)
      _ -> pure ()

listenStreams :: Typeable ev => (ev -> IO ()) -> IO (Listener ev)
listenStreams f = do
  u <- newUnique
  addListener (Left u) (traverse_ g)
  where
    g = \case
      Write ev -> f (unsafeCoerce ev)
      Transact ev _ -> f (unsafeCoerce ev)
      _ -> pure ()

subscribeStream :: (Typeable ev, Ord (Stream ev), Elm msg) => Stream ev -> (ev -> msg) -> IO (StreamListener ev)
subscribeStream s f = do
  mtid <- myThreadId
  addStreamListener (Right mtid) s (traverse_ g)
  where
    g = \case
      Write ev -> ?command (f (unsafeCoerce ev)) (pure ()) >>= \b -> unless b (unsubscribeStream s)
      Transact ev _ -> ?command (f (unsafeCoerce ev)) (pure ()) >>= \b -> unless b (unsubscribeStream s)
      _ -> pure ()

subscribeStreams :: forall ev msg. (Typeable ev, Elm msg) => (ev -> msg) -> IO (Listener ev)
subscribeStreams f = do
  mtid <- myThreadId
  addListener (Right mtid) (traverse_ g)
  where
    g = \case
      Write ev -> ?command (f (unsafeCoerce ev)) (pure ()) >>= \b -> unless b (unsubscribeStreams @ev)
      Transact ev _ -> ?command (f (unsafeCoerce ev)) (pure ()) >>= \b -> unless b (unsubscribeStreams @ev)
      _ -> pure ()

unsubscribeStream :: (Typeable ev, Ord (Stream ev)) => Stream ev -> IO ()
unsubscribeStream s = do
  mtid <- myThreadId
  unlisten (StreamListener (s,Right mtid))

unsubscribeStreams :: forall ev. Typeable ev => IO ()
unsubscribeStreams = do
  mtid <- myThreadId
  unlisten (Listener (Right mtid) :: Listener ev)

module Pure.Sorcerer.Manager where

import Pure.Sorcerer.Aggregate
import Pure.Sorcerer.Aggregator
import Pure.Sorcerer.Dispatcher
import Pure.Sorcerer.JSON
import Pure.Sorcerer.Log
import Pure.Sorcerer.Queue
import Pure.Sorcerer.Streamable
import qualified Pure.Sorcerer.Streamable as S

import Pure.Data.Time

import Data.ByteString.Lazy.Builder as BSB

import Data.Map.Strict as Map

import Control.Concurrent
import Control.Exception
import Control.Monad
import Data.IORef
import Data.Maybe
import Data.Traversable
import Data.Typeable
import GHC.Exts
import System.Directory
import System.FilePath
import System.IO.Unsafe
import System.Timeout
import Unsafe.Coerce

{-# NOINLINE builders #-}
builders :: IORef (Map TypeRep (Stream Any -> TransactionId -> IO [Aggregator Any]))
builders = unsafePerformIO (newIORef Map.empty)

getBuilders :: forall ev. Typeable ev => IO (Maybe (Stream ev -> TransactionId -> IO [Aggregator ev]))
getBuilders = do
  let ty = typeOf (undefined :: ev)
  bs <- readIORef builders
  pure (unsafeCoerce (Map.lookup ty bs))

startManagerWithBuilders :: forall (ev :: *). (Manageable ev, ToJSON ev, Streamable ev, Ord (Stream ev), FromJSON ev, Typeable ev) => [Event] -> StreamManager ev -> IO ()
startManagerWithBuilders =
  let
    getter stream tid = do
      mbs <- getBuilders @ev
      case mbs of
        Nothing -> pure []
        Just bs -> bs stream tid
  in
    startManager getter 

class Manageable ev where
  threshold :: Time
  threshold = Milliseconds 30 0

  batch :: Int
  batch = 1
  
instance {-# OVERLAPPABLE #-} Manageable ev

startManager :: forall ev. (Typeable ev, Manageable ev, Ord (Stream ev), ToJSON ev, FromJSON ev, Streamable ev) => (Stream ev -> TransactionId -> IO [Aggregator ev]) -> [Event] -> StreamManager ev -> IO ()
startManager builder evs sm@(StreamManager (stream,callback)) =
  void do
    forkIO do
      q <- newQueue
      arriveMany q evs
      putMVar callback (arriveMany q)
      exists <- doesFileExist fp
      if exists then do
        (log,tid) <- resume @ev fp
        ags <- builder stream tid
        run q log 0 mempty ags tid
      else 
        nostream q
  where
    fp = S.stream stream
  
    satisfy acc@(!count,!evs,ags,!tid) ev = case ev of
      Read f -> do
        let 
          test :: Aggregator ev -> Maybe (IO ())
          test (Aggregator _ mag _ _ _) = 
            case cast mag of
              Nothing -> Nothing
              Just x  -> Just (f x)
        case catMaybes (fmap test ags) of
          (g : _) -> g
          _ -> f Nothing
        pure acc

      Write e -> do
        let 
          !count' = count + 1
          !tid' = tid + 1
          !evs' = evs <> BSB.lazyByteString (encode_ (tid',e)) <> "\n"
        ags' <- for ags (flip (integrate @ev) (tid',ev))
        pure (count',evs',ags',tid')

      Transact e _ -> do
        let 
          !count' = count + 1
          !tid' = tid + 1
          !evs' = evs <> BSB.lazyByteString (encode_ (tid',e)) <> "\n"
        ags' <- for ags (flip (integrate @ev) (tid',ev))
        pure (count',evs',ags',tid')

    run :: Queue Event -> Log -> Int -> BSB.Builder -> [Aggregator ev] -> TransactionId -> IO ()
    run events log = running 
      where
        running :: Int -> BSB.Builder -> [Aggregator ev] -> TransactionId -> IO ()
        running !count !evs ags !tid = do
          tryTimeout (collect events) >>= \case
            Nothing -> do
              when (count > 0) do
                record log evs tid
              ags' <- traverse persist ags
              did <- tryShutdownWith events (close log)
              unless did do
                running 0 mempty ags' tid
                  
            Just ms -> do
              (count',evs',ags',tid') <- foldM satisfy (count,evs,ags,tid) ms
              if count' >= (batch @ev) then do
                record log evs' tid'
                running 0 mempty ags' tid'
              else
                running count' evs' ags' tid'

    -- `run` can correctly satisfy all requests, but requires an existing stream file. If we know
    -- there is no stream file, we know there are no aggregates and we can avoid creating the 
    -- stream file or the aggregate files and satisfy all reads with Nothing.  If this loop finds 
    -- a `Write` or `Transact`, it creates the stream file and initializes the aggregates and 
    -- transitions to the normal `run` loop.
    nostream :: Queue Event -> IO ()
    nostream events = loop
      where
        loop =
          tryTimeout (collect events) >>= \case
            Nothing -> do
              did <- tryShutdownWith events (pure ())
              unless did loop
            Just ms -> 
              go ms
          where
            go = \case
              [] -> loop
              (Read f : evs) -> f Nothing >> go evs
              evs -> do
                createDirectoryIfMissing True (takeDirectory fp)
                (log,tid) <- resume @ev fp
                ags <- builder stream tid
                (count',evs',ags',tid') <- foldM satisfy (0,mempty,ags,tid) evs
                if count' >= (batch @ev) then do
                  record log evs' tid'
                  run events log 0 mempty ags' tid'
                else
                  run events log count' evs' ags' tid'

    tryShutdownWith :: Queue Event -> IO () -> IO Bool
    tryShutdownWith events f = do
      cb <- takeMVar callback
      empty <- isEmptyQueue events
      if empty then do
        f
        removeStreamManager sm
        pure True
      else do
        putMVar callback cb
        pure False

    tryTimeout :: forall x. IO x -> IO (Maybe x)
    tryTimeout io =
      let Microseconds us _ = threshold @ev
      in catch (timeout us io) (\(_ :: SomeException) -> pure Nothing)


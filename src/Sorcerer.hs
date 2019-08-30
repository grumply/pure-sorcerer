{-# LANGUAGE ScopedTypeVariables, BangPatterns, DefaultSignatures, TypeFamilies,
  DeriveAnyClass, AllowAmbiguousTypes, TypeApplications, RecordWildCards, 
  MultiParamTypeClasses, FlexibleContexts, ExistentialQuantification, CPP,
  DeriveGeneric, RankNTypes, LambdaCase, OverloadedStrings, PatternSynonyms,
  ImplicitParams #-}
module Sorcerer
  (-- Storage layer
   sorcerer
  ,read,read',write,transact,observe,events
  ,Listener,listener
  ,Source(..),Aggregable(..)
  ,pattern Update, pattern Ignore, pattern Delete
  ,pattern Added,pattern Updated, pattern Deleted, pattern Ignored

  -- Pub/Sub layer
  ,sorcery
  ,Unlisten(..)
  ,Event(..)
  ,TargetedListener,listenTargeted,listenTargetedEvent
  ,UntargetedListener,listenUntargeted,listenUntargetedEvent,listenUntargetedEvent'
  ,subscribeStream,unsubscribeStream
  ,subscribeStreamEvent,unsubscribeStreamEvent
  ,subscribeStreams,unsubscribeStreams,unsubscribeStreams'
  ,subscribeStreamsEvent,unsubscribeStreamsEvent,unsubscribeStreamsEvent'
  ) where

import Pure.Elm hiding (Left,Right,(<.>),Listener,listeners,record,write,Delete)
#ifdef __GHCJS__
import Pure.Data.JSON
#else
import Data.Aeson
#endif

import qualified Data.ByteString as BS
import qualified Data.ByteString.Internal as BS
import qualified Data.ByteString.Lazy.Internal as BSL
import qualified Data.ByteString.Char8 as BSC
import qualified Data.ByteString.Lazy.Char8 as BSLC
import qualified Data.ByteString.Unsafe as BSU
import qualified Data.ByteString.Builder as BSB

import Control.Concurrent
import Control.Exception
import Control.Monad
import Data.Bool
import Data.Foldable
import Data.Function
import Data.List as List
import Data.Traversable
import Data.Typeable
import Data.Unique
import Data.Maybe
import Data.Hashable
import GHC.Fingerprint
import GHC.Generics
import System.Directory
import System.IO
import System.IO.Unsafe
import System.FilePath
import Text.Read (readMaybe)

import qualified Data.IntMap.Strict as IntMap
import qualified Data.Map.Strict as Map

import Prelude hiding (read)
import qualified Prelude

import qualified System.Posix.Files       as P
import qualified System.Posix.IO          as P
import           System.Posix.Types       as P (Fd, FileOffset, ByteCount)
import Foreign.Marshal
import Foreign.ForeignPtr
import Foreign.Ptr

{-# INLINE encode_ #-}
{-# INLINE decode_ #-}
#ifdef __GHCJS__
encode_ :: ToJSON a => a -> BSL.ByteString
encode_ = fromTxt . encode . toJSON
decode_ :: FromJSON a => BSL.ByteString -> Maybe a
decode_ = decode . toTxt
#else
encode_ :: ToJSON a => a -> BSL.ByteString
encode_ = encode
decode_ :: FromJSON a => BSL.ByteString -> Maybe a
decode_ = decode
#endif

--------------------------------------------------------------------------------
-- Modified version of Neil Mitchell's `The Flavor of MVar` Queue from:
-- http://neilmitchell.blogspot.com/2012/06/flavours-of-mvar_04.html
-- Modified for amortized O(1) read and O(1) write.
type Queue a = MVar (Either [a] (MVar [a]))

{-# INLINE newQueue #-}
newQueue :: IO (Queue a)
newQueue = newMVar (Left [])

{-# INLINE withEmptyQueue #-}
withEmptyQueue :: Queue a -> IO () -> IO Bool
withEmptyQueue q_ io =
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
isEmptyQueue q_ = withEmptyQueue q_ (pure ())

{-# INLINE arrive #-}
arrive :: Queue a -> a -> IO ()
arrive q_ x = 
  modifyMVar_ q_ $ \case
    Left xs -> return $ Left (x:xs)
    Right b -> do putMVar b [x]
                  return (Left [])

{-# INLINE collect #-}
collect :: Queue a -> IO [a]
collect q_ = 
  join $ modifyMVar q_ $ \case
    Right b -> do putMVar b []
                  b <- newEmptyMVar
                  return (Right b,readMVar b)
    Left [] -> do b <- newEmptyMVar
                  return (Right b,readMVar b)
    Left xs -> return (Left [], return $ List.reverse xs)

--------------------------------------------------------------------------------

getEvents :: forall ev. Source ev => FilePath -> IO [ev]
getEvents fp = do
  cnts <- BSLC.readFile fp
  pure $ fmap snd $ catMaybes $ fmap (decode_ @(Int,ev)) $ List.drop 1 $ BSLC.lines cnts

--------------------------------------------------------------------------------
-- Core Source/Aggregable types for defining event sourcing resources.

class (Hashable (Stream ev), ToJSON ev, FromJSON ev, Typeable ev) => Source ev where
  data Stream ev

  {-# INLINE stream #-}
  stream :: Stream ev -> FilePath
  stream sev = show (abs (hash (typeOf (undefined :: ev)))) </> show (abs (hash sev)) <.> "stream"

class (Hashable (Stream ev), Typeable ag, Source ev) => Aggregable ev ag where
  -- Try to avoid creating aggregates that will expect a lot of update messages 
  -- and take a long time to execute each update, as the updates will back up
  -- in a Chan and could cause a memory leak. It is also your responsibility to 
  -- make sure your update isn't too lazy for your use case.

  {-# INLINE aggregate #-}
  aggregate :: FilePath
  aggregate = show (abs (hash (typeOf (undefined :: ag)))) <.> "aggregate"

  update :: ev -> Maybe ag -> Maybe (Maybe ag)

pattern Update :: a -> Maybe (Maybe a)
pattern Update a = Just (Just a)

pattern Delete :: Maybe (Maybe a)
pattern Delete = Just Nothing

pattern Ignore :: Maybe (Maybe a)
pattern Ignore = Nothing

pattern Added :: a -> (Maybe a,Maybe (Maybe a))
pattern Added a <- (Nothing,Just (Just a))

pattern Updated :: a -> a -> (Maybe a,Maybe (Maybe a))
pattern Updated a a' <- (Just a,Just (Just a'))

pattern Deleted :: a -> (Maybe a,Maybe (Maybe a))
pattern Deleted a <- (Just a,Just Nothing)

pattern Ignored :: (Maybe a,Maybe (Maybe a)) 
pattern Ignored <- (_,Nothing)

--------------------------------------------------------------------------------
-- Core event types for reading/writing aggregates.

data Event 
  = forall ev. (Hashable (Stream ev), Source ev) => Write
    { _ty    :: {-# UNPACK #-}!Int
    , _ident :: Stream ev
    , event  :: ev
    }
  | forall ev ag. (Hashable (Stream ev), Aggregable ev ag) => Read
    { _ty      :: {-# UNPACK #-}!Int
    , _ident   :: Stream ev
    , _ag      :: {-# UNPACK #-}!Int
    , callback :: Maybe ag -> IO ()
    }
  | forall ev ag. (Hashable (Stream ev), Aggregable ev ag) => Transact
    { _ty     :: {-# UNPACK #-}!Int
    , _ident  :: Stream ev
    , _ag     :: {-# UNPACK #-}!Int
    , event   :: ev
    , inspect :: Maybe ag -> Maybe (Maybe ag) -> IO ()
    }

--------------------------------------------------------------------------------
-- Transaction tag; monotonically increasing

type TransactionId = Int

--------------------------------------------------------------------------------
-- Aggregator; manages an aggregate file and updates the aggregate on-demand.
-- Guarantees that the aggregate is up-to-date w.r.t. the TransactionId.

data Aggregate ag = Aggregate
  { aCurrent   :: TransactionId
  , aAggregate :: Maybe ag
  } deriving (Generic,ToJSON,FromJSON)

data AggregatorEnv = AggregatorEnv
  { aeEvents   :: FilePath
  , aeMessages :: Chan AggregatorMsg
  , aeLatest   :: TransactionId
  }

data AggregatorMsg 
  = AggregatorEvent TransactionId Event
  | Persist (IO ())
  | Shutdown

{-# INLINE writeAggregate #-}
writeAggregate :: ToJSON ag => FilePath -> Aggregate ag -> IO ()
writeAggregate fp (Aggregate tid mag) = do
  fd <- P.openFd fp P.WriteOnly (Just $ P.unionFileModes P.ownerReadMode P.ownerWriteMode) P.defaultFileFlags
  P.setFdOption fd P.SynchronousWrites True
  let 
    stid = encode_ tid
    commit = BSB.lazyByteString stid <> BSB.lazyByteString (BSLC.replicate (11 - BSLC.length stid) ' ')
    bsb = commit <> "\n" <> BSB.lazyByteString (encode_ mag)
    (fptr, off, len) = BS.toForeignPtr $ BSLC.toStrict $ BSB.toLazyByteString bsb
  withForeignPtr fptr $
    \wptr -> P.fdWriteBuf fd (plusPtr wptr off) (fromIntegral len)
  P.closeFd fd

{-# INLINE readAggregateLazy #-}
readAggregateLazy :: FromJSON ag => FilePath -> IO (Maybe (TransactionId,Maybe ag))
readAggregateLazy fp = do
  exists <- doesFileExist fp
  if exists then do
    cnts <- BSLC.readFile fp
    pure $
      case BSLC.lines cnts of
        (ln:rest) ->
          Just 
            ( Prelude.read (BSLC.unpack ln)
            , decode_ (List.head rest)
            )
        _ -> 
          Nothing
  else 
    pure Nothing

{-# INLINE commitAggregate #-}
commitAggregate :: FilePath -> TransactionId -> IO ()
commitAggregate fp tid = do
  fd <- P.openFd fp P.WriteOnly (Just $ P.unionFileModes P.ownerReadMode P.ownerWriteMode) P.defaultFileFlags
  P.setFdOption fd P.SynchronousWrites True
  let 
    stid = encode_ tid
    commit = BSB.lazyByteString stid <> BSB.lazyByteString (BSLC.replicate (11 - BSLC.length stid) ' ')
    bsb = commit <> "\n"
    (fptr, off, len) = BS.toForeignPtr $ BSLC.toStrict $ BSB.toLazyByteString bsb
  withForeignPtr fptr $
    \wptr -> P.fdWriteBuf fd (plusPtr wptr off) (fromIntegral len)
  P.closeFd fd

{-# INLINE aggregator #-}
aggregator :: forall ev ag. (ToJSON ag, FromJSON ag, Source ev, Aggregable ev ag) 
           => FilePath -> AggregatorEnv -> IO ()
aggregator fp AggregatorEnv {..} = do
  createDirectoryIfMissing True (takeDirectory fp)
  Control.Monad.void $ forkIO $ do
    cag <- prepare
    ms <- getChanContents aeMessages
    foldM_ run cag ms
  where
    prepare = do
      mtag <- readAggregateLazy fp
      case mtag of
        Nothing 
          | aeLatest == 0 -> pure (True,True,Aggregate 0 Nothing) 
          | otherwise -> do
            evs0 :: [ev] <- getEvents aeEvents
            let 
              mkAggregatorEvent tid ev = AggregatorEvent tid (Write 0 undefined ev)
              evs = zipWith mkAggregatorEvent [0 ..] evs0
            foldM run (True,True,Aggregate 0 Nothing) evs

        Just ~(tid,mag) 
          | tid == aeLatest -> pure (False,False,Aggregate tid mag)
          | otherwise -> do
            evs0 :: [ev] <- getEvents aeEvents
            let 
              mkAggregatorEvent tid ev = AggregatorEvent tid (Write 0 undefined ev)
              evs = zipWith mkAggregatorEvent [tid + 1 ..] (List.take (aeLatest - tid) (List.drop (tid + 1) evs0))
            foldM run (True,False,Aggregate tid mag) evs
                   
    run :: (Bool,Bool,Aggregate ag) -> AggregatorMsg -> IO (Bool,Bool,Aggregate ag)
    run (shouldWriteTransaction,!shouldWriteAggregate,cur) am =
      case am of
        AggregatorEvent tid e ->
          case e of
            Write _ _ ev -> do
              case cast ev of
                Just e -> do
                  let !mmag = (update @ev @ag) e (aAggregate cur)
                  pure (True,shouldWriteAggregate || isJust mmag,Aggregate tid (fromMaybe (aAggregate cur) mmag))
                _ -> 
                  error "aggregator.runner: invariant broken; received impossible write event"

            Read _ _ _ cb -> do
              for_ (cast cb :: Maybe (Maybe ag -> IO ())) $ \f -> f (aAggregate cur)
              pure (shouldWriteTransaction,shouldWriteAggregate,cur)

            Transact _ _ _ ev cb -> do
              case cast ev of
                Just e -> do
                  let !mmag = (update @ev @ag) e (aAggregate cur)
                  for_ (cast cb :: Maybe (Maybe ag -> Maybe (Maybe ag) -> IO ())) $ \f -> 
                    f (aAggregate cur) mmag 
                  pure (True, shouldWriteAggregate || isJust mmag,Aggregate tid (fromMaybe (aAggregate cur) mmag))
                Nothing -> 
                  error "aggregator.runner: invariant broken; received impossible update event"

        Persist persisted 
          | shouldWriteAggregate -> do
            writeAggregate (fp <> ".temp") cur
            renameFile (fp <> ".temp") fp
            persisted 
            pure (False,False,cur)
          | shouldWriteTransaction -> do
            commitAggregate fp (aCurrent cur)
            persisted
            pure (False,False,cur)
          | otherwise -> do
            persisted
            pure (False,False,cur)

        Shutdown -> do
          myThreadId >>= killThread
          pure (False,False,cur)

--------------------------------------------------------------------------------
-- Abstract representation of an aggregator that ties into Source/Aggregable for
-- easy instantiation.

data Listener = Listener 
  { _lty :: (Int,Int)
  , _aggregate :: FilePath
  , _listener :: FilePath -> AggregatorEnv -> IO ()
  }

-- listener @SomeEvent @SomeAggregate
listener :: forall ev ag. (ToJSON ag, FromJSON ag, Aggregable ev ag) => Listener
listener = 
  let 
    ev = 
      case typeRepFingerprint (typeOf (undefined :: ev)) of
        Fingerprint x _ -> fromIntegral x
    ag =
      case typeRepFingerprint (typeOf (undefined :: ag)) of
        Fingerprint x _ -> fromIntegral x

  in 
    Listener (ev,ag) (aggregate @ev @ag) (aggregator @ev @ag)

{-# INLINE startAggregators #-}
startAggregators :: Source ev => Stream ev -> FilePath -> TransactionId -> [Listener] -> IO (Chan AggregatorMsg)
startAggregators s stream_fp tid ls = do
  chan <- newChan
  for_ ls $ \(Listener i fp ag) -> do
    ch <- dupChan chan
    ag (dropExtension (stream s) </> fp) (AggregatorEnv stream_fp ch tid)
  pure chan

--------------------------------------------------------------------------------
-- Log resumption; should be able to safely recover a majority of failures

{-# INLINE resumeLog #-}
resumeLog :: forall ev. Source ev => P.Fd -> FilePath -> IO TransactionId
resumeLog fd fp = do
  off <- P.fdSeek fd SeekFromEnd 0
  let eof = off == 0
  if eof then 
    newStream 
  else 
    recover
  where
    newStream :: IO TransactionId
    newStream = do
      P.fdWrite fd "00          "
      pure 0

    recover :: IO TransactionId
    recover = do
      _ <- P.fdSeek fd AbsoluteSeek 0
      (ln,_) <- P.fdRead fd 12
      i <- 
        case ln of
          '1':tid ->
            case readMaybe tid of
              Nothing -> repair
              Just i  -> pure i
          '0':tid -> repair
          _ -> error $ "manager: unrecoverable steram file " ++ fp

      P.fdSeek fd AbsoluteSeek 0
      P.fdWrite fd "0"
      P.fdSeek fd SeekFromEnd 0

      pure i

    repair :: IO TransactionId
    repair = do
      sane <- hasTrailingNewline
      end <- P.fdSeek fd SeekFromEnd 0
      i <-
        if sane then do
          findNthFromLastNewline 1
          ln <- fdGetLn
          case decode_ (BSLC.pack ln) :: Maybe (Int,Value) of
            Just (i,_) -> do
              commit i
              pure i
            Nothing -> 
              error $ "resumeLog.repair: unrecoverable event stream: " ++ fp
        else do
          off <- findNthFromLastNewline 0
          let count = fromIntegral end - off
          P.fdWrite fd (List.replicate count ' ')
          _ <- findNthFromLastNewline 1
          ln <- fdGetLn
          case decode_ (BSLC.pack ln) :: Maybe (Int,Value) of
            Just (i,_) -> do
              commit i
              pure i
            Nothing ->
              error $ "resumeLog.repair: unrecoverable event stream: " ++ fp
         
      P.fdSeek fd SeekFromEnd 0

      pure i

      where
        hasTrailingNewline :: IO Bool
        hasTrailingNewline = do
          P.fdSeek fd SeekFromEnd (-1)
          (c,_) <- P.fdRead fd 1
          pure (c == "\n")

        findNthFromLastNewline :: Int -> IO Int
        findNthFromLastNewline n = go 0 1
          where
            go i p = do
              off <- P.fdSeek fd SeekFromEnd (fromIntegral $ negate p)
              (c,_) <- P.fdRead fd 1
              if c == "\n" then
                if n == i then 
                  pure (fromIntegral off + 1)
                else
                  go (i + 1) (p + 1)
              else
                go i (p + 1)

        commit :: Int -> IO ()
        commit c = do
          P.fdSeek fd AbsoluteSeek 0
          P.fdWrite fd (replicate 11 ' ' ++ "\n")
          P.fdSeek fd AbsoluteSeek 0
          P.fdWrite fd ('1':show c)
          pure ()

        fdGetLn :: IO String
        fdGetLn = lazyRead
          where
            lazyRead = unsafeInterleaveIO loop
            loop = do
              (s,_) <- P.fdRead fd 1
              case s of
                [] -> error "fdGetLn: failed to read full line"
                ['\n'] -> pure []
                ~[c] -> do
                  cs <- lazyRead
                  pure (c:cs)

{-# INLINE record #-}
record :: ToJSON ev => P.Fd -> TransactionId -> ev -> IO ()
record fd tid e = do
  let (fptr, off, len) = BS.toForeignPtr (BSLC.toStrict $ encode_ (tid,e))
  withForeignPtr fptr $
    \wptr -> do
      P.fdWriteBuf fd (plusPtr wptr off) (fromIntegral len)
      P.fdWrite fd "\n"
      pure ()

{-# INLINE records #-}
records :: P.Fd -> BSB.Builder -> IO ()
records fd bsb = do
  let (fptr, off, len) = BS.toForeignPtr $ BSLC.toStrict $ BSB.toLazyByteString bsb
  !i <- withForeignPtr fptr $
    \wptr -> P.fdWriteBuf fd (plusPtr wptr off) (fromIntegral len)
  pure ()

{-# INLINE commit #-}
commit :: P.Fd -> TransactionId -> IO ()
commit fd tid = do
  P.fdSeek fd AbsoluteSeek 0
  let 
    commitString = 
      let str = show tid
      in '1':str ++ (replicate (10 - List.length str) ' ')
  P.fdWrite fd (commitString ++ "\n")
  P.fdSeek fd SeekFromEnd 0
  pure ()

--------------------------------------------------------------------------------
-- Manager; maintains aggregators and an event Stream; accepts and dispatches
-- reads and writes; manages shutting down aggregators when no more messages
-- are available and triggering de-registration of itself in the sorcerer.

data ManagerMsg
  = Stream Event
  | Persisted

data ManagerState = ManagerState
  { reopen :: Maybe (IO ())
  , shutdown :: IO ()
  }

-- a call to suspend in manager must be within a `withEmptyQueue`
{-# INLINE unsafeSuspendManager #-}
unsafeSuspendManager :: MVar ManagerState -> IO () -> IO () -> IO ()
unsafeSuspendManager st_ sd resume =
  modifyMVar_ st_ $ \ManagerState {..} -> do
    case reopen of
      Just _ -> error "suspend: invariant broken; manager already suspended"
      _      -> pure (ManagerState (Just resume) sd)

{-# INLINE resumeManager #-}
resumeManager :: MVar ManagerState -> IO ()
resumeManager st_ = 
  modifyMVar_ st_ $ \ManagerState {..} -> do
    fromMaybe (return ()) reopen
    pure (ManagerState Nothing (pure ()))

type Manager = (Queue ManagerMsg,MVar ManagerState)

{-# INLINE suspend #-}
suspend :: Manager -> IO () -> IO () -> IO () -> IO Bool
suspend (q_,st_) shutdown resume done = 
  withEmptyQueue q_ (unsafeSuspendManager st_ shutdown resume >> done)

{-# INLINE dispatch #-}
dispatch :: Manager -> ManagerMsg -> IO ()
dispatch (q_,st_) msg = do
  resumeManager st_ 
  arrive q_ msg

-- Extra harmless whitespace may be possible in the stream file after a recovery.
-- consider lazily initializing aggregators for read-heavy workloads
manager :: forall ev. Source ev => [Listener] -> IO () -> Stream ev -> Manager -> IO ()
manager ls done s mgr@(q_,st_) = do
  createDirectoryIfMissing True (takeDirectory fp)
  initialize
  where
    fp = stream @ev s

    count = List.length ls

    initialize = do
      fd <- P.openFd fp P.ReadWrite (Just $ P.unionFileModes P.ownerReadMode P.ownerWriteMode) P.defaultFileFlags
      P.setFdOption fd P.SynchronousWrites True
      !tid <- resumeLog @ev fd fp 
      chan <- startAggregators s fp tid ls
      start fd chan tid 

    start fd ch = \tid -> Control.Monad.void $ forkIO $ go tid 0
      where
        go :: TransactionId -> Int -> IO ()
        go = running
          where
            running :: TransactionId -> Int -> IO ()
            running !tid !i = do
              ms <- collect q_
              (!evs',!c',!newtid,!i') <- foldM fold (mempty,0,tid,i) ms
              isClosing <- withEmptyQueue q_ $
                writeChan ch (Persist (arrive q_ Persisted))
              when (c' > 0) $ do
                records fd evs'
                commit fd newtid
              if isClosing then 
                closing newtid (i' + count)
              else 
                running newtid i'
              where
                fold (evs,c,tid,i) msg =
                  case msg of
                    Persisted -> 
                      pure (evs,c,tid,i - 1)

                    Stream ev@(Write _ _ e) -> do
                      let newTid = tid + 1
                      e <- case cast e :: Maybe ev of
                        Just ev -> pure ev
                        Nothing -> error "manager: Invariant broken; invalid message type"
                      writeChan ch (AggregatorEvent newTid ev)
                      let bs = BSB.lazyByteString (encode_ (newTid,e)) <> "\n"
                      pure (evs <> bs,c + 1,newTid,i)
                    
                    Stream ev@(Read e _ a _) -> do
                      writeChan ch (AggregatorEvent tid ev)
                      pure (evs,c,tid,i)

                    Stream ev@(Transact _ _ _ e _) -> do
                      let newTid = tid + 1
                      e <- case cast e :: Maybe ev of
                        Just ev -> pure ev
                        Nothing -> error "manager: Invariant broken; invalid message type"
                      writeChan ch (AggregatorEvent newTid ev)
                      let bs = BSB.lazyByteString (encode_ (newTid,e)) <> "\n"
                      pure (evs <> bs,c + 1,newTid,i)

            closing :: TransactionId -> Int -> IO ()
            closing !tid !i = do
              ms <- collect q_
              (!evs',!c',!isClosing,!newtid,!i') <- foldM fold (mempty,0,True,tid,i) ms
              when (c' > 0) $ do
                records fd evs'
                commit fd newtid
              if isClosing && i' == 0 then do
                let shutdown = do
                      writeChan ch Shutdown
                      P.closeFd fd
                closed <- suspend mgr shutdown (Control.Monad.void $ forkIO $ running newtid 0) done
                unless closed $ running newtid 0
              else if isClosing then
                closing newtid i'
              else
                running newtid i'
              where
                fold (evs,c,isClosing,tid,i) msg =
                  case msg of
                    Persisted -> 
                      pure (evs,c,isClosing,tid,i - 1)

                    Stream ev@(Write _ _ e) -> do
                      let newTid = tid + 1
                      e <- case cast e :: Maybe ev of
                        Just ev -> pure ev
                        Nothing -> error "manager: Invariant broken; invalid message type"
                      writeChan ch (AggregatorEvent newTid ev)
                      let bs = BSB.lazyByteString (encode_ (newTid,e)) <> "\n"
                      pure (evs <> bs,c + 1,False,newTid,i)
            
                    Stream ev@(Read _ _ _ _) -> do
                      writeChan ch (AggregatorEvent tid ev)
                      -- I think it's safe to keep closing with a read event in flight
                      pure (evs,c,isClosing,tid,i)

                    Stream ev@(Transact _ _ _ e _) -> do
                      let newTid = tid + 1
                      e <- case cast e :: Maybe ev of
                        Just ev -> pure ev
                        Nothing -> error "manager: Invariant broken; invalid message type"
                      writeChan ch (AggregatorEvent newTid ev)
                      let bs = BSB.lazyByteString (encode_ (newTid,e)) <> "\n"
                      pure (evs <> bs,c + 1,False,newTid,i)

data SorcererEnv = SorcererEnv 
  { listeners :: IntMap.IntMap [Listener] }

data SorcererModel 
  = SorcererModel
    { sourcing :: !(IntMap.IntMap (IntMap.IntMap Manager)) 
    }

data SorcererMsg
  = Startup
  | Done Int Int
  | SorcererEvent Event

-- Get the current value of an aggregate.
{-# INLINE read' #-}
read' :: forall ev ag. (Hashable (Stream ev), Aggregable ev ag) => Stream ev -> IO (Maybe ag)
read' s = do
  let 
    !ev = case typeRepFingerprint (typeOf (undefined :: ev)) of Fingerprint x _ -> fromIntegral x
    !ag = case typeRepFingerprint (typeOf (undefined :: ag)) of Fingerprint x _ -> fromIntegral x
  mv <- newEmptyMVar
  publish (SorcererEvent (Read ev s ag (putMVar mv)))
  takeMVar mv

{-# INLINE read #-}
read :: forall ev ag. (FromJSON ag, Aggregable ev ag) => Stream ev -> IO (Maybe ag)
read s = do
  let fp = dropExtension (stream s) </> aggregate @ev @ag
  exists <- doesFileExist fp
  if exists then do
    cnt <- BSLC.readFile fp
    case BSLC.lines cnt of
      (_:ag:_) -> pure (decode_ ag)
      _ -> pure Nothing
  else
    pure Nothing

-- Write an event to an event stream.
{-# INLINE write #-}
write :: forall ev. (Hashable (Stream ev), Source ev) => Stream ev -> ev -> IO ()
write s ev = 
  let !ety = case typeRepFingerprint (typeOf (undefined :: ev)) of Fingerprint x _ -> fromIntegral x
  in publish (SorcererEvent (Write ety s ev))

-- Transactional version of write s ev >> read s.
{-# INLINE transact #-}
transact :: forall ev ag. (Hashable (Stream ev), Aggregable ev ag) => Stream ev -> ev -> IO (Maybe (Maybe ag))
transact s ev = do
  let
    !ety = case typeRepFingerprint (typeOf (undefined :: ev)) of Fingerprint x _ -> fromIntegral x
    !aty = case typeRepFingerprint (typeOf (undefined :: ag)) of Fingerprint x _ -> fromIntegral x
  mv <- newEmptyMVar
  publish (SorcererEvent (Transact ety s aty ev (\_ -> putMVar mv)))
  takeMVar mv

{-# INLINE observe #-}
observe :: forall ev ag. (Hashable (Stream ev), Aggregable ev ag) => Stream ev -> ev -> IO (Maybe ag,Maybe (Maybe ag))
observe s ev = do
  let
    !ety = case typeRepFingerprint (typeOf (undefined :: ev)) of Fingerprint x _ -> fromIntegral x
    !aty = case typeRepFingerprint (typeOf (undefined :: ag)) of Fingerprint x _ -> fromIntegral x
  mv <- newEmptyMVar
  publish (SorcererEvent (Transact ety s aty ev (\before after -> putMVar mv (before,after))))
  takeMVar mv

{-
-- deprecated in favor of Fd approach to enable laziness and avoid opening aggregates
{-# INLINE events' #-}
events' :: forall ev. (Hashable (Stream ev),Source ev) => Stream ev -> IO [ev]
events' s = do
  let !ety = case typeRepFingerprint (typeOf (undefined :: ev)) of
              Fingerprint x _ -> fromIntegral x
  mv <- newEmptyMVar
  publish (SorcererEvent (Log ety s (putMVar mv)))
  takeMVar mv
-}

-- lazily read events from a FD (to avoid GHCs single-writer OR multiple reader
-- constraint). We know that all events (new lines) are immutable after commit,
-- so it is safe to read those events, and we can stream them on demand.
{-# INLINE events #-}
events :: forall ev. (Hashable (Stream ev), Source ev) => Stream ev -> IO [ev]
events s = try (getEvents (stream @ev s)) >>= either (\(_ :: SomeException) -> pure []) pure

-- sorcerer_ True guarantees events are flushed and not buffered
sorcerer :: [Listener] -> View
sorcerer ls = run app env
  where
    toListenerMap :: [Listener] -> IntMap.IntMap [Listener]
    toListenerMap ls = IntMap.fromListWith (++) [ (k,[l]) | l@(Listener (k,_) _ _) <- ls ]
    app = App [Startup] [] [] mdl update view
    mdl = SorcererModel mempty
    env = SorcererEnv (toListenerMap ls)
    update :: Elm SorcererMsg => SorcererMsg -> SorcererEnv -> SorcererModel -> IO SorcererModel
    update msg env !mdl = do
      case msg of
        Startup -> do
          subscribe
          pure mdl

        Done ty s ->
          case IntMap.lookup ty (sourcing mdl) of
            Nothing -> pure mdl
            Just managers ->
              case IntMap.lookup s managers of
                Nothing  -> pure mdl
                Just (q_,st_) -> do
                  empty <- isEmptyQueue q_
                  if empty then do
                    ms <- takeMVar st_
                    case reopen ms of
                      Nothing -> do
                        putMVar st_ ms
                        pure mdl
                      Just _ -> do
                        shutdown ms
                        pure mdl 
                          { sourcing =
                            let managers' = IntMap.delete s managers
                            in if IntMap.null managers' then 
                                  IntMap.delete ty (sourcing mdl)
                                else 
                                  IntMap.insert ty managers' (sourcing mdl)
                          }
                  else
                    pure mdl

        SorcererEvent ev -> do
          let 
            go :: forall ev. Source ev => Int -> Stream ev -> IO SorcererModel
            go _ty s = do
              case IntMap.lookup _ty (sourcing mdl) of
                Nothing ->
                  case IntMap.lookup _ty (listeners env) of
                    Nothing -> pure mdl
                    Just ls -> do
                      q_ <- newQueue
                      arrive q_ (Stream ev)
                      st_ <- newMVar (ManagerState Nothing (pure ()))
                      let 
                        mgr = (q_,st_)
                        managers = IntMap.singleton (hash s) mgr
                        done = publish (Done _ty (hash s))
                      manager ls done s mgr
                      pure mdl 
                        { sourcing = IntMap.insert _ty managers (sourcing mdl)
                        }
                Just managers ->
                  case IntMap.lookup (hash s) managers of
                    Nothing ->
                      case IntMap.lookup _ty (listeners env) of
                        Nothing -> pure mdl
                        Just ls -> do
                          q_ <- newQueue
                          arrive q_ (Stream ev)
                          st_ <- newMVar (ManagerState Nothing (pure ()))
                          let 
                            mgr = (q_,st_)
                            managers' = IntMap.insert (hash s) mgr managers
                            done = publish (Done _ty (hash s))
                          manager ls done s mgr
                          pure mdl 
                            { sourcing = IntMap.insert _ty managers' (sourcing mdl)
                            }
                    Just mgr -> do
                      dispatch mgr (Stream ev)
                      pure mdl
          case ev of
            Read _ty s  _ _    -> go _ty s
            Write _ty s _      -> go _ty s
            Transact _ty s _ _ _ -> go _ty s

    view _ _ = Pure.Elm.Null

{-
-- Simple counter example

instance Source () where
  data Stream () = CounterId {-# UNPACK #-}!Int
    deriving (Generic,Hashable)

instance Aggregable () Counter where
  update () mc = Just $
    case mc of
      Nothing -> Counter 1
      Just (Counter i) -> Counter (i + 1)

data Counter = Counter {-# UNPACK #-}!Int 
  deriving (Generic,ToJSON,FromJSON)

increment :: Stream () -> IO ()
increment i = ES.write i ()

decrement :: Stream () -> IO ()
decrement i = ES.write i ()

test = sorcerer
  [ listener @() @Counter
  ]

-}

data SorceryEnv = SorceryEnv

data SorceryModel = SorceryModel
  { untargeted :: !(IntMap.IntMap                (IntMap.IntMap (Event -> IO Bool)))
  , targeted   :: !(IntMap.IntMap (IntMap.IntMap (IntMap.IntMap (Event -> IO Bool))))
  }

data SorceryMsg
  = StartupSorcery
  | SorcererMsg SorcererMsg
  | ListenUntargeted Int Int (Event -> IO Bool)
  | UnlistenUntargeted Int Int 
  | ListenTargeted Int Int Int (Event -> IO Bool)
  | UnlistenTargeted Int Int Int 

newtype TargetedListener = MkTargetedListener (IO ())
newtype UntargetedListener = MkUntargetedListener (IO ())

class Unlisten listener where
  unlisten :: listener -> IO ()

instance Unlisten TargetedListener where
  unlisten (MkTargetedListener unl) = unl

instance Unlisten UntargetedListener where
  unlisten (MkUntargetedListener unl) = unl

listenTargeted :: forall ev. Source ev => Stream ev -> (ev -> IO ()) -> IO TargetedListener
listenTargeted st f = do
  let
    ev = 
      case typeRepFingerprint (typeOf (undefined :: ev)) of
        Fingerprint x _ -> fromIntegral x
    h = hash st
    g (Write _ _ ev) = 
      case cast ev :: Maybe ev of
        Just e -> f e
        _ -> error "Sorcerer: Invariant broken: mistargeted listener event"
    g (Transact _ _ _ ev _) =
      case cast ev :: Maybe ev of
        Just e -> f e
        _ -> error "Sorcerer: Invariant broken: mistargeted listener event"
    g _ = pure ()
  u <- hashUnique <$> newUnique
  publish (ListenTargeted ev h u (\ev -> g ev >> pure True))
  pure (MkTargetedListener (publish $ UnlistenTargeted ev h u))

listenUntargeted :: forall ev. Source ev => (ev -> IO ()) -> IO UntargetedListener
listenUntargeted f = do
  let
    ev = 
      case typeRepFingerprint (typeOf (undefined :: ev)) of
        Fingerprint x _ -> fromIntegral x
    g (Write _ _ ev) = 
      case cast ev :: Maybe ev of
        Just e -> f e >> pure True
        _ -> error "Sorcerer: Invariant broken: mistargeted listener event"
    g (Transact _ _ _ ev _) = 
      case cast ev :: Maybe ev of
        Just e -> f e >> pure True
        _ -> error "Sorcerer: Invariant broken: mistargeted listener event"
    g _ = pure True
  u <- hashUnique <$> newUnique
  publish (ListenUntargeted ev u (\ev -> g ev >> pure True))
  pure (MkUntargetedListener (publish $ UnlistenUntargeted ev u))

listenTargetedEvent :: forall ev. Source ev => Stream ev -> (Event -> IO ()) -> IO TargetedListener
listenTargetedEvent st f = do
  let
    ev = 
      case typeRepFingerprint (typeOf (undefined :: ev)) of
        Fingerprint x _ -> fromIntegral x
    h = hash st
  u <- hashUnique <$> newUnique
  publish (ListenTargeted ev h u (\ev -> f ev >> pure True))
  pure (MkTargetedListener (publish $ UnlistenTargeted ev h u))

listenUntargetedEvent :: forall ev. Source ev => (Event -> IO ()) -> IO UntargetedListener
listenUntargetedEvent f = do
  let
    ev = 
      case typeRepFingerprint (typeOf (undefined :: ev)) of
        Fingerprint x _ -> fromIntegral x
  u <- hashUnique <$> newUnique
  publish (ListenUntargeted ev u (\ev -> f ev >> pure True))
  pure (MkUntargetedListener (publish $ UnlistenUntargeted ev u))

listenUntargetedEvent' :: forall ev. Source ev => Proxy ev -> (Event -> IO ()) -> IO UntargetedListener
listenUntargetedEvent' _ f = listenUntargetedEvent @ev f

subscribeStream :: forall ev msg. (Source ev, Elm msg) => (ev -> msg) -> Stream ev -> IO TargetedListener
subscribeStream inj st = do
  tid <- myThreadId
  let 
    htid = hash tid
    u =
      case signum htid of
        1 -> negate htid
        _ -> htid
    ev = 
      case typeRepFingerprint (typeOf (undefined :: ev)) of
        Fingerprint x _ -> fromIntegral x
    h = hash st
    g (Write _ _ ev) = 
      case cast ev :: Maybe ev of
        Just e -> ?command (inj e)
        _ -> error "Sorcerer: Invariant broken: mistargeted listener event"
    g (Transact _ _ _ ev _) =
      case cast ev :: Maybe ev of
        Just e -> ?command (inj e)
        _ -> error "Sorcerer: Invariant broken: mistargeted listener event"
    g _ = pure True
  publish (ListenTargeted ev h u g)
  pure (MkTargetedListener (publish $ UnlistenTargeted ev h u))

unsubscribeStream :: forall ev. Source ev => Stream ev -> IO ()
unsubscribeStream st = do
  tid <- myThreadId
  let 
    htid = hash tid
    u =
      case signum htid of
        1 -> negate htid
        _ -> htid
    h = hash st
    ev = 
      case typeRepFingerprint (typeOf (undefined :: ev)) of
        Fingerprint x _ -> fromIntegral x
  publish (UnlistenTargeted ev h u)

subscribeStreamEvent :: forall ev msg. (Source ev, Elm msg) => (Event -> msg) -> Stream ev -> IO TargetedListener
subscribeStreamEvent inj st = do
  tid <- myThreadId
  let 
    htid = hash tid
    u =
      case signum htid of
        1 -> negate htid
        _ -> htid
    ev = 
      case typeRepFingerprint (typeOf (undefined :: ev)) of
        Fingerprint x _ -> fromIntegral x
    h = hash st
  publish (ListenTargeted ev h u (?command . inj))
  pure (MkTargetedListener (publish $ UnlistenTargeted ev h u))

unsubscribeStreamEvent :: forall ev. (Source ev) => Stream ev -> IO ()
unsubscribeStreamEvent st = do
  tid <- myThreadId
  let 
    htid = hash tid
    u =
      case signum htid of
        1 -> negate htid
        _ -> htid
    h = hash st
    ev = 
      case typeRepFingerprint (typeOf (undefined :: ev)) of
        Fingerprint x _ -> fromIntegral x
  publish (UnlistenTargeted ev h u)

subscribeStreams :: forall ev msg. (Source ev, Elm msg) => (ev -> msg) -> IO UntargetedListener
subscribeStreams inj = do
  tid <- myThreadId
  let 
    htid = hash tid
    u =
      case signum htid of
        1 -> negate htid
        _ -> htid
    ev = 
      case typeRepFingerprint (typeOf (undefined :: ev)) of
        Fingerprint x _ -> fromIntegral x
    g (Write _ _ ev) = 
      case cast ev :: Maybe ev of
        Just e -> ?command (inj e)
        _ -> error "Sorcerer: Invariant broken: mistargeted listener event"
    g (Transact _ _ _ ev _) =
      case cast ev :: Maybe ev of
        Just e -> ?command (inj e)
        _ -> error "Sorcerer: Invariant broken: mistargeted listener event"
    g _ = pure True
  publish (ListenUntargeted ev u g)
  pure (MkUntargetedListener (publish $ UnlistenUntargeted ev u))

unsubscribeStreams :: forall ev. Source ev => IO ()
unsubscribeStreams = do
  tid <- myThreadId
  let 
    htid = hash tid
    u =
      case signum htid of
        1 -> negate htid
        _ -> htid
    ev = 
      case typeRepFingerprint (typeOf (undefined :: ev)) of
        Fingerprint x _ -> fromIntegral x
  publish (UnlistenUntargeted ev u)

unsubscribeStreams' :: forall ev. Source ev => Proxy ev -> IO ()
unsubscribeStreams' _ = unsubscribeStreams @ev

subscribeStreamsEvent :: forall ev msg. (Source ev, Elm msg) => (Event -> msg) -> IO UntargetedListener
subscribeStreamsEvent inj = do
  tid <- myThreadId
  let 
    htid = hash tid
    u =
      case signum htid of
        1 -> negate htid
        _ -> htid
    ev = 
      case typeRepFingerprint (typeOf (undefined :: ev)) of
        Fingerprint x _ -> fromIntegral x
  publish (ListenUntargeted ev u (?command . inj))
  pure (MkUntargetedListener (publish $ UnlistenUntargeted ev u))

subscribeStreamsEvent' :: forall ev msg. (Source ev, Elm msg) => Proxy ev -> (Event -> msg) -> IO UntargetedListener
subscribeStreamsEvent' _ inj = subscribeStreamsEvent @ev inj

unsubscribeStreamsEvent :: forall ev. Source ev => IO ()
unsubscribeStreamsEvent = do
  tid <- myThreadId
  let 
    htid = hash tid
    u =
      case signum htid of
        1 -> negate htid
        _ -> htid
    ev = 
      case typeRepFingerprint (typeOf (undefined :: ev)) of
        Fingerprint x _ -> fromIntegral x
  publish (UnlistenUntargeted ev u)

unsubscribeStreamsEvent' :: forall ev. Source ev => Proxy ev -> IO ()
unsubscribeStreamsEvent' _ = unsubscribeStreamsEvent @ev

-- Note about Listener ids:
--  With Elm subscriptions, we hash the ThreadId of the subscriber and then guarantee it is negative by negating as necessary.
--    This prevents multiple stream subscriptions from the same thread to the same stream or streams if untargeted, but simplifies
--    the call to unsubscribe by not requiring storage of a TargetedListener or an UntargetedListener.
--  With regular subscribers, we hash a unique which is guaranteed to be positive by the current (could change) hashUnique implementation.
--    This allows any number of subscriptions, but requires storage of a TargetedListener/UntargetedListener
--
-- Basically, Listener IDs are positive if they're constructed via Uniques and negative if they're constructed via ThreadIds.
sorcery :: View
sorcery = run app env
  where
    env = SorceryEnv
    app = App [StartupSorcery] [] [] mdl update view
    mdl = SorceryModel mempty mempty
    update :: Elm SorceryMsg => SorceryMsg -> SorceryEnv -> SorceryModel -> IO SorceryModel
    update msg env !mdl =
      case msg of
        StartupSorcery -> do
          subscribe
          subscribeWith SorcererMsg
          pure mdl

        SorcererMsg (SorcererEvent ev) -> do
          let 
            (ty,h) =
              case ev of
                Read _ty st _ _       -> (_ty,hash st)
                Write _ty st _        -> (_ty,hash st)
                Transact _ty st _ _ _ -> (_ty,hash st)

          untargeted' <-
            case IntMap.lookup ty (untargeted mdl) of
              Just ues -> do
                results <- traverse ($ ev) ues
                let 
                  remove Nothing = Nothing
                  remove (Just ufs) =
                    let ufs' = IntMap.difference ufs (IntMap.filter not results)
                     in bool (Just ufs') Nothing (IntMap.null ufs')
                pure (IntMap.alter remove ty (untargeted mdl))
              Nothing -> 
                pure (untargeted mdl)

          targeted' <- 
            case IntMap.lookup ty (targeted mdl) of
              Nothing -> pure (targeted mdl)
              Just im ->
                case IntMap.lookup h im of
                  Just ues -> do
                    results <- traverse ($ ev) ues
                    let 
                      remove Nothing = Nothing
                      remove (Just stufs) = 
                        let stufs' = IntMap.alter remove' h stufs
                         in bool (Just stufs') Nothing (IntMap.null stufs')
                      remove' Nothing = Nothing
                      remove' (Just ufs) = 
                        let ufs' = IntMap.difference ufs (IntMap.filter not results)
                         in bool (Just ufs') Nothing (IntMap.null ufs')
                    pure (IntMap.alter remove ty (targeted mdl))
                  Nothing -> 
                    pure (targeted mdl)

          pure mdl { untargeted = untargeted', targeted = targeted' }

        ListenUntargeted ty u f ->
          let 
            add Nothing    = Just $ IntMap.singleton u f
            add (Just ufs) = Just $ IntMap.insert u f ufs
           in 
            pure mdl { untargeted = IntMap.alter add ty (untargeted mdl) }

        UnlistenUntargeted ty u ->
          let
            remove Nothing = Nothing
            remove (Just ufs) =
              let ufs' = IntMap.delete u ufs
               in bool (Just ufs') Nothing (IntMap.null ufs')
           in
            pure mdl { untargeted = IntMap.alter remove ty (untargeted mdl) }

        ListenTargeted ty st u f ->
          let 
            add Nothing      = Just $ IntMap.singleton st (IntMap.singleton u f)
            add (Just stufs) = Just $ IntMap.alter add' st stufs
            add' Nothing     = Just $ IntMap.singleton u f
            add' (Just ufs)  = Just $ IntMap.insert u f ufs
           in
            pure mdl { targeted = IntMap.alter add ty (targeted mdl) }

        UnlistenTargeted ty st u ->
          let 
            remove Nothing = Nothing
            remove (Just stufs) =
              let stufs' = IntMap.alter remove' st stufs
               in bool (Just stufs') Nothing (IntMap.null stufs')
            remove' Nothing = Nothing
            remove' (Just ufs) =
              let ufs' = IntMap.delete u ufs
               in bool (Just ufs') Nothing (IntMap.null ufs')
           in 
            pure mdl { targeted = IntMap.alter remove ty (targeted mdl) }

        -- Other SorcererMsgs are ignored
        _ -> pure mdl

    view _ _ = Pure.Elm.Null




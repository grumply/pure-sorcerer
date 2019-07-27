{-# LANGUAGE ScopedTypeVariables, BangPatterns, DefaultSignatures, TypeFamilies,
  DeriveAnyClass, AllowAmbiguousTypes, TypeApplications, RecordWildCards, 
  MultiParamTypeClasses, FlexibleContexts, ExistentialQuantification, 
  DeriveGeneric, RankNTypes, LambdaCase #-}
module Sorcerer (sorcerer_,sorcerer,read,unsafeRead,write,events,Listener,listener,Source(..),Aggregable(..)) where

import Pure.Elm hiding (Left,Right,(<.>),Listener,listeners,record,write)

import Data.Aeson as A
import qualified Data.ByteString as BS
import qualified Data.ByteString.Internal as BS
import qualified Data.ByteString.Lazy.Internal as BSL
import qualified Data.ByteString.Char8 as BSC
import qualified Data.ByteString.Lazy.Char8 as BSLC

import Control.Concurrent
import Control.Exception
import Control.Monad
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
-- A Handle for use in asynchronous cases with multiple readers/writers.
-- Specifically for supporting a lazy seeking reader a la `unsafeInterleaveIO`,
-- and an atomic logger with a multi-phase seeking write.

-- Stream a lazy ByteString on-demand from a shared Handle ignoring `beginning` bytes.
{-# INLINE sGetContentsFrom #-}
sGetContentsFrom :: Int -> MVar Handle -> IO BSL.ByteString
sGetContentsFrom beginning mh = start
  where
    decode_ ln =
      case A.decode ln of
        Nothing -> Nothing
        Just a -> Just a

    start = do
      h <- takeMVar mh
      end <- hTell h
      putMVar mh h
      go (fromIntegral end) beginning
      where
        go end = lazyRead 
          where

            lazyRead off = unsafeInterleaveIO (loop off)

            loop off = do
              h <- takeMVar mh 
              reset <- hTell h
              hSeek h AbsoluteSeek (fromIntegral off)
              let
                eo = end - off
                mk = if eo < BSL.defaultChunkSize then Just eo else Nothing
              c <- BS.hGetSome h (fromMaybe BSL.defaultChunkSize mk)
              hSeek h AbsoluteSeek reset
              c `seq` putMVar mh h
              case mk of
                Nothing -> do
                  cs <- lazyRead (off + BSL.defaultChunkSize) 
                  return (BSL.Chunk c cs)
                Just _  ->
                  return (BSL.Chunk c mempty)

--------------------------------------------------------------------------------
-- Custom streaming methods that avoid the first 13 bytes of a file
-- and lazily stream each line as a JSON value, encoded or decoded.

{-# INLINE events' #-}
events' :: FromJSON ev => MVar Handle -> IO [ev]
events' s = fmap (expectSuccess . A.fromJSON . snd) <$> transactions' s
  where
    expectSuccess (A.Success a) = a
    expectSuccess (Error s) = error $ "events': Invariant broken; invalid event: " ++ s

{-# INLINE transactions' #-}
transactions' :: MVar Handle -> IO [(A.Value,A.Value)]
transactions' s = fmap fromJust <$> transactions s

{-# INLINE transactions #-}
transactions :: MVar Handle -> IO [Maybe (A.Value,A.Value)]
transactions s = (fmap A.decode . BSLC.lines) <$> sGetContentsFrom 13 s

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

  update :: ev -> Maybe ag -> Maybe ag

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
  | forall ev ag. (Hashable (Stream ev), Source ev) => Log
    { _ty     :: {-# UNPACK #-}!Int
    , _ident  :: Stream ev
    , withLog :: [ev] -> IO () -- the list will be strict
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
  { aeEvents   :: MVar Handle
  , aeMessages :: Chan AggregatorMsg
  , aeLatest   :: TransactionId
  }

data AggregatorMsg 
  = AggregatorEvent TransactionId Event
  | Persist (IO ())
  | Shutdown

{-# INLINE aggregator #-}
aggregator :: forall ev ag. (ToJSON ag, FromJSON (Aggregate ag), ToJSON (Aggregate ag), Aggregable ev ag) 
           => FilePath -> AggregatorEnv -> IO ()
aggregator fp AggregatorEnv {..} = do
  createDirectoryIfMissing True (takeDirectory fp)
  void $ forkIO $ do
    ag <- prepare 
    ms <- getChanContents aeMessages
    foldM_ run ag ms
  where
    prepare :: IO (Aggregate ag)
    prepare = do
      try (A.decodeFileStrict fp) >>= either synthesize synchronize
      where

        synthesize :: SomeException -> IO (Aggregate ag)
        synthesize _ 
          | aeLatest == 0 = pure (Aggregate 0 Nothing)
          | otherwise = fold 0 Nothing

        synchronize :: Maybe (Aggregate ag) -> IO (Aggregate ag)
        synchronize (Just ag)
          | aeLatest == aCurrent ag = pure ag
          | otherwise               = fold (aCurrent ag) (aAggregate ag)
        synchronize Nothing         = fold 0 Nothing

        fold :: Int -> Maybe ag -> IO (Aggregate ag)
        fold from initial = do
          vs <- events' aeEvents
          let 
            !mag = List.foldl' (flip (update @ev @ag)) initial (List.drop from vs)
            cur = Aggregate aeLatest mag
          A.encodeFile (fp <> ".temp") cur 
          renameFile (fp <> ".temp") fp
          pure cur

    run :: Aggregate ag -> AggregatorMsg -> IO (Aggregate ag)
    run cur@Aggregate {..} am =
      case am of
        AggregatorEvent tid e ->
          case e of
            Write _ _ ev ->
              case cast ev of
                Just e -> let ag = (update @ev @ag) e aAggregate in pure (Aggregate tid ag)
                _      -> error "aggregator.runner: invariant broken; received impossible write event"

            Read _ _ _ cb -> do
              print "Reading"
              case cast cb :: Maybe (Maybe ag -> IO ()) of
                Just f -> f aAggregate >> pure cur
                _      -> error "aggregator.runner: invariant broken; received impossible read event"

        Persist persisted -> do
          A.encodeFile (fp <> ".temp") cur 
          renameFile (fp <> ".temp") fp
          persisted 
          pure cur

        Shutdown -> myThreadId >>= killThread >> pure cur

--------------------------------------------------------------------------------
-- Abstract representation of an aggregator that ties into Source/Aggregable for
-- easy instantiation.

data Listener = Listener 
  { _lty :: (Int,Int)
  , _aggregate :: FilePath
  , _listener :: FilePath -> AggregatorEnv -> IO ()
  }

-- listener @SomeEvent @SomeAggregate
listener :: forall ev ag. (ToJSON ag, FromJSON (Aggregate ag), ToJSON (Aggregate ag), Aggregable ev ag) => Listener
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
startAggregators :: Source ev => Stream ev -> MVar Handle -> TransactionId -> [Listener] -> IO (Chan AggregatorMsg)
startAggregators s mh tid ls = do
  chan <- newChan
  for_ ls $ \(Listener i fp ag) -> do
    ch <- dupChan chan
    ag (dropExtension (stream s) </> fp) (AggregatorEnv mh ch tid)
  pure chan

--------------------------------------------------------------------------------
-- Log resumption; should be able to safely recover a majority of failures

{-# INLINE resumeLog #-}
resumeLog :: forall ev. Source ev => MVar Handle -> FilePath -> IO TransactionId
resumeLog mh fp = do
  h <- takeMVar mh
  hSeek h AbsoluteSeek 0
  eof <- hIsEOF h
  i <- if eof then newStream h else recover h
  putMVar mh h
  pure i
  where
    newStream :: Handle -> IO TransactionId
    newStream h = do
      hPutStrLn h "00          "
      pure 0

    recover :: Handle -> IO TransactionId
    recover h = do
      ln <- hGetLine h
      i <- 
        case ln of
          '1':tid ->
            case readMaybe tid of
              Nothing -> repair h
              Just i  -> pure i
          '0':tid -> repair h
          _ -> error $ "manager: unrecoverable steram file " ++ fp

      hSeek h AbsoluteSeek 0
      hPutChar h '0'
      hSeek h SeekFromEnd 0

      pure i

    repair :: Handle -> IO TransactionId
    repair h = do
      sane <- hasTrailingNewline
      hSeek h SeekFromEnd 0
      i <-
        if sane then do
          findNthFromLastNewline 1
          ln <- BSC.hGetLine h
          case A.decodeStrict ln :: Maybe (Int,A.Value) of
            Just (i,_) -> do
              commit i
              pure i
            Nothing -> 
              error $ "resumeLog.repair: unrecoverable event stream: " ++ fp
        else do
          end <- hTell h
          findNthFromLastNewline 0
          off <- hTell h
          let count = fromIntegral (end - off)
          BSC.hPutStr h (BSC.replicate count ' ')
          findNthFromLastNewline 1
          ln <- BSC.hGetLine h
          case A.decodeStrict ln :: Maybe (Int,A.Value) of
            Just (i,_) -> do
              commit i
              pure i
            Nothing ->
              error $ "resumeLog.repair: unrecoverable event stream: " ++ fp
         
      hSeek h SeekFromEnd 0

      pure i

      where
        hasTrailingNewline :: IO Bool
        hasTrailingNewline = do
          hSeek h SeekFromEnd (-1)
          c <- hGetChar h
          pure (c == '\n')

        findNthFromLastNewline :: Int -> IO ()
        findNthFromLastNewline n = go 0 1
          where
            go i p = do
              hSeek h SeekFromEnd (negate p)
              c <- hGetChar h
              if c == '\n' then
                if n == i then 
                  pure ()
                else
                  go (i + 1) (p + 1)
              else
                go i (p + 1)

        commit :: Int -> IO ()
        commit c = do
          hSeek h AbsoluteSeek 0
          hPutStrLn h (replicate 12 ' ')
          hSeek h AbsoluteSeek 0
          hPutStr h ('1':show c)

{-# INLINE record #-}
record :: ToJSON ev => MVar Handle -> TransactionId -> ev -> IO ()
record mh tid e = do
  h <- takeMVar mh
  BSLC.hPutStrLn h (A.encode (tid,e))
  putMVar mh h

{-# INLINE commit #-}
commit :: Handle -> TransactionId -> IO ()
commit h tid = do
  hSeek h AbsoluteSeek 0
  let 
    commitString = 
      let str = show tid
      in '1':str ++ (replicate (11 - List.length str) ' ')
  hPutStrLn h commitString
  hSeek h SeekFromEnd 0

--------------------------------------------------------------------------------
-- Manager; maintains aggregators and an event Stream; accepts and dispatches
-- reads and writes; manages shutting down aggregators when no more messages
-- are available and triggering de-registration of itself in the sorcerer.

data ManagerMsg
  = Stream Event
  | Events ([A.Value] -> IO ())
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
manager :: forall ev. Source ev => Bool -> [Listener] -> IO () -> Stream ev -> Manager -> IO ()
manager tx ls done s mgr@(q_,st_) = do
  createDirectoryIfMissing True (takeDirectory fp)
  initialize
  where
    fp = stream @ev s

    count = List.length ls

    initialize = do
      h <- openBinaryFile fp ReadWriteMode
      hSetBuffering h (if tx then LineBuffering else BlockBuffering Nothing)
      mh <- newMVar h
      tid <- resumeLog @ev mh fp 
      chan <- startAggregators s mh tid ls
      start mh chan tid 

    start mh ch = \tid -> void $ forkIO $ go tid 0 
      where
        go :: TransactionId -> Int -> IO ()
        go = running
          where
            running tid i = do
              ms <- collect q_
              (newtid,i') <- foldM fold (tid,i) ms
              isClosing <- withEmptyQueue q_ $
                writeChan ch (Persist (arrive q_ Persisted))
              if isClosing then 
                closing newtid (i' + count)
              else 
                running newtid i'
              where
                fold (tid,i) msg =
                  case msg of
                    Persisted -> 
                      pure (tid,i - 1)

                    Events f -> do
                      es <- events' mh
                      f es
                      pure (tid,i)

                    Stream ev@(Write _ _ e) -> do
                      let newTid = tid + 1
                      case cast e :: Maybe ev of
                        Just ev -> record mh newTid ev
                        Nothing -> error "manager: Invariant broken; invalid message type"
                      writeChan ch (AggregatorEvent newTid ev)
                      pure (newTid,i)
                    
                    Stream ev@(Read e _ a _) -> do
                      writeChan ch (AggregatorEvent tid ev)
                      pure (tid,i)

                    Stream (Log e _ f) -> do
                      case cast f :: Maybe ([ev] -> IO ()) of
                        Just g -> do
                          es <- events' mh
                          let l = List.length es
                          l `seq` f es
                        Nothing -> pure ()
                      pure (tid,i)

            closing tid i = do
              ms <- collect q_
              (isClosing,newtid,i') <- foldM fold (True,tid,i) ms
              if isClosing && i' == 0 then do
                let shutdown = do
                      h <- takeMVar mh
                      commit h newtid
                      writeChan ch Shutdown
                      hClose h
                closed <- suspend mgr shutdown (start mh ch newtid) done
                unless closed $ 
                  running newtid 0
              else if isClosing then
                closing newtid i'
              else
                running newtid i'
              where
                fold (isClosing,tid,i) msg =
                  case msg of
                    Persisted -> 
                      pure (isClosing,tid,i - 1)

                    Events f -> do
                      es <- events' mh
                      f es
                      pure (isClosing,tid,i)

                    Stream ev@(Write _ _ e) -> do
                      let newTid = tid + 1
                      case cast e :: Maybe ev of
                        Just ev -> record mh newTid ev
                        Nothing -> error "manager: Invariant broken; invalid message type"
                      writeChan ch (AggregatorEvent newTid ev)
                      pure (False,newTid,i)
            
                    Stream ev@(Read e _ a _) -> do
                      writeChan ch (AggregatorEvent tid ev)
                      -- I think it's safe to keep closing with a read event in flight
                      pure (isClosing,tid,i)

                    Stream (Log e _ f) -> do
                      case cast f :: Maybe ([ev] -> IO ()) of
                        Just g -> do
                          es <- events' mh
                          let l = List.length es
                          l `seq` f es
                        Nothing -> pure ()
                      pure (isClosing,tid,i)

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

{-# INLINE read #-}
read :: forall ev ag. (Hashable (Stream ev), Aggregable ev ag) => Stream ev -> IO (Maybe ag)
read s = do
  let 
    !ev = case typeRepFingerprint (typeOf (undefined :: ev)) of
          Fingerprint x _ -> fromIntegral x
    !ag = case typeRepFingerprint (typeOf (undefined :: ag)) of
          Fingerprint x _ -> fromIntegral x
  mv <- newEmptyMVar
  publish (SorcererEvent (Read ev s ag (putMVar mv)))
  takeMVar mv

{-# INLINE unsafeRead #-}
unsafeRead :: forall ev ag. (FromJSON (Aggregate ag), Aggregable ev ag) => Stream ev -> IO (Maybe ag)
unsafeRead s = do
  let fp = dropExtension (stream s) </> aggregate @ev @ag
  esemag <- try (A.decodeFileStrict fp)
  case esemag of
    Left (_ :: SomeException) -> pure Nothing
    Right mag -> pure (join $ fmap aAggregate mag)

{-# INLINE write #-}
write :: forall ev. (Hashable (Stream ev), Source ev) => Stream ev -> ev -> IO ()
write s ev = 
  let !ety = case typeRepFingerprint (typeOf (undefined :: ev)) of
              Fingerprint x _ -> fromIntegral x
  in publish (SorcererEvent (Write ety s ev))

{-# INLINE events #-}
events :: forall ev. (Hashable (Stream ev),Source ev) => Stream ev -> IO [ev]
events s = do
  let !ety = case typeRepFingerprint (typeOf (undefined :: ev)) of
              Fingerprint x _ -> fromIntegral x
  mv <- newEmptyMVar
  publish (SorcererEvent (Log ety s (putMVar mv)))
  takeMVar mv

sorcerer :: [Listener] -> View
sorcerer = sorcerer_ False

-- sorcerer_ True guarantees events are flushed and not buffered
sorcerer_ :: Bool -> [Listener] -> View
sorcerer_ tx ls = run app env
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
                      manager tx ls done s mgr
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
                          manager tx ls done s mgr
                          pure mdl 
                            { sourcing = IntMap.insert _ty managers' (sourcing mdl)
                            }
                    Just mgr -> do
                      dispatch mgr (Stream ev)
                      pure mdl
          case ev of
            Read _ty s  _ _ -> go _ty s
            Write _ty s _   -> go _ty s
            Log _ty s _     -> go _ty s

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
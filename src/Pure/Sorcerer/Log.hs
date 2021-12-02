module Pure.Sorcerer.Log 
  ( Log
  , resume
  , record
  , commit
  , close
  ) where

import Pure.Sorcerer.Aggregate
import Pure.Sorcerer.JSON
import Pure.Sorcerer.Streamable

import Pure.Data.JSON (Value)

import qualified Data.ByteString.Internal as BS
import qualified Data.ByteString.Lazy.Builder as BSB
import qualified Data.ByteString.Lazy.Char8 as BSLC

import qualified System.Posix.Files       as P
import qualified System.Posix.IO          as P
import           System.Posix.Types       as P (Fd, FileOffset, ByteCount, COff)

import Foreign.Ptr
import Foreign.ForeignPtr
import Foreign.Marshal.Alloc
import Foreign.Storable
import System.IO
import Text.Read

newtype Log = Log P.Fd

resume :: forall ev. Streamable ev => FilePath -> IO (Log,TransactionId)
resume fp = do
  fd <- P.openFd fp P.ReadWrite (Just $ P.unionFileModes P.ownerReadMode P.ownerWriteMode) P.defaultFileFlags
  P.setFdOption fd P.SynchronousWrites True
  !tid <- resumeLog @ev fd fp 
  pure (Log fd,tid)

resumeLog :: forall ev. Streamable ev => P.Fd -> FilePath -> IO TransactionId
resumeLog fd fp = do
  off <- P.fdSeek fd SeekFromEnd 0
  if off == 0 then newStream else resume
  where
    -- Write statusline and return 0
    newStream :: IO TransactionId
    newStream = do
      commit 0
      pure 0

    -- Commit a transaction id to the statusline
    -- Has to write twice in case the transaction id is rolled back
    commit :: Int -> IO ()
    commit c = do
      P.fdSeek fd AbsoluteSeek 0
      P.fdWrite fd (replicate 13 ' ' ++ "\n")
      P.fdSeek fd AbsoluteSeek 0
      P.fdWrite fd ('1':show c)
      P.fdSeek fd SeekFromEnd 0
      pure ()

    resume :: IO TransactionId
    resume = do
      _ <- P.fdSeek fd AbsoluteSeek 0
      (ln,_) <- P.fdRead fd 13
      i <- 
        case ln of
          '1':tid | Just i <- readMaybe tid -> pure i
          _  :tid                           -> recover
      P.fdSeek fd SeekFromEnd 0
      pure i

    -- Recoverable failures:
    --   1. Power failed before transaction write completed; delete unfinished transaction and attempt to roll back to the previous transaction.
    --   2. Power failed during transaction commit; verify the latest transaction and attempt to commit it.
    recover :: IO TransactionId
    recover = do
      (o,ln) <- fdGetLnReverse (-1)
      case decode_ (BSLC.pack ln) :: Maybe (Int,Value) of
        Just (i,_) -> do
          commit i
          pure i
        Nothing -> do
          (_,ln) <- fdGetLnReverse o
          case decode_ (BSLC.pack ln) :: Maybe (Int,Value) of
            Just (i,_) -> do
              off <- P.fdSeek fd SeekFromEnd (o + 2) -- must not truncate the newline
              P.setFdSize fd off
              commit i
              pure i
            Nothing -> error $ Prelude.unlines
              [ "Pure.Sorcerer.Log.resumeLog.recover:" 
              , ""
              ,     "\tUnrecoverable event stream: "
              , ""
              ,     "\t\t" ++ fp
              , ""
              ,     "\tProblem:"
              ,         "\t\tThe latest commit was partial and the previous commit was not valid JSON."
              , ""
              ,     "\tSolution:"
              ,         "\t\tUnknown. This should not be possible using aeson for encoding." 
              ,         "\t\tReview the transaction stream manually to determine a solution." 
              ]
      where
        fdGetLnReverse :: FileOffset -> IO (FileOffset,String)
        fdGetLnReverse i = alloca $ \p -> go p [] i
          where
            go p = go'
              where
                go' s i = do
                  P.fdSeek fd SeekFromEnd i
                  P.fdReadBuf fd p 1
                  w <- peek p
                  let c = toEnum (fromIntegral w)
                  if c == '\n'
                    then pure (i - 1,s)
                    else go' (c:s) (i-1)

-- Assumes fd is at (SeekFromEnd 0)
record :: Log -> BSB.Builder -> IO ()
record (Log fd) bsb = do
  let (fptr, off, len) = BS.toForeignPtr $ BSLC.toStrict $ BSB.toLazyByteString bsb
  !i <- withForeignPtr fptr $ \wptr -> 
    P.fdWriteBuf fd (plusPtr wptr off) (fromIntegral len)
  pure ()

-- Assumes monotonically increasing transaction id in a valid transaction log
commit :: Log -> TransactionId -> IO ()
commit (Log fd) tid = do
  let 
    bsb = BSB.intDec 1 <> BSB.intDec tid
    (fptr,off,len) = BS.toForeignPtr $ BSLC.toStrict $ BSB.toLazyByteString bsb
  P.fdSeek fd AbsoluteSeek 0
  !i <- withForeignPtr fptr $ \wptr ->
    P.fdWriteBuf fd (plusPtr wptr off) (fromIntegral len)
  P.fdSeek fd SeekFromEnd 0
  pure ()

close :: Log -> IO ()
close (Log fd) = P.closeFd fd
module Pure.Sorcerer.Aggregate where

import Pure.Sorcerer.JSON

import Pure.Data.JSON (ToJSON,FromJSON)

import qualified Data.ByteString as BS
import qualified Data.ByteString.Internal as BS
import qualified Data.ByteString.Builder as BSB
import qualified Data.ByteString.Lazy.Char8 as BSLC

import qualified System.Posix.Files as P
import qualified System.Posix.IO as P
import System.Posix.Types as P (Fd, FileOffset, ByteCount, COff)

import Foreign.ForeignPtr (withForeignPtr)
import Foreign.Ptr (plusPtr)
import GHC.Generics (Generic)
import System.Directory (doesFileExist)

type TransactionId = Int

data Aggregate ag = Aggregate
  { current   :: TransactionId
  , aggregate :: Maybe ag
  } deriving stock Generic
    deriving anyclass (ToJSON,FromJSON)

{-# INLINE writeAggregate #-}
writeAggregate :: ToJSON ag => FilePath -> TransactionId -> Maybe ag -> IO ()
writeAggregate fp tid mag = do
  fd <- P.openFd fp P.WriteOnly (Just $ P.unionFileModes P.ownerReadMode P.ownerWriteMode) P.defaultFileFlags

  -- Just in case a write previously failed.
  P.setFdSize fd 0

  P.setFdOption fd P.SynchronousWrites True
  let 
    stid = encode_ tid
    tidl = succ (round (logBase 10 (fromIntegral tid)))
    commit = BSB.lazyByteString stid <> BSB.lazyByteString (BSLC.replicate (12 - tidl) ' ')
    bsb = commit <> "\n" <> BSB.lazyByteString (encode_ mag)
    (fptr, off, len) = BS.toForeignPtr $ BSLC.toStrict $ BSB.toLazyByteString bsb

  withForeignPtr fptr $ \wptr -> do
    P.fdWriteBuf fd (plusPtr wptr off) (fromIntegral len)

  P.closeFd fd

{-# INLINE readAggregate #-}
readAggregate :: FromJSON ag => FilePath -> IO (Maybe (Aggregate ag))
readAggregate fp = do
  exists <- doesFileExist fp
  if exists then do
    cnts <- BSLC.readFile fp
    case BSLC.lines cnts of
      (ln:rest) -> pure $ Just (Aggregate (Prelude.read (BSLC.unpack ln)) (decode_ (head rest)))
      _ -> pure Nothing
  else 
    pure Nothing

{-# INLINE commitTransaction #-}
commitTransaction :: FilePath -> TransactionId -> IO ()
commitTransaction fp tid = do
  fd <- P.openFd fp P.WriteOnly (Just $ P.unionFileModes P.ownerReadMode P.ownerWriteMode) P.defaultFileFlags
  P.setFdOption fd P.SynchronousWrites True
  let (fptr, off, len) = BS.toForeignPtr $ BSLC.toStrict $ BSB.toLazyByteString (BSB.intDec tid)
  withForeignPtr fptr $ \wptr -> 
    P.fdWriteBuf fd (plusPtr wptr off) (fromIntegral len)
  P.closeFd fd

 
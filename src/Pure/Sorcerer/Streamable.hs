module Pure.Sorcerer.Streamable where

import Pure.Sorcerer.Aggregable
import Pure.Sorcerer.JSON

import qualified Data.ByteString.Lazy.Char8 as BSLC
import qualified Data.ByteString.Lazy as BSL
import Data.Hashable
import System.Directory

import Control.Monad.IO.Class
import Data.List as List
import Data.Maybe
import Data.Typeable

class Streamable ev where
  data Stream ev

  stream :: Stream ev -> FilePath
  default stream :: (Typeable ev,Hashable (Stream ev)) => Stream ev -> FilePath
  stream sev = show (abs (hash (typeOf (undefined :: ev)))) <> "/" <> show (abs (hash sev)) <> ".stream"

-- lazily read events from a FD (to avoid GHCs single-writer OR multiple reader
-- constraint). We know that all events (new lines) are immutable after commit,
-- so it is safe to read those events, and we can stream them on demand.
{-# INLINE events #-}
events :: forall ev m. (MonadIO m, Streamable ev, FromJSON ev) => Stream ev -> m [ev]
events s = liftIO do
  let fp = stream s
  fe <- doesFileExist fp
  if fe then do
    cnts <- BSLC.readFile fp
    pure $ fmap snd $ catMaybes $ fmap (decode_ @(Int,ev)) $ List.drop 1 $ BSLC.lines cnts
  else
    pure []

data Event 
  = forall ev. (Typeable ev, ToJSON ev) => Write
    { event :: ev
    }
  | forall ag. (Typeable ag) => Read
    { callback :: Maybe ag -> IO ()
    }
  | forall ev ag. (Typeable ev, Typeable ag, ToJSON ev) => Transact
    { event   :: ev
    , inspect :: Maybe ag -> Maybe (Maybe ag) -> IO ()
    }


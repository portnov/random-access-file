module Common where

import Control.Monad
import Control.Concurrent
import Control.Concurrent.STM
import Control.Exception
import qualified Data.Map as M
import qualified Data.ByteString as B
import System.IO

type Offset = Int
type Size = Int

class FileAccess a where
  mkFile :: FilePath -> IO a
  readData :: a -> Offset -> Size -> IO B.ByteString
  writeData :: a -> Offset -> B.ByteString -> IO ()
  close :: a -> IO ()

writeZeros :: FileAccess a => a -> Size -> IO ()
writeZeros h size =
  writeData h 0 $ B.replicate size 0


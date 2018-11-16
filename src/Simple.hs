{-# LANGUAGE TypeFamilies #-}
module Simple where

import Control.Monad
import Control.Concurrent
import Control.Concurrent.STM
import Control.Exception
import qualified Data.Map as M
import qualified Data.ByteString as B
import System.IO

import Common

data Simple = Simple Handle
  deriving (Eq, Show)

instance FileAccess Simple where
  data AccessParams Simple = SimpleParams

  mkFile _ path = do
    handle <- openFile path ReadWriteMode
    return $ Simple handle

  readData (Simple handle) offset size = do
    hSeek handle AbsoluteSeek (fromIntegral offset)
    bstr <- B.hGet handle size
    return bstr

  writeData (Simple handle) offset bstr = do
    hSeek handle AbsoluteSeek (fromIntegral offset)
    B.hPut handle bstr

  close (Simple handle) = hClose handle


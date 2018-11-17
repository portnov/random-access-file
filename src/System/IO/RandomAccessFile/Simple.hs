{-# LANGUAGE TypeFamilies #-}
module System.IO.RandomAccessFile.Simple where

import qualified Data.ByteString as B
import System.IO

import System.IO.RandomAccessFile.Common

data Simple = Simple Handle
  deriving (Eq, Show)

instance FileAccess Simple where
  data AccessParams Simple = SimpleParams

  initFile _ path = do
    handle <- openFile path ReadWriteMode
    return $ Simple handle

  readData (Simple handle) offset size = do
    hSeek handle AbsoluteSeek (fromIntegral offset)
    bstr <- B.hGet handle size
    return bstr

  writeData (Simple handle) offset bstr = do
    hSeek handle AbsoluteSeek (fromIntegral offset)
    B.hPut handle bstr

  closeFile (Simple handle) = hClose handle

{-# LANGUAGE TypeFamilies #-}
module System.IO.RandomAccessFile.Simple where

import Control.Concurrent
import Control.Exception
import qualified Data.ByteString as B
import System.IO

import System.IO.RandomAccessFile.Common

data Simple = Simple Handle QSem

instance FileAccess Simple where
  data AccessParams Simple = SimpleParams

  initFile _ path = do
    handle <- openFile path ReadWriteMode
    sem <- newQSem 1
    return $ Simple handle sem

  readBytes (Simple handle sem) offset size = do
    bracket_ (waitQSem sem) (signalQSem sem) $ do
        hSeek handle AbsoluteSeek (fromIntegral offset)
        bstr <- B.hGet handle $ fromIntegral size
        return bstr

  writeBytes (Simple handle sem) offset bstr = do
    bracket_ (waitQSem sem) (signalQSem sem) $ do
        hSeek handle AbsoluteSeek (fromIntegral offset)
        B.hPut handle bstr

  currentFileSize (Simple handle sem) =
    fromIntegral `fmap` hFileSize handle

  closeFile (Simple handle sem) =
    bracket_ (waitQSem sem) (signalQSem sem) $ do
        hClose handle


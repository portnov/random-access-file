{-# LANGUAGE PackageImports #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}

module System.IO.RandomAccessFile.Threaded where

import Control.Concurrent.STM
import Control.Exception
import qualified Data.Map as M
import qualified Data.ByteString as B
import System.Posix.Types
import System.Posix.IO
import "unix-bytestring" System.Posix.IO.ByteString
import Text.Printf

import System.IO.RandomAccessFile.Common

data Threaded = Threaded Fd Size (TVar FileLocks)

instance FileAccess Threaded where
  data AccessParams Threaded = ThreadedParams Size

  initFile (ThreadedParams lockPageSize) path = do
    locks <- atomically $ newTVar M.empty
    let fileMode = Just 0o644
    let flags = defaultFileFlags
    fd <- openFd path ReadWrite fileMode flags
    return $ Threaded fd lockPageSize locks

  readData (Threaded fd lockPageSize locks) offset size = do
    let dataOffset0 = offset `mod` lockPageSize
        pageOffset0 = offset - dataOffset0
        dataOffset1 = (offset + size) `mod` lockPageSize
        pageOffset1 = (offset + size) - dataOffset1
        pageOffsets = [pageOffset0, pageOffset0 + lockPageSize .. pageOffset1]
    underBlockLocks locks ReadAccess pageOffsets $
      fdPread fd (fromIntegral size) (fromIntegral offset)

  writeData (Threaded fd lockPageSize locks) offset bstr = do
    let size = fromIntegral $ B.length bstr
        dataOffset0 = offset `mod` lockPageSize
        pageOffset0 = offset - dataOffset0
        dataOffset1 = (offset + size) `mod` lockPageSize
        pageOffset1 = (offset + size) - dataOffset1
        pageOffsets = [pageOffset0, pageOffset0 + lockPageSize .. pageOffset1]
    underBlockLocks locks WriteAccess pageOffsets $
        (fdPwrite fd bstr (fromIntegral offset) >> return ())
          `catch` (\(e :: SomeException) ->
                     printf "pwrite: offset %d, len %d: %s\n"
                            offset (B.length bstr) (show e))
  
  closeFile (Threaded fd _ _) = closeFd fd

dfltThreaded :: AccessParams Threaded
dfltThreaded = ThreadedParams 4096


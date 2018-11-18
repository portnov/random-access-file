{-# LANGUAGE PackageImports #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}

module System.IO.RandomAccessFile.Threaded where

import Control.Monad
import Control.Concurrent.STM
import Control.Exception
import qualified Data.Map as M
import qualified Data.ByteString as B
import System.Posix.Types
import System.Posix.IO
import System.IO
import "unix-bytestring" System.Posix.IO.ByteString
import Text.Printf

import System.IO.RandomAccessFile.Common

data Threaded = Threaded {
    tFile :: Fd
  , tFileSize :: TVar Size
  , tLockSize :: Size
  , tLocks :: TVar FileLocks
  }

instance FileAccess Threaded where
  data AccessParams Threaded = ThreadedParams Size

  initFile (ThreadedParams lockPageSize) path = do
    locks <- atomically $ newTVar M.empty
    let fileMode = Just 0o644
    let flags = defaultFileFlags
    handle <- openFile path ReadWriteMode
    size <- hFileSize handle
    sizeVar <- newTVarIO (fromIntegral size)
    fd <- handleToFd handle
    -- fd <- openFd path ReadWrite fileMode flags
    return $ Threaded fd sizeVar lockPageSize locks

  readBytes (Threaded fd fileSizeVar lockPageSize locks) offset size = do
    let dataOffset0 = offset `mod` lockPageSize
        pageOffset0 = offset - dataOffset0
        dataOffset1 = (offset + size) `mod` lockPageSize
        pageOffset1 = (offset + size) - dataOffset1
        pageOffsets = [pageOffset0, pageOffset0 + lockPageSize .. pageOffset1]
    fsize <- atomically $ readTVar fileSizeVar
    if offset > fsize || (offset + size) > fsize
      then fail $ printf "readBytes: read after EOF: size %d, offset %d, size %d" fsize offset size
      else do
        underBlockLocks locks ReadAccess pageOffsets $
          fdPread fd (fromIntegral size) (fromIntegral offset)
              `catch` (\(e :: SomeException) -> do
                         printf "pread: offset %d, len %d: %s\n"
                                offset size (show e)
                         throw e)

  writeBytes (Threaded fd fileSizeVar lockPageSize locks) offset bstr = do
      let size = fromIntegral $ B.length bstr
          dataOffset0 = offset `mod` lockPageSize
          pageOffset0 = offset - dataOffset0
          dataOffset1 = (offset + size) `mod` lockPageSize
          pageOffset1 = (offset + size) - dataOffset1
          pageOffsets = [pageOffset0, pageOffset0 + lockPageSize .. pageOffset1]

          pwrite bytes off =
            (fdPwrite fd bytes (fromIntegral off) >> return ())
              `catch` (\(e :: SomeException) ->
                         printf "pwrite: offset %d, len %d: %s\n"
                                offset (B.length bstr) (show e))
      fsize <- atomically $ readTVar fileSizeVar
      underBlockLocks locks WriteAccess pageOffsets $ do
          let delta = max 0 $ (offset + size) - fsize
          when (delta > 0) $
              pwrite (B.replicate (fromIntegral delta) 0) fsize
          pwrite bstr offset
          atomically $ writeTVar fileSizeVar (fsize + delta)

  currentFileSize h = do
    let var = tFileSize h
    atomically $ readTVar var
  
  closeFile (Threaded fd _ _ _) = closeFd fd

dfltThreaded :: AccessParams Threaded
dfltThreaded = ThreadedParams 4096


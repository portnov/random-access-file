{-# LANGUAGE PackageImports #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}

module System.IO.RandomAccessFile.MMap where

import Control.Concurrent.STM
import qualified Data.Map as M
import qualified Data.ByteString as B
import Data.ByteString.Unsafe
import System.IO
import System.Posix.Types
import System.Posix.IO
import Foreign.Ptr
import Foreign.C.Types
import Foreign.Marshal
import System.Posix.Memory

import System.IO.RandomAccessFile.Common

data MMaped = MMaped Fd (Ptr CChar) CSize Size (TVar FileLocks)

instance FileAccess MMaped where
  data AccessParams MMaped = MMapedParams Size

  initFile (MMapedParams lockPageSize) path = do
    locks <- atomically $ newTVar M.empty
    let fileMode = Just 0o644
    let flags = defaultFileFlags
    handle <- openFile path ReadWriteMode
    size <- hFileSize handle
    fd <- handleToFd handle
--     fd <- openFd path ReadWrite fileMode flags
--     handle <- fdToHandle fd
    ptr <- memoryMap Nothing (fromIntegral size) [MemoryProtectionRead, MemoryProtectionWrite] MemoryMapShared (Just fd) 0
    return $ MMaped fd ptr (fromIntegral size) lockPageSize locks

  readBytes (MMaped _ ptr _ lockPageSize locks) offset size = do
    let dataOffset0 = offset `mod` lockPageSize
        pageOffset0 = offset - dataOffset0
        dataOffset1 = (offset + size) `mod` lockPageSize
        pageOffset1 = (offset + size) - dataOffset1
        pageOffsets = [pageOffset0, pageOffset0 + lockPageSize .. pageOffset1]
    underBlockLocks locks ReadAccess pageOffsets $ do
      let bstrPtr = plusPtr ptr (fromIntegral offset)
      unsafePackCStringLen (bstrPtr, fromIntegral size)

  writeBytes (MMaped _ ptr _ lockPageSize locks) offset bstr = do
    let size = fromIntegral $ B.length bstr
        dataOffset0 = offset `mod` lockPageSize
        pageOffset0 = offset - dataOffset0
        dataOffset1 = (offset + size) `mod` lockPageSize
        pageOffset1 = (offset + size) - dataOffset1
        pageOffsets = [pageOffset0, pageOffset0 + lockPageSize .. pageOffset1]
    underBlockLocks locks WriteAccess pageOffsets $
      unsafeUseAsCStringLen bstr $ \(bstrPtr,len) ->
        copyBytes (plusPtr ptr (fromIntegral offset)) bstrPtr len

  currentFileSize _ = undefined

  syncFile (MMaped _ ptr size _ _) =
    memorySync ptr size [MemorySyncSync, MemorySyncInvalidate]
  
  closeFile (MMaped fd ptr size _ _) = do
    memoryUnmap ptr size


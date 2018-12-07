{-# LANGUAGE PackageImports #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}

module System.IO.RandomAccessFile.MMap
  (MMaped,
   AccessParams (..),
   extendFile
  ) where

import Control.Monad
import Control.Concurrent.STM
import qualified Control.Concurrent.ReadWriteLock as RWL
import qualified Data.Map as M
import qualified Data.ByteString as B
import Data.ByteString.Unsafe
import Data.Int
import System.IO
import System.Posix.Types
import System.Posix.IO
import Foreign.Ptr
import Foreign.C.Types
import Foreign.Marshal
import System.Posix.Memory
import Text.Printf

import System.IO.RandomAccessFile.Common

data MMaped = MMaped {
    mmFile :: TVar Fd
  , mmPath :: FilePath
  , mmData :: TVar (Ptr CChar)
  , mmExtendable :: Bool
  , mmFileSize :: TVar CSize
  , mmLockPageSize :: Size
  , mmLocks :: TVar FileLocks
  , mmResizeLock :: RWL.RWLock
  }

mmap :: CSize -> Fd -> IO (Ptr CChar)
mmap size fd = 
    memoryMap Nothing size [MemoryProtectionRead, MemoryProtectionWrite] MemoryMapShared (Just fd) 0

-- | Resize file to be at least of specified size.
-- Does nothing if size of file is already greater or equal
-- to specified.
-- While file is resized, all reading and writing to it are locked.
extendFile :: MMaped -> Size -> IO ()
extendFile handle newSize = when (mmExtendable handle) $ do
  let sizeVar = mmFileSize handle
      ptrVar = mmData handle
      page = mmLockPageSize handle
  ptr <- atomically $ readTVar ptrVar
  oldSize <- atomically $ readTVar sizeVar
  let delta :: Int64
      delta = fromIntegral newSize - fromIntegral oldSize

  when (delta > 0) $
    -- We are acquiring "general" lock here to
    -- make sure that there will be no newly created
    -- page-level locks
    withLock (mmResizeLock handle) WriteAccess $ do
      -- Acquire all existing page-level locks in Write mode
      -- so that noone may read or write the file: we are going to
      -- unmap it
      locks <- atomically $ readTVar (mmLocks handle)
      withLocks (M.elems locks) WriteAccess $ do

        -- Unmap existing mapping
        memorySync ptr oldSize [MemorySyncSync, MemorySyncInvalidate]
        memoryUnmap ptr oldSize

        -- Open file again
        h <- openFile (mmPath handle) ReadWriteMode
        -- Write zeros to the end
        size <- hFileSize h
        hSeek h AbsoluteSeek size
        B.hPut h $ B.replicate (fromIntegral page) 0
        hFlush h
        fd <- handleToFd h
        -- MMap file again
        let newSize' = oldSize + fromIntegral page
        ptr' <- mmap (fromIntegral newSize') fd

        atomically $ do
          writeTVar (mmFile handle) fd
          writeTVar ptrVar ptr'
          writeTVar sizeVar (fromIntegral newSize')

instance FileAccess MMaped where
  data AccessParams MMaped = MMapedParams Size Bool

  initFile (MMapedParams lockPageSize extendable) path = do
    locks <- atomically $ newTVar M.empty
    let fileMode = Just 0o644
    let flags = defaultFileFlags
    handle <- openFile path ReadWriteMode
    size <- do
      sz <- hFileSize handle
      if sz == 0
        then do
          B.hPut handle $ B.replicate (fromIntegral lockPageSize) 0
          hFlush handle
          return lockPageSize
        else return $ fromIntegral sz
--     printf "Init size: %d\n" size
    sizeVar <- newTVarIO (fromIntegral size)
    fd <- handleToFd handle
--     fd <- openFd path ReadWrite fileMode flags
--     handle <- fdToHandle fd
    fdVar <- newTVarIO fd
    ptr <- mmap (fromIntegral size) fd
    ptrVar <- newTVarIO ptr
    resizeLock <- RWL.new
    return $ MMaped fdVar path ptrVar extendable sizeVar lockPageSize locks resizeLock

  readBytes handle offset size = do
    let ptrVar = mmData handle
        lockPageSize = mmLockPageSize handle
        locks = mmLocks handle
    ptr <- atomically $ readTVar ptrVar
    let dataOffset0 = offset `mod` lockPageSize
        pageOffset0 = offset - dataOffset0
        dataOffset1 = (offset + size) `mod` lockPageSize
        pageOffset1 = (offset + size) - dataOffset1
        pageOffsets = [pageOffset0, pageOffset0 + lockPageSize .. pageOffset1]

    withLock_ (mmExtendable handle) (mmResizeLock handle) ReadAccess $ -- just check that file is not currently being resized
      underBlockLocks locks ReadAccess pageOffsets $ do
        let bstrPtr = plusPtr ptr (fromIntegral offset)
        unsafePackCStringLen (bstrPtr, fromIntegral size)

  writeBytes handle offset bstr = do
    let ptrVar = mmData handle
        sizeVar = mmFileSize handle
        lockPageSize = mmLockPageSize handle
        locks = mmLocks handle
    fsize <- atomically $ readTVar sizeVar
    let size = fromIntegral $ B.length bstr
        dataOffset0 = offset `mod` lockPageSize
        pageOffset0 = offset - dataOffset0
        dataOffset1 = (offset + size) `mod` lockPageSize
        pageOffset1 = (offset + size) - dataOffset1
        pageOffsets = [pageOffset0, pageOffset0 + lockPageSize .. pageOffset1]
    -- printf "write: offset %d, size %d, fsize %s\n" offset size (show fsize)
    if (offset + size) > fromIntegral fsize
      then do
           extendFile handle (offset+size)
           writeBytes handle offset bstr
      else
        -- just check that file is not currently being resized
        withLock_ (mmExtendable handle) (mmResizeLock handle) ReadAccess $
          underBlockLocks locks WriteAccess pageOffsets $ do
            ptr <- atomically $ readTVar ptrVar
            unsafeUseAsCStringLen bstr $ \(bstrPtr,len) ->
              copyBytes (plusPtr ptr (fromIntegral offset)) bstrPtr len

  currentFileSize handle = do
    sz <- atomically $ readTVar $ mmFileSize handle
    return $ fromIntegral sz

  syncFile handle = do
    (ptr, size) <- atomically $ do
                     p <- readTVar (mmData handle)
                     s <- readTVar (mmFileSize handle)
                     return (p,s)
    memorySync ptr size [MemorySyncSync, MemorySyncInvalidate]
  
  closeFile handle = do
    (ptr, size) <- atomically $ do
                     p <- readTVar (mmData handle)
                     s <- readTVar (mmFileSize handle)
                     return (p,s)
    memoryUnmap ptr size


{-# LANGUAGE PackageImports #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Cached where

import Control.Monad
import Control.Concurrent
import Control.Concurrent.STM
import Control.Exception
import qualified Control.Concurrent.ReadWriteLock as RWL
import qualified Data.Map as M
import qualified Data.ByteString as B
import System.IO
import System.Random
import System.Posix.Types
import System.Posix.IO
import "unix-bytestring" System.Posix.IO.ByteString
import Text.Printf

import Common

data Page = Page {pDirty :: Bool, pData :: B.ByteString, pLock :: RWL.RWLock}
  deriving (Eq)

instance Show Page where
  show p = printf "[Page: %s]" (show $ pDirty p)

-- data WriteQueueItem =
--     CloseFile
--   | QueueItem {
--       qiOffset :: Offset
--     , qiData :: B.ByteString
--     }

data AccessType = ReadAccess | WriteAccess

data CacheData = CacheData {
    cdMap :: M.Map Offset Page
  }

type Cache = TVar CacheData

type FileLocks = M.Map Offset RWL.RWLock

data Cached = Cached Fd (TVar FileLocks) Cache

cachePageSize :: Size
cachePageSize = 4096

withLock :: RWL.RWLock -> AccessType -> IO a -> IO a
withLock lock ReadAccess action =
  bracket_
    (RWL.acquireRead lock)
    (RWL.releaseRead lock)
    action
withLock lock WriteAccess action =
  bracket_
    (RWL.acquireWrite lock)
    (RWL.releaseWrite lock)
    action

underBlockLock :: TVar FileLocks -> AccessType -> Offset -> IO a -> IO a
underBlockLock locksVar access n action = do
  newLock <- RWL.new
  lock <- atomically $ do
            locks <- readTVar locksVar
            case M.lookup n locks of
              Just lock -> return lock
              Nothing -> do
                writeTVar locksVar $ M.insert n newLock locks
                return newLock
  withLock lock access action

instance FileAccess Cached where
  mkFile path = do
      locks <- atomically $ newTVar M.empty
      var <- atomically $ newTVar $ CacheData M.empty
      let fileMode = Just 0o644
      let flags = defaultFileFlags
      fd <- openFd path ReadWrite fileMode flags
      forkIO $ dumpQueue fd locks var
      return $ Cached fd locks var
    where
      dumpQueue :: Fd -> TVar FileLocks -> Cache -> IO ()
      dumpQueue fd locks var = forever $ do
        threadDelay $ 10 * 1000
        dump <- atomically $ do
                  cache <- readTVar var
                  let pages = filter (pDirty . snd) $ M.assocs $ cdMap cache
                      cache' = M.map (\p -> p {pDirty = False}) $ cdMap cache
                  writeTVar var $ CacheData cache'
                  return $ forM_ pages $ \(offset, page) -> do
                    underBlockLock locks WriteAccess offset $ do
                      -- printf "Writing: %d, %d\n" offset (B.length $ pData page)
                      (fdPwrite fd (pData page) (fromIntegral offset) >> return ())
                        `catch` (\(e :: SomeException) ->
                                   printf "pwrite: offset %d, len %d: %s\n"
                                          offset (B.length $ pData page) (show e))

        dump
                      
  readData handle offset size = do
    let dataOffset0 = offset `mod` cachePageSize
        pageOffset0 = offset - dataOffset0
        dataOffset1 = (offset + size) `mod` cachePageSize
        pageOffset1 = (offset + size) - dataOffset1
        pageOffsets = [pageOffset0, pageOffset0 + cachePageSize .. pageOffset1]
        inputs = flip map pageOffsets $ \page ->
                  if page == pageOffset0
                    then (page, dataOffset0, min size (cachePageSize - dataOffset0))
                    else if page == pageOffset1
                           then (page, 0, dataOffset1)
                           else (page, 0, cachePageSize)
    -- printf "PO: %s\n" (show pageOffsets)
    -- printf "I: %s\n" (show inputs)
    fragments <- forM inputs $ \(pageOffset, dataOffset, sz) ->
                   readDataAligned handle pageOffset dataOffset sz
    return $ B.concat fragments
  
  writeData handle offset bstr = do
    let size = B.length bstr
        dataOffset0 = offset `mod` cachePageSize
        pageOffset0 = offset - dataOffset0
        dataOffset1 = (offset + size) `mod` cachePageSize
        pageOffset1 = (offset + size) - dataOffset1
        pageOffsets = [pageOffset0, pageOffset0 + cachePageSize .. pageOffset1]
        inputs = flip map pageOffsets $ \page ->
                  if page == pageOffset0
                    then (page, dataOffset0, min size (cachePageSize - dataOffset0))
                    else if page == pageOffset1
                           then (page, 0, dataOffset1)
                           else (page, 0, cachePageSize)
        fragments = flip map inputs $ \(pageOffset, dataOffset, sz) ->
                      let strOffset = pageOffset + dataOffset - offset
                          fragment = B.take sz $ B.drop strOffset bstr
                      in  (pageOffset, dataOffset, fragment)
    -- printf "PO: %s\n" (show pageOffsets)
    -- printf "I: %s\n" (show inputs)
    -- printf "F: %s\n" (show fragments)
    forM_ fragments $ \(pageOffset, dataOffset, fragment) ->
        writeDataAligned handle pageOffset dataOffset fragment

  close (Cached fd locks var) = do
    closeFd fd

readDataAligned (Cached fd locks var) pageOffset dataOffset size = do
  mbCached <- atomically $ do
    cache <- readTVar var
    return $ M.lookup pageOffset $ cdMap cache
  case mbCached of
    Nothing -> do
      page <- underBlockLock locks ReadAccess pageOffset $
                fdPread fd (fromIntegral cachePageSize) (fromIntegral pageOffset)
      let result = B.take size $ B.drop dataOffset page
      lock <- RWL.new
      atomically $ modifyTVar var $ \cache ->
        cache {cdMap = M.insert pageOffset (Page False page lock) (cdMap cache)}
      return result
    Just page -> do
      withLock (pLock page) ReadAccess $ do
        let result = B.take size $ B.drop dataOffset $ pData page
        return result

writeDataAligned (Cached fd locks var) pageOffset dataOffset bstr = do
  -- printf "WA: page %d, data %d, len %d\n" pageOffset dataOffset (B.length bstr)
  mbCached <- atomically $ do
    cache <- readTVar var
    return $ M.lookup pageOffset $ cdMap cache
  case mbCached of
    Nothing -> do
      page <- underBlockLock locks ReadAccess pageOffset $
                fdPread fd (fromIntegral cachePageSize) (fromIntegral pageOffset)
      let page' = B.take dataOffset page `B.append` bstr `B.append` B.drop (dataOffset + B.length bstr) page
      when (B.length page /= B.length page') $
        fail $ printf "W/N: %d /= %d! data: %d, page: %d, len: %d" (B.length page) (B.length page') pageOffset dataOffset (B.length bstr)
      lock <- RWL.new
      atomically $ modifyTVar var $ \cache ->
        cache {cdMap = M.insert pageOffset (Page True page' lock) (cdMap cache)}

    Just page -> do
      withLock (pLock page) WriteAccess $ do
        let pageData = pData page
        let pageData' = B.take dataOffset pageData `B.append` bstr `B.append` B.drop (dataOffset + B.length bstr) pageData
        let page' = page {pDirty = True, pData = pageData'}
        when (B.length pageData /= B.length pageData') $
          fail $ printf "W/J: %d /= %d!" (B.length pageData) (B.length pageData')
        atomically $ modifyTVar var $ \cache ->
          cache {cdMap = M.insert pageOffset page' (cdMap cache)}


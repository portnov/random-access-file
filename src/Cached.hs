{-# LANGUAGE PackageImports #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}

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

data CacheData = CacheData {
    cdMap :: M.Map Offset Page
  }

type Cache = TVar CacheData

data Cached a = Cached a QSem Cache

cachePageSize :: Size
cachePageSize = 4096

instance FileAccess a => FileAccess (Cached a) where
  data AccessParams (Cached a) = CachedBackend (AccessParams a)

  mkFile (CachedBackend params) path = do
      a <- mkFile params path
      var <- atomically $ newTVar $ CacheData M.empty
      let fileMode = Just 0o644
      let flags = defaultFileFlags
      closeLock <- newQSem 1
      forkIO $ dumpQueue a closeLock var
      return $ Cached a closeLock var
    where
      dumpQueue a closeLock var = forever $ do
        threadDelay $ 10 * 1000
        pages <- atomically $ do
                  cache <- readTVar var
                  let pages = filter (pDirty . snd) $ M.assocs $ cdMap cache
                      cache' = M.map (\p -> p {pDirty = False}) $ cdMap cache
                  writeTVar var $ CacheData cache'
                  return pages
        if null pages
          then signalQSem closeLock
          else do 
            forM_ pages $ \(offset, page) -> do
                writeData a offset (pData page)
            -- syncFile a
                      
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

  syncFile (Cached a _ _) = do
    syncFile a

  close (Cached a closeLock var) = do
    waitQSem closeLock
    close a

readDataAligned (Cached a _ var) pageOffset dataOffset size = do
  mbCached <- atomically $ do
    cache <- readTVar var
    return $ M.lookup pageOffset $ cdMap cache
  case mbCached of
    Nothing -> do
      page <- readData a pageOffset cachePageSize
      let result = B.take size $ B.drop dataOffset page
      lock <- RWL.new
      atomically $ modifyTVar var $ \cache ->
        cache {cdMap = M.insert pageOffset (Page False page lock) (cdMap cache)}
      return result
    Just page -> do
      withLock (pLock page) ReadAccess $ do
        let result = B.take size $ B.drop dataOffset $ pData page
        return result

writeDataAligned (Cached a _ var) pageOffset dataOffset bstr = do
  -- printf "WA: page %d, data %d, len %d\n" pageOffset dataOffset (B.length bstr)
  mbCached <- atomically $ do
    cache <- readTVar var
    return $ M.lookup pageOffset $ cdMap cache
  case mbCached of
    Nothing -> do
      page <- readData a pageOffset cachePageSize
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


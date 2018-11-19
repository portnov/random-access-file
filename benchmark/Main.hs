{-# LANGUAGE OverloadedStrings #-}

import Control.Monad
import Control.Concurrent
import qualified Data.ByteString as B
import System.Random
import System.IO
import Criterion.Main

import System.IO.RandomAccessFile

createFile :: FilePath -> IO ()
createFile path = do
  h <- openFile path ReadWriteMode
  B.hPut h $ B.replicate (100*1024*1024) 0
  hClose h

concurrently :: Int -> IO () -> IO ()
concurrently n action = do
  vars <- replicateM n newEmptyMVar
  forM_ vars $ \var -> forkIO $ do
    action
    putMVar var ()
  forM_ vars takeMVar

execute :: FileAccess a => AccessParams a -> FilePath -> Bool -> IO ()
execute params path doClose = do

  h <- initFile params path
  -- writeZeros h (1024*1024)

  concurrently 20 $ do
      replicateM_ 100 $ do
        offset <- randomRIO (100, 100*900*1024)
        writeBytes h offset "abdefgh0123456789ABCDEFGIJK"
        return ()

      replicateM_ 100 $ do
        offset <- randomRIO (100, 100*900*1024)
        readBytes h offset 512
        return ()

  when doClose $
    closeFile h

pageSize = 1024

main :: IO ()
main = defaultMain [
  env (createFile "test.data") $ \_ -> bgroup "main" [
      bench "simple" $ whnfIO $ execute SimpleParams "test.data" True
    , bench "threaded" $ whnfIO $ execute (ThreadedParams pageSize) "test.data" True
    , bench "mmaped" $ whnfIO $ execute (MMapedParams pageSize False) "test.data" True
    , bench "cached/threaded" $ whnfIO $ execute ((dfltCached (ThreadedParams pageSize)) {cachePageSize = pageSize}) "test.data" False
    , bench "cached/mmaped" $ whnfIO $ execute ((dfltCached (MMapedParams pageSize False)) {cachePageSize = pageSize}) "test.data" False
    ]
  ]


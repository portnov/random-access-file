{-# LANGUAGE OverloadedStrings #-}

import Control.Monad
import Control.Concurrent
import qualified Data.ByteString as B
import qualified Data.Vector as V
import Data.Vector ((!))
import System.Random
import System.Random.MWC
import System.IO
import Criterion.Main

import System.IO.RandomAccessFile

mkRandomOffsets :: Int -> (Offset, Offset) -> IO (V.Vector Offset)
mkRandomOffsets n (from, to) =
  withSystemRandom . asGenIO $ \gen -> V.fromList `fmap` replicateM n (uniformR (from, to) gen)

createFile :: FilePath -> IO ()
createFile path = do
  h <- openFile path ReadWriteMode
  B.hPut h $ B.replicate (100*1024*1024) 0
  hClose h

concurrently :: Int -> (Int -> IO ()) -> IO ()
concurrently n action = do
  vars <- replicateM n newEmptyMVar
  forM_ (zip [0..] vars) $ \(i, var) -> forkIO $ do
    action i
    putMVar var ()
  forM_ vars takeMVar

execute :: FileAccess a => AccessParams a -> FilePath -> Bool -> IO ()
execute params path doClose = do

  h <- initFile params path
  -- writeZeros h (1024*1024)
  offsets <- mkRandomOffsets (40*(500+1000)) (100, 100*900*1024)

  concurrently 40 $ \threadId -> do
      forM_ [0..499] $ \i -> do
        let offset = offsets ! (threadId*(500+1000) + i)
        writeBytes h offset "abdefgh0123456789ABCDEFGIJK"
        return ()

      forM_ [0.. 999] $ \i ->  do
        let offset = offsets ! (threadId*(500+1000) + 500 + i)
        readBytes h offset 512
        return ()

  when doClose $
    closeFile h

benchList name list benchmarks = bgroup name
  [bgroup (name ++ ":" ++ show param) [fn param | fn <- benchmarks] 
   | param <- list]

main :: IO ()
main = defaultMain [
  env (createFile "test.data") $ \_ -> bgroup "main" [
        bench "simple" $ whnfIO $ execute SimpleParams "test.data" True,
        benchList "raw" [1024, 4*1024, 16*1024] [
          \pageSize -> bench "threaded" $ whnfIO $ execute (ThreadedParams pageSize) "test.data" True,
          \pageSize -> bench "mmaped" $ whnfIO $ execute (MMapedParams pageSize False) "test.data" True
        ],
        benchList "cached" [1024, 4*1024, 16*1024] [
          \pageSize -> bench "threaded" $ whnfIO $ execute ((dfltCached (ThreadedParams pageSize)) {cachePageSize = pageSize}) "test.data" True,
          \pageSize -> bench "mmaped" $ whnfIO $ execute ((dfltCached (MMapedParams pageSize False)) {cachePageSize = pageSize}) "test.data" True
        ]
    ]
  ]


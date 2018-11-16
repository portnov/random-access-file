{-# LANGUAGE OverloadedStrings #-}

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
import Criterion.Main

import Common
import Simple
import Cached

executeSimple :: IO ()
executeSimple = do
  h <- mkFile "test.data" :: IO Simple
--   writeZeros h (1024*1024)

  replicateM 50 $ do
    offset <- randomRIO (100, 900*1024)
    writeData h offset "abdefgh0123456789"
    return ()

  replicateM 50 $ do
    offset <- randomRIO (100, 900*1024)
    readData h offset 512
    return ()

  close h

executeCached :: IO ()
executeCached = do
  h <- mkFile "test.data" :: IO Cached
  -- writeZeros h (1024*1024)

  replicateM_ 50 $ do
    offset <- randomRIO (100, 900*1024)
    writeData h offset "abdefgh0123456789"
    return ()

  replicateM_ 50 $ do
    offset <- randomRIO (100, 900*1024)
    readData h offset 512
    return ()

--   close h


main :: IO ()
main = defaultMain [
    bench "simple" $ whnfIO executeSimple
  , bench "cached" $ whnfIO executeCached
  ]

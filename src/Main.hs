
import Control.Monad
import Control.Concurrent.STM
import qualified Data.Map as M
import qualified Data.ByteString as B
import System.IO
import System.Random
import Criterion.Main

type Offset = Int
type Size = Int

class FileAccess a where
  mkFile :: FilePath -> IO a
  readData :: a -> Offset -> Size -> IO B.ByteString
  writeData :: a -> Offset -> B.ByteString -> IO ()
  close :: a -> IO ()

writeZeros :: FileAccess a => a -> Size -> IO ()
writeZeros h size =
  writeData h 0 $ B.replicate size 0

data Simple = Simple Handle
  deriving (Eq, Show)

instance FileAccess Simple where
  mkFile path = do
    handle <- openFile path ReadWriteMode
    return $ Simple handle

  readData (Simple handle) offset size = do
    hSeek handle AbsoluteSeek (fromIntegral offset)
    bstr <- B.hGet handle size
    return bstr

  writeData (Simple handle) offset bstr = do
    hSeek handle AbsoluteSeek (fromIntegral offset)
    B.hPut handle bstr

  close (Simple handle) = hClose handle

data Cached = Cached Handle (TVar (M.Map Offset B.ByteString))

cachePageSize :: Size
cachePageSize = 4096

instance FileAccess Cached where
  mkFile path = do
    handle <- openFile path ReadWriteMode
    var <- atomically $ newTVar M.empty
    return $ Cached handle var
  
  readData (Cached handle var) offset size = do
    mbCached <- atomically $ do
      cache <- readTVar var
      return $ M.lookupLE offset cache
    case mbCached of
      Nothing -> do
        let offset' = (offset `div` cachePageSize) * cachePageSize
        hSeek handle AbsoluteSeek (fromIntegral offset')
        page <- B.hGet handle size



executeSimple :: IO ()
executeSimple = do
  h <- mkFile "test.data" :: IO Simple
  writeZeros h (1024*1024)

  replicateM 50 $ do
    offset <- randomRIO (100, 900*1024)
    readData h offset 512
    return ()

  close h


main :: IO ()
main = defaultMain [
    bench "simple" $ whnfIO executeSimple
  ]

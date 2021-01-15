{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
module Main where

import qualified Data.Text as T
import qualified Data.Map as M
import qualified Data.Serialize                as S
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Builder as BB
import qualified ByteString.StrictBuilder as BSB
import qualified Knit.Report as K
import qualified Knit.Utilities.Streamly as K
import qualified Knit.Report.Cache as KC
import qualified Knit.Effect.AtomicCache as KAC
import qualified Knit.Effect.Serialize as KS
import qualified Polysemy as P
import qualified Control.Foldl                 as FL
import qualified BlueRipple.Data.DataFrames    as DS.Loaders
import qualified BlueRipple.Data.ACS_PUMS as PUMS
import qualified BlueRipple.Data.ACS_PUMS_Loader.ACS_PUMS_Frame as PUMS
import           Data.String.Here               ( i, here )
import qualified Data.Word as Word
import qualified Frames as F
import qualified Frames.Streamly.CSV as FStreamly
import qualified Frames.Streamly.InCore as FStreamly
import qualified Frames.Serialize as FS
import qualified BlueRipple.Data.LoadersCore as Loaders
import qualified BlueRipple.Utilities.KnitUtils as BR
import qualified Frames.CSV                     as Frames
import qualified Streamly.Data.Fold            as Streamly.Fold
import qualified Streamly.Internal.Data.Fold.Types            as Streamly.Fold
import qualified Streamly.Prelude              as Streamly
import qualified Streamly.Internal.Prelude              as Streamly
import qualified Streamly              as Streamly
import qualified Streamly.Internal.FileSystem.File
                                               as Streamly.File
import qualified Streamly.External.ByteString  as Streamly.ByteString

import qualified Streamly.Internal.Data.Array  as Streamly.Data.Array
import qualified Streamly.Internal.Memory.Array as Streamly.Memory.Array

yamlAuthor :: T.Text
yamlAuthor = [here|
- name: Adam Conner-Sax
- name: Frank David
|]

templateVars =
  M.fromList [("lang", "English")
             , ("site-title", "Blue Ripple Politics")
             , ("home-url", "https://www.blueripplepolitics.org")
--  , ("author"   , T.unpack yamlAuthor)
             ]


pandocTemplate = K.FullySpecifiedTemplatePath "pandoc-templates/blueripple_basic.html"

main :: IO ()
main= do
--  testsInIO
  pandocWriterConfig <- K.mkPandocWriterConfig pandocTemplate
                                               templateVars
                                               K.mindocOptionsF
  let  knitConfig = (K.defaultKnitConfig Nothing)
        { K.outerLogPrefix = Just "Testbed.Main"
        , K.logIf = K.logDiagnostic
        , K.pandocWriterConfig = pandocWriterConfig
        }
  resE <- K.knitHtml knitConfig makeDoc
  case resE of
    Right htmlAsText ->
      K.writeAndMakePathLT "testbed.html" htmlAsText
    Left err -> putStrLn $ "Pandoc Error: " ++ show err

encodeOne :: S.Serialize a => a -> BB.Builder
encodeOne !x = S.execPut $ S.put x

bldrToCT  = Streamly.ByteString.toArray . BL.toStrict . BB.toLazyByteString

encodeBS :: S.Serialize a => a -> BSB.Builder
encodeBS !x = BSB.bytes $! S.runPut $! S.put x

bsToCT  = Streamly.ByteString.toArray . BSB.builderBytes


data Accum b = Accum { count :: !Int, bldr :: !b }

streamlySerializeF :: forall c bldr m a ct.(Monad m, Monoid bldr, c a, c Word.Word64)
                   => (forall b. c b => b -> bldr)
                   -> (bldr -> ct)
                   -> Streamly.Fold.Fold m a ct
streamlySerializeF encodeOne bldrToCT = Streamly.Fold.Fold step initial extract where
  step (Accum n b) !a = return $ Accum (n + 1) (b <> encodeOne a)
  initial = return $ Accum 0 mempty
  extract (Accum n b) = return $ bldrToCT $ encodeOne (fromIntegral @Int @Word.Word64 n) <> b
{-# INLINEABLE streamlySerializeF #-}

{-
streamlySerializeF2 :: forall c bldr m a ct.(Monad m, Monoid bldr, c a, c Word.Word64)
                   => (forall b. c b => b -> bldr)
                   -> (bldr -> ct)
                   -> Streamly.Fold.Fold m a ct
streamlySerializeF2 encodeOne bldrToCT = Streamly.Fold.Fold step initial extract where
  step (Accum n b) !a = return $ Accum (n + 1) (b <> encodeOne a)
  initial = return $ Accum 0 mempty
  extract (Accum n b) = return $ bldrToCT $ encodeOne (fromIntegral @Int @Word.Word64 n) <> b
{-# INLINEABLE streamlySerializeF #-}
-}

makeDoc :: forall r. (K.KnitOne r,  K.CacheEffectsD r) => K.Sem r ()
makeDoc = do
  let pumsCSV = "../bigData/test/acs100k.csv"
      dataPath = (Loaders.LocalData $ T.pack $ pumsCSV)
  K.logLE K.Info "Testing File.toBytes..."
  let rawBytesS =  Streamly.File.toBytes pumsCSV
  rawBytes <-  K.streamlyToKnit $ Streamly.fold Streamly.Fold.length rawBytesS
  K.logLE K.Info $ "raw PUMS data has " <> (T.pack $ show rawBytes) <> " bytes."
  K.logLE K.Info "Testing readTable..."
  let sPumsRawRows :: Streamly.SerialT K.StreamlyM PUMS.PUMS_Raw2
        = FStreamly.readTableOpt Frames.defaultParser pumsCSV
  iRows <-  K.streamlyToKnit $ Streamly.fold Streamly.Fold.length sPumsRawRows
  K.logLE K.Info $ "raw PUMS data has " <> (T.pack $ show iRows) <> " rows."
  K.logLE K.Info "Testing Frames.Streamly.inCoreAoS:"
  fPums <- K.streamlyToKnit $ FStreamly.inCoreAoS sPumsRawRows
  K.logLE K.Info $ "raw PUMS frame has " <> show (FL.fold FL.length fPums) <> " rows."
  -- Previous goes up to 28MB, looks like via doubling.  Then to 0 (collects fPums after counting?)
  -- This one then climbs to 10MB, rows are smaller.  No large leaks.
  K.logLE K.Info "Testing Frames.Streamly.inCoreAoS with row transform:"
  fPums' <- K.streamlyToKnit $ FStreamly.inCoreAoS $ Streamly.map PUMS.transformPUMSRow' sPumsRawRows
  K.logLE K.Info $ "transformed PUMS frame has " <> show (FL.fold FL.length fPums') <> " rows."
  K.logLE K.Info "loadToRecStream..."
  let sRawRows2 :: Streamly.SerialT K.StreamlyM PUMS.PUMS_Raw2
        = DS.Loaders.loadToRecStream Frames.defaultParser pumsCSV (const True)
  iRawRows2 <-  K.streamlyToKnit $ Streamly.fold Streamly.Fold.length sRawRows2
  K.logLE K.Info $ "PUMS data (via loadToRecStream) has " <> show iRows <> " rows."
  K.logLE K.Info $ "transform and load that stream to frame..."
  let countFold = Loaders.runningCountF "reading..." (\n -> "read " <> show (250000 * n) <> " rows") "finished"
      sPUMSRunningCount = Streamly.map PUMS.transformPUMSRow'
                          $ Streamly.tapOffsetEvery 250000 250000 countFold sRawRows2
  fPums'' <- K.streamlyToKnit $ FStreamly.inCoreAoS sPUMSRunningCount
  K.logLE K.Info $ "frame has " <> show (FL.fold FL.length fPums'') <> " rows."
  K.logLE K.Info $ "toS and fromS transform and load that stream to frame..."
  let sPUMSRCToS = Streamly.map FS.toS sPUMSRunningCount
  K.logLE K.Info $ "retrieveOrMake"
  let testCacheKey = "test/fPumsCached.sbin"
--    sDict  = KS.cerealStreamlyDict
  serializedBytes :: KS.DefaultCacheData  <- K.streamlyToKnit
                                             $ Streamly.fold (streamlySerializeF @S.Serialize encodeBS bsToCT)  sPUMSRCToS
  print $ Streamly.Memory.Array.length serializedBytes
{-
  K.logLE K.Info "test array "
  K.streamlyToKnit $ do
    arr <- Streamly.fold Streamly.Data.Array.write sPUMSRunningCount
    print $ Streamly.Data.Array.length arr
-}
{-
  let --bufferFold = fmap Streamly.Data.Array.toStream Streamly.Data.Array.write
      buffer :: Streamly.SerialT K.StreamlyM a -> K.StreamlyM (Streamly.SerialT K.StreamlyM a)
      buffer = fmap (Streamly.unfold Streamly.Data.Array.read) . Streamly.fold Streamly.Data.Array.write
  sBuffered :: Streamly.SerialT K.StreamlyM (F.Record PUMS.PUMS_Typed) <- K.streamlyToKnit
                                                                          $ buffer sPUMSRunningCount
  bufferRows <- K.streamlyToKnit $ Streamly.length sBuffered
  K.logLE K.Info $ "buffer stream is " <> show bufferRows <> " bytes long"
-}
  {-
  BR.clearIfPresentD testCacheKey
  K.logLE K.Info $ "retrieveOrMake (action)"
  awctPUMSRunningCount2 <- KAC.retrieveOrMake @KS.DefaultCacheData  (KC.knitSerializeStream sDict) testCacheKey (pure ())
                           $ const
                           $ return sPUMSRCToS
  swctPUMSRunningCount2 <- K.ignoreCacheTime awctPUMSRunningCount2
  --sPUMSRunningCount2 <- K.ignoreCacheTime csPUMSRunningCount
  iRows2 <-  K.streamlyToKnit $ Streamly.fold Streamly.Fold.length swctPUMSRunningCount2
  K.logLE K.Info $ "raw PUMS data has " <> (T.pack $ show iRows2) <> " rows."
-}

{-
  K.logLE K.Info "Testing pumsRowsLoader..."
  let pumsRowsFixedS = PUMS.pumsRowsLoader dataPath Nothing
  fixedRows <- K.streamlyToKnit $ Streamly.fold Streamly.Fold.length pumsRowsFixedS
  K.logLE K.Info $ "fixed PUMS data has " <> (T.pack $ show fixedRows) <> " rows."

  K.logLE K.Info "Testing pumsLoader..."
  BR.clearIfPresentD (T.pack "test/ACS_1YR.sbin")
  BR.clearIfPresentD (T.pack "data/test/ACS_1YR_Raw.sbin")
  pumsAge5F_C <- PUMS.pumsLoader' dataPath (Just "test/ACS_1YR_Raw.sbin") "test/ACS_1YR.sbin" Nothing
  pumsAge5F <- K.ignoreCacheTime pumsAge5F_C
  K.logLE K.Info $ "PUMS data has " <> (T.pack $ show $ FL.fold FL.length pumsAge5F) <> " rows."
-}

  K.logLE K.Info "done"
testsInIO :: IO ()
testsInIO = do
  let pumsCSV = "testbed/medPUMS.csv"
  putStrLn "Tests in IO"
  putStrLn $ T.unpack "Testing File.toBytes..."
  let rawBytesS = Streamly.File.toBytes pumsCSV
  rawBytes <- Streamly.fold Streamly.Fold.length rawBytesS
  putStrLn $ T.unpack $ "raw PUMS data has " <> (T.pack $ show rawBytes) <> " bytes."
  putStrLn $ T.unpack "Testing streamTable..."
  let pumsRowsRawS :: Streamly.SerialT IO PUMS.PUMS_Raw
        = FStreamly.readTableOpt Frames.defaultParser pumsCSV
  rawRows <- Streamly.fold Streamly.Fold.length pumsRowsRawS
  putStrLn $ T.unpack $ "raw PUMS data has " <> (T.pack $ show rawRows) <> " rows."
  let --pumsRowsFixedS :: Streamly.SerialT IO PUMS.PUMS
      pumsRowsFixedS = Loaders.recStreamLoader (Loaders.LocalData $ T.pack $ pumsCSV) Nothing Nothing PUMS.transformPUMSRow
  fixedRows <- Streamly.fold Streamly.Fold.length pumsRowsFixedS
  putStrLn $ T.unpack $ "fixed PUMS data has " <> (T.pack $ show fixedRows) <> " rows."

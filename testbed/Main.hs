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
{-# LANGUAGE TypeOperators #-}

{-# OPTIONS_GHC -O0 #-}
module Main where

import qualified Data.Text as T
import qualified Data.Map as M
import qualified Data.Serialize                as S
import qualified Data.Binary                as B
import qualified Data.Binary.Builder                as B
import qualified Data.Binary.Put                as B
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
import qualified Flat
import qualified Frames as F
import qualified Data.Vinyl.TypeLevel as V
import qualified Data.Vinyl as V
import qualified Frames.Streamly.CSV as FStreamly
import qualified Frames.Streamly.InCore as FStreamly
import qualified Frames.Serialize as FS
import qualified Frames.MapReduce as FMR

import qualified Frames.CSV                     as Frames
import qualified Streamly.Data.Fold            as Streamly.Fold
import qualified Streamly.Internal.Data.Fold.Types            as Streamly.Fold
import qualified Streamly.Internal.Data.Fold            as Streamly.Fold
import qualified Streamly.Prelude              as Streamly
import qualified Streamly.Internal.Prelude              as Streamly
--import qualified Streamly              as Streamly
import qualified Streamly.Internal.FileSystem.File
                                               as Streamly.File
import qualified Streamly.External.ByteString  as Streamly.ByteString

import qualified Streamly.Internal.Data.Array  as Streamly.Data.Array
import qualified Streamly.Internal.Memory.Array as Streamly.Memory.Array
import qualified Control.DeepSeq as DeepSeq

import qualified BlueRipple.Data.LoadersCore as Loaders
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Utilities.KnitUtils as BR
import qualified BlueRipple.Utilities.FramesUtils as BRF
import qualified BlueRipple.Data.CensusTables as BRC
import qualified BlueRipple.Data.CensusLoaders as BRL
import qualified BlueRipple.Data.KeyedTables as BRK

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
main = do

  pandocWriterConfig <- K.mkPandocWriterConfig pandocTemplate
                                               templateVars
                                               K.mindocOptionsF
  let cacheDir =  ".flat-kh-cache"
  let  knitConfig :: K.KnitConfig BR.SerializerC BR.CacheData Text = (K.defaultKnitConfig $ Just cacheDir)
        { K.outerLogPrefix = Just "Testbed.Main"
        , K.logIf = K.logDiagnostic
        , K.pandocWriterConfig = pandocWriterConfig
        , K.serializeDict = BR.flatSerializeDict
        , K.persistCache = KAC.persistStrictByteString (\t -> toString (cacheDir <> "/" <> t))
        }
  resE <- K.knitHtml knitConfig makeDoc
  case resE of
    Right htmlAsText ->
      K.writeAndMakePathLT "testbed.html" htmlAsText
    Left err -> putStrLn $ "Pandoc Error: " ++ show err

makeDoc :: forall r. (K.KnitOne r,  BR.CacheEffects r) => K.Sem r ()
makeDoc = do
  ctbd <- K.ignoreCacheTimeM BRL.censusTablesByDistrict
  BR.logFrame $ BRL.ageSexRace ctbd
{-
  let censusFile = "../GeoData/output_data/US_2018_cd116/cd116Raw.csv"
      tableDescriptions = BRK.allTableDescriptions BRC.sexByAge BRC.sexByAgePrefix
  (_, vTableRows) <- K.knitEither =<< (K.liftKnit $ BRK.decodeCSVTablesFromFile @BRC.CDPrefix tableDescriptions censusFile)
  K.logLE K.Info "Loaded and parsed CSV file"
  vRaceBySexByAgeTRs <- K.knitEither $ traverse (BRK.consolidateTables BRC.sexByAge BRC.sexByAgePrefix) vTableRows
  K.logLE K.Info "merged and typed sexByAge tables"
  let fRaceBySexByAge' = BRL.frameFromTableRows BRC.unCDPrefix BRL.raceBySexByAgeKeyRec 2018 vRaceBySexByAgeTRs
      fRaceBySexByAge = FL.fold (BRL.rekeyFrameF  @BRC.CDPrefixR BRL.raceBySexByAgeToASR4) fRaceBySexByAge'
  BR.logFrame fRaceBySexByAge
--  K.logLE K.Info $ show vSexByAge
-}
  return ()

{-

encodeOne :: S.Serialize a => a -> BB.Builder
encodeOne !x = S.execPut $ S.put x

bEncodeOne :: B.Binary a => a -> BB.Builder
bEncodeOne !x = B.fromLazyByteString $ B.runPut $ B.put x


bldrToCT  = Streamly.ByteString.toArray . BL.toStrict . BB.toLazyByteString

encodeBSB :: S.Serialize a => a -> BSB.Builder
encodeBSB !x = BSB.bytes $! encodeBS x

bEncodeBSB :: B.Binary a => a -> BSB.Builder
bEncodeBSB !x = BSB.bytes $! bEncodeBS x

encodeBS :: S.Serialize a => a -> BS.ByteString
encodeBS !x = S.runPut $! S.put x

bEncodeBS :: B.Binary a => a -> BS.ByteString
bEncodeBS !x = BL.toStrict $ B.runPut $! B.put x


bsbToCT  = Streamly.ByteString.toArray . BSB.builderBytes


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

toCT :: BSB.Builder -> Int -> Streamly.Memory.Array.Array Word8
toCT bldr n = bsbToCT $ encodeBSB (fromIntegral @Int @Word.Word64 n) <> bldr

bToCT :: BSB.Builder -> Int -> Streamly.Memory.Array.Array Word8
bToCT bldr n = bsbToCT $ bEncodeBSB (fromIntegral @Int @Word.Word64 n) <> bldr

streamlySerializeF2 :: forall c bldr m a ct.(Monad m, Monoid bldr, c a, c Word.Word64)
                   => (forall b. c b => b -> bldr)
                   -> (bldr -> ct)
                   -> Streamly.Fold.Fold m a ct
streamlySerializeF2 encodeOne bldrToCT =
  let fBuilder = Streamly.Fold.Fold step initial return where
        step !b !a = return $ b <> encodeOne a
        initial = return mempty
      toCT' bldr n = bldrToCT $ encodeOne (fromIntegral @Int @Word.Word64 n) <> bldr
  in toCT' <$> fBuilder <*> Streamly.Fold.length
--        extract (Accum n b) = return $ bldrToCT $ encodeOne (fromIntegral @Int @Word.Word64 n) <> b
{-# INLINEABLE streamlySerializeF2 #-}

toStreamlyFold :: Monad m => FL.Fold a b -> Streamly.Fold.Fold m a b
toStreamlyFold (FL.Fold step init done) = Streamly.Fold.mkPure step init done

toStreamlyFoldM :: Monad m => FL.FoldM m a b -> Streamly.Fold.Fold m a b
toStreamlyFoldM (FL.FoldM step init done) = Streamly.Fold.mkFold step init done

makeDoc :: forall r. (K.KnitOne r,  BR.CacheEffects r) => K.Sem r ()
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
  K.logLE K.Info $ "testing Memory.Array (fold to raw bytes)"

  K.logLE K.Info "Testing typedPUMSRowsLoader..."
  cfPUMSRaw <- PUMS.typedPUMSRowsLoader' dataPath (Just "testbed/acs1YR_All_Typed.bin")
  fPUMSRaw <- K.ignoreCacheTime cfPUMSRaw
  let nRaw = FL.fold FL.length fPUMSRaw
  K.logLE K.Info $ "PUMS data has " <> show nRaw <> " rows."

  K.logLE K.Info "Testing pumsRowsLoader..."
  cfPUMSSmall <- PUMS.pumsRowsLoader' dataPath (Just "testbed/acs1YR_Small.bin") Nothing
  fPUMSSmall <- K.ignoreCacheTime cfPUMSSmall
  let nSmall = FL.fold FL.length fPUMSSmall
  K.logLE K.Info $ "PUMS data has " <> show nSmall <> " rows."

  K.logLE K.Info "Testing pumsLoader"
  cfPUMS_C <- PUMS.pumsLoader' dataPath  (Just "testbed/acs1YR_All_Typed.bin") "testbed/pums.bin" Nothing
  fPUMS <- K.ignoreCacheTime cfPUMS_C
  let n = FL.fold FL.length fPUMS
  K.logLE K.Info $ "final PUMS data has " <> show n <> " rows."
-}

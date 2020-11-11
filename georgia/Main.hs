{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-# OPTIONS_GHC -fplugin=Polysemy.Plugin #-}
module Main where

import qualified GA_DataFrames as GA

import qualified Control.Foldl as FL
import Control.Monad (when)
import qualified Numeric.Foldl as NFL
import qualified Data.IntMap.Strict as IM
import qualified Data.Map as M
import qualified Data.Maybe as Maybe

import qualified Data.ByteString.Lazy as BL
import qualified Data.Random.Source.PureMT     as PureMT
import qualified Data.Text as T
import qualified Text.Printf as Printf
import qualified Data.Text.IO as T

import qualified Frames as F
import qualified Frames.Streamly.CSV as FS
import qualified Frames.MapReduce as FMR
import qualified Frames.Folds as FF
import qualified Frames.SimpleJoins as FJ
import qualified Data.Text.Encoding as TE
import qualified Data.Text.Encoding.Error as TE
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V
import qualified Data.Vinyl.Functor            as V
import qualified Data.Vector as Vec

import qualified Data.Csv as CSV

import qualified Frames.MapReduce as FMR

import qualified Graphics.Vega.VegaLite        as GV
import           Graphics.Vega.VegaLite.Configuration as FV
import qualified Graphics.Vega.VegaLite.Compat as FV
import qualified Frames.Visualization.VegaLite.Data
                                               as FV

import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Data.LoadersCore as BR
import qualified BlueRipple.Data.Loaders as BR
import qualified BlueRipple.Utilities.KnitUtils as BR
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.ElectionTypes as ET
import qualified BlueRipple.Model.MRP as BR
import qualified BlueRipple.Data.CCES as CCES
import qualified BlueRipple.Model.CCES_MRP_Analysis as CCES
import qualified BlueRipple.Data.Keyed as BK
import qualified BlueRipple.Utilities.KnitUtils as BR

import qualified BlueRipple.Model.House.ElectionResult as HEM
import qualified BlueRipple.Model.CachedModels as BRC
import qualified BlueRipple.Model.StanCCES as BRS
import qualified BlueRipple.Data.ACS_PUMS as PUMS

import qualified Streamly.Prelude as Streamly

import qualified CmdStan as CS
import qualified CmdStan.Types as CS
import qualified Stan.JSON as SJ
import qualified Stan.Parameters as SP
import qualified Stan.ModelRunner as SM
import qualified System.Environment as Env

import qualified Knit.Report as K
import           Polysemy.RandomFu              (RandomFu, runRandomIO, runRandomIOPureMT)
import Data.String.Here (here)


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
  pandocWriterConfig <- K.mkPandocWriterConfig pandocTemplate
                                               templateVars
                                               K.mindocOptionsF
  let  knitConfig = (K.defaultKnitConfig Nothing)
        { K.outerLogPrefix = Just "stan.Main"
        , K.logIf = K.logAll
        , K.pandocWriterConfig = pandocWriterConfig
        }
  let pureMTseed = PureMT.pureMT 1
  resE <- K.knitHtml knitConfig $ runRandomIOPureMT pureMTseed $ gaPUMAs
  case resE of
    Right htmlAsText ->
      K.writeAndMakePathLT "../Georgia/GA.html" htmlAsText
    Left err -> putStrLn $ "Pandoc Error: " ++ show err

type X = [BR.StateAbbreviation
         , BR.PUMA
         , PUMS.Citizens         
         , DT.PopPerSqMile
         , DT.SimpleAgeC
         , DT.SexC
         , DT.CollegeGradC
         , DT.Race5C
         , DT.AvgIncome
         ]
         
formatPct :: (V.KnownField t, Printf.PrintfArg (V.Snd t), Num (V.Snd t)) => V.Lift (->) V.ElField (V.Const T.Text) t
formatPct = FS.liftFieldFormatter (T.pack . Printf.printf "%.1f")

formatWholeNumber :: (V.KnownField t, Printf.PrintfArg (V.Snd t), Num (V.Snd t)) => V.Lift (->) V.ElField (V.Const T.Text) t
formatWholeNumber = FS.liftFieldFormatter (T.pack . Printf.printf "%.0f")

type CountyDescWA = [BR.StateFIPS, BR.StateAbbreviation, DT.CensusRegionC, BR.CountyFIPS, BR.CountyName] 
type FracBlack = "FracBlack" F.:-> Double
type FracLatinX = "FracLatinX" F.:-> Double
type FracAsian = "FracAsian" F.:-> Double
type FracYoung = "FracYoung" F.:-> Double
type FracGrad = "FracGrad" F.:-> Double

type CountySummary = [FracYoung, FracGrad, FracBlack, FracLatinX, FracAsian, PUMS.Citizens, DT.PopPerSqMile]

countySummaryF :: FL.Fold (F.Record (DT.CatColsASER5 V.++ PUMS.PUMSCountToFields)) (F.Record CountySummary)
countySummaryF =
  let wgt = realToFrac . F.rgetField @PUMS.Citizens
      wgtdFracF bf = (/) <$> FL.prefilter bf (FL.premap wgt FL.sum) <*> FL.premap wgt FL.sum
      wgtdSumF f = (/) <$> FL.premap (\r -> f r * wgt r) FL.sum <*> FL.premap wgt FL.sum
  in FF.sequenceRecFold
     $ FF.toFoldRecord (wgtdFracF ((== DT.Under) . F.rgetField @DT.SimpleAgeC))
     V.:& FF.toFoldRecord (wgtdFracF ((== DT.Grad) . F.rgetField @DT.CollegeGradC))
     V.:& FF.toFoldRecord (wgtdFracF ((== DT.R5_Black) . F.rgetField @DT.Race5C))
     V.:& FF.toFoldRecord (wgtdFracF ((== DT.R5_Latinx) . F.rgetField @DT.Race5C))
     V.:& FF.toFoldRecord (wgtdFracF ((== DT.R5_Asian) . F.rgetField @DT.Race5C))
     V.:& FF.toFoldRecord (FL.premap (F.rgetField @PUMS.Citizens) FL.sum)
     V.:& FF.toFoldRecord (wgtdSumF (F.rgetField @DT.PopPerSqMile))
     V.:& V.RNil
      


gaPUMAs :: forall r.(K.KnitOne r,  K.CacheEffectsD r, K.Member RandomFu r) => K.Sem r ()
gaPUMAs = do
  let f r = F.rgetField @BR.StateAbbreviation r == "GA" && F.rgetField @BR.Year r == 2018
  gaPUMAs_C <- fmap (F.filterFrame f) <$> PUMS.pumsLoaderAdults
--  K.clearIfPresent "georgia/gaPUMAs.bin"
  gaPUMAsRolled_C <- BR.retrieveOrMakeFrame "georgia/gaPUMAs.bin" gaPUMAs_C $ \gaPUMAs_Raw -> do
    let rolledUp = FL.fold (PUMS.pumsRollupF (const True) (PUMS.pumsKeysToASER5 True)) gaPUMAs_Raw
        zeroCount :: F.Record PUMS.PUMSCountToFields
        zeroCount = 0 F.&: 0 F.&: 0 F.&: 0 F.&: 0 F.&: 0 F.&: 0 F.&: 0 F.&: 0 F.&: 0 F.&: 0 F.&: 0 F.&: V.RNil
        addZeroCountsF =  FMR.concatFold $ FMR.mapReduceFold
                          (FMR.noUnpack)
                          (FMR.assignKeysAndData @PUMS.PUMADescWA)
                          ( FMR.makeRecsWithKey id
                            $ FMR.ReduceFold
                            $ const
                            $ BK.addDefaultRec @DT.CatColsASER5 zeroCount
                          )
        rolledUpWZ = F.toFrame $ FL.fold addZeroCountsF rolledUp
        formatRec =
          FS.formatTextAsIs
          V.:& FS.formatWithShow
          V.:& FS.formatWithShow
          V.:& formatPct
          V.:& FS.formatWithShow
          V.:& FS.formatWithShow
          V.:& FS.formatWithShow
          V.:& FS.formatWithShow
          V.:& formatWholeNumber
          V.:&  V.RNil
--    BR.logFrame $ fmap (F.rcast @X) $ rolledUpWZ
    K.liftKnit @IO $ FS.writeLines "gaPUMAs.csv" $ FS.streamSV' formatRec "," $ Streamly.fromFoldable  $ fmap (F.rcast @X) $ rolledUpWZ
    return rolledUpWZ
  countyFromPUMA_C <- BR.countyToPUMALoader
  let gaCountyDeps = (,) <$> gaPUMAsRolled_C <*> countyFromPUMA_C
  gaCounties_C <- BR.retrieveOrMakeFrame "georgia/gaCounties.bin" gaCountyDeps $ \(gaPUMAs, countyFromPUMA') -> do
    let countyFromPUMA = fmap (F.rcast @[BR.StateFIPS, BR.PUMA, BR.CountyFIPS, BR.CountyName, BR.FracPUMAInCounty]) countyFromPUMA'
        (byPUMAwCountyAndWeight, missing) = FJ.leftJoinWithMissing @[BR.StateFIPS, BR.PUMA] gaPUMAs countyFromPUMA
    when (not $ null missing) $ K.knitError $ "missing items in join: " <> (T.pack $ show missing)
    let demoByCountyF = FMR.concatFold
                       $ FMR.mapReduceFold
                       FMR.noUnpack
                       (FMR.assignKeysAndData
                         @('[BR.Year] V.++ CountyDescWA V.++ DT.CatColsASER5)
                         @('[BR.FracPUMAInCounty] V.++ PUMS.PUMSCountToFields))
                       (FMR.foldAndAddKey (PUMS.sumPUMSCountedF (Just $ F.rgetField @BR.FracPUMAInCounty) F.rcast))
    let demoByCounty = FL.fold demoByCountyF byPUMAwCountyAndWeight
    return demoByCounty

  let countiesSummaryF = FMR.concatFold
                         $ FMR.mapReduceFold
                         FMR.noUnpack
                         (FMR.assignKeysAndData @[BR.CountyFIPS, BR.CountyName] @(DT.CatColsASER5 V.++ PUMS.PUMSCountToFields))
                         (FMR.foldAndAddKey countySummaryF)
                         
  gaCounties <- K.ignoreCacheTime gaCounties_C
  BR.logFrame gaCounties
  let countiesSummary = FL.fold countiesSummaryF gaCounties
  BR.logFrame countiesSummary
  return ()
{-  
  _ <- K.addHvega Nothing Nothing $ vlDensityByPUMA
    "Young & College Educated by PUMA"
    (FV.ViewConfig 600 600 10)
    gaPUMAs
-}

gaSenateAnalysis :: (K.KnitOne r,  K.CacheEffectsD r, K.Member RandomFu r) => K.Sem r ()
gaSenateAnalysis = do
  gaSenate1_C <- gaSenate1Loader
  gaSenate2_C <- gaSenate2Loader
  gaSenate1 <- K.ignoreCacheTime gaSenate1_C
  let senate1PM = FL.fold votesByMethodAndPartyF $ F.filterFrame ((/= "Total") . F.rgetField @GA.Method) gaSenate1
  _ <- K.addHvega Nothing Nothing $ vlVotingMethod
    "Voting Method by County for Democratic votes in the Ossoff/Perdue race"
    (FV.ViewConfig 600 1500 10)
    (fmap F.rcast $ F.filterFrame ((== "D") . F.rgetField @GA.Party) senate1PM)
  _ <- K.addHvega Nothing Nothing $ vlVotingMethod
    "Voting Method by County for Republican votes in the Ossoff/Perdue race"
    (FV.ViewConfig 600 1500 10)
    (fmap F.rcast $ F.filterFrame ((== "R") . F.rgetField @GA.Party) senate1PM)    
--  gaProcessElex
  return ()


votesByMethodAndPartyF :: FL.Fold GA.Senate (F.FrameRec [GA.CountyFIPS, GA.County, GA.Party, GA.Method, GA.Votes])
votesByMethodAndPartyF = FMR.concatFold
                         $ FMR.mapReduceFold
                         FMR.noUnpack
                         (FMR.assignKeysAndData @[GA.CountyFIPS, GA.County, GA.Party, GA.Method] @'[GA.Votes])
                         (FMR.foldAndAddKey (FF.foldAllConstrained @Num FL.sum))

gaSenate1Loader :: (K.KnitEffects r, K.CacheEffectsD r) => K.Sem r (K.ActionWithCacheTime r (F.Frame GA.Senate))
gaSenate1Loader = BR.cachedFrameLoader (BR.LocalData $ T.pack GA.senate1CSV) Nothing Nothing id Nothing "georgia/senate1.bin"

gaSenate2Loader :: (K.KnitEffects r, K.CacheEffectsD r) => K.Sem r (K.ActionWithCacheTime r (F.Frame GA.Senate))
gaSenate2Loader = BR.cachedFrameLoader (BR.LocalData $ T.pack GA.senate2CSV) Nothing Nothing id Nothing "georgia/senate2.bin"

gaProcessElex :: (K.KnitEffects r) => K.Sem r ()
gaProcessElex = do
  let s1CSV = "../Georgia/data/election/Senate1.csv"
      s2CSV = "../Georgia/data/election/Senate2.csv"
      processOne :: (T.Text, T.Text) -> Maybe [T.Text]
      processOne (h, n) = 
        let hParts = T.splitOn "_" h
        in if length hParts < 3
           then Nothing
           else let (p : (c : ms)) = hParts in Just [p, c, mconcat ms, n]

      processLine :: M.Map T.Text T.Text -> Maybe [T.Text]
      processLine m = do
        stateFIPS <- M.lookup "State_FIPS" m
        countyFIPS <- M.lookup "County_FIPS" m
        county <- M.lookup "County" m
        let common = [stateFIPS, countyFIPS, county]
        let m' = M.delete "State_FIPS" $ M.delete "County_FIPS" $ M.delete "County" $ M.delete "Total" m
        processed <- traverse processOne $ M.toList m'
        let prefixed = fmap (common <>) processed
        return $ fmap (T.intercalate ",") prefixed

      processFile :: Vec.Vector (M.Map T.Text T.Text) -> Maybe T.Text
      processFile v = do
        let h :: T.Text = "StateFIPS,CountyFIPS,County,Party,Candidate,Method,Votes"
        rows <- mconcat . Vec.toList <$> traverse processLine v
        return $ T.intercalate "\n" (h : rows)
        
  csv1 <- loadCSVToMaps s1CSV
  res1 <- K.knitMaybe "csv loading error" $ processFile csv1
  K.liftKnit $ T.writeFile "../Georgia/data/election/Senate1_long.csv" res1

  csv2 <- loadCSVToMaps s2CSV
  res2 <- K.knitMaybe "csv loading error" $ processFile csv2
  K.liftKnit $ T.writeFile "../Georgia/data/election/Senate2_long.csv" res2
          
        
loadCSVToMaps :: (K.KnitEffects r) => FilePath -> K.Sem r (Vec.Vector (M.Map T.Text T.Text))
loadCSVToMaps fp = do
  csvData <- K.liftKnit $ BL.readFile fp
  case CSV.decodeByName csvData of
    Left err -> K.knitError $ "CSV parsing error: " <> (T.pack err)
    Right (_, rowsV) -> return rowsV
    
{-    do
      let headerV = Vec.map (TE.decodeUtf8With TE.strictDecode) headerV'
      return $ fmap (M.fromList . Vec.toList . Vec.zip headerV) rowsV
-}

padTo :: Int -> Int -> T.Text
padTo width n =
  let nText = T.pack $ show n
  in T.replicate (width - T.length nText) "0" <> nText 
  
gaPUMATopoJSONUrl =  "https://raw.githubusercontent.com/blueripple/Georgia/main/topojson/ga_PUMAs.json"
gaCountyTopoJSONUrl = "https://raw.githubusercontent.com/blueripple/Georgia/main/topojson/ga-counties.json"

vlDensityByPUMA :: Foldable f
                => T.Text
                -> FV.ViewConfig
                -> f (F.Record X)
                -> GV.VegaLite
vlDensityByPUMA title vc rows = 
  let toVLDataRec = FV.textAsVLStr "State"
                    V.:& FV.asVLData (GV.Str . padTo 5) "PUMA"
                    V.:& FV.useColName FV.asVLNumber
                    V.:& FV.useColName FV.asVLNumber
                    V.:& FV.asVLStrViaShow "Age45"
                    V.:& FV.asVLStrViaShow "Sex"
                    V.:& FV.asVLStrViaShow "Education"
                    V.:& FV.asVLStrViaShow "Race"
                    V.:& FV.useColName FV.asVLNumber
                    V.:& V.RNil
      dat = FV.recordsToData toVLDataRec rows
      datGeo = GV.dataFromUrl gaPUMATopoJSONUrl [GV.TopojsonFeature "ga_PUMAs"]
      filter = GV.filter (GV.FExpr $ "datum.Education == 'Grad' && datum.Age45 == 'Under'")
      projection = GV.projection [GV.PrType GV.AlbersUsa]
      transform = GV.transform . GV.lookup "PUMA" datGeo "properties.PUMA" (GV.LuAs "geo") . filter
      mark = GV.mark GV.Geoshape []
      colorEnc = GV.color [GV.MName "Citizens", GV.MmType GV.Quantitative]
      shapeEnc = GV.shape [GV.MName "geo", GV.MmType GV.GeoFeature]
      enc = GV.encoding . colorEnc . shapeEnc
  in FV.configuredVegaLite vc [FV.title title, transform [], enc [], mark, projection, dat]


vlVotingMethod :: Foldable f
               => T.Text
               -> FV.ViewConfig
               -> f (F.Record [GA.County, GA.Party, GA.Method, GA.Votes])
               -> GV.VegaLite
vlVotingMethod title vc rows =
  let toVLDataRec = FV.useColName FV.textAsVLStr
                    V.:& FV.useColName FV.textAsVLStr
                    V.:& FV.useColName FV.textAsVLStr
                    V.:& FV.useColName FV.asVLNumber
                    V.:& V.RNil
      dat = FV.recordsToData toVLDataRec rows
      encY = GV.position GV.Y [GV.PName "County", GV.PmType GV.Nominal]
      encX = GV.position GV.X [GV.PName "Votes", GV.PmType GV.Quantitative]
      encColor = GV.color [GV.MName "Method", GV.MmType GV.Nominal]
      enc = GV.encoding . encX . encY . encColor
      mark = GV.mark GV.Bar []
  in FV.configuredVegaLite vc [FV.title title, enc [], mark, dat]

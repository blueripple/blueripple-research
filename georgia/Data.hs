{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-# OPTIONS_GHC -fplugin=Polysemy.Plugin #-}
module Data where

import qualified GA_DataFrames as GA
import qualified Parsers as GA

import qualified Control.Foldl as FL
import Control.Monad (when)
import qualified Numeric.Foldl as NFL
import qualified Data.IntMap.Strict as IM
import qualified Data.Map as M
import qualified Data.Maybe as Maybe

import qualified Data.ByteString.Lazy as BL
import qualified Data.Random.Source.PureMT     as PureMT
import qualified Data.List as L
import qualified Data.Text as T
import qualified Text.Printf as Printf
import qualified Data.Text.IO as T

import qualified Frames as F
import qualified Frames.Streamly.CSV as FS
import qualified Frames.MapReduce as FMR
import qualified Frames.Folds as FF
import qualified Frames.SimpleJoins as FJ
import qualified Frames.Transform as FT
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
import qualified System.Directory              as System

import qualified Knit.Report as K
import           Polysemy.RandomFu              (RandomFu, runRandomIO, runRandomIOPureMT)
import Data.String.Here (here)

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
type FracNonWhite = "FracNonWhite" F.:-> Double


type CountySummary = [FracYoung, FracGrad, FracBlack, FracLatinX, FracAsian, FracNonWhite, PUMS.Citizens, DT.PopPerSqMile]

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
     V.:& FF.toFoldRecord (wgtdFracF ((/= DT.R5_WhiteNonLatinx) . F.rgetField @DT.Race5C))
     V.:& FF.toFoldRecord (FL.premap (F.rgetField @PUMS.Citizens) FL.sum)
     V.:& FF.toFoldRecord (wgtdSumF (F.rgetField @DT.PopPerSqMile))
     V.:& V.RNil



votesF :: forall v t. (V.KnownField v
                      , V.Snd v ~ Int
                      , V.KnownField t
                      , V.Snd t ~ Int
                      )
       => T.Text -> FL.Fold GA.Senate (F.FrameRec [BR.CountyFIPS, GA.County, v, t])
votesF p =
  let f r = F.rgetField @GA.Method r == "Total"
      totalF = (\v t -> v F.&: t F.&: V.RNil)
               <$> FL.prefilter ((== p) . F.rgetField @GA.Party) (FL.premap (F.rgetField @GA.Votes) FL.sum)
               <*> FL.premap (F.rgetField @GA.Votes) FL.sum
  in FMR.concatFold
     $ FMR.mapReduceFold
     (FMR.unpackFilterRow f)
     (FMR.assignKeysAndData @[BR.CountyFIPS, GA.County] @[GA.Party, GA.Votes])
     (FMR.foldAndAddKey totalF)

type DVotes1 = "DVotes1" F.:-> Int
type DVotes2 = "DVotes2" F.:-> Int

type TVotes1 = "TVotes1" F.:-> Int
type TVotes2 = "TVotes2" F.:-> Int


type SenateByCounty = [BR.CountyFIPS,GA.County, DVotes1, TVotes1, DVotes2, TVotes2] V.++ CountySummary

gaSenateToModel :: forall r.(K.KnitOne r,  K.CacheEffectsD r)
           => K.Sem r (K.ActionWithCacheTime r (F.FrameRec SenateByCounty))
gaSenateToModel = do
  let addGAFIPS :: GA.Senate -> GA.Senate
      addGAFIPS r = F.rputField @BR.CountyFIPS (F.rgetField @BR.CountyFIPS r + 13000) r
  gaSenate1_C <- fmap (fmap addGAFIPS) <$> gaSenate1Loader
  gaSenate2_C <- fmap (fmap addGAFIPS) <$> gaSenate2Loader
  gaDemo <- fmap (F.rcast @('[BR.CountyFIPS] V.++ CountySummary)) <$> gaCountyDemographics  
  BR.logFrame gaDemo
  let deps = (,) <$> gaSenate1_C <*> gaSenate2_C
--  K.clearIfPresent "georgia/senateVotesAndDemo.bin"
  toModel_C <- BR.retrieveOrMakeFrame "georgia/senateVotesAndDemo.bin" deps $ \(s1, s2) -> do
    let senate1Votes = FL.fold (votesF @DVotes1 @TVotes1 "D") s1
        senate2Votes = fmap (F.rcast @[BR.CountyFIPS, DVotes2, TVotes2]) $ FL.fold (votesF @DVotes2 @TVotes2 "D") s2
        (combined, missing1, missing2) = FJ.leftJoin3WithMissing @'[BR.CountyFIPS] senate1Votes senate2Votes gaDemo
    when (not $ null missing1) $  K.knitError $ "missing counties in votes1 and votes2 join: " <> (T.pack $ show missing1)
    when (not $ null missing2) $  K.knitError $ "missing counties in votes1 and demo join: " <> (T.pack $ show missing2)
    return $ F.toFrame $ L.reverse $ L.sortOn (F.rgetField @DVotes1) $ FL.fold FL.list combined
  K.ignoreCacheTime toModel_C >>= BR.logFrame
  return $ toModel_C

gaCountyDemographics :: (K.KnitEffects r,  K.CacheEffectsD r)
  => K.Sem r (F.FrameRec ([BR.CountyFIPS, BR.CountyName] V.++ CountySummary))
gaCountyDemographics = do
  let f r = F.rgetField @BR.StateAbbreviation r == "GA" && F.rgetField @BR.Year r == 2018
  gaPUMAs_C <- fmap (F.filterFrame f) <$> PUMS.pumsLoaderAdults
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
--  BR.logFrame gaCounties
  let countiesSummary = FL.fold countiesSummaryF gaCounties
--  BR.logFrame countiesSummary
  return countiesSummary

  
{-  
  _ <- K.addHvega Nothing Nothing $ vlDensityByPUMA
    "Young & College Educated by PUMA"
    (FV.ViewConfig 600 600 10)
    gaPUMAs
-}

gaCountyToCountyFIPS :: (K.KnitEffects r,  K.CacheEffectsD r) => K.Sem r (K.ActionWithCacheTime r (M.Map T.Text T.Text))
gaCountyToCountyFIPS = do
  gaSenate1_C <- gaSenate1Loader
  K.retrieveOrMake "georgia/countyToFIPS.bin" gaSenate1_C $
    return . FL.fold (FL.premap (\r -> (F.rgetField @GA.County r, T.pack $ show $ F.rgetField @GA.CountyFIPS r)) FL.map)




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

gaProcessSenate :: K.KnitEffects r => K.Sem r ()
gaProcessSenate = do
  gaProcessElex1 "../Georgia/data/election/Senate1.csv"
  gaProcessElex1 "../Georgia/data/election/Senate2.csv"
  

wide2LongByCounty :: GA.Parsec [[T.Text]]
wide2LongByCounty = GA.wideReturns
                     ["County","RegisteredVoters"]
                     ["Candidate","Party","Method"]
                     GA.parseWideHeader

wide2LongByPrecinct :: GA.Parsec [[T.Text]]
wide2LongByPrecinct  = GA.wideReturns
                     ["County","RegisteredVoters"]
                     ["Candidate","Party","Method"]
                     GA.parseWideHeader                     


--type County = "County
type RegVoters = "RegVoters" F.: -> Int
type CandidateName = "CandidateName" F.:-> T.Text
type PartyT = "Party" F.:-> T.Text
type VoteMethod = "VoteMethod" F.:-> T.Text
type Votes = "Votes" F.:-> Int

type CountyReturns = [BR.CountyName, BR.CountyFIPS, CandidateName, PartyT, VoteMethod, Votes]

loadReturnsByCountyFromWide ::  M.Map T.Text T.Text
                            -> T.Text
                            -> FilePath
                            -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec CountyReturns))
loadReturnsByCountyFromWide gaFIPSByCounty cacheKey wideFP = do
  wideCSVDep <- K.fileDependency wideFP
  K.retrieveOrMakeFrame ("georgia/countyReturns/" <> cacheKey) wideCSVDep $ \_ ->
    K.logLE K.Info $ "\"" <> wideFP <> "\" newer than cached or cached not found.  Rebuilding."
    K.logLE K.Info $ "Parsing"
    
    
  
  

type Precinct = "Precinct" F.: -> T.Text
type PrecinctReturns = [BR.CountyName, BR.CountyFIPS, Precinct, CandidateName, PartyT, VoteMethod, Votes]

loadReturnsByPrecinctFromWide ::  M.Map T.Text T.Text -> FilePath -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec PrecinctReturns))
loadReturnsByPrecinctFromWide = undefined


meltReturnData :: (K.KnitEffects r, K.CacheEffectsD r) => GA.Parsec [[T.Text]] -> [T.Text] -> K.Sem r ()
meltReturnData parser dirs = do
  let processDir d = do
        createDirIfNecessary (d <> "/long")
        csvFiles <- filter (T.isSuffixOf ".csv") . fmap T.pack <$> K.liftKnit (System.listDirectory $ T.unpack d)
        let inputFP fp = T.unpack $ d <> "/" <> fp
            outputFP fp = T.unpack $  d <> "/long/" <> fp
            meltOne fp = GA.reformatFileToCSV  (inputFP fp) parser (outputFP fp) 
        traverse (K.liftKnit . meltOne) csvFiles
  _ <- traverse processDir dirs
  return ()      


  
gaProcessElex1 :: K.KnitEffects r => FilePath -> K.Sem r ()
gaProcessElex1 fp = do
  let outFile = (\(d, f) -> d <> "long/" <> f) $ T.breakOnEnd "/" (T.pack fp)
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
        
  csv <- loadCSVToMaps fp  
  res <- K.knitMaybe ("csv loading error in \"" <> T.pack fp <> "\"") $ processFile csv
  K.liftKnit $ T.writeFile (T.unpack outFile) res

gaMeltByCountyRepo :: (K.KnitEffects r, K.CacheEffectsD r) => K.Sem r ()
gaMeltByCountyRepo = meltReturnData wide2LongByCounty
             $ fmap ("/Users/adam/DataScience/techiesforga/data/elections/2020_november/" <>)
             ["all_counties_joined","all_counties_joined/other_races"]

gaMeltByPrecinctRepo :: (K.KnitEffects r, K.CacheEffectsD r) => K.Sem r ()
gaMeltByPrecinctRepo = meltReturnData wide2LongByPrecinct
                       $ fmap ("/Users/adam/DataScience/techiesforga/data/elections/2020_november/" <>)
                       ["all_counties_joined","all_counties_joined/other_races"]
  


gaProcessElex2 :: K.KnitEffects r => M.Map T.Text T.Text -> FilePath -> K.Sem r ()
gaProcessElex2 countyFIPSByCounty fp = do
  let outFile = (\(d, f) -> d <> "long/" <> f) $ T.breakOnEnd "/" (T.pack fp)
  K.logLE K.Diagnostic $ "re-formatting \"" <> (T.pack fp) <> "\". Saving as \"" <> outFile <> "\"" 
  let processOne :: (T.Text, T.Text) -> Either T.Text [T.Text]
      processOne (h, n) = 
        let (x, y) = T.breakOn "_" h
            method = T.drop 1 y -- drop the "_"
            (cand', party') = T.breakOn "(" x
            cand = T.dropEnd 1 cand' -- drop the space
            party = T.drop 1 $ T.dropEnd 1 party' -- drop the surrounding ()
        in
          if (T.length method > 0) && (T.length cand > 0) && (T.length party > 0)
          then Right [party, cand, method, n]
          else Left $ "parse Error: " <> h <> "-> method=" <> method <> "; cand=" <> cand <> "; party=" <> party

      processLine :: M.Map T.Text T.Text -> Either T.Text [T.Text]
      processLine m = do
        county <- maybe (Left "Failed to find County header") Right $ M.lookup "County" m
        countyFIPS <- maybe (Left $ "Failed to find countyFIPS for " <> county) Right  $ M.lookup county countyFIPSByCounty
        let common = ["13", countyFIPS, county]
        let m' = M.delete "County" $ M.delete "Total" m
        processed <- either (\msg -> Left $ (T.pack $ show m') <> " -> " <> msg) Right
                     $ traverse processOne
                     $ filter (\(h,_) -> T.length h > 0)
                     $ M.toList m'
        let prefixed = fmap (common <>) processed
        return $ fmap (T.intercalate ",") prefixed

      processFile :: Vec.Vector (M.Map T.Text T.Text) -> Either T.Text T.Text
      processFile v = do
        let h :: T.Text = "StateFIPS,CountyFIPS,County,Party,Candidate,Method,Votes"
        rows <- mconcat . Vec.toList <$> traverse processLine v
        return $ T.intercalate "\n" (h : rows)
        
  csv <- loadCSVToMaps fp  
  res <- K.knitEither $ processFile csv
  K.liftKnit $ T.writeFile (T.unpack outFile) res

    
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

createDirIfNecessary
  :: K.KnitEffects r
  => T.Text
  -> K.Sem r ()
createDirIfNecessary dir = K.wrapPrefix "createDirIfNecessary" $ do
  K.logLE K.Diagnostic $ "Checking if cache path (\"" <> dir <> "\") exists."
  existsB <- K.liftKnit $ (System.doesDirectoryExist (T.unpack dir))
  case existsB of
    True -> do
      K.logLE K.Diagnostic $ "\"" <> dir <> "\" exists."
      return ()
    False -> do
      K.logLE K.Info
        $  "Cache directory (\""
        <> dir
        <> "\") not found. Atttempting to create."
      K.liftKnit
        $ System.createDirectoryIfMissing True (T.unpack dir)
{-# INLINEABLE createDirIfNecessary #-}

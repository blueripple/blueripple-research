{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# OPTIONS_GHC -O0 -freduction-depth=0 #-}
{-# OPTIONS_GHC -fplugin=Polysemy.Plugin #-}
module Main where


import qualified Control.Foldl as FL
import           Data.Discrimination            ( Grouping )
import qualified Data.Map as M
import           Data.Maybe (fromJust)
import qualified Data.List as L
import qualified Data.Ord as Ord
import qualified Data.Random.Source.PureMT     as PureMT
import qualified Data.Text as T
import qualified Data.Vector as Vec
import qualified Data.Vinyl as V
import qualified Data.Vinyl.Core as V
import qualified Data.Vinyl.Functor as V
import qualified Data.Vinyl.TypeLevel as V

import qualified Data.Serialize as S

import qualified Text.Printf as Printf

import qualified Frames as F
import qualified Frames.CSV as F
import qualified Frames.InCore as FI
import qualified Frames.Transform as FT
import qualified Frames.SimpleJoins as FJ
import qualified Frames.Streamly.CSV as FS
import qualified Frames.MapReduce as FMR

import qualified Graphics.Vega.VegaLite        as GV
import           Graphics.Vega.VegaLite.Configuration as FV
import qualified Graphics.Vega.VegaLite.Compat as FV
import qualified Frames.Visualization.VegaLite.Data
                                               as FV

import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Data.Loaders as BR
import qualified BlueRipple.Data.LoadersCore as BR
import qualified BlueRipple.Utilities.KnitUtils as BR
import qualified BlueRipple.Data.ElectionTypes as ET
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.ACS_PUMS as PUMS
import qualified BlueRipple.Data.Keyed         as BK
import qualified BlueRipple.Model.CachedModels as MRP

import qualified Polysemy.RandomFu as P

import qualified Streamly as Streamly
import qualified Streamly.Prelude as Streamly

import GHC.Generics (Generic)

import qualified Knit.Report as K
--import qualified Knit.Report.Cache as K
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
        { K.outerLogPrefix = Just "house-data.Main"
        , K.logIf = K.logDiagnostic
        , K.pandocWriterConfig = pandocWriterConfig
        }
       pureMTseed = PureMT.pureMT 1  
  resE <- K.knitHtml knitConfig $ P.runRandomIOPureMT pureMTseed $ makeDoc
  case resE of
    Right htmlAsText ->
      K.writeAndMakePathLT "housedata.html" htmlAsText
    Left err -> putStrLn $ "Pandoc Error: " ++ show err

makeDoc :: forall r.(K.KnitOne r,  K.CacheEffectsD r, K.Member P.RandomFu r) => K.Sem r ()
makeDoc = do
  pollData_C <- housePolls2020Loader
  fecData_C <- fec2020Loader
  houseElections_C <- BR.houseElectionsLoader
  stateAbbr_C <- BR.stateAbbrCrosswalkLoader

  let houseRaceDeps = (,,,) <$> pollData_C <*> stateAbbr_C <*> houseElections_C <*> fecData_C
--  K.clearIfPresent "house-data/houseRaceInfo.bin" -- until we have it right
  houseRaceInfo_C <- BR.retrieveOrMakeFrame "house-data/houseRaceInfo.bin" houseRaceDeps $ \(pollsF, abbrF, electionsF, fecF) -> do
    -- add state abbreviations to pollData
    pollsWithAbbr <- K.knitEither
                     $ either (Left . T.pack . show) Right
                     $ FJ.leftJoinE @'[BR.StateName] pollsF abbrF
    -- make each poll a row
    let includeRow r = (F.rgetField @BR.Cycle r == 2020) && (F.rgetField @ET.Party r `elem` [ET.Democratic, ET.Republican])
        flattenPollsF = FMR.concatFold
                        $ FMR.mapReduceFold
                        (FMR.unpackFilterRow includeRow)
                        (FMR.assignKeysAndData @'[BR.StateAbbreviation, BR.Pollster, BR.FteGrade, BR.CongressionalDistrict, BR.EndDate] @'[ET.Party, BR.Pct])
                        (FMR.foldAndAddKey pctVoteShareF)
    let flattenedPollsF = FL.fold flattenPollsF pollsWithAbbr
    let grades = ["A+", "A", "A-", "A/B", "B+", "B", "B-", "B/C", "C+", "C", "C-", "C/D", "D+", "D", "D-", "D/F", "F"]
        acceptableGrade = ["A+", "A", "A-", "A/B", "B+", "B"]
        comparePolls :: F.Record '[BR.FteGrade, BR.EndDate] -> F.Record '[BR.FteGrade, BR.EndDate] -> Ord.Ordering
        comparePolls p1 p2 =
          let p1Grade = F.rgetField @BR.FteGrade p1
              p2Grade = F.rgetField @BR.FteGrade p2
              p1End = F.rgetField @BR.EndDate p1
              p2End = F.rgetField @BR.EndDate p2
              p1Valid = p1Grade `elem` acceptableGrade
              p2Valid = p2Grade `elem` acceptableGrade
          in case (p1Valid, p2Valid) of
            (True, True) -> compare p1End p2End
            (True, False) -> GT
            (False, True) -> LT
            (False, False) -> compare p1End p2End
        choosePollF = FMR.concatFold
                      $ FMR.mapReduceFold
                      FMR.noUnpack
                      (FMR.assignKeysAndData @[BR.StateAbbreviation, BR.CongressionalDistrict] @[BR.Pollster, BR.FteGrade, BR.EndDate, BestPollShare, BestPollDiff])
                      (FMR.foldAndAddKey (fmap fromJust $ FL.maximumBy (\p1 p2 -> comparePolls (F.rcast p1) (F.rcast p2))))
        chosenPollFrame = FL.fold choosePollF flattenedPollsF
        
        houseVoteShareF = FMR.concatFold $ FMR.mapReduceFold
                          (FMR.unpackFilterOnField @BR.Year (==2018))
                          (FMR.assignKeysAndData @[BR.StateAbbreviation, BR.CongressionalDistrict])
                          (FMR.foldAndAddKey votesToVoteShareF)
        adjCD r = if (F.rgetField @BR.CongressionalDistrict r == 0) then (F.rputField @BR.CongressionalDistrict 1 r) else r -- 538 codes at-large as district 1 while our results file codes them as 0
        houseResultsFrame = fmap adjCD $ FL.fold houseVoteShareF electionsF

    -- Now the money
    let sumMoneyF :: FL.Fold (F.Record [BR.CashOnHand, BR.Disbursements, BR.IndSupport, BR.IndOppose, BR.PartyExpenditures]) (F.Record '[AllMoney])
        sumMoneyF = FL.Fold
                    (\s r -> s
                             + (realToFrac $ F.rgetField @BR.CashOnHand r)
                             + (realToFrac $ F.rgetField @BR.Disbursements r)
                             + (realToFrac $ F.rgetField @BR.IndSupport r)
                             + (realToFrac $ F.rgetField @BR.IndOppose r)
                             + (realToFrac $ F.rgetField @BR.PartyExpenditures r))
                    0
                    FT.recordSingleton
        filterToHouse r = "H" `T.isPrefixOf` F.rgetField @BR.CandidateId r 
        flattenMoneyF = FMR.concatFold
                        $ FMR.mapReduceFold
                        (FMR.unpackFilterRow filterToHouse)
                        (FMR.assignKeysAndData @[BR.StateAbbreviation, BR.CongressionalDistrict])
                        (FMR.foldAndAddKey sumMoneyF)
        allMoneyFrame = FL.fold flattenMoneyF fecF
    moneyElexFrame <- K.knitEither
                      $ either (Left . T.pack . show) Right
                      $ FJ.leftJoinE @[BR.StateAbbreviation, BR.CongressionalDistrict] houseResultsFrame (fmap adjCD allMoneyFrame)

    let (polledFrame, unpolledKeys) = FJ.leftJoinWithMissing @[BR.StateAbbreviation, BR.CongressionalDistrict] moneyElexFrame chosenPollFrame
        unPolledFrame = F.filterFrame ((`elem` unpolledKeys) . F.rcast @[BR.StateAbbreviation, BR.CongressionalDistrict]) moneyElexFrame
    K.liftKnit $ F.writeCSV "polled.csv" polledFrame    
    K.liftKnit $ F.writeCSV "unpolled.csv" unPolledFrame
    
    return moneyElexFrame
  -- congressional district analysis via MRP
  -- roll PUMS data up to CDs
  cd116FromPUMA2012_C <- BR.cd116FromPUMA2012Loader
  pumsDemographics_C <- PUMS.pumsLoader Nothing
  let pumsByCDDeps = (,) <$> cd116FromPUMA2012_C <*> pumsDemographics_C
--  K.clearIfPresent "house-data/pumsByCD2018.bin"
  pumsByCD2018ASER5_C <- BR.retrieveOrMakeFrame "house-data/pumsByCD2018.bin" pumsByCDDeps $ \(cdFromPUMA, pums) -> do
    let g r = (F.rgetField @BR.Year r == 2018)
              && (F.rgetField @DT.Age5FC r /= DT.A5F_Under18)
              && (F.rgetField @BR.StateFIPS r < 60) -- filter out non-states, except DC
    let pums2018WeightTotal =
          FL.fold (FL.prefilter g
                    $ FL.premap  (\r -> F.rgetField @PUMS.Citizens r + F.rgetField @PUMS.NonCitizens r) FL.sum) pums
    K.logLE K.Diagnostic $ "(Raw) Sum of all Citizens + NonCitizens in 2018=" <> (T.pack $ show pums2018WeightTotal)
    PUMS.pumsCDRollup g (PUMS.pumsKeysToASER5 True . F.rcast) cdFromPUMA pums
    
--  K.ignoreCacheTime  pumsByCD2018ASER5_C >>= BR.logFrame
  ccesMR_Prefs_C <- MRP.ccesPreferencesASER5_MRP
  censusBasedAdjEWs_C <- MRP.adjCensusElectoralWeightsMRP_ASER5
  -- join with Prefs and EWs and post-stratify
  let cdPostStratDeps = (,,) <$> pumsByCD2018ASER5_C <*> ccesMR_Prefs_C <*> censusBasedAdjEWs_C
  --K.clearIfPresent "house-data/cdPostStrat.bin"
  cdPostStrat_C <- BR.retrieveOrMakeFrame "house-data/cdPostStrat.bin" cdPostStratDeps
    $ \(cdASER5, ccesMR_Prefs, censusBasedAdjEWs) -> do
    let pums2018WeightTotal =
          FL.fold (FL.premap  (\r -> F.rgetField @PUMS.Citizens r + F.rgetField @PUMS.NonCitizens r) FL.sum) cdASER5
    K.logLE K.Diagnostic $ "(cdASER5) Sum of all Citizens + NonCitizens in 2018=" <> (T.pack $ show pums2018WeightTotal)          
    let prefs2018 = F.filterFrame (\r -> F.rgetField @BR.Year r == 2018) ccesMR_Prefs
        ew2018 = F.filterFrame (\r -> F.rgetField @BR.Year r == 2018) censusBasedAdjEWs
    cdWithPrefsAndEWs <- K.knitEither
                         $ either (Left . T.pack . show) Right
                         $ FJ.leftJoinE3 @('[BR.StateAbbreviation] V.++ DT.CatColsASER5) cdASER5 prefs2018 ew2018
    let postStratF = FMR.concatFold
                     $ FMR.mapReduceFold
                     FMR.noUnpack
                     (FMR.assignKeysAndData @[BR.StateAbbreviation, BR.CongressionalDistrict])
                     (FMR.foldAndAddKey postStratOneF)
        cdPostStrat = FL.fold postStratF cdWithPrefsAndEWs
        renderRec =
          FS.formatTextAsIs
          V.:& FS.formatWithShow
          V.:& FS.formatWithShow
          V.:& formatPct
          V.:& formatPct
          V.:& formatPct
          V.:& formatPct
          V.:& formatPct
          V.:& formatPct
          V.:& formatPct
          V.:& formatPct
          V.:& formatPct
          V.:& formatPct
          V.:& V.RNil
    K.liftKnit $ FS.writeLines "houseMRP.csv" $ FS.streamSV' renderRec "," $ Streamly.fromFoldable cdPostStrat
    return cdPostStrat    
--  K.ignoreCacheTime  cdPostStrat_C >>= BR.logFrame
--    return $ fmap (F.rcast @([BR.StateAbbreviation, BR.CongressionalDistrict] V.++ DT.CatColsASER5 V.++ [PUMS.Citizens, ET.DemPref, ET.ElectoralWeight])) cdWithPrefsAndEWs

  

  -- State Leg overlaps
  cdToStateLower_C <- cd116FromStateLower2016Loader
  cdToStateUpper_C <- cd116FromStateUpper2016Loader
  let sortedOverlapDeps = (,) <$> cdToStateLower_C <*> cdToStateUpper_C
--  K.clearIfPresent "house-data/sortedOverlap.bin"
  sortedOverlap_C <- BR.retrieveOrMakeFrame "house-data/sortedOverlap.bin" sortedOverlapDeps $ \(cdToLower, cdToUpper) -> do
    let fixedLower = fmap (F.rcast @OverlapRow . FT.transform fixLower) cdToLower
        fixedUpper = fmap (F.rcast @OverlapRow . FT.transform fixUpper) cdToUpper
        comp a b = compare (F.rgetField @BR.StateAbbreviation a) (F.rgetField @BR.StateAbbreviation b)
                   <> compare (F.rgetField @BR.CongressionalDistrict a) (F.rgetField @BR.CongressionalDistrict b)
                   <> compare (F.rgetField @StateOffice b) (F.rgetField @StateOffice a)
                   <> compare (F.rgetField @BR.FracCDFromSLD b) (F.rgetField @BR.FracCDFromSLD a)
        sortedOverlap = F.toFrame $ L.sortBy comp $ FL.fold FL.list $ fixedLower <> fixedUpper
        renderOverlapRec {-:: F.Rec (V.Lift (->) V.ElField (V.Const T.Text)) OverlapRow -} =
          FS.formatTextAsIs
          V.:& FS.formatWithShow 
          V.:& FS.formatWithShow 
          V.:& FS.formatWithShow 
          V.:& FS.liftFieldFormatter (T.pack . Printf.printf "%.2f" . (*100))
          V.:& FS.liftFieldFormatter (T.pack . Printf.printf "%.2f" . (*100))
          V.:& V.RNil
    K.liftKnit $ FS.writeLines "cdStateOverlaps.csv" $ FS.streamSV' renderOverlapRec "," $ Streamly.fromFoldable sortedOverlap
    return sortedOverlap
--  K.ignoreCacheTime sortedOverlap_C >>= BR.logFrame
  -- StateLeg demographics and post-stratification
  -- We'll do the PUMS rollup separately because it might take a while and will not change much

  pums2018ByPUMA_C <- BR.retrieveOrMakeFrame "house-data/pums2018ByPUMA.bin" pumsDemographics_C $ \pums -> do
    let g r = (F.rgetField @BR.Year r == 2018) && (F.rgetField @DT.Age5FC r /= DT.A5F_Under18)
        rolledUpToPUMA = FL.fold (PUMS.pumsRollupF g $ PUMS.pumsKeysToASER5 True . F.rcast) pums
        zeroCount :: F.Record [PUMS.Citizens, PUMS.NonCitizens]
        zeroCount = 0 F.&: 0 F.&: V.RNil
        addZeroCountsF =  FMR.concatFold $ FMR.mapReduceFold
                          (FMR.noUnpack)
                          (FMR.assignKeysAndData @[BR.StateAbbreviation, BR.PUMA])
                          ( FMR.makeRecsWithKey id
                            $ FMR.ReduceFold
                            $ const
                            $ BK.addDefaultRec @DT.CatColsASER5 zeroCount
                          )
    return $ F.toFrame $ FL.fold addZeroCountsF rolledUpToPUMA
    
  stateUpperFromPUMA_C <- stateUpperFromPUMA
--  K.ignoreCacheTime stateUpperFromPUMA_C >>= BR.logFrame
  stateLowerFromPUMA_C <- stateLowerFromPUMA

  let stateLegFromPUMADeps = (,,)
                              <$> stateUpperFromPUMA_C
                              <*> stateLowerFromPUMA_C
                              <*> pums2018ByPUMA_C
--                              <*> ccesMR_Prefs_C
--                              <*> censusBasedAdjEWs_C
  --K.clearIfPresent "house-data/stateLegPostStrat.bin" -- until it's working
  sldFromPUMA_ASER5_C <- BR.retrieveOrMakeFrame "house-data/sldFromPUMA_ASER5.bin" stateLegFromPUMADeps
                        $ \(upperFromPUMA, lowerFromPUMA, pumsDemographics) -> do
    let fixedUpper = fmap (F.rcast @SLDPumaRow . FT.transform fixUpper) upperFromPUMA
        fixedLower = fmap (F.rcast @SLDPumaRow . FT.transform fixLower) lowerFromPUMA
        sldFromPUMA = fixedLower <> fixedUpper
    sldFromPUMA_ASER5 <- K.knitEither
                        $ either (Left . T.pack . show) Right
                        $ FJ.leftJoinE @[BR.StateAbbreviation, BR.PUMA] sldFromPUMA pumsDemographics
    return sldFromPUMA_ASER5
{-  K.ignoreCacheTime sldFromPUMA_ASER5_C >>= BR.logFrame
  K.ignoreCacheTime sldFromPUMA_ASER5_C  >>= K.liftKnit
    . FS.writeCSV_Show "sldFromPUMS_ASER5.csv"
    . F.filterFrame (\r -> F.rgetField @BR.StateAbbreviation r == "PA"
                           && F.rgetField @StateOffice r == Lower
                           && F.rgetField @StateDistrict r == "63")
-}
  K.clearIfPresent "house-data/sldASER5" -- until it's working
  sldASER5_C <- BR.retrieveOrMakeFrame "house-data/sldASER5.bin" sldFromPUMA_ASER5_C $ \sldFromPUMA_ASER5 -> do
    let pumaWeightedPeopleF :: FL.Fold
                               (F.Record [BR.FracSLDFromPUMA, BR.FracPUMAFromSLD, PUMS.Citizens, PUMS.NonCitizens])
                               (F.Record [TotalFromPUMA, PUMS.Citizens, PUMS.NonCitizens])
        pumaWeightedPeopleF =
          let wgt = F.rgetField @BR.FracPUMAFromSLD
              cit = F.rgetField @PUMS.Citizens
              ncit = F.rgetField @PUMS.NonCitizens
              totalF = FL.premap (F.rgetField @BR.FracSLDFromPUMA) FL.sum
              wgtdCitF = fmap round $ FL.premap (\r -> wgt r * (realToFrac $ cit r)) FL.sum
              wgtdNCitF = fmap round $ FL.premap (\r -> wgt r * (realToFrac $ ncit r)) FL.sum
          in (\t c n -> t F.&: c F.&: n F.&: V.RNil) <$> totalF <*> wgtdCitF <*> wgtdNCitF
        pumasF = FMR.concatFold
                 $ FMR.mapReduceFold
                 (FMR.noUnpack)
                 (FMR.assignKeysAndData @([BR.StateAbbreviation, StateOffice, StateDistrict] V.++ DT.CatColsASER5))
                 (FMR.foldAndAddKey pumaWeightedPeopleF)
    K.logLE K.Info "pre PUMA fold"
    return $ FL.fold pumasF sldFromPUMA_ASER5

  let sldWithPrefAndWeightDeps = (,,) <$> sldASER5_C <*> ccesMR_Prefs_C <*> censusBasedAdjEWs_C 
  sldWithPrefAndWeight_C <- BR.retrieveOrMakeFrame "house-data/sldWithPrefAndWeight.bin" sldWithPrefAndWeightDeps
    $ \(sldASER5, ccesMR_Prefs, censusBasedAdjEWs) -> do
    let prefs2018 = F.filterFrame (\r -> F.rgetField @BR.Year r == 2018) ccesMR_Prefs
        ew2018 = F.filterFrame (\r -> F.rgetField @BR.Year r == 2018) censusBasedAdjEWs
    sldASER5wPrefs <- K.knitEither
      $ either (Left . T.pack . show) Right
      $ FJ.leftJoinE @('[BR.StateAbbreviation] V.++ DT.CatColsASER5) sldASER5 prefs2018
    sldASER5wPrefsEws <- K.knitEither
      $ either (Left . T.pack . show) Right
      $ FJ.leftJoinE @('[BR.StateAbbreviation] V.++ DT.CatColsASER5) sldASER5wPrefs ew2018
    return $ fmap (F.rcast @([BR.StateAbbreviation, StateOffice, StateDistrict] V.++ DT.CatColsASER5 V.++ [PUMS.Citizens, ET.DemPref, ET.ElectoralWeight])) sldASER5wPrefsEws
--  K.ignoreCacheTime sldWithPrefAndWeight_C >>= BR.logFrame
  --K.clearIfPresent "house-data/postStratifiedSLD.bin"
  postStratifiedSLD_C <- BR.retrieveOrMakeFrame "house-data/postStratifiedSLD.bin" sldWithPrefAndWeight_C $ \sldWithPrefAndWeight -> do
    let postStratF = FMR.concatFold
                     $ FMR.mapReduceFold
                     FMR.noUnpack
                     (FMR.assignKeysAndData @[BR.StateAbbreviation, StateOffice, StateDistrict])
                     (FMR.foldAndAddKey postStratOneF)
    return $ FL.fold postStratF sldWithPrefAndWeight
--  K.ignoreCacheTime postStratifiedSLD_C >>= BR.logFrame
  let keyDistricts :: [F.Record [BR.StateAbbreviation, BR.CongressionalDistrict]]
        = fmap (\(a, d) -> a F.&: d F.&: V.RNil) [("AZ", 6)
                                                 , ("GA", 7)
                                                 , ("MI", 6)
                                                 , ("OH", 1)
                                                 , ("OH", 12)
                                                 , ("PA", 10)
                                                 , ("TX", 10)
                                                 , ("TX", 24)
                                                 , ("TX", 31)]
      minSLDInCD = 0.5
      closeEnough = 0.1
  K.clearIfPresent "house-data/overlappingSLD.bin"
  let overlappingSLDDeps = (,) <$> sortedOverlap_C <*> postStratifiedSLD_C 
  overlappingSLDs_C <- BR.retrieveOrMakeFrame "house-data/overlappingSLD.bin" overlappingSLDDeps $ \(sldCDOverlaps, postStratifiedSLDs) -> do
    let enoughInKeyDistrict r = (F.rcast @[BR.StateAbbreviation, BR.CongressionalDistrict] r `elem` keyDistricts)
                                && (F.rgetField @BR.FracSLDFromCD r >= minSLDInCD)
        overlapsInKeyDs = fmap (F.rcast @[BR.StateAbbreviation, BR.CongressionalDistrict, StateOffice, StateDistrict, BR.FracSLDFromCD])
                          $ F.filterFrame enoughInKeyDistrict sldCDOverlaps
    overlapsPS <- fmap (F.filterFrame ((<= closeEnough) . abs . F.rgetField @DEdgePS))
                  <$> K.knitEither
                  $ either (Left . T.pack . show) Right
                  $ FJ.leftJoinE @[BR.StateAbbreviation, StateOffice, StateDistrict] overlapsInKeyDs postStratifiedSLDs   
    let renderRec =
          FS.formatTextAsIs
          V.:& FS.formatWithShow
          V.:& FS.formatWithShow
          V.:& FS.formatTextAsIs
          V.:& formatPct          
          V.:& FS.formatWithShow
          V.:& formatPct
          V.:& formatPct
          V.:& formatPct
          V.:& formatPct
          V.:& formatPct
          V.:& formatPct
          V.:& formatPct
          V.:& formatPct
          V.:& formatPct
          V.:& formatPct
          V.:& V.RNil
    K.liftKnit $ FS.writeLines "overlappingSLDs.csv" $ FS.streamSV' renderRec "," $ Streamly.fromFoldable overlapsPS
    return overlapsPS
--  K.ignoreCacheTime overlappingSLDs_C  >>= K.liftKnit . FS.writeCSV_Show "overlappingSLDs.csv"  
--  K.ignoreCacheTime overlappingSLDs >>= BR.logFrame
  overlappingSLDs <- K.ignoreCacheTime overlappingSLDs_C
  _ <- K.addHvega Nothing Nothing
    $ sldScatter
    "State Legislative Districts: Overlap vs. Edge"
    (FV.ViewConfig 800 800 10)
    (fmap F.rcast overlappingSLDs)
  return ()


sldScatter :: Foldable f
  => T.Text
  -> FV.ViewConfig
  -> f (F.Record [BR.StateAbbreviation, BR.CongressionalDistrict, StateOffice, StateDistrict, BR.FracSLDFromCD, DSharePS, DEdgePS])
  -> GV.VegaLite
sldScatter title vc rows =
  let dat = FV.recordsToVLData id FV.defaultParse rows      
      encX = GV.position GV.X [FV.pName @BR.FracSLDFromCD, GV.PmType GV.Quantitative]
      encY = GV.position GV.Y [FV.pName @DEdgePS, GV.PmType GV.Quantitative]
      encColor = GV.color [FV.mName @BR.StateAbbreviation, GV.MmType GV.Nominal]
      enc = (GV.encoding . encX . encY . encColor) []
      mark = GV.mark GV.Point [GV.MTooltip GV.TTData]
  in FV.configuredVegaLite vc [FV.title title, enc, mark, dat]

type OverlapRow = [BR.StateAbbreviation, BR.CongressionalDistrict, StateOffice, StateDistrict, BR.FracCDFromSLD, BR.FracSLDFromCD]
type SLDPumaRow = [BR.StateAbbreviation, StateOffice, StateDistrict, BR.PUMA, BR.FracSLDFromPUMA, BR.FracPUMAFromSLD]

type VAP = "VAP" F.:-> Int
type PctWhite = "PctWhite" F.:-> Double
type PctBlack = "PctBlack" F.:-> Double
type PctLatinx = "PctLatinx" F.:-> Double
type PctAsian = "PctAsian" F.:-> Double
type PctOther = "PctOther" F.:-> Double
type DPrefPS = "DPrefPS" F.:-> Double
type EWPS = "EWPS" F.:-> Double
type DSharePS = "DSharePS" F.:-> Double
type DEdgePS = "DEdgePS" F.:-> Double                
type DSharePS_EP = "DSharePS_EP" F.:-> Double
type DEdgePS_EP = "DEdgePS_EP" F.:-> Double                

formatPct :: (V.KnownField t, Printf.PrintfArg (V.Snd t), Num (V.Snd t)) => V.Lift (->) V.ElField (V.Const T.Text) t
formatPct = FS.liftFieldFormatter (T.pack . Printf.printf "%.2f" . (*100))

postStratOneF :: FL.Fold
                 (F.Record (DT.CatColsASER5 V.++ [PUMS.Citizens, ET.DemPref, ET.ElectoralWeight]))
                 (F.Record [VAP, PctWhite, PctBlack, PctLatinx, PctAsian, PctOther, DPrefPS, EWPS, DSharePS, DEdgePS, DEdgePS_EP])
postStratOneF =
  let cit = F.rgetField @PUMS.Citizens
      ew = F.rgetField @ET.ElectoralWeight
      dPref = F.rgetField @ET.DemPref
      citF = FL.premap cit FL.sum
      racePctF x = (/) <$> (fmap realToFrac $ FL.prefilter (\r -> F.rgetField @DT.Race5C r == x) citF) <*> (fmap realToFrac citF)
      citWgtdF g = (/) <$> (FL.premap (\r -> realToFrac (cit r) * g r) FL.sum) <*> (fmap realToFrac citF)
      wgtdDPrefF = citWgtdF dPref
      wgtdEWF = citWgtdF ew
      voteWgtd g = (/) <$> (FL.premap (\r ->  realToFrac (cit r) * ew r * g r) FL.sum) <*> (FL.premap (\r -> realToFrac (cit r) * ew r) FL.sum)
      wgtdDShareF = voteWgtd dPref --citWgtdF (\r -> F.rgetField @ET.DemPref r * F.rgetField @ET.ElectoralWeight r)
      wgtdDEdgeF = voteWgtd (\r -> (2 * dPref r) - 1)
      wgtdDEdge_EPF = citWgtdF (\r -> (2 * dPref r) - 1)
  in (\vap pw pb pl pa po prefPS ewPS sharePS edgePS edgePS_EP
       -> vap F.&: pw F.&: pb F.&: pl F.&: pa F.&: po F.&: prefPS F.&: ewPS F.&: sharePS F.&: edgePS F.&: edgePS_EP F.&: V.RNil)
     <$> citF
     <*> racePctF DT.R5_WhiteNonLatinx
     <*> racePctF DT.R5_Black
     <*> racePctF DT.R5_Latinx
     <*> racePctF DT.R5_Asian
     <*> racePctF DT.R5_Other
     <*> wgtdDPrefF
     <*> wgtdEWF
     <*> wgtdDShareF
     <*> wgtdDEdgeF
     <*> wgtdDEdge_EPF
             
type TotalFromPUMA = "TotalFromPUMA" F.:-> Double

data StateOfficeT = Upper | Lower deriving (Show, Eq, Ord, Generic)
type instance FI.VectorFor StateOfficeT = Vec.Vector
instance S.Serialize StateOfficeT
instance Grouping StateOfficeT

type StateOffice = "State Office" F.:-> StateOfficeT
type StateDistrict = "State District" F.:-> T.Text
instance FV.ToVLDataValue (F.ElField StateOffice) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Str $ T.pack $ show $ V.getField x)

fixLower :: F.Record '[BR.StateLower] -> F.Record '[StateOffice, StateDistrict]
fixLower r =
  let d = T.pack . show $ F.rgetField @BR.StateLower r
  in Lower F.&: d F.&: V.RNil

fixUpper :: F.Record '[BR.StateUpper] -> F.Record '[StateOffice, StateDistrict]
fixUpper r =
  let d = F.rgetField @BR.StateUpper r
  in Upper F.&: d F.&: V.RNil


type AllMoney = "All Money" F.:-> Double
      
type BestPollShare = "Best Poll Two-Party D Share" F.:-> Double
type BestPollDiff = "Best Poll D-R" F.:-> Double

pctVoteShareF :: FL.Fold (F.Record [ET.Party, BR.Pct]) (F.Record '[BestPollShare ,BestPollDiff])
pctVoteShareF =
  let
    party = F.rgetField @ET.Party
    pct = F.rgetField @BR.Pct
    demVotesF = FL.prefilter (\r -> party r == ET.Democratic) $ FL.premap pct FL.sum
    repVotesF = FL.prefilter (\r -> party r == ET.Republican) $ FL.premap pct FL.sum
--    demRepVotesF = FL.prefilter (\r -> let p = party r in (p == ET.Democratic || p == ET.Republican)) $ FL.premap pct FL.sum
    demShare d r = if d + r > 0 then d / (d + r) else 0
    demDiff d r = if (d + r) > 0 then (d - r) / (d + r) else 0
    demShareF = demShare <$> demVotesF <*> repVotesF
    demDiffF = demDiff <$> demVotesF <*> repVotesF
  in (\s d -> s F.&: d F.&: V.RNil) <$> demShareF <*> demDiffF

type Share2018 = "2018 Two-Party D Share" F.:-> Double
type Diff2018 = "2018 D-R" F.:-> Double

votesToVoteShareF :: FL.Fold (F.Record [ET.Party, ET.Votes]) (F.Record '[Share2018, Diff2018])
votesToVoteShareF =
  let
    party = F.rgetField @ET.Party
    votes = F.rgetField @ET.Votes
    demVotesF = FL.prefilter (\r -> party r == ET.Democratic) $ FL.premap votes FL.sum
    repVotesF = FL.prefilter (\r -> party r == ET.Republican) $ FL.premap votes FL.sum
    thirdPartyVotes = FL.prefilter (\r -> not $ party r `elem` [ET.Democratic, ET.Republican]) $ FL.premap votes FL.sum
--    demRepVotesF = FL.prefilter (\r -> let p = party r in (p == ET.Democratic || p == ET.Republican)) $ FL.premap votes FL.sum
    fixVotes r tp = if r == 0 then tp else r
    demShare d r tp = let ov = fixVotes r tp in if d + ov > 0 then realToFrac d / realToFrac (d + ov) else 0
    demDiff d r tp = let ov = fixVotes r tp in if d + ov > 0 then realToFrac (d - ov) / realToFrac (d + ov) else 0
    demShareF = demShare <$> demVotesF <*> repVotesF <*> thirdPartyVotes
    demDiffF = demDiff <$> demVotesF <*> repVotesF <*> thirdPartyVotes
  in (\s d -> s F.&: d F.&: V.RNil) <$> demShareF <*> demDiffF



cd116FromStateLower2016Loader :: (K.KnitOne r,  K.CacheEffectsD r) => K.Sem r (K.ActionWithCacheTime r (F.Frame BR.CD116FromStateLower2016))
cd116FromStateLower2016Loader =  BR.cachedFrameLoader (BR.DataSets $ T.pack BR.cd116FromStateLower2016CSV) Nothing Nothing id Nothing "cd116FromStateLower2016.bin"

cd116FromStateUpper2016Loader :: (K.KnitOne r,  K.CacheEffectsD r) => K.Sem r (K.ActionWithCacheTime r (F.Frame BR.CD116FromStateUpper2016))
cd116FromStateUpper2016Loader =  BR.cachedFrameLoader (BR.DataSets $ T.pack BR.cd116FromStateUpper2016CSV) Nothing Nothing id Nothing "cd116FromStateUpper2016.bin"

stateUpperFromPUMA :: (K.KnitOne r,  K.CacheEffectsD r) => K.Sem r (K.ActionWithCacheTime r (F.Frame BR.StateUpper2016FromPUMA))
stateUpperFromPUMA =  BR.cachedFrameLoader (BR.DataSets $ T.pack BR.stateUpper2016FromPUMACSV) Nothing Nothing id Nothing "stateUpper2016FromPUMA.bin"

stateLowerFromPUMA :: (K.KnitOne r,  K.CacheEffectsD r) => K.Sem r (K.ActionWithCacheTime r (F.Frame BR.StateLower2016FromPUMA))
stateLowerFromPUMA =  BR.cachedFrameLoader (BR.DataSets $ T.pack BR.stateLower2016FromPUMACSV) Nothing Nothing id Nothing "stateLower2016FromPUMA.bin"




type HousePollRow' = '[BR.State, BR.Cycle, BR.Pollster, BR.FteGrade, BR.CongressionalDistrict, BR.StartDate, BR.EndDate, BR.Internal, BR.CandidateName, BR.CandidateParty, BR.Pct]
type HousePollRow = '[BR.StateName, BR.Cycle, BR.Pollster, BR.FteGrade, BR.CongressionalDistrict, BR.StartDate, BR.EndDate, BR.Internal, BR.CandidateName, ET.Party, BR.Pct]

housePolls2020Loader :: (K.KnitOne r,  K.CacheEffectsD r) => K.Sem r (K.ActionWithCacheTime r (F.FrameRec HousePollRow))
housePolls2020Loader =  BR.cachedMaybeFrameLoader @(F.RecordColumns BR.HousePolls2020) @HousePollRow' @HousePollRow
                        (BR.DataSets $ T.pack BR.housePolls2020CSV)
                        Nothing
                        Nothing
                        id
                        fixHousePollRow
                        Nothing
                        "housePolls2020.bin"


parseHousePollParty :: T.Text -> ET.PartyT
parseHousePollParty t
  | T.isInfixOf "DEM" t = ET.Democratic
  | T.isInfixOf "REP" t = ET.Republican
  | otherwise = ET.Other

fixHousePollRow :: F.Record HousePollRow' -> F.Record HousePollRow
fixHousePollRow = F.rcast . addCols where
  addCols = FT.addName @BR.State @BR.StateName
            . FT.addOneFromOne @BR.CandidateParty @ET.Party parseHousePollParty


type FECRaw = [BR.CandidateId, BR.CandidateName, BR.StateAbbreviation, BR.CongressionalDistrict, BR.Party, BR.CashOnHand, BR.Disbursements, BR.Receipts, BR.IndSupport, BR.IndOppose, BR.PartyExpenditures]
type FECRow =  [BR.CandidateId,BR.CandidateName, BR.StateAbbreviation, BR.CongressionalDistrict, ET.Party, BR.CashOnHand, BR.Disbursements, BR.Receipts, BR.IndSupport, BR.IndOppose, BR.PartyExpenditures]


fec2020Loader ::  (K.KnitOne r,  K.CacheEffectsD r) => K.Sem r (K.ActionWithCacheTime r (F.FrameRec FECRow))
fec2020Loader = BR.cachedFrameLoader (BR.DataSets $ T.pack BR.allMoney2020CSV) Nothing Nothing fixFECRow Nothing "allMoney2020.bin"

parseFECParty :: T.Text -> ET.PartyT
parseFECParty t
  | T.isInfixOf "Democrat" t = ET.Democratic
  | T.isInfixOf "Republican" t = ET.Republican
  | otherwise = ET.Other


fixFECRow :: F.Record FECRaw -> F.Record FECRow
fixFECRow = F.rcast . addCols where
  addCols = FT.addOneFromOne @BR.Party @ET.Party parseFECParty

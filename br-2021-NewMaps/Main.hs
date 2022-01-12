{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE OverloadedLabels #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-# OPTIONS_GHC -O0 #-}

module Main where

import qualified BlueRipple.Configuration as BR
import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.ElectionTypes as ET
import qualified BlueRipple.Data.ModelingTypes as MT
import qualified BlueRipple.Data.ACS_PUMS as PUMS
--import qualified BlueRipple.Data.CCES as CCES
--import qualified BlueRipple.Data.CPSVoterPUMS as CPS
import qualified BlueRipple.Data.Loaders as BR
import qualified BlueRipple.Data.CensusTables as BRC
import qualified BlueRipple.Data.Loaders.Redistricting as Redistrict
import qualified BlueRipple.Data.Visualizations.DemoComparison as BRV
import qualified BlueRipple.Utilities.KnitUtils as BR
--import qualified BlueRipple.Utilities.Heidi as BR
import qualified BlueRipple.Utilities.TableUtils as BR
import qualified BlueRipple.Model.House.ElectionResult as BRE
import qualified BlueRipple.Data.CensusLoaders as BRC
import qualified BlueRipple.Model.StanMRP as MRP
--import qualified BlueRipple.Data.CountFolds as BRCF
--import qualified BlueRipple.Data.Keyed as BRK

import qualified Colonnade as C
import qualified Text.Blaze.Colonnade as C
import qualified Text.Blaze.Html5.Attributes   as BHA
import qualified Control.Foldl as FL
import qualified Control.Foldl.Statistics as FLS
import qualified Data.List as List
import qualified Data.IntMap as IM
import qualified Data.Map.Strict as M
import Data.String.Here (here, i)
import qualified Data.Set as Set
import qualified Data.Text as T
import qualified Text.Printf as T
import qualified Text.Read  as T
import qualified Data.Time.Calendar            as Time
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V
import qualified Data.Vector as Vector
import qualified System.Console.CmdArgs as CmdArgs
import qualified Flat
import qualified Frames as F
import qualified Frames.Melt as F
import qualified Frames.Streamly.InCore as FI
import qualified Frames.MapReduce as FMR
import qualified Frames.Aggregation as FA
import qualified Frames.Folds as FF
--import qualified Frames.Heidi as FH
import qualified Frames.SimpleJoins as FJ
import qualified Frames.Serialize as FS
import qualified Frames.Transform  as FT
import qualified Graphics.Vega.VegaLite as GV
import qualified Graphics.Vega.VegaLite.Compat as FV
import qualified Frames.Visualization.VegaLite.Data as FVD

import qualified Relude.Extra as Extra

import qualified Graphics.Vega.VegaLite.Configuration as FV
import qualified Graphics.Vega.VegaLite.Heidi as HV

import qualified Knit.Report as K
import qualified Knit.Effect.AtomicCache as KC
import qualified Text.Pandoc.Error as Pandoc
import qualified Numeric
import qualified Path
import Path (Rel, Abs, Dir, File)

import qualified Stan.ModelConfig as SC
import qualified Stan.ModelBuilder.BuildingBlocks as SB
import qualified Stan.ModelBuilder.SumToZero as SB
import qualified Stan.Parameters as SP
import qualified Stan.Parameters.Massiv as SPM
import qualified CmdStan as CS
import qualified Data.Vinyl.Core as V
import qualified Stan.ModelBuilder as SB

yamlAuthor :: T.Text
yamlAuthor =
  [here|
- name: Adam Conner-Sax
- name: Frank David
|]

templateVars :: M.Map String String
templateVars =
  M.fromList
    [ ("lang", "English"),
      ("site-title", "Blue Ripple Politics"),
      ("home-url", "https://www.blueripplepolitics.org")
      --  , ("author"   , T.unpack yamlAuthor)
    ]

pandocTemplate = K.FullySpecifiedTemplatePath "pandoc-templates/blueripple_basic.html"

main :: IO ()
main = do
  pandocWriterConfig <-
    K.mkPandocWriterConfig
      pandocTemplate
      templateVars
      K.mindocOptionsF
  let cacheDir = ".flat-kh-cache"
      knitConfig :: K.KnitConfig BR.SerializerC BR.CacheData Text =
        (K.defaultKnitConfig $ Just cacheDir)
          { K.outerLogPrefix = Just "2021-NewMaps"
          , K.logIf = K.logDiagnostic
          , K.pandocWriterConfig = pandocWriterConfig
          , K.serializeDict = BR.flatSerializeDict
          , K.persistCache = KC.persistStrictByteString (\t -> toString (cacheDir <> "/" <> t))
          }
  cmdLine <- CmdArgs.cmdArgs BR.commandLine
  let stanParallelCfg = BR.clStanParallel cmdLine
      parallel =  case BR.cores stanParallelCfg of
        BR.MaxCores -> True
        BR.FixedCores n -> n > BR.parallelChains stanParallelCfg
  resE <- K.knitHtmls knitConfig $ do
    K.logLE K.Info $ "Command Line: " <> show cmdLine
    newMapAnalysis stanParallelCfg parallel

  case resE of
    Right namedDocs ->
      K.writeAllPandocResultsWithInfoAsHtml "" namedDocs
    Left err -> putTextLn $ "Pandoc Error: " <> Pandoc.renderError err


postDir = [Path.reldir|br-2021-NewMaps/posts|]
postInputs p = postDir BR.</> p BR.</> [Path.reldir|inputs|]
postLocalDraft p = postDir BR.</> p BR.</> [Path.reldir|draft|]
postOnline p =  [Path.reldir|research/NewMaps|] BR.</> p

postPaths :: (K.KnitEffects r, MonadIO (K.Sem r))
          => Text
          -> K.Sem r (BR.PostPaths BR.Abs)
postPaths t = do
  postSpecificP <- K.knitEither $ first show $ Path.parseRelDir $ toString t
  BR.postPaths
    BR.defaultLocalRoot
    (postInputs postSpecificP)
    (postLocalDraft postSpecificP)
    (postOnline postSpecificP)

-- data
type CCESVoted = "CCESVoters" F.:-> Int
type CCESHouseVotes = "CCESHouseVotes" F.:-> Int
type CCESHouseDVotes = "CCESHouseDVotes" F.:-> Int

type PredictorR = [DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.Race5C, DT.HispC]

type CDDemographicsR = '[BR.StateAbbreviation] V.++ BRC.CensusRecodedR V.++ '[DT.Race5C]
type CDLocWStAbbrR = '[BR.StateAbbreviation, ET.DistrictTypeC, ET.DistrictNumber] -- V.++ BRC.LDLocationR

filterCcesAndPumsByYear :: (Int -> Bool) -> BRE.CCESAndPUMS -> BRE.CCESAndPUMS
filterCcesAndPumsByYear f (BRE.CCESAndPUMS cces cps pums dd) = BRE.CCESAndPUMS (q cces) (q cps) (q pums) (q dd) where
  q :: (F.ElemOf rs BR.Year, FI.RecVec rs) => F.FrameRec rs -> F.FrameRec rs
  q = F.filterFrame (f . F.rgetField @BR.Year)

aggregatePredictorsCDFld fldData = FMR.concatFold
                                   $ FMR.mapReduceFold
                                   FMR.noUnpack
                                   (FMR.assignKeysAndData @[BR.Year, DT.StateAbbreviation, ET.CongressionalDistrict])
                                   (FMR.foldAndAddKey fldData)

aggregatePredictorsCountyFld fldData = FMR.concatFold
                                       $ FMR.mapReduceFold
                                       FMR.noUnpack
                                       (FMR.assignKeysAndData @[BR.Year, DT.StateAbbreviation, BR.CountyFIPS])
                                       (FMR.foldAndAddKey fldData)

debugCES :: K.KnitEffects r => F.FrameRec BRE.CCESByCDR -> K.Sem r ()
debugCES ces = do
  let aggFld :: FL.Fold (F.Record BRE.CCESVotingDataR) (F.Record BRE.CCESVotingDataR)
      aggFld = FF.foldAllConstrained @Num FL.sum
      genderFld = FMR.concatFold
                  $ FMR.mapReduceFold
                  FMR.noUnpack
                  (FMR.assignKeysAndData @[BR.Year, DT.SexC])
                  (FMR.foldAndAddKey aggFld)
      cesByYearAndGender = FL.fold genderFld ces
  BR.logFrame cesByYearAndGender

debugPUMS :: K.KnitEffects r => F.FrameRec BRE.PUMSByCDR -> K.Sem r ()
debugPUMS pums = do
  let aggFld :: FL.Fold (F.Record '[PUMS.Citizens, PUMS.NonCitizens]) (F.Record '[PUMS.Citizens, PUMS.NonCitizens])
      aggFld = FF.foldAllConstrained @Num FL.sum
      raceFld = FMR.concatFold
                  $ FMR.mapReduceFold
                  FMR.noUnpack
                  (FMR.assignKeysAndData @[BR.Year, DT.RaceAlone4C, DT.HispC])
                  (FMR.foldAndAddKey aggFld)
      pumsByYearAndRace = FL.fold raceFld pums
  BR.logFrame pumsByYearAndRace

{-
showVACPS :: (K.KnitEffects r, BR.CacheEffects r) => F.FrameRec BRE.CPSVByCDR -> K.Sem r ()
showVACPS cps = do
  let cps2020VA = F.filterFrame (\r -> F.rgetField @BR.Year r == 2020 && F.rgetField @BR.StateAbbreviation r == "VA") cps
      nVA = FL.fold (FL.premap (F.rgetField @BRCF.Count) FL.sum) cps2020VA
      nVoted = FL.fold (FL.premap (F.rgetField @BRCF.Successes) FL.sum) cps2020VA
  K.logLE K.Info $ "CPS VA: " <> show nVA <> " rows and " <> show nVoted <> " voters."
  let aggFld :: FL.Fold (F.Record [BRCF.Count, BRCF.Successes]) (F.Record [BRCF.Count, BRCF.Successes])
      aggFld = FF.foldAllConstrained @Num FL.sum
      aggregated = FL.fold (aggregatePredictorsCDFld aggFld) cps2020VA
  BR.logFrame aggregated
  cpsRaw <- K.ignoreCacheTimeM CPS.cpsVoterPUMSLoader
  let cpsRaw2020VA = F.filterFrame (\r -> F.rgetField @BR.Year r == 2020 && F.rgetField @BR.StateAbbreviation r == "VA") cpsRaw
      aFld :: FL.Fold (F.Record '[CPS.CPSVoterPUMSWeight]) (F.Record '[CPS.CPSVoterPUMSWeight])
      aFld = FF.foldAllConstrained @Num FL.sum
      aggregatedRaw = FL.fold (aggregatePredictorsCountyFld aFld) cpsRaw
  BR.logFrame aggregatedRaw
-}


prepCensusDistrictData :: (K.KnitEffects r, BR.CacheEffects r)
                   => Bool
                   -> Text
                   -> K.ActionWithCacheTime r BRC.LoadedCensusTablesByLD
                   -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec CDDemographicsR))
prepCensusDistrictData clearCaches cacheKey cdData_C = do
  stateAbbreviations <-  BR.stateAbbrCrosswalkLoader
  let deps = (,) <$> cdData_C <*> stateAbbreviations
  when clearCaches $ BR.clearIfPresentD cacheKey
  BR.retrieveOrMakeFrame cacheKey deps $ \(cdData, stateAbbrs) -> do
    let addRace5 = FT.mutate (\r -> FT.recordSingleton @DT.Race5C
                                    $ DT.race5FromRaceAlone4AndHisp True (F.rgetField @DT.RaceAlone4C r) (F.rgetField @DT.HispC r))
        cdDataSER' = BRC.censusDemographicsRecode $ BRC.sexEducationRace cdData
        (cdDataSER, cdMissing) =  FJ.leftJoinWithMissing @'[BR.StateFips] cdDataSER'
                                  $ fmap (F.rcast @[BR.StateFips, BR.StateAbbreviation] . FT.retypeColumn @BR.StateFIPS @BR.StateFips) stateAbbrs
    when (not $ null cdMissing) $ K.knitError $ "state FIPS missing in proposed district demographics/stateAbbreviation join."
    return $ (F.rcast . addRace5 <$> cdDataSER)


newMapAnalysis :: forall r. (K.KnitMany r, BR.CacheEffects r) => BR.StanParallel -> Bool -> K.Sem r ()
newMapAnalysis stanParallelCfg parallel = do
  ccesAndPums_C <-  BRE.prepCCESAndPums False
  proposedCDs_C <- prepCensusDistrictData False "model/newMaps/newCDDemographicsDR.bin" =<< BRC.censusTablesForProposedCDs
  drExtantCDs_C <- prepCensusDistrictData False "model/newMaps/extantCDDemographicsDR.bin" =<< BRC.censusTablesForDRACDs
  let addRace5 r = r F.<+> (FT.recordSingleton @DT.Race5C $ DT.race5FromRaceAlone4AndHisp True (F.rgetField @DT.RaceAlone4C r) (F.rgetField @DT.HispC r))
      addCount r = r F.<+> (FT.recordSingleton @BRC.Count $ F.rgetField @PUMS.Citizens r)
      addDistrict r = r F.<+> ((ET.Congressional F.&: F.rgetField @ET.CongressionalDistrict r F.&: V.RNil) :: F.Record [ET.DistrictTypeC, ET.DistrictNumber])
      fixPums :: F.Record BRE.PUMSByCDR -> F.Record PostStratR
      fixPums = F.rcast . addRace5 . addDistrict . addCount
      onlyState :: (F.ElemOf xs BR.StateAbbreviation, FI.RecVec xs) => Text -> F.FrameRec xs -> F.FrameRec xs
      onlyState x = F.filterFrame ((== x) . F.rgetField @BR.StateAbbreviation)
{-
  pumsTX <- K.ignoreCacheTime $ fmap (onlyState "TX" . BRE.pumsRows) ccesAndPums_C
  debugPUMS pumsTX
  K.knitError "STOP"
-}
  let postInfoNC = BR.PostInfo BR.LocalDraft (BR.PubTimes (BR.Published $ Time.fromGregorian 2021 12 15) (Just BR.Unpublished))
  ncPaths <-  postPaths "NC_Congressional"
  BR.brNewPost ncPaths postInfoNC "NC" $ do
    ncNMPS <- NewMapPostSpec "NC" ncPaths
              <$> (K.ignoreCacheTimeM $ Redistrict.loadRedistrictingPlanAnalysis (Redistrict.redistrictingPlanId "NC" "CST-13" ET.Congressional))
    newMapsTest False stanParallelCfg parallel ncNMPS postInfoNC (K.liftActionWithCacheTime ccesAndPums_C)
      (K.liftActionWithCacheTime $ fmap (fmap F.rcast . onlyState "NC") drExtantCDs_C)
      (K.liftActionWithCacheTime $ fmap (fmap F.rcast . onlyState "NC") proposedCDs_C)

  let postInfoTX = BR.PostInfo BR.LocalDraft (BR.PubTimes BR.Unpublished Nothing)
  txPaths <- postPaths "TX_Congressional"
  BR.brNewPost txPaths postInfoTX "TX" $ do
    txNMPS <- NewMapPostSpec "TX" txPaths
            <$> (K.ignoreCacheTimeM $ Redistrict.loadRedistrictingPlanAnalysis (Redistrict.redistrictingPlanId "TX" "Passed" ET.Congressional))
    newMapsTest False stanParallelCfg parallel txNMPS postInfoTX (K.liftActionWithCacheTime ccesAndPums_C)
      (K.liftActionWithCacheTime $ fmap (fmap fixPums . onlyState "TX" . BRE.pumsRows) ccesAndPums_C)
      (K.liftActionWithCacheTime $ fmap (fmap F.rcast . onlyState "TX") proposedCDs_C)


districtColonnade cas =
  let state = F.rgetField @DT.StateAbbreviation
      dNum = F.rgetField @ET.DistrictNumber
      share5 = MT.ciLower . F.rgetField @BRE.ModeledShare
      share50 = MT.ciMid . F.rgetField @BRE.ModeledShare
      share95 = MT.ciUpper . F.rgetField @BRE.ModeledShare
  in C.headed "State" (BR.toCell cas "State" "State" (BR.textToStyledHtml . state))
     <> C.headed "District" (BR.toCell cas "District" "District" (BR.numberToStyledHtml "%d" . dNum))
--     <> C.headed "2019 Result" (BR.toCell cas "2019" "2019" (BR.numberToStyledHtml "%2.2f" . (100*) . F.rgetField @BR.DShare))
     <> C.headed "5%" (BR.toCell cas "5%" "5%" (BR.numberToStyledHtml "%2.2f" . (100*) . share5))
     <> C.headed "50%" (BR.toCell cas "50%" "50%" (BR.numberToStyledHtml "%2.2f" . (100*) . share50))
     <> C.headed "95%" (BR.toCell cas "95%" "95%" (BR.numberToStyledHtml "%2.2f" . (100*) . share95))

modelCompColonnade states cas =
  C.headed "Model" (BR.toCell cas "Model" "Model" (BR.textToStyledHtml . fst))
  <> mconcat (fmap (\s -> C.headed (BR.textToCell s) (BR.toCell cas s s (BR.maybeNumberToStyledHtml "%2.2f" . M.lookup s . snd))) states)



type ModelPredictorR = [DT.SexC, DT.CollegeGradC, DT.Race5C, DT.HispC, DT.PopPerSqMile]
type PostStratR = [BR.StateAbbreviation, ET.DistrictTypeC, ET.DistrictNumber] V.++ ModelPredictorR V.++ '[BRC.Count]
type ElexDShare = "ElexDShare" F.:-> Double
type TwoPartyDShare = "2-Party DShare" F.:-> Double

twoPartyDShare r =
  let ds = F.rgetField @ET.DemShare r
      rs = F.rgetField @ET.RepShare r
  in FT.recordSingleton @TwoPartyDShare $ ds/(ds + rs)

addTwoPartyDShare r = r F.<+> twoPartyDShare r

--data ExtantDistricts = PUMSDistricts | DRADistricts

data NewMapPostSpec = NewMapPostSpec Text (BR.PostPaths BR.Abs) (F.Frame Redistrict.DRAnalysis)

newMapsTest :: forall r.(K.KnitMany r, K.KnitOne r, BR.CacheEffects r)
            => Bool
            -> BR.StanParallel
            -> Bool
            -> NewMapPostSpec
            -> BR.PostInfo
            -> K.ActionWithCacheTime r BRE.CCESAndPUMS
            -> K.ActionWithCacheTime r (F.FrameRec PostStratR) -- extant districts
            -> K.ActionWithCacheTime r (F.FrameRec PostStratR) -- new districts
            -> K.Sem r ()
newMapsTest clearCaches stanParallelCfg parallel postSpec postInfo ccesAndPums_C extantDemo_C proposedDemo_C = K.wrapPrefix "newMapsTest" $ do
  let (NewMapPostSpec stateAbbr postPaths drAnalysis) = postSpec
  K.logLE K.Info $ "Re-building NewMaps " <> stateAbbr <> " post"
  let ccesAndPums2018_C = fmap (filterCcesAndPumsByYear (==2018)) ccesAndPums_C
      ccesAndPums2020_C = fmap (filterCcesAndPumsByYear (==2020)) ccesAndPums_C
      addDistrict r = r F.<+> ((ET.Congressional F.&: F.rgetField @ET.CongressionalDistrict r F.&: V.RNil) :: F.Record [ET.DistrictTypeC, ET.DistrictNumber])
      onlyState :: (F.ElemOf xs BR.StateAbbreviation, FI.RecVec xs) => F.FrameRec xs -> F.FrameRec xs
      onlyState = F.filterFrame ((== stateAbbr) . F.rgetField @BR.StateAbbreviation)
      addElexDShare r = let dv = F.rgetField @BRE.DVotes r
                            rv = F.rgetField @BRE.RVotes r
                        in r F.<+> (FT.recordSingleton @ElexDShare $ if (dv + rv) == 0 then 0 else (realToFrac dv/realToFrac (dv + rv)))

  let psGroupSet = SB.addGroupToSet BRE.sexGroup
                   $ SB.addGroupToSet BRE.educationGroup
                   $ SB.addGroupToSet BRE.raceGroup
                   $ SB.addGroupToSet BRE.stateGroup
                   $ SB.emptyGroupSet
      modelDir =  "br-2021-NewMaps/stanGQ"
      mapGroup :: SB.GroupTypeTag (F.Record CDLocWStAbbrR) = SB.GroupTypeTag "CD"
      psInfo name model = (mapGroup, name <> "_" <> (BRE.printDensityTransform $ BRE.densityTransform model), psGroupSet)
      model2020 :: BRE.Model
                -> Text
                -> K.ActionWithCacheTime r (F.FrameRec PostStratR)
                -> K.Sem r (F.FrameRec (BRE.ModelResultsR CDLocWStAbbrR))
      model2020 m name
        =  K.ignoreCacheTimeM . BRE.electionModel False parallel stanParallelCfg modelDir m 2020 (psInfo name m) ccesAndPums2020_C
  let baseLog = BRE.Model BRE.HouseVS BRE.BaseG BRE.LogDensity BRE.BaseD
      baseQuantile n = BRE.Model BRE.HouseVS BRE.BaseG (BRE.QuantileDensity n) BRE.BaseD
      stateRaceQuantile n = BRE.Model BRE.HouseVS BRE.PlusStateRaceG (BRE.QuantileDensity n) BRE.BaseD
      partiallyPooledQuantile n = BRE.Model BRE.HouseVS BRE.PartiallyPooledStateG (BRE.QuantileDensity n) BRE.BaseD
      partiallyPooledLog = BRE.Model BRE.HouseVS BRE.PartiallyPooledStateG BRE.LogDensity BRE.BaseD
      partiallyPooledDLog = BRE.Model BRE.HouseVS BRE.PartiallyPooledStateG BRE.LogDensity BRE.PlusNCHStateD
  extantBaseHV <- model2020 partiallyPooledLog (stateAbbr <> "_Extant") $ (fmap F.rcast <$> extantDemo_C)
  proposedBaseHV <- model2020 partiallyPooledLog (stateAbbr <> "_Proposed") $ (fmap F.rcast <$> proposedDemo_C)
{-
  extantPlusStateHV
    <- model2020 (BRE.Model BRE.HouseVS BRE.PlusStateG BRE.BaseD) (stateAbbr <> "_Extant") $ (fmap F.rcast <$> extantDemo_C)
  proposedPlusStateHV
    <- model2020 (BRE.Model BRE.HouseVS BRE.PlusStateG BRE.BaseD) (stateAbbr <> "_Proposed") $ (fmap F.rcast <$> proposedDemo_C)
-}
{-  extantBasePV <- model2020 (BRE.Model BRE.PresVS BRE.BaseG BRE.BaseD) (stateAbbr <> "_Extant") $ (fmap F.rcast <$> extantDemo_C)
  proposedBasePV <- model2020 (BRE.Model BRE.PresVS BRE.BaseG BRE.BaseD) (stateAbbr <> "_Proposed") $ (fmap F.rcast <$> proposedDemo_C)
  extantPlusInteractionsGHV <- model2020 (BRE.Model BRE.HouseVS BRE.PlusInteractionsG BRE.BaseD) (stateAbbr <> "_Extant") $ (fmap F.rcast <$> extantDemo_C)
  proposedPlusInteractionsGHV <- model2020 (BRE.Model BRE.HouseVS BRE.PlusInteractionsG BRE.BaseD) (stateAbbr <> "_Proposed") $ (fmap F.rcast <$> proposedDemo_C)
  extantPlusInteractionsGDHV <- model2020 (BRE.Model BRE.HouseVS BRE.PlusInteractionsG BRE.PlusInteractionsD) (stateAbbr <> "_Extant") $ (fmap F.rcast <$> extantDemo_C)
  proposedPlusInteractionsGDHV <- model2020 (BRE.Model BRE.HouseVS BRE.PlusInteractionsG BRE.PlusInteractionsD) (stateAbbr <> "_Proposed") $ (fmap F.rcast <$> proposedDemo_C)
-}

--  extantBaseCV <- model2020 (BRE.Model BRE.CompositeVS BRE.BaseG BRE.BaseD) (stateAbbr <> "_Extant") $ (fmap F.rcast <$> extantDemo_C)
--  proposedBaseCV <- model2020 (BRE.Model BRE.CompositeVS BRE.BaseG BRE.BaseD) (stateAbbr <> "_Proposed") $ (fmap F.rcast <$> proposedDemo_C)
{-  extantPlusStateAndStateRace_RaceDensityNC
   <- model2020 (BRE.Model BRE.HouseVS BRE.PlusStateG BRE.PlusNCHRaceD) (stateAbbr <> "_Extant") $ (fmap F.rcast <$> extantDemo_C)
  proposedPlusStateAndStateRace_RaceDensityNC
   <- model2020 (BRE.Model BRE.HouseVS BRE.PlusStateG BRE.PlusNCHRaceD) (stateAbbr <> "_Proposed") $ (fmap F.rcast <$> proposedDemo_C)
-}
{-  extantPlusStateAndStateRace_RaceDensityNC
   <- model2020 (BRE.Model BRE.HouseVS BRE.PlusStateAndStateRaceG BRE.PlusNCHRaceD) (stateAbbr <> "_Extant") $ (fmap F.rcast <$> extantDemo_C)
  proposedPlusStateAndStateRace_RaceDensityNC
   <- model2020 (BRE.Model BRE.HouseVS BRE.PlusStateAndStateRaceG BRE.PlusNCHRaceD) (stateAbbr <> "_Proposed") $ (fmap F.rcast <$> proposedDemo_C)
-}
  let extantForPost = extantBaseHV
      proposedForPost = proposedBaseHV
  elections_C <- BR.houseElectionsWithIncumbency
  elections <- fmap onlyState $ K.ignoreCacheTime elections_C
  flattenedElections <- fmap (addDistrict . addElexDShare) . F.filterFrame ((==2020) . F.rgetField @BR.Year)
                        <$> (K.knitEither $ FL.foldM (BRE.electionF @[BR.Year, BR.StateAbbreviation, BR.CongressionalDistrict]) $ F.rcast <$> elections)
  let textDist r = let x = F.rgetField @ET.DistrictNumber r in if x < 10 then "0" <> show x else show x
      distLabel r = F.rgetField @DT.StateAbbreviation r <> "-" <> textDist r
      raceSort = Just $ show <$> [DT.R5_WhiteNonHispanic, DT.R5_Black, DT.R5_Hispanic, DT.R5_Asian, DT.R5_Other]
      eduSort = Just $ show <$> [DT.NonGrad, DT.Grad]
      modelShareSort = reverse . fmap fst . sortOn snd
                       . fmap (\r -> (distLabel r, MT.ciMid $ F.rgetField @BRE.ModeledShare r))
                       . FL.fold FL.list
      safeLog x = if x < 1e-12 then 0 else Numeric.log x
      xyFold' = FMR.mapReduceFold
                FMR.noUnpack
                (FMR.assignKeysAndData @[DT.StateAbbreviation, ET.DistrictNumber] @[BRC.Count, DT.Race5C, DT.CollegeGradC, DT.PopPerSqMile, BRE.ModeledShare])
                (FMR.foldAndLabel foldData (\k (x :: Double, y :: Double, c, s) -> (distLabel k, x, y, c, s)))
        where
          allF = FL.premap (F.rgetField @BRC.Count) FL.sum
          wnhF = FL.prefilter ((/= DT.R5_WhiteNonHispanic) . F.rgetField @DT.Race5C) allF
          gradsF = FL.prefilter ((== DT.Grad) . F.rgetField @DT.CollegeGradC) allF
          densityF = fmap (fromMaybe 0) $ FL.premap (safeLog . F.rgetField @DT.PopPerSqMile) FL.last
          modelF = fmap (fromMaybe 0) $ FL.premap (MT.ciMid . F.rgetField @BRE.ModeledShare) FL.last
          foldData = (\a wnh grads m d -> (100 * realToFrac wnh/ realToFrac a, 100 * realToFrac grads/realToFrac a, 100*(m - 0.5), d))
                     <$> allF <*> wnhF <*> gradsF <*> modelF <*> densityF
      oldDistrictsNoteName = BR.Used "Old_Districts"
  extantDemo <- K.ignoreCacheTime extantDemo_C
  mOldDistrictsUrl <- BR.brNewNote postPaths postInfo oldDistrictsNoteName (stateAbbr <> ": Old Districts") $ do
    BR.brAddNoteMarkDownFromFile postPaths oldDistrictsNoteName "_intro"
    let extantByModelShare = modelShareSort extantBaseHV --extantPlusStateAndStateRace_RaceDensityNC
    _ <- K.addHvega Nothing Nothing
         $ BRV.demoCompare
         ("Race", show . F.rgetField @DT.Race5C, raceSort)
         ("Education", show . F.rgetField @DT.CollegeGradC, eduSort)
         (F.rgetField @BRC.Count)
         ("District", \r -> F.rgetField @DT.StateAbbreviation r <> "-" <> textDist r, Just extantByModelShare)
         (Just ("log(Density)", Numeric.log . F.rgetField @DT.PopPerSqMile))
         (stateAbbr <> " Old: By Race and Education")
         (FV.ViewConfig 600 600 5)
         extantDemo
    BR.brAddNoteMarkDownFromFile postPaths oldDistrictsNoteName "_afterDemographicsBar"
    let (demoElexModelExtant, missing1E, missing2E)
          = FJ.leftJoin3WithMissing @[DT.StateAbbreviation, ET.DistrictTypeC, ET.DistrictNumber]
            (onlyState extantDemo)
            flattenedElections
            extantBaseHV
--            extantPlusStateAndStateRace_RaceDensityNC
    when (not $ null missing1E) $ do
      BR.logFrame extantDemo
      K.knitError $ "Missing keys in join of extant demographics and election results:" <> show missing1E
    when (not $ null missing2E) $ K.knitError $ "Missing keys in join of extant demographics and model:" <> show missing2E
    _ <- K.addHvega Nothing Nothing
      $ BRV.demoCompareXYCS
      "District"
      "% non-white"
      "% college grad"
      "Modeled D-Edge"
      "log density"
      (stateAbbr <> " demographic scatter")
      (FV.ViewConfig 600 600 5)
      (FL.fold xyFold' demoElexModelExtant)
    BR.brAddNoteMarkDownFromFile postPaths oldDistrictsNoteName "_afterDemographicsScatter"

    let (oldMapsCompare, missing)
          = FJ.leftJoinWithMissing @[BR.Year, DT.StateAbbreviation, ET.DistrictTypeC, ET.DistrictNumber]
            flattenedElections
            extantForPost
    when (not $ null missing) $ K.knitError $ "Missing keys in join of election results and model:" <> show missing
    _ <- K.addHvega Nothing Nothing
         $ modelAndElectionScatter
         True
         (stateAbbr <> " 2020: Election vs Demographic Model")
         (FV.ViewConfig 600 600 5)
         (fmap F.rcast oldMapsCompare)
    BR.brAddNoteMarkDownFromFile postPaths oldDistrictsNoteName "_afterModelElection"
  oldDistrictsNoteUrl <- K.knitMaybe "extant districts Note Url is Nothing" $ mOldDistrictsUrl
  let oldDistrictsNoteRef = "[oldDistricts]:" <> oldDistrictsNoteUrl
  BR.brAddPostMarkDownFromFile postPaths "_intro"
  let (modelAndDR, missing)
        = FJ.leftJoinWithMissing @[DT.StateAbbreviation, ET.DistrictTypeC, ET.DistrictNumber]
          proposedForPost
--          proposedPlusStateAndStateRace_RaceDensityNC
          (fmap addTwoPartyDShare drAnalysis)
  _ <- K.addHvega Nothing Nothing
       $ modelAndDaveScatterChart
       True
       (stateAbbr <> " 2022: Historical vs. Demographic models")
       (FV.ViewConfig 600 600 5)
       (fmap F.rcast modelAndDR)
  BR.brAddPostMarkDownFromFile postPaths "_afterDaveModel"
  let sortedModelAndDRA = reverse $ sortOn (MT.ciMid . F.rgetField @BRE.ModeledShare) $ FL.fold FL.list modelAndDR
      longShot ci = MT.ciUpper ci < 0.48
      leanR ci = MT.ciMid ci < 0.5 && MT.ciUpper ci >= 0.48
      leanD ci = MT.ciMid ci >= 0.5 && MT.ciLower ci <= 0.52
      safeD ci = MT.ciLower ci > 0.52
      mi = F.rgetField @BRE.ModeledShare
      bordered c = "border: 3px solid " <> c
      longShotCS  = bordered "red" `BR.cellStyleIf` \r h -> longShot (mi r) && h == "Demographic"
      leanRCS =  bordered "pink" `BR.cellStyleIf` \r h -> leanR (mi r) && h `elem` ["Demographic"]
      leanDCS = bordered "skyblue" `BR.cellStyleIf` \r h -> leanD (mi r) && h `elem` ["Demographic"]
      safeDCS = bordered "blue"  `BR.cellStyleIf` \r h -> safeD (mi r) && h == "Demographic"
      dra = F.rgetField @TwoPartyDShare
      longShotDRACS = bordered "red" `BR.cellStyleIf` \r h -> (dra r < 0.45) && h == "Historical"
      leanRDRACS = bordered "pink" `BR.cellStyleIf` \r h -> (dra r >= 0.45 && dra r < 0.50) && h == "Historical"
      leanDDRACS = bordered "skyblue" `BR.cellStyleIf` \r h -> (dra r >= 0.5 && dra r < 0.55) && h == "Historical"
      safeDDRACS = bordered "blue" `BR.cellStyleIf` \r h -> (dra r > 0.55) && h == "Historical"
      tableCellStyle = mconcat [longShotCS, leanRCS, leanDCS, safeDCS, longShotDRACS, leanRDRACS, leanDDRACS, safeDDRACS]
  BR.brAddRawHtmlTable
    ("Calculated Dem Vote Share, " <> stateAbbr <> " 2022: Demographic Model vs. Historical Model (DR)")
    (BHA.class_ "brTable")
    (daveModelColonnade tableCellStyle)
    sortedModelAndDRA
  BR.brAddPostMarkDownFromFile postPaths "_daveModelTable"
  BR.brAddPostMarkDownFromFile postPaths "_beforeNewDemographics"
  let proposedByModelShare = modelShareSort proposedBaseHV --proposedPlusStateAndStateRace_RaceDensityNC
  proposedDemo <- K.ignoreCacheTime proposedDemo_C
  _ <- K.addHvega Nothing Nothing
       $ BRV.demoCompare
       ("Race", show . F.rgetField @DT.Race5C, raceSort)
       ("Education", show . F.rgetField @DT.CollegeGradC, eduSort)
       (F.rgetField @BRC.Count)
       ("District", \r -> F.rgetField @DT.StateAbbreviation r <> "-" <> textDist r, Just proposedByModelShare)
       (Just ("log(Density)", (\x -> x) . Numeric.log . F.rgetField @DT.PopPerSqMile))
       (stateAbbr <> " New: By Race and Education")
       (FV.ViewConfig 600 600 5)
       proposedDemo
  let (demoModelAndDR, missing1P, missing2P)
        = FJ.leftJoin3WithMissing @[DT.StateAbbreviation, ET.DistrictTypeC, ET.DistrictNumber]
          (onlyState proposedDemo)
          proposedForPost
--          proposedPlusStateAndStateRace_RaceDensityNC
          (fmap addTwoPartyDShare drAnalysis)
  when (not $ null missing1P) $ K.knitError $ "Missing keys when joining demographics results and model: " <> show missing1P
  when (not $ null missing2P) $ K.knitError $ "Missing keys when joining demographics results and Dave's redistricting analysis: " <> show missing2P
--  BR.brAddPostMarkDownFromFile postPaths "_afterNewDemographicsBar"
  _ <- K.addHvega Nothing Nothing
    $ BRV.demoCompareXYCS
    "District"
    "% non-white"
    "% college grad"
    "Modeled D-Edge"
    "log density"
    (stateAbbr <> " demographic scatter")
    (FV.ViewConfig 600 600 5)
    (FL.fold xyFold' demoModelAndDR)
  BR.brAddPostMarkDownFromFileWith postPaths "_afterNewDemographics" (Just oldDistrictsNoteRef)

  return ()

daveModelColonnade cas =
  let state = F.rgetField @DT.StateAbbreviation
      dNum = F.rgetField @ET.DistrictNumber
      dave = F.rgetField @TwoPartyDShare
      share5 = MT.ciLower . F.rgetField @BRE.ModeledShare
      share50 = MT.ciMid . F.rgetField @BRE.ModeledShare
      share95 = MT.ciUpper . F.rgetField @BRE.ModeledShare
  in C.headed "State" (BR.toCell cas "State" "State" (BR.textToStyledHtml . state))
     <> C.headed "District" (BR.toCell cas "District" "District" (BR.numberToStyledHtml "%d" . dNum))
--     <> C.headed "Model Variation" (BR.toCell cas "ModelId" "ModelId" (BR.textToStyledHtml . BRE.modelLabel . F.rgetField @(MT.ModelId BRE.Model)))
     <> C.headed "Demographic Model (Blue Ripple)" (BR.toCell cas "Demographic" "Demographic" (BR.numberToStyledHtml "%2.1f" . (100*) . share50))
     <> C.headed "Historical Model (Dave's Redistricting)" (BR.toCell cas "Historical" "Historical" (BR.numberToStyledHtml "%2.1f" . (100*) . F.rgetField @TwoPartyDShare))
--     <> C.headed "2019 Result" (BR.toCell cas "2019" "2019" (BR.numberToStyledHtml "%2.2f" . (100*) . F.rgetField @BR.DShare))
--     <> C.headed "5% Model CI" (BR.toCell cas "5% Model CI" "5% Model CI" (BR.numberToStyledHtml "%2.2f" . (100*) . share5))
--     <> C.headed "95% Model CI" (BR.toCell cas "95% Model CI" "95% Model CI" (BR.numberToStyledHtml "%2.2f" . (100*) . share95))


race5FromCPS :: F.Record BRE.CPSVByCDR -> DT.Race5
race5FromCPS r =
  let race4A = F.rgetField @DT.RaceAlone4C r
      hisp = F.rgetField @DT.HispC r
  in DT.race5FromRaceAlone4AndHisp True race4A hisp

densityHistogram :: Foldable f => Text -> FV.ViewConfig -> (Double -> Double) -> Double -> f (F.Record '[DT.PopPerSqMile]) -> GV.VegaLite
densityHistogram title vc g stepSize rows =
  let toVLDataRec = FVD.asVLData (GV.Number . g) "f(Density)" V.:& V.RNil
      vlData = FVD.recordsToData toVLDataRec rows
      encDensity = GV.position GV.X [GV.PName "f(Density)", GV.PmType GV.Quantitative, GV.PBin [GV.Step stepSize]]
      encCount = GV.position GV.Y [GV.PAggregate GV.Count, GV.PmType GV.Quantitative]
      enc = GV.encoding . encDensity . encCount
  in FV.configuredVegaLite vc [FV.title title, enc [], GV.mark GV.Bar [], vlData]



modelAndElectionScatter :: Bool
                         -> Text
                         -> FV.ViewConfig
                         -> F.FrameRec [DT.StateAbbreviation, ET.DistrictNumber, ElexDShare, (MT.ModelId BRE.Model), BRE.ModeledShare]
                         -> GV.VegaLite
modelAndElectionScatter single title vc rows =
  let toVLDataRec = FVD.asVLData GV.Str "State"
                    V.:& FVD.asVLData (GV.Number . realToFrac) "District"
                    V.:& FVD.asVLData (GV.Number . (*100)) "Election Result"
                    V.:& FVD.asVLData (GV.Str . show) "Demographic Model Type"
                    V.:& FVD.asVLData' [("Demographic Model", GV.Number . (*100) . MT.ciMid)
                                       ,("Demographic Model (95% CI)", GV.Number . (*100) . MT.ciUpper)
                                       ,("Demographic Model (5% CI)", GV.Number . (*100) . MT.ciLower)
                                       ]
                    V.:& V.RNil
      vlData = FVD.recordsToData toVLDataRec rows
      makeDistrictName = GV.transform . GV.calculateAs "datum.State + '-' + datum.District" "District Name"
--      xScale = GV.PScale [GV.SDomain (GV.DNumbers [30, 80])]
--      yScale = GV.PScale [GV.SDomain (GV.DNumbers [30, 80])]
      xScale = GV.PScale [GV.SZero False]
      yScale = GV.PScale [GV.SZero False]
      facetModel = [GV.FName "Demographic Model Type", GV.FmType GV.Nominal]
      encModelMid = GV.position GV.Y ([GV.PName "Demographic Model"
                                     , GV.PmType GV.Quantitative
                                     , GV.PScale [GV.SZero False]
                                     , yScale
                                     , GV.PAxis [GV.AxTitle "Demographic Model"]
                                     ]

                                     )
      encModelLo = GV.position GV.Y [GV.PName "Demographic Model (5% CI)"
                                    , GV.PmType GV.Quantitative
                                    , GV.PAxis [GV.AxTitle "Demographic Model"]
                                    , yScale
                                  ]
      encModelHi = GV.position GV.Y2 [GV.PName "Demographic Model (95% CI)"
                                  , GV.PmType GV.Quantitative
                                  , yScale
                                  ]
      encElection = GV.position GV.X [GV.PName "Election Result"
                                     , GV.PmType GV.Quantitative
                                     , GV.PAxis [GV.AxTitle "Election D-Share"]
                                     , xScale
                                  ]
      enc45 =  GV.position GV.X [GV.PName "Demographic Model"
                                  , GV.PmType GV.Quantitative
                                  , GV.PAxis [GV.AxTitle ""]
                                  , GV.PAxis [GV.AxTitle "Election D-Share"]
                                  , xScale
                                  ]
      encDistrictName = GV.text [GV.TName "District Name", GV.TmType GV.Nominal]
      encTooltips = GV.tooltips [[GV.TName "District", GV.TmType GV.Nominal]
                                , [GV.TName "Election Result", GV.TmType GV.Quantitative]
                                , [GV.TName "Demographic Model", GV.TmType GV.Quantitative]
                                ]
      encCITooltips = GV.tooltips [[GV.TName "District", GV.TmType GV.Nominal]
                                  , [GV.TName "Election Result", GV.TmType GV.Quantitative]
                                  , [GV.TName "Demographic Model (5% CI)", GV.TmType GV.Quantitative]
                                  , [GV.TName "Demographic Model", GV.TmType GV.Quantitative]
                                  , [GV.TName "Demographic Model (95% CI)", GV.TmType GV.Quantitative]
                                  ]

      facets = GV.facet [GV.RowBy facetModel]
      selection = (GV.selection . GV.select "view" GV.Interval [GV.Encodings [GV.ChX, GV.ChY], GV.BindScales, GV.Clear "click[event.shiftKey]"]) []
      ptEnc = GV.encoding . encModelMid . encElection . encTooltips -- . encSurvey
      ptSpec = GV.asSpec [selection, ptEnc [], GV.mark GV.Circle [], selection]
      lineEnc = GV.encoding . encModelMid . enc45
      labelEnc = ptEnc . encDistrictName
      ciEnc = GV.encoding . encModelLo . encModelHi . encElection . encCITooltips
      ciSpec = GV.asSpec [ciEnc [], GV.mark GV.ErrorBar [GV.MTicks [GV.MColor "black"]]]
      lineSpec = GV.asSpec [lineEnc [], GV.mark GV.Line [GV.MTooltip GV.TTNone]]
      labelSpec = GV.asSpec [labelEnc [], GV.mark GV.Text [GV.MdX 20], makeDistrictName []]
      finalSpec = if single
                  then [FV.title title, GV.layer [lineSpec, labelSpec, ciSpec, ptSpec], vlData]
                  else [FV.title title, facets, GV.specification (GV.asSpec [GV.layer [lineSpec, labelSpec, ciSpec, ptSpec]]), vlData]
  in FV.configuredVegaLite vc finalSpec --



modelAndDaveScatterChart :: Bool
                         -> Text
                         -> FV.ViewConfig
                         -> F.FrameRec ([BR.StateAbbreviation, ET.DistrictNumber, MT.ModelId BRE.Model, BRE.ModeledShare, TwoPartyDShare])
                         -> GV.VegaLite
modelAndDaveScatterChart single title vc rows =
  let toVLDataRec = FVD.asVLData GV.Str "State"
                    V.:& FVD.asVLData (GV.Number . realToFrac) "District"
                    V.:& FVD.asVLData (GV.Str . show) "Demographic Model Type"
                    V.:& FVD.asVLData' [("Demographic Model", GV.Number . (*100) . MT.ciMid)
                                       ,("Demographic Model (95% CI)", GV.Number . (*100) . MT.ciUpper)
                                       ,("Demographic Model (5% CI)", GV.Number . (*100) . MT.ciLower)
                                       ]
                    V.:& FVD.asVLData (GV.Number . (*100)) "Historical Model"
                    V.:& V.RNil
      vlData = FVD.recordsToData toVLDataRec rows
      makeDistrictName = GV.transform . GV.calculateAs "datum.State + '-' + datum.District" "District Name"
--      xScale = GV.PScale [GV.SDomain (GV.DNumbers [35, 75])]
--      yScale = GV.PScale [GV.SDomain (GV.DNumbers [35, 75])]
      xScale = GV.PScale [GV.SZero False]
      yScale = GV.PScale [GV.SZero False]
      facetModel = [GV.FName "Demographic Model Type", GV.FmType GV.Nominal]
      encModelMid = GV.position GV.Y ([GV.PName "Demographic Model"
                                     , GV.PmType GV.Quantitative
                                     , GV.PAxis [GV.AxTitle "Demographic Model"]
                                     , GV.PScale [GV.SZero False]
                                     , yScale
                                     ]

--                                     ++ [GV.PScale [if single then GV.SZero False else GV.SDomain (GV.DNumbers [0, 100])]]
                                     )
      encModelLo = GV.position GV.Y [GV.PName "Demographic Model (5% CI)"
                                  , GV.PmType GV.Quantitative
                                  , yScale
                                  , GV.PAxis [GV.AxTitle "Demographic Model"]
                                  ]
      encModelHi = GV.position GV.Y2 [GV.PName "Demographic Model (95% CI)"
                                  , GV.PmType GV.Quantitative
                                  , yScale
                                  , GV.PAxis [GV.AxNoTitle]
                                  ]
      encDaves = GV.position GV.X [GV.PName "Historical Model"
                                  , GV.PmType GV.Quantitative
                                  , xScale
                                  , GV.PAxis [GV.AxTitle "Historical Model"]
                                  ]
      enc45 =  GV.position GV.X [GV.PName "Demographic Model"
                                  , GV.PmType GV.Quantitative
                                  , GV.PAxis [GV.AxNoTitle]
                                  , yScale
                                  , GV.PAxis [GV.AxTitle "Historical Model"]
                                  ]
      encDistrictName = GV.text [GV.TName "District Name", GV.TmType GV.Nominal]
      encTooltips = GV.tooltips [[GV.TName "District", GV.TmType GV.Nominal]
                                , [GV.TName "Historical Model", GV.TmType GV.Quantitative]
                                , [GV.TName "Demographic Model", GV.TmType GV.Quantitative]
                                ]
      encCITooltips = GV.tooltips [[GV.TName "District", GV.TmType GV.Nominal]
                                  , [GV.TName "Historical", GV.TmType GV.Quantitative]
                                  , [GV.TName "Demographic Model (5% CI)", GV.TmType GV.Quantitative]
                                  , [GV.TName "Demographic Model", GV.TmType GV.Quantitative]
                                  , [GV.TName "Demographic Model (95% CI)", GV.TmType GV.Quantitative]
                                  ]

      facets = GV.facet [GV.RowBy facetModel]
      selection = (GV.selection . GV.select "view" GV.Interval [GV.Encodings [GV.ChX, GV.ChY], GV.BindScales, GV.Clear "click[event.shiftKey]"]) []
      ptEnc = GV.encoding . encModelMid . encDaves . encTooltips
      lineEnc = GV.encoding . encModelMid . enc45
      labelEnc = ptEnc . encDistrictName . encTooltips
      ciEnc = GV.encoding . encModelLo . encModelHi . encDaves . encCITooltips
      ciSpec = GV.asSpec [ciEnc [], GV.mark GV.ErrorBar [GV.MTicks [GV.MColor "black"]]]
      labelSpec = GV.asSpec [labelEnc [], GV.mark GV.Text [GV.MdX 20], makeDistrictName [] ]
      ptSpec = GV.asSpec [selection, ptEnc [], GV.mark GV.Circle []]
      lineSpec = GV.asSpec [lineEnc [], GV.mark GV.Line [GV.MTooltip GV.TTNone]]
      resolve = GV.resolve . GV.resolution (GV.RAxis [(GV.ChY, GV.Shared)])
      finalSpec = if single
                  then [FV.title title, GV.layer [ciSpec, lineSpec, labelSpec, ptSpec], vlData]
                  else [FV.title title, facets, GV.specification (GV.asSpec [GV.layer [ptSpec, ciSpec, lineSpec, labelSpec]]), vlData]
  in FV.configuredVegaLite vc finalSpec --

-- fold CES data over districts
aggregateDistricts :: FL.Fold (F.Record BRE.CCESByCDR) (F.FrameRec (BRE.StateKeyR V.++ PredictorR V.++ BRE.CCESVotingDataR))
aggregateDistricts = FMR.concatFold
                     $ FMR.mapReduceFold
                     FMR.noUnpack
                     (FMR.assignKeysAndData @(BRE.StateKeyR V.++ PredictorR) @BRE.CCESVotingDataR)
                     (FMR.foldAndAddKey $ FF.foldAllConstrained @Num FL.sum)

aggregatePredictors :: FL.Fold (F.Record (BRE.StateKeyR V.++ PredictorR V.++ BRE.CCESVotingDataR)) (F.FrameRec (BRE.StateKeyR V.++ BRE.CCESVotingDataR))
aggregatePredictors = FMR.concatFold
                     $ FMR.mapReduceFold
                     FMR.noUnpack
                     (FMR.assignKeysAndData @BRE.StateKeyR @BRE.CCESVotingDataR)
                     (FMR.foldAndAddKey $ FF.foldAllConstrained @Num FL.sum)

aggregatePredictorsInDistricts ::  FL.Fold (F.Record BRE.CCESByCDR) (F.FrameRec (BRE.CDKeyR V.++ BRE.CCESVotingDataR))
aggregatePredictorsInDistricts = FMR.concatFold
                                 $ FMR.mapReduceFold
                                 FMR.noUnpack
                                 (FMR.assignKeysAndData @BRE.CDKeyR @BRE.CCESVotingDataR)
                                 (FMR.foldAndAddKey $ FF.foldAllConstrained @Num FL.sum)

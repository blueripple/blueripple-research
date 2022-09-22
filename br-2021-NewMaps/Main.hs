{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE OverloadedLabels #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-# OPTIONS_GHC -O0 #-}
{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}
{-# OPTIONS_GHC -Wno-missing-signatures #-}
{-# OPTIONS_GHC -Wno-type-defaults #-}
{-# HLINT ignore "Redundant <$>" #-}

module Main where

import CDistrictPost (cdPostTop, cdPostBottom, cdPostCompChart, cdPostStateChart)
import qualified DemoCompChart as DCC

import qualified BlueRipple.Configuration as BR
import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.ElectionTypes as ET
import qualified BlueRipple.Data.ModelingTypes as MT
import qualified BlueRipple.Data.ACS_PUMS as PUMS
import qualified BlueRipple.Data.DistrictOverlaps as DO
import qualified BlueRipple.Data.Loaders as BR
import qualified BlueRipple.Data.Loaders.Redistricting as Redistrict
--import qualified BlueRipple.Data.Quantiles as BRQ
import qualified BlueRipple.Data.Visualizations.DemoComparison as BRV
import qualified BlueRipple.Utilities.KnitUtils as BR
import qualified BlueRipple.Utilities.TableUtils as BR
import qualified BlueRipple.Data.CensusLoaders as BRC
import qualified BlueRipple.Data.CountFolds as BRCF
import qualified BlueRipple.Model.DistrictClusters as BRDC
import qualified BlueRipple.Model.TSNE as TSNE
import qualified BlueRipple.Model.Election.DataPrep as BRE
import qualified BlueRipple.Model.Election.StanModel as BRE

import qualified Colonnade as C
import qualified Text.Blaze.Html5.Attributes   as BHA
import qualified Control.Foldl as FL
import qualified Data.Map.Strict as M
--import qualified Data.IntMap.Strict as IM
import qualified Data.Map.Merge.Strict as M
import qualified Data.Monoid as Monoid
import Data.String.Here (here)
import qualified Data.Set as Set
import qualified Data.Text as T
import qualified Data.Time.Calendar            as Time
import qualified Data.Vector as Vector
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V
import qualified System.Console.CmdArgs as CmdArgs
import qualified Frames as F
import qualified Frames.Melt as F
import qualified Frames.Streamly.InCore as FI
import qualified Frames.Streamly.TH as FS
import qualified Frames.MapReduce as FMR
import qualified Control.MapReduce.Simple as MR
import qualified Frames.Folds as FF
import qualified Frames.SimpleJoins as FJ
import qualified Frames.Transform  as FT
import qualified Graphics.Vega.VegaLite as GV
import qualified Graphics.Vega.VegaLite.Compat as FV
import qualified Frames.Visualization.VegaLite.Data as FVD

import qualified Polysemy.State as PS

--import qualified Relude.Extra as Extra

import qualified Graphics.Vega.VegaLite.Configuration as FV

import qualified Knit.Report as K
import qualified Knit.Effect.AtomicCache as KC
import qualified Text.Pandoc.Error as Pandoc
import qualified Numeric
import qualified Path
import Path (Rel, Dir)

import qualified Stan.ModelConfig as SC
import qualified Stan.ModelBuilder as SB
--import Stan.ModelBuilder (binomialLogitDistWithConstants)
--import Stan.ModelBuilder.BuildingBlocks (parallelSampleDistV)
--import BlueRipple.Model.DistrictClusters (districtsForClustering)
import qualified Data.Vector.Unboxed as UVec
--import qualified BlueRipple.Utilities.KnitUtils as K
import qualified BlueRipple.Data.Loaders.Redistricting as BR
--import qualified BlueRipple.Data.CountFolds as BRQ
--import BlueRipple.Data.Quantiles (quantileLookup')
--import BlueRipple.Data.ElectionTypes (VoteShareType(TwoPartyShare))
--import qualified Data.Array as Array
--import qualified Data.List as List
import qualified BlueRipple.Data.CensusTables as DT
--import BlueRipple.Data.DistrictOverlaps (overlapFractionsForRowByName)

FS.declareColumn "DistCategory" ''Text

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

pandocTemplate :: K.TemplatePath
pandocTemplate = K.FullySpecifiedTemplatePath "pandoc-templates/blueripple_basic.html"


main :: IO ()
main = do
  cmdLine <- CmdArgs.cmdArgsRun BR.commandLine
  pandocWriterConfig <-
    K.mkPandocWriterConfig
      pandocTemplate
      templateVars
      (BR.brWriterOptionsF . K.mindocOptionsF)
  let cacheDir = ".flat-kh-cache"
      knitConfig :: K.KnitConfig BR.SerializerC BR.CacheData Text =
        (K.defaultKnitConfig $ Just cacheDir)
          { K.outerLogPrefix = Just "2021-NewMaps"
          , K.logIf = BR.knitLogSeverity $ BR.logLevel cmdLine --K.logDiagnostic
          , K.pandocWriterConfig = pandocWriterConfig
          , K.serializeDict = BR.flatSerializeDict
          , K.persistCache = KC.persistStrictByteString (\t -> toString (cacheDir <> "/" <> t))
          }
--  let stanParallelCfg = BR.clStanParallel cmdLine
--      parallel =  case BR.cores stanParallelCfg of
--        BR.MaxCores -> True
--        BR.FixedCores n -> n > BR.parallelChains stanParallelCfg
  resE <- K.knitHtmls knitConfig $ do
    K.logLE K.Info $ "Command Line: " <> show cmdLine
    let runAll = null $ BR.postNames cmdLine
        runThis x = runAll || x `elem` BR.postNames cmdLine
    when (runThis "modelDetails") $ modelDetails cmdLine
    when (runThis "modelDiagnostics") $ modelDiagnostics cmdLine
    when (runThis "newCDs") $ newCongressionalMapPosts cmdLine
    when (runThis "newSLDs") $ newStateLegMapPosts cmdLine
    when (runThis "allCDs") $ allCDsPost cmdLine
    when (runThis "stateAnalysis") $ stateAnalysis cmdLine
    when (runThis "CAPA") $ capa cmdLine
--    when (runThis "deepDive") $ deepDive2022CD cmdLine "TX" "24"
--    when (runThis "deepDive") $ deepDive2022CD cmdLine "TX" "11"
--    when (runThis "deepDive") $ deepDive2022CD cmdLine "TX" "31"
--    when (runThis "deepDive") $ deepDive2022CD cmdLine "CA" "2"
--    when (runThis "deepDive") $ deepDive2020CD cmdLine "AZ" 1
    when (runThis "deepDive") $ deepDive2022SLD cmdLine "AZ" ET.StateUpper "6"
    when (runThis "deepDive") $ deepDive2022SLD cmdLine "AZ" ET.StateUpper "23"

--    when (runThis "deepDive") $ deepDive2020CD cmdLine "AZ" 9
--    when (runThis "deepDive") $ deepDive2020CD cmdLine "AZ" 3
--    when (runThis "deepDive") $ deepDive2022CD cmdLine "AZ" "7"
--    when (runThis "deepDive") $ deepDiveState cmdLine "CA"

  case resE of
    Right namedDocs ->
      K.writeAllPandocResultsWithInfoAsHtml "" namedDocs
    Left err -> putTextLn $ "Pandoc Error: " <> Pandoc.renderError err

modelDir :: Text
modelDir = "br-2021-NewMaps/stan"

modelVariant :: BRE.Model k
modelVariant = BRE.Model
               ET.TwoPartyShare
               (Set.fromList [ET.President, ET.Senate, ET.House])
               (BRE.BinDensity 10 5)
               (Set.fromList [BRE.DMDensity, BRE.DMSex, BRE.DMEduc, BRE.DMRace, BRE.DMWNG, BRE.DMInc])
               (BRE.BetaBinomial 10)
               (BRE.DSAlphaHNC)
               BRE.HierarchicalBeta
               1

--emptyRel = [Path.reldir||]
postDir :: Path.Path Rel Dir
postDir = [Path.reldir|br-2021-NewMaps/posts|]

postInputs :: Path.Path Rel Dir -> Path.Path Rel Dir
postInputs p = postDir BR.</> p BR.</> [Path.reldir|inputs|]

sharedInputs :: Path.Path Rel Dir
sharedInputs = postDir BR.</> [Path.reldir|Shared|] BR.</> [Path.reldir|inputs|]

postLocalDraft :: Path.Path Rel Dir
               -> Maybe (Path.Path Rel Dir) -> Path.Path Rel Dir
postLocalDraft p mRSD = case mRSD of
  Nothing -> postDir BR.</> p BR.</> [Path.reldir|draft|]
  Just rsd -> postDir BR.</> p BR.</> rsd

postOnline :: Path.Path Rel t -> Path.Path Rel t
postOnline p =  [Path.reldir|research/NewMaps|] BR.</> p

postOnlineExp :: Path.Path Rel t -> Path.Path Rel t
postOnlineExp p = [Path.reldir|explainer/model|] BR.</> p

postOnlineSP :: p -> p
postOnlineSP p = {- [Path.reldir|SawbuckPatriots|] BR.</> -} p

postPaths :: (K.KnitEffects r, MonadIO (K.Sem r))
          => Text
          -> BR.CommandLine
          -> K.Sem r (BR.PostPaths BR.Abs)
postPaths t cmdLine = do
  let mRelSubDir = case cmdLine of
        BR.CLLocalDraft _ _ mS _ -> maybe Nothing BR.parseRelDir $ fmap toString mS
        _ -> Nothing
  postSpecificP <- K.knitEither $ first show $ Path.parseRelDir $ toString t
  BR.postPaths
    BR.defaultLocalRoot
    sharedInputs
    (postInputs postSpecificP)
    (postLocalDraft postSpecificP mRelSubDir)
    (postOnline postSpecificP)

explainerPostPaths :: (K.KnitEffects r, MonadIO (K.Sem r))
                   => Text
                   -> BR.CommandLine
                   -> K.Sem r (BR.PostPaths BR.Abs)
explainerPostPaths t cmdLine = do
  let mRelSubDir = case cmdLine of
        BR.CLLocalDraft _ _ mS _ -> maybe Nothing BR.parseRelDir $ fmap toString mS
        _ -> Nothing
  postSpecificP <- K.knitEither $ first show $ Path.parseRelDir $ toString t
  BR.postPaths
    BR.defaultLocalRoot
    sharedInputs
    (postInputs postSpecificP)
    (postLocalDraft postSpecificP mRelSubDir)
    (postOnlineExp postSpecificP)

sawbuckPostPaths :: (K.KnitEffects r, MonadIO (K.Sem r))
                   => Text
                   -> BR.CommandLine
                   -> K.Sem r (BR.PostPaths BR.Abs)
sawbuckPostPaths t cmdLine = do
  let mRelSubDir = case cmdLine of
        BR.CLLocalDraft _ _ mS _ -> maybe Nothing BR.parseRelDir $ fmap toString mS
        _ -> Nothing
  postSpecificP <- K.knitEither $ first show $ Path.parseRelDir $ toString t
  BR.postPaths
    BR.defaultLocalRoot
    sharedInputs
    (postInputs postSpecificP)
    (postLocalDraft postSpecificP mRelSubDir)
    (postOnlineSP postSpecificP)

-- data
type CCESVoted = "CCESVoters" F.:-> Int
type CCESHouseVotes = "CCESHouseVotes" F.:-> Int
type CCESHouseDVotes = "CCESHouseDVotes" F.:-> Int

type PredictorR = [DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.Race5C]

type CDDemographicsR = '[BR.StateAbbreviation] V.++ BRC.CensusRecodedR V.++ '[DT.Race5C]
type CDLocWStAbbrR = '[BR.StateAbbreviation, ET.DistrictTypeC, ET.DistrictName] -- V.++ BRC.LDLocationR

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
onlyState :: (F.ElemOf xs BR.StateAbbreviation, FI.RecVec xs) => Text -> F.FrameRec xs -> F.FrameRec xs
onlyState x = F.filterFrame ((== x) . F.rgetField @BR.StateAbbreviation)

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
    let addRace5' = FT.mutate (\r -> FT.recordSingleton @DT.Race5C
                                    $ DT.race5FromRaceAlone4AndHisp True (F.rgetField @DT.RaceAlone4C r) (F.rgetField @DT.HispC r))
        cdDataSER' = BRC.censusDemographicsRecode $ BRC.sexEducationRace cdData
        (cdDataSER, cdMissing) =  FJ.leftJoinWithMissing @'[BR.StateFips] cdDataSER'
                                  $ fmap (F.rcast @[BR.StateFips, BR.StateAbbreviation] . FT.retypeColumn @BR.StateFIPS @BR.StateFips) stateAbbrs
    when (not $ null cdMissing) $ K.knitError $ "state FIPS missing in proposed district demographics/stateAbbreviation join."
    return $ (F.rcast . addRace5' <$> cdDataSER)

modelDetails ::  forall r. (K.KnitMany r, BR.CacheEffects r) => BR.CommandLine -> K.Sem r () --BR.StanParallel -> Bool -> K.Sem r ()
modelDetails cmdLine = do
  let postInfoDetails = BR.PostInfo (BR.postStage cmdLine) (BR.PubTimes (BR.Published $ Time.fromGregorian 2021 9 23) (Just BR.Unpublished))
  detailsPaths <- explainerPostPaths "ElectionModel" cmdLine
  BR.brNewPost detailsPaths postInfoDetails "ElectionModel"
    $ BR.brAddPostMarkDownFromFile detailsPaths "_intro"



type StatePostStratR = '[BR.StateAbbreviation] V.++ ModelPredictorR V.++ '[BRC.Count]

fixACSState :: F.Record BRE.PUMSWithDensityEM -> F.Record StatePostStratR
fixACSState = F.rcast . addRace5 . addCount

stateAnalysis :: forall r. (K.KnitMany r, BR.CacheEffects r) => BR.CommandLine -> K.Sem r ()
stateAnalysis cmdLine = do
  K.logLE K.Info "Rebuilding stateAnalysis post (if necessary)."
  let postInfo = BR.PostInfo (BR.postStage cmdLine) (BR.PubTimes BR.Unpublished  Nothing)
  stateAnalysisPaths <- postPaths "StateAnalysis" cmdLine
  ccesAndCPSEM_C <-  BRE.prepCCESAndCPSEM False
  acs_C <- BRE.prepACS False
  let ccesAndCPS2020_C = fmap (BRE.ccesAndCPSForYears [2020]) ccesAndCPSEM_C
      acs2020_C = fixACSState <<$>> (fmap (BRE.acsForYears [2020]) $ acs_C)
      mapGroup :: SB.GroupTypeTag (F.Record '[BR.StateAbbreviation]) = SB.GroupTypeTag "State"
      psInfoDM name = (mapGroup, name)
      stanParams = SC.StanMCParameters 4 4 (Just 1000) (Just 1000) (Just 0.8) (Just 10) Nothing

      modelDM :: Text -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec (BRE.ModelResultsR '[BR.StateAbbreviation])))
      modelDM name =
        BRE.electionModelDM False cmdLine False (Just stanParams) modelDir modelVariant 2020 (psInfoDM name)
        ccesAndCPS2020_C (fmap (F.rcast @StatePostStratR) <$> acs2020_C)
  modeled_C <- modelDM ("All_State")
  let deps = (,) <$> modeled_C <*> acs2020_C
  modelAndDemo_C <- BR.retrieveOrMakeFrame "posts/newMaps/stateAnalysis/modelAndDemo.bin" deps $ \(modeled, demo) -> do
    let densityAndGowFld = DCC.buildMRFold
          @'[BR.StateAbbreviation]
          @[BRC.Count,  DT.PopPerSqMile, DT.Race5C, DT.CollegeGradC]
          (pwldFC V.:& wFC V.:& wgFC V.:& V.RNil)
        (withDemo, missing) = FJ.leftJoinWithMissing @'[BR.StateAbbreviation] modeled (FL.fold densityAndGowFld demo)
    when (not $ null missing) $ K.knitError $ "stateAnalysis: missing keys in model/demographics join: " <> show missing
    return withDemo
  modelAndDemo <- K.ignoreCacheTime modelAndDemo_C
  let categoryNames = ["Density", "%Grad-Among-White", "%Voters-Of-Color"]
      categoryFunctions = zipWith DCC.SBCCategoryData categoryNames
                          [F.rgetField @DT.PopPerSqMile
                          , F.rgetField @BRE.FracGradOfWhite
                          , (\r -> 1 - F.rgetField @BRE.FracWhiteNonHispanic r)
                          ]
      quantileBreaks = DCC.sbcQuantileBreaks 20 categoryFunctions modelAndDemo
      quantileFunctionsE = DCC.sbcQuantileFunctions quantileBreaks
      quantileFunctionsDblE = (fmap (realToFrac @Int @Double) .) <<$>> quantileFunctionsE
      partyFilters = let f = MT.ciMid . F.rgetField @BRE.ModeledShare in DCC.SBCPartyData ((>= 0.5) . f) ((< 0.5) . f)
      partyMediansE = DCC.partyMedians partyFilters quantileFunctionsDblE modelAndDemo
      partyRanksE = DCC.partyRanks partyFilters quantileFunctionsDblE modelAndDemo
      partyLoHisE = DCC.partyLoHis <$> partyRanksE
  partyLoHis <- K.knitEither partyLoHisE
  partyMedians <- K.knitEither partyMediansE

  let eachState r = do
        let sa = F.rgetField @BR.StateAbbreviation r
        BR.brAddMarkDown $ "State: " <> sa
        BR.brAddMarkDown $ "Modeled Share: " <> show (MT.ciMid $ F.rgetField @BRE.ModeledShare r)
        let sbcsE = DCC.sbcComparison quantileFunctionsDblE partyMedians
        sbcs <- K.knitEither $ sbcsE r
        _<- K.addHvega Nothing Nothing $ DCC.sbcChart DCC.SBCNational 20 10 (FV.ViewConfig 300 80 5) (Just categoryNames)
          (fmap (\(n, m) -> (realToFrac n, realToFrac m)) <<$>> partyLoHis)
          $ one (sa, True, sbcs)
        pure ()
  BR.logFrame modelAndDemo
  BR.brNewPost stateAnalysisPaths postInfo "StateAnalysis" $ do
    traverse_ eachState modelAndDemo

deepDive2022CD :: forall r. (K.KnitMany r, BR.CacheEffects r) => BR.CommandLine -> Text -> Text -> K.Sem r ()
deepDive2022CD cmdLine sa dn = do
  proposedCDs_C <- prepCensusDistrictData False "model/newMaps/newCDDemographicsDR.bin" =<< BRC.censusTablesForProposedCDs
  let filter' r = F.rgetField @BR.StateAbbreviation r == sa && F.rgetField @ET.DistrictName r == dn
  deepDive cmdLine ("2022-" <> sa <> dn) (fmap (FL.fold postStratRollupFld . fmap F.rcast . F.filterFrame filter') proposedCDs_C)

deepDive2022SLD :: forall r. (K.KnitMany r, BR.CacheEffects r) => BR.CommandLine -> Text -> ET.DistrictType -> Text -> K.Sem r ()
deepDive2022SLD cmdLine sa dt dn = do
  proposedSLDs_C <- prepCensusDistrictData False "model/NewMaps/newStateLegDemographics.bin" =<< BRC.censusTablesFor2022SLDs
  let filter' r = F.rgetField @BR.StateAbbreviation r == sa && F.rgetField @ET.DistrictTypeC r == dt && F.rgetField @ET.DistrictName r == dn
  deepDive cmdLine ("2022-" <> sa <> "_" <> houseChar dt <> dn) (fmap (FL.fold postStratRollupFld . fmap F.rcast . F.filterFrame filter') proposedSLDs_C)

deepDive2020CD :: forall r. (K.KnitMany r, BR.CacheEffects r) => BR.CommandLine -> Text -> Int -> K.Sem r ()
deepDive2020CD cmdLine sa dn = do
  acs_C <- BRE.prepACS False
  let filter' r = F.rgetField @BR.Year r == 2020 && F.rgetField @BR.StateAbbreviation r == sa && F.rgetField @BR.CongressionalDistrict r == dn
  deepDive cmdLine ("2020-" <> sa <> show dn) (fmap (FL.fold postStratRollupFld . fmap fixACS . F.filterFrame filter') acs_C)


deepDiveState :: forall r. (K.KnitMany r, BR.CacheEffects r) => BR.CommandLine -> Text -> K.Sem r ()
deepDiveState cmdLine sa = do
  proposedCDs_C <- prepCensusDistrictData False "model/newMaps/newCDDemographicsDR.bin" =<< BRC.censusTablesForProposedCDs
  let filter' r = F.rgetField @BR.StateAbbreviation r == sa
  deepDive cmdLine sa (fmap (FL.fold postStratRollupFld . fmap F.rcast . F.filterFrame filter') proposedCDs_C)


type FracPop = "FracPop" F.:-> Double
type DSDT = "dS_dT" F.:-> Double
type DSDP =   "dS_dP" F.:-> Double

type DeepDiveR = [DT.SexC, DT.CollegeGradC, DT.Race5C]

deepDive :: forall r. (K.KnitMany r, BR.CacheEffects r) => BR.CommandLine -> Text -> K.ActionWithCacheTime r (F.FrameRec PostStratR) -> K.Sem r ()
deepDive cmdLine ddName psData_C = do
  ccesAndCPSEM_C <-  BRE.prepCCESAndCPSEM False
--  acs_C <- BRE.prepACS False
  let ccesAndCPS2020_C = fmap (BRE.ccesAndCPSForYears [2020]) ccesAndCPSEM_C
--      acs2020_C = fmap (BRE.acsForYears [2020]) acs_C
      demographicGroup :: SB.GroupTypeTag (F.Record DeepDiveR) = SB.GroupTypeTag "Demographics"
      postStratInfo = (demographicGroup, "DeepDive_" <> ddName)
      stanParams = SC.StanMCParameters 4 4 (Just 1000) (Just 1000) (Just 0.8) (Just 10) Nothing
  let modelDM ::  K.Sem r (F.FrameRec (BRE.ModelResultsR DeepDiveR))
      modelDM =
        K.ignoreCacheTimeM $ BRE.electionModelDM False cmdLine False (Just stanParams) modelDir modelVariant 2020 postStratInfo ccesAndCPS2020_C psData_C
      postInfoDeepDive = BR.PostInfo (BR.postStage cmdLine) (BR.PubTimes BR.Unpublished Nothing)
  deepDiveModel <- modelDM
  psData <- K.ignoreCacheTime psData_C
  let (deepDive', missing) = FJ.leftJoinWithMissing @DeepDiveR deepDiveModel psData
  when (not $ null missing) $ K.knitError $ "Missing keys in depDiveModel/psData join:" <> show missing
  let turnout = MT.ciMid . F.rgetField @BRE.ModeledTurnout
      pref = MT.ciMid . F.rgetField @BRE.ModeledPref
      cvap = F.rgetField @BRC.Count
      cvFld = fmap realToFrac $ FL.premap cvap FL.sum
      tvF r = realToFrac (cvap r) * turnout r
      tvFld = FL.premap tvF FL.sum
      dvF r = realToFrac (cvap r) * turnout r * pref r
      dvFld = FL.premap dvF FL.sum
      (totalCVAP, totalVotes, totalDVotes) = FL.fold ((,,) <$> cvFld <*> tvFld <*> dvFld) deepDive'
      popFrac r = FT.recordSingleton @FracPop $ realToFrac (F.rgetField @BRC.Count r) / totalCVAP
      dSdP r = FT.recordSingleton @DSDP $ turnout r * (realToFrac $ cvap r) / totalVotes
      dSdT r = FT.recordSingleton @DSDT $ (pref r - (realToFrac totalDVotes/realToFrac totalVotes)) * (realToFrac $ cvap r) / totalVotes
      deepDiveWFrac = fmap (FT.mutate dSdP . FT.mutate dSdT . FT.mutate popFrac) deepDive'
      deepDiveSummary = FL.fold deepDiveSummaryFld $ fmap F.rcast deepDiveWFrac
      deepDiveWSummary = (BR.dataRow <$> FL.fold FL.list deepDiveWFrac) ++ [BR.summaryRow deepDiveSummary]
-- summarize
  BR.logFrame' K.Diagnostic deepDiveWFrac
  deepDivePaths <- postPaths ("DeepDive_" <> ddName) cmdLine
  BR.brNewPost deepDivePaths postInfoDeepDive ("DeepDive_" <> ddName) $ do
    BR.brAddRawHtmlTable
      ("Deep Dive: " <> ddName)
      (BHA.class_ "brTable")
      (deepDiveColonnade mempty)
      deepDiveWSummary

data DeepDiveSummary = DeepDiveSummary { ddsCVAP :: Int, ddsFracPop :: Double, ddsDensity :: Double, ddsTurnout :: Double, ddsPref :: Double, ddsShare :: Double}

deepDiveSummaryFld :: FL.Fold (F.Record [BRC.Count, FracPop, DT.PopPerSqMile, BRE.ModeledTurnout, BRE.ModeledPref, BRE.ModeledShare]) DeepDiveSummary
deepDiveSummaryFld =
  let cntFld = FL.premap (F.rgetField @BRC.Count) FL.sum
      fracPopFld = FL.premap (F.rgetField @FracPop) FL.sum
      wgtdFld w f = (/) <$> FL.premap (\r -> w r * f r) FL.sum <*> FL.premap w FL.sum
      geomWgtdFld w f = Numeric.exp <$> wgtdFld w (Numeric.log . f)
      cvapWgt = realToFrac . F.rgetField @BRC.Count
      densFld = geomWgtdFld cvapWgt (F.rgetField @DT.PopPerSqMile)
      cvapWgtdFld = wgtdFld cvapWgt
      tFld = cvapWgtdFld (MT.ciMid . F.rgetField @BRE.ModeledTurnout)
      pFld = cvapWgtdFld (MT.ciMid . F.rgetField @BRE.ModeledPref)
      sFld = wgtdFld (\r -> realToFrac (F.rgetField @BRC.Count r) * (MT.ciMid . F.rgetField @BRE.ModeledTurnout $ r)) (MT.ciMid . F.rgetField @BRE.ModeledPref)
  in DeepDiveSummary <$> cntFld <*> fracPopFld <*> densFld <*> tFld <*> pFld<*> sFld

--deepDiveColonnade :: _
deepDiveColonnade cas =
  let orNA g = BR.dataOrSummary g (BR.textToStyledHtml . const "N/A")
      showOrNA f = orNA (BR.textToStyledHtml . show . f)
      density = F.rgetField @DT.PopPerSqMile `BR.dataOrSummary` ddsDensity
      mTurnout = (MT.ciMid . F.rgetField @BRE.ModeledTurnout) `BR.dataOrSummary` ddsTurnout
      mPref = (MT.ciMid . F.rgetField @BRE.ModeledPref) `BR.dataOrSummary` ddsPref
      mShare' = MT.ciMid . F.rgetField @BRE.ModeledShare
      mShare = mShare' `BR.dataOrSummary` ddsShare
      mDiff = orNA (\r -> let x = mShare' r in BR.numberToStyledHtml "%2.1f" . (100*) $ (2 * x - 1))
      cvap = F.rgetField @BRC.Count `BR.dataOrSummary` ddsCVAP
      fracPop = F.rgetField @FracPop `BR.dataOrSummary` ddsFracPop
      sex = showOrNA $ F.rgetField @DT.SexC
      education = showOrNA $ F.rgetField @DT.CollegeGradC
      race = showOrNA $ F.rgetField @DT.Race5C
      dSdT = orNA $ BR.numberToStyledHtml "%2.1f" . (100*) . F.rgetField @DSDT
      dSdP = orNA $ BR.numberToStyledHtml "%2.1f" . (100*) . F.rgetField @DSDP
  in C.headed "Sex" (BR.toCell cas "Sex" "Sex" sex)
     <> C.headed "Education" (BR.toCell cas "Edu" "Edu" education)
     <> C.headed "Race" (BR.toCell cas "Race" "Race" race)
     <> C.headed "CVAP" (BR.toCell cas "CVAP" "CVAP" (BR.numberToStyledHtml "%d" . cvap))
     <> C.headed "%Pop" (BR.toCell cas "CVAP" "CVAP" (BR.numberToStyledHtml "%2.1f" . (100*) . fracPop))
     <> C.headed "Ppl/SqMi" (BR.toCell cas "Ppl/SqMi" "Ppl/SqMi" (BR.numberToStyledHtml "%2.0f" . density))
     <> C.headed "Modeled Turnout" (BR.toCell cas "M Turnout" "M Turnout" (BR.numberToStyledHtml "%2.1f" . (100*) . mTurnout))
     <> C.headed "Modeled 2-party D Pref" (BR.toCell cas "M Share" "M Share" (BR.numberToStyledHtml "%2.1f" . (100*) . mPref))
     <> C.headed "Modeled 2-party D Share" (BR.toCell cas "M Share" "M Share" (BR.numberToStyledHtml "%2.1f" . (100*) . mShare))
     <> C.headed "Modeled 2-party D Diff" (BR.toCell cas "M Diff" "M Diff" mDiff)
     <> C.headed "dS/dT" (BR.toCell cas "dS/dT" "dS/dT" dSdT)
     <> C.headed "dS/dP" (BR.toCell cas "dS/dP" "dS/dP" dSdP)

ccesAndCPSForStates :: [Text] -> BRE.CCESAndCPSEM -> BRE.CCESAndCPSEM
ccesAndCPSForStates sas (BRE.CCESAndCPSEM cces cpsV acs stElex cdElex) =
  let f :: (FI.RecVec rs, F.ElemOf rs BR.StateAbbreviation) => F.FrameRec rs -> F.FrameRec rs
      f = F.filterFrame ((`elem` sas) . F.rgetField @BR.StateAbbreviation)
  in BRE.CCESAndCPSEM (f cces) (f cpsV) (f acs) (f stElex) (f cdElex)


type SimpleDR = [BRE.FracFemale
                , BRE.FracGrad
                , BRE.FracWhiteNonHispanic
                , BRE.FracOther
                , BRE.FracBlack
                , BRE.FracHispanic
                , BRE.FracAsian
                , BRE.FracWhiteNonGrad
                , DT.PopPerSqMile
                ]


psDemographicsInnerFld :: FL.Fold (F.Record (ModelPredictorR V.++ '[BRC.Count])) (F.Record SimpleDR)
psDemographicsInnerFld =
  let cnt = F.rgetField @BRC.Count
      sex = F.rgetField @DT.SexC
      grad = F.rgetField @DT.CollegeGradC
      race = F.rgetField @DT.Race5C
      density = F.rgetField @DT.PopPerSqMile
      logDensity = Numeric.log . density
      intRatio x y = realToFrac x / realToFrac y
      cntF = FL.premap cnt FL.sum
      fracF f = intRatio <$> FL.prefilter f cntF <*> cntF
      cntWgtdSumF f = FL.premap (\r -> realToFrac (cnt r) * f r) FL.sum
--      cntWgtdF f = (/) <$> cntWgtdSumF f <*> fmap realToFrac cntF
  in FF.sequenceRecFold $
     FF.toFoldRecord (fracF ((== DT.Female) . sex))
     V.:& FF.toFoldRecord (fracF ((== DT.Grad) . grad))
     V.:& FF.toFoldRecord (fracF ((== DT.R5_WhiteNonHispanic) . race))
     V.:& FF.toFoldRecord (fracF ((== DT.R5_Other) . race))
     V.:& FF.toFoldRecord (fracF ((== DT.R5_Black) . race))
     V.:& FF.toFoldRecord (fracF ((== DT.R5_Hispanic) . race))
     V.:& FF.toFoldRecord (fracF ((== DT.R5_Asian) . race))
     V.:& FF.toFoldRecord (fracF (\r -> race r == DT.R5_WhiteNonHispanic && grad r == DT.NonGrad))
     V.:& FF.toFoldRecord (fmap Numeric.exp $ cntWgtdSumF logDensity)
     V.:& V.RNil

psDemographicsFld :: (Ord (F.Record ks)
                     , FI.RecVec (ks V.++ SimpleDR)
                     )
                  => (F.Record PostStratR -> F.Record ks)
                  -> FL.Fold (F.Record PostStratR) (F.FrameRec (ks V.++ SimpleDR))
psDemographicsFld f = FMR.concatFold
                      $ FMR.mapReduceFold
                      FMR.noUnpack
                      (FMR.Assign $ \r -> (f r, F.rcast r))
                      (FMR.foldAndAddKey psDemographicsInnerFld)

ccesCounts :: forall ks.(Ord (F.Record ks)
                        , FI.RecVec (ks V.++ [BRE.Surveyed, BRE.Voted, BRE.HouseVotes, BRE.HouseDVotes, BRE.PresVotes, BRE.PresDVotes])
                        , ks F.⊆ BRE.CCESWithDensity
                        )
           => FL.Fold
           (F.Record BRE.CCESWithDensity)
           (F.FrameRec (ks V.++ [BRE.Surveyed, BRE.Voted, BRE.HouseVotes, BRE.HouseDVotes, BRE.PresVotes, BRE.PresDVotes]))
ccesCounts = FMR.concatFold
             $ FMR.mapReduceFold
             FMR.noUnpack
             (FMR.assignKeysAndData @ks @[BRE.Surveyed, BRE.Voted, BRE.HouseVotes, BRE.HouseDVotes, BRE.PresVotes, BRE.PresDVotes])
             (FMR.foldAndAddKey $ (FF.foldAllConstrained @Num FL.sum))

modelDiagnostics ::  forall r. (K.KnitMany r, BR.CacheEffects r) => BR.CommandLine -> K.Sem r () --BR.StanParallel -> Bool -> K.Sem r ()
modelDiagnostics cmdLine = do
  ccesAndPums_C <- BRE.prepCCESAndPums False
  ccesAndCPSEM_C <-  BRE.prepCCESAndCPSEM False
  acs_C <- BRE.prepACS False
{-
  pumsRaw_C <- PUMS.pumsLoaderAdults
  let pbpFilter r = F.rgetField @BR.Year r == 2020 && F.rgetField @BR.StateAbbreviation r == "AZ"
  pumsByPUMA <- BRE.pumsByPUMA pbpFilter <$> K.ignoreCacheTime pumsRaw_C
  K.logLE K.Info "PumsByPUMA"
  BR.logFrame pumsByPUMA
  cdByPUMA_C <- BR.allCDFromPUMA2012Loader
  pumsByCD_C <- BRE.cachedPumsByCD pumsRaw_C cdByPUMA_C
  let pbCDFilter r = pbpFilter r && F.rgetField @BR.CongressionalDistrict r == 9
  pumsByCD <-  F.filterFrame pbCDFilter <$> K.ignoreCacheTime pumsByCD_C
  K.logLE K.Info "PumsByCD"
  BR.logFrame pumsByCD
  let acsByCD_C = fmap BRE.acsRows ccesAndCPSEM_C
  acsByCD <- F.filterFrame pbCDFilter <$> K.ignoreCacheTime acsByCD_C
  K.logLE K.Info "acsByCD"
  BR.logFrame acsByCD
  preppedACS_C <- BRE.prepACS False
  preppedACSByCD <-  F.filterFrame pbCDFilter <$> K.ignoreCacheTime preppedACS_C
  K.logLE K.Info "preppedACSByCD"
  BR.logFrame preppedACSByCD
  K.knitError "STOP"
-}

  BRE.prepHouseElectionData False 2020 >>= K.ignoreCacheTime >>= BR.logFrame' K.Diagnostic
  BRE.prepSenateElectionData False 2020 >>= K.ignoreCacheTime >>= BR.logFrame' K.Diagnostic
  BRE.prepPresidentialElectionData False 2020 >>= K.ignoreCacheTime >>= BR.logFrame' K.Diagnostic
  let ccesAndCPS2020_C = fmap (BRE.ccesAndCPSForYears [2020]) ccesAndCPSEM_C
      acs2020_C = fmap (BRE.acsForYears [2020]) acs_C
      fixedACS_C =  FL.fold postStratRollupFld . fmap fixACS <$> acs2020_C
--      ccesWD_C = fmap BRE.ccesEMRows ccesAndCPSEM_C
      pElexRowsFilter r = F.rgetField @ET.Office r == ET.President && F.rgetField @BR.Year r == 2020
      presElex2020_C = fmap (F.filterFrame pElexRowsFilter . BRE.stateElectionRows) $ ccesAndCPSEM_C
      hElexRowsFilter r = F.rgetField @ET.Office r == ET.House && F.rgetField @BR.Year r == 2020
      houseElex2020_C = fmap (F.filterFrame hElexRowsFilter . BRE.cdElectionRows) $ ccesAndCPSEM_C
      stanParams = SC.StanMCParameters 4 4 (Just 1000) (Just 1000) (Just 0.8) (Just 10) Nothing
--      stateGroup :: SB.GroupTypeTag (F.Record CDLocWStAbbrR) = SB.GroupTypeTag "CD"
      sexGroup :: SB.GroupTypeTag (F.Record '[DT.SexC]) = SB.GroupTypeTag "Sex"
      educationGroup :: SB.GroupTypeTag (F.Record '[DT.CollegeGradC]) = SB.GroupTypeTag "Education"
      raceGroup :: SB.GroupTypeTag (F.Record '[DT.Race5C]) = SB.GroupTypeTag "Race"
      stateGroup :: SB.GroupTypeTag (F.Record '[BR.StateAbbreviation]) = SB.GroupTypeTag "State"
      cdGroup :: SB.GroupTypeTag (F.Record '[BR.StateAbbreviation, ET.DistrictName]) = SB.GroupTypeTag "CD"
      modelDM :: (BRE.ModelKeyC ks
                 , ks F.⊆ PostStratR
                 )
              => Bool
              -> SB.GroupTypeTag (F.Record ks)
              -> K.Sem r (F.FrameRec (BRE.ModelResultsR ks))
      modelDM includePP gtt  =
        K.ignoreCacheTimeM
          $ BRE.electionModelDM
          False
          cmdLine
          includePP
          (Just stanParams)
          modelDir
          modelVariant
          2020
          (gtt, "Diagnostics_By" <> SB.taggedGroupName gtt)
          ccesAndCPS2020_C
          fixedACS_C
      postInfoDiagnostics = BR.PostInfo (BR.postStage cmdLine) (BR.PubTimes BR.Unpublished Nothing)
--  fixedACS <- K.ignoreCacheTime fixedACS_C
--  K.logLE K.Info "Demographics By State"
--  BR.logFrame $ FL.fold (psDemographicsFld (F.rcast @'[BR.StateAbbreviation])) fixedACS

  modelBySex  <- modelDM False sexGroup
  modelByEducation  <- modelDM False educationGroup
  modelByRace  <- modelDM False raceGroup
  modelByState  <- modelDM True stateGroup
  modelByCD <- modelDM False cdGroup
  diag_C <- BRE.ccesDiagnostics False "DiagPost"
            (fmap (fmap F.rcast . BRE.pumsRows) ccesAndPums_C)
            (fmap (fmap F.rcast . BRE.ccesRows) ccesAndPums_C)
  ccesDiagByState <- K.ignoreCacheTime diag_C
  K.logLE K.Diagnostic "CCES Diag By State"
  BR.logFrame' K.Diagnostic ccesDiagByState
  ccesRows <- K.ignoreCacheTime $ fmap BRE.ccesRows ccesAndPums_C
  let ccesCountsByRace = FL.fold (ccesCounts @'[DT.Race5C]) ccesRows
  BR.logFrame' K.Diagnostic ccesCountsByRace
  presElexByState <- K.ignoreCacheTime presElex2020_C
  let (stDiagTable1 , missingCTElex, missingCCES) = FJ.leftJoin3WithMissing @[BR.Year, BR.StateAbbreviation] modelByState presElexByState ccesDiagByState
  when (not $ null missingCTElex) $ K.logLE K.Diagnostic $ "Missing keys in state crossTabs/presElex join: " <> show missingCTElex
  when (not $ null missingCCES) $ K.logLE K.Diagnostic $ "Missing keys in state crossTabs/presElex -> cces join: " <> show missingCCES
  stateTurnout <- fmap (F.rcast @[BR.Year, BR.StateAbbreviation, BR.BallotsCountedVEP, BR.HighestOfficeVEP, BR.VEP]) <$> K.ignoreCacheTimeM BR.stateTurnoutLoader
  cpsDiag <- K.ignoreCacheTimeM $ BRE.cpsDiagnostics "" $ fmap (fmap F.rcast . BRE.cpsVRows) ccesAndPums_C
  let cpsByState = snd cpsDiag
      (stDiagTable2, missingTableTurnout, missingCPS) = FJ.leftJoin3WithMissing @[BR.Year, BR.StateAbbreviation] stDiagTable1 stateTurnout cpsByState
  when (not $ null missingTableTurnout) $ K.logLE K.Diagnostic $ "Missing keys when joining stateTurnout: " <> show missingTableTurnout
  when (not $ null missingCPS) $ K.logLE K.Diagnostic $ "Missing keys when joining CPS: " <> show missingCPS
  houseElexByCD <- fmap (FT.mutate (FT.recordSingleton @ET.DistrictName . show . F.rgetField @BR.CongressionalDistrict)) <$> K.ignoreCacheTime houseElex2020_C
  let (cdDiagTable, missingHElex) = FJ.leftJoinWithMissing @[BR.Year, BR.StateAbbreviation, ET.DistrictName] modelByCD houseElexByCD
  when (not $ null missingHElex) $ K.logLE K.Diagnostic $ "Missing keys in cd crossTabs/houseElex join: " <> show missingHElex
  diagnosticsPaths <- postPaths "Diagnostics" cmdLine
  BR.brNewPost diagnosticsPaths postInfoDiagnostics "Diagnostics" $ do
    BR.brAddRawHtmlTable
      "By Race"
      (BHA.class_ "brTable")
      (byCategoryColonnade "Race" (show . F.rgetField @DT.Race5C) mempty)
      modelByRace
    BR.brAddRawHtmlTable
      "By Sex"
      (BHA.class_ "brTable")
      (byCategoryColonnade "Sex" (show . F.rgetField @DT.SexC) mempty)
      modelBySex
    BR.brAddRawHtmlTable
      "By Education"
      (BHA.class_ "brTable")
      (byCategoryColonnade "Education" (show . F.rgetField @DT.CollegeGradC) mempty)
      modelByEducation
    BR.brAddRawHtmlTable
      "Diagnostics By State"
      (BHA.class_ "brTable")
      (stDiagTableColonnade mempty)
      stDiagTable2
    BR.brAddRawHtmlTable
      "Diagnostics By CD"
      (BHA.class_ "brTable")
      (cdDiagTableColonnade mempty)
      cdDiagTable
    pure ()

byCategoryColonnade :: (F.ElemOf rs BRE.ModeledTurnout
                       , F.ElemOf rs BRE.ModeledPref
                       , F.ElemOf rs BRE.ModeledShare
                       )
                    => Text
                    -> (F.Record rs -> Text)
                    -> BR.CellStyle (F.Record rs) K.Cell
                    -> K.Colonnade K.Headed (F.Record rs) K.Cell
byCategoryColonnade catName f cas =
  let mTurnout = MT.ciMid . F.rgetField @BRE.ModeledTurnout
      mTurnoutL = MT.ciLower . F.rgetField @BRE.ModeledTurnout
      mTurnoutU = MT.ciUpper . F.rgetField @BRE.ModeledTurnout
      mPref = MT.ciMid . F.rgetField @BRE.ModeledPref
      mPrefL = MT.ciLower . F.rgetField @BRE.ModeledPref
      mPrefU = MT.ciUpper . F.rgetField @BRE.ModeledPref
      mShare = MT.ciMid . F.rgetField @BRE.ModeledShare
      mDiff r = let x = mShare r in (2 * x - 1)
  in  C.headed (BR.textToCell catName) (BR.toCell cas (BR.textToCell catName) catName (BR.textToStyledHtml . f))
      <> C.headed "Modeled Turnout (5%)" (BR.toCell cas "M Turnout" "M Turnout" (BR.numberToStyledHtml "%2.1f" . (100*) . mTurnoutL))
      <> C.headed "Modeled Turnout" (BR.toCell cas "M Turnout" "M Turnout" (BR.numberToStyledHtml "%2.1f" . (100*) . mTurnout))
      <> C.headed "Modeled Turnout (95%)" (BR.toCell cas "M Turnout" "M Turnout" (BR.numberToStyledHtml "%2.1f" . (100*) . mTurnoutU))
      <> C.headed "Modeled 2-party D Pref (5%)" (BR.toCell cas "M Share" "M Pref" (BR.numberToStyledHtml "%2.1f" . (100*) . mPrefL))
      <> C.headed "Modeled 2-party D Pref" (BR.toCell cas "M Share" "M Pref" (BR.numberToStyledHtml "%2.1f" . (100*) . mPref))
      <> C.headed "Modeled 2-party D Pref (95%)" (BR.toCell cas "M Share" "M Pref" (BR.numberToStyledHtml "%2.1f" . (100*) . mPrefU))
      <> C.headed "Modeled 2-party D Share" (BR.toCell cas "M Share" "M Share" (BR.numberToStyledHtml "%2.1f" . (100*) . mShare))
      <> C.headed "Modeled 2-party D Diff" (BR.toCell cas "M Diff" "M Diff" (BR.numberToStyledHtml "%2.1f" . (100*) . mDiff))

stDiagTableColonnade cas =
  let state' = F.rgetField @DT.StateAbbreviation
      mTurnout = MT.ciMid . F.rgetField @BRE.ModeledTurnout
      mPref = MT.ciMid . F.rgetField @BRE.ModeledPref
      mShare = MT.ciMid . F.rgetField @BRE.ModeledShare
      mDiff r = let x = mShare r in (2 * x - 1)
      acsCVAP = F.rgetField @ET.CVAP
      elexCVAP = F.rgetField @PUMS.Citizens
      voters = F.rgetField @BRE.TVotes
      demVoters = F.rgetField @BRE.DVotes
      repVoters = F.rgetField @BRE.RVotes
      ratio x y = realToFrac @_ @Double x / realToFrac @_ @Double y
      rawTurnout r = ratio (voters r) (elexCVAP r)
      ahTurnoutTarget = F.rgetField @BR.BallotsCountedVEP
      ccesRawTurnout r = realToFrac @_ @Double (F.rgetField @BRE.Voted r) / realToFrac @_ @Double (F.rgetField @BRE.Surveyed r)
      ccesTurnout  r = ratio (F.rgetField @BRE.PSVoted r) (elexCVAP r)
      cpsTurnout r = ratio (F.rgetField @BRCF.Successes r) (F.rgetField @BRCF.Count r)
      rawDShare r = ratio (demVoters r) (demVoters r + repVoters r)
      ccesDShare r = ratio (F.rgetField @BRE.PresDVotes r) (F.rgetField @BRE.PresDVotes r + F.rgetField @BRE.PresRVotes r)
  in  C.headed "State" (BR.toCell cas "State" "State" (BR.textToStyledHtml . state'))
      <> C.headed "ACS CVAP" (BR.toCell cas "ACS CVAP" "ACS CVAP" (BR.numberToStyledHtml "%d" . acsCVAP))
      <> C.headed "Elex CVAP" (BR.toCell cas "Elex CVAP" "Elex CVAP" (BR.numberToStyledHtml "%d" . elexCVAP))
      <> C.headed "Elex Votes" (BR.toCell cas "Elex Votes" "Votes" (BR.numberToStyledHtml "%d" . voters))
      <> C.headed "Elex Dem Votes" (BR.toCell cas "Elex D Votes" "Elex D Votes" (BR.numberToStyledHtml "%d" . demVoters))
      <> C.headed "Elex Turnout" (BR.toCell cas "Elex Turnout" "Elex Turnout" (BR.numberToStyledHtml "%2.1f" . (100*) . rawTurnout))
      <> C.headed "AH Turnout Target" (BR.toCell cas "AH T Tgt" "AH T Tgt" (BR.numberToStyledHtml "%2.1f" . (100*) . ahTurnoutTarget))
      <> C.headed "CPS (PS) Turnout" (BR.toCell cas "CPS Turnout" "CPS Turnout" (BR.numberToStyledHtml "%2.1f" . (100*) . cpsTurnout))
      <> C.headed "CCES Raw Turnout" (BR.toCell cas "CCES Raw Turnout" "CCES Raw Turnout" (BR.numberToStyledHtml "%2.1f" . (100*) . ccesRawTurnout))
      <> C.headed "CCES (PS) Turnout" (BR.toCell cas "CCES Turnout" "CCES Turnout" (BR.numberToStyledHtml "%2.1f" . (100*) . ccesTurnout))
      <> C.headed "Modeled Turnout" (BR.toCell cas "M Turnout" "M Turnout" (BR.numberToStyledHtml "%2.1f" . (100*) . mTurnout))
      <> C.headed "Raw 2-party D Share" (BR.toCell cas "Raw D Share" "Raw D Share" (BR.numberToStyledHtml "%2.1f" . (100*) . rawDShare))
      <> C.headed "CCES (PS) 2-party D Share" (BR.toCell cas "CCES D Share" "CCES D Share" (BR.numberToStyledHtml "%2.1f" . (100*) . ccesDShare))
      <> C.headed "Modeled 2-party D Pref" (BR.toCell cas "M Share" "M Share" (BR.numberToStyledHtml "%2.1f" . (100*) . mPref))
      <> C.headed "Modeled 2-party D Share" (BR.toCell cas "M Share" "M Share" (BR.numberToStyledHtml "%2.1f" . (100*) . mShare))
      <> C.headed "Modeled 2-party D Diff" (BR.toCell cas "M Diff" "M Diff" (BR.numberToStyledHtml "%2.1f" . (100*) . mDiff))

cdDiagTableColonnade cas =
  let state' = F.rgetField @DT.StateAbbreviation
      dist = F.rgetField @BR.CongressionalDistrict
      mTurnout = MT.ciMid . F.rgetField @BRE.ModeledTurnout
      mPref = MT.ciMid . F.rgetField @BRE.ModeledPref
      mShare = MT.ciMid . F.rgetField @BRE.ModeledShare
      mDiff r = let x = mShare r in (2 * x - 1)
      elexCVAP = F.rgetField @PUMS.Citizens
      voters = F.rgetField @BRE.TVotes
      demVoters = F.rgetField @BRE.DVotes
      repVoters = F.rgetField @BRE.RVotes
      ratio x y = realToFrac @_ @Double x / realToFrac @_ @Double y
      rawTurnout r = ratio (voters r) (elexCVAP r)
--      ccesRawTurnout r = realToFrac @_ @Double (F.rgetField @BRE.Voted r) / realToFrac @_ @Double (F.rgetField @BRE.Surveyed r)
--      ccesTurnout  r = ratio (F.rgetField @BRE.PSVoted r) (cvap r)
      rawDShare r = ratio (demVoters r) (demVoters r + repVoters r)
--      ccesDShare r = ratio (F.rgetField @BRE.PresDVotes r) (F.rgetField @BRE.PresDVotes r + F.rgetField @BRE.PresRVotes r)
  in  C.headed "State" (BR.toCell cas "State" "State" (BR.textToStyledHtml . state'))
      <> C.headed "District" (BR.toCell cas "State" "State" (BR.textToStyledHtml . show . dist))
      <> C.headed "Elex CVAP" (BR.toCell cas "Elex CVAP" "Elex CVAP" (BR.numberToStyledHtml "%d" . elexCVAP))
      <> C.headed "Elex Votes" (BR.toCell cas "Elex Votes" "Votes" (BR.numberToStyledHtml "%d" . voters))
      <> C.headed "Elex Dem Votes" (BR.toCell cas "Elex D Votes" "Elex D Votes" (BR.numberToStyledHtml "%d" . demVoters))
      <> C.headed "Elex Turnout" (BR.toCell cas "Elex Turnout" "Elex Turnout" (BR.numberToStyledHtml "%2.1f" . (100*) . rawTurnout))
--      <> C.headed "CCES Raw Turnout" (BR.toCell cas "CCES Raw Turnout" "CCES Raw Turnout" (BR.numberToStyledHtml "%2.1f" . (100*) . ccesRawTurnout))
--      <> C.headed "CCES (PS) Turnout" (BR.toCell cas "CCES Turnout" "CCES Turnout" (BR.numberToStyledHtml "%2.1f" . (100*) . ccesTurnout))
      <> C.headed "Modeled Turnout" (BR.toCell cas "M Turnout" "M Turnout" (BR.numberToStyledHtml "%2.1f" . (100*) . mTurnout))
      <> C.headed "Raw 2-party D Share" (BR.toCell cas "Raw D Share" "Raw D Share" (BR.numberToStyledHtml "%2.1f" . (100*) . rawDShare))
--      <> C.headed "CCES (PS) 2-party D Share" (BR.toCell cas "CCES D Share" "CCES D Share" (BR.numberToStyledHtml "%2.1f" . (100*) . ccesDShare))
      <> C.headed "Modeled 2-party D Pref" (BR.toCell cas "M Share" "M Share" (BR.numberToStyledHtml "%2.1f" . (100*) . mPref))
      <> C.headed "Modeled 2-party D Share" (BR.toCell cas "M Share" "M Share" (BR.numberToStyledHtml "%2.1f" . (100*) . mShare))
      <> C.headed "Modeled 2-party D Diff" (BR.toCell cas "M Diff" "M Diff" (BR.numberToStyledHtml "%2.1f" . (100*) . mDiff))


houseChar :: ET.DistrictType -> Text
houseChar ET.StateLower = "L"
houseChar ET.StateUpper = "U"
houseChar ET.Congressional = "C"

newStateLegMapPosts :: forall r. (K.KnitMany r, BR.CacheEffects r) => BR.CommandLine -> K.Sem r ()
newStateLegMapPosts cmdLine = do
  ccesAndCPSEM_C <-  BRE.prepCCESAndCPSEM False
  acs_C <- BRE.prepACS False
  proposedSLDs_C <- prepCensusDistrictData False "model/NewMaps/newStateLegDemographics.bin" =<< BRC.censusTablesFor2022SLDs
  proposedCDs_C <- prepCensusDistrictData False "model/newMaps/newCDDemographicsDR.bin" =<< BRC.censusTablesForProposedCDs
  drSLDPlans <- Redistrict.allPassedSLDPlans
  drCDPlans <- Redistrict.allPassedCongressionalPlans
  let dType r = F.rgetField @ET.DistrictTypeC r
      dName r = toString $ F.rgetField @ET.DistrictName r
      regSLDPost pi' sa contested' interestingOnly houses desc spDists' = do
        let houseList = Set.toList houses
        paPaths <- postPaths (sa <> "_StateLeg") cmdLine
        BR.brNewPost paPaths pi' (sa <> "_SLD") $ do
          let overlapF h = (h,) <$> DO.loadOverlapsFromCSV (toString $ "data/districtOverlaps/" <> sa <> "_SLD" <> houseChar h <> "_CD.csv") sa h ET.Congressional
          overlapsL <- traverse overlapF houseList
          let draF h = K.ignoreCacheTimeM $ Redistrict.lookupAndLoadRedistrictingPlanAnalysis drSLDPlans (Redistrict.redistrictingPlanId sa "Passed" h)
          sldDRAs <- traverse draF houseList
          cdDRA <- K.ignoreCacheTimeM $ Redistrict.lookupAndLoadRedistrictingPlanAnalysis drCDPlans (Redistrict.redistrictingPlanId sa "Passed" ET.Congressional)

          let postSpec = NewSLDMapsPostSpec sa desc paPaths (mconcat sldDRAs) cdDRA (M.fromList overlapsL) contested' spDists'
          newStateLegMapAnalysis cmdLine postSpec interestingOnly
            (K.liftActionWithCacheTime ccesAndCPSEM_C)
            (K.liftActionWithCacheTime acs_C)
            (K.liftActionWithCacheTime $ fmap (FL.fold postStratRollupFld . fmap F.rcast . onlyState sa) proposedCDs_C)
            (K.liftActionWithCacheTime $ fmap (FL.fold postStratRollupFld . fmap F.rcast . onlyState sa) proposedSLDs_C)

  let bothHouses = Set.fromList [ET.StateUpper, ET.StateLower]

      postInfoNC = BR.PostInfo (BR.postStage cmdLine) (BR.PubTimes BR.Unpublished Nothing)
      contestedNC = const True
  regSLDPost postInfoNC "NC" contestedNC True bothHouses "StateBoth" []

  {-
  let postInfoNM = BR.PostInfo (BR.postStage cmdLine) (BR.PubTimes BR.Unpublished Nothing)
      contestedNM r = F.rgetField @ET.DistrictTypeC r == ET.StateLower -- state senate every four years, 2024 next
  regSLDPost postInfoNM "NM" contestedNM True bothHouses "StateBoth"

  -- GA senate is 2-year terms
  let postInfoGA = BR.PostInfo (BR.postStage cmdLine) (BR.PubTimes BR.Unpublished Nothing)
      contestedGA = const True
  regSLDPost postInfoGA "GA" contestedGA True bothHouses "StateBoth"
-}
  -- PA senate seats are even numbered in mid-term years
  let postInfoPA = BR.PostInfo (BR.postStage cmdLine) (BR.PubTimes BR.Unpublished Nothing)
      contestedPA r = dType r == ET.StateLower || fromMaybe True (((==0) . flip mod 2) <$> readMaybe @Int (dName r))
      spDistsPA = fmap (\x -> (ET.StateLower,x)) ["88", "142", "144", "160"]
                  <> fmap (\x -> (ET.StateUpper,x))  ["14", "24"]
  regSLDPost postInfoPA "PA" contestedPA True (Set.fromList [ET.StateUpper, ET.StateLower]) "StateBoth" spDistsPA


  -- MI senate is 4-year terms, all elected in *mid-term* years. So all in 2022 and none in 2024.
  let postInfoMI = BR.PostInfo (BR.postStage cmdLine) (BR.PubTimes BR.Unpublished Nothing)
      contestedMI = const True
      spDistsMI = ((ET.StateLower,) <$> ["38", "46", "54", "58", "81", "84"])
                  <> ((ET.StateUpper,) <$> ["11", "12", "30", "35"])
  regSLDPost postInfoMI "MI" contestedMI True bothHouses "StateBoth" spDistsMI

  -- NB: AZ has only one set of districts.  Upper and lower house candidates run in the same districts!
  let postInfoAZ = BR.PostInfo (BR.postStage cmdLine) (BR.PubTimes BR.Unpublished Nothing)
      contestedAZ = const True -- correct here
  regSLDPost postInfoAZ "AZ" contestedAZ False (Set.fromList [ET.StateUpper]) "StateUpper" []


  let postInfoNH = BR.PostInfo (BR.postStage cmdLine) (BR.PubTimes BR.Unpublished Nothing)
      contestedNH = const True
  regSLDPost postInfoNH "NH" contestedNH True bothHouses "StateBoth" []


addRace5 :: (F.ElemOf rs DT.RaceAlone4C, F.ElemOf rs DT.HispC) => F.Record rs -> F.Record (rs V.++ '[DT.Race5C])
addRace5 r = r F.<+> (FT.recordSingleton @DT.Race5C $ DT.race5FromRaceAlone4AndHisp True (F.rgetField @DT.RaceAlone4C r) (F.rgetField @DT.HispC r))

addCount :: (F.ElemOf rs PUMS.Citizens) => F.Record rs -> F.Record (rs V.++ '[BRC.Count])
addCount r = r F.<+> (FT.recordSingleton @BRC.Count $ F.rgetField @PUMS.Citizens r)

addDistrict :: (F.ElemOf rs ET.CongressionalDistrict) => F.Record rs -> F.Record (rs V.++ '[ET.DistrictTypeC, ET.DistrictName])
addDistrict r = r F.<+> ((ET.Congressional F.&: show (F.rgetField @ET.CongressionalDistrict r) F.&: V.RNil) :: F.Record [ET.DistrictTypeC, ET.DistrictName])

fixACS :: F.Record BRE.PUMSWithDensityEM -> F.Record PostStratR
fixACS = F.rcast . addRace5 . addDistrict . addCount

postStratRollupFld :: FL.Fold (F.Record PostStratR) (F.FrameRec PostStratR)
postStratRollupFld = FMR.concatFold
                     $ FMR.mapReduceFold
                     FMR.noUnpack
                     (FMR.assignKeysAndData
                      @[BR.StateAbbreviation, ET.DistrictTypeC, ET.DistrictName,DT.SexC, DT.CollegeGradC, DT.Race5C]
                      @[DT.PopPerSqMile, BRC.Count])
                     (FMR.foldAndAddKey innerFld)
  where
    innerFld :: FL.Fold (F.Record [DT.PopPerSqMile, BRC.Count]) (F.Record [DT.PopPerSqMile, BRC.Count])
    innerFld =
      let dFold = BRE.wgtdGMeanF (realToFrac . F.rgetField @BRC.Count) (F.rgetField @DT.PopPerSqMile) --fmap (fromMaybe 0) (FL.premap (F.rgetField @DT.PopPerSqMile) FL.last)
          cFold = FL.premap (F.rgetField @BRC.Count) FL.sum
      in (\d c -> d F.&: c F.&: V.RNil) <$> dFold <*> cFold

peopleWeightedLogDensityFld :: (F.ElemOf rs DT.PopPerSqMile)
                            => (F.Record rs -> Int)
                            -> FL.Fold (F.Record rs) Double
peopleWeightedLogDensityFld ppl =
  let dens = F.rgetField @DT.PopPerSqMile
      x r = if dens r >= 1 then realToFrac (ppl r) * Numeric.log (dens r) else 0
      safeDivInt y n = if n == 0 then 0 else y / realToFrac n
      fld = safeDivInt <$> FL.premap x FL.sum <*> fmap realToFrac (FL.premap ppl FL.sum)
  in fld

--peopleWeightedLogDensityFld' :: (F.ElemOf rs DT.PopPerSqMile, F.ElemOf)

pwldByStateFld :: (F.ElemOf rs BR.StateAbbreviation, F.ElemOf rs DT.PopPerSqMile)
               => (F.Record rs -> Int)
               -> FL.Fold (F.Record rs) (Map Text Double)
pwldByStateFld ppl = fmap M.fromList
                  $ MR.mapReduceFold
                  MR.noUnpack
                  (MR.assign (F.rgetField @BR.StateAbbreviation) id)
                  (MR.foldAndLabel (peopleWeightedLogDensityFld ppl) (,))

pwldByDistrictFld :: forall rs. (F.ElemOf rs BR.StateAbbreviation
                                , F.ElemOf rs ET.DistrictName
                                , F.ElemOf rs ET.DistrictTypeC
                                , F.ElemOf rs DT.PopPerSqMile
                                , rs F.⊆ rs
                                )
                  => (F.Record rs -> Int)
                  -> FL.Fold (F.Record rs) (F.FrameRec [BR.StateAbbreviation, ET.DistrictTypeC, ET.DistrictName, DT.PopPerSqMile])
pwldByDistrictFld ppl = FMR.concatFold
                        $ FMR.mapReduceFold
                        FMR.noUnpack
                        (FMR.assignKeysAndData @[BR.StateAbbreviation, ET.DistrictTypeC, ET.DistrictName])
                        (FMR.foldAndAddKey (fmap (FT.recordSingleton @DT.PopPerSqMile) $ peopleWeightedLogDensityFld ppl))


gradByDistrictFld ::  (F.ElemOf rs BR.StateAbbreviation
                      , F.ElemOf rs ET.DistrictTypeC
                      , F.ElemOf rs ET.DistrictName
                      , F.ElemOf rs DT.PopPerSqMile
                      , rs F.⊆ rs
                      )
                  => (F.Record rs -> Int)
                  -> (F.Record rs -> Double)
                  -> FL.Fold (F.Record rs) (F.FrameRec [BR.StateAbbreviation, ET.DistrictTypeC, ET.DistrictName, BRE.FracGrad])
gradByDistrictFld ppl f =
  let fracPpl = realToFrac . ppl
      gradFld ::  (F.Record rs -> Double) -> (F.Record rs -> Double) -> FL.Fold (F.Record rs) Double
      gradFld wgt x = (/) <$> FL.premap (\r -> wgt r * x r) FL.sum <*> FL.premap wgt FL.sum
  in FMR.concatFold
     $ FMR.mapReduceFold
     FMR.noUnpack
     (FMR.assignKeysAndData @[BR.StateAbbreviation, ET.DistrictTypeC, ET.DistrictName])
     (FMR.foldAndAddKey (fmap (FT.recordSingleton @BRE.FracGrad) $ gradFld fracPpl f))

gradOfWhiteByDistrictFld ::  (F.ElemOf rs BR.StateAbbreviation
                             , F.ElemOf rs ET.DistrictName
                             , F.ElemOf rs ET.DistrictTypeC
                             , rs F.⊆ rs
                             )
  => (F.Record rs -> Int)
  -> (F.Record rs -> Bool)
  -> (F.Record rs -> Bool)
  -> FL.Fold (F.Record rs) (F.FrameRec [BR.StateAbbreviation, ET.DistrictTypeC, ET.DistrictName, BRE.FracWhiteNonHispanic, BRE.FracGradOfWhite])
gradOfWhiteByDistrictFld ppl isWhite' isGrad =
  let wgt = realToFrac . ppl
      isWhiteGrad' r = isWhite' r && isGrad r
      wFld = (/) <$> (FL.prefilter isWhite' $ FL.premap wgt FL.sum) <*> (FL.premap wgt FL.sum)
      wGradFld = (/) <$> (FL.prefilter isWhiteGrad' $ FL.premap wgt FL.sum) <*> (FL.prefilter isWhite' $ FL.premap wgt FL.sum)
      mkRecord :: Double -> Double -> F.Record [BRE.FracWhiteNonHispanic, BRE.FracGradOfWhite]
      mkRecord x y = x F.&: y F.&: V.RNil
  in FMR.concatFold
     $ FMR.mapReduceFold
     FMR.noUnpack
     (FMR.assignKeysAndData @[BR.StateAbbreviation, ET.DistrictTypeC, ET.DistrictName])
     (FMR.foldAndAddKey (mkRecord <$> wFld <*> wGradFld))


pwldFC :: (F.ElemOf ds DT.PopPerSqMile, F.ElemOf ds BRC.Count) => DCC.FoldComponent ds DT.PopPerSqMile
pwldFC = DCC.FoldComponent $ peopleWeightedLogDensityFld (F.rgetField @BRC.Count)

isWhite :: F.ElemOf rs DT.Race5C => F.Record rs -> Bool
isWhite r = F.rgetField @DT.Race5C r == DT.R5_WhiteNonHispanic

censusWgt :: F.ElemOf rs BRC.Count => F.Record rs -> Double
censusWgt = realToFrac . F.rgetField @BRC.Count

censusWgtSumFld :: F.ElemOf rs BRC.Count => FL.Fold (F.Record rs) Double
censusWgtSumFld = FL.premap censusWgt FL.sum

wFC :: (F.ElemOf ds DT.Race5C, F.ElemOf ds BRC.Count) => DCC.FoldComponent ds BRE.FracWhiteNonHispanic
wFC = DCC.FoldComponent $ (/) <$> (FL.prefilter isWhite $ censusWgtSumFld) <*> censusWgtSumFld

isWhiteGrad :: (F.ElemOf rs DT.Race5C, F.ElemOf rs DT.CollegeGradC) => F.Record rs -> Bool
isWhiteGrad r = isWhite r && F.rgetField @DT.CollegeGradC r == DT.Grad

wgFC :: (F.ElemOf ds DT.CollegeGradC, F.ElemOf ds DT.Race5C, F.ElemOf ds BRC.Count) => DCC.FoldComponent ds BRE.FracGradOfWhite
wgFC = DCC.FoldComponent $ (/) <$> (FL.prefilter isWhiteGrad $ censusWgtSumFld) <*> (FL.prefilter isWhite censusWgtSumFld)

rescaleDensity :: (F.ElemOf rs DT.PopPerSqMile, Functor f)
               => Double
               -> f (F.Record rs)
               -> f (F.Record rs)
rescaleDensity s = fmap g
  where
    g = FT.fieldEndo @DT.PopPerSqMile (*s)


capa :: forall r. (K.KnitMany r, BR.CacheEffects r) => BR.CommandLine -> K.Sem r ()
capa cmdLine = do
  K.logLE K.Info "Rebuilding CAPA post (if necessary)."
  let postInfo = BR.PostInfo (BR.postStage cmdLine) (BR.PubTimes BR.Unpublished  Nothing)
      state' = F.rgetField @BR.StateAbbreviation
      distr = F.rgetField @ET.DistrictName
      histDS = F.rgetField @TwoPartyDShare
--      modelDS = MT.ciMid . F.rgetField @BRE.ModeledShare
--      modelT = MT.ciMid . F.rgetField @BRE.ModeledTurnout
      bipoc r = 1 - F.rgetField @BRE.FracWhiteNonHispanic r
      density = F.rgetField @DT.PopPerSqMile
  capaPaths <- postPaths "CAPA" cmdLine
  (modelAndDRWith_C, _) <- newCDModeled cmdLine
  let senateStates = Set.fromList ["CO", "AZ", "GA", "NV", "NH", "PA", "FL", "NC", "WI"]
      capaFilter r = not (state' r `Set.member` senateStates)
                     && histDS r >= 0.47
                     && histDS r <= 0.53
                     && Numeric.exp (density r) < 2000
                     && bipoc r >= 0.25
  capaCDs <- (FL.fold FL.list . F.filterFrame capaFilter) <$> K.ignoreCacheTime modelAndDRWith_C
  -- get overlapping SLDs
  let capaStates = Set.fromList $ fmap (F.rgetField @BR.StateAbbreviation) capaCDs
      capaStates' = Set.delete "NJ" capaStates -- no state-leg elections in NJ
  capaOverlapsL <- DO.overlapCollection capaStates' (\sa -> toString $ "data/districtOverlaps/" <> sa <> "_SLDL" <> "_CD.csv") ET.StateLower ET.Congressional
  capaOverlapsU <- DO.overlapCollection capaStates' (\sa -> toString $ "data/districtOverlaps/" <> sa <> "_SLDU" <> "_CD.csv") ET.StateUpper ET.Congressional
  let overlappingSLDs' threshold cdn overlaps' = do
        let --numLD = Vector.length $ DO.populations overlaps'
            pwo = Vector.zip (DO.populations overlaps') (DO.overlaps overlaps')
            nameByRow = fmap fst $ sortOn snd $ M.toList $ DO.rowByName overlaps'
            intDiv x y = realToFrac x/ realToFrac y
            olFracM (pop, ols) = fmap (\x -> intDiv x pop) $ M.lookup cdn ols
            olFracsM = traverse olFracM pwo
        olFracs <- K.knitMaybe ("Missing CD in overlap data for " <> cdn) olFracsM
        let namedOlFracs = zip nameByRow (Vector.toList olFracs)
        return $ filter ((>=threshold) . snd) namedOlFracs

      overlappingSLDs threshold sa cdn = do
        overlapsU <- K.knitMaybe ("capa: Failed ot find opverlap data for upper chamber of " <> sa) $ M.lookup sa capaOverlapsU
        overlapsL <- K.knitMaybe ("capa: Failed ot find opverlap data for lower chamber of " <> sa) $ M.lookup sa capaOverlapsL
        namedOverU <- overlappingSLDs' threshold cdn overlapsU
        namedOverL <- overlappingSLDs' threshold cdn overlapsL
        return $ fmap (\(n,x) -> (ET.StateUpper, n, x)) namedOverU
          <>  fmap (\(n,x) -> (ET.StateLower, n, x)) namedOverL

      f r = fmap ((state' r, distr r),) $ overlappingSLDs 0.5 (state' r) (distr r)
  overlapsByCD <- traverse f $ filter ((/= "NJ") . F.rgetField @BR.StateAbbreviation) capaCDs -- NJ state-leg elecitons in odd years
  let olList = concatMap (\(k,slds) -> fmap (q k) slds) overlapsByCD
        where q k (dt, dn, x) = (k, (dt, dn), x)
--      flattenOL ((sa, dn), slds) = fmap ((sa, dn),) $ fmap (\(dt, dn, x) -> ((dt, dn) , x)) slds
--      flattenedOLs = concatMap flattenOL overlapsByCD
      sldsToModel' ((sa, _), slds) = fmap (sa,) $ fmap (\(dt, dn, _) -> (dt, dn)) slds
      sldsToModel = Set.fromList $ concatMap sldsToModel' overlapsByCD
      sldsToModelFilter r = (state' r, (F.rgetField @ET.DistrictTypeC r, distr r)) `Set.member` sldsToModel
  K.logLE K.Info $ "Overlaps: " <> show olList
  -- FIX: We should do density rescaling here
  proposedSLDs_C <- F.filterFrame sldsToModelFilter <<$>> (prepCensusDistrictData False "model/NewMaps/newStateLegDemographics.bin" =<< BRC.censusTablesFor2022SLDs)
  ccesAndCPSEM_C <-  BRE.prepCCESAndCPSEM False
  let ccesAndCPS2020_C = fmap (BRE.ccesAndCPSForYears [2020]) ccesAndCPSEM_C
      stanParams = SC.StanMCParameters 4 4 (Just 1000) (Just 1000) (Just 0.8) (Just 10) Nothing
      mapGroup :: SB.GroupTypeTag (F.Record CDLocWStAbbrR) = SB.GroupTypeTag "CD"
      postStratInfo = (mapGroup, "CAPA_SLDs")
      model :: K.ActionWithCacheTime r (F.FrameRec PostStratR) -> K.Sem r (F.FrameRec (BRE.ModelResultsR CDLocWStAbbrR))
      model x = K.ignoreCacheTimeM $ BRE.electionModelDM False cmdLine False (Just stanParams) modelDir modelVariant 2020 postStratInfo ccesAndCPS2020_C x
  modeledSLDs <- model $ fmap (FL.fold postStratRollupFld . fmap F.rcast) proposedSLDs_C
  BR.logFrame modeledSLDs
  let cdDataMap = FL.fold (FL.premap (\r -> ((state' r, distr r), r)) FL.map) capaCDs
      addCDDataM (k, y, z) = fmap (, y, z) $ M.lookup k cdDataMap
      dType = F.rgetField @ET.DistrictTypeC
      sldDataMap = FL.fold (FL.premap (\r -> ((state' r, dType r, distr r), r)) FL.map) modeledSLDs
      addSLDDataM (cd, (dt, dn), z) = fmap (cd, , z) $ M.lookup (state' cd, dt, dn) sldDataMap
  olList1 <- K.knitMaybe "capa: Missing CD in cd data map" $ traverse addCDDataM olList
  olList2 <- K.knitMaybe "capa: Missing SLD in sld data map" $ traverse addSLDDataM olList1
  drSLDPlans <- Redistrict.allPassedSLDPlans
  let sldSet = Set.fromList $ fmap (\(cd, sld, _) -> (state' cd, dType sld)) olList2
      draF sa h = FL.fold FL.list <$> (K.ignoreCacheTimeM $ Redistrict.lookupAndLoadRedistrictingPlanAnalysis drSLDPlans (Redistrict.redistrictingPlanId sa "Passed" h))
  sldDRAs' <- concat <$> (traverse (uncurry draF) $ Set.toList sldSet)
  let sld2PSMap = FL.fold (FL.premap (\r -> (F.rcast @[BR.StateAbbreviation, ET.DistrictTypeC, ET.DistrictName] r, F.rgetField @TwoPartyDShare r)) FL.map)
                            $ fmap addTwoPartyDShare sldDRAs'
      addSLDDAM (cd, sld, ol) = (cd, sld, ol,) <$> M.lookup (state' cd F.&: dType sld F.&: distr sld F.&: V.RNil) sld2PSMap
  olList3 <- K.knitMaybe "capa: Missing Dave's analysis for an sld" $ traverse addSLDDAM olList2
  let sldFilter (_, _, _, hs) = hs >= 0.47 && hs <= 0.53
      olList4 = filter sldFilter olList3
--  let (capaCDsAndSLDs, _) = FJ.leftJoinWithMissing @[BR.StateAbbreviation, BR.] (FrameRec bs)
  BR.brNewPost capaPaths postInfo "CAPA" $ do
    BR.brAddRawHtmlTable
      "Possible CDs"
      (BHA.class_ "brTable")
      (capaCDColonnade mempty)
      (reverse $ sortOn bipoc capaCDs)
    when False -- so this is switchabel without losing inference
      $ BR.brAddRawHtmlTable
      "Competitive Underlying SLDs With >50% in CD"
      (BHA.class_ "brTable")
      (capaSLDColonnade sld2PSMap mempty)
      olList4

  --BR.logFrame capaCDs


capaSLDColonnade _ cas =
  let cdData (r, _, _, _) = r
      state' = F.rgetField @BR.StateAbbreviation . cdData
      cd = F.rgetField @ET.DistrictName . cdData
      histDS = round @_ @Int . (100*) . F.rgetField @TwoPartyDShare . cdData
      modelDS_CD = round @_ @Int . (100 *) . MT.ciMid . F.rgetField @BRE.ModeledShare . cdData
--      modelT_CD = round @_ @Int . (100 *) . MT.ciMid . F.rgetField @BRE.ModeledTurnout . cdData
      bipoc y = 1 - F.rgetField @BRE.FracWhiteNonHispanic (cdData y)
      density = F.rgetField @DT.PopPerSqMile . cdData
      sldData (_, y, _, _) = y
      dType = F.rgetField @ET.DistrictTypeC . sldData
      dName = F.rgetField @ET.DistrictName . sldData
      overlapFrac (_, _, x, _) = x
      sld y = show (dType y) <> "-" <> dName y
      modelDS_SLD = round @_ @Int . (100 *) . MT.ciMid . F.rgetField @BRE.ModeledShare . sldData
--      modelT_SLD = round @_ @Int . (100 *) . MT.ciMid . F.rgetField @BRE.ModeledTurnout . sldData
      hs (_, _, _, d) = round @_ @Int (100 * d)
  in  C.headed "State" (BR.toCell cas "State" "State" (BR.textToStyledHtml . state'))
      <> C.headed "Congressional District" (BR.toCell cas "District" "District" (BR.textToStyledHtml . cd))
      <> C.headed "Historical Model (Dave's Redistricting)" (BR.toCell cas "Historical" "Historical" (BR.numberToStyledHtml "%d" . histDS))
      <> C.headed "Model D Share" (BR.toCell cas "Demographic" "Demographic" (BR.numberToStyledHtml "%d" . modelDS_CD))
--      <> C.headed "Model Turnout" (BR.toCell cas "Demographic" "Demographic" (BR.numberToStyledHtml "%d" . modelT_CD))
      <> C.headed "%Non-White" (BR.toCell cas "%NW" "%NW" (BR.numberToStyledHtml "%d" . round @_ @Int . (100*) . bipoc))
      <> C.headed "Pop/SqMile" (BR.toCell cas "P/SqMi" "P/SqMi" (BR.numberToStyledHtml "%2.0f" . Numeric.exp . density))
      <> C.headed "State-Leg District" (BR.toCell cas "District" "District" (BR.textToStyledHtml . sld))
      <> C.headed "%SLD in CD" (BR.toCell cas "Overlap" "Overlap" (BR.numberToStyledHtml "%d" . round @_ @Int . (100*) . overlapFrac))
--      <> C.headed "Model Turnout" (BR.toCell cas "SLD Turnout" "SLD Turnout" (BR.numberToStyledHtml "%d" . modelT_SLD))
      <> C.headed "SLD Historical (Dave's)" (BR.toCell cas "SLD Historical" "SLD Historical" (BR.numberToStyledHtml "%d" . hs))
      <> C.headed "Model D Share" (BR.toCell cas "SLD DShare" "SLD DShare" (BR.numberToStyledHtml "%d" . modelDS_SLD))

capaCDColonnade cas =
  let state' = F.rgetField @BR.StateAbbreviation
      distr = F.rgetField @ET.DistrictName
      histDS = round @_ @Int . (100*) . F.rgetField @TwoPartyDShare
      modelDS = round @_ @Int . (100 *) . MT.ciMid . F.rgetField @BRE.ModeledShare
--      modelT = round @_ @Int . (100 *) . MT.ciMid . F.rgetField @BRE.ModeledTurnout
      bipoc r = 1 - F.rgetField @BRE.FracWhiteNonHispanic r
      density = F.rgetField @DT.PopPerSqMile
  in  C.headed "State" (BR.toCell cas "State" "State" (BR.textToStyledHtml . state'))
      <> C.headed "District" (BR.toCell cas "District" "District" (BR.textToStyledHtml . distr))
      <> C.headed "Historical Model (Dave's Redistricting)" (BR.toCell cas "Historical" "Historical" (BR.numberToStyledHtml "%d" . histDS))
      <> C.headed "Model D Share" (BR.toCell cas "Demographic" "Demographic" (BR.numberToStyledHtml "%d" . modelDS))
--      <> C.headed "Model Turnout" (BR.toCell cas "Demographic" "Demographic" (BR.numberToStyledHtml "%d" . modelT))
      <> C.headed "%Non-White" (BR.toCell cas "%NW" "%NW" (BR.numberToStyledHtml "%d" . round @_ @Int . (100*) . bipoc))
      <> C.headed "Pop/SqMile" (BR.toCell cas "P/SqMi" "P/SqMi" (BR.numberToStyledHtml "%2.0f" . Numeric.exp . density))

type ModeledWithDRAndAggDemo = [BR.Year, BR.StateAbbreviation, ET.DistrictTypeC, ET.DistrictName
                               , BRE.ModelDesc, BRE.ModeledTurnout, BRE.ModeledPref, BRE.ModeledShare, BR.PlanName
                               , BR.Population, ET.DemShare, ET.RepShare, BR.OthShare
                               , ET.VAP, TwoPartyDShare, DT.PopPerSqMile, BRE.FracWhiteNonHispanic, BRE.FracGradOfWhite]

type RescaledProposedCD = [BR.StateAbbreviation, ET.DistrictTypeC, ET.DistrictName
                          , BR.Population, DT.PWPopPerSqMile, DT.SexC
                          , DT.CollegeGradC, DT.RaceAlone4C, DT.HispC, BRC.Count, DT.PopPerSqMile, DT.Race5C]


cachedRows fn dsn x = K.ignoreCacheTime x >>= (\y ->  K.logLE K.Diagnostic (fn <> ": " <> dsn <> "has " <> show (FL.fold FL.length y) <> " rows."))
cachedUnique fn dsn f x = K.ignoreCacheTime x >>= (\y ->  K.logLE K.Diagnostic (fn <> ": " <> dsn <> "has " <> show (Set.size $ FL.fold (FL.premap f FL.set) y) <> " uniques."))

newCDModeled :: forall r.(K.KnitEffects r, BR.CacheEffects r)
             => BR.CommandLine -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec ModeledWithDRAndAggDemo)
                                          , K.ActionWithCacheTime r (F.FrameRec RescaledProposedCD))
newCDModeled cmdLine = do
  K.logLE K.Info "Rebuilding all CD model results (if necessary)."
  ccesAndCPSEM_C <-  BRE.prepCCESAndCPSEM False
  acs_C <- BRE.prepACS False
  proposedCDs_C <- prepCensusDistrictData False "model/newMaps/newCDDemographicsDR.bin" =<< BRC.censusTablesForProposedCDs
  cachedRows "newCDModeled" "proposedCDs" proposedCDs_C
  cachedUnique "newCDModeled" "proposedCDs" (F.rcast @[BR.StateAbbreviation, ET.DistrictName]) proposedCDs_C
  let ccesAndCPS2020_C = fmap (BRE.ccesAndCPSForYears [2020]) ccesAndCPSEM_C
      acs2020_C = fmap (BRE.acsForYears [2020]) acs_C
      rescaleDeps = (,) <$> acs2020_C <*> proposedCDs_C
  rescaledProposed_C <- BR.retrieveOrMakeFrame "posts/newMaps/allCDs/rescaledProposed.bin" rescaleDeps $ \(acs, proposed) -> do
    let proposedPWLD = FL.fold (pwldByStateFld (F.rgetField @BRC.Count)) proposed
        acsPWLD = FL.fold (pwldByStateFld (F.rgetField @PUMS.Citizens)) acs
--        whenMissing _ _ = Nothing
        whenMatched _ acsPWLD' xPWLD = Numeric.exp (acsPWLD' - xPWLD)
        rescaleMap = M.merge M.dropMissing M.dropMissing (M.zipWithMatched whenMatched) acsPWLD proposedPWLD
        rescaleRow r = fmap (\s -> FT.fieldEndo @DT.PopPerSqMile (*s) r) $ M.lookup (F.rgetField @BR.StateAbbreviation r) rescaleMap
    F.toFrame <$> (K.knitMaybe "allCDsPost: missing key in traversal of proposed for density rescaling" $ traverse rescaleRow $ FL.fold FL.list proposed)
  cachedRows "newCDModeled" "rescaledProposed" rescaledProposed_C

  let mapGroup :: SB.GroupTypeTag (F.Record CDLocWStAbbrR) = SB.GroupTypeTag "CD"
      psInfoDM name = (mapGroup, name)
      stanParams = SC.StanMCParameters 4 4 (Just 1000) (Just 1000) (Just 0.8) (Just 10) Nothing

      modelDM :: Text -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec (BRE.ModelResultsR CDLocWStAbbrR)))
      modelDM name =
        BRE.electionModelDM False cmdLine False
        (Just stanParams) modelDir modelVariant 2020 (psInfoDM name)
        ccesAndCPS2020_C (fmap (F.rcast @PostStratR) <$> rescaledProposed_C)
  modeled_C <- modelDM ("All_New_CD")
  cachedRows "newCDModeled" "modeled" modeled_C
  drAnalysis <- K.ignoreCacheTimeM Redistrict.allPassedCongressional
  let deps = (,) <$> modeled_C <*> proposedCDs_C
  result_C <- BR.retrieveOrMakeFrame "posts/newMaps/allCDs/modelAndDRWith.bin" deps $ \(modeled, prop) -> do
    let (modelAndDR, missingDR) = FJ.leftJoinWithMissing @[BR.StateAbbreviation, ET.DistrictTypeC, ET.DistrictName] modeled (fmap addTwoPartyDShare drAnalysis)
    when (not $ null missingDR) $ K.knitError $ "allCDsPost: Missing keys in model/DR join=" <> show missingDR
    let densityAndGowFld = DCC.buildMRFold
                 @[BR.StateAbbreviation, ET.DistrictTypeC, ET.DistrictName]
                 @[BRC.Count,  DT.PopPerSqMile, DT.Race5C, DT.CollegeGradC]
                 (pwldFC V.:& wFC V.:& wgFC V.:& V.RNil)
        (withDensityAndGow, missingDGW) = FJ.leftJoinWithMissing @[BR.StateAbbreviation, ET.DistrictTypeC, ET.DistrictName]
                                           modelAndDR
                                           (FL.fold densityAndGowFld prop)
    when (not $ null missingDGW) $ K.knitError $ "allCDsPost: missing keys in modelWithDensity/FracGrad join=" <> show missingDGW
    return withDensityAndGow
  return $ (F.rcast <<$>> result_C, F.rcast <<$>> rescaledProposed_C)


type OldCDOverlap = "OldCDOverlap" F.:-> Text

allCDsPost :: forall r. (K.KnitMany r, BR.CacheEffects r) => BR.CommandLine -> K.Sem r ()
allCDsPost cmdLine = K.wrapPrefix "allCDsPost" $ do
  K.logLE K.Info "Rebuilding AllCDs post (if necessary)."
  let postInfo = BR.PostInfo (BR.postStage cmdLine) (BR.PubTimes BR.Unpublished  Nothing)
      state' = F.rgetField @BR.StateAbbreviation
      distr = F.rgetField @ET.DistrictName
  allCDsPaths <- sawbuckPostPaths "SawbuckPatriots" cmdLine
  (modelAndDRWith_C, rescaledProposed_C) <- newCDModeled cmdLine
  overlaps' <- DO.oldCDOverlapCollection
  let oldCDOverlapsE :: (F.ElemOf rs BR.StateAbbreviation, F.ElemOf rs ET.DistrictName) => F.Record rs -> Either Text Text
      oldCDOverlapsE r = do
        stateOverlaps <- maybe (Left $ "Failed to find stateAbbreviation=" <> state' r <> " in " <> show overlaps') Right
                         $ M.lookup (state' r) overlaps'
        cdOverlaps <- maybe (Left $ "Failed to find dist=" <> distr r <> " in " <> state' r <> " in " <> show overlaps') Right
                      $ DO.overlapsOverThresholdForRowByName 0.5 stateOverlaps (distr r)
        return $ T.intercalate "," . fmap (\(dn, fo) -> dn <> " (" <> show (round (100 * fo)) <> "%)") . M.toList $ cdOverlaps

  modelAndDRWith <- K.ignoreCacheTime modelAndDRWith_C
  K.logLE K.Diagnostic $ "allCDsPost: modelAndDRWith has " <> show (FL.fold FL.length modelAndDRWith) <> " rows."
  let brDF r = brDistrictFramework DFLong DFUnk brShareRange draShareRangeCD (share50 r) (dave r)
  sortedFilteredModelAndDRA <- K.knitEither
                               $ F.toFrame
                               <$> (traverse (FT.mutateM (fmap (FT.recordSingleton @OldCDOverlap) . oldCDOverlapsE))
                                     $ sortOn brDF
                                     $ FL.fold FL.list modelAndDRWith
                                   )
  rp <- K.ignoreCacheTime rescaledProposed_C
  let (demoModelDRA, missing) = FJ.leftJoinWithMissing @[BR.StateAbbreviation, ET.DistrictTypeC, ET.DistrictName] rp sortedFilteredModelAndDRA
  when (not $ null missing) $ K.knitError $ "Missing keys in AllCDs acs and model/DRA join: " <> show missing
  districtsForClustering_C <- BR.retrieveOrMakeD "newMaps/allCDs/districtsForClustering.bin" (pure ())
                              $ const
                              $ pure
                              $ BRDC.districtsForClustering @[DT.SexC, DT.CollegeGradC, DT.Race5C, DT.PopPerSqMile] @BRC.Count $ fmap F.rcast demoModelDRA
  K.logLE K.Info $ "tSNE embedding..."
  let tsnePerplexity = 10
      tsneIters = [1000]
      tsnePerplexities = [tsnePerplexity]
      tsneLearningRates = [10]
--      tsneClusters = 5
  tsneResult_C <- BR.retrieveOrMakeFrame "mrp/DistrictClusters/tsne.bin" districtsForClustering_C $ \dfc -> do
    K.logLE K.Info $ "Running tSNE gradient descent for " <> (T.pack $ show tsneIters) <> " iterations."
    tsneMs <- TSNE.runTSNE
              (Just 1)
              BRDC.districtId
              (UVec.toList . BRDC.districtVec)
              tsnePerplexities
              tsneLearningRates
              tsneIters
              (\x -> (TSNE.tsneIteration2D_M x, TSNE.tsneCost2D_M x, TSNE.solutionToList $ TSNE.tsneSolution2D_M x))
              TSNE.tsne2D_S
              dfc
    let tSNERec :: TSNE.TSNEParams -> (Double, Double) -> F.Record [BRDC.TSNEPerplexity, BRDC.TSNELearningRate, BRDC.TSNEIters, BRDC.TSNE1, BRDC.TSNE2]
        tSNERec (TSNE.TSNEParams p lr n) (x, y) = p F.&: lr F.&: n F.&: x F.&: y F.&: V.RNil
        tSNERecs p = fmap (\(k, tSNEXY) -> k F.<+> tSNERec p tSNEXY) . M.toList
        fullTSNEResult =  mconcat $ fmap (\(p, solM) -> F.toFrame $ tSNERecs p solM) $ tsneMs
    pure fullTSNEResult
  tsneResult <- K.ignoreCacheTime tsneResult_C
  let (tsneWith, tsneWithMissing) = FJ.leftJoinWithMissing @[BR.StateAbbreviation, ET.DistrictName] tsneResult sortedFilteredModelAndDRA
  when (not $ null tsneWithMissing) $ K.knitError $ "Missing keys tsneWith join."
  let categoryNames = ["Density", "%Grad-Among-White", "%Voters-Of-Color"]
      categoryFunctions = zipWith DCC.SBCCategoryData categoryNames
                          [F.rgetField @DT.PopPerSqMile
                          , F.rgetField @BRE.FracGradOfWhite
                          , (\r -> 1 - F.rgetField @BRE.FracWhiteNonHispanic r)
                          ]
      quantileBreaks = DCC.sbcQuantileBreaks 20 categoryFunctions sortedFilteredModelAndDRA
      quantileFunctionsE = DCC.sbcQuantileFunctions quantileBreaks
      quantileFunctionsDblE = (fmap (realToFrac @Int @Double) .) <<$>> quantileFunctionsE
      partyFilters = let f = F.rgetField @TwoPartyDShare in DCC.SBCPartyData ((>= 0.5) . f) ((< 0.5) . f)
      partyMediansE = DCC.partyMedians partyFilters quantileFunctionsDblE sortedFilteredModelAndDRA
      partyRanksE = DCC.partyRanks partyFilters quantileFunctionsDblE sortedFilteredModelAndDRA
      partyLoHisE = DCC.partyLoHis <$> partyRanksE
  partyLoHis <- K.knitEither partyLoHisE
  partyMedians <- K.knitEither partyMediansE
  let stateAndPartyFilters sa =
        let f = F.rgetField @TwoPartyDShare
            inState r = F.rgetField @BR.StateAbbreviation r == sa
        in DCC.SBCPartyData (\r -> inState r && f r >= 0.5) (\r -> inState r && f r < 0.5)
      sApMediansE sa = DCC.partyMedians (stateAndPartyFilters sa) quantileFunctionsDblE sortedFilteredModelAndDRA
--      sApRanksE sa = DCC.partyRanks (stateAndPartyFilters sa) quantileFunctionsDblE sortedFilteredModelAndDRA
--      sApLoHisE sa = DCC.partyLoHis <$> (sApRanksE sa)
--      sApLoHisE sa = K.knitEither $ sApLoHisE sa
--      sApMediansE sa = K.knitEither $ sApMediansE sa

  let sbcE comp r = case comp of
        DCC.SBCNational -> DCC.sbcComparison quantileFunctionsDblE partyMedians r
        DCC.SBCState -> do
          medians <- sApMediansE (F.rgetField @BR.StateAbbreviation r)
          DCC.sbcComparison quantileFunctionsDblE medians r

--  putTextLn $ show dRanksSBD
--  putTextLn $ show rRanksSBD
--  BR.logFrame tsneResult
  let districtCompare x y =
        let
          st = F.rgetField @BR.StateAbbreviation
          dn = F.rgetField @ET.DistrictName
        in compare (st x)  (st y) <> ET.districtNameCompare (dn x) (dn y)

  BR.brNewPost allCDsPaths postInfo "AllCDs" $ do
    BR.brAddMarkDown "**NB: AK, DE, MT, ND, SD, VT and WY are missing since each has only one district and so no redistricting data was available."
    let fTable t ds = do
          (densQuantiles, gowQuantiles, nwQuantiles) <- case quantileFunctionsE of
            [DCC.SBCCategoryData _ q1, DCC.SBCCategoryData _ q2, DCC.SBCCategoryData _ q3] -> return (q1, q2, q3)
            _ -> K.knitError "Wrong number of quantile functions in allCDPost.fTable"
          when (not $ null ds) $ do
--            BR.logFrame ds
            BR.brAddRawHtmlTable
              t
              (BHA.class_ "brTable")
              (allCDsColonnade nwQuantiles gowQuantiles densQuantiles $ modelVsHistoricalTableCellStyle brShareRange draShareRangeCD)
              (sortBy districtCompare ds)
            let dists :: Set (F.Record [BR.StateAbbreviation, ET.DistrictName]) = Set.fromList $ fmap (F.rcast @[BR.StateAbbreviation, ET.DistrictName]) ds
            _<- K.addHvega Nothing Nothing
              $ BRV.demoCompareXYCS
              "District"
              "% non-white"
              "% white college grad"
              "Modeled D-Edge"
              "log density"
              (t <> " demographic scatter")
              (FV.ViewConfig 600 600 5)
              (FL.fold (xyFold2' sldDistLabel) $ F.filterFrame (\r -> F.rcast @[BR.StateAbbreviation, ET.DistrictName] r `Set.member` dists) demoModelDRA)
            pure ()
    K.logLE K.Info $ "sortedFilteredModelAndDRA has " <> show (FL.fold FL.length sortedFilteredModelAndDRA) <> " rows."
    categorized <- categorizeDistricts' (const True) brShareRange draShareRangeCD dCategories4 sortedFilteredModelAndDRA
    let withCategories = addCategoriesToRecords categorized
        (tsneWithCategories, _) = FJ.leftJoinWithMissing @[BR.StateAbbreviation, ET.DistrictTypeC, ET.DistrictName] tsneResult withCategories
    traverse_ (uncurry fTable) categorized
--    let getSBCs :: Monad m
--                => F.Record [BR.StateAbbreviation, ET.DistrictName]
--                -> StateT (Map (F.Record [BR.StateAbbreviation, ET.DistrictName]) (Either Text [SBComparison])) m (Either Text [SBComparison])
    let getSBCs comp d = do
          let dk = (F.rgetField @BR.StateAbbreviation d, F.rgetField @ET.DistrictName d)
          m <- PS.gets (getSBCMap comp)
          case M.lookup dk m of
            Just sbcs -> return sbcs
            Nothing -> do
              let sbcs = sbcE comp d
                  newMap = M.insert dk sbcs m
              PS.modify' $ updateSBCMap comp newMap
              return sbcs

    _ <- K.addHvega Nothing Nothing
      $ BRDC.tsneChartCat @DistCategory "All" "Category" (FV.ViewConfig 600 600 5) (fmap F.rcast tsneWithCategories)
    let distKey r = F.rgetField @BR.StateAbbreviation r <> "-"
                    <> (let dn = F.rgetField @ET.DistrictName r in if T.length dn == 1 then "0" <> dn else dn)
        tsneMapPair r = (distKey r, (F.rgetField @BRDC.TSNE1 r, F.rgetField @BRDC.TSNE2 r))
        tsneDistMap = FL.fold (FL.premap tsneMapPair FL.map) tsneResult
        tsneResFilter radius t1 t2 r =
          let x = F.rgetField @BRDC.TSNE1 r
              y = F.rgetField @BRDC.TSNE2 r
          in (x - t1)^2 + (y - t2)^2 <= radius^2
    let {- tsneEach :: (K.KnitMany r
                    , F.ElemOf qs BR.StateAbbreviation
                    , F.ElemOf qs ET.DistrictName
                    , F.ElemOf qs SBComparison
                    )
                 => Maybe (F.Record [BR.StateAbbreviation, ET.DistrictName])
                 -> F.Record qs
                 -> K.Sem (PS.State SBCS ': r) ()-}
        tsneEach spcdM d = do
          let dk = distKey d
              noteName = BR.Used $ dk <> if sp then "_sawbuck" else "_details"
              sp = isJust spcdM
          _ <- BR.brNewNote allCDsPaths postInfo noteName  ("District Details: " <> dk) $ do
            when sp $ cdPostTop (BR.inputsDir allCDsPaths) (distKey d) >>= BR.brAddMarkDown
            case M.lookup dk tsneDistMap of
              Nothing -> K.knitError $ dk <> " is missing from tsne Results"
              Just (t1, t2) -> do
                (densQuantiles, gowQuantiles, nwQuantiles) <- case quantileFunctionsE of
                  [DCC.SBCCategoryData _ q1, DCC.SBCCategoryData _ q2, DCC.SBCCategoryData _ q3] -> return (q1, q2, q3)
                  _ -> K.knitError "Wrong number of quantile functions in allCDPost.fTable"
                let tsneNear = F.filterFrame (tsneResFilter 15 t1 t2) $ tsneWith
                when (not sp) $ do
                  BR.brAddRawHtmlTable dk
                    (BHA.class_ "brTable")
                    (allCDsColonnade nwQuantiles gowQuantiles densQuantiles $ modelVsHistoricalTableCellStyle brShareRange draShareRangeCD)
                    (sortBy districtCompare $ fmap F.rcast $ FL.fold FL.list tsneNear)
                  _ <- K.addHvega Nothing Nothing
                    $ BRDC.tsneChartNum @TwoPartyDShare dk "Hist Share" (\x -> 100 * x - 50) (FV.ViewConfig 600 600 5) (fmap F.rcast tsneNear)
                  pure ()
                sbcsNatE <- getSBCs DCC.SBCNational $ F.rcast d
                sbcsNat <- K.knitEither sbcsNatE
--                K.addHvega Nothing Nothing $ sbcChart SBCNational 20 10 (FV.ViewConfig 300 80 5) dRanksSBD rRanksSBD $ one (dk, True, sbcsNat)
                _ <- K.addHvega Nothing Nothing $ DCC.sbcChart DCC.SBCNational 20 10 (FV.ViewConfig 300 80 5) (Just categoryNames)
                  (fmap (\(n, m) -> (realToFrac n, realToFrac m)) <<$>> partyLoHis)
                  $ one (dk, True, sbcsNat)
                sbcsStE <- getSBCs DCC.SBCState $ F.rcast d
                case sbcsStE of
                  Left _ -> pure ()
                  Right sbcsSt -> do
                    BR.brAddMarkDown $ cdPostStateChart (BR.inputsDir allCDsPaths) dk
--                    (K.addHvega Nothing Nothing $ sbcChart SBCState 20 10 (FV.ViewConfig 300 80 5) dRanksSBD rRanksSBD $ one (dk, True, sbcsSt)) >> pure ()
--                    sApLoHis <- K.knitEither $ sApLoHisE $ F.rgetField @BR.StateAbbreviation d
                    _ <- K.addHvega Nothing Nothing $ DCC.sbcChart DCC.SBCState 20 10 (FV.ViewConfig 300 80 5) (Just categoryNames)
                      (fmap (\(n, m) -> (realToFrac n, realToFrac m)) <<$>> partyLoHis)
                      $ one (dk, True, sbcsSt)
                    pure ()
                let tsneNear' = F.filterFrame (\r -> distKey r /= distKey d) tsneNear
                    eachNear dNear = do
                      let dkNear = distKey dNear
                      sbcsNearE <- getSBCs DCC.SBCNational $ F.rcast dNear
                      sbcsNear <- K.knitEither sbcsNearE
                      _ <- K.addHvega Nothing Nothing $ DCC.sbcChart DCC.SBCNational 20 10 (FV.ViewConfig 300 80 5) (Just categoryNames)
                        (fmap (\(n, m) -> (realToFrac n, realToFrac m)) <<$>> partyLoHis)
                        $ (dk, True, sbcsNat) :| [(dkNear, False, sbcsNear)]
                      pure ()
                case spcdM of
                  Nothing -> traverse_ eachNear tsneNear'
                  Just cds -> do
                    BR.brAddMarkDown (cdPostCompChart (BR.inputsDir allCDsPaths) dk (distKey <$> cds)) >> traverse_ eachNear cds

                when sp $ BR.brAddMarkDown $ cdPostBottom (BR.inputsDir allCDsPaths) (distKey d)
          pure ()
    precomputedSBCS <- PS.execState
                       (SBCS mempty mempty)
                       (traverse_ (traverse_ $ tsneEach Nothing) $ fmap snd $ categorized)
    let distRecMap = FL.fold (FL.premap (\r -> (F.rcast @[BR.StateAbbreviation, ET.DistrictName] r, r)) FL.map) sortedFilteredModelAndDRA
        tsneWithMap = FL.fold (FL.premap (\r -> (F.rcast @[BR.StateAbbreviation, ET.DistrictName] r, r)) FL.map) tsneWith
        tsneEachFromKeys k1 k2 = do
          comps <- K.knitMaybe ("missing at least one of " <> show k1 <> " from distInfo") $ traverse (flip M.lookup tsneWithMap) k1
          d2 <- K.knitMaybe (show k2 <> " missing from distInfo") $ M.lookup k2 distRecMap
          tsneEach (Just comps) d2
    _ <- PS.runState precomputedSBCS
         (traverse_ (\(x, y) -> tsneEachFromKeys y x) spCD)
    pure ()
--    BR.brAddRawHtmlTable
--      ("Calculated Dem Vote Share 2022: Demographic Model vs. Historical Model (DR)")
--      (BHA.class_ "brTable")
--      (allCDsColonnade $ modelVsHistoricalTableCellStyle brShareRange draShareRangeCD)
--      sortedFilteredModelAndDRA
  pure ()

mkCDKey :: Text -> Text -> F.Record [BR.StateAbbreviation, ET.DistrictName]
mkCDKey sa dn = sa F.&: dn F.&: V.RNil

spCD :: [(F.Record [BR.StateAbbreviation, ET.DistrictName], NonEmpty (F.Record [BR.StateAbbreviation, ET.DistrictName]))]
spCD = [(mkCDKey "CA" "9", one $ mkCDKey "AZ" "7" )
--       , (mkCDKey "CA" "40", one $ mkCDKey "NJ" "6")
       , (mkCDKey "NM" "3", one $ mkCDKey "NM" "2")
       , (mkCDKey "CT" "5", one $ mkCDKey "MA" "3")
--       , (mkCDKey "CA" "45", one $ mkCDKey "CA" "14")
       , (mkCDKey "CA" "47", one $ mkCDKey "CA" "40")
       , (mkCDKey "NJ" "11", mkCDKey "CT" "4" :| [mkCDKey "GA" "6", mkCDKey "TX" "24"])
       , (mkCDKey "NV" "4", one $ mkCDKey "CA" "6")
       , (mkCDKey "PA" "6", one $ mkCDKey "NJ" "7")
       , (mkCDKey "NJ" "3", mkCDKey "FL" "5" :| [mkCDKey "GA" "11"])
       , (mkCDKey "MI" "3", mkCDKey "NJ" "1" :| [mkCDKey "OK" "5"])
       , (mkCDKey "NV" "3", one $ mkCDKey "CA" "6")
       , (mkCDKey "NH" "1", one $ mkCDKey "CT" "2")
       , (mkCDKey "ME" "2", one $ mkCDKey "IA" "1")
       , (mkCDKey "KS" "3", one $ mkCDKey "CO" "7")
       , (mkCDKey "IA" "3", one $ mkCDKey "OR" "4")
       , (mkCDKey "NV" "1", one $ mkCDKey "CA" "6")
       , (mkCDKey "MN" "2", one $ mkCDKey "WI" "2")
       , (mkCDKey "CA" "13", one $ mkCDKey "CA" "25")
       , (mkCDKey "OH" "9", one $ mkCDKey "MI" "8")
       , (mkCDKey "NC" "13", one $ mkCDKey "NC" "4")
       , (mkCDKey "TX" "28", one $ mkCDKey "TX" "34")
       , (mkCDKey "IN" "1", one $ mkCDKey "IL" "13")
       , (mkCDKey "PA" "8", mkCDKey "NY" "19" :| [mkCDKey "NY" "22"])
       ]

--data SBCComp = SBCNational | SBCState deriving (Eq, Ord, Bounded, Enum, Array.Ix)
data  SBCS = SBCS { sbcNational :: Map (Text, Text) (Either Text [DCC.SBCCategoryData DCC.SBComparison])
                     , sbcState :: Map (Text, Text) (Either Text [DCC.SBCCategoryData DCC.SBComparison])
                     }
getSBCMap :: DCC.SBCComp -> SBCS ->  Map (Text, Text) (Either Text [DCC.SBCCategoryData DCC.SBComparison])
getSBCMap DCC.SBCNational = sbcNational
getSBCMap DCC.SBCState = sbcState

updateSBCMap :: DCC.SBCComp -> Map (Text, Text) (Either Text [DCC.SBCCategoryData DCC.SBComparison]) -> SBCS -> SBCS
updateSBCMap DCC.SBCNational m sbcs = sbcs { sbcNational = m }
updateSBCMap DCC.SBCState m sbcs = sbcs { sbcState = m }


allCDsColonnade nwQ gowQ dQ cas =
  let state' = F.rgetField @DT.StateAbbreviation
      dName = F.rgetField @ET.DistrictName
      was = F.rgetField @OldCDOverlap
      dave' = round @_ @Int . (100*) . F.rgetField @TwoPartyDShare
      fracNW r = 1 - F.rgetField @BRE.FracWhiteNonHispanic r
      qF x = case x of
        Left _ -> BR.textToStyledHtml "Err"
        Right n -> BR.numberToStyledHtml "%d" n
      fracGOW r = F.rgetField @BRE.FracGradOfWhite r
      share50' = round @_ @Int . (100 *) . MT.ciMid . F.rgetField @BRE.ModeledShare
  in C.headed "State" (BR.toCell cas "State" "State" (BR.textToStyledHtml . state'))
     <> C.headed "District" (BR.toCell cas "District" "District" (BR.textToStyledHtml . dName))
     <> C.headed "Was" (BR.toCell cas "Was" "Was" (BR.textToStyledHtml . was))
     <> C.headed "Demographic Model (Blue Ripple)" (BR.toCell cas "Demographic" "Demographic" (BR.numberToStyledHtml "%d" . share50'))
     <> C.headed "Historical Model (Dave's Redistricting)" (BR.toCell cas "Historical" "Historical" (BR.numberToStyledHtml "%d" . dave'))
     <> C.headed "BR Stance" (BR.toCell cas "BR Stance" "BR Stance" (BR.textToStyledHtml . (\r -> brDistrictFramework DFLong DFUnk brShareRange draShareRangeCD (share50' r) (dave r))))
     <> C.headed "%Non-White" (BR.toCell cas "%NW" "%NW" (BR.numberToStyledHtml "%d" . round @_ @Int . (100*) . fracNW))
     <> C.headed "NW Quantile" (BR.toCell cas "NWQ" "NWQ" (qF . nwQ))
     <> C.headed "%Grad-White" (BR.toCell cas "%GW" "%GW" (BR.numberToStyledHtml "%d" . round @_ @Int . (100*) . fracGOW))
     <> C.headed "GOW Quantile" (BR.toCell cas "GOWQ" "GOWQ" (qF . gowQ))
     <> C.headed "Pop/SqMile" (BR.toCell cas "P/SqMi" "P/SqMi" (BR.numberToStyledHtml "%2.0f" . Numeric.exp . F.rgetField @DT.PopPerSqMile))
     <> C.headed "Density Quantile" (BR.toCell cas "DQ" "DQ" (qF . dQ))

diffVsChart :: (V.KnownField t, V.Snd t ~ Double)
            => Text
            -> (Text, Double -> Double)
            -> FV.ViewConfig
            -> F.FrameRec ([BR.StateAbbreviation, ET.DistrictName, BRE.ModeledShare, TwoPartyDShare, t])
            -> GV.VegaLite
diffVsChart title (xLabel, f) vc rows =
  let toVLDataRec = FVD.asVLData GV.Str "State"
                    V.:& FVD.asVLData GV.Str "District"
                    V.:& FVD.asVLData (GV.Number . (*100) . MT.ciMid) "Modeled_Share"
                    V.:& FVD.asVLData (GV.Number . (*100)) "Historical_Share"
                    V.:& FVD.asVLData (GV.Number . f) xLabel
                    V.:& V.RNil
      vlData = FVD.recordsToData toVLDataRec rows
--      makeDistrictName = GV.transform . GV.calculateAs "datum.State + '-' + datum.District" "District Name"
      makeShareDiff = GV.transform . GV.calculateAs "datum.Modeled_Share - datum.Historical_Share" "Delta"
      encDiff = GV.position GV.Y ([GV.PName "Delta"
                                  , GV.PmType GV.Quantitative
                                  , GV.PAxis [GV.AxTitle "Delta"]
                                  ]
                                 )
      encFracHisp = GV.position GV.X ([GV.PName xLabel
                                      , GV.PmType GV.Quantitative
                                      , GV.PAxis [GV.AxTitle xLabel]
                                      ]
                                     )
      ptEnc = GV.encoding . encFracHisp . encDiff
      ptSpec = GV.asSpec [ptEnc [], GV.mark GV.Circle []]
      finalSpec = [FV.title title, GV.layer [ptSpec], makeShareDiff [], vlData]
  in FV.configuredVegaLite vc finalSpec

diffVsHispChart :: Text
                -> FV.ViewConfig
                -> F.FrameRec ([BR.StateAbbreviation, ET.DistrictName, BRE.ModeledShare, TwoPartyDShare, Redistrict.HispanicFrac])
                -> GV.VegaLite
diffVsHispChart title vc rows = diffVsChart @Redistrict.HispanicFrac title ("Fraction Hispanic", (*100)) vc rows


diffVsLogDensityChart :: Text
                      -> FV.ViewConfig
                      -> F.FrameRec ([BR.StateAbbreviation, ET.DistrictName, BRE.ModeledShare, TwoPartyDShare, DT.PopPerSqMile])
                      -> GV.VegaLite
diffVsLogDensityChart title vc rows = diffVsChart @DT.PopPerSqMile title ("Log Density", id) vc rows
--

newCongressionalMapPosts :: forall r. (K.KnitMany r, BR.CacheEffects r) => BR.CommandLine -> K.Sem r ()
newCongressionalMapPosts cmdLine = do
  ccesAndCPSEM_C <-  BRE.prepCCESAndCPSEM False
  acs_C <- BRE.prepACS False
  let ccesWD_C = fmap BRE.ccesEMRows ccesAndCPSEM_C
  proposedCDs_C <- fmap (fmap (F.rcast @PostStratR)) <$> (prepCensusDistrictData False "model/newMaps/newCDDemographicsDR.bin" =<< BRC.censusTablesForProposedCDs)
  drExtantCDs_C <- fmap (fmap (F.rcast @PostStratR)) <$> (prepCensusDistrictData False "model/newMaps/extantCDDemographicsDR.bin" =<< BRC.censusTablesForDRACDs)
  drCDPlans <- Redistrict.allPassedCongressionalPlans
  let acsExtantCDs_C = fmap fixACS <$> acs_C

  let regCDPost pi' sa extantDemo_C = do
        paths' <- postPaths (sa <> "_Congressional") cmdLine
        BR.brNewPost paths' pi' sa $ do
          postSpec <- NewCDMapPostSpec sa paths'
            <$> (K.ignoreCacheTimeM $ Redistrict.lookupAndLoadRedistrictingPlanAnalysis drCDPlans (Redistrict.redistrictingPlanId sa "Passed" ET.Congressional))
          newCongressionalMapAnalysis False cmdLine postSpec pi'
            (K.liftActionWithCacheTime ccesWD_C)
            (K.liftActionWithCacheTime ccesAndCPSEM_C)
            (K.liftActionWithCacheTime acs_C)
            (K.liftActionWithCacheTime $ fmap (FL.fold postStratRollupFld . onlyState sa) extantDemo_C)
            (K.liftActionWithCacheTime $ fmap (FL.fold postStratRollupFld . onlyState sa) proposedCDs_C)

  let postInfoAZ = BR.PostInfo (BR.postStage cmdLine) (BR.PubTimes (BR.Published $ Time.fromGregorian 2022 05 13) Nothing)
  regCDPost postInfoAZ "AZ" acsExtantCDs_C

  let postInfoGA = BR.PostInfo (BR.postStage cmdLine) (BR.PubTimes (BR.Published $ Time.fromGregorian 2022 05 17) Nothing)
  regCDPost postInfoGA "GA" acsExtantCDs_C

  let postInfoMI = BR.PostInfo (BR.postStage cmdLine) (BR.PubTimes (BR.Published $ Time.fromGregorian 2022 05 13) Nothing)
  regCDPost postInfoMI "MI" acsExtantCDs_C

  -- NC is different because ACS doesn't have correct CD boundaries (this might have changed w 2020 ACS?)
  let postInfoNC = BR.PostInfo
                   (BR.postStage cmdLine)
                   (BR.PubTimes
                     (BR.Published $ Time.fromGregorian 2021 12 15) (Just $ BR.Published $ Time.fromGregorian 2022 05 13)
                   )
  regCDPost postInfoNC "NC" drExtantCDs_C

  let postInfoNY = BR.PostInfo (BR.postStage cmdLine) (BR.PubTimes (BR.Published $ Time.fromGregorian 2022 05 17) (Just $ BR.Published $ Time.fromGregorian 2022 05 21))
  regCDPost postInfoNY "NY" acsExtantCDs_C

  let postInfoPA = BR.PostInfo (BR.postStage cmdLine) (BR.PubTimes (BR.Published $ Time.fromGregorian 2022 05 13) Nothing)
  regCDPost postInfoPA "PA" acsExtantCDs_C

  let postInfoTX = BR.PostInfo
                   (BR.postStage cmdLine)
                   (BR.PubTimes
                     (BR.Published $ Time.fromGregorian 2022 2 25) (Just $  BR.Published $ Time.fromGregorian 2022 05 13)
                   )
  regCDPost postInfoTX "TX" acsExtantCDs_C

districtColonnade cas =
  let state' = F.rgetField @DT.StateAbbreviation
      dName = F.rgetField @ET.DistrictName
      share5 = MT.ciLower . F.rgetField @BRE.ModeledShare
      share50' = MT.ciMid . F.rgetField @BRE.ModeledShare
      share95 = MT.ciUpper . F.rgetField @BRE.ModeledShare
  in C.headed "State" (BR.toCell cas "State" "State" (BR.textToStyledHtml . state'))
     <> C.headed "District" (BR.toCell cas "District" "District" (BR.textToStyledHtml . dName))
     <> C.headed "5%" (BR.toCell cas "5%" "5%" (BR.numberToStyledHtml "%2.2f" . (100*) . share5))
     <> C.headed "50%" (BR.toCell cas "50%" "50%" (BR.numberToStyledHtml "%2.2f" . (100*) . share50'))
     <> C.headed "95%" (BR.toCell cas "95%" "95%" (BR.numberToStyledHtml "%2.2f" . (100*) . share95))

modelCompColonnade states cas =
  C.headed "Model" (BR.toCell cas "Model" "Model" (BR.textToStyledHtml . fst))
  <> mconcat (fmap (\s -> C.headed (BR.textToCell s) (BR.toCell cas s s (BR.maybeNumberToStyledHtml "%2.2f" . M.lookup s . snd))) states)

type ModelPredictorR = [DT.SexC, DT.CollegeGradC, DT.Race5C, DT.PopPerSqMile]
type PostStratR = [BR.StateAbbreviation, ET.DistrictTypeC, ET.DistrictName] V.++ ModelPredictorR V.++ '[BRC.Count]
type ElexDShare = "ElexDShare" F.:-> Double
type TwoPartyDShare = "2-Party DShare" F.:-> Double

twoPartyDShare r =
  let ds = F.rgetField @ET.DemShare r
      rs = F.rgetField @ET.RepShare r
  in FT.recordSingleton @TwoPartyDShare $ ds/(ds + rs)

addTwoPartyDShare r = r F.<+> twoPartyDShare r

--data ExtantDistricts = PUMSDistricts | DRADistricts


dave :: F.ElemOf rs TwoPartyDShare => F.Record rs -> Int
dave = round @_ @Int . (100*) . F.rgetField @TwoPartyDShare

share50 :: F.ElemOf rs BRE.ModeledShare => F.Record rs -> Int
share50 = round @_ @Int . (100 *) . MT.ciMid . F.rgetField @BRE.ModeledShare


--contestedCond :: (F.ElemOf rs ET.DistrictTypeC, F.ElemOf rs ET.DistrictName)
--              => (F.Record [ET.DistrictTypeC, ET.DistrictName] -> Bool) -> F.Record rs -> Bool
--contestedCond f = f . F.rcast @[ET.DistrictTypeC, ET.DistrictName]

{-
rowFilter :: (F.ElemOf rs ET.DistrictTypeC, F.ElemOf rs ET.DistrictName, F.ElemOf rs TwoPartyDShare, F.ElemOf rs BRE.ModeledShare)
          => Bool
          -> (F.Record rs -> Bool)
          -> F.Record rs
          -> Bool
rowFilter interestingOnly cc r
  = cc r
    && if interestingOnly
       then (not $ modelDRALeans brShareRange draShareRangeSLD (share50 r) (dave r) `elem` [(SafeD, SafeD), (SafeR, SafeR)])
       else True
-}

--filteredSorted = filter rowFilter sortedModelAndDRA
categoryFilter :: (F.ElemOf rs TwoPartyDShare,F.ElemOf rs BRE.ModeledShare)
               =>  (F.Record rs -> Bool)
               -> (Int, Int)
               -> (Int, Int)
               -> [DistType] -> [DistType] -> F.Record rs -> Bool
categoryFilter cc brRange draRange brs drs r =
  let (brLean, draLean) =  modelDRALeans brRange draRange (share50 r) (dave r)
  in cc r
     && brLean `elem` brs
     && draLean `elem` drs

bothCloseFilter :: (F.ElemOf rs TwoPartyDShare,F.ElemOf rs BRE.ModeledShare)
                  => (F.Record rs -> Bool) -> (Int, Int) -> (Int, Int) -> F.Record rs -> Bool
bothCloseFilter cc brR draR = categoryFilter cc brR draR [LeanR, Tossup, LeanD] [LeanR, Tossup, LeanD]

plausibleSurpriseFilter :: (F.ElemOf rs TwoPartyDShare,F.ElemOf rs BRE.ModeledShare)
                        => (F.Record rs -> Bool) -> (Int, Int) -> (Int, Int) -> F.Record rs -> Bool
plausibleSurpriseFilter cc brR draR r = categoryFilter cc brR draR [SafeD] [Tossup, LeanR] r
                                      || categoryFilter cc brR draR [SafeR] [Tossup, LeanD] r

implausibleSurpriseFilter :: (F.ElemOf rs TwoPartyDShare,F.ElemOf rs BRE.ModeledShare)
                  => (F.Record rs -> Bool) -> (Int, Int) -> (Int, Int) -> F.Record rs -> Bool
implausibleSurpriseFilter cc brR draR r = categoryFilter cc brR draR [SafeR, LeanR, Tossup] [SafeD] r
                                         || categoryFilter cc brR draR  [SafeD, LeanD, Tossup] [SafeR] r


implausibleSafeDFilter :: (F.ElemOf rs TwoPartyDShare,F.ElemOf rs BRE.ModeledShare)
                  => (F.Record rs -> Bool) -> (Int, Int) -> (Int, Int) -> F.Record rs -> Bool
implausibleSafeDFilter cc brR draR r = categoryFilter cc brR draR [SafeR, LeanR, Tossup] [SafeD] r

implausibleSafeRFilter :: (F.ElemOf rs TwoPartyDShare,F.ElemOf rs BRE.ModeledShare)
                  => (F.Record rs -> Bool) -> (Int, Int) -> (Int, Int) -> F.Record rs -> Bool
implausibleSafeRFilter cc brR draR r = categoryFilter cc brR draR  [SafeD, LeanD, Tossup] [SafeR] r

plausibleSafeDFilter :: (F.ElemOf rs TwoPartyDShare,F.ElemOf rs BRE.ModeledShare)
                  => (F.Record rs -> Bool) -> (Int, Int) -> (Int, Int) -> F.Record rs -> Bool
plausibleSafeDFilter cc brR draR r = categoryFilter cc brR draR [LeanD] [SafeD] r

plausibleSafeRFilter :: (F.ElemOf rs TwoPartyDShare,F.ElemOf rs BRE.ModeledShare)
                  => (F.Record rs -> Bool) -> (Int, Int) -> (Int, Int) -> F.Record rs -> Bool
plausibleSafeRFilter cc brR draR r = categoryFilter cc brR draR  [LeanR] [SafeR] r



demographicallyFavorableFilter :: (F.ElemOf rs TwoPartyDShare,F.ElemOf rs BRE.ModeledShare)
                               => (F.Record rs -> Bool) -> (Int, Int) -> (Int, Int) -> F.Record rs -> Bool
demographicallyFavorableFilter cc brR draR = categoryFilter cc brR draR [SafeD] [LeanD, Tossup, LeanR]

demographicallyUnfavorableFilter :: (F.ElemOf rs TwoPartyDShare,F.ElemOf rs BRE.ModeledShare)
                               => (F.Record rs -> Bool) -> (Int, Int) -> (Int, Int) -> F.Record rs -> Bool
demographicallyUnfavorableFilter cc brR draR = categoryFilter cc brR draR [SafeR] [LeanD, Tossup, LeanR]



diffOfDegreeFilter :: (F.ElemOf rs TwoPartyDShare,F.ElemOf rs BRE.ModeledShare)
                  => (F.Record rs -> Bool) -> (Int, Int) -> (Int, Int) -> F.Record rs -> Bool
diffOfDegreeFilter cc brR draR r = categoryFilter cc brR draR [SafeD] [LeanD] r || categoryFilter cc brR draR [SafeR] [LeanR] r

safeSafeFilter :: (F.ElemOf rs TwoPartyDShare,F.ElemOf rs BRE.ModeledShare)
                  => (F.Record rs -> Bool) -> (Int, Int) -> (Int, Int) -> F.Record rs -> Bool
safeSafeFilter cc brR draR r = categoryFilter cc brR draR [SafeD] [SafeD] r || categoryFilter cc brR draR [SafeR] [SafeR] r


dCategories1 =
  [DistrictCategory "Both Close" bothCloseFilter
  ,DistrictCategory "Plausible Surprises" plausibleSurpriseFilter
  ,DistrictCategory "Difference of Degree" diffOfDegreeFilter
  , DistrictCategory "Implausible Surprises" implausibleSurpriseFilter
  ]

dCategories2 =
  [DistrictCategory "Both Close" bothCloseFilter
  ,DistrictCategory "Demographically Favorable Toss-ups" demographicallyFavorableFilter
  ,DistrictCategory "Demographically Unfavorable Toss-ups" demographicallyUnfavorableFilter
  , DistrictCategory "Demographically Surprising But Historically Safe" implausibleSurpriseFilter
  ]

dCategories3 =
  [DistrictCategory "Both Close" bothCloseFilter
  ,DistrictCategory "Demographically Favorable Toss-ups" demographicallyFavorableFilter
  ,DistrictCategory "Demographically Unfavorable Toss-ups" demographicallyUnfavorableFilter
  , DistrictCategory "Demographically Surprising But Historically Safe D" implausibleSafeDFilter
  , DistrictCategory "Demographically Surprising But Historically Safe R" implausibleSafeRFilter
  ]

dCategories4 =
  [DistrictCategory "Both Close" bothCloseFilter
  ,DistrictCategory "Demographically Favorable Toss-ups" demographicallyFavorableFilter
  ,DistrictCategory "Demographically Unfavorable Toss-ups" demographicallyUnfavorableFilter
  , DistrictCategory "Demographically Vulnerable But Historically Safe D" plausibleSafeDFilter
  , DistrictCategory "Demographically Vulnerable But Historically Safe D" plausibleSafeRFilter
  , DistrictCategory "Demographically Surprising But Historically Safe D" implausibleSafeDFilter
  , DistrictCategory "Demographically Surprising But Historically Safe R" implausibleSafeRFilter
  , DistrictCategory "Safe/Safe" safeSafeFilter
  ]


data CategorizedDistricts f rs
  = CategorizedDistricts
    { bothClose :: f (F.Record rs)
    , plausibleSurprise :: f (F.Record rs)
    , diffOfDegree :: f (F.Record rs)
    , implausibleSurprise :: f (F.Record rs)
    , safeSafe :: f (F.Record rs)
    }


categorizeDistricts :: forall f rs r. (K.KnitEffects r, Foldable f
                                      , F.ElemOf rs TwoPartyDShare
                                      , F.ElemOf rs BRE.ModeledShare
                                      , F.ElemOf rs ET.DistrictTypeC
                                      , F.ElemOf rs ET.DistrictName
                                      )
                    => (F.Record rs -> Bool) -> (Int, Int) -> (Int, Int) -> f (F.Record rs) -> K.Sem r (CategorizedDistricts [] rs)
categorizeDistricts cc brR draR allDists = do
  let length' = FL.fold FL.length
      bothCloseFld = FL.prefilter (bothCloseFilter cc brR draR) FL.list
      plausibleFld = FL.prefilter (plausibleSurpriseFilter cc brR draR) FL.list
      diffOfDegreeFld = FL.prefilter (diffOfDegreeFilter cc brR draR) FL.list
      implausibleFld = FL.prefilter (implausibleSurpriseFilter cc brR draR) FL.list
      safeSafeFld = FL.prefilter (safeSafeFilter cc brR draR) FL.list
      restFld = FL.prefilter (not . cc) FL.list
      allFld = (\c r bc ps dd is ss -> (c, r, CategorizedDistricts bc ps dd is ss))
               <$> FL.length
               <*> restFld
               <*> bothCloseFld
               <*> plausibleFld
               <*> diffOfDegreeFld
               <*> implausibleFld
               <*> safeSafeFld
      (countAll, uninteresting, categorized) = FL.fold allFld allDists
      checkTotal =
        let cnt x = length' (x categorized)
        in (countAll - length' uninteresting == cnt bothClose + cnt plausibleSurprise + cnt diffOfDegree + cnt implausibleSurprise + cnt safeSafe)
  when (not checkTotal) $ K.logLE K.Info $ "Categorized Districts: count matches"
  let findOverlaps :: (Foldable g)
                   => g (F.Record rs) -> g (F.Record rs) -> Set.Set (F.Record [ET.DistrictTypeC, ET.DistrictName])
      findOverlaps a b =
        let f :: Foldable h => h (F.Record rs) -> Set (F.Record [ET.DistrictTypeC, ET.DistrictName])
            f = Set.fromList . FL.fold (FL.premap (F.rcast @[ET.DistrictTypeC, ET.DistrictName]) FL.list)
        in Set.intersection (f a) (f b)
      reportOverlaps na nb a b = do
        let ols = findOverlaps a b
        when (not $ Set.null ols) $ K.logLE K.Warning $ "Overlaps in district categorization between " <> na <> " and " <> nb <> ": " <> show ols
  reportOverlaps "Both Close" "Plausible" (bothClose categorized) (plausibleSurprise categorized)
  reportOverlaps "Both Close" "Diff Of Degree" (bothClose categorized) (diffOfDegree categorized)
  reportOverlaps "Both Close" "Implausible" (bothClose categorized) (implausibleSurprise categorized)
  reportOverlaps "Both Close" "Safe Safe" (bothClose categorized) (safeSafe categorized)
  reportOverlaps "Plausible" "Diff of Degree" (plausibleSurprise categorized) (diffOfDegree categorized)
  reportOverlaps "Plausible" "Implausible" (plausibleSurprise categorized) (implausibleSurprise categorized)
  reportOverlaps "Plausible" "Safe" (plausibleSurprise categorized) (safeSafe categorized)
  reportOverlaps "Diff of Degree" "Implausible" (diffOfDegree categorized) (implausibleSurprise categorized)
  reportOverlaps "Diff of Degree" "Safe Safe" (diffOfDegree categorized) (safeSafe categorized)
  reportOverlaps "Implausible" "Safe Safe" (implausibleSurprise categorized) (safeSafe categorized)
  return categorized


data DistrictCategory rs where
  DistrictCategory :: (F.ElemOf rs TwoPartyDShare, F.ElemOf rs BRE.ModeledShare)
                   => Text -> ((F.Record rs -> Bool) -> (Int, Int) -> (Int, Int) -> F.Record rs -> Bool) -> DistrictCategory rs

districtCategoryName :: DistrictCategory rs -> Text
districtCategoryName (DistrictCategory n _) = n

districtCategoryCriteria :: (F.ElemOf rs TwoPartyDShare, F.ElemOf rs BRE.ModeledShare)
                         =>  DistrictCategory rs -> (F.Record rs -> Bool) -> (Int, Int) -> (Int, Int) -> F.Record rs -> Bool
districtCategoryCriteria (DistrictCategory _ f) = f

categorizeDistricts' :: forall f rs r. (K.KnitEffects r
                                       , Foldable f
                                       , F.ElemOf rs TwoPartyDShare
                                       , F.ElemOf rs BRE.ModeledShare
                                       , F.ElemOf rs BR.StateAbbreviation
                                       , F.ElemOf rs ET.DistrictTypeC
                                       , F.ElemOf rs ET.DistrictName
                                       )
                    => (F.Record rs -> Bool)
                     -> (Int, Int) -> (Int, Int)
                     -> [DistrictCategory rs]
                     -> f (F.Record rs)
                     -> K.Sem r ([(Text, [F.Record rs])])
categorizeDistricts' cc brR draR cats allDists = do
  let length' = FL.fold FL.length
      catFld c =  (,) <$> pure (districtCategoryName c) <*> FL.prefilter (districtCategoryCriteria c cc brR draR) FL.list
      catsFld = traverse catFld cats
      restFld = FL.prefilter (not . cc) FL.list
      allFld = (,,) <$> FL.length <*> catsFld <*> restFld
      (countAll, categorized, uninteresting) = FL.fold allFld allDists
      checkTotal = (countAll - length' uninteresting) == FL.fold (FL.premap (length' . snd) FL.sum) categorized
  K.logLE K.Info $ show (length' uninteresting) <> " of " <> show countAll <> " are uninteresting districts."
  when (not checkTotal) $ K.logLE K.Info "Categorized Districts: counts don't match!"
  let findOverlaps :: (Foldable g)
                   => g (F.Record rs) -> g (F.Record rs) -> Set.Set (F.Record [BR.StateAbbreviation, ET.DistrictTypeC, ET.DistrictName])
      findOverlaps a b =
        let f :: Foldable h => h (F.Record rs) -> Set (F.Record [BR.StateAbbreviation, ET.DistrictTypeC, ET.DistrictName])
            f = Set.fromList . FL.fold (FL.premap (F.rcast @[BR.StateAbbreviation,ET.DistrictTypeC, ET.DistrictName]) FL.list)
        in Set.intersection (f a) (f b)
      reportOverlaps (na, a) (nb, b) = do
        let ols = findOverlaps a b
        when (not $ Set.null ols) $ K.logLE K.Warning $ "Overlaps in district categorization between " <> na <> " and " <> nb <> ": " <> show ols
      allPairs = go categorized []
        where
          go [] y = y
          go [_] y = y
          go (x : xs) y = go xs (((x,) <$> xs) ++ y)
  traverse_ (uncurry reportOverlaps) allPairs
  traverse_ (\(t,c) -> K.logLE K.Info $ t <> " has " <> show (length c) <> " districts.") categorized
  return categorized

addCategoriesToRecords :: FI.RecVec (rs V.++ '[DistCategory]) => [(Text, [F.Record rs])] -> F.FrameRec (rs V.++ '[DistCategory])
addCategoriesToRecords = F.toFrame . concat . fmap f
  where
    f :: (Text, [F.Record rs]) -> [F.Record (rs V.++ '[DistCategory])]
    f (dc, rs) = fmap (V.<+> FT.recordSingleton @DistCategory dc) rs

data NewSLDMapsPostSpec = NewSLDMapsPostSpec
                          { stateAbbr :: Text
                          , districtDescription :: Text
                          , paths :: BR.PostPaths BR.Abs
                          , sldDRAnalysis :: F.Frame Redistrict.DRAnalysis
                          , cdDRAnalysis :: F.Frame Redistrict.DRAnalysis
                          , overlaps :: Map ET.DistrictType (DO.DistrictOverlaps Int)
                          , contested :: F.Record [ET.DistrictTypeC, ET.DistrictName] -> Bool
                          , spDists :: [(ET.DistrictType, Text)]
                          }


newStateLegMapAnalysis :: forall r.(K.KnitMany r, K.KnitOne r, BR.CacheEffects r)
                       => BR.CommandLine
                       -> NewSLDMapsPostSpec
                       -> Bool
                       -> K.ActionWithCacheTime r BRE.CCESAndCPSEM
                       -> K.ActionWithCacheTime r (F.FrameRec BRE.PUMSWithDensityEM) -- ACS data
                       -> K.ActionWithCacheTime r (F.FrameRec PostStratR) -- (proposed) congressional districts
                       -> K.ActionWithCacheTime r (F.FrameRec PostStratR) -- proposed SLDs
                       -> K.Sem r ()
newStateLegMapAnalysis cmdLine postSpec interestingOnly ccesAndCPSEM_C acs_C cdDemo_C sldDemo_C = K.wrapPrefix "newStateLegMapAnalysis" $ do
  K.logLE K.Info $ "Rebuilding state-leg map analysis for " <> stateAbbr postSpec <> "( " <> districtDescription postSpec <> ")"
  BR.brAddPostMarkDownFromFile (paths postSpec) "_intro"
  let ccesAndCPS2020_C = fmap (BRE.ccesAndCPSForYears [2020]) ccesAndCPSEM_C
      acs2020_C = fmap (BRE.acsForYears [2020]) acs_C
  acsForState <- fmap (F.filterFrame ((== stateAbbr postSpec) . F.rgetField @BR.StateAbbreviation)) $ K.ignoreCacheTime acs2020_C
  sldDemo <- K.ignoreCacheTime sldDemo_C
  cdDemo <- K.ignoreCacheTime cdDemo_C
  let sldPWLD = FL.fold (peopleWeightedLogDensityFld (F.rgetField @BRC.Count)) sldDemo
      cdPWLD = FL.fold (peopleWeightedLogDensityFld (F.rgetField @BRC.Count)) cdDemo
      acs2020PWLD = FL.fold (peopleWeightedLogDensityFld (F.rgetField @PUMS.Citizens)) acsForState
      rescaleSLD = rescaleDensity $ Numeric.exp (acs2020PWLD - sldPWLD)
      rescaleCD = rescaleDensity $ Numeric.exp (acs2020PWLD - cdPWLD)
  K.logLE K.Info $ "People-weighted log-density: acs=" <> show acs2020PWLD <> "; SLD=" <> show sldPWLD <> "; CD=" <> show cdPWLD
  let stanParams = SC.StanMCParameters 4 4 (Just 1000) (Just 1000) (Just 0.8) (Just 10) Nothing
      mapGroup :: SB.GroupTypeTag (F.Record CDLocWStAbbrR) = SB.GroupTypeTag "CD"
      postStratInfo dd = (mapGroup
                         , "DM" <> "_" <> stateAbbr postSpec <> "_" <> dd
                         )
      modelDM :: Text
              -> K.ActionWithCacheTime r (F.FrameRec PostStratR)
              -> K.Sem r (F.FrameRec (BRE.ModelResultsR CDLocWStAbbrR))
      modelDM dd x = do
        K.ignoreCacheTimeM $ BRE.electionModelDM False cmdLine False (Just stanParams) modelDir modelVariant 2020 (postStratInfo dd) ccesAndCPS2020_C x
  modeledCDs <- modelDM "Congressional" (rescaleCD . fmap F.rcast <$> cdDemo_C)
  modeledSLDs <- modelDM (districtDescription postSpec) (rescaleSLD . fmap F.rcast <$> sldDemo_C)
  sldDemo' <- K.ignoreCacheTime sldDemo_C
  let (modelDRA, modelDRAMissing)
        = FJ.leftJoinWithMissing @[BR.StateAbbreviation, ET.DistrictTypeC, ET.DistrictName]
        modeledSLDs
        (fmap addTwoPartyDShare $ sldDRAnalysis postSpec)
  when (not $ null modelDRAMissing) $ K.knitError $ "newStateLegAnalysis: missing keys in model/DRA join. " <> show modelDRAMissing
  let modMid = round . (100*). MT.ciMid . F.rgetField @BRE.ModeledShare
      dra = round . (100*) . F.rgetField @TwoPartyDShare
      dName = F.rgetField @ET.DistrictName
      dType = F.rgetField @ET.DistrictTypeC
      cdModelMap = FL.fold (FL.premap (\r -> (dName r, modMid r)) FL.map) modeledCDs
      cdDRAMap = FL.fold (FL.premap (\r -> (dName r, dra r)) FL.map) $ fmap addTwoPartyDShare $ cdDRAnalysis postSpec
      modelCompetitive n = brCompetitive || draCompetitive
        where draCompetitive = fromMaybe False $ fmap (between draShareRangeCD) $ M.lookup n cdDRAMap
              brCompetitive = fromMaybe False $ fmap (between brShareRange) $ M.lookup n cdModelMap
      sortedModelAndDRA = reverse $ sortOn share50 $ FL.fold FL.list modelDRA
  let overlapsMMap (dt, dn) = M.lookup dt (overlaps postSpec) >>= (\d -> DO.overlapsOverThresholdForRowByName 0.5 d dn)
      tableCAS ::  (F.ElemOf rs BRE.ModeledShare, F.ElemOf rs TwoPartyDShare, F.ElemOf rs ET.DistrictName, F.ElemOf rs ET.DistrictTypeC)
               => BR.CellStyle (F.Record rs) String
      tableCAS =  modelVsHistoricalTableCellStyle brShareRange draShareRangeSLD <> "border: 3px solid green" `BR.cellStyleIf` \r h -> f r && h == "CD Overlaps"
        where
          f r = Monoid.getAny $ mconcat
                $ fmap (Monoid.Any . modelCompetitive . fst)
                $ M.toList $ fromMaybe mempty $ overlapsMMap (dType r, dName r)
      contestedCond = contested postSpec . F.rcast @[ET.DistrictTypeC, ET.DistrictName]
      rowFilter r = contestedCond r
                    && if interestingOnly
                       then (not $ modelDRALeans brShareRange draShareRangeSLD (share50 r) (dave r) `elem` [(SafeD, SafeD), (SafeR, SafeR)])
                       else True
      filteredSorted = filter rowFilter sortedModelAndDRA
      interestingDistricts = Set.fromList $ (F.rcast @[ET.DistrictTypeC, ET.DistrictName] <$> filteredSorted)
      interestingFilter :: (F.ElemOf rs ET.DistrictName, F.ElemOf rs ET.DistrictTypeC) => F.Record rs -> Bool
      interestingFilter r = F.rcast @[ET.DistrictTypeC, ET.DistrictName] r `Set.member` interestingDistricts
  K.logLE K.Info $ "For " <> districtDescription postSpec <> " in " <> stateAbbr postSpec
    <> " there are " <> show (length interestingDistricts) <> " interesting districts."
  let districtOrder ra rb = let cd = compare (dType ra) (dType rb)
                            in if cd /= EQ then cd
                               else ET.districtNameCompare (dName ra) (dName rb)
  BR.brAddRawHtmlTable
    ("Dem Vote Share, " <> stateAbbr postSpec <> " State-Leg 2022: In District order")
    (BHA.class_ "brTable")
    (dmColonnadeOverlap overlapsMMap tableCAS)
    (sortBy districtOrder sortedModelAndDRA)
  -- for Sawbuck
  let sawBuckFilter r = dra r >= (48 :: Int) && dra r <= (52 :: Int)
      sawBuckDelta r = dra r - modMid r
  BR.brAddRawHtmlTable
    ("Dem Vote Share, " <> stateAbbr postSpec <> " State-Leg 2022: Sawbuck Sort")
    (BHA.class_ "brTable")
    (dmColonnadeOverlap overlapsMMap tableCAS)
    (sortOn sawBuckDelta $ filter sawBuckFilter sortedModelAndDRA)
  categorized <- categorizeDistricts' (contested postSpec . F.rcast) brShareRange draShareRangeSLD dCategories3 sortedModelAndDRA
  let fTable t ds = do
        when (not $ null ds)
          $  BR.brAddRawHtmlTable
          ("Dem Vote Share, " <> stateAbbr postSpec <> " State-Leg 2022: " <> t)
          (BHA.class_ "brTable")
          (dmColonnadeOverlap overlapsMMap tableCAS)
          ds
  traverse_ (uncurry fTable) categorized
--  fTable (bothClose categorized) "Both Models Close"
--  fTable (plausibleSurprise categorized) "Plausible Surprises"
--  fTable (diffOfDegree categorized) "Differences of Degree"
--  fTable (implausibleSurprise categorized) "Implausible Suprises"
{-  BR.brAddRawHtmlTable
    ("Dem Vote Share, " <> stateAbbr postSpec <> " State-Leg 2022: All Interesting")
    (BHA.class_ "brTable")
    (dmColonnadeOverlap overlapsMMap tableCAS)
    (filter interestingFilter sortedModelAndDRA)
-}
  BR.brAddPostMarkDownFromFile (paths postSpec) "_afterModelDRATable"
  let sldByModelShare = modelShareSort sldDistLabel modeledSLDs --proposedPlusStateAndStateRace_RaceDensityNC
  _ <- K.addHvega Nothing Nothing
       $ BRV.demoCompare
       ("Race", show . F.rgetField @DT.Race5C, raceSort)
       ("Education", show . F.rgetField @DT.CollegeGradC, eduSort)
       (F.rgetField @BRC.Count)
       ("District", sldDistLabel, Just sldByModelShare)
       (Just ("log(Density)", (\x -> x) . Numeric.log . F.rgetField @DT.PopPerSqMile))
       (stateAbbr postSpec <> " New: By Race and Education")
       (FV.ViewConfig 600 600 5)
       (F.filterFrame interestingFilter sldDemo')

  let (modelDRADemo, demoMissing) = FJ.leftJoinWithMissing @[BR.StateAbbreviation, ET.DistrictTypeC, ET.DistrictName]
                                    ({- F.filterFrame interestingFilter-} modelDRA)
                                    ({- F.filterFrame interestingFilter-} sldDemo')
  when (not $ null demoMissing) $ K.knitError $ "newStateLegAnalysis: missing keys in modelDRA/demo join. " <> show demoMissing
  let interestingModelDRADemo = F.filterFrame interestingFilter modelDRADemo
  _ <- K.addHvega Nothing Nothing
      $ BRV.demoCompareXYCS
      "District"
     "% non-white"
      "% college grad"
      "Modeled D-Edge"
      "log density"
      (stateAbbr postSpec <> " demographic scatter")
      (FV.ViewConfig 600 600 5)
      (FL.fold (xyFold' sldDistLabel) interestingModelDRADemo)
  BR.brAddMarkDown "## 3. Methods (for non-experts)"
  BR.brAddSharedMarkDownFromFile (paths postSpec) "modelExplainer"
  -- Add sbc charts

  let densityAndGoWFld = DCC.buildMRFold
                         @[BR.StateAbbreviation, ET.DistrictTypeC, ET.DistrictName]
                         @[BRC.Count, DT.PopPerSqMile, DT.Race5C, DT.CollegeGradC]
                         (pwldFC V.:& wFC V.:& wgFC V.:& V.RNil)
      (modelDRADemoPlus, missing) = FJ.leftJoinWithMissing @[BR.StateAbbreviation, ET.DistrictTypeC, ET.DistrictName]
                                    modelDRA
                                    (FL.fold densityAndGoWFld modelDRADemo)
  when (not $ null missing) $ K.knitError $ "missing keys in state-leg modelDRADemo join with Density and GoW: " <> show missing
--  BR.logFrame modelDRADemoPlus
  let partyFilters = let f r =  F.rgetField @TwoPartyDShare r in DCC.SBCPartyData ((>= 0.5) . f) ((< 0.5) . f)
      categoryNames = ["Density", "%Grad-Among-White", "%Voters-Of-Color"]
      categoryFunctions = zipWith DCC.SBCCategoryData categoryNames
                          [F.rgetField @DT.PopPerSqMile
                          , F.rgetField @BRE.FracGradOfWhite
                          , (\r -> 1 - F.rgetField @BRE.FracWhiteNonHispanic r)
                          ]
      quantileBreaks = DCC.sbcQuantileBreaks 20 categoryFunctions modelDRADemoPlus
      quantileFunctionsE = DCC.sbcQuantileFunctions quantileBreaks
      quantileFunctionsDblE = (fmap (realToFrac @Int @Double) .) <<$>> quantileFunctionsE
      partyMediansE = DCC.partyMedians partyFilters quantileFunctionsDblE modelDRADemoPlus
      partyRanksE = DCC.partyRanks partyFilters quantileFunctionsDblE modelDRADemoPlus
      partyLoHisE = DCC.partyLoHis <$> partyRanksE
  partyLoHis <- K.knitEither partyLoHisE
  partyMedians <- K.knitEither partyMediansE
  let sbcE r = DCC.sbcComparison quantileFunctionsDblE partyMedians r
      sbcChartE :: K.KnitOne r => Text -> Either Text [DCC.SBCCategoryData DCC.SBComparison] -> K.Sem r ()
      sbcChartE dk sbcsNatE = do
        sbcsNat <- K.knitEither sbcsNatE
        _ <- K.addHvega Nothing Nothing
          $ DCC.sbcChart DCC.SBCState 20 10 (FV.ViewConfig 300 80 5)
          (Just categoryNames)
          (fmap (\(n, m) -> (realToFrac n, realToFrac m)) <<$>> partyLoHis)
          $ one (dk, True, sbcsNat)
        pure ()
  let sbcChartsFilter r =  (F.rgetField @ET.DistrictTypeC r, F.rgetField @ET.DistrictName r) `elem` spDists postSpec
      ppDType x = case x of
        ET.Congressional -> "Congressional"
        ET.StateUpper -> "Upper"
        ET.StateLower -> "Lower"

  traverse_ (uncurry sbcChartE)
    $ fmap (\r -> (ppDType (F.rgetField @ET.DistrictTypeC r) <> "-" <> F.rgetField @ET.DistrictName r, sbcE r))
    $ filter sbcChartsFilter
    $ FL.fold FL.list modelDRADemoPlus
  pure ()

dmColonnadeOverlap olMM cas =
  let state' = F.rgetField @DT.StateAbbreviation
      dType = F.rgetField @ET.DistrictTypeC
      dName = F.rgetField @ET.DistrictName
      dKey r = (dType r, dName r)
      dave' = round @_ @Int . (100*) . F.rgetField @TwoPartyDShare
      share50' = round @_ @Int . (100 *) . MT.ciMid . F.rgetField @BRE.ModeledShare
  in C.headed "State" (BR.toCell cas "State" "State" (BR.textToStyledHtml . state'))
     <> C.headed "House" (BR.toCell cas "District" "District" (BR.textToStyledHtml . printDType . dType))
     <> C.headed "District" (BR.toCell cas "District" "District" (BR.textToStyledHtml . dName))
     <> C.headed "Demographic Model (Blue Ripple)" (BR.toCell cas "Demographic" "Demographic" (BR.numberToStyledHtml "%d" . share50'))
     <> C.headed "Historical Model (Dave's Redistricting)" (BR.toCell cas "Historical" "Historical" (BR.numberToStyledHtml "%d" . dave'))
     <> C.headed "BR Stance" (BR.toCell cas "BR Stance" "BR Stance" (BR.textToStyledHtml . (\r -> brDistrictFramework DFLong DFUnk brShareRange draShareRangeSLD (share50' r) (dave' r))))
     <> C.headed "CD Overlaps" (BR.toCell cas "CD Overlaps" "CD Overlaps" (BR.textToStyledHtml . T.intercalate "," . fmap fst . M.toList . fromMaybe mempty . olMM . dKey))

data NewCDMapPostSpec = NewCDMapPostSpec Text (BR.PostPaths BR.Abs) (F.Frame Redistrict.DRAnalysis)
printDType :: ET.DistrictType -> Text
printDType ET.StateUpper = "Upper"
printDType ET.StateLower = "Lower"
printDType ET.Congressional = "Congressional"

newCongressionalMapAnalysis :: forall r.(K.KnitMany r, K.KnitOne r, BR.CacheEffects r)
                            => Bool
                            -> BR.CommandLine
                            -> NewCDMapPostSpec
                            -> BR.PostInfo
                            -> K.ActionWithCacheTime r (F.FrameRec BRE.CCESWithDensityEM)
                            -> K.ActionWithCacheTime r BRE.CCESAndCPSEM
                            -> K.ActionWithCacheTime r (F.FrameRec BRE.PUMSWithDensityEM) -- ACS data
                            -> K.ActionWithCacheTime r (F.FrameRec PostStratR) -- extant districts
                            -> K.ActionWithCacheTime r (F.FrameRec PostStratR) -- new districts
                            -> K.Sem r ()
newCongressionalMapAnalysis _ cmdLine postSpec postInfo _ ccesAndCPSEM_C acs_C extantDemo_C proposedDemo_C = K.wrapPrefix "newCongressionalMapsAnalysis" $ do
  let (NewCDMapPostSpec stateAbbr' postPaths' drAnalysis) = postSpec
  K.logLE K.Info $ "Re-building NewMaps " <> stateAbbr' <> " post"
  let --ccesAndCPS2018_C = fmap (BRE.ccesAndCPSForYears [2018]) ccesAndCPSEM_C
      ccesAndCPS2020_C = fmap (BRE.ccesAndCPSForYears [2020]) ccesAndCPSEM_C
      acs2020_C = fmap (BRE.acsForYears [2020]) acs_C
  extant <- K.ignoreCacheTime extantDemo_C
  proposed <- K.ignoreCacheTime proposedDemo_C
  acsForState <- fmap (F.filterFrame ((== stateAbbr') . F.rgetField @BR.StateAbbreviation)) $ K.ignoreCacheTime acs2020_C
  let extantPWLD = FL.fold (peopleWeightedLogDensityFld (F.rgetField @BRC.Count)) extant
      proposedPWLD = FL.fold (peopleWeightedLogDensityFld (F.rgetField @BRC.Count)) proposed
      acs2020PWLD = FL.fold (peopleWeightedLogDensityFld (F.rgetField @PUMS.Citizens)) acsForState
      rescaleExtant = rescaleDensity $ Numeric.exp (acs2020PWLD - extantPWLD)
      rescaleProposed = rescaleDensity $ Numeric.exp (acs2020PWLD - proposedPWLD)
  K.logLE K.Info $ "People-weighted log-density: acs=" <> show acs2020PWLD <> "; extant=" <> show extantPWLD <> "; proposed=" <> show proposedPWLD
  let addDistrict' r = r F.<+> ((ET.Congressional F.&: show (F.rgetField @ET.CongressionalDistrict r) F.&: V.RNil) :: F.Record [ET.DistrictTypeC, ET.DistrictName])
      addElexDShare r = let dv = F.rgetField @BRE.DVotes r
                            rv = F.rgetField @BRE.RVotes r
                        in r F.<+> (FT.recordSingleton @ElexDShare $ if (dv + rv) == 0 then 0 else (realToFrac dv/realToFrac (dv + rv)))
      mapGroup :: SB.GroupTypeTag (F.Record CDLocWStAbbrR) = SB.GroupTypeTag "CD"
      psInfoDM name = (mapGroup
                      , "DM" <> "_" <> name
                      )
      stanParams = SC.StanMCParameters 4 4 (Just 1000) (Just 1000) (Just 0.8) (Just 10) Nothing
      modelDM :: BRE.Model k -> Text -> K.ActionWithCacheTime r (F.FrameRec PostStratR)
              -> K.Sem r (F.FrameRec (BRE.ModelResultsR CDLocWStAbbrR))
      modelDM _ name x = do
        K.ignoreCacheTimeM $ BRE.electionModelDM False cmdLine False (Just stanParams) modelDir modelVariant 2020 (psInfoDM name) ccesAndCPS2020_C x

  proposedBaseHV <- modelDM modelVariant (stateAbbr' <> "_Proposed") (rescaleProposed . fmap F.rcast <$> proposedDemo_C)
  extantBaseHV <- modelDM modelVariant (stateAbbr' <> "_Extant") (rescaleExtant . fmap F.rcast <$> extantDemo_C)

  let extantForPost = extantBaseHV
      proposedForPost = proposedBaseHV
  elections_C <- BR.houseElectionsWithIncumbency
  elections <- fmap (onlyState stateAbbr' . F.filterFrame ((==2020) . F.rgetField @BR.Year)) $ K.ignoreCacheTime elections_C
  K.logLE K.Diagnostic $ "flattening "
  flattenedElections <- fmap (addDistrict' . addElexDShare)
                        <$> (K.knitEither $ FL.foldM (BRE.electionF @[BR.Year, BR.StateAbbreviation, BR.CongressionalDistrict]) $ F.rcast <$> elections)
  let
      oldDistrictsNoteName = BR.Used "Old_Districts"
  extantDemo <- K.ignoreCacheTime extantDemo_C
  mOldDistrictsUrl <- BR.brNewNote postPaths' postInfo oldDistrictsNoteName (stateAbbr' <> ": Old Districts") $ do
    BR.brAddNoteMarkDownFromFile postPaths' oldDistrictsNoteName "_intro"
    let extantByModelShare = modelShareSort cdDistLabel extantBaseHV --extantPlusStateAndStateRace_RaceDensityNC
    _ <- K.addHvega Nothing Nothing
         $ BRV.demoCompare
         ("Race", show . F.rgetField @DT.Race5C, raceSort)
         ("Education", show . F.rgetField @DT.CollegeGradC, eduSort)
         (F.rgetField @BRC.Count)
         ("District", \r -> F.rgetField @DT.StateAbbreviation r <> "-" <> F.rgetField @ET.DistrictName r, Just extantByModelShare)
         (Just ("log(Density)", Numeric.log . F.rgetField @DT.PopPerSqMile))
         (stateAbbr' <> " Old: By Race and Education")
         (FV.ViewConfig 600 600 5)
         extantDemo
    BR.brAddNoteMarkDownFromFile postPaths' oldDistrictsNoteName "_afterDemographicsBar"
    let (demoElexModelExtant, missing1E, missing2E)
          = FJ.leftJoin3WithMissing @[DT.StateAbbreviation, ET.DistrictTypeC, ET.DistrictName]
            (onlyState stateAbbr' extantDemo)
            flattenedElections
            extantBaseHV
--            extantPlusStateAndStateRace_RaceDensityNC
    when (not $ null missing1E) $ do
      BR.logFrame' K.Warning extantDemo
      K.knitError $ "Missing keys in join of extant demographics and election results:" <> show missing1E
    when (not $ null missing2E) $ K.knitError $ "Missing keys in join of extant demographics and model:" <> show missing2E
    _ <- K.addHvega Nothing Nothing
      $ BRV.demoCompareXYCS
      "District"
     "% non-white"
      "% college grad"
      "Modeled D-Edge"
      "log density"
      (stateAbbr' <> " demographic scatter")
      (FV.ViewConfig 600 600 5)
      (FL.fold (xyFold' cdDistLabel) demoElexModelExtant)
    BR.brAddNoteMarkDownFromFile postPaths' oldDistrictsNoteName "_afterDemographicsScatter"

    let (oldMapsCompare, missing)
          = FJ.leftJoinWithMissing @[BR.Year, DT.StateAbbreviation, ET.DistrictTypeC, ET.DistrictName]
            flattenedElections
            extantForPost
    when (not $ null missing) $ K.knitError $ "Missing keys in join of election results and model:" <> show missing
    _ <- K.addHvega Nothing Nothing
         $ modelAndElectionScatter
         True
         (stateAbbr' <> " 2020: Election vs Demographic Model")
         (FV.ViewConfig 600 600 5)
         (fmap F.rcast oldMapsCompare)
    BR.brAddNoteMarkDownFromFile postPaths' oldDistrictsNoteName "_afterModelElection"
    BR.brAddRawHtmlTable
      ("2020 Dem Vote Share, " <> stateAbbr' <> ": Demographic Model vs. Election Results")
      (BHA.class_ "brTable")
      (extantModeledColonnade mempty)
      oldMapsCompare
  oldDistrictsNoteUrl <- K.knitMaybe "extant districts Note Url is Nothing" $ mOldDistrictsUrl
  let oldDistrictsNoteRef = "[oldDistricts]:" <> oldDistrictsNoteUrl
  BR.brAddPostMarkDownFromFileWith postPaths' "_intro" (Just oldDistrictsNoteRef)
  let (modelAndDR, _)
        = FJ.leftJoinWithMissing @[DT.StateAbbreviation, ET.DistrictTypeC, ET.DistrictName]
          proposedForPost
--          proposedPlusStateAndStateRace_RaceDensityNC
          (fmap addTwoPartyDShare drAnalysis)
  _ <- K.addHvega Nothing Nothing
       $ modelAndDaveScatterChart
       True
       (stateAbbr' <> " 2022: Historical vs. Demographic models")
       (FV.ViewConfig 600 600 5)
       (fmap F.rcast modelAndDR)
  BR.brAddPostMarkDownFromFile postPaths' "_afterDaveModel"
  let sortedModelAndDRA = reverse $ sortOn (MT.ciMid . F.rgetField @BRE.ModeledShare) $ FL.fold FL.list modelAndDR
  BR.brAddRawHtmlTable
    ("Calculated Dem Vote Share, " <> stateAbbr' <> " 2022: Demographic Model vs. Historical Model (DR)")
    (BHA.class_ "brTable")
    (daveModelColonnade brShareRange draShareRangeCD $ modelVsHistoricalTableCellStyle brShareRange draShareRangeCD)
    sortedModelAndDRA
  BR.brAddPostMarkDownFromFile postPaths' "_daveModelTable"
--  BR.brAddPostMarkDownFromFile postPaths' "_beforeNewDemographics"
  let proposedByModelShare = modelShareSort cdDistLabel proposedBaseHV --proposedPlusStateAndStateRace_RaceDensityNC
  proposedDemo <- K.ignoreCacheTime proposedDemo_C
  _ <- K.addHvega Nothing Nothing
       $ BRV.demoCompare
       ("Race", show . F.rgetField @DT.Race5C, raceSort)
       ("Education", show . F.rgetField @DT.CollegeGradC, eduSort)
       (F.rgetField @BRC.Count)
       ("District", \r -> F.rgetField @DT.StateAbbreviation r <> "-" <> F.rgetField @ET.DistrictName r, Just proposedByModelShare)
       (Just ("log(Density)", (\x -> x) . Numeric.log . F.rgetField @DT.PopPerSqMile))
       (stateAbbr' <> " New: By Race and Education")
       (FV.ViewConfig 600 600 5)
       proposedDemo
  let (demoModelAndDR, missing1P, missing2P)
        = FJ.leftJoin3WithMissing @[DT.StateAbbreviation, ET.DistrictTypeC, ET.DistrictName]
          (onlyState stateAbbr' proposedDemo)
          proposedForPost
--          proposedPlusStateAndStateRace_RaceDensityNC
          (fmap addTwoPartyDShare drAnalysis)
  when (not $ null missing1P) $ K.knitError $ "Missing keys when joining demographics results and model: " <> show missing1P
  when (not $ null missing2P) $ K.knitError $ "Missing keys when joining demographics results and Dave's redistricting analysis: " <> show missing2P
--  BR.brAddPostMarkDownFromFile postPaths' "_afterNewDemographicsBar"
  _ <- K.addHvega Nothing Nothing
    $ BRV.demoCompareXYCS
    "District"
    "% non-white"
    "% college grad"
    "Modeled D-Edge"
    "log density"
    (stateAbbr' <> " demographic scatter")
    (FV.ViewConfig 600 600 5)
    (FL.fold (xyFold' cdDistLabel) demoModelAndDR)
  BR.brAddPostMarkDownFromFileWith postPaths' "_afterNewDemographics" (Just oldDistrictsNoteRef)
  BR.brAddSharedMarkDownFromFile postPaths' "modelExplainer"
  return ()

safeLog :: Double -> Double
safeLog x = if x < 1e-12 then 0 else Numeric.log x

xyFold' labelFunc = FMR.mapReduceFold
                    FMR.noUnpack
                    (FMR.assignKeysAndData @[DT.StateAbbreviation, ET.DistrictName, ET.DistrictTypeC] @[BRC.Count, DT.Race5C, DT.CollegeGradC, DT.PopPerSqMile, BRE.ModeledShare])
                    (FMR.foldAndLabel foldData (\k (x :: Double, y :: Double, c, s) -> (labelFunc k, x, y, c, s)))
  where
    allF = FL.premap (F.rgetField @BRC.Count) FL.sum
    wnhF = FL.prefilter ((/= DT.R5_WhiteNonHispanic) . F.rgetField @DT.Race5C) allF
    gradsF = FL.prefilter ((== DT.Grad) . F.rgetField @DT.CollegeGradC) allF
    densityF = fmap (fromMaybe 0) $ FL.premap (safeLog . F.rgetField @DT.PopPerSqMile) FL.last
    modelF = fmap (fromMaybe 0) $ FL.premap (MT.ciMid . F.rgetField @BRE.ModeledShare) FL.last
    foldData = (\a wnh grads m d -> (100 * realToFrac wnh/ realToFrac a, 100 * realToFrac grads/realToFrac a, 100*(m - 0.5), d))
               <$> allF <*> wnhF <*> gradsF <*> modelF <*> densityF


xyFold2' labelFunc = FMR.mapReduceFold
                     FMR.noUnpack
                     (FMR.assignKeysAndData @[DT.StateAbbreviation, ET.DistrictName, ET.DistrictTypeC] @[BRC.Count, DT.Race5C, DT.CollegeGradC, DT.PopPerSqMile, BRE.ModeledShare])
                     (FMR.foldAndLabel foldData (\k (x :: Double, y :: Double, c, s) -> (labelFunc k, x, y, c, s)))
  where
    allF = FL.premap (F.rgetField @BRC.Count) FL.sum
    wnhF = FL.prefilter ((/= DT.R5_WhiteNonHispanic) . F.rgetField @DT.Race5C) allF
    wnhGrad r = F.rgetField @DT.Race5C r == DT.R5_WhiteNonHispanic && F.rgetField @DT.CollegeGradC r == DT.Grad
    gradsF = FL.prefilter wnhGrad allF
    densityF = fmap (fromMaybe 0) $ FL.premap (safeLog . F.rgetField @DT.PopPerSqMile) FL.last
    modelF = fmap (fromMaybe 0) $ FL.premap (MT.ciMid . F.rgetField @BRE.ModeledShare) FL.last
    foldData = (\a wnh grads m d -> (100 * realToFrac wnh/ realToFrac a, 100 * realToFrac grads/realToFrac a, 100*(m - 0.5), d))
               <$> allF <*> wnhF <*> gradsF <*> modelF <*> densityF


raceSort = Just $ show <$> [DT.R5_WhiteNonHispanic, DT.R5_Black, DT.R5_Hispanic, DT.R5_Asian, DT.R5_Other]

eduSort = Just $ show <$> [DT.NonGrad, DT.Grad]

{-
textDist :: F.ElemOf rs ET.DistrictName => F.Record rs -> Text
textDist r = let x = F.rgetField @ET.DistrictName r in if x < 10 then "0" <> show x else show x
-}

cdDistLabel :: (F.ElemOf rs ET.DistrictName, F.ElemOf rs BR.StateAbbreviation) => F.Record rs -> Text
cdDistLabel r = F.rgetField @DT.StateAbbreviation r <> "-" <> F.rgetField @ET.DistrictName r  --textDist r

sldDistLabel :: (F.ElemOf rs ET.DistrictTypeC, F.ElemOf rs ET.DistrictName, F.ElemOf rs BR.StateAbbreviation) => F.Record rs -> Text
sldDistLabel r = F.rgetField @BR.StateAbbreviation r <> "-" <> dtLabel (F.rgetField @ET.DistrictTypeC r) <> "-" <> F.rgetField @ET.DistrictName r
  where
    dtLabel dt = case dt of
      ET.StateUpper -> "U"
      ET.StateLower -> "L"
      ET.Congressional -> "C"

modelShareSort :: (Foldable f
                  , F.ElemOf rs BRE.ModeledShare
                  , F.ElemOf rs ET.DistrictName
                  , F.ElemOf rs BR.StateAbbreviation
                  ) => (F.Record rs -> Text) -> f (F.Record rs) -> [Text]
modelShareSort labelFunc = reverse . fmap fst . sortOn snd
                           . fmap (\r -> (labelFunc r, MT.ciMid $ F.rgetField @BRE.ModeledShare r))
                           . FL.fold FL.list

brShareRange :: (Int, Int)
brShareRange = (45, 55)

draShareRangeCD :: (Int, Int)
draShareRangeCD = (47, 53)

draShareRangeSLD :: (Int, Int)
draShareRangeSLD = (43, 57)

between :: (Int, Int) -> Int -> Bool
between (l, h) x = x >= l && x <= h


modelVsHistoricalTableCellStyle :: (F.ElemOf rs BRE.ModeledShare
                                   , F.ElemOf rs TwoPartyDShare)
                                => (Int, Int) -> (Int, Int) -> BR.CellStyle (F.Record rs) String
modelVsHistoricalTableCellStyle brSR draSR = mconcat [longShotCS, leanRCS, leanDCS, safeDCS, longShotDRACS, leanRDRACS, leanDDRACS, safeDDRACS]
  where
    safeR (l, _) x = x <= l
    leanR (l, _) x = x < 50 && x  >= l
    leanD (_, u) x = x >= 50 && x <= u
    safeD (_, u) x = x > u
    modMid = round . (100*). MT.ciMid . F.rgetField @BRE.ModeledShare
    bordered c = "border: 3px solid " <> c
    longShotCS  = bordered "red" `BR.cellStyleIf` \r h -> safeR brSR (modMid r) && h == "Demographic"
    leanRCS =  bordered "pink" `BR.cellStyleIf` \r h -> leanR brSR (modMid r) && h `elem` ["Demographic"]
    leanDCS = bordered "skyblue" `BR.cellStyleIf` \r h -> leanD brSR (modMid r) && h `elem` ["Demographic"]
    safeDCS = bordered "blue"  `BR.cellStyleIf` \r h -> safeD brSR (modMid r) && h == "Demographic"
    dra = round . (100*) . F.rgetField @TwoPartyDShare
    longShotDRACS = bordered "red" `BR.cellStyleIf` \r h -> safeR draSR (dra r) && h == "Historical"
    leanRDRACS = bordered "pink" `BR.cellStyleIf` \r h -> leanR draSR (dra r) && h == "Historical"
    leanDDRACS = bordered "skyblue" `BR.cellStyleIf` \r h -> leanD draSR (dra r)&& h == "Historical"
    safeDDRACS = bordered "blue" `BR.cellStyleIf` \r h -> safeD draSR (dra r) && h == "Historical"

data DistType = SafeR | LeanR | Tossup | LeanD | SafeD deriving (Eq, Ord, Show)
distType :: Int -> Int -> Int -> DistType
distType safeRUpper safeDLower x
  | x < safeRUpper = SafeR
  | x >= safeRUpper && x < 50 = LeanR
  | x == 50 = Tossup
  | x > 50 && x <= safeDLower = LeanD
  | otherwise = SafeD

modelDRALeans :: (Int, Int) -> (Int, Int) -> Int -> Int -> (DistType, DistType)
modelDRALeans brRange draRange brModel dra = (uncurry distType brRange brModel, uncurry distType draRange dra)

data BRDFStyle = DFLong | DFShort deriving (Eq)
data DFIncumbency = DFUnk | DFInc ET.PartyT | DFOpen deriving (Eq)

brDistrictFramework :: BRDFStyle -> DFIncumbency -> (Int, Int) -> (Int, Int) -> Int -> Int -> Text
brDistrictFramework long inc brRange draRange brModel dra =
  let ifLong x = if long == DFLong then " (" <> x <> ")" else ""
      ifInc o d r = case inc of
        DFUnk -> o
        DFOpen -> o
        DFInc ET.Other -> o
        DFInc ET.Democratic -> d
        DFInc ET.Republican -> r
      topLeft = "Flippable" <> ifLong "Strongly D-leaning"
      midLeft =  "Becoming Flippable" <> ifLong "More balanced than Advertised"
      topCenter = ifInc ("Toss-up" <> ifLong "Highly Winnable by D")
                      ("Safe D" <> ifLong "No near-term D risk")
                      ("Toss-up" <> ifLong "Highly Winnable by D")
      center = "Toss-up" <> ifLong "Down to the Wire"
      bottomCenterL = ifInc ("Safe R" <> ifLong "No near-term D hope")
                      ("Toss-up" <> ifLong "Highly Vulnerable for D")
                      ("Safe R" <> ifLong "No near-term D hope")
      bottomCenterR = ("Toss-up" <> ifLong "Highly vulnerable for D")
--      ifInc ("Toss-up" <> ifLong "Highly vulnerable for D")
--                      ("Toss-up" <> ifLong "Highly Vulnerable for D")
--                      ("Toss-up" <> ifLong "Highly Vulnerable for D")
      midRight = "Becoming At-Risk" <> ifLong "More Balanced than Advertised"
      bottomRight = "At-Risk" <> ifLong "Moving away from D"
  in case modelDRALeans brRange draRange brModel dra of
    (SafeR, SafeR) -> "Safe R" <> ifLong "No near-term D hope"
    (LeanR, SafeR) -> midLeft
    (Tossup, SafeR) -> midLeft
    (LeanD, SafeR) -> topLeft
    (SafeD, SafeR) -> topLeft
    (SafeR, LeanR) -> bottomCenterL
    (LeanR, LeanR) -> center
    (Tossup, LeanR) -> center
    (LeanD, LeanR) -> center
    (SafeD, LeanR) -> topCenter
    (SafeR, Tossup) -> bottomCenterR
    (LeanR, Tossup) -> center
    (Tossup, Tossup) -> center
    (LeanD, Tossup) -> center
    (SafeD, Tossup) -> topCenter
    (SafeR, LeanD) -> bottomCenterR
    (LeanR, LeanD) -> center
    (Tossup, LeanD) -> center
    (LeanD, LeanD) -> center
    (SafeD, LeanD) -> topCenter
    (SafeR, SafeD) -> bottomRight
    (LeanR, SafeD) -> bottomRight
    (Tossup, SafeD) -> midRight
    (LeanD, SafeD) -> midRight
    (SafeD, SafeD) -> "Safe D" <> ifLong "No near-term D risk"


daveModelColonnade brSR draSR cas =
  let state' = F.rgetField @DT.StateAbbreviation
      dName = F.rgetField @ET.DistrictName
      dave' = round @_ @Int . (100*) . F.rgetField @TwoPartyDShare
      share50' = round @_ @Int . (100 *) . MT.ciMid . F.rgetField @BRE.ModeledShare
  in C.headed "State" (BR.toCell cas "State" "State" (BR.textToStyledHtml . state'))
     <> C.headed "District" (BR.toCell cas "District" "District" (BR.textToStyledHtml . dName))
     <> C.headed "Demographic Model (Blue Ripple)" (BR.toCell cas "Demographic" "Demographic" (BR.numberToStyledHtml "%d" . share50'))
     <> C.headed "Historical Model (Dave's Redistricting)" (BR.toCell cas "Historical" "Historical" (BR.numberToStyledHtml "%d" . dave'))
     <> C.headed "BR Stance" (BR.toCell cas "BR Stance" "BR Stance" (BR.textToStyledHtml . (\r -> brDistrictFramework DFLong DFUnk brSR draSR (share50 r) (dave r))))


extantModeledColonnade cas =
  let state' = F.rgetField @DT.StateAbbreviation
      dName = F.rgetField @ET.DistrictName
      share50' = round @_ @Int . (100*) . MT.ciMid . F.rgetField @BRE.ModeledShare
      elexDVotes = F.rgetField @BRE.DVotes
      elexRVotes = F.rgetField @BRE.RVotes
      elexShare r = realToFrac @_ @Double (elexDVotes r)/realToFrac (elexDVotes r + elexRVotes r)
  in C.headed "State" (BR.toCell cas "State" "State" (BR.textToStyledHtml . state'))
     <> C.headed "District" (BR.toCell cas "District" "District" (BR.textToStyledHtml . dName))
     <> C.headed "Demographic Model (Blue Ripple)" (BR.toCell cas "Demographic" "Demographic" (BR.numberToStyledHtml "%d" . share50'))
     <> C.headed "2020 Election" (BR.toCell cas "Election" "Election" (BR.numberToStyledHtml "%2.0f" . (100*) . elexShare))
--


{-
race5FromCPS :: F.Record BRE.CPSVByCDR -> DT.Race5
race5FromCPS r =
  let race4A = F.rgetField @DT.RaceAlone4C r
      hisp = F.rgetField @DT.HispC r
  in DT.race5FromRaceAlone4AndHisp True race4A hisp
-}

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
                         -> F.FrameRec [DT.StateAbbreviation, ET.DistrictName, ElexDShare, BRE.ModelDesc, BRE.ModeledShare]
                         -> GV.VegaLite
modelAndElectionScatter single title vc rows =
  let toVLDataRec = FVD.asVLData GV.Str "State"
                    V.:& FVD.asVLData GV.Str  "District"
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
                         -> F.FrameRec ([BR.StateAbbreviation, ET.DistrictName, BRE.ModelDesc, BRE.ModeledShare, TwoPartyDShare])
                         -> GV.VegaLite
modelAndDaveScatterChart single title vc rows =
  let toVLDataRec = FVD.asVLData GV.Str "State"
                    V.:& FVD.asVLData GV.Str "District"
                    V.:& FVD.asVLData GV.Str "Demographic Model Type"
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
--      resolve = GV.resolve . GV.resolution (GV.RAxis [(GV.ChY, GV.Shared)])
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

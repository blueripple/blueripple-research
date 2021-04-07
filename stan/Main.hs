{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE OverloadedLabels #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-# OPTIONS_GHC -O0 #-}


module Main where

import qualified BlueRipple.Data.ACS_PUMS as PUMS
import qualified BlueRipple.Data.CCES as CCES
import qualified BlueRipple.Data.CountFolds as BRCF
import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.ElectionTypes as ET
import qualified BlueRipple.Model.House.ElectionResult as BRE
import qualified BlueRipple.Model.StanMRP as MRP
import qualified BlueRipple.Model.StanCCES as BRS
import qualified BlueRipple.Utilities.KnitUtils as BR
import qualified BlueRipple.Utilities.FramesUtils as BRF
import qualified BlueRipple.Utilities.TableUtils as BR

import qualified Control.Foldl as FL
import qualified Data.List as List
import qualified Data.IntMap as IM
import qualified Data.Map as M
--import qualified Data.Random.Source.PureMT as PureMT
import qualified Data.Semigroup as Semigroup
import qualified Data.Set as S
import Data.String.Here (here)
import qualified Data.Text as T
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V
import qualified Data.Vector as Vector
import qualified Frames as F
import qualified Control.MapReduce as MR
import qualified Frames.MapReduce as FMR
import qualified Frames.Folds as FF
import qualified Frames.SimpleJoins  as FJ
import qualified Frames.Transform  as FT
--import qualified Frames.Table as FTable
import qualified Frames.Visualization.VegaLite.Correlation as FV
import qualified Frames.Visualization.VegaLite.Histogram as FV
import qualified Graphics.Vega.VegaLite as GV
import qualified Graphics.Vega.VegaLite.Compat as FV
--import qualified Frames.Visualization.VegaLite.Data as FV

import Graphics.Vega.VegaLite.Configuration as FV
  ( AxisBounds (DataMinMax),
    ViewConfig (ViewConfig),
  )
import qualified Graphics.Vega.VegaLite.Configuration as FV
import qualified Frames.Visualization.VegaLite.Data
                                               as FV
import qualified Visualization.VegaLite.Histogram as VL

import qualified Data.MapRow as MapRow
import qualified Knit.Report as K
import qualified Knit.Effect.AtomicCache as KC
import qualified Numeric
import qualified Optics
import Optics.Operators
--import Polysemy.RandomFu (RandomFu, runRandomIOPureMT)

import qualified Stan.ModelConfig as SC
import qualified Stan.ModelBuilder as SB
import qualified Stan.ModelBuilder.BuildingBlocks as SB
--import qualified Stan
import qualified Stan.Parameters as SP
import qualified CmdStan as CS
import qualified Stan.RScriptBuilder as SR
--import qualified Text.Blaze.Html5              as BH

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
          { K.outerLogPrefix = Just "HouseModel"
          , K.logIf = K.logDiagnostic
          , K.pandocWriterConfig = pandocWriterConfig
          , K.serializeDict = BR.flatSerializeDict
          , K.persistCache = KC.persistStrictByteString (\t -> toString (cacheDir <> "/" <> t))
          }
--  let pureMTseed = PureMT.pureMT 1
  --
--  resE <- K.knitHtmls knitConfig testHouseModel
  resE <- K.knitHtmls knitConfig testStanMRP

  case resE of
    Right namedDocs ->
      K.writeAllPandocResultsWithInfoAsHtml "house_model" namedDocs
    Left err -> putStrLn $ "Pandoc Error: " ++ show err

type PctTurnout = "PctTurnout" F.:-> Double
type DShare = "DShare" F.:-> Double

--districtKey :: F.Record BRE.CCESByCD -> Text
districtKey r = F.rgetField @BR.StateAbbreviation r <> "-" <> show (F.rgetField @BR.CongressionalDistrict r)

districtPredictors r = Vector.fromList $ [Numeric.log (F.rgetField @DT.PopPerSqMile r)
                                         , realToFrac $F.rgetField @BRE.Incumbency r]

ccesGroupBuilder :: SB.StanGroupBuilderM (F.Record BRE.CCESByCDR) ()
ccesGroupBuilder = do
  SB.addGroup "CD" $ SB.makeIndexByCounting show districtKey
  SB.addGroup "State" $ SB.makeIndexByCounting show $ F.rgetField @BR.StateAbbreviation
  SB.addGroup "Race" $ SB.makeIndexFromEnum (F.rgetField @DT.Race5C)
  SB.addGroup "Sex" $ SB.makeIndexFromEnum (F.rgetField @DT.SexC)
  SB.addGroup "Education" $ SB.makeIndexFromEnum (F.rgetField @DT.CollegeGradC)
  SB.addGroup "Age" $ SB.makeIndexFromEnum (F.rgetField @DT.SimpleAgeC)

cpsVGroupBuilder :: [Text] -> [Text] -> SB.StanGroupBuilderM (F.Record BRE.CPSVByCDR) ()
cpsVGroupBuilder districts states = do
  SB.addGroup "CD" $ SB.makeIndexFromFoldable show districtKey districts
  SB.addGroup "State" $ SB.makeIndexFromFoldable show (F.rgetField @BR.StateAbbreviation) states
  SB.addGroup "Race" $ SB.makeIndexFromEnum (F.rgetField @DT.RaceAlone4C)
  SB.addGroup "Ethnicity" $ SB.makeIndexFromEnum (F.rgetField @DT.HispC)
  SB.addGroup "Sex" $ SB.makeIndexFromEnum (F.rgetField @DT.SexC)
  SB.addGroup "Education" $ SB.makeIndexFromEnum (F.rgetField @DT.CollegeGradC)
  SB.addGroup "Age" $ SB.makeIndexFromEnum (F.rgetField @DT.SimpleAgeC)

race5FromPUMS :: F.Record BRE.PUMSByCDR -> DT.Race5
race5FromPUMS r =
  let race4A = F.rgetField @DT.RaceAlone4C r
      hisp = F.rgetField @DT.HispC r
  in DT.race5FromRaceAlone4AndHisp True race4A hisp

pumsPSGroupRowMap :: SB.GroupRowMap (F.Record BRE.PUMSByCDR)
pumsPSGroupRowMap = SB.addRowMap "CD" districtKey
                    $ SB.addRowMap "State" (F.rgetField @BR.StateAbbreviation)
                    $ SB.addRowMap "Sex" (F.rgetField @DT.SexC)
                    $ SB.addRowMap "Race" (F.rgetField @DT.RaceAlone4C)
                    $ SB.addRowMap "Ethnicity" (F.rgetField @DT.HispC)
                    $ SB.addRowMap "Age" (F.rgetField @DT.SimpleAgeC)
                    $ SB.addRowMap "Education" (F.rgetField @DT.CollegeGradC)
                    $ SB.emptyGroupRowMap

catsPSGroupRowMap :: SB.GroupRowMap (F.Record BRE.AllCatR)
catsPSGroupRowMap = SB.addRowMap @Text "State" (const "NY-21")
                    $ SB.addRowMap "Sex" (F.rgetField @DT.SexC)
                    $ SB.addRowMap "Education" (F.rgetField @DT.CollegeGradC) SB.emptyGroupRowMap


dataAndCodeBuilder :: Typeable modelRow
                       => (modelRow -> Int)
                       -> (modelRow -> Int)
                       -> MRP.BuilderM modelRow BRE.CCESAndPUMS ()
dataAndCodeBuilder totalF succF = do
  SB.addIndexedDataSet "CD" (SB.ToFoldable BRE.districtRows) districtKey
  vTotal <- SB.addCountData "T" "N" totalF
  vSucc <- SB.addCountData "S" "N" succF
  alphaE <- MRP.intercept "alpha" 2
  (feCDE, xBetaE, betaE) <- MRP.addFixedEffects @(F.Record BRE.DistrictDataR)
                            True
                            "data"
                            2 -- prior
                            (SB.RowTypeTag  "CD")
                            (MRP.FixedEffects 2 districtPredictors)
  gSexE <- MRP.addMRGroup 2 2 0.01 "Sex"
  gRaceE <- MRP.addMRGroup 2 2 0.01 "Race"
  gEthE <- MRP.addMRGroup 2 2 0.01 "Ethnicity"
  gAgeE <- MRP.addMRGroup 2 2 0.01 "Age"
  gEduE <- MRP.addMRGroup 2 2 0.01 "Education"
  gStateE <- MRP.addMRGroup 2 2 0.01 "State"
  let dist = SB.binomialLogitDist vSucc vTotal
      logitPE = SB.multiOp "+" $ alphaE :| [feCDE, gSexE, gRaceE, gEthE, gAgeE, gEduE, gStateE]
  SB.sampleDistV dist logitPE
  SB.generatePosteriorPrediction (SB.StanVar "SPred" $ SB.StanArray [SB.NamedDim "N"] SB.StanInt) dist logitPE

  SB.addUnIndexedDataSet "ACS" (SB.ToFoldable BRE.pumsRows)
  MRP.addPostStratification
    dist
    logitPE
    "State"
    (SB.RowTypeTag "ACS")
    pumsPSGroupRowMap
    (S.fromList ["CD", "Sex", "Race","Ethnicity","Age","Education"])
    (realToFrac . F.rgetField @PUMS.Citizens)
    MRP.PSShare
    (Just $ SB.GroupTypeTag @Text "State")

--  MRP.addPostStratification dist logitPE "Sex" (SB.ToFoldable BRE.allCategoriesRows) (Set.fromList ["Sex","CD"]) catsPSGroupRowMap (const 1) MRP.PSShare (Just $ SB.GroupTypeTag @DT.Sex "Sex")

indexStanResults :: Ord k => IM.IntMap k -> Vector.Vector a -> Either Text (Map k a)
indexStanResults im v = do
  when (IM.size im /= Vector.length v) $ Left "Mismatched sizes in indexStanResults"
  return $ M.fromList $ zip (IM.elems im) (Vector.toList v)

extractTestResults :: K.KnitEffects r => SC.ResultAction r d SB.GroupIntMaps () (Map Text [Double])
extractTestResults = SC.UseSummary f where
  f summary _ aAndEb_C = do
    let eb_C = fmap snd aAndEb_C
    eb <- K.ignoreCacheTime eb_C
    groupIndexes <- K.knitEither eb
    psIndexIM <- K.knitEither $ SB.getGroupIndex @Text "State" groupIndexes
    vResults <- K.knitEither $ fmap (SP.getVector . fmap CS.percents) $ SP.parse1D "PS_State" (CS.paramStats summary)
    K.knitEither $ indexStanResults psIndexIM vResults

subSampleCCES :: K.KnitEffects r => Word32 -> Int -> BRE.CCESAndPUMS -> K.Sem r BRE.CCESAndPUMS
subSampleCCES seed samples (BRE.CCESAndPUMS cces cps pums dist cats) = do
  subSampledCCES <- K.liftKnit @IO $ BR.sampleFrame seed samples cces
  return $ BRE.CCESAndPUMS subSampledCCES cps pums dist cats

countInCategory :: (Eq a, Num b) => (F.Record rs -> b) -> (F.Record rs -> a) -> [a] -> FL.Fold (F.Record rs) [(a, b)]
countInCategory count key as =
  let countF a = fmap (a,) $ FL.prefilter ((== a) . key) $ FL.premap count FL.sum
  in traverse countF as

testStanMRP :: forall r. (K.KnitMany r, BR.CacheEffects r) => K.Sem r ()
testStanMRP = do
  K.logLE K.Info "Data prep..."
  data_C' <- fmap (BRE.ccesAndPUMSForYear 2018) <$> BRE.prepCCESAndPums False
  let data_C = {- KC.wctBind (subSampleCCES 3 2000) $ -} data_C'
  dat <- K.ignoreCacheTime data_C
  let popFilter r = True --F.rgetField @DT.Race5C r == DT.R5_Black
      surveyedF = FL.prefilter popFilter $ FL.premap (F.rgetField @BRE.Surveyed) FL.sum
      votedF = FL.prefilter popFilter $ FL.premap (F.rgetField @BRE.TVotes) FL.sum
      turnoutF = (/) <$> fmap realToFrac votedF <*> fmap realToFrac surveyedF
      femaleTurnoutF = FL.prefilter ((== DT.Female) . F.rgetField @DT.SexC) turnoutF
      maleTurnoutF = FL.prefilter ((== DT.Male) . F.rgetField @DT.SexC) turnoutF
      (ft, mt) = FL.fold ((,) <$> femaleTurnoutF <*> maleTurnoutF) $ BRE.ccesRows dat
      raceCountsCCES = FL.fold (countInCategory (F.rgetField @BRE.Surveyed) (F.rgetField @DT.Race5C) [(minBound :: DT.Race5)..]) $ BRE.ccesRows dat
      raceCountsCPSV_UW = FL.fold (countInCategory (F.rgetField @BRCF.Count) (F.rgetField @DT.RaceAlone4C) [(minBound :: DT.RaceAlone4)..]) $ BRE.cpsVRows dat
      raceCountsCPSV_W = FL.fold (countInCategory (F.rgetField @BRCF.WeightedCount) (F.rgetField @DT.RaceAlone4C) [(minBound :: DT.RaceAlone4)..]) $ BRE.cpsVRows dat
      raceCountsPUMS = FL.fold (countInCategory (F.rgetField @PUMS.Citizens) (F.rgetField @DT.RaceAlone4C) [(minBound :: DT.RaceAlone4)..]) $ BRE.pumsRows dat
      districtsinCPSV = FL.fold (FL.premap districtKey FL.set) $ BRE.cpsVRows dat
--  K.logLE K.Info $ "In (subsampled) CCES data (Female Turnout, Male Turnout) " <> show (ft, mt)
  K.logLE K.Info $ "CPS Race counts (Unweighted): " <> show raceCountsCPSV_UW
  K.logLE K.Info $ "CPS Race counts (Weighted): " <> show raceCountsCPSV_W
  K.logLE K.Info $ "PUMS Race counts: " <> show raceCountsPUMS
  K.logLE K.Info "Building json data wrangler and model code..."
  let cpsVBuilder = dataAndCodeBuilder (round . F.rgetField @BRCF.WeightedCount) (round . F.rgetField @BRCF.WeightedSuccesses)
      (districts, states) = FL.fold
                            ((,)
                              <$> (FL.premap districtKey FL.list)
                              <*> (FL.premap (F.rgetField @BR.StateAbbreviation) FL.list)
                            )
                            $ BRE.districtRows dat
      cpsVGroups = cpsVGroupBuilder districts states
  (dw, stanCode) <- K.knitEither $ MRP.buildDataWranglerAndCode cpsVGroups () cpsVBuilder dat (SB.ToFoldable BRE.cpsVRows)
--  K.logLE K.Info $ show (FL.fold (FL.premap (F.rgetField @BRE.Surveyed) FL.sum) $ BRE.ccesRows dat) <> " people surveyed in mrpData.modeled"
  res_C <- MRP.runMRPModel
    True
    (Just "stan/mrp/cpsV")
    "test"
    "test"
    dw
    stanCode
    "S"
    extractTestResults
    data_C
    (Just 1000)
    (Just 0.95)
    Nothing
  res <- K.ignoreCacheTime res_C
  K.logLE K.Info $ "results: " <> show res

testHouseModel :: forall r. (K.KnitMany r, BR.CacheEffects r) => K.Sem r ()
testHouseModel = do
  K.logLE K.Info "Data prep..."
  houseData_C <- BRE.prepCachedDataPUMS False
  let demoSource = BRE.DS_1YRACSPUMS
  hmd <- K.ignoreCacheTime houseData_C
  K.logLE K.Info "(predictors.html): Predictor & Predicted: Distributions & Correlations"
  K.newPandoc (K.PandocInfo "examine_predictors" $ one ("pagetitle","Examine Predictors")) $ do
    BR.brAddMarkDown "## Predictors/Predicted: Distributions in house elections 2012-2018"
    let vcDist = FV.ViewConfig 200 200 5
        mhStyle = FV.FacetedBar
        votes r = F.rgetField @BRE.DVotes r + F.rgetField @BRE.RVotes r
        turnout r = realToFrac (votes r) / realToFrac (F.rgetField @PUMS.Citizens r)
        dShare r = if votes r > 0 then realToFrac (F.rgetField @BRE.DVotes r) / realToFrac (votes r) else 0
    _ <- K.addHvega Nothing Nothing
         $ FV.multiHistogram @BRE.FracUnder45 @BR.Year "% Under 45" Nothing 50 FV.DataMinMax True mhStyle vcDist (hmd ^. #houseElectionData)
    _ <- K.addHvega Nothing Nothing
         $ FV.multiHistogram @BRE.FracFemale @BR.Year "% Female" Nothing 50 FV.DataMinMax True mhStyle vcDist (hmd ^. #houseElectionData)
    _ <- K.addHvega Nothing Nothing
         $ FV.multiHistogram @BRE.FracGrad @BR.Year "% Grad" Nothing 50 FV.DataMinMax True  mhStyle vcDist (hmd ^. #houseElectionData)
--    _ <- K.addHvega Nothing Nothing
--         $ FV.multiHistogram @BRE.FracNonWhite @BR.Year "% Non-White" Nothing 50 FV.DataMinMax True mhStyle vcDist (hmd ^. #houseElectionData)
    _ <- K.addHvega Nothing Nothing
         $ VL.multiHistogram
         "% Non-White"
         Nothing Nothing
         (Just "Year")
         (\r -> 1 - BRE.pumsWhiteNH r)
         (realToFrac . F.rgetField @BR.Year)
         GV.Number
         50
         FV.DataMinMax
         True
         mhStyle
         vcDist
         (hmd ^. #houseElectionData)
    _ <- K.addHvega Nothing Nothing
         $ VL.multiHistogram
         "% Hispanic"
         Nothing Nothing
         (Just "Year")
         (BRE.pumsHispanic)
         (realToFrac . F.rgetField @BR.Year)
         GV.Number
         50
         FV.DataMinMax
         True
         mhStyle
         vcDist
         (hmd ^. #houseElectionData)
    _ <- K.addHvega Nothing Nothing
         $ VL.multiHistogram
         "% Hispanic Identifying as White"
         Nothing Nothing
         (Just "Year")
         (BRE.pumsHispanicWhiteFraction)
         (realToFrac . F.rgetField @BR.Year)
         GV.Number
         50
         FV.DataMinMax
         True
         mhStyle
         vcDist
         (hmd ^. #houseElectionData)
    _ <- K.addHvega Nothing Nothing
         $ FV.multiHistogram @BRE.FracBlack @BR.Year "% Black" Nothing 50 FV.DataMinMax True mhStyle vcDist (hmd ^. #houseElectionData)
    _ <- K.addHvega Nothing Nothing
         $ FV.multiHistogram @BRE.FracAsian @BR.Year "% Asian" Nothing 50 FV.DataMinMax True mhStyle vcDist (hmd ^. #houseElectionData)
    _ <- K.addHvega Nothing Nothing
         $ FV.multiHistogram @BRE.FracOther @BR.Year "% Other (Race)" Nothing 50 FV.DataMinMax True mhStyle vcDist (hmd ^. #houseElectionData)
    _ <- K.addHvega Nothing Nothing
         $ FV.multiHistogram @DT.AvgIncome @BR.Year "Average Income" Nothing 50 FV.DataMinMax True mhStyle vcDist (hmd ^. #houseElectionData)
    _ <- K.addHvega Nothing Nothing
         $ FV.multiHistogram @DT.PopPerSqMile @BR.Year "Density [log(ppl/sq mile)]" Nothing 50 FV.DataMinMax True mhStyle vcDist
         $ fmap (FT.fieldEndo @DT.PopPerSqMile (Numeric.logBase 10)) (hmd ^. #houseElectionData)
    _ <- K.addHvega Nothing Nothing
         $ FV.multiHistogram @PctTurnout @BR.Year "Turnout %" Nothing 50 FV.DataMinMax True mhStyle vcDist
         $ FT.mutate (FT.recordSingleton @PctTurnout . (*100) . turnout) <$> (hmd ^. #houseElectionData)
    _ <- K.addHvega Nothing Nothing
         $ FV.multiHistogram @DShare @BR.Year "Dem Share" Nothing 50 FV.DataMinMax True mhStyle vcDist
         $ FT.mutate (FT.recordSingleton @DShare . (*100). dShare) <$> (hmd ^. #houseElectionData)
    BR.brAddMarkDown "## Predictors/Predicted: Correlations in house elections 2012-2018"
    let corrSet = S.fromList [FV.LabeledCol "% Under 45" (F.rgetField @BRE.FracUnder45)
                             ,FV.LabeledCol "% Female" (F.rgetField @BRE.FracFemale)
                             ,FV.LabeledCol "% Grad" (F.rgetField @BRE.FracGrad)
                             ,FV.LabeledCol "% Hispanic" BRE.pumsHispanic
                             ,FV.LabeledCol "WhiteHispanic/Hispanic" BRE.pumsHispanicWhiteFraction
                             ,FV.LabeledCol "% Black" (F.rgetField @BRE.FracBlack)
                             ,FV.LabeledCol "% Asian" (F.rgetField @BRE.FracAsian)
                             ,FV.LabeledCol "Avg. Income" (F.rgetField @DT.AvgIncome)
                             ,FV.LabeledCol "Density" (F.rgetField @DT.PopPerSqMile)
                             ,FV.LabeledCol "Incumbency" (realToFrac . F.rgetField @BRE.Incumbency)
                             ,FV.LabeledCol "Turnout" turnout
                             ,FV.LabeledCol "D Share" dShare
                             ]
    corrChart <- K.knitEither
                 $ FV.frameCorrelations
                 "Correlations among predictors & predicted (election data only)"
                 (FV.ViewConfig 600 600 10)
                 False
                 corrSet
                 (hmd ^. #houseElectionData)
    _ <- K.addHvega Nothing Nothing corrChart
    K.logLE K.Info "run model(s)"

  K.newPandoc
    (K.PandocInfo "compare_predictors" $ one ("pagetitle","Compare Predictors"))
    $ comparePredictors False demoSource $ K.liftActionWithCacheTime houseData_C

  K.newPandoc
    (K.PandocInfo "compare_data_sets" $ one ("pagetitle","Compare Data Sets"))
    $ compareData False demoSource $ K.liftActionWithCacheTime houseData_C

  K.newPandoc
    (K.PandocInfo "compare_vote_totals" $ one ("pagetitle","Compare Vote Totals Across Races"))
    $ examineVoteTotals False demoSource $ K.liftActionWithCacheTime houseData_C

  K.newPandoc
    (K.PandocInfo "examine_turnout_gaps" $ one ("pagetitle","Turnout Gaps")) turnoutGaps


{-
  K.newPandoc
    (K.PandocInfo "examine_fit" $ one ("pagetitle","Examine Fit"))
    $ examineFit False demoSource $ K.liftActionWithCacheTime houseData_C
-}
{-
  K.newPandoc
    (K.PandocInfo "compare_models" $ one ("pagetitle","Compare Models"))
    $ compareModels True demoSource $ K.liftActionWithCacheTime houseData_C
-}

writeCompareScript :: K.KnitEffects r => [SC.ModelRunnerConfig] -> Text -> K.Sem r ()
writeCompareScript configs compareScriptName = do
  let modelDir = "/Users/adam/BlueRipple/research/stan/house/election"
  writeFileText (toString $ modelDir <> "/R/" <> compareScriptName <> ".R")
    $ SR.compareScript configs 10 Nothing

examineVoteTotals :: forall r. (K.KnitOne r, BR.CacheEffects r)
  => Bool -> BRE.DemographicSource -> K.ActionWithCacheTime r BRE.HouseModelData  -> K.Sem r ()
examineVoteTotals clearCached ds houseData_C = do
  houseData <- K.ignoreCacheTime houseData_C
  -- Aggregate house votes by state to compare to senate and prez
  let houseVoteByStateF = FMR.concatFold
                          $ FMR.mapReduceFold
                          FMR.noUnpack
                          (FMR.assignKeysAndData @[BR.Year, BR.StateAbbreviation] @[BRE.DVotes, BRE.RVotes, PUMS.Citizens])
                          (FMR.foldAndAddKey $ FF.foldAllConstrained @Num FL.sum)
      fHouseVotesByState = FL.fold houseVoteByStateF $ BRE.houseElectionData houseData
  let totalVotes r = F.rgetField @BRE.RVotes r + F.rgetField @BRE.DVotes r
      fracOfCit x r = realToFrac x / realToFrac (F.rgetField @PUMS.Citizens r)
      dFracOfCit r = fracOfCit (F.rgetField @BRE.DVotes r) r
      rFracOfCit r = fracOfCit (F.rgetField @BRE.RVotes r) r
      tFracOfCit r = fracOfCit (F.rgetField @BRE.TVotes r) r
      forChart x =  fmap
                    (F.rcast @[BR.Year, BR.StateAbbreviation, ET.Office, RFracOfCit, DFracOfCit, TFracOfCit]
                     . (FT.mutate (const $ FT.recordSingleton @ET.Office x))
                     . (FT.mutate (FT.recordSingleton @DFracOfCit . dFracOfCit))
                     . (FT.mutate (FT.recordSingleton @RFracOfCit . rFracOfCit))
                     . (FT.mutate (FT.recordSingleton @TFracOfCit . tFracOfCit))
                     . (FT.mutate (FT.recordSingleton @BRE.TVotes . totalVotes))
                    )
      fForChartH = forChart ET.House fHouseVotesByState
      fForChartS = forChart ET.Senate $ BRE.senateElectionData houseData
      fForChartP = forChart ET.President $ BRE.presidentialElectionData houseData

--  BR.logFrame $ F.melt (Proxy :: Proxy '[BR.Year, BR.StateAbbreviation, ET.Office]) $ F.filterFrame ((==2018) . F.rgetField @BR.Year) fForChart
  _ <- K.addHvega Nothing Nothing
       $ voteTotalChart ("Turnout By Office (" <> BRE.demographicSourceLabel ds <> ")") (FV.ViewConfig  200 30 2)
       $ (fForChartH <> fForChartS <> fForChartP)
  return ()

type LeanDem = "LeanDemVoters" F.:-> Double
type LeanRep = "LeanRepVoters" F.:-> Double
type LDReg = "LDRegistered" F.:-> Double
type LRReg = "LRRegistered" F.:-> Double
type LDVoted = "LDVoted" F.:-> Double
type LRVoted = "LRVoted" F.:-> Double
type Gap = "Gap" F.:-> Double
type BreakEven = "BreakEven" F.:-> Double

type TurnoutGapR = [LeanDem, LDReg, LDVoted, LeanRep, LRReg, LRVoted, Gap, BreakEven]

fTurnoutGap :: FL.Fold (F.Record [CCES.CCESWeight, CCES.PartisanId3, CCES.Turnout, CCES.Registration]) (F.Record TurnoutGapR)
fTurnoutGap =
  let leanDem r = F.rgetField @CCES.PartisanId3 r == CCES.PI3_Democrat
      leanRep r = F.rgetField @CCES.PartisanId3 r == CCES.PI3_Republican
      reg r = F.rgetField @CCES.Registration r == CCES.R_Active
      voted r = F.rgetField @CCES.Turnout r == CCES.T_Voted
      wgt r = F.rgetField @CCES.CCESWeight r
      fLD = FL.prefilter leanDem $ FL.premap wgt $ FL.sum
      fLDR = FL.prefilter (\r -> leanDem r && reg r) $ FL.premap wgt $ FL.sum
      fLDV = FL.prefilter (\r -> leanDem r && voted r) $ FL.premap wgt $ FL.sum
      fLR = FL.prefilter leanRep $ FL.premap wgt $ FL.sum
      fLRR = FL.prefilter (\r -> leanRep r && reg r) $ FL.premap wgt $ FL.sum
      fLRV = FL.prefilter (\r -> leanRep r && voted r) $ FL.premap wgt $ FL.sum
      gap ld ldv lr lrv = (realToFrac lr/realToFrac lrv) - (realToFrac ld/realToFrac ldv)
      breakEven ld ldv lr lrv = (realToFrac lrv / realToFrac lr) * (realToFrac ld/realToFrac ldv) - 1.0
  in (\ld ldr ldv lr lrr lrv
       -> ld F.&: ldr F.&: ldv F.&: lr F.&: lrr F.&: lrv F.&: gap ld ldv lr lrv  F.&: breakEven ld ldv lr lrv F.&: V.RNil)
     <$> fLD <*> fLDR <*> fLDV <*> fLR <*> fLRR <*> fLRV

turnoutGaps :: forall r. (K.KnitOne r, BR.CacheEffects r) => K.Sem r ()
turnoutGaps = do
  fCCES <- K.ignoreCacheTimeM CCES.ccesDataLoader
  fTurnoutGapByStateYear <- BRF.frameCompactMRM
                            FMR.noUnpack
                            (FMR.assignKeysAndData @[BR.Year,BR.StateAbbreviation])
                            fTurnoutGap
                            $ F.filterFrame ((== 2018) . F.rgetField @BR.Year) fCCES
  BR.logFrame fTurnoutGapByStateYear





type DFracOfCit = "DemFracOfCit" F.:-> Double
type RFracOfCit = "RepFracOfCit" F.:-> Double
type TFracOfCit = "TotFracOfCit" F.:-> Double

type VoteTotalData = [RFracOfCit, DFracOfCit, TFracOfCit]
type DataCol = "value" F.:-> F.CoRec V.ElField VoteTotalData
type VoteTotalR = [BR.Year, BR.StateAbbreviation, ET.Office] V.++ VoteTotalData

voteTotalChart :: (Functor f, Foldable f)
               => Text
               -> FV.ViewConfig
               -> f (F.Record VoteTotalR)
               -> GV.VegaLite
voteTotalChart title vc rows =
  let toVLDataRec = FV.asVLNumber "Year"
                    V.:& FV.textAsVLStr "State"
                    V.:& FV.asVLStrViaShow "Office"
                    V.:& FV.asVLNumber "Rep"
                    V.:& FV.asVLNumber "Dem"
                    V.:& FV.asVLNumber "Total"
                    V.:& V.RNil
      dat = FV.recordsToDataWithParse [{-("Year", GV.FoDate "%Y")-}] toVLDataRec rows
--      filterParty = GV.filter (GV.FEqual "Party" (GV.Str "Total"))
      filterYear = GV.filter (GV.FEqual "Year" (GV.Number 2012))
--      imputeVotes = GV.impute "Votes" "Office" [GV.ImMethod GV.ImValue, GV.ImNewValue $ GV.Number 0]
      foldVotes = GV.foldAs ["Dem", "Rep"] "Party" "Votes"
      transform = GV.transform . foldVotes
      encState = GV.row [GV.FName "State", GV.FmType GV.Nominal]
      encYear = GV.column [GV.FName "Year", GV.FmType GV.Nominal]
      encParty = GV.color [GV.MName "Party", GV.MmType GV.Nominal]
      encOffice = GV.position GV.Y [GV.PName "Office"
                                  , GV.PmType GV.Nominal
                                  , GV.PAxis [GV.AxNoTitle]
                                  , GV.PSort [GV.CustomSort $ GV.Strings ["President", "Senate", "House"]]
                                  ]
      encVotes = GV.position GV.X [GV.PName "Votes"
                                  , GV.PmType GV.Quantitative
                                  , GV.PAxis [GV.AxTitle "Total Votes/Citizen Population"]
                                  ]
      encoding = GV.encoding . encOffice . encVotes . encState . encYear . encParty
      mark = GV.mark GV.Bar []
--      spec = GV.specification $ GV.asSpec [encoding [], mark]
  in FV.configuredVegaLite vc [FV.title title, transform [],  encoding [], mark, dat]




compareModels :: forall r. (K.KnitOne r, BR.CacheEffects r)
  => Bool -> BRE.DemographicSource -> K.ActionWithCacheTime r BRE.HouseModelData -> K.Sem r ()
compareModels clearCached ds houseData_C =  K.wrapPrefix "compareModels" $ do
  let predictors = ["Incumbency","PopPerSqMile","PctNonWhite", "PctGrad"]
      modeledDataSets = S.fromList [BRE.HouseE]
      models =
        [ ("betaBinomialInc", Just $ BRE.demographicSourceLabel ds, modeledDataSets, BRE.HouseE, BRE.betaBinomialInc, 500)
        , ("binomial", Just $ BRE.demographicSourceLabel ds, modeledDataSets, BRE.HouseE, BRE.binomial, 500)
        ]
      isYear x = (== x) . F.rgetField @BR.Year
      year = 2018
      runOne x =
        BRE.runHouseModel
        clearCached
        predictors
        x
        year
        (fmap (Optics.over #houseElectionData (F.filterFrame (isYear year))
                . Optics.over #senateElectionData (F.filterFrame (isYear year))
                . Optics.over #presidentialElectionData (F.filterFrame (isYear year))
                . Optics.over #ccesData (F.filterFrame (isYear year)))
          houseData_C
        )
  results <- traverse runOne models
  looDeps <- K.knitMaybe "No results (compareModels)"
             $ fmap Semigroup.sconcat $ nonEmpty $ (fmap (const ()) . fst <$> results) -- unit dependency on newest of results
  fLoo_C <- BR.retrieveOrMakeFrame "model/house/modelsLoo.bin" looDeps $ const $ do
    K.logLE K.Info "model run(s) are newer than loo comparison data.  Re-running comparisons."
    writeCompareScript (snd <$> results) ("compareModels_" <> show year)
    K.liftKnit $ SR.compareModels (zip ((\(n,_,_,_,_,_) -> n) <$> models) (snd <$> results)) 10
  fLoo <- K.ignoreCacheTime fLoo_C
  BR.brAddRawHtmlTable
    "Predictor LOO (Leave-One-Out Cross Validation) comparison"
    mempty
    (BR.toCell mempty () "" BR.textToStyledHtml <$> SR.looTextColonnade 2)
    (reverse $ sortOn (F.rgetField @SR.ELPD_Diff) $ FL.fold FL.list fLoo)
  return ()

comparePredictors :: forall r. (K.KnitOne r, BR.CacheEffects r) => Bool -> BRE.DemographicSource -> K.ActionWithCacheTime r BRE.HouseModelData  -> K.Sem r ()
comparePredictors clearCached ds houseData_C = K.wrapPrefix "comparePredictors" $ do
  let predictors = [("IDRE",["Incumbency", "PopPerSqMile", "PctNonWhite", "PctGrad"])
                   ,("IDEBHFAO",["Incumbency", "PopPerSqMile", "PctGrad", "PctBlack", "PctHispanic", "HispanicWhiteFraction", "PctAsian", "PctOther"])
                   ,("IDEwBHFAO",["Incumbency", "PopPerSqMile", "PctWhiteGrad", "PctBlack", "PctHispanic", "HispanicWhiteFraction", "PctAsian", "PctOther"])
                   ,("IDEBHAO",["Incumbency", "PopPerSqMile", "PctGrad", "PctBlack", "PctHispanic", "PctAsian", "PctOther"])
                   ,("IRE", ["Incumbency", "PctNonWhite", "PctGrad"])
                   ,("IDR", ["Incumbency", "PopPerSqMile", "PctNonWhite"])
                   ,("IDE", ["Incumbency", "PopPerSqMile", "PctGrad"])
                   ,("IBHF", ["Incumbency", "PctBlack", "PctHispanic", "HispanicWhiteFraction"])
                   ,("IBH", ["Incumbency", "PctBlack", "PctHispanic"])
                   ,("PctBlack", ["PctBlack"])
                   ,("PctNonWhite", ["PctNonWhite"])
                   ,("PctNonWhiteNH", ["PctNonWhiteNH"])
                   ,("Incumbency", ["Incumbency"])
                   ,("Density", ["PopPerSqMile"])
                   ,("EducationW", ["PctWhiteGrad"])
                   ,("Education", ["PctGrad"])
                   ,("Income", ["AvgIncome"])
                   ,("IntOnly", [])
                   ]
      isYear y = (== y) . F.rgetField @BR.Year
      extraLabel x = BRE.demographicSourceLabel ds <> "_" <> fst x
      year = 2018
      runOne x =
        BRE.runHouseModel
        clearCached
        (snd x)
        ("betaBinomialInc", Just $ extraLabel x, S.fromList [BRE.HouseE, BRE.SenateE], BRE.HouseE, BRE.betaBinomialInc, 500)
        year
        (fmap (Optics.over #houseElectionData (F.filterFrame (isYear year))
                . Optics.over #senateElectionData (F.filterFrame (isYear year))
                . Optics.over #presidentialElectionData (F.filterFrame (isYear year))
                . Optics.over #ccesData (F.filterFrame (isYear year)))
          houseData_C
        )
  results <- traverse runOne predictors
  looDeps <- K.knitMaybe "No results (comparePredictors)"
             $ fmap Semigroup.sconcat $ nonEmpty $ (fmap (const ()) . fst <$> results) -- unit dependency on newest of results
  fLoo_C <- BR.retrieveOrMakeFrame "model/house/predictorsLoo.bin" looDeps $ const $ do
    K.logLE K.Info "model run(s) are newer than loo comparison data.  Re-running comparisons."
    writeCompareScript (snd <$> results) ("comparePredictors_" <> show year)
    K.liftKnit $ SR.compareModels (zip (fst <$> predictors) (snd <$> results)) 10
  fLoo <- K.ignoreCacheTime fLoo_C
  BR.brAddRawHtmlTable
    "Predictor LOO (Leave-One-Out Cross Validation) comparison"
    mempty
    (BR.toCell mempty () "" BR.textToStyledHtml <$> SR.looTextColonnade 2)
    (reverse $ sortOn (F.rgetField @SR.ELPD_Diff) $ FL.fold FL.list fLoo)
  return ()


compareData :: forall r. (K.KnitOne r, BR.CacheEffects r) => Bool -> BRE.DemographicSource -> K.ActionWithCacheTime r BRE.HouseModelData -> K.Sem r ()
compareData clearCached ds houseData_C =  K.wrapPrefix "compareData" $ do
  let --predictors = ["Incumbency", "PopPerSqMile", "PctGrad", "PctNonWhite"]
      predictors = ["Incumbency", "PopPerSqMile", "PctWhiteGrad", "PctBlack", "PctHispanic", "HispanicWhiteFraction", "PctAsian", "PctOther"]
      presPredictors = List.filter (/= "Incumbency") predictors
      years = [2012, 2014, 2016, 2018]
      presYears = [2012, 2016]
      modelWiths = [(S.fromList [BRE.HouseE], BRE.HouseE)
                   , (S.fromList [BRE.SenateE], BRE.SenateE)
                   , (S.fromList [BRE.PresidentialE], BRE.PresidentialE)
                   , (S.fromList [BRE.HouseE, BRE.PresidentialE], BRE.HouseE)
                   ]
      modelWithLabels = fmap (BRE.modeledDataSetsLabel . fst) modelWiths
      runYear (mds, cds) y =
        BRE.runHouseModel
        clearCached
        (if mds == S.fromList [BRE.PresidentialE] then presPredictors else predictors)
        ("betaBinomialInc", Just $ BRE.demographicSourceLabel ds, mds, cds, BRE.betaBinomialInc, 500)
        y
        (fmap (Optics.over #houseElectionData (F.filterFrame (isYear y))
                . Optics.over #senateElectionData (F.filterFrame (isYear y))
                . Optics.over #presidentialElectionData (F.filterFrame (isYear y))
                . Optics.over #ccesData (F.filterFrame (isYear y)))
          houseData_C
        )
      isYear year = (== year) . F.rgetField @BR.Year
      nameType l =
        let (name, t) = T.splitAt (T.length l - 1) l
        in case t of
             "D" -> Right (name, "D Pref")
             "V" -> Right (name, "Turnout")
             _ -> Left $ "Bad last character in delta label (" <> l <> ")."
      expandInterval :: (T.Text, [Double]) -> Either T.Text (MapRow.MapRow GV.DataValue) --T.Text (T.Text, [(T.Text, Double)])
      expandInterval (l, vals) = do
        (name, t) <- nameType l
        case vals of
          [lo, mid, hi] -> Right $ M.fromList [("Name", GV.Str name), ("Type", GV.Str t), ("lo", GV.Number $ 100 * (lo - mid)), ("mid", GV.Number $ 100 * mid), ("hi", GV.Number $ 100 * (hi - mid))]
          _ -> Left "Wrong length list in what should be a (lo, mid, hi) interval"
      expandMapRow f (y, modelResults)
        = fmap (M.insert "Year" (GV.Str $ show y)) <$> traverse expandInterval (M.toList $ f modelResults)
      modelMR mw = one ("Model", GV.Str $ BRE.modeledDataSetsLabel $ fst mw)
      modelAndDeltaMR mw d = modelMR mw <> one ("Delta",GV.Str d)
      runModelWith mw = do
        let yrs = if BRE.PresidentialE `elem` S.toList (fst mw) then presYears else years
        results_C <- sequenceA <$> traverse (fmap fst . runYear mw) yrs
        results <- zip yrs <$> K.ignoreCacheTime results_C
        sigmaDeltaMapRows <- fmap (<> modelAndDeltaMR mw "Std. Dev") <<$>> K.knitEither (traverse (expandMapRow BRE.sigmaDeltas) results)
        unitDeltaMapRows <- fmap (<> modelAndDeltaMR mw "Min/Max") <<$>> K.knitEither (traverse (expandMapRow BRE.unitDeltas) results)
        avgProbMapRows <- fmap (<> modelAndDeltaMR mw "Avg") <<$>> K.knitEither (traverse (expandMapRow BRE.avgProbs) results)
        return $ concat $ sigmaDeltaMapRows <> unitDeltaMapRows <> avgProbMapRows
  results <- mconcat <$> traverse runModelWith modelWiths
  let dataValueAsText :: GV.DataValue -> Text
      dataValueAsText (GV.Str x) = x
      dataValueAsText _ = error "Non-string given to dataValueAsString"
  _ <- K.addHvega Nothing Nothing
       $ modelChart
       "Average Probability"
       ["probV", "probD"]
       modelWithLabels
       (FV.ViewConfig 200 200 5)
       ""
       $ filter (maybe False ((== "Avg") . dataValueAsText) . M.lookup "Delta") results
  -- Do one just for incumbency
  _ <- K.addHvega Nothing Nothing
       $ modelChart
       "Change in Probability for Incumbency (with 90% confidence bands)"
       ["incumbency"]
       modelWithLabels
       (FV.ViewConfig 200 200 5)
       "D Pref"
       $ filter (maybe False ((== "Min/Max") . dataValueAsText) . M.lookup "Delta")
       $ filter (maybe False ((== "Incumbency") . dataValueAsText) . M.lookup "Name") results
  _ <- K.addHvega Nothing Nothing
       $ modelChart
       "Change in Probability for full range of predictor (with 90% confidence bands)"
       (List.filter (/= "Incumbency") predictors)
       modelWithLabels
       (FV.ViewConfig 200 200 5)
       "D Pref"
       $ filter (maybe False ((== "Min/Max") . dataValueAsText) . M.lookup "Delta")
       $ filter (maybe False ((/= "Incumbency") . dataValueAsText) . M.lookup "Name") results
  _ <- K.addHvega Nothing Nothing
       $ modelChart
       "Change in Probability for 1 std dev change in predictor (1/2 below avg to 1/2 above) (with 90% confidence bands)"
       predictors
       modelWithLabels
       (FV.ViewConfig 200 200 5)
       "D Pref"
       $ filter (maybe False ((== "Std. Dev") . dataValueAsText) . M.lookup "Delta")
       $ filter (maybe False ((/= "Incumbency") . dataValueAsText) . M.lookup "Name") results
--    K.logLE K.Info $ show results
  return ()

modelChart :: (Functor f, Foldable f) => T.Text -> [Text] -> [Text] -> FV.ViewConfig -> T.Text -> f (MapRow.MapRow GV.DataValue) -> GV.VegaLite
modelChart title predOrder modelOrder vc t rows =
  let vlData = MapRow.toVLData M.toList [GV.Parse [("Year", GV.FoDate "%Y")]] rows
      encX = GV.position GV.X [GV.PName "Year", GV.PmType GV.Temporal]
      encY = GV.position GV.Y [GV.PName "mid", GV.PmType GV.Quantitative, axis{-, scale-}]
      axis = GV.PAxis [GV.AxNoTitle  {-, GV.AxValues (GV.Numbers [-15, -10, -5, 0, 5, 10, 15])-}]
      scale = GV.PScale [GV.SDomain $ GV.DNumbers [-12, 12]]
      encYLo = GV.position GV.YError [GV.PName "lo", GV.PmType GV.Quantitative]
      encYHi = GV.position GV.YError2 [GV.PName "hi", GV.PmType GV.Quantitative]
      encColor = GV.color [GV.MName "Type", GV.MmType GV.Nominal]
      encL = GV.encoding . encX . encY . encColor
      encB = GV.encoding . encX . encYLo . encY . encYHi . encColor
      markBand = GV.mark GV.ErrorBand [GV.MTooltip GV.TTData]
      markLine = GV.mark GV.Line []
      markPoint = GV.mark GV.Point [GV.MTooltip GV.TTData]
      specBand = GV.asSpec [encB [], markBand]
      specLine = GV.asSpec [encL [], markLine]
      specPoint = GV.asSpec [encL [], markPoint]
      spec = GV.asSpec [GV.layer [specBand, specLine, specPoint]]
      facet = GV.facet [GV.ColumnBy [GV.FName "Model"
                                    , GV.FmType GV.Nominal
                                    , GV.FSort [GV.CustomSort $ GV.Strings modelOrder]
                                    ]
                       ,GV.RowBy [GV.FName "Name"
                                 , GV.FmType GV.Nominal
                                 , GV.FSort [GV.CustomSort $ GV.Strings predOrder]
                                 ]
                       ]
   in FV.configuredVegaLite vc [FV.title title, facet, GV.specification spec, vlData]

examineFit :: forall r. (K.KnitOne r, BR.CacheEffects r) => Bool -> BRE.DemographicSource -> K.ActionWithCacheTime r BRE.HouseModelData -> K.Sem r ()
examineFit clearCached ds houseData_C =  K.wrapPrefix "examineFit" $ do
  let predictors = ["Incumbency","PopPerSqMile","PctGrad", "PctNonWhite"]
      model = ("betaBinomialInc", Just $ BRE.demographicSourceLabel ds , S.fromList [BRE.HouseE], BRE.HouseE, BRE.betaBinomialInc, 500)
      isYear year = (== year) . F.rgetField @BR.Year
      year = 2018
      runOne x =
        BRE.runHouseModel
        clearCached
        predictors
        x
        year
        (fmap (Optics.over #houseElectionData (F.filterFrame (isYear year))
                . Optics.over #senateElectionData (F.filterFrame (isYear year))
                . Optics.over #presidentialElectionData (F.filterFrame (isYear year))
                . Optics.over #ccesData (F.filterFrame (isYear year)))
          houseData_C
        )
  results <- runOne model
  electionData <- K.ignoreCacheTime
                  $ fmap (F.rcast @[BR.StateAbbreviation, BR.CongressionalDistrict, BRE.FracGrad, BRE.FracWhiteNonHispanic])
                  . F.filterFrame (isYear year)
                  . BRE.houseElectionData
                  <$> houseData_C
  electionFit <- K.ignoreCacheTime $ fmap BRE.houseElectionFit . fst $ results
  let (fitWithDemo, missing) =  FJ.leftJoinWithMissing @[BR.StateAbbreviation, BR.CongressionalDistrict] electionFit electionData
  unless (null missing) $ K.knitError "Missing keys in electionFit/electionData join"
  _ <- K.addHvega Nothing Nothing
       $ fitScatter1
       "Test"
       (FV.ViewConfig 800 800 10)
       $ fmap F.rcast fitWithDemo
  _ <- K.addHvega Nothing Nothing
       $ fitScatter2
       "Test"
       (FV.ViewConfig 800 800 10)
       $ fmap F.rcast fitWithDemo
--  BR.logFrame fitWithDemo
  return ()

fitScatter1 :: (Functor f, Foldable f)
           => Text
           -> FV.ViewConfig
           -> f (F.Record ([BR.StateAbbreviation
                           , BR.CongressionalDistrict
                           , BRE.TVotes
                           , BRE.DVotes
                           , BRE.EDVotes5
                           , BRE.EDVotes
                           , BRE.EDVotes95
                           , BRE.FracGrad
                           , BRE.FracWhiteNonHispanic]))
           -> GV.VegaLite
fitScatter1 title vc rows =
  let toVLDataRec = FV.useColName FV.textAsVLStr
                    V.:& FV.asVLStrViaShow "District"
                    V.:& FV.asVLNumber "Votes"
                    V.:& FV.useColName FV.asVLNumber
                    V.:& FV.useColName FV.asVLNumber
                    V.:& FV.useColName FV.asVLNumber
                    V.:& FV.useColName FV.asVLNumber
                    V.:& FV.asVLData (GV.Number . (*100)) "% Grad"
                    V.:& FV.asVLData (\x -> GV.Number $ 100 * (1 - x)) "% Non-White"
                    V.:& V.RNil
      dat = FV.recordsToData toVLDataRec rows
      encX = GV.position GV.X [GV.PName "% Grad", GV.PmType GV.Quantitative]
      encY = GV.position GV.Y [GV.PName "% Non-White", GV.PmType GV.Quantitative]
      calcFitDiff = GV.calculateAs ("(datum.DVotes - datum.EstDVotes)/datum.Votes") "actual - fit (D Share)"
      calcShare = GV.calculateAs ("datum.DVotes/datum.Votes - 0.5") "D Share"
      transform = GV.transform . calcFitDiff . calcShare
      encColor = GV.color [GV.MName "D Share", GV.MmType GV.Quantitative]
      encSize = GV.size [GV.MName "actual - fit (D Share)", GV.MmType GV.Quantitative]
      enc = GV.encoding . encX . encY . encColor . encSize
      mark = GV.mark GV.Circle [GV.MTooltip GV.TTData]
  in FV.configuredVegaLite vc [FV.title title, transform [], enc [], mark, dat]

fitScatter2 :: (Functor f, Foldable f)
           => Text
           -> FV.ViewConfig
           -> f (F.Record ([BR.StateAbbreviation
                           , BR.CongressionalDistrict
                           , BRE.TVotes
                           , BRE.DVotes
                           , BRE.EDVotes5
                           , BRE.EDVotes
                           , BRE.EDVotes95
                           , BRE.FracGrad
                           , BRE.FracWhiteNonHispanic]))
           -> GV.VegaLite
fitScatter2 title vc rows =
  let toVLDataRec = FV.useColName FV.textAsVLStr
                    V.:& FV.asVLStrViaShow "District"
                    V.:& FV.asVLNumber "Votes"
                    V.:& FV.useColName FV.asVLNumber
                    V.:& FV.useColName FV.asVLNumber
                    V.:& FV.useColName FV.asVLNumber
                    V.:& FV.useColName FV.asVLNumber
                    V.:& FV.asVLData (GV.Number . (*100)) "% Grad"
                    V.:& FV.asVLData (\x -> GV.Number $ 100 * (1 - x)) "% Non-White"
                    V.:& V.RNil
      dat = FV.recordsToData toVLDataRec rows
      calcFitDiff = GV.calculateAs ("(datum.DVotes - datum.EstDVotes)/datum.Votes") "actual - fit (D Share)"
      calcShare = GV.calculateAs ("datum.DVotes/datum.Votes - 0.5") "D Share"
      transform = GV.transform . calcFitDiff . calcShare
      encX = GV.position GV.X [GV.PName "D Share", GV.PmType GV.Quantitative]
      encY = GV.position GV.Y [GV.PName "actual - fit (D Share)", GV.PmType GV.Quantitative]
      encColor = GV.color [GV.MName "% Grad", GV.MmType GV.Quantitative]
      encSize = GV.size [GV.MName "% Non-White", GV.MmType GV.Quantitative]
      enc = GV.encoding . encX . encY . encColor . encSize
      mark = GV.mark GV.Circle [GV.MTooltip GV.TTData]
  in FV.configuredVegaLite vc [FV.title title, transform [], enc [], mark, dat]

testCCESPref :: forall r. (K.KnitMany r, BR.CacheEffects r) => K.Sem r ()
testCCESPref =   K.newPandoc (K.PandocInfo "test CCES MRP" $ one ("pagetitle","Test CCES MRP")) $ do
  K.logLE K.Info "Stan model fit for 2016 presidential votes:"
{-
  stan_allBuckets <-
    K.ignoreCacheTimeM $
      BRS.prefASER5_MR
        ("v1", BRS.ccesDataWrangler)
        ("binomial_allBuckets", BRS.model_BinomialAllBuckets)
        ET.President
        2016
-}
  stan_sepFixedWithStates <-
    K.ignoreCacheTimeM $
      BRS.prefASER5_MR
        ("v2", BRS.ccesDataWrangler2)
        ("binomial_sepFixedWithStates", BRS.model_v5)
        ET.President
        2016

  stan_sepFixedWithStates3 <-
    K.ignoreCacheTimeM $
      BRS.prefASER5_MR
        ("v2", BRS.ccesDataWrangler2)
        ("binomial_sepFixedWithStates3", BRS.model_v7)
        ET.President
        2016

  K.logLE K.Info $ "sepFixedWithStates vs sepFixedWithStates3"
  let compList = zip (FL.fold FL.list stan_sepFixedWithStates) $ fmap (F.rgetField @ET.DemPref) $ FL.fold FL.list stan_sepFixedWithStates3
  K.logLE K.Info $ T.intercalate "\n" . fmap (T.pack . show) $ compList

--  BRS.prefASER5_MR_Loo ("v1", BRS.ccesDataWrangler) ("binomial_allBuckets", BRS.model_BinomialAllBuckets) ET.President 2016
--  BRS.prefASER5_MR_Loo ("v1", BRS.ccesDataWrangler) ("binomial_bucketFixedStateIntcpt", BRS.model_v2) ET.President 2016
--  BRS.prefASER5_MR_Loo ("v1", BRS.ccesDataWrangler) ("binomial_bucketFixedOnly", BRS.model_v3) ET.President 2016
  BRS.prefASER5_MR_Loo ("v2", BRS.ccesDataWrangler2) ("binomial_sepFixedOnly", BRS.model_v4) ET.President 2016
  BRS.prefASER5_MR_Loo ("v2", BRS.ccesDataWrangler2) ("binomial_sepFixedWithStates", BRS.model_v5) ET.President 2016
  BRS.prefASER5_MR_Loo ("v2", BRS.ccesDataWrangler2) ("binomial_sepFixedWithStates2", BRS.model_v6) ET.President 2016
  BRS.prefASER5_MR_Loo ("v2", BRS.ccesDataWrangler2) ("binomial_sepFixedWithStates3", BRS.model_v7) ET.President 2016

--  BR.logFrame stan
{-
  K.logLE K.Info "glm-haskell model fit for 2016 presidential votes:"
  let g r = (F.rgetField @BR.Year r == 2016) && (F.rgetField @ET.Office r == ET.President)
  glmHaskell <- F.filterFrame g <$> (K.ignoreCacheTimeM $ BRC.ccesPreferencesASER5_MRP)
  BR.logFrame glmHaskell
-}

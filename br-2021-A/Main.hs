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
import Data.String.Here (here, i)
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

import qualified Stan.ModelConfig as SC
import qualified Stan.ModelBuilder as SB
import qualified Stan.ModelBuilder.BuildingBlocks as SB
import qualified Stan.ModelBuilder.SumToZero as SB
import qualified Stan.Parameters as SP
import qualified CmdStan as CS
import qualified Stan.RScriptBuilder as SR
--import qualified Text.Pandoc.Class as Pandoc
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

{-
cpsStateRaceIntroRST :: Text
cpsStateRaceIntroRST = [here|
.. include:: br-2021-A/RST/cpsStateRace/intro.RST
|]
-}

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
  resE <- K.knitHtmls knitConfig cpsVAnalysis

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

densityPredictor r = Vector.fromList $ [Numeric.log (F.rgetField @DT.PopPerSqMile r)]

--acsWhite :: F.Record BRE.PUMSByCDR -> Bool
ra4White = (== DT.RA4_White) . F.rgetField @DT.RaceAlone4C

wnh r = (F.rgetField @DT.RaceAlone4C r == DT.RA4_White) && (F.rgetField @DT.HispC r == DT.NonHispanic)

ra4WhiteNonGrad r = ra4White r && (F.rgetField @DT.CollegeGradC r == DT.NonGrad)
wnhNonGrad r = wnh r && (F.rgetField @DT.CollegeGradC r == DT.NonGrad)

ccesGroupBuilder :: SB.StanGroupBuilderM (F.Record BRE.CCESByCDR) ()
ccesGroupBuilder = do
  SB.addGroup "CD" $ SB.makeIndexByCounting show districtKey
  SB.addGroup "State" $ SB.makeIndexByCounting show $ F.rgetField @BR.StateAbbreviation
  SB.addGroup "Race" $ SB.makeIndexFromEnum (F.rgetField @DT.Race5C)
  SB.addGroup "Sex" $ SB.makeIndexFromEnum (F.rgetField @DT.SexC)
  SB.addGroup "Education" $ SB.makeIndexFromEnum (F.rgetField @DT.CollegeGradC)
  SB.addGroup "Age" $ SB.makeIndexFromEnum (F.rgetField @DT.SimpleAgeC)

race5FromPUMS :: F.Record BRE.PUMSByCDR -> DT.Race5
race5FromPUMS r =
  let race4A = F.rgetField @DT.RaceAlone4C r
      hisp = F.rgetField @DT.HispC r
  in DT.race5FromRaceAlone4AndHisp True race4A hisp

race5FromCPS :: F.Record BRE.CPSVByCDR -> DT.Race5
race5FromCPS r =
  let race4A = F.rgetField @DT.RaceAlone4C r
      hisp = F.rgetField @DT.HispC r
  in DT.race5FromRaceAlone4AndHisp True race4A hisp


catsPSGroupRowMap :: SB.GroupRowMap (F.Record BRE.AllCatR)
catsPSGroupRowMap = SB.addRowMap @Text "State" (const "NY-21")
                    $ SB.addRowMap "Sex" (F.rgetField @DT.SexC)
                    $ SB.addRowMap "Education" (F.rgetField @DT.CollegeGradC) SB.emptyGroupRowMap

indexStanResults :: (Show k, Ord k) => IM.IntMap k -> Vector.Vector a -> Either Text (Map k a)
indexStanResults im v = do
  when (IM.size im /= Vector.length v)
    $ Left $
    "Mismatched sizes in indexStanResults. Result vector has " <> show (Vector.length v) <> " result and IntMap = " <> show im
  return $ M.fromList $ zip (IM.elems im) (Vector.toList v)

expandInterval :: Text -> (T.Text, [Double]) -> Either T.Text (MapRow.MapRow GV.DataValue)
expandInterval label (t, vals)  = do
        case vals of
          [lo, mid, hi] -> Right $ M.fromList [(label, GV.Str t), ("lo", GV.Number $ (lo - mid)), ("mid", GV.Number $ mid), ("hi", GV.Number $ (hi - mid))]
          _ -> Left "Wrong length list in what should be a (lo, mid, hi) interval"

subSampleCCES :: K.KnitEffects r => Word32 -> Int -> BRE.CCESAndPUMS -> K.Sem r BRE.CCESAndPUMS
subSampleCCES seed samples (BRE.CCESAndPUMS cces cps pums dist cats) = do
  subSampledCCES <- K.liftKnit @IO $ BR.sampleFrame seed samples cces
  return $ BRE.CCESAndPUMS subSampledCCES cps pums dist cats

countInCategory :: (Eq a, Num b) => (F.Record rs -> b) -> (F.Record rs -> a) -> [a] -> FL.Fold (F.Record rs) [(a, b)]
countInCategory count key as =
  let countF a = fmap (a,) $ FL.prefilter ((== a) . key) $ FL.premap count FL.sum
  in traverse countF as

cpsVAnalysis :: forall r. (K.KnitMany r, BR.CacheEffects r) => K.Sem r ()
cpsVAnalysis = do
  K.logLE K.Info "Data prep..."
  data_C <- BRE.prepCCESAndPums False
  cpsV <- BRE.cpsVRows <$> K.ignoreCacheTime data_C
  let cpsCountsByYear = FMR.concatFold
                        $ FMR.mapReduceFold
                        FMR.noUnpack
                        (FMR.assignKeysAndData @'[BR.Year] @'[BRCF.Count])
                        (FMR.foldAndAddKey $ fmap (FT.recordSingleton @BRCF.Count) $ FL.premap (F.rgetField @BRCF.Count) FL.sum)
  BR.logFrame $ FL.fold cpsCountsByYear cpsV
--  dat <- K.ignoreCacheTime data_C
--  K.absorbPandocMonad $ Pandoc.setResourcePath ["br-2021-A/RST"]

  K.newPandoc
    (K.PandocInfo "State_Race Interaction" $ one ("pagetitle","State_Race interaction"))
    $ cpsStateRace False $ K.liftActionWithCacheTime data_C
{-
  K.newPandoc
    (K.PandocInfo "Model Test" $ one ("pagetitle","Model Test"))
    $ cpsModelTest False $ K.liftActionWithCacheTime data_C
-}


cpsModelTest :: (K.KnitOne r, BR.CacheEffects r) => Bool -> K.ActionWithCacheTime r BRE.CCESAndPUMS -> K.Sem r ()
cpsModelTest clearCaches dataAllYears_C = K.wrapPrefix "cpsStateRace" $ do
  let year = 2018
      data_C = fmap (BRE.ccesAndPUMSForYear year) dataAllYears_C
      cpsVGroupBuilder :: [Text] -> [Text] -> SB.StanGroupBuilderM (F.Record BRE.CPSVByCDR) ()
      cpsVGroupBuilder districts states = do
        SB.addGroup "CD" $ SB.makeIndexFromFoldable show districtKey districts
        SB.addGroup "State" $ SB.SupplementalIndex states --SB.makeIndexFromFoldable show (F.rgetField @BR.StateAbbreviation) states
        SB.addGroup "Race" $ SB.makeIndexFromEnum (F.rgetField @DT.RaceAlone4C)
        SB.addGroup "Sex" $ SB.makeIndexFromEnum (F.rgetField @DT.SexC)
--        SB.addGroup "Education" $ SB.makeIndexFromEnum (F.rgetField @DT.CollegeGradC)
--        SB.addGroup "Age" $ SB.makeIndexFromEnum (F.rgetField @DT.SimpleAgeC)
--        SB.addGroup "Ethnicity" $ SB.makeIndexFromEnum (F.rgetField @DT.HispC)
--        SB.addGroup "WNH" $ SB.makeIndexFromEnum wnh

      pumsPSGroupRowMap :: SB.GroupRowMap (F.Record BRE.PUMSByCDR)
      pumsPSGroupRowMap = SB.addRowMap "CD" districtKey
        $ SB.addRowMap "State" (F.rgetField @BR.StateAbbreviation)
        $ SB.addRowMap "Race" (F.rgetField @DT.RaceAlone4C)
        $ SB.addRowMap "Sex" (F.rgetField @DT.SexC)
--        $ SB.addRowMap "Education" (F.rgetField @DT.CollegeGradC)
--        $ SB.addRowMap "Age" (F.rgetField @DT.SimpleAgeC)
        $ SB.emptyGroupRowMap

      dataAndCodeBuilder :: Typeable modelRow
                         => (modelRow -> Int)
                         -> (modelRow -> Int)
                         -> MRP.BuilderM modelRow BRE.CCESAndPUMS ()
      dataAndCodeBuilder totalF succF = do
        cdDataRT <- SB.addIndexedDataSet "CD" (SB.ToFoldable BRE.districtRows) districtKey
        vTotal <- SB.addCountData "T" totalF
        vSucc <- SB.addCountData "S"  succF
        let normal x = SB.normal Nothing $ SB.scalar $ show x
            binaryPrior = normal 1
            sigmaPrior = normal 1
            fePrior = normal 1
            stzPrior = normal 0.01
        alphaE <- SB.intercept "alpha" (normal 2)

        (feCDE, xBetaE, betaE) <- MRP.addFixedEffects @(F.Record BRE.DistrictDataR)
                                  True
                                  fePrior
                                  cdDataRT
                                  (MRP.FixedEffects 1 densityPredictor)

        gSexE <- MRP.addMRGroup binaryPrior sigmaPrior SB.STZNone "Sex"
--        gEduE <- MRP.addMRGroup binaryPrior sigmaPrior SB.STZNone "Education"
--        gAgeE <- MRP.addMRGroup binaryPrior sigmaPrior SB.STZNone "Age"
--        gWNHE <- MRP.addMRGroup binaryPrior sigmaPrior SB.STZNone "WNH"
--        gStateE <- MRP.addMRGroup binaryPrior sigmaPrior SB.STZNone "State"
--        (gWNHStateEV, gWNHStateE) <- MRP.addNestedMRGroup sigmaPrior SB.STZNone "WNH" "State"
--        gEthE <- MRP.addMRGroup binaryPrior nonBinaryPrior SB.STZNone "Ethnicity"
        gRaceE <- MRP.addMRGroup binaryPrior sigmaPrior SB.STZQR "Race"
--        (gRaceStateEV, gRaceStateE) <- MRP.addNestedMRGroup sigmaPrior SB.STZNone "Race" "State"
        let dist = SB.binomialLogitDist vSucc vTotal
            logitPE = SB.multiOp "+" $ alphaE :| [feCDE, gSexE, gRaceE]
        SB.sampleDistV dist logitPE

        acsData <- SB.addUnIndexedDataSet "ACS" (SB.ToFoldable BRE.pumsRows)
        SB.addDataSetIndexes acsData pumsPSGroupRowMap

        MRP.addPostStratification
          dist
          logitPE
          Nothing
          acsData
          pumsPSGroupRowMap
          (S.fromList ["CD", "Sex", "Race"])
          (realToFrac . F.rgetField @PUMS.Citizens)
          MRP.PSShare
          (Just $ SB.GroupTypeTag @Text "State")

--        SB.generateLogLikelihood dist logitPE
--        SB.generatePosteriorPrediction (SB.StanVar "SPred" $ SB.StanArray [SB.NamedDim SB.modeledDataIndexName] SB.StanInt) dist logitPE
        return ()

      extractTestResults :: K.KnitEffects r => SC.ResultAction r d SB.DataSetGroupIntMaps () ()
      extractTestResults = SC.UseSummary f where
        f summary _ aAndEb_C = return ()

  K.logLE K.Info "Building json data wrangler and model code..."
  dat <- K.ignoreCacheTime data_C
  let cpsVBuilder = dataAndCodeBuilder
                    (round . F.rgetField @BRCF.WeightedCount)
                    (round . F.rgetField @BRCF.WeightedSuccesses)
      (districts, states) = FL.fold
                            ((,)
                              <$> (FL.premap districtKey FL.list)
                              <*> (FL.premap (F.rgetField @BR.StateAbbreviation) FL.list)
                            )
                            $ BRE.districtRows dat
      cpsVGroups = cpsVGroupBuilder districts states
  (dw, stanCode) <- K.knitEither
    $ MRP.buildDataWranglerAndCode cpsVGroups () cpsVBuilder dat (SB.ToFoldable BRE.cpsVRows)
  res_C <- MRP.runMRPModel
    True
    (Just "br-2021-A/stan/cpsV")
    ("test")
    ("test" <> show year)
    dw
    stanCode
    "S"
    extractTestResults
    data_C
    (Just 1000)
    (Just 0.99)
    (Just 15)
  return ()

cpsStateRace :: (K.KnitOne r, BR.CacheEffects r) => Bool -> K.ActionWithCacheTime r BRE.CCESAndPUMS -> K.Sem r ()
cpsStateRace clearCaches dataAllYears_C = K.wrapPrefix "cpsStateRace" $ do
  let rstDir = "br-2021-A/RST/cpsStateRace/"
      year = 2016
      data_C = fmap (BRE.ccesAndPUMSForYear year) dataAllYears_C
      cpsVGroupBuilder :: [Text] -> [Text] -> SB.StanGroupBuilderM (F.Record BRE.CPSVByCDR) ()
      cpsVGroupBuilder districts states = do
        SB.addGroup "CD" $ SB.makeIndexFromFoldable show districtKey districts
        SB.addGroup "State" $ SB.makeIndexFromFoldable show (F.rgetField @BR.StateAbbreviation) states
        SB.addGroup "Race" $ SB.makeIndexFromEnum race5FromCPS
--        SB.addGroup "White" $ SB.makeIndexFromEnum ra4White
        SB.addGroup "WNH" $ SB.makeIndexFromEnum wnh
--        SB.addGroup "Ethnicity" $ SB.makeIndexFromEnum (F.rgetField @DT.HispC)
        SB.addGroup "Sex" $ SB.makeIndexFromEnum (F.rgetField @DT.SexC)
        SB.addGroup "Education" $ SB.makeIndexFromEnum (F.rgetField @DT.CollegeGradC)
        SB.addGroup "WhiteNonGrad" $ SB.makeIndexFromEnum wnhNonGrad
        SB.addGroup "Age" $ SB.makeIndexFromEnum (F.rgetField @DT.SimpleAgeC)

      pumsPSGroupRowMap :: SB.GroupRowMap (F.Record BRE.PUMSByCDR)
      pumsPSGroupRowMap = SB.addRowMap "CD" districtKey
        $ SB.addRowMap "State" (F.rgetField @BR.StateAbbreviation)
        $ SB.addRowMap "Sex" (F.rgetField @DT.SexC)
        $ SB.addRowMap "WNH"  wnh
--      $ SB.addRowMap "White" ra4White --(F.rgetField @DT.RaceAlone4C)
        $ SB.addRowMap "Race" race5FromPUMS --(F.rgetField @DT.RaceAlone4C)
--        $ SB.addRowMap "Ethnicity" (F.rgetField @DT.HispC)
        $ SB.addRowMap "Age" (F.rgetField @DT.SimpleAgeC)
        $ SB.addRowMap "Education" (F.rgetField @DT.CollegeGradC)
        $ SB.addRowMap "WhiteNonGrad" wnhNonGrad
        $ SB.emptyGroupRowMap

      dataAndCodeBuilder :: Typeable modelRow
                         => (modelRow -> Int)
                         -> (modelRow -> Int)
                         -> MRP.BuilderM modelRow BRE.CCESAndPUMS ()
      dataAndCodeBuilder totalF succF = do
        cdDataRT <- SB.addIndexedDataSet "CD" (SB.ToFoldable BRE.districtRows) districtKey
        vTotal <- SB.addCountData "T" totalF
        vSucc <- SB.addCountData "S" succF
        let normal x = SB.normal Nothing $ SB.scalar $ show x
            binaryPrior = normal 2
            sigmaPrior = normal 2
            fePrior = normal 2
            sumToZeroPrior = normal 0.01
        alphaE <- SB.intercept "alpha" (normal 2)
        (feCDE, xBetaE, betaE) <- MRP.addFixedEffects @(F.Record BRE.DistrictDataR)
                                  True
                                  fePrior
                                  cdDataRT
                                  (MRP.FixedEffects 2 districtPredictors)
        gSexE <- MRP.addMRGroup binaryPrior sigmaPrior SB.STZNone "Sex"
--        gWNHE <- MRP.addMRGroup binaryPrior sigmaPrior SB.STZNone "WNH"
        gRaceE <- MRP.addMRGroup binaryPrior sigmaPrior SB.STZNone "Race"
--        gEthE <- MRP.addMRGroup binaryPrior sigmaPrior SB.STZNone "Ethnicity"
        gAgeE <- MRP.addMRGroup binaryPrior sigmaPrior SB.STZNone "Age"
        gEduE <- MRP.addMRGroup binaryPrior sigmaPrior SB.STZNone "Education"
        gWNGE <- MRP.addMRGroup binaryPrior sigmaPrior SB.STZNone "WhiteNonGrad"
        gStateE <- MRP.addMRGroup binaryPrior sigmaPrior SB.STZNone "State"
        (gWNHStateV, gWNHState) <- MRP.addNestedMRGroup sigmaPrior SB.STZNone "WNH" "State"
        let dist = SB.binomialLogitDist vSucc vTotal
            logitPE_sample = SB.multiOp "+" $ alphaE :| [feCDE, gSexE, gRaceE, gAgeE, gEduE, gWNGE, gStateE, gWNHStateV]
            logitPE = SB.multiOp "+" $ alphaE :| [feCDE, gSexE, gRaceE, gAgeE, gEduE, gWNGE, gStateE, gWNHState]
            logitPE' = SB.multiOp "+" $ alphaE :| [feCDE, gSexE, gRaceE, gAgeE, gEduE, gWNGE, gStateE]
        SB.sampleDistV dist logitPE_sample
--        SB.generatePosteriorPrediction (SB.StanVar "SPred" $ SB.StanArray [SB.NamedDim "N"] SB.StanInt) dist logitPE
        SB.generateLogLikelihood dist logitPE

        acsData_W <- SB.addUnIndexedDataSet "ACS_WNH" (SB.ToFoldable $ F.filterFrame wnh . BRE.pumsRows)
        SB.addDataSetIndexes acsData_W pumsPSGroupRowMap

        acsData_NW <- SB.addUnIndexedDataSet "ACS_NWNH" (SB.ToFoldable $ F.filterFrame (not . wnh) . BRE.pumsRows)
        SB.addDataSetIndexes acsData_NW pumsPSGroupRowMap

        let postStratByState nameHead modelExp dataSet =
              MRP.addPostStratification
                dist
                modelExp
                (Just nameHead)
                dataSet
                pumsPSGroupRowMap
                (S.fromList ["CD", "Sex", "Race", "WNH", "Age", "Education", "WhiteNonGrad", "State"])
                (realToFrac . F.rgetField @PUMS.Citizens)
                MRP.PSShare
                (Just $ SB.GroupTypeTag @Text "State")

        (SB.StanVar whiteWI psType) <- postStratByState "WI" logitPE acsData_W
        (SB.StanVar nonWhiteWI _) <- postStratByState "WI" logitPE acsData_NW
        (SB.StanVar whiteNI _) <- postStratByState "NI" logitPE' acsData_W
        (SB.StanVar nonWhiteNI _) <- postStratByState "NI" logitPE' acsData_NW


        _ <- SB.inBlock SB.SBGeneratedQuantities $ do
          SB.stanDeclareRHS "rtDiffWI" psType "" $ SB.name whiteWI `SB.minus` SB.name nonWhiteWI
          SB.stanDeclareRHS "rtDiffNI" psType "" $ SB.name whiteNI `SB.minus` SB.name nonWhiteNI
          SB.stanDeclareRHS "rtDiffI" psType "" $ SB.name "rtDiffWI" `SB.minus` SB.name "rtDiffNI"

        return ()

      extractTestResults :: K.KnitEffects r => SC.ResultAction r d SB.DataSetGroupIntMaps () (Map Text [Double], Map Text [Double], Map Text [Double])
      extractTestResults = SC.UseSummary f where
        f summary _ aAndEb_C = do
          let eb_C = fmap snd aAndEb_C
          eb <- K.ignoreCacheTime eb_C
          K.knitEither $ do
            groupIndexes <- eb
            psIndexIM <- SB.getGroupIndex
                         (SB.RowTypeTag @(F.Record BRE.PUMSByCDR) "ACS_WNH")
                         (SB.GroupTypeTag @Text "State")
                         groupIndexes
            vRTDiffWI <- fmap (SP.getVector . fmap CS.percents)
                         $ SP.parse1D "rtDiffWI" (CS.paramStats summary)
            vRTDiffNI <- fmap (SP.getVector . fmap CS.percents)
                         $ SP.parse1D "rtDiffNI" (CS.paramStats summary)
            vRTDiffI <- fmap (SP.getVector . fmap CS.percents)
                        $ SP.parse1D "rtDiffI" (CS.paramStats summary)
{-            cpsVStateIndexIM <- SB.getGroupIndex
                                (SB.ModeledRowTag @(F.Record BRE.CPSVByCDR))
                                (SB.GroupTypeTag @Text "State")
                                groupIndexes
            vRDiff <-fmap (SP.getVector . fmap CS.percents) $ SP.parse1D "rDiff" (CS.paramStats summary)
-}
            rtDiffWI <- indexStanResults psIndexIM vRTDiffWI
            rtDiffNI <- indexStanResults psIndexIM vRTDiffNI
            rtDiffI <- indexStanResults psIndexIM vRTDiffI
            return (rtDiffWI, rtDiffNI, rtDiffI)
--            indexStanResults cpsVStateIndexIM vRDiff
--    K.knitEither $

  K.logLE K.Info "Building json data wrangler and model code..."
  dat <- K.ignoreCacheTime data_C
  let cpsVBuilder = dataAndCodeBuilder
                    (round . F.rgetField @BRCF.WeightedCount)
                    (round . F.rgetField @BRCF.WeightedSuccesses)
      (districts, states) = FL.fold
                            ((,)
                              <$> (FL.premap districtKey FL.list)
                              <*> (FL.premap (F.rgetField @BR.StateAbbreviation) FL.list)
                            )
                            $ BRE.districtRows dat
      cpsVGroups = cpsVGroupBuilder districts states
  (dw, stanCode) <- K.knitEither
    $ MRP.buildDataWranglerAndCode cpsVGroups () cpsVBuilder dat (SB.ToFoldable BRE.cpsVRows)
--  K.logLE K.Info $ show (FL.fold (FL.premap (F.rgetField @BRE.Surveyed) FL.sum) $ BRE.ccesRows dat) <> " people surveyed in mrpData.modeled"
  res_C <- MRP.runMRPModel
    False
    (Just "br-2021-A/stan/cpsV")
    ("stateXrace")
    ("stateXrace" <> show year)
    dw
    stanCode
    "S"
    extractTestResults
    data_C
    (Just 1000)
    (Just 0.99)
    (Just 15)
  (rtDiffWI, rtDiffNI, rtDiffI) <- K.ignoreCacheTime res_C
  K.logLE K.Info $ "results: " <> show rtDiffI
  -- sort on median coefficient
  let sortedStates x = fst <$> (sortOn (\(_,[_,x,_]) -> x) $ M.toList x)
      addCols l y m = M.fromList [("Label", GV.Str l), ("Year", GV.Str $ show y)] <> m
  rtDiffWIMR <- K.knitEither $ fmap (addCols "With Interaction" year) <$> traverse (expandInterval "State") (M.toList rtDiffWI)
  rtDiffNIMR <- K.knitEither $ fmap (addCols "Without Interaction" year) <$> traverse (expandInterval "State") (M.toList rtDiffNI)
  rtDiffIMR <- K.knitEither $ fmap (addCols "Interaction" year) <$> traverse (expandInterval "State") (M.toList rtDiffI)
  K.addRSTFromFile $ rstDir ++ "Intro.rst" -- cpsStateRaceIntroRST
  _ <- K.addHvega Nothing Nothing
    $ coefficientChart
    ("Turnout Gaps without State x Race term (" <> show year <> ")")
    (sortedStates rtDiffNI)
    (FV.ViewConfig 500 1000 5)
    rtDiffNIMR
  K.addRSTFromFile $ rstDir ++ "P2.rst"
  _ <- K.addHvega Nothing Nothing
    $ coefficientChart
    ("Turnout Gaps with State x Race term (" <> show year <> ")")
    (sortedStates rtDiffWI)
    (FV.ViewConfig 500 1000 5)
    rtDiffWIMR
  K.addRSTFromFile $ rstDir ++ "P3.rst"
  _ <- K.addHvega Nothing Nothing
    $ coefficientChart
    ("State x Race contribution to Turnout Gap (" <> show year <> ")")
    (sortedStates rtDiffI)
    (FV.ViewConfig 500 1000 5)
    rtDiffIMR
  return ()

coefficientChart :: (Functor f, Foldable f)
                 => Text
                 -> [Text]
                 ->  FV.ViewConfig
                 -> f (MapRow.MapRow GV.DataValue)
                 -> GV.VegaLite
coefficientChart title sortedStates vc rows =
  let vlData = MapRow.toVLData M.toList [GV.Parse [("Year", GV.FoDate "%Y")]] rows
      encY = GV.position GV.Y [GV.PName "State", GV.PmType GV.Nominal, GV.PSort [GV.CustomSort $ GV.Strings sortedStates]]
      encX = GV.position GV.X [GV.PName "mid", GV.PmType GV.Quantitative]
      xScale = GV.PScale [GV.SDomain $ GV.DNumbers [-0.45, 0.45]]
      encXLo = GV.position GV.XError [GV.PName "lo"]
      encXHi = GV.position GV.XError2 [GV.PName "hi"]
      encL = GV.encoding . encX . encY -- . encColor
      encB = GV.encoding . encX . encXLo . encY . encXHi -- . encColor
      markBar = GV.mark GV.ErrorBar [GV.MTooltip GV.TTData, GV.MTicks []]
      markLine = GV.mark GV.Line []
      markPoint = GV.mark GV.Point [GV.MTooltip GV.TTData]
      specBar = GV.asSpec [encB [], markBar]
      specLine = GV.asSpec [encL [], markLine]
      specPoint = GV.asSpec [encL [], markPoint]
      encXZero = GV.position GV.X [GV.PDatum (GV.Number 0), GV.PAxis [GV.AxNoTitle]]
      encXMean = GV.position GV.X [GV.PName "mid", GV.PAggregate GV.Mean, GV.PAxis [GV.AxNoTitle]]
      specZero = GV.asSpec [(GV.encoding . encXZero) [], GV.mark GV.Rule [GV.MColor "blue"]]
      specMean = GV.asSpec [(GV.encoding . encXMean) [], GV.mark GV.Rule [GV.MColor "red"]]
      layers = GV.layer [specBar, specPoint, specZero, specMean]
      spec = GV.asSpec [layers]
  in FV.configuredVegaLite vc [FV.title title, layers , vlData]

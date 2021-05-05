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
import qualified BlueRipple.Data.Keyed as BRK
import qualified BlueRipple.Data.Loaders as BR
import qualified BlueRipple.Model.House.ElectionResult as BRE
import qualified BlueRipple.Model.StanMRP as MRP
import qualified BlueRipple.Model.StanCCES as BRS
import qualified BlueRipple.Utilities.KnitUtils as BR
import qualified BlueRipple.Utilities.FramesUtils as BRF
import qualified BlueRipple.Utilities.TableUtils as BR

import qualified Control.Foldl as FL
import qualified Data.GenericTrie as GT
import qualified Data.List as List
import qualified Data.IntMap as IM
import qualified Data.Map.Strict as M
import qualified Data.Map.Merge.Strict as M
import qualified Data.MapRow as MapRow

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
import qualified Frames.Visualization.VegaLite.Correlation as FV
import qualified Frames.Visualization.VegaLite.Histogram as FV
import qualified Graphics.Vega.VegaLite as GV
import qualified Graphics.Vega.VegaLite.Compat as FV

import qualified Heidi

import Graphics.Vega.VegaLite.Configuration as FV
  ( AxisBounds (DataMinMax),
    ViewConfig (ViewConfig),
  )
import qualified Graphics.Vega.VegaLite.Configuration as FV
import qualified Frames.Visualization.VegaLite.Data
                                               as FV
import qualified Visualization.VegaLite.Histogram as VL

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
  let htmlDir = "turnoutModel/stateSpecificGaps/"
      notesPath x = htmlDir <> "Notes/" <> x -- where does the file go?
      notesURL x = "Notes/" <> x <> ".html" -- how do we link to it in test?
      postPath = htmlDir <> "/post"

  K.newPandoc
    (K.PandocInfo postPath $ one ("pagetitle","State-Specific Gaps"))
    $ cpsStateRace False notesPath notesURL $ K.liftActionWithCacheTime data_C
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
        let age = F.rgetField @DT.SimpleAgeC
            sex = F.rgetField @DT.SexC
            edu = F.rgetField @DT.CollegeGradC
            race = DT.race4FromRace5 . race5FromCPS
            ageSex r = (age r, sex r)
            ageEdu r = (age r, edu r)
            ageRace r = (age r, race r)
            sexEdu r = (sex r, edu r)
            sexRace r = (sex r, race r)
            eduRace r = (edu r, race r)
        SB.addGroup "CD" $ SB.makeIndexFromFoldable show districtKey districts
        SB.addGroup "Age" $ SB.makeIndexFromEnum age
        SB.addGroup "Sex" $ SB.makeIndexFromEnum sex
        SB.addGroup "Education" $ SB.makeIndexFromEnum edu
        SB.addGroup "Race" $ SB.makeIndexFromEnum (DT.race4FromRace5 . race5FromCPS)
        SB.addGroup "AgeSex" $ SB.makeIndexFromFoldable show ageSex BRK.elements
        SB.addGroup "AgeEdu" $ SB.makeIndexFromFoldable show ageEdu BRK.elements
        SB.addGroup "AgeRace" $ SB.makeIndexFromFoldable show ageRace BRK.elements
        SB.addGroup "SexEdu" $ SB.makeIndexFromFoldable show sexEdu BRK.elements
        SB.addGroup "SexRace" $ SB.makeIndexFromFoldable show sexRace BRK.elements
        SB.addGroup "EduRace" $ SB.makeIndexFromFoldable show eduRace BRK.elements


--        SB.addGroup "State" $ SB.SupplementalIndex states --SB.makeIndexFromFoldable show (F.rgetField @BR.StateAbbreviation) states
--        SB.addGroup "Ethnicity" $ SB.makeIndexFromEnum (F.rgetField @DT.HispC)
--        SB.addGroup "WNH" $ SB.makeIndexFromEnum wnh

      pumsPSGroupRowMap :: SB.GroupRowMap (F.Record BRE.PUMSByCDR)
      pumsPSGroupRowMap = SB.addRowMap "CD" districtKey
        $ SB.addRowMap "State" (F.rgetField @BR.StateAbbreviation)
        $ SB.addRowMap "Race" (DT.race4FromRace5 . race5FromPUMS)
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

        gAgeE <- MRP.addMRGroup binaryPrior sigmaPrior SB.STZNone "Age"
        gSexE <- MRP.addMRGroup binaryPrior sigmaPrior SB.STZNone "Sex"
        gEduE <- MRP.addMRGroup binaryPrior sigmaPrior SB.STZNone "Education"
        gRaceE <- MRP.addMRGroup binaryPrior sigmaPrior SB.STZQR "Race"
        gAgeSexE <- MRP.addMRGroup binaryPrior sigmaPrior SB.STZQR "AgeSex"
        gAgeEduE <- MRP.addMRGroup binaryPrior sigmaPrior SB.STZQR "AgeEdu"
        gAgeRaceE <- MRP.addMRGroup binaryPrior sigmaPrior SB.STZQR "AgeRace"
        gSexEduE <- MRP.addMRGroup binaryPrior sigmaPrior SB.STZQR "SexEdu"
        gSexRaceE <- MRP.addMRGroup binaryPrior sigmaPrior SB.STZQR "SexRace"
        gEduRaceE <- MRP.addMRGroup binaryPrior sigmaPrior SB.STZQR "EduRace"

--        gWNHE <- MRP.addMRGroup binaryPrior sigmaPrior SB.STZNone "WNH"
--        gStateE <- MRP.addMRGroup binaryPrior sigmaPrior SB.STZNone "State"
--        (gWNHStateEV, gWNHStateE) <- MRP.addNestedMRGroup sigmaPrior SB.STZNone "WNH" "State"
--        gEthE <- MRP.addMRGroup binaryPrior nonBinaryPrior SB.STZNone "Ethnicity"
--        (gRaceStateEV, gRaceStateE) <- MRP.addNestedMRGroup sigmaPrior SB.STZNone "Race" "State"
        let dist = SB.binomialLogitDist vSucc vTotal
            logitPE = SB.multiOp "+" $ alphaE :| [feCDE, gAgeE, gSexE, gEduE, gRaceE, gAgeSexE, gAgeEduE, gAgeRaceE, gSexEduE, gSexRaceE, gEduRaceE]
        SB.sampleDistV dist logitPE
{-
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
-}
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
    False
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

cpsStateRace :: (K.KnitMany r, K.KnitOne r, BR.CacheEffects r)
             => Bool
             -> (Text -> Text)
             -> (Text -> Text)
             -> K.ActionWithCacheTime r BRE.CCESAndPUMS -> K.Sem r ()
cpsStateRace clearCaches notesPath notesURL dataAllYears_C = K.wrapPrefix "cpsStateRace" $ do
  let rstDir = "br-2021-A/RST/cpsStateRace/"

      cpsVGroupBuilder :: [Text] -> [Text] -> SB.StanGroupBuilderM (F.Record BRE.CPSVByCDR) ()
      cpsVGroupBuilder districts states = do
        SB.addGroup "CD" $ SB.makeIndexFromFoldable show districtKey districts
        SB.addGroup "State" $ SB.makeIndexFromFoldable show (F.rgetField @BR.StateAbbreviation) states
        SB.addGroup "Race" $ SB.makeIndexFromEnum (DT.race4FromRace5 . race5FromCPS)
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
        $ SB.addRowMap "Race" (DT.race4FromRace5 . race5FromPUMS) --(F.rgetField @DT.RaceAlone4C)
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
          SB.stanDeclareRHS "dNWNH" psType "" $ SB.name nonWhiteWI `SB.minus` SB.name nonWhiteNI
          SB.stanDeclareRHS "dWNH" psType "" $ SB.name whiteWI `SB.minus` SB.name whiteNI

        return ()

      extractTestResults :: K.KnitEffects r
                         => SC.ResultAction r d SB.DataSetGroupIntMaps () (Map Text [Double]
                                                                          , Map Text [Double]
                                                                          , Map Text [Double]
                                                                          , Map Text [Double]
                                                                          , Map Text [Double]
                                                                          , Map Text [Double]
                                                                          )
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
            let parseAndIndexPctsWith f vn = do
                  v <- SP.getVector . fmap CS.percents <$> SP.parse1D vn (CS.paramStats summary)
                  indexStanResults psIndexIM $ Vector.map f v

            rtDiffWI <- parseAndIndexPctsWith (reverse . fmap negate) "rtDiffWI"
            rtDiffNI <- parseAndIndexPctsWith (reverse . fmap negate) "rtDiffNI"
            rtDiffI <- parseAndIndexPctsWith (reverse . fmap negate) "rtDiffI"
            rtNWNH_WI <- parseAndIndexPctsWith id "WI_ACS_NWNH_State"
            dNWNH <- parseAndIndexPctsWith id "dNWNH"
            dWNH <- parseAndIndexPctsWith id "dWNH"
            return (rtDiffWI, rtDiffNI, rtDiffI, rtNWNH_WI, dNWNH, dWNH)

  K.logLE K.Info "Building json data wrangler and model code..."
--  let year = 2016
--      data_C = fmap (BRE.ccesAndPUMSForYear year) dataAllYears_C
--  dataAllYears <- K.ignoreCacheTime dataAllYears_C

  let cpsVCodeBuilder = dataAndCodeBuilder
                        (round . F.rgetField @BRCF.WeightedCount)
                        (round . F.rgetField @BRCF.WeightedSuccesses)

      dataWranglerAndCode data_C years = do
        dat <- K.ignoreCacheTime data_C
        let (districts, states) = FL.fold
                                  ((,)
                                   <$> (FL.premap districtKey FL.list)
                                   <*> (FL.premap (F.rgetField @BR.StateAbbreviation) FL.list)
                                  )
                                  $ BRE.districtRows dat
            cpsVGroups = cpsVGroupBuilder districts states

        K.knitEither
          $ MRP.buildDataWranglerAndCode cpsVGroups () cpsVCodeBuilder dat (SB.ToFoldable BRE.cpsVRows)

      runModel years = do
        let data_C = fmap (BRE.ccesAndPUMSForYears years) dataAllYears_C
        (dw, stanCode) <- dataWranglerAndCode data_C years
        MRP.runMRPModel
          False
          (Just "br-2021-A/stan/cpsV")
          ("stateXrace")
          ("stateXrace" <> (T.intercalate "_" $ fmap show years))
          dw
          stanCode
          "S"
          extractTestResults
          data_C
          (Just 1000)
          (Just 0.99)
          (Just 15)

--  K.logLE K.Info $ show (FL.fold (FL.premap (F.rgetField @BRE.Surveyed) FL.sum) $ BRE.ccesRows dat) <> " people surveyed in mrpData.modeled"
  res2016_C <- runModel [2016]
  (rtDiffWI_2016, rtDiffNI_2016, rtDiffI_2016, rtNWNH, _, _) <- K.ignoreCacheTime res2016_C
  let valToLabeledKV l = GT.toList . Heidi.flatten GT.empty (\tcs -> GT.insert (Heidi.mkTyN l : tcs)) . Heidi.toVal
  let toHeidiRows :: Map Text [Double] -> Heidi.Frame (Heidi.Row [Heidi.TC] Heidi.VP)
      toHeidiRows m = Heidi.frameFromList $ fmap f $ M.toList m where
        f :: (Text, [Double]) -> Heidi.Row [Heidi.TC] Heidi.VP
        f (s, [lo, mid, hi]) = Heidi.rowFromList
                               $ concat [(valToLabeledKV "State" s)
                                        , (valToLabeledKV "lo" lo)
                                        , (valToLabeledKV "mid" mid)
                                        , (valToLabeledKV "hi" hi)
                                        ]

  -- sort on median coefficient
  let rtNWNH_mids = fmap (\[_, x,_] -> x) rtNWNH
  stateTurnout <- K.ignoreCacheTimeM $ BR.stateTurnoutLoader
  let stateVEP y = FL.fold (FL.premap (\r -> (F.rgetField @BR.StateAbbreviation r, F.rgetField @BR.VEP r)) FL.map)
                   $ F.filterFrame ((== y) . F.rgetField @BR.Year) stateTurnout
      stateNWNH y = M.merge M.dropMissing M.dropMissing (M.zipWithMaybeMatched (\_ nwnh vep -> Just $ (nwnh, realToFrac vep))) rtNWNH_mids $ stateVEP y
      avgNWNH y = FL.fold ((/) <$> (FL.premap (\(a, b) -> a * b) FL.sum) <*> (FL.premap snd FL.sum)) $ stateNWNH y
      stateDiffFromAvgMRs y = fmap (\(nwnh, _) -> one ("delta", GV.Number $ nwnh - avgNWNH y)) $ stateNWNH y
  let sortedStates x = fst <$> (sortOn (\(_,[_,x,_]) -> -x) $ M.toList x)
      addCols l y m = M.fromList [("Label", GV.Str l), ("Year", GV.Str y)] <> m
  rtDiffWIMR_2016 <- K.knitEither
                     $ fmap (addCols "With Interaction" "2016") <$> traverse (expandInterval "State") (M.toList rtDiffWI_2016)
                     >>= MapRow.keyedMapRows (\(GV.Str x) -> x) "State"
  rtDiffNIMR_2016 <- K.knitEither
                     $ fmap (addCols "Without Interaction" "2016") <$> traverse (expandInterval "State") (M.toList rtDiffNI_2016)
                     >>= MapRow.keyedMapRows (\(GV.Str x) -> x) "State"
  rtDiffIMR_2016 <- K.knitEither
                    $ fmap (addCols "Interaction" "2016") <$> traverse (expandInterval "State") (M.toList rtDiffI_2016)
                    >>= MapRow.keyedMapRows (\(GV.Str x) -> x) "State"
  let diffNIMR_2016 = MapRow.joinKeyedMapRows rtDiffNIMR_2016 $ stateDiffFromAvgMRs 2016
      diffWIMR_2016 = MapRow.joinKeyedMapRows rtDiffWIMR_2016 $ stateDiffFromAvgMRs 2016
      diffIMR_2016 = MapRow.joinKeyedMapRows rtDiffIMR_2016 $ stateDiffFromAvgMRs 2016
  K.addRSTFromFile $ rstDir ++ "P1a.rst"
  K.addRST $ "`Demographic-only gaps and total gaps <" <> notesURL "1" <> ">`_"
  K.newPandoc
    (K.PandocInfo (notesPath "1") $ one ("pagetitle","State-Specific gaps, Note 1")) $ do
    K.addRSTFromFile $ rstDir ++ "N1.rst"
    _ <- K.addHvega Nothing Nothing
      $ coefficientChart
      ("NWNH/WNH Turnout Gap without State-specific effects (2016)")
      (sortedStates rtDiffNI_2016)
      True
      True
      (FV.ViewConfig 500 1000 5)
      diffNIMR_2016
    K.addRSTFromFile $ rstDir ++ "N2.rst"
    _ <- K.addHvega Nothing Nothing
      $ coefficientChart
      ("NWNH/WNH Turnout Gaps with State-specific effects (2016)")
      (sortedStates rtDiffWI_2016)
      True
      True
      (FV.ViewConfig 500 1000 5)
      diffWIMR_2016
    return ()
  K.addRSTFromFile $ rstDir ++ "P3.rst"
  _ <- K.addHvega Nothing Nothing
    $ coefficientChart
    ("NWNH/WNH State-specific contribution to turnout gap (2016)")
    (sortedStates rtDiffI_2016)
    True
    True
    (FV.ViewConfig 500 1000 5)
    diffIMR_2016
  K.addRSTFromFile $ rstDir ++ "P4.rst"
  res2012_C <- runModel [2012]
  (_, _, rtDiffI_2012, _, _, _) <- K.ignoreCacheTime res2012_C
  rtDiffIMR_2012 <- K.knitEither
                    $ fmap (addCols "Interaction" "2012") <$> traverse (expandInterval "State") (M.toList rtDiffI_2012)
                    >>= MapRow.keyedMapRows (\(GV.Str x) -> x) "State"
  let diffIMR_2012 = MapRow.joinKeyedMapRows rtDiffIMR_2012 $ stateDiffFromAvgMRs 2012
  _ <- K.addHvega Nothing Nothing
    $ coefficientChart
    ("NWNH/WNH State-specific contribution to Turnout Gap (2012)")
    (sortedStates rtDiffI_2012)
    True
    True
    (FV.ViewConfig 500 1000 5)
    diffIMR_2012
  let filterState states x = M.filterWithKey (\k _ -> k `elem` states) x
{-
        K.knitMaybe "Missing State in MapRow"
                             $ fmap (fmap fst . List.filter ((`elem` states) . snd) . zip x)
                             $ traverse (fmap (\(GV.Str x) -> x) . M.lookup "State") x
-}
      sig lo hi = lo * hi > 0
      oneSig [loA,_ , hiA] [loB,_ , hiB] = if sig loA hiA || sig loB hiB then Just () else Nothing
      oneSigStates =  M.keys
                      $ M.merge M.dropMissing M.dropMissing (M.zipWithMaybeMatched (const oneSig)) rtDiffI_2012 rtDiffI_2016
  let combinedOneSig = M.elems (filterState oneSigStates rtDiffIMR_2016) <> M.elems (filterState oneSigStates rtDiffIMR_2012)
  _ <- K.addHvega' Nothing Nothing True
    $ turnoutGapScatter
    ("State-Specific NWNH/WNH Turnout Gaps: 2012 vs. 2016")
    (FV.ViewConfig 500 500 5)
    combinedOneSig
  K.addRSTFromFile $ rstDir ++ "P5.rst"
  let sigBoth [loA,_ , hiA] [loB,_ , hiB] = if sig loA hiA && sig loB hiB && loA * loB > 0 then Just () else Nothing
      sigMove [loA,_ , hiA] [loB,_ , hiB] = if hiA < loB || hiB < loA then Just () else Nothing
      significantPersistent = M.keys
                              $ M.merge M.dropMissing M.dropMissing (M.zipWithMaybeMatched (const sigBoth)) rtDiffI_2012 rtDiffI_2016
      significantMove = M.keys
                        $ M.merge M.dropMissing M.dropMissing (M.zipWithMaybeMatched (const sigMove)) rtDiffI_2012 rtDiffI_2016
  let significantGapsBothMR = M.elems (filterState significantPersistent diffIMR_2016) <> M.elems (filterState significantPersistent diffIMR_2012)
  _ <- K.addHvega Nothing Nothing
    $ coefficientChart
    ("NWNH/WNH State-specific contribution to Turnout Gap: significant *and* persistent gaps in 2012 and 2016")
    (sortedStates rtDiffI_2012)
    False
    False
    (FV.ViewConfig 400 400 5)
    significantGapsBothMR
  let significantMoveMR = M.elems (filterState significantMove diffIMR_2016) <> M.elems (filterState significantMove diffIMR_2012)
  _ <- K.addHvega Nothing Nothing
    $ coefficientChart
    ("State-specific contribution to Turnout Gap: significant changes 2012 to 2016")
    (sortedStates rtDiffI_2012)
    False
    False
    (FV.ViewConfig 400 400 5)
    significantMoveMR
  res2012_2016_C <- runModel [2012, 2016]
  (_, _, rtDiffI_2012_2016, _, _, _) <- K.ignoreCacheTime res2012_2016_C
  rtDiffIMR_2012_2016 <- K.knitEither
                         $ fmap (addCols "Interaction" "2012 & 2016") <$> traverse (expandInterval "State") (M.toList rtDiffI_2012_2016)
                         >>= MapRow.keyedMapRows (\(GV.Str x) -> x) "State"
  let diffIMR_2012_2016 = MapRow.joinKeyedMapRows rtDiffIMR_2012 $ stateDiffFromAvgMRs 2016
  _ <- K.addHvega Nothing Nothing
    $ coefficientChart
    ("State-specific contribution to Turnout Gap: 2012 & 2016")
    (sortedStates rtDiffI_2012_2016)
    True
    True
    (FV.ViewConfig 500 1000 5)
    diffIMR_2012_2016
  return ()


coefficientChart :: (Functor f, Foldable f)
                 => Text
                 -> [Text]
                 -> Bool
                 -> Bool
                 ->  FV.ViewConfig
                 -> f (MapRow.MapRow GV.DataValue)
                 -> GV.VegaLite
coefficientChart title sortedStates showAvg colorIsDelta vc rows =
  let vlData = MapRow.toVLData M.toList [] rows --[GV.Parse [("Year", GV.FoDate "%Y")]] rows
      encY = GV.position GV.Y [GV.PName "State", GV.PmType GV.Nominal, GV.PSort [GV.CustomSort $ GV.Strings sortedStates]]
      encX = GV.position GV.X [GV.PName "mid", GV.PmType GV.Quantitative]
      encColor = if colorIsDelta
                 then GV.color [GV.MName "delta", GV.MmType GV.Quantitative, GV.MScale [GV.SScheme "blueorange" []]]
                 else GV.color [GV.MName "Year", GV.MmType GV.Nominal]
      xScale = GV.PScale [GV.SDomain $ GV.DNumbers [-0.45, 0.45]]
      encXLo = GV.position GV.XError [GV.PName "lo"]
      encXHi = GV.position GV.XError2 [GV.PName "hi"]
      encL = GV.encoding . encX . encY  . encColor
      encB = GV.encoding . encX . encXLo . encY . encXHi . encColor
      markBar = GV.mark GV.ErrorBar [GV.MTooltip GV.TTData, GV.MTicks []]
      markLine = GV.mark GV.Line []
      markPoint = GV.mark GV.Point [GV.MTooltip GV.TTData]
      specBar = GV.asSpec [encB [], markBar]
      specLine = GV.asSpec [encL [], markLine]
      specPoint = GV.asSpec [encL [], markPoint]
      encXZero = GV.position GV.X [GV.PDatum (GV.Number 0), GV.PAxis [GV.AxNoTitle]]
      encXMean = GV.position GV.X [GV.PName "mid", GV.PAggregate GV.Mean, GV.PAxis [GV.AxNoTitle]]
      specZero = GV.asSpec [(GV.encoding . encXZero) [], GV.mark GV.Rule [GV.MColor "skyblue"]]
      specMean = GV.asSpec [(GV.encoding . encXMean) [], GV.mark GV.Rule [GV.MColor "orange"]]
      layers = GV.layer $ [specBar, specPoint, specZero] ++ if showAvg then [specMean] else []
      spec = GV.asSpec [layers]
  in FV.configuredVegaLite vc [FV.title title, layers , vlData]


-- 2 x 2
turnoutGapScatter ::  (Functor f, Foldable f)
                  => Text
                  -> FV.ViewConfig
                  -> f (MapRow.MapRow GV.DataValue)
                  -> GV.VegaLite
turnoutGapScatter title vc@(FV.ViewConfig w h _) rows =
  let vlData = MapRow.toVLData M.toList [] rows -- [GV.Parse [("Year", GV.FoDate "%Y")]] rows
      foldMids = GV.pivot "Year" "mid" [GV.PiGroupBy ["State"]]
      gapScale = GV.PScale [GV.SDomain $ GV.DNumbers [-0.11, 0.12]]
      encY = GV.position GV.Y [GV.PName "2016", GV.PmType GV.Quantitative]
      encX = GV.position GV.X [GV.PName "2012", GV.PmType GV.Quantitative]
      label = GV.text [GV.TName "State", GV.TmType GV.Nominal]
      enc = GV.encoding . encX . encY

--      enc45_X = GV.position GV.X [GV.PName "2016", GV.PmType GV.Quantitative, gapScale, GV.PAxis [GV.AxNoTitle]]
--      mark45 = GV.mark GV.Line
--      spec45 = GV.asSpec [(GV.encoding . enc45_X . encY) [], mark45 []]
      specXaxis = GV.asSpec [GV.encoding . (GV.position GV.Y [GV.PDatum $ GV.Number 0]) $ [],  GV.mark GV.Rule []]
      specYaxis = GV.asSpec [GV.encoding . (GV.position GV.X [GV.PDatum $ GV.Number 0]) $ [],  GV.mark GV.Rule []]
      stateLabelSpec = GV.asSpec [(enc . label) [], GV.mark GV.Text [GV.MTooltip GV.TTData]]
      labelSpec x y l = GV.asSpec [GV.encoding . (GV.position GV.X [GV.PNumber $ x * w]) . (GV.position GV.Y [GV.PNumber $ y * h]) $ []
                              ,GV.mark GV.Text [GV.MText l, GV.MFont "Verdana" ]
                              ]
      labelSpecs = [labelSpec 0.1 0.05 "Bad -> Good"
                   , labelSpec 0.1 0.95 "Both Bad"
                   , labelSpec 0.9 0.95 "Good -> Bad"
                   , labelSpec 0.9 0.05 "Both Good"
                   ]
      mark = GV.mark GV.Point [GV.MTooltip GV.TTData]
      transform = GV.transform . foldMids
      specPts = GV.asSpec [enc [], mark]
  in FV.configuredVegaLiteSchema
     (GV.vlSchema 5 (Just 1) Nothing Nothing)
     vc
     [FV.title title, GV.layer ([{-specPts ,-} stateLabelSpec, specXaxis, specYaxis]), transform [], vlData]

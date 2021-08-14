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

import qualified Data.Massiv.Array

import qualified BlueRipple.Configuration as BR

import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.ElectionTypes as ET
import qualified BlueRipple.Data.Loaders as BR
import qualified BlueRipple.Utilities.KnitUtils as BR
import qualified BlueRipple.Utilities.Heidi as BR
import qualified BlueRipple.Model.House.ElectionResult as BRE
import qualified BlueRipple.Data.CensusLoaders as BRC
import qualified BlueRipple.Model.StanMRP as MRP
import qualified BlueRipple.Data.CountFolds as BRCF

import qualified Control.Foldl as FL
import qualified Data.List as List
import qualified Data.Map.Strict as M
import Data.String.Here (here, i)
import qualified Data.Text as T
import qualified Data.Time.Calendar            as Time
--import qualified Data.Time.Clock               as Time
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V
import qualified Data.Vector as Vector
import qualified Flat
import qualified Frames as F
import qualified Frames.InCore as FI
import qualified Frames.MapReduce as FMR
import qualified Frames.Aggregation as FA
import qualified Frames.Folds as FF
import qualified Frames.Heidi as FH
import qualified Frames.SimpleJoins as FJ
import qualified Frames.Serialize as FS
import qualified Frames.Transform  as FT
import qualified Graphics.Vega.VegaLite as GV
import qualified Graphics.Vega.VegaLite.Compat as FV

import qualified Heidi
import Lens.Micro.Platform ((^?))

import qualified Graphics.Vega.VegaLite.Configuration as FV
import qualified Graphics.Vega.VegaLite.Heidi as HV

import qualified Knit.Report as K
import qualified Knit.Effect.AtomicCache as KC
import qualified Numeric
import qualified Path
import Path (Rel, Abs, Dir, File)
--import qualified Polysemy


import qualified Stan.ModelConfig as SC
import qualified Stan.ModelBuilder as SB
import qualified Stan.ModelBuilder.BuildingBlocks as SB
import qualified Stan.ModelBuilder.SumToZero as SB
import qualified Stan.Parameters as SP
import qualified Stan.Parameters.Massiv as SPM
import qualified CmdStan as CS
--import Stan.ModelBuilder (addUnIndexedDataSet)
import BlueRipple.Data.DataFrames (totalIneligibleFelon', Internal)
import Frames.CSV (prefixInference)
import qualified BlueRipple.Data.CountFolds as BRCF
import qualified BlueRipple.Data.CCES as BRE
import qualified Data.Vinyl.Core as V
import BlueRipple.Model.House.ElectionResult (ccesDataToModelRows)
import qualified Frames.SimpleJoins as FJ
import qualified BlueRipple.Model.House.ElectionResult as BRE
import qualified BlueRipple.Data.Keyed as BRK
import qualified Stan.ModelBuilder as SB
import qualified BlueRipple.Data.CensusTables as BRC
import qualified Frames.MapReduce as FMR
--import qualified Frames.MapReduce.General as FMR


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
          { K.outerLogPrefix = Just "2021-VA"
          , K.logIf = K.logDiagnostic
          , K.pandocWriterConfig = pandocWriterConfig
          , K.serializeDict = BR.flatSerializeDict
          , K.persistCache = KC.persistStrictByteString (\t -> toString (cacheDir <> "/" <> t))
          }
  resE <- K.knitHtmls knitConfig vaAnalysis

  case resE of
    Right namedDocs ->
      K.writeAllPandocResultsWithInfoAsHtml "" namedDocs
    Left err -> putStrLn $ "Pandoc Error: " ++ show err


postDir = [Path.reldir|br-2021-VA/posts|]
postInputs p = postDir BR.</> p BR.</> [Path.reldir|inputs|]
postLocalDraft p = postDir BR.</> p BR.</> [Path.reldir|draft|]
postOnline p =  [Path.reldir|research/StateLeg|] BR.</> p

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
type CPSCVAP = "CPSCVAP" F.:-> Int
type CPSVoters = "CPSVoters" F.:-> Int
type CCESSurveyed = "CPSSurveyed" F.:-> Int
type CCESVoters = "CCESVoters" F.:-> Int
type CCESDVotes = "CCESDVotes" F.:-> Int
type PredictorR = [DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.Race5C, DT.HispC]
type VotingDataR = [CPSCVAP, CPSVoters, CCESSurveyed, CCESVoters, CCESDVotes]

type CensusSERR = BRC.CensusRow BRC.SLDLocationR BRC.ExtensiveDataR [DT.SexC, BRC.Education4C, BRC.RaceEthnicityC]
type SLDDemographicsR = BRC.SLDLocationR V.++ BRC.ExtensiveDataR V.++ [DT.SexC, DT.CollegeGradC, DT.RaceAlone4C, DT.HispC, BRC.Count]

-- fold the census table into an SLDDemographics table
sldDemographicsFld :: FL.Fold (F.Record CensusSERR) (F.FrameRec SLDDemographicsR)
sldDemographicsFld = FMR.concatFold
                     $ FMR.mapReduceFold
                     FMR.noUnpack
                     (FMR.assignKeysAndData @(BRC.SLDLocationR V.++ BRC.ExtensiveDataR V.++ '[DT.SexC]))
                     (FMR.makeRecsWithKey id $ FMR.ReduceFold $ const f) where
  f :: FL.Fold (F.Record [BRC.Education4C, BRC.RaceEthnicityC, BRC.Count]) (F.FrameRec [DT.CollegeGradC, DT.RaceAlone4C, DT.HispC, BRC.Count])
  f =
    let ed4ToCG ed4 = if ed4 == BRC.E4_CollegeGrad then DT.Grad else DT.NonGrad
        edAggF :: BRK.AggF Bool DT.CollegeGrad BRC.Education4 = BRK.AggF g where
          g DT.Grad BRC.E4_CollegeGrad = True
          g DT.Grad _ = False
          g _ _ = True
        edAggFRec = BRK.toAggFRec edAggF

        raceAggFRec :: BRK.AggFRec Bool '[DT.RaceAlone4C, DT.HispC] '[BRC.RaceEthnicityC]
        raceAggFRec = BRK.AggF $ \r1 r2  -> h (F.rgetField @DT.RaceAlone4C r1) (F.rgetField @DT.HispC r1) (F.rgetField @BRC.RaceEthnicityC r2) where
          h :: DT.RaceAlone4 -> DT.Hisp -> BRC.RaceEthnicity -> Bool
          h _ DT.Hispanic BRC.E_Hispanic = True
          h DT.RA4_White DT.NonHispanic BRC.E_WhiteNonHispanic = True
          h DT.RA4_White _ BRC.R_White = True
          h DT.RA4_Black _ BRC.R_Black = True
          h DT.RA4_Asian _ BRC.R_Asian = True
          h DT.RA4_Other _ BRC.R_Other = True
          h _ _ _ = False

        aggFRec = BRK.aggFProductRec edAggFRec raceAggFRec
        collapse = BRK.dataFoldCollapseBool $ fmap (FT.recordSingleton @BRC.Count) $ FL.premap (F.rgetField @BRC.Count) FL.sum
    in fmap F.toFrame $ BRK.aggFoldAllRec aggFRec collapse




type CPSAndCCESR = BRE.CDKeyR V.++ PredictorR V.++ VotingDataR --BRCF.CountCols V.++ [BRE.Surveyed, BRE.TVotes, BRE.DVotes]
data SLDModelData = SLDModelData
  {
    cpsVAndccesRows :: F.FrameRec CPSAndCCESR
  , sldTables :: F.FrameRec SLDDemographicsR
  , districtRows :: F.FrameRec BRE.DistrictDemDataR
  } deriving (Generic)

instance Flat.Flat SLDModelData where
  size (SLDModelData v sld dd) n = Flat.size (FS.SFrame v, FS.SFrame sld, FS.SFrame dd) n
  encode (SLDModelData v sld dd) = Flat.encode (FS.SFrame v, FS.SFrame sld, FS.SFrame dd)
  decode = (\(v, sld, dd) -> SLDModelData (FS.unSFrame v) (FS.unSFrame sld) (FS.unSFrame dd)) <$> Flat.decode

{-
addRaceAlone4 r =
  let r5 = F.rgetField @Race5C r
      rA4 = DT.raceAlone
  in r F.<+> FT.recordSingleton @DT.RaceAloneC

  :: F.Record BRE.CCESPredictorR -> F.Record BRE.CPSPredictorR
ccesPredictorToCpsPredictor r =
  let f :: F.Record '[DT.Race5C] -> F.Record '[DT.RaceAlone4C]
      f = F.rgetField @DT
-}

prepSLDModelData :: (K.KnitEffects r, BR.CacheEffects r)
                 => Bool
                 -> K.Sem r (K.ActionWithCacheTime r SLDModelData)
prepSLDModelData clearCaches = do
  ccesAndCPS_C <- BRE.prepCCESAndPums clearCaches
  sld_C <- BRC.censusTablesBySLD
  let deps = (,) <$> ccesAndCPS_C <*> sld_C
      cacheKey = "model/stateLeg/SLD_VA.bin"
  when clearCaches $ BR.clearIfPresentD cacheKey
  BR.clearIfPresentD cacheKey
  BR.retrieveOrMakeD cacheKey deps $ \(ccesAndCPS, sld) -> do
    let (BRE.CCESAndPUMS ccesRows cpsVRows _ distRows) = ccesAndCPS
        cpsVCols :: F.Record BRE.CPSVByCDR -> F.Record [CPSCVAP, CPSVoters]
        cpsVCols r = round (F.rgetField @BRCF.WeightedCount r) F.&: round (F.rgetField @BRCF.WeightedSuccesses r) F.&: V.RNil
        cpsPredictor :: F.Record BRE.CPSVByCDR -> F.Record PredictorR
        cpsPredictor r = F.rcast $  r F.<+> FT.recordSingleton @DT.Race5C (race5FromCPS r)
        cpsRow r = F.rcast @BRE.CDKeyR r F.<+> cpsPredictor r F.<+> cpsVCols r
        cpsForJoin = cpsRow <$> cpsVRows
        ccesVCols :: F.Record BRE.CCESByCDR -> F.Record [CCESSurveyed, CCESVoters, CCESDVotes]
        ccesVCols r = F.rgetField @BRE.Surveyed r F.&: F.rgetField @BRE.TVotes r F.&: F.rgetField @BRE.DVotes r F.&: V.RNil
        ccesRow r = F.rcast @(BRE.CDKeyR V.++ PredictorR) r F.<+> ccesVCols r
        -- cces data will be missing some rows.  We add zeros.
        ccesForJoin' = ccesRow <$> ccesRows
        defaultCCESV :: F.Record [CCESSurveyed, CCESVoters, CCESDVotes] = 0 F.&: 0 F.&: 0 F.&: V.RNil
        ccesForJoinFld = FMR.concatFold
                         $ FMR.mapReduceFold
                         FMR.noUnpack
                         (FMR.assignKeysAndData @BRE.CDKeyR @(PredictorR V.++ [CCESSurveyed, CCESVoters, CCESDVotes]))
                         (FMR.makeRecsWithKey id $ FMR.ReduceFold (const $ BRK.addDefaultRec @PredictorR defaultCCESV))
        ccesForJoin = FL.fold ccesForJoinFld ccesForJoin'
        (cpsAndCces, missing) = FJ.leftJoinWithMissing @(BRE.CDKeyR V.++ PredictorR) cpsForJoin ccesForJoin
    unless (null missing) $ K.knitError $ "Missing keys in cpsV/cces join: " <> show missing
    --BR.logFrame cpsAndCces
    K.logLE K.Info $ "Re-folding census table..."
    let sldSER = FL.fold sldDemographicsFld $ BRC.sexEducationRace sld
    BR.logFrame sldSER
    return $ SLDModelData cpsAndCces sldSER distRows

vaAnalysis :: forall r. (K.KnitMany r, BR.CacheEffects r) => K.Sem r ()
vaAnalysis = do
  K.logLE K.Info "Data prep..."
  data_C <- prepSLDModelData False
  let va1PostInfo = BR.PostInfo BR.LocalDraft (BR.PubTimes BR.Unpublished Nothing)
  va1Paths <- postPaths "VA1"
  BR.brNewPost va1Paths va1PostInfo "Virginia Lower House"
    $ vaLower False va1Paths va1PostInfo $ K.liftActionWithCacheTime data_C

vaLower :: (K.KnitMany r, K.KnitOne r, BR.CacheEffects r)
        => Bool
        -> BR.PostPaths BR.Abs
        -> BR.PostInfo
        -> K.ActionWithCacheTime r SLDModelData
        -> K.Sem r ()
vaLower clearCaches postPaths postInfo sldDat_C = K.wrapPrefix "vaLower" $ do
  K.logLE K.Info $ "Re-building VA Lower post"
  modelData_C <- prepSLDModelData False
  BR.brAddPostMarkDownFromFile postPaths "_intro"



groupBuilder :: [Text] -> [Text] -> SB.StanGroupBuilderM SLDModelData ()
groupBuilder districts states = do
  voterData <- SB.addDataSetToGroupBuilder "VData" (SB.ToFoldable cpsVAndccesRows)
  SB.addGroupIndexForDataSet cdGroup voterData $ SB.makeIndexFromFoldable show districtKey districts
  SB.addGroupIndexForDataSet stateGroup voterData $ SB.makeIndexFromFoldable show (F.rgetField @BR.StateAbbreviation) states
  SB.addGroupIndexForDataSet ageGroup voterData $ SB.makeIndexFromEnum (F.rgetField @DT.SimpleAgeC)
  SB.addGroupIndexForDataSet sexGroup voterData $ SB.makeIndexFromEnum (F.rgetField @DT.SexC)
  SB.addGroupIndexForDataSet educationGroup voterData $ SB.makeIndexFromEnum (F.rgetField @DT.CollegeGradC)
  SB.addGroupIndexForDataSet raceGroup voterData $ SB.makeIndexFromEnum (DT.race4FromRace5 . F.rgetField @DT.Race5C)

--sldPSGroupRowMap :: SB.GroupRowMap (F.Record  )


{-
stateLegModel :: (K.KnitEffects r, BR.CacheEffects r) => Bool -> K.ActionWithCacheTime r SLDModelData -> K.Sem r ()
stateLegModel clearCaches dat_C = K.wrapPrefix "stateLegModel" $ do
  let modelDir = "br-2021-VA/stan/"
      jsonDataName = "stateLeg_ASR"
      cpsVGroupBuilder :: [Text] -> [Text] -> SB.StanGroupBuilderM (F.Record BRE.CPSVByCDR) ()
      cpsVGroupBuilder districts states = do
        SB.addGroup "CD_CPS" $ SB.makeIndexFromFoldable show districtKey districts
        SB.addGroup "State_CPS" $ SB.makeIndexFromFoldable show (F.rgetField @BR.StateAbbreviation) states
        SB.addGroup "Race_CPS" $ SB.makeIndexFromEnum (DT.race4FromRace5 . race5FromCPS)
        SB.addGroup "WNH_CPS" $ SB.makeIndexFromEnum wnh
        SB.addGroup "Sex_CPS" $ SB.makeIndexFromEnum (F.rgetField @DT.SexC)
        SB.addGroup "Education_CPS" $ SB.makeIndexFromEnum (F.rgetField @DT.CollegeGradC)
        SB.addGroup "WhiteNonGrad_CPS" $ SB.makeIndexFromEnum wnhNonGrad

      ccesGroupBuilder :: [Text] -> [Text] -> SB.StanGroupBuilderM (F.Record BRE.CCESByCDR) ()
      ccesGroupBuilder districts states = do
        SB.addGroup "CD_CCES" $ SB.makeIndexFromFoldable show districtKey districts
        SB.addGroup "State_CCES" $ SB.makeIndexFromFoldable show (F.rgetField @BR.StateAbbreviation) states
        SB.addGroup "Race_CCES" $ SB.makeIndexFromEnum (DT.race4FromRace5 . F.rgetField @DT.Race5C)
        SB.addGroup "WNH_CCES" $ SB.makeIndexFromEnum wnhCCES
        SB.addGroup "Sex_CCES" $ SB.makeIndexFromEnum (F.rgetField @DT.SexC)
        SB.addGroup "Education_CCES" $ SB.makeIndexFromEnum (F.rgetField @DT.CollegeGradC)
        SB.addGroup "WhiteNonGrad_CCES" $ SB.makeIndexFromEnum wnhNonGradCCES

      dataAndCodeBuilder :: Typeable modelRow => MRP.BuilderM modelRow SLDModelData ()
      dataAndCodeBuilder = do
        -- data
        cdDataRT <- addUnIndexedDataSet "CD" (SB.ToFoldable districtRows)
        ccesDataRT <- addUnIndexedDataSet "CCES" (SB.ToFoldable ccesRows)
        cpsDataRT <- addUnIndexedDataSet "CPS" (SB.ToFoldable cpsVRows)
        cpsCVAP <- SB.addCountData' @(F.Record BRE.CPSVByCDR) cpsDataRT "CVAP" (round . F.rgetField @BRCF.WeightedCount)
        cpsVotes <- SB.addCountData' @(F.Record BRE.CPSVByCDR) cpsDataRT "VOTED" (round . F.rgetField @BRCF.WeightedSuccesses)
        ccesVotes <- SB.addCountData' @(F.Record BRE.CCESByCDR) ccesDataRT "VOTED_C" (F.rgetField @BRE.DVotes)
        ccesDVotes <- SB.addCountData' @(F.Record BRE.CCESByCDR) ccesDataRT "DVOTES_C" (F.rgetField @BRE.DVotes)
        -- model
        let normal x = SB.normal Nothing $ SB.scalar $ show x
            binaryPrior = normal 2
            sigmaPrior = normal 2
            fePrior = normal 2
        MRP.addFixedEffectsData @(F.Record BRE.DistrictDemDataR) cdDataRT (MRP.FixedEffects 1 densityPredictor)
        alphaT <- SB.intercept "alphaT" (normal 2)
        (feCDT, xBetaT, betaT) <- MRP.addFixedEffectsParametersAndPriors @(F.Record BRE.DistrictDemDataR)
                                  True
                                  fePrior
                                  cdDataRT
                                  cpsDataRT
        gSexT <- MRP.addMRGroup binaryPrior sigmaPrior SB.STZNone "Sex_CPS"
        gRaceT <- MRP.addMRGroup binaryPrior sigmaPrior SB.STZNone "Race_CPS"
        let distT = SB.binomialLogitDist cpsVotes cpsCVAP
            logitT_sample = SB.multiOp "+" $ alphaT :| [feCDT, gSexT, gRaceT]
        SB.sampleDistV' cpsDataRT distT logitT_sample
        -- Preference
        alphaP <- SB.intercept "alphaP" (normal 2)
        (feCDP, xBetaP, betaP) <- MRP.addFixedEffectsParametersAndPriors @(F.Record BRE.DistrictDemDataR)
                                  True
                                  fePrior
                                  cdDataRT
                                  ccesDataRT
        gSexP <- MRP.addMRGroup binaryPrior sigmaPrior SB.STZNone "Sex_CCES"
        gRaceP <- MRP.addMRGroup binaryPrior sigmaPrior SB.STZNone "Race_CCES"
        let distP = SB.binomialLogitDist ccesVotes ccesDVotes
            logitP_sample = SB.multiOp "+" $ alphaP :| [feCDP, gSexP, gRaceP]
        SB.sampleDistV' ccesDataRT distP logitP_sample
        return ()
  return ()

-}
cdGroup :: SB.GroupTypeTag Text
cdGroup = SB.GroupTypeTag "CD"

stateGroup :: SB.GroupTypeTag Text
stateGroup = SB.GroupTypeTag "State"

ageGroup :: SB.GroupTypeTag DT.SimpleAge
ageGroup = SB.GroupTypeTag "Age"

sexGroup :: SB.GroupTypeTag DT.Sex
sexGroup = SB.GroupTypeTag "Sex"

educationGroup :: SB.GroupTypeTag DT.CollegeGrad
educationGroup = SB.GroupTypeTag "Education"

raceGroup :: SB.GroupTypeTag DT.Race4
raceGroup = SB.GroupTypeTag "Race"

wnhGroup :: SB.GroupTypeTag Bool
wnhGroup = SB.GroupTypeTag "WNH"

wngGroup :: SB.GroupTypeTag Bool
wngGroup = SB.GroupTypeTag "WNG"

race5FromCPS :: F.Record BRE.CPSVByCDR -> DT.Race5
race5FromCPS r =
  let race4A = F.rgetField @DT.RaceAlone4C r
      hisp = F.rgetField @DT.HispC r
  in DT.race5FromRaceAlone4AndHisp True race4A hisp

districtKey r = F.rgetField @BR.StateAbbreviation r <> "-" <> show (F.rgetField @BR.CongressionalDistrict r)
wnh r = (F.rgetField @DT.RaceAlone4C r == DT.RA4_White) && (F.rgetField @DT.HispC r == DT.NonHispanic)
wnhNonGrad r = wnh r && (F.rgetField @DT.CollegeGradC r == DT.NonGrad)
wnhCCES r = (F.rgetField @DT.Race5C r == DT.R5_WhiteNonLatinx) && (F.rgetField @DT.HispC r == DT.NonHispanic)
wnhNonGradCCES r = wnhCCES r && (F.rgetField @DT.CollegeGradC r == DT.NonGrad)
densityPredictor r = Vector.fromList $ [Numeric.log (F.rgetField @DT.PopPerSqMile r)]

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

import qualified ElectionResultsLoaders as BR
import qualified BlueRipple.Configuration as BR
import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.ElectionTypes as ET
import qualified BlueRipple.Data.ModelingTypes as MT
import qualified BlueRipple.Data.CCES as CCES
import qualified BlueRipple.Data.CPSVoterPUMS as CPS
import qualified BlueRipple.Data.Loaders as BR
import qualified BlueRipple.Data.CensusTables as BRC
import qualified BlueRipple.Utilities.KnitUtils as BR
import qualified BlueRipple.Utilities.Heidi as BR
import qualified BlueRipple.Utilities.TableUtils as BR
import qualified BlueRipple.Model.House.ElectionResult as BRE
import qualified BlueRipple.Data.CensusLoaders as BRC
import qualified BlueRipple.Model.StanMRP as MRP
import qualified BlueRipple.Data.CountFolds as BRCF
import qualified BlueRipple.Data.Keyed as BRK

import qualified Colonnade as C
import qualified Text.Blaze.Colonnade as C
import qualified Text.Blaze.Html5.Attributes   as BHA
import qualified Text.Megaparsec as MP
import qualified Text.Megaparsec.Char as MP
import qualified Text.Megaparsec.Char.Lexer as MPL
import qualified Control.Foldl as FL
import qualified Control.Foldl.Statistics as FLS
import qualified Data.Aeson as A
import qualified Data.Aeson.Lens as A
import qualified Data.Csv as CSV hiding (decode)
import qualified Data.Csv.Streaming as CSV
import Data.Csv ((.!))
import qualified Data.List as List
import qualified Data.IntMap as IM
import qualified Data.Map.Strict as M
import qualified Data.Massiv.Array
import Data.String.Here (here, i)
import qualified Data.Set as Set
import qualified Data.Text as T
import qualified Text.Printf as T
import qualified Text.Read  as T
import qualified Data.Time.Calendar            as Time
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V
import qualified Data.Vector as Vector
import qualified Flat
import qualified Frames as F
import qualified Frames.Melt as F
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
import BlueRipple.Data.DataFrames (totalIneligibleFelon', Internal)
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
          { K.outerLogPrefix = Just "2021-StateLegModel"
          , K.logIf = K.logDiagnostic
          , K.pandocWriterConfig = pandocWriterConfig
          , K.serializeDict = BR.flatSerializeDict
          , K.persistCache = KC.persistStrictByteString (\t -> toString (cacheDir <> "/" <> t))
          }
  resE <- K.knitHtmls knitConfig vaAnalysis

  case resE of
    Right namedDocs ->
      K.writeAllPandocResultsWithInfoAsHtml "" namedDocs
    Left err -> putTextLn $ "Pandoc Error: " <> Pandoc.renderError err


postDir = [Path.reldir|br-2021-StateLegModel/posts|]
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
type CCESSurveyed = "CCESSurveyed" F.:-> Int
type CCESVoted = "CCESVoters" F.:-> Int
type CCESHouseVotes = "CCESHouseVotes" F.:-> Int
type CCESHouseDVotes = "CCESHouseDVotes" F.:-> Int

type PredictorR = [DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.Race5C, DT.HispC]
type VotingDataR = [CPSCVAP, CPSVoters, CCESSurveyed, CCESVoted, CCESHouseVotes, CCESHouseDVotes]

--type CensusSERR = BRC.CensusRow BRC.SLDLocationR BRC.ExtensiveDataR [DT.SexC, BRC.Education4C, BRC.RaceEthnicityC]
--type SLDRecodedR = BRC.SLDLocationR
--                   V.++ BRC.ExtensiveDataR
--                   V.++ [DT.SexC, DT.CollegeGradC, DT.RaceAlone4C, DT.HispC, BRC.Count, DT.PopPerSqMile]
type SLDDemographicsR = '[BR.StateAbbreviation] V.++ BRC.CensusRecodedR BRC.LDLocationR V.++ '[DT.Race5C]
type SLDLocWStAbbrR = '[BR.StateAbbreviation] V.++ BRC.LDLocationR

type CPSAndCCESR = BRE.CDKeyR V.++ PredictorR V.++ VotingDataR --BRCF.CountCols V.++ [BRE.Surveyed, BRE.TVotes, BRE.DVotes]
data SLDModelData = SLDModelData
  {
    cpsVAndccesRows :: F.FrameRec CPSAndCCESR
  , ccesRows :: F.FrameRec BRE.CCESByCDR
  , sldTables :: F.FrameRec SLDDemographicsR
  , districtRows :: F.FrameRec BRE.DistrictDemDataR
  } deriving (Generic)

filterVotingDataByYear :: (Int -> Bool) -> SLDModelData -> SLDModelData
filterVotingDataByYear f (SLDModelData a b c d) = SLDModelData (q a) (q b) c d where
  q :: (F.ElemOf rs BR.Year, FI.RecVec rs) => F.FrameRec rs -> F.FrameRec rs
  q = F.filterFrame (f . F.rgetField @BR.Year)


filterCcesAndPumsByYear :: (Int -> Bool) -> BRE.CCESAndPUMS -> BRE.CCESAndPUMS
filterCcesAndPumsByYear f (BRE.CCESAndPUMS cces cps pums dd) = BRE.CCESAndPUMS (q cces) (q cps) (q pums) (q dd) where
  q :: (F.ElemOf rs BR.Year, FI.RecVec rs) => F.FrameRec rs -> F.FrameRec rs
  q = F.filterFrame (f . F.rgetField @BR.Year)

instance Flat.Flat SLDModelData where
  size (SLDModelData v c sld dd) n = Flat.size (FS.SFrame v, FS.SFrame c, FS.SFrame sld, FS.SFrame dd) n
  encode (SLDModelData v c sld dd) = Flat.encode (FS.SFrame v, FS.SFrame c, FS.SFrame sld, FS.SFrame dd)
  decode = (\(v, c, sld, dd) -> SLDModelData (FS.unSFrame v) (FS.unSFrame c) (FS.unSFrame sld) (FS.unSFrame dd)) <$> Flat.decode

aggregatePredictorsCDFld fldData = FMR.concatFold
                                   $ FMR.mapReduceFold
                                   FMR.noUnpack
                                   (FMR.assignKeysAndData @[BR.Year, BR.StateAbbreviation, BR.CongressionalDistrict])
                                   (FMR.foldAndAddKey fldData)

aggregatePredictorsCountyFld fldData = FMR.concatFold
                                       $ FMR.mapReduceFold
                                       FMR.noUnpack
                                       (FMR.assignKeysAndData @[BR.Year, BR.StateAbbreviation, BR.CountyFIPS])
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
{-
  let ces2020VA = F.filterFrame (\r -> F.rgetField @BR.Year r == 2020 && F.rgetField @BR.StateAbbreviation r == "VA") ces
      nVA = FL.fold (FL.premap (F.rgetField @BRE.Surveyed) FL.sum) ces2020VA
      nVoted = FL.fold (FL.premap (F.rgetField @BRE.TVotes) FL.sum) ces2020VA
  K.logLE K.Info $ "CES VA: " <> show nVA <> " rows and " <> show nVoted <> " voters."
  let aggFld :: FL.Fold (F.Record [BRE.Surveyed, BRE.TVotes]) (F.Record [BRE.Surveyed, BRE.TVotes])
      aggFld = FF.foldAllConstrained @Num FL.sum
      aggregated = FL.fold (aggregatePredictorsCDFld aggFld) ces2020VA
  BR.logFrame aggregated
-}

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

prepCCESPUMSandSLD :: (K.KnitEffects r, BR.CacheEffects r)
                  => Bool
                  ->  K.Sem r (K.ActionWithCacheTime r (BRE.CCESAndPUMS, F.FrameRec SLDDemographicsR))
prepCCESPUMSandSLD clearCaches = do
  ccesAndCPS_C <- BRE.prepCCESAndPums clearCaches
  sld_C <- BRC.censusTablesForSLDs
  stateAbbreviations_C <- BR.stateAbbrCrosswalkLoader
  let deps = (,,) <$> ccesAndCPS_C <*> sld_C <*> stateAbbreviations_C
      cacheKey = "model/stateLeg/CCESPUMSandSLD.bin"
  when clearCaches $ BR.clearIfPresentD cacheKey
  res_C <- BR.retrieveOrMakeD cacheKey deps $ \(ccesAndCPS, sld, stateAbbrs) -> do
    let sldSER' =  BRC.censusDemographicsRecode @BRC.LDLocationR $ BRC.sexEducationRace sld
        (sldSER, saMissing) = FJ.leftJoinWithMissing @'[BR.StateFips] sldSER'
                              $ fmap (F.rcast @[BR.StateFips, BR.StateAbbreviation] . FT.retypeColumn @BR.StateFIPS @BR.StateFips) stateAbbrs
        addRace5 = FT.mutate (\r -> FT.recordSingleton @DT.Race5C
                                    $ DT.race5FromRaceAlone4AndHisp True (F.rgetField @DT.RaceAlone4C r) (F.rgetField @DT.HispC r))
    return $ (ccesAndCPS, FS.SFrame $ F.rcast . addRace5 <$> sldSER)
  return $ fmap (second FS.unSFrame) $ res_C


vaAnalysis :: forall r. (K.KnitMany r, BR.CacheEffects r) => K.Sem r ()
vaAnalysis = do
  allData_C <- prepCCESPUMSandSLD False --prepSLDModelData False
{-
  ces <- K.ignoreCacheTimeM CCES.cesLoader
  let tx2018 r = F.rgetField @BR.StateAbbreviation r == "TX" && F.rgetField @BR.Year r == 2018
      ces2018TX = F.filterFrame tx2018 ces
  BR.logFrame ces2018TX

  ces <- K.ignoreCacheTime $ fmap ccesRows allData_C
  debugCES $ F.filterFrame tx2018 ces
  K.knitError "STOP"
-}
  let va1PostInfo = BR.PostInfo BR.LocalDraft (BR.PubTimes (BR.Published $ Time.fromGregorian 2021 9 24) (Just BR.Unpublished))
  va1Paths <- postPaths "VA1"
  BR.brNewPost va1Paths va1PostInfo "Virginia Lower House" $ do
    vaLower False va1Paths va1PostInfo $ K.liftActionWithCacheTime allData_C

--vaLowerColonnade :: BR.CellStyle (F.Record rs) [Char] -> K.Colonnade K.Headed (SLDLocation, [Double]) K.Cell
vaLowerColonnade cas =
  let state = F.rgetField @BR.StateAbbreviation
      dType = F.rgetField @ET.DistrictTypeC
      dNum = F.rgetField @ET.DistrictNumber
      share5 = MT.ciLower . F.rgetField @BRE.ModeledShare
      share50 = MT.ciMid . F.rgetField @BRE.ModeledShare
      share95 = MT.ciUpper . F.rgetField @BRE.ModeledShare
  in C.headed "State" (BR.toCell cas "State" "State" (BR.textToStyledHtml . state))
     <> C.headed "District" (BR.toCell cas "District" "District" (BR.numberToStyledHtml "%d" . dNum))
     <> C.headed "2019 Result" (BR.toCell cas "2019" "2019" (BR.numberToStyledHtml "%2.2f" . (100*) . F.rgetField @BR.DShare))
     <> C.headed "5%" (BR.toCell cas "5%" "5%" (BR.numberToStyledHtml "%2.2f" . (100*) . share5))
     <> C.headed "50%" (BR.toCell cas "50%" "50%" (BR.numberToStyledHtml "%2.2f" . (100*) . share50))
     <> C.headed "95%" (BR.toCell cas "95%" "95%" (BR.numberToStyledHtml "%2.2f" . (100*) . share95))

modelCompColonnade states cas =
  C.headed "Model" (BR.toCell cas "Model" "Model" (BR.textToStyledHtml . fst))
  <> mconcat (fmap (\s -> C.headed (BR.textToCell s) (BR.toCell cas s s (BR.maybeNumberToStyledHtml "%2.2f" . M.lookup s . snd))) states)


comparison :: K.KnitOne r
           => F.FrameRec (BRE.ModelResultsR SLDLocWStAbbrR)
           -> F.FrameRec BR.SLDRaceResultR
           -> Text
           -> K.Sem r (F.FrameRec ((BRE.ModelResultsR SLDLocWStAbbrR) V.++ [BR.Year, BR.Contested, BR.DVotes, BR.RVotes, BR.DShare]))
comparison mr er t = do
  let (modelAndResult, missing)
        = FJ.leftJoinWithMissing @[BR.StateAbbreviation, ET.DistrictTypeC, ET.DistrictNumber] mr er
  when (not $ null missing) $ K.knitError $ "Missing join keys between model and election results: " <> show missing
  let  dShare = F.rgetField @BR.DShare
       dVotes = F.rgetField @BR.DVotes
       rVotes = F.rgetField @BR.RVotes
       delta r =  dShare r - (MT.ciMid $ F.rgetField @BRE.ModeledShare r)
       contested r = dVotes r /= 0 && rVotes r /= 0 -- r > 0.01 && dShare r < 0.99
       means f = FL.fold ((,) <$> FL.prefilter f (FL.premap dShare FLS.mean)
                           <*> FL.prefilter f (FL.premap delta FLS.mean)) modelAndResult
       vars f (meanResult, meanDelta) =  FL.fold ((,) <$> FL.prefilter f (FL.premap dShare (FLS.varianceUnbiased meanResult))
                                                  <*> FL.prefilter f (FL.premap delta (FLS.varianceUnbiased meanDelta))) modelAndResult
       modelY r = (F.rgetField @(MT.ModelId BRE.Model) r, F.rgetField @BR.Year r)
  let statesFld = Set.toList <$> FL.premap (F.rgetField @BR.StateAbbreviation) FL.set
      modelsYrsFld = sortOn show . Set.toList <$> FL.premap modelY FL.set
      (states, modelYrs) = FL.fold ((,) <$> statesFld <*> modelsYrsFld) modelAndResult
      contestedStateModel s (m, y) r = F.rgetField @BR.StateAbbreviation r == s
                                       && F.rgetField @(MT.ModelId BRE.Model) r == m
                                       && F.rgetField @BR.Year r == y
                                       && contested r
      h (model, year) state = (state, 100 * (varResult - varDelta) / varResult)
        where
          f = contestedStateModel state (model, year)
          ms = means f
          (varResult, varDelta) = vars f ms
      g (model, year) = (show model <> "_" <> show year, M.fromList $ fmap (h (model, year)) states)
      explainedVariances = fmap g modelYrs

      width = 700 / realToFrac (length states)
      height = width * realToFrac (length modelYrs)
      single = length modelYrs == 1 && length states == 1
      singleCaption (pctVar :: Double) = " model explains " <> (toText @String $ T.printf "%.1f" pctVar) <> "% of the SLD to SLD variance in contested election results."
      caption = if single
                then head <$> nonEmpty explainedVariances >>= fmap (singleCaption . snd . head) . nonEmpty . M.toList . snd
                else Nothing
  K.logLE K.Info $ show explainedVariances
  _ <- K.addHvega Nothing caption
       $ modelResultScatterChart
       single
       ("Modeled Vs. Actual (" <> t <> ")")
       (FV.ViewConfig width width 5)
       (fmap F.rcast modelAndResult)


  unless single $ BR.brAddRawHtmlTable "Model Comparison: % of variance explained" (BHA.class_ "brTable") (modelCompColonnade states mempty) explainedVariances
  K.logLE K.Info $ show modelYrs
  return modelAndResult

sldDataFilterCensus :: (F.Record SLDDemographicsR -> Bool) -> SLDModelData -> SLDModelData
sldDataFilterCensus g (SLDModelData a b c d) = SLDModelData a b (F.filterFrame g c) d

filterCensus :: (F.Record SLDDemographicsR -> Bool) -> F.FrameRec SLDDemographicsR -> F.FrameRec SLDDemographicsR
filterCensus g = F.filterFrame g


vaLower :: (K.KnitMany r, K.KnitOne r, BR.CacheEffects r)
        => Bool
        -> BR.PostPaths BR.Abs
        -> BR.PostInfo
        -> K.ActionWithCacheTime r (BRE.CCESAndPUMS, F.FrameRec SLDDemographicsR)
        -> K.Sem r ()
vaLower clearCaches postPaths postInfo data_C = K.wrapPrefix "vaLower" $ do
  vaResults <- K.ignoreCacheTimeM BR.getVAResults
  txResults <- K.ignoreCacheTimeM BR.getTXResults
  gaResults <- K.ignoreCacheTimeM BR.getGAResults
  ohResults <- K.ignoreCacheTimeM BR.getOHResults
  nvResults <- K.ignoreCacheTimeM BR.getNVResults
--  K.logLE K.Info $ "OH Election Results"
--  BR.logFrame ohResults
  dlccDistricts :: [Int] <- snd <<$>> (K.knitMaybe "Couldn't find VA in dlccDistricts" $ M.lookup "VA" BR.dlccDistricts)
  let onlyLower r =  F.rgetField @ET.DistrictTypeC r == ET.StateLower
      onlyStates s r = F.rgetField @BR.StateAbbreviation r `elem` s
      onlyState s = onlyStates [s] --F.rgetField @BR.StateAbbreviation r == s
      isVALower r = onlyLower r && onlyState "VA" r
      onlyVALower = F.filterFrame isVALower
      isTXLower r = onlyLower r && onlyState "TX" r
      onlyTXLower = F.filterFrame isTXLower
      isGALower r = onlyLower r && onlyState "GA" r
      onlyGALower = F.filterFrame isGALower
      isNVLower r = onlyLower r && onlyState "NV" r
      onlyNVLower = F.filterFrame isNVLower
      isOHLower r = onlyLower r && onlyState "OH" r
      onlyOHLower = F.filterFrame isOHLower
  K.logLE K.Info $ "Re-building VA Lower post"
  let modelNoteName = BR.Used "Model_Details"
  mModelNoteUrl <- BR.brNewNote postPaths postInfo modelNoteName "State Legislative Election Model" $ do
    BR.brAddNoteMarkDownFromFile postPaths modelNoteName "_intro"
  modelNoteUrl <- K.knitMaybe "naive Model Note Url is Nothing" $ mModelNoteUrl
  let modelRef = "[model_description]: " <> modelNoteUrl
  BR.brAddPostMarkDownFromFileWith postPaths "_intro" (Just modelRef)

  let isDistrict s dt dn r = F.rgetField @BR.StateAbbreviation r == s
                              && F.rgetField @ET.DistrictTypeC r == dt
                              && F.rgetField @ET.DistrictNumber r == dn
  let data2018_C = fmap (first $ filterCcesAndPumsByYear (==2018)) data_C
      data2020_C = fmap (first $ filterCcesAndPumsByYear (==2020)) data_C
      agg = FL.fold aggregatePredictorsInDistricts -- FL.fold aggregatePredictors . FL.fold aggregateDistricts

  let psGroupSet = SB.addGroupToSet BRE.sexGroup
                   $ SB.addGroupToSet BRE.educationGroup
                   $ SB.addGroupToSet BRE.raceGroup
                   $ SB.addGroupToSet BRE.stateGroup
                   $ SB.emptyGroupSet
      modelDir =  "br-2021-StateLegModel/stan"
      psDataSetName = "SLD_Demographics"
      psGroup :: SB.GroupTypeTag (F.Record SLDLocWStAbbrR) = SB.GroupTypeTag "SLD"
      psInfo = (psGroup, psDataSetName, psGroupSet)
      model2018 m =  K.ignoreCacheTimeM $ BRE.electionModel False modelDir m 2018 psInfo (fst <$> data2018_C) (snd <$> data2018_C)
      model2020 m =  K.ignoreCacheTimeM $ BRE.electionModel False modelDir m 2020 psInfo (fst <$> data2020_C) (snd <$> data2020_C)
  modelBase <- model2018 BRE.Base
  modelBase2020 <- model2020 BRE.Base
  modelPlusState <- model2018 BRE.PlusState
  modelPlusState2020 <- model2020 BRE.PlusState
  modelPlusRaceEdu <- model2018 BRE.PlusRaceEdu
  modelPlusRaceEdu2020 <- model2020 BRE.PlusRaceEdu
  modelPlusStateAndStateRace <- model2018 BRE.PlusStateAndStateRace
  modelPlusStateAndStateRace2020 <- model2020 BRE.PlusStateAndStateRace
  modelPlusInteractions <- model2018 BRE.PlusInteractions
  modelPlusInteractions2020 <- model2020 BRE.PlusInteractions
  modelPlusStateAndStateInteractions <- model2018 BRE.PlusStateAndStateInteractions
  modelPlusStateAndStateInteractions2020 <- model2020 BRE.PlusStateAndStateInteractions
  let allModels2018 = modelPlusState
                      <> modelBase
                      <> modelPlusRaceEdu
                      <> modelPlusStateAndStateRace
                      <> modelPlusInteractions
                      <> modelPlusStateAndStateInteractions
      allModels2020 =  modelBase2020
                       <> modelPlusRaceEdu2020
                       <> modelPlusStateAndStateRace2020
                       <> modelPlusInteractions2020
                       <> modelPlusStateAndStateInteractions2020
      allResults = vaResults
                   <> (onlyTXLower txResults)
                   <> (onlyGALower gaResults)
                   <> (onlyNVLower nvResults)
                   <> (onlyOHLower ohResults)
  let f s = F.filterFrame (\r -> onlyLower r && onlyStates s r)
      allStates = ["VA","GA","TX","OH"]
      allModels = [BRE.Base, BRE.PlusState, BRE.PlusRaceEdu, BRE.PlusStateAndStateRace, BRE.PlusInteractions, BRE.PlusStateAndStateInteractions]
      fPost = f ["VA"]
  m <- comparison (fPost modelPlusStateAndStateRace) (fPost allResults) "All"
  m2020 <- comparison (fPost modelPlusStateAndStateRace2020) (fPost allResults) "All"

  BR.brAddPostMarkDownFromFile postPaths "_chartDiscussion"

  let tableNoteName = BR.Used "District_Table"
  _ <- BR.brNewNote postPaths postInfo tableNoteName "VA Lower House Districts" $ do
    let -- sorted = sortOn ( MT.ciMid . F.rgetField @ModeledShare) $ FL.fold FL.list m
        -- sorted2020 = sortOn ( MT.ciMid . F.rgetField @ModeledShare) $ FL.fold FL.list m2020
        sorted2018 = sortOn (F.rgetField @ET.DistrictNumber) $ FL.fold FL.list m
        sorted2020 = sortOn (F.rgetField @ET.DistrictNumber) $ FL.fold FL.list m2020
        bordered c = "border: 3px solid " <> c
        dlccChosenCS  = bordered "purple" `BR.cellStyleIf` \r h -> (F.rgetField @ET.DistrictNumber r `elem` dlccDistricts && h == "District")
        longShot ci = MT.ciUpper ci < 0.48
        leanR ci = MT.ciMid ci < 0.5 && MT.ciUpper ci >= 0.48
        leanD ci = MT.ciMid ci >= 0.5 && MT.ciLower ci <= 0.52
        safeD ci = MT.ciLower ci > 0.52
        mi = F.rgetField @BRE.ModeledShare
        eRes = F.rgetField @BR.DShare
        longShotCS  = bordered "red" `BR.cellStyleIf` \r h -> longShot (mi r) && h == "95%"
        leanRCS =  bordered "pink" `BR.cellStyleIf` \r h -> leanR (mi r) && h `elem` ["95%", "50%"]
        leanDCS = bordered "skyblue" `BR.cellStyleIf` \r h -> leanD (mi r) && h `elem` ["5%","50%"]
        safeDCS = bordered "blue"  `BR.cellStyleIf` \r h -> safeD (mi r) && h == "5%"
        resLongShotCS = bordered "red" `BR.cellStyleIf` \r h -> eRes r < 0.48 && T.isPrefixOf "2019" h
        resLeanRCS = bordered "pink" `BR.cellStyleIf` \r h -> eRes r >= 0.48 && eRes r < 0.5 && T.isPrefixOf "2019" h
        resLeanDCS = bordered "skyblue" `BR.cellStyleIf` \r h -> eRes r >= 0.5 && eRes r <= 0.52 && T.isPrefixOf "2019" h
        resSafeDCS = bordered "blue" `BR.cellStyleIf` \r h -> eRes r > 0.52 && T.isPrefixOf "2019" h

        tableCellStyle = mconcat [dlccChosenCS, longShotCS, leanRCS, leanDCS, safeDCS
                                 , resLongShotCS, resLeanRCS, resLeanDCS, resSafeDCS
                                 ]
    BR.brAddRawHtmlTable "VA Lower Model (2018 data)" (BHA.class_ "brTable") (vaLowerColonnade tableCellStyle) sorted2018
    BR.brAddRawHtmlTable "VA Lower Model (2020 data)" (BHA.class_ "brTable") (vaLowerColonnade tableCellStyle) sorted2020
  return ()


race5FromCPS :: F.Record BRE.CPSVByCDR -> DT.Race5
race5FromCPS r =
  let race4A = F.rgetField @DT.RaceAlone4C r
      hisp = F.rgetField @DT.HispC r
  in DT.race5FromRaceAlone4AndHisp True race4A hisp



modelResultScatterChart :: Bool
                        -> Text
                        -> FV.ViewConfig
                        -> F.FrameRec ([BR.Year, BR.StateAbbreviation, ET.DistrictNumber, BR.Contested, MT.ModelId BRE.Model, BRE.ModeledShare, BR.DShare])
                        -> GV.VegaLite
modelResultScatterChart single title vc rows =
  let toVLDataRec = FVD.asVLData (GV.Str . show) "DataYear"
                    V.:& FVD.asVLData GV.Str "State"
                    V.:& FVD.asVLData (GV.Number . realToFrac) "District Number"
                    V.:& FVD.asVLData GV.Boolean "Contested"
                    V.:& FVD.asVLData (GV.Str . show) "Model"
                    V.:& FVD.asVLData' [("Model_Mid", GV.Number . (*100) . MT.ciMid)
                                      ,("Model_Upper", GV.Number . (*100) . MT.ciUpper)
                                      ,("Model_Lower", GV.Number . (*100) . MT.ciLower)
                                      ]
                    V.:& FVD.asVLData (GV.Number . (*100)) "Election_Result"
                    V.:& V.RNil
      makeModelYear = GV.transform . GV.calculateAs "datum.Model + datum.DataYear" "ModelYear"
      vlData = FVD.recordsToData toVLDataRec rows
      facetState = [GV.FName "State", GV.FmType GV.Nominal]
      encContested = GV.color [GV.MName "Contested", GV.MmType GV.Nominal
                              , GV.MSort [GV.CustomSort $ GV.Booleans [True, False]]]
      facetModel = [GV.FName "ModelYear", GV.FmType GV.Nominal]
      encModelMid = GV.position GV.Y ([GV.PName "Model_Mid"
                                     , GV.PmType GV.Quantitative
                                     , GV.PAxis [GV.AxTitle "Model_Mid"]]
                                     ++ [GV.PScale [if single then GV.SZero False else GV.SDomain (GV.DNumbers [0, 100])]])

      encModelLo = GV.position GV.Y [GV.PName "Model_Lower"
                                  , GV.PmType GV.Quantitative
                                  , GV.PAxis [GV.AxTitle "Model_Low"]
--                                  , GV.PScale [GV.SDomain $ GV.DNumbers [0, 100]]
--                                  , GV.PScale [GV.SZero False]
                                  ]
      encModelHi = GV.position GV.Y2 [GV.PName "Model_Upper"
                                  , GV.PmType GV.Quantitative
                                  , GV.PAxis [GV.AxTitle "Model_High"]
--                                  , GV.PScale [GV.SDomain $ GV.DNumbers [0, 100]]
--                                  , GV.PScale [GV.SZero False]
                                  ]
      encElection = GV.position GV.X [GV.PName "Election_Result"
                                     , GV.PmType GV.Quantitative
                                     --                               , GV.PScale [GV.SZero False]
                                     , GV.PAxis [GV.AxTitle "Election_Result"]]
      enc45 =  GV.position GV.X [GV.PName "Model_Mid"
                                  , GV.PmType GV.Quantitative
                                  , GV.PAxis [GV.AxTitle ""]
                                  ]
      facets = GV.facet [GV.RowBy facetModel, GV.ColumnBy facetState]
      ptEnc = GV.encoding . encModelMid . encElection . encContested
      lineEnc = GV.encoding . encModelMid . enc45
      ptSpec = GV.asSpec [ptEnc [], GV.mark GV.Circle [GV.MTooltip GV.TTData]]
      lineSpec = GV.asSpec [lineEnc [], GV.mark GV.Line [GV.MTooltip GV.TTNone]]

{-
      regression p = GV.transform
                     . GV.filter (GV.FExpr "datum.Election_Result > 1 && datum.Election_Result < 99")
                     . GV.regression "Model_Mid" "Election_Result" [GV.RgParams p]
      errorT = GV.transform
               . GV.calculateAs "datum.Model_Hi - datum.Model_Mid" "E_Model_Hi"
               . GV.calculateAs "datum.Model_Mid - datum.Model_Lo" "E_Model_Lo"
--      rangeEnc = GV.encoding . encModelLo . encModelHi . encElection
--      rangeSpec = GV.asSpec [rangeEnc [], GV.mark GV.Rule []]
      regressionSpec = GV.asSpec [regression False [], ptEnc [], GV.mark GV.Line [GV.MStroke "red"]]
      r2Enc = GV.encoding
              . GV.position GV.X [GV.PNumber 200]
              . GV.position GV.Y [GV.PNumber 20]
              . GV.text [GV.TName "rSquared", GV.TmType GV.Nominal]
      r2Spec = GV.asSpec [regression True [], r2Enc [], GV.mark GV.Text []]
-}
      finalSpec = if single
                  then [FV.title title, GV.layer [ptSpec, lineSpec], makeModelYear [], vlData]
                  else [FV.title title, facets, GV.specification (GV.asSpec [GV.layer [ptSpec, lineSpec]]), makeModelYear [], vlData]
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




---


prepSLDModelData :: (K.KnitEffects r, BR.CacheEffects r)
                 => Bool
                 -> K.Sem r (K.ActionWithCacheTime r SLDModelData)
prepSLDModelData clearCaches = do
  ccesAndCPS_C <- BRE.prepCCESAndPums clearCaches
  sld_C <- BRC.censusTablesForSLDs
  stateAbbreviations_C <- BR.stateAbbrCrosswalkLoader
  let deps = (,,) <$> ccesAndCPS_C <*> sld_C <*> stateAbbreviations_C
      cacheKey = "model/stateLeg/SLD_VA.bin"
  when clearCaches $ BR.clearIfPresentD cacheKey
  BR.retrieveOrMakeD cacheKey deps $ \(ccesAndCPS, sld, stateAbbrs) -> do
    let (BRE.CCESAndPUMS ccesRows cpsVRows _ distRows) = ccesAndCPS
        cpsVCols :: F.Record BRE.CPSVByCDR -> F.Record [CPSCVAP, CPSVoters]
        cpsVCols r = round (F.rgetField @BRCF.WeightedCount r) F.&: round (F.rgetField @BRCF.WeightedSuccesses r) F.&: V.RNil
        cpsPredictor :: F.Record BRE.CPSVByCDR -> F.Record PredictorR
        cpsPredictor r = F.rcast $  r F.<+> FT.recordSingleton @DT.Race5C (race5FromCPS r)
        cpsRow r = F.rcast @BRE.CDKeyR r F.<+> cpsPredictor r F.<+> cpsVCols r
        cpsForJoin = cpsRow <$> cpsVRows
        ccesVCols :: F.Record BRE.CCESByCDR -> F.Record [CCESSurveyed, CCESVoted, CCESHouseVotes, CCESHouseDVotes]
        ccesVCols r = F.rgetField @BRE.Surveyed r F.&: F.rgetField @BRE.Voted r F.&: F.rgetField @BRE.HouseVotes r F.&: F.rgetField @BRE.HouseDVotes r F.&: V.RNil
        ccesRow r = F.rcast @(BRE.CDKeyR V.++ PredictorR) r F.<+> ccesVCols r
        -- cces data will be missing some rows.  We add zeros.
        ccesForJoin' = ccesRow <$> ccesRows
        defaultCCESV :: F.Record [CCESSurveyed, CCESVoted, CCESHouseVotes, CCESHouseDVotes] = 0 F.&: 0 F.&: 0 F.&: 0 F.&: V.RNil
        ccesForJoinFld = FMR.concatFold
                         $ FMR.mapReduceFold
                         FMR.noUnpack
                         (FMR.assignKeysAndData @BRE.CDKeyR @(PredictorR V.++ [CCESSurveyed, CCESVoted, CCESHouseVotes, CCESHouseDVotes]))
                         (FMR.makeRecsWithKey id $ FMR.ReduceFold (const $ BRK.addDefaultRec @PredictorR defaultCCESV))
        ccesForJoin = FL.fold ccesForJoinFld ccesForJoin'
        (cpsAndCces, missing) = FJ.leftJoinWithMissing @(BRE.CDKeyR V.++ PredictorR) cpsForJoin ccesForJoin
    unless (null missing) $ K.knitError $ "Missing keys in cpsV/cces join: " <> show missing
    --BR.logFrame cpsAndCces
    K.logLE K.Info $ "Re-folding census table..."
    let sldSER' = BRC.censusDemographicsRecode @BRC.LDLocationR $ BRC.sexEducationRace sld
    -- add state abbreviations
        (sldSER, saMissing) = FJ.leftJoinWithMissing @'[BR.StateFips] sldSER'
                              $ fmap (F.rcast @[BR.StateFips, BR.StateAbbreviation] . FT.retypeColumn @BR.StateFIPS @BR.StateFips) stateAbbrs
        addRace5 = FT.mutate (\r -> FT.recordSingleton @DT.Race5C
                                    $ DT.race5FromRaceAlone4AndHisp True (F.rgetField @DT.RaceAlone4C r) (F.rgetField @DT.HispC r))
    return $ SLDModelData cpsAndCces ccesRows (F.rcast . addRace5 <$> sldSER) distRows

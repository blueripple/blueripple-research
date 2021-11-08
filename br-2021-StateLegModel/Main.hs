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
import Frames.CSV (prefixInference)
import qualified Data.Vinyl.Core as V
--import BlueRipple.Model.House.ElectionResult (ccesDataToModelRows)
import qualified Stan.ModelBuilder as SB
import qualified Text.Pandoc as Pandoc
import qualified Debug.Trace as DT
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
type SLDDemographicsR = '[BR.StateAbbreviation] V.++ BRC.SLDRecodedR BRC.SLDLocationR

{-
sldDemographicsRecode ::  F.FrameRec CensusSERR -> F.FrameRec SLDRecodedR
sldDemographicsRecode rows =
  let fld1 = FMR.concatFold
             $ FMR.mapReduceFold
             FMR.noUnpack
             (FMR.assignKeysAndData @(BRC.SLDLocationR V.++ BRC.ExtensiveDataR V.++ '[DT.SexC]))
             (FMR.makeRecsWithKey id $ FMR.ReduceFold $ const edFld)
      fld2 = FMR.concatFold
             $ FMR.mapReduceFold
             FMR.noUnpack
             (FMR.assignKeysAndData @(BRC.SLDLocationR V.++ BRC.ExtensiveDataR V.++ '[DT.SexC, DT.CollegeGradC]))
             (FMR.makeRecsWithKey id $ FMR.ReduceFold $ const reFld)

      edFld :: FL.Fold (F.Record [BRC.Education4C, BRC.RaceEthnicityC, BRC.Count]) (F.FrameRec [DT.CollegeGradC, BRC.RaceEthnicityC, BRC.Count])
      edFld  = let ed4ToCG ed4 = if ed4 == BRC.E4_CollegeGrad then DT.Grad else DT.NonGrad
                   edAggF :: BRK.AggF Bool DT.CollegeGrad BRC.Education4 = BRK.AggF g where
                     g DT.Grad BRC.E4_CollegeGrad = True
                     g DT.Grad _ = False
                     g _ _ = True
                   edAggFRec = BRK.toAggFRec edAggF
                   raceAggFRec :: BRK.AggFRec Bool '[BRC.RaceEthnicityC] '[BRC.RaceEthnicityC] = BRK.toAggFRec BRK.aggFId
                   aggFRec = BRK.aggFProductRec edAggFRec raceAggFRec
                   collapse = BRK.dataFoldCollapseBool $ fmap (FT.recordSingleton @BRC.Count) $ FL.premap (F.rgetField @BRC.Count) FL.sum
               in fmap F.toFrame $ BRK.aggFoldAllRec aggFRec collapse
      reFld ::  FL.Fold (F.Record [BRC.RaceEthnicityC, BRC.Count]) (F.FrameRec [DT.RaceAlone4C, DT.HispC, BRC.Count])
      reFld =
        let withRE re = FL.prefilter ((== re) . F.rgetField @BRC.RaceEthnicityC) $ FL.premap (F.rgetField @BRC.Count) FL.sum
            wFld = withRE BRC.R_White
            bFld = withRE BRC.R_Black
            aFld = withRE BRC.R_Asian
            oFld = withRE BRC.R_Other
            hFld = withRE BRC.E_Hispanic
            wnhFld = withRE BRC.E_WhiteNonHispanic
            makeRec :: DT.RaceAlone4 -> DT.Hisp -> Int -> F.Record [DT.RaceAlone4C, DT.HispC, BRC.Count]
            makeRec ra4 e c = ra4 F.&: e F.&: c F.&: V.RNil
            recode w b a o h wnh =
              let wh = w - wnh
                  oh = min o (h - wh) --assumes most Hispanic people who don't choose white, choose "other"
                  onh = o - oh
                  bh = h - wh - oh
                  bnh = b - bh
              in F.toFrame
                 [
                   makeRec DT.RA4_White DT.Hispanic wh
                 , makeRec DT.RA4_White DT.NonHispanic wnh
                 , makeRec DT.RA4_Black DT.Hispanic bh
                 , makeRec DT.RA4_Black DT.NonHispanic bnh
                 , makeRec DT.RA4_Asian DT.Hispanic 0
                 , makeRec DT.RA4_Asian DT.NonHispanic a
                 , makeRec DT.RA4_Other DT.Hispanic oh
                 , makeRec DT.RA4_Other DT.NonHispanic onh
                 ]
        in recode <$> wFld <*> bFld <*> aFld <*> oFld <*> hFld <*> wnhFld
      addDensity r = FT.recordSingleton @DT.PopPerSqMile
                     $ realToFrac (F.rgetField @BR.Population r)/ F.rgetField @BRC.SqMiles r
  in fmap (FT.mutate addDensity) $ FL.fold fld2 (FL.fold fld1 rows)
-}

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



prepSLDModelData :: (K.KnitEffects r, BR.CacheEffects r)
                 => Bool
                 -> K.Sem r (K.ActionWithCacheTime r SLDModelData)
prepSLDModelData clearCaches = do
  ccesAndCPS_C <- BRE.prepCCESAndPums clearCaches
  sld_C <- BRC.censusTablesBySLD
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
    let sldSER' = BRC.sldDemographicsRecode @BRC.SLDLocationR $ BRC.sexEducationRace sld
    -- add state abbreviations
        (sldSER, saMissing) = FJ.leftJoinWithMissing @'[BR.StateFips] sldSER'
                              $ fmap (F.rcast @[BR.StateFips, BR.StateAbbreviation] . FT.retypeColumn @BR.StateFIPS @BR.StateFips) stateAbbrs
    return $ SLDModelData cpsAndCces ccesRows (F.rcast <$> sldSER) distRows

vaAnalysis :: forall r. (K.KnitMany r, BR.CacheEffects r) => K.Sem r ()
vaAnalysis = do
  allData_C <- prepSLDModelData False
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
      share5 = MT.ciLower . F.rgetField @ModeledShare
      share50 = MT.ciMid . F.rgetField @ModeledShare
      share95 = MT.ciUpper . F.rgetField @ModeledShare
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
           => F.FrameRec ModelResultsR
           -> F.FrameRec BR.SLDRaceResultR
           -> Text
           -> K.Sem r (F.FrameRec (ModelResultsR V.++ [BR.Year, BR.Contested, BR.DVotes, BR.RVotes, BR.DShare]))
comparison mr er t = do
  let (modelAndResult, missing)
        = FJ.leftJoinWithMissing @[BR.StateAbbreviation, ET.DistrictTypeC, ET.DistrictNumber] mr er
  when (not $ null missing) $ K.knitError $ "Missing join keys between model and election results: " <> show missing
  let  dShare = F.rgetField @BR.DShare
       dVotes = F.rgetField @BR.DVotes
       rVotes = F.rgetField @BR.RVotes
       delta r =  dShare r - (MT.ciMid $ F.rgetField @ModeledShare r)
       contested r = dVotes r /= 0 && rVotes r /= 0 -- r > 0.01 && dShare r < 0.99
       means f = FL.fold ((,) <$> FL.prefilter f (FL.premap dShare FLS.mean)
                           <*> FL.prefilter f (FL.premap delta FLS.mean)) modelAndResult
       vars f (meanResult, meanDelta) =  FL.fold ((,) <$> FL.prefilter f (FL.premap dShare (FLS.varianceUnbiased meanResult))
                                                  <*> FL.prefilter f (FL.premap delta (FLS.varianceUnbiased meanDelta))) modelAndResult
       modelY r = (F.rgetField @(MT.ModelId Model) r, F.rgetField @BR.Year r)
  let statesFld = Set.toList <$> FL.premap (F.rgetField @BR.StateAbbreviation) FL.set
      modelsYrsFld = sortOn show . Set.toList <$> FL.premap modelY FL.set
      (states, modelYrs) = FL.fold ((,) <$> statesFld <*> modelsYrsFld) modelAndResult
      contestedStateModel s (m, y) r = F.rgetField @BR.StateAbbreviation r == s
                                       && F.rgetField @(MT.ModelId Model) r == m
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

vaLower :: (K.KnitMany r, K.KnitOne r, BR.CacheEffects r)
        => Bool
        -> BR.PostPaths BR.Abs
        -> BR.PostInfo
        -> K.ActionWithCacheTime r SLDModelData
        -> K.Sem r ()
vaLower clearCaches postPaths postInfo sldDat_C = K.wrapPrefix "vaLower" $ do
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
  let sldDat2018_C = fmap (filterVotingDataByYear (==2018)) sldDat_C
      sldDat2020_C = fmap (filterVotingDataByYear (==2020)) sldDat_C

      agg = FL.fold aggregatePredictorsInDistricts -- FL.fold aggregatePredictors . FL.fold aggregateDistricts
  K.ignoreCacheTime sldDat2018_C >>= BR.logFrame . agg . F.filterFrame (onlyStates ["TX"]) . ccesRows
  K.ignoreCacheTime sldDat2020_C >>= BR.logFrame . agg . F.filterFrame (onlyStates ["TX"]) . ccesRows

{-
  K.ignoreCacheTime sldDat_C >>= BR.logFrame . F.filterFrame (isDistrict "VA" ET.StateLower 12) . sldTables
  K.ignoreCacheTime sldDat_C >>= BR.logFrame . F.filterFrame (isDistrict "VA" ET.StateLower 100) . sldTables
  K.ignoreCacheTime sldDat_C >>= BR.logFrame . F.filterFrame (isDistrict "VA" ET.StateLower 84) . sldTables
  K.ignoreCacheTime sldDat_C >>= BR.logFrame . F.filterFrame (isDistrict "VA" ET.StateLower 90) . sldTables
-}
  modelBase <- K.ignoreCacheTimeM $ stateLegModel False Base 2018 sldDat2018_C
  modelBase2020 <- K.ignoreCacheTimeM $ stateLegModel False Base 2020 sldDat2020_C
  modelPlusState <- K.ignoreCacheTimeM $ stateLegModel False PlusState 2018 sldDat2018_C
  modelPlusState2020 <- K.ignoreCacheTimeM $ stateLegModel False PlusState 2020 sldDat2020_C
  modelPlusRaceEdu <- K.ignoreCacheTimeM $ stateLegModel False PlusRaceEdu 2018 sldDat2018_C
  modelPlusRaceEdu2020 <- K.ignoreCacheTimeM $ stateLegModel False PlusRaceEdu 2020 sldDat2020_C
  modelPlusStateAndStateRace <- K.ignoreCacheTimeM $ stateLegModel False PlusStateAndStateRace 2018 sldDat2018_C
  modelPlusStateAndStateRace2020 <- K.ignoreCacheTimeM $ stateLegModel False PlusStateAndStateRace 2020 sldDat2020_C
  modelPlusInteractions <- K.ignoreCacheTimeM $ stateLegModel False PlusInteractions 2018 sldDat2018_C
  modelPlusInteractions2020 <- K.ignoreCacheTimeM $ stateLegModel False PlusInteractions 2020 sldDat2020_C
  modelPlusStateAndStateInteractions <- K.ignoreCacheTimeM $ stateLegModel False PlusStateAndStateInteractions 2018 sldDat2018_C
  modelPlusStateAndStateInteractions2020 <- K.ignoreCacheTimeM $ stateLegModel False PlusStateAndStateInteractions 2020 sldDat2020_C
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
      allModels = [Base, PlusState, PlusRaceEdu, PlusStateAndStateRace, PlusInteractions, PlusStateAndStateInteractions]
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
        mi = F.rgetField @ModeledShare
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


{-
  modelPlusState_C <- stateLegModel False PlusState CES_Turnout sldDat_C
  modelPlusState <- K.ignoreCacheTime modelPlusState_C
  comparison modelPlusState "CES Turnout"

  modelBase_C <- stateLegModel False Base CPS_Turnout sldDat_C
  modelBase <- K.ignoreCacheTime modelBase_C
  comparison modelBase "Base  (CPS Turnout)"

  modelPlusInteractions_C <- stateLegModel False PlusInteractions CPS_Turnout sldDat_C
  modelPlusInteractions <- K.ignoreCacheTime modelPlusInteractions_C
  comparison modelPlusInteractions "Plus Interactions (CPS Turnout)"


  modelPlusSexEdu_C <- stateLegModel False PlusSexEdu CPS_Turnout sldDat_C
  modelPlusSexEdu <- K.ignoreCacheTime modelPlusSexEdu_C
  comparison modelPlusSexEdu "Plus Sex/Education Interactions (CPS Turnout)"

  modelPlusStateAndStateRace_C <- stateLegModel False PlusStateAndStateRace CPS_Turnout sldDat_C
  modelPlusStateAndStateRace <- K.ignoreCacheTime modelPlusStateAndStateRace_C
  comparison modelPlusStateAndStateRace "Plus State and State/Race Interactions (CPS Turnout)"
-}
--  BR.logFrame model

--  BR.logFrame vaResults
  -- join model results with election results


  --        BR.brAddRawHtmlTable "VA Lower Model (2018 data)" (BHA.class_ "brTable") (vaLowerColonnade mempty) m


race5 r = DT.race5FromRaceAlone4AndHisp True (F.rgetField @DT.RaceAlone4C r) (F.rgetField @DT.HispC r)


groupBuilder :: [Text]
             -> [Text]
             -> [SLDLocation]
             -> SB.StanGroupBuilderM SLDModelData ()
groupBuilder districts states slds = do
  voterData <- SB.addDataSetToGroupBuilder "VData" (SB.ToFoldable ccesRows)
  SB.addGroupIndexForDataSet cdGroup voterData $ SB.makeIndexFromFoldable show districtKey districts
  SB.addGroupIndexForDataSet stateGroup voterData $ SB.makeIndexFromFoldable show (F.rgetField @BR.StateAbbreviation) states
--  SB.addGroupIndexForDataSet ageGroup voterData $ SB.makeIndexFromEnum (F.rgetField @DT.SimpleAgeC)
  SB.addGroupIndexForDataSet sexGroup voterData $ SB.makeIndexFromEnum (F.rgetField @DT.SexC)
  SB.addGroupIndexForDataSet educationGroup voterData $ SB.makeIndexFromEnum (F.rgetField @DT.CollegeGradC)
  SB.addGroupIndexForDataSet raceGroup voterData $ SB.makeIndexFromEnum (F.rgetField @DT.Race5C)
  SB.addGroupIndexForDataSet hispanicGroup voterData $ SB.makeIndexFromEnum (F.rgetField @DT.HispC)
  cdData <- SB.addDataSetToGroupBuilder "CDData" (SB.ToFoldable districtRows)
--  SB.addGroupIndexForDataSet cdGroup cdData $ SB.makeIndexFromFoldable show districtKey districts
  SB.addGroupIndexForCrosswalk cdData $ SB.makeIndexFromFoldable show districtKey districts
  sldData <- SB.addDataSetToGroupBuilder "SLD_Demographics" (SB.ToFoldable sldTables)
  SB.addGroupIndexForDataSet stateGroup sldData $ SB.makeIndexFromFoldable show (F.rgetField @BR.StateAbbreviation) states
  SB.addGroupIndexForDataSet educationGroup sldData $ SB.makeIndexFromEnum (F.rgetField @DT.CollegeGradC)
  SB.addGroupIndexForDataSet sexGroup sldData $ SB.makeIndexFromEnum (F.rgetField @DT.SexC)
  SB.addGroupIndexForDataSet raceGroup sldData $ SB.makeIndexFromEnum race5
  SB.addGroupIndexForDataSet sldGroup sldData $ SB.makeIndexFromFoldable show sldKey slds
  return ()


sldPSGroupRowMap :: SB.GroupRowMap (F.Record SLDDemographicsR)
sldPSGroupRowMap = SB.addRowMap sexGroup (F.rgetField @DT.SexC)
                   $ SB.addRowMap educationGroup (F.rgetField @DT.CollegeGradC)
                   $ SB.addRowMap raceGroup race5
                   $ SB.addRowMap stateGroup (F.rgetField @BR.StateAbbreviation)
                   $ SB.emptyGroupRowMap

sldPSGroupSet :: SB.GroupSet
sldPSGroupSet = SB.addGroupToSet sexGroup
                $ SB.addGroupToSet educationGroup
                $ SB.addGroupToSet raceGroup
                $ SB.addGroupToSet stateGroup
                $ SB.emptyGroupSet


data Model = Base
           | PlusState
           | PlusSexEdu
           | PlusRaceEdu
           | PlusInteractions
           | PlusStateAndStateRace
           | PlusStateAndStateInteractions
           deriving (Show, Eq, Ord, Generic)

instance Flat.Flat Model
type instance FI.VectorFor Model = Vector.Vector

stateLegModel :: (K.KnitEffects r, BR.CacheEffects r)
              => Bool
              -> Model
              -> Int
              -> K.ActionWithCacheTime r SLDModelData
              -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec ModelResultsR))
stateLegModel clearCaches model datYear dat_C = K.wrapPrefix "stateLegModel" $ do
  K.logLE K.Info $ "(Re-)running state-leg model if necessary."
  let modelDir = "br-2021-StateLegModel/stan/"
      jsonDataName = "stateLeg_ASR_" <> show model <> "_" <> show datYear
      dataAndCodeBuilder :: MRP.BuilderM SLDModelData ()
      dataAndCodeBuilder = do
        -- data
        voteData <- SB.dataSetTag @(F.Record BRE.CCESByCDR) "VData"
        cdData <- SB.dataSetTag @(F.Record BRE.DistrictDemDataR) "CDData"
        SB.addDataSetsCrosswalk voteData cdData cdGroup
        SB.setDataSetForBindings voteData

        centerF <- MRP.addFixedEffectsData cdData (MRP.FixedEffects 1 densityPredictor)

        let normal x = SB.normal Nothing $ SB.scalar $ show x
            binaryPrior = normal 2
            sigmaPrior = normal 2
            fePrior = normal 2

        let simpleGroupModel x = SB.NonHierarchical SB.STZNone (normal x)
            muH gn s = "mu" <> s <> "_" <> gn
            sigmaH gn s = "sigma" <> s <> "_" <> gn
            hierHPs gn s = M.fromList [(muH gn s, ("", SB.stdNormal)), (sigmaH gn s, ("<lower=0>", SB.normal (Just $ SB.scalar "0") (SB.scalar "2")))]
            hierGroupModel' gn s = SB.Hierarchical SB.STZNone (hierHPs gn s) (SB.Centered $ SB.normal (Just $ SB.name $ muH gn s) (SB.name $ sigmaH gn s))
            hierGroupModel gtt s = hierGroupModel' (SB.taggedGroupName gtt) s
--              let gn = SB.taggedGroupName gtt
--              in SB.Hierarchical SB.STZNone (hierHPs gn s) (SB.Centered $ SB.normal (Just $ SB.name $ muH gn s) (SB.name $ sigmaH gn s))
            gmSigmaName gtt suffix = "sigma" <> suffix <> "_" <> SB.taggedGroupName gtt
            groupModelMR gtt s = SB.hierarchicalCenteredFixedMeanNormal 0 (gmSigmaName gtt s) sigmaPrior SB.STZNone
        -- Turnout
        cvap <- SB.addCountData voteData "CVAP" (F.rgetField @BRE.Surveyed)
        votes <- SB.addCountData voteData "VOTED" (F.rgetField @BRE.Voted)

        (feCDT, betaT) <- MRP.addFixedEffectsParametersAndPriors
                          True
                          fePrior
                          cdData
                          voteData
                          (Just "T")

        gSexT <- MRP.addGroup voteData binaryPrior (simpleGroupModel 1) sexGroup (Just "T")
        gEduT <- MRP.addGroup voteData binaryPrior (simpleGroupModel 1) educationGroup (Just "T")
        gRaceT <- MRP.addGroup voteData binaryPrior (hierGroupModel raceGroup "T") raceGroup (Just "T")
--        gStateT <- MRP.addGroup voteData binaryPrior (hierGroupModel stateGroup) stateGroup (Just "T")
        let distT = SB.binomialLogitDist votes cvap

        -- Preference
        hVotes <- SB.addCountData voteData "HVOTES_C" (F.rgetField @BRE.HouseVotes)
        dVotes <- SB.addCountData voteData "HDVOTES_C" (F.rgetField @BRE.HouseDVotes)
        (feCDP, betaP) <- MRP.addFixedEffectsParametersAndPriors
                          True
                          fePrior
                          cdData
                          voteData
                          (Just "P")

        gSexP <- MRP.addGroup voteData binaryPrior (simpleGroupModel 1) sexGroup (Just "P")
        gEduP <- MRP.addGroup voteData binaryPrior (simpleGroupModel 1) educationGroup (Just "P")
        gRaceP <- MRP.addGroup voteData binaryPrior (hierGroupModel raceGroup "P") raceGroup (Just "P")
        let distP = SB.binomialLogitDist dVotes hVotes


        (logitT_sample, logitT_ps, logitP_sample, logitP_ps, psGroupSet) <- case model of
              Base -> return (\d -> SB.multiOp "+" $ d :| [gRaceT, gSexT, gEduT]
                             ,\d -> SB.multiOp "+" $ d :| [gRaceT, gSexT, gEduT]
                             ,\d -> SB.multiOp "+" $ d :| [gRaceP, gSexP, gEduP]
                             ,\d -> SB.multiOp "+" $ d :| [gRaceP, gSexP, gEduP]
                             , Set.fromList ["Sex", "Race", "Education"]
                             )
              PlusState -> do
                gStateT <- MRP.addGroup voteData binaryPrior (groupModelMR stateGroup "T") stateGroup (Just "T")
                gStateP <- MRP.addGroup voteData binaryPrior (groupModelMR stateGroup "P") stateGroup (Just "P")
                let logitT d = SB.multiOp "+" $ d :| [gRaceT, gSexT, gEduT, gStateT]
                    logitP d = SB.multiOp "+" $ d :| [gRaceP, gSexP, gEduP, gStateP]
                return (logitT, logitT, logitP, logitP, Set.fromList ["Sex", "Race", "Education", "State"])
              PlusSexEdu -> do
                let hierGM s = SB.hierarchicalCenteredFixedMeanNormal 0 ("sigmaSexEdu" <> s) sigmaPrior SB.STZNone
                sexEduT <- MRP.addInteractions2 voteData (hierGM "T") sexGroup educationGroup (Just "T")
                vSexEduT <- SB.inBlock SB.SBModel $ SB.vectorizeVar sexEduT voteData
                sexEduP <- MRP.addInteractions2 voteData (hierGM "P") sexGroup educationGroup (Just "P")
                vSexEduP <- SB.inBlock SB.SBModel $ SB.vectorizeVar sexEduP voteData
                let logitT_sample d = SB.multiOp "+" $ d :| [gRaceT, gSexT, gEduT, SB.useVar vSexEduT]
                    logitT_ps d = SB.multiOp "+" $ d :| [gRaceT, gSexT, gEduT, SB.useVar sexEduT]
                    logitP_sample d = SB.multiOp "+" $ d :| [gRaceP, gSexP, gEduP, SB.useVar vSexEduP]
                    logitP_ps d = SB.multiOp "+" $ d :| [gRaceP, gSexP, gEduP, SB.useVar sexEduP]
                return (logitT_sample, logitT_ps, logitP_sample, logitP_ps, Set.fromList ["Sex", "Race", "Education"])
              PlusRaceEdu -> do
                let groups = MRP.addGroupForInteractions raceGroup
                             $ MRP.addGroupForInteractions educationGroup mempty
                let hierGM s = SB.hierarchicalCenteredFixedMeanNormal 0 ("sigmaRaceEdu" <> s) sigmaPrior SB.STZNone
                raceEduT <- MRP.addInteractions voteData (hierGM "T") groups 2 (Just "T")
                vRaceEduT <- traverse (\x -> SB.inBlock SB.SBModel $ SB.vectorizeVar x voteData) raceEduT
                raceEduP <- MRP.addInteractions voteData (hierGM "P") groups 2 (Just "P")
                vRaceEduP <- traverse (\x -> SB.inBlock SB.SBModel $ SB.vectorizeVar x voteData) raceEduP
                let logitT_sample d = SB.multiOp "+" $ d :| ([gRaceT, gSexT, gEduT] ++ fmap SB.useVar vRaceEduT)
                    logitT_ps d = SB.multiOp "+" $ d :| ([gRaceT, gSexT, gEduT] ++ fmap SB.useVar raceEduT)
                    logitP_sample d = SB.multiOp "+" $ d :| ([gRaceP, gSexP, gEduP] ++ fmap SB.useVar vRaceEduP)
                    logitP_ps d = SB.multiOp "+" $ d :| ([gRaceP, gSexP, gEduP] ++ fmap SB.useVar raceEduP)
                return (logitT_sample, logitT_ps, logitP_sample, logitP_ps, Set.fromList ["Sex", "Race", "Education"])
              PlusInteractions -> do
                let groups = MRP.addGroupForInteractions raceGroup
                             $ MRP.addGroupForInteractions sexGroup
                             $ MRP.addGroupForInteractions educationGroup mempty
                let hierGM s = SB.hierarchicalCenteredFixedMeanNormal 0 ("sigmaRaceSexEdu" <> s) sigmaPrior SB.STZNone
                interT <- MRP.addInteractions voteData (hierGM "T") groups 2 (Just "T")
                vInterT <- traverse (\x -> SB.inBlock SB.SBModel $ SB.vectorizeVar x voteData) interT
                interP <- MRP.addInteractions voteData (hierGM "P") groups 2 (Just "P")
                vInterP <- traverse (\x -> SB.inBlock SB.SBModel $ SB.vectorizeVar x voteData) interP
                let logitT_sample d = SB.multiOp "+" $ d :| ([gRaceT, gSexT, gEduT] ++ fmap SB.useVar vInterT)
                    logitT_ps d = SB.multiOp "+" $ d :| ([gRaceT, gSexT, gEduT] ++ fmap SB.useVar interT)
                    logitP_sample d = SB.multiOp "+" $ d :| ([gRaceP, gSexP, gEduP] ++ fmap SB.useVar vInterP)
                    logitP_ps d = SB.multiOp "+" $ d :| ([gRaceP, gSexP, gEduP] ++ fmap SB.useVar interP)
                return (logitT_sample, logitT_ps, logitP_sample, logitP_ps, Set.fromList ["Sex", "Race", "Education"])
              PlusStateAndStateRace -> do
                gStateT <- MRP.addGroup voteData binaryPrior (groupModelMR stateGroup "T") stateGroup (Just "T")
                gStateP <- MRP.addGroup voteData binaryPrior (groupModelMR stateGroup "P") stateGroup (Just "P")
                let groups = MRP.addGroupForInteractions stateGroup
                             $ MRP.addGroupForInteractions raceGroup mempty
                let hierGM s = SB.hierarchicalCenteredFixedMeanNormal 0 ("sigmaStateRaceEdu" <> s) sigmaPrior SB.STZNone
                interT <- MRP.addInteractions voteData (hierGM "T") groups 2 (Just "T")
                vInterT <- traverse (\x -> SB.inBlock SB.SBModel $ SB.vectorizeVar x voteData) interT
                interP <- MRP.addInteractions voteData (hierGM "P") groups 2 (Just "P")
                vInterP <- traverse (\x -> SB.inBlock SB.SBModel $ SB.vectorizeVar x voteData) interP
                let logitT_sample d = SB.multiOp "+" $ d :| ([gRaceT, gSexT, gEduT, gStateT] ++ fmap SB.useVar vInterT)
                    logitT_ps d = SB.multiOp "+" $ d :| ([gRaceT, gSexT, gEduT, gStateT] ++ fmap SB.useVar interT)
                    logitP_sample d = SB.multiOp "+" $ d :| ([gRaceP, gSexP, gEduP, gStateP] ++ fmap SB.useVar vInterP)
                    logitP_ps d = SB.multiOp "+" $ d :| ([gRaceP, gSexP, gEduP, gStateP] ++ fmap SB.useVar interP)
                return (logitT_sample, logitT_ps, logitP_sample, logitP_ps, Set.fromList ["Sex", "Race", "Education", "State"])
              PlusStateAndStateInteractions -> do
                gStateT <- MRP.addGroup voteData binaryPrior (groupModelMR stateGroup "T") stateGroup (Just "T")
                gStateP <- MRP.addGroup voteData binaryPrior (groupModelMR stateGroup "P") stateGroup (Just "P")
                let iGroupRace = MRP.addGroupForInteractions stateGroup
                                 $ MRP.addGroupForInteractions raceGroup mempty
                    iGroupSex = MRP.addGroupForInteractions stateGroup
                                $ MRP.addGroupForInteractions sexGroup mempty
                    iGroupEdu = MRP.addGroupForInteractions stateGroup
                                $ MRP.addGroupForInteractions educationGroup mempty
                let hierGM s = SB.hierarchicalCenteredFixedMeanNormal 0 ("sigmaStateRaceEdu" <> s) sigmaPrior SB.STZNone
                stateRaceT <- MRP.addInteractions voteData (hierGM "T") iGroupRace 2 (Just "T")
                vStateRaceT <- traverse (\x -> SB.inBlock SB.SBModel $ SB.vectorizeVar x voteData) stateRaceT
                stateRaceP <- MRP.addInteractions voteData (hierGM "P") iGroupRace 2 (Just "P")
                vStateRaceP <- traverse (\x -> SB.inBlock SB.SBModel $ SB.vectorizeVar x voteData) stateRaceP
                stateSexT <- MRP.addInteractions voteData (hierGM "T") iGroupSex 2 (Just "T")
                vStateSexT <- traverse (\x -> SB.inBlock SB.SBModel $ SB.vectorizeVar x voteData) stateSexT
                stateSexP <- MRP.addInteractions voteData (hierGM "P") iGroupSex 2 (Just "P")
                vStateSexP <- traverse (\x -> SB.inBlock SB.SBModel $ SB.vectorizeVar x voteData) stateSexP
                stateEduT <- MRP.addInteractions voteData (hierGM "T") iGroupEdu 2 (Just "T")
                vStateEduT <- traverse (\x -> SB.inBlock SB.SBModel $ SB.vectorizeVar x voteData) stateEduT
                stateEduP <- MRP.addInteractions voteData (hierGM "P") iGroupEdu 2 (Just "P")
                vStateEduP <- traverse (\x -> SB.inBlock SB.SBModel $ SB.vectorizeVar x voteData) stateEduP
                let logitT_sample d = SB.multiOp "+" $ d :| ([gRaceT, gSexT, gEduT, gStateT]
                                                              ++ fmap SB.useVar vStateRaceT
                                                              ++ fmap SB.useVar vStateSexT
                                                              ++ fmap SB.useVar vStateEduT
                                                            )
                    logitT_ps d = SB.multiOp "+" $ d :| ([gRaceT, gSexT, gEduT, gStateT]
                                                          ++ fmap SB.useVar stateRaceT
                                                          ++ fmap SB.useVar stateSexT
                                                          ++ fmap SB.useVar stateEduT
                                                        )
                    logitP_sample d = SB.multiOp "+" $ d :| ([gRaceP, gSexP, gEduP, gStateP]
                                                              ++ fmap SB.useVar vStateRaceP
                                                              ++ fmap SB.useVar vStateSexP
                                                              ++ fmap SB.useVar vStateEduP
                                                            )
                    logitP_ps d = SB.multiOp "+" $ d :| ([gRaceP, gSexP, gEduP, gStateP]
                                                          ++ fmap SB.useVar stateRaceP
                                                          ++ fmap SB.useVar stateSexP
                                                          ++ fmap SB.useVar stateEduP
                                                        )
                return (logitT_sample, logitT_ps, logitP_sample, logitP_ps, Set.fromList ["Sex", "Race", "Education", "State"])


        SB.sampleDistV voteData distT (logitT_sample feCDT)
        SB.sampleDistV voteData distP (logitP_sample feCDP)


--        sldData <- SB.addDataSet "SLD_Demographics" (SB.ToFoldable sldTables)
--        SB.addDataSetIndexes sldData voteData sldPSGroupRowMap
        sldData <- SB.dataSetTag @(F.Record SLDDemographicsR) "SLD_Demographics"
--        SB.duplicateDataSetBindings sldData [("SLD_Race","Race"), ("SLD_Education", "Education"), ("SLD_Sex", "Sex"),("SLD_State", "State")]
        let getDensity = F.rgetField @DT.PopPerSqMile
        mv <- SB.add2dMatrixData sldData "Density" 1 (Just 0) Nothing densityPredictor --(Vector.singleton . getDensity)
        (SB.StanVar cmn _) <- centerF mv
        let cmE = (SB.indexBy (SB.name cmn) "SLD_Demographics")
            densityTE = cmE `SB.times` betaT
            densityPE = cmE `SB.times` betaP
            psExprF ik = do
              pT <- SB.stanDeclareRHS "pT" SB.StanReal "" $ SB.familyExp distT ik $ logitT_ps densityTE
              pD <- SB.stanDeclareRHS "pD" SB.StanReal "" $ SB.familyExp distP ik $ logitP_ps densityPE
              --return $ SB.useVar pT `SB.times` SB.paren ((SB.scalar "2" `SB.times` SB.useVar pD) `SB.minus` SB.scalar "1")
              return $ SB.useVar pT `SB.times` SB.useVar pD
--            pTExprF ik = SB.stanDeclareRHS "pT" SB.StanReal "" $ SB.familyExp distT ik $ logitT_ps densityTE
--            pDExprF ik = SB.stanDeclareRHS "pD" SB.StanReal "" $ SB.familyExp distP ik $ logitP_ps densityPE

        let postStratBySLD =
              MRP.addPostStratification @SLDModelData
              psExprF
              Nothing
              voteData
              sldData
              sldPSGroupSet
              (realToFrac . F.rgetField @BRC.Count)
              (MRP.PSShare $ Just $ SB.name "pT")
              (Just sldGroup)
        postStratBySLD

{-
  -- raw probabilities
        SB.inBlock SB.SBGeneratedQuantities $ do
          let pArrayDims =
                [ SB.NamedDim $ SB.taggedGroupName sexGroup,
                  SB.NamedDim $ SB.taggedGroupName educationGroup,
                  SB.NamedDim $ SB.taggedGroupName raceGroup
                ]
              pTRHS = SB.familyExp distT "" $ logitT_ps densityTE
              pDRHS = SB.familyExp distT "" $ logitP_ps densityPE
          pTVar <- SB.stanDeclare "probT" (SB.StanArray pArrayDims SB.StanReal) "<lower=0, upper=1>"
          pDVar <- SB.stanDeclare "probD" (SB.StanArray pArrayDims SB.StanReal) "<lower=0, upper=1>"
          SB.useDataSetForBindings sldData $ do
            SB.stanForLoopB "n" Nothing "SLD_Demographics" $ do
              SB.addExprLine "ProbsT" $ SB.useVar pTVar `SB.eq` pTRHS
              SB.addExprLine "ProbsD" $ SB.useVar pDVar `SB.eq` pDRHS
-}
        SB.generateLogLikelihood' voteData ((distT, logitT_ps feCDT) :| [(distP, logitP_ps feCDP)])


        return ()

      addModelIdAndYear :: F.Record [BR.StateAbbreviation, ET.DistrictTypeC, ET.DistrictNumber, ModeledShare]
                        -> F.Record [BR.Year,BR.StateAbbreviation, ET.DistrictTypeC, ET.DistrictNumber, MT.ModelId Model, ModeledShare]
      addModelIdAndYear r = F.rcast $ FT.recordSingleton @BR.Year datYear F.<+> FT.recordSingleton @(MT.ModelId Model) model F.<+> r
      extractResults :: K.KnitEffects r
                     => SC.ResultAction r d SB.DataSetGroupIntMaps () (FS.SFrameRec ModelResultsR)
      extractResults = SC.UseSummary f where
        f summary _ aAndEb_C = do
          let eb_C = fmap snd aAndEb_C
          eb <- K.ignoreCacheTime eb_C
          resultsMap <- K.knitEither $ do
            groupIndexes <- eb
            psIndexIM <- SB.getGroupIndex
              (SB.RowTypeTag @(F.Record SLDDemographicsR) "SLD_Demographics")
              sldGroup
              groupIndexes
            let parseAndIndexPctsWith idx g vn = do
                  v <- SP.getVector . fmap CS.percents <$> SP.parse1D vn (CS.paramStats summary)
                  indexStanResults idx $ Vector.map g v
            parseAndIndexPctsWith psIndexIM id "PS_SLD_Demographics_SLD"
          res :: F.FrameRec [BR.StateAbbreviation, ET.DistrictTypeC, ET.DistrictNumber, ModeledShare] <- K.knitEither
                                                                                                      $ MT.keyedCIsToFrame sldLocationToRec
                                                                                                      $ M.toList resultsMap
          return $ FS.SFrame . fmap addModelIdAndYear $ res
--      dataWranglerAndCode :: K.ActionWithCacheTime r SLDModelData
--                          -> K.Sem r (SC.DataWrangler SLDModelData SB.DataSetGroupIntMaps (), SB.StanCode)
      dataWranglerAndCode data_C = do
        dat <-  K.ignoreCacheTime data_C
        K.logLE K.Info
          $ "Voter data (CCES) has "
          <> show (FL.fold FL.length $ ccesRows dat)
          <> " rows."
--        BR.logFrame $ sldTables dat
--        BR.logFrame $ districtRows dat
        let (districts, states) = FL.fold
                                  ((,)
                                   <$> (FL.premap districtKey FL.list)
                                   <*> (FL.premap (F.rgetField @BR.StateAbbreviation) FL.list)
                                  )
                                  $ districtRows dat
            slds = FL.fold (FL.premap sldKey FL.list) $ sldTables dat
            groups = groupBuilder districts states slds
--            builderText = SB.dumpBuilderState $ SB.runStanGroupBuilder groups dat
--        K.logLE K.Diagnostic $ "Initial Builder: " <> builderText
        K.logLE K.Info $ show $ zip [1..] $ Set.toList $ FL.fold FL.set states
        K.knitEither $ MRP.buildDataWranglerAndCode groups () dataAndCodeBuilder dat
  (dw, stanCode) <- dataWranglerAndCode dat_C
  fmap (fmap FS.unSFrame)
    $ MRP.runMRPModel
    clearCaches
    (Just modelDir)
    ("sld_" <> show model)
    jsonDataName
    dw
    stanCode
    "DVOTES_C"
    extractResults
    dat_C
    (Just 1000)
    (Just 0.8)
    (Just 15)


type SLDLocation = (Text, ET.DistrictType, Int)

sldLocationToRec :: SLDLocation -> F.Record [BR.StateAbbreviation, ET.DistrictTypeC, ET.DistrictNumber]
sldLocationToRec (sa, dt, dn) = sa F.&: dt F.&: dn F.&: V.RNil

type ModeledShare = "ModeledShare" F.:-> MT.ConfidenceInterval

type ModelResultsR = [BR.Year, BR.StateAbbreviation, ET.DistrictTypeC, ET.DistrictNumber, MT.ModelId Model, ModeledShare]


sldGroup :: SB.GroupTypeTag SLDLocation
sldGroup = SB.GroupTypeTag "SLD"

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

raceGroup :: SB.GroupTypeTag DT.Race5
raceGroup = SB.GroupTypeTag "Race"

hispanicGroup :: SB.GroupTypeTag DT.Hisp
hispanicGroup = SB.GroupTypeTag "Hispanic"

wnhGroup :: SB.GroupTypeTag Bool
wnhGroup = SB.GroupTypeTag "WNH"

wngGroup :: SB.GroupTypeTag Bool
wngGroup = SB.GroupTypeTag "WNG"

race5FromCPS :: F.Record BRE.CPSVByCDR -> DT.Race5
race5FromCPS r =
  let race4A = F.rgetField @DT.RaceAlone4C r
      hisp = F.rgetField @DT.HispC r
  in DT.race5FromRaceAlone4AndHisp True race4A hisp

race5FromCensus :: F.Record BRE.CPSVByCDR -> DT.Race5
race5FromCensus r =
  let race4A = F.rgetField @DT.RaceAlone4C r
      hisp = F.rgetField @DT.HispC r
  in DT.race5FromRaceAlone4AndHisp True race4A hisp

--sldKey r = F.rgetField @BR.StateAbbreviation r <> "-" <> show (F.rgetField @ET.DistrictTypeC r) <> "-" <> show (F.rgetField @ET.DistrictNumber r)
sldKey :: (F.ElemOf rs BR.StateAbbreviation
          ,F.ElemOf rs ET.DistrictTypeC
          ,F.ElemOf rs ET.DistrictNumber)
       => F.Record rs -> SLDLocation
sldKey r = (F.rgetField @BR.StateAbbreviation r
           , F.rgetField @ET.DistrictTypeC r
           , F.rgetField @ET.DistrictNumber r
           )
districtKey r = F.rgetField @BR.StateAbbreviation r <> "-" <> show (F.rgetField @BR.CongressionalDistrict r)
wnh r = (F.rgetField @DT.RaceAlone4C r == DT.RA4_White) && (F.rgetField @DT.HispC r == DT.NonHispanic)
wnhNonGrad r = wnh r && (F.rgetField @DT.CollegeGradC r == DT.NonGrad)
wnhCCES r = (F.rgetField @DT.Race5C r == DT.R5_WhiteNonLatinx) && (F.rgetField @DT.HispC r == DT.NonHispanic)
wnhNonGradCCES r = wnhCCES r && (F.rgetField @DT.CollegeGradC r == DT.NonGrad)
densityPredictor r = Vector.fromList $ [Numeric.log (F.rgetField @DT.PopPerSqMile r)]
raceAlone4FromRace5 :: DT.Race5 -> DT.RaceAlone4
raceAlone4FromRace5 DT.R5_Other = DT.RA4_Other
raceAlone4FromRace5 DT.R5_Black = DT.RA4_Black
raceAlone4FromRace5 DT.R5_Latinx = DT.RA4_Other
raceAlone4FromRace5 DT.R5_Asian = DT.RA4_Asian
raceAlone4FromRace5 DT.R5_WhiteNonLatinx = DT.RA4_White

indexStanResults :: (Show k, Ord k)
                 => IM.IntMap k
                 -> Vector.Vector a
                 -> Either Text (Map k a)
indexStanResults im v = do
  when (IM.size im /= Vector.length v)
    $ Left $
    "Mismatched sizes in indexStanResults. Result vector has " <> show (Vector.length v) <> " result and IntMap = " <> show im
  return $ M.fromList $ zip (IM.elems im) (Vector.toList v)




modelResultScatterChart :: Bool
                        -> Text
                        -> FV.ViewConfig
                        -> F.FrameRec ([BR.Year, BR.StateAbbreviation, ET.DistrictNumber, BR.Contested, MT.ModelId Model, ModeledShare, BR.DShare])
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



-- UGH. Old version.  Using CPS.  Which we can't do at CD level. UGH.
{-
groupBuilder :: [Text]
             -> [Text]
             -> [SLDLocation]
             -> SB.StanGroupBuilderM SLDModelData ()
groupBuilder districts states slds = do
  voterData <- SB.addDataSetToGroupBuilder "VData" (SB.ToFoldable cpsVAndccesRows)
  SB.addGroupIndexForDataSet cdGroup voterData $ SB.makeIndexFromFoldable show districtKey districts
  SB.addGroupIndexForDataSet stateGroup voterData $ SB.makeIndexFromFoldable show (F.rgetField @BR.StateAbbreviation) states
--  SB.addGroupIndexForDataSet ageGroup voterData $ SB.makeIndexFromEnum (F.rgetField @DT.SimpleAgeC)
  SB.addGroupIndexForDataSet sexGroup voterData $ SB.makeIndexFromEnum (F.rgetField @DT.SexC)
  SB.addGroupIndexForDataSet educationGroup voterData $ SB.makeIndexFromEnum (F.rgetField @DT.CollegeGradC)
  SB.addGroupIndexForDataSet raceGroup voterData $ SB.makeIndexFromEnum (F.rgetField @DT.Race5C)
  SB.addGroupIndexForDataSet hispanicGroup voterData $ SB.makeIndexFromEnum (F.rgetField @DT.HispC)
  cdData <- SB.addDataSetToGroupBuilder "CDData" (SB.ToFoldable districtRows)
  SB.addGroupIndexForCrosswalk cdData $ SB.makeIndexFromFoldable show districtKey districts
  sldData <- SB.addDataSetToGroupBuilder "SLD_Demographics" (SB.ToFoldable sldTables)
  SB.addGroupIndexForDataSet sldStateGroup sldData $ SB.makeIndexFromFoldable show (F.rgetField @BR.StateAbbreviation) states
  SB.addGroupIndexForDataSet sldEducationGroup sldData $ SB.makeIndexFromEnum (F.rgetField @DT.CollegeGradC)
  SB.addGroupIndexForDataSet sldSexGroup sldData $ SB.makeIndexFromEnum (F.rgetField @DT.SexC)
  SB.addGroupIndexForDataSet sldRaceGroup sldData $ SB.makeIndexFromEnum race5
  SB.addGroupIndexForDataSet sldGroup sldData $ SB.makeIndexFromFoldable show sldKey slds
  return ()


sldPSGroupRowMap :: SB.GroupRowMap (F.Record SLDDemographicsR)
sldPSGroupRowMap = SB.addRowMap sexGroup (F.rgetField @DT.SexC)
                   $ SB.addRowMap educationGroup (F.rgetField @DT.CollegeGradC)
                   $ SB.addRowMap raceGroup race5
                   $ SB.addRowMap stateGroup (F.rgetField @BR.StateAbbreviation)
                   $ SB.emptyGroupRowMap


data Model = Base
           | PlusState
           | PlusSexEdu
           | PlusRaceEdu
           | PlusInteractions
           | PlusStateAndStateRace
           | PlusStateAndStateInteractions
           deriving (Show, Eq, Ord, Generic)

instance Flat.Flat Model
type instance FI.VectorFor Model = Vector.Vector

data TurnoutSource = CPS_Turnout | CES_Turnout deriving (Show, Eq, Ord)

stateLegModel :: (K.KnitEffects r, BR.CacheEffects r)
              => Bool
              -> Model
              -> Text
              -> TurnoutSource
              -> K.ActionWithCacheTime r SLDModelData
              -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec ModelResultsR))
stateLegModel clearCaches model modelNameExtra tSource dat_C = K.wrapPrefix "stateLegModel" $ do
  K.logLE K.Info $ "(Re-)running state-leg model if necessary."
  let modelDir = "br-2021-StateLegModel/stan/"
      jsonDataName = "stateLeg_ASR_" <> show model <> "_" <> modelNameExtra <> "_" <> show tSource
      dataAndCodeBuilder :: MRP.BuilderM SLDModelData ()
      dataAndCodeBuilder = do
        -- data
        voteData <- SB.dataSetTag @(F.Record CPSAndCCESR) "VData"
        cdData <- SB.dataSetTag @(F.Record BRE.DistrictDemDataR) "CDData"
        SB.addDataSetsCrosswalk voteData cdData cdGroup
        SB.setDataSetForBindings voteData

        centerF <- MRP.addFixedEffectsData cdData (MRP.FixedEffects 1 densityPredictor)

        let normal x = SB.normal Nothing $ SB.scalar $ show x
            binaryPrior = normal 2
            sigmaPrior = normal 2
            fePrior = normal 2

        let simpleGroupModel x = SB.NonHierarchical SB.STZNone (normal x)
            muH gn s = "mu" <> s <> "_" <> gn
            sigmaH gn s = "sigma" <> s <> "_" <> gn
            hierHPs gn s = M.fromList [(muH gn s, ("", SB.stdNormal)), (sigmaH gn s, ("<lower=0>", SB.normal (Just $ SB.scalar "0") (SB.scalar "2")))]
            hierGroupModel' gn s = SB.Hierarchical SB.STZNone (hierHPs gn s) (SB.Centered $ SB.normal (Just $ SB.name $ muH gn s) (SB.name $ sigmaH gn s))
            hierGroupModel gtt s = hierGroupModel' (SB.taggedGroupName gtt) s
--              let gn = SB.taggedGroupName gtt
--              in SB.Hierarchical SB.STZNone (hierHPs gn s) (SB.Centered $ SB.normal (Just $ SB.name $ muH gn s) (SB.name $ sigmaH gn s))
            gmSigmaName gtt suffix = "sigma" <> suffix <> "_" <> SB.taggedGroupName gtt
            groupModelMR gtt s = SB.hierarchicalCenteredFixedMeanNormal 0 (gmSigmaName gtt s) sigmaPrior SB.STZNone
        -- Turnout
        (cvap, votes) <- case tSource of
          CPS_Turnout -> do
            cvapCPS <- SB.addCountData voteData "CVAP" (F.rgetField @CPSCVAP)
            votesCPS <- SB.addCountData voteData "VOTED" (F.rgetField @CPSVoters)
            return (cvapCPS, votesCPS)
          CES_Turnout -> do
            cvapCES <- SB.addCountData voteData "CVAP" (F.rgetField @CCESSurveyed)
            votesCES <- SB.addCountData voteData "VOTED" (F.rgetField @CCESVoters)
            return (cvapCES, votesCES)

        (feCDT, betaT) <- MRP.addFixedEffectsParametersAndPriors
                          True
                          fePrior
                          cdData
                          voteData
                          (Just "T")

        gSexT <- MRP.addGroup voteData binaryPrior (simpleGroupModel 1) sexGroup (Just "T")
        gEduT <- MRP.addGroup voteData binaryPrior (simpleGroupModel 1) educationGroup (Just "T")
        gRaceT <- MRP.addGroup voteData binaryPrior (hierGroupModel raceGroup "T") raceGroup (Just "T")
--        gStateT <- MRP.addGroup voteData binaryPrior (hierGroupModel stateGroup) stateGroup (Just "T")
        let distT = SB.binomialLogitDist votes cvap

        -- Preference
        ccesVotes <- SB.addCountData voteData "VOTED_C" (F.rgetField @CCESVoters)
        ccesDVotes <- SB.addCountData voteData "DVOTES_C" (F.rgetField @CCESDVotes)
        (feCDP, betaP) <- MRP.addFixedEffectsParametersAndPriors
                          True
                          fePrior
                          cdData
                          voteData
                          (Just "P")

        gSexP <- MRP.addGroup voteData binaryPrior (simpleGroupModel 1) sexGroup (Just "P")
        gEduP <- MRP.addGroup voteData binaryPrior (simpleGroupModel 1) educationGroup (Just "P")
        gRaceP <- MRP.addGroup voteData binaryPrior (hierGroupModel raceGroup "P") raceGroup (Just "P")
        let distP = SB.binomialLogitDist ccesDVotes ccesVotes


        (logitT_sample, logitT_ps, logitP_sample, logitP_ps, psGroupSet) <- case model of
              Base -> return (\d -> SB.multiOp "+" $ d :| [gRaceT, gSexT, gEduT]
                             ,\d -> SB.multiOp "+" $ d :| [gRaceT, gSexT, gEduT]
                             ,\d -> SB.multiOp "+" $ d :| [gRaceP, gSexP, gEduP]
                             ,\d -> SB.multiOp "+" $ d :| [gRaceP, gSexP, gEduP]
                             , Set.fromList ["Sex", "Race", "Education"]
                             )
              PlusState -> do
                gStateT <- MRP.addGroup voteData binaryPrior (groupModelMR stateGroup "T") stateGroup (Just "T")
                gStateP <- MRP.addGroup voteData binaryPrior (groupModelMR stateGroup "P") stateGroup (Just "P")
                let logitT d = SB.multiOp "+" $ d :| [gRaceT, gSexT, gEduT, gStateT]
                    logitP d = SB.multiOp "+" $ d :| [gRaceP, gSexP, gEduP, gStateP]
                return (logitT, logitT, logitP, logitP, Set.fromList ["Sex", "Race", "Education", "State"])
              PlusSexEdu -> do
                let hierGM s = SB.hierarchicalCenteredFixedMeanNormal 0 ("sigmaSexEdu" <> s) sigmaPrior SB.STZNone
                sexEduT <- MRP.addInteractions2 voteData (hierGM "T") sexGroup educationGroup (Just "T")
                vSexEduT <- SB.inBlock SB.SBModel $ SB.vectorizeVar sexEduT voteData
                sexEduP <- MRP.addInteractions2 voteData (hierGM "P") sexGroup educationGroup (Just "P")
                vSexEduP <- SB.inBlock SB.SBModel $ SB.vectorizeVar sexEduP voteData
                let logitT_sample d = SB.multiOp "+" $ d :| [gRaceT, gSexT, gEduT, SB.useVar vSexEduT]
                    logitT_ps d = SB.multiOp "+" $ d :| [gRaceT, gSexT, gEduT, SB.useVar sexEduT]
                    logitP_sample d = SB.multiOp "+" $ d :| [gRaceP, gSexP, gEduP, SB.useVar vSexEduP]
                    logitP_ps d = SB.multiOp "+" $ d :| [gRaceP, gSexP, gEduP, SB.useVar sexEduP]
                return (logitT_sample, logitT_ps, logitP_sample, logitP_ps, Set.fromList ["Sex", "Race", "Education"])
              PlusRaceEdu -> do
                let groups = MRP.addGroupForInteractions raceGroup
                             $ MRP.addGroupForInteractions educationGroup mempty
                let hierGM s = SB.hierarchicalCenteredFixedMeanNormal 0 ("sigmaRaceEdu" <> s) sigmaPrior SB.STZNone
                raceEduT <- MRP.addInteractions voteData (hierGM "T") groups 2 (Just "T")
                vRaceEduT <- traverse (\x -> SB.inBlock SB.SBModel $ SB.vectorizeVar x voteData) raceEduT
                raceEduP <- MRP.addInteractions voteData (hierGM "P") groups 2 (Just "P")
                vRaceEduP <- traverse (\x -> SB.inBlock SB.SBModel $ SB.vectorizeVar x voteData) raceEduP
                let logitT_sample d = SB.multiOp "+" $ d :| ([gRaceT, gSexT, gEduT] ++ fmap SB.useVar vRaceEduT)
                    logitT_ps d = SB.multiOp "+" $ d :| ([gRaceT, gSexT, gEduT] ++ fmap SB.useVar raceEduT)
                    logitP_sample d = SB.multiOp "+" $ d :| ([gRaceP, gSexP, gEduP] ++ fmap SB.useVar vRaceEduP)
                    logitP_ps d = SB.multiOp "+" $ d :| ([gRaceP, gSexP, gEduP] ++ fmap SB.useVar raceEduP)
                return (logitT_sample, logitT_ps, logitP_sample, logitP_ps, Set.fromList ["Sex", "Race", "Education"])
              PlusInteractions -> do
                let groups = MRP.addGroupForInteractions raceGroup
                             $ MRP.addGroupForInteractions sexGroup
                             $ MRP.addGroupForInteractions educationGroup mempty
                let hierGM s = SB.hierarchicalCenteredFixedMeanNormal 0 ("sigmaRaceSexEdu" <> s) sigmaPrior SB.STZNone
                interT <- MRP.addInteractions voteData (hierGM "T") groups 2 (Just "T")
                vInterT <- traverse (\x -> SB.inBlock SB.SBModel $ SB.vectorizeVar x voteData) interT
                interP <- MRP.addInteractions voteData (hierGM "P") groups 2 (Just "P")
                vInterP <- traverse (\x -> SB.inBlock SB.SBModel $ SB.vectorizeVar x voteData) interP
                let logitT_sample d = SB.multiOp "+" $ d :| ([gRaceT, gSexT, gEduT] ++ fmap SB.useVar vInterT)
                    logitT_ps d = SB.multiOp "+" $ d :| ([gRaceT, gSexT, gEduT] ++ fmap SB.useVar interT)
                    logitP_sample d = SB.multiOp "+" $ d :| ([gRaceP, gSexP, gEduP] ++ fmap SB.useVar vInterP)
                    logitP_ps d = SB.multiOp "+" $ d :| ([gRaceP, gSexP, gEduP] ++ fmap SB.useVar interP)
                return (logitT_sample, logitT_ps, logitP_sample, logitP_ps, Set.fromList ["Sex", "Race", "Education"])
              PlusStateAndStateRace -> do
                gStateT <- MRP.addGroup voteData binaryPrior (groupModelMR stateGroup "T") stateGroup (Just "T")
                gStateP <- MRP.addGroup voteData binaryPrior (groupModelMR stateGroup "P") stateGroup (Just "P")
                let groups = MRP.addGroupForInteractions stateGroup
                             $ MRP.addGroupForInteractions raceGroup mempty
                let hierGM s = SB.hierarchicalCenteredFixedMeanNormal 0 ("sigmaStateRaceEdu" <> s) sigmaPrior SB.STZNone
                interT <- MRP.addInteractions voteData (hierGM "T") groups 2 (Just "T")
                vInterT <- traverse (\x -> SB.inBlock SB.SBModel $ SB.vectorizeVar x voteData) interT
                interP <- MRP.addInteractions voteData (hierGM "P") groups 2 (Just "P")
                vInterP <- traverse (\x -> SB.inBlock SB.SBModel $ SB.vectorizeVar x voteData) interP
                let logitT_sample d = SB.multiOp "+" $ d :| ([gRaceT, gSexT, gEduT, gStateT] ++ fmap SB.useVar vInterT)
                    logitT_ps d = SB.multiOp "+" $ d :| ([gRaceT, gSexT, gEduT, gStateT] ++ fmap SB.useVar interT)
                    logitP_sample d = SB.multiOp "+" $ d :| ([gRaceP, gSexP, gEduP, gStateP] ++ fmap SB.useVar vInterP)
                    logitP_ps d = SB.multiOp "+" $ d :| ([gRaceP, gSexP, gEduP, gStateP] ++ fmap SB.useVar interP)
                return (logitT_sample, logitT_ps, logitP_sample, logitP_ps, Set.fromList ["Sex", "Race", "Education", "State"])
              PlusStateAndStateInteractions -> do
                gStateT <- MRP.addGroup voteData binaryPrior (groupModelMR stateGroup "T") stateGroup (Just "T")
                gStateP <- MRP.addGroup voteData binaryPrior (groupModelMR stateGroup "P") stateGroup (Just "P")
                let iGroupRace = MRP.addGroupForInteractions stateGroup
                                 $ MRP.addGroupForInteractions raceGroup mempty
                    iGroupSex = MRP.addGroupForInteractions stateGroup
                                $ MRP.addGroupForInteractions sexGroup mempty
                    iGroupEdu = MRP.addGroupForInteractions stateGroup
                                $ MRP.addGroupForInteractions educationGroup mempty
                let hierGM s = SB.hierarchicalCenteredFixedMeanNormal 0 ("sigmaStateRaceEdu" <> s) sigmaPrior SB.STZNone
                stateRaceT <- MRP.addInteractions voteData (hierGM "T") iGroupRace 2 (Just "T")
                vStateRaceT <- traverse (\x -> SB.inBlock SB.SBModel $ SB.vectorizeVar x voteData) stateRaceT
                stateRaceP <- MRP.addInteractions voteData (hierGM "P") iGroupRace 2 (Just "P")
                vStateRaceP <- traverse (\x -> SB.inBlock SB.SBModel $ SB.vectorizeVar x voteData) stateRaceP
                stateSexT <- MRP.addInteractions voteData (hierGM "T") iGroupSex 2 (Just "T")
                vStateSexT <- traverse (\x -> SB.inBlock SB.SBModel $ SB.vectorizeVar x voteData) stateSexT
                stateSexP <- MRP.addInteractions voteData (hierGM "P") iGroupSex 2 (Just "P")
                vStateSexP <- traverse (\x -> SB.inBlock SB.SBModel $ SB.vectorizeVar x voteData) stateSexP
                stateEduT <- MRP.addInteractions voteData (hierGM "T") iGroupEdu 2 (Just "T")
                vStateEduT <- traverse (\x -> SB.inBlock SB.SBModel $ SB.vectorizeVar x voteData) stateEduT
                stateEduP <- MRP.addInteractions voteData (hierGM "P") iGroupEdu 2 (Just "P")
                vStateEduP <- traverse (\x -> SB.inBlock SB.SBModel $ SB.vectorizeVar x voteData) stateEduP
                let logitT_sample d = SB.multiOp "+" $ d :| ([gRaceT, gSexT, gEduT, gStateT]
                                                              ++ fmap SB.useVar vStateRaceT
                                                              ++ fmap SB.useVar vStateSexT
                                                              ++ fmap SB.useVar vStateEduT
                                                            )
                    logitT_ps d = SB.multiOp "+" $ d :| ([gRaceT, gSexT, gEduT, gStateT]
                                                          ++ fmap SB.useVar stateRaceT
                                                          ++ fmap SB.useVar stateSexT
                                                          ++ fmap SB.useVar stateEduT
                                                        )
                    logitP_sample d = SB.multiOp "+" $ d :| ([gRaceP, gSexP, gEduP, gStateP]
                                                              ++ fmap SB.useVar vStateRaceP
                                                              ++ fmap SB.useVar vStateSexP
                                                              ++ fmap SB.useVar vStateEduP
                                                            )
                    logitP_ps d = SB.multiOp "+" $ d :| ([gRaceP, gSexP, gEduP, gStateP]
                                                          ++ fmap SB.useVar stateRaceP
                                                          ++ fmap SB.useVar stateSexP
                                                          ++ fmap SB.useVar stateEduP
                                                        )
                return (logitT_sample, logitT_ps, logitP_sample, logitP_ps, Set.fromList ["Sex", "Race", "Education", "State"])


        SB.sampleDistV voteData distT (logitT_sample feCDT)
        SB.sampleDistV voteData distP (logitP_sample feCDP)


--        sldData <- SB.addDataSet "SLD_Demographics" (SB.ToFoldable sldTables)
--        SB.addDataSetIndexes sldData voteData sldPSGroupRowMap
        sldData <- SB.dataSetTag @(F.Record SLDDemographicsR) "SLD_Demographics"
        SB.duplicateDataSetBindings sldData [("SLD_Race","Race"), ("SLD_Education", "Education"), ("SLD_Sex", "Sex"),("SLD_State", "State")]
        let getDensity = F.rgetField @DT.PopPerSqMile
        mv <- SB.add2dMatrixData sldData "Density" 1 (Just 0) Nothing densityPredictor --(Vector.singleton . getDensity)
        (SB.StanVar cmn _) <- centerF mv
        let cmE = (SB.indexBy (SB.name cmn) "SLD_Demographics")
            densityTE = cmE `SB.times` betaT
            densityPE = cmE `SB.times` betaP
            psExprF ik = do
              pT <- SB.stanDeclareRHS "pT" SB.StanReal "" $ SB.familyExp distT ik $ logitT_ps densityTE
              pD <- SB.stanDeclareRHS "pD" SB.StanReal "" $ SB.familyExp distP ik $ logitP_ps densityPE
              --return $ SB.useVar pT `SB.times` SB.paren ((SB.scalar "2" `SB.times` SB.useVar pD) `SB.minus` SB.scalar "1")
              return $ SB.useVar pT `SB.times` SB.useVar pD
--            pTExprF ik = SB.stanDeclareRHS "pT" SB.StanReal "" $ SB.familyExp distT ik $ logitT_ps densityTE
--            pDExprF ik = SB.stanDeclareRHS "pD" SB.StanReal "" $ SB.familyExp distP ik $ logitP_ps densityPE

        let postStratBySLD =
              MRP.addPostStratification @SLDModelData
              psExprF
              (Nothing)
              voteData
              sldData
              (SB.addRowMap sldGroup sldKey $ sldPSGroupRowMap)
              psGroupSet
              (realToFrac . F.rgetField @BRC.Count)
              (MRP.PSShare $ Just $ SB.name "pT")
              (Just sldGroup)
        postStratBySLD

{-
  -- raw probabilities
        SB.inBlock SB.SBGeneratedQuantities $ do
          let pArrayDims =
                [ SB.NamedDim $ SB.taggedGroupName sexGroup,
                  SB.NamedDim $ SB.taggedGroupName educationGroup,
                  SB.NamedDim $ SB.taggedGroupName raceGroup
                ]
              pTRHS = SB.familyExp distT "" $ logitT_ps densityTE
              pDRHS = SB.familyExp distT "" $ logitP_ps densityPE
          pTVar <- SB.stanDeclare "probT" (SB.StanArray pArrayDims SB.StanReal) "<lower=0, upper=1>"
          pDVar <- SB.stanDeclare "probD" (SB.StanArray pArrayDims SB.StanReal) "<lower=0, upper=1>"
          SB.useDataSetForBindings sldData $ do
            SB.stanForLoopB "n" Nothing "SLD_Demographics" $ do
              SB.addExprLine "ProbsT" $ SB.useVar pTVar `SB.eq` pTRHS
              SB.addExprLine "ProbsD" $ SB.useVar pDVar `SB.eq` pDRHS
-}
        SB.generateLogLikelihood' voteData ((distT, logitT_ps feCDT) :| [(distP, logitP_ps feCDP)])


        return ()

      addModelId :: F.Record [BR.StateAbbreviation, ET.DistrictTypeC, ET.DistrictNumber, ModeledShare]
                 -> F.Record [BR.StateAbbreviation, ET.DistrictTypeC, ET.DistrictNumber, MT.ModelId Model, ModeledShare]
      addModelId r = F.rcast $ FT.recordSingleton @(MT.ModelId Model) model F.<+> r
      extractResults :: K.KnitEffects r
                     => SC.ResultAction r d SB.DataSetGroupIntMaps () (FS.SFrameRec ModelResultsR)
      extractResults = SC.UseSummary f where
        f summary _ aAndEb_C = do
          let eb_C = fmap snd aAndEb_C
          eb <- K.ignoreCacheTime eb_C
          resultsMap <- K.knitEither $ do
            groupIndexes <- eb
            psIndexIM <- SB.getGroupIndex
              (SB.RowTypeTag @(F.Record SLDDemographicsR) "SLD_Demographics")
              sldGroup
              groupIndexes
            let parseAndIndexPctsWith idx g vn = do
                  v <- SP.getVector . fmap CS.percents <$> SP.parse1D vn (CS.paramStats summary)
                  indexStanResults idx $ Vector.map g v
            parseAndIndexPctsWith psIndexIM id "PS_SLD_Demographics_SLD"
          res :: F.FrameRec [BR.StateAbbreviation, ET.DistrictTypeC, ET.DistrictNumber, ModeledShare] <- K.knitEither
                                                                                                      $ MT.keyedCIsToFrame sldLocationToRec
                                                                                                      $ M.toList resultsMap
          return $ FS.SFrame . fmap addModelId $ res
--      dataWranglerAndCode :: K.ActionWithCacheTime r SLDModelData
--                          -> K.Sem r (SC.DataWrangler SLDModelData SB.DataSetGroupIntMaps (), SB.StanCode)
      dataWranglerAndCode data_C = do
        dat <-  K.ignoreCacheTime data_C
        K.logLE K.Info
          $ "Voter data (CPS and CCES) has "
          <> show (FL.fold FL.length $ cpsVAndccesRows dat)
          <> " rows."
--        BR.logFrame $ sldTables dat
--        BR.logFrame $ districtRows dat
        let (districts, states) = FL.fold
                                  ((,)
                                   <$> (FL.premap districtKey FL.list)
                                   <*> (FL.premap (F.rgetField @BR.StateAbbreviation) FL.list)
                                  )
                                  $ districtRows dat
            slds = FL.fold (FL.premap sldKey FL.list) $ sldTables dat
            groups = groupBuilder districts states slds
--            builderText = SB.dumpBuilderState $ SB.runStanGroupBuilder groups dat
--        K.logLE K.Diagnostic $ "Initial Builder: " <> builderText
        K.logLE K.Info $ show $ zip [1..] $ Set.toList $ FL.fold FL.set states
        K.knitEither $ MRP.buildDataWranglerAndCode groups () dataAndCodeBuilder dat
  (dw, stanCode) <- dataWranglerAndCode dat_C
  fmap (fmap FS.unSFrame)
    $ MRP.runMRPModel
    clearCaches
    (Just modelDir)
    ("sld_" <> show model <> "_" <> modelNameExtra)
    jsonDataName
    dw
    stanCode
    "DVOTES_C"
    extractResults
    dat_C
    (Just 1000)
    (Just 0.8)
    (Just 15)
-}

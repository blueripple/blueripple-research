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
import qualified BlueRipple.Data.ModelingTypes as MT
import qualified BlueRipple.Data.Loaders as BR
import qualified BlueRipple.Utilities.KnitUtils as BR
import qualified BlueRipple.Utilities.Heidi as BR
import qualified BlueRipple.Utilities.TableUtils as BR
import qualified BlueRipple.Model.House.ElectionResult as BRE
import qualified BlueRipple.Data.CensusLoaders as BRC
import qualified BlueRipple.Model.StanMRP as MRP
import qualified BlueRipple.Data.CountFolds as BRCF

import qualified Colonnade as C
import qualified Text.Blaze.Colonnade as C
import qualified Text.Blaze.Html5.Attributes   as BHA
import qualified Control.Foldl as FL
import qualified Control.Foldl.Statistics as FLS
import qualified Data.Aeson as A
import qualified Data.Aeson.Lens as A
import qualified Data.List as List
import qualified Data.IntMap as IM
import qualified Data.Map.Strict as M
import Data.String.Here (here, i)
import qualified Data.Set as Set
import qualified Data.Text as T
import qualified Text.Read  as T
import qualified Data.Time.Calendar            as Time
--import qualified Data.Time.Clock               as Time
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
import qualified Frames.Visualization.VegaLite.Data
                                               as FV

import Control.Lens.Operators

import qualified Heidi
--import Lens.Micro.Platform ((^?))

import qualified Graphics.Vega.VegaLite.Configuration as FV
import qualified Graphics.Vega.VegaLite.Heidi as HV

import qualified Knit.Report as K
import qualified Knit.Effect.AtomicCache as KC
import qualified Text.Pandoc.Error as Pandoc
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
import qualified Text.Pandoc as Pandoc
import qualified Debug.Trace as DT
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
    Left err -> putTextLn $ "Pandoc Error: " <> Pandoc.renderError err


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
type SLDRecodedR = BRC.SLDLocationR
                   V.++ BRC.ExtensiveDataR
                   V.++ [DT.SexC, DT.CollegeGradC, DT.RaceAlone4C, DT.HispC, BRC.Count, DT.PopPerSqMile]
type SLDDemographicsR = '[BR.StateAbbreviation] V.++ SLDRecodedR

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

type CPSAndCCESR = BRE.CDKeyR V.++ PredictorR V.++ VotingDataR --BRCF.CountCols V.++ [BRE.Surveyed, BRE.TVotes, BRE.DVotes]
data SLDModelData = SLDModelData
  {
    cpsVAndccesRows :: F.FrameRec CPSAndCCESR
  , sldTables :: F.FrameRec SLDDemographicsR
  , districtRows :: F.FrameRec BRE.DistrictDemDataR
  } deriving (Generic)

filterVotingDataByYear :: (Int -> Bool) -> SLDModelData -> SLDModelData
filterVotingDataByYear f (SLDModelData a b c) = SLDModelData (q a) b c where
  q = F.filterFrame (f . F.rgetField @BR.Year)


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
    let sldSER' = sldDemographicsRecode $ BRC.sexEducationRace sld
    -- add state abbreviations
        (sldSER, saMissing) = FJ.leftJoinWithMissing @'[BR.StateFips] sldSER'
                              $ fmap (F.rcast @[BR.StateFips, BR.StateAbbreviation] . FT.retypeColumn @BR.StateFIPS @BR.StateFips) stateAbbrs
    -- BR.logFrame sldSER
    return $ SLDModelData cpsAndCces (F.rcast <$> sldSER) distRows


type VotePct = "VotePct" F.:-> Text
type SLDResultR = [BR.Year, BR.StateAbbreviation, ET.DistrictTypeC, ET.DistrictNumber, BR.Candidate, ET.Party, ET.Votes, VotePct]

parseParty :: Text -> Maybe ET.PartyT
parseParty "Democrat" = Just ET.Democratic
parseParty "Democratic" = Just ET.Democratic
parseParty "Republican" = Just ET.Republican
parseParty _ = Just ET.Other

parseVARace :: Text -> Maybe (F.Record [BR.Year, BR.StateAbbreviation, ET.DistrictTypeC, ET.DistrictNumber])
parseVARace raceDesc = do
  let isLower = T.isInfixOf "Delegates" raceDesc
      dType = if isLower then ET.StateLower else ET.StateUpper
  let numText = T.dropWhileEnd (\x -> x /= ')') $  T.dropWhile (\x -> x /= '(') raceDesc
  dNum :: Int <- T.readMaybe $ toString numText
  return $ 2019 F.&: "VA" F.&: dType F.&: dNum F.&: V.RNil


getVAResults :: (K.KnitEffects r, BR.CacheEffects r) => FilePath -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec SLDRaceResultR))
getVAResults fp = do
  fileDep <-  K.fileDependency fp
--  BR.clearIfPresentD "data/SLD/VA_2019_General.bin"
  BR.retrieveOrMakeFrame "data/SLD/VA_2019_General.bin" fileDep $ \_ -> do
    mJSON :: Maybe A.Value <- K.liftKnit $ A.decodeFileStrict' fp
    races <- case mJSON of
      Nothing -> K.knitError "Error loading VA general json"
      Just json -> K.knitMaybe "Error Decoding VA General json" $ do
        racesA <- json ^? A.key "Races"
        let listOfRaces = racesA ^.. A.values
            g :: A.Value -> Maybe (F.Record [BR.Candidate, ET.Party, ET.Votes, VotePct])
            g cO = do
              name <- cO ^?  A.key "BallotName" . A._String
              party <- cO ^? A.key "PoliticalParty" . A._String >>= parseParty
              votes <- cO ^? A.key "Votes" . A._Integer
              pct <- cO ^? A.key "Percentage" . A._String
              return $ name F.&: party F.&: fromInteger votes F.&: pct F.&: V.RNil
            f raceO = do
              raceRec <- raceO ^? A.key "RaceName" . A._String >>= parseVARace
              candidatesA <- raceO ^? A.key "Candidates"
              let candidates = catMaybes $ g <$> (candidatesA ^.. A.values)
              return $ fmap (raceRec F.<+>) candidates
        races <- traverse f listOfRaces
        return $ FL.fold candidatesToRaces $ concat races
    return races

type DVotes = "DVotes" F.:-> Int
type RVotes = "RVotes" F.:-> Int
type DShare = "DShare" F.:-> Double

type SLDRaceResultR = [BR.Year, BR.StateAbbreviation, ET.DistrictTypeC, ET.DistrictNumber, DVotes, RVotes, DShare]

candidatesToRaces :: FL.Fold (F.Record SLDResultR) (F.FrameRec SLDRaceResultR)
candidatesToRaces = FMR.concatFold
                    $ FMR.mapReduceFold
                    FMR.noUnpack
                    (FMR.assignKeysAndData @[BR.Year, BR.StateAbbreviation, ET.DistrictTypeC, ET.DistrictNumber])
                    (FMR.foldAndAddKey f) where
  f :: FL.Fold (F.Record [BR.Candidate, ET.Party, ET.Votes, VotePct]) (F.Record [DVotes, RVotes, DShare])
  f =
    let party = F.rgetField @ET.Party
        votes = F.rgetField @ET.Votes
        dVotes = FL.prefilter ((== ET.Democratic) . party) $ FL.premap votes FL.sum
        rVotes = FL.prefilter ((== ET.Republican) . party) $ FL.premap votes FL.sum
    in (\d r -> d F.&: r F.&: realToFrac d/ realToFrac (d + r) F.&: V.RNil) <$> dVotes <*> rVotes


vaAnalysis :: forall r. (K.KnitMany r, BR.CacheEffects r) => K.Sem r ()
vaAnalysis = do
  K.logLE K.Info "Data prep..."
  data_C <- fmap (filterVotingDataByYear (==2018)) <$> prepSLDModelData False
  let va1PostInfo = BR.PostInfo BR.OnlineDraft (BR.PubTimes BR.Unpublished Nothing)
  va1Paths <- postPaths "VA1"
  BR.brNewPost va1Paths va1PostInfo "Virginia Lower House"
    $ vaLower False va1Paths va1PostInfo $ K.liftActionWithCacheTime data_C

--vaLowerColonnade :: BR.CellStyle (SLDLocation, [Double]) col -> K.Colonnade K.Headed (SLDLocation, [Double]) K.Cell
vaLowerColonnade cas =
  let state = F.rgetField @BR.StateAbbreviation
      dType = F.rgetField @ET.DistrictTypeC
      dNum = F.rgetField @ET.DistrictNumber
      share5 = MT.ciLower . F.rgetField @ModeledShare
      share50 = MT.ciMid . F.rgetField @ModeledShare
      share95 = MT.ciUpper . F.rgetField @ModeledShare
  in C.headed "State " (BR.toCell cas "State" "State" (BR.textToStyledHtml . state))
     <> C.headed "District" (BR.toCell cas "District" "District" (BR.numberToStyledHtml "%d" . dNum))
     <> C.headed "5%" (BR.toCell cas "5%" "5%" (BR.numberToStyledHtml "%2.2f" . (100*) . share5))
     <> C.headed "50%" (BR.toCell cas "50%" "50%" (BR.numberToStyledHtml "%2.2f" . (100*) . share50))
     <> C.headed "95%" (BR.toCell cas "95%" "95%" (BR.numberToStyledHtml "%2.2f" . (100*) . share95))



vaLower :: (K.KnitMany r, K.KnitOne r, BR.CacheEffects r)
        => Bool
        -> BR.PostPaths BR.Abs
        -> BR.PostInfo
        -> K.ActionWithCacheTime r SLDModelData
        -> K.Sem r ()
vaLower clearCaches postPaths postInfo sldDat_C = K.wrapPrefix "vaLower" $ do
  vaResults_C <- getVAResults "data/forPosts/VA_2019_General.json"
  vaResults <- K.ignoreCacheTime vaResults_C
  let comparison m t = do
        let (modelAndResult, missing) = FJ.leftJoinWithMissing @[BR.StateAbbreviation, ET.DistrictTypeC, ET.DistrictNumber] m vaResults
        when (not $ null missing) $ K.knitError $ "Missing join keys between model and election results: " <> show missing
        let  dShare = F.rgetField @DShare
             delta r =  dShare r - (MT.ciMid $ F.rgetField @ModeledShare r)
             contested r = dShare r > 0.01 && dShare r < 0.99
             (meanResult, meanDelta) = FL.fold ((,) <$> FL.prefilter contested (FL.premap dShare FLS.mean)
                                                 <*> FL.prefilter contested (FL.premap delta FLS.mean)) modelAndResult
             (varResult, varDelta) = FL.fold ((,) <$> FL.prefilter contested (FL.premap dShare (FLS.varianceUnbiased meanResult))
                                             <*> FL.prefilter contested (FL.premap delta (FLS.varianceUnbiased meanDelta))) modelAndResult
             caption = "Using " <> t <> ". "
                       <> "Model explains " <> show (100 * (varResult - varDelta)/varResult)
                       <> "% of the variance among the results."
        BR.logFrame modelAndResult
        _ <- K.addHvega Nothing (Just caption)
          $ modelResultScatterChart
          ("Modeled Vs. Actual (" <> t <> ")")
          (FV.ViewConfig 600 600 10)
          (fmap F.rcast modelAndResult)
        return ()
  K.logLE K.Info $ "Re-building VA Lower post"
  let modelNoteName = BR.Used "Model_Details"
  mModelNoteUrl <- BR.brNewNote postPaths postInfo modelNoteName "State Legislative Election Model" $ do
    BR.brAddNoteMarkDownFromFile postPaths modelNoteName "_intro"
  modelNoteUrl <- K.knitMaybe "naive Model Note Url is Nothing" $ mModelNoteUrl
  let modelRef = "[model_description]: " <> modelNoteUrl
  BR.brAddPostMarkDownFromFileWith postPaths "_intro" (Just modelRef)

  modelPlusState_C <- stateLegModel False PlusState CPS_Turnout sldDat_C
  modelPlusState <- K.ignoreCacheTime modelPlusState_C
  comparison modelPlusState "CPS Turnout"
  BR.brAddPostMarkDownFromFile postPaths "_chartDiscussion"

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


data Model = Base | PlusState | PlusSexEdu | PlusRaceEdu | PlusInteractions | PlusStateAndStateRace deriving (Show, Eq, Ord)
data TurnoutSource = CPS_Turnout | CES_Turnout deriving (Show, Eq, Ord)

stateLegModel :: (K.KnitEffects r, BR.CacheEffects r)
              => Bool
              -> Model
              -> TurnoutSource
              -> K.ActionWithCacheTime r SLDModelData
              -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec ModelResultsR))
stateLegModel clearCaches model tSource dat_C = K.wrapPrefix "stateLegModel" $ do
  K.logLE K.Info $ "(Re-)running state-leg model if necessary."
  let modelDir = "br-2021-VA/stan/"
      jsonDataName = "stateLeg_ASR_" <> show tSource
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
            hierGroupModel gtt s =
              let gn = SB.taggedGroupName gtt
              in SB.Hierarchical SB.STZNone (hierHPs gn s) (SB.Centered $ SB.normal (Just $ SB.name $ muH gn s) (SB.name $ sigmaH gn s))
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

        SB.generateLogLikelihood' voteData ((distT, logitT_ps feCDT) :| [(distP, logitP_ps feCDP)])


        return ()

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
          fmap FS.SFrame $ K.knitEither $ MT.keyedCIsToFrame sldLocationToRec $ M.toList resultsMap
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

type ModelResultsR = [BR.StateAbbreviation, ET.DistrictTypeC, ET.DistrictNumber, ModeledShare]

sldGroup :: SB.GroupTypeTag SLDLocation
sldGroup = SB.GroupTypeTag "SLD"

cdGroup :: SB.GroupTypeTag Text
cdGroup = SB.GroupTypeTag "CD"

stateGroup :: SB.GroupTypeTag Text
stateGroup = SB.GroupTypeTag "State"

sldStateGroup :: SB.GroupTypeTag Text
sldStateGroup = SB.GroupTypeTag "SLD_State"

ageGroup :: SB.GroupTypeTag DT.SimpleAge
ageGroup = SB.GroupTypeTag "Age"

sexGroup :: SB.GroupTypeTag DT.Sex
sexGroup = SB.GroupTypeTag "Sex"

sldSexGroup :: SB.GroupTypeTag DT.Sex
sldSexGroup = SB.GroupTypeTag "SLD_Sex"

educationGroup :: SB.GroupTypeTag DT.CollegeGrad
educationGroup = SB.GroupTypeTag "Education"

sldEducationGroup :: SB.GroupTypeTag DT.CollegeGrad
sldEducationGroup = SB.GroupTypeTag "SLD_Education"

raceGroup :: SB.GroupTypeTag DT.Race5
raceGroup = SB.GroupTypeTag "Race"

sldRaceGroup :: SB.GroupTypeTag DT.Race5
sldRaceGroup = SB.GroupTypeTag "SLD_Race"


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




modelResultScatterChart :: Text
                        -> FV.ViewConfig
                        -> F.FrameRec ([ET.DistrictNumber, ModeledShare, DShare])
                        -> GV.VegaLite
modelResultScatterChart title vc rows =
  let toVLDataRec = FV.asVLData (GV.Number . realToFrac) "District Number"
                    V.:& FV.asVLData' [("Model_Mid", GV.Number . (*100) . MT.ciMid)
                                      ,("Model_Upper", GV.Number . (*100) . MT.ciUpper)
                                      ,("Model_Lower", GV.Number . (*100) . MT.ciLower)
                                      ]
                    V.:& FV.asVLData (GV.Number . (*100)) "Election_Result"
                    V.:& V.RNil
      vlData = FV.recordsToData toVLDataRec rows
      encModelMid = GV.position GV.Y [GV.PName "Model_Mid"
                                     , GV.PmType GV.Quantitative
                                     , GV.PAxis [GV.AxTitle "Model_Mid"]
                                     --                                  , GV.PScale [GV.SDomain $ GV.DNumbers [0, 100]]
                                  , GV.PScale [GV.SZero False]
                                  ]
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

      regression p = GV.transform
                     . GV.filter (GV.FExpr "datum.Election_Result > 1 && datum.Election_Result < 99")
                     . GV.regression "Model_Mid" "Election_Result" [GV.RgParams p]
      errorT = GV.transform
               . GV.calculateAs "datum.Model_Hi - datum.Model_Mid" "E_Model_Hi"
               . GV.calculateAs "datum.Model_Mid - datum.Model_Lo" "E_Model_Lo"
      dotEnc = GV.encoding . encModelMid . encElection
      rangeEnc = GV.encoding . encModelLo . encModelHi . encElection
      lineEnc = GV.encoding . encModelMid . enc45
      dotSpec = GV.asSpec [dotEnc [], GV.mark GV.Circle [GV.MTooltip GV.TTData]]
      rangeSpec = GV.asSpec [rangeEnc [], GV.mark GV.Rule []]
      lineSpec = GV.asSpec [lineEnc [], GV.mark GV.Line [GV.MTooltip GV.TTNone]]
      regressionSpec = GV.asSpec [regression False [], dotEnc [], GV.mark GV.Line [GV.MStroke "red"]]
      r2Enc = GV.encoding
              . GV.position GV.X [GV.PNumber 200]
              . GV.position GV.Y [GV.PNumber 20]
              . GV.text [GV.TName "rSquared", GV.TmType GV.Nominal]
      r2Spec = GV.asSpec [regression True [], r2Enc [], GV.mark GV.Text []]
  in FV.configuredVegaLite vc [FV.title title, GV.layer [dotSpec, lineSpec], vlData]

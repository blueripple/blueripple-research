{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE OverloadedLabels #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE UndecidableInstances #-}
{-# OPTIONS_GHC -O0 #-}

module BlueRipple.Model.House.ElectionResult where

import qualified BlueRipple.Data.ACS_PUMS as PUMS
import qualified BlueRipple.Data.CCES as CCES
import qualified BlueRipple.Model.MRP as MRP
import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.ElectionTypes as ET
import qualified BlueRipple.Data.Loaders as BR
import qualified BlueRipple.Utilities.KnitUtils as BR
import qualified CmdStan as CS
import qualified Control.Foldl as FL
import qualified Data.Aeson as A
import qualified Data.Map as M
import Data.Maybe (fromMaybe)
import qualified Data.Set as Set
import Data.String.Here (here)
import qualified Data.Serialize                as S
import qualified Data.Text as T
import qualified Data.Vector as Vec
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V
import qualified Frames as F
import qualified Frames.Conversion as FC
import qualified Frames.Melt as F
import qualified Frames.InCore as FI
import qualified Frames.Folds as FF
import qualified Frames.MapReduce as FMR
import qualified Frames.Serialize as FS
import qualified Frames.SimpleJoins as FJ
import qualified Frames.Transform as FT
import qualified Graphics.Vega.VegaLite.MapRow as MapRow
import qualified Knit.Effect.AtomicCache as K hiding (retrieveOrMake)
import qualified Knit.Report as K
import qualified Numeric.Foldl as NFL
import qualified Optics.TH as Optics
import qualified Optics
import qualified Stan.JSON as SJ
import qualified Stan.ModelBuilder as SB
import qualified Stan.ModelConfig as SC
import qualified Stan.ModelRunner as SM
import qualified Stan.Parameters as SP
import qualified Stan.RScriptBuilder as SR

type FracUnder45 = "FracUnder45" F.:-> Double

type FracFemale = "FracFemale" F.:-> Double

type FracGrad = "FracGrad" F.:-> Double

type FracNonWhite = "FracNonWhite" F.:-> Double

type FracCitizen = "FracCitizen" F.:-> Double

type KeyR = [BR.Year, BR.StateAbbreviation, BR.CongressionalDistrict]

type DemographicsR =
  [ FracUnder45,
    FracFemale,
    FracGrad,
    FracNonWhite,
    FracCitizen,
    DT.AvgIncome,
    DT.MedianIncome,
    DT.PopPerSqMile,
    PUMS.Citizens
  ]

type DVotes = "DVotes" F.:-> Int
type RVotes = "RVotes" F.:-> Int
type TVotes = "TVotes" F.:-> Int

-- +1 for Dem incumbent, 0 for no incumbent, -1 for Rep incumbent
type Incumbency = "Incumbency" F.:-> Int
type ElectionR = [Incumbency, DVotes, RVotes]
type ElectionDataR = KeyR V.++ DemographicsR V.++ ElectionR
type ElectionPredictor = [FracUnder45, FracFemale, FracGrad, FracNonWhite, FracCitizen, DT.AvgIncome, DT.PopPerSqMile]
type ElectionData = F.FrameRec ElectionDataR


-- CCES data
type Surveyed = "Surveyed" F.:-> Int -- total people in each bucket
type CCESByCD = KeyR V.++ [DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.SimpleRaceC, Surveyed, TVotes, DVotes]
type CCESDataR = CCESByCD V.++ [Incumbency, FracCitizen, DT.AvgIncome, DT.MedianIncome, DT.PopPerSqMile]
type CCESPredictorR = [DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.SimpleRaceC, FracCitizen, DT.AvgIncome, DT.PopPerSqMile]
type CCESData = F.FrameRec CCESDataR

data HouseModelData = HouseModelData { electionData :: ElectionData, ccesData :: CCESData } 
Optics.makeFieldLabelsWith Optics.noPrefixFieldLabels ''HouseModelData

-- frames are not directly serializable so we have to do...shenanigans
instance S.Serialize HouseModelData where
  put (HouseModelData a b) = S.put (FS.SFrame a, FS.SFrame b)
  get = (\(a, b) -> HouseModelData (FS.unSFrame a) (FS.unSFrame b)) <$> S.get


pumsF :: FL.Fold (F.Record (PUMS.CDCounts DT.CatColsASER)) (F.FrameRec (KeyR V.++ DemographicsR))
pumsF =
  FMR.concatFold $
    FMR.mapReduceFold
      FMR.noUnpack
      (FMR.assignKeysAndData @KeyR)
      (FMR.foldAndAddKey pumsDataF)

pumsDataF ::
  FL.Fold
    (F.Record [DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.SimpleRaceC, DT.AvgIncome, DT.MedianIncome, DT.PopPerSqMile, PUMS.Citizens, PUMS.NonCitizens])
    (F.Record DemographicsR)
pumsDataF =
  let cit = F.rgetField @PUMS.Citizens
      citF = FL.premap cit FL.sum
      intRatio x y = realToFrac x / realToFrac y
      fracF f = intRatio <$> FL.prefilter f citF <*> citF
      citWgtdSumF f = FL.premap (\r -> realToFrac (cit r) * f r) FL.sum
      citWgtdF f = (/) <$> citWgtdSumF f <*> fmap realToFrac citF
   in FF.sequenceRecFold $
        FF.toFoldRecord (fracF ((== DT.Under) . F.rgetField @DT.SimpleAgeC))
          V.:& FF.toFoldRecord (fracF ((== DT.Female) . F.rgetField @DT.SexC))
          V.:& FF.toFoldRecord (fracF ((== DT.Grad) . F.rgetField @DT.CollegeGradC))
          V.:& FF.toFoldRecord (fracF ((== DT.NonWhite) . F.rgetField @DT.SimpleRaceC))
          V.:& FF.toFoldRecord ((\x y -> intRatio x (x + y)) <$> citF <*> FL.premap (F.rgetField @PUMS.NonCitizens) FL.sum)
          V.:& FF.toFoldRecord (citWgtdF (F.rgetField @DT.AvgIncome))
          V.:& FF.toFoldRecord (NFL.weightedMedianF (realToFrac . cit) (F.rgetField @DT.MedianIncome)) -- FL.premap (\r -> (realToFrac (cit r), F.rgetField @DT.MedianIncome r)) PUMS.medianIncomeF)
          V.:& FF.toFoldRecord (citWgtdF (F.rgetField @DT.PopPerSqMile))
          V.:& FF.toFoldRecord citF
          V.:& V.RNil

electionF :: FL.FoldM (Either T.Text) (F.Record (BR.HouseElectionCols V.++ '[ET.Incumbent])) (F.FrameRec (KeyR V.++ ElectionR))
electionF =
  FMR.concatFoldM $
    FMR.mapReduceFoldM
      (FMR.generalizeUnpack FMR.noUnpack)
      (FMR.generalizeAssign $ FMR.assignKeysAndData @KeyR)
      (FMR.makeRecsWithKeyM id $ FMR.ReduceFoldM $ const $ fmap (pure @[]) flattenVotesF)

data IncParty = None | Inc ET.PartyT | Multi

updateIncParty :: IncParty -> ET.PartyT -> IncParty
updateIncParty Multi _ = Multi
updateIncParty (Inc _) _ = Multi
updateIncParty None p = Inc p

incPartyToInt :: IncParty -> Either T.Text Int
incPartyToInt None = Right 0
incPartyToInt (Inc ET.Democratic) = Right 1
incPartyToInt (Inc ET.Republican) = Right (negate 1)
incPartyToInt (Inc _) = Right 0
incPartyToInt Multi = Left "Error: Multiple incumbents!"

flattenVotesF :: FL.FoldM (Either T.Text) (F.Record [BR.Candidate, ET.Incumbent, ET.Party, ET.Votes]) (F.Record ElectionR)
flattenVotesF = FMR.postMapM (FL.foldM flattenF) aggregatePartiesF
  where
    party = F.rgetField @ET.Party
    votes = F.rgetField @ET.Votes
    incumbentPartyF =
      FMR.postMapM incPartyToInt $
        FL.generalize $
          FL.prefilter (F.rgetField @ET.Incumbent) $
            FL.premap (F.rgetField @ET.Party) (FL.Fold updateIncParty None id)
    demVotesF = FL.generalize $ FL.prefilter (\r -> party r == ET.Democratic) $ FL.premap votes FL.sum
    repVotesF = FL.generalize $ FL.prefilter (\r -> party r == ET.Republican) $ FL.premap votes FL.sum
    flattenF = (\ii dv rv -> ii F.&: dv F.&: rv F.&: V.RNil) <$> incumbentPartyF <*> demVotesF <*> repVotesF

aggregatePartiesF ::
  FL.FoldM
    (Either T.Text)
    (F.Record [BR.Candidate, ET.Incumbent, ET.Party, ET.Votes])
    (F.FrameRec [BR.Candidate, ET.Incumbent, ET.Party, ET.Votes])
aggregatePartiesF =
  let apF :: FL.FoldM (Either T.Text) (F.Record [ET.Party, ET.Votes]) (F.Record [ET.Party, ET.Votes])
      apF = FMR.postMapM ap (FL.generalize $ FL.premap (\r -> (F.rgetField @ET.Party r, F.rgetField @ET.Votes r)) FL.map)
        where
          ap pvs =
            let demvM = M.lookup ET.Democratic pvs
                repvM = M.lookup ET.Republican pvs
                votes = FL.fold FL.sum $ M.elems pvs
                partyE = case (demvM, repvM) of
                  (Nothing, Nothing) -> Right ET.Other
                  (Just _, Nothing) -> Right ET.Democratic
                  (Nothing, Just _) -> Right ET.Republican
                  (Just dv, Just rv) -> Left "Votes on both D and R lines"
             in fmap (\p -> p F.&: votes F.&: V.RNil) partyE
   in FMR.concatFoldM $
        FMR.mapReduceFoldM
          (FMR.generalizeUnpack FMR.noUnpack)
          (FMR.generalizeAssign $ FMR.assignKeysAndData @[BR.Candidate, ET.Incumbent] @[ET.Party, ET.Votes])
          (FMR.makeRecsWithKeyM id $ FMR.ReduceFoldM $ const $ fmap (pure @[]) apF)


countCCESVotesF :: FL.Fold (F.Record [CCES.Turnout, CCES.HouseVoteParty]) (F.Record [Surveyed, TVotes, DVotes])
countCCESVotesF =
  let surveyedF = FL.length
      votedF = FL.prefilter ((== CCES.T_Voted) . F.rgetField @CCES.Turnout) FL.length
      dVoteF = FL.prefilter ((== ET.Democratic) . F.rgetField @CCES.HouseVoteParty) FL.length -- or votedF?
  in (\s v d -> s F.&: v F.&: d F.&: V.RNil) <$> surveyedF <*> votedF <*> dVoteF

ccesF :: Int -> FL.Fold (F.Record CCES.CCES_MRP) (F.FrameRec CCESByCD)
ccesF earliestYear = FMR.concatFold
        $ FMR.mapReduceFold
        (FMR.unpackFilterOnField @BR.Year (>= earliestYear))
        (FMR.assignKeysAndData @[BR.Year, BR.StateAbbreviation, BR.CongressionalDistrict, DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.SimpleRaceC])
        (FMR.foldAndAddKey countCCESVotesF)

ccesCountedDemHouseVotesByCD :: (K.KnitEffects r, K.CacheEffectsD r) => K.Sem r (K.ActionWithCacheTime r (F.FrameRec CCESByCD))
ccesCountedDemHouseVotesByCD = do
  cces_C <- CCES.ccesDataLoader 
  BR.retrieveOrMakeFrame "model/house/ccesByCD.bin" cces_C $ return . FL.fold (ccesF 2012)
    

prepCachedData ::
  (K.KnitEffects r, K.CacheEffectsD r) =>
  K.Sem r (K.ActionWithCacheTime r HouseModelData)
prepCachedData = do
  pums_C <- PUMS.pumsLoaderAdults
  cdFromPUMA_C <- BR.allCDFromPUMA2012Loader
  let pumsByCDDeps = (,) <$> pums_C <*> cdFromPUMA_C
  pumsByCD_C <- BR.retrieveOrMakeFrame "model/house/pumsByCD.bin" pumsByCDDeps $ \(pums, cdFromPUMA) ->
    PUMS.pumsCDRollup ((>= 2012) . F.rgetField @BR.Year) (PUMS.pumsKeysToASER True) cdFromPUMA pums
  houseElections_C <- BR.houseElectionsWithIncumbency
  countedCCES_C <- fmap (BR.fixAtLargeDistricts 0) <$> ccesCountedDemHouseVotesByCD
  K.ignoreCacheTime countedCCES_C >>= BR.logFrame . F.filterFrame ((== "MT") . F.rgetField @BR.StateAbbreviation)
  let houseDataDeps = (,,) <$> pumsByCD_C <*> houseElections_C <*> countedCCES_C
  BR.clearIfPresentD "model/house/houseData.bin"
  BR.retrieveOrMakeD {-@K.DefaultSerializer @K.DefaultCacheData @T.Text -} "model/house/houseData.bin" houseDataDeps $ \(pumsByCD, elex, countedCCES) -> do
    K.logLE K.Info "ElectionData for election model out of date/unbuilt.  Loading demographic and election data and joining."
    let demographics = FL.fold pumsF $ F.filterFrame ((/= "DC") . F.rgetField @BR.StateAbbreviation) pumsByCD
        isYear year = (== year) . F.rgetField @BR.Year
        afterYear year = (>= year) . F.rgetField @BR.Year
        dVotes = F.rgetField @DVotes
        rVotes = F.rgetField @RVotes
        competitive r = dVotes r > 0 && rVotes r > 0
        competitiveIn y r = isYear y r && competitive r
        competitiveAfter y r = afterYear y r && competitive r
        hasVoters r = F.rgetField @Surveyed r > 0
        hasVotes r = F.rgetField @TVotes r > 0
    electionResults <- K.knitEither $ FL.foldM electionF (F.filterFrame (afterYear 2012) elex)

    let (demoAndElex, missingElex) = FJ.leftJoinWithMissing @KeyR demographics electionResults
    K.knitEither $ if null missingElex
                   then Right ()
                   else Left $ "Missing keys in left-join of demographics and election data in house model prep:"
                        <> T.pack
                        (show missingElex)
    let competitiveElectionResults = F.filterFrame competitive demoAndElex
        competitiveCDs = FL.fold (FL.premap (F.rcast @KeyR) FL.set) demoAndElex
        competitiveCCES = F.filterFrame (\r -> Set.member (F.rcast @KeyR r) competitiveCDs) countedCCES
        toJoinWithCCES = fmap (F.rcast @(KeyR V.++ [Incumbency,FracCitizen, DT.AvgIncome, DT.MedianIncome,DT.PopPerSqMile])) competitiveElectionResults
        (ccesWithDD, missingDemo) = FJ.leftJoinWithMissing @KeyR toJoinWithCCES competitiveCCES --toJoinWithCCES        
    K.knitEither $ if null missingDemo
                   then Right ()
                   else Left $ "Missing keys in left-join of ccesByCD and demographic data in house model prep:"
                        <> T.pack
                        (show missingDemo)
    let ccesWithoutNullVotes = F.filterFrame (\r -> hasVoters r && hasVotes r) ccesWithDD -- ICK.  This will bias our turnout model
    BR.logFrame ccesWithoutNullVotes
    return $ HouseModelData competitiveElectionResults (fmap F.rcast ccesWithoutNullVotes)

type HouseDataWrangler = SC.DataWrangler HouseModelData  () ()

district r = F.rgetField @BR.StateAbbreviation r <> (T.pack $ show $ F.rgetField @BR.CongressionalDistrict r)
tVotes r = F.rgetField @DVotes r + F.rgetField @RVotes r
enumStateF = FL.premap (F.rgetField @BR.StateAbbreviation) (SJ.enumerate 1)
enumDistrictF = FL.premap district (SJ.enumerate 1)
maxAvgIncomeF = fmap (fromMaybe 1) $ FL.premap (F.rgetField @DT.AvgIncome) FL.maximum
maxDensityF = fmap (fromMaybe 1) $ FL.premap (F.rgetField @DT.PopPerSqMile) FL.maximum

makeElectionResultJSON :: ElectionData -> Either T.Text A.Series
makeElectionResultJSON er = do
  let ((stateM, _), (cdM, _), maxAvgIncome, maxDensity) =
        FL.fold
        ( (,,,)
          <$> enumStateF
          <*> enumDistrictF
          <*> maxAvgIncomeF
          <*> maxDensityF
        )
        er
      predictRow :: F.Record DemographicsR -> Vec.Vector Double
      predictRow r =
        Vec.fromList
        $ F.recToList 
        $ FT.fieldEndo @DT.AvgIncome (/ maxAvgIncome) 
        $ FT.fieldEndo @DT.PopPerSqMile (/ maxDensity) 
        $ F.rcast @ElectionPredictor r
      dataF =
        SJ.namedF "N" FL.length
        <> SJ.constDataF "K" (7 :: Int)
        <> SJ.valueToPairF "stateE" (SJ.jsonArrayMF (stateM . F.rgetField @BR.StateAbbreviation))
        <> SJ.valueToPairF "districtE" (SJ.jsonArrayMF (cdM . district))
        <> SJ.valueToPairF "Xe" (SJ.jsonArrayF (predictRow . F.rcast))
        <> SJ.valueToPairF "IncE" (SJ.jsonArrayF (F.rgetField @Incumbency))
        <> SJ.valueToPairF "VAPe" (SJ.jsonArrayF (F.rgetField @PUMS.Citizens))
        <> SJ.valueToPairF "TVotesE" (SJ.jsonArrayF tVotes)
        <> SJ.valueToPairF "DVotesE" (SJ.jsonArrayF (F.rgetField @DVotes))
  SJ.frameToStanJSONSeries dataF er

makeCCESDataJSON :: CCESData ->  Either T.Text A.Series
makeCCESDataJSON cd = do
  let ((stateM, _), (cdM, _), maxAvgIncome, maxDensity) =
        FL.fold ((,,,) <$> enumStateF <*> enumDistrictF <*> maxAvgIncomeF <*> maxDensityF) cd
      boolToNumber b = if b then 1 else 0
      predictRowC = FC.mkConverter (boolToNumber . (== DT.Under))
                    V.:& FC.mkConverter (boolToNumber . (== DT.Female))
                    V.:& FC.mkConverter (boolToNumber . (== DT.Grad))
                    V.:& FC.mkConverter (boolToNumber . (== DT.NonWhite))
                    V.:& FC.mkConverter id
                    V.:& FC.mkConverter id
                    V.:& FC.mkConverter id
                    V.:& V.RNil
      predictRow :: F.Record CCESPredictorR -> Vec.Vector Double
      predictRow r = Vec.fromList $ FC.toListVia predictRowC r
      dataF = SJ.namedF "M" FL.length
--              <> SJ.constDataF "K" (7 :: Int)
              <> SJ.valueToPairF "stateC" (SJ.jsonArrayMF (stateM . F.rgetField @BR.StateAbbreviation))
              <> SJ.valueToPairF "districtC" (SJ.jsonArrayMF (cdM . district))
              <> SJ.valueToPairF "Xc" (SJ.jsonArrayF (predictRow . F.rcast))
              <> SJ.valueToPairF "IncC" (SJ.jsonArrayF (F.rgetField @Incumbency))
              <> SJ.valueToPairF "VAPc" (SJ.jsonArrayF (F.rgetField @Surveyed))
              <> SJ.valueToPairF "TVotesC" (SJ.jsonArrayF (F.rgetField @TVotes))
              <> SJ.valueToPairF "DVotesC" (SJ.jsonArrayF (F.rgetField @DVotes))
  SJ.frameToStanJSONSeries dataF cd
  
houseDataWrangler :: HouseDataWrangler
houseDataWrangler = SC.Wrangle SC.NoIndex f
  where
    f _ = ((), makeDataJsonE)
      where
        makeDataJsonE hmd = do
          electionResultJsonE <- makeElectionResultJSON $ electionData hmd
          ccesDataJSONE <- makeCCESDataJSON $ ccesData hmd
          return $ electionResultJsonE <> ccesDataJSONE

data ModelWith = UseElectionResults | UseCCES | UseBoth deriving (Show, Eq, Ord)
        
type VoteP = "VoteProb" F.:-> Double
type DVoteP = "DVoteProb" F.:-> Double
type EVotes = "EstVotes" F.:-> Int
type EDVotes = "EstDVotes" F.:-> Int
type EDVotes5 = "EstDVotes5" F.:-> Int
type EDVotes95 = "EstDVotes95" F.:-> Int

type Modeled = [VoteP, DVoteP, EVotes, EDVotes5, EDVotes, EDVotes95]

type ElectionFit = [BR.StateAbbreviation, BR.CongressionalDistrict, PUMS.Citizens, DVotes, TVotes] V.++ Modeled
type CCESFit = [BR.StateAbbreviation, BR.CongressionalDistrict, Surveyed, TVotes, DVotes] V.++ Modeled
                
data HouseModelResults = HouseModelResults { electionFit :: F.FrameRec ElectionFit
                                           , ccesFit :: F.FrameRec CCESFit
                                           , parameterDeltas :: MapRow.MapRow [Double] }
                         
Optics.makeFieldLabelsWith Optics.noPrefixFieldLabels ''HouseModelResults

-- frames are not directly serializable so we have to do...shenanigans. 
instance S.Serialize HouseModelResults where
  put (HouseModelResults ef cf fp) = S.put (FS.SFrame ef, FS.SFrame cf, fp)
  get = (\(ef, cf, fp) -> HouseModelResults (FS.unSFrame ef) (FS.unSFrame cf) fp) <$> S.get

extractResults ::
  ModelWith
  -> CS.StanSummary
  -> HouseModelData
  -> Either T.Text HouseModelResults
extractResults modelWith summary hmd = do
  -- predictions
  pVotedP <- fmap CS.mean <$> SP.parse1D "pVotedP" (CS.paramStats summary)
  pDVotedP <- fmap CS.mean <$> SP.parse1D "pDVoteP" (CS.paramStats summary)
  eTVote <- fmap CS.mean <$> SP.parse1D "eTVotes" (CS.paramStats summary)
  eDVotePcts <- fmap CS.percents <$> SP.parse1D "eDVotes" (CS.paramStats summary)
  --deltaVs = fmap (\x -> "deltaV[")
  --deltaNameRow = nameRowFromList ["deltaV[1]", "deltaV[2]"]
  let modeled =
          Vec.zip4
            (SP.getVector pVotedP)
            (SP.getVector pDVotedP)
            (SP.getVector eTVote)
            (SP.getVector eDVotePcts)
      makeElectionFitRow (edRow, (pV, pD, etVotes, dVotesPcts)) = do
        if length dVotesPcts == 3
          then
            let [d5, d, d95] = dVotesPcts
             in Right $
                  F.rgetField @BR.StateAbbreviation edRow
                    F.&: F.rgetField @BR.CongressionalDistrict edRow
                    F.&: F.rgetField @PUMS.Citizens edRow
                    F.&: F.rgetField @DVotes edRow
                    F.&: (F.rgetField @DVotes edRow + F.rgetField @RVotes edRow)
                    F.&: pV
                    F.&: pD
                    F.&: round etVotes
                    F.&: round d5
                    F.&: round d
                    F.&: round d95
                    F.&: V.RNil
          else Left ("Wrong number of percentiles in stan statistic")
      makeCCESFitRow (cdRow, (pV, pD, etVotes, dVotesPcts)) = do
        if length dVotesPcts == 3
          then
            let [d5, d, d95] = dVotesPcts
             in Right $
                  F.rgetField @BR.StateAbbreviation cdRow
                    F.&: F.rgetField @BR.CongressionalDistrict cdRow
                    F.&: F.rgetField @Surveyed cdRow
                    F.&: F.rgetField @DVotes cdRow
                    F.&: F.rgetField @TVotes cdRow
                    F.&: pV
                    F.&: pD
                    F.&: round etVotes
                    F.&: round d5
                    F.&: round d
                    F.&: round d95
                    F.&: V.RNil
          else Left ("Wrong number of percentiles in stan statistic")
      ccesIndex = case modelWith of
        UseCCES -> 0
        _ -> FL.fold FL.length (electionData hmd)
  electionFit <- traverse makeElectionFitRow $ Vec.zip (FL.fold FL.vector $ electionData hmd) (Vec.take ccesIndex modeled)
  ccesFit <- traverse makeCCESFitRow $ Vec.zip (FL.fold FL.vector $ ccesData hmd) (Vec.drop ccesIndex modeled)
  -- deltas
  deltaIncD <- fmap CS.percents <$> SP.parseScalar "deltaIncD" (CS.paramStats summary)
  deltaD <- fmap CS.percents <$> SP.parse1D "deltaD" (CS.paramStats summary)
  deltaV <- fmap CS.percents <$> SP.parse1D "deltaV" (CS.paramStats summary)
  deltaDMR <-
    MapRow.withNames
      [ "PctUnder45D",
        "PctFemaleD",
        "PctGradD",
        "PctNonWhiteD",
        "PctCitizenD",
        "AvgIncomeD",
        "PopPerSqMileD"
      ]
      (SP.getVector deltaD)
  deltaVMR <-
    MapRow.withNames
      [ "PctUnder45V",
        "PctFemaleV",
        "PctGradV",
        "PctNonWhiteV",
        "PctCitizenV",
        "AvgIncomeV",
        "PopPerSqMileV"
      ]
      (SP.getVector deltaV)
  let deltas = deltaDMR <> deltaVMR <> M.singleton "Incumbency" (SP.getScalar deltaIncD)
  return $ HouseModelResults (F.toFrame electionFit) (F.toFrame ccesFit) deltas

runHouseModel ::
  forall r.
  (K.KnitEffects r, K.CacheEffectsD r) =>
  HouseDataWrangler ->
  (T.Text, ModelWith, ModelWith -> SB.StanModel) ->
  Int ->
  K.ActionWithCacheTime r HouseModelData ->
  K.Sem r (K.ActionWithCacheTime r HouseModelResults)
runHouseModel hdw (modelName, modelWith, model) year houseData_C = K.wrapPrefix "BlueRipple.Model.House.ElectionResults.runHouseModel" $ do
  K.logLE K.Info "Running..."
  let workDir = "stan/house/election"
      modelNameSuffix = modelName <> "_" <> (T.pack $ show modelWith) <> "_" <> (T.pack $ show year) 
  let stancConfig = (SM.makeDefaultStancConfig (T.unpack $ workDir <> "/" <> modelName)) {CS.useOpenCL = False}
  stanConfig <-
    SC.setSigFigs 4
      . SC.noLogOfSummary
      <$> SM.makeDefaultModelRunnerConfig
        workDir
        (modelName <> "_" <> (T.pack $ show modelWith) <> "_model")
        (Just (SB.All, model modelWith))
        (Just $ "election_" <> (T.pack $ show year) <> ".json")
        (Just $ "election_" <> modelNameSuffix)
        4
        (Just 1000)
        (Just 1000)
        (Just stancConfig)
  let resultCacheKey = "house/model/stan/election_" <> modelNameSuffix <> ".bin"
      filterToYear :: (F.ElemOf rs BR.Year, FI.RecVec rs) => F.FrameRec rs -> F.FrameRec rs
      filterToYear = F.filterFrame ((== year) . F.rgetField @BR.Year)
      houseDataForYear_C = fmap (Optics.over #electionData filterToYear . Optics.over #ccesData filterToYear) houseData_C
  modelDep <- SM.modelCacheTime stanConfig
  K.logLE K.Diagnostic $ "modelDep: " <> (T.pack $ show $ K.cacheTime modelDep)
  K.logLE K.Diagnostic $ "houseDataDep: " <> (T.pack $ show $ K.cacheTime houseData_C)
  let dataModelDep = const <$> modelDep <*> houseDataForYear_C
      getResults s () inputAndIndex_C = do
        (houseModelData, _) <- K.ignoreCacheTime inputAndIndex_C
        K.knitEither $ extractResults modelWith s houseModelData
  BR.retrieveOrMakeD resultCacheKey dataModelDep $ \() -> do
    K.logLE K.Info "Data or model newer then last cached result. (Re)-running..."
    SM.runModel
      stanConfig
      (SM.Both [SR.UnwrapJSON "DVotes" "DVotes", SR.UnwrapJSON "TVotes" "TVotes"])
      hdw
      (SC.UseSummary getResults)
      ()
      houseDataForYear_C

tdb :: ModelWith -> SB.TransformedDataBlock
tdb UseElectionResults = binomialTransformedDataBlock_ElexOnly
tdb UseCCES = binomialTransformedDataBlock_CCESOnly
tdb UseBoth = binomialTransformedDataBlock_Both

binomial_v1 :: ModelWith -> SB.StanModel
binomial_v1 modelWith =
  SB.StanModel
    binomialDataBlock
    (Just $ tdb modelWith)
    binomialParametersBlock
    (Just binomialTransformedParametersBlock)
    binomialModelBlock
    (Just binomialGeneratedQuantitiesBlock)
    binomialGQLLBlock

betaBinomial_v1 :: ModelWith -> SB.StanModel
betaBinomial_v1 modelWith =
  SB.StanModel
    binomialDataBlock
    (Just $ tdb modelWith)
    betaBinomialParametersBlock
    (Just betaBinomialTransformedParametersBlock)
    betaBinomialModelBlock
    (Just betaBinomialGeneratedQuantitiesBlock)
    betaBinomialGQLLBlock

betaBinomialInc :: ModelWith -> SB.StanModel
betaBinomialInc modelWith =
  SB.StanModel
  binomialDataBlock
  (Just $ tdb modelWith)
  betaBinomialIncParametersBlock
  (Just betaBinomialIncTransformedParametersBlock)
  betaBinomialIncModelBlock
  (Just betaBinomialIncGeneratedQuantitiesBlock)
  betaBinomialGQLLBlock

betaBinomialHS :: ModelWith -> SB.StanModel
betaBinomialHS modelWith =
  SB.StanModel
    binomialDataBlock
    (Just $ tdb modelWith)
    betaBinomialHSParametersBlock
    (Just betaBinomialTransformedParametersBlock)
    betaBinomialHSModelBlock
    (Just betaBinomialGeneratedQuantitiesBlock)
    betaBinomialGQLLBlock

binomialDataBlock :: SB.DataBlock
binomialDataBlock =
  [here|  
  int<lower = 1> N; // number of districts
  int<lower = 1> M; // number of cces rows
  int<lower = 1> K; // number of predictors
  int<lower = 1, upper = N> districtE[N]; // do we need this?
  int<lower = 1, upper = M> districtC[M]; // do we need this?
  matrix[N, K] Xe;
  matrix[M, K] Xc;
  int<lower=-1, upper=1> IncE[N];
  int<lower=-1, upper=1> IncC[M];
  int<lower = 0> VAPe[N];
  int<lower = 0> VAPc[M];
  int<lower = 0> TVotesE[N];
  int<lower = 0> DVotesE[N];
  int<lower = 0> TVotesC[M];
  int<lower = 0> DVotesC[M];
|]

transformedDataBlockCommon :: T.Text
transformedDataBlockCommon = [here|
 vector<lower=0>[K] sigma;
  matrix[G, K] X_centered;
  for (k in 1:K) {
    real col_mean = mean(X[,k]);
    X_centered[,k] = X[,k] - col_mean;
    sigma[k] = sd(X_centered[,k]);
  } 
  
  matrix[G, K] Q_ast;
  matrix[K, K] R_ast;
  matrix[K, K] R_ast_inverse;
  // thin and scale the QR decomposition
  Q_ast = qr_Q(X_centered)[, 1:K] * sqrt(G - 1);
  R_ast = qr_R(X_centered)[1:K,]/sqrt(G - 1);
  R_ast_inverse = inverse(R_ast);
|]
    
binomialTransformedDataBlock_ElexOnly :: SB.TransformedDataBlock
binomialTransformedDataBlock_ElexOnly =
  [here|
  int<lower=0> G = N;
  matrix[G, K] X = Xe;
  int<lower=-1, upper=1> Inc[G] = IncE;
  int<lower=0> VAP[G] = VAPe;
  int<lower=0> TVotes[G] = TVotesE;
  int<lower=0> DVotes[G] = DVotesE;  
  |] <> transformedDataBlockCommon

binomialTransformedDataBlock_CCESOnly :: SB.TransformedDataBlock
binomialTransformedDataBlock_CCESOnly =
  [here|
  int<lower=0> G = M;
  matrix[G, K] X = Xc;
  int<lower=-1, upper=1> Inc[G] = IncC;
  int<lower=0> VAP[G] = VAPc;
  int<lower=0> TVotes[G] = TVotesC;
  int<lower=0> DVotes[G] = DVotesC;  
  |] <> transformedDataBlockCommon

binomialTransformedDataBlock_Both :: SB.TransformedDataBlock
binomialTransformedDataBlock_Both =
  [here|
  int<lower=0> G = M + N;
  matrix[G, K] X = append_row (Xe, Xc);
  int<lower=-1, upper=1> Inc[G] = append_array(IncE, IncE);
  int<lower=0> VAP[G] = append_array(VAPe, VAPc);
  int<lower=0> TVotes[G] = append_array (TVotesE, TVotesC);
  int<lower=0> DVotes[G] = append_array (DVotesE, DVotesC);  
  |] <> transformedDataBlockCommon
  
binomialParametersBlock :: SB.ParametersBlock
binomialParametersBlock =
  [here|
  real alphaD;                             
  vector[K] thetaV;
  real alphaV;
  vector[K] thetaD;
|]

binomialTransformedParametersBlock :: SB.TransformedParametersBlock
binomialTransformedParametersBlock =
  [here|
  vector [K] betaV;
  vector [K] betaD;
  betaV = R_ast_inverse * thetaV;
  betaD = R_ast_inverse * thetaD;
|]

binomialModelBlock :: SB.ModelBlock
binomialModelBlock =
  [here|
  alphaD ~ cauchy(0, 10);
  alphaV ~ cauchy(0, 10);
  betaD ~ cauchy(0, 2.5);
  betaV ~ cauchy(0,2.5);
  TVotes ~ binomial_logit(VAP, alphaV + Q_ast * thetaV);
  DVotes ~ binomial_logit(TVotes, alphaD + Q_ast * thetaD);
|]

binomialGeneratedQuantitiesBlock :: SB.GeneratedQuantitiesBlock
binomialGeneratedQuantitiesBlock =
  [here|
  vector<lower = 0, upper = 1>[G] pVotedP;
  vector<lower = 0, upper = 1>[G] pDVoteP;
  pVotedP = inv_logit(alphaV + (Q_ast * thetaV));
  pDVoteP = inv_logit(alphaD + (Q_ast * thetaD));
  vector<lower = 0>[G] eTVotes;
  vector<lower = 0>[G] eDVotes;
  for (g in 1:G) {
    eTVotes[g] = inv_logit(alphaV + (Q_ast[g] * thetaV)) * VAP[g];
    eDVotes[g] = inv_logit(alphaD + (Q_ast[g] * thetaD)) * TVotes[g];
  }
|]

binomialGQLLBlock :: SB.GeneratedQuantitiesBlock
binomialGQLLBlock =
  [here|
  vector[G] log_lik;
//  log_lik = binomial_logit_lpmf(DVotes1 | TVotes1, alphaD + Q_ast * thetaD);
  for (g in 1:G) {
    log_lik[g] =  binomial_logit_lpmf(DVotes[g] | TVotes[g], alphaD + Q_ast[g] * thetaD);
  }
|]

betaBinomialParametersBlock :: SB.ParametersBlock
betaBinomialParametersBlock =
  [here|
  real alphaD;
  real <lower=0, upper=1> dispD;                             
  vector[K] thetaV;
  real alphaV;
  real <lower=0, upper=1> dispV;
  vector[K] thetaD;
|]

betaBinomialTransformedParametersBlock :: SB.TransformedParametersBlock
betaBinomialTransformedParametersBlock =
  [here|
  real <lower=0> phiD = (1-dispD)/dispD;
  real <lower=0> phiV = (1-dispV)/dispV;
  vector [G] pDVoteP = inv_logit (alphaD + Q_ast * thetaD);
  vector [G] pVotedP = inv_logit (alphaV + Q_ast * thetaV);
  vector [K] betaV;
  vector [K] betaD;
  betaV = R_ast_inverse * thetaV;
  betaD = R_ast_inverse * thetaD;
|]

betaBinomialModelBlock :: SB.ModelBlock
betaBinomialModelBlock =
  [here|
  alphaD ~ cauchy(0, 10);
  alphaV ~ cauchy(0, 10);
  betaV ~ cauchy(0, 2.5);
  betaD ~ cauchy(0, 2.5);

  TVotes ~ beta_binomial(VAP, pVotedP * phiV, (1 - pVotedP) * phiV);
  DVotes ~ beta_binomial(TVotes, pDVoteP * phiD, (1 - pDVoteP) * phiD);
|]

betaBinomialGeneratedQuantitiesBlock :: SB.GeneratedQuantitiesBlock
betaBinomialGeneratedQuantitiesBlock =
  [here|
  vector<lower = 0>[G] eTVotes;
  vector<lower = 0>[G] eDVotes;
  for (g in 1:G) {
    eTVotes[g] = pVotedP[g] * VAP[g];
    eDVotes[g] = pDVoteP[g] * TVotes[g];
  }
|]

betaBinomialGQLLBlock :: SB.GeneratedQuantitiesBlock
betaBinomialGQLLBlock =
  [here|
  vector[G] log_lik;
  for (g in 1:G) {
    log_lik[g] =  beta_binomial_lpmf(DVotes[g] | TVotes[g], pDVoteP[g] * phiD, (1 - pDVoteP[g]) * phiD) ;
  }
|]

betaBinomialIncParametersBlock :: SB.ParametersBlock
betaBinomialIncParametersBlock =
  [here|
  real alphaD;
  real <lower=0, upper=1> dispD;                             
  vector[K] thetaV;
  real alphaV;
  real <lower=0, upper=1> dispV;
  vector[K] thetaD;
  real incBetaD;
|]

betaBinomialIncTransformedParametersBlock :: SB.TransformedParametersBlock
betaBinomialIncTransformedParametersBlock =
  [here|
  real <lower=0> phiD = (1-dispD)/dispD;
  real <lower=0> phiV = (1-dispV)/dispV;
  vector [G] pDVoteP = inv_logit (alphaD + Q_ast * thetaD + to_vector(Inc) * incBetaD);
  vector [G] pVotedP = inv_logit (alphaV + Q_ast * thetaV);
  vector [K] betaV;
  vector [K] betaD;
  betaV = R_ast_inverse * thetaV;
  betaD = R_ast_inverse * thetaD;
|]

betaBinomialIncModelBlock :: SB.ModelBlock
betaBinomialIncModelBlock =
  [here|
  alphaD ~ cauchy(0, 10);
  alphaV ~ cauchy(0, 10);
  betaV ~ cauchy(0, 2.5);
  betaD ~ cauchy(0, 2.5);
  incBetaD ~ cauchy(0, 2.5);
  TVotes ~ beta_binomial(VAP, pVotedP * phiV, (1 - pVotedP) * phiV);
  DVotes ~ beta_binomial(TVotes, pDVoteP * phiD, (1 - pDVoteP) * phiD);
|]

betaBinomialIncGeneratedQuantitiesBlock :: SB.GeneratedQuantitiesBlock
betaBinomialIncGeneratedQuantitiesBlock =
  [here|
  vector<lower = 0>[G] eTVotes;
  vector<lower = 0>[G] eDVotes;
  for (g in 1:G) {
    eTVotes[g] = pVotedP[g] * VAP[g];
    eDVotes[g] = pDVoteP[g] * TVotes[g];
  }
  real avgPVoted = inv_logit (alphaV);
  real avgPDVote = inv_logit (alphaD);
  vector[K] deltaV;
  vector[K] deltaD;
  for (k in 1:K) {
    deltaV [k] = inv_logit (alphaV + sigma [k] * betaV [k]) - avgPVoted;
    deltaD [k] = inv_logit (alphaD + sigma [k] * betaD [k]) - avgPDVote;
  }
  real deltaIncD = inv_logit(alphaD + incBetaD) - avgPDVote;
|]

betaBinomialHSParametersBlock :: SB.ParametersBlock
betaBinomialHSParametersBlock =
  [here|
  real alphaD;
  real <lower=0, upper=1> dispD;                             
  vector[K] thetaV;
  vector<lower=0>[K] lambdaV;
  real<lower=0> tauV;
  real alphaV;
  real <lower=0, upper=1> dispV;
  vector[K] thetaD;
  vector<lower=0>[K] lambdaD;
  real<lower=0> tauD;
|]

betaBinomialHSModelBlock :: SB.ModelBlock
betaBinomialHSModelBlock =
  [here|
  alphaD ~ cauchy(0, 10);
  lambdaD ~ cauchy(0, 1);
  tauD ~ cauchy (0, 2.5);
  alphaV ~ cauchy(0, 10);
  lambdaV ~ cauchy(0, 2.5);
  tauV ~ cauchy (0,1);
  for (k in 1:K) {
    thetaV[k] ~ cauchy(0, lambdaV[k] * tauV);
    thetaD[k] ~ cauchy(0, lambdaD[k] * tauD);
  }
  TVotes ~ beta_binomial(VAP, pVotedP * phiV, (1 - pVotedP) * phiV);
  DVotes ~ beta_binomial(TVotes, pDVoteP * phiD, (1 - pDVoteP) * phiD);
|]

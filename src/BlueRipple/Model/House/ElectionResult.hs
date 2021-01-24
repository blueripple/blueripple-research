{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE OverloadedLabels #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}
{-# OPTIONS_GHC -O0 #-}

module BlueRipple.Model.House.ElectionResult where

import Prelude hiding (pred)
import qualified BlueRipple.Data.ACS_PUMS as PUMS
import qualified BlueRipple.Data.CCES as CCES
import qualified BlueRipple.Model.MRP as MRP
import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.ElectionTypes as ET
import qualified BlueRipple.Data.Loaders as BR
import qualified BlueRipple.Utilities.KnitUtils as BR
import qualified BlueRipple.Utilities.FramesUtils as BRF
import qualified CmdStan as CS
import qualified Control.Foldl as FL
import qualified Data.Aeson as A
import qualified Data.Generics.Labels as GLabels
import qualified Data.Map as M
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
import GHC.Generics (Generic)
import qualified Data.MapRow as MapRow
import qualified Knit.Effect.AtomicCache as K hiding (retrieveOrMake)
import qualified Knit.Report as K
import qualified Knit.Utilities.Streamly as K
import qualified Numeric.Foldl as NFL
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

type FracWhiteNonHispanic = "FracWhiteNonHispanic" F.:-> Double
type FracWhiteHispanic = "FracWhiteHispanic" F.:-> Double
type FracNonWhiteHispanic = "FracNonWhiteHispanic" F.:-> Double
type FracBlack = "FracBlack" F.:-> Double
type FracAsian = "FracAsian" F.:-> Double
type FracOther = "FracOther" F.:-> Double

type FracCitizen = "FracCitizen" F.:-> Double

type KeyR = [BR.Year, BR.StateAbbreviation, BR.CongressionalDistrict]

type DemographicsR =
  [ FracUnder45
  , FracFemale
  , FracGrad
  , FracWhiteNonHispanic
  , FracWhiteHispanic
  , FracNonWhiteHispanic
  , FracBlack
  , FracAsian
  , FracOther
  , DT.AvgIncome
  , DT.PopPerSqMile
  , PUMS.Citizens
  ]

type DVotes = "DVotes" F.:-> Int
type RVotes = "RVotes" F.:-> Int
type TVotes = "TVotes" F.:-> Int

-- +1 for Dem incumbent, 0 for no incumbent, -1 for Rep incumbent
type Incumbency = "Incumbency" F.:-> Int
type ElectionR = [Incumbency, DVotes, RVotes]
type ElectionDataR = KeyR V.++ DemographicsR V.++ ElectionR
type ElectionPredictorR = [FracUnder45
                          , FracFemale
                          , FracGrad
                          , FracWhiteNonHispanic
                          , FracWhiteHispanic
                          , FracNonWhiteHispanic
                          , FracBlack
                          , FracAsian
                          , FracOther
                          , DT.AvgIncome
                          , DT.PopPerSqMile]
type ElectionData = F.FrameRec ElectionDataR


-- CCES data
type Surveyed = "Surveyed" F.:-> Int -- total people in each bucket
type CCESByCD = KeyR V.++ [DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.Race5C, DT.HispC, Surveyed, TVotes, DVotes]
type CCESDataR = CCESByCD V.++ [Incumbency, DT.AvgIncome, DT.PopPerSqMile]
type CCESPredictorR = [DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.Race5C, DT.HispC, DT.AvgIncome, DT.PopPerSqMile]
type CCESData = F.FrameRec CCESDataR

data HouseModelData = HouseModelData { electionData :: ElectionData, ccesData :: CCESData } deriving (Generic)

-- frames are not directly serializable so we have to do...shenanigans
instance S.Serialize HouseModelData where
  put (HouseModelData a b) = S.put (FS.SFrame a, FS.SFrame b)
  get = (\(a, b) -> HouseModelData (FS.unSFrame a) (FS.unSFrame b)) <$> S.get


pumsMR :: (Foldable f, Monad m)
       => f (F.Record (PUMS.CDCounts [DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.RaceAlone4C, DT.HispC]))
       -> m (F.FrameRec (KeyR V.++ DemographicsR))
pumsMR = BRF.frameCompactMRM
        FMR.noUnpack
        (FMR.assignKeysAndData @KeyR)
        pumsDataF

pumsDataF ::
  FL.Fold
    (F.Record [DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.RaceAlone4C, DT.HispC, DT.AvgIncome, DT.PopPerSqMile, PUMS.Citizens, PUMS.NonCitizens])
    (F.Record DemographicsR)
pumsDataF =
  let cit = F.rgetField @PUMS.Citizens
      citF = FL.premap cit FL.sum
      intRatio x y = realToFrac x / realToFrac y
      fracF f = intRatio <$> FL.prefilter f citF <*> citF
      citWgtdSumF f = FL.premap (\r -> realToFrac (cit r) * f r) FL.sum
      citWgtdF f = (/) <$> citWgtdSumF f <*> fmap realToFrac citF
      race4A = F.rgetField @DT.RaceAlone4C
      hisp = F.rgetField @DT.HispC
      wnh r = race4A r == DT.RA4_White && hisp r == DT.NonHispanic
      wh r = race4A r == DT.RA4_White && hisp r == DT.Hispanic
      nwh r = race4A r /= DT.RA4_White && hisp r == DT.Hispanic -- this overlaps other categories
      black r = race4A r /= DT.RA4_Black
      asian r = race4A r /= DT.RA4_Asian
      other r = race4A r /= DT.RA4_Other
  in FF.sequenceRecFold $
     FF.toFoldRecord (fracF ((== DT.Under) . F.rgetField @DT.SimpleAgeC))
     V.:& FF.toFoldRecord (fracF ((== DT.Female) . F.rgetField @DT.SexC))
     V.:& FF.toFoldRecord (fracF ((== DT.Grad) . F.rgetField @DT.CollegeGradC))
     V.:& FF.toFoldRecord (fracF wnh)
     V.:& FF.toFoldRecord (fracF wh)
     V.:& FF.toFoldRecord (fracF nwh)
     V.:& FF.toFoldRecord (fracF black)
     V.:& FF.toFoldRecord (fracF asian)
     V.:& FF.toFoldRecord (fracF other)
     V.:& FF.toFoldRecord (citWgtdF (F.rgetField @DT.AvgIncome))
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
      dVoteF = FL.prefilter ((== ET.Democratic) . F.rgetField @CCES.HouseVoteParty) votedF
  in (\s v d -> s F.&: v F.&: d F.&: V.RNil) <$> surveyedF <*> votedF <*> dVoteF

ccesMR :: (Foldable f, Monad m) => Int -> f (F.Record CCES.CCES_MRP) -> m (F.FrameRec CCESByCD)
ccesMR earliestYear = BRF.frameCompactMRM
                     (FMR.unpackFilterOnField @BR.Year (>= earliestYear))
                     (FMR.assignKeysAndData @[BR.Year, BR.StateAbbreviation, BR.CongressionalDistrict, DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.Race5C, DT.HispC])
                     countCCESVotesF

ccesCountedDemHouseVotesByCD :: (K.KnitEffects r, K.CacheEffectsD r) => K.Sem r (K.ActionWithCacheTime r (F.FrameRec CCESByCD))
ccesCountedDemHouseVotesByCD = do
  cces_C <- CCES.ccesDataLoader
--  BR.clearIfPresentD "model/house/ccesByCD.bin"
  BR.retrieveOrMakeFrame "model/house/ccesByCD.bin" cces_C $ ccesMR 2012

pumsReKey :: F.Record '[DT.Age5FC, DT.SexC, DT.CollegeGradC, DT.InCollege, DT.RaceAlone4C, DT.HispC]
          ->  F.Record '[DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.RaceAlone4C, DT.HispC]
pumsReKey r =
  let cg = F.rgetField @DT.CollegeGradC r
      ic = F.rgetField @DT.InCollege r
  in DT.age5FToSimple (F.rgetField @DT.Age5FC r)
     F.&: F.rgetField @DT.SexC r
     F.&: (if cg == DT.Grad || ic then DT.Grad else DT.NonGrad)
     F.&: F.rgetField @DT.RaceAlone4C r
     F.&: F.rgetField @DT.HispC r
     F.&: V.RNil

prepCachedData ::
  (K.KnitEffects r, K.CacheEffectsD r) => Bool -> K.Sem r (K.ActionWithCacheTime r HouseModelData)
prepCachedData clearCache = do
  pums_C <- PUMS.pumsLoaderAdults
  cdFromPUMA_C <- BR.allCDFromPUMA2012Loader
  let pumsByCDDeps = (,) <$> pums_C <*> cdFromPUMA_C
  pumsByCD_C <- BR.retrieveOrMakeFrame "model/house/pumsByCD.bin" pumsByCDDeps $ \(pums, cdFromPUMA) ->
    PUMS.pumsCDRollup ((>= 2012) . F.rgetField @BR.Year) (pumsReKey . F.rcast) cdFromPUMA pums
  houseElections_C <- BR.houseElectionsWithIncumbency
  countedCCES_C <- fmap (BR.fixAtLargeDistricts 0) <$> ccesCountedDemHouseVotesByCD
  let houseDataDeps = (,,) <$> pumsByCD_C <*> houseElections_C <*> countedCCES_C
  when clearCache $ BR.clearIfPresentD "model/house/houseData.bin"
  BR.retrieveOrMakeD "model/house/houseData.bin" houseDataDeps $ \(pumsByCD, elex, countedCCES) -> do
    K.logLE K.Info "ElectionData for election model out of date/unbuilt.  Loading demographic and election data and joining."
    demographics <- K.streamlyToKnit $ pumsMR $ F.filterFrame ((/= "DC") . F.rgetField @BR.StateAbbreviation) pumsByCD
    let isYear year = (== year) . F.rgetField @BR.Year
        afterYear year = (>= year) . F.rgetField @BR.Year
        dVotes = F.rgetField @DVotes
        rVotes = F.rgetField @RVotes
        competitive r = dVotes r > 0 && rVotes r > 0
        competitiveIn y r = isYear y r && competitive r
        competitiveAfter y r = afterYear y r && competitive r
        hasVoters r = F.rgetField @Surveyed r > 0
        hasVotes r = F.rgetField @TVotes r > 0
        hasDVotes r = F.rgetField @DVotes r > 0
    electionResults <- K.knitEither $ FL.foldM electionF (F.filterFrame (afterYear 2012) elex)

    let (demoAndElex, missingElex) = FJ.leftJoinWithMissing @KeyR demographics electionResults
    K.knitEither $ if null missingElex
                   then Right ()
                   else Left $ "Missing keys in left-join of demographics and election data in house model prep:"
                        <> show missingElex
    let competitiveElectionResults = F.filterFrame competitive demoAndElex
        competitiveCDs = FL.fold (FL.premap (F.rcast @KeyR) FL.set) demoAndElex
        competitiveCCES = F.filterFrame (\r -> Set.member (F.rcast @KeyR r) competitiveCDs) countedCCES
        toJoinWithCCES = fmap (F.rcast @(KeyR V.++ [Incumbency, DT.AvgIncome{-, DT.MedianIncome-}, DT.PopPerSqMile])) competitiveElectionResults
        (ccesWithDD, missingDemo) = FJ.leftJoinWithMissing @KeyR toJoinWithCCES competitiveCCES --toJoinWithCCES
    K.knitEither $ if null missingDemo
                   then Right ()
                   else Left $ "Missing keys in left-join of ccesByCD and demographic data in house model prep:"
                        <> show missingDemo
    let ccesWithoutNullVotes = F.filterFrame (\r -> hasVoters r && hasVotes r) ccesWithDD -- ICK.  This will bias our turnout model
    return $ HouseModelData competitiveElectionResults (fmap F.rcast ccesWithoutNullVotes)

type HouseDataWrangler = SC.DataWrangler HouseModelData  () ()

district r = F.rgetField @BR.StateAbbreviation r <> show (F.rgetField @BR.CongressionalDistrict r)

enumStateF = FL.premap (F.rgetField @BR.StateAbbreviation) (SJ.enumerate 1)
enumDistrictF = FL.premap district (SJ.enumerate 1)
maxAvgIncomeF = fromMaybe 1 <$> FL.premap (F.rgetField @DT.AvgIncome) FL.maximum
maxDensityF = fromMaybe 1 <$> FL.premap (F.rgetField @DT.PopPerSqMile) FL.maximum

type PredictorMap = Map Text (F.Record ElectionDataR -> Double, F.Record CCESDataR -> Double)

ccesWNH r = (F.rgetField @DT.Race5C r == DT.R5_WhiteNonLatinx) && (F.rgetField @DT.HispC r == DT.NonHispanic)
ccesWH r = (F.rgetField @DT.Race5C r == DT.R5_WhiteNonLatinx) && (F.rgetField @DT.HispC r == DT.Hispanic)
ccesW r = ccesWNH r || ccesWH r
ccesNWH r = (F.rgetField @DT.Race5C r /= DT.R5_WhiteNonLatinx) && (F.rgetField @DT.HispC r == DT.Hispanic)

pumsWhite r = F.rgetField @FracWhiteNonHispanic r + F.rgetField @FracWhiteHispanic r
pumsWhiteNH r = F.rgetField @FracWhiteNonHispanic r

predictorMap :: PredictorMap
predictorMap =
  let boolToNumber b = if b then 1 else 0
  in M.fromList [("PctUnder45",(F.rgetField @FracUnder45, boolToNumber . (== DT.Under) . F.rgetField @DT.SimpleAgeC))
                ,("PctFemale",(F.rgetField @FracFemale, boolToNumber . (== DT.Female) . F.rgetField @DT.SexC))
                ,("PctGrad",(F.rgetField @FracGrad, boolToNumber . (== DT.Grad) . F.rgetField @DT.CollegeGradC))
                ,("PcWhiteNonHispanic", (F.rgetField @FracWhiteNonHispanic , boolToNumber . ccesWNH))
                ,("PctWhite",(pumsWhite, boolToNumber . ccesW))
                ,("PctNonWhiteNH", (\r -> 1 - pumsWhiteNH r, boolToNumber . not . ccesWNH))
                ,("PctNonWhite", (\r -> 1 - pumsWhite r, boolToNumber . not . ccesW))
                ,("AvgIncome",(F.rgetField @DT.AvgIncome, F.rgetField @DT.AvgIncome))
                ,("PopPerSqMile",(F.rgetField @DT.PopPerSqMile, F.rgetField @DT.PopPerSqMile))
                ,("Incumbency",(realToFrac . F.rgetField @Incumbency, realToFrac . F.rgetField @Incumbency))
                ]

adjustPredictor :: (Double -> Double) -> Text -> PredictorMap -> PredictorMap
adjustPredictor f k =
  let g (h1, h2) = (f . h1, f . h2)
  in M.adjust g k

data ModelRow = ModelRow { distLabel :: Text
                         , pred :: Vec.Vector Double
--                         , inc :: Int
                         , vap :: Int
                         , tVotes :: Int
                         , dVotes :: Int
                         } deriving (Show)


electionResultToModelRow :: (F.Record ElectionDataR -> Vec.Vector Double) -> F.Record ElectionDataR -> ModelRow
electionResultToModelRow predictRow r =
  (ModelRow <$> district <*> predictRow <*> F.rgetField @PUMS.Citizens <*> tVotes <*> dVotes) $ r where
  dVotes = F.rgetField @DVotes
  tVotes r = dVotes r + F.rgetField @RVotes r

electionResultsToModelRows :: Foldable f => [Text] -> f (F.Record ElectionDataR) -> Either Text (Vec.Vector ModelRow)
electionResultsToModelRows predictors er = do
  let  ((stateM, _), (cdM, _), maxAvgIncome, maxDensity) =
         FL.fold
         ( (,,,)
           <$> enumStateF
           <*> enumDistrictF
           <*> maxAvgIncomeF
           <*> maxDensityF
         )
         er
       pMap = adjustPredictor (/maxAvgIncome) "AvgIncome"
              $ adjustPredictor (/maxDensity) "PopPerSqMile"
              predictorMap
  rowMaker <- maybeToRight "Text in given predictors not found in predictors map"
              $ traverse (fmap fst . flip M.lookup pMap) predictors
  let predictRow :: F.Record ElectionDataR -> Vec.Vector Double
      predictRow r = Vec.fromList $ fmap ($ F.rcast r) rowMaker
  return $ FL.fold (FL.premap (electionResultToModelRow $ predictRow . F.rcast) FL.vector) er


ccesDataToModelRow :: (F.Record CCESDataR -> Vec.Vector Double) -> F.Record CCESDataR -> ModelRow
ccesDataToModelRow predictRow r =
  (ModelRow <$> district <*> predictRow <*> F.rgetField @Surveyed <*> tVotes <*> dVotes) $ r where
  dVotes = F.rgetField @DVotes
  tVotes = F.rgetField @TVotes

ccesDataToModelRows :: Foldable f => [Text] -> f (F.Record CCESDataR) -> Either Text (Vec.Vector ModelRow)
ccesDataToModelRows predictors cd = do
  let cd' = F.toFrame $ take 1000 $ FL.fold FL.list cd -- test with way less CCES data
  let ((stateM, _), (cdM, _), maxAvgIncome, maxDensity) =
        FL.fold ((,,,) <$> enumStateF <*> enumDistrictF <*> maxAvgIncomeF <*> maxDensityF) cd
      pMap = adjustPredictor (/maxAvgIncome) "AvgIncome"
             $ adjustPredictor (/maxDensity) "PopPerSqMile"
             predictorMap
  rowMaker <- maybeToRight "Text in given predictors not found in predictors map"
              $ traverse (fmap snd . flip M.lookup pMap) predictors
  let predictRow :: F.Record CCESDataR -> Vec.Vector Double
      predictRow r = Vec.fromList $ fmap ($ F.rcast r) rowMaker
  return $ FL.fold (FL.premap (ccesDataToModelRow $ predictRow . F.rcast) FL.vector) cd


houseDataWrangler :: ModelWith -> [Text] -> HouseDataWrangler
houseDataWrangler mw predictors = SC.Wrangle SC.NoIndex f
  where
    f _ = ((), makeDataJsonE)
    numDataSets :: Int = if mw == UseBoth then 2 else 1
    makeDataJsonE hmd = do
      (modelRows, dataSetIndex, edRows) <- case mw of
        UseElectionResults ->  do
          edModelRows <- electionResultsToModelRows predictors $ electionData hmd
          let dataSetIndex = Vec.replicate (Vec.length edModelRows) (1 :: Int)
          return (edModelRows, dataSetIndex, Vec.length edModelRows)
        UseCCES -> do
          ccesModelRows <- ccesDataToModelRows predictors $ ccesData hmd
          let dataSetIndex = Vec.replicate (Vec.length ccesModelRows) (1 :: Int)
          return (ccesModelRows, dataSetIndex, Vec.length ccesModelRows)
        UseBoth -> do
          edModelRows <- electionResultsToModelRows predictors $ electionData hmd
          ccesModelRows <- ccesDataToModelRows predictors $ ccesData hmd
          let modelRows = edModelRows <> ccesModelRows
              dataSetIndex = Vec.replicate (Vec.length edModelRows) (1 :: Int) <> Vec.replicate (Vec.length ccesModelRows) 2
          return (modelRows, dataSetIndex, Vec.length edModelRows)
      let incumbencyCol = fromMaybe (0 :: Int) $ fmap fst $ find ((== "Incumbency") . snd)$ zip [1..] predictors
          dataF =
            SJ.namedF "G" FL.length
            <> SJ.constDataF "N" edRows
            <> SJ.constDataF "D" numDataSets
            <> SJ.constDataF "K" (length predictors)
            <> SJ.constDataF "IC" incumbencyCol
            <> SJ.valueToPairF "X" (SJ.jsonArrayF pred)
--            <> SJ.valueToPairF "Inc" (SJ.jsonArrayF inc)
            <> SJ.valueToPairF "VAP" (SJ.jsonArrayF vap)
            <> SJ.valueToPairF "TVotes"  (SJ.jsonArrayF tVotes)
            <> SJ.valueToPairF "DVotes"  (SJ.jsonArrayF dVotes)
      modelRowJson <- SJ.frameToStanJSONSeries dataF modelRows
      dataSetIndexJson <- SJ.frameToStanJSONSeries (SJ.valueToPairF "dataSet" (SJ.jsonArrayF id)) dataSetIndex
      return $ modelRowJson <> dataSetIndexJson


data ModelWith = UseElectionResults | UseCCES | UseBoth deriving (Show, Eq, Ord)

type VoteP = "VoteProb" F.:-> Double
type DVoteP = "DVoteProb" F.:-> Double
type EVotes = "EstVotes" F.:-> Int
type EDVotes = "EstDVotes" F.:-> Int
type EDVotes5 = "EstDVotes5" F.:-> Int
type EDVotes95 = "EstDVotes95" F.:-> Int

type Modeled = [VoteP, DVoteP, EVotes, EDVotes5, EDVotes, EDVotes95]

type ElectionFit = [BR.StateAbbreviation, BR.CongressionalDistrict, PUMS.Citizens, TVotes, DVotes] V.++ Modeled
type CCESFit = [BR.StateAbbreviation, BR.CongressionalDistrict, Surveyed, TVotes, DVotes] V.++ Modeled

data HouseModelResults = HouseModelResults { electionFit :: F.FrameRec ElectionFit
                                           , ccesFit :: F.FrameRec CCESFit
                                           , avgProbs :: MapRow.MapRow [Double]
                                           , sigmaDeltas :: MapRow.MapRow [Double]
                                           , unitDeltas :: MapRow.MapRow [Double]
                                           } deriving (Generic)

-- frames are not directly serializable so we have to do...shenanigans.
instance S.Serialize HouseModelResults where
  put (HouseModelResults ef cf aps sd ud) = S.put (FS.SFrame ef, FS.SFrame cf, aps, sd, ud)
  get = (\(ef, cf, aps, sd, ud) -> HouseModelResults (FS.unSFrame ef) (FS.unSFrame cf) aps sd ud) <$> S.get

extractResults ::
  ModelWith
  -> [Text]
  -> CS.StanSummary
  -> HouseModelData
  -> Either T.Text HouseModelResults
extractResults modelWith predictors summary hmd = do
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
                    F.&: (F.rgetField @DVotes edRow + F.rgetField @RVotes edRow)
                    F.&: F.rgetField @DVotes edRow
                    F.&: pV
                    F.&: pD
                    F.&: round etVotes
                    F.&: round d5
                    F.&: round d
                    F.&: round d95
                    F.&: V.RNil
          else Left "Wrong number of percentiles in stan statistic"
      makeCCESFitRow (cdRow, (pV, pD, etVotes, dVotesPcts)) = do
        if length dVotesPcts == 3
          then
            let [d5, d, d95] = dVotesPcts
             in Right $
                  F.rgetField @BR.StateAbbreviation cdRow
                    F.&: F.rgetField @BR.CongressionalDistrict cdRow
                    F.&: F.rgetField @Surveyed cdRow
                    F.&: F.rgetField @TVotes cdRow
                    F.&: F.rgetField @DVotes cdRow
                    F.&: pV
                    F.&: pD
                    F.&: round etVotes
                    F.&: round d5
                    F.&: round d
                    F.&: round d95
                    F.&: V.RNil
          else Left "Wrong number of percentiles in stan statistic"
      ccesIndex = case modelWith of
        UseCCES -> 0
        _ -> FL.fold FL.length (electionData hmd)
  electionFit <- traverse makeElectionFitRow $ Vec.zip (FL.fold FL.vector $ electionData hmd) (Vec.take ccesIndex modeled)
  ccesFit <- traverse makeCCESFitRow $ Vec.zip (FL.fold FL.vector $ ccesData hmd) (Vec.drop ccesIndex modeled)
  -- deltas
  let rowNamesD = (<> "D") <$> predictors
      rowNamesV = (<> "V") <$> predictors
      g :: Either Text (MapRow.MapRow a) -> Either Text (MapRow.MapRow a)
      g x = if null predictors then Right mempty else x
  -- deltas
  sigmaDeltaDMR <- g $ (MapRow.withNames rowNamesD . SP.getVector . fmap CS.percents =<<) $ SP.parse1D "sigmaDeltaD" (CS.paramStats summary)
  sigmaDeltaVMR <- g $ (MapRow.withNames rowNamesV . SP.getVector .fmap CS.percents =<<) $ SP.parse1D "sigmaDeltaV" (CS.paramStats summary)
  unitDeltaDMR <- g $ (MapRow.withNames rowNamesD . SP.getVector . fmap CS.percents =<<) $ SP.parse1D "unitDeltaD" (CS.paramStats summary)
  unitDeltaVMR <- g $ (MapRow.withNames rowNamesV . SP.getVector . fmap CS.percents =<<) $ SP.parse1D "unitDeltaV" (CS.paramStats summary)
  elexPVote <- M.singleton "probV" . SP.getScalar . fmap CS.percents <$> SP.parseScalar "avgPVoted" (CS.paramStats summary)
  elexPDVote <- M.singleton "probD" . SP.getScalar . fmap CS.percents <$> SP.parseScalar "avgPDVote" (CS.paramStats summary)
  let sigmaDeltas = sigmaDeltaDMR <> sigmaDeltaVMR
      unitDeltas = unitDeltaDMR <> unitDeltaVMR
--      avgProbs = _ --elexPVote <> elexPDVote
  return $ HouseModelResults (F.toFrame electionFit) (F.toFrame ccesFit) (elexPVote <> elexPDVote) sigmaDeltas unitDeltas


runHouseModel ::
  forall r.
  (K.KnitEffects r, K.CacheEffectsD r)
  => Bool
  -> [Text]
  -> (Text, Maybe Text, ModelWith, SB.StanModel, Int)
  -> Int
  -> K.ActionWithCacheTime r HouseModelData
  -> K.Sem r (K.ActionWithCacheTime r HouseModelResults, SC.ModelRunnerConfig)
runHouseModel clearCache predictors (modelName, mNameExtra, modelWith, model, nSamples) year houseData_C
  = K.wrapPrefix "BlueRipple.Model.House.ElectionResults.runHouseModel" $ do
  K.logLE K.Info "Running..."
  let workDir = "stan/house/election"
      nameExtra = fromMaybe "" $ fmap ("_" <>) mNameExtra
      dataLabel = show modelWith <> nameExtra <> "_" <> show year
      outputLabel = modelName <> "_" <> dataLabel
  let stancConfig = (SM.makeDefaultStancConfig (T.unpack $ workDir <> "/" <> modelName)) {CS.useOpenCL = False}
  stanConfig <-
    SC.setSigFigs 4
      . SC.noLogOfSummary
      <$> SM.makeDefaultModelRunnerConfig
        workDir
        (modelName <> "_model")
        (Just (SB.All, model))
        (Just $ dataLabel <> ".json")
        (Just $ outputLabel)
        4
        (Just nSamples)
        (Just nSamples)
        (Just stancConfig)
  let resultCacheKey = "house/model/stan/election_" <> outputLabel <> ".bin"
      filterToYear :: (F.ElemOf rs BR.Year, FI.RecVec rs) => F.FrameRec rs -> F.FrameRec rs
      filterToYear = F.filterFrame ((== year) . F.rgetField @BR.Year)
      houseDataForYear_C = fmap (Optics.over #electionData filterToYear . Optics.over #ccesData filterToYear) houseData_C
  modelDep <- SM.modelCacheTime stanConfig
  K.logLE K.Diagnostic $ "modelDep: " <> show (K.cacheTime modelDep)
  K.logLE K.Diagnostic $ "houseDataDep: " <> show (K.cacheTime houseData_C)
  let dataModelDep = const <$> modelDep <*> houseDataForYear_C
      getResults s () inputAndIndex_C = do
        (houseModelData, _) <- K.ignoreCacheTime inputAndIndex_C
        K.knitEither $ extractResults modelWith predictors s houseModelData
      unwraps = [SR.UnwrapNamed "DVotes" "DVotes"
                , SR.UnwrapNamed "TVotes" "TVotes"
                , SR.UnwrapNamed "VAP" "VAP"
                , SR.UnwrapExpr "DVotes/TVotes" "ProbD"
                , SR.UnwrapExpr "TVotes/VAP" "ProbV"
                ]
  when clearCache $ BR.clearIfPresentD resultCacheKey
  res_C <- BR.retrieveOrMakeD resultCacheKey dataModelDep $ \() -> do
    K.logLE K.Info "Data or model newer then last cached result. (Re)-running..."
    SM.runModel
      stanConfig
      (SM.Both unwraps)
      (houseDataWrangler modelWith predictors)
      (SC.UseSummary getResults)
      ()
      houseDataForYear_C
  return (res_C, stanConfig)

binomial :: SB.StanModel
binomial =
  SB.StanModel
    binomialDataBlock
    (Just transformedDataBlock)
    binomialParametersBlock
    (Just binomialTransformedParametersBlock)
    binomialModelBlock
    (Just binomialGeneratedQuantitiesBlock)
    binomialGQLLBlock

betaBinomial_v1 :: SB.StanModel
betaBinomial_v1 =
  SB.StanModel
    binomialDataBlock
    (Just transformedDataBlock)
    betaBinomialParametersBlock
    (Just betaBinomialTransformedParametersBlock)
    betaBinomialModelBlock
    (Just betaBinomialGeneratedQuantitiesBlock)
    betaBinomialGQLLBlock

betaBinomialInc :: SB.StanModel
betaBinomialInc =
  SB.StanModel
  binomialDataBlock
  (Just transformedDataBlock)
  betaBinomialIncParametersBlock
  (Just betaBinomialIncTransformedParametersBlock)
  betaBinomialIncModelBlock
  (Just betaBinomialIncGeneratedQuantitiesBlock)
  betaBinomialGQLLBlock

betaBinomialInc2 :: SB.StanModel
betaBinomialInc2 =
  SB.StanModel
  binomialDataBlock
  (Just transformedDataBlock)
  betaBinomialInc2ParametersBlock
  (Just betaBinomialInc2TransformedParametersBlock)
  betaBinomialInc2ModelBlock
  (Just betaBinomialIncGeneratedQuantitiesBlock)
  betaBinomialInc2GQLLBlock

betaBinomialHS :: SB.StanModel
betaBinomialHS =
  SB.StanModel
    binomialDataBlock
    (Just transformedDataBlock)
    betaBinomialHSParametersBlock
    (Just betaBinomialTransformedParametersBlock)
    betaBinomialHSModelBlock
    (Just betaBinomialGeneratedQuantitiesBlock)
    betaBinomialGQLLBlock

binomialDataBlock :: SB.DataBlock
binomialDataBlock =
  [here|
  int<lower = 1> G; // number of rows
  int<lower = 1> N; // number of rows in data to use for sigma of predictors
  int<lower = 1> D; // number of datasets
  int<lower = 0> IC; // incumbency column, 0 if incumbency is not a predictor
  int<lower = 0> K; // number of predictors
  matrix[G, K] X;
  int<lower=1> dataSet[G];
  int<lower = 0> VAP[G];
  int<lower = 0> TVotes[G];
  int<lower = 0> DVotes[G];
|]

transformedDataBlock :: T.Text
transformedDataBlock = [here|
  vector<lower=0>[K] sigmaPred;
  vector[K] meanPredD;
  vector[K] meanPredV;
  matrix[G, K] X_centered;
  for (k in 1:K) {
    meanPredD[k] = mean(X[,k] .* to_vector(TVotes))/mean(to_vector(TVotes)); // we only want mean of the data for districts. Weighted by votes.
    meanPredV[k] = mean(X[,k] .* to_vector(VAP))/mean(to_vector(VAP)); // we only want mean of the data for districts. Weighted by VAP.
    sigmaPred[k] = sd(X[1:N,k]); // we only want std dev of the data for districts
  }
  if (IC > 0) // if incumbency is present as a predictor, set the "mean" to be non-incumbent
  {
    meanPredD[IC] = 0;
    meanPredV[IC] = 0;
  }
  print("dims(TVotes)=",dims(TVotes));
  print("dims(DVotes)=",dims(DVotes));
  print("dims(X)=",dims(X));

  matrix[G, K] Q_ast;
  matrix[K, K] R_ast;
  matrix[K, K] R_ast_inverse;
  // thin and scale the QR decomposition
  if (K > 0)
    {
      Q_ast = qr_thin_Q(X) * sqrt(G - 1);
      R_ast = qr_thin_R(X) /sqrt(G - 1);
      R_ast_inverse = inverse(R_ast);
    }
|]

binomialParametersBlock :: SB.ParametersBlock
binomialParametersBlock =
  [here|
  vector[D] alphaD;
  vector[K] thetaV;
  vector[D] alphaV;
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
  TVotes ~ binomial_logit(VAP, alphaV[dataSet] + Q_ast * thetaV);
  DVotes ~ binomial_logit(TVotes, alphaD[dataSet] + Q_ast * thetaD);
|]

binomialGeneratedQuantitiesBlock :: SB.GeneratedQuantitiesBlock
binomialGeneratedQuantitiesBlock =
  [here|
  vector<lower = 0, upper = 1>[G] pVotedP = inv_logit(alphaV[dataSet] + Q_ast * thetaV);
  vector<lower = 0, upper = 1>[G] pDVoteP = inv_logit(alphaD[dataSet] + Q_ast * thetaD);

  vector<lower = 0>[G] eTVotes = pVotedP .* to_vector(VAP);
  vector<lower = 0>[G] eDVotes = pDVoteP .* to_vector(TVotes);

  int<lower=0> DVote_ppred[G] = binomial_rng(TVotes, pDVoteP);
  int<lower=0> TVote_ppred[G] = binomial_rng(VAP, pVotedP);

  real avgPVoted = inv_logit (alphaV[1] + dot_product(meanPredV, betaV));
  real avgPDVote = inv_logit (alphaD[1] + dot_product(meanPredD, betaD));

  vector[K] sigmaDeltaV;
  vector[K] sigmaDeltaD;
  vector[K] unitDeltaV;
  vector[K] unitDeltaD;
  for (k in 1:K) {
    sigmaDeltaV [k] = inv_logit (alphaV[1] + meanPredV[k] + sigmaPred[k]/2 * betaV[k]) - inv_logit (alphaV[1] + meanPredV[k] - sigmaPred[k]/2 * betaV[k]);
    sigmaDeltaD [k] = inv_logit (alphaD[1] + meanPredD[k] + sigmaPred[k]/2 * betaD[k]) - inv_logit (alphaD[1] + meanPredD[k] - sigmaPred[k]/2 * betaD[k]);
    unitDeltaV[k] = inv_logit (alphaV[1] + (1-meanPredV[k]) * betaV[k]) - inv_logit (alphaV[1] - meanPredV[k] * betaV[k]);
    unitDeltaD[k] = inv_logit (alphaD[1] + (1-meanPredD[k]) * betaD[k]) - inv_logit (alphaD[1] - meanPredD[k] * betaD[k]);
  }

|]

binomialGQLLBlock :: SB.GeneratedQuantitiesBlock
binomialGQLLBlock =
  [here|
  vector[G] log_lik;
  for (g in 1:G) {
    log_lik[g] =  binomial_logit_lpmf(DVotes[g] | TVotes[g], alphaD[dataSet[g]] + (Q_ast[g] * thetaD));
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

betaBinomialIncParametersBlock ::  SB.ParametersBlock
betaBinomialIncParametersBlock =
  [here|
  vector[D] alphaD;
  vector[D] alphaV;
  vector[K] thetaV;
  vector[K] thetaD;
  real <lower=0, upper=1> dispD;
  real <lower=0, upper=1> dispV;
  |]

betaBinomialIncTransformedParametersBlock :: SB.TransformedParametersBlock
betaBinomialIncTransformedParametersBlock =
  [here|
  real<lower=0> phiV = dispV/(1-dispV);
  real<lower=0> phiD = dispD/(1-dispD);
  vector<lower=0, upper=1> [G] pDVoteP = inv_logit (alphaD[dataSet] + Q_ast * thetaD);
  vector<lower=0, upper=1> [G] pVotedP = inv_logit (alphaV[dataSet] + Q_ast * thetaV);
  vector[K] betaV;
  vector[K] betaD;
  betaV = R_ast_inverse * thetaV;
  betaD = R_ast_inverse * thetaD;
|]

betaBinomialIncModelBlock :: SB.ModelBlock
betaBinomialIncModelBlock =
  [here|
  alphaD ~ cauchy(0, 10);
  alphaV ~ cauchy(0, 10);
  betaV ~ cauchy(0, 10);
  betaD ~ cauchy(0, 10);
  TVotes ~ beta_binomial(VAP, pVotedP * phiV, (1 - pVotedP) * phiV);
  DVotes ~ beta_binomial(TVotes, pDVoteP * phiD, (1 - pDVoteP) * phiD);
|]

betaBinomialIncGeneratedQuantitiesBlock :: SB.GeneratedQuantitiesBlock
betaBinomialIncGeneratedQuantitiesBlock =
  [here|
  vector<lower = 0>[G] eTVotes;
  vector<lower = 0>[G] eDVotes;
  int<lower=0> DVote_ppred[G];
  int<lower=0> TVote_ppred[G];
  for (g in 1:G) {
    eTVotes[g] = pVotedP[g] * VAP[g];
    eDVotes[g] = pDVoteP[g] * TVotes[g];
    TVote_ppred[g] = beta_binomial_rng(VAP[g], pVotedP[g] * phiV, (1 - pVotedP[g]) * phiV);
    DVote_ppred[g] = beta_binomial_rng(TVotes[g], pDVoteP[g] * phiD, (1 - pDVoteP[g]) * phiD);
  }
  real avgPVoted = inv_logit (alphaV[1] + dot_product(meanPredV, betaV));
  real avgPDVote = inv_logit (alphaD[1] + dot_product(meanPredD, betaD));
  vector[K] sigmaDeltaV;
  vector[K] sigmaDeltaD;
  vector[K] unitDeltaV;
  vector[K] unitDeltaD;
  for (k in 1:K) {
    sigmaDeltaV [k] = inv_logit (alphaV[1] + meanPredV[k] + sigmaPred[k]/2 * betaV[k]) - inv_logit (alphaV[1] + meanPredV[k] - sigmaPred[k]/2 * betaV[k]);
    sigmaDeltaD [k] = inv_logit (alphaD[1] + meanPredD[k] + sigmaPred[k]/2 * betaD[k]) - inv_logit (alphaD[1] + meanPredD[k] - sigmaPred[k]/2 * betaD[k]);
    unitDeltaV[k] = inv_logit (alphaV[1] + (1-meanPredV[k]) * betaV[k]) - inv_logit (alphaV[1] - meanPredV[k] * betaV[k]);
    unitDeltaD[k] = inv_logit (alphaD[1] + (1-meanPredD[k]) * betaD[k]) - inv_logit (alphaD[1] - meanPredD[k] * betaD[k]);
  }
|]

betaBinomialInc2ParametersBlock :: SB.ParametersBlock
betaBinomialInc2ParametersBlock = [here|
  vector[D] alphaD;
  vector[D] alphaV;
  vector[K] thetaV;
  vector[K] thetaD;
  real <lower=0, upper=1> dispD;
  real <lower=0, upper=1> dispV;
  vector<lower=0, upper=1>[K] gammaD;
  vector<lower=0, upper=1>[K] gammaV;
|]

betaBinomialInc2TransformedParametersBlock :: SB.TransformedParametersBlock
betaBinomialInc2TransformedParametersBlock = [here|
  vector<lower=0>[G] phiV = dispV/(1-dispV) + Q_ast * gammaD;
  vector<lower=0>[G] phiD = dispD/(1-dispD) + Q_ast * gammaV;
  vector<lower=0, upper=1> [G] pDVoteP = inv_logit (alphaD[dataSet] + Q_ast * thetaD);
  vector<lower=0, upper=1> [G] pVotedP = inv_logit (alphaV[dataSet] + Q_ast * thetaV);
  vector[K] betaV;
  vector[K] betaD;
  betaV = R_ast_inverse * thetaV;
  betaD = R_ast_inverse * thetaD;
  vector[K] etaV;
  vector[K] etaD;
  etaV = R_ast_inverse * gammaV;
  etaD = R_ast_inverse * gammaD;
|]

betaBinomialInc2ModelBlock :: SB.ModelBlock
betaBinomialInc2ModelBlock =
  [here|
  alphaD ~ cauchy(0, 10);
  alphaV ~ cauchy(0, 10);
  betaV ~ cauchy(0, 10);
  betaD ~ cauchy(0, 10);
  TVotes ~ beta_binomial(VAP, pVotedP .* phiV, (1 - pVotedP) .* phiV);
  DVotes ~ beta_binomial(TVotes, pDVoteP .* phiD, (1 - pDVoteP) .* phiD);
|]

betaBinomialInc2GQLLBlock :: SB.GeneratedQuantitiesBlock
betaBinomialInc2GQLLBlock =
  [here|
  vector[G] log_lik;
  for (g in 1:G) {
    log_lik[g] =  beta_binomial_lpmf(DVotes[g] | TVotes[g], pDVoteP[g] * phiD[g], (1 - pDVoteP[g]) * phiD[g]) ;
  }
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

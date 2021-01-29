{-# LANGUAGE AllowAmbiguousTypes #-}
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
import qualified Data.List as List
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

type StateKeyR = [BR.Year, BR.StateAbbreviation]
type CDKeyR = StateKeyR V.++ '[BR.CongressionalDistrict]

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
type HouseElectionDataR = CDKeyR V.++ DemographicsR V.++ ElectionR
type HouseElectionData = F.FrameRec HouseElectionDataR

type SenateElectionDataR = SenateRaceKeyR V.++ DemographicsR V.++ ElectionR
type SenateElectionData = F.FrameRec SenateElectionDataR

type PresidentialElectionDataR = StateKeyR V.++ DemographicsR V.++ ElectionR
type PresidentialElectionData  = F.FrameRec PresidentialElectionDataR

-- CCES data
type Surveyed = "Surveyed" F.:-> Int -- total people in each bucket
type CCESByCD = CDKeyR V.++ [DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.Race5C, DT.HispC, Surveyed, TVotes, DVotes]
type CCESDataR = CCESByCD V.++ [Incumbency, DT.AvgIncome, DT.PopPerSqMile]
type CCESPredictorR = [DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.Race5C, DT.HispC, DT.AvgIncome, DT.PopPerSqMile]
type CCESData = F.FrameRec CCESDataR

data HouseModelData = HouseModelData { houseElectionData :: HouseElectionData
                                     , senateElectionData :: SenateElectionData
                                     , presidentialElectionData :: PresidentialElectionData
                                     , ccesData :: CCESData
                                     } deriving (Generic)

houseRaceKey :: F.Record HouseElectionDataR -> F.Record CDKeyR
houseRaceKey = F.rcast

senateRaceKey :: F.Record SenateElectionDataR -> F.Record SenateRaceKeyR
senateRaceKey = F.rcast

presidentialRaceKey :: F.Record PresidentialElectionDataR -> F.Record StateKeyR
presidentialRaceKey = F.rcast

-- frames are not directly serializable so we have to do...shenanigans
instance S.Serialize HouseModelData where
  put (HouseModelData h s p c) = S.put (FS.SFrame h, FS.SFrame s, FS.SFrame p, FS.SFrame c)
  get = (\(h, s, p, c) -> HouseModelData (FS.unSFrame h) (FS.unSFrame s) (FS.unSFrame p) (FS.unSFrame c)) <$> S.get

type PUMSDataR = [DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.RaceAlone4C, DT.HispC, DT.AvgIncome, DT.PopPerSqMile, PUMS.Citizens, PUMS.NonCitizens]

pumsMR :: forall ks f m.(Foldable f
                        , Ord (F.Record ks)
                        , FI.RecVec (ks V.++ DemographicsR)
                        , ks F.⊆ (ks V.++ PUMSDataR)
                        , F.ElemOf (ks V.++ PUMSDataR) DT.AvgIncome
                        , F.ElemOf (ks V.++ PUMSDataR) PUMS.Citizens
                        , F.ElemOf (ks V.++ PUMSDataR) DT.CollegeGradC
                        , F.ElemOf (ks V.++ PUMSDataR) DT.HispC
                        , F.ElemOf (ks V.++ PUMSDataR) PUMS.NonCitizens
                        , F.ElemOf (ks V.++ PUMSDataR) DT.PopPerSqMile
                        , F.ElemOf (ks V.++ PUMSDataR) DT.RaceAlone4C
                        , F.ElemOf (ks V.++ PUMSDataR) DT.SexC
                        , F.ElemOf (ks V.++ PUMSDataR) DT.SimpleAgeC
                        )
       => f (F.Record (ks V.++ PUMSDataR))
       -> (F.FrameRec (ks V.++ DemographicsR))
pumsMR = runIdentity
         . BRF.frameCompactMRM
         FMR.noUnpack
         (FMR.assignKeysAndData @ks)
         pumsDataF

pumsDataF ::
  FL.Fold
    (F.Record PUMSDataR)
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
      black r = race4A r == DT.RA4_Black
      asian r = race4A r == DT.RA4_Asian
      other r = race4A r == DT.RA4_Other
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

electionF :: forall ks.(Ord (F.Record ks)
                       , ks F.⊆ (ks V.++ '[BR.Candidate, ET.Party, ET.Votes, ET.Incumbent])
                       , F.ElemOf (ks V.++ '[BR.Candidate, ET.Party, ET.Votes, ET.Incumbent]) ET.Incumbent
                       , F.ElemOf (ks V.++ '[BR.Candidate, ET.Party, ET.Votes, ET.Incumbent]) ET.Party
                       , F.ElemOf (ks V.++ '[BR.Candidate, ET.Party, ET.Votes, ET.Incumbent]) ET.Votes
                       , F.ElemOf (ks V.++ '[BR.Candidate, ET.Party, ET.Votes, ET.Incumbent]) BR.Candidate
                       , FI.RecVec (ks V.++ ElectionR)
                       )
          => FL.FoldM (Either T.Text) (F.Record (ks V.++ [BR.Candidate, ET.Party, ET.Votes, ET.Incumbent])) (F.FrameRec (ks V.++ ElectionR))
electionF =
  FMR.concatFoldM $
    FMR.mapReduceFoldM
      (FMR.generalizeUnpack FMR.noUnpack)
      (FMR.generalizeAssign $ FMR.assignKeysAndData @ks)
      (FMR.makeRecsWithKeyM id $ FMR.ReduceFoldM $ const $ fmap (pure @[]) flattenVotesF)

data IncParty = None | Inc (ET.PartyT, Text) | Multi [Text]

updateIncParty :: IncParty -> (ET.PartyT, Text) -> IncParty
updateIncParty (Multi cs) (_, c) = Multi (c:cs)
updateIncParty (Inc (_, c)) (_, c') = Multi [c, c']
updateIncParty None (p, c) = Inc (p, c)

incPartyToInt :: IncParty -> Either T.Text Int
incPartyToInt None = Right 0
incPartyToInt (Inc (ET.Democratic, _)) = Right 1
incPartyToInt (Inc (ET.Republican, _)) = Right (negate 1)
incPartyToInt (Inc _) = Right 0
incPartyToInt (Multi cs) = Left $ "Error: Multiple incumbents: " <> T.intercalate "," cs

flattenVotesF :: FL.FoldM (Either T.Text) (F.Record [BR.Candidate, ET.Incumbent, ET.Party, ET.Votes]) (F.Record ElectionR)
flattenVotesF = FMR.postMapM (FL.foldM flattenF) aggregatePartiesF
  where
    party = F.rgetField @ET.Party
    votes = F.rgetField @ET.Votes
    incumbentPartyF =
      FMR.postMapM incPartyToInt $
        FL.generalize $
          FL.prefilter (F.rgetField @ET.Incumbent) $
            FL.premap (\r -> (F.rgetField @ET.Party r, F.rgetField @BR.Candidate r)) (FL.Fold updateIncParty None id)
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

type SenateRaceKeyR = [BR.Year, BR.StateAbbreviation, BR.Special, BR.Stage]

type ElexDataR = [ET.Office, BR.Stage, BR.Runoff, BR.Special, BR.Candidate, ET.Party, ET.Votes, ET.Incumbent]

prepCachedData ::forall r.
  (K.KnitEffects r, K.CacheEffectsD r) => Bool -> K.Sem r (K.ActionWithCacheTime r HouseModelData)
prepCachedData clearCache = do
  pums_C <- PUMS.pumsLoaderAdults
  cdFromPUMA_C <- BR.allCDFromPUMA2012Loader
  let pumsByCDDeps = (,) <$> pums_C <*> cdFromPUMA_C
      pumsByCD :: F.FrameRec PUMS.PUMS -> F.FrameRec BR.DatedCDFromPUMA2012 -> K.Sem r (F.FrameRec (CDKeyR V.++ PUMSDataR))
      pumsByCD pums cdFromPUMA =  fmap F.rcast <$> PUMS.pumsCDRollup ((>= 2012) . F.rgetField @BR.Year) (pumsReKey . F.rcast) cdFromPUMA pums
  pumsByCD_C <- BR.retrieveOrMakeFrame "model/house/pumsByCD.bin" pumsByCDDeps $ \(pums, cdFromPUMA) -> pumsByCD pums cdFromPUMA

  let pumsByState :: F.FrameRec PUMS.PUMS -> F.FrameRec (StateKeyR V.++ PUMSDataR)
      pumsByState = fmap F.rcast
                    . FL.fold
                    (FL.prefilter ((>= 2012) . F.rgetField @BR.Year)
                      $ PUMS.pumsStateRollupF (pumsReKey . F.rcast)
                    )

  pumsByState_C <- BR.retrieveOrMakeFrame "model/house/pumsByState.bin" pums_C (return . pumsByState)

  houseElections_C <- BR.houseElectionsWithIncumbency
  senateElections_C <- BR.senateElectionsWithIncumbency
  presByStateElections_C <- BR.presidentialElectionsWithIncumbency
--  K.logLE K.Info $ "Senate "
--  K.ignoreCacheTime senateElection_C >>= BR.logFrame -- . F.filterFrame ((> 2010) . F.rgetField @BR.Year)
  countedCCES_C <- fmap (BR.fixAtLargeDistricts 0) <$> ccesCountedDemHouseVotesByCD
  let houseDataDeps = (,,,,,) <$> pumsByCD_C <*> pumsByState_C <*> houseElections_C <*> senateElections_C <*> presByStateElections_C <*> countedCCES_C
  when clearCache $ BR.clearIfPresentD "model/house/houseData.bin"
  BR.retrieveOrMakeD "model/house/houseData.bin" houseDataDeps $ \(pumsByCD, pumsByState, houseElex, senateElex, presElex, countedCCES) -> do
    K.logLE K.Info "ElectionData for election model out of date/unbuilt.  Loading demographic and election data and joining."
    let cdDemographics = pumsMR @CDKeyR pumsByCD
        stateDemographics = pumsMR @StateKeyR pumsByState
        isYear year = (== year) . F.rgetField @BR.Year
        afterYear year = (>= year) . F.rgetField @BR.Year
        betweenYears earliest latest r = let y = F.rgetField @BR.Year r in y >= earliest && y <= latest
        dVotes = F.rgetField @DVotes
        rVotes = F.rgetField @RVotes
        competitive r = dVotes r > 0 && rVotes r > 0
        competitiveIn y r = isYear y r && competitive r
        competitiveAfter y r = afterYear y r && competitive r
        hasVoters r = F.rgetField @Surveyed r > 0
        hasVotes r = F.rgetField @TVotes r > 0
        hasDVotes r = F.rgetField @DVotes r > 0
        fHouseElex :: FL.FoldM (Either Text) (F.Record BR.HouseElectionColsI) (F.FrameRec (CDKeyR V.++ ElectionR))
        fHouseElex = FL.prefilterM (return . afterYear 2012) $ FL.premapM (return . F.rcast) $ electionF @CDKeyR
        fSenateElex :: FL.FoldM (Either Text) (F.Record BR.SenateElectionColsI) (F.FrameRec (SenateRaceKeyR V.++ ElectionR))
        fSenateElex = FL.prefilterM (return . betweenYears 2012 2018) $ FL.premapM (return . F.rcast) $ electionF @SenateRaceKeyR
        fPresElex :: FL.FoldM (Either Text) (F.Record BR.PresidentialElectionColsI) (F.FrameRec (StateKeyR V.++ ElectionR))
        fPresElex = FL.prefilterM (return . afterYear 2012) $ FL.premapM (return . F.rcast) $ electionF @StateKeyR

    houseElectionResults <- K.knitEither $ FL.foldM fHouseElex houseElex
    senateElectionResults <- K.knitEither $ FL.foldM fSenateElex senateElex
    presElectionResults <- K.knitEither $ FL.foldM fPresElex presElex

    let (houseDemoAndElex, missinghElex) = FJ.leftJoinWithMissing @CDKeyR houseElectionResults cdDemographics
    K.knitEither $ if null missinghElex
                   then Right ()
                   else Left $ "Missing keys in left-join of demographics and house election data in house model prep:"
                        <> show missinghElex
    let competitiveHouseElectionResults = F.rcast <$> F.filterFrame competitive houseDemoAndElex
        competitiveCDs = FL.fold (FL.premap (F.rcast @CDKeyR) FL.set) houseDemoAndElex
        competitiveCCES = F.filterFrame (\r -> Set.member (F.rcast @CDKeyR r) competitiveCDs) countedCCES
        toJoinWithCCES = fmap (F.rcast @(CDKeyR V.++ [Incumbency, DT.AvgIncome, DT.PopPerSqMile])) competitiveHouseElectionResults
        (ccesWithDD, missingDemo) = FJ.leftJoinWithMissing @CDKeyR toJoinWithCCES competitiveCCES --toJoinWithCCES
    K.knitEither $ if null missingDemo
                   then Right ()
                   else Left $ "Missing keys in left-join of ccesByCD and demographic data in house model prep:"
                        <> show missingDemo
    let ccesWithoutNullVotes = F.filterFrame (\r -> hasVoters r && hasVotes r) ccesWithDD -- ICK.  This will bias our turnout model
    let (senateDemoAndElex, missingsElex) = FJ.leftJoinWithMissing @StateKeyR senateElectionResults stateDemographics
    K.knitEither $ if null missingsElex
                   then Right ()
                   else Left $ "Missing keys in left-join of demographics and senate election data in house model prep:"
                        <> show missingsElex
    let (presDemoAndElex, missingpElex) = FJ.leftJoinWithMissing @StateKeyR presElectionResults stateDemographics
    K.knitEither $ if null missingpElex
                   then Right ()
                   else Left $ "Missing keys in left-join of demographics and presidential election data in house model prep:"
                        <> show missingpElex


    return $ HouseModelData
      competitiveHouseElectionResults
      (F.rcast <$> F.filterFrame competitive senateDemoAndElex)
      (F.rcast <$> F.filterFrame competitive presDemoAndElex)
      (fmap F.rcast ccesWithoutNullVotes)

type HouseDataWrangler = SC.DataWrangler HouseModelData  () ()

district r = F.rgetField @BR.StateAbbreviation r <> show (F.rgetField @BR.CongressionalDistrict r)

enumStateF = FL.premap (F.rgetField @BR.StateAbbreviation) (SJ.enumerate 1)
enumDistrictF = FL.premap district (SJ.enumerate 1)
maxAvgIncomeF = fromMaybe 1 <$> FL.premap (F.rgetField @DT.AvgIncome) FL.maximum
maxDensityF = fromMaybe 1 <$> FL.premap (F.rgetField @DT.PopPerSqMile) FL.maximum

type PredictorMap = Map Text (F.Record (ElectionPredictorR V.++ ElectionR) -> Double, F.Record (CCESDataR V.++ [DVotes, TVotes]) -> Double)

ccesWNH r = (F.rgetField @DT.Race5C r == DT.R5_WhiteNonLatinx) && (F.rgetField @DT.HispC r == DT.NonHispanic)
ccesWH r = (F.rgetField @DT.Race5C r == DT.R5_WhiteNonLatinx) && (F.rgetField @DT.HispC r == DT.Hispanic)
ccesW r = ccesWNH r || ccesWH r
ccesNWH r = (F.rgetField @DT.Race5C r /= DT.R5_WhiteNonLatinx) && (F.rgetField @DT.HispC r == DT.Hispanic)
ccesH r = ccesWH r || ccesNWH r
ccesHWFrac = ccesWH

pumsWhite r = F.rgetField @FracWhiteNonHispanic r + F.rgetField @FracWhiteHispanic r
pumsWhiteNH r = F.rgetField @FracWhiteNonHispanic r
pumsWhiteH = F.rgetField @FracWhiteHispanic
pumsHispanic r = F.rgetField @FracWhiteHispanic r + F.rgetField @FracNonWhiteHispanic r
pumsHispanicWhiteFraction r = pumsWhiteH r/pumsHispanic r

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
                ,("PctWhiteHispanic", (F.rgetField @FracWhiteHispanic, boolToNumber . ccesWH))
                ,("PctNonWhiteHispanic", (F.rgetField @FracNonWhiteHispanic, boolToNumber . ccesNWH))
                ,("HispanicWhiteFraction",(pumsHispanicWhiteFraction, boolToNumber . ccesHWFrac))
                ,("PctHispanic", (pumsHispanic, boolToNumber . ccesH))
                ,("PctBlack", (F.rgetField @FracBlack, boolToNumber . (== DT.R5_Black) . F.rgetField @DT.Race5C))
                ,("PctAsian", (F.rgetField @FracAsian, boolToNumber . (== DT.R5_Asian) . F.rgetField @DT.Race5C))
                ,("PctOther", (F.rgetField @FracOther, boolToNumber . (== DT.R5_Other) . F.rgetField @DT.Race5C))
                ,("AvgIncome",(F.rgetField @DT.AvgIncome, F.rgetField @DT.AvgIncome))
                ,("PopPerSqMile",(F.rgetField @DT.PopPerSqMile, F.rgetField @DT.PopPerSqMile))
                ,("Incumbency",(realToFrac . F.rgetField @Incumbency, realToFrac . F.rgetField @Incumbency))
                ]

adjustPredictor :: (Double -> Double) -> Text -> PredictorMap -> PredictorMap
adjustPredictor f k =
  let g (h1, h2) = (f . h1, f . h2)
  in M.adjust g k

data ModelRow = ModelRow { stateAbbr :: Text
                         , pred :: Vec.Vector Double
                         , vap :: Int
                         , tVotes :: Int
                         , dVotes :: Int
                         } deriving (Show)


electionResultToModelRow :: (F.Record (DemographicsR V.++ ElectionR) -> Vec.Vector Double)
                         -> F.Record (BR.StateAbbreviation  ': (DemographicsR V.++ ElectionR))
                         -> ModelRow
electionResultToModelRow predictRow r =
  (ModelRow <$> F.rgetField @BR.StateAbbreviation  <*> (predictRow . F.rcast) <*> F.rgetField @PUMS.Citizens <*> tVotes <*> dVotes) $ r where
  dVotes = F.rgetField @DVotes
  tVotes r = dVotes r + F.rgetField @RVotes r

electionResultsToModelRows :: Foldable f
                           => [Text]
                           -> f (F.Record (BR.StateAbbreviation ': (DemographicsR V.++ ElectionR)))
                           -> Either Text (Vec.Vector ModelRow)
electionResultsToModelRows predictors er = do
  let  ((stateM, _), maxAvgIncome, maxDensity) =
         FL.fold
         ( (,,)
           <$> enumStateF
           <*> maxAvgIncomeF
           <*> maxDensityF
         )
         er
       pMap = adjustPredictor (/maxAvgIncome) "AvgIncome"
              $ adjustPredictor (/maxDensity) "PopPerSqMile"
              predictorMap
  rowMaker <- maybeToRight "Text in given predictors not found in predictors map"
              $ traverse (fmap fst . flip M.lookup pMap) predictors
  let predictRow :: F.Record (DemographicsR V.++ ElectionR) -> Vec.Vector Double
      predictRow r = Vec.fromList $ fmap ($ F.rcast r) rowMaker
  return $ FL.fold (FL.premap (electionResultToModelRow predictRow) FL.vector) er


ccesDataToModelRow :: (F.Record CCESDataR -> Vec.Vector Double) -> F.Record CCESDataR -> ModelRow
ccesDataToModelRow predictRow r =
  (ModelRow <$> F.rgetField @BR.StateAbbreviation <*> predictRow <*> F.rgetField @Surveyed <*> tVotes <*> dVotes) $ r where
  dVotes = F.rgetField @DVotes
  tVotes = F.rgetField @TVotes

ccesDataToModelRows :: Foldable f => [Text] -> f (F.Record CCESDataR) -> Either Text (Vec.Vector ModelRow)
ccesDataToModelRows predictors cd = do
  let cd' = F.toFrame $ take 1000 $ FL.fold FL.list cd -- test with way less CCES data
  let ((stateM, _), maxAvgIncome, maxDensity) =
        FL.fold ((,,) <$> enumStateF <*> maxAvgIncomeF <*> maxDensityF) cd
      pMap = adjustPredictor (/maxAvgIncome) "AvgIncome"
             $ adjustPredictor (/maxDensity) "PopPerSqMile"
             predictorMap
  rowMaker <- maybeToRight "Text in given predictors not found in predictors map"
              $ traverse (fmap snd . flip M.lookup pMap) predictors
  let predictRow :: F.Record CCESDataR -> Vec.Vector Double
      predictRow r = Vec.fromList $ fmap ($ F.rcast r) rowMaker
  return $ FL.fold (FL.premap (ccesDataToModelRow predictRow) FL.vector) cd

dataSetToModelRows :: [Text] -> HouseModelData -> DataSet -> Either Text (Vec.Vector ModelRow)
dataSetToModelRows predictors hmd HouseE =  electionResultsToModelRows predictors
                                           $ fmap F.rcast $ houseElectionData hmd
dataSetToModelRows predictors hmd SenateE =  electionResultsToModelRows predictors
                                            $ fmap F.rcast $ senateElectionData hmd
dataSetToModelRows predictors hmd PresidentialE =  electionResultsToModelRows predictors
                                                  $ fmap F.rcast $ presidentialElectionData hmd
dataSetToModelRows predictors hmd CCES =  ccesDataToModelRows predictors
                                         $ fmap F.rcast $ ccesData hmd


dataSetRows :: HouseModelData -> DataSet -> Int
dataSetRows hmd HouseE = FL.fold FL.length $ houseElectionData hmd
dataSetRows hmd SenateE = FL.fold FL.length $ senateElectionData hmd
dataSetRows hmd PresidentialE = FL.fold FL.length $ presidentialElectionData hmd
dataSetRows hmd CCES = FL.fold FL.length $ ccesData hmd

houseDataWrangler :: ModeledDataSets -> DataSet -> [Text] -> HouseDataWrangler
houseDataWrangler mds cds predictors = SC.Wrangle SC.NoIndex f
  where
    f _ = ((), makeDataJsonE)
    numDataSets :: Int = Set.size mds --if mw == UseBoth then 2 else 1
    makeDataJsonE hmd = do
      -- 0 based index of cds set in Set.toAscList mds
      cdsIndex <- if Set.member cds mds
                  then Right $ Set.findIndex cds mds
                  else Left $ "Chosen data set (" <> show cds <> ") mising from modeledDataSets (" <> show mds <> ")"
      lModelRows <- traverse (dataSetToModelRows predictors hmd) $ Set.toAscList mds
      let lRowCounts = fmap Vec.length lModelRows
          modelRows = mconcat lModelRows
          dataSetIndex = mconcat $ fmap (uncurry Vec.replicate) $ zip lRowCounts [(1 :: Int)..]
          incumbencyCol = fromMaybe (0 :: Int) $ fmap fst $ find ((== "Incumbency") . snd)$ zip [1..] predictors
          dataF =
            SJ.namedF "G" FL.length
            <> SJ.constDataF "D" numDataSets
            <> SJ.constDataF "CD" (cdsIndex + 1)
            <> SJ.constDataF "K" (length predictors)
            <> SJ.constDataF "IC" incumbencyCol
            <> SJ.valueToPairF "X" (SJ.jsonArrayF pred)
            <> SJ.valueToPairF "VAP" (SJ.jsonArrayF vap)
            <> SJ.valueToPairF "TVotes"  (SJ.jsonArrayF tVotes)
            <> SJ.valueToPairF "DVotes"  (SJ.jsonArrayF dVotes)
      modelRowJson <- SJ.frameToStanJSONSeries dataF modelRows
      dataSetIndexJson <- SJ.frameToStanJSONSeries (SJ.valueToPairF "dataSet" (SJ.jsonArrayF id)) dataSetIndex
      return $ modelRowJson <> dataSetIndexJson


data ModelWith = UseElectionResults | UseCCES | UseBoth deriving (Show, Eq, Ord)
data DataSet = HouseE | SenateE | PresidentialE | CCES deriving (Show, Eq, Ord, Enum, Bounded)

dataSetKey :: DataSet -> Text
dataSetKey HouseE = "H"
dataSetKey SenateE = "S"
dataSetKey PresidentialE = "P"
dataSetKey CCES = "C"

type ModeledDataSets = Set DataSet

modeledDataSetsKey :: ModeledDataSets -> Text
modeledDataSetsKey mds = mconcat $ fmap dataSetKey $ Set.toAscList mds

type VoteP = "VoteProb" F.:-> Double
type DVoteP = "DVoteProb" F.:-> Double
type EVotes = "EstVotes" F.:-> Int
type EDVotes = "EstDVotes" F.:-> Int
type EDVotes5 = "EstDVotes5" F.:-> Int
type EDVotes95 = "EstDVotes95" F.:-> Int

type Modeled = [VoteP, DVoteP, EVotes, EDVotes5, EDVotes, EDVotes95]

type ElectionFitR as = as V.++ [PUMS.Citizens, TVotes, DVotes] V.++ Modeled
type HouseElectionFitR = ElectionFitR CDKeyR
type SenateElectionFitR = ElectionFitR SenateRaceKeyR
type PresidentialElectionFitR = ElectionFitR StateKeyR

type CCESFit = [BR.StateAbbreviation, BR.CongressionalDistrict, Surveyed, TVotes, DVotes] V.++ Modeled

data HouseModelResults = HouseModelResults { houseElectionFit :: F.FrameRec HouseElectionFitR
                                           , senateElectionFit :: F.FrameRec SenateElectionFitR
                                           , presidentialElectionFit :: F.FrameRec PresidentialElectionFitR
                                           , ccesFit :: F.FrameRec CCESFit
                                           , avgProbs :: MapRow.MapRow [Double]
                                           , sigmaDeltas :: MapRow.MapRow [Double]
                                           , unitDeltas :: MapRow.MapRow [Double]
                                           } deriving (Generic)

-- frames are not directly serializable so we have to do...shenanigans.
instance S.Serialize HouseModelResults where
  put (HouseModelResults hef sef pef cf aps sd ud) =
    S.put (FS.SFrame hef, FS.SFrame sef, FS.SFrame pef, FS.SFrame cf, aps, sd, ud)
  get = (\(hef, sef, pef, cf, aps, sd, ud) -> HouseModelResults
                                              (FS.unSFrame hef)
                                              (FS.unSFrame sef)
                                              (FS.unSFrame pef)
                                              (FS.unSFrame cf)
                                              aps
                                              sd
                                              ud) <$> S.get

extractResults ::
  ModeledDataSets
  -> [Text]
  -> CS.StanSummary
  -> HouseModelData
  -> Either T.Text HouseModelResults
extractResults mds predictors summary hmd = do
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
      makeElectionFitRow key (edRow, (pV, pD, etVotes, dVotesPcts)) = do
        if length dVotesPcts == 3
          then
            let [d5, d, d95] = dVotesPcts
             in Right $ key edRow `V.rappend` (
              F.rgetField @PUMS.Citizens edRow
              F.&: (F.rgetField @DVotes edRow + F.rgetField @RVotes edRow)
              F.&: F.rgetField @DVotes edRow
              F.&: pV
              F.&: pD
              F.&: round etVotes
              F.&: round d5
              F.&: round d
              F.&: round d95
              F.&: V.RNil
              )
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
      lMDS = Set.toAscList mds
      lDSLengths = fmap (dataSetRows hmd) lMDS
      lDSStartLength  = let x = List.scanl1 (+) lDSLengths in zip (0 : x) lDSLengths
      mDSIndices = M.fromList $ zip lMDS lDSStartLength
  houseElectionFit <- case M.lookup HouseE mDSIndices of
    Nothing -> Right mempty
    Just (start, length) -> traverse (makeElectionFitRow houseRaceKey)
                            $ Vec.zip (FL.fold FL.vector $ houseElectionData hmd)
                            (Vec.take length . Vec.drop start $ modeled)
  senateElectionFit <- case M.lookup SenateE mDSIndices of
    Nothing -> Right mempty
    Just (start, length) -> traverse (makeElectionFitRow senateRaceKey)
                            $ Vec.zip (FL.fold FL.vector $ senateElectionData hmd)
                            (Vec.take length . Vec.drop start $ modeled)
  presidentialElectionFit <- case M.lookup SenateE mDSIndices of
    Nothing -> Right mempty
    Just (start, length) -> traverse (makeElectionFitRow presidentialRaceKey)
                             $ Vec.zip (FL.fold FL.vector $ presidentialElectionData hmd)
                             (Vec.take length . Vec.drop start $ modeled)
  ccesFit <- case M.lookup CCES mDSIndices of
    Nothing -> Right mempty
    Just (start, length) -> traverse makeCCESFitRow
                            $ Vec.zip (FL.fold FL.vector $ ccesData hmd)
                            (Vec.take length . Vec.drop start $ modeled)
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
  return $ HouseModelResults
    (F.toFrame houseElectionFit)
    (F.toFrame senateElectionFit)
    (F.toFrame presidentialElectionFit)
    (F.toFrame ccesFit)
    (elexPVote <> elexPDVote)
    sigmaDeltas
    unitDeltas


runHouseModel ::
  forall r.
  (K.KnitEffects r, K.CacheEffectsD r)
  => Bool
  -> [Text]
  -> (Text, Maybe Text, ModeledDataSets, DataSet, SB.StanModel, Int)
  -> Int
  -> K.ActionWithCacheTime r HouseModelData
  -> K.Sem r (K.ActionWithCacheTime r HouseModelResults, SC.ModelRunnerConfig)
runHouseModel clearCache predictors (modelName, mNameExtra, mds, cds, model, nSamples) year houseData_C
  = K.wrapPrefix "BlueRipple.Model.House.ElectionResults.runHouseModel" $ do
  K.logLE K.Info "Running..."
  let workDir = "stan/house/election"
      nameExtra = fromMaybe "" $ fmap ("_" <>) mNameExtra
      dataLabel = modeledDataSetsKey mds <> nameExtra <> "_" <> show year
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
  when clearCache $ do
    K.liftKnit $ SM.deleteStaleFiles stanConfig [SM.StaleData]
    BR.clearIfPresentD resultCacheKey
  let  filterToYear :: (F.ElemOf rs BR.Year, FI.RecVec rs) => F.FrameRec rs -> F.FrameRec rs
       filterToYear = F.filterFrame ((== year) . F.rgetField @BR.Year)
       houseDataForYear_C = fmap (Optics.over #houseElectionData filterToYear . Optics.over #ccesData filterToYear) houseData_C
  modelDep <- SM.modelCacheTime stanConfig
  K.logLE K.Diagnostic $ "modelDep: " <> show (K.cacheTime modelDep)
  K.logLE K.Diagnostic $ "houseDataDep: " <> show (K.cacheTime houseData_C)
  let dataModelDep = const <$> modelDep <*> houseDataForYear_C
      getResults s () inputAndIndex_C = do
        (houseModelData, _) <- K.ignoreCacheTime inputAndIndex_C
        K.knitEither $ extractResults mds predictors s houseModelData
      unwraps = [SR.UnwrapNamed "DVotes" "DVotes"
                , SR.UnwrapNamed "TVotes" "TVotes"
                , SR.UnwrapNamed "VAP" "VAP"
                , SR.UnwrapExpr "DVotes/TVotes" "ProbD"
                , SR.UnwrapExpr "TVotes/VAP" "ProbV"
                ]
  res_C <- BR.retrieveOrMakeD resultCacheKey dataModelDep $ \() -> do
    K.logLE K.Info "Data or model newer then last cached result. (Re)-running..."
    SM.runModel
      stanConfig
      (SM.Both unwraps)
      (houseDataWrangler mds cds predictors)
      (SC.UseSummary getResults)
      ()
      houseDataForYear_C
  return (res_C, stanConfig)

binomial :: SB.StanModel
binomial =
  SB.StanModel
    dataBlock
    (Just transformedDataBlock)
    binomialParametersBlock
    (Just binomialTransformedParametersBlock)
    binomialModelBlock
    (Just binomialGeneratedQuantitiesBlock)
    binomialGQLLBlock

betaBinomial_v1 :: SB.StanModel
betaBinomial_v1 =
  SB.StanModel
    dataBlock
    (Just transformedDataBlock)
    betaBinomialParametersBlock
    (Just betaBinomialTransformedParametersBlock)
    betaBinomialModelBlock
    (Just betaBinomialGeneratedQuantitiesBlock)
    betaBinomialGQLLBlock

betaBinomialInc :: SB.StanModel
betaBinomialInc =
  SB.StanModel
  dataBlock
  (Just transformedDataBlock)
  betaBinomialIncParametersBlock
  (Just betaBinomialIncTransformedParametersBlock)
  betaBinomialIncModelBlock
  (Just betaBinomialIncGeneratedQuantitiesBlock)
  betaBinomialGQLLBlock

betaBinomialInc2 :: SB.StanModel
betaBinomialInc2 =
  SB.StanModel
  dataBlock
  (Just transformedDataBlock)
  betaBinomialInc2ParametersBlock
  (Just betaBinomialInc2TransformedParametersBlock)
  betaBinomialInc2ModelBlock
  (Just betaBinomialIncGeneratedQuantitiesBlock)
  betaBinomialInc2GQLLBlock

betaBinomialHS :: SB.StanModel
betaBinomialHS =
  SB.StanModel
    dataBlock
    (Just transformedDataBlock)
    betaBinomialHSParametersBlock
    (Just betaBinomialTransformedParametersBlock)
    betaBinomialHSModelBlock
    (Just betaBinomialGeneratedQuantitiesBlock)
    betaBinomialGQLLBlock

dataBlock :: SB.DataBlock
dataBlock =
  [here|
  int<lower = 1> G; // number of rows
  int<lower = 1> D; // number of datasets
  int<lower = 1, upper=D> CD; // dataset to use for generated stats
  int<lower = 0> IC; // incumbency column, 0 if incumbency is not a predictor
  int<lower = 0> K; // number of predictors
  matrix[G, K] X;
  int<lower=1> dataSet[G];
  int<lower = 0> VAP[G];
  int<lower = 0> TVotes[G];
  int<lower = 0> DVotes[G];
|]

transformedDataBlock :: SB.TransformedDataBlock
transformedDataBlock = [here|
  vector<lower=0>[K] sigmaPred;
  vector[K] meanPredD;
  vector[K] meanPredV;
  int<lower=1, upper=G> cdStart = 1;
  int<lower=1, upper=G> cdEnd = G;
  for (g in 1:G) {
    if (dataSet[g] < CD)
    {
      cdStart = g + 1;
    }
    if (dataSet[G - g - 1] > CD)
    {
      cdEnd = G - g;
    }
  }
  for (k in 1:K) {
    meanPredD[k] = mean(X[,k] .* to_vector(TVotes))/mean(to_vector(TVotes));
    meanPredV[k] = mean(X[,k] .* to_vector(VAP))/mean(to_vector(VAP));
    sigmaPred[k] = sd(X[cdStart:cdEnd,k]); // we only want std dev of the data for the chosen set
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
  real avgPVoted = inv_logit (alphaV[CD] + dot_product(meanPredV, betaV));
  real avgPDVote = inv_logit (alphaD[CD] + dot_product(meanPredD, betaD));
  vector[K] sigmaDeltaV;
  vector[K] sigmaDeltaD;
  vector[K] unitDeltaV;
  vector[K] unitDeltaD;
  for (k in 1:K) {
    sigmaDeltaV [k] = inv_logit (alphaV[CD] + meanPredV[k] + sigmaPred[k]/2 * betaV[k]) - inv_logit (alphaV[CD] + meanPredV[k] - sigmaPred[k]/2 * betaV[k]);
    sigmaDeltaD [k] = inv_logit (alphaD[CD] + meanPredD[k] + sigmaPred[k]/2 * betaD[k]) - inv_logit (alphaD[CD] + meanPredD[k] - sigmaPred[k]/2 * betaD[k]);
    unitDeltaV[k] = inv_logit (alphaV[CD] + (1-meanPredV[k]) * betaV[k]) - inv_logit (alphaV[CD] - meanPredV[k] * betaV[k]);
    unitDeltaD[k] = inv_logit (alphaD[CD] + (1-meanPredD[k]) * betaD[k]) - inv_logit (alphaD[CD] - meanPredD[k] * betaD[k]);
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



{-
houseDataWrangler' :: ModelWith -> [Text] -> HouseDataWrangler
houseDataWrangler' mw predictors = SC.Wrangle SC.NoIndex f
  where
    f _ = ((), makeDataJsonE)
    numDataSets :: Int = if mw == UseBoth then 2 else 1
    makeDataJsonE hmd = do
      (modelRows, dataSetIndex, nCD) <- case mw of
        UseElectionResults ->  do
          edModelRows <- electionResultsToModelRows predictors $ fmap F.rcast $ houseElectionData hmd
          let dataSetIndex = Vec.replicate (Vec.length edModelRows) (1 :: Int)
          return (edModelRows, dataSetIndex, 1 :: Int)
        UseCCES -> do
          ccesModelRows <- ccesDataToModelRows predictors $ ccesData hmd
          let dataSetIndex = Vec.replicate (Vec.length ccesModelRows) (1 :: Int)
          return (ccesModelRows, dataSetIndex, 1 :: Int)
        UseBoth -> do
          edModelRows <- electionResultsToModelRows predictors $ fmap F.rcast $ houseElectionData hmd
          ccesModelRows <- ccesDataToModelRows predictors $ ccesData hmd
          let modelRows = edModelRows <> ccesModelRows
              dataSetIndex = Vec.replicate (Vec.length edModelRows) (1 :: Int) <> Vec.replicate (Vec.length ccesModelRows) 2
          return (modelRows, dataSetIndex, 1 :: Int)
      let incumbencyCol = fromMaybe (0 :: Int) $ fmap fst $ find ((== "Incumbency") . snd)$ zip [1..] predictors
          dataF =
            SJ.namedF "G" FL.length
            <> SJ.constDataF "D" numDataSets
            <> SJ.constDataF "CD" nCD
            <> SJ.constDataF "K" (length predictors)
            <> SJ.constDataF "IC" incumbencyCol
            <> SJ.valueToPairF "X" (SJ.jsonArrayF pred)
            <> SJ.valueToPairF "VAP" (SJ.jsonArrayF vap)
            <> SJ.valueToPairF "TVotes"  (SJ.jsonArrayF tVotes)
            <> SJ.valueToPairF "DVotes"  (SJ.jsonArrayF dVotes)
      modelRowJson <- SJ.frameToStanJSONSeries dataF modelRows
      dataSetIndexJson <- SJ.frameToStanJSONSeries (SJ.valueToPairF "dataSet" (SJ.jsonArrayF id)) dataSetIndex
      return $ modelRowJson <> dataSetIndexJson
-}

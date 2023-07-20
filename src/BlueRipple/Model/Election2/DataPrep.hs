{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedLabels #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE UnicodeSyntax #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# OPTIONS_GHC -O0 #-}

module BlueRipple.Model.Election2.DataPrep
  (
    module BlueRipple.Model.Election2.DataPrep
  )
where

import qualified BlueRipple.Data.ACS_PUMS as PUMS
import qualified BlueRipple.Model.Demographic.EnrichCensus as DEC
import qualified BlueRipple.Model.Demographic.DataPrep as DDP

import qualified BlueRipple.Data.CCES as CCES
import qualified BlueRipple.Data.CPSVoterPUMS as CPS
import qualified BlueRipple.Data.CensusLoaders as Census
import qualified BlueRipple.Data.CensusTables as Census
import qualified BlueRipple.Data.CountFolds as BRCF
import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Data.DemographicTypes as DT
import BlueRipple.Data.ElectionTypes (CVAP)
import qualified BlueRipple.Data.ElectionTypes as ET
import qualified BlueRipple.Data.GeographicTypes as GT
import qualified BlueRipple.Data.Keyed as BRK
import qualified BlueRipple.Data.Loaders as BR
import qualified BlueRipple.Data.ModelingTypes as MT
import qualified BlueRipple.Model.PostStratify as BRPS
import qualified BlueRipple.Utilities.FramesUtils as BRF
import qualified BlueRipple.Utilities.KnitUtils as BR
import Control.Lens (view)
import qualified Control.Foldl as FL
import qualified Control.MapReduce as FMR
import qualified Data.Map.Strict as M
--import qualified Data.Serialize as S
import qualified Data.Text as T
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V
import qualified Flat
import qualified Frames as F
import qualified Frames.Folds as FF
import qualified Frames.MapReduce as FMR
import qualified Frames.Melt as F
import qualified Frames.Serialize as FS
import qualified Frames.SimpleJoins as FJ
import qualified Frames.Streamly.InCore as FI
import qualified Frames.Streamly.TH as FS
import qualified Frames.Transform as FT
import qualified Knit.Effect.AtomicCache as K hiding (retrieveOrMake)
import qualified Knit.Report as K
import qualified Numeric
import Prelude hiding (pred)

--import qualified Control.MapReduce as FMR

FS.declareColumn "Surveyed" ''Int
FS.declareColumn "Registered" ''Int
{-
FS.declareColumn "AHVoted" ''Double
FS.declareColumn "AHHouseDVotes" ''Double
FS.declareColumn "AHHouseRVotes" ''Double
FS.declareColumn "AHPresDVotes" ''Double
FS.declareColumn "AHPresRVotes" ''Double
FS.declareColumn "AchenHurWeight" ''Double
-}
FS.declareColumn "VotesInRace" ''Int
FS.declareColumn "DVotes" ''Int
FS.declareColumn "RVotes" ''Int
FS.declareColumn "TVotes" ''Int
FS.declareColumn "Voted" ''Int
FS.declareColumn "PresVotes" ''Int
FS.declareColumn "PresDVotes" ''Int
FS.declareColumn "PresRVotes" ''Int
FS.declareColumn "HouseVotes" ''Int
FS.declareColumn "HouseDVotes" ''Int
FS.declareColumn "HouseRVotes" ''Int

-- +1 for Dem incumbent, 0 for no incumbent, -1 for Rep incumbent
FS.declareColumn "Incumbency" ''Int
FS.declareColumn "HouseIncumbency" ''Int

type StateKeyR = [BR.Year, GT.StateAbbreviation]
type CDKeyR = StateKeyR V.++ '[GT.CongressionalDistrict]

type ElectionR = [Incumbency, ET.Unopposed, DVotes, RVotes, TVotes]

type CountDataR = [Surveyed, Registered, Voted]
type DCatsR = [DT.Age5C, DT.SexC, DT.Education4C, DT.Race5C]
type PredictorsR = DT.PWPopPerSqMile ': DCatsR
type VotePredictorsR = HouseIncumbency ': PredictorsR
type VoteDataR = [VotesInRace, DVotes, RVotes]
type CESByCDR = CDKeyR V.++ VotePredictorsR V.++ CountDataR V.++ VoteDataR

type ElectionDataR = [HouseIncumbency, HouseVotes, HouseDVotes, HouseRVotes, PresVotes, PresDVotes, PresRVotes]

newtype CESData = CESData (F.FrameRec CESByCDR) deriving stock Generic

instance Flat.Flat CESData where
  size (CESData c) n = Flat.size (FS.SFrame c) n
  encode (CESData c) = Flat.encode (FS.SFrame c)
  decode = (\c → CESData (FS.unSFrame c)) <$> Flat.decode

type CPSByStateR = StateKeyR V.++ PredictorsR V.++ CountDataR

newtype CPSData = CPSData (F.FrameRec CPSByStateR) deriving stock Generic

instance Flat.Flat CPSData where
  size (CPSData c) n = Flat.size (FS.SFrame c) n
  encode (CPSData c) = Flat.encode (FS.SFrame c)
  decode = (\c → CPSData (FS.unSFrame c)) <$> Flat.decode


data ModelData = ModelData
  { ccesData ∷ CESData
  , cpsData ∷ CPSData
  }
  deriving stock Generic

deriving instance Flat.Flat ModelData


-- Add Density
cesAddDensity :: (K.KnitEffects r, BR.CacheEffects r)
              => Either Text Text
              -> K.ActionWithCacheTime r (F.FrameRec (CDKeyR V.++ DCatsR V.++ CountDataR V.++ VoteDataR))
              -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec (CDKeyR V.++ PredictorsR V.++ CountDataR V.++ VoteDataR)))
cesAddDensity cacheE ces_C = do
  acs_C <- DDP.cachedACSa5ByCD
  cacheKey <- case cacheE of
    Right ck -> pure ck
    Left ck -> BR.clearIfPresentD ck >> pure ck
  BR.retrieveOrMakeFrame cacheKey ((,) <$> acs_C <*> ces_C) $ \(acs, ces) -> do
    let (joined, missing) = FJ.leftJoinWithMissing @(CDKeyR V.++ DCatsR) ces acs
    when (not $ null missing) $ K.knitError $ "cesAddDensity: Missing keys in CES/ACS join: " <> show missing
    pure $ fmap F.rcast joined

-- add House Incumbency
cesAddHouseIncumbency :: (K.KnitEffects r, BR.CacheEffects r)
              => Either Text Text
              -> K.ActionWithCacheTime r (F.FrameRec (CDKeyR V.++ PredictorsR V.++ CountDataR V.++ VoteDataR))
              -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec (CDKeyR V.++ VotePredictorsR V.++ CountDataR V.++ VoteDataR)))
cesAddHouseIncumbency cacheE ces_C = do
  elections_C <- fmap (FL.foldM (electionF @CDKeyR) . fmap F.rcast) <$> BR.houseElectionsWithIncumbency
  cacheKey <- case cacheE of
    Right ck -> pure ck
    Left ck -> BR.clearIfPresentD ck >> pure ck
  BR.retrieveOrMakeFrame cacheKey ((,) <$> elections_C <*> ces_C) $ \(electionsE, ces) -> do
    elections <- K.knitEither electionsE
    let (joined, missing) = FJ.leftJoinWithMissing @CDKeyR ces elections
    when (not $ null missing) $ K.knitError $ "cesAddHouseIncumbency: Missing keys in CES/Elections join: " <> show missing
    let f = FT.mutate $ \r -> FT.recordSingleton @HouseIncumbency (F.rgetField @Incumbency r)
    pure $ fmap (F.rcast . f) joined

-- an example for presidential 2020 vote.
cesCountedDemPresVotesByCD ∷ (K.KnitEffects r, BR.CacheEffects r)
                       ⇒ Bool
                       → K.Sem r (K.ActionWithCacheTime r (F.FrameRec (CDKeyR V.++ DCatsR V.++ CountDataR V.++ VoteDataR)))
cesCountedDemPresVotesByCD clearCaches = do
  ces2020_C ← CCES.ces20Loader
  let cacheKey = "model/election2/ces20ByCD.bin"
  when clearCaches $ BR.clearIfPresentD cacheKey
  BR.retrieveOrMakeFrame cacheKey ces2020_C $ \ces → cesMR 2020 (F.rgetField @CCES.MPresVoteParty) ces



modelDataForYear ∷ Int → ModelData → ModelData
modelDataForYear y = modelDataForYears [y]

modelDataForYears ∷ [Int] → ModelData → ModelData
modelDataForYears ys (ModelData (CESData ces) (CPSData cps)) =
  let f ∷ (FI.RecVec rs, F.ElemOf rs BR.Year) ⇒ F.FrameRec rs → F.FrameRec rs
      f = F.filterFrame ((`elem` ys) . F.rgetField @BR.Year)
   in ModelData (CESData $ f ces) (CPSData $ f cps)

-- CCES

countCESVotesF :: (F.ElemOf rs CCES.CatalistRegistrationC, F.ElemOf rs CCES.CatalistTurnoutC)
               => (F.Record rs -> MT.MaybeData ET.PartyT)
               -> FL.Fold
                  (F.Record rs)
                  (F.Record (CountDataR V.++ VoteDataR))
countCESVotesF votePartyMD =
  let vote (MT.MaybeData x) = maybe False (const True) x
      dVote (MT.MaybeData x) = maybe False (== ET.Democratic) x
      rVote (MT.MaybeData x) = maybe False (== ET.Republican) x
      surveyedF = FL.length
      registeredF = FL.prefilter (CCES.catalistRegistered . view CCES.catalistRegistrationC) FL.length
      votedF = FL.prefilter (CCES.catalistVoted . view CCES.catalistTurnoutC) FL.length
      votesF = FL.prefilter (vote . votePartyMD) votedF
      dVotesF = FL.prefilter (dVote . votePartyMD) votedF
      rVotesF = FL.prefilter (rVote . votePartyMD) votedF
   in (\s r v vs dvs rvs  → s F.&: r F.&: v F.&: vs F.&: dvs F.&: rvs F.&: V.RNil)
      <$> surveyedF
      <*> registeredF
      <*> votedF
      <*> votesF
      <*> dVotesF
      <*> rVotesF

cesRecodeHispanic ∷ (F.ElemOf rs DT.HispC, F.ElemOf rs DT.Race5C) => F.Record rs -> F.Record rs
cesRecodeHispanic r =
  let h = F.rgetField @DT.HispC r
      f r5 = if h == DT.Hispanic then DT.R5_Hispanic else r5
   in FT.fieldEndo @DT.Race5C f r

cesAddEducation4 ∷ (F.ElemOf rs DT.EducationC) => F.Record rs -> F.Record (DT.Education4C ': rs)
cesAddEducation4 r =
  let e4 = DT.educationToEducation4 $ F.rgetField @DT.EducationC r
  in e4 F.&: r

-- using each year's common content
cesMR ∷ forall rs f m .
        (Foldable f, Functor f, Monad m
        , F.ElemOf rs BR.Year
        , F.ElemOf rs GT.StateAbbreviation
        , F.ElemOf rs GT.CongressionalDistrict
        , F.ElemOf rs DT.Age5C
        , F.ElemOf rs DT.SexC
        , F.ElemOf rs DT.EducationC
        , F.ElemOf rs DT.HispC
        , F.ElemOf rs DT.Race5C
        , rs F.⊆ (DT.Education4C ': rs)
        , F.ElemOf rs CCES.CatalistRegistrationC
        , F.ElemOf rs CCES.CatalistTurnoutC
        )
      ⇒ Int → (F.Record rs -> MT.MaybeData ET.PartyT) -> f (F.Record rs) → m (F.FrameRec (CDKeyR V.++ DCatsR V.++ CountDataR V.++ VoteDataR))
cesMR earliestYear votePartyMD =
  BRF.frameCompactMR
  (FMR.unpackFilterOnField @BR.Year (>= earliestYear))
  (FMR.assignKeysAndData @(CDKeyR V.++ DCatsR) @rs)
  (countCESVotesF votePartyMD)
  . fmap (cesAddEducation4 . cesRecodeHispanic)

-- This is the thing to apply to loaded result data (with incumbents)
electionF
  ∷ ∀ ks
   . ( Ord (F.Record ks)
     , ks F.⊆ (ks V.++ '[BR.Candidate, ET.Party, ET.Votes, ET.Incumbent])
     , F.ElemOf (ks V.++ '[BR.Candidate, ET.Party, ET.Votes, ET.Incumbent]) ET.Incumbent
     , F.ElemOf (ks V.++ '[BR.Candidate, ET.Party, ET.Votes, ET.Incumbent]) ET.Party
     , F.ElemOf (ks V.++ '[BR.Candidate, ET.Party, ET.Votes, ET.Incumbent]) ET.Votes
     , F.ElemOf (ks V.++ '[BR.Candidate, ET.Party, ET.Votes, ET.Incumbent]) BR.Candidate
     , FI.RecVec (ks V.++ ElectionR)
     )
  ⇒ FL.FoldM (Either T.Text) (F.Record (ks V.++ [BR.Candidate, ET.Party, ET.Votes, ET.Incumbent])) (F.FrameRec (ks V.++ ElectionR))
electionF =
  FMR.concatFoldM $
    FMR.mapReduceFoldM
      (FMR.generalizeUnpack FMR.noUnpack)
      (FMR.generalizeAssign $ FMR.assignKeysAndData @ks)
      (FMR.makeRecsWithKeyM id $ FMR.ReduceFoldM $ const $ fmap (pure @[]) flattenVotesF)

data IncParty = None | Inc (ET.PartyT, Text) | Multi [Text]

updateIncParty ∷ IncParty → (ET.PartyT, Text) → IncParty
updateIncParty (Multi cs) (_, c) = Multi (c : cs)
updateIncParty (Inc (_, c)) (_, c') = Multi [c, c']
updateIncParty None (p, c) = Inc (p, c)

incPartyToInt ∷ IncParty → Either T.Text Int
incPartyToInt None = Right 0
incPartyToInt (Inc (ET.Democratic, _)) = Right 1
incPartyToInt (Inc (ET.Republican, _)) = Right (negate 1)
incPartyToInt (Inc _) = Right 0
incPartyToInt (Multi cs) = Left $ "Error: Multiple incumbents: " <> T.intercalate "," cs

flattenVotesF ∷ FL.FoldM (Either T.Text) (F.Record [BR.Candidate, ET.Incumbent, ET.Party, ET.Votes]) (F.Record ElectionR)
flattenVotesF = FMR.postMapM (FL.foldM flattenF) aggregatePartiesF
 where
  party = F.rgetField @ET.Party
  votes = F.rgetField @ET.Votes
  incumbentPartyF =
    FMR.postMapM incPartyToInt $
      FL.generalize $
        FL.prefilter (F.rgetField @ET.Incumbent) $
          FL.premap (\r → (F.rgetField @ET.Party r, F.rgetField @BR.Candidate r)) (FL.Fold updateIncParty None id)
  totalVotes = FL.premap votes FL.sum
  demVotesF = FL.generalize $ FL.prefilter (\r → party r == ET.Democratic) $ totalVotes
  repVotesF = FL.generalize $ FL.prefilter (\r → party r == ET.Republican) $ totalVotes
  unopposedF = (\x y → x == 0 || y == 0) <$> demVotesF <*> repVotesF
  flattenF = (\ii uo dv rv tv → ii F.&: uo F.&: dv F.&: rv F.&: tv F.&: V.RNil) <$> incumbentPartyF <*> unopposedF <*> demVotesF <*> repVotesF <*> FL.generalize totalVotes

aggregatePartiesF
  ∷ FL.FoldM
      (Either T.Text)
      (F.Record [BR.Candidate, ET.Incumbent, ET.Party, ET.Votes])
      (F.FrameRec [BR.Candidate, ET.Incumbent, ET.Party, ET.Votes])
aggregatePartiesF =
  let apF ∷ Text → FL.FoldM (Either T.Text) (F.Record [ET.Party, ET.Votes]) (F.Record [ET.Party, ET.Votes])
      apF c = FMR.postMapM ap (FL.generalize $ FL.premap (\r → (F.rgetField @ET.Party r, F.rgetField @ET.Votes r)) FL.map)
       where
        ap pvs =
          let demvM = M.lookup ET.Democratic pvs
              repvM = M.lookup ET.Republican pvs
              votes = FL.fold FL.sum $ M.elems pvs
              partyE = case (demvM, repvM) of
                (Nothing, Nothing) → Right ET.Other
                (Just _, Nothing) → Right ET.Democratic
                (Nothing, Just _) → Right ET.Republican
                (Just _, Just _) → Left $ c <> " has votes on both D and R lines!"
           in fmap (\p → p F.&: votes F.&: V.RNil) partyE
   in FMR.concatFoldM $
        FMR.mapReduceFoldM
          (FMR.generalizeUnpack FMR.noUnpack)
          (FMR.generalizeAssign $ FMR.assignKeysAndData @[BR.Candidate, ET.Incumbent] @[ET.Party, ET.Votes])
          (FMR.makeRecsWithKeyM id $ FMR.ReduceFoldM $ \r → fmap (pure @[]) (apF $ F.rgetField @BR.Candidate r))


{-
cesFold ∷ Int → FL.Fold (F.Record CCES.CESPR) (F.FrameRec CCESByCDR)
cesFold earliestYear =
  FMR.concatFold $
    FMR.mapReduceFold
      (FMR.unpackFilterOnField @BR.Year (>= earliestYear))
      (FMR.assignKeysAndData @[BR.Year, GT.StateAbbreviation, GT.CongressionalDistrict, DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.Race5C])
      (FMR.foldAndAddKey countCESVotesF)
-}



{-
prepCCESAndCPSEM
  ∷ (K.KnitEffects r, BR.CacheEffects r)
  ⇒ Bool
  → K.Sem r (K.ActionWithCacheTime r CCESAndCPSEM)
prepCCESAndCPSEM clearCache = do
  ccesAndPUMS_C ← prepCCESAndPums clearCache
  presElex_C ← prepPresidentialElectionData clearCache 2016
  senateElex_C ← prepSenateElectionData clearCache 2016
  houseElex_C ← prepHouseElectionData clearCache 2016
  --  K.logLE K.Diagnostic "Presidential Election Rows"
  --  K.ignoreCacheTime elex_C >>= BR.logFrame
  let cacheKey = "model/house/CCESAndCPSEM.bin"
      deps = (,,,) <$> ccesAndPUMS_C <*> presElex_C <*> senateElex_C <*> houseElex_C
  when clearCache $ BR.clearIfPresentD cacheKey
  BR.retrieveOrMakeD cacheKey deps $ \(ccesAndPums, pElex, sElex, hElex) → do
    let ccesEM = FL.fold fldAgeInCCES $ ccesRows ccesAndPums
        cpsVEM = FL.fold fldAgeInCPS $ cpsVRows ccesAndPums
        acsEM = FL.fold fixACSFld $ pumsRows ccesAndPums
    return $ CCESAndCPSEM ccesEM cpsVEM acsEM (pElex <> sElex) hElex

prepACS
  ∷ (K.KnitEffects r, BR.CacheEffects r)
  ⇒ Bool
  → K.Sem r (K.ActionWithCacheTime r (F.FrameRec PUMSWithDensityEM))
prepACS clearCache = do
  ccesAndPUMS_C ← prepCCESAndPums clearCache
  let cacheKey = "model/house/ACS.bin"
  when clearCache $ BR.clearIfPresentD cacheKey
  BR.retrieveOrMakeFrame cacheKey ccesAndPUMS_C $ \ccesAndPums → return $ FL.fold fldAgeInACS $ pumsRows ccesAndPums

acsForYears ∷ [Int] → F.FrameRec PUMSWithDensityEM → F.FrameRec PUMSWithDensityEM
acsForYears years x =
  let f ∷ (FI.RecVec rs, F.ElemOf rs BR.Year) ⇒ F.FrameRec rs → F.FrameRec rs
      f = F.filterFrame ((`elem` years) . F.rgetField @BR.Year)
   in f x

pumsMR
  ∷ ∀ ks f
   . ( Foldable f
     , Ord (F.Record ks)
     , FI.RecVec (ks V.++ DemographicsR)
     , ks F.⊆ (ks V.++ PUMSDataR)
     , F.ElemOf (ks V.++ PUMSDataR) DT.AvgIncome
     , F.ElemOf (ks V.++ PUMSDataR) DT.PopCount
     , F.ElemOf (ks V.++ PUMSDataR) DT.CitizenC
     , F.ElemOf (ks V.++ PUMSDataR) DT.CollegeGradC
     , F.ElemOf (ks V.++ PUMSDataR) DT.HispC
     , F.ElemOf (ks V.++ PUMSDataR) DT.PWPopPerSqMile
     , F.ElemOf (ks V.++ PUMSDataR) DT.RaceAlone4C
     , F.ElemOf (ks V.++ PUMSDataR) DT.SexC
     , F.ElemOf (ks V.++ PUMSDataR) DT.SimpleAgeC
     )
  ⇒ f (F.Record (ks V.++ PUMSDataR))
  → (F.FrameRec (ks V.++ DemographicsR))
pumsMR =
  runIdentity
    . BRF.frameCompactMR
      FMR.noUnpack
      (FMR.assignKeysAndData @ks)
      pumsDataF

pumsDataF
  ∷ FL.Fold
      (F.Record PUMSDataR)
      (F.Record DemographicsR)
pumsDataF =
  let ppl = F.rgetField @DT.PopCount
      pplF = FL.premap ppl FL.sum
      intRatio x y = realToFrac x / realToFrac y
      fracF f = intRatio <$> FL.prefilter f pplF <*> pplF
      pplWgtdSumF f = FL.premap (\r → realToFrac (ppl r) * f r) FL.sum
      pplWgtdF f = (/) <$> pplWgtdSumF f <*> fmap realToFrac pplF
      race4A = F.rgetField @DT.RaceAlone4C
      hisp = F.rgetField @DT.HispC
      wnh r = race4A r == DT.RA4_White && hisp r == DT.NonHispanic
      wh r = race4A r == DT.RA4_White && hisp r == DT.Hispanic
      nwh r = race4A r /= DT.RA4_White && hisp r == DT.Hispanic
      black r = race4A r == DT.RA4_Black && hisp r == DT.NonHispanic
      asian r = race4A r == DT.RA4_Asian && hisp r == DT.NonHispanic
      other r = race4A r == DT.RA4_Other && hisp r == DT.NonHispanic
--      white r = race4A r == DT.RA4_White
      whiteNonHispanicGrad r = (wnh r) && (F.rgetField @DT.CollegeGradC r == DT.Grad)
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
          V.:& FF.toFoldRecord (fracF whiteNonHispanicGrad)
          V.:& FF.toFoldRecord (pplWgtdF (F.rgetField @DT.AvgIncome))
          V.:& FF.toFoldRecord (pplWgtdF (F.rgetField @DT.PWPopPerSqMile))
          V.:& FF.toFoldRecord pplF
          V.:& V.RNil


type ElectionResultWithDemographicsR ks = ks V.++ '[ET.Office] V.++ ElectionR V.++ DemographicsR
type ElectionResultR ks = ks V.++ '[ET.Office] V.++ ElectionR V.++ '[DT.PopCount]

{-
addUnopposed :: (F.ElemOf rs DVotes, F.ElemOf rs RVotes) => F.Record rs -> F.Record (rs V.++ '[ET.Unopposed])
addUnopposed = FT.mutate (FT.recordSingleton @ET.Unopposed . unopposed) where
  unopposed r = F.rgetField @DVotes r == 0 || F.rgetField @RVotes r == 0
-}

makeStateElexDataFrame
  ∷ (K.KnitEffects r)
  ⇒ ET.OfficeT
  → Int
  → F.FrameRec (StateKeyR V.++ CensusPredictorR V.++ '[DT.PopCount])
  → F.FrameRec [BR.Year, GT.StateAbbreviation, BR.Candidate, ET.Party, ET.Votes, ET.Incumbent, BR.Special]
  → K.Sem r (F.FrameRec (ElectionResultR [BR.Year, GT.StateAbbreviation]))
makeStateElexDataFrame office earliestYear acsByState elex = do
  let addOffice rs = FT.recordSingleton @ET.Office office F.<+> rs
      length' = FL.fold FL.length
  let cvapFld =
        FMR.concatFold $
          FMR.mapReduceFold
            FMR.noUnpack
            (FMR.assignKeysAndData @[BR.Year, GT.StateAbbreviation] @'[DT.PopCount])
            (FMR.foldAndAddKey $ FF.foldAllConstrained @Num FL.sum)
      cvapByState = FL.fold cvapFld acsByState
  flattenedElex ←
    K.knitEither $
      FL.foldM
        (electionF @[BR.Year, GT.StateAbbreviation, BR.Special])
        (fmap F.rcast $ F.filterFrame ((>= earliestYear) . F.rgetField @BR.Year) elex)
  let (elexWithCVAP, missing) = FJ.leftJoinWithMissing @[BR.Year, GT.StateAbbreviation] flattenedElex cvapByState
  when (not $ null missing) $ K.knitError $ "makeStateElexDataFrame: missing keys in elex/ACS join=" <> show missing
  when (length' elexWithCVAP /= length' flattenedElex) $ K.knitError "makeStateElexDataFrame: added rows in elex/ACS join"
  return $ fmap (F.rcast . addOffice) elexWithCVAP

addSpecial ∷ F.Record rs → F.Record (rs V.++ '[BR.Special])
addSpecial = FT.mutate (const $ FT.recordSingleton @BR.Special False)

prepPresidentialElectionData
  ∷ (K.KnitEffects r, BR.CacheEffects r)
  ⇒ Bool
  → Int
  → K.Sem r (K.ActionWithCacheTime r (F.FrameRec (ElectionResultR '[BR.Year, GT.StateAbbreviation])))
prepPresidentialElectionData clearCache earliestYear = do
  let cacheKey = "model/house/presElexWithCVAP.bin"
  when clearCache $ BR.clearIfPresentD cacheKey
  presElex_C ← BR.presidentialElectionsWithIncumbency
  acs_C ← PUMS.pumsLoaderAdults
  acsByState_C ← cachedPumsByState acs_C
  let deps = (,) <$> acsByState_C <*> presElex_C
  BR.retrieveOrMakeFrame cacheKey deps $
    \(acsByState, pElex) → makeStateElexDataFrame ET.President earliestYear acsByState (fmap (F.rcast . addSpecial) $ pElex)

prepSenateElectionData
  ∷ (K.KnitEffects r, BR.CacheEffects r)
  ⇒ Bool
  → Int
  → K.Sem r (K.ActionWithCacheTime r (F.FrameRec (ElectionResultR '[BR.Year, GT.StateAbbreviation])))
prepSenateElectionData clearCache earliestYear = do
  let cacheKey = "model/house/senateElexWithCVAP.bin"
  when clearCache $ BR.clearIfPresentD cacheKey
  senateElex_C ← BR.senateElectionsWithIncumbency
  acs_C ← PUMS.pumsLoaderAdults
  acsByState_C ← cachedPumsByState acs_C
  let deps = (,) <$> acsByState_C <*> senateElex_C
  BR.retrieveOrMakeFrame cacheKey deps $
    \(acsByState, senateElex) → makeStateElexDataFrame ET.Senate earliestYear acsByState (fmap F.rcast senateElex)

makeCDElexDataFrame
  ∷ (K.KnitEffects r)
  ⇒ ET.OfficeT
  → Int
  → F.FrameRec PUMSByCDR
  → F.FrameRec [BR.Year, GT.StateAbbreviation, GT.CongressionalDistrict, BR.Candidate, ET.Party, ET.Votes, ET.Incumbent]
  → K.Sem r (F.FrameRec (ElectionResultR [BR.Year, GT.StateAbbreviation, GT.CongressionalDistrict]))
makeCDElexDataFrame office earliestYear acsByCD elex = do
  let addOffice rs = FT.recordSingleton @ET.Office office F.<+> rs
      lengthF = FL.fold FL.length
      fixDC_CD r =
        if (F.rgetField @GT.StateAbbreviation r == "DC")
          then FT.fieldEndo @GT.CongressionalDistrict (const 1) r
          else r
  let cvapFld =
        FMR.concatFold $
          FMR.mapReduceFold
            FMR.noUnpack
            (FMR.assignKeysAndData @[BR.Year, GT.StateAbbreviation, GT.CongressionalDistrict] @'[DT.PopCount])
            (FMR.foldAndAddKey $ FF.foldAllConstrained @Num FL.sum)
      cvapByCD = FL.fold cvapFld acsByCD
  flattenedElex ←
    K.knitEither $
      FL.foldM
        (electionF @[BR.Year, GT.StateAbbreviation, GT.CongressionalDistrict])
        (fmap F.rcast $ F.filterFrame ((>= earliestYear) . F.rgetField @BR.Year) elex)
  let (elexWithCVAP, missing) =
        FJ.leftJoinWithMissing @[BR.Year, GT.StateAbbreviation, GT.CongressionalDistrict]
          (fmap fixDC_CD flattenedElex)
          (fmap fixDC_CD cvapByCD)
  when (not $ null missing) $ K.knitError $ "makeCDElexDataFrame: missing keys in elex/ACS join=" <> show missing
  when (lengthF elexWithCVAP /= lengthF flattenedElex) $ K.knitError "makeCDElexDataFrame: added rows in elex/ACS join"
  return $ fmap (F.rcast . addOffice) elexWithCVAP

prepHouseElectionData
  ∷ (K.KnitEffects r, BR.CacheEffects r)
  ⇒ Bool
  → Int
  → K.Sem r (K.ActionWithCacheTime r (F.FrameRec (ElectionResultR '[BR.Year, GT.StateAbbreviation, GT.CongressionalDistrict])))
prepHouseElectionData clearCache earliestYear = do
  let cacheKey = "model/house/houseElexWithCVAP.bin"
  when clearCache $ BR.clearIfPresentD cacheKey
  houseElex_C ← BR.houseElectionsWithIncumbency
  acs_C ← PUMS.pumsLoaderAdults
  cdByPUMA_C ← BR.allCDFromPUMA2012Loader
  acsByCD_C ← cachedPumsByCD acs_C cdByPUMA_C
  let deps = (,) <$> acsByCD_C <*> houseElex_C
  BR.retrieveOrMakeFrame cacheKey deps $
    \(acsByCD, houseElex) → makeCDElexDataFrame ET.House earliestYear acsByCD (fmap F.rcast houseElex)

-- TODO:  Use weights?  Design effect?
{-
countCCESVotesF :: FL.Fold (F.Record [CCES.Turnout, CCES.HouseVoteParty]) (F.Record [Surveyed, TVotes, DVotes])
countCCESVotesF =
  let surveyedF = FL.length
      votedF = FL.prefilter ((== CCES.T_Voted) . F.rgetField @CCES.Turnout) FL.length
      dVoteF = FL.prefilter ((== ET.Democratic) . F.rgetField @CCES.HouseVoteParty) votedF
  in (\s v d -> s F.&: v F.&: d F.&: V.RNil) <$> surveyedF <*> votedF <*> dVoteF

-- using cumulative
ccesMR :: (Foldable f, Monad m) => Int -> f (F.Record CCES.CCES_MRP) -> m (F.FrameRec CCESByCDR)
ccesMR earliestYear = BRF.frameCompactMRM
                     (FMR.unpackFilterOnField @BR.Year (>= earliestYear))
                     (FMR.assignKeysAndData @[BR.Year, GT.StateAbbreviation, GT.CongressionalDistrict, DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.Race5C, DT.HispC])
                     countCCESVotesF

ccesCountedDemHouseVotesByCD :: (K.KnitEffects r, BR.CacheEffects r) => Bool -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec CCESByCDR))
ccesCountedDemHouseVotesByCD clearCaches = do
  cces_C <- CCES.ccesDataLoader
  let cacheKey = "model/house/ccesByCD.bin"
  when clearCaches $  BR.clearIfPresentD cacheKey
  BR.retrieveOrMakeFrame cacheKey cces_C $ \cces -> do
--    BR.logFrame cces
    ccesMR 2012 cces
-}


--  BR.retrieveOrMakeFrame cacheKey ces2020_C $ return . FL.fold (cesFold 2020)

-- NB: StateKey includes year
cpsCountedTurnoutByState ∷ (K.KnitEffects r, BR.CacheEffects r) ⇒ K.Sem r (K.ActionWithCacheTime r (F.FrameRec CPSVByStateR))
cpsCountedTurnoutByState = do
  let afterYear y r = F.rgetField @BR.Year r >= y
      possible r = CPS.cpsPossibleVoter $ F.rgetField @ET.VotedYNC r
      citizen r = F.rgetField @DT.CitizenC r == DT.Citizen
      includeRow r = afterYear 2012 r && possible r && citizen r
      votedF r = CPS.cpsVoted $ F.rgetField @ET.VotedYNC r
      wgt r = F.rgetField @CPS.CPSVoterPUMSWeight r
      fld =
        BRCF.weightedCountFold @_ @CPS.CPSVoterPUMS
          (\r → F.rcast @StateKeyR r `V.rappend` CPS.cpsKeysToCASER4H True (F.rcast r))
          (F.rcast @[ET.VotedYNC, CPS.CPSVoterPUMSWeight])
          includeRow
          votedF
          wgt
  cpsRaw_C ← CPS.cpsVoterPUMSLoader -- NB: this is only useful for CD rollup since counties may appear in multiple CDs.
  BR.retrieveOrMakeFrame "model/house/cpsVByState.bin" cpsRaw_C $ return . FL.fold fld

pumsReKey
  ∷ F.Record '[DT.CitizenC, DT.Age5FC, DT.SexC, DT.EducationC, DT.InCollege, DT.RaceAlone4C, DT.HispC]
  → F.Record '[DT.CitizenC, DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.RaceAlone4C, DT.HispC]
pumsReKey r =
  let cg = DT.collegeGrad $ F.rgetField @DT.EducationC r
      ic = F.rgetField @DT.InCollege r
   in   F.rgetField @DT.CitizenC r
        F.&: DT.age5FToSimple (F.rgetField @DT.Age5FC r)
        F.&: F.rgetField @DT.SexC r
        F.&: (if cg == DT.Grad || ic then DT.Grad else DT.NonGrad)
        F.&: F.rgetField @DT.RaceAlone4C r
        F.&: F.rgetField @DT.HispC r
        F.&: V.RNil

copy2019to2020 ∷ (FI.RecVec rs, F.ElemOf rs BR.Year) ⇒ F.FrameRec rs → F.FrameRec rs
copy2019to2020 rows = rows <> fmap changeYear2020 (F.filterFrame year2019 rows)
 where
  year2019 r = F.rgetField @BR.Year r == 2019
  changeYear2020 r = F.rputField @BR.Year 2020 r

type SenateRaceKeyR = [BR.Year, GT.StateAbbreviation, BR.Special, BR.Stage]

type ElexDataR = [ET.Office, BR.Stage, BR.Runoff, BR.Special, BR.Candidate, ET.Party, ET.Votes, ET.Incumbent]

type HouseModelCensusTablesByCD =
  Census.CensusTables Census.LDLocationR Census.CensusDataR DT.Age5FC DT.SexC DT.CollegeGradC Census.RaceEthnicityC DT.CitizenC Census.EmploymentC

type HouseModelCensusTablesByState =
  Census.CensusTables '[BR.StateFips] Census.CensusDataR DT.Age5FC DT.SexC DT.CollegeGradC Census.RaceEthnicityC DT.CitizenC Census.EmploymentC

pumsByPUMA
  ∷ (F.Record PUMS.PUMS → Bool)
  → F.FrameRec PUMS.PUMS
  → F.FrameRec (PUMS.PUMACounts [DT.CitizenC, DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.RaceAlone4C, DT.HispC])
pumsByPUMA keepIf = FL.fold (PUMS.pumsRollupF keepIf $ pumsReKey . F.rcast)

pumsByCD ∷ (K.KnitEffects r) ⇒ F.FrameRec PUMS.PUMS → F.FrameRec BR.DatedCDFromPUMA2012 → K.Sem r (F.FrameRec PUMSByCDR)
pumsByCD pums cdFromPUMA = fmap F.rcast <$> PUMS.pumsCDRollup (earliest earliestYear) (pumsReKey . F.rcast) cdFromPUMA pums
 where
  earliestYear = 2016
  earliest year = (>= year) . F.rgetField @BR.Year

cachedPumsByCD
  ∷ ∀ r
   . (K.KnitEffects r, BR.CacheEffects r)
  ⇒ K.ActionWithCacheTime r (F.FrameRec PUMS.PUMS)
  → K.ActionWithCacheTime r (F.FrameRec BR.DatedCDFromPUMA2012)
  → K.Sem r (K.ActionWithCacheTime r (F.FrameRec PUMSByCDR))
cachedPumsByCD pums_C cdFromPUMA_C = do
  let pumsByCDDeps = (,) <$> pums_C <*> cdFromPUMA_C
  BR.retrieveOrMakeFrame "model/house/pumsByCD.bin" pumsByCDDeps $
    \(pums, cdFromPUMA) → pumsByCD pums cdFromPUMA

pumsByState ∷ F.FrameRec PUMS.PUMS → F.FrameRec PUMSByStateR
pumsByState pums = F.rcast <$> FL.fold (PUMS.pumsStateRollupF (pumsReKey . F.rcast)) filteredPums
 where
  earliestYear = 2016
  earliest year = (>= year) . F.rgetField @BR.Year
  filteredPums = F.filterFrame (earliest earliestYear) pums

cachedPumsByState
  ∷ ∀ r
   . (K.KnitEffects r, BR.CacheEffects r)
  ⇒ K.ActionWithCacheTime r (F.FrameRec PUMS.PUMS)
  → K.Sem r (K.ActionWithCacheTime r (F.FrameRec (StateKeyR V.++ CensusPredictorR V.++ '[DT.PopCount])))
cachedPumsByState pums_C = do
  let zeroCount ∷ F.Record '[DT.PopCount]
      zeroCount = 0 F.&: V.RNil
      addZeroF =
        FMR.concatFold $
          FMR.mapReduceFold
            FMR.noUnpack
            (FMR.assignKeysAndData @StateKeyR @(CensusPredictorR V.++ '[DT.PopCount]))
            ( FMR.makeRecsWithKey id $
                FMR.ReduceFold $
                  const $
                    BRK.addDefaultRec @CensusPredictorR zeroCount
            )
  BR.retrieveOrMakeFrame "model/house/pumsByState.bin" pums_C $
    pure . FL.fold addZeroF . pumsByState

-- caFilter = F.filterFrame (\r -> F.rgetField @BR.Year r == 2020 && F.rgetField @GT.StateAbbreviation r == "CA")

prepCCESAndPums ∷ ∀ r. (K.KnitEffects r, BR.CacheEffects r) ⇒ Bool → K.Sem r (K.ActionWithCacheTime r CCESAndPUMS)
prepCCESAndPums clearCache = do
  let testRun = False
      cacheKey x k = k <> if x then ".tbin" else ".bin"
  let earliestYear = 2016 -- set by ces for now
      earliest year = (>= year) . F.rgetField @BR.Year
      fixDC_CD r =
        if (F.rgetField @GT.StateAbbreviation r == "DC")
          then FT.fieldEndo @GT.CongressionalDistrict (const 1) r
          else r
--      fLength = FL.fold FL.length
--      lengthInYear y = fLength . F.filterFrame ((== y) . F.rgetField @BR.Year)
  pums_C ← PUMS.pumsLoaderAdults
--  pumsByState_C ← cachedPumsByState pums_C
  countedCCES_C ← fmap (BR.fixAtLargeDistricts 0) <$> cesCountedDemVotesByCD clearCache
  cpsVByState_C ← fmap (F.filterFrame $ earliest earliestYear) <$> cpsCountedTurnoutByState
  cdFromPUMA_C ← BR.allCDFromPUMA2012Loader
  pumsByCD_C ← cachedPumsByCD pums_C cdFromPUMA_C
  let deps = (,,) <$> countedCCES_C <*> cpsVByState_C <*> pumsByCD_C
      allCacheKey = cacheKey testRun "model/house/CCESAndPUMS"
  when clearCache $ BR.clearIfPresentD allCacheKey
  BR.retrieveOrMakeD allCacheKey deps $ \(ccesByCD, cpsVByState, acsByCD) → do
    -- get Density and avg income from PUMS and combine with election data for the district level data
    let acsCDFixed = fmap fixDC_CD acsByCD
        diInnerFold ∷ FL.Fold (F.Record [DT.PWPopPerSqMile, DT.AvgIncome, DT.PopCount]) (F.Record [DT.PopCount, DT.PWPopPerSqMile, DT.AvgIncome])
        diInnerFold =
          let ppl = F.rgetField @DT.PopCount -- Weight by voters. If voters/non-voters live in different places, we get voters experience.
          --              ppl r = cit r + F.rgetField @PUMS.NonCitizens r
              pplF = FL.premap ppl FL.sum
--              wgtdAMeanF w f = (/) <$> FL.premap (\r → w r * f r) FL.sum <*> FL.premap w FL.sum
--              wgtdGMeanF w f = fmap Numeric.exp $ (/) <$> FL.premap (\r → w r * Numeric.log (f r)) FL.sum <*> FL.premap w FL.sum
              pplWeightedAMeanF = wgtdAMeanF (realToFrac . ppl) -- (/) <$> FL.premap (\r -> realToFrac (cit r) * f r) FL.sum <*> fmap realToFrac citF
              pplWeightedGMeanF = wgtdGMeanF (realToFrac . ppl) -- (/) <$> FL.premap (\r -> realToFrac (cit r) * f r) FL.sum <*> fmap realToFrac citF
              --              pplWgtdAMeanF = wgtdAMeanF (realToFrac . ppl)
              --              pplF = FL.premap ppl FL.sum
              --              pplWeightedSumF f = (/) <$> FL.premap (\r -> realToFrac (ppl r) * f r) FL.sum <*> fmap realToFrac pplF
           in (\c d i → c F.&: d F.&: i F.&: V.RNil) <$> pplF <*> pplWeightedGMeanF (F.rgetField @DT.PWPopPerSqMile) <*> pplWeightedAMeanF (F.rgetField @DT.AvgIncome)
        diByCDFold ∷ FL.Fold (F.Record PUMSByCDR) (F.FrameRec DistrictDemDataR)
        diByCDFold =
          FMR.concatFold $
            FMR.mapReduceFold
              (FMR.filterUnpack $ \r -> F.rgetField @DT.CitizenC r == DT.Citizen)
              (FMR.assignKeysAndData @CDKeyR)
              (FMR.foldAndAddKey diInnerFold)
        diByStateFold ∷ FL.Fold (F.Record PUMSByCDR) (F.FrameRec StateDemDataR)
        diByStateFold =
          FMR.concatFold $
            FMR.mapReduceFold
              FMR.noUnpack
              (FMR.assignKeysAndData @StateKeyR)
              (FMR.foldAndAddKey diInnerFold)
        diByCD = FL.fold diByCDFold acsCDFixed
        diByState = FL.fold diByStateFold acsCDFixed
    ccesWD ← K.knitEither $ addPopDensByDistrict diByCD ccesByCD
    cpsVWD ← K.knitEither $ addPopDensByState diByState cpsVByState
    --    acsWD <- K.knitEither $ addPopDensByDistrict diByCD acsCDFixed
    return $ CCESAndPUMS (fmap F.rcast ccesWD) cpsVWD acsCDFixed diByCD -- (F.toFrame $ fmap F.rcast $ cats)

type CCESWithDensity = CCESByCDR V.++ '[DT.PWPopPerSqMile]
type CCESWithDensityEM = CCESByCDEMR V.++ '[DT.PWPopPerSqMile]

addPopDensByDistrict
  ∷ ∀ rs
   . ( FJ.CanLeftJoinM
        [BR.Year, GT.StateAbbreviation, GT.CongressionalDistrict]
        rs
        [BR.Year, GT.StateAbbreviation, GT.CongressionalDistrict, DT.PWPopPerSqMile]
--     , [BR.Year, GT.StateAbbreviation, GT.CongressionalDistrict] F.⊆ [BR.Year, GT.StateAbbreviation, GT.CongressionalDistrict, DT.PopPerSqMile]
     , F.ElemOf (rs V.++ '[DT.PWPopPerSqMile]) GT.CongressionalDistrict
     , F.ElemOf (rs V.++ '[DT.PWPopPerSqMile]) GT.StateAbbreviation
     , F.ElemOf (rs V.++ '[DT.PWPopPerSqMile]) BR.Year
     )
  ⇒ F.FrameRec DistrictDemDataR
  → F.FrameRec rs
  → Either Text (F.FrameRec (rs V.++ '[DT.PWPopPerSqMile]))
addPopDensByDistrict ddd rs = do
  let ddd' = F.rcast @[BR.Year, GT.StateAbbreviation, GT.CongressionalDistrict, DT.PWPopPerSqMile] <$> ddd
      (joined, missing) =
        FJ.leftJoinWithMissing
          @[BR.Year, GT.StateAbbreviation, GT.CongressionalDistrict]
          @rs
          @[BR.Year, GT.StateAbbreviation, GT.CongressionalDistrict, DT.PWPopPerSqMile]
          rs
          ddd'
  when (not $ null missing) $ Left $ "missing keys in join of density data by district: " <> show missing
  Right joined

addPopDensByState
  ∷ ∀ rs
   . ( FJ.CanLeftJoinM
        [BR.Year, GT.StateAbbreviation]
        rs
        [BR.Year, GT.StateAbbreviation, DT.PWPopPerSqMile]
--     , [BR.Year, GT.StateAbbreviation] F.⊆ [BR.Year, GT.StateAbbreviation, DT.PopPerSqMile]
     , F.ElemOf (rs V.++ '[DT.PWPopPerSqMile]) GT.StateAbbreviation
     , F.ElemOf (rs V.++ '[DT.PWPopPerSqMile]) BR.Year
     )
  ⇒ F.FrameRec StateDemDataR
  → F.FrameRec rs
  → Either Text (F.FrameRec (rs V.++ '[DT.PWPopPerSqMile]))
addPopDensByState ddd rs = do
  let ddd' = F.rcast @[BR.Year, GT.StateAbbreviation, DT.PWPopPerSqMile] <$> ddd
      (joined, missing) =
        FJ.leftJoinWithMissing
          @[BR.Year, GT.StateAbbreviation]
          @rs
          @[BR.Year, GT.StateAbbreviation, DT.PWPopPerSqMile]
          rs
          ddd'
  when (not $ null missing) $ Left $ "missing keys in join of density data by state: " <> show missing
  Right joined

wgtdAMeanF ∷ (a → Double) → (a → Double) → FL.Fold a Double
wgtdAMeanF w f = (/) <$> FL.premap (\a → w a * f a) FL.sum <*> FL.premap w FL.sum

wgtdGMeanF ∷ (a → Double) → (a → Double) → FL.Fold a Double
wgtdGMeanF w f = Numeric.exp <$> wgtdAMeanF w (Numeric.log . f)

sumButLeaveDensity
  ∷ ∀ as
   . ( as F.⊆ (as V.++ '[DT.PWPopPerSqMile])
     , F.ElemOf (as V.++ '[DT.PWPopPerSqMile]) DT.PWPopPerSqMile
     , FF.ConstrainedFoldable Num as
     )
  ⇒ (F.Record (as V.++ '[DT.PWPopPerSqMile]) → Double)
  → FL.Fold (F.Record (as V.++ '[DT.PWPopPerSqMile])) (F.Record (as V.++ '[DT.PWPopPerSqMile]))
sumButLeaveDensity w =
  let sumF = FL.premap (F.rcast @as) $ FF.foldAllConstrained @Num FL.sum
      densF = fmap (FT.recordSingleton @DT.PWPopPerSqMile) $ wgtdGMeanF w (F.rgetField @DT.PWPopPerSqMile)
   in (F.<+>) <$> sumF <*> densF

fldAgeInCPS ∷ FL.Fold (F.Record CPSVWithDensity) (F.FrameRec CPSVWithDensityEM)
fldAgeInCPS =
  FMR.concatFold $
    FMR.mapReduceFold
      FMR.noUnpack
      (FMR.assignKeysAndData @(StateKeyR V.++ CensusPredictorEMR))
      (FMR.foldAndAddKey $ sumButLeaveDensity @BRCF.CountCols (realToFrac . F.rgetField @BRCF.Count))

fldAgeInACS ∷ FL.Fold (F.Record PUMSWithDensity) (F.FrameRec PUMSWithDensityEM)
fldAgeInACS =
  FMR.concatFold $
    FMR.mapReduceFold
      FMR.noUnpack
      (FMR.assignKeysAndData @(CDKeyR V.++ CensusPredictorEMR))
      (FMR.foldAndAddKey $ sumButLeaveDensity @'[DT.PopCount] (realToFrac . F.rgetField @DT.PopCount))

sumButLeaveDensityCCES ∷ FL.Fold (F.Record ((CCESVotingDataR V.++ '[DT.PWPopPerSqMile]))) (F.Record ((CCESVotingDataR V.++ '[DT.PWPopPerSqMile])))
sumButLeaveDensityCCES =
  let sumF f = FL.premap f FL.sum
      densF = wgtdGMeanF (realToFrac . F.rgetField @Surveyed) (F.rgetField @DT.PWPopPerSqMile)
   in (\f1 f2 f3 f4 f5 f6 f7 f8 f9 → f1 F.&: f2 F.&: f3 F.&: f4 F.&: f5 F.&: f6 F.&: f7 F.&: f8 F.&: f9 F.&: V.RNil)
        <$> sumF (F.rgetField @Surveyed)
        <*> sumF (F.rgetField @Voted)
        <*> sumF (F.rgetField @HouseVotes)
        <*> sumF (F.rgetField @HouseDVotes)
        <*> sumF (F.rgetField @HouseRVotes)
        <*> sumF (F.rgetField @PresVotes)
        <*> sumF (F.rgetField @PresDVotes)
        <*> sumF (F.rgetField @PresRVotes)
        <*> densF

-- NB : the polymorphic sumButLeaveDensity caused some sort of memory blowup compiling this one.
fldAgeInCCES ∷ FL.Fold (F.Record CCESWithDensity) (F.FrameRec CCESWithDensityEM)
fldAgeInCCES =
  FMR.concatFold $
    FMR.mapReduceFold
      FMR.noUnpack
      (FMR.assignKeysAndData @(CDKeyR V.++ CCESPredictorEMR))
      (FMR.foldAndAddKey sumButLeaveDensityCCES)

type PUMSWithDensity = PUMSByCDR -- V.++ '[DT.PWPopPerSqMile]
type PUMSWithDensityEM = PUMSByCDEMR V.++ '[DT.PWPopPerSqMile]
type ACSWithDensityEM = CDKeyR V.++ CCESPredictorEMR V.++ [DT.PWPopPerSqMile, DT.PopCount]

type CPSVWithDensity = CPSVByStateR V.++ '[DT.PWPopPerSqMile]
type CPSVWithDensityEM = CPSVByStateEMR V.++ '[DT.PWPopPerSqMile]

fixACSFld ∷ FL.Fold (F.Record PUMSWithDensity) (F.FrameRec ACSWithDensityEM)
fixACSFld =
  let --safeLog x = if x < 1e-12 then 0 else Numeric.log x
      density = F.rgetField @DT.PWPopPerSqMile
      ppl = F.rgetField @DT.PopCount
      pplFld = FL.premap ppl FL.sum
      pplWgtdDensityFld = wgtdGMeanF (realToFrac . ppl) density -- fmap Numeric.exp ((/) <$> FL.premap (\r -> realToFrac (cit r) * safeLog (density r)) FL.sum <*> fmap realToFrac citFld)
      dataFld ∷ FL.Fold (F.Record [DT.PWPopPerSqMile, DT.PopCount]) (F.Record [DT.PWPopPerSqMile, DT.PopCount])
      dataFld = (\d c → d F.&: c F.&: V.RNil) <$> pplWgtdDensityFld <*> pplFld
   in FMR.concatFold $
        FMR.mapReduceFold
          (FMR.simpleUnpack $ addRace5)
          (FMR.assignKeysAndData @(CDKeyR V.++ CCESPredictorEMR))
          (FMR.foldAndAddKey dataFld)

race5FromRace4AAndHisp ∷ (F.ElemOf rs DT.RaceAlone4C, F.ElemOf rs DT.HispC) ⇒ F.Record rs → DT.Race5
race5FromRace4AAndHisp r =
  let race4A = F.rgetField @DT.RaceAlone4C r
      hisp = F.rgetField @DT.HispC r
   in DT.race5FromRaceAlone4AndHisp True race4A hisp

addRace5
  ∷ (F.ElemOf rs DT.RaceAlone4C, F.ElemOf rs DT.HispC)
  ⇒ F.Record rs
  → F.Record (rs V.++ '[DT.Race5C])
addRace5 r = r V.<+> FT.recordSingleton @DT.Race5C (race5FromRace4AAndHisp r)

-- replaceRace

psFldCPS ∷ FL.Fold (F.Record [CVAP, Voters, DemVoters]) (F.Record [CVAP, Voters, DemVoters])
psFldCPS = FF.foldAllConstrained @Num FL.sum

cpsDiagnostics
  ∷ (K.KnitEffects r, BR.CacheEffects r)
  ⇒ Text
  → K.ActionWithCacheTime r (F.FrameRec CPSVByStateR)
  → K.Sem
      r
      ( K.ActionWithCacheTime
          r
          ( F.FrameRec [BR.Year, GT.StateAbbreviation, BRCF.Count, BRCF.Successes]
          , F.FrameRec [BR.Year, GT.StateAbbreviation, BRCF.Count, BRCF.Successes]
          )
      )
cpsDiagnostics _ cpsByState_C = K.wrapPrefix "cpDiagnostics" $ do
  let cpsCountsByYearAndStateFld =
        FMR.concatFold $
          FMR.mapReduceFold
            FMR.noUnpack
            (FMR.assignKeysAndData @'[BR.Year, GT.StateAbbreviation] @[BRCF.Count, BRCF.Successes])
            (FMR.foldAndAddKey $ FF.foldAllConstrained @Num FL.sum)
      surveyedF = F.rgetField @BRCF.Count
      votedF = F.rgetField @BRCF.Successes
      cvap = F.rgetField @DT.PopCount
      ratio x y = realToFrac @_ @Double x / realToFrac @_ @Double y
      pT r = if surveyedF r == 0 then 0.6 else ratio (votedF r) (surveyedF r)
      compute rw rc =
        let voters = pT rw * realToFrac (cvap rc)
         in (cvap rc F.&: round voters F.&: V.RNil) ∷ F.Record [BRCF.Count, BRCF.Successes]
{-      addTurnout r =
        let cv = realToFrac (F.rgetField @CVAP r)
         in r
              F.<+> ( FT.recordSingleton @Turnout $
                        if cv < 1 then 0 else F.rgetField @Voters r / cv
                    )
-}
  let rawCK = "model/house/rawCPSByState.bin"
  rawCPS_C ← BR.retrieveOrMakeFrame rawCK cpsByState_C $ return . fmap F.rcast . FL.fold cpsCountsByYearAndStateFld
  acsByState_C ← PUMS.pumsLoaderAdults >>= cachedPumsByState
  let psCK = "model/house/psCPSByState.bin"
      psDeps = (,) <$> acsByState_C <*> cpsByState_C
  psCPS_C ← BR.retrieveOrMakeFrame psCK psDeps $ \(acsByState, cpsByState) → do
    let acsFixed = F.filterFrame (\r → F.rgetField @BR.Year r >= 2016) acsByState
        cpsFixed = F.filterFrame (\r → F.rgetField @BR.Year r >= 2016) cpsByState
        (psByState, _, rowDiff) =
          BRPS.joinAndPostStratify @'[BR.Year, GT.StateAbbreviation] @CensusPredictorR @[BRCF.Count, BRCF.Successes] @'[DT.PopCount]
            compute
            (FF.foldAllConstrained @Num FL.sum)
            (F.rcast <$> cpsFixed)
            (F.rcast <$> acsFixed)
    when (rowDiff /= 0) $ K.knitError $ "cpsDiagnostics: joinAndPostStratify join added/lost rows! (diff=)" <> show rowDiff
    pure psByState
  pure $ (,) <$> rawCPS_C <*> psCPS_C

-- CCES diagnostics
type Voters = "Voters" F.:-> Double
type DemVoters = "DemVoters" F.:-> Double
type Turnout = "Turnout" F.:-> Double

type CCESBucketR = [DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.Race5C]

type PSVoted = "PSVoted" F.:-> Double

ccesDiagnostics
  ∷ (K.KnitEffects r, BR.CacheEffects r)
  ⇒ Bool
  → Text
  --                -> CCESVoteSource
  → K.ActionWithCacheTime r (F.FrameRec PUMSByCDR)
  → K.ActionWithCacheTime r (F.FrameRec CCESByCDR)
  → K.Sem r (K.ActionWithCacheTime r (F.FrameRec [BR.Year, GT.StateAbbreviation, CVAP, Surveyed, Voted, PSVoted, PresVotes, PresDVotes, PresRVotes, HouseVotes, HouseDVotes, HouseRVotes]))
ccesDiagnostics clearCaches cacheSuffix acs_C cces_C = K.wrapPrefix "ccesDiagnostics" $ do
  K.logLE K.Info $ "computing CES diagnostics..."
  let surveyedF = F.rgetField @Surveyed
      votedF = F.rgetField @Voted
      ratio x y = realToFrac @_ @Double x / realToFrac @_ @Double y
      pT r = if surveyedF r == 0 then 0.6 else ratio (votedF r) (surveyedF r)
      presVotesF = F.rgetField @PresVotes
      presDVotesF = F.rgetField @PresDVotes
      presRVotesF = F.rgetField @PresRVotes
      houseVotesF = F.rgetField @HouseVotes
      houseDVotesF = F.rgetField @HouseDVotes
      houseRVotesF = F.rgetField @HouseRVotes
      pDP r = if presVotesF r == 0 then 0.5 else ratio (presDVotesF r) (presVotesF r)
      pRP r = if presVotesF r == 0 then 0.5 else ratio (presRVotesF r) (presVotesF r)
      pDH r = if houseVotesF r == 0 then 0.5 else ratio (houseDVotesF r) (houseVotesF r)
      pRH r = if houseVotesF r == 0 then 0.5 else ratio (houseRVotesF r) (houseVotesF r)
      cvap = F.rgetField @DT.PopCount
      addRace5F r = r F.<+> (FT.recordSingleton @DT.Race5C $ DT.race5FromRaceAlone4AndHisp True (F.rgetField @DT.RaceAlone4C r) (F.rgetField @DT.HispC r))
      compute rw rc =
        let psVoted = pT rw * realToFrac (cvap rc)
            presVotesInRace = round $ realToFrac (cvap rc) * ratio (presVotesF rw) (surveyedF rw)
            houseVotesInRace = round $ realToFrac (cvap rc) * ratio (houseVotesF rw) (surveyedF rw)
            dVotesP = round $ realToFrac (cvap rc) * pDP rw
            rVotesP = round $ realToFrac (cvap rc) * pRP rw
            dVotesH = round $ realToFrac (cvap rc) * pDH rw
            rVotesH = round $ realToFrac (cvap rc) * pRH rw
         in (cvap rc F.&: surveyedF rw F.&: votedF rw F.&: psVoted F.&: presVotesInRace F.&: dVotesP F.&: rVotesP F.&: houseVotesInRace F.&: dVotesH F.&: rVotesH F.&: V.RNil) ∷ F.Record [CVAP, Surveyed, Voted, PSVoted, PresVotes, PresDVotes, PresRVotes, HouseVotes, HouseDVotes, HouseRVotes]
      deps = (,) <$> acs_C <*> cces_C
  let statesCK = "diagnostics/ccesPSByPumsStates" <> cacheSuffix <> ".bin"
  when clearCaches $ BR.clearIfPresentD statesCK
  BR.retrieveOrMakeFrame statesCK deps $ \(acs, cces) → do
    let acsFixFld =
          FMR.concatFold $
            FMR.mapReduceFold
              FMR.noUnpack
              (FMR.assignKeysAndData @([BR.Year, GT.StateAbbreviation, GT.CongressionalDistrict] V.++ CCESBucketR) @'[DT.PopCount])
              (FMR.foldAndAddKey $ FF.foldAllConstrained @Num FL.sum)
        acsFixed = FL.fold acsFixFld $ (fmap addRace5F $ F.filterFrame (\r → F.rgetField @BR.Year r >= 2016) acs)
        ccesZero ∷ F.Record [Surveyed, Voted, PresVotes, PresDVotes, PresRVotes, HouseVotes, HouseDVotes, HouseRVotes]
        ccesZero = 0 F.&: 0 F.&: 0 F.&: 0 F.&: 0 F.&: 0 F.&: 0 F.&: 0 F.&: V.RNil
        addZeroFld =
          FMR.concatFold $
            FMR.mapReduceFold
              FMR.noUnpack
              (FMR.assignKeysAndData @[BR.Year, GT.StateAbbreviation, GT.CongressionalDistrict])
              ( FMR.makeRecsWithKey id $
                  FMR.ReduceFold $
                    const $
                      BRK.addDefaultRec @CCESBucketR ccesZero
              )
        ccesWithZeros = FL.fold addZeroFld cces
    let (psByState, _, rowDiff) =
          BRPS.joinAndPostStratify @'[BR.Year, GT.StateAbbreviation] @(GT.CongressionalDistrict ': CCESBucketR) @[Surveyed, Voted, PresVotes, PresDVotes, PresRVotes, HouseVotes, HouseDVotes, HouseRVotes] @'[DT.PopCount]
            compute
            (FF.foldAllConstrained @Num FL.sum)
            (F.rcast <$> ccesWithZeros)
            (F.rcast <$> acsFixed) -- acs has all rows and we don't want to drop any for CVAP sum
            --    when (not $ null missing) $ K.knitError $ "ccesDiagnostics: Missing keys in cces/pums join: " <> show missing
    when (rowDiff /= 0) $ K.knitError $ "ccesDiagnostics: joinAndPostStratify join added/lost rows! (diff=)" <> show rowDiff
    pure psByState

type StateElectionR = ElectionResultR '[BR.Year, GT.StateAbbreviation]
type CDElectionR = ElectionResultR '[BR.Year, GT.StateAbbreviation, GT.CongressionalDistrict]

{-
-- many many people who identify as hispanic also identify as white. So we need to choose.
-- Better to model using both
mergeRace5AndHispanic r =
  let r5 = F.rgetField @DT.Race5C r
      h = F.rgetField @DT.HispC r
  in if (h == DT.Hispanic) then DT.R5_Hispanic else r5

--sldKey r = F.rgetField @GT.StateAbbreviation r <> "-" <> show (F.rgetField @ET.DistrictTypeC r) <> "-" <> show (F.rgetField @ET.DistrictNumber r)
sldKey :: (F.ElemOf rs GT.StateAbbreviation
          ,F.ElemOf rs ET.DistrictTypeC
          ,F.ElemOf rs ET.DistrictNumber)
       => F.Record rs -> SLDLocation
sldKey r = (F.rgetField @GT.StateAbbreviation r
           , F.rgetField @ET.DistrictTypeC r
           , F.rgetField @ET.DistrictNumber r
           )
districtKey r = F.rgetField @GT.StateAbbreviation r <> "-" <> show (F.rgetField @GT.CongressionalDistrict r)

wnh r = (F.rgetField @DT.RaceAlone4C r == DT.RA4_White) && (F.rgetField @DT.HispC r == DT.NonHispanic)
wnhNonGrad r = wnh r && (F.rgetField @DT.CollegeGradC r == DT.NonGrad)
wnhCCES r = (F.rgetField @DT.Race5C r == DT.R5_WhiteNonHispanic) && (F.rgetField @DT.HispC r == DT.NonHispanic)
wnhNonGradCCES r = wnhCCES r && (F.rgetField @DT.CollegeGradC r == DT.NonGrad)

raceAlone4FromRace5 :: DT.Race5 -> DT.RaceAlone4
raceAlone4FromRace5 DT.R5_Other = DT.RA4_Other
raceAlone4FromRace5 DT.R5_Black = DT.RA4_Black
raceAlone4FromRace5 DT.R5_Hispanic = DT.RA4_Other
raceAlone4FromRace5 DT.R5_Asian = DT.RA4_Asian
raceAlone4FromRace5 DT.R5_WhiteNonHispanic = DT.RA4_White
-}
-}

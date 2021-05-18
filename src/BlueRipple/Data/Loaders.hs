{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE Rank2Types #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# OPTIONS_GHC -O0 #-}
{-# OPTIONS_GHC -fplugin=Polysemy.Plugin #-}

module BlueRipple.Data.Loaders where

import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.ElectionTypes as ET
import BlueRipple.Data.LoadersCore
import qualified BlueRipple.Utilities.KnitUtils as BR
import qualified Control.Foldl as FL
import Control.Lens ((%~))
import qualified Data.Map as M
import qualified Data.Sequence as Sequence
import Data.Serialize.Text ()
import qualified Data.Text as T
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V
import qualified Frames as F
import qualified Frames.InCore as FI
import qualified Frames.Melt as F
import qualified Frames.MaybeUtils as FM
import qualified Frames.Serialize as FS
import qualified Frames.Transform as FT
import qualified Knit.Report as K

electoralCollegeFrame ::
  (K.KnitEffects r, BR.CacheEffects r) =>
  K.Sem r (K.ActionWithCacheTime r (F.Frame BR.ElectoralCollege))
electoralCollegeFrame = cachedFrameLoader (DataSets $ T.pack BR.electorsCSV) Nothing Nothing id Nothing "electoralCollege.bin"

parsePEParty :: T.Text -> ET.PartyT
parsePEParty t
  | T.isInfixOf "democrat" t = ET.Democratic
  | T.isInfixOf "republican" t = ET.Republican
  | T.isInfixOf "DEMOCRAT" t = ET.Democratic
  | T.isInfixOf "REPUBLICAN" t = ET.Republican
  | otherwise = ET.Other

type PEFromCols = [BR.Year, BR.State, BR.StatePo, BR.StateFips, BR.StateIc, BR.Office, BR.Candidate, BR.Party, BR.Writein, BR.Candidatevotes, BR.Totalvotes]

type ElectionDataCols = [ET.Office, BR.Candidate, ET.Party, ET.Votes, ET.TotalVotes]

type PresidentialElectionCols = [BR.Year, BR.State, BR.StateAbbreviation, BR.StateFIPS] V.++ ElectionDataCols
type PresidentialElectionColsI = PresidentialElectionCols V.++ '[ET.Incumbent]

fixPresidentialElectionRow ::
  F.Record PEFromCols -> F.Record PresidentialElectionCols
fixPresidentialElectionRow = F.rcast . addCols
  where
    addCols =
      (FT.addName @BR.StatePo @BR.StateAbbreviation)
        . (FT.addName @BR.StateFips @BR.StateFIPS)
        . (FT.addName @BR.Candidatevotes @ET.Votes)
        . (FT.addName @BR.Totalvotes @ET.TotalVotes)
        . (FT.addOneFromValue @ET.Office ET.President)
        . (FT.addOneFromOne @BR.Party @ET.Party parsePEParty)

presidentialByStateFrame ::
  (K.KnitEffects r, BR.CacheEffects r) => K.Sem r (K.ActionWithCacheTime r (F.FrameRec PresidentialElectionCols))
presidentialByStateFrame =
  cachedMaybeFrameLoader @(F.RecordColumns BR.PresidentialByState) @PEFromCols @PresidentialElectionCols
    (DataSets $ toText BR.presidentialByStateCSV)
    Nothing
    Nothing
    id
    fixPresidentialElectionRow
    Nothing
    "presByState.bin"

presidentialElectionKey :: F.Record PresidentialElectionCols -> (Text, Int)
presidentialElectionKey r = (F.rgetField @BR.StateAbbreviation r, F.rgetField @BR.Year r)

presidentialElectionsWithIncumbency ::
  (K.KnitEffects r, BR.CacheEffects r) =>
  K.Sem r (K.ActionWithCacheTime r (F.FrameRec PresidentialElectionColsI))
presidentialElectionsWithIncumbency = do
  presidentialElex_C <- presidentialByStateFrame
  let g elex = fmap (let wm = winnerMap (F.rgetField @ET.Votes) presidentialElectionKey elex in addIncumbency 1 presidentialElectionKey sameCandidate (const False) wm) elex
  --  K.clearIfPresent "data/presidentialWithIncumbency.bin"
  BR.retrieveOrMakeFrame "data/presidentialWithIncumbency.bin" presidentialElex_C (return . g)

type ElectionIntegrityCols = [BR.Year, BR.StateAbbreviation, BR.StateFIPS
                             , BR.PEIRatingstate
                             , BR.PEIVoting, BR.PEIVotingi
                             ]
                             {-
                             , BR.PEILaws, BR.PEILawsi
                             , BR.PEIProcedures, BR.PEIProceduresi
                             , BR.PEIBoundaries, BR.PEIBoundariesi
                             , BR.PEIVotereg, BR.PEIVoteregi
                             , BR.PEIPartyreg, BR.PEIPartyregi
                             , BR.PEIMedia, BR.PEIMediai
                             , BR.PEIFinance, BR.PEIFinancei
                             , BR.PEIVoting, BR.PEIVotingi
                             , BR.PEICount, BR.PEICounti
                             , BR.PEIResults, BR.PEIResultsi
                             , BR.PEIEMBs, BR.PEIEMBsi]
-}
type ElectionIntegrityColsRaw = [BR.PEIYear, BR.PEIStateAbbreviation, BR.PEIStateFIPS
                                , BR.PEIRatingstate
                                , BR.PEIVoting, BR.PEIVotingi
                                ]
{-
                                , BR.PEILaws, BR.PEILawsi
                                , BR.PEIProcedures, BR.PEIProceduresi
                                , BR.PEIBoundaries, BR.PEIBoundariesi
                                , BR.PEIVotereg, BR.PEIVoteregi
                                , BR.PEIPartyreg, BR.PEIPartyregi
                                , BR.PEIMedia, BR.PEIMediai
                                , BR.PEIFinance, BR.PEIFinancei
                                , BR.PEICount, BR.PEICounti
                                , BR.PEIResults, BR.PEIResultsi
                                , BR.PEIEMBs, BR.PEIEMBsi]
-}
electionIntegrityByState2016 ::  (K.KnitEffects r, BR.CacheEffects r) =>
  K.Sem r (K.ActionWithCacheTime r (F.FrameRec ElectionIntegrityCols))
electionIntegrityByState2016 = cachedMaybeFrameLoader
                               @(F.RecordColumns BR.ElectionIntegrityByState2016)
                               @ElectionIntegrityColsRaw
                               (DataSets $ T.pack BR.electionIntegrityByState2016CSV)
                               Nothing
                               Nothing
                               id
                               (F.rcast @ElectionIntegrityCols . addCols)
                               Nothing "electionIntegrityByState2016.bin"
  where
    addCols = (FT.addName @BR.PEIStateAbbreviation @BR.StateAbbreviation)
              . (FT.addName @BR.PEIYear @BR.Year)
              . (FT.addName @BR.PEIStateFIPS @BR.StateFIPS)


cdFromPUMA2012Loader ::
  (K.KnitEffects r, BR.CacheEffects r) =>
  Int ->
  K.Sem r (K.ActionWithCacheTime r (F.Frame BR.CDFromPUMA2012))
cdFromPUMA2012Loader congress = do
  (csvPath, cacheKey) <- case congress of
    113 -> return (BR.cd113FromPUMA2012CSV, "data/cd113FromPUMA2012.bin")
    114 -> return (BR.cd114FromPUMA2012CSV, "data/cd114FromPUMA2012.bin")
    115 -> return (BR.cd115FromPUMA2012CSV, "data/cd115FromPUMA2012.bin")
    116 -> return (BR.cd116FromPUMA2012CSV, "data/cd116FromPUMA2012.bin")
    _ -> K.knitError "PUMA for congressional district crosswalk only available for 113th, 114th, 115th and 116th congress"
  cachedFrameLoader (DataSets $ toText csvPath) Nothing Nothing id Nothing cacheKey --"cd116FromPUMA2012.bin"


type DatedCDFromPUMA2012 = '[BR.Year] V.++ F.RecordColumns BR.CDFromPUMA2012

allCDFromPUMA2012Loader ::
  (K.KnitEffects r, BR.CacheEffects r) =>
  K.Sem r (K.ActionWithCacheTime r (F.FrameRec DatedCDFromPUMA2012))
allCDFromPUMA2012Loader = do
  let addYear :: Int -> BR.CDFromPUMA2012 -> F.Record DatedCDFromPUMA2012
      addYear y r = (y F.&: V.RNil) `V.rappend` r
      loadWithYear :: (K.KnitEffects r, BR.CacheEffects r) => (Int, Int) -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec DatedCDFromPUMA2012))
      loadWithYear (year, congress) = fmap (fmap (addYear year)) <$> cdFromPUMA2012Loader congress
  withYears_C <- sequenceA <$> traverse loadWithYear [(2012, 113), (2014, 114), (2016, 115), (2018, 116)]
  BR.retrieveOrMakeFrame "data/cdFromPUMA2012.bin" withYears_C $ \withYears -> return $ mconcat withYears

{-
puma2000ToCD116Loader :: (K.KnitEffects r, BR.CacheEffects r)
                      => K.Sem r (K.ActionWithCacheTime r (F.Frame BR.PUMA2000ToCD116))
puma2000ToCD116Loader = cachedFrameLoader (DataSets $ T.pack BR.puma2000ToCD116CSV) Nothing Nothing id Nothing "puma2000ToCD116.sbin"
-}
county2010ToCD116Loader ::
  (K.KnitEffects r, BR.CacheEffects r) =>
  K.Sem r (K.ActionWithCacheTime r (F.Frame BR.CountyToCD116))
county2010ToCD116Loader = cachedFrameLoader (DataSets $ toText BR.countyToCD116CSV) Nothing Nothing id Nothing "county2010ToCD116.sbin"

countyToPUMALoader ::
  (K.KnitEffects r, BR.CacheEffects r) =>
  K.Sem r (K.ActionWithCacheTime r (F.Frame BR.CountyFromPUMA))
countyToPUMALoader = cachedFrameLoader (DataSets $ toText BR.county2014FromPUMA2012CSV) Nothing Nothing id Nothing "countyFromPUMA.bin"

aseDemographicsLoader :: (K.KnitEffects r, BR.CacheEffects r) => K.Sem r (K.ActionWithCacheTime r (F.Frame BR.ASEDemographics))
aseDemographicsLoader =
  cachedFrameLoader
    (DataSets $ toText BR.ageSexEducationDemographicsLongCSV)
    Nothing
    Nothing
    id
    Nothing
    "aseDemographics.sbin"

simpleASEDemographicsLoader ::
  (K.KnitEffects r, BR.CacheEffects r) =>
  K.Sem r (K.ActionWithCacheTime r (F.FrameRec (DT.ACSKeys V.++ '[DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, BR.ACSCount])))
simpleASEDemographicsLoader = do
  cachedASE_Demographics <- aseDemographicsLoader -- get cache time and action to decode data if required
  let make aseACSRaw = K.knitEither $ FL.foldM DT.simplifyACS_ASEFold aseACSRaw
  K.retrieveOrMakeTransformed (fmap FS.toS . FL.fold FL.list) (F.toFrame . fmap FS.fromS) ("data/acs_simpleASE.bin" :: T.Text) cachedASE_Demographics make

asrDemographicsLoader ::
  (K.KnitEffects r, BR.CacheEffects r) =>
  K.Sem r (K.ActionWithCacheTime r (F.Frame BR.ASRDemographics))
asrDemographicsLoader =
  cachedFrameLoader
    (DataSets $ toText BR.ageSexRaceDemographicsLongCSV)
    Nothing
    Nothing
    id
    Nothing
    "asrDemographics.sbin"

simpleASRDemographicsLoader ::
  (K.KnitEffects r, BR.CacheEffects r) =>
  K.Sem r (K.ActionWithCacheTime r (F.FrameRec (DT.ACSKeys V.++ '[DT.SimpleAgeC, DT.SexC, DT.SimpleRaceC, BR.ACSCount])))
simpleASRDemographicsLoader = do
  cachedASR_Demographics <- asrDemographicsLoader -- get cache time and action to decode data if required
  let make asrDemographics = K.knitEither $ FL.foldM DT.simplifyACS_ASRFold asrDemographics
  K.retrieveOrMakeTransformed @BR.SerializerC @BR.CacheData
    (fmap FS.toS . FL.fold FL.list)
    (F.toFrame . fmap FS.fromS)
    ("data/acs_simpleASR.bin" :: T.Text)
    cachedASR_Demographics
    make

aseTurnoutLoader :: (K.KnitEffects r, BR.CacheEffects r) => K.Sem r (K.ActionWithCacheTime r (F.Frame BR.TurnoutASE))
aseTurnoutLoader =
  cachedFrameLoader
    (DataSets $ toText BR.detailedASETurnoutCSV)
    Nothing
    Nothing
    id
    Nothing
    "aseTurnout.sbin"

simpleASETurnoutLoader ::
  (K.KnitEffects r, BR.CacheEffects r) =>
  K.Sem r (K.ActionWithCacheTime r (F.FrameRec [BR.Year, DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, BR.Population, BR.Citizen, BR.Registered, BR.Voted]))
simpleASETurnoutLoader = do
  cachedASE_Turnout <- aseTurnoutLoader -- get cache time and action to decode data if required
  let make aseTurnoutRaw = K.knitEither $ FL.foldM DT.simplifyTurnoutASEFold aseTurnoutRaw
  K.retrieveOrMakeTransformed @BR.SerializerC @BR.CacheData
    (fmap FS.toS . FL.fold FL.list)
    (F.toFrame . fmap FS.fromS)
    ("data/turnout_simpleASE.bin" :: T.Text)
    cachedASE_Turnout
    make

asrTurnoutLoader :: (K.KnitEffects r, BR.CacheEffects r) => K.Sem r (K.ActionWithCacheTime r (F.Frame BR.TurnoutASR))
asrTurnoutLoader =
  cachedFrameLoader
    (DataSets $ toText BR.detailedASRTurnoutCSV)
    Nothing
    Nothing
    id
    Nothing
    "asrTurnout.sbin"

simpleASRTurnoutLoader ::
  (K.KnitEffects r, BR.CacheEffects r) =>
  K.Sem r (K.ActionWithCacheTime r (F.FrameRec [BR.Year, DT.SimpleAgeC, DT.SexC, DT.SimpleRaceC, BR.Population, BR.Citizen, BR.Registered, BR.Voted]))
simpleASRTurnoutLoader = do
  cachedASR_Turnout <- asrTurnoutLoader
  let make asrTurnoutRaw = K.knitEither $ FL.foldM DT.simplifyTurnoutASRFold asrTurnoutRaw
  K.retrieveOrMakeTransformed @BR.SerializerC @BR.CacheData
    (fmap FS.toS . FL.fold FL.list)
    (F.toFrame . fmap FS.fromS)
    ("data/turnout_simpleASR.bin" :: T.Text)
    cachedASR_Turnout
    make

rawStateAbbrCrosswalkLoader ::
  (K.KnitEffects r, BR.CacheEffects r) =>
  K.Sem r (K.ActionWithCacheTime r (F.Frame BR.States))
rawStateAbbrCrosswalkLoader = cachedFrameLoader (DataSets $ toText BR.statesCSV) Nothing Nothing id Nothing "statesRaw.bin"
{-# INLINEABLE rawStateAbbrCrosswalkLoader #-}

stateAbbrCrosswalkLoader ::
  (K.KnitEffects r, BR.CacheEffects r) =>
  K.Sem r (K.ActionWithCacheTime r (F.FrameRec [BR.StateName, BR.StateFIPS, BR.StateAbbreviation, DT.CensusRegionC, DT.CensusDivisionC]))
stateAbbrCrosswalkLoader = do
  statesRaw_C <- rawStateAbbrCrosswalkLoader
  BR.retrieveOrMakeFrame "data/stateAbbr.bin" statesRaw_C $ \fRaw ->
    K.knitEither $ F.toFrame <$> (traverse parseCensusCols $ FL.fold FL.list fRaw)
{-# INLINEABLE stateAbbrCrosswalkLoader #-}

parseCensusRegion :: T.Text -> Either Text DT.CensusRegion
parseCensusRegion "Northeast" = Right DT.Northeast
parseCensusRegion "Midwest" = Right DT.Midwest
parseCensusRegion "South" = Right DT.South
parseCensusRegion "West" = Right DT.West
parseCensusRegion "OtherRegion" = Right DT.OtherRegion
parseCensusRegion x = Left $ "Unparsed census region: " <> x

parseCensusDivision :: T.Text -> Either Text DT.CensusDivision
parseCensusDivision "NewEngland" = Right DT.NewEngland
parseCensusDivision "MiddleAtlantic" = Right DT.MiddleAtlantic
parseCensusDivision "EastNorthCentral" = Right DT.EastNorthCentral
parseCensusDivision "WestNorthCentral" = Right DT.WestNorthCentral
parseCensusDivision "SouthAtlantic" = Right DT.SouthAtlantic
parseCensusDivision "EastSouthCentral" = Right DT.EastSouthCentral
parseCensusDivision "WestSouthCentral" = Right DT.WestSouthCentral
parseCensusDivision "Mountain" = Right DT.Mountain
parseCensusDivision "Pacific" = Right DT.Pacific
parseCensusDivision "OtherDivision" = Right DT.OtherDivision
parseCensusDivision x = Left $ "Unparsed census division: " <> x


parseCensusCols :: BR.States -> Either Text (F.Record [BR.StateName, BR.StateFIPS, BR.StateAbbreviation, DT.CensusRegionC, DT.CensusDivisionC])
parseCensusCols r = do
  region <- parseCensusRegion $ F.rgetField @BR.Region r
  division <- parseCensusDivision $ F.rgetField @BR.Division r
  return $ F.rcast @[BR.StateName, BR.StateFIPS, BR.StateAbbreviation] r `V.rappend` (region F.&: division F.&: V.RNil)



type StateTurnoutCols = F.RecordColumns BR.StateTurnout

stateTurnoutLoader ::
  (K.KnitEffects r, BR.CacheEffects r) =>
  K.Sem r (K.ActionWithCacheTime r (F.Frame BR.StateTurnout))
stateTurnoutLoader =
  cachedMaybeFrameLoader @StateTurnoutCols @StateTurnoutCols
    (DataSets $ toText BR.stateTurnoutCSV)
    Nothing
    Nothing
    fixMaybes
    id
    Nothing
    "stateTurnout.sbin"
  where
    missingOETo0 :: F.Rec (Maybe F.:. F.ElField) '[BR.OverseasEligible] -> F.Rec (Maybe F.:. F.ElField) '[BR.OverseasEligible]
    missingOETo0 = FM.fromMaybeMono 0
    missingBCVEPTo0 :: F.Rec (Maybe F.:. F.ElField) '[BR.BallotsCountedVEP] -> F.Rec (Maybe F.:. F.ElField) '[BR.BallotsCountedVEP]
    missingBCVEPTo0 = FM.fromMaybeMono 0
    missingBCTo0 :: F.Rec (Maybe F.:. F.ElField) '[BR.BallotsCounted] -> F.Rec (Maybe F.:. F.ElField) '[BR.BallotsCounted]
    missingBCTo0 = FM.fromMaybeMono 0
    fixMaybes = (F.rsubset %~ missingOETo0) . (F.rsubset %~ missingBCVEPTo0) . (F.rsubset %~ missingBCTo0)


type HouseElectionCols = [BR.Year, BR.State, BR.StateAbbreviation, BR.StateFIPS, BR.CongressionalDistrict] V.++ ([BR.Special, BR.Stage] V.++ ElectionDataCols)

type HouseElectionColsI = HouseElectionCols V.++ '[ET.Incumbent]

isRunoff r = F.rgetField @BR.Stage r == "runoff"

processHouseElectionRow :: BR.HouseElections -> F.Record HouseElectionCols
processHouseElectionRow r = F.rcast @HouseElectionCols (mutate r)
  where
    mutate =
      FT.retypeColumn @BR.StatePo @BR.StateAbbreviation
        . FT.retypeColumn @BR.StateFips @BR.StateFIPS
        . FT.mutate (const $ FT.recordSingleton @ET.Office ET.House)
        . FT.retypeColumn @BR.District @BR.CongressionalDistrict
        . FT.retypeColumn @BR.Candidatevotes @ET.Votes
        . FT.retypeColumn @BR.Totalvotes @ET.TotalVotes
        . FT.mutate
          (FT.recordSingleton @ET.Party . parsePEParty . F.rgetField @BR.Party)

houseElectionsRawLoader ::
  (K.KnitEffects r, BR.CacheEffects r) =>
  K.Sem r (K.ActionWithCacheTime r (F.FrameRec HouseElectionCols))
houseElectionsRawLoader = cachedFrameLoader (DataSets $ toText BR.houseElectionsCSV) Nothing Nothing processHouseElectionRow Nothing "houseElectionsRaw.bin"

houseElectionsLoader ::   (K.KnitEffects r, BR.CacheEffects r) =>
  K.Sem r (K.ActionWithCacheTime r (F.FrameRec HouseElectionCols))
houseElectionsLoader = do
  elexRaw_C <- houseElectionsRawLoader
  BR.retrieveOrMakeFrame "data/houseElections.bin" elexRaw_C $ return . fixRunoffYear houseRaceKey isRunoff


houseElectionsWithIncumbency ::
  (K.KnitEffects r, BR.CacheEffects r) =>
  K.Sem r (K.ActionWithCacheTime r (F.FrameRec HouseElectionColsI))
houseElectionsWithIncumbency = do
  houseElex_C <- houseElectionsLoader
  --K.ignoreCacheTime houseElex_C >>= K.logLE K.Diagnostic . T.pack . show . winnerMap
  let g elex = fmap (let wm = winnerMap (F.rgetField @ET.Votes) houseElectionKey elex in addIncumbency 1 houseElectionKey sameCandidate isRunoff wm) elex
  --  K.clearIfPresent "data/houseWithIncumbency.bin"
  BR.retrieveOrMakeFrame "data/houseWithIncumbency.bin" houseElex_C (return . g)

fixRunoffYear :: forall rs raceKey f. (FI.RecVec rs, Foldable f, Ord raceKey, F.ElemOf rs BR.Year)
  => (F.Record rs -> raceKey) -> (F.Record rs -> Bool) -> f (F.Record rs) -> F.FrameRec rs
fixRunoffYear rKey runoff rows = flip evalState M.empty $ FL.foldM g rows where
  year = F.rgetField @BR.Year
  g :: FL.FoldM (State (Map raceKey Int)) (F.Record rs) (F.FrameRec rs)
  g = FL.FoldM step (return Sequence.empty) (return . F.toFrame) where
    step :: Sequence.Seq (F.Record rs) -> F.Record rs -> State (Map raceKey Int) (Sequence.Seq (F.Record rs))
    step s r = if not $ runoff r
               then modify (M.insert (rKey r) (year r)) >> return (s <> Sequence.singleton r)
               else (do
                        year <- fromMaybe (year r) <$> gets (M.lookup $ rKey r)
                        let r' = FT.fieldEndo @BR.Year (const year) r
                        return (s <> Sequence.singleton r')
                    )

winnerMap :: forall rs f kl ky.(Foldable f, Ord kl, Ord ky)
          => (F.Record rs -> Int)
          -> (F.Record rs -> (kl, ky))
          -> f (F.Record rs)
          -> M.Map kl (Map ky (F.Record rs))
winnerMap votes key = FL.fold (FL.Fold g mempty id)
  where
    chooseWinner :: F.Record rs -> F.Record rs -> F.Record rs
    chooseWinner a b = if votes a > votes b then a else b
    g :: M.Map kl (M.Map ky (F.Record rs)) -> F.Record rs -> M.Map kl (M.Map ky (F.Record rs))
    g m r =
      let (kl, ky) = key r
          mi =  case M.lookup kl m of
            Nothing -> one (ky, r)
            Just mi' -> M.insertWith chooseWinner ky r mi'
      in M.insert kl mi m

lastWinners :: (Ord kl, Ord ky) => Int -> (F.Record rs -> (kl, ky)) -> M.Map kl (Map ky (F.Record rs)) -> F.Record rs -> Maybe [F.Record rs]
lastWinners n key winnerMap r = do
  let (kl, ky) = key r
  ml <- M.lookup kl winnerMap
  let ascendingWinners = M.toAscList ml
      lastLess :: forall a b.Ord a => Int -> a -> [(a, b)] -> Maybe (a, b)
      lastLess n a abs =
        let paired = zip abs $ drop n abs
            go :: [((a, b), (a, b))] -> Maybe (a, b)
            go [] = Nothing
            go (((al, bl), (ar, br)) : pabs) = if a > al && a <= ar then Just (al, bl) else go pabs
        in go paired
  fmap snd <$> traverse (\m -> lastLess m ky ascendingWinners) [1..n]

houseRaceKey :: F.Record HouseElectionCols -> (Text, Int)
houseRaceKey r = (F.rgetField @BR.StateAbbreviation r, F.rgetField @BR.CongressionalDistrict r)
houseElectionKey :: F.Record HouseElectionCols -> ((Text, Int), Int)
houseElectionKey r = (houseRaceKey r, F.rgetField @BR.Year r)

sameCandidate :: F.ElemOf rs BR.Candidate => F.Record rs -> F.Record rs -> Bool
sameCandidate a b = F.rgetField @BR.Candidate a == F.rgetField @BR.Candidate b

addIncumbency :: (Ord kl, Ord ky)
  => Int
  -> (F.Record rs -> (kl, ky))
  -> (F.Record rs -> F.Record rs -> Bool)
  -> (F.Record rs -> Bool)
  -> M.Map kl (M.Map ky (F.Record rs))
  -> F.Record rs
  -> F.Record (rs V.++ '[ET.Incumbent])
addIncumbency n key sameCand runoff wm r =
  let lws = lastWinners (n+1) key wm r
      lwsFixed = if runoff r
                 then fmap (take n) lws -- we got n + 1 and drop the last since that's the general leading to this runoff, not the previous
                 else fmap (drop 1) lws -- not a runoff so get rid of the extra one
      incumbent = case lwsFixed of
        Nothing -> False
        Just prs -> not $ null $ filter (sameCand r) prs
  in r V.<+> ((incumbent F.&: V.RNil) :: F.Record '[ET.Incumbent])

fixAtLargeDistricts :: (F.ElemOf rs BR.StateAbbreviation, F.ElemOf rs BR.CongressionalDistrict, Functor f) => Int -> f (F.Record rs) -> f (F.Record rs)
fixAtLargeDistricts n = fmap fixOne where
  statesWithAtLargeCDs = ["AK", "DE", "MT", "ND", "SD", "VT", "WY"]
  fixOne r = if F.rgetField @BR.StateAbbreviation r `elem` statesWithAtLargeCDs then F.rputField @BR.CongressionalDistrict n r else r


type SenateElectionCols = [BR.Year, BR.State, BR.StateAbbreviation, BR.StateFIPS] V.++ ([BR.Special, BR.Stage] V.++ ElectionDataCols)

type SenateElectionColsI = SenateElectionCols V.++ '[ET.Incumbent]
-- NB: If there are 2 specials at the same time, this will fail to distinguish them. :(
senateRaceKey :: F.Record SenateElectionCols -> (Text, Bool)
senateRaceKey r = (F.rgetField @BR.StateAbbreviation r, F.rgetField @BR.Special r)

senateElectionKey :: F.Record SenateElectionCols -> ((Text, Bool), Int)
senateElectionKey r = (senateRaceKey r, F.rgetField @BR.Year r)

processSenateElectionRow :: BR.SenateElections -> F.Record SenateElectionCols
processSenateElectionRow r = F.rcast @SenateElectionCols (mutate r)
  where
    mutate =
      FT.retypeColumn @BR.SenateStatePo @BR.StateAbbreviation
        . FT.retypeColumn @BR.SenateStateFips @BR.StateFIPS
        . FT.retypeColumn @BR.SenateYear @BR.Year
        . FT.retypeColumn @BR.SenateState @BR.State
        . FT.retypeColumn @BR.SenateStage @BR.Stage
        . FT.retypeColumn @BR.SenateSpecial @BR.Special
        . FT.mutate (const $ FT.recordSingleton @ET.Office ET.Senate)
        . FT.retypeColumn @BR.SenateCandidatevotes @ET.Votes
        . FT.retypeColumn @BR.SenateCandidate @BR.Candidate
        . FT.retypeColumn @BR.SenateTotalvotes @ET.TotalVotes
        . FT.mutate
          (FT.recordSingleton @ET.Party . parsePEParty . F.rgetField @BR.SenatePartyDetailed)

senateElectionsRawLoader ::
  (K.KnitEffects r, BR.CacheEffects r) =>
  K.Sem r (K.ActionWithCacheTime r (F.FrameRec SenateElectionCols))
senateElectionsRawLoader = cachedFrameLoader (DataSets $ toText BR.senateElectionsCSV) Nothing Nothing processSenateElectionRow Nothing "senateElectionsRaw.bin"

senateElectionsLoader ::   (K.KnitEffects r, BR.CacheEffects r) =>
  K.Sem r (K.ActionWithCacheTime r (F.FrameRec SenateElectionCols))
senateElectionsLoader = do
  elexRaw_C <- senateElectionsRawLoader
  BR.retrieveOrMakeFrame "data/senateElections.bin" elexRaw_C $ return . fixRunoffYear senateRaceKey isRunoff


senateElectionsWithIncumbency ::
  (K.KnitEffects r, BR.CacheEffects r) =>
  K.Sem r (K.ActionWithCacheTime r (F.FrameRec SenateElectionColsI))
senateElectionsWithIncumbency = do
  senateElex_C <- senateElectionsLoader
  --K.ignoreCacheTime houseElex_C >>= K.logLE K.Diagnostic . T.pack . show . winnerMap
  let g elex = fmap (let wm = winnerMap (F.rgetField @ET.Votes) senateElectionKey elex in addIncumbency 2 senateElectionKey sameCandidate isRunoff wm) elex
  --  K.clearIfPresent "data/houseWithIncumbency.bin"
  BR.retrieveOrMakeFrame "data/senateWithIncumbency.bin" senateElex_C (return . g)

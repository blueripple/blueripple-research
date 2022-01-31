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
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}
{-# OPTIONS_GHC -O0 #-}

module BlueRipple.Model.House.ElectionResult where

import Prelude hiding (pred)
import qualified BlueRipple.Configuration as BR
import qualified BlueRipple.Data.ACS_PUMS as PUMS
import qualified BlueRipple.Data.CCES as CCES
import qualified BlueRipple.Data.CPSVoterPUMS as CPS
import qualified BlueRipple.Data.CensusTables as Census
import qualified BlueRipple.Data.CensusLoaders as Census
import qualified BlueRipple.Data.CountFolds as BRCF
import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.ElectionTypes as ET
import qualified BlueRipple.Data.ModelingTypes as MT
import qualified BlueRipple.Data.Keyed as BRK
import qualified BlueRipple.Data.Loaders as BR
import qualified BlueRipple.Model.TurnoutAdjustment as BRTA
import qualified BlueRipple.Model.PostStratify as BRPS
import qualified BlueRipple.Utilities.KnitUtils as BR
import qualified BlueRipple.Utilities.FramesUtils as BRF
import qualified BlueRipple.Model.StanMRP as MRP
import qualified Numeric
import qualified CmdStan as CS
import qualified Control.Foldl as FL
import qualified Data.Aeson as A
import qualified Data.List as List
import qualified Data.Map as M
import qualified Data.IntMap as IM
import qualified Data.Set as Set
import Data.String.Here (here)
import qualified Data.Serialize                as S
import qualified Data.Text as T
import qualified Data.Vector as Vec
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V
import qualified Data.Vector as Vector
import qualified Data.Vector.Unboxed as VU
import qualified Flat
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
import qualified Stan.ModelBuilder.BuildingBlocks as SB
import qualified Stan.ModelBuilder.GroupModel as SB
import qualified Stan.ModelBuilder.FixedEffects as SFE
import qualified Stan.ModelBuilder.DesignMatrix as DM
import qualified Stan.ModelConfig as SC
import qualified Stan.ModelRunner as SM
import qualified Stan.Parameters as SP
import qualified Stan.RScriptBuilder as SR
import qualified BlueRipple.Data.CCES as CCES
import BlueRipple.Data.CCESFrame (cces2018C_CSV)
import BlueRipple.Data.ElectionTypes (CVAP)
import qualified Control.MapReduce as FMR
import qualified Frames.MapReduce as FMR
import qualified Stan.ModelBuilder.DesignMatrix as DM
import qualified Stan.ModelBuilder.DesignMatrix as DM
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified Stan.ModelBuilder as SB

type FracUnder45 = "FracUnder45" F.:-> Double

type FracFemale = "FracFemale" F.:-> Double

type FracGrad = "FracGrad" F.:-> Double

type FracWhiteNonHispanic = "FracWhiteNonHispanic" F.:-> Double
type FracWhiteHispanic = "FracWhiteHispanic" F.:-> Double
type FracNonWhiteHispanic = "FracNonWhiteHispanic" F.:-> Double
type FracBlack = "FracBlack" F.:-> Double
type FracAsian = "FracAsian" F.:-> Double
type FracOther = "FracOther" F.:-> Double
type FracWhiteGrad = "FracWhiteGrad" F.:-> Double -- fraction of white people who are college grads

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
  , FracWhiteGrad
  , DT.AvgIncome
  , DT.PopPerSqMile
  , PUMS.Citizens
  ]

type DVotes = "DVotes" F.:-> Int
type RVotes = "RVotes" F.:-> Int
type TVotes = "TVotes" F.:-> Int
type Voted = "Voted" F.:-> Int
type HouseVotes = "HouseVotes" F.:-> Int
type HouseDVotes = "HouseDVotes" F.:-> Int
type PresVotes = "PresVotes" F.:-> Int
type PresDVotes = "PresDVotes" F.:-> Int

type DVotesW = "DVotesW" F.:-> Double
type RVotesW = "RVotesW" F.:-> Double
type TVotesW = "TVotesW" F.:-> Double
type VotedW = "VotedW" F.:-> Double
type HouseVotesW = "HouseVotesW" F.:-> Double
type HouseDVotesW = "HouseDVotesW" F.:-> Double
type PresVotesW = "PresVotesW" F.:-> Double
type PresDVotesW = "PresDVotesW" F.:-> Double


-- +1 for Dem incumbent, 0 for no incumbent, -1 for Rep incumbent
type Incumbency = "Incumbency" F.:-> Int
type ElectionR = [Incumbency, DVotes, RVotes, TVotes]
type ElectionPredictorR = [FracUnder45
                          , FracFemale
                          , FracGrad
                          , FracWhiteNonHispanic
                          , FracWhiteHispanic
                          , FracNonWhiteHispanic
                          , FracBlack
                          , FracAsian
                          , FracOther
                          , FracWhiteGrad
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
type SurveyedW = "SurveyedW" F.:-> Double  -- total people in each bucket (Weighted)
type CCESVotingDataR = [Surveyed, Voted, HouseVotes, HouseDVotes, PresVotes, PresDVotes]
type CCESVotingDataWR = [SurveyedW, VotedW, HouseVotesW, HouseDVotesW, PresVotesW, PresDVotesW]
type CCESByCDR = CDKeyR V.++ [DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.Race5C, DT.HispC] V.++ CCESVotingDataR
type CCESWByCDR = CDKeyR V.++ [DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.Race5C, DT.HispC] V.++ CCESVotingDataWR
type CCESDataR = CCESByCDR V.++ [Incumbency, DT.AvgIncome, DT.PopPerSqMile]
type CCESWDataR = CCESWByCDR V.++ [Incumbency, DT.AvgIncome, DT.PopPerSqMile]
type CCESPredictorR = [DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.Race5C, DT.HispC] --, DT.AvgIncome, DT.PopPerSqMile]
type CCESData = F.FrameRec CCESDataR
type CCESWData = F.FrameRec CCESWDataR

data DemographicSource = DS_1YRACSPUMS | DS_5YRACSTracts deriving (Show, Eq, Ord)
demographicSourceLabel :: DemographicSource -> Text
demographicSourceLabel DS_1YRACSPUMS = "ACS1P"
demographicSourceLabel DS_5YRACSTracts = "ACS5T"

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

instance Flat.Flat HouseModelData where
  size (HouseModelData h s p c) n = Flat.size (FS.SFrame h, FS.SFrame s, FS.SFrame p, FS.SFrame c) n
  encode (HouseModelData h s p c) = Flat.encode (FS.SFrame h, FS.SFrame s, FS.SFrame p, FS.SFrame c)
  decode = (\(h, s, p, c) -> HouseModelData (FS.unSFrame h) (FS.unSFrame s) (FS.unSFrame p) (FS.unSFrame c)) <$> Flat.decode

type PUMSDataR = [DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.RaceAlone4C, DT.HispC, DT.AvgIncome, DT.PopPerSqMile, PUMS.Citizens, PUMS.NonCitizens]

type PUMSByCDR = CDKeyR V.++ PUMSDataR
type PUMSByCD = F.FrameRec PUMSByCDR
type PUMSByStateR = StateKeyR V.++ PUMSDataR
type PUMSByState = F.FrameRec PUMSByStateR
-- unweighted, which we address via post-stratification
type CensusPredictorR = [DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.RaceAlone4C, DT.HispC]
type CPSVDataR  = CensusPredictorR V.++ BRCF.CountCols
type CPSVByStateR = StateKeyR V.++ CPSVDataR
type CPSVByCDR = CDKeyR V.++ CPSVDataR

type DistrictDataR = CDKeyR V.++ [DT.PopPerSqMile, DT.AvgIncome] V.++ ElectionR
type DistrictDemDataR = CDKeyR V.++ [PUMS.Citizens, DT.PopPerSqMile, DT.AvgIncome]
type StateDemDataR = StateKeyR V.++ [PUMS.Citizens, DT.PopPerSqMile, DT.AvgIncome]
type AllCatR = '[BR.Year] V.++ CensusPredictorR
data CCESAndPUMS = CCESAndPUMS { ccesRows :: F.FrameRec CCESWithDensity
                               , cpsVRows :: F.FrameRec CPSVWithDensity
                               , pumsRows :: F.FrameRec PUMSWithDensity
                               , districtRows :: F.FrameRec DistrictDemDataR
--                               , allCategoriesRows :: F.FrameRec AllCatR
                               } deriving (Generic)

ccesAndPUMSForYear :: Int -> CCESAndPUMS -> CCESAndPUMS
ccesAndPUMSForYear y = ccesAndPUMSForYears [y]

ccesAndPUMSForYears :: [Int] -> CCESAndPUMS -> CCESAndPUMS
ccesAndPUMSForYears ys (CCESAndPUMS cces cpsV pums dist) =
  let f :: (FI.RecVec rs, F.ElemOf rs BR.Year) => F.FrameRec rs -> F.FrameRec rs
      f = F.filterFrame ((`elem` ys) . F.rgetField @BR.Year)
  in CCESAndPUMS (f cces) (f cpsV) (f pums) (f dist)


instance S.Serialize CCESAndPUMS where
  put (CCESAndPUMS cces cpsV pums dist) = S.put (FS.SFrame cces, FS.SFrame cpsV, FS.SFrame pums, FS.SFrame dist)
  get = (\(cces, cpsV, pums, dist) -> CCESAndPUMS (FS.unSFrame cces) (FS.unSFrame cpsV) (FS.unSFrame pums) (FS.unSFrame dist)) <$> S.get

instance Flat.Flat CCESAndPUMS where
  size (CCESAndPUMS cces cpsV pums dist) n = Flat.size (FS.SFrame cces, FS.SFrame cpsV, FS.SFrame pums, FS.SFrame dist) n
  encode (CCESAndPUMS cces cpsV pums dist) = Flat.encode (FS.SFrame cces, FS.SFrame cpsV, FS.SFrame pums, FS.SFrame dist)
  decode = (\(cces, cpsV, pums, dist) -> CCESAndPUMS (FS.unSFrame cces) (FS.unSFrame cpsV) (FS.unSFrame pums) (FS.unSFrame dist)) <$> Flat.decode

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
      white r = race4A r == DT.RA4_White
      whiteGrad r = (white r) && (F.rgetField @DT.CollegeGradC r == DT.Grad)
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
     V.:& FF.toFoldRecord (intRatio <$> FL.prefilter whiteGrad citF <*> FL.prefilter white citF)
     V.:& FF.toFoldRecord (citWgtdF (F.rgetField @DT.AvgIncome))
     V.:& FF.toFoldRecord (citWgtdF (F.rgetField @DT.PopPerSqMile))
     V.:& FF.toFoldRecord citF
     V.:& V.RNil

-- This is the thing to apply to loaded result data (with incumbents)
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
    totalVotes =  FL.premap votes FL.sum
    demVotesF = FL.generalize $ FL.prefilter (\r -> party r == ET.Democratic) $ totalVotes
    repVotesF = FL.generalize $ FL.prefilter (\r -> party r == ET.Republican) $ totalVotes
    flattenF = (\ii dv rv tv -> ii F.&: dv F.&: rv F.&: tv F.&: V.RNil) <$> incumbentPartyF <*> demVotesF <*> repVotesF <*> FL.generalize totalVotes

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
                     (FMR.assignKeysAndData @[BR.Year, BR.StateAbbreviation, BR.CongressionalDistrict, DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.Race5C, DT.HispC])
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

countCESVotesF :: FL.Fold
                  (F.Record [CCES.CatalistTurnoutC, CCES.MHouseVoteParty, CCES.MPresVoteParty])
                  (F.Record [Surveyed, Voted, HouseVotes, HouseDVotes, PresVotes, PresDVotes])
countCESVotesF =
  let houseVote (MT.MaybeData x) = maybe False (const True) x
      houseDVote (MT.MaybeData x) = maybe False (== ET.Democratic) x
      presVote (MT.MaybeData x) = maybe False (const True) x
      presDVote (MT.MaybeData x) = maybe False (== ET.Democratic) x
      surveyedF = FL.length
      votedF = FL.prefilter (CCES.catalistVoted . F.rgetField @CCES.CatalistTurnoutC) FL.length
      houseVotesF = FL.prefilter (houseVote . F.rgetField @CCES.MHouseVoteParty) votedF
      houseDVotesF = FL.prefilter (houseDVote . F.rgetField @CCES.MHouseVoteParty) votedF
      presVotesF = FL.prefilter (presVote . F.rgetField @CCES.MPresVoteParty) votedF
      presDVotesF = FL.prefilter (presDVote . F.rgetField @CCES.MPresVoteParty) votedF
  in (\s v hv hdv pv pdv -> s F.&: v F.&: hv F.&: hdv F.&: pv F.&: pdv F.&: V.RNil)
     <$> surveyedF <*> votedF <*> houseVotesF <*> houseDVotesF <*> presVotesF <*> presDVotesF

countCESVotesWF :: FL.Fold
                  (F.Record [CCES.CESWeight, CCES.CatalistTurnoutC, CCES.MHouseVoteParty, CCES.MPresVoteParty])
                  (F.Record [SurveyedW, VotedW, HouseVotesW, HouseDVotesW, PresVotesW, PresDVotesW])
countCESVotesWF =
  let houseVote (MT.MaybeData x) = maybe False (const True) x
      houseDVote (MT.MaybeData x) = maybe False (== ET.Democratic) x
      presVote (MT.MaybeData x) = maybe False (const True) x
      presDVote (MT.MaybeData x) = maybe False (== ET.Democratic) x
      surveyedF = FL.premap (F.rgetField @CCES.CESWeight) FL.sum
      votedF = FL.prefilter (CCES.catalistVoted . F.rgetField @CCES.CatalistTurnoutC) surveyedF
      houseVotesF = FL.prefilter (houseVote . F.rgetField @CCES.MHouseVoteParty) votedF
      houseDVotesF = FL.prefilter (houseDVote . F.rgetField @CCES.MHouseVoteParty) votedF
      presVotesF = FL.prefilter (presVote . F.rgetField @CCES.MPresVoteParty) votedF
      presDVotesF = FL.prefilter (presDVote . F.rgetField @CCES.MPresVoteParty) votedF
  in (\s v hv hdv pv pdv -> s F.&: v F.&: hv F.&: hdv F.&: pv F.&: pdv F.&: V.RNil)
     <$> surveyedF <*> votedF <*> houseVotesF <*> houseDVotesF <*> presVotesF <*> presDVotesF


-- using each year's common content
cesMR :: (Foldable f, Monad m) => Int -> f (F.Record CCES.CESPR) -> m (F.FrameRec CCESByCDR)
cesMR earliestYear = BRF.frameCompactMRM
                     (FMR.unpackFilterOnField @BR.Year (>= earliestYear))
                     (FMR.assignKeysAndData @[BR.Year, BR.StateAbbreviation, BR.CongressionalDistrict, DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.Race5C, DT.HispC])
                     countCESVotesF

cesFold :: Int -> FL.Fold (F.Record CCES.CESPR) (F.FrameRec CCESByCDR)
cesFold earliestYear = FMR.concatFold
          $ FMR.mapReduceFold
          (FMR.unpackFilterOnField @BR.Year (>= earliestYear))
          (FMR.assignKeysAndData @[BR.Year, BR.StateAbbreviation, BR.CongressionalDistrict, DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.Race5C, DT.HispC])
          (FMR.foldAndAddKey countCESVotesF)

-- using each year's common content
cesWMR :: (Foldable f, Monad m) => Int -> f (F.Record CCES.CESPR) -> m (F.FrameRec CCESWByCDR)
cesWMR earliestYear = BRF.frameCompactMRM
                     (FMR.unpackFilterOnField @BR.Year (>= earliestYear))
                     (FMR.assignKeysAndData @[BR.Year, BR.StateAbbreviation, BR.CongressionalDistrict, DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.Race5C, DT.HispC])
                     countCESVotesWF


cesCountedDemVotesByCD :: (K.KnitEffects r, BR.CacheEffects r) => Bool -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec CCESByCDR))
cesCountedDemVotesByCD clearCaches = do
  ces2020_C <- CCES.ces20Loader
  let cacheKey = "model/house/ces20ByCD.bin"
  when clearCaches $  BR.clearIfPresentD cacheKey
  BR.retrieveOrMakeFrame cacheKey ces2020_C $ \ces -> cesMR 2020 ces
--  BR.retrieveOrMakeFrame cacheKey ces2020_C $ return . FL.fold (cesFold 2020)

cesWCountedDemVotesByCD :: (K.KnitEffects r, BR.CacheEffects r) => Bool -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec CCESWByCDR))
cesWCountedDemVotesByCD clearCaches = do
  ces2020_C <- CCES.ces20Loader
  let cacheKey = "model/house/cesW20ByCD.bin"
  when clearCaches $  BR.clearIfPresentD cacheKey
  BR.retrieveOrMakeFrame cacheKey ces2020_C $ \ces -> do
    cesWMR 2020 ces

-- NB: CDKey includes year
cpsCountedTurnoutByCD :: (K.KnitEffects r, BR.CacheEffects r) => K.Sem r (K.ActionWithCacheTime r (F.FrameRec CPSVByCDR))
cpsCountedTurnoutByCD = do
  let afterYear y r = F.rgetField @BR.Year r >= y
      possible r = CPS.cpsPossibleVoter $ F.rgetField @ET.VotedYNC r
      citizen r = F.rgetField @DT.IsCitizen r
      includeRow r = afterYear 2012 r &&  possible r && citizen r
      voted r = CPS.cpsVoted $ F.rgetField @ET.VotedYNC r
      wgt r = {- F.rgetField @CPSVoterPUMSWeight r * -} F.rgetField @BR.CountyWeight r
      fld = BRCF.weightedCountFold @_ @(CPS.CPSVoterPUMS V.++ [BR.CongressionalDistrict, BR.CountyWeight])
            (\r -> F.rcast @CDKeyR r `V.rappend` CPS.cpsKeysToASER4H True (F.rcast r))
            (F.rcast  @[ET.VotedYNC, CPS.CPSVoterPUMSWeight, BR.CountyWeight])
            includeRow
            voted
            wgt
  cpsRaw_C <- CPS.cpsVoterPUMSWithCDLoader -- NB: this is only useful for CD rollup since counties may appear in multiple CDs.
  BR.retrieveOrMakeFrame "model/house/cpsVByCD.bin" cpsRaw_C $ return . FL.fold fld

-- NB: StateKey includes year
cpsCountedTurnoutByState :: (K.KnitEffects r, BR.CacheEffects r) => K.Sem r (K.ActionWithCacheTime r (F.FrameRec CPSVByStateR))
cpsCountedTurnoutByState = do
  let afterYear y r = F.rgetField @BR.Year r >= y
      possible r = CPS.cpsPossibleVoter $ F.rgetField @ET.VotedYNC r
      citizen r = F.rgetField @DT.IsCitizen r
      includeRow r = afterYear 2012 r &&  possible r && citizen r
      voted r = CPS.cpsVoted $ F.rgetField @ET.VotedYNC r
      wgt r = F.rgetField @CPS.CPSVoterPUMSWeight r
      fld = BRCF.weightedCountFold @_ @CPS.CPSVoterPUMS
            (\r -> F.rcast @StateKeyR r `V.rappend` CPS.cpsKeysToASER4H True (F.rcast r))
            (F.rcast  @[ET.VotedYNC, CPS.CPSVoterPUMSWeight])
            includeRow
            voted
            wgt
  cpsRaw_C <- CPS.cpsVoterPUMSLoader -- NB: this is only useful for CD rollup since counties may appear in multiple CDs.
  BR.retrieveOrMakeFrame "model/house/cpsVByState.bin" cpsRaw_C $ return . FL.fold fld

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

copy2019to2020 :: (FI.RecVec rs, F.ElemOf rs BR.Year) => F.FrameRec rs -> F.FrameRec rs
copy2019to2020 rows = rows <> fmap changeYear2020 (F.filterFrame year2019 rows) where
  year2019 r = F.rgetField @BR.Year r == 2019
  changeYear2020 r = F.rputField @BR.Year 2020 r


type SenateRaceKeyR = [BR.Year, BR.StateAbbreviation, BR.Special, BR.Stage]

type ElexDataR = [ET.Office, BR.Stage, BR.Runoff, BR.Special, BR.Candidate, ET.Party, ET.Votes, ET.Incumbent]

type HouseModelCensusTablesByCD =
  Census.CensusTables Census.LDLocationR Census.ExtensiveDataR DT.Age5FC DT.SexC DT.CollegeGradC Census.RaceEthnicityC DT.IsCitizen Census.EmploymentC

type HouseModelCensusTablesByState =
  Census.CensusTables '[BR.StateFips] Census.ExtensiveDataR DT.Age5FC DT.SexC DT.CollegeGradC Census.RaceEthnicityC DT.IsCitizen Census.EmploymentC

pumsByCD :: (K.KnitEffects r, BR.CacheEffects r) => F.FrameRec PUMS.PUMS -> F.FrameRec BR.DatedCDFromPUMA2012 -> K.Sem r (F.FrameRec PUMSByCDR)
pumsByCD pums cdFromPUMA =  fmap F.rcast <$> PUMS.pumsCDRollup (earliest earliestYear) (pumsReKey . F.rcast) cdFromPUMA pums
  where
    earliestYear = 2016
    earliest year = (>= year) . F.rgetField @BR.Year

cachedPumsByCD :: forall r.(K.KnitEffects r, BR.CacheEffects r)
               => K.ActionWithCacheTime r (F.FrameRec PUMS.PUMS)
               -> K.ActionWithCacheTime r (F.FrameRec BR.DatedCDFromPUMA2012)
               -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec PUMSByCDR))
cachedPumsByCD pums_C cdFromPUMA_C = do
  let pumsByCDDeps = (,) <$> pums_C <*> cdFromPUMA_C
  fmap (fmap copy2019to2020)
    $ BR.retrieveOrMakeFrame "model/house/pumsByCD.bin" pumsByCDDeps
    $ \(pums, cdFromPUMA) -> pumsByCD pums cdFromPUMA


pumsByState :: F.FrameRec PUMS.PUMS -> F.FrameRec PUMSByStateR
pumsByState pums = F.rcast <$> FL.fold (PUMS.pumsStateRollupF (pumsReKey . F.rcast)) filteredPums
  where
    earliestYear = 2016
    earliest year = (>= year) . F.rgetField @BR.Year
    filteredPums = F.filterFrame (earliest earliestYear) pums

cachedPumsByState :: forall r.(K.KnitEffects r, BR.CacheEffects r)
               => K.ActionWithCacheTime r (F.FrameRec PUMS.PUMS)
               -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec (StateKeyR V.++ CensusPredictorR V.++ '[PUMS.Citizens])))
cachedPumsByState pums_C = do
  let zeroCount :: F.Record '[PUMS.Citizens]
      zeroCount = 0 F.&: V.RNil
      addZeroF = FMR.concatFold
                 $ FMR.mapReduceFold
                 FMR.noUnpack
                 (FMR.assignKeysAndData @StateKeyR @(CensusPredictorR V.++ '[PUMS.Citizens]))
                 (FMR.makeRecsWithKey id
                  $ FMR.ReduceFold
                  $ const
                  $ BRK.addDefaultRec @CensusPredictorR zeroCount
                 )
  fmap (fmap copy2019to2020)
    $ BR.retrieveOrMakeFrame "model/house/pumsByState.bin" pums_C
    $ pure . FL.fold addZeroF . pumsByState


adjUsing :: forall x n d rs a.(V.KnownField x
                              , F.ElemOf rs x
                              , V.Snd x ~ Double
                              , V.KnownField n
                              , F.ElemOf rs n
                              , V.Snd n ~ a
                              , V.KnownField d
                              , F.ElemOf rs d
                              , V.Snd d ~ a
                              )
         => (a -> Double) -> (Double -> a) -> F.Record rs -> F.Record rs
adjUsing toDbl fromDbl r = F.rputField @n (fromDbl $ F.rgetField @x r * toDbl (F.rgetField @d r)) r

prepCCESAndPums :: forall r.(K.KnitEffects r, BR.CacheEffects r) => Bool -> K.Sem r (K.ActionWithCacheTime r CCESAndPUMS)
prepCCESAndPums clearCache = do
  let earliestYear = 2016 -- set by ces for now
      earliest year = (>= year) . F.rgetField @BR.Year
      fixDC_CD r = if (F.rgetField @BR.StateAbbreviation r == "DC")
                   then FT.fieldEndo @BR.CongressionalDistrict (const 1) r
                   else r
      fLength = FL.fold FL.length
      lengthInYear y = fLength . F.filterFrame ((== y) . F.rgetField @BR.Year)
  pums_C <- PUMS.pumsLoaderAdults
  cdFromPUMA_C <- BR.allCDFromPUMA2012Loader
  pumsByCD_C <- cachedPumsByCD pums_C cdFromPUMA_C
  pumsByState_C <- cachedPumsByState pums_C
  countedCCES_C <- fmap (BR.fixAtLargeDistricts 0) <$> cesCountedDemVotesByCD clearCache
  cpsVByState_C <- fmap (F.filterFrame $ earliest earliestYear) <$> cpsCountedTurnoutByState
  K.ignoreCacheTime cpsVByState_C >>= cpsDiagnostics "Pre Achen/Hur"

  -- Do the turnout corrections, CPS first
  stateTurnout_C <- BR.stateTurnoutLoader
  let cpsAchenHurDeps = (,,) <$> cpsVByState_C <*> pumsByState_C <*> stateTurnout_C
      cpsAchenHurCacheKey = "model/house/CPSV_AchenHur.bin"
  when clearCache $ BR.clearIfPresentD cpsAchenHurCacheKey
  cpsV_AchenHur_C <- BR.retrieveOrMakeFrame cpsAchenHurCacheKey cpsAchenHurDeps $ \(cpsV, acsByState, stateTurnout) -> do
    K.logLE K.Diagnostic $ "Pre Achen-Hur: CPS (by state) rows per year:"
    K.logLE K.Diagnostic $ show $ fmap (\y -> lengthInYear y cpsV) [2012, 2014, 2016, 2018, 2020]
    K.logLE K.Diagnostic $ "ACS (by state) rows per year:"
    K.logLE K.Diagnostic $ show $ fmap (\y -> lengthInYear y acsByState) [2012, 2014, 2016, 2018, 2020]

    K.logLE K.Info "Doing (Ghitza/Gelman) logistic Achen/Hur adjustment to correct CPS for state-specific under-reporting."
    let ew r = FT.recordSingleton @ET.ElectoralWeight (F.rgetField @BRCF.WeightedSuccesses r / F.rgetField @BRCF.WeightedCount r)
        cpsWithProb = fmap (FT.mutate ew) cpsV
        (cpsWithProbAndCit, missing) = FJ.leftJoinWithMissing @(StateKeyR V.++ CensusPredictorR) cpsWithProb $ acsByState
    when (not $ null missing) $ K.knitError $ "prepCCESAndPums: Missing keys in cpsV/acs join: " <> show missing
    when (fLength cpsWithProb /= fLength cpsWithProbAndCit) $ K.knitError "prepCCESAndPums: rows added/deleted by left-join(cps,acs)"
    adjCPSProb <- FL.foldM (BRTA.adjTurnoutFold @PUMS.Citizens @ET.ElectoralWeight stateTurnout) cpsWithProbAndCit
    let adjVoters = adjUsing @ET.ElectoralWeight @BRCF.WeightedSuccesses @BRCF.WeightedCount id id --F.rputField @BRCF.WeightedSuccesses (F.rgetField @BRCF.WeightedCount r * F.rgetField @ET.ElectoralWeight r) r
        res = fmap (F.rcast @CPSVByStateR . adjVoters) adjCPSProb
    K.logLE K.Diagnostic $ "Post Achen-Hur: CPS (by state) rows per year:"
    K.logLE K.Diagnostic $ show $ fmap (\y -> lengthInYear y res) [2012, 2014, 2016, 2018, 2020]
    return res
  K.ignoreCacheTime cpsV_AchenHur_C >>= cpsDiagnostics "CPS: Post Achen/Hur"
  K.logLE K.Info "Pre Achen-Hur CCES Diagnostics (post-stratification of raw turnout * raw pref using ACS weights.)"
  ccesDiagnosticStatesPre <- fmap fst . K.ignoreCacheTimeM $ ccesDiagnostics clearCache "Pre" pumsByCD_C countedCCES_C
  BR.logFrame ccesDiagnosticStatesPre

  let ccesAchenHurDeps = (,,) <$> countedCCES_C <*> pumsByCD_C <*> stateTurnout_C
      ccesAchenHurCacheKey = "model/house/CCES_AchenHur.bin"
  when clearCache $ BR.clearIfPresentD ccesAchenHurCacheKey
  ccesAchenHur_C <- BR.retrieveOrMakeFrame ccesAchenHurCacheKey ccesAchenHurDeps $ \(cces, acsByCD, stateTurnout) -> do
    K.logLE K.Diagnostic $ "Pre Achen-Hur: CCES (by CD) rows per year:"
    K.logLE K.Diagnostic $ show $ fmap (\y -> lengthInYear y cces) [2012, 2014, 2016, 2018, 2020]
    K.logLE K.Diagnostic $ "ACS (by CD) rows per year:"
    K.logLE K.Diagnostic $ show $ fmap (\y -> lengthInYear y acsByCD) [2012, 2014, 2016, 2018, 2020]

    K.logLE K.Info "Doing (Ghitza/Gelman) logistic Achen/Hur adjustment to correct CCES for state-specific under-reporting."
    K.logLE K.Info "Adding missing zeroes to ACS data with CCES predictor cols"
    let ew r = FT.recordSingleton @ET.ElectoralWeight (realToFrac (F.rgetField @Voted r) / realToFrac (F.rgetField @Surveyed r))
        zeroCount :: F.Record '[PUMS.Citizens] = 0 F.&: V.RNil
        addZeroF = FMR.concatFold
                 $ FMR.mapReduceFold
                 FMR.noUnpack
                 (FMR.assignKeysAndData @CDKeyR @(CCESPredictorR V.++ '[PUMS.Citizens]))
                 (FMR.makeRecsWithKey id
                  $ FMR.ReduceFold
                  $ const
                  $ BRK.addDefaultRec @CCESPredictorR zeroCount
                 )
        fixedACS = FL.fold addZeroF $ fmap (fixDC_CD . addRace5) acsByCD
        ccesWithProb = fmap (FT.mutate ew) cces
        (ccesWithProbAndCit, missing) = FJ.leftJoinWithMissing @(CDKeyR V.++ CCESPredictorR) ccesWithProb fixedACS
    when (not $ null missing) $ K.knitError $ "Missing keys in cces/acs join: " <> show missing
    when (fLength ccesWithProb /= fLength ccesWithProbAndCit) $ K.knitError "prepCCESAndPums: rows added/deleted by left-join(cces,acs)"
    adjCCESProb <- FL.foldM (BRTA.adjTurnoutFold @PUMS.Citizens @ET.ElectoralWeight stateTurnout) ccesWithProbAndCit

    -- NB: only turnout cols adjusted. HouseVotes and HouseDVotes, PresVotes and PresDVotes not adjusted
    let adjVoters = adjUsing @ET.ElectoralWeight @Voted @Surveyed realToFrac round
        res = fmap (F.rcast @CCESByCDR . adjVoters) adjCCESProb
    K.logLE K.Diagnostic $ "Post Achen-Hur: CCES (by CD) rows per year:"
    K.logLE K.Diagnostic $ show $ fmap (\y -> lengthInYear y res) [2012, 2014, 2016, 2018, 2020]
    return res

  K.logLE K.Info "CCES Diagnostics (post-stratification of raw turnout * raw pref using ACS weights.)"
  ccesDiagnosticStatesPost <- fmap fst . K.ignoreCacheTimeM $ ccesDiagnostics clearCache "Post "pumsByCD_C ccesAchenHur_C
  BR.logFrame ccesDiagnosticStatesPost
  let deps = (,,) <$> ccesAchenHur_C <*> cpsV_AchenHur_C <*> pumsByCD_C
      cacheKey = "model/house/CCESAndPUMS.bin"
  when clearCache $ BR.clearIfPresentD cacheKey
  BR.retrieveOrMakeD cacheKey deps $ \(ccesByCD, cpsVByState, acsByCD) -> do
    -- get Density and avg income from PUMS and combine with election data for the district level data
    let acsCDFixed = fmap fixDC_CD acsByCD
        diInnerFold :: FL.Fold (F.Record [DT.PopPerSqMile, DT.AvgIncome, PUMS.Citizens, PUMS.NonCitizens]) (F.Record [PUMS.Citizens, DT.PopPerSqMile, DT.AvgIncome])
        diInnerFold =
          let cit = F.rgetField @PUMS.Citizens
              ppl r = cit r + F.rgetField @PUMS.NonCitizens r
              citF = FL.premap cit FL.sum
              citWeightedSumF f = (/) <$> FL.premap (\r -> realToFrac (cit r) * f r) FL.sum <*> fmap realToFrac citF
              pplF = FL.premap ppl FL.sum
              pplWeightedSumF f = (/) <$> FL.premap (\r -> realToFrac (ppl r) * f r) FL.sum <*> fmap realToFrac pplF
          in (\c d i -> c F.&: d F.&: i F.&: V.RNil) <$> citF <*> citWeightedSumF (F.rgetField @DT.PopPerSqMile) <*> citWeightedSumF (F.rgetField @DT.AvgIncome)
        diByCDFold :: FL.Fold (F.Record PUMSByCDR) (F.FrameRec DistrictDemDataR)
        diByCDFold = FMR.concatFold
                     $ FMR.mapReduceFold
                     FMR.noUnpack
                     (FMR.assignKeysAndData @CDKeyR)
                     (FMR.foldAndAddKey diInnerFold)
        diByStateFold :: FL.Fold (F.Record PUMSByCDR) (F.FrameRec StateDemDataR)
        diByStateFold = FMR.concatFold
                        $ FMR.mapReduceFold
                        FMR.noUnpack
                        (FMR.assignKeysAndData @StateKeyR)
                        (FMR.foldAndAddKey diInnerFold)
        diByCD = FL.fold diByCDFold acsCDFixed
        diByState = FL.fold diByStateFold acsCDFixed
    ccesWD <- K.knitEither $ addPopDensByDistrictToCCES diByCD ccesByCD
    cpsVWD <- K.knitEither $ addPopDensByStateToCPSV diByState cpsVByState
    acsWD <- K.knitEither $ addPopDensByDistrictToPUMS diByCD acsCDFixed
    return $ CCESAndPUMS ccesWD cpsVWD acsWD diByCD -- (F.toFrame $ fmap F.rcast $ cats)


type CCESWithDensity = CCESByCDR V.++ '[DT.PopPerSqMile]
addPopDensByDistrictToCCES :: F.FrameRec DistrictDemDataR -> F.FrameRec CCESByCDR -> Either Text (F.FrameRec CCESWithDensity)
addPopDensByDistrictToCCES ddd cces = do
  let ddd' = F.rcast @[BR.Year, BR.StateAbbreviation, BR.CongressionalDistrict, DT.PopPerSqMile] <$> ddd
      (joined, missing) = FJ.leftJoinWithMissing @[BR.Year, BR.StateAbbreviation, BR.CongressionalDistrict] cces ddd'
  when (not $ null missing) $ Left $ "missing keys in join of density data to cces: " <> show missing
  Right joined

type PUMSWithDensity = PUMSByCDR V.++ '[DT.PopPerSqMile]
addPopDensByDistrictToPUMS :: F.FrameRec DistrictDemDataR -> F.FrameRec PUMSByCDR -> Either Text (F.FrameRec PUMSWithDensity)
addPopDensByDistrictToPUMS ddd cces = do
  let ddd' = F.rcast @[BR.Year, BR.StateAbbreviation, BR.CongressionalDistrict, DT.PopPerSqMile] <$> ddd
      (joined, missing) = FJ.leftJoinWithMissing @[BR.Year, BR.StateAbbreviation, BR.CongressionalDistrict] cces ddd'
  when (not $ null missing) $ Left $ "missing keys in join of density data to pums: " <> show missing
  Right joined

type CPSVWithDensity = CPSVByStateR V.++ '[DT.PopPerSqMile]
addPopDensByStateToCPSV :: F.FrameRec StateDemDataR -> F.FrameRec CPSVByStateR -> Either Text (F.FrameRec CPSVWithDensity)
addPopDensByStateToCPSV sdd cpsV = do
  let sdd' = F.rcast @[BR.Year, BR.StateAbbreviation, DT.PopPerSqMile] <$> sdd
      (joined, missing) = FJ.leftJoinWithMissing @[BR.Year, BR.StateAbbreviation] cpsV sdd'
  when (not $ null missing) $ Left $ "missing keys in join of density data to cpsV: " <> show missing
  Right joined

race5FromRace4AAndHisp :: (F.ElemOf rs DT.RaceAlone4C, F.ElemOf rs DT.HispC) => F.Record rs -> DT.Race5
race5FromRace4AAndHisp r =
  let race4A = F.rgetField @DT.RaceAlone4C r
      hisp = F.rgetField @DT.HispC r
  in DT.race5FromRaceAlone4AndHisp True race4A hisp

addRace5 :: (F.ElemOf rs DT.RaceAlone4C, F.ElemOf rs DT.HispC)
         => F.Record rs -> F.Record (rs V.++ '[DT.Race5C])
addRace5 r = r V.<+> FT.recordSingleton @DT.Race5C (race5FromRace4AAndHisp r)


cpsDiagnostics :: K.KnitEffects r => Text -> F.FrameRec CPSVByStateR -> K.Sem r ()
cpsDiagnostics t cpsByState = K.wrapPrefix "ccesDiagnostics" $ do
  let cpsCountsByYearFld = FMR.concatFold
                           $ FMR.mapReduceFold
                           FMR.noUnpack
                           (FMR.assignKeysAndData @'[BR.Year] @'[BRCF.Count, BRCF.WeightedCount])
                           (FMR.foldAndAddKey $ FF.foldAllConstrained @Num FL.sum)
  K.logLE K.Diagnostic t
  K.logLE K.Diagnostic "cps counts by year"
  BR.logFrame $ FL.fold cpsCountsByYearFld cpsByState
  let isYear y r = F.rgetField @BR.Year r == y
      turnoutByRaceFld year mState =
        let stateFilter r = All $ maybe True (\s -> F.rgetField @BR.StateAbbreviation r == s) mState
            fltr r = getAll $ All (F.rgetField @BR.Year r == year) <> stateFilter r
        in FMR.concatFold
           $ FMR.mapReduceFold
           (FMR.unpackFilterRow fltr)
           (FMR.assign
             (FT.recordSingleton @DT.Race4C . DT.race4FromRace5 . race5FromRace4AAndHisp)
             (F.rcast @[BRCF.WeightedCount, BRCF.WeightedSuccesses]))
           (FMR.foldAndAddKey $ FF.foldAllConstrained @Num FL.sum)
  let (allCounts, nvCounts) = FL.fold ((,) <$> turnoutByRaceFld 2020 Nothing <*> turnoutByRaceFld 2020 (Just "NV")) cpsByState
  K.logLE K.Diagnostic $ "All by race: "
  BR.logFrame allCounts
  K.logLE K.Diagnostic $ "NV by race: "
  BR.logFrame nvCounts


-- CCES diagnostics
type Voters = "Voters" F.:-> Double
type DemVoters = "DemVoters" F.:-> Double
--type CVAP = "CVAP" F.:-> Double
type Turnout = "Turnout" F.:-> Double

type CCESBucketR = [DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.Race5C, DT.HispC]

ccesDiagnostics :: (K.KnitEffects r, BR.CacheEffects r)
                => Bool
                -> Text
                -> K.ActionWithCacheTime r (F.FrameRec PUMSByCDR)
                -> K.ActionWithCacheTime r (F.FrameRec CCESByCDR)
                -> K.Sem r (K.ActionWithCacheTime r ((F.FrameRec [BR.Year,BR.StateAbbreviation, CVAP, Voters, DemVoters, Turnout, ET.DemShare]
                                                      , F.FrameRec [BR.Year,BR.StateAbbreviation, ET.CongressionalDistrict, CVAP, Voters, DemVoters, Turnout, ET.DemShare])))
ccesDiagnostics clearCaches cacheSuffix acs_C cces_C = K.wrapPrefix "ccesDiagnostics" $ do
  K.logLE K.Info $ "computing CES diagnostics..."
  let surveyed r =  F.rgetField @Surveyed r
      pT r = (realToFrac $ F.rgetField @Voted r)/(realToFrac $ surveyed r)
      pD r = let hv = F.rgetField @HouseVotes r in if hv == 0 then 0 else (realToFrac $ F.rgetField @HouseDVotes r)/(realToFrac hv)
      cvap = realToFrac . F.rgetField @PUMS.Citizens
      addRace5 r = r F.<+> (FT.recordSingleton @DT.Race5C $ DT.race5FromRaceAlone4AndHisp True (F.rgetField @DT.RaceAlone4C r) (F.rgetField @DT.HispC r))
      compute rw rc = let voters = (pT rw * cvap rc) in (F.rgetField @PUMS.Citizens rc F.&: voters F.&: (pD rw * voters) F.&: V.RNil ) :: F.Record [CVAP, Voters, DemVoters]
      psFld :: FL.Fold (F.Record [CVAP, Voters, DemVoters]) (F.Record [CVAP, Voters, DemVoters])
      psFld = FF.foldAllConstrained @Num FL.sum
      addShare r = let v = F.rgetField @Voters r in r F.<+> (FT.recordSingleton @ET.DemShare $ if v < 1 then 0 else F.rgetField @DemVoters r / v)
      addTurnout r = let v = realToFrac (F.rgetField @CVAP r) in r F.<+> (FT.recordSingleton @Turnout $ if v < 1 then 0 else F.rgetField @Voters r / v)
      deps = (,) <$> acs_C <*> cces_C
      onlyAlaska_C = F.filterFrame (\r -> F.rgetField @BR.StateAbbreviation r == "AK") <$> cces_C
  K.ignoreCacheTime onlyAlaska_C >>= BR.logFrame
  let statesCK = "diagnostics/ccesPSByPumsStates" <> cacheSuffix <> ".bin"
  when clearCaches $ BR.clearIfPresentD statesCK
  states_C <- BR.retrieveOrMakeFrame statesCK deps $ \(acs, cces) -> do
    let acsFixed =  addRace5 <$> (F.filterFrame (\r -> F.rgetField @BR.Year r >= 2016) acs)
        (psByState, missing) = BRPS.joinAndPostStratify @'[BR.Year,BR.StateAbbreviation] @CCESBucketR @CCESVotingDataR @'[PUMS.Citizens]
                               compute
                               psFld
                               (F.rcast <$> cces)
                               (F.rcast <$> acsFixed)
--    when (not $ null missing) $ K.knitError $ "ccesDiagnostics: Missing keys in cces/pums join: " <> show missing
    return $ (addShare . addTurnout) <$> psByState
  let districtsCK = "diagnostics/ccesPSByPumsCDs" <> cacheSuffix <> ".bin"
  when clearCaches $ BR.clearIfPresentD districtsCK
  districts_C <- BR.retrieveOrMakeFrame districtsCK deps $ \(acs, cces) -> do
    let acsFixed =  addRace5 <$> (F.filterFrame (\r -> F.rgetField @BR.Year r >= 2016) acs)
        (psByCD, missing) = BRPS.joinAndPostStratify @'[BR.Year,BR.StateAbbreviation,ET.CongressionalDistrict] @CCESBucketR @CCESVotingDataR @'[PUMS.Citizens]
                               compute
                               psFld
                               (F.rcast <$> cces)
                               (F.rcast <$> acsFixed)
--    when (not $ null missing) $ K.knitError $ "ccesDiagnostics: Missing keys in cces/pums join: " <> show missing
    return $ addShare. addTurnout <$> psByCD
--  K.ignoreCacheTime states_C >>= BR.logFrame
  return $ (,) <$> states_C <*> districts_C

data DMModel = BaseDM
             | WithCPSTurnoutDM
             deriving (Show, Eq, Ord, Generic)


groupBuilderDM :: forall rs ks.
                  (F.ElemOf rs BR.StateAbbreviation
                  , Typeable rs
                  , Typeable ks
                  , V.RMap ks
                  , V.ReifyConstraint Show F.ElField ks
                  , V.RecordToList ks
                  , ks F.⊆ rs
                  , Ord (F.Record ks)
                  )
               => SB.GroupTypeTag (F.Record ks)
               -> [Text]
               -> [F.Record ks]
               -> SB.StanGroupBuilderM CCESAndPUMS (F.FrameRec rs) ()
groupBuilderDM psGroup states psKeys = do
  ccesData <- SB.addModelDataToGroupBuilder "CCES" (SB.ToFoldable ccesRows)
  SB.addGroupIndexForData stateGroup ccesData $ SB.makeIndexFromFoldable show (F.rgetField @BR.StateAbbreviation) states
  cpsData <- SB.addModelDataToGroupBuilder "CPS" (SB.ToFoldable cpsVRows)
  SB.addGroupIndexForData stateGroup cpsData $ SB.makeIndexFromFoldable show (F.rgetField @BR.StateAbbreviation) states
  psData <- SB.addGQDataToGroupBuilder "DistrictPS" (SB.ToFoldable id)
  SB.addGroupIndexForData stateGroup psData $ SB.makeIndexFromFoldable show (F.rgetField @BR.StateAbbreviation) states
  SB.addGroupIndexForData psGroup psData $ SB.makeIndexFromFoldable show F.rcast psKeys

designMatrixRow :: forall rs.(F.ElemOf rs DT.CollegeGradC
                             , F.ElemOf rs DT.SexC
                             , F.ElemOf rs DT.Race5C
                             , F.ElemOf rs DT.HispC
                             , F.ElemOf rs DT.PopPerSqMile
                             )
                => DM.DesignMatrixRow (F.Record rs)
designMatrixRow = DM.DesignMatrixRow "EMDM" $ [sexRP, eduRP, raceRP, densRP, wngRP]
  where
    sexRP = DM.boundedEnumRowPart "Sex" (F.rgetField @DT.SexC)
    eduRP = DM.boundedEnumRowPart "Education" (F.rgetField @DT.CollegeGradC)
    raceRP = DM.boundedEnumRowPart "Race" mergeRace5AndHispanic
    densRP = DM.DesignMatrixRowPart "Density" 1 logDensityPredictor
    wngRP = DM.boundedEnumRowPart "WhiteNonGrad" wnhNonGradCCES

designMatrixRowCPS :: DM.DesignMatrixRow (F.Record CPSVWithDensity)
designMatrixRowCPS = DM.DesignMatrixRow "EMDM" $ [sexRP, eduRP, raceRP, densRP, wngRP]
 where
    sexRP = DM.boundedEnumRowPart "Sex" (F.rgetField @DT.SexC)
    eduRP = DM.boundedEnumRowPart "Education" (F.rgetField @DT.CollegeGradC)
    raceRP = DM.boundedEnumRowPart "Race" race5Census
    densRP = DM.DesignMatrixRowPart "Density" 1 logDensityPredictor
    wngRP = DM.boundedEnumRowPart "WhiteNonGrad" wnhNonGradCensus


race5Census r = DT.race5FromRaceAlone4AndHisp True (F.rgetField @DT.RaceAlone4C r) (F.rgetField @DT.HispC r)
wnhCensus r = F.rgetField @DT.RaceAlone4C r == DT.RA4_White && F.rgetField @DT.HispC r == DT.NonHispanic
wnhNonGradCensus r = wnhCensus r && F.rgetField @DT.CollegeGradC r == DT.NonGrad

data TurnoutDataSet r where
   JustCCES :: TurnoutDataSet (F.Record CCESWithDensity)
   JustCPS :: TurnoutDataSet (F.Record CPSVWithDensity)
   CCESAndCPS :: TurnoutDataSet ()

printTurnoutDataSet :: TurnoutDataSet tr -> Text
printTurnoutDataSet JustCCES = "CCES"
printTurnoutDataSet JustCPS = "CPS"
printTurnoutDataSet CCESAndCPS = "CCESAndCPS"

electionModelDM :: forall rs ks r tr.
                   (K.KnitEffects r
                   , BR.CacheEffects r
                   , V.RecordToList ks
                   , V.ReifyConstraint Show F.ElField ks
                   , V.RecordToList ks
                   , V.RMap ks
                   , F.ElemOf rs BR.StateAbbreviation
                   , F.ElemOf rs DT.CollegeGradC
                   , F.ElemOf rs DT.SexC
                   , F.ElemOf rs DT.Race5C
                   , F.ElemOf rs DT.HispC
                   , F.ElemOf rs DT.PopPerSqMile
                   , F.ElemOf rs Census.Count
                   ,  (ks V.++ '[MT.ModelId Model, ModeledShare]) F.⊆  (BR.Year : MT.ModelId Model : (ks V.++ '[ModeledShare]))
                   , FI.RecVec (ks V.++ '[ModeledShare])
                   , FI.RecVec  (ks V.++ '[MT.ModelId Model, ModeledShare])
                   , ks F.⊆ rs
                   , V.RMap (ks V.++ '[MT.ModelId Model, ModeledShare])
                   , Show (F.Record ks)
                   , Typeable rs
                   , Typeable ks
                   , Ord (F.Record ks)
                   , Flat.GFlatDecode
                     (Flat.Rep
                      (F.Rec FS.SElField (ks V.++ '[MT.ModelId Model, ModeledShare])))
                   , Flat.GFlatEncode
                     (Flat.Rep
                      (F.Rec FS.SElField (ks V.++ '[MT.ModelId Model, ModeledShare])))
                   , Flat.GFlatSize
                     (Flat.Rep
                       (F.Rec FS.SElField (ks V.++ '[MT.ModelId Model, ModeledShare])))
                   , Generic
                     (F.Rec FS.SElField (ks V.++ '[MT.ModelId Model, ModeledShare]))
                 )
                => Bool
                -> Bool
                -> BR.StanParallel
                -> Text
                -> TurnoutDataSet tr
                -> Model
                -> Int
                -> (SB.GroupTypeTag (F.Record ks), Text, SB.GroupSet)
                -> K.ActionWithCacheTime r CCESAndPUMS
                -> K.ActionWithCacheTime r (F.FrameRec rs)
                -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec (ModelResultsR ks)))
electionModelDM clearCaches parallel stanParallelCfg modelDir turnoutDataSet model datYear (psGroup, psDataSetName, psGroupSet) dat_C psDat_C = K.wrapPrefix "stateLegModel" $ do
  K.logLE K.Info $ "(Re-)running DM turnout/pref model if necessary."
  when (groupModel model /= DMG || densityModel model /= DMD) $ K.knitError $ "electionModelDM called with wrong model type: " <> show model
  let modelName = "LegDistricts_" <> show turnoutDataSet <> "_" <> modelLabel model <> if parallel then "_P" else ""
      jsonDataName = "DM_" <> show datYear <> if parallel then "_P" else ""  -- because of grainsize
      dataAndCodeBuilder :: MRP.BuilderM CCESAndPUMS (F.FrameRec rs) ()
      dataAndCodeBuilder = do
        -- data
        ccesData <- SB.dataSetTag @(F.Record CCESWithDensity) SC.ModelData "CCES"
        dmCCES <- DM.addDesignMatrix ccesData designMatrixRow
        DM.addDesignMatrixIndexes ccesData designMatrixRow -- just need it for one
        let makeIndexArray rtt n =
              let dsName = SB.dataSetName rtt
                  sizeName = "N_" <> dsName
              in SB.inBlock SB.SBTransformedData
                 $ SB.stanDeclareRHS ("dsIndex_" <> dsName) (SB.StanArray [SB.NamedDim dsName] SB.StanInt) "<lower=0>"
                 $ SB.function "rep_array" (SB.scalar (show n) :| [SB.name sizeName])
            setupCCESData :: Int -> SB.StanBuilderM md gq (SB.RowTypeTag (F.Record CCESWithDensity), SB.StanVar, SB.StanVar, SB.StanVar, SB.StanVar)
            setupCCESData n = do
              cvapCCES <- SB.addCountData ccesData "CVAP_CCES" (F.rgetField @Surveyed)
              votedCCES <- SB.addCountData ccesData "VOTED_CCES" (F.rgetField @Voted)
              indexCCES <- makeIndexArray ccesData n
              return (ccesData, cvapCCES, votedCCES, dmCCES, indexCCES)
            setupCPSData :: Int -> SB.StanBuilderM md gq (SB.RowTypeTag (F.Record CPSVWithDensity), SB.StanVar, SB.StanVar, SB.StanVar, SB.StanVar)
            setupCPSData n = do
              cpsData <- SB.dataSetTag @(F.Record CPSVWithDensity) SC.ModelData "CPS"
              cvapCPS <- SB.addCountData cpsData "CPS_CVAP" (F.rgetField @BRCF.Count)
              votedCPS <- SB.addCountData cpsData "CPS_VOTED" (F.rgetField @BRCF.Successes)
              dmCPS <- DM.addDesignMatrix cpsData designMatrixRowCPS
              indexCPS <- makeIndexArray cpsData n
              return (cpsData, cvapCPS, votedCPS, dmCPS, indexCPS)
            groupSet =  SB.addGroupToSet stateGroup SB.emptyGroupSet
            setupTurnoutData :: forall x md gq.TurnoutDataSet x -> SB.StanBuilderM md gq (SB.RowTypeTag tr, SB.StanVar, SB.StanVar, SB.StanVar, SB.StanVar)
            setupTurnoutData td = case turnoutDataSet of
              JustCCES -> setupCCESData 0
              JustCPS -> setupCPSData 0
              CCESAndCPS -> do
                (ccesData, ccesCVAP, ccesVoted, ccesDM, ccesIndex) <- setupCCESData 0
                (cpsData, cpsCVAP, cpsVoted, cpsDM, cpsIndex) <- setupCPSData 1
                (comboData, dsIndexV, stackVarsF) <- SB.stackDataSets "combo" ccesData cpsData groupSet
                (cvap, voted, dmCombo, indexCombo) <-  SB.inBlock SB.SBTransformedData $ do
                  cvapCombo' <- stackVarsF "CVAP" ccesCVAP cpsCVAP
                  votedCombo' <- stackVarsF "Voted" ccesVoted cpsVoted
                  dmCombo' <- stackVarsF "DM" ccesDM cpsDM
                  indexCombo' <- stackVarsF "DataSetIndex" ccesIndex cpsIndex
                  return (cvapCombo', votedCombo', dmCombo', indexCombo')
                return (comboData, cvap, voted, dmCombo, indexCombo)
        (turnoutData, cvap, voted, dmTurnout, dsIndex) <- setupTurnoutData
        let distT = SB.binomialLogitDist cvap
            (votesF, dVotesF) = getVotes $ voteSource model
        hVotes <- SB.addCountData ccesData "VOTES_C" votesF --(F.rgetField @HouseVotes)
        dVotes <- SB.addCountData ccesData "DVOTES_C" dVotesF --(F.rgetField @HouseDVotes)
        let dmPref = dmCCES
            distP = SB.binomialLogitDist hVotes

        (dmT, centerTF) <- DM.centerDataMatrix dmTurnout Nothing
        (dmP, centerPF) <- DM.centerDataMatrix dmPref Nothing
        (alphaT, thetaT, muT, tauT, lT) <-
          DM.addDMParametersAndPriors ccesData (designMatrixRow @CCESWithDensity) stateGroup "beta" DM.NonCentered (SB.stdNormal, SB.stdNormal, 4) (Just "T")
        (alphaP, thetaP, muP, tauP, lP) <-
          DM.addDMParametersAndPriors ccesData (designMatrixRow @CCESWithDensity) stateGroup "beta" DM.NonCentered (SB.stdNormal, SB.stdNormal, 4) (Just "P")
        dsPhi <- do
          case turnoutDataSet of
            JustCCES -> SB.StanVar "phi_unusued" SB.StanReal ""
            JustCPS -> SB.StanVar "phi_unusued" SB.StanReal ""
            CCESAndCPS -> do
              dsPhi' <- SB.inBlock SB.SBParameters $ SB.stanDeclare "phi" SB.StanReal ""
              SB.inBlock SB.SBModel $ SB.addExprLine "electionModelDM" $ SB.var dsPhi' `SB.vectorSample` SB.stdNormal
              return dsPhi'

        let dmBetaE dm beta = SB.vectorizedOne "EMDM_Cols" $ SB.function "dot_product" (SB.var dm :| [SB.var beta])
            predE a dm beta = SB.var a `SB.plus` dmBetaE dm beta
            iPredE a dm beta = case turnoutDataSet of
              JustCCES -> predE a dm beta
              JustCPS -> predE a dm beta
              CCESAndCPS -> predE a dm beta `SB.plus` (SB.var dsPhi `SB.times` SB.var dsIndex)
            vecT = SB.vectorizeExpr "voteDataBetaT" (iPredE alphaT dmT thetaT) (SB.dataSetName turnoutData)
            vecP = SB.vectorizeExpr "voteDataBetaP" (predE alphaP dmP thetaP) (SB.dataSetName ccesData)
        SB.inBlock SB.SBModel $ do
          SB.useDataSetForBindings turnoutData $ do
            voteDataBetaT_v <- vecT
            SB.sampleDistV turnoutData distT (SB.var voteDataBetaT_v) voted
          SB.useDataSetForBindings ccesData $ do
            voteDataBetaP_v <- vecP
            SB.sampleDistV ccesData distP (SB.var voteDataBetaP_v) dVotes

        -- split parameters back to input categories
        SB.inBlock SB.SBGeneratedQuantities $ do
            SB.useDataSetForBindings ccesData $ DM.splitToGroupVars (designMatrixRow @CCESWithDensity) muT
            SB.useDataSetForBindings ccesData $ DM.splitToGroupVars (designMatrixRow @CCESWithDensity) muP
        let llSet = SB.addToLLSet turnoutData (SB.LLDetails distT (pure $ iPredE alphaT dmT thetaT) voted)
                    $ SB.addToLLSet ccesData (SB.LLDetails distP (pure $ predE alphaP dmP thetaP) dVotes)
                    $ SB.emptyLLSet
        SB.generateLogLikelihood' llSet

        let ppVar = SB.StanVar "PVotes" (SB.StanVector $ SB.NamedDim $ SB.dataSetName turnoutData)
        SB.useDataSetForBindings turnoutData
          $ SB.generatePosteriorPrediction turnoutData ppVar distT $ iPredE alphaT dmT thetaT
        psData <- SB.dataSetTag @(F.Record rs) SC.GQData "DistrictPS"
        dmPS' <- DM.addDesignMatrix psData designMatrixRow

        let psPreCompute = do
              dmPS_T <- centerTF dmPS' (Just "T")
              dmPS_P <- centerPF dmPS' (Just "P")
              psT_v <- SB.vectorizeExpr "psBetaT" (predE alphaT dmPS_T thetaT) (SB.dataSetName psData)
              psP_v <- SB.vectorizeExpr "psBetaP" (predE alphaP dmPS_P thetaP) (SB.dataSetName psData)
              pure (psT_v, psP_v)

            psExprF (psT_v, psP_v) = do
              pT <- SB.stanDeclareRHS "pT" SB.StanReal "" $ SB.familyExp distT (SB.var psT_v)
              pD <- SB.stanDeclareRHS "pD" SB.StanReal "" $ SB.familyExp distP (SB.var psP_v)
              pure $ SB.var pT `SB.times` SB.var pD

        let postStrat =
              MRP.addPostStratification -- @(CCESAndPUMS, F.FrameRec rs)
              (psPreCompute, psExprF)
              Nothing
              ccesData
              psData
              psGroupSet
              (realToFrac . F.rgetField @Census.Count)
              (MRP.PSShare $ Just $ SB.name "pT")
              (Just psGroup)
        postStrat
        pure ()

      addModelIdAndYear :: F.Record (ks V.++ '[ModeledShare])
                        -> F.Record (ModelResultsR ks)
      addModelIdAndYear r = F.rcast $ FT.recordSingleton @BR.Year datYear F.<+> FT.recordSingleton @(MT.ModelId Model) model F.<+> r
      extractResults :: K.KnitEffects r
                     => SC.ResultAction r md gq SB.DataSetGroupIntMaps () (FS.SFrameRec (ModelResultsR ks))
      extractResults = SC.UseSummary f where
        f summary _ modelDataAndIndex_C mGQDataAndIndex_C = do
          gqIndexes_C <- K.knitMaybe "StanMRP.extractResults: gqDataAndIndex is Nothing" $ mGQDataAndIndex_C
          gqIndexesE <- K.ignoreCacheTime $ fmap snd gqIndexes_C
          resultsMap <- K.knitEither $ do
            gqIndexes <- gqIndexesE
            psIndexIM <- SB.getGroupIndex
              (SB.RowTypeTag @(F.Record rs) SC.GQData "DistrictPS")
              psGroup
              gqIndexes
            let parseAndIndexPctsWith idx g vn = do
                  v <- SP.getVector . fmap CS.percents <$> SP.parse1D vn (CS.paramStats summary)
                  indexStanResults idx $ Vector.map g v
            parseAndIndexPctsWith psIndexIM id $ "PS_DistrictPS_" <> SB.taggedGroupName psGroup
          res :: F.FrameRec (ks V.++ '[ModeledShare]) <- K.knitEither
                                                         $ MT.keyedCIsToFrame @ModeledShare id
                                                         $ M.toList resultsMap
          return $ FS.SFrame $ fmap addModelIdAndYear res
      dataWranglerAndCode :: K.ActionWithCacheTime r CCESAndPUMS
                          -> K.ActionWithCacheTime r (F.FrameRec rs)
                          -> K.Sem r (SC.DataWrangler CCESAndPUMS (F.FrameRec rs) SB.DataSetGroupIntMaps (), SB.StanCode)
      dataWranglerAndCode modelData_C gqData_C = do
        modelData <-  K.ignoreCacheTime modelData_C
        gqData <-  K.ignoreCacheTime gqData_C
        K.logLE K.Info
          $ "Voter data (CCES) has "
          <> show (FL.fold FL.length $ ccesRows modelData)
          <> " rows."
        let states = FL.fold (FL.premap (F.rgetField @BR.StateAbbreviation) FL.list) (ccesRows modelData)
            psKeys = FL.fold (FL.premap F.rcast FL.list) gqData
            groups = groupBuilderDM psGroup states psKeys
        K.logLE K.Info $ show $ zip [1..] $ Set.toList $ FL.fold FL.set states
        MRP.buildDataWranglerAndCode @BR.SerializerC @BR.CacheData groups dataAndCodeBuilder modelData_C gqData_C
  (dw, stanCode) <- dataWranglerAndCode dat_C psDat_C
  fmap (fmap FS.unSFrame)
    $ MRP.runMRPModel
    clearCaches
    (SC.RunnerInputNames modelDir modelName (Just psDataSetName) jsonDataName)
    (SC.StanMCParameters 4 4 (Just 1000) (Just 1000) (Just 0.8) (Just 10) Nothing)
    stanParallelCfg
    dw
    stanCode
    "Votes"
    extractResults
    dat_C
    psDat_C

groupBuilder :: forall rs ks.
                ( F.ElemOf rs BR.StateAbbreviation
                , F.ElemOf rs DT.CollegeGradC
                , F.ElemOf rs DT.SexC
                , F.ElemOf rs DT.Race5C
                , F.ElemOf rs DT.HispC
                , ks F.⊆ rs
                , Show (F.Record ks)
                , Typeable rs
                , Typeable ks
                , Ord (F.Record ks))
             => SB.GroupTypeTag (F.Record ks)
             -> [Text]
             -> [Text]
             -> [F.Record ks]
             -> SB.StanGroupBuilderM CCESAndPUMS (F.FrameRec rs) ()
groupBuilder psGroup districts states psKeys = do
  voterData <- SB.addModelDataToGroupBuilder "VoteData" (SB.ToFoldable ccesRows)
  SB.addGroupIndexForData cdGroup voterData $ SB.makeIndexFromFoldable show districtKey districts
  SB.addGroupIndexForData stateGroup voterData $ SB.makeIndexFromFoldable show (F.rgetField @BR.StateAbbreviation) states
  SB.addGroupIndexForData sexGroup voterData $ SB.makeIndexFromEnum (F.rgetField @DT.SexC)
  SB.addGroupIndexForData educationGroup voterData $ SB.makeIndexFromEnum (F.rgetField @DT.CollegeGradC)
  SB.addGroupIndexForData raceGroup voterData $ SB.makeIndexFromEnum mergeRace5AndHispanic
  SB.addGroupIndexForData hispanicGroup voterData $ SB.makeIndexFromEnum (F.rgetField @DT.HispC)
  cdData <- SB.addModelDataToGroupBuilder "CDData" (SB.ToFoldable districtRows)
  SB.addGroupIndexForModelCrosswalk cdData $ SB.makeIndexFromFoldable show districtKey districts
  psData <- SB.addGQDataToGroupBuilder "DistrictPS" (SB.ToFoldable id)
  SB.addGroupIndexForData stateGroup psData $ SB.makeIndexFromFoldable show (F.rgetField @BR.StateAbbreviation) states
  SB.addGroupIndexForData educationGroup psData $ SB.makeIndexFromEnum (F.rgetField @DT.CollegeGradC)
  SB.addGroupIndexForData sexGroup psData $ SB.makeIndexFromEnum (F.rgetField @DT.SexC)
  SB.addGroupIndexForData raceGroup psData $ SB.makeIndexFromEnum (F.rgetField @DT.Race5C)
  SB.addGroupIndexForData psGroup psData $ SB.makeIndexFromFoldable show F.rcast psKeys
  return ()


data GroupModel = BaseG
                | DMG
                | PlusStateG
                | PlusSexEduG
                | PlusRaceEduG
                | PlusStateRaceG
                | PartiallyPooledStateG
                | PlusInteractionsG
                | PlusStateAndStateRaceG
                | PlusStateAndStateInteractionsG
                deriving (Show, Eq, Ord, Generic)

instance Flat.Flat GroupModel
type instance FI.VectorFor GroupModel = Vector.Vector

data DensityTransform = RawDensity
                      | LogDensity
                      | QuantileDensity Int
                      deriving (Show, Eq, Ord, Generic)
instance Flat.Flat DensityTransform
type instance FI.VectorFor DensityTransform = Vector.Vector

printDensityTransform :: DensityTransform -> Text
printDensityTransform RawDensity = "Raw"
printDensityTransform LogDensity = "Log"
printDensityTransform (QuantileDensity n) = "Quantile_" <> show n

data DensityModel = BaseD
                  | DMD
                  | PlusEduD
                  | PlusRaceD
                  | PlusHRaceD
                  | PlusInteractionsD
                  | PlusNCHRaceD
                  | PlusHStateD
                  | PlusNCHStateD
                  deriving (Show, Eq, Ord, Generic)

instance Flat.Flat DensityModel
type instance FI.VectorFor DensityModel = Vector.Vector

data VoteSource = HouseVS | PresVS | CompositeVS deriving (Show, Eq, Ord, Generic)
instance Flat.Flat VoteSource
type instance FI.VectorFor VoteSource = Vector.Vector

printVoteSource :: VoteSource -> Text
printVoteSource HouseVS = "HouseVotes"
printVoteSource PresVS = "PresVotes"
printVoteSource CompositeVS = "CompositeVotes"

getVotes :: (F.ElemOf rs HouseVotes
            , F.ElemOf rs HouseDVotes
            , F.ElemOf rs PresVotes
            , F.ElemOf rs PresDVotes
            )
         => VoteSource -> (F.Record rs -> Int, F.Record rs -> Int)
getVotes HouseVS = (F.rgetField @HouseVotes, F.rgetField @HouseDVotes)
getVotes PresVS = (F.rgetField @PresVotes, F.rgetField @PresDVotes)
getVotes CompositeVS =
  let hv = F.rgetField @HouseVotes
      hdv = F.rgetField @HouseVotes
      pv = F.rgetField @PresVotes
      pdv = F.rgetField @PresDVotes
      f h p r = round (realToFrac ((hv r * h r) + (pv r * p r))/realToFrac (hv r + pv r))
  in (f hv pv, f hdv pdv)

data Model = Model { voteSource :: VoteSource
                   , groupModel :: GroupModel
                   , densityTransform :: DensityTransform
                   , densityModel :: DensityModel
                   }  deriving (Show, Eq, Ord, Generic)

instance Flat.Flat Model
type instance FI.VectorFor Model = Vector.Vector

modelLabel :: Model -> Text
modelLabel m = printVoteSource (voteSource m)
               <> "_" <> show (groupModel m)
               <> "_" <> printDensityTransform (densityTransform m)
               <> "_" <> show (densityModel m)

densityModelBuilder :: forall md gq. (Typeable md, Typeable gq)
                    => DensityModel
                    -> SFE.FEMatrixes
                    -> SB.StanExpr
                    -> SB.RowTypeTag (F.Record CCESByCDR)
                    -> SB.RowTypeTag (F.Record DistrictDemDataR)
                    -> SB.StanBuilderM md gq (SFE.MakeVecE md gq, SFE.MakeVecE md gq, SFE.MakeVecE md gq, SFE.MakeVecE md gq)
densityModelBuilder densityModelType feMatrices fePrior voteData cdData = do
  (feColDim, _, colIndexKey) <- SFE.colDimAndDimKeys feMatrices
  let baseFEM = (SFE.NonInteractingFE True fePrior)
      modelSpecific ::  DensityModel -> SB.StanBuilderM md gq (SFE.MakeVecE md gq, SFE.MakeVecE md gq, SFE.MakeVecE md gq, SFE.MakeVecE md gq)
      modelSpecific BaseD = do
        (thetaTMultF', betaTMultF') <- SFE.addFixedEffectsParametersAndPriors baseFEM feMatrices cdData voteData (Just "T")
        (thetaPMultF', betaPMultF') <- SFE.addFixedEffectsParametersAndPriors baseFEM feMatrices cdData voteData (Just "P")
        return (thetaTMultF', betaTMultF', thetaPMultF', betaPMultF')
      modelSpecific PlusEduD = do
        let baseFEM = (SFE.NonInteractingFE True fePrior)
        (thetaTMultF', betaTMultF') <- SFE.addFixedEffectsParametersAndPriors baseFEM feMatrices cdData voteData (Just "T")
        (thetaPMultF', betaPMultF') <- SFE.addFixedEffectsParametersAndPriors baseFEM feMatrices cdData voteData (Just "P")
        let eduDensityGM = SB.BinarySymmetric fePrior
            eduDensityFEM = SFE.InteractingFE True educationGroup eduDensityGM
        (thetaEduTMultF, betaEduTMultF) <- SFE.addFixedEffectsParametersAndPriors eduDensityFEM feMatrices cdData voteData (Just "T")
        (thetaEduPMultF, betaEduPMultF) <- SFE.addFixedEffectsParametersAndPriors eduDensityFEM feMatrices cdData voteData (Just "P")
        let tTMultF' ik x = SB.plus <$> thetaTMultF' ik x <*> thetaEduTMultF ik x
            tPMultF' ik x = SB.plus <$> thetaPMultF' ik x <*> thetaEduPMultF ik x
            bTMultF' ik x = SB.plus <$> betaTMultF' ik x <*> betaEduTMultF ik x
            bPMultF' ik x = SB.plus <$> betaPMultF' ik x <*> betaEduPMultF ik x
        return (tTMultF', bTMultF', tPMultF', bPMultF')
      modelSpecific PlusRaceD = do
        let baseFEM = (SFE.NonInteractingFE True fePrior)
        (thetaTMultF', betaTMultF') <- SFE.addFixedEffectsParametersAndPriors baseFEM feMatrices cdData voteData (Just "T")
        (thetaPMultF', betaPMultF') <- SFE.addFixedEffectsParametersAndPriors baseFEM feMatrices cdData voteData (Just "P")
        let raceDensityGM = SB.NonHierarchical SB.STZNone fePrior
            raceDensityFEM = SFE.InteractingFE True raceGroup raceDensityGM
        (thetaRaceTMultF, betaRaceTMultF) <- SFE.addFixedEffectsParametersAndPriors raceDensityFEM feMatrices cdData voteData (Just "T")
        (thetaRacePMultF, betaRacePMultF) <- SFE.addFixedEffectsParametersAndPriors raceDensityFEM feMatrices cdData voteData (Just "P")
        let tTMultF' ik x = SB.plus <$> thetaTMultF' ik x <*> thetaRaceTMultF ik x
            tPMultF' ik x = SB.plus <$> thetaPMultF' ik x <*> thetaRacePMultF ik x
            bTMultF' ik x = SB.plus <$> betaTMultF' ik x <*> betaRaceTMultF ik x
            bPMultF' ik x = SB.plus <$> betaPMultF' ik x <*> betaRacePMultF ik x
        return (tTMultF', bTMultF', tPMultF', bPMultF')
      modelSpecific PlusHRaceD = do
        let muV s = SB.StanVar ("muRaceDensity" <> s) SB.StanReal
            sigmaV s = SB.StanVar ("sigmaRaceDensity" <> s) SB.StanReal
            hyperParameters s = M.fromList
              [
                (muV s, ("", \v -> SB.var v `SB.vectorSample` SB.stdNormal))
              , (sigmaV s, ("<lower=0>", \v -> SB.var v `SB.vectorSample` SB.stdNormal))
              ]
            cPriorF s v = SB.addExprLine "ELectionResult.densityModel"
              $ SB.vectorizedOne (SB.taggedGroupName raceGroup) $ SB.var v `SB.eq` SB.normal (Just $ SB.var $ muV s) (SB.var $ sigmaV s)
            raceDensityGM s = SB.Hierarchical SB.STZNone (hyperParameters s) (SB.Centered $ cPriorF s)
            raceDensityFEM s = SFE.InteractingFE True raceGroup (raceDensityGM s)
        (thetaRaceTMultF, betaRaceTMultF) <- SFE.addFixedEffectsParametersAndPriors (raceDensityFEM "T") feMatrices cdData voteData (Just "T")
        (thetaRacePMultF, betaRacePMultF) <- SFE.addFixedEffectsParametersAndPriors (raceDensityFEM "P") feMatrices cdData voteData (Just "P")
        return (thetaRaceTMultF, betaRaceTMultF, thetaRacePMultF, betaRacePMultF)
      modelSpecific PlusInteractionsD = do
        let muV s = SB.StanVar ("muRaceDensity" <> s) SB.StanReal
            sigmaV s = SB.StanVar ("sigmaRaceDensity" <> s) SB.StanReal
            hyperParameters s = M.fromList
              [
                (muV s, ("", \v -> SB.var v `SB.vectorSample` SB.stdNormal))
              , (sigmaV s, ("<lower=0>", \v -> SB.var v `SB.vectorSample` SB.stdNormal))
              ]
            cPriorF s v = SB.addExprLine "ELectionResult.densityModel"
              $ SB.vectorizedOne (SB.taggedGroupName raceGroup) $ SB.var v `SB.eq` SB.normal (Just $ SB.var $ muV s) (SB.var $ sigmaV s)
            raceDensityGM s = SB.Hierarchical SB.STZNone (hyperParameters s) (SB.Centered $ cPriorF s)
            raceDensityFEM s = SFE.InteractingFE True raceGroup (raceDensityGM s)
        (thetaRaceTMultF, betaRaceTMultF) <- SFE.addFixedEffectsParametersAndPriors (raceDensityFEM "T") feMatrices cdData voteData (Just "T")
        (thetaRacePMultF, betaRacePMultF) <- SFE.addFixedEffectsParametersAndPriors (raceDensityFEM "P") feMatrices cdData voteData (Just "P")
        let eduDensityGM = SB.BinarySymmetric fePrior
            eduDensityFEM = SFE.InteractingFE True educationGroup eduDensityGM
        (thetaEduTMultF, betaEduTMultF) <- SFE.addFixedEffectsParametersAndPriors eduDensityFEM feMatrices cdData voteData (Just "T")
        (thetaEduPMultF, betaEduPMultF) <- SFE.addFixedEffectsParametersAndPriors eduDensityFEM feMatrices cdData voteData (Just "P")
        let sexDensityGM = SB.BinarySymmetric fePrior
            sexDensityFEM = SFE.InteractingFE True sexGroup sexDensityGM
        (thetaSexTMultF, betaSexTMultF) <- SFE.addFixedEffectsParametersAndPriors sexDensityFEM feMatrices cdData voteData (Just "T")
        (thetaSexPMultF, betaSexPMultF) <- SFE.addFixedEffectsParametersAndPriors sexDensityFEM feMatrices cdData voteData (Just "P")
        let add3 x y z = SB.multiOp "+" (x :| [y,z])
            tTMultF' ik x = add3 <$> thetaRaceTMultF ik x <*> thetaEduTMultF ik x <*> thetaSexTMultF ik x
            tPMultF' ik x = add3 <$> thetaRacePMultF ik x <*> thetaEduPMultF ik x <*> thetaSexPMultF ik x
            bTMultF' ik x = add3 <$> betaRaceTMultF ik x <*> betaEduTMultF ik x <*> betaSexTMultF ik x
            bPMultF' ik x = add3 <$> betaRacePMultF ik x <*> betaEduPMultF ik x <*> betaSexPMultF ik x
        return (tTMultF', bTMultF', tPMultF', bPMultF')
      modelSpecific PlusNCHRaceD = do
        let muV s = SB.StanVar ("muRaceDensity" <> s) (SB.StanVector feColDim)
            sigmaV s = SB.StanVar ("sigmaRaceDensity" <> s) (SB.StanVector feColDim)
            hyperParameters s = M.fromList
              [
                (muV s, ("", \v -> SB.vectorizedOne colIndexKey (SB.var v) `SB.vectorSample` SB.stdNormal))
              , (sigmaV s, ("<lower=0>", \v -> SB.vectorizedOne colIndexKey (SB.var v) `SB.vectorSample` SB.stdNormal))
              ]
            rawPriorF v = SB.addExprLine "ElectionResult.densityModel"
              $ SB.vectorizedOne (SB.taggedGroupName raceGroup) $ SB.var v `SB.eq` SB.stdNormal --SB.normal (Just $ SB.name $ mu s) (SB.name $ sigma s)
            centerF s bv@(SB.StanVar sn st) brv = do
              bv' <- SB.stanDeclare sn st ""
              SB.stanForLoopB "k" Nothing colIndexKey
                $ SB.stanForLoopB "g" Nothing "Race"
                $ SB.addExprLine "PlusNCHRaceD"
                $ SB.var bv' `SB.eq` (SB.var (muV s) `SB.plus`  (SB.var (sigmaV s) `SB.times`  SB.var brv))
            raceDensityGM s = SB.Hierarchical SB.STZNone (hyperParameters s) (SB.NonCentered rawPriorF (centerF s))
            raceDensityFEM s = SFE.InteractingFE True raceGroup (raceDensityGM s)
        (thetaRaceTMultF, betaRaceTMultF) <- SFE.addFixedEffectsParametersAndPriors (raceDensityFEM "T") feMatrices cdData voteData (Just "T")
        (thetaRacePMultF, betaRacePMultF) <- SFE.addFixedEffectsParametersAndPriors (raceDensityFEM "P") feMatrices cdData voteData (Just "P")
        return (thetaRaceTMultF, betaRaceTMultF, thetaRacePMultF, betaRacePMultF)
      modelSpecific PlusHStateD = do
        let muV s = SB.StanVar ("muStateDensity" <> s) SB.StanReal
            sigmaV s = SB.StanVar ("sigmaStateDensity" <> s) SB.StanReal
            hyperParameters s = M.fromList
              [
                (muV s, ("", \v -> SB.var v `SB.vectorSample` SB.stdNormal))
              , (sigmaV s, ("<lower=0>", \v -> SB.var v `SB.vectorSample` SB.stdNormal))
              ]
            cPriorF s v = SB.addExprLine "ElectionResult.densityModel"
              $ SB.vectorizedOne (SB.taggedGroupName stateGroup) $ SB.var v `SB.eq` SB.normal (Just $ SB.var $ muV s) (SB.var $ sigmaV s)
--            cPrior s = SB.normal (Just $ SB.name $ mu s) (SB.name $ sigma s)
            stateDensityGM s = SB.Hierarchical SB.STZNone (hyperParameters s) (SB.Centered $ cPriorF s)
            stateDensityFEM s = SFE.InteractingFE True stateGroup (stateDensityGM s)
        (thetaStateTMultF, betaStateTMultF) <- SFE.addFixedEffectsParametersAndPriors (stateDensityFEM "T") feMatrices cdData voteData (Just "T")
        (thetaStatePMultF, betaStatePMultF) <- SFE.addFixedEffectsParametersAndPriors (stateDensityFEM "P") feMatrices cdData voteData (Just "P")
        return (thetaStateTMultF, betaStateTMultF, thetaStatePMultF, betaStatePMultF)
      modelSpecific PlusNCHStateD = do
        let muV s = SB.StanVar ("muStateDensity" <> s) (SB.StanVector $ SB.NamedDim colIndexKey)
            sigmaV s = SB.StanVar ("sigmaStateDensity" <> s) (SB.StanVector $ SB.NamedDim colIndexKey)
            hyperParameters s = M.fromList
              [
                (muV s, ("", \v -> SB.vectorizedOne colIndexKey $ SB.var v `SB.vectorSample` SB.stdNormal))
              , (sigmaV s, ("<lower=0>", \v -> SB.vectorizedOne colIndexKey $ SB.var v `SB.vectorSample` SB.stdNormal))
              ]
            rawPriorF v = SB.stanForLoopB "s" Nothing (SB.taggedGroupName stateGroup)
--                          $ SB.stanForLoopB "k" Nothing colIndexKey
                          $ SB.addExprLine "ElectionResult.densityModel"
                          $ SB.vectorizedOne colIndexKey $ SB.var v `SB.vectorSample` SB.stdNormal
            centerF s bv@(SB.StanVar sn st) brv = do
              bv' <- SB.stanDeclare sn st ""
              SB.stanForLoopB "k" Nothing colIndexKey
                $ SB.stanForLoopB "g" Nothing "State"
                $ SB.addExprLine "PlusNCHStateD"
                $ SB.var bv' `SB.eq` (SB.var (muV s) `SB.plus`  (SB.var (sigmaV s) `SB.times`  SB.var brv))
            stateDensityGM s = SB.Hierarchical SB.STZNone (hyperParameters s) (SB.NonCentered rawPriorF $ centerF s)
            stateDensityFEM s = SFE.InteractingFE True stateGroup (stateDensityGM s)
        (thetaStateTMultF, betaStateTMultF) <- SFE.addFixedEffectsParametersAndPriors (stateDensityFEM "T") feMatrices cdData voteData (Just "T")
        (thetaStatePMultF, betaStatePMultF) <- SFE.addFixedEffectsParametersAndPriors (stateDensityFEM "P") feMatrices cdData voteData (Just "P")
        return (thetaStateTMultF, betaStateTMultF, thetaStatePMultF, betaStatePMultF)
      modelSpecific _ = SB.stanBuildError $ "Unknown density model type given to densityModelBuilder: " <> show densityModelType
  modelSpecific densityModelType

groupModelBuilder :: (Typeable md, Typeable gq)
                  => GroupModel
                  -> SB.StanExpr
                  -> SB.StanExpr
                  -> SB.RowTypeTag (F.Record CCESByCDR)
                  -> SB.StanBuilderM md gq (SB.StanExpr -> SB.StanExpr
                                           , SB.StanExpr -> SB.StanExpr
                                           , SB.StanExpr -> SB.StanExpr
                                           , SB.StanExpr -> SB.StanExpr
                                           )
groupModelBuilder groupModel binaryPrior sigmaPrior voteData = do
   let normal x = SB.normal Nothing $ SB.scalar $ show x
       simpleGroupModel = SB.NonHierarchical SB.STZNone sigmaPrior --(normal x)
       muV gtt s = SB.StanVar ("mu" <> s <> "_" <> SB.taggedGroupName gtt) SB.StanReal
       sigmaV gtt s = SB.StanVar ("sigma" <> s <> "_" <> SB.taggedGroupName gtt) SB.StanReal
       hierHPs :: forall k.SB.GroupTypeTag k -> Text -> Map SB.StanVar (Text, SB.StanVar -> SB.StanExpr)
       hierHPs gtt s = M.fromList
         [
           (muV gtt s, ("", \v -> SB.var v `SB.vectorSample` SB.stdNormal))
         , (sigmaV gtt s, ("<lower=0>", \v -> SB.var v `SB.vectorSample` sigmaPrior))
         ]
       cPriorF gtt s v = SB.addExprLine "ELectionResult.groupModel"
                         $ SB.vectorizedOne (SB.taggedGroupName gtt) $ SB.var v `SB.vectorSample` SB.normal (Just $ SB.var $ muV gtt s) (SB.var $ sigmaV gtt s)
       hierGroupModel gtt s = SB.Hierarchical SB.STZNone (hierHPs gtt s) (SB.Centered $ cPriorF gtt s)
       ncGMCenterF gtt s bv@(SB.StanVar sn st) brv = do
         bv' <- SB.stanDeclare sn st ""
         SB.addExprLine ("nonCentered for " <> SB.taggedGroupName gtt)
           $ SB.vectorizedOne (SB.taggedGroupName gtt) (SB.var bv' `SB.eq` (SB.var (muV gtt s) `SB.plus`  (SB.var (sigmaV gtt s) `SB.times`  SB.var brv)))
       rawPriorF gtt s v = SB.addExprLine "ELectionResult.groupModel"
         $ SB.vectorizedOne (SB.taggedGroupName gtt) $ SB.var v `SB.vectorSample` SB.stdNormal
       hierGroupModelNC :: forall k gq md.SB.GroupTypeTag k -> Text -> SB.GroupModel gq md
       hierGroupModelNC gtt s = SB.Hierarchical SB.STZNone (hierHPs gtt s) (SB.NonCentered (rawPriorF gtt s) (ncGMCenterF gtt s))
       gmSigmaName gtt suffix = "sigma" <> suffix <> "_" <> SB.taggedGroupName gtt
       groupModelMR gtt s = SB.hierarchicalCenteredFixedMeanNormal 0 (gmSigmaName gtt s) sigmaPrior SB.STZNone

--   gStateT <- MRP.addGroup voteData binaryPrior (hierGroupModel stateGroup) stateGroup (Just "T")

   let modelSpecific BaseG = do
         (gSexT, sexTV) <- MRP.addGroup voteData binaryPrior simpleGroupModel sexGroup (Just "T")
         (gEduT, eduTV) <- MRP.addGroup voteData binaryPrior simpleGroupModel educationGroup (Just "T")
         (gSexP, sexPV) <- MRP.addGroup voteData binaryPrior simpleGroupModel sexGroup (Just "P")
         (gEduP, eduPV) <- MRP.addGroup voteData binaryPrior simpleGroupModel educationGroup (Just "P")
         (gRaceT, raceTV) <- MRP.addGroup voteData binaryPrior (hierGroupModel raceGroup "T") raceGroup (Just "T")
         (gRaceP, racePV) <- MRP.addGroup voteData binaryPrior (hierGroupModel raceGroup "P") raceGroup (Just "P")
         return (\d -> SB.multiOp "+" $ d :| [gRaceT, gSexT, gEduT]
                ,\d -> SB.multiOp "+" $ d :| [gRaceT, gSexT, gEduT]
                ,\d -> SB.multiOp "+" $ d :| [gRaceP, gSexP, gEduP]
                ,\d -> SB.multiOp "+" $ d :| [gRaceP, gSexP, gEduP]
                )
       modelSpecific PlusStateG = do
         (gSexT, sexTV) <- MRP.addGroup voteData binaryPrior simpleGroupModel sexGroup (Just "T")
         (gEduT, eduTV) <- MRP.addGroup voteData binaryPrior simpleGroupModel educationGroup (Just "T")
         (gSexP, sexPV) <- MRP.addGroup voteData binaryPrior simpleGroupModel sexGroup (Just "P")
         (gEduP, eduPV) <- MRP.addGroup voteData binaryPrior simpleGroupModel educationGroup (Just "P")
         (gRaceT, raceTV) <- MRP.addGroup voteData binaryPrior (hierGroupModel raceGroup "T") raceGroup (Just "T")
         (gRaceP, racePV) <- MRP.addGroup voteData binaryPrior (hierGroupModel raceGroup "P") raceGroup (Just "P")
         (gStateT, stateTV) <- MRP.addGroup voteData binaryPrior (groupModelMR stateGroup "T") stateGroup (Just "T")
         (gStateP, statePV) <- MRP.addGroup voteData binaryPrior (groupModelMR stateGroup "P") stateGroup (Just "P")
         let logitT d = SB.multiOp "+" $ d :| [gRaceT, gSexT, gEduT, gStateT]
             logitP d = SB.multiOp "+" $ d :| [gRaceP, gSexP, gEduP, gStateP]
         return (logitT, logitT, logitP, logitP)
       modelSpecific PlusSexEduG = do
         (gSexT, sexTV) <- MRP.addGroup voteData binaryPrior simpleGroupModel sexGroup (Just "T")
         (gEduT, eduTV) <- MRP.addGroup voteData binaryPrior simpleGroupModel educationGroup (Just "T")
         (gSexP, sexPV) <- MRP.addGroup voteData binaryPrior simpleGroupModel sexGroup (Just "P")
         (gEduP, eduPV) <- MRP.addGroup voteData binaryPrior simpleGroupModel educationGroup (Just "P")
         (gRaceT, raceTV) <- MRP.addGroup voteData binaryPrior (hierGroupModel raceGroup "T") raceGroup (Just "T")
         (gRaceP, racePV) <- MRP.addGroup voteData binaryPrior (hierGroupModel raceGroup "P") raceGroup (Just "P")
         let hierGM s = SB.hierarchicalCenteredFixedMeanNormal 0 ("sigmaSexEdu" <> s) sigmaPrior SB.STZNone
         sexEduT <- MRP.addInteractions2 voteData (hierGM "T") sexGroup educationGroup (Just "T")
         vSexEduT <- SB.inBlock SB.SBModel $ SB.vectorizeVar sexEduT (SB.dataSetName voteData)
         sexEduP <- MRP.addInteractions2 voteData (hierGM "P") sexGroup educationGroup (Just "P")
         vSexEduP <- SB.inBlock SB.SBModel $ SB.vectorizeVar sexEduP (SB.dataSetName voteData)
         let logitT_sample d = SB.multiOp "+" $ d :| [gRaceT, gSexT, gEduT, SB.var vSexEduT]
             logitT_ps d = SB.multiOp "+" $ d :| [gRaceT, gSexT, gEduT, SB.var sexEduT]
             logitP_sample d = SB.multiOp "+" $ d :| [gRaceP, gSexP, gEduP, SB.var vSexEduP]
             logitP_ps d = SB.multiOp "+" $ d :| [gRaceP, gSexP, gEduP, SB.var sexEduP]
         return (logitT_sample, logitT_ps, logitP_sample, logitP_ps)
       modelSpecific PlusRaceEduG = do
         (gSexT, sexTV) <- MRP.addGroup voteData binaryPrior simpleGroupModel sexGroup (Just "T")
         (gEduT, eduTV) <- MRP.addGroup voteData binaryPrior simpleGroupModel educationGroup (Just "T")
         (gSexP, sexPV) <- MRP.addGroup voteData binaryPrior simpleGroupModel sexGroup (Just "P")
         (gEduP, eduPV) <- MRP.addGroup voteData binaryPrior simpleGroupModel educationGroup (Just "P")
         (gRaceT, raceTV) <- MRP.addGroup voteData binaryPrior (hierGroupModel raceGroup "T") raceGroup (Just "T")
         (gRaceP, racePV) <- MRP.addGroup voteData binaryPrior (hierGroupModel raceGroup "P") raceGroup (Just "P")
         let groups = MRP.addGroupForInteractions raceGroup
                      $ MRP.addGroupForInteractions educationGroup mempty
             hierGM s = SB.hierarchicalCenteredFixedMeanNormal 0 ("sigmaRaceEdu" <> s) sigmaPrior SB.STZNone
         raceEduT <- MRP.addInteractions voteData (hierGM "T") groups 2 (Just "T")
         vRaceEduT <- traverse (\x -> SB.inBlock SB.SBModel $ SB.vectorizeVar x (SB.dataSetName voteData)) raceEduT
         raceEduP <- MRP.addInteractions voteData (hierGM "P") groups 2 (Just "P")
         vRaceEduP <- traverse (\x -> SB.inBlock SB.SBModel $ SB.vectorizeVar x (SB.dataSetName voteData)) raceEduP
         let logitT_sample d = SB.multiOp "+" $ d :| ([gRaceT, gSexT, gEduT] ++ fmap SB.var vRaceEduT)
             logitT_ps d = SB.multiOp "+" $ d :| ([gRaceT, gSexT, gEduT] ++ fmap SB.var raceEduT)
             logitP_sample d = SB.multiOp "+" $ d :| ([gRaceP, gSexP, gEduP] ++ fmap SB.var vRaceEduP)
             logitP_ps d = SB.multiOp "+" $ d :| ([gRaceP, gSexP, gEduP] ++ fmap SB.var raceEduP)
         return (logitT_sample, logitT_ps, logitP_sample, logitP_ps)
       modelSpecific PlusStateRaceG = do
         (gSexT, sexTV) <- MRP.addGroup voteData binaryPrior simpleGroupModel sexGroup (Just "T")
         (gEduT, eduTV) <- MRP.addGroup voteData binaryPrior simpleGroupModel educationGroup (Just "T")
         (gSexP, sexPV) <- MRP.addGroup voteData binaryPrior simpleGroupModel sexGroup (Just "P")
         (gEduP, eduPV) <- MRP.addGroup voteData binaryPrior simpleGroupModel educationGroup (Just "P")
         let hyperPriorE = SB.normal Nothing (SB.scalar $ show 5)
         (stateRaceTE, stateRaceTE') <- MRP.addMultivariateHierarchical
                                     voteData
                                     undefined
                                     (False, hyperPriorE, hyperPriorE, hyperPriorE, 4)
                                     stateGroup
                                     raceGroup
                                     (Just "T")
         (stateRacePE, stateRacePE') <- MRP.addMultivariateHierarchical
                                       voteData
                                       undefined
                                       (False, hyperPriorE, hyperPriorE, hyperPriorE, 4)
                                       stateGroup
                                       raceGroup
                                       (Just "P")
         let logitT_sample d = SB.multiOp "+" $ d :| [gSexT, gEduT, stateRaceTE]
             logitT_ps d = SB.multiOp "+" $ d :| [gSexT, gEduT, stateRaceTE']
             logitP_sample d = SB.multiOp "+" $ d :| [gSexP, gEduP, stateRacePE]
             logitP_ps d = SB.multiOp "+" $ d :| [gSexP, gEduP, stateRacePE']
         return (logitT_sample, logitT_ps, logitP_sample, logitP_ps)
       modelSpecific PartiallyPooledStateG = do
         let hyperPriorE = SB.normal Nothing (SB.scalar $ show 2)
         (stateSexTE, stateSexTE') <- MRP.addMultivariateHierarchical
                                     voteData
                                     (hierGroupModelNC stateGroup "ST")
                                     (False, hyperPriorE, hyperPriorE, hyperPriorE, 4)
                                     stateGroup
                                     sexGroup
                                     (Just "T")
         (stateSexPE, stateSexPE') <- MRP.addMultivariateHierarchical
                                     voteData
                                     (hierGroupModelNC stateGroup "SP")
                                     (False, hyperPriorE, hyperPriorE, hyperPriorE, 4)
                                     stateGroup
                                     sexGroup
                                     (Just "P")
         (stateEduTE, stateEduTE') <- MRP.addMultivariateHierarchical
                                     voteData
                                     (hierGroupModelNC stateGroup "ET")
                                     (False, hyperPriorE, hyperPriorE, hyperPriorE, 4)
                                     stateGroup
                                     educationGroup
                                     (Just "T")
         (stateEduPE, stateEduPE') <- MRP.addMultivariateHierarchical
                                     voteData
                                     (hierGroupModelNC stateGroup "EP")
                                     (False, hyperPriorE, hyperPriorE, hyperPriorE, 4)
                                     stateGroup
                                     educationGroup
                                     (Just "P")
         (stateRaceTE, stateRaceTE') <- MRP.addMultivariateHierarchical
                                     voteData
                                     simpleGroupModel
                                     (False, hyperPriorE, hyperPriorE, hyperPriorE, 4)
                                     stateGroup
                                     raceGroup
                                     (Just "T")
         (stateRacePE, stateRacePE') <- MRP.addMultivariateHierarchical
                                       voteData
                                       simpleGroupModel
                                       (False, hyperPriorE, hyperPriorE, hyperPriorE, 4)
                                       stateGroup
                                       raceGroup
                                       (Just "P")
         let logitT_sample d = SB.multiOp "+" $ d :| [stateSexTE, stateEduTE, stateRaceTE]
             logitT_ps d = SB.multiOp "+" $ d :| [stateSexTE', stateEduTE', stateRaceTE']
             logitP_sample d = SB.multiOp "+" $ d :| [stateSexPE, stateEduPE, stateRacePE]
             logitP_ps d = SB.multiOp "+" $ d :| [stateSexPE', stateEduPE', stateRacePE']
         return (logitT_sample, logitT_ps, logitP_sample, logitP_ps)
       modelSpecific PlusInteractionsG = do
         (gSexT, sexTV) <- MRP.addGroup voteData binaryPrior simpleGroupModel sexGroup (Just "T")
         (gEduT, eduTV) <- MRP.addGroup voteData binaryPrior simpleGroupModel educationGroup (Just "T")
         (gSexP, sexPV) <- MRP.addGroup voteData binaryPrior simpleGroupModel sexGroup (Just "P")
         (gEduP, eduPV) <- MRP.addGroup voteData binaryPrior simpleGroupModel educationGroup (Just "P")
         (gRaceT, raceTV) <- MRP.addGroup voteData binaryPrior (hierGroupModel raceGroup "T") raceGroup (Just "T")
         (gRaceP, racePV) <- MRP.addGroup voteData binaryPrior (hierGroupModel raceGroup "P") raceGroup (Just "P")
         let groups = MRP.addGroupForInteractions raceGroup
                      $ MRP.addGroupForInteractions sexGroup
                      $ MRP.addGroupForInteractions educationGroup mempty
             hierGM s = SB.hierarchicalCenteredFixedMeanNormal 0 ("sigmaRaceSexEdu" <> s) sigmaPrior SB.STZNone
         interT <- MRP.addInteractions voteData (hierGM "T") groups 2 (Just "T")
         vInterT <- traverse (\x -> SB.inBlock SB.SBModel $ SB.vectorizeVar x (SB.dataSetName voteData)) interT
         interP <- MRP.addInteractions voteData (hierGM "P") groups 2 (Just "P")
         vInterP <- traverse (\x -> SB.inBlock SB.SBModel $ SB.vectorizeVar x (SB.dataSetName voteData)) interP
         let logitT_sample d = SB.multiOp "+" $ d :| ([gRaceT, gSexT, gEduT] ++ fmap SB.var vInterT)
             logitT_ps d = SB.multiOp "+" $ d :| ([gRaceT, gSexT, gEduT] ++ fmap SB.var interT)
             logitP_sample d = SB.multiOp "+" $ d :| ([gRaceP, gSexP, gEduP] ++ fmap SB.var vInterP)
             logitP_ps d = SB.multiOp "+" $ d :| ([gRaceP, gSexP, gEduP] ++ fmap SB.var interP)
         return (logitT_sample, logitT_ps, logitP_sample, logitP_ps)
       modelSpecific PlusStateAndStateRaceG = do
         (gSexT, sexTV) <- MRP.addGroup voteData binaryPrior simpleGroupModel sexGroup (Just "T")
         (gEduT, eduTV) <- MRP.addGroup voteData binaryPrior simpleGroupModel educationGroup (Just "T")
         (gSexP, sexPV) <- MRP.addGroup voteData binaryPrior simpleGroupModel sexGroup (Just "P")
         (gEduP, eduPV) <- MRP.addGroup voteData binaryPrior simpleGroupModel educationGroup (Just "P")
         (gRaceT, raceTV) <- MRP.addGroup voteData binaryPrior (hierGroupModel raceGroup "T") raceGroup (Just "T")
         (gRaceP, racePV) <- MRP.addGroup voteData binaryPrior (hierGroupModel raceGroup "P") raceGroup (Just "P")
         (gStateT, stateTV) <- MRP.addGroup voteData binaryPrior (groupModelMR stateGroup "T") stateGroup (Just "T")
         (gStateP, statePV) <- MRP.addGroup voteData binaryPrior (groupModelMR stateGroup "P") stateGroup (Just "P")
         let groups = MRP.addGroupForInteractions stateGroup
                      $ MRP.addGroupForInteractions raceGroup mempty
             hierGM s = SB.hierarchicalCenteredFixedMeanNormal 0 ("sigmaStateRaceEdu" <> s) sigmaPrior SB.STZNone
         interT <- MRP.addInteractions voteData (hierGM "T") groups 2 (Just "T")
         vInterT <- traverse (\x -> SB.inBlock SB.SBModel $ SB.vectorizeVar x (SB.dataSetName voteData)) interT
         interP <- MRP.addInteractions voteData (hierGM "P") groups 2 (Just "P")
         vInterP <- traverse (\x -> SB.inBlock SB.SBModel $ SB.vectorizeVar x (SB.dataSetName voteData)) interP
         let logitT_sample d = SB.multiOp "+" $ d :| ([gRaceT, gSexT, gEduT, gStateT] ++ fmap SB.var vInterT)
             logitT_ps d = SB.multiOp "+" $ d :| ([gRaceT, gSexT, gEduT, gStateT] ++ fmap SB.var interT)
             logitP_sample d = SB.multiOp "+" $ d :| ([gRaceP, gSexP, gEduP, gStateP] ++ fmap SB.var vInterP)
             logitP_ps d = SB.multiOp "+" $ d :| ([gRaceP, gSexP, gEduP, gStateP] ++ fmap SB.var interP)
         return (logitT_sample, logitT_ps, logitP_sample, logitP_ps)
       modelSpecific PlusStateAndStateInteractionsG = do
         (gSexT, sexTV) <- MRP.addGroup voteData binaryPrior simpleGroupModel sexGroup (Just "T")
         (gEduT, eduTV) <- MRP.addGroup voteData binaryPrior simpleGroupModel educationGroup (Just "T")
         (gSexP, sexPV) <- MRP.addGroup voteData binaryPrior simpleGroupModel sexGroup (Just "P")
         (gEduP, eduPV) <- MRP.addGroup voteData binaryPrior simpleGroupModel educationGroup (Just "P")
         (gRaceT, raceTV) <- MRP.addGroup voteData binaryPrior (hierGroupModel raceGroup "T") raceGroup (Just "T")
         (gRaceP, racePV) <- MRP.addGroup voteData binaryPrior (hierGroupModel raceGroup "P") raceGroup (Just "P")
         (gStateT, stateTV) <- MRP.addGroup voteData binaryPrior (groupModelMR stateGroup "T") stateGroup (Just "T")
         (gStateP, statePV) <- MRP.addGroup voteData binaryPrior (groupModelMR stateGroup "P") stateGroup (Just "P")
         let iGroupRace = MRP.addGroupForInteractions stateGroup
                          $ MRP.addGroupForInteractions raceGroup mempty
             iGroupSex = MRP.addGroupForInteractions stateGroup
                         $ MRP.addGroupForInteractions sexGroup mempty
             iGroupEdu = MRP.addGroupForInteractions stateGroup
                         $ MRP.addGroupForInteractions educationGroup mempty
             hierGM s = SB.hierarchicalCenteredFixedMeanNormal 0 ("sigmaStateRaceEdu" <> s) sigmaPrior SB.STZNone
         stateRaceT <- MRP.addInteractions voteData (hierGM "T") iGroupRace 2 (Just "T")
         vStateRaceT <- traverse (\x -> SB.inBlock SB.SBModel $ SB.vectorizeVar x (SB.dataSetName voteData)) stateRaceT
         stateRaceP <- MRP.addInteractions voteData (hierGM "P") iGroupRace 2 (Just "P")
         vStateRaceP <- traverse (\x -> SB.inBlock SB.SBModel $ SB.vectorizeVar x (SB.dataSetName voteData)) stateRaceP
         stateSexT <- MRP.addInteractions voteData (hierGM "T") iGroupSex 2 (Just "T")
         vStateSexT <- traverse (\x -> SB.inBlock SB.SBModel $ SB.vectorizeVar x (SB.dataSetName voteData)) stateSexT
         stateSexP <- MRP.addInteractions voteData (hierGM "P") iGroupSex 2 (Just "P")
         vStateSexP <- traverse (\x -> SB.inBlock SB.SBModel $ SB.vectorizeVar x (SB.dataSetName voteData)) stateSexP
         stateEduT <- MRP.addInteractions voteData (hierGM "T") iGroupEdu 2 (Just "T")
         vStateEduT <- traverse (\x -> SB.inBlock SB.SBModel $ SB.vectorizeVar x (SB.dataSetName voteData)) stateEduT
         stateEduP <- MRP.addInteractions voteData (hierGM "P") iGroupEdu 2 (Just "P")
         vStateEduP <- traverse (\x -> SB.inBlock SB.SBModel $ SB.vectorizeVar x (SB.dataSetName voteData)) stateEduP
         let logitT_sample d = SB.multiOp "+" $ d :| ([gRaceT, gSexT, gEduT, gStateT]
                                                       ++ fmap SB.var vStateRaceT
                                                       ++ fmap SB.var vStateSexT
                                                       ++ fmap SB.var vStateEduT
                                                     )
             logitT_ps d = SB.multiOp "+" $ d :| ([gRaceT, gSexT, gEduT, gStateT]
                                                   ++ fmap SB.var stateRaceT
                                                   ++ fmap SB.var stateSexT
                                                   ++ fmap SB.var stateEduT
                                                 )
             logitP_sample d = SB.multiOp "+" $ d :| ([gRaceP, gSexP, gEduP, gStateP]
                                                       ++ fmap SB.var vStateRaceP
                                                       ++ fmap SB.var vStateSexP
                                                       ++ fmap SB.var vStateEduP
                                                     )
             logitP_ps d = SB.multiOp "+" $ d :| ([gRaceP, gSexP, gEduP, gStateP]
                                                   ++ fmap SB.var stateRaceP
                                                   ++ fmap SB.var stateSexP
                                                   ++ fmap SB.var stateEduP
                                                 )
         return (logitT_sample, logitT_ps, logitP_sample, logitP_ps)
       modelSpecific _ = SB.stanBuildError $ "Unknown group model type given to groupModelBuilder: " <> show groupModel
   modelSpecific groupModel

electionModel :: forall rs ks r.
                 (K.KnitEffects r
                 , BR.CacheEffects r
                 , F.ElemOf rs BR.StateAbbreviation
                 , F.ElemOf rs DT.CollegeGradC
                 , F.ElemOf rs DT.SexC
                 , F.ElemOf rs DT.Race5C
                 , F.ElemOf rs DT.HispC
                 , F.ElemOf rs DT.PopPerSqMile
                 , F.ElemOf rs Census.Count
                 ,  (ks V.++ '[MT.ModelId Model, ModeledShare]) F.⊆  (BR.Year : MT.ModelId Model : (ks V.++ '[ModeledShare]))
                 , FI.RecVec (ks V.++ '[ModeledShare])
                 , FI.RecVec  (ks V.++ '[MT.ModelId Model, ModeledShare])
                 , ks F.⊆ rs
                 , V.RMap (ks V.++ '[MT.ModelId Model, ModeledShare])
                 , Show (F.Record ks)
                 , Typeable rs
                 , Typeable ks
                 , Ord (F.Record ks)
                 , Flat.GFlatDecode
                          (Flat.Rep
                             (F.Rec FS.SElField (ks V.++ '[MT.ModelId Model, ModeledShare])))
                 , Flat.GFlatEncode
                          (Flat.Rep
                             (F.Rec FS.SElField (ks V.++ '[MT.ModelId Model, ModeledShare])))
                 , Flat.GFlatSize
                          (Flat.Rep
                             (F.Rec FS.SElField (ks V.++ '[MT.ModelId Model, ModeledShare])))
                 , Generic
                   (F.Rec FS.SElField (ks V.++ '[MT.ModelId Model, ModeledShare]))
                 )
              => Bool
              -> Bool
              -> BR.StanParallel
              -> Text
              -> Model
              -> Int
              -> (SB.GroupTypeTag (F.Record ks), Text, SB.GroupSet)
              -> K.ActionWithCacheTime r CCESAndPUMS
              -> K.ActionWithCacheTime r (F.FrameRec rs)
              -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec (ModelResultsR ks)))
electionModel clearCaches parallel stanParallelCfg modelDir model datYear (psGroup, psDataSetName, psGroupSet) dat_C psDat_C = K.wrapPrefix "stateLegModel" $ do
  K.logLE K.Info $ "(Re-)running turnout/pref model if necessary."
  densityData <- districtRows <$> K.ignoreCacheTime dat_C
  let jsonDataName = modelLabel model <> "_" <> show datYear <> if parallel then "_P" else ""  -- because of grainsize
      densityRowFromData :: (F.ElemOf ls DT.PopPerSqMile) => SB.MatrixRowFromData (F.Record ls)
      densityRowFromData = densityMatrixRowFromData (densityTransform model) densityData

      dataAndCodeBuilder :: MRP.BuilderM CCESAndPUMS (F.FrameRec rs) ()
      dataAndCodeBuilder = do
        -- data
        voteData <- SB.dataSetTag @(F.Record CCESByCDR) SC.ModelData "VoteData"
        cdData <- SB.dataSetTag @(F.Record DistrictDemDataR) SC.ModelData "CDData"
        SB.addDataSetsCrosswalk voteData cdData cdGroup
        SB.setDataSetForBindings voteData
        pplWgtsCD <- SB.addCountData cdData "Citizens" (F.rgetField @PUMS.Citizens)

        -- Turnout
        cvap <- SB.addCountData voteData "CVAP" (F.rgetField @Surveyed)
        votes <- SB.addCountData voteData "VOTED" (F.rgetField @Voted)
        let distT = SB.binomialLogitDist cvap

        -- Preference
        let (votesF, dVotesF) = getVotes $ voteSource model
        hVotes <- SB.addCountData voteData "VOTES_C" votesF --(F.rgetField @HouseVotes)
        dVotes <- SB.addCountData voteData "DVOTES_C" dVotesF --(F.rgetField @HouseDVotes)
        let distP = SB.binomialLogitDist hVotes

        -- fixed effects (density)
        let normal x = SB.normal Nothing $ SB.scalar $ show x
            fePrior = normal 2
        (feMatrices, centerF) <- SFE.addFixedEffectsData cdData  densityRowFromData (Just pplWgtsCD)--(SFE.FixedEffects 1 densityPredictor)
        (thetaTMultF, betaTMultF, thetaPMultF, betaPMultF) <- densityModelBuilder (densityModel model) feMatrices fePrior voteData cdData

        (q', feCDT, feCDP) <- SB.inBlock SB.SBModel $ SB.useDataSetForBindings voteData $ do
          q <- SB.stanBuildEither $ SFE.qrM SFE.qM feMatrices
          reIndexedQ <- SFE.reIndex (SB.dataSetName cdData) (SB.crosswalkIndexKey cdData) q
          feCDT' <- thetaTMultF (SB.dataSetName voteData) reIndexedQ
          feCDP' <- thetaPMultF (SB.dataSetName voteData) reIndexedQ
          return (reIndexedQ, feCDT', feCDP')
--        SB.stanBuildError $ "Q'="  <> show q'

        -- groups
        let binaryPrior = normal 2
            sigmaPrior = normal 2
        (logitT_sample, logitT_ps, logitP_sample, logitP_ps) <- groupModelBuilder (groupModel model) binaryPrior sigmaPrior voteData

        let serialSample = do
              SB.sampleDistV voteData distT (logitT_sample feCDT) votes
              SB.sampleDistV voteData distP (logitP_sample feCDP) dVotes
            parallelSample = do
              SB.parallelSampleDistV "turnout" voteData distT (logitT_sample feCDT) votes
              SB.parallelSampleDistV "preference" voteData distP (logitP_sample feCDP) dVotes

        if parallel then parallelSample else serialSample

        psData <- SB.dataSetTag @(F.Record rs) SC.GQData "DistrictPS"

        let psPreCompute = do
              mv <- SB.add2dMatrixData psData densityRowFromData (Just 0) Nothing --(Vector.singleton . getDensity)
              cmVar <- centerF mv
              densityTV' <- betaTMultF (SB.dataSetName psData) cmVar --SB.matMult cmVar betaTVar
              densityPV' <- betaPMultF (SB.dataSetName psData) cmVar -- SB.matMult cmVar betaPVar
              return (densityTV', densityPV')

            psExprF (densityTV, densityPV) = do
              pT <- SB.stanDeclareRHS "pT" SB.StanReal "" $ SB.familyExp distT $ logitT_ps densityTV
              pD <- SB.stanDeclareRHS "pD" SB.StanReal "" $ SB.familyExp distP $ logitP_ps densityPV
              --return $ SB.var pT `SB.times` SB.paren ((SB.scalar "2" `SB.times` SB.var pD) `SB.minus` SB.scalar "1")
              return $ SB.var pT `SB.times` SB.var pD
--            pTExprF ik = SB.stanDeclareRHS "pT" SB.StanReal "" $ SB.familyExp distT ik $ logitT_ps densityTE
--            pDExprF ik = SB.stanDeclareRHS "pD" SB.StanReal "" $ SB.familyExp distP ik $ logitP_ps densityPE

        let postStrat =
              MRP.addPostStratification -- @(CCESAndPUMS, F.FrameRec rs)
              (psPreCompute, psExprF)
              Nothing
              voteData
              psData
              psGroupSet
              (realToFrac . F.rgetField @Census.Count)
              (MRP.PSShare $ Just $ SB.name "pT")
              (Just psGroup)
        postStrat

{-
        SB.generateLogLikelihood' voteData ((distT, logitT_ps <$> thetaTMultF (SB.dataSetName voteData) q', votes)
                                             :| [(distP, logitP_ps <$> thetaPMultF (SB.dataSetName voteData) q', dVotes)])
-}
        return ()

      addModelIdAndYear :: F.Record (ks V.++ '[ModeledShare])
                        -> F.Record (ModelResultsR ks)
      addModelIdAndYear r = F.rcast $ FT.recordSingleton @BR.Year datYear F.<+> FT.recordSingleton @(MT.ModelId Model) model F.<+> r
      extractResults :: K.KnitEffects r
                     => SC.ResultAction r md gq SB.DataSetGroupIntMaps () (FS.SFrameRec (ModelResultsR ks))
      extractResults = SC.UseSummary f where
        f summary _ modelDataAndIndex_C mGQDataAndIndex_C = do
          gqIndexes_C <- K.knitMaybe "StanMRP.extractResults: gqDataAndIndex is Nothing" $ mGQDataAndIndex_C
          gqIndexesE <- K.ignoreCacheTime $ fmap snd gqIndexes_C
          resultsMap <- K.knitEither $ do
            gqIndexes <- gqIndexesE
            psIndexIM <- SB.getGroupIndex
              (SB.RowTypeTag @(F.Record rs) SC.GQData "DistrictPS")
              psGroup
              gqIndexes
            let parseAndIndexPctsWith idx g vn = do
                  v <- SP.getVector . fmap CS.percents <$> SP.parse1D vn (CS.paramStats summary)
                  indexStanResults idx $ Vector.map g v
            parseAndIndexPctsWith psIndexIM id $ "PS_DistrictPS_" <> SB.taggedGroupName psGroup
          res :: F.FrameRec (ks V.++ '[ModeledShare]) <- K.knitEither
                                                         $ MT.keyedCIsToFrame @ModeledShare id
                                                         $ M.toList resultsMap
          return $ FS.SFrame $ fmap addModelIdAndYear res
      dataWranglerAndCode :: K.ActionWithCacheTime r CCESAndPUMS
                          -> K.ActionWithCacheTime r (F.FrameRec rs)
                          -> K.Sem r (SC.DataWrangler CCESAndPUMS (F.FrameRec rs) SB.DataSetGroupIntMaps (), SB.StanCode)
      dataWranglerAndCode modelData_C gqData_C = do
        modelData <-  K.ignoreCacheTime modelData_C
        gqData <-  K.ignoreCacheTime gqData_C
        K.logLE K.Info
          $ "Voter data (CCES) has "
          <> show (FL.fold FL.length $ ccesRows $ modelData)
          <> " rows."
        let (districts, states) = FL.fold
                                  ((,)
                                   <$> (FL.premap districtKey FL.list)
                                   <*> (FL.premap (F.rgetField @BR.StateAbbreviation) FL.list)
                                  )
                                  $ districtRows modelData
            psKeys = FL.fold (FL.premap F.rcast FL.list) gqData
            groups = groupBuilder psGroup districts states psKeys
        K.logLE K.Info $ show $ zip [1..] $ Set.toList $ FL.fold FL.set states
        MRP.buildDataWranglerAndCode @BR.SerializerC @BR.CacheData groups dataAndCodeBuilder modelData_C gqData_C
  (dw, stanCode) <- dataWranglerAndCode dat_C psDat_C
  fmap (fmap FS.unSFrame)
    $ MRP.runMRPModel
    clearCaches
    (SC.RunnerInputNames modelDir  ("LegDistricts_" <> modelLabel model <> if parallel then "_P" else "") (Just psDataSetName) jsonDataName)
    (SC.StanMCParameters 4 4 (Just 1000) (Just 1000) (Just 0.8) (Just 10) Nothing)
    stanParallelCfg
    dw
    stanCode
    "DVOTES_C"
    extractResults
    dat_C
    psDat_C

type SLDLocation = (Text, ET.DistrictType, Int)

sldLocationToRec :: SLDLocation -> F.Record [BR.StateAbbreviation, ET.DistrictTypeC, ET.DistrictNumber]
sldLocationToRec (sa, dt, dn) = sa F.&: dt F.&: dn F.&: V.RNil

type ModeledShare = "ModeledShare" F.:-> MT.ConfidenceInterval

type ModelResultsR ks = '[BR.Year] V.++ ks V.++ '[MT.ModelId Model, ModeledShare]

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

{-
race5FromCPS :: F.Record CPSVByCDR -> DT.Race5
race5FromCPS r =
  let race4A = F.rgetField @DT.RaceAlone4C r
      hisp = F.rgetField @DT.HispC r
  in DT.race5FromRaceAlone4AndHisp True race4A hisp
-}
race5FromCensus :: F.Record CPSVByCDR -> DT.Race5
race5FromCensus r =
  let race4A = F.rgetField @DT.RaceAlone4C r
      hisp = F.rgetField @DT.HispC r
  in DT.race5FromRaceAlone4AndHisp True race4A hisp

-- many many people who identify as hispanic also identify as white. So we need to choose.
-- Better to model using both
mergeRace5AndHispanic r =
  let r5 = F.rgetField @DT.Race5C r
      h = F.rgetField @DT.HispC r
  in if (h == DT.Hispanic) then DT.R5_Hispanic else r5

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
wnhCCES r = (F.rgetField @DT.Race5C r == DT.R5_WhiteNonHispanic) && (F.rgetField @DT.HispC r == DT.NonHispanic)
wnhNonGradCCES r = wnhCCES r && (F.rgetField @DT.CollegeGradC r == DT.NonGrad)


densityMatrixRowFromData :: (F.ElemOf rs DT.PopPerSqMile)
                         => DensityTransform
                         -> F.FrameRec DistrictDemDataR
                         -> SB.MatrixRowFromData (F.Record rs)
densityMatrixRowFromData RawDensity _ = (SB.MatrixRowFromData "Density" 1 f)
  where
   f = VU.fromList . pure . F.rgetField @DT.PopPerSqMile
densityMatrixRowFromData LogDensity _ =
  (SB.MatrixRowFromData "Density" 1 logDensityPredictor)
densityMatrixRowFromData (QuantileDensity n) dat = SB.MatrixRowFromData "Density" 1 f where
  sortedData = List.sort $ FL.fold (FL.premap (F.rgetField @DT.PopPerSqMile) FL.list) dat
  quantileSize = List.length sortedData `div` n
  quantilesExtra = List.length sortedData `rem` n
  quantileMaxIndex k = quantilesExtra + k * quantileSize - 1 -- puts extra in 1st bucket
  quantileBreaks = fmap (\k -> sortedData List.!! quantileMaxIndex k) $ [1..n]
  indexedBreaks = zip quantileBreaks [1..n] -- should this be 0 centered??
  go x [] = n
  go x ((y, k): xs) = if x < y then k else go x xs
  quantileF x = go x indexedBreaks
  g x = VU.fromList [realToFrac $ quantileF x]
  f = g . F.rgetField @DT.PopPerSqMile


--  SB.MatrixRowFromData "Density" 1


--densityRowFromData = SB.MatrixRowFromData "Density" 1 densityPredictor
logDensityPredictor = safeLogV . F.rgetField @DT.PopPerSqMile
safeLogV x =  VU.singleton $ if x < 1e-12 then 0 else Numeric.log x -- won't matter because Pop will be 0 here

raceAlone4FromRace5 :: DT.Race5 -> DT.RaceAlone4
raceAlone4FromRace5 DT.R5_Other = DT.RA4_Other
raceAlone4FromRace5 DT.R5_Black = DT.RA4_Black
raceAlone4FromRace5 DT.R5_Hispanic = DT.RA4_Other
raceAlone4FromRace5 DT.R5_Asian = DT.RA4_Asian
raceAlone4FromRace5 DT.R5_WhiteNonHispanic = DT.RA4_White

indexStanResults :: (Show k, Ord k)
                 => IM.IntMap k
                 -> Vector.Vector a
                 -> Either Text (Map k a)
indexStanResults im v = do
  when (IM.size im /= Vector.length v)
    $ Left $
    "Mismatched sizes in indexStanResults. Result vector has " <> show (Vector.length v) <> " result and IntMap = " <> show im
  return $ M.fromList $ zip (IM.elems im) (Vector.toList v)

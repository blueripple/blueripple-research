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
import Relude.Extra (secondF)
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
import qualified Data.Map.Strict as M
import qualified Data.Map.Merge.Strict as M
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
import qualified Control.MapReduce as FMR
import qualified Control.MapReduce as FMR
import qualified Control.MapReduce as FMR
import qualified Frames.Folds as FF
import qualified BlueRipple.Data.DemographicTypes as DT
import Data.Vector.Internal.Check (checkSlice)
import BlueRipple.Data.DemographicTypes (demographicsFold)

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
type CCESPredictorEMR = [DT.SexC, DT.CollegeGradC, DT.Race5C, DT.HispC] --, DT.AvgIncome, DT.PopPerSqMile]
type CCESByCDEMR = CDKeyR V.++ [DT.SexC, DT.CollegeGradC, DT.Race5C, DT.HispC] V.++ CCESVotingDataR
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
type PUMSDataEMR = [DT.SexC, DT.CollegeGradC, DT.RaceAlone4C, DT.HispC, PUMS.Citizens, PUMS.NonCitizens]

type PUMSByCDR = CDKeyR V.++ PUMSDataR
type PUMSByCDEMR = CDKeyR V.++ PUMSDataEMR
type PUMSByCD = F.FrameRec PUMSByCDR
type PUMSByStateR = StateKeyR V.++ PUMSDataR
type PUMSByState = F.FrameRec PUMSByStateR
-- unweighted, which we address via post-stratification
type CensusPredictorR = [DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.RaceAlone4C, DT.HispC]
type CensusPredictorEMR = [DT.SexC, DT.CollegeGradC, DT.RaceAlone4C, DT.HispC]
type CPSVDataR  = CensusPredictorR V.++ BRCF.CountCols
type CPSVDataEMR  = CensusPredictorEMR V.++ BRCF.CountCols
type CPSVByStateR = StateKeyR V.++ CPSVDataR
type CPSVByStateEMR = StateKeyR V.++ CPSVDataEMR
--type CPSVByCDR = CDKeyR V.++ CPSVDataR

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

data CCESAndCPSEM = CCESAndCPSEM { ccesEMRows :: F.FrameRec CCESWithDensityEM
                                 , cpsVEMRows :: F.FrameRec CPSVWithDensityEM
                                 , electionRows :: F.FrameRec (ElectionResultWithDemographicsR '[BR.Year, BR.StateAbbreviation])
                                 } deriving (Generic)

instance Flat.Flat CCESAndCPSEM where
  size (CCESAndCPSEM cces cpsV elex) n = Flat.size (FS.SFrame cces, FS.SFrame cpsV, FS.SFrame elex) n
  encode (CCESAndCPSEM cces cpsV elex) = Flat.encode (FS.SFrame cces, FS.SFrame cpsV, FS.SFrame elex)
  decode = (\(cces, cpsV, elex) -> CCESAndCPSEM (FS.unSFrame cces) (FS.unSFrame cpsV) (FS.unSFrame elex)) <$> Flat.decode

ccesAndCPSForYears :: [Int] -> CCESAndCPSEM -> CCESAndCPSEM
ccesAndCPSForYears ys (CCESAndCPSEM cces cpsV elex) =
  let f :: (FI.RecVec rs, F.ElemOf rs BR.Year) => F.FrameRec rs -> F.FrameRec rs
      f = F.filterFrame ((`elem` ys) . F.rgetField @BR.Year)
  in CCESAndCPSEM (f cces) (f cpsV) (f elex)

prepCCESAndCPSEM :: (K.KnitEffects r, BR.CacheEffects r)
                 => Bool -> K.Sem r (K.ActionWithCacheTime r CCESAndCPSEM)
prepCCESAndCPSEM clearCache = do
  ccesAndPUMS_C <- prepCCESAndPums clearCache
  elex_C <- prepPresidentialElectionData clearCache 2016
--  K.logLE K.Diagnostic "Presidential Election Rows"
--  K.ignoreCacheTime elex_C >>= BR.logFrame
  let cacheKey = "model/house/CCESAndCPSEM.bin"
      deps = (,) <$> ccesAndPUMS_C <*> elex_C
  when clearCache $ BR.clearIfPresentD cacheKey
  BR.retrieveOrMakeD cacheKey deps $ \(ccesAndPums, elex) -> do
    let ccesEM = FL.fold fldAgeInCCES $ ccesRows ccesAndPums
        cpsVEM = FL.fold fldAgeInCPS $ cpsVRows ccesAndPums
    return $ CCESAndCPSEM ccesEM cpsVEM elex

prepACS :: (K.KnitEffects r, BR.CacheEffects r)
        => Bool -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec PUMSWithDensityEM))
prepACS clearCache = do
  ccesAndPUMS_C <- prepCCESAndPums clearCache
  let cacheKey = "model/house/ACS.bin"
  when clearCache $ BR.clearIfPresentD cacheKey
  BR.retrieveOrMakeFrame cacheKey ccesAndPUMS_C $ \ccesAndPums -> return $ FL.fold fldAgeInACS $ pumsRows ccesAndPums

acsForYears :: [Int] -> F.FrameRec PUMSWithDensityEM -> F.FrameRec PUMSWithDensityEM
acsForYears years x =
 let f :: (FI.RecVec rs, F.ElemOf rs BR.Year) => F.FrameRec rs -> F.FrameRec rs
     f = F.filterFrame ((`elem` years) . F.rgetField @BR.Year)
 in f x

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

type ElectionResultWithDemographicsR ks = ks V.++ '[ET.Office] V.++ ElectionR V.++ DemographicsR

makeStateElexDataFrame ::  (K.KnitEffects r)
                       => ET.OfficeT
                       -> Int
                       -> F.FrameRec [BR.Year, BR.StateAbbreviation, BR.Candidate, ET.Party, ET.Votes, ET.Incumbent]
                       -> F.FrameRec PUMS.PUMS
                       -> K.Sem r (F.FrameRec (ElectionResultWithDemographicsR [BR.Year, BR.StateAbbreviation]))
makeStateElexDataFrame office earliestYear elex acs = do
        let addOffice rs = FT.recordSingleton @ET.Office office F.<+> rs
        flattenedElex <- K.knitEither
                         $ FL.foldM (electionF @[BR.Year, BR.StateAbbreviation])
                         (fmap F.rcast $ F.filterFrame ((>= earliestYear) . F.rgetField @BR.Year) elex)
        let demographics = pumsMR @[BR.Year, BR.StateAbbreviation] $ pumsByState $ copy2019to2020 acs
            (elexWithDemo, missing) = FJ.leftJoinWithMissing @[BR.Year, BR.StateAbbreviation] flattenedElex demographics
        when (not $ null missing) $ K.knitError $ "makeStateElexDataFrame: missing keys in flattenedElex/acs join=" <> show missing
        return $ fmap (F.rcast . addOffice) elexWithDemo

prepPresidentialElectionData :: (K.KnitEffects r, BR.CacheEffects r)
                             => Bool
                             -> Int
                             -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec (ElectionResultWithDemographicsR '[BR.Year,BR.StateAbbreviation])))
prepPresidentialElectionData clearCache earliestYear = do
  let cacheKey = "model/house/presElexWithDemographics.bin"
  when clearCache $ BR.clearIfPresentD cacheKey
  presElex_C <- BR.presidentialElectionsWithIncumbency
  acs_C <- PUMS.pumsLoaderAdults
--  acsByState_C <- cachedPumsByState acs_C
  let deps = (,) <$> presElex_C <*> acs_C
  BR.retrieveOrMakeFrame cacheKey deps
    $ \(pElex, acs) -> makeStateElexDataFrame ET.President earliestYear (fmap F.rcast pElex) (fmap F.rcast acs)


prepSenateElectionData :: (K.KnitEffects r, BR.CacheEffects r)
                       => Bool
                       -> Int
                       -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec (ElectionResultWithDemographicsR '[BR.Year, BR.StateAbbreviation])))
prepSenateElectionData clearCache earliestYear = do
  let cacheKey = "model/house/senateElexWithDemographics.bin"
  when clearCache $ BR.clearIfPresentD cacheKey
  senateElex_C <- BR.senateElectionsWithIncumbency
  acs_C <- PUMS.pumsLoaderAdults
--  acsByState_C <- cachedPumsByState acs_C
  let deps = (,) <$> senateElex_C <*> acs_C
  BR.retrieveOrMakeFrame cacheKey deps
    $ \(senateElex, acs) -> makeStateElexDataFrame ET.Senate earliestYear (fmap F.rcast senateElex) (fmap F.rcast acs)


makeCDElexDataFrame ::  (K.KnitEffects r, BR.CacheEffects r)
                       => ET.OfficeT
                       -> Int
                       -> F.FrameRec [BR.Year, BR.StateAbbreviation, BR.CongressionalDistrict, BR.Candidate, ET.Party, ET.Votes, ET.Incumbent]
                       -> K.ActionWithCacheTime r (F.FrameRec PUMS.PUMS)
                       -> K.Sem r (F.FrameRec (ElectionResultWithDemographicsR [BR.Year, BR.StateAbbreviation, BR.CongressionalDistrict]))
makeCDElexDataFrame office earliestYear elex acs_C = do
        let addOffice rs = FT.recordSingleton @ET.Office office F.<+> rs
            fixDC_CD r = if (F.rgetField @BR.StateAbbreviation r == "DC")
                         then FT.fieldEndo @BR.CongressionalDistrict (const 1) r
                         else r

        flattenedElex <- K.knitEither
                         $ FL.foldM (electionF @[BR.Year, BR.StateAbbreviation, BR.CongressionalDistrict])
                         (fmap F.rcast $ F.filterFrame ((>= earliestYear) . F.rgetField @BR.Year) elex)
        cdFromPUMA_C <- BR.allCDFromPUMA2012Loader
        pumsByCD_C <- cachedPumsByCD acs_C cdFromPUMA_C
        pumsByCD <- K.ignoreCacheTime pumsByCD_C
        let demographics = fmap fixDC_CD $ pumsMR @[BR.Year, BR.StateAbbreviation, BR.CongressionalDistrict] pumsByCD
            (elexWithDemo, missing)
              = FJ.leftJoinWithMissing @[BR.Year, BR.StateAbbreviation, BR.CongressionalDistrict]
                (fmap fixDC_CD flattenedElex)
                demographics
        when (not $ null missing) $ K.knitError $ "makeCDElexDataFrame: missing keys in flattenedElex/acs join=" <> show missing
        return $ fmap (F.rcast . addOffice) elexWithDemo


prepHouseElectionData :: (K.KnitEffects r, BR.CacheEffects r)
                       => Bool
                       -> Int
                       -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec (ElectionResultWithDemographicsR '[BR.Year, BR.StateAbbreviation, BR.CongressionalDistrict])))
prepHouseElectionData clearCache earliestYear = do
  let cacheKey = "model/house/houseElexWithDemographics.bin"
  when clearCache $ BR.clearIfPresentD cacheKey
  houseElex_C <- BR.houseElectionsWithIncumbency
  acs_C <- PUMS.pumsLoaderAdults
  let deps = (,) <$> houseElex_C <*> acs_C
  BR.retrieveOrMakeFrame cacheKey deps
    $ \(houseElex, acs) -> makeCDElexDataFrame ET.House earliestYear (fmap F.rcast houseElex) acs_C


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

cesCountedDemVotesByCD :: (K.KnitEffects r, BR.CacheEffects r) => Bool -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec CCESByCDR))
cesCountedDemVotesByCD clearCaches = do
  ces2020_C <- CCES.ces20Loader
  let cacheKey = "model/house/ces20ByCD.bin"
  when clearCaches $  BR.clearIfPresentD cacheKey
  BR.retrieveOrMakeFrame cacheKey ces2020_C $ \ces -> cesMR 2020 ces
--  BR.retrieveOrMakeFrame cacheKey ces2020_C $ return . FL.fold (cesFold 2020)

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

type AchenHurWeight = "AchenHurWeight" F.:-> Double

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
--  K.ignoreCacheTime cpsVByState_C >>= cpsDiagnostics "Pre Achen/Hur"

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
    let ewU r = FT.recordSingleton @AchenHurWeight (realToFrac (F.rgetField @BRCF.Successes r) / realToFrac (F.rgetField @BRCF.Count r))
        cpsWithProbU = fmap (FT.mutate ewU) cpsV
        (cpsWithProbAndCitU, missingU) = FJ.leftJoinWithMissing @(StateKeyR V.++ CensusPredictorR) cpsWithProbU $ acsByState
    when (not $ null missingU) $ K.knitError $ "prepCCESAndPums: Missing keys in unweighted cpsV/acs join: " <> show missingU
    when (fLength cpsWithProbU /= fLength cpsWithProbAndCitU) $ K.knitError "prepCCESAndPums: rows added/deleted by left-join(unweighted cps,acs)"
    adjCPSProbU <- FL.foldM (BRTA.adjTurnoutFold @PUMS.Citizens @AchenHurWeight stateTurnout) cpsWithProbAndCitU
    let adjVotersU = adjUsing @AchenHurWeight @BRCF.Successes @BRCF.Count realToFrac round
        res = fmap (F.rcast @CPSVByStateR . adjVotersU) adjCPSProbU
    K.logLE K.Diagnostic $ "Post Achen-Hur: CPS (by state) rows per year:"
    K.logLE K.Diagnostic $ show $ fmap (\y -> lengthInYear y res) [2012, 2014, 2016, 2018, 2020]
    return res
--  K.ignoreCacheTime cpsV_AchenHur_C >>= cpsDiagnostics "CPS: Post Achen/Hur"
--  K.logLE K.Info "Pre Achen-Hur CCES Diagnostics (post-stratification of raw turnout * raw pref using ACS weights.)"
--  ccesDiagnosticStatesPre <- fmap fst . K.ignoreCacheTimeM $ ccesDiagnostics clearCache "CompositePre" CCESComposite pumsByCD_C countedCCES_C
--  BR.logFrame ccesDiagnosticStatesPre
  let acsWithZeroesCacheKey = "model/house/acsWithZeroes.bin"
  acsWithZeroes_C <- BR.retrieveOrMakeFrame acsWithZeroesCacheKey pumsByCD_C $ \acsByCD -> do
    K.logLE K.Info "Adding missing zeroes to ACS data with CCES predictor cols"
    let zeroCount = 0 F.&: V.RNil :: F.Record '[PUMS.Citizens]
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
    return fixedACS
  let ccesTAchenHurDeps = (,,) <$> countedCCES_C <*> acsWithZeroes_C <*> stateTurnout_C
      ccesTAchenHurCacheKey = "model/house/CCEST_AchenHur.bin"
  when clearCache $ BR.clearIfPresentD ccesTAchenHurCacheKey
  ccesTAchenHur_C <- BR.retrieveOrMakeFrame ccesTAchenHurCacheKey ccesTAchenHurDeps $ \(cces, acsByCD, stateTurnout) -> do
    K.logLE K.Diagnostic $ "Pre Achen-Hur (for turnout): CCES (by CD) rows per year:"
    K.logLE K.Diagnostic $ show $ fmap (\y -> lengthInYear y cces) [2012, 2014, 2016, 2018, 2020]
    K.logLE K.Diagnostic $ "ACS (by CD) rows per year:"
    K.logLE K.Diagnostic $ show $ fmap (\y -> lengthInYear y acsByCD) [2012, 2014, 2016, 2018, 2020]

    K.logLE K.Info "Doing (Ghitza/Gelman) logistic Achen/Hur adjustment to correct CCES for state-specific under-reporting."
    let ew r = FT.recordSingleton @AchenHurWeight (realToFrac (F.rgetField @Voted r) / realToFrac (F.rgetField @Surveyed r))
        ccesWithProb = fmap (FT.mutate ew) cces
        (ccesWithProbAndCit, missing) = FJ.leftJoinWithMissing @(CDKeyR V.++ CCESPredictorR) ccesWithProb acsByCD
    when (not $ null missing) $ K.knitError $ "Missing keys in cces/acs join: " <> show missing
    when (fLength ccesWithProb /= fLength ccesWithProbAndCit) $ K.knitError "prepCCESAndPums: rows added/deleted by left-join(cces,acs)"
    adjCCESProb <- FL.foldM (BRTA.adjTurnoutFold @PUMS.Citizens @AchenHurWeight stateTurnout) ccesWithProbAndCit

    -- NB: only turnout cols adjusted. HouseVotes and HouseDVotes, PresVotes and PresDVotes not adjusted
    let adjVoters = adjUsing @AchenHurWeight @Voted @Surveyed realToFrac round
        res = fmap (F.rcast @CCESByCDR . adjVoters) adjCCESProb
    K.logLE K.Diagnostic $ "Post Achen-Hur: CCES (by CD) rows per year:"
    K.logLE K.Diagnostic $ show $ fmap (\y -> lengthInYear y res) [2012, 2014, 2016, 2018, 2020]
    return res

-- now Achen-Hur for presidential voting, to match known Dem fraction in each state. ??
  presidentialElectionResults_C <- prepPresidentialElectionData clearCache earliestYear
  let ccesPVAchenHurDeps = (,,) <$> ccesTAchenHur_C <*> acsWithZeroes_C <*> presidentialElectionResults_C
      ccesPVAchenHurCacheKey = "model/house/CCESPV_AchenHur.bin"
  when clearCache $ BR.clearIfPresentD ccesPVAchenHurCacheKey
  ccesPVAchenHur_C <- BR.retrieveOrMakeFrame ccesPVAchenHurCacheKey ccesPVAchenHurDeps $ \(cces, acsByCD, presElex) -> do
    K.logLE K.Diagnostic $ "Pre Achen-Hur (for voting): CCES (by CD) rows per year:"
    K.logLE K.Diagnostic $ show $ fmap (\y -> lengthInYear y cces) [2012, 2014, 2016, 2018, 2020]
    K.logLE K.Diagnostic $ "ACS (by CD) rows per year:"
    K.logLE K.Diagnostic $ show $ fmap (\y -> lengthInYear y acsByCD) [2012, 2014, 2016, 2018, 2020]

    K.logLE K.Info "Doing (Ghitza/Gelman) logistic Achen/Hur adjustment to correct CCES voting totals. ??"
    K.logLE K.Info "Adding missing zeroes to ACS data with CCES predictor cols"
    let ew r = FT.recordSingleton @AchenHurWeight (realToFrac (F.rgetField @PresDVotes r) / realToFrac (F.rgetField @PresVotes r))
        ccesWithProb = fmap (FT.mutate ew) cces
        (ccesWithProbAndCit, missing) = FJ.leftJoinWithMissing @(CDKeyR V.++ CCESPredictorR) ccesWithProb acsByCD
    when (not $ null missing) $ K.knitError $ "Missing keys in cces/acs join: " <> show missing
    when (fLength ccesWithProb /= fLength ccesWithProbAndCit) $ K.knitError "prepCCESAndPums: rows added/deleted by left-join(cces,acs)"
    adjCCESProb <- FL.foldM (BRTA.adjTurnoutFoldG @PUMS.Citizens @AchenHurWeight @[BR.Year, BR.StateAbbreviation] (realToFrac . F.rgetField @DVotes) presElex) ccesWithProbAndCit
    let adjVotes = adjUsing @AchenHurWeight @PresDVotes @PresVotes realToFrac round
        res = fmap (F.rcast @CCESByCDR . adjVotes) adjCCESProb
    K.logLE K.Diagnostic $ "Post Achen-Hur (presidential votes): CCES (by CD) rows per year:"
    K.logLE K.Diagnostic $ show $ fmap (\y -> lengthInYear y res) [2012, 2014, 2016, 2018, 2020]
    return res

-- now Achen-Hur for house voting, to match known Dem fraction in each CD. ??
  houseElectionResults_C <- prepHouseElectionData clearCache earliestYear
  let ccesHVAchenHurDeps = (,,) <$> ccesPVAchenHur_C <*> acsWithZeroes_C <*> houseElectionResults_C
      ccesHVAchenHurCacheKey = "model/house/CCESHV_AchenHur.bin"
  when clearCache $ BR.clearIfPresentD ccesHVAchenHurCacheKey
  ccesHVAchenHur_C <- BR.retrieveOrMakeFrame ccesHVAchenHurCacheKey ccesHVAchenHurDeps $ \(cces, acsByCD, houseElex) -> do
    K.logLE K.Diagnostic $ "Pre Achen-Hur (for voting): CCES (by CD) rows per year:"
    K.logLE K.Diagnostic $ show $ fmap (\y -> lengthInYear y cces) [2012, 2014, 2016, 2018, 2020]
    K.logLE K.Diagnostic $ "ACS (by CD) rows per year:"
    K.logLE K.Diagnostic $ show $ fmap (\y -> lengthInYear y acsByCD) [2012, 2014, 2016, 2018, 2020]

    K.logLE K.Info "Doing (Ghitza/Gelman) logistic Achen/Hur adjustment to correct CCES house vote totals. ??"
    let ew r = FT.recordSingleton @AchenHurWeight (realToFrac (F.rgetField @HouseDVotes r) / realToFrac (F.rgetField @HouseVotes r))
        ccesWithProb = fmap (FT.mutate ew) cces
        (ccesWithProbAndCit, missing) = FJ.leftJoinWithMissing @(CDKeyR V.++ CCESPredictorR) ccesWithProb acsByCD
    when (not $ null missing) $ K.knitError $ "Missing keys in cces/acs join: " <> show missing
    when (fLength ccesWithProb /= fLength ccesWithProbAndCit) $ K.knitError "prepCCESAndPums: rows added/deleted by left-join(cces,acs)"
    adjCCESProb <- FL.foldM (BRTA.adjTurnoutFoldG @PUMS.Citizens @AchenHurWeight @[BR.Year, BR.StateAbbreviation, BR.CongressionalDistrict] (realToFrac . F.rgetField @DVotes) houseElex) ccesWithProbAndCit
    let adjVotes = adjUsing @AchenHurWeight @HouseDVotes @HouseVotes realToFrac round
        res = fmap (F.rcast @CCESByCDR . adjVotes) adjCCESProb
    K.logLE K.Diagnostic $ "Post Achen-Hur (house votes): CCES (by CD) rows per year:"
    K.logLE K.Diagnostic $ show $ fmap (\y -> lengthInYear y res) [2012, 2014, 2016, 2018, 2020]
    return res

--  K.logLE K.Info "CCES Diagnostics (post-stratification of raw turnout * raw pref using ACS weights.)"
--  ccesDiagnosticStatesPost <- fmap fst . K.ignoreCacheTimeM $ ccesDiagnostics clearCache "CompositePost" CCESComposite pumsByCD_C ccesAchenHur_C
--  BR.logFrame ccesDiagnosticStatesPost
  let deps = (,,) <$> ccesHVAchenHur_C <*> cpsV_AchenHur_C <*> pumsByCD_C
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
    ccesWD <- K.knitEither $ addPopDensByDistrict diByCD ccesByCD
    cpsVWD <- K.knitEither $ addPopDensByState diByState cpsVByState
    acsWD <- K.knitEither $ addPopDensByDistrict diByCD acsCDFixed
    return $ CCESAndPUMS ccesWD cpsVWD acsWD diByCD -- (F.toFrame $ fmap F.rcast $ cats)


type CCESWithDensity = CCESByCDR V.++ '[DT.PopPerSqMile]
type CCESWithDensityEM = CCESByCDEMR V.++ '[DT.PopPerSqMile]
addPopDensByDistrict :: forall rs.(FJ.CanLeftJoinM
                                    [BR.Year, BR.StateAbbreviation, BR.CongressionalDistrict]
                                    rs
                                    [BR.Year, BR.StateAbbreviation, BR.CongressionalDistrict, DT.PopPerSqMile]
                                  , [BR.Year, BR.StateAbbreviation, BR.CongressionalDistrict] F.⊆ [BR.Year, BR.StateAbbreviation, BR.CongressionalDistrict, DT.PopPerSqMile]
                                  , F.ElemOf (rs V.++ '[DT.PopPerSqMile]) BR.CongressionalDistrict
                                  , F.ElemOf (rs V.++ '[DT.PopPerSqMile]) BR.StateAbbreviation
                                  , F.ElemOf (rs V.++ '[DT.PopPerSqMile]) BR.Year
                                  )
                     => F.FrameRec DistrictDemDataR -> F.FrameRec rs -> Either Text (F.FrameRec (rs V.++ '[DT.PopPerSqMile]))
addPopDensByDistrict ddd rs = do
  let ddd' = F.rcast @[BR.Year, BR.StateAbbreviation, BR.CongressionalDistrict, DT.PopPerSqMile] <$> ddd
      (joined, missing) = FJ.leftJoinWithMissing
                          @[BR.Year, BR.StateAbbreviation, BR.CongressionalDistrict]
                          @rs
                          @[BR.Year, BR.StateAbbreviation, BR.CongressionalDistrict, DT.PopPerSqMile] rs ddd'
  when (not $ null missing) $ Left $ "missing keys in join of density data by dsitrict: " <> show missing
  Right joined

addPopDensByState :: forall rs.(FJ.CanLeftJoinM
                                    [BR.Year, BR.StateAbbreviation]
                                    rs
                                    [BR.Year, BR.StateAbbreviation, DT.PopPerSqMile]
                                  , [BR.Year, BR.StateAbbreviation] F.⊆ [BR.Year, BR.StateAbbreviation, DT.PopPerSqMile]
                                  , F.ElemOf (rs V.++ '[DT.PopPerSqMile]) BR.StateAbbreviation
                                  , F.ElemOf (rs V.++ '[DT.PopPerSqMile]) BR.Year
                                  )
                     => F.FrameRec StateDemDataR -> F.FrameRec rs -> Either Text (F.FrameRec (rs V.++ '[DT.PopPerSqMile]))
addPopDensByState ddd rs = do
  let ddd' = F.rcast @[BR.Year, BR.StateAbbreviation, DT.PopPerSqMile] <$> ddd
      (joined, missing) = FJ.leftJoinWithMissing
                          @[BR.Year, BR.StateAbbreviation]
                          @rs
                          @[BR.Year, BR.StateAbbreviation, DT.PopPerSqMile] rs ddd'
  when (not $ null missing) $ Left $ "missing keys in join of density data by state: " <> show missing
  Right joined


sumButLeaveDensity :: forall as.(as F.⊆ (as V.++ '[DT.PopPerSqMile])
                                , F.ElemOf (as V.++ '[DT.PopPerSqMile]) DT.PopPerSqMile
                                , FF.ConstrainedFoldable Num as
                                )
                   => FL.Fold (F.Record (as V.++ '[DT.PopPerSqMile])) (F.Record (as V.++ '[DT.PopPerSqMile]))
sumButLeaveDensity =
  let sumF = FL.premap (F.rcast @as) $ FF.foldAllConstrained @Num FL.sum
      densF =  fmap (FT.recordSingleton @DT.PopPerSqMile . fromMaybe 0)
               $ FL.premap (F.rgetField @DT.PopPerSqMile) FL.last
  in (F.<+>) <$> sumF <*> densF


fldAgeInCPS :: FL.Fold (F.Record CPSVWithDensity) (F.FrameRec CPSVWithDensityEM)
fldAgeInCPS = FMR.concatFold
              $ FMR.mapReduceFold
              FMR.noUnpack
              (FMR.assignKeysAndData @(StateKeyR V.++ CensusPredictorEMR))
              (FMR.foldAndAddKey $ sumButLeaveDensity @BRCF.CountCols)

fldAgeInACS :: FL.Fold (F.Record PUMSWithDensity) (F.FrameRec PUMSWithDensityEM)
fldAgeInACS = FMR.concatFold
               $ FMR.mapReduceFold
               FMR.noUnpack
               (FMR.assignKeysAndData @(CDKeyR V.++ CensusPredictorEMR))
               (FMR.foldAndAddKey $ sumButLeaveDensity @'[PUMS.Citizens, PUMS.NonCitizens])


fldAgeInCCES :: FL.Fold (F.Record CCESWithDensity) (F.FrameRec CCESWithDensityEM)
fldAgeInCCES = FMR.concatFold
               $ FMR.mapReduceFold
               FMR.noUnpack
               (FMR.assignKeysAndData @(CDKeyR V.++ CCESPredictorEMR))
               (FMR.foldAndAddKey $ sumButLeaveDensity @CCESVotingDataR)

type PUMSWithDensity = PUMSByCDR V.++ '[DT.PopPerSqMile]
type PUMSWithDensityEM = PUMSByCDEMR V.++ '[DT.PopPerSqMile]

type CPSVWithDensity = CPSVByStateR V.++ '[DT.PopPerSqMile]
type CPSVWithDensityEM = CPSVByStateEMR V.++ '[DT.PopPerSqMile]

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
                -> CCESVoteSource
                -> K.ActionWithCacheTime r (F.FrameRec PUMSByCDR)
                -> K.ActionWithCacheTime r (F.FrameRec CCESByCDR)
                -> K.Sem r (K.ActionWithCacheTime r ((F.FrameRec [BR.Year,BR.StateAbbreviation, CVAP, Voters, DemVoters, Turnout, ET.DemShare]
                                                      , F.FrameRec [BR.Year,BR.StateAbbreviation, ET.CongressionalDistrict, CVAP, Voters, DemVoters, Turnout, ET.DemShare])))
ccesDiagnostics clearCaches cacheSuffix vs acs_C cces_C = K.wrapPrefix "ccesDiagnostics" $ do
  K.logLE K.Info $ "computing CES diagnostics..."
  let surveyed r =  F.rgetField @Surveyed r
      pT r = (realToFrac $ F.rgetField @Voted r)/(realToFrac $ surveyed r)
      pD r = let (v, dv) = getVotes vs in if v r == 0 then 0 else (realToFrac $ dv r)/(realToFrac $ v r)
      cvap = realToFrac . F.rgetField @PUMS.Citizens
      addRace5 r = r F.<+> (FT.recordSingleton @DT.Race5C $ DT.race5FromRaceAlone4AndHisp True (F.rgetField @DT.RaceAlone4C r) (F.rgetField @DT.HispC r))
      compute rw rc = let voters = (pT rw * cvap rc) in (F.rgetField @PUMS.Citizens rc F.&: voters F.&: (pD rw * voters) F.&: V.RNil ) :: F.Record [CVAP, Voters, DemVoters]
      psFld :: FL.Fold (F.Record [CVAP, Voters, DemVoters]) (F.Record [CVAP, Voters, DemVoters])
      psFld = (\cv v dv -> cv F.&: v F.&: dv F.&: V.RNil) <$> cvF <*> vF <*> dvF where
        cvF = fromMaybe 0 <$> FL.premap (F.rgetField @CVAP) FL.last
        vF = FL.premap (F.rgetField @Voters) FL.sum
        dvF = FL.premap (F.rgetField @Voters) FL.sum
      addShare r = let v = F.rgetField @Voters r in r F.<+> (FT.recordSingleton @ET.DemShare $ if v < 1 then 0 else F.rgetField @DemVoters r / v)
      addTurnout r = let v = realToFrac (F.rgetField @CVAP r) in r F.<+> (FT.recordSingleton @Turnout $ if v < 1 then 0 else F.rgetField @Voters r / v)
      deps = (,) <$> acs_C <*> cces_C
--      onlyAlaska_C = F.filterFrame (\r -> F.rgetField @BR.StateAbbreviation r == "AK") <$> cces_C
--  K.ignoreCacheTime onlyAlaska_C >>= BR.logFrame
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

--data DMModel = BaseDM
--             | WithCPSTurnoutDM
--             deriving (Show, Eq, Ord, Generic)

type ElectionWithDemographicsR = ElectionResultWithDemographicsR '[BR.StateAbbreviation]


groupBuilderDM :: forall rs ks tr pr.
                  (F.ElemOf rs BR.StateAbbreviation
                  , F.ElemOf rs Census.Count
                  , Typeable rs
                  , Typeable ks
                  , V.RMap ks
                  , V.ReifyConstraint Show F.ElField ks
                  , V.RecordToList ks
                  , FI.RecVec rs
                  , ks F.⊆ rs
                  , Ord (F.Record ks)
                  )
               => Model tr pr
               -> SB.GroupTypeTag (F.Record ks)
               -> [Text]
               -> [F.Record ks]
               -> SB.StanGroupBuilderM CCESAndCPSEM (F.FrameRec PUMSWithDensityEM, F.FrameRec rs) ()
groupBuilderDM model psGroup states psKeys = do
  let loadCPSTurnoutData :: SB.StanGroupBuilderM CCESAndCPSEM (F.FrameRec PUMSWithDensityEM, F.FrameRec rs) () = do
        cpsData <- SB.addModelDataToGroupBuilder "CPS" (SB.ToFoldable $ F.filterFrame ((/=0) . F.rgetField @BRCF.Count) . cpsVEMRows)
        SB.addGroupIndexForData stateGroup cpsData $ SB.makeIndexFromFoldable show (F.rgetField @BR.StateAbbreviation) states
      loadElexTurnoutData :: SB.StanGroupBuilderM CCESAndCPSEM (F.FrameRec PUMSWithDensityEM, F.FrameRec rs) () = do
        elexTurnoutData <- SB.addModelDataToGroupBuilder "ElectionsT" (SB.ToFoldable electionRows)
        SB.addGroupIndexForData stateGroup elexTurnoutData $ SB.makeIndexFromFoldable show (F.rgetField @BR.StateAbbreviation) states
      loadElexPrefData :: SB.StanGroupBuilderM CCESAndCPSEM (F.FrameRec PUMSWithDensityEM, F.FrameRec rs) () = do
        elexPrefData <- SB.addModelDataToGroupBuilder "ElectionsP" (SB.ToFoldable electionRows)
        SB.addGroupIndexForData stateGroup elexPrefData $ SB.makeIndexFromFoldable show (F.rgetField @BR.StateAbbreviation) states
      loadCCESTurnoutData :: SB.StanGroupBuilderM CCESAndCPSEM (F.FrameRec PUMSWithDensityEM, F.FrameRec rs) () = do
        ccesTurnoutData <- SB.addModelDataToGroupBuilder "CCEST" (SB.ToFoldable ccesEMRows)
        SB.addGroupIndexForData stateGroup ccesTurnoutData $ SB.makeIndexFromFoldable show (F.rgetField @BR.StateAbbreviation) states
      loadCCESPrefData :: CCESVoteSource ->  SB.StanGroupBuilderM CCESAndCPSEM (F.FrameRec PUMSWithDensityEM, F.FrameRec rs) ()
      loadCCESPrefData vs = do
        ccesPrefData <- SB.addModelDataToGroupBuilder "CCESP" (SB.ToFoldable $ F.filterFrame (not . zeroVotes vs) . ccesEMRows)
        SB.addGroupIndexForData stateGroup ccesPrefData $ SB.makeIndexFromFoldable show (F.rgetField @BR.StateAbbreviation) states
      addTurnout :: SB.StanGroupBuilderM CCESAndCPSEM (F.FrameRec PUMSWithDensityEM, F.FrameRec rs) ()
      addTurnout = case turnoutData model of
        T_CCES -> loadCCESTurnoutData
        T_CPS -> loadCPSTurnoutData
        T_Elex _ -> loadElexTurnoutData
        T_CCESAndCPS -> loadCCESTurnoutData >> loadCPSTurnoutData
        T_ElexAndCPS _ -> loadElexTurnoutData >> loadCPSTurnoutData
      addPref :: SB.StanGroupBuilderM CCESAndCPSEM (F.FrameRec PUMSWithDensityEM, F.FrameRec rs) ()
      addPref = case prefData model of
        P_CCES vs -> loadCCESPrefData vs
        P_Elex _ -> loadElexPrefData
        P_ElexAndCCES _ vs -> loadElexPrefData >> loadCCESPrefData vs
  addTurnout
  addPref

  -- GQ
  acsData <- SB.addGQDataToGroupBuilder "ACS" (SB.ToFoldable fst)
  SB.addGroupIndexForData stateGroup acsData $ SB.makeIndexFromFoldable show (F.rgetField @BR.StateAbbreviation) states
  SB.addGroupIndexForData raceGroup acsData $ SB.makeIndexFromEnum race5Census
  SB.addGroupIndexForData educationGroup acsData $ SB.makeIndexFromEnum (F.rgetField @DT.CollegeGradC)
  SB.addGroupIndexForData sexGroup acsData $ SB.makeIndexFromEnum (F.rgetField @DT.SexC)
  psData <- SB.addGQDataToGroupBuilder "DistrictPS" (SB.ToFoldable $ F.filterFrame ((/=0) . F.rgetField @Census.Count) . snd)
  SB.addGroupIndexForData stateGroup psData $ SB.makeIndexFromFoldable show (F.rgetField @BR.StateAbbreviation) states
  SB.addGroupIndexForData psGroup psData $ SB.makeIndexFromFoldable show F.rcast psKeys

designMatrixRow :: forall rs.(F.ElemOf rs DT.CollegeGradC
                             , F.ElemOf rs DT.SexC
                             , F.ElemOf rs DT.Race5C
                             , F.ElemOf rs DT.HispC
                             , F.ElemOf rs DT.PopPerSqMile
                             )
                => DM.DesignMatrixRow (F.Record rs)
designMatrixRow = DM.DesignMatrixRow "DM" $ [sexRP, eduRP, raceRP, densRP, wngRP]
  where
    sexRP = DM.boundedEnumRowPart "Sex" (F.rgetField @DT.SexC)
    eduRP = DM.boundedEnumRowPart "Education" (F.rgetField @DT.CollegeGradC)
    raceRP = DM.boundedEnumRowPart "Race" mergeRace5AndHispanic
    densRP = DM.DesignMatrixRowPart "Density" 1 logDensityPredictor
    wngRP = DM.boundedEnumRowPart "WhiteNonGrad" wnhNonGradCCES

designMatrixRowCCES :: DM.DesignMatrixRow (F.Record CCESWithDensityEM)
designMatrixRowCCES = designMatrixRow

designMatrixRowCPS :: DM.DesignMatrixRow (F.Record CPSVWithDensityEM)
designMatrixRowCPS = DM.DesignMatrixRow "DM" $ [sexRP, eduRP, raceRP, densRP, wngRP]
 where
    sexRP = DM.boundedEnumRowPart "Sex" (F.rgetField @DT.SexC)
    eduRP = DM.boundedEnumRowPart "Education" (F.rgetField @DT.CollegeGradC)
    raceRP = DM.boundedEnumRowPart "Race" race5Census
    densRP = DM.DesignMatrixRowPart "Density" 1 logDensityPredictor
    wngRP = DM.boundedEnumRowPart "WhiteNonGrad" wnhNonGradCensus


designMatrixRowACS :: DM.DesignMatrixRow (F.Record PUMSWithDensityEM)
designMatrixRowACS = DM.DesignMatrixRow "DM" $ [sexRP, eduRP, raceRP, densRP, wngRP]
  where
    sexRP = DM.boundedEnumRowPart "Sex" (F.rgetField @DT.SexC)
    eduRP = DM.boundedEnumRowPart "Education" (F.rgetField @DT.CollegeGradC)
    raceRP = DM.boundedEnumRowPart "Race" race5Census
    densRP = DM.DesignMatrixRowPart "Density" 1 logDensityPredictor
    wngRP = DM.boundedEnumRowPart "WhiteNonGrad" wnhNonGradCensus

designMatrixRowElex :: Bool -> DM.DesignMatrixRow (F.Record ElectionWithDemographicsR)
designMatrixRowElex stateAndDensityOnly = DM.DesignMatrixRow "DM"
                                          $ if stateAndDensityOnly
                                            then [zeroDMRP "Sex" 1, zeroDMRP "Education" 1, zeroDMRP "Race" 5, densRP, zeroDMRP "WhiteNonGrad" 1]
                                            else [sexRP, eduRP, raceRP, densRP, wngRP]
  where
    zeroDMRP t n = DM.DesignMatrixRowPart t n (const $ VU.replicate n 0.0)
    fracWhite r = F.rgetField @FracWhiteNonHispanic r + F.rgetField @FracWhiteHispanic r
    fracHispanic r =  F.rgetField @FracWhiteHispanic r + F.rgetField @FracNonWhiteHispanic r
    fracWhiteNonGrad r = fracWhite r - F.rgetField @FracWhiteGrad r
    sexRP = DM.rowPartFromFunctions "Sex" [\r ->  1 - (2 * F.rgetField @FracFemale r)] -- Female is -1 since 1st in enum
    eduRP = DM.rowPartFromFunctions "Education" [\r ->  (2 * F.rgetField @FracGrad r) - 1] -- Grad is +1 since 2nd in enum
    raceRP = DM.rowPartFromFunctions "Race" [F.rgetField @FracOther
                                            , F.rgetField @FracBlack
                                            , fracHispanic
                                            , F.rgetField @FracAsian
                                            , F.rgetField @FracWhiteNonHispanic
                                            ]
    densRP = DM.DesignMatrixRowPart "Density" 1 logDensityPredictor
    wngRP = DM.rowPartFromFunctions "WhiteNonGrad" [\r ->  (2 * fracWhiteNonGrad r) - 1] -- white non-grad is 1 since True in Bool, thus 2nd in enum

race5Census r = DT.race5FromRaceAlone4AndHisp True (F.rgetField @DT.RaceAlone4C r) (F.rgetField @DT.HispC r)
wnhCensus r = F.rgetField @DT.RaceAlone4C r == DT.RA4_White && F.rgetField @DT.HispC r == DT.NonHispanic
wnhNonGradCensus r = wnhCensus r && F.rgetField @DT.CollegeGradC r == DT.NonGrad

data TurnoutDataSet r where
   T_CCES :: TurnoutDataSet (F.Record CCESWithDensityEM)
   T_CPS :: TurnoutDataSet (F.Record CPSVWithDensityEM)
   T_Elex :: Int -> TurnoutDataSet (F.Record ElectionWithDemographicsR)
   T_CCESAndCPS :: TurnoutDataSet (Either (F.Record CCESWithDensityEM) (F.Record CPSVWithDensityEM))
   T_ElexAndCPS :: Int -> TurnoutDataSet (Either (F.Record ElectionWithDemographicsR)  (F.Record CPSVWithDensityEM))


turnoutDataSetModelText :: TurnoutDataSet x -> Text
turnoutDataSetModelText T_CCES = "CCES"
turnoutDataSetModelText T_CPS = "CPS"
turnoutDataSetModelText (T_Elex _) = "Elex"
turnoutDataSetModelText T_CCESAndCPS = "CCESAndCPS"
turnoutDataSetModelText (T_ElexAndCPS _) = "ElexAndCPS"

turnoutDataSetDataText :: TurnoutDataSet x -> Text
turnoutDataSetDataText T_CCES = "CCES"
turnoutDataSetDataText T_CPS = "CPS"
turnoutDataSetDataText (T_Elex n) = "ElexBy" <> show n
turnoutDataSetDataText T_CCESAndCPS = "CCESAndCPS"
turnoutDataSetDataText (T_ElexAndCPS n) = "ElexBy" <> show n <> "AndCPS"

data PrefDataSet r where
  P_CCES :: CCESVoteSource -> PrefDataSet (F.Record CCESWithDensityEM)
  P_Elex :: Int -> PrefDataSet (F.Record ElectionWithDemographicsR)
  P_ElexAndCCES :: Int -> CCESVoteSource -> PrefDataSet (Either (F.Record ElectionWithDemographicsR) (F.Record CCESWithDensityEM))

prefDataSetModelText :: PrefDataSet r -> Text
prefDataSetModelText (P_CCES vs) = "CCES_" <> printVoteSource vs
prefDataSetModelText (P_Elex _) = "Elex"
prefDataSetModelText (P_ElexAndCCES _ vs) = "ElexAndCCES_" <> printVoteSource vs

prefDataSetDataText :: PrefDataSet r -> Text
prefDataSetDataText (P_CCES vs) = "CCES_" <> printVoteSource vs
prefDataSetDataText (P_Elex n) = "ElexBy" <> show n
prefDataSetDataText (P_ElexAndCCES n vs) = "ElexBy" <> show n <> "AndCCES_" <> printVoteSource vs

predictorFunctions :: SB.RowTypeTag r
                   -> DM.DesignMatrixRow r
                   -> Maybe Text
                   -> SB.IndexKey
                   -> SB.StanVar
                   -> SB.StanBuilderM md gq (SB.StanVar -> SB.StanVar -> SB.StanVar -> SB.StanExpr
                                            , SB.StanVar -> SB.StanVar -> SB.StanVar -> SB.StanExpr
                                            )
predictorFunctions rtt dmr suffixM dmColIndex dsIndexV = do
  let dmBetaE dmE betaE = SB.vectorizedOne dmColIndex $ SB.function "dot_product" (dmE :| [betaE])
      predE aE dmE betaE = aE `SB.plus` dmBetaE dmE betaE
      pred a dm beta = predE (SB.var a) (SB.var dm) (SB.var beta)
      suffix = fromMaybe "" suffixM
      iPredF :: SB.StanBuilderM md gq (SB.StanExpr -> SB.StanExpr -> SB.StanExpr -> SB.StanExpr)
      iPredF = SB.useDataSetForBindings rtt $ do
        dsAlpha <- SB.addSimpleParameter ("dsAplha" <> suffix) SB.StanReal SB.stdNormal
        dsPhi <- SB.addParameterWithVectorizedPrior ("dsPhi" <> suffix) (SB.StanVector $ SB.NamedDim dmColIndex) SB.stdNormal dmColIndex
        SB.inBlock SB.SBGeneratedQuantities $ SB.useDataSetForBindings rtt $ DM.splitToGroupVars dmr dsPhi
        return $ \aE dmE betaE -> predE (aE `SB.plus` SB.paren (SB.var dsAlpha `SB.times` SB.var dsIndexV))
                                         dmE
                                         (betaE `SB.plus` SB.paren (SB.var dsPhi `SB.times` SB.var dsIndexV))
  iPredE <- iPredF
  let iPred a dm beta = iPredE (SB.var a) (SB.var dm) (SB.var beta)
  return (pred, iPred)

setupCCESTData :: (Typeable md, Typeable gq)
                   => Int
                   -> SB.StanBuilderM md gq (SB.RowTypeTag (F.Record CCESWithDensityEM)
                                            , DM.DesignMatrixRow (F.Record CCESWithDensityEM)
                                            , SB.StanVar, SB.StanVar, SB.StanVar, SB.StanVar)
setupCCESTData n = do
  ccesTData <- SB.dataSetTag @(F.Record CCESWithDensityEM) SC.ModelData "CCEST"
  ccesTIndex <- SB.indexedConstIntArray ccesTData Nothing n
  dmCCES <- DM.addDesignMatrix ccesTData designMatrixRowCCES
  cvapCCES <- SB.addCountData ccesTData "CVAP_CCES" (F.rgetField @Surveyed)
  votedCCES <- SB.addCountData ccesTData "Voted_CCES" (F.rgetField @Voted)
  return (ccesTData, designMatrixRowCCES, cvapCCES, votedCCES, dmCCES, ccesTIndex)


setupCPSData ::  (Typeable md, Typeable gq)
             => Int
             -> SB.StanBuilderM md gq (SB.RowTypeTag (F.Record CPSVWithDensityEM)
                                      , DM.DesignMatrixRow (F.Record CPSVWithDensityEM)
                                      , SB.StanVar, SB.StanVar, SB.StanVar, SB.StanVar)
setupCPSData n = do
  cpsData <- SB.dataSetTag @(F.Record CPSVWithDensityEM) SC.ModelData "CPS"
  cpsIndex <- SB.indexedConstIntArray cpsData Nothing n
  cvapCPS <- SB.addCountData cpsData "CVAP_CPS" (F.rgetField @BRCF.Count)
  votedCPS <- SB.addCountData cpsData "Voted_CPS" (F.rgetField @BRCF.Successes)
  dmCPS <- DM.addDesignMatrix cpsData designMatrixRowCPS
  return (cpsData, designMatrixRowCPS, cvapCPS, votedCPS, dmCPS, cpsIndex)


setupElexTData :: (Typeable md, Typeable gq)
               => Bool
               -> Int
               -> Int
               -> SB.StanBuilderM md gq (SB.RowTypeTag (F.Record ElectionWithDemographicsR)
                                        , DM.DesignMatrixRow (F.Record ElectionWithDemographicsR)
                                        , SB.StanVar, SB.StanVar, SB.StanVar, SB.StanVar)
setupElexTData stateAndDensityOnly voterScale dsIndex = do
  let scaleVoters x = x `div` voterScale
  elexTData <- SB.dataSetTag @(F.Record ElectionWithDemographicsR) SC.ModelData "ElectionsT"
  elexTIndex <- SB.indexedConstIntArray elexTData Nothing dsIndex
  cvapElex <- SB.addCountData elexTData "CVAP_Elex" (scaleVoters . F.rgetField @PUMS.Citizens)
  votedElex <- SB.addCountData elexTData "Voted_Elex" (scaleVoters . F.rgetField @TVotes)
  dmElexT <- DM.addDesignMatrix elexTData $ designMatrixRowElex stateAndDensityOnly
  return (elexTData, designMatrixRowElex stateAndDensityOnly, cvapElex, votedElex, dmElexT, elexTIndex)


setupTurnoutData :: forall x md gq. (Typeable md, Typeable gq)
                 => TurnoutDataSet x
                 -> SB.StanBuilderM md gq (SB.RowTypeTag x, DM.DesignMatrixRow x, SB.StanVar, SB.StanVar, SB.StanVar, SB.StanVar)
setupTurnoutData td = do
  let groupSet =  SB.addGroupToSet stateGroup SB.emptyGroupSet
  case td of
    T_CCES -> setupCCESTData 0
    T_CPS -> setupCPSData 0
    T_Elex n -> setupElexTData False n 0
    T_CCESAndCPS -> do
      (ccesData, ccesDMR, ccesCVAP, ccesVoted, ccesDM, ccesIndex) <- setupCCESTData (-1)
      (cpsData, cpsDMR, cpsCVAP, cpsVoted, cpsDM, cpsIndex) <- setupCPSData 1
      (comboData, stackVarsF) <- SB.stackDataSets "comboT" ccesData cpsData groupSet
      comboDMR <- SB.stanBuildEither $ DM.stackDesignMatrixRows ccesDMR cpsDMR
      (cvap, voted, dmCombo, indexCombo) <-  SB.inBlock SB.SBTransformedData $ do
        cvapCombo' <- stackVarsF "CVAP" ccesCVAP cpsCVAP
        votedCombo' <- stackVarsF "Voted" ccesVoted cpsVoted
        dmCombo' <- stackVarsF "DMTurnout" ccesDM cpsDM
        indexCombo' <- stackVarsF "DataSetIndexT" ccesIndex cpsIndex
        return (cvapCombo', votedCombo', dmCombo', indexCombo')
      return (comboData, comboDMR, cvap, voted, dmCombo, indexCombo)
    T_ElexAndCPS n -> do
      (elexData, elexDMR, elexCVAP, elexVoted, elexDM, elexIndex) <- setupElexTData True n 0
      (cpsData, cpsDMR, cpsCVAP, cpsVoted, cpsDM, cpsIndex) <- setupCPSData 1
      (comboData, stackVarsF) <- SB.stackDataSets "comboT" elexData cpsData groupSet
      comboDMR <- SB.stanBuildEither $ DM.stackDesignMatrixRows elexDMR cpsDMR
      (cvap, voted, dmCombo, indexCombo) <-  SB.inBlock SB.SBTransformedData $ do
        cvapCombo' <- stackVarsF "CVAP" elexCVAP cpsCVAP
        votedCombo' <- stackVarsF "Voted" elexVoted cpsVoted
        dmCombo' <- stackVarsF "DMTurnout" elexDM cpsDM
        indexCombo' <- stackVarsF "DataSetIndexT" elexIndex cpsIndex
        return (cvapCombo', votedCombo', dmCombo', indexCombo')
      return (comboData, comboDMR, cvap, voted, dmCombo, indexCombo)

setupCCESPData :: (Typeable md, Typeable gq)
               => Int
               -> CCESVoteSource
               -> SB.StanBuilderM md gq (SB.RowTypeTag (F.Record CCESWithDensityEM)
                                        , DM.DesignMatrixRow (F.Record CCESWithDensityEM)
                                        , SB.StanVar, SB.StanVar, SB.StanVar, SB.StanVar)
setupCCESPData n vs = do
  let (votesF, dVotesF) = getVotes vs
  ccesPData <- SB.dataSetTag @(F.Record CCESWithDensityEM) SC.ModelData "CCESP"
  ccesPIndex <- SB.indexedConstIntArray ccesPData (Just "P") n
  dmCCESP <- DM.addDesignMatrix ccesPData designMatrixRowCCES
  raceVotesCCES <- SB.addCountData ccesPData "VotesInRace_CCES" votesF
  dVotesInRaceCCES <- SB.addCountData ccesPData "DVotesInRace_CCES" dVotesF
  return (ccesPData, designMatrixRowCCES, raceVotesCCES, dVotesInRaceCCES, dmCCESP, ccesPIndex)

setupElexPData :: (Typeable md, Typeable gq)
               => Bool
               -> Int
               -> Int
               -> SB.StanBuilderM md gq (SB.RowTypeTag (F.Record ElectionWithDemographicsR)
                                        , DM.DesignMatrixRow (F.Record ElectionWithDemographicsR)
                                        , SB.StanVar, SB.StanVar, SB.StanVar, SB.StanVar)
setupElexPData stateAndDensityOnly voterScale dsIndex = do
  let scale x = x `div` voterScale
  elexPData <- SB.dataSetTag @(F.Record ElectionWithDemographicsR) SC.ModelData "ElectionsP"
  elexPIndex <- SB.indexedConstIntArray elexPData Nothing dsIndex
  dmElexP <- DM.addDesignMatrix elexPData $ designMatrixRowElex stateAndDensityOnly
  raceVotesElex <- SB.addCountData elexPData "VotesInRace_Elections" (scale . F.rgetField @TVotes)
  dVotesInRaceElex <- SB.addCountData elexPData "DVotesInRace_Elections" (scale . F.rgetField @DVotes)
  return (elexPData, designMatrixRowElex stateAndDensityOnly, raceVotesElex, dVotesInRaceElex, dmElexP, elexPIndex)

setupPrefData :: forall x md gq. (Typeable md, Typeable gq)
              => PrefDataSet x
              -> SB.StanBuilderM md gq (SB.RowTypeTag x, DM.DesignMatrixRow x, SB.StanVar, SB.StanVar, SB.StanVar, SB.StanVar)
setupPrefData pd = do
  let groupSet =  SB.addGroupToSet stateGroup SB.emptyGroupSet
  case pd of
    P_CCES vs -> setupCCESPData 0 vs
    P_Elex n -> setupElexPData False n 0
    P_ElexAndCCES n vs -> do
      (elexData, elexDMR, elexVIR, elexDVIR, elexDM, elexIndex) <- setupElexPData True n 0
      (ccesData, ccesDMR, ccesVIR, ccesDVIR, ccesDM, ccesIndex) <- setupCCESPData 1 vs
      (comboData, stackVarsF) <- SB.stackDataSets "comboP" elexData ccesData groupSet
      comboDMR <- SB.stanBuildEither $ DM.stackDesignMatrixRows elexDMR ccesDMR
      (votesInRace, dVotesInRace, dmComboP, indexCombo) <-  SB.inBlock SB.SBTransformedData $ do
        virCombo <- stackVarsF "VotesInRace" elexVIR ccesVIR
        dvirCombo <- stackVarsF "DVotesInRace" elexDVIR ccesDVIR
        dmCombo <- stackVarsF "DMPref" elexDM ccesDM
        indexCombo' <- stackVarsF "DataSetIndexP" elexIndex ccesIndex
        return (virCombo, dvirCombo, dmCombo, indexCombo')
      return (comboData, comboDMR, votesInRace, dVotesInRace, dmComboP, indexCombo)

electionModelDM :: forall rs ks r tr pr.
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
                   , FI.RecVec rs
                   ,  (ks V.++ '[ModelDesc, ModeledShare]) F.⊆  (BR.Year : ModelDesc : (ks V.++ '[ModeledShare]))
                   , FI.RecVec (ks V.++ '[ModeledShare])
                   , FI.RecVec  (ks V.++ '[ModelDesc, ModeledShare])
                   , ks F.⊆ rs
                   , V.RMap (ks V.++ '[ModelDesc, ModeledShare])
                   , Show (F.Record ks)
                   , Typeable rs
                   , Typeable ks
                   , Ord (F.Record ks)
                   , Flat.GFlatDecode
                     (Flat.Rep
                      (F.Rec FS.SElField (ks V.++ '[ModelDesc, ModeledShare])))
                   , Flat.GFlatEncode
                     (Flat.Rep
                      (F.Rec FS.SElField (ks V.++ '[ModelDesc, ModeledShare])))
                   , Flat.GFlatSize
                     (Flat.Rep
                       (F.Rec FS.SElField (ks V.++ '[ModelDesc, ModeledShare])))
                   , Generic
                     (F.Rec FS.SElField (ks V.++ '[ModelDesc, ModeledShare]))
                 )
                => Bool
                -> Bool
                -> BR.StanParallel
                -> Maybe SC.StanMCParameters
                -> Text
                -> Model tr pr
                -> Int
                -> (SB.GroupTypeTag (F.Record ks), Text, SB.GroupSet)
                -> K.ActionWithCacheTime r CCESAndCPSEM
                -> K.ActionWithCacheTime r (F.FrameRec PUMSWithDensityEM, F.FrameRec rs)
                -> K.Sem r (K.ActionWithCacheTime r (ModelCrossTabs, F.FrameRec (ModelResultsR ks)))
electionModelDM clearCaches parallel stanParallelCfg mStanParams modelDir model datYear (psGroup, psDataSetName, psGroupSet) dat_C psDat_C = K.wrapPrefix "stateLegModel" $ do
  K.logLE K.Info $ "(Re-)running DM turnout/pref model if necessary."
  x <- K.ignoreCacheTime $ fmap ccesEMRows dat_C
  let turnoutDataSet = turnoutData model
      prefDataSet = prefData model
      reportZeroRows :: K.Sem r ()
      reportZeroRows = do
        case prefDataSet of
          P_CCES vs -> do
            let numZeroVoteRows = countZeroVoteRows vs x
            K.logLE K.Diagnostic $ "CCES data has " <> show numZeroVoteRows <> " rows with no votes (for preference purposes)."
          P_Elex _ -> pure ()
          P_ElexAndCCES _ vs -> do
            let numZeroVoteRows = countZeroVoteRows vs x
            K.logLE K.Diagnostic $ "CCES data has " <> show numZeroVoteRows <> " rows with no votes (for preference purposes)."
  reportZeroRows
  let modelName = "LegDistricts_" <> modelLabel model <> if parallel then "_P" else ""
      jsonDataName = "DM_" <> dataLabel model
                     <> "_" <> show datYear <> if parallel then "_P" else ""  -- because of grainsize
      dataAndCodeBuilder :: MRP.BuilderM CCESAndCPSEM (F.FrameRec PUMSWithDensityEM, F.FrameRec rs) ()
      dataAndCodeBuilder = do

        (turnoutData, turnoutDesignMatrixRow, cvap, voted, dmTurnout, dsIndexT) <- setupTurnoutData turnoutDataSet
        DM.addDesignMatrixIndexes turnoutData turnoutDesignMatrixRow

        let distT = SB.binomialLogitDistWithConstants cvap

        (prefData, prefDesignMatrixRow, votesInRace, dVotesInRace, dmPref, dsIndexP) <- setupPrefData prefDataSet
        let distP = SB.binomialLogitDistWithConstants votesInRace

        (dmT, centerTF) <- DM.centerDataMatrix dmTurnout Nothing
        (dmP, centerPF) <- DM.centerDataMatrix dmPref Nothing

        (alphaT, thetaT, muT, tauT, lT) <-
          DM.addDMParametersAndPriors "DM" stateGroup "beta" DM.NonCentered (SB.stdNormal, SB.stdNormal, 4) (Just "T")
        (alphaP, thetaP, muP, tauP, lP) <-
          DM.addDMParametersAndPriors "DM" stateGroup "beta" DM.NonCentered (SB.stdNormal, SB.stdNormal, 4) (Just "P")

        DM.addDesignMatrixIndexes turnoutData turnoutDesignMatrixRow
        DM.addDesignMatrixIndexes prefData prefDesignMatrixRow
        SB.inBlock SB.SBGeneratedQuantities $ do
          SB.useDataSetForBindings turnoutData $ DM.splitToGroupVars turnoutDesignMatrixRow muT
          SB.useDataSetForBindings prefData $ DM.splitToGroupVars prefDesignMatrixRow muP

        let (SB.StanVar _ (SB.StanMatrix (_, SB.NamedDim colIndexT))) = dmT
        let (SB.StanVar _ (SB.StanMatrix (_, SB.NamedDim colIndexP))) = dmP

        (predT, iPredT') <- predictorFunctions turnoutData turnoutDesignMatrixRow (Just "T") "DM_Cols" dsIndexT
        (predP, iPredP') <- predictorFunctions prefData prefDesignMatrixRow (Just "P") "DM_Cols" dsIndexP

        let iPredT :: SB.StanVar -> SB.StanVar -> SB.StanVar -> SB.StanExpr
            iPredT = case turnoutDataSet of
                       T_CCES -> predT
                       T_CPS -> predT
                       T_Elex _ -> predT
                       T_CCESAndCPS -> iPredT'
                       T_ElexAndCPS _ -> iPredT'

            iPredP :: SB.StanVar -> SB.StanVar -> SB.StanVar -> SB.StanExpr
            iPredP = case prefDataSet of
                        P_CCES _ -> predP
                        P_Elex _ -> predP
                        P_ElexAndCCES _ _ -> iPredP'

            vecT = SB.vectorizeExpr "voteDataBetaT" (iPredT alphaT dmT thetaT) (SB.dataSetName turnoutData)
            vecP = SB.vectorizeExpr "voteDataBetaP" (iPredP alphaP dmP thetaP) (SB.dataSetName prefData)
            timesElt = SB.binOp ".*"
            toVec intV = SB.function "to_vector" $ SB.var intV :| []

        SB.inBlock SB.SBModel $ do
          SB.useDataSetForBindings turnoutData $ do
            voteDataBetaT_v <- vecT
            SB.sampleDistV turnoutData distT (SB.var voteDataBetaT_v) voted
          SB.useDataSetForBindings prefData $ do
            voteDataBetaP_v <- vecP
            SB.sampleDistV prefData distP (SB.var voteDataBetaP_v) dVotesInRace

        let llSet :: SB.LLSet CCESAndCPSEM (F.FrameRec PUMSWithDensityEM, F.FrameRec rs) SB.StanExpr =
              SB.addToLLSet turnoutData (SB.LLDetails distT (pure $ iPredT alphaT dmT thetaT) voted)
              $ SB.addToLLSet prefData (SB.LLDetails distP (pure $ iPredP alphaP dmP thetaP) dVotesInRace)
              $ SB.emptyLLSet
        SB.generateLogLikelihood' llSet

        -- for posterior predictive checks
        let ppVoted = SB.StanVar "PVoted" (SB.StanVector $ SB.NamedDim $ SB.dataSetName turnoutData)
        SB.useDataSetForBindings turnoutData
          $ SB.generatePosteriorPrediction turnoutData ppVoted distT $ iPredT alphaT dmT thetaT

        let ppDVotes = SB.StanVar "PDVotesInRace" (SB.StanVector $ SB.NamedDim $ SB.dataSetName prefData)
        SB.useDataSetForBindings prefData
          $ SB.generatePosteriorPrediction prefData ppDVotes distP $ iPredP alphaP dmP thetaP

        -- post-stratification for overall checks
        acsData <- SB.dataSetTag @(F.Record PUMSWithDensityEM) SC.GQData "ACS"
        dmACS' <- DM.addDesignMatrix acsData designMatrixRowACS
        (dmACS_T, dmACS_P) <- SB.useDataSetForBindings acsData $ do
          dmACS_T' <- centerTF SC.GQData dmACS' (Just "T")
          dmACS_P' <- centerPF SC.GQData dmACS' (Just "P")
          return (dmACS_T', dmACS_P')
        let psTPrecompute = SB.vectorizeExpr "acsBetaT" (predT alphaT dmACS_T thetaT) (SB.dataSetName acsData)
            psTExpr :: SB.StanVar -> SB.StanBuilderM md gq SB.StanExpr
            psTExpr =  pure . SB.familyExp distT . SB.var
            psPPrecompute = SB.vectorizeExpr "acsBetaP" (predP alphaP dmACS_P thetaP) (SB.dataSetName acsData)
            psPExpr :: SB.StanVar -> SB.StanBuilderM md gq SB.StanExpr
            psPExpr =  pure . SB.familyExp distP . SB.var
            turnoutPS = (psTPrecompute, psTExpr)
            prefPS = (psPPrecompute, psPExpr)
            psACS :: (Typeable md, Typeable gq, Ord k)
                  => Text -> (SB.StanBuilderM md gq x, x -> SB.StanBuilderM md gq SB.StanExpr) -> SB.GroupTypeTag k -> SB.StanBuilderM md gq SB.StanVar
            psACS name psCalcs grp =
              MRP.addPostStratification
              psCalcs
              (Just name)
              turnoutData
              acsData
              (realToFrac . F.rgetField @PUMS.Citizens)
              (MRP.PSShare Nothing)
              (Just grp)
        psACS "Turnout" turnoutPS raceGroup
        psACS "Turnout" turnoutPS educationGroup
        psACS "Turnout" turnoutPS sexGroup
        psACS "Turnout" turnoutPS stateGroup
        psACS "Pref" prefPS raceGroup
        psACS "Pref" prefPS educationGroup
        psACS "Pref" prefPS sexGroup
        psACS "Pref" prefPS stateGroup

        -- post-stratification for results
        psData <- SB.dataSetTag @(F.Record rs) SC.GQData "DistrictPS"
        dmPS' <- DM.addDesignMatrix psData designMatrixRow

        let psPreCompute = do
              dmPS_T <- centerTF SC.GQData dmPS' (Just "T")
              dmPS_P <- centerPF SC.GQData dmPS' (Just "P")
              psT_v <- SB.vectorizeExpr "psBetaT" (predT alphaT dmPS_T thetaT) (SB.dataSetName psData)
              psP_v <- SB.vectorizeExpr "psBetaP" (predP alphaP dmPS_P thetaP) (SB.dataSetName psData)
              pure (psT_v, psP_v)

            psExprF (psT_v, psP_v) = do
              pT <- SB.stanDeclareRHS "pT" SB.StanReal "" $ SB.familyExp distT (SB.var psT_v)
              pD <- SB.stanDeclareRHS "pD" SB.StanReal "" $ SB.familyExp distP (SB.var psP_v)
              pure $ SB.var pT `SB.times` SB.var pD

        let postStrat =
              MRP.addPostStratification -- @(CCESAndPUMS, F.FrameRec rs)
              (psPreCompute, psExprF)
              Nothing
              turnoutData
              psData
              (realToFrac . F.rgetField @Census.Count)
              (MRP.PSShare $ Just $ SB.name "pT")
              (Just psGroup)
        postStrat
        pure ()

      addModelIdAndYear :: F.Record (ks V.++ '[ModeledShare])
                        -> F.Record (ModelResultsR ks)
      addModelIdAndYear r = F.rcast $ FT.recordSingleton @BR.Year datYear F.<+> FT.recordSingleton @ModelDesc (modelLabel model) F.<+> r
      extractResults :: K.KnitEffects r
                     => SC.ResultAction r md gq SB.DataSetGroupIntMaps () (ModelCrossTabs, FS.SFrameRec (ModelResultsR ks))
      extractResults = SC.UseSummary f where
        f summary _ modelDataAndIndex_C mGQDataAndIndex_C = do
          gqIndexes_C <- K.knitMaybe "StanMRP.extractResults: gqDataAndIndex is Nothing" $ mGQDataAndIndex_C
          gqIndexesE <- K.ignoreCacheTime $ fmap snd gqIndexes_C
          let resultsMap :: (Typeable k, Show k, Ord k) => SB.RowTypeTag x -> SB.GroupTypeTag k -> Text -> K.Sem r (Map k [Double])
              resultsMap rtt gtt psPrefix = K.knitEither $ do
                gqIndexes <- gqIndexesE
                psIndexIM <- SB.getGroupIndex rtt gtt gqIndexes
                let parseAndIndexPctsWith idx g vn = do
                      v <- SP.getVector . fmap CS.percents <$> SP.parse1D vn (CS.paramStats summary)
                      indexStanResults idx $ Vector.map g v
                parseAndIndexPctsWith psIndexIM id $ psPrefix <> SB.taggedGroupName gtt
          modelResultsMap <- resultsMap (SB.RowTypeTag @(F.Record rs) SC.GQData "DistrictPS")  psGroup "PS_DistrictPS_"
          modelResultsFrame :: F.FrameRec (ks V.++ '[ModeledShare]) <- K.knitEither
                                                                       $ MT.keyedCIsToFrame @ModeledShare id
                                                                       $ M.toList modelResultsMap
          let acsRowTag = SB.RowTypeTag @(F.Record PUMSWithDensityEM) SC.GQData "ACS"
              rmByGroup :: forall k kc.(Typeable k, Show k, Ord k, V.Snd kc ~ k, V.KnownField kc, FL.Vector (FI.VectorFor k) k)
                        => SB.GroupTypeTag k -> K.Sem r (CrossTabFrame kc)
              rmByGroup g = do
                turnoutRM <- resultsMap acsRowTag g "Turnout_ACS_"
                prefRM <- resultsMap acsRowTag  g "Pref_ACS_"
                let rm = M.merge M.dropMissing M.dropMissing (M.zipWithMatched $ \_ x y -> (x,y)) turnoutRM prefRM
                    g (k, (t, p)) = do
                      tCI <- MT.listToCI t
                      pCI <- MT.listToCI p
                      return $ datYear F.&: k F.&: dataLabel model F.&: tCI F.&: pCI F.&: V.RNil
                K.knitEither $ F.toFrame <$> (traverse g $ M.toList rm)
          ctBySex <- rmByGroup sexGroup
          ctByEducation <- rmByGroup educationGroup
          ctByRace <- rmByGroup raceGroup
          ctByState <- rmByGroup stateGroup
          let mct = ModelCrossTabs ctBySex ctByEducation ctByRace ctByState
          return $ (mct, FS.SFrame (fmap addModelIdAndYear modelResultsFrame))
      dataWranglerAndCode :: K.ActionWithCacheTime r CCESAndCPSEM
                          -> K.ActionWithCacheTime r (F.FrameRec PUMSWithDensityEM, F.FrameRec rs)
                          -> K.Sem r (SC.DataWrangler CCESAndCPSEM (F.FrameRec PUMSWithDensityEM, F.FrameRec rs) SB.DataSetGroupIntMaps (), SB.StanCode)
      dataWranglerAndCode modelData_C gqData_C = do
        modelData <-  K.ignoreCacheTime modelData_C
        gqData <-  K.ignoreCacheTime gqData_C
        K.logLE K.Info
          $ "CCES has "
          <> show (FL.fold FL.length $ ccesEMRows modelData)
          <> " rows."
        K.logLE K.Info
          $ "CPSV "
          <> show (FL.fold FL.length $ cpsVEMRows modelData)
          <> " rows."
        let states = FL.fold (FL.premap (F.rgetField @BR.StateAbbreviation) FL.list) (ccesEMRows modelData)
            psKeys = FL.fold (FL.premap F.rcast FL.list) (snd gqData)
            groups = groupBuilderDM model psGroup states psKeys
        K.logLE K.Info $ show $ zip [1..] $ Set.toList $ FL.fold FL.set states
        MRP.buildDataWranglerAndCode @BR.SerializerC @BR.CacheData groups dataAndCodeBuilder modelData_C gqData_C
  let unwrapVoted :: SR.UnwrapJSON = case turnoutDataSet of
        T_CCES -> SR.UnwrapNamed "Voted_CCES" "ObsVoted"
        T_CPS -> SR.UnwrapNamed "Voted_CPS" "ObsVoted"
        T_Elex _ -> SR.UnwrapNamed "Voted_Elex" "ObsVoted"
        T_CCESAndCPS -> SR.UnwrapExpr "c(jsonData$Voted_CCES, jsonData$Voted_CPS)" "ObsVoted"
        T_ElexAndCPS _ -> SR.UnwrapExpr "c(jsonData$Voted_Elex, jsonData$Voted_CPS)" "ObsVoted"
      unwrapDVotes :: SR.UnwrapJSON = case prefDataSet of
        P_CCES _ -> SR.UnwrapNamed "DVotesInRace_CCES" "ObsDVotes"
        P_Elex _ -> SR.UnwrapNamed "DVotesInRace_Elections" "ObsDVotes"
        P_ElexAndCCES _ _ -> SR.UnwrapExpr "c(jsonData$DVotesInRace_Elections, jsonData$DVotesInRace_CCES)" "ObsDVotes"
  (dw, stanCode) <- dataWranglerAndCode dat_C psDat_C
  fmap (secondF FS.unSFrame)
    $ MRP.runMRPModel
    clearCaches
    (SC.RunnerInputNames modelDir modelName (Just psDataSetName) jsonDataName)
    (fromMaybe (SC.StanMCParameters 4 4 (Just 1000) (Just 1000) (Just 0.9) (Just 15) Nothing) mStanParams)
    stanParallelCfg
    dw
    stanCode
    (MRP.Both [unwrapVoted, unwrapDVotes])
    extractResults
    dat_C
    psDat_C

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

data CCESVoteSource = CCESHouse | CCESPres | CCESComposite deriving (Show, Eq, Ord, Generic)
instance Flat.Flat CCESVoteSource
type instance FI.VectorFor CCESVoteSource = Vector.Vector

printVoteSource :: CCESVoteSource -> Text
printVoteSource CCESHouse = "HouseVotes"
printVoteSource CCESPres = "PresVotes"
printVoteSource CCESComposite = "CompositeVotes"

getVotes :: (F.ElemOf rs HouseVotes
            , F.ElemOf rs HouseDVotes
            , F.ElemOf rs PresVotes
            , F.ElemOf rs PresDVotes
            )
         => CCESVoteSource -> (F.Record rs -> Int, F.Record rs -> Int)
getVotes CCESHouse = (F.rgetField @HouseVotes, F.rgetField @HouseDVotes)
getVotes CCESPres = (F.rgetField @PresVotes, F.rgetField @PresDVotes)
getVotes CCESComposite =
  let hv = F.rgetField @HouseVotes
      hdv = F.rgetField @HouseDVotes
      pv = F.rgetField @PresVotes
      pdv = F.rgetField @PresDVotes
      f h p r = round (realToFrac ((hv r * h r) + (pv r * p r))/realToFrac (hv r + pv r))
  in (f hv pv, f hdv pdv)

zeroVotes ::  (F.ElemOf rs HouseVotes
              , F.ElemOf rs HouseDVotes
              , F.ElemOf rs PresVotes
              , F.ElemOf rs PresDVotes
              )
          => CCESVoteSource -> F.Record rs -> Bool
zeroVotes CCESHouse r = F.rgetField @HouseVotes r == 0
zeroVotes CCESPres r = F.rgetField @PresVotes r == 0
zeroVotes CCESComposite r = F.rgetField @HouseVotes r == 0 && F.rgetField @PresVotes r == 0

countZeroVoteRows :: (F.ElemOf rs HouseVotes
                     , F.ElemOf rs HouseDVotes
                     , F.ElemOf rs PresVotes
                     , F.ElemOf rs PresDVotes
                     , Foldable f
                     )
                  => CCESVoteSource -> f (F.Record rs) -> Int
countZeroVoteRows vs = FL.fold (FL.prefilter (zeroVotes vs) FL.length)

data Model tr pr = Model { turnoutData :: TurnoutDataSet tr
                         , prefData :: PrefDataSet pr
                         , densityTransform :: DensityTransform
                         }  deriving (Generic)

--instance Flat.Flat (Model tr pr)
--type instance FI.VectorFor (Model tr pr) = Vector.Vector

modelLabel :: Model tr pr -> Text
modelLabel m = turnoutDataSetModelText (turnoutData m) <> "_"
               <> prefDataSetModelText (prefData m) <> "_"
               <> show (densityTransform m)

dataLabel :: Model tr pr -> Text
dataLabel m = turnoutDataSetDataText (turnoutData m) <> "_"
               <> prefDataSetDataText (prefData m) <> "_"
               <> show (densityTransform m)

type SLDLocation = (Text, ET.DistrictType, Int)

sldLocationToRec :: SLDLocation -> F.Record [BR.StateAbbreviation, ET.DistrictTypeC, ET.DistrictNumber]
sldLocationToRec (sa, dt, dn) = sa F.&: dt F.&: dn F.&: V.RNil

type ModeledShare = "ModeledShare" F.:-> MT.ConfidenceInterval
type ModeledTurnout = "ModeledTurnout" F.:-> MT.ConfidenceInterval
type ModeledPref = "ModeledPref" F.:-> MT.ConfidenceInterval
type ModelDesc = "ModelDescription" F.:-> Text

type ModelResultsR ks  = '[BR.Year] V.++ ks V.++ '[ModelDesc, ModeledShare]

type CrossTabFrame k  = F.FrameRec [BR.Year, k, ModelDesc, ModeledTurnout, ModeledPref]

data ModelCrossTabs = ModelCrossTabs
  {
    bySex :: CrossTabFrame DT.SexC
  , byEducation :: CrossTabFrame DT.CollegeGradC
  , byRace :: CrossTabFrame DT.Race5C
  , byState :: CrossTabFrame BR.StateAbbreviation
  }

instance Flat.Flat ModelCrossTabs where
  size (ModelCrossTabs s e r st) n = Flat.size (FS.SFrame s, FS.SFrame e, FS.SFrame r, FS.SFrame st) n
  encode (ModelCrossTabs s e r st) = Flat.encode (FS.SFrame s, FS.SFrame e, FS.SFrame r, FS.SFrame st)
  decode = (\(s, e, r, st) -> ModelCrossTabs (FS.unSFrame s) (FS.unSFrame e) (FS.unSFrame r) (FS.unSFrame st)) <$> Flat.decode


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

race5FromCensus :: F.Record CPSVByCDR -> DT.Race5
race5FromCensus r =
  let race4A = F.rgetField @DT.RaceAlone4C r
      hisp = F.rgetField @DT.HispC r
  in DT.race5FromRaceAlone4AndHisp True race4A hisp
-}

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

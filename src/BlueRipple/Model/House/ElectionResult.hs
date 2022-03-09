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
{-# LANGUAGE TemplateHaskell #-}
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
import qualified Frames.Streamly.InCore as FI
import qualified Frames.Streamly.TH as FS
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
import qualified Stan.ModelBuilder.ModelParameters as SMP
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
import qualified Stan.ModelBuilder as SB
import Graphics.Vega.VegaLite (dataFromColumns)
import BlueRipple.Data.DataFrames (cVAPByCDAndRace_RawParser)
import BlueRipple.Configuration (clStanParallel)
import qualified Stan.ModelBuilder.BuildingBlocks as SB


type Surveyed = "Surveyed" F.:-> Int -- total people in each bucket
type AHVoted = "AHVoted" F.:-> Double

FS.declareColumn "AHHouseDVotes" ''Double
FS.declareColumn "AHHouseRVotes" ''Double
FS.declareColumn "AHPresDVotes" ''Double
FS.declareColumn "AHPresRVotes" ''Double
FS.declareColumn "AchenHurWeight" ''Double
FS.declareColumn "VotesInRace" ''Int
FS.declareColumn "DVotes" ''Int
FS.declareColumn "RVotes" ''Int
FS.declareColumn "TVotes" ''Int
FS.declareColumn "Voted" ''Int
FS.declareColumn "HouseVotes" ''Int
FS.declareColumn "HouseDVotes" ''Int
FS.declareColumn "HouseRVotes" ''Int
FS.declareColumn "PresVotes" ''Int
FS.declareColumn "PresDVotes" ''Int
FS.declareColumn "PresRVotes" ''Int


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

--type AHPresDVotes = "AHPresDVotes" F.:-> Double
type CCESAH = [AHVoted, AHHouseDVotes, AHHouseRVotes, AHPresDVotes, AHPresRVotes]
type CCESVotingDataR = [Surveyed, Voted, HouseVotes, HouseDVotes, HouseRVotes, PresVotes, PresDVotes, PresRVotes]
type CCESByCDR = CDKeyR V.++ [DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.Race5C, DT.HispC] V.++ CCESVotingDataR
type CCESByCDAH = CDKeyR V.++ [DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.Race5C, DT.HispC] V.++ CCESVotingDataR V.++ CCESAH
type CCESDataR = CCESByCDR V.++ [Incumbency, DT.AvgIncome, DT.PopPerSqMile]
type CCESPredictorR = [DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.Race5C, DT.HispC] --, DT.AvgIncome, DT.PopPerSqMile]
type CCESPredictorEMR = [DT.SexC, DT.CollegeGradC, DT.Race5C, DT.HispC] --, DT.AvgIncome, DT.PopPerSqMile]
type CCESByCDEMR = CDKeyR V.++ [DT.SexC, DT.CollegeGradC, DT.Race5C, DT.HispC] V.++ CCESVotingDataR V.++ CCESAH
type CCESData = F.FrameRec CCESDataR
--type CCESWData = F.FrameRec CCESWDataR
--type CCESVotingDataWR = [SurveyedW, VotedW, HouseVotesW, HouseDVotesW, PresVotesW, PresDVotesW]
--type CCESWByCDR = CDKeyR V.++ [DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.Race5C, DT.HispC] V.++ CCESVotingDataWR
--type CCESWDataR = CCESWByCDR V.++ [Incumbency, DT.AvgIncome, DT.PopPerSqMile]

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
type AHSuccesses = "AHSucceses" F.:-> Double
type CensusPredictorR = [DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.RaceAlone4C, DT.HispC]
type CensusPredictorEMR = [DT.SexC, DT.CollegeGradC, DT.RaceAlone4C, DT.HispC]
type CPSVDataR  = CensusPredictorR V.++ BRCF.CountCols
type CPSVDataEMR  = CensusPredictorEMR V.++ BRCF.CountCols V.++ '[AHSuccesses]
type CPSVByStateR = StateKeyR V.++ CPSVDataR
type CPSVByStateEMR = StateKeyR V.++ CPSVDataEMR
type CPSVByStateAH = CPSVByStateR V.++ '[AHSuccesses]

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
                                 , stateElectionRows :: F.FrameRec (ElectionResultWithDemographicsR '[BR.Year, BR.StateAbbreviation])
                                 , cdElectionRows :: F.FrameRec (ElectionResultWithDemographicsR '[BR.Year, BR.StateAbbreviation, BR.CongressionalDistrict])
                                 } deriving (Generic)

instance Flat.Flat CCESAndCPSEM where
  size (CCESAndCPSEM cces cpsV stElex cdElex) n = Flat.size (FS.SFrame cces, FS.SFrame cpsV, FS.SFrame stElex, FS.SFrame cdElex) n
  encode (CCESAndCPSEM cces cpsV stElex cdElex) = Flat.encode (FS.SFrame cces, FS.SFrame cpsV, FS.SFrame stElex, FS.SFrame cdElex)
  decode = (\(cces, cpsV, stElex, cdElex) -> CCESAndCPSEM (FS.unSFrame cces) (FS.unSFrame cpsV) (FS.unSFrame stElex) (FS.unSFrame cdElex)) <$> Flat.decode

ccesAndCPSForYears :: [Int] -> CCESAndCPSEM -> CCESAndCPSEM
ccesAndCPSForYears ys (CCESAndCPSEM cces cpsV stElex cdElex) =
  let f :: (FI.RecVec rs, F.ElemOf rs BR.Year) => F.FrameRec rs -> F.FrameRec rs
      f = F.filterFrame ((`elem` ys) . F.rgetField @BR.Year)
  in CCESAndCPSEM (f cces) (f cpsV) (f stElex) (f cdElex)


prepCCESAndCPSEM :: (K.KnitEffects r, BR.CacheEffects r)
                 => Bool -> K.Sem r (K.ActionWithCacheTime r CCESAndCPSEM)
prepCCESAndCPSEM clearCache = do
  ccesAndPUMS_C <- prepCCESAndPums clearCache
  presElex_C <- prepPresidentialElectionData clearCache 2016
  senateElex_C <- prepSenateElectionData clearCache 2016
  houseElex_C <- prepHouseElectionData clearCache 2016
--  K.logLE K.Diagnostic "Presidential Election Rows"
--  K.ignoreCacheTime elex_C >>= BR.logFrame
  let cacheKey = "model/house/CCESAndCPSEM.bin"
      deps = (,,,) <$> ccesAndPUMS_C <*> presElex_C <*> senateElex_C <*> houseElex_C
  when clearCache $ BR.clearIfPresentD cacheKey
  BR.retrieveOrMakeD cacheKey deps $ \(ccesAndPums, pElex, sElex, hElex) -> do
    let ccesEM = FL.fold fldAgeInCCES $ ccesRows ccesAndPums
        cpsVEM = FL.fold fldAgeInCPS $ cpsVRows ccesAndPums
    return $ CCESAndCPSEM ccesEM cpsVEM (pElex <> sElex) hElex

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
      nwh r = race4A r /= DT.RA4_White && hisp r == DT.Hispanic
      black r = race4A r == DT.RA4_Black && hisp r == DT.NonHispanic
      asian r = race4A r == DT.RA4_Asian && hisp r == DT.NonHispanic
      other r = race4A r == DT.RA4_Other && hisp r == DT.NonHispanic
      white r = race4A r == DT.RA4_White
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
                       -> F.FrameRec PUMSByCDR
                       -> K.Sem r (F.FrameRec (ElectionResultWithDemographicsR [BR.Year, BR.StateAbbreviation, BR.CongressionalDistrict]))
makeCDElexDataFrame office earliestYear elex acsByCD = do
        let addOffice rs = FT.recordSingleton @ET.Office office F.<+> rs
            fixDC_CD r = if (F.rgetField @BR.StateAbbreviation r == "DC")
                         then FT.fieldEndo @BR.CongressionalDistrict (const 1) r
                         else r

        flattenedElex <- K.knitEither
                         $ FL.foldM (electionF @[BR.Year, BR.StateAbbreviation, BR.CongressionalDistrict])
                         (fmap F.rcast $ F.filterFrame ((>= earliestYear) . F.rgetField @BR.Year) elex)
        let demographics = fmap fixDC_CD $ pumsMR @[BR.Year, BR.StateAbbreviation, BR.CongressionalDistrict] acsByCD
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
  cdFromPUMA_C <- BR.allCDFromPUMA2012Loader
  acsByCD_C <- cachedPumsByCD acs_C cdFromPUMA_C
  let deps = (,) <$> houseElex_C <*> acsByCD_C
  BR.retrieveOrMakeFrame cacheKey deps
    $ \(houseElex, acsByCD) -> makeCDElexDataFrame ET.House earliestYear (fmap F.rcast houseElex) acsByCD


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
                  (F.Record [Surveyed, Voted, HouseVotes, HouseDVotes, HouseRVotes, PresVotes, PresDVotes, PresRVotes])
countCESVotesF =
  let vote (MT.MaybeData x) = maybe False (const True) x
      dVote (MT.MaybeData x) = maybe False (== ET.Democratic) x
      rVote (MT.MaybeData x) = maybe False (== ET.Republican) x
      surveyedF = FL.length
      votedF = FL.prefilter (CCES.catalistVoted . F.rgetField @CCES.CatalistTurnoutC) FL.length
      houseVotesF = FL.prefilter (vote . F.rgetField @CCES.MHouseVoteParty) votedF
      houseDVotesF = FL.prefilter (dVote . F.rgetField @CCES.MHouseVoteParty) votedF
      houseRVotesF = FL.prefilter (rVote . F.rgetField @CCES.MHouseVoteParty) votedF
      presVotesF = FL.prefilter (vote . F.rgetField @CCES.MPresVoteParty) votedF
      presDVotesF = FL.prefilter (dVote . F.rgetField @CCES.MPresVoteParty) votedF
      presRVotesF = FL.prefilter (rVote . F.rgetField @CCES.MPresVoteParty) votedF
  in (\s v hv hdv hrv pv pdv prv -> s F.&: v F.&: hv F.&: hdv F.&: hrv F.&: pv F.&: pdv F.&: prv F.&: V.RNil)
     <$> surveyedF <*> votedF <*> houseVotesF <*> houseDVotesF <*> houseRVotesF <*> presVotesF <*> presDVotesF <*> presRVotesF

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


adjUsing :: forall x n d rs a.(V.KnownField x
                              , F.ElemOf rs x
                              , V.Snd x ~ Double
                              , V.KnownField n
                              , V.Snd n ~ Double
                              , V.KnownField d
                              , F.ElemOf rs d
                              , V.Snd d ~ a
                              )
         => (a -> Double) -> F.Record rs -> F.Record (rs V.++ '[n])
adjUsing toDbl r = r F.<+> FT.recordSingleton @n (F.rgetField @x r * toDbl (F.rgetField @d r))

type CCESAdj cs = CCESByCDR V.++ '[PUMS.Citizens] V.++ cs
addAHColToCCES :: forall ahc ahd cs r.
                  (F.ElemOf (CCESAdj cs V.++ '[AchenHurWeight]) ahd
                  , F.ElemOf (cs V.++ '[AchenHurWeight]) AchenHurWeight
                  , V.KnownField ahc
                  , V.KnownField ahd
                  , V.Snd ahc ~ Double
                  , Real (V.Snd ahd)
                  , (cs V.++ '[ahc])  F.⊆ (CCESAdj (cs V.++ '[AchenHurWeight]) V.++ '[ahc])
                  )
               => (F.Record (CCESAdj cs) -> Double)
               ->  FL.FoldM
                  (K.Sem r)
                  (F.Record (CCESAdj cs V.++ '[AchenHurWeight]))
                  (F.FrameRec (CCESAdj cs V.++ '[AchenHurWeight]))
               -> F.FrameRec (CCESAdj cs)
               -> K.Sem r (F.FrameRec (CCESAdj cs V.++ '[ahc]))
addAHColToCCES wgtF ahFld rows = do
  let ahw = FT.recordSingleton @AchenHurWeight . wgtF
      rowsWithWeight = fmap (FT.mutate ahw) rows
  adjProb <- FL.foldM ahFld rowsWithWeight
  let adjResult :: F.Record (CCESAdj cs V.++ '[AchenHurWeight]) -> F.Record (CCESAdj cs V.++ '[ahc])
      adjResult = F.rcast . adjUsing @AchenHurWeight @ahc @ahd realToFrac
      res = fmap adjResult adjProb
  return res


prepCCESAndPums :: forall r.(K.KnitEffects r, BR.CacheEffects r) => Bool -> K.Sem r (K.ActionWithCacheTime r CCESAndPUMS)
prepCCESAndPums clearCache = do
  let testRun = False
      cacheKey x k = k <> if x then ".tbin" else ".bin"
  let earliestYear = 2016 -- set by ces for now
      earliest year = (>= year) . F.rgetField @BR.Year
      fixDC_CD r = if (F.rgetField @BR.StateAbbreviation r == "DC")
                   then FT.fieldEndo @BR.CongressionalDistrict (const 1) r
                   else r
      fLength = FL.fold FL.length
      lengthInYear y = fLength . F.filterFrame ((== y) . F.rgetField @BR.Year)
  pums_C <- PUMS.pumsLoaderAdults
  pumsByState_C <- cachedPumsByState pums_C
  countedCCES_C <- fmap (BR.fixAtLargeDistricts 0) <$> cesCountedDemVotesByCD clearCache
  cpsVByState_C <- fmap (F.filterFrame $ earliest earliestYear) <$> cpsCountedTurnoutByState

  -- Do the turnout corrections, CPS first
  stateTurnout_C <- BR.stateTurnoutLoader
  let cpsAchenHurDeps = (,,) <$> cpsVByState_C <*> pumsByState_C <*> stateTurnout_C
      cpsAchenHurCacheKey = cacheKey testRun "model/house/CPSV_AchenHur"
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
    let adjVotersU = adjUsing @AchenHurWeight @AHSuccesses @BRCF.Count realToFrac
        res = fmap (F.rcast @CPSVByStateAH . adjVotersU) adjCPSProbU
    K.logLE K.Diagnostic $ "Post Achen-Hur: CPS (by state) rows per year:"
    K.logLE K.Diagnostic $ show $ fmap (\y -> lengthInYear y res) [2012, 2014, 2016, 2018, 2020]
    return res

  cdFromPUMA_C <- BR.allCDFromPUMA2012Loader
  pumsByCD_C <- cachedPumsByCD pums_C cdFromPUMA_C
  let ccesWithACSDeps = (,) <$> countedCCES_C <*> pumsByCD_C
      ccesWithACSCacheKey = "model/house/ccesWithACS.bin"
  when clearCache $ BR.clearIfPresentD ccesWithACSCacheKey
  ccesWithACS_C <- BR.retrieveOrMakeFrame ccesWithACSCacheKey ccesWithACSDeps $ \(cces, acsByCD) -> do
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
        (ccesWithACS, missing) =  FJ.leftJoinWithMissing @(CDKeyR V.++ CCESPredictorR) cces fixedACS
    when (not $ null missing) $ K.knitError $ "Missing keys in cces/acs join: " <> show missing
    when (fLength cces /= fLength ccesWithACS) $ K.knitError "prepCCESAndPums: rows added/deleted by left-join(cces,acs)"
    return $ fmap (F.rcast @(CCESByCDR V.++'[PUMS.Citizens])) $ ccesWithACS


  let ccesTAchenHurDeps = (,) <$> ccesWithACS_C <*> stateTurnout_C
      ccesTAchenHurCacheKey = cacheKey testRun "model/house/CCEST_AchenHur"
  when clearCache $ BR.clearIfPresentD ccesTAchenHurCacheKey
  ccesTAchenHur_C <- BR.retrieveOrMakeFrame ccesTAchenHurCacheKey ccesTAchenHurDeps $ \(ccesWithACS, stateTurnout) -> do
    let wgtF r = let s = F.rgetField @Surveyed r in if s == 0 then 0.5 else realToFrac (F.rgetField @Voted r) / realToFrac s
        ahFld = BRTA.adjTurnoutFold @PUMS.Citizens @AchenHurWeight stateTurnout
    addAHColToCCES @AHVoted @Surveyed @'[] wgtF ahFld ccesWithACS

  -- now vote totals corrections ??
  presidentialElectionResults_C <- prepPresidentialElectionData clearCache earliestYear
  let ccesPVAchenHurDeps = (,) <$> ccesTAchenHur_C <*> presidentialElectionResults_C
      ccesPVAchenHurCacheKey = cacheKey testRun "model/house/CCESPV_AchenHur"
  when clearCache $ BR.clearIfPresentD ccesPVAchenHurCacheKey
  ccesPVAchenHur_C <- BR.retrieveOrMakeFrame ccesPVAchenHurCacheKey ccesPVAchenHurDeps $ \(ccesT, presElex) -> do
    let wgtD r = let pv = F.rgetField @PresVotes r in if pv == 0 then 0.5 else realToFrac (F.rgetField @PresDVotes r) / realToFrac pv
        elexDShare r = realToFrac (F.rgetField @DVotes r) / realToFrac (F.rgetField @TVotes r)
        ahDFld = BRTA.adjTurnoutFoldG @PUMS.Citizens @AchenHurWeight @[BR.Year, BR.StateAbbreviation] elexDShare presElex
    ccesWithPD <- addAHColToCCES @AHPresDVotes @PresVotes @'[AHVoted] wgtD ahDFld ccesT
    let wgtR r = let pv = F.rgetField @PresVotes r in if pv == 0 then 0.5 else realToFrac (F.rgetField @PresRVotes r) / realToFrac pv
        elexRShare r = realToFrac (F.rgetField @RVotes r) / realToFrac (F.rgetField @TVotes r)
        ahRFld = BRTA.adjTurnoutFoldG @PUMS.Citizens @AchenHurWeight @[BR.Year, BR.StateAbbreviation] elexRShare presElex
    addAHColToCCES @AHPresRVotes @PresVotes @'[AHVoted, AHPresDVotes] wgtR ahRFld ccesWithPD

  houseElectionResults_C <- prepHouseElectionData clearCache earliestYear
  let ccesHVAchenHurDeps = (,) <$> ccesPVAchenHur_C <*> houseElectionResults_C
      ccesHVAchenHurCacheKey = cacheKey testRun "model/house/CCESHV_AchenHur"
  when clearCache $ BR.clearIfPresentD ccesHVAchenHurCacheKey
  ccesHVAchenHur_C <- BR.retrieveOrMakeFrame ccesHVAchenHurCacheKey ccesHVAchenHurDeps $ \(ccesTP, houseElex) -> do
    let wgtD r = let pv = F.rgetField @HouseVotes r in if pv == 0 then 0.5 else realToFrac (F.rgetField @HouseDVotes r) / realToFrac pv
        elexDShare r = realToFrac (F.rgetField @DVotes r) / realToFrac (F.rgetField @TVotes r)
        ahDFld = BRTA.adjTurnoutFoldG @PUMS.Citizens @AchenHurWeight @[BR.Year, BR.StateAbbreviation, BR.CongressionalDistrict] elexDShare houseElex
    ccesWithHD <- addAHColToCCES @AHHouseDVotes @HouseVotes @'[AHVoted, AHPresDVotes, AHPresRVotes] wgtD ahDFld ccesTP
    let wgtR r = let pv = F.rgetField @HouseVotes r in if pv == 0 then 0.5 else realToFrac (F.rgetField @HouseRVotes r) / realToFrac pv
        elexRShare r = realToFrac (F.rgetField @RVotes r) / realToFrac (F.rgetField @TVotes r)
        ahRFld = BRTA.adjTurnoutFoldG @PUMS.Citizens @AchenHurWeight @[BR.Year, BR.StateAbbreviation] elexRShare houseElex
    addAHColToCCES @AHHouseRVotes @HouseVotes @'[AHVoted, AHPresDVotes, AHPresRVotes, AHHouseDVotes] wgtR ahRFld ccesWithHD


  let deps = (,,) <$> ccesHVAchenHur_C <*> cpsV_AchenHur_C <*> pumsByCD_C
      allCacheKey = cacheKey testRun "model/house/CCESAndPUMS"
  when clearCache $ BR.clearIfPresentD allCacheKey
  BR.retrieveOrMakeD allCacheKey deps $ \(ccesByCD, cpsVByState, acsByCD) -> do
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
    return $ CCESAndPUMS (fmap F.rcast ccesWD) cpsVWD acsWD diByCD -- (F.toFrame $ fmap F.rcast $ cats)


type CCESWithDensity = CCESByCDAH V.++ '[DT.PopPerSqMile]
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
              (FMR.foldAndAddKey $ sumButLeaveDensity @(BRCF.CountCols V.++ '[AHSuccesses]))

fldAgeInACS :: FL.Fold (F.Record PUMSWithDensity) (F.FrameRec PUMSWithDensityEM)
fldAgeInACS = FMR.concatFold
               $ FMR.mapReduceFold
               FMR.noUnpack
               (FMR.assignKeysAndData @(CDKeyR V.++ CensusPredictorEMR))
               (FMR.foldAndAddKey $ sumButLeaveDensity @'[PUMS.Citizens, PUMS.NonCitizens])


sumButLeaveDensityCCES :: FL.Fold (F.Record ((CCESVotingDataR V.++ CCESAH V.++ '[DT.PopPerSqMile]))) (F.Record ((CCESVotingDataR V.++ CCESAH V.++ '[DT.PopPerSqMile])))
sumButLeaveDensityCCES =
  let sumF f = FL.premap f FL.sum
      densF = fmap (fromMaybe 0) $ FL.premap (F.rgetField @DT.PopPerSqMile) FL.last
  in (\f1 f2 f3 f4 f5 f6 f7 f8 f9 f10 f11 f12 f13 f14 -> f1 F.&: f2 F.&: f3 F.&: f4 F.&: f5 F.&: f6 F.&: f7 F.&: f8 F.&: f9 F.&: f10 F.&: f11 F.&: f12 F.&: f13 F.&: f14 F.&: V.RNil)
     <$> sumF (F.rgetField @Surveyed)
     <*> sumF (F.rgetField @Voted)
     <*> sumF (F.rgetField @HouseVotes)
     <*> sumF (F.rgetField @HouseDVotes)
     <*> sumF (F.rgetField @HouseRVotes)
     <*> sumF (F.rgetField @PresVotes)
     <*> sumF (F.rgetField @PresDVotes)
     <*> sumF (F.rgetField @PresRVotes)
     <*> sumF (F.rgetField @AHVoted)
     <*> sumF (F.rgetField @AHHouseDVotes)
     <*> sumF (F.rgetField @AHHouseRVotes)
     <*> sumF (F.rgetField @AHPresDVotes)
     <*> sumF (F.rgetField @AHPresRVotes)
     <*> densF


-- NB : the polymorphic sumButLeaveDensity caused some sort of memory blowup compiling this one.
fldAgeInCCES :: FL.Fold (F.Record CCESWithDensity) (F.FrameRec CCESWithDensityEM)
fldAgeInCCES = FMR.concatFold
               $ FMR.mapReduceFold
               FMR.noUnpack
               (FMR.assignKeysAndData @(CDKeyR V.++ CCESPredictorEMR))
               (FMR.foldAndAddKey sumButLeaveDensityCCES)


type PUMSWithDensity = PUMSByCDR V.++ '[DT.PopPerSqMile]
type PUMSWithDensityEM = PUMSByCDEMR V.++ '[DT.PopPerSqMile]

type CPSVWithDensity = CPSVByStateAH V.++ '[DT.PopPerSqMile]
type CPSVWithDensityEM = CPSVByStateEMR V.++ '[DT.PopPerSqMile]

race5FromRace4AAndHisp :: (F.ElemOf rs DT.RaceAlone4C, F.ElemOf rs DT.HispC) => F.Record rs -> DT.Race5
race5FromRace4AAndHisp r =
  let race4A = F.rgetField @DT.RaceAlone4C r
      hisp = F.rgetField @DT.HispC r
  in DT.race5FromRaceAlone4AndHisp True race4A hisp

addRace5 :: (F.ElemOf rs DT.RaceAlone4C, F.ElemOf rs DT.HispC)
         => F.Record rs -> F.Record (rs V.++ '[DT.Race5C])
addRace5 r = r V.<+> FT.recordSingleton @DT.Race5C (race5FromRace4AAndHisp r)

psFldCPS :: FL.Fold (F.Record [CVAP, Voters, DemVoters]) (F.Record [CVAP, Voters, DemVoters])
psFldCPS = FF.foldAllConstrained @Num FL.sum

{-(\cv v dv -> cv F.&: v F.&: dv F.&: V.RNil) <$> cvF <*> vF <*> dvF where
  cvF = FL.premap (F.rgetField @CVAP) FL.sum
  vF = FL.premap (F.rgetField @Voters) FL.sum
  dvF = FL.premap (F.rgetField @DemVoters) FL.sum
-}

cpsDiagnostics :: (K.KnitEffects r, BR.CacheEffects r)
               => Text
               -> K.ActionWithCacheTime r (F.FrameRec CPSVByStateAH)
               -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec [BR.Year, BR.StateAbbreviation, BRCF.Count, AHSuccesses]
                                                   , F.FrameRec [BR.Year, BR.StateAbbreviation, BRCF.Count, AHSuccesses]
                                                   )
                          )
cpsDiagnostics t cpsByState_C = K.wrapPrefix "cpDiagnostics" $ do
  let cpsCountsByYearAndStateFld = FMR.concatFold
                                   $ FMR.mapReduceFold
                                   FMR.noUnpack
                                   (FMR.assignKeysAndData @'[BR.Year, BR.StateAbbreviation] @[BRCF.Count, AHSuccesses])
                                   (FMR.foldAndAddKey $ FF.foldAllConstrained @Num FL.sum)
      surveyed = F.rgetField @BRCF.Count
      voted = F.rgetField @BRCF.Successes
      ahVoted = F.rgetField @AHSuccesses
      cvap = F.rgetField @PUMS.Citizens
      ratio x y = realToFrac @_ @Double x / realToFrac @_ @Double y
      pT r = ratio (ahVoted r) (surveyed r)
      compute rw rc = let voters = pT rw * realToFrac (cvap rc)
                      in (cvap rc F.&: voters F.&: V.RNil ) :: F.Record [BRCF.Count, AHSuccesses]
      addTurnout r = let cv = realToFrac (F.rgetField @CVAP r)
                     in r F.<+> (FT.recordSingleton @Turnout
                                  $ if cv < 1 then 0 else F.rgetField @Voters r / cv)
  let rawCK = "model/house/rawCPSByState.bin"
  rawCPS_C <- BR.retrieveOrMakeFrame rawCK cpsByState_C $ return . fmap F.rcast . FL.fold cpsCountsByYearAndStateFld
  acsByState_C <- PUMS.pumsLoaderAdults >>= cachedPumsByState
  let psCK = "model/house/psCPSByState.bin"
      psDeps = (,) <$> acsByState_C <*> cpsByState_C
  psCPS_C <- BR.retrieveOrMakeFrame psCK psDeps $ \(acsByState, cpsByState) -> do
    let acsFixed = F.filterFrame (\r -> F.rgetField @BR.Year r >= 2016) acsByState
        cpsFixed = F.filterFrame (\r -> F.rgetField @BR.Year r >= 2016) cpsByState
        (psByState, missing) =  BRPS.joinAndPostStratify @'[BR.Year,BR.StateAbbreviation] @CensusPredictorR @[BRCF.Count, AHSuccesses] @'[PUMS.Citizens]
                                compute
                                (FF.foldAllConstrained @Num FL.sum)
                                (F.rcast <$> cpsFixed)
                                (F.rcast <$> acsFixed)
    pure psByState
  pure $ (,) <$> rawCPS_C <*> psCPS_C


-- CCES diagnostics
type Voters = "Voters" F.:-> Double
type DemVoters = "DemVoters" F.:-> Double
--type CVAP = "CVAP" F.:-> Double
type Turnout = "Turnout" F.:-> Double

type CCESBucketR = [DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.Race5C, DT.HispC]

--psFldCCES :: FL.Fold (F.Record [CVAP, Voters, DemVoters]) (F.Record [CVAP, Voters, DemVoters])
--psFldCCES = FF.foldAllConstrained @Num FL.sum


ccesDiagnostics :: (K.KnitEffects r, BR.CacheEffects r)
                => Bool
                -> Text
--                -> CCESVoteSource
                -> K.ActionWithCacheTime r (F.FrameRec PUMSByCDR)
                -> K.ActionWithCacheTime r (F.FrameRec CCESByCDAH)
                -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec [BR.Year,BR.StateAbbreviation, CVAP, Surveyed, AHVoted, PresVotes, AHPresDVotes, AHPresRVotes, HouseVotes, AHHouseDVotes, AHHouseRVotes]))
ccesDiagnostics clearCaches cacheSuffix acs_C cces_C = K.wrapPrefix "ccesDiagnostics" $ do
  K.logLE K.Info $ "computing CES diagnostics..."
  let surveyed =  F.rgetField @Surveyed
      voted = F.rgetField @AHVoted
      ratio x y = realToFrac @_ @Double x / realToFrac @_ @Double y
      pT r = ratio (voted r) (surveyed r)
--      (votesInRace, dVotesInRace) = getVotes vs
      presVotes = F.rgetField @PresVotes
      presDVotes = F.rgetField @AHPresDVotes
      presRVotes = F.rgetField @AHPresRVotes
      houseVotes = F.rgetField @HouseVotes
      houseDVotes = F.rgetField @AHHouseDVotes
      houseRVotes = F.rgetField @AHHouseRVotes
      pDP r = if presVotes r == 0 then 0.5 else ratio (presDVotes r) (presVotes r)
      pRP r = if presVotes r == 0 then 0.5 else ratio (presRVotes r) (presVotes r)
      pDH r = if houseVotes r == 0 then 0.5 else ratio (houseDVotes r) (houseVotes r)
      pRH r = if houseVotes r == 0 then 0.5 else ratio (houseRVotes r) (houseVotes r)
      cvap = F.rgetField @PUMS.Citizens
      addRace5 r = r F.<+> (FT.recordSingleton @DT.Race5C $ DT.race5FromRaceAlone4AndHisp True (F.rgetField @DT.RaceAlone4C r) (F.rgetField @DT.HispC r))
      compute rw rc = let voters = pT rw * realToFrac (cvap rc)
                          wSurveyed = round $ realToFrac (cvap rc) * realToFrac (surveyed rw)
                          presVotesInRace = realToFrac (cvap rc) * ratio (presVotes rw) (surveyed rw)
                          houseVotesInRace = realToFrac (cvap rc) * ratio (houseVotes rw) (surveyed rw)
                          dVotesP =  realToFrac (cvap rc) * pDP rw
                          rVotesP =  realToFrac (cvap rc) * pRP rw
                          dVotesH =   realToFrac (cvap rc) * pDH rw
                          rVotesH =   realToFrac (cvap rc) * pRH rw
                      in (cvap rc F.&: wSurveyed F.&: voters F.&: round presVotesInRace F.&: dVotesP F.&: rVotesP F.&: round houseVotesInRace F.&: dVotesH F.&: rVotesH F.&: V.RNil ) :: F.Record [CVAP, Surveyed, AHVoted, PresVotes, AHPresDVotes, AHPresRVotes, HouseVotes, AHHouseDVotes, AHHouseRVotes]
      deps = (,) <$> acs_C <*> cces_C
  let statesCK = "diagnostics/ccesPSByPumsStates" <> cacheSuffix <> ".bin"
  when clearCaches $ BR.clearIfPresentD statesCK
  BR.retrieveOrMakeFrame statesCK deps $ \(acs, cces) -> do
    let acsFixed =  addRace5 <$> (F.filterFrame (\r -> F.rgetField @BR.Year r >= 2016) acs)
        (psByState, missing) = BRPS.joinAndPostStratify @'[BR.Year,BR.StateAbbreviation] @CCESBucketR @[Surveyed, AHVoted, PresVotes, AHPresDVotes, AHPresRVotes, HouseVotes, AHHouseDVotes, AHHouseRVotes] @'[PUMS.Citizens]
                               compute
                               (FF.foldAllConstrained @Num FL.sum)
                               (F.rcast <$> cces)
                               (F.rcast <$> acsFixed)
--    when (not $ null missing) $ K.knitError $ "ccesDiagnostics: Missing keys in cces/pums join: " <> show missing
    pure psByState


--data DMModel = BaseDM
--             | WithCPSTurnoutDM
--             deriving (Show, Eq, Ord, Generic)

type ElectionWithDemographicsR = ElectionResultWithDemographicsR '[BR.Year, BR.StateAbbreviation]


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
               => Model
               -> SB.GroupTypeTag (F.Record ks)
               -> [Text]
               -> [F.Record ks]
               -> SB.StanGroupBuilderM CCESAndCPSEM (F.FrameRec PUMSWithDensityEM, F.FrameRec rs) ()
groupBuilderDM model psGroup states psKeys = do
  let loadCPSTurnoutData :: SB.StanGroupBuilderM CCESAndCPSEM (F.FrameRec PUMSWithDensityEM, F.FrameRec rs) () = do
        cpsData <- SB.addModelDataToGroupBuilder "CPS" (SB.ToFoldable $ F.filterFrame ((/=0) . F.rgetField @BRCF.Count) . cpsVEMRows)
        SB.addGroupIndexForData stateGroup cpsData $ SB.makeIndexFromFoldable show (F.rgetField @BR.StateAbbreviation) states
      officeFilterF offices r = Set.member (F.rgetField @ET.Office r) offices
      allElexRows :: CCESAndCPSEM -> F.FrameRec (ElectionResultWithDemographicsR '[BR.Year, BR.StateAbbreviation])
      allElexRows x = stateElectionRows x <> (fmap F.rcast $ cdElectionRows x)
      loadElexTurnoutData :: Set ET.OfficeT -> SB.StanGroupBuilderM CCESAndCPSEM (F.FrameRec PUMSWithDensityEM, F.FrameRec rs) ()
      loadElexTurnoutData offices = do
        elexTurnoutData <- SB.addModelDataToGroupBuilder "ElectionsT" (SB.ToFoldable $ F.filterFrame (officeFilterF offices) . allElexRows)
        SB.addGroupIndexForData stateGroup elexTurnoutData $ SB.makeIndexFromFoldable show (F.rgetField @BR.StateAbbreviation) states
      loadElexPrefData :: Set ET.OfficeT -> SB.StanGroupBuilderM CCESAndCPSEM (F.FrameRec PUMSWithDensityEM, F.FrameRec rs) ()
      loadElexPrefData offices = do
        elexPrefData <- SB.addModelDataToGroupBuilder "ElectionsP" (SB.ToFoldable $ F.filterFrame (officeFilterF offices) . allElexRows)
        SB.addGroupIndexForData stateGroup elexPrefData $ SB.makeIndexFromFoldable show (F.rgetField @BR.StateAbbreviation) states
      loadCCESTurnoutData :: SB.StanGroupBuilderM CCESAndCPSEM (F.FrameRec PUMSWithDensityEM, F.FrameRec rs) () = do
        ccesTurnoutData <- SB.addModelDataToGroupBuilder "CCEST" (SB.ToFoldable ccesEMRows)
        SB.addGroupIndexForData stateGroup ccesTurnoutData $ SB.makeIndexFromFoldable show (F.rgetField @BR.StateAbbreviation) states
      loadCCESPrefData :: ET.OfficeT -> ET.VoteShareType -> SB.StanGroupBuilderM CCESAndCPSEM (F.FrameRec PUMSWithDensityEM, F.FrameRec rs) ()
      loadCCESPrefData office vst = do
        ccesPrefData <- SB.addModelDataToGroupBuilder ("CCESP_" <> show office)
                        (SB.ToFoldable $ F.filterFrame (not . zeroCCESVotes office vst) . ccesEMRows)
        SB.addGroupIndexForData stateGroup ccesPrefData $ SB.makeIndexFromFoldable show (F.rgetField @BR.StateAbbreviation) states
  loadElexTurnoutData $ votesFrom model
  loadCPSTurnoutData
  loadCCESTurnoutData
  loadElexPrefData $ votesFrom model
  when (Set.member ET.President $ votesFrom model) $ loadCCESPrefData ET.President (voteShareType model)
  when (Set.member ET.House $ votesFrom model) $ loadCCESPrefData ET.House (voteShareType model)

  -- GQ
  acsData <- SB.addGQDataToGroupBuilder "ACS" (SB.ToFoldable fst)
  SB.addGroupIndexForData stateGroup acsData $ SB.makeIndexFromFoldable show (F.rgetField @BR.StateAbbreviation) states
  SB.addGroupIndexForData raceGroup acsData $ SB.makeIndexFromEnum race5Census
  SB.addGroupIndexForData educationGroup acsData $ SB.makeIndexFromEnum (F.rgetField @DT.CollegeGradC)
  SB.addGroupIndexForData sexGroup acsData $ SB.makeIndexFromEnum (F.rgetField @DT.SexC)
  psData <- SB.addGQDataToGroupBuilder "DistrictPS" (SB.ToFoldable $ F.filterFrame ((/=0) . F.rgetField @Census.Count) . snd)
  SB.addGroupIndexForData stateGroup psData $ SB.makeIndexFromFoldable show (F.rgetField @BR.StateAbbreviation) states
  SB.addGroupIndexForData psGroup psData $ SB.makeIndexFromFoldable show F.rcast psKeys

-- If only presidential eleciton is included, incumbency is not useful since it's a constant
data DMType = DMTurnout | DMPref | DMPresOnlyPref deriving (Show, Eq)

designMatrixRowPS :: forall rs.(F.ElemOf rs DT.CollegeGradC
                             , F.ElemOf rs DT.SexC
                             , F.ElemOf rs DT.Race5C
                             , F.ElemOf rs DT.HispC
                             , F.ElemOf rs DT.PopPerSqMile
                             )
                  => DM.DesignMatrixRowPart (F.Record rs)
                  -> DMType
                  -> DM.DesignMatrixRow (F.Record rs)
designMatrixRowPS densRP dmType = DM.DesignMatrixRow (show dmType)
                                  $ case dmType of
                                      DMTurnout -> [densRP, sexRP, eduRP, raceRP, wngRP]
                                      DMPref -> [densRP, incRP, sexRP, eduRP, raceRP, wngRP]
                                      DMPresOnlyPref -> [densRP, sexRP, eduRP, raceRP, wngRP]
  where
    incRP = DM.DesignMatrixRowPart "Incumbency" 1 (const $ VU.replicate 1 0) -- we set incumbency to 0 for PS
    sexRP = DM.boundedEnumRowPart "Sex" (F.rgetField @DT.SexC)
    eduRP = DM.boundedEnumRowPart "Education" (F.rgetField @DT.CollegeGradC)
    raceRP = DM.boundedEnumRowPart "Race" mergeRace5AndHispanic
    wngRP = DM.boundedEnumRowPart "WhiteNonGrad" wnhNonGradCCES

designMatrixRowCCES :: DM.DesignMatrixRowPart (F.Record CCESWithDensityEM)
                    -> DMType
                    -> (F.Record  CCESWithDensityEM -> Double)
                    -> DM.DesignMatrixRow (F.Record CCESWithDensityEM)
designMatrixRowCCES densRP dmType incF = DM.DesignMatrixRow (show dmType)
                                         $ case dmType of
                                             DMTurnout -> [densRP, sexRP, eduRP, raceRP, wngRP]
                                             DMPref -> [densRP, incRP, sexRP, eduRP, raceRP, wngRP]
                                             DMPresOnlyPref -> [densRP, sexRP, eduRP, raceRP, wngRP]
  where
    incRP = DM.rowPartFromFunctions "Incumbency" [incF]
    sexRP = DM.boundedEnumRowPart "Sex" (F.rgetField @DT.SexC)
    eduRP = DM.boundedEnumRowPart "Education" (F.rgetField @DT.CollegeGradC)
    raceRP = DM.boundedEnumRowPart "Race" mergeRace5AndHispanic
    wngRP = DM.boundedEnumRowPart "WhiteNonGrad" wnhNonGradCCES

designMatrixRowCPS :: DM.DesignMatrixRowPart (F.Record CPSVWithDensityEM) -> DM.DesignMatrixRow (F.Record CPSVWithDensityEM)
designMatrixRowCPS densRP = DM.DesignMatrixRow (show DMTurnout) $ [densRP, sexRP, eduRP, raceRP, wngRP]
 where
    sexRP = DM.boundedEnumRowPart "Sex" (F.rgetField @DT.SexC)
    eduRP = DM.boundedEnumRowPart "Education" (F.rgetField @DT.CollegeGradC)
    raceRP = DM.boundedEnumRowPart "Race" race5Census
    wngRP = DM.boundedEnumRowPart "WhiteNonGrad" wnhNonGradCensus

designMatrixRowACS :: DM.DesignMatrixRowPart (F.Record PUMSWithDensityEM)
                   -> DMType
                   -> DM.DesignMatrixRow (F.Record PUMSWithDensityEM)
designMatrixRowACS densRP dmType = DM.DesignMatrixRow (show dmType)
                                   $ case dmType of
                                       DMTurnout -> [densRP, sexRP, eduRP, raceRP, wngRP]
                                       DMPref -> [densRP, incRP, sexRP, eduRP, raceRP, wngRP]
                                       DMPresOnlyPref -> [densRP, sexRP, eduRP, raceRP, wngRP]

  where
    incRP = DM.DesignMatrixRowPart "Incumbency" 1 (const $ VU.replicate 1 0) -- we set incumbency to 0 for ACS/diagnostics
    sexRP = DM.boundedEnumRowPart "Sex" (F.rgetField @DT.SexC)
    eduRP = DM.boundedEnumRowPart "Education" (F.rgetField @DT.CollegeGradC)
    raceRP = DM.boundedEnumRowPart "Race" race5Census
    wngRP = DM.boundedEnumRowPart "WhiteNonGrad" wnhNonGradCensus

designMatrixRowElex :: DM.DesignMatrixRowPart (F.Record ElectionWithDemographicsR)
                    -> DMType
                    -> DM.DesignMatrixRow (F.Record ElectionWithDemographicsR)
designMatrixRowElex densRP dmType = DM.DesignMatrixRow (show dmType)
                                    $ case dmType of
                                        DMTurnout -> [densRP, sexRP, eduRP, raceRP, wngRP]
                                        DMPref -> [densRP, incRP, sexRP, eduRP, raceRP, wngRP]
                                        DMPresOnlyPref -> [densRP, sexRP, eduRP, raceRP, wngRP]
  where
    incRP = DM.rowPartFromFunctions "Incumbency" [realToFrac . F.rgetField @Incumbency]
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
    wngRP = DM.rowPartFromFunctions "WhiteNonGrad" [\r ->  (2 * fracWhiteNonGrad r) - 1] -- white non-grad is 1 since True in Bool, thus 2nd in enum


race5Census r = DT.race5FromRaceAlone4AndHisp True (F.rgetField @DT.RaceAlone4C r) (F.rgetField @DT.HispC r)
wnhCensus r = F.rgetField @DT.RaceAlone4C r == DT.RA4_White && F.rgetField @DT.HispC r == DT.NonHispanic
wnhNonGradCensus r = wnhCensus r && F.rgetField @DT.CollegeGradC r == DT.NonGrad

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
        dsAlpha <- SMP.addParameter ("dsAplha" <> suffix) SB.StanReal "" (SB.UnVectorized SB.stdNormal)
        dsPhi <- SMP.addParameter ("dsPhi" <> suffix) (SB.StanVector $ SB.NamedDim dmColIndex) "" (SB.Vectorized (one dmColIndex) SB.stdNormal)
        SB.inBlock SB.SBGeneratedQuantities $ SB.useDataSetForBindings rtt $ DM.splitToGroupVars dmr dsPhi
        return $ \aE dmE betaE -> predE (aE `SB.plus` SB.paren (SB.var dsAlpha `SB.times` SB.var dsIndexV))
                                         dmE
                                         (betaE `SB.plus` SB.paren (SB.var dsPhi `SB.times` SB.var dsIndexV))
  iPredE <- iPredF
  let iPred a dm beta = iPredE (SB.var a) (SB.var dm) (SB.var beta)
  return (pred, iPred)

setupCCESTData :: (Typeable md, Typeable gq)
               => DM.DesignMatrixRowPart (F.Record CCESWithDensityEM)
               -> SB.StanBuilderM md gq (SB.RowTypeTag (F.Record CCESWithDensityEM)
                                        , DM.DesignMatrixRow (F.Record CCESWithDensityEM)
                                        , SB.StanVar, SB.StanVar, SB.StanVar)
setupCCESTData densRP = do
  ccesTData <- SB.dataSetTag @(F.Record CCESWithDensityEM) SC.ModelData "CCEST"
  dmCCES <- DM.addDesignMatrix ccesTData (designMatrixRowCCES densRP DMTurnout (const 0))
  cvapCCES <- SB.addCountData ccesTData "CVAP_CCES" (F.rgetField @Surveyed)
  votedCCES <- SB.addCountData ccesTData "Voted_CCES" (round . F.rgetField @AHVoted)
  return (ccesTData, designMatrixRowCCES densRP DMTurnout (const 0), cvapCCES, votedCCES, dmCCES)


setupCPSData ::  (Typeable md, Typeable gq)
             => DM.DesignMatrixRowPart (F.Record CPSVWithDensityEM)
             -> SB.StanBuilderM md gq (SB.RowTypeTag (F.Record CPSVWithDensityEM)
                                      , DM.DesignMatrixRow (F.Record CPSVWithDensityEM)
                                      , SB.StanVar, SB.StanVar, SB.StanVar)
setupCPSData densRP = do
  cpsData <- SB.dataSetTag @(F.Record CPSVWithDensityEM) SC.ModelData "CPS"
  cvapCPS <- SB.addCountData cpsData "CVAP_CPS" (F.rgetField @BRCF.Count)
  votedCPS <- SB.addCountData cpsData "Voted_CPS"  (round . F.rgetField @AHSuccesses)
  dmCPS <- DM.addDesignMatrix cpsData (designMatrixRowCPS densRP)
  return (cpsData, designMatrixRowCPS densRP, cvapCPS, votedCPS, dmCPS)


setupElexTData :: (Typeable md, Typeable gq)
               => DM.DesignMatrixRowPart (F.Record ElectionWithDemographicsR)
               -> SB.StanBuilderM md gq (SB.RowTypeTag (F.Record ElectionWithDemographicsR)
                                        , DM.DesignMatrixRow (F.Record ElectionWithDemographicsR)
                                        , SB.StanVar, SB.StanVar, SB.StanVar)
setupElexTData densRP = do
  elexTData <- SB.dataSetTag @(F.Record ElectionWithDemographicsR) SC.ModelData "ElectionsT"
  cvapElex <- SB.addCountData elexTData "CVAP_Elections" (F.rgetField @PUMS.Citizens)
  votedElex <- SB.addCountData elexTData "Voted_Elections" (F.rgetField @TVotes)
  dmElexT <- DM.addDesignMatrix elexTData (designMatrixRowElex densRP DMTurnout)
  return (elexTData, designMatrixRowElex densRP DMTurnout, cvapElex, votedElex, dmElexT)


setupCCESPData :: (Typeable md, Typeable gq)
               => DM.DesignMatrixRowPart (F.Record CCESWithDensityEM)
               -> DMType
               -> (ET.OfficeT -> F.Record CCESWithDensityEM -> Double)
               -> ET.OfficeT
               -> ET.VoteShareType
               -> SB.StanBuilderM md gq (SB.RowTypeTag (F.Record CCESWithDensityEM)
                                        , DM.DesignMatrixRow (F.Record CCESWithDensityEM)
                                        , SB.StanVar, SB.StanVar, SB.StanVar)
setupCCESPData densRP dmPrefType incF office vst = do
  let (votesF, dVotesF) = getCCESVotes office vst
  ccesPData <- SB.dataSetTag @(F.Record CCESWithDensityEM) SC.ModelData ("CCESP_" <> show office)
--  ccesPIndex <- SB.indexedConstIntArray ccesPData (Just "P") n
  dmCCESP <- DM.addDesignMatrix ccesPData (designMatrixRowCCES densRP dmPrefType (incF office))
  raceVotesCCES <- SB.addCountData ccesPData ("VotesInRace_CCES_" <> show office) (round . votesF)
  dVotesInRaceCCES <- SB.addCountData ccesPData ("DVotesInRace_CCES_" <> show office) (round . dVotesF)
  return (ccesPData, designMatrixRowCCES densRP DMPref (incF office), raceVotesCCES, dVotesInRaceCCES, dmCCESP)

setupElexPData :: (Typeable md, Typeable gq)
               => DM.DesignMatrixRowPart (F.Record ElectionWithDemographicsR)
               -> DMType
               -> ET.VoteShareType
               -> SB.StanBuilderM md gq (SB.RowTypeTag (F.Record ElectionWithDemographicsR)
                                        , DM.DesignMatrixRow (F.Record ElectionWithDemographicsR)
                                        , SB.StanVar, SB.StanVar, SB.StanVar)
setupElexPData densRP dmPrefType vst = do
  elexPData <- SB.dataSetTag @(F.Record ElectionWithDemographicsR) SC.ModelData "ElectionsP"
--  elexPIndex <- SB.indexedConstIntArray elexPData Nothing dsIndex
  dmElexP <- DM.addDesignMatrix elexPData (designMatrixRowElex densRP dmPrefType)
  raceVotesElex <- SB.addCountData elexPData "VotesInRace_Elections"
    $ case vst of
        ET.TwoPartyShare -> (\r -> F.rgetField @DVotes r + F.rgetField @RVotes r)
        ET.FullShare -> F.rgetField @TVotes
  dVotesInRaceElex <- SB.addCountData elexPData "DVotesInRace_Elections" (F.rgetField @DVotes)
  return (elexPData, designMatrixRowElex densRP DMPref, raceVotesElex, dVotesInRaceElex, dmElexP)

data DataSetAlpha = DataSetAlpha | NoDataSetAlpha deriving (Show, Eq)

addModelForDataSet :: (Typeable md, Typeable gq)
                   => Text
                   -> SB.StanBuilderM md gq (SB.RowTypeTag r, DM.DesignMatrixRow r, SB.StanVar, SB.StanVar, SB.StanVar)
                   -> DataSetAlpha
                   -> Maybe (SC.InputDataType -> SB.StanVar -> Maybe Text -> SB.StanBuilderM md gq SB.StanVar)
                   -> SB.StanVar
                   -> SB.StanVar
                   -> SB.LLSet md gq
                   -> SB.StanBuilderM md gq (SC.InputDataType -> SB.StanVar -> Maybe Text -> SB.StanBuilderM md gq SB.StanVar, SB.LLSet md gq)
addModelForDataSet dataSetLabel dataSetupM dataSetAlpha centerM alpha beta llSet = do
  let addLabel x = x <> "_" <> dataSetLabel
  (rtt, designMatrixRow, counts, successes, dm) <- dataSetupM
  dmColIndex <- case dm of
    (SB.StanVar _ (SB.StanMatrix (_, SB.NamedDim ik))) -> return ik
    (SB.StanVar m _) -> SB.stanBuildError $ "addModelForData: dm is not a matrix with named row index"
  invSamples <- SMP.addParameter (addLabel "invSamples") SB.StanReal "<lower=0>" (SB.UnVectorized SB.stdNormal)
  dsIxM <-  case dataSetAlpha of
              NoDataSetAlpha -> return Nothing
              DataSetAlpha -> do
                ix <- SMP.addParameter (addLabel "ix") SB.StanReal "" (SB.UnVectorized SB.stdNormal)
                return $ Just ix
  (dmC, centerF) <- case centerM of
    Nothing -> DM.centerDataMatrix dm Nothing
    Just f -> do
      dmC' <- f SC.ModelData dm Nothing --(Just dataSetLabel)
      return (dmC', f)
  let dist = SB.betaBinomialDist True counts
      dmBetaE dmE betaE = SB.vectorizedOne dmColIndex $ SB.function "dot_product" (dmE :| [betaE])
      muE aE dmE betaE = SB.function "inv_logit" $ one $ aE `SB.plus` dmBetaE dmE betaE
      muT ixM dm = case ixM of
        Nothing -> muE (SB.var alpha) (SB.var dm) (SB.var beta)
        Just ixV -> muE (SB.var ixV `SB.plus` SB.var alpha) (SB.var dm) (SB.var beta)
      betaA ixM is dm = muT ixM dm `SB.divide` SB.var is
      betaB ixM is dm = SB.paren (SB.scalar "1.0" `SB.minus` muT ixM dm) `SB.divide` SB.var is
      vecBetaA = SB.vectorizeExpr (addLabel "betaA") (betaA dsIxM invSamples dmC) (SB.dataSetName rtt)
      vecBetaB = SB.vectorizeExpr (addLabel "betaB") (betaB dsIxM invSamples dmC) (SB.dataSetName rtt)
  SB.inBlock SB.SBModel $ do
    SB.useDataSetForBindings rtt $ do
      betaA <- vecBetaA
      betaB <- vecBetaB
      SB.sampleDistV rtt dist (SB.var betaA, SB.var betaB) successes
  let llDetails =  SB.LLDetails dist (pure (betaA dsIxM invSamples dmC, betaB dsIxM invSamples dmC)) successes
      llSet' = SB.addToLLSet rtt llDetails llSet
      pp = SB.StanVar (addLabel "PP") (SB.StanVector $ SB.NamedDim $ SB.dataSetName rtt)
  SB.useDataSetForBindings rtt
    $ SB.generatePosteriorPrediction rtt pp dist (betaA dsIxM invSamples dmC, betaB dsIxM invSamples dmC)
  return (centerF, llSet')

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
                -> BR.CommandLine
                -> Maybe SC.StanMCParameters
                -> Text
                -> Model
                -> Int
                -> (SB.GroupTypeTag (F.Record ks), Text, SB.GroupSet)
                -> K.ActionWithCacheTime r CCESAndCPSEM
                -> K.ActionWithCacheTime r (F.FrameRec PUMSWithDensityEM, F.FrameRec rs)
                -> K.Sem r (K.ActionWithCacheTime r (ModelCrossTabs, F.FrameRec (ModelResultsR ks)))
electionModelDM clearCaches cmdLine mStanParams modelDir model datYear (psGroup, psDataSetName, psGroupSet) dat_C psDat_C = K.wrapPrefix "stateLegModel" $ do
  K.logLE K.Info $ "(Re-)running DM turnout/pref model if necessary."
  ccesDataRows <- K.ignoreCacheTime $ fmap ccesEMRows dat_C
  let densityMatrixRowPart :: forall x. F.ElemOf x DT.PopPerSqMile => DM.DesignMatrixRowPart (F.Record x)
      densityMatrixRowPart = densityMatrixRowPartFromData (densityTransform model) ccesDataRows
      reportZeroRows :: K.Sem r ()
      reportZeroRows = do
        let numZeroHouseRows = countCCESZeroVoteRows ET.House (voteShareType model) ccesDataRows
            numZeroPresRows = countCCESZeroVoteRows ET.President (voteShareType model) ccesDataRows
        K.logLE K.Diagnostic $ "CCES data has " <> show numZeroHouseRows <> " rows with no house votes and "
          <> show numZeroPresRows <> " rows with no votes for president."
  reportZeroRows
  stElexRows <- K.ignoreCacheTime $ fmap stateElectionRows dat_C
  cdElexRows <- K.ignoreCacheTime $ fmap cdElectionRows dat_C
  let stIncPair r = (F.rcast @[BR.Year, BR.StateAbbreviation] r, realToFrac $ F.rgetField @Incumbency r)
      cdIncPair r =  (F.rcast @[BR.Year, BR.StateAbbreviation, BR.CongressionalDistrict] r, realToFrac $ F.rgetField @Incumbency r)
      presIncMap = FL.fold (FL.premap stIncPair FL.map) $ F.filterFrame ((== ET.President) . F.rgetField @ET.Office) stElexRows
      houseIncMap = FL.fold (FL.premap cdIncPair FL.map) $ F.filterFrame ((== ET.House) . F.rgetField @ET.Office) cdElexRows
      ccesIncF o r = case o of
        ET.President -> fromMaybe 0 $ M.lookup (F.rcast r) presIncMap
        ET.House -> fromMaybe 0 $ M.lookup (F.rcast r) houseIncMap
        _ -> 0
      dmPrefType = if votesFrom model == Set.fromList [ET.President] then DMPresOnlyPref else DMPref
      modelName = "LegDistricts_" <> modelLabel model <> "_HierAlpha_Inc"
      jsonDataName = "DM_" <> dataLabel model <> "_" <> show datYear
      psDataSetName' = psDataSetName <> "_"  <> printDensityTransform (densityTransform model)
      dataAndCodeBuilder :: MRP.BuilderM CCESAndCPSEM (F.FrameRec PUMSWithDensityEM, F.FrameRec rs) ()
      dataAndCodeBuilder = do
        let (dmColIndexT, dmColExprT) = DM.designMatrixColDimBinding $ designMatrixRowCCES densityMatrixRowPart DMTurnout (const 0)
            meanTurnout = 0.6
            logit x = Numeric.log (x / (1 - x))
            logitMeanTurnout = logit meanTurnout
        elexTData <- SB.dataSetTag @(F.Record ElectionWithDemographicsR) SC.ModelData "ElectionsT"
        alphaT <- SB.useDataSetForBindings elexTData $ do
          muAlphaT <- SMP.addParameter "muAlphaT" SB.StanReal "" (SB.UnVectorized $ SB.normal (Just $ SB.scalar $ show logitMeanTurnout) (SB.scalar "1"))
          sigmaAlphaT <- SMP.addParameter "sigmaAlphaT" SB.StanReal "<lower=0>"  (SB.UnVectorized $ SB.normal Nothing (SB.scalar "1"))
          alphaTNonCenterF <- SMP.scalarNonCenteredF muAlphaT sigmaAlphaT
          SMP.addHierarchicalScalar "alphaT" stateGroup (SMP.NonCentered alphaTNonCenterF) SB.stdNormal
--          pure muAlphaT
        thetaT <- SB.useDataSetForBindings elexTData $ do
          SB.addDeclBinding' dmColIndexT dmColExprT
          SB.addUseBinding' dmColIndexT dmColExprT
          muThetaT <- SMP.addParameter "muThetaT" (SB.StanVector $ SB.NamedDim dmColIndexT) "" (SB.Vectorized (one dmColIndexT) SB.stdNormal)
--          tauThetaT <- SMP.addParameter "tauThetaT" (SB.StanVector $ SB.NamedDim dmColIndexT) "<lower=0>" (SB.Vectorized (one dmColIndexT) SB.stdNormal)
--          corrThetaT <- SMP.lkjCorrelationMatrixParameter "corrT" dmColIndexT 4
--          thetaTNonCenteredF <- SMP.vectorNonCenteredF (SB.taggedGroupName stateGroup) muThetaT tauThetaT corrThetaT
--          SMP.addHierarchicalVector "thetaT" dmColIndexT stateGroup (SMP.NonCentered thetaTNonCenteredF) SB.stdNormal
          pure muThetaT
        (centerTF, llSetT1) <- addModelForDataSet "ElexT" (setupElexTData densityMatrixRowPart) NoDataSetAlpha Nothing alphaT thetaT SB.emptyLLSet
        (_, llSetT2) <- addModelForDataSet "CPST" (setupCPSData densityMatrixRowPart) DataSetAlpha (Just centerTF) alphaT thetaT llSetT1
        (_, llSetT) <- addModelForDataSet "CCEST" (setupCCESTData densityMatrixRowPart) DataSetAlpha Nothing alphaT thetaT llSetT2

        elexPData <- SB.dataSetTag @(F.Record ElectionWithDemographicsR) SC.ModelData "ElectionsP"
        let (dmColIndexP, dmColExprP) = DM.designMatrixColDimBinding $ designMatrixRowCCES densityMatrixRowPart dmPrefType (const 0)
        alphaP <- SB.useDataSetForBindings elexPData $ do
          muAlphaP <- SMP.addParameter "muAlphaP" SB.StanReal "" (SB.UnVectorized SB.stdNormal)
          sigmaAlphaP <- SMP.addParameter "sigmaAlphaP" SB.StanReal "<lower=0>"  (SB.UnVectorized $ SB.normal Nothing (SB.scalar "1"))
          alphaPNonCenterF <- SMP.scalarNonCenteredF muAlphaP sigmaAlphaP
          SMP.addHierarchicalScalar "alphaP" stateGroup (SMP.NonCentered alphaPNonCenterF) SB.stdNormal
--          pure muAlphaP
        thetaP <- SB.useDataSetForBindings elexPData $ do
          SB.addDeclBinding' dmColIndexP dmColExprP
          SB.addUseBinding' dmColIndexP dmColExprP
          muThetaP <- SMP.addParameter "muThetaP" (SB.StanVector $ SB.NamedDim dmColIndexP) "" (SB.Vectorized (one dmColIndexP) SB.stdNormal)
--          tauThetaP <- SMP.addParameter "tauThetaP" (SB.StanVector $ SB.NamedDim dmColIndexP) "<lower=0>" (SB.Vectorized (one dmColIndexP) SB.stdNormal)
--          corrThetaP <- SMP.lkjCorrelationMatrixParameter "corrP" dmColIndexP 4
--          thetaPNonCenteredF <- SMP.vectorNonCenteredF (SB.taggedGroupName stateGroup) muThetaP tauThetaP corrThetaP
--          SMP.addHierarchicalVector "thetaP" dmColIndexP stateGroup (SMP.NonCentered thetaPNonCenteredF) SB.stdNormal
          pure muThetaP
        (centerPF, llSetP1) <- addModelForDataSet "ElexP" (setupElexPData densityMatrixRowPart dmPrefType (voteShareType model)) NoDataSetAlpha Nothing alphaP thetaP SB.emptyLLSet
        let ccesP (centerFM, llS) office = do
              (centerF, llS) <- addModelForDataSet
                                ("CCESP" <> show office)
                                (setupCCESPData densityMatrixRowPart dmPrefType ccesIncF office (voteShareType model))
                                DataSetAlpha
                                centerFM
                                alphaP
                                thetaP
                                llS
              return (Just centerF, llS)
            llFoldM = FL.FoldM ccesP (return (Nothing, llSetP1)) return
        (_, llSetP) <- FL.foldM llFoldM (votesFrom model)
        SB.generateLogLikelihood' $ SB.mergeLLSets llSetT llSetP

        -- post-stratification for crosstabs
        -- NB: invSamples does not matter for expectations
        let  dmThetaE ci dmE thetaE = SB.vectorizedOne ci $ SB.function "dot_product" (dmE :| [thetaE])
             muE ci aE dmE thetaE = SB.function "inv_logit" $ one $ aE `SB.plus` dmThetaE ci dmE thetaE
             mu ci alpha dm theta =  muE ci (SB.var alpha) (SB.var dm) (SB.var theta)
             betaA ci alpha dm theta = mu ci alpha dm theta
             betaB ci alpha dm theta =  SB.paren (SB.scalar "1.0" `SB.minus` mu ci alpha dm theta)
             betaAT dm = betaA dmColIndexT alphaT dm thetaT
             betaBT dm = betaB dmColIndexT alphaT dm thetaT
             betaAP dm = betaA dmColIndexP alphaP dm thetaP
             betaBP dm = betaB dmColIndexP alphaP dm thetaP
        acsData <- SB.dataSetTag @(F.Record PUMSWithDensityEM) SC.GQData "ACS"
        dmACS_T' <- DM.addDesignMatrix acsData (designMatrixRowACS densityMatrixRowPart DMTurnout)
        dmACS_P' <- DM.addDesignMatrix acsData (designMatrixRowACS densityMatrixRowPart dmPrefType)
        dmACS_T <- SB.useDataSetForBindings acsData $ centerTF SC.GQData dmACS_T' (Just "T")
        dmACS_P <- SB.useDataSetForBindings acsData $ centerPF SC.GQData dmACS_P' (Just "P")
        let psTPrecompute = do
              betaA' <- SB.vectorizeExpr "acsBetaAT" (betaAT dmACS_T) (SB.dataSetName acsData)
              betaB' <- SB.vectorizeExpr "acsBetaBT" (betaBT dmACS_T) (SB.dataSetName acsData)
              return (betaA', betaB')
            psTExpr :: (SB.StanVar, SB.StanVar) -> SB.StanBuilderM md gq SB.StanExpr
            psTExpr (bA, bB) =  pure $ SB.betaMu (SB.var bA) (SB.var bB)
            psPPrecompute = do
              betaA' <- SB.vectorizeExpr "acsBetaAP" (betaAP dmACS_P) (SB.dataSetName acsData)
              betaB' <- SB.vectorizeExpr "acsBetaBP" (betaBP dmACS_P) (SB.dataSetName acsData)
              return (betaA', betaB')
            psPExpr :: (SB.StanVar, SB.StanVar) -> SB.StanBuilderM md gq SB.StanExpr
            psPExpr (bA, bB) =  pure $ SB.betaMu (SB.var bA) (SB.var bB)
            psDVotePreCompute = do
              betaTs <- psTPrecompute
              betaPs <- psPPrecompute
              return (betaTs, betaPs)
            psDVoteExpr :: ((SB.StanVar, SB.StanVar), (SB.StanVar, SB.StanVar))  -> SB.StanBuilderM md gq SB.StanExpr
            psDVoteExpr ((bAT, bBT), (bAP, bBP)) = do
              pT <- SB.stanDeclareRHS "pT" SB.StanReal "" $ SB.betaMu (SB.var bAT) (SB.var bBT)
              pD <- SB.stanDeclareRHS "pD" SB.StanReal "" $ SB.betaMu (SB.var bAP) (SB.var bBP)
              pure $ SB.var pT `SB.times` SB.var pD

            turnoutPS = ((psTPrecompute, psTExpr), Nothing)
            prefPS = ((psPPrecompute, psPExpr), Nothing)
            dVotePS = ((psDVotePreCompute, psDVoteExpr), Just $ SB.name "pT")
            psACS :: (Typeable md, Typeable gq, Ord k)
                  => Text
                  -> ((SB.StanBuilderM md gq x, x -> SB.StanBuilderM md gq SB.StanExpr), Maybe SB.StanExpr)
                  -> SB.GroupTypeTag k -> SB.StanBuilderM md gq SB.StanVar
            psACS name psCalcs grp =
              MRP.addPostStratification
              (fst psCalcs)
              (Just name)
              elexTData
              acsData
              (realToFrac . F.rgetField @PUMS.Citizens)
              (MRP.PSShare $ snd psCalcs)
              (Just grp)
        psACS "Turnout" turnoutPS raceGroup
        psACS "Turnout" turnoutPS educationGroup
        psACS "Turnout" turnoutPS sexGroup
        psACS "Turnout" turnoutPS stateGroup
        psACS "Pref" prefPS raceGroup
        psACS "Pref" prefPS educationGroup
        psACS "Pref" prefPS sexGroup
        psACS "Pref" prefPS stateGroup
        psACS "DVote" dVotePS raceGroup
        psACS "DVote" dVotePS educationGroup
        psACS "DVote" dVotePS sexGroup
        psACS "DVote" dVotePS stateGroup

        -- post-stratification for results
        psData <- SB.dataSetTag @(F.Record rs) SC.GQData "DistrictPS"
        dmPS_T' <- DM.addDesignMatrix psData (designMatrixRowPS densityMatrixRowPart DMTurnout)
        dmPS_P' <- DM.addDesignMatrix psData (designMatrixRowPS densityMatrixRowPart dmPrefType)

        let psPreCompute = do
              dmPS_T <- centerTF SC.GQData dmPS_T' (Just "T")
              dmPS_P <- centerPF SC.GQData dmPS_P' (Just "P")
              psBetaAT <- SB.vectorizeExpr "psBetaAT" (betaAT dmPS_T) (SB.dataSetName psData)
              psBetaBT <- SB.vectorizeExpr "psBetaBT" (betaBT dmPS_T) (SB.dataSetName psData)
              psBetaAP <- SB.vectorizeExpr "psBetaAP" (betaAP dmPS_P) (SB.dataSetName psData)
              psBetaBP <- SB.vectorizeExpr "psBetaBP" (betaBP dmPS_P) (SB.dataSetName psData)
              pure (psBetaAT, psBetaBT, psBetaAP, psBetaBP)

            psExprF (bAT, bBT, bAP, bBP) = do --(psT_v, psP_v) = do
              pT <- SB.stanDeclareRHS "pT" SB.StanReal "" $ SB.betaMu (SB.var bAT) (SB.var bBT)
              pD <- SB.stanDeclareRHS "pD" SB.StanReal "" $ SB.betaMu (SB.var bAP) (SB.var bBP)
              pure $ SB.var pT `SB.times` SB.var pD

        let postStrat =
              MRP.addPostStratification -- @(CCESAndPUMS, F.FrameRec rs)
              (psPreCompute, psExprF)
              Nothing
              elexTData
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
                dVoteRM <- resultsMap acsRowTag  g "DVote_ACS_"
                let rmTP = M.merge M.dropMissing M.dropMissing (M.zipWithMatched $ \_ x y -> (x,y)) turnoutRM prefRM
                    rmTPD = M.merge M.dropMissing M.dropMissing (M.zipWithMatched $ \_ (x, y) z -> (x,y,z)) rmTP dVoteRM
                    g (k, (t, p, d)) = do
                      tCI <- MT.listToCI t
                      pCI <- MT.listToCI p
                      dCI <- MT.listToCI d
                      return $ datYear F.&: k F.&: dataLabel model F.&: tCI F.&: pCI F.&: dCI F.&: V.RNil
                K.knitEither $ F.toFrame <$> (traverse g $ M.toList rmTPD)
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
  let unwrapVoted = [SR.UnwrapNamed "Voted_Elections" "yElectionsVotes", SR.UnwrapNamed "Voted_CPS" "yCPSVotes", SR.UnwrapNamed "Voted_CCES" "yCCESVotes"]
  let unwrapDVotes = [SR.UnwrapNamed "DVotesInRace_Elections" "yElectionsDVotes"]
                     ++ if (Set.member ET.President $ votesFrom model) then [SR.UnwrapNamed "DVotesInRace_CCES_President" "yCCESDVotesPresident"] else []
                     ++ if (Set.member ET.House $ votesFrom model) then [SR.UnwrapNamed "DVotesInRace_CCES_House" "yCCESDVotesHouse"] else []


  (dw, stanCode) <- dataWranglerAndCode dat_C psDat_C
  fmap (secondF FS.unSFrame)
    $ MRP.runMRPModel
    clearCaches
    (SC.RunnerInputNames modelDir modelName (Just psDataSetName') jsonDataName)
    (fromMaybe (SC.StanMCParameters 4 4 (Just 1000) (Just 1000) (Just 0.9) (Just 15) Nothing) mStanParams)
    (BR.clStanParallel cmdLine)
    dw
    stanCode
    (MRP.Both $ unwrapVoted ++ unwrapDVotes)
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
                      | BinDensity Int Int
                      deriving (Eq, Ord, Generic)
instance Flat.Flat DensityTransform
type instance FI.VectorFor DensityTransform = Vector.Vector

printDensityTransform :: DensityTransform -> Text
printDensityTransform RawDensity = "RawDensity"
printDensityTransform LogDensity = "LogDensity"
printDensityTransform (BinDensity bins range) = "BinDensity_" <> show bins <> "_" <> show range

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

--data VoteSource = HouseVotes | PresVot es | CompositeVotes deriving (Show, Eq, Ord, Generic)
--instance Flat.Flat VoteSource
--type instance FI.VectorFor VoteSource = Vector.Vector

--printVoteSource :: CCESVoteSource -> Text
--printVoteSource CCESHouse = "HouseVotes"
--printVoteSource CCESPres = "PresVotes"
--printVoteSource CCESComposite = "CompositeVotes"


getCCESVotes :: (F.ElemOf rs HouseVotes
                , F.ElemOf rs AHHouseDVotes
                , F.ElemOf rs AHHouseRVotes
                , F.ElemOf rs PresVotes
                , F.ElemOf rs AHPresDVotes
                , F.ElemOf rs AHPresRVotes
                )
             => ET.OfficeT -> ET.VoteShareType -> (F.Record rs -> Double, F.Record rs -> Double)
getCCESVotes ET.House ET.FullShare = (realToFrac . F.rgetField @HouseVotes, F.rgetField @AHHouseDVotes)
getCCESVotes ET.House ET.TwoPartyShare = (\r -> F.rgetField @AHHouseDVotes r + F.rgetField @AHHouseRVotes r, F.rgetField @AHHouseDVotes)
getCCESVotes ET.President ET.FullShare = (realToFrac . F.rgetField @PresVotes, F.rgetField @AHPresDVotes)
getCCESVotes ET.President ET.TwoPartyShare = (\r ->  F.rgetField @AHPresDVotes r + F.rgetField @AHPresRVotes r, F.rgetField @AHPresDVotes)
getCCESVotes _ _ = (const 0, const 0) -- we shouldn't call this

zeroCCESVotes ::  (F.ElemOf rs HouseVotes
                  , F.ElemOf rs PresVotes
                  , F.ElemOf rs AHHouseDVotes
                  , F.ElemOf rs AHHouseRVotes
                  , F.ElemOf rs AHPresDVotes
                  , F.ElemOf rs AHPresRVotes
                  )
              => ET.OfficeT -> ET.VoteShareType -> F.Record rs -> Bool
zeroCCESVotes office vst r = votesInRace r == 0 where
  (votesInRace, _) = getCCESVotes office vst

countCCESZeroVoteRows :: (F.ElemOf rs HouseVotes
                         , F.ElemOf rs PresVotes
                         , F.ElemOf rs AHHouseDVotes
                         , F.ElemOf rs AHHouseRVotes
                         , F.ElemOf rs AHPresDVotes
                         , F.ElemOf rs AHPresRVotes
                         , Foldable f
                         )
                      => ET.OfficeT -> ET.VoteShareType -> f (F.Record rs) -> Int
countCCESZeroVoteRows office vst = FL.fold (FL.prefilter (zeroCCESVotes office vst) FL.length)

data Model = Model { voteShareType :: ET.VoteShareType
                   , votesFrom :: Set ET.OfficeT
                   , densityTransform :: DensityTransform
                   }  deriving (Generic)

--instance Flat.Flat (Model tr pr)
--type instance FI.VectorFor (Model tr pr) = Vector.Vector

modelLabel :: Model  -> Text
modelLabel m = show (voteShareType m) <> "_" <> T.intercalate "+" (show <$> Set.toList (votesFrom m)) <> "_" <> printDensityTransform (densityTransform m)

dataLabel :: Model -> Text
dataLabel = modelLabel

type SLDLocation = (Text, ET.DistrictType, Int)

sldLocationToRec :: SLDLocation -> F.Record [BR.StateAbbreviation, ET.DistrictTypeC, ET.DistrictNumber]
sldLocationToRec (sa, dt, dn) = sa F.&: dt F.&: dn F.&: V.RNil

type ModeledShare = "ModeledShare" F.:-> MT.ConfidenceInterval
type ModeledTurnout = "ModeledTurnout" F.:-> MT.ConfidenceInterval
type ModeledPref = "ModeledPref" F.:-> MT.ConfidenceInterval
type ModelDesc = "ModelDescription" F.:-> Text

type ModelResultsR ks  = '[BR.Year] V.++ ks V.++ '[ModelDesc, ModeledShare]

type CrossTabFrame k  = F.FrameRec [BR.Year, k, ModelDesc, ModeledTurnout, ModeledPref, ModeledShare]

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


densityMatrixRowPartFromData :: forall rs rs'.(F.ElemOf rs DT.PopPerSqMile, F.ElemOf rs' DT.PopPerSqMile)
                         => DensityTransform
                         -> F.FrameRec rs'
                         -> DM.DesignMatrixRowPart (F.Record rs)
densityMatrixRowPartFromData RawDensity _ = (DM.DesignMatrixRowPart "Density" 1 f)
  where
   f = VU.fromList . pure . F.rgetField @DT.PopPerSqMile
densityMatrixRowPartFromData LogDensity _ =
  (DM.DesignMatrixRowPart "Density" 1 logDensityPredictor)
densityMatrixRowPartFromData (BinDensity bins range) dat = DM.DesignMatrixRowPart "Density" 1 f where
  sortedData = List.sort $ FL.fold (FL.premap (F.rgetField @DT.PopPerSqMile) FL.list) dat
  quantileSize = List.length sortedData `div` bins
  quantilesExtra = List.length sortedData `rem` bins
  quantileMaxIndex k = quantilesExtra + k * quantileSize - 1 -- puts extra in 1st bucket
  quantileBreaks = fmap (\k -> sortedData List.!! quantileMaxIndex k) $ [1..bins]
  indexedBreaks = zip quantileBreaks $ fmap (\k -> (realToFrac $ k * range)/(realToFrac bins)) [1..bins] -- should this be 0 centered??
  go x [] = realToFrac range
  go x ((y, k): xs) = if x < y then k else go x xs
  quantileF x = go x indexedBreaks
  g x = VU.fromList [quantileF x]
  f = g . F.rgetField @DT.PopPerSqMile


--  SB.MatrixRowFromData "Density" 1


--densityRowFromData = SB.MatrixRowFromData "Density" 1 densityPredictor
logDensityPredictor :: F.ElemOf rs DT.PopPerSqMile => F.Record rs -> VU.Vector Double
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

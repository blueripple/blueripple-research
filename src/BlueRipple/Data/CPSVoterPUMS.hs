{-# LANGUAGE AllowAmbiguousTypes  #-}
{-# LANGUAGE ConstraintKinds      #-}
{-# LANGUAGE DataKinds            #-}
{-# LANGUAGE PolyKinds            #-}
{-# LANGUAGE GADTs                #-}
{-# LANGUAGE FlexibleContexts     #-}
{-# LANGUAGE FlexibleInstances    #-}
{-# LANGUAGE OverloadedStrings    #-}
{-# LANGUAGE TypeFamilies         #-}
{-# LANGUAGE TypeOperators        #-}
{-# LANGUAGE TypeApplications     #-}
{-# LANGUAGE Rank2Types           #-}
{-# LANGUAGE ScopedTypeVariables  #-}
{-# LANGUAGE UndecidableInstances       #-}
{-# OPTIONS_GHC -O0 -freduction-depth=0 #-}
module BlueRipple.Data.CPSVoterPUMS
  (
    cpsVoterPUMSLoader
  , cpsVoterPUMSWithCDLoader
  , CPSVoterPUMS
  , CPSVoterPUMSWeight
  , cpsVoterPUMSRollup
  , cpsVoterPUMSRollupWeightedCounts
  , cpsVoterPUMSElectoralWeights
  , cpsVoterPUMSElectoralWeightsByCD
  , cpsVoterPUMSElectoralWeightsByState
  , cpsVoterPUMSNationalElectoralWeights
  , cpsCountVotersByStateF
--  , cpsKeysToASE
--  , cpsKeysToASR
  , cpsKeysToASER
--  , cpsKeysToASER4
  , cpsKeysToASER5
  , cpsKeysToASER4H
  , cpsKeysToCASER4H
--  , cpsKeysToIdentity
  , cpsPossibleVoter
  , cpsVoted

  ) where

import qualified BlueRipple.Data.CPSVoterPUMS.CPSVoterPUMS_Frame as CPS
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.ElectionTypes as ET
import qualified BlueRipple.Data.GeographicTypes as GT
import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Data.LoadersCore as BR
import qualified BlueRipple.Data.Loaders as BR
import qualified BlueRipple.Data.Keyed as BR
import qualified BlueRipple.Utilities.KnitUtils as BR
import qualified BlueRipple.Data.CountFolds as BRCF

import qualified Control.Foldl                 as FL
import qualified Data.Text                     as T
import Data.Type.Equality (type (~))
import qualified Data.Vinyl                    as V
import           Data.Vinyl.TypeLevel                     (type (++))
import qualified Data.Vinyl.TypeLevel          as V
import qualified Frames                        as F
import qualified Frames.Streamly.InCore        as FI
import qualified Frames.Melt                   as F

import qualified Frames.MapReduce              as FMR
import qualified Frames.Transform              as FT
import qualified Frames.SimpleJoins            as FJ

import qualified Knit.Report as K

cpsVoterPUMSLoader :: (K.KnitEffects r, BR.CacheEffects r)
                   => K.Sem r (K.ActionWithCacheTime r (F.FrameRec CPSVoterPUMS))
cpsVoterPUMSLoader = do
  let cpsPUMSDataPath = BR.LocalData $ T.pack CPS.cpsVoterPUMSCSV
  cachedStateAbbrCrosswalk <- BR.stateAbbrCrosswalkLoader
  cachedCPSDataPath <- K.liftKnit $ BR.dataPathWithCacheTime cpsPUMSDataPath
  let cachedDeps = (,) <$> cachedStateAbbrCrosswalk <*> cachedCPSDataPath
  BR.retrieveOrMakeFrame "data/cpsVoterPUMSWithAbbrs.bin" cachedDeps $ \(stateAbbrCrosswalk, dataPath) -> do
    let filterF r = (F.rgetField @CPS.CPSAGE r >= 18) && (F.rgetField @CPS.CPSCITIZEN r /= 5)
    withoutAbbr <- K.ignoreCacheTimeM
                   $ BR.cachedFrameLoader @(F.RecordColumns CPS.CPSVoterPUMS_Raw) @CPSVoterPUMS'
                   dataPath
                   Nothing
                   (Just filterF)
                   transformCPSVoterPUMSRow
                   Nothing
                   "cpsVoterPUMS.bin"
    fmap (fmap F.rcast) (K.knitMaybe "missing state abbreviation in state abbreviation crosswalk"
                          $ FJ.leftJoinM @'[GT.StateFIPS] withoutAbbr stateAbbrCrosswalk)


-- NB: This should not be used for state-level rollup since some rows will be duplicated if the county is in more than one CD.
cpsVoterPUMSWithCDLoader :: (K.KnitEffects r, BR.CacheEffects r)
                          => K.Sem r (K.ActionWithCacheTime r (F.FrameRec (CPSVoterPUMS V.++ [BR.CongressionalDistrict, BR.CountyWeight])))
cpsVoterPUMSWithCDLoader = do
  cachedCPSVoterPUMS <- cpsVoterPUMSLoader
  cachedCountyToCD <- BR.county2010ToCD116Loader
  let cachedDeps = (,) <$> cachedCPSVoterPUMS <*> cachedCountyToCD
  BR.retrieveOrMakeFrame "data/cpsVoterPUMSWithAbbrsAndCDs.bin" cachedDeps $ \(cpsVoterPUMS, countyToCD) -> do
    K.knitEither . either (Left . show) Right
      $ FJ.leftJoinE
      @[BR.StateFIPS, BR.CountyFIPS]
      (F.filterFrame ((> 0) . F.rgetField @BR.CountyFIPS) cpsVoterPUMS)
      (fmap (F.rcast @[BR.CountyFIPS, BR.StateFIPS, BR.CongressionalDistrict, BR.CountyWeight]) countyToCD)


type CPSVoterPUMSWeight = "CPSVoterPUMSWeight" F.:-> Double

type CPSVoterPUMS = '[ BR.Year
                     , GT.StateFIPS
                     , GT.StateAbbreviation
                     , GT.CountyFIPS
                     , DT.CitizenC
                     , DT.Age4C
                     , DT.SexC
                     , DT.RaceAlone4C
                     , DT.HispC
                     , DT.CollegeGradC
                     , DT.InCollege
                     , ET.VoteWhyNotC
                     , ET.RegWhyNotC
                     , ET.VoteHowC
                     , ET.VoteWhenC
                     , ET.VotedYNC
                     , ET.RegisteredYNC
                     , CPSVoterPUMSWeight
                     ]

type CPSVoterPUMS' = V.RDelete GT.StateAbbreviation CPSVoterPUMS


cpsVoterPUMSRollup
  :: forall cs rs ks ds.
  ( cs F.⊆ rs
  , ks F.⊆ (ks V.++ cs)
  , cs F.⊆ (ks V.++ cs)
  , FI.RecVec (ks V.++ ds)
  , Ord (F.Record ks)
  )
  => (F.Record rs -> F.Record ks)
  -> FL.Fold (F.Record cs) (F.Record ds)
  -> FL.Fold (F.Record rs) (F.FrameRec (ks V.++ ds))
cpsVoterPUMSRollup getKeys foldData =
  FMR.concatFold
  $ FMR.mapReduceFold
  (FMR.Unpack $ \r -> [getKeys r `V.rappend` F.rcast @cs r])
  (FMR.assignKeysAndData @ks @cs)
  (FMR.foldAndAddKey foldData)

cpsVoterPUMSRollupWithDefault
  ::  forall cs rs ls ks ds.
  ( cs F.⊆ rs
  , (ls V.++ ks) F.⊆ (ls V.++ ks V.++ cs)
  , cs F.⊆ (ls V.++ ks V.++ cs)
  , FI.RecVec (ls V.++ ks V.++ ds)
  , Ord (F.Record (ls V.++ ks))
  , BR.FiniteSet (F.Record ks)
  , (ls V.++ (ks V.++ ds)) ~ ((ls V.++ ks) V.++ ds)
  , Ord (F.Record ls)
  , ls F.⊆ ((ls V.++ ks) V.++ ds)
  , (ks V.++ ds) F.⊆ ((ls V.++ ks) V.++ ds)
  , ks F.⊆ (ks V.++ ds)
  , ds F.⊆ (ks V.++ ds)
  )
  => (F.Record rs -> F.Record ls)
  -> (F.Record rs -> F.Record ks)
  -> FL.Fold (F.Record cs) (F.Record ds)
  -> F.Record ds -- default value if there are no records for a given value of F.Record ks
  -> FL.Fold (F.Record rs) (F.FrameRec (ls V.++ ks V.++ ds))
cpsVoterPUMSRollupWithDefault getLoc getKey foldData defD =
  let addDefaultF = FMR.concatFold
                    $ FMR.mapReduceFold
                    FMR.noUnpack
                    (FMR.assignKeysAndData @ls @(ks V.++ ds))
                    (FMR.makeRecsWithKey id
                     $ FMR.ReduceFold
                     $ const
                     $ BR.addDefaultRec @ks defD
                    )
  in FL.fold addDefaultF <$> cpsVoterPUMSRollup (\r -> getLoc r `V.rappend` getKey r) foldData

cpsVoterPUMSRollupWeightedCounts
  :: forall cs rs ls ks ds.
  ( cs F.⊆ rs
  , (ls V.++ ks) F.⊆ (ls V.++ ks V.++ cs)
  , cs F.⊆ (ls V.++ ks V.++ cs)
  , FI.RecVec (ls V.++ ks V.++ ds)
  , Ord (F.Record (ls V.++ ks))
  , (ls V.++ (ks V.++ ds)) ~ ((ls V.++ ks) V.++ ds)
  , BR.FiniteSet (F.Record ks)
  , Ord (F.Record ls)
  , ls F.⊆ ((ls V.++ ks) V.++ ds)
  , (ks V.++ ds) F.⊆ ((ls V.++ ks) V.++ ds)
  , ks F.⊆ (ks V.++ ds)
  , ds F.⊆ (ks V.++ ds)
  )
  => (F.Record rs -> F.Record ls)
  -> (F.Record rs -> F.Record ks)
  -> (F.Record cs -> Bool) -- which to include
  -> (F.Record cs -> Bool) -- which to count
  -> (F.Record cs -> Double) -- get weight
  -> (Double -> F.Record ds) -- turn the weighted counts into a record
  -> F.Record ds -- default
  -> FL.Fold (F.Record rs) (F.FrameRec (ls V.++ ks V.++ ds))
cpsVoterPUMSRollupWeightedCounts getLoc getKey filterData countIf wgt countToRec defD =
  let countF :: FL.Fold (F.Record cs) (F.Record ds)
      countF =
        let wgtdAllF = FL.premap wgt FL.sum
            wgtdCountF = FL.prefilter countIf $ FL.premap wgt FL.sum
            safeDiv n d = if d > 0 then n / d else 0
            wF = safeDiv <$> wgtdCountF <*> wgtdAllF -- this is acceptable here since (d == 0) iff (n == 0)
        in fmap countToRec wF
      ewFold :: FL.Fold (F.Record cs) (F.Record ds)
      ewFold = FL.prefilter filterData countF
  in cpsVoterPUMSRollupWithDefault getLoc getKey ewFold defD

cpsVoterPUMSElectoralWeights
  :: forall cs rs ls ks.
  ((ls V.++ ks) F.⊆ (ls V.++ ks V.++ cs)
  , cs F.⊆ (ls V.++ ks V.++ cs)
  , cs F.⊆ rs
  , F.ElemOf cs DT.CitizenC
  , F.ElemOf cs ET.VotedYNC
  , FI.RecVec (ls V.++ ks V.++ ET.EWCols)
  , (ls V.++ (ks V.++ ET.EWCols)) ~ ((ls V.++ ks) V.++ ET.EWCols)
  , Ord (F.Record (ls V.++ ks))
  , Ord (F.Record ls)
  , BR.FiniteSet (F.Record ks)
  , ls F.⊆ ((ls V.++ ks) V.++ ET.EWCols)
  , (ks V.++ ET.EWCols) F.⊆ ((ls V.++ ks) V.++ ET.EWCols)
  , ks F.⊆ (ks V.++ ET.EWCols)
  , ET.EWCols F.⊆ (ks V.++ ET.EWCols)
  )
  => (F.Record rs -> F.Record ls)
  -> (F.Record rs -> F.Record ks)
  -> (F.Record cs -> Double)
  -> FL.Fold (F.Record rs) (F.FrameRec (ls V.++ ks V.++ ET.EWCols))
cpsVoterPUMSElectoralWeights getLoc getKey getWgt =
  let toRec :: Double -> F.Record ET.EWCols
      toRec w =  ET.EW_Census F.&: ET.EW_Citizen F.&: w F.&: V.RNil
      citizen r = F.rgetField @DT.CitizenC r == DT.Citizen
      possibleVoter r = cpsPossibleVoter $ F.rgetField @ET.VotedYNC r
      voted r = cpsVoted $ F.rgetField @ET.VotedYNC r
  in cpsVoterPUMSRollupWeightedCounts @cs
     getLoc
     getKey
     (\r -> citizen r &&  possibleVoter r)
     voted
     getWgt
     toRec
     (toRec 0) -- this is a reasonable default to use for inference but not great to use this data directly


cpsVoterPUMSElectoralWeightsByState
  :: forall ks.
  (F.ElemOf (ks ++ '[DT.CitizenC, ET.VotedYNC, CPSVoterPUMSWeight]) CPSVoterPUMSWeight
  , F.ElemOf (ks ++ '[DT.CitizenC, ET.VotedYNC, CPSVoterPUMSWeight]) DT.CitizenC
  , F.ElemOf (ks ++ '[DT.CitizenC, ET.VotedYNC, CPSVoterPUMSWeight]) ET.VotedYNC
  , FI.RecVec (ks ++ ET.EWCols)
  , Ord (F.Record ks)
  , ks F.⊆ ('[BR.Year, GT.StateAbbreviation, BR.StateFIPS] V.++ ks V.++ '[DT.CitizenC, ET.VotedYNC, CPSVoterPUMSWeight])
  , BR.FiniteSet (F.Record ks)
  , F.ElemOf (ks V.++ ET.EWCols) ET.ElectoralWeight
  , F.ElemOf (ks V.++ ET.EWCols) ET.ElectoralWeightOf
  , F.ElemOf (ks V.++ ET.EWCols) ET.ElectoralWeightSource
  , (ks V.++ ET.EWCols) F.⊆ ('[BR.Year, GT.StateAbbreviation, BR.StateFIPS] V.++ ks V.++ ET.EWCols)
  , ks F.⊆ (ks V.++ ET.EWCols)
  )
  => (F.Record CPSVoterPUMS -> F.Record ks)
  -> FL.Fold (F.Record CPSVoterPUMS) (F.FrameRec ('[BR.Year, GT.StateAbbreviation, BR.StateFIPS] V.++ ks V.++ ET.EWCols))
cpsVoterPUMSElectoralWeightsByState getCatKey =
  cpsVoterPUMSElectoralWeights @[DT.CitizenC, ET.VotedYNC, CPSVoterPUMSWeight]
  (F.rcast @[BR.Year, GT.StateAbbreviation, BR.StateFIPS]) getCatKey (F.rgetField @CPSVoterPUMSWeight)

cpsVoterPUMSNationalElectoralWeights
  :: forall ks.
  (F.ElemOf (ks ++ '[DT.CitizenC, ET.VotedYNC, CPSVoterPUMSWeight]) CPSVoterPUMSWeight
  , F.ElemOf (ks ++ '[DT.CitizenC, ET.VotedYNC, CPSVoterPUMSWeight]) DT.CitizenC
  , F.ElemOf (ks ++ '[DT.CitizenC, ET.VotedYNC, CPSVoterPUMSWeight]) ET.VotedYNC
  , FI.RecVec (ks ++ ET.EWCols)
  , Ord (F.Record ks)
  , ks F.⊆ ('[BR.Year] V.++ ks V.++ '[DT.CitizenC, ET.VotedYNC, CPSVoterPUMSWeight])
  , BR.FiniteSet (F.Record ks)
  , F.ElemOf (ks V.++ ET.EWCols) ET.ElectoralWeight
  , F.ElemOf (ks V.++ ET.EWCols) ET.ElectoralWeightOf
  , F.ElemOf (ks V.++ ET.EWCols) ET.ElectoralWeightSource
  , (ks V.++ ET.EWCols) F.⊆ ('[BR.Year] V.++ ks V.++ ET.EWCols)
  , ks F.⊆ (ks V.++ ET.EWCols)
  )
  => (F.Record CPSVoterPUMS -> F.Record ks)
  -> FL.Fold (F.Record CPSVoterPUMS) (F.FrameRec ('[BR.Year] V.++ ks V.++ ET.EWCols))
cpsVoterPUMSNationalElectoralWeights getCatKey =
  cpsVoterPUMSElectoralWeights @[DT.CitizenC, ET.VotedYNC, CPSVoterPUMSWeight] (F.rcast @'[BR.Year]) getCatKey (F.rgetField @CPSVoterPUMSWeight)

cpsVoterPUMSElectoralWeightsByCD
  :: forall ks.
  (F.ElemOf (ks ++ '[DT.CitizenC, ET.VotedYNC, CPSVoterPUMSWeight, BR.CountyWeight]) CPSVoterPUMSWeight
  , F.ElemOf (ks ++ '[DT.CitizenC, ET.VotedYNC, CPSVoterPUMSWeight, BR.CountyWeight]) BR.CountyWeight
  , F.ElemOf (ks ++ '[DT.CitizenC, ET.VotedYNC, CPSVoterPUMSWeight, BR.CountyWeight]) DT.CitizenC
  , F.ElemOf (ks ++ '[DT.CitizenC, ET.VotedYNC, CPSVoterPUMSWeight, BR.CountyWeight]) ET.VotedYNC
  , FI.RecVec (ks ++ ET.EWCols)
  , Ord (F.Record ks)
  , ks F.⊆ ('[BR.Year, GT.StateAbbreviation, GT.StateFIPS, GT.CongressionalDistrict]
             V.++ ks
             V.++ '[DT.CitizenC, ET.VotedYNC, CPSVoterPUMSWeight, BR.CountyWeight])
  , BR.FiniteSet (F.Record ks)
  , F.ElemOf (ks V.++ ET.EWCols) ET.ElectoralWeight
  , F.ElemOf (ks V.++ ET.EWCols) ET.ElectoralWeightOf
  , F.ElemOf (ks V.++ ET.EWCols) ET.ElectoralWeightSource
  , (ks V.++ ET.EWCols) F.⊆ ('[BR.Year, GT.StateAbbreviation, GT.StateFIPS, GT.CongressionalDistrict] V.++ ks V.++ ET.EWCols)
  , ks F.⊆ (ks V.++ ET.EWCols)
  )
  => (F.Record (CPSVoterPUMS V.++ [GT.CongressionalDistrict, BR.CountyWeight]) -> F.Record ks)
  -> FL.Fold (F.Record (CPSVoterPUMS V.++ [GT.CongressionalDistrict, BR.CountyWeight]))
  (F.FrameRec ('[BR.Year, GT.StateAbbreviation, GT.StateFIPS, GT.CongressionalDistrict] V.++ ks V.++ ET.EWCols))
cpsVoterPUMSElectoralWeightsByCD getCatKey =
  let wgt r = F.rgetField @CPSVoterPUMSWeight r * F.rgetField @BR.CountyWeight r
  in cpsVoterPUMSElectoralWeights @[DT.CitizenC, ET.VotedYNC, CPSVoterPUMSWeight, BR.CountyWeight]
     (F.rcast @[BR.Year, GT.StateAbbreviation, GT.StateFIPS, GT.CongressionalDistrict]) getCatKey wgt


-- rollup for MRP
cpsCountVotersByStateF
  :: forall ks.
  (Ord (F.Record ks)
  , FI.RecVec (ks V.++ BRCF.CountCols)
  , ks F.⊆ CPSVoterPUMS
  )
  => (F.Record CPSVoterPUMS -> F.Record ks)
  -> Int -- year
  -> FMR.Fold
  (F.Record CPSVoterPUMS)
  (F.FrameRec ('[GT.StateAbbreviation] V.++ ks V.++ BRCF.CountCols))
cpsCountVotersByStateF _ year =
  let isYear y r = F.rgetField @BR.Year r == y
      possible r = cpsPossibleVoter $ F.rgetField @ET.VotedYNC r
      citizen r = F.rgetField @DT.CitizenC r == DT.Citizen
      includeRow r = isYear year r &&  possible r && citizen r
      voted r = cpsVoted $ F.rgetField @ET.VotedYNC r
  in BRCF.weightedCountFold
     (F.rcast @(GT.StateAbbreviation ': ks))
     (F.rcast @[ET.VotedYNC, CPSVoterPUMSWeight])
     includeRow
     voted
     (F.rgetField @CPSVoterPUMSWeight)

{-
cpsCountVotersByCDF
  :: forall ks.
  (Ord (F.Record ks)
  , FI.RecVec (ks V.++ BRCF.CountCols)
  , ks F.⊆ (CPSVoterPUMS V.++ [BR.CongressionalDistrict, BR.CountyWeight])
  )
  => (F.Record (CPSVoterPUMS V.++ [BR.CongressionalDistrict, BR.CountyWeight]) -> F.Record ks)
  -> Int -- year
  -> FMR.Fold
  (F.Record (CPSVoterPUMS V.++ [BR.CongressionalDistrict, BR.CountyWeight]))
  (F.FrameRec ('[BR.StateAbbreviation, BR.CongressionalDistrict] V.++ ks V.++ BRCF.CountCols))
cpsCountVotersByCDF _ year =
  let isYear y r = F.rgetField @BR.Year r == y
      possible r = cpsPossibleVoter $ F.rgetField @BR.VotedYNC r
      citizen r = F.rgetField @BR.IsCitizen r
      includeRow r = isYear year r &&  possible r && citizen r
      voted r = cpsVoted $ F.rgetField @BR.VotedYNC r
      wgt r = F.rgetField @CPSVoterPUMSWeight r * F.rgetField @BR.CountyWeight r
  in BRCF.weightedCountFold
     (F.rcast @([BR.StateAbbreviation, BR.CongressionalDistrict] V.++ ks))
     (F.rcast  @[BR.VotedYNC, CPSVoterPUMSWeight, BR.CountyWeight])
     includeRow
     voted
     wgt
-}


-- We give the option of counting "In College" as "College Grad". This is different from what the census summary tables do.
-- NB: This needs to be done consistently with the demographics.
-- We don't have this information for the preferences, at least not from CCES, so doing this amounts to assigning
-- "in college" people to either Grad or NonGrad buckets in terms of voting pref.

cpsKeysToASER5 :: Bool
               -> F.Record '[DT.Age4C, DT.SexC, DT.CollegeGradC, DT.InCollege, DT.RaceAlone4C, DT.HispC]
               -> F.Record DT.CatColsASER5
cpsKeysToASER5 addInCollegeToGrads r =
  let cg = F.rgetField @DT.CollegeGradC r
      ic = addInCollegeToGrads && F.rgetField @DT.InCollege r
      ra4 =  F.rgetField @DT.RaceAlone4C r
      h = F.rgetField @DT.HispC r
  in (DT.age4ToSimple $ F.rgetField @DT.Age4C r)
     F.&: (F.rgetField @DT.SexC r)
     F.&: (if cg == DT.Grad || ic then DT.Grad else DT.NonGrad)
     F.&: DT.race5FromRaceAlone4AndHisp True ra4 h
     F.&: V.RNil


cpsKeysToASER4H :: Bool
                -> F.Record [DT.Age4C, DT.SexC, DT.CollegeGradC, DT.InCollege, DT.RaceAlone4C, DT.HispC]
                -> F.Record [DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.RaceAlone4C, DT.HispC]
cpsKeysToASER4H addInCollegeToGrads r =
  let cg = F.rgetField @DT.CollegeGradC r
      ic = addInCollegeToGrads && F.rgetField @DT.InCollege r
  in (DT.age4ToSimple $ F.rgetField @DT.Age4C r)
     F.&: (F.rgetField @DT.SexC r)
     F.&: (if cg == DT.Grad || ic then DT.Grad else DT.NonGrad)
     F.&: F.rgetField @DT.RaceAlone4C r
     F.&: F.rgetField @DT.HispC r
     F.&: V.RNil

cpsKeysToCASER4H :: Bool
                 -> F.Record [DT.CitizenC, DT.Age4C, DT.SexC, DT.CollegeGradC, DT.InCollege, DT.RaceAlone4C, DT.HispC]
                 -> F.Record [DT.CitizenC, DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.RaceAlone4C, DT.HispC]
cpsKeysToCASER4H addInCollegeToGrads r =
  let cg = F.rgetField @DT.CollegeGradC r
      ic = addInCollegeToGrads && F.rgetField @DT.InCollege r
  in F.rgetField @DT.CitizenC r
     F.&: (DT.age4ToSimple $ F.rgetField @DT.Age4C r)
     F.&: (F.rgetField @DT.SexC r)
     F.&: (if cg == DT.Grad || ic then DT.Grad else DT.NonGrad)
     F.&: F.rgetField @DT.RaceAlone4C r
     F.&: F.rgetField @DT.HispC r
     F.&: V.RNil



cpsKeysToASER :: Bool -> F.Record '[DT.Age4C, DT.SexC, DT.CollegeGradC, DT.InCollege, DT.RaceAlone4C, DT.HispC] -> F.Record DT.CatColsASER
cpsKeysToASER addInCollegeToGrads r =
  let cg = F.rgetField @DT.CollegeGradC r
      ic = addInCollegeToGrads && F.rgetField @DT.InCollege r
      ra4 =  F.rgetField @DT.RaceAlone4C r
      h = F.rgetField @DT.HispC r
  in (DT.age4ToSimple $ F.rgetField @DT.Age4C r)
     F.&: (F.rgetField @DT.SexC r)
     F.&: (if cg == DT.Grad || ic then DT.Grad else DT.NonGrad)
     F.&: (DT.simpleRaceFromRaceAlone4AndHisp True ra4 h)
     F.&: V.RNil


{-
cpsKeysToASER4 :: Bool -> F.Record '[DT.Age4C, DT.SexC, DT.CollegeGradC, DT.InCollege, DT.Race5C] -> F.Record DT.CatColsASER4
cpsKeysToASER4 addInCollegeToGrads r =
  let cg = F.rgetField @DT.CollegeGradC r
      ic = addInCollegeToGrads && F.rgetField @DT.InCollege r
  in (DT.age4ToSimple $ F.rgetField @DT.Age4C r)
     F.&: (F.rgetField @DT.SexC r)
     F.&: (if cg == DT.Grad || ic then DT.Grad else DT.NonGrad)
     F.&: (DT.race4FromRace5 $ F.rgetField @DT.Race5C r)
     F.&: V.RNil

cpsKeysToASER :: Bool -> F.Record '[DT.Age4C, DT.SexC, DT.CollegeGradC, DT.InCollege, DT.Race5C] -> F.Record DT.CatColsASER
cpsKeysToASER addInCollegeToGrads r =
  let cg = F.rgetField @DT.CollegeGradC r
      ic = addInCollegeToGrads && F.rgetField @DT.InCollege r
  in (DT.age4ToSimple $ F.rgetField @DT.Age4C r)
     F.&: (F.rgetField @DT.SexC r)
     F.&: (if cg == DT.Grad || ic then DT.Grad else DT.NonGrad)
     F.&: (DT.simpleRaceFromRace5 $ F.rgetField @DT.Race5C r)
     F.&: V.RNil

cpsKeysToASE :: Bool -> F.Record '[DT.Age4C, DT.SexC, DT.CollegeGradC, DT.InCollege, DT.Race5C] -> F.Record DT.CatColsASE
cpsKeysToASE addInCollegeToGrads r =
  let cg = F.rgetField @DT.CollegeGradC r
      ic = addInCollegeToGrads && F.rgetField @DT.InCollege r
  in (DT.age4ToSimple $ F.rgetField @DT.Age4C r)
     F.&: (F.rgetField @DT.SexC r)
     F.&: (if cg == DT.Grad || ic then DT.Grad else DT.NonGrad)
     F.&: V.RNil

cpsKeysToASR :: F.Record '[DT.Age4C, DT.SexC, DT.CollegeGradC, DT.InCollege, DT.Race5C] -> F.Record DT.CatColsASR
cpsKeysToASR r =
  (DT.age4ToSimple $ F.rgetField @DT.Age4C r)
  F.&: (F.rgetField @DT.SexC r)
  F.&: (DT.simpleRaceFromRace5 $ F.rgetField @DT.Race5C r)
  F.&: V.RNil

cpsKeysToIdentity :: F.Record '[DT.Age4C, DT.SexC, DT.CollegeGradC, DT.InCollege, DT.Race5C] -> F.Record '[]
cpsKeysToIdentity = const V.RNil
-}
-- we have to drop all records with age < 18
intToAge4 :: Int -> DT.Age4
intToAge4 n
  | n < 18 = error "CPS Voter record with age < 18"
  | n < 24 = DT.A4_18To24
  | n < 45 = DT.A4_25To44
  | n < 65 = DT.A4_45To64
  | otherwise = DT.A4_65AndOver

intToCollegeGrad :: Int -> DT.CollegeGrad
intToCollegeGrad n = if n < 111 then DT.NonGrad else DT.Grad

intToInCollege :: Int -> Bool
intToInCollege n = n == 4

intToSex :: Int -> DT.Sex
intToSex n = if n == 1 then DT.Male else DT.Female

intToRaceAlone4 :: Int -> DT.RaceAlone4
intToRaceAlone4 rN
  | rN == 100 = DT.RA4_White
  | rN == 200 = DT.RA4_Black
  | rN == 651 = DT.RA4_Asian
  | otherwise = DT.RA4_Other

intToHisp :: Int -> DT.Hisp
intToHisp hN
  | (hN >= 100) && (hN <= 901) = DT.Hispanic
  | otherwise = DT.NonHispanic

{-
intsToRace5 :: Int -> Int -> DT.Race5
intsToRace5 hN rN
  | (hN >= 100) && (hN <= 901) = DT.R5_Hispanic
  | rN == 100 = DT.R5_WhiteNonHispanic
  | rN == 200 = DT.R5_Black
  | rN == 651 = DT.R5_Asian
  | otherwise = DT.R5_Other
-}

intToCit :: Int -> DT.Citizen
intToCit n = if n /= 5 then DT.Citizen else DT.NonCitizen

intToVoteWhyNot :: Int -> ET.VoteWhyNot
intToVoteWhyNot n
  | n == 1 = ET.VWN_PhysicallyUnable
  | n == 2 = ET.VWN_Away
  | n == 3 = ET.VWN_Forgot
  | n == 4 = ET.VWN_NotInterested
  | n == 5 = ET.VWN_Busy
  | n == 6 = ET.VWN_Transport
  | n == 7 = ET.VWN_DislikeChoices
  | n == 8 = ET.VWN_RegIssue
  | n == 9 = ET.VWN_Weather
  | n == 10 = ET.VWN_BadPollingPlace
  | otherwise = ET.VWN_Other

intToRegWhyNot :: Int -> ET.RegWhyNot
intToRegWhyNot n
  | n == 1 = ET.RWN_MissedDeadline
  | n == 2 = ET.RWN_DidntKnowHow
  | n == 3 = ET.RWN_Residency
  | n == 4 = ET.RWN_PhysicallyUnable
  | n == 5 = ET.RWN_Language
  | n == 6 = ET.RWN_NotInterested
  | n == 7 = ET.RWN_MyVoteIrrelevant
  | otherwise = ET.RWN_Other


intToVoteHow :: Int -> ET.VoteHow
intToVoteHow n
  | n == 1 = ET.VH_InPerson
  | n == 2 = ET.VH_ByMail
  | otherwise = ET.VH_Other

intToVoteWhen :: Int -> ET.VoteWhen
intToVoteWhen n
  | n == 1 = ET.VW_ElectionDay
  | n == 2 = ET.VW_BeforeElectionDay
  | otherwise = ET.VW_Other

intToVotedYN :: Int -> ET.VotedYN
intToVotedYN n
  | n == 1 = ET.VYN_DidNotVote
  | n == 2 = ET.VYN_Voted
  | n == 96 = ET.VYN_Refused
  | n == 97 = ET.VYN_DontKnow
  | n == 98 = ET.VYN_NoResponse
  | otherwise = ET.VYN_NotInUniverse

cpsPossibleVoter :: ET.VotedYN -> Bool
cpsPossibleVoter ET.VYN_DidNotVote = True
cpsPossibleVoter ET.VYN_Voted = True
cpsPossibleVoter ET.VYN_Refused = False
cpsPossibleVoter ET.VYN_DontKnow = False
cpsPossibleVoter ET.VYN_NoResponse = False
cpsPossibleVoter ET.VYN_NotInUniverse = False

cpsVoted :: ET.VotedYN -> Bool
cpsVoted ET.VYN_Voted = True
cpsVoted _ = False

intToRegisteredYN :: Int -> ET.RegisteredYN
intToRegisteredYN n
  | n == 1 = ET.RYN_NotRegistered
  | n == 2 = ET.RYN_Registered
  | otherwise = ET.RYN_Other



transformCPSVoterPUMSRow :: CPS.CPSVoterPUMS_Raw -> F.Record CPSVoterPUMS'
transformCPSVoterPUMSRow = F.rcast . addCols  where
  addCols = (FT.addOneFromOne @CPS.CPSRACE @DT.RaceAlone4C intToRaceAlone4)
            . (FT.addOneFromOne @CPS.CPSHISPAN @DT.HispC intToHisp)
            . (FT.addName @CPS.CPSVOSUPPWT @CPSVoterPUMSWeight)
--            . (FT.addName @BR.StateAbbreviation @GT.StateAbbreviation)
            . (FT.addName @CPS.CPSCOUNTY @BR.CountyFIPS)
            . (FT.addName @CPS.CPSSTATEFIP @BR.StateFIPS)
            . (FT.addName @CPS.CPSYEAR @BR.Year)
            . (FT.addOneFromOne @CPS.CPSVOREG @ET.RegisteredYNC intToRegisteredYN)
            . (FT.addOneFromOne @CPS.CPSVOTED @ET.VotedYNC intToVotedYN)
            . (FT.addOneFromOne @CPS.CPSVOTEWHEN @ET.VoteWhenC intToVoteWhen)
            . (FT.addOneFromOne @CPS.CPSVOTEHOW @ET.VoteHowC intToVoteHow)
            . (FT.addOneFromOne @CPS.CPSVOYNOTREG @ET.RegWhyNotC intToRegWhyNot)
            . (FT.addOneFromOne @CPS.CPSVOWHYNOT @ET.VoteWhyNotC intToVoteWhyNot)
            . (FT.addOneFromOne @CPS.CPSSCHLCOLL @DT.InCollege intToInCollege)
            . (FT.addOneFromOne @CPS.CPSEDUC @DT.CollegeGradC intToCollegeGrad)
            . (FT.addOneFromOne @CPS.CPSCITIZEN @DT.CitizenC intToCit)
            . (FT.addOneFromOne @CPS.CPSSEX @DT.SexC intToSex)
            . (FT.addOneFromOne @CPS.CPSAGE @DT.Age4C intToAge4)


{-
transformCPSVoterPUMSRow :: CPS.CPSVoterPUMS_Raw -> F.Record CPSVoterPUMS'
transformCPSVoterPUMSRow r = F.rcast @CPSVoterPUMS' (mutate r) where
  addAge = FT.recordSingleton @DT.Age4C . intToAge4 . F.rgetField @CPS.CPSAGE
  addSex =  FT.recordSingleton @DT.SexC . intToSex . F.rgetField @CPS.CPSSEX
  hN = F.rgetField @CPS.CPSHISPAN
  rN = F.rgetField @CPS.CPSRACE
  addRace r = FT.recordSingleton @DT.Race5C $ intsToRace5 (hN r) (rN r)
  addIsCit = FT.recordSingleton @DT.CitizenC . intToCit . F.rgetField @CPS.CPSCITIZEN
  addCollegeGrad = FT.recordSingleton @DT.CollegeGradC . intToCollegeGrad . F.rgetField @CPS.CPSEDUC
  addInCollege = FT.recordSingleton @DT.InCollege . intToInCollege . F.rgetField @CPS.CPSSCHLCOLL
  addVoteWhyNot = FT.recordSingleton @ET.VoteWhyNotC . intToVoteWhyNot . F.rgetField @CPS.CPSVOWHYNOT
  addRegWhyNot = FT.recordSingleton @ET.RegWhyNotC . intToRegWhyNot . F.rgetField @CPS.CPSVOYNOTREG
  addVoteHow = FT.recordSingleton @ET.VoteHowC . intToVoteHow . F.rgetField @CPS.CPSVOTEHOW
  addVoteWhen = FT.recordSingleton @ET.VoteWhenC . intToVoteWhen . F.rgetField @CPS.CPSVOTEWHEN
  addVotedYN = FT.recordSingleton @ET.VotedYNC . intToVotedYN . F.rgetField @CPS.CPSVOTED
  addRegisteredYN = FT.recordSingleton @ET.RegisteredYNC . intToRegisteredYN . F.rgetField @CPS.CPSVOREG
  mutate = FT.retypeColumn @CSP.CPSYEAR @BR.Year
           . FT.retypeColumn @CPS.CPSSTATEFIP @BR.StateFIPS
           . FT.retypeColumn @CPS.CPSCOUNTY @BR.CountyFIPS
           . FT.mutate addAge
           . FT.mutate addSex
           . FT.mutate addRace
           . FT.mutate addIsCit
           . FT.mutate addCollegeGrad
           . FT.mutate addInCollege
           . FT.mutate addVoteWhyNot
           . FT.mutate addRegWhyNot
           . FT.mutate addVoteHow
           . FT.mutate addVoteWhen
           . FT.mutate addVotedYN
           . FT.mutate addRegisteredYN
           . FT.retypeColumn @CPS.CPSVOSUPPWT @CPSVoterPUMSWeight
-}

-- to use in maybeRecsToFrame
-- if SCHG indicates not in school we map to 0 so we will interpret as "Not In College"
{-
fixPUMSRow :: F.Rec (Maybe F.:. F.ElField) PUMS_Raw -> F.Rec (Maybe F.:. F.ElField) PUMS_Raw
fixPUMSRow r = (F.rsubset %~ missingInCollegeTo0)
               $ r where
  missingInCollegeTo0 :: F.Rec (Maybe :. F.ElField) '[BR.PUMSSCHG] -> F.Rec (Maybe :. F.ElField) '[BR.PUMSSCHG]
  missingInCollegeTo0 = FM.fromMaybeMono 0
-}

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
--  , cpsKeysToIdentity
  , cpsPossibleVoter
  , cpsVoted

  ) where


import qualified BlueRipple.Data.CPSVoterPUMS.CPSVoterPUMS_Frame as BR
import qualified BlueRipple.Data.DemographicTypes as BR
import qualified BlueRipple.Data.ElectionTypes as BR
import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Data.LoadersCore as BR
import qualified BlueRipple.Data.Loaders as BR
import qualified BlueRipple.Data.Keyed as BR
import qualified BlueRipple.Utilities.KnitUtils as BR
import qualified BlueRipple.Data.CountFolds as BRCF

import qualified Control.Foldl                 as FL
import           Control.Lens                   ((%~))
import qualified Control.Monad.Except          as X
import qualified Control.Monad.State           as ST
import qualified Data.Array                    as A
import qualified Data.Serialize                as S
import qualified Data.Serialize.Text           as S
import qualified Data.List                     as L
import qualified Data.Map                      as M
import           Data.Maybe                     ( fromMaybe, catMaybes)
import qualified Data.Text                     as T
import           Data.Text                      ( Text )
import           Text.Read                      (readMaybe)
import qualified Data.Vinyl                    as V
import           Data.Vinyl.TypeLevel                     (type (++))
import qualified Data.Vinyl.TypeLevel          as V
import qualified Data.Vinyl.Functor            as V
import qualified Frames                        as F
import           Data.Vinyl.Lens               (type (⊆))
import           Frames                         ( (:.)(..) )
import qualified Frames.CSV                    as F
import qualified Frames.InCore                 as FI
import qualified Frames.TH                     as F
import qualified Frames.Melt                   as F
import qualified Text.Read                     as TR

import qualified Frames.Folds                  as FF
import qualified Frames.MapReduce              as FMR
import qualified Frames.ParseableTypes         as FP
import qualified Frames.Transform              as FT
import qualified Frames.MaybeUtils             as FM
import qualified Frames.Utils                  as FU
import qualified Frames.MapReduce              as MR
import qualified Frames.Enumerations           as FE
import qualified Frames.Serialize              as FS
import qualified Frames.SimpleJoins            as FJ
import qualified Frames.Visualization.VegaLite.Data
                                               as FV
--import qualified Graphics.Vega.VegaLite        as GV

{-
import qualified Data.IndexedSet               as IS
import qualified Numeric.GLM.ProblemTypes      as GLM
import qualified Numeric.GLM.ModelTypes      as GLM
import qualified Numeric.GLM.Predict            as GLM
import qualified Numeric.LinearAlgebra         as LA
-}

import           Data.Hashable                  ( Hashable )
import qualified Data.Vector                   as V
--import qualified Data.Vector.Boxed             as VB
import           GHC.Generics                   ( Generic, Rep )

import qualified Knit.Report as K
import qualified Polysemy.Error                as P (mapError, Error)
import qualified Polysemy                as P (raise)


import GHC.TypeLits (Symbol)
import Data.Kind (Type)

cpsVoterPUMSLoader :: (K.KnitEffects r, BR.CacheEffects r)
                   => K.Sem r (K.ActionWithCacheTime r (F.FrameRec CPSVoterPUMS))
cpsVoterPUMSLoader = do
  let cpsPUMSDataPath = BR.LocalData $ T.pack BR.cpsVoterPUMSCSV
  cachedStateAbbrCrosswalk <- BR.stateAbbrCrosswalkLoader
  cachedCPSDataPath <- K.liftKnit $ BR.dataPathWithCacheTime cpsPUMSDataPath
  let cachedDeps = (,) <$> cachedStateAbbrCrosswalk <*> cachedCPSDataPath
  BR.retrieveOrMakeFrame "data/cpsVoterPUMSWithAbbrs.bin" cachedDeps $ \(stateAbbrCrosswalk, dataPath) -> do
    let filter r = (F.rgetField @BR.CPSAGE r >= 18) && (F.rgetField @BR.CPSCITIZEN r /= 5)
    withoutAbbr <- K.ignoreCacheTimeM
                   $ BR.cachedFrameLoader @(F.RecordColumns BR.CPSVoterPUMS_Raw) @CPSVoterPUMS'
                   dataPath
                   Nothing
                   (Just filter)
                   transformCPSVoterPUMSRow
                   Nothing
                   "cpsVoterPUMS.bin"
    fmap (fmap F.rcast) (K.knitMaybe "missing state abbreviation in state abbreviation crosswalk"
                          $ FJ.leftJoinM @'[BR.StateFIPS] withoutAbbr stateAbbrCrosswalk)


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
                     , BR.StateFIPS
                     , BR.StateAbbreviation
                     , BR.CountyFIPS
                     , BR.Age4C
                     , BR.SexC
                     , BR.RaceAlone4C
                     , BR.HispC
                     , BR.IsCitizen
                     , BR.CollegeGradC
                     , BR.InCollege
                     , BR.VoteWhyNotC
                     , BR.RegWhyNotC
                     , BR.VoteHowC
                     , BR.VoteWhenC
                     , BR.VotedYNC
                     , BR.RegisteredYNC
                     , CPSVoterPUMSWeight
                     ]

type CPSVoterPUMS' = V.RDelete BR.StateAbbreviation CPSVoterPUMS


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
            safeDiv n d = if d > 0 then n/d else 0
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
  , F.ElemOf cs BR.IsCitizen
  , F.ElemOf cs BR.VotedYNC
  , FI.RecVec (ls V.++ ks V.++ BR.EWCols)
  , (ls V.++ (ks V.++ BR.EWCols)) ~ ((ls V.++ ks) V.++ BR.EWCols)
  , Ord (F.Record (ls V.++ ks))
  , Ord (F.Record ls)
  , BR.FiniteSet (F.Record ks)
  , ls F.⊆ ((ls V.++ ks) V.++ BR.EWCols)
  , (ks V.++ BR.EWCols) F.⊆ ((ls V.++ ks) V.++ BR.EWCols)
  , ks F.⊆ (ks V.++ BR.EWCols)
  , BR.EWCols F.⊆ (ks V.++ BR.EWCols)
  )
  => (F.Record rs -> F.Record ls)
  -> (F.Record rs -> F.Record ks)
  -> (F.Record cs -> Double)
  -> FL.Fold (F.Record rs) (F.FrameRec (ls V.++ ks V.++ BR.EWCols))
cpsVoterPUMSElectoralWeights getLoc getKey getWgt =
  let toRec :: Double -> F.Record BR.EWCols
      toRec w =  BR.EW_Census F.&: BR.EW_Citizen F.&: w F.&: V.RNil
      citizen r = F.rgetField @BR.IsCitizen r
      possibleVoter r = cpsPossibleVoter $ F.rgetField @BR.VotedYNC r
      voted r = cpsVoted $ F.rgetField @BR.VotedYNC r
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
  (F.ElemOf (ks ++ '[BR.IsCitizen, BR.VotedYNC, CPSVoterPUMSWeight]) CPSVoterPUMSWeight
  , F.ElemOf (ks ++ '[BR.IsCitizen, BR.VotedYNC, CPSVoterPUMSWeight]) BR.IsCitizen
  , F.ElemOf (ks ++ '[BR.IsCitizen, BR.VotedYNC, CPSVoterPUMSWeight]) BR.VotedYNC
  , FI.RecVec (ks ++ BR.EWCols)
  , Ord (F.Record ks)
  , ks F.⊆ ('[BR.Year, BR.StateAbbreviation, BR.StateFIPS] V.++ ks V.++ '[BR.IsCitizen, BR.VotedYNC, CPSVoterPUMSWeight])
  , BR.FiniteSet (F.Record ks)
  , F.ElemOf (ks V.++ BR.EWCols) BR.ElectoralWeight
  , F.ElemOf (ks V.++ BR.EWCols) BR.ElectoralWeightOf
  , F.ElemOf (ks V.++ BR.EWCols) BR.ElectoralWeightSource
  , (ks V.++ BR.EWCols) F.⊆ ('[BR.Year, BR.StateAbbreviation, BR.StateFIPS] V.++ ks V.++ BR.EWCols)
  , ks F.⊆ (ks V.++ BR.EWCols)
  )
  => (F.Record CPSVoterPUMS -> F.Record ks)
  -> FL.Fold (F.Record CPSVoterPUMS) (F.FrameRec ('[BR.Year, BR.StateAbbreviation, BR.StateFIPS] V.++ ks V.++ BR.EWCols))
cpsVoterPUMSElectoralWeightsByState getCatKey =
  cpsVoterPUMSElectoralWeights @[BR.IsCitizen, BR.VotedYNC, CPSVoterPUMSWeight]
  (F.rcast @[BR.Year, BR.StateAbbreviation, BR.StateFIPS]) getCatKey (F.rgetField @CPSVoterPUMSWeight)

cpsVoterPUMSNationalElectoralWeights
  :: forall ks.
  (F.ElemOf (ks ++ '[BR.IsCitizen, BR.VotedYNC, CPSVoterPUMSWeight]) CPSVoterPUMSWeight
  , F.ElemOf (ks ++ '[BR.IsCitizen, BR.VotedYNC, CPSVoterPUMSWeight]) BR.IsCitizen
  , F.ElemOf (ks ++ '[BR.IsCitizen, BR.VotedYNC, CPSVoterPUMSWeight]) BR.VotedYNC
  , FI.RecVec (ks ++ BR.EWCols)
  , Ord (F.Record ks)
  , ks F.⊆ ('[BR.Year] V.++ ks V.++ '[BR.IsCitizen, BR.VotedYNC, CPSVoterPUMSWeight])
  , BR.FiniteSet (F.Record ks)
  , F.ElemOf (ks V.++ BR.EWCols) BR.ElectoralWeight
  , F.ElemOf (ks V.++ BR.EWCols) BR.ElectoralWeightOf
  , F.ElemOf (ks V.++ BR.EWCols) BR.ElectoralWeightSource
  , (ks V.++ BR.EWCols) F.⊆ ('[BR.Year] V.++ ks V.++ BR.EWCols)
  , ks F.⊆ (ks V.++ BR.EWCols)
  )
  => (F.Record CPSVoterPUMS -> F.Record ks)
  -> FL.Fold (F.Record CPSVoterPUMS) (F.FrameRec ('[BR.Year] V.++ ks V.++ BR.EWCols))
cpsVoterPUMSNationalElectoralWeights getCatKey =
  cpsVoterPUMSElectoralWeights @[BR.IsCitizen, BR.VotedYNC, CPSVoterPUMSWeight] (F.rcast @'[BR.Year]) getCatKey (F.rgetField @CPSVoterPUMSWeight)

cpsVoterPUMSElectoralWeightsByCD
  :: forall ks.
  (F.ElemOf (ks ++ '[BR.IsCitizen, BR.VotedYNC, CPSVoterPUMSWeight, BR.CountyWeight]) CPSVoterPUMSWeight
  , F.ElemOf (ks ++ '[BR.IsCitizen, BR.VotedYNC, CPSVoterPUMSWeight, BR.CountyWeight]) BR.CountyWeight
  , F.ElemOf (ks ++ '[BR.IsCitizen, BR.VotedYNC, CPSVoterPUMSWeight, BR.CountyWeight]) BR.IsCitizen
  , F.ElemOf (ks ++ '[BR.IsCitizen, BR.VotedYNC, CPSVoterPUMSWeight, BR.CountyWeight]) BR.VotedYNC
  , FI.RecVec (ks ++ BR.EWCols)
  , Ord (F.Record ks)
  , ks F.⊆ ('[BR.Year, BR.StateAbbreviation, BR.StateFIPS, BR.CongressionalDistrict]
             V.++ ks
             V.++ '[BR.IsCitizen, BR.VotedYNC, CPSVoterPUMSWeight, BR.CountyWeight])
  , BR.FiniteSet (F.Record ks)
  , F.ElemOf (ks V.++ BR.EWCols) BR.ElectoralWeight
  , F.ElemOf (ks V.++ BR.EWCols) BR.ElectoralWeightOf
  , F.ElemOf (ks V.++ BR.EWCols) BR.ElectoralWeightSource
  , (ks V.++ BR.EWCols) F.⊆ ('[BR.Year, BR.StateAbbreviation, BR.StateFIPS, BR.CongressionalDistrict] V.++ ks V.++ BR.EWCols)
  , ks F.⊆ (ks V.++ BR.EWCols)
  )
  => (F.Record (CPSVoterPUMS V.++ [BR.CongressionalDistrict, BR.CountyWeight]) -> F.Record ks)
  -> FL.Fold (F.Record (CPSVoterPUMS V.++ [BR.CongressionalDistrict, BR.CountyWeight]))
  (F.FrameRec ('[BR.Year, BR.StateAbbreviation, BR.StateFIPS, BR.CongressionalDistrict] V.++ ks V.++ BR.EWCols))
cpsVoterPUMSElectoralWeightsByCD getCatKey =
  let wgt r = F.rgetField @CPSVoterPUMSWeight r * F.rgetField @BR.CountyWeight r
  in cpsVoterPUMSElectoralWeights @[BR.IsCitizen, BR.VotedYNC, CPSVoterPUMSWeight, BR.CountyWeight]
     (F.rcast @[BR.Year, BR.StateAbbreviation, BR.StateFIPS, BR.CongressionalDistrict]) getCatKey wgt


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
  (F.FrameRec ('[BR.StateAbbreviation] V.++ ks V.++ BRCF.CountCols))
cpsCountVotersByStateF getCatKey year =
  let isYear y r = F.rgetField @BR.Year r == y
      possible r = cpsPossibleVoter $ F.rgetField @BR.VotedYNC r
      citizen r = F.rgetField @BR.IsCitizen r
      includeRow r = isYear year r &&  possible r && citizen r
      voted r = cpsVoted $ F.rgetField @BR.VotedYNC r
  in BRCF.weightedCountFold
     (F.rcast @(BR.StateAbbreviation ': ks))
     (F.rcast @[BR.VotedYNC, CPSVoterPUMSWeight])
     includeRow
     voted
     (F.rgetField @CPSVoterPUMSWeight)

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
cpsCountVotersByCDF getCatKey year =
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



-- We give the option of counting "In College" as "College Grad". This is different from what the census summary tables do.
-- NB: This needs to be done consistently with the demographics.
-- We don't have this information for the preferences, at least not from CCES, so doing this amounts to assigning
-- "in college" people to either Grad or NonGrad buckets in terms of voting pref.

cpsKeysToASER5 :: Bool
               -> F.Record '[BR.Age4C, BR.SexC, BR.CollegeGradC, BR.InCollege, BR.RaceAlone4C, BR.HispC]
               -> F.Record BR.CatColsASER5
cpsKeysToASER5 addInCollegeToGrads r =
  let cg = F.rgetField @BR.CollegeGradC r
      ic = addInCollegeToGrads && F.rgetField @BR.InCollege r
      ra4 =  F.rgetField @BR.RaceAlone4C r
      h = F.rgetField @BR.HispC r
  in (BR.age4ToSimple $ F.rgetField @BR.Age4C r)
     F.&: (F.rgetField @BR.SexC r)
     F.&: (if cg == BR.Grad || ic then BR.Grad else BR.NonGrad)
     F.&: BR.race5FromRaceAlone4AndHisp True ra4 h
     F.&: V.RNil


cpsKeysToASER4H :: Bool
                -> F.Record [BR.Age4C, BR.SexC, BR.CollegeGradC, BR.InCollege, BR.RaceAlone4C, BR.HispC]
                -> F.Record [BR.SimpleAgeC, BR.SexC, BR.CollegeGradC, BR.RaceAlone4C, BR.HispC]
cpsKeysToASER4H addInCollegeToGrads r =
  let cg = F.rgetField @BR.CollegeGradC r
      ic = addInCollegeToGrads && F.rgetField @BR.InCollege r
  in (BR.age4ToSimple $ F.rgetField @BR.Age4C r)
     F.&: (F.rgetField @BR.SexC r)
     F.&: (if cg == BR.Grad || ic then BR.Grad else BR.NonGrad)
     F.&: F.rgetField @BR.RaceAlone4C r
     F.&: F.rgetField @BR.HispC r
     F.&: V.RNil


cpsKeysToASER :: Bool -> F.Record '[BR.Age4C, BR.SexC, BR.CollegeGradC, BR.InCollege, BR.RaceAlone4C, BR.HispC] -> F.Record BR.CatColsASER
cpsKeysToASER addInCollegeToGrads r =
  let cg = F.rgetField @BR.CollegeGradC r
      ic = addInCollegeToGrads && F.rgetField @BR.InCollege r
      ra4 =  F.rgetField @BR.RaceAlone4C r
      h = F.rgetField @BR.HispC r
  in (BR.age4ToSimple $ F.rgetField @BR.Age4C r)
     F.&: (F.rgetField @BR.SexC r)
     F.&: (if cg == BR.Grad || ic then BR.Grad else BR.NonGrad)
     F.&: (BR.simpleRaceFromRaceAlone4AndHisp True ra4 h)
     F.&: V.RNil


{-
cpsKeysToASER4 :: Bool -> F.Record '[BR.Age4C, BR.SexC, BR.CollegeGradC, BR.InCollege, BR.Race5C] -> F.Record BR.CatColsASER4
cpsKeysToASER4 addInCollegeToGrads r =
  let cg = F.rgetField @BR.CollegeGradC r
      ic = addInCollegeToGrads && F.rgetField @BR.InCollege r
  in (BR.age4ToSimple $ F.rgetField @BR.Age4C r)
     F.&: (F.rgetField @BR.SexC r)
     F.&: (if cg == BR.Grad || ic then BR.Grad else BR.NonGrad)
     F.&: (BR.race4FromRace5 $ F.rgetField @BR.Race5C r)
     F.&: V.RNil

cpsKeysToASER :: Bool -> F.Record '[BR.Age4C, BR.SexC, BR.CollegeGradC, BR.InCollege, BR.Race5C] -> F.Record BR.CatColsASER
cpsKeysToASER addInCollegeToGrads r =
  let cg = F.rgetField @BR.CollegeGradC r
      ic = addInCollegeToGrads && F.rgetField @BR.InCollege r
  in (BR.age4ToSimple $ F.rgetField @BR.Age4C r)
     F.&: (F.rgetField @BR.SexC r)
     F.&: (if cg == BR.Grad || ic then BR.Grad else BR.NonGrad)
     F.&: (BR.simpleRaceFromRace5 $ F.rgetField @BR.Race5C r)
     F.&: V.RNil

cpsKeysToASE :: Bool -> F.Record '[BR.Age4C, BR.SexC, BR.CollegeGradC, BR.InCollege, BR.Race5C] -> F.Record BR.CatColsASE
cpsKeysToASE addInCollegeToGrads r =
  let cg = F.rgetField @BR.CollegeGradC r
      ic = addInCollegeToGrads && F.rgetField @BR.InCollege r
  in (BR.age4ToSimple $ F.rgetField @BR.Age4C r)
     F.&: (F.rgetField @BR.SexC r)
     F.&: (if cg == BR.Grad || ic then BR.Grad else BR.NonGrad)
     F.&: V.RNil

cpsKeysToASR :: F.Record '[BR.Age4C, BR.SexC, BR.CollegeGradC, BR.InCollege, BR.Race5C] -> F.Record BR.CatColsASR
cpsKeysToASR r =
  (BR.age4ToSimple $ F.rgetField @BR.Age4C r)
  F.&: (F.rgetField @BR.SexC r)
  F.&: (BR.simpleRaceFromRace5 $ F.rgetField @BR.Race5C r)
  F.&: V.RNil

cpsKeysToIdentity :: F.Record '[BR.Age4C, BR.SexC, BR.CollegeGradC, BR.InCollege, BR.Race5C] -> F.Record '[]
cpsKeysToIdentity = const V.RNil
-}
-- we have to drop all records with age < 18
intToAge4 :: Int -> BR.Age4
intToAge4 n
  | n < 18 = error "CPS Voter record with age < 18"
  | n < 24 = BR.A4_18To24
  | n < 45 = BR.A4_25To44
  | n < 65 = BR.A4_45To64
  | otherwise = BR.A4_65AndOver

intToCollegeGrad :: Int -> BR.CollegeGrad
intToCollegeGrad n = if n < 111 then BR.NonGrad else BR.Grad

intToInCollege :: Int -> Bool
intToInCollege n = n == 4

intToSex :: Int -> BR.Sex
intToSex n = if n == 1 then BR.Male else BR.Female

intToRaceAlone4 :: Int -> BR.RaceAlone4
intToRaceAlone4 rN
  | rN == 100 = BR.RA4_White
  | rN == 200 = BR.RA4_Black
  | rN == 651 = BR.RA4_Asian
  | otherwise = BR.RA4_Other

intToHisp :: Int -> BR.Hisp
intToHisp hN
  | (hN >= 100) && (hN <= 901) = BR.Hispanic
  | otherwise = BR.NonHispanic

intsToRace5 :: Int -> Int -> BR.Race5
intsToRace5 hN rN
  | (hN >= 100) && (hN <= 901) = BR.R5_Hispanic
  | rN == 100 = BR.R5_WhiteNonHispanic
  | rN == 200 = BR.R5_Black
  | rN == 651 = BR.R5_Asian
  | otherwise = BR.R5_Other

intToIsCitizen :: Int -> Bool
intToIsCitizen n = n /= 5

intToVoteWhyNot :: Int -> BR.VoteWhyNot
intToVoteWhyNot n
  | n == 1 = BR.VWN_PhysicallyUnable
  | n == 2 = BR.VWN_Away
  | n == 3 = BR.VWN_Forgot
  | n == 4 = BR.VWN_NotInterested
  | n == 5 = BR.VWN_Busy
  | n == 6 = BR.VWN_Transport
  | n == 7 = BR.VWN_DislikeChoices
  | n == 8 = BR.VWN_RegIssue
  | n == 9 = BR.VWN_Weather
  | n == 10 = BR.VWN_BadPollingPlace
  | otherwise = BR.VWN_Other

intToRegWhyNot :: Int -> BR.RegWhyNot
intToRegWhyNot n
  | n == 1 = BR.RWN_MissedDeadline
  | n == 2 = BR.RWN_DidntKnowHow
  | n == 3 = BR.RWN_Residency
  | n == 4 = BR.RWN_PhysicallyUnable
  | n == 5 = BR.RWN_Language
  | n == 6 = BR.RWN_NotInterested
  | n == 7 = BR.RWN_MyVoteIrrelevant
  | otherwise = BR.RWN_Other


intToVoteHow :: Int -> BR.VoteHow
intToVoteHow n
  | n == 1 = BR.VH_InPerson
  | n == 2 = BR.VH_ByMail
  | otherwise = BR.VH_Other

intToVoteWhen :: Int -> BR.VoteWhen
intToVoteWhen n
  | n == 1 = BR.VW_ElectionDay
  | n == 2 = BR.VW_BeforeElectionDay
  | otherwise = BR.VW_Other

intToVotedYN :: Int -> BR.VotedYN
intToVotedYN n
  | n == 1 = BR.VYN_DidNotVote
  | n == 2 = BR.VYN_Voted
  | n == 96 = BR.VYN_Refused
  | n == 97 = BR.VYN_DontKnow
  | n == 98 = BR.VYN_NoResponse
  | otherwise = BR.VYN_NotInUniverse

cpsPossibleVoter :: BR.VotedYN -> Bool
cpsPossibleVoter BR.VYN_DidNotVote = True
cpsPossibleVoter BR.VYN_Voted = True
cpsPossibleVoter BR.VYN_Refused = False
cpsPossibleVoter BR.VYN_DontKnow = False
cpsPossibleVoter BR.VYN_NoResponse = False
cpsPossibleVoter BR.VYN_NotInUniverse = False

cpsVoted :: BR.VotedYN -> Bool
cpsVoted BR.VYN_Voted = True
cpsVoted _ = False

intToRegisteredYN :: Int -> BR.RegisteredYN
intToRegisteredYN n
  | n == 1 = BR.RYN_NotRegistered
  | n == 2 = BR.RYN_Registered
  | otherwise = BR.RYN_Other



transformCPSVoterPUMSRow :: BR.CPSVoterPUMS_Raw -> F.Record CPSVoterPUMS'
transformCPSVoterPUMSRow = F.rcast . addCols  where
  addCols = (FT.addOneFromOne @BR.CPSRACE @BR.RaceAlone4C intToRaceAlone4)
            . (FT.addOneFromOne @BR.CPSHISPAN @BR.HispC intToHisp)
            . (FT.addName @BR.CPSVOSUPPWT @CPSVoterPUMSWeight)
            . (FT.addName @BR.CPSCOUNTY @BR.CountyFIPS)
            . (FT.addName @BR.CPSSTATEFIP @BR.StateFIPS)
            . (FT.addName @BR.CPSYEAR @BR.Year)
            . (FT.addOneFromOne @BR.CPSVOREG @BR.RegisteredYNC intToRegisteredYN)
            . (FT.addOneFromOne @BR.CPSVOTED @BR.VotedYNC intToVotedYN)
            . (FT.addOneFromOne @BR.CPSVOTEWHEN @BR.VoteWhenC intToVoteWhen)
            . (FT.addOneFromOne @BR.CPSVOTEHOW @BR.VoteHowC intToVoteHow)
            . (FT.addOneFromOne @BR.CPSVOYNOTREG @BR.RegWhyNotC intToRegWhyNot)
            . (FT.addOneFromOne @BR.CPSVOWHYNOT @BR.VoteWhyNotC intToVoteWhyNot)
            . (FT.addOneFromOne @BR.CPSSCHLCOLL @BR.InCollege intToInCollege)
            . (FT.addOneFromOne @BR.CPSEDUC @BR.CollegeGradC intToCollegeGrad)
            . (FT.addOneFromOne @BR.CPSCITIZEN @BR.IsCitizen intToIsCitizen)
            . (FT.addOneFromOne @BR.CPSSEX @BR.SexC intToSex)
            . (FT.addOneFromOne @BR.CPSAGE @BR.Age4C intToAge4)


{-
transformCPSVoterPUMSRow :: BR.CPSVoterPUMS_Raw -> F.Record CPSVoterPUMS'
transformCPSVoterPUMSRow r = F.rcast @CPSVoterPUMS' (mutate r) where
  addAge = FT.recordSingleton @BR.Age4C . intToAge4 . F.rgetField @BR.CPSAGE
  addSex =  FT.recordSingleton @BR.SexC . intToSex . F.rgetField @BR.CPSSEX
  hN = F.rgetField @BR.CPSHISPAN
  rN = F.rgetField @BR.CPSRACE
  addRace r = FT.recordSingleton @BR.Race5C $ intsToRace5 (hN r) (rN r)
  addIsCitizen = FT.recordSingleton @BR.IsCitizen . intToIsCitizen . F.rgetField @BR.CPSCITIZEN
  addCollegeGrad = FT.recordSingleton @BR.CollegeGradC . intToCollegeGrad . F.rgetField @BR.CPSEDUC
  addInCollege = FT.recordSingleton @BR.InCollege . intToInCollege . F.rgetField @BR.CPSSCHLCOLL
  addVoteWhyNot = FT.recordSingleton @BR.VoteWhyNotC . intToVoteWhyNot . F.rgetField @BR.CPSVOWHYNOT
  addRegWhyNot = FT.recordSingleton @BR.RegWhyNotC . intToRegWhyNot . F.rgetField @BR.CPSVOYNOTREG
  addVoteHow = FT.recordSingleton @BR.VoteHowC . intToVoteHow . F.rgetField @BR.CPSVOTEHOW
  addVoteWhen = FT.recordSingleton @BR.VoteWhenC . intToVoteWhen . F.rgetField @BR.CPSVOTEWHEN
  addVotedYN = FT.recordSingleton @BR.VotedYNC . intToVotedYN . F.rgetField @BR.CPSVOTED
  addRegisteredYN = FT.recordSingleton @BR.RegisteredYNC . intToRegisteredYN . F.rgetField @BR.CPSVOREG
  mutate = FT.retypeColumn @BR.CPSYEAR @BR.Year
           . FT.retypeColumn @BR.CPSSTATEFIP @BR.StateFIPS
           . FT.retypeColumn @BR.CPSCOUNTY @BR.CountyFIPS
           . FT.mutate addAge
           . FT.mutate addSex
           . FT.mutate addRace
           . FT.mutate addIsCitizen
           . FT.mutate addCollegeGrad
           . FT.mutate addInCollege
           . FT.mutate addVoteWhyNot
           . FT.mutate addRegWhyNot
           . FT.mutate addVoteHow
           . FT.mutate addVoteWhen
           . FT.mutate addVotedYN
           . FT.mutate addRegisteredYN
           . FT.retypeColumn @BR.CPSVOSUPPWT @CPSVoterPUMSWeight
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

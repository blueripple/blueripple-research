{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE UnicodeSyntax #-}

module BlueRipple.Model.Demographic.DataPrep
  (
    module BlueRipple.Model.Demographic.DataPrep
  )
where

import qualified BlueRipple.Data.ACS_PUMS as PUMS
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.GeographicTypes as GT
import qualified BlueRipple.Utilities.KnitUtils as BRK
import qualified BlueRipple.Data.DataFrames as BRDF

import qualified Control.MapReduce.Simple as MR
import qualified Frames.Transform as FT
import qualified Frames.Streamly.Transform as FST

import qualified Control.Foldl as FL
import qualified Data.Map as M

import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V
import qualified Data.Vector.Unboxed as VU
import qualified Frames as F
import qualified Frames.Melt as F
import qualified Knit.Report as K
import qualified Numeric


type Categoricals = [DT.CitizenC, DT.Age4C, DT.SexC, DT.Education4C, DT.RaceAlone4C, DT.HispC]
type ACSByCD = PUMS.CDCounts Categoricals
type ACSByState = PUMS.StateCounts Categoricals

acsByState ∷ F.FrameRec PUMS.PUMS → F.FrameRec ACSByState
acsByState acsByPUMA = F.rcast <$> FST.mapMaybe simplifyAgeM (FL.fold (PUMS.pumsStateRollupF (reKey .  F.rcast)) filteredACSByPUMA)
 where
  earliestYear = 2016
  earliest year = (>= year) . F.rgetField @BRDF.Year
  filteredACSByPUMA = F.filterFrame (earliest earliestYear) acsByPUMA


simplifyAgeM :: F.ElemOf rs DT.Age5FC => F.Record rs -> Maybe (F.Record (DT.Age4C ': rs))
simplifyAgeM r =
  let f g = Just $ FT.recordSingleton @DT.Age4C g F.<+> r
  in case F.rgetField @DT.Age5FC r of
    DT.A5F_Under18 -> Nothing
    DT.A5F_18To24 -> f DT.A4_18To24
    DT.A5F_25To44 -> f DT.A4_25To44
    DT.A5F_45To64 -> f DT.A4_45To64
    DT.A5F_65AndOver -> f DT.A4_65AndOver

shrinkEducation :: (F.ElemOf rs DT.EducationC) => F.Record rs -> F.Record (DT.Education4C ': rs)
shrinkEducation r = FT.recordSingleton @DT.Education4C (ed4 r)  F.<+> r where
  ed4 = DT.educationToEducation4 . F.rgetField @DT.EducationC

cachedACSByState
  ∷ ∀ r
   . (K.KnitEffects r, BRK.CacheEffects r)
  ⇒ K.ActionWithCacheTime r (F.FrameRec PUMS.PUMS)
  → K.Sem r (K.ActionWithCacheTime r (F.FrameRec ACSByState))
cachedACSByState acs_C = do
  BRK.retrieveOrMakeFrame "model/demographic/acsByState.bin" acs_C $
    pure . acsByState

reKey :: F.Record '[DT.CitizenC, DT.Age5FC, DT.SexC, DT.EducationC, DT.InCollege, DT.RaceAlone4C, DT.HispC]
        -> F.Record '[DT.CitizenC, DT.Age5FC, DT.SexC, DT.Education4C, DT.RaceAlone4C, DT.HispC]
reKey = F.rcast . shrinkEducation

{-
acsReKey
  ∷ F.Record '[DT.Age5FC, DT.SexC, DT.CollegeGradC, DT.InCollege, DT.RaceAlone4C, DT.HispC]
  → F.Record '[DT.Age5FC, DT.SexC, DT.CollegeGradC, DT.RaceAlone4C, DT.HispC]
acsReKey r =
  F.rgetField @DT.Age5FC r
  F.&: F.rgetField @DT.SexC r
  F.&: (if collegeGrad r || inCollege r then DT.Grad else DT.NonGrad)
  F.&: F.rgetField @DT.RaceAlone4C r
  F.&: F.rgetField @DT.HispC r
  F.&: V.RNil
-}
forMultinomial :: forall ks as bs rs l. (ks F.⊆ rs, as F.⊆ rs, Ord (F.Record ks), Enum l, Bounded l, Ord l)
               => (F.Record rs -> l) -- label
               -> (F.Record rs -> Int) -- count
               -> FL.Fold (F.Record as) (F.Record bs)
               -> FL.Fold (F.Record rs) [(F.Record (ks V.++ bs), VU.Vector Int)]
forMultinomial label count extraF =
  let vecF :: FL.Fold (l, Int) (VU.Vector Int)
      vecF = let zeroMap = M.fromList $ zip [(minBound :: l)..] $ repeat 0
             in VU.fromList . fmap snd . M.toList . M.unionWith (+) zeroMap <$> FL.foldByKeyMap FL.sum
--      lastMF :: FL.FoldM Maybe a a
--      lastMF = FL.FoldM (\_ a -> Just a) Nothing Just
      datF :: FL.Fold (F.Record as, (l, Int)) (F.Record bs, VU.Vector Int)
      datF = (,) <$> FL.premap fst extraF <*> FL.premap snd vecF
  in MR.concatFold
     $ MR.mapReduceFold
     MR.noUnpack
     (MR.assign (F.rcast @ks) (\r -> (F.rcast @as r, (label r, count r))))
     (MR.foldAndLabel datF (\ks (bs, v) -> [(ks F.<+> bs, v)]))



type ACSByStateAgeMNR =  [BRDF.Year, GT.StateAbbreviation, BRDF.StateFIPS, DT.CitizenC, DT.SexC, DT.Education4C, DT.RaceAlone4C, DT.HispC, DT.PopPerSqMile]
type ACSByStateAgeMN = (F.Record ACSByStateAgeMNR, VU.Vector Int)

type ACSByStateEduMNR = [BRDF.Year, GT.StateAbbreviation, BRDF.StateFIPS, DT.CitizenC, DT.Age4C, DT.SexC, DT.RaceAlone4C, DT.HispC, DT.PopPerSqMile]
type ACSByStateEduMN = (F.Record ACSByStateEduMNR, VU.Vector Int)

densityF :: FL.Fold (F.Record [DT.PopCount, DT.PopPerSqMile]) (F.Record '[DT.PopPerSqMile])
densityF =
  let nPeople r = F.rgetField @DT.PopCount r
      density r = F.rgetField @DT.PopPerSqMile r
      f r = (realToFrac $ nPeople r, density r)
  in FT.recordSingleton @DT.PopPerSqMile <$> FL.premap f PUMS.densityF

geomDensityF :: FL.Fold (Double, Double) Double
geomDensityF =
  let wgtF = FL.premap fst FL.sum
      wgtSumF = Numeric.exp <$> FL.premap (\(w, d) -> w * safeLog d) FL.sum
  in (/) <$> wgtSumF <*> wgtF
{-# INLINE geomDensityF #-}

filterZeroes :: [(a, VU.Vector Int)] -> [(a, VU.Vector Int)]
filterZeroes = filter (\(_, v) -> v VU.! 0 > 0 || v VU.! 1 > 0)

acsByStateAgeMN :: F.FrameRec ACSByState -> [ACSByStateAgeMN]
acsByStateAgeMN = filterZeroes
                  . FL.fold (forMultinomial @[BRDF.Year, GT.StateAbbreviation, BRDF.StateFIPS, DT.CitizenC, DT.SexC, DT.Education4C, DT.RaceAlone4C, DT.HispC]
                             (DT.age4ToSimple . F.rgetField @DT.Age4C)
                             (F.rgetField @DT.PopCount)
                             densityF
                            )

acsByStateEduMN :: F.FrameRec ACSByState -> [ACSByStateEduMN]
acsByStateEduMN = filterZeroes
                  .  FL.fold (forMultinomial @[BRDF.Year, GT.StateAbbreviation, BRDF.StateFIPS, DT.CitizenC, DT.Age4C, DT.SexC, DT.RaceAlone4C, DT.HispC]
                              (\r -> DT.education4ToCollegeGrad (F.rgetField @DT.Education4C r) == DT.Grad)
                              (F.rgetField @DT.PopCount)
                              densityF
                             )

collegeGrad :: (F.ElemOf rs DT.EducationC, F.ElemOf rs DT.InCollege) => F.Record rs -> Bool
collegeGrad r = F.rgetField @DT.InCollege r || F.rgetField @DT.EducationC r `elem` [DT.BA, DT.AD]

inCollege :: F.ElemOf rs DT.InCollege => F.Record rs -> Bool
inCollege = F.rgetField @DT.InCollege

-- if you are in college we treat it like you have a BA ??
educationWithInCollege :: (F.ElemOf rs DT.EducationC, F.ElemOf rs DT.InCollege)
                     => F.Record rs -> DT.Education
educationWithInCollege r = case inCollege r of
  False -> F.rgetField @DT.EducationC r
  True -> case F.rgetField @DT.EducationC r of
    DT.L9 -> DT.BA
    DT.L12 -> DT.BA
    DT.HS -> DT.BA
    DT.SC -> DT.BA
    DT.AS -> DT.BA
    DT.BA -> DT.BA
    DT.AD -> DT.AD


districtKey :: (F.ElemOf rs GT.StateAbbreviation, F.ElemOf rs GT.CongressionalDistrict) => F.Record rs -> Text
districtKey r = F.rgetField @GT.StateAbbreviation r <> "-" <> show (F.rgetField @GT.CongressionalDistrict r)

logDensityPredictor :: F.ElemOf rs DT.PopPerSqMile => F.Record rs -> VU.Vector Double
logDensityPredictor = safeLogV . F.rgetField @DT.PopPerSqMile

safeLog :: Double -> Double
safeLog x =  if x < 1e-12 then 0 else Numeric.log x -- won't matter because Pop will be 0 here

safeLogV :: Double -> K.Vector Double
safeLogV x =  VU.singleton $ safeLog x

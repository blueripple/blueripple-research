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
import qualified BlueRipple.Data.Loaders as BRL
import qualified BlueRipple.Data.BasicRowFolds as BRF

import qualified Control.MapReduce.Simple as MR
import qualified Frames.Transform as FT
import qualified Frames.SimpleJoins as FJ
--import qualified Frames.Streamly.Transform as FST

import qualified Control.Foldl as FL
import qualified Control.Lens as Lens
import Control.Lens (view , (^.))
import qualified Data.Map as M

import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V
import qualified Data.Vector.Unboxed as VU
import qualified Frames as F
import qualified Frames.Melt as F
import qualified Knit.Report as K
import qualified Knit.Utilities.Streamly as K
import qualified Numeric



type Categoricals = [DT.CitizenC, DT.Age4C, DT.SexC, DT.Education4C, DT.Race5C]
type DatFieldsFrom = [PUMS.PUMSWeight, DT.PopPerSqMile]
type DatFieldsTo = [DT.PopCount, DT.PWPopPerSqMile]

type ACSByStateGeoRF = [BRDF.Year, GT.StateFIPS]
type ACSByStateGeoR = [BRDF.Year, GT.StateAbbreviation, GT.StateFIPS]

type ACSByStateRF = ACSByStateGeoRF V.++ Categoricals V.++ DatFieldsTo
type ACSByStateR = ACSByStateGeoR V.++ Categoricals V.++ DatFieldsTo

type ACSByPUMAGeoRF = [BRDF.Year, GT.StateFIPS, GT.PUMA]
type ACSByPUMAGeoR = [BRDF.Year, GT.StateAbbreviation, GT.StateFIPS, GT.PUMA]

type ACSByPUMARF = ACSByPUMAGeoRF V.++ Categoricals V.++ DatFieldsTo
type ACSByPUMAR = ACSByPUMAGeoR V.++ Categoricals V.++ DatFieldsTo


type ACSWorkingR = DT.Age4C ': PUMS.PUMS_Typed

acsFixAgeYear :: F.Record PUMS.PUMS_Typed -> Maybe (F.Record ACSWorkingR)
acsFixAgeYear r = do
  guard (F.rgetField @BRDF.Year r == 2020)
  simplifyAgeM r

acsByStateKeys :: F.Record ACSWorkingR -> F.Record (ACSByStateGeoRF V.++ Categoricals)
acsByStateKeys = F.rcast . addEdu4 . addRace5

acsByPUMAKeys :: F.Record ACSWorkingR -> F.Record (ACSByPUMAGeoRF V.++ Categoricals)
acsByPUMAKeys = F.rcast . addEdu4 . addRace5

datFieldsFrom :: (DatFieldsFrom F.⊆ rs) => F.Record rs -> F.Record DatFieldsFrom
datFieldsFrom = F.rcast

datFromToFld :: FL.Fold (F.Record DatFieldsFrom) (F.Record DatFieldsTo)
datFromToFld =
  let wgt = F.rgetField @PUMS.PUMSWeight
      density = F.rgetField @DT.PopPerSqMile
      countFld = FL.premap wgt FL.sum
      densityFld = FL.premap (\r -> (realToFrac (wgt r), density r)) PUMS.wgtdDensityF
  in (\pc d -> pc F.&: d F.&: V.RNil) <$> countFld <*> densityFld

acsByStateRF :: F.FrameRec PUMS.PUMS_Typed -> K.StreamlyM (F.FrameRec ACSByStateRF)
acsByStateRF = BRF.rowFold acsFixAgeYear acsByStateKeys datFieldsFrom datFromToFld

acsByPUMARF :: F.FrameRec PUMS.PUMS_Typed -> K.StreamlyM (F.FrameRec ACSByPUMARF)
acsByPUMARF = BRF.rowFold acsFixAgeYear acsByPUMAKeys datFieldsFrom datFromToFld

cachedACSByState' :: (K.KnitEffects r, BRK.CacheEffects r) => K.Sem r (K.ActionWithCacheTime r (F.FrameRec ACSByStateR))
cachedACSByState' = K.wrapPrefix "Model.Demographic.cachedACSByState" $ do
  rawACS_C <- PUMS.typedPUMSRowsLoader
  stateAbbrXWalk_C <- BRL.stateAbbrCrosswalkLoader
  let deps = (,) <$> rawACS_C <*> stateAbbrXWalk_C
  BRK.retrieveOrMakeFrame "model/demographic/data/acs2020ByState.bin" deps $ \(acs, xWalk) -> do
    K.logLE K.Info "Cached doesn't exist or is older than dependencies. Loading raw ACS rows..."
    K.logLE K.Info $ "raw ACS has " <> show (FL.fold FL.length acs) <> " rows. Aggregating..."
    aggregatedACS <- K.streamlyToKnit $ acsByStateRF acs
    K.logLE K.Info $ "aggregated ACS (by State) has " <> show (FL.fold FL.length aggregatedACS) <> " rows. Adding state abbreviations..."
    let (withSA, missing) = FJ.leftJoinWithMissing @'[GT.StateFIPS] aggregatedACS xWalk
    when (not $ null missing) $ K.knitError $ "Missing abbreviations in acsByState' left join: " <> show missing
    K.logLE K.Info $ "Done"
    pure $ fmap F.rcast withSA

cachedACSByPUMA :: (K.KnitEffects r, BRK.CacheEffects r) => K.Sem r (K.ActionWithCacheTime r (F.FrameRec ACSByPUMAR))
cachedACSByPUMA = K.wrapPrefix "Model.Demographic.cachedACSByPUMA" $ do
  rawACS_C <- PUMS.typedPUMSRowsLoader
  stateAbbrXWalk_C <- BRL.stateAbbrCrosswalkLoader
  let deps = (,) <$> rawACS_C <*> stateAbbrXWalk_C
  BRK.retrieveOrMakeFrame "model/demographic/data/acs2020ByPUMA.bin" deps $ \(acs, xWalk) -> do
    K.logLE K.Info "Cached doesn't exist or is older than dependencies. Loading raw ACS rows..."
    K.logLE K.Info $ "raw ACS has " <> show (FL.fold FL.length acs) <> " rows. Aggregating..."
    aggregatedACS <- K.streamlyToKnit $ acsByPUMARF acs
    K.logLE K.Info $ "aggregated ACS (by PUMA) has " <> show (FL.fold FL.length aggregatedACS) <> " rows. Adding state abbreviations..."
    let (withSA, missing) = FJ.leftJoinWithMissing @'[GT.StateFIPS] aggregatedACS xWalk
    when (not $ null missing) $ K.knitError $ "Missing abbreviations in acsByState' left join: " <> show missing
    K.logLE K.Info $ "Done"
    pure $ fmap F.rcast withSA


simplifyAgeM :: F.ElemOf rs DT.Age5FC => F.Record rs -> Maybe (F.Record (DT.Age4C ': rs))
simplifyAgeM r =
  let f g = Just $ FT.recordSingleton @DT.Age4C g F.<+> r
  in case F.rgetField @DT.Age5FC r of
    DT.A5F_Under18 -> Nothing
    DT.A5F_18To24 -> f DT.A4_18To24
    DT.A5F_25To44 -> f DT.A4_25To44
    DT.A5F_45To64 -> f DT.A4_45To64
    DT.A5F_65AndOver -> f DT.A4_65AndOver

addEdu4 :: (F.ElemOf rs DT.EducationC) => F.Record rs -> F.Record (DT.Education4C ': rs)
addEdu4 r = ed4 F.&: r where
  ed4 = DT.educationToEducation4 $ F.rgetField @DT.EducationC r

addRace5 :: (F.ElemOf rs DT.HispC, F.ElemOf rs DT.RaceAlone4C) => F.Record rs -> F.Record (DT.Race5C ': rs)
addRace5 r = r5 F.&: r
  where r5 = DT.race5FromRaceAlone4AndHisp True (F.rgetField @DT.RaceAlone4C r) (F.rgetField @DT.HispC r)

forMultinomial :: forall ks as bs rs l.
                  (as F.⊆ rs
                  , Ord (F.Record ks)
                  , Enum l, Bounded l, Ord l
                  )
               => (F.Record rs -> F.Record ks) -- category keys
               -> (F.Record rs -> l) -- label
               -> (F.Record rs -> Int) -- count
               -> FL.Fold (F.Record as) (F.Record bs)
               -> FL.Fold (F.Record rs) [(F.Record (ks V.++ bs), VU.Vector Int)]
forMultinomial cKeys label count extraF =
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
     (MR.assign cKeys (\r -> (F.rcast @as r, (label r, count r))))
     (MR.foldAndLabel datF (\ks (bs, v) -> [(ks F.<+> bs, v)]))

type ACSByStateCitizenMNR =  [BRDF.Year, GT.StateAbbreviation, BRDF.StateFIPS, DT.SexC, DT.Education4C, DT.Race5C, DT.PWPopPerSqMile]
type ACSByStateCitizenMN = (F.Record ACSByStateCitizenMNR, VU.Vector Int)

type ACSByStateAgeMNR =  [BRDF.Year, GT.StateAbbreviation, BRDF.StateFIPS, DT.CitizenC, DT.SexC, DT.Education4C, DT.Race5C, DT.PWPopPerSqMile]
type ACSByStateAgeMN = (F.Record ACSByStateAgeMNR, VU.Vector Int)

type ACSByStateEduMNR = [BRDF.Year, GT.StateAbbreviation, BRDF.StateFIPS, DT.CitizenC, DT.Age4C, DT.SexC, DT.Race5C, DT.PWPopPerSqMile]
type ACSByStateEduMN = (F.Record ACSByStateEduMNR, VU.Vector Int)

wgtdDensityF :: FL.Fold (F.Record [DT.PopCount, DT.PWPopPerSqMile]) (F.Record '[DT.PWPopPerSqMile])
wgtdDensityF =
  let nPeople r = F.rgetField @DT.PopCount r
      density r = F.rgetField @DT.PWPopPerSqMile r
      f r = (realToFrac $ nPeople r, density r)
  in FT.recordSingleton @DT.PWPopPerSqMile <$> FL.premap f PUMS.wgtdDensityF
{-# INLINE wgtdDensityF #-}

geomWgtdDensityF :: FL.Fold (Double, Double) Double
geomWgtdDensityF =
  let wgtF = FL.premap fst FL.sum
      wgtSumF = Numeric.exp <$> FL.premap (\(w, d) -> w * safeLog d) FL.sum
  in (/) <$> wgtSumF <*> wgtF
{-# INLINE geomWgtdDensityF #-}

aggregatePeopleAndDensityF :: FL.Fold (F.Record [DT.PopCount, DT.PWPopPerSqMile]) (F.Record [DT.PopCount, DT.PWPopPerSqMile])
aggregatePeopleAndDensityF = (\pc pwd -> FT.recordSingleton @DT.PopCount pc F.<+> pwd) <$> FL.premap (view DT.popCount) FL.sum <*> wgtdDensityF

filterZeroes :: [(a, VU.Vector Int)] -> [(a, VU.Vector Int)]
filterZeroes = filter (\(_, v) -> v VU.! 0 > 0 || v VU.! 1 > 0)

type ACSByStateMNR ks = [BRDF.Year, GT.StateAbbreviation, BRDF.StateFIPS] V.++ ks V.++ '[DT.PWPopPerSqMile]
type ACSByStateMNT ks = (F.Record (ACSByStateMNR ks), VU.Vector Int)

acsByStateMN :: forall ks l . (Ord (F.Record ks), Enum l, Bounded l, Ord l
--                              , ks F.⊆ ACSByStateR
                              )
             =>  (F.Record ACSByStateR -> F.Record ks) -> (F.Record ACSByStateR -> l) -> F.FrameRec ACSByStateR -> [ACSByStateMNT ks]
acsByStateMN cKey label = filterZeroes .
                          FL.fold (forMultinomial @([BRDF.Year, GT.StateAbbreviation, BRDF.StateFIPS] V.++ ks)
                                   (\r -> F.rcast @[BRDF.Year, GT.StateAbbreviation, BRDF.StateFIPS] r F.<+> cKey r)
                                   label
                                   (view DT.popCount)
                                   wgtdDensityF
                                  )

acsByStateCitizenMN :: F.FrameRec ACSByStateR -> [ACSByStateCitizenMN]
acsByStateCitizenMN = acsByStateMN (F.rcast @[DT.SexC, DT.Education4C, DT.Race5C]) (view DT.citizenC)

acsByStateAgeMN :: F.FrameRec ACSByStateR -> [ACSByStateAgeMN]
acsByStateAgeMN = acsByStateMN (F.rcast @[DT.CitizenC, DT.SexC, DT.Education4C, DT.Race5C]) (view DT.age4C)


acsByStateEduMN :: F.FrameRec ACSByStateR -> [ACSByStateEduMN]
acsByStateEduMN = acsByStateMN (F.rcast @[DT.CitizenC, DT.Age4C, DT.SexC, DT.Race5C]) ((== DT.Grad) . DT.education4ToCollegeGrad . view DT.education4C)


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

logDensityPredictor :: F.ElemOf rs DT.PWPopPerSqMile => F.Record rs -> VU.Vector Double
logDensityPredictor = safeLogV . F.rgetField @DT.PWPopPerSqMile

safeLog :: Double -> Double
safeLog x =  if x < 1e-12 then 0 else Numeric.log x -- won't matter because Pop will be 0 here

safeLogV :: Double -> K.Vector Double
safeLogV x =  VU.singleton $ safeLog x

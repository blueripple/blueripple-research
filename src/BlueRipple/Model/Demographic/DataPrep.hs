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
import qualified BlueRipple.Utilities.FramesUtils as BRF

import qualified Control.MapReduce.Simple as MR
import qualified Frames.MapReduce as FMR
import qualified Frames.Transform as FT
import qualified Frames.SimpleJoins as FJ
import qualified Frames.Streamly.Transform as FST

import qualified Control.Foldl as FL
import qualified Control.Lens as Lens
import Control.Lens (view , (^.), over)
import qualified Data.Map as M

import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V
import qualified Data.Vector.Unboxed as VU
import qualified Frames as F
import qualified Frames.Melt as F
import qualified Frames.Serialize as FS
import qualified Frames.Streamly.InCore as FSI
import qualified Knit.Report as K
import qualified Knit.Utilities.Streamly as K
import qualified Numeric
import qualified BlueRipple.Data.Loaders as BR


-- without Under 18s
type CategoricalsA5 = [DT.CitizenC, DT.Age5C, DT.SexC, DT.Education4C, DT.Race5C]

-- with under 18s
type CategoricalsA6 = [DT.CitizenC, DT.Age6C, DT.SexC, DT.Education4C, DT.Race5C]

type DatFieldsFrom = [PUMS.PUMSWeight, DT.PopPerSqMile]
type DatFieldsTo = [DT.PopCount, DT.PWPopPerSqMile]

type ACSByStateGeoRF = [BRDF.Year, GT.StateFIPS]
type ACSByStateGeoR = [BRDF.Year, GT.StateAbbreviation, GT.StateFIPS]

type ACSa5ByStateRF = ACSByStateGeoRF V.++ CategoricalsA5 V.++ DatFieldsTo
type ACSa5ByStateR = ACSByStateGeoR V.++ CategoricalsA5 V.++ DatFieldsTo

type ACSa6ByStateRF = ACSByStateGeoRF V.++ CategoricalsA6 V.++ DatFieldsTo
type ACSa6ByStateR = ACSByStateGeoR V.++ CategoricalsA6 V.++ DatFieldsTo

type ACSByPUMAGeoRF = [BRDF.Year, GT.StateFIPS, GT.PUMA]
type ACSByPUMAGeoR = [BRDF.Year, GT.StateAbbreviation, GT.StateFIPS, GT.PUMA]

type ACSa5ByPUMARF = ACSByPUMAGeoRF V.++ CategoricalsA5 V.++ DatFieldsTo
type ACSa5ByPUMAR = ACSByPUMAGeoR V.++ CategoricalsA5 V.++ DatFieldsTo

type ACSa6ByPUMARF = ACSByPUMAGeoRF V.++ CategoricalsA6 V.++ DatFieldsTo
type ACSa6ByPUMAR = ACSByPUMAGeoR V.++ CategoricalsA6 V.++ DatFieldsTo

type ACSByCDGeoRF = [BRDF.Year, GT.StateFIPS, GT.CongressionalDistrict]
type ACSByCDGeoR = [BRDF.Year, GT.StateAbbreviation, GT.StateFIPS, GT.CongressionalDistrict]

type ACSa5ByCDRF = ACSByCDGeoRF V.++ CategoricalsA5 V.++ DatFieldsTo
type ACSa5ByCDR = ACSByCDGeoR V.++ CategoricalsA5 V.++ DatFieldsTo

type ACSa6ByCDRF = ACSByCDGeoRF V.++ CategoricalsA6 V.++ DatFieldsTo
type ACSa6ByCDR = ACSByCDGeoR V.++ CategoricalsA6 V.++ DatFieldsTo

type ACSa5WorkingR = DT.Age5C ': PUMS.PUMS_Typed

acsFixAgeYear :: Int -> F.Record PUMS.PUMS_Typed -> Maybe (F.Record ACSa5WorkingR)
acsFixAgeYear y r = do
  guard (F.rgetField @BRDF.Year r == y)
  simplifyAgeM r

acsFixYear :: Int -> F.Record PUMS.PUMS_Typed -> Maybe (F.Record PUMS.PUMS_Typed)
acsFixYear y r = do
  guard (F.rgetField @BRDF.Year r == y)
  pure r


acsA5ByStateKeys :: F.Record ACSa5WorkingR -> F.Record (ACSByStateGeoRF V.++ CategoricalsA5)
acsA5ByStateKeys = F.rcast . addEdu4 . addRace5

acsA6ByStateKeys :: F.Record PUMS.PUMS_Typed -> F.Record (ACSByStateGeoRF V.++ CategoricalsA6)
acsA6ByStateKeys = F.rcast . addEdu4 . addRace5

acsA5ByPUMAKeys :: F.Record ACSa5WorkingR -> F.Record (ACSByPUMAGeoRF V.++ CategoricalsA5)
acsA5ByPUMAKeys = F.rcast . addEdu4 . addRace5

acsA6ByPUMAKeys :: F.Record PUMS.PUMS_Typed -> F.Record (ACSByPUMAGeoRF V.++ CategoricalsA6)
acsA6ByPUMAKeys = F.rcast . addEdu4 . addRace5

datFieldsFrom :: (DatFieldsFrom F.⊆ rs) => F.Record rs -> F.Record DatFieldsFrom
datFieldsFrom = F.rcast

datFromToFld :: FL.Fold (F.Record DatFieldsFrom) (F.Record DatFieldsTo)
datFromToFld =
  let wgt = F.rgetField @PUMS.PUMSWeight
      density = F.rgetField @DT.PopPerSqMile
      countFld = FL.premap wgt FL.sum
      densityFld = FL.premap (\r -> (realToFrac (wgt r), density r)) PUMS.wgtdDensityF
  in (\pc d -> pc F.&: d F.&: V.RNil) <$> countFld <*> densityFld

acsA5ByStateRF :: Int -> F.FrameRec PUMS.PUMS_Typed -> K.StreamlyM (F.FrameRec ACSa5ByStateRF)
acsA5ByStateRF y = BRF.rowFold (acsFixAgeYear y) acsA5ByStateKeys datFieldsFrom datFromToFld

acsA6ByStateRF :: Int -> F.FrameRec PUMS.PUMS_Typed -> K.StreamlyM (F.FrameRec ACSa6ByStateRF)
acsA6ByStateRF y = BRF.rowFold (acsFixYear y) acsA6ByStateKeys datFieldsFrom datFromToFld

acsA5ByPUMARF :: Int -> F.FrameRec PUMS.PUMS_Typed -> K.StreamlyM (F.FrameRec ACSa5ByPUMARF)
acsA5ByPUMARF y = BRF.rowFold (acsFixAgeYear y) acsA5ByPUMAKeys datFieldsFrom datFromToFld

acsA6ByPUMARF :: Int -> F.FrameRec PUMS.PUMS_Typed -> K.StreamlyM (F.FrameRec ACSa6ByPUMARF)
acsA6ByPUMARF y = BRF.rowFold (acsFixYear y) acsA6ByPUMAKeys datFieldsFrom datFromToFld

type ACSa5ModelRow =  [BRDF.Year, GT.StateAbbreviation, GT.StateFIPS, GT.PUMA] V.++ CategoricalsA5 V.++ DatFieldsFrom
type ACSa6ModelRow =  [BRDF.Year, GT.StateAbbreviation, GT.StateFIPS, GT.PUMA] V.++ CategoricalsA6 V.++ DatFieldsFrom

--acsSource :: (K.KnitEffects r, BRK.CacheEffects r) => K.Sem r (K.ActionWithCacheTime r (F.FrameRec PUMS.PUMS_Typed))
--acsSource = acs1Yr

cachedACSa5 :: (K.KnitEffects r, BRK.CacheEffects r)
  => K.Sem r (K.ActionWithCacheTime r (F.FrameRec PUMS.PUMS_Typed))
  -> Int
  -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec ACSa5ModelRow))
cachedACSa5 source year = do
  typedACS_C <- source
  stateAbbrXWalk_C <- BRL.stateAbbrCrosswalkLoader
  let cacheKey = "model/demographic/data/acs" <> show year <> "_a5.bin"
      typedACSForYear_C = F.filterFrame ((== year) . view BRDF.year) <$> typedACS_C
      deps = (,) <$> typedACSForYear_C <*> stateAbbrXWalk_C
  BRK.retrieveOrMakeFrame cacheKey deps $ \(acs, xWalk) -> do
    K.logLE K.Info "Cached doesn't exist or is older than dependencies. Loading raw ACS rows..."
    K.logLE K.Info $ "raw ACS has " <> show (FL.fold FL.length acs) <> " rows. Adding state abbreviations..."
    let (withSA, missing) = FJ.leftJoinWithMissing @'[GT.StateFIPS] acs xWalk
    when (not $ null missing) $ K.knitError $ "Missing abbreviations in cachedCSWithAbbreviations left join: " <> show missing
    K.logLE K.Info $ "Done"
    pure $ FST.mapMaybe (fmap F.rcast . simplifyAgeM . addEdu4 . addRace5) withSA

cachedACSa6 :: (K.KnitEffects r, BRK.CacheEffects r)
  => K.Sem r (K.ActionWithCacheTime r (F.FrameRec PUMS.PUMS_Typed))
  -> Int
  -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec ACSa6ModelRow))
cachedACSa6 source year = do
  typedACS_C <- source
  stateAbbrXWalk_C <- BRL.stateAbbrCrosswalkLoader
  let cacheKey = "model/demographic/data/acs" <> show year <> "_a6.bin"
      typedACSForYear_C = F.filterFrame ((== year) . view BRDF.year) <$> typedACS_C
      deps = (,) <$> typedACSForYear_C <*> stateAbbrXWalk_C
  BRK.retrieveOrMakeFrame cacheKey deps $ \(acs, xWalk) -> do
    K.logLE K.Info "Cached doesn't exist or is older than dependencies. Loading raw ACS rows..."
    K.logLE K.Info $ "raw ACS has " <> show (FL.fold FL.length acs) <> " rows. Adding state abbreviations..."
    let (withSA, missing) = FJ.leftJoinWithMissing @'[GT.StateFIPS] acs xWalk
    when (not $ null missing) $ K.knitError $ "Missing abbreviations in cachedCSWithAbbreviations left join: " <> show missing
    K.logLE K.Info $ "Done"
    pure $ fmap (F.rcast . addEdu4 . addRace5) withSA

cachedACSa5ByState :: (K.KnitEffects r, BRK.CacheEffects r)
                   => K.Sem r (K.ActionWithCacheTime r (F.FrameRec PUMS.PUMS_Typed))
                   -> Int
                   -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec ACSa5ByStateR))
cachedACSa5ByState source year = K.wrapPrefix "Model.Demographic.cachedACSa5ByState" $ do
  typedACS_C <- source
  stateAbbrXWalk_C <- BRL.stateAbbrCrosswalkLoader
  let cacheKey = "model/demographic/data/acs" <> show year <> "ByState_a5.bin"
      typedACSForYear_C = F.filterFrame ((== year) . view BRDF.year) <$> typedACS_C
      deps = (,) <$> typedACSForYear_C <*> stateAbbrXWalk_C
  BRK.retrieveOrMakeFrame cacheKey deps $ \(acs, xWalk) -> do
    K.logLE K.Info "Cached doesn't exist or is older than dependencies. Loading raw ACS rows..."
    K.logLE K.Info $ "raw ACS has " <> show (FL.fold FL.length acs) <> " rows. Aggregating..."
    aggregatedACS <- K.streamlyToKnit $ acsA5ByStateRF year acs
    K.logLE K.Info $ "aggregated ACS (by State) has " <> show (FL.fold FL.length aggregatedACS) <> " rows. Adding state abbreviations..."
    let (withSA, missing) = FJ.leftJoinWithMissing @'[GT.StateFIPS] aggregatedACS xWalk
    when (not $ null missing) $ K.knitError $ "Missing abbreviations in acsByState' left join: " <> show missing
    K.logLE K.Info $ "Done"
    pure $ fmap F.rcast withSA

cachedACSa6ByState :: (K.KnitEffects r, BRK.CacheEffects r)
                   => K.Sem r (K.ActionWithCacheTime r (F.FrameRec PUMS.PUMS_Typed))
                   -> Int
                   -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec ACSa6ByStateR))
cachedACSa6ByState source year = K.wrapPrefix "Model.Demographic.cachedACSa6ByState" $ do
  typedACS_C <- source
  stateAbbrXWalk_C <- BRL.stateAbbrCrosswalkLoader
  let cacheKey = "model/demographic/data/acs" <> show year <> "ByState_a6.bin"
      typedACSForYear_C = F.filterFrame ((== year) . view BRDF.year) <$> typedACS_C
      deps = (,) <$> typedACSForYear_C <*> stateAbbrXWalk_C
  BRK.retrieveOrMakeFrame cacheKey deps $ \(acs, xWalk) -> do
    K.logLE K.Info "Cached doesn't exist or is older than dependencies. Loading raw ACS rows..."
    K.logLE K.Info $ "raw ACS has " <> show (FL.fold FL.length acs) <> " rows. Aggregating..."
    aggregatedACS <- K.streamlyToKnit $ acsA6ByStateRF year acs
    K.logLE K.Info $ "aggregated ACS (by State) has " <> show (FL.fold FL.length aggregatedACS) <> " rows. Adding state abbreviations..."
    let (withSA, missing) = FJ.leftJoinWithMissing @'[GT.StateFIPS] aggregatedACS xWalk
    when (not $ null missing) $ K.knitError $ "Missing abbreviations in acsByState' left join: " <> show missing
    K.logLE K.Info $ "Done"
    pure $ fmap F.rcast withSA

cachedACSa5ByPUMA :: (K.KnitEffects r, BRK.CacheEffects r)
                  => K.Sem r (K.ActionWithCacheTime r (F.FrameRec PUMS.PUMS_Typed))
                  -> Int
                  -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec ACSa5ByPUMAR))
cachedACSa5ByPUMA source year = K.wrapPrefix "Model.Demographic.cachedACSa5ByPUMA" $ do
  typedACS_C <- source
  stateAbbrXWalk_C <- BRL.stateAbbrCrosswalkLoader
  let cacheKey = "model/demographic/data/acs" <> show year <> "ByPUMA_a5.bin"
      deps = (,) <$> typedACS_C <*> stateAbbrXWalk_C
  BRK.retrieveOrMakeFrame cacheKey deps $ \(acs, xWalk) -> do
    K.logLE K.Info "Cached doesn't exist or is older than dependencies. Loading raw ACS rows..."
    let acsForYear =  F.filterFrame ((== year) . view BRDF.year) acs
    K.logLE K.Info $ "full ACS has " <> show (FL.fold FL.length acs) <> " rows."
    let lengthACSForYear = FL.fold FL.length acsForYear
    when (lengthACSForYear == 0) $ BRK.logFrame $ F.takeRows 100 acs
    K.logLE K.Info $ show year <> " ACS has " <> show lengthACSForYear <> " rows. Aggregating..."
    aggregatedACS <- K.streamlyToKnit $ acsA5ByPUMARF year acs
    K.logLE K.Info $ "aggregated ACS (by PUMA) has " <> show (FL.fold FL.length aggregatedACS) <> " rows. Adding state abbreviations..."
    let (withSA, missing) = FJ.leftJoinWithMissing @'[GT.StateFIPS] aggregatedACS xWalk
    when (not $ null missing) $ K.knitError $ "Missing abbreviations in acsByState' left join: " <> show missing
    K.logLE K.Info $ "Done"
    pure $ fmap F.rcast withSA

cachedACSa6ByPUMA :: (K.KnitEffects r, BRK.CacheEffects r)
                  => K.Sem r (K.ActionWithCacheTime r (F.FrameRec PUMS.PUMS_Typed))
                  -> Int
                  -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec ACSa6ByPUMAR))
cachedACSa6ByPUMA source year = K.wrapPrefix "Model.Demographic.cachedACSa6ByPUMA" $ do
  typedACS_C <- source
  stateAbbrXWalk_C <- BRL.stateAbbrCrosswalkLoader
  let cacheKey = "model/demographic/data/acs" <> show year <> "ByPUMA_a6.bin"
      typedACSForYear_C = F.filterFrame ((== year) . view BRDF.year) <$> typedACS_C
      deps = (,) <$> typedACSForYear_C <*> stateAbbrXWalk_C
  BRK.retrieveOrMakeFrame cacheKey deps $ \(acs, xWalk) -> do
    K.logLE K.Info "Cached doesn't exist or is older than dependencies. Loading raw ACS rows..."
    K.logLE K.Info $ "raw ACS has " <> show (FL.fold FL.length acs) <> " rows. Aggregating..."
    aggregatedACS <- K.streamlyToKnit $ acsA6ByPUMARF year acs
    K.logLE K.Info $ "aggregated ACS (by PUMA) has " <> show (FL.fold FL.length aggregatedACS) <> " rows. Adding state abbreviations..."
    let (withSA, missing) = FJ.leftJoinWithMissing @'[GT.StateFIPS] aggregatedACS xWalk
    when (not $ null missing) $ K.knitError $ "Missing abbreviations in acsByState' left join: " <> show missing
    K.logLE K.Info $ "Done"
    pure $ fmap F.rcast withSA

type PUMAToCDRow ks = [BRDF.Year, GT.StateAbbreviation, GT.StateFIPS, GT.PUMA] V.++ ks V.++ DatFieldsTo
type CDFromPUMARow ks = [BRDF.Year, GT.StateAbbreviation, GT.StateFIPS, GT.CongressionalDistrict] V.++ ks V.++ DatFieldsTo
type PUMAJoinKey = [BRDF.Year, GT.StateAbbreviation, GT.StateFIPS, GT.PUMA]
type XRow ks = (ks V.++ DatFieldsTo) V.++ [GT.CongressionalDistrict, BRDF.Population, BRDF.FracCDInPUMA, BRDF.FracPUMAInCD]

cachedPUMAsToCDs :: forall ks r .
                    (FS.RecFlat (ks V.++ DatFieldsTo)
                    , FJ.CanLeftJoinM PUMAJoinKey (PUMAToCDRow ks) BR.DatedCDFromPUMA2012
                    , FSI.RecVec (ks V.++ DatFieldsTo)
                    , V.RMap (ks V.++ DatFieldsTo)
                    , Ord (F.Record ks)
                    , ks F.⊆ (PUMAJoinKey V.++ XRow ks)
                    , F.ElemOf (XRow ks) GT.CongressionalDistrict
                    , F.ElemOf (XRow ks) BRDF.FracPUMAInCD
                    , F.ElemOf (XRow ks) DT.PopCount
                    , F.ElemOf (XRow ks) DT.PWPopPerSqMile
                    , K.KnitEffects r
                    , BRK.CacheEffects r
                    )
                 => Text
                 -> K.ActionWithCacheTime r (F.FrameRec (PUMAToCDRow ks))
                 -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec (CDFromPUMARow ks)))
cachedPUMAsToCDs cacheKey byPUMAs_C = do
  cdFromPUMA_C <- BR.allCDFromPUMA2012Loader
--  K.ignoreCacheTime cdFromPUMA_C >>= BRK.logFrame . F.filterFrame ((== 2022) . view BRDF.year)
  let deps = (,) <$> byPUMAs_C <*> cdFromPUMA_C
  BRK.retrieveOrMakeFrame cacheKey deps $ \(byPUMAs, cdFromPUMA) -> do
    K.logLE K.Info $ "cachedPUMAsToCDs (" <> cacheKey <> "): Cached doesn't exist or is older than dependencies. Loading byPUMAs rows..."
    K.logLE K.Info $ "byPUMAs (" <> cacheKey <> ") has " <> show (FL.fold FL.length byPUMAs) <> " rows. Joining with CD weights..."
    let (byPUMAWithCDAndWeight, missing) = FJ.leftJoinWithMissing @PUMAJoinKey byPUMAs cdFromPUMA
    when (not $ null missing) $ K.knitError $ "Missing PUMAs in cachedPUMAsToCD left join: " <> show missing
    aggregated <-
      K.streamlyToKnit
      $ BRF.frameCompactMR
      FMR.noUnpack
      (FMR.assignKeysAndData @([BRDF.Year, GT.StateAbbreviation, GT.StateFIPS, GT.CongressionalDistrict] V.++ ks)  @(BRDF.FracPUMAInCD ': DatFieldsTo))
      pumaToCDFld
      byPUMAWithCDAndWeight
    K.logLE K.Info $ "aggregated (by PUMA) has " <> show (FL.fold FL.length aggregated)
    pure aggregated

pumaToCDFld :: FL.Fold (F.Record ((BRDF.FracPUMAInCD ': DatFieldsTo))) (F.Record DatFieldsTo)
pumaToCDFld =
  let cdWgt = F.rgetField @BRDF.FracPUMAInCD
      ppl = realToFrac . view DT.popCount
      wgtdPpl r = cdWgt r * ppl r
      density = view DT.pWPopPerSqMile
      countFld = fmap round $ FL.premap wgtdPpl FL.sum
      densityFld = FL.premap (\r -> (wgtdPpl r, density r)) PUMS.wgtdDensityF
  in (\pc d -> pc F.&: d F.&: V.RNil) <$> countFld <*> densityFld

cachedACSa5ByCD :: (K.KnitEffects r, BRK.CacheEffects r)
                => K.Sem r (K.ActionWithCacheTime r (F.FrameRec PUMS.PUMS_Typed))
                -> Int
                -> Maybe Int
                -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec ACSa5ByCDR))
cachedACSa5ByCD source year cdYearM = K.wrapPrefix "Model.Demographic.cachedACSByCD" $ do
  acsByPUMA_C <- cachedACSa5ByPUMA source year
  let adjCDYear = case cdYearM of
        Nothing -> id
        Just y -> over BRDF.year (const y)
      cacheKey = case cdYearM of
        Nothing -> "model/demographic/data/acs" <> show year <> "ByCD_a5.bin"
        Just y -> "model/demographic/data/acs" <> show year <> "ByCD" <> show y <> "_a5.bin"
  cachedPUMAsToCDs @CategoricalsA5 cacheKey $ fmap (fmap adjCDYear) acsByPUMA_C

cachedACSa6ByCD :: (K.KnitEffects r, BRK.CacheEffects r)
                => K.Sem r (K.ActionWithCacheTime r (F.FrameRec PUMS.PUMS_Typed))
                -> Int
                -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec ACSa6ByCDR))
cachedACSa6ByCD source year = K.wrapPrefix "Model.Demographic.cachedACSByCD" $ do
  acsByPUMA_C <- cachedACSa6ByPUMA source year
  cachedPUMAsToCDs @CategoricalsA6 ("model/demographic/data/acs" <> show year <> "ByCD_a6.bin") acsByPUMA_C

districtKey :: ([GT.StateAbbreviation, GT.CongressionalDistrict]  F.⊆ rs)
            => F.Record rs -> F.Record [GT.StateAbbreviation, GT.CongressionalDistrict]
districtKey = F.rcast

districtKeyT :: (F.ElemOf rs GT.StateAbbreviation, F.ElemOf rs GT.CongressionalDistrict)
             => F.Record rs -> Text
districtKeyT r = view GT.stateAbbreviation r <> "-" <> show (view GT.congressionalDistrict r)

simplifyAgeM :: F.ElemOf rs DT.Age6C => F.Record rs -> Maybe (F.Record (DT.Age5C ': rs))
simplifyAgeM r =
  let f g = Just $ FT.recordSingleton @DT.Age5C g F.<+> r
  in case F.rgetField @DT.Age6C r of
    DT.A6_Under18 -> Nothing
    DT.A6_18To24 -> f DT.A5_18To24
    DT.A6_25To34 -> f DT.A5_25To34
    DT.A6_35To44 -> f DT.A5_35To44
    DT.A6_45To64 -> f DT.A5_45To64
    DT.A6_65AndOver -> f DT.A5_65AndOver

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

type ACSByStateEduMNR = [BRDF.Year, GT.StateAbbreviation, BRDF.StateFIPS, DT.CitizenC, DT.Age6C, DT.SexC, DT.Race5C, DT.PWPopPerSqMile]
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
             =>  (F.Record ACSa6ByStateR -> F.Record ks) -> (F.Record ACSa6ByStateR -> l) -> F.FrameRec ACSa6ByStateR -> [ACSByStateMNT ks]
acsByStateMN cKey label = filterZeroes .
                          FL.fold (forMultinomial @([BRDF.Year, GT.StateAbbreviation, BRDF.StateFIPS] V.++ ks)
                                   (\r -> F.rcast @[BRDF.Year, GT.StateAbbreviation, BRDF.StateFIPS] r F.<+> cKey r)
                                   label
                                   (view DT.popCount)
                                   wgtdDensityF
                                  )

acsByStateCitizenMN :: F.FrameRec ACSa6ByStateR -> [ACSByStateCitizenMN]
acsByStateCitizenMN = acsByStateMN (F.rcast @[DT.SexC, DT.Education4C, DT.Race5C]) (view DT.citizenC)

acsByStateAgeMN :: F.FrameRec ACSa6ByStateR -> [ACSByStateAgeMN]
acsByStateAgeMN = acsByStateMN (F.rcast @[DT.CitizenC, DT.SexC, DT.Education4C, DT.Race5C]) (view DT.age6C)


acsByStateEduMN :: F.FrameRec ACSa6ByStateR -> [ACSByStateEduMN]
acsByStateEduMN = acsByStateMN (F.rcast @[DT.CitizenC, DT.Age6C, DT.SexC, DT.Race5C])
                  ((== DT.Grad) . DT.education4ToCollegeGrad . view DT.education4C)


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

--districtKey :: (F.ElemOf rs GT.StateAbbreviation, F.ElemOf rs GT.CongressionalDistrict) => F.Record rs -> Text
--districtKey r = F.rgetField @GT.StateAbbreviation r <> "-" <> show (F.rgetField @GT.CongressionalDistrict r)

logDensityPredictor :: F.ElemOf rs DT.PWPopPerSqMile => F.Record rs -> VU.Vector Double
logDensityPredictor = safeLogV . F.rgetField @DT.PWPopPerSqMile

safeLog :: Double -> Double
safeLog x =  if x < 1e-12 then 0 else Numeric.log x -- won't matter because Pop will be 0 here

safeLogV :: Double -> K.Vector Double
safeLogV x =  VU.singleton $ safeLog x

{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UnicodeSyntax #-}
--{-# LANGUAGE NoStrictData #-}

module BlueRipple.Model.Demographic.EnrichCensus
  (
    module BlueRipple.Model.Demographic.EnrichCensus
  )
where

import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.GeographicTypes as GT
import qualified BlueRipple.Data.DataFrames as BRDF
import qualified BlueRipple.Model.Demographic.StanModels as SM
import qualified BlueRipple.Model.Demographic.DataPrep as DDP
import qualified BlueRipple.Model.Demographic.EnrichData as DED
import qualified BlueRipple.Model.Demographic.MarginalStructure as DMS

import qualified BlueRipple.Data.Keyed as Keyed

import qualified BlueRipple.Data.CensusLoaders as BRC
import qualified BlueRipple.Data.CensusTables as BRC

import qualified BlueRipple.Data.ACS_PUMS as PUMS
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.DataFrames as BRDF
import qualified BlueRipple.Utilities.KnitUtils as BRK

import qualified Knit.Report as K
import qualified Knit.Utilities.Streamly as KS

import qualified Stan.ModelRunner as SMR

import qualified Data.Map as M
import qualified Data.Map.Merge.Strict as MM
import qualified Data.Set as S
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V
import qualified Control.Foldl as FL
import qualified Frames as F
import qualified Frames.Melt as F
import qualified Frames.Transform as FT
import qualified Frames.Streamly.Transform as FST
import qualified Frames.Streamly.InCore as FSI
import qualified Frames.MapReduce as FMR
import qualified Control.MapReduce as MR

import Control.Lens (view, Lens')
import BlueRipple.Model.Demographic.TPModel3 (ModelConfig(distribution))

type CensusASERR = [BRDF.Year, GT.StateFIPS, GT.DistrictTypeC, GT.DistrictName
                   , DT.Age5FC, DT.SexC, DT.Education4C, DT.Race5C, DT.PopCount, DT.PWPopPerSqMile
                   ]


{-
1. Correct marginal structure within joint for the model.
2. Recode loaded block-group data to correct inputs for model (cached district-demographics -> cached recoded district-demographics)
2a. Recode values (block-group categories to ACS categories)
2b. Build product distribution
3. Model runner (when necessary), along with correct design-matrix and ACS row-fold(s).
4. Model applier to recoded census (cached model-results + cached recoded-district-demographics -> cached enriched district demographics)
-}
type SR = [DT.SexC, DT.Race5C]
type SER = [DT.SexC, DT.Education4C, DT.Race5C]
type ASR = [DT.Age5FC, DT.SexC, DT.Race5C]
type ASER = [DT.Age5FC, DT.SexC, DT.Education4C, DT.Race5C]
type LDKey ks = BRDF.Year ': BRC.LDLocationR V.++ ks
type KeysWD ks = ks V.++ [DT.PopCount, DT.PWPopPerSqMile]

msSER_ASR :: Monoid w
          => Lens' w Double
          -> (Map (F.Record '[DT.Education4C]) w -> Map (F.Record '[DT.Age5FC]) w -> Map (F.Record '[DT.Education4C], F.Record '[DT.Age5FC]) w)
          -> DMS.MarginalStructure w (F.Record ASER)
msSER_ASR wl innerProduct = DMS.reKeyMarginalStructure
                            (F.rcast @[DT.SexC, DT.Race5C, DT.Education4C, DT.Age5FC])
                            (F.rcast @ASER)
                            $ DMS.combineMarginalStructuresF @'[DT.SexC, DT.Race5C]
                            wl innerProduct (DMS.identityMarginalStructure wl) (DMS.identityMarginalStructure wl)

type LDRecoded ks = LDKey ks V.++ [DT.PopCount, DT.PWPopPerSqMile]

recodeASR :: F.FrameRec (BRC.CensusRow BRC.LDLocationR BRC.ExtensiveDataR [BRC.Age14C, DT.SexC, BRC.RaceEthnicityC])
          -> F.FrameRec (LDRecoded ASR)
recodeASR = fmap F.rcast . FL.fold reFld . FL.fold ageFld
  where
    ageFld = FMR.concatFold
             $ FMR.mapReduceFold
             FMR.noUnpack
             (FMR.assignKeysAndData @(LDKey [DT.SexC, BRC.RaceEthnicityC]))
             (FMR.makeRecsWithKey id $ FMR.ReduceFold $ const BRC.age14ToAge5FFld)
    reFld = FMR.concatFold
             $ FMR.mapReduceFold
             FMR.noUnpack
             (FMR.assignKeysAndData @(LDKey [DT.SexC, DT.Age5FC]))
             (FMR.makeRecsWithKey id $ FMR.ReduceFold $ const BRC.reToR5Fld)


recodeSER ::  F.FrameRec (BRC.CensusRow BRC.LDLocationR BRC.ExtensiveDataR [DT.SexC, DT.Education4C, BRC.RaceEthnicityC])
          -> F.FrameRec (LDRecoded SER)
recodeSER = fmap F.rcast . FL.fold reFld
  where
    reFld = FMR.concatFold
             $ FMR.mapReduceFold
             FMR.noUnpack
             (FMR.assignKeysAndData @(LDKey [DT.SexC, DT.Education4C]))
             (FMR.makeRecsWithKey id $ FMR.ReduceFold $ const BRC.reToR5Fld)



ser_asr_tableProductWithDensity :: F.FrameRec (KeysWD SER) -> F.FrameRec (KeysWD ASR) -> F.FrameRec (KeysWD ASER)
ser_asr_tableProductWithDensity ser asr = F.toFrame $ fmap toRecord $ concat $ fmap pushOuterIn $ M.toList $ fmap M.toList
                                          $ DMS.tableProduct DMS.innerProductCWD serMap asrMap
  where
    pushOuterIn (o, xs) = fmap (o,) xs
    n = FL.fold (FL.premap (view DT.popCount) FL.sum) ser
    tcwd :: (F.ElemOf rs DT.PopCount, F.ElemOf rs DT.PWPopPerSqMile) => F.Record rs -> DMS.CellWithDensity
    tcwd r = DMS.CellWithDensity (realToFrac $ view DT.popCount r) (view DT.pWPopPerSqMile r)
    okF :: (SR F.⊆ rs) => F.Record rs -> F.Record SR
    okF = F.rcast @SR
    serTMFld :: FL.Fold (F.Record (KeysWD SER)) (Map (F.Record SR) (Map (F.Record '[DT.Education4C]) DMS.CellWithDensity))
    serTMFld = FL.premap (\r -> ((okF r, F.rcast @'[DT.Education4C] r), tcwd r)) $ DMS.normalizedTableMapFld DMS.cwdWgtLens
    asrTMFld :: FL.Fold (F.Record (KeysWD ASR)) (Map (F.Record SR) (Map (F.Record '[DT.Age5FC]) DMS.CellWithDensity))
    asrTMFld = FL.premap (\r -> ((okF r, F.rcast @'[DT.Age5FC] r), tcwd r)) $ DMS.normalizedTableMapFld DMS.cwdWgtLens
    serMap = FL.fold serTMFld ser
    asrMap = FL.fold asrTMFld asr
    cwdToRec :: DMS.CellWithDensity -> F.Record [DT.PopCount, DT.PWPopPerSqMile]
    cwdToRec cwd = round (realToFrac n * DMS.cwdWgt cwd) F.&: DMS.cwdDensity cwd F.&: V.RNil
    toRecord :: (F.Record SR
                , ((F.Record '[DT.Education4C], F.Record '[DT.Age5FC]), DMS.CellWithDensity)
                )
             -> F.Record (KeysWD ASER)
    toRecord (outer, ((e, a), cwd)) = F.rcast $ outer F.<+> e F.<+> a F.<+> cwdToRec cwd


type ProdOuter = [BRDF.Year, GT.StateFIPS, GT.DistrictTypeC, GT.DistrictName]

censusASR_SER_Products :: (K.KnitEffects r, BRK.CacheEffects r)
                       => Text
                       -> K.ActionWithCacheTime r BRC.LoadedCensusTablesByLD
                       -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec CensusASERR))
censusASR_SER_Products cacheKey censusTables_C = BRK.retrieveOrMakeFrame cacheKey censusTables_C f
  where
    f :: K.KnitEffects r => BRC.LoadedCensusTablesByLD -> K.Sem r (F.FrameRec CensusASERR)
    f (BRC.CensusTables asr _ ser _) = do
      let --toMapFld :: (row -> k) -> (row -> F.Record ds) -> FL.Fold row (Map k (F.FrameRec ds))
          toMapFld kF dF = fmap M.fromList
                           (MR.mapReduceFold
                              MR.noUnpack
                              (MR.assign kF dF)
                              (MR.foldAndLabel (fmap F.toFrame FL.list) (,))
                           )
          keyF :: (ProdOuter F.⊆ rs) => F.Record rs -> F.Record ProdOuter
          keyF = F.rcast
          asrF :: ((KeysWD ASR) F.⊆ rs) => F.Record rs -> F.Record (KeysWD ASR)
          asrF = F.rcast
          serF :: ((KeysWD SER) F.⊆ rs) => F.Record rs -> F.Record (KeysWD SER)
          serF = F.rcast
          asrMap = FL.fold (toMapFld keyF asrF) $ recodeASR asr
          serMap = FL.fold (toMapFld keyF serF) $ recodeSER ser
          whenMatchedF k asr' ser' = pure $ ser_asr_tableProductWithDensity ser' asr'
          whenMissingASRF k _ = K.knitError $ "Missing ASR table for k=" <> show k
          whenMissingSERF k _ = K.knitError $ "Missing SER table for k=" <> show k
      productMap <- MM.mergeA
                    (MM.traverseMissing whenMissingSERF)
                    (MM.traverseMissing whenMissingASRF)
                    (MM.zipWithAMatched whenMatchedF)
                    asrMap
                    serMap
      pure $ mconcat $ fmap (\(k, fr) -> fmap (k F.<+>) fr) $ M.toList productMap

{-
type CensusCASERR = BRC.CensusRow BRC.LDLocationR BRC.ExtensiveDataR [DT.CitizenC, DT.Age4C, DT.SexC, DT.Education4C, BRC.RaceEthnicityC]
type CensusASERRecodedR = BRC.LDLocationR
                          V.++ BRC.ExtensiveDataR
                          V.++ [DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.RaceAlone4C, DT.HispC, DT.PopCount, DT.PopPerSqMile]
-}

{-
-- Step 1
-- NB all this data is without a year field. So this step needs to be done for each year
enrichCensusData :: SM.AgeStateModelResult -> BRC.LoadedCensusTablesByLD -> K.Sem r (F.FrameRec CensusCASERR)
enrichCensusData amr censusTables = do
  enrichedViaModel <- KS.streamlyToKnit
                      $ DED.enrichFrameFromBinaryModel @DT.SimpleAge @BRC.Count
                      amr
                      (F.rgetField @BRDF.StateAbbreviation)
                      DT.EqualOrOver
                      DT.Under
                      (BRC.sexEducationRace censusTables)
  let rowSumFld = DED.desiredRowSumsFld @DT.Age4C @BRC.Count @[BRDF.StateAbbreviation, DT.SexC, DT.RaceAlone4C, DT.HispC] allSimpleAges DT.age4ToSimple
  let ncFld = DED.nearestCountsFrameFld @BRC.Count @DT.SimpleAgeC @DT.Education4C (DED.nearestCountsFrameIFld DED.nearestCountsKL) desiredRowSumLookup allEdus
-}

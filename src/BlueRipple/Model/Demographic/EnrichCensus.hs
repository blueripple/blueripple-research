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
import qualified BlueRipple.Model.Demographic.TableProducts as DTP
import qualified BlueRipple.Model.Demographic.TPModel3 as DTM3

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

import Control.Lens (view, (^.))
import qualified Control.Foldl as FL

import qualified Data.Map as M
import qualified Data.Map.Merge.Strict as MM
import qualified Data.Set as S
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Storable as VS
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V
import qualified Frames as F
import qualified Frames.Melt as F
import qualified Frames.Transform as FT
import qualified Frames.Streamly.Transform as FST
import qualified Frames.Streamly.InCore as FSI
import qualified Frames.MapReduce as FMR
import qualified Control.MapReduce as MR
import qualified Numeric
import qualified Numeric.LinearAlgebra as LA


import Control.Lens (view, Lens')
import BlueRipple.Model.Demographic.TPModel3 (ModelConfig(distribution))
import qualified BlueRipple.Data.Loaders as BRDF
import qualified BlueRipple.Configuration as BR


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
type LDOuterKeyR = BRDF.Year ': BRC.LDLocationR
type LDKey ks = LDOuterKeyR V.++ ks
type KeysWD ks = ks V.++ [DT.PopCount, DT.PWPopPerSqMile]

--type ASERDataR =   [DT.PopCount, DT.PWPopPerSqMile]
type CensusOuterKeyR = [BRDF.Year, GT.StateFIPS, GT.StateAbbreviation, GT.DistrictTypeC, GT.DistrictName]
type CensusASERR = CensusOuterKeyR V.++ KeysWD ASER


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

recodeASR :: F.FrameRec (BRC.CensusRow BRC.LDLocationR BRC.CensusDataR [BRC.Age14C, DT.SexC, BRC.RaceEthnicityC])
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


recodeSER ::  F.FrameRec (BRC.CensusRow BRC.LDLocationR BRC.CensusDataR [DT.SexC, DT.Education4C, BRC.RaceEthnicityC])
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
                                          $ DMS.tableProduct DMS.innerProductCWD' serMap asrMap
  where
    pushOuterIn (o, xs) = fmap (o,) xs
    tcwd :: (F.ElemOf rs DT.PopCount, F.ElemOf rs DT.PWPopPerSqMile) => F.Record rs -> DMS.CellWithDensity
    tcwd r = DMS.CellWithDensity (realToFrac $ view DT.popCount r) (view DT.pWPopPerSqMile r)
    okF :: (SR F.⊆ rs) => F.Record rs -> F.Record SR
    okF = F.rcast
    serTMFld :: FL.Fold (F.Record (KeysWD SER)) (Map (F.Record SR) (Map (F.Record '[DT.Education4C]) DMS.CellWithDensity))
    serTMFld = FL.premap (\r -> ((okF r, F.rcast @'[DT.Education4C] r), tcwd r)) DMS.tableMapFld
    asrTMFld :: FL.Fold (F.Record (KeysWD ASR)) (Map (F.Record SR) (Map (F.Record '[DT.Age5FC]) DMS.CellWithDensity))
    asrTMFld = FL.premap (\r -> ((okF r, F.rcast @'[DT.Age5FC] r), tcwd r)) DMS.tableMapFld
    serMap = FL.fold serTMFld ser
    asrMap = FL.fold asrTMFld asr
    cwdToRec :: DMS.CellWithDensity -> F.Record [DT.PopCount, DT.PWPopPerSqMile]
    cwdToRec cwd = round (DMS.cwdWgt cwd) F.&: DMS.cwdDensity cwd F.&: V.RNil
    toRecord :: (F.Record SR
                , ((F.Record '[DT.Education4C], F.Record '[DT.Age5FC]), DMS.CellWithDensity)
                )
             -> F.Record (KeysWD ASER)
    toRecord (outer, ((e, a), cwd)) = F.rcast $ outer F.<+> e F.<+> a F.<+> cwdToRec cwd


--type ProdOuter = [BRDF.Year, GT.StateFIPS, GT.DistrictTypeC, GT.DistrictName]

censusASR_SER_Products :: forall r . (K.KnitEffects r, BRK.CacheEffects r)
                       => Text
                       -> K.ActionWithCacheTime r BRC.LoadedCensusTablesByLD
                       -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec CensusASERR))
censusASR_SER_Products cacheKey censusTables_C = do
  let
    f :: BRC.LoadedCensusTablesByLD -> K.Sem r (F.FrameRec CensusASERR)
    f (BRC.CensusTables asr _ ser _) = do
      K.logLE K.Info "Building/re-building censusASR_SER products"
      stateAbbrFromFIPSMap <- BRDF.stateAbbrFromFIPSMapLoader
      let toMapFld :: (FSI.RecVec ds, Ord k) => (row -> k) -> (row -> F.Record ds) -> FL.Fold row (Map k (F.FrameRec ds))
          toMapFld kF dF = fmap M.fromList
                           (MR.mapReduceFold
                              MR.noUnpack
                              (MR.assign kF dF)
                              (MR.foldAndLabel (fmap F.toFrame FL.list) (,))
                           )
          keyF :: (LDOuterKeyR F.⊆ rs) => F.Record rs -> F.Record LDOuterKeyR
          keyF = F.rcast
          asrF :: ((KeysWD ASR) F.⊆ rs) => F.Record rs -> F.Record (KeysWD ASR)
          asrF = F.rcast
          serF :: ((KeysWD SER) F.⊆ rs) => F.Record rs -> F.Record (KeysWD SER)
          serF = F.rcast
          recodedASR = recodeASR asr
          recodedSER = recodeSER ser
--      BRK.logFrame ser
--      BRK.logFrame recodedSER
      let asrMap = FL.fold (toMapFld keyF asrF) $ recodedASR
          serMap = FL.fold (toMapFld keyF serF) $ recodedSER
          checkFrames k ta tb = do
            let na = FL.fold (FL.premap (view DT.popCount) FL.sum) ta
                nb = FL.fold (FL.premap (view DT.popCount) FL.sum) tb
            when (na /= nb) $ K.logLE K.Error $ "Mismatched ASR/SER tables at k=" <> show k
            pure ()
          whenMatchedF k asr' ser' = checkFrames k asr' ser' >> pure (ser_asr_tableProductWithDensity ser' asr')
          whenMissingASRF k _ = K.knitError $ "Missing ASR table for k=" <> show k
          whenMissingSERF k _ = K.knitError $ "Missing SER table for k=" <> show k
      productMap <- MM.mergeA
                    (MM.traverseMissing whenMissingSERF)
                    (MM.traverseMissing whenMissingASRF)
                    (MM.zipWithAMatched whenMatchedF)
                    asrMap
                    serMap
      let assocToFrame (k, fr) = do
            let fips = k ^. GT.stateFIPS
            sa <- K.knitMaybe ("Missing FIPS=" <> show fips) $ M.lookup fips stateAbbrFromFIPSMap
            let newK :: F.Record CensusOuterKeyR
                newK = F.rcast $ (FT.recordSingleton @GT.StateAbbreviation sa) F.<+> k
            pure $ fmap (newK F.<+>) fr
      mconcat <$> fmap assocToFrame $ M.toList productMap
  BRK.retrieveOrMakeFrame cacheKey censusTables_C f

logitMarginalsCMatrix :: LA.Matrix Double
logitMarginalsCMatrix =
  let serInASER :: F.Record ASER -> F.Record SER
      serInASER = F.rcast
      asrInASER :: F.Record ASER -> F.Record ASR
      asrInASER = F.rcast
      serInASER_stencils = fmap (DMS.expandStencil serInASER)
                           $ DMS.msStencils
                           $ DMS.identityMarginalStructure @(F.Record SER) DMS.cwdWgtLens
      asrInASER_stencils = fmap (DMS.expandStencil asrInASER)
                           $ DMS.msStencils
                           $ DMS.identityMarginalStructure @(F.Record SER) DMS.cwdWgtLens
  in DED.mMatrix (S.size $ Keyed.elements @(F.Record ASER)) (serInASER_stencils <> asrInASER_stencils)

logitMarginals :: LA.Matrix Double -> VS.Vector Double -> VS.Vector Double
logitMarginals cMat prodDistV = VS.map (DTM3.bLogit 1e-10) (cMat LA.#> prodDistV)

popAndpwDensityFld :: FL.Fold DMS.CellWithDensity (Double, Double)
popAndpwDensityFld = DT.densityAndPopFld' (const 1) DMS.cwdWgt DMS.cwdDensity

predictedCensusASER :: forall r . (K.KnitEffects r, BRK.CacheEffects r)
                    => Bool
                    -> Text
                    -> K.ActionWithCacheTime r (DTM3.Predictor Text)
                    -> K.ActionWithCacheTime r BRC.LoadedCensusTablesByLD
                    -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec CensusASERR), K.ActionWithCacheTime r (F.FrameRec CensusASERR))
predictedCensusASER rebuild cacheRoot predictor_C censusTables_C = do
  let productCacheKey = cacheRoot <> "_products.bin"
      predictedCacheKey = cacheRoot <> "_modeled.bin"
      logitMarginals' = logitMarginals logitMarginalsCMatrix
  when rebuild $ BRK.clearIfPresentD productCacheKey >> BRK.clearIfPresentD predictedCacheKey
  products_C <- censusASR_SER_Products productCacheKey censusTables_C
  let predictFld :: DTM3.Predictor Text -> Text -> FL.FoldM (Either Text) (F.Record (KeysWD ASER)) (F.FrameRec (KeysWD ASER))
      predictFld predictor sa =
        let key = F.rcast @ASER
            w r = DMS.CellWithDensity (realToFrac $ r ^. DT.popCount) (r ^. DT.pWPopPerSqMile)
            g r = (key r, w r)
            prodMapFld = FL.premap g DMS.zeroFillSummedMapFld
            posLog x = if x < 1 then 0 else Numeric.log x
            popAndPWDensity :: Map (F.Record ASER) DMS.CellWithDensity -> (Double, Double)
            popAndPWDensity = FL.fold popAndpwDensityFld
            covariates pwD prodDistV = mconcat [VS.singleton (posLog pwD), logitMarginals' prodDistV]
            prodV pm = VS.fromList $ fmap DMS.cwdWgt $ M.elems pm
            toRec :: (F.Record ASER, DMS.CellWithDensity) -> F.Record (KeysWD ASER)
            toRec (k, cwd) = k F.<+> (round (DMS.cwdWgt cwd) F.&: DMS.cwdDensity cwd F.&: V.RNil)
            predict pm = let (n, pwD) = popAndPWDensity pm
                             pV = prodV pm
                             pDV = VS.map (/ n) pV
                         in F.toFrame . fmap toRec <$> DTM3.predictedJoint DMS.cwdWgtLens predictor sa (covariates pwD pDV) (M.toList pm)
        in FMR.postMapM predict $ FL.generalize prodMapFld
  let f :: DTM3.Predictor Text -> F.FrameRec CensusASERR -> K.Sem r (F.FrameRec CensusASERR)
      f predictor products = do
        let rFld :: F.Record CensusOuterKeyR -> FL.FoldM (K.Sem r) (F.Record (KeysWD ASER)) (F.FrameRec CensusASERR)
            rFld k = FL.hoists K.knitEither $ fmap (fmap (k F.<+>)) $ predictFld predictor (k ^. GT.stateAbbreviation)
            fldM = FMR.concatFoldM
                   $ FMR.mapReduceFoldM
                   (FMR.generalizeUnpack FMR.noUnpack)
                   (FMR.generalizeAssign $ FMR.assignKeysAndData @CensusOuterKeyR @(KeysWD ASER))
                   (MR.ReduceFoldM rFld)
        K.logLE K.Info "Building/re-building censusASER predictions"
        FL.foldM fldM products
      predictionDeps = (,) <$> predictor_C <*> products_C
  predicted_C <- BRK.retrieveOrMakeFrame predictedCacheKey predictionDeps $ uncurry f
  pure (predicted_C, products_C)


predictorModel3 :: forall r . (K.KnitEffects r, BRK.CacheEffects r)
                => Bool
                -> BR.CommandLine
                -> K.Sem r (K.ActionWithCacheTime r (DTM3.Predictor Text))
predictorModel3 clearCache cmdLine = do
  acsA5ByPUMA_C <- DDP.cachedACSa5ByPUMA
  let nvpsCacheKey = "model/demographic/ser_asr_aserNVPs.bin"
      predictorCacheKey = "model/demographic/ser_asr_aserPredictor.bin"
  when clearCache $ traverse_ BRK.clearIfPresentD [nvpsCacheKey, predictorCacheKey]
  let msSER_A5SR_cwd = DMS.reKeyMarginalStructure
                         (F.rcast @[DT.SexC, DT.Race5C, DT.Education4C, DT.Age5FC])
                         (F.rcast @ASER)
                         $ DMS.combineMarginalStructuresF  @'[DT.SexC, DT.Race5C] @'[DT.Education4C] @'[DT.Age5FC]
                         DMS.cwdWgtLens DMS.innerProductCWD'
                         (DMS.identityMarginalStructure DMS.cwdWgtLens)
                          (DMS.identityMarginalStructure DMS.cwdWgtLens)
  let projCovariancesFld =
        DTP.diffCovarianceFldMS
        DMS.cwdWgtLens
        (F.rcast @[GT.StateAbbreviation, GT.PUMA])
        (F.rcast @ASER)
        DTM3.cwdF
        msSER_A5SR_cwd
  nullVectorProjections_C <- BRK.retrieveOrMakeD nvpsCacheKey acsA5ByPUMA_C
                             $ \acsA5ByPUMA -> do
    K.logLE K.Info $ "Computing covariance matrix of projected differences."
    let (projMeans, projCovariances) = FL.fold projCovariancesFld acsA5ByPUMA
        (eigVals, _) = LA.eigSH projCovariances
    K.logLE K.Diagnostic
      $ "mean=" <> toText (DED.prettyVector projMeans)
      <> "\ncov=" <> toText (LA.disps 3 $ LA.unSym $ projCovariances)
      <> "\ncovariance eigenValues: " <> DED.prettyVector eigVals
    pure $ DTP.uncorrelatedNullVecsMS msSER_A5SR_cwd projCovariances
  let tp3NumKeys = S.size (Keyed.elements @(F.Record [DT.SexC, DT.Education4C, DT.Race5C]))
                   +  S.size (Keyed.elements @(F.Record [DT.Age5FC, DT.SexC, DT.Race5C]))
      tp3InnerFld ::  (F.ElemOf rs DT.PopCount, F.ElemOf rs DT.PWPopPerSqMile
                      , F.ElemOf rs DT.Age5FC, F.ElemOf rs DT.SexC, F.ElemOf rs DT.Education4C, F.ElemOf rs DT.Race5C)
                  => FL.Fold (F.Record rs) (VS.Vector Double)
      tp3InnerFld = DTM3.mergeInnerFlds [VS.singleton . DTM3.cwdListToLogPWDensity <$> DTM3.cwdInnerFld (F.rcast  @[DT.SexC, DT.Education4C, DT.Race5C]) DTM3.cwdF
                                        , DTM3.cwdListToLogitVec <$> DTM3.cwdInnerFld (F.rcast  @[DT.SexC, DT.Education4C, DT.Race5C]) DTM3.cwdF
                                        , DTM3.cwdListToLogitVec <$> DTM3.cwdInnerFld (F.rcast  @[DT.Age5FC, DT.SexC, DT.Race5C]) DTM3.cwdF
                                        ]
      tp3RunConfig n = DTM3.RunConfig n False False Nothing
      tp3ModelConfig = DTM3.ModelConfig True (DTM3.dmr "SER_ASR" (tp3NumKeys + 1)) -- +1 for pop density
                         DTM3.AlphaHierNonCentered DTM3.NormalDist
      modelOne n = DTM3.runProjModel False cmdLine (tp3RunConfig n) tp3ModelConfig acsA5ByPUMA_C nullVectorProjections_C msSER_A5SR_cwd tp3InnerFld

  K.logLE K.Info "Running marginals as covariates model, if necessary."
  let modelResultDeps = (,) <$> acsA5ByPUMA_C <*> nullVectorProjections_C
  model3Res_C <- BRK.retrieveOrMakeD predictorCacheKey modelResultDeps
                 $ \(_, nvps) -> (do
                                     cachedModelResults <- sequenceA <$> traverse modelOne [0..(DTP.numProjections nvps - 1)]
                                     modelResults <- K.ignoreCacheTime cachedModelResults
                                     pure $ DTM3.Predictor nvps modelResults
                                 )
  pure model3Res_C

{-
type CensusCASERR = BRC.CensusRow BRC.LDLocationR BRC.CensusDataR [DT.CitizenC, DT.Age4C, DT.SexC, DT.Education4C, BRC.RaceEthnicityC]
type CensusASERRecodedR = BRC.LDLocationR
                          V.++ BRC.CensusDataR
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

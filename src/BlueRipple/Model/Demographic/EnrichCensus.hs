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
type SRo = [DT.SexC, BRC.RaceEthnicityC]
type SER = [DT.SexC, DT.Education4C, DT.Race5C]
type CSR = [DT.CitizenC, DT.SexC, DT.Race5C]
type ASR = [DT.Age4C, DT.SexC, DT.Race5C]
type CASR = [DT.CitizenC, DT.Age4C, DT.SexC, DT.Race5C]
type ASE = [DT.Age4C, DT.SexC, DT.Education4C]
type ASER = [DT.Age4C, DT.SexC, DT.Education4C, DT.Race5C]
type CASER = [DT.CitizenC, DT.Age4C, DT.SexC, DT.Education4C, DT.Race5C]
type CA5SR = [DT.CitizenC, DT.Age5FC, DT.SexC, DT.Race5C]
type A5SR = [DT.Age5FC, DT.SexC, DT.Race5C]
type A5SER = [DT.Age5FC, DT.SexC, DT.Education4C, DT.Race5C]
type LDOuterKeyR = BRDF.Year ': BRC.LDLocationR
type LDKey ks = LDOuterKeyR V.++ ks
type KeysWD ks = ks V.++ [DT.PopCount, DT.PWPopPerSqMile]

--type ASERDataR =   [DT.PopCount, DT.PWPopPerSqMile]
type CensusOuterKeyR = [BRDF.Year, GT.StateFIPS, GT.StateAbbreviation, GT.DistrictTypeC, GT.DistrictName]
type CensusASERR = CensusOuterKeyR V.++ KeysWD ASER
type CensusA5SERR = CensusOuterKeyR V.++ KeysWD A5SER


msSER_A5SR :: Monoid w
           => Lens' w Double
           -> (Map (F.Record '[DT.Education4C]) w -> Map (F.Record '[DT.Age5FC]) w -> Map (F.Record '[DT.Education4C], F.Record '[DT.Age5FC]) w)
           -> DMS.MarginalStructure w (F.Record A5SER)
msSER_A5SR wl innerProduct = DMS.reKeyMarginalStructure
                            (F.rcast @[DT.SexC, DT.Race5C, DT.Education4C, DT.Age5FC])
                            (F.rcast @A5SER)
                            $ DMS.combineMarginalStructuresF @'[DT.SexC, DT.Race5C]
                            wl innerProduct (DMS.identityMarginalStructure wl) (DMS.identityMarginalStructure wl)


msCSR_A5SR :: Monoid w
           => Lens' w Double
           -> (Map (F.Record '[DT.CitizenC]) w -> Map (F.Record '[DT.Age5FC]) w -> Map (F.Record '[DT.CitizenC], F.Record '[DT.Age5FC]) w)
           -> DMS.MarginalStructure w (F.Record CA5SR)
msCSR_A5SR wl innerProduct = DMS.reKeyMarginalStructure
                            (F.rcast @[DT.SexC, DT.Race5C, DT.CitizenC, DT.Age5FC])
                            (F.rcast @CA5SR)
                            $ DMS.combineMarginalStructuresF  @'[DT.SexC, DT.Race5C]
                            wl innerProduct (DMS.identityMarginalStructure wl) (DMS.identityMarginalStructure wl)


msASE_CASR :: Monoid w
           => Lens' w Double
           -> (Map (F.Record '[DT.Education4C]) w -> Map (F.Record '[DT.CitizenC, DT.Race5C]) w -> Map (F.Record '[DT.Education4C], F.Record '[DT.CitizenC, DT.Race5C]) w)
           -> DMS.MarginalStructure w (F.Record CASER)
msASE_CASR wl innerProduct = DMS.reKeyMarginalStructure
                            (F.rcast @[DT.Age4C, DT.SexC, DT.Education4C, DT.CitizenC, DT.Race5C])
                            (F.rcast @CASER)
                            $ DMS.combineMarginalStructuresF  @'[DT.Age4C, DT.SexC]
                            wl innerProduct (DMS.identityMarginalStructure wl) (DMS.identityMarginalStructure wl)




type LDRecoded ks = LDKey ks V.++ [DT.PopCount, DT.PWPopPerSqMile]

recodeA5SR :: F.FrameRec (BRC.CensusRow BRC.LDLocationR BRC.CensusDataR [BRC.Age14C, DT.SexC, BRC.RaceEthnicityC])
          -> F.FrameRec (LDRecoded A5SR)
recodeA5SR = fmap F.rcast . FL.fold reFld . FL.fold ageFld
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

ser_asr_tableProductWithDensity :: F.FrameRec (KeysWD SER) -> F.FrameRec (KeysWD A5SR) -> F.FrameRec (KeysWD A5SER)
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
    asrTMFld :: FL.Fold (F.Record (KeysWD A5SR)) (Map (F.Record SR) (Map (F.Record '[DT.Age5FC]) DMS.CellWithDensity))
    asrTMFld = FL.premap (\r -> ((okF r, F.rcast @'[DT.Age5FC] r), tcwd r)) DMS.tableMapFld
    serMap = FL.fold serTMFld ser
    asrMap = FL.fold asrTMFld asr
    cwdToRec :: DMS.CellWithDensity -> F.Record [DT.PopCount, DT.PWPopPerSqMile]
    cwdToRec cwd = round (DMS.cwdWgt cwd) F.&: DMS.cwdDensity cwd F.&: V.RNil
    toRecord :: (F.Record SR
                , ((F.Record '[DT.Education4C], F.Record '[DT.Age5FC]), DMS.CellWithDensity)
                )
             -> F.Record (KeysWD A5SER)
    toRecord (outer, ((e, a), cwd)) = F.rcast $ outer F.<+> e F.<+> a F.<+> cwdToRec cwd


--type ProdOuter = [BRDF.Year, GT.StateFIPS, GT.DistrictTypeC, GT.DistrictName]

censusASR_SER_Products :: forall r . (K.KnitEffects r, BRK.CacheEffects r)
                       => Text
                       -> K.ActionWithCacheTime r BRC.LoadedCensusTablesByLD
                       -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec CensusA5SERR))
censusASR_SER_Products cacheKey censusTables_C = do
  let
    f :: BRC.LoadedCensusTablesByLD -> K.Sem r (F.FrameRec CensusA5SERR)
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
          asrF :: ((KeysWD A5SR) F.⊆ rs) => F.Record rs -> F.Record (KeysWD A5SR)
          asrF = F.rcast
          serF :: ((KeysWD SER) F.⊆ rs) => F.Record rs -> F.Record (KeysWD SER)
          serF = F.rcast
      -- check tables before recoding
          compareF (s1, s2) =
            let n1 = getSum s1
                n2 = getSum s2
            in if n1 /= n2 then Just $ "N(A5SR)=" <> show n1 <> "; N(SER)=" <> show n2 else Nothing
      innerCompareMap <- K.knitEither
                         $ compareMarginals
                         (\asrR -> (F.rcast @(LDOuterKeyR V.++ SRo) asrR, Sum $ view DT.popCount asrR))
                         (\serR -> (F.rcast @(LDOuterKeyR V.++ SRo) serR, Sum $ view DT.popCount serR))
                         asr ser
      let outerCompareMap = compareOuter (F.rcast @LDOuterKeyR) innerCompareMap
      compareMapToLog show compareF outerCompareMap
      compareMapToLog show compareF innerCompareMap
      let recodedA5SR = recodeA5SR asr
          recodedSER = recodeSER ser
--      BRK.logFrame ser
--      BRK.logFrame recodedSER
      let asrMap = FL.fold (toMapFld keyF asrF) $ recodedA5SR
          serMap = FL.fold (toMapFld keyF serF) $ recodedSER
          checkFrames :: (Show k, F.ElemOf as DT.PopCount, F.ElemOf bs DT.PopCount)
                      => k -> F.FrameRec as -> F.FrameRec bs -> K.Sem r ()
          checkFrames k ta tb = do
            let na = FL.fold (FL.premap (view DT.popCount) FL.sum) ta
                nb = FL.fold (FL.premap (view DT.popCount) FL.sum) tb
            when (na /= nb) $ K.logLE K.Error $ "Mismatched ASR/SER tables at k=" <> show k <> ". N(ASR)=" <> show na <> "; N(SER)=" <> show nb
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
  let serInASER :: F.Record A5SER -> F.Record SER
      serInASER = F.rcast
      asrInASER :: F.Record A5SER -> F.Record A5SR
      asrInASER = F.rcast
      serInASER_stencils = fmap (DMS.expandStencil serInASER)
                           $ DMS.msStencils
                           $ DMS.identityMarginalStructure @(F.Record SER) DMS.cwdWgtLens
      asrInASER_stencils = fmap (DMS.expandStencil asrInASER)
                           $ DMS.msStencils
                           $ DMS.identityMarginalStructure @(F.Record SER) DMS.cwdWgtLens
  in DED.mMatrix (S.size $ Keyed.elements @(F.Record A5SER)) (serInASER_stencils <> asrInASER_stencils)

logitMarginals :: LA.Matrix Double -> VS.Vector Double -> VS.Vector Double
logitMarginals cMat prodDistV = VS.map (DTM3.bLogit 1e-10) (cMat LA.#> prodDistV)

popAndpwDensityFld :: FL.Fold DMS.CellWithDensity (Double, Double)
popAndpwDensityFld = DT.densityAndPopFld' (const 1) DMS.cwdWgt DMS.cwdDensity

predictedCensusASER :: forall r . (K.KnitEffects r, BRK.CacheEffects r)
                    => Bool
                    -> Text
                    -> K.ActionWithCacheTime r (DTM3.Predictor Text)
                    -> K.ActionWithCacheTime r BRC.LoadedCensusTablesByLD
                    -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec CensusA5SERR), K.ActionWithCacheTime r (F.FrameRec CensusA5SERR))
predictedCensusASER rebuild cacheRoot predictor_C censusTables_C = do
  let productCacheKey = cacheRoot <> "_products.bin"
      predictedCacheKey = cacheRoot <> "_modeled.bin"
      logitMarginals' = logitMarginals logitMarginalsCMatrix
  when rebuild $ BRK.clearIfPresentD productCacheKey >> BRK.clearIfPresentD predictedCacheKey
  products_C <- censusASR_SER_Products productCacheKey censusTables_C
  let predictFld :: DTM3.Predictor Text -> Text -> FL.FoldM (Either Text) (F.Record (KeysWD A5SER)) (F.FrameRec (KeysWD A5SER))
      predictFld predictor sa =
        let key = F.rcast @A5SER
            w r = DMS.CellWithDensity (realToFrac $ r ^. DT.popCount) (r ^. DT.pWPopPerSqMile)
            g r = (key r, w r)
            prodMapFld = FL.premap g DMS.zeroFillSummedMapFld
            posLog x = if x < 1 then 0 else Numeric.log x
            popAndPWDensity :: Map (F.Record A5SER) DMS.CellWithDensity -> (Double, Double)
            popAndPWDensity = FL.fold popAndpwDensityFld
            covariates pwD prodDistV = mconcat [VS.singleton (posLog pwD), logitMarginals' prodDistV]
            prodV pm = VS.fromList $ fmap DMS.cwdWgt $ M.elems pm
            toRec :: (F.Record A5SER, DMS.CellWithDensity) -> F.Record (KeysWD A5SER)
            toRec (k, cwd) = k F.<+> (round (DMS.cwdWgt cwd) F.&: DMS.cwdDensity cwd F.&: V.RNil)
            predict pm = let (n, pwD) = popAndPWDensity pm
                             pV = prodV pm
                             pDV = VS.map (/ n) pV
                         in F.toFrame . fmap toRec <$> DTM3.predictedJoint DMS.cwdWgtLens predictor sa (covariates pwD pDV) (M.toList pm)
        in FMR.postMapM predict $ FL.generalize prodMapFld
  let f :: DTM3.Predictor Text -> F.FrameRec CensusA5SERR -> K.Sem r (F.FrameRec CensusA5SERR)
      f predictor products = do
        let rFld :: F.Record CensusOuterKeyR -> FL.FoldM (K.Sem r) (F.Record (KeysWD A5SER)) (F.FrameRec CensusA5SERR)
            rFld k = FL.hoists K.knitEither $ fmap (fmap (k F.<+>)) $ predictFld predictor (k ^. GT.stateAbbreviation)
            fldM = FMR.concatFoldM
                   $ FMR.mapReduceFoldM
                   (FMR.generalizeUnpack FMR.noUnpack)
                   (FMR.generalizeAssign $ FMR.assignKeysAndData @CensusOuterKeyR @(KeysWD A5SER))
                   (MR.ReduceFoldM rFld)
        K.logLE K.Info "Building/re-building censusASER predictions"
        FL.foldM fldM products
      predictionDeps = (,) <$> predictor_C <*> products_C
  predicted_C <- BRK.retrieveOrMakeFrame predictedCacheKey predictionDeps $ uncurry f
  pure (predicted_C, products_C)

projCovFld :: forall rs ks . (ks F.⊆ rs, F.ElemOf rs GT.PUMA, F.ElemOf rs GT.StateAbbreviation, F.ElemOf rs DT.PopCount, F.ElemOf rs DT.PWPopPerSqMile)
           => DMS.MarginalStructure DMS.CellWithDensity (F.Record ks) -> FL.Fold (F.Record rs) (LA.Vector Double, LA.Herm Double)
projCovFld ms = DTP.diffCovarianceFldMS
                DMS.cwdWgtLens
                (F.rcast @[GT.StateAbbreviation, GT.PUMA])
                (F.rcast @ks)
                DTM3.cwdF
                ms

cachedNVProjections :: forall rs ks r .
                       (K.KnitEffects r, BRK.CacheEffects r
                       , ks F.⊆ rs, F.ElemOf rs GT.PUMA, F.ElemOf rs GT.StateAbbreviation, F.ElemOf rs DT.PopCount, F.ElemOf rs DT.PWPopPerSqMile)
                    => Text
                    -> DMS.MarginalStructure DMS.CellWithDensity (F.Record ks)
                    -> K.ActionWithCacheTime r (F.FrameRec rs)
                    -> K.Sem r (K.ActionWithCacheTime r DTP.NullVectorProjections)
cachedNVProjections cacheKey ms cachedDataRows = do
  let fld = projCovFld ms

  BRK.retrieveOrMakeD cacheKey cachedDataRows
                             $ \dataRows -> do
    K.logLE K.Info $ "Computing covariance matrix of projected differences."
    let (projMeans, projCovariances) = FL.fold fld dataRows
        (eigVals, _) = LA.eigSH projCovariances
    K.logLE K.Diagnostic
      $ "mean=" <> toText (DED.prettyVector projMeans)
      <> "\ncov=" <> toText (LA.disps 3 $ LA.unSym projCovariances)
      <> "\ncovariance eigenValues: " <> DED.prettyVector eigVals
    pure $ DTP.uncorrelatedNullVecsMS ms projCovariances


runAllModels :: (K.KnitEffects r, BRK.CacheEffects r)
             => Text
             -> (Int -> K.Sem r (K.ActionWithCacheTime r (DTM3.ComponentPredictor Text)))
             -> K.ActionWithCacheTime r (F.FrameRec rs)
             -> K.ActionWithCacheTime r DTP.NullVectorProjections
             -> K.Sem r (K.ActionWithCacheTime r (DTM3.Predictor Text))
runAllModels cacheKey modelOne cachedDataRows cachedNVPs = do
  K.logLE K.Info "Running marginals as covariates model, if necessary."
  let modelResultDeps = (,) <$> cachedDataRows <*> cachedNVPs
  model3Res_C <- BRK.retrieveOrMakeD cacheKey modelResultDeps
                 $ \(_, nvps) -> (do
                                     cachedModelResults <- sequenceA <$> traverse modelOne [0..(DTP.numProjections nvps - 1)]
                                     modelResults <- K.ignoreCacheTime cachedModelResults
                                     pure $ DTM3.Predictor nvps modelResults
                                 )
  pure model3Res_C

predictorModel3_SER_A5SR :: forall r . (K.KnitEffects r, BRK.CacheEffects r)
                         => Bool
                         -> BR.CommandLine
                         -> K.Sem r (K.ActionWithCacheTime r (DTM3.Predictor Text))
predictorModel3_SER_A5SR clearCache cmdLine = do
  acsA5ByPUMA_C <- DDP.cachedACSa5ByPUMA
  let nvpsCacheKey = "model/demographic/ser_a5sr_a5serNVPs.bin"
      predictorCacheKey = "model/demographic/ser_a5sr_a5serPredictor.bin"
  when clearCache $ traverse_ BRK.clearIfPresentD [nvpsCacheKey, predictorCacheKey]
  let msSER_A5SR_cwd = msSER_A5SR DMS.cwdWgtLens DMS.innerProductCWD'
  nullVectorProjections_C <- cachedNVProjections nvpsCacheKey msSER_A5SR_cwd acsA5ByPUMA_C

  let tp3NumKeys = S.size (Keyed.elements @(F.Record SER)) + S.size (Keyed.elements @(F.Record A5SR))
      tp3InnerFld = DTM3.mergeInnerFlds [VS.singleton . DTM3.cwdListToLogPWDensity <$> DTM3.cwdInnerFld (F.rcast @SER) DTM3.cwdF
                                        , DTM3.cwdListToLogitVec <$> DTM3.cwdInnerFld (F.rcast @SER) DTM3.cwdF
                                        , DTM3.cwdListToLogitVec <$> DTM3.cwdInnerFld (F.rcast @A5SR) DTM3.cwdF
                                        ]
      tp3RunConfig n = DTM3.RunConfig n False False Nothing
      tp3ModelConfig = DTM3.ModelConfig True (DTM3.dmr "SER_A5SR" (tp3NumKeys + 1)) -- +1 for pop density
                         DTM3.AlphaHierNonCentered DTM3.NormalDist
      modelOne n = DTM3.runProjModel False cmdLine (tp3RunConfig n) tp3ModelConfig acsA5ByPUMA_C nullVectorProjections_C msSER_A5SR_cwd tp3InnerFld
  runAllModels predictorCacheKey modelOne acsA5ByPUMA_C nullVectorProjections_C

predictorModel3_CSR_A5SR :: forall r . (K.KnitEffects r, BRK.CacheEffects r)
                         => Bool
                         -> BR.CommandLine
                         -> K.Sem r (K.ActionWithCacheTime r (DTM3.Predictor Text))
predictorModel3_CSR_A5SR clearCache cmdLine = do
  acsA5ByPUMA_C <- DDP.cachedACSa5ByPUMA
  let nvpsCacheKey = "model/demographic/csr_a5sr_a5serNVPs.bin"
      predictorCacheKey = "model/demographic/csr_a5sr_a5serPredictor.bin"
  when clearCache $ traverse_ BRK.clearIfPresentD [nvpsCacheKey, predictorCacheKey]
  let msCSR_A5SR_cwd = msSER_A5SR DMS.cwdWgtLens DMS.innerProductCWD'
  nullVectorProjections_C <- cachedNVProjections nvpsCacheKey msCSR_A5SR_cwd acsA5ByPUMA_C

  let tp3NumKeys = S.size (Keyed.elements @(F.Record CSR)) + S.size (Keyed.elements @(F.Record A5SR))
      tp3InnerFld = DTM3.mergeInnerFlds [VS.singleton . DTM3.cwdListToLogPWDensity <$> DTM3.cwdInnerFld (F.rcast @CSR) DTM3.cwdF
                                        , DTM3.cwdListToLogitVec <$> DTM3.cwdInnerFld (F.rcast @CSR) DTM3.cwdF
                                        , DTM3.cwdListToLogitVec <$> DTM3.cwdInnerFld (F.rcast @A5SR) DTM3.cwdF
                                        ]
      tp3RunConfig n = DTM3.RunConfig n False False Nothing
      tp3ModelConfig = DTM3.ModelConfig True (DTM3.dmr "CSR_A5SR" (tp3NumKeys + 1)) -- +1 for pop density
                         DTM3.AlphaHierNonCentered DTM3.NormalDist
      modelOne n = DTM3.runProjModel False cmdLine (tp3RunConfig n) tp3ModelConfig acsA5ByPUMA_C nullVectorProjections_C msCSR_A5SR_cwd tp3InnerFld
  runAllModels predictorCacheKey modelOne acsA5ByPUMA_C nullVectorProjections_C

predictorModel3_ASE_CASR :: forall r . (K.KnitEffects r, BRK.CacheEffects r)
                         => Bool
                         -> BR.CommandLine
                         -> K.Sem r (K.ActionWithCacheTime r (DTM3.Predictor Text))
predictorModel3_ASE_CASR clearCache cmdLine = do
  acsA4ByPUMA_C <- DDP.cachedACSa4ByPUMA
  let nvpsCacheKey = "model/demographic/ase_casr_a5serNVPs.bin"
      predictorCacheKey = "model/demographic/ase_casr_a5serPredictor.bin"
  when clearCache $ traverse_ BRK.clearIfPresentD [nvpsCacheKey, predictorCacheKey]
  let msASE_CASR_cwd = msASE_CASR DMS.cwdWgtLens DMS.innerProductCWD'
  nullVectorProjections_C <- cachedNVProjections nvpsCacheKey msASE_CASR_cwd acsA4ByPUMA_C

  let tp3NumKeys = S.size (Keyed.elements @(F.Record ASE)) + S.size (Keyed.elements @(F.Record CASR))
      tp3InnerFld = DTM3.mergeInnerFlds [VS.singleton . DTM3.cwdListToLogPWDensity <$> DTM3.cwdInnerFld (F.rcast @ASE) DTM3.cwdF
                                        , DTM3.cwdListToLogitVec <$> DTM3.cwdInnerFld (F.rcast @ASE) DTM3.cwdF
                                        , DTM3.cwdListToLogitVec <$> DTM3.cwdInnerFld (F.rcast @CASR) DTM3.cwdF
                                        ]
      tp3RunConfig n = DTM3.RunConfig n False False Nothing
      tp3ModelConfig = DTM3.ModelConfig True (DTM3.dmr "ASE_CASR" (tp3NumKeys + 1)) -- +1 for pop density
                         DTM3.AlphaHierNonCentered DTM3.NormalDist
      modelOne n = DTM3.runProjModel False cmdLine (tp3RunConfig n) tp3ModelConfig acsA4ByPUMA_C nullVectorProjections_C msASE_CASR_cwd tp3InnerFld
  runAllModels predictorCacheKey modelOne acsA4ByPUMA_C nullVectorProjections_C


{-
predictorModel3_CSR_A5SR :: forall r . (K.KnitEffects r, BRK.CacheEffects r)
                         => Bool
                         -> BR.CommandLine
                         -> K.Sem r (K.ActionWithCacheTime r (DTM3.Predictor Text))
predictorModel3_CSR_A5SR clearCache cmdLine = do
  acsA5ByPUMA_C <- DDP.cachedACSa5ByPUMA
  let nvpsCacheKey = "model/demographic/csr_a5sr_a5serNVPs.bin"
      predictorCacheKey = "model/demographic/csr_a5sr_a5serPredictor.bin"
  when clearCache $ traverse_ BRK.clearIfPresentD [nvpsCacheKey, predictorCacheKey]
  let msCSR_A5SR_cwd = msSER_A5SR DMS.cwdWgtLens DMS.innerProductCWD'
  nullVectorProjections_C <- cachedNVProjections nvpsCacheKey msCSR_A5SR_cwd acsA5ByPUMA_C

  let tp3NumKeys = S.size (Keyed.elements @(F.Record CSR)) + S.size (Keyed.elements @(F.Record A5SR))
      tp3InnerFld = DTM3.mergeInnerFlds [VS.singleton . DTM3.cwdListToLogPWDensity <$> DTM3.cwdInnerFld (F.rcast @CSR) DTM3.cwdF
                                        , DTM3.cwdListToLogitVec <$> DTM3.cwdInnerFld (F.rcast @CSR) DTM3.cwdF
                                        , DTM3.cwdListToLogitVec <$> DTM3.cwdInnerFld (F.rcast @A5SR) DTM3.cwdF
                                        ]
      tp3RunConfig n = DTM3.RunConfig n False False Nothing
      tp3ModelConfig = DTM3.ModelConfig True (DTM3.dmr "CSR_A5SR" (tp3NumKeys + 1)) -- +1 for pop density
                         DTM3.AlphaHierNonCentered DTM3.NormalDist
      modelOne n = DTM3.runProjModel False cmdLine (tp3RunConfig n) tp3ModelConfig acsA5ByPUMA_C nullVectorProjections_C msCSR_A5SR_cwd tp3InnerFld
  runAllModels predictorCacheKey modelOne acsA5ByPUMA_C nullVectorProjections_C
-}


{-
predictorModel3_ASE_ASR :: forall r . (K.KnitEffects r, BRK.CacheEffects r)
                         => Bool
                         -> BR.CommandLine
                         -> K.Sem r (K.ActionWithCacheTime r (DTM3.Predictor Text))
predictorModel3_ASE_ASR clearCache cmdLine = do
  acsA4ByPUMA_C <- DDP.cachedACSa4ByPUMA
  let nvpsCacheKey = "model/demographic/ase_asr_aserNVPs.bin"
      predictorCacheKey = "model/demographic/ase_asr_aserPredictor.bin"
  when clearCache $ traverse_ BRK.clearIfPresentD [nvpsCacheKey, predictorCacheKey]
  let msASE_ASR_cwd = msASE_ASR DMS.cwdWgtLens DMS.innerProductCWD'
  let projCovariancesFld :: FL.Fold (F.Record )
      projCovariancesFld =
        DTP.diffCovarianceFldMS
        DMS.cwdWgtLens
        (F.rcast @[GT.StateAbbreviation, GT.PUMA])
        (F.rcast @ASER)
        DTM3.cwdF
        msASE_ASR_cwd
  nullVectorProjections_C <- BRK.retrieveOrMakeD nvpsCacheKey acsA4ByPUMA_C
                             $ \acsByPUMA -> do
    K.logLE K.Info $ "Computing covariance matrix of projected differences."
    let (projMeans, projCovariances) = FL.fold projCovariancesFld acsByPUMA
        (eigVals, _) = LA.eigSH projCovariances
    K.logLE K.Diagnostic
      $ "mean=" <> toText (DED.prettyVector projMeans)
      <> "\ncov=" <> toText (LA.disps 3 $ LA.unSym projCovariances)
      <> "\ncovariance eigenValues: " <> DED.prettyVector eigVals
    pure $ DTP.uncorrelatedNullVecsMS msASE_ASR_cwd projCovariances
  let tp3NumKeys = S.size (Keyed.elements @(F.Record ASE))
                   +  S.size (Keyed.elements @(F.Record ASR))
      tp3InnerFld ::  (F.ElemOf rs DT.PopCount, F.ElemOf rs DT.PWPopPerSqMile
                      , F.ElemOf rs DT.Age4C, F.ElemOf rs DT.SexC, F.ElemOf rs DT.Education4C, F.ElemOf rs DT.Race5C)
                  => FL.Fold (F.Record rs) (VS.Vector Double)
      tp3InnerFld = DTM3.mergeInnerFlds [VS.singleton . DTM3.cwdListToLogPWDensity <$> DTM3.cwdInnerFld (F.rcast @ASE) DTM3.cwdF
                                        , DTM3.cwdListToLogitVec <$> DTM3.cwdInnerFld (F.rcast @ASE) DTM3.cwdF
                                        , DTM3.cwdListToLogitVec <$> DTM3.cwdInnerFld (F.rcast @ASR) DTM3.cwdF
                                        ]
      tp3RunConfig n = DTM3.RunConfig n False False Nothing
      tp3ModelConfig = DTM3.ModelConfig True (DTM3.dmr "SER_ASR" (tp3NumKeys + 1)) -- +1 for pop density
                         DTM3.AlphaHierNonCentered DTM3.NormalDist
      modelOne n = DTM3.runProjModel False cmdLine (tp3RunConfig n) tp3ModelConfig acsA4ByPUMA_C nullVectorProjections_C msASE_ASR_cwd tp3InnerFld

  K.logLE K.Info "Running marginals as covariates model, if necessary."
  let modelResultDeps = (,) <$> acsA4ByPUMA_C <*> nullVectorProjections_C
  model3Res_C <- BRK.retrieveOrMakeD predictorCacheKey modelResultDeps
                 $ \(_, nvps) -> (do
                                     cachedModelResults <- sequenceA <$> traverse modelOne [0..(DTP.numProjections nvps - 1)]
                                     modelResults <- K.ignoreCacheTime cachedModelResults
                                     pure $ DTM3.Predictor nvps modelResults
                                 )
  pure model3Res_C
-}
subsetsMapFld :: (Monoid w, Ord k) => (row -> (k, w)) -> FL.Fold row (Map k w)
subsetsMapFld f = fmap M.fromList
                       $ MR.mapReduceFold
                       MR.noUnpack
                       (MR.Assign f)
                       (MR.ReduceFold $ \k -> fmap (k,) FL.mconcat)

compareMarginals :: (Monoid w, Ord k, Foldable g, Show k) => (rowA -> (k, w)) -> (rowB -> (k, w)) -> g rowA -> g rowB -> Either Text (Map k (w, w))
compareMarginals aF bF aRows bRows =
  let ma = FL.fold (subsetsMapFld aF) aRows
      mb = FL.fold (subsetsMapFld bF) bRows
      whenMatched _ wa wb = pure (wa, wb)
      whenMissingA k _ = Left $ "Missing bRows for k=" <> show k
      whenMissingB k _ = Left $ "Missing aRows for k=" <> show k
  in MM.mergeA (MM.traverseMissing whenMissingB) (MM.traverseMissing whenMissingA) (MM.zipWithAMatched whenMatched) ma mb


compareOuter :: (Ord k', Monoid w) => (k -> k') -> Map k (w, w) -> Map k' (w, w)
compareOuter outerKeyF =
  let fld = fmap M.fromList
            $ MR.mapReduceFold
            MR.noUnpack
            (MR.assign (outerKeyF . fst) snd)
            (MR.ReduceFold $ \k' -> fmap (k',) ((,) <$> FL.premap fst FL.mconcat <*> FL.premap snd FL.mconcat))
  in FL.fold fld . M.toList

compareMapToLog :: K.KnitEffects r => (k -> Text) -> ((w, w) -> Maybe Text) -> Map k (w, w) -> K.Sem r ()
compareMapToLog printK compareW = traverse_ f . M.toList
  where f (k, ws) = case compareW ws of
          Nothing -> pure ()
          Just mismatchT -> K.logLE K.Error ("Mismatch at k=" <> printK k <> ": " <> mismatchT)

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

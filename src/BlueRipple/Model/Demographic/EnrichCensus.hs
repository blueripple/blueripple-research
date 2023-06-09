{-# LANGUAGE AllowAmbiguousTypes #-}
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

import qualified BlueRipple.Configuration as BR
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.GeographicTypes as GT
import qualified BlueRipple.Data.DataFrames as BRDF
import BlueRipple.Model.Demographic.TPModel3 (ModelConfig(distribution))
import qualified BlueRipple.Data.Loaders as BRDF

import qualified BlueRipple.Model.Demographic.DataPrep as DDP
import qualified BlueRipple.Model.Demographic.EnrichData as DED
import qualified BlueRipple.Model.Demographic.MarginalStructure as DMS
import qualified BlueRipple.Model.Demographic.TableProducts as DTP
import qualified BlueRipple.Model.Demographic.TPModel3 as DTM3

import qualified BlueRipple.Data.Keyed as Keyed

import qualified BlueRipple.Data.CensusLoaders as BRC
import qualified BlueRipple.Data.CensusTables as BRC

import qualified BlueRipple.Utilities.KnitUtils as BRK

import qualified Knit.Report as K

import Control.Lens (Lens',view, (^.))
import qualified Control.Foldl as FL

import Data.Type.Equality (type (~))
import qualified Data.Map as M
import qualified Data.Map.Merge.Strict as MM
import qualified Data.Set as S
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
import qualified Colonnade as C

import GHC.TypeLits (Symbol)


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
type ASR = [DT.Age5C, DT.SexC, DT.Race5C]
type CASR = [DT.CitizenC, DT.Age5C, DT.SexC, DT.Race5C]
type ASE = [DT.Age5C, DT.SexC, DT.Education4C]
type ASER = [DT.Age5C, DT.SexC, DT.Education4C, DT.Race5C]
type CASER = [DT.CitizenC, DT.Age5C, DT.SexC, DT.Education4C, DT.Race5C]
type CA6SR = [DT.CitizenC, DT.Age6C, DT.SexC, DT.Race5C]
type A6SR = [DT.Age6C, DT.SexC, DT.Race5C]
type A6SER = [DT.Age6C, DT.SexC, DT.Education4C, DT.Race5C]
type LDOuterKeyR = BRDF.Year ': BRC.LDLocationR
type LDKey ks = LDOuterKeyR V.++ ks
type KeysWD ks = ks V.++ [DT.PopCount, DT.PWPopPerSqMile]

--type ASERDataR =   [DT.PopCount, DT.PWPopPerSqMile]
type CensusOuterKeyR = [BRDF.Year, GT.StateFIPS, GT.StateAbbreviation, GT.DistrictTypeC, GT.DistrictName]
type PUMAOuterKeyR = [BRDF.Year, GT.StateAbbreviation, GT.StateFIPS, GT.PUMA]
type PUMARowR ks = PUMAOuterKeyR V.++ KeysWD ks
type CensusASER = CensusOuterKeyR V.++ KeysWD ASER
type CensusA6SER = CensusOuterKeyR V.++ KeysWD A6SER
type CensusCASR = CensusOuterKeyR V.++ KeysWD CASR
type CensusCA6SR = CensusOuterKeyR V.++ KeysWD CA6SR

marginalStructure :: forall ks as bs w qs .
                     (Monoid w
                     , qs ~ F.RDeleteAll (as V.++ bs) ks
                     , qs V.++ (as V.++ bs) ~ (qs V.++ as) V.++ bs
                     , Ord (F.Record ks)
                     , Keyed.FiniteSet (F.Record ks)
                     , ((qs V.++ as) V.++ bs) F.⊆ ks
                     , ks F.⊆ ((qs V.++ as) V.++ bs)
                     , Ord (F.Record qs)
                     , Ord (F.Record as)
                     , Ord (F.Record bs)
                     , Ord (F.Record (qs V.++ as))
                     , Ord (F.Record (qs V.++ bs))
                     , Ord (F.Record ((qs V.++ as) V.++ bs))
                     , Keyed.FiniteSet (F.Record qs)
                     , Keyed.FiniteSet (F.Record as)
                     , Keyed.FiniteSet (F.Record bs)
                     , Keyed.FiniteSet (F.Record (qs V.++ as))
                     , Keyed.FiniteSet (F.Record (qs V.++ bs))
                     , Keyed.FiniteSet (F.Record ((qs V.++ as) V.++ bs))
                     , as F.⊆ (qs V.++ as)
                     , bs F.⊆ (qs V.++ bs)
                     , qs F.⊆ (qs V.++ as)
                     , qs F.⊆ (qs V.++ bs)
                     , (qs V.++ as) F.⊆ ((qs V.++ as) V.++ bs)
                     , (qs V.++ bs) F.⊆ ((qs V.++ as) V.++ bs)
                     )
                 => Lens' w Double
                 -> (Map (F.Record as) w -> Map (F.Record bs) w -> Map (F.Record as, F.Record bs) w)
                 -> DMS.MarginalStructure w (F.Record ks)
marginalStructure wl innerProduct =  DMS.reKeyMarginalStructure
                                     (F.rcast @(qs V.++ as V.++ bs))
                                     (F.rcast @ks)
                                     $ DMS.combineMarginalStructuresF @qs
                                     wl innerProduct
                                     (DMS.identityMarginalStructure @(F.Record (qs V.++ as)) wl)
                                     (DMS.identityMarginalStructure @(F.Record (qs V.++ bs)) wl)


type LDRecoded ks = LDKey ks V.++ [DT.PopCount, DT.PWPopPerSqMile]

recodeASR :: F.FrameRec (BRC.CensusRow BRC.LDLocationR BRC.CensusDataR [DT.Age5C, DT.SexC, BRC.RaceEthnicityC])
          -> F.FrameRec (LDRecoded ASR)
recodeASR = fmap F.rcast . FL.fold reFld
  where
    reFld = FMR.concatFold
             $ FMR.mapReduceFold
             FMR.noUnpack
             (FMR.assignKeysAndData @(LDKey [DT.SexC, DT.Age5C]))
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

recodeCSR ::  F.FrameRec (BRC.CensusRow BRC.LDLocationR BRC.CensusDataR [BRC.CitizenshipC, DT.SexC, BRC.RaceEthnicityC])
          -> F.FrameRec (LDRecoded CSR)
recodeCSR = fmap F.rcast . FL.fold reFld . FL.fold cFld
  where
    reFld = FMR.concatFold
             $ FMR.mapReduceFold
             FMR.noUnpack
             (FMR.assignKeysAndData @(LDKey [DT.CitizenC, DT.SexC]))
             (FMR.makeRecsWithKey id $ FMR.ReduceFold $ const BRC.reToR5Fld)
    cFld =  FMR.concatFold
             $ FMR.mapReduceFold
             FMR.noUnpack
             (FMR.assignKeysAndData @(LDKey [DT.SexC, BRC.RaceEthnicityC]))
             (FMR.makeRecsWithKey id $ FMR.ReduceFold $ const BRC.cToCFld)


{-
recodeASE ::  F.FrameRec (BRC.CensusRow BRC.LDLocationR BRC.CensusDataR [DT.Age5C, DT.SexC, DT.EducationC])
          -> F.FrameRec (LDRecoded ASE)
recodeASE = fmap F.rcast . FL.fold edFld
  where
    edFld = FMR.concatFold
             $ FMR.mapReduceFold
             FMR.noUnpack
             (FMR.assignKeysAndData @(LDKey [DT.Age6C, DT.SexC]))
             (FMR.makeRecsWithKey id $ FMR.ReduceFold $ const BRC.eToe4Fld)
-}


csr_asr_tableProductWithDensity :: F.FrameRec (KeysWD CSR) -> F.FrameRec (KeysWD ASR) -> F.FrameRec (KeysWD CASR)
csr_asr_tableProductWithDensity csr asr = F.toFrame $ fmap toRecord $ concat $ fmap pushOuterIn $ M.toList $ fmap M.toList
                                          $ DMS.tableProduct DMS.innerProductCWD' csrMap a6srMap
  where
    pushOuterIn (o, xs) = fmap (o,) xs
    tcwd :: (F.ElemOf rs DT.PopCount, F.ElemOf rs DT.PWPopPerSqMile) => F.Record rs -> DMS.CellWithDensity
    tcwd r = DMS.CellWithDensity (realToFrac $ view DT.popCount r) (view DT.pWPopPerSqMile r)
    okF :: (SR F.⊆ rs) => F.Record rs -> F.Record SR
    okF = F.rcast
    csrTMFld :: FL.Fold (F.Record (KeysWD CSR)) (Map (F.Record SR) (Map (F.Record '[DT.CitizenC]) DMS.CellWithDensity))
    csrTMFld = FL.premap (\r -> ((okF r, F.rcast @'[DT.CitizenC] r), tcwd r)) DMS.tableMapFld
    asrTMFld :: FL.Fold (F.Record (KeysWD ASR)) (Map (F.Record SR) (Map (F.Record '[DT.Age5C]) DMS.CellWithDensity))
    asrTMFld = FL.premap (\r -> ((okF r, F.rcast @'[DT.Age5C] r), tcwd r)) DMS.tableMapFld
    csrMap = FL.fold csrTMFld csr
    a6srMap = FL.fold asrTMFld asr
    cwdToRec :: DMS.CellWithDensity -> F.Record [DT.PopCount, DT.PWPopPerSqMile]
    cwdToRec cwd = round (DMS.cwdWgt cwd) F.&: DMS.cwdDensity cwd F.&: V.RNil
    toRecord :: (F.Record SR
                , ((F.Record '[DT.CitizenC], F.Record '[DT.Age5C]), DMS.CellWithDensity)
                )
             -> F.Record (KeysWD CASR)
    toRecord (outer, ((c, a), cwd)) = F.rcast $ c F.<+> a F.<+> outer F.<+> cwdToRec cwd


--type ProdOuter = [BRDF.Year, GT.StateFIPS, GT.DistrictTypeC, GT.DistrictName]

checkCensusTables :: K.KnitEffects x => K.ActionWithCacheTime x BRC.LoadedCensusTablesByLD ->  K.Sem x ()
checkCensusTables censusTables_C = do
  censusTables <- K.ignoreCacheTime censusTables_C
  let popFld = fmap Sum $ FL.premap (view DT.popCount) FL.sum
      label r = (r ^. GT.stateFIPS, GT.LegislativeDistrict (r ^. GT.districtTypeC) (r ^. GT.districtName))
  compMap <- K.knitEither $ BRC.analyzeAllPerPrefix label popFld censusTables
  K.logLE K.Info $ "\n" <> toText (C.ascii (fmap toString censusCompColonnade) $ M.toList compMap)

censusCompColonnade :: C.Colonnade C.Headed ((Int, GT.LegislativeDistrict), (Sum Int, Sum Int, Sum Int, Sum Int, Sum Int)) Text
censusCompColonnade =
  let a6sr (x, _, _, _, _) = x
      csr (_, x, _, _, _) = x
      ser (_, _, x, _, _) = x
      srl (_, _, _, x, _) = x
      ase (_, _, _, _, x) = x
  in C.headed "District" (\((sf, ld), _) -> show sf <> ": " <> show ld)
     <>  C.headed "ASR' (18+)" (show . getSum . a6sr . snd)
     <>  C.headed "CSR' (18+)" (show . getSum . csr . snd)
     <>  C.headed "SER' (25+)" (show . getSum . ser . snd)
     <>  C.headed "SR'L" (show . getSum . srl . snd)
     <>  C.headed "ASE (18+)" (show . getSum . ase . snd)





censusASR_CSR_Products :: forall r . (K.KnitEffects r, BRK.CacheEffects r)
                       => Text
                       -> K.ActionWithCacheTime r BRC.LoadedCensusTablesByLD
                       -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec CensusCASR))
censusASR_CSR_Products cacheKey censusTables_C = do
  let
    f :: BRC.LoadedCensusTablesByLD -> K.Sem r (F.FrameRec CensusCASR)
    f (BRC.CensusTables asr csr _ _ _) = do
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
          csrF :: (KeysWD CSR F.⊆ rs) => F.Record rs -> F.Record (KeysWD CSR)
          csrF = F.rcast
          asrF :: (KeysWD ASR F.⊆ rs) => F.Record rs -> F.Record (KeysWD ASR)
          asrF = F.rcast
      -- check tables before recoding
          compareF (s1, s2) =
            let n1 = getSum s1
                n2 = getSum s2
            in if abs (n1 - n2) > 20 then Just $ "N(CSR)=" <> show n1 <> "; N(ASR)=" <> show n2 else Nothing
      innerCompareMap <- K.knitEither
                         $ compareMarginals
                         (\csrR -> (F.rcast @(LDOuterKeyR V.++ SRo) csrR, Sum $ view DT.popCount csrR))
                         (\asrR -> (F.rcast @(LDOuterKeyR V.++ SRo) asrR, Sum $ view DT.popCount asrR))
                         csr asr
      let outerCompareMap = compareOuter (F.rcast @LDOuterKeyR) innerCompareMap
      compareMapToLog show compareF outerCompareMap
      compareMapToLog show compareF innerCompareMap
      let recodedCSR = recodeCSR csr
          recodedA6SR = recodeASR asr
--      BRK.logFrame ser
--      BRK.logFrame recodedSER
      let csrMap = FL.fold (toMapFld keyF csrF) $ recodedCSR
          asrMap = FL.fold (toMapFld keyF asrF) $ recodedA6SR
          checkFrames :: (Show k, F.ElemOf as DT.PopCount, F.ElemOf bs DT.PopCount)
                      => k -> F.FrameRec as -> F.FrameRec bs -> K.Sem r ()
          checkFrames k ta tb = do
            let na = FL.fold (FL.premap (view DT.popCount) FL.sum) ta
                nb = FL.fold (FL.premap (view DT.popCount) FL.sum) tb
            when (abs (na - nb) > 20) $ K.logLE K.Error $ "Mismatched CSR/ASR tables at k=" <> show k <> ". N(CSR)=" <> show na <> "; N(ASR)=" <> show nb
            pure ()
          whenMatchedF k csr' asr' = checkFrames k csr' asr' >> pure (csr_asr_tableProductWithDensity csr' asr')
          whenMissingCSRF k _ = K.knitError $ "Missing CSR table for k=" <> show k
          whenMissingASRF k _ = K.knitError $ "Missing ASR table for k=" <> show k
      productMap <- MM.mergeA
                    (MM.traverseMissing whenMissingASRF)
                    (MM.traverseMissing whenMissingCSRF)
                    (MM.zipWithAMatched whenMatchedF)
                    csrMap
                    asrMap
      let assocToFrame (k, fr) = do
            let fips = k ^. GT.stateFIPS
            sa <- K.knitMaybe ("Missing FIPS=" <> show fips) $ M.lookup fips stateAbbrFromFIPSMap
            let newK :: F.Record CensusOuterKeyR
                newK = F.rcast $ (FT.recordSingleton @GT.StateAbbreviation sa) F.<+> k
            pure $ fmap (newK F.<+>) fr
      mconcat <$> fmap assocToFrame $ M.toList productMap
  BRK.retrieveOrMakeFrame cacheKey censusTables_C f


logitMarginalsCMatrixCSR_ASR :: LA.Matrix Double
logitMarginalsCMatrixCSR_ASR =
  let csrInCASR :: F.Record CA6SR -> F.Record CSR
      csrInCASR = F.rcast
      asrInCASR :: F.Record CASR -> F.Record ASR
      asrInCASR = F.rcast
      csrInCASR_stencils = fmap (DMS.expandStencil csrInCASR)
                           $ DMS.msStencils
                           $ DMS.identityMarginalStructure @(F.Record CSR) DMS.cwdWgtLens
      asrInCASR_stencils = fmap (DMS.expandStencil asrInCASR)
                           $ DMS.msStencils
                           $ DMS.identityMarginalStructure @(F.Record A6SR) DMS.cwdWgtLens
  in DED.mMatrix (S.size $ Keyed.elements @(F.Record CASR)) (csrInCASR_stencils <> asrInCASR_stencils)

logitMarginals :: LA.Matrix Double -> VS.Vector Double -> VS.Vector Double
logitMarginals cMat prodDistV = VS.map (DTM3.bLogit 1e-10) (cMat LA.#> prodDistV)

popAndpwDensityFld :: FL.Fold DMS.CellWithDensity (Double, Double)
popAndpwDensityFld = DT.densityAndPopFld' (const 1) DMS.cwdWgt DMS.cwdDensity

predictedCensusCASR :: forall r . (K.KnitEffects r, BRK.CacheEffects r)
                    => Bool
                    -> Text
                    -> K.ActionWithCacheTime r (DTM3.Predictor Text)
                    -> K.ActionWithCacheTime r BRC.LoadedCensusTablesByLD
                    -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec CensusCASR), K.ActionWithCacheTime r (F.FrameRec CensusCASR))
predictedCensusCASR rebuild cacheRoot predictor_C censusTables_C = do
  let productCacheKey = cacheRoot <> "_products.bin"
      predictedCacheKey = cacheRoot <> "_modeled.bin"
      logitMarginals' = logitMarginals logitMarginalsCMatrixCSR_ASR
  when rebuild $ BRK.clearIfPresentD productCacheKey >> BRK.clearIfPresentD predictedCacheKey
  products_C <- censusASR_CSR_Products productCacheKey censusTables_C
  K.ignoreCacheTime products_C >>= BRK.logFrame
  let predictFld :: DTM3.Predictor Text -> Text -> FL.FoldM (Either Text) (F.Record (KeysWD CASR)) (F.FrameRec (KeysWD CASR))
      predictFld predictor sa =
        let key = F.rcast @CASR
            w r = DMS.CellWithDensity (realToFrac $ r ^. DT.popCount) (r ^. DT.pWPopPerSqMile)
            g r = (key r, w r)
            prodMapFld = FL.premap g DMS.zeroFillSummedMapFld
            posLog x = if x < 1 then 0 else Numeric.log x
            popAndPWDensity :: Map (F.Record CASR) DMS.CellWithDensity -> (Double, Double)
            popAndPWDensity = FL.fold popAndpwDensityFld
            covariates pwD prodDistV = mconcat [VS.singleton (posLog pwD), logitMarginals' prodDistV]
            prodV pm = VS.fromList $ fmap DMS.cwdWgt $ M.elems pm
            toRec :: (F.Record CASR, DMS.CellWithDensity) -> F.Record (KeysWD CASR)
            toRec (k, cwd) = k F.<+> (round (DMS.cwdWgt cwd) F.&: DMS.cwdDensity cwd F.&: V.RNil)
            predict pm = let (n, pwD) = popAndPWDensity pm
                             pV = prodV pm
                             pDV = VS.map (/ n) pV
                         in F.toFrame . fmap toRec <$> DTM3.predictedJoint DMS.cwdWgtLens predictor sa (covariates pwD pDV) (M.toList pm)
        in FMR.postMapM predict $ FL.generalize prodMapFld
  let f :: DTM3.Predictor Text -> F.FrameRec CensusCASR -> K.Sem r (F.FrameRec CensusCASR)
      f predictor products = do
        let rFld :: F.Record CensusOuterKeyR -> FL.FoldM (K.Sem r) (F.Record (KeysWD CASR)) (F.FrameRec CensusCASR)
            rFld k = FMR.postMapM (\x -> K.logLE K.Info ("predicting " <> show k) >> pure x)
                     $ FL.hoists K.knitEither $ fmap (fmap (k F.<+>)) $ predictFld predictor (k ^. GT.stateAbbreviation)
            fldM = FMR.concatFoldM
                   $ FMR.mapReduceFoldM
                   (FMR.generalizeUnpack FMR.noUnpack)
                   (FMR.generalizeAssign $ FMR.assignKeysAndData @CensusOuterKeyR @(KeysWD CASR))
                   (MR.ReduceFoldM rFld)
        K.logLE K.Info "Building/re-building censusCASR predictions"
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

  nvp_C <- BRK.retrieveOrMakeD cacheKey cachedDataRows
                             $ \dataRows -> do
    K.logLE K.Info $ "Computing covariance matrix of projected differences."
    let (projMeans, projCovariances) = FL.fold fld dataRows
        (eigVals, _) = LA.eigSH projCovariances
    K.logLE K.Diagnostic
      $ "mean=" <> toText (DED.prettyVector projMeans)
      <> "\ncov=" <> toText (LA.disps 3 $ LA.unSym projCovariances)
      <> "\ncovariance eigenValues: " <> DED.prettyVector eigVals
    pure $ DTP.uncorrelatedNullVecsMS ms projCovariances
  nvp <- K.ignoreCacheTime nvp_C
  K.logLE K.Info $ "Null-Space is " <> show (fst $ LA.size $ DTP.nvpProj nvp) <> " dimensional."
  pure nvp_C

innerFoldWD :: forall as bs rs . (F.ElemOf rs DT.PopCount, F.ElemOf rs DT.PWPopPerSqMile
                                 , Ord (F.Record as), Keyed.FiniteSet (F.Record as)
                                 , Ord (F.Record bs), Keyed.FiniteSet (F.Record bs)
                                 )
            => (F.Record rs -> F.Record as)
            -> (F.Record rs -> F.Record bs)
            -> FL.Fold (F.Record rs) (VS.Vector Double)
innerFoldWD toAs toBs = DTM3.mergeInnerFlds [VS.singleton . DTM3.cwdListToLogPWDensity <$> DTM3.cwdInnerFld toAs DTM3.cwdF
                                            , DTM3.cwdListToLogitVec <$> DTM3.cwdInnerFld toAs DTM3.cwdF
                                            , DTM3.cwdListToLogitVec <$> DTM3.cwdInnerFld toBs DTM3.cwdF
                                            ]

runAllModels :: (K.KnitEffects r, BRK.CacheEffects r)
             => Text
             -> (Int -> K.Sem r (K.ActionWithCacheTime r (DTM3.ComponentPredictor Text)))
             -> K.ActionWithCacheTime r (F.FrameRec rs)
             -> K.ActionWithCacheTime r DTP.NullVectorProjections
             -> K.Sem r (K.ActionWithCacheTime r (DTM3.Predictor Text)
                        , K.ActionWithCacheTime r DTP.NullVectorProjections
                        )
runAllModels cacheKey modelOne cachedDataRows cachedNVPs = do
  K.logLE K.Info "Running marginals as covariates model, if necessary."
  let modelResultDeps = (,) <$> cachedDataRows <*> cachedNVPs
  model3Res_C <- BRK.retrieveOrMakeD cacheKey modelResultDeps
                 $ \(_, nvps) -> (do
                                     cachedModelResults <- sequenceA <$> traverse modelOne [0..(DTP.numProjections nvps - 1)]
                                     modelResults <- K.ignoreCacheTime cachedModelResults
                                     pure $ DTM3.Predictor nvps modelResults
                                 )
  pure (model3Res_C, cachedNVPs)


predictorModel3 :: forall (as :: [(Symbol, Type)]) (bs :: [(Symbol, Type)]) ks qs r
                   . (K.KnitEffects r, BRK.CacheEffects r
                     , qs ~ F.RDeleteAll (as V.++ bs) ks
                     , qs V.++ (as V.++ bs) ~ (qs V.++ as) V.++ bs
                     , Ord (F.Record ks)
                     , V.RMap as
                     , V.ReifyConstraint Show F.ElField as
                     , V.RecordToList as
                     , V.RMap bs
                     , V.ReifyConstraint Show F.ElField bs
                     , V.RecordToList bs
                     , ks F.⊆ PUMARowR ks
                     , Keyed.FiniteSet (F.Record ks)
                     , ((qs V.++ as) V.++ bs) F.⊆ ks
                     , ks F.⊆ ((qs V.++ as) V.++ bs)
                     , Ord (F.Record qs)
                     , Ord (F.Record as)
                     , Ord (F.Record bs)
                     , Ord (F.Record (qs V.++ as))
                     , Ord (F.Record (qs V.++ bs))
                     , Ord (F.Record ((qs V.++ as) V.++ bs))
                     , Keyed.FiniteSet (F.Record qs)
                     , Keyed.FiniteSet (F.Record as)
                     , Keyed.FiniteSet (F.Record bs)
                     , Keyed.FiniteSet (F.Record (qs V.++ as))
                     , Keyed.FiniteSet (F.Record (qs V.++ bs))
                     , Keyed.FiniteSet (F.Record ((qs V.++ as) V.++ bs))
                     , as F.⊆ (qs V.++ as)
                     , bs F.⊆ (qs V.++ bs)
                     , qs F.⊆ (qs V.++ as)
                     , qs F.⊆ (qs V.++ bs)
                     , (qs V.++ as) F.⊆ ((qs V.++ as) V.++ bs)
                     , (qs V.++ bs) F.⊆ ((qs V.++ as) V.++ bs)
                     , F.ElemOf (KeysWD ks) DT.PopCount
                     , F.ElemOf (KeysWD ks) DT.PWPopPerSqMile
                     , qs V.++ as F.⊆ PUMARowR ks
                     , qs V.++ bs F.⊆ PUMARowR ks
                     )
                => Either Text Text
                -> Text
                -> BR.CommandLine
                -> K.ActionWithCacheTime r (F.FrameRec (PUMARowR ks))
                -> K.Sem r (K.ActionWithCacheTime r (DTM3.Predictor Text)
                           , K.ActionWithCacheTime r DTP.NullVectorProjections
                           , DMS.MarginalStructure DMS.CellWithDensity (F.Record ks)
                           )
predictorModel3 cachePrefixE modelId cmdLine acs_C = do
  (ckp, clearCaches) <- case cachePrefixE of
    Left ck -> pure (ck, True)
    Right ck -> pure (ck, False)
  let
    nvpsCacheKey = ckp <> "_NVPs.bin"
    predictorCacheKey = ckp <> "_Predictor.bin"
  when clearCaches $ traverse_ BRK.clearIfPresentD [nvpsCacheKey, predictorCacheKey]
  let ms = marginalStructure @ks @as @bs @DMS.CellWithDensity @qs DMS.cwdWgtLens DMS.innerProductCWD'
  nullVectorProjections_C <- cachedNVProjections nvpsCacheKey ms acs_C

  let tp3NumKeys = S.size (Keyed.elements @(F.Record (qs V.++ bs))) + S.size (Keyed.elements @(F.Record (qs V.++ as)))
      tp3InnerFld = innerFoldWD @(qs V.++ bs) @(qs V.++ as) @(PUMARowR ks) (F.rcast @(qs V.++ bs)) (F.rcast @(qs V.++ as))
      tp3RunConfig n = DTM3.RunConfig n False False Nothing
      tp3ModelConfig = DTM3.ModelConfig True (DTM3.dmr modelId (tp3NumKeys + 1)) -- +1 for pop density
                         DTM3.AlphaHierNonCentered DTM3.NormalDist
      modelOne n = DTM3.runProjModel @ks @(PUMARowR ks) clearCaches cmdLine (tp3RunConfig n) tp3ModelConfig acs_C nullVectorProjections_C ms tp3InnerFld
  (predictor_C, nvps_C) <- runAllModels predictorCacheKey modelOne acs_C nullVectorProjections_C
  pure (predictor_C, nvps_C, ms)


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
type CensusCASERR = BRC.CensusRow BRC.LDLocationR BRC.CensusDataR [DT.CitizenC, DT.Age4C, DT.SexC, DT.EducationC, BRC.RaceEthnicityC]
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
  let ncFld = DED.nearestCountsFrameFld @BRC.Count @DT.SimpleAgeC @DT.EducationC (DED.nearestCountsFrameIFld DED.nearestCountsKL) desiredRowSumLookup allEdus
-}

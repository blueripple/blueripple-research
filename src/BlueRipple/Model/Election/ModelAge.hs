{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE UnicodeSyntax #-}

module BlueRipple.Model.Election.ModelAge where

import qualified BlueRipple.Data.ACS_PUMS as PUMS
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.ElectionTypes as ET
import qualified BlueRipple.Data.Loaders as BRL
import qualified BlueRipple.Utilities.KnitUtils as BRK
import qualified BlueRipple.Data.DataFrames as BRDF
import qualified BlueRipple.Data.Keyed as BRK

import qualified Stan.ModelBuilder as S
import qualified Stan.ModelBuilder.DesignMatrix as DM
import qualified Stan.ModelBuilder.BuildingBlocks as SB
import qualified Stan.ModelConfig as SC
import qualified Stan.ModelBuilder.TypedExpressions.Statements as TE
import qualified Stan.ModelBuilder.TypedExpressions.Expressions as TE
import qualified Stan.ModelBuilder.TypedExpressions.Operations as TEO
import qualified Stan.ModelBuilder.TypedExpressions.Indexing as TEI
import qualified Stan.ModelBuilder.TypedExpressions.StanFunctions as SF
import qualified Stan.ModelBuilder.Distributions as SD
import qualified Stan.ModelBuilder.TypedExpressions.DAG as DAG
import qualified Stan.ModelBuilder.TypedExpressions.DAGTypes as DAG
import Stan.ModelBuilder.TypedExpressions.TypedList (TypedList(..))
import qualified Stan.ModelBuilder.TypedExpressions.TypedList as STL

import qualified Control.MapReduce.Simple as MR
import qualified Frames.MapReduce as FMR
import qualified Frames.Transform as FT
import qualified Frames.Streamly.Transform as FST
import qualified Frames.Streamly.InCore as FS
import qualified Frames.Serialize as FS
import qualified Flat

import qualified Control.Foldl as FL
import qualified Data.Map as M
import qualified Data.Set as S
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V
import qualified Data.Vector as Vec
import qualified Data.Vector.Unboxed as VU
import qualified Frames as F
import qualified Frames.Melt as F
import qualified Knit.Report as K
import qualified Numeric
import qualified Stan.ModelBuilder.TypedExpressions.DAG as DAg
import qualified CmdStan as CS
import qualified Stan.Parameters as SP
import qualified Data.IntMap.Strict as IM



data ModelConfig a = ModelConfig { modelID :: a, includeAlpha :: Bool, includeDensity :: Bool} deriving (Functor, Show)

modelConfigSuffix :: ModelConfig a -> Text
modelConfigSuffix (ModelConfig _ ia id) = case (ia, id) of
  (False, False) -> ""
  (True, False) ->  "_a"
  (False, True) -> "_d"
  (True, True) -> "_ad"

modelName :: ModelConfig Text -> Text
modelName mc = modelID mc <> modelConfigSuffix mc

addTermMaybe :: Maybe a -> (a -> TE.UExpr t -> TE.UExpr t) -> TE.UExpr t -> TE.UExpr t
addTermMaybe mA combine e = case mA of
  Nothing -> e
  Just a -> combine a e

betaBinomialModel :: forall rs. (Typeable rs, F.ElemOf rs DT.PopPerSqMile)
            => DM.DesignMatrixRow (F.Record rs)
            -> ModelConfig ()
            -> S.StanBuilderM [(F.Record rs, VU.Vector Int)] () ()
betaBinomialModel dmr mc = do
  acsData <- S.dataSetTag @(F.Record rs, VU.Vector Int) SC.ModelData "ACS"
  let nData = S.dataSetSizeE acsData
      nStates = S.groupSizeE stateGroup
--  countsE <- SB.addIntArrayData acsData "counts" (TE.intE 2) (Just 0) Nothing snd
  let trialsF v = v VU.! 0 + v VU.! 1
      successesF v = v VU.! 1
  trials <- SB.addCountData acsData "trials" (trialsF . snd)
  successes <- SB.addCountData acsData "successes" (successesF . snd)
  mRawDensity <- case includeDensity mc of
    True -> Just <$> SB.addRealData acsData "rawLogDensity" Nothing Nothing (safeLog . F.rgetField @DT.PopPerSqMile . fst)
    False -> pure Nothing
  acsMat <- DM.addDesignMatrix acsData (contramap fst dmr) Nothing
  let (_, nPredictors) = DM.designMatrixColDimBinding dmr Nothing
      at x n = TE.sliceE TEI.s0 n x
      by v i = TE.indexE TEI.s0 i v
      eltTimes = TE.binaryOpE (TEO.SElementWise TEO.SMultiply)
      eltDivide = TE.binaryOpE (TEO.SElementWise TEO.SDivide)
  -- transformed data
  absACSMat <- S.inBlock S.SBTransformedData $ S.addFromCodeWriter
               $ TE.declareRHSNW (TE.NamedDeclSpec "absACSMat" $ TE.matrixSpec nData nPredictors [])
               $ TE.functionE SF.abs (acsMat :> TNil)

  -- standardize density. Subtract mean and divide by standard deviation
  mDensity <- case mRawDensity of
    Nothing -> pure Nothing
    Just d -> Just <$> do
      uwmvf <- SB.unWeightedMeanVarianceFunction
      S.inBlock S.SBTransformedData $ S.addFromCodeWriter $ do
        let m = TE.functionE SF.mean (d :> TNil)
            sd = TE.functionE SF.sqrt (TE.functionE SF.variance (d :> TNil) :> TNil)
--        mv <- TE.declareRHSNW (TE.NamedDeclSpec "meanVarLD" $ TE.vectorSpec (TE.intE 2) []) $ TE.functionE uwmvf (d :> d :> TNil)
        TE.declareRHSNW (TE.NamedDeclSpec "stdLogDensity" $ TE.vectorSpec nData [])
          $ (d `TE.minusE` m) `TE.divideE` sd

  -- parameters

  mAlpha0P <- case includeAlpha mc of
    True -> Just
            <$> DAG.simpleParameterWA
            (TE.NamedDeclSpec "alpha0" $ TE.realSpec [])
            (TE.DensityWithArgs SF.normalS (TE.realE 0 :> TE.realE 1 :> TNil))
    False -> pure Nothing

  sigmaAlphaP <- DAG.simpleParameterWA
             (TE.NamedDeclSpec "sigmaAlpha" $ TE.realSpec [TE.lowerM $ TE.realE 0])
             (TE.DensityWithArgs SF.normalS (TE.realE 0 :> TE.realE 1 :> TNil))

  alphaP <- DAG.addCenteredHierarchical
            (TE.NamedDeclSpec "alpha" $ TE.vectorSpec nStates [])
            (DAG.given (TE.realE 0) :> DAG.build sigmaAlphaP :> TNil)
            SF.normalS

  mBetaDensityP <- case includeDensity mc of
    True -> Just <$> DAG.simpleParameterWA
                   (TE.NamedDeclSpec "beta_Density" $ TE.realSpec [])
                   (TE.DensityWithArgs SF.normalS (TE.realE 0 :> TE.realE 1 :> TNil))
    False -> pure Nothing

{-
  alphaP <- DAG.simpleNonCentered
            (TE.NamedDeclSpec "alpha" $ TE.vectorSpec nStates [])
            (TE.vectorSpec nStates [])
            (TE.DensityWithArgs SF.normalS $ TE.realE 0 :> TE.realE 1 :> TNil)
            (DAG.given (TE.realE 0) :> DAG.build sigmaAlphaP :> TNil)
            (\(ma :> sa :> TNil) r -> ma `TE.plusE` (sa `TE.timesE` r))
-}

  betaP <- DAG.simpleParameterWA
         (TE.NamedDeclSpec "beta" $ TE.vectorSpec nPredictors [])
         (TE.DensityWithArgs SF.normalS (TE.realE 0 :> TE.realE 2 :> TNil))
{-
  phiP <- DAG.simpleParameterWA
        (TE.NamedDeclSpec "phi" $ TE.vectorSpec nPredictors [TE.lowerM $ TE.realE 0])
        (TE.DensityWithArgs SF.normalS (TE.realE 0 :> TE.realE 30 :> TNil))
-}
  phiP <- DAG.addTransformedHP
          (TE.NamedDeclSpec "phi" $ TE.vectorSpec nPredictors [])
          (Just $ [TE.lowerM $ TE.realE 0, TE.upperM $ TE.realE 1]) -- constraints on phi_raw
          (TE.DensityWithArgs SF.betaS (TE.realE 99 :> TE.realE 1 :> TNil)) -- phi_raw is beta distributed
          (\t -> t `eltDivide` (TE.realE 1 `TE.minusE` t)) -- phi = phi_raw / (1 - phi_raw), component-wise


  let mAlpha0 = DAG.parameterTagExpr <$> mAlpha0P
      alpha = DAG.parameterTagExpr alphaP
      mBetaDensity = DAG.parameterTagExpr <$> mBetaDensityP
      beta = DAG.parameterTagExpr betaP
      phi = DAG.parameterTagExpr phiP
      mBetaDensityTerm = TE.timesE <$> mBetaDensity <*> mDensity
      logitMu = let x = (alpha `by` (S.byGroupIndexE acsData stateGroup))
                      `TE.plusE` (acsMat `TE.timesE` beta)
                in addTermMaybe mAlpha0 (\a e -> a `TE.plusE` e)
                   $ addTermMaybe mBetaDensityTerm (\bd e -> bd `TE.plusE` e) x
      vSpec = TE.vectorSpec nData []
      tempPs = do
        mu <- TE.declareRHSNW (TE.NamedDeclSpec "muV" vSpec) $ TE.functionE SF.inv_logit (logitMu :> TNil)
        phiV <- TE.declareRHSNW (TE.NamedDeclSpec "mV" vSpec) $  absACSMat `TE.timesE` phi
        betaA <- TE.declareRHSNW (TE.NamedDeclSpec "aV" vSpec) $ phiV `eltTimes` mu
        betaB <-TE.declareRHSNW (TE.NamedDeclSpec "bV" vSpec) $ phiV `eltTimes` (TE.realE 1 `TE.minusE` mu)
        pure (betaA, betaB)

  S.inBlock S.SBModel $ S.addFromCodeWriter $ do
    (betaA, betaB) <- tempPs
    let ps = trials :> betaA :> betaB :> TNil
    TE.addStmt $ TE.target $ TE.densityE SF.beta_binomial_lpmf successes ps

  SB.generateLogLikelihood
    acsData
    (S.betaBinomialDist' True)
    ((\(a, b) n -> trials `at` n :> a `at` n :> b `at` n :> TNil) <$> tempPs)
    (pure $ (successes `at`))

  S.inBlock S.SBGeneratedQuantities $ DM.splitToGroupVars dmr beta (Just "beta")
  S.inBlock S.SBGeneratedQuantities $ DM.splitToGroupVars dmr phi (Just "phi")
  _ <- SB.generatePosteriorPrediction
    acsData
    (TE.NamedDeclSpec "pObserved" $ TE.array1Spec nData $ TE.intSpec [])
    (SD.betaBinomialDist' True)
    ((\(a, b) n -> trials `at` n :> a `at` n :> b `at` n :> TNil) <$> tempPs)
  pure ()

groupBuilderState :: (F.ElemOf rs DT.StateAbbreviation, Typeable rs, Typeable a) => [Text] -> S.StanGroupBuilderM [(F.Record rs, a)] () ()
groupBuilderState states = do
  acsData <- S.addModelDataToGroupBuilder "ACS" (S.ToFoldable id)
  S.addGroupIndexForData stateGroup acsData $ S.makeIndexFromFoldable show (F.rgetField @DT.StateAbbreviation . fst) states
  S.addGroupIntMapForDataSet stateGroup acsData $ S.dataToIntMapFromFoldable (F.rgetField @DT.StateAbbreviation . fst) states

groupBuilderCD :: [Text] -> [Text] -> S.StanGroupBuilderM (F.FrameRec ACSByCD) () ()
groupBuilderCD states cds = do
  acsData <- S.addModelDataToGroupBuilder "ACS" (S.ToFoldable id)
  S.addGroupIndexForData stateGroup acsData $ S.makeIndexFromFoldable show (F.rgetField @DT.StateAbbreviation) states
  S.addGroupIndexForData cdGroup acsData $ S.makeIndexFromFoldable show districtKey cds

cdGroup :: S.GroupTypeTag Text
cdGroup = S.GroupTypeTag "CD"

stateGroup :: S.GroupTypeTag Text
stateGroup = S.GroupTypeTag "State"

designMatrixRowEdu7 :: forall rs a.(F.ElemOf rs DT.Age4C
                                  , F.ElemOf rs DT.SexC
                                  , F.ElemOf rs DT.RaceAlone4C
                                  , F.ElemOf rs DT.HispC
                                  )
                   => DM.DesignMatrixRow (F.Record rs)
designMatrixRowEdu7 = DM.DesignMatrixRow "DMEdu7" [sexRP, raceAgeRP]
  where
    sexRP = DM.boundedEnumRowPart Nothing "Sex" (F.rgetField @DT.SexC)
    race5Census r = DT.race5FromRaceAlone4AndHisp True (F.rgetField @DT.RaceAlone4C r) (F.rgetField @DT.HispC r)
    raceAgeRP = DM.boundedEnumRowPart Nothing "RaceAge"
                $ \r -> DM.BEProduct2 (race5Census r, F.rgetField @DT.Age4C r)


designMatrixRowEdu2 :: forall rs a.(F.ElemOf rs DT.Age4C
                                  , F.ElemOf rs DT.SexC
                                  , F.ElemOf rs DT.RaceAlone4C
                                  , F.ElemOf rs DT.HispC
                                  )
                   => DM.DesignMatrixRow (F.Record rs)
designMatrixRowEdu2 = DM.DesignMatrixRow "DMEdu2" [sexRP, raceAgeRP]
  where
    sexRP = DM.boundedEnumRowPart Nothing "Sex" (F.rgetField @DT.SexC)
    race5Census r = DT.race5FromRaceAlone4AndHisp True (F.rgetField @DT.RaceAlone4C r) (F.rgetField @DT.HispC r)
    raceAgeRP = DM.boundedEnumRowPart (Just $ DM.BEProduct2 (DT.R5_WhiteNonHispanic, DT.A4_25To44)) "RaceAge"
                $ \r -> DM.BEProduct2 (race5Census r, F.rgetField @DT.Age4C r)


--
data ModelResult g ks = ModelResult { alpha :: Double, geoAlpha :: Map g Double, ldSI :: (Double, Double), catAlpha :: Map (F.Record ks) Double }

deriving instance (Show (F.Record ks), Show g) => Show (ModelResult g ks)

modelResultToFTuple :: (Ord (V.Rec FS.SElField ks), V.RMap ks) => ModelResult g ks -> (Double, Map g Double, (Double, Double), Map (V.Rec FS.SElField ks) Double)
modelResultToFTuple (ModelResult a b c d) = (a, b, c, M.mapKeys FS.toS d)

modelResultFromFTuple :: (Ord (F.Record ks), V.RMap ks) => (Double, Map g Double, (Double, Double), Map (V.Rec FS.SElField ks) Double) -> ModelResult g ks
modelResultFromFTuple (a, b, c, d) = ModelResult a b c (M.mapKeys FS.fromS d)

instance (V.RMap ks, FS.RecVec ks, FS.RecFlat ks, Flat.Flat g, Ord g, Ord (F.Rec FS.SElField ks), Ord (F.Record ks)) => Flat.Flat (ModelResult g ks) where
  size = Flat.size . modelResultToFTuple
  encode = Flat.encode . modelResultToFTuple
  decode = fmap modelResultFromFTuple Flat.decode


applyModelResult :: (F.ElemOf rs DT.PopPerSqMile, ks F.⊆ rs, Ord g, Show g, Ord (F.Record ks), Show (F.Record rs))
                 => ModelResult g ks -> g -> F.Record rs -> Either Text Double
applyModelResult (ModelResult a ga (ldS, ldI) ca) g r = invLogit <$> xE where
  invLogit y = 1 / (1 + Numeric.exp (negate y))
  geoXE = maybe (Left $ "applyModelResult: " <> show g <> " missing from geography alpha map") Right $ M.lookup g ga
  densX = ldI + ldS * (safeLog $ F.rgetField @DT.PopPerSqMile r)
  catXE = maybe (Left $ "applyModelResult: " <> show r <> " missing from category alpha map") Right $ M.lookup (F.rcast r) ca
  xE = (\a d g c -> a + d + g + c) <$> pure a <*> pure densX <*> geoXE <*> catXE


stateModelResultAction :: forall rs ks a r gq.
                          (K.KnitEffects r
                          , Typeable rs
                          , Typeable a
                          , F.ElemOf rs DT.PopPerSqMile
                          , ks F.⊆ rs
                          , Ord (F.Record ks)
                          , BRK.FiniteSet (F.Record ks)
                          )
                       => ModelConfig Text
                       -> DM.DesignMatrixRow (F.Record ks)
                       -> SC.ResultAction r [(F.Record rs, a)] gq S.DataSetGroupIntMaps () (ModelResult Text ks)
stateModelResultAction mc dmr = SC.UseSummary f where
  f summary _ modelDataAndIndexes_C _ = do
    let resultCacheKey = modelID mc <> "_" <> DM.dmName dmr <> modelConfigSuffix mc
    (modelData, resultIndexesE) <- K.ignoreCacheTime modelDataAndIndexes_C
    -- we need to rescale the density component to work
    let premap = safeLog . F.rgetField @DT.PopPerSqMile . fst
        msFld = (,) <$> FL.mean <*> FL.std
        (ldMean, ldSigma) = FL.fold (FL.premap premap msFld) modelData
    stateIM <- K.knitEither
      $ resultIndexesE >>= S.getGroupIndex (S.RowTypeTag @(F.Record rs, a) SC.ModelData "ACS") stateGroup
    let getScalar n = K.knitEither $ SP.getScalar . fmap CS.mean <$> SP.parseScalar n (CS.paramStats summary)
        getVector n = K.knitEither $ SP.getVector . fmap CS.mean <$> SP.parse1D n (CS.paramStats summary)
    alpha <- case includeAlpha mc of
      False -> pure 0
      True -> getScalar "alpha0"
    geoMap <- (\stIM alphaV -> M.fromList $ zip (IM.elems stIM) (Vec.toList alphaV)) <$> pure stateIM <*> getVector "alpha"
    (ldSlope, ldIntercept) <- case includeDensity mc of
      False -> pure (0, 0)
      True -> (\x -> (x / ldSigma, negate $ x * ldMean / ldSigma)) <$> getScalar "beta_Density"
    catBeta <- VU.convert <$> getVector "beta"
    let (S.MatrixRowFromData _ _ rowLength rowVecF) = DM.matrixFromRowData dmr Nothing
        allCatRows = S.toList $ BRK.elements @(F.Record ks)
        g v1 v2 = VU.foldl' (\a (b, c) -> a + b * c) 0 $ VU.zip v1 v2
        catMap = M.fromList $ zip allCatRows (g catBeta . rowVecF <$> allCatRows)
    pure $ ModelResult alpha geoMap (ldSlope, ldIntercept) catMap

--    modelResult <- ModelResult <$> getVector "alpha"

type EduStateModelResult = ModelResult Text [DT.SexC, DT.Race5C, DT.Age4C]

--

designMatrixRowEdu :: forall rs a.(F.ElemOf rs DT.Age4C
                                  , F.ElemOf rs DT.SexC
                                  , F.ElemOf rs DT.RaceAlone4C
                                  , F.ElemOf rs DT.HispC
                                  )
                   => DM.DesignMatrixRow (F.Record rs)
designMatrixRowEdu = DM.DesignMatrixRow "DMEdu" [sexRP, ageRP, raceRP]
  where
    sexRP = DM.boundedEnumRowPart Nothing "Sex" (F.rgetField @DT.SexC )
    ageRP = DM.boundedEnumRowPart (Just DT.A4_25To44) "Age" (F.rgetField @DT.Age4C)
    race5Census r = DT.race5FromRaceAlone4AndHisp True (F.rgetField @DT.RaceAlone4C r) (F.rgetField @DT.HispC r)
    raceRP = DM.boundedEnumRowPart (Just DT.R5_WhiteNonHispanic) "Race" race5Census


designMatrixRowEdu3 :: forall rs a.(F.ElemOf rs DT.Age4C
                                  , F.ElemOf rs DT.SexC
                                  , F.ElemOf rs DT.RaceAlone4C
                                  , F.ElemOf rs DT.HispC
                                  )
                   => DM.DesignMatrixRow (F.Record rs)
designMatrixRowEdu3 = DM.DesignMatrixRow "DMEdu3" [sexRaceAgeRP]
  where
--    sexRP = DM.boundedEnumRowPart Nothing "Sex" (F.rgetField @DT.SexC . fst)
    race5Census r = DT.race5FromRaceAlone4AndHisp True (F.rgetField @DT.RaceAlone4C r) (F.rgetField @DT.HispC r)
    sexRaceAgeRP = DM.boundedEnumRowPart (Just $ DM.BEProduct3 (DT.Female, DT.R5_WhiteNonHispanic, DT.A4_25To44)) "SexRaceAge"
                $ \r -> DM.BEProduct3 (F.rgetField @DT.SexC r, race5Census r, F.rgetField @DT.Age4C r)

designMatrixRowEdu4 :: forall rs a.(F.ElemOf rs DT.Age4C
                                  , F.ElemOf rs DT.SexC
                                  , F.ElemOf rs DT.RaceAlone4C
                                  , F.ElemOf rs DT.HispC
                                  )
                   => DM.DesignMatrixRow (F.Record rs)
designMatrixRowEdu4 = DM.DesignMatrixRow "DMEdu4" [sexRaceAgeRP]
  where
    race5Census r = DT.race5FromRaceAlone4AndHisp True (F.rgetField @DT.RaceAlone4C r) (F.rgetField @DT.HispC r)
    sexRaceAgeRP = DM.boundedEnumRowPart Nothing "SexRaceAge"
                $ \r -> DM.BEProduct3 (F.rgetField @DT.SexC r, race5Census r, F.rgetField @DT.Age4C r)

designMatrixRowEdu8 :: forall rs a.(F.ElemOf rs DT.Age4C
                                  , F.ElemOf rs DT.SexC
                                  , F.ElemOf rs DT.RaceAlone4C
                                  , F.ElemOf rs DT.HispC
                                  )
                   => DM.DesignMatrixRow (F.Record rs)
designMatrixRowEdu8 = DM.DesignMatrixRow "DMEdu8" [sexRP, raceAgeRP]
  where
    sexRP = DM.boundedEnumRowPart Nothing "Sex" (F.rgetField @DT.SexC)
    race5Census r = DT.race5FromRaceAlone4AndHisp True (F.rgetField @DT.RaceAlone4C r) (F.rgetField @DT.HispC r)
    raceAgeRP = DM.boundedEnumRowPart (Just $ DM.BEProduct2 (DT.R5_WhiteNonHispanic, DT.A4_25To44)) "RaceAge"
                $ \r -> DM.BEProduct2 (race5Census r, F.rgetField @DT.Age4C r)

designMatrixRowEdu5 :: forall rs a.(F.ElemOf rs DT.Age4C
                                  , F.ElemOf rs DT.SexC
                                  , F.ElemOf rs DT.RaceAlone4C
                                  , F.ElemOf rs DT.HispC
                                  )
                   => DM.DesignMatrixRow (F.Record rs)
designMatrixRowEdu5 = DM.DesignMatrixRow "DMEdu5" [sexRP, ageRP, raceRP, sexRaceAgeRP]
  where
    race5Census r = DT.race5FromRaceAlone4AndHisp True (F.rgetField @DT.RaceAlone4C r) (F.rgetField @DT.HispC r)
    sexRP = DM.boundedEnumRowPart Nothing "Sex" (F.rgetField @DT.SexC)
    ageRP = DM.boundedEnumRowPart (Just  DT.A4_25To44) "Age" (F.rgetField @DT.Age4C)
    raceRP = DM.boundedEnumRowPart (Just DT.R5_WhiteNonHispanic) "Race" race5Census
    sexRaceAgeRP = DM.boundedEnumRowPart (Just $ DM.BEProduct3 (DT.Female, DT.R5_WhiteNonHispanic, DT.A4_25To44)) "SexRaceAge"
                $ \r -> DM.BEProduct3 (F.rgetField @DT.SexC r, race5Census r, F.rgetField @DT.Age4C r)

designMatrixRowEdu6 :: forall rs a.(F.ElemOf rs DT.Age4C
                                  , F.ElemOf rs DT.SexC
                                  , F.ElemOf rs DT.RaceAlone4C
                                  , F.ElemOf rs DT.HispC
                                  )
                   => DM.DesignMatrixRow (F.Record rs)
designMatrixRowEdu6 = DM.DesignMatrixRow "DMEdu6" [sexRP, ageRP, raceRP, sexRaceAgeRP]
  where
    race5Census r = DT.race5FromRaceAlone4AndHisp True (F.rgetField @DT.RaceAlone4C r) (F.rgetField @DT.HispC r)
    sexRP = DM.boundedEnumRowPart Nothing "Sex" (F.rgetField @DT.SexC)
    ageRP = DM.boundedEnumRowPart Nothing "Age" (F.rgetField @DT.Age4C)
    raceRP = DM.boundedEnumRowPart Nothing "Race" race5Census
    sexRaceAgeRP = DM.boundedEnumRowPart Nothing "SexRaceAge"
                $ \r -> DM.BEProduct3 (F.rgetField @DT.SexC r, race5Census r, F.rgetField @DT.Age4C r)

designMatrixRowAge :: forall rs a.(F.ElemOf rs DT.CollegeGradC
                                  , F.ElemOf rs DT.SexC
                                  , F.ElemOf rs DT.RaceAlone4C
                                  , F.ElemOf rs DT.HispC
                                  , F.ElemOf rs DT.PopPerSqMile
                                  )
                   => DM.DesignMatrixRowPart (F.Record rs)
                   -> DM.DesignMatrixRow (F.Record rs)
designMatrixRowAge densRP = DM.DesignMatrixRow "DMAge" [densRP, sexRP, eduRP, raceRP]
  where
    sexRP = DM.boundedEnumRowPart Nothing "Sex" (F.rgetField @DT.SexC)
    eduRP = DM.boundedEnumRowPart Nothing "Education" (collegeGrad)
    race5Census r = DT.race5FromRaceAlone4AndHisp True (F.rgetField @DT.RaceAlone4C r) (F.rgetField @DT.HispC r)
    raceRP = DM.boundedEnumRowPart (Just DT.R5_WhiteNonHispanic) "Race" race5Census



{-
setupACSRows :: (Typeable md, Typeable gq)
             => DM.DesignMatrixRowPart (F.Record ACSByCD)
             -> S.StanBuilderM md gq (S.RowTypeTag (F.Record ACSByCD)
                                     , TE.IntArrayE
                                     , TE.MatrixE)
setupACSRows densRP = do
  let dmRow = designMatrixRowACS densRP
  acsData <- S.dataSetTag @(F.Record ACSByCD) SC.ModelData "ACS"
  acsCit <- SB.addCountData acsData "ACS_CVAP" (F.rgetField @PUMS.Citizens)
  dmACS <- DM.addDesignMatrix acsData dmRow (Just "DM")
  return (acsData, acsCit, dmACS)
-}

type Categoricals = [DT.Age4C, DT.SexC, DT.CollegeGradC, DT.RaceAlone4C, DT.HispC]
type ACSByCD = PUMS.CDCounts Categoricals
type ACSByState = PUMS.StateCounts Categoricals

{-
acsByCD ∷ (K.KnitEffects r, BRK.CacheEffects r)
        ⇒ F.FrameRec PUMS.PUMS
        → F.FrameRec BRL.DatedCDFromPUMA2012
        → K.Sem r (F.FrameRec ACSByCD)
acsByCD acsByPUMA cdFromPUMA = fmap F.rcast <$> PUMS.pumsCDRollup (earliest earliestYear) (acsReKey . F.rcast) cdFromPUMA acsByPUMA
 where
  earliestYear = 2016
  earliest year = (>= year) . F.rgetField @BRDF.Year

cachedACSByCD
  ∷ ∀ r
   . (K.KnitEffects r, BRK.CacheEffects r)
  ⇒ K.ActionWithCacheTime r (F.FrameRec PUMS.PUMS)
  → K.ActionWithCacheTime r (F.FrameRec BRL.DatedCDFromPUMA2012)
  → K.Sem r (K.ActionWithCacheTime r (F.FrameRec ACSByCD))
cachedACSByCD acs_C cdFromPUMA_C = do
  let acsByCDDeps = (,) <$> acs_C <*> cdFromPUMA_C
  BRK.retrieveOrMakeFrame "model/age/acsByCD.bin" acsByCDDeps $
    \(acsByPUMA, cdFromPUMA) → acsByCD acsByPUMA cdFromPUMA
-}
acsByState ∷ F.FrameRec PUMS.PUMS → F.FrameRec ACSByState
acsByState acsByPUMA = F.rcast <$> FST.mapMaybe simplifyAge (FL.fold (PUMS.pumsStateRollupF (acsReKey . F.rcast)) filteredACSByPUMA)
 where
  earliestYear = 2016
  earliest year = (>= year) . F.rgetField @BRDF.Year
  filteredACSByPUMA = F.filterFrame (earliest earliestYear) acsByPUMA

simplifyAge :: F.ElemOf rs DT.Age5FC => F.Record rs -> Maybe (F.Record (DT.Age4C ': rs))
simplifyAge r =
  let f g = Just $ FT.recordSingleton @DT.Age4C g F.<+> r
  in case F.rgetField @DT.Age5FC r of
  DT.A5F_Under18 -> Nothing
  DT.A5F_18To24 -> f DT.A4_18To24
  DT.A5F_25To44 -> f DT.A4_25To44
  DT.A5F_45To64 -> f DT.A4_45To64
  DT.A5F_65AndOver -> f DT.A4_65AndOver

cachedACSByState
  ∷ ∀ r
   . (K.KnitEffects r, BRK.CacheEffects r)
  ⇒ K.ActionWithCacheTime r (F.FrameRec PUMS.PUMS)
  → K.Sem r (K.ActionWithCacheTime r (F.FrameRec ACSByState))
cachedACSByState acs_C = do
  BRK.retrieveOrMakeFrame "model/age/acsByState.bin" acs_C $
    pure . acsByState

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


type ACSByStateAgeMN = (F.Record ([BRDF.Year, BRDF.StateAbbreviation, BRDF.StateFIPS, DT.SexC, DT.CollegeGradC, DT.RaceAlone4C, DT.HispC] V.++ '[DT.PopPerSqMile]), VU.Vector Int)
type ACSByStateEduMNR = [BRDF.Year, BRDF.StateAbbreviation, BRDF.StateFIPS, DT.SexC, DT.Age4C, DT.RaceAlone4C, DT.HispC, DT.PopPerSqMile]
type ACSByStateEduMN = (F.Record ACSByStateEduMNR, VU.Vector Int)

densityF :: FL.Fold (F.Record [PUMS.Citizens, PUMS.NonCitizens, DT.PopPerSqMile]) (F.Record '[DT.PopPerSqMile])
densityF =
  let nPeople r = F.rgetField @PUMS.Citizens r + F.rgetField @PUMS.NonCitizens r
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
                  . FL.fold (forMultinomial @[BRDF.Year, BRDF.StateAbbreviation, BRDF.StateFIPS, DT.SexC, DT.CollegeGradC, DT.RaceAlone4C, DT.HispC]
                             (F.rgetField @DT.Age4C)
                             (F.rgetField @PUMS.Citizens)
                             densityF
                            )

acsByStateEduMN :: F.FrameRec ACSByState -> [ACSByStateEduMN]
acsByStateEduMN = filterZeroes
                  .  FL.fold (forMultinomial @[BRDF.Year, BRDF.StateAbbreviation, BRDF.StateFIPS,DT.SexC, DT.Age4C, DT.RaceAlone4C, DT.HispC]
                              (F.rgetField @DT.CollegeGradC)
                              (F.rgetField @PUMS.Citizens)
                              densityF
                             )

collegeGrad :: F.ElemOf rs DT.CollegeGradC => F.Record rs -> Bool
collegeGrad r = F.rgetField @DT.CollegeGradC r == DT.Grad

inCollege :: F.ElemOf rs DT.InCollege => F.Record rs -> Bool
inCollege = F.rgetField @DT.InCollege

districtKey :: (F.ElemOf rs BRDF.StateAbbreviation, F.ElemOf rs BRDF.CongressionalDistrict) => F.Record rs -> Text
districtKey r = F.rgetField @BRDF.StateAbbreviation r <> "-" <> show (F.rgetField @BRDF.CongressionalDistrict r)

logDensityPredictor :: F.ElemOf rs DT.PopPerSqMile => F.Record rs -> VU.Vector Double
logDensityPredictor = safeLogV . F.rgetField @DT.PopPerSqMile

safeLog x =  if x < 1e-12 then 0 else Numeric.log x -- won't matter because Pop will be 0 here
safeLogV x =  VU.singleton $ safeLog x

logDensityDMRP :: F.ElemOf rs DT.PopPerSqMile => DM.DesignMatrixRowPart (F.Record rs)
logDensityDMRP = DM.DesignMatrixRowPart "Density" 1 logDensityPredictor


----

categoricalModel :: forall rs.Typeable rs
                 => Int
                 -> DM.DesignMatrixRow (F.Record rs)
                 -> S.StanBuilderM [(F.Record rs, VU.Vector Int)] () ()
categoricalModel numInCat dmr = do
  acsData <- S.dataSetTag @(F.Record rs, VU.Vector Int) SC.ModelData "ACS"
  let nDataE = S.dataSetSizeE acsData
  nInCatE <- SB.addFixedInt "K" numInCat
  countsE <- SB.addIntArrayData acsData "counts" nInCatE (Just 0) Nothing snd
  acsMatE <- DM.addDesignMatrix acsData (contramap fst dmr) Nothing
  let (_, nPredictorsE) = DM.designMatrixColDimBinding dmr Nothing
  -- parameters
  -- zero vector for identifiability trick
  zvP <- DAG.addBuildParameter
         $ DAG.TransformedDataP
         $ DAG.TData
         (TE.NamedDeclSpec "zeroes" $ TE.vectorSpec nPredictorsE [])
         []
         TNil
         (const $ DAG.DeclRHS $ TE.functionE SF.rep_vector (TE.realE 0 :> nPredictorsE :> TNil))

  betaRawP <- DAG.addBuildParameter
              $ DAG.UntransformedP
              (TE.NamedDeclSpec "beta_raw" $ TE.matrixSpec nPredictorsE (nInCatE `TE.minusE` TE.intE 1) [])
              []
              TNil
              (\_ _ -> pure ())

  betaP <- DAG.addBuildParameter
           $ DAG.TransformedP
           (TE.NamedDeclSpec "beta" $ TE.matrixSpec nPredictorsE nInCatE [])
           []
           (DAG.build betaRawP :> DAG.build zvP :> TNil)
           (\ps -> DAG.DeclRHS $ TE.functionE SF.append_col ps)
           (DAG.given (TE.realE 0) :> DAG.given (TE.realE 2) :> TNil)
           (\normalPS x -> TE.addStmt $ TE.sample (TE.functionE SF.to_vector (x :> TNil)) SF.normalS normalPS)

  let betaE = DAG.parameterTagExpr betaP
      betaXD = TE.declareRHSNW
               (TE.NamedDeclSpec "beta_x" $ TE.matrixSpec nDataE nInCatE [])
               (acsMatE `TE.timesE` betaE)
      at x n = TE.sliceE TEI.s0 n x

  S.inBlock S.SBModel $ S.addStmtsToCode $ TE.writerL' $ do
    let sizeE e = TE.functionE SF.size (e :> TNil)
    betaX <- betaXD
    TE.addStmt $ TE.for "n" (TE.SpecificNumbered (TE.intE 1) nDataE) $ \n ->
      [TE.target $ TE.densityE SF.multinomial_logit_lupmf (countsE `at` n) (TE.transposeE (betaX `at` n) :> TNil)]

  gqBetaX <- S.inBlock S.SBLogLikelihood $ S.addFromCodeWriter betaXD
  SB.generateLogLikelihood
    acsData
    SD.multinomialLogitDist
    (pure $ \nE -> TE.transposeE (gqBetaX `at` nE) :> TNil)
    (pure $ \nE -> countsE `at` nE)


normalModel :: forall rs.Typeable rs
            => DM.DesignMatrixRow (F.Record rs)
            -> S.StanBuilderM [(F.Record rs, VU.Vector Int)] () ()
normalModel dmr = do
  acsData <- S.dataSetTag @(F.Record rs, VU.Vector Int) SC.ModelData "ACS"
  let nDataE = S.dataSetSizeE acsData
      nStatesE = S.groupSizeE stateGroup
--  countsE <- SB.addIntArrayData acsData "counts" (TE.intE 2) (Just 0) Nothing snd
  let trials v = v VU.! 0 + v VU.! 1
      successes v = v VU.! 1
  trialsE <- SB.addCountData acsData "trials" (trials . snd)
  successesE <- SB.addCountData acsData "successes" (successes . snd)
  acsMatE <- DM.addDesignMatrix acsData (contramap fst dmr) Nothing
  let (_, nPredictorsE) = DM.designMatrixColDimBinding dmr Nothing
      at x n = TE.sliceE TEI.s0 n x

  -- parameters
  muAlpha <- DAG.simpleParameterWA
             (TE.NamedDeclSpec "muAlpha" $ TE.realSpec [])
             (TE.DensityWithArgs SF.normalS (TE.realE 0 :> TE.realE 1 :> TNil))

  sigmaAlpha <- DAG.simpleParameterWA
             (TE.NamedDeclSpec "sigmaAlpha" $ TE.realSpec [TE.lowerM $ TE.realE 0])
             (TE.DensityWithArgs SF.normalS (TE.realE 0 :> TE.realE 1 :> TNil))

  alphaP <- DAG.addCenteredHierarchical
            (TE.NamedDeclSpec "alpha" $ TE.vectorSpec nStatesE [])
            (DAG.tagsAsParams (muAlpha :> sigmaAlpha :> TNil))
            SF.normalS

  betaP <- DAG.simpleParameterWA
         (TE.NamedDeclSpec "beta" $ TE.vectorSpec nPredictorsE [])
         (TE.DensityWithArgs SF.normalS (TE.realE 0 :> TE.realE 5 :> TNil))

  sigma0P <- DAG.simpleParameterWA
         (TE.NamedDeclSpec "sigma0" $ TE.realSpec [TE.lowerM $ TE.realE 0])
         (TE.DensityWithArgs SF.normalS (TE.realE 0 :> TE.realE 50 :> TNil))

  sigmaP <- DAG.simpleParameterWA
         (TE.NamedDeclSpec "sigma" $ TE.vectorSpec nPredictorsE [TE.lowerM $ TE.realE 0])
         (TE.DensityWithArgs SF.normalS (TE.realE 0 :> TE.realE 50 :> TNil))


  let alphaE = DAG.parameterTagExpr alphaP
      betaE = DAG.parameterTagExpr betaP
      sigma0E = DAG.parameterTagExpr sigma0P
      sigmaE = DAG.parameterTagExpr sigmaP
      eltTimes = TE.binaryOpE (TEO.SElementWise TEO.SMultiply)
      observed = TE.functionE SF.to_vector (successesE :> TNil)
      mu = TE.indexE TEI.s0 (S.byGroupIndexE acsData stateGroup) alphaE `TE.plusE` (acsMatE `TE.timesE` betaE)
      expected = TE.functionE SF.to_vector (trialsE :> TNil) `eltTimes` TE.functionE SF.inv_logit (mu :> TNil)
      sigma = TE.functionE SF.sqrt (TE.functionE SF.to_vector (trialsE :> TNil) `eltTimes` (sigma0E `TE.plusE` (acsMatE `TE.timesE` sigmaE)) :> TNil)
      ps = expected :> sigma :> TNil

  S.inBlock S.SBModel $ S.addFromCodeWriter $ TE.addStmt $ TE.sample observed SF.normal ps

  let vSpec = TE.vectorSpec nDataE []
      tempPs = do
        e <- TE.declareRHSNW (TE.NamedDeclSpec "expectedV" vSpec) expected
        s <- TE.declareRHSNW (TE.NamedDeclSpec "sigmaV" vSpec) sigma
        return (e, s)
      tempObs = TE.declareRHSNW (TE.NamedDeclSpec "observedV" vSpec) observed


--  (observedLL, expectedLL, sigmaLL) <- S.inBlock S.SBLogLikelihood $ S.addFromCodeWriter tempVars

  SB.generateLogLikelihood
    acsData
    SD.normalDist
    ((\(exp, sig) n -> exp `at` n :> sig `at` n :> TNil) <$> tempPs)
    ((\o n ->  o `at` n) <$> tempObs)

  _ <- SB.generatePosteriorPrediction
    acsData
    (TE.NamedDeclSpec "pObserved" $ TE.array1Spec nDataE $ TE.realSpec [])
    SD.normalDist
    ((\(exp, sig) n -> exp `at` n :> sig `at` n :> TNil) <$> tempPs)
  pure ()


binomialNormalModel :: forall rs.Typeable rs
                 => DM.DesignMatrixRow (F.Record rs)
                 -> S.StanBuilderM [(F.Record rs, VU.Vector Int)] () ()
binomialNormalModel dmr = do
  acsData <- S.dataSetTag @(F.Record rs, VU.Vector Int) SC.ModelData "ACS"
  let nData = S.dataSetSizeE acsData
      nStates = S.groupSizeE stateGroup
--  countsE <- SB.addIntArrayData acsData "counts" (TE.intE 2) (Just 0) Nothing snd
  let trials v = v VU.! 0 + v VU.! 1
      successes v = v VU.! 1
  trials <- SB.addCountData acsData "trials" (trials . snd)
  successes <- SB.addCountData acsData "successes" (successes . snd)
  acsMat <- DM.addDesignMatrix acsData (contramap fst dmr) Nothing
  let (_, nPredictors) = DM.designMatrixColDimBinding dmr Nothing
      at x n = TE.sliceE TEI.s0 n x
  -- parameters
  sigmaAlphaP <- DAG.simpleParameterWA
             (TE.NamedDeclSpec "sigmaAlpha" $ TE.realSpec [TE.lowerM $ TE.realE 0])
             (TE.DensityWithArgs SF.normalS (TE.realE 0 :> TE.realE 1 :> TNil))

  alphaP <- DAG.addCenteredHierarchical
            (TE.NamedDeclSpec "alpha" $ TE.vectorSpec nStates [])
            (DAG.given (TE.realE 0) :> DAG.build sigmaAlphaP :> TNil)
            SF.normalS

  betaP <- DAG.simpleParameterWA
           (TE.NamedDeclSpec "beta" $ TE.vectorSpec nPredictors [])
           (TE.DensityWithArgs SF.normalS (TE.realE 0 :> TE.realE 2 :> TNil))

  muErrP <- DAG.simpleParameterWA
            (TE.NamedDeclSpec "muErr" $ TE.realSpec [])
            (TE.DensityWithArgs SF.normalS (TE.realE 0 :> TE.realE 1 :> TNil))

  sigmaErrP <- DAG.simpleParameterWA
               (TE.NamedDeclSpec "sigmaErr" $ TE.realSpec [TE.lowerM $ TE.realE 0])
               (TE.DensityWithArgs SF.normalS (TE.realE 0 :> TE.realE 1 :> TNil))

  errP <- DAG.addCenteredHierarchical
          (TE.NamedDeclSpec "err" $ TE.vectorSpec nData [])
          (DAG.build muErrP :> DAG.build sigmaErrP :> TNil)
          SF.normalS

  let alpha = DAG.parameterTagExpr alphaP
      beta  = DAG.parameterTagExpr betaP
      err = DAG.parameterTagExpr errP
      p =  TE.indexE TEI.s0 (S.byGroupIndexE acsData stateGroup) alpha `TE.plusE` (acsMat `TE.timesE` beta) `TE.plusE` err
      vSpec = TE.vectorSpec nData []
      tmpP = TE.declareRHSNW (TE.NamedDeclSpec "pV" vSpec) p

  S.inBlock S.SBModel $ S.addFromCodeWriter $ do
    p <- tmpP
    TE.addStmt $ TE.for "n" (TE.SpecificNumbered (TE.intE 1) nData) $ \n ->
      let lhs = successes `at` n
          ps = trials `at` n :> p `at` n :> TNil
      in [TE.target $ TE.densityE SF.binomial_logit_lpmf lhs ps]

  SB.generateLogLikelihood
    acsData
    SD.binomialLogitDist
    ((\p n -> trials `at` n :> p `at` n :> TNil) <$> tmpP)
    (pure (successes `at`))

  _ <- SB.generatePosteriorPrediction
    acsData
    (TE.NamedDeclSpec "pObserved" $ TE.array1Spec nData $ TE.intSpec [])
    SD.binomialLogitDist
    ((\p n -> trials `at` n :> p `at` n :> TNil) <$> tmpP)
  pure ()

binomialModel :: forall rs.Typeable rs
                 => DM.DesignMatrixRow (F.Record rs)
                 -> S.StanBuilderM [(F.Record rs, VU.Vector Int)] () ()
binomialModel dmr = do
  acsData <- S.dataSetTag @(F.Record rs, VU.Vector Int) SC.ModelData "ACS"
  let nData = S.dataSetSizeE acsData
      nStates = S.groupSizeE stateGroup
      trialsF v = v VU.! 0 + v VU.! 1
      successesF v = v VU.! 1
  trials <- SB.addCountData acsData "trials" (trialsF . snd)
  successes <- SB.addCountData acsData "successes" (successesF . snd)
  acsMat <- DM.addDesignMatrix acsData (contramap fst dmr) Nothing
  let (_, nPredictors) = DM.designMatrixColDimBinding dmr Nothing
      at x n = TE.sliceE TEI.s0 n x
      by v i = TE.indexE TEI.s0 i v

  -- parameters

  betaP <- DAG.simpleParameterWA
           (TE.NamedDeclSpec "beta" $ TE.vectorSpec nPredictors [])
           (TE.DensityWithArgs SF.normalS (TE.realE 0 :> TE.realE 2 :> TNil))

  sigmaAlphaP <- DAG.simpleParameterWA
             (TE.NamedDeclSpec "sigmaAlpha" $ TE.realSpec [TE.lowerM $ TE.realE 0])
             (TE.DensityWithArgs SF.normalS (TE.realE 0 :> TE.realE 1 :> TNil))

  alphaP <- DAG.addCenteredHierarchical
            (TE.NamedDeclSpec "alpha" $ TE.vectorSpec nStates [])
            (DAG.given (TE.realE 0) :> DAG.build sigmaAlphaP :> TNil)
            SF.normalS

  let beta = DAG.parameterTagExpr betaP
      alpha = DAG.parameterTagExpr alphaP
      logitMu = (alpha `by` (S.byGroupIndexE acsData stateGroup)) `TE.plusE` (acsMat `TE.timesE` beta)
      vSpec = TE.vectorSpec nData []
      tempLM = TE.declareRHSNW (TE.NamedDeclSpec "lmV" vSpec) logitMu

  S.inBlock S.SBModel $ S.addFromCodeWriter $ do
    TE.addStmt $ TE.target $ TE.densityE SF.binomial_logit_lpmf successes (trials :> logitMu :> TNil)

  SB.generateLogLikelihood
    acsData
    SD.binomialLogitDist
    ((\lm n -> (trials `at` n :> lm `at` n :> TNil)) <$> tempLM)
    (pure $ \n -> successes `at` n)

  S.inBlock S.SBGeneratedQuantities $ DM.splitToGroupVars dmr beta (Just "beta")
  _ <- SB.generatePosteriorPrediction
    acsData
    (TE.NamedDeclSpec "pObserved" $ TE.array1Spec nData $ TE.intSpec [])
    (SD.binomialLogitDist' True)
    ((\lm n -> trials `at` n :> lm `at` n :> TNil) <$> tempLM)
  pure ()



{-
negBinomialModel :: forall rs.Typeable rs
                 => DM.DesignMatrixRow (F.Record rs)
                 -> S.StanBuilderM [(F.Record rs, VU.Vector Int)] () ()
negBinomialModel dmr = do
  acsData <- S.dataSetTag @(F.Record rs, VU.Vector Int) SC.ModelData "ACS"
  let nData = S.dataSetSizeE acsData
      nStates = S.groupSizeE stateGroup
      trials v = v VU.! 0 + v VU.! 1
      successes v = v VU.! 1
  trials <- SB.addCountData acsData "trials" (trials . snd)
  successes <- SB.addCountData acsData "successes" (successes . snd)
  acsMat <- DM.addDesignMatrix acsData (contramap fst dmr) Nothing
  let (_, nPredictors) = DM.designMatrixColDimBinding dmr Nothing
      at x n = TE.sliceE TEI.s0 n x
      by v d g = TE,indexE TEI.s0 (S.byGroupIndexE d g) v
      vSpec = TE.vectorSpec nData []
  -- transformed data
  realSuccesses <- S.inBlock S.SBTransformedData $ S.addFromCodeWriter
                   $ TE.declareRHSNW (TE.NamedDeclSpec "rSuccesses" vSpec)
                   $ TE.functionE SF.to_vector (successes :> TNil)

  -- parameters
  sigmaAlphaP <- DAG.simpleParameterWA
             (TE.NamedDeclSpec "sigmaAlpha" $ TE.realSpec [TE.lowerM $ TE.realE 0])
             (TE.DensityWithArgs SF.normalS (TE.realE 0 :> TE.realE 1 :> TNil))

  alphaP <- DAG.addCenteredHierarchical
            (TE.NamedDeclSpec "alpha" $ TE.vectorSpec nStates [])
            (DAG.given (TE.realE 0) :> DAG.build sigmaAlphaP :> TNil)
            SF.normalS

  betaP <- DAG.simpleParameterWA
           (TE.NamedDeclSpec "beta" $ TE.vectorSpec nPredictors [])
           (TE.DensityWithArgs SF.normalS (TE.realE 0 :> TE.realE 2 :> TNil))

  phiP <- DAG.simpleParameterWA
           (TE.NamedDeclSpec "phi" $ TE.vectorSpec nPredictors [])
           (TE.DensityWithArgs SF.normalS (TE.realE 0 :> TE.realE 1 :> TNil))

  let alpha = DAG.parameterTagExpr alphaP
      beta  = DAG.parameterTagExpr betaP
      phi = DAG.parameterTagExpr phiP
      p =  (alpha `by` acsData stateGroup) `TE.plusE` (acsMat `TE.timesE` beta)
      tmpMu = TE.declareRHSNW (TE.NamedDeclSpec "muV" vSpec) $ p `eltTimes` realSuccesses

  S.inBlock S.SBModel $ S.addFromCodeWriter $ do
    mu <- tmpMu
    TE.addStmt $ TE.target $ TE.densityE SF.neg_binomial_2 trials (mu :> phi :> TNil)

  SB.generateLogLikelihood
    acsData
    SD.binomialLogitDist
    ((\p n -> trials `at` n :> p `at` n :> TNil) <$> tmpP)
    (pure (successes `at`))

  _ <- SB.generatePosteriorPrediction
    acsData
    (TE.NamedDeclSpec "pObserved" $ TE.array1Spec nData $ TE.intSpec [])
    SD.binomialLogitDist
    ((\p n -> trials `at` n :> p `at` n :> TNil) <$> tmpP)
  pure ()
-}

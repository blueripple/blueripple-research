{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE UnicodeSyntax #-}

module BlueRipple.Model.Demographic.StanModels
  (
    module BlueRipple.Model.Demographic.StanModels
  )
where

import qualified BlueRipple.Model.Demographic.DataPrep as DDP
import qualified BlueRipple.Configuration as BR
import qualified BlueRipple.Data.DataFrames as BRDF
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.GeographicTypes as GT
import qualified BlueRipple.Data.Keyed as BRK
import qualified BlueRipple.Utilities.KnitUtils as BRKU

import qualified Stan.ModelBuilder as S
import qualified Stan.ModelBuilder.DesignMatrix as DM
import qualified Stan.ModelBuilder.BuildingBlocks as SB
import qualified Stan.ModelConfig as SC
import qualified Stan.RScriptBuilder as SR
import qualified Stan.ModelRunner as SMR
import qualified Stan.ModelBuilder.TypedExpressions.Statements as TE
import qualified Stan.ModelBuilder.TypedExpressions.Operations as TEO
import qualified Stan.ModelBuilder.TypedExpressions.Indexing as TEI
import qualified Stan.ModelBuilder.TypedExpressions.StanFunctions as SF
import qualified Stan.ModelBuilder.Distributions as SD
import qualified Stan.ModelBuilder.TypedExpressions.DAG as DAG
import Stan.ModelBuilder.TypedExpressions.TypedList (TypedList(..))

--import qualified Frames.Streamly.InCore as FS
import qualified Frames.Serialize as FS
import qualified Flat
import Control.Lens (view, (^.))
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
import qualified CmdStan as CS
import qualified Stan.Parameters as SP
import qualified Data.IntMap.Strict as IM

logLengthC :: (K.KnitEffects r, Foldable f) => K.ActionWithCacheTime r (f a) -> Text -> K.Sem r ()
logLengthC xC t = K.ignoreCacheTime xC >>= \x -> K.logLE K.Info $ t <> " has " <> show (FL.fold FL.length x) <> " rows."

runModel :: forall ks l r .
            (K.KnitEffects r, BRKU.CacheEffects r
            , Ord (F.Record ks)
            , Enum l, Bounded l, Ord l
            , Typeable (ks V.++ '[DT.PWPopPerSqMile])
            , F.ElemOf  (ks V.++ '[DT.PWPopPerSqMile]) DT.PWPopPerSqMile
            , ks F.⊆ ([BRDF.Year, GT.StateAbbreviation, GT.StateFIPS] V.++ (ks V.++ '[DT.PWPopPerSqMile]))
            , V.RMap ks
            , FS.RecFlat ks
            , Ord (F.Rec FS.SElField ks)
            , BRK.FiniteSet (F.Record ks)
            )
         => Bool
         -> BR.CommandLine
         -> ModelConfig ()
         -> (Text, F.Record DDP.ACSByStateR -> l)
         -> (Text, F.Record DDP.ACSByStateR -> F.Record ks, DM.DesignMatrixRow (F.Record ks))
         -> K.Sem r (K.ActionWithCacheTime r (ModelResult Text ks))
runModel clearCaches cmdLine mc (modeledT, modeledK) (fromT, cKey, dmr) = do
  let cacheDirE = let k = ("model/demographic/" <> modeledT <> "/") in if clearCaches then Left k else Right k
      dataName = "acs" <> modeledT <> "_" <> DM.dmName dmr <> modelConfigSuffix mc
      runnerInputNames = SC.RunnerInputNames
                         ("br-2022-Demographics/stan" <> modeledT)
                         ("normal" <> fromT <> "_" <> DM.dmName dmr <> modelConfigSuffix mc)
                         (Just $ SC.GQNames "pp" dataName)
                         dataName
      _postInfo = BR.PostInfo (BR.postStage cmdLine) (BR.PubTimes BR.Unpublished Nothing)
--  _ageModelPaths <- postPaths ("Model" cmdLine
  acs_C <- DDP.cachedACSByState'
--  K.ignoreCacheTime acs_C >>= BRK.logFrame
  logLengthC acs_C "acsByState"
  let acsMN_C = fmap (DDP.acsByStateMN cKey modeledK) acs_C
      mcWithId = "normal" <$ mc
--  K.ignoreCacheTime acsMN_C >>= print
  logLengthC acsMN_C ("acsByState Counted for " <> modeledT)
  states <- FL.fold (FL.premap (view GT.stateAbbreviation . fst) FL.set) <$> K.ignoreCacheTime acsMN_C
  (dw, code) <- SMR.dataWranglerAndCode acsMN_C (pure ())
                (groupBuilderState (S.toList states))
                (normalModel (contramap F.rcast dmr) mc)
  res_C <-SMR.runModel' @BRKU.SerializerC @BRKU.CacheData
          cacheDirE
          (Right runnerInputNames)
          dw
          code
          (stateModelResultAction mcWithId dmr)
          (SMR.Both [SR.UnwrapNamed "successes" "yObserved"])
          acsMN_C
          (pure ())
  K.logLE K.Info "citizenModel run complete."
  pure res_C

data HierarchicalType = HCentered | HNonCentered deriving stock (Show, Eq, Ord)

data ModelConfig a = ModelConfig { modelID :: a
                                 , includeAlpha0 :: Bool
                                 , alphaType :: HierarchicalType
                                 , includeDensity :: Bool
                                 } deriving stock (Functor, Show)

modelConfigSuffix :: ModelConfig a -> Text
modelConfigSuffix (ModelConfig _ ia at id') = a0s <> ats <> ids
  where
    a0s = if ia then "_a0" else ""
    ats = if at == HCentered then "_ac" else "_anc"
    ids = if id' then "_d" else ""

modelName :: ModelConfig Text -> Text
modelName mc = modelID mc <> modelConfigSuffix mc

addTermMaybe :: Maybe a -> (a -> TE.UExpr t -> TE.UExpr t) -> TE.UExpr t -> TE.UExpr t
addTermMaybe mA combine e = case mA of
  Nothing -> e
  Just a -> combine a e

data ModelData rs = ModelData { acsDataTag :: S.RowTypeTag (F.Record rs, VU.Vector Int)
                              , nData :: TE.IntE
                              , nStates :: TE.IntE
                              , nPredictors :: TE.IntE
                              , trials :: TE.IntArrayE
                              , successes :: TE.IntArrayE
                              , predictors :: TE.MatrixE
                              , mDensity :: Maybe TE.VectorE
                                                }

data BasicParameters = BasicParameters { mAlpha0 :: Maybe TE.RealE
                                       , alpha :: TE.VectorE
                                       , beta :: TE.VectorE
                                       , logitMu :: TE.VectorE
--                                       , mBetaDensity :: Maybe TE.RealE
                                       }

modelData :: forall rs . (Typeable rs, F.ElemOf rs DT.PWPopPerSqMile)
          => DM.DesignMatrixRow (F.Record rs)
          -> ModelConfig ()
          -> S.StanBuilderM [(F.Record rs, VU.Vector Int)] () (ModelData rs)
modelData dmr mc = do
  acsData <- S.dataSetTag @(F.Record rs, VU.Vector Int) SC.ModelData "ACS"
  let nData' = S.dataSetSizeE acsData
      nStates' = S.groupSizeE stateGroup
  let trialsF v = v VU.! 0 + v VU.! 1
      successesF v = v VU.! 1
  trials' <- SB.addCountData acsData "trials" (trialsF . snd)
  successes' <- SB.addCountData acsData "successes" (successesF . snd)

  acsMat' <- DM.addDesignMatrix acsData (contramap fst dmr) Nothing
  let (_, nPredictors') = DM.designMatrixColDimBinding dmr Nothing
  mDensity' <- case includeDensity mc of
    False -> pure Nothing
    True -> do
      rawDensity <- SB.addRealData acsData "rawLogDensity" Nothing Nothing (DDP.safeLog . F.rgetField @DT.PWPopPerSqMile . fst)
      stdDensity <- S.inBlock S.SBTransformedData $ S.addFromCodeWriter $ do
        let m = TE.functionE SF.mean (rawDensity :> TNil)
            sd = TE.functionE SF.sqrt (TE.functionE SF.variance (rawDensity :> TNil) :> TNil)
        TE.declareRHSNW (TE.NamedDeclSpec "stdLogDensity" $ TE.vectorSpec nData' [])
          $ (rawDensity `TE.minusE` m) `TE.divideE` sd
      pure $ Just stdDensity

  pure $ ModelData acsData nData' nStates' nPredictors' trials' successes' acsMat' mDensity'

basicParameters :: ModelConfig () -> ModelData rs -> S.StanBuilderM [(F.Record rs, VU.Vector Int)] () BasicParameters
basicParameters mc md = do
  mAlpha0P <- case mc.includeAlpha0  of
    True -> Just
            <$> DAG.simpleParameterWA
            (TE.NamedDeclSpec "alpha0" $ TE.realSpec [])
            (TE.DensityWithArgs SF.normalS (TE.realE 0 :> TE.realE 1 :> TNil))
    False -> pure Nothing

  sigmaAlphaP <- DAG.simpleParameterWA
             (TE.NamedDeclSpec "sigmaAlpha" $ TE.realSpec [TE.lowerM $ TE.realE 0])
             (TE.DensityWithArgs SF.normalS (TE.realE 0 :> TE.realE 1 :> TNil))

  alphaP <- case mc.alphaType of
    HCentered -> DAG.addCenteredHierarchical
                 (TE.NamedDeclSpec "alpha" $ TE.vectorSpec md.nStates [])
                 (DAG.given (TE.realE 0) :> DAG.build sigmaAlphaP :> TNil)
                 SF.normalS
    HNonCentered -> DAG.simpleNonCentered
                    (TE.NamedDeclSpec "alpha" $ TE.vectorSpec md.nStates [])
                    (TE.vectorSpec md.nStates [])
                    (TE.DensityWithArgs SF.normalS $ TE.realE 0 :> TE.realE 1 :> TNil)
                    (DAG.given (TE.realE 0) :> DAG.build sigmaAlphaP :> TNil)
                    (\(ma :> sa :> TNil) r -> ma `TE.plusE` (sa `TE.timesE` r))

  betaP <- DAG.simpleParameterWA
         (TE.NamedDeclSpec "beta" $ TE.vectorSpec md.nPredictors [])
         (TE.DensityWithArgs SF.normalS (TE.realE 0 :> TE.realE 2 :> TNil))

  mBetaDensityP <- case includeDensity mc of
    True -> Just <$> DAG.simpleParameterWA
                   (TE.NamedDeclSpec "beta_Density" $ TE.realSpec [])
                   (TE.DensityWithArgs SF.normalS (TE.realE 0 :> TE.realE 1 :> TNil))
    False -> pure Nothing

  let f = DAG.parameterTagExpr
      by v i = TE.indexE TEI.s0 i v
      mBetaDensityTerm = TE.timesE <$> (f <$> mBetaDensityP) <*> md.mDensity
      logitMu' = let x = (f alphaP `by` (S.byGroupIndexE md.acsDataTag stateGroup))
                       `TE.plusE` (md.predictors `TE.timesE` f betaP)
                 in addTermMaybe (f <$> mAlpha0P) (\a e -> a `TE.plusE` e)
                    $ addTermMaybe mBetaDensityTerm (\bd e -> bd `TE.plusE` e) x
  pure $ BasicParameters (f <$> mAlpha0P) (f alphaP) (f betaP) logitMu'


normalModel :: forall rs . (Typeable rs, F.ElemOf rs DT.PWPopPerSqMile)
            => DM.DesignMatrixRow (F.Record rs)
            -> ModelConfig ()
            -> S.StanBuilderM [(F.Record rs, VU.Vector Int)] () ()
normalModel dmr mc = do
  -- data
  md <- modelData dmr mc

  -- transformed data
  let toVec a = TE.functionE SF.to_vector (a :> TNil)
      eltTimes = TE.binaryOpE (TEO.SElementWise TEO.SMultiply)
      eltDivide = TE.binaryOpE (TEO.SElementWise TEO.SDivide)

  (obsP, binomialSigma2) <- S.inBlock S.SBTransformedData $ S.addFromCodeWriter $ do
    oP <- TE.declareRHSNW (TE.NamedDeclSpec "obsP" $ TE.vectorSpec md.nData [])
          $ toVec md.successes `eltDivide` toVec md.trials
    bS <- TE.declareRHSNW (TE.NamedDeclSpec "binomialSigma" $ TE.vectorSpec md.nData [])
          $ oP `eltTimes` (TE.realE 1 `TE.minusE` oP) `eltDivide` toVec md.trials
    pure (oP, bS)

  -- parameters & priors
  bParams <- basicParameters mc md

  sigmaP <- DAG.simpleParameterWA
         (TE.NamedDeclSpec "sigma" $ TE.realSpec [TE.lowerM $ TE.realE 0])
         (TE.DensityWithArgs SF.normalS (TE.realE 0 :> TE.realE 1 :> TNil))

  let sigma0 = DAG.parameterTagExpr sigmaP
      mu = TE.functionE SF.inv_logit (bParams.logitMu :> TNil)
      sigma = TE.functionE SF.sqrt (binomialSigma2 `TE.plusE` (sigma0 `TE.timesE` sigma0) :> TNil)
      ps = mu :> sigma :> TNil

  -- model
  S.inBlock S.SBModel $ S.addFromCodeWriter $ TE.addStmt $ TE.sample obsP SF.normal ps

  -- generated quantities
  let vSpec = TE.vectorSpec md.nData []
      tempPs = do
        mu' <- TE.declareRHSNW (TE.NamedDeclSpec "muV" vSpec) mu
        s <- TE.declareRHSNW (TE.NamedDeclSpec "sigmaV" vSpec) sigma
        return (mu', s)
      tempP = TE.declareRHSNW (TE.NamedDeclSpec "pV" vSpec) obsP

  let at x n = TE.sliceE TEI.s0 n x
  SB.generateLogLikelihood
    md.acsDataTag
    SD.normalDist
    ((\(e, sig) n -> e `at` n :> sig `at` n :> TNil) <$> tempPs)
    ((\o n ->  o `at` n) <$> tempP)

  _ <- S.inBlock S.SBGeneratedQuantities $ DM.splitToGroupVars dmr bParams.beta (Just "beta")
  _ <- SB.generatePosteriorPrediction'
    md.acsDataTag
    (TE.NamedDeclSpec "pObserved" $ TE.array1Spec md.nData $ TE.realSpec [])
    SD.normalDist
    ((\(e, sig) n -> e `at` n :> sig `at` n :> TNil) <$> tempPs)
    (\n p -> md.trials `at` n `TE.timesE` p)
  pure ()


betaBinomialModel :: forall rs. (Typeable rs, F.ElemOf rs DT.PWPopPerSqMile)
            => DM.DesignMatrixRow (F.Record rs)
            -> ModelConfig ()
            -> S.StanBuilderM [(F.Record rs, VU.Vector Int)] () ()
betaBinomialModel dmr mc = do
  md <- modelData dmr mc
  absPredictors <- S.inBlock S.SBTransformedData $ S.addFromCodeWriter
                   $ TE.declareRHSNW (TE.NamedDeclSpec "absACSMat" $ TE.matrixSpec md.nData md.nPredictors [])
                   $ TE.functionE SF.abs (md.predictors :> TNil)
  -- parameters
  bParams <- basicParameters mc md
  let at x n = TE.sliceE TEI.s0 n x
--      by v i = TE.indexE TEI.s0 i v
      eltTimes = TE.binaryOpE (TEO.SElementWise TEO.SMultiply)
      eltDivide = TE.binaryOpE (TEO.SElementWise TEO.SDivide)
  phiP <- DAG.addTransformedHP
          (TE.NamedDeclSpec "phi" $ TE.vectorSpec md.nPredictors [])
          (Just $ [TE.lowerM $ TE.realE 0, TE.upperM $ TE.realE 1]) -- constraints on phi_raw
          (TE.DensityWithArgs SF.betaS (TE.realE 99 :> TE.realE 1 :> TNil)) -- phi_raw is beta distributed
          (\t -> t `eltDivide` (TE.realE 1 `TE.minusE` t)) -- phi = phi_raw / (1 - phi_raw), component-wise

  let phi = DAG.parameterTagExpr phiP
      vSpec = TE.vectorSpec md.nData []
      tempPs = do
        mu <- TE.declareRHSNW (TE.NamedDeclSpec "muV" vSpec) $ TE.functionE SF.inv_logit (bParams.logitMu :> TNil)
        phiV <- TE.declareRHSNW (TE.NamedDeclSpec "mV" vSpec) $  absPredictors `TE.timesE` phi
        betaA <- TE.declareRHSNW (TE.NamedDeclSpec "aV" vSpec) $ phiV `eltTimes` mu
        betaB <-TE.declareRHSNW (TE.NamedDeclSpec "bV" vSpec) $ phiV `eltTimes` (TE.realE 1 `TE.minusE` mu)
        pure (betaA, betaB)

  S.inBlock S.SBModel $ S.addFromCodeWriter $ do
    (betaA, betaB) <- tempPs
    let ps = md.trials :> betaA :> betaB :> TNil
    TE.addStmt $ TE.target $ TE.densityE SF.beta_binomial_lpmf md.successes ps

  SB.generateLogLikelihood
    md.acsDataTag
    (S.betaBinomialDist' True)
    ((\(a, b) n -> md.trials `at` n :> a `at` n :> b `at` n :> TNil) <$> tempPs)
    (pure $ (md.successes `at`))

  _ <- S.inBlock S.SBGeneratedQuantities $ DM.splitToGroupVars dmr bParams.beta (Just "beta")
  _ <- S.inBlock S.SBGeneratedQuantities $ DM.splitToGroupVars dmr phi (Just "phi")
  _ <- SB.generatePosteriorPrediction
    md.acsDataTag
    (TE.NamedDeclSpec "pObserved" $ TE.array1Spec md.nData $ TE.intSpec [])
    (SD.betaBinomialDist' True)
    ((\(a, b) n -> md.trials `at` n :> a `at` n :> b `at` n :> TNil) <$> tempPs)
  pure ()

groupBuilderState :: (F.ElemOf rs GT.StateAbbreviation, Typeable rs, Typeable a) => [Text] -> S.StanGroupBuilderM [(F.Record rs, a)] () ()
groupBuilderState states = do
  acsData <- S.addModelDataToGroupBuilder "ACS" (S.ToFoldable id)
  S.addGroupIndexForData stateGroup acsData $ S.makeIndexFromFoldable show (F.rgetField @GT.StateAbbreviation . fst) states
  S.addGroupIntMapForDataSet stateGroup acsData $ S.dataToIntMapFromFoldable (F.rgetField @GT.StateAbbreviation . fst) states

{-
groupBuilderCD :: [Text] -> [Text] -> S.StanGroupBuilderM (F.FrameRec DDP.ACSByCD) () ()
groupBuilderCD states cds = do
  acsData <- S.addModelDataToGroupBuilder "ACS" (S.ToFoldable id)
  S.addGroupIndexForData stateGroup acsData $ S.makeIndexFromFoldable show (F.rgetField @GT.StateAbbreviation) states
  S.addGroupIndexForData cdGroup acsData $ S.makeIndexFromFoldable show DDP.districtKey cds
-}
cdGroup :: S.GroupTypeTag Text
cdGroup = S.GroupTypeTag "CD"

stateGroup :: S.GroupTypeTag Text
stateGroup = S.GroupTypeTag "State"

dmrS_ER :: forall rs . (F.ElemOf rs DT.Education4C
                           , F.ElemOf rs DT.SexC
                           , F.ElemOf rs DT.Race5C
                           )
                        => DM.DesignMatrixRow (F.Record rs)
dmrS_ER = DM.DesignMatrixRow "S_ER" [sexRP, raceEduRP]
  where
    sexRP = DM.boundedEnumRowPart Nothing "Sex" (F.rgetField @DT.SexC)
    raceEduRP = DM.boundedEnumRowPart (Just $ DM.BEProduct2 (DT.R5_WhiteNonHispanic, DT.E4_HSGrad)) "RaceEdu"
                $ \r -> DM.BEProduct2 (F.rgetField @DT.Race5C  r, F.rgetField @DT.Education4C r)

dmrC_S_ER :: forall rs . (F.ElemOf rs DT.CitizenC
                                   , F.ElemOf rs DT.Education4C
                                   , F.ElemOf rs DT.SexC
                                   , F.ElemOf rs DT.Race5C
                                   )
                   => DM.DesignMatrixRow (F.Record rs)
dmrC_S_ER = DM.DesignMatrixRow "C_S_ER" [citRP, sexRP, raceEduRP]
  where
    citRP = DM.boundedEnumRowPart Nothing "Citizen" (F.rgetField @DT.CitizenC )
    sexRP = DM.boundedEnumRowPart Nothing "Sex" (F.rgetField @DT.SexC)
    raceEduRP = DM.boundedEnumRowPart (Just $ DM.BEProduct2 (DT.R5_WhiteNonHispanic, DT.E4_HSGrad)) "RaceEdu"
                $ \r -> DM.BEProduct2 (F.rgetField @DT.Race5C  r, F.rgetField @DT.Education4C r)

dmrS_CR :: forall rs . (F.ElemOf rs DT.CitizenC
                           , F.ElemOf rs DT.SexC
                           , F.ElemOf rs DT.Race5C
                           )
                   => DM.DesignMatrixRow (F.Record rs)
dmrS_CR = DM.DesignMatrixRow "S_CR" [sexRP, citRaceRP]
  where
    sexRP = DM.boundedEnumRowPart Nothing "Sex" (F.rgetField @DT.SexC)
    citRaceRP = DM.boundedEnumRowPart (Just $ DM.BEProduct2 (DT.Citizen,  DT.R5_WhiteNonHispanic)) "CttRace"
                $ \r -> DM.BEProduct2 (r ^. DT.citizenC, r ^. DT.race5C)

dmrC_S_A2R :: forall rs . (F.ElemOf rs DT.CitizenC
                         , F.ElemOf rs DT.SimpleAgeC
                         , F.ElemOf rs DT.SexC
                         , F.ElemOf rs DT.Race5C
                         )
                   => DM.DesignMatrixRow (F.Record rs)
dmrC_S_A2R = DM.DesignMatrixRow "C_S_A2R" [citRP, sexRP, ageRaceRP]
  where
    citRP = DM.boundedEnumRowPart Nothing "Citizen" (F.rgetField @DT.CitizenC )
    sexRP = DM.boundedEnumRowPart Nothing "Sex" (F.rgetField @DT.SexC)
    ageRaceRP = DM.boundedEnumRowPart (Just $ DM.BEProduct2 (DT.Under, DT.R5_WhiteNonHispanic)) "Age2Race"
                $ \r -> DM.BEProduct2 (r ^. DT.simpleAgeC, r ^. DT.race5C)


dmrS_A2ER :: forall rs . (F.ElemOf rs DT.Education4C
                            , F.ElemOf rs DT.SexC
                            , F.ElemOf rs DT.Race5C
                            , F.ElemOf rs DT.SimpleAgeC
                            )
                        => DM.DesignMatrixRow (F.Record rs)
dmrS_A2ER = DM.DesignMatrixRow "S_A2ER" [sexRP, ageRaceEduRP]
  where
    sexRP = DM.boundedEnumRowPart Nothing "Sex" (F.rgetField @DT.SexC)
    ageRaceEduRP = DM.boundedEnumRowPart (Just $ DM.BEProduct3 (DT.Under, DT.R5_WhiteNonHispanic, DT.E4_HSGrad)) "Age2RaceEdu"
                   $ \r -> DM.BEProduct3 (r ^. DT.simpleAgeC, r ^. DT.race5C, r ^. DT.education4C)


dmrS_A2CR :: forall rs . (F.ElemOf rs DT.CitizenC
                        , F.ElemOf rs DT.SexC
                        , F.ElemOf rs DT.Race5C
                        , F.ElemOf rs DT.Age4C
                        )
              => DM.DesignMatrixRow (F.Record rs)
dmrS_A2CR = DM.DesignMatrixRow "S_A2CR" [sexRP, citAgeRaceRP]
  where
    sexRP = DM.boundedEnumRowPart Nothing "Sex" (F.rgetField @DT.SexC)
    citAgeRaceRP = DM.boundedEnumRowPart (Just $ DM.BEProduct3 (DT.Citizen, DT.Under, DT.R5_WhiteNonHispanic)) "CitAgeRace"
                   $ \r -> DM.BEProduct3 (r ^. DT.citizenC, DT.age4ToSimple $ r ^. DT.age4C, r ^. DT.race5C)


dmrS_AR :: forall rs . (F.ElemOf rs DT.SexC
                       , F.ElemOf rs DT.Race5C
                       , F.ElemOf rs DT.Age4C
                       )
              => DM.DesignMatrixRow (F.Record rs)
dmrS_AR = DM.DesignMatrixRow "S_AR" [sexRP, ageRaceRP]
  where
    sexRP = DM.boundedEnumRowPart Nothing "Sex" (F.rgetField @DT.SexC)
    ageRaceRP = DM.boundedEnumRowPart (Just $ DM.BEProduct2 (DT.A4_25To44, DT.R5_WhiteNonHispanic)) "AgeRace"
                $ \r -> DM.BEProduct2 (r ^. DT.age4C, r ^. DT.race5C)


dmrS_CAR :: forall rs . (F.ElemOf rs DT.CitizenC
                        , F.ElemOf rs DT.SexC
                        , F.ElemOf rs DT.Race5C
                        , F.ElemOf rs DT.Age4C
                        )
              => DM.DesignMatrixRow (F.Record rs)
dmrS_CAR = DM.DesignMatrixRow "S_CAR" [sexRP, citAgeRaceRP]
  where
    sexRP = DM.boundedEnumRowPart Nothing "Sex" (F.rgetField @DT.SexC)
    citAgeRaceRP = DM.boundedEnumRowPart (Just $ DM.BEProduct3 (DT.Citizen, DT.A4_25To44, DT.R5_WhiteNonHispanic)) "CitAgeRace"
                   $ \r -> DM.BEProduct3 (r ^. DT.citizenC, r ^. DT.age4C, r ^. DT.race5C)


dmrS_C_AR :: forall rs . (F.ElemOf rs DT.CitizenC
                        , F.ElemOf rs DT.SexC
                        , F.ElemOf rs DT.Race5C
                        , F.ElemOf rs DT.Age4C
                        )
              => DM.DesignMatrixRow (F.Record rs)
dmrS_C_AR = DM.DesignMatrixRow "S_C_AR" [sexRP, citRP, ageRaceRP]
  where
    sexRP = DM.boundedEnumRowPart Nothing "Sex" (F.rgetField @DT.SexC)
    citRP = DM.boundedEnumRowPart Nothing "Citizen" (F.rgetField @DT.CitizenC )
    ageRaceRP = DM.boundedEnumRowPart (Just $ DM.BEProduct2 (DT.A4_25To44, DT.R5_WhiteNonHispanic)) "AgeRace"
                $ \r -> DM.BEProduct2 (r ^. DT.age4C, r ^. DT.race5C)


designMatrixRowAge :: forall rs . (F.ElemOf rs DT.CitizenC
                                  , F.ElemOf rs DT.Education4C
                                  , F.ElemOf rs DT.SexC
                                  , F.ElemOf rs DT.Race5C
                                  )
                   => DM.DesignMatrixRow (F.Record rs)
designMatrixRowAge = DM.DesignMatrixRow "DMAge" [citRP, sexRP, eduRP, raceRP]
  where
    citRP = DM.boundedEnumRowPart Nothing "Citizen" (F.rgetField @DT.CitizenC )
    sexRP = DM.boundedEnumRowPart Nothing "Sex" (F.rgetField @DT.SexC)
    eduRP = DM.boundedEnumRowPart (Just DT.E4_HSGrad) "Education" (F.rgetField @DT.Education4C)
    raceRP = DM.boundedEnumRowPart (Just DT.R5_WhiteNonHispanic) "Race" (F.rgetField @DT.Race5C)



designMatrixRowCitizen :: forall rs . (F.ElemOf rs DT.Education4C
                                      , F.ElemOf rs DT.SexC
                                      , F.ElemOf rs DT.Race5C
                                      )
                       => DM.DesignMatrixRow (F.Record rs)
designMatrixRowCitizen = DM.DesignMatrixRow "DMCitizen" [sexRP, eduRP, raceRP]
  where
    sexRP = DM.boundedEnumRowPart Nothing "Sex" (F.rgetField @DT.SexC)
    eduRP = DM.boundedEnumRowPart (Just DT.E4_HSGrad) "Education" (F.rgetField @DT.Education4C)
    raceRP = DM.boundedEnumRowPart (Just DT.R5_WhiteNonHispanic) "Race" (F.rgetField @DT.Race5C)



--
newtype ModelResult2 ks = ModelResult2 { unModelResult :: Map (F.Record ks) Double }

deriving stock instance (Show (F.Record ks)) => Show (ModelResult2 ks)

instance (V.RMap ks, FS.RecFlat ks, Ord (F.Rec FS.SElField ks), Ord (F.Record ks)) => Flat.Flat (ModelResult2 ks) where
  size = Flat.size . M.mapKeys FS.toS . unModelResult
  encode = Flat.encode . M.mapKeys FS.toS . unModelResult
  decode = fmap (ModelResult2 . M.mapKeys FS.fromS) Flat.decode


applyModelResult2 :: (ks F.⊆ rs, Ord (F.Record ks), Show (F.Record rs))
                 => ModelResult2 ks -> F.Record rs -> Either Text Double
applyModelResult2 (ModelResult2 m) r = case M.lookup (F.rcast r) m of
                                         Nothing -> Left $ "applyModelResult2: key=" <> show r <> " not found in model result map."
                                         Just p -> Right p



--
data ModelResult g ks = ModelResult { alpha0 :: Double, geoAlpha :: Map g Double, ldSI :: (Double, Double), catAlpha :: Map (F.Record ks) Double }

deriving stock instance (Show (F.Record ks), Show g) => Show (ModelResult g ks)

modelResultToFTuple :: (Ord (V.Rec FS.SElField ks), V.RMap ks) => ModelResult g ks -> (Double, Map g Double, (Double, Double), Map (V.Rec FS.SElField ks) Double)
modelResultToFTuple (ModelResult a b c d) = (a, b, c, M.mapKeys FS.toS d)

modelResultFromFTuple :: (Ord (F.Record ks), V.RMap ks) => (Double, Map g Double, (Double, Double), Map (V.Rec FS.SElField ks) Double) -> ModelResult g ks
modelResultFromFTuple (a, b, c, d) = ModelResult a b c (M.mapKeys FS.fromS d)

instance (V.RMap ks, FS.RecFlat ks, Flat.Flat g, Ord g, Ord (F.Rec FS.SElField ks), Ord (F.Record ks)) => Flat.Flat (ModelResult g ks) where
  size = Flat.size . modelResultToFTuple
  encode = Flat.encode . modelResultToFTuple
  decode = fmap modelResultFromFTuple Flat.decode


applyModelResult :: (F.ElemOf rs DT.PWPopPerSqMile, ks F.⊆ rs, Ord g, Show g, Ord (F.Record ks), Show (F.Record rs))
                 => ModelResult g ks -> g -> F.Record rs -> Either Text Double
applyModelResult (ModelResult a ga (ldS, ldI) ca) g r = invLogit <$> xE where
  invLogit y = 1 / (1 + Numeric.exp (negate y))
  geoXE = maybe (Left $ "applyModelResult: " <> show g <> " missing from geography alpha map") Right $ M.lookup g ga
  densX = ldI + ldS * (DDP.safeLog $ F.rgetField @DT.PWPopPerSqMile r)
  catXE = maybe (Left $ "applyModelResult: " <> show r <> " missing from category alpha map") Right $ M.lookup (F.rcast r) ca
  xE = (\a' d g' c -> a' + d + g' + c) <$> pure a <*> pure densX <*> geoXE <*> catXE

stateModelResultAction :: forall rs ks a r gq.
                          (K.KnitEffects r
                          , Typeable rs
                          , Typeable a
                          , F.ElemOf rs DT.PWPopPerSqMile
--                          , ks F.⊆ rs
                          , Ord (F.Record ks)
                          , BRK.FiniteSet (F.Record ks)
                          )
                       => ModelConfig Text
                       -> DM.DesignMatrixRow (F.Record ks)
                       -> SC.ResultAction r [(F.Record rs, a)] gq S.DataSetGroupIntMaps () (ModelResult Text ks)
stateModelResultAction mc dmr = SC.UseSummary f where
  f summary _ modelDataAndIndexes_C _ = do
--    let resultCacheKey = modelID mc <> "_" <> DM.dmName dmr <> modelConfigSuffix mc
    (modelData', resultIndexesE) <- K.ignoreCacheTime modelDataAndIndexes_C
    -- we need to rescale the density component to work
    let premap = DDP.safeLog . F.rgetField @DT.PWPopPerSqMile . fst
        msFld = (,) <$> FL.mean <*> FL.std
        (ldMean, ldSigma) = FL.fold (FL.premap premap msFld) modelData'
    stateIM <- K.knitEither
      $ resultIndexesE >>= S.getGroupIndex (S.RowTypeTag @(F.Record rs, a) SC.ModelData "ACS") stateGroup
    let getScalar n = K.knitEither $ SP.getScalar . fmap CS.mean <$> SP.parseScalar n (CS.paramStats summary)
        getVector n = K.knitEither $ SP.getVector . fmap CS.mean <$> SP.parse1D n (CS.paramStats summary)
    alpha' <- case mc.includeAlpha0  of
      False -> pure 0
      True -> getScalar "alpha0"
    geoMap <- (\stIM alphaV -> M.fromList $ zip (IM.elems stIM) (Vec.toList alphaV)) <$> pure stateIM <*> getVector "alpha"
    (ldSlope, ldIntercept) <- case includeDensity mc of
      False -> pure (0, 0)
      True -> (\x -> (x / ldSigma, negate $ x * ldMean / ldSigma)) <$> getScalar "beta_Density"
    catBeta <- VU.convert <$> getVector "beta"
    let (S.MatrixRowFromData _ _ _ rowVecF) = DM.matrixFromRowData dmr Nothing
        allCatRows = S.toList $ BRK.elements @(F.Record ks)
        g v1 v2 = VU.foldl' (\a (b, c) -> a + b * c) 0 $ VU.zip v1 v2
        catMap = M.fromList $ zip allCatRows (g catBeta . rowVecF <$> allCatRows)
    pure $ ModelResult alpha' geoMap (ldSlope, ldIntercept) catMap

--    modelResult <- ModelResult <$> getVector "alpha"
type CitizenStateModelResult = ModelResult Text [DT.SexC, DT.Education4C, DT.Race5C]
type AgeStateModelResult = ModelResult Text [DT.CitizenC, DT.SexC, DT.Education4C, DT.Race5C]
type EduStateModelResult = ModelResult Text [DT.Age4C, DT.SexC, DT.Race5C]




logDensityDMRP :: F.ElemOf rs DT.PWPopPerSqMile => DM.DesignMatrixRowPart (F.Record rs)
logDensityDMRP = DM.DesignMatrixRowPart "Density" 1 DDP.logDensityPredictor

----

categoricalModel :: forall rs . Typeable rs
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
--    let sizeE e = TE.functionE SF.size (e :> TNil)
    betaX <- betaXD
    TE.addStmt $ TE.for "n" (TE.SpecificNumbered (TE.intE 1) nDataE) $ \n ->
      [TE.target $ TE.densityE SF.multinomial_logit_lupmf (countsE `at` n) (TE.transposeE (betaX `at` n) :> TNil)]

  gqBetaX <- S.inBlock S.SBLogLikelihood $ S.addFromCodeWriter betaXD
  SB.generateLogLikelihood
    acsData
    SD.multinomialLogitDist
    (pure $ \nE -> TE.transposeE (gqBetaX `at` nE) :> TNil)
    (pure $ \nE -> countsE `at` nE)


designMatrixRowEdu3 :: forall rs . (F.ElemOf rs DT.Age4C
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

designMatrixRowEdu4 :: forall rs . (F.ElemOf rs DT.Age4C
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

designMatrixRowEdu8 :: forall rs . (F.ElemOf rs DT.Age4C
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

designMatrixRowEdu5 :: forall rs . ( F.ElemOf rs DT.Age4C
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

designMatrixRowEdu6 :: forall rs . (F.ElemOf rs DT.Age4C
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


designMatrixRowEdu :: forall rs . (F.ElemOf rs DT.CitizenC
                                  ,  F.ElemOf rs DT.Age4C
                                  ,  F.ElemOf rs DT.SexC
                                  ,  F.ElemOf rs DT.Race5C
                                  )
                   => DM.DesignMatrixRow (F.Record rs)
designMatrixRowEdu = DM.DesignMatrixRow "DMEdu" [citRP, sexRP, ageRP, raceRP]
  where
    citRP = DM.boundedEnumRowPart Nothing "Citizen" (F.rgetField @DT.CitizenC )
    sexRP = DM.boundedEnumRowPart Nothing "Sex" (F.rgetField @DT.SexC )
    ageRP = DM.boundedEnumRowPart (Just DT.A4_25To44) "Age" (F.rgetField @DT.Age4C)
    raceRP = DM.boundedEnumRowPart (Just DT.R5_WhiteNonHispanic) "Race" (F.rgetField @DT.Race5C)

designMatrixRowEdu7 :: forall rs . (F.ElemOf rs DT.CitizenC
                                   , F.ElemOf rs DT.Age4C
                                   , F.ElemOf rs DT.SexC
                                   , F.ElemOf rs DT.Race5C
                                   )
                   => DM.DesignMatrixRow (F.Record rs)
designMatrixRowEdu7 = DM.DesignMatrixRow "DMEdu7" [citRP, sexRP, raceAgeRP]
  where
    citRP = DM.boundedEnumRowPart Nothing "Citizen" (F.rgetField @DT.CitizenC )
    sexRP = DM.boundedEnumRowPart Nothing "Sex" (F.rgetField @DT.SexC)
    raceAgeRP = DM.boundedEnumRowPart Nothing "RaceAge"
                $ \r -> DM.BEProduct2 (F.rgetField @DT.Race5C r, F.rgetField @DT.Age4C r)

designMatrixRowEdu2 :: forall rs . (F.ElemOf rs DT.CitizenC
                                   , F.ElemOf rs DT.Age4C
                                   , F.ElemOf rs DT.SexC
                                   , F.ElemOf rs DT.Race5C
                                   )
                   => DM.DesignMatrixRow (F.Record rs)
designMatrixRowEdu2 = DM.DesignMatrixRow "DMEdu2" [citRP, sexRP, raceAgeRP]
  where
    citRP = DM.boundedEnumRowPart Nothing "Citizen" (F.rgetField @DT.CitizenC )
    sexRP = DM.boundedEnumRowPart Nothing "Sex" (F.rgetField @DT.SexC)
    raceAgeRP = DM.boundedEnumRowPart (Just $ DM.BEProduct2 (DT.R5_WhiteNonHispanic, DT.A4_25To44)) "RaceAge"
                $ \r -> DM.BEProduct2 (F.rgetField @DT.Race5C r, F.rgetField @DT.Age4C r)




{-
binomialNormalModel :: forall rs . Typeable rs
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

binomialModel :: forall rs . Typeable rs
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
-}


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

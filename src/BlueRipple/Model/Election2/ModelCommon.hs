{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE UnicodeSyntax #-}
{-# LANGUAGE StandaloneDeriving #-}

module BlueRipple.Model.Election2.ModelCommon
  (
    module BlueRipple.Model.Election2.ModelCommon
  )
where

import qualified BlueRipple.Configuration as BR
import qualified BlueRipple.Utilities.KnitUtils as BRKU
import qualified BlueRipple.Model.Election2.DataPrep as DP
import qualified BlueRipple.Data.DemographicTypes as DT

import qualified Knit.Report as K hiding (elements)


import qualified Control.Foldl as FL
import Control.Lens (view)
import qualified Data.IntMap.Strict as IM
import qualified Data.Map.Strict as M
import qualified Data.Set as S

import qualified Data.List as List
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU

import qualified Frames as F

import qualified Numeric

import qualified CmdStan as CS
import qualified Stan.ModelBuilder as SMB
import qualified Stan.ModelRunner as SMR
import qualified Stan.ModelConfig as SC
import qualified Stan.Parameters as SP
import qualified Stan.RScriptBuilder as SR
import qualified Stan.ModelBuilder.BuildingBlocks as SBB
import qualified Stan.ModelBuilder.Distributions as SMD
import qualified Stan.ModelBuilder.DesignMatrix as DM
import qualified Stan.ModelBuilder.TypedExpressions.Types as TE
import qualified Stan.ModelBuilder.TypedExpressions.Statements as TE
import qualified Stan.ModelBuilder.TypedExpressions.Indexing as TEI
import qualified Stan.ModelBuilder.TypedExpressions.DAG as DAG
import qualified Stan.ModelBuilder.TypedExpressions.StanFunctions as SF
import Stan.ModelBuilder.TypedExpressions.TypedList (TypedList(..))
import qualified Flat
import Flat.Instances.Vector ()


-- design matrix rows
safeLog :: Double -> Double
safeLog x = if x >= 1 then Numeric.log x else 0

tDesignMatrixRow_d_A_S_E_R :: DM.DesignMatrixRow (F.Record DP.PredictorsR)
tDesignMatrixRow_d_A_S_E_R = DM.DesignMatrixRow "d_A_S_E_R" [dRP, aRP, sRP, eRP, rRP]
  where
    dRP = DM.DesignMatrixRowPart "logDensity" 1 (VU.singleton . safeLog . view DT.pWPopPerSqMile)
    aRP = DM.boundedEnumRowPart (Just DT.A5_18To24) "Age" (view DT.age5C)
    sRP = DM.boundedEnumRowPart Nothing "Sex" (view DT.sexC)
    eRP = DM.boundedEnumRowPart (Just DT.E4_NonHSGrad) "Edu" (view DT.education4C)
    rRP = DM.boundedEnumRowPart (Just DT.R5_WhiteNonHispanic) "Race" (view DT.race5C)

stateG :: SMB.GroupTypeTag Text
stateG = SMB.GroupTypeTag "State"


stateGroupBuilder :: (Foldable f, Foldable g, Typeable a)
                  => (a -> Text) -> (md -> f a) -> g Text -> SMB.StanGroupBuilderM md () ()
stateGroupBuilder saF foldableRows states = do
  projData <- SMB.addModelDataToGroupBuilder "ElectionModelData" (SMB.ToFoldable foldableRows)
  SMB.addGroupIndexForData stateG projData $ SMB.makeIndexFromFoldable show saF states
  SMB.addGroupIntMapForDataSet stateG projData $ SMB.dataToIntMapFromFoldable saF states

data StateAlpha = StateAlphaSimple | StateAlphaHierCentered | StateAlphaHierNonCentered deriving stock (Show)

stateAlphaModelText :: StateAlphaModel -> Text
stateAlphaModelText StateAlphaSimple = "AS"
stateAlphaModelText StateAlphaHierCentered = "AHC"
stateAlphaModelText StateAlphaHierNonCentered = "AHNC"

data ModelType = TurnoutMT | RegistrationMT | PreferenceMT | FullMT
data TurnoutSurvey = CESSurvey | CPSSurvey
turnoutSurveyText :: TurnoutSurvey -> Text
turnoutSurveyText CESSurvey = "CES"
turnoutSurveyText CPSSurvey = "CPS"

data PSTargets = NoPSTargets | PSTargets
psTargetsText :: PSTargets -> Text
psTargetsText NoPSTargets = "noPSTgt"
psTargetsText PSTargets = "PSTgt"

-- we can use the designMatrixRow to get (a -> Vector Double) for prediction
data TurnoutConfig =
  TurnoutConfig
  {
    tDataSource :: TurnoutDataSource
  , tPSTargets :: PSTargets
  , tDesignMatrixRow :: DM.DesignMatrixRow DP.PredictorsR
  , tStateAlphaModel :: StateAlpha
  }

-- for now we model only alpha hierarchically. Beta will be the same everywhere.
data TurnoutPrediction =
  TurnoutPrediction
  {
    tpAlphaMap :: Map Text Double
  , tpBetaSI :: VU.Vector (Double, Double)
  , tpLogisticAdjMapM :: Maybe (Map Text Double)
  }

deriving anyclass instance Flat.Flat TurnoutPrediction

predictedTurnoutP :: TurnoutConfig -> TurnoutPrediction -> Text -> DP.PredictorsR -> Either Text Double
predictedTurnoutP tc tp sa p = do
  alpha <- case M.lookup sa tp.tpAlphaMap of
    Nothing -> Left $ "Model.Election2.ModelCommon.predictedP: alphaMap lookup failed for k=" <> sa
    Just x -> pure x
  logisticAdj <- case pd.pdLogisticAdjMapM of
    Nothing -> pure 0
    Just m -> case M.lookup sa m of
      Nothing -> Left $ "Model.Election2.ModelCommon.predictedP: pdLogisticAdjMap lookup failed for k=" <> show sa
      Just x -> pure x
  let covariatesV = DM.designMatrixRowF tc.tDesignMatrixRow p
      invLogit x = 1 / (1 + exp (negate x))
      applySI x (s, i) = i + s * x
  pure $ invLogit (alpha + VU.sum (VU.zipWith applySI covariatesV pd.pdBetaSI) + logisticAdj)

turnoutModelText :: TurnoutConfig  -> Text
turnoutModelText (TurnoutConfig ts tPs dmr am) = "Turnout_" <> turnoutSurveyText ts
                                                 <> "_"
                                                 <> (if (tPS == PSTargets) then "PSTgt_" else "")
                                                 <> dmr.dmName <> "_" <> stateAlphaModelText am

turnoutModelDataText :: TurnoutConfig -> Text
turnoutModelDataText (TurnoutConfig ts tPs dmr am) = "Turnout_" <> turnoutSurveyText ts
                                                 <> "_"
                                                 <> (if (tPS == PSTargets) then "PSTgt" else "")


data CovariatesAndCounts a =
  CovariatesAndCounts
  {
    ccSurveyDataTag :: SMB.RowTypeTag a
  , ccNCovariates :: TE.IntE
  , ccCovariates :: TE.MatrixE
  , ccTrials :: TE.IntArrayE
  , ccSuccesses :: TE.IntArrayE
  }

data StateTargetsData td =
  StateTargetsData
  {
    stdTargetTypeTag :: SMB.RowTypeTag td
  , stdStateTargets :: TE.IntArrayE
  , stdACSTag :: SMB.RowTypeTag (F.Record DDP.ACSa5ByStateR)
  , stdACSWgts :: TE.IntArrayE
  , stdACSCovariates :: TE.MatrixE
  }

data TurnoutModelData where
  NoPT_CPSTurnoutModelData :: CovariatesAndCounts (F.Record CPSByStateR) -> TurnoutModelData
  NoPT_CESTurnoutModelData :: CovariatesAndCounts (F.Record CESByCDR) -> TurnoutModelData
  PT_CPSTurnoutModelData :: CovariatesAndCounts (F.Record CPSByStateR) -> StateTargetsData BR.StateTurnoutCols -> TurnoutModelData
  PT_CESTurnoutModelData :: CovariatesAndCounts (F.Record CESByCDR) -> StateTargetsData BR.StateTurnoutCols -> TurnoutModelData


turnoutModelData :: TurnoutConfig
                 -> SMB.StanBuilderM ModelData () TurnoutModelData
turnoutModelData tc = do
  let cpsCC = do
        surveyDataTag <- SMB.dataSetTag @(F.Record CPSByStateR) SC.ModelData "CPSSurvey"
        let (_, nCovariatesE) = DM.designMatrixColDimBinding tc.tDesignMatrixRow Nothing
        dmE <- if DM.rowLength tc.tDesignMatrixRow > 0
          then DM.addDesignMatrix surveyDataTag (contramap F.rcast tc.tDesignMatrixRow) Nothing
          else pure $ TE.namedE "ERROR" TE.SMat -- this shouldn't show up in stan code at all
        trialsE <- SBB.addCountData modelDataTag "Surveyed" (view DP.surveyed)
        successesE <- SBB.addCountData modelDataTag "Voted" (view DP.voted)
        pure $ CovariatesAndCounts surveyDataTag nCovariatesE dmE trialsE successesE
      cesCC = do
        surveyDataTag <- SMB.dataSetTag @(F.Record CESByCDR) SC.ModelData "CESSurvey"
        let (_, nCovariatesE) = DM.designMatrixColDimBinding tc.tDesignMatrixRow Nothing
        dmE <- if DM.rowLength tc.tDesignMatrixRow > 0
          then DM.addDesignMatrix surveyDataTag (contramap F.rcast tc.tDesignMatrixRow) Nothing
          else pure $ TE.namedE "ERROR" TE.SMat -- this shouldn't show up in stan code at all
        trialsE <- SBB.addCountData modelDataTag "Surveyed" (view DP.surveyed)
        successesE <- SBB.addCountData modelDataTag "Voted" (view DP.voted)
        pure $ CovariatesAndCounts surveyDataTag nCovariatesE dmE trialsE successesE
  case tc.tPSTargets of
    NoPSTargets -> case tc.tDataSource of
      CPSSurvey -> NoPT_CPSTurnoutModelData cpsCC
      CESSurvey -> NoPT_CESTurnoutModelData cesCC
    PSTargets -> do
      stateTurnoutTargetTag <- SMB.dataSetTag @(F.Record BR.StateTurnoutCols) SC.ModelData "StateTurnout"
      turnoutTargets <- SBB.addCountData stateTurnoutTargetTag "TurnoutTargets" (view BR.ballotsCounted)
      acsTag <- SMB.dataSetTag @(F.Record DP.ACSa5ByStateR) SC.ModelData "ACS"
      acsWgts <- SBB.addCountData acsTag "ACSWgts" (view DT.popCount)
      acsCovariates <- if DM.rowLength tc.tDesignMatrixRow > 0
          then DM.addDesignMatrix acsTag (contramap F.rcast tc.tDesignMatrixRow) Nothing
          else pure $ TE.namedE "ERROR" TE.SMat -- this shouldn't show up in stan code at all
      let std = StateTargetData stateTurnoutTag turnoutTargets acsTag acsWgts acsCovariates
      case tc.tDataSource of
        CPSSurvey -> PT_CPSTurnoutModelData cpsCC std
        CESSurvey -> PT_CESTurnoutModelData cpsCC std
{-
  modelDataTag <- SMB.dataSetTag @a SC.ModelData "ElectionModelData"
  let (_, nCovariatesE') = DM.designMatrixColDimBinding mc.designMatrixRow Nothing
  dmE <- if DM.rowLength mc.designMatrixRow > 0
         then DM.addDesignMatrix modelDataTag mc.designMatrixRow Nothing
         else pure $ TE.namedE "ERROR" TE.SMat -- this shouldn't show up in stan code at all
  trialsE' <- SBB.addCountData modelDataTag mc.trialsT mc.trialsF
  successesE' <- SBB.addCountData modelDataTag mc.successesT mc.successesF
  pure $ ModelData modelDataTag nCovariatesE' dmE trialsE' successesE'
-}

-- given S states
-- alpha is a scalar or S col-vector
data Alpha = SimpleAlpha (DAG.Parameter TE.EReal) | HierarchicalAlpha (DAG.Parameter TE.ECVec)

-- and K predictors
-- theta is K col-vector (or Nothing)
newtype Theta = Theta (Maybe (DAG.Parameter TE.ECVec))

data ModelParameters where
  BinomialLogitModelParameters :: Alpha -> Theta -> ModelParameters

paramAlpha :: ModelParameters -> Alpha
paramAlpha (BinomialLogitModelParameters a _) = a

paramTheta :: ModelParameters -> Theta
paramTheta (BinomialLogitModelParameters _ t) = t

modelParameters :: DM.DesignMatrixRow a -> StateAlphaModel -> SMB.StanBuilderM md () ModelParameters
modelParameters dmr sa = do
  let stdNormalDWA :: (TE.TypeOneOf t [TE.EReal, TE.ECVec, TE.ERVec], TE.GenSType t) => TE.DensityWithArgs t
      stdNormalDWA = TE.DensityWithArgs SF.std_normal TNil
      numPredictors = DM.rowLength dmr
      (_, nCovariatesE) = DM.designMatrixColDimBinding dmr Nothing

  -- for now all the thetas are iid std normals
  theta <- if numPredictors > 0 then
               (Theta . Just)
               <$> DAG.simpleParameterWA
               (TE.NamedDeclSpec "theta" $ TE.vectorSpec nCovariatesE [])
               stdNormalDWA
             else pure $ Theta Nothing
  let nStatesE = SMB.groupSizeE stateG
      hierAlphaNDS = TE.NamedDeclSpec "alpha" $ TE.vectorSpec nStatesE []
      hierAlphaPs = do
        muAlphaP <- DAG.simpleParameterWA
                    (TE.NamedDeclSpec "muAlpha" $ TE.realSpec [])
                    stdNormalDWA
        sigmaAlphaP <-  DAG.simpleParameterWA
                        (TE.NamedDeclSpec "sigmaAlpha" $ TE.realSpec [TE.lowerM $ TE.realE 0])
                        stdNormalDWA
        pure (muAlphaP :> sigmaAlphaP :> TNil)
  alpha <- case sa of
    StateAlphaSimple -> do
      fmap SimpleAlpha
        $ DAG.simpleParameterWA
        (TE.NamedDeclSpec "alpha" $ TE.realSpec [])
        stdNormalDWA
    StateAlphaHierCentered -> do
      alphaPs <- hierAlphaPs
      fmap HierarchicalAlpha
        $ DAG.addBuildParameter
        $ DAG.UntransformedP hierAlphaNDS [] alphaPs
        $ \(muAlphaE :> sigmaAlphaE :> TNil) m
          -> TE.addStmt $ TE.sample m SF.normalS (muAlphaE :> sigmaAlphaE :> TNil)
    StateAlphaHierNonCentered -> do
      alphaPs <- hierAlphaPs
      let rawNDS = TE.NamedDeclSpec (TE.declName hierAlphaNDS <> "_raw") $ TE.decl hierAlphaNDS
      rawAlphaP <- DAG.simpleParameterWA rawNDS stdNormalDWA
      fmap HierarchicalAlpha
        $ DAG.addBuildParameter
        $ DAG.TransformedP hierAlphaNDS []
        (rawAlphaP :> alphaPs) DAG.TransformedParametersBlock
        (\(rawE :> muAlphaE :> muSigmaE :> TNil) -> DAG.DeclRHS $ muAlphaE `TE.plusE` (muSigmaE `TE.timesE` rawE))
        TNil (\_ _ -> pure ())
  pure $ BinomialLogitModelParameters alpha theta

data RunConfig = RunConfig { rcIncludePPCheck :: Bool, rcIncludeLL :: Bool, rcIncludeDMSplits :: Bool }

estimates :: ModelParameters -> TE.IntE -> TE.IntArrayE -> TE.MatrixE -> TE.RealArrayE
estimates ps states covariates = case ps of
  BinomialLogitModelParameters alpha theta ->
    let ap = case alpha of
          SimpleAlpha sa -> pExpr sa
          HierarchicalAlpha ->

-- not returning anything for now
turnoutModel ::  RunConfig
             -> TurnoutConfig
             -> SMB.StanBuilderM md () ()
turnoutModel rc tmc = do
  mData <- turnoutModelData tmc
  mParams <- modelParameters tmc.designMatrixRow tmc.stateAlphaModel
  let betaNDS = TE.NamedDeclSpec "beta" $ TE.vectorSpec mData.nCovariatesE []
      nRowsE = SMB.dataSetSizeE mData.modelDataTag
      pExpr = DAG.parameterExpr

  -- to apply the same transformation to another matrix, we center with centerF and then post-multiply by r
  -- or we could apply beta to centered matrix ?
  (covariatesM, r, centerF, mBeta) <- case paramTheta mParams of
    Theta (Just thetaP) -> do
      (centeredCovariatesE, centerF) <- DM.centerDataMatrix DM.DMCenterOnly mData.covariatesE Nothing "DM"
      (dmQ, r, _, mBeta') <- DM.thinQR centeredCovariatesE "DM" $ Just (pExpr thetaP, betaNDS)
      pure (dmQ, r, centerF, mBeta')
    Theta Nothing -> pure (TE.namedE "ERROR" TE.SMat, \_ x _ -> pure x, Nothing)

  -- model
  let reIndexByState = TE.indexE TEI.s0 (SMB.byGroupIndexE mData.modelDataTag stateG)
      lpE :: Alpha -> Theta -> TE.VectorE
      lpE a t =  case a of
       SimpleAlpha alphaP -> case t of
         Theta Nothing -> TE.functionE SF.rep_vector (pExpr alphaP :> nRowsE :> TNil)
         Theta (Just thetaP) -> pExpr alphaP `TE.plusE` (covariatesM `TE.timesE` pExpr thetaP)
       HierarchicalAlpha alpha -> case t of
         Theta Nothing -> reIndexByState $ pExpr alpha
         Theta (Just thetaP) -> reIndexByState (pExpr alpha) `TE.plusE` (covariatesM `TE.timesE` pExpr thetaP)

  let ppF :: SMD.StanDist TE.EInt pts xs --((TE.IntE -> TE.ExprList xs) -> TE.IntE -> TE.UExpr TE.EReal)
          -> TE.CodeWriter (TE.IntE -> TE.ExprList xs)
          -> SMB.StanBuilderM md () (TE.ArrayE TE.EInt)
      ppF dist rngPSCW = SBB.generatePosteriorPrediction
                         mData.modelDataTag
                         (TE.NamedDeclSpec ("pred" <> mc.successesT) $ TE.array1Spec nRowsE $ TE.intSpec [])
                         dist --rngF
                         rngPSCW
--                         (\_ p -> p)
      llF :: SMD.StanDist t pts rts
          -> TE.CodeWriter (TE.IntE -> TE.ExprList pts)
          -> TE.CodeWriter (TE.IntE -> TE.UExpr t)
          -> SMB.StanBuilderM md gq ()
      llF = SBB.generateLogLikelihood mData.modelDataTag


  let (sampleStmtF, pp, ll) = case mParams of
        BinomialLogitModelParameters a t ->
          let ssF e = SMB.familySample SMD.binomialLogitDist e (mData.trialsE :> lpE a t :> TNil)
--              rF f nE = TE.functionE SF.normal_rng (f nE)
              rpF :: TE.CodeWriter (TE.IntE -> TE.ExprList '[TE.EInt, TE.EReal])
              rpF = pure $ \nE -> mData.trialsE `TE.at` nE :> lpE a t `TE.at` nE :> TNil
--              rpF = pure $ \nE -> lpE `fstI` nE :> mData.trialsE `fstI` nE :> TNil
              ll = llF SMD.binomialLogitDist rpF (pure $ \nE -> mData.successesE `TE.at` nE)
          in (ssF, ppF SMD.binomialLogitDist rpF, ll)

  SMB.inBlock SMB.SBModel $ SMB.addFromCodeWriter $ TE.addStmt $ sampleStmtF mData.successesE
  -- generated quantities
  when rc.rcIncludePPCheck $ void pp
  when rc.rcIncludeLL ll
  when rc.rcIncludeDMSplits $ case mBeta of
    Nothing -> pure ()
    Just beta -> SMB.inBlock SMB.SBGeneratedQuantities $ do
      _ <- DM.splitToGroupVars mc.designMatrixRow beta Nothing
      pure()
  pure ()

--cwdF :: (F.ElemOf rs DT.PopCount, F.ElemOf rs DT.PWPopPerSqMile) => F.Record rs -> DMS.CellWithDensity
--cwdF r = DMS.CellWithDensity (realToFrac $ r ^. DT.popCount) (r ^. DT.pWPopPerSqMile)

runModel :: (K.KnitEffects r
            , BRKU.CacheEffects r
            , Foldable f
            , Typeable a
            )
         => Either Text Text
         -> Either Text Text
         -> Text
         -> BR.CommandLine
         -> RunConfig
         -> (md -> f a)
         -> ModelConfig a
         -> K.ActionWithCacheTime r md
         -> K.Sem r (K.ActionWithCacheTime r PredictionData)
runModel modelDirE cacheDirE modelName _cmdLine runConfig foldableRows modelConfig modelData_C = do
  cacheRoot <- case cacheDirE of
    Left cd -> BRKU.clearIfPresentD cd >> pure cd
    Right cd -> pure cd

  let dataName = dataText modelConfig
      runnerInputNames = SC.RunnerInputNames
                         ("br-2023-electionModel/stan/" <> modelName <> "/")
                         (modelText modelConfig)
                         (Just $ SC.GQNames "pp" dataName) -- posterior prediction vars to wrap
                         dataName
--  modelData <- K.ignoreCacheTime modelData_C
  states <- FL.fold (FL.premap modelConfig.stateAbbrF FL.set) . foldableRows <$> K.ignoreCacheTime modelData_C
  (dw, code) <-  SMR.dataWranglerAndCode modelData_C (pure ())
                (stateGroupBuilder modelConfig.stateAbbrF foldableRows (S.toList states))
                (model runConfig modelConfig)

  let unwraps = [SR.UnwrapNamed modelConfig.successesT ("y" <> modelConfig.successesT)]

  res_C <- SMR.runModel' @BRKU.SerializerC @BRKU.CacheData
           modelDirE
           (Right runnerInputNames)
           Nothing
           dw
           code
           (modelResultAction foldableRows modelConfig) --SC.DoNothing -- (stateModelResultAction mcWithId dmr)
           (SMR.Both unwraps) --(SMR.Both [SR.UnwrapNamed "successes" "yObserved"])
           modelData_C
           (pure ())
  K.logLE K.Info $ modelName <> " run complete."
  pure res_C

--NB: parsed summary data has stan indexing, i.e., Arrays start at 1.
modelResultAction :: forall a r md f . (K.KnitEffects r, Foldable f, Typeable a)
                  => (md -> f a)
                  -> ModelConfig a
                  -> SC.ResultAction r md () SMB.DataSetGroupIntMaps () PredictionData
modelResultAction foldableRows modelConfig = SC.UseSummary f where
  f summary _ modelDataAndIndexes_C _ = do
    (modelData, resultIndexesE) <- K.ignoreCacheTime modelDataAndIndexes_C
    -- compute means of predictors because model was zero-centered in them
    let covariates = DM.designMatrixRowF modelConfig.designMatrixRow
        nPredictors = DM.rowLength modelConfig.designMatrixRow
        mdMeansFld = FL.premap (VU.toList . covariates)
                    $ traverse (\n -> FL.premap (List.!! n) FL.mean) [0..(nPredictors - 1)]
        mdMeansL = FL.fold mdMeansFld $ foldableRows modelData
    stateIM <- K.knitEither
      $ resultIndexesE >>= SMB.getGroupIndex (SMB.RowTypeTag @a SC.ModelData "ElectionModelData") stateG
    let allStates = IM.elems stateIM
        getScalar n = K.knitEither $ SP.getScalar . fmap CS.mean <$> SP.parseScalar n (CS.paramStats summary)
        getVector n = K.knitEither $ SP.getVector . fmap CS.mean <$> SP.parse1D n (CS.paramStats summary)
--        getMatrix n = K.knitEither $ fmap CS.mean <$> SP.parse2D n (CS.paramStats summary)
    geoMap <- case modelConfig.stateAlphaModel of
      StateAlphaSimple -> do
        alpha <- getScalar "alpha"
        pure $ M.fromList $ fmap (, alpha) allStates
      _ -> do
        alphaV <- getVector "alpha"
--        let mRowToList row = SP.getIndexed alphaV row
        pure $ M.fromList $ fmap (\(stateIdx, stateAbbr) -> (stateAbbr, alphaV V.! (stateIdx - 1))) $ IM.toList stateIM
    betaSI <- case nPredictors of
      0 -> pure V.empty
      p -> do
        betaV <- getVector "beta"
        pure $ V.fromList $ zip (V.toList betaV) mdMeansL
    pure $ PredictionData geoMap (VU.convert betaSI) Nothing

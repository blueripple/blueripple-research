{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE UnicodeSyntax #-}
{-# LANGUAGE StandaloneDeriving #-}

module BlueRipple.Model.Election2.ModelCommon
  (
    module BlueRipple.Model.Election2.ModelCommon
  )
where

import qualified BlueRipple.Configuration as BR
import qualified BlueRipple.Data.Loaders as BRDF
import qualified BlueRipple.Data.DataFrames as BRDF
import qualified BlueRipple.Utilities.KnitUtils as BRKU
import qualified BlueRipple.Model.Election2.DataPrep as DP
import qualified BlueRipple.Model.Demographic.DataPrep as DDP
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.GeographicTypes as GT

import qualified Knit.Report as K hiding (elements)


import qualified Control.Foldl as FL
import Control.Lens (view, (^.))
import qualified Data.IntMap.Strict as IM
import qualified Data.Map.Strict as M
import qualified Data.Set as S

import qualified Data.List as List
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vinyl as V

import qualified Frames as F
import qualified Frames.Melt as F
import qualified Frames.Serialize as FS

import qualified Numeric

import qualified CmdStan as CS
import qualified Stan.ModelBuilder as SMB
import qualified Stan.ModelRunner as SMR
import qualified Stan.ModelConfig as SC
import qualified Stan.Parameters as SP
import qualified Stan.RScriptBuilder as SR
import qualified Stan.ModelBuilder.BuildingBlocks as SBB
import qualified Stan.ModelBuilder.Distributions as SMD
import qualified Stan.ModelBuilder.Distributions.RealBinomial as SMD
import qualified Stan.ModelBuilder.DesignMatrix as DM
import qualified Stan.ModelBuilder.TypedExpressions.Types as TE
import qualified Stan.ModelBuilder.TypedExpressions.Statements as TE
import qualified Stan.ModelBuilder.TypedExpressions.Indexing as TEI
import qualified Stan.ModelBuilder.TypedExpressions.Operations as TEO
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

tDesignMatrixRow_d_A_S_E_R_RE :: DM.DesignMatrixRow (F.Record DP.PredictorsR)
tDesignMatrixRow_d_A_S_E_R_RE = DM.DesignMatrixRow "d_A_S_E_R_RE" [dRP, aRP, sRP, eRP, rRP, reRP]
  where
    dRP = DM.DesignMatrixRowPart "logDensity" 1 (VU.singleton . safeLog . view DT.pWPopPerSqMile)
    aRP = DM.boundedEnumRowPart (Just DT.A5_18To24) "Age" (view DT.age5C)
    sRP = DM.boundedEnumRowPart Nothing "Sex" (view DT.sexC)
    eRP = DM.boundedEnumRowPart (Just DT.E4_NonHSGrad) "Edu" (view DT.education4C)
    rRP = DM.boundedEnumRowPart (Just DT.R5_WhiteNonHispanic) "Race" (view DT.race5C)
    re r = DM.BEProduct2 (r ^. DT.race5C, r ^. DT.education4C)
    reRP = DM.boundedEnumRowPart (Just (DM.BEProduct2 (DT.R5_WhiteNonHispanic, DT.E4_NonHSGrad))) "RaceEdu" re

tDesignMatrixRow_d_A_S_RE :: DM.DesignMatrixRow (F.Record DP.PredictorsR)
tDesignMatrixRow_d_A_S_RE = DM.DesignMatrixRow "d_A_S_RE" [dRP, aRP, sRP, reRP]
  where
    dRP = DM.DesignMatrixRowPart "logDensity" 1 (VU.singleton . safeLog . view DT.pWPopPerSqMile)
    aRP = DM.boundedEnumRowPart (Just DT.A5_18To24) "Age" (view DT.age5C)
    sRP = DM.boundedEnumRowPart Nothing "Sex" (view DT.sexC)
--    eRP = DM.boundedEnumRowPart (Just DT.E4_NonHSGrad) "Edu" (view DT.education4C)
--    rRP = DM.boundedEnumRowPart (Just DT.R5_WhiteNonHispanic) "Race" (view DT.race5C)
    re r = DM.BEProduct2 (r ^. DT.race5C, r ^. DT.education4C)
    reRP = DM.boundedEnumRowPart (Just (DM.BEProduct2 (DT.R5_WhiteNonHispanic, DT.E4_NonHSGrad))) "RaceEdu" re

stateG :: SMB.GroupTypeTag Text
stateG = SMB.GroupTypeTag "State"

psGroupTag :: forall k . Typeable k => SMB.GroupTypeTag (F.Record k)
psGroupTag = SMB.GroupTypeTag "PSGrp"


-- NB: often l ~ k, e.g., for predicting district turnout/preference
-- But say you want to predict turnout by race, nationally.
-- Now l ~ '[Race5C]
-- How about turnout by Education in each state? Then l ~ [StateAbbreviation, Education4C]
stateGroupBuilder :: forall g k l .
                     (Foldable g
                     , Typeable (DP.PSDataR k)
                     , Show (F.Record l)
                     , Ord (F.Record l)
                     , l F.⊆ DP.PSDataR k
                     , Typeable l
                     )
                  => TurnoutConfig
                  -> g Text
                  -> g (F.Record l)
                  -> SMB.StanGroupBuilderM DP.ModelData (DP.PSData k) ()
stateGroupBuilder turnoutConfig states psKeys = do
  let saF :: F.ElemOf rs GT.StateAbbreviation => F.Record rs -> Text
      saF = view GT.stateAbbreviation
      addSurveyIndexes :: F.ElemOf rs GT.StateAbbreviation => SMB.RowTypeTag (F.Record rs) -> SMB.StanGroupBuilderM DP.ModelData gq ()
      addSurveyIndexes surveyDataTag = do
        SMB.addGroupIndexForData stateG surveyDataTag $ SMB.makeIndexFromFoldable show saF states
        SMB.addGroupIntMapForDataSet stateG surveyDataTag $ SMB.dataToIntMapFromFoldable saF states
  case turnoutConfig.tSurvey of
    CESSurvey -> SMB.addModelDataToGroupBuilder "SurveyData" (SMB.ToFoldable DP.cesData) >>= addSurveyIndexes
    CPSSurvey -> SMB.addModelDataToGroupBuilder "SurveyData" (SMB.ToFoldable DP.cpsData) >>= addSurveyIndexes
  case turnoutConfig.tPSTargets of
    NoPSTargets -> pure ()
    PSTargets -> do
      turnoutTargetDataTag <- SMB.addModelDataToGroupBuilder "TurnoutTargetData" (SMB.ToFoldable DP.stateTurnoutData)
      SMB.addGroupIndexForData stateG turnoutTargetDataTag $ SMB.makeIndexFromFoldable show (view GT.stateAbbreviation) states
      acsDataTag <- SMB.addModelDataToGroupBuilder "ACSData" (SMB.ToFoldable DP.acsData)
      SMB.addGroupIndexForData stateG acsDataTag $ SMB.makeIndexFromFoldable show saF states
  let psGtt = psGroupTag @l
  psTag <- SMB.addGQDataToGroupBuilder "PSData" (SMB.ToFoldable DP.unPSData)
  SMB.addGroupIndexForData psGtt psTag $ SMB.makeIndexFromFoldable show F.rcast psKeys
  SMB.addGroupIntMapForDataSet psGtt psTag $ SMB.dataToIntMapFromFoldable F.rcast psKeys

data StateAlpha = StateAlphaSimple | StateAlphaHierCentered | StateAlphaHierNonCentered deriving stock (Show)

stateAlphaModelText :: StateAlpha -> Text
stateAlphaModelText StateAlphaSimple = "AS"
stateAlphaModelText StateAlphaHierCentered = "AHC"
stateAlphaModelText StateAlphaHierNonCentered = "AHNC"

data ModelType = TurnoutMT | RegistrationMT | PreferenceMT | FullMT deriving stock (Eq, Ord, Show)
data TurnoutSurvey = CESSurvey | CPSSurvey deriving stock (Eq, Ord, Show)
turnoutSurveyText :: TurnoutSurvey -> Text
turnoutSurveyText CESSurvey = "CES"
turnoutSurveyText CPSSurvey = "CPS"

data PSTargets = NoPSTargets | PSTargets deriving stock (Eq, Ord, Show)
psTargetsText :: PSTargets -> Text
psTargetsText NoPSTargets = "noPSTgt"
psTargetsText PSTargets = "PSTgt"

-- we can use the designMatrixRow to get (a -> Vector Double) for prediction
data TurnoutConfig =
  TurnoutConfig
  {
    tSurvey :: TurnoutSurvey
  , tPSTargets :: PSTargets
  , tDesignMatrixRow :: DM.DesignMatrixRow (F.Record DP.PredictorsR)
  , tStateAlphaModel :: StateAlpha
  }

-- for now we model only alpha hierarchically. Beta will be the same everywhere.
data TurnoutPrediction =
  TurnoutPrediction
  {
    tpAlphaMap :: Map Text Double
  , tpBetaSI :: VU.Vector (Double, Double)
  , tpLogisticAdjMapM :: Maybe (Map Text Double)
  } deriving stock (Generic)

deriving anyclass instance Flat.Flat TurnoutPrediction

predictedTurnoutP :: TurnoutConfig -> TurnoutPrediction -> Text -> F.Record DP.PredictorsR -> Either Text Double
predictedTurnoutP tc tp sa p = do
  alpha <- case M.lookup sa tp.tpAlphaMap of
    Nothing -> Left $ "Model.Election2.ModelCommon.predictedP: alphaMap lookup failed for k=" <> sa
    Just x -> pure x
  logisticAdj <- case tp.tpLogisticAdjMapM of
    Nothing -> pure 0
    Just m -> case M.lookup sa m of
      Nothing -> Left $ "Model.Election2.ModelCommon.predictedP: pdLogisticAdjMap lookup failed for k=" <> show sa
      Just x -> pure x
  let covariatesV = DM.designMatrixRowF tc.tDesignMatrixRow p
      invLogit x = 1 / (1 + exp (negate x))
      applySI x (s, i) = i + s * x
  pure $ invLogit (alpha + VU.sum (VU.zipWith applySI covariatesV tp.tpBetaSI) + logisticAdj)

turnoutModelText :: TurnoutConfig  -> Text
turnoutModelText (TurnoutConfig ts tPs dmr am) = "Turnout" <> turnoutSurveyText ts
                                                 <> (if (tPs == PSTargets) then "_PSTgt" else "")
                                                 <> "_" <> dmr.dmName <> "_" <> stateAlphaModelText am

turnoutModelDataText :: TurnoutConfig -> Text
turnoutModelDataText (TurnoutConfig ts tPs dmr _) = "Turnout_" <> turnoutSurveyText ts
                                                    <> (if (tPs == PSTargets) then "_PSTgt" else "")
                                                    <> "_" <> dmr.dmName


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
  , stdStateBallotsCountedVAP :: TE.VectorE
  , stdACSTag :: SMB.RowTypeTag (F.Record DDP.ACSa5ByStateR)
  , stdACSWgts :: TE.IntArrayE
  , stdACSCovariates :: TE.MatrixE
  }

data TurnoutModelData where
  NoPT_CPSTurnoutModelData :: CovariatesAndCounts (F.Record DP.CPSByStateR) -> TurnoutModelData
  NoPT_CESTurnoutModelData :: CovariatesAndCounts (F.Record DP.CESByCDR) -> TurnoutModelData
  PT_CPSTurnoutModelData :: CovariatesAndCounts (F.Record DP.CPSByStateR) -> StateTargetsData (F.Record BRDF.StateTurnoutCols) -> TurnoutModelData
  PT_CESTurnoutModelData :: CovariatesAndCounts (F.Record DP.CESByCDR) -> StateTargetsData (F.Record BRDF.StateTurnoutCols) -> TurnoutModelData


withCC :: (forall a . CovariatesAndCounts a -> b) -> TurnoutModelData -> b
withCC f (NoPT_CPSTurnoutModelData cc) = f cc
withCC f (NoPT_CESTurnoutModelData cc) = f cc
withCC f (PT_CPSTurnoutModelData cc _) = f cc
withCC f (PT_CESTurnoutModelData cc _) = f cc

turnoutModelData :: TurnoutConfig
                 -> SMB.StanBuilderM DP.ModelData gq TurnoutModelData
turnoutModelData tc = do
  let cpsCC = do
        surveyDataTag <- SMB.dataSetTag @(F.Record DP.CPSByStateR) SC.ModelData "SurveyData"
        let (_, nCovariatesE) = DM.designMatrixColDimBinding tc.tDesignMatrixRow Nothing
        dmE <- if DM.rowLength tc.tDesignMatrixRow > 0
          then DM.addDesignMatrix surveyDataTag (contramap F.rcast tc.tDesignMatrixRow) Nothing
          else pure $ TE.namedE "ERROR" TE.SMat -- this shouldn't show up in stan code at all
        trialsE <- SBB.addCountData surveyDataTag "Surveyed" (view DP.surveyed)
        successesE <- SBB.addCountData surveyDataTag "Voted" (view DP.voted)
        pure $ CovariatesAndCounts surveyDataTag nCovariatesE dmE trialsE successesE
      cesCC = do
        surveyDataTag <- SMB.dataSetTag @(F.Record DP.CESByCDR) SC.ModelData "SurveyData"
        let (_, nCovariatesE) = DM.designMatrixColDimBinding tc.tDesignMatrixRow Nothing
        dmE <- if DM.rowLength tc.tDesignMatrixRow > 0
          then DM.addDesignMatrix surveyDataTag (contramap F.rcast tc.tDesignMatrixRow) Nothing
          else pure $ TE.namedE "ERROR" TE.SMat -- this shouldn't show up in stan code at all
        trialsE <- SBB.addCountData surveyDataTag "Surveyed" (view DP.surveyed)
        successesE <- SBB.addCountData surveyDataTag "Voted" (view DP.voted)
        pure $ CovariatesAndCounts surveyDataTag nCovariatesE dmE trialsE successesE
  case tc.tPSTargets of
    NoPSTargets -> case tc.tSurvey of
      CPSSurvey -> fmap NoPT_CPSTurnoutModelData cpsCC
      CESSurvey -> fmap NoPT_CESTurnoutModelData cesCC
    PSTargets -> do
      stateTurnoutTargetTag <- SMB.dataSetTag @(F.Record BRDF.StateTurnoutCols) SC.ModelData "TurnoutTargetData"
      turnoutBallotsCountedVAP <- SBB.addRealData stateTurnoutTargetTag "BallotsCountedVAP" (Just 0) (Just 1) (view BRDF.ballotsCountedVAP)
      acsTag <- SMB.dataSetTag @(F.Record DDP.ACSa5ByStateR) SC.ModelData "ACSData"
      acsWgts <- SBB.addCountData acsTag "ACSWgts" (view DT.popCount)
      acsCovariates <- if DM.rowLength tc.tDesignMatrixRow > 0
          then DM.addDesignMatrix acsTag (contramap F.rcast tc.tDesignMatrixRow) Nothing
          else pure $ TE.namedE "ERROR" TE.SMat -- this shouldn't show up in stan code at all
      let std = StateTargetsData stateTurnoutTargetTag turnoutBallotsCountedVAP acsTag acsWgts acsCovariates
      case tc.tSurvey of
        CPSSurvey -> fmap (\x -> PT_CPSTurnoutModelData x std) cpsCC
        CESSurvey -> fmap (\x -> PT_CESTurnoutModelData x std) cesCC


turnoutTargetsTD :: CovariatesAndCounts a
                 -> StateTargetsData td
                 -> Maybe (TE.MatrixE -> TE.StanName -> SMB.StanBuilderM md gq TE.MatrixE)
                 -> Maybe TE.MatrixE
                 -> SMB.StanBuilderM md gq (TE.MatrixE, TE.IntArrayE)
turnoutTargetsTD cc st cM rM = do
  dmACS' <- case cM of
    Nothing -> pure st.stdACSCovariates
    Just c -> c st.stdACSCovariates "dmACS_Centered"
  dmACS <- case rM of
    Nothing -> pure dmACS'
    Just r -> SMB.inBlock SMB.SBTransformedData $ SMB.addFromCodeWriter $ do
      let rowsE = SMB.dataSetSizeE st.stdACSTag
          colsE = cc.ccNCovariates
      TE.declareRHSNW (TE.NamedDeclSpec "acsDM_QR" $ TE.matrixSpec rowsE colsE [])
           $ dmACS' `TE.timesE` r
  acsNByState <- SMB.inBlock SMB.SBTransformedData $ SMB.addFromCodeWriter $ do
    let nStatesE = SMB.groupSizeE stateG
        nACSRowsE = SMB.dataSetSizeE st.stdACSTag
        acsStateIndex = SMB.byGroupIndexE st.stdACSTag stateG
        plusEq = TE.opAssign TEO.SAdd
        acsNByStateNDS = TE.NamedDeclSpec "acsNByState" $ TE.intArraySpec nStatesE [TE.lowerM $ TE.intE 0]
    acsNByState <- TE.declareRHSNW acsNByStateNDS $ TE.functionE SF.rep_array (TE.intE 0 :> nStatesE :> TNil)
    TE.addStmt
      $ TE.for "k" (TE.SpecificNumbered (TE.intE 1) nACSRowsE) $ \kE ->
      [(TE.indexE TEI.s0 acsStateIndex acsNByState `TE.at` kE) `plusEq` (st.stdACSWgts `TE.at` kE)]
    pure acsNByState
{-  vecOne <-  SMB.inBlock SMB.SBTransformedData
             $ SMB.addFromCodeWriter
             $ TE.declareRHSNW (TE.NamedDeclSpec "vStateOnes")  -}
  pure (dmACS, acsNByState)

turnoutTargetsModel :: ModelParameters
                    -> CovariatesAndCounts a
                    -> StateTargetsData td
                    -> Maybe (TE.MatrixE -> TE.StanName -> SMB.StanBuilderM md gq TE.MatrixE)
                    -> Maybe TE.MatrixE -> SMB.StanBuilderM md gq ()
turnoutTargetsModel mp cc std cM rM = do
  (dmACS, acsNByState) <- turnoutTargetsTD cc std cM rM
  let acsStateIndex = SMB.byGroupIndexE std.stdACSTag stateG
      toVec x = TE.functionE SF.to_vector (x :> TNil)
      pE = probabilitiesExpr mp std.stdACSTag stateG dmACS
  acsPS <- SBB.postStratifiedParameterF False SMB.SBTransformedParameters (Just "psTByState") std.stdACSTag stateG acsStateIndex (toVec std.stdACSWgts) (pure pE) Nothing
  let normalDWA = TE.DensityWithArgs SF.normal (TE.realE 1 :> TE.realE 4 :> TNil)
  sigmaTargetsP <-  DAG.simpleParameterWA
                    (TE.NamedDeclSpec "sigmaTTgt" $ TE.realSpec [TE.lowerM $ TE.realE 1])
                    normalDWA
  SMB.inBlock SMB.SBModel $ SMB.addFromCodeWriter $ do
    let eMult = TE.binaryOpE (TEO.SElementWise TEO.SMultiply)
        eDiv = TE.binaryOpE (TEO.SElementWise TEO.SDivide)
        tP = TE.indexE TEI.s0 (SMB.byGroupIndexE std.stdTargetTypeTag stateG) std.stdStateBallotsCountedVAP
        sd1 = tP `eMult` (TE.realE 1 `TE.minusE` tP) `eDiv` toVec acsNByState
        sd = DAG.parameterExpr sigmaTargetsP `TE.timesE` TE.functionE SF.sqrt (sd1 :> TNil)
    TE.addStmt $ TE.sample acsPS SF.normal (tP :> sd :> TNil)

turnoutPS :: forall l md k . (Typeable (DP.PSDataR k)
                             , F.ElemOf (DP.PSDataR k) DT.PopCount
                             , DP.PredictorsR F.⊆ DP.PSDataR k
                             )
          => ModelParameters
          -> DM.DesignMatrixRow (F.Record DP.PredictorsR)
          -> Maybe (TE.MatrixE -> TE.StanName -> SMB.StanBuilderM md (DP.PSData k) TE.MatrixE)
          -> Maybe TE.MatrixE
          -> SMB.GroupTypeTag l
          -> SMB.StanBuilderM md (DP.PSData k) ()
turnoutPS mp dmr cM rM gtt = do
  psDataTag <- SMB.dataSetTag @(F.Record (DP.PSDataR k)) SC.GQData "PSData"
  let psDataGrpIndex = SMB.byGroupIndexE psDataTag gtt
  psWgts <- SBB.addCountData psDataTag "PSWgts" (view DT.popCount)
  let nCovariates = DM.rowLength dmr
  psCovariates <-  if nCovariates > 0
                   then DM.addDesignMatrix psDataTag (contramap F.rcast dmr) Nothing
                   else pure $ TE.namedE "ERROR" TE.SMat -- this shouldn't show up in stan code at all
  dmPS' <- case cM of
    Nothing -> pure psCovariates
    Just c -> c psCovariates "dmPS_Centered"
  dmPS <- case rM of
    Nothing -> pure dmPS'
    Just r -> SMB.inBlock SMB.SBTransformedDataGQ $ SMB.addFromCodeWriter $ do
      let rowsE = SMB.dataSetSizeE psDataTag
          colsE = SMB.mrfdColumnsE $ DM.matrixFromRowData dmr Nothing
      TE.declareRHSNW (TE.NamedDeclSpec "dmPS_QR" $ TE.matrixSpec rowsE colsE []) $ dmPS' `TE.timesE` r
  let toVec x = TE.functionE SF.to_vector (x :> TNil)
      psPE = probabilitiesExpr mp psDataTag gtt dmPS
  _ <- SBB.postStratifiedParameterF False SMB.SBGeneratedQuantities (Just "tByGrp") psDataTag gtt psDataGrpIndex (toVec psWgts) (pure psPE) Nothing
  pure ()

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
data Alpha where
  SimpleAlpha :: DAG.Parameter TE.EReal -> Alpha
  HierarchicalAlpha :: TE.IntArrayE -> DAG.Parameter TE.ECVec -> Alpha

-- and K predictors
-- theta is K col-vector (or Nothing)
newtype Theta = Theta (Maybe (DAG.Parameter TE.ECVec))

data ModelParameters where
  BinomialLogitModelParameters :: Alpha -> Theta -> ModelParameters
  RealBinomialLogitModelParameters :: Alpha -> Theta -> ModelParameters

paramAlpha :: ModelParameters -> Alpha
paramAlpha (BinomialLogitModelParameters a _) = a
paramAlpha (RealBinomialLogitModelParameters a _) = a

paramTheta :: ModelParameters -> Theta
paramTheta (BinomialLogitModelParameters _ t) = t
paramTheta (RealBinomialLogitModelParameters _ t) = t

modelParameters :: DM.DesignMatrixRow a -> SMB.RowTypeTag a -> StateAlpha -> SMB.StanBuilderM md gq ModelParameters
modelParameters dmr rtt sa = do
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
      indexE <- SMB.getGroupIndexVar rtt stateG
      fmap (HierarchicalAlpha indexE)
        $ DAG.addBuildParameter
        $ DAG.UntransformedP hierAlphaNDS [] alphaPs
        $ \(muAlphaE :> sigmaAlphaE :> TNil) m
          -> TE.addStmt $ TE.sample m SF.normalS (muAlphaE :> sigmaAlphaE :> TNil)
    StateAlphaHierNonCentered -> do
      alphaPs <- hierAlphaPs
      indexE <- SMB.getGroupIndexVar rtt stateG
      let rawNDS = TE.NamedDeclSpec (TE.declName hierAlphaNDS <> "_raw") $ TE.decl hierAlphaNDS
      rawAlphaP <- DAG.simpleParameterWA rawNDS stdNormalDWA
      fmap (HierarchicalAlpha indexE)
        $ DAG.addBuildParameter
        $ DAG.TransformedP hierAlphaNDS []
        (rawAlphaP :> alphaPs) DAG.TransformedParametersBlock
        (\(rawE :> muAlphaE :> muSigmaE :> TNil) -> DAG.DeclRHS $ muAlphaE `TE.plusE` (muSigmaE `TE.timesE` rawE))
        TNil (\_ _ -> pure ())
  pure $ RealBinomialLogitModelParameters alpha theta

data RunConfig l = RunConfig { rcIncludePPCheck :: Bool, rcIncludeLL :: Bool, rcIncludeDMSplits :: Bool, rcTurnoutPS :: Maybe (SMB.GroupTypeTag (F.Record l)) }

probabilitiesExpr :: ModelParameters -> SMB.RowTypeTag a -> SMB.GroupTypeTag b -> TE.MatrixE -> TE.VectorE
probabilitiesExpr mps rtt gtt covariatesM = TE.functionE SF.inv_logit (lp :> TNil)
  where
    stateIndexE = SMB.byGroupIndexE rtt gtt
    lp =
      let aV = case paramAlpha mps of
            SimpleAlpha saP -> TE.functionE SF.rep_vector (DAG.parameterExpr saP :> (SMB.dataSetSizeE rtt) :> TNil)
            HierarchicalAlpha _ haP -> TE.indexE TEI.s0 stateIndexE $ DAG.parameterExpr haP
      in case paramTheta mps of
           (Theta (Just thetaP)) -> aV `TE.plusE` (covariatesM `TE.timesE` DAG.parameterExpr thetaP)
           _ -> aV

-- not returning anything for now
turnoutModel :: (Typeable (DP.PSDataR k)
--                , k F.⊆ DP.PSDataR k
                , F.ElemOf (DP.PSDataR k) DT.PopCount
                , DP.PredictorsR F.⊆ DP.PSDataR k
                )
             => RunConfig l
             -> TurnoutConfig
             -> SMB.StanBuilderM DP.ModelData (DP.PSData k) ()
turnoutModel rc tmc = do
  mData <- turnoutModelData tmc
  mParams <- case mData of
    NoPT_CPSTurnoutModelData cc -> modelParameters (contramap F.rcast tmc.tDesignMatrixRow) cc.ccSurveyDataTag tmc.tStateAlphaModel
    NoPT_CESTurnoutModelData cc -> modelParameters (contramap F.rcast tmc.tDesignMatrixRow) cc.ccSurveyDataTag tmc.tStateAlphaModel
    PT_CPSTurnoutModelData cc _ -> modelParameters (contramap F.rcast tmc.tDesignMatrixRow) cc.ccSurveyDataTag tmc.tStateAlphaModel
    PT_CESTurnoutModelData cc _ -> modelParameters (contramap F.rcast tmc.tDesignMatrixRow) cc.ccSurveyDataTag tmc.tStateAlphaModel

  let --betaNDS = TE.NamedDeclSpec "beta" $ TE.vectorSpec (withCC ccNCovariates mData) []
      nRowsE = withCC (SMB.dataSetSizeE . ccSurveyDataTag) mData
      pExpr = DAG.parameterExpr

  -- to apply the same transformation to another matrix, we center with centerF and then post-multiply by r
  -- or we could apply beta to centered matrix ?
  {-
  (covariatesM, r, centerF, mBeta) <- case paramTheta mParams of
    Theta (Just thetaP) -> do
      (centeredCovariatesE, centerF) <- DM.centerDataMatrix DM.DMCenterOnly (withCC ccCovariates mData) Nothing "DM"
      (dmQ, r, _, mBeta') <- DM.thinQR centeredCovariatesE "DM" $ Just (pExpr thetaP, betaNDS)
      pure (dmQ, r, centerF, mBeta')
    Theta Nothing -> pure (TE.namedE "ERROR" TE.SMat, TE.namedE "ERROR" TE.SMat, \_ x _ -> pure x, Nothing)
-}
  case mData of
    PT_CPSTurnoutModelData cc st -> turnoutTargetsModel mParams cc st Nothing Nothing --(Just $ centerF SC.ModelData) (Just r)
    PT_CESTurnoutModelData cc st -> turnoutTargetsModel mParams cc st Nothing Nothing --(Just $ centerF SC.ModelData) (Just r)
    _ -> pure ()

  -- model
  let --reIndexByState = TE.indexE TEI.s0 $ withCC (\cc -> SMB.byGroupIndexE cc.ccSurveyDataTag stateG) mData
      covariatesM = withCC ccCovariates mData
      lpE :: Alpha -> Theta -> TE.VectorE
      lpE a t =  case a of
       SimpleAlpha alphaP -> case t of
         Theta Nothing -> TE.functionE SF.rep_vector (pExpr alphaP :> nRowsE :> TNil)
         Theta (Just thetaP) -> pExpr alphaP `TE.plusE` (covariatesM `TE.timesE` pExpr thetaP)
       HierarchicalAlpha indexE alpha -> case t of
         Theta Nothing -> TE.indexE TEI.s0 indexE $ pExpr alpha
         Theta (Just thetaP) -> TE.indexE TEI.s0 indexE (pExpr alpha) `TE.plusE` (covariatesM `TE.timesE` pExpr thetaP)

      llF :: SMD.StanDist t pts rts
          -> TE.CodeWriter (TE.IntE -> TE.ExprList pts)
          -> TE.CodeWriter (TE.IntE -> TE.UExpr t)
          -> SMB.StanBuilderM md gq ()
      llF = withCC (SBB.generateLogLikelihood . ccSurveyDataTag) mData

  case mParams of
    BinomialLogitModelParameters a t -> do
      let ssF e = SMB.familySample SMD.binomialLogitDist e (withCC ccTrials mData :> lpE a t :> TNil)
          rpF :: TE.CodeWriter (TE.IntE -> TE.ExprList '[TE.EInt, TE.EReal])
          rpF = pure $ \nE -> withCC ccTrials mData `TE.at` nE :> lpE a t `TE.at` nE :> TNil
          ppF = withCC  (\cc -> SBB.generatePosteriorPrediction cc.ccSurveyDataTag
                                (TE.NamedDeclSpec ("predVotes") $ TE.array1Spec nRowsE $ TE.intSpec [])
                                SMD.binomialLogitDist rpF
                        ) mData
          ll = llF SMD.binomialLogitDist rpF (pure $ \nE -> withCC ccSuccesses mData `TE.at` nE)
      SMB.inBlock SMB.SBModel $ SMB.addFromCodeWriter $ TE.addStmt $ ssF $ withCC ccSuccesses mData
      when rc.rcIncludePPCheck $ void ppF
      when rc.rcIncludeLL ll
    RealBinomialLogitModelParameters a t -> do
      realBinomialLogitDistV <- SMD.realBinomialLogitDistM @TE.ECVec
      realBinomialLogitDistS <- SMD.realBinomialLogitDistSM
      let toVec x = TE.functionE SF.to_vector (x :> TNil)
          ssF e = SMB.familySample realBinomialLogitDistV e (toVec (withCC ccTrials mData) :> lpE a t :> TNil)
          rpF :: TE.CodeWriter (TE.IntE -> TE.ExprList '[TE.EReal, TE.EReal])
          rpF = pure $ \nE -> toVec (withCC ccTrials mData) `TE.at` nE :> lpE a t `TE.at` nE :> TNil
          ppF = withCC (\cc -> SBB.generatePosteriorPrediction cc.ccSurveyDataTag
                         (TE.NamedDeclSpec ("predVotes") $ TE.array1Spec nRowsE $ TE.realSpec [])
                         realBinomialLogitDistS rpF) mData
          ll = llF realBinomialLogitDistS rpF (pure $ \nE -> toVec (withCC ccSuccesses mData) `TE.at` nE)
      SMB.inBlock SMB.SBModel $ SMB.addFromCodeWriter $ TE.addStmt $ ssF $ toVec $ withCC ccSuccesses mData
      when rc.rcIncludePPCheck $ void ppF
      when rc.rcIncludeLL ll

--  SMB.inBlock SMB.SBModel $ SMB.addFromCodeWriter $ TE.addStmt $ sampleStmtF $ withCC ccSuccesses mData
  -- generated quantities

{-  when rc.rcIncludeDMSplits $ case mBeta of
    Nothing -> pure ()
    Just beta -> SMB.inBlock SMB.SBGeneratedQuantities $ do
      _ <- DM.splitToGroupVars tmc.tDesignMatrixRow beta Nothing
      pure()
-}
  case rc.rcTurnoutPS of
    Nothing -> pure ()
    Just gtt -> turnoutPS mParams tmc.tDesignMatrixRow Nothing Nothing gtt --(Just $ centerF SC.GQData) (Just r)
  pure ()




--cwdF :: (F.ElemOf rs DT.PopCount, F.ElemOf rs DT.PWPopPerSqMile) => F.Record rs -> DMS.CellWithDensity
--cwdF r = DMS.CellWithDensity (realToFrac $ r ^. DT.popCount) (r ^. DT.pWPopPerSqMile)

newtype PSMap l a = PSMap { unPSMap :: Map (F.Record l) a}

instance (V.RMap l, Ord (F.Record l), FS.RecFlat l, Flat.Flat a) => Flat.Flat (PSMap l a) where
  size (PSMap m) n = Flat.size (fmap (first  FS.toS) $ M.toList m) n
  encode (PSMap m) = Flat.encode (fmap (first  FS.toS) $ M.toList m)
  decode = (\sl -> PSMap $ M.fromList $ fmap (first FS.fromS) sl) <$> Flat.decode


runModel :: forall l k r .
            (K.KnitEffects r
            , BRKU.CacheEffects r
            , l F.⊆ DP.PSDataR k
            , F.ElemOf (DP.PSDataR k) DT.PopCount
            , DP.PredictorsR F.⊆ DP.PSDataR k
            , V.RMap l
            , Ord (F.Record l)
            , FS.RecFlat l
            , Typeable (DP.PSDataR k)
            , Show (F.Record l)
            , Typeable l
            )
         => Either Text Text
         -> Text
         -> Text
         -> BR.CommandLine
         -> RunConfig l
         -> TurnoutConfig
         -> K.ActionWithCacheTime r DP.ModelData
         -> K.ActionWithCacheTime r (DP.PSData k)
         -> K.Sem r (K.ActionWithCacheTime r (TurnoutPrediction, PSMap l Double))
runModel modelDirE modelName gqName _cmdLine runConfig turnoutConfig modelData_C psData_C = do
  let dataName = turnoutModelDataText turnoutConfig
      runnerInputNames = SC.RunnerInputNames
                         ("br-2023-electionModel/stan/" <> modelName <> "/")
                         (turnoutModelText turnoutConfig)
                         (Just $ SC.GQNames "GQ" (dataName <> "_" <> gqName))
                         dataName
--  modelData <- K.ignoreCacheTime modelData_C
  states <- S.toList . FL.fold (FL.premap (view GT.stateAbbreviation) FL.set) . DP.cesData <$> K.ignoreCacheTime modelData_C
  psKeys <- S.toList . FL.fold (FL.premap (F.rcast @l) FL.set) . DP.unPSData <$> K.ignoreCacheTime psData_C
  (dw, code) <-  case turnoutConfig.tSurvey of
    CESSurvey -> SMR.dataWranglerAndCode modelData_C psData_C
                 (stateGroupBuilder turnoutConfig states psKeys)
                 (turnoutModel runConfig turnoutConfig)
    CPSSurvey -> SMR.dataWranglerAndCode modelData_C psData_C
                 (stateGroupBuilder turnoutConfig states psKeys)
                 (turnoutModel runConfig turnoutConfig)

  let unwraps = [SR.UnwrapNamed "Voted" "yVoted"]

  res_C <- SMR.runModel' @BRKU.SerializerC @BRKU.CacheData
           modelDirE
           (Right runnerInputNames)
           Nothing
           dw
           code
           (modelResultAction turnoutConfig runConfig) --SC.DoNothing -- (stateModelResultAction mcWithId dmr)
           (SMR.Both unwraps) --(SMR.Both [SR.UnwrapNamed "successes" "yObserved"])
           modelData_C
           psData_C
  K.logLE K.Info $ modelName <> " run complete."
  pure res_C

--NB: parsed summary data has stan indexing, i.e., Arrays start at 1.
modelResultAction :: forall k l r .
                     (Ord (F.Record l)
                     , K.KnitEffects r
                     , Typeable (DP.PSDataR k)
                     , Typeable l
                     )
                  => TurnoutConfig
                  -> RunConfig l
                  -> SC.ResultAction r DP.ModelData (DP.PSData k) SMB.DataSetGroupIntMaps () (TurnoutPrediction, PSMap l Double)
modelResultAction turnoutConfig runConfig = SC.UseSummary f where
  f summary _ modelDataAndIndexes_C gqDataAndIndexes_CM = do
    (modelData, resultIndexesE) <- K.ignoreCacheTime modelDataAndIndexes_C
    -- compute means of predictors because model was zero-centered in them
    let covariates = DM.designMatrixRowF (contramap F.rcast turnoutConfig.tDesignMatrixRow)
        nPredictors = DM.rowLength turnoutConfig.tDesignMatrixRow
        mdMeansFld = FL.premap (VU.toList . covariates)
                    $ traverse (\n -> FL.premap (List.!! n) FL.mean) [0..(nPredictors - 1)]
        mdMeansL = case turnoutConfig.tSurvey of
          CESSurvey -> FL.fold (FL.premap (F.rcast @DP.PredictorsR) mdMeansFld) $ DP.cesData modelData
          CPSSurvey -> FL.fold (FL.premap (F.rcast @DP.PredictorsR) mdMeansFld) $ DP.cpsData modelData
    stateIM <- case turnoutConfig.tSurvey of
      CESSurvey -> K.knitEither
                   $ resultIndexesE >>= SMB.getGroupIndex (SMB.RowTypeTag @(F.Record DP.CESByCDR) SC.ModelData "SurveyData") stateG
      CPSSurvey -> K.knitEither
                   $ resultIndexesE >>= SMB.getGroupIndex (SMB.RowTypeTag @(F.Record DP.CPSByStateR) SC.ModelData "SurveyData") stateG
    let allStates = IM.elems stateIM
        getScalar n = K.knitEither $ SP.getScalar . fmap CS.mean <$> SP.parseScalar n (CS.paramStats summary)
        getVector n = K.knitEither $ SP.getVector . fmap CS.mean <$> SP.parse1D n (CS.paramStats summary)
    K.logLE K.Info $ "stateIM=" <> show stateIM
    geoMap <- case turnoutConfig.tStateAlphaModel of
      StateAlphaSimple -> do
        alpha <- getScalar "alpha"
        pure $ M.fromList $ fmap (, alpha) allStates
      _ -> do
        alphaV <- getVector "alpha"
        pure $ M.fromList $ fmap (\(stateIdx, stateAbbr) -> (stateAbbr, alphaV V.! (stateIdx - 1))) $ IM.toList stateIM
    betaSI <- case nPredictors of
      0 -> pure V.empty
      p -> do
        betaV <- getVector "theta"
        pure $ V.fromList $ zip (V.toList betaV) mdMeansL
    psMap <- case runConfig.rcTurnoutPS of
      Nothing -> mempty
      Just gtt -> case gqDataAndIndexes_CM of
        Nothing -> K.knitError "modelResultAction: Expected gq data and indexes but got Nothing."
        Just gqDaI_C -> do
          (_, gqIndexesE) <- K.ignoreCacheTime gqDaI_C
          grpIM <- K.knitEither
             $ gqIndexesE >>= SMB.getGroupIndex (SMB.RowTypeTag @(F.Record (DP.PSDataR k)) SC.GQData "PSData") (psGroupTag @l)
          psTByGrpV <- getVector "tByGrp"
          pure $ M.fromList $ zip (IM.elems grpIM) $ V.toList psTByGrpV
    pure $ (TurnoutPrediction geoMap (VU.convert betaSI) Nothing, PSMap psMap)

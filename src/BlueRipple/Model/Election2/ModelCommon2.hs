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

module BlueRipple.Model.Election2.ModelCommon2
  (
    module BlueRipple.Model.Election2.ModelCommon2
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
import qualified BlueRipple.Data.ModelingTypes as MT
import qualified BlueRipple.Model.Election2.ModelCommon as MC


import qualified Knit.Report as K hiding (elements)


import qualified Control.Foldl as FL
import Control.Lens (view, (^.))
import qualified Data.IntMap.Strict as IM
import qualified Data.Map.Strict as M
import qualified Data.Set as S

import qualified Data.Dependent.Sum as DSum
import qualified Data.Dependent.HashMap as DHash


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
import qualified Stan.ModelBuilder.BuildingBlocks.GroupAlpha as SG
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

--stateG :: SMB.GroupTypeTag Text
--stateG = SMB.GroupTypeTag "State"

ageG :: SMB.GroupTypeTag DT.Age5
ageG = SMB.GroupTypeTag "Age"

sexG :: SMB.GroupTypeTag DT.Sex
sexG = SMB.GroupTypeTag "Sex"

eduG :: SMB.GroupTypeTag DT.Education4
eduG = SMB.GroupTypeTag "Edu"

raceG :: SMB.GroupTypeTag DT.Race5
raceG = SMB.GroupTypeTag "Race"

--psGroupTag :: forall k . Typeable k => SMB.GroupTypeTag (F.Record k)
--psGroupTag = SMB.GroupTypeTag "PSGrp"

type GroupsR = GT.StateAbbreviation ': DP.DCatsR

groups :: Foldable g => g Text -> [DSum.DSum SMB.GroupTypeTag (SG.GroupFromData (F.Record GroupsR))]
groups states = [MC.stateG DSum.:=>
           SG.GroupFromData (view GT.stateAbbreviation)
           (SMB.makeIndexFromFoldable show (view GT.stateAbbreviation) states)
           (SMB.dataToIntMapFromFoldable (view GT.stateAbbreviation) states)
         , ageG DSum.:=> SG.groupFromDataEnum (view DT.age5C)
         , sexG DSum.:=> SG.groupFromDataEnum (view DT.sexC)
         , eduG DSum.:=> SG.groupFromDataEnum (view DT.education4C)
         , raceG DSum.:=> SG.groupFromDataEnum (view DT.race5C)
         ]

-- NB: often l ~ k, e.g., for predicting district turnout/preference
-- But say you want to predict turnout by race, nationally.
-- Now l ~ '[Race5C]
-- How about turnout by Education in each state? Then l ~ [StateAbbreviation, Education4C]
groupBuilder :: forall g k l a b .
                     (Foldable g
                     , Typeable (DP.PSDataR k)
                     , Show (F.Record l)
                     , Ord (F.Record l)
                     , l F.⊆ DP.PSDataR k
                     , Typeable l
                     , F.ElemOf (DP.PSDataR k) GT.StateAbbreviation
                     , DP.DCatsR F.⊆ DP.PSDataR k
                     )
                  => TurnoutConfig a b
                  -> g Text
                  -> g (F.Record l)
                  -> SMB.StanGroupBuilderM DP.ModelData (DP.PSData k) ()
groupBuilder turnoutConfig states psKeys = do
  let groups' = groups states
      addBoth :: (GroupsR F.⊆ rs)  => SMB.RowTypeTag (F.Record rs) -> SMB.StanGroupBuilderM DP.ModelData gq ()
      addBoth dataTag = do
        SG.addModelIndexes dataTag F.rcast groups'
        SG.addGroupIntMaps dataTag F.rcast groups'
  -- the return type must be explcit here so GHC knows the GADT type parameter does not escape its scope
  () <- case turnoutConfig.tSurvey of
    MC.CESSurvey -> SMB.addModelDataToGroupBuilder "SurveyData" (SMB.ToFoldable DP.cesData) >>= addBoth
    MC.CPSSurvey -> SMB.addModelDataToGroupBuilder "SurveyData" (SMB.ToFoldable DP.cpsData) >>= addBoth
  case turnoutConfig.tPSTargets of
    MC.NoPSTargets -> pure ()
    MC.PSTargets -> do
      turnoutTargetDataTag <- SMB.addModelDataToGroupBuilder "TurnoutTargetData" (SMB.ToFoldable DP.stateTurnoutData)
      SMB.addGroupIndexForData MC.stateG turnoutTargetDataTag $ SMB.makeIndexFromFoldable show (view GT.stateAbbreviation) states
      acsDataTag <- SMB.addModelDataToGroupBuilder "ACSData" (SMB.ToFoldable DP.acsData)
      SMB.addGroupIndexForData MC.stateG acsDataTag $ SMB.makeIndexFromFoldable show (view GT.stateAbbreviation) states
  let psGtt = MC.psGroupTag @l
  psTag <- SMB.addGQDataToGroupBuilder "PSData" (SMB.ToFoldable DP.unPSData)
  SMB.addGroupIndexForData psGtt psTag $ SMB.makeIndexFromFoldable show F.rcast psKeys
  SMB.addGroupIntMapForDataSet psGtt psTag $ SMB.dataToIntMapFromFoldable F.rcast psKeys
  SG.addModelIndexes psTag F.rcast groups'

-- design matrix rows
safeLog :: Double -> Double
safeLog x = if x >= 1 then Numeric.log x else 0

tDesignMatrixRow_d :: DM.DesignMatrixRow (F.Record DP.PredictorsR)
tDesignMatrixRow_d = DM.DesignMatrixRow "d" [dRP]
  where
    dRP = DM.DesignMatrixRowPart "logDensity" 1 (VU.singleton . safeLog . view DT.pWPopPerSqMile)

-- we can use the designMatrixRow to get (a -> Vector Double) for prediction
data TurnoutConfig a (b :: TE.EType) =
  TurnoutConfig
  {
    tSurvey :: MC.TurnoutSurvey a
  , tSurveyAggregation :: MC.SurveyAggregation b
  , tPSTargets :: MC.PSTargets
  , tAlphas :: Alphas
  , tDesignMatrixRow :: DM.DesignMatrixRow (F.Record DP.PredictorsR)
  }

data Alphas = StH_A_S_E_R

alphasText :: Alphas -> Text
alphasText StH_A_S_E_R = "StH_A_S_E_R"

{-
data ModelType = TurnoutMT | RegistrationMT | PreferenceMT | FullMT deriving stock (Eq, Ord, Show)

data TurnoutSurvey a where
  CESSurvey :: TurnoutSurvey (F.Record DP.CESByCDR)
  CPSSurvey :: TurnoutSurvey (F.Record DP.CPSByStateR)

data SurveyAggregation b where
  UnweightedAggregation :: SurveyAggregation TE.EIntArray
  WeightedAggregation :: SurveyAggregation TE.ECVec

turnoutSurveyText :: TurnoutSurvey a -> Text
turnoutSurveyText CESSurvey = "CES"
turnoutSurveyText CPSSurvey = "CPS"

data PSTargets = NoPSTargets | PSTargets deriving stock (Eq, Ord, Show)
psTargetsText :: PSTargets -> Text
psTargetsText NoPSTargets = "noPSTgt"
psTargetsText PSTargets = "PSTgt"
-}

{-
-- for now we model only alpha hierarchically. Beta will be the same everywhere.
data TurnoutPrediction =
  TurnoutPrediction
  {
    tpAlphaMap :: Map Text Double
  , tpBetaSI :: VU.Vector (Double, Double)
  , tpLogisticAdjMapM :: Maybe (Map Text Double)
  } deriving stock (Generic)

deriving anyclass instance Flat.Flat TurnoutPrediction

predictedTurnoutP :: TurnoutConfig a b -> TurnoutPrediction -> Text -> F.Record DP.PredictorsR -> Either Text Double
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
-}

{-
addAggregationText :: SurveyAggregation b -> Text
addAggregationText UnweightedAggregation = ""
addAggregationText WeightedAggregation = "_WA"
-}

turnoutModelText :: TurnoutConfig a b -> Text
turnoutModelText (TurnoutConfig ts tsa tPs alphas dmr) = "Turnout" <> MC.turnoutSurveyText ts
                                                         <> (if (tPs == MC.PSTargets) then "_PSTgt" else "")
                                                         <> MC.addAggregationText tsa
                                                         <> "_" <> alphasText alphas <> "_" <> dmr.dmName

turnoutModelDataText :: TurnoutConfig a b -> Text
turnoutModelDataText (TurnoutConfig ts tsa tPs alphas dmr) = "Turnout" <> MC.turnoutSurveyText ts
                                                             <> (if (tPs == MC.PSTargets) then "_PSTgt" else "")
                                                             <> MC.addAggregationText tsa
                                                             <> "_" <> alphasText alphas <> "_" <> dmr.dmName


{-
data CovariatesAndCounts a (b :: TE.EType) =
  CovariatesAndCounts
  {
    ccSurveyDataTag :: SMB.RowTypeTag a
  , ccNCovariates :: TE.IntE
  , ccCovariates :: TE.MatrixE
  , ccTrials :: TE.UExpr b
  , ccSuccesses :: TE.UExpr b
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

data TurnoutModelData a (b :: TE.EType) where
  NoPT_TurnoutModelData :: CovariatesAndCounts a b -> TurnoutModelData a b
  PT_TurnoutModelData :: CovariatesAndCounts a b -> StateTargetsData (F.Record BRDF.StateTurnoutCols) -> TurnoutModelData a b

withCC :: (forall a b . CovariatesAndCounts a b -> c) -> TurnoutModelData a b -> c
withCC f (NoPT_TurnoutModelData cc) = f cc
withCC f (PT_TurnoutModelData cc _) = f cc
-}
turnoutModelData :: forall a b gq . TurnoutConfig a b
                 -> SMB.StanBuilderM DP.ModelData gq (MC.TurnoutModelData a b)
turnoutModelData tc = do
  let cpsCC_UW :: SMB.StanBuilderM DP.ModelData gq (MC.CovariatesAndCounts (F.Record DP.CPSByStateR) TE.EIntArray)
      cpsCC_UW = do
        surveyDataTag <- SMB.dataSetTag @(F.Record DP.CPSByStateR) SC.ModelData "SurveyData"
        let (_, nCovariatesE) = DM.designMatrixColDimBinding tc.tDesignMatrixRow Nothing
        dmE <- if DM.rowLength tc.tDesignMatrixRow > 0
          then DM.addDesignMatrix surveyDataTag (contramap F.rcast tc.tDesignMatrixRow) Nothing
          else pure $ TE.namedE "ERROR" TE.SMat -- this shouldn't show up in stan code at all
        trialsE <- SBB.addCountData surveyDataTag "Surveyed" (view DP.surveyed)
        successesE <- SBB.addCountData surveyDataTag "Voted" (view DP.voted)
        pure $ MC.CovariatesAndCounts surveyDataTag nCovariatesE dmE trialsE successesE
      cpsCC_W :: SMB.StanBuilderM DP.ModelData gq (MC.CovariatesAndCounts (F.Record DP.CPSByStateR) TE.ECVec)
      cpsCC_W = do
        surveyDataTag <- SMB.dataSetTag @(F.Record DP.CPSByStateR) SC.ModelData "SurveyData"
        let (_, nCovariatesE) = DM.designMatrixColDimBinding tc.tDesignMatrixRow Nothing
        dmE <- if DM.rowLength tc.tDesignMatrixRow > 0
          then DM.addDesignMatrix surveyDataTag (contramap F.rcast tc.tDesignMatrixRow) Nothing
          else pure $ TE.namedE "ERROR" TE.SMat -- this shouldn't show up in stan code at all
        trialsE <- SBB.addRealData surveyDataTag "Surveyed" (Just 0.99) Nothing (view DP.surveyedW)
        successesE <- SBB.addRealData surveyDataTag "Voted" (Just 0) Nothing (view DP.votedW)
        pure $ MC.CovariatesAndCounts surveyDataTag nCovariatesE dmE trialsE successesE
      cesCC_UW :: SMB.StanBuilderM DP.ModelData gq (MC.CovariatesAndCounts (F.Record DP.CESByCDR) TE.EIntArray)
      cesCC_UW = do
        surveyDataTag <- SMB.dataSetTag @(F.Record DP.CESByCDR) SC.ModelData "SurveyData"
        let (_, nCovariatesE) = DM.designMatrixColDimBinding tc.tDesignMatrixRow Nothing
        dmE <- if DM.rowLength tc.tDesignMatrixRow > 0
          then DM.addDesignMatrix surveyDataTag (contramap F.rcast tc.tDesignMatrixRow) Nothing
          else pure $ TE.namedE "ERROR" TE.SMat -- this shouldn't show up in stan code at all
        trialsE <- SBB.addCountData surveyDataTag "Surveyed" (view DP.surveyed)
        successesE <- SBB.addCountData surveyDataTag "Voted" (view DP.voted)
        pure $ MC.CovariatesAndCounts surveyDataTag nCovariatesE dmE trialsE successesE
      cesCC_W :: SMB.StanBuilderM DP.ModelData gq (MC.CovariatesAndCounts (F.Record DP.CESByCDR) TE.ECVec)
      cesCC_W = do
        surveyDataTag <- SMB.dataSetTag @(F.Record DP.CESByCDR) SC.ModelData "SurveyData"
        let (_, nCovariatesE) = DM.designMatrixColDimBinding tc.tDesignMatrixRow Nothing
        dmE <- if DM.rowLength tc.tDesignMatrixRow > 0
          then DM.addDesignMatrix surveyDataTag (contramap F.rcast tc.tDesignMatrixRow) Nothing
          else pure $ TE.namedE "ERROR" TE.SMat -- this shouldn't show up in stan code at all
        trialsE <- SBB.addRealData surveyDataTag "Surveyed" (Just 0.99) Nothing (view DP.surveyedW)
        successesE <- SBB.addRealData surveyDataTag "Voted" (Just 0) Nothing (view DP.votedW)
        pure $ MC.CovariatesAndCounts surveyDataTag nCovariatesE dmE trialsE successesE
  case tc.tPSTargets of
    MC.NoPSTargets -> case tc.tSurvey of
      MC.CPSSurvey -> case tc.tSurveyAggregation of
        MC.UnweightedAggregation -> fmap MC.NoPT_TurnoutModelData cpsCC_UW
        MC.WeightedAggregation -> fmap MC.NoPT_TurnoutModelData cpsCC_W
      MC.CESSurvey -> case tc.tSurveyAggregation of
        MC.UnweightedAggregation -> fmap MC.NoPT_TurnoutModelData cesCC_UW
        MC.WeightedAggregation -> fmap MC.NoPT_TurnoutModelData cesCC_W
    MC.PSTargets -> do
      stateTurnoutTargetTag <- SMB.dataSetTag @(F.Record BRDF.StateTurnoutCols) SC.ModelData "TurnoutTargetData"
      turnoutBallotsCountedVAP <- SBB.addRealData stateTurnoutTargetTag "BallotsCountedVAP" (Just 0) (Just 1) (view BRDF.ballotsCountedVAP)
      acsTag <- SMB.dataSetTag @(F.Record DDP.ACSa5ByStateR) SC.ModelData "ACSData"
      acsWgts <- SBB.addCountData acsTag "ACSWgts" (view DT.popCount)
      acsCovariates <- if DM.rowLength tc.tDesignMatrixRow > 0
          then DM.addDesignMatrix acsTag (contramap F.rcast tc.tDesignMatrixRow) Nothing
          else pure $ TE.namedE "ERROR" TE.SMat -- this shouldn't show up in stan code at all
      let std = MC.StateTargetsData stateTurnoutTargetTag turnoutBallotsCountedVAP acsTag acsWgts acsCovariates
      case tc.tSurvey of
        MC.CPSSurvey -> case tc.tSurveyAggregation of
          MC.UnweightedAggregation -> fmap (\x -> MC.PT_TurnoutModelData x std) cpsCC_UW
          MC.WeightedAggregation -> fmap (\x -> MC.PT_TurnoutModelData x std) cpsCC_W
        MC.CESSurvey -> case tc.tSurveyAggregation of
          MC.UnweightedAggregation -> fmap (\x -> MC.PT_TurnoutModelData x std) cesCC_UW
          MC.WeightedAggregation -> fmap (\x -> MC.PT_TurnoutModelData x std) cesCC_W
{-
turnoutTargetsTD :: CovariatesAndCounts a b
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
  pure (dmACS, acsNByState)
-}
stdNormalDWA :: (TE.TypeOneOf t [TE.EReal, TE.ECVec, TE.ERVec], TE.GenSType t) => TE.DensityWithArgs t
stdNormalDWA = TE.DensityWithArgs SF.std_normal TNil

setupAlphaSum :: Alphas -> SMB.StanBuilderM md gq (SG.AlphaByDataVecCW md gq)
setupAlphaSum alphas = do
  let nStatesE = SMB.groupSizeE MC.stateG
      alphaNDS n t = TE.NamedDeclSpec ("a" <> t) $ TE.vectorSpec n []
      hierAlphaPs t = do
        muAlphaP <- DAG.simpleParameterWA
                    (TE.NamedDeclSpec ("mu" <> t) $ TE.realSpec [])
                    stdNormalDWA
        sigmaAlphaP <-  DAG.simpleParameterWA
                        (TE.NamedDeclSpec ("sigma" <> t) $ TE.realSpec [TE.lowerM $ TE.realE 0])
                        stdNormalDWA
        pure (muAlphaP :> sigmaAlphaP :> TNil)
      aStBP :: DAG.Parameters [TE.EReal, TE.EReal] -> DAG.BuildParameter TE.ECVec
      aStBP hps = DAG.UntransformedP (alphaNDS (SMB.groupSizeE MC.stateG) "St") [] hps
                  $ \(muAlphaE :> sigmaAlphaE :> TNil) m
                    -> TE.addStmt $ TE.sample m SF.normalS (muAlphaE :> sigmaAlphaE :> TNil)
  case alphas of
    StH_A_S_E_R -> do
      stAG <- fmap (\hps -> SG.firstOrderAlpha MC.stateG (aStBP hps)) $ hierAlphaPs "St"
      let stdNormalBP nds =  DAG.UntransformedP nds [] TNil (\TNil m -> TE.addStmt $ TE.sample m SF.std_normal TNil)
          ageAG = SG.firstOrderAlphaDC ageG DT.A5_45To64 (stdNormalBP $ alphaNDS (SMB.groupSizeE ageG `TE.minusE` TE.intE 1) "age")
          sexAG = SG.binaryAlpha sexG (stdNormalBP $ TE.NamedDeclSpec ("aSex") $ TE.realSpec [])
          eduAG = SG.firstOrderAlphaDC eduG DT.E4_HSGrad (stdNormalBP $ alphaNDS (SMB.groupSizeE eduG `TE.minusE` TE.intE 1) "edu")
          raceAG = SG.firstOrderAlphaDC raceG DT.R5_WhiteNonHispanic (stdNormalBP $ alphaNDS (SMB.groupSizeE raceG `TE.minusE` TE.intE 1) "race")
      SG.setupAlphaSum (stAG :| [ageAG, sexAG, eduAG, raceAG])


setupBeta :: TurnoutConfig a b -> SMB.StanBuilderM md gq (Maybe TE.VectorE)
setupBeta tc = do
  let dmr = tc.tDesignMatrixRow
      (_, nCovariatesE) = DM.designMatrixColDimBinding dmr Nothing
  betaM <- if DM.rowLength dmr > 0 then
               (Just . DAG.parameterExpr)
               <$> DAG.simpleParameterWA
               (TE.NamedDeclSpec "beta" $ TE.vectorSpec nCovariatesE [])
               stdNormalDWA
             else pure $ Nothing
  pure betaM

data ParameterSetup md gq = LogitSetup (SG.AlphaByDataVecCW md gq) (Maybe TE.VectorE)

hasBeta :: ParameterSetup md gq -> Bool
hasBeta (LogitSetup _ mb) = isJust mb

setupParameters :: TurnoutConfig a b -> SMB.StanBuilderM md gq (ParameterSetup md gq)
setupParameters tc = do
  as <- setupAlphaSum tc.tAlphas
  bs <- setupBeta tc
  pure $ LogitSetup as bs

{-
modelParameters :: DM.DesignMatrixRow a -> SMB.RowTypeTag a -> StateAlpha -> SMB.StanBuilderM md gq ModelParameters
modelParameters dmr rtt sa = do
  let
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
  pure $ BinomialLogitModelParameters alpha theta
-}


logitProbCW :: ParameterSetup md gq -> SMB.RowTypeTag a -> TE.MatrixE -> SMB.StanBuilderM md gq (TE.CodeWriter TE.VectorE)
logitProbCW ps rtt covM =
  case ps of
    LogitSetup (SG.AlphaByDataVecCW f) mBeta -> do
      alphaSumCW <- f rtt
      case mBeta of
        Nothing -> pure alphaSumCW
        Just beta -> pure $ do
          alphaSum <- alphaSumCW
          pure $ alphaSum `TE.plusE` (covM `TE.timesE` beta)


probabilitiesCW :: ParameterSetup md gq -> SMB.RowTypeTag a -> TE.MatrixE -> SMB.StanBuilderM md gq (TE.CodeWriter TE.VectorE)
probabilitiesCW ps rtt covM = fmap (\x -> TE.functionE SF.inv_logit (x :> TNil)) <$> (logitProbCW ps rtt covM)


turnoutTargetsModel :: ParameterSetup md gq
                    -> MC.CovariatesAndCounts a b
                    -> MC.StateTargetsData td
                    -> Maybe (TE.MatrixE -> TE.StanName -> SMB.StanBuilderM md gq TE.MatrixE)
                    -> Maybe TE.MatrixE
                    -> SMB.StanBuilderM md gq ()
turnoutTargetsModel ps cc std cM rM = do
  (dmACS, acsNByState) <- MC.turnoutTargetsTD cc std cM rM
  let acsStateIndex = SMB.byGroupIndexE std.stdACSTag MC.stateG
      toVec x = TE.functionE SF.to_vector (x :> TNil)
  pCW <- probabilitiesCW ps std.stdACSTag dmACS
  acsPS <- SBB.postStratifiedParameterF False SMB.SBTransformedParameters (Just "psTByState") std.stdACSTag MC.stateG acsStateIndex (toVec std.stdACSWgts) pCW Nothing
  let normalDWA = TE.DensityWithArgs SF.normal (TE.realE 1 :> TE.realE 4 :> TNil)
  sigmaTargetsP <-  DAG.simpleParameterWA
                    (TE.NamedDeclSpec "sigmaTTgt" $ TE.realSpec [TE.lowerM $ TE.realE 1])
                    normalDWA
  SMB.inBlock SMB.SBModel $ SMB.addFromCodeWriter $ do
    let eMult = TE.binaryOpE (TEO.SElementWise TEO.SMultiply)
        eDiv = TE.binaryOpE (TEO.SElementWise TEO.SDivide)
        tP = TE.indexE TEI.s0 (SMB.byGroupIndexE std.stdTargetTypeTag MC.stateG) std.stdStateBallotsCountedVAP
        sd1 = tP `eMult` (TE.realE 1 `TE.minusE` tP) `eDiv` toVec acsNByState
        sd = DAG.parameterExpr sigmaTargetsP `TE.timesE` TE.functionE SF.sqrt (sd1 :> TNil)
    TE.addStmt $ TE.sample acsPS SF.normal (tP :> sd :> TNil)


turnoutPS :: forall l md k . (Typeable (DP.PSDataR k)
                             , F.ElemOf (DP.PSDataR k) DT.PopCount
                             , DP.PredictorsR F.⊆ DP.PSDataR k
                             )
          => ParameterSetup md (DP.PSData k)
          -> DM.DesignMatrixRow (F.Record DP.PredictorsR)
          -> Maybe (TE.MatrixE -> TE.StanName -> SMB.StanBuilderM md (DP.PSData k) TE.MatrixE)
          -> Maybe TE.MatrixE
          -> SMB.GroupTypeTag l
          -> SMB.StanBuilderM md (DP.PSData k) ()
turnoutPS ps dmr cM rM gtt = do
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
  psCW <- probabilitiesCW ps psDataTag dmPS
  _ <- SBB.postStratifiedParameterF False SMB.SBGeneratedQuantities (Just "tByGrp") psDataTag gtt psDataGrpIndex (toVec psWgts) psCW Nothing
  pure ()


--data RunConfig l = RunConfig { rcIncludePPCheck :: Bool, rcIncludeLL :: Bool, rcIncludeDMSplits :: Bool, rcTurnoutPS :: Maybe (SMB.GroupTypeTag (F.Record l)) }


-- not returning anything for now
turnoutModel :: (Typeable (DP.PSDataR k)
--                , k F.⊆ DP.PSDataR k
                , F.ElemOf (DP.PSDataR k) DT.PopCount
                , DP.PredictorsR F.⊆ DP.PSDataR k
                )
             => MC.RunConfig l
             -> TurnoutConfig a b
             -> SMB.StanBuilderM DP.ModelData (DP.PSData k) ()
turnoutModel rc tmc = do
  mData <- turnoutModelData tmc
  paramSetup <- setupParameters tmc
  let nRowsE = MC.withCC (SMB.dataSetSizeE . MC.ccSurveyDataTag) mData
      pExpr = DAG.parameterExpr

  (covariatesM, centerF) <- case hasBeta paramSetup of
    True -> do
      (centeredCovariatesE, centerF) <- DM.centerDataMatrix DM.DMCenterOnly (MC.withCC MC.ccCovariates mData) Nothing "DM"
      pure (centeredCovariatesE, centerF)
    False -> pure (TE.namedE "ERROR" TE.SMat, \_ x _ -> pure x)
{-
  (covariatesM, r, centerF, mBeta) <- case paramTheta mParams of
    Theta (Just thetaP) -> do
      (centeredCovariatesE, centerF) <- DM.centerDataMatrix DM.DMCenterOnly (withCC ccCovariates mData) Nothing "DM"
      (dmQ, r, _, mBeta') <- DM.thinQR centeredCovariatesE "DM" $ Just (pExpr thetaP, betaNDS)
      pure (dmQ, r, centerF, mBeta')
    Theta Nothing -> pure (TE.namedE "ERROR" TE.SMat, TE.namedE "ERROR" TE.SMat, \_ x _ -> pure x, Nothing)
-}
  case mData of
    MC.PT_TurnoutModelData cc st -> turnoutTargetsModel paramSetup cc st (Just $ centerF SC.ModelData) Nothing
    _ -> pure ()

  -- model
  let --covariatesM = MC.withCC MC.ccCovariates mData
  lpCW <- MC.withCC (\cc -> logitProbCW paramSetup cc.ccSurveyDataTag covariatesM) mData
  let llF :: SMD.StanDist t pts rts
          -> TE.CodeWriter (TE.IntE -> TE.ExprList pts)
          -> TE.CodeWriter (TE.IntE -> TE.UExpr t)
          -> SMB.StanBuilderM md gq ()
      llF = MC.withCC (SBB.generateLogLikelihood . MC.ccSurveyDataTag) mData

  () <- case tmc.tSurveyAggregation of
    MC.UnweightedAggregation -> do
      let model :: SMB.RowTypeTag a -> TE.IntArrayE -> TE.IntArrayE -> SMB.StanBuilderM md gq ()
          model rtt n k = do
            let ssF lp e = SMB.familySample (SMD.binomialLogitDist @TE.EIntArray) e (n :> lp :> TNil)
                rpF :: TE.CodeWriter (TE.IntE -> TE.ExprList '[TE.EInt, TE.EReal])
                rpF = (\x -> (\nE -> n `TE.at` nE :> x `TE.at` nE :> TNil)) <$> lpCW
                ppF = SBB.generatePosteriorPrediction rtt
                      (TE.NamedDeclSpec ("predVotes") $ TE.array1Spec nRowsE $ TE.intSpec [])
                      SMD.binomialLogitDist rpF
                ll = llF SMD.binomialLogitDist rpF (pure $ \nE -> k `TE.at` nE)
            SMB.inBlock SMB.SBModel $ SMB.addFromCodeWriter $ do
              lp <- lpCW
              TE.addStmt $ ssF lp k
            when rc.rcIncludePPCheck $ void ppF
            when rc.rcIncludeLL ll
      case mData of
        MC.NoPT_TurnoutModelData cc -> model cc.ccSurveyDataTag cc.ccTrials cc.ccSuccesses
        MC.PT_TurnoutModelData cc _ -> model cc.ccSurveyDataTag cc.ccTrials cc.ccSuccesses
    MC.WeightedAggregation -> do
      realBinomialLogitDistV <- SMD.realBinomialLogitDistM @TE.ECVec
      realBinomialLogitDistS <- SMD.realBinomialLogitDistSM
      let model ::  SMB.RowTypeTag a -> TE.VectorE -> TE.VectorE -> SMB.StanBuilderM md gq ()
          model rtt n k = do
            let ssF lp e = SMB.familySample realBinomialLogitDistV e (n :> lp :> TNil)
                rpF :: TE.CodeWriter (TE.IntE -> TE.ExprList '[TE.EReal, TE.EReal])
                rpF = (\x -> (\nE -> n `TE.at` nE :> x `TE.at` nE :> TNil)) <$> lpCW
                ppF = SBB.generatePosteriorPrediction rtt
                      (TE.NamedDeclSpec ("predVotes") $ TE.array1Spec nRowsE $ TE.realSpec [])
                      realBinomialLogitDistS rpF
                ll = llF realBinomialLogitDistS rpF (pure $ \nE -> k `TE.at` nE)
            SMB.inBlock SMB.SBModel $ SMB.addFromCodeWriter $ do
              lp <- lpCW
              TE.addStmt $ ssF lp k
            when rc.rcIncludePPCheck $ void ppF
            when rc.rcIncludeLL ll
      case mData of
        MC.NoPT_TurnoutModelData cc -> model cc.ccSurveyDataTag cc.ccTrials cc.ccSuccesses
        MC.PT_TurnoutModelData cc _ -> model cc.ccSurveyDataTag cc.ccTrials cc.ccSuccesses
  case rc.rcTurnoutPS of
    Nothing -> pure ()
    Just gtt -> turnoutPS paramSetup tmc.tDesignMatrixRow (Just $ centerF SC.GQData) Nothing gtt --(Just $ centerF SC.GQData) (Just r)
  pure ()
{-
newtype PSMap l a = PSMap { unPSMap :: Map (F.Record l) a}

instance (V.RMap l, Ord (F.Record l), FS.RecFlat l, Flat.Flat a) => Flat.Flat (PSMap l a) where
  size (PSMap m) n = Flat.size (fmap (first  FS.toS) $ M.toList m) n
  encode (PSMap m) = Flat.encode (fmap (first  FS.toS) $ M.toList m)
  decode = (\sl -> PSMap $ M.fromList $ fmap (first FS.fromS) sl) <$> Flat.decode
-}
runModel :: forall l k r a b .
            (K.KnitEffects r
            , BRKU.CacheEffects r
            , l F.⊆ DP.PSDataR k
            , F.ElemOf (DP.PSDataR k) DT.PopCount
            , DP.PredictorsR F.⊆ DP.PSDataR k
            , V.RMap l
            , Ord (F.Record l)
            , FS.RecFlat l
            , Typeable (DP.PSDataR k)
            , F.ElemOf (DP.PSDataR k) GT.StateAbbreviation
            , DP.DCatsR F.⊆ DP.PSDataR k
            , Show (F.Record l)
            , Typeable l
            )
         => Either Text Text
         -> Text
         -> Text
         -> BR.CommandLine
         -> MC.RunConfig l
         -> TurnoutConfig a b
         -> K.ActionWithCacheTime r DP.ModelData
         -> K.ActionWithCacheTime r (DP.PSData k)
         -> K.Sem r (K.ActionWithCacheTime r (MC.PSMap l MT.ConfidenceInterval))
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
    MC.CESSurvey -> SMR.dataWranglerAndCode modelData_C psData_C
                    (groupBuilder turnoutConfig states psKeys)
                    (turnoutModel runConfig turnoutConfig)
    MC.CPSSurvey -> SMR.dataWranglerAndCode modelData_C psData_C
                    (groupBuilder turnoutConfig states psKeys)
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
modelResultAction :: forall k l r a b .
                     (Ord (F.Record l)
                     , K.KnitEffects r
                     , Typeable (DP.PSDataR k)
                     , Typeable l
                     )
                  => TurnoutConfig a b
                  -> MC.RunConfig l
                  -> SC.ResultAction r DP.ModelData (DP.PSData k) SMB.DataSetGroupIntMaps () (MC.PSMap l MT.ConfidenceInterval)
modelResultAction turnoutConfig runConfig = SC.UseSummary f where
  f summary _ modelDataAndIndexes_C gqDataAndIndexes_CM = do
    (modelData, resultIndexesE) <- K.ignoreCacheTime modelDataAndIndexes_C
    -- compute means of predictors because model was zero-centered in them
{-
    let mdMeansFld :: DM.DesignMatrixRow (F.Record rs) -> FL.Fold (F.Record rs) [Double]
        mdMeansFld dmr =
          let  covariates = DM.designMatrixRowF dmr
               nPredictors = DM.rowLength dmr
          in FL.premap (VU.toList . covariates)
             $ traverse (\n -> FL.premap (List.!! n) FL.mean) [0..(nPredictors - 1)]
        mdMeansL = case turnoutConfig.tSurvey of
          CESSurvey -> FL.fold (FL.premap (F.rcast @DP.PredictorsR) $ mdMeansFld turnoutConfig.tDesignMatrixRow) $ DP.cesData modelData
          CPSSurvey -> FL.fold (FL.premap (F.rcast @DP.PredictorsR) $ mdMeansFld turnoutConfig.tDesignMatrixRow) $ DP.cpsData modelData
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
    betaSI <- case DM.rowLength turnoutConfig.tDesignMatrixRow of
      0 -> pure V.empty
      p -> do
        betaV <- getVector "theta"
        pure $ V.fromList $ zip (V.toList betaV) mdMeansL
-}
    psMap <- case runConfig.rcTurnoutPS of
      Nothing -> mempty
      Just gtt -> case gqDataAndIndexes_CM of
        Nothing -> K.knitError "modelResultAction: Expected gq data and indexes but got Nothing."
        Just gqDaI_C -> do
          let getVectorPcts n = K.knitEither $ SP.getVector . fmap CS.percents <$> SP.parse1D n (CS.paramStats summary)
          (_, gqIndexesE) <- K.ignoreCacheTime gqDaI_C
          grpIM <- K.knitEither
             $ gqIndexesE >>= SMB.getGroupIndex (SMB.RowTypeTag @(F.Record (DP.PSDataR k)) SC.GQData "PSData") (MC.psGroupTag @l)
          psTByGrpV <- getVectorPcts "tByGrp"
          K.knitEither $ M.fromList . zip (IM.elems grpIM) <$> (traverse MT.listToCI $ V.toList psTByGrpV)
    pure $ MC.PSMap psMap

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

import qualified BlueRipple.Model.Election2.DataPrep as DP
import qualified BlueRipple.Model.Demographic.DataPrep as DDP
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.GeographicTypes as GT

import Control.Lens (view, (^.))
import qualified Data.Map.Strict as M

import qualified Data.Vector.Unboxed as VU
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V

import qualified Data.Dependent.Sum as DSum

import qualified Frames as F
import qualified Frames.Melt as F
import qualified Frames.Serialize as FS
import qualified Frames.Transform as FT
import qualified Frames.Streamly.InCore as FSI

import qualified Numeric

import qualified Stan.ModelBuilder as SMB
import qualified Stan.ModelBuilder.BuildingBlocks.GroupAlpha as SG
import qualified Stan.ModelBuilder.DesignMatrix as DM
import qualified Stan.ModelBuilder.TypedExpressions.Types as TE
import qualified Stan.ModelBuilder.TypedExpressions.Statements as TE
import qualified Stan.ModelBuilder.TypedExpressions.Indexing as TEI
import qualified Stan.ModelBuilder.TypedExpressions.Operations as TEO
import qualified Stan.ModelBuilder.TypedExpressions.StanFunctions as SF
import Stan.ModelBuilder.TypedExpressions.TypedList (TypedList(..))
import qualified Flat
import Flat.Instances.Vector ()

-- design matrix rows
safeLog :: Double -> Double
safeLog x = if x >= 1 then Numeric.log x else 0

stateG :: SMB.GroupTypeTag Text
stateG = SMB.GroupTypeTag "State"

psGroupTag :: forall k . Typeable k => SMB.GroupTypeTag (F.Record k)
psGroupTag = SMB.GroupTypeTag "PSGrp"

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

data Alphas = St_A_S_E_R | St_A_S_E_R_ER | St_A_S_E_R_StR | St_A_S_E_R_ER_StR | St_A_S_E_R_ER_StR_StER deriving stock (Eq, Ord, Show)

alphasText :: Alphas -> Text
alphasText = show
{-
alphasText St_A_S_E_R = "St_A_S_E_R"
alphasText St_A_S_E_R_ER = "St_A_S_E_R_ER"
alphasText St_A_S_E_R_StR = "St_A_S_E_R_StR"
alphasText St_A_S_E_R_ER_StR = "St_A_S_E_R_ER_StR"
alphasText St_A_S_E_R_ER_StR_StER = "St_A_S_E_R_ER_StR_StER"
-}

data ModelConfig (b :: TE.EType) =
  ModelConfig
  {
    mcSurveyAggregation :: SurveyAggregation b
  , mcAlphas :: Alphas
  , mcDesignMatrixRow :: DM.DesignMatrixRow (F.Record DP.LPredictorsR)
  }

modelConfigText :: ModelConfig b -> Text
modelConfigText (ModelConfig sa alphas dmr) =  aggregationText sa <> "_" <> alphasText alphas <> "_" <> dmr.dmName

data RegistrationConfig a b =
  RegistrationConfig
  {
    rcSurvey :: TurnoutSurvey a
  , rcModelConfig :: ModelConfig b
  }

data TurnoutConfig a b =
  TurnoutConfig
  {
    tcSurvey :: TurnoutSurvey a
  , tcModelConfig :: ModelConfig b
  }

data PrefConfig b =
  PrefConfig
  {
    pcModelConfig :: ModelConfig b
  }

type GroupsR = GT.StateAbbreviation ': DP.DCatsR

groups :: Foldable g => g Text -> [DSum.DSum SMB.GroupTypeTag (SG.GroupFromData (F.Record GroupsR))]
groups states = [stateG DSum.:=>
                 SG.GroupFromData (view GT.stateAbbreviation)
                 (SMB.makeIndexFromFoldable show (view GT.stateAbbreviation) states)
                 (SMB.dataToIntMapFromFoldable (view GT.stateAbbreviation) states)
                , ageG DSum.:=> SG.groupFromDataEnum (view DT.age5C)
                , sexG DSum.:=> SG.groupFromDataEnum (view DT.sexC)
                , eduG DSum.:=> SG.groupFromDataEnum (view DT.education4C)
                , raceG DSum.:=> SG.groupFromDataEnum (view DT.race5C)
                ]

addGroupIndexesAndIntMaps :: (GroupsR F.⊆ rs)
        => [DSum.DSum SMB.GroupTypeTag (SG.GroupFromData (F.Record GroupsR))]
        -> SMB.RowTypeTag (F.Record rs)
        -> SMB.StanGroupBuilderM md gq ()
addGroupIndexesAndIntMaps groups' dataTag = do
  SG.addModelIndexes dataTag F.rcast groups'
  SG.addGroupIntMaps dataTag F.rcast groups'

surveyDataGroupBuilder :: (Foldable g, Typeable rs, GroupsR F.⊆ rs)
                       => g Text -> Text -> SMB.ToFoldable md (F.Record rs) -> SMB.StanGroupBuilderM md gq ()
surveyDataGroupBuilder states sName sTF = SMB.addModelDataToGroupBuilder sName sTF >>= addGroupIndexesAndIntMaps (groups states)

{-
stateTargetsGroupBuilder :: (Foldable g, F.ElemOf rs GT.StateAbbreviation
                            )
                         => g Text -> SMB.RowTypeTag (F.Record rs) -> SMB.StanGroupBuilderM md gq ()
stateTargetsGroupBuilder states rtt =
  SMB.addGroupIndexForData stateG rtt
  $ SMB.makeIndexFromFoldable show (view GT.stateAbbreviation) states

acsDataGroupBuilder :: [DSum.DSum SMB.GroupTypeTag (SG.GroupFromData (F.Record GroupsR))]
                    ->  SMB.StanGroupBuilderM (DP.ModelData lk) gq ()
acsDataGroupBuilder groups' = do
   acsDataTag <- SMB.addModelDataToGroupBuilder "ACSData" (SMB.ToFoldable DP.acsData)
   SG.addModelIndexes acsDataTag F.rcast groups'
-}
-- NB: often l ~ k, e.g., for predicting district turnout/preference
-- But say you want to predict turnout by race, nationally.
-- Now l ~ '[Race5C]
-- How about turnout by Education in each state? Then l ~ [StateAbbreviation, Education4C]
psGroupBuilder :: forall g k lk l .
                 (Foldable g
                 , Typeable (DP.PSDataR k)
                 , Show (F.Record l)
                 , Ord (F.Record l)
                 , l F.⊆ DP.PSDataR k
                 , Typeable l
                 , F.ElemOf (DP.PSDataR k) GT.StateAbbreviation
                 , DP.DCatsR F.⊆ DP.PSDataR k
                 )
               => g Text
               -> g (F.Record l)
               -> SMB.StanGroupBuilderM (DP.ModelData lk) (DP.PSData k) ()
psGroupBuilder states psKeys = do
  let groups' = groups states
      psGtt = psGroupTag @l
  psTag <- SMB.addGQDataToGroupBuilder "PSData" (SMB.ToFoldable DP.unPSData)
  SMB.addGroupIndexForData psGtt psTag $ SMB.makeIndexFromFoldable show F.rcast psKeys
  SMB.addGroupIntMapForDataSet psGtt psTag $ SMB.dataToIntMapFromFoldable F.rcast psKeys
  SG.addModelIndexes psTag F.rcast groups'

-- design matrix rows
tDesignMatrixRow_d :: DM.DesignMatrixRow (F.Record DP.LPredictorsR)
tDesignMatrixRow_d = DM.DesignMatrixRow "d" [dRP]
  where
    dRP = DM.DesignMatrixRowPart "logDensity" 1 (VU.singleton . safeLog . view DT.pWPopPerSqMile)

data ModelType = TurnoutMT | RegistrationMT | PreferenceMT | FullMT deriving stock (Eq, Ord, Show)

data TurnoutSurvey a where
  CESSurvey :: TurnoutSurvey (F.Record DP.CESByCDR)
  CPSSurvey :: TurnoutSurvey (F.Record DP.CPSByStateR)

data RealCountModel = ContinuousBinomial | BetaProportion deriving stock (Eq)

realCountModelText :: RealCountModel -> Text
realCountModelText ContinuousBinomial = "CB"
realCountModelText BetaProportion = "BP"

data SurveyAggregation b where
  UnweightedAggregation :: SurveyAggregation TE.EIntArray
  WeightedAggregation :: RealCountModel -> SurveyAggregation TE.ECVec

turnoutSurveyText :: TurnoutSurvey a -> Text
turnoutSurveyText CESSurvey = "CES"
turnoutSurveyText CPSSurvey = "CPS"

data PSTargets = NoPSTargets | PSTargets deriving stock (Eq, Ord, Show)
psTargetsText :: PSTargets -> Text
psTargetsText NoPSTargets = "noPSTgt"
psTargetsText PSTargets = "PSTgt"

aggregationText :: SurveyAggregation b -> Text
aggregationText UnweightedAggregation = "UW"
aggregationText (WeightedAggregation cm) = "WA" <> realCountModelText cm

addAggregationText :: SurveyAggregation b -> Text
addAggregationText UnweightedAggregation = "_UW"
addAggregationText (WeightedAggregation cm) = "_WA" <> realCountModelText cm

data BinomialData (b :: TE.EType) =
  BinomialData
  {
    bdTrials :: TE.UExpr b
  , bdSuccesses :: TE.UExpr b
  }

binomialData :: SMB.RowTypeTag a
             -> (SMB.RowTypeTag a -> SMB.StanBuilderM md gq (TE.UExpr b))
             -> (SMB.RowTypeTag a -> SMB.StanBuilderM md gq (TE.UExpr b))
             -> SMB.StanBuilderM md gq (BinomialData b)
binomialData rtt trialsF succF = do
  trialsE <- trialsF rtt --SBB.addCountData surveyDataTag "Surveyed" (view DP.surveyed)
  successesE <- succF rtt --SBB.addCountData surveyDataTag "Voted" (view DP.voted)
  pure $ BinomialData trialsE successesE

data CovariatesAndCounts a (b :: TE.EType) =
  CovariatesAndCounts
  {
    ccSurveyDataTag :: SMB.RowTypeTag a
  , ccNCovariates :: TE.IntE
  , ccCovariates :: TE.MatrixE
  , ccBinomialData :: BinomialData b
  }

covariatesAndCountsFromData :: DP.LPredictorsR F.⊆ rs
                            => SMB.RowTypeTag (F.Record rs)
                            -> ModelConfig b
                            -> (SMB.RowTypeTag (F.Record rs) -> SMB.StanBuilderM md gq (TE.UExpr b))
                            -> (SMB.RowTypeTag (F.Record rs) -> SMB.StanBuilderM md gq (TE.UExpr b))
                            -> SMB.StanBuilderM md gq (CovariatesAndCounts (F.Record rs) b)
covariatesAndCountsFromData rtt modelConfig trialsF succF = do
  trialsE <- trialsF rtt --SBB.addCountData surveyDataTag "Surveyed" (view DP.surveyed)
  successesE <- succF rtt --SBB.addCountData surveyDataTag "Voted" (view DP.voted)
  let (_, nCovariatesE) = DM.designMatrixColDimBinding modelConfig.mcDesignMatrixRow Nothing
  dmE <- if DM.rowLength modelConfig.mcDesignMatrixRow > 0
    then DM.addDesignMatrix rtt (contramap F.rcast modelConfig.mcDesignMatrixRow) Nothing
    else pure $ TE.namedE "ERROR" TE.SMat -- this shouldn't show up in stan code at all
  pure $ CovariatesAndCounts rtt nCovariatesE dmE $ BinomialData trialsE successesE

{-
data StateTargetsData tds =
  StateTargetsData
  {
    stdTargetTypeTag :: SMB.RowTypeTag (F.Record tds)
  , stdTarget :: TE.VectorE
  , stdACSTag :: SMB.RowTypeTag (F.Record DDP.ACSa5ByStateR)
  , stdACSWgts :: TE.IntArrayE
  , stdACSCovariates :: TE.MatrixE
  }
-}

data ModelData tds a (b :: TE.EType) where
  ModelData :: CovariatesAndCounts a b -> ModelData tds a b
--  PT_ModelData :: CovariatesAndCounts a b -> StateTargetsData tds -> ModelData tds a b

covariatesAndCounts :: ModelData tds a b -> CovariatesAndCounts a b
covariatesAndCounts (ModelData cc) = cc
--covariatesAndCounts (PT_ModelData cc _) = cc

withCC :: (forall x y . CovariatesAndCounts x y -> c) -> ModelData tds a b -> c
withCC f (ModelData cc) = f cc
--withCC f (PT_ModelData cc _) = f cc

{-
stateTargetsTD :: Maybe Text
               -> CovariatesAndCounts a b
               -> StateTargetsData td
               -> Maybe (TE.MatrixE -> TE.StanName -> SMB.StanBuilderM md gq TE.MatrixE)
               -> Maybe TE.MatrixE
               -> SMB.StanBuilderM md gq (TE.MatrixE, TE.IntArrayE)
stateTargetsTD prefixM cc st cM rM = do
  let prefixed t = maybe t (\p -> p <> "_" <> t) prefixM
  dmACS' <- case cM of
    Nothing -> pure st.stdACSCovariates
    Just c -> c st.stdACSCovariates $ prefixed "dmACS_Centered"
  dmACS <- case rM of
    Nothing -> pure dmACS'
    Just r -> SMB.inBlock SMB.SBTransformedData $ SMB.addFromCodeWriter $ do
      let rowsE = SMB.dataSetSizeE st.stdACSTag
          colsE = cc.ccNCovariates
      TE.declareRHSNW (TE.NamedDeclSpec (prefixed "acsDM_QR") $ TE.matrixSpec rowsE colsE [])
           $ dmACS' `TE.timesE` r
  acsNByState <- SMB.inBlock SMB.SBTransformedData $ SMB.addFromCodeWriter $ do
    let nStatesE = SMB.groupSizeE stateG
        nACSRowsE = SMB.dataSetSizeE st.stdACSTag
        acsStateIndex = SMB.byGroupIndexE st.stdACSTag stateG
        plusEq = TE.opAssign TEO.SAdd
        acsNByStateNDS = TE.NamedDeclSpec (prefixed "acsNByState") $ TE.intArraySpec nStatesE [TE.lowerM $ TE.intE 0]
    acsNByState <- TE.declareRHSNW acsNByStateNDS $ TE.functionE SF.rep_array (TE.intE 0 :> nStatesE :> TNil)
    TE.addStmt
      $ TE.for "k" (TE.SpecificNumbered (TE.intE 1) nACSRowsE) $ \kE ->
      [(TE.indexE TEI.s0 acsStateIndex acsNByState `TE.at` kE) `plusEq` (st.stdACSWgts `TE.at` kE)]
    pure acsNByState
  pure (dmACS, acsNByState)
-}
data RunConfig l = RunConfig { rcIncludePPCheck :: Bool, rcIncludeLL :: Bool, rcPS :: Maybe (SMB.GroupTypeTag (F.Record l)) }

newtype PSMap l a = PSMap { unPSMap :: Map (F.Record l) a} deriving newtype (Functor)

instance (V.RMap l, Ord (F.Record l), FS.RecFlat l, Flat.Flat a) => Flat.Flat (PSMap l a) where
  size (PSMap m) n = Flat.size (fmap (first  FS.toS) $ M.toList m) n
  encode (PSMap m) = Flat.encode (fmap (first  FS.toS) $ M.toList m)
  decode = (\sl -> PSMap $ M.fromList $ fmap (first FS.fromS) sl) <$> Flat.decode

psMapToFrame :: forall t l a . (V.KnownField t, V.Snd t ~ a, FSI.RecVec (l V.++ '[t]))
             => PSMap l a -> F.FrameRec (l V.++ '[t])
psMapToFrame (PSMap m) = F.toFrame $ fmap toRec $ M.toList m
  where
    toRec (l, a) = l F.<+> FT.recordSingleton @t a

{-
-- NB: often l ~ k, e.g., for predicting district turnout/preference
-- But say you want to predict turnout by race, nationally.
-- Now l ~ '[Race5C]
-- How about turnout by Education in each state? Then l ~ [StateAbbreviation, Education4C]
stateGroupBuilder :: forall g k l a b .
                     (Foldable g
                     , Typeable (DP.PSDataR k)
                     , Show (F.Record l)
                     , Ord (F.Record l)
                     , l F.⊆ DP.PSDataR k
                     , Typeable l
                     , F.ElemOf (DP.PSDataR k) GT.StateAbbreviation
                     )
                  => TurnoutConfig a b
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
  -- the return type must be explcit here so GHC knows the GADT type parameter does not escape its scope
  () <- case turnoutConfig.tSurvey of
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
  SMB.addGroupIndexForData stateG psTag $ SMB.makeIndexFromFoldable show saF states
--  SMB.addGroupIntMapForDataSet stateG psTag $ SMB.dataToIntMapFromFoldable saF states

turnoutModelData :: forall a b gq . TurnoutConfig a b
                 -> SMB.StanBuilderM DP.ModelData gq (TurnoutModelData a b)
turnoutModelData tc = do
  let cpsCC_UW :: SMB.StanBuilderM DP.ModelData gq (CovariatesAndCounts (F.Record DP.CPSByStateR) TE.EIntArray)
      cpsCC_UW = do
        surveyDataTag <- SMB.dataSetTag @(F.Record DP.CPSByStateR) SC.ModelData "SurveyData"
        let (_, nCovariatesE) = DM.designMatrixColDimBinding tc.tDesignMatrixRow Nothing
        dmE <- if DM.rowLength tc.tDesignMatrixRow > 0
          then DM.addDesignMatrix surveyDataTag (contramap F.rcast tc.tDesignMatrixRow) Nothing
          else pure $ TE.namedE "ERROR" TE.SMat -- this shouldn't show up in stan code at all
        trialsE <- SBB.addCountData surveyDataTag "Surveyed" (view DP.surveyed)
        successesE <- SBB.addCountData surveyDataTag "Voted" (view DP.voted)
        pure $ CovariatesAndCounts surveyDataTag nCovariatesE dmE trialsE successesE
      cpsCC_W :: SMB.StanBuilderM DP.ModelData gq (CovariatesAndCounts (F.Record DP.CPSByStateR) TE.ECVec)
      cpsCC_W = do
        surveyDataTag <- SMB.dataSetTag @(F.Record DP.CPSByStateR) SC.ModelData "SurveyData"
        let (_, nCovariatesE) = DM.designMatrixColDimBinding tc.tDesignMatrixRow Nothing
        dmE <- if DM.rowLength tc.tDesignMatrixRow > 0
          then DM.addDesignMatrix surveyDataTag (contramap F.rcast tc.tDesignMatrixRow) Nothing
          else pure $ TE.namedE "ERROR" TE.SMat -- this shouldn't show up in stan code at all
        trialsE <- SBB.addRealData surveyDataTag "Surveyed" (Just 0.99) Nothing (view DP.surveyedW)
        successesE <- SBB.addRealData surveyDataTag "Voted" (Just 0) Nothing (view DP.votedW)
        pure $ CovariatesAndCounts surveyDataTag nCovariatesE dmE trialsE successesE
      cesCC_UW :: SMB.StanBuilderM DP.ModelData gq (CovariatesAndCounts (F.Record DP.CESByCDR) TE.EIntArray)
      cesCC_UW = do
        surveyDataTag <- SMB.dataSetTag @(F.Record DP.CESByCDR) SC.ModelData "SurveyData"
        let (_, nCovariatesE) = DM.designMatrixColDimBinding tc.tDesignMatrixRow Nothing
        dmE <- if DM.rowLength tc.tDesignMatrixRow > 0
          then DM.addDesignMatrix surveyDataTag (contramap F.rcast tc.tDesignMatrixRow) Nothing
          else pure $ TE.namedE "ERROR" TE.SMat -- this shouldn't show up in stan code at all
        trialsE <- SBB.addCountData surveyDataTag "Surveyed" (view DP.surveyed)
        successesE <- SBB.addCountData surveyDataTag "Voted" (view DP.voted)
        pure $ CovariatesAndCounts surveyDataTag nCovariatesE dmE trialsE successesE
      cesCC_W :: SMB.StanBuilderM DP.ModelData gq (CovariatesAndCounts (F.Record DP.CESByCDR) TE.ECVec)
      cesCC_W = do
        surveyDataTag <- SMB.dataSetTag @(F.Record DP.CESByCDR) SC.ModelData "SurveyData"
        let (_, nCovariatesE) = DM.designMatrixColDimBinding tc.tDesignMatrixRow Nothing
        dmE <- if DM.rowLength tc.tDesignMatrixRow > 0
          then DM.addDesignMatrix surveyDataTag (contramap F.rcast tc.tDesignMatrixRow) Nothing
          else pure $ TE.namedE "ERROR" TE.SMat -- this shouldn't show up in stan code at all
        trialsE <- SBB.addRealData surveyDataTag "Surveyed" (Just 0.99) Nothing (view DP.surveyedW)
        successesE <- SBB.addRealData surveyDataTag "Voted" (Just 0) Nothing (view DP.votedW)
        pure $ CovariatesAndCounts surveyDataTag nCovariatesE dmE trialsE successesE
  case tc.tPSTargets of
    NoPSTargets -> case tc.tSurvey of
      CPSSurvey -> case tc.tSurveyAggregation of
        UnweightedAggregation -> fmap NoPT_TurnoutModelData cpsCC_UW
        WeightedAggregation _ -> fmap NoPT_TurnoutModelData cpsCC_W
      CESSurvey -> case tc.tSurveyAggregation of
        UnweightedAggregation -> fmap NoPT_TurnoutModelData cesCC_UW
        WeightedAggregation _ -> fmap NoPT_TurnoutModelData cesCC_W
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
        CPSSurvey -> case tc.tSurveyAggregation of
          UnweightedAggregation -> fmap (\x -> PT_TurnoutModelData x std) cpsCC_UW
          WeightedAggregation _ -> fmap (\x -> PT_TurnoutModelData x std) cpsCC_W
        CESSurvey -> case tc.tSurveyAggregation of
          UnweightedAggregation -> fmap (\x -> PT_TurnoutModelData x std) cesCC_UW
          WeightedAggregation _ -> fmap (\x -> PT_TurnoutModelData x std) cesCC_W



turnoutModelText :: TurnoutConfig a b -> Text
turnoutModelText (TurnoutConfig ts tsa tPs dmr am) = "Turnout" <> turnoutSurveyText ts
                                                 <> (if (tPs == PSTargets) then "_PSTgt" else "")
                                                 <> addAggregationText tsa
                                                 <> "_" <> dmr.dmName <> "_" <> stateAlphaModelText am

turnoutModelDataText :: TurnoutConfig a b -> Text
turnoutModelDataText (TurnoutConfig ts tsa tPs dmr _) = "Turnout" <> turnoutSurveyText ts
                                                    <> (if (tPs == PSTargets) then "_PSTgt" else "")
                                                    <> addAggregationText tsa
                                                    <> "_" <> dmr.dmName


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

-- we can use the designMatrixRow to get (a -> Vector Double) for prediction
data TurnoutConfig a (b :: TE.EType) =
  TurnoutConfig
  {
    tSurvey :: TurnoutSurvey a
  , tSurveyAggregation :: SurveyAggregation b
  , tPSTargets :: PSTargets
  , tDesignMatrixRow :: DM.DesignMatrixRow (F.Record DP.PredictorsR)
  , tStateAlphaModel :: StateAlpha
  }

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
    re r = DM.BEProduct2 (r ^. DT.race5C, r ^. DT.education4C)
    reRP = DM.boundedEnumRowPart (Just (DM.BEProduct2 (DT.R5_WhiteNonHispanic, DT.E4_NonHSGrad))) "RaceEdu" re

data StateAlpha = StateAlphaSimple | StateAlphaHierCentered | StateAlphaHierNonCentered deriving stock (Show)

stateAlphaModelText :: StateAlpha -> Text
stateAlphaModelText StateAlphaSimple = "AS"
stateAlphaModelText StateAlphaHierCentered = "AHC"
stateAlphaModelText StateAlphaHierNonCentered = "AHNC"

turnoutTargetsModel :: ModelParameters
                    -> CovariatesAndCounts a b
                    -> StateTargetsData td
                    -> Maybe (TE.MatrixE -> TE.StanName -> SMB.StanBuilderM md gq TE.MatrixE)
                    -> Maybe TE.MatrixE -> SMB.StanBuilderM md gq ()
turnoutTargetsModel mp cc std cM rM = do
  (dmACS, acsNByState) <- turnoutTargetsTD cc std cM rM
  let acsStateIndex = SMB.byGroupIndexE std.stdACSTag stateG
      toVec x = TE.functionE SF.to_vector (x :> TNil)
      pE = probabilitiesExpr mp std.stdACSTag dmACS
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
      psPE = probabilitiesExpr mp psDataTag dmPS
  _ <- SBB.postStratifiedParameterF False SMB.SBGeneratedQuantities (Just "tByGrp") psDataTag gtt psDataGrpIndex (toVec psWgts) (pure psPE) Nothing
  pure ()

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

paramAlpha :: ModelParameters -> Alpha
paramAlpha (BinomialLogitModelParameters a _) = a

paramTheta :: ModelParameters -> Theta
paramTheta (BinomialLogitModelParameters _ t) = t

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
  pure $ BinomialLogitModelParameters alpha theta


probabilitiesExpr :: ModelParameters -> SMB.RowTypeTag a -> TE.MatrixE -> TE.VectorE
probabilitiesExpr mps rtt covariatesM = TE.functionE SF.inv_logit (lp :> TNil)
  where
    stateIndexE = SMB.byGroupIndexE rtt stateG
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
             -> TurnoutConfig a b
             -> SMB.StanBuilderM DP.ModelData (DP.PSData k) ()
turnoutModel rc tmc = do
  mData <- turnoutModelData tmc
  mParams <- case tmc.tSurvey of
    CESSurvey -> case mData of
      NoPT_TurnoutModelData cc -> modelParameters (contramap F.rcast tmc.tDesignMatrixRow) cc.ccSurveyDataTag tmc.tStateAlphaModel
      PT_TurnoutModelData cc _ -> modelParameters (contramap F.rcast tmc.tDesignMatrixRow) cc.ccSurveyDataTag tmc.tStateAlphaModel
    CPSSurvey -> case mData of
      NoPT_TurnoutModelData cc -> modelParameters (contramap F.rcast tmc.tDesignMatrixRow) cc.ccSurveyDataTag tmc.tStateAlphaModel
      PT_TurnoutModelData cc _ -> modelParameters (contramap F.rcast tmc.tDesignMatrixRow) cc.ccSurveyDataTag tmc.tStateAlphaModel
  let --betaNDS = TE.NamedDeclSpec "beta" $ TE.vectorSpec (withCC ccNCovariates mData) []
      nRowsE = withCC (SMB.dataSetSizeE . ccSurveyDataTag) mData
      pExpr = DAG.parameterExpr

  -- to apply the same transformation to another matrix, we center with centerF and then post-multiply by r
  -- or we could apply beta to centered matrix ?

{-
  (covariatesM, centerF) <- case paramTheta mParams of
    Theta (Just thetaP) -> do
      (centeredCovariatesE, centerF) <- DM.centerDataMatrix DM.DMCenterOnly (withCC ccCovariates mData) Nothing "DM"
      pure (centeredCovariatesE, centerF)
    Theta Nothing -> pure (TE.namedE "ERROR" TE.SMat, \_ x _ -> pure x)
-}
{-
  (covariatesM, r, centerF, mBeta) <- case paramTheta mParams of
    Theta (Just thetaP) -> do
      (centeredCovariatesE, centerF) <- DM.centerDataMatrix DM.DMCenterOnly (withCC ccCovariates mData) Nothing "DM"
      (dmQ, r, _, mBeta') <- DM.thinQR centeredCovariatesE "DM" $ Just (pExpr thetaP, betaNDS)
      pure (dmQ, r, centerF, mBeta')
    Theta Nothing -> pure (TE.namedE "ERROR" TE.SMat, TE.namedE "ERROR" TE.SMat, \_ x _ -> pure x, Nothing)
-}
  case mData of
    PT_TurnoutModelData cc st -> turnoutTargetsModel mParams cc st Nothing Nothing --(Just $ centerF SC.ModelData) (Just r)
    _ -> pure ()

  -- model
  let covariatesM = withCC ccCovariates mData
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

  () <- case tmc.tSurveyAggregation of
    UnweightedAggregation -> do
      let a = paramAlpha mParams
          t = paramTheta mParams
          model :: SMB.RowTypeTag a -> TE.IntArrayE -> TE.IntArrayE -> SMB.StanBuilderM md gq ()
          model rtt n k = do
            let ssF e = SMB.familySample (SMD.binomialLogitDist @TE.EIntArray) e (n :> lpE a t :> TNil)
                rpF :: TE.CodeWriter (TE.IntE -> TE.ExprList '[TE.EInt, TE.EReal])
                rpF = pure $ \nE -> n `TE.at` nE :> lpE a t `TE.at` nE :> TNil
                ppF = SBB.generatePosteriorPrediction rtt
                      (TE.NamedDeclSpec ("predVotes") $ TE.array1Spec nRowsE $ TE.intSpec [])
                      SMD.binomialLogitDist rpF
                ll = llF SMD.binomialLogitDist rpF (pure $ \nE -> k `TE.at` nE)
            SMB.inBlock SMB.SBModel $ SMB.addFromCodeWriter $ TE.addStmt $ ssF k
            when rc.rcIncludePPCheck $ void ppF
            when rc.rcIncludeLL ll
      case mData of
        NoPT_TurnoutModelData cc -> model cc.ccSurveyDataTag cc.ccTrials cc.ccSuccesses
        PT_TurnoutModelData cc _ -> model cc.ccSurveyDataTag cc.ccTrials cc.ccSuccesses
    WeightedAggregation -> do
      let a = paramAlpha mParams
          t = paramTheta mParams
      realBinomialLogitDistV <- SMD.realBinomialLogitDistM @TE.ECVec
      realBinomialLogitDistS <- SMD.realBinomialLogitDistSM
      let model ::  SMB.RowTypeTag a -> TE.VectorE -> TE.VectorE -> SMB.StanBuilderM md gq ()
          model rtt n k = do
            let ssF e = SMB.familySample realBinomialLogitDistV e (n :> lpE a t :> TNil)
                rpF :: TE.CodeWriter (TE.IntE -> TE.ExprList '[TE.EReal, TE.EReal])
                rpF = pure $ \nE -> n `TE.at` nE :> lpE a t `TE.at` nE :> TNil
                ppF = SBB.generatePosteriorPrediction rtt
                      (TE.NamedDeclSpec ("predVotes") $ TE.array1Spec nRowsE $ TE.realSpec [])
                      realBinomialLogitDistS rpF
                ll = llF realBinomialLogitDistS rpF (pure $ \nE -> k `TE.at` nE)
            SMB.inBlock SMB.SBModel $ SMB.addFromCodeWriter $ TE.addStmt $ ssF k
            when rc.rcIncludePPCheck $ void ppF
            when rc.rcIncludeLL ll
      case mData of
        NoPT_TurnoutModelData cc -> model cc.ccSurveyDataTag cc.ccTrials cc.ccSuccesses
        PT_TurnoutModelData cc _ -> model cc.ccSurveyDataTag cc.ccTrials cc.ccSuccesses

  case rc.rcTurnoutPS of
    Nothing -> pure ()
    Just gtt -> turnoutPS mParams tmc.tDesignMatrixRow Nothing Nothing gtt --(Just $ centerF SC.GQData) (Just r)
  pure ()



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
            , Show (F.Record l)
            , Typeable l
            )
         => Either Text Text
         -> Text
         -> Text
         -> BR.CommandLine
         -> RunConfig l
         -> TurnoutConfig a b
         -> K.ActionWithCacheTime r DP.ModelData
         -> K.ActionWithCacheTime r (DP.PSData k)
         -> K.Sem r (K.ActionWithCacheTime r (TurnoutPrediction, PSMap l MT.ConfidenceInterval))
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
modelResultAction :: forall k l r a b .
                     (Ord (F.Record l)
                     , K.KnitEffects r
                     , Typeable (DP.PSDataR k)
                     , Typeable l
                     )
                  => TurnoutConfig a b
                  -> RunConfig l
                  -> SC.ResultAction r DP.ModelData (DP.PSData k) SMB.DataSetGroupIntMaps () (TurnoutPrediction, PSMap l MT.ConfidenceInterval)
modelResultAction turnoutConfig runConfig = SC.UseSummary f where
  f summary _ modelDataAndIndexes_C gqDataAndIndexes_CM = do
    (modelData, resultIndexesE) <- K.ignoreCacheTime modelDataAndIndexes_C
    -- compute means of predictors because model was zero-centered in them
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
    psMap <- case runConfig.rcTurnoutPS of
      Nothing -> mempty
      Just gtt -> case gqDataAndIndexes_CM of
        Nothing -> K.knitError "modelResultAction: Expected gq data and indexes but got Nothing."
        Just gqDaI_C -> do
          let getVectorPcts n = K.knitEither $ SP.getVector . fmap CS.percents <$> SP.parse1D n (CS.paramStats summary)
          (_, gqIndexesE) <- K.ignoreCacheTime gqDaI_C
          grpIM <- K.knitEither
             $ gqIndexesE >>= SMB.getGroupIndex (SMB.RowTypeTag @(F.Record (DP.PSDataR k)) SC.GQData "PSData") (psGroupTag @l)
          psTByGrpV <- getVectorPcts "tByGrp"
          K.knitEither $ M.fromList . zip (IM.elems grpIM) <$> (traverse MT.listToCI $ V.toList psTByGrpV)
    pure $ (TurnoutPrediction geoMap (VU.convert betaSI) Nothing, PSMap psMap)
-}

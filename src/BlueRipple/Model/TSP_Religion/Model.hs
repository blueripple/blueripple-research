{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE UnicodeSyntax #-}

module BlueRipple.Model.TSP_Religion.Model
  (
    module BlueRipple.Model.TSP_Religion.Model
  , module BlueRipple.Model.Election2.ModelCommon
  )
where

import qualified BlueRipple.Model.Election2.DataPrep as DP
import qualified BlueRipple.Model.Election2.ModelCommon as MC
import BlueRipple.Model.Election2.ModelCommon (ModelConfig(..))
import qualified BlueRipple.Model.Election2.ModelCommon2 as MC2
import qualified BlueRipple.Model.Election2.ModelRunner as MR
import qualified BlueRipple.Model.Demographic.DataPrep as DDP

import qualified BlueRipple.Configuration as BR
import qualified BlueRipple.Utilities.KnitUtils as BRKU
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.GeographicTypes as GT
import qualified BlueRipple.Data.ElectionTypes as ET
import qualified BlueRipple.Data.ModelingTypes as MT
import qualified BlueRipple.Data.CCES as CCES
import qualified BlueRipple.Data.ACS_PUMS as ACS
import qualified BlueRipple.Data.Loaders as BR
import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Utilities.FramesUtils as BRF
import qualified BlueRipple.Utilities.KnitUtils as BR

import qualified Knit.Report as K

import qualified Stan.ModelBuilder as SMB
import qualified Stan.ModelRunner as SMR
import qualified Stan.ModelBuilder.TypedExpressions.Types as TE
import qualified Stan.ModelBuilder.TypedExpressions.Statements as TE
import qualified Stan.ModelBuilder.TypedExpressions.Operations as TEO
import qualified Stan.ModelBuilder.TypedExpressions.StanFunctions as SF
import qualified Stan.Parameters as SP
import qualified Stan.ModelConfig as SC
import qualified Stan.RScriptBuilder as SR
import qualified Stan.ModelBuilder.BuildingBlocks as SBB
import qualified Stan.ModelBuilder.BuildingBlocks.GroupAlpha as SG
import qualified Stan.ModelBuilder.DesignMatrix as DM
import qualified CmdStan as CS

import qualified Frames as F
import qualified Frames.Melt as F
import qualified Frames.MapReduce as FMR
import qualified Frames.SimpleJoins as FJ
import qualified Frames.Constraints as FC
import qualified Frames.Streamly.TH as FS
import qualified Frames.Streamly.InCore as FI
import qualified Frames.Serialize as FS

import qualified Control.Foldl as FL
import qualified Control.Foldl.Statistics as FLS
import Control.Lens (view, (^.))

import qualified Flat
import qualified Data.IntMap.Strict as IM
import qualified Data.List as List
import qualified Data.Map.Strict as M
import qualified Data.Set as S
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V

FS.declareColumn "NEvangelical" ''Int
FS.declareColumn "NEvangelicalW" ''Double

type CountDataR = [DP.SurveyWeight, DP.Surveyed, NEvangelical, DP.SurveyedW, NEvangelicalW]

type CESByR k = k V.++ DP.PredictorsR V.++ CountDataR

type CESByCDR = CESByR DP.CDKeyR

type CESByCD = F.Record CESByCDR

runEvangelicalModel :: forall l r ks a b .
                   (K.KnitEffects r
                   , BRKU.CacheEffects r
                   , V.RMap l
                   , Ord (F.Record l)
                   , FS.RecFlat l
                   , Typeable l
                   , Typeable (DP.PSDataR ks)
                   , F.ElemOf (DP.PSDataR ks) GT.StateAbbreviation
                   , DP.LPredictorsR F.⊆ DP.PSDataR ks
                   , F.ElemOf (DP.PSDataR ks) DT.PopCount
                   , DP.DCatsR F.⊆ DP.PSDataR ks
                   , l F.⊆ DP.PSDataR ks --'[GT.StateAbbreviation]
                   , Show (F.Record l)
                   )
                => Int
                -> MR.CacheStructure () ()
                -> BR.CommandLine
                -> PSType (F.Record DP.DCatsR -> Bool)
                -> ModelConfig b
                -> K.ActionWithCacheTime r (DP.PSData ks)
                -> K.Sem r (K.ActionWithCacheTime r (MC.PSMap l MT.ConfidenceInterval, MC2.ModelParameters))
runEvangelicalModel year cacheStructure cmdLine psType mc psData_C = do
  let --config = MC2.TurnoutOnly tc
      runConfig = RunConfig False False (Just (MC.psGroupTag @l, psType))
  modelData_C <- cachedPreppedCES cacheStructure
  runModel (MR.csModelDirE cacheStructure)  ("E_" <> show year)
    (MR.csPSName cacheStructure) cmdLine runConfig mc modelData_C psData_C

evangelicalModelData ::  forall b gq . ModelConfig b -> SMB.StanBuilderM CESData gq (MC.ModelData '[] CESByCD b)
evangelicalModelData mc = do
  let cesSurveyDataTag = SMB.dataSetTag @CESByCD SC.ModelData "CES"
      uwSurveyed rtt = SBB.addCountData rtt "Surveyed" (view DP.surveyed)
      uwEvangelical rtt = SBB.addCountData rtt "Evangelical" (view nEvangelical)
      wSurveyed rtt = SBB.addRealData rtt "Surveyed" (Just 0) Nothing (view DP.surveyedW)
      wEvangelical rtt = SBB.addRealData rtt "Evangelical" (Just 0) Nothing (view nEvangelicalW)
  case mc.mcSurveyAggregation of
        MC.UnweightedAggregation -> fmap MC.ModelData $ cesSurveyDataTag >>= \rtt -> MC.covariatesAndCountsFromData rtt mc uwSurveyed uwEvangelical
        MC.WeightedAggregation _ -> fmap MC.ModelData $ cesSurveyDataTag >>= \rtt -> MC.covariatesAndCountsFromData rtt mc wSurveyed wEvangelical


data PSType a = PSAll Text | PSGiven Text Text a | PSAmong Text Text a

psPrefix :: PSType a -> Text
psPrefix (PSAll p) = p
psPrefix (PSGiven p _ _) = p
psPrefix (PSAmong p _ _) = p

psTypeDataGQ :: DP.DCatsR F.⊆ DP.PSDataR k
             => SMB.RowTypeTag (F.Record (DP.PSDataR k)) -> PSType (F.Record DP.DCatsR -> Bool) -> SMB.StanBuilderM md gq (PSType TE.IntArrayE)
psTypeDataGQ psRowTag pst = do
--  cesGQRowTag <- SMB.dataSetTag @CESByCD SC.ModelData "CES"
  let boolToInt :: Bool -> Int
      boolToInt False = 0
      boolToInt True = 1
  case pst of
    PSAll p -> pure $ PSAll p
    PSGiven p vName cF -> SBB.addIntData psRowTag vName (Just 0) (Just 1) (boolToInt . cF . F.rcast) >>= pure . PSGiven p vName
    PSAmong p vName cF -> SBB.addIntData psRowTag vName (Just 0) (Just 1) (boolToInt . cF . F.rcast) >>= pure . PSAmong p vName

psTypeToArgs :: PSType TE.IntArrayE
             -> (Text, Maybe (TE.CodeWriter (TE.VectorE -> TE.VectorE)), Maybe (TE.CodeWriter (TE.IntArrayE -> TE.IntArrayE)))
psTypeToArgs (PSAll p) = (p, Nothing, Nothing)
psTypeToArgs (PSGiven p vName ia) = let eltTimes = TE.binaryOpE (TEO.SElementWise TEO.SMultiply)
                                    in (p, Just $ pure $ \v -> v `eltTimes` SF.toVec ia, Nothing)
psTypeToArgs (PSAmong p vName ia) = let eltTimes = TE.binaryOpE (TEO.SElementWise TEO.SMultiply)
                                    in (p, Nothing, Just $ pure $ \v -> v `eltTimes` ia)


data RunConfig l = RunConfig { rcIncludePPCheck :: Bool, rcIncludeLL :: Bool, rcPS :: Maybe (SMB.GroupTypeTag (F.Record l), PSType (F.Record DP.DCatsR -> Bool)) }
-- not returning anything for now
model :: forall k lk l a b .
         (Typeable (DP.PSDataR k)
         , F.ElemOf (DP.PSDataR k) DT.PopCount
         , DP.LPredictorsR F.⊆ DP.PSDataR k
         , DP.DCatsR F.⊆ DP.PSDataR k
         )
      => RunConfig l
      -> ModelConfig b
      -> [Text]
      -> SMB.StanBuilderM CESData (DP.PSData k) ()
model rc mc states = do
  mData <- evangelicalModelData mc
  paramSetup <- MC2.setupParameters Nothing states mc
  (MC2.Components modelM llM ppM centerF) <- MC2.components Nothing (MC.covariatesAndCounts mData) paramSetup mc.mcSurveyAggregation
  modelM
  when rc.rcIncludePPCheck $ void ppM
  when rc.rcIncludeLL llM
  case rc.rcPS of
    Nothing -> pure ()
    Just (gtt, pst) -> do
      psRowTag <- SMB.dataSetTag @(F.Record (DP.PSDataR k)) SC.GQData "PSData"
      (prefix, modP, modN) <- fmap psTypeToArgs $ psTypeDataGQ @k psRowTag pst
      MC2.postStratifyOne psRowTag (view DT.popCount) F.rcast prefix paramSetup modP modN mc.mcDesignMatrixRow (Just $ centerF SC.GQData) Nothing gtt >> pure ()

runModel :: forall l k r b .
            (K.KnitEffects r
            , BRKU.CacheEffects r
            , l F.⊆ DP.PSDataR k
            , F.ElemOf (DP.PSDataR k) DT.PopCount
            , DP.LPredictorsR F.⊆ DP.PSDataR k
            , V.RMap l
            , Ord (F.Record l)
            , FS.RecFlat l
            , Typeable (DP.PSDataR k)
            , F.ElemOf (DP.PSDataR k) GT.StateAbbreviation
            , DP.DCatsR F.⊆ DP.PSDataR k
            , DP.DCatsR F.⊆ CESByCDR
            , DP.LPredictorsR F.⊆ CESByCDR
            , Show (F.Record l)
            , Typeable l
            )
         => Either Text Text
         -> Text
         -> Text
         -> BR.CommandLine
         -> RunConfig l
         -> ModelConfig b
         -> K.ActionWithCacheTime r CESData
         -> K.ActionWithCacheTime r (DP.PSData k)
         -> K.Sem r (K.ActionWithCacheTime r (MC.PSMap l MT.ConfidenceInterval, MC2.ModelParameters))
runModel modelDirE modelName gqName _cmdLine runConfig config modelData_C psData_C = do
  let dataName = MC.modelConfigText config
      runnerInputNames = SC.RunnerInputNames
                         ("br-2023-TSP/stan/" <> modelName <> "/")
                         (MC.modelConfigText config)
                         (Just $ SC.GQNames "GQ" (dataName <> "_" <> gqName))
                         dataName
--  modelData <- K.ignoreCacheTime modelData_C
  states <- S.toList . FL.fold (FL.premap (view GT.stateAbbreviation) FL.set) . unCESData <$> K.ignoreCacheTime modelData_C
  psKeys <- S.toList . FL.fold (FL.premap (F.rcast @l) FL.set) . DP.unPSData <$> K.ignoreCacheTime psData_C
  (dw, code) <- SMR.dataWranglerAndCode modelData_C psData_C
                (groupBuilder config states psKeys)
                (model runConfig config states)

  let datSuffix = SC.rinData runnerInputNames
      jsonData t = "jsonData_" <> datSuffix <> "$" <> t
      evangelical = jsonData "Evangelical"
      surveyed = jsonData "Surveyed"
      rSuffix = SC.rinModel runnerInputNames <> "_" <> datSuffix
      unwraps = case config.mcSurveyAggregation of
        MC.WeightedAggregation MC.BetaProportion -> [SR.UnwrapExpr (evangelical <> " / " <> surveyed) ("yEvangelicalRate_" <> rSuffix)]
        _ -> [SR.UnwrapNamed "Evangelical" ("yEvangelical_" <> rSuffix)]
  res_C <- SMR.runModel' @BRKU.SerializerC @BRKU.CacheData
           modelDirE
           (Right runnerInputNames)
           Nothing
           dw
           code
           (modelResultAction config runConfig) --SC.DoNothing -- (stateModelResultAction mcWithId dmr)
           (SMR.Both unwraps) --(SMR.Both [SR.UnwrapNamed "successes" "yObserved"])
           modelData_C
           psData_C
  K.logLE K.Info $ modelName <> " run complete."
  pure res_C

groupBuilder :: forall g k l b .
                 (Foldable g
                 , Typeable (DP.PSDataR k)
                 , Show (F.Record l)
                 , Ord (F.Record l)
                 , l F.⊆ DP.PSDataR k
                 , Typeable l
                 , F.ElemOf (DP.PSDataR k) GT.StateAbbreviation
                 , DP.DCatsR F.⊆ DP.PSDataR k
                 )
               => ModelConfig b
               -> g Text
               -> g (F.Record l)
               -> SMB.StanGroupBuilderM CESData (DP.PSData k) ()
groupBuilder config states psKeys = do
  let groups' = MC.groups states
  SMB.addModelDataToGroupBuilder "CES" (SMB.ToFoldable unCESData) >>= MC.addGroupIndexesAndIntMaps groups'
  psGroupBuilder states psKeys

-- NB: often l ~ k, e.g., for predicting district turnout/preference
-- But say you want to predict turnout by race, nationally.
-- Now l ~ '[Race5C]
-- How about turnout by Education in each state? Then l ~ [StateAbbreviation, Education4C]
psGroupBuilder :: forall g k l md .
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
               -> SMB.StanGroupBuilderM md (DP.PSData k) ()
psGroupBuilder states psKeys = do
  let groups' = MC.groups states
      psGtt = MC.psGroupTag @l
  psRowTag <- SMB.addGQDataToGroupBuilder "PSData" (SMB.ToFoldable DP.unPSData)
  SMB.addGroupIndexForData psGtt psRowTag $ SMB.makeIndexFromFoldable show F.rcast psKeys
  SMB.addGroupIntMapForDataSet psGtt psRowTag $ SMB.dataToIntMapFromFoldable F.rcast psKeys
  SG.addModelIndexes psRowTag F.rcast groups'

--NB: parsed summary data has stan indexing, i.e., Arrays start at 1.
--NB: Will return no prediction (Nothing) for "both" model for now. Might eventually return both predictions?
modelResultAction :: forall k l r b .
                     (Ord (F.Record l)
                     , K.KnitEffects r
                     , Typeable (DP.PSDataR k)
                     , Typeable l
--                     , DP.LPredictorsR F.⊆ DP.CESByR lk
                     )
                  => ModelConfig b
                  -> RunConfig l
                  -> SC.ResultAction r CESData (DP.PSData k) SMB.DataSetGroupIntMaps () (MC.PSMap l MT.ConfidenceInterval, MC2.ModelParameters)
modelResultAction config runConfig = SC.UseSummary f where
  f summary _ modelDataAndIndexes_C gqDataAndIndexes_CM = do
    (modelData, resultIndexesE) <- K.ignoreCacheTime modelDataAndIndexes_C
     -- compute means of predictors because model was zero-centered in them
    let mdMeansFld :: DP.LPredictorsR F.⊆ rs
                   => DM.DesignMatrixRow (F.Record DP.LPredictorsR) -> FL.Fold (F.Record rs) [Double]
        mdMeansFld dmr =
          let covariates = DM.designMatrixRowF $ contramap F.rcast dmr
              nPredictors = DM.rowLength dmr
          in FL.premap (VU.toList . covariates)
             $ traverse (\n -> FL.premap (List.!! n) FL.mean) [0..(nPredictors - 1)]
        mdMeansLM :: DM.DesignMatrixRow (F.Record DP.LPredictorsR) -> [Double]
        mdMeansLM dmr = FL.fold (FL.premap (F.rcast @DP.LPredictorsR) $ mdMeansFld dmr) $ unCESData modelData
        getVector n = K.knitEither $ SP.getVector . fmap CS.mean <$> SP.parse1D n (CS.paramStats summary)
        betaSIF :: DM.DesignMatrixRow (F.Record DP.LPredictorsR) -> [Double] -> K.Sem r (VU.Vector (Double, Double))
        betaSIF dmr mdMeansL = do
          case DM.rowLength dmr of
            0 -> pure VU.empty
            p -> do
              betaV <- getVector "beta"
              pure $ VU.fromList $ zip (V.toList betaV) mdMeansL
    betaSIM <- betaSIF config.mcDesignMatrixRow $ mdMeansLM config.mcDesignMatrixRow
    psMap <- case runConfig.rcPS of
      Nothing -> mempty
      Just (gtt, pst) -> case gqDataAndIndexes_CM of
        Nothing -> K.knitError "modelResultAction: Expected gq data and indexes but got Nothing."
        Just gqDaI_C -> do
          let getVectorPcts n = K.knitEither $ SP.getVector . fmap CS.percents <$> SP.parse1D n (CS.paramStats summary)
          (_, gqIndexesE) <- K.ignoreCacheTime gqDaI_C
          grpIM <- K.knitEither
             $ gqIndexesE >>= SMB.getGroupIndex (SMB.RowTypeTag @(F.Record (DP.PSDataR k)) SC.GQData "PSData") (MC.psGroupTag @l)
          psTByGrpV <- getVectorPcts $ psPrefix pst <> "_byGrp"
          K.knitEither $ M.fromList . zip (IM.elems grpIM) <$> (traverse MT.listToCI $ V.toList psTByGrpV)
    pure $ (MC.PSMap psMap, MC2.ModelParameters betaSIM)

newtype CESData = CESData { unCESData :: F.FrameRec CESByCDR } deriving stock Generic

instance Flat.Flat CESData where
  size (CESData c) n = Flat.size (FS.SFrame c) n
  encode (CESData c) = Flat.encode (FS.SFrame c)
  decode = (\c → CESData (FS.unSFrame c)) <$> Flat.decode

cachedPreppedCES :: (K.KnitEffects r, BR.CacheEffects r)
                 => MR.CacheStructure () ()
                 -> K.Sem r (K.ActionWithCacheTime r CESData)
cachedPreppedCES cacheStructure = do
  cacheDirE' <- K.knitMaybe "cachedPreppedCES: Empty cacheDir given!" $ BRKU.insureFinalSlashE $ MR.csProjectCacheDirE cacheStructure
  rawCESByCD_C <- cesCountedEvangelicalsByCD False
  acs_C <- fmap (F.filterFrame ((== DT.Citizen) . view DT.citizenC)) <$> DDP.cachedACSa5ByCD ACS.acs1Yr2010_20 2020 -- so we get density from same year as survey
  let appendCacheFile :: Text -> Text -> Text
      appendCacheFile t d = d <> t
      cesByCDModelCacheE = bimap (appendCacheFile "CESModelData.bin") (appendCacheFile "CESByCDModelData.bin") cacheDirE'
  cacheKey <- case cesByCDModelCacheE of
    Left ck -> BR.clearIfPresentD ck >> pure ck
    Right ck -> pure ck
  let deps = (,) <$> rawCESByCD_C <*> acs_C -- <*> houseElections_C
  BR.retrieveOrMakeD cacheKey deps $ \(ces, acs) -> (fmap CESData $ cesAddDensity acs ces)

cesAddDensity :: (K.KnitEffects r)
              => F.FrameRec DDP.ACSa5ByCDR
              -> F.FrameRec (DP.CDKeyR V.++ DP.DCatsR V.++ CountDataR)
              -> K.Sem r (F.FrameRec CESByCDR)
cesAddDensity acs ces = K.wrapPrefix "TSP_Religion.Model.cesAddDensity" $ do
  K.logLE K.Info "Adding people-weighted pop density to CES"
  let fixSingleDistricts = BR.fixSingleDistricts ("DC" : BR.atLargeDistrictStates) 1
      (joined, missing) = FJ.leftJoinWithMissing @(DP.CDKeyR V.++ DP.DCatsR) ces
                          $ DP.withZeros @DP.CDKeyR @DP.DCatsR
                          $ fmap F.rcast $ fixSingleDistricts acs
  when (not $ null missing) $ do
--    BR.logFrame $ F.filterFrame ((== "DC") . view GT.stateAbbreviation) acs
    K.knitError $ "cesAddDensity: Missing keys in CES/ACS join: " <> show missing
  pure $ fmap F.rcast joined

cesCountedEvangelicalsByCD ∷ (K.KnitEffects r, BR.CacheEffects r)
                           ⇒ Bool
                           → K.Sem r (K.ActionWithCacheTime r (F.FrameRec (DP.CDKeyR V.++ DP.DCatsR V.++ CountDataR)))
cesCountedEvangelicalsByCD clearCaches = do
  ces2020_C ← CCES.ces20Loader
  let cacheKey = "model/TSP_Religion/ces20ByCD.bin"
  when clearCaches $ BR.clearIfPresentD cacheKey
  BR.retrieveOrMakeFrame cacheKey ces2020_C $ (fmap (fmap F.rcast) . cesMR @DP.CDKeyR 2020)

cesMR ∷ forall lk rs f m .
        (Foldable f, Functor f, Monad m
        , FC.ElemsOf rs [BR.Year, DT.EvangelicalC, DT.EducationC, DT.HispC, DT.Race5C, CCES.CESWeight]
        , rs F.⊆ (DT.Education4C ': rs)
        , (lk V.++ DP.DCatsR) V.++ CountDataR ~ ((lk V.++ DP.DCatsR) V.++ CountDataR)
        , Ord (F.Record (lk V.++ DP.DCatsR))
        , (lk V.++ DP.DCatsR) F.⊆ (DT.Education4C ': rs)
        , FI.RecVec ((lk V.++ DP.DCatsR) V.++ CountDataR)
        )
      ⇒ Int -> f (F.Record rs) → m (F.FrameRec (lk V.++ DP.DCatsR V.++ CountDataR))
cesMR earliestYear =
  BRF.frameCompactMR
  (FMR.unpackFilterOnField @BR.Year (>= earliestYear))
  (FMR.assignKeysAndData @(lk V.++ DP.DCatsR) @rs)
  countCESF
  . fmap (DP.cesAddEducation4 . DP.cesRecodeHispanic)

countCESF :: (FC.ElemsOf rs [DT.EvangelicalC, CCES.CESWeight])
          => FL.Fold
             (F.Record rs)
             (F.Record CountDataR)
countCESF =
  let wgt = view CCES.cESWeight
      evangelical = (== DT.Evangelical) . view DT.evangelicalC
      surveyedF = FL.length
      surveyWgtF = FL.premap wgt FL.sum
      lmvskSurveyedF = FL.premap wgt FLS.fastLMVSK
      essSurveyedF = DP.effSampleSizeFld lmvskSurveyedF
      evangelicalF = FL.prefilter evangelical FL.length
      waEvangelicalF = DP.wgtdAverageBoolFld wgt evangelical
   in (\sw s ev eS wEv →
          sw F.&: s F.&: ev F.&: eS F.&: min eS (eS * wEv) F.&: V.RNil)
      <$> surveyWgtF
      <*> surveyedF
      <*> evangelicalF
      <*> essSurveyedF
      <*> waEvangelicalF

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
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE UnicodeSyntax #-}
{-# LANGUAGE StandaloneDeriving #-}

module BlueRipple.Model.Election2.ModelRunner
  (
    module BlueRipple.Model.Election2.ModelRunner
  )
where

import qualified BlueRipple.Model.Election2.DataPrep as DP
import qualified BlueRipple.Model.Demographic.DataPrep as DDP
import qualified BlueRipple.Model.Election2.ModelCommon as MC
import qualified BlueRipple.Model.Election2.ModelCommon2 as MC2
import qualified BlueRipple.Data.DataFrames as BRDF
import qualified BlueRipple.Data.Loaders as BRDF
import qualified BlueRipple.Configuration as BR
import qualified BlueRipple.Utilities.KnitUtils as BRKU
import qualified BlueRipple.Data.GeographicTypes as GT
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.ModelingTypes as MT
import qualified BlueRipple.Data.ACS_PUMS as ACS
import qualified BlueRipple.Data.Keyed as Keyed
import qualified BlueRipple.Model.TurnoutAdjustment as TA

import qualified Knit.Report as K hiding (elements)
import qualified Path

import qualified Control.Foldl as FL
import Control.Lens (view, (^.), over)

import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V
import qualified Data.Set as Set
import qualified Data.Map.Strict as M
import qualified Data.Map.Merge.Strict as MM

import qualified Numeric

import qualified Frames as F
import qualified Frames.Melt as F
import qualified Frames.Transform as FT
import qualified Frames.SimpleJoins as FJ
import qualified Frames.Folds as FF
import qualified Frames.Streamly.TH as FTH
import qualified Frames.Streamly.InCore as FSI
import qualified Frames.Serialize as FS

import qualified Frames.MapReduce as FMR

import qualified Stan.ModelBuilder.DesignMatrix as DM
import Flat.Instances.Vector ()

import qualified Graphics.Vega.VegaLite as GV
import qualified Graphics.Vega.VegaLite.Compat as FV
import qualified Graphics.Vega.VegaLite.Configuration as FV
import qualified Graphics.Vega.VegaLite.JSON as VJ

FTH.declareColumn "ModelPr" ''Double
FTH.declareColumn "ModelCI" ''MT.ConfidenceInterval


cachedPreppedModelData :: (K.KnitEffects r, BRKU.CacheEffects r)
                       => Either Text Text -> K.Sem r (K.ActionWithCacheTime r (DP.ModelData DP.CDKeyR))
cachedPreppedModelData cacheDirE = do
  cacheDirE' <- K.knitMaybe "ModelRunner: Empty cacheDir given to runTurnoutModel" $ BRKU.insureFinalSlashE cacheDirE
  let appendCacheFile :: Text -> Text -> Text
      appendCacheFile t d = d <> t
      cpsModelCacheE = bimap (appendCacheFile "CPSModelData.bin") (appendCacheFile "CPSModelData.bin") cacheDirE'
      cesByStateModelCacheE = bimap (appendCacheFile "CESModelData.bin") (appendCacheFile "CESByStateModelData.bin") cacheDirE'
      cesByCDModelCacheE = bimap (appendCacheFile "CESModelData.bin") (appendCacheFile "CESByCDModelData.bin") cacheDirE'
  rawCESByCD_C <- DP.cesCountedDemPresVotesByCD False
  rawCESByState_C <- DP.cesCountedDemPresVotesByState False
  rawCPS_C <- DP.cpsCountedTurnoutByState
  DP.cachedPreppedModelDataCD cpsModelCacheE rawCPS_C cesByStateModelCacheE rawCESByState_C cesByCDModelCacheE rawCESByCD_C


runTurnoutModel :: (K.KnitEffects r
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
                -> Either Text Text
                -> Either Text Text
                -> Text
                -> BR.CommandLine
                -> MC.RunConfig l
                -> MC.TurnoutSurvey a
                -> MC.SurveyAggregation b
                -> DM.DesignMatrixRow (F.Record DP.LPredictorsR)
                -> MC.PSTargets
                -> MC.Alphas
                -> K.ActionWithCacheTime r (DP.PSData ks)
                -> K.Sem r (K.ActionWithCacheTime r (MC.PSMap l MT.ConfidenceInterval, Maybe MC2.ModelParameters))
runTurnoutModel year modelDirE cacheDirE gqName cmdLine runConfig ts sa dmr pst am psData_C = do
  let config = MC2.TurnoutOnly (MC.TurnoutConfig ts pst (MC.ModelConfig sa am dmr))
  modelData_C <- cachedPreppedModelData cacheDirE
  MC2.runModel modelDirE (MC.turnoutSurveyText ts <> "T_" <> show year) gqName cmdLine runConfig config modelData_C psData_C

psFold :: forall ks rs .
          (ks F.⊆ rs
          , Ord (F.Record ks)
          , rs F.⊆ rs
          , FSI.RecVec (ks V.++ [ModelPr, DT.PopCount])
          )
       => (F.Record rs -> Double)
       -> (F.Record rs -> Double)
       -> (F.Record rs -> Int)
       -> FL.Fold (F.Record rs) (F.FrameRec (ks V.++ [ModelPr, DT.PopCount]))
psFold num den pop = FMR.concatFold
                     $ FMR.mapReduceFold
                     FMR.noUnpack
                     (FMR.assignKeysAndData @ks @rs)
                     (FMR.foldAndAddKey innerFld)
  where
    innerFld :: FL.Fold (F.Record rs) (F.Record [ModelPr, DT.PopCount])
    innerFld =
      let nSum = FL.premap num FL.sum
          dSum = FL.premap den FL.sum
          popSum = FL.premap pop FL.sum
          safeDiv x y = if y > 0 then x / y else 0
      in (\n d p -> safeDiv n d F.&: p F.&: V.RNil) <$> nSum <*> dSum <*> popSum


adjustCI :: Double -> MT.ConfidenceInterval -> MT.ConfidenceInterval
adjustCI p (MT.ConfidenceInterval lo mid hi) = MT.ConfidenceInterval (f lo logitDelta) p (f hi logitDelta) where
  logitDelta | p == 0 || mid == 0 || p == 1 || mid == 1 = 0
             | otherwise = Numeric.log (p * (1 - mid) / (mid * (1 - p)))
  g x = 1 / (1 + Numeric.exp (negate x))
  f x y |  x == 0 = 0
        |  x == 1 = 1
        |  otherwise = g $ Numeric.log (x / (1 - x)) + y

{-
adjustCIR :: (F.ElemOf rs ModelPr, F.ElemOf rs ModelCI)
          => F.Record rs -> F.Record (F.RDeleteAll [ModelPr] rs)
adjustCIR r = F.rcast $ F.rputField @ModelCI newCI r where
  newCI = adjustCI (r ^. modelPr) (r ^. modelCI)
-}
runTurnoutModelAH :: forall l ks r a b .
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
                     , FJ.CanLeftJoinM  (GT.StateAbbreviation ': DP.DCatsR) (DP.PSDataR ks) ((GT.StateAbbreviation ': DP.DCatsR) V.++ '[ModelPr])
                     , F.ElemOf ((ks V.++ DP.PredictorsR V.++ '[DT.PopCount]) V.++ '[ModelPr]) DT.PWPopPerSqMile
                     , F.ElemOf ((ks V.++ DP.PredictorsR V.++ '[DT.PopCount]) V.++ '[ModelPr]) DT.PopCount
                     , F.ElemOf ((ks V.++ DP.PredictorsR V.++ '[DT.PopCount]) V.++ '[ModelPr]) ModelPr
                     , F.ElemOf ((ks V.++ DP.PredictorsR V.++ '[DT.PopCount]) V.++ '[ModelPr]) GT.StateAbbreviation
                     , F.ElemOf ((ks V.++ DP.PredictorsR V.++ '[DT.PopCount]) V.++ '[ModelPr]) DT.SexC
                     , F.ElemOf ((ks V.++ DP.PredictorsR V.++ '[DT.PopCount]) V.++ '[ModelPr]) DT.Age5C
                     , F.ElemOf ((ks V.++ DP.PredictorsR V.++ '[DT.PopCount]) V.++ '[ModelPr]) DT.Education4C
                     , F.ElemOf ((ks V.++ DP.PredictorsR V.++ '[DT.PopCount]) V.++ '[ModelPr]) DT.Race5C
                     , (GT.StateAbbreviation ': DP.DCatsR) F.⊆ ((ks V.++ DP.PredictorsR V.++ '[DT.PopCount]) V.++ '[ModelPr])
                     , FSI.RecVec (l V.++ '[ModelPr, DT.PopCount])
                     , l F.⊆ ((GT.StateAbbreviation ': DP.DCatsR) V.++ [ModelPr, DT.PopCount])
                     , l F.⊆ (l V.++ [ModelPr, DT.PopCount])
                     , F.ElemOf (l V.++ [ModelPr, DT.PopCount]) ModelPr
--                     , FSI.RecVec (ks V.++ DP.PredictorsR V.++ '[DT.PopCount, ModelPr])
                     )
                  => Int
                  -> Either Text Text
                  -> Either Text Text
                  -> Text
                  -> BR.CommandLine
                  -> MC.TurnoutSurvey a
                  -> MC.SurveyAggregation b
                  -> DM.DesignMatrixRow (F.Record DP.LPredictorsR)
                  -> MC.PSTargets
                  -> MC.Alphas
                  -> K.ActionWithCacheTime r (DP.PSData ks)
                  -> K.Sem r (K.ActionWithCacheTime r (MC.PSMap l MT.ConfidenceInterval))
runTurnoutModelAH year modelDirE cacheDirE gqName cmdLine ts sa dmr pst am psData_C = K.wrapPrefix "runTurnoutModelAH" $ do
--  turnoutRes_C <- runTurnoutModel year modelDirE cacheDirE gqName cmdLine runConfig ts sa dmr pst am psData_C
  modelData <- K.ignoreCacheTimeM $ cachedPreppedModelData cacheDirE
  let (allStates, avgPWPopPerSqMile) = case ts of
        MC.CESSurvey -> FL.fold ((,) <$> FL.premap (view GT.stateAbbreviation) FL.set <*> FL.premap (view DT.pWPopPerSqMile) FL.mean) modelData.cesData
        MC.CPSSurvey -> FL.fold ((,) <$> FL.premap (view GT.stateAbbreviation) FL.set <*> FL.premap (view DT.pWPopPerSqMile) FL.mean) modelData.cpsData
      acRunConfig = MC.RunConfig False False (Just $ MC.psGroupTag @(GT.StateAbbreviation ': DP.DCatsR))
  allCellProbsPS_C <-  BRKU.retrieveOrMakeD "model/election2/allCellProbsPS.bin" (pure ())
                       $ \_ -> pure $ allCellProbsPS allStates avgPWPopPerSqMile
  K.logLE K.Info "Running all cell model/post-stratification, if necessary"
  turnoutCP_C <- runTurnoutModel year modelDirE cacheDirE gqName cmdLine acRunConfig ts sa dmr pst am allCellProbsPS_C
  let stFilter r = r ^. BRDF.year == year && r ^. GT.stateAbbreviation /= "US"
  stateTurnout_C <- fmap (fmap (F.filterFrame stFilter)) BRDF.stateTurnoutLoader
  let cachePrefix = "model/election2/" <> MC.turnoutSurveyText ts <> show year <> "_" <> MC.aggregationText sa <> "_" <> MC.alphasText am <> "/"
      ahDeps = (,,) <$> turnoutCP_C <*> psData_C <*> stateTurnout_C
  turnoutCPAH_C <- BRKU.retrieveOrMakeFrame (cachePrefix <> gqName <> "_ACProbsAH.bin") ahDeps $ \((tCP, tMPm), psD, stateTurnout) -> do
    K.logLE K.Info "(Re)building AH adjusted all-cell probs."
    let probFrame =  fmap (\(ks, p) -> ks F.<+> FT.recordSingleton @ModelPr p) $ M.toList $ fmap MT.ciMid $ MC.unPSMap tCP
    tMP <- K.knitMaybe "runTurnoutModelAH: Nothing in turnout ModelParameters after allCellProbs run!" $ tMPm
    let (joined, missing) = FJ.leftJoinWithMissing
                            @(GT.StateAbbreviation ': DP.DCatsR)
                            @(DP.PSDataR ks)
                            @((GT.StateAbbreviation ': DP.DCatsR) V.++ '[ModelPr])
                            (DP.unPSData psD) (F.toFrame probFrame)
    when (not $ null missing) $ K.knitError $ "runTurnoutModelAH: missing keys in psData/prob-frame join: " <> show missing
    let densAdjProbFrame = fmap (MC2.adjustPredictionsForDensity (view modelPr) (over modelPr . const) tMP dmr) joined
    FL.foldM (TA.adjTurnoutFoldG @DT.PopCount @ModelPr @'[GT.StateAbbreviation] @_ @(DP.DCatsR V.++ [ModelPr, DT.PopCount])
               (view BRDF.ballotsCountedVEP) stateTurnout) (fmap F.rcast densAdjProbFrame)
  let psNum r = (realToFrac $ r ^. DT.popCount) * r ^. modelPr
      psDen r = realToFrac $ r ^. DT.popCount
      turnoutAHPS_C = fmap (FL.fold (psFold @l psNum psDen (view DT.popCount))) turnoutCPAH_C
      psRunConfig = MC.RunConfig False False (Just $ MC.psGroupTag @l)
  K.logLE K.Info "Running model/specific post-stratification, if necessary"
  turnoutPSForCI_C <- runTurnoutModel year modelDirE cacheDirE gqName cmdLine psRunConfig ts sa dmr pst am psData_C
  let resMapDeps = (,) <$> turnoutAHPS_C <*> turnoutPSForCI_C
  BRKU.retrieveOrMakeD (cachePrefix <> gqName <> "_resMap.bin") resMapDeps $ \(ahps, (cisM, _)) -> do
    K.logLE K.Info "merging AH probs and CIs"
    let psProbMap = M.fromList $ fmap (\r -> (F.rcast @l r, r ^. modelPr)) $ FL.fold FL.list ahps
        whenMatched _ p ci = Right $ adjustCI p ci
        whenMissingPS l _ = Left $ "runTurnoutModelAH:key present in model CIs is missing from AHPS: " <> show l
        whenMissingCI l _ = Left $ "runTurnoutModelAH:key present in AHPS is missing from CIs: " <> show l
    MC.PSMap
      <$> (K.knitEither
            $ MM.mergeA (MM.traverseMissing whenMissingPS) (MM.traverseMissing whenMissingCI) (MM.zipWithAMatched whenMatched) psProbMap (MC.unPSMap cisM))


runPrefModel :: (K.KnitEffects r
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
                , l F.⊆ DP.PSDataR ks
                , Show (F.Record l)
                )
             => Int
             -> Either Text Text
             -> Either Text Text
             -> Text
             -> BR.CommandLine
             -> MC.RunConfig l
             -> MC.SurveyAggregation b
             -> DM.DesignMatrixRow (F.Record DP.LPredictorsR)
             -> MC.PSTargets
             -> MC.Alphas
             -> K.ActionWithCacheTime r (DP.PSData ks)
             -> K.Sem r (K.ActionWithCacheTime r (MC.PSMap l MT.ConfidenceInterval, Maybe MC2.ModelParameters))
runPrefModel year modelDirE cacheDirE gqName cmdLine runConfig sa dmr pst am psData_C = do
  let config = MC2.PrefOnly (MC.PrefConfig pst (MC.ModelConfig sa am dmr))
  modelData_C <- cachedPreppedModelData cacheDirE
  MC2.runModel modelDirE ("P_" <> show year) gqName cmdLine runConfig config modelData_C psData_C

runFullModel :: (K.KnitEffects r
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
                , l F.⊆ DP.PSDataR ks
                , Show (F.Record l)
                )
             => Int
             -> Either Text Text
             -> Either Text Text
             -> Text
             -> BR.CommandLine
             -> MC.RunConfig l
             -> MC.TurnoutSurvey a
             -> MC.SurveyAggregation b
             -> DM.DesignMatrixRow (F.Record DP.LPredictorsR)
             -> MC.PSTargets
             -> MC.Alphas
             -> K.ActionWithCacheTime r (DP.PSData ks)
             -> K.Sem r (K.ActionWithCacheTime r (MC.PSMap l MT.ConfidenceInterval, Maybe MC2.ModelParameters))
runFullModel year modelDirE cacheDirE gqName cmdLine runConfig ts sa dmr pst am psData_C = do
  let config = MC2.TurnoutAndPref (MC.TurnoutConfig ts pst (MC.ModelConfig sa am dmr)) (MC.PrefConfig pst (MC.ModelConfig sa am dmr))
  modelData_C <- cachedPreppedModelData cacheDirE
--  acsByState_C <- fmap (DP.PSData @'[GT.StateAbbreviation] . fmap F.rcast) <$> DDP.cachedACSa5ByState ACS.acs1Yr2012_21 2021
  MC2.runModel modelDirE (MC.turnoutSurveyText ts <> "_VS_" <> show year) gqName cmdLine runConfig config modelData_C psData_C


allCellProbsPS :: Set Text -> Double -> DP.PSData (GT.StateAbbreviation ': DP.DCatsR)
allCellProbsPS states avgPWDensity =
  let catRec ::  DT.Age5 -> DT.Sex -> DT.Education4 -> DT.Race5 -> F.Record DP.DCatsR
      catRec a s e r = a F.&: s F.&: e F.&: r F.&: V.RNil
      densRec :: F.Record '[DT.PWPopPerSqMile]
      densRec = FT.recordSingleton avgPWDensity
      popRec :: F.Record '[DT.PopCount]
      popRec = FT.recordSingleton 1
      allCellRec :: Text -> DT.Age5 -> DT.Sex -> DT.Education4 -> DT.Race5
                 -> F.Record (DP.PSDataR (GT.StateAbbreviation ': DP.DCatsR))
      allCellRec st a s e r = (st F.&: catRec a s e r) F.<+> densRec F.<+> catRec a s e r F.<+> popRec
      allCellRecList = [allCellRec st a s e r | st <- Set.toList states
                                              , a <- Set.toList Keyed.elements
                                              , s <- Set.toList Keyed.elements
                                              , e <- Set.toList Keyed.elements
                                              , r <- Set.toList Keyed.elements
                                              ]
  in DP.PSData $ F.toFrame allCellRecList

modelCompBy :: forall ks r b . (K.KnitEffects r, BRKU.CacheEffects r, PSByC ks)
            => (MC.RunConfig ks -> Text -> MC.SurveyAggregation b -> MC.Alphas -> MC.PSTargets -> K.Sem r (K.ActionWithCacheTime r (MC.PSMap ks MT.ConfidenceInterval)))
            -> Text -> MC.SurveyAggregation b -> MC.Alphas -> MC.PSTargets -> K.Sem r (Text, F.FrameRec (ks V.++ '[DT.PopCount, ModelCI]))
modelCompBy runModel catLabel agg am pt = do
          comp <- psBy @ks (\rc -> runModel rc catLabel agg am pt)
          pure (MC.aggregationText agg <> "_" <> MC.alphasText am <> (if (pt == MC.PSTargets) then "_PSTgt" else ""), comp)

allModelsCompBy :: forall ks r b . (K.KnitEffects r, BRKU.CacheEffects r, PSByC ks)
                => (MC.RunConfig ks -> Text -> MC.SurveyAggregation b -> MC.Alphas -> MC.PSTargets -> K.Sem r (K.ActionWithCacheTime r (MC.PSMap ks MT.ConfidenceInterval)))
                -> Text -> [MC.SurveyAggregation b] -> [MC.Alphas] -> [MC.PSTargets] -> K.Sem r [(Text, F.FrameRec (ks V.++ '[DT.PopCount, ModelCI]))]
allModelsCompBy runModel catLabel aggs' alphaModels' psTs' =
  traverse (\(agg, am, pt) -> modelCompBy @ks runModel catLabel agg am pt) [(agg, am, pt) | agg <- aggs',  am <- alphaModels', pt <- psTs']

allModelsCompChart :: forall ks r b . (K.KnitOne r, BRKU.CacheEffects r, PSByC ks, Keyed.FiniteSet (F.Record ks)
                                        , F.ElemOf (ks V.++ [DT.PopCount, ModelCI]) DT.PopCount
                                        , F.ElemOf (ks V.++ [DT.PopCount, ModelCI]) ModelCI
                                        , ks F.⊆ (ks V.++ [DT.PopCount, ModelCI])
                                        )
                   => BR.PostPaths Path.Abs
                   -> BR.PostInfo
                   -> (MC.RunConfig ks -> Text -> MC.SurveyAggregation b -> MC.Alphas -> MC.PSTargets -> K.Sem r (K.ActionWithCacheTime r (MC.PSMap ks MT.ConfidenceInterval)))
                   -> Text
                   -> Text
                   -> (F.Record ks -> Text)
                   -> [MC.SurveyAggregation b]
                   -> [MC.Alphas]
                   -> [MC.PSTargets]
                   -> K.Sem r ()
allModelsCompChart pp postInfo runModel catLabel modelType catText aggs' alphaModels' psTs' = do
  allModels <- allModelsCompBy @ks runModel catLabel aggs' alphaModels' psTs'
  let cats = Set.toList $ Keyed.elements @(F.Record ks)
      _numCats = length cats
      numSources = length allModels
  catCompChart <- categoryChart @ks pp postInfo (modelType <> " Comparison By Category") (modelType <> "Comp")
                  (FV.ViewConfig 300 (30 * realToFrac numSources) 10) (Just cats) (Just $ fmap fst allModels)
                  catText allModels
  _ <- K.addHvega Nothing Nothing catCompChart
  pure ()

type PSByC ks = (Show (F.Record ks)
                 , Typeable ks
                 , V.RMap ks
                 , Ord (F.Record ks)
                 , ks F.⊆ DP.PSDataR '[GT.StateAbbreviation]
                 , ks F.⊆ DDP.ACSa5ByStateR
                 , ks F.⊆ (ks V.++ '[DT.PopCount])
                 , F.ElemOf (ks V.++ '[DT.PopCount]) DT.PopCount
                 , FSI.RecVec (ks V.++ '[DT.PopCount])
                 , FSI.RecVec (ks V.++ '[DT.PopCount, ModelCI])
                 , FS.RecFlat ks)

psBy :: forall ks r .
         (PSByC ks
         , K.KnitEffects r
         , BRKU.CacheEffects r
         )
      => (MC.RunConfig ks -> K.Sem r (K.ActionWithCacheTime r (MC.PSMap ks MT.ConfidenceInterval)))
      -> K.Sem r (F.FrameRec (ks V.++ [DT.PopCount, ModelCI]))
psBy runModel = do
    let runConfig = MC.RunConfig False False (Just $ MC.psGroupTag @ks)
    (MC.PSMap psMap) <- K.ignoreCacheTimeM $ runModel runConfig
    pcMap <- DDP.cachedACSa5ByState ACS.acs1Yr2012_21 2021 >>= popCountByMap @ks
    let whenMatched :: F.Record ks -> MT.ConfidenceInterval -> Int -> Either Text (F.Record  (ks V.++ [DT.PopCount, ModelCI]))
        whenMatched k t p = pure $ k F.<+> (p F.&: t F.&: V.RNil :: F.Record [DT.PopCount, ModelCI])
        whenMissingPC k _ = Left $ "psBy: " <> show k <> " is missing from PopCount map."
        whenMissingT k _ = Left $ "psBy: " <> show k <> " is missing from ps map."
    mergedMap <- K.knitEither
                 $ MM.mergeA (MM.traverseMissing whenMissingPC) (MM.traverseMissing whenMissingT) (MM.zipWithAMatched whenMatched) psMap pcMap
    pure $ F.toFrame $ M.elems mergedMap


psByState ::  (K.KnitEffects r, BRKU.CacheEffects r)
          => (MC.RunConfig '[GT.StateAbbreviation] -> K.Sem r (K.ActionWithCacheTime r (MC.PSMap '[GT.StateAbbreviation] MT.ConfidenceInterval)))
          -> (F.FrameRec [GT.StateAbbreviation, DT.PopCount, ModelCI] -> K.Sem r (F.FrameRec ([GT.StateAbbreviation, DT.PopCount, ModelCI] V.++ bs)))
          -> K.Sem r (F.FrameRec ([GT.StateAbbreviation, DT.PopCount, ModelCI] V.++ bs))
psByState runModel addStateFields = psBy @'[GT.StateAbbreviation] runModel >>= addStateFields

popCountBy :: forall ks rs r .
              (--K.KnitEffects r
--              , BRKU.CacheEffects r
                ks F.⊆ rs, F.ElemOf rs DT.PopCount, Ord (F.Record ks)
              , FSI.RecVec (ks V.++ '[DT.PopCount])
              )
           => K.ActionWithCacheTime r (F.FrameRec rs)
           -> K.Sem r (F.FrameRec (ks V.++ '[DT.PopCount]))
popCountBy counts_C = do
  counts <- K.ignoreCacheTime counts_C
  let aggFld = FMR.concatFold
               $ FMR.mapReduceFold
               FMR.noUnpack
               (FMR.assignKeysAndData @ks @'[DT.PopCount])
               (FMR.foldAndAddKey $ FF.foldAllConstrained @Num FL.sum)
  pure $ FL.fold aggFld counts

popCountByMap :: forall ks rs r .
                 (K.KnitEffects r
--                 , BRKU.CacheEffects r
                 , ks F.⊆ rs
                 , F.ElemOf rs DT.PopCount
                 , Ord (F.Record ks)
                 , ks F.⊆ (ks V.++ '[DT.PopCount])
                 , F.ElemOf (ks V.++ '[DT.PopCount])  DT.PopCount
                 , FSI.RecVec (ks V.++ '[DT.PopCount])
                 )
              => K.ActionWithCacheTime r (F.FrameRec rs)
              -> K.Sem r (Map (F.Record ks) Int)
popCountByMap = fmap (FL.fold (FL.premap keyValue FL.map)) . popCountBy @ks where
  keyValue r = (F.rcast @ks r, r ^. DT.popCount)




surveyDataBy :: forall ks rs a .
                (Ord (F.Record ks)
                , ks F.⊆ rs
                , DP.CountDataR F.⊆ rs
                , FSI.RecVec (ks V.++ '[ModelPr])
                )
             => Maybe (MC.SurveyAggregation a) -> F.FrameRec rs -> F.FrameRec (ks V.++ '[ModelPr])
surveyDataBy saM = FL.fold fld
  where
    safeDiv :: Double -> Double -> Double
    safeDiv x y = if (y /= 0) then x / y else 0
    uwInnerFld :: FL.Fold (F.Record DP.CountDataR) (F.Record '[ModelPr])
    uwInnerFld =
      let sF = fmap realToFrac $ FL.premap (view DP.surveyed) FL.sum
          vF = fmap realToFrac $ FL.premap (view DP.voted) FL.sum
      in (\s v -> safeDiv v s F.&: V.RNil) <$> sF <*> vF
    mwInnerFld :: FL.Fold (F.Record DP.CountDataR) (F.Record '[ModelPr])
    mwInnerFld =
      let sF = FL.premap (view DP.surveyedW) FL.sum
          vF = FL.premap (view DP.votedW) FL.sum
      in (\s v -> safeDiv v s F.&: V.RNil) <$> sF <*> vF
    wInnerFld :: FL.Fold (F.Record DP.CountDataR) (F.Record '[ModelPr])
    wInnerFld =
      let swF = FL.premap (view DP.surveyWeight) FL.sum
          swvF = FL.premap (\r -> view DP.surveyWeight r * realToFrac (view DP.voted r) / realToFrac (view DP.surveyed r)) FL.sum
      in (\s v -> safeDiv v s F.&: V.RNil) <$> swF <*> swvF

    innerFld :: FL.Fold (F.Record DP.CountDataR) (F.Record '[ModelPr])
    innerFld = case saM of
      Nothing -> mwInnerFld
      Just sa -> case sa of
        MC.UnweightedAggregation -> uwInnerFld
        MC.WeightedAggregation _ _ -> wInnerFld
    fld :: FL.Fold (F.Record rs) (F.FrameRec (ks V.++ '[ModelPr]))
    fld = FMR.concatFold
          $ FMR.mapReduceFold
          FMR.noUnpack
          (FMR.assignKeysAndData @ks @DP.CountDataR)
          (FMR.foldAndAddKey innerFld)

addBallotsCountedVAP :: (K.KnitEffects r, BRKU.CacheEffects r
                        , FJ.CanLeftJoinM '[GT.StateAbbreviation] rs BRDF.StateTurnoutCols
                        , F.ElemOf (rs V.++ (F.RDelete GT.StateAbbreviation BRDF.StateTurnoutCols)) GT.StateAbbreviation
                        , rs V.++ '[BRDF.BallotsCountedVAP, BRDF.VAP] F.⊆ (rs V.++ (F.RDelete GT.StateAbbreviation BRDF.StateTurnoutCols))
                        )
                     => F.FrameRec rs ->  K.Sem r (F.FrameRec (rs V.++ '[BRDF.BallotsCountedVAP, BRDF.VAP]))
addBallotsCountedVAP fr = do
  turnoutByState <- F.filterFrame ((== 2020) . view BRDF.year) <$> K.ignoreCacheTimeM BRDF.stateTurnoutLoader
  let (joined, missing) = FJ.leftJoinWithMissing @'[GT.StateAbbreviation] fr turnoutByState
  when (not $ null missing) $ K.knitError $ "addBallotysCOuntedVAP: missing keys in state turnout target join: " <> show missing
  pure $ fmap F.rcast joined

addBallotsCountedVEP :: (K.KnitEffects r, BRKU.CacheEffects r
                        , FJ.CanLeftJoinM '[GT.StateAbbreviation] rs BRDF.StateTurnoutCols
                        , F.ElemOf (rs V.++ (F.RDelete GT.StateAbbreviation BRDF.StateTurnoutCols)) GT.StateAbbreviation
                        , rs V.++ '[BRDF.BallotsCountedVEP, BRDF.VAP] F.⊆ (rs V.++ (F.RDelete GT.StateAbbreviation BRDF.StateTurnoutCols))
                        )
                     => F.FrameRec rs ->  K.Sem r (F.FrameRec (rs V.++ '[BRDF.BallotsCountedVEP, BRDF.VAP]))
addBallotsCountedVEP fr = do
  turnoutByState <- F.filterFrame ((== 2020) . view BRDF.year) <$> K.ignoreCacheTimeM BRDF.stateTurnoutLoader
  let (joined, missing) = FJ.leftJoinWithMissing @'[GT.StateAbbreviation] fr turnoutByState
  when (not $ null missing) $ K.knitError $ "addBallotysCOuntedVAP: missing keys in state turnout target join: " <> show missing
  pure $ fmap F.rcast joined



modelCIToModelPr :: (F.RDelete ModelCI rs V.++ '[ModelPr] F.⊆ ('[ModelPr] V.++ rs)
                    , F.ElemOf rs ModelCI)
                 => F.Record rs -> F.Record (F.RDelete ModelCI rs V.++ '[ModelPr])
modelCIToModelPr r = F.rcast $ FT.recordSingleton @ModelPr (MT.ciMid $ view modelCI r) F.<+> r

stateChart :: (K.KnitEffects r, F.ElemOf rs GT.StateAbbreviation, F.ElemOf rs ModelPr)
           => BR.PostPaths Path.Abs
           -> BR.PostInfo
           -> Text
           -> Text
           -> Text
           -> FV.ViewConfig
           -> (F.Record rs -> Int)
           -> Maybe (F.Record rs -> Double)
           -> [(Text, F.FrameRec rs)]
           -> K.Sem r GV.VegaLite
stateChart postPaths' postInfo chartID title modelType vc vap tgtM tableRowsByModel = do
  let colData (t, r)
        = [("State", GV.Str $ r ^. GT.stateAbbreviation)
          , ("VAP", GV.Number $ realToFrac $ vap r)
          , (modelType, GV.Number $ 100 * r ^. modelPr)
          , ("Source", GV.Str t)
          ] ++ case tgtM of
                 Nothing -> []
                 Just tgt ->
                   [ ("Tgt " <> modelType, GV.Number $ 100 * tgt r)
                   , ("Model - Tgt", GV.Number $ 100 * (r ^. modelPr - tgt r))
                   ]

--      toData kltr = fmap ($ kltr) $ fmap colData [0..(n-1)]
      jsonRows = FL.fold (VJ.rowsToJSON colData [] Nothing) $ concat $ fmap (\(s, fr) -> fmap (s,) $ FL.fold FL.list fr) tableRowsByModel
  jsonFilePrefix <- K.getNextUnusedId $ ("statePSWithTargets_" <> chartID)
  jsonUrl <-  BRKU.brAddJSON postPaths' postInfo jsonFilePrefix jsonRows

  let vlData = GV.dataFromUrl jsonUrl [GV.JSON "values"]
  --
      encY = case tgtM of
        Nothing -> GV.position GV.Y [GV.PName modelType, GV.PmType GV.Quantitative]
        Just _ -> GV.position GV.Y [GV.PName "Model - Tgt", GV.PmType GV.Quantitative]
      encScatter = GV.encoding
        . GV.position GV.X [GV.PName "State", GV.PmType GV.Nominal]
        . encY
        . GV.color [GV.MName "Source", GV.MmType GV.Nominal]
        . GV.size [GV.MName "VAP", GV.MmType GV.Quantitative]
      markScatter = GV.mark GV.Circle [GV.MTooltip GV.TTEncoding]
      scatterSpec = GV.asSpec [encScatter [], markScatter]
      layers = GV.layer [scatterSpec]
  pure $ FV.configuredVegaLite vc [FV.title title
                                  , layers
                                  , vlData
                                  ]



categoryChart :: forall ks rs r . (K.KnitEffects r, F.ElemOf rs DT.PopCount, F.ElemOf rs ModelCI, ks F.⊆ rs)
           => BR.PostPaths Path.Abs
           -> BR.PostInfo
           -> Text
           -> Text
           -> FV.ViewConfig
           -> Maybe [F.Record ks]
           -> Maybe [Text]
           -> (F.Record ks -> Text)
           -> [(Text, F.FrameRec rs)] --[k, TurnoutCI, DT.PopCount])]
           -> K.Sem r GV.VegaLite
categoryChart postPaths' postInfo title chartID vc catSortM sourceSortM catText tableRowsByModel = do
  let colData (t, r)
        = [("Category", GV.Str $ catText $ F.rcast r)
          , ("Ppl", GV.Number $ realToFrac  $ r ^. DT.popCount)
          , ("Lo", GV.Number $ MT.ciLower $ r ^. modelCI)
          , ("Mid", GV.Number $ MT.ciMid $ r ^. modelCI)
          , ("Hi", GV.Number $ MT.ciUpper $ r ^. modelCI)
          , ("Source", GV.Str t)
          ]

--      toData kltr = fmap ($ kltr) $ fmap colData [0..(n-1)]
      jsonRows = FL.fold (VJ.rowsToJSON colData [] Nothing) $ concat $ fmap (\(s, fr) -> fmap (s,) $ FL.fold FL.list fr) tableRowsByModel
  jsonFilePrefix <- K.getNextUnusedId $ ("statePSWithTargets_" <> chartID)
  jsonUrl <-  BRKU.brAddJSON postPaths' postInfo jsonFilePrefix jsonRows

  let vlData = GV.dataFromUrl jsonUrl [GV.JSON "values"]
  --
      xScale = GV.PScale [GV.SZero False]
      xSort = case sourceSortM of
        Nothing -> []
        Just so -> [GV.PSort [GV.CustomSort $ GV.Strings so]]
      facetSort = case catSortM of
        Nothing -> []
        Just so -> [GV.FSort [GV.CustomSort $ GV.Strings $ fmap catText so]]
      encMid = GV.encoding
        . GV.position GV.Y [GV.PName "Source", GV.PmType GV.Nominal]
        . GV.position GV.X ([GV.PName "Mid", GV.PmType GV.Quantitative, xScale] <> xSort)
--        . GV.color [GV.MName "Source", GV.MmType GV.Nominal]
--        . GV.size [GV.MName "Ppl", GV.MmType GV.Quantitative]
      markMid = GV.mark GV.Circle [GV.MTooltip GV.TTEncoding]
      midSpec = GV.asSpec [encMid [], markMid]
      encError = GV.encoding
        . GV.position GV.Y [GV.PName "Source", GV.PmType GV.Nominal]
        . GV.position GV.X ([GV.PName "Lo", GV.PmType GV.Quantitative, xScale] <> xSort)
        . GV.position GV.X2 ([GV.PName "Hi", GV.PmType GV.Quantitative, xScale] <> xSort)
  --      . GV.color [GV.MName "Source", GV.MmType GV.Nominal]
      markError = GV.mark GV.ErrorBar [GV.MTooltip GV.TTEncoding]
      errorSpec = GV.asSpec [encError [], markError]
      layers = GV.layer [midSpec, errorSpec]
  pure $ FV.configuredVegaLite vc [FV.title title
                                  , GV.facet [GV.RowBy ([GV.FName "Category", GV.FmType GV.Nominal] <> facetSort)]
                                  , GV.specification (GV.asSpec [layers])
                                  , vlData
                                  ]

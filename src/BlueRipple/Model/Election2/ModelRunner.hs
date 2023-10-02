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
import qualified BlueRipple.Data.ElectionTypes as ET
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
import Data.Type.Equality (type (~))
import qualified Numeric

import qualified Frames as F
import qualified Frames.Melt as F
import qualified Frames.Transform as FT
import qualified Frames.SimpleJoins as FJ
import qualified Frames.Constraints as FC
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
FTH.declareColumn "PSNumer" ''Double
FTH.declareColumn "PSDenom" ''Double
FTH.declareColumn "ModelCI" ''MT.ConfidenceInterval
FTH.declareColumn "ModelT" ''Double
FTH.declareColumn "ModelP" ''Double

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


runTurnoutModel :: forall l r ks a b .
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
                -> K.Sem r (K.ActionWithCacheTime r (MC.PSMap l MT.ConfidenceInterval, Maybe MC2.ModelParameters))
runTurnoutModel year modelDirE cacheDirE gqName cmdLine ts sa dmr pst am psData_C = do
  let config = MC2.TurnoutOnly (MC.TurnoutConfig ts pst (MC.ModelConfig sa am dmr))
      runConfig = MC.RunConfig False False (Just $ MC.psGroupTag @l)
  modelData_C <- cachedPreppedModelData cacheDirE
  MC2.runModel modelDirE (MC.turnoutSurveyText ts <> "T_" <> show year) gqName cmdLine runConfig config modelData_C psData_C

type PSResultR = [ModelPr, DT.PopCount, PSNumer, PSDenom]

psFold :: forall ks rs .
          (ks F.⊆ rs
          , Ord (F.Record ks)
          , rs F.⊆ rs
          , FSI.RecVec (ks V.++ PSResultR)
          )
       => (F.Record rs -> Double)
       -> (F.Record rs -> Double)
       -> (F.Record rs -> Int)
       -> FL.Fold (F.Record rs) (F.FrameRec (ks V.++ PSResultR))
psFold num den pop = FMR.concatFold
                     $ FMR.mapReduceFold
                     FMR.noUnpack
                     (FMR.assignKeysAndData @ks @rs)
                     (FMR.foldAndAddKey innerFld)
  where
    innerFld :: FL.Fold (F.Record rs) (F.Record PSResultR)
    innerFld =
      let nSum = FL.premap num FL.sum
          dSum = FL.premap den FL.sum
          popSum = FL.premap pop FL.sum
          safeDiv x y = if y > 0 then x / y else 0
      in (\n d p -> safeDiv n d F.&: p F.&: n F.&: d F.&: V.RNil) <$> nSum <*> dSum <*> popSum


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
type StateAndCats = (GT.StateAbbreviation ': DP.DCatsR)
type StateCatsPlus as = StateAndCats V.++ as

type JoinR ks ms = FJ.JoinResult StateAndCats (DP.PSDataR ks) (StateCatsPlus ms)

type AHrs ks ms = F.RDeleteAll '[GT.StateAbbreviation] (JoinR ks ms)
type AH ks ms = GT.StateAbbreviation ': AHrs ks ms


type PSTypeC l ks ms = (V.RMap l
                       , Ord (F.Record l)
                       , FS.RecFlat l
                       , Typeable l
                       , l F.⊆ DP.PSDataR ks --'[GT.StateAbbreviation]
                       , Show (F.Record l)
                       --                     , l F.⊆ (StateAndCats V.++ [ModelPr, DT.PopCount])
                       , l F.⊆ (l V.++ PSResultR)
                       , F.ElemOf (l V.++ PSResultR) ModelPr
                       , FSI.RecVec (l V.++ PSResultR)
                       , l F.⊆ AH ks ms
                     )



type AHC ks ms = (FC.ElemsOf (AHrs ks ms) ('[DT.PopCount] V.++ ms V.++ DP.DCatsR)
                 , FSI.RecVec (AHrs ks ms)
                 , FS.RecFlat (AHrs ks ms)
                 , V.RMap (AHrs ks ms)
                 , AHrs ks ms F.⊆ AH ks ms
                 , F.ElemOf (((ks V.++ (DT.PWPopPerSqMile ': DP.DCatsR)) V.++ '[DT.PopCount]) V.++ ms) GT.StateAbbreviation
                 , V.RMap ks
                 , Show (F.Record ks)
                 , Ord (F.Record ks)
                 , ks F.⊆ (ks V.++ (DP.DCatsR V.++ [ModelPr, DT.PopCount]))
                 , AHrs ks ms F.⊆ (((ks V.++ (DT.PWPopPerSqMile ': DP.DCatsR)) V.++ '[DT.PopCount]) V.++ ms)
                 , ks V.++ (DP.DCatsR V.++ PSResultR) F.⊆ (ks V.++ (DP.DCatsR V.++ PSResultR))
                 )

type PSDataTypeTC ks = ( Typeable (DP.PSDataR ks)
                       , FC.ElemsOf (DP.PSDataR ks) [GT.StateAbbreviation, DT.PopCount]
                       , DP.LPredictorsR F.⊆ DP.PSDataR ks
                       , DP.DCatsR F.⊆ DP.PSDataR ks
                       , FJ.CanLeftJoinWithMissing StateAndCats (DP.PSDataR ks) (StateCatsPlus '[ModelPr])
                       , FC.ElemsOf (JoinR ks '[ModelPr]) [DT.PWPopPerSqMile, ModelPr, DT.PopCount]
                       , StateCatsPlus [ModelPr, DT.PopCount] F.⊆ JoinR ks '[ModelPr]
                       , AHC ks '[ModelPr]
                       )

turnoutModelCPs :: forall r a b .
                     (K.KnitEffects r
                     , BRKU.CacheEffects r
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
                  -> K.Sem r (K.ActionWithCacheTime r (MC.PSMap StateAndCats MT.ConfidenceInterval, Maybe MC2.ModelParameters))
turnoutModelCPs year modelDirE cacheDirE acName cmdLine ts sa dmr pst am = K.wrapPrefix "turnoutModelAHCPs" $ do
  modelData <- K.ignoreCacheTimeM $ cachedPreppedModelData cacheDirE
  let (allStates, avgPWPopPerSqMile) = case ts of
        MC.CESSurvey -> FL.fold ((,) <$> FL.premap (view GT.stateAbbreviation) FL.set <*> FL.premap (view DT.pWPopPerSqMile) FL.mean) modelData.cesData
        MC.CPSSurvey -> FL.fold ((,) <$> FL.premap (view GT.stateAbbreviation) FL.set <*> FL.premap (view DT.pWPopPerSqMile) FL.mean) modelData.cpsData
  allCellProbsPS_C <-  BRKU.retrieveOrMakeD ("model/election2/allCellProbs" <> MC.turnoutSurveyText ts <> "_PS.bin") (pure ())
                       $ \_ -> pure $ allCellProbsPS allStates avgPWPopPerSqMile
  K.logLE K.Info "Running all cell model/post-stratification, if necessary"
  runTurnoutModel @StateAndCats year modelDirE cacheDirE acName cmdLine ts sa dmr pst am allCellProbsPS_C

runTurnoutModelCPAH :: forall ks r a b .
                       (K.KnitEffects r
                       , BRKU.CacheEffects r
                       , PSDataTypeTC ks
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
                    -> Text
                    -> K.ActionWithCacheTime r (DP.PSData ks)
                    -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec (AH ks '[ModelPr])))
runTurnoutModelCPAH year modelDirE cacheDirE gqName cmdLine ts sa dmr pst am acName psData_C = K.wrapPrefix "runTurnoutModelCPAH" $ do
  turnoutCPs_C <- turnoutModelCPs year modelDirE cacheDirE acName cmdLine ts sa dmr pst am
  let stFilter r = r ^. BRDF.year == year && r ^. GT.stateAbbreviation /= "US"
  stateTurnout_C <- fmap (fmap (F.filterFrame stFilter)) BRDF.stateTurnoutLoader
  let cachePrefix = "model/election2/Turnout/" <> MC.turnoutSurveyText ts <> show year <> "_" <> MC.aggregationText sa <> "_" <> MC.alphasText am <> "/"
      ahDeps = (,,) <$> turnoutCPs_C <*> psData_C <*> stateTurnout_C
  BRKU.retrieveOrMakeFrame (cachePrefix <> gqName <> "_ACProbsAH.bin") ahDeps $ \((tCP, tMPm), psD, stateTurnout) -> do
    K.logLE K.Info "(Re)building AH adjusted all-cell probs."
    let probFrame =  fmap (\(ks, p) -> ks F.<+> FT.recordSingleton @ModelPr p) $ M.toList $ fmap MT.ciMid $ MC.unPSMap tCP
    tMP <- K.knitMaybe "runTurnoutModelAH: Nothing in turnout ModelParameters after allCellProbs run!" $ tMPm
    let (joined, missing) = FJ.leftJoinWithMissing
                            @StateAndCats
                            @(DP.PSDataR ks)
                            @(StateCatsPlus '[ModelPr])
                            (DP.unPSData psD) (F.toFrame probFrame)
    when (not $ null missing) $ K.knitError $ "runTurnoutModelAH: missing keys in psData/prob-frame join: " <> show missing
    let densAdjProbFrame = fmap (MC2.adjustPredictionsForDensity (view modelPr) (over modelPr . const) tMP dmr) joined
    FL.foldM (TA.adjTurnoutFoldG @ModelPr @'[GT.StateAbbreviation] @_  @(AHrs ks '[ModelPr])
               (realToFrac . view DT.popCount) (view BRDF.ballotsCountedVEP) stateTurnout) (fmap F.rcast densAdjProbFrame)


runTurnoutModelAH :: forall l ks r a b .
                     (K.KnitEffects r
                     , BRKU.CacheEffects r
                     , PSTypeC l ks '[ModelPr]
                     , PSDataTypeTC ks
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
                  -> Text
                  -> K.ActionWithCacheTime r (DP.PSData ks)
                  -> K.Sem r (K.ActionWithCacheTime r (MC.PSMap l MT.ConfidenceInterval))
runTurnoutModelAH year modelDirE cacheDirE gqName cmdLine ts sa dmr pst am acName psData_C = K.wrapPrefix "runTurnoutModelAH" $ do
  let cachePrefix = "model/election2/Turnout/" <> MC.turnoutSurveyText ts <> show year <> "_" <> MC.aggregationText sa <> "_" <> MC.alphasText am <> "/"
  turnoutCPAH_C <- runTurnoutModelCPAH year modelDirE cacheDirE gqName cmdLine ts sa dmr pst am acName psData_C
  let psNum r = (realToFrac $ r ^. DT.popCount) * r ^. modelPr
      psDen r = realToFrac $ r ^. DT.popCount
      turnoutAHPS_C = fmap (FL.fold (psFold @l psNum psDen (view DT.popCount))) turnoutCPAH_C
  K.logLE K.Info "Running model/specific post-stratification, if necessary"
  turnoutPSForCI_C <- runTurnoutModel @l year modelDirE cacheDirE gqName cmdLine ts sa dmr pst am psData_C
  let resMapDeps = (,) <$> turnoutAHPS_C <*> turnoutPSForCI_C
  BRKU.retrieveOrMakeD (cachePrefix <> gqName <> "_resMap.bin") resMapDeps $ \(ahps, (cisM, _)) -> do
    K.logLE K.Info "merging AH probs and CIs"
    let psProbMap = M.fromList $ fmap (\r -> (F.rcast @l r, r ^. modelPr)) $ FL.fold FL.list ahps
        whenMatched _ p ci = Right $ adjustCI p ci
        whenMissingPS l _ = Left $ "runTurnoutModelAH: key present in model CIs is missing from AHPS: " <> show l
        whenMissingCI l _ = Left $ "runTurnoutModelAH: key present in AHPS is missing from CIs: " <> show l
    MC.PSMap
      <$> (K.knitEither
            $ MM.mergeA (MM.traverseMissing whenMissingPS) (MM.traverseMissing whenMissingCI) (MM.zipWithAMatched whenMatched) psProbMap (MC.unPSMap cisM))


runPrefModel :: forall l r ks b .
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
                , l F.⊆ DP.PSDataR ks
                , Show (F.Record l)
                )
             => Int
             -> Either Text Text
             -> Either Text Text
             -> Text
             -> BR.CommandLine
             -> MC.SurveyAggregation b
             -> DM.DesignMatrixRow (F.Record DP.LPredictorsR)
             -> MC.PSTargets
             -> MC.Alphas
             -> K.ActionWithCacheTime r (DP.PSData ks)
             -> K.Sem r (K.ActionWithCacheTime r (MC.PSMap l MT.ConfidenceInterval, Maybe MC2.ModelParameters))
runPrefModel year modelDirE cacheDirE gqName cmdLine sa dmr pst am psData_C = do
  let config = MC2.PrefOnly (MC.PrefConfig pst (MC.ModelConfig sa am dmr))
      runConfig = MC.RunConfig False False (Just $ MC.psGroupTag @l)
  modelData_C <- cachedPreppedModelData cacheDirE
  MC2.runModel modelDirE ("P_" <> show year) gqName cmdLine runConfig config modelData_C psData_C

prefModelCPs :: forall r b .
                (K.KnitEffects r
                , BRKU.CacheEffects r
                )
             => Int
             -> Either Text Text
             -> Either Text Text
             -> Text
             -> BR.CommandLine
             -> MC.SurveyAggregation b
             -> DM.DesignMatrixRow (F.Record DP.LPredictorsR)
             -> MC.PSTargets
             -> MC.Alphas
             -> K.Sem r (K.ActionWithCacheTime r (MC.PSMap StateAndCats MT.ConfidenceInterval, Maybe MC2.ModelParameters))
prefModelCPs year modelDirE cacheDirE acName cmdLine sa dmr pst am = K.wrapPrefix "prefModelAHCPs" $ do
  modelData <- K.ignoreCacheTimeM $ cachedPreppedModelData cacheDirE
  let (allStates, avgPWPopPerSqMile) = FL.fold ((,) <$> FL.premap (view GT.stateAbbreviation) FL.set <*> FL.premap (view DT.pWPopPerSqMile) FL.mean) modelData.cesData
  allCellProbsPS_C <-  BRKU.retrieveOrMakeD ("model/election2/allCellProbsCES_PS.bin") (pure ())
                       $ \_ -> pure $ allCellProbsPS allStates avgPWPopPerSqMile
  K.logLE K.Info "Running all cell model/post-stratification, if necessary"
  runPrefModel @StateAndCats year modelDirE cacheDirE acName cmdLine sa dmr pst am allCellProbsPS_C

type JoinPR ks = FJ.JoinResult3 StateAndCats (DP.PSDataR ks) (StateCatsPlus '[ModelPr]) (StateCatsPlus '[ModelT])

type PSDataTypePC ks = ( FJ.CanLeftJoinWithMissing3 StateAndCats (DP.PSDataR ks) (StateCatsPlus '[ModelPr]) (StateCatsPlus '[ModelT])
                       , FC.ElemsOf (JoinPR ks) [DT.PWPopPerSqMile, ModelPr, ModelT]
                       , StateCatsPlus [ModelPr, ModelT, DT.PopCount] F.⊆ JoinPR ks
                       , FC.ElemsOf (JoinPR ks) [GT.StateAbbreviation, ModelT]
                       , AHC ks '[ModelPr, ModelT]
                       , FC.ElemsOf (FT.Rename "ModelPr" "ModelT" (AH ks '[ModelPr])) (StateCatsPlus '[ModelT])
                       , (GT.StateAbbreviation ': AHrs ks [ModelPr, ModelT]) F.⊆ JoinPR ks
                       )

runPrefModelCPAH :: forall ks es r a b .
                  (K.KnitEffects r
                  , BRKU.CacheEffects r
                  , PSDataTypeTC ks
                  , PSDataTypePC ks
                  , FC.ElemsOf es [BRDF.Year, GT.StateAbbreviation, ET.Party, ET.Votes, ET.TotalVotes]
                  , FSI.RecVec es
                  )
               => Int
               -> Either Text Text
               -> Either Text Text
               -> Text
               -> BR.CommandLine
               -> MC.TurnoutSurvey a  -- we need a turnout model for the AH adjustment
               -> MC.SurveyAggregation b
               -> DM.DesignMatrixRow (F.Record DP.LPredictorsR)
               -> MC.PSTargets
               -> MC.Alphas
               -> Int
               -> K.ActionWithCacheTime r (F.FrameRec es)
               -> Text
               -> K.ActionWithCacheTime r (DP.PSData ks)
               -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec (AH ks '[ModelPr, ModelT])))
runPrefModelCPAH year modelDirE cacheDirE gqName cmdLine ts sa dmr pst am elexTgtYear electionData_C acName psData_C = K.wrapPrefix "ruPrefModelCPAH" $ do
  let cachePrefix = "model/election2/Pref/CES" <> show year <> "_" <> MC.aggregationText sa <> "_" <> MC.alphasText am <> "/"
  turnoutCPAH_C <- runTurnoutModelCPAH year modelDirE cacheDirE gqName cmdLine ts sa dmr pst am acName psData_C
  prefCPs_C <- prefModelCPs year modelDirE cacheDirE acName cmdLine sa dmr pst am
--  turnoutCPAH_C <- runTurnoutModelAH
  let elexFilter r = r ^. BRDF.year == elexTgtYear && r ^. ET.party == ET.Democratic
      elections_C = fmap (F.filterFrame elexFilter) electionData_C
      ahDeps = (,,,) <$> turnoutCPAH_C <*> prefCPs_C <*> psData_C <*> elections_C
  BRKU.retrieveOrMakeFrame (cachePrefix <> gqName <> "_ACProbsAH.bin") ahDeps $ \(tCPF, (pCP, pMPm), psD, elex) -> do
    K.logLE K.Info "(Re)building AH adjusted all-cell probs."
    let probFrame =  fmap (\(ks, p) -> ks F.<+> FT.recordSingleton @ModelPr p) $ M.toList $ fmap MT.ciMid $ MC.unPSMap pCP
        turnoutFrame = fmap (F.rcast @(StateCatsPlus '[ModelT]) . FT.rename @"ModelPr" @"ModelT") tCPF
    tMP <- K.knitMaybe "runTurnoutPrefAH: Nothing in pref ModelParameters after allCellProbs run!" $ pMPm
    let (joined, missingPSPref, missingT) = FJ.leftJoin3WithMissing
                            @StateAndCats
                            (DP.unPSData psD) (F.toFrame probFrame) turnoutFrame
    when (not $ null missingPSPref) $ K.knitError $ "runPrefModelAH: missing keys in psData/prob-frame join: " <> show missingPSPref
    when (not $ null missingT) $ K.knitError $ "runPrefModelAH: missing keys in psData+prob-frame/turnout frame join: " <> show missingT
    let adjForDensity = MC2.adjustPredictionsForDensity (view modelPr) (over modelPr . const) tMP dmr
        densAdjProbFrame = fmap adjForDensity joined
        dVotesFrac r = realToFrac (r ^. ET.votes) / realToFrac (r ^. ET.totalVotes)
        modeledVoters r = r ^. modelT * realToFrac (r ^. DT.popCount)
    FL.foldM
      (TA.adjTurnoutFoldG @ModelPr @'[GT.StateAbbreviation] @_ @(AHrs ks [ModelPr, ModelT]) modeledVoters dVotesFrac elex)
      (fmap F.rcast densAdjProbFrame)

runPrefModelAH :: forall l ks es r a b .
                  (K.KnitEffects r
                  , BRKU.CacheEffects r
                  , PSTypeC l ks '[ModelPr, ModelT]
                  , PSDataTypeTC ks
                  , PSDataTypePC ks
                  , FC.ElemsOf es [BRDF.Year, GT.StateAbbreviation, ET.Party, ET.Votes, ET.TotalVotes]
                  , FSI.RecVec es
                  )
               => Int
               -> Either Text Text
               -> Either Text Text
               -> Text
               -> BR.CommandLine
               -> MC.TurnoutSurvey a  -- we need a turnout model for the AH adjustment
               -> MC.SurveyAggregation b
               -> DM.DesignMatrixRow (F.Record DP.LPredictorsR)
               -> MC.PSTargets
               -> MC.Alphas
               -> Int
               -> K.ActionWithCacheTime r (F.FrameRec es)
               -> Text
               -> K.ActionWithCacheTime r (DP.PSData ks)
               -> K.Sem r (K.ActionWithCacheTime r (MC.PSMap l MT.ConfidenceInterval))
runPrefModelAH year modelDirE cacheDirE gqName cmdLine ts sa dmr pst am elexTgtYear electionData_C acName psData_C = K.wrapPrefix "ruPrefModelAH" $ do
  let cachePrefix = "model/election2/Pref/CES" <> show year <> "_" <> MC.aggregationText sa <> "_" <> MC.alphasText am <> "/"
  prefCPAH_C <- runPrefModelCPAH year modelDirE cacheDirE gqName cmdLine ts sa dmr pst am elexTgtYear electionData_C acName psData_C
  let psNum r = (realToFrac $ r ^. DT.popCount) * r ^. modelPr
      psDen r = realToFrac $ r ^. DT.popCount
      prefAHPS_C = fmap (FL.fold (psFold @l psNum psDen (view DT.popCount))) prefCPAH_C
  K.logLE K.Info "Running model/specific post-stratification, if necessary"
  prefPSForCI_C <- runPrefModel @l year modelDirE cacheDirE gqName cmdLine sa dmr pst am psData_C
  let resMapDeps = (,) <$> prefAHPS_C <*> prefPSForCI_C
  BRKU.retrieveOrMakeD (cachePrefix <> gqName <> "_resMap.bin") resMapDeps $ \(ahps, (cisM, _)) -> do
    K.logLE K.Info "merging AH probs and CIs"
    let psProbMap = M.fromList $ fmap (\r -> (F.rcast @l r, r ^. modelPr)) $ FL.fold FL.list ahps
        whenMatched _ p ci = Right $ adjustCI p ci
        whenMissingPS l _ = Left $ "runPrefModelAH: key present in model CIs is missing from AHPS: " <> show l
        whenMissingCI l _ = Left $ "runPrefModelAH: key present in AHPS is missing from CIs: " <> show l
    MC.PSMap
      <$> (K.knitEither
            $ MM.mergeA (MM.traverseMissing whenMissingPS) (MM.traverseMissing whenMissingCI) (MM.zipWithAMatched whenMatched) psProbMap (MC.unPSMap cisM))

voteShareFrame :: FC.ElemsOf es [BRDF.Year, GT.StateAbbreviation, ET.Party, ET.Votes, ET.TotalVotes]
               => F.FrameRec es
               -> Map (F.Record [BRDF.Year, GT.StateAbbreviation]) Double
               -> F.FrameRec [BRDF.Year, GT.StateAbbreviation, ET.DemShare]
voteShareFrame elexFrame overrides =
  FMR.concatFold
  $ FMR.mapReduceFold
  FMR.noUnpack
  (FMR.assignKeysAndData @[BRDF.Year, GT.StateAbbreviation] @[ET.Party, ET.Votes, ET.TotalVotes])
  (FMR.ReduceFold innerFld)
  where
    keyedShare k x = k F.<+> FT.recordSingleton @ET.DemShare x
    safeDiv x y = if y > 0 then x /y else 0
    innerFld :: F.Record [BRDF.Year, GT.StateAbbreviation]
             -> FL.Fold (F.Record [ET.Party, ET.Votes, ET.TotalVotes]) (F.Record [BRDF.Year, GT.StateAbbreviation, ET.DemShare])
    innerFld k = case M.lookup k overrides of
      Just x -> pure $ keyedShare k x
      Nothing ->
        let ifP p r = r ^. ET.Party == p
            ifD = ifP ET.Democratic
            ifBoth r = ifP ET.Democratic r && ifP ET.Republican r
            votes = view ET.votes
            totalBoth = FL.prefilter ifBoth (FL.premap votes FL.sum)
            totalD = FL.prefilter ifD (FL.premap votes FL.sum)
        in (\td tb -> keyedShare k (safeDiv td tb)) <$> totalD <*> totalBoth



runFullModel :: forall l r ks a b .
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
                , l F.⊆ DP.PSDataR ks
                , Show (F.Record l)
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
             -> K.Sem r (K.ActionWithCacheTime r (MC.PSMap l MT.ConfidenceInterval, Maybe MC2.ModelParameters))
runFullModel year modelDirE cacheDirE gqName cmdLine ts sa dmr pst am psData_C = do
  let config = MC2.TurnoutAndPref (MC.TurnoutConfig ts pst (MC.ModelConfig sa am dmr)) (MC.PrefConfig pst (MC.ModelConfig sa am dmr))
      runConfig = MC.RunConfig False False (Just $ MC.psGroupTag @l)
  modelData_C <- cachedPreppedModelData cacheDirE
--  acsByState_C <- fmap (DP.PSData @'[GT.StateAbbreviation] . fmap F.rcast) <$> DDP.cachedACSa5ByState ACS.acs1Yr2012_21 2021
  MC2.runModel modelDirE (MC.turnoutSurveyText ts <> "_VS_" <> show year) gqName cmdLine runConfig config modelData_C psData_C

--type ToJoinTR ks = FT.Rename "ModelPr" "ModelT" (AH ks '[ModelPr]) --(ks V.++ (DP.DCatsR V.++ [ModelPr, DT.PopCount]))
type ToJoinPR ks = FT.Rename "ModelPr" "ModelP" (AH ks '[ModelPr, ModelT]) --(ks V.++ (DP.DCatsR V.++ [ModelPr, DT.PopCount]))
--type Join3R ks = FJ.JoinResult3 (ks V.++ DP.DCatsR) (DP.PSDataR ks) (ToJoinTR ks) (ToJoinPR ks)
type JoinFR ks = FJ.JoinResult (ks V.++ DP.DCatsR) (DP.PSDataR ks) (ToJoinPR ks)

runFullModelAH :: forall l ks es r a b .
                  (K.KnitEffects r
                  , BRKU.CacheEffects r
                  , PSTypeC l ks '[ModelPr, ModelT]
                  , PSDataTypeTC ks
                  , PSDataTypePC ks
                  , FC.ElemsOf es [BRDF.Year, GT.StateAbbreviation, ET.Party, ET.Votes, ET.TotalVotes]
                  , FSI.RecVec es
                  , FJ.CanLeftJoinWithMissing  (ks V.++ DP.DCatsR) (DP.PSDataR ks) (ToJoinPR ks)
                  , l F.⊆ JoinFR ks
                  , JoinFR ks F.⊆ JoinFR ks
                  , FC.ElemsOf (JoinFR ks) [DT.PopCount, ModelT, ModelP]
                  , V.RMap (l V.++ PSResultR)
                  , FS.RecFlat (l V.++ PSResultR)
                  , Show (F.Record (ks V.++ DP.DCatsR))
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
               -> Int
               -> K.ActionWithCacheTime r (F.FrameRec es)
               -> K.ActionWithCacheTime r (DP.PSData ks)
               -> K.Sem r (K.ActionWithCacheTime r (MC.PSMap l MT.ConfidenceInterval))
runFullModelAH year modelDirE cacheDirE gqName cmdLine ts sa dmr pst am elexTgtYear electionData_C psData_C = K.wrapPrefix "runFullModelAH" $ do
--  let cachePrefixT = "model/election2/Turnout/" <> MC.turnoutSurveyText ts <> show year <> "_" <> MC.aggregationText sa <> "_" <> MC.alphasText am <> "/"
--  turnoutCPAH_C <- runTurnoutModelCPAH year modelDirE cacheDirE gqName cmdLine ts sa dmr pst am "AllCells" psData_C
  prefCPAH_C <- runPrefModelCPAH year modelDirE cacheDirE gqName cmdLine ts sa dmr pst am elexTgtYear electionData_C "AllCells" psData_C
  let cachePrefix = "model/election2/Full/" <> MC.turnoutSurveyText ts <> show year <> "_" <> MC.aggregationText sa <> "_" <> MC.alphasText am <> "/"
      joinDeps = (,) <$> prefCPAH_C <*> psData_C
  fullAHPS_C <- BRKU.retrieveOrMakeFrame (cachePrefix <> gqName <> "_PS.bin") joinDeps $ \(pref, psData) -> do
    K.logLE K.Info "Doing 3-way join..."
    let-- turnoutFrame = fmap (FT.rename @"ModelPr" @"ModelT") turnout
        prefFrame = fmap (FT.rename @"ModelPr" @"ModelP") pref
        (joined, missing) = FJ.leftJoinWithMissing
                            @(ks V.++ DP.DCatsR)
                            (DP.unPSData psData) prefFrame
    when (not $ null missing) $ K.knitError $ "runFullModelAH: Missing keys in psData/prefCPAH join: " <> show missing
    K.logLE K.Info "Doing post-stratification..."
    let ppl r = realToFrac $ r ^. DT.popCount
        t r = r ^. modelT
        p r = r ^. modelP
        psNum r = ppl r * t r * p r
        psDen r = ppl r * t r
    pure $ FL.fold (psFold @l psNum psDen (view DT.popCount)) joined
  K.logLE K.Info "Running model/specific post-stratification, if necessary"
  fullPSForCI_C <- runFullModel @l year modelDirE cacheDirE gqName cmdLine ts sa dmr pst am psData_C

  let resMapDeps = (,) <$> fullAHPS_C <*> fullPSForCI_C
  BRKU.retrieveOrMakeD (cachePrefix <> gqName <> "_resMap.bin") resMapDeps $ \(ahps, (cisM, _)) -> do
    K.logLE K.Info "merging AH probs and CIs"
    let psProbMap = M.fromList $ fmap (\r -> (F.rcast @l r, r ^. modelPr)) $ FL.fold FL.list ahps
        whenMatched _ p ci = Right $ adjustCI p ci
        whenMissingPS l _ = Left $ "runFullModelAH: key present in model CIs is missing from AHPS: " <> show l
        whenMissingCI l _ = Left $ "runFullModelAH: key present in AHPS is missing from CIs: " <> show l
    MC.PSMap
      <$> (K.knitEither
            $ MM.mergeA (MM.traverseMissing whenMissingPS) (MM.traverseMissing whenMissingCI) (MM.zipWithAMatched whenMatched) psProbMap (MC.unPSMap cisM))

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
            => (Text -> MC.SurveyAggregation b -> MC.Alphas -> MC.PSTargets -> K.Sem r (K.ActionWithCacheTime r (MC.PSMap ks MT.ConfidenceInterval)))
            -> Text -> MC.SurveyAggregation b -> MC.Alphas -> MC.PSTargets -> K.Sem r (Text, F.FrameRec (ks V.++ '[DT.PopCount, ModelCI]))
modelCompBy runModel catLabel agg am pt = do
          comp <- psBy @ks (runModel catLabel agg am pt)
          pure (MC.aggregationText agg <> "_" <> MC.alphasText am <> (if (pt == MC.PSTargets) then "_PSTgt" else ""), comp)

allModelsCompBy :: forall ks r b . (K.KnitEffects r, BRKU.CacheEffects r, PSByC ks)
                => (Text -> MC.SurveyAggregation b -> MC.Alphas -> MC.PSTargets -> K.Sem r (K.ActionWithCacheTime r (MC.PSMap ks MT.ConfidenceInterval)))
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
                   -> (Text -> MC.SurveyAggregation b -> MC.Alphas -> MC.PSTargets -> K.Sem r (K.ActionWithCacheTime r (MC.PSMap ks MT.ConfidenceInterval)))
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
      => K.Sem r (K.ActionWithCacheTime r (MC.PSMap ks MT.ConfidenceInterval))
      -> K.Sem r (F.FrameRec (ks V.++ [DT.PopCount, ModelCI]))
psBy runModel = do
    (MC.PSMap psMap) <- K.ignoreCacheTimeM runModel
    pcMap <- DDP.cachedACSa5ByState ACS.acs1Yr2012_21 2021 >>= popCountByMap @ks
    let whenMatched :: F.Record ks -> MT.ConfidenceInterval -> Int -> Either Text (F.Record  (ks V.++ [DT.PopCount, ModelCI]))
        whenMatched k t p = pure $ k F.<+> (p F.&: t F.&: V.RNil :: F.Record [DT.PopCount, ModelCI])
        whenMissingPC k _ = Left $ "psBy: " <> show k <> " is missing from PopCount map."
        whenMissingT k _ = Left $ "psBy: " <> show k <> " is missing from ps map."
    mergedMap <- K.knitEither
                 $ MM.mergeA (MM.traverseMissing whenMissingPC) (MM.traverseMissing whenMissingT) (MM.zipWithAMatched whenMatched) psMap pcMap
    pure $ F.toFrame $ M.elems mergedMap


psByState ::  (K.KnitEffects r, BRKU.CacheEffects r)
          => (K.Sem r (K.ActionWithCacheTime r (MC.PSMap '[GT.StateAbbreviation] MT.ConfidenceInterval)))
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

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

module BlueRipple.Model.Election2.TurnoutModel
  (
    module BlueRipple.Model.Election2.TurnoutModel
  )
where

import qualified BlueRipple.Configuration as BR
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

import qualified Knit.Report as K hiding (elements)
import qualified Path

import qualified Control.Foldl as FL
import Control.Lens (view, (^.))
import qualified Data.IntMap.Strict as IM
import qualified Data.Map.Strict as M
import qualified Data.Set as S

import qualified Data.List as List
--import qualified Data.Vector as V
--import qualified Data.Vector.Unboxed as VU
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V

import qualified Frames as F
import qualified Frames.Melt as F
import qualified Frames.Transform as FT
import qualified Frames.SimpleJoins as FJ
import qualified Frames.Streamly.TH as FTH
import qualified Frames.Streamly.InCore as FSI
import qualified Frames.Serialize as FS

import qualified Frames.MapReduce as FMR

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

import qualified Graphics.Vega.VegaLite as GV
import qualified Graphics.Vega.VegaLite.Compat as FV
import qualified Graphics.Vega.VegaLite.Configuration as FV
import qualified Graphics.Vega.VegaLite.JSON as VJ

{-
runTurnoutModel :: (K.KnitEffects r
                   , BRKU.CacheEffects r
                   , V.RMap l
                   , Ord (F.Record l)
                   , FS.RecFlat l
                   , Typeable l
                   , l F.⊆ DP.PSDataR '[GT.StateAbbreviation]
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
                -> DM.DesignMatrixRow (F.Record DP.PredictorsR)
                -> MC.PSTargets
                -> MC.StateAlpha
                -> K.Sem r (K.ActionWithCacheTime r (MC.TurnoutPrediction, MC.PSMap l MT.ConfidenceInterval))
runTurnoutModel year modelDirE cacheDirE gqName cmdLine runConfig ts sa dmr pst sam = do
  let modelConfig = MC.TurnoutConfig ts sa pst dmr sam
  rawCES_C <- DP.cesCountedDemPresVotesByCD False
  cpCES_C <-  DP.cachedPreppedCES (Right "model/election2/test/CESTurnoutModelDataRaw.bin") rawCES_C
  rawCPS_C <- DP.cpsCountedTurnoutByState
  cpCPS_C <- DP.cachedPreppedCPS (Right "model/election2/test/CPSTurnoutModelDataRaw.bin") rawCPS_C
  modelData_C <- DP.cachedPreppedModelData
                 (Right "model/election2/test/CPSTurnoutModelData.bin") rawCPS_C
                 (Right "model/election2/test/CESTurnoutModelData.bin") rawCES_C
  acsByState_C <- fmap (DP.PSData @'[GT.StateAbbreviation] . fmap F.rcast) <$> DDP.cachedACSa5ByState
  MC.runModel modelDirE (MC.turnoutSurveyText ts <> "Turnout_" <> show year) gqName cmdLine runConfig modelConfig modelData_C acsByState_C
-}

runTurnoutModel2 :: (K.KnitEffects r
                   , BRKU.CacheEffects r
                   , V.RMap l
                   , Ord (F.Record l)
                   , FS.RecFlat l
                   , Typeable l
                   , l F.⊆ DP.PSDataR '[GT.StateAbbreviation]
                   , Show (F.Record l)
--                   , F.ElemOf (DP.PSDataR k) GT.StateAbbreviation
--                   , DP.DCatsR F.⊆ DP.PSDataR k
                   )
                => Int
                -> Either Text Text
                -> Either Text Text
                -> Text
                -> BR.CommandLine
                -> MC.RunConfig l
                -> MC.TurnoutSurvey a
                -> MC.SurveyAggregation b
                -> DM.DesignMatrixRow (F.Record DP.PredictorsR)
                -> MC.PSTargets
                -> MC2.Alphas
                -> K.Sem r (K.ActionWithCacheTime r (MC.PSMap l MT.ConfidenceInterval))
runTurnoutModel2 year modelDirE cacheDirE gqName cmdLine runConfig ts sa dmr pst am = do
  let modelConfig = MC2.TurnoutConfig ts sa pst am dmr
  rawCES_C <- DP.cesCountedDemPresVotesByCD False
  cpCES_C <-  DP.cachedPreppedCES (Right "model/election2/test/CESTurnoutModelDataRaw.bin") rawCES_C
  rawCPS_C <- DP.cpsCountedTurnoutByState
  cpCPS_C <- DP.cachedPreppedCPS (Right "model/election2/test/CPSTurnoutModelDataRaw.bin") rawCPS_C
  modelData_C <- DP.cachedPreppedModelData
                 (Right "model/election2/test/CPSTurnoutModelData.bin") rawCPS_C
                 (Right "model/election2/test/CESTurnoutModelData.bin") rawCES_C
  acsByState_C <- fmap (DP.PSData @'[GT.StateAbbreviation] . fmap F.rcast) <$> DDP.cachedACSa5ByState
  MC2.runModel modelDirE (MC.turnoutSurveyText ts <> "Turnout2_" <> show year) gqName cmdLine runConfig modelConfig modelData_C acsByState_C

FTH.declareColumn "TurnoutP" ''Double
FTH.declareColumn "TurnoutCI" ''MT.ConfidenceInterval

surveyDataBy :: forall ks rs a .
                (Ord (F.Record ks)
                , ks F.⊆ rs
                , DP.CountDataR F.⊆ rs
                , FSI.RecVec (ks V.++ '[TurnoutP])
                )
             => Maybe (MC.SurveyAggregation a) -> F.FrameRec rs -> F.FrameRec (ks V.++ '[TurnoutP])
surveyDataBy saM = FL.fold fld
  where
    safeDiv :: Double -> Double -> Double
    safeDiv x y = if (y /= 0) then x / y else 0
    uwInnerFld :: FL.Fold (F.Record DP.CountDataR) (F.Record '[TurnoutP])
    uwInnerFld =
      let sF = fmap realToFrac $ FL.premap (view DP.surveyed) FL.sum
          vF = fmap realToFrac $ FL.premap (view DP.voted) FL.sum
      in (\s v -> safeDiv v s F.&: V.RNil) <$> sF <*> vF
    mwInnerFld :: FL.Fold (F.Record DP.CountDataR) (F.Record '[TurnoutP])
    mwInnerFld =
      let sF = FL.premap (view DP.surveyedW) FL.sum
          vF = FL.premap (view DP.votedW) FL.sum
      in (\s v -> safeDiv v s F.&: V.RNil) <$> sF <*> vF
    wInnerFld :: FL.Fold (F.Record DP.CountDataR) (F.Record '[TurnoutP])
    wInnerFld =
      let swF = FL.premap (view DP.surveyWeight) FL.sum
          swvF = FL.premap (\r -> view DP.surveyWeight r * realToFrac (view DP.voted r) / realToFrac (view DP.surveyed r)) FL.sum
      in (\s v -> safeDiv v s F.&: V.RNil) <$> swF <*> swvF

    innerFld :: FL.Fold (F.Record DP.CountDataR) (F.Record '[TurnoutP])
    innerFld = case saM of
      Nothing -> mwInnerFld
      Just sa -> case sa of
        MC.UnweightedAggregation -> uwInnerFld
        MC.WeightedAggregation _ -> wInnerFld
    fld :: FL.Fold (F.Record rs) (F.FrameRec (ks V.++ '[TurnoutP]))
    fld = FMR.concatFold
          $ FMR.mapReduceFold
          FMR.noUnpack
          (FMR.assignKeysAndData @ks @DP.CountDataR)
          (FMR.foldAndAddKey innerFld)


addBallotsCountedVAP :: (K.KnitEffects r, BRKU.CacheEffects r
                        , F.ElemOf rs GT.StateAbbreviation
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


turnoutCIToTurnoutP :: (F.RDelete TurnoutCI rs V.++ '[TurnoutP] F.⊆ ('[TurnoutP] V.++ rs)
                       , F.ElemOf rs TurnoutCI)
                    => F.Record rs -> F.Record (F.RDelete TurnoutCI rs V.++ '[TurnoutP])
turnoutCIToTurnoutP r = F.rcast $ FT.recordSingleton @TurnoutP (MT.ciMid $ view turnoutCI r) F.<+> r



stateChart :: K.KnitEffects r
           => BR.PostPaths Path.Abs
           -> BR.PostInfo
           -> Text
           -> Text
           -> FV.ViewConfig
           -> [(Text, F.FrameRec ([GT.StateAbbreviation, BRDF.VAP, TurnoutP, BRDF.BallotsCountedVAP]))]
           -> K.Sem r GV.VegaLite
stateChart postPaths' postInfo title chartID vc tableRowsByModel = do
  let colData (t, r)
        = [("State", GV.Str $ r ^. GT.stateAbbreviation)
          , ("VAP", GV.Number $ realToFrac  $ r ^. BRDF.vAP)
          , ("SEP Turnout", GV.Number $ 100 * r ^. BRDF.ballotsCountedVAP)
          , ("Modeled Turnout", GV.Number $ 100 * r ^. turnoutP)
          , ("Model - SEP", GV.Number $ 100 * (r ^. turnoutP - r ^. BRDF.ballotsCountedVAP) )
          , ("Source", GV.Str t)
          ]

--      toData kltr = fmap ($ kltr) $ fmap colData [0..(n-1)]
      jsonRows = FL.fold (VJ.rowsToJSON colData [] Nothing) $ concat $ fmap (\(s, fr) -> fmap (s,) $ FL.fold FL.list fr) tableRowsByModel
  jsonFilePrefix <- K.getNextUnusedId $ ("statePSWithTargets_" <> chartID)
  jsonUrl <-  BRKU.brAddJSON postPaths' postInfo jsonFilePrefix jsonRows

  let vlData = GV.dataFromUrl jsonUrl [GV.JSON "values"]
  --
      encScatter = GV.encoding
        . GV.position GV.X [GV.PName "State", GV.PmType GV.Nominal]
        . GV.position GV.Y [GV.PName "Model - SEP", GV.PmType GV.Quantitative]
        . GV.color [GV.MName "Source", GV.MmType GV.Nominal]
        . GV.size [GV.MName "VAP", GV.MmType GV.Quantitative]
      markScatter = GV.mark GV.Circle [GV.MTooltip GV.TTEncoding]
      scatterSpec = GV.asSpec [encScatter [], markScatter]
      layers = GV.layer [scatterSpec]
  pure $ FV.configuredVegaLite vc [FV.title title
                                  , layers
                                  , vlData
                                  ]


categoryChart :: forall ks rs r . (K.KnitEffects r, F.ElemOf rs DT.PopCount, F.ElemOf rs TurnoutCI, ks F.⊆ rs)
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
          , ("Lo", GV.Number $ MT.ciLower $ r ^. turnoutCI)
          , ("Mid", GV.Number $ MT.ciMid $ r ^. turnoutCI)
          , ("Hi", GV.Number $ MT.ciUpper $ r ^. turnoutCI)
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

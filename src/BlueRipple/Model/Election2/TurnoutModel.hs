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

import qualified BlueRipple.Model.Election2.DataPrep as DP
import qualified BlueRipple.Model.Demographic.DataPrep as DDP
import qualified BlueRipple.Model.Election2.ModelCommon as MC
import qualified BlueRipple.Data.DataFrames as BRDF
import qualified BlueRipple.Configuration as BR
import qualified BlueRipple.Utilities.KnitUtils as BRKU
import qualified BlueRipple.Data.GeographicTypes as GT
import qualified BlueRipple.Data.ModelingTypes as MT

import qualified Knit.Report as K hiding (elements)


import qualified Control.Foldl as FL
import Control.Lens (view)
import qualified Data.IntMap.Strict as IM
import qualified Data.Map.Strict as M
import qualified Data.Set as S

import qualified Data.List as List
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vinyl as Vinyl

import qualified Frames as F
import qualified Frames.Streamly.TH as FTH
import qualified Frames.Serialize as FS

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

runTurnoutModel :: (K.KnitEffects r
                   , BRKU.CacheEffects r
                   , Vinyl.RMap l
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
                -> MC.TurnoutSurvey
                -> DM.DesignMatrixRow (F.Record DP.PredictorsR)
                -> MC.PSTargets
                -> MC.StateAlpha
                -> K.Sem r (K.ActionWithCacheTime r (MC.TurnoutPrediction, MC.PSMap l Double))
runTurnoutModel year modelDirE cacheDirE gqName cmdLine runConfig ts dmr pst sam = do
  let modelConfig = MC.TurnoutConfig ts pst dmr sam
  rawCES_C <- DP.cesCountedDemPresVotesByCD False
  cpCES_C <-  DP.cachedPreppedCES (Right "model/election2/test/CESTurnoutModelDataRaw.bin") rawCES_C
  rawCPS_C <- DP.cpsCountedTurnoutByState
  cpCPS_C <- DP.cachedPreppedCPS (Right "model/election2/test/CPSTurnoutModelDataRaw.bin") rawCPS_C
  modelData_C <- DP.cachedPreppedModelData
                 (Right "model/election2/test/CPSTurnoutModelData.bin") rawCPS_C
                 (Right "model/election2/test/CESTurnoutModelData.bin") rawCES_C
  acsByState_C <- fmap (DP.PSData @'[GT.StateAbbreviation] . fmap F.rcast) <$> DDP.cachedACSa5ByState
  MC.runModel modelDirE (MC.turnoutSurveyText ts <> "TurnoutR_" <> show year) gqName cmdLine runConfig modelConfig modelData_C acsByState_C

FTH.declareColumn "TurnoutP" ''Double
FTH.declareColumn "TurnoutP_CI" ''MT.ConfidenceInterval

{-
statePSWithTargetsChart :: K.KnitEffects r
                      => BR.PostPaths Path.Abs
                      -> BR.PostInfo
                      -> Text
                      -> Text
                      -> FV.ViewConfig
                      -> F.FrameRec F.FrameRec ([GT.StateAbbreviation, DT.PopCount, TM.TurnoutP, BRDF.BallotsCountedVAP])
                      -> K.Sem r GV.VegaLite
statePSWithTargetsChart postPaths' postInfo title chartID vc tableRows = do
  let colData r
        = [("State", GV.Str $ r ^. GT.stateAbbreviation)
          , ("Population", GV.Number $ realToFrac   r ^. DT.popCount)
          , ("Actual Turnout (VAP)", GV.Number $ 100 * r ^. BRDF.ballotsCountedVAP)
          , ("Modeled Turnout", GV.Number $ 100 * r ^. TM.turnoutP)
          , ("Actual - Model", GV.Number $ 100 * (r ^. BRDF.ballotsCountedVAP - r ^. TM.turnoutP) )
          ]
--      toData kltr = fmap ($ kltr) $ fmap colData [0..(n-1)]
      jsonRows = FL.fold (VJ.rowsToJSON colData [] Nothing) tableRows
  jsonFilePrefix <- K.getNextUnusedId $ ("statePSWithTargets_" <> chartID)
  jsonUrl <-  BRK.brAddJSON postPaths' postInfo jsonFilePrefix jsonRows

  let vlData = GV.dataFromUrl jsonUrl [GV.JSON "values"]
  --
      encScatter = GV.encoding
        . GV.position GV.X [GV.PName xLabel, GV.PmType GV.Quantitative]
        . GV.position GV.Y [GV.PName yLabel, GV.PmType GV.Quantitative]
      markScatter = GV.mark GV.Point [GV.MSize 1]
      scatterSpec = GV.asSpec [encScatter [], markScatter]
      encXYLine = GV.encoding
                  . GV.position GV.Y [GV.PName xLabel, GV.PmType GV.Quantitative]
                  . GV.position GV.X [GV.PName xLabel, GV.PmType GV.Quantitative]
      markXYLine = GV.mark GV.Line [GV.MColor "black", GV.MFillOpacity 0.5]
      xyLineSpec = GV.asSpec [encXYLine [], markXYLine]
      layers = GV.layer [scatterSpec, xyLineSpec]
  pure $ FV.configuredVegaLite vc [FV.title title
                                  , layers
                                  , transf []
                                  , vlData
                                  ]
-}

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

module BlueRipple.Model.Election2.TurnoutModel
  (
    module BlueRipple.Model.Election2.TurnoutModel
  )
where

import qualified BlueRipple.Model.Election2.DataPrep as DP
import qualified BlueRipple.Model.Election2.ModelCommon as MC
import qualified BlueRipple.Data.DataFrames as BRDF
import qualified BlueRipple.Configuration as BR
import qualified BlueRipple.Utilities.KnitUtils as BRKU
import qualified BlueRipple.Data.GeographicTypes as GT

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

cesTurnoutModelConfig :: DM.DesignMatrixRow (F.Record DP.PredictorsR) -> MC.PSTargets -> MC.StateAlpha -> MC.TurnoutConfig
cesTurnoutModelConfig dmr pst sam = MC.TurnoutConfig MC.CESSurvey pst dmr sam

runCESTurnoutModel :: (K.KnitEffects r
                      , BRKU.CacheEffects r
                      )
                   => Int
                   -> Either Text Text
                   -> Either Text Text
                   -> BR.CommandLine
                   -> MC.RunConfig
                   -> DM.DesignMatrixRow (F.Record DP.PredictorsR)
                   -> MC.PSTargets
                   -> MC.StateAlpha
                   -> K.Sem r (K.ActionWithCacheTime r MC.TurnoutPrediction)
runCESTurnoutModel year modelDirE cacheDirE cmdLine runConfig dmr pst sam = do
  let modelConfig = cesTurnoutModelConfig dmr pst sam
  rawCES_C <- DP.cesCountedDemPresVotesByCD False
  cpCES_C <-  DP.cachedPreppedCES (Right "model/election2/test/CESTurnoutModelData.bin") rawCES_C
  rawCPS_C <- DP.
  modelData_C <- fmap (DP.CESData . F.filterFrame ((== year) . view BRDF.year))
                 <$> DP.cachedPreppedCES (Right "model/election2/test/CESTurnoutModelData.bin") rawCES_C
  MC.runModel modelDirE cacheDirE ("CESTurnout_" <> show year) cmdLine runConfig DP.unCESData modelConfig modelData_C

{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UnicodeSyntax #-}
{-# LANGUAGE StrictData #-}

module Main
  (main)
where

import qualified BlueRipple.Configuration as BR
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.GeographicTypes as GT
import qualified BlueRipple.Data.ModelingTypes as MT
import qualified BlueRipple.Data.ACS_PUMS as ACS
import qualified BlueRipple.Data.DataFrames as BRDF
import qualified BlueRipple.Data.Keyed as Keyed
import qualified BlueRipple.Utilities.KnitUtils as BRK
import qualified BlueRipple.Model.Demographic.DataPrep as DDP
import qualified BlueRipple.Model.Election2.DataPrep as DP
import qualified BlueRipple.Model.Election2.ModelCommon as MC
--import qualified BlueRipple.Model.Election2.ModelCommon2 as MC2
import qualified BlueRipple.Model.Election2.ModelRunner as MR

import qualified Knit.Report as K
import qualified Knit.Effect.AtomicCache as KC
import qualified Text.Pandoc.Error as Pandoc
import qualified System.Console.CmdArgs as CmdArgs
import qualified Colonnade as C

--import GHC.TypeLits (Symbol)
import qualified Control.Foldl as FL
import Control.Lens (view, (^.))

import qualified Frames as F
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V
import qualified Data.Map.Merge.Strict as MM
import qualified Data.Set as Set
import qualified Frames.Melt as F
import qualified Frames.MapReduce as FMR
import qualified Frames.Folds as FF
import qualified Frames.Transform as FT
import qualified Frames.SimpleJoins as FJ
import qualified Frames.Streamly.InCore as FSI
import qualified Frames.Streamly.TH as FSTH
import qualified Frames.Serialize as FS

import Path (Dir, Rel)
import qualified Path

import qualified Stan.ModelBuilder.DesignMatrix as DM

import qualified Data.Map.Strict as M

import qualified Text.Printf as PF
import qualified Graphics.Vega.VegaLite as GV
import qualified Graphics.Vega.VegaLite.Compat as FV
import qualified Graphics.Vega.VegaLite.Configuration as FV
import qualified Graphics.Vega.VegaLite.JSON as VJ

--import Data.Monoid (Sum(getSum))

templateVars ∷ Map String String
templateVars =
  M.fromList
    [ ("lang", "English")
    , ("site-title", "Blue Ripple Politics")
    , ("home-url", "https://www.blueripplepolitics.org")
    --  , ("author"   , T.unpack yamlAuthor)
    ]

pandocTemplate ∷ K.TemplatePath
pandocTemplate = K.FullySpecifiedTemplatePath "pandoc-templates/blueripple_basic.html"


main :: IO ()
main = do
  cmdLine ← CmdArgs.cmdArgsRun BR.commandLine
  pandocWriterConfig ←
    K.mkPandocWriterConfig
    pandocTemplate
    templateVars
    (BRK.brWriterOptionsF . K.mindocOptionsF)
  let cacheDir = ".flat-kh-cache"
      knitConfig ∷ K.KnitConfig BRK.SerializerC BRK.CacheData Text =
        (K.defaultKnitConfig $ Just cacheDir)
          { K.outerLogPrefix = Just "2023-ElectionModel"
          , K.logIf = BR.knitLogSeverity $ BR.logLevel cmdLine -- K.logDiagnostic
          , K.pandocWriterConfig = pandocWriterConfig
          , K.serializeDict = BRK.flatSerializeDict
          , K.persistCache = KC.persistStrictByteString (\t → toString (cacheDir <> "/" <> t))
          }
  resE ← K.knitHtmls knitConfig $ do
    K.logLE K.Info $ "Command Line: " <> show cmdLine
    let postInfo = BR.PostInfo (BR.postStage cmdLine) (BR.PubTimes BR.Unpublished Nothing)
        dmr = MC.tDesignMatrixRow_d
        survey = MC.CESSurvey
        aggregations = [MC.WeightedAggregation MC.ContinuousBinomial MC.NoAchenHur
                       , MC.WeightedAggregation MC.ContinuousBinomial MC.UseAchenHur
                       ]
        alphaModels = [MC.St_A_S_E_R] --, MC.St_A_S_E_R_ER_StR, MC.St_A_S_E_R_ER_StR_StER]
        psTs = [MC.NoPSTargets] --, MC.PSTargets]
    rawCES_C <- DP.cesCountedDemPresVotesByCD False
    cpCES_C <-  DP.cachedPreppedCES (Right "model/election2/test/CESTurnoutModelDataRaw.bin") rawCES_C
    rawCPS_C <- DP.cpsCountedTurnoutByState
    cpCPS_C <- DP.cachedPreppedCPS (Right "model/election2/test/CPSTurnoutModelDataRaw.bin") rawCPS_C
    cps <- K.ignoreCacheTime cpCPS_C
    ces <- K.ignoreCacheTime cpCES_C
    let modelDirE = Right "model/election2/stan/"
        cacheDirE = Right "model/election2/"

    modelPostPaths <- postPaths "Models" cmdLine

    BRK.brNewPost modelPostPaths postInfo "Models" $ do
      acsA5ByState_C <- DDP.cachedACSa5ByState ACS.acs1Yr2012_21 2021
      acsByState_C <- BRK.retrieveOrMakeD "model/election2/acsByStatePS.bin" acsA5ByState_C $ \acsFull ->
        pure $ DP.PSData @'[GT.StateAbbreviation] . fmap F.rcast . F.filterFrame ((== DT.Citizen) . view DT.citizenC) $ acsFull
      let runTurnoutModel rc gqName agg am pt = MR.runTurnoutModel 2020 modelDirE cacheDirE gqName cmdLine rc survey agg (contramap F.rcast dmr) pt am acsByState_C
          runPrefModel rc gqName agg am pt = MR.runPrefModel 2020 modelDirE cacheDirE gqName cmdLine rc agg (contramap F.rcast dmr) pt am acsByState_C
          runDVSModel rc gqName agg am pt = MR.runFullModel 2020 modelDirE cacheDirE gqName cmdLine rc survey agg (contramap F.rcast dmr) pt am acsByState_C
          g f (a, b) = f b >>= pure . (a, )
          h f = traverse (g f)
      stateComparisonsT <- MR.allModelsCompBy @'[GT.StateAbbreviation] runTurnoutModel "State" aggregations alphaModels psTs >>= h MR.addBallotsCountedVEP
      stateComparisonsP <- MR.allModelsCompBy @'[GT.StateAbbreviation] runPrefModel "State" aggregations alphaModels psTs >>= h MR.addBallotsCountedVEP
      stateComparisonsDVS <- MR.allModelsCompBy @'[GT.StateAbbreviation] runDVSModel "State" aggregations alphaModels psTs >>= h MR.addBallotsCountedVEP

      turnoutStateChart <- MR.stateChart -- @[GT.StateAbbreviation, MR.ModelPr, BRDF.VAP, BRDF.BallotsCounted]
                           modelPostPaths postInfo "TComp" "Turnout Model Comparison by State" "Turnout" (FV.ViewConfig 500 500 10)
                           (view BRDF.vAP) (Just $ view BRDF.ballotsCountedVEP)
                           (fmap (second $ (fmap (MR.modelCIToModelPr))) stateComparisonsT)
      _ <- K.addHvega Nothing Nothing turnoutStateChart
      MR.allModelsCompChart @'[DT.Age5C] modelPostPaths postInfo runTurnoutModel "Age" "Turnout" (show . view DT.age5C) aggregations alphaModels psTs
      MR.allModelsCompChart @'[DT.SexC] modelPostPaths postInfo runTurnoutModel "Sex" "Turnout" (show . view DT.sexC) aggregations alphaModels psTs
      MR.allModelsCompChart @'[DT.Education4C] modelPostPaths postInfo runTurnoutModel "Education" "Turnout" (show . view DT.education4C) aggregations alphaModels psTs
      MR.allModelsCompChart @'[DT.Race5C] modelPostPaths postInfo runTurnoutModel "Race" "Turnout" (show . view DT.race5C) aggregations alphaModels psTs
      let srText r = show (r ^. DT.education4C) <> "-" <> show (r ^. DT.race5C)
      MR.allModelsCompChart @'[DT.Education4C, DT.Race5C] modelPostPaths postInfo runTurnoutModel "Education_Race" "Turnout" srText aggregations alphaModels psTs
      prefStateChart <- MR.stateChart modelPostPaths postInfo "PComp" "Pref Comparison by State" "Pref" (FV.ViewConfig 500 500 10) (view BRDF.vAP) Nothing
                        (fmap (second $ (fmap (MR.modelCIToModelPr))) stateComparisonsP)
      _ <- K.addHvega Nothing Nothing prefStateChart
      MR.allModelsCompChart @'[DT.Age5C] modelPostPaths postInfo runPrefModel "Age" "Pref" (show . view DT.age5C) aggregations alphaModels psTs
      MR.allModelsCompChart @'[DT.SexC] modelPostPaths postInfo runPrefModel "Sex" "Pref" (show . view DT.sexC) aggregations alphaModels psTs
      MR.allModelsCompChart @'[DT.Education4C] modelPostPaths postInfo runPrefModel "Education" "Pref" (show . view DT.education4C) aggregations alphaModels psTs
      MR.allModelsCompChart @'[DT.Race5C] modelPostPaths postInfo runPrefModel "Race" "Pref" (show . view DT.race5C) aggregations alphaModels psTs
      let srText r = show (r ^. DT.education4C) <> "-" <> show (r ^. DT.race5C)
      MR.allModelsCompChart @'[DT.Education4C, DT.Race5C] modelPostPaths postInfo runPrefModel "Education_Race" "Pref" srText aggregations alphaModels psTs
      dvsStateChart <- MR.stateChart modelPostPaths postInfo "DVSComp" "DVS Comparison by State" "Pref" (FV.ViewConfig 500 500 10) (view BRDF.vAP) Nothing
                        (fmap (second $ (fmap (MR.modelCIToModelPr))) stateComparisonsDVS)
      _ <- K.addHvega Nothing Nothing dvsStateChart
      MR.allModelsCompChart @'[DT.Age5C] modelPostPaths postInfo runDVSModel "Age" "DVS" (show . view DT.age5C) aggregations alphaModels psTs
      MR.allModelsCompChart @'[DT.SexC] modelPostPaths postInfo runDVSModel "Sex" "DVS" (show . view DT.sexC) aggregations alphaModels psTs
      MR.allModelsCompChart @'[DT.Education4C] modelPostPaths postInfo runDVSModel "Education" "DVS" (show . view DT.education4C) aggregations alphaModels psTs
      MR.allModelsCompChart @'[DT.Race5C] modelPostPaths postInfo runDVSModel "Race" "DVS" (show . view DT.race5C) aggregations alphaModels psTs
      let srText r = show (r ^. DT.education4C) <> "-" <> show (r ^. DT.race5C)
      MR.allModelsCompChart @'[DT.Education4C, DT.Race5C] modelPostPaths postInfo runDVSModel "Education_Race" "DVS" srText aggregations alphaModels psTs
      pure ()
    pure ()
  pure ()
  case resE of
    Right namedDocs →
      K.writeAllPandocResultsWithInfoAsHtml "" namedDocs
    Left err → putTextLn $ "Pandoc Error: " <> Pandoc.renderError err

postDir ∷ Path.Path Rel Dir
postDir = [Path.reldir|br-2023-electionModel/posts|]

postLocalDraft
  ∷ Path.Path Rel Dir
  → Maybe (Path.Path Rel Dir)
  → Path.Path Rel Dir
postLocalDraft p mRSD = case mRSD of
  Nothing → postDir BR.</> p BR.</> [Path.reldir|draft|]
  Just rsd → postDir BR.</> p BR.</> rsd

postInputs ∷ Path.Path Rel Dir → Path.Path Rel Dir
postInputs p = postDir BR.</> p BR.</> [Path.reldir|inputs|]

sharedInputs ∷ Path.Path Rel Dir
sharedInputs = postDir BR.</> [Path.reldir|Shared|] BR.</> [Path.reldir|inputs|]

postOnline ∷ Path.Path Rel t → Path.Path Rel t
postOnline p = [Path.reldir|research/Election|] BR.</> p

postPaths
  ∷ (K.KnitEffects r)
  ⇒ Text
  → BR.CommandLine
  → K.Sem r (BR.PostPaths BR.Abs)
postPaths t cmdLine = do
  let mRelSubDir = case cmdLine of
        BR.CLLocalDraft _ _ mS _ → maybe Nothing BR.parseRelDir $ fmap toString mS
        _ → Nothing
  postSpecificP ← K.knitEither $ first show $ Path.parseRelDir $ toString t
  BR.postPaths
    BR.defaultLocalRoot
    sharedInputs
    (postInputs postSpecificP)
    (postLocalDraft postSpecificP mRelSubDir)
    (postOnline postSpecificP)

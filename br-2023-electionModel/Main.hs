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
import qualified BlueRipple.Data.Loaders as BRDF
import qualified BlueRipple.Data.DataFrames as BRDF
import qualified BlueRipple.Data.Keyed as Keyed
import qualified BlueRipple.Utilities.KnitUtils as BRK
import qualified BlueRipple.Model.Demographic.DataPrep as DDP
import qualified BlueRipple.Model.Election2.DataPrep as DP
import qualified BlueRipple.Model.Election2.ModelCommon as MC
import qualified BlueRipple.Model.Election2.ModelCommon2 as MC2
import qualified BlueRipple.Model.Election2.TurnoutModel as TM

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

type PSByC ks = (Show (F.Record ks)
                 , Typeable ks
                 , V.RMap ks
                 , Ord (F.Record ks)
                 , ks F.⊆ DP.PSDataR '[GT.StateAbbreviation]
                 , ks F.⊆ DDP.ACSa5ByStateR
                 , ks F.⊆ (ks V.++ '[DT.PopCount])
                 , F.ElemOf (ks V.++ '[DT.PopCount]) DT.PopCount
                 , FSI.RecVec (ks V.++ '[DT.PopCount])
                 , FSI.RecVec (ks V.++ '[DT.PopCount, TM.TurnoutCI])
                 , FS.RecFlat ks)

psBy :: forall ks r a b.
        (PSByC ks
        , K.KnitEffects r
        , BRK.CacheEffects r
        )
     => BR.CommandLine
     -> Text
     -> MC.TurnoutSurvey a
     -> MC.SurveyAggregation b
     -> DM.DesignMatrixRow (F.Record DP.PredictorsR)
     -> MC.PSTargets
     -> MC2.Alphas
     -> K.Sem r (F.FrameRec (ks V.++ [DT.PopCount, TM.TurnoutCI]))
psBy cmdLine gqName ts sAgg dmr pst am = do
    let runConfig = MC.RunConfig True True True (Just $ MC.psGroupTag @ks)
    (MC.PSMap psTMap) <- K.ignoreCacheTimeM
                              $ TM.runTurnoutModel2 2020
                              (Right "model/election2/test/stan") (Right "model/election2/test")
                              gqName cmdLine runConfig ts sAgg (contramap F.rcast dmr) pst am
    pcMap <- DDP.cachedACSa5ByState >>= popCountByMap @ks
    let whenMatched :: F.Record ks -> MT.ConfidenceInterval -> Int -> Either Text (F.Record  (ks V.++ [DT.PopCount, TM.TurnoutCI]))
        whenMatched k t p = pure $ k F.<+> (p F.&: t F.&: V.RNil :: F.Record [DT.PopCount, TM.TurnoutCI])
        whenMissingPC k _ = Left $ "psBy: " <> show k <> " is missing from PopCount map."
        whenMissingT k _ = Left $ "psBy: " <> show k <> " is missing from ps Turnout map."
    mergedMap <- K.knitEither
                 $ MM.mergeA (MM.traverseMissing whenMissingPC) (MM.traverseMissing whenMissingT) (MM.zipWithAMatched whenMatched) psTMap pcMap
    pure $ F.toFrame $ M.elems mergedMap

psByState ::  (K.KnitEffects r, BRK.CacheEffects r)
          => BR.CommandLine
          -> MC.TurnoutSurvey a
          -> MC.SurveyAggregation b
          -> DM.DesignMatrixRow (F.Record DP.PredictorsR)
          -> MC.PSTargets
          -> MC2.Alphas
          -> K.Sem r (F.FrameRec ([GT.StateAbbreviation, DT.PopCount, TM.TurnoutCI, BRDF.BallotsCountedVAP, BRDF.VAP]))
psByState cmdLine ts sAgg dmr pst am = do
  byStatePS <- psBy @'[GT.StateAbbreviation] cmdLine "State" ts sAgg dmr pst am
  TM.addBallotsCountedVAP byStatePS


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
--        runConfig = MC.RunConfig False False True (Just $ MC.psGroupTag @'[GT.StateAbbreviation])
--        dmr = MC.tDesignMatrixRow_d_A_S_E_R
        dmr2 = MC2.tDesignMatrixRow_d
--        stateAlphaModel = MC.StateAlphaHierCentered
        survey = MC.CESSurvey
        aggregation = MC.WeightedAggregation
        alphaModels = [MC2.St_A_S_E_R, MC2.St_A_S_E_R_ER, MC2.St_A_S_E_R_StR, MC2.St_A_S_E_R_ER_StR, MC2.St_A_S_E_R_ER_StR_StER]
        psTs = [MC.NoPSTargets, MC.PSTargets]
    rawCES_C <- DP.cesCountedDemPresVotesByCD False
    cpCES_C <-  DP.cachedPreppedCES (Right "model/election2/test/CESTurnoutModelDataRaw.bin") rawCES_C
    rawCPS_C <- DP.cpsCountedTurnoutByState
    cpCPS_C <- DP.cachedPreppedCPS (Right "model/election2/test/CPSTurnoutModelDataRaw.bin") rawCPS_C
    cps <- K.ignoreCacheTime cpCPS_C
    ces <- K.ignoreCacheTime cpCES_C
    let stateComp am pt = do
          comp <- psByState cmdLine survey aggregation dmr2 pt am
          pure (MC2.alphasText am <> (if (pt == MC.PSTargets) then "_PSTgt" else ""), comp)
    stateComparisons <- traverse (uncurry stateComp) [(am, pt) | am <- alphaModels, pt <- psTs]
    byStateFromRawCPS <- TM.addBallotsCountedVAP (TM.surveyDataBy @'[GT.StateAbbreviation] (Just aggregation) cps)
    byStateFromRawCES <- TM.addBallotsCountedVAP (TM.surveyDataBy @'[GT.StateAbbreviation] (Just aggregation) ces)

    let modelCompBy :: forall ks r . (K.KnitEffects r, BRK.CacheEffects r, PSByC ks)
                    => Text -> MC2.Alphas -> MC.PSTargets -> K.Sem r (Text, F.FrameRec (ks V.++ '[DT.PopCount, TM.TurnoutCI]))
        modelCompBy catLabel am pt = do
          comp <- psBy @ks cmdLine catLabel survey aggregation dmr2 pt am
          pure (MC2.alphasText am <> (if (pt == MC.PSTargets) then "_PSTgt" else ""), comp)

        allModelsCompBy :: forall ks r . (K.KnitEffects r, BRK.CacheEffects r, PSByC ks)
                        => Text -> [MC2.Alphas] -> [MC.PSTargets] -> K.Sem r [(Text, F.FrameRec (ks V.++ '[DT.PopCount, TM.TurnoutCI]))]
        allModelsCompBy catLabel alphaModels' psTs' = traverse (\(am, pt) -> modelCompBy @ks catLabel am pt) [(am, pt) | am <- alphaModels', pt <- psTs']

        allModelsCompChart :: forall ks r . (K.KnitOne r, BRK.CacheEffects r, PSByC ks, Keyed.FiniteSet (F.Record ks)
                                            , F.ElemOf (ks V.++ [DT.PopCount, TM.TurnoutCI]) DT.PopCount
                                            , F.ElemOf (ks V.++ [DT.PopCount, TM.TurnoutCI]) TM.TurnoutCI
                                            , ks F.⊆ (ks V.++ [DT.PopCount, TM.TurnoutCI])
                                            )
                           =>  BR.PostPaths Path.Abs -> Text -> (F.Record ks -> Text) -> [MC2.Alphas] -> [MC.PSTargets] -> K.Sem r ()
        allModelsCompChart pp catLabel catText alphaModels' psTs' = do
          allModels <- allModelsCompBy @ks catLabel alphaModels' psTs'
          let cats = Set.toList $ Keyed.elements @(F.Record ks)
              numCats = length cats
              numSources = length allModels
          catCompChart <- TM.categoryChart @ks pp postInfo "Model Comparison By Category" "CatComp"
                          (FV.ViewConfig 300 (15 * realToFrac numSources) 10) (Just cats) (Just $ fmap fst allModels)
                          catText allModels
          _ <- K.addHvega Nothing Nothing catCompChart
          pure ()
--    BRK.logFrame modelComparison
    let fromRawCPS = TM.surveyDataBy @'[DT.Race5C] (Just aggregation) cps
        fromRawCES = TM.surveyDataBy @'[DT.Race5C] (Just aggregation) ces
    BRK.logFrame fromRawCES
    turnoutModelPostPaths <- postPaths "Turnout Models" cmdLine

    BRK.brNewPost turnoutModelPostPaths postInfo "Turnout Models" $ do
      modelCompChart <- TM.stateChart turnoutModelPostPaths postInfo "Model Comparison by State" "ModelComp" (FV.ViewConfig 500 500 10)
                        ([{-("WeightedCPS", fmap F.rcast byStateFromRawCPS)
                        , ("WeightedCES", fmap F.rcast byStateFromRawCES) -}
                        ] <> fmap (second $ (fmap (F.rcast . TM.turnoutCIToTurnoutP))) stateComparisons)
      _ <- K.addHvega Nothing Nothing modelCompChart
      let modelCompByRace am pt = do
            comp <- psBy @'[DT.Race5C] cmdLine "Race" survey aggregation dmr2 pt am
            pure (MC2.alphasText am <> (if (pt == MC.PSTargets) then "_PSTgt" else ""), comp)

      allModelsCompChart @'[DT.Age5C] turnoutModelPostPaths "Age" (show . view DT.age5C) alphaModels psTs
      allModelsCompChart @'[DT.SexC] turnoutModelPostPaths "Sex" (show . view DT.sexC) alphaModels psTs
      allModelsCompChart @'[DT.Education4C] turnoutModelPostPaths "Education" (show . view DT.education4C) alphaModels psTs
      allModelsCompChart @'[DT.Race5C] turnoutModelPostPaths "Race" (show . view DT.race5C) alphaModels psTs
      let srText r = show (r ^. DT.education4C) <> "-" <> show (r ^. DT.race5C)
      allModelsCompChart @'[DT.Education4C, DT.Race5C] turnoutModelPostPaths "Education_Race" srText alphaModels psTs
{-
    modelComparisonsByRace <- traverse (uncurry modelCompByRace) [(am, pt) | am <- alphaModels, pt <- psTs]
      let cats = Set.toList $ Keyed.elements @(F.Record '[DT.Race5C])
          numCats = length cats
          numSources = length modelComparisonsByRace
      catCompChart <- TM.categoryChart @'[DT.Race5C] turnoutModelPostPaths postInfo "Model Comparison By Category" "CatComp"
                      (FV.ViewConfig 300 (20 * realToFrac numSources) 10) (Just cats) (Just $ fmap fst modelComparisonsByRace)
                      (show . view DT.race5C)
                      $ modelComparisonsByRace
      _ <- K.addHvega Nothing Nothing catCompChart
-}
      pure ()
    pure ()
  pure ()
  case resE of
    Right namedDocs →
      K.writeAllPandocResultsWithInfoAsHtml "" namedDocs
    Left err → putTextLn $ "Pandoc Error: " <> Pandoc.renderError err

popCountBy :: forall ks rs r .
              (K.KnitEffects r, BRK.CacheEffects r
              , ks F.⊆ rs, F.ElemOf rs DT.PopCount, Ord (F.Record ks)
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
                 (K.KnitEffects r, BRK.CacheEffects r
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

acsNByState :: (K.KnitEffects r, BRK.CacheEffects r) => K.Sem r (F.FrameRec [GT.StateAbbreviation, DT.PopCount])
acsNByState = DDP.cachedACSa5ByState >>= popCountBy @'[GT.StateAbbreviation]


postDir ∷ Path.Path Rel Dir
postDir = [Path.reldir|br-2023-Demographics/posts|]

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

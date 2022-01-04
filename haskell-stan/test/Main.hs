{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}
module Main where

import Models

import qualified KnitEnvironment as KE


import qualified Stan.ModelBuilder as S
import qualified Stan.ModelConfig as SC
import qualified Stan.ModelRunner as SMR
import qualified Stan.RScriptBuilder as SR
import qualified CmdStan as CS
import qualified Knit.Report as K
import qualified Knit.Effect.AtomicCache as K (cacheTime)

import qualified Data.Text as T
--import qualified Flat
import qualified Control.Foldl as FL
import Control.Lens (view)
--import Stan.ModelConfig (modelWrangler)

main :: IO ()
main = KE.knitToIO KE.defaultConfig $ do
  fbResults_C <- fbResults
  fbMatchups1_C <- fbMatchups 1
  fbMatchups2_C <- fbMatchups 2
--  let modelData = fbResults_C <*> fbMatchups_C
  teams <- FL.fold (FL.premap (view favoriteName) FL.set) <$> K.ignoreCacheTime fbResults_C
  (dw1, code1) <- dataWranglerAndCode fbResults_C fbMatchups1_C (groupBuilder teams) spreadDiffNormal
  (musCI, sigmaMuCI, sigmaCI, eScoreDiff) <- do
    K.ignoreCacheTimeM
    $ runModel @KE.SerializerC @KE.CacheData
    True
    (SC.RunnerInputNames "haskell-stan/test/stan" "normalSpreadDiff" (Just "mu1") "fb")
    dw1
    code1
    ""
    normalParamCIs
    fbResults_C
    fbMatchups1_C
  K.logLE K.Info $ "mus: " <> show musCI
  K.logLE K.Info $ "sigma_mu_fav: " <> show sigmaMuCI
  K.logLE K.Info $ "sigma: " <> show sigmaCI
  K.logLE K.Info $ "eScoreDiff: " <> show eScoreDiff

  let rin = SC.RunnerInputNames "haskell-stan/test/stan" "normalSpreadDiff" (Just $ "mu2") "fb"
      outputLabel = SC.rinModel rin  <> "_" <> SC.rinData rin <> maybe "" ("_" <>) (SC.rinGQ rin)
  K.clearIfPresent @Text @KE.CacheData $ "stan/test/result/" <> outputLabel <> ".bin"
  (dw2, code2) <- dataWranglerAndCode fbResults_C fbMatchups2_C (groupBuilder teams) spreadDiffNormal
  (musCI2, sigmaMuCI2, sigmaCI2, eScoreDiff2) <-
    K.ignoreCacheTimeM
    $ runModel @KE.SerializerC @KE.CacheData
    False
    rin
    dw2
    code2
    ""
    normalParamCIs
    fbResults_C
    fbMatchups2_C
  K.logLE K.Info $ "mus (matchups=2): " <> show musCI2
  K.logLE K.Info $ "sigma_mu_fav (matchups=2): " <> show sigmaMuCI2
  K.logLE K.Info $ "sigma (matchups=2): " <> show sigmaCI2
  K.logLE K.Info $ "eScoreDiff2 (matchups=2): " <> show eScoreDiff2


-- This whole thing should be wrapped in the core for this very common variation.
dataWranglerAndCode :: forall md gq r. (K.KnitEffects r, Typeable md, Typeable gq)
                    => K.ActionWithCacheTime r md --F.Frame FB_Result
                    -> K.ActionWithCacheTime r gq --F.Frame FB_Matchup
                    -> S.StanGroupBuilderM md gq ()
                    -> S.StanBuilderM md gq ()
                    -> K.Sem r (SC.DataWrangler md gq S.DataSetGroupIntMaps (), S.StanCode)
dataWranglerAndCode modelData_C gqData_C gb sb = do
  modelDat <- K.ignoreCacheTime modelData_C
  gqDat <- K.ignoreCacheTime gqData_C
  let builderWithWrangler = do
        S.buildGroupIndexes
        sb
        modelJsonF <- S.buildModelJSONFromDataM
        gqJsonF <- S.buildGQJSONFromDataM
        modelIntMapsBuilder <- S.modelIntMapsBuilder
        gqIntMapsBuilder <- S.gqIntMapsBuilder
        let modelWrangle md = (modelIntMapsBuilder md, modelJsonF)
            gqWrangle gq = (gqIntMapsBuilder gq, gqJsonF)
            wrangler :: SC.DataWrangler md gq S.DataSetGroupIntMaps () =
              SC.Wrangle
              SC.TransientIndex
              modelWrangle
              (Just gqWrangle)
        return wrangler
      resE = S.runStanBuilder modelDat gqDat gb builderWithWrangler
  K.knitEither $ fmap (\(bs, dw) -> (dw, S.code bs)) resE

runModel :: forall st cd md gq b c r.
            (SC.KnitStan st cd r
            , Typeable md
            , Typeable gq
            , st c
            )
         => Bool
         -> SC.RunnerInputNames
         -> SC.DataWrangler md gq b ()
         -> S.StanCode
         -> Text
         -> SC.ResultAction r md gq b () c
         -> K.ActionWithCacheTime r md
         -> K.ActionWithCacheTime r gq
         -> K.Sem r (K.ActionWithCacheTime r c)
runModel clearCaches rin dataWrangler stanCode ppName resultAction modelData_C gqData_C =
  K.wrapPrefix "haskell-stan-test.runModel" $ do
  K.logLE K.Info
    $ "Running: model="
    <> SC.rinModel rin <> " using model data=" <> SC.rinData rin
    <> maybe "" (" and GQ data=" <>) (SC.rinGQ rin)
  let --workDir = SC.rinModelDir rin -- fromMaybe "stan" mWorkDir
      outputLabel = SC.rinModel rin  <> "_" <> SC.rinData rin <> maybe "" ("_" <>) (SC.rinGQ rin)
--      nSamples = 1000
      stancConfig =
        (CS.makeDefaultStancConfig (toString $ SC.rinModelDir rin <> "/" <> SC.rinModel rin)) {CS.useOpenCL = False}
  stanConfig <-
    SC.setSigFigs 4
    . SC.noLogOfSummary
    <$> SMR.makeDefaultModelRunnerConfig @st @cd
    rin
    (Just (S.All, S.stanCodeToStanModel stanCode))
    (SC.StanMCParameters 4 4 Nothing Nothing Nothing Nothing)
    (Just stancConfig)
  let resultCacheKey = "stan/test/result/" <> outputLabel <> ".bin"
  when clearCaches $ do
    SMR.deleteStaleFiles @st @cd stanConfig [SMR.StaleData]
    K.clearIfPresent @Text @cd resultCacheKey
  modelDep <- SC.modelDependency $ SC.mrcInputNames stanConfig
  K.logLE (K.Debug 1) $ "modelDep: " <> show (K.cacheTime modelDep)
  K.logLE (K.Debug 1) $ "modelDataDep: " <> show (K.cacheTime modelData_C)
  K.logLE (K.Debug 1) $ "gqDataDep: " <> show (K.cacheTime gqData_C)
  let dataModelDep = (,,) <$> modelDep <*> modelData_C <*> gqData_C
--      getResults s () inputAndIndex_C = return ()
      unwraps = if T.null ppName then [] else [SR.UnwrapNamed ppName ppName]
  K.retrieveOrMake @st @cd resultCacheKey dataModelDep $ \_ -> do
    K.logLE K.Diagnostic "Data or model newer then last cached result. (Re)-running..."
    SMR.runModel @st @cd
      stanConfig
      (SMR.Both unwraps)
      dataWrangler
      SC.UnCacheable
      resultAction
      ()
      modelData_C
      gqData_C

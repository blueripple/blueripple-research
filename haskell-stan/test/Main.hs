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

import qualified Knit.Report as K
import qualified Knit.Effect.AtomicCache as K (cacheTime)

import qualified Flat
import qualified CmdStan as CS


main :: IO ()
main = KE.knitToIO KE.defaultConfig $ do
  fbResults_C <- fbResults
  fbMatchups_C <- fbMatchups 1
  let data_C = (,) <$> fbResults_C <*> fbMatchups_C
  (dw, code) <- dataWranglerAndCode data_C groupBuilder spreadDiffNormal
  (muCI, sigmaCI) <- K.ignoreCacheTimeM $ runModel False (Just "haskell-stan/test/stan") "normalSpreadDiff" "fb" dw code "" normalParamCIs data_C
  K.logLE K.Info $ "mu: " <> show muCI
  K.logLE K.Info $ "sigma: " <> show sigmaCI



-- This whole thing should be wrapped in the core for this very common variation.
dataWranglerAndCode :: (K.KnitEffects r, Typeable a)
                    => K.ActionWithCacheTime r a --(F.Frame FB_Result, F.Frame FB_Matchup)
                    -> S.StanGroupBuilderM a ()
                    -> S.StanBuilderM () a ()
                    -> K.Sem r (SC.DataWrangler a S.DataSetGroupIntMaps (), S.StanCode)
dataWranglerAndCode data_C gb sb = do
  dat <- K.ignoreCacheTime data_C
  let builderWithWrangler = do
        S.buildGroupIndexes
        sb
        jsonF <- S.buildJSONFromDataM
        intMapsBuilder <- S.intMapsBuilder
        return
          $ SC.Wrangle SC.TransientIndex
          $ \d -> (intMapsBuilder d, jsonF)
      resE = S.runStanBuilder dat () gb builderWithWrangler
  K.knitEither $ fmap (\(bs, dw) -> (dw, S.code bs)) resE

runModel :: (K.KnitEffects r, KE.CacheEffects r, Typeable a, Flat.Flat c)
         => Bool
         -> Maybe Text
         -> Text
         -> Text
         -> SC.DataWrangler a b ()
         -> S.StanCode
         -> Text
         -> SC.ResultAction r a b () c
         -> K.ActionWithCacheTime r a
         -> K.Sem r (K.ActionWithCacheTime r c)
runModel clearCaches mWorkDir modelName dataName dataWrangler stanCode ppName resultAction data_C =
  K.wrapPrefix "haskell-stan-test.runModel" $ do
  K.logLE K.Info $ "Running: model=" <> modelName <> " using data=" <> dataName
  let workDir = fromMaybe "stan" mWorkDir
      outputLabel = modelName <> "_" <> dataName
      nSamples = 1000
      stancConfig = (CS.makeDefaultStancConfig (toString $ workDir <> "/" <> modelName)) {CS.useOpenCL = False}
  stanConfig <-
    SC.setSigFigs 4
    . SC.noLogOfSummary
    <$> SMR.makeDefaultModelRunnerConfig
    workDir
    (modelName <> "_model")
    (Just (S.All, S.stanCodeToStanModel stanCode))
    (Just $ dataName <> ".json")
    (Just outputLabel)
    4 -- chains
    (Just (-1)) -- use all available cores
    Nothing
    Nothing
    Nothing
    Nothing
    (Just stancConfig)
  let resultCacheKey = "stan/test/result/" <> outputLabel <> ".bin"
  when clearCaches $ do
    K.liftKnit $ SMR.deleteStaleFiles stanConfig [SMR.StaleData]
    K.clearIfPresent @Text @KE.CacheData resultCacheKey
  modelDep <- SMR.modelCacheTime stanConfig
  K.logLE (K.Debug 1) $ "modelDep: " <> show (K.cacheTime modelDep)
  K.logLE (K.Debug 1) $ "houseDataDep: " <> show (K.cacheTime data_C)
  let dataModelDep = const <$> modelDep <*> data_C
      getResults s () inputAndIndex_C = return ()
      unwraps = [SR.UnwrapNamed ppName ppName]
  K.retrieveOrMake @KE.SerializerC @KE.CacheData resultCacheKey dataModelDep $ \() -> do
    K.logLE K.Diagnostic "Data or model newer then last cached result. (Re)-running..."
    SMR.runModel @KE.SerializerC @KE.CacheData
      stanConfig
      (SMR.Both unwraps)
      dataWrangler
      SC.UnCacheable
      resultAction
      ()
      data_C

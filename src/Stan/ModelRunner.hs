{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}
module Stan.ModelRunner where

import qualified CmdStan as CS
import qualified CmdStan.Types as CS

import qualified Knit.Report as K
import qualified BlueRipple.Utilities.KnitUtils as BR

import qualified Data.Aeson.Encoding as A
import qualified Data.ByteString.Lazy as BL
import qualified Polysemy as P
import qualified Data.Text as T
import qualified System.Environment as Env

data ModelRunnerConfig = ModelRunnerConfig
  { mrcStanMakeConfig :: CS.MakeConfig
  , mrcStanExeConfigF :: Int -> CS.StanExeConfig
  , mrcStanSummaryConfig :: CS.StansummaryConfig
  , mrcModelDir :: T.Text
  , mrcModel :: T.Text
  , mrcNumChains :: Int
  }

makeDefaultModelRunnerConfig :: P.Member (P.Embed IO) r
                             => T.Text
                             -> T.Text
                             -> Int
                             -> Maybe Int
                             -> Maybe Int
                             -> K.Sem r ModelRunnerConfig
makeDefaultModelRunnerConfig modelDirT modelNameT numChains numWarmupM numSamplesM = do
  let addDir d fp = d ++ "/" ++ fp
      modelDirS = T.unpack modelDirT
      modelNameS = T.unpack modelNameT
      datFile = modelNameS ++ "_dat.json"      
      outputFile n =  modelNameS ++ "_" ++ show n ++ ".csv"
  stanMakeConfig <- K.liftKnit $ CS.makeDefaultMakeConfig (addDir modelDirS modelNameS)
  let stanExeConfigF chainIndex = (CS.makeDefaultSample modelNameS chainIndex) { CS.inputData = Just (addDir modelDirS datFile)
                                                                               , CS.output = Just (addDir modelDirS $ outputFile chainIndex) 
                                                                               , CS.numSamples = numSamplesM
                                                                               , CS.numWarmup = numWarmupM
                                                                               }
  let stanOutputFiles = fmap (\n -> outputFile n) [1..numChains]
  stanSummaryConfig <- K.liftKnit $ CS.useCmdStanDirForStansummary (CS.makeDefaultSummaryConfig $ fmap (addDir modelDirS) stanOutputFiles)
  return $ ModelRunnerConfig stanMakeConfig stanExeConfigF stanSummaryConfig modelDirT modelNameT numChains
  

-- produce JSON from the data, store in FilePath
type JSONAction r a = a -> K.Sem r A.Encoding

-- produce a result of type b from the data and the model summary
type ResultAction r a b = CS.StanSummary -> K.ActionWithCacheTime r a -> K.Sem r b

runModel :: (K.KnitEffects r,  K.CacheEffectsD r)
         => ModelRunnerConfig
         -> JSONAction r a
         -> ResultAction r a b
         -> K.ActionWithCacheTime r a
         -> K.Sem r b
runModel config makeJSON makeResult cachedA = do
  let addDir d fp = d ++ "/" ++ fp
      modelNameS = T.unpack $ mrcModel config
      modelDirS = T.unpack $ mrcModelDir config
      modelFile = modelNameS ++ ".stan"
      datFile = modelNameS ++ "_dat.json"      
      outputFile n = modelNameS ++ "_" ++ show n ++ ".csv"
      outputFiles = fmap (\n -> outputFile n) [1..(mrcNumChains config)]
  clangBinDirM <- K.liftKnit $ Env.lookupEnv "CLANG_BINDIR"
  case clangBinDirM of
    Nothing -> K.logLE K.Info "CLANG_BINDIR not set. Using existing path for clang."
    Just clangBinDir -> do
      curPath <- K.liftKnit $ Env.getEnv "PATH"
      K.logLE K.Info $ "Current path: " <> (T.pack $ show curPath) <> ".  Adding " <> (T.pack $ show clangBinDir) <> " for llvm clang."
  let modelDir = mrcModelDir config
  json_C <- do
    let jsonFP = addDir modelDirS datFile
    curJSON_C <- BR.fileDependency jsonFP
    BR.updateIf curJSON_C cachedA $ \a -> makeJSON a >>= K.liftKnit . BL.writeFile datFile . A.encodingToLazyByteString

  stanOutput_C <-  do
    curStanOutputs_C <- fmap BR.oldestUnit $ traverse (BR.fileDependency . addDir modelDirS) outputFiles
    curModel_C <- BR.fileDependency (addDir modelDirS modelFile)
    let runStanDeps = (,) <$> json_C <*> curModel_C
        runOneChain chainIndex = do 
          let exeConfig = (mrcStanExeConfigF config) chainIndex          
          K.logLE K.Info $ "Running " <> T.pack modelNameS <> " for chain " <> (T.pack $ show chainIndex)
          K.logLE K.Diagnostic $ "Command: " <> T.pack (CS.toStanExeCmdLine exeConfig)
          K.liftKnit $ CS.stan (addDir modelDirS modelNameS) exeConfig
          K.logLE K.Info $ "Finished chain " <> (T.pack $ show chainIndex)
    res <- K.ignoreCacheTimeM $ BR.updateIf (fmap Just curStanOutputs_C) runStanDeps $ \_ ->  do
      K.logLE K.Info "Stan outputs older than input data or model.  Rebuilding Stan exe and running."
      K.liftKnit $ CS.make (mrcStanMakeConfig config)
      maybe Nothing (const $ Just ()) . sequence <$> (K.sequenceConcurrently $ fmap runOneChain [1..(mrcNumChains config)])
    K.knitMaybe "THere was an error running an MCMC chain." res
  K.logLE K.Diagnostic $ "Summary command: "
    <> (T.pack $ (CS.cmdStanDir . mrcStanMakeConfig $ config) ++ "/bin/stansummary")
    <> " "
    <> T.intercalate " " (fmap T.pack (CS.stansummaryConfigToCmdLine (mrcStanSummaryConfig config)))
  summary <- K.liftKnit $ CS.stansummary (mrcStanSummaryConfig config)
  makeResult summary cachedA 
                         
                           

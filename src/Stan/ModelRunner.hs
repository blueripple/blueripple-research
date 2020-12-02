{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}

module Stan.ModelRunner
  ( module Stan.ModelRunner,
    module CmdStan,
    --  , module CmdStan.Types
  )
where

import CmdStan
  ( StanExeConfig (..),
    StanSummary,
    StancConfig (..),
    makeDefaultStancConfig,
  )
import qualified CmdStan as CS
import qualified CmdStan.Types as CS
import Control.Monad (when)
import qualified Data.Aeson.Encoding as A
import qualified Data.ByteString.Lazy as BL
import qualified Data.Maybe as Maybe
import qualified Data.Text as T
import qualified Data.Text.IO as T
import qualified Knit.Effect.AtomicCache as K (cacheTime)
import qualified Knit.Report as K
import qualified Polysemy as P
import qualified Stan.ModelBuilder as SB
import qualified Stan.ModelConfig as SC
import qualified Stan.RScriptBuilder as SR
import qualified System.Directory as Dir
import qualified System.Environment as Env

makeDefaultModelRunnerConfig ::
  K.KnitEffects r =>
  T.Text ->
  T.Text ->
  -- | Assume model file exists when Nothing.  Otherwise generate from this and use.
  Maybe (SB.GeneratedQuantities, SB.StanModel) ->
  Maybe T.Text ->
  Maybe T.Text ->
  Int ->
  Maybe Int ->
  Maybe Int ->
  Maybe CS.StancConfig ->
  K.Sem r SC.ModelRunnerConfig
makeDefaultModelRunnerConfig modelDirT modelNameT modelM datFileM outputFilePrefixM numChains numWarmupM numSamplesM stancConfigM = do
  let modelDirS = T.unpack modelDirT
      outputFilePrefix = maybe modelNameT id outputFilePrefixM
  case modelM of
    Nothing -> return ()
    Just (gq, m) -> do
      modelState <- K.liftKnit $ SB.renameAndWriteIfNotSame gq m modelDirT modelNameT
      case modelState of
        SB.New -> K.logLE K.Info $ "Given model was new."
        SB.Same -> K.logLE K.Info $ "Given model was the same as existing model file."
        SB.Updated newName -> K.logLE K.Info $ "Given model was different from exisiting.  Old one was moved to \"" <> newName <> "\"."
  let datFileS = maybe (SC.defaultDatFile modelNameT) T.unpack datFileM
  stanMakeConfig' <- K.liftKnit $ CS.makeDefaultMakeConfig (T.unpack $ SC.addDirT modelDirT modelNameT)
  let stanMakeConfig = stanMakeConfig' {CS.stancFlags = stancConfigM}
      stanExeConfigF chainIndex =
        (CS.makeDefaultSample (T.unpack modelNameT) chainIndex)
          { CS.inputData = Just (SC.addDirFP (modelDirS ++ "/data") $ datFileS),
            CS.output = Just (SC.addDirFP (modelDirS ++ "/output") $ SC.outputFile outputFilePrefix chainIndex),
            CS.numSamples = numSamplesM,
            CS.numWarmup = numWarmupM
          }
  let stanOutputFiles = fmap (\n -> SC.outputFile outputFilePrefix n) [1 .. numChains]
  stanSummaryConfig <-
    K.liftKnit $
      CS.useCmdStanDirForStansummary (CS.makeDefaultSummaryConfig $ fmap (SC.addDirFP (modelDirS ++ "/output")) stanOutputFiles)
  return $
    SC.ModelRunnerConfig
      stanMakeConfig
      stanExeConfigF
      stanSummaryConfig
      modelDirT
      modelNameT
      (T.pack datFileS)
      outputFilePrefix
      numChains
      True

modelCacheTime :: (K.KnitEffects r, K.CacheEffectsD r) => SC.ModelRunnerConfig -> K.Sem r (K.ActionWithCacheTime r ())
modelCacheTime config = K.fileDependency (T.unpack $ SC.addDirT (SC.mrcModelDir config) $ SB.modelFile $ SC.mrcModel config)

data RScripts = None | ShinyStan [SR.UnwrapJSON] | Loo | Both [SR.UnwrapJSON] deriving (Show, Eq, Ord)

writeRScripts :: RScripts -> SC.ModelRunnerConfig -> IO ()
writeRScripts rScripts config = do
  dirBase <- T.pack <$> Dir.getCurrentDirectory
  let modelDir = SC.mrcModelDir config
      scriptPrefix = SC.mrcOutputPrefix config
      writeShiny ujs = SR.shinyStanScript config dirBase ujs >>= T.writeFile (T.unpack $ modelDir <> "/R/" <> scriptPrefix <> "_shinystan.R")
      writeLoo = SR.looScript config dirBase scriptPrefix 10 >>= T.writeFile (T.unpack $ modelDir <> "/R/" <> scriptPrefix <> ".R")
  case rScripts of
    None -> return ()
    ShinyStan ujs -> writeShiny ujs
    Loo -> writeLoo
    Both ujs -> writeShiny ujs >> writeLoo

wrangleDataWithoutPredictions ::
  (K.KnitEffects r, K.CacheEffectsD r) =>
  SC.ModelRunnerConfig ->
  SC.DataWrangler a b () ->
  K.ActionWithCacheTime r a ->
  K.Sem r (K.ActionWithCacheTime r b)
wrangleDataWithoutPredictions config dw a = wrangleData config dw a ()

wrangleData ::
  (K.KnitEffects r, K.CacheEffectsD r) =>
  SC.ModelRunnerConfig ->
  SC.DataWrangler a b p ->
  K.ActionWithCacheTime r a ->
  p ->
  K.Sem r (K.ActionWithCacheTime r b)
wrangleData config (SC.Wrangle indexType indexAndEncoder) cachedA _ = do
  let indexAndEncoder_C = fmap indexAndEncoder cachedA
      b_C = fmap fst indexAndEncoder_C
      encoder_C = fmap snd indexAndEncoder_C
  index_C <- manageIndex config indexType b_C
  curJSON_C <- K.fileDependency $ jsonFP config
  let newJSON_C = encoder_C <*> cachedA
  updatedJSON_C <- K.updateIf curJSON_C newJSON_C $ \e -> do
    jsonEncoding <- K.knitEither e
    K.liftKnit . BL.writeFile (jsonFP config) $ A.encodingToLazyByteString $ A.pairs jsonEncoding
  return $ const <$> index_C <*> updatedJSON_C -- if json is newer than index, use that time, esp when no index
wrangleData config (SC.WrangleWithPredictions indexType indexAndEncoder encodeToPredict) cachedA p = do
  let indexAndEncoder_C = fmap indexAndEncoder cachedA
      b_C = fmap fst indexAndEncoder_C
      encoder_C = fmap snd indexAndEncoder_C
  index_C <- manageIndex config indexType b_C
  curJSON_C <- K.fileDependency $ jsonFP config
  let newJSON_C = encoder_C <*> cachedA
      jsonDeps = (,) <$> newJSON_C <*> index_C
  updatedJSON_C <- K.updateIf curJSON_C jsonDeps $ \(e, b) -> do
    dataEncoding <- K.knitEither e
    toPredictEncoding <- K.knitEither $ encodeToPredict b p
    K.liftKnit . BL.writeFile (jsonFP config) $ A.encodingToLazyByteString $ A.pairs (dataEncoding <> toPredictEncoding)
  return $ const <$> index_C <*> updatedJSON_C -- if json is newer than index, use that time, esp when no index

manageIndex ::
  (K.KnitEffects r, K.CacheEffectsD r) =>
  SC.ModelRunnerConfig ->
  SC.DataIndexerType b ->
  K.ActionWithCacheTime r b ->
  K.Sem r (K.ActionWithCacheTime r b)
manageIndex config dataIndexer bFromA_C = do
  case dataIndexer of
    SC.CacheableIndex indexCacheKey -> do
      curJSON_C <- K.fileDependency $ jsonFP config
      when (Maybe.isNothing $ K.cacheTime curJSON_C) $ do
        K.logLE K.Diagnostic $ "JSON data (\"" <> T.pack (jsonFP config) <> "\") is missing.  Deleting cached indices to force rebuild."
        K.clearIfPresent @_ @K.DefaultCacheData (indexCacheKey config)
      K.retrieveOrMake @K.DefaultSerializer @K.DefaultCacheData (indexCacheKey config) bFromA_C (return . id)
    _ -> return bFromA_C

jsonFP :: SC.ModelRunnerConfig -> FilePath
jsonFP config = SC.addDirFP (T.unpack (SC.mrcModelDir config) ++ "/data") $ T.unpack $ SC.mrcDatFile config

runModel ::
  (K.KnitEffects r, K.CacheEffectsD r) =>
  SC.ModelRunnerConfig ->
  RScripts ->
  SC.DataWrangler a b p ->
  SC.ResultAction r a b p c ->
  p ->
  K.ActionWithCacheTime r a ->
  K.Sem r c
runModel config rScriptsToWrite dataWrangler makeResult toPredict cachedA = K.wrapPrefix "Stan.ModelRunner.runModel" $ do
  let modelNameS = T.unpack $ SC.mrcModel config
      modelDirS = T.unpack $ SC.mrcModelDir config
  K.logLE K.Info "running Model"
  let outputFiles = fmap (\n -> SC.outputFile (SC.mrcOutputPrefix config) n) [1 .. (SC.mrcNumChains config)]
  checkClangEnv
  checkDir (SC.mrcModelDir config) >>= K.knitMaybe "Model directory is missing!"
  createDirIfNecessary (SC.mrcModelDir config <> "/data") -- json inputs
  createDirIfNecessary (SC.mrcModelDir config <> "/output") -- csv model run output
  createDirIfNecessary (SC.mrcModelDir config <> "/R") -- scripts to load fit into R for shinyStan or loo.
  indices_C <- wrangleData config dataWrangler cachedA toPredict
  curModel_C <- K.fileDependency (SC.addDirFP modelDirS $ T.unpack $ SB.modelFile $ SC.mrcModel config)
  stanOutput_C <- do
    curStanOutputs_C <- fmap K.oldestUnit $ traverse (K.fileDependency . SC.addDirFP (modelDirS ++ "/output")) outputFiles
    let runStanDeps = (,) <$> indices_C <*> curModel_C -- indices_C carries input data update time
        runOneChain chainIndex = do
          let exeConfig = (SC.mrcStanExeConfigF config) chainIndex
          K.logLE K.Info $ "Running " <> T.pack modelNameS <> " for chain " <> (T.pack $ show chainIndex)
          K.logLE K.Diagnostic $ "Command: " <> T.pack (CS.toStanExeCmdLine exeConfig)
          K.liftKnit $ CS.stan (SC.addDirFP modelDirS modelNameS) exeConfig
          K.logLE K.Info $ "Finished chain " <> (T.pack $ show chainIndex)
    res_C <- K.updateIf (fmap Just curStanOutputs_C) runStanDeps $ \_ -> do
      K.logLE K.Info "Stan outputs older than input data or model.  Rebuilding Stan exe and running."
      K.logLE K.Info $ "Make CommandLine: " <> (T.pack $ CS.makeConfigToCmdLine (SC.mrcStanMakeConfig config))
      K.liftKnit $ CS.make (SC.mrcStanMakeConfig config)
      maybe Nothing (const $ Just ()) . sequence <$> (K.sequenceConcurrently $ fmap runOneChain [1 .. (SC.mrcNumChains config)])
    K.ignoreCacheTime res_C >>= K.knitMaybe "There was an error running an MCMC chain."
    K.logLE K.Info "writing R scripts"
    K.liftKnit $ writeRScripts rScriptsToWrite config
    return res_C
  let resultDeps = (\a b c -> (a, b)) <$> cachedA <*> indices_C <*> stanOutput_C
  case makeResult of
    SC.UseSummary f -> do
      K.logLE K.Diagnostic $
        "Summary command: "
          <> (T.pack $ (CS.cmdStanDir . SC.mrcStanMakeConfig $ config) ++ "/bin/stansummary")
          <> " "
          <> T.intercalate " " (fmap T.pack (CS.stansummaryConfigToCmdLine (SC.mrcStanSummaryConfig config)))
      summary <- K.liftKnit $ CS.stansummary (SC.mrcStanSummaryConfig config)
      when (SC.mrcLogSummary config) $ K.logLE K.Info $ "Stan Summary:\n" <> (T.pack $ CS.unparsed summary)
      f summary toPredict resultDeps
    SC.SkipSummary f -> f toPredict resultDeps
    SC.DoNothing -> return ()

checkClangEnv :: (P.Members '[P.Embed IO] r, K.LogWithPrefixesLE r) => K.Sem r ()
checkClangEnv = K.wrapPrefix "checkClangEnv" $ do
  clangBinDirM <- K.liftKnit $ Env.lookupEnv "CLANG_BINDIR"
  case clangBinDirM of
    Nothing -> K.logLE K.Info "CLANG_BINDIR not set. Using existing path for clang."
    Just clangBinDir -> do
      curPath <- K.liftKnit $ Env.getEnv "PATH"
      K.logLE K.Info $ "Current path: " <> (T.pack $ show curPath) <> ".  Adding " <> (T.pack $ show clangBinDir) <> " for llvm clang."
      K.liftKnit $ Env.setEnv "PATH" (clangBinDir ++ ":" ++ curPath)

createDirIfNecessary ::
  (P.Members '[P.Embed IO] r, K.LogWithPrefixesLE r) =>
  T.Text ->
  K.Sem r ()
createDirIfNecessary dir = K.wrapPrefix "createDirIfNecessary" $ do
  K.logLE K.Diagnostic $ "Checking if cache path (\"" <> dir <> "\") exists."
  existsB <- P.embed $ (Dir.doesDirectoryExist (T.unpack dir))
  case existsB of
    True -> do
      K.logLE K.Diagnostic $ "\"" <> dir <> "\" exists."
      return ()
    False -> do
      K.logLE K.Info $
        "Cache directory (\""
          <> dir
          <> "\") not found. Atttempting to create."
      P.embed $
        Dir.createDirectoryIfMissing True (T.unpack dir)
{-# INLINEABLE createDirIfNecessary #-}

checkDir ::
  (P.Members '[P.Embed IO] r, K.LogWithPrefixesLE r) =>
  T.Text ->
  P.Sem r (Maybe ())
checkDir dir = K.wrapPrefix "checkDir" $ do
  K.logLE K.Diagnostic $ "Checking if cache path (\"" <> dir <> "\") exists."
  existsB <- P.embed $ (Dir.doesDirectoryExist (T.unpack dir))
  case existsB of
    True -> do
      K.logLE K.Diagnostic $ "\"" <> dir <> "\" exists."
      return $ Just ()
    False -> do
      K.logLE K.Diagnostic $ "\"" <> dir <> "\" is missing."
      return Nothing

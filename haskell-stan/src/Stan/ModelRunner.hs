{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}

module Stan.ModelRunner
  ( module Stan.ModelRunner,
    module CmdStan,
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
import qualified Data.Aeson as A
import qualified Data.Aeson.Encoding as A
import qualified Data.ByteString.Lazy as BL
import qualified Data.Maybe as Maybe
import qualified Data.Text as T
import qualified Data.Text.IO as T
import qualified Knit.Effect.AtomicCache as K (cacheTime)
import qualified Knit.Report as K
import qualified Polysemy as P
import qualified Say
import qualified Stan.ModelBuilder as SB
import qualified Stan.ModelConfig as SC
import qualified Stan.RScriptBuilder as SR
import qualified Stan.SamplerCSV as SCSV
import qualified System.Directory as Dir
import qualified System.Environment as Env
import qualified Relude.Extra as Relude

makeDefaultModelRunnerConfig :: forall st cd r. SC.KnitStan st cd r
  => SC.RunnerInputNames
  -- | Assume model file exists when Nothing.  Otherwise generate from this and use.
  -> Maybe (SB.GeneratedQuantities, SB.StanModel)
  -> SC.StanMCParameters
  -> Maybe CS.StancConfig
  -> K.Sem r SC.ModelRunnerConfig
makeDefaultModelRunnerConfig runnerInputNames modelM stanMCParameters mStancConfig = do
  let doOnlyLL = case modelM of
        Nothing -> False
        Just (_, m) ->  SB.genLogLikelihoodBlock m /= T.empty
      stanMakeConfig mr = do
        writeModel runnerInputNames mr modelM
        stanMakeNoGQConfig' <- K.liftKnit $ CS.makeDefaultMakeConfig (toString $ SC.modelPath mr runnerInputNames)
        return $  stanMakeNoGQConfig' {CS.stancFlags = mStancConfig}
  stanMakeNoGQConfig <- stanMakeConfig SC.MRNoGQ
  stanMakeOnlyLLConfig <- stanMakeConfig SC.MROnlyLL
  stanMakeFullConfig <- stanMakeConfig SC.MRFull
  let makeConfigs :: SC.ModelRun -> CS.MakeConfig
      makeConfigs SC.MRNoGQ = stanMakeNoGQConfig
      makeConfigs SC.MROnlyLL = stanMakeOnlyLLConfig
      makeConfigs SC.MRFull = stanMakeFullConfig
  stanSummaryConfig <-
    K.liftKnit $
      CS.useCmdStanDirForStansummary (CS.makeDefaultSummaryConfig [])
  return $
    SC.ModelRunnerConfig
      doOnlyLL
      makeConfigs
      stanSummaryConfig
      runnerInputNames
      stanMCParameters
      True
      True
{-# INLINEABLE makeDefaultModelRunnerConfig #-}

modelGQ :: SC.ModelRun -> SB.GeneratedQuantities -> SB.GeneratedQuantities
modelGQ SC.MRNoGQ _ = SB.NoGQ
modelGQ SC.MROnlyLL _ = SB.OnlyLL
modelGQ SC.MRFull x = SB.NoLL

writeModel ::  K.KnitEffects r
  => SC.RunnerInputNames
  -> SC.ModelRun
  -- | Assume model file exists when Nothing.  Otherwise generate from this and use.
  -> Maybe (SB.GeneratedQuantities, SB.StanModel)
  -> K.Sem r ()
writeModel runnerInputNames modelRun modelM = do
  let modelDir = SC.rinModelDir runnerInputNames
      modelName = SC.modelName modelRun runnerInputNames
  case modelM of
    Nothing -> return ()
    Just (gq', m) -> do
      createDirIfNecessary modelDir
      let gq = modelGQ modelRun gq'
      modelState <- K.liftKnit $ SB.renameAndWriteIfNotSame gq m modelDir modelName
      case modelState of
        SB.New -> K.logLE K.Diagnostic "Given model was new."
        SB.Same -> K.logLE K.Diagnostic "Given model was the same as existing model file."
        SB.Updated newName -> K.logLE K.Diagnostic
                              $ "Given model was different from exisiting.  Old one was moved to \""
                              <> newName <> "\"."
{-# INLINEABLE writeModel #-}

sampleExeConfig :: SC.RunnerInputNames -> SC.StanMCParameters -> CS.StanExeConfig
sampleExeConfig rin smp =
    (CS.makeDefaultSample (toString $  SC.modelName SC.MRNoGQ rin) Nothing)
    { CS.inputData = Just (SC.dataDirPath rin $ SC.combinedDataFileName rin)
    , CS.output = Just (SC.outputDirPath rin $ SC.outputPrefix SC.MRNoGQ rin <> ".csv")
    , CS.numChains = Just $ SC.smcNumChains smp
    , CS.numThreads = Just $ SC.smcNumThreads smp
    , CS.numSamples = SC.smcNumSamplesM smp
    , CS.numWarmup = SC.smcNumWarmupM smp
    , CS.adaptDelta = SC.smcAdaptDeltaM smp
    , CS.maxTreeDepth = SC.smcMaxTreeDepth smp
    , CS.randomSeed = SC.smcRandomSeed smp
    }
{-# INLINEABLE sampleExeConfig #-}

gqExeConfig :: SC.ModelRun
                -> SC.RunnerInputNames
                -> SC.StanMCParameters
                -> Int
                -> CS.StanExeConfig
gqExeConfig mr rin smp n = do
  (CS.makeDefaultGenerateQuantities (toString $ SC.modelName mr rin) n)
    { CS.inputData = Just (SC.dataDirPath rin $ SC.combinedDataFileName rin)
    , CS.fittedParams = Just (SC.outputDirPath rin $ SC.outputPrefix SC.MRNoGQ rin <> "_" <> show n <> ".csv")
    , CS.output = Just (SC.outputDirPath rin $ SC.outputPrefix mr rin <> "_" <> show n <> ".csv")
    , CS.randomSeed = SC.smcRandomSeed smp
    }
{-# INLINEABLE gqExeConfig #-}

data RScripts = None | ShinyStan [SR.UnwrapJSON] | Loo | Both [SR.UnwrapJSON] deriving (Show, Eq, Ord)
looOf :: RScripts -> RScripts
looOf None = None
looOf (ShinyStan _) = None
looOf Loo = Loo
looOf (Both _) = Loo

shinyOf :: RScripts -> RScripts
shinyOf None = None
shinyOf x@(ShinyStan _) = x
shinyOf Loo = None
shinyOf (Both x) = ShinyStan x

writeRScripts :: forall st cd r. SC.KnitStan st cd r => RScripts -> SC.ModelRun -> SC.ModelRunnerConfig -> K.Sem r ()
writeRScripts rScripts mr config = do
  let scriptPrefix = SC.mergedPrefix mr $ SC.mrcInputNames config
      write mSuffix t = writeFileText (SC.rDirPath (SC.mrcInputNames config) scriptPrefix   <> fromMaybe "" mSuffix <> ".R") t
      writeShiny ujs = write (Just "_shinystan") $ SR.shinyStanScript mr config ujs
      writeLoo = write Nothing $ SR.looScript mr config scriptPrefix 10
  case rScripts of
    None -> pure ()
    ShinyStan ujs -> writeShiny ujs
    Loo -> writeLoo
    Both ujs -> writeShiny ujs >> writeLoo
{-# INLINEABLE writeRScripts #-}

wrangleDataWithoutPredictions :: forall st cd md gq b r.
  (SC.KnitStan st cd r)
  => SC.ModelRunnerConfig
  -> SC.DataWrangler md gq b ()
  -> SC.Cacheable st b
  -> K.ActionWithCacheTime r md
  -> K.ActionWithCacheTime r gq
  -> K.Sem r (K.ActionWithCacheTime r (Either T.Text b), K.ActionWithCacheTime r (Either T.Text b))
wrangleDataWithoutPredictions config dw cb md_C gq_C = wrangleData @st @cd config dw cb md_C gq_C ()
{-# INLINE wrangleDataWithoutPredictions #-}

wrangleData :: forall st cd md gq b p r.SC.KnitStan st cd r
  => SC.ModelRunnerConfig
  -> SC.DataWrangler md gq b p
  -> SC.Cacheable st b
  -> K.ActionWithCacheTime r md
  -> K.ActionWithCacheTime r gq
  -> p
  -> K.Sem r (K.ActionWithCacheTime r (Either T.Text b), K.ActionWithCacheTime r (Either T.Text b))
wrangleData config w cb md_C gq_C p = do
  let (indexerType, modelIndexAndEncoder, mGQIndexAndEncoder) = case w of
        SC.Wrangle x y z -> (x, y, z)
        SC.WrangleWithPredictions x y z _ -> (x, y, z)
  curModelData_C <- SC.modelDataDependency $ SC.mrcInputNames config
  (newModelData_C, modelIndexes_C) <- wranglerPrep @st @cd config SC.ModelData indexerType modelIndexAndEncoder cb md_C
  modelJSON_C <- K.updateIf curModelData_C newModelData_C $ \e -> do
    let modelDataFileName = SC.modelDataFileName $ SC.mrcInputNames config
    K.logLE K.Diagnostic $ "existing model json (" <> modelDataFileName  <> ") appears older than cached data."
    jsonEncoding <- K.knitEither e
    K.liftKnit . BL.writeFile (SC.dataDirPath (SC.mrcInputNames config) modelDataFileName)
      $ A.encodingToLazyByteString $ A.pairs jsonEncoding
  let model_C = const <$> modelIndexes_C <*> modelJSON_C
  gq_C <- case mGQIndexAndEncoder of
    Nothing -> return $ pure $ Left "wrangleData: Attempt to use GQ indexes but No GQ wrangler given."
    Just gqIndexAndEncoder -> do
      mGQData_C <- SC.gqDataDependency $ SC.mrcInputNames config
      case mGQData_C of
        Nothing -> return $ pure $ Left "wrangleData: Attempt to wrangle GQ data but config.mrcInputNames.rinQG is Nothing."
        Just gqData_C -> do
          (newGQData_C, gqIndexes_C) <- wranglerPrep @st @cd config SC.GQData indexerType gqIndexAndEncoder cb gq_C
          let gqJSONDeps = (,) <$> newGQData_C <*> gqIndexes_C
          gqJSON_C <- K.updateIf gqData_C gqJSONDeps $ \(e, eb) -> do
            gqDataFileName <- K.knitMaybe "Attempt to build gq json but rinGQ is Nothing." $ SC.gqDataFileName $ SC.mrcInputNames config
            K.logLE K.Diagnostic $ "existing GQ json (" <> gqDataFileName  <> ") appears older than cached data."
            jsonEncoding <- K.knitEither e
            indexEncoding <- case w of
              SC.Wrangle _ _ _ -> return mempty
              SC.WrangleWithPredictions _ _ _ encodeToPredict -> K.knitEither $ encodeToPredict eb p
            writeFileLBS (SC.dataDirPath (SC.mrcInputNames config) gqDataFileName)
              $ A.encodingToLazyByteString $ A.pairs (jsonEncoding <> indexEncoding)
          return $ const <$> gqIndexes_C <*> gqJSON_C
  return (model_C, gq_C)
{-# INLINEABLE wrangleData #-}

-- create function to rebuild json along with time stamp from data used
wranglerPrep :: forall st cd a b r.
  SC.KnitStan st cd r
  => SC.ModelRunnerConfig
  -> SC.InputDataType
  -> SC.DataIndexerType b
  -> SC.Wrangler a b
  -> SC.Cacheable st b
  -> K.ActionWithCacheTime r a
  -> K.Sem r (K.ActionWithCacheTime r (Either Text A.Series), K.ActionWithCacheTime r (Either T.Text b))
wranglerPrep config inputDataType indexerType wrangler cb a_C = do
  let indexAndEncoder_C = fmap wrangler a_C
      eb_C = fmap fst indexAndEncoder_C
      encoder_C = fmap snd indexAndEncoder_C
  index_C <- manageIndex @st @cd config inputDataType indexerType cb eb_C
  let newJSON_C = encoder_C <*> a_C
  return (newJSON_C, index_C)
{-# INLINEABLE wranglerPrep #-}

-- if we are caching the index (not sure this is ever worth it!)
-- here is where we check if that cache needs updating.  Otherwise we
-- just return it
manageIndex :: forall st cd b r.
  SC.KnitStan st cd r
  => SC.ModelRunnerConfig
  -> SC.InputDataType
  -> SC.DataIndexerType b
  -> SC.Cacheable st b
  -> K.ActionWithCacheTime r (Either T.Text b)
  -> K.Sem r (K.ActionWithCacheTime r (Either T.Text b))
manageIndex config inputDataType dataIndexer cb ebFromA_C = do
  case dataIndexer of
    SC.CacheableIndex indexCacheKey ->
      case cb of
        SC.Cacheable -> do
          curJSON_C <- case inputDataType of
            SC.ModelData -> SC.modelDataDependency (SC.mrcInputNames config)
            SC.GQData -> do
              mGQJSON_C <- SC.gqDataDependency (SC.mrcInputNames config)
              maybe (K.knitError "ModelRunner.manageIndex called with input type GQ but no GQ setup.") return mGQJSON_C
          when (isNothing $ K.cacheTime curJSON_C) $ do
            let jsonFP = case inputDataType of
                  SC.ModelData -> SC.modelDataFileName $ SC.mrcInputNames config
                  SC.GQData -> fromMaybe "Error:No GQ Setup" $ SC.gqDataFileName $ SC.mrcInputNames config
            K.logLE (K.Debug 1)  $ "JSON data (\"" <> jsonFP <> "\") is missing.  Deleting cached indices to force rebuild."
            K.clearIfPresent @Text @cd (indexCacheKey config inputDataType)
          K.retrieveOrMake @st @cd (indexCacheKey config inputDataType) ebFromA_C return
        _ -> K.knitError "Cacheable index type provided but b is Uncacheable."
    _ -> pure ebFromA_C
{-# INLINEABLE manageIndex #-}

-- where do we combine data??
runModel :: forall st cd md gq b p c r.
  (SC.KnitStan st cd r)
  => SC.ModelRunnerConfig
  -> RScripts
  -> SC.DataWrangler md gq b p
  -> SC.Cacheable st b
  -> SC.ResultAction r md gq b p c
  -> p
  -> K.ActionWithCacheTime r md
  -> K.ActionWithCacheTime r gq
  -> K.Sem r c
runModel config rScriptsToWrite dataWrangler cb makeResult toPredict md_C gq_C = K.wrapPrefix "Stan.ModelRunner.runModel" $ do
  K.logLE K.Info "running Model (if necessary)"
  let runnerInputNames = SC.mrcInputNames config
      stanMCParameters = SC.mrcStanMCParameters config
  checkClangEnv
  createDirIfNecessary (SC.mrcModelDir config)
  createDirIfNecessary (SC.mrcModelDir config <> "/data") -- json inputs
  createDirIfNecessary (SC.mrcModelDir config <> "/output") -- csv model run output
  createDirIfNecessary (SC.mrcModelDir config <> "/R") -- scripts to load fit into R for shinyStan or loo.
  (modelIndices_C, gqIndices_C) <- wrangleData @st @cd config dataWrangler cb md_C gq_C toPredict
  curModelNoGQ_C <- SC.modelDependency SC.MRNoGQ runnerInputNames
  curModel_C <- SC.modelDependency SC.MRFull runnerInputNames
  -- run model and/or build gq samples as necessary
  (modelResDep, mGQResDep) <- do
    let runModel = do
          let modelExeConfig = sampleExeConfig runnerInputNames stanMCParameters
          K.logLE K.Diagnostic $ "Running Model: " <> SC.modelName SC.MRNoGQ runnerInputNames
          K.logLE K.Diagnostic $ "Command: " <> toText (CS.toStanExeCmdLine modelExeConfig)
          K.liftKnit $ CS.stan (SC.modelPath SC.MRNoGQ runnerInputNames) modelExeConfig
          K.logLE K.Diagnostic $ "Finished " <> SC.modelName SC.MRNoGQ runnerInputNames
        runOneGQ mr n = do
          let exeConfig = gqExeConfig mr runnerInputNames stanMCParameters n
          K.logLE K.Diagnostic $ "Generating " <> show mr
          K.logLE K.Diagnostic $ "Using fitted parameters from model " <> SC.modelName SC.MRNoGQ runnerInputNames
          K.logLE K.Diagnostic $ "Command: " <> toText (CS.toStanExeCmdLine $ exeConfig)
          K.liftKnit $ CS.stan (SC.modelPath mr runnerInputNames) exeConfig
          K.logLE K.Diagnostic $ "Finished generating " <> show mr
          K.logLE K.Diagnostic $ "Merging samples..."
          samplesFP <- K.knitMaybe "runModel.runOneGQ: fittedParams field is Nothing in exeConfig"
            $ CS.fittedParams exeConfig
          llFP <- K.knitMaybe "runModel.runOneGQ: output field is Nothing in exeConfig" $ CS.output exeConfig
          let mergedFP = SC.outputDirPath (SC.mrcInputNames config)
                $ SC.mergedPrefix mr runnerInputNames <> "_" <> show n <> ".csv"
          K.liftKnit $ SCSV.appendGQsToSamplerCSV samplesFP llFP mergedFP
    SC.combineData (SC.mrcInputNames config)  -- this only does anything if either data set has changed
    let modelSamplesFileNames =  SC.samplesFileNames SC.MRNoGQ config
    modelSamplesFilesDep <- K.oldestUnit <$> traverse K.fileDependency modelSamplesFileNames
    let runModelDeps = (,) <$> modelIndices_C <*> curModelNoGQ_C -- indices carries data update time
    modelRes_C <- K.updateIf modelSamplesFilesDep runModelDeps $ \_ -> do
      K.logLE K.Diagnostic "Stan model outputs older than model input data or model code.  Rebuilding Stan exe and running."
      K.logLE (K.Debug 1) $ "Make CommandLine: " <> show (CS.makeConfigToCmdLine (SC.mrcStanMakeConfig config SC.MRNoGQ))
      K.liftKnit $ CS.make (SC.mrcStanMakeConfig config SC.MRNoGQ)
      res <- runModel
      when (SC.mrcRunDiagnose config) $ do
        K.logLE K.Info "Running stan diagnostics"
        K.liftKnit $ CS.diagnoseCSD modelSamplesFileNames
      K.logLE K.Diagnostic "writing R scripts for new model run."
      writeRScripts @st @cd (shinyOf rScriptsToWrite) SC.MRNoGQ config
      return res
    case SC.mrcDoOnlyLL config of
      False -> return ()
      True -> do
        curModelOnlyLL_C <- SC.modelDependency SC.MROnlyLL runnerInputNames
        let runLLDeps  = (,) <$> modelIndices_C <*> curModelOnlyLL_C -- indices carries data update time
        let onlySamplesFileNames = SC.samplesFileNames SC.MROnlyLL config
        onlyLLSamplesFileDep <- K.oldestUnit <$> traverse K.fileDependency onlySamplesFileNames
        K.updateIf onlyLLSamplesFileDep runLLDeps $ \_ -> do
          let llOnlyMakeConfig = SC.mrcStanMakeConfig config SC.MROnlyLL
          K.logLE K.Diagnostic "Stan log likelihood  outputs older than model input data or model code. Generating LL."
          K.logLE (K.Debug 1) $ "Make CommandLine: " <> show (CS.makeConfigToCmdLine llOnlyMakeConfig)
          K.liftKnit $ CS.make llOnlyMakeConfig
          mRes <- maybe Nothing (const $ Just ()) . sequence
                  <$> K.sequenceConcurrently (fmap (runOneGQ SC.MROnlyLL) [1 .. (SC.smcNumChains $ SC.mrcStanMCParameters config)])
          K.knitMaybe "There was an error generating LL for a chain." mRes
        writeRScripts @st @cd (looOf rScriptsToWrite) SC.MROnlyLL config
        return ()
    mGQRes_C <- case SC.rinGQ runnerInputNames of
      Nothing -> pure Nothing
      Just _ -> do
        let runGQDeps = (,,) <$> modelIndices_C <*> gqIndices_C <*> curModel_C
        gqSamplesFileDep <- K.oldestUnit <$> traverse K.fileDependency (SC.samplesFileNames SC.MRFull config)
        res_C <- K.updateIf gqSamplesFileDep runGQDeps $ const $ do
          K.logLE K.Diagnostic "Stan GQ outputs older than model input data, GQ input data or model code. Running GQ."
          K.logLE (K.Debug 1) $ "Make CommandLine: " <> show (CS.makeConfigToCmdLine (SC.mrcStanMakeConfig config SC.MRFull))
          K.liftKnit $ CS.make (SC.mrcStanMakeConfig config SC.MRFull)
--          SC.combineData $ SC.mrcInputNames config
          mRes <- maybe Nothing (const $ Just ()) . sequence
                <$> K.sequenceConcurrently (fmap (runOneGQ SC.MRFull) [1 .. (SC.smcNumChains $ SC.mrcStanMCParameters config)])
          K.knitMaybe "There was an error running GQ for a chain." mRes
        writeRScripts @st @cd (shinyOf rScriptsToWrite) SC.MRFull config
        return $ Just res_C
    return (modelRes_C, mGQRes_C)
  let outputFileNames = SC.finalSamplesFileNames SC.MRFull config
      outputDep = case mGQResDep of
        Nothing -> modelResDep
        Just gqResDep -> const <$> gqResDep <*> modelResDep
      makeSummaryFromCSVs csvFileNames summaryPath = do
        K.logLE K.Diagnostic "Stan summary older output.  Re-summarizing."
        K.logLE (K.Debug 1) $
          "Summary command: "
          <> show ((CS.cmdStanDir $ SC.mrcStanMakeConfig config SC.MRFull) ++ "/bin/stansummary")
          <> " "
          <> T.intercalate " " (fmap T.pack (CS.stansummaryConfigToCmdLine (SC.mrcStanSummaryConfig config)))
        summary <- K.liftKnit $ CS.stansummary ((SC.mrcStanSummaryConfig config) {CS.sampleFiles = csvFileNames})
        P.embed $ A.encodeFile summaryPath summary
        return summary
      getSummary csvFileNames summaryPath = do
        summaryE <- K.ignoreCacheTimeM
                    $ K.loadOrMakeFile
                    summaryPath
                    ((K.knitEither =<<) . P.embed . Relude.firstF toText . A.eitherDecodeFileStrict . toString)
                    outputDep -- this only is here to carry the timing to compare the output file with
                    (const $ makeSummaryFromCSVs csvFileNames summaryPath)
        K.knitEither $ first toText $ summaryE
      modelResultDeps = (\a b _ -> (a, b)) <$> md_C <*> modelIndices_C <*> modelResDep
      mGQResultDeps = case mGQResDep of
        Nothing -> Nothing
        Just gqResDep -> Just $ (\a b _ -> (a, b)) <$> gq_C <*> gqIndices_C <*> gqResDep
  case makeResult of
    SC.UseSummary f -> do
      let summaryFileName = SC.summaryFileName SC.MRFull config
          samplesFileNames = SC.finalSamplesFileNames SC.MRFull config
      summary <-  getSummary samplesFileNames (SC.outputDirPath (SC.mrcInputNames config) summaryFileName)
      when (SC.mrcLogSummary config) $ do
        K.logLE K.Info $ "Stan Summary:\n"
        Say.say $ toText (CS.unparsed summary)
      f summary toPredict modelResultDeps mGQResultDeps
    SC.SkipSummary f -> f toPredict modelResultDeps mGQResultDeps
    SC.DoNothing -> return ()
{-# INLINEABLE runModel #-}

data StaleFiles = StaleData | StaleOutput | StaleSummary deriving (Show, Eq, Ord)

deleteStaleFiles :: forall st cd r.SC.KnitStan st cd r => SC.ModelRunnerConfig -> [StaleFiles] -> K.Sem r ()
deleteStaleFiles config staleFiles = do
  let samplesFilePaths = concat $ fmap (\mr -> SC.samplesFileNames mr config) [SC.MRNoGQ, SC.MROnlyLL, SC.MRFull]
      modelSummaryPath = SC.outputDirPath (SC.mrcInputNames config) $ SC.summaryFileName SC.MRNoGQ config
      gqSummaryPath = SC.outputDirPath (SC.mrcInputNames config) $ SC.summaryFileName SC.MRFull config
      modelDataPath = SC.dataDirPath (SC.mrcInputNames config) $ SC.modelDataFileName $ SC.mrcInputNames config
      mGQDataPath = SC.dataDirPath (SC.mrcInputNames config) <$> (SC.gqDataFileName $ SC.mrcInputNames config)
      toDelete x = case x of
        StaleData -> [modelDataPath] ++ maybe [] one mGQDataPath
        StaleOutput -> samplesFilePaths
        StaleSummary -> [modelSummaryPath, gqSummaryPath]
      exists fp = K.liftKnit $ Dir.doesFileExist fp >>= \x -> return $ if x then Just fp else Nothing
      filesToDelete = ordNub $ concat $ toDelete <$> staleFiles
  extantPaths <- catMaybes <$> traverse exists filesToDelete
  Say.say $ "Deleting output files: " <> T.intercalate "," (toText <$> extantPaths)
  traverse_ (K.liftKnit. Dir.removeFile) extantPaths
{-# INLINEABLE deleteStaleFiles #-}


checkClangEnv :: (P.Member (P.Embed IO) r, K.LogWithPrefixesLE r) => K.Sem r ()
checkClangEnv = K.wrapPrefix "checkClangEnv" $ do
  clangBinDirM <- K.liftKnit $ Env.lookupEnv "CLANG_BINDIR"
  case clangBinDirM of
    Nothing -> K.logLE K.Diagnostic "CLANG_BINDIR not set. Using existing path for clang."
    Just clangBinDir -> do
      curPath <- K.liftKnit $ Env.getEnv "PATH"
      K.logLE K.Diagnostic $ "Current path: " <> show curPath <> ".  Adding " <> show clangBinDir <> " for llvm clang."
      K.liftKnit $ Env.setEnv "PATH" (clangBinDir ++ ":" ++ curPath)
{-# INLINEABLE checkClangEnv #-}

createDirIfNecessary ::
  (P.Member (P.Embed IO) r, K.LogWithPrefixesLE r) =>
  T.Text ->
  K.Sem r ()
createDirIfNecessary dir = K.wrapPrefix "createDirIfNecessary" $ do
  K.logLE (K.Debug 1) $ "Checking if cache path (\"" <> dir <> "\") exists."
  existsB <- P.embed $ Dir.doesDirectoryExist (toString dir)
  if existsB
    then (do
             K.logLE (K.Debug 1) $ "\"" <> dir <> "\" exists."
             return ())
    else (do
             K.logLE K.Diagnostic $
               "Cache directory (\""
               <> dir
               <> "\") not found. Atttempting to create."
             P.embed $
               Dir.createDirectoryIfMissing True (T.unpack dir))
{-# INLINEABLE createDirIfNecessary #-}

checkDir ::
  (P.Member (P.Embed IO) r, K.LogWithPrefixesLE r) =>
  T.Text ->
  P.Sem r (Maybe ())
checkDir dir = K.wrapPrefix "checkDir" $ do
  cwd <- P.embed Dir.getCurrentDirectory
  K.logLE (K.Debug 1) $ "CWD = \"" <> show cwd <> "\""
  K.logLE (K.Debug 1) $ "Checking if cache path (\"" <> dir <> "\") exists."
  existsB <- P.embed $ Dir.doesDirectoryExist (toString dir)
  if existsB
    then (do
             K.logLE (K.Debug 1) $ "\"" <> dir <> "\" exists."
             return $ Just ()
         )
    else (do
             K.logLE (K.Debug 1) $ "\"" <> dir <> "\" is missing."
             return Nothing
         )
{-# INLINEABLE checkDir #-}

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
makeDefaultModelRunnerConfig runnerInputNames modelM stanMCParameters stancConfigM = do
  writeModel runnerInputNames True modelM
  writeModel runnerInputNames False modelM
  stanMakeConfig' <- K.liftKnit $ CS.makeDefaultMakeConfig (toString $ SC.modelPath runnerInputNames)
  let stanMakeConfig = stanMakeConfig' {CS.stancFlags = stancConfigM}
  stanMakeNoGQConfig' <- K.liftKnit $ CS.makeDefaultMakeConfig (toString $ SC.modelNoGQPath runnerInputNames)
  let stanMakeNoGQConfig = stanMakeNoGQConfig' {CS.stancFlags = stancConfigM}
  stanSummaryConfig <-
    K.liftKnit $
      CS.useCmdStanDirForStansummary (CS.makeDefaultSummaryConfig [])
  return $
    SC.ModelRunnerConfig
      stanMakeConfig
      stanMakeNoGQConfig
      (sampleExeConfig runnerInputNames stanMCParameters)
      (mGQExeConfig runnerInputNames stanMCParameters)
      stanSummaryConfig
      runnerInputNames
      stanMCParameters
      True
      True
{-# INLINEABLE makeDefaultModelRunnerConfig #-}

writeModel ::  K.KnitEffects r
  => SC.RunnerInputNames
  -> Bool
  -- | Assume model file exists when Nothing.  Otherwise generate from this and use.
  -> Maybe (SB.GeneratedQuantities, SB.StanModel)
  -> K.Sem r ()
writeModel runnerInputNames noGQ modelM = do
  let modelDir = SC.rinModelDir runnerInputNames
      modelName = let mn = SC.rinModel runnerInputNames in mn <> if noGQ then SC.noGQSuffix else ""
  case modelM of
    Nothing -> return ()
    Just (gq', m) -> do
      createDirIfNecessary modelDir
      let gq = if noGQ then SB.NoGQ else gq'
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
    (CS.makeDefaultSample (toString $  SC.rinModelNoGQ rin) Nothing)
    { CS.inputData = Just (SC.dataDirPath rin $ SC.combinedDataFileName rin)
    , CS.output = Just (SC.outputDirPath rin $ SC.modelPrefix rin <> ".csv")
    , CS.numChains = Just $ SC.smcNumChains smp
    , CS.numThreads = Just $ SC.smcNumThreads smp
    , CS.numSamples = SC.smcNumSamplesM smp
    , CS.numWarmup = SC.smcNumWarmupM smp
    , CS.adaptDelta = SC.smcAdaptDeltaM smp
    , CS.maxTreeDepth = SC.smcMaxTreeDepth smp
    , CS.randomSeed = SC.smcRandomSeed smp
    }
{-# INLINEABLE sampleExeConfig #-}

mGQExeConfig :: SC.RunnerInputNames -> SC.StanMCParameters -> Maybe (Int -> CS.StanExeConfig)
mGQExeConfig rin smp = do
  gqPrefix <- SC.gqPrefix rin
  return
    $ \n -> (CS.makeDefaultGenerateQuantities (toString $ SC.rinModel rin) n)
    { CS.inputData = Just (SC.dataDirPath rin $ SC.combinedDataFileName rin)
    , CS.fittedParams = Just (SC.outputDirPath rin $ SC.modelPrefix rin <> "_" <> show n <> ".csv")
    , CS.output = Just (SC.outputDirPath rin $ gqPrefix <> "_gq" <> show n <> ".csv")
    , CS.randomSeed = SC.smcRandomSeed smp
    }
{-# INLINEABLE mGQExeConfig #-}

data RScripts = None | ShinyStan [SR.UnwrapJSON] | Loo | Both [SR.UnwrapJSON] deriving (Show, Eq, Ord)

writeRScripts :: forall st cd r. SC.KnitStan st cd r => RScripts -> SC.InputDataType -> SC.ModelRunnerConfig -> K.Sem r ()
writeRScripts rScripts idt config = do
  let rSamplesPrefix = case idt of
        SC.ModelData -> SC.modelPrefix (SC.mrcInputNames config)
        SC.GQData -> SC.finalPrefix (SC.mrcInputNames config)
  let write mSuffix t = writeFileText (SC.rDirPath (SC.mrcInputNames config) rSamplesPrefix  <> fromMaybe "" mSuffix <> ".R") t
      writeShiny ujs = write (Just "_shinystan") $ SR.shinyStanScript config ujs
      writeLoo = write Nothing $ SR.looScript config rSamplesPrefix 10
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
  let modelSamplesFileNames = SC.modelSamplesFileNames config
  checkClangEnv
  createDirIfNecessary (SC.mrcModelDir config)
  createDirIfNecessary (SC.mrcModelDir config <> "/data") -- json inputs
  createDirIfNecessary (SC.mrcModelDir config <> "/output") -- csv model run output
  createDirIfNecessary (SC.mrcModelDir config <> "/R") -- scripts to load fit into R for shinyStan or loo.
  (modelIndices_C, gqIndices_C) <- wrangleData @st @cd config dataWrangler cb md_C gq_C toPredict
  curModelNoGQ_C <- SC.modelNoGQDependency $ SC.mrcInputNames config
  curModel_C <- SC.modelDependency $ SC.mrcInputNames config
  -- run model and/or build gq samples as necessary
  (modelResDep, mGQResDep) <- do
    let runModelDeps = (,) <$> modelIndices_C <*> curModelNoGQ_C -- indices carries data update time
        runModel = do
          let modelExeConfig = SC.mrcStanExeModelConfig config
          K.logLE K.Diagnostic $ "Running Model: " <> SC.rinModelNoGQ (SC.mrcInputNames config)
          K.logLE K.Diagnostic $ "Command: " <> toText (CS.toStanExeCmdLine modelExeConfig)
          K.liftKnit $ CS.stan (SC.modelNoGQPath $ SC.mrcInputNames config) modelExeConfig
          K.logLE K.Diagnostic $ "Finished " <> SC.rinModel (SC.mrcInputNames config)
        runGQDeps = (,,) <$> modelIndices_C <*> gqIndices_C <*> curModel_C
        runOneGQ n = case SC.mrcStanExeGQConfigM config of
          Nothing -> K.knitError "Attempt to run Stan in GQ mode with no GQ setup (mrcStanExeGQConfigM is Nothing)"
          Just gqExeConfigF -> do
            let gqExeConfig = gqExeConfigF n
            gqName <- case SC.rinGQ (SC.mrcInputNames config) of
              Nothing -> K.knitError "runModel.runGQ: Trying to run GQ setup but GQ name not set (rinGQ is Nothing)"
              Just name -> return name
            K.logLE K.Diagnostic $ "Running GQ: " <> gqName
            K.logLE K.Diagnostic $ "Using results from model " <> SC.rinModel (SC.mrcInputNames config)
            K.logLE K.Diagnostic $ "Command: " <> toText (CS.toStanExeCmdLine $ gqExeConfig)
            K.liftKnit $ CS.stan (SC.modelPath $ SC.mrcInputNames config) gqExeConfig
            K.logLE K.Diagnostic $ "Finished GQ: " <> gqName
            K.logLE K.Diagnostic $ "Merging samples..."
            samplesFP <- K.knitMaybe "runModel.runOneGQ: fittedParams field is Nothing in gq StanExeConfig" $ CS.fittedParams gqExeConfig
            gqFP <- K.knitMaybe "runModel.runOneGQ: output field is Nothing in gq StanExeConfig" $ CS.output gqExeConfig
            mergedFP <- K.knitMaybe "runModel.runOneGQ: gqPrefix returned Nothing when building mergedFP" $ do
              gqPrefix <- SC.gqPrefix (SC.mrcInputNames config)
              return $ SC.outputDirPath (SC.mrcInputNames config) $ gqPrefix <> "_" <> show n <> ".csv"
            K.liftKnit $ SCSV.mergeSamplerAndGQCSVs samplesFP gqFP mergedFP
    modelSamplesFilesDep <- K.oldestUnit <$> traverse K.fileDependency modelSamplesFileNames
    modelRes_C <- K.updateIf modelSamplesFilesDep runModelDeps $ \_ -> do
      K.logLE K.Diagnostic "Stan model outputs older than model input data or model code.  Rebuilding Stan exe and running."
      K.logLE (K.Debug 1) $ "Make CommandLine: " <> show (CS.makeConfigToCmdLine (SC.mrcStanMakeNoGQConfig config))
      K.liftKnit $ CS.make (SC.mrcStanMakeNoGQConfig config)
      SC.combineData (SC.mrcInputNames config)
      res <- runModel
      when (SC.mrcRunDiagnose config) $ do
        K.logLE K.Info "Running stan diagnostics"
        K.liftKnit $ CS.diagnoseCSD modelSamplesFileNames
      K.logLE K.Diagnostic "writing R scripts for new model run."
      writeRScripts @st @cd rScriptsToWrite SC.ModelData config
      return res
    mGQRes_C <- case (SC.mrcStanExeGQConfigM config) of
      Nothing -> pure Nothing
      Just _ -> do
        gqSamplesFileNames <- K.knitMaybe "runModel: GQ samples file names are Nothing but the exeConfig isn't!" $ SC.gqSamplesFileNames config
        gqSamplesFileDep <- K.oldestUnit <$> traverse K.fileDependency gqSamplesFileNames
        res_C <- K.updateIf gqSamplesFileDep runGQDeps $ const $ do
          K.logLE K.Diagnostic "Stan GQ outputs older than model input data, GQ input data or model code. Running GQ."
          K.logLE (K.Debug 1) $ "Make CommandLine: " <> show (CS.makeConfigToCmdLine (SC.mrcStanMakeConfig config))
          K.liftKnit $ CS.make (SC.mrcStanMakeConfig config)
          SC.combineData $ SC.mrcInputNames config
          mRes <- maybe Nothing (const $ Just ()) . sequence <$> K.sequenceConcurrently (fmap runOneGQ [1 .. (SC.smcNumChains $ SC.mrcStanMCParameters config)])
          K.knitMaybe "There was an error running GQ for a chain." mRes
        writeRScripts @st @cd rScriptsToWrite SC.GQData config
        return $ Just res_C
    return (modelRes_C, mGQRes_C)
  let outputFileNames = SC.finalSamplesFileNames config
      outputDep = case mGQResDep of
        Nothing -> modelResDep
        Just gqResDep -> const <$> gqResDep <*> modelResDep
      makeSummaryFromCSVs csvFileNames summaryPath = do
        K.logLE K.Diagnostic "Stan summary older output.  Re-summarizing."
        K.logLE (K.Debug 1) $
          "Summary command: "
          <> show ((CS.cmdStanDir . SC.mrcStanMakeConfig $ config) ++ "/bin/stansummary")
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
      let summaryFileName = fromMaybe (SC.modelSummaryFileName config) $ SC.gqSummaryFileName config
          samplesFileNames = SC.finalSamplesFileNames config
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
  let modelSamplesFilePaths = SC.modelSamplesFileNames config
      gqSamplesFilePaths = SC.gqSamplesFileNames config
      modelSummaryPath = SC.outputDirPath (SC.mrcInputNames config) $ SC.modelSummaryFileName config
      mGQSummaryPath = SC.outputDirPath (SC.mrcInputNames config) <$> SC.gqSummaryFileName config
      modelDataPath = SC.dataDirPath (SC.mrcInputNames config) $ SC.modelDataFileName $ SC.mrcInputNames config
      mGQDataPath = SC.dataDirPath (SC.mrcInputNames config) <$> (SC.gqDataFileName $ SC.mrcInputNames config)
      toDelete x = case x of
        StaleData -> [modelDataPath] ++ maybe [] one mGQDataPath
        StaleOutput -> modelSamplesFilePaths ++ fromMaybe [] gqSamplesFilePaths
        StaleSummary -> [modelSummaryPath] ++ maybe [] one mGQSummaryPath
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

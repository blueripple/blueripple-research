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
import qualified System.Directory as Dir
import qualified System.Environment as Env
import qualified Relude.Extra as Relude
import System.Process (runInteractiveCommand)
import Stan.ModelConfig (modelDataDependency)
import Knit.Report (confusing)
import System.Process.Internals (ProcessHandle(mb_delegate_ctlc))



makeDefaultModelRunnerConfig :: K.KnitEffects r
  => SC.RunnerInputNames
  -- | Assume model file exists when Nothing.  Otherwise generate from this and use.
  -> Maybe (SB.GeneratedQuantities, SB.StanModel)
  -> SC.StanMCParameters
  -> Maybe CS.StancConfig
  -> K.Sem r SC.ModelRunnerConfig
makeDefaultModelRunnerConfig runnerInputNames modelM stanMCParameters stancConfigM = do
  let modelDir = SC.rinModelDir runnerInputNames
      modelName = SC.rinModel runnerInputNames
  writeModel config modelM
  --  let datFileS = maybe (SC.defaultDatFile modelNameT) T.unpack datFileM
  stanMakeConfig' <- K.liftKnit $ CS.makeDefaultMakeConfig (toString SC.modelPath)
  let stanMakeConfig = stanMakeConfig' {CS.stancFlags = stancConfigM}
{-
  modelFileDep <- SC.modelDependency runnerInputNames
  modelDataDep <- SC.modelDataDependency runnerInputNames
  let sampleConfigDeps = (,) <$> modelFileDep <*> modelDataDep
  modelSamplesFileNames <- SC.modelSamplesFileNames runnerInputNames
  let stanExeConfig = if (K.cacheTime modelSamplesFileNames > K.cacheTime sampleConfigDeps)
                      then gqExeConfig runnerInputNames stanMCParameters
                      else sampleExeConfig runnerInputNames stanMCParameters
-}
  samplesPrefix <- SC.samplesPrefix
  let stanSamplesFiles = fmap (SC.sampleFile samplesPrefix) $ Just <$> [1 .. numChains]
  stanSummaryConfig <-
    K.liftKnit $
      CS.useCmdStanDirForStansummary (CS.makeDefaultSummaryConfig $ fmap (SC.outputDirPath rin) stanSamplesFiles)
  return $
    SC.ModelRunnerConfig
      stanMakeConfig
      (sampleExeConfig runnerInputNames stanMCParameters)
      (gqExeConfig runnerInputNames stanMCParameters <$ mrc.rinQG runnerInputNames)
      stanSummaryConfig
      runnerInputNames
      stanMCParameters
      True
      True

writeModel ::  K.KnitEffects r
  => SC.RunnerInputNames
  -- | Assume model file exists when Nothing.  Otherwise generate from this and use.
  -> Maybe (SB.GeneratedQuantities, SB.StanModel)
writeModel config modelM = do
  let modelDir = SC.rinModelDir runnerInputNames
      modelName = SC.rinModel runnerInputNames
  case modelM of
    Nothing -> return ()
    Just (gq, m) -> do
      createDirIfNecessary modelDir
      modelState <- K.liftKnit $ SB.renameAndWriteIfNotSame gq m modelDir modelName
      case modelState of
        SB.New -> K.logLE K.Diagnostic "Given model was new."
        SB.Same -> K.logLE K.Diagnostic "Given model was the same as existing model file."
        SB.Updated newName -> K.logLE K.Diagnostic
                              $ "Given model was different from exisiting.  Old one was moved to \""
                              <> newName <> "\"."

sampleExeConfig :: K.KnitEffects r => SC.RunnerInputNames -> SC.StanMCParameters -> K.Sem r SC.StanExeConfig
sampleExeConfig rin smp =  do
  let samplesKey = SC.ModelSamples
  modelSamplesPrefix <- K.ignoreCacheTimeM $ SC.samplesPrefix rin samplesKey
  return
    $ (CS.makeDefaultSample (toString $  modelName rin) Nothing)
    { CS.inputData = Just (SC.dataDirPath $ SC.combinedDataFileName rin)
    , CS.output = Just (SC.outputDirPath $ modelSamplesPrefix <> ".csv")
    , CS.numChains = Just $ SC.smcNumChains smp
    , CS.numThreads = Just $ SC.smcNumThreads smp
    , CS.numSamples = SC.smcNumSamplesM smp
    , CS.numWarmup = SC.smcNumWarmupM smp
    , CS.adaptDelta = SC.smcAdaptDeltaM smp
    , CS.maxTreeDepth = SC.smcMaxTreeDepth smp
    }

gqExeConfig :: K.KnitEffects r => SC.RunnerInputNames -> SC.StanMCParameters -> K.Sem r (Int -> SC.StanExeConfig)
gqExeConfig rin smp = do
  gqName <- K.knitMaybe "gqExeConfig: RunnerInputNames.rinGQ is Nothing" $ SC.rinGQ rin
  gqSamplesPrefix <- K.ignoreCacheTimeM $ SC.samplesPrefix rin (SC.GQSamples gqName)
  modelSamplesPrefix <- K.ignoreCacheTimeM $ SC.samplesPrefix rin SC.ModelSamples
  when (gqSamplesPrefix == modelSamplesPrefix)
    $ K.knitError "gqExeConfig: model samples and gq samples have same prefix!"
  return
    $ \n -> (CS.makeDefaultGenerateQuantities (toString $ modelName rin) n)
    { CS.inputData = Just (SC.dataDirPath $ SC.combinedDataFileName rin)
    , CS.fittedParams = Just (SC.outputDirPath $ modelSamplesPrefix <> "_" <> show n <> ".csv")
    , CS.output = Just (SC.outputDirPath $ gqSamplesPrefix <> "_" <> show n <> ".csv")
    , CS.numChains = Just $ SC.smcNumChains smp
    , CS.numThreads = Just $ SC.smcNumThreads smp
    , CS.numSamples = SC.smcNumSamplesM smp
    , CS.numWarmup = SC.smcNumWarmupM smp
    , CS.adaptDelta = SC.smcAdaptDeltaM smp
    , CS.maxTreeDepth = SC.smcMaxTreeDepth smp
    }

inputsCacheTime :: forall r. (K.KnitEffects r)
               => SC.ModelRunnerConfig
               -> K.Sem r (K.ActionWithCacheTime r ())
inputsCacheTime config = K.wrapPrefix "modelCacheTime" $ do
  dataDep <- dataDependency $ SC.mrcInputNames config
  modelDep <- modelDependency $ SC.mrcInputNames config
  return $ (\_ _ -> ()) <$> modelDep <*> dataDep

data RScripts = None | ShinyStan [SR.UnwrapJSON] | Loo | Both [SR.UnwrapJSON] deriving (Show, Eq, Ord)

writeRScripts :: K.KnitEffects r => RScripts -> SC.ModelRunnerConfig -> K.Sem r ()
writeRScripts rScripts config = do
  let samplesKey = maybe SC.ModelSamples SC.GQSamples $ SC.rinGQ rin
  samplesPrefix <- SC.samplesPrefix (SC.mrcInputNames config) samplesKey
  let write mSuffix t =  writeFileText (SC.rDirPath $ samplesPrefix <> fromMaybe "" mSuffix <> ".R") t
      writeShiny ujs = write (Just "_shinystan") $ SR.shinyStanScript config ujs
      writeLoo = write Nothing $ SR.looScript config scriptPrefix 10
  case rScripts of
    None -> return ()
    ShinyStan ujs -> writeShiny ujs
    Loo -> writeLoo
    Both ujs -> writeShiny ujs >> writeLoo

wrangleDataWithoutPredictions :: forall st cd a b r.
  (K.KnitEffects r
  , K.CacheEffects st cd Text r
  , Monoid b
  )
  => SC.ModelRunnerConfig
  -> SC.DataWrangler a b ()
  -> SC.Cacheable st b
  -> K.ActionWithCacheTime r a
  -> K.Sem r (K.ActionWithCacheTime r (Either T.Text b))
wrangleDataWithoutPredictions config dw cb a = wrangleData @st @cd config dw cb a ()

wrangleData :: forall st cd md gq b p r.
  (K.KnitEffects r
  , K.CacheEffects st cd Text r
  )
  => SC.ModelRunnerConfig
  -> SC.DataWrangler md gq b p
  -> SC.Cacheable st b
  -> K.ActionWithCacheTime r md
  -> K.ActionWithCacheTime r gq
  -> p
  -> K.Sem r (K.ActionWithCacheTime r (Either T.Text b), K.ActionWithCacheTime r (Either T.Text b))
wrangleData config w {- (SC.Wrangle indexerType modelIndexAndEncoder mGQIndexAndEncoder) -} cb md_C gq_C p = do
  let (indexerType, modelIndexAndEncoder, mGQIndexAndEncoder) = case w of
        SC.Wrangle x y z -> (x, y, z)
        SC.WrangleWithPredictions x y z _ -> (x, y, z)
  curModelData_C <- modelDataDependency $ SC.mrcInputNames config
  (newModelData_C, modelIndexes_C) <- wranglerPrep config SC.ModelData indexerType modelIndexAndEncoder cb md_C
  modelJSON_C <- K.updateIf curModelData_C newModelData_C $ \e -> do
    let modelDataFileName = SC.modelDataFileName $ SC.mrcInputNames config
    K.logLE K.Diagnostic $ "existing model json (" <> modelDataFileName  <> ") appears older than cached data."
    jsonEncoding <- K.knitEither e
    K.liftKnit . BL.writeFile (SC.dataDirPath (SC.mrcInputNames config) modelDataFileName)
      $ A.encodingToLazyByteString $ A.pairs jsonEncoding
  model_C <- const <$> modelIndexes_C <*> modelJSON_C
  gq_C <- case mGQIndexAndEncoder of
    Nothing -> return $ pure $ Left "wrangleData: Attempt to use GQ indexes but No GQ wrangler given."
    Just gqIndexAndEncoder -> do
      mGQData_C <- SC.gqDataDependency $ SC.mrcInputNames config
      case curGQData_C of
        Nothing -> return $ pure $ Left "wrangleData: Attempt to wrangle GQ data but config.mrcInputNames.rinQG is Nothing."
        Just gqData_C -> do
          (newGQData_C, gqIndexes_C) <- wranglerPrep config SC.GQData indexerType gqIndexAndEncoder cb gq_C
          let gqJSONDeps = (,) <$> newGQData_C <*> gqIndexes_C
          gqJSON_C <- K.updateIf curGQData_C gqJSONDeps $ \(e, eb) -> do
            gqDataFileName <- K.knitMaybe "Attempt to build gq json but rinGQ is Nothing." $ SC.gqDataFileName $ SC.mrcInputNames config
            K.logLE K.Diagnostic $ "existing GQ json (" <> gqDataFileName  <> ") appears older than cached data."
            jsonEncoding <- K.knitEither e
            indexEncoding <- case w of
              SC.Wrangle _ _ _ -> return mempty
              SC.WrangleWithPredictions _ _ _ encodeToPredict -> K.knitEither $ encodeToPredict eb p
            K.liftKnit . BL.writeFile (SC.dataDirPath (SC.mrcInputNames config) gqDataFileName)
              $ A.encodingToLazyByteString $ A.pairs (jsonEncoding <> indexEncoding)
          return $ const <$> gqIndexes_C <*> gqJSON_C
  return (model_C, gq_C)

-- create function to rebuild json along with time stamp from data used
wranglerPrep :: forall st cd a b r.
  (
    K.KnitEffects r
  , K.CacheEffects st cd Text r
  )
  => SC.ModelRunnerConfig
  -> SC.InputDataType
  -> SC.DataIndexerType b
  -> SC.Wrangler a
  -> SC.Cacheable st b
  -> K.ActionWithCacheTime r a
  -> K.Sem r (K.ActionWithCacheTime r (), K.ActionWithCacheTime r (Either T.Text b))
wranglerPrep config inputDataType indexerType wrangler cb a_C = do
  let indexAndEncoder_C = fmap wrangler a_C
      eb_C = fmap fst indexAndEncoder_C
      encoder_C = fmap snd indexAndEncoder_C
  index_C <- manageIndex @st @cd config inputDataType indexerType cb eb_C
  let newJSON_C = encoder_C <*> a_C
  return (newJSON_C, index_C)

manageIndex :: forall st cd b r.
  (K.KnitEffects r
  , K.CacheEffects st cd Text r
  )
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
          curJSON_C <- K.fileDependency $ jsonFP config
          when (isNothing $ K.cacheTime curJSON_C) $ do
            K.logLE (K.Debug 1)  $ "JSON data (\"" <> T.pack (jsonFP config) <> "\") is missing.  Deleting cached indices to force rebuild."
            K.clearIfPresent @Text @cd (indexCacheKey config inputDataType)
          K.retrieveOrMake @st @cd (indexCacheKey config inputDataType) ebFromA_C return
        _ -> K.knitError "Cacheable index type provided but b is Uncacheable."
    _ -> return ebFromA_C

runModel :: forall st cd md gq b p c r.
  (K.KnitEffects r
  , K.CacheEffects st cd Text r
  )
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
  let modelNameS = toString $ SC.mrcModel config
      modelDirS = toString $ SC.mrcModelDir config
  K.logLE K.Info "running Model (if necessary)"
  modelSamplesFileNames <- SC.modelSamplesFileNames runnerInputNames
--  let outputFiles = toString <$> SC.stanOutputFiles config --fmap (SC.outputFile (SC.mrcOutputPrefix config)) [1 .. (SC.mrcNumChains config)]
  checkClangEnv
  checkDir (SC.mrcModelDir config) >>= K.knitMaybe "Model directory is missing!"
  createDirIfNecessary (SC.mrcModelDir config <> "/data") -- json inputs
  createDirIfNecessary (SC.mrcModelDir config <> "/output") -- csv model run output
  createDirIfNecessary (SC.mrcModelDir config <> "/R") -- scripts to load fit into R for shinyStan or loo.
  (modelIndices_C, gqIndices_C) <- wrangleData @st @cd config dataWrangler cb md_C gq_C toPredict
  curModel_C <- SC.modelDependency $ SC.mrcInputNames config
  (modelResDep, mGQResDep) <- do
    let runModelDeps = (,) <$> modelIndices_C <*> curModel_C -- indices carries data update time
        runModel = do
          let modelExeConfig = SC.mrcStanExeModelConfig config
          K.logLE K.Diagnostic $ "Running Model: " <> SC.rinModel (SC.mrcInputNames config)
          K.logLE K.Diagnostic $ "Command: " <> toText (CS.toStanExeCmdLine modelExeConfig)
          K.liftKnit $ CS.stan (SC.modelPath config) modelExeConfig
          K.logLE K.Diagnostic $ "Finished " <> SC.rinModel (SC.mrcInputNames config)
        runGQDeps = (,,) <$> modelIndices <*> gqIndices <*> curModel_C
        runOneGQ n = case SC.mrcStanExeGQConfigM config of
          Nothing -> K.knitError "Attempt to run Stan in GQ mode with no GQ setup (mrcStanExeGQConfigM is Nothing)"
          Just gqExeConfigF -> do
            gqName <- case SC.rinGQ (SC.mrcInputNames config) of
              Nothing -> K.knitError "runModel.runGQ: Trying to run GQ setup but GQ name not set (rinGQ is Nothing)"
              Just name -> return name
            K.logLE K.Diagnostic $ "Running GQ: " <> gqName
            K.logLE K.Diagnostic $ "Using results from model " <> SC.rinModel (SC.mrcInputNames config)
            K.logLE K.Diagnostic $ "Command: " <> toText (CS.toStanExeCmdLine $ gqExeConfigF n)
            K.liftKnit $ CS.stan (SC.modelPath config) (gqExeConfigF n)
            K.logLE K.Diagnostic $ "Finished GQ: " <> gqName
    modelSamplesFileNames <- K.ignoreCacheTimeM $ SC.modelSamplesFileNames config
    modelSamplesFilesDep <- K.oldestUnit <$> traverse K.fileDependency modelSamplesFileNames
    modelRes_C <- K.updateIf modelSamplesFilesDep runModelDeps $ \_ -> do
      K.logLE K.Diagnostic "Stan model outputs older than model input data or model code.  Rebuilding Stan exe and running."
      K.logLE (K.Debug 1) $ "Make CommandLine: " <> show (CS.makeConfigToCmdLine (SC.mrcStanMakeConfig config))
      K.liftKnit $ CS.make (SC.mrcStanMakeConfig config)
      res_C <- runModel
      when (SC.mrcRunDiagnose config) $ do
        K.logLE K.Info "Running stan diagnostics"
        K.liftKnit $ CS.diagnoseCSD modelSamplesFileNames
      return res_C
    mGQRes <- case SC.mrcStanExeGQConfigM config of
      Nothing -> return Nothing
      Just _ -> do
        gqSamplesFileNames <- K.ignoreCacheTimeM $ SC.gqSamplesFileNames config
        gqSamplesFileDep <- K.oldestUnit <$> traverse K.fileDependency gqSamplesFileNames
        K.updateIf gqSamplesFileDep runGQDeps $ \_ -> do
          K.logLE K.Diagnostic "Stan GQ outputs older than model input data, GQ input data or model code. Running GQ."
          mRes_C <- maybe Nothing (const $ Just ()) . sequence <$> K.sequenceConcurrently (fmap runOneGQ [1 .. (SC.smcNumChains $ SC.mrcStanMCParameters config)])
          K.ignoreCacheTime mRes_C >>= K.knitMaybe "There was an error running GQ for a chain."
{-          when (SC.mrcRunDiagnose config) $ do
            K.logLE K.Info "Running stan diagnostics (GQ)"
            K.liftKnit $ CS.diagnoseCSD gqSamplesFileNames
-}
          return $ Just mRes_C

    K.logLE K.Diagnostic "writing R scripts"
    K.liftKnit $ writeRScripts rScriptsToWrite config

    return (modelRes_C, mGQRes)
  let (outputDep, outputFileNames) = case mGQRes of
        Nothing -> (modelResDep, modelSamplesFileNames)
        Just gqResDep -> (const <$> gqResDep <*> modelResDep, gqSamplesFileNames)
  let makeSummaryFromCSVs = do
        K.logLE K.Diagnostic "Stan summary older than model output.  Re-summarizing."
        summary <- K.liftKnit $ CS.stansummary (SC.mrcStanSummaryConfig config {CS.sampleFiles = outputFileNames})
        P.embed $ A.encodeFile (toString $ SC.summaryFilePath config) summary
        return summary
      modelResultDeps = (\a b _ -> (a, b)) <$> md_C <*> modelIndices_C <*> modelResDep
      mGQResultDeps = case mGQResDep of
        Nothing -> Nothing
        Just gqResDep -> Just $ (\a b _ -> (a, b)) <$> gq_C <*> gqIndices_C <*> gqResDep
  case makeResult of
    SC.UseSummary f -> do
      K.logLE (K.Debug 1) $
        "Summary command: "
        <> show ((CS.cmdStanDir . SC.mrcStanMakeConfig $ config) ++ "/bin/stansummary")
        <> " "
        <> T.intercalate " " (fmap T.pack (CS.stansummaryConfigToCmdLine (SC.mrcStanSummaryConfig config)))
      summary_C <- K.loadOrMakeFile
                   (toString $ SC.summaryFilePath config)
                   ((K.knitEither =<<) . P.embed . Relude.firstF toText . A.eitherDecodeFileStrict . toString)
                   outputDep -- this only is here to carry the timing to compare the output file with
                   (const makeSummaryFromCSVs)
      summaryE <- K.ignoreCacheTime summary_C
      summary <- K.knitEither $ first toText $ summaryE
      when (SC.mrcLogSummary config) $ do
        K.logLE K.Info $ "Stan Summary:\n"
        Say.say $ toText (CS.unparsed summary)
      f summary toPredict modelResultDeps mGQResultDeps
    SC.UseSummaryGQ f -> do
      K.logLE (K.Debug 1) $
        "Summary command: "
        <> show ((CS.cmdStanDir . SC.mrcStanMakeConfig $ config) ++ "/bin/stansummary")
        <> " "
        <> T.intercalate " " (fmap T.pack (CS.stansummaryConfigToCmdLine (SC.mrcStanSummaryConfig config)))
      summary_C <- K.loadOrMakeFile
                   (toString $ SC.summaryFilePath config)
                   ((K.knitEither =<<) . P.embed . Relude.firstF toText . A.eitherDecodeFileStrict . toString)
                   outputDep -- this only is here to carry the timing to compare the output file with
                   (const makeSummaryFromCSVs)
      summaryE <- K.ignoreCacheTime summary_C
      summary <- K.knitEither $ first toText $ summaryE
      when (SC.mrcLogSummary config) $ do
        K.logLE K.Info $ "Stan Summary:\n"
        Say.say $ toText (CS.unparsed summary)
      case mGQResultDeps of
        Nothing -> K.knitError "UseSumamryGQ: No GQ results!"
        Just gqResultDeps ->  f summary toPredict gqResultDeps
    SC.SkipSummary f -> f toPredict resultDeps mGQResultDeps
    SC.DoNothing -> return ()

data StaleFiles = StaleData | StaleOutput | StaleSummary deriving (Show, Eq, Ord)

deleteStaleFiles :: SC.ModelRunnerConfig -> [StaleFiles] -> IO ()
deleteStaleFiles config staleFiles = do
   let
       modelDirS = toString $ SC.mrcModelDir config
       outputCSVPaths = SC.addDirFP (modelDirS ++ "/output") . toString <$> SC.stanOutputFiles config
       summaryFilePath = toString $ SC.summaryFilePath config
       dataPath = jsonFP config
       toDelete x = case x of
         StaleData -> [dataPath]
         StaleOutput -> outputCSVPaths
         StaleSummary -> [summaryFilePath]
       exists fp = Dir.doesFileExist fp >>= \x -> return $ if x then Just fp else Nothing
       filesToDelete = ordNub $ concat $ toDelete <$> staleFiles
   extantPaths <- catMaybes <$> traverse exists filesToDelete
   Say.say $ "Deleting output files: " <> T.intercalate "," (toText <$> extantPaths)
   traverse_ Dir.removeFile extantPaths


checkClangEnv :: (P.Member (P.Embed IO) r, K.LogWithPrefixesLE r) => K.Sem r ()
checkClangEnv = K.wrapPrefix "checkClangEnv" $ do
  clangBinDirM <- K.liftKnit $ Env.lookupEnv "CLANG_BINDIR"
  case clangBinDirM of
    Nothing -> K.logLE K.Diagnostic "CLANG_BINDIR not set. Using existing path for clang."
    Just clangBinDir -> do
      curPath <- K.liftKnit $ Env.getEnv "PATH"
      K.logLE K.Diagnostic $ "Current path: " <> show curPath <> ".  Adding " <> show clangBinDir <> " for llvm clang."
      K.liftKnit $ Env.setEnv "PATH" (clangBinDir ++ ":" ++ curPath)

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

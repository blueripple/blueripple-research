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
--  let datFileS = maybe (SC.defaultDatFile modelNameT) T.unpack datFileM
  stanMakeConfig' <- K.liftKnit $ CS.makeDefaultMakeConfig (T.unpack $ SC.addDirT modelDir modelName)
  let stanMakeConfig = stanMakeConfig' {CS.stancFlags = stancConfigM}
  modelFileDep <- SC.modelDependency runnerInputNames
  modelDataDep <- SC.modelDataDependency runnerInputNames
  let sampleConfigDeps = (,) <$> modelFileDep <*> modelDataDep
  modelSamplesFileNames <- SC.modelSamplesFileNames runnerInputNames
  let stanExeConfig = if (K.cacheTime modelSamplesFileNames > K.cacheTime sampleConfigDeps)
                      then gqExeConfig
                      else sampleExeConfig
  samplesPrefix <- SC.samplesPrefix
  let stanSamplesFiles = fmap (SC.sampleFile samplesPrefix) $ Just <$> [1 .. numChains]
  stanSummaryConfig <-
    K.liftKnit $
      CS.useCmdStanDirForStansummary (CS.makeDefaultSummaryConfig $ fmap (SC.addDirFP (modelDirS ++ "/output")) stanOutputFiles)
  return $
    SC.ModelRunnerConfig
      stanMakeConfig
      stanExeConfig
      stanSummaryConfig
      runnerInputNames
      stanMCParameters
      True
      True

sampleExeConfig :: K.KnitEffects r => SC.RunnerInputNames -> SC.StanMCParameters -> K.Sem r SC.StanExeConfig
sampleExeConfig rin smp =  do
  samplesPrefix <- SC.samplesPrefix rin
  return $ SC.SampleConfig
    $ (CS.makeDefaultSample (toString $  modelName rin) Nothing)
    { CS.inputData = Just (SC.dataDirPath $ SC.rinData rin)
    , CS.output = Just (SC.addDirFP (modelDirS ++ "/output") $ SC.samplesFile outputFilePrefix Nothing)
    , CS.numChains = Just $ SC.smcNumChains smp
    , CS.numThreads = Just numThreads
    , CS.numSamples = numSamplesM
    , CS.numWarmup = numWarmupM
    , CS.adaptDelta = adaptDeltaM
    , CS.maxTreeDepth = maxTreeDepthM
    }

gqExeConfig :: K.KnitEffects r => SC.RunnerInputNames -> SC.StanMCParameters -> K.Sem r SC.StanExeConfig
gqExeConfig = do
  return $ SC.GQConfig
    $ \n -> (CS.makeDefaultGenerateQuantities (T.unpack modelNameT) n)
    { CS.inputData = Just (SC.addDirFP (modelDirS ++ "/data") datFileS)
    , CS.output = Just (SC.addDirFP (modelDirS ++ "/output") $ SC.outputFile outputFilePrefix Nothing)
    , CS.numChains = Just numChains
    , CS.numThreads = Just numThreads
    , CS.numSamples = numSamplesM
    , CS.numWarmup = numWarmupM
    , CS.adaptDelta = adaptDeltaM
    , CS.maxTreeDepth = maxTreeDepthM
    }


modelCacheTime :: forall r. (K.KnitEffects r)
               => SC.ModelRunnerConfig -> K.Sem r (K.ActionWithCacheTime r ())
modelCacheTime config = K.wrapPrefix "modelCacheTime" $ do
  let modelFile = T.unpack $ SC.addDirT (SC.mrcModelDir config) $ SB.modelFile $ SC.mrcModel config
      dataFile = jsonFP config
  K.logLE K.Diagnostic $ "Building dependency info for model (" <> T.pack modelFile <> ") and data (" <> T.pack dataFile <> ")"
  modelFileDep <- K.fileDependency modelFile
  jsonDataDep <- K.fileDependency dataFile
  return $ (\_ _ -> ()) <$> modelFileDep <*> jsonDataDep

data RScripts = None | ShinyStan [SR.UnwrapJSON] | Loo | Both [SR.UnwrapJSON] deriving (Show, Eq, Ord)

writeRScripts :: RScripts -> SC.ModelRunnerConfig -> IO ()
writeRScripts rScripts config = do
  let modelDir = SC.mrcModelDir config
      scriptPrefix = SC.mrcOutputPrefix config
      write mSuffix t =  writeFileText (toString $ modelDir <> "/R/" <> scriptPrefix <> fromMaybe "" mSuffix <> ".R") t
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
--  , st b
  )
  => SC.ModelRunnerConfig
  -> SC.DataWrangler a b ()
  -> SC.Cacheable st b
  -> K.ActionWithCacheTime r a
  -> K.Sem r (K.ActionWithCacheTime r (Either T.Text b))
wrangleDataWithoutPredictions config dw cb a = wrangleData @st @cd config dw cb a ()

wrangleData :: forall st cd a b p r.
  (K.KnitEffects r
  , K.CacheEffects st cd Text r
--  , st b
  )
  => SC.ModelRunnerConfig
  -> SC.DataWrangler a b p
  -> SC.Cacheable st b
  -> K.ActionWithCacheTime r a
  -> p
  -> K.Sem r (K.ActionWithCacheTime r (Either T.Text b))
wrangleData config (SC.Wrangle indexType indexAndEncoder) cb cachedA _ = do
  let indexAndEncoder_C = fmap indexAndEncoder cachedA
      eb_C = fmap fst indexAndEncoder_C
      encoder_C = fmap snd indexAndEncoder_C
  index_C <- manageIndex @st @cd config indexType cb eb_C
  curJSON_C <- K.fileDependency $ jsonFP config
  let newJSON_C = encoder_C <*> cachedA
  updatedJSON_C <- K.updateIf curJSON_C newJSON_C $ \e -> do
    K.logLE K.Diagnostic $ "existing json (" <> T.pack (jsonFP config) <> ") appears older than cached data."
    jsonEncoding <- K.knitEither e
    K.liftKnit . BL.writeFile (jsonFP config) $ A.encodingToLazyByteString $ A.pairs jsonEncoding
  return $ const <$> index_C <*> updatedJSON_C -- if json is newer than index, use that time, esp when no index

wrangleData config (SC.WrangleWithPredictions indexType indexAndEncoder encodeToPredict) cb cachedA p = do
  let indexAndEncoder_C = fmap indexAndEncoder cachedA
      eb_C = fmap fst indexAndEncoder_C
      encoder_C = fmap snd indexAndEncoder_C
  index_C <- manageIndex @st @cd config indexType cb eb_C
  curJSON_C <- K.fileDependency $ jsonFP config
  let newJSON_C = encoder_C <*> cachedA
      jsonDeps = (,) <$> newJSON_C <*> index_C
  updatedJSON_C <- K.updateIf curJSON_C jsonDeps $ \(e, eb) -> do
    K.logLE K.Diagnostic $ "existing json (" <> show (jsonFP config) <> ") appears older than cached data."
--    b <- K.knitEither eb
    dataEncoding <- K.knitEither e
    toPredictEncoding <- K.knitEither $ encodeToPredict eb p
    K.liftKnit . BL.writeFile (jsonFP config) $ A.encodingToLazyByteString $ A.pairs (dataEncoding <> toPredictEncoding)
  return $ const <$> index_C <*> updatedJSON_C -- if json is newer than index, use that time, esp when no index

manageIndex :: forall st cd b r.
  (K.KnitEffects r
  , K.CacheEffects st cd Text r
--  , st b
  )
  => SC.ModelRunnerConfig
  -> SC.DataIndexerType b
  -> SC.Cacheable st b
  -> K.ActionWithCacheTime r (Either T.Text b)
  -> K.Sem r (K.ActionWithCacheTime r (Either T.Text b))
manageIndex config dataIndexer cb ebFromA_C = do
  case dataIndexer of
    SC.CacheableIndex indexCacheKey ->
      case cb of
        SC.Cacheable -> do
          curJSON_C <- K.fileDependency $ jsonFP config
          when (Maybe.isNothing $ K.cacheTime curJSON_C) $ do
            K.logLE (K.Debug 1)  $ "JSON data (\"" <> T.pack (jsonFP config) <> "\") is missing.  Deleting cached indices to force rebuild."
            K.clearIfPresent @Text @cd (indexCacheKey config)
          K.retrieveOrMake @st @cd (indexCacheKey config) ebFromA_C return
        _ -> K.knitError "Cacheable index type provided but b is Uncacheable."
    _ -> return ebFromA_C

jsonFP :: SC.ModelRunnerConfig -> FilePath
jsonFP config = SC.addDirFP (T.unpack (SC.mrcModelDir config) ++ "/data") $ T.unpack $ SC.mrcDatFile config



runModel :: forall st cd a b p c r.
  (K.KnitEffects r
  , K.CacheEffects st cd Text r
  )
  => SC.ModelRunnerConfig
  -> RScripts
  -> SC.DataWrangler a b p
  -> SC.Cacheable st b
  -> SC.ResultAction r a b p c
  -> p
  -> K.ActionWithCacheTime r a
  -> K.Sem r c
runModel config rScriptsToWrite dataWrangler cb makeResult toPredict cachedA = K.wrapPrefix "Stan.ModelRunner.runModel" $ do
  let modelNameS = toString $ SC.mrcModel config
      modelDirS = toString $ SC.mrcModelDir config
  K.logLE K.Info "running Model"
  let outputFiles = toString <$> SC.stanOutputFiles config --fmap (SC.outputFile (SC.mrcOutputPrefix config)) [1 .. (SC.mrcNumChains config)]
  checkClangEnv
  checkDir (SC.mrcModelDir config) >>= K.knitMaybe "Model directory is missing!"
  createDirIfNecessary (SC.mrcModelDir config <> "/data") -- json inputs
  createDirIfNecessary (SC.mrcModelDir config <> "/output") -- csv model run output
  createDirIfNecessary (SC.mrcModelDir config <> "/R") -- scripts to load fit into R for shinyStan or loo.
  indices_C <- wrangleData @st @cd config dataWrangler cb cachedA toPredict
  curModel_C <- K.fileDependency (SC.addDirFP modelDirS $ toString $ SB.modelFile $ SC.mrcModel config)
  stanOutput_C <- do
    curStanOutputs_C <- K.oldestUnit <$> traverse (K.fileDependency . SC.addDirFP (modelDirS ++ "/output")) outputFiles
    let runStanDeps = (,) <$> indices_C <*> curModel_C -- indices_C carries input data update time
        runChains = do
          let exeConfig = SC.mrcStanExeConfig config
          K.logLE K.Diagnostic $ "Running " <> toText modelNameS
          K.logLE K.Diagnostic $ "Command: " <> toText (CS.toStanExeCmdLine exeConfig)
          K.liftKnit $ CS.stan (SC.addDirFP modelDirS modelNameS) exeConfig
          K.logLE K.Diagnostic $ "Finished " <> toText modelNameS
    res_C <- K.updateIf curStanOutputs_C runStanDeps $ \_ -> do
      K.logLE K.Diagnostic "Stan outputs older than input data or model.  Rebuilding Stan exe and running."
      K.logLE (K.Debug 1) $ "Make CommandLine: " <> show (CS.makeConfigToCmdLine (SC.mrcStanMakeConfig config))
      K.liftKnit $ CS.make (SC.mrcStanMakeConfig config)
      runChains
--      maybe Nothing (const $ Just ()) . sequence <$> K.sequenceConcurrently (fmap runOneChain [1 .. (SC.mrcNumChains config)])
--    K.ignoreCacheTime res_C >>= K.knitMaybe "There was an error running an MCMC chain."
    K.logLE K.Diagnostic "writing R scripts"
    K.liftKnit $ writeRScripts rScriptsToWrite config
    when (SC.mrcRunDiagnose config) $ do
      K.logLE K.Info "Running stan diagnostics"
      K.liftKnit $ CS.diagnoseCSD $ fmap (SC.addDirFP (modelDirS ++ "/output")) outputFiles
    return res_C
  let resultDeps = (\a b _ -> (a, b)) <$> cachedA <*> indices_C <*> stanOutput_C
      makeSummaryFromCSVs = do
        K.logLE K.Diagnostic "Stan summary older than model output.  Re-summarizing."
        summary <- K.liftKnit $ CS.stansummary (SC.mrcStanSummaryConfig config)
        P.embed $ A.encodeFile (toString $ SC.summaryFilePath config) summary
        return summary
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
                   stanOutput_C -- this only is here to carry the timing to compare the output file with
                   (const $ makeSummaryFromCSVs)
      summaryE <- K.ignoreCacheTime summary_C
      summary <- K.knitEither $ first toText $ summaryE
      when (SC.mrcLogSummary config) $ do
        K.logLE K.Info $ "Stan Summary:\n"
        Say.say $ toText (CS.unparsed summary)
      f summary toPredict resultDeps
    SC.SkipSummary f -> f toPredict resultDeps
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

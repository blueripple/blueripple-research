{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeApplications #-}
module Stan.ModelConfig where

import qualified CmdStan as CS
import qualified CmdStan.Types as CS
import qualified Knit.Report as K
import qualified Knit.Effect.Serialize as K
import qualified Data.Aeson.Encoding as A
import qualified Data.Map as M
import qualified Data.Text as T

data SamplesKey = ModelSamples | GQSamples Text deriving (Show, Ord, Eq)

type SamplesPrefixMap = Map SamplesKey Text

data ModelRunnerConfig = ModelRunnerConfig
  { mrcStanMakeConfig :: CS.MakeConfig
  , mrcStanExeConfig :: CS.StanExeConfig
  , mrcStanSummaryConfig :: CS.StansummaryConfig
  , mrcModelDir :: T.Text
  , mrcModelName :: T.Text
  , mrcGQName :: Maybe T.Text -- provide if there will be runs differing only in GQ
  , mrcDataName :: T.Text
--  , mrcOutputPrefix :: SampleFiles
  , mrcNumChains :: Int
  , mrcNumThreads :: Int
  , mrcAdaptDelta :: Maybe Double
  , mrcMaxTreeDepth :: Maybe Int
  , mrcLogSummary :: Bool
  , mrcRunDiagnose :: Bool
  }

samplesPrefixCacheKey :: ModelRunnerConfig -> Text
samplesPrefixCacheKey config = mrcModelDir config <> "/" <> mrcModelName config <> "/" <> mrcDataName config

modelFileName :: ModelRunnerConfig -> Text
modelFileName config = mrcModelName config <> ".stan"

addModelDirectory :: ModelRunnerConfig -> Text -> Text
addModelDirectory config x = mrcModelDir config <> "/" <> x

modelDependency :: forall st cd r. (K.KnitEffects r) => ModelRunnerConfig -> K.Sem r (K.ActionWithCacheTime r ())
modelDependency config = K.fileDependency (toString modelFile)  where
  modelFile = addModelDirectory config (modelFileName config)

modelDataFileName :: ModelRunnerConfig -> Text
modelDataFileName config = mrcDataName config <> ".json"

gqDataFileName :: ModelRunnerConfig -> Maybe Text
gqDataFileName config = fmap (<> ".json") $ mrcGQName config

dataDependency :: forall st cd r. (K.KnitEffects r) => ModelRunnerConfig -> K.Sem r (K.ActionWithCacheTime r ())
dataDependency config = do
  modelDataDep <- K.fileDependency $ (toString $ addModelDirectory config $ ("data/" <> modelDataFileName config))
  case gqDataFileName config of
    Nothing -> return modelDataDep
    Just fn -> do
      gqDataDep <- K.fileDependency $ (toString $ addModelDirectory config fn)
      return $ const <$> modelDataDep <*> gqDataDep

-- if the cached map is outdated, return an empty one
samplesPrefixCache :: forall st cd r. (K.KnitEffects r, K.CacheEffects st cd Text r, st SamplesPrefixMap)
                => ModelRunnerConfig -> K.Sem r (K.ActionWithCacheTime r SamplesPrefixMap)
samplesPrefixCache config = do
  dataDep <- dataDependency config
  modelDep <- modelDependency config
  let deps = const <$> dataDep <*> modelDep
  K.retrieveOrMake @st @cd (samplesPrefixCacheKey config) deps $ const $ return mempty

samplesPrefix :: ModelRunnerConfig -> Text
samplesPrefix config =
  mrcModelName config
  <> "_" <> mrcDataName config
  <> fromMaybe "" (("_" <>) <$> mrcGQName config)

-- This is not atomic so care should be used that only one thread uses it at a time.
-- I should fix this in knit-haskell where I could provide an atomic update.
samplesFileNames ::  forall st cd r. (K.KnitEffects r, K.CacheEffects st cd Text r, st SamplesPrefixMap)
                 => ModelRunnerConfig -> SamplesKey -> K.Sem r (K.ActionWithCacheTime r [Text])
samplesFileNames config key = do
  samplePrefixCache_C <- samplesPrefixCache @st @cd config
  samplePrefixCache <- K.ignoreCacheTime samplePrefixCache_C
  case M.lookup key samplePrefixCache of
    Nothing -> do -- missing so use prefix from current setup
      let prefix = samplesPrefix config
      K.store @st @cd (samplesPrefixCacheKey config) $ M.insert key prefix samplePrefixCache
      let files = addModelDirectory config . (\n -> "output/" <> prefix <> "_" <> show n <> ".csv") <$> [1..mrcNumChains config]
      return $ const files <$> samplePrefixCache_C
    Just prefix -> do -- exists, so use those files
      let files = addModelDirectory config . (\n -> "output/" <> prefix <> "_" <> show n <> ".csv") <$> [1..mrcNumChains config]
      return $ const files <$> samplePrefixCache_C

-- This is not atomic so care should be used that only one thread uses it at a time.
-- I should fix this in knit-haskell where I could provide an atomic update.
modelSamplesFileNames :: forall st cd r. (K.KnitEffects r, K.CacheEffects st cd Text r, st SamplesPrefixMap)
                      => ModelRunnerConfig -> K.Sem r (K.ActionWithCacheTime r [Text])
modelSamplesFileNames config = samplesFileNames @st @cd config ModelSamples

-- This is not atomic so care should be used that only one thread uses it at a time.
-- I should fix this in knit-haskell where I could provide an atomic update.
gqSamplesFileNames :: forall st cd r. (K.KnitEffects r, K.CacheEffects st cd Text r, st SamplesPrefixMap)
                   => ModelRunnerConfig -> K.Sem r (K.ActionWithCacheTime r [Text])
gqSamplesFileNames config = do
  case mrcGQName config of
    Nothing -> return $ pure []
    Just gqName -> samplesFileNames @st @cd config (GQSamples gqName)



setSigFigs :: Int -> ModelRunnerConfig -> ModelRunnerConfig
setSigFigs sf mrc = let sc = mrcStanSummaryConfig mrc in mrc { mrcStanSummaryConfig = sc { CS.sigFigs = Just sf } }

noLogOfSummary :: ModelRunnerConfig -> ModelRunnerConfig
noLogOfSummary sc = sc { mrcLogSummary = False }

noDiagnose :: ModelRunnerConfig -> ModelRunnerConfig
noDiagnose sc = sc { mrcRunDiagnose = False }

-- produce indexes and json producer from the data as well as a data-set to predict.
data DataIndexerType (b :: Type) where
  NoIndex :: DataIndexerType ()
  TransientIndex :: DataIndexerType b
  CacheableIndex :: (ModelRunnerConfig -> T.Text) -> DataIndexerType b

-- pattern matching on the first brings the constraint into scope
-- This allows us to choose to not have the constraint unless we need it.
data Cacheable st b where
  Cacheable :: st (Either Text b) => Cacheable st b
  UnCacheable :: Cacheable st b

data DataWrangler a b p where
  Wrangle :: DataIndexerType b
          -> (a -> (Either T.Text b, a -> Either T.Text A.Series))
          -> DataWrangler a b ()
  WrangleWithPredictions :: DataIndexerType b
                         -> (a -> (Either T.Text b, a -> Either T.Text A.Series))
                         -> (Either T.Text b -> p -> Either T.Text A.Series)
                         -> DataWrangler a b p

noPredictions :: DataWrangler a b p -> DataWrangler a b ()
noPredictions w@(Wrangle _ _) = w
noPredictions (WrangleWithPredictions x y _) = Wrangle x y


dataIndexerType :: DataWrangler a b p -> DataIndexerType b
dataIndexerType (Wrangle i _) = i
dataIndexerType (WrangleWithPredictions i _ _) = i

indexerAndEncoder :: DataWrangler a b p -> a -> (Either T.Text b, a -> Either T.Text A.Series)
indexerAndEncoder (Wrangle _ x) = x
indexerAndEncoder (WrangleWithPredictions _ x _) = x

-- produce a result of type b from the data and the model summary
-- NB: the cache time will give you newest of data, indices and stan output
--type ResultAction r a b c = CS.StanSummary -> K.ActionWithCacheTime r (a, b) -> K.Sem r c

data ResultAction r a b p c where
  UseSummary :: (CS.StanSummary -> p -> K.ActionWithCacheTime r (a, Either T.Text b) -> K.Sem r c) -> ResultAction r a b p c
  SkipSummary :: (p -> K.ActionWithCacheTime r (a, Either T.Text b) -> K.Sem r c) -> ResultAction r a b p c
  DoNothing :: ResultAction r a b c ()

emptyResult :: ResultAction r a b p ()
emptyResult = SkipSummary $ \_ _ -> return ()

addDirT :: T.Text -> T.Text -> T.Text
addDirT dir fp = dir <> "/" <> fp

addDirFP :: FilePath -> FilePath -> FilePath
addDirFP dir fp = dir ++ "/" ++ fp

defaultDatFile :: T.Text -> FilePath
defaultDatFile modelNameT = toString modelNameT ++ ".json"

sampleFile :: T.Text -> Maybe Int -> FilePath
sampleFile outputFilePrefix chainIndexM = toString outputFilePrefix <> (maybe "" (("_" <>) . show) chainIndexM) <> ".csv"

--stanOutputFiles :: ModelRunnerConfig -> [T.Text]
--stanOutputFiles config = fmap (toText . outputFile (outputPrefix config)) $ Just <$> [1..(mrcNumChains config)]

summaryFileName :: ModelRunnerConfig -> T.Text
summaryFileName config = samplesPrefix config <> ".json"

summaryFilePath :: ModelRunnerConfig -> T.Text
summaryFilePath config = mrcModelDir config <> "/output/" <> summaryFileName config

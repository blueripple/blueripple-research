{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Stan.ModelConfig where

import qualified CmdStan as CS
import qualified CmdStan.Types as CS
import qualified Knit.Report as K
import qualified Data.Serialize as Cereal
import qualified Data.Aeson as A
import qualified Data.Aeson.Encoding as A
import qualified Data.Map as M
import qualified Data.Text as T

data GQNames = GQNames { gqModelName :: Text, gqDataName :: Text} deriving (Show, Eq, Ord)

data RunnerInputNames = RunnerInputNames
  { rinModelDir :: Text
  , rinModel :: Text
  , rinGQ :: Maybe GQNames
  , rinData :: Text
  }  deriving (Show, Ord, Eq)

data ModelRun = MRNoGQ | MROnlyLL | MRFull deriving (Show, Eq)

-- for merged samples
llSuffix :: Text
llSuffix = "_LL"

modelSuffix :: ModelRun -> RunnerInputNames -> Text
modelSuffix MRNoGQ _ = "_noGQ"
modelSuffix MROnlyLL _ = "_onlyLL"
modelSuffix MRFull rin = "_" <> fromMaybe "GQ" (gqModelName <$> rinGQ rin)
{-# INLINEABLE modelSuffix #-}

unmergedSamplesSuffix :: ModelRun -> RunnerInputNames -> Text
unmergedSamplesSuffix mr rin = modelSuffix mr rin
{-# INLINEABLE unmergedSamplesSuffix #-}

mergedSamplesSuffix :: ModelRun -> Text
mergedSamplesSuffix MRNoGQ = "_noGQ"
mergedSamplesSuffix MROnlyLL = "_ll"
mergedSamplesSuffix MRFull = ""
{-# INLINEABLE mergedSamplesSuffix #-}

modelName :: ModelRun -> RunnerInputNames -> Text
modelName mr rin = rinModel rin <> modelSuffix mr rin
{-# INLINEABLE modelName #-}

modelDirPath :: RunnerInputNames -> Text -> FilePath
modelDirPath rin fName = toString $ rinModelDir rin <> "/" <> fName

modelPath :: ModelRun -> RunnerInputNames -> FilePath
modelPath mr rin = modelDirPath rin $ modelName mr rin
{-# INLINEABLE modelPath #-}

dirPath :: RunnerInputNames -> Text -> Text -> FilePath
dirPath rin subDirName fName = toString $ rinModelDir rin <> "/" <> subDirName <> "/" <> fName

outputDirPath :: RunnerInputNames -> Text -> FilePath
outputDirPath rin = dirPath rin "output"

dataDirPath :: RunnerInputNames -> Text -> FilePath
dataDirPath rin = dirPath rin "data"

rDirPath :: RunnerInputNames -> Text -> FilePath
rDirPath rin = dirPath rin "R"

data StanMCParameters = StanMCParameters
  { smcNumChains :: Int
  , smcNumThreads :: Int
  , smcNumWarmupM :: Maybe Int
  , smcNumSamplesM :: Maybe Int
  , smcAdaptDeltaM :: Maybe Double
  , smcMaxTreeDepth :: Maybe Int
  , smcRandomSeed :: Maybe Int
  } deriving (Show, Eq, Ord)

data StanExeConfigWrapper = MultiThreadedExeConfig CS.StanExeConfig |  SingleThreadedExeConfig (Int -> CS.StanExeConfig)

data ModelRunnerConfig = ModelRunnerConfig
  { mrcDoOnlyLL :: Bool
  , mrcStanMakeConfig :: ModelRun -> CS.MakeConfig
  , mrcStanSummaryConfig :: CS.StansummaryConfig
  , mrcInputNames :: RunnerInputNames
  , mrcStanMCParameters :: StanMCParameters
  , mrcLogSummary :: Bool
  , mrcRunDiagnose :: Bool
  }

mrcModelDir :: ModelRunnerConfig -> Text
mrcModelDir = rinModelDir . mrcInputNames

modelFileName :: ModelRun -> RunnerInputNames -> Text
modelFileName mr rin = modelName mr rin <> ".stan"

addModelDirectory :: RunnerInputNames -> Text -> Text
addModelDirectory rin x = rinModelDir rin <> "/" <> x

modelDependency :: K.KnitEffects r => ModelRun -> RunnerInputNames -> K.Sem r (K.ActionWithCacheTime r ())
modelDependency mr rin = K.fileDependency (toString modelFile)  where
  modelFile = addModelDirectory rin (modelFileName mr rin)

modelDataFileName :: RunnerInputNames -> Text
modelDataFileName rin = rinData rin <> ".json"

gqDataFileName :: RunnerInputNames -> Maybe Text
gqDataFileName rin = fmap (<> ".json") $ (gqDataName <$> rinGQ rin)

modelDataDependency :: K.KnitEffects r => RunnerInputNames -> K.Sem r (K.ActionWithCacheTime r ())
modelDataDependency rin = K.fileDependency $ (toString $ addModelDirectory rin $ ("data/" <> modelDataFileName rin))

gqDataDependency :: K.KnitEffects r => RunnerInputNames -> K.Sem r (Maybe (K.ActionWithCacheTime r ()))
gqDataDependency rin = case gqDataFileName rin of
  Nothing -> return Nothing
  Just gqName -> do
    dep <- K.fileDependency $ (toString $ addModelDirectory rin $ ("data/" <> gqName))
    return $ Just dep

combinedDataFileName :: RunnerInputNames -> Text
combinedDataFileName rin = rinData rin <> maybe "" ("_" <>) (gqDataName <$> rinGQ rin) <> ".json"

combineData :: K.KnitEffects r => RunnerInputNames -> K.Sem r (K.ActionWithCacheTime r ())
combineData rin = do
  modelDataDep <- modelDataDependency rin
  gqDataDependencyM <- gqDataDependency rin
  case gqDataFileName rin of
    Nothing -> return modelDataDep
    Just gqName -> do
      let gqFP = dataDirPath rin gqName
      gqDep <- K.fileDependency $ toString $ gqFP
      let comboDeps = (,) <$> modelDataDep <*> gqDep
          comboFP = dataDirPath rin $ combinedDataFileName rin
      comboFileDep <- K.fileDependency comboFP
      K.updateIf comboFileDep comboDeps $ const $ do
          modelDataE <- K.liftKnit $ A.eitherDecodeFileStrict $ dataDirPath rin $ modelDataFileName rin
          modelData <- K.knitEither $ first toText modelDataE
          gqDataE <- K.liftKnit $ A.eitherDecodeFileStrict $ toString gqFP
          gqData <- K.knitEither $ first toText gqDataE
          let combined :: A.Object = modelData <> gqData
          K.liftKnit $ A.encodeFile comboFP combined
          return ()

dataDependency :: K.KnitEffects r => RunnerInputNames -> K.Sem r (K.ActionWithCacheTime r ())
dataDependency rin = do
  modelDataDep <- modelDataDependency rin
  gqDataDepM <- gqDataDependency rin
  case gqDataDepM of
    Nothing -> return modelDataDep
    Just gqDataDep -> return $ const <$> modelDataDep <*> gqDataDep

type KnitStan st cd r = (K.KnitEffects r, K.CacheEffects st cd Text r)

outputPrefix :: ModelRun -> RunnerInputNames -> Text
outputPrefix mr rin = rinModel rin <> "_" <> rinData rin <> gqPart <> unmergedSamplesSuffix mr rin where
  gqName (GQNames mn dn) = mn <> "_" <> dn
  gqPart = if mr == MRFull then maybe "" ("_" <>) $ (gqName <$> rinGQ rin) else ""

mergedPrefix :: ModelRun -> RunnerInputNames -> Text
mergedPrefix mr rin = rinModel rin <> "_" <> rinData rin <> gqPart <> mergedSamplesSuffix mr where
  gqName (GQNames mn dn) = mn <> "_" <> dn
  gqPart = if mr == MRFull then maybe "" ("_" <>) $ (gqName <$> rinGQ rin) else ""

samplesFileNames :: ModelRun -> ModelRunnerConfig -> [FilePath]
samplesFileNames mr config =
  let rin = mrcInputNames config
      numChains = smcNumChains $ mrcStanMCParameters config
  in outputDirPath rin . (\n -> outputPrefix mr rin <> "_" <> show n <> ".csv") <$> [1..numChains]

mergedSamplesFP :: ModelRun -> ModelRunnerConfig -> Int -> FilePath
mergedSamplesFP MRNoGQ _ _ = error "mergedFP: called with MRNoGQ argument!"
mergedSamplesFP mr config n = outputDirPath (mrcInputNames config) $ mergedPrefix mr (mrcInputNames config) <> "_" <> show n <> ".csv"

finalSamplesFileNames :: ModelRun -> ModelRunnerConfig -> [FilePath]
finalSamplesFileNames MRNoGQ config = samplesFileNames MRNoGQ config
finalSamplesFileNames mr config = fmap (mergedSamplesFP mr config) $ [1..(smcNumChains $ mrcStanMCParameters config)]

setSigFigs :: Int -> ModelRunnerConfig -> ModelRunnerConfig
setSigFigs sf mrc = let sc = mrcStanSummaryConfig mrc in mrc { mrcStanSummaryConfig = sc { CS.sigFigs = Just sf } }

noLogOfSummary :: ModelRunnerConfig -> ModelRunnerConfig
noLogOfSummary sc = sc { mrcLogSummary = False }

noDiagnose :: ModelRunnerConfig -> ModelRunnerConfig
noDiagnose sc = sc { mrcRunDiagnose = False }

data InputDataType = ModelData | GQData deriving (Show, Eq, Ord, Enum, Bounded, Generic)
instance Hashable InputDataType

-- produce indexes and json producer from the data as well as a data-set to predict.
data DataIndexerType (b :: Type) where
  NoIndex :: DataIndexerType ()
  TransientIndex :: DataIndexerType b
  CacheableIndex :: (ModelRunnerConfig -> InputDataType -> Text) -> DataIndexerType b

-- pattern matching on the first brings the constraint into scope
-- This allows us to choose to not have the constraint unless we need it.
data Cacheable st b where
  Cacheable :: st (Either Text b) => Cacheable st b
  UnCacheable :: Cacheable st b

data JSONSeries = JSONSeries { modelSeries :: A.Series, gqSeries :: A.Series}

type Wrangler a b = a -> (Either T.Text b, a -> Either T.Text A.Series)

unitWrangle :: Wrangler () b
unitWrangle _ = (Left "Wrangle Error. Attempt to build index using a \"Wrangle () _\""
                , const $ Left "Wrangle Error. Attempt to build json using a \"Wrangle () _\""
                )

data DataWrangler md gq b p where
  Wrangle :: DataIndexerType b
          -> Wrangler md b
          -> Maybe (Wrangler gq b)
          -> DataWrangler md gq b ()
  WrangleWithPredictions :: DataIndexerType b
                         -> Wrangler md b
                         -> Maybe (Wrangler gq b)
                         -> (Either T.Text b -> p -> Either T.Text A.Series)
                         -> DataWrangler md gq b p

noPredictions :: DataWrangler md gq b p -> DataWrangler md gq b ()
noPredictions w@(Wrangle _ _ _) = w
noPredictions (WrangleWithPredictions x y z _) = Wrangle x y z

dataIndexerType :: DataWrangler md gq b p -> DataIndexerType b
dataIndexerType (Wrangle i _ _) = i
dataIndexerType (WrangleWithPredictions i _ _ _) = i

modelWrangler :: DataWrangler md gq b p -> Wrangler md b -- -> (Either T.Text b, a -> Either T.Text JSONSeries)
modelWrangler (Wrangle _ x _) = x
modelWrangler (WrangleWithPredictions _ x _ _) = x

mGQWrangler :: DataWrangler md gq b p -> Maybe (Wrangler gq b)  -- -> (Either T.Text b, a -> Either T.Text JSONSeries)
mGQWrangler (Wrangle _ _ x) = x
mGQWrangler (WrangleWithPredictions _ _ x _) = x

-- produce a result of type b from the data and the model summary
-- NB: the cache time will give you newest of data, indices and stan output
type ResultF r md gq b p c
  = p
    -> K.ActionWithCacheTime r (md, Either T.Text b)
    -> Maybe (K.ActionWithCacheTime r (gq, Either T.Text b))
    -> K.Sem r c

data ResultAction r md gq b p c where
  UseSummary :: (CS.StanSummary -> ResultF r md gq b p c) -> ResultAction r md gq b p c
  SkipSummary :: ResultF r md gq b p c -> ResultAction r md gq b p c
  DoNothing :: ResultAction r md gq b p ()

emptyResult :: ResultAction r md gq b p ()
emptyResult = SkipSummary $ \_ _ _ -> return ()

sampleFile :: T.Text -> Maybe Int -> FilePath
sampleFile outputFilePrefix chainIndexM = toString outputFilePrefix <> (maybe "" (("_" <>) . show) chainIndexM) <> ".csv"

summaryFileName :: ModelRun -> ModelRunnerConfig -> T.Text
summaryFileName mr config = outputPrefix mr (mrcInputNames config) <> "_summary.json"
{-
gqSummaryFileName :: ModelRunnerConfig -> Maybe T.Text
gqSummaryFileName config = fmap (<> "_summary.json") $ gqPrefix (mrcInputNames config)
-}

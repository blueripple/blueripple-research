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
import qualified Data.Constraint as Constraint
import qualified Knit.Report as K
import qualified Knit.Effect.Serialize            as K
import qualified Data.Aeson.Encoding as A
import qualified Data.Text as T


data ModelRunnerConfig = ModelRunnerConfig
  { mrcStanMakeConfig :: CS.MakeConfig
  , mrcStanExeConfigF :: Int -> CS.StanExeConfig
  , mrcStanSummaryConfig :: CS.StansummaryConfig
  , mrcModelDir :: T.Text
  , mrcModel :: T.Text
  , mrcDatFile :: T.Text
  , mrcOutputPrefix :: T.Text
  , mrcNumChains :: Int
  , mrcAdaptDelta :: Maybe Double
  , mrcLogSummary :: Bool
  , mrcRunDiagnose :: Bool
  }

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
  Cacheable :: st b => Cacheable st b
  UnCacheable :: Cacheable st b

data DataWrangler a b p where
  Wrangle :: DataIndexerType b -> (a -> (b, a -> Either T.Text A.Series)) -> DataWrangler a b ()
  WrangleWithPredictions :: DataIndexerType b -> (a -> (b, a -> Either T.Text A.Series)) -> (b -> p -> Either T.Text A.Series) -> DataWrangler a b p

noPredictions :: DataWrangler a b p -> DataWrangler a b ()
noPredictions w@(Wrangle _ _) = w
noPredictions (WrangleWithPredictions x y _) = Wrangle x y


dataIndexerType :: DataWrangler a b p -> DataIndexerType b
dataIndexerType (Wrangle i _) = i
dataIndexerType (WrangleWithPredictions i _ _) = i

indexerAndEncoder :: DataWrangler a b p -> a -> (b, a -> Either T.Text A.Series)
indexerAndEncoder (Wrangle _ x) = x
indexerAndEncoder (WrangleWithPredictions _ x _) = x

-- produce a result of type b from the data and the model summary
-- NB: the cache time will give you newest of data, indices and stan output
--type ResultAction r a b c = CS.StanSummary -> K.ActionWithCacheTime r (a, b) -> K.Sem r c

data ResultAction r a b p c where
  UseSummary :: (CS.StanSummary -> p -> K.ActionWithCacheTime r (a, b) -> K.Sem r c) -> ResultAction r a b p c
  SkipSummary :: (p -> K.ActionWithCacheTime r (a, b) -> K.Sem r c) -> ResultAction r a b p c
  DoNothing :: ResultAction r a b c ()

emptyResult :: ResultAction r a b p ()
emptyResult = SkipSummary $ \_ _ -> return ()

addDirT :: T.Text -> T.Text -> T.Text
addDirT dir fp = dir <> "/" <> fp

addDirFP :: FilePath -> FilePath -> FilePath
addDirFP dir fp = dir ++ "/" ++ fp

defaultDatFile :: T.Text -> FilePath
defaultDatFile modelNameT = toString modelNameT ++ ".json"

outputFile :: T.Text -> Int -> FilePath
outputFile outputFilePrefix chainIndex = toString outputFilePrefix <> "_" <> show chainIndex <> ".csv"

stanOutputFiles :: ModelRunnerConfig -> [T.Text]
stanOutputFiles config = fmap (toText . outputFile (mrcOutputPrefix config)) [1..(mrcNumChains config)]

summaryFileName :: ModelRunnerConfig -> T.Text
summaryFileName config = mrcOutputPrefix config <> ".json"

summaryFilePath :: ModelRunnerConfig -> T.Text
summaryFilePath config = mrcModelDir config <> "/output/" <> summaryFileName config

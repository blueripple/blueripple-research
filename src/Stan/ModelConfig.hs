{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeApplications #-}
module Stan.ModelConfig where

import qualified CmdStan as CS
import qualified CmdStan.Types as CS

import qualified Knit.Report as K
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
  , mrcLogSummary :: Bool
  }

-- produce indexes and json producer from the data 
type DataWrangler a b = a -> (b, a -> Either T.Text A.Encoding)

-- produce a result of type b from the data and the model summary
-- NB: the cache time will give you newest of data, indices and stan output
type ResultAction r a b c = CS.StanSummary -> K.ActionWithCacheTime r (a, b) -> K.Sem r c

addDirT :: T.Text -> T.Text -> T.Text
addDirT dir fp = dir <> "/" <> fp

addDirFP :: FilePath -> FilePath -> FilePath
addDirFP dir fp = dir ++ "/" ++ fp

defaultDatFile :: T.Text -> FilePath
defaultDatFile modelNameT = (T.unpack modelNameT) ++ ".json"      

outputFile :: T.Text -> Int -> FilePath
outputFile outputFilePrefix chainIndex = (T.unpack outputFilePrefix ++ "_" ++ show chainIndex ++ ".csv")

stanOutputFiles :: ModelRunnerConfig -> [T.Text]
stanOutputFiles config = fmap (\n -> T.pack $ outputFile (mrcOutputPrefix config) n) [1..(mrcNumChains config)]

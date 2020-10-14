module Stan.ModelRunner where

import qualified CmdStan as CS
import qualified CmdStan.Types as CS

data ModelRunnerConfig = ModelRunnerConfig
  { mrcCSMakeConfig :: CS.MakeConfig
  , mrcStanExeConfigF :: Int -> CS.StanExeConfig
  , mrcStanSummaryConfig :: CS.StanSummaryConfig
  , mrcModelDir :: T.Text
  , mrcModel :: T.Text
  , mrcNumChains :: Int
  }

makeDefaultModelRunnerConfig :: T.Text -> T.Text -> Int -> Maybe Int -> Maybe Int -> K.Sem r ModelRunnerConfig
makeDefaultModelRunnerConfig modelDir modelName numChains numWarmupM numSamplesM = do
  let addDir d fp = d ++ "/" ++ fp
      datFile = modelName ++ "_dat.json"      
      outputFile n =  model ++ "_" ++ show n ++ ".csv"
  stanMakeConfig <- K.liftKnit $ CS.makeDefaultMakeConfig (addDir modelDir modelName)
  let stanExeConfigF chainIndex = (CS.makeDefaultSample model chainIndex) { CS.inputData = Just (addDir modelDir datFile)
                                                                         , CS.output = Just (addDir modelDir $ outputFile chainIndex) 
                                                                         , CS.numSamples = numSamplesM
                                                                         , CS.numWarmup = numWarmupM
                                                                         }
  let stanOutputFiles = fmap (\n -> outputFile n) [1..numChains]
  stanSummaryConfig <- K.liftKnit $ CS.useCmdStanDirForStansummary (CS.makeDefaultSummaryConfig $ fmap (addDir modelDir) stanOutputFiles)
      

-- produce JSON from the data
type JSONAction r a = a -> K.ActionWithCacheTime r ()
type ResultAction r a b = a -> CS.StanSummary -> K.ActionWithCacheTime r b


runModel :: ModelRunnerConfig
         -> JSONAction r a
         -> ResultAction r a b
         -> K.ActionWithCacheTime r a
         -> K.Sem r b
runModel makeJSON makeResult cachedA = do
  

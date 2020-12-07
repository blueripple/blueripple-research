{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}
module Stan.RScriptBuilder where

{-
import qualified Knit.Report as K
import qualified Knit.Effect.Logger            as K
import qualified BlueRipple.Utilities.KnitUtils as BR

-}

import qualified Stan.ModelConfig as SC
import qualified Stan.ModelBuilder as SB

import           Control.Monad (when)
import Data.Maybe ()
import qualified Data.Text as T
import qualified Data.Text.IO as T
import qualified System.Directory as Dir

import Data.String.Here (here)


libsForShinyStan = ["rstan", "shinystan", "rjson"]
libsForLoo = ["rstan", "shinystan", "loo"]

addLibs :: [T.Text] -> T.Text
addLibs = foldMap addOneLib where
  addOneLib t = "library(" <> t <> ")\n"

rArray :: (a -> T.Text) -> [a] -> T.Text
rArray toText as = "c(" <> T.intercalate "," (fmap toText as) <> ")"

rSetWorkingDirectory :: SC.ModelRunnerConfig -> T.Text -> IO T.Text
rSetWorkingDirectory config dirBase = do
  let wd = dirBase <> "/" <> (SC.mrcModelDir config)
  cwd <- T.pack <$> Dir.canonicalizePath (T.unpack wd)
  return $ "setwd(\"" <> cwd <> "\")"

rReadStanCSV :: SC.ModelRunnerConfig -> T.Text -> T.Text
rReadStanCSV config fitName = fitName <> " <- read_stan_csv(" <> rArray (\x -> "\"output/" <> x <> "\"") (SC.stanOutputFiles config) <> ")"
  
rStanModel :: SC.ModelRunnerConfig -> T.Text
rStanModel config = "stan_model(" <> (SB.modelFile $ SC.mrcModel config) <> ")"

rExtractLogLikelihood :: SC.ModelRunnerConfig -> T.Text -> T.Text
rExtractLogLikelihood config fitName = "extract_log_lik(" <> fitName <> ", merge_chains = FALSE)"

rReadJSON :: SC.ModelRunnerConfig -> T.Text
rReadJSON config = "jsonData <- fromJSON(file = \"data/" <> (SC.mrcDatFile config) <> "\")" 

rPrint :: T.Text -> T.Text
rPrint t = "print(\"" <> t <> "\")"

           
data UnwrapJSON = UnwrapJSON { jsonName :: T.Text, rName :: T.Text } deriving (Show, Eq, Ord)
           
shinyStanScript :: SC.ModelRunnerConfig -> T.Text -> [UnwrapJSON] -> IO T.Text
shinyStanScript config dirBase unwrapJSONs = do
  rSetCWD <- rSetWorkingDirectory config dirBase
  let unwrapCode = if null unwrapJSONs
                   then ""
                   else
                     let unwrap (UnwrapJSON jn rn) = rn <> " <- jsonData $ " <> jn <> "\n"
                         unwraps = mconcat $ fmap unwrap unwrapJSONs
                     in rReadJSON config
                        <> "\n"
                        <> unwraps
  let rScript = addLibs libsForShinyStan
                <> "\n"
                <> rSetCWD <> "\n"
                <> rPrint "Loading csv output.  Might take a minute or two..." <> "\n"
                <> rReadStanCSV config "stanFit" <> "\n"
                <> unwrapCode
--                <> "stanFit@stanModel <- " <> rStanModel config
                <> rPrint "Launching shinystan...." <> "\n"
                <> "launch_shinystan(stanFit)\n"
  return rScript
                               
looScript :: SC.ModelRunnerConfig -> T.Text -> T.Text-> Int -> IO T.Text
looScript config dirBase looName nCores = do
  rSetCWD <- rSetWorkingDirectory config dirBase
  let rScript = addLibs libsForLoo
                <> rSetCWD <> "\n"
                <> rPrint "Loading csv output.  Might take a minute or two..." <> "\n"
                <> rReadStanCSV config "stanFit" <> "\n"
                <> rPrint "Extracting log likelihood for loo..." <> "\n"
                <> "log_lik <-" <> rExtractLogLikelihood config "stanFit" <> "\n"
                <> rPrint "Computing r_eff for loo..." <> "\n"
                <> "rel_eff <- relative_eff(exp(log_lik), cores = " <> (T.pack $ show nCores) <> ")\n"
                <> rPrint "Computing loo.." <> "\n"
                <> looName <> " <- loo(log_lik, r_eff = rel_eff, cores = " <> (T.pack $ show nCores) <> ")\n"
                <> "print(" <> looName <> ")\n"
  return rScript


  
                

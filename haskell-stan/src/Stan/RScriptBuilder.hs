{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
module Stan.RScriptBuilder where

{-
import qualified Knit.Report as K
import qualified Knit.Effect.Logger            as K
import qualified BlueRipple.Utilities.KnitUtils as BR

-}

import qualified Stan.ModelConfig as SC
import qualified Stan.ModelBuilder as SB

--import           Control.Monad (when)
import qualified Control.Foldl as Foldl
--import Data.Maybe ()
import Data.String.Here (here)
import qualified Data.Text as T
import qualified Data.Text.IO as T
import qualified Frames as F
import qualified Frames.Streamly.CSV as FStreamly
import qualified Frames.Streamly.InCore as FStreamly
import qualified System.Directory as Dir
import qualified System.Process as Process
import qualified Streamly.Prelude as Streamly
import qualified Data.Vinyl as V

libsForShinyStan = ["rstan", "shinystan", "rjson"]
libsForLoo = ["rstan", "shinystan", "loo"]

addLibs :: [T.Text] -> T.Text
addLibs = foldMap addOneLib where
  addOneLib t = "library(" <> t <> ")\n"

rArray :: (a -> T.Text) -> [a] -> T.Text
rArray toText as = "c(" <> T.intercalate "," (fmap toText as) <> ")"

{-
rSetWorkingDirectory :: SC.ModelRunnerConfig -> T.Text -> IO T.Text
rSetWorkingDirectory config dirBase = do
  let wd = dirBase <> "/" <> SC.mrcModelDir config
  cwd <- T.pack <$> Dir.canonicalizePath (T.unpack wd)
  return $ "setwd(\"" <> cwd <> "\")"
-}

rReadStanCSV :: SC.ModelRunnerConfig -> T.Text -> T.Text
rReadStanCSV config fitName =
  let modelDir = SC.mrcModelDir config
  in  fitName <> " <- read_stan_csv(" <> rArray (\x -> "\"" <> modelDir <> "/output/" <> x <> "\"") (SC.stanOutputFiles config) <> ")"

rStanModel :: SC.ModelRunnerConfig -> T.Text
rStanModel config = "stan_model(" <> SC.mrcModelDir config <> "/" <> SB.modelFile (SC.mrcModel config) <> ")"

rExtractLogLikelihood :: SC.ModelRunnerConfig -> T.Text -> T.Text
rExtractLogLikelihood config fitName = "extract_log_lik(" <> fitName <> ", merge_chains = FALSE)"

rReadJSON :: SC.ModelRunnerConfig -> T.Text
rReadJSON config =
  let modelDir = SC.mrcModelDir config
  in "jsonData <- fromJSON(file = \"" <> modelDir <> "/data/" <> SC.mrcDatFile config <> "\")"

rMessage :: T.Text -> T.Text
rMessage t = "sink(stderr())\n" <> rPrint t <> "\nsink()\n"

rMessageText :: T.Text -> T.Text
rMessageText t = rMessage $ "\"" <> t <> "\""

rPrint :: T.Text -> T.Text
rPrint t = "print(" <> t <> ")"

rPrintText :: T.Text -> T.Text
rPrintText t = rPrint $ "\"" <> t <> "\""

-- Named version is simpler if you just need to copy a value from jsonData into global namespace
-- Expr version lets you run R code to build the value to put in global namespace
data UnwrapJSON = UnwrapNamed T.Text T.Text | UnwrapExpr T.Text T.Text deriving (Show, Eq, Ord)

unwrap :: UnwrapJSON -> T.Text
unwrap (UnwrapNamed jn rn) = rn <> " <- jsonData $ " <> jn <> "\n"
unwrap (UnwrapExpr je rn) = rn <> " <- " <> je <> "\n"

shinyStanScript :: SC.ModelRunnerConfig -> [UnwrapJSON] -> T.Text
shinyStanScript config unwrapJSONs =
  let unwrapCode = if null unwrapJSONs
                   then ""
                   else
                     let unwraps = mconcat $ fmap unwrap unwrapJSONs
                     in rReadJSON config
                        <> "\n"
                        <> unwraps
      rScript = addLibs libsForShinyStan
                <> "\n"
                <> rMessageText "Loading csv output.  Might take a minute or two..." <> "\n"
                <> rReadStanCSV config "stanFit" <> "\n"
                <> unwrapCode
--                <> "stanFit@stanModel <- " <> rStanModel config
                <> rMessageText "Launching shinystan...." <> "\n"
                <> "launch_shinystan(stanFit)\n"
  in rScript

looOne :: SC.ModelRunnerConfig -> Text -> Maybe Text -> Int -> Text
looOne config fitName mLooName nCores =
  let llName = "ll_" <> fitName
      reName = "re_" <> fitName
      looName = fromMaybe ("loo_" <> fitName) mLooName
      rScript =  rMessageText ("Loading csv output for " <> fitName <> ".  Might take a minute or two...") <> "\n"
                 <> rReadStanCSV config fitName <> "\n"
                 <> rMessageText "Extracting log likelihood for loo..." <> "\n"
                 <> llName <> " <-" <> rExtractLogLikelihood config fitName <> "\n"
                 <> rMessageText "Computing r_eff for loo..." <> "\n"
                 <> reName <> " <- relative_eff(exp(" <> llName <> "), cores = " <> show nCores <> ")\n"
                 <> rMessageText "Computing loo.." <> "\n"
                 <> looName <> " <- loo(" <> llName <> ", r_eff = " <> reName <> ", cores = " <> show nCores <> ")\n"
                 <> rMessage looName <> "\n"
  in rScript

looScript :: SC.ModelRunnerConfig -> T.Text-> Int -> T.Text
looScript config looName nCores =
  let justLoo = looOne config "stanFit" (Just looName) nCores
  in addLibs libsForLoo <> justLoo


compareScript :: Foldable f => f SC.ModelRunnerConfig -> Int -> Maybe Text -> Text
compareScript configs nCores mOutCSV =
  let  doOne (n, c) = looOne c (SC.mrcOutputPrefix c) (Just $ "model" <> show n) nCores
       (numModels, configList) = Foldl.fold ((,) <$> Foldl.length <*> Foldl.list) configs
       looScripts = mconcat $ fmap doOne  $ zip [1..] configList
       compare = "c <- loo_compare(" <> T.intercalate "," (("model" <>) . show <$> [1..numModels]) <> ")\n"
       writeTable = rMessage "c,simplify=FALSE" <> "\n"
       writeCSV = "write.csv(c" <> maybe ")\n" (\csvName -> "," <> csvName <> ")\n") mOutCSV
       rScript = addLibs libsForLoo <> looScripts  <> compare <> writeTable <> writeCSV
  in rScript

-- The below requires Frames and thus adds a dependency

type Model = "Model" F.:-> Text
type ELPD_Diff = "elpd_diff" F.:-> Double
type SE_Diff = "se_diff" F.:-> Double
type ELPD_Loo = "elpd_loo" F.:-> Double
type SE_ELPD_Loo = "se_elpd_loo" F.:-> Double
type P_Loo = "p_loo" F.:-> Double
type SE_P_Loo = "se_p_loo" F.:-> Double
type LOOIC = "looic" F.:-> Double
type SE_LOOIC = "se_looic" F.:-> Double

type LOO_DataR = [ELPD_Diff, SE_Diff, ELPD_Loo, SE_ELPD_Loo, P_Loo, SE_P_Loo, LOOIC, SE_LOOIC]
type LOO_R = Model : LOO_DataR

compareModels :: (Foldable f, Functor f) => f (Text, SC.ModelRunnerConfig) -> Int -> IO () -- (F.FrameRec LLO_R)
compareModels configs nCores = do
  let script = compareScript (snd <$> configs) nCores Nothing
      cp = Process.proc "R" ["BATCH", "--no-save", "--no-restore"]
  putTextLn "Running R for loo comparisons..."
  rOut <- toText <$> Process.readCreateProcess cp (toString script)
  putTextLn "R finished."
  let sRText = Streamly.filter (not . T.isPrefixOf ">") $ Streamly.fromList $ lines rOut
  fLooRaw :: F.FrameRec LOO_R <- FStreamly.inCoreAoS $ FStreamly.streamTable $ Streamly.drop 1 $ sRText
  let nameFrame = F.toFrame $ fmap (F.&: V.RNil) $ fst <$> configs
      fLoo :: F.FrameRec LOO_R = nameFrame `F.zipFrames` (F.rcast @LOO_DataR <$> fLooRaw)
--  Streamly.toList sRText >>= traverse putTextLn
  putTextLn $ T.intercalate "\n" $ fmap show $ Foldl.fold Foldl.list fLoo

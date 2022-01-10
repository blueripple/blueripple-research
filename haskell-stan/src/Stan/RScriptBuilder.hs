{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
module Stan.RScriptBuilder where


import qualified Stan.ModelConfig as SC
import qualified Stan.ModelBuilder as SB

import qualified Colonnade as Col
import qualified Control.Foldl as Foldl
import qualified Data.Map as M
import Data.String.Here (here)
import qualified Data.Text as T
import qualified Data.Text.IO as T
import qualified Frames as F
import qualified Frames.Streamly.CSV as FStreamly
import qualified Frames.Streamly.InCore as FStreamly
import Frames.Streamly.Streaming.Streamly (StreamlyStream(..), SerialT)
import qualified System.Directory as Dir
import qualified System.Process as Process
import qualified Streamly.Prelude as Streamly
import qualified Text.Printf as Printf
import qualified Data.Vinyl as V
import qualified Knit.Report as K
import qualified Stan.ModelConfig as SC



type StreamlyS = StreamlyStream SerialT

libsForShinyStan :: [Text] = ["rstan", "shinystan", "rjson"]
libsForLoo :: [Text] = ["rstan", "shinystan", "loo", "bayesplot"]

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

rReadStanCSV :: SC.ModelRunnerConfig -> T.Text -> Text
rReadStanCSV config fitName = fitName <> " <- read_stan_csv(" <> rArray (\x -> "\"" <> toText x <> "\"") (SC.finalSamplesFileNames config) <> ")"

rStanModel :: SC.ModelRunnerConfig -> T.Text
rStanModel config =
  let rin = SC.mrcInputNames config
      in "stan_model(" <> SC.rinModelDir rin <> "/" <> SC.modelFileName rin <> ")"

llName :: Text -> Text
llName = ("ll_" <>)

reName :: Text -> Text
reName = ("re_" <>)

rExtractLogLikelihood :: T.Text -> T.Text
rExtractLogLikelihood fitName = "extract_log_lik(" <> fitName <> ", merge_chains = FALSE)"

rPSIS :: T.Text -> Int -> T.Text
rPSIS fitName nCores = "psis(" <> llName fitName <> ", r_eff=" <> reName fitName <> ", cores=" <> show nCores <> ")"

rExtract :: T.Text -> T.Text
rExtract fitName = "extract(" <> fitName <> ")"

rReadJSON :: SC.ModelRunnerConfig -> T.Text
rReadJSON config =
  let modelDir = SC.mrcModelDir config
  in "jsonData <- fromJSON(file = \"" <> modelDir <> "/data/" <> SC.modelDataFileName (SC.mrcInputNames config) <> "\")"

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
  let readStanCSV = rReadStanCSV config "stanfit"
      unwrapCode = if null unwrapJSONs
                   then ""
                   else
                     let unwraps = mconcat $ fmap unwrap unwrapJSONs
                     in rReadJSON config
                        <> "\n"
                        <> unwraps
      rScript = addLibs libsForShinyStan
                <> "\n"
                <> rMessageText "Loading csv output.  Might take a minute or two..." <> "\n"
                <> readStanCSV <> "\n"
                <> unwrapCode
--                <> "stanfit@stanModel <- " <> rStanModel config
                <> rMessageText "Launching shinystan...." <> "\n"
                <> "launch_shinystan(stanfit)\n"
  in rScript

looOne :: SC.ModelRunnerConfig -> Text -> Maybe Text -> Int -> Text
looOne config fitName mLooName nCores =
  let readStanCSV = rReadStanCSV config fitName
      psisName = "psis_" <> fitName
      looName = fromMaybe ("loo_" <> fitName) mLooName
      samplesName = "samples_" <> fitName
      rScript =  rMessageText ("Loading csv output for " <> fitName <> ".  Might take a minute or two...") <> "\n"
                 <> readStanCSV <> "\n"
                 <> rMessageText "Extracting log likelihood for loo..." <> "\n"
                 <> llName fitName <> " <-" <> rExtractLogLikelihood fitName <> "\n"
                 <> rMessageText "Computing r_eff for loo..." <> "\n"
                 <> reName fitName <> " <- relative_eff(exp(" <> llName fitName <> "), cores = " <> show nCores <> ")\n"
                 <> rMessageText "Computing loo.." <> "\n"
                 <> looName <> " <- loo(" <> llName fitName <> ", r_eff=" <> reName fitName <> ", cores=" <> show nCores <> ")\n"
                 <> rMessage looName <> "\n"
                 <> rMessageText "Computing PSIS..."
                 <> psisName <> " <- " <> looName <> "$psis_object" <> "\n"
                 <> rMessageText ("Placing samples in " <> samplesName) <> "\n"
                 <> samplesName <> " <- " <> rExtract fitName <> "\n"
                 <> rMessageText ("E.g., 'ppc_loo_pit_qq(y,as.matrix(" <> samplesName <> "$y_ppred)," <> psisName <> "$log_weights)'") <> "\n"
  in rScript

looScript ::  SC.ModelRunnerConfig -> T.Text-> Int -> T.Text
looScript config looName nCores =
  let justLoo = looOne config "stanfit" (Just looName) nCores
  in addLibs libsForLoo <> justLoo


compareScript ::  Foldable f
              => f SC.ModelRunnerConfig -> Int -> Maybe Text -> Text
compareScript configs nCores mOutCSV =
  let  doOne (n, c) = looOne c (SC.finalPrefix $ SC.mrcInputNames c) (Just $ "model" <> show n) nCores
       (numModels, configList) = Foldl.fold ((,) <$> Foldl.length <*> Foldl.list) configs
       compare = "c <- loo_compare(" <> T.intercalate "," (("model" <>) . show <$> [1..numModels]) <> ")\n"
       writeTable = rMessage "c,simplify=FALSE" <> "\n"
       writeCSV = "write.csv(c" <> maybe ")\n" (\csvName -> "," <> csvName <> ")\n") mOutCSV
       looScripts = mconcat $ fmap doOne  $ zip [1..] configList
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

compareModels :: forall st cd r f. (SC.KnitStan st cd r, Traversable f)
              => f (Text, SC.ModelRunnerConfig) -> Int -> K.Sem r (F.FrameRec LOO_R)
compareModels configs nCores = do
  let script = compareScript (snd <$> configs) nCores Nothing
  let cp = Process.proc "R" ["BATCH", "--no-save", "--no-restore"]
  K.liftKnit  @IO $ putTextLn "Running R for loo comparisons..."
  rOut <- toText <$> (K.liftKnit $ Process.readCreateProcess cp (toString script))
  putTextLn "R finished."
  let sRText = Streamly.filter (not . T.isPrefixOf ">") $ Streamly.fromList $ lines rOut
  fLooRaw :: F.FrameRec LOO_R <- K.liftKnit @IO
                                 $ FStreamly.inCoreAoS @_ @_ @StreamlyS
                                 $ FStreamly.streamTable
                                 $ StreamlyStream
                                 $ Streamly.map (T.split (== ','))
                                 $ Streamly.drop 1 sRText
  -- map results to models
  let resultModelMap :: Map Text Text = M.fromList $ zip ((\n -> "model"<> show n) <$> [1..]) (Foldl.fold (Foldl.premap fst Foldl.list) configs)
      fixName :: F.Record LOO_R -> F.Record LOO_R
      fixName r =
        let oldName = F.rgetField @Model r
            newName = fromMaybe oldName $ M.lookup oldName resultModelMap
        in F.rputField @Model newName r
  return $ fmap fixName fLooRaw

--  let nameFrame = F.toFrame $ fmap (F.&: V.RNil) $ fst <$> configs
--      fLoo :: F.FrameRec LOO_R = nameFrame `F.zipFrames` (F.rcast @LOO_DataR <$> fLooRaw)

looTextColonnade :: Int -> Col.Colonnade Col.Headed (F.Record LOO_R) Text
looTextColonnade digits =
  let printDouble = toText @String . Printf.printf "%.*f" digits
  in mconcat
     [
       Col.headed "Model" (F.rgetField @Model)
     , Col.headed "elpd diff" (printDouble . F.rgetField @ELPD_Diff)
     , Col.headed "se diff" (printDouble . F.rgetField @SE_Diff)
     , Col.headed "elpd loo" (printDouble . F.rgetField @ELPD_Loo)
     , Col.headed "se_elpd_loo" (printDouble . F.rgetField @SE_ELPD_Loo)
     , Col.headed "p_loo" (printDouble . F.rgetField @P_Loo)
     , Col.headed "se_p_loo" (printDouble . F.rgetField @SE_P_Loo)
     ]

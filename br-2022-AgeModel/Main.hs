{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UnicodeSyntax #-}

module Main where

import qualified BlueRipple.Configuration as BR
import qualified BlueRipple.Model.Election.ModelAge as AM
import qualified BlueRipple.Data.ACS_PUMS as PUMS
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.Loaders as BRL
import qualified BlueRipple.Data.DataFrames as BRDF
import qualified BlueRipple.Utilities.KnitUtils as BRK

import qualified Stan.ModelBuilder.TypedExpressions.DAG as DAG

import qualified Stan.ModelBuilder as S
import qualified Stan.ModelBuilder.TypedExpressions.Program as SP
import qualified Stan.ModelConfig as SC
import qualified Stan.ModelRunner as SMR
import qualified Stan.RScriptBuilder as SR
import qualified CmdStan as CS

import qualified Knit.Report as K
import qualified Knit.Effect.AtomicCache as K (cacheTime)
import qualified Knit.Effect.AtomicCache as KC
import qualified Text.Pandoc.Error as Pandoc
import qualified System.Console.CmdArgs as CmdArgs

import qualified Data.Map as M
import qualified Data.Set as S
import qualified Data.Text as T
import qualified Data.Vector.Unboxed as VU
import qualified Control.Foldl as FL
import qualified Frames as F
import Control.Lens (view)

import qualified Frames.Visualization.VegaLite.Data as FVD
import qualified Graphics.Vega.VegaLite as GV
import qualified Graphics.Vega.VegaLite.Compat as FV
import qualified Graphics.Vega.VegaLite.Configuration as FV

templateVars ∷ M.Map String String
templateVars =
  M.fromList
    [ ("lang", "English")
    , ("site-title", "Blue Ripple Politics")
    , ("home-url", "https://www.blueripplepolitics.org")
    --  , ("author"   , T.unpack yamlAuthor)
    ]

pandocTemplate ∷ K.TemplatePath
pandocTemplate = K.FullySpecifiedTemplatePath "pandoc-templates/blueripple_basic.html"

main :: IO ()
main = do
  cmdLine ← CmdArgs.cmdArgsRun BR.commandLine
  pandocWriterConfig ←
    K.mkPandocWriterConfig
    pandocTemplate
    templateVars
    (BRK.brWriterOptionsF . K.mindocOptionsF)
  let cacheDir = ".flat-kh-cache"
      knitConfig ∷ K.KnitConfig BRK.SerializerC BRK.CacheData Text =
        (K.defaultKnitConfig $ Just cacheDir)
          { K.outerLogPrefix = Just "2022-AgeModel"
          , K.logIf = BR.knitLogSeverity $ BR.logLevel cmdLine -- K.logDiagnostic
          , K.pandocWriterConfig = pandocWriterConfig
          , K.serializeDict = BRK.flatSerializeDict
          , K.persistCache = KC.persistStrictByteString (\t → toString (cacheDir <> "/" <> t))
          }
  resE ← K.knitHtmls knitConfig $ do
    runAgeModel False
    runEduModel False
  case resE of
    Right namedDocs →
      K.writeAllPandocResultsWithInfoAsHtml "" namedDocs
    Left err → putTextLn $ "Pandoc Error: " <> Pandoc.renderError err

logLengthC :: (K.KnitEffects r, Foldable f) => K.ActionWithCacheTime r (f a) -> Text -> K.Sem r ()
logLengthC xC t = K.ignoreCacheTime xC >>= \x -> K.logLE K.Info $ t <> "has " <> show (FL.fold FL.length x) <> " rows."

runAgeModel :: (K.KnitEffects r, BRK.CacheEffects r) => Bool -> K.Sem r ()
runAgeModel clearCaches = do
  let cacheKeyE = let k = "model/AgeModel/test" in if clearCaches then Left k else Right k
      runnerInputNames = SC.RunnerInputNames
                         "br-2022-AgeModel/stanAge"
                         "categoricalSER"
                         Nothing
                         "acsAge"
      only2020 r = F.rgetField @BRDF.Year r == 2020
  acs_C <- fmap (F.filterFrame only2020) <$> PUMS.pumsLoader Nothing >>= AM.cachedACSByState
  logLengthC acs_C "acsByState"
  let acsMN_C = fmap AM.acsByStateAgeMN acs_C
  logLengthC acsMN_C "acsByStateMNAge"
  states <- FL.fold (FL.premap (view BRDF.stateAbbreviation . fst) FL.set) <$> K.ignoreCacheTime acsMN_C
  (dw, code) <- SMR.dataWranglerAndCode acsMN_C (pure ())
                (AM.groupBuilderState (S.toList states))
                (AM.categoricalModel (length [(minBound :: DT.Age5F)..]) (AM.designMatrixRowAge AM.logDensityDMRP))
  () <- do
    K.ignoreCacheTimeM
      $ SMR.runModel' @BRK.SerializerC @BRK.CacheData
      cacheKeyE
      (Right runnerInputNames)
      dw
      code
      SC.DoNothing
      (SMR.Both [])
      acsMN_C
      (pure ())
  K.logLE K.Info "Test run complete."

runEduModel :: (K.KnitEffects r, BRK.CacheEffects r) => Bool -> K.Sem r ()
runEduModel clearCaches = do
  let cacheKeyE = let k = "model/AgeModel/test" in if clearCaches then Left k else Right k
      runnerInputNames = SC.RunnerInputNames
                         "br-2022-AgeModel/stanEdu"
                         "categoricalSEA"
                         Nothing
                         "acsEdu"
      only2020 r = F.rgetField @BRDF.Year r == 2020
  acs_C <- fmap (F.filterFrame only2020) <$> PUMS.pumsLoader Nothing >>= AM.cachedACSByState
  logLengthC acs_C "acsByState"
  let acsMN_C = fmap AM.acsByStateEduMN acs_C
  logLengthC acsMN_C "acsByStateMNEdu"
  states <- FL.fold (FL.premap (view BRDF.stateAbbreviation . fst) FL.set) <$> K.ignoreCacheTime acsMN_C
  (dw, code) <- SMR.dataWranglerAndCode acsMN_C (pure ())
                (AM.groupBuilderState (S.toList states))
                (AM.binomialModel (AM.designMatrixRowEdu2 Nothing)) -- (Just AM.logDensityDMRP)
  () <- do
    K.ignoreCacheTimeM acsMN_C >>= (K.addHvega Nothing Nothing $ chart (FV.ViewConfig 500 500 5))
    K.ignoreCacheTimeM
      $ SMR.runModel' @BRK.SerializerC @BRK.CacheData
      cacheKeyE
      (Right runnerInputNames)
      dw
      code
      SC.DoNothing
      (SMR.Both [])
      acsMN_C
      (pure ())
  K.logLE K.Info "Test run complete."

chart :: Foldable f => FV.ViewConfig -> f AM.ACSByStateEduMN -> GV.VegaLite
chart vc rows =
  let total v = v VU.! 0 + v VU.! 1
      grads v = v VU.! 1
      rowToData (r, v) = [("Sex", GV.Str $ show $ F.rgetField @DT.SexC r)
                         , ("Age", GV.Str $ show $ F.rgetField @DT.Age5FC r)
                         , ("Race", GV.Str $ show (F.rgetField @DT.RaceAlone4C r) <> "_" <> show (F.rgetField @DT.HispC r))
                         , ("Total", GV.Number $ realToFrac $ total v)
                         , ("Grads", GV.Number $ realToFrac $ grads v)
                         , ("FracGrad", GV.Number $ realToFrac (grads v)/realToFrac (total v))
                         ]
      toVLDataRows x = GV.dataRow (rowToData x) []
      vlData = GV.dataFromRows [] $ concat $ fmap toVLDataRows $ FL.fold FL.list rows
      encX = GV.position GV.X [GV.PName "Age", GV.PmType GV.Nominal]
      encY = GV.position GV.Y [GV.PName "FracGrad", GV.PmType GV.Quantitative]
      mark = GV.mark GV.Circle []
      enc = (GV.encoding . encX . encY)
  in FV.configuredVegaLite vc [FV.title "FracGrad v Age", enc [], mark, vlData]

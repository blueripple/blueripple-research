{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}

module Main where

import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.ElectionTypes as ET
import qualified BlueRipple.Data.Loaders as BR
import qualified BlueRipple.Model.House.ElectionResult as BRE
import qualified BlueRipple.Model.StanCCES as BRS
import qualified BlueRipple.Utilities.KnitUtils as BR
import qualified Control.Foldl as FL
import qualified Data.Map as M
import qualified Data.Random.Source.PureMT as PureMT
import Data.String.Here (here)
import qualified Data.Text as T
import qualified Frames as F
import qualified Frames.MapReduce as FMR
import Frames.Visualization.VegaLite.Data as FV
import Frames.Visualization.VegaLite.Histogram as FV
--import Graphics.Vega.VegaLite as GV
import Graphics.Vega.VegaLite.Configuration as FV
  ( AxisBounds (DataMinMax),
    ViewConfig (ViewConfig),
  )
import qualified Knit.Report as K
import Polysemy.RandomFu (RandomFu, runRandomIOPureMT)

yamlAuthor :: T.Text
yamlAuthor =
  [here|
- name: Adam Conner-Sax
- name: Frank David
|]

templateVars :: M.Map String String
templateVars =
  M.fromList
    [ ("lang", "English"),
      ("site-title", "Blue Ripple Politics"),
      ("home-url", "https://www.blueripplepolitics.org")
      --  , ("author"   , T.unpack yamlAuthor)
    ]

pandocTemplate = K.FullySpecifiedTemplatePath "pandoc-templates/blueripple_basic.html"

main :: IO ()
main = do
  pandocWriterConfig <-
    K.mkPandocWriterConfig
      pandocTemplate
      templateVars
      K.mindocOptionsF
  let knitConfig =
        (K.defaultKnitConfig Nothing)
          { K.outerLogPrefix = Just "stan.Main",
            K.logIf = K.logAll,
            K.pandocWriterConfig = pandocWriterConfig
          }
  let pureMTseed = PureMT.pureMT 1
  --
  resE <- K.knitHtml knitConfig $ testHouseModel
  case resE of
    Right htmlAsText ->
      K.writeAndMakePathLT "stan.html" htmlAsText
    Left err -> putStrLn $ "Pandoc Error: " ++ show err

--  (demographics, elex) <- K.ignoreCacheTimeM $ HEM.prepCachedData
--  BR.logFrame demographics
--  BR.logFrame elex
--  return ()

testHouseModel :: forall r. (K.KnitOne r, K.CacheEffectsD r) => K.Sem r ()
testHouseModel =
  do
    K.logLE K.Info "Test: Stan model fit for house turnout and dem votes. Data prep..."
    houseData_C <- BRE.prepCachedData
    houseData <- K.ignoreCacheTime houseData_C
    BR.logFrame $ F.filterFrame ((== 2018) . F.rgetField @BR.Year) houseData
    _ <- K.addHvega Nothing Nothing $ FV.singleHistogram @BRE.FracUnder45 "% Under 45" Nothing 50 FV.DataMinMax True (FV.ViewConfig 400 400 5) houseData
    _ <- K.addHvega Nothing Nothing $ FV.singleHistogram @BRE.FracFemale "% Female" Nothing 50 FV.DataMinMax True (FV.ViewConfig 400 400 5) houseData
    _ <- K.addHvega Nothing Nothing $ FV.singleHistogram @BRE.FracGrad "% Grad" Nothing 50 FV.DataMinMax True (FV.ViewConfig 400 400 5) houseData
    _ <- K.addHvega Nothing Nothing $ FV.singleHistogram @BRE.FracNonWhite "% Non-White" Nothing 50 FV.DataMinMax True (FV.ViewConfig 400 400 5) houseData
    _ <- K.addHvega Nothing Nothing $ FV.singleHistogram @BRE.FracCitizen "% Citizen" Nothing 50 FV.DataMinMax True (FV.ViewConfig 400 400 5) houseData
    _ <- K.addHvega Nothing Nothing $ FV.singleHistogram @DT.AvgIncome "Average Income" Nothing 50 FV.DataMinMax True (FV.ViewConfig 400 400 5) houseData
    _ <- K.addHvega Nothing Nothing $ FV.singleHistogram @DT.PopPerSqMile "Density (ppl/sq mile)" Nothing 50 FV.DataMinMax True (FV.ViewConfig 400 400 5) houseData
    let isYear year = (== year) . F.rgetField @BR.Year
        dVotes = F.rgetField @BRE.DVotes
        rVotes = F.rgetField @BRE.RVotes
        competitive r = dVotes r > 0 && rVotes r > 0
        competitiveIn y r = isYear y r && competitive r
    K.logLE K.Info "run model(s)"
    let models =
          [ ("betaBinomialInc", BRE.betaBinomialInc {-},
                                                    ("binomial_v1", BRE.binomial_v1),
                                                    ("betaBinomial_v1", BRE.betaBinomial_v1),
                                                    ("betaBinomialHS", BRE.betaBinomialHS)-})
          ]
        runOne x =
          BRE.runHouseModel
            BRE.houseDataWrangler
            x
            (fmap (F.filterFrame (competitiveIn 2018)) houseData_C)
    results <- K.ignoreCacheTimeM $ fmap sequenceA $ traverse runOne models
    let (resultPredictions, resultDeltas) = results !! 0
    BR.logFrame resultPredictions
    K.logLE K.Info $ "Deltas :" <> (T.pack $ show resultDeltas)

testCCESPref :: forall r. (K.KnitOne r, K.CacheEffectsD r, K.Member RandomFu r) => K.Sem r ()
testCCESPref = do
  K.logLE K.Info "Stan model fit for 2016 presidential votes:"
  stan_allBuckets <-
    K.ignoreCacheTimeM $
      BRS.prefASER5_MR
        ("v1", BRS.ccesDataWrangler)
        ("binomial_allBuckets", BRS.model_BinomialAllBuckets)
        ET.President
        2016

  stan_sepFixedWithStates <-
    K.ignoreCacheTimeM $
      BRS.prefASER5_MR
        ("v2", BRS.ccesDataWrangler2)
        ("binomial_sepFixedWithStates", BRS.model_v5)
        ET.President
        2016

  stan_sepFixedWithStates3 <-
    K.ignoreCacheTimeM $
      BRS.prefASER5_MR
        ("v2", BRS.ccesDataWrangler2)
        ("binomial_sepFixedWithStates3", BRS.model_v7)
        ET.President
        2016

  K.logLE K.Info $ "allBuckets vs sepFixedWithStates3"
  let compList = zip (FL.fold FL.list stan_allBuckets) $ fmap (F.rgetField @ET.DemPref) $ FL.fold FL.list stan_sepFixedWithStates3
  K.logLE K.Info $ T.intercalate "\n" . fmap (T.pack . show) $ compList

  BRS.prefASER5_MR_Loo ("v1", BRS.ccesDataWrangler) ("binomial_allBuckets", BRS.model_BinomialAllBuckets) ET.President 2016
  BRS.prefASER5_MR_Loo ("v1", BRS.ccesDataWrangler) ("binomial_bucketFixedStateIntcpt", BRS.model_v2) ET.President 2016
  BRS.prefASER5_MR_Loo ("v1", BRS.ccesDataWrangler) ("binomial_bucketFixedOnly", BRS.model_v3) ET.President 2016
  BRS.prefASER5_MR_Loo ("v2", BRS.ccesDataWrangler2) ("binomial_sepFixedOnly", BRS.model_v4) ET.President 2016
  BRS.prefASER5_MR_Loo ("v2", BRS.ccesDataWrangler2) ("binomial_sepFixedWithStates", BRS.model_v5) ET.President 2016
  BRS.prefASER5_MR_Loo ("v2", BRS.ccesDataWrangler2) ("binomial_sepFixedWithStates2", BRS.model_v6) ET.President 2016
  BRS.prefASER5_MR_Loo ("v2", BRS.ccesDataWrangler2) ("binomial_sepFixedWithStates3", BRS.model_v7) ET.President 2016

--  BR.logFrame stan
{-
  K.logLE K.Info "glm-haskell model fit for 2016 presidential votes:"
  let g r = (F.rgetField @BR.Year r == 2016) && (F.rgetField @ET.Office r == ET.President)
  glmHaskell <- F.filterFrame g <$> (K.ignoreCacheTimeM $ BRC.ccesPreferencesASER5_MRP)
  BR.logFrame glmHaskell
-}

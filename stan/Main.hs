{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedLabels #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}

module Main where

import qualified BlueRipple.Data.ACS_PUMS as PUMS
import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.ElectionTypes as ET
--import qualified BlueRipple.Data.Loaders as BR
import qualified BlueRipple.Model.House.ElectionResult as BRE
import qualified BlueRipple.Model.StanCCES as BRS
import qualified BlueRipple.Utilities.KnitUtils as BR
import qualified Control.Foldl as FL
import qualified Data.Map as M
import qualified Data.Random.Source.PureMT as PureMT
import qualified Data.Set as S
import Data.String.Here (here)
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Frames as F
--import qualified Frames.MapReduce as FMR
import qualified Frames.Visualization.VegaLite.Correlation as FV
--import qualified Frames.Visualization.VegaLite.Data as FV
import qualified Frames.Visualization.VegaLite.Histogram as FV
import qualified Graphics.Vega.VegaLite as GV
import qualified Graphics.Vega.VegaLite.Compat as FV
import Graphics.Vega.VegaLite.Configuration as FV
  ( AxisBounds (DataMinMax),
    ViewConfig (ViewConfig),
  )
import qualified Graphics.Vega.VegaLite.Configuration as FV
import qualified Graphics.Vega.VegaLite.MapRow as MapRow
import qualified Knit.Report as K
import qualified Optics
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
    hmd <- K.ignoreCacheTime houseData_C
    BR.logFrame $ F.filterFrame ((== "GA") . F.rgetField @BR.StateAbbreviation) (Optics.view #ccesData hmd)
    _ <- K.addHvega Nothing Nothing $ FV.singleHistogram @BRE.FracUnder45 "% Under 45" Nothing 50 FV.DataMinMax True (FV.ViewConfig 400 400 5) (Optics.view #electionData hmd)
    _ <- K.addHvega Nothing Nothing $ FV.singleHistogram @BRE.FracFemale "% Female" Nothing 50 FV.DataMinMax True (FV.ViewConfig 400 400 5) (Optics.view #electionData hmd)
    _ <- K.addHvega Nothing Nothing $ FV.singleHistogram @BRE.FracGrad "% Grad" Nothing 50 FV.DataMinMax True (FV.ViewConfig 400 400 5) (Optics.view #electionData hmd)
    _ <- K.addHvega Nothing Nothing $ FV.singleHistogram @BRE.FracNonWhite "% Non-White" Nothing 50 FV.DataMinMax True (FV.ViewConfig 400 400 5) (Optics.view #electionData hmd)
    _ <- K.addHvega Nothing Nothing $ FV.singleHistogram @BRE.FracCitizen "% Citizen" Nothing 50 FV.DataMinMax True (FV.ViewConfig 400 400 5) (Optics.view #electionData hmd)
    _ <- K.addHvega Nothing Nothing $ FV.singleHistogram @DT.AvgIncome "Average Income" Nothing 50 FV.DataMinMax True (FV.ViewConfig 400 400 5) (Optics.view #electionData hmd)
    _ <- K.addHvega Nothing Nothing $ FV.singleHistogram @DT.PopPerSqMile "Density (ppl/sq mile)" Nothing 50 FV.DataMinMax True (FV.ViewConfig 400 400 5) (Optics.view #electionData hmd)
    let votes r = F.rgetField @BRE.DVotes r + F.rgetField @BRE.RVotes r
        turnout r = realToFrac (votes r) / realToFrac (F.rgetField @PUMS.Citizens r)
        dShare r = if (votes r > 0) then realToFrac (F.rgetField @BRE.DVotes r) / realToFrac (votes r) else 0
    let corrSet = S.fromList [FV.LabeledCol "% Under 45" (F.rgetField @BRE.FracUnder45)
                             ,FV.LabeledCol "% Female" (F.rgetField @BRE.FracFemale)
                             ,FV.LabeledCol "% Grad" (F.rgetField @BRE.FracGrad)
                             ,FV.LabeledCol "% NonWhite" (F.rgetField @BRE.FracNonWhite)
                             ,FV.LabeledCol "% Citizen" (F.rgetField @BRE.FracCitizen)
                             ,FV.LabeledCol "Avg. Income" (F.rgetField @DT.AvgIncome)
                             ,FV.LabeledCol "Density" (F.rgetField @DT.PopPerSqMile)
                             ,FV.LabeledCol "Incumbency" (realToFrac . F.rgetField @BRE.Incumbency)
                             ,FV.LabeledCol "Turnout" turnout
                             ,FV.LabeledCol "D Share" dShare
                             ]
    corrChart <- K.knitEither $ FV.frameCorrelations "Correlations among predictors & predicted" (FV.ViewConfig 600 600 10) False corrSet (Optics.view #electionData hmd)
    _ <- K.addHvega Nothing Nothing corrChart 
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
            2018
            (fmap (Optics.over #electionData (F.filterFrame (competitiveIn 2018))
                   . Optics.over #ccesData (F.filterFrame (isYear 2018)))
              houseData_C
            )
        years = [2012, 2014, 2016, 2018]
        runYear y =
          BRE.runHouseModel
            BRE.houseDataWrangler
            ("betaBinomialInc", BRE.betaBinomialInc)
            y
            (fmap (Optics.over #electionData (F.filterFrame (competitiveIn y))
              . Optics.over #ccesData (F.filterFrame (isYear y)))
              houseData_C
            )
--            (fmap (F.filterFrame (competitiveIn y)) houseData_C)
    results <- zip years <$> (K.ignoreCacheTimeM $ fmap sequenceA $ traverse runYear years)
    let nameType l =
          let (name, t) = T.splitAt (T.length l - 1) l
           in case t of
                "D" -> Right (name, "D Pref")
                "y" -> Right (l, "D Pref")
                "V" -> Right (name, "Turnout")
                _ -> Left $ "Bad last character in delta label (" <> l <> ")."
        expandInterval :: (T.Text, [Double]) -> Either T.Text (MapRow.MapRow GV.DataValue) --T.Text (T.Text, [(T.Text, Double)])
        expandInterval (l, vals) = do
          (name, t) <- nameType l
          case vals of
            [lo, mid, hi] -> Right $ M.fromList [("Name", GV.Str name), ("Type", GV.Str t), ("lo", GV.Number $ 100 * (lo - mid)), ("mid", GV.Number $ 100 * mid), ("hi", GV.Number $ 100 * (hi - mid))]
            _ -> Left $ "Wrong length list in what should be a (lo, mid, hi) interval"
        expandMapRow (y, (_, mr)) = fmap (M.insert "Year" (GV.Str $ T.pack $ show y)) <$> traverse expandInterval (M.toList mr)
    mapRows <- K.knitEither $ traverse expandMapRow results
    --    K.logLE K.Info $ T.pack $ show $ fmap (fmap MapRow.dataValueText) $ concat mapRows
    _ <- K.addHvega Nothing Nothing $ modelChart "Change in Probability for 1 std dev change in predictor (with 90% confidence bands)" (FV.ViewConfig 200 200 5) "D Pref" $ concat mapRows

    return ()

modelChart :: (Functor f, Foldable f) => T.Text -> FV.ViewConfig -> T.Text -> f (MapRow.MapRow GV.DataValue) -> GV.VegaLite
modelChart title vc t rows =
  let vlData = MapRow.toVLData M.toList [GV.Parse [("Year", GV.FoDate "%Y")]] rows
      encX = GV.position GV.X [GV.PName "Year", GV.PmType GV.Temporal]
      encY = GV.position GV.Y [GV.PName "mid", GV.PmType GV.Quantitative, axis, scale]
      axis = GV.PAxis [GV.AxTitle "% Change" {-, GV.AxValues (GV.Numbers [-15, -10, -5, 0, 5, 10, 15])-}]
      scale = GV.PScale [GV.SDomain $ GV.DNumbers [-12, 12]]
      encYLo = GV.position GV.YError [GV.PName "lo", GV.PmType GV.Quantitative]
      encYHi = GV.position GV.YError2 [GV.PName "hi", GV.PmType GV.Quantitative]
      encColor = GV.color [GV.MName "Type", GV.MmType GV.Nominal]
      enc = GV.encoding . encX . encYLo . encY . encYHi . encColor
      markBand = GV.mark GV.ErrorBand [GV.MInterpolate GV.Basis]
      markLine = GV.mark GV.Line [GV.MInterpolate GV.Basis]
      transform = GV.transform . GV.filter (GV.FEqual "Type" (GV.Str t))
      specBand = GV.asSpec [enc [], markBand]
      specLine = GV.asSpec [enc [], markLine]
      spec = GV.asSpec [GV.layer [specBand, specLine]]
      facet = GV.facetFlow [GV.FName "Name", GV.FmType GV.Nominal, GV.FTitle ""]
   in FV.configuredVegaLite vc [FV.title title, GV.columns 4, facet, GV.specification spec, vlData]

{-
getCorrelation :: (T.Text, V.Vector Double) -> (T.Text, V.Vector Double) -> Double
getCorrelation (_, v1) (_, v2) =
  let v12 = V.zip v1 v2
      (m1, var1, m2, var2) = FL.fold ((,,,)
                                      <$> FL.premap fst FL.mean
                                      <*> FL.premap fst FL.variance
                                      <*> FL.premap snd FL.mean
                                      <*> FL.premap snd FL.variance
                                     )
                             v12
      
      covF = (/) <$> (FL.Fold (\s (x1, x2) -> (s + (x1 - m1) * (x2 - m2))) 0 id) <*> fmap realToFrac FL.length
--      corrF = (\var1 var2 cov -> cov / sqrt (var1 * var2)) <$> var1F <*> var2F <*> covF
   in (FL.fold covF v12) / sqrt (var1 * var2)

-}
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

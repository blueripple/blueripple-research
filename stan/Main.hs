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
import qualified BlueRipple.Model.House.ElectionResult as BRE
import qualified BlueRipple.Model.StanCCES as BRS
import qualified BlueRipple.Utilities.KnitUtils as BR
import qualified BlueRipple.Utilities.TableUtils as BR
import qualified Control.Foldl as FL
import qualified Data.Map as M
import qualified Data.Random.Source.PureMT as PureMT
import qualified Data.Semigroup as Semigroup
import qualified Data.Set as S
import Data.String.Here (here)
import qualified Data.Text as T
--import qualified Data.Vector as V
import qualified Data.Vinyl as V
import qualified Frames as F
import qualified Frames.SimpleJoins  as FJ
import qualified Frames.Transform  as FT
import qualified Frames.Table as FTable
import qualified Frames.Visualization.VegaLite.Correlation as FV
import qualified Frames.Visualization.VegaLite.Histogram as FV
import qualified Graphics.Vega.VegaLite as GV
import qualified Graphics.Vega.VegaLite.Compat as FV
import Graphics.Vega.VegaLite.Configuration as FV
  ( AxisBounds (DataMinMax),
    ViewConfig (ViewConfig),
  )
import qualified Graphics.Vega.VegaLite.Configuration as FV
import qualified Frames.Visualization.VegaLite.Data
                                               as FV

import qualified Data.MapRow as MapRow
import qualified Knit.Report as K
import qualified Knit.Effect.AtomicCache as KC
import qualified Numeric
import qualified Optics
import Optics.Operators
import Polysemy.RandomFu (RandomFu, runRandomIOPureMT)

import qualified Stan.ModelConfig as SC
import qualified Stan.RScriptBuilder as SR
import qualified Text.Blaze.Html5              as BH

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
          { K.outerLogPrefix = Just "HouseModel",
            K.logIf = K.nonDiagnostic,
            K.pandocWriterConfig = pandocWriterConfig
          }
--  let pureMTseed = PureMT.pureMT 1
  --
  resE <- K.knitHtmls knitConfig testHouseModel
  case resE of
    Right namedDocs ->
      K.writeAllPandocResultsWithInfoAsHtml "house_model" namedDocs
    Left err -> putStrLn $ "Pandoc Error: " ++ show err

type PctTurnout = "PctTurnout" F.:-> Double
type DShare = "DShare" F.:-> Double


testHouseModel :: forall r. (K.KnitMany r, K.CacheEffectsD r) => K.Sem r ()
testHouseModel = do
  K.logLE K.Info "Data prep..."
  houseData_C <- BRE.prepCachedData False
  hmd <- K.ignoreCacheTime houseData_C
  K.logLE K.Info "(predictors.html): Predictor & Predicted: Distributions & Correlations"
  K.newPandoc (K.PandocInfo "examine_predictors" $ one ("pagetitle","Examine Predictors")) $ do
    BR.brAddMarkDown "## Predictors/Predicted: Distributions in house elections 2012-2018"
    let vcDist = FV.ViewConfig 200 200 5
        mhStyle = FV.FacetedBar
        votes r = F.rgetField @BRE.DVotes r + F.rgetField @BRE.RVotes r
        turnout r = realToFrac (votes r) / realToFrac (F.rgetField @PUMS.Citizens r)
        dShare r = if votes r > 0 then realToFrac (F.rgetField @BRE.DVotes r) / realToFrac (votes r) else 0
    _ <- K.addHvega Nothing Nothing
         $ FV.multiHistogram @BRE.FracUnder45 @BR.Year "% Under 45" Nothing 50 FV.DataMinMax True mhStyle vcDist (hmd ^. #electionData)
    _ <- K.addHvega Nothing Nothing
         $ FV.multiHistogram @BRE.FracFemale @BR.Year "% Female" Nothing 50 FV.DataMinMax True mhStyle vcDist (hmd ^. #electionData)
    _ <- K.addHvega Nothing Nothing
         $ FV.multiHistogram @BRE.FracGrad @BR.Year "% Grad" Nothing 50 FV.DataMinMax True  mhStyle vcDist (hmd ^. #electionData)
    _ <- K.addHvega Nothing Nothing
         $ FV.multiHistogram @BRE.FracNonWhite @BR.Year "% Non-White" Nothing 50 FV.DataMinMax True mhStyle vcDist (hmd ^. #electionData)
    _ <- K.addHvega Nothing Nothing
         $ FV.multiHistogram @DT.AvgIncome @BR.Year "Average Income" Nothing 50 FV.DataMinMax True mhStyle vcDist (hmd ^. #electionData)
    _ <- K.addHvega Nothing Nothing
         $ FV.multiHistogram @DT.PopPerSqMile @BR.Year "Density [log(ppl/sq mile)]" Nothing 50 FV.DataMinMax True mhStyle vcDist
         $ fmap (FT.fieldEndo @DT.PopPerSqMile (Numeric.logBase 10)) (hmd ^. #electionData)
    _ <- K.addHvega Nothing Nothing
         $ FV.multiHistogram @PctTurnout @BR.Year "Turnout %" Nothing 50 FV.DataMinMax True mhStyle vcDist
         $ FT.mutate (FT.recordSingleton @PctTurnout . (*100) . turnout) <$> (hmd ^. #electionData)
    _ <- K.addHvega Nothing Nothing
         $ FV.multiHistogram @DShare @BR.Year "Dem Share" Nothing 50 FV.DataMinMax True mhStyle vcDist
         $ FT.mutate (FT.recordSingleton @DShare . (*100). dShare) <$> (hmd ^. #electionData)
    BR.brAddMarkDown "## Predictors/Predicted: Correlations in house elections 2012-2018"
    let corrSet = S.fromList [FV.LabeledCol "% Under 45" (F.rgetField @BRE.FracUnder45)
                             ,FV.LabeledCol "% Female" (F.rgetField @BRE.FracFemale)
                             ,FV.LabeledCol "% Grad" (F.rgetField @BRE.FracGrad)
                             ,FV.LabeledCol "% NonWhite" (F.rgetField @BRE.FracNonWhite)
                             ,FV.LabeledCol "Avg. Income" (F.rgetField @DT.AvgIncome)
                             ,FV.LabeledCol "Density" (F.rgetField @DT.PopPerSqMile)
                             ,FV.LabeledCol "Incumbency" (realToFrac . F.rgetField @BRE.Incumbency)
                             ,FV.LabeledCol "Turnout" turnout
                             ,FV.LabeledCol "D Share" dShare
                             ]
    corrChart <- K.knitEither
                 $ FV.frameCorrelations
                 "Correlations among predictors & predicted (election data only)"
                 (FV.ViewConfig 600 600 10)
                 False
                 corrSet
                 (hmd ^. #electionData)
    _ <- K.addHvega Nothing Nothing corrChart
    K.logLE K.Info "run model(s)"
  K.newPandoc
    (K.PandocInfo "compare_predictors" $ one ("pagetitle","Compare Predictors"))
    $ comparePredictors False $ K.liftActionWithCacheTime houseData_C
  K.newPandoc
    (K.PandocInfo "compare_data_sets" $ one ("pagetitle","Compare Data Sets"))
    $ compareData False $ K.liftActionWithCacheTime houseData_C
  K.newPandoc
    (K.PandocInfo "examine_fit" $ one ("pagetitle","Examine Fit"))
    $ examineFit False $ K.liftActionWithCacheTime houseData_C
  K.newPandoc
    (K.PandocInfo "compare_models" $ one ("pagetitle","Compare Models"))
    $ compareModels False $ K.liftActionWithCacheTime houseData_C

writeCompareScript :: K.KnitEffects r => [SC.ModelRunnerConfig] -> Text -> K.Sem r ()
writeCompareScript configs compareScriptName = do
  let modelDir = "/Users/adam/BlueRipple/research/stan/house/election"
  writeFileText (toString $ modelDir <> "/R/" <> compareScriptName <> ".R")
    $ SR.compareScript configs 10 Nothing

compareModels :: forall r. (K.KnitOne r, K.CacheEffectsD r) => Bool -> K.ActionWithCacheTime r BRE.HouseModelData  -> K.Sem r ()
compareModels clearCached houseData_C = do
  let predictors = ["Incumbency","PopPerSqMile","PctNonWhite", "PctGrad"]
      models =
        [ ("betaBinomialInc", Nothing, BRE.UseElectionResults, BRE.betaBinomialInc, 500)
        , ("binomial", Nothing, BRE.UseElectionResults, BRE.binomial, 500)
        ]
      isYear year = (== year) . F.rgetField @BR.Year
      year = 2018
      runOne x =
        BRE.runHouseModel
        clearCached
        predictors
        x
        year
        (fmap (Optics.over #electionData (F.filterFrame (isYear year))
                . Optics.over #ccesData (F.filterFrame (isYear year)))
          houseData_C
        )
  results <- traverse runOne models
  looDeps <- K.knitMaybe "No results (compareModels)"
             $ fmap Semigroup.sconcat $ nonEmpty $ (fmap (const ()) . fst <$> results) -- unit dependency on newest of results
  fLoo_C <- BR.retrieveOrMakeFrame "model/house/modelsLoo.bin" looDeps $ const $ do
    K.logLE K.Info "model run(s) are newer than loo comparison data.  Re-running comparisons."
    writeCompareScript (snd <$> results) ("compareModels_" <> show year)
    K.liftKnit $ SR.compareModels (zip ((\(n,_,_,_,_) -> n) <$> models) (snd <$> results)) 10
  fLoo <- K.ignoreCacheTime fLoo_C
  BR.brAddRawHtmlTable
    "Predictor LOO (Leave-One-Out Cross Validatiion) comparison"
    mempty
    (BR.toCell mempty () "" BR.textToStyledHtml <$> SR.looTextColonnade 2)
    (reverse $ sortOn (F.rgetField @SR.ELPD_Diff) $ FL.fold FL.list fLoo)
  return ()

comparePredictors :: forall r. (K.KnitOne r, K.CacheEffectsD r) => Bool -> K.ActionWithCacheTime r BRE.HouseModelData  -> K.Sem r ()
comparePredictors clearCached houseData_C = do
  let predictors = [("IDRE",["Incumbency", "PopPerSqMile", "PctNonWhite", "PctGrad"])
                   ,("IRE", ["Incumbency", "PctNonWhite", "PctGrad"])
                   ,("IDR", ["Incumbency", "PopPerSqMile", "PctNonWhite"])
                   ,("IDE", ["Incumbency", "PopPerSqMile", "PctGrad"])
                   ,("Race", ["PctNonWhite"])
                   ,("Incumbency", ["Incumbency"])
                   ,("Density", ["PopPerSqMile"])
                   ,("Education", ["PctGrad"])
                   ,("Income", ["AvgIncome"])
                   ,("IntOnly", [])
                   ]
      isYear year = (== year) . F.rgetField @BR.Year
      year = 2018
      runOne x =
        BRE.runHouseModel
        clearCached
        (snd x)
        ("betaBinomialInc", Just $ fst x, BRE.UseElectionResults, BRE.betaBinomialInc, 500)
        year
        (fmap (Optics.over #electionData (F.filterFrame (isYear year))
                . Optics.over #ccesData (F.filterFrame (isYear year)))
          houseData_C
        )
  results <- traverse runOne predictors
  looDeps <- K.knitMaybe "No results (comparePredictors)"
             $ fmap Semigroup.sconcat $ nonEmpty $ (fmap (const ()) . fst <$> results) -- unit dependency on newest of results
  fLoo_C <- BR.retrieveOrMakeFrame "model/house/predictorsLoo.bin" looDeps $ const $ do
    K.logLE K.Info "model run(s) are newer than loo comparison data.  Re-running comparisons."
    writeCompareScript (snd <$> results) ("comparePredictors_" <> show year)
    K.liftKnit $ SR.compareModels (zip (fst <$> predictors) (snd <$> results)) 10
  fLoo <- K.ignoreCacheTime fLoo_C
  BR.brAddRawHtmlTable
    "Predictor LOO (Leave-One-Out Cross Validatiion) comparison"
    mempty
    (BR.toCell mempty () "" BR.textToStyledHtml <$> SR.looTextColonnade 2)
    (reverse $ sortOn (F.rgetField @SR.ELPD_Diff) $ FL.fold FL.list fLoo)
  return ()


compareData :: forall r. (K.KnitOne r, K.CacheEffectsD r) => Bool -> K.ActionWithCacheTime r BRE.HouseModelData -> K.Sem r ()
compareData clearCached houseData_C = do
  let predictors = ["Incumbency", "PopPerSqMile", "PctGrad", "PctNonWhite"]
      years = [2012, 2014, 2016, 2018]
      modelWiths = [BRE.UseElectionResults, BRE.UseCCES, BRE.UseBoth]
      runYear mw y =
        BRE.runHouseModel
        clearCached
        predictors
        ("betaBinomialInc", Nothing, mw, BRE.betaBinomialInc, 500)
        y
        (fmap (Optics.over #electionData (F.filterFrame (isYear y))
                . Optics.over #ccesData (F.filterFrame (isYear y)))
          houseData_C
        )
      isYear year = (== year) . F.rgetField @BR.Year
      nameType l =
        let (name, t) = T.splitAt (T.length l - 1) l
        in case t of
             "D" -> Right (name, "D Pref")
             "V" -> Right (name, "Turnout")
             _ -> Left $ "Bad last character in delta label (" <> l <> ")."
      expandInterval :: (T.Text, [Double]) -> Either T.Text (MapRow.MapRow GV.DataValue) --T.Text (T.Text, [(T.Text, Double)])
      expandInterval (l, vals) = do
        (name, t) <- nameType l
        case vals of
          [lo, mid, hi] -> Right $ M.fromList [("Name", GV.Str name), ("Type", GV.Str t), ("lo", GV.Number $ 100 * (lo - mid)), ("mid", GV.Number $ 100 * mid), ("hi", GV.Number $ 100 * (hi - mid))]
          _ -> Left "Wrong length list in what should be a (lo, mid, hi) interval"
      expandMapRow f (y, modelResults)
        = fmap (M.insert "Year" (GV.Str $ show y)) <$> traverse expandInterval (M.toList $ f modelResults)
      modelMR mw = one ("Model", GV.Str $ show @Text mw)
      modelAndDeltaMR mw d = modelMR mw <> one ("Delta",GV.Str d)
      runModelWith mw = do
        results_C <- sequenceA <$> traverse (fmap fst . runYear mw) years
        results <- zip years <$> K.ignoreCacheTime results_C
        sigmaDeltaMapRows <- fmap (<> modelAndDeltaMR mw "Std. Dev") <<$>> K.knitEither (traverse (expandMapRow BRE.sigmaDeltas) results)
        unitDeltaMapRows <- fmap (<> modelAndDeltaMR mw "Min/Max") <<$>> K.knitEither (traverse (expandMapRow BRE.unitDeltas) results)
        avgProbMapRows <- fmap (<> modelAndDeltaMR mw "Avg") <<$>> K.knitEither (traverse (expandMapRow BRE.avgProbs) results)
        return $ concat $ sigmaDeltaMapRows <> unitDeltaMapRows <> avgProbMapRows
  results <- mconcat <$> traverse runModelWith modelWiths
  let dataValueAsText :: GV.DataValue -> Text
      dataValueAsText (GV.Str x) = x
      dataValueAsText _ = error "Non-string given to dataValueAsString"
  _ <- K.addHvega Nothing Nothing
       $ modelChart
       "Average Probability"
       ["probV", "probD"]
       ["UseElectionResults", "UseCCES", "UseBoth"]
       (FV.ViewConfig 200 200 5)
       ""
       $ filter (maybe False ((== "Avg") . dataValueAsText) . M.lookup "Delta") results
  _ <- K.addHvega Nothing Nothing
       $ modelChart
       "Change in Probability for 1 std dev change in predictor (1/2 below avg to 1/2 above) (with 90% confidence bands)"
       predictors
       ["UseElectionResults", "UseCCES", "UseBoth"]
       (FV.ViewConfig 200 200 5)
       "D Pref"
       $ filter (maybe False ((== "Std. Dev") . dataValueAsText) . M.lookup "Delta") results
  _ <- K.addHvega Nothing Nothing
       $ modelChart
       "Change in Probability for full range of predictor (with 90% confidence bands)"
       predictors
       ["UseElectionResults", "UseCCES", "UseBoth"]
       (FV.ViewConfig 200 200 5)
       "D Pref"
       $ filter (maybe False ((== "Min/Max") . dataValueAsText) . M.lookup "Delta") results
--    K.logLE K.Info $ show results
  return ()

modelChart :: (Functor f, Foldable f) => T.Text -> [Text] -> [Text] -> FV.ViewConfig -> T.Text -> f (MapRow.MapRow GV.DataValue) -> GV.VegaLite
modelChart title predOrder modelOrder vc t rows =
  let vlData = MapRow.toVLData M.toList [GV.Parse [("Year", GV.FoDate "%Y")]] rows
      encX = GV.position GV.X [GV.PName "Year", GV.PmType GV.Temporal]
      encY = GV.position GV.Y [GV.PName "mid", GV.PmType GV.Quantitative, axis{-, scale-}]
      axis = GV.PAxis [GV.AxNoTitle  {-, GV.AxValues (GV.Numbers [-15, -10, -5, 0, 5, 10, 15])-}]
      scale = GV.PScale [GV.SDomain $ GV.DNumbers [-12, 12]]
      encYLo = GV.position GV.YError [GV.PName "lo", GV.PmType GV.Quantitative]
      encYHi = GV.position GV.YError2 [GV.PName "hi", GV.PmType GV.Quantitative]
      encColor = GV.color [GV.MName "Type", GV.MmType GV.Nominal]
      encL = GV.encoding . encX . encY . encColor
      encB = GV.encoding . encX . encYLo . encY . encYHi . encColor
      markBand = GV.mark GV.ErrorBand [GV.MTooltip GV.TTData]
      markLine = GV.mark GV.Line []
      markPoint = GV.mark GV.Point [GV.MTooltip GV.TTData]
      specBand = GV.asSpec [encB [], markBand]
      specLine = GV.asSpec [encL [], markLine]
      specPoint = GV.asSpec [encL [], markPoint]
      spec = GV.asSpec [GV.layer [specBand, specLine, specPoint]]
      facet = GV.facet [GV.ColumnBy [GV.FName "Model", GV.FmType GV.Nominal, GV.FSort [GV.CustomSort $ GV.Strings modelOrder]]
                       ,GV.RowBy [GV.FName "Name", GV.FmType GV.Nominal, GV.FSort [GV.CustomSort $ GV.Strings predOrder]]
                       ]
   in FV.configuredVegaLite vc [FV.title title, facet, GV.specification spec, vlData]

examineFit :: forall r. (K.KnitOne r, K.CacheEffectsD r) => Bool -> K.ActionWithCacheTime r BRE.HouseModelData -> K.Sem r ()
examineFit clearCached houseData_C = do
  let predictors = ["Incumbency","PopPerSqMile","PctGrad", "PctNonWhite"]
      model = ("betaBinomialInc", Nothing, BRE.UseBoth, BRE.betaBinomialInc, 500)
      isYear year = (== year) . F.rgetField @BR.Year
      year = 2018
      runOne x =
        BRE.runHouseModel
        clearCached
        predictors
        x
        year
        (fmap (Optics.over #electionData (F.filterFrame (isYear year))
                . Optics.over #ccesData (F.filterFrame (isYear year)))
          houseData_C
        )
  results <- runOne model
  electionData <- K.ignoreCacheTime
                  $ fmap (F.rcast @[BR.StateAbbreviation, BR.CongressionalDistrict, BRE.FracGrad, BRE.FracNonWhite])
                  . F.filterFrame (isYear year)
                  . BRE.electionData
                  <$> houseData_C
  electionFit <- K.ignoreCacheTime $ fmap BRE.electionFit . fst $ results
  let (fitWithDemo, missing) =  FJ.leftJoinWithMissing @[BR.StateAbbreviation, BR.CongressionalDistrict] electionFit electionData
  unless (null missing) $ K.knitError "Missing keys in electionFit/electionData join"
  _ <- K.addHvega Nothing Nothing
       $ fitScatter1
       "Test"
       (FV.ViewConfig 800 800 10)
       $ fmap F.rcast fitWithDemo
  _ <- K.addHvega Nothing Nothing
       $ fitScatter2
       "Test"
       (FV.ViewConfig 800 800 10)
       $ fmap F.rcast fitWithDemo
--  BR.logFrame fitWithDemo
  return ()

fitScatter1 :: (Functor f, Foldable f)
           => Text
           -> FV.ViewConfig
           -> f (F.Record ([BR.StateAbbreviation
                           , BR.CongressionalDistrict
                           , BRE.TVotes
                           , BRE.DVotes
                           , BRE.EDVotes5
                           , BRE.EDVotes
                           , BRE.EDVotes95
                           , BRE.FracGrad
                           , BRE.FracNonWhite]))
           -> GV.VegaLite
fitScatter1 title vc rows =
  let toVLDataRec = FV.useColName FV.textAsVLStr
                    V.:& FV.asVLStrViaShow "District"
                    V.:& FV.asVLNumber "Votes"
                    V.:& FV.useColName FV.asVLNumber
                    V.:& FV.useColName FV.asVLNumber
                    V.:& FV.useColName FV.asVLNumber
                    V.:& FV.useColName FV.asVLNumber
                    V.:& FV.asVLData (GV.Number . (*100)) "% Grad"
                    V.:& FV.asVLData (GV.Number . (*100)) "% Non-White"
                    V.:& V.RNil
      dat = FV.recordsToData toVLDataRec rows
      encX = GV.position GV.X [GV.PName "% Grad", GV.PmType GV.Quantitative]
      encY = GV.position GV.Y [GV.PName "% Non-White", GV.PmType GV.Quantitative]
      calcFitDiff = GV.calculateAs ("(datum.DVotes - datum.EstDVotes)/datum.Votes") "actual - fit (D Share)"
      calcShare = GV.calculateAs ("datum.DVotes/datum.Votes - 0.5") "D Share"
      transform = GV.transform . calcFitDiff . calcShare
      encColor = GV.color [GV.MName "D Share", GV.MmType GV.Quantitative]
      encSize = GV.size [GV.MName "actual - fit (D Share)", GV.MmType GV.Quantitative]
      enc = GV.encoding . encX . encY . encColor . encSize
      mark = GV.mark GV.Circle [GV.MTooltip GV.TTData]
  in FV.configuredVegaLite vc [FV.title title, transform [], enc [], mark, dat]

fitScatter2 :: (Functor f, Foldable f)
           => Text
           -> FV.ViewConfig
           -> f (F.Record ([BR.StateAbbreviation
                           , BR.CongressionalDistrict
                           , BRE.TVotes
                           , BRE.DVotes
                           , BRE.EDVotes5
                           , BRE.EDVotes
                           , BRE.EDVotes95
                           , BRE.FracGrad
                           , BRE.FracNonWhite]))
           -> GV.VegaLite
fitScatter2 title vc rows =
  let toVLDataRec = FV.useColName FV.textAsVLStr
                    V.:& FV.asVLStrViaShow "District"
                    V.:& FV.asVLNumber "Votes"
                    V.:& FV.useColName FV.asVLNumber
                    V.:& FV.useColName FV.asVLNumber
                    V.:& FV.useColName FV.asVLNumber
                    V.:& FV.useColName FV.asVLNumber
                    V.:& FV.asVLData (GV.Number . (*100)) "% Grad"
                    V.:& FV.asVLData (GV.Number . (*100)) "% Non-White"
                    V.:& V.RNil
      dat = FV.recordsToData toVLDataRec rows
      calcFitDiff = GV.calculateAs ("(datum.DVotes - datum.EstDVotes)/datum.Votes") "actual - fit (D Share)"
      calcShare = GV.calculateAs ("datum.DVotes/datum.Votes - 0.5") "D Share"
      transform = GV.transform . calcFitDiff . calcShare
      encX = GV.position GV.X [GV.PName "D Share", GV.PmType GV.Quantitative]
      encY = GV.position GV.Y [GV.PName "actual - fit (D Share)", GV.PmType GV.Quantitative]
      encColor = GV.color [GV.MName "% Grad", GV.MmType GV.Quantitative]
      encSize = GV.size [GV.MName "% Non-White", GV.MmType GV.Quantitative]
      enc = GV.encoding . encX . encY . encColor . encSize
      mark = GV.mark GV.Circle [GV.MTooltip GV.TTData]
  in FV.configuredVegaLite vc [FV.title title, transform [], enc [], mark, dat]

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

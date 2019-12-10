{-# LANGUAGE DataKinds                 #-}
{-# LANGUAGE DeriveDataTypeable        #-}
{-# LANGUAGE DeriveGeneric             #-}
{-# LANGUAGE PolyKinds                 #-}
{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE GADTs                     #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE TypeApplications          #-}
{-# LANGUAGE TypeOperators             #-}
{-# LANGUAGE QuasiQuotes               #-}
{-# LANGUAGE AllowAmbiguousTypes       #-}
{-# LANGUAGE TupleSections             #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# OPTIONS_GHC  -fplugin=Polysemy.Plugin  #-}

module MRP.Intro (post) where

import qualified Control.Foldl                 as FL
import qualified Data.Array                    as A
import qualified Data.List as L
import qualified Data.Set as S
import qualified Data.Map                      as M
--import  Data.Ord (Compare)
import           Data.Proxy (Proxy(..))
import qualified Data.Text                     as T
import qualified Data.Serialize                as SE
import qualified Data.Vector.Storable               as VS


import           Graphics.Vega.VegaLite.Configuration as FV
import qualified Graphics.Vega.VegaLite.Compat as FV
import qualified Frames as F
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V

import qualified Frames.Transform              as FT
import qualified Frames.Folds                  as FF
import qualified Frames.MapReduce              as MR
import qualified Frames.Enumerations           as FE
import qualified Frames.Utils                  as FU

import qualified Frames.Visualization.VegaLite.Data
                                               as FV
import qualified Frames.Visualization.VegaLite.ParameterPlots
                                               as FV                                               

import qualified Graphics.Vega.VegaLite        as GV
import qualified Knit.Report                   as K
import qualified Polysemy.Error                as P (mapError)
import qualified Polysemy                      as P (raise)
import           Text.Pandoc.Error             as PE
import qualified Text.Blaze.Colonnade          as BC

import           Data.String.Here               ( here, i )

import qualified Colonnade                     as C
import qualified Text.Blaze.Colonnade          as BC
import qualified Text.Blaze.Html               as BH
import qualified Text.Blaze.Html5.Attributes   as BHA

import           BlueRipple.Configuration 
import           BlueRipple.Utilities.KnitUtils 
import           BlueRipple.Utilities.TableUtils 
--import           BlueRipple.Data.DataFrames 

import qualified Data.IndexedSet               as IS
import qualified Numeric.GLM.ProblemTypes      as GLM
import qualified Numeric.GLM.ModelTypes      as GLM
import qualified Numeric.GLM.FunctionFamily    as GLM
import           Numeric.GLM.MixedModel        as GLM
import qualified Numeric.GLM.Bootstrap            as GLM
import qualified Numeric.GLM.Report            as GLM
import qualified Numeric.GLM.Predict            as GLM
import qualified Numeric.GLM.Confidence            as GLM
import qualified Numeric.SparseDenseConversions as SD

import qualified Statistics.Types              as ST
import GHC.Generics (Generic)


import BlueRipple.Data.DataFrames
import qualified BlueRipple.Data.DemographicTypes as BR
import MRP.Common
import MRP.CCES

import qualified PreferenceModel.Common as PrefModel

brIntro :: T.Text
brIntro = [i|
In our research pieces so far, we've looked only at aggregate data, that
is data which comes from adding together a large number of people: census
counts or election results, for example.  In this piece we look at some
per-person data, namely the CCES survey, which surveys about 60,000 people
in every election cycle. For each survey response,
the CCES includes geographic and demographic information along with opinion about
various political questions, whether the person is registered to vote, and whether they
voted and who for in elections for Governor, House, Senate and President, whenever
each is applicable.

The geographic information makes this interesting.  This allows
us to start estimating a variety of things at the state level, something we couldn't do using
only aggregate data.  We do this using multi-level regression
(the "MR" of "MRP", which stands for Multi-level Regression with Post-stratification), a technique
we explain in more detail [here][MRP:Methods].

Following up on our earlier posts about
[changing voter preference][PrefModel:AcrossTime] and
the [white working class][PrefModel:WWCV] (WWC),
we look again at WWC voters in 2016 and
2018, this time estimating voter preference in each state and looking at
the changes, with a particular focus on the battleground states in the
mid-west: Indiana, Michigan, Ohio, Pennsylvania and Wisconsin. 

[MRP:Methods]: <${brGithubUrl (postPath PostMethods)}>
[PrefModel:WWCV]: <${brGithubUrl (PrefModel.postPath PrefModel.PostWWCV)}>
[PrefModel:AcrossTime]: <${brGithubUrl (PrefModel.postPath PrefModel.PostAcrossTime)}>
|]

glmErrorToPandocError :: GLM.GLMError -> PE.PandocError
glmErrorToPandocError x = PE.PandocSomeError $ T.pack $ show x

type GroupCols = '[StateAbbreviation]
type CCESGroup = Proxy GroupCols
  
post :: (K.KnitOne r, K.Member GLM.RandomFu r, K.Member GLM.Async r)
     => M.Map T.Text T.Text -- state names from state abbreviations
     -> K.CachedRunnable r [F.Record CCES_MRP]
     -> K.Sem r ()
post stateNameByAbbreviation ccesRecordListAllCR = P.mapError glmErrorToPandocError $ K.wrapPrefix "Intro" $ do
  K.logLE K.Info $ "Working on Intro post..."                                                                                
  let isWWC r = (F.rgetField @SimpleRace r == BR.White) && (F.rgetField @SimpleEducation r == BR.NonGrad)
      countWWCDemHouseVotesF = MR.concatFold
                               $ weightedCountFold @ByCCESPredictors @CCES_MRP @'[HouseVoteParty,CCESWeightCumulative]
                               ((== VP_Democratic) . F.rgetField @HouseVoteParty)
                               (F.rgetField @CCESWeightCumulative)
      countWWCDemPres2016VotesF = MR.concatFold
                                  $ weightedCountFold @ByCCESPredictors @CCES_MRP @'[Pres2016VoteParty,CCESWeightCumulative]
                                  ((== VP_Democratic) . F.rgetField @Pres2016VoteParty)
                                  (F.rgetField @CCESWeightCumulative)
      modelWWCV :: (K.KnitOne r, K.Member GLM.RandomFu r, K.Member GLM.Async r)
                => FL.Fold (F.Record CCES_MRP) (F.FrameRec (ByCCESPredictors V.++ '[Count, WeightedSuccesses, MeanWeight, VarWeight]))
                -> Int
                -> F.FrameRec CCES_MRP
                -> K.Sem r (GLM.MixedModel CCESPredictor CCESGroup
                           , GLM.RowClassifier CCESGroup
                           , GLM.EffectsByGroup CCESGroup CCESPredictor
                           , GLM.BetaU
                           , VS.Vector Double
                           , [(GLM.BetaU, VS.Vector Double)]) 
      modelWWCV cf y ccesFrameAll = P.mapError glmErrorToPandocError $ K.wrapPrefix ("modelWWCV " <> (T.pack $ show y) <> ":") $ do
        let recFilter r = (F.rgetField @Turnout r == T_Voted) && (F.rgetField @Year r == y)            
            ccesFrame = F.filterFrame recFilter ccesFrameAll
            counted = FL.fold FL.list $ FL.fold cf (fmap F.rcast ccesFrame)
            vCounts  = VS.fromList $ fmap (F.rgetField @Count) counted
            designEffect mw vw = 1 + (vw / (mw * mw))
            vWeights  = VS.fromList $ fmap (\r ->
                                               let mw = F.rgetField @MeanWeight r
                                                   vw = F.rgetField @VarWeight r
                                               in 1 / sqrt (designEffect mw vw) 
                                           ) counted -- VS.replicate (VS.length vCounts) 1.0
            fixedEffects = GLM.FixedEffects $ IS.fromList [GLM.Intercept, GLM.Predictor P_WWC]
            groups = IS.fromList [Proxy]
            (observations, fixedEffectsModelMatrix, rcM) = FL.fold
              (lmePrepFrame getFractionWeighted fixedEffects groups ccesPredictor (recordToGroupKey @GroupCols)) counted
            regressionModelSpec = GLM.RegressionModelSpec fixedEffects fixedEffectsModelMatrix observations
        rowClassifier <- case rcM of
          Left msg -> K.knitError msg
          Right x -> return x
        let effectsByGroup = M.fromList [(Proxy, IS.fromList [GLM.Intercept, GLM.Predictor P_WWC])]
        fitSpecByGroup <- GLM.fitSpecByGroup fixedEffects effectsByGroup rowClassifier        
        let lmmControls = GLM.LMMControls GLM.LMM_BOBYQA 1e-6
            lmmSpec = GLM.LinearMixedModelSpec (GLM.MixedModelSpec regressionModelSpec fitSpecByGroup) lmmControls
            cc = GLM.PIRLSConvergenceCriterion GLM.PCT_Deviance 1e-6 20
            glmmControls = GLM.GLMMControls GLM.UseCanonical 10 cc
            glmmSpec = GLM.GeneralizedLinearMixedModelSpec lmmSpec vWeights (GLM.Binomial vCounts) glmmControls
            mixedModel = GLM.GeneralizedLinearMixedModel glmmSpec
        randomEffectsModelMatrix <- GLM.makeZ fixedEffectsModelMatrix fitSpecByGroup rowClassifier
        let randomEffectCalc = GLM.RandomEffectCalculated randomEffectsModelMatrix (GLM.makeLambda fitSpecByGroup)
            th0 = GLM.setCovarianceVector fitSpecByGroup 1 0
            mdVerbosity = MDVNone
        GLM.checkProblem mixedModel randomEffectCalc
        K.logLE K.Info "Fitting data..."
        ((th, pd, sigma2, betaU, vb, cs), vMuSol, cf) <- GLM.minimizeDeviance mdVerbosity ML mixedModel randomEffectCalc th0
        GLM.report mixedModel randomEffectsModelMatrix (GLM.bu_vBeta betaU) (SD.toSparseVector vb)          
        let fes = GLM.fixedEffectStatistics mixedModel sigma2 cs betaU
        K.logLE K.Diagnostic $ "FixedEffectStatistics: " <> (T.pack $ show fes)
        epg <- GLM.effectParametersByGroup rowClassifier effectsByGroup vb
        K.logLE K.Diagnostic $ "EffectParametersByGroup: " <> (T.pack $ show epg)
        gec <- GLM.effectCovariancesByGroup effectsByGroup mixedModel sigma2 th      
        K.logLE K.Diagnostic $ "EffectCovariancesByGroup: " <> (T.pack $ show gec)
        rebl <- GLM.randomEffectsByLabel epg rowClassifier
        K.logLE K.Diagnostic
          $  "Random Effects:\n"
          <> GLM.printRandomEffectsByLabel rebl
        smCondVar <- GLM.conditionalCovariances mixedModel
                     cf
                     randomEffectCalc
                     th
                     betaU
        K.logLE K.Info "Bootstrappping for confidence intervals..."
        bootstraps <- GLM.parametricBootstrap mdVerbosity
                      ML
                      mixedModel
                      randomEffectCalc
                      cf
                      th
                      vMuSol
                      (sqrt sigma2)
                      200
                      True
        let GLM.FixedEffectStatistics _ mBetaCov = fes                      
        let f r = do
              let obs = getFractionWeighted r
              bootWCI <- GLM.predictWithCI
                         mixedModel
                         (Just . ccesPredictor r)
                         (Just . recordToGroupKey @GroupCols r)
                         rowClassifier
                         effectsByGroup
                         betaU
                         vb
                         (ST.mkCL 0.95)
                         (GLM.BootstrapCI GLM.BCI_Accelerated bootstraps)
                
              predictCVCI <- GLM.predictWithCI
                             mixedModel
                             (Just . ccesPredictor r)
                             (Just . recordToGroupKey @GroupCols r)
                             rowClassifier
                             effectsByGroup
                             betaU
                             vb
                             (ST.mkCL 0.95)
                             (GLM.NaiveCondVarCI mBetaCov smCondVar)

              return (r, obs, bootWCI, predictCVCI)
        fitted <- traverse f (FL.fold FL.list counted)
        K.logLE K.Diagnostic $ "Fitted:\n" <> (T.intercalate "\n" $ fmap (T.pack . show) fitted)
        fixedEffectTable <- GLM.printFixedEffects fes
        K.logLE K.Diagnostic $ "FixedEffects:\n" <> fixedEffectTable
        let toPredict = [("WWC (all States)", M.fromList [(P_WWC, 1)], M.empty)
                        ,("Non-WWC (all States)", M.fromList [(P_WWC, 0)], M.empty)
                        ]
            GLM.FixedEffectStatistics fep _ = fes            
        predictionTable <- GLM.printPredictions mixedModel fep epg rowClassifier toPredict
        K.logLE K.Diagnostic $ "Predictions:\n" <> predictionTable
--        return (mixedModel, fes, epg, rowClassifier, bootstraps)
        return (mixedModel, rowClassifier, effectsByGroup, betaU, vb, bootstraps) -- fes, epg, rowClassifier, bootstraps)
--  wwcModelByYear <- M.fromList <$> (traverse (\y -> (modelWWCV y >>= (\x -> return (y,x)))) $ [2016,2018])
--  (mm2016p, (GLM.FixedEffectStatistics fep2016p _), epg2016p, rc2016p) <- K.knitRetrieveOrMake "mrp/intro/demPres2016.bin" $ modelWWCV countWWCDemPres2016VotesF 2016
  let --makeTableRows :: K.Sem r [WWCTableRow]
      makeWWCTableRows = do
        ccesFrameAll <- F.toFrame <$> P.raise (K.useCached ccesRecordListAllCR)
        (mm2016p, rc2016p, ebg2016p, bu2016p, vb2016p, bs2016p) <- modelWWCV countWWCDemPres2016VotesF 2016 ccesFrameAll
        (mm2016, rc2016, ebg2016, bu2016, vb2016, bs2016) <- modelWWCV countWWCDemHouseVotesF 2016 ccesFrameAll
        (mm2018, rc2018, ebg2018, bu2018, vb2018, bs2018) <- modelWWCV countWWCDemHouseVotesF 2018 ccesFrameAll
        let states = FL.fold FL.set $ fmap (F.rgetField @StateAbbreviation) ccesFrameAll
            toPredict = [("National", M.empty)] <> (fmap (\s -> (s,M.singleton Proxy (s F.&: V.RNil))) $ S.toList states)
            wwc = M.singleton P_WWC 1
            addPredictions (s, groupMap) = P.mapError glmErrorToPandocError $ do
              let cl = ST.mkCL 0.95
                  ct bs = GLM.BootstrapCI GLM.BCI_Accelerated bs
              p2016p <- GLM.predictWithCI mm2016p (flip M.lookup wwc) (flip M.lookup groupMap) rc2016p ebg2016p bu2016p vb2016p cl (ct bs2016p)
              p2016 <- GLM.predictWithCI mm2016 (flip M.lookup wwc) (flip M.lookup groupMap) rc2016 ebg2016 bu2016 vb2016 cl (ct bs2016)
              p2018 <- GLM.predictWithCI mm2018 (flip M.lookup wwc) (flip M.lookup groupMap) rc2018 ebg2018 bu2018 vb2018 cl (ct bs2018)
              return  $ WWCTableRow s p2016p p2016 p2018
        L.sortOn deltaHouse <$> traverse addPredictions toPredict
  forWWCTable <-  K.retrieveOrMake "mrp/intro/wwcPrefTable" makeWWCTableRows -- cache this so we don't need to redo
  forWWCChart <-  GLM.eitherToSem $ traverse (\tr -> do
                                            let sa = stateAbbr tr
                                            fullState <- maybe (Left $ "Couldn't find " <> sa) Right $ M.lookup sa stateNameByAbbreviation
                                            return (fullState, significantDeltaHouse tr)) $ filter (\tr -> (stateAbbr tr) /= "National") forWWCTable
  brAddMarkDown brIntro
  _ <- K.addHvega Nothing Nothing $ (vlPctStateChoropleth "Significant Change in WWC Dem Voter Preference 2016 to 2018" (FV.ViewConfig 800 400 10) forWWCChart)
  let mwBG = ["IA", "MI", "OH", "PA", "WI"]
  brAddRawHtmlTable
    "WWC Democratic Voter Preference"
    (BHA.class_ "brTable")
    (colWWC_Change $ emphasizeStates mwBG <> emphasizeNational) forWWCTable

  brAddMarkDown brReadMore

data WWCTableRow = WWCTableRow { stateAbbr :: T.Text
                               , pref2016Pres :: (Double, (Double, Double))
                               , pref2016House :: (Double, (Double, Double))
                               , pref2018House :: (Double, (Double, Double))
                               } deriving (Generic)

instance SE.Serialize WWCTableRow
                   
deltaHouse tr = fst (pref2018House tr) - fst (pref2016House tr)
inStates s tr = stateAbbr tr `elem` s
                
emphasizeStates s = CellStyle (\tr _ -> if inStates s tr then highlightCellBlue else "")
emphasizeNational = CellStyle (\tr _ -> if  stateAbbr tr == "National" then highlightCellPurple else "")

significantGivenCI :: (Double, Double) -> (Double, Double) -> Double
significantGivenCI (loA, hiA) (loB, hiB) =
  let maxLo = max loA loB
      minHi = min hiA hiB
      overlap = max 0 (minHi - maxLo)
      shortest = min (hiA - loA) (hiB - loB)
  in overlap / shortest

significantDeltaHouse tr = if signifWWCT tr < 0.25 then deltaHouse tr else 0

signifWWCT x = significantGivenCI (snd $ pref2016House x) (snd $ pref2018House x)
                    
colWWC_Change :: CellStyle WWCTableRow T.Text -> C.Colonnade C.Headed WWCTableRow BC.Cell
colWWC_Change cas =
  C.headed "State" (toCell cas "State" "State" (textToStyledHtml . stateAbbr))
  <> C.headed "2016 Pres (%)" (toCell cas "2016 Pres" "2016 Pres" (numberToStyledHtml "%2.1f" . (*100) . fst . pref2016Pres))
  <> C.headed "2016 House (%)" (toCell cas "2016 House" "2016 House" (numberToStyledHtml "%2.1f" . (*100) . fst . pref2016House))
  <> C.headed "Low (95%)" (toCell cas "Low (95%)" "Low (95%)" (numberToStyledHtml "%2.1f" . (*100) . fst . snd . pref2016House))
  <> C.headed "High (95%)" (toCell cas "High (95%)" "High (95%)" (numberToStyledHtml "%2.1f" . (*100) . snd . snd . pref2016House))
  <> C.headed "2018 House (%)" (toCell cas "2018 D Pref" "2018 House" (numberToStyledHtml "%2.1f" . (*100) . fst . pref2018House))
  <> C.headed "2018-2016 House" (toCell cas "2018-2016 House" "2018-2016 House" (numberToStyledHtml "%2.1f" . (*100) . deltaHouse))
  <> C.headed "CI Overlap" (toCell cas "CI Overlap" "CI Overlap" (numberToStyledHtml "%2.1f" . (*100) . signifWWCT))

usStatesTopoJSONUrl = "https://cdn.jsdelivr.net/npm/us-atlas@3/states-10m.json"
usStatesAlbersTopoJSONUrl = "https://cdn.jsdelivr.net/npm/us-atlas@3/states-albers-10m.json"

-- Lessons learned
-- The data is the geography
-- transform with lookup (properties.name) to add data
-- to figure out the right lookup name you can just plot the geography (no transform, no encoding) and look at loaded data
-- in vega editor
vlPctStateChoropleth :: Foldable f => T.Text -> FV.ViewConfig -> f (T.Text, Double) -> GV.VegaLite
vlPctStateChoropleth title vc stateData =
  let datGeo = GV.dataFromUrl usStatesTopoJSONUrl [GV.TopojsonFeature "states"]
      datVal = GV.dataFromRows [] $ concat $ fmap (\(s,x) -> GV.dataRow [("state", GV.Str s),("value", GV.Number x)] []) $ FL.fold FL.list stateData
      dataSets = GV.datasets [("stateNums",datVal)]
      projection = GV.projection [GV.PrType GV.AlbersUsa]
      transform = GV.transform . GV.lookup "properties.name" datVal "state" ["state","value"]
      mark = GV.mark GV.Geoshape []
      colorEnc = GV.color [GV.MName "value", GV.MmType GV.Quantitative, GV.MScale [GV.SScheme "redyellowgreen" [], GV.SDomain (GV.DNumbers [-0.5,0.5])]]
      tooltip = GV.tooltips [[GV.TName "state", GV.TmType GV.Nominal],[GV.TName "value", GV.TmType GV.Quantitative, GV.TFormat ".0%"]]      
      enc = GV.encoding .  colorEnc . tooltip
  in FV.configuredVegaLite vc [FV.title title, datGeo, mark, projection, transform [], enc []]

vlTest :: FV.ViewConfig -> GV.VegaLite
vlTest vc =
  let
    datGeo = GV.dataFromUrl usStatesTopoJSONUrl [GV.TopojsonFeature "states"]
    projection = GV.projection [GV.PrType GV.AlbersUsa]
    mark = GV.mark GV.Geoshape [GV.MFill "lightgrey" ]
  in FV.configuredVegaLite vc [datGeo, mark, projection]

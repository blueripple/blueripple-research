{-# LANGUAGE DataKinds                 #-}
{-# LANGUAGE DeriveDataTypeable        #-}
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

import qualified Data.Text                     as T
import qualified Data.Vector.Storable               as VS


import           Graphics.Vega.VegaLite.Configuration as FV
import qualified Graphics.Vega.VegaLite.Compat as FV
import qualified Frames as F
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
import qualified Numeric.GLM.Report            as GLM
import qualified Numeric.SparseDenseConversions as SD

import BlueRipple.Data.DataFrames
import MRP.Common
import MRP.CCES

import qualified PreferenceModel.Common as PrefModel

brIntro :: T.Text
brIntro = [i|
In our research pieces so far, we;ve looked only at aggregate data, that
is data which comes from adding together a large number of people: census
counts or election results, for example.  In this piece we look at some
per-person data, namely the CCES survey, which surveys about 60,000 people
in every election cycle. For each survey response,
the CCES includes geographic and demographic information along with opinion about
various political questions, whether the person is registered to vote, whether they
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
glmErrorToPandocError x = PE.PandocSomeError $ show x
  
post :: K.KnitOne r
     => M.Map T.Text T.Text -- state names from state abbreviations
     -> F.FrameRec CCES_MRP
     -> K.Sem r ()
post stateNameByAbbreviation ccesFrameAll = P.mapError glmErrorToPandocError $ K.wrapPrefix "Intro" $ do
  K.logLE K.Info $ "Working on Intro post..."                                                                                
  let isWWC r = (F.rgetField @WhiteNonHispanic r == True) && (F.rgetField @CollegeGrad r == False)
      countWWCDemHouseVotesF = MR.concatFold
                               $ countFold @ByStateGenderRaceEducation @_ @'[HouseVoteParty] ((== VP_Democratic) . F.rgetField @HouseVoteParty)
      countWWCDemPres2016VotesF = MR.concatFold
                                  $ countFold @ByStateGenderRaceEducation @_ @'[Pres2016VoteParty] ((== VP_Democratic) . F.rgetField @Pres2016VoteParty)
      modelWWCV :: (K.KnitOne r)
                => FL.Fold (F.Record CCES_MRP) (F.FrameRec (ByStateGenderRaceEducation V.++ '[Count, Successes]))
                -> Int
                -> K.Sem r (GLM.MixedModel CCESPredictor CCESGroup
                           ,GLM.FixedEffectStatistics CCESPredictor
                           ,GLM.EffectParametersByGroup CCESGroup CCESPredictor
                           ,GLM.RowClassifier CCESGroup)
      modelWWCV cf y = K.wrapPrefix ("modelWWCV " <> (T.pack $ show y) <> ":") $ P.mapError glmErrorToPandocError $ do
        let recFilter r = (F.rgetField @Turnout r == T_Voted) && (F.rgetField @Year r == y)
            ccesFrame = F.filterFrame recFilter ccesFrameAll
            counted = FL.fold FL.list $ FL.fold cf (fmap F.rcast ccesFrame)
            vCounts  = VS.fromList $ fmap (F.rgetField @Count) counted
            vWeights  = VS.replicate (VS.length vCounts) 1.0
            fixedEffects = GLM.FixedEffects $ IS.fromList [GLM.Intercept, GLM.Predictor P_WWC]
            groups = IS.fromList [G_State]
            (observations, fixedEffectsModelMatrix, rcM) = FL.fold
              (lmePrepFrame getFraction fixedEffects groups ccesPredictor ccesGroupLabels) counted
            regressionModelSpec = GLM.RegressionModelSpec fixedEffects fixedEffectsModelMatrix observations
        rowClassifier <- case rcM of
          Left msg -> K.knitError msg
          Right x -> return x
        let effectsByGroup = M.fromList [(G_State, IS.fromList [GLM.Intercept, GLM.Predictor P_WWC])]
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
        (th, pd, sigma2, betaU, b, cs) <- GLM.minimizeDeviance mdVerbosity ML mixedModel randomEffectCalc th0
        GLM.report mixedModel randomEffectsModelMatrix (GLM.bu_vBeta betaU) (SD.toSparseVector b)          
        let fes = GLM.fixedEffectStatistics mixedModel sigma2 cs betaU
        K.logLE K.Diagnostic $ "FixedEffectStatistics: " <> (T.pack $ show fes)
        epg <- GLM.effectParametersByGroup rowClassifier effectsByGroup b
        K.logLE K.Diagnostic $ "EffectParametersByGroup: " <> (T.pack $ show epg)
        gec <- GLM.effectCovariancesByGroup effectsByGroup mixedModel sigma2 th      
        K.logLE K.Diagnostic $ "EffectCovariancesByGroup: " <> (T.pack $ show gec)
        rebl <- GLM.randomEffectsByLabel epg rowClassifier
        K.logLE K.Diagnostic
          $  "Random Effects:\n"
          <> GLM.printRandomEffectsByLabel rebl
        let f r = do
              let obs = getFraction r
              fitted <- GLM.fitted mixedModel
                        ccesPredictor
                        ccesGroupLabels
                        fes
                        epg
                        rowClassifier
                        r
              return (r, obs, fitted)
        fitted <- traverse f (FL.fold FL.list counted)
        K.logLE K.Diagnostic $ "Fitted:\n" <> (T.intercalate "\n" $ fmap (T.pack . show) fitted)
        fixedEffectTable <- GLM.printFixedEffects fes
        K.logLE K.Diagnostic $ "FixedEffects:\n" <> fixedEffectTable
        let toPredict = [("WWC (all States)", M.fromList [(P_WWC, 1)], M.empty)
                        ,("Non-WWC (all States)", M.fromList [(P_WWC, 0)], M.empty)
                        ]
        predictionTable <- GLM.printPredictions mixedModel fes epg rowClassifier toPredict
        K.logLE K.Diagnostic $ "Predictions:\n" <> predictionTable
        return (mixedModel, fes, epg, rowClassifier)
--  wwcModelByYear <- M.fromList <$> (traverse (\y -> (modelWWCV y >>= (\x -> return (y,x)))) $ [2016,2018])
  (mm2016p, fes2016p, epg2016p, rc2016p) <- modelWWCV countWWCDemPres2016VotesF 2016
  (mm2016, fes2016, epg2016, rc2016) <- modelWWCV countWWCDemHouseVotesF 2016
  (mm2018, fes2018, epg2018, rc2018) <- modelWWCV countWWCDemHouseVotesF 2018
  let states = FL.fold FL.set $ fmap (F.rgetField @StateAbbreviation) ccesFrameAll
      toPredict = [("National", M.empty)] <> (fmap (\s -> (s,M.singleton G_State s)) $ S.toList states)
      wwc = M.singleton P_WWC 1
      mwBG = ["IA", "MI", "OH", "PA", "WI"]
      addPredictions (s,groupMap) = do
        p2016p <- fst <$> (GLM.predict mm2016p (flip M.lookup wwc) (flip M.lookup groupMap) fes2016p epg2016p rc2016p)
        p2016 <- fst <$> (GLM.predict mm2016 (flip M.lookup wwc) (flip M.lookup groupMap) fes2016 epg2016 rc2016)
        p2018 <- fst <$> (GLM.predict mm2018 (flip M.lookup wwc) (flip M.lookup groupMap) fes2018 epg2018 rc2018)
        return  $ WWCTableRow s p2016p p2016 p2018  
  forTable <-  L.sortOn deltaHouse <$> traverse addPredictions toPredict
  forChart <-  GLM.eitherToSem $ traverse (\tr -> do
                                            let sa = stateAbbr tr
                                            fullState <- maybe (Left $ "Couldn't find " <> sa) Right $ M.lookup sa stateNameByAbbreviation
                                            return (fullState, deltaHouse tr)) $ filter (\tr -> (stateAbbr tr) /= "National") forTable
  brAddMarkDown brIntro
  _ <- K.addHvega Nothing Nothing $ (vlPctStateChloropleth "Test" (FV.ViewConfig 800 400 10) forChart)
  brAddRawHtmlTable
    "WWC Democratic Voter Preference"
    (BHA.class_ "brTable")
    (colWWC_Change $ emphasizeStates mwBG <> emphasizeNational) forTable

  brAddMarkDown brReadMore

data WWCTableRow = WWCTableRow { stateAbbr :: T.Text
                               , pref2016Pres :: Double
                               , pref2016House :: Double
                               , pref2018House :: Double
                               }
                   
deltaHouse tr = pref2018House tr - pref2016House tr 
inStates s tr = stateAbbr tr `elem` s
                
emphasizeStates s = CellStyle (\tr _ -> if inStates s tr then highlightCellBlue else "")
emphasizeNational = CellStyle (\tr _ -> if  stateAbbr tr == "National" then highlightCellPurple else "")
                                         
colWWC_Change :: CellStyle WWCTableRow T.Text -> C.Colonnade C.Headed WWCTableRow BC.Cell
colWWC_Change cas =
  C.headed "State" (toCell cas "State" "State" (textToStyledHtml . stateAbbr))
  <> C.headed "2016 Pres (%)" (toCell cas "2016 Pres" "2016 Pres" (numberToStyledHtml "%2.1f" . (*100) . pref2016Pres))
  <> C.headed "2016 House (%)" (toCell cas "2016 House" "2016 House" (numberToStyledHtml "%2.1f" . (*100) . pref2016House))
  <> C.headed "2018 House (%)" (toCell cas "2018 D Pref" "2018 House" (numberToStyledHtml "%2.1f" . (*100) . pref2018House))
  <> C.headed "2018-2016 House" (toCell cas "2018-2016 House" "2018-2016 House" (numberToStyledHtml "%2.1f" . (*100) . deltaHouse))
  

usStatesTopoJSONUrl = "https://cdn.jsdelivr.net/npm/us-atlas@3/states-10m.json"
usStatesAlbersTopoJSONUrl = "https://cdn.jsdelivr.net/npm/us-atlas@3/states-albers-10m.json"

-- Lessons learned
-- The data is the geography
-- transform with lookup (properties.name) to add data
-- to figure out the right lookup name you can just plot the geography (no transform, no encoding) and look at loaded data
-- in vega editor
vlPctStateChloropleth :: Foldable f => T.Text -> FV.ViewConfig -> f (T.Text, Double) -> GV.VegaLite
vlPctStateChloropleth title vc stateData =
  let datGeo = GV.dataFromUrl usStatesTopoJSONUrl [GV.TopojsonFeature "states"]
      datVal = GV.dataFromRows [] $ concat $ fmap (\(s,x) -> GV.dataRow [("state", GV.Str s),("value", GV.Number x)] []) $ FL.fold FL.list stateData
      dataSets = GV.datasets [("stateNums",datVal)]
      projection = GV.projection [GV.PrType GV.AlbersUsa]
      transform = GV.transform . GV.lookup "properties.name" datVal "state" ["state","value"]
      mark = GV.mark GV.Geoshape []
      tooltip = GV.tooltips [[GV.TName "state", GV.TmType GV.Nominal],[GV.TName "value", GV.TmType GV.Quantitative, GV.TFormat ".0%"]]
      enc = GV.encoding . GV.color [GV.MName "value", GV.MmType GV.Quantitative, GV.MScale [GV.SScheme "redyellowgreen" []]] . tooltip
  in FV.configuredVegaLite vc [FV.title title, datGeo, mark, projection, transform [], enc []]

vlTest :: FV.ViewConfig -> GV.VegaLite
vlTest vc =
  let
    datGeo = GV.dataFromUrl usStatesTopoJSONUrl [GV.TopojsonFeature "states"]
    projection = GV.projection [GV.PrType GV.AlbersUsa]
    mark = GV.mark GV.Geoshape [GV.MFill "lightgrey" ]
  in FV.configuredVegaLite vc [datGeo, mark, projection]

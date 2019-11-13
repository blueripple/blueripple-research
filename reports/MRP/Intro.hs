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
{-# OPTIONS_GHC  -fplugin=Polysemy.Plugin  #-}

module MRP.Intro (post) where

import qualified Control.Foldl                 as FL
import qualified Data.Map                      as M
import qualified Data.Array                    as A

import qualified Data.Text                     as T
import qualified Data.Vector.Storable               as VS


import           Graphics.Vega.VegaLite.Configuration as FV
import qualified Frames as F

import qualified Frames.Transform              as FT
import qualified Frames.Folds                  as FF
import qualified Frames.MapReduce              as MR
import qualified Frames.Enumerations           as FE
import qualified Frames.Utils                  as FU

import qualified Frames.Visualization.VegaLite.Data
                                               as FV
import qualified Frames.Visualization.VegaLite.ParameterPlots
                                               as FV                                               

import qualified Knit.Report                   as K
import qualified Polysemy.Error                as P (mapError)
import           Text.Pandoc.Error             as PE

import           Data.String.Here               ( here, i )

import           BlueRipple.Configuration
import           BlueRipple.Utilities.KnitUtils
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

brIntro :: T.Text
brIntro = [i|
So far in our research pieces we've looked only at *aggregate* data, that is data
|]

glmErrorToPandocError :: GLM.GLMError -> PE.PandocError
glmErrorToPandocError x = PE.PandocSomeError $ show x
  
post :: K.KnitOne r
     => F.FrameRec CCES_MRP
     -> K.Sem r ()
post ccesFrameAll = P.mapError glmErrorToPandocError $ K.wrapPrefix "Intro" $ do
  K.logLE K.Info $ "Working on Intro post..."    
  let countVotedByStateAndGenderF = MR.concatFold
                                    $ countFold @ByStateGender @[StateAbbreviation, Gender, HouseVoteParty] @'[HouseVoteParty] ((== VP_Democratic) . F.rgetField @HouseVoteParty)
                                    
      isWWC r = (F.rgetField @WhiteNonHispanic r == True) && (F.rgetField @CollegeGrad r == False)
      countWWCDemVotesF = MR.concatFold
                          $ countFold @ByStateGenderRaceEducation @[StateAbbreviation, Gender, WhiteNonHispanic, CollegeGrad, HouseVoteParty] @'[HouseVoteParty] ((== VP_Democratic) . F.rgetField @HouseVoteParty) 
      modelWWCV (y :: Int) = K.wrapPrefix ("modelWWCV " <> (T.pack $ show y) <> ":") $ do
        let recFilter r = (F.rgetField @Turnout r == T_Voted) && (F.rgetField @Year r == y)
            ccesFrame = F.filterFrame recFilter ccesFrameAll
            counted = FL.fold FL.list $ FL.fold countWWCDemVotesF (fmap F.rcast ccesFrame)
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
  wwcModelByYear <- M.fromList <$> (traverse (\y -> (modelWWCV y >>= (\x -> return (y,x)))) $ [2016,2018])
  brAddMarkDown brIntro  
  brAddMarkDown brReadMore

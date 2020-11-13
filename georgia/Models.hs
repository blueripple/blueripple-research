{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-# OPTIONS_GHC  -O0 #-}
module Models where

import qualified Data as GA
import qualified GA_DataFrames as GA

--import qualified BlueRipple.Data.DataFrames as BR
--import qualified BlueRipple.Data.Loaders as BR
--import qualified BlueRipple.Utilities.KnitUtils as BR
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.ElectionTypes as ET
import qualified BlueRipple.Data.ACS_PUMS as PUMS
import qualified BlueRipple.Utilities.KnitUtils as BR

import qualified Control.Foldl as FL
import qualified Data.IntMap.Strict as IM
import qualified Data.Map as M
import qualified Data.Maybe as Maybe

import qualified Data.Aeson.Encoding as A
import qualified Data.Text as T
import qualified Frames as F
import qualified Data.Vector as Vec
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V

import qualified Frames.MapReduce as FMR
import qualified Frames.Serialize as FS
import qualified Frames.Transform as FT
import qualified Frames.SimpleJoins as FJ

import qualified CmdStan as CS
import qualified CmdStan.Types as CS
import qualified Stan.JSON as SJ
import qualified Stan.Frames as SF
import qualified Stan.Parameters as SP
import qualified Stan.ModelRunner as SM
import qualified Stan.ModelBuilder as SB
import qualified Stan.ModelConfig as SC
import qualified System.Environment as Env

import qualified Knit.Report as K
import Data.String.Here (here)

type GASenateDataWrangler = SC.DataWrangler (F.FrameRec GA.SenateByCounty) () ()

gaSenateModelDataWrangler :: GASenateDataWrangler
gaSenateModelDataWrangler = SC.Wrangle SC.NoIndex f where
--  dataIndex = SC.CacheableIndex $ \c -> "georgia/stan/index/" <> (SC.mrcOutputPrefix c) <> ".bin"
  enumCountyF = FL.premap (F.rgetField @GA.County) (SJ.enumerate 1)
  f :: F.FrameRec GA.SenateByCounty -> ((), F.FrameRec GA.SenateByCounty -> Either T.Text A.Series)
  f sbc = ((), makeDataJsonE) where
    (countyM, toCounty) = FL.fold enumCountyF sbc
    makeDataJsonE :: F.FrameRec GA.SenateByCounty -> Either T.Text A.Series
    makeDataJsonE x = SJ.frameToStanJSONSeries dataF sbc where
      predictRow :: F.Record GA.SenateByCounty -> Vec.Vector Double
      predictRow r = Vec.fromList $ F.recToList $ F.rcast @[GA.FracYoung, GA.FracGrad, GA.FracBlack, GA.FracLatinX, GA.FracAsian, DT.PopPerSqMile] r 
      dataF = SJ.namedF "G" FL.length -- number of counties
              <> SJ.constDataF "K" (6 :: Int) -- number of predictors, not including intercept
              <> SJ.valueToPairF "county" (SJ.jsonArrayMF (countyM . F.rgetField @GA.County))
              <> SJ.valueToPairF "X" (SJ.jsonArrayF predictRow)
              <> SJ.valueToPairF "VAP" (SJ.jsonArrayF (F.rgetField @PUMS.Citizens))
              <> SJ.valueToPairF "DVotes1" (SJ.jsonArrayF (F.rgetField @GA.DVotes1))
              <> SJ.valueToPairF "DVotes2" (SJ.jsonArrayF (F.rgetField @GA.DVotes2))
              <> SJ.valueToPairF "TVotes1" (SJ.jsonArrayF (F.rgetField @GA.TVotes1))
              <> SJ.valueToPairF "TVotes2" (SJ.jsonArrayF (F.rgetField @GA.TVotes2))
              

type VoteP = "TurnoutP" F.:-> Double
type DVoteP = "DVoteP" F.:-> Double
type ETVotes = "ETVotes" F.:-> Int
type EDVotes = "EDVotes" F.:-> Int

type Modeled = [VoteP, DVoteP, ETVotes, EDVotes]

extractResults :: CS.StanSummary               
               -> F.FrameRec GA.SenateByCounty
               -> Either T.Text (F.FrameRec ('[GA.County] V.++ Modeled))
extractResults summary senateByCounty = do
  pVotedP <- fmap CS.mean <$> SP.parse1D "pVotedP" (CS.paramStats summary)
  pDVoteP <- fmap CS.mean <$> SP.parse1D "pDVoteP" (CS.paramStats summary)
  let probList = zip (FL.fold FL.list pVotedP) (FL.fold FL.list pDVoteP)
      makeRow (sbcR, (pV, pD)) = F.rgetField @GA.County sbcR
                                 F.&: pV
                                 F.&: pD
                                 F.&: round (pV * realToFrac (F.rgetField @PUMS.Citizens sbcR))
                                 F.&: round (pD * pV * realToFrac (F.rgetField @PUMS.Citizens sbcR))
                                 F.&: V.RNil
      result = F.toFrame $ fmap makeRow $ zip (FL.fold FL.list senateByCounty) probList
  return result
      

runSenateModel :: (K.KnitEffects r,  K.CacheEffectsD r)
               => GASenateDataWrangler
               -> (T.Text, SB.StanModel)
               -> K.ActionWithCacheTime r (F.FrameRec GA.SenateByCounty)
               -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec ('[GA.County] V.++ Modeled)))
runSenateModel dw (modelName, model) senateByCounty_C = do
  senateByCounty <- K.ignoreCacheTime senateByCounty_C
  let stancConfig = (SM.makeDefaultStancConfig (T.unpack $ "georgia/stan/senateByCounty/" <> modelName)) { CS.useOpenCL = False }
  stanConfig <- SC.noLogOfSummary
                <$> SM.makeDefaultModelRunnerConfig
                "georgia/stan/senateByCounty"
                (modelName <> "_model")
                (Just (SB.NoLL, model))
                (Just $ "senateByCounty.json")
                (Just $ "senateByCounty_" <> modelName <> "_model")
                8
                (Just 1000)
                (Just 4000)
                (Just stancConfig)
  let resultCacheKey = "georgia/model/stan/senateByCounty_" <> modelName <> ".bin"
  modelDep <- SM.modelCacheTime stanConfig
  let dataModelDep = const <$> modelDep <*> senateByCounty_C
      getResults s () inputAndIndex_C = do
        (input, _) <- K.ignoreCacheTime inputAndIndex_C
        predictions <- K.knitEither $ extractResults s input
        return predictions
  BR.retrieveOrMakeFrame resultCacheKey dataModelDep  $ \() -> do
    K.logLE K.Info "Data or model newer than last cached result. (Re)-running..."
    SM.runModel stanConfig SM.Both dw (SC.UseSummary getResults) () senateByCounty_C


model_v1 :: SB.StanModel
model_v1 = SB.StanModel
           binomialDataBlock
           Nothing
           binomialParametersBlock
           Nothing
           binomialModelBlock
           (Just binomialGeneratedQuantitiesBlock)
           binomialGQLLBlock

binomialDataBlock :: SB.DataBlock
binomialDataBlock = [here|  
  int<lower = 1> G; // number of counties
  int<lower = 1> K; // number of predictors
  int<lower = 1, upper = G> county[G]; // do we need this?
  matrix[G, K] X;
  int<lower = 0> VAP[G];
  int<lower = 0> DVotes1[G];
  int<lower = 0> DVotes2[G];
  int<lower = 0> TVotes1[G];
  int<lower = 0> TVotes2[G];
|]

binomialParametersBlock :: SB.ParametersBlock
binomialParametersBlock = [here|
  real alphaD;                             
  vector[K] betaV;
  real alphaV;
  vector[K] betaD;
|]

binomialModelBlock :: SB.ModelBlock
binomialModelBlock = [here|
  alphaD ~ normal(0, 2);
  alphaV ~ normal(0, 2);
  betaV ~ normal(0, 1);
  betaD ~ normal(0, 1);
  TVotes1 ~ binomial_logit(VAP, alphaV + X * betaV);
//  TVotes2 ~ binomial_logit(VAP, alphaV + X * betaV);
  DVotes1 ~ binomial_logit(TVotes1, alphaD + X * betaD);
//  DVotes2 ~ binomial_logit(TVotes2, alphaD + X * betaD);
|]

binomialGeneratedQuantitiesBlock :: SB.GeneratedQuantitiesBlock
binomialGeneratedQuantitiesBlock = [here|
  vector<lower = 0, upper = 1>[G] pVotedP;
  vector<lower = 0, upper = 1>[G] pDVoteP;
  pVotedP = inv_logit(alphaV + (X * betaV));
  pDVoteP = inv_logit(alphaD + (X * betaD));
|]

binomialGQLLBlock :: SB.GeneratedQuantitiesBlock
binomialGQLLBlock = [here|
  vector[G] log_lik;
  log_lik = binomial_logit_lpmf(DVotes1 | TVotes1, alphaD + X * betaD);
//  for (g in 1:G) {
//    log_lik[g] =  binomial_logit_lpmf(DVotes1[g] | TVotes1[g], alphaD + X[g] * beta + aState[state[g]]);
//  }
|]   

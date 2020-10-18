{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-# OPTIONS_GHC  -O0 #-}

module BlueRipple.Model.StanCCES where

import qualified Control.Foldl as FL
import qualified Data.IntMap.Strict as IM
import qualified Data.Map as M
import qualified Data.Maybe as Maybe

import qualified Data.Text as T
import qualified Frames as F
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V

import qualified Frames.MapReduce as FMR
import qualified Frames.Serialize as FS

import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Utilities.KnitUtils as BR
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.ElectionTypes as ET
import qualified BlueRipple.Model.MRP as BR
import qualified BlueRipple.Data.CCES as CCES
import qualified BlueRipple.Model.CCES_MRP_Analysis as CCES
import qualified BlueRipple.Data.Keyed as BK
import qualified BlueRipple.Utilities.KnitUtils as BR

import qualified CmdStan as CS
import qualified CmdStan.Types as CS
import qualified Stan.JSON as SJ
import qualified Stan.Parameters as SP
import qualified Stan.ModelRunner as SM
import qualified Stan.ModelBuilder as SB
import qualified Stan.ModelConfig as SC
import qualified System.Environment as Env

import qualified Knit.Report as K
import Data.String.Here (here)


type CCES_CountRow = '[BR.StateAbbreviation] V.++ DT.CatColsASER5 V.++ BR.CountCols
ccesDataWrangler :: SC.DataWrangler
                    (F.FrameRec CCES_CountRow)
                    (IM.IntMap T.Text, IM.IntMap (F.Rec FS.SElField DT.CatColsASER5))
ccesDataWrangler cces = ((toState, fmap FS.toS toCategory), makeJsonE) where
  enumSexF = SJ.enumerateField (T.pack . show) (SJ.enumerate 1) (F.rgetField @DT.SexC)
  enumAgeF = SJ.enumerateField (T.pack . show) (SJ.enumerate 1) (F.rgetField @DT.SimpleAgeC)
  enumEducationF = SJ.enumerateField (T.pack . show) (SJ.enumerate 1) (F.rgetField @DT.CollegeGradC)
  enumRaceF = SJ.enumerateField (T.pack . show) (SJ.enumerate 1) (F.rgetField @DT.Race5C)
  enumStateF = SJ.enumerateField id (SJ.enumerate 1) (F.rgetField @BR.StateAbbreviation)
  enumCategoryF = SJ.enumerateField (T.pack . show) (SJ.enumerate 1) (F.rcast @DT.CatColsASER5)
  -- do outer enumeration fold for indices
  enumF = (,,,,,)
    <$> enumSexF
    <*> enumAgeF
    <*> enumEducationF
    <*> enumRaceF
    <*> enumStateF
    <*> enumCategoryF
  ((sexF, toSex)
    , (ageF, toAge)
    , (educationF, toEducation)
    , (raceF, toRace)
    , (stateF, toState)
    , (categoryF, toCategory)) = FL.fold enumF cces
  makeJsonE x = SJ.frameToStanJSONEncoding dataF cces where
    dataF = SJ.namedF "G" FL.length
            <> SJ.constDataF "J_state" (IM.size toState)
            <> SJ.constDataF "J_sex" (IM.size toSex)
            <> SJ.constDataF "J_age" (IM.size toAge)
            <> SJ.constDataF "J_educ" (IM.size toEducation)
            <> SJ.constDataF "J_race" (IM.size toRace)
--          <> SJ.valueToPairF "sex" sexF
--          <> SJ.valueToPairF "age" ageF
--          <> SJ.valueToPairF "education" educationF
--          <> SJ.valueToPairF "race" raceF
            <> SJ.valueToPairF "category" categoryF
            <> SJ.valueToPairF "state" stateF
            <> SJ.valueToPairF "D_votes" (SJ.jsonArrayF $ (round @_ @Int . F.rgetField @BR.WeightedSuccesses))
            <> SJ.valueToPairF "Total_votes" (SJ.jsonArrayF $ F.rgetField @BR.Count)
  

countCCESASER5 :: K.KnitEffects r => ET.OfficeT -> Int -> F.FrameRec CCES.CCES_MRP -> K.Sem r (F.FrameRec CCES_CountRow)
countCCESASER5 office year ccesMRP = do
  countFold <- K.knitEither $ case (office, year) of
    (ET.President, 2008) -> Right $ CCES.countDemPres2008VotesF @DT.CatColsASER5
    (ET.President, 2012) -> Right $ CCES.countDemPres2012VotesF @DT.CatColsASER5
    (ET.President, 2016) -> Right $ CCES.countDemPres2016VotesF @DT.CatColsASER5
    (ET.House, y) -> Right $  CCES.countDemHouseVotesF @DT.CatColsASER5 y
    _ -> Left $ T.pack (show office) <> "/" <> (T.pack $ show year) <> " not available."
  let addZerosF =  FMR.concatFold $ FMR.mapReduceFold
                   (FMR.noUnpack)
                   (FMR.splitOnKeys @'[BR.StateAbbreviation])
                   ( FMR.makeRecsWithKey id
                     $ FMR.ReduceFold
                     $ const
                     $ BK.addDefaultRec @DT.CatColsASER5 BR.zeroCount )
      counted = FL.fold countFold ccesMRP
      countedWithZeros = FL.fold addZerosF counted
  return countedWithZeros

prefASER5_MR :: forall r.(K.KnitEffects r,  K.CacheEffectsD r)
             => ET.OfficeT
             -> Int
             -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec
                                                  ( '[BR.StateAbbreviation]
                                                    V.++
                                                    DT.CatColsASER5
                                                    V.++
                                                    '[BR.Year, ET.Office, ET.DemVPV, BR.DemPref]
                                                  )))
prefASER5_MR office year = do
  -- count data
  let officeYearT = (T.pack $ show office) <> "_" <> (T.pack $ show year)
      countCacheKey = "data/stan/cces/stateVotesASER5_" <> officeYearT <> ".bin"
  cces_C <- CCES.ccesDataLoader
  ccesASER5_C <- BR.retrieveOrMakeFrame countCacheKey cces_C $ countCCESASER5 office year
  let resultsWithStates summary cachedDataIndex = do
        (_, (toState, toCategoryS)) <- K.ignoreCacheTime cachedDataIndex
        let toCategory = fmap FS.fromS toCategoryS
        stateProbs <- fmap CS.mean <$> (K.knitEither $ SP.parse2D "stateProbs" (CS.paramStats summary))
        -- build rows to left join
        let (states, cats) = SP.getDims stateProbs
            indices = [(stateI, catI) | stateI <- [1..states], catI <- [1..cats]]
            makeRow (stateI, catI) = do
              abbr <- IM.lookup stateI toState 
              catRec <- IM.lookup catI toCategory
              let prob = SP.getIndexed stateProbs (stateI, catI)
                  vpv = 2 * prob - 1
                  x :: F.Record ('[BR.StateAbbreviation] V.++ DT.CatColsASER5 V.++ [BR.Year, ET.Office, ET.DemVPV, BR.DemPref])
                  x = (abbr F.&: catRec) F.<+> (year F.&: office F.&: vpv F.&: prob F.&: V.RNil)
              return x
        probRows <- fmap F.toFrame
                    $ K.knitMaybe "Error looking up indices.  Should be impossible!"
                    $ traverse makeRow indices
        --BR.logFrame probRows
        return probRows       
  let stancConfig = (SM.makeDefaultStancConfig "stan/voterPref/binomial_ASER5_state") { CS.useOpenCL = False }
      model = SB.StanModel
              binomialASER5_StateDataBlock
              (Just binomialASER5_StateTransformedDataBlock)
              binomialASER5_StateParametersBlock
              Nothing
              binomialASER5_StateModelBlock
              (Just binomialASER5_StateGeneratedQuantitiesBlock)
  stanConfig <- SM.makeDefaultModelRunnerConfig
                "stan/voterPref"
                "binomial_ASER5_state_model"
                (Just model)
                (Just $ "cces_" <> officeYearT <> ".json")
                (Just $ "cces_" <> officeYearT <> "_binomial_ASER5_state_model")
                4
                (Just 1000)
                (Just 1000)
                (Just stancConfig)
  let resultCacheKey = "model/stan/cces/statePrefsASER5_" <> officeYearT <> ".bin"
  modelDep <- SM.modelCacheTime stanConfig
  let dataModelDep = const <$> modelDep <*> ccesASER5_C
  BR.retrieveOrMakeFrame resultCacheKey dataModelDep $ \() -> do
    K.logLE K.Info "Data or model newer than last cached result. Rerunning."
    SM.runModel stanConfig SM.ShinyStan ccesDataWrangler resultsWithStates ccesASER5_C


prefASER5_MR_Loo :: forall r.(K.KnitEffects r,  K.CacheEffectsD r)
             => ET.OfficeT
             -> Int
             -> K.Sem r ()
prefASER5_MR_Loo office year = do
  -- count data
  let officeYearT = (T.pack $ show office) <> "_" <> (T.pack $ show year)
      countCacheKey = "data/stan/cces/stateVotesASER5_" <> officeYearT <> ".bin"
  cces_C <- CCES.ccesDataLoader
  ccesASER5_C <- BR.retrieveOrMakeFrame countCacheKey cces_C $ countCCESASER5 office year
  let results _ _ = return ()
  let stancConfig = (SM.makeDefaultStancConfig "stan/voterPref/binomial_ASER5_state_loo") { CS.useOpenCL = False }
      model = SB.StanModel
              binomialASER5_StateDataBlock
              (Just binomialASER5_StateTransformedDataBlock)
              binomialASER5_StateParametersBlock
              Nothing
              binomialASER5_StateModelBlock
              (Just binomialASER5_StateGQLooBlock)
  stanConfig <- SM.makeDefaultModelRunnerConfig
                "stan/voterPref"
                "binomial_ASER5_state_loo"
                (Just model)
                (Just $ "cces_" <> officeYearT <> ".json")
                (Just $ "cces_" <> officeYearT <> "_binomial_ASER5_state_loo")
                4
                (Just 1000)
                (Just 1000)
                (Just stancConfig)
  SM.runModel stanConfig SM.Loo ccesDataWrangler results ccesASER5_C

prefASER5_MR_v2_Loo :: forall r.(K.KnitEffects r,  K.CacheEffectsD r)
             => ET.OfficeT
             -> Int
             -> K.Sem r ()
prefASER5_MR_v2_Loo office year = do
  -- count data
  let officeYearT = (T.pack $ show office) <> "_" <> (T.pack $ show year)
      countCacheKey = "data/stan/cces/stateVotesASER5_" <> officeYearT <> ".bin"
  cces_C <- CCES.ccesDataLoader
  ccesASER5_C <- BR.retrieveOrMakeFrame countCacheKey cces_C $ countCCESASER5 office year
  let results _ _ = return ()
  let stancConfig = (SM.makeDefaultStancConfig "stan/voterPref/binomial_ASER5_state_v2_loo") { CS.useOpenCL = False }
      model = SB.StanModel
              binomialASER5_StateDataBlock
              (Just binomialASER5_StateTransformedDataBlock)
              binomialASER5_v2_StateParametersBlock
              Nothing
              binomialASER5_v2_StateModelBlock
              (Just binomialASER5_v2_StateGQLooBlock)
  stanConfig <- SM.makeDefaultModelRunnerConfig
                "stan/voterPref"
                "binomial_ASER5_state_v2_loo"
                (Just model)
                (Just $ "cces_" <> officeYearT <> ".json")
                (Just $ "cces_" <> officeYearT <> "_binomial_ASER5_state_v2_loo")
                4
                (Just 1000)
                (Just 1000)
                (Just stancConfig)
  SM.runModel stanConfig SM.Loo ccesDataWrangler results ccesASER5_C


binomialASER5_StateDataBlock :: SB.DataBlock
binomialASER5_StateDataBlock = [here|
  int<lower = 0> G; // number of cells
  int<lower = 1> J_state; // number of states
  int<lower = 1> J_sex; // number of sex categories
  int<lower = 1> J_age; // number of age categories
  int<lower = 1> J_educ; // number of education categories
  int<lower = 1> J_race; // number of race categories  
  //  int<lower = 1, upper = J_sex> sex[G];
  //  int<lower = 1, upper = J_age> age[G];
  //  int<lower = 1, upper = J_educ> education[G];
  //  int<lower = 1, upper = J_race> race[G];
  int<lower = 1, upper = J_state> state[G];
  int<lower = 1, upper = J_age * J_sex * J_educ * J_race> category[G];
  int<lower = 0> D_votes[G];
  int<lower = 0> Total_votes[G];
|]

binomialASER5_StateTransformedDataBlock :: SB.TransformedDataBlock
binomialASER5_StateTransformedDataBlock = [here|
  int <lower=1> nCat;
  nCat =  J_age * J_sex * J_educ * J_race;
|]

binomialASER5_StateParametersBlock :: SB.ParametersBlock
binomialASER5_StateParametersBlock = [here|
  vector[nCat] beta;
  real<lower=0> sigma_alpha;
  matrix<multiplier=sigma_alpha>[J_state, nCat] alpha;
|]

binomialASER5_StateModelBlock :: SB.ModelBlock
binomialASER5_StateModelBlock = [here|
  sigma_alpha ~ normal (0, 10);
  to_vector(alpha) ~ normal (0, sigma_alpha);
  for (g in 1:G) {
   D_votes[g] ~ binomial_logit(Total_votes[g], beta[category[g]] + alpha[state[g], category[g]]);
  }
|]

binomialASER5_StateGeneratedQuantitiesBlock :: SB.GeneratedQuantitiesBlock
binomialASER5_StateGeneratedQuantitiesBlock = [here|
  vector <lower = 0, upper = 1> [nCat] nationalProbs;
  matrix <lower = 0, upper = 1> [J_state, nCat] stateProbs;
  nationalProbs = inv_logit(beta[category]);
  for (s in 1:J_state) {
    for (c in 1:nCat) {
      stateProbs[s, c] = inv_logit(beta[c] + alpha[s, c]);
    }
  }
|]

binomialASER5_StateGQLooBlock :: SB.GeneratedQuantitiesBlock
binomialASER5_StateGQLooBlock = [here|
  vector[G] log_lik;  
  for (g in 1:G) {
      log_lik[g] =  binomial_logit_lpmf(D_votes[g] | Total_votes[g], beta[category[g]] + alpha[state[g], category[g]]);
  }
|]    


binomialASER5_v2_StateParametersBlock :: SB.ParametersBlock
binomialASER5_v2_StateParametersBlock = [here|
  vector[nCat] beta;
  real<lower=0> sigma_alpha;
  vector<multiplier=sigma_alpha>[J_state] alpha;
|]

binomialASER5_v2_StateModelBlock :: SB.ModelBlock
binomialASER5_v2_StateModelBlock = [here|
  sigma_alpha ~ normal (0, 10);
  alpha ~ normal (0, sigma_alpha);
  D_votes ~ binomial_logit(Total_votes, beta[category] + alpha[state]);  
|]

binomialASER5_v2_StateGeneratedQuantitiesBlock :: SB.GeneratedQuantitiesBlock
binomialASER5_v2_StateGeneratedQuantitiesBlock = [here|
  vector <lower = 0, upper = 1> [nCat] nationalProbs;
  matrix <lower = 0, upper = 1> [J_state, nCat] stateProbs;
  nationalProbs = inv_logit(beta[category]);
  stateProbs = inv_logit(beta[category] + alpha[state])
|]

binomialASER5_v2_StateGQLooBlock :: SB.GeneratedQuantitiesBlock
binomialASER5_v2_StateGQLooBlock = [here|
  vector[G] log_lik;
  for (g in 1:G) {
    log_lik[g] =  binomial_logit_lpmf(D_votes[g] | Total_votes[g], beta[category[g]] + alpha[state[g]]);
  }
|]      

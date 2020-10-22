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
import qualified Data.Vector as Vec
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V

import qualified Frames.MapReduce as FMR
import qualified Frames.Serialize as FS

import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Data.Loaders as BR
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
import qualified Stan.Frames as SF
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
                    (F.FrameRec ('[BR.StateAbbreviation] V.++ DT.CatColsASER5))
ccesDataWrangler = SC.WrangleWithPredictions (SC.CacheableIndex $ \c -> "stan/index/" <> (SC.mrcOutputPrefix c) <> ".bin") f g where  
  enumSexF = SJ.enumerateField (T.pack . show) (SJ.enumerate 1) (F.rgetField @DT.SexC)
  enumAgeF = SJ.enumerateField (T.pack . show) (SJ.enumerate 1) (F.rgetField @DT.SimpleAgeC)
  enumEducationF = SJ.enumerateField (T.pack . show) (SJ.enumerate 1) (F.rgetField @DT.CollegeGradC)
  enumRaceF = SJ.enumerateField (T.pack . show) (SJ.enumerate 1) (F.rgetField @DT.Race5C)
  enumStateF = SJ.enumerateField id (SJ.enumerate 1) (F.rgetField @BR.StateAbbreviation)
  enumCategoryF = SJ.enumerateField (T.pack . show) (SJ.enumerate 1) (F.rcast @DT.CatColsASER5)
  -- do outer enumeration fold for indices
  f cces = ((toState, fmap FS.toS toCategory), makeJsonE) where
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
    makeJsonE x = SJ.frameToStanJSONSeries dataF cces where
      dataF = SJ.namedF "G" FL.length
              <> SJ.constDataF "J_state" (IM.size toState)
              <> SJ.constDataF "J_sex" (IM.size toSex)
              <> SJ.constDataF "J_age" (IM.size toAge)
              <> SJ.constDataF "J_educ" (IM.size toEducation)
              <> SJ.constDataF "J_race" (IM.size toRace)
              <> SJ.valueToPairF "sex" sexF
              <> SJ.valueToPairF "age" ageF
              <> SJ.valueToPairF "education" educationF
              <> SJ.valueToPairF "race" raceF
              <> SJ.valueToPairF "category" categoryF
              <> SJ.valueToPairF "state" stateF
              <> SJ.valueToPairF "D_votes" (SJ.jsonArrayF $ (round @_ @Int . F.rgetField @BR.WeightedSuccesses))
              <> SJ.valueToPairF "Total_votes" (SJ.jsonArrayF $ F.rgetField @BR.Count)
  g (toState, toCategory) toPredict = SJ.frameToStanJSONSeries predictF toPredict where
    toStateIndexM sa = M.lookup sa $ SJ.flipIntIndex toState
    toCategoryIndexM c = M.lookup c$ SJ.flipIntIndex (fmap FS.fromS toCategory)
    predictF = SJ.namedF "M" FL.length
               <> SJ.valueToPairF "predict_State" (SJ.jsonArrayMF (toStateIndexM . F.rgetField @BR.StateAbbreviation))
               <> SJ.valueToPairF "predict_Category" (SJ.jsonArrayMF (toCategoryIndexM . F.rcast @DT.CatColsASER5))
               

{-
<> SJ.valueToPairF "predict_Age" (SJ.jsonArrayMF (flip IM.lookup toAge . F.rgetField @DT.SimpleAgeC))
               <> SJ.valueToPairF "predict_Sex" (SJ.jsonArrayMF (flip IM.lookup toSex . F.rgetField @DT.SexC))
               <> SJ.valueToPairF "predict_Education" (SJ.jsonArrayMF (flip IM.lookup toEducation . F.rgetField @DT.CollegeGradC))
               <> SJ.valueToPairF "predict_Sex" (SJ.jsonArrayMF (flip IM.lookup toSex . F.rgetField @DT.SexC))
               
-}                                                  
ccesDataWrangler2 :: SC.DataWrangler
                     (F.FrameRec CCES_CountRow)
                    (IM.IntMap T.Text, M.Map SJ.IntVec (F.Rec FS.SElField DT.CatColsASER5))
                    (F.FrameRec ('[BR.StateAbbreviation] V.++ DT.CatColsASER5))
ccesDataWrangler2 = SC.WrangleWithPredictions (SC.CacheableIndex $ \c -> "stan/index/" <> (SC.mrcOutputPrefix c) <> ".bin") f g where
  enumStateF = FL.premap (F.rgetField @BR.StateAbbreviation) (SJ.enumerate 1) 
  encodeAge = SF.toRecEncoding @DT.SimpleAgeC $ SJ.dummyEncodeEnum @DT.SimpleAge
  encodeSex = SF.toRecEncoding @DT.SexC $ SJ.dummyEncodeEnum @DT.Sex
  encodeEducation = SF.toRecEncoding @DT.CollegeGradC $ SJ.dummyEncodeEnum @DT.CollegeGrad
  encodeRace = SF.toRecEncoding @DT.Race5C $ SJ.dummyEncodeEnum @DT.Race5
  encodeCatCols :: SJ.Encoding SJ.IntVec (F.Record DT.CatColsASER5)
  encodeCatCols = SF.composeIntVecRecEncodings encodeAge 
                  $ SF.composeIntVecRecEncodings encodeSex
                  $ SF.composeIntVecRecEncodings encodeEducation encodeRace
  (catColsIndexer, toCatCols) = encodeCatCols
  f cces = ((toState, fmap FS.toS toCatCols), makeJsonE) where
    (stateM, toState) = FL.fold enumStateF cces
    k = SJ.vecEncodingLength encodeCatCols 
    makeJsonE x = SJ.frameToStanJSONSeries dataF cces where
      dataF = SJ.namedF "G" FL.length
              <> SJ.constDataF "J_state" (IM.size toState)
              <> SJ.constDataF "K" k
              <> SJ.valueToPairF "X" (SJ.jsonArrayMF (\r -> catColsIndexer $ F.rcast @DT.CatColsASER5 r))
              <> SJ.valueToPairF "state" (SJ.jsonArrayMF (stateM . F.rgetField @BR.StateAbbreviation))
              <> SJ.valueToPairF "D_votes" (SJ.jsonArrayF (round @_ @Int . F.rgetField @BR.WeightedSuccesses))
              <> SJ.valueToPairF "Total_votes" (SJ.jsonArrayF (F.rgetField @BR.Count))
  g (toState, _) toPredict = SJ.frameToStanJSONSeries predictF toPredict where
    toStateIndexM sa = M.lookup sa $ SJ.flipIntIndex toState
    predictF = SJ.namedF "M" FL.length
               <> SJ.valueToPairF "predict_State" (SJ.jsonArrayMF (toStateIndexM . F.rgetField @BR.StateAbbreviation))
               <> SJ.valueToPairF "predict_X" (SJ.jsonArrayMF (catColsIndexer . F.rcast @DT.CatColsASER5)) 

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

countCCESASER5' :: K.KnitEffects r => ET.OfficeT -> Int -> F.FrameRec CCES.CCES_MRP -> K.Sem r (F.FrameRec CCES_CountRow)
countCCESASER5' office year ccesMRP = do
  countFold <- K.knitEither $ case (office, year) of
    (ET.President, 2008) -> Right $ CCES.countDemPres2008VotesF @DT.CatColsASER5
    (ET.President, 2012) -> Right $ CCES.countDemPres2012VotesF @DT.CatColsASER5
    (ET.President, 2016) -> Right $ CCES.countDemPres2016VotesF @DT.CatColsASER5
    (ET.House, y) -> Right $  CCES.countDemHouseVotesF @DT.CatColsASER5 y
    _ -> Left $ T.pack (show office) <> "/" <> (T.pack $ show year) <> " not available."
  let counted = FL.fold countFold ccesMRP
  return counted

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
  allStatesL <- do
    stateXWalk <- K.ignoreCacheTimeM $ BR.stateAbbrCrosswalkLoader
    return $ fmap (F.rgetField @BR.StateAbbreviation) .  FL.fold FL.list . F.filterFrame ((<60) . F.rgetField @BR.StateFIPS) $ stateXWalk 
  let toPredict :: F.FrameRec ('[BR.StateAbbreviation] V.++ DT.CatColsASER5)
      toPredict = F.toFrame [ s F.&: cat | s <- allStatesL, cat <- DT.allCatKeysASER5]
  cces_C <- CCES.ccesDataLoader
  ccesASER5_C <- BR.retrieveOrMakeFrame countCacheKey cces_C $ countCCESASER5 office year
  let resultsWithStates summary toPredict cachedDataIndex = do
        (_, (toState, toCategoryS)) <- K.ignoreCacheTime cachedDataIndex
        predictProbs <- fmap CS.mean <$> (K.knitEither $ SP.parse1D "predicted" (CS.paramStats summary))
        let yoRec :: F.Record '[BR.Year, ET.Office] = year F.&: office F.&: V.RNil
            probRec :: Double -> F.Record [ET.DemVPV, BR.DemPref]
            probRec x = (2*x -1 ) F.&: x F.&: V.RNil
            makeRow key prob = key F.<+> yoRec F.<+> probRec prob        
        return $ F.toFrame $ fmap (uncurry makeRow) $ zip (FL.fold FL.list toPredict) $ Vec.toList $ SP.getVector predictProbs
  let stancConfig = (SM.makeDefaultStancConfig "stan/voterPref/binomial_ASER5_state") { CS.useOpenCL = False }
  stanConfig <- SM.makeDefaultModelRunnerConfig
                "stan/voterPref"
                "binomial_ASER5_state_model"
                (Just (SB.NoLL, model_v1))
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
    SM.runModel stanConfig SM.ShinyStan ccesDataWrangler (SC.UseSummary resultsWithStates) toPredict ccesASER5_C


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
  let stancConfig = (SM.makeDefaultStancConfig "stan/voterPref/binomial_ASER5_state_loo") { CS.useOpenCL = False }
  stanConfig <- SM.makeDefaultModelRunnerConfig
                "stan/voterPref"
                "binomial_ASER5_state_loo"
                (Just (SB.OnlyLL, model_v1))
                (Just $ "cces_" <> officeYearT <> ".json")
                (Just $ "cces_" <> officeYearT <> "_binomial_ASER5_state_loo")
                4
                (Just 1000)
                (Just 1000)
                (Just stancConfig)
  SM.runModel stanConfig SM.Loo (SC.noPredictions ccesDataWrangler) SC.DoNothing () ccesASER5_C 

prefASER5_MR_v2_Loo :: forall r.(K.KnitEffects r,  K.CacheEffectsD r)
             => ET.OfficeT
             -> Int
             -> K.Sem r ()
prefASER5_MR_v2_Loo office year = do
  -- count data
  let officeYearT = (T.pack $ show office) <> "_" <> (T.pack $ show year)
      countCacheKey = "data/stan/cces/stateVotesASER5_v2" <> officeYearT <> ".bin"
  cces_C <- CCES.ccesDataLoader
  ccesASER5_C <- BR.retrieveOrMakeFrame countCacheKey cces_C $ countCCESASER5' office year
  let stancConfig = (SM.makeDefaultStancConfig "stan/voterPref/binomial_ASER5_state_v2_loo") { CS.useOpenCL = False }
  stanConfig <- SM.makeDefaultModelRunnerConfig
                "stan/voterPref"
                "binomial_ASER5_state_v2_loo"
                (Just (SB.OnlyLL, model_v2))
                (Just $ "cces_" <> officeYearT <> ".json")
                (Just $ "cces_" <> officeYearT <> "_binomial_ASER5_state_v2_loo")
                4
                (Just 1000)
                (Just 1000)
                (Just stancConfig)
  SM.runModel stanConfig SM.Loo (SC.noPredictions ccesDataWrangler) SC.DoNothing () ccesASER5_C

prefASER5_MR_v3_Loo :: forall r.(K.KnitEffects r,  K.CacheEffectsD r)
             => ET.OfficeT
             -> Int
             -> K.Sem r ()
prefASER5_MR_v3_Loo office year = do
  -- count data
  let officeYearT = (T.pack $ show office) <> "_" <> (T.pack $ show year)
      countCacheKey = "data/stan/cces/stateVotesASER5_v3" <> officeYearT <> ".bin"
  cces_C <- CCES.ccesDataLoader
  ccesASER5_C <- BR.retrieveOrMakeFrame countCacheKey cces_C $ countCCESASER5' office year
  let stancConfig = (SM.makeDefaultStancConfig "stan/voterPref/binomial_ASER5_state_v3_loo") { CS.useOpenCL = False }
  stanConfig <- SM.makeDefaultModelRunnerConfig
                "stan/voterPref"
                "binomial_ASER5_state_v3_loo"
                (Just (SB.OnlyLL, model_v3))
                (Just $ "cces_" <> officeYearT <> ".json")
                (Just $ "cces_" <> officeYearT <> "_binomial_ASER5_state_v3_loo")
                4
                (Just 1000)
                (Just 1000)
                (Just stancConfig)
  SM.runModel stanConfig SM.Loo (SC.noPredictions ccesDataWrangler) SC.DoNothing () ccesASER5_C

prefASER5_MR_v4_Loo :: forall r.(K.KnitEffects r,  K.CacheEffectsD r)
             => ET.OfficeT
             -> Int
             -> K.Sem r ()
prefASER5_MR_v4_Loo office year = do
  -- count data
  let officeYearT = (T.pack $ show office) <> "_" <> (T.pack $ show year)
      countCacheKey = "data/stan/cces/stateVotesASER5_v4" <> officeYearT <> ".bin"
  cces_C <- CCES.ccesDataLoader
  ccesASER5_C <- BR.retrieveOrMakeFrame countCacheKey cces_C $ countCCESASER5' office year
  let stancConfig = (SM.makeDefaultStancConfig "stan/voterPref/binomial_ASER5_state_v4_loo") { CS.useOpenCL = False }
  stanConfig <- SM.makeDefaultModelRunnerConfig
                "stan/voterPref"
                "binomial_ASER5_state_v4_loo"
                (Just (SB.OnlyLL, model_v4))
                (Just $ "cces_" <> officeYearT <> "_v2.json")
                (Just $ "cces_" <> officeYearT <> "_binomial_ASER5_state_v4_loo")
                4
                (Just 1000)
                (Just 1000)
                (Just stancConfig)
  SM.runModel stanConfig SM.Loo (SC.noPredictions ccesDataWrangler2) SC.DoNothing () ccesASER5_C    

model_v1 :: SB.StanModel
model_v1 = SB.StanModel
           binomialASER5_StateDataBlock
           (Just binomialASER5_StateTransformedDataBlock)
           binomialASER5_StateParametersBlock
           Nothing
           binomialASER5_StateModelBlock
           (Just binomialASER5_StateGeneratedQuantitiesBlock)
           binomialASER5_StateGQLLBlock

model_v2 :: SB.StanModel
model_v2 = SB.StanModel
           binomialASER5_StateDataBlock
           (Just binomialASER5_StateTransformedDataBlock)
           binomialASER5_v2_StateParametersBlock
           Nothing
           binomialASER5_v2_StateModelBlock
           (Just binomialASER5_v2_StateGeneratedQuantitiesBlock)
           binomialASER5_v2_StateGQLLBlock

model_v3 :: SB.StanModel
model_v3 = SB.StanModel
           binomialASER5_StateDataBlock
           (Just binomialASER5_StateTransformedDataBlock)
           binomialASER5_v3_ParametersBlock
           Nothing
           binomialASER5_v3_ModelBlock
           (Just binomialASER5_v3_GeneratedQuantitiesBlock)
           binomialASER5_v3_GQLLBlock

model_v4 :: SB.StanModel
model_v4 = SB.StanModel
           binomialASER5_v4_DataBlock
           Nothing
           binomialASER5_v4_ParametersBlock
           Nothing
           binomialASER5_v4_ModelBlock
           Nothing
           binomialASER5_v4_GQLLBlock


binomialASER5_StateDataBlock :: SB.DataBlock
binomialASER5_StateDataBlock = [here|
  int<lower = 0> G; // number of cells
  int<lower = 1> J_state; // number of states
  int<lower = 1> J_sex; // number of sex categories
  int<lower = 1> J_age; // number of age categories
  int<lower = 1> J_educ; // number of education categories
  int<lower = 1> J_race; // number of race categories  
  int<lower = 1, upper = J_state> state[G];
  int<lower = 1, upper = J_age * J_sex * J_educ * J_race> category[G];
  int<lower = 0> D_votes[G];
  int<lower = 0> Total_votes[G];
  int<lower = 0> M; // number of predictions
  int<lower = 0> predict_State[M];
  int<lower = 0> predict_Category[M];
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
  vector <lower = 0, upper = 1> [M] predicted;
  for (p in 1:M) {
    predicted[p] = inv_logit(beta[predict_Category[p]] + alpha[predict_State[p], predict_Category[p]]);
  }
|]

binomialASER5_StateGQLLBlock :: SB.GeneratedQuantitiesBlock
binomialASER5_StateGQLLBlock = [here|
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

binomialASER5_v2_StateGQLLBlock :: SB.GeneratedQuantitiesBlock
binomialASER5_v2_StateGQLLBlock = [here|
  vector[G] log_lik;
  for (g in 1:G) {
    log_lik[g] =  binomial_logit_lpmf(D_votes[g] | Total_votes[g], beta[category[g]] + alpha[state[g]]);
  }
|]      


binomialASER5_v3_ParametersBlock :: SB.ParametersBlock
binomialASER5_v3_ParametersBlock = [here|
  vector[nCat] beta;
|]

binomialASER5_v3_ModelBlock :: SB.ModelBlock
binomialASER5_v3_ModelBlock = [here|
  D_votes ~ binomial_logit(Total_votes, beta[category]);  
|]

binomialASER5_v3_GeneratedQuantitiesBlock :: SB.GeneratedQuantitiesBlock
binomialASER5_v3_GeneratedQuantitiesBlock = [here|
  vector <lower = 0, upper = 1> [nCat] nationalProbs;
  nationalProbs = inv_logit(beta[category]);
|]

binomialASER5_v3_GQLLBlock :: SB.GeneratedQuantitiesBlock
binomialASER5_v3_GQLLBlock = [here|
  vector[G] log_lik;
  for (g in 1:G) {
    log_lik[g] =  binomial_logit_lpmf(D_votes[g] | Total_votes[g], beta[category[g]]);
  }
|]

binomialASER5_v4_DataBlock :: SB.DataBlock
binomialASER5_v4_DataBlock = [here|
  int<lower = 0> G; // number of cells
  int<lower = 1> J_state; // number of states
  int<lower = 1, upper = J_state> state[G];
  int<lower = 1> K; // number of cols in predictor matrix
  matrix[G, K] X;
  int<lower = 0> D_votes[G];
  int<lower = 0> Total_votes[G];
|]

  

binomialASER5_v4_ParametersBlock :: SB.ParametersBlock
binomialASER5_v4_ParametersBlock = [here|
  real alpha;
  vector[K] beta;
|]

binomialASER5_v4_ModelBlock :: SB.ModelBlock
binomialASER5_v4_ModelBlock = [here|
    D_votes ~ binomial_logit(Total_votes, alpha + X * beta);
|]

  {-
binomialASER5_v4_GeneratedQuantitiesBlock :: SB.GeneratedQuantitiesBlock
binomialASER5_v4_GeneratedQuantitiesBlock = [here|
  matrix<lower = 0, upper = 1>[K] nationalProbs;
|]
-}
  
binomialASER5_v4_GQLLBlock :: SB.GeneratedQuantitiesBlock
binomialASER5_v4_GQLLBlock = [here|
  vector[G] log_lik;
  for (g in 1:G) {
    log_lik[g] =  binomial_logit_lpmf(D_votes[g] | Total_votes[g], alpha + X[g] * beta);
  }
|]       


  

{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-# OPTIONS_GHC  -O0 #-}

module BlueRipple.Model.StanMRP where

import qualified Control.Foldl as FL
import qualified Data.IntMap.Strict as IM
import qualified Data.Map as M
import qualified Data.Maybe as Maybe

import qualified Data.Text as T
import qualified Frames as F
import qualified Frames.InCore as FI
import qualified Data.Vector as Vec
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V
import Flat.Instances.Vector()
import Flat.Instances.Containers()

import qualified Frames.MapReduce as FMR
import qualified Frames.Serialize as FS
import qualified Frames.Transform as FT
import qualified Frames.SimpleJoins as FJ

import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Data.Loaders as BR
import qualified BlueRipple.Utilities.KnitUtils as BR
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.ElectionTypes as ET
import qualified BlueRipple.Data.CountFolds as BR
import qualified BlueRipple.Data.CCES as CCES
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
import qualified Stan.RScriptBuilder as SR
import qualified System.Environment as Env

import qualified Knit.Report as K
import Data.String.Here (here)

{-
data MRP_DataWrangler as bs b where
  MRP_ModelSubset :: (bs F.⊆ as)
    => SC.DataWrangler (F.FrameRec (as V.++ BR.CountCols)) b (F.FrameRec as)
    -> MRP_DataWrangler as bs b
  MRP_ModelAll :: SC.DataWrangler (F.FrameRec (as V.++ BR.CountCols)) b (F.FrameRec as)
               ->  MRP_DataWrangler as as b
-}

data MRPData f predRow datRow psRow where
  MRData :: Traversable f => f row -> MRPData f row row ()
  MRPData :: Traversable f => f datRow -> f psRow -> (datRow -> predRow) -> (psRow -> predRow) -> MRPData f predRow modRow psRow

modeledDataRows :: MRPData f predRow datRow psRow -> f datRow
modeledDataRows (MPRData modeledRows _ _ _)  = modeledRows
modeledDataRows (MRData modelRows) = modeledRows

modelToPred :: MRPData f predRow datRow psRow -> (datRow -> predRow)
modelToPred (MRData _) = id
modelToPred (MRPData _ _ f _) = f

data IntIndex row = IntIndex { i_Size :: Int, i_Index :: row -> Maybe Int }

type StanBuilderEnv row = StanBuilderEnv { groupIndices :: Map Text (IntIndex row) }
type StanBuilderM row a = ExceptT Text (Reader (StanBuilderEnv row)) a

runStanBuilder :: StanBuilderEnv row -> StanBuilderM row a -> Either Text a
runStanBuilder e = fmap (usingReader e) . runExceptT

stanBuildError :: Text -> StanBuilderM ()
stanBuildError t = ExceptT (pure $ Left t)

getEnv :: StanBuilderM row (StanBuilderEnv row)
getEnv =

getIndex :: Group row -> StanBuilderM row IntIndex
getIndex = do


data Group row where
  EnumeratedGroup :: Text -> IntIndex row -> Group row
  LabeledGroup :: Text -> FL.Fold row (IntIndex row) -> Group row

groupName :: Group row -> Text
groupName (EnumeratedGroup n _) = n
groupName (LabeledGroup n _) = n

groupIndex :: Foldable f => Group row -> f row -> Index row
groupIndex (EnumeratedGroup _ i) _ = f
groupIndex (LabeledGroup _ fld) rows = FL.fold fld rows

groupDataFold :: Foldable f => Group row -> StanBuilderM (StanJSONF row A.Series)
groupDataFold predRows g = do

  SJ.constDataF ("J_" <> name) indexSize <> SJ.namedF name (SJ.jsonArrayMF indexM)
  where
    name = groupName g
    Index indexSize indexM = groupIndex g predRows

groupsModelDataFold :: (Foldable f, Functor f, Foldable g) => g row -> f (Group row) -> StanJSONF row A.Series
groupsModelDataFold rows = foldMap . fmap (groupDataFold rows)

data Binomial_MRP_Model predRow modelRow =
  Binomial_MRP_Model
  {
    bmm_Name :: Text -- we'll need this for (unique) file names
  , bmm_FixedEffects :: predRow -> Vec.Vector Double
  , bmm_Groups :: [Group predRow]
  , bmm_Total :: modelRow -> Int
  , bmm_Success :: modelRow -> Int
  }

buildEnv :: Binomial_MRP_Model predRow modelRow -> MRPData f predRow datRow psRow -> StanBuilderEnv predRow
buildEnv model dat = StanBuilderEnv groupIndexMap
  where
    groupIndexMap = Map.fromList $ fmap (\g -> (groupName g, groupIndex g (modeledDataRows dat))) (bmm_Groups model)

type PostStratificationWeight psRow = psRow -> Double

binomialMRPModelDataFold :: Foldable g
                         => Binomial_MRP_Model predRow modeledRow
                         -> MRPData g predRow modeledRow psRow
                         -> StanBuilderM (StanJSONF modeledRow A.Series)
binomialMRPModelDataFold model dat =
  SJ.constDataF "N" numRows
  <> SJ.namedF "X" (SJ.jsonArrayF (bmm_FixedEffects model . modelToPred))
  <> FL.premap (modelToPred dat) $ groupsDataFold rows (bmm_Groups model)
  <> SJ.namedF "Total" (SJ.jsonArrayF (bmm_Total model))
  <> SJ.namedF "Success" (SJ.jsonArrayF (bmm_Success model))

--binomialMRPPostStratification

mrpDataWrangler :: Text -> MRP_Model -> MRP_DataWrangler as bs ()
mrpDataWrangler cacheDir model =
  MRP_DataWrangler
  $ SC.WrangleWithPredictions (SC.CacheableIndex $ \c -> cacheDir <> "stan/index" <> SC.mrcOutputPrefix c <> ".bin") f g
  where

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
              <> SJ.valueToPairF "X" (SJ.jsonArrayMF (catColsIndexer . F.rcast @DT.CatColsASER5))
              <> SJ.valueToPairF "state" (SJ.jsonArrayMF (stateM . F.rgetField @BR.StateAbbreviation))
              <> SJ.valueToPairF "D_votes" (SJ.jsonArrayF (round @_ @Int . F.rgetField @BR.WeightedSuccesses))
              <> SJ.valueToPairF "Total_votes" (SJ.jsonArrayF (F.rgetField @BR.Count))
  g (toState, _) toPredict = SJ.frameToStanJSONSeries predictF toPredict where
    toStateIndexM sa = M.lookup sa $ SJ.flipIntIndex toState
    predictF = SJ.namedF "M" FL.length
               <> SJ.valueToPairF "predict_State" (SJ.jsonArrayMF (toStateIndexM . F.rgetField @BR.StateAbbreviation))
               <> SJ.valueToPairF "predict_X" (SJ.jsonArrayMF (catColsIndexer . F.rcast @DT.CatColsASER5))


extractResults :: Int
               -> ET.OfficeT
               -> CS.StanSummary
               -> F.FrameRec (CCES_KeyRow DT.CatColsASER5)
               -> Either T.Text (F.FrameRec (CCES_KeyRow DT.CatColsASER5 V.++ '[BR.Year, ET.Office, ET.DemVPV, BR.DemPref]))
extractResults year office summary toPredict = do
   predictProbs <- fmap CS.mean <$> SP.parse1D "predicted" (CS.paramStats summary)
   let yoRec :: F.Record '[BR.Year, ET.Office] = year F.&: office F.&: V.RNil
       probRec :: Double -> F.Record [ET.DemVPV, BR.DemPref]
       probRec x = (2*x -1 ) F.&: x F.&: V.RNil
       makeRow key prob = key F.<+> yoRec F.<+> probRec prob
   return $ F.toFrame $ uncurry makeRow <$> zip (FL.fold FL.list toPredict) (FL.fold FL.list predictProbs)

comparePredictions ::  K.KnitEffects r
                   => F.FrameRec (CCES_KeyRow DT.CatColsASER5 V.++ '[BR.Year, ET.Office, ET.DemVPV, BR.DemPref])
                   -> F.FrameRec (CCES_CountRow DT.CatColsASER5)
                   -> K.Sem r ()
comparePredictions predictions input = do
  joined <- K.knitEither
            $ either (Left. show) Right
            $ FJ.leftJoinE @(CCES_KeyRow DT.CatColsASER5) input predictions
  let p = F.rgetField @ET.DemPref
      n = F.rgetField @BR.Count
      s = realToFrac . round @_ @Int . F.rgetField @BR.WeightedSuccesses
      rowCountError r = abs (p r * realToFrac (n r) - s r)
      countErrorOfVotersF = (\x y -> x / realToFrac y) <$> FL.premap rowCountError FL.sum <*> FL.premap n FL.sum
      countErrorOfDemsF = (\x y -> x / realToFrac y) <$> FL.premap rowCountError FL.sum <*> FL.premap s FL.sum
      (countErrorV, countErrorD) = FL.fold ((,) <$> countErrorOfVotersF <*> countErrorOfDemsF) joined
  K.logLE K.Info $ "absolute count error (fraction of all votes): " <> show countErrorV
  K.logLE K.Info $ "absolute count error (fraction of D votes): " <> show countErrorD


count :: forall ks r.
              (K.KnitEffects r
              , Ord (F.Record ks)
              , FI.RecVec (ks V.++ BR.CountCols)
              , ks F.⊆ CCES.CCES_MRP
              )
      => (F.Record CCES.CCES_MRP -> F.Record ks)
      -> ET.OfficeT
      -> Int
      -> F.FrameRec CCES.CCES_MRP
      -> K.Sem r (F.FrameRec (ks V.++ BR.CountCols))
count getKey office year ccesMRP = do
  countFold <- K.knitEither $ case (office, year) of
    (ET.President, 2008) ->
      Right $ CCES.countDVotesF @CCES.Pres2008VoteParty getKey 2008
    (ET.President, 2012) ->
      Right $ CCES.countDVotesF @CCES.Pres2012VoteParty getKey 2012
    (ET.President, 2016) ->
      Right $ CCES.countDVotesF @CCES.Pres2016VoteParty getKey 2016
    (ET.House, y) ->
      Right $  CCES.countDVotesF @CCES.HouseVoteParty getKey y
    _ -> Left $ show office <> "/" <> show year <> " not available."
  let counted = FL.fold countFold ccesMRP
  return counted

prefASER5_MR :: (K.KnitEffects r,  BR.CacheEffects r, BR.SerializerC b)
             => (T.Text, CCESDataWrangler DT.CatColsASER5 b)
             -> (T.Text, SB.StanModel)
             -> ET.OfficeT
             -> Int
             -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec
                                                  (CCES_KeyRow DT.CatColsASER5
                                                    V.++
                                                    '[BR.Year, ET.Office, ET.DemVPV, BR.DemPref]
                                                  )))
prefASER5_MR (dataLabel, ccesDataWrangler) (modelName, model) office year = do
  -- count data
  let officeYearT = show office <> "_" <> show year
      countCacheKey = "data/stan/cces/stateVotesASER5_" <> officeYearT <> ".bin"
  allStatesL <- do
    stateXWalk <- K.ignoreCacheTimeM BR.stateAbbrCrosswalkLoader
    return $ fmap (F.rgetField @BR.StateAbbreviation) .  FL.fold FL.list . F.filterFrame ((<60) . F.rgetField @BR.StateFIPS) $ stateXWalk
  let toPredict :: F.FrameRec ('[BR.StateAbbreviation] V.++ DT.CatColsASER5)
      toPredict = F.toFrame [ s F.&: cat | s <- allStatesL, cat <- DT.allCatKeysASER5]
  cces_C <- CCES.ccesDataLoader
  ccesASER5_C <- BR.retrieveOrMakeFrame countCacheKey cces_C
                 $ count (F.rcast @(BR.StateAbbreviation ': DT.CatColsASER5)) office year
  let stancConfig = (SM.makeDefaultStancConfig (toString $ "stan/voterPref/" <> modelName)) { CS.useOpenCL = False }
  stanConfig <- SC.noLogOfSummary
                <$> SM.makeDefaultModelRunnerConfig
                "stan/voterPref"
                (modelName <> "_model")
                (Just (SB.NoLL, model))
                (Just $ "cces_" <> officeYearT <> "_" <> dataLabel <> ".json")
                (Just $ "cces_" <> officeYearT <> "_" <> modelName <> "_model")
                4
                (Just 1000)
                (Just 1000)
                (Just stancConfig)
  let resultCacheKey = "model/stan/cces/statePrefsASER5_" <> officeYearT <> "_" <> modelName <> ".bin"
  modelDep <- SM.modelCacheTime stanConfig
  let dataModelDep = const <$> modelDep <*> ccesASER5_C
      getResults s tp inputAndIndex_C = do
        (input, _) <- K.ignoreCacheTime inputAndIndex_C
        predictions <- K.knitEither $ extractResults year office s tp
        comparePredictions predictions input
        return predictions
  BR.retrieveOrMakeFrame resultCacheKey dataModelDep $ \() -> do
    K.logLE K.Info "Data or model newer than last cached result. Rerunning."
    SM.runModel @BR.SerializerC @BR.CacheData stanConfig (SM.ShinyStan [SR.UnwrapNamed "D_votes" "D_votes"]) ccesDataWrangler (SC.UseSummary getResults) toPredict ccesASER5_C


prefASER5_MR_Loo :: (K.KnitEffects r,  BR.CacheEffects r, BR.SerializerC b)
                 => (T.Text, CCESDataWrangler DT.CatColsASER5 b)
                 -> (T.Text, SB.StanModel)
                 -> ET.OfficeT
                 -> Int
                 -> K.Sem r ()
prefASER5_MR_Loo (dataLabel, ccesDataWrangler) (modelName, model) office year = do
  -- count data
  let officeYearT = show office <> "_" <> show year
      countCacheKey = "data/stan/cces/stateVotesASER5_" <> officeYearT <> ".bin"
  cces_C <- CCES.ccesDataLoader
  K.logLE K.Diagnostic "Finished loading (cached) CCES data"
  ccesASER5_C <- BR.retrieveOrMakeFrame countCacheKey cces_C
                 $ count (F.rcast  @(BR.StateAbbreviation ': DT.CatColsASER5)) office year
  let stancConfig = (SM.makeDefaultStancConfig $ toString $ "stan/voterPref/" <> modelName <> "_loo") { CS.useOpenCL = False }
  stanConfig <- SC.noLogOfSummary
                <$> SM.makeDefaultModelRunnerConfig
                "stan/voterPref"
                (modelName <> "_loo")
                (Just (SB.OnlyLL, model))
                (Just $ "cces_" <> officeYearT <> "_" <> dataLabel <> ".json")
                (Just $ "cces_" <> officeYearT <> "_" <> modelName <> "_loo")
                4
                (Just 1000)
                (Just 1000)
                (Just stancConfig)
  SM.runModel @BR.SerializerC @BR.CacheData stanConfig SM.Loo (SC.noPredictions ccesDataWrangler) SC.DoNothing () ccesASER5_C


model_BinomialAllBuckets :: SB.StanModel
model_BinomialAllBuckets = SB.StanModel
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
           (Just binomialASER5_v4_GeneratedQuantitiesBlock)
           binomialASER5_v4_GQLLBlock


model_v5 :: SB.StanModel
model_v5 = SB.StanModel
           binomialASER5_v4_DataBlock
           Nothing
           binomialASER5_v5_ParametersBlock
           Nothing
           binomialASER5_v5_ModelBlock
           (Just binomialASER5_v5_GeneratedQuantitiesBlock)
           binomialASER5_v5_GQLLBlock

model_v6 :: SB.StanModel
model_v6 = SB.StanModel
           binomialASER5_v4_DataBlock
           (Just binomialASER5_v6_TransformedDataBlock)
           binomialASER5_v6_ParametersBlock
           Nothing
           binomialASER5_v6_ModelBlock
           (Just binomialASER5_v6_GeneratedQuantitiesBlock)
           binomialASER5_v6_GQLLBlock


model_v7 :: SB.StanModel
model_v7 = SB.StanModel
           binomialASER5_v4_DataBlock
           (Just binomialASER5_v6_TransformedDataBlock)
           binomialASER5_v7_ParametersBlock
           (Just binomialASER5_v7_TransformedParametersBlock)
           binomialASER5_v7_ModelBlock
           (Just binomialASER5_v7_GeneratedQuantitiesBlock)
           binomialASER5_v7_GQLLBlock


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
  int<lower = 0> M;
  int<lower = 0> predict_State[M];
  matrix[M, K] predict_X;

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


binomialASER5_v4_GeneratedQuantitiesBlock :: SB.GeneratedQuantitiesBlock
binomialASER5_v4_GeneratedQuantitiesBlock = [here|
  vector<lower = 0, upper = 1>[M] predicted;
  for (p in 1:M) {
    real xBeta;
//    for (k in 1:K) {
//      xBeta = predict_X[p, k] * beta[k];
//    }
    predicted[p] = inv_logit(alpha + predict_X[p] * beta);
  }
|]


binomialASER5_v4_GQLLBlock :: SB.GeneratedQuantitiesBlock
binomialASER5_v4_GQLLBlock = [here|
  vector[G] log_lik;
  for (g in 1:G) {
    log_lik[g] =  binomial_logit_lpmf(D_votes[g] | Total_votes[g], alpha + X[g] * beta);
  }
|]


binomialASER5_v5_ParametersBlock :: SB.ParametersBlock
binomialASER5_v5_ParametersBlock = [here|
  real alpha;
  vector[K] beta;
  real<lower=0> sigma_aState;
  vector<multiplier=sigma_aState> [J_state] aState;
|]

binomialASER5_v5_ModelBlock :: SB.ModelBlock
binomialASER5_v5_ModelBlock = [here|
  alpha ~ normal(0,2);
  beta ~ normal(0,1);
  sigma_aState ~ normal(0, 10);
  aState ~ normal(0, sigma_aState);
  D_votes ~ binomial_logit(Total_votes, alpha + (X * beta) + aState[state]);
|]


binomialASER5_v5_GeneratedQuantitiesBlock :: SB.GeneratedQuantitiesBlock
binomialASER5_v5_GeneratedQuantitiesBlock = [here|
  vector<lower = 0, upper = 1>[M] predicted;
  predicted = inv_logit(alpha + (predict_X * beta) + aState[predict_State]);
|]


binomialASER5_v5_GQLLBlock :: SB.GeneratedQuantitiesBlock
binomialASER5_v5_GQLLBlock = [here|
  vector[G] log_lik;
  for (g in 1:G) {
    log_lik[g] =  binomial_logit_lpmf(D_votes[g] | Total_votes[g], alpha + X[g] * beta + aState[state[g]]);
  }
|]

binomialASER5_v6_TransformedDataBlock :: SB.TransformedDataBlock
binomialASER5_v6_TransformedDataBlock = [here|
  vector[G] intcpt;
  vector[M] predictIntcpt;
  matrix[G, K+1] XI; // add the intercept so the covariance matrix is easier to deal with
  matrix[M, K+1] predict_XI;
  for (g in 1:G)
    intcpt[g] = 1;
  XI = append_col(intcpt, X);
  for (m in 1:M)
    predictIntcpt[m] = 1;
  predict_XI = append_col(predictIntcpt, predict_X);
|]

binomialASER5_v6_ParametersBlock :: SB.ParametersBlock
binomialASER5_v6_ParametersBlock = [here|
  real alpha; // overall intercept
  vector[K] beta; // fixed effects
  vector<lower=0> [K+1] sigma;
  vector[K+1] betaState[J_state]; // state-level coefficients
|]

binomialASER5_v6_ModelBlock :: SB.ModelBlock
binomialASER5_v6_ModelBlock = [here|
  alpha ~ normal(0,2); // weak prior around 50%
  beta ~ normal(0,1);
  sigma ~ normal(0,10);
  for (s in 1:J_state)
    betaState[s] ~ normal(0, sigma);
  {
    vector[G] xiBetaState;
    for (g in 1:G)
      xiBetaState[g] = XI[g] * betaState[state[g]];
    D_votes ~ binomial_logit(Total_votes, alpha + (X * beta) + xiBetaState);
  }
|]


binomialASER5_v6_GeneratedQuantitiesBlock :: SB.GeneratedQuantitiesBlock
binomialASER5_v6_GeneratedQuantitiesBlock = [here|
  vector<lower = 0, upper = 1>[M] predicted;
  for (m in 1:M)
    predicted[m] = inv_logit(alpha + (predict_X[m] * beta) + (predict_XI[m] * betaState[predict_State[m]]));
|]


binomialASER5_v6_GQLLBlock :: SB.GeneratedQuantitiesBlock
binomialASER5_v6_GQLLBlock = [here|
  vector[G] log_lik;
  for (g in 1:G) {
    log_lik[g] =  binomial_logit_lpmf(D_votes[g] | Total_votes[g], alpha + X[g] * beta + XI[g] * betaState[state[g]]);
  }
|]


binomialASER5_v7_ParametersBlock :: SB.ParametersBlock
binomialASER5_v7_ParametersBlock = [here|
  real alpha; // overall intercept
  vector[K] beta; // fixed effects
  vector<lower=0, upper=pi()/2> [K+1] tau_unif; // group effects scales
  cholesky_factor_corr[K+1] L_Omega; // group effect correlations
  matrix[K+1, J_state] z; // state-level coefficients pre-transform
|]

binomialASER5_v7_TransformedParametersBlock :: SB.TransformedParametersBlock
binomialASER5_v7_TransformedParametersBlock = [here|
  vector<lower=0>[K+1] tau;
  matrix[J_state, K+1] betaState; // state-level coefficients
  for (k in 1:(K+1))
    tau[k] = 2.5 * tan(tau_unif[k]);
  betaState = (diag_pre_multiply(tau, L_Omega) * z)';
|]



binomialASER5_v7_ModelBlock :: SB.ModelBlock
binomialASER5_v7_ModelBlock = [here|
  alpha ~ normal(0,2); // weak prior around 50%
  beta ~ normal(0,1);
  to_vector(z) ~ std_normal();
  L_Omega ~ lkj_corr_cholesky(2);
  D_votes ~ binomial_logit(Total_votes, alpha + X * beta + rows_dot_product(betaState[state], XI));
|]


binomialASER5_v7_GeneratedQuantitiesBlock :: SB.GeneratedQuantitiesBlock
binomialASER5_v7_GeneratedQuantitiesBlock = [here|
  vector<lower = 0, upper = 1>[M] predicted;
  for (m in 1:M)
    predicted[m] = inv_logit(alpha + (predict_X[m] * beta) + dot_product(predict_XI[m], betaState[predict_State[m]]));
|]


binomialASER5_v7_GQLLBlock :: SB.GeneratedQuantitiesBlock
binomialASER5_v7_GQLLBlock = [here|
  vector[G] log_lik;
  for (g in 1:G) {
    log_lik[g] =  binomial_logit_lpmf(D_votes[g] | Total_votes[g], alpha + X[g] * beta + dot_product(XI[g], betaState[state[g]]));
  }
|]
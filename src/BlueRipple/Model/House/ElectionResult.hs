{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-# OPTIONS_GHC -O0 #-}

module BlueRipple.Model.House.ElectionResult where

import qualified BlueRipple.Data.ACS_PUMS as PUMS
import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.ElectionTypes as ET
import qualified BlueRipple.Data.Loaders as BR
import qualified BlueRipple.Utilities.KnitUtils as BR
import qualified CmdStan as CS
import qualified Control.Foldl as FL
import qualified Data.Aeson as A
import Data.String.Here (here)
import qualified Data.Text as T
import qualified Data.Vector as Vec
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V
import qualified Frames as F
import qualified Frames.Folds as FF
import qualified Frames.MapReduce as FMR
import qualified Frames.SimpleJoins as FJ
import qualified Knit.Report as K
import qualified Numeric.Foldl as NFL
import qualified Stan.JSON as SJ
import qualified Stan.ModelBuilder as SB
import qualified Stan.ModelConfig as SC
import qualified Stan.ModelRunner as SM
import qualified Stan.Parameters as SP
import qualified Stan.RScriptBuilder as SR

type FracUnder45 = "FracUnder45" F.:-> Double

type FracFemale = "FracFemale" F.:-> Double

type FracGrad = "FracGrad" F.:-> Double

type FracNonWhite = "FracNonWhite" F.:-> Double

type FracCitizen = "FracCitizen" F.:-> Double

type KeyR = [BR.Year, BR.StateAbbreviation, BR.CongressionalDistrict]

type DemographicsR =
  [ FracUnder45,
    FracFemale,
    FracGrad,
    FracNonWhite,
    FracCitizen,
    DT.AvgIncome,
    DT.MedianIncome,
    DT.PopPerSqMile,
    PUMS.Citizens
  ]

type DVotes = "DVotes" F.:-> Int

type RVotes = "RVotes" F.:-> Int

type TVotes = "TVotes" F.:-> Int

type ElectionR = [DVotes, RVotes]

type HouseDataR = KeyR V.++ DemographicsR V.++ ElectionR

type HouseData = F.FrameRec HouseDataR

pumsF :: FL.Fold (F.Record (PUMS.CDCounts DT.CatColsASER)) (F.FrameRec (KeyR V.++ DemographicsR))
pumsF =
  FMR.concatFold $
    FMR.mapReduceFold
      FMR.noUnpack
      (FMR.assignKeysAndData @KeyR)
      (FMR.foldAndAddKey pumsDataF)

pumsDataF ::
  FL.Fold
    (F.Record [DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.SimpleRaceC, DT.AvgIncome, DT.MedianIncome, DT.PopPerSqMile, PUMS.Citizens, PUMS.NonCitizens])
    (F.Record DemographicsR)
pumsDataF =
  let cit = F.rgetField @PUMS.Citizens
      citF = FL.premap cit FL.sum
      intRatio x y = realToFrac x / realToFrac y
      fracF f = intRatio <$> FL.prefilter f citF <*> citF
      citWgtdSumF f = FL.premap (\r -> realToFrac (cit r) * f r) FL.sum
      citWgtdF f = (/) <$> citWgtdSumF f <*> fmap realToFrac citF
   in FF.sequenceRecFold $
        FF.toFoldRecord (fracF ((== DT.Under) . F.rgetField @DT.SimpleAgeC))
          V.:& FF.toFoldRecord (fracF ((== DT.Female) . F.rgetField @DT.SexC))
          V.:& FF.toFoldRecord (fracF ((== DT.Grad) . F.rgetField @DT.CollegeGradC))
          V.:& FF.toFoldRecord (fracF ((== DT.NonWhite) . F.rgetField @DT.SimpleRaceC))
          V.:& FF.toFoldRecord ((\x y -> intRatio x (x + y)) <$> citF <*> FL.premap (F.rgetField @PUMS.NonCitizens) FL.sum)
          V.:& FF.toFoldRecord (citWgtdF (F.rgetField @DT.AvgIncome))
          V.:& FF.toFoldRecord (NFL.weightedMedianF (realToFrac . cit) (F.rgetField @DT.MedianIncome)) -- FL.premap (\r -> (realToFrac (cit r), F.rgetField @DT.MedianIncome r)) PUMS.medianIncomeF)
          V.:& FF.toFoldRecord (citWgtdF (F.rgetField @DT.PopPerSqMile))
          V.:& FF.toFoldRecord citF
          V.:& V.RNil

electionF :: FL.Fold (F.Record BR.HouseElectionCols) (F.FrameRec (KeyR V.++ ElectionR))
electionF =
  FMR.concatFold $
    FMR.mapReduceFold
      FMR.noUnpack
      (FMR.assignKeysAndData @KeyR)
      (FMR.foldAndAddKey flattenVotesF)

flattenVotesF :: FL.Fold (F.Record [ET.Party, ET.Votes]) (F.Record ElectionR)
flattenVotesF =
  let party = F.rgetField @ET.Party
      votes = F.rgetField @ET.Votes
      demVotesF = FL.prefilter (\r -> party r == ET.Democratic) $ FL.premap votes FL.sum
      repVotesF = FL.prefilter (\r -> party r == ET.Republican) $ FL.premap votes FL.sum
   in (\dv rv -> dv F.&: rv F.&: V.RNil) <$> demVotesF <*> repVotesF

prepCachedData ::
  (K.KnitEffects r, K.CacheEffectsD r) =>
  K.Sem r (K.ActionWithCacheTime r HouseData)
prepCachedData = do
  pums_C <- PUMS.pumsLoaderAdults
  cdFromPUMA_C <- BR.allCDFromPUMA2012Loader
  let pumsByCDDeps = (,) <$> pums_C <*> cdFromPUMA_C
  pumsByCD_C <- BR.retrieveOrMakeFrame "model/house/pumsByCD.bin" pumsByCDDeps $ \(pums, cdFromPUMA) ->
    PUMS.pumsCDRollup ((>= 2012) . F.rgetField @BR.Year) (PUMS.pumsKeysToASER True) cdFromPUMA pums
  -- investigate college grad %
  {-}
  pums <- K.ignoreCacheTime pums_C
  let yrState y sa r =
        F.rgetField @BR.Year r == y
          && F.rgetField @BR.StateAbbreviation r == sa
      pumsGA2018 = FL.fold (PUMS.pumsStateRollupF (PUMS.pumsKeysToASER True)) $ F.filterFrame (yrState 2018 "GA") pums
  BR.logFrame pumsGA2018
  -}
  --
  houseElections_C <- BR.houseElectionsLoader
  let houseDataDeps = (,) <$> pumsByCD_C <*> houseElections_C
  --  BR.clearIfPresentD "model/house/demographicsAndElections.bin"
  BR.retrieveOrMakeFrame "model/house/houseData.bin" houseDataDeps $ \(pumsByCD, elex) -> do
    K.logLE K.Info "HouseData for election model out of date/unbuilt.  Loading demographic and election data and joining."
    let demographics = FL.fold pumsF $ F.filterFrame ((/= "DC") . F.rgetField @BR.StateAbbreviation) pumsByCD
        electionResults = FL.fold electionF (F.filterFrame ((>= 2012) . F.rgetField @BR.Year) elex)
        (demoAndElex, missing) = FJ.leftJoinWithMissing @KeyR demographics electionResults
    K.knitEither
      ( if null missing
          then Right ()
          else
            ( Left $
                "Missing keys in left-join of demographics and election data in house model prep:"
                  <> T.pack
                    (show missing)
            )
      )
    return demoAndElex

type HouseDataWrangler = SC.DataWrangler HouseData () ()

houseDataWrangler :: Int -> HouseDataWrangler
houseDataWrangler year = SC.Wrangle SC.NoIndex f
  where
    -- house cols
    district r = F.rgetField @BR.StateAbbreviation r <> (T.pack $ show $ F.rgetField @BR.CongressionalDistrict r)
    tVotes r = F.rgetField @DVotes r + F.rgetField @RVotes r
    enumStateF = FL.premap (F.rgetField @BR.StateAbbreviation) (SJ.enumerate 1)
    enumDistrictF = FL.premap district (SJ.enumerate 1)
    f houseData = ((), makeDataJsonE)
      where
        (stateM, toState) = FL.fold enumStateF houseData
        (cdM, toCD) = FL.fold enumDistrictF houseData
        makeDataJsonE :: HouseData -> Either T.Text A.Series
        makeDataJsonE houseData = do
          let predictRow :: F.Record DemographicsR -> Vec.Vector Double
              predictRow r =
                Vec.fromList $
                  F.recToList $
                    F.rcast
                      @[ FracUnder45,
                         FracFemale,
                         FracGrad,
                         FracNonWhite,
                         FracCitizen,
                         DT.AvgIncome,
                         DT.PopPerSqMile
                       ]
                      r
              dataF =
                SJ.namedF "G" FL.length
                  <> SJ.constDataF "K" (7 :: Int)
                  <> SJ.valueToPairF "state" (SJ.jsonArrayMF (stateM . F.rgetField @BR.StateAbbreviation))
                  <> SJ.valueToPairF "district" (SJ.jsonArrayMF (cdM . district))
                  <> SJ.valueToPairF "X" (SJ.jsonArrayF (predictRow . F.rcast))
                  <> SJ.valueToPairF "VAP" (SJ.jsonArrayF (F.rgetField @PUMS.Citizens))
                  <> SJ.valueToPairF "TVotes" (SJ.jsonArrayF tVotes)
                  <> SJ.valueToPairF "DVotes" (SJ.jsonArrayF (F.rgetField @DVotes))
          SJ.frameToStanJSONSeries dataF houseData

type VoteP = "VoteProb" F.:-> Double

type DVoteP = "DVoteProb" F.:-> Double

type EVotes = "EstVotes" F.:-> Int

type EDVotes = "EstDVotes" F.:-> Int

type EDVotes5 = "EstDVotes5" F.:-> Int

type EDVotes95 = "EstDVotes95" F.:-> Int

type Modeled = [VoteP, DVoteP, EVotes, EDVotes5, EDVotes, EDVotes95]

type HouseModelResults = [BR.StateAbbreviation, BR.CongressionalDistrict, PUMS.Citizens, DVotes, TVotes] V.++ Modeled

extractResults ::
  Int ->
  CS.StanSummary ->
  HouseData ->
  Either T.Text (F.FrameRec HouseModelResults)
extractResults year summary demographicsByDistrict = do
  pVotedP <- fmap CS.mean <$> SP.parse1D "pVotedP" (CS.paramStats summary)
  pDVotedP <- fmap CS.mean <$> SP.parse1D "pDVotedP" (CS.paramStats summary)
  eTVote <- fmap CS.mean <$> SP.parse1D "eTVotes" (CS.paramStats summary)
  eDVotePcts <- fmap CS.percents <$> SP.parse1D "eDVotes" (CS.paramStats summary)
  let modeledL =
        FL.fold FL.list $
          Vec.zip4
            (SP.getVector pVotedP)
            (SP.getVector pDVotedP)
            (SP.getVector eTVote)
            (SP.getVector eDVotePcts)
      makeRow (hdRow, (pV, pD, etVotes, dVotesPcts)) = do
        if length dVotesPcts == 3
          then
            let [d5, d, d95] = dVotesPcts
             in Right $
                  F.rgetField @BR.StateAbbreviation hdRow
                    F.&: F.rgetField @BR.CongressionalDistrict hdRow
                    F.&: F.rgetField @PUMS.Citizens hdRow
                    F.&: F.rgetField @DVotes hdRow
                    F.&: (F.rgetField @DVotes hdRow + F.rgetField @RVotes hdRow)
                    F.&: pV
                    F.&: pD
                    F.&: round etVotes
                    F.&: round d5
                    F.&: round d
                    F.&: round d95
                    F.&: V.RNil
          else Left ("Wrong number of percentiles in stan statistic")
  result <-
    traverse makeRow $
      zip (FL.fold (FL.prefilter ((== year) . F.rgetField @BR.Year) FL.list) demographicsByDistrict) modeledL
  return $ F.toFrame result

runHouseModel ::
  (K.KnitEffects r, K.CacheEffectsD r) =>
  (Int -> HouseDataWrangler) ->
  (T.Text, SB.StanModel) ->
  K.ActionWithCacheTime r HouseData ->
  Int ->
  K.Sem r (K.ActionWithCacheTime r (F.FrameRec HouseModelResults))
runHouseModel hdwForYear (modelName, model) houseData_C year = do
  --houseData <- K.ignoreCacheTime houseData_C
  let workDir = "stan/house/election/"
  let stancConfig = (SM.makeDefaultStancConfig (T.unpack $ workDir <> modelName)) {CS.useOpenCL = False}
  stanConfig <-
    SC.setSigFigs 4
      . SC.noLogOfSummary
      <$> SM.makeDefaultModelRunnerConfig
        workDir
        (modelName <> "_model")
        (Just (SB.All, model))
        (Just $ "election.json")
        (Just $ "election_" <> modelName <> "_model")
        4
        (Just 1000)
        (Just 1000)
        (Just stancConfig)
  let resultCacheKey = "house/model/stan/election_" <> modelName <> ".bin"
  modelDep <- SM.modelCacheTime stanConfig
  let dataModelDep = const <$> modelDep <*> houseData_C
      getResults s () inputAndIndex_C = do
        (houseData, _) <- K.ignoreCacheTime inputAndIndex_C
        K.knitEither $ (extractResults year) s houseData
  BR.retrieveOrMakeFrame resultCacheKey dataModelDep $ \() -> do
    K.logLE K.Info "Data or model newer then last cached result. (Re)-running..."
    SM.runModel
      stanConfig
      (SM.Both [SR.UnwrapJSON "DVotes" "DVotes", SR.UnwrapJSON "TVotes" "TVotes"])
      (hdwForYear year)
      (SC.UseSummary getResults)
      ()
      houseData_C

model_v1 :: SB.StanModel
model_v1 =
  SB.StanModel
    binomialDataBlock
    (Just binomialTransformedDataBlock)
    binomialParametersBlock
    Nothing
    binomialModelBlock
    (Just binomialGeneratedQuantitiesBlock)
    binomialGQLLBlock

binomialDataBlock :: SB.DataBlock
binomialDataBlock =
  [here|  
  int<lower = 1> G; // number of counties
  int<lower = 1> K; // number of predictors
  int<lower = 1, upper = G> district[G]; // do we need this?
  matrix[G, K] X;
  int<lower = 0> VAP[G];
  int<lower = 0> TVotes[G];
  int<lower = 0> DVotes[G];
|]

binomialTransformedDataBlock :: SB.TransformedDataBlock
binomialTransformedDataBlock =
  [here|
  matrix[G, K] Q_ast;
  matrix[K, K] R_ast;
  matrix[K, K] R_ast_inverse;
  // thin and scale the QR decomposition
  Q_ast = qr_Q(X)[, 1:K] * sqrt(G - 1);
  R_ast = qr_R(X)[1:K,]/sqrt(G - 1);
  R_ast_inverse = inverse(R_ast);
|]

binomialParametersBlock :: SB.ParametersBlock
binomialParametersBlock =
  [here|
  real alphaD;                             
  vector[K] thetaV;
  real alphaV;
  vector[K] thetaD;
|]

binomialTransformedParametersBlock :: SB.TransformedParametersBlock
binomialTransformedParametersBlock =
  [here|
                                            
|]

binomialModelBlock :: SB.ModelBlock
binomialModelBlock =
  [here|
//  alphaD ~ normal(0, 2);
//  alphaV ~ normal(0, 2);
//  betaV ~ normal(0, 5);
//  betaD ~ normal(0, 5);
  TVotes ~ binomial_logit(VAP, alphaV + Q_ast * thetaV);
  DVotes ~ binomial_logit(TVotes, alphaD + Q_ast * thetaD);
|]

binomialGeneratedQuantitiesBlock :: SB.GeneratedQuantitiesBlock
binomialGeneratedQuantitiesBlock =
  [here|
  vector[K] betaV;
  vector[K] betaD;
  betaV = R_ast_inverse * thetaV;
  betaD = R_ast_inverse * thetaD;
  vector<lower = 0, upper = 1>[G] pVotedP;
  vector<lower = 0, upper = 1>[G] pDVoteP;
  pVotedP = inv_logit(alphaV + (Q_ast * thetaV));
  pDVoteP = inv_logit(alphaD + (Q_ast * thetaD));
  vector<lower = 0>[G] eTVotes;
  vector<lower = 0>[G] eDVotes;
  for (g in 1:G) {
    eTVotes[g] = inv_logit(alphaV + (Q_ast[g] * thetaV)) * VAP[g];
    eDVotes[g] = inv_logit(alphaD + (Q_ast[g] * thetaD)) * TVotes[g];
  }
|]

binomialGQLLBlock :: SB.GeneratedQuantitiesBlock
binomialGQLLBlock =
  [here|
  vector[G] log_lik;
//  log_lik = binomial_logit_lpmf(DVotes1 | TVotes1, alphaD + Q_ast * thetaD);
  for (g in 1:G) {
    log_lik[g] =  binomial_logit_lpmf(DVotes[g] | TVotes[g], alphaD + Q_ast[g] * thetaD);
  }
|]

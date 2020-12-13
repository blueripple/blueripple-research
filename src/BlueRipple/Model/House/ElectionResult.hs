{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# OPTIONS_GHC -O0 #-}

module BlueRipple.Model.House.ElectionResult where

import qualified BlueRipple.Data.ACS_PUMS as PUMS
import qualified BlueRipple.Data.CCES as CCES
import qualified BlueRipple.Model.MRP as MRP
import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.ElectionTypes as ET
import qualified BlueRipple.Data.Loaders as BR
import qualified BlueRipple.Utilities.KnitUtils as BR
import qualified CmdStan as CS
import qualified Control.Foldl as FL
import qualified Data.Aeson as A
import qualified Data.Map as M
import Data.Maybe (fromMaybe)
import Data.String.Here (here)
import qualified Data.Text as T
import qualified Data.Vector as Vec
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V
import qualified Frames as F
import qualified Frames.Folds as FF
import qualified Frames.MapReduce as FMR
import qualified Frames.SimpleJoins as FJ
import qualified Frames.Transform as FT
import qualified Graphics.Vega.VegaLite.MapRow as MapRow
import qualified Knit.Effect.AtomicCache as K
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

-- +1 for Dem incumbent, 0 for no incumbent, -1 for Rep incumbent
type Incumbency = "Incumbency" F.:-> Int
type ElectionR = [Incumbency, DVotes, RVotes]
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

electionF :: FL.FoldM (Either T.Text) (F.Record (BR.HouseElectionCols V.++ '[ET.Incumbent])) (F.FrameRec (KeyR V.++ ElectionR))
electionF =
  FMR.concatFoldM $
    FMR.mapReduceFoldM
      (FMR.generalizeUnpack FMR.noUnpack)
      (FMR.generalizeAssign $ FMR.assignKeysAndData @KeyR)
      (FMR.makeRecsWithKeyM id $ FMR.ReduceFoldM $ const $ fmap (pure @[]) flattenVotesF)

data IncParty = None | Inc ET.PartyT | Multi

updateIncParty :: IncParty -> ET.PartyT -> IncParty
updateIncParty Multi _ = Multi
updateIncParty (Inc _) _ = Multi
updateIncParty None p = Inc p

incPartyToInt :: IncParty -> Either T.Text Int
incPartyToInt None = Right 0
incPartyToInt (Inc ET.Democratic) = Right 1
incPartyToInt (Inc ET.Republican) = Right (negate 1)
incPartyToInt (Inc _) = Right 0
incPartyToInt Multi = Left "Error: Multiple incumbents!"

flattenVotesF :: FL.FoldM (Either T.Text) (F.Record [BR.Candidate, ET.Incumbent, ET.Party, ET.Votes]) (F.Record ElectionR)
flattenVotesF = FMR.postMapM (FL.foldM flattenF) aggregatePartiesF
  where
    party = F.rgetField @ET.Party
    votes = F.rgetField @ET.Votes
    incumbentPartyF =
      FMR.postMapM incPartyToInt $
        FL.generalize $
          FL.prefilter (F.rgetField @ET.Incumbent) $
            FL.premap (F.rgetField @ET.Party) (FL.Fold updateIncParty None id)
    demVotesF = FL.generalize $ FL.prefilter (\r -> party r == ET.Democratic) $ FL.premap votes FL.sum
    repVotesF = FL.generalize $ FL.prefilter (\r -> party r == ET.Republican) $ FL.premap votes FL.sum
    flattenF = (\ii dv rv -> ii F.&: dv F.&: rv F.&: V.RNil) <$> incumbentPartyF <*> demVotesF <*> repVotesF

aggregatePartiesF ::
  FL.FoldM
    (Either T.Text)
    (F.Record [BR.Candidate, ET.Incumbent, ET.Party, ET.Votes])
    (F.FrameRec [BR.Candidate, ET.Incumbent, ET.Party, ET.Votes])
aggregatePartiesF =
  let apF :: FL.FoldM (Either T.Text) (F.Record [ET.Party, ET.Votes]) (F.Record [ET.Party, ET.Votes])
      apF = FMR.postMapM ap (FL.generalize $ FL.premap (\r -> (F.rgetField @ET.Party r, F.rgetField @ET.Votes r)) FL.map)
        where
          ap pvs =
            let demvM = M.lookup ET.Democratic pvs
                repvM = M.lookup ET.Republican pvs
                votes = FL.fold FL.sum $ M.elems pvs
                partyE = case (demvM, repvM) of
                  (Nothing, Nothing) -> Right ET.Other
                  (Just _, Nothing) -> Right ET.Democratic
                  (Nothing, Just _) -> Right ET.Republican
                  (Just dv, Just rv) -> Left "Votes on both D and R lines"
             in fmap (\p -> p F.&: votes F.&: V.RNil) partyE
   in FMR.concatFoldM $
        FMR.mapReduceFoldM
          (FMR.generalizeUnpack FMR.noUnpack)
          (FMR.generalizeAssign $ FMR.assignKeysAndData @[BR.Candidate, ET.Incumbent] @[ET.Party, ET.Votes])
          (FMR.makeRecsWithKeyM id $ FMR.ReduceFoldM $ const $ fmap (pure @[]) apF)

-- CCES data
type Surveyed = "Surveyed" F.:-> Int -- total people in each bucket
type CCESByCD = [BR.Year, BR.StateAbbreviation, BR.CongressionalDistrict, DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.SimpleRaceC, Surveyed, TVotes, DVotes]

countCCESVotesF :: FL.Fold (F.Record [CCES.Turnout, CCES.HouseVoteParty]) (F.Record [Surveyed, TVotes, DVotes])
countCCESVotesF =
  let surveyedF = FL.length
      votedF = FL.prefilter ((== CCES.T_Voted) . F.rgetField @CCES.Turnout) FL.length
      dVoteF = FL.prefilter ((== ET.Democratic) . F.rgetField @CCES.HouseVoteParty) FL.length -- or votedF?
  in (\s v d -> s F.&: v F.&: d F.&: V.RNil) <$> surveyedF <*> votedF <*> dVoteF

ccesF :: Int -> FL.Fold (F.Record CCES.CCES_MRP) (F.FrameRec CCESByCD)
ccesF earliestYear = FMR.concatFold
        $ FMR.mapReduceFold
        (FMR.unpackFilterOnField @BR.Year (>= earliestYear))
        (FMR.assignKeysAndData @[BR.Year, BR.StateAbbreviation, BR.CongressionalDistrict, DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.SimpleRaceC])
        (FMR.foldAndAddKey countCCESVotesF)

ccesCountedDemHouseVotesByCD :: (K.KnitEffects r, K.CacheEffectsD r) => K.Sem r (K.ActionWithCacheTime r (F.FrameRec CCESByCD))
ccesCountedDemHouseVotesByCD = do
  cces_C <- CCES.ccesDataLoader 
  BR.retrieveOrMakeFrame "model/house/ccesByCD.bin" cces_C $ return . FL.fold (ccesF 2012)
    

prepCachedData ::
  (K.KnitEffects r, K.CacheEffectsD r) =>
  K.Sem r (K.ActionWithCacheTime r HouseData)
prepCachedData = do
  pums_C <- PUMS.pumsLoaderAdults
  cdFromPUMA_C <- BR.allCDFromPUMA2012Loader
  let pumsByCDDeps = (,) <$> pums_C <*> cdFromPUMA_C
  pumsByCD_C <- BR.retrieveOrMakeFrame "model/house/pumsByCD.bin" pumsByCDDeps $ \(pums, cdFromPUMA) ->
    PUMS.pumsCDRollup ((>= 2012) . F.rgetField @BR.Year) (PUMS.pumsKeysToASER True) cdFromPUMA pums
  houseElections_C <- BR.houseElectionsWithIncumbency
  countedCCES_C <- ccesCountedDemHouseVotesByCD
  K.ignoreCacheTime countedCCES_C >>= BR.logFrame . F.filterFrame ((== "GA") . F.rgetField @BR.StateAbbreviation)
  let houseDataDeps = (,) <$> pumsByCD_C <*> houseElections_C
  --BR.clearIfPresentD "model/house/houseData.bin"
  BR.retrieveOrMakeFrame "model/house/houseData.bin" houseDataDeps $ \(pumsByCD, elex) -> do
    K.logLE K.Info "HouseData for election model out of date/unbuilt.  Loading demographic and election data and joining."
    let demographics = FL.fold pumsF $ F.filterFrame ((/= "DC") . F.rgetField @BR.StateAbbreviation) pumsByCD
    electionResults <- K.knitEither $ FL.foldM electionF (F.filterFrame ((>= 2012) . F.rgetField @BR.Year) elex)
    let (demoAndElex, missing) = FJ.leftJoinWithMissing @KeyR demographics electionResults
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

houseDataWrangler :: HouseDataWrangler
houseDataWrangler = SC.Wrangle SC.NoIndex f
  where
    -- house cols
    district r = F.rgetField @BR.StateAbbreviation r <> (T.pack $ show $ F.rgetField @BR.CongressionalDistrict r)
    tVotes r = F.rgetField @DVotes r + F.rgetField @RVotes r
    enumStateF = FL.premap (F.rgetField @BR.StateAbbreviation) (SJ.enumerate 1)
    enumDistrictF = FL.premap district (SJ.enumerate 1)
    maxAvgIncomeF = fmap (fromMaybe 1) $ FL.premap (F.rgetField @DT.AvgIncome) FL.maximum
    maxDensityF = fmap (fromMaybe 1) $ FL.premap (F.rgetField @DT.PopPerSqMile) FL.maximum
    f _ = ((), makeDataJsonE)
      where
        makeDataJsonE :: HouseData -> Either T.Text A.Series
        makeDataJsonE houseData = do
          let ((stateM, _), (cdM, _), maxAvgIncome, maxDensity) =
                FL.fold
                  ( (,,,)
                      <$> enumStateF
                      <*> enumDistrictF
                      <*> maxAvgIncomeF
                      <*> maxDensityF
                  )
                  houseData

              predictRowV :: F.Record DemographicsR -> Vec.Vector Double
              predictRowV r =
                Vec.fromList $
                  F.recToList $
                    FT.fieldEndo @DT.AvgIncome (/ maxAvgIncome) $
                      FT.fieldEndo @DT.PopPerSqMile (/ maxDensity) $
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
                  <> SJ.valueToPairF "X" (SJ.jsonArrayF (predictRowV . F.rcast))
                  <> SJ.valueToPairF "Inc" (SJ.jsonArrayF (F.rgetField @Incumbency))
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

{-
--getParameters :: (CS.StanSummary -> T.Text -> Either T.Text a) -> MapRow () -> CS.StanSummary -> Either T.Text (MapRow a)
--getParameters getP nameRow s = sequenceA $ M.mapWithKey (\k _ -> getP s k) nameRow
-}
extractResults ::
  CS.StanSummary ->
  HouseData ->
  Either T.Text (F.FrameRec HouseModelResults, MapRow.MapRow [Double])
extractResults summary demographicsByDistrict = do
  -- predictions
  pVotedP <- fmap CS.mean <$> SP.parse1D "pVotedP" (CS.paramStats summary)
  pDVotedP <- fmap CS.mean <$> SP.parse1D "pDVoteP" (CS.paramStats summary)
  eTVote <- fmap CS.mean <$> SP.parse1D "eTVotes" (CS.paramStats summary)
  eDVotePcts <- fmap CS.percents <$> SP.parse1D "eDVotes" (CS.paramStats summary)
  --deltaVs = fmap (\x -> "deltaV[")
  --deltaNameRow = nameRowFromList ["deltaV[1]", "deltaV[2]"]
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
  predictions <-
    traverse makeRow $
      zip (FL.fold FL.list demographicsByDistrict) modeledL
  -- deltas
  deltaIncD <- fmap CS.percents <$> SP.parseScalar "deltaIncD" (CS.paramStats summary)
  deltaD <- fmap CS.percents <$> SP.parse1D "deltaD" (CS.paramStats summary)
  deltaV <- fmap CS.percents <$> SP.parse1D "deltaV" (CS.paramStats summary)
  deltaDMR <-
    MapRow.withNames
      [ "PctUnder45D",
        "PctFemaleD",
        "PctGradD",
        "PctNonWhiteD",
        "PctCitizenD",
        "AvgIncomeD",
        "PopPerSqMileD"
      ]
      (SP.getVector deltaD)
  deltaVMR <-
    MapRow.withNames
      [ "PctUnder45V",
        "PctFemaleV",
        "PctGradV",
        "PctNonWhiteV",
        "PctCitizenV",
        "AvgIncomeV",
        "PopPerSqMileV"
      ]
      (SP.getVector deltaV)
  let deltas = deltaDMR <> deltaVMR <> M.singleton "Incumbency" (SP.getScalar deltaIncD)
  return $ (F.toFrame predictions, deltas)

runHouseModel ::
  forall r.
  (K.KnitEffects r, K.CacheEffectsD r) =>
  HouseDataWrangler ->
  (T.Text, SB.StanModel) ->
  Int ->
  K.ActionWithCacheTime r HouseData ->
  K.Sem r (K.ActionWithCacheTime r (F.FrameRec HouseModelResults, MapRow.MapRow [Double]))
runHouseModel hdw (modelName, model) year houseData_C = K.wrapPrefix "BlueRipple.Model.House.ElectionResults.runHouseModel" $ do
  K.logLE K.Info "Running..."
  let workDir = "stan/house/election"
  let stancConfig = (SM.makeDefaultStancConfig (T.unpack $ workDir <> "/" <> modelName)) {CS.useOpenCL = False}
  stanConfig <-
    SC.setSigFigs 4
      . SC.noLogOfSummary
      <$> SM.makeDefaultModelRunnerConfig
        workDir
        (modelName <> "_model")
        (Just (SB.All, model))
        (Just $ "election_" <> (T.pack $ show year) <> ".json")
        (Just $ "election_" <> modelName <> "_" <> (T.pack $ show year))
        4
        (Just 1000)
        (Just 1000)
        (Just stancConfig)
  let resultCacheKey = "house/model/stan/election_" <> modelName <> "_" <> (T.pack $ show year) <> ".bin"
      houseDataForYear_C = fmap (F.filterFrame ((== year) . F.rgetField @BR.Year)) houseData_C
  modelDep <- SM.modelCacheTime stanConfig
  K.logLE K.Diagnostic $ "modelDep: " <> (T.pack $ show $ K.cacheTime modelDep)
  K.logLE K.Diagnostic $ "houseDataDep: " <> (T.pack $ show $ K.cacheTime houseData_C)
  let dataModelDep = const <$> modelDep <*> houseDataForYear_C
      getResults s () inputAndIndex_C = do
        (houseData, _) <- K.ignoreCacheTime inputAndIndex_C
        K.knitEither $ extractResults s houseData
  BR.retrieveOrMakeFrameAnd resultCacheKey dataModelDep $ \() -> do
    K.logLE K.Info "Data or model newer then last cached result. (Re)-running..."
    SM.runModel
      stanConfig
      (SM.Both [SR.UnwrapJSON "DVotes" "DVotes", SR.UnwrapJSON "TVotes" "TVotes"])
      hdw
      (SC.UseSummary getResults)
      ()
      houseDataForYear_C

binomial_v1 :: SB.StanModel
binomial_v1 =
  SB.StanModel
    binomialDataBlock
    (Just binomialTransformedDataBlock)
    binomialParametersBlock
    (Just binomialTransformedParametersBlock)
    binomialModelBlock
    (Just binomialGeneratedQuantitiesBlock)
    binomialGQLLBlock

betaBinomial_v1 :: SB.StanModel
betaBinomial_v1 =
  SB.StanModel
    binomialDataBlock
    (Just binomialTransformedDataBlock)
    betaBinomialParametersBlock
    (Just betaBinomialTransformedParametersBlock)
    betaBinomialModelBlock
    (Just betaBinomialGeneratedQuantitiesBlock)
    betaBinomialGQLLBlock

betaBinomialInc :: SB.StanModel
betaBinomialInc =
  SB.StanModel
    binomialDataBlock
    (Just binomialTransformedDataBlock)
    betaBinomialIncParametersBlock
    (Just betaBinomialIncTransformedParametersBlock)
    betaBinomialIncModelBlock
    (Just betaBinomialIncGeneratedQuantitiesBlock)
    betaBinomialGQLLBlock

betaBinomialHS :: SB.StanModel
betaBinomialHS =
  SB.StanModel
    binomialDataBlock
    (Just binomialTransformedDataBlock)
    betaBinomialHSParametersBlock
    (Just betaBinomialTransformedParametersBlock)
    betaBinomialHSModelBlock
    (Just betaBinomialGeneratedQuantitiesBlock)
    betaBinomialGQLLBlock

binomialDataBlock :: SB.DataBlock
binomialDataBlock =
  [here|  
  int<lower = 1> G; // number of districts 
  int<lower = 1> K; // number of predictors
  int<lower = 1, upper = G> district[G]; // do we need this?
  matrix[G, K] X;
  int<lower=-1, upper=1> Inc[G];
  int<lower = 0> VAP[G];
  int<lower = 0> TVotes[G];
  int<lower = 0> DVotes[G];
|]

binomialTransformedDataBlock :: SB.TransformedDataBlock
binomialTransformedDataBlock =
  [here|
  vector<lower=0>[K] sigma;
  matrix[G, K] X_centered;
  for (k in 1:K) {
    real col_mean = mean(X[,k]);
    X_centered[,k] = X[,k] - col_mean;
    sigma[k] = sd(X_centered[,k]);
  } 
  
  matrix[G, K] Q_ast;
  matrix[K, K] R_ast;
  matrix[K, K] R_ast_inverse;
  // thin and scale the QR decomposition
  Q_ast = qr_Q(X_centered)[, 1:K] * sqrt(G - 1);
  R_ast = qr_R(X_centered)[1:K,]/sqrt(G - 1);
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
  vector [K] betaV;
  vector [K] betaD;
  betaV = R_ast_inverse * thetaV;
  betaD = R_ast_inverse * thetaD;
|]

binomialModelBlock :: SB.ModelBlock
binomialModelBlock =
  [here|
  alphaD ~ cauchy(0, 10);
  alphaV ~ cauchy(0, 10);
  betaD ~ cauchy(0, 2.5);
  betaV ~ cauchy(0,2.5);
  TVotes ~ binomial_logit(VAP, alphaV + Q_ast * thetaV);
  DVotes ~ binomial_logit(TVotes, alphaD + Q_ast * thetaD);
|]

binomialGeneratedQuantitiesBlock :: SB.GeneratedQuantitiesBlock
binomialGeneratedQuantitiesBlock =
  [here|
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

betaBinomialParametersBlock :: SB.ParametersBlock
betaBinomialParametersBlock =
  [here|
  real alphaD;
  real <lower=0, upper=1> dispD;                             
  vector[K] thetaV;
  real alphaV;
  real <lower=0, upper=1> dispV;
  vector[K] thetaD;
|]

betaBinomialTransformedParametersBlock :: SB.TransformedParametersBlock
betaBinomialTransformedParametersBlock =
  [here|
  real <lower=0> phiD = (1-dispD)/dispD;
  real <lower=0> phiV = (1-dispV)/dispV;
  vector [G] pDVoteP = inv_logit (alphaD + Q_ast * thetaD);
  vector [G] pVotedP = inv_logit (alphaV + Q_ast * thetaV);
  vector [K] betaV;
  vector [K] betaD;
  betaV = R_ast_inverse * thetaV;
  betaD = R_ast_inverse * thetaD;
|]

betaBinomialModelBlock :: SB.ModelBlock
betaBinomialModelBlock =
  [here|
  alphaD ~ cauchy(0, 10);
  alphaV ~ cauchy(0, 10);
  betaV ~ cauchy(0, 2.5);
  betaD ~ cauchy(0, 2.5);

  TVotes ~ beta_binomial(VAP, pVotedP * phiV, (1 - pVotedP) * phiV);
  DVotes ~ beta_binomial(TVotes, pDVoteP * phiD, (1 - pDVoteP) * phiD);
|]

betaBinomialGeneratedQuantitiesBlock :: SB.GeneratedQuantitiesBlock
betaBinomialGeneratedQuantitiesBlock =
  [here|
  vector<lower = 0>[G] eTVotes;
  vector<lower = 0>[G] eDVotes;
  for (g in 1:G) {
    eTVotes[g] = pVotedP[g] * VAP[g];
    eDVotes[g] = pDVoteP[g] * TVotes[g];
  }
|]

betaBinomialGQLLBlock :: SB.GeneratedQuantitiesBlock
betaBinomialGQLLBlock =
  [here|
  vector[G] log_lik;
  for (g in 1:G) {
    log_lik[g] =  beta_binomial_lpmf(DVotes[g] | TVotes[g], pDVoteP[g] * phiD, (1 - pDVoteP[g]) * phiD) ;
  }
|]

betaBinomialIncParametersBlock :: SB.ParametersBlock
betaBinomialIncParametersBlock =
  [here|
  real alphaD;
  real <lower=0, upper=1> dispD;                             
  vector[K] thetaV;
  real alphaV;
  real <lower=0, upper=1> dispV;
  vector[K] thetaD;
  real incBetaD;
|]

betaBinomialIncTransformedParametersBlock :: SB.TransformedParametersBlock
betaBinomialIncTransformedParametersBlock =
  [here|
  real <lower=0> phiD = (1-dispD)/dispD;
  real <lower=0> phiV = (1-dispV)/dispV;
  vector [G] pDVoteP = inv_logit (alphaD + Q_ast * thetaD + to_vector(Inc) * incBetaD);
  vector [G] pVotedP = inv_logit (alphaV + Q_ast * thetaV);
  vector [K] betaV;
  vector [K] betaD;
  betaV = R_ast_inverse * thetaV;
  betaD = R_ast_inverse * thetaD;
|]

betaBinomialIncModelBlock :: SB.ModelBlock
betaBinomialIncModelBlock =
  [here|
  alphaD ~ cauchy(0, 10);
  alphaV ~ cauchy(0, 10);
  betaV ~ cauchy(0, 2.5);
  betaD ~ cauchy(0, 2.5);
  incBetaD ~ cauchy(0, 2.5);
  TVotes ~ beta_binomial(VAP, pVotedP * phiV, (1 - pVotedP) * phiV);
  DVotes ~ beta_binomial(TVotes, pDVoteP * phiD, (1 - pDVoteP) * phiD);
|]

betaBinomialIncGeneratedQuantitiesBlock :: SB.GeneratedQuantitiesBlock
betaBinomialIncGeneratedQuantitiesBlock =
  [here|
  vector<lower = 0>[G] eTVotes;
  vector<lower = 0>[G] eDVotes;
  for (g in 1:G) {
    eTVotes[g] = pVotedP[g] * VAP[g];
    eDVotes[g] = pDVoteP[g] * TVotes[g];
  }
  real avgPVoted = inv_logit (alphaV);
  real avgPDVote = inv_logit (alphaD);
  vector[K] deltaV;
  vector[K] deltaD;
  for (k in 1:K) {
    deltaV [k] = inv_logit (alphaV + sigma [k] * betaV [k]) - avgPVoted;
    deltaD [k] = inv_logit (alphaD + sigma [k] * betaD [k]) - avgPDVote;
  }
  real deltaIncD = inv_logit(alphaD + incBetaD) - avgPDVote;
|]

betaBinomialHSParametersBlock :: SB.ParametersBlock
betaBinomialHSParametersBlock =
  [here|
  real alphaD;
  real <lower=0, upper=1> dispD;                             
  vector[K] thetaV;
  vector<lower=0>[K] lambdaV;
  real<lower=0> tauV;
  real alphaV;
  real <lower=0, upper=1> dispV;
  vector[K] thetaD;
  vector<lower=0>[K] lambdaD;
  real<lower=0> tauD;
|]

betaBinomialHSModelBlock :: SB.ModelBlock
betaBinomialHSModelBlock =
  [here|
  alphaD ~ cauchy(0, 10);
  lambdaD ~ cauchy(0, 1);
  tauD ~ cauchy (0, 2.5);
  alphaV ~ cauchy(0, 10);
  lambdaV ~ cauchy(0, 2.5);
  tauV ~ cauchy (0,1);
  for (k in 1:K) {
    thetaV[k] ~ cauchy(0, lambdaV[k] * tauV);
    thetaD[k] ~ cauchy(0, lambdaD[k] * tauD);
  }
  TVotes ~ beta_binomial(VAP, pVotedP * phiV, (1 - pVotedP) * phiV);
  DVotes ~ beta_binomial(TVotes, pDVoteP * phiD, (1 - pDVoteP) * phiD);
|]

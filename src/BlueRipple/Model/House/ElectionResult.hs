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
import qualified Stan.Parameters as SP

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

type DVotes = "DVotes" F.:-> Int

type RVotes = "RVotes" F.:-> Int

type ElectionR = [DVotes, RVotes]

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
  K.Sem r (K.ActionWithCacheTime r (F.FrameRec (KeyR V.++ DemographicsR), F.FrameRec (KeyR V.++ ElectionR)))
prepCachedData = do
  pums_C <- PUMS.pumsLoaderAdults
  cdFromPUMA_C <- BR.allCDFromPUMA2012Loader
  let pumsByCDDeps = (,) <$> pums_C <*> cdFromPUMA_C
  pumsByCD_C <- BR.retrieveOrMakeFrame "model/house/pumsByCD.bin" pumsByCDDeps $ \(pums, cdFromPUMA) ->
    PUMS.pumsCDRollup ((>= 2012) . F.rgetField @BR.Year) (PUMS.pumsKeysToASER True) cdFromPUMA pums
  -- investigate college grad %

  pums <- K.ignoreCacheTime pums_C
  let yrState y sa r =
        F.rgetField @BR.Year r == y
          && F.rgetField @BR.StateAbbreviation r == sa
      pumsGA2018 = FL.fold (PUMS.pumsStateRollupF (PUMS.pumsKeysToASER True)) $ F.filterFrame (yrState 2018 "GA") pums
  BR.logFrame pumsGA2018

  --
  BR.clearIfPresentD "model/house/demographics.bin"
  demographicsByCD_C <- BR.retrieveOrMakeFrame "model/house/demographics.bin" pumsByCD_C $ return . FL.fold pumsF
  houseElections_C <- BR.houseElectionsLoader
  elections_C <- BR.retrieveOrMakeFrame "model/house/electionResults.bin" houseElections_C $ \houseElex -> do
    let electionResults = FL.fold electionF (F.filterFrame ((>= 2012) . F.rgetField @BR.Year) houseElex)
    return electionResults
  return $ (,) <$> demographicsByCD_C <*> elections_C

type HouseData = (F.FrameRec (KeyR V.++ DemographicsR), F.FrameRec BR.HouseElectionCols)

type HouseDataWrangler = SC.DataWrangler HouseData () ()

houseDataWrangler :: Int -> HouseDataWrangler
houseDataWrangler year = SC.Wrangle SC.NoIndex f
  where
    -- house cols
    district r = F.rgetField @BR.StateAbbreviation r <> (T.pack $ show $ F.rgetField @BR.CongressionalDistrict r)
    tVotes r = F.rgetField @DVotes r + F.rgetField @RVotes r
    enumStateF = FL.premap (F.rgetField @BR.StateAbbreviation) (SJ.enumerate 1)
    enumDistrictF = FL.premap district (SJ.enumerate 1)
    f (demographics, elex) = ((), makeDataJsonE)
      where
        (stateM, toState) = FL.fold enumStateF demographics
        (cdM, toCD) = FL.fold enumDistrictF demographics
        makeDataJsonE :: HouseData -> Either T.Text A.Series
        makeDataJsonE (demographics, elex) = do
          let flattenedElexFromYear = FL.fold electionF $ F.filterFrame ((== year) . F.rgetField @BR.Year) elex
              demographicsFromYear = F.filterFrame ((== year) . F.rgetField @BR.Year) demographics
              (demoAndElex, missing) = FJ.leftJoinWithMissing @KeyR demographicsFromYear flattenedElexFromYear
          _ <-
            if null missing
              then Left "Missing keys in leftJoinWithMissing demographics flattenedElexFrom2012"
              else Right ()
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
                  <> SJ.valueToPairF "TVotes" (SJ.jsonArrayF tVotes)
                  <> SJ.valueToPairF "DVotes" (SJ.jsonArrayF (F.rgetField @DVotes))
          SJ.frameToStanJSONSeries dataF demoAndElex

type VoteP = "VoteProb" F.:-> Double

type DVoteP = "DVoteProb" F.:-> Double

type EVotes = "EstVotes" F.:-> Int

type EDVotes = "EstDVotes" F.:-> Int

type EDVotes5 = "EstDVotes5" F.:-> Int

type EDVotes95 = "EstDVotes95" F.:-> Int

type Modeled = [VoteP, DVoteP, EVotes, EDVotes5, EDVotes, EDVotes95]

type HouseModelResults = [BR.StateAbbreviation, BR.CongressionalDistrict] V.++ Modeled

extractResults ::
  Int ->
  CS.StanSummary ->
  F.FrameRec (KeyR V.++ DemographicsR) ->
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
      makeRow (dByCDRow, (pV, pD, etVotes, dVotesPcts)) = do
        if length dVotesPcts == 3
          then
            let [d5, d, d95] = dVotesPcts
             in Right $
                  F.rgetField @BR.StateAbbreviation dByCDRow
                    F.&: F.rgetField @BR.CongressionalDistrict dByCDRow
                    F.&: pV
                    F.&: pD
                    F.&: round etVotes
                    F.&: round d5
                    F.&: round d
                    F.&: round d95
                    F.&: V.RNil
          else Left ("Wrong number of percentiles in stan statistic")
  result <- traverse makeRow $ zip (FL.fold FL.list demographicsByDistrict) modeledL
  return $ F.toFrame result

runHouseModel ::
  (K.KnitEffects r, K.CacheEffectsD r) =>
  HouseDataWrangler ->
  (T.Text, SB.StanModel) ->
  K.ActionWithCacheTime r HouseData ->
  K.Sem r (K.ActionWithCacheTime r (F.FrameRec HouseModelResults))
runHouseModel = undefined

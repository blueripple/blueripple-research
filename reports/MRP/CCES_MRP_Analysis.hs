{-# LANGUAGE AllowAmbiguousTypes       #-}
{-# LANGUAGE DataKinds                 #-}
{-# LANGUAGE DeriveDataTypeable        #-}
{-# LANGUAGE DeriveGeneric             #-}
{-# LANGUAGE PolyKinds                 #-}
{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE FlexibleInstances         #-}
{-# LANGUAGE GADTs                     #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE TypeApplications          #-}
{-# LANGUAGE TypeFamilies              #-}
{-# LANGUAGE TypeOperators             #-}
{-# LANGUAGE QuasiQuotes               #-}
{-# LANGUAGE StandaloneDeriving        #-}
{-# LANGUAGE TupleSections             #-}
{-# OPTIONS_GHC  -fplugin=Polysemy.Plugin  #-}

module MRP.CCES_MRP_Analysis where

import qualified Control.Foldl                 as FL
import qualified Data.Map                      as M
import           Data.Maybe                     ( isJust
                                                , catMaybes
                                                , fromMaybe
                                                )

import qualified Data.Text                     as T
import qualified Frames                        as F
import qualified Frames.Melt                   as F
import qualified Frames.InCore                 as FI
import qualified Data.Vinyl                    as V
import qualified Data.Vinyl.TypeLevel          as V

import qualified Frames.Transform              as FT
import qualified Frames.MapReduce              as FMR
import qualified Frames.Serialize              as FS

import qualified Knit.Report                   as K

import qualified Numeric.GLM.Bootstrap         as GLM

import qualified BlueRipple.Data.DataFrames    as BR
import qualified BlueRipple.Data.DemographicTypes
                                               as BR
import qualified BlueRipple.Data.ElectionTypes as ET
import qualified BlueRipple.Model.MRP_Pref     as BR
import           MRP.CCES
import           MRP.DeltaVPV                   ( DemVPV )

import qualified BlueRipple.Data.Keyed         as BR


countDemHouseVotesF
  :: forall cs
  . (Ord (F.Record cs)
    , FI.RecVec (cs V.++ BR.CountCols)
    , cs F.⊆ CCES_MRP
    )
  => Int
  -> FMR.Fold
  (F.Record CCES_MRP)
  (F.FrameRec ('[BR.StateAbbreviation] V.++ cs V.++ BR.CountCols))
countDemHouseVotesF y =
  BR.weightedCountFold @('[BR.StateAbbreviation] V.++ cs) @CCES_MRP
    @'[HouseVoteParty, CCESWeightCumulative]
    (\r ->
      (F.rgetField @BR.Year r == y)
        && (F.rgetField @HouseVoteParty r `elem` [ET.Republican, ET.Democratic])
    )
    ((== ET.Democratic) . F.rgetField @HouseVoteParty)
    (F.rgetField @CCESWeightCumulative)


countDemPres2008VotesF
  :: forall cs
  . (Ord (F.Record cs)
    , FI.RecVec (cs V.++ BR.CountCols)
    , cs F.⊆ CCES_MRP
    )
  => FMR.Fold
  (F.Record CCES_MRP)
  (F.FrameRec ('[BR.StateAbbreviation] V.++ cs V.++ BR.CountCols))
countDemPres2008VotesF =
  BR.weightedCountFold @('[BR.StateAbbreviation] V.++ cs) @CCES_MRP
    @'[Pres2008VoteParty, CCESWeightCumulative]
    (\r ->
      (F.rgetField @BR.Year r == 2008)
        && (      F.rgetField @Pres2008VoteParty r
           `elem` [ET.Republican, ET.Democratic]
           )
    )
    ((== ET.Democratic) . F.rgetField @Pres2008VoteParty)
    (F.rgetField @CCESWeightCumulative)

countDemPres2012VotesF
  :: forall cs
  . (Ord (F.Record cs)
    , FI.RecVec (cs V.++ BR.CountCols)
    , cs F.⊆ CCES_MRP
    )
  => FMR.Fold
  (F.Record CCES_MRP)
  (F.FrameRec ('[BR.StateAbbreviation] V.++ cs V.++ BR.CountCols))
countDemPres2012VotesF =
  BR.weightedCountFold @('[BR.StateAbbreviation] V.++ cs) @CCES_MRP
    @'[Pres2012VoteParty, CCESWeightCumulative]
    (\r ->
      (F.rgetField @BR.Year r == 2012)
        && (      F.rgetField @Pres2012VoteParty r
           `elem` [ET.Republican, ET.Democratic]
           )
    )
    ((== ET.Democratic) . F.rgetField @Pres2012VoteParty)
    (F.rgetField @CCESWeightCumulative)

countDemPres2016VotesF
  :: forall cs
  . (Ord (F.Record cs)
    , FI.RecVec (cs V.++ BR.CountCols)
    , cs F.⊆ CCES_MRP
    )
  => FMR.Fold
  (F.Record CCES_MRP)
  (F.FrameRec ('[BR.StateAbbreviation] V.++ cs V.++ BR.CountCols))
countDemPres2016VotesF =
  BR.weightedCountFold @('[BR.StateAbbreviation] V.++ cs) @CCES_MRP
    @'[Pres2016VoteParty, CCESWeightCumulative]
    (\r ->
      (F.rgetField @BR.Year r == 2016)
        && (      F.rgetField @Pres2016VoteParty r
           `elem` [ET.Republican, ET.Democratic]
           )
    )
    ((== ET.Democratic) . F.rgetField @Pres2016VoteParty)
    (F.rgetField @CCESWeightCumulative)

mrpPrefs
  :: forall cc r
   . ( K.KnitEffects r
     , K.Member GLM.RandomFu r
     , ( (((cc V.++ '[BR.Year]) V.++ '[ET.Office]) V.++ '[DemVPV])
           V.++
           '[BR.DemPref]
       )
         ~
         (cc V.++ '[BR.Year, ET.Office, DemVPV, BR.DemPref])
     , FS.RecSerialize (cc V.++ '[BR.Year, ET.Office, DemVPV, BR.DemPref])
     , FI.RecVec (cc V.++ '[BR.Year, ET.Office, DemVPV, BR.DemPref])
     , V.RMap (cc V.++ '[BR.Year, ET.Office, DemVPV, BR.DemPref])
     , cc F.⊆ (LocationCols V.++ cc V.++ BR.CountCols)
     , cc F.⊆ (cc V.++ BR.CountCols)
     , (cc V.++ BR.CountCols) F.⊆ (LocationCols V.++ cc V.++ BR.CountCols)
     , (cc V.++ BR.CountCols) F.⊆ (LocationCols V.++ [BR.SimpleAgeC, BR.SexC, BR.CollegeGradC, BR.SimpleRaceC]  V.++ BR.CountCols)
     , FI.RecVec (cc V.++ BR.CountCols)
     , F.ElemOf (cc V.++ BR.CountCols) BR.Count
     , F.ElemOf (cc V.++ BR.CountCols) BR.MeanWeight
     , F.ElemOf (cc V.++ BR.CountCols) BR.UnweightedSuccesses
     , F.ElemOf (cc V.++ BR.CountCols) BR.VarWeight
     , F.ElemOf (cc V.++ BR.CountCols) BR.WeightedSuccesses
     , BR.FiniteSet (F.Record cc)
     , Show (F.Record (cc V.++ BR.CountCols))
     , V.RMap (cc V.++ BR.CountCols)
     , V.ReifyConstraint Show F.ElField (cc V.++ BR.CountCols)
     , V.RecordToList (cc V.++ BR.CountCols) 
     , V.RMap cc
     , V.ReifyConstraint Show V.ElField cc
     , V.RecordToList cc
     , cc F.⊆ CCES_MRP
     , Ord (F.Record cc)
     )
  => Maybe T.Text
  -> K.Sem r (F.FrameRec CCES_MRP)
  -> [CCESSimpleEffect cc]
  -> M.Map (F.Record cc) (M.Map (CCESSimplePredictor cc) Double)
  -> K.Sem
       r
       ( F.FrameRec
           ( '[BR.StateAbbreviation]
               V.++
               cc
               V.++
               '[BR.Year, ET.Office, DemVPV, BR.DemPref]
           )
       )
mrpPrefs cacheTmpDirM ccesDataAction predictor catPredMap = do
  let vpv x = 2 * x - 1
      lhToRecs year office (LocationHolder lp lkM predMap) =
        let addCols p =
              FT.mutate (const $ FT.recordSingleton @BR.DemPref p)
                . FT.mutate (const $ FT.recordSingleton @DemVPV (vpv p))
                . FT.mutate (const $ FT.recordSingleton @ET.Office office)
                . FT.mutate (const $ FT.recordSingleton @BR.Year year)
            g lkM =
              let lk = fromMaybe (lp F.&: V.RNil) lkM
              in  fmap (\(ck, p) -> addCols p (lk `V.rappend` ck))
                    $ M.toList predMap
        in  g lkM
      lhsToFrame y o = F.toFrame . concat . fmap (lhToRecs y o)
  K.logLE K.Info "Doing ASER MR..."
  let cacheIt cn fa = 
        case cacheTmpDirM of
          Nothing -> fa
          Just tmpDir -> K.retrieveOrMakeTransformed
                         (fmap FS.toS . FL.fold FL.list)
                         (F.toFrame . fmap FS.fromS)
                         ("mrp/tmp/" <> tmpDir <> "/" <> cn)
                         fa
  let p2008 = cacheIt
              "pres2008"
              (   lhsToFrame 2008 ET.President
                <$> (predictionsByLocation ccesDataAction
                      (countDemPres2008VotesF @cc)
                      predictor
                      catPredMap
                    )
              )
      p2012 = cacheIt
              "pres2012"
              (   lhsToFrame 2012 ET.President
                <$> (predictionsByLocation ccesDataAction
                      (countDemPres2012VotesF @cc)
                      predictor
                      catPredMap
                    )
              )
      p2016 = cacheIt
              "pres2016"
              (   lhsToFrame 2016 ET.President
                <$> (predictionsByLocation ccesDataAction
                      (countDemPres2016VotesF @cc)
                      predictor
                      catPredMap
                    )
              )
      pHouse = fmap
               (\y -> cacheIt
                 ("house" <> T.pack (show y))
                 (   lhsToFrame y ET.House
                   <$> (predictionsByLocation ccesDataAction
                         (countDemHouseVotesF @cc y)
                         predictor
                         catPredMap
                       )
                 )
                )
                [2008, 2010, 2012, 2014, 2016, 2018]
      allActions = [p2008, p2012, p2016] ++ pHouse
{-      
  allResults <- sequence allActions
  return $ mconcat allResults
-}
      
  allResultsM <- sequence <$> K.sequenceConcurrently allActions
  case allResultsM of
    Nothing -> K.knitError "Error in MR run (mrpPrefs)."
    Just allResults -> return $ mconcat allResults

{-
return
    $  predsByLocationPres2008
    <> predsByLocationPres2012
    <> predsByLocationPres2016
    <> mconcat predsByLocationHouse
-}



countVotersF
  :: forall cs
  . (Ord (F.Record cs)
    , FI.RecVec (cs V.++ BR.CountCols)
    , cs F.⊆ CCES_MRP
    )
  => Int
  -> FMR.Fold
  (F.Record CCES_MRP)
  (F.FrameRec ('[BR.StateAbbreviation] V.++ cs V.++ BR.CountCols))
countVotersF year =
  let isYear y r = F.rgetField @BR.Year r == y
      inVoterFile r = isYear year r &&  (F.rgetField @Turnout r `elem` [T_Voted, T_NoRecord])
      voted r = isYear year r && (F.rgetField @Turnout r == T_Voted)
      wgt = F.rgetField @CCESWeightCumulative 
  in BR.weightedCountFold @('[BR.StateAbbreviation] V.++ cs) @CCES_MRP @'[Turnout, BR.Year, CCESWeightCumulative] inVoterFile voted wgt



mrpTurnout
  :: forall cc r
   . ( K.KnitEffects r
     , K.Member GLM.RandomFu r
     , ( (((cc V.++ '[BR.Year]) V.++ '[ET.ElectoralWeightSource]) V.++ '[ET.ElectoralWeightOf])
           V.++
           '[ET.ElectoralWeight]
       )
         ~
         (cc V.++ '[BR.Year, ET.ElectoralWeightSource, ET.ElectoralWeightOf, ET.ElectoralWeight])
     , FS.RecSerialize (cc V.++ '[BR.Year, ET.ElectoralWeightSource, ET.ElectoralWeightOf, ET.ElectoralWeight])
     , FI.RecVec (cc V.++ '[BR.Year, ET.ElectoralWeightSource, ET.ElectoralWeightOf, ET.ElectoralWeight])
     , V.RMap (cc V.++ '[BR.Year, ET.ElectoralWeightSource, ET.ElectoralWeightOf, ET.ElectoralWeight])
     , cc F.⊆ (LocationCols V.++ cc V.++ BR.CountCols)
     , cc F.⊆ (cc V.++ BR.CountCols)
     , (cc V.++ BR.CountCols) F.⊆ (LocationCols V.++ cc V.++ BR.CountCols)
     , (cc V.++ BR.CountCols) F.⊆ (LocationCols V.++ [BR.SimpleAgeC, BR.SexC, BR.CollegeGradC, BR.SimpleRaceC]  V.++ BR.CountCols)
     , FI.RecVec (cc V.++ BR.CountCols)
     , F.ElemOf (cc V.++ BR.CountCols) BR.Count
     , F.ElemOf (cc V.++ BR.CountCols) BR.MeanWeight
     , F.ElemOf (cc V.++ BR.CountCols) BR.UnweightedSuccesses
     , F.ElemOf (cc V.++ BR.CountCols) BR.VarWeight
     , F.ElemOf (cc V.++ BR.CountCols) BR.WeightedSuccesses
     , BR.FiniteSet (F.Record cc)
     , Show (F.Record (cc V.++ BR.CountCols))
     , V.RMap (cc V.++ BR.CountCols)
     , V.ReifyConstraint Show F.ElField (cc V.++ BR.CountCols)
     , V.RecordToList (cc V.++ BR.CountCols) 
     , V.RMap cc
     , V.ReifyConstraint Show V.ElField cc
     , V.RecordToList cc
     , cc F.⊆ CCES_MRP
     , Ord (F.Record cc)
     )
  => Maybe T.Text
  -> K.Sem r (F.FrameRec CCES_MRP)
  -> [CCESSimpleEffect cc]
  -> M.Map (F.Record cc) (M.Map (CCESSimplePredictor cc) Double)
  -> K.Sem
       r
       ( F.FrameRec
           ( '[BR.StateAbbreviation]
               V.++
               cc
               V.++
               '[BR.Year,  ET.ElectoralWeightSource, ET.ElectoralWeightOf, ET.ElectoralWeight]
           )
       )
mrpTurnout cacheTmpDirM ccesDataAction predictor catPredMap = do
  let lhToRecs year (LocationHolder lp lkM predMap) =
        let recToAdd :: Double -> F.Record [BR.Year, ET.ElectoralWeightSource, ET.ElectoralWeightOf, ET.ElectoralWeight]
            recToAdd w = year F.&: (ET.ewRec ET.EW_CCES ET.EW_Citizen w)
            addCols w r = r `V.rappend` (recToAdd w)
            g x =
              let lk = fromMaybe (lp F.&: V.RNil) x
              in  fmap (\(ck, p) -> addCols p (lk `V.rappend` ck))
                  $ M.toList predMap
        in  g lkM
      lhsToFrame y = F.toFrame . concat . fmap (lhToRecs y)
  K.logLE K.Info "(Turnout) Doing MR..."
  let cacheIt cn fa = 
        case cacheTmpDirM of
          Nothing -> fa
          Just tmpDir -> K.retrieveOrMakeTransformed
                         (fmap FS.toS . FL.fold FL.list)
                         (F.toFrame . fmap FS.fromS)
                         ("mrp/tmp/" <> tmpDir <> "/" <> cn)
                         fa
      wYearActions = fmap
                     (\y -> cacheIt
                       ("turnout" <> T.pack (show y))
                       (   lhsToFrame y 
                         <$> (predictionsByLocation ccesDataAction
                              (countVotersF @cc y)
                              predictor
                              catPredMap
                             )
                       )
                     )
                     [2008, 2010, 2012, 2014, 2016, 2018]
{-      
  allResults <- sequence allActions
  return $ mconcat allResults
-}
      
  allResultsM <- sequence <$> K.sequenceConcurrently wYearActions
  case allResultsM of
    Nothing -> K.knitError "Error in MR run (mrpPrefs)."
    Just allResults -> return $ mconcat allResults

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
{-# OPTIONS_GHC -fplugin=Polysemy.Plugin #-}
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
import qualified Numeric.GLM.MixedModel         as GLM

import qualified BlueRipple.Data.DataFrames    as BR
import qualified BlueRipple.Data.DemographicTypes
                                               as BR
import qualified BlueRipple.Data.ElectionTypes as ET
import qualified BlueRipple.Model.MRP     as BR
import           MRP.CCES


import qualified BlueRipple.Data.Keyed         as BR


countDemHouseVotesF
  :: forall cs
  . (Ord (F.Record cs)
    , FI.RecVec (cs V.++ BR.CountCols)
    , cs F.⊆ CCES_MRP
    , cs F.⊆ ('[BR.StateAbbreviation] V.++ cs V.++ CCES_MRP)
    , F.ElemOf (cs V.++ CCES_MRP) HouseVoteParty
    , F.ElemOf (cs V.++ CCES_MRP) CCESWeightCumulative
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
    , cs F.⊆ ('[BR.StateAbbreviation] V.++ cs V.++ CCES_MRP)
    , cs F.⊆ CCES_MRP
    , F.ElemOf (cs V.++ CCES_MRP) Pres2008VoteParty
    , F.ElemOf (cs V.++ CCES_MRP) CCESWeightCumulative
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
    , cs F.⊆ ('[BR.StateAbbreviation] V.++ cs V.++ CCES_MRP)
    , F.ElemOf (cs V.++ CCES_MRP) Pres2012VoteParty
    , F.ElemOf (cs V.++ CCES_MRP) CCESWeightCumulative
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
    , cs F.⊆ ('[BR.StateAbbreviation] V.++ cs V.++ CCES_MRP)
    , F.ElemOf (cs V.++ CCES_MRP) Pres2016VoteParty
    , F.ElemOf (cs V.++ CCES_MRP) CCESWeightCumulative
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
     , K.CacheEffectsD r
     , K.Member GLM.RandomFu r
     , ( (((cc V.++ '[BR.Year]) V.++ '[ET.Office]) V.++ '[ET.DemVPV])
           V.++
           '[BR.DemPref]
       )
         ~
         (cc V.++ '[BR.Year, ET.Office, ET.DemVPV, BR.DemPref])
     , FS.RecSerialize (cc V.++ '[BR.Year, ET.Office, ET.DemVPV, BR.DemPref])
     , FI.RecVec (cc V.++ '[BR.Year, ET.Office, ET.DemVPV, BR.DemPref])
     , V.RMap (cc V.++ '[BR.Year, ET.Office, ET.DemVPV, BR.DemPref])
     , cc F.⊆ (BR.LocationCols V.++ cc V.++ BR.CountCols)
     , cc F.⊆ (cc V.++ BR.CountCols)
     , (cc V.++ BR.CountCols) F.⊆ (BR.LocationCols V.++ cc V.++ BR.CountCols)
     , cc F.⊆ ('[BR.StateAbbreviation] V.++ cc V.++ CCES_MRP)
     , FI.RecVec (cc V.++ BR.CountCols)
     , F.ElemOf (cc V.++ BR.CountCols) BR.Count
     , F.ElemOf (cc V.++ BR.CountCols) BR.MeanWeight
     , F.ElemOf (cc V.++ BR.CountCols) BR.UnweightedSuccesses
     , F.ElemOf (cc V.++ BR.CountCols) BR.VarWeight
     , F.ElemOf (cc V.++ BR.CountCols) BR.WeightedSuccesses
     , F.ElemOf (cc V.++ CCES_MRP) HouseVoteParty
     , F.ElemOf (cc V.++ CCES_MRP) Pres2008VoteParty
     , F.ElemOf (cc V.++ CCES_MRP) Pres2012VoteParty
     , F.ElemOf (cc V.++ CCES_MRP) Pres2016VoteParty
     , F.ElemOf (cc V.++ CCES_MRP) CCESWeightCumulative
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
  => GLM.MinimizeDevianceVerbosity
  -> Maybe T.Text
  -> K.ActionWithCacheTime r (F.FrameRec CCES_MRP)
  -> [BR.SimpleEffect cc]
  -> M.Map (F.Record cc) (M.Map (BR.SimplePredictor cc) Double)
  -> K.Sem
       r
       ( F.FrameRec
           ( '[BR.StateAbbreviation]
               V.++
               cc
               V.++
               '[BR.Year, ET.Office, ET.DemVPV, BR.DemPref]
           )
       )
mrpPrefs mdv cacheTmpDirM cachedCCES_Data predictor catPredMap = do
  let vpv x = 2 * x - 1
      lhToRecs year office (BR.LocationHolder lp lkM predMap) =
        let addCols p =
              FT.mutate (const $ FT.recordSingleton @BR.DemPref p)
                . FT.mutate (const $ FT.recordSingleton @ET.DemVPV (vpv p))
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
          Nothing -> K.ignoreCacheTime cachedCCES_Data >>= fa
          Just tmpDir -> K.ignoreCacheTimeM
                         $ K.retrieveOrMakeTransformed
                         (fmap FS.toS . FL.fold FL.list)
                         (F.toFrame . fmap FS.fromS)
                         ("mrp/tmp/" <> tmpDir <> "/" <> cn)
                         cachedCCES_Data
                         fa
  let p2008 = cacheIt
              "pres2008"
              (\ccesData -> lhsToFrame 2008 ET.President
                <$> (BR.predictionsByLocation mdv ccesData
                      (countDemPres2008VotesF @cc)
                      predictor
                      catPredMap
                    )
              )
      p2012 = cacheIt
              "pres2012"
              (\ccesData -> lhsToFrame 2012 ET.President
                <$> (BR.predictionsByLocation mdv ccesData
                      (countDemPres2012VotesF @cc)
                      predictor
                      catPredMap
                    )
              )
      p2016 = cacheIt
              "pres2016"
              (\ccesData -> lhsToFrame 2016 ET.President
                <$> (BR.predictionsByLocation mdv ccesData
                      (countDemPres2016VotesF @cc)
                      predictor
                      catPredMap
                    )
              )
      pHouse = fmap
               (\y -> cacheIt
                 ("house" <> T.pack (show y))
                 (\ccesData -> lhsToFrame y ET.House
                   <$> (BR.predictionsByLocation mdv ccesData
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



countVotersOfValidatedF
  :: forall cs
  . (Ord (F.Record cs)
    , FI.RecVec (cs V.++ BR.CountCols)
    , cs F.⊆ CCES_MRP
    , cs F.⊆ ('[BR.StateAbbreviation] V.++ cs V.++ CCES_MRP)
    , F.ElemOf (cs V.++ CCES_MRP) Turnout
    , F.ElemOf (cs V.++ CCES_MRP) CCESWeightCumulative
    , F.ElemOf (cs V.++ CCES_MRP) BR.Year
    )
  => Int
  -> FMR.Fold
  (F.Record CCES_MRP)
  (F.FrameRec ('[BR.StateAbbreviation] V.++ cs V.++ BR.CountCols))
countVotersOfValidatedF year =
  let isYear y r = F.rgetField @BR.Year r == y
      inVoterFile r = isYear year r &&  (F.rgetField @Turnout r `elem` [T_Voted, T_NoRecord])
      voted r = isYear year r && (F.rgetField @Turnout r == T_Voted)
      wgt = F.rgetField @CCESWeightCumulative 
  in BR.weightedCountFold @('[BR.StateAbbreviation] V.++ cs) @CCES_MRP @'[Turnout, BR.Year, CCESWeightCumulative] inVoterFile voted wgt


countVotersOfAllF
  :: forall cs
  . (Ord (F.Record cs)
    , FI.RecVec (cs V.++ BR.CountCols)
    , cs F.⊆ CCES_MRP
    , cs F.⊆ ('[BR.StateAbbreviation] V.++ cs V.++ CCES_MRP)
    , F.ElemOf (cs V.++ CCES_MRP) Turnout
    , F.ElemOf (cs V.++ CCES_MRP) CCESWeightCumulative
    , F.ElemOf (cs V.++ CCES_MRP) BR.Year
    )
  => Int
  -> FMR.Fold
  (F.Record CCES_MRP)
  (F.FrameRec ('[BR.StateAbbreviation] V.++ cs V.++ BR.CountCols))
countVotersOfAllF year =
  let isYear y r = F.rgetField @BR.Year r == y
--      inVoterFile r = isYear year r &&  (F.rgetField @Turnout r `elem` [T_Voted, T_NoRecord])
      voted r = isYear year r && (F.rgetField @Turnout r == T_Voted)
      wgt = F.rgetField @CCESWeightCumulative 
  in BR.weightedCountFold @('[BR.StateAbbreviation] V.++ cs) @CCES_MRP @'[Turnout, BR.Year, CCESWeightCumulative] (isYear year) voted wgt

{-

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
     , cc F.⊆ (BR.LocationCols V.++ cc V.++ BR.CountCols)
     , cc F.⊆ (cc V.++ BR.CountCols)
     , (cc V.++ BR.CountCols) F.⊆ (BR.LocationCols V.++ cc V.++ BR.CountCols)
     , (cc V.++ BR.CountCols) F.⊆ (BR.LocationCols V.++ [BR.SimpleAgeC, BR.SexC, BR.CollegeGradC, BR.SimpleRaceC]  V.++ BR.CountCols)
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
  -> [BR.SimpleEffect cc]
  -> M.Map (F.Record cc) (M.Map (BR.SimplePredictor cc) Double)
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
  let lhToRecs year (BR.LocationHolder lp lkM predMap) =
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
                         <$> (BR.predictionsByLocation ccesDataAction
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
-}

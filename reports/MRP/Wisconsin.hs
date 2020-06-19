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

module MRP.Wisconsin where

import qualified Control.Foldl                 as FL
import qualified Data.List as L
import           Data.Maybe (catMaybes)
import qualified Data.Text                     as T

import           Graphics.Vega.VegaLite.Configuration as FV
import qualified Frames as F
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V

import qualified Frames.Transform              as FT
import qualified Frames.Folds                  as FF
import qualified Frames.MapReduce              as FMR
import qualified Frames.Serialize              as FS

import qualified Knit.Report                   as K
import qualified Polysemy.Error                as P (mapError)
import qualified Polysemy                      as P (raise)

import           Data.String.Here               ( i )

import           BlueRipple.Configuration 
import           BlueRipple.Utilities.KnitUtils 

import qualified Numeric.GLM.ProblemTypes      as GLM
import qualified Numeric.GLM.Bootstrap            as GLM
import qualified Numeric.GLM.MixedModel as GLM

import qualified BlueRipple.Utilities.KnitUtils as BR
import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Data.Loaders as BR
import qualified BlueRipple.Data.DemographicTypes as BR
import qualified BlueRipple.Data.ElectionTypes as ET
import qualified BlueRipple.Model.MRP as BR
import qualified BlueRipple.Model.PostStratify as BR
import qualified BlueRipple.Data.UsefulDataJoins as BR
import qualified MRP.CCES_MRP_Analysis as BR

import MRP.CCES


import qualified Visualizations.StatePrefs as BR

text1 :: T.Text
text1 = [i|
|]

  
foldPrefAndTurnoutData :: FF.EndoFold (F.Record '[BR.ACSCount, BR.VotedPctOfAll, ET.DemVPV, BR.DemPref])
foldPrefAndTurnoutData =  FF.sequenceRecFold
                          $ FF.toFoldRecord (FL.premap (F.rgetField @BR.ACSCount) FL.sum)
                          V.:& FF.toFoldRecord (BR.weightedSumRecF @BR.ACSCount @BR.VotedPctOfAll)
                          V.:& FF.toFoldRecord (BR.weightedSumRecF @BR.ACSCount @ET.DemVPV)
                          V.:& FF.toFoldRecord (BR.weightedSumRecF @BR.ACSCount @BR.DemPref)
                          V.:& V.RNil

post :: forall es r.(K.KnitOne r , K.Member GLM.RandomFu r) => K.Sem r ()
post = P.mapError BR.glmErrorToPandocError $ K.wrapPrefix "Wisconsin" $ do
  let stateAbbr = "WI"
      stateOnly = F.filterFrame (\r -> F.rgetField @BR.StateAbbreviation r == stateAbbr)
      stateAndNation = F.filterFrame (\r -> F.rgetField @BR.StateAbbreviation r `L.elem` [stateAbbr, "National"])
{-      
  stateTurnoutRaw <- BR.stateTurnoutLoader -- P.raise $ K.useCached stateTurnoutCA
  aseACS <- BR.simpleASEDemographicsLoader 
  asrACS <- BR.simpleASRDemographicsLoader 
  aseTurnout <- BR.simpleASETurnoutLoader 
  asrTurnout <- BR.simpleASRTurnoutLoader
-}
  let showRecs = T.intercalate "\n" . fmap (T.pack . show) . FL.fold FL.list
  let predictorsASER = GLM.Intercept : fmap GLM.Predictor (BR.allSimplePredictors @BR.CatColsASER)
      predictorsASE =  GLM.Intercept : fmap GLM.Predictor (BR.allSimplePredictors @BR.CatColsASE)
      predictorsASR = GLM.Intercept : fmap GLM.Predictor (BR.allSimplePredictors @BR.CatColsASR)
  (K.WithCacheTime ccesTime ccesDataA) <- ccesDataLoader
  inferredPrefsASER <-  K.getCachedAction $ do
    ccesData <- ccesDataA
    BR.retrieveOrMakeFrame "mrp/simpleASER_MR.bin" (Just ccesTime) $ 
      stateAndNation <$> (P.raise $ BR.mrpPrefs @BR.CatColsASER GLM.MDVNone (Just "ASER") (Just ccesTime) ccesData predictorsASER BR.catPredMaps)

  inferredPrefsASE <-  K.getCachedAction $ do
    ccesData <- ccesDataA
    BR.retrieveOrMakeFrame "mrp/simpleASE_MR.bin" (Just ccesTime) $
      stateAndNation <$> (P.raise $ BR.mrpPrefs @BR.CatColsASE GLM.MDVNone (Just "ASE") (Just ccesTime) ccesData predictorsASE BR.catPredMaps)      

  inferredPrefsASR <-  K.getCachedAction $ do
        ccesData <- ccesDataA
        BR.retrieveOrMakeFrame "mrp/simpleASR_MR.bin" (Just ccesTime) $
          stateAndNation <$> (P.raise $ BR.mrpPrefs @BR.CatColsASR GLM.MDVNone (Just "ASR") (Just ccesTime) ccesData predictorsASR BR.catPredMaps)
--  inferredPrefsASER <- K.ignoreCacheTime inferredPrefsASER_C
  brAddMarkDown text1
  _ <- K.addHvega Nothing Nothing $ BR.vlPrefVsTime "Dem Preference By Demographic Split" stateAbbr (FV.ViewConfig 800 800 10) $ fmap F.rcast inferredPrefsASER  
  -- get adjusted turnouts (national rates, adj by state) for each CD
  demographicsAndTurnoutASE <- K.getCachedAction $ do
    K.WithCacheTime aseACS_Time aseACS_A <- BR.simpleASEDemographicsLoader
    K.WithCacheTime aseTurnoutTime aseTurnoutA <- BR.simpleASETurnoutLoader
    K.WithCacheTime stateTurnoutTime stateTurnoutA <- BR.stateTurnoutLoader
    aseACS <- aseACS_A
    aseTurnout <- aseTurnoutA
    stateTurnout <- stateTurnoutA
    let newestDepTime = maximum [aseACS_Time, aseTurnoutTime, stateTurnoutTime]
    fmap (fmap stateOnly) <$> BR.cachedASEDemographicsWithAdjTurnoutByCD (Just newestDepTime) aseACS aseTurnout stateTurnout

  demographicsAndTurnoutASR <- K.getCachedAction $ do
    K.WithCacheTime asrACS_Time asrACS_A <- BR.simpleASRDemographicsLoader
    K.WithCacheTime asrTurnoutTime asrTurnoutA <- BR.simpleASRTurnoutLoader
    K.WithCacheTime stateTurnoutTime stateTurnoutA <- BR.stateTurnoutLoader
    asrACS <- asrACS_A
    asrTurnout <- asrTurnoutA
    stateTurnout <- stateTurnoutA
    let newestDepTime = maximum [asrACS_Time, asrTurnoutTime, stateTurnoutTime]
    fmap (fmap stateOnly) <$> BR.cachedASRDemographicsWithAdjTurnoutByCD (Just newestDepTime) asrACS asrTurnout stateTurnout

  -- join with the prefs
  K.logLE K.Info "Joining turnout by CD and prefs"
  let aseTurnoutAndPrefs = catMaybes
                           $ fmap F.recMaybe
                           $ F.leftJoin @([BR.StateAbbreviation, BR.Year] V.++ BR.CatColsASE) demographicsAndTurnoutASE inferredPrefsASE
      asrTurnoutAndPrefs = catMaybes
                           $ fmap F.recMaybe
                           $ F.leftJoin @([BR.StateAbbreviation, BR.Year] V.++ BR.CatColsASR) demographicsAndTurnoutASR inferredPrefsASR
      labelPSBy x = V.rappend (FT.recordSingleton @ET.PrefType x)
      psCellVPVByBothF =  (<>)
                          <$> fmap pure (fmap (labelPSBy ET.PSByVAP)
                                         $ BR.postStratifyCell @ET.DemVPV
                                         (realToFrac . F.rgetField @BR.ACSCount)
                                         (realToFrac . F.rgetField @ET.DemVPV))
                          <*> fmap pure (fmap (labelPSBy ET.PSByVoted)
                                         $ BR.postStratifyCell @ET.DemVPV
                                         (\r -> realToFrac (F.rgetField @BR.ACSCount r) * F.rgetField @BR.VotedPctOfAll r)
                                         (realToFrac . F.rgetField @ET.DemVPV))
      psVPVByDistrictF =  BR.postStratifyF
                          @[BR.Year, ET.Office, BR.StateAbbreviation, BR.StateFIPS, BR.CongressionalDistrict]
                          @[ET.DemVPV, BR.ACSCount, BR.VotedPctOfAll]
                          @[ET.PrefType, ET.DemVPV]
                          psCellVPVByBothF
      vpvPostStratifiedByASE = FL.fold psVPVByDistrictF aseTurnoutAndPrefs
      vpvPostStratifiedByASR = FL.fold psVPVByDistrictF asrTurnoutAndPrefs
--      plotEmAll :: Int -> ET.OfficeT -> K.Sem r ()
      plotEmAll y o r = do
        let tStart = (T.pack $ show y) <> " " <> (T.pack $ show o)
            cs = Just $ BR.vpvChoroColorScale (negate r) r
            vc = FV.ViewConfig 800 800 10
        _ <- K.addHvega Nothing Nothing
             $ BR.vlByCD @ET.DemVPV (tStart <> " District VPV (Voting Age Pop, Post-Stratified by Age, Sex, Education)") cs vc
             $ F.filterFrame (\r -> (F.rgetField @BR.Year r == y)
                                    && (F.rgetField @ET.Office r == o)
                                    && (F.rgetField @ET.PrefType r == ET.PSByVAP)
                             ) vpvPostStratifiedByASE
        return ()                     
        _ <- K.addHvega Nothing Nothing
             $ BR.vlByCD @ET.DemVPV (tStart <> " District VPV (Voting Age Pop, Post-Stratified by Age, Sex, Race)") cs vc
             $ F.filterFrame (\r -> (F.rgetField @BR.Year r == y)
                               && (F.rgetField @ET.Office r == o)
                               && (F.rgetField @ET.PrefType r == ET.PSByVAP)
                             ) vpvPostStratifiedByASR
        _ <- K.addHvega Nothing Nothing
          $ BR.vlByCD @ET.DemVPV (tStart <> " District VPV (Voted, Post-Stratified by Age, Sex, Education)") cs vc
          $ F.filterFrame (\r -> (F.rgetField @BR.Year r == 2016)
                                 && (F.rgetField @ET.Office r == ET.President)
                                 && (F.rgetField @ET.PrefType r == ET.PSByVoted)
                          ) vpvPostStratifiedByASE
        _ <- K.addHvega Nothing Nothing
          $ BR.vlByCD @ET.DemVPV (tStart <> " District VPV (Voted, Post-Stratified by Age, Sex, Race)") cs vc
          $ F.filterFrame (\r -> (F.rgetField @BR.Year r == 2016)
                            && (F.rgetField @ET.Office r == ET.President)
                            && (F.rgetField @ET.PrefType r == ET.PSByVoted)
                          ) vpvPostStratifiedByASR
        return ()
  plotEmAll 2016 ET.President 0.25
  plotEmAll 2018 ET.House 0.35
  let aseDemoF = FMR.concatFold $ FMR.mapReduceFold
              (FMR.unpackFilterRow ((== 2018) . F.rgetField @BR.Year))
              (FMR.assignKeysAndData @BR.CatColsASE @[BR.ACSCount, BR.VotedPctOfAll, ET.DemVPV, BR.DemPref])
              (FMR.foldAndAddKey foldPrefAndTurnoutData)
  let asrDemoF = FMR.concatFold $ FMR.mapReduceFold
              (FMR.unpackFilterRow ((== 2018) . F.rgetField @BR.Year))
              (FMR.assignKeysAndData @BR.CatColsASR @[BR.ACSCount, BR.VotedPctOfAll, ET.DemVPV, BR.DemPref])
              (FMR.foldAndAddKey foldPrefAndTurnoutData)              
      asrSums = FL.fold asrDemoF asrTurnoutAndPrefs
      aseSums = FL.fold aseDemoF aseTurnoutAndPrefs
  K.logLE K.Info $ T.intercalate "\n" $ fmap (T.pack . show) $ FL.fold FL.list aseSums
  K.logLE K.Info $ T.intercalate "\n" $ fmap (T.pack . show) $ FL.fold FL.list asrSums
  brAddMarkDown brReadMore


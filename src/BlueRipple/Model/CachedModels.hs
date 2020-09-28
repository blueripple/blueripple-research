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
{-# OPTIONS_GHC  -O0 -fplugin=Polysemy.Plugin  #-}

module BlueRipple.Model.CachedModels where

import qualified Control.Foldl                 as FL
import Control.Monad (join)
import qualified Data.List as L
import qualified Data.Map                      as M
import qualified Data.Text                     as T

import           Graphics.Vega.VegaLite.Configuration as FV
import qualified Graphics.Vega.VegaLite.Compat as FV
import qualified Frames as F
import qualified Frames.Melt as F
import qualified Frames.InCore as FI
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V


import qualified Control.MapReduce             as MR
import qualified Frames.Transform              as FT
import qualified Frames.Folds                  as FF
import qualified Frames.MapReduce              as FMR
import qualified Frames.SimpleJoins            as FJ

import qualified Frames.Visualization.VegaLite.Data
                                               as FV

import qualified Graphics.Vega.VegaLite        as GV
import qualified Knit.Report                   as K
import qualified Polysemy.Error                as P (mapError)
import qualified Polysemy                      as P (raise)

import           Data.String.Here               ( i )

import           BlueRipple.Configuration 
import           BlueRipple.Utilities.KnitUtils 

import qualified Numeric.GLM.ProblemTypes      as GLM
import qualified Numeric.GLM.Bootstrap            as GLM
import qualified Numeric.GLM.MixedModel            as GLM

import qualified Data.Time.Calendar            as Time
import qualified Data.Time.Clock               as Time

import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Data.Loaders as BR
import qualified BlueRipple.Data.ACS_PUMS as PUMS
import qualified BlueRipple.Data.CPSVoterPUMS as CPS
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.ElectionTypes as ET

import qualified BlueRipple.Model.MRP as BR
import qualified BlueRipple.Model.Turnout_MRP as BR

import qualified BlueRipple.Data.UsefulDataJoins as BR
import qualified BlueRipple.Model.CCES_MRP_Analysis as BR
import qualified BlueRipple.Utilities.KnitUtils as BR
import qualified BlueRipple.Data.Keyed         as Keyed
import qualified BlueRipple.Data.CCES as CCES

{-
foldPrefAndTurnoutData :: FF.EndoFold (F.Record '[PUMS.Citizens, ET.ElectoralWeight, ET.DemVPV, BR.DemPref])
foldPrefAndTurnoutData =  FF.sequenceRecFold
                          $ FF.toFoldRecord (FL.premap (F.rgetField @PUMS.Citizens) FL.sum)
                          V.:& FF.toFoldRecord (BR.weightedSumRecF @PUMS.Citizens @ET.ElectoralWeight)
                          V.:& FF.toFoldRecord (BR.weightedSumRecF @PUMS.Citizens @ET.DemVPV)
                          V.:& FF.toFoldRecord (BR.weightedSumRecF @PUMS.Citizens @BR.DemPref)
                          V.:& V.RNil

votesToVoteShareF :: FL.Fold (F.Record [ET.Party, ET.Votes]) (F.Record '[ET.PrefType, BR.DemPref])
votesToVoteShareF =
  let
    party = F.rgetField @ET.Party
    votes = F.rgetField @ET.Votes
    demVotesF = FL.prefilter (\r -> party r == ET.Democratic) $ FL.premap votes FL.sum
    demRepVotesF = FL.prefilter (\r -> let p = party r in (p == ET.Democratic || p == ET.Republican)) $ FL.premap votes FL.sum
    demPref d dr = if dr > 0 then realToFrac d/realToFrac dr else 0
    demPrefF = demPref <$> demVotesF <*> demRepVotesF
  in fmap (\x -> FT.recordSingleton ET.VoteShare `V.rappend` FT.recordSingleton @BR.DemPref x) demPrefF
-}

predictorsASER = fmap GLM.Predictor (BR.allSimplePredictors @DT.CatColsASER)
predictorsASER5 = fmap GLM.Predictor (BR.allSimplePredictors @DT.CatColsASER5)
statesAfter y r = F.rgetField @BR.Year r > y && F.rgetField @BR.StateAbbreviation r /= "National"


pumsByStateASER :: (K.KnitEffects r, K.CacheEffectsD r)
  => K.Sem r (K.ActionWithCacheTime r (F.FrameRec ([BR.Year, BR.StateAbbreviation, BR.StateFIPS] V.++ DT.CatColsASER V.++ [PUMS.NonCitizens, DT.PopCountOf, PUMS.Citizens])))
pumsByStateASER = do  
  cachedPUMS_Demographics <- PUMS.pumsLoaderAdults
  BR.retrieveOrMakeFrame "model/MRP_ASER/PumsByState.bin" cachedPUMS_Demographics $ \pumsDemographics -> do
    let rollup = fmap (FT.mutate $ const $ FT.recordSingleton @DT.PopCountOf DT.PC_Citizen)                         
                 $ FL.fold (PUMS.pumsStateRollupF $ PUMS.pumsKeysToASER True . F.rcast) pumsDemographics
        addDefaultsOneF :: FL.Fold (F.Record (DT.CatColsASER V.++ [PUMS.NonCitizens, DT.PopCountOf, PUMS.Citizens]))
                           (F.FrameRec (DT.CatColsASER V.++ [PUMS.NonCitizens, DT.PopCountOf, PUMS.Citizens]))
        addDefaultsOneF = fmap F.toFrame $ Keyed.addDefaultRec @DT.CatColsASER (0 F.&: DT.PC_Citizen F.&: 0 F.&: V.RNil)
        addDefaultsF = FMR.concatFold
                       $ FMR.mapReduceFold
                       FMR.noUnpack
                       (FMR.assignKeysAndData @[BR.Year, BR.StateAbbreviation, BR.StateFIPS]) 
                       (FMR.makeRecsWithKey id $ FMR.ReduceFold $ const addDefaultsOneF)
    return $ FL.fold addDefaultsF $ F.filterFrame (\r -> F.rgetField @BR.StateFIPS r < 60) rollup

pumsByStateASER5 :: (K.KnitEffects r, K.CacheEffectsD r)
  => K.Sem r (K.ActionWithCacheTime r (F.FrameRec ([BR.Year, BR.StateAbbreviation, BR.StateFIPS] V.++ DT.CatColsASER5 V.++ [PUMS.NonCitizens, DT.PopCountOf, PUMS.Citizens])))
pumsByStateASER5 = do  
  cachedPUMS_Demographics <- PUMS.pumsLoaderAdults
  BR.retrieveOrMakeFrame "model/MRP_ASER5/PumsByState.bin" cachedPUMS_Demographics $ \pumsDemographics -> do
    let rollup = fmap (FT.mutate $ const $ FT.recordSingleton @DT.PopCountOf DT.PC_Citizen)                         
                 $ FL.fold (PUMS.pumsStateRollupF $ PUMS.pumsKeysToASER5 True . F.rcast) pumsDemographics
        addDefaultsOneF :: FL.Fold (F.Record (DT.CatColsASER5 V.++ [PUMS.NonCitizens, DT.PopCountOf, PUMS.Citizens]))
                           (F.FrameRec (DT.CatColsASER5 V.++ [PUMS.NonCitizens, DT.PopCountOf, PUMS.Citizens]))
        addDefaultsOneF = fmap F.toFrame $ Keyed.addDefaultRec @DT.CatColsASER5 (0 F.&: DT.PC_Citizen F.&: 0 F.&: V.RNil)
        addDefaultsF = FMR.concatFold
                       $ FMR.mapReduceFold
                       FMR.noUnpack
                       (FMR.assignKeysAndData @[BR.Year, BR.StateAbbreviation, BR.StateFIPS]) 
                       (FMR.makeRecsWithKey id $ FMR.ReduceFold $ const addDefaultsOneF)
    return $ FL.fold addDefaultsF $ F.filterFrame (\r -> F.rgetField @BR.StateFIPS r < 60) rollup


censusElectoralWeightsASER_MRP :: (K.KnitEffects r, K.CacheEffectsD r, K.Member GLM.RandomFu r)
                      => K.Sem r (K.ActionWithCacheTime r (F.FrameRec (BR.StateAbbreviation ': (DT.CatColsASER V.++ (BR.Year ': ET.EWCols)))))
censusElectoralWeightsASER_MRP = do
  cachedCPS_VoterPUMS_Data <- CPS.cpsVoterPUMSLoader
  BR.retrieveOrMakeFrame "model/MRP_ASER/Census_ElectoralWeights.bin" cachedCPS_VoterPUMS_Data $ const $                                
    BR.mrpTurnout @DT.CatColsASER
    GLM.MDVNone
    (Just "T_CensusASER")
    ET.EW_Census
    ET.EW_Citizen
    cachedCPS_VoterPUMS_Data
    (CPS.cpsCountVotersByStateF $ CPS.cpsKeysToASER True . F.rcast)
    predictorsASER
    BR.catPredMaps

censusElectoralWeightsASER5_MRP :: (K.KnitEffects r, K.CacheEffectsD r, K.Member GLM.RandomFu r)
                      => K.Sem r (K.ActionWithCacheTime r (F.FrameRec (BR.StateAbbreviation ': (DT.CatColsASER5 V.++ (BR.Year ': ET.EWCols)))))
censusElectoralWeightsASER5_MRP = do
  cachedCPS_VoterPUMS_Data <- CPS.cpsVoterPUMSLoader
  BR.retrieveOrMakeFrame "model/MRP_ASER5/Census_ElectoralWeights.bin" cachedCPS_VoterPUMS_Data $ const $                                
    BR.mrpTurnout @DT.CatColsASER5
    GLM.MDVNone
    (Just "T_CensusASER5")
    ET.EW_Census
    ET.EW_Citizen
    cachedCPS_VoterPUMS_Data
    (CPS.cpsCountVotersByStateF $ CPS.cpsKeysToASER5 True . F.rcast)
    predictorsASER5
    BR.catPredMaps


ccesElectoralWeightsASER_MRP :: (K.KnitEffects r, K.CacheEffectsD r, K.Member GLM.RandomFu r)
                      => K.Sem r (K.ActionWithCacheTime r (F.FrameRec (BR.StateAbbreviation ': (DT.CatColsASER V.++ (BR.Year ': ET.EWCols)))))
ccesElectoralWeightsASER_MRP = do
  cachedCCES_Data <- CCES.ccesDataLoader
  BR.retrieveOrMakeFrame "model/MRP_ASER/CCES_ElectoralWeights.bin" cachedCCES_Data $ const $
      BR.mrpTurnout @DT.CatColsASER
        GLM.MDVNone
        (Just "T_CCES_ASER")
        ET.EW_CCES
        ET.EW_Citizen
        cachedCCES_Data
        (BR.countVotersOfAllF @DT.CatColsASER)
        predictorsASER
        BR.catPredMaps

ccesElectoralWeightsASER5_MRP :: (K.KnitEffects r, K.CacheEffectsD r, K.Member GLM.RandomFu r)
                      => K.Sem r (K.ActionWithCacheTime r (F.FrameRec (BR.StateAbbreviation ': (DT.CatColsASER5 V.++ (BR.Year ': ET.EWCols)))))
ccesElectoralWeightsASER5_MRP = do
  cachedCCES_Data <- CCES.ccesDataLoader
  BR.retrieveOrMakeFrame "model/MRP_ASER5/CCES_ElectoralWeights.bin" cachedCCES_Data $ const $
      BR.mrpTurnout @DT.CatColsASER5
        GLM.MDVNone
        (Just "T_CCES_ASER5")
        ET.EW_CCES
        ET.EW_Citizen
        cachedCCES_Data
        (BR.countVotersOfAllF @DT.CatColsASER5)
        predictorsASER5
        BR.catPredMaps        

-- adjusted turnout
type AdjCols cc = [BR.Year, BR.StateAbbreviation, PUMS.NonCitizens, DT.PopCountOf, BR.StateFIPS] V.++ cc V.++ [DT.PopCountOf, PUMS.Citizens] V.++ ET.EWCols

adjCensusElectoralWeightsMRP_ASER :: (K.KnitEffects r, K.CacheEffectsD r, K.Member GLM.RandomFu r)
  => K.Sem r (K.ActionWithCacheTime r (F.FrameRec (AdjCols DT.CatColsASER)))
adjCensusElectoralWeightsMRP_ASER = do
    stateTurnout_C <- BR.stateTurnoutLoader
    pumsByState_C <- fmap (F.filterFrame (statesAfter 2007)) <$> pumsByStateASER
    censusEW_MRP_C <- censusElectoralWeightsASER_MRP    
    let cachedDeps = (,,) <$> stateTurnout_C <*> pumsByState_C <*> censusEW_MRP_C
    BR.retrieveOrMakeFrame "model/MRP_ASER/PUMS_Census_adjElectoralWeights.bin" cachedDeps $ \(stateTurnout, pumsByState, inferredCensusTurnout) -> 
      BR.demographicsWithAdjTurnoutByState
        @DT.CatColsASER
        @PUMS.Citizens
        @'[PUMS.NonCitizens, DT.PopCountOf, BR.StateFIPS]
        @'[BR.Year, BR.StateAbbreviation]
        stateTurnout
        (fmap F.rcast pumsByState)
        (fmap F.rcast inferredCensusTurnout)

adjCensusElectoralWeightsMRP_ASER5 :: (K.KnitEffects r, K.CacheEffectsD r, K.Member GLM.RandomFu r)
  => K.Sem r (K.ActionWithCacheTime r (F.FrameRec (AdjCols DT.CatColsASER5)))
adjCensusElectoralWeightsMRP_ASER5 = do
    stateTurnoutC <- BR.stateTurnoutLoader
    pumsByStateC <- fmap (F.filterFrame (statesAfter 2007)) <$> pumsByStateASER5
    censusEW_MRP_C <- censusElectoralWeightsASER5_MRP    
    let cachedDeps = (,,) <$> stateTurnoutC <*> pumsByStateC <*> censusEW_MRP_C
    BR.retrieveOrMakeFrame "model/MRP_ASER5/PUMS_Census_adjElectoralWeights.bin" cachedDeps $ \(stateTurnout, pumsByState, inferredCensusTurnout) -> 
      BR.demographicsWithAdjTurnoutByState
        @DT.CatColsASER5
        @PUMS.Citizens
        @'[PUMS.NonCitizens, DT.PopCountOf, BR.StateFIPS]
        @'[BR.Year, BR.StateAbbreviation]
        stateTurnout
        (fmap F.rcast pumsByState)
        (fmap F.rcast inferredCensusTurnout)

adjCCES_ElectoralWeightsMRP_ASER :: (K.KnitEffects r, K.CacheEffectsD r, K.Member GLM.RandomFu r)
  => K.Sem r (K.ActionWithCacheTime r (F.FrameRec (AdjCols DT.CatColsASER)))
adjCCES_ElectoralWeightsMRP_ASER =   do
    stateTurnoutC <- BR.stateTurnoutLoader
    pumsByStateC <- fmap (F.filterFrame (statesAfter 2007)) <$> pumsByStateASER
    ccesEW_MRP_C <- ccesElectoralWeightsASER_MRP
    let cachedDeps = (,,) <$> stateTurnoutC <*> pumsByStateC <*> ccesEW_MRP_C
    BR.retrieveOrMakeFrame "model/MRP_ASER/PUMS_CCES_adjElectoralWeights.bin" cachedDeps $ \(stateTurnout, pumsByState, prefs) -> 
      BR.demographicsWithAdjTurnoutByState
        @DT.CatColsASER
        @PUMS.Citizens
        @'[PUMS.NonCitizens, DT.PopCountOf, BR.StateFIPS]
        @'[BR.Year, BR.StateAbbreviation] stateTurnout (fmap F.rcast pumsByState) (fmap F.rcast prefs)


adjCCES_ElectoralWeightsMRP_ASER5 :: (K.KnitEffects r, K.CacheEffectsD r, K.Member GLM.RandomFu r)
  => K.Sem r (K.ActionWithCacheTime r (F.FrameRec (AdjCols DT.CatColsASER5)))
adjCCES_ElectoralWeightsMRP_ASER5 =   do
    stateTurnoutC <- BR.stateTurnoutLoader
    pumsByStateC <- fmap (F.filterFrame (statesAfter 2007)) <$> pumsByStateASER5
    ccesEW_MRP_C <- ccesElectoralWeightsASER5_MRP
    let cachedDeps = (,,) <$> stateTurnoutC <*> pumsByStateC <*> ccesEW_MRP_C
    BR.retrieveOrMakeFrame "model/MRP_ASER5/PUMS_CCES_adjElectoralWeights.bin" cachedDeps $ \(stateTurnout, pumsByState, prefs) -> 
      BR.demographicsWithAdjTurnoutByState
        @DT.CatColsASER5
        @PUMS.Citizens
        @'[PUMS.NonCitizens, DT.PopCountOf, BR.StateFIPS]
        @'[BR.Year, BR.StateAbbreviation] stateTurnout (fmap F.rcast pumsByState) (fmap F.rcast prefs)

      
-- preferences
ccesPreferencesASER_MRP ::  (K.KnitEffects r, K.CacheEffectsD r, K.Member GLM.RandomFu r)
  => K.Sem r (K.ActionWithCacheTime r (F.FrameRec (BR.StateAbbreviation ': (DT.CatColsASER V.++ [BR.Year, ET.Office, ET.DemVPV, ET.DemPref]))))
ccesPreferencesASER_MRP = do
  cachedCCES_Data <- CCES.ccesDataLoader
  BR.retrieveOrMakeFrame "model/MRP_ASER/CCES_Preferences.bin" cachedCCES_Data $ const $ 
      BR.mrpPrefs @DT.CatColsASER GLM.MDVNone (Just "ASER") cachedCCES_Data predictorsASER BR.catPredMaps

ccesPreferencesASER5_MRP ::  (K.KnitEffects r, K.CacheEffectsD r, K.Member GLM.RandomFu r)
                        => K.Sem r (K.ActionWithCacheTime r (F.FrameRec (BR.StateAbbreviation ': (DT.CatColsASER5 V.++ [BR.Year, ET.Office, ET.DemVPV, ET.DemPref]))))
ccesPreferencesASER5_MRP = do
  cachedCCES_Data <- CCES.ccesDataLoader
  BR.retrieveOrMakeFrame "model/MRP_ASER5/CCES_Preferences.bin" cachedCCES_Data $ const $ 
      BR.mrpPrefs @DT.CatColsASER5 GLM.MDVNone (Just "ASER5") cachedCCES_Data predictorsASER5 BR.catPredMaps      



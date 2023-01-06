{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE Rank2Types #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# OPTIONS_GHC -O0 #-} -- otherwise we get a simplifier ticks issue
module BlueRipple.Data.Loaders.Redistricting
  (
    module BlueRipple.Data.Loaders.Redistricting
  , module BlueRipple.Data.Loaders.RedistrictingTables
  )
  where

import BlueRipple.Data.Loaders.RedistrictingTables

import qualified BlueRipple.Utilities.KnitUtils as BR
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.ElectionTypes as ET

import qualified Data.Map as M
import qualified Data.Vinyl.TypeLevel as V
import qualified Frames as F
import qualified Frames.Streamly.LoadInCore as FS
import qualified Frames.Streamly.TH as FS
import qualified Knit.Report as K

-- so these are not re-declared and take on the correct types
import BlueRipple.Data.DataFrames (Population)
import BlueRipple.Data.ElectionTypes (DistrictName,VAP,DemShare,RepShare)

FS.tableTypes' redistrictingAnalysisRowGen -- declare types and build parser

type DRAnalysis = F.Record ([DT.StateAbbreviation, PlanName, ET.DistrictTypeC] V.++ F.RecordColumns DRAnalysisRaw)

fixRow :: RedistrictingPlanId -> DRAnalysisRaw -> Maybe DRAnalysis
fixRow pi' r = Just $ pi' F.<+> r

lookupAndLoadRedistrictingPlanAnalysis ::  (K.KnitEffects r, BR.CacheEffects r)
                              => Map RedistrictingPlanId RedistrictingPlanFiles
                              -> RedistrictingPlanId
                              -> K.Sem r (K.ActionWithCacheTime r (F.Frame DRAnalysis))
lookupAndLoadRedistrictingPlanAnalysis plans pi' = do
  let noPlanErr = "No plan found for info:" <> show (pi :: Double)
  pf <- K.knitMaybe noPlanErr $ M.lookup pi' plans
  loadRedistrictingPlanAnalysis pi' pf

loadRedistrictingPlanAnalysis ::  (K.KnitEffects r, BR.CacheEffects r)
                              => RedistrictingPlanId
                              -> RedistrictingPlanFiles
                              -> K.Sem r (K.ActionWithCacheTime r (F.Frame DRAnalysis))
loadRedistrictingPlanAnalysis pi' pf = do
  let RedistrictingPlanFiles _ aFP = pf
  let cacheKey = "data/redistricting/" <> F.rgetField @DT.StateAbbreviation pi'
                 <> "_" <> show (F.rgetField @ET.DistrictTypeC pi')
                 <> "_" <> F.rgetField @PlanName pi' <> ".bin"
  fileDep <- K.fileDependency $ toString aFP
  BR.retrieveOrMakeFrame cacheKey fileDep $ const $ K.liftKnit $ FS.loadInCore @FS.DefaultStream @IO dRAnalysisRawParser (toString aFP) (fixRow pi')

allPassedCongressional :: (K.KnitEffects r, BR.CacheEffects r)
                       => K.Sem r (K.ActionWithCacheTime r (F.Frame DRAnalysis))
allPassedCongressional = do
  plans <- allPassedCongressionalPlans
  deps <- sequenceA <$> (traverse (uncurry loadRedistrictingPlanAnalysis) $ M.toList plans)
  BR.retrieveOrMakeFrame "data/redistricting/allPassedCongressional.bin" deps $ pure . mconcat


allPassedSLD :: (K.KnitEffects r, BR.CacheEffects r)
                       => K.Sem r (K.ActionWithCacheTime r (F.Frame DRAnalysis))
allPassedSLD = do
  plans <- allPassedSLDPlans
  deps <- sequenceA <$> (traverse (uncurry loadRedistrictingPlanAnalysis) $ M.toList plans)
  BR.retrieveOrMakeFrame "data/redistricting/allPassedSLD.bin" deps $ pure . mconcat

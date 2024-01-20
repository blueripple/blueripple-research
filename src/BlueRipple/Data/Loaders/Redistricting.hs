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
--import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.ElectionTypes as ET
import qualified BlueRipple.Data.GeographicTypes as GT
import qualified BlueRipple.Data.CensusTables as ACS

import qualified Data.Map as M
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V
import qualified Frames as F
import qualified Frames.Streamly.LoadInCore as FS
import qualified Frames.Streamly.TH as FS
import qualified Knit.Report as K

import Control.Lens (view, (^.))

-- so these are not re-declared and take on the correct types
import BlueRipple.Data.DataFrames (Population)
import BlueRipple.Data.ElectionTypes (VAP, DemPct, RepPct, DemShare, RepShare, RawPVI, SharePVI)
import BlueRipple.Data.GeographicTypes (DistrictName)

FS.tableTypes' redistrictingAnalysisRowGen -- declare types and build parser

type DRAnalysisR = ([GT.StateAbbreviation, PlanName, GT.DistrictTypeC] V.++ F.RecordColumns DRAnalysisRaw V.++ [DemShare, RepShare, RawPVI, SharePVI])
type DRAnalysis = F.Record DRAnalysisR

fixRow :: RedistrictingPlanId -> DRAnalysisRaw -> Maybe DRAnalysis
fixRow pi' r = Just $ pi' F.<+> r F.<+> sharesAndPVI r

sharesAndPVI :: DRAnalysisRaw -> F.Record [DemShare, RepShare, RawPVI, SharePVI]
sharesAndPVI r =
  let dPct = r ^. ET.demPct
      rPct = r ^. ET.repPct
      twoPartyTotal = dPct + rPct
      dShare = dPct  / twoPartyTotal
      rShare = rPct / twoPartyTotal
      rawPVI = dPct - rPct
      sharePVI = rawPVI  / twoPartyTotal
  in dShare F.&: rShare F.&: rawPVI F.&: sharePVI F.&: V.RNil

lookupAndLoadRedistrictingPlanAnalysis ::  (K.KnitEffects r, BR.CacheEffects r)
                              => Map RedistrictingPlanId RedistrictingPlanFiles
                              -> RedistrictingPlanId
                              -> K.Sem r (K.ActionWithCacheTime r (F.Frame DRAnalysis))
lookupAndLoadRedistrictingPlanAnalysis plans pi' = do
  let noPlanErr = "No plan found for info:" <> show pi'
  pf <- K.knitMaybe noPlanErr $ M.lookup pi' plans
  loadRedistrictingPlanAnalysis pi' pf

loadRedistrictingPlanAnalysis ::  (K.KnitEffects r, BR.CacheEffects r)
                              => RedistrictingPlanId
                              -> RedistrictingPlanFiles
                              -> K.Sem r (K.ActionWithCacheTime r (F.Frame DRAnalysis))
loadRedistrictingPlanAnalysis pi' pf = do
  let RedistrictingPlanFiles _ aFP = pf
  let cacheKey = "data/redistricting/" <> F.rgetField @GT.StateAbbreviation pi'
                 <> "_" <> show (F.rgetField @GT.DistrictTypeC pi')
                 <> "_" <> F.rgetField @PlanName pi' <> ".bin"
  fileDep <- K.fileDependency $ toString aFP
  BR.retrieveOrMakeFrame cacheKey fileDep $ const $ do
    K.logLE K.Info $ "(re)loading map analysis for " <> planIdText pi'
    K.liftKnit $ FS.loadInCore @FS.DefaultStream @IO dRAnalysisRawParser (toString aFP) (fixRow pi')

allPassedCongressional :: (K.KnitEffects r, BR.CacheEffects r)
                       => Int -> ACS.TableYear -> K.Sem r (K.ActionWithCacheTime r (F.Frame DRAnalysis))
allPassedCongressional mapYear acsTableYear = do
  plans <- allPassedCongressionalPlans mapYear acsTableYear
  deps <- sequenceA <$> (traverse (uncurry loadRedistrictingPlanAnalysis) $ M.toList plans)
  BR.retrieveOrMakeFrame "data/redistricting/allPassedCongressional.bin" deps $ pure . mconcat


allPassedSLD :: (K.KnitEffects r, BR.CacheEffects r)
             => Int -> ACS.TableYear -> K.Sem r (K.ActionWithCacheTime r (F.Frame DRAnalysis))
allPassedSLD mapYear acsTableYear = do
  plans <- allPassedSLDPlans mapYear acsTableYear
  deps <- sequenceA <$> (traverse (uncurry loadRedistrictingPlanAnalysis) $ M.toList plans)
  BR.retrieveOrMakeFrame "data/redistricting/allPassedSLD.bin" deps $ pure . mconcat

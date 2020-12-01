{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE PolyKinds           #-}
{-# LANGUAGE QuasiQuotes         #-}
{-# LANGUAGE TemplateHaskell     #-}
{-# LANGUAGE TypeFamilies        #-}
{-# LANGUAGE TypeOperators       #-}
{-# LANGUAGE TypeApplications    #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE Rank2Types          #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections       #-}
{-# OPTIONS_GHC  -O0             #-}
{-# OPTIONS_GHC -fplugin=Polysemy.Plugin #-}

module BlueRipple.Data.Loaders where

import BlueRipple.Data.LoadersCore
import qualified BlueRipple.Data.DataFrames    as BR
import qualified BlueRipple.Data.DemographicTypes
                                               as DT
import qualified BlueRipple.Data.ElectionTypes as ET
import qualified BlueRipple.Utilities.KnitUtils as BR

import qualified Knit.Report                   as K

import           Control.Lens                   ((%~))
import qualified Control.Foldl                 as FL
import           Data.Serialize.Text            ( )
import qualified Data.Text                     as T

import qualified Data.Vinyl                    as V
import qualified Data.Vinyl.TypeLevel          as V
import qualified Frames                        as F

import qualified Frames.MaybeUtils             as FM
import qualified Frames.Transform              as FT
import qualified Frames.Serialize              as FS


electoralCollegeFrame :: (K.KnitEffects r, K.CacheEffectsD r)
  => K.Sem r (K.ActionWithCacheTime r (F.Frame BR.ElectoralCollege))
electoralCollegeFrame = cachedFrameLoader (DataSets $ T.pack BR.electorsCSV) Nothing Nothing id Nothing "electoralCollege.bin"


parsePEParty :: T.Text -> ET.PartyT
parsePEParty t
  | T.isInfixOf "democrat" t = ET.Democratic
  | T.isInfixOf "republican" t = ET.Republican
  | otherwise = ET.Other

type PEFromCols = [BR.Year, BR.State, BR.StatePo, BR.StateFips, BR.Candidatevotes, BR.Totalvotes, BR.Party]
type PresidentialElectionCols = [BR.Year, BR.State, BR.StateAbbreviation, BR.StateFIPS, ET.Office, ET.Party, ET.Votes, ET.TotalVotes]

fixPresidentialElectionRow
  :: F.Record PEFromCols -> F.Record PresidentialElectionCols
fixPresidentialElectionRow = F.rcast . addCols where
  addCols = (FT.addName @BR.StatePo @BR.StateAbbreviation)
            . (FT.addName @BR.StateFips @BR.StateFIPS)
            . (FT.addName @BR.Candidatevotes @ET.Votes)
            . (FT.addName @BR.Totalvotes @ET.TotalVotes)
            . (FT.addOneFromValue @ET.Office ET.President)
            . (FT.addOneFromOne @BR.Party @ET.Party parsePEParty)



presidentialByStateFrame
  :: (K.KnitEffects r, K.CacheEffectsD r) => K.Sem r (K.ActionWithCacheTime r (F.FrameRec PresidentialElectionCols))
presidentialByStateFrame = cachedMaybeFrameLoader @(F.RecordColumns BR.PresidentialByState) @PEFromCols @PresidentialElectionCols
  (DataSets $ T.pack BR.presidentialByStateCSV)
  Nothing
  Nothing
  id
  fixPresidentialElectionRow
  Nothing
  "presByState.sbin"


cdFromPUMA2012Loader :: (K.KnitEffects r, K.CacheEffectsD r)
                      => Int -> K.Sem r (K.ActionWithCacheTime r (F.Frame BR.CDFromPUMA2012))
cdFromPUMA2012Loader congress = do 
  (csvPath, cacheKey) <- case congress of
    113 -> return $ (BR.cd113FromPUMA2012CSV, "data/cd113FromPUMA2012.bin")
    114 -> return $ (BR.cd114FromPUMA2012CSV, "data/cd114FromPUMA2012.bin")
    115 -> return $ (BR.cd115FromPUMA2012CSV, "data/cd115FromPUMA2012.bin")
    116 -> return $ (BR.cd116FromPUMA2012CSV, "data/cd116FromPUMA2012.bin")
    _ -> K.knitError "PUMA for congressional district crosswalk only available for 113th, 114th, 115th and 116th congress"
  cachedFrameLoader (DataSets $ T.pack csvPath) Nothing Nothing id Nothing cacheKey where --"cd116FromPUMA2012.bin"

type DatedCDFromPUMA2012 = '[BR.Year] V.++ (F.RecordColumns BR.CDFromPUMA2012)

allCDFromPUMA2012Loader :: (K.KnitEffects r, K.CacheEffectsD r)
                        => K.Sem r (K.ActionWithCacheTime r (F.FrameRec DatedCDFromPUMA2012))
allCDFromPUMA2012Loader = do
  let addYear :: Int -> BR.CDFromPUMA2012 -> F.Record DatedCDFromPUMA2012
      addYear y r = (y F.&: V.RNil) `V.rappend` r
      loadWithYear :: (K.KnitEffects r, K.CacheEffectsD r) => (Int, Int) -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec DatedCDFromPUMA2012))
      loadWithYear (year, congress) = fmap (fmap (addYear year)) <$> cdFromPUMA2012Loader congress
  withYears_C <- sequenceA <$> traverse loadWithYear [(2012, 113), (2014, 114), (2016, 115), (2018, 116)]
  BR.retrieveOrMakeFrame "data/cdFromPUMA2012.bin" withYears_C $ \withYears -> return $ mconcat withYears      


{-
puma2000ToCD116Loader :: (K.KnitEffects r, K.CacheEffectsD r)
                      => K.Sem r (K.ActionWithCacheTime r (F.Frame BR.PUMA2000ToCD116))
puma2000ToCD116Loader = cachedFrameLoader (DataSets $ T.pack BR.puma2000ToCD116CSV) Nothing Nothing id Nothing "puma2000ToCD116.sbin"
-}
county2010ToCD116Loader :: (K.KnitEffects r, K.CacheEffectsD r)
                        => K.Sem r (K.ActionWithCacheTime r (F.Frame BR.CountyToCD116))
county2010ToCD116Loader = cachedFrameLoader (DataSets $ T.pack BR.countyToCD116CSV) Nothing Nothing id Nothing "county2010ToCD116.sbin"


countyToPUMALoader :: (K.KnitEffects r, K.CacheEffectsD r)
                        => K.Sem r (K.ActionWithCacheTime r (F.Frame BR.CountyFromPUMA))
countyToPUMALoader = cachedFrameLoader (DataSets $ T.pack BR.county2014FromPUMA2012CSV) Nothing Nothing id Nothing "countyFromPUMA.bin"

aseDemographicsLoader :: (K.KnitEffects r, K.CacheEffectsD r) => K.Sem r (K.ActionWithCacheTime r (F.Frame BR.ASEDemographics))
aseDemographicsLoader =
  cachedFrameLoader
  (DataSets $ T.pack BR.ageSexEducationDemographicsLongCSV)
  Nothing
  Nothing
  id
  Nothing
  "aseDemographics.sbin"

simpleASEDemographicsLoader :: (K.KnitEffects r, K.CacheEffectsD r)
                            => K.Sem r (K.ActionWithCacheTime r (F.FrameRec (DT.ACSKeys V.++ '[DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, BR.ACSCount])))
simpleASEDemographicsLoader = do
  cachedASE_Demographics <- aseDemographicsLoader  -- get cache time and action to decode data if required
  let make aseACSRaw = K.knitEither $ FL.foldM DT.simplifyACS_ASEFold aseACSRaw
  K.retrieveOrMakeTransformed (fmap FS.toS . FL.fold FL.list) (F.toFrame . fmap FS.fromS) ("data/acs_simpleASE.bin" :: T.Text) cachedASE_Demographics make
  
asrDemographicsLoader :: (K.KnitEffects r, K.CacheEffectsD r)
                      => K.Sem r (K.ActionWithCacheTime r (F.Frame BR.ASRDemographics))
asrDemographicsLoader =
  cachedFrameLoader
  (DataSets $ T.pack BR.ageSexRaceDemographicsLongCSV)
  Nothing
  Nothing
  id
  Nothing
  "asrDemographics.sbin"

simpleASRDemographicsLoader :: (K.KnitEffects r, K.CacheEffectsD r)
  => K.Sem r (K.ActionWithCacheTime r (F.FrameRec (DT.ACSKeys V.++ '[DT.SimpleAgeC, DT.SexC, DT.SimpleRaceC, BR.ACSCount])))
simpleASRDemographicsLoader = do
  cachedASR_Demographics <- asrDemographicsLoader  -- get cache time and action to decode data if required
  let make asrDemographics = K.knitEither $ FL.foldM DT.simplifyACS_ASRFold asrDemographics
  K.retrieveOrMakeTransformed @K.DefaultSerializer @K.DefaultCacheData
    (fmap FS.toS . FL.fold FL.list)
    (F.toFrame . fmap FS.fromS)
    ("data/acs_simpleASR.bin" :: T.Text)
    cachedASR_Demographics
    make

aseTurnoutLoader :: (K.KnitEffects r, K.CacheEffectsD r) => K.Sem r (K.ActionWithCacheTime r (F.Frame BR.TurnoutASE))
aseTurnoutLoader =
  cachedFrameLoader
  (DataSets $ T.pack BR.detailedASETurnoutCSV)
  Nothing
  Nothing
  id
  Nothing
  "aseTurnout.sbin"

simpleASETurnoutLoader :: (K.KnitEffects r, K.CacheEffectsD r)
                       => K.Sem r (K.ActionWithCacheTime r (F.FrameRec [BR.Year, DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, BR.Population, BR.Citizen, BR.Registered, BR.Voted]))
simpleASETurnoutLoader = do
  cachedASE_Turnout <- aseTurnoutLoader  -- get cache time and action to decode data if required
  let make aseTurnoutRaw = K.knitEither $ FL.foldM DT.simplifyTurnoutASEFold aseTurnoutRaw
  K.retrieveOrMakeTransformed @K.DefaultSerializer @K.DefaultCacheData
    (fmap FS.toS . FL.fold FL.list)
    (F.toFrame . fmap FS.fromS)
    ("data/turnout_simpleASE.bin" :: T.Text)
    cachedASE_Turnout make

asrTurnoutLoader :: (K.KnitEffects r, K.CacheEffectsD r) => K.Sem r (K.ActionWithCacheTime r (F.Frame BR.TurnoutASR))
asrTurnoutLoader =
  cachedFrameLoader
  (DataSets $ T.pack BR.detailedASRTurnoutCSV)
  Nothing
  Nothing
  id
  Nothing
  "asrTurnout.sbin"

simpleASRTurnoutLoader :: (K.KnitEffects r, K.CacheEffectsD r)
  => K.Sem r (K.ActionWithCacheTime r (F.FrameRec [BR.Year, DT.SimpleAgeC, DT.SexC, DT.SimpleRaceC, BR.Population, BR.Citizen, BR.Registered, BR.Voted]))
simpleASRTurnoutLoader = do
  cachedASR_Turnout <- asrTurnoutLoader
  let make asrTurnoutRaw = K.knitEither $ FL.foldM DT.simplifyTurnoutASRFold asrTurnoutRaw
  K.retrieveOrMakeTransformed @K.DefaultSerializer @K.DefaultCacheData
    (fmap FS.toS . FL.fold FL.list)
    (F.toFrame . fmap FS.fromS)
    ("data/turnout_simpleASR.bin" :: T.Text)
    cachedASR_Turnout make


stateAbbrCrosswalkLoader ::  (K.KnitEffects r, K.CacheEffectsD r)
                         => K.Sem r (K.ActionWithCacheTime r (F.Frame BR.States))
stateAbbrCrosswalkLoader = cachedFrameLoader (DataSets $ T.pack BR.statesCSV) Nothing Nothing id Nothing "stateAbbr.sbin"

type StateTurnoutCols = F.RecordColumns BR.StateTurnout

stateTurnoutLoader :: (K.KnitEffects r, K.CacheEffectsD r)
                   => K.Sem r (K.ActionWithCacheTime r (F.Frame BR.StateTurnout))
stateTurnoutLoader = cachedMaybeFrameLoader @StateTurnoutCols @StateTurnoutCols
                     (DataSets $ T.pack BR.stateTurnoutCSV)
                     Nothing
                     Nothing 
                     fixMaybes   
                     id
                     Nothing
                     "stateTurnout.sbin"
  where
    missingOETo0 :: F.Rec (Maybe F.:. F.ElField) '[BR.OverseasEligible] -> F.Rec (Maybe F.:. F.ElField) '[BR.OverseasEligible]
    missingOETo0 = FM.fromMaybeMono 0
    missingBCVEPTo0 :: F.Rec (Maybe F.:. F.ElField) '[BR.BallotsCountedVEP] -> F.Rec (Maybe F.:. F.ElField) '[BR.BallotsCountedVEP]
    missingBCVEPTo0 = FM.fromMaybeMono 0                
    missingBCTo0 :: F.Rec (Maybe F.:. F.ElField) '[BR.BallotsCounted] -> F.Rec (Maybe F.:. F.ElField) '[BR.BallotsCounted]
    missingBCTo0 = FM.fromMaybeMono 0
    fixMaybes = (F.rsubset %~ missingOETo0) . (F.rsubset %~ missingBCVEPTo0) . (F.rsubset %~ missingBCTo0)

type HouseElectionCols = [BR.Year
                         , BR.State
                         , BR.StateAbbreviation
                         , BR.StateFIPS
                         , ET.Office
                         , BR.CongressionalDistrict
                         , BR.Stage
                         , BR.Runoff
                         , BR.Special
                         , ET.Party
                         , ET.Votes
                         , ET.TotalVotes]

processHouseElectionRow :: BR.HouseElections -> F.Record HouseElectionCols
processHouseElectionRow r = F.rcast @HouseElectionCols (mutate r)
 where
  mutate =
    FT.retypeColumn @BR.StatePo @BR.StateAbbreviation
    . FT.retypeColumn @BR.StateFips @BR.StateFIPS
    . FT.mutate (const $ FT.recordSingleton @ET.Office ET.House)
    . FT.retypeColumn @BR.District @BR.CongressionalDistrict
    . FT.retypeColumn @BR.Candidatevotes @ET.Votes
    . FT.retypeColumn @BR.Totalvotes @ET.TotalVotes
    . FT.mutate
          (FT.recordSingleton @ET.Party . parsePEParty . F.rgetField @BR.Party)

houseElectionsLoader :: (K.KnitEffects r, K.CacheEffectsD r)
                     => K.Sem r (K.ActionWithCacheTime r (F.FrameRec HouseElectionCols))
houseElectionsLoader = cachedFrameLoader (DataSets $ T.pack BR.houseElectionsCSV) Nothing Nothing processHouseElectionRow Nothing "houseElections.sbin"

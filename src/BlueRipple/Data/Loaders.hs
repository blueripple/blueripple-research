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
module BlueRipple.Data.Loaders where

import qualified BlueRipple.Data.DataFrames    as BR
import qualified BlueRipple.Data.DemographicTypes
                                               as DT
import qualified BlueRipple.Data.ElectionTypes as ET

import qualified Knit.Report                   as K
import qualified Knit.Report.Cache             as K
import qualified Polysemy                      as P
import           Control.Monad.IO.Class         ( MonadIO(liftIO) )
import           Control.Lens                   ((%~))

import qualified Control.Foldl                 as FL
import qualified Data.List                     as L
import           Data.Maybe                     ( catMaybes
                                                , fromMaybe
                                                )
import qualified Data.Serialize                as S
import           Data.Serialize.Text            ( )
import           GHC.Generics                   ( Generic
                                                , Rep
                                                )
import qualified Data.Text                     as T

import qualified Data.Vinyl                    as V
import qualified Data.Vinyl.TypeLevel          as V
import qualified Frames                        as F
import qualified Frames.CSV                    as F
import qualified Frames.InCore                 as FI
import qualified Frames.TH                     as F

import qualified Frames.ParseableTypes         as FP
import qualified Frames.MaybeUtils             as FM
import qualified Frames.Transform              as FT
import qualified Frames.Serialize              as FS




electoralCollegeFrame :: K.KnitEffects r => K.Sem r (F.Frame BR.ElectoralCollege)
electoralCollegeFrame = cachedFrameLoader (DataSets $ T.pack BR.electorsCSV) Nothing id Nothing "electoralCollege.bin"


parsePEParty :: T.Text -> ET.PartyT
parsePEParty "democrat"   = ET.Democratic
parsePEParty "republican" = ET.Republican
parsePEParty _            = ET.Other

type PEFromCols = [BR.Year, BR.State, BR.StatePo, BR.StateFips, BR.Candidatevotes, BR.Totalvotes, BR.Party]
type PresidentialElectionCols = [BR.Year, BR.State, BR.StateAbbreviation, BR.StateFIPS, ET.Office, ET.Party, ET.Votes, ET.TotalVotes]

fixPresidentialElectionRow
  :: F.Record PEFromCols -> F.Record PresidentialElectionCols
fixPresidentialElectionRow r = F.rcast @PresidentialElectionCols (mutate r)
 where
  mutate =
    FT.retypeColumn @BR.StatePo @BR.StateAbbreviation
      . FT.retypeColumn @BR.StateFips @BR.StateFIPS
      . FT.retypeColumn @BR.Candidatevotes @ET.Votes
      . FT.retypeColumn @BR.Totalvotes @ET.TotalVotes
      . FT.mutate (const $ FT.recordSingleton @ET.Office ET.President)
      . FT.mutate
          (FT.recordSingleton @ET.Party . parsePEParty . F.rgetField @BR.Party)

--fixPEMaybes :: F.Rec (Maybe F.:. F.ElField) PresidentialElectionCols -> F.Rec (Maybe F.:. F.ElField) PresidentialElectionCols
--fixPEMaybes r = 

presidentialByStateFrame
  :: K.KnitEffects r => K.Sem r (F.FrameRec PresidentialElectionCols)
presidentialByStateFrame = cachedMaybeFrameLoader @PEFromCols @(F.RecordColumns BR.PresidentialByState)
  (DataSets $ T.pack BR.presidentialByStateCSV)
  Nothing
  (const True)
  id
  fixPresidentialElectionRow
  Nothing
  "presByState.bin"

aseDemographicsLoader :: K.KnitEffects r => K.Sem r (F.Frame BR.ASEDemographics)
aseDemographicsLoader =
  cachedFrameLoader
  (DataSets $ T.pack BR.ageSexEducationDemographicsLongCSV)
  Nothing
  id
  Nothing
  "aseDemographics.bin"

simpleASEDemographicsLoader :: K.KnitEffects r => K.Sem r (F.FrameRec (DT.ACSKeys V.++ '[DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, BR.ACSCount]))
simpleASEDemographicsLoader =
  let make = do
         aseACSRaw <- aseDemographicsLoader
         K.knitEither $ FL.foldM DT.simplifyACS_ASEFold aseACSRaw
  in  K.retrieveOrMakeTransformed (fmap FS.toS . FL.fold FL.list) (F.toFrame . fmap FS.fromS) "data/acs_simpleASE.bin" make
  
asrDemographicsLoader :: K.KnitEffects r => K.Sem r (F.Frame BR.ASRDemographics)
asrDemographicsLoader =
  cachedFrameLoader
  (DataSets $ T.pack BR.ageSexRaceDemographicsLongCSV)
  Nothing
  id
  Nothing
  "asrDemographics.bin"

simpleASRDemographicsLoader :: K.KnitEffects r => K.Sem r (F.FrameRec (DT.ACSKeys V.++ '[DT.SimpleAgeC, DT.SexC, DT.SimpleRaceC, BR.ACSCount]))
simpleASRDemographicsLoader =
  let make = do
         asrACSRaw <- asrDemographicsLoader
         K.knitEither $ FL.foldM DT.simplifyACS_ASRFold asrACSRaw
  in  K.retrieveOrMakeTransformed (fmap FS.toS . FL.fold FL.list) (F.toFrame . fmap FS.fromS) "data/acs_simpleASR.bin" make

aseTurnoutLoader :: K.KnitEffects r => K.Sem r (F.Frame BR.TurnoutASE)
aseTurnoutLoader =
  cachedFrameLoader
  (DataSets $ T.pack BR.detailedASETurnoutCSV)
  Nothing
  id
  Nothing
  "aseTurnout.bin"

simpleASETurnoutLoader :: K.KnitEffects r => K.Sem r (F.FrameRec [BR.Year, DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, BR.Population, BR.Citizen, BR.Registered, BR.Voted])
simpleASETurnoutLoader =
  let make = do
         aseTurnoutRaw <- aseTurnoutLoader
         K.knitEither $ FL.foldM DT.simplifyTurnoutASEFold aseTurnoutRaw
  in  K.retrieveOrMakeTransformed (fmap FS.toS . FL.fold FL.list) (F.toFrame . fmap FS.fromS) "data/turnout_simpleASE.bin" make

asrTurnoutLoader :: K.KnitEffects r => K.Sem r (F.Frame BR.TurnoutASR)
asrTurnoutLoader =
  cachedFrameLoader
  (DataSets $ T.pack BR.detailedASRTurnoutCSV)
  Nothing
  id
  Nothing
  "asrTurnout.bin"

simpleASRTurnoutLoader :: K.KnitEffects r => K.Sem r (F.FrameRec [BR.Year, DT.SimpleAgeC, DT.SexC, DT.SimpleRaceC, BR.Population, BR.Citizen, BR.Registered, BR.Voted])
simpleASRTurnoutLoader =
  let make = do
         asrTurnoutRaw <- asrTurnoutLoader
         K.knitEither $ FL.foldM DT.simplifyTurnoutASRFold asrTurnoutRaw
  in  K.retrieveOrMakeTransformed (fmap FS.toS . FL.fold FL.list) (F.toFrame . fmap FS.fromS) "data/turnout_simpleASR.bin" make


stateAbbrCrosswalkLoader ::  K.KnitEffects r => K.Sem r (F.Frame BR.States)
stateAbbrCrosswalkLoader = cachedFrameLoader (DataSets $ T.pack BR.statesCSV) Nothing id Nothing "stateAbbr.bin"

type StateTurnoutCols = F.RecordColumns BR.StateTurnout

stateTurnoutLoader :: K.KnitEffects r => K.Sem r (F.Frame BR.StateTurnout)
stateTurnoutLoader = cachedMaybeFrameLoader @StateTurnoutCols @StateTurnoutCols
                     (DataSets $ T.pack BR.stateTurnoutCSV)
                     Nothing
                     (const True)
                     fixMaybes
                     id
                     Nothing
                     "stateTurnout.bin"
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

houseElectionsLoader :: K.KnitEffects r => K.Sem r (F.FrameRec HouseElectionCols)
houseElectionsLoader = cachedFrameLoader (DataSets $ T.pack BR.houseElectionsCSV) Nothing processHouseElectionRow Nothing "houseElections.bin"

data DataPath = DataSets T.Text | LocalData T.Text

getPath :: DataPath -> IO FilePath
getPath dataPath = case dataPath of
  DataSets fp -> liftIO $ BR.usePath (T.unpack fp)
  LocalData fp -> return (T.unpack fp)

cachedFrameLoader
  :: forall qs rs r
   . ( V.RMap rs
     , V.RMap qs
     , F.ReadRec qs
     , FI.RecVec qs
     , FI.RecVec rs
     , S.GSerializePut (Rep (F.Rec FS.SElField rs))
     , S.GSerializeGet (Rep (F.Rec FS.SElField rs))
     , Generic (F.Rec FS.SElField rs)
     , K.KnitEffects r
     )
  => DataPath
  -> Maybe F.ParserOptions
  -> (F.Record qs -> F.Record rs)
  -> Maybe T.Text -- ^ optional cache-path. Defaults to "data/"
  -> T.Text -- ^ cache key
  -> K.Sem r (F.FrameRec rs)
cachedFrameLoader filePath parserOptionsM fixRow cachePathM key = do
  let cacheFrame = K.retrieveOrMakeTransformed
        (fmap FS.toS . FL.fold FL.list)
        (F.toFrame . fmap FS.fromS)
      cacheKey      = (fromMaybe "data/" cachePathM) <> key
  K.logLE K.Diagnostic
    $  "loading or retrieving and saving data at key="
    <> cacheKey
  cacheFrame cacheKey $ frameLoader filePath parserOptionsM fixRow

frameLoader
  :: forall qs rs r
  . (K.KnitEffects r
    , F.ReadRec qs
    , FI.RecVec qs
    , V.RMap qs
    )
  => DataPath
  -> Maybe F.ParserOptions
  -> (F.Record qs -> F.Record rs)
  -> K.Sem r (F.FrameRec rs)
frameLoader filePath parserOptionsM fixRow = do
  let csvParserOptions =
        F.defaultParser { F.quotingMode = F.RFC4180Quoting ' ' }
      parserOptions = (fromMaybe csvParserOptions parserOptionsM)
  path <- liftIO $ getPath filePath
  K.logLE K.Diagnostic ("Attempting to loading data from " <> (T.pack path) <> " into a frame.")
  fmap fixRow <$> BR.loadToFrame parserOptions path (const True)

cachedMaybeFrameLoader
  :: forall qs ls rs r
   .   -- ^ we load with ls, rcast to qs before fixing maybes then transform qs to rs before caching and returning 
     ( V.RMap rs
     , V.RMap qs
     , V.RMap ls
     , V.RFoldMap qs
     , V.RPureConstrained V.KnownField qs
     , V.RecApplicative qs
     , V.RApply qs
     , F.ReadRec ls
     , F.ReadRec qs
     , FI.RecVec ls
     , FI.RecVec qs
     , FI.RecVec rs
     , qs F.⊆ qs
     , qs F.⊆ ls
     , S.GSerializePut (Rep (F.Rec FS.SElField rs))
     , S.GSerializeGet (Rep (F.Rec FS.SElField rs))
     , Generic (F.Rec FS.SElField rs)
     , K.KnitEffects r
--     , Show (F.Rec (Maybe F.:. F.ElField) qs)
     )
  => DataPath
  -> Maybe F.ParserOptions
  -> (F.Rec (Maybe F.:. F.ElField) qs -> Bool)
  -> (  F.Rec (Maybe F.:. F.ElField) qs
     -> (F.Rec (Maybe F.:. F.ElField) qs)
     )
  -> (F.Record qs -> F.Record rs)
  -> Maybe T.Text -- ^ optional cache-path. Defaults to "data/"
  -> T.Text -- ^ cache key
  -> K.Sem r (F.FrameRec rs)
cachedMaybeFrameLoader filePath parserOptionsM filterMaybes fixMaybes fixRow cachePathM key
  = do
    let cacheFrame = K.retrieveOrMakeTransformed
          (fmap FS.toS . FL.fold FL.list)
          (F.toFrame . fmap FS.fromS)
        cacheKey      = (fromMaybe "data/" cachePathM) <> key
    K.logLE K.Diagnostic
      $  "loading or retrieving and saving data at key="
      <> cacheKey
    cacheFrame cacheKey $ maybeFrameLoader @qs @ls @rs filePath parserOptionsM filterMaybes fixMaybes fixRow

maybeFrameLoader 
   :: forall qs ls rs r
   . (K.KnitEffects r
     , V.RMap ls
     , F.ReadRec ls
     , qs F.⊆ ls
     , FI.RecVec ls
     , FI.RecVec qs
     , V.RFoldMap qs
     , V.RPureConstrained V.KnownField qs
     , V.RecApplicative qs
     , V.RApply qs
--     , Show (F.Rec (Maybe F.:. F.ElField) qs)
     )
   => DataPath
   -> Maybe F.ParserOptions
   -> (F.Rec (Maybe F.:. F.ElField) qs -> Bool)
   -> (  F.Rec (Maybe F.:. F.ElField) qs -> (F.Rec (Maybe F.:. F.ElField) qs))
   -> (F.Record qs -> F.Record rs)
   -> K.Sem r (F.FrameRec rs)
maybeFrameLoader filePath parserOptionsM filterMaybes fixMaybes fixRow = do
 let csvParserOptions =
       F.defaultParser { F.quotingMode = F.RFC4180Quoting ' ' }
     parserOptions = (fromMaybe csvParserOptions parserOptionsM)  
 path      <- liftIO $ getPath filePath
 K.logLE K.Diagnostic
       ("Attempting to load data from " <> (T.pack path) <> " into a Frame (Maybe .: ElField), filter and fix.")
 maybeRecs <- BR.loadToMaybeRecs @qs @ls @r parserOptions
              filterMaybes
              path
 fmap fixRow <$> BR.maybeRecsToFrame fixMaybes (const True) maybeRecs         
  
cachedRecListLoader
  :: forall qs rs r
   . ( V.RMap rs
     , V.RMap qs
     , F.ReadRec qs
     , FI.RecVec qs
     , FI.RecVec rs
     , V.RFoldMap qs
     , V.RPureConstrained V.KnownField qs
     , V.RecApplicative qs
     , V.RApply qs
     , qs F.⊆ qs
     , S.GSerializePut (Rep (F.Rec FS.SElField rs))
     , S.GSerializeGet (Rep (F.Rec FS.SElField rs))
     , Generic (F.Rec FS.SElField rs)
     , K.KnitEffects r
--     , Show (F.Rec (Maybe F.:. F.ElField) qs)
     )
  => T.Text -- ^ path to file
  -> Maybe F.ParserOptions
  -> (F.Record qs -> F.Record rs)
  -> Maybe
       ( F.Rec (Maybe F.:. F.ElField) qs -> Bool
       , (  F.Rec (Maybe F.:. F.ElField) qs
         -> (F.Rec (Maybe F.:. F.ElField) qs)
         )
       )
  -> Maybe T.Text -- ^ optional cache-path. Defaults to "data/"
  -> T.Text -- ^ cache key
  -> K.Sem r [F.Record rs]
cachedRecListLoader filePath parserOptionsM fixRow maybeFuncsM cachePathM key =
  do
    let csvParserOptions =
          F.defaultParser { F.quotingMode = F.RFC4180Quoting ' ' }
        cachedRecList =
          K.retrieveOrMakeTransformed (fmap FS.toS) (fmap FS.fromS)
        parserOptions = (fromMaybe csvParserOptions parserOptionsM)
        cacheKey      = fromMaybe "data/" cachePathM <> key
    cachedRecList cacheKey $ case maybeFuncsM of
      Nothing ->
        fmap fixRow
          .   FL.fold FL.list
          <$> do
                path <- liftIO $ BR.usePath (T.unpack filePath) -- translate data-sets path to local 
                BR.loadToFrame parserOptions path (const True)
      Just (filterMaybes, fixMaybes) ->
        fmap fixRow
          .   FL.fold FL.list
          <$> do
                path      <- liftIO $ BR.usePath (T.unpack filePath) -- translate data-sets path to local
                maybeRecs <- BR.loadToMaybeRecs @qs @qs parserOptions
                                                        filterMaybes
                                                        path
                BR.maybeRecsToFrame fixMaybes (const True) maybeRecs

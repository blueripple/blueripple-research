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
import qualified BlueRipple.Utilities.KnitUtils as BR

import qualified Knit.Report                   as K
import qualified Knit.Report.Cache             as K
import qualified Knit.Utilities.Streamly       as K

import qualified Polysemy                      as P
import qualified Polysemy.Error                      as P
import           Control.Monad.IO.Class         ( MonadIO(liftIO) )
import qualified Control.Lens                  as Lens      
import           Control.Lens                   ((%~))
import qualified Control.Monad.Catch.Pure      as Exceptions
import           Control.Monad (join)
import qualified Control.Foldl                 as FL
import qualified Data.List                     as L
import qualified Data.Map as M
import           Data.Maybe                     ( catMaybes
                                                , fromMaybe
                                                )
import qualified Data.Set                      as Set                 
import qualified Data.Serialize                as S
import           Data.Serialize.Text            ( )
import           GHC.Generics                   ( Generic
                                                , Rep
                                                )
import qualified Data.Text                     as T
import qualified Data.Text.Read                as T

import qualified Data.Vinyl                    as V
import qualified Data.Vinyl.TypeLevel          as V
import qualified Data.Vinyl.Curry              as V
import qualified Frames                        as F
import qualified Frames.CSV                    as F
import qualified Frames.InCore                 as FI
import qualified Frames.TH                     as F

import qualified Streamly as Streamly
import qualified Streamly.Prelude as Streamly
import qualified Streamly.Internal.Prelude as Streamly


import qualified Frames.ParseableTypes         as FP
import qualified Frames.MaybeUtils             as FM
import qualified Frames.Transform              as FT
import qualified Frames.Serialize              as FS
import qualified Frames.SimpleJoins            as FJ

import qualified System.Directory as System
import GHC.TypeLits (Symbol)
import Data.Kind (Type)

electoralCollegeFrame :: K.DefaultEffects r => K.Sem r (K.ActionWithCacheTime r (F.Frame BR.ElectoralCollege))
electoralCollegeFrame = cachedFrameLoader (DataSets $ T.pack BR.electorsCSV) Nothing Nothing id Nothing "electoralCollege.bin"


parsePEParty :: T.Text -> ET.PartyT
parsePEParty "democrat"   = ET.Democratic
parsePEParty "republican" = ET.Republican
parsePEParty _            = ET.Other

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
  :: K.DefaultEffects r => K.Sem r (K.ActionWithCacheTime r (F.FrameRec PresidentialElectionCols))
presidentialByStateFrame = cachedMaybeFrameLoader @(F.RecordColumns BR.PresidentialByState) @PEFromCols @PresidentialElectionCols
  (DataSets $ T.pack BR.presidentialByStateCSV)
  Nothing
  Nothing
  id
  fixPresidentialElectionRow
  Nothing
  "presByState.sbin"

puma2012ToCD116Loader :: K.DefaultEffects r => K.Sem r (K.ActionWithCacheTime r (F.Frame BR.PUMA2012ToCD116))
puma2012ToCD116Loader = cachedFrameLoader (DataSets $ T.pack BR.puma2012ToCD116CSV) Nothing Nothing id Nothing "puma2012ToCD116.sbin"

puma2000ToCD116Loader :: K.DefaultEffects r => K.Sem r (K.ActionWithCacheTime r (F.Frame BR.PUMA2000ToCD116))
puma2000ToCD116Loader = cachedFrameLoader (DataSets $ T.pack BR.puma2000ToCD116CSV) Nothing Nothing id Nothing "puma2000ToCD116.sbin"

county2010ToCD116Loader :: K.DefaultEffects r => K.Sem r (K.ActionWithCacheTime r (F.Frame BR.CountyToCD116))
county2010ToCD116Loader = cachedFrameLoader (DataSets $ T.pack BR.countyToCD116CSV) Nothing Nothing id Nothing "county2010ToCD116.sbin"


aseDemographicsLoader :: K.DefaultEffects r => K.Sem r (K.ActionWithCacheTime r (F.Frame BR.ASEDemographics))
aseDemographicsLoader =
  cachedFrameLoader
  (DataSets $ T.pack BR.ageSexEducationDemographicsLongCSV)
  Nothing
  Nothing
  id
  Nothing
  "aseDemographics.sbin"

simpleASEDemographicsLoader :: K.DefaultEffects r
                            => K.Sem r (K.ActionWithCacheTime r (F.FrameRec (DT.ACSKeys V.++ '[DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, BR.ACSCount])))
simpleASEDemographicsLoader = do
  cachedASE_Demographics <- aseDemographicsLoader  -- get cache time and action to decode data if required
  let make aseACSRaw = K.knitEither $ FL.foldM DT.simplifyACS_ASEFold aseACSRaw
  K.retrieveOrMakeTransformed @K.DefaultSerializer @K.DefaultCacheData (fmap FS.toS . FL.fold FL.list) (F.toFrame . fmap FS.fromS) ("data/acs_simpleASE.bin" :: T.Text) cachedASE_Demographics make
  
asrDemographicsLoader :: K.DefaultEffects r => K.Sem r (K.ActionWithCacheTime r (F.Frame BR.ASRDemographics))
asrDemographicsLoader =
  cachedFrameLoader
  (DataSets $ T.pack BR.ageSexRaceDemographicsLongCSV)
  Nothing
  Nothing
  id
  Nothing
  "asrDemographics.sbin"

simpleASRDemographicsLoader :: K.DefaultEffects r
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

aseTurnoutLoader :: K.DefaultEffects r => K.Sem r (K.ActionWithCacheTime r (F.Frame BR.TurnoutASE))
aseTurnoutLoader =
  cachedFrameLoader
  (DataSets $ T.pack BR.detailedASETurnoutCSV)
  Nothing
  Nothing
  id
  Nothing
  "aseTurnout.sbin"

simpleASETurnoutLoader :: K.DefaultEffects r
                       => K.Sem r (K.ActionWithCacheTime r (F.FrameRec [BR.Year, DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, BR.Population, BR.Citizen, BR.Registered, BR.Voted]))
simpleASETurnoutLoader = do
  cachedASE_Turnout <- aseTurnoutLoader  -- get cache time and action to decode data if required
  let make aseTurnoutRaw = K.knitEither $ FL.foldM DT.simplifyTurnoutASEFold aseTurnoutRaw
  K.retrieveOrMakeTransformed @K.DefaultSerializer @K.DefaultCacheData
    (fmap FS.toS . FL.fold FL.list)
    (F.toFrame . fmap FS.fromS)
    ("data/turnout_simpleASE.bin" :: T.Text)
    cachedASE_Turnout make

asrTurnoutLoader :: K.DefaultEffects r => K.Sem r (K.ActionWithCacheTime r (F.Frame BR.TurnoutASR))
asrTurnoutLoader =
  cachedFrameLoader
  (DataSets $ T.pack BR.detailedASRTurnoutCSV)
  Nothing
  Nothing
  id
  Nothing
  "asrTurnout.sbin"

simpleASRTurnoutLoader :: K.DefaultEffects r
  => K.Sem r (K.ActionWithCacheTime r (F.FrameRec [BR.Year, DT.SimpleAgeC, DT.SexC, DT.SimpleRaceC, BR.Population, BR.Citizen, BR.Registered, BR.Voted]))
simpleASRTurnoutLoader = do
  cachedASR_Turnout <- asrTurnoutLoader
  let make asrTurnoutRaw = K.knitEither $ FL.foldM DT.simplifyTurnoutASRFold asrTurnoutRaw
  K.retrieveOrMakeTransformed @K.DefaultSerializer @K.DefaultCacheData
    (fmap FS.toS . FL.fold FL.list)
    (F.toFrame . fmap FS.fromS)
    ("data/turnout_simpleASR.bin" :: T.Text)
    cachedASR_Turnout make


stateAbbrCrosswalkLoader ::  K.DefaultEffects r => K.Sem r (K.ActionWithCacheTime r (F.Frame BR.States))
stateAbbrCrosswalkLoader = cachedFrameLoader (DataSets $ T.pack BR.statesCSV) Nothing Nothing id Nothing "stateAbbr.sbin"

type StateTurnoutCols = F.RecordColumns BR.StateTurnout

stateTurnoutLoader :: K.DefaultEffects r => K.Sem r (K.ActionWithCacheTime r (F.Frame BR.StateTurnout))
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

houseElectionsLoader :: K.DefaultEffects r => K.Sem r (K.ActionWithCacheTime r (F.FrameRec HouseElectionCols))
houseElectionsLoader = cachedFrameLoader (DataSets $ T.pack BR.houseElectionsCSV) Nothing Nothing processHouseElectionRow Nothing "houseElections.sbin"

data DataPath = DataSets T.Text | LocalData T.Text

getPath :: DataPath -> IO FilePath
getPath dataPath = case dataPath of
  DataSets fp -> liftIO $ BR.usePath (T.unpack fp)
  LocalData fp -> return (T.unpack fp)

-- if the file is local, use modTime
-- if the file is from the data-set library, assume the cached version is good since the modtime
-- may change any time that library is re-installed.
dataPathWithCacheTime :: Applicative m => DataPath -> IO (K.WithCacheTime m DataPath)
dataPathWithCacheTime dp@(LocalData _) = do
  modTime <- getPath dp >>= System.getModificationTime
  return $ K.withCacheTime (Just modTime) (pure dp)
dataPathWithCacheTime dp@(DataSets _) = return $ K.withCacheTime Nothing (pure dp)


-- file has qs
-- Filter qs
-- transform to rs
cachedRecStreamLoader
  :: forall qs rs r
   . ( V.RMap rs
     , V.RMap qs
     , F.ReadRec qs
     , S.GSerializePut (Rep (F.Rec FS.SElField rs))
     , S.GSerializeGet (Rep (F.Rec FS.SElField rs))
     , Generic (F.Rec FS.SElField rs)
     , K.DefaultEffects r
     )
  => DataPath
  -> Maybe F.ParserOptions
  -> Maybe (F.Record qs -> Bool)
  -> (F.Record qs -> F.Record rs)
  -> Maybe T.Text -- ^ optional cache-path. Defaults to "data/"
  -> T.Text -- ^ cache key
  -> K.Sem r (K.StreamWithCacheTime r (F.Record rs)) -- Streamly.SerialT (K.Sem r) (F.Record rs)
cachedRecStreamLoader dataPath parserOptionsM filterM fixRow cachePathM key = do
  let cacheRecList :: T.Text
                   -> K.ActionWithCacheTime r DataPath
                   -> (DataPath -> Streamly.SerialT (P.Sem r) (F.Record rs))
                   -> K.Sem r (K.StreamWithCacheTime r (F.Record rs))
      cacheRecList = K.retrieveOrMakeTransformedStream @K.DefaultSerializer @K.DefaultCacheData FS.toS FS.fromS 
      cacheKey      = (fromMaybe "data/" cachePathM) <> key      
  K.logLE K.Diagnostic $ "loading or retrieving and saving data at key=" <> cacheKey
  cachedDataPath :: K.ActionWithCacheTime r DataPath <- liftIO $ dataPathWithCacheTime dataPath 
  cacheRecList cacheKey cachedDataPath (\dataPath -> fixMonadCatch $ recStreamLoader dataPath parserOptionsM filterM fixRow)

recStreamLoader
  :: forall qs rs t m
   . ( V.RMap rs
     , V.RMap qs
     , F.ReadRec qs
     , Monad m
     , Monad (t m)
     , MonadIO m
     , Exceptions.MonadCatch m
     , Streamly.IsStream t
     )
  => DataPath
  -> Maybe F.ParserOptions
  -> Maybe (F.Record qs -> Bool)
  -> (F.Record qs -> F.Record rs)
  -> t m (F.Record rs)
recStreamLoader dataPath parserOptionsM filterM fixRow = do
  let csvParserOptions =
        F.defaultParser { F.quotingMode = F.RFC4180Quoting ' ' }
      parserOptions = (fromMaybe csvParserOptions parserOptionsM)
      filter = fromMaybe (const True) filterM
  path <- Streamly.yieldM $ liftIO $ getPath dataPath
  Streamly.map fixRow
    $ BR.loadToRecStream @qs csvParserOptions path filter

-- file has qs
-- Filter qs
-- transform to rs
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
     , K.DefaultEffects r
     )
  => DataPath
  -> Maybe F.ParserOptions
  -> Maybe (F.Record qs -> Bool)
  -> (F.Record qs -> F.Record rs)
  -> Maybe T.Text -- ^ optional cache-path. Defaults to "data/"
  -> T.Text -- ^ cache key
  -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec rs))
cachedFrameLoader filePath parserOptionsM filterM fixRow cachePathM key = 
  (fmap F.toFrame . K.streamToAction Streamly.toList) <$> cachedRecStreamLoader filePath parserOptionsM filterM fixRow cachePathM key


-- file has qs
-- Filter qs
-- transform to rs
-- This one uses "FrameLoader" directly since there's an efficient disk to Frame
-- routine available.
frameLoader
  :: forall qs rs r
  . (K.DefaultEffects r
    , F.ReadRec qs
    , FI.RecVec qs
    , V.RMap qs
    )
  => DataPath
  -> Maybe F.ParserOptions
  -> Maybe (F.Record qs -> Bool)
  -> (F.Record qs -> F.Record rs)
  -> K.Sem r (F.FrameRec rs)
frameLoader filePath parserOptionsM filterM fixRow = do
  let csvParserOptions =
        F.defaultParser { F.quotingMode = F.RFC4180Quoting ' ' }
      parserOptions = (fromMaybe csvParserOptions parserOptionsM)
      filter = fromMaybe (const True) filterM
  path <- liftIO $ getPath filePath
  K.logLE K.Diagnostic ("Attempting to load data from " <> (T.pack path) <> " into a frame.")
  fmap fixRow <$> BR.loadToFrame parserOptions path filter

-- file has fs
-- load fs
-- rcast to qs
-- filter qs
-- "fix" the maybes in qs
-- transform to rs
maybeFrameLoader
  :: forall (fs :: [(Symbol, Type)]) qs rs r
   . ( V.RMap rs
     , V.RMap fs
     , F.ReadRec fs
     , FI.RecVec qs
     , V.RFoldMap qs
     , V.RPureConstrained V.KnownField qs
     , V.RecApplicative qs
     , V.RApply qs
     , qs F.⊆ fs
     , FI.RecVec rs
     , V.RFoldMap rs
     , S.GSerializePut (Rep (F.Rec FS.SElField rs))
     , S.GSerializeGet (Rep (F.Rec FS.SElField rs))
     , Generic (F.Rec FS.SElField rs)
     , K.DefaultEffects r
     , Show (F.Record qs)
     , V.RMap qs
     , V.RecordToList qs
     , (V.ReifyConstraint Show (Maybe F.:. F.ElField) qs)
     )
  => DataPath
  -> Maybe F.ParserOptions
  -> Maybe (F.Rec (Maybe F.:. F.ElField) qs -> Bool)
  -> (F.Rec (Maybe F.:. F.ElField) qs -> (F.Rec (Maybe F.:. F.ElField) qs))
  -> (F.Record qs -> F.Record rs)
  -> K.Sem r (F.FrameRec rs)
maybeFrameLoader  dataPath parserOptionsM filterMaybesM fixMaybes transformRow
  = fmap F.toFrame $ Streamly.toList $ K.streamlyToKnitS $ maybeRecStreamLoader @fs @qs @rs dataPath parserOptionsM filterMaybesM fixMaybes transformRow

-- file has fs
-- load fs
-- rcast to qs
-- filter qs
-- "fix" the maybes in qs
-- transform to rs
cachedMaybeRecStreamLoader
  :: forall (fs :: [(Symbol, Type)]) qs rs r
   . ( V.RMap rs
     , V.RMap fs
     , F.ReadRec fs
     , V.RFoldMap qs
     , V.RPureConstrained V.KnownField qs
     , V.RecApplicative qs
     , V.RApply qs
     , qs F.⊆ fs
     , FI.RecVec qs
     , V.RFoldMap rs
     , S.GSerializePut (Rep (F.Rec FS.SElField rs))
     , S.GSerializeGet (Rep (F.Rec FS.SElField rs))
     , Generic (F.Rec FS.SElField rs)
     , K.DefaultEffects r
     , Show (F.Record qs)
     , V.RMap qs
     , V.RecordToList qs
     , (V.ReifyConstraint Show (Maybe F.:. F.ElField) qs)
     )
  => DataPath
  -> Maybe F.ParserOptions
  -> Maybe (F.Rec (Maybe F.:. F.ElField) qs -> Bool)
  -> (F.Rec (Maybe F.:. F.ElField) qs -> (F.Rec (Maybe F.:. F.ElField) qs))
  -> (F.Record qs -> F.Record rs)
  -> Maybe T.Text -- ^ optional cache-path. Defaults to "data/"
  -> T.Text -- ^ cache key
  -> K.Sem r (K.StreamWithCacheTime r (F.Record rs))
cachedMaybeRecStreamLoader dataPath parserOptionsM filterMaybesM fixMaybes transformRow cachePathM key = do
  let cacheRecStream :: T.Text
                     -> K.ActionWithCacheTime r DataPath
                     -> (DataPath -> Streamly.SerialT (P.Sem r) (F.Record rs))
                     -> K.Sem r (K.StreamWithCacheTime r (F.Record rs))
      cacheRecStream = K.retrieveOrMakeTransformedStream @K.DefaultSerializer @K.DefaultCacheData FS.toS FS.fromS 
      cacheKey      = (fromMaybe "data/" cachePathM) <> key
  K.logLE K.Diagnostic
    $  "loading or retrieving and saving data at key="
    <> cacheKey
  cachedDataPath <- liftIO $ dataPathWithCacheTime dataPath
  cacheRecStream cacheKey cachedDataPath $ \dataPath -> K.streamlyToKnitS $ maybeRecStreamLoader @fs @qs @rs dataPath parserOptionsM filterMaybesM fixMaybes transformRow

-- file has fs
-- load fs
-- rcast to qs
-- filter qs
-- "fix" the maybes in qs
-- transform to rs
maybeRecStreamLoader
  :: forall fs qs rs
   . ( V.RMap fs
     , F.ReadRec fs
     , FI.RecVec qs
     , V.RFoldMap qs
     , V.RMap qs
     , V.RPureConstrained V.KnownField qs
     , V.RecApplicative qs
     , V.RApply qs
     , qs F.⊆ fs
--     , K.DefaultEffects r
     , Show (F.Record qs)
     , V.RecordToList qs
     , (V.ReifyConstraint Show (Maybe F.:. F.ElField) qs)
     )
  => DataPath
  -> Maybe F.ParserOptions
  -> Maybe (F.Rec (Maybe F.:. F.ElField) qs -> Bool)
  -> (F.Rec (Maybe F.:. F.ElField) qs -> (F.Rec (Maybe F.:. F.ElField) qs))
  -> (F.Record qs -> F.Record rs)
  -> Streamly.SerialT K.StreamlyM (F.Record rs)
maybeRecStreamLoader dataPath parserOptionsM filterMaybesM fixMaybes transformRow = do
  let csvParserOptions =
        F.defaultParser { F.quotingMode = F.RFC4180Quoting ' ' }
      parserOptions = (fromMaybe csvParserOptions parserOptionsM)
      filterMaybes = fromMaybe (const True) filterMaybesM
  path <- liftIO $ getPath dataPath
  Streamly.map transformRow
    $ BR.processMaybeRecStream fixMaybes (const True)
    $ BR.loadToMaybeRecStream @fs csvParserOptions path filterMaybes

-- file has fs
-- load fs
-- rcast to qs
-- filter qs
-- "fix" the maybes in qs
-- transform to rs
cachedMaybeFrameLoader
  :: forall fs qs rs r
   . ( K.DefaultEffects r
     , V.RMap rs
     , V.RFoldMap rs
     , V.RMap fs
     , F.ReadRec fs
     , FI.RecVec qs
     , V.RFoldMap qs
     , V.RPureConstrained V.KnownField qs
     , V.RecApplicative qs
     , V.RApply qs
     , qs F.⊆ fs
     , FI.RecVec rs
     , S.GSerializePut (Rep (F.Rec FS.SElField rs))
     , S.GSerializeGet (Rep (F.Rec FS.SElField rs))
     , Generic (F.Rec FS.SElField rs)
     , Show (F.Record qs)
     , V.RMap qs
     , V.RecordToList qs
     , (V.ReifyConstraint Show (Maybe F.:. F.ElField) qs)
     )
  => DataPath
  -> Maybe F.ParserOptions
  -> Maybe (F.Rec (Maybe F.:. F.ElField) qs -> Bool)
  -> (F.Rec (Maybe F.:. F.ElField) qs -> (F.Rec (Maybe F.:. F.ElField) qs))
  -> (F.Record qs -> F.Record rs)
  -> Maybe T.Text -- ^ optional cache-path. Defaults to "data/"
  -> T.Text -- ^ cache key
  -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec rs))
cachedMaybeFrameLoader dataPath parserOptionsM filterMaybesM fixMaybes transformRow cachePathM key
  = (fmap F.toFrame . K.streamToAction Streamly.toList) <$> cachedMaybeRecStreamLoader @fs @qs @rs dataPath parserOptionsM filterMaybesM fixMaybes transformRow cachePathM key



fixMonadCatch :: (P.MemberWithError (P.Error Exceptions.SomeException) r)
              => Streamly.SerialT (Exceptions.CatchT (K.Sem r)) a -> Streamly.SerialT (K.Sem r) a
fixMonadCatch = Streamly.hoist f where
  f :: forall r a. (P.MemberWithError (P.Error Exceptions.SomeException) r) =>  Exceptions.CatchT (K.Sem r) a -> K.Sem r a
  f = join . fmap P.fromEither . Exceptions.runCatchT
{-# INLINEABLE fixMonadCatch #-}


{-
maybeRecListLoader 
   :: forall qs ls rs r
   . (K.DefaultEffects r
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
   -> Maybe (F.Rec (Maybe F.:. F.ElField) qs -> Bool)
   -> F.Rec (Maybe F.:. F.ElField) qs -> (F.Rec (Maybe F.:. F.ElField) qs))
   -> (F.Record qs -> F.Record rs)
   -> K.Sem r [F.Record rs]
maybeRecListLoader filePath parserOptionsM filterMaybes fixMaybes fixRow =
  Streaml.toList $


 let csvParserOptions =
       F.defaultParser { F.quotingMode = F.RFC4180Quoting ' ' }
     parserOptions = (fromMaybe csvParserOptions parserOptionsM)  
 path      <- liftIO $ getPath filePath
 K.logLE K.Diagnostic
       ("Attempting to load data from " <> (T.pack path) <> " into a Frame (Maybe .: ElField), filter and fix.")
 maybeRecs <- BR.loadToMaybeRecs @qs @ls @r parserOptions
              filterMaybes
              path
 fmap fixRow <$> BR.processMaybeRecs fixMaybes (const True) maybeRecs         
  
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
     , K.DefaultEffects r
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
          <$> do
                path <- liftIO $ BR.usePath (T.unpack filePath) -- translate data-sets path to local 
                BR.loadToRecList parserOptions path (const True)
      Just (filterMaybes, fixMaybes) ->
        fmap fixRow
          <$> do
                path      <- liftIO $ BR.usePath (T.unpack filePath) -- translate data-sets path to local
                maybeRecs <- BR.loadToMaybeRecs @qs @qs parserOptions
                                                        filterMaybes
                                                        path
                BR.processMaybeRecs fixMaybes (const True) maybeRecs

-}
{-
stateCountyCDLoader :: K.DefaultEffects r => K.Sem r (F.Frame BR.StateCountyCD)
stateCountyCDLoader = cachedFrameLoader (DataSets $ T.pack BR.stateCounty116CDCSV) Nothing id Nothing "stateCountyCD.bin"

stateCountyTractPUMALoader :: K.DefaultEffects r => K.Sem r (F.Frame BR.StateCountyTractPUMA)
stateCountyTractPUMALoader = cachedFrameLoader (DataSets $ T.pack BR.stateCountyTractPUMACSV) Nothing id Nothing "stateCountyTractPUMA.bin"


type CountyCrosswalkWithPop = [BR.StateFIPS, BR.CountyFIPS, BR.CongressionalDistrict, BR.PUMA5CE, BR.Year, DT.PopCountOf, DT.PopCount]

processPopByCountyRow :: M.Map (Int,Int) Int -> M.Map (Int, Int) (Set.Set Int) ->  F.Record PopByCountyRaw -> Either T.Text [F.Record CountyCrosswalkWithPop]
processPopByCountyRow cdByStateCounty pumaByStateCounty r = do 
  let sf :: Int = F.rgetField @BR.STATE r
      cf :: Int = F.rgetField @BR.COUNTY r
      pcOf = DT.PC_All
      cd = fromMaybe 0 $ M.lookup (sf, cf) cdByStateCounty
  pumas <- maybe
           (Left $ "Couldn't find (stateFIPS, countyFIPS) " <> (T.pack $ show (sf, cf)) <> " in pumaByStateCounty")
           Right
           (fmap Set.toList $ M.lookup (sf, cf) pumaByStateCounty)
  let prefixRecs :: [F.Record [BR.StateFIPS, BR.CountyFIPS, BR.CongressionalDistrict, BR.PUMA5CE]]
      prefixRecs = fmap (\x -> sf F.&: cf F.&: cd F.&: x F.&: V.RNil ) pumas
      pop2010 =  F.rgetField @BR.POPESTIMATE2010 r
      pop2012 =  F.rgetField @BR.POPESTIMATE2012 r
      pop2014 =  F.rgetField @BR.POPESTIMATE2014 r
      pop2016 =  F.rgetField @BR.POPESTIMATE2016 r
      pop2018 =  F.rgetField @BR.POPESTIMATE2018 r
      r2010 = fmap (`V.rappend` (2010 F.&: DT.PC_All F.&: pop2010 F.&: V.RNil)) prefixRecs
      r2012 = fmap (`V.rappend` (2012 F.&: DT.PC_All F.&: pop2012 F.&: V.RNil)) prefixRecs
      r2014 = fmap (`V.rappend` (2014 F.&: DT.PC_All F.&: pop2014 F.&: V.RNil)) prefixRecs
      r2016 = fmap (`V.rappend` (2016 F.&: DT.PC_All F.&: pop2016 F.&: V.RNil)) prefixRecs
      r2018 = fmap (`V.rappend` (2018 F.&: DT.PC_All F.&: pop2018 F.&: V.RNil)) prefixRecs
  return $ concat $ [r2010, r2012, r2014, r2016, r2018]


type PopByCountyRaw = [BR.STATE, BR.COUNTY, BR.POPESTIMATE2010, BR.POPESTIMATE2012, BR.POPESTIMATE2014, BR.POPESTIMATE2016, BR.POPESTIMATE2018]
popByCountyRawLoader :: K.DefaultEffects r => K.Sem r (F.FrameRec PopByCountyRaw)
popByCountyRawLoader = do  
  let action :: K.DefaultEffects r => K.Sem r (F.FrameRec PopByCountyRaw)
      action = do
        F.filterFrame (\r -> F.rgetField @BR.COUNTY r > 0)
          <$> frameLoader @(F.RecordColumns BR.PopulationsByCounty_Raw) (DataSets $ T.pack BR.popsByCountyCSV) Nothing F.rcast
  BR.retrieveOrMakeFrame "data/popByCountyRaw.bin" action
  
countyCrosswalkWithPopLoader :: K.DefaultEffects r => K.Sem r (F.FrameRec CountyCrosswalkWithPop)
countyCrosswalkWithPopLoader = do
  let action = do
        let key r = (F.rgetField @BR.StateFIPS r, F.rgetField @BR.CountyFIPS r)
            keyVal f r = (key r, f r)
            mapWithF f = FL.Fold (\m (k, a) -> M.insertWith f k a m) M.empty id
        stateCountyCD_map <-  FL.fold (FL.premap (keyVal (F.rgetField @BR.CongressionalDistrict)) FL.map) <$> stateCountyCDLoader
        stateCountyTractPUMA_map <- FL.fold (FL.premap (keyVal (Set.singleton . F.rgetField @BR.PUMA5CE)) $ mapWithF (<>)) <$> stateCountyTractPUMALoader
        rawFrame <- popByCountyRawLoader
{-        rawFrame <- F.filterFrame (\r -> F.rgetField @BR.COUNTY r > 0)
                    <$> frameLoader (DataSets $ T.pack BR.popsByCountyCSV) Nothing id -}
        processed <- K.knitEither $ traverse (processPopByCountyRow stateCountyCD_map stateCountyTractPUMA_map ) $ FL.fold FL.list rawFrame
        return $ F.toFrame $ concat $ processed
  BR.retrieveOrMakeFrame "data/popByCounty.bin" action



{-
countyCrosswalkWithPopLoader :: K.DefaultEffects r => K.Sem r (F.FrameRec CountyCrosswalkWithPop)
countyCrosswalkWithPopLoader = do
  let action = do
        let key r = (F.rgetField @BR.StateFIPS r, F.rgetField @BR.CountyFIPS r)
            keyVal f r = (key r, f r)
        popByCounty <- popByCountyLoader
        stateCountyCD_map <-  FL.fold (FL.premap (keyVal (F.rgetField @BR.CongressionalDistrict)) FL.map) <$> stateCountyCDLoader
        stateCountyTractPUMA_map <- FL.fold (FL.premap (keyVal (F.rgetField @BR.PUMA5CE)) FL.map) <$> stateCountyTractPUMALoader
        let add r = do
              let cd = fromMaybe 0 $ M.lookup (key r) stateCountyCD_map
              puma <- M.lookup (key r) stateCountyTractPUMA_map
              return $ FT.mutate (const $ FT.recordSingleton @BR.CongressionalDistrict cd)
                $ FT.mutate (const $ FT.recordSingleton @BR.PUMA5CE puma) r
        K.knitMaybe "Missing State/County pair in PUMA crosswalk" $ fmap (F.toFrame . fmap F.rcast) $ traverse add $ FL.fold FL.list popByCounty
  BR.retrieveOrMakeFrame "data/countyCrosswalkWithPop.bin" action
-}

type CVAPByCDAndRace = [BR.StateAbbreviation, BR.StateFIPS, BR.CongressionalDistrict, DT.Race5C, BR.VAP, ET.CVAP]
type CVAPByCDAndRace' = [BR.StateFIPS, BR.CongressionalDistrict, DT.Race5C, BR.VAP, ET.CVAP]

mapLeft :: (a -> b) -> Either a c -> Either b c
mapLeft f (Left x) = Left (f x)
mapLeft _ (Right y) = Right y

processCVAPByCDRow :: BR.CVAPByCDAndRace_Raw -> Either T.Text (Maybe (F.Record CVAPByCDAndRace'))
processCVAPByCDRow r = do
  let cat = F.rgetField @BR.Lntitle 
      geoid = F.rgetField @BR.Geoid
  stateFIPS <- mapLeft T.pack $ fmap fst $ T.decimal $ T.drop 7 . T.take 2 $ geoid r
  cd <- mapLeft T.pack $ fmap fst $ T.decimal $ T.drop 9 $ geoid r
  let r5M = case cat r of
          "White Alone" -> Just DT.R5_WhiteNonLatinx
          "Black or African American Alone" -> Just DT.R5_Black
          "Hispanic or Latino" -> Just DT.R5_Latinx
          "Asian Alone" -> Just DT.R5_Asian
          "Total" -> Nothing
          "Not Hispanic Or Latino" -> Nothing
          _ -> Just DT.R5_Other
  let mutate r5 = FT.retypeColumn @BR.CvapEst @ET.CVAP
                  . FT.retypeColumn @BR.AduEst @BR.VAP
                  . FT.mutate (const $ FT.recordSingleton @BR.StateFIPS stateFIPS)
                  . FT.mutate (const $ FT.recordSingleton @BR.CongressionalDistrict cd)
                  . FT.mutate (const $ FT.recordSingleton @DT.Race5C r5)                                       
  return $ F.rcast <$> ((fmap mutate r5M) <*> pure r)

cvapByCDLoader :: K.DefaultEffects r => K.Sem r (F.FrameRec CVAPByCDAndRace)
cvapByCDLoader = do
  let csvParserOptions =
        F.defaultParser { F.quotingMode = F.RFC4180Quoting ' ' }
  path <- liftIO $ getPath (DataSets $ T.pack BR.cvapByCDAndRace2014_2018CSV)
  K.logLE K.Diagnostic ("Attempting to loading data from " <> (T.pack path))
  rawFrame <- BR.loadToFrame csvParserOptions path (const True)
  processed <- K.knitEither $ fmap catMaybes $ sequence $ fmap processCVAPByCDRow $ FL.fold FL.list rawFrame
  stateAbbrCrosswalk <- stateAbbrCrosswalkLoader
  withAbbrs <- K.knitMaybe "Missing state in state crosswalk" $ FJ.leftJoinM @'[BR.StateFIPS] (F.toFrame processed) stateAbbrCrosswalk
  return $ F.toFrame $ fmap F.rcast withAbbrs
  
-}

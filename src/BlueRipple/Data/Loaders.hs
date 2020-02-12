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
import qualified Frames                        as F
import qualified Frames.CSV                    as F
import qualified Frames.InCore                 as FI
import qualified Frames.TH                     as F

import qualified Frames.ParseableTypes         as FP
import qualified Frames.MaybeUtils             as FM
import qualified Frames.Transform              as FT
import qualified Frames.Serialize              as FS




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
presidentialByStateFrame =
  cachedMaybeFrameLoader @(F.RecordColumns BR.PresidentialByState) @PEFromCols
    (T.pack BR.presidentialByStateCSV)
    Nothing
    (const True)
    id
    fixPresidentialElectionRow
    Nothing
    "presByState.bin"

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
  => T.Text -- ^ path to file
  -> Maybe F.ParserOptions
  -> (F.Record qs -> F.Record rs)
  -> Maybe T.Text -- ^ optional cache-path. Defaults to "data/"
  -> T.Text -- ^ cache key
  -> K.Sem r (F.FrameRec rs)
cachedFrameLoader filePath parserOptionsM fixRow cachePathM key = do
  let csvParserOptions =
        F.defaultParser { F.quotingMode = F.RFC4180Quoting ' ' }
      cacheFrame = K.retrieveOrMakeTransformed
        (fmap FS.toS . FL.fold FL.list)
        (F.toFrame . fmap FS.fromS)
      cacheKey      = (fromMaybe "data/" cachePathM) <> key
      parserOptions = (fromMaybe csvParserOptions parserOptionsM)
  K.logLE K.Diagnostic
    $  "loading or retrieving and saving data at key="
    <> cacheKey
  cacheFrame cacheKey
    $   fmap fixRow
    <$> do
          path <- liftIO $ BR.usePath (T.unpack filePath) -- translate data-sets path to local 
          BR.loadToFrame parserOptions path (const True)


cachedMaybeFrameLoader
  :: forall qs ls rs r
   . ( V.RMap rs
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
     )
  => T.Text -- ^ path to file
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
    let csvParserOptions =
          F.defaultParser { F.quotingMode = F.RFC4180Quoting ' ' }
        cacheFrame = K.retrieveOrMakeTransformed
          (fmap FS.toS . FL.fold FL.list)
          (F.toFrame . fmap FS.fromS)
        cacheKey      = (fromMaybe "data/" cachePathM) <> key
        parserOptions = (fromMaybe csvParserOptions parserOptionsM)
    K.logLE K.Diagnostic
      $  "loading or retrieving and saving data at key="
      <> cacheKey
    cacheFrame cacheKey
      $   fmap fixRow
      <$> do
            path      <- liftIO $ BR.usePath (T.unpack filePath) -- translate data-sets path to local 
            maybeRecs <- BR.loadToMaybeRecs @qs @ls @r parserOptions
                                                       filterMaybes
                                                       path
            BR.maybeRecsToFrame fixMaybes (const True) maybeRecs



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

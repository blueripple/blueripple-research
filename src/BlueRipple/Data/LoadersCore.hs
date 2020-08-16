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
--{-# OPTIONS_GHC  -O0             #-}
{-# OPTIONS_GHC -fplugin=Polysemy.Plugin #-}

module BlueRipple.Data.LoadersCore where

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
import qualified Streamly.Data.Fold as Streamly.Fold
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
     , K.KnitEffects r
     , K.CacheEffectsD r
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
        F.defaultParser { F.quotingMode = F.RFC4180Quoting '"' }
      parserOptions = (fromMaybe csvParserOptions parserOptionsM)
      filter = fromMaybe (const True) filterM
  path <- Streamly.yieldM $ liftIO $ getPath dataPath
  Streamly.map fixRow
    $ Streamly.tapOffsetEvery 500000 500000 (Streamly.Fold.drainBy (const $ liftIO $ putStrLn "recStreamLoader: 500,000"))
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
     , K.KnitEffects r
     , K.CacheEffectsD r
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
  . (K.KnitEffects r
    , K.CacheEffectsD r
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
     , K.KnitEffects r
     , K.CacheEffectsD r
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
     , K.KnitEffects r
     , K.CacheEffectsD r
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
--     , K.KnitEffects r, K.CacheEffectsD r
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
   . ( K.KnitEffects r
     , K.CacheEffectsD r
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


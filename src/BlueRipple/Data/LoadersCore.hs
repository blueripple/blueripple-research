{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE PolyKinds           #-}
{-# LANGUAGE TypeFamilies        #-}
{-# LANGUAGE TypeOperators       #-}
{-# LANGUAGE TypeApplications    #-}
{-# LANGUAGE Rank2Types          #-}
{-# LANGUAGE ScopedTypeVariables #-}
--{-# OPTIONS_GHC  -O0             #-}
{-# OPTIONS_GHC -fplugin=Polysemy.Plugin #-}

module BlueRipple.Data.LoadersCore
  (
    module BlueRipple.Data.LoadersCore
  )
where

import qualified BlueRipple.Data.DataFrames    as BR
import qualified BlueRipple.Utilities.KnitUtils as BR

import qualified Knit.Report                   as K
import qualified Knit.Utilities.Streamly       as K

import qualified Control.Monad.Catch.Pure      as Exceptions
import qualified Control.Monad.State           as ST
import qualified Control.Foldl                 as FL
import           Data.Serialize.Text            ( )
import qualified Data.Text                     as T

import qualified Data.Vinyl                    as V
import qualified Frames                        as F

#if MIN_VERSION_streamly(0,9,0)
import qualified Streamly.Data.Stream as Streamly
import qualified Streamly.Data.Stream.Prelude as SP
--import qualified Streamly.Internal.Data.Stream.StreamD.Transform as Streamly
import qualified Streamly.Internal.Data.Stream as Streamly
#elif MIN_VERSION_streamly(0,8,0)
import qualified Streamly.Prelude as Streamly
import qualified Streamly.Internal.Data.Stream.IsStream.Transform as Streamly
#else
import qualified Streamly.Prelude as Streamly
import qualified Streamly
import qualified Streamly.Internal.Prelude as Streamly
#endif

import qualified Streamly.Data.Fold as Streamly.Fold
import qualified Streamly.Internal.Data.Fold as Streamly.Fold

import qualified Frames.Streamly.InCore        as FStreamly
import qualified Frames.Streamly.CSV        as FStreamly
import Frames.Streamly.Streaming.Streamly (StreamlyStream(..))

import qualified System.Directory as System
import qualified System.Clock
import GHC.TypeLits (Symbol)

#if MIN_VERSION_streamly(0,9,0)
type Stream = Streamly.Stream
type MonadAsync m = SP.MonadAsync m

sMap :: Monad m => (a -> b) -> Stream m a  -> Stream m b
sMap = fmap
{-# INLINE sMap #-}
#else
type Stream = Streamly.SerialT
type MonadAsync m = Streamly.MonadAsync m

sMap :: Monad m => (a -> b) -> Stream m a  -> Stream m b
sMap = Streamly.map
{-# INLINE sMap #-}
#endif


data DataPath = DataSets T.Text | LocalData T.Text

getPath :: DataPath -> IO FilePath
getPath dataPath = case dataPath of
  DataSets fp -> liftIO $ BR.dataPath (toString fp)
  LocalData fp -> return (toString fp)

-- if the file is local, use modTime
-- if the file is from the data-set library, assume the cached version is good since the modtime
-- may change any time that library is re-installed.
dataPathWithCacheTime :: Applicative m => DataPath -> IO (K.WithCacheTime m DataPath)
dataPathWithCacheTime dp@(LocalData _) = do
  modTime <- getPath dp >>= System.getModificationTime
  return $ K.withCacheTime (Just modTime) (pure dp)
dataPathWithCacheTime dp@(DataSets _) = return $ K.withCacheTime Nothing (pure dp)

#if MIN_VERSION_streamly(0,9,0)
recStreamLoader
  :: forall qs rs m
   . ( --V.RMap rs
     V.RMap qs
     , FStreamly.StrictReadRec qs
     , MonadAsync m
     , FL.PrimMonad m
     , Exceptions.MonadCatch m
     )
  => DataPath
  -> Maybe FStreamly.ParserOptions
  -> Maybe (F.Record qs -> Bool)
  -> (F.Record qs -> F.Record rs)
  -> Stream m (F.Record rs)
recStreamLoader dataPath parserOptionsM mFilter fixRow = Streamly.concatEffect $ do
  let csvParserOptions =
        FStreamly.defaultParser --{ F.quotingMode = F.RFC4180Quoting '"' }
      parserOptions = (fromMaybe csvParserOptions parserOptionsM)
      filterF !r = fromMaybe (const True) mFilter r
      strictFix !r = fixRow r
  path <- liftIO $ getPath dataPath
  pure
    $ sMap strictFix
    $! Streamly.tapOffsetEvery
    250000
    250000
    (runningCountF
      ("Read (k rows, from \"" <> toText path <> "\")")
      (\n-> " "
            <> ("from \""
                 <> toText path
                 <> "\": "
                 <> (show $ 250000 * n))
      )
      ("loading \"" <> toText path <> "\" from disk finished.")
    )
    $! BR.loadToRecStream @qs parserOptions path filterF

-- file/rowGen has qs
-- Filter qs
-- transform to rs
cachedFrameLoader
  :: forall qs rs r
   . ( V.RMap rs
     , V.RMap qs
     , FStreamly.StrictReadRec qs
--     , FStreamly.RecVec qs
     , FStreamly.RecVec rs
     , BR.RecSerializerC rs
     , K.KnitEffects r
     , BR.CacheEffects r
     )
  => DataPath
  -> Maybe FStreamly.ParserOptions
  -> Maybe (F.Record qs -> Bool)
  -> (F.Record qs -> F.Record rs)
  -> Maybe T.Text -- ^ optional cache-path. Defaults to "data/"
  -> T.Text -- ^ cache key
  -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec rs))
cachedFrameLoader filePath parserOptionsM mFilter fixRow cachePathM key = do
  let cacheKey      = fromMaybe "data/" cachePathM <> key
  cachedDataPath :: K.ActionWithCacheTime r DataPath <- liftIO $ dataPathWithCacheTime filePath
  K.logLE (K.Debug 3) $ "loading or retrieving and saving data at key=" <> cacheKey
  BR.retrieveOrMakeFrame cacheKey cachedDataPath $ \dataPath -> do
    let recStream = recStreamLoader @qs @rs dataPath parserOptionsM mFilter fixRow
    K.streamlyToKnit $ FStreamly.inCoreAoS @_ @rs @StreamlyS $ StreamlyStream recStream

#else
recStreamLoader
  :: forall qs rs t m
   . ( --V.RMap rs
     V.RMap qs
     , FStreamly.StrictReadRec qs
     , MonadAsync m
     , FL.PrimMonad m
     , Monad (t m)
     , Exceptions.MonadCatch m
     )
  => DataPath
  -> Maybe FStreamly.ParserOptions
  -> Maybe (F.Record qs -> Bool)
  -> (F.Record qs -> F.Record rs)
  -> Stream m (F.Record rs)
recStreamLoader dataPath parserOptionsM mFilter fixRow = do
  let csvParserOptions =
        FStreamly.defaultParser --{ F.quotingMode = F.RFC4180Quoting '"' }
      parserOptions = (fromMaybe csvParserOptions parserOptionsM)
      filterF !r = fromMaybe (const True) mFilter r
      strictFix !r = fixRow r
#if MIN_VERSION_streamly(0,8,0)
  path <- Streamly.fromEffect $ liftIO $ getPath dataPath
#else
  path <- Streamly.yieldM $ liftIO $ getPath dataPath
#endif
  sMap strictFix
    $! Streamly.tapOffsetEvery
    250000
    250000
    (runningCountF
      ("Read (k rows, from \"" <> toText path <> "\")")
      (\n-> " "
            <> ("from \""
                 <> toText path
                 <> "\": "
                 <> (show $ 250000 * n))
      )
      ("loading \"" <> toText path <> "\" from disk finished.")
    )
    $! BR.loadToRecStream @qs parserOptions path filterF


-- file/rowGen has qs
-- Filter qs
-- transform to rs
cachedFrameLoader
  :: forall qs rs r
   . ( V.RMap rs
     , V.RMap qs
     , FStreamly.StrictReadRec qs
--     , FStreamly.RecVec qs
     , FStreamly.RecVec rs
     , BR.RecSerializerC rs
     , K.KnitEffects r
     , BR.CacheEffects r
     )
  => DataPath
  -> Maybe FStreamly.ParserOptions
  -> Maybe (F.Record qs -> Bool)
  -> (F.Record qs -> F.Record rs)
  -> Maybe T.Text -- ^ optional cache-path. Defaults to "data/"
  -> T.Text -- ^ cache key
  -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec rs))
cachedFrameLoader filePath parserOptionsM mFilter fixRow cachePathM key = do
  let cacheKey      = fromMaybe "data/" cachePathM <> key
  cachedDataPath :: K.ActionWithCacheTime r DataPath <- liftIO $ dataPathWithCacheTime filePath
  K.logLE (K.Debug 3) $ "loading or retrieving and saving data at key=" <> cacheKey
  BR.retrieveOrMakeFrame cacheKey cachedDataPath $ \dataPath -> do
    let recStream = recStreamLoader @qs @rs @Stream dataPath parserOptionsM mFilter fixRow
    K.streamlyToKnit $ FStreamly.inCoreAoS @_ @rs @StreamlyS $ StreamlyStream recStream
#endif
type StreamlyS = StreamlyStream Stream

-- file has qs
-- Filter qs
-- transform to rs
-- This one uses "FrameLoader" directly since there's an efficient disk to Frame
-- routine available.
frameLoader
  :: forall qs rs r
  . (K.KnitEffects r
--    , BR.CacheEffects r
    , FStreamly.StrictReadRec qs
    , FStreamly.RecVec qs
    , V.RMap qs
    )
  => DataPath
  -> Maybe FStreamly.ParserOptions
  -> Maybe (F.Record qs -> Bool)
  -> (F.Record qs -> F.Record rs)
  -> K.Sem r (F.FrameRec rs)
frameLoader filePath mParserOptions mFilter fixRow = do
  let csvParserOptions =
        FStreamly.defaultParser --
--        { FStreamly.quotingMode = F.RFC4180Quoting ' ' }
      parserOptions = fromMaybe csvParserOptions mParserOptions
      filterF !r = fromMaybe (const True) mFilter r
      strictFix !r = fixRow r
  path <- liftIO $ getPath filePath
  K.logLE (K.Debug 3) ("Attempting to load data from " <> toText path <> " into a frame.")
  fmap strictFix <$> BR.loadToFrame parserOptions path filterF

-- file has fs
-- load fs
-- rcast to qs
-- filter qs
-- "fix" the maybes in qs
-- transform to rs
maybeFrameLoader
  :: forall (fs :: [(Symbol, Type)]) qs qs' rs r
   . ( --V.RMap rs
     V.RMap fs
     , FStreamly.StrictReadRec fs
--     , FStreamly.RecVec qs
     , V.RFoldMap qs
     , V.RPureConstrained V.KnownField qs
     , V.RecApplicative qs
     , V.RApply qs
     , V.RFoldMap qs'
     , V.RPureConstrained V.KnownField qs'
     , V.RecApplicative qs'
     , V.RApply qs'
--     , V.RMap qs'
     , qs F.⊆ fs
     , FStreamly.RecVec rs
--     , V.RFoldMap rs
--     , BR.RecSerializerC rs
     , K.KnitEffects r
--     , BR.CacheEffects r
     , Show (F.Record qs)
     , V.RMap qs
     , V.RecordToList qs
     , (V.ReifyConstraint Show (Maybe F.:. F.ElField) qs)
     )
  => DataPath
  -> Maybe FStreamly.ParserOptions
  -> Maybe (F.Rec (Maybe F.:. F.ElField) qs -> Bool)
  -> (F.Rec (Maybe F.:. F.ElField) qs -> F.Rec (Maybe F.:. F.ElField) qs')
  -> (F.Record qs' -> F.Record rs)
  -> K.Sem r (F.FrameRec rs)
maybeFrameLoader  dataPath parserOptionsM mFilterMaybes fixMaybes transformRow
  = K.streamlyToKnit $ FStreamly.inCoreAoS $ StreamlyStream $ maybeRecStreamLoader @fs @qs @qs' @rs dataPath parserOptionsM mFilterMaybes fixMaybes transformRow

#if MIN_VERSION_streamly(0,9,0)
-- file has fs
-- load fs
-- rcast to qs
-- filter qs
-- "fix" the maybes in qs
-- transform to rs
maybeRecStreamLoader
  :: forall fs qs qs' rs
   . ( V.RMap fs
     , FStreamly.StrictReadRec fs
--     , FStreamly.RecVec qs
     , V.RFoldMap qs
     , V.RMap qs
     , V.RPureConstrained V.KnownField qs
     , V.RecApplicative qs
     , V.RApply qs
     , V.RFoldMap qs'
--     , V.RMap qs'
     , V.RPureConstrained V.KnownField qs'
     , V.RecApplicative qs'
     , V.RApply qs'
     , qs F.⊆ fs
--     , K.KnitEffects r, BR.CacheEffects r
     , Show (F.Record qs)
     , V.RecordToList qs
     , (V.ReifyConstraint Show (Maybe F.:. F.ElField) qs)
     )
  => DataPath
  -> Maybe FStreamly.ParserOptions
  -> Maybe (F.Rec (Maybe F.:. F.ElField) qs -> Bool)
  -> (F.Rec (Maybe F.:. F.ElField) qs -> F.Rec (Maybe F.:. F.ElField) qs')
  -> (F.Record qs' -> F.Record rs)
  -> Stream K.StreamlyM (F.Record rs)
maybeRecStreamLoader dataPath mParserOptions mFilterMaybes fixMaybes transformRow = Streamly.concatEffect $ do
  let csvParserOptions = FStreamly.defaultParser
      parserOptions = fromMaybe csvParserOptions mParserOptions
      filterMaybes !r = fromMaybe (const True) mFilterMaybes r
      strictTransform r = transformRow r
  path <- liftIO $ getPath dataPath
  pure
    $ Streamly.map strictTransform
    $ BR.processMaybeRecStream fixMaybes (const True)
    $ BR.loadToMaybeRecStream @fs parserOptions path filterMaybes
#else
-- file has fs
-- load fs
-- rcast to qs
-- filter qs
-- "fix" the maybes in qs
-- transform to rs
maybeRecStreamLoader
  :: forall fs qs qs' rs
   . ( V.RMap fs
     , FStreamly.StrictReadRec fs
--     , FStreamly.RecVec qs
     , V.RFoldMap qs
     , V.RMap qs
     , V.RPureConstrained V.KnownField qs
     , V.RecApplicative qs
     , V.RApply qs
     , V.RFoldMap qs'
--     , V.RMap qs'
     , V.RPureConstrained V.KnownField qs'
     , V.RecApplicative qs'
     , V.RApply qs'
     , qs F.⊆ fs
--     , K.KnitEffects r, BR.CacheEffects r
     , Show (F.Record qs)
     , V.RecordToList qs
     , (V.ReifyConstraint Show (Maybe F.:. F.ElField) qs)
     )
  => DataPath
  -> Maybe FStreamly.ParserOptions
  -> Maybe (F.Rec (Maybe F.:. F.ElField) qs -> Bool)
  -> (F.Rec (Maybe F.:. F.ElField) qs -> F.Rec (Maybe F.:. F.ElField) qs')
  -> (F.Record qs' -> F.Record rs)
  -> Stream K.StreamlyM (F.Record rs)
maybeRecStreamLoader dataPath mParserOptions mFilterMaybes fixMaybes transformRow = do
  let csvParserOptions = FStreamly.defaultParser
      parserOptions = fromMaybe csvParserOptions mParserOptions
      filterMaybes !r = fromMaybe (const True) mFilterMaybes r
      strictTransform r = transformRow r
  path <- liftIO $ getPath dataPath
  Streamly.map strictTransform
    $ BR.processMaybeRecStream fixMaybes (const True)
    $ BR.loadToMaybeRecStream @fs parserOptions path filterMaybes
#endif

-- file has fs
-- load fs
-- rcast to qs
-- filter qs
-- "fix" the maybes in qs
-- transform to rs
cachedMaybeFrameLoader
  :: forall fs qs qs' rs r
   . ( K.KnitEffects r
     , BR.CacheEffects r
     , V.RMap rs
--     , V.RFoldMap rs
     , V.RMap fs
     , FStreamly.StrictReadRec fs
--     , FStreamly.RecVec qs
     , V.RFoldMap qs
     , V.RPureConstrained V.KnownField qs
     , V.RecApplicative qs
     , V.RApply qs
     , V.RFoldMap qs'
     , V.RPureConstrained V.KnownField qs'
     , V.RecApplicative qs'
     , V.RApply qs'
--     , V.RMap qs'
     , qs F.⊆ fs
     , FStreamly.RecVec rs
     , BR.RecSerializerC rs
     , Show (F.Record qs)
     , V.RMap qs
     , V.RecordToList qs
     , (V.ReifyConstraint Show (Maybe F.:. F.ElField) qs)
     )
  => DataPath
  -> Maybe FStreamly.ParserOptions
  -> Maybe (F.Rec (Maybe F.:. F.ElField) qs -> Bool)
  -> (F.Rec (Maybe F.:. F.ElField) qs -> F.Rec (Maybe F.:. F.ElField) qs')
  -> (F.Record qs' -> F.Record rs)
  -> Maybe T.Text -- ^ optional cache-path. Defaults to "data/"
  -> T.Text -- ^ cache key
  -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec rs))
cachedMaybeFrameLoader dataPath mParserOptions mFilterMaybes fixMaybes transformRow cachePathM key = do
  let cachePath = fromMaybe "data" cachePathM
      cacheKey = cachePath <> "/" <> key
  BR.retrieveOrMakeFrame cacheKey (pure ())
    $ const
    $ maybeFrameLoader @fs dataPath mParserOptions mFilterMaybes fixMaybes transformRow

--  = K.streamToAction FStreamly.inCoreAoS
--    <$> cachedMaybeRecStreamLoader @fs @qs @rs dataPath mParserOptions mFilterMaybes fixMaybes transformRow cachePathM key


-- tracing fold
runningCountF :: ST.MonadIO m => T.Text -> (Int -> T.Text) -> T.Text -> Streamly.Fold.Fold m a ()
runningCountF startMsg countMsg endMsg = Streamly.Fold.Fold step start done where
#if MIN_VERSION_streamly(0,8,0)
  start = ST.liftIO (putText startMsg) >> return (Streamly.Fold.Partial 0)
  step !n _ = ST.liftIO $ do
    t <- System.Clock.getTime System.Clock.ProcessCPUTime
    putStr $ show t ++ ": "
    putTextLn $ countMsg n
    pure $ Streamly.Fold.Partial (n + 1)
  done _ = ST.liftIO $ putTextLn endMsg
#else
  start = ST.liftIO (putText startMsg) >> return 0
  step !n _ = ST.liftIO $ do
    t <- System.Clock.getTime System.Clock.ProcessCPUTime
    putStr $ show t ++ ": "
    putTextLn $ countMsg n
    return (n + 1)
  done _ = ST.liftIO $ putTextLn endMsg
#endif


{-
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
     , BR.CacheEffects r
     , Show (F.Record qs)
     , V.RMap qs
     , V.RecordToList qs
     , (V.ReifyConstraint Show (Maybe F.:. F.ElField) qs)
     )
  => DataPath
  -> Maybe F.ParserOptions
  -> Maybe (F.Rec (Maybe F.:. F.ElField) qs -> Bool)
  -> (F.Rec (Maybe F.:. F.ElField) qs -> F.Rec (Maybe F.:. F.ElField) qs)
  -> (F.Record qs -> F.Record rs)
  -> Maybe T.Text -- ^ optional cache-path. Defaults to "data/"
  -> T.Text -- ^ cache key
  -> K.Sem r (K.StreamWithCacheTime (F.Record rs))
cachedMaybeRecStreamLoader dataPath mParserOptions mFilterMaybes fixMaybes transformRow mCachePath key = do
  let cacheRecStream :: T.Text
                     -> K.ActionWithCacheTime r DataPath
                     -> (DataPath -> Streamly.SerialT K.StreamlyM (F.Record rs))
                     -> K.Sem r (K.StreamWithCacheTime (F.Record rs))
      cacheRecStream = K.retrieveOrMakeTransformedStream @K.DefaultSerializer @K.DefaultCacheData FS.toS FS.fromS
      cacheKey      = fromMaybe "data/" mCachePath <> key
  K.logLE K.Diagnostic
    $  "loading or retrieving and saving data at key="
    <> cacheKey
  cachedDataPath <- liftIO $ dataPathWithCacheTime dataPath
  cacheRecStream cacheKey cachedDataPath $ \dataPath -> maybeRecStreamLoader @fs @qs @rs dataPath mParserOptions mFilterMaybes fixMaybes transformRow
-}

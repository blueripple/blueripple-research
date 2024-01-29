{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
--{-# OPTIONS_GHC -fplugin=Polysemy.Plugin #-}

module BlueRipple.Utilities.KnitUtils
  (
    module BlueRipple.Utilities.KnitUtils
  )
where

import qualified BlueRipple.Configuration as BRC

import qualified Control.Foldl as FL
import qualified Control.Monad.Except as X
import qualified Control.Monad.Primitive as Prim
import qualified Data.Aeson as A
import qualified Data.ByteString as BS
import qualified Data.Map as M
import qualified Data.Sequence as Seq
import qualified Data.Text as T
import qualified Data.Text.IO as T
import qualified Data.Time.Calendar as Time
import qualified Data.Time.Format as Time
import qualified Data.Time.LocalTime as Time
import qualified Data.Vector as Vector
import qualified Data.Vinyl as V
import qualified Flat
import qualified Frames as F
import qualified Frames.InCore as FI
import qualified Frames.Serialize as FS
import qualified Knit.Effect.Serialize as KS
import qualified Knit.Report as K
import qualified Knit.Report.Input.MarkDown.PandocMarkDown as K
import qualified Path
import Polysemy.Error (Error)
#if MIN_VERSION_streamly(0,8,0)
--import qualified Streamly.Data.Array.Foreign         as Streamly.Array
#else
import qualified Streamly
import qualified Streamly.Memory.Array         as Streamly.Array
#endif
import qualified System.Directory as SD
import qualified System.Random.MWC as MWC
import qualified Text.Pandoc.Options as PA
--import qualified Streamly.Internal.Memory.ArrayStream as Streamly.ByteString

insureFinalSlash :: Text -> Maybe Text
insureFinalSlash t = f <$> T.unsnoc t
  where
    f (_, l) = if l == '/' then t else T.snoc t '/'

insureFinalSlashE :: Either Text Text -> Maybe (Either Text Text)
insureFinalSlashE e = case e of
  Left t -> Left <$> insureFinalSlash t
  Right t -> Right <$> insureFinalSlash t


mapDirE :: (Text -> Text) -> Either Text Text -> Either Text Text
mapDirE f (Left t) = Left $ f t
mapDirE f (Right t) = Right $ f t

cacheFromDirE :: (K.KnitEffects r, CacheEffects r)
              => Either Text Text -> Text -> K.Sem r Text
cacheFromDirE dirE n = do
  dirE' <- K.knitMaybe "cacheDirFromDirE: empty directory given in dirE argument!" $ insureFinalSlashE dirE
  case dirE' of
    Left cd -> clearIfPresentD' (cd <> n)
    Right cd -> pure (cd <> n)

clearIfPresentD' :: (K.KnitEffects r, CacheEffects r) => T.Text -> K.Sem r Text
clearIfPresentD' k = do
  K.logLE K.Warning $ "Clearing cached item with key=" <> show k
  K.clearIfPresent @T.Text @CacheData k
  pure k

clearIf' :: (K.KnitEffects r, CacheEffects r) => Bool -> Text -> K.Sem r Text
clearIf' flag k = if flag then clearIfPresentD' k else pure k

clearIfPresentD :: (K.KnitEffects r, CacheEffects r) => T.Text -> K.Sem r ()
clearIfPresentD k = void $ clearIfPresentD' k

knitX ::
  forall r a.
  K.Member (Error K.PandocError) r =>
  X.ExceptT T.Text (K.Sem r) a ->
  K.Sem r a
knitX ma = runExceptT ma >>= (K.knitEither @r)

copyAsset :: K.KnitOne r => T.Text -> T.Text -> K.Sem r ()
copyAsset sourcePath destDir = do
  sourceExists <- K.liftKnit $ SD.doesFileExist (toString sourcePath)
  if sourceExists
    then (do
             K.logLE K.Info $
               "If necessary, creating \""
               <> destDir
               <> "\" and copying \""
               <> sourcePath
               <> "\" there"
             K.liftKnit $ do
               let (_, fName) = T.breakOnEnd "/" sourcePath
               SD.createDirectoryIfMissing True (toString destDir)
               SD.copyFile (toString sourcePath) (toString $ destDir <> "/" <> fName))
    else K.knitError $ "\"" <> sourcePath <> "\" doesn't exist (copyAsset)."

brWriterOptionsF :: PA.WriterOptions -> PA.WriterOptions
brWriterOptionsF o =
  let exts = PA.writerExtensions o
   in o
        { PA.writerExtensions =
            PA.enableExtension PA.Ext_header_attributes $
--            PA.enableExtension PA.Ext_grid_tables $
--            PA.enableExtension PA.Ext_pipe_tables $
--            PA.enableExtension PA.Ext_fancy_lists $
--            PA.enableExtension PA.Ext_tex_math_dollars $
              PA.enableExtension PA.Ext_smart exts,
          PA.writerSectionDivs = True
        }

brAddMarkDown :: K.KnitOne r => T.Text -> K.Sem r ()
brAddMarkDown = K.addMarkDownWithOptions brMarkDownReaderOptions
  where
    brMarkDownReaderOptions =
      let exts = PA.readerExtensions K.markDownReaderOptions
       in PA.def
            { PA.readerStandalone = True,
              PA.readerExtensions =
                PA.enableExtension PA.Ext_smart
                  . PA.enableExtension PA.Ext_raw_html
                  . PA.enableExtension PA.Ext_escaped_line_breaks
                  $ exts
            }

brAddAnchor :: K.KnitOne r => T.Text -> K.Sem r Text
brAddAnchor t = do
  brAddMarkDown $ "<a name=\"" <> t <> "\"></a>"
  return $ "#" <> t

brLineBreak :: K.KnitOne r => K.Sem r ()
brLineBreak = brAddMarkDown "\\\n"

brAddDates ::
  Bool -> Time.Day -> Time.Day -> M.Map String String -> M.Map String String
brAddDates updated pubDate updateDate tMap =
  let formatTime t = Time.formatTime Time.defaultTimeLocale "%B %e, %Y" t
      pubT = one ("published", formatTime pubDate)
      updT = if updated
             then
               (
                 if updateDate > pubDate
                 then one ("updated", formatTime updateDate)
                 else M.empty
               )
             else M.empty
   in tMap <> pubT <> updT

brGetTextFromFile :: K.KnitEffects r => Path.Path Path.Abs Path.File -> K.Sem r (Maybe Text)
brGetTextFromFile p = do
  isPresent <- K.liftKnit $ SD.doesFileExist $ Path.toFilePath p
  if isPresent
    then Just <$> K.liftKnit (T.readFile $ Path.toFilePath p)
    else return Nothing

brAddToPostFromFile :: K.KnitOne r => (Text -> K.Sem r ()) -> Bool -> Path.Path Path.Abs Path.File -> K.Sem r ()
brAddToPostFromFile toPost errorIfMissing p = do
  tM <- brGetTextFromFile p
  case tM of
    Just t -> toPost t
    Nothing -> do
      case errorIfMissing of
        True -> K.knitError $ "Post input file " <> show p <> " not found!"
        False -> do
          K.logLE K.Warning $ "Post input file " <> show p <> " not found. Inserting placeholder text"
          brAddMarkDown $ "**Post input file " <> show p <> " not found. Please fix/remove**"

brAddToPostFromFileWith :: K.KnitOne r => (Text -> K.Sem r ()) -> Bool -> Path.Path Path.Abs Path.File -> Maybe Text -> K.Sem r ()
brAddToPostFromFileWith toPost errorIfMissing p mRefs = brAddToPostFromFile toPost' errorIfMissing p
  where
    toPost' = maybe toPost (\t -> toPost . (<> "\n" <> t)) mRefs

brAddPostMarkDownFromFileWith :: K.KnitOne r => BRC.PostPaths Path.Abs -> Text -> Maybe Text -> K.Sem r ()
brAddPostMarkDownFromFileWith pp postFileEnd mRefs = do
  postInputPath <- K.knitEither $ BRC.postInputPath pp (postFileEnd <> ".md")
  brAddToPostFromFileWith K.addMarkDown False postInputPath mRefs

brAddSharedMarkDownFromFileWith :: K.KnitOne r => BRC.PostPaths Path.Abs -> Text -> Maybe Text -> K.Sem r ()
brAddSharedMarkDownFromFileWith pp postFileEnd mRefs = do
  postInputPath <- K.knitEither $ BRC.sharedInputPath pp (postFileEnd <> ".md")
  brAddToPostFromFileWith K.addMarkDown False postInputPath mRefs

brAddPostMarkDownFromFile :: K.KnitOne r => BRC.PostPaths Path.Abs -> Text -> K.Sem r ()
brAddPostMarkDownFromFile pp postFileEnd = brAddPostMarkDownFromFileWith pp postFileEnd Nothing

brAddSharedMarkDownFromFile :: K.KnitOne r => BRC.PostPaths Path.Abs -> Text -> K.Sem r ()
brAddSharedMarkDownFromFile pp postFileEnd = brAddSharedMarkDownFromFileWith pp postFileEnd Nothing


brAddNoteMarkDownFromFileWith :: K.KnitOne r
  => BRC.PostPaths Path.Abs -> BRC.NoteName -> Text -> Maybe Text -> K.Sem r ()
brAddNoteMarkDownFromFileWith  pp nn noteFileEnd mRefs = do
  notePath <- K.knitEither $ BRC.noteInputPath pp nn (noteFileEnd <> ".md")
  brAddToPostFromFileWith K.addMarkDown False notePath mRefs

brAddNoteMarkDownFromFile :: K.KnitOne r => BRC.PostPaths Path.Abs -> BRC.NoteName -> Text -> K.Sem r ()
brAddNoteMarkDownFromFile  pp nn noteFileEnd = brAddNoteMarkDownFromFileWith pp nn noteFileEnd Nothing

brAddNoteRSTFromFileWith :: K.KnitOne r
  => BRC.PostPaths Path.Abs -> BRC.NoteName -> Text -> Maybe Text -> K.Sem r ()
brAddNoteRSTFromFileWith pp nn noteFileEnd mRefs = do
  notePath <- K.knitEither $ BRC.noteInputPath pp nn (noteFileEnd <> ".rst")
  brAddToPostFromFileWith K.addRST False notePath mRefs

brAddNoteRSTFromFile :: K.KnitOne r => BRC.PostPaths Path.Abs -> BRC.NoteName -> Text -> K.Sem r ()
brAddNoteRSTFromFile  pp nn noteFileEnd = brAddNoteRSTFromFileWith pp nn noteFileEnd Nothing

brDatesFromPostInfo :: K.KnitEffects r => BRC.PostInfo -> K.Sem r (M.Map String String)
brDatesFromPostInfo (BRC.PostInfo postStage  (BRC.PubTimes ppt mUpt)) = do
  tz <- liftIO $ Time.getCurrentTimeZone
  let formatPTime t = Time.formatTime Time.defaultTimeLocale "%B %e, %Y" t
      formatUPTime t = formatPTime t <> Time.formatTime Time.defaultTimeLocale " (%H:%M:%S)" t
--      getCurrentDay :: K.KnitEffects r => K.Sem r Time.Day
--      getCurrentDay = K.getCurrentTime
      dayFromPT = \case
        BRC.Unpublished -> case postStage of
          BRC.OnlinePublished -> formatPTime . Time.utcToLocalTime tz <$> K.getCurrentTime
          _ -> formatUPTime . Time.utcToLocalTime tz <$> K.getCurrentTime
        BRC.Published d -> return $ formatPTime d
  pt <- dayFromPT ppt
  let mp = one ("published", pt)
  case mUpt of
    Nothing -> return mp
    (Just upt) -> do
      ut <- dayFromPT upt
      return $ mp <> one ("updated", ut)


brNewPost :: K.KnitMany r
          => BRC.PostPaths Path.Abs
          -> BRC.PostInfo
          -> Text
          -> K.Sem (K.ToPandoc ': r) ()
          -> K.Sem r ()
brNewPost pp pi' pageTitle content = do
  dates <- brDatesFromPostInfo pi'
  let postPath = BRC.postPath pp pi'
      pageConfig = dates <> one ("pagetitle", toString pageTitle)
  K.newPandoc (K.PandocInfo (toText $ Path.toFilePath postPath) pageConfig) $ do
    content
    brAddMarkDown BRC.brReadMore

brNewNote :: K.KnitMany r
          => BRC.PostPaths Path.Abs
          -> BRC.PostInfo
          -> BRC.NoteName
          -> Text
          -> K.Sem (K.ToPandoc ': r) ()
          -> K.Sem r (Maybe Text)
brNewNote pp pi' nn pageTitle content = do
  dates <- brDatesFromPostInfo pi'
  notePath <- K.knitEither $ BRC.notePath pp pi' nn
  let pageConfig = dates <> one ("pagetitle", toString pageTitle)
  K.newPandoc (K.PandocInfo (toText $ Path.toFilePath notePath) pageConfig) content
  case nn of
    BRC.Unused _ -> return Nothing
    BRC.Used _ -> Just <$> (K.knitEither $ BRC.noteUrl pp pi' nn)

-- returns URL
brAddJSON :: K.KnitEffects r
          => BRC.PostPaths Path.Abs
          -> BRC.PostInfo
          -> Text
          -> A.Value
          -> K.Sem r Text
brAddJSON pp postInfo jsonName jsonVal = do
  let destDir = BRC.dataDir pp postInfo
      jsonFileName = jsonName <> ".json"
  K.liftKnit $ SD.createDirectoryIfMissing True (Path.toFilePath destDir)
  jsonPath' <- K.knitEither $ BRC.jsonPath pp postInfo jsonFileName
  K.liftKnit $ A.encodeFile (Path.toFilePath jsonPath') jsonVal
  K.knitEither $ BRC.dataURL pp postInfo jsonFileName

-- Copy data from an absolute path into data directory so it's available for, e.g.,
-- hvega
data WhenDestExists = ReplaceExisting | ExistsIsError | LeaveExisting deriving stock (Eq)
brCopyDataForPost ::  K.KnitEffects r
                => BRC.PostPaths Path.Abs
                -> BRC.PostInfo
                -> WhenDestExists
                -> Path.Path Path.Abs Path.File
                -> Maybe Text
                -> K.Sem r Text
brCopyDataForPost pp postInfo whenExists srcFile destFileNameM = do
  let destDir = BRC.dataDir pp postInfo
  srcExists <- K.liftKnit $ SD.doesFileExist (Path.toFilePath srcFile)
  destFileName <- K.liftKnit @IO -- FIXME: this is a hack to handle the possible thrown error
                  $ maybe (pure $ Path.filename srcFile) (Path.parseRelFile)
                  $ fmap toString destFileNameM
  let tgtFile = destDir Path.</> Path.filename destFileName
      doCopy = K.liftKnit $ SD.copyFile (Path.toFilePath srcFile) (Path.toFilePath tgtFile)
      createDirIfNec = K.liftKnit $ SD.createDirectoryIfMissing True (Path.toFilePath destDir)
      url = K.knitEither $ BRC.dataURL pp postInfo (toText $ Path.toFilePath $ Path.filename tgtFile)
  when (not srcExists) $ K.knitError $ "brCopyDataForPost: source file (" <> show srcFile <> ") does not exist."
  destExists <- K.liftKnit $ SD.doesFileExist (Path.toFilePath tgtFile)
  when (destExists && whenExists == ExistsIsError)
    $ K.knitError
    $ "brCopyDataForPost: file (" <> show (Path.filename srcFile) <> ") exists in destination and whenExisting==ExistsIsError."
  if destExists && whenExists == LeaveExisting
    then K.logLE K.Info $ "brCopyDataForPost: target (" <> show tgtFile <> ") exists. Not copying since whenExists==LeaveExisting"
    else (do
             if destExists
               then K.logLE K.Info $ "brCopyDataForPost: overwriting " <> show tgtFile <> " with " <> show srcFile <> ", since whenExists==ReplaceExisting"
               else K.logLE K.Info $ "brCopyDataForPost: copying " <> show srcFile <> " to " <> show tgtFile
             createDirIfNec
             doCopy
         )
  url
--  let noteParent = Path.parent notePath
--  K.logLE K.Info $ "If necessary, creating note path \"" <> toText (Path.toFilePath noteParent)


logFrame ::
  (K.KnitEffects r, Foldable f, Show (F.Record rs)) =>
  f (F.Record rs) ->
  K.Sem r ()
logFrame = logFrame' K.Info
{-# INLINEABLE logFrame #-}

logFrame' ::
  (K.KnitEffects r, Foldable f, Show (F.Record rs))
  => K.LogSeverity
  -> f (F.Record rs)
  ->K.Sem r ()
logFrame' ll fr =
  K.logLE ll $ "\n" <> (T.intercalate "\n" . fmap show $ FL.fold FL.list fr)
{-# INLINEABLE logFrame' #-}

logCachedFrame ::
  (K.KnitEffects r, Foldable f, Show (F.Record rs)) =>
  K.ActionWithCacheTime r (f (F.Record rs)) ->
  K.Sem r ()
logCachedFrame fr_C = do
  fr <- K.ignoreCacheTime fr_C
  K.logLE K.Info $ "\n" <> (T.intercalate "\n" . fmap show $ FL.fold FL.list fr)

retrieveOrMakeD ::
  ( K.KnitEffects r,
    CacheEffects r,
    SerializerC b
  ) =>
  T.Text ->
  K.ActionWithCacheTime r a ->
  (a -> K.Sem r b) ->
  K.Sem r (K.ActionWithCacheTime r b)
retrieveOrMakeD = K.retrieveOrMake @SerializerC @CacheData @T.Text


retrieveOrMakeFrame ::
  ( K.KnitEffects r,
    CacheEffects r,
    RecSerializerC rs,
    V.RMap rs,
    FI.RecVec rs
  ) =>
  T.Text ->
  K.ActionWithCacheTime r b ->
  (b -> K.Sem r (F.FrameRec rs)) ->
  K.Sem r (K.ActionWithCacheTime r (F.FrameRec rs)) -- inner action does deserialization. But we may not need to, so we defer
retrieveOrMakeFrame key cachedDeps action =
  K.wrapPrefix ("BlueRipple.retrieveOrMakeFrame (key=" <> key <> ")") $
    K.retrieveOrMakeTransformed @SerializerC @CacheData fromFrame toFrame key cachedDeps action

retrieveOrMakeFrameAnd ::
  ( K.KnitEffects r,
    CacheEffects r,
    RecSerializerC rs,
    V.RMap rs,
    FI.RecVec rs,
    SerializerC c
  ) =>
  T.Text ->
  K.ActionWithCacheTime r b ->
  (b -> K.Sem r (F.FrameRec rs, c)) ->
  K.Sem r (K.ActionWithCacheTime r (F.FrameRec rs, c)) -- inner action does deserialization. But we may not need to, so we defer
retrieveOrMakeFrameAnd key cachedDeps action =
  K.wrapPrefix ("BlueRipple.retrieveOrMakeFrameAnd (key=" <> key <> ")") $ do
    let toFirst = first fromFrame
        fromFirst = first toFrame
    K.retrieveOrMakeTransformed  @SerializerC @CacheData toFirst fromFirst key cachedDeps action

retrieveOrMake2Frames ::
  ( K.KnitEffects r,
    CacheEffects r,
    RecSerializerC rs1,
    V.RMap rs1,
    FI.RecVec rs1,
    RecSerializerC rs2,
    V.RMap rs2,
    FI.RecVec rs2
  ) =>
  T.Text ->
  K.ActionWithCacheTime r b ->
  (b -> K.Sem r (F.FrameRec rs1, F.FrameRec rs2)) ->
  K.Sem r (K.ActionWithCacheTime r (F.FrameRec rs1, F.FrameRec rs2)) -- inner action does deserialization. But we may not need to, so we defer
retrieveOrMake2Frames key cachedDeps action =
  let from (f1, f2) = (toFrame f1, toFrame f2)
      to (s1, s2) = (fromFrame s1, fromFrame s2)
   in K.wrapPrefix ("BlueRipple.retrieveOrMake2Frames (key=" <> key <> ")") $
        K.retrieveOrMakeTransformed  @SerializerC @CacheData to from key cachedDeps action

retrieveOrMake3Frames ::
  ( K.KnitEffects r,
    CacheEffects r,
    RecSerializerC rs1,
    V.RMap rs1,
    FI.RecVec rs1,
    RecSerializerC rs2,
    V.RMap rs2,
    FI.RecVec rs2,
    RecSerializerC rs3,
    V.RMap rs3,
    FI.RecVec rs3
  ) =>
  T.Text ->
  K.ActionWithCacheTime r b ->
  (b -> K.Sem r (F.FrameRec rs1, F.FrameRec rs2, F.FrameRec rs3)) ->
  K.Sem r (K.ActionWithCacheTime r (F.FrameRec rs1, F.FrameRec rs2, F.FrameRec rs3)) -- inner action does deserialization. But we may not need to, so we defer
retrieveOrMake3Frames key cachedDeps action =
  let from (f1, f2, f3) = (toFrame f1, toFrame f2, toFrame f3)
      to (s1, s2, s3) = (fromFrame s1, fromFrame s2, fromFrame s3)
   in K.wrapPrefix ("BlueRipple.retrieveOrMake3Frames (key=" <> key <> ")") $
        K.retrieveOrMakeTransformed  @SerializerC @CacheData to from key cachedDeps action

retrieveOrMakeRecList ::
  ( K.KnitEffects r
  , CacheEffects r
  , RecSerializerC rs
  , V.RMap rs
  ) =>
  T.Text ->
  K.ActionWithCacheTime r b ->
  (b -> K.Sem r [F.Record rs]) ->
  K.Sem r (K.ActionWithCacheTime r [F.Record rs])
retrieveOrMakeRecList key cachedDeps action =
  K.wrapPrefix ("BlueRipple.retrieveOrMakeRecList (key=" <> key <> ")") $
    K.retrieveOrMakeTransformed  @SerializerC @CacheData (fmap FS.toS) (fmap FS.fromS) key cachedDeps action



type SerializerC = Flat.Flat
type RecSerializerC rs = FS.RecFlat rs

type CacheData = BS.ByteString
type CacheEffects r = K.CacheEffects SerializerC CacheData T.Text r

fromFrame :: F.Frame a -> FS.SFrame a
fromFrame = FS.SFrame
{-# INLINABLE fromFrame #-}

toFrame :: FS.SFrame a -> F.Frame a
toFrame = FS.unSFrame
{-# INLINABLE toFrame #-}

flatSerializeDict :: KS.SerializeDict Flat.Flat BS.ByteString
flatSerializeDict =
  KS.SerializeDict
  Flat.flat
  (first (KS.SerializationError . show) . Flat.unflat)
  id
  id
  (fromIntegral . BS.length)
{-# INLINEABLE flatSerializeDict #-}


sampleFrame :: (FI.RecVec rs, Prim.PrimMonad m) => Word32 -> Int -> F.FrameRec rs -> m (F.FrameRec rs)
sampleFrame seed n rows = do
  gen <- MWC.initialize (Vector.singleton seed)
  F.toFrame <$> sample (FL.fold FL.list rows) n gen

sample :: Prim.PrimMonad m => [a] -> Int -> MWC.Gen (Prim.PrimState m) -> m [a]
sample ys size = go 0 (l - 1) (Seq.fromList ys) where
    l = length ys
    go !n !i xs g | n >= size = return $! (toList . Seq.drop (l - size)) xs
                  | otherwise = do
                      j <- MWC.uniformR (0, i) g
                      let toI  = xs `Seq.index` j
                          toJ  = xs `Seq.index` i
                          next = (Seq.update i toI . Seq.update j toJ) xs
                      go (n + 1) (i - 1) next g
{-# INLINEABLE sample #-}

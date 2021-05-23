{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
--{-# OPTIONS_GHC -fplugin=Polysemy.Plugin #-}

module BlueRipple.Utilities.KnitUtils where

import qualified BlueRipple.Configuration as BRC

import qualified Control.Arrow as Arrow
import qualified Control.Exception as EX
import qualified Control.Foldl as FL
import qualified Control.Monad.Except as X
import qualified Control.Monad.Primitive as Prim
import qualified Data.ByteString as BS
import qualified Data.IntSet as IntSet
import qualified Data.Map as M
import qualified Data.Sequence as Seq
import qualified Data.Serialize as S
import qualified Data.Text as T
import qualified Data.Text.IO as T
import qualified Data.Text.Lazy as TL
import qualified Data.Time.Calendar as Time
import qualified Data.Time.Clock as Time
import qualified Data.Time.Format as Time
import qualified Data.Vector as Vector
import qualified Data.Vinyl as V
import qualified Flat
import qualified Flat.Encoder as Flat
import qualified Flat.Encoder.Types as Flat
import qualified Flat.Filler as Flat
import qualified Frames as F
import qualified Frames.InCore as FI
import qualified Frames.Serialize as FS
import qualified Frames.Streamly.InCore as FStreamly
import qualified Knit.Effect.AtomicCache as KC
import qualified Knit.Effect.Serialize as KS
import qualified Knit.Report as K
import qualified Knit.Report.Cache as KC
import qualified Knit.Report.Input.MarkDown.PandocMarkDown as K
import qualified Knit.Utilities.Streamly as KStreamly
import qualified Path
import qualified Path.IO as Path
import qualified Polysemy as P
import Polysemy.Error (Error)
import Relude.Extra as Relude
import qualified Streamly
import qualified Streamly.External.ByteString  as Streamly.ByteString
import qualified Streamly.Memory.Array         as Streamly.Array
import qualified Streamly.Prelude as Streamly
import qualified System.Directory as SD
import qualified System.Directory as System
import qualified System.IO.Error as SE
import qualified System.Random.MWC as MWC
import qualified Text.Blaze.Colonnade as BC
import qualified Text.Blaze.Html.Renderer.Text as B
import qualified Text.Blaze.Html5 as BH
import qualified Text.Blaze.Html5.Attributes as BHA
import qualified Text.Pandoc.Options as PA
--import qualified Streamly.Internal.Memory.ArrayStream as Streamly.ByteString

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

brAddPostMarkDownFromFileWith :: K.KnitOne r => BRC.PostPaths Path.Abs -> Text -> Maybe Text -> K.Sem r ()
brAddPostMarkDownFromFileWith pp postFileEnd mRefs = do
  postInputPath <- K.knitEither $ BRC.postInputPath pp postFileEnd
  fText <- K.liftKnit (T.readFile $ Path.toFilePath postInputPath)
  K.addMarkDown $ case mRefs of
                    Nothing -> fText
                    Just refs -> fText <> "\n" <> refs


brAddPostMarkDownFromFile :: K.KnitOne r => BRC.PostPaths Path.Abs -> Text -> K.Sem r ()
brAddPostMarkDownFromFile pp postFileEnd = brAddPostMarkDownFromFileWith pp postFileEnd Nothing

brAddNoteMarkDownFromFileWith :: K.KnitOne r => BRC.PostPaths Path.Abs -> BRC.NoteName -> Text -> Maybe Text -> K.Sem r ()
brAddNoteMarkDownFromFileWith  pp nn noteFileEnd mRefs = do
  notePath <- K.knitEither $ BRC.noteInputPath pp nn (noteFileEnd <> ".md")
  fText <- K.liftKnit (T.readFile $ Path.toFilePath notePath)
  K.addMarkDown $ case mRefs of
                    Nothing -> fText
                    Just refs -> fText <> "\n" <> refs

brAddNoteMarkDownFromFile :: K.KnitOne r => BRC.PostPaths Path.Abs -> BRC.NoteName -> Text -> K.Sem r ()
brAddNoteMarkDownFromFile  pp nn noteFileEnd = brAddNoteMarkDownFromFileWith pp nn noteFileEnd Nothing


brAddNoteRSTFromFileWith :: K.KnitOne r => BRC.PostPaths Path.Abs -> BRC.NoteName -> Text -> Maybe Text -> K.Sem r ()
brAddNoteRSTFromFileWith pp nn noteFileEnd mRefs = do
  notePath <- K.knitEither $ BRC.noteInputPath pp nn (noteFileEnd <> ".rst")
  fText <- K.liftKnit (T.readFile $ Path.toFilePath notePath)
  K.addRST $ case mRefs of
               Nothing -> fText
               Just refs -> fText <> "\n" <> refs

brAddNoteRSTFromFile :: K.KnitOne r => BRC.PostPaths Path.Abs -> BRC.NoteName -> Text -> K.Sem r ()
brAddNoteRSTFromFile  pp nn noteFileEnd = brAddNoteRSTFromFileWith pp nn noteFileEnd Nothing


brDatesFromPostInfo :: K.KnitEffects r => BRC.PostInfo -> K.Sem r (M.Map String String)
brDatesFromPostInfo (BRC.PostInfo _ (BRC.PubTimes ppt mUpt)) = do
  let formatTime t = Time.formatTime Time.defaultTimeLocale "%B %e, %Y" t
      getCurrentDay :: K.KnitEffects r => K.Sem r Time.Day
      getCurrentDay = (\(Time.UTCTime d _) -> d) <$> K.getCurrentTime
      dayFromPT = \case
        BRC.Unpublished -> formatTime <$> getCurrentDay
        BRC.Published d -> return $ formatTime d
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
brNewPost pp pi pageTitle content = do
  dates <- brDatesFromPostInfo pi
  let postPath = BRC.postPath pp pi
      pageConfig = dates <> one ("pagetitle", toString pageTitle)
  K.newPandoc (K.PandocInfo (toText $ Path.toFilePath postPath) pageConfig) content

brNewNote :: K.KnitMany r
          => BRC.PostPaths Path.Abs
          -> BRC.PostInfo
          -> BRC.NoteName
          -> Text
          -> K.Sem (K.ToPandoc ': r) ()
          -> K.Sem r (Maybe Text)
brNewNote pp pi nn pageTitle content = do
  dates <- brDatesFromPostInfo pi
  notePath <- K.knitEither $ BRC.notePath pp pi nn
  let pageConfig = dates <> one ("pagetitle", toString pageTitle)
  K.newPandoc (K.PandocInfo (toText $ Path.toFilePath notePath) pageConfig) content
  case nn of
    BRC.Unused _ -> return Nothing
    BRC.Used _ -> Just <$> (K.knitEither $ BRC.noteUrl pp pi nn)

--  let noteParent = Path.parent notePath
--  K.logLE K.Info $ "If necessary, creating note path \"" <> toText (Path.toFilePath noteParent)


logFrame ::
  (K.KnitEffects r, Foldable f, Show (F.Record rs)) =>
  f (F.Record rs) ->
  K.Sem r ()
logFrame =
  K.logLE K.Info . T.intercalate "\n" . fmap show . FL.fold FL.list

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
  ( K.KnitEffects r,
    CacheEffects r,
    RecSerializerC rs,
    V.RMap rs,
    FI.RecVec rs
  ) =>
  T.Text ->
  K.ActionWithCacheTime r b ->
  (b -> K.Sem r [F.Record rs]) ->
  K.Sem r (K.ActionWithCacheTime r [F.Record rs])
retrieveOrMakeRecList key cachedDeps action =
  K.wrapPrefix ("BlueRipple.retrieveOrMakeRecList (key=" <> key <> ")") $
    K.retrieveOrMakeTransformed  @SerializerC @CacheData (fmap FS.toS) (fmap FS.fromS) key cachedDeps action

clearIfPresentD :: (K.KnitEffects r, CacheEffects r) => T.Text -> K.Sem r ()
clearIfPresentD = K.clearIfPresent @T.Text @CacheData

type SerializerC = Flat.Flat
type RecSerializerC rs = FS.RecFlat rs

type CacheData = BS.ByteString
type CacheEffects r = K.CacheEffects SerializerC CacheData T.Text r

fromFrame = FS.SFrame
toFrame = FS.unSFrame

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

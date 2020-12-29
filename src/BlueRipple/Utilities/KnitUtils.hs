{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# OPTIONS_GHC -fplugin=Polysemy.Plugin #-}

module BlueRipple.Utilities.KnitUtils where

import qualified Control.Arrow as Arrow
import qualified Control.Exception as EX
import qualified Control.Foldl as FL
import qualified Control.Monad.Except as X
import qualified Data.Map as M
import qualified Data.Serialize as S
import qualified Data.Text as T
import qualified Data.Text.Lazy as TL
import qualified Data.Time.Calendar as Time
import qualified Data.Time.Clock as Time
import qualified Data.Time.Format as Time
import qualified Data.Vinyl as V
import qualified Frames as F
import qualified Frames.InCore as FI
import qualified Frames.Serialize as FS
import qualified Frames.Streamly.InCore as FStreamly
import qualified Knit.Effect.AtomicCache as KC
import qualified Knit.Report as K
import qualified Knit.Report.Cache as KC
import qualified Knit.Report.Input.MarkDown.PandocMarkDown as K
import qualified Knit.Utilities.Streamly as KStreamly
import qualified Polysemy as P
import Polysemy.Error (Error)
import Relude.Extra as Relude
import qualified Streamly
import qualified Streamly.Prelude as Streamly
import qualified System.Directory as SD
import qualified System.Directory as System
import qualified System.IO.Error as SE
import qualified Text.Blaze.Colonnade as BC
import qualified Text.Blaze.Html.Renderer.Text as B
import qualified Text.Blaze.Html5 as BH
import qualified Text.Blaze.Html5.Attributes as BHA
import qualified Text.Pandoc.Options as PA

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

logFrame ::
  (K.KnitEffects r, Foldable f, Show (F.Record rs)) =>
  f (F.Record rs) ->
  K.Sem r ()
logFrame =
  K.logLE K.Info . T.intercalate "\n" . fmap show . FL.fold FL.list

retrieveOrMakeD ::
  ( K.KnitEffects r,
    K.CacheEffectsD r,
    S.Serialize b
  ) =>
  T.Text ->
  K.ActionWithCacheTime r a ->
  (a -> K.Sem r b) ->
  K.Sem r (K.ActionWithCacheTime r b)
retrieveOrMakeD = K.retrieveOrMake @K.DefaultSerializer @K.DefaultCacheData @T.Text

retrieveOrMakeFrame ::
  ( K.KnitEffects r,
    K.CacheEffectsD r,
    FS.RecSerialize rs,
    V.RMap rs,
    FI.RecVec rs
  ) =>
  T.Text ->
  K.ActionWithCacheTime r b ->
  (b -> K.Sem r (F.FrameRec rs)) ->
  K.Sem r (K.ActionWithCacheTime r (F.FrameRec rs)) -- inner action does deserialization. But we may not need to, so we defer
retrieveOrMakeFrame key cachedDeps action =
  K.wrapPrefix ("BlueRipple.retrieveOrMakeFrame (key=" <> key <> ")") $
    K.retrieveOrMakeTransformed FS.SFrame FS.unSFrame key cachedDeps action

retrieveOrMakeFrameAnd ::
  ( K.KnitEffects r,
    K.CacheEffectsD r,
    FS.RecSerialize rs,
    V.RMap rs,
    FI.RecVec rs,
    S.Serialize c
  ) =>
  T.Text ->
  K.ActionWithCacheTime r b ->
  (b -> K.Sem r (F.FrameRec rs, c)) ->
  K.Sem r (K.ActionWithCacheTime r (F.FrameRec rs, c)) -- inner action does deserialization. But we may not need to, so we defer
retrieveOrMakeFrameAnd key cachedDeps action =
  K.wrapPrefix ("BlueRipple.retrieveOrMakeFrameAnd (key=" <> key <> ")") $ do
    let toFirst = Arrow.first FS.SFrame
        fromFirst = Arrow.first FS.unSFrame
    K.retrieveOrMakeTransformed toFirst fromFirst key cachedDeps action


retrieveOrMakeFrameS ::
  ( K.KnitEffects r,
    K.CacheEffectsD r,
    FS.RecSerialize rs,
    V.RMap rs,
    FI.RecVec rs
  ) =>
  T.Text ->
  K.ActionWithCacheTime r b ->
  (b -> Streamly.SerialT KStreamly.StreamlyM (F.Record rs)) ->
  K.Sem r (K.ActionWithCacheTime r (F.FrameRec rs)) -- inner action does deserialization. But we may not need to, so we defer
retrieveOrMakeFrameS key cachedDeps action =
  K.wrapPrefix ("BlueRipple.retrieveOrMakeFrameS (key=" <> key <> ")") $
    do
      fmap (K.streamToAction FStreamly.inCoreAoS)
      $ K.retrieveOrMakeTransformedStream FS.toS FS.fromS key cachedDeps action


retrieveOrMake2Frames ::
  ( K.KnitEffects r,
    K.CacheEffectsD r,
    FS.RecSerialize rs1,
    V.RMap rs1,
    FI.RecVec rs1,
    FS.RecSerialize rs2,
    V.RMap rs2,
    FI.RecVec rs2
  ) =>
  T.Text ->
  K.ActionWithCacheTime r b ->
  (b -> K.Sem r (F.FrameRec rs1, F.FrameRec rs2)) ->
  K.Sem r (K.ActionWithCacheTime r (F.FrameRec rs1, F.FrameRec rs2)) -- inner action does deserialization. But we may not need to, so we defer
retrieveOrMake2Frames key cachedDeps action =
  let from (f1, f2) = (FS.unSFrame f1, FS.unSFrame f2)
      to (s1, s2) = (FS.SFrame s1, FS.SFrame s2)
   in K.wrapPrefix ("BlueRipple.retrieveOrMake2Frames (key=" <> key <> ")") $
        K.retrieveOrMakeTransformed to from key cachedDeps action

retrieveOrMake3Frames ::
  ( K.KnitEffects r,
    K.CacheEffectsD r,
    FS.RecSerialize rs1,
    V.RMap rs1,
    FI.RecVec rs1,
    FS.RecSerialize rs2,
    V.RMap rs2,
    FI.RecVec rs2,
    FS.RecSerialize rs3,
    V.RMap rs3,
    FI.RecVec rs3
  ) =>
  T.Text ->
  K.ActionWithCacheTime r b ->
  (b -> K.Sem r (F.FrameRec rs1, F.FrameRec rs2, F.FrameRec rs3)) ->
  K.Sem r (K.ActionWithCacheTime r (F.FrameRec rs1, F.FrameRec rs2, F.FrameRec rs3)) -- inner action does deserialization. But we may not need to, so we defer
retrieveOrMake3Frames key cachedDeps action =
  let from (f1, f2, f3) = (FS.unSFrame f1, FS.unSFrame f2, FS.unSFrame f3)
      to (s1, s2, s3) = (FS.SFrame s1, FS.SFrame s2, FS.SFrame s3)
   in K.wrapPrefix ("BlueRipple.retrieveOrMake3Frames (key=" <> key <> ")") $
        K.retrieveOrMakeTransformed to from key cachedDeps action

retrieveOrMakeRecList ::
  ( K.KnitEffects r,
    K.CacheEffectsD r,
    FS.RecSerialize rs,
    V.RMap rs,
    FI.RecVec rs
  ) =>
  T.Text ->
  K.ActionWithCacheTime r b ->
  (b -> K.Sem r [F.Record rs]) ->
  K.Sem r (K.ActionWithCacheTime r [F.Record rs])
retrieveOrMakeRecList key cachedDeps action =
  K.wrapPrefix ("BlueRipple.retrieveOrMakeRecList (key=" <> key <> ")") $
    K.retrieveOrMakeTransformed (fmap FS.toS) (fmap FS.fromS) key cachedDeps action

clearIfPresentD :: (K.KnitEffects r, K.CacheEffectsD r) => T.Text -> K.Sem r ()
clearIfPresentD = K.clearIfPresent @T.Text @_

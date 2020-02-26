{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE GADTs                     #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE QuasiQuotes               #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE TypeApplications          #-}
{-# LANGUAGE TypeOperators             #-}
module BlueRipple.Utilities.KnitUtils where

import qualified Knit.Report                   as K
import qualified Knit.Report.Input.MarkDown.PandocMarkDown
                                               as K
import qualified Knit.Report.Cache             as KC

import qualified Control.Monad.Except          as X
import qualified Control.Foldl                 as FL
import qualified Data.Map                      as M
import qualified Data.Text                     as T
import qualified Data.Text.Lazy                as TL
import qualified System.Directory              as SD
import qualified Data.Time.Calendar            as Time
import qualified Data.Time.Clock               as Time
import qualified Data.Time.Format              as Time
import qualified Data.Vinyl                    as V

import           Polysemy.Error                 ( Error )

import qualified Text.Pandoc.Options           as PA

import qualified Text.Blaze.Colonnade          as BC
import qualified Text.Blaze.Html5              as BH
import qualified Text.Blaze.Html.Renderer.Text as B
import qualified Text.Blaze.Html5.Attributes   as BHA

import qualified Frames.Serialize              as FS
import qualified Frames                        as F
import qualified Frames.InCore                 as FI

knitX
  :: forall r a
   . K.Member (Error K.PandocError) r
  => X.ExceptT T.Text (K.Sem r) a
  -> K.Sem r a
knitX ma = X.runExceptT ma >>= (knitEither @r)

knitMaybe
  :: forall r a
   . K.Member (Error K.PandocError) r
  => T.Text
  -> Maybe a
  -> K.Sem r a
knitMaybe msg ma = maybe (K.knitError msg) return ma

knitEither
  :: forall r a
   . K.Member (Error K.PandocError) r
  => Either T.Text a
  -> K.Sem r a
knitEither = either K.knitError return

copyAsset :: K.KnitOne r => T.Text -> T.Text -> K.Sem r ()
copyAsset sourcePath destDir = do
  sourceExists <- K.liftKnit $ SD.doesFileExist (T.unpack sourcePath)
  case sourceExists of
    False ->
      K.knitError $ "\"" <> sourcePath <> "\" doesn't exist (copyAsset)."
    True -> do
      K.logLE K.Info
        $  "If necessary, creating \""
        <> destDir
        <> "\" and copying \""
        <> sourcePath
        <> "\" there"
      K.liftKnit $ do
        let (_, fName) = T.breakOnEnd "/" sourcePath
        SD.createDirectoryIfMissing True (T.unpack destDir)
        SD.copyFile (T.unpack sourcePath) (T.unpack $ destDir <> "/" <> fName)

brWriterOptionsF :: PA.WriterOptions -> PA.WriterOptions
brWriterOptionsF o =
  let exts = PA.writerExtensions o
  in  o
        { PA.writerExtensions  = PA.enableExtension PA.Ext_header_attributes
                                   $ PA.enableExtension PA.Ext_smart exts
        , PA.writerSectionDivs = True
        }

brAddMarkDown :: K.KnitOne r => T.Text -> K.Sem r ()
brAddMarkDown = K.addMarkDownWithOptions brMarkDownReaderOptions
 where
  brMarkDownReaderOptions =
    let exts = PA.readerExtensions K.markDownReaderOptions
    in  PA.def
          { PA.readerStandalone = True
          , PA.readerExtensions = PA.enableExtension PA.Ext_smart
                                  . PA.enableExtension PA.Ext_raw_html
                                  $ exts
          }

brAddDates
  :: Bool -> Time.Day -> Time.Day -> M.Map String String -> M.Map String String
brAddDates updated pubDate updateDate tMap =
  let formatTime t = Time.formatTime Time.defaultTimeLocale "%B %e, %Y" t
      pubT = M.singleton "published" $ formatTime pubDate
      updT = case updated of
        True -> if (updateDate > pubDate)
          then M.singleton "updated" (formatTime updateDate)
          else M.empty
        False -> M.empty
  in  tMap <> pubT <> updT


logFrame
  :: (K.KnitEffects r, Foldable f, Show (F.Record rs))
  => f (F.Record rs)
  -> K.Sem r ()
logFrame =
  K.logLE K.Info . T.intercalate "\n" . fmap (T.pack . show) . FL.fold FL.list

retrieveOrMakeFrame :: (K.KnitEffects r
                       , FS.RecSerialize rs
                       , V.RMap rs
                       , FI.RecVec rs
                       ) => T.Text -> K.Sem r (F.FrameRec rs) -> K.Sem r (F.FrameRec rs)
retrieveOrMakeFrame key action =  K.retrieveOrMakeTransformed (fmap FS.toS . FL.fold FL.list) (F.toFrame . fmap FS.fromS) key action

retrieveOrMakeRecList :: (K.KnitEffects r
                       , FS.RecSerialize rs
                       , V.RMap rs
                       , FI.RecVec rs
                       ) => T.Text -> K.Sem r[F.Record rs] -> K.Sem r [F.Record rs]
retrieveOrMakeRecList key action =  K.retrieveOrMakeTransformed (fmap FS.toS) (fmap FS.fromS) key action

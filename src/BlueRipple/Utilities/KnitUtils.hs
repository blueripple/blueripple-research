{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE GADTs                     #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE QuasiQuotes               #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE TypeApplications          #-}
{-# LANGUAGE TypeOperators             #-}
module BlueRipple.Utilities.KnitUtils where

import qualified Knit.Report                   as K
import qualified Control.Monad.Except          as X
import qualified Data.Text                     as T
import qualified System.Directory              as SD

import           Polysemy.Error                 ( Error )

import           Data.String.Here               ( here )

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


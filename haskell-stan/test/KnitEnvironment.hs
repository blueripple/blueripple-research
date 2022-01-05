{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
module KnitEnvironment where

import qualified Knit.Report as K
import qualified Knit.Effect.AtomicCache as KA
import qualified Knit.Effect.Serialize as KS
import qualified Knit.Report.Output as KO
import qualified Frames.Serialize as FS
import qualified Flat
import qualified Data.ByteString as BS

type SerializerC = Flat.Flat
type RecSerializerC rs = FS.RecFlat rs

type CacheData = BS.ByteString
type CacheEffects r = K.CacheEffects SerializerC CacheData Text r

knitToIO c x = K.knitHtml c x >>= either err knitDoc where
  knitDoc = KO.writeAndMakePath "haskell-stan/test/test.html" toStrict
  err = putTextLn . ("Knit Error: " <>) . show


defaultConfig :: K.KnitConfig SerializerC CacheData Text
defaultConfig =
  let cacheDir :: Text = "haskell-stan/test/.flat-kh-cache/"
  in  (K.defaultKnitConfig $ Just "haskell-stan-test")
      { K.outerLogPrefix = Just "haskell-stan-test"
      , K.logIf = K.logDiagnostic
      , K.serializeDict = flatSerializeDict
      , K.persistCache = KA.persistStrictByteString (\t -> toString (cacheDir <> t))
      }

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

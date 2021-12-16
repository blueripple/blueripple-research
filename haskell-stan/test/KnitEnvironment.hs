{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
module KnitEnvironment where

import qualified Knit.Report as K
import qualified Knit.Report.EffectStack as K
import qualified Knit.Effect.AtomicCache as KA
import qualified Knit.Effect.Serialize as KS
import qualified Frames.Serialize as FS
import qualified Flat
import qualified Data.ByteString as BS

type SerializerC = Flat.Flat
type RecSerializerC rs = FS.RecFlat rs

type CacheData = BS.ByteString
type CacheEffects r = K.CacheEffects SerializerC CacheData Text r


defaultConfig :: K.KnitConfig SerializerC CacheData Text
defaultConfig =
  let cacheDir =
  K.defaultKnitConfig (Just "haskell-stan-test")
                {K.logIf = K.logDiagnostic
                , K.serializeDict = flatSerializeDict
                , persistCache = KA.persistStrictByteString (\t -> toString (cacheDir))



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

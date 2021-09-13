{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE DeriveAnyClass    #-}
{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE GADTs             #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes       #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell   #-}
{-# LANGUAGE TypeApplications  #-}
{-# LANGUAGE TypeFamilies      #-}
{-# LANGUAGE TypeOperators     #-}
module BlueRipple.Data.ModelingTypes where

--import qualified BlueRipple.Keyed as K
import qualified Control.Foldl                 as FL
import Data.Hashable (Hashable)
import qualified Data.Binary as B
import qualified Data.Ix as Ix
import qualified Data.Text                     as T
import qualified Data.Serialize                as S
import qualified Flat
import qualified Frames as F
import qualified Frames.InCore                 as FI
import qualified Frames.Transform              as FT
import qualified Data.Vinyl.Derived                    as V
import qualified Data.Vinyl.TypeLevel                    as V
import qualified Data.Vector           as Vec
import qualified Data.Vector.Unboxed           as UVec
import           Data.Vector.Unboxed.Deriving   (derivingUnbox)
import           Data.Word                      (Word8)
import           GHC.Generics                   ( Generic )
import           Data.Discrimination            ( Grouping )

-- Serialize for caching
-- Binary for caching
-- Flat for caching
-- FI.VectorFor for frames
-- Grouping for leftJoin
-- FiniteSet for composition of aggregations


data ConfidenceInterval = ConfidenceInterval
                          { ciLower :: !Double
                          , ciMid :: !Double
                          , ciUpper :: !Double
                          } deriving (Eq, Ord, Show, Generic)
instance S.Serialize ConfidenceInterval
instance B.Binary ConfidenceInterval
instance Flat.Flat ConfidenceInterval
type instance FI.VectorFor ConfidenceInterval = UVec.Vector
derivingUnbox "ConfidenceInterval"
  [t|ConfidenceInterval->(Double, Double, Double)|]
  [|\(ConfidenceInterval x y z) -> (x, y, z)|]
  [|\(x, y, z) -> ConfidenceInterval x y z|]

listToCI :: [Double] -> Either Text ConfidenceInterval
listToCI (x : (y : (z : []))) = Right $ ConfidenceInterval x y z
listToCI l = Left $ "listToCI: " <> show l <> "is wrong size list.  Should be exactly 3 elements."

keyedCIsToFrame :: forall t f k ks.
                   (V.KnownField t
                   , V.Snd t ~ ConfidenceInterval
                   , Traversable f
                   , FI.RecVec (ks V.++ '[t]))
                => (k -> F.Record ks)
                -> f (k,[Double])
                -> Either Text (F.FrameRec (ks V.++ '[t]))
keyedCIsToFrame keyToRec kvs =
  let g (k, ds) = (\ci -> keyToRec k F.<+> FT.recordSingleton @t ci) <$> listToCI ds
  in F.toFrame <$> traverse g kvs


type ModelId t = "ModelId" F.:-> t

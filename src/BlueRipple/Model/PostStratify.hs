{-# LANGUAGE AllowAmbiguousTypes       #-}
{-# LANGUAGE DataKinds                 #-}
{-# LANGUAGE DeriveDataTypeable        #-}
{-# LANGUAGE DeriveGeneric             #-}
{-# LANGUAGE PolyKinds                 #-}
{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE FlexibleInstances         #-}
{-# LANGUAGE GADTs                     #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE TypeApplications          #-}
{-# LANGUAGE TypeFamilies              #-}
{-# LANGUAGE TypeOperators             #-}
{-# LANGUAGE QuasiQuotes               #-}
{-# LANGUAGE StandaloneDeriving        #-}
{-# LANGUAGE TupleSections             #-}

module BlueRipple.Model.PostStratify where

import qualified Control.Foldl                 as FL
import           Control.Monad                  ( join )
import qualified Data.Array                    as A
import           Data.Function                  ( on )
import qualified Data.List                     as L
import qualified Data.Set                      as S
import qualified Data.Map                      as M
import           Data.Maybe                     ( isJust
                                                , catMaybes
                                                )
import           Data.Proxy                     ( Proxy(..) )
--import  Data.Ord (Compare)

import qualified Data.Text                     as T
import qualified Data.Serialize                as SE
import qualified Data.Vector                   as V

import qualified Frames                        as F
import qualified Frames.Melt                   as F
import qualified Frames.InCore                 as FI
import qualified Data.Vinyl                    as V
import qualified Data.Vinyl.TypeLevel          as V

import qualified Control.MapReduce             as MR
import qualified Frames.Transform              as FT
import qualified Frames.Folds                  as FF
import qualified Frames.MapReduce              as FMR
import qualified Frames.Enumerations           as FE
import qualified Frames.Utils                  as FU
import qualified Frames.Visualization.VegaLite.Data
                                               as FV
import qualified Graphics.Vega.VegaLite        as GV
import           GHC.Generics                   ( Generic )
import           Data.Discrimination            ( Grouping )
postStratifyCell
  :: forall t q
   . (V.KnownField t, V.Snd t ~ Double)
  => (q -> Double)
  -> (q -> Double)
  -> FL.Fold q (F.Record '[t])
postStratifyCell weight count =
  (\sdw sw -> FT.recordSingleton @t (sdw / sw))
    <$> FL.premap (\x -> weight x * count x) FL.sum
    <*> FL.premap weight FL.sum

postStratifyF
  :: forall k cs ts rs
   . (k F.⊆ rs, cs F.⊆ rs, Ord (F.Record k), FI.RecVec (k V.++ ts))
  => FL.Fold (F.Record cs) [F.Record ts]
  -> FL.Fold (F.Record rs) (F.FrameRec (k V.++ ts))
postStratifyF cellFold = fmap (F.toFrame . concat) $ MR.mapReduceFold
  MR.noUnpack
  (FMR.assignKeysAndData @k @cs)
  (MR.ReduceFold (\k -> fmap (fmap (k `V.rappend`)) cellFold))

labeledPostStratifyF
  :: forall k cs p ts rs
   . ( k F.⊆ rs
     , cs F.⊆ rs
     , Ord (F.Record k)
     , FI.RecVec (k V.++ ('[p] V.++ ts))
     , V.KnownField p
     )
  => V.Snd p
  -> FL.Fold (F.Record cs) [F.Record ts]
  -> FL.Fold (F.Record rs) (F.FrameRec (k V.++ ('[p] V.++ ts)))
labeledPostStratifyF lv cf =
  postStratifyF @k $ fmap (V.rappend (FT.recordSingleton @p lv)) <$> cf

data PostStratifiedByT = Voted | VAP deriving (Enum, Bounded, Eq , Ord, Show, Generic)
type PostStratifiedBy = "PostStratifiedBy" F.:-> PostStratifiedByT
type instance FI.VectorFor PostStratifiedByT = V.Vector
instance Grouping PostStratifiedByT
instance SE.Serialize PostStratifiedByT

instance FV.ToVLDataValue (F.ElField PostStratifiedBy) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Str $ T.pack $ show $ V.getField x)

{-# LANGUAGE AllowAmbiguousTypes       #-}
{-# LANGUAGE DataKinds                 #-}
{-# LANGUAGE PolyKinds                 #-}
{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE FlexibleInstances         #-}
{-# LANGUAGE GADTs                     #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE TypeApplications          #-}
{-# LANGUAGE TypeFamilies              #-}
{-# LANGUAGE TypeOperators             #-}

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

import qualified Data.Text                     as T
import qualified Data.Serialize                as SE
import qualified Data.Vector                   as V

import qualified Frames                        as F
import qualified Frames.Streamly.InCore        as FI
import qualified Frames.Melt                   as F
import qualified Data.Vinyl                    as V
import qualified Data.Vinyl.TypeLevel          as V

import qualified Control.MapReduce             as MR
import qualified Frames.Transform              as FT
import qualified Frames.SimpleJoins            as FJ
import qualified Frames.Folds                  as FF
import qualified Frames.MapReduce              as FMR
import qualified Frames.Enumerations           as FE
--import qualified Frames.Visualization.VegaLite.Data
--                                               as FV
--import qualified Graphics.Vega.VegaLite        as GV
--import           GHC.Generics                   ( Generic )
--import           Data.Discrimination            ( Grouping )

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
  (MR.ReduceFold (\k -> (k `V.rappend`) <<$>> cellFold))

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


joinAndPostStratify
  :: forall ks pks ws cs ds es aks.
     (
       FJ.CanLeftJoinM (ks V.++ pks) ((ks V.++ pks) V.++ ws) ((ks V.++ pks) V.++ cs)
     , Ord (F.Record ks)
     , aks ~ (ks V.++ pks)
     , aks F.⊆ ((aks V.++ ws) V.++ F.RDeleteAll aks (aks V.++ cs))
     , ws F.⊆ ((aks V.++ ws) V.++ F.RDeleteAll aks (aks V.++ cs))
     , cs F.⊆ ((aks V.++ ws) V.++ F.RDeleteAll aks (aks V.++ cs))
     , ks F.⊆ (aks V.++ ds)
     , ds F.⊆ (aks V.++ ds)
     , FI.RecVec (ks V.++ es)
     )
  => (F.Record ws -> F.Record cs -> F.Record ds)
  -> FL.Fold (F.Record ds) (F.Record es)
  -> F.FrameRec ((ks V.++ pks) V.++ ws)
  -> F.FrameRec ((ks V.++ pks) V.++ cs)
  -> (F.FrameRec (ks V.++ es), [F.Record (ks V.++ pks)], Int)
joinAndPostStratify compute psFld wgts cnts = (FL.fold fld computed, missing, length joined - length wgts)
  where
    length = FL.fold FL.length
    (joined, missing) = FJ.leftJoinWithMissing @(ks V.++ pks) wgts cnts
    computeRow r = F.rcast @(ks V.++ pks) r F.<+> compute (F.rcast @ws r) (F.rcast @cs r)
    computed = fmap computeRow joined
    fld :: FL.Fold (F.Record ((ks V.++ pks) V.++ ds)) (F.FrameRec (ks V.++es))
    fld = FMR.concatFold
          $ FMR.mapReduceFold
          FMR.noUnpack
          (FMR.assignKeysAndData @ks @ds)
          (FMR.foldAndAddKey psFld)

{-
weightedSumFold :: FL.Fold (F.Record (w ': rs)) (F.Record (w ': rs))
weightedSumFold
-}

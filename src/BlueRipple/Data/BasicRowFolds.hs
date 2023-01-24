{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TypeOperators    #-}

module BlueRipple.Data.BasicRowFolds
  (
    module BlueRipple.Data.BasicRowFolds
  )
where

import qualified BlueRipple.Utilities.FramesUtils as BRF

import qualified Control.Foldl as FL

import qualified Frames as F
import qualified Frames.Streamly.InCore        as FI
import qualified Frames.MapReduce              as FMR
import qualified Control.MapReduce             as MR

import qualified Data.Vinyl.TypeLevel as V

import qualified Knit.Report as K
import qualified Knit.Utilities.Streamly as K


{-
Given:
1. A function which maps each row to a Traversable of new rows
  a. This can be used to split rows via something like [] or to filter them using Maybe
2. A function from the fixed row to geographic/time keys and demographic keys
3. A function from the fixed row to the data fields of interest
4. A fold to aggregate those data fields
5. A Foldable of rows

Produce a new set of rows with only the given geographic and demographic keys,
with the data fields aggregated as specified for common sets of keys.
-}
rowFold :: (Ord (F.Record gks)
           , FI.RecVec (gks V.++ ds)
           , Foldable f
           , Traversable g
           )
        => (F.Record rs -> g (F.Record rs')) -- Note that Maybe and Either are Traversable in the way that works here
        -> (F.Record rs' -> F.Record gks) -- time and geographic keys
        -> (F.Record rs' -> F.Record cs) -- data fields
        -> FL.Fold (F.Record cs) (F.Record ds) -- how to merge data once rows are grouped
--        -> (F.Record ks -> )
        -> f (F.Record rs)
        -> K.StreamlyM (F.FrameRec (gks V.++ ds))
rowFold fixRow keys datFields datFld rows =
  BRF.frameCompactMR
  (MR.Unpack fixRow)
  (MR.Assign $ \r -> (keys r, datFields r))
  datFld
  rows

-- same as above but without any row modification before aggregation
simpleRowFold :: (Ord (F.Record gks)
                 , FI.RecVec (gks V.++ ds)
                 , Foldable f
                 )
              => (F.Record rs -> F.Record gks) -- time, geographic and demographic keys
              -> (F.Record rs -> F.Record cs) -- data fields
              -> FL.Fold (F.Record cs) (F.Record ds) -- how to merge data once rows are grouped
              -> f (F.Record rs)
              -> K.StreamlyM (F.FrameRec (gks V.++ ds))
simpleRowFold = rowFold Identity


-- First we do the simple fold to the base geography, folding the data as prescribed
-- then we expand the base geography to something else, with weights
-- then we re-fold, combining common rows in the new geography
{-
Given:
1. A function which maps each row to a Traversable of new rows
  a. This can be used to split rows via something like [] or to filter them using Maybe
2. A function from the fixed row to geographic/time keys
3. A function from any set of geogrpahic/time keys to list of weights and new geographic/time keys
4. A function from fixed row to demographic keys
5. A function from the fixed row to the data fields of interest
6. A fold to aggregate those data fields
7. A function to weight the data fields given a weight
8. A function to recombine the data fields once weighted

Produce a new set of rows with only the new geographic and
given demographic keys, with the data fields aggregated as specified,
weighted and re-aggregated as specified
for common sets of keys.
-}
geoUnfoldRowFold :: forall gs hs ks cs ds f g rs rs' r .
                    (Ord (F.Record (gs V.++ ks))
                    , Ord (F.Record (hs V.++ ks))
                    , FI.RecVec (gs V.++ ks V.++ ds)
                    , ks F.⊆ (gs V.++ ks V.++ ds)
                    , gs F.⊆ (gs V.++ ks V.++ ds)
                    , ds F.⊆ (gs V.++ ks V.++ ds)
                    , ds F.⊆ (hs V.++ (ks V.++ ds))
                    , (hs V.++ ks) F.⊆ (hs V.++ (ks V.++ ds))
                    , FI.RecVec (hs V.++ ks V.++ ds)
                    , Foldable f
                    , Traversable g
                    , K.KnitEffects r
                    )
                 => (F.Record rs -> g (F.Record rs')) -- Note that Maybe and Either are Traversable in the way that works here
                 -> (F.Record rs' -> F.Record gs) -- time and geographic keys
                 -> (F.Record gs -> Either Text [(Double, F.Record hs)]) -- weighted membership in different geographies
                 -> (F.Record rs' -> F.Record ks) -- demographic keys
                 -> (F.Record rs' -> F.Record cs) -- data fields
                 -> FL.Fold (F.Record cs) (F.Record ds) -- how to merge data once rows are grouped
                 -> (Double -> F.Record ds -> F.Record ds) -- how to re-weight data into component geographies
                 -> FL.Fold (F.Record ds) (F.Record ds) -- how to re-combine data
                 -> f (F.Record rs)
                 -> K.Sem r (F.FrameRec (hs V.++ ks V.++ ds))
geoUnfoldRowFold fixRow geoKeys geoExpand demoKeys datFields datFld applyWgt foldedDatFld rows = do
  foldedToBaseGeo <- K.streamlyToKnit
                     $ rowFold fixRow (\r -> geoKeys r F.<+> demoKeys r) datFields datFld rows
  let unpackRec r = fmap (fmap (\(wgt, gh) -> gh F.<+> F.rcast @ks r F.<+> applyWgt wgt (F.rcast @ds r))) $ geoExpand (F.rcast @gs r)
      geoFldE = MR.concatFoldM
                $ MR.mapReduceFoldM
                 (MR.UnpackM unpackRec)
                 (MR.generalizeAssign $ FMR.assignKeysAndData @(hs V.++ ks) @ds)
                 (MR.generalizeReduce $ FMR.foldAndAddKey foldedDatFld)
  K.knitEither $ FL.foldM geoFldE foldedToBaseGeo

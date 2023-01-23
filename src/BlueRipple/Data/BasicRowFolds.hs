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


simpleRowFold :: (Ord (F.Record (gs V.++ ks))
                 , FI.RecVec (gs V.++ ks V.++ ds)
                 , Foldable f
                 )
              => (F.Record rs -> F.Record gs) -- time and geographic keys
              -> (F.Record rs -> F.Record ks) -- demographic keys
              -> (F.Record rs -> F.Record cs) -- data fields
              -> FL.Fold (F.Record cs) (F.Record ds) -- how to merge data once rows are grouped
              -> f (F.Record rs)
              -> K.StreamlyM (F.FrameRec (gs V.++ ks V.++ ds))
simpleRowFold geoKeys demoKeys datFields datFld rows =
  BRF.frameCompactMRM
  FMR.noUnpack
  (FMR.Assign (\r -> (geoKeys r F.<+> demoKeys r, datFields r)))
  datFld
  rows


-- First we do the simple fold to the base geography, folding the data as prescribed
-- then we expand the base geography to something else, with weights
-- then we re-fold, combining common rows in the new geography
geoUnfoldRowFold :: forall gs hs ks cs ds f rs r .
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
                    , K.KnitEffects r
                    )
                 => (F.Record rs -> F.Record gs) -- time and geographic keys
                 -> (F.Record gs -> Either Text [(Double, F.Record hs)]) -- weighted membership in different geographies
                 -> (F.Record rs -> F.Record ks) -- demographic keys
                 -> (F.Record rs -> F.Record cs) -- data fields
                 -> FL.Fold (F.Record cs) (F.Record ds) -- how to merge data once rows are grouped
                 -> (Double -> F.Record ds -> F.Record ds) -- how to re-weight data into component geographies
                 -> FL.Fold (F.Record ds) (F.Record ds) -- how to re-combine data
                 -> f (F.Record rs)
                 -> K.Sem r (F.FrameRec (hs V.++ ks V.++ ds))
geoUnfoldRowFold geoKeys geoExpand demoKeys datFields datFld applyWgt foldedDatFld rows = do
  foldedToBaseGeo <- K.streamlyToKnit
                     $ simpleRowFold geoKeys demoKeys datFields datFld rows
  let unpackRec r = fmap (fmap (\(wgt, gh) -> gh F.<+> F.rcast @ks r F.<+> applyWgt wgt (F.rcast @ds r))) $ geoExpand (F.rcast @gs r)
      geoFldE = MR.concatFoldM
                $ MR.mapReduceFoldM
                 (MR.UnpackM unpackRec)
                 (MR.generalizeAssign $ FMR.assignKeysAndData @(hs V.++ ks) @ds)
                 (MR.generalizeReduce $ FMR.foldAndAddKey foldedDatFld)
  K.knitEither $ FL.foldM geoFldE foldedToBaseGeo

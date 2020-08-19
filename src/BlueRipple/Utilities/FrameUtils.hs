module BlueRipple.Utilities.FramesUtils where

import qualified Control.MapReduce.Core as MapReduce
import qualified Control.MapReduce.Engines as MapReduce

import qualified Control.Foldl as Foldl
import qualified Control.Monad.Primitive as Prim

import qualified Data.Map.Strict as Map

import qualified Frames 
import qualified Frames.InCore
import qualified Frames.Streamly 
import qualified Streamly
import qualified Streamly.Prelude as Streamly
import qualified Streamly.Prelude.Internal as Streamly


streamGrouper ::
  forall ks cs m.
  (Prim.PrimMonad m
  , Ord (Frames.Record ks)
  , Frames.InCore.RecVec cs)
  => Streamly.SerialT m (Frames.Record ks, Frames.Record cs)
  -> Streamly.SerialT m (Frames.Record ks, Frames.FrameRec cs)
streamGrouper = Streamly.concatM . Streamly.fold groupFold
  where groupFold = fmap mapToStream $ Streamly.Fold.classify Frames.Streamly.inCoreAoS_F
        mapToStream = Streamly.fromFoldable . Map.toList
        
framesStreamlyMR ::
  (Prim.PrimMonad m
  )
  => MR.MapReduceFoldM m (F.Record bs) (F.Record ks) (F.Record cs) Frames.Frame (F.Record as) (F.Record ds)
framesStreamlyMR unpack (AssignM af) reduce =
  let unpackS = case unpack of
        MapReduce.FilterM f -> Streamly.filterM f
        MapReduce.UnpackM f -> Streamly.concatMapM (fmap Streamly.fromFoldable . f)
      assignS = Streamly.mapM af
      groupS = streamGrouper
      reduceS = Streamly.mapM (\(k, cF) -> MapReduce.reduceFunctionM reduce)
      processS = Frames.Streamly.inCoreAoS . reduceS . groupS . assignS . unpackS 
  in Foldl.FoldM (return . flip Streamly.cons) (return Streamly.nil) processS


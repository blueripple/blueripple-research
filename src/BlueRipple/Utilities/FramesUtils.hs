{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TypeFamilies     #-}
{-# LANGUAGE TypeOperators    #-}
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
import qualified Streamly.Internal.Prelude as Streamly
import qualified Streamly.Internal.Data.Fold as Streamly.Fold

streamGrouper ::
  ( Prim.PrimMonad m
  , Ord (Frames.Record ks)
  , Frames.InCore.RecVec cs
  )
  => Streamly.SerialT m (Frames.Record ks, Frames.Record cs)
  -> Streamly.SerialT m (Frames.Record ks, Frames.FrameRec cs)
streamGrouper = Streamly.concatM . Streamly.fold groupFold
  where groupFold = fmap mapToStream $ Streamly.Fold.classify Frames.Streamly.inCoreAoS_F
        mapToStream = Streamly.fromFoldable . Map.toList

-- defer toAoS step until grouping is complete?
streamGrouper2 ::
  ( Prim.PrimMonad m
  , Ord (Frames.Record ks)
  , Frames.InCore.RecVec cs
  )
  => Streamly.SerialT m (Frames.Record ks, Frames.Record cs)
  -> Streamly.SerialT m (Frames.Record ks, (Int, Frames.Rec (((->) Int) Frames.:. Frames.ElField) cs))
streamGrouper2 = Streamly.concatM . Streamly.fold groupFold
  where groupFold = fmap mapToStream $ Streamly.Fold.classify Frames.Streamly.inCoreSoA_F
        mapToStream = Streamly.fromFoldable . Map.toList        

fixStreamGrouper2 :: Monad m
                  => Streamly.SerialT m (Frames.Record ks, (Int, Frames.Rec (((->) Int) Frames.:. Frames.ElField) cs))
                  -> Streamly.SerialT m (Frames.Record ks, Frames.FrameRec cs)
fixStreamGrouper2 = Streamly.map (\(k, (n, soa)) ->  (k, Frames.InCore.toAoS n soa))
        

{-
streamGrouperST :: (Prim.PrimMonad m
                   , Ord (Frames.Record ks)
                   , Frames.InCore.RecVec cs)
-}

framesStreamlyMRM ::
  (Prim.PrimMonad m
  , Streamly.MonadAsync m
  , Ord (Frames.Record ks)
  , Frames.InCore.RecVec cs
  , Frames.InCore.RecVec ds
  )
  => MapReduce.MapReduceFoldM m
  (Frames.Record bs)
  (Frames.Record ks)
  (Frames.Record cs)
  Frames.Frame
  (Frames.Record as)
  (Frames.Record ds)
framesStreamlyMRM unpack (MapReduce.AssignM af) reduce =
  let unpackS = case unpack of
        MapReduce.FilterM f -> Streamly.filterM f
        MapReduce.UnpackM f -> Streamly.concatMapM (fmap Streamly.fromFoldable . f)
      assignS = Streamly.mapM af
      groupS = fixStreamGrouper2 . streamGrouper2
      reduceS = Streamly.mapM (\(k, cF) -> reduceFunctionM reduce k cF)
      processS = Frames.Streamly.inCoreAoS . reduceS . groupS . assignS . unpackS 
  in Foldl.FoldM (\s a -> return $ a `Streamly.cons` s) (return Streamly.nil) processS
{-# INLINEABLE framesStreamlyMRM #-}

framesStreamlyMR ::
  (Prim.PrimMonad m
  , Streamly.MonadAsync m
  , Ord (Frames.Record ks)
  , Frames.InCore.RecVec cs
  , Frames.InCore.RecVec ds
  )
  => MapReduce.Unpack (Frames.Record as) (Frames.Record bs)
  -> MapReduce.Assign (Frames.Record ks) (Frames.Record bs) (Frames.Record cs)
  -> MapReduce.Reduce (Frames.Record ks) (Frames.Record cs) (Frames.Record ds)
  -> Foldl.FoldM m (Frames.Record as) (Frames.FrameRec ds)
framesStreamlyMR u a r = framesStreamlyMRM
                         (MapReduce.generalizeUnpack u)
                         (MapReduce.generalizeAssign a)
                         (MapReduce.generalizeReduce r)
{-# INLINEABLE framesStreamlyMR #-}

-- | Turn @ReduceM@ into a function we can apply
reduceFunctionM
  :: (Foldable h, Functor h, Monad m) => MapReduce.ReduceM m k x d -> k -> h x -> m d
reduceFunctionM (MapReduce.ReduceM     f) k = f k
reduceFunctionM (MapReduce.ReduceFoldM f) k = Foldl.foldM (f k)
{-# INLINABLE reduceFunctionM #-}

{-# LANGUAGE BangPatterns #-}
--{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeFamilies     #-}
{-# LANGUAGE TypeOperators    #-}
module BlueRipple.Utilities.FramesUtils where

import qualified Control.MapReduce.Core as MapReduce
import qualified Control.MapReduce.Engines as MapReduce

import qualified Control.Foldl as Foldl
import qualified Control.Monad as Monad
import qualified Control.Monad.Primitive as Prim
import qualified Control.Monad.ST as ST

import qualified Data.Hashable as Hashable
import Data.Proxy (Proxy(..))
import qualified Data.Map.Strict as Map

import qualified Frames 
import qualified Frames.InCore
import qualified Frames.Streamly 
import qualified Streamly
import qualified Streamly.Prelude as Streamly
import qualified Streamly.Internal.Prelude as Streamly
import qualified Streamly.Internal.Data.Fold as Streamly.Fold
import qualified Streamly.Internal.Data.Fold.Types as Streamly.Fold

import qualified Data.HashTable.Class          as HashTable
import qualified Data.HashTable.ST.Cuckoo      as HashTable.Cuckoo

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
  -> Streamly.SerialT m (Frames.Record ks, Frames.FrameRec cs)
streamGrouper2 = Streamly.map (\(k, (n, soa)) ->  (k, Frames.InCore.toAoS n soa))
                 . Streamly.concatM
                 . Streamly.fold groupFold
  where groupFold = fmap mapToStream $ Streamly.Fold.classify Frames.Streamly.inCoreSoA_F
        mapToStream = Streamly.fromFoldable . Map.toList        
 
  
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
      groupS = streamGrouper2
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


type RecOfVecs m cs = Frames.Record (Frames.InCore.VectorMs (ST.ST (Prim.PrimState m)) cs)
type FeedIn m cs = (Int, Int, RecOfVecs m cs)
type HT m ks cs =  HashTable.Cuckoo.HashTable (Prim.PrimState m) (Frames.Record ks) (FeedIn m cs)

-- TODO: we can assume MonadAsync here since we assume it below
-- so we can use consM or mapM in "done"
classifyHT :: forall m ks cs . (Prim.PrimMonad m
                               , Hashable.Hashable (Frames.Record ks)
                               , Eq (Frames.Record ks)
                               , Frames.InCore.RecVec cs)
           => Streamly.Fold.Fold m (Frames.Record ks, Frames.Record cs) (Streamly.SerialT m (Frames.Record ks, Frames.FrameRec cs))
classifyHT = Streamly.Fold.Fold step init done where
  init :: m (HT m ks cs) = Prim.stToPrim $ HashTable.new

  step :: HT m ks cs -> (Frames.Record ks, Frames.Record cs) -> m (HT m ks cs)
  step ht (k, c) = Prim.stToPrim $ do
    HashTable.mutateST ht k (addOneST c)
    return ht

  feed (!i, !sz, !mvs') row
    | i == sz = Frames.InCore.growRec (Proxy::Proxy cs) mvs'
                >>= flip feed row . (i, sz*2,)
    | otherwise = do Frames.InCore.writeRec (Proxy::Proxy cs) i mvs' row
                     return (i+1, sz, mvs')                         
  initial = do
    mvs <- Frames.InCore.allocRec (Proxy :: Proxy cs) Frames.InCore.initialCapacity
    return (0, Frames.InCore.initialCapacity, mvs)          

  addOneST :: Frames.Record cs -> Maybe (FeedIn m cs) -> ST.ST (Prim.PrimState m) (Maybe (FeedIn m cs), ())
  addOneST c prevM = fmap (\x -> (Just x, ())) $ case prevM of
    Nothing -> initial >>= flip feed c
    Just ht -> feed ht c    

  fin (n, _, mvs') =
    do vs <- Frames.InCore.freezeRec (Proxy::Proxy cs) n mvs'
       return . (n,) $ Frames.InCore.produceRec (Proxy::Proxy cs) vs
  
  finalize :: (Frames.Record k, FeedIn (ST.ST (Prim.PrimState m)) cs)
           -> (ST.ST (Prim.PrimState m)) (Frames.Record k, Frames.FrameRec cs)
  finalize (k, v) = do
    frame <- uncurry Frames.toAoS <$> fin v
    return (k, frame)

  done :: HT m ks cs -> m (Streamly.SerialT m (Frames.Record ks, Frames.FrameRec cs))
  done =  Prim.stToPrim . fmap Streamly.fromList . Monad.join . fmap (traverse finalize) . HashTable.toList
{-    
  done :: HT m ks cs -> m (Streamly.SerialT m (Frames.Record ks, Frames.FrameRec cs))
  done = Prim.stToPrim . HashTable.foldM (\s (k, v) -> return $ finalize (k, v) `Streamly.consM` s) Streamly.nil
-}

streamGrouperHT :: (Prim.PrimMonad m
                   , Monad m
                   , Hashable.Hashable (Frames.Record ks)
                   , Eq (Frames.Record ks)
                   , Frames.InCore.RecVec cs
                   )
                => Streamly.SerialT m (Frames.Record ks, Frames.Record cs)
                -> Streamly.SerialT m (Frames.Record ks, Frames.FrameRec cs)
streamGrouperHT = Streamly.concatM . Streamly.fold classifyHT


framesStreamlyMRM_HT ::
  (Prim.PrimMonad m
  , Streamly.MonadAsync m
  , Hashable.Hashable (Frames.Record ks)
  , Eq (Frames.Record ks)
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
framesStreamlyMRM_HT unpack (MapReduce.AssignM af) reduce =
  let unpackS = case unpack of
        MapReduce.FilterM f -> Streamly.filterM f
        MapReduce.UnpackM f -> Streamly.concatMapM (fmap Streamly.fromFoldable . f)
      assignS = Streamly.mapM af
      groupS = streamGrouperHT
      reduceS = Streamly.mapM (\(k, cF) -> reduceFunctionM reduce k cF)
      processS = Frames.Streamly.inCoreAoS . reduceS . groupS . assignS . unpackS 
  in Foldl.FoldM (\s a -> return $ a `Streamly.cons` s) (return Streamly.nil) processS
{-# INLINEABLE framesStreamlyMRM_HT #-}

framesStreamlyMR_HT ::
  (Prim.PrimMonad m
  , Streamly.MonadAsync m
  , Hashable.Hashable (Frames.Record ks)
  , Eq (Frames.Record ks)
  , Frames.InCore.RecVec cs
  , Frames.InCore.RecVec ds
  )
  => MapReduce.Unpack (Frames.Record as) (Frames.Record bs)
  -> MapReduce.Assign (Frames.Record ks) (Frames.Record bs) (Frames.Record cs)
  -> MapReduce.Reduce (Frames.Record ks) (Frames.Record cs) (Frames.Record ds)
  -> Foldl.FoldM m (Frames.Record as) (Frames.FrameRec ds)
framesStreamlyMR_HT u a r = framesStreamlyMRM_HT
                         (MapReduce.generalizeUnpack u)
                         (MapReduce.generalizeAssign a)
                         (MapReduce.generalizeReduce r)
{-# INLINEABLE framesStreamlyMR_HT #-}


-- | Turn @ReduceM@ into a function we can apply
reduceFunctionM
  :: (Foldable h, Functor h, Monad m) => MapReduce.ReduceM m k x d -> k -> h x -> m d
reduceFunctionM (MapReduce.ReduceM     f) k = f k
reduceFunctionM (MapReduce.ReduceFoldM f) k = Foldl.foldM (f k)
{-# INLINABLE reduceFunctionM #-}

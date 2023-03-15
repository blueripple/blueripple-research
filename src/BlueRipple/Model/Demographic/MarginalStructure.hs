{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE UnicodeSyntax #-}
{-# LANGUAGE StandaloneDeriving #-}

module BlueRipple.Model.Demographic.MarginalStructure
  (
    module BlueRipple.Model.Demographic.MarginalStructure
  )
where

import qualified BlueRipple.Model.Demographic.EnrichData as DED

import qualified BlueRipple.Data.Keyed as BRK


import qualified Control.Foldl as FL
import qualified Data.Map.Strict as M
import qualified Data.Map.Merge.Strict as MM
import qualified Data.Profunctor as PF
import qualified Data.Set as S
import Data.Type.Equality (type (~))

import qualified Data.List as List
import qualified Data.Vinyl.TypeLevel as V
import qualified Frames as F


reKeyMarginalStructure :: (Ord k2, BRK.FiniteSet k2) => (k2 -> k1) -> (k1 -> k2) -> MarginalStructure k1 -> MarginalStructure k2
reKeyMarginalStructure f21 f12 ms = case ms of
  MarginalStructure sts ptFld -> MarginalStructure sts' ptFld' where
    sts' = fmap (expandStencil f21) sts
    ptFld' = PF.dimap (first f21) (sortOn fst . fmap (first f12)) ptFld

-- given a k2 stencil and a map (k1 -> k2), we can generate a k1 stencil by testing each k1 to see if it maps to a k2 which
-- is in the stencil
expandStencil :: forall k1 k2 . (BRK.FiniteSet k1, Ord k2, BRK.FiniteSet k2) => (k1 -> k2) -> DED.Stencil Int -> DED.Stencil Int
expandStencil f st = DED.Stencil dil
  where
    rl = S.toList $ BRK.elements @k2
    sis = foldl' (\s i -> flip S.insert s $ rl List.!! i) mempty (DED.stencilIndexes st)
    dl = zip (S.toList $ BRK.elements @k1) [0..]
    dil = snd <$> filter (flip S.member sis . f . fst) dl
{-# INLINEABLE expandStencil #-}


combineMarginalStructures :: forall ok ka kb .
                             (Ord ok, BRK.FiniteSet ok, Ord ka, BRK.FiniteSet ka, Ord kb, BRK.FiniteSet kb)
                          => MarginalStructure (ok, ka) -> MarginalStructure (ok, kb) -> MarginalStructure (ok, ka, kb)
combineMarginalStructures = combineMarginalStructures' (\(ok, ka, _) -> (ok, ka)) id (\(ok, _, kb) -> (ok, kb)) id id

combineMarginalStructuresF :: forall k ka kb .
                              (Ord (F.Record k), BRK.FiniteSet (F.Record k)
                              , Ord (F.Record ka), BRK.FiniteSet (F.Record ka)
                              , Ord (F.Record kb), BRK.FiniteSet (F.Record kb)
                              , Ord (F.Record (k V.++ ka V.++ kb))
                              , Ord (F.Record (k V.++ ka)), BRK.FiniteSet (F.Record (k V.++ ka))
                              , Ord (F.Record (k V.++ kb)), BRK.FiniteSet (F.Record (k V.++ kb))
                              , BRK.FiniteSet (F.Record (k V.++ ka V.++ kb))
                              , k F.⊆ (k V.++ ka)
                              , ka F.⊆ (k V.++ ka)
                              , k F.⊆ (k V.++ kb)
                              , kb F.⊆ (k V.++ kb)
                              , (k V.++ ka) F.⊆ (k V.++ ka V.++ kb)
                              , (k V.++ kb) F.⊆ (k V.++ ka V.++ kb)
                              , (k V.++ (ka V.++ kb)) ~ ((k V.++ ka) V.++ kb)
                              )
                           => MarginalStructure (F.Record (k V.++ ka))
                           -> MarginalStructure (F.Record (k V.++ kb))
                           -> MarginalStructure (F.Record (k V.++ ka V.++ kb))
combineMarginalStructuresF = combineMarginalStructures'
                             (F.rcast @(k V.++ ka))
                             (\r -> (F.rcast @k r, F.rcast @ka r))
                             (F.rcast @(k V.++ kb))
                             (\r -> (F.rcast @k r, F.rcast @kb r))
                             (\(kr, kar, kbr) -> kr F.<+> kar F.<+> kbr)
{-# INLINEABLE combineMarginalStructuresF #-}

combineMarginalStructures' :: forall k ka kb ok ika ikb .
                             (Ord k, BRK.FiniteSet k
                             , Ord ka, BRK.FiniteSet ka
                             , Ord kb, BRK.FiniteSet kb
                             , Ord ok, BRK.FiniteSet ok
                             , Ord ika, BRK.FiniteSet ika
                             , Ord ikb, BRK.FiniteSet ikb)
                           => (k -> ka) -> (ka -> (ok, ika))
                           -> (k -> kb) -> (kb -> (ok, ikb))
                           -> ((ok, ika, ikb) -> k)
                           -> MarginalStructure ka -> MarginalStructure kb -> MarginalStructure k
combineMarginalStructures' kaF expandA kbF expandB collapse msA msB = MarginalStructure sts prodFld
  where
    expandedStencilsA = fmap (expandStencil kaF) $ msStencils msA
    expandedStencilsB = fmap (expandStencil kbF) $ msStencils msB
    sts = expandedStencilsA <> expandedStencilsB
    aMapTblFld :: FL.Fold (k, Double) (Map ok (Map ika Double))
    aMapTblFld = FL.fold normalizedTableMapFld <$> (fmap (fmap (first expandA)) $ FL.premap (first kaF) $ msProdFld msA)
    bMapTblFld :: FL.Fold (k, Double) (Map ok (Map ikb Double))
    bMapTblFld = FL.fold normalizedTableMapFld <$> (fmap (fmap (first expandB)) $ FL.premap (first kbF) $ msProdFld msB)
    prodFld :: FL.Fold (k, Double) [(k, Double)]
    prodFld = fmap (fmap (first collapse)) $ tableProductL <$> aMapTblFld <*> bMapTblFld
{-# INLINEABLE combineMarginalStructures' #-}

identityMarginalStructure :: forall k . (Ord k, BRK.FiniteSet k) => MarginalStructure k
identityMarginalStructure = MarginalStructure (fmap (DED.Stencil . pure) $ [0..(numCats-1)] ) (M.toList <$> normalizeAndFillMapFld)
  where
    numCats = S.size $ BRK.elements @k
{-# INLINEABLE identityMarginalStructure #-}

data MarginalStructure k where
  MarginalStructure :: (BRK.FiniteSet k, Ord k) => [DED.Stencil Int] -> FL.Fold (k, Double) [(k, Double)]-> MarginalStructure k

msStencils :: MarginalStructure k -> [DED.Stencil Int]
msStencils (MarginalStructure sts _) = sts
{-# INLINE msStencils #-}

msProdFld :: MarginalStructure k -> FL.Fold (k, Double) [(k, Double)]
msProdFld (MarginalStructure _ ptF) = ptF
{-# INLINE msProdFld #-}

msNumCategories :: forall k . MarginalStructure k -> Int
msNumCategories ms = case ms of
  MarginalStructure _ _ -> S.size $ BRK.elements @k
{-# INLINEABLE msNumCategories #-}

constMap :: (BRK.FiniteSet k, Ord k) => a -> Map k a
constMap x = M.fromList $ fmap (, x) $ S.toList BRK.elements
{-# INLINEABLE constMap #-}

zeroMap :: (BRK.FiniteSet k, Ord k, Num a) => Map k a
zeroMap = constMap 0
{-# INLINEABLE zeroMap #-}
{-# SPECIALIZE zeroMap :: (BRK.FiniteSet k, Ord k) => Map k Double #-}

summedMap :: (Num b, Ord a) => FL.Fold (a, b) (Map a b)
summedMap = FL.foldByKeyMap FL.sum --fmap getSum <$> FL.premap (second Sum) appendingMap
{-# INLINE summedMap #-}
{-# SPECIALIZE summedMap :: Ord a => FL.Fold (a, Double) (Map a Double) #-}

zeroFillSummedMapFld :: (BRK.FiniteSet k, Ord k, Num a) => FL.Fold (k, a) (Map k a)
zeroFillSummedMapFld = fmap (<> zeroMap) summedMap
{-# INLINEABLE zeroFillSummedMapFld #-}
{-# SPECIALIZE zeroFillSummedMapFld ::  (BRK.FiniteSet k, Ord k) => FL.Fold (k, Double) (Map k Double) #-}

normalized :: (Foldable f, Functor f) => f Double -> f Double
normalized xs = let s = FL.fold FL.sum xs in fmap ( / s) xs

normalizeMapFld :: (Ord k) => FL.Fold (k, Double) (Map k Double)
normalizeMapFld = normalized <$> summedMap
{-# INLINEABLE normalizeMapFld #-}

normalizeAndFillMapFld :: (BRK.FiniteSet k, Ord k) => FL.Fold (k, Double) (Map k Double)
normalizeAndFillMapFld = normalized <$> zeroFillSummedMapFld
{-# INLINEABLE normalizeAndFillMapFld #-}

unNestTableProduct :: Map outerK (Map (a, b) x) -> [((outerK, a, b), x)]
unNestTableProduct = concatMap (\(ok, mab) -> fmap (\((a, b), x) -> ((ok, a, b), x)) $ M.toList mab) . M.toList
{-# INLINEABLE unNestTableProduct #-}

-- Nested maps make the products easier
-- this fills in missing items with 0
tableMapFld :: forall outerK x  . (BRK.FiniteSet outerK, Ord outerK, BRK.FiniteSet x, Ord x) => FL.Fold ((outerK, x), Double) (Map outerK (Map x Double))
tableMapFld = FL.premap keyPreMap $ fmap (<> constMap zeroMap) (FL.foldByKeyMap zeroFillSummedMapFld) where
    keyPreMap ((o, k), n) = (o, (k, n))
{-# INLINEABLE tableMapFld #-}

normalizedTableMapFld :: forall outerK x  . (BRK.FiniteSet outerK, Ord outerK, BRK.FiniteSet x, Ord x) => FL.Fold ((outerK, x), Double) (Map outerK (Map x Double))
normalizedTableMapFld = FL.premap keyPreMap $ fmap (normalize . (<> constMap zeroMap)) (FL.foldByKeyMap zeroFillSummedMapFld) where
    keyPreMap ((o, k), n) = (o, (k, n))
    sum' mm = FL.fold FL.sum $ fmap (FL.fold FL.sum) mm
    normalize mm = fmap (fmap ( / sum' mm)) mm
{-# INLINEABLE normalizedTableMapFld #-}


tableProductL' ::  forall outerK a b .
                  (Ord outerK, BRK.FiniteSet a, Ord a, BRK.FiniteSet b, Ord b)
              => Map outerK (Map a Double)
              -> Map outerK (Map b Double)
              -> [Double]
tableProductL' ma mb = snd <$> tableProductL ma mb
{-# INLINEABLE tableProductL' #-}

tableProductL ::  forall outerK a b .
                  (Ord outerK, BRK.FiniteSet a, Ord a, BRK.FiniteSet b, Ord b)
              => Map outerK (Map a Double)
              -> Map outerK (Map b Double)
              -> [((outerK, a, b), Double)]
tableProductL ma mb = unNestTableProduct $ tableProduct ma mb
{-# INLINEABLE tableProductL #-}

-- This is not symmetric. Table a is used for weights on outer key
-- Also, this treats missing entries as 0
tableProduct :: forall outerK a b .
                (Ord outerK, BRK.FiniteSet a, Ord a, BRK.FiniteSet b, Ord b)
             => Map outerK (Map a Double)
             -> Map outerK (Map b Double)
             -> Map outerK (Map (a, b) Double)
tableProduct aTableMap bTableMap = MM.merge
                                   (MM.mapMissing whenMissing)
                                   (MM.mapMissing whenMissing)
                                   (MM.zipWithMatched whenMatched)
                                   aTableMap bTableMap
  where
    whenMissing _ _ = zeroMap
    whenMatched _ = innerProduct
    innerProduct :: Map a Double -> Map b Double -> Map (a, b) Double
    innerProduct ma mb = M.fromList [f ae be | ae <- M.toList ma, be <- fracs mb]
      where
        f (a, x) (b, y) = ((a, b), x * y)
        fracs :: Map x Double -> [(x, Double)]
        fracs m = let s = FL.fold FL.sum m in M.toList $ if s == 0 then m else fmap (/ s) m
{-# INLINEABLE tableProduct #-}

{-
-- given a k2 product fold and a map (k1 -> k2) we can generate a k1 product zero-info product fold by assuming every k1 which maps to a k2
-- gets an equal share of that k2s weight
-- This will always fold to Just something because k1 is a finite set. But I can't get rid of the Maybe.
expandProdFld :: forall k1 k2 . (Ord k2, BRK.FiniteSet k1)
              => (k1 -> k2) -> FL.Fold (k2, Double) [(k2, Double)] -> FL.Fold (k1, Double) [(k1, Double)]
expandProdFld f k2Fld = expandList <$> fldA
  where
    fldA = FL.premap (first f) k2Fld
    k2k1Map = foldl' (\m k1 -> M.insertWith (<>) (f k1) [k1] m) mempty BRK.elements
    eF (k2, x) = case M.lookup k2 k2k1Map of
      Just k1s -> fmap (, x / realToFrac (length k1s)) k1s
      Nothing -> []
    expandList :: [(k2, Double)] -> [(k1, Double)]
    expandList = concatMap eF
{-# INLINEABLE expandProdFld #-}


expandMarginalStructure :: (Ord k1, BRK.FiniteSet k1) => (k1 -> k2) -> MarginalStructure k2 -> MarginalStructure k1
expandMarginalStructure f m2 = case m2 of
  MarginalStructure sts2 ptFld2 -> MarginalStructure (fmap (expandStencil f) sts2) (expandProdFld f ptFld2)
{-# INLINEABLE expandMarginalStructure #-}
-}

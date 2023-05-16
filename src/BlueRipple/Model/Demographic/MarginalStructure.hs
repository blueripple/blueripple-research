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
import Control.Lens (Lens', Traversal, view, set, over, lens)
import qualified Data.Map.Strict as M
import qualified Data.Map.Merge.Strict as MM
import qualified Data.Profunctor as PF
import qualified Data.Set as S
import Data.Type.Equality (type (~))

import qualified Data.List as List
import qualified Data.Vinyl.TypeLevel as V
import qualified Data.Vector.Unboxed as VU
import qualified Frames as F

normalize :: (Functor f, Foldable f) => Lens' a Double -> f a -> f a
normalize l xs = let s = FL.fold (FL.premap (view l) FL.sum) xs in fmap (over l (/ s)) xs


reKeyMarginalStructure :: (Ord k2, BRK.FiniteSet k2) => (k2 -> k1) -> (k1 -> k2) -> MarginalStructure w k1 -> MarginalStructure w k2
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


combineMarginalStructures :: forall ok ka kb w .
                             (Ord ok, BRK.FiniteSet ok, Ord ka, BRK.FiniteSet ka, Ord kb, BRK.FiniteSet kb, Monoid w)
                          => Lens' w Double
                          -> (Map ka w -> Map kb w -> Map (ka, kb) w)
                          -> MarginalStructure w (ok, ka) -> MarginalStructure w (ok, kb) -> MarginalStructure w (ok, ka, kb)
combineMarginalStructures wgtLens ip = combineMarginalStructures' wgtLens ip (\(ok, ka, _) -> (ok, ka)) id (\(ok, _, kb) -> (ok, kb)) id id

combineMarginalStructuresF :: forall k ka kb w .
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
                              , Monoid w
                              )
                           => Lens' w Double
                           -> (Map (F.Record ka) w -> Map (F.Record kb) w -> Map (F.Record ka, F.Record kb) w)
                           -> MarginalStructure w (F.Record (k V.++ ka))
                           -> MarginalStructure w (F.Record (k V.++ kb))
                           -> MarginalStructure w (F.Record (k V.++ ka V.++ kb))
combineMarginalStructuresF wgtLens innerProduct = combineMarginalStructures' wgtLens innerProduct
                                                (F.rcast @(k V.++ ka))
                                                (\r -> (F.rcast @k r, F.rcast @ka r))
                                                (F.rcast @(k V.++ kb))
                                                (\r -> (F.rcast @k r, F.rcast @kb r))
                                          (\(kr, kar, kbr) -> kr F.<+> kar F.<+> kbr)
{-# INLINEABLE combineMarginalStructuresF #-}

combineMarginalStructures' :: forall k ka kb ok ika ikb w .
                             (Ord k, BRK.FiniteSet k
                             , Ord ka, BRK.FiniteSet ka
                             , Ord kb, BRK.FiniteSet kb
                             , Ord ok, BRK.FiniteSet ok
                             , Ord ika, BRK.FiniteSet ika
                             , Ord ikb, BRK.FiniteSet ikb
                             , Monoid w)
                           => Lens' w Double
                           -> (Map ika w -> Map ikb w -> Map (ika, ikb) w)
                           -> (k -> ka) -> (ka -> (ok, ika))
                           -> (k -> kb) -> (kb -> (ok, ikb))
                           -> ((ok, ika, ikb) -> k)
                           -> MarginalStructure w ka -> MarginalStructure w kb -> MarginalStructure w k
combineMarginalStructures' wgtLens innerProduct kaF expandA kbF expandB collapse msA msB = MarginalStructure sts prodFld
  where
    expandedStencilsA = fmap (expandStencil kaF) $ msStencils msA
    expandedStencilsB = fmap (expandStencil kbF) $ msStencils msB
    sts = expandedStencilsA <> expandedStencilsB
    aMapTblFld :: FL.Fold (k, w) (Map ok (Map ika w))
    aMapTblFld = FL.fold (normalizedTableMapFld wgtLens) <$> (fmap (fmap (first expandA)) $ FL.premap (first kaF) $ msProdFld msA)
    bMapTblFld :: FL.Fold (k, w) (Map ok (Map ikb w))
    bMapTblFld = FL.fold (normalizedTableMapFld wgtLens) <$> (fmap (fmap (first expandB)) $ FL.premap (first kbF) $ msProdFld msB)
    prodFld :: FL.Fold (k, w) [(k, w)]
    prodFld = fmap (fmap (first collapse)) $ tableProductL innerProduct <$> aMapTblFld <*> bMapTblFld
{-# INLINEABLE combineMarginalStructures' #-}

identityMarginalStructure :: forall k w . (Ord k, BRK.FiniteSet k, Monoid w)
                          => Lens' w Double -> MarginalStructure w k
identityMarginalStructure wgtLens = MarginalStructure (fmap (DED.Stencil . pure) $ [0..(numCats-1)] ) (M.toList <$> normalizeAndFillMapFld wgtLens)
  where
    numCats = S.size $ BRK.elements @k
{-# INLINEABLE identityMarginalStructure #-}



data MarginalStructure w k where
  MarginalStructure :: (Monoid w, BRK.FiniteSet k, Ord k) => [DED.Stencil Int] -> FL.Fold (k, w) [(k, w)]-> MarginalStructure w k

msStencils :: MarginalStructure w k -> [DED.Stencil Int]
msStencils (MarginalStructure sts _) = sts
{-# INLINE msStencils #-}

msProdFld :: MarginalStructure w k -> FL.Fold (k, w) [(k, w)]
msProdFld (MarginalStructure _ ptF) = ptF
{-# INLINE msProdFld #-}

msNumCategories :: forall k w . MarginalStructure w k -> Int
msNumCategories ms = case ms of
  MarginalStructure _ _ -> S.size $ BRK.elements @k
{-# INLINEABLE msNumCategories #-}

constMap :: (BRK.FiniteSet k, Ord k) => a -> Map k a
constMap x = M.fromList $ fmap (, x) $ S.toList BRK.elements
{-# INLINEABLE constMap #-}

zeroMap :: (Monoid w, BRK.FiniteSet k, Ord k, Num a) => Map k w
zeroMap = constMap mempty
{-# INLINEABLE zeroMap #-}
{-# SPECIALIZE zeroMap :: (BRK.FiniteSet k, Ord k, Num b) => Map k (Sum b) #-}
{-# SPECIALIZE zeroMap :: (BRK.FiniteSet k, Ord k) => Map k CellWithDensity #-}

summedMap :: (Monoid b, Ord a) => FL.Fold (a, b) (Map a b)
summedMap = FL.foldByKeyMap FL.mconcat --fmap getSum <$> FL.premap (second Sum) appendingMap
{-# INLINE summedMap #-}
{-# SPECIALIZE summedMap :: (Ord a, Num b) => FL.Fold (a, Sum b) (Map a (Sum b)) #-}
{-# SPECIALIZE summedMap :: Ord a => FL.Fold (a, CellWithDensity ) (Map a CellWithDensity) #-}

zeroFillSummedMapFld :: (BRK.FiniteSet k, Ord k, Monoid a) => FL.Fold (k, a) (Map k a)
zeroFillSummedMapFld = fmap (<> zeroMap) summedMap
{-# INLINEABLE zeroFillSummedMapFld #-}
{-# SPECIALIZE zeroFillSummedMapFld ::  (BRK.FiniteSet k, Ord k, Num b) => FL.Fold (k, Sum b) (Map k (Sum b)) #-}
{-# SPECIALIZE zeroFillSummedMapFld ::  (BRK.FiniteSet k, Ord k) => FL.Fold (k, CellWithDensity) (Map k CellWithDensity) #-}

--normalized :: (Foldable f, Functor f, Normalizable c) => f Double -> f Double
--normalized xs = let s = FL.fold FL.sum xs in fmap ( / s) xs

normalizeMapFld :: (Ord k, Monoid w) =>  Lens' w Double -> FL.Fold (k, w) (Map k w)
normalizeMapFld wgtLens = normalize wgtLens <$> summedMap
{-# INLINEABLE normalizeMapFld #-}

normalizeAndFillMapFld :: (BRK.FiniteSet k, Ord k, Monoid w)
                       => Lens' w Double -> FL.Fold (k, w) (Map k w)
normalizeAndFillMapFld wgtLens = normalize wgtLens <$> zeroFillSummedMapFld
{-# INLINEABLE normalizeAndFillMapFld #-}

unNestTableProduct :: Map outerK (Map (a, b) x) -> [((outerK, a, b), x)]
unNestTableProduct = concatMap (\(ok, mab) -> fmap (\((a, b), x) -> ((ok, a, b), x)) $ M.toList mab) . M.toList
{-# INLINEABLE unNestTableProduct #-}

-- Nested maps make the products easier
-- this fills in missing items with 0
tableMapFld :: forall outerK x w . (BRK.FiniteSet outerK, Ord outerK, BRK.FiniteSet x, Ord x, Monoid w) => FL.Fold ((outerK, x), w) (Map outerK (Map x w))
tableMapFld = FL.premap keyPreMap $ fmap (<> constMap zeroMap) (FL.foldByKeyMap zeroFillSummedMapFld) where
    keyPreMap ((o, k), n) = (o, (k, n))
{-# INLINEABLE tableMapFld #-}


--mapWgtLens :: (w -> Double, (Double -> Double) -> w -> w) -> (Map x w -> Double, (Double -> Double) -> Map x w -> Map x w)
--mapWgtLens (getWgt, modWgt) = (FL.fold (FL.premap getWgt FL.sum), fmap . modWgt)
{-
mapWgtLens' :: Ord x => Traversal w w Double Double -> Traversal (Map x w) (Map x w) Double Double
mapWgtLens' l afa mxw = s2
  where
    s1 = fmap (\w -> (w, afa $ view l w)) mxw
    s2 = traverse (\(w, fx) -> fmap (\x -> set l x w) fx) s1
-}
normalizedTableMapFld :: forall outerK x w . (BRK.FiniteSet outerK, Ord outerK, BRK.FiniteSet x, Ord x, Monoid w)
                      =>  Lens' w Double -> FL.Fold ((outerK, x), w) (Map outerK (Map x w))
normalizedTableMapFld wgtLens = FL.premap keyPreMap $ fmap (normalizeMap . (<> constMap zeroMap)) (FL.foldByKeyMap zeroFillSummedMapFld)
  where
    keyPreMap ((o, k), n) = (o, (k, n))
    sum' mm = FL.fold FL.sum $ fmap (FL.fold (FL.premap (view wgtLens) FL.sum)) mm
    normalizeMap mm = let mmSum = sum' mm in fmap (fmap (over wgtLens (/ mmSum))) mm
{-# INLINEABLE normalizedTableMapFld #-}


tableProductL' ::  forall outerK a b w .
                   (Ord outerK, BRK.FiniteSet a, Ord a, BRK.FiniteSet b, Ord b, Monoid w)
               => (Map a w -> Map b w -> Map (a, b) w)
               -> Map outerK (Map a w)
               -> Map outerK (Map b w)
               -> [w]
tableProductL' innerProduct ma mb = snd <$> tableProductL innerProduct ma mb
{-# INLINEABLE tableProductL' #-}

tableProductL ::  forall outerK a b w .
                  (Ord outerK, BRK.FiniteSet a, Ord a, BRK.FiniteSet b, Ord b, Monoid w)
              => (Map a w -> Map b w -> Map (a, b) w)
              -> Map outerK (Map a w)
              -> Map outerK (Map b w)
              -> [((outerK, a, b), w)]
tableProductL innerProduct ma mb = unNestTableProduct $ tableProduct innerProduct ma mb
{-# INLINEABLE tableProductL #-}

-- This is not symmetric. Table a is used for weights on outer key
-- Also, this treats missing entries as 0
tableProduct :: forall outerK a b w .
                (Ord outerK, BRK.FiniteSet a, Ord a, BRK.FiniteSet b, Ord b, Monoid w)
             => (Map a w -> Map b w -> Map (a, b) w)
             -> Map outerK (Map a w)
             -> Map outerK (Map b w)
             -> Map outerK (Map (a, b) w)
tableProduct innerProduct aTableMap bTableMap = MM.merge
                                                (MM.mapMissing whenMissing)
                                                (MM.mapMissing whenMissing)
                                                (MM.zipWithMatched whenMatched)
                                                aTableMap bTableMap
  where
    whenMissing _ _ = zeroMap
    whenMatched _ = innerProduct
{-# INLINEABLE tableProduct #-}

innerProductSum :: (Ord a, Ord b, Eq x, Fractional x) => Map a (Sum x) -> Map b (Sum x) -> Map (a, b) (Sum x)
innerProductSum ma mb = M.fromList [f ae be | ae <- M.toList ma, be <- fracs mb]
      where
        f (a, x) (b, y) = ((a, b), Sum $ getSum x * getSum y)
        fracs :: (Eq x, Fractional x) => Map z (Sum x) -> [(z, Sum x)]
        fracs m = let s = getSum (FL.fold FL.mconcat m) in M.toList $ if s == 0 then m else fmap (Sum . (/ s) . getSum) m
{-# INLINEABLE innerProductSum #-}

data CellWithDensity = CellWithDensity { cwdWgt :: !Double, cwdDensity :: !Double } deriving (Show)
updateWgt :: CellWithDensity -> Double -> CellWithDensity
updateWgt (CellWithDensity _ wd) x = CellWithDensity x wd
{-# INLINE updateWgt #-}

cwdWgtLens :: Lens' CellWithDensity Double --(CellWithDensity -> Double, (Double -> Double) -> CellWithDensity -> CellWithDensity)
cwdWgtLens = lens cwdWgt updateWgt

safeDiv :: Double -> Double -> Double
safeDiv x y = if y /= 0 then x / y else 0

instance Semigroup CellWithDensity where
  (CellWithDensity x xwd) <> (CellWithDensity y ywd) =
    CellWithDensity
    (x + y)
    ((x * xwd + y * ywd) `safeDiv` (x + y))

instance Semigroup CellWithDensity => Monoid CellWithDensity where
  mempty = CellWithDensity 0 0
  mappend = (<>)

updateWeightCWD :: Double -> CellWithDensity -> CellWithDensity
updateWeightCWD x (CellWithDensity wgt pwDensity) = CellWithDensity x (pwDensity * x / wgt)

innerProductCWD :: (Ord a, Ord b) => Map a CellWithDensity -> Map b CellWithDensity -> Map (a, b) CellWithDensity
innerProductCWD ma mb = M.fromList [f ae be | ae <- M.toList ma, be <- forProd mb]
  where
    f (a, CellWithDensity xw xwd) (b, (yw', ywd')) = ((a, b), CellWithDensity (xw * yw') (xwd * ywd'))
    forProd :: Map x CellWithDensity -> [(x, (Double, Double))]
    forProd m =
      let sumWgtFld = FL.premap cwdWgt FL.sum
          wgtdDensityFld = FL.premap (\t -> cwdWgt t * cwdDensity t) FL.sum
          (sumWgts, sumWgtdDensities) = FL.fold ((,) <$> sumWgtFld <*> wgtdDensityFld) m
          g (CellWithDensity x xwd) = if sumWgts == 0 then (x, 0) else (x / sumWgts, xwd / sumWgtdDensities)
      in M.toList $ fmap g m
{-# INLINEABLE innerProductCWD #-}

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

{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE UnicodeSyntax #-}

module BlueRipple.Model.Demographic.EnrichData
  (
    module BlueRipple.Model.Demographic.EnrichData
  )
where

import qualified BlueRipple.Model.Demographic.StanModels as DM

--import qualified BlueRipple.Data.ACS_PUMS as PUMS
import qualified BlueRipple.Data.DemographicTypes as DT

import qualified Control.MapReduce.Simple as MR
import qualified Frames.MapReduce as FMR
import qualified Frames.Streamly.Transform as FST
import qualified Frames.Streamly.InCore as FSI
import qualified Streamly.Prelude as Streamly

import qualified Control.Foldl as FL
import Control.Monad.Catch (throwM, MonadThrow)
import qualified Data.IntMap as IM
import qualified Data.List as L
import qualified Data.Map.Strict as M
import qualified Data.Map.Merge.Strict as MM
import qualified Data.Set as S
import Data.Type.Equality (type (~))
import qualified Control.Monad.Primitive as Prim

import qualified Data.List as List
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V
import qualified Frames as F
import qualified Frames.Melt as F
import qualified Frames.Transform as FT
import qualified Numeric.LinearAlgebra as LA
--import qualified Numeric.NLOPT as NLOPT
--import qualified Numeric
import qualified Data.Vector.Storable as VS

import qualified System.IO.Unsafe as Unsafe ( unsafePerformIO )
import qualified Say as Say
--import BlueRipple.Data.KeyedTables (keyF)



-- produce sums for all given values of a key
-- With a zero sum for given values with no
-- rows matching
-- We do this for each value of kOuter so we can
-- choose what to fold over (e.g., State)
-- and what to match on (e.g., CSR)
desiredSumsFld :: forall kOuter k d .
                     (Ord kOuter, Ord k)
                  => (d -> Int)
                  -> (d -> kOuter)
                  -> (d -> k)
                  -> Set k
                  -> FL.Fold d (Map kOuter (Map k Int))
desiredSumsFld count outerKey innerKey allBs  =
  fmap M.fromList
  $ MR.mapReduceFold
  MR.noUnpack
  (MR.assign outerKey innerAndCount)
  (MR.foldAndLabel reduceFld (,))

  where
    innerAndCount r = (innerKey r, count r)
    zeroMap = M.fromList $ (, 0) <$> S.toList allBs
--    asKeyVal r = (reKey $ F.rgetField @a r, F.rgetField @count r)
    reduceFld :: FL.Fold (k, Int) (Map k Int)
    reduceFld = M.unionWith (+) zeroMap <$> FL.foldByKeyMap FL.sum

rowMajorMapFldInt :: forall d rk ck .
                  (Ord rk, Ord ck)
               => (d -> Int)
               -> (d -> rk)
               -> (d -> ck)
               -> Set rk
               -> Set ck
               -> FL.Fold d (Map (rk, ck) Int)
rowMajorMapFldInt toData rowKey colKey allRowKeysS allColKeysS = fmap (fmap getSum)
                                                                 $ rowMajorMapFld (Sum . toData) rowKey colKey allRowKeysS allColKeysS


rowMajorMapFld :: forall d rk ck b .
                  (Ord rk, Ord ck, Monoid b)
               => (d -> b)
               -> (d -> rk)
               -> (d -> ck)
               -> Set rk
               -> Set ck
               -> FL.Fold d (Map (rk, ck) b)
rowMajorMapFld toData rowKey colKey allRowKeysS allColKeysS = givenMapFld where
  allRowKeysL = S.elems allRowKeysS
  allColKeysL = S.elems allColKeysS
  allRowColKeysL = [(rk, ck) | rk <- allRowKeysL, ck <- allColKeysL]

  dfltMap :: Map (rk, ck) b
  dfltMap = M.fromList $ (, mempty) <$> allRowColKeysL

  asKeyVal :: d -> ((rk, ck), b)
  asKeyVal d = ((rowKey d, colKey d), toData d)

  -- fold the input records to a map keyed by categories and add 0s when things are missing
  givenMapFld :: FL.Fold d (Map (rk, ck) b)
  givenMapFld = M.unionWith (<>) dfltMap <$> FL.premap asKeyVal (FL.foldByKeyMap FL.mconcat)

rowMajorMapTable :: (Ord a, Ord b) => Map (a, b) Int -> Map a (Map b Int)
rowMajorMapTable = M.fromListWith (M.unionWith (+)) .  fmap (\(k, n) -> (fst k, M.singleton (snd k) n)) .  M.toList

totaledTable :: (Ord a, Ord b, Show a) => Map (a, b) Int -> [(Text, Map b Int)]
totaledTable m = mList ++ [("Total", totals)] where
  mAsList = M.toList $ rowMajorMapTable m
  mList = fmap (first show) mAsList
  totals = foldl' (M.unionWith (+)) mempty $ fmap snd mAsList

newtype StencilSumLookupFld as bs
  = StencilSumLookupFld { stencilSumSLookupFold :: as -> FL.FoldM (Either Text) bs [StencilSum Int Int] }

instance Contravariant (StencilSumLookupFld as) where
  contramap f (StencilSumLookupFld g) = StencilSumLookupFld h where
    h a = FL.premapM (pure . f) $ g a

instance Semigroup (StencilSumLookupFld as bs) where
  (StencilSumLookupFld g1) <> (StencilSumLookupFld g2) = StencilSumLookupFld $ \a -> g1 a <> g2 a

instance Monoid (StencilSumLookupFld as bs) where
  mempty = StencilSumLookupFld $ const mempty
  mappend = (<>)

desiredSumMapToLookup :: forall qs outerKs ks .
                         ( ks F.⊆ qs
                         , Show (F.Record ks)
                         , Show (F.Record outerKs)
                         , Ord (F.Record ks)
                         , Ord (F.Record outerKs)

                         )
                      => Map (F.Record outerKs) (Map (F.Record ks) Int)
                      -> StencilSumLookupFld (F.Record outerKs) (F.Record qs)
desiredSumMapToLookup desiredSumsMaps = StencilSumLookupFld dsFldF
  where
    ssFld :: Map (F.Record ks) Int ->  FL.FoldM (Either Text) (F.Record qs) [StencilSum Int Int]
    ssFld m = mapStencilSum getSum <<$>> subgroupStencilSums (Sum <$> m) F.rcast
    errTxt ok = "nearestCountFrameFld: Lookup of desired sums failed for outer key=" <> show ok
    convertFldE :: Either d (FL.FoldM (Either d) e f) -> FL.FoldM (Either d) e f
    convertFldE fldE = case fldE of
      Right fld -> fld
      Left a -> FL.FoldM (\_ _ -> Left a) (Left a) (const $ Left a) -- we prolly only need one of these "Left a". But which?
    dsFldF :: F.Record outerKs -> FL.FoldM (Either Text) (F.Record qs) [StencilSum Int Int]
    dsFldF ok = convertFldE $ maybeToRight (errTxt ok) . fmap ssFld $ M.lookup ok desiredSumsMaps

nearestCountsFrameFld :: forall ks outerKs ds rs .
                         (Ord (F.Record outerKs)
                         , outerKs F.⊆ rs
                         , ks F.⊆ (ks V.++ ds)
                         , FSI.RecVec (outerKs V.++ ks V.++ ds)
                         , (outerKs V.++ (ks V.++ ds)) ~ ((outerKs V.++ ks) V.++ ds)
                         , (ks V.++ ds) F.⊆ rs
                         )
                      => ([StencilSum Int Int] -> FL.FoldM (Either Text) (F.Record (ks V.++ ds)) [F.Record (ks V.++ ds)])
                      -> StencilSumLookupFld (F.Record outerKs) (F.Record ks)
                      -> FL.FoldM (Either Text) (F.Record rs) (F.FrameRec (outerKs V.++ ks V.++ ds))
nearestCountsFrameFld iFldE (StencilSumLookupFld stencilSumsLookupF) =
  FMR.concatFoldM $
  FMR.mapReduceFoldM
  (FMR.generalizeUnpack FMR.noUnpack)
  (FMR.generalizeAssign $ FMR.assignKeysAndData @outerKs @(ks V.++ ds))
  (FMR.ReduceM reduceM)
  where
    reduceM :: Foldable h => F.Record outerKs -> h (F.Record (ks V.++ ds)) -> Either Text (F.FrameRec (outerKs V.++ ks V.++ ds))
    reduceM ok rows = do
      stSums <- FL.foldM (FL.premapM (pure . F.rcast) $ stencilSumsLookupF ok) rows
      F.toFrame . fmap (ok F.<+>) <$> FL.foldM (iFldE stSums) rows

--  (FMR.makeRecsWithKey id $ FMR.ReduceFold $ const $ nearestCountsFrameIFld @a @b @count desiredRowSums cols)
{-
newtype RowMajorKey a b = RowMajorKey (a, b) deriving newtype Show

instance (Eq a, Eq b) => Eq (RowMajorKey a b) where
  (RowMajorKey (a1, b1)) == (RowMajorKey (a2, b2)) = a1 == a2 && b1 == b2

-- compare b first so the ordering will be row major
instance (Ord a, Ord b) => Ord (RowMajorKey a b) where
  compare (RowMajorKey (a1, b1)) (RowMajorKey (a2, b2)) = case compare a1 a2 of
    EQ -> compare b1 b2
    x -> x

rowVal :: RowMajorKey a b -> a
rowVal (RowMajorKey (a, _)) = a

colVal :: RowMajorKey a b -> b
colVal (RowMajorKey (_, b)) = b
-}

nearestCountsFrameIFld :: forall ks ds b .
                          ( Ord (F.Record ks)
                          , ds F.⊆ (ks V.++ ds)
                          , ks F.⊆ (ks V.++ ds)
                          , Monoid b
                          )
                       => ([StencilSum Int Int] -> [Int] -> Either Text [Int])
                       -> (F.Record ds -> b)
                       -> (b -> F.Record ds)
                       -> (b -> Int)
                       -> (Int -> b -> b)
                       -> [StencilSum Int Int]
                       -> FL.FoldM (Either Text) (F.Record (ks V.++ ds)) [F.Record (ks V.++ ds)]
nearestCountsFrameIFld innerComp toMonoid fromMonoid toInt updateInt sums =
  fmap (fmap toRecord)
  $ nearestCountsIFld innerComp (toMonoid . F.rcast) toInt updateInt (F.rcast @ks) sums
  where
    toRecord :: (F.Record ks, b) -> F.Record (ks V.++ ds)
    toRecord (kr, b) = kr F.<+> fromMonoid b

{-
nearestCountsIFld_RC :: forall d rk ck b .
                     (Ord rk, Ord ck, Monoid b)
                     => ([Int] -> [[Int]] -> Either Text [[Int]])
                     -> (d -> b)
                     -> (b -> Int)
                     -> (Int -> b -> b)
                     -> (d -> rk)
                     -> (d -> ck)
                     -> Map rk Int
                     -> Set ck
                     -> FL.FoldM (Either Text) d [(rk, ck, b)]
nearestCountsIFld_RC innerComp toData dataCount updateCount rowKey colKey desiredRowSums colLabels =
  nearestCountsIFld
-}

nearestCountsIFld :: forall d k b .
                     (Ord k, Monoid b)
                  => ([StencilSum Int Int] -> [Int] -> Either Text [Int])
                  -> (d -> b)
                  -> (b -> Int)
                  -> (Int -> b -> b)
                  -> (d -> k)
                  -> [StencilSum Int Int]
                  -> FL.FoldM (Either Text) d [(k, b)]
nearestCountsIFld innerComp toData dataCount updateCount key desiredSums =
  fmap (uncurry zip . second (uncurry mergeInts))
  <$> mapMFold (liftTuple . applyInnerComp)
  $ fmap (second splitInts . L.unzip . M.toList)
  $ FL.premap (\d -> (key d, toData d)) (FL.foldByKeyMap FL.mconcat)
  where
    splitInts :: [b] -> ([Int], [b])
    splitInts bs = (dataCount <$> bs, bs)

    mergeInts :: [Int] -> [b] -> [b]
    mergeInts = zipWith updateCount

--    liftTupleFst :: Applicative t => (t x, y) -> t (x, y)
--    liftTupleFst (tx, y) = (,) <$> tx <*> pure y

    liftTuple :: Applicative t => (x, (t y, z)) -> t (x, (y, z))
    liftTuple (x, (ty, z)) = (\yy zz -> (x, (yy, zz))) <$> ty <*> pure z

    applyInnerComp :: ([k], ([Int], [b])) -> ([k], (Either Text [Int], [b]))
    applyInnerComp (ks, (ns, bs)) = (ks, (innerComp desiredSums ns, bs))

--    rowLabels = M.keysSet desiredSums

--    toRows :: Map k x -> [x]
--    toRows = splitAtAll (S.size colLabels) . M.elems

    mapMFold :: Monad m => (y -> m z) -> FL.Fold x y -> FL.FoldM m x z
    mapMFold f (FL.Fold step init' extract) = FL.FoldM (\x h -> pure $ step x h) (pure init') (f . extract)

--    makeTuple :: rk -> (ck, b) -> (rk, ck, b)
--    makeTuple r (c, b) =  (r, c, b)

--    makeTuples :: [[b]] -> [[(rk, ck, b)]]
--    makeTuples = zipWith (fmap . makeTuple) (S.toList rowLabels) . fmap (zip (S.toList colLabels))

 -- splitAt but keeps going until entire list is split into things of given length and whatever is left
splitAtAll :: Int -> [c] -> [[c]]
splitAtAll n l = reverse $ go l [] where
  go remaining accum =
    if null rest
    then remaining : accum
    else go rest (slice : accum)
    where
      (slice, rest) = List.splitAt n remaining

nearestCountsProp :: [Int] -> [[Int]] -> Either Text [[Int]]
nearestCountsProp dRowSums oCounts = do
  let adjustRow :: Int -> [Int] -> [Int]
      adjustRow desiredSum counts = fmap adjustCell counts
        where rowSum = FL.fold FL.sum counts
              totalDiff = desiredSum - rowSum
              mult :: Double = realToFrac totalDiff /  realToFrac rowSum
              adjustCell n = n + round (mult * realToFrac n)

  pure $ zipWith adjustRow dRowSums oCounts

-- represents various positions in the list of numbers
newtype Stencil a = Stencil { stencilIndexes :: [a] }
  deriving stock Show
  deriving newtype Functor

data StencilSum b a = StencilSum { stPositions :: Stencil a, stSum :: b }
  deriving stock (Show, Functor)
--  deriving newtype (Functor)

mapStencilSum :: (b -> c) -> StencilSum b a -> StencilSum c a
mapStencilSum f (StencilSum ps x) = StencilSum ps (f x)

mapStencilPositions :: (Stencil a -> Stencil a) -> StencilSum b a -> StencilSum b a
mapStencilPositions f (StencilSum sa x) = StencilSum (f sa) x

rowStencil :: Int -> Int -> Stencil Int
rowStencil cols row = Stencil $ (+ (row * cols)) <$> [0..cols - 1]

colStencil :: Int -> Int -> Int -> Stencil Int
colStencil rows cols col = Stencil $ fmap (\n -> col + cols * n) [0..rows - 1]

transp :: [[a]] -> [[a]]
transp = go [] where
  go x [] = fmap reverse x
  go [] (r : rs) = go (fmap (: []) r) rs
  go x (r :rs) = go (List.zipWith (:) r x) rs

subgroupStencils :: Ord a => (r -> a) -> FL.Fold r (Map a (Stencil Int))
subgroupStencils key = mapFromList <$> FL.list
  where
    mapFromList = fmap Stencil . foldl' (\m (k, r) -> M.insertWith (<>) (key r) [k] m) mempty . zip [0..]

subgroupStencilUnion :: forall a b c . (Ord a, Monoid b) => Show a => Map a b -> Map a (Stencil c) -> Either Text [StencilSum b c]
subgroupStencilUnion sumMap = fmap M.elems . MM.mergeA whenMissingStencil whenMissingSum whenMatched sumMap
  where
    whenMatchedF :: a -> b -> Stencil c -> Either Text (StencilSum b c)
    whenMatchedF _ desiredSum stencil = Right $ StencilSum stencil desiredSum
    whenMatched = MM.zipWithAMatched whenMatchedF
    whenMissingSumF :: a -> Stencil c -> Either Text (StencilSum b c)
    whenMissingSumF _ stencil = Right $ StencilSum stencil (mempty :: b)
    whenMissingSum = MM.traverseMissing whenMissingSumF
    whenMissingStencilF :: a -> b -> Either Text (StencilSum b c)
    whenMissingStencilF k _ = Left $ "Missing stencil for  group present in sum map (key=" <> show k <> ")"
    whenMissingStencil = MM.traverseMissing whenMissingStencilF

subgroupStencilSums :: (Show a, Ord a, Monoid b) => Map a b -> (r -> a) -> FL.FoldM (Either Text) r [StencilSum b Int]
subgroupStencilSums sumMap key = FMR.postMapM (subgroupStencilUnion sumMap) $ FL.generalize (subgroupStencils key)


nearestCountsKL_RC :: [Int] -> [[Int]] -> Either Text [[Int]]
nearestCountsKL_RC rowSums oCountsI = do
    let rows = length oCountsI
    cols <- maybeToRight "Empty list of counts given to nearestCountsKL_RC" $ viaNonEmpty (length . head) oCountsI
    let rowSumStencils = zipWith (\j s -> (StencilSum (rowStencil cols j) s)) [0..(rows - 1)] rowSums
        colSums = fmap (FL.fold FL.sum) $ transp oCountsI
        colSumStencils = zipWith (\k s -> (StencilSum (colStencil rows cols k) s)) [0..(cols - 1)] colSums
    splitAtAll cols <$> nearestCountsKL (rowSumStencils <> colSumStencils) (concat oCountsI)

removeFromListAtIndexes :: [Int] -> [a] -> [a]
removeFromListAtIndexes is ls =
  snd
  $ foldl'
  (\(numRemoved, curList) ir -> let (h, t) = L.splitAt (ir - numRemoved) curList in (numRemoved + 1, h <> L.drop 1 t))
  (0, ls)
  is

fillListAtIndexes :: a -> [Int] -> [a] -> [a]
fillListAtIndexes fill is ls = foldl' (\curList ir -> let (h, t) = splitAt ir curList in h <> (fill : t)) ls is

removeFromStencilAtIndexes :: [Int] -> Stencil Int -> Stencil Int
removeFromStencilAtIndexes is (Stencil ks) = Stencil ms where
  stencilIM = IM.union (IM.fromList $ fmap (,1 :: Int) ks) (IM.fromList $ zip [0..L.maximum ks] $ repeat 0)
  ms = fmap fst $ filter ((/= 0) . snd) $ zip [0..] $ removeFromListAtIndexes is $ fmap snd $ IM.toList stencilIM

-- Hand rolled solver
-- Linearize equations solving for the stationary point
-- of minimizing KL divergence plus lagrange multipliers for
-- row and column sums
-- then iterate linearized solution until delta is small.
-- Requires SVD because the multipliers sometimes only show up as sums. More in notes.
nearestCountsKL :: [StencilSum Int Int] -> [Int] -> Either Text [Int]
nearestCountsKL stencilSumsI oCountsI = do
  let toDouble = realToFrac @Int @LA.R
      nV = LA.fromList $ fmap toDouble oCountsI
      !_ = Unsafe.unsafePerformIO $ Say.say $ "\nN=\n" <> show nV
      !_ = Unsafe.unsafePerformIO $ Say.say $ "\nStencils=\n" <> show stencilSumsI
      -- Remove zeroes, both from N and all of any stencils with zero sum
      removedIndexes =
        S.toList
        $ S.fromList $
        fmap fst (filter ((== 0) . snd) (zip [0..] $ VS.toList nV))
        <> concatMap (stencilIndexes . stPositions) (filter ((== 0). stSum) stencilSumsI)
      nV' = VS.fromList $ removeFromListAtIndexes removedIndexes $ VS.toList nV
      stencilSumsI' = mapStencilPositions (removeFromStencilAtIndexes removedIndexes) <$> filter ((/= 0) . stSum) stencilSumsI
      stencilSumsD' = fmap (mapStencilSum toDouble) stencilSumsI'
      nSums' = length stencilSumsI'
      nProbs' = LA.size nV'

      -- we should check here if the constraints have a solution

      fullFirstGuessV = VS.concat [nV', VS.replicate nSums' 0]
      nMax = 10
      absTol = 0.0001
  solL <- fillListAtIndexes 0 removedIndexes . fmap round . take nProbs' . VS.toList
    <$> converge (updateGuess nProbs') (newDelta stencilSumsD' nV') checkNewGuessInf nMax absTol fullFirstGuessV
--  let !_ = Unsafe.unsafePerformIO $ Say.say $ "\n(bootstrap) sol=\n" <> show solL
  pure solL

-- NB: delta itself will not be 0 because it can be anything in places where N and Q are 0.
-- So rather than check the smallness of delta, we check the smallness of the actual change in
-- probabilities, lambda & gamma
checkNewGuess :: LA.Vector LA.R -> LA.Vector LA.R -> Double
checkNewGuess xV yV = sqrt (LA.norm_2 (xV - yV)) / realToFrac (VS.length xV)

-- maximum of the absolute differences
checkNewGuessInf :: LA.Vector LA.R -> LA.Vector LA.R -> Double
checkNewGuessInf xV yV = VS.maximum $ VS.map abs (xV - yV)

updateGuess :: Int -> LA.Vector LA.R -> LA.Vector LA.R -> LA.Vector LA.R
updateGuess nProbs g d = VS.concat [qV', mV']
  where (qV, mV) = VS.splitAt nProbs g
        (dqV, dmV) = VS.splitAt nProbs d
        fixdU x
          | x > 0.25 = 0.25
          | x < -(0.25) = -(0.25)
          | otherwise = x
        qV' = VS.zipWith (\qU dU -> qU * (1 + dU)) qV dqV
        mV' = mV + dmV

converge :: (LA.Vector LA.R -> LA.Vector LA.R -> LA.Vector LA.R)
         -> (LA.Vector LA.R -> Either Text (LA.Vector LA.R))
         -> (LA.Vector LA.R -> LA.Vector LA.R -> Double)
         -> Int -> Double -> LA.Vector LA.R -> Either Text (LA.Vector LA.R)
converge updGuess deltaF tolF maxN tol guessV = go 0 guessV Nothing
  where
    go n gV deltaVM = do
      when ((n :: Int) >= maxN) $ Left $ "Max iterations (" <> show maxN <> ") exceeded in nearestCountsKL"
      let gV' = maybe guessV (updGuess gV) deltaVM
      if n == 0 || (tolF gV gV' >= tol)
        then (do
                 deltaV <- deltaF gV'
                 go (n + 1) gV' (Just deltaV)
             )
        else pure gV'

mMatrix :: Int -> [Stencil Int] -> LA.Matrix LA.R
mMatrix nProbs stencils = LA.assoc (nStencils, nProbs) 0 $ mconcat $ fmap f $ zip [0..nStencils] stencils
  where
    nStencils = length stencils
    f (n, Stencil ks) = (\k -> ((n, k), 1)) <$> ks

rhsUV :: [Stencil Int] -> LA.Vector LA.R -> LA.Vector LA.R -> LA.Vector LA.R -> LA.Vector LA.R
rhsUV stencils nV qV mV = VS.zipWith rhsuF nDivq mShiftV
  where
    nProbs = VS.length nV
    safeDiv x y = if y == 0 then 1 else x / y
    nDivq = VS.zipWith safeDiv nV qV
    nSum = VS.sum nV
    qSum = VS.sum qV
    mM = mMatrix nProbs stencils
    mShiftV = LA.tr mM LA.#> mV
    rhsuF x y = x - (nSum / qSum) - y

rhsUV0 :: [Stencil Int] -> LA.Vector LA.R -> LA.Vector LA.R -> LA.Vector LA.R
rhsUV0 stencils nV qV = rhsUV stencils nV qV (VS.replicate (length stencils) 0)

newDelta :: [StencilSum Double Int] -- desired sums
         -> LA.Vector LA.R -- N
         -> LA.Vector LA.R -- current guess, then multipliers
         -> Either Text (LA.Vector LA.R) -- delta, in same format
newDelta stencilSums nV gV = do
  let nProbs = VS.length nV
      nStencils = length stencilSums
      (qV, mV) = VS.splitAt nProbs gV
      nSum = VS.sum nV
      qSum = VS.sum qV
      invnSum = 1 / nSum
      safeDiv x y = if y == 0 then 1 else x / y
      nDivq = VS.zipWith safeDiv nV qV
      ul = LA.diag nDivq - (LA.scale (nSum / (qSum * qSum)) $ LA.fromRows $ replicate nProbs qV)
      multM = mMatrix nProbs $ fmap stPositions stencilSums
      ll = LA.scale invnSum $ LA.fromRows $ fmap (* qV) $ LA.toRows multM
      lr = LA.konst 0 (nStencils, nStencils)
      m = (ul LA.||| LA.tr multM ) LA.=== (ll LA.||| lr) -- LA.fromBlocks [[ul, ur],[ll, 0]]
      stencilSumsV = multM LA.#> qV
      stencilSumDiffsV = LA.fromList (fmap stSum stencilSums) - stencilSumsV
      rhsV = LA.vjoin [rhsUV (fmap stPositions stencilSums) nV qV mV
                      , LA.scale invnSum stencilSumDiffsV
                      ]
      rhsM = LA.fromColumns [rhsV]
      solM = LA.linearSolveSVD m rhsM
  solV :: LA.Vector LA.R <- maybeToRight "empty list of solutions??" $ viaNonEmpty head $ LA.toColumns solM
  pure solV

subVectors :: Int -> Int -> LA.Vector LA.R -> (LA.Vector LA.R, LA.Vector LA.R, LA.Vector LA.R)
subVectors rows cols v = (mV, lambdaV, gammaV) where
  nSize = rows * cols
  mV = LA.subVector 0 nSize v
  lambdaV = LA.subVector nSize rows v
  gammaV = LA.subVector (nSize + rows) cols v


enrichFrameFromBinaryModel :: forall t count m g ks rs a .
                              (rs F.⊆ rs
                              , ks F.⊆ rs
                              , FSI.RecVec rs
                              , FSI.RecVec (t ': rs)
                              , V.KnownField t
                              , V.Snd t ~ a
                              , V.KnownField count
                              , F.ElemOf rs count
                              , Integral (V.Snd count)
                              , MonadThrow m
                              , Prim.PrimMonad m
                              , F.ElemOf rs DT.PopPerSqMile
                              , Ord g
                              , Show g
                              , Ord (F.Record ks)
                              , Show (F.Record rs)
                              , Ord a
                              )
                           => DM.ModelResult g ks
                           -> (F.Record rs -> g)
                           -> a
                           -> a
                           -> F.FrameRec rs
                           -> m (F.FrameRec (t ': rs))
enrichFrameFromBinaryModel mr getGeo aTrue aFalse = enrichFrameFromModel @count smf where
  smf r = do
    let smf' x = SplitModelF $ \n ->  let nTrue = round (realToFrac n * x)
                                      in M.fromList [(FT.recordSingleton @t aTrue, nTrue)
                                                    , (FT.recordSingleton @t aFalse, n - nTrue)
                                                    ]
    pTrue <- DM.applyModelResult mr (getGeo r) r
    pure $ smf' pTrue

-- | produce a map of splits across type a. Doubles must sum to 1.
newtype  SplitModelF n a = SplitModelF { splitModelF :: n -> Map a n }

splitRec :: forall count rs cks . (V.KnownField count
                                  , F.ElemOf rs count
                                  )
         => SplitModelF (V.Snd count) (F.Record cks) -> F.Record rs -> [F.Record (cks V.++ rs)]
splitRec (SplitModelF f) r =
  let makeOne (colRec, n) = colRec F.<+> F.rputField @count n r
      splits = M.toList $ f $ F.rgetField @count r
  in makeOne <$> splits

enrichFromModel :: forall count ks rs cks . (ks F.⊆ rs
                                            , V.KnownField count
                                            , F.ElemOf rs count
                                            )
                      => (F.Record ks -> Either Text (SplitModelF (V.Snd count) (F.Record cks))) -- ^ model results to apply
                      -> F.Record rs -- ^ record to enrich
                      -> Either Text [F.Record (cks V.++ rs)]
enrichFromModel modelResultF recordToEnrich = flip (splitRec @count) recordToEnrich <$> modelResultF (F.rcast recordToEnrich)

data ModelLookupException = ModelLookupException Text deriving stock (Show)
instance Exception ModelLookupException


-- this should build the new frame in-core and in a streaming manner,
-- not needing to build the entire list before placing in-core
enrichFrameFromModel :: forall count ks rs cks m . (ks F.⊆ rs
                                                   , FSI.RecVec rs
                                                   , FSI.RecVec (cks V.++  rs)
                                                   , V.KnownField count
                                                   , F.ElemOf rs count
                                                   , MonadThrow m
                                                   , Prim.PrimMonad m
                                                   )
                     => (F.Record ks -> Either Text (SplitModelF (V.Snd count) (F.Record cks))) -- ^ model results to apply
                     -> F.FrameRec rs -- ^ record to enrich
                     -> m (F.FrameRec (cks V.++ rs))
enrichFrameFromModel modelResultF =  FST.transform f where
  f = Streamly.concatMapM g
  g r = do
    case enrichFromModel @count modelResultF r of
      Left err -> throwM $ ModelLookupException err
      Right rs -> pure $ Streamly.fromList rs



{-

nearestCountsKL_NL1 :: [Int] -> [[Int]] -> Either Text [[Int]]
nearestCountsKL_NL1 dRowSumsI oCountsI = do
  let toDouble = realToFrac @Int @LA.R
      nM :: LA.Matrix LA.I = LA.fromRows $ fmap (LA.fromList . fmap fromIntegral) oCountsI
      nVRaw = LA.fromList (toDouble <$> mconcat oCountsI)
      nSum = VS.sum nVRaw
      nV = LA.scale (1 / nSum) nVRaw
      aV = let avRaw =  LA.fromList $ fmap toDouble dRowSumsI in LA.scale (1 / VS.sum avRaw) avRaw
      (rows, cols) = LA.size nM
--      ul = LA.diag nDivq - (LA.scale (nSum / (qSum * qSum)) $ LA.fromRows $ replicate (rows * cols) qV)
      indexes l = l `divMod` cols
      mGuessV = LA.scale (1 / nSum) $ LA.fromList $ mconcat $ zipWith (\l rs -> fmap ((* rs) . toDouble) l) oCountsI (VS.toList aV)

      rowSums :: LA.Vector LA.R -> LA.Vector LA.R
      rowSums = LA.fromList . fmap VS.sum . LA.toRows . LA.reshape cols
      colSums :: LA.Vector LA.R -> LA.Vector LA.R
      colSums = LA.fromList . fmap VS.sum . LA.toColumns . LA.reshape cols
--      nRowSums = rowSums cols nV
      nColSumsV = colSums nV
      nSize = rows * cols

      kl :: LA.Vector LA.R -> Double
      kl mV = result where
        klTermF x y = if y == 0 then 0 else x * Numeric.log (x / y)
--        logNMV = VS.zipWith klTermF nV mV
        result =  Numeric.log (VS.sum mV) + VS.sum (VS.zipWith klTermF nV mV)
        !_ = Unsafe.unsafePerformIO $ Say.say $ "\nmV=" <> show mV <> "\nkl=" <> show result
      -- our vector is components of M in row major order, followed by Lagrange multipliers
      subVectors :: LA.Vector LA.R -> (LA.Vector LA.R, LA.Vector LA.R, LA.Vector LA.R)
      subVectors v = (mV, lambdaV, gammaV) where
        mV = LA.subVector 0 nSize v
        lambdaV = LA.subVector nSize rows v
        gammaV = LA.subVector (nSize + rows) cols v

      f :: LA.Vector LA.R -> Double
      f v = result
        where
          (mV, lambdaV, gammaV) = subVectors v
          rowSumErrs = rowSums mV - aV
          colSumErrs = colSums mV - nColSumsV
          mult3 x y z = x * y * z
          result =  0.5 * kl mV
                    + VS.sum (VS.zipWith3 mult3 lambdaV rowSumErrs rowSumErrs)
                    + VS.sum (VS.zipWith3 mult3 gammaV colSumErrs colSumErrs)
--          !_ = Unsafe.unsafePerformIO $ Say.say $ "\nmV=" <> show mV <> "\nlV=" <> show lambdaV <> "; gV=" <> show gammaV <> "\nf=" <> show result

      fGradients :: LA.Vector LA.R -> LA.Vector LA.R
      fGradients v = VS.concat [mGradV, lambdaGradV, gammaGradV] where
        (mV, lambdaV, gammaV) = subVectors v
        sumM = VS.sum mV
        rowSumErrors = LA.scale 2 $ rowSums mV - aV
        colSumErrors = LA.scale 2 $ colSums mV - nColSumsV
        klGrad x y = if y == 0 || x == 0 then (1 / sumM) - 1 else (1 / sumM) - (x / y)
        klGrads = VS.zipWith klGrad nV mV
        mLambdaGradV = VS.zipWith (*) lambdaV rowSumErrors
        mGammaGradV = VS.zipWith (*) gammaV colSumErrors
        mGradF l = klGrads VS.! l + mLambdaGradV VS.! j + mGammaGradV VS.! k
          where
            (j, k) = indexes l
        mGradV = VS.generate nSize mGradF
        lambdaGradV = rowSums mV - aV
        gammaGradV = colSums mV - nColSumsV
        !_ = Unsafe.unsafePerformIO $ Say.say $ "\nmV=" <> show mV <> "\nlV=" <> show lambdaV <> "; gV=" <> show gammaV
             <> "\nmGradV=" <> show mGradV <> "\nlGradV=" <> show lambdaGradV <> "; gGradV=" <> show gammaGradV

      nlStarting = VS.concat [VS.drop 1 mGuessV, VS.replicate (rows + cols) 0] -- we start at n with 0 multipliers
      nlObjectiveD v = (f v, fGradients v)
      nlBounds = [NLOPT.LowerBounds $ VS.concat [VS.replicate nSize 0, VS.replicate (rows + cols) $ negate 100]
                 , NLOPT.UpperBounds $ VS.concat [VS.replicate nSize 1, VS.replicate (rows + cols) 100]
                 ]
      nlStop = NLOPT.ParameterRelativeTolerance 0.01 :| [NLOPT.MaximumEvaluations 100]
--      nlAlgo = NLOPT.TNEWTON nlObjectiveD Nothing --nlBounds [] []
--      nlAlgo = NLOPT.SLSQP nlObjectiveD nlBounds [] []
      nlAlgo = NLOPT.NELDERMEAD f nlBounds Nothing
      nlProblem = NLOPT.LocalProblem (fromIntegral $ nSize + rows + cols) nlStop nlAlgo
      nlSol = NLOPT.minimizeLocal nlProblem nlStarting
  case nlSol of
    Left result -> Left $ show result <> "\n" <> "rowSums=\n" <> show dRowSumsI <> "\nN=\n" <> show oCountsI
    Right solution -> do
      let sol = splitAtAll cols . fmap (round @_ @Int) . LA.toList . LA.scale nSum $ LA.subVector 0 nSize (NLOPT.solutionParams solution)
      Left $ "Succeeded! rowSums=\n" <> show dRowSumsI <> "\nN=\n" <> show oCountsI <> "\nM=\n" <> show sol

nearestCountsKL_NL2 :: [Int] -> [[Int]] -> Either Text [[Int]]
nearestCountsKL_NL2 dRowSumsI oCountsI = do
  let toDouble = realToFrac @Int @LA.R
      nM :: LA.Matrix LA.I = LA.fromRows $ fmap (LA.fromList . fmap fromIntegral) oCountsI
      nVRaw = LA.fromList (toDouble <$> mconcat oCountsI)
      nSum = VS.sum nVRaw
      nV = LA.scale (1 / nSum) nVRaw
      aV = let avRaw =  LA.fromList $ fmap toDouble dRowSumsI in LA.scale (1 / VS.sum avRaw) avRaw
      (rows, cols) = LA.size nM
--      ul = LA.diag nDivq - (LA.scale (nSum / (qSum * qSum)) $ LA.fromRows $ replicate (rows * cols) qV)
      indexes l = l `divMod` cols
      mGuessV = LA.scale (1 / nSum) $ LA.fromList $ mconcat $ zipWith (\l rs -> fmap ((* rs) . toDouble) l) oCountsI (VS.toList aV)


      rowSums :: LA.Vector LA.R -> LA.Vector LA.R
      rowSums = LA.fromList . fmap VS.sum . LA.toRows . LA.reshape cols
      colSums :: LA.Vector LA.R -> LA.Vector LA.R
      colSums = LA.fromList . fmap VS.sum . LA.toColumns . LA.reshape cols
--      nRowSums = rowSums cols nV
      nColSumsV = colSums nV
      nSize = rows * cols

      kl :: LA.Vector LA.R -> Double
      kl mV = result where
        safeLogRatio x y = if y == 0 then 0 else x * Numeric.log (x / y)
        logNMV = VS.zipWith safeLogRatio nV mV
        result =  Numeric.log (VS.sum mV) + VS.sum (VS.zipWith (*) nV logNMV)
        !_ = Unsafe.unsafePerformIO $ Say.say $ "\nmV=" <> show mV <> "\nkl=" <> show result

      nlCBounds = [NLOPT.LowerBounds $ VS.replicate nSize 0, NLOPT.UpperBounds $ VS.replicate nSize 1]

      klGradients :: LA.Vector LA.R -> LA.Vector LA.R
      klGradients mV = result where
        mSum = VS.sum mV
        g x y = if y == 0 then 0 else (1 / mSum) - x / y -- M_jk should be zero if N_jk is, and then this ratio is 1. Any other M_jk == 0 is a real error
        result = VS.zipWith g nV mV
--        !_ = Unsafe.unsafePerformIO $ Say.say $ "\nmV=" <> show mV <> "\ngrad(kl)=" <> show result


      rowSumConstraint j v = (rowSums v) VS.! j - aV VS.! j
      colSumConstraint k v = (colSums v) VS.! k  - nColSumsV VS.! k
      sumConstraint v = VS.sum v - 1
      toNLEC f = NLOPT.EqualityConstraint (NLOPT.Scalar f) 0.01
--      toNLECD f = NLOPT.EqualityConstraintD (NLOPT.Scalar f) 0.01
{-
      rowSumConstraintD j v =  VS.generate nSize (\l -> if fst (indexes l) == j then 1 else 0)
      colSumConstraintD k v = VS.generate nSize (\l -> if snd (indexes l) == k then 1 else 0)

      nlScalarConstraintsD = fmap ((\n -> toNLEC $ \v -> (rowSumConstraint n v, rowSumConstraintD n v))) [0..(rows - 1)]
                            <> fmap ((\n -> toNLEC $ \v -> (colSumConstraint n v, colSumConstraintD n v))) [0..(cols - 1)]

-}
      nlScalarConstraints = [toNLEC sumConstraint]
                            <> fmap (toNLEC . rowSumConstraint) [0..(rows - 1)]
                            <> fmap (toNLEC . colSumConstraint) [0..(cols - 1)]


{-
      rowSumConstraint v size = (rowSums v - aV, LA.konst 0 (nSize, rows))
  {-                               , LA.tr
                                  $ LA.reshape rows
                                  $ VS.generate (rows * nSize) (\q -> let (row, l) = divMod rows q in if fst (indexes l) == row then 1 else 0))
-}
      colSumConstraint v size = (colSums v - nColSums, LA.konst 0 (nSize, cols))
{-                                , LA.tr
                                  $ LA.reshape cols
                                  $ VS.generate (cols * nSize) (\q -> let (col, l) = divMod cols q in if snd (indexes l) == col then 1 else 0))
-}

      nlRowSumConstraint = NLOPT.EqualityConstraint (NLOPT.Vector (fromIntegral rows) rowSumConstraint) 0.001
      nlColSumConstraint = NLOPT.EqualityConstraint (NLOPT.Vector (fromIntegral cols) colSumConstraint) 0.001

      nlVecConstraints = [nlRowSumConstraint, nlColSumConstraint]
-}
      nlCObjectiveD v = (kl v, klGradients v)
      nlCObjective v = kl v
      nlStop = NLOPT.ParameterRelativeTolerance 0.01 :| [NLOPT.MaximumEvaluations 100]
--      nlCAlgo = NLOPT.SLSQP nlCObjectiveD nlCBounds [] nlScalarConstraintsD
--      nlCAlgo = NLOPT.SLSQP nlCObjectiveD nlCBounds [] nlScalarConstraintsD
      nlCAlgo = NLOPT.COBYLA nlCObjective nlCBounds [] nlScalarConstraints Nothing
      nlCProblem = NLOPT.LocalProblem (fromIntegral nSize) nlStop nlCAlgo
      nlCSol = NLOPT.minimizeLocal nlCProblem mGuessV
  case nlCSol of
    Left result -> Left $ show result
    Right solution -> do
      let solV = NLOPT.solutionParams solution
          sol = splitAtAll cols . fmap round . LA.toList . LA.scale nSum $ solV
      Left
        $ "Succeeded! (" <> show (NLOPT.solutionResult solution) <> ")\n"
        <> "rowSums=\n" <> show dRowSumsI <> "\nN=\n" <> show oCountsI <> "\nM=\n" <> show sol
        <> "\nsolV=" <> show solV
        <> "\naV=" <> show aV
        <> "\nsumConstraint=" <> show (sumConstraint solV)
        <> "\nrowSumConstraints=" <> show ((fmap ((flip rowSumConstraint) solV)) [0..(rows - 1)])
        <> "\ncolSumConstraints=" <> show ((fmap ((flip colSumConstraint) solV)) [0..(cols - 1)])

--      Right . splitAtAll cols . fmap round . LA.toList . LA.scale nSum $ LA.subVector 0 nSize (NLOPT.solutionParams solution)

{-

-}

-}

{-
-- given counts N_jk, as a list of (Vector Int) rows in givenCounts
-- desired row_sums B_j, as a Vector Int in desiredSumOfRows
-- provide counts M_jk s.t
-- 1. columns sums are the same
-- 2. row sums are B_j
-- 3. Given 1 and 2, the sum-squared differences of M_jk and N_jk are minimized
nearestCounts :: LA.Vector Int -> [LA.Vector Int] -> Either Text [LA.Vector Int]
nearestCounts desiredSumOfRow givenCounts = do
  let toDbl = realToFrac @Int @Double
  let desiredSumOfRowD = VS.map toDbl desiredSumOfRow
      givenCountsD = VS.map toDbl <$> givenCounts
      rows = length givenCountsD
      dsRows = LA.size desiredSumOfRowD
  _ <- if  dsRows /= rows
       then Left $ show dsRows <> " row-sums given but matrix of given counts has " <> show rows
       else Right ()
  let topRow = LA.fromList $ 1 : replicate rows (negate $ 1 / realToFrac rows)
      otherRow j = LA.assoc (rows + 1) 0 [(0, realToFrac @Int @Double 1), (j + 1, realToFrac @Int @Double 1)]
      otherRows = fmap otherRow [0..(rows - 1)]
      m =  LA.fromRows $ topRow : otherRows
      rowSumDiffs j = VS.sum (givenCountsD List.!! j) - (desiredSumOfRowD `LA.atIndex` j)
      rhs = LA.fromList $ 0 : fmap rowSumDiffs [0..(rows - 1)]
      luP = LA.luPacked m
      lambdaGamma = LA.flatten $ LA.luSolve luP $ LA.fromColumns [rhs]
      lambda = lambdaGamma `LA.atIndex` 0
      gamma = LA.fromList . drop 1 . LA.toList $ lambdaGamma
      new rIndex val = val - lambda + gamma `LA.atIndex` rIndex
      newRow (rIndex , r) = VS.map (round . new rIndex) r
  pure $ newRow <$> zip [0..(rows - 1)] givenCountsD

-}

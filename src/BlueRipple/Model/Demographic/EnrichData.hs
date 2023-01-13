{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
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

import qualified BlueRipple.Data.ACS_PUMS as PUMS
import qualified BlueRipple.Data.DemographicTypes as DT

import qualified Control.MapReduce.Simple as MR
import qualified Frames.MapReduce as FMR
import qualified Frames.Streamly.Transform as FST
import qualified Frames.Streamly.InCore as FSI
import qualified Streamly.Prelude as Streamly

import qualified Control.Foldl as FL
import Control.Monad.Catch (throwM, MonadThrow)
import qualified Data.Map as M
import qualified Data.Set as S
import Data.Type.Equality (type (~))
import qualified Control.Monad.Primitive as Prim

import qualified Data.List as List
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V
import qualified Frames as F
import qualified Frames.Melt as F

-- produce rowSums
desiredRowSumsFld :: forall a count ks b rs .
                     (ks F.⊆ rs
                     , F.ElemOf rs a
                     , F.ElemOf rs count
                     , V.Snd count ~ Int
                     , V.KnownField count
                     , V.KnownField a
                     , Ord (F.Record ks)
                     , Ord b
                     , F.ElemOf [a, count] a
                     , F.ElemOf [a, count] count
                     )
                  => Set b -> (V.Snd a -> b) -> FL.Fold (F.Record rs) (Map (F.Record ks) (Map b Int))
desiredRowSumsFld allBs reKey =
  fmap M.fromList
  $ MR.mapReduceFold
  MR.noUnpack
  (MR.assign (F.rcast @ks) (F.rcast @[a, count]))
  (MR.foldAndLabel reduceFld (,))

  where
    zeroMap = M.fromList $ (, 0) <$> S.toList allBs
    asKeyVal r = (reKey $ F.rgetField @a r, F.rgetField @count r)
    reduceFld :: FL.Fold (F.Record [a, count]) (Map b Int)
    reduceFld = M.unionWith (+) zeroMap <$> FL.premap asKeyVal (FL.foldByKeyMap FL.sum)

nearestCountsFrameFld :: forall count a b ks rs .
                         (F.ElemOf rs a
                         , F.ElemOf rs b
                         , F.ElemOf rs count
                         , V.Snd count ~ Int
                         , Ord (V.Snd a)
                         , Ord (V.Snd b)
                         , V.KnownField count
                         , V.KnownField a
                         , V.KnownField b
                         , F.ElemOf [a, b, count] a
                         , F.ElemOf [a, b, count] b
                         , F.ElemOf [a, b, count] count
                         , Ord (F.Record ks)
                         , ks F.⊆ rs
                         , FSI.RecVec (ks V.++ [a, b, count])
                         )
                      => (F.Record ks -> Either Text (Map (V.Snd a) Int)) -- desired row sums, probably from a map produced by function above
                      -> Set (V.Snd b) -- column category in full in case some are missing from the frame
                      -> FL.FoldM (Either Text) (F.Record rs) (F.FrameRec (ks V.++ [a, b, count]))
nearestCountsFrameFld desiredRowSumsLookup cols =
  FMR.concatFoldM $
  FMR.mapReduceFoldM
  (FMR.generalizeUnpack FMR.noUnpack)
  (FMR.generalizeAssign $ FMR.assignKeysAndData @ks @[a, b, count])
  (MR.ReduceFoldM reduceM)
  where
    convertFldE :: Either d (FL.Fold e f) -> FL.FoldM (Either d) e f
    convertFldE fldE = case fldE of
      Right fld -> FL.generalize fld
      Left a -> FL.FoldM (\_ _ -> Left a) (Left a) (const $ Left a) -- we prolly only need one of these "Left a". But which?
    reduceM rk =
      let eFld = (\x -> nearestCountsFrameIFld @a @b @count x cols) <$> desiredRowSumsLookup rk
      in F.toFrame . fmap (rk V.<+>) <$> convertFldE eFld

--  (FMR.makeRecsWithKey id $ FMR.ReduceFold $ const $ nearestCountsFrameIFld @a @b @count desiredRowSums cols)

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

nearestCountsFrameIFld :: forall aC bC count .
                          (V.Snd count ~ Int
                          , Ord (V.Snd aC)
                          , Ord (V.Snd bC)
                          , V.KnownField aC
                          , V.KnownField bC
                          , V.KnownField count
                          , F.ElemOf [aC, bC, count] aC
                          , F.ElemOf [aC, bC, count] bC
                          , F.ElemOf [aC, bC, count] count
                          )
                       => Map (V.Snd aC) Int
                       -> Set (V.Snd bC)
                       -> FL.Fold (F.Record [aC, bC, count]) [F.Record [aC, bC, count]]
nearestCountsFrameIFld desiredRowSums colLabels = concat . makeRecords . newRows <$> givenMapFld where
  allAs = M.keys desiredRowSums
  allBs = S.elems colLabels
  allABs = [RowMajorKey (a, b) | b <- allBs, a <- allAs]

  zeroMap :: Map (RowMajorKey (V.Snd aC) (V.Snd bC)) Int
  zeroMap = M.fromList $ (, 0) <$> allABs

  asKeyVal :: F.Record [aC, bC, count] -> (RowMajorKey (V.Snd aC) (V.Snd bC), Int)
  asKeyVal r = (RowMajorKey (F.rgetField @aC r, F.rgetField @bC r), F.rgetField @count r)

  -- fold the input records to a map keyed by categories and add 0s when things are missing
  givenMapFld :: FL.Fold (F.Record [aC, bC, count]) (Map (RowMajorKey (V.Snd aC) (V.Snd bC)) Int)
  givenMapFld = M.unionWith (+) zeroMap <$> FL.premap asKeyVal (FL.foldByKeyMap FL.sum)

  -- splitAt but keeps going until entire list is split into things of given length and whatever is left
  splitAtAll :: Int -> [c] -> [[c]]
  splitAtAll n l = reverse $ go l [] where
    go remaining accum =
      if null rest
      then remaining : accum
      else go rest (slice : accum)
      where
        (slice, rest) = List.splitAt n remaining

  toRows :: Map k a -> [[a]]
  toRows = splitAtAll (length allBs) . M.elems

  adjustRow :: Int -> [Int] -> [Int]
  adjustRow desiredSum counts = fmap adjustCell counts
    where rowSum = FL.fold FL.sum counts
          totalDiff = desiredSum - rowSum
          mult :: Double = realToFrac totalDiff /  realToFrac rowSum
          adjustCell n = n + round (mult * realToFrac n)

  newRows :: Map (RowMajorKey (V.Snd aC) (V.Snd bC)) Int -> [[Int]]
  newRows = zipWith adjustRow (M.elems desiredRowSums) . toRows

  makeRec :: V.Snd aC -> (V.Snd bC, Int) -> F.Record [aC, bC, count]
  makeRec a (b, n) =  a F.&: b F.&: n F.&: V.RNil

  makeRecords :: [[Int]] -> [[F.Record [aC, bC, count]]]
  makeRecords = zipWith (fmap . makeRec) allAs . fmap (zip allBs)


---

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
enrichFrameFromBinaryModel mr getGeo aTrue aFalse = enrichFrameFromModel @t @count smf where
  smf r = do
    let smf' x = SplitModelF $ \n ->  let nTrue = round (realToFrac n * x) in M.fromList [(aTrue, nTrue), (aFalse, n - nTrue)]
    pTrue <- DM.applyModelResult mr (getGeo r) r
    pure $ smf' pTrue

-- | produce a map of splits across type a. Doubles must sum to 1.
newtype  SplitModelF n a = SplitModelF { splitModelF :: n -> Map a n }

splitRec :: forall t count rs a . (V.KnownField t
                                  , V.Snd t ~ a
                                  , V.KnownField count
                                  , F.ElemOf rs count
                                  )
         => SplitModelF (V.Snd count) a -> F.Record rs -> [F.Record (t ': rs)]
splitRec (SplitModelF f) r =
  let makeOne (a, n) = a F.&: (F.rputField @count n) r
      splits = M.toList $ f $ F.rgetField @count r
  in makeOne <$> splits

enrichFromModel :: forall t count ks rs a . (ks F.⊆ rs
                                            , V.KnownField t
                                            , V.Snd t ~ a
                                            , V.KnownField count
                                            , F.ElemOf rs count
                                            )
                      => (F.Record ks -> Either Text (SplitModelF (V.Snd count) a)) -- ^ model results to apply
                      -> F.Record rs -- ^ record to enrich
                      -> Either Text [F.Record (t ': rs)]
enrichFromModel modelResultF recordToEnrich = flip (splitRec @t @count) recordToEnrich <$> modelResultF (F.rcast recordToEnrich)

data ModelLookupException = ModelLookupException Text deriving stock (Show)
instance Exception ModelLookupException


-- this should build the new frame in-core and in a streaming manner,
-- not needing to build the entire list before placing in-core
enrichFrameFromModel :: forall t count ks rs a m . (ks F.⊆ rs
                                                   , FSI.RecVec rs
                                                   , FSI.RecVec (t ': rs)
                                                   , V.KnownField t
                                                   , V.Snd t ~ a
                                                   , V.KnownField count
                                                   , F.ElemOf rs count
                                                   , MonadThrow m
                                                   , Prim.PrimMonad m
                                                   )
                     => (F.Record ks -> Either Text (SplitModelF (V.Snd count) a)) -- ^ model results to apply
                     -> F.FrameRec rs -- ^ record to enrich
                     -> m (F.FrameRec (t ': rs))
enrichFrameFromModel modelResultF =  FST.transform f where
  f = Streamly.concatMapM g
  g r = do
    case enrichFromModel @t @count modelResultF r of
      Left err -> throwM $ ModelLookupException err
      Right rs -> pure $ Streamly.fromList rs



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

{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE BangPatterns #-}
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
import qualified Numeric.LinearAlgebra as LA
import qualified Numeric.NLOPT as NLOPT
import qualified Numeric
import qualified Data.Vector.Storable as VS

import qualified System.IO.Unsafe as Unsafe ( unsafePerformIO )
import qualified Say as Say
import GHC.Base (VecElem(DoubleElemRep))



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
                         , V.KnownField count
                         , V.KnownField a
                         , V.KnownField b
                         , Ord (F.Record ks)
                         , ks F.⊆ rs
                         , FSI.RecVec (ks V.++ [a, b, count])
                         )
                      => (Map (V.Snd a) Int -> Set (V.Snd b) ->  FL.FoldM (Either Text) (F.Record [a, b, count]) [F.Record [a, b, count]])
                      -> (F.Record ks -> Either Text (Map (V.Snd a) Int)) -- desired row sums, probably from a map produced by function above
                      -> Set (V.Snd b) -- column category in full in case some are missing from the frame
                      -> FL.FoldM (Either Text) (F.Record rs) (F.FrameRec (ks V.++ [a, b, count]))
nearestCountsFrameFld iFldE desiredRowSumsLookup cols =
  FMR.concatFoldM $
  FMR.mapReduceFoldM
  (FMR.generalizeUnpack FMR.noUnpack)
  (FMR.generalizeAssign $ FMR.assignKeysAndData @ks @[a, b, count])
  (MR.ReduceFoldM reduceM)
  where
    convertFldE :: Either d (FL.FoldM (Either d) e f) -> FL.FoldM (Either d) e f
    convertFldE fldE = case fldE of
      Right fld -> fld
      Left a -> FL.FoldM (\_ _ -> Left a) (Left a) (const $ Left a) -- we prolly only need one of these "Left a". But which?
    reduceM rk =
      let eFld = (\x -> iFldE x cols) <$> desiredRowSumsLookup rk
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

rowMajorMapFld :: forall count a b c .
                  (V.Snd count ~ Int
                   , Ord c
                   , Ord (V.Snd b)
                   , V.KnownField a
                   , V.KnownField b
                   , V.KnownField count
                   , F.ElemOf [a, b, count] a
                   , F.ElemOf [a, b, count] b
                   , F.ElemOf [a, b, count] count
                   )
               => (V.Snd a -> c) -> Set c -> Set (V.Snd b) -> FL.Fold (F.Record [a, b, count]) (Map (RowMajorKey c (V.Snd b)) Int)
rowMajorMapFld rowMerge cs bs = givenMapFld where
  allCs = S.elems cs
  allBs = S.elems bs
  allCBs = [RowMajorKey (c, b) | c <- allCs, b <- allBs]

  zeroMap :: Map (RowMajorKey c (V.Snd b)) Int
  zeroMap = M.fromList $ (, 0) <$> allCBs

  asKeyVal :: F.Record [a, b, count] -> (RowMajorKey c (V.Snd b), Int)
  asKeyVal r = (RowMajorKey (rowMerge (F.rgetField @a r), F.rgetField @b r), F.rgetField @count r)

  -- fold the input records to a map keyed by categories and add 0s when things are missing
  givenMapFld :: FL.Fold (F.Record [a, b, count]) (Map (RowMajorKey c (V.Snd b)) Int)
  givenMapFld = M.unionWith (+) zeroMap <$> FL.premap asKeyVal (FL.foldByKeyMap FL.sum)

rowMajorMapTable :: (Ord a, Ord b) => Map (RowMajorKey a b) Int -> Map a (Map b Int)
rowMajorMapTable = M.fromListWith (M.unionWith (+)) .  fmap (\(k, n) -> (rowVal k, M.singleton (colVal k) n)) .  M.toList

nearestCountsFrameIFld :: forall a b count .
                          (V.Snd count ~ Int
                          , Ord (V.Snd a)
                          , Ord (V.Snd b)
                          , V.KnownField a
                          , V.KnownField b
                          , V.KnownField count
                          , F.ElemOf [a, b, count] a
                          , F.ElemOf [a, b, count] b
                          , F.ElemOf [a, b, count] count
                          )
                       => ([Int] -> [[Int]] -> Either Text [[Int]])
                       -> Map (V.Snd a) Int
                       -> Set (V.Snd b)
                       -> FL.FoldM (Either Text) (F.Record [a, b, count]) [F.Record [a, b, count]]
nearestCountsFrameIFld innerComp desiredRowSums colLabels =
  fmap (concat . makeRecords)
  $ mapMFold (innerComp  (M.elems desiredRowSums))
  $ (toRows <$> rowMajorMapFld @count @a id rowLabels colLabels) where
  rowLabels = M.keysSet desiredRowSums

  toRows :: Map k x -> [[x]]
  toRows = splitAtAll (S.size colLabels) . M.elems

  mapMFold :: Monad m => (y -> m z) -> FL.Fold x y -> FL.FoldM m x z
  mapMFold f (FL.Fold step init' extract) = FL.FoldM (\x h -> pure $ step x h) (pure init') (f . extract)

  makeRec :: V.Snd a -> (V.Snd b, Int) -> F.Record [a, b, count]
  makeRec a (b, n) =  a F.&: b F.&: n F.&: V.RNil

  makeRecords :: [[Int]] -> [[F.Record [a, b, count]]]
  makeRecords = zipWith (fmap . makeRec) (S.toList rowLabels) . fmap (zip (S.toList colLabels))

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

nearestCountsKL_NL :: [Int] -> [[Int]] -> Either Text [[Int]]
nearestCountsKL_NL dRowSumsI oCountsI = do
  let toDouble = realToFrac @Int @LA.R
      nM :: LA.Matrix LA.I = LA.fromRows $ fmap (LA.fromList . fmap fromIntegral) oCountsI
      nVRaw = LA.fromList (toDouble <$> mconcat oCountsI)
      nSum = VS.sum nVRaw
      nV = LA.scale (1 / nSum) nVRaw
      aV = LA.scale (1 / nSum) $ LA.fromList $ fmap toDouble dRowSumsI
      (rows, cols) = LA.size nM
--      ul = LA.diag nDivq - (LA.scale (nSum / (qSum * qSum)) $ LA.fromRows $ replicate (rows * cols) qV)
      indexes l = l `divMod` cols
      rowSums :: LA.Vector LA.R -> LA.Vector LA.R
      rowSums = LA.fromList . fmap VS.sum . LA.toRows . LA.reshape cols
      colSums :: LA.Vector LA.R -> LA.Vector LA.R
      colSums = LA.fromList . fmap VS.sum . LA.toColumns . LA.reshape cols
--      nRowSums = rowSums cols nV
      nColSums = colSums nV
      nSize = rows * cols
      -- our vector is components of M in row major order, followed by Lagrange multipliers
      subVectors :: LA.Vector LA.R -> (LA.Vector LA.R, LA.Vector LA.R, LA.Vector LA.R)
      subVectors v = (mV, lambdaV, gammaV) where
        mV = LA.subVector 0 nSize v
        lambdaV = LA.subVector nSize rows v
        gammaV = LA.subVector (nSize + rows) cols v
      kl :: LA.Vector LA.R -> Double
      kl v = Numeric.log (VS.sum v) + VS.sum (VS.zipWith (*) nV logNMV) where
        safeLogRatio x y = if y == 0 then 0 else Numeric.log (x / y)
        logNMV = VS.zipWith safeLogRatio nV v
{-      nlCBounds = [NLOPT.LowerBounds $ VS.replicate nSize 0, NLOPT.UpperBounds $ VS.replicate nSize 1]

      klGradients :: LA.Vector LA.R -> LA.Vector LA.R
      klGradients v = VS.zipWith g nV v where
        mSum = VS.sum v
        g x y = if y == 0 then 0 else (1 / mSum) - x / y -- M_jk should be zero if N_jk is, and then this ratio is 1. Any other M_jk == 0 is a real error

      rowSumConstraint j v = (rowSums v VS.! j - aV VS.! j, VS.generate nSize (\l -> if fst (indexes l) == j then 1 else 0))
      colSumConstraint k v = (colSums v VS.! k - nColSums VS.! k, VS.generate nSize (\l -> if snd (indexes l) == k then 1 else 0))
      toNLEC f = NLOPT.EqualityConstraint (NLOPT.Scalar f) 0.01
      nlConstraints = fmap (toNLEC . rowSumConstraint) [0..(rows - 1)]
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

      nlConstraints = [nlRowSumConstraint, nlColSumConstraint]
-}
      nlCObjectiveD v = (kl v, klGradients v)
      nlStop = NLOPT.ParameterRelativeTolerance 0.01 :| [NLOPT.MaximumEvaluations 10]
      nlCAlgo = NLOPT.SLSQP nlCObjectiveD nlCBounds [] nlConstraints
      nlCProblem = NLOPT.LocalProblem (fromIntegral nSize) nlStop nlCAlgo
      nlCSol = NLOPT.minimizeLocal nlCProblem nV
  case nlCSol of
    Left result -> Left $ show result
    Right solution -> Right . splitAtAll cols . fmap round . LA.toList . LA.scale nSum $ LA.subVector 0 nSize (NLOPT.solutionParams solution)
-}

      f :: LA.Vector LA.R -> Double
      f v = 0.5 * kl mV
            + (VS.sum $ VS.zipWith (*) lambdaV (rowSums mV - aV))
            + (VS.sum $ VS.zipWith (*) gammaV (colSums mV - nColSums))
        where
          (mV, lambdaV, gammaV) = subVectors v

      fGradients :: LA.Vector LA.R -> LA.Vector LA.R
      fGradients v = VS.concat [mGradV, lambdaGradV, gammaGradV] where
        (mV, lambdaV, gammaV) = subVectors v
        sumM = VS.sum mV
        klGrad x y = if y == 0 then 0 else (1 / sumM) - (x / y)
        klGrads = VS.zipWith klGrad nV mV
        mGradF l = klGrads VS.! l + lambdaV VS.! j + gammaV VS.! k
          where
            (j, k) = indexes l
        mGradV = VS.generate nSize mGradF
        lambdaGradV = rowSums mV - aV
        gammaGradV = colSums mV - nColSums
      nlStarting = VS.concat [nV, VS.replicate (rows + cols) 0] -- we start at n with 0 multipliers
      nlObjectiveD v = (f v, fGradients v)
      nlBounds = [NLOPT.LowerBounds $ VS.concat [VS.replicate nSize 0, VS.replicate (rows + cols) $ negate 100]
                 , NLOPT.UpperBounds $ VS.concat [VS.replicate nSize 1, VS.replicate (rows + cols) 100]
                 ]
      nlStop = NLOPT.ParameterRelativeTolerance 0.01 :| [NLOPT.MaximumEvaluations 10]
      nlAlgo = NLOPT.VAR2 nlObjectiveD Nothing --nlBounds [] []
      nlProblem = NLOPT.LocalProblem (fromIntegral $ nSize + rows + cols) nlStop nlAlgo
      nlSol = NLOPT.minimizeLocal nlProblem nlStarting
  case nlSol of
    Left result -> Left $ show result
    Right solution -> Right . splitAtAll cols . fmap round . LA.toList . LA.scale nSum $ LA.subVector 0 nSize (NLOPT.solutionParams solution)


nearestCountsKL :: [Int] -> [[Int]] -> Either Text [[Int]]
nearestCountsKL dRowSumsI oCountsI = do
  let toDouble = realToFrac @Int @LA.R
      rsZeroIx = fmap fst $ filter ((== 0) . snd) $ zip [(0 :: Int)..] dRowSumsI
      dRowSumsI' = filter (/= 0) dRowSumsI
      aV = LA.fromList $ fmap toDouble dRowSumsI'
      oCountsI' = remove 0 rsZeroIx oCountsI where
        remove _ [] l = l
        remove n (ix : ixs) l = remove (n + 1) ixs $ let (h, t) = List.splitAt (ix - n) l in h <> List.drop 1 t
      nM :: LA.Matrix LA.I = LA.fromRows $ fmap (LA.fromList . fmap fromIntegral) oCountsI'
      nV = LA.fromList (toDouble <$> mconcat oCountsI')
      (rows, cols) = LA.size nM
      oneV = LA.fromList $ replicate (rows * cols) 1
      update q d = VS.zipWith (*) q (VS.zipWith (+) oneV d)
      converge maxN n tol tolF qV deltaV = do
        when ((n :: Int) >= maxN) $ Left $ "Max iterations (" <> show maxN <> ")exceeded in nearestCountsKL"
        if tolF deltaV >= tol
          then (do
                   let qV' = update qV deltaV
                   deltaV' <- updateFromGuess rows cols aV nV qV'
                   converge maxN (n + 1) tol tolF qV' deltaV'
               )
          else pure qV
  let nMax = 5
      absTol = 0.001
      tolF dV = VS.sum $ VS.zipWith (*) dV dV
  deltaV <- updateFromGuess rows cols aV nV nV
  qVsol <- converge nMax 0 absTol tolF (update nV deltaV) deltaV
  let sol' = splitAtAll cols (fmap round $ LA.toList qVsol)
      sol = restore rsZeroIx sol' where
        restore [] l = l
        restore (ix : ixs) l = restore ixs $ take ix l <> (replicate cols 0 : drop ix l)
  pure sol



updateFromGuess :: Int -- rows
                -> Int -- cols
                -> LA.Vector LA.R -- desired row sums
                -> LA.Vector LA.R -- N in row major form
                -> LA.Vector LA.R -- current guess, row major
                -> Either Text (LA.Vector LA.R) -- delta, row major
updateFromGuess rows cols aV nV qV = do
  let nSum = VS.sum nV
      qSum = VS.sum qV
      invnSum = 1 / nSum
      safeDiv x y = if y == 0 then 1 else x / y
      nDivq = VS.zipWith safeDiv nV qV
      ul = LA.diag nDivq - (LA.scale (nSum / (qSum * qSum)) $ LA.fromRows $ replicate (rows * cols) qV)
      rowIndex l = l `div` cols
      colIndex l = l `mod` cols
      f l = [((l, rowIndex l), 1), ((l, rows + colIndex l), 1)]

      ur :: LA.Matrix LA.R
      ur = LA.assoc (rows * cols, rows + cols) 0 $ mconcat $ fmap f [0..(rows * cols - 1)]
      rowSumMask :: Int -> LA.Vector LA.R
      rowSumMask j = LA.assoc (rows * cols) 0 $ fmap (\k -> (k + j * cols, 1)) [0..(cols - 1)]
      rowSumVec j = LA.scale invnSum $ VS.zipWith (*) qV (rowSumMask j)
      colSumMask :: Int -> LA.Vector LA.R
      colSumMask k = LA.assoc (rows * cols) 0 $ fmap (\j -> (k + j * cols, 1)) [0..(rows - 1)]
      colSumVec k = LA.scale invnSum $ VS.zipWith (*) qV (colSumMask k)
      ll =  LA.fromRows (fmap rowSumVec [0..(rows - 1)]) LA.=== LA.fromRows (fmap colSumVec [0..(cols - 1)])
      lr = LA.konst 0 (rows + cols, rows +  cols)
      m = (ul LA.||| ur ) LA.=== (ll LA.||| lr) -- LA.fromBlocks [[ul, ur],[ll, 0]]
      rowSums v = LA.fromList $ fmap VS.sum $ LA.toRows $ LA.reshape cols v
      colSums v = LA.fromList $ fmap VS.sum $ LA.toColumns $ LA.reshape cols v
      qRowSumsV = rowSums qV

--      colSumsL = fmap (realToFrac @LA.I @LA.R . VS.sum) $ LA.toColumns oCountsM
      rhsV = LA.vjoin [VS.map (\x -> x - nSum / qSum) nDivq
                      , LA.scale invnSum (VS.zipWith (-) aV qRowSumsV)
                      , LA.scale invnSum (colSums (VS.zipWith (-) nV qV))
                      ]
      rhsM = LA.fromColumns [rhsV]
      funcDetails = "rowSums=\n" <> show aV <> "\nN=\n" <> show nV <> "\nQ=\n" <> show qV
                    <> "\nm=\n" <> toText (LA.dispf 4 m)
                    <> "\nrhs=\n" <> toText (LA.dispf 4 rhsM)
--      luPacked = LA.luPacked m

--      luSol = LA.luSolve luPacked rhsM
  let !_ = Unsafe.unsafePerformIO $ Say.say $ funcDetails
  luSol <- maybeToRight ("LU solver failure:" <> funcDetails) $ LA.linearSolve m rhsM
  let !_ = Unsafe.unsafePerformIO $ Say.say $ "\ndelta=\n" <> toText (LA.dispf 4 luSol)
  mSol :: LA.Vector LA.R <- maybeToRight "empty list of solutions??" $ viaNonEmpty (LA.subVector 0 (rows * cols) . head) $ LA.toColumns luSol
  pure mSol

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

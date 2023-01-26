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

--import qualified BlueRipple.Data.ACS_PUMS as PUMS
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
import qualified Frames.Transform as FT
import qualified Numeric.LinearAlgebra as LA
--import qualified Numeric.NLOPT as NLOPT
--import qualified Numeric
import qualified Data.Vector.Storable as VS

import qualified System.IO.Unsafe as Unsafe ( unsafePerformIO )
import qualified Say as Say



-- produce rowSums
desiredRowSumsFld :: forall kOuter rk d .
                     (Ord kOuter, Ord rk)
                  => (d -> Int)
                  -> (d -> kOuter)
                  -> (d -> rk)
                  -> Set rk
                  -> FL.Fold d (Map kOuter (Map rk Int))
desiredRowSumsFld count outerKey innerKey allBs  =
  fmap M.fromList
  $ MR.mapReduceFold
  MR.noUnpack
  (MR.assign outerKey innerAndCount)
  (MR.foldAndLabel reduceFld (,))

  where
    innerAndCount r = (innerKey r, count r)
    zeroMap = M.fromList $ (, 0) <$> S.toList allBs
--    asKeyVal r = (reKey $ F.rgetField @a r, F.rgetField @count r)
    reduceFld :: FL.Fold (rk, Int) (Map rk Int)
    reduceFld = M.unionWith (+) zeroMap <$> FL.foldByKeyMap FL.sum

rowMajorMapFld :: forall d rk ck .
                  (Ord rk, Ord ck)
               => (d -> Int)
               -> (d -> rk)
               -> (d -> ck)
               -> Set rk
               -> Set ck
               -> FL.Fold d (Map (rk, ck) Int)
rowMajorMapFld count rowKey colKey allRowKeysS allColKeysS = givenMapFld where
  allRowKeysL = S.elems allRowKeysS
  allColKeysL = S.elems allColKeysS
  allRowColKeysL = [(rk, ck) | rk <- allRowKeysL, ck <- allColKeysL]

  zeroMap :: Map (rk, ck) Int
  zeroMap = M.fromList $ (, 0) <$> allRowColKeysL

  asKeyVal :: d -> ((rk, ck), Int)
  asKeyVal d = ((rowKey d, colKey d), count d)

  -- fold the input records to a map keyed by categories and add 0s when things are missing
  givenMapFld :: FL.Fold d (Map (rk, ck) Int)
  givenMapFld = M.unionWith (+) zeroMap <$> FL.premap asKeyVal (FL.foldByKeyMap FL.sum)

rowMajorMapTable :: (Ord a, Ord b) => Map (a, b) Int -> Map a (Map b Int)
rowMajorMapTable = M.fromListWith (M.unionWith (+)) .  fmap (\(k, n) -> (fst k, M.singleton (snd k) n)) .  M.toList

totaledTable :: (Ord a, Ord b, Show a) => Map (a, b) Int -> [(Text, Map b Int)]
totaledTable m = mList ++ [("Total", totals)] where
  mAsList = M.toList $ rowMajorMapTable m
  mList = fmap (first show) mAsList
  totals = foldl' (M.unionWith (+)) mempty $ fmap snd mAsList

nearestCountsFrameFld :: forall count rks cks outerKs rs .
                         (V.Snd count ~ Int
                         , V.KnownField count
                         , Ord (F.Record outerKs)
                         , outerKs F.⊆ rs
                         , FSI.RecVec (outerKs V.++ rks V.++ cks V.++ '[count])
                         , (outerKs V.++ ((rks V.++ cks) V.++ '[count]) ~ ((outerKs V.++ rks) V.++ cks) V.++ '[count])
                         , ((rks V.++ cks) V.++ '[count]) F.⊆ rs
                         )
                      => (Map (F.Record rks) Int -> Set (F.Record cks)
                          ->  FL.FoldM (Either Text) (F.Record (rks V.++ cks V.++ '[count])) [F.Record (rks V.++ cks V.++ '[count])])
                      -> (F.Record outerKs -> Either Text (Map (F.Record rks) Int)) -- desired row sums
                      -> Set (F.Record cks) -- column category in full in case some are missing from the frame
                      -> FL.FoldM (Either Text) (F.Record rs) (F.FrameRec (outerKs V.++ rks V.++ cks V.++ '[count]))
nearestCountsFrameFld iFldE desiredRowSumsLookup cols =
  FMR.concatFoldM $
  FMR.mapReduceFoldM
  (FMR.generalizeUnpack FMR.noUnpack)
  (FMR.generalizeAssign $ FMR.assignKeysAndData @outerKs @(rks V.++ cks V.++ '[count]))
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

nearestCountsFrameIFld :: forall count rks cks .
                          (F.ElemOf ((rks V.++ cks) V.++ '[count]) count
                          , V.Snd count ~ Int
                          , V.KnownField count
                          , Ord (F.Record rks)
                          , Ord (F.Record cks)
                          , rks F.⊆ ((rks V.++ cks) V.++ '[count])
                          , cks F.⊆ ((rks V.++ cks) V.++ '[count])
                          , (rks V.++ (cks V.++ '[count])) ~  ((rks V.++ cks) V.++ '[count])
                          )
                       =>  ([Int] -> [[Int]] -> Either Text [[Int]])
                       -> Map (F.Record rks) Int
                       -> Set (F.Record cks)
                       ->  FL.FoldM (Either Text) (F.Record (rks V.++ cks V.++ '[count])) [F.Record (rks V.++ cks V.++ '[count])]
nearestCountsFrameIFld innerComp rowSums allColumns =
  fmap (fmap toRecord)
  $ nearestCountsIFld innerComp (F.rgetField @count) (F.rcast @rks) (F.rcast @cks) rowSums allColumns
  where
    toRecord :: (F.Record rks, F.Record cks, Int) -> F.Record (rks V.++ cks V.++ '[count])
    toRecord (rkr, ckr, n) = rkr F.<+> ckr F.<+> FT.recordSingleton @count n


nearestCountsIFld :: forall d rk ck .
                     (Ord rk, Ord ck)
                  => ([Int] -> [[Int]] -> Either Text [[Int]])
                  -> (d -> Int)
                  -> (d -> rk)
                  -> (d -> ck)
                  -> Map rk Int
                  -> Set ck
                  -> FL.FoldM (Either Text) d [(rk, ck, Int)]
nearestCountsIFld innerComp count rowKey colKey desiredRowSums colLabels =
  fmap (concat . makeTuples)
  $ mapMFold (innerComp  (M.elems desiredRowSums))
  $ (toRows <$> rowMajorMapFld count rowKey colKey rowLabels colLabels)
  where
    rowLabels = M.keysSet desiredRowSums

    toRows :: Map k x -> [[x]]
    toRows = splitAtAll (S.size colLabels) . M.elems

    mapMFold :: Monad m => (y -> m z) -> FL.Fold x y -> FL.FoldM m x z
    mapMFold f (FL.Fold step init' extract) = FL.FoldM (\x h -> pure $ step x h) (pure init') (f . extract)

    makeTuple :: rk -> (ck, Int) -> (rk, ck, Int)
    makeTuple a (b, n) =  (a, b, n)

    makeTuples :: [[Int]] -> [[(rk, ck, Int)]]
    makeTuples = zipWith (fmap . makeTuple) (S.toList rowLabels) . fmap (zip (S.toList colLabels))

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

-- Hand rolled solver
-- Linearize equations solving for the stationary point
-- of minimizing KL divergence plus lagrange multipliers for
-- row and column sums
-- then iterate linearized solution until delta is small.
-- Requires SVD. Why?
nearestCountsKL :: [Int] -> [[Int]] -> Either Text [[Int]]
nearestCountsKL dRowSumsI oCountsI = do
  let toDouble = realToFrac @Int @LA.R

      transp :: [[a]] -> [[a]]
      transp = go [] where
        go x [] = fmap reverse x
        go [] (r : rs) = go (fmap (\y -> [y]) r) rs
        go x (r :rs) = go (List.zipWith (:) r x) rs

      -- we need to remove zero rows and columns.
      remove :: Int -> [Int] -> [[Int]] -> [[Int]]
      remove _ [] l = l
      remove n (ix : ixs) l = remove (n + 1) ixs $ let (h, t) = List.splitAt (ix - n) l in h <> List.drop 1 t

      -- But then put them back after solving.
      restore :: [Int] -> [[Int]] -> [[Int]]
      restore [] l = l
      restore (ix : ixs) l = restore ixs $ take ix l <> (replicate cols 0 : drop ix l)

      rsZeroIx = fmap fst $ filter ((== 0) . snd) $ zip [(0 :: Int)..] dRowSumsI
      csZeroIx = fmap fst $ filter ((== 0) . snd) $ zip [(0 :: Int)..]
                 $ fmap (FL.fold FL.sum) $ transp oCountsI
      dRowSumsI' = filter (/= 0) dRowSumsI
      aV = LA.fromList $ fmap toDouble dRowSumsI'
      oCountsI' = remove 0 rsZeroIx oCountsI
      oCountsI'' = transp $ remove 0 csZeroIx $ transp oCountsI'

      nM :: LA.Matrix LA.I = LA.fromRows $ fmap (LA.fromList . fmap fromIntegral) oCountsI''
      (rows, cols) = LA.size nM
  case cols of
    1 -> Right [dRowSumsI] -- if there is only 1 column, the desired row sums are the answer
    _ -> do
      let oCountsD = fmap (fmap toDouble) oCountsI''
          nV = LA.fromList $ mconcat oCountsD
          nRowSums = VS.fromList $ fmap (FL.fold FL.sum) oCountsD
--          !_ = Unsafe.unsafePerformIO $ Say.say $ "\nN=" <> show nV <> "\nRowSums=" <> show nRowSums
          rowSumRatios = VS.zipWith (/) aV nRowSums
          firstGuessV = LA.fromList $ mconcat $ zipWith (\l r -> fmap (* r) l) oCountsD (VS.toList rowSumRatios)

          -- This is likely a pretty good guess! It satisfies all the constraints. So step 1 is just to bootstrap the iteration
          -- by finding a starting value of the Lagrange multipliers, an overdetermined problem
          lgSolM = LA.linearSolveLS (lgMatrix rows cols) (LA.fromColumns [rhsUVec0 rows cols nV firstGuessV])
      lgSolV <-  maybeToRight "empty list of LS solutions??" $ viaNonEmpty head $ LA.toColumns lgSolM
      let !_ = Unsafe.unsafePerformIO $ Say.say $ "\n(bootstrap) LG=" <> show lgSolM
      let fullFirstGuessV = VS.concat [firstGuessV, lgSolV]
          update g d = VS.concat [qV', lV', gaV']
            where (qV, lV, gaV) = subVectors rows cols g
                  (dqV, dlV, dgaV) = subVectors rows cols d
                  qV' = VS.zipWith (\qU dU -> qU * (1 + dU)) qV dqV
                  lV' = lV + dlV
                  gaV' = gaV + dgaV
          converge maxN n tol tolF gV deltaVM = do
            when ((n :: Int) >= maxN) $ Left $ "Max iterations (" <> show maxN <> ")exceeded in nearestCountsKL"
            let gV' = maybe gV (update gV) deltaVM
            if maybe True (\dV -> tolF dV >= tol) deltaVM
              then (do
--                       let gV' = update gV deltaV
                       deltaV' <- updateFromGuess rows cols aV nV gV'
                       converge maxN (n + 1) tol tolF gV' (Just deltaV')
                   )
              else pure gV
      let nMax = 100
          absTol = 0.0001
          tolF dV = let tV = VS.take (rows * cols) dV in (VS.sum $ VS.zipWith (*) tV tV) / (realToFrac $ rows * cols)
--      deltaV <- updateFromGuess rows cols aV nV fullFirstGuessV
      xVsol <- converge nMax 0 absTol tolF fullFirstGuessV Nothing
      let qVSol = VS.take (rows * cols) xVsol
      let sol'' = splitAtAll cols (fmap round $ LA.toList qVSol)
          sol' = transp $ restore csZeroIx $ transp sol''
          sol = restore rsZeroIx sol'
      pure sol

subVectors :: Int -> Int -> LA.Vector LA.R -> (LA.Vector LA.R, LA.Vector LA.R, LA.Vector LA.R)
subVectors rows cols v = (mV, lambdaV, gammaV) where
  nSize = rows * cols
  mV = LA.subVector 0 nSize v
  lambdaV = LA.subVector nSize rows v
  gammaV = LA.subVector (nSize + rows) cols v

lgMatrix :: Int -> Int -> LA.Matrix LA.R
lgMatrix rows cols = LA.assoc (rows * cols, rows + cols) 0 $ mconcat $ fmap f [0..(rows * cols - 1)]
  where
    indexes l = l `divMod` cols
    f l = let (j, k) = indexes l in [((l, j), 1), ((l, rows + k), 1)]

rhsUVec :: Int -> Int -> LA.Vector LA.R -> LA.Vector LA.R -> LA.Vector LA.R -> LA.Vector LA.R -> LA.Vector LA.R
rhsUVec rows cols nV qV lambdaV gammaV = VS.zipWith rhsuF (VS.generate (rows * cols) id) nDivq
  where
    indexes l = l `divMod` cols
    safeDiv x y = if y == 0 then 1 else x / y
    nDivq = VS.zipWith safeDiv nV qV
    nSum = VS.sum nV
    qSum = VS.sum qV
    rhsuF l x = let (j, k) = indexes l in x - (nSum / qSum) - lambdaV VS.! j - gammaV VS.! k

rhsUVec0 :: Int -> Int -> LA.Vector LA.R -> LA.Vector LA.R -> LA.Vector LA.R
rhsUVec0 rows cols nV qV = rhsUVec rows cols nV qV (VS.replicate rows 0) (VS.replicate cols 0)

updateFromGuess :: Int -- rows
                -> Int -- cols
                -> LA.Vector LA.R -- desired row sums
                -> LA.Vector LA.R -- N in row major form
                -> LA.Vector LA.R -- current guess, row major, then lambda, then gamma
                -> Either Text (LA.Vector LA.R) -- delta, row major
updateFromGuess rows cols aV nV gV = do
  let (qV, lambdaV, gammaV) = subVectors rows cols gV
      nSum = VS.sum nV
      qSum = VS.sum qV
      invnSum = 1 / nSum
      safeDiv x y = if y == 0 then 1 else x / y
      nDivq = VS.zipWith safeDiv nV qV
      ul = LA.diag nDivq - (LA.scale (nSum / (qSum * qSum)) $ LA.fromRows $ replicate (rows * cols) qV)
      indexes l = l `divMod` cols
      ur = lgMatrix rows cols

      rowSumMask :: Int -> LA.Vector LA.R
      rowSumMask j = LA.assoc (rows * cols) 0 $ fmap (\k -> (k + j * cols, 1)) [0..(cols - 1)]
      rowSumVec j = LA.scale invnSum $ VS.zipWith (*) qV (rowSumMask j)

      colSumMask :: Int -> LA.Vector LA.R
      colSumMask k = LA.assoc (rows * cols) 0 $ fmap (\j -> (k + j * cols, 1)) [0..(rows - 1)]
      colSumVec k = LA.scale invnSum $ VS.zipWith (*) qV (colSumMask k)

      ll =  LA.fromRows (fmap rowSumVec [0..(rows - 1)]) LA.=== LA.fromRows (fmap colSumVec [0..(cols - 1)])
      lr = LA.konst 0 (rows + cols, rows +  cols)
      m = (ul LA.||| ur ) LA.=== (ll LA.||| lr) -- LA.fromBlocks [[ul, ur],[ll, 0]]
      (sVals, sVecs) = LA.rightSV m
      funcDetails = "\nQ=\n" <> show qV <> "\nLambda=" <> show lambdaV <> "\nGamma=" <> show gammaV
--      !_ = Unsafe.unsafePerformIO $ Say.say $ funcDetails
  case  VS.minimum sVals < 1e-15 of
    True -> do
      let solIndex = VS.minIndex sVals
          singSolM =  sVecs LA.¿ [solIndex] --LA.subMatrix (0, solIndex) (rows * cols + rows + cols - 1, 1) sVecs
      singSolV <- maybeToRight "No singular vectors??" $ viaNonEmpty head $ LA.toColumns singSolM
--      let !_ = Unsafe.unsafePerformIO $ Say.say $ "\n (Sing): delta|lambda|gamma=\n" <> toText (LA.dispf 4 $ LA.tr singSolM)
      pure singSolV
    False -> do
      let rowSums v = LA.fromList $ fmap VS.sum $ LA.toRows $ LA.reshape cols v
          colSums v = LA.fromList $ fmap VS.sum $ LA.toColumns $ LA.reshape cols v
          qRowSumsV = rowSums qV
--          rhsuV = VS.zipWith rhsuF (VS.generate (rows * cols) id) nDivq
          rhsV = LA.vjoin [rhsUVec rows cols nV qV lambdaV gammaV
                          , LA.scale invnSum (VS.zipWith (-) aV qRowSumsV)
                          , LA.scale invnSum (colSums (VS.zipWith (-) nV qV))
                          ]
          rhsM = LA.fromColumns [rhsV]
--          !_ = Unsafe.unsafePerformIO $ Say.say $ "\nrhs=\n" <> toText (LA.dispf 4 $ LA.tr rhsM)
      luSolM <- maybeToRight "no LU solution even though SVD says no singular value!" $ LA.linearSolve m rhsM
      luSolV :: LA.Vector LA.R <- maybeToRight "empty list of solutions??" $ viaNonEmpty head $ LA.toColumns luSolM
--      let !_ = Unsafe.unsafePerformIO $ Say.say $ "\n (LU): delta|lambda|gamma=\n" <> toText (LA.dispf 4 $ LA.tr luSolM)
      pure luSolV


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

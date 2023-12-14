{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE UnicodeSyntax #-}
{-# LANGUAGE StandaloneDeriving #-}

module BlueRipple.Model.Demographic.TableProducts
  (
    module BlueRipple.Model.Demographic.TableProducts
  )
where

import qualified BlueRipple.Model.Demographic.EnrichData as DED
import qualified BlueRipple.Model.Demographic.MarginalStructure as DMS

import qualified BlueRipple.Data.Keyed as BRK

import qualified Knit.Report as K
import qualified Polysemy.Error as PE

import qualified Control.MapReduce.Simple as MR
import qualified Frames.MapReduce as FMR
import qualified Frames.Streamly.InCore as FSI

import qualified Control.Foldl as FL
import qualified Data.IntMap.Strict as IM
import qualified Data.List as L
import qualified Data.Map.Strict as M
import qualified Data.Map.Merge.Strict as MM
--import Data.Maybe (fromJust)
import qualified Data.Set as S
import Data.Type.Equality (type (~))

import qualified Data.List as List
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V
import qualified Frames as F
import qualified Frames.Melt as F
import qualified Frames.Transform as FT
import qualified Numeric.LinearAlgebra as LA
import qualified Numeric.NLOPT as NLOPT
import qualified Numeric
import qualified Data.Vector as V
import qualified Data.Vector.Storable as VS
import qualified Data.Vector.Unboxed as VU

import Control.Lens (Lens', view, over, lens)

import qualified Flat
import Flat.Instances.Vector()

normalizedVec :: VS.Vector Double -> VS.Vector Double
normalizedVec v = VS.map (/ VS.sum v) v

stencils ::  forall a b.
             (Ord a
             , Ord b
             , BRK.FiniteSet b
             )
         => (b -> a) -> [DED.Stencil Int]
stencils bFromA = M.elems $ FL.fold (DED.subgroupStencils bFromA) $ S.toList BRK.elements
{-# INLINEABLE stencils #-}


zeroKnowledgeTable :: forall outerK a . (Ord outerK, BRK.FiniteSet outerK, Ord a, BRK.FiniteSet a) => Map outerK (Map a Double)
zeroKnowledgeTable = DMS.constMap (DMS.constMap (1 / num)) where
    num = realToFrac $ S.size (BRK.elements @outerK) * S.size (BRK.elements @a)

-- does a pure product
-- for each outerK we split each bs cell into as many cells as are represented by as
-- and we split in proportion to the counts in as
frameTableProduct :: forall outerK as bs count r .
                     (V.KnownField count
                     , DED.EnrichDataEffects r
                     , (bs V.++ (outerK V.++ as V.++ '[count])) ~ (((bs V.++ outerK) V.++ as) V.++ '[count])
                     , outerK F.⊆ (outerK V.++ as V.++ '[count])
                     , outerK F.⊆ (outerK V.++ bs V.++ '[count])
                     , bs F.⊆ (outerK V.++ bs V.++ '[count])
                     , FSI.RecVec (outerK V.++ as V.++ '[count])
                     , FSI.RecVec (bs V.++ (outerK V.++ as V.++ '[count]))
                     , F.ElemOf (outerK V.++ as V.++ '[count]) count
                     , F.ElemOf (outerK V.++ bs V.++ '[count]) count
                     , Show (F.Record outerK)
                     , BRK.FiniteSet (F.Record bs)
                     , Ord (F.Record bs)
                     , Ord (F.Record outerK)
                     , V.Snd count ~ Int
                     )
                  => F.FrameRec (outerK V.++ as V.++ '[count])
                  -> F.FrameRec (outerK V.++ bs V.++ '[count])
                  -> K.Sem r (F.FrameRec (bs V.++ outerK V.++ as V.++ '[count]))
frameTableProduct base splitUsing = DED.enrichFrameFromModel @count (fmap (DED.mapSplitModel round realToFrac) . splitFLookup) base
  where
    splitFLookup = FL.fold (DED.splitFLookupFld (F.rcast @outerK) (F.rcast @bs) (realToFrac @Int @Double . F.rgetField @count)) splitUsing

-- to get a set of counts from the product counts and null-space weights
-- take the weights and left-multiply them with the vectors
-- to get the weighted sum of those vectors and then add it to the
-- product counts


-- k is a phantom here, unmentioned in the data. But it forces us to line things up with the marginal structure, etc.
-- Given N entries in table and a marginal structure with a Q dimensional null space
-- Given C constraints, f irst matrix is the C x N matrix of constraints coming from the structure of known marginals
-- Second matrix, P, is projections, Q x N, each row is a basis vector of the null space.
-- So Pv = projection of v onto the null space
-- Third matrix, R, is rotation from SVD computed null space basis to eigenvector basis. Columns are eigenvectors.
-- So R'P projects onto the null space and rotates into the basis of eigenvectors of covariance matrix
-- And P'R rotates a vector in the nullspace expressed in the basis of eigenvectors of the covariance matrix and
-- rotates back to the SVD computed null space and then injects into the space of the full table.

data NullVectorProjections k where
  NullVectorProjections :: (Ord k, BRK.FiniteSet k) =>  LA.Matrix Double -> LA.Matrix Double -> LA.Matrix Double -> NullVectorProjections k
--  AvgdNullVectorProjections :: (Ord k, BRK.FiniteSet k) =>  LA.Matrix Double -> LA.Matrix Double -> LA.Matrix Double -> LA.Matrix Double -> NullVectorProjections k



nvpConstraints :: NullVectorProjections k -> LA.Matrix Double
nvpConstraints (NullVectorProjections c _ _) = c

nvpProj :: NullVectorProjections k -> LA.Matrix Double
nvpProj (NullVectorProjections _ p _) = p

nvpRot :: NullVectorProjections k -> LA.Matrix Double
nvpRot (NullVectorProjections _ _ r) = r

instance (Ord k, BRK.FiniteSet k) => Flat.Flat (NullVectorProjections k) where
  size (NullVectorProjections c p r) = Flat.size (LA.toRows c, LA.toRows p, LA.toRows r)
  encode (NullVectorProjections c p r) = Flat.encode (LA.toRows c, LA.toRows p, LA.toRows r)
  decode = (\(cVs, pVs, rVs) -> NullVectorProjections (LA.fromRows cVs) (LA.fromRows pVs) (LA.fromRows rVs)) <$> Flat.decode

data ProjectionsToDiff k where
  RawDiff :: NullVectorProjections k -> ProjectionsToDiff k
  AvgdDiff :: LA.Matrix Double -> NullVectorProjections k -> ProjectionsToDiff k

type PTDEither k = Either (NullVectorProjections k) ([LA.Vector Double], NullVectorProjections k)

instance Flat.Flat (NullVectorProjections k) => Flat.Flat (ProjectionsToDiff k) where
  size (RawDiff nvp) = Flat.size @(PTDEither k) (Left nvp)
  size (AvgdDiff m nvp) = Flat.size @(PTDEither k) (Right (LA.toRows m, nvp))
  encode (RawDiff nvp) = Flat.encode @(PTDEither k) (Left nvp)
  encode (AvgdDiff m nvp) = Flat.encode @(PTDEither k) (Right (LA.toRows m, nvp))
  decode = f <$> Flat.decode where
    f = \case
      Left nvp -> RawDiff nvp
      Right (mRows, nvp) -> AvgdDiff (LA.fromRows mRows) nvp

nullVectorProjections :: ProjectionsToDiff k -> NullVectorProjections k
nullVectorProjections (RawDiff x) = x
nullVectorProjections (AvgdDiff _ x) = x

numProjections :: NullVectorProjections k -> Int
numProjections (NullVectorProjections _ p _) = fst $ LA.size p

fullToProjM :: NullVectorProjections k -> LA.Matrix LA.R
fullToProjM (NullVectorProjections _ pM rM) = LA.tr rM LA.<> pM

projToFullM :: NullVectorProjections k -> LA.Matrix LA.R
projToFullM (NullVectorProjections _ pM rM) = LA.tr pM <> rM

-- NB: fullToProjM and projToFullM are transposes of each other
-- So left-multiplcation by fullToProjM is the same as right multiplication by projToFullM
projToFull :: NullVectorProjections k -> LA.Vector LA.R -> LA.Vector LA.R
projToFull nvps v = v LA.<# fullToProjM nvps

fullToProj :: NullVectorProjections k -> LA.Vector LA.R -> LA.Vector LA.R
fullToProj nvps v = fullToProjM nvps LA.#> v

{-
baseNullVectorProjections :: forall w k . (BRK.FiniteSet k) => DMS.MarginalStructure w k -> NullVectorProjections k
baseNullVectorProjections ms = case ms of
  DMS.MarginalStructure _ _ -> NullVectorProjections cM nullVecs (LA.ident nNullVecs)
  where
    nProbs = S.size $ BRK.elements @k
    cM = DED.mMatrix nProbs $ DMS.msStencils ms
    nullVecs = nullSpaceVectors (DMS.msNumCategories ms) (DMS.msStencils ms)
    nNullVecs = fst $ LA.size nullVecs
-}

-- given list in a order, produce list in b order
permuteList :: forall a b c . DMS.IsomorphicKeys a b -> [c] -> [c]
permuteList ik cs = case ik of
  DMS.IsomorphicKeys abF _ -> fmap snd $ sortOn (abF . fst) $ zip (S.toList $ BRK.elements @a) cs

permutationMatrix :: forall a b . DMS.IsomorphicKeys a b -> LA.Matrix Double
permutationMatrix ik = case ik of
  DMS.IsomorphicKeys abF _ -> let aIndexL = zip (S.toList $ BRK.elements @a) [(0 :: Int)..]
                                  bIndexL = sortOn (abF . fst) aIndexL
                                  mAssoc = fmap (,1) $ zip [0..] $ fmap snd bIndexL
                                  n = length aIndexL
                              in LA.assoc (n, n) 0 mAssoc

mapNullVectorProjections :: DMS.IsomorphicKeys a b -> NullVectorProjections a -> NullVectorProjections b
mapNullVectorProjections ikab nva = case ikab of
  (DMS.IsomorphicKeys _ _) -> case nva of
    (NullVectorProjections cM pM rM) -> let permM = permutationMatrix ikab
                                     in NullVectorProjections (cM LA.<> LA.tr permM) (pM LA.<> LA.tr permM) rM

applyNSPWeights :: ProjectionsToDiff k -> LA.Vector LA.R -> LA.Vector LA.R -> LA.Vector LA.R
applyNSPWeights (RawDiff nvps) projWs pV = pV + projToFull nvps projWs
applyNSPWeights (AvgdDiff aM nvps) projWs pV = pV + aM LA.#> projToFull nvps projWs

type ObjectiveF = forall k . NullVectorProjections k -> LA.Vector Double -> LA.Vector Double -> LA.Vector Double -> (Double, LA.Vector Double)
type OptimalOnSimplexF r =  forall k . ProjectionsToDiff k -> VS.Vector Double -> VS.Vector Double -> K.Sem r (VS.Vector Double)

viaOptimalWeights :: K.KnitEffects r => ObjectiveF -> OptimalOnSimplexF r
viaOptimalWeights objF ptd projWs prodV = do
  let n = VS.sum prodV
      pV = VS.map (/ n) prodV
  ows <- DED.mapPE $ optimalWeights objF (nullVectorProjections ptd) projWs pV
  pure $ VS.map (* n) $ applyNSPWeights ptd ows pV

euclideanNS :: ObjectiveF
euclideanNS _nvps projWs _ v =
  let x = (v - projWs)
  in (VS.sum $ VS.map (^ (2 :: Int)) x, 2 * x)

euclideanFull :: ObjectiveF
euclideanFull nvps projWs _ v =
  let d = v - projWs
      a = projToFullM nvps
      x = a LA.#> d
--      ata = LA.tr (projToFullM nvps) LA.<> (projToFullM nvps)
  in (VS.sum $ VS.map (^ (2 :: Int)) x, LA.tr a LA.#> (2 * x))

klDiv :: ObjectiveF
klDiv nvps projWs pV v =
  let nV = pV + projToFull nvps projWs
      mV = pV + projToFull nvps v
  in (DED.klDiv nV mV, fullToProj nvps (DED.klGrad nV mV))

optimalWeights :: DED.EnrichDataEffects r
               => ObjectiveF
               -> NullVectorProjections k
               -> LA.Vector LA.R
               -> LA.Vector LA.R
               -> K.Sem r (LA.Vector LA.R)
optimalWeights objectiveF nvps projWs pV = do
--  K.logLE K.Info $ "Initial: pV + nsWs <.> nVs = " <> DED.prettyVector (pV + nsWs LA.<# nsVs)
  let n = VS.length projWs
--      prToFull = projToFull nvps
--      scaleGradM = fullToProjM nvps LA.<> LA.tr (fullToProjM nvps)
      objD = objectiveF nvps projWs pV
      constraintData =  L.zip (VS.toList pV) (LA.toColumns $ fullToProjM nvps)
      constraintF :: (Double, LA.Vector LA.R)-> LA.Vector LA.R -> (Double, LA.Vector LA.R)
      constraintF (p, projToNullC) v = (negate (p + v `LA.dot` projToNullC), negate projToNullC)
      constraintFs = fmap constraintF constraintData
      nlConstraintsD = fmap (\cf -> NLOPT.InequalityConstraint (NLOPT.Scalar cf) 1e-6) constraintFs
--      nlConstraints = fmap (\cf -> NLOPT.InequalityConstraint (NLOPT.Scalar $ fst . cf) 1e-5) constraintFs
      maxIters = 1000
      absTol = 1e-6
      absTolV = VS.fromList $ L.replicate n absTol
      nlStop = NLOPT.ParameterAbsoluteTolerance absTolV :| [NLOPT.MaximumEvaluations maxIters]
      nlAlgo = NLOPT.SLSQP objD [] nlConstraintsD []
--      nlAlgo = NLOPT.MMA objD nlConstraintsD
      nlProblem =  NLOPT.LocalProblem (fromIntegral n) nlStop nlAlgo
      nlSol = NLOPT.minimizeLocal nlProblem projWs
  case nlSol of
    Left result -> PE.throw $ DED.TableMatchingException  $ "minConstrained: NLOPT solver failed: " <> show result
    Right solution -> case NLOPT.solutionResult solution of
      NLOPT.MAXEVAL_REACHED -> PE.throw $ DED.TableMatchingException $ "minConstrained: NLOPT Solver hit max evaluations (" <> show maxIters <> ")."
      NLOPT.MAXTIME_REACHED -> PE.throw $ DED.TableMatchingException $ "minConstrained: NLOPT Solver hit max time."
      _ -> do
        let oWs = NLOPT.solutionParams solution
--        K.logLE K.Info $ "solution=" <> DED.prettyVector oWs
--        K.logLE K.Info $ "Solution: pV + oWs <.> nVs = " <> DED.prettyVector (pV + projToFull nvps oWs)
        pure oWs

viaNearestOnSimplex :: OptimalOnSimplexF r
viaNearestOnSimplex nvps projWs prodV = do
  let n = VS.sum prodV
  pure $ VS.map (* n) $ projectToSimplex $ applyNSPWeights nvps projWs (VS.map (/ n) prodV)


-- after Chen & Ye: https://arxiv.org/pdf/1101.6081
projectToSimplex :: VS.Vector Double -> VS.Vector Double
projectToSimplex y = VS.fromList $ fmap (\x -> max 0 (x - tHat)) yL
  where
    yL = VS.toList y
    n = VS.length y
    sY = sort yL
    t i = (FL.fold FL.sum (L.drop i sY) - 1) / realToFrac (n - i)
    tHat = go (n - 1)
    go 0 = t 0
    go k = let tk = t k in if tk > sY L.!! k then tk else go (k - 1)

applyNSPWeightsO :: DED.EnrichDataEffects r => ObjectiveF -> ProjectionsToDiff k -> LA.Vector LA.R -> LA.Vector LA.R -> K.Sem r (LA.Vector LA.R)
applyNSPWeightsO objF ptd nsWs pV = f <$> optimalWeights objF (nullVectorProjections ptd) nsWs pV
  where f oWs = applyNSPWeights ptd oWs pV

-- this aggregates over cells with the same given key
labeledRowsToVecFld :: (Ord k, BRK.FiniteSet k, VS.Storable x, Num x, Monoid a) => Lens' a x -> (row -> k) -> (row -> a) -> FL.Fold row (VS.Vector x)
labeledRowsToVecFld wgtLens key dat
  = VS.fromList . fmap getSum . M.elems <$> (FL.premap (\r -> (key r, Sum $ view wgtLens $ dat r)) DMS.zeroFillSummedMapFld)
{-# SPECIALIZE labeledRowsToVecFld :: (Ord k, BRK.FiniteSet k, Monoid a) => Lens' a Double -> (row -> k) -> (row -> a) -> FL.Fold row (VS.Vector Double) #-}

labeledRowsToListFld :: (Ord k, BRK.FiniteSet k, Monoid w) => (row -> k) -> (row -> w) -> FL.Fold row [w]
labeledRowsToListFld key dat = M.elems <$> (FL.premap (\r -> (key r, dat r)) DMS.zeroFillSummedMapFld)


labeledRowsToNormalizedTableMapFld :: forall a outerK row w . (Ord a, BRK.FiniteSet a, Ord outerK, BRK.FiniteSet outerK, Monoid w)
                                   => Lens' w Double -> (row -> outerK) -> (row -> a) -> (row -> w) -> FL.Fold row (Map outerK (Map a w))
labeledRowsToNormalizedTableMapFld wgtLens outerKey innerKey nF = f <$> labeledRowsToKeyedListFld keyF nF where
  keyF row = (outerKey row, innerKey row)
  f :: [((outerK, a), w)] -> Map outerK (Map a w)
  f = FL.fold (DMS.normalizedTableMapFld wgtLens)

labeledRowsToTableMapFld :: forall a outerK row w . (Ord a, BRK.FiniteSet a, Ord outerK, BRK.FiniteSet outerK, Monoid w)
                         => (row -> outerK) -> (row -> a) -> (row -> w) -> FL.Fold row (Map outerK (Map a w))
labeledRowsToTableMapFld outerKey innerKey nF = f <$> labeledRowsToKeyedListFld keyF nF where
  keyF row = (outerKey row, innerKey row)
  f :: [((outerK, a), w)] -> Map outerK (Map a w)
  f = FL.fold DMS.tableMapFld

labeledRowsToKeyedListFld :: (Ord k, BRK.FiniteSet k, Monoid w) => (row -> k) -> (row -> w) -> FL.Fold row [(k, w)]
labeledRowsToKeyedListFld key dat =  M.toList <$> FL.premap (\r -> (key r, dat r)) DMS.zeroFillSummedMapFld
  -- fmap (zip (S.toList BRK.elements) . VS.toList) $ labeledRowsToVecFld key dat

labeledRowsToVec :: (Ord k, BRK.FiniteSet k, VS.Storable x, Num x, Foldable f, Monoid a)
                 => Lens' a x -> (row -> k) -> (row -> a) -> f row -> VS.Vector x
labeledRowsToVec wl key dat = FL.fold (labeledRowsToVecFld wl key dat)

labeledRowsToList :: (Ord k, BRK.FiniteSet k, Foldable f, Monoid w) => (row -> k) -> (row -> w) -> f row -> [w]
labeledRowsToList key dat = FL.fold (labeledRowsToListFld key dat)

productFromJointFld :: DMS.MarginalStructure w k -> (row -> k) -> (row -> w) -> FL.Fold row [(k, w)]
productFromJointFld ms keyF datF = case ms of
  DMS.MarginalStructure _ ptFld -> FL.fold ptFld . M.toList <$> FL.premap (\r -> (keyF r, datF r)) DMS.zeroFillSummedMapFld

productVecFromJointFld :: DMS.MarginalStructure w k -> Lens' w Double -> (row -> k) -> (row -> w) -> FL.Fold row (VS.Vector Double)
productVecFromJointFld ms wl keyF datF = fmap (VS.fromList . fmap (view wl . snd)) $ productFromJointFld ms keyF datF

diffProjectionsFromJointKeyedList :: forall k w . DMS.MarginalStructure w k
                                -> Lens' w Double
                                -> (VS.Vector Double -> VS.Vector Double)
                                -> [(k, w)]
                                -> VS.Vector Double
diffProjectionsFromJointKeyedList ms wl projDiff kl = projDiff (wgtVec kl - prodWeights kl)
  where
    wgtVec = VS.fromList . fmap (view wl . snd)
    prodWeights :: [(k, w)] -> VS.Vector Double
    prodWeights x = case ms of
      DMS.MarginalStructure _ ptFld -> wgtVec $ FL.fold ptFld x

diffProjectionsFromJointFld :: forall row k w . (BRK.FiniteSet k, Ord k, Monoid w)
                               => DMS.MarginalStructure w k
                               -> Lens' w Double
                               -> (VS.Vector Double -> VS.Vector Double)
                               -> (row -> k)
                               -> (row -> w)
                               -> FL.Fold row (VS.Vector Double)
diffProjectionsFromJointFld ms wl projDiff keyF datF = fmap (diffProjectionsFromJointKeyedList ms wl projDiff . M.toList)
                                                       $ FL.premap (\r -> (keyF r, datF r)) (DMS.normalizeAndFillMapFld wl)


sumLens :: Lens' (Sum x) x
sumLens = lens getSum (\sx x -> Sum x)

applyNSPWeightsFld :: forall outerKs ks count rs r .
                      ( DED.EnrichDataEffects r
                      , V.KnownField count
                      , outerKs V.++ (ks V.++ '[count]) ~ (outerKs V.++ ks) V.++ '[count]
                      , ks F.⊆ (ks V.++ '[count])
                      , Integral (V.Snd count)
                      , F.ElemOf (ks V.++ '[count]) count
                      , Ord (F.Record ks)
                      , BRK.FiniteSet (F.Record ks)
                      , Ord (F.Record outerKs)
                      , outerKs F.⊆ rs
                      , (ks V.++ '[count]) F.⊆ rs
                      , FSI.RecVec (outerKs V.++ (ks V.++ '[count]))
                      )
                   => (F.Record outerKs -> Maybe Text)
                   -> ProjectionsToDiff (F.Record ks) -- ?
                   -> (F.Record outerKs -> FL.FoldM (K.Sem r) (F.Record (ks V.++ '[count])) (LA.Vector Double))
                   -> FL.FoldM (K.Sem r) (F.Record rs) (F.FrameRec (outerKs V.++ ks V.++ '[count]))
applyNSPWeightsFld logM ptd nsWsFldF =
  let keysL = S.toList $ BRK.elements @(F.Record ks) -- these are the same for each outerK
      precomputeFld :: F.Record outerKs -> FL.FoldM (K.Sem r) (F.Record (ks V.++ '[count])) (LA.Vector LA.R, Double, LA.Vector LA.R)
      precomputeFld ok =
        let sumFld = FL.generalize $ FL.premap (realToFrac . F.rgetField @count) FL.sum
            vecFld = FL.generalize $ labeledRowsToVecFld sumLens (F.rcast @ks) (Sum . realToFrac . F.rgetField @count)
        in (,,) <$> nsWsFldF ok <*> sumFld <*> vecFld
      compute :: F.Record outerKs -> (LA.Vector LA.R, Double, LA.Vector LA.R) -> K.Sem r [F.Record (outerKs V.++ ks V.++ '[count])]
      compute ok (nsWs, vSum, v) = do
        maybe (pure ()) (\msg -> K.logLE K.Info $ msg <> " nsWs=" <> DED.prettyVector nsWs) $ logM ok
        let optimalV = VS.map (* vSum) $ projectToSimplex $ applyNSPWeights ptd nsWs $ VS.map (/ vSum) v
--        optimalV <- fmap (VS.map (* vSum)) $ applyNSPWeightsO nvps nsWs $ VS.map (/ vSum) v
        pure $ zipWith (\k c -> ok F.<+> k F.<+> FT.recordSingleton @count (round c)) keysL (VS.toList optimalV)
      innerFld ok = FMR.postMapM (compute ok) (precomputeFld ok)
  in  FMR.concatFoldM
        $ FMR.mapReduceFoldM
        (FMR.generalizeUnpack $ FMR.noUnpack)
        (FMR.generalizeAssign $ FMR.assignKeysAndData @outerKs @(ks V.++ '[count]))
        (FMR.ReduceFoldM $ \k -> F.toFrame <$> innerFld k)


applyNSPWeightsFldG :: forall outerK k w r .
                      ( DED.EnrichDataEffects r
                      , Monoid w
                      , BRK.FiniteSet k
                      , Ord k
                      , Ord outerK
                      )
                    => Lens' w Double
                    -> (Double -> w -> w) -- not sure if I can make this update follow lens laws
                    -> (outerK -> Maybe Text)
                    -> ProjectionsToDiff k
                    -> (outerK -> FL.FoldM (K.Sem r) (k, w) (LA.Vector Double))
                    -> FL.FoldM (K.Sem r) (outerK, k, w) [(outerK, k, w)]
applyNSPWeightsFldG wl updateW logM ptd nsWsFldF =
  let keysL = S.toList $ BRK.elements @k -- these are the same for each outerK
      okF (ok, _, _) = ok
      kwF (_, k, w) = (k, w)
      precomputeFld :: outerK -> FL.FoldM (K.Sem r) (k, w) (LA.Vector Double, Double, [w])
      precomputeFld ok =
        let sumFld = FL.generalize $ FL.premap (view wl . snd) FL.sum
            vecFld = FL.generalize $ labeledRowsToListFld fst snd
        in (,,) <$> nsWsFldF ok <*> sumFld <*> vecFld
      compute :: outerK -> (LA.Vector LA.R, Double, [w]) -> K.Sem r [(outerK, k, w)]
      compute ok (nsWs, vSum, ws) = do
        maybe (pure ()) (\msg -> K.logLE K.Info $ msg <> " nsWs=" <> DED.prettyVector nsWs) $ logM ok
        let optimalV = VS.map (* vSum) $ projectToSimplex $ applyNSPWeights ptd nsWs $ VS.fromList $ fmap ((/ vSum) . view wl) ws
--        optimalV <- fmap (VS.map (* vSum)) $ applyNSPWeightsO nvps nsWs $ VS.map (/ vSum) v
        pure $ List.zipWith3 (\k c w -> (ok, k, updateW c w)) keysL (VS.toList optimalV) ws
      innerFldM ok = FMR.postMapM (compute ok) (precomputeFld ok)
  in  MR.concatFoldM
        $ MR.mapReduceFoldM
        (MR.generalizeUnpack MR.noUnpack)
        (MR.generalizeAssign $ MR.assign okF kwF)
        (FMR.ReduceFoldM innerFldM)

nullSpaceVectorsMS :: forall k w . DMS.MarginalStructure w k -> LA.Matrix LA.R
nullSpaceVectorsMS = \cases
  (DMS.MarginalStructure subsets _) -> nullSpaceVectors (S.size $ BRK.elements @k) $ fmap DMS.subsetToStencil subsets

nullSpaceVectors :: Int -> [DED.Stencil Int] -> LA.Matrix LA.R
nullSpaceVectors n sts = LA.tr $ LA.nullspace $ DED.mMatrix n sts

nullSpaceProjections :: (Ord k, Ord outerK, Show outerK, DED.EnrichDataEffects r, Foldable f)
                            => LA.Matrix LA.R
                            -> (row -> outerK)
                            -> (row -> k)
                            -> (row -> Double)
                            -> f row
                            -> f row
                            -> K.Sem r (Map outerK (Double, LA.Vector LA.R))
nullSpaceProjections nullVs outerKey key wgt actuals products = do
  let normalizedVec' m s = VS.fromList $ (/ s)  <$> M.elems m
      mapData m s = (s, normalizedVec' m s)
      toDatFld = mapData <$> FL.map <*> FL.premap snd FL.sum
      toMapFld = FL.premap (\r -> (outerKey r, (key r, wgt r))) $ FL.foldByKeyMap toDatFld
      actualM = FL.fold toMapFld actuals
      prodM = FL.fold toMapFld products
      whenMatchedF _ (na, aV) (_, pV) = pure (na, nullVs LA.#> (aV - pV))
      whenMissingAF outerK _ = PE.throw $ DED.TableMatchingException $ "averageNullSpaceProjections: Missing actuals for outerKey=" <> show outerK
      whenMissingPF outerK _ = PE.throw $ DED.TableMatchingException $ "averageNullSpaceProjections: Missing product for outerKey=" <> show outerK
  MM.mergeA (MM.traverseMissing whenMissingAF) (MM.traverseMissing whenMissingPF) (MM.zipWithAMatched whenMatchedF) actualM prodM

avgNullSpaceProjections :: (Double -> Double) -> Map a (Double, LA.Vector LA.R) -> LA.Vector LA.R
avgNullSpaceProjections popWeightF m = VS.map (/ totalWeight) . VS.fromList . fmap (FL.fold FL.mean . VS.toList) . LA.toColumns . LA.fromRows . fmap weight $ M.elems m
  where
    totalWeight = FL.fold (FL.premap (popWeightF . fst) FL.sum) m / realToFrac (M.size m)
    weight (n, v) = VS.map (* popWeightF n) v

diffCovarianceFldMS :: forall outerK k row w .
                       (Ord outerK)
                    => Lens' w Double
                    -> (row -> outerK)
                    -> (row -> k)
                    -> (row -> w)
                    -> DMS.MarginalStructure w k
                    -> FL.Fold row (LA.Vector Double, LA.Herm Double)
diffCovarianceFldMS wl outerKey catKey dat = \cases
  (DMS.MarginalStructure subsets ptFld) -> diffCovarianceFld wl outerKey catKey dat
                                           (fmap DMS.subsetToStencil subsets)
                                           (fmap snd . FL.fold ptFld)

diffCovarianceFld :: forall outerK k row w .
                     (Ord outerK, Ord k, BRK.FiniteSet k, Monoid w)
                  => Lens' w Double
                  -> (row -> outerK)
                  -> (row -> k)
                  -> (row -> w)
                  -> [DED.Stencil Int]
                  -> ([(k, w)] -> [w])
                  -> FL.Fold row (LA.Vector LA.R, LA.Herm LA.R)
diffCovarianceFld wl outerKey catKey dat sts prodTableF = LA.meanCov . LA.fromRows <$> vecsFld
  where
    allKs :: Set k = BRK.elements
    n = S.size allKs
    nullVecs' = nullSpaceVectors n sts
    wgtVec = VS.fromList . fmap (view wl)
    pcF :: [w] -> VS.Vector Double
    pcF = wgtVec . prodTableF . zip (S.toList allKs)
    projections ws = let ws' = DMS.normalize wl ws in nullVecs' LA.#> (wgtVec ws' - pcF ws')
    innerFld = projections <$> labeledRowsToListFld fst snd
    vecsFld = MR.mapReduceFold
              MR.noUnpack
              (MR.assign outerKey (\row -> (catKey row, dat row)))
              (MR.foldAndLabel innerFld (\_ v -> v))

significantNullVecsMS :: forall k w . Double
                      -> DMS.MarginalStructure w k
                      -> LA.Herm LA.R
                      -> NullVectorProjections k
significantNullVecsMS fracCovToInclude ms cov = case ms of
  (DMS.MarginalStructure _ _) -> significantNullVecs fracCovToInclude cM (nullSpaceVectorsMS ms) cov
    where
      cM = DED.mMatrix (S.size $ BRK.elements @k) (DMS.msStencils ms)

significantNullVecs :: (Ord k, BRK.FiniteSet k)
                    => Double
                    -> LA.Matrix LA.R
                    -> LA.Matrix LA.R
                    -> LA.Herm LA.R
                    -> NullVectorProjections k
significantNullVecs fracCovToInclude cMatrix nullVecs cov = NullVectorProjections cMatrix (LA.tr sigEVecs LA.<> nullVecs) eigVecs
  where
    (eigVals, eigVecs) = LA.eigSH cov
    totCov = VS.sum eigVals
    nSig = fst $ VS.foldl' (\(m, csf) c -> if csf / totCov < fracCovToInclude then (m + 1, csf + c) else (m, csf)) (0, 0) eigVals
    sigEVecs = LA.takeColumns nSig eigVecs


uncorrelatedNullVecsMS :: forall k w . DMS.MarginalStructure w k -> LA.Herm LA.R -> NullVectorProjections k
uncorrelatedNullVecsMS ms = case ms of
  (DMS.MarginalStructure _ _) -> uncorrelatedNullVecs cM (nullSpaceVectorsMS ms)
    where
      cM = DED.mMatrix (S.size $ BRK.elements @k) (DMS.msStencils ms)

uncorrelatedNullVecs :: (Ord k, BRK.FiniteSet k)
                     => LA.Matrix LA.R
                     -> LA.Matrix LA.R
                     -> LA.Herm LA.R
                     -> NullVectorProjections k
uncorrelatedNullVecs cMatrix nullVecs cov = NullVectorProjections cMatrix nullVecs eigVecs
  where
    (_, eigVecs) = LA.eigSH cov

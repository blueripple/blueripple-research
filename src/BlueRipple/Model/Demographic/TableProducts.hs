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

import qualified BlueRipple.Configuration as BR
import qualified BlueRipple.Utilities.KnitUtils as BRKU
import qualified BlueRipple.Model.Demographic.DataPrep as DDP
import qualified BlueRipple.Model.Demographic.EnrichData as DED
import qualified BlueRipple.Model.Demographic.MarginalStructure as DMS

import qualified BlueRipple.Data.Keyed as BRK
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.GeographicTypes as GT

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

import Control.Lens (view)
import GHC.TypeLits (Symbol)

import qualified CmdStan as CS
import qualified Stan.ModelBuilder as SMB
import qualified Stan.ModelRunner as SMR
import qualified Stan.ModelConfig as SC
import qualified Stan.Parameters as SP
import qualified Stan.RScriptBuilder as SR
import qualified Stan.ModelBuilder.BuildingBlocks as SBB
import qualified Stan.ModelBuilder.DesignMatrix as DM
import qualified Stan.ModelBuilder.TypedExpressions.Types as TE
import qualified Stan.ModelBuilder.TypedExpressions.Statements as TE
import qualified Stan.ModelBuilder.TypedExpressions.Indexing as TEI
import qualified Stan.ModelBuilder.TypedExpressions.Operations as TEO
import qualified Stan.ModelBuilder.TypedExpressions.DAG as DAG
import qualified Stan.ModelBuilder.TypedExpressions.StanFunctions as SF
import Stan.ModelBuilder.TypedExpressions.TypedList (TypedList(..))
import qualified Flat

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

data NullVectorProjections =
  NullVectorProjections { nvpProj :: LA.Matrix LA.R, nvpRot :: LA.Matrix LA.R }

fullToProjM :: NullVectorProjections -> LA.Matrix LA.R
fullToProjM (NullVectorProjections pM rM) = LA.tr rM LA.<> pM

projToFullM :: NullVectorProjections -> LA.Matrix LA.R
projToFullM (NullVectorProjections pM rM) = LA.tr pM <> rM

projToFull :: NullVectorProjections -> LA.Vector LA.R -> LA.Vector LA.R
projToFull nvps v = v LA.<# fullToProjM nvps

fullToProj :: NullVectorProjections -> LA.Vector LA.R -> LA.Vector LA.R
fullToProj nvps v = fullToProjM nvps LA.#> v

baseNullVectorProjections :: DMS.MarginalStructure k -> NullVectorProjections
baseNullVectorProjections ms = NullVectorProjections nullVecs (LA.ident nNullVecs)
  where
    nullVecs = nullSpaceVectors (DMS.msNumCategories ms) (DMS.msStencils ms)
    nNullVecs = fst $ LA.size nullVecs

applyNSPWeights :: NullVectorProjections -> LA.Vector LA.R -> LA.Vector LA.R -> LA.Vector LA.R
applyNSPWeights nvps projWs pV = pV + projToFull nvps projWs

optimalWeights :: DED.EnrichDataEffects r => NullVectorProjections -> LA.Vector LA.R -> LA.Vector LA.R -> K.Sem r (LA.Vector LA.R)
optimalWeights nvps projWs pV = do
--  K.logLE K.Info $ "Initial: pV + nsWs <.> nVs = " <> DED.prettyVector (pV + nsWs LA.<# nsVs)
  let n = VS.length projWs
--      nsVs2 = nsVs LA.<> LA.tr nsVs
      -- We minimize (Euclidean, but why?) distance between (v - nsWs)nsVs nsVs' (v - nsWs)'
      prToFull = projToFull nvps
      scaleGradM = fullToProjM nvps LA.<> LA.tr (fullToProjM nvps)
      objD v =
--        let x = (v - projWs) in (VS.sum $ VS.map (^ (2 :: Int)) x, 2 * x)
        let x = (v - projWs) in (VS.sum $ VS.map (^ (2 :: Int)) $ prToFull x, 2 * (scaleGradM LA.#> x))


  {-      obj2D v =
        let srNormV = Numeric.sqrt (LA.norm_2 v)
            srNormN = Numeric.sqrt (LA.norm_2 nsWs)
            nRatio = srNormN / srNormV
        in (v `LA.dot` nsWs - srNormV * srNormN, nsWs - (LA.scale nRatio v))
-}
--      obj v = fst . objD
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

-- after Chen & Ye: https://arxiv.org/pdf/1101.6081
projectToSimplex :: VS.Vector Double -> VS.Vector Double
projectToSimplex y = VS.fromList $ fmap (\x -> max 0 (x-tHat)) yL
  where
    yL = VS.toList y
    n = VS.length y
    sY = sort yL
    t i = (FL.fold FL.sum (L.drop i sY) - 1) / realToFrac (n - i)
    tHat = go (n - 1)
    go 0 = t 0
    go k = let tk = t k in if tk > sY L.!! k then tk else go (k - 1)

applyNSPWeightsO :: DED.EnrichDataEffects r => NullVectorProjections -> LA.Vector LA.R -> LA.Vector LA.R -> K.Sem r (LA.Vector LA.R)
applyNSPWeightsO  nvps nsWs pV = f <$> optimalWeights nvps nsWs pV
  where f oWs = applyNSPWeights nvps oWs pV

-- this aggregates over cells with the same given key
labeledRowsToVecFld :: (Ord k, BRK.FiniteSet k, VS.Storable a, Num a) => (row -> k) -> (row -> a) -> FL.Fold row (VS.Vector a)
labeledRowsToVecFld key dat = VS.fromList . M.elems <$> (FL.premap (\r -> (key r, dat r)) DMS.zeroFillSummedMapFld)

labeledRowsToNormalizedTableMapFld :: forall a outerK row . (Ord a, BRK.FiniteSet a, Ord outerK, BRK.FiniteSet outerK)
                                   => (row -> outerK) -> (row -> a) -> (row -> Double) -> FL.Fold row (Map outerK (Map a Double))
labeledRowsToNormalizedTableMapFld outerKey innerKey nF = f <$> labeledRowsToKeyedListFld keyF nF where
  keyF row = (outerKey row, innerKey row)
  f :: [((outerK, a), Double)] -> Map outerK (Map a Double)
  f = FL.fold DMS.normalizedTableMapFld

labeledRowsToTableMapFld :: forall a outerK row . (Ord a, BRK.FiniteSet a, Ord outerK, BRK.FiniteSet outerK)
                         => (row -> outerK) -> (row -> a) -> (row -> Double) -> FL.Fold row (Map outerK (Map a Double))
labeledRowsToTableMapFld outerKey innerKey nF = f <$> labeledRowsToKeyedListFld keyF nF where
  keyF row = (outerKey row, innerKey row)
  f :: [((outerK, a), Double)] -> Map outerK (Map a Double)
  f = FL.fold DMS.tableMapFld

labeledRowsToKeyedListFld :: (Ord k, BRK.FiniteSet k, Num a) => (row -> k) -> (row -> a) -> FL.Fold row [(k, a)]
labeledRowsToKeyedListFld key dat =  M.toList <$> FL.premap (\r -> (key r, dat r)) DMS.zeroFillSummedMapFld
  -- fmap (zip (S.toList BRK.elements) . VS.toList) $ labeledRowsToVecFld key dat

labeledRowsToVec :: (Ord k, BRK.FiniteSet k, VS.Storable a, Foldable f, Num a) => (row -> k) -> (row -> a) -> f row -> VS.Vector a
labeledRowsToVec key dat = FL.fold (labeledRowsToVecFld key dat)

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
                   -> NullVectorProjections
                   -> (F.Record outerKs -> FL.FoldM (K.Sem r) (F.Record (ks V.++ '[count])) (LA.Vector LA.R))
                   -> FL.FoldM (K.Sem r) (F.Record rs) (F.FrameRec (outerKs V.++ ks V.++ '[count]))
applyNSPWeightsFld logM nvps nsWsFldF =
  let keysL = S.toList $ BRK.elements @(F.Record ks) -- these are the same for each outerK
      precomputeFld :: F.Record outerKs -> FL.FoldM (K.Sem r) (F.Record (ks V.++ '[count])) (LA.Vector LA.R, Double, LA.Vector LA.R)
      precomputeFld ok =
        let sumFld = FL.generalize $ FL.premap (realToFrac . F.rgetField @count) FL.sum
            vecFld = FL.generalize $ labeledRowsToVecFld (F.rcast @ks) (realToFrac . F.rgetField @count)
        in (,,) <$> nsWsFldF ok <*> sumFld <*> vecFld
      compute :: F.Record outerKs -> (LA.Vector LA.R, Double, LA.Vector LA.R) -> K.Sem r [F.Record (outerKs V.++ ks V.++ '[count])]
      compute ok (nsWs, vSum, v) = do
        maybe (pure ()) (\msg -> K.logLE K.Info $ msg <> " nsWs=" <> DED.prettyVector nsWs) $ logM ok
        let optimalV = VS.map (* vSum) $ projectToSimplex $ applyNSPWeights nvps nsWs $ VS.map (/ vSum) v
--        optimalV <- fmap (VS.map (* vSum)) $ applyNSPWeightsO nvps nsWs $ VS.map (/ vSum) v
        pure $ zipWith (\k c -> ok F.<+> k F.<+> FT.recordSingleton @count (round c)) keysL (VS.toList optimalV)
      innerFld ok = FMR.postMapM (compute ok) (precomputeFld ok)
  in  FMR.concatFoldM
        $ FMR.mapReduceFoldM
        (FMR.generalizeUnpack $ FMR.noUnpack)
        (FMR.generalizeAssign $ FMR.assignKeysAndData @outerKs @(ks V.++ '[count]))
        (FMR.ReduceFoldM $ \k -> F.toFrame <$> innerFld k)

nullSpaceVectorsMS :: forall k . DMS.MarginalStructure k -> LA.Matrix LA.R
nullSpaceVectorsMS = \cases
  (DMS.MarginalStructure sts _) -> nullSpaceVectors (S.size $ BRK.elements @k) sts

nullSpaceVectors :: Int -> [DED.Stencil Int] -> LA.Matrix LA.R
nullSpaceVectors n sts = LA.tr $ LA.nullspace $ DED.mMatrix n sts

nullSpaceProjections :: (Ord k, Ord outerK, Show outerK, DED.EnrichDataEffects r, Foldable f)
                            => LA.Matrix LA.R
                            -> (row -> outerK)
                            -> (row -> k)
                            -> (row -> Int)
                            -> f row
                            -> f row
                            -> K.Sem r (Map outerK (Double, LA.Vector LA.R))
nullSpaceProjections nullVs outerKey key count actuals products = do
  let normalizedVec' m s = VS.fromList $ (/ s)  <$> M.elems m
      mapData m s = (s, normalizedVec' m s)
      toDatFld = mapData <$> FL.map <*> FL.premap snd FL.sum
      toMapFld = FL.premap (\r -> (outerKey r, (key r, realToFrac (count r)))) $ FL.foldByKeyMap toDatFld
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

diffCovarianceFldMS :: forall outerK k row .
                       (Ord outerK)
                    => (row -> outerK)
                    -> (row -> k)
                    -> (row -> LA.R)
                    -> DMS.MarginalStructure k
                    -> FL.Fold row (LA.Vector LA.R, LA.Herm LA.R)
diffCovarianceFldMS outerKey catKey dat = \cases
  (DMS.MarginalStructure sts ptFld) -> diffCovarianceFld outerKey catKey dat sts (fmap snd . FL.fold ptFld)

diffCovarianceFld :: forall outerK k row .
                     (Ord outerK, Ord k, BRK.FiniteSet k)
                  => (row -> outerK)
                  -> (row -> k)
                  -> (row -> LA.R)
                  -> [DED.Stencil Int]
                  -> ([(k, Double)] -> [Double])
                  -> FL.Fold row (LA.Vector LA.R, LA.Herm LA.R)
diffCovarianceFld outerKey catKey dat sts prodTableF = LA.meanCov . LA.fromRows <$> vecsFld
  where
    allKs :: Set k = BRK.elements
    n = S.size allKs
    nullVecs' = nullSpaceVectors n sts
    pcF :: VS.Vector Double -> VS.Vector Double
    pcF = VS.fromList . prodTableF . zip (S.toList allKs) . VS.toList
    projections v = let w = normalizedVec v in nullVecs' LA.#> (w - pcF w)
    innerFld = projections <$> labeledRowsToVecFld fst snd
    vecsFld = MR.mapReduceFold
              MR.noUnpack
              (MR.assign outerKey (\row -> (catKey row, dat row)))
              (MR.foldAndLabel innerFld (\_ v -> v))


significantNullVecsMS :: Double
                      -> DMS.MarginalStructure k
                      -> LA.Herm LA.R
                      -> NullVectorProjections
significantNullVecsMS fracCovToInclude ms cov = significantNullVecs fracCovToInclude (nullSpaceVectorsMS ms) cov

significantNullVecs :: Double
                    -> LA.Matrix LA.R
                    -> LA.Herm LA.R
                    -> NullVectorProjections
significantNullVecs fracCovToInclude nullVecs cov = NullVectorProjections (LA.tr sigEVecs LA.<> nullVecs) eigVecs
  where
    (eigVals, eigVecs) = LA.eigSH cov
    totCov = VS.sum eigVals
    nSig = fst $ VS.foldl' (\(m, csf) c -> if csf / totCov < fracCovToInclude then (m + 1, csf + c) else (m, csf)) (0, 0) eigVals
    sigEVecs = LA.takeColumns nSig eigVecs


uncorrelatedNullVecsMS :: DMS.MarginalStructure k -> LA.Herm LA.R -> NullVectorProjections
uncorrelatedNullVecsMS ms = uncorrelatedNullVecs (nullSpaceVectorsMS ms)

uncorrelatedNullVecs :: LA.Matrix LA.R
                     -> LA.Herm LA.R
                     -> NullVectorProjections
uncorrelatedNullVecs nullVecs cov = NullVectorProjections nullVecs eigVecs
  where
    (_, eigVecs) = LA.eigSH cov

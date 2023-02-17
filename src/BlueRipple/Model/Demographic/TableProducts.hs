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

module BlueRipple.Model.Demographic.TableProducts
  (
    module BlueRipple.Model.Demographic.TableProducts
  )
where

import qualified BlueRipple.Model.Demographic.DataPrep as DDP
import qualified BlueRipple.Model.Demographic.EnrichData as DED

import qualified BlueRipple.Data.Keyed as BRK
import qualified BlueRipple.Data.DemographicTypes as DT

import qualified Knit.Report as K
--import qualified Knit.Effect.Logger as K
import qualified Knit.Effect.PandocMonad as KPM
import qualified Text.Pandoc.Error as PA
import qualified Polysemy as P
import qualified Polysemy.Error as PE

import qualified Control.MapReduce.Simple as MR
import qualified Frames.MapReduce as FMR
import qualified Frames.Folds as FF
import qualified Frames.Streamly.Transform as FST
import qualified Frames.Streamly.InCore as FSI
import qualified Streamly.Prelude as Streamly

import qualified Control.Foldl as FL
import qualified Data.IntMap as IM
import qualified Data.List as L
import qualified Data.Map.Strict as M
import qualified Data.Map.Merge.Strict as MM
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
import qualified Data.Vector.Storable as VS

import Control.Monad.Except (throwError)
import GHC.TypeLits (Symbol)


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

applyNSPWeights :: LA.Matrix LA.R -> LA.Vector LA.R -> LA.Vector LA.R -> LA.Vector LA.R
applyNSPWeights nsVs nsWs pV = pV + nsWs LA.<# nsVs

applyNSPWeightsO :: DED.EnrichDataEffects r => LA.Matrix LA.R -> LA.Vector LA.R -> LA.Vector LA.R -> K.Sem r (LA.Vector LA.R)
applyNSPWeightsO  nsVs nsWs pV = do
  let n = VS.length nsWs
--      nP = VS.length pV
      objD v = (LA.norm_2 (v - nsWs), 2 * (v - nsWs))
      constraintData =  L.zip (VS.toList pV) (LA.toColumns nsVs)
      constraintF :: (Double, LA.Vector LA.R)-> LA.Vector LA.R -> (Double, LA.Vector LA.R)
      constraintF (p, nullC) v = (negate $ p + (v `LA.dot` nullC), negate nullC)
      constraintFs = fmap constraintF $ constraintData
      nlConstraintsD = fmap (\cf -> NLOPT.InequalityConstraint (NLOPT.Scalar cf) 1e-5) constraintFs
--      constraintV v =  VS.map negate $ pV + v LA.<# nsVs
--      constraintH _ = (n LA.>< nP) $ repeat (2 :: Double)
--      constraintF v _ = (constraintV v, constraintH v)
--      nlConstraintsD = NLOPT.InequalityConstraint (NLOPT.Vector (fromIntegral nP) constraintF) 1e-5
      maxIters = 50
      absTolV = VS.fromList $ L.replicate n 1e-6
      nlStop = NLOPT.ParameterAbsoluteTolerance absTolV :| [NLOPT.MaximumEvaluations maxIters]
      nlAlgo = NLOPT.MMA objD nlConstraintsD
      nlProblem =  NLOPT.LocalProblem (fromIntegral n) nlStop nlAlgo
      nlSol = NLOPT.minimizeLocal nlProblem nsWs
  case nlSol of
    Left result -> PE.throw $ DED.TableMatchingException  $ "minConstrained: NLOPT solver failed: " <> show result
    Right solution -> case NLOPT.solutionResult solution of
      NLOPT.MAXEVAL_REACHED -> PE.throw $ DED.TableMatchingException $ "minConstrained: NLOPT Solver hit max evaluations (" <> show maxIters <> ")."
      NLOPT.MAXTIME_REACHED -> PE.throw $ DED.TableMatchingException $ "minConstrained: NLOPT Solver hit max time."
      _ -> pure $ NLOPT.solutionParams solution

applyNSPWeightsFld :: forall outerKs ks count rs r .
                      ( DED.EnrichDataEffects r
                      , V.KnownField count
                      , outerKs V.++ (ks V.++ '[count]) ~ (outerKs V.++ ks) V.++ '[count]
                      , ks F.⊆ (ks V.++ '[count])
                      , Real (V.Snd count)
                      , Integral (V.Snd count)
                      , F.ElemOf (ks V.++ '[count]) count
                      , F.ElemOf rs count
                      , Ord (F.Record ks)
                      , Ord (F.Record outerKs)
                      , outerKs F.⊆ rs
                      , ks F.⊆ rs
                      , (ks V.++ '[count]) F.⊆ rs
                      , FSI.RecVec (outerKs V.++ (ks V.++ '[count]))
                      )
                   => LA.Matrix LA.R
                   -> LA.Vector LA.R
                   -> FL.FoldM (K.Sem r) (F.Record rs) (F.FrameRec (outerKs V.++ ks V.++ '[count]))
applyNSPWeightsFld nsVs nsWs =
  let pm :: F.Record (ks V.++ '[count]) -> (F.Record ks, Double)
      pm d = (F.rcast @ks d, realToFrac $ F.rgetField @count d)
      computeM :: Map (F.Record ks) Double -> Double -> K.Sem r [F.Record (ks V.++ '[count])]
      computeM m s = (\(ks, mcs) -> fmap (\cs -> L.zipWith (\ks c -> ks F.<+> FT.recordSingleton @count c) ks cs) mcs)
                    $ second (fmap (fmap round . VS.toList . VS.map (* s)) . applyNSPWeightsO nsVs nsWs . VS.map (/ s) . VS.fromList)
                    $ unzip
                    $ M.toList m
--      fld1M :: FL.FoldM (K.Sem r) (F.Record ks, Double) [F.Record (ks V.++ '[count])]
      fld1M = FMR.postMapM (uncurry computeM) $ (,) <$> FL.generalize FL.map <*> FL.generalize (FL.premap snd FL.sum)
--      fldM :: F.Record outerKs -> FL.FoldM (K.Sem r) (F.Record (ks V.++ '[count])) [F.Record (outerKs V.++ ks V.++ '[count])]
      fldM k = fmap (fmap (k F.<+>)) $ FL.premapM (pure . pm) fld1M
  in  FMR.concatFoldM
        $ FMR.mapReduceFoldM
        (FMR.generalizeUnpack $ FMR.noUnpack)
        (FMR.generalizeAssign $ FMR.assignKeysAndData @outerKs @(ks V.++ '[count]))
        (FMR.ReduceFoldM $ \k -> F.toFrame <$> fldM k)

nullSpaceVectors :: Int -> [DED.Stencil Int] -> LA.Matrix LA.R
nullSpaceVectors n stencilSumsI = LA.tr $ LA.nullspace $ DED.mMatrix n stencilSumsI

averageNullSpaceProjections :: (Ord k, Ord outerK, Show outerK, DED.EnrichDataEffects r)
                            => LA.Matrix LA.R
                            -> (F.Record rs -> outerK)
                            -> (F.Record rs -> k)
                            -> (F.Record rs -> Int)
                            -> F.FrameRec rs
                            -> F.FrameRec rs
                            -> K.Sem r (LA.Vector LA.R)
averageNullSpaceProjections nullVs outerKey key count actuals products = do
  let normalizedVec m s = VS.fromList $ (/ s)  <$> M.elems m
      toVecFld = normalizedVec <$> FL.map <*> FL.premap snd FL.sum
      toMapFld = FL.premap (\r -> (outerKey r, (key r, realToFrac (count r)))) $ FL.foldByKeyMap toVecFld
      actualM = FL.fold toMapFld actuals
      prodM = FL.fold toMapFld products
      whenMatchedF k aV pV = pure $ nullVs LA.#> (aV - pV)
      whenMissingAF outerK _ = PE.throw $ DED.TableMatchingException $ "averageNullSpaceProjections: Missing actuals for outerKey=" <> show outerK
      whenMissingPF outerK _ = PE.throw $ DED.TableMatchingException $ "averageNullSpaceProjections: Missing product for outerKey=" <> show outerK
  computedM <- MM.mergeA (MM.traverseMissing whenMissingAF) (MM.traverseMissing whenMissingPF) (MM.zipWithAMatched whenMatchedF) actualM prodM
  let avgNSP = VS.fromList $ fmap (FL.fold FL.mean . VS.toList) $ LA.toColumns $ LA.fromRows $ M.elems computedM
  pure avgNSP

stencils :: forall (as :: [(Symbol, Type)]) (bs :: [(Symbol, Type)]) .
            (Ord (F.Record as)
            , Ord (F.Record bs)
            , as F.⊆ bs
            , BRK.FiniteSet (F.Record bs)
            )
         => [DED.Stencil Int]
stencils = M.elems $ FL.fold (DED.subgroupStencils (F.rcast @as)) $ S.toList $ BRK.elements @(F.Record bs)

{-
data TableProductException = TableProductException Text
                           deriving stock (Show)

instance Exception TableProductException

-- IO here because we need to be able to execute things requiring a PrimMonad environment
-- for Frames inCore
--type TableProductEffects r = (K.LogWithPrefixesLE r, P.Member (PE.Error TableProductException) r, P.Member (P.Embed IO) r)

tableProductExceptionToPandocError :: TableProductException -> KPM.PandocError
tableProductExceptionToPandocError = PA.PandocSomeError . KPM.textToPandocText . show


mapTPE :: P.Member (PE.Error PA.PandocError) r => K.Sem (PE.Error TableProductException  ': r) a -> K.Sem r a
mapTPE = PE.mapError tableProductExceptionToPandocError
-}

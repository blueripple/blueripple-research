{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE LambdaCase #-}

module Stan.ModelBuilder.GroupModel
  (
    module Stan.ModelBuilder.GroupModel
  , module Stan.ModelBuilder.SumToZero
  )
  where

import Prelude hiding (All)
import qualified Data.Map as Map
import qualified Data.Set as Set
import qualified Stan.ModelBuilder.Distributions as SD
import qualified Stan.ModelBuilder as SB
import qualified Stan.ModelBuilder.SumToZero as STZ
import Stan.ModelBuilder.SumToZero (SumToZero(..))

{-
import qualified Stan.ModelBuilder.TypedExpressions.Functions as TE
import qualified Stan.ModelBuilder.TypedExpressions.Expressions as TE
import qualified Stan.ModelBuilder.TypedExpressions.Operations as TE
import Stan.ModelBuilder.TypedExpressions.Recursion (hfmap, htraverse, K(..))
-}
import qualified Stan.ModelBuilder.TypedExpressions.Types as TE
import qualified Stan.ModelBuilder.TypedExpressions.TypedList as TE
import qualified Stan.ModelBuilder.TypedExpressions.Functions as TE
import qualified Stan.ModelBuilder.TypedExpressions.StanFunctions as TE
import qualified Stan.ModelBuilder.TypedExpressions.Statements as TE
import Stan.ModelBuilder.TypedExpressions.ParameterBuilder

import qualified Stan.ModelBuilder as TE
import Data.Vec.Lazy (Vec(..))
import Stan.ModelBuilder.TypedExpressions.Types (TypeOneOf)
import Stan.ModelBuilder.TypedExpressions.Statements (DensityWithArgs)
import qualified Data.GADT.Compare as GC
import qualified Data.Type.Equality as GC
import qualified Data.Dependent.Map as DM
import qualified Data.Dependent.Sum as DM
import qualified Data.Graph as Gr
import qualified Data.Some as Some
import qualified Control.Foldl as FL
import Data.Aeson.KeyMap (keys)
import Streamly.Internal.Data.SVar (SVar(needDoorBell))

-- various special cases

simpleP :: TE.NamedDeclSpec t -> TE.DensityWithArgs t -> Parameter t
simpleP nds (TE.DensityWithArgs d dArgs) = BuildP $ UntransformedP nds [] (exprListToParameters dArgs) d

centeredHP :: TE.NamedDeclSpec t -> Parameters ts -> TE.Density t ts -> Parameter t
centeredHP nds ps d = BuildP $ UntransformedP [] nds ps d

nonCenteredHP :: TE.NamedDeclSpec t
              -> Parameters ts
              -> TE.DensityWithArgs t
              -> (TE.ExprList ts -> TE.UExpr t -> TE.UExpr t)
              -> Parameter t
nonCenteredHP nds hps (TE.DensityWithArgs d dargs) ncF =
  BuildP $ TransformedSameTypeP nds [] (exprListToParameters dargs) d hps ncF

transformedHP :: TE.NamedDeclSpec t
              -> Maybe [TE.VarModifier TE.UExpr (TE.ScalarType t)]
              -> TE.DensityWithArgs t
              -> (TE.UExpr t -> TE.UExpr t)
              -> Parameter t
transformedHP nds rawCsM rawPrior fromRawF =
  let TE.DeclSpec st dims cs = TE.decl nds
      rawDS = maybe (TE.decl nds) (TE.DeclSpec st dims) rawCsM
  -- can't pattern match because the typelist of args would escape its scope
  in TE.withDWA (\dRaw pRaw -> BuildP $ TransformedDiffTypeP nds [] rawDS (exprListToParameters pRaw) dRaw TE.TNil (const fromRawF)) rawPrior

{-
addTransformedParameter :: TE.NamedDeclSpec t
                        -> Maybe [TE.VarModifier TE.UExpr (TE.ScalarType t)]
                        -> TE.DensityWithArgs t
                        -> (TE.UExpr t -> TE.UExpr t)
                        -> SB.StanBuilderM md gq (TE.UExpr t)
addTransformedParameter nds@(TE.NamedDeclSpec _ ds) rawCsM rawPrior fromRawF =
  addParameter $ transformedHP nds rawCsM rawPrior fromRawF

addCenteredHierarchical :: TE.NamedDeclSpec t
                        -> Parameters args
                        -> TE.Density t args
                        -> SB.StanBuilderM md gq (TE.UExpr t)
addCenteredHierarchical nds p d = addParameter $ centeredHP nds p d

addNonCenteredHierarchical :: TE.NamedDeclSpec t
                           -> Parameters ts
                           -> TE.DensityWithArgs t
                           -> (TE.ExprList ts -> TE.UExpr t -> TE.UExpr t)
                           -> SB.StanBuilderM md gq (TE.UExpr t)
addNonCenteredHierarchical nds p rawPrior fromRawF = addParameter $ nonCenteredHP nds p rawPrior fromRawF
-}

-- This will make each component of the declared parameter independently normal
hierarchicalCenteredFixedMeanNormal :: TE.NamedDeclSpec t -> Double -> Parameter TE.EReal -> Parameter t
hierarchicalCenteredFixedMeanNormal nds mean sigmaP =
  centeredHP nds (GivenP (TE.realE mean) TE.:> sigmaP TE.:> TE.TNil) $ TE.normalDensity (TE.sTypeFromStanType $ TE.declType $ TE.decl nds)

stdNormalDensity :: TE.SType t -> TE.DensityWithArgs t
stdNormalDensity st = TE.DensityWithArgs (TE.normalDensity st) (TE.realE 0 TE.:> TE.realE 1 TE.:> TE.TNil)

-- THis seems more complex than it should be. The constraint helps.
hierarchicalNonCenteredFixedMeanNormal :: forall t. (TE.TypeOneOf t '[TE.EReal, TE.ECVec, TE.EMat, TE.ESqMat])
                                       => TE.NamedDeclSpec t -> Double -> Parameter TE.EReal -> Parameter t
hierarchicalNonCenteredFixedMeanNormal nds mean sigmaP =
  nonCenteredHP nds (GivenP (TE.realE mean) TE.:> sigmaP TE.:> TE.TNil) nd ncF
  where
    ds = TE.decl nds
    nd = TE.DensityWithArgs (TE.normalDensity $ TE.sTypeFromStanType $ TE.declType ds) (TE.realE 0 TE.:> TE.realE 1 TE.:> TE.TNil)
    ncF ::  TE.TypedList TE.UExpr '[TE.EReal, TE.EReal] -> TE.UExpr t -> TE.UExpr t
    ncF (m TE.:> sd TE.:> TE.TNil) x = case TE.sTypeFromStanType (TE.declType ds) of
      TE.SReal -> m `TE.plusE` (sd `TE.timesE` x)
      TE.SCVec -> case TE.declDims ds of
        (vecSizeE ::: VNil) -> TE.functionE TE.rep_vector (m TE.:> vecSizeE TE.:> TE.TNil) `TE.plusE` (sd `TE.timesE` x)
      TE.SMat -> case TE.declDims ds of
        (rowsE ::: colsE ::: VNil) -> TE.functionE TE.rep_matrix (m TE.:> rowsE TE.:> colsE TE.:> TE.TNil) `TE.plusE` (sd `TE.timesE` x)
      TE.SSqMat -> case TE.declDims ds of
        (nE ::: VNil) -> TE.functionE TE.rep_sq_matrix (m TE.:> nE TE.:> TE.TNil) `TE.plusE` (sd `TE.timesE` x)


centeredMultiNormalVectorCholeskyPrior :: TE.UExpr TE.ECVec -> TE.UExpr TE.ECVec -> TE.UExpr TE.ESqMat -> DensityWithArgs TE.ECVec
centeredMultiNormalVectorCholeskyPrior mu tau lCorr = TE.DensityWithArgs TE.multi_normal_cholesky (mu TE.:> tau TE.:> lCorr TE.:> TE.TNil )

{-
addSimpleParameter :: forall t md gq. (Typeable md, Typeable gq) => TE.NamedDeclSpec t -> TE.DensityWithArgs t -> SB.StanBuilderM md gq (TE.UExpr t)
addSimpleParameter nds (TE.DensityWithArgs d as) = addParameter $ UntransformedP nds (exprListToParameters as) d

intercept :: forall md gq. (Typeable md, Typeable gq) => TE.NamedDeclSpec TE.EReal -> TE.DensityWithArgs TE.EReal -> SB.StanBuilderM md gq (TE.UExpr TE.EReal)
intercept = addSimpleParameter
-}
--addHyperParameters :: Parameters es -> SB.StanBuilderM md gq (TE.TypedList TE.UExpr es)
--addHyperParameters = htraverse addParameter

-- we have some sum-to-zero options for vectors
withSumToZero :: STZ.SumToZero -> Parameter TE.ECVec -> SB.StanBuilderM (Parameter TE.ECVec)
withSumToZero stz p = do
  case stz of
    STZ.STZNone -> return p
    STZ.STZSoft dw -> do
      STZ.softSumToZero v dw
      return v
    STZ.STZSoftWeighted gi dw -> do
      nds <- getNamedDecl p
      let (TE.NamedDeclSpec n _) = nds
      v <- addParameter p
      STZ.weightedSoftSumToZero n v gi dw
      return v
    STZ.STZQR -> do -- yikes
      nds <- getNamedDecl p
      let (TE.NamedDeclSpec n (TE.DeclSpec _ (vecSizeE ::: VNil) cs)) = nds
          vecSizeMinusOneE = vecSizeE `TE.minusE` TE.intE 1
          nds' = TE.NamedDeclSpec (n <> "_stz") $ TE.vectorSpec vecSizeMinusOneE cs
      p' <- setNamedDecl nds' p
      v <- addParameter p'
      STZ.sumToZeroQR n v

{-
withSumToZero :: STZ.SumToZero -> Parameter TE.ECVec  -> SB.StanBuilderM md gq (TE.UExpr TE.ECVec)
withSumToZero stz p = do
  case stz of
    STZ.STZNone -> addParameter p
    STZ.STZSoft dw -> do
      v <- addParameter p
      STZ.softSumToZero v dw
      return v
    STZ.STZSoftWeighted gi dw -> do
      nds <- getNamedDecl p
      let (TE.NamedDeclSpec n _) = nds
      v <- addParameter p
      STZ.weightedSoftSumToZero n v gi dw
      return v
    STZ.STZQR -> do -- yikes
      nds <- getNamedDecl p
      let (TE.NamedDeclSpec n (TE.DeclSpec _ (vecSizeE ::: VNil) cs)) = nds
          vecSizeMinusOneE = vecSizeE `TE.minusE` TE.intE 1
          nds' = TE.NamedDeclSpec (n <> "_stz") $ TE.vectorSpec vecSizeMinusOneE cs
      p' <- setNamedDecl nds' p
      v <- addParameter p'
      STZ.sumToZeroQR n v
-}

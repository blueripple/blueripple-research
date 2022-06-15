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

import qualified Stan.ModelBuilder.TypedExpressions.Types as TE
import qualified Stan.ModelBuilder.TypedExpressions.TypedList as TE
import qualified Stan.ModelBuilder.TypedExpressions.Statements as TE
import qualified Stan.ModelBuilder.TypedExpressions.Functions as TE
import qualified Stan.ModelBuilder.TypedExpressions.StanFunctions as TE
import qualified Stan.ModelBuilder.TypedExpressions.Expressions as TE
import qualified Stan.ModelBuilder.TypedExpressions.Operations as TE
import Stan.ModelBuilder.TypedExpressions.Recursion (hfmap, htraverse)
import qualified Stan.ModelBuilder as TE
import Data.Vec.Lazy (Vec(..))


-- parameterized by the type of the hyper parameter
data Parameter :: TE.EType -> Type where
  HyperParameter :: SB.StanName -> TE.DeclSpec t ->  TE.DensityWithArgs t -> Parameter t
  ExtantParameter :: TE.UExpr t -> Parameter t

type Parameters ts = TE.TypedList Parameter ts

exprListToParameters :: TE.ExprList ts  -> Parameters ts
exprListToParameters = hfmap ExtantParameter

data Parameterization :: TE.EType -> Type where
  UnTransformedP :: Parameters ts -> TE.Density t ts -> Parameterization t
  TransformedSameP :: Parameters qs
                   -> TE.Density t qs
                   -> Parameters ts
                   -> (TE.ExprList ts -> TE.UExpr t -> TE.UExpr t)
                   -> Parameterization t
  TransformedDiffP :: TE.DeclSpec q
                   -> Parameters qs
                   -> TE.Density q qs
                   -> Parameters ts
                   -> (TE.ExprList ts -> TE.UExpr q -> TE.UExpr t)
                   -> Parameterization t

simpleP :: TE.DensityWithArgs t -> Parameterization t
simpleP (TE.DensityWithArgs d dArgs) = UnTransformedP (exprListToParameters dArgs) d

centeredHP :: Parameters ts -> TE.Density t ts -> Parameterization t
centeredHP = UnTransformedP

nonCenteredHP :: Parameters ts -> TE.DensityWithArgs t -> (TE.ExprList ts -> TE.UExpr t -> TE.UExpr t) -> Parameterization t
nonCenteredHP hps (TE.DensityWithArgs d dargs) ncF = TransformedSameP (exprListToParameters dargs) d hps ncF

hierarchicalCenteredFixedMeanNormal :: TE.SType t -> Double -> Parameter TE.EReal -> Parameterization t
hierarchicalCenteredFixedMeanNormal st mean sigmaP =
  centeredHP (ExtantParameter (TE.realE mean) TE.:> sigmaP TE.:> TE.TNil) $ TE.normalDensity st

-- THis seems more complex than it should be.
hierarchicalNonCenteredFixedMeanNormal :: forall t. TE.DeclSpec t -> Double -> Parameter TE.EReal -> Parameterization t
hierarchicalNonCenteredFixedMeanNormal ds mean sigmaP =
  nonCenteredHP (ExtantParameter (TE.realE mean) TE.:> sigmaP TE.:> TE.TNil) nd ncF
  where
    nd = TE.DensityWithArgs (TE.normalDensity $ TE.sTypeFromStanType $ TE.declType ds) (TE.realE 0 TE.:> TE.realE 1 TE.:> TE.TNil)
    ncF ::  TE.TypedList TE.UExpr '[TE.EReal, TE.EReal] -> TE.UExpr t -> TE.UExpr t
    ncF (m TE.:> sd TE.:> TE.TNil) x = case TE.sTypeFromStanType (TE.declType ds) of
      TE.SReal -> m `TE.plusE` (sd `TE.timesE` x)
      TE.SCVec -> case TE.declDims ds of
        (vecSizeE ::: VNil) -> TE.functionE TE.rep_vector (m TE.:> vecSizeE TE.:> TE.TNil) `TE.plusE` (sd `TE.timesE` x)
      TE.SMat -> case TE.declDims ds of
        (rowsE ::: colsE ::: VNil) -> TE.functionE TE.rep_matrix (m TE.:> rowsE TE.:> colsE TE.:> TE.TNil) `TE.plusE` (sd `TE.timesE` x)
      TE.SSqMat -> case TE.declDims ds of
        (nE ::: VNil) -> TE.functionE TE.rep_sq_matrix (m TE.:> nE TE.:> nE TE.:> TE.TNil) `TE.plusE` (sd `TE.timesE` x)
      _ -> undefined


groupModel :: TE.NamedDeclSpec t -> Parameterization t  -> SB.StanBuilderM md gq (TE.UExpr t)
groupModel (TE.NamedDeclSpec n ds) (UnTransformedP ps d) = do
  pEs <- addHyperParameters ps
  v <- SB.inBlock SB.SBParameters $ SB.stanDeclare n ds
  SB.inBlock SB.SBModel $ TE.addStmtToCode $ TE.sample v d pEs
  return v

groupModel (TE.NamedDeclSpec n ds) (TransformedSameP pd d pt tF) = do
  pEs <- addHyperParameters pd
  vRaw <- SB.inBlock SB.SBParameters $ SB.stanDeclare (rawName n) ds
  SB.inBlock SB.SBModel $ TE.addStmtToCode $ TE.sample vRaw d pEs
  tEs <- addHyperParameters pt
  SB.inBlock SB.SBTransformedParameters $ SB.stanDeclareRHS n ds $ tF tEs vRaw

groupModel (TE.NamedDeclSpec n ds) (TransformedDiffP dq pd d pt tF) = do
  pEs <- addHyperParameters pd
  vRaw <- SB.inBlock SB.SBParameters $ SB.stanDeclare (rawName n) dq
  SB.inBlock SB.SBModel $ TE.addStmtToCode $ TE.sample vRaw d pEs
  tEs <- addHyperParameters pt
  SB.inBlock SB.SBTransformedParameters $ SB.stanDeclareRHS n ds $ tF tEs vRaw

-- declare and add given priors for hyper parameters. Do nothing for extant.
addHyperParameters :: Parameters es -> SB.StanBuilderM md gq (TE.TypedList TE.UExpr es)
addHyperParameters hps = do
  let f :: Parameter t -> SB.StanBuilderM md gq (TE.UExpr t)
      f (HyperParameter n ds pDens) = do
        v <- SB.inBlock SB.SBParameters $ SB.stanDeclare n ds
        SB.inBlock SB.SBModel $ TE.addStmtToCode $ TE.sampleW v pDens
        return v
      f (ExtantParameter p) = pure p
  htraverse f hps

rawName :: Text -> Text
rawName t = t <> "_raw"

-- we have some sum-to-zero options for vectors
withSumToZero :: STZ.SumToZero -> TE.NamedDeclSpec TE.ECVec -> Parameterization TE.ECVec  -> SB.StanBuilderM md gq (TE.UExpr TE.ECVec)
withSumToZero stz nds p = do
  case stz of
    STZ.STZNone -> groupModel nds p
    STZ.STZSoft dw -> do
      v <- groupModel nds p
      STZ.softSumToZero v dw
      return v
    STZ.STZSoftWeighted gi dw -> do
      let (TE.NamedDeclSpec n _) = nds
      v <- groupModel nds p
      STZ.weightedSoftSumToZero n v gi dw
      return v
    STZ.STZQR -> do -- yikes
      let (TE.NamedDeclSpec n (TE.DeclSpec _ (vecSizeE ::: VNil) cs)) = nds
          vecSizeMinusOneE = vecSizeE `TE.minusE` TE.intE 1
          nds' = TE.NamedDeclSpec (n <> "_stz") $ TE.vectorSpec vecSizeMinusOneE cs
      v <- groupModel nds' p
      STZ.sumToZeroQR n v

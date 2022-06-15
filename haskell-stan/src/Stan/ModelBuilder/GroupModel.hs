{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}

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

import Stan.ModelBuilder.TypedExpressions.Types as TE
import Stan.ModelBuilder.TypedExpressions.Statements as TE
import Stan.ModelBuilder.TypedExpressions.Functions as TE
import Stan.ModelBuilder.TypedExpressions.Expressions as TE
--import Data.Hashable.Generic (HashArgs)

data HyperParameter :: TE.EType -> Type where
  HyperParameter :: SB.StanName (TE.DeclSpec t) (TE.DensityWithArgs t)

type HyperParameters es = TE.ArgList HyperParameter es

--type HyperParameters = Map SB.StanVar (Text, SB.StanVar -> SB.StanExpr) -- var, constraint for declaration, var -> prior
type BetaPrior t = TE.DensityWithArgs t

data HierarchicalParameterization :: EType -> Type where
  Centered :: BetaPrior t -> HierarchicalParameterization t -- beta prior
  NonCentered :: BetaPrior t -> (TE.UExpr t -> TE.UExpr t -> UStmt) ->  HierarchicalParameterization t -- raw prior and declaration of transformed

-- hyper-parameter types, beta type
data GroupModel :: [EType] -> EType -> Type  where
  BinarySymmetric :: TE.DensityWithArgs TE.EReal -> GroupModel TE.EReal '[]
  BinaryHierarchical :: HyperParameters ts -> HierarchicalParameterization t -> GroupModel t ts
  NonHierarchical :: STZ.SumToZero -> BetaPrior t -> GroupModel t '[] -- beta prior
  Hierarchical :: STZ.SumToZero -> HyperParameters es -> HierarchicalParameterization t -> GroupModel t ts

groupModel :: SB.StanName -> TE.DeclSpec t -> GroupModel t ts -> SB.StanBuilderM md gq (TE.UExpr t)
groupModel vn vds gm = do
  gmRes <- groupModel' [bv] gm
  case gmRes of
    [x] -> return x
    _ -> SB.stanBuildError "groupModel: Returned a number of variables /= 1.  This should never happen!"

groupModel' :: [SB.StanVar] -> GroupModel md gq -> SB.StanBuilderM md gq [SB.StanVar]
groupModel' bvs (BinarySymmetric priorE) = do
  let declareOne (SB.StanVar bn bt) = SB.inBlock SB.SBParameters $ SB.stanDeclare bn bt ""
  traverse_ declareOne bvs
  let bPriorM v = SB.addExprLine "Stan.GroupModel.groupModel'" $ SB.var v `SB.eq` priorE
  traverse (\bv -> groupBetaPrior bPriorM bv) bvs
  return bvs

groupModel' bvs (BinaryHierarchical hps (Centered bPriorM)) = do
  let declareOne (SB.StanVar bn bt) = SB.inBlock SB.SBParameters $ SB.stanDeclare bn bt ""
  addHyperParameters hps
  traverse_ declareOne bvs
  traverse (\bv -> groupBetaPrior bPriorM bv) bvs
  return bvs

groupModel' bvs (BinaryHierarchical hps (NonCentered rPriorM nonCenteredF)) = do
  let declareRaw (SB.StanVar bn bt) = SB.inBlock SB.SBParameters $ SB.stanDeclare (rawName bn) bt ""
  brvs <- traverse declareRaw bvs
--  let declareBeta bv@(SB.StanVar bn bt) = SB.inBlock SB.SBTransformedParameters $ SB.stanDeclareRHS bn bt "" (nonCenteredF $ SB.name $ rawName bn)
  let declareBeta bv@(SB.StanVar bn bt) = SB.inBlock SB.SBTransformedParameters $ nonCenteredF bv (SB.StanVar (rawName bn) bt)
  traverse_ declareBeta bvs
  addHyperParameters hps
  traverse (\brv -> groupBetaPrior rPriorM brv) brvs
  return bvs

groupModel' bvs (NonHierarchical stz priorE) = do
  let declareOne (SB.StanVar bn bt) = SB.inBlock SB.SBParameters $ SB.stanDeclare bn bt ""
  when (stz /= STZQR) $ traverse_ declareOne bvs --do { SB.inBlock SB.SBParameters $ SB.stanDeclare bn bt ""; return ()}
  traverse_ (\bv -> STZ.sumToZero bv stz) bvs
  let bPriorM v = SB.addExprLine "Stan.GroupModel.groupModel'" $ SB.var v `SB.eq` priorE
  traverse_ (\bv -> groupBetaPrior bPriorM bv) bvs
  return bvs

groupModel' bvs (Hierarchical stz hps (Centered bPriorM)) = do
  let declareOne (SB.StanVar bn bt) = SB.inBlock SB.SBParameters $ SB.stanDeclare bn bt ""
  addHyperParameters hps
  traverse_ declareOne bvs
  traverse_ (\bv -> STZ.sumToZero bv stz) bvs
  traverse_ (\bv -> groupBetaPrior bPriorM bv) bvs
  return bvs

groupModel' bvs (Hierarchical stz hps (NonCentered rPriorM nonCenteredF)) = do
  let declareRaw (SB.StanVar bn bt) = SB.inBlock SB.SBParameters $ SB.stanDeclare (rawName bn) bt ""
  brvs <- traverse declareRaw bvs
  traverse (\brv -> STZ.sumToZero brv stz) brvs -- ?
--  let declareBeta (SB.StanVar bn bt) = SB.inBlock SB.SBTransformedParameters $ SB.stanDeclareRHS bn bt "" (nonCenteredF $ SB.name $ rawName bn)
  let declareBeta bv@(SB.StanVar bn bt) = SB.inBlock SB.SBTransformedParameters $ nonCenteredF bv (SB.StanVar (rawName bn) bt)
  traverse_ declareBeta bvs
  addHyperParameters hps
  traverse_ (\brv -> groupBetaPrior rPriorM brv) brvs
  return bvs

rawName :: Text -> Text
rawName t = t <> "_raw"

-- some notes:
-- The array of vectors is vectorizing over those vectors, so you can think of it as an array of columns
-- The matrix version is looping over columns and setting each to the prior.
groupBetaPrior :: BetaPrior md gq -> SB.StanVar -> SB.StanBuilderM md gq ()
groupBetaPrior f v = SB.inBlock SB.SBModel $ f v

{-
groupBetaPrior :: SB.StanVar -> IndexedPrior -> SB.StanBuilderM md gq ()
groupBetaPrior bv@(SB.StanVar bn bt) (IndexedPrior priorE mSet) = do
  let loopsFromDims = SB.loopOverNamedDims
      vectorizing x = maybe (SB.vectorizedOne x) (SB.vectorized . Set.insert x) mSet
  SB.inBlock SB.SBModel $ case bt of
    SB.StanReal -> SB.addExprLine "groupBetaPrior" $ SB.var bv `SB.vectorSample` priorE
    SB.StanVector (SB.NamedDim ik) -> SB.addExprLine "groupBetaPrior" $ vectorizing ik $ (SB.var bv) `SB.vectorSample` priorE
    SB.StanVector _ -> SB.addExprLine "groupBetaPrior" $ SB.name bn `SB.vectorSample` priorE
    SB.StanArray dims SB.StanReal -> loopsFromDims dims $ SB.addExprLine "groupBetaPrior" $ SB.var bv `SB.vectorSample` priorE
    SB.StanArray dims (SB.StanVector (SB.NamedDim ik)) -> loopsFromDims dims $ SB.addExprLine "groupBetaPrior" $ SB.vectorizedOne ik (SB.var bv) `SB.vectorSample` priorE
    SB.StanMatrix (SB.NamedDim rowKey, colDim) -> loopsFromDims [colDim] $ SB.addExprLine "groupBetaPrior" $ SB.vectorizedOne rowKey (SB.var bv) `SB.vectorSample` priorE
    _ -> SB.stanBuildError $ "groupBetaPrior: " <> bn <> " has type " <> show bt <> "which is not real scalar, vector, or array of real."
-}
addHyperParameters :: HyperParameters -> SB.StanBuilderM md gq ()
addHyperParameters hps = do
   let f ((SB.StanVar sn st), (t, eF)) = do
         v <- SB.inBlock SB.SBParameters $ SB.stanDeclare sn st t
         SB.inBlock SB.SBModel $  SB.addExprLine "groupModel.addHyperParameters" $ eF v --SB.var v `SB.vectorSample` e
   traverse_ f $ Map.toList hps

hierarchicalCenteredFixedMeanNormal :: Double -> SB.StanName -> SB.StanExpr -> SumToZero -> GroupModel md gq
hierarchicalCenteredFixedMeanNormal mean sigmaName sigmaPrior stz = Hierarchical stz hpps (Centered bp) where
  hpps = one (SB.StanVar sigmaName SB.StanReal, ("<lower=0>",\v -> SB.var v `SB.vectorSample` sigmaPrior))
  bp v = SB.addExprLine "Stan.GroupModel.hierarchicalCenteredFixedNormal"
    $ SB.var v `SB.vectorSample` SB.normal (Just $ SB.scalar $ show mean) (SB.name sigmaName)

hierarchicalNonCenteredFixedMeanNormal :: Double -> SB.StanVar -> SB.StanExpr -> SumToZero -> GroupModel md gq
hierarchicalNonCenteredFixedMeanNormal mean sigmaVar sigmaPrior stz = Hierarchical stz hpps (NonCentered rp ncF) where
  hpps = one (sigmaVar, ("<lower=0>",\v -> SB.var v `SB.vectorSample` sigmaPrior))
  rp v = SB.addExprLine "Stan.GroupModel.hierarchicalNonCenteredFixedMeanNormal" $ SB.var v `SB.vectorSample` SB.stdNormal
  ncF (SB.StanVar sn st) brv = do
    SB.stanDeclareRHS sn st "" $ SB.var brv `SB.times` SB.var sigmaVar
    return ()

{-
populationBeta :: PopulationModelParameterization -> SB.StanVar -> SB.StanName -> SB.StanExpr -> SB.StanBuilderM env d SB.StanVar
populationBeta NonCentered beta@(SB.StanVar bn bt) sn sigmaPriorE = do
  SB.inBlock SB.SBParameters $ SB.stanDeclare sn SB.StanReal "<lower=0>"
  rbv <- SB.inBlock SB.SBTransformedParameters $ SB.stanDeclareRHS bn bt "" $ SB.name sn `SB.times` SB.name (rawName bn)
  SB.inBlock SB.SBModel $ do
     let sigmaPriorL =  SB.name sn `SB.vectorSample` sigmaPriorE
         betaRawPriorL = SB.name (rawName bn) `SB.vectorSample` SB.stdNormal
     SB.addExprLines "rescaledSumToZero (No sum)" [sigmaPriorL, betaRawPriorL]
  return rbv

populationBeta Centered beta@(SB.StanVar bn bt) sn sigmaPriorE = do
  SB.inBlock SB.SBParameters $ SB.stanDeclare sn SB.StanReal "<lower=0>"
  SB.inBlock SB.SBModel $ do
     let sigmaPriorL =  SB.name sn `SB.vectorSample` sigmaPriorE
         betaRawPriorL = SB.name bn `SB.vectorSample` SB.normal Nothing (SB.name sn)
     SB.addExprLines "rescaledSumToZero (No sum)" [sigmaPriorL, betaRawPriorL]
  return beta


rescaledSumToZero :: SumToZero -> PopulationModelParameterization -> SB.StanVar ->  SB.StanName -> SB.StanExpr -> SB.StanBuilderM env d ()
rescaledSumToZero STZNone pmp beta@(SB.StanVar bn bt) sigmaName sigmaPriorE = do
  (SB.StanVar bn bt) <- populationBeta pmp beta sigmaName sigmaPriorE
  SB.inBlock SB.SBParameters $ SB.stanDeclare bn bt ""
  return ()
{-
  SB.inBlock SB.SBTransformedParameters $ SB.stanDeclareRHS bn bt "" $ SB.name sn `SB.times` SB.name (rawName bn)
  SB.inBlock SB.SBModel $ do
     let betaRawPrior = SB.name (rawName bn) `SB.vectorSample` SB.stdNormal
     SB.addExprLines "rescaledSumToZero (No sum)" [betaRawPrior]
  return ()
-}
rescaledSumToZero (STZSoft prior) pmp beta@(SB.StanVar bn bt) sigmaName sigmaPriorE = do
  bv <- populationBeta pmp beta sigmaName sigmaPriorE
  softSumToZero bv {- (SB.StanVar (rawName bn) bt) -} prior
{-
  SB.inBlock SB.SBTransformedParameters $ SB.stanDeclareRHS bn bt "" $ SB.name sn `SB.times` SB.name (rawName bn)
  SB.inBlock SB.SBModel $ do
     let betaRawPrior = SB.name (rawName bn) `SB.vectorSample` SB.stdNormal
     SB.addExprLines "rescaledSumToZero (No sum)" [betaRawPrior]
  return ()
-}
rescaledSumToZero (STZSoftWeighted gV prior) pmp beta@(SB.StanVar bn bt) sigmaName sigmaPriorE = do
  bv <- populationBeta pmp beta sigmaName sigmaPriorE
  weightedSoftSumToZero bv {-(SB.StanVar (rawName bn) bt) -}  gV prior
{-
  SB.inBlock SB.SBTransformedParameters $ SB.stanDeclareRHS bn bt "" $ SB.name sn `SB.times` SB.name (rawName bn)
  SB.inBlock SB.SBModel $ do
     let betaRawPrior = SB.name (rawName bn) `SB.vectorSample` SB.stdNormal
     SB.addExprLines "rescaledSumToZero (No sum)" [betaRawPrior]
  return ()
-}
rescaledSumToZero STZQR pmp beta@(SB.StanVar bn bt) sigmaName sigmaPriorE = do
  bv <- populationBeta pmp beta sigmaName sigmaPriorE
  sumToZeroQR bv
--  SB.inBlock SB.SBTransformedParameters $ SB.stanDeclareRHS bn bt "" $ SB.name sn `SB.times` SB.name (rawName bn)
  return ()
-}

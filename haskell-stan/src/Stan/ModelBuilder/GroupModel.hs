{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}
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
import qualified Stan.ModelBuilder.Distributions as SD
import qualified Stan.ModelBuilder as SB
import qualified Stan.ModelBuilder.SumToZero as STZ
import Stan.ModelBuilder.SumToZero (SumToZero(..))

type HyperParameters = Map SB.StanVar (Text, SB.StanVar -> SB.StanExpr) -- var, constraint for declaration, var -> prior

data HierarchicalParameterization env d = Centered SB.StanExpr -- beta prior
                                        | NonCentered SB.StanExpr (SB.StanVar -> SB.StanVar -> SB.StanBuilderM env d ()) -- raw prior and declaration of transformed

data GroupModel env d = BinarySymmetric SB.StanExpr -- epsilon prior
                      | BinaryHierarchical HyperParameters (HierarchicalParameterization env d)
                      | NonHierarchical STZ.SumToZero SB.StanExpr -- beta prior
                      | Hierarchical STZ.SumToZero HyperParameters (HierarchicalParameterization env d)



groupModel :: SB.StanVar -> GroupModel env d -> SB.StanBuilderM env d SB.StanVar
groupModel bv gm = do
  gmRes <- groupModel' [bv] gm
  case gmRes of
    (x:[]) -> return x
    _ -> SB.stanBuildError "groupModel: Returned a number of variables /= 1.  This should never happen!"

groupModel' :: [SB.StanVar] -> GroupModel env d -> SB.StanBuilderM env d [SB.StanVar]
groupModel' bvs (BinarySymmetric priorE) = do
  let declareOne (SB.StanVar bn bt) = SB.inBlock SB.SBParameters $ SB.stanDeclare bn bt ""
  traverse_ declareOne bvs
  traverse (\bv -> groupBetaPrior bv priorE) bvs
  return bvs

groupModel' bvs (BinaryHierarchical hps (Centered betaPriorE)) = do
  let declareOne (SB.StanVar bn bt) = SB.inBlock SB.SBParameters $ SB.stanDeclare bn bt ""
  addHyperParameters hps
  traverse_ declareOne bvs
  traverse (\bv -> groupBetaPrior bv betaPriorE) bvs
  return bvs

groupModel' bvs (BinaryHierarchical hps (NonCentered rawPriorE nonCenteredF)) = do
  let declareRaw (SB.StanVar bn bt) = SB.inBlock SB.SBParameters $ SB.stanDeclare (rawName bn) bt ""
  brvs <- traverse declareRaw bvs
--  let declareBeta bv@(SB.StanVar bn bt) = SB.inBlock SB.SBTransformedParameters $ SB.stanDeclareRHS bn bt "" (nonCenteredF $ SB.name $ rawName bn)
  let declareBeta bv@(SB.StanVar bn bt) = SB.inBlock SB.SBTransformedParameters $ nonCenteredF bv (SB.StanVar (rawName bn) bt)
  traverse_ declareBeta bvs
  addHyperParameters hps
  traverse (\brv -> groupBetaPrior brv rawPriorE) brvs
  return bvs

groupModel' bvs (NonHierarchical stz priorE) = do
  let declareOne (SB.StanVar bn bt) = SB.inBlock SB.SBParameters $ SB.stanDeclare bn bt ""
  when (stz /= STZQR) $ traverse_ declareOne bvs --do { SB.inBlock SB.SBParameters $ SB.stanDeclare bn bt ""; return ()}
  traverse_ (\bv -> STZ.sumToZero bv stz) bvs
  traverse_ (\bv -> groupBetaPrior bv priorE) bvs
  return bvs

groupModel' bvs (Hierarchical stz hps (Centered betaPrior)) = do
  let declareOne (SB.StanVar bn bt) = SB.inBlock SB.SBParameters $ SB.stanDeclare bn bt ""
  addHyperParameters hps
  traverse_ declareOne bvs
  traverse_ (\bv -> STZ.sumToZero bv stz) bvs
  traverse_ (\bv -> groupBetaPrior bv betaPrior) bvs
  return bvs

groupModel' bvs (Hierarchical stz hps (NonCentered rawPrior nonCenteredF)) = do
  let declareRaw (SB.StanVar bn bt) = SB.inBlock SB.SBParameters $ SB.stanDeclare (rawName bn) bt ""
  brvs <- traverse declareRaw bvs
  traverse (\brv -> STZ.sumToZero brv stz) brvs -- ?
--  let declareBeta (SB.StanVar bn bt) = SB.inBlock SB.SBTransformedParameters $ SB.stanDeclareRHS bn bt "" (nonCenteredF $ SB.name $ rawName bn)
  let declareBeta bv@(SB.StanVar bn bt) = SB.inBlock SB.SBTransformedParameters $ nonCenteredF bv (SB.StanVar (rawName bn) bt)
  traverse_ declareBeta bvs
  addHyperParameters hps
  traverse_ (\brv -> groupBetaPrior brv rawPrior) brvs
  return bvs

rawName :: Text -> Text
rawName t = t <> "_raw"

-- some notes:
-- The array of vectors is vectorizing over those vectors, so you can think of it as an array of columns
-- The matrix version is looping over columns and setting each to the prior.
groupBetaPrior :: SB.StanVar -> SB.StanExpr -> SB.StanBuilderM env d ()
groupBetaPrior bv@(SB.StanVar bn bt) priorE = do
  let loopsFromDims = SB.loopOverNamedDims
  SB.inBlock SB.SBModel $ case bt of
    SB.StanReal -> SB.addExprLine "groupBetaPrior" $ SB.var bv `SB.vectorSample` priorE
    SB.StanVector (SB.NamedDim ik) -> SB.addExprLine "groupBetaPrior" $ SB.vectorizedOne ik (SB.var bv) `SB.vectorSample` priorE
    SB.StanVector _ -> SB.addExprLine "groupBetaPrior" $ SB.name bn `SB.vectorSample` priorE
    SB.StanArray dims SB.StanReal -> loopsFromDims dims $ SB.addExprLine "groupBetaPrior" $ SB.var bv `SB.vectorSample` priorE
    SB.StanArray dims (SB.StanVector (SB.NamedDim ik)) -> loopsFromDims dims $ SB.addExprLine "groupBetaPrior" $ SB.vectorizedOne ik (SB.var bv) `SB.vectorSample` priorE
    SB.StanMatrix (SB.NamedDim rowKey, colDim) -> loopsFromDims [colDim] $ SB.addExprLine "groupBetaPrior" $ SB.vectorizedOne rowKey (SB.var bv) `SB.vectorSample` priorE
    _ -> SB.stanBuildError $ "groupBetaPrior: " <> bn <> " has type " <> show bt <> "which is not real scalar, vector, or array of real."

addHyperParameters :: HyperParameters -> SB.StanBuilderM env d ()
addHyperParameters hps = do
   let f ((SB.StanVar sn st), (t, eF)) = do
         v <- SB.inBlock SB.SBParameters $ SB.stanDeclare sn st t
         SB.inBlock SB.SBModel $  SB.addExprLine "groupModel.addHyperParameters" $ eF v --SB.var v `SB.vectorSample` e
   traverse_ f $ Map.toList hps

hierarchicalCenteredFixedMeanNormal :: Double -> SB.StanName -> SB.StanExpr -> SumToZero -> GroupModel env d
hierarchicalCenteredFixedMeanNormal mean sigmaName sigmaPrior stz = Hierarchical stz hpps (Centered bp) where
  hpps = one (SB.StanVar sigmaName SB.StanReal, ("<lower=0>",\v -> SB.var v `SB.vectorSample` sigmaPrior))
  bp = SB.normal (Just $ SB.scalar $ show mean) (SB.name sigmaName)

hierarchicalNonCenteredFixedMeanNormal :: Double -> SB.StanVar -> SB.StanExpr -> SumToZero -> GroupModel env d
hierarchicalNonCenteredFixedMeanNormal mean sigmaVar sigmaPrior stz = Hierarchical stz hpps (NonCentered rp ncF) where
  hpps = one (sigmaVar, ("<lower=0>",\v -> SB.var v `SB.vectorSample` sigmaPrior))
  rp = SB.stdNormal
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

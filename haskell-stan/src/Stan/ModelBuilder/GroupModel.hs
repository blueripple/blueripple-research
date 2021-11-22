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

data HierarchicalParameterization = Centered SB.StanExpr
                                  | NonCentered SB.StanExpr (SB.StanExpr -> SB.StanExpr)

data GroupModel = NonHierarchical STZ.SumToZero SB.StanExpr
                | Hierarchical STZ.SumToZero HyperParameters HierarchicalParameterization



groupModel :: SB.StanVar -> GroupModel -> SB.StanBuilderM env d SB.StanVar
groupModel bv gm = do
  gmRes <- groupModel' [bv] gm
  case gmRes of
    (x:[]) -> return x
    _ -> SB.stanBuildError "groupModel: Returned a number of variables /= 1.  This should never happen!"

groupModel' :: [SB.StanVar] -> GroupModel -> SB.StanBuilderM env d [SB.StanVar]
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
  let declareBeta (SB.StanVar bn bt) = SB.inBlock SB.SBTransformedParameters $ SB.stanDeclareRHS bn bt "" (nonCenteredF $ SB.name $ rawName bn)
  traverse_ declareBeta bvs
  addHyperParameters hps
  traverse (\brv -> groupBetaPrior brv rawPrior) brvs
  return bvs

rawName :: Text -> Text
rawName t = t <> "_raw"


{-
groupModel :: SB.StanVar -> GroupModel -> SB.StanBuilderM env d SB.StanVar
groupModel bv@(SB.StanVar bn bt) (NonHierarchical stz priorE) = do
  when (stz /= STZQR) $ do { SB.inBlock SB.SBParameters $ SB.stanDeclare bn bt ""; return ()}
  sumToZero bv stz
  groupBetaPrior bv priorE
  return bv

groupModel  bv@(SB.StanVar bn bt) (Hierarchical stz hps (Centered betaPrior)) = do
  bv <- SB.inBlock SB.SBParameters $ SB.stanDeclare bn bt ""
  sumToZero bv stz
  addHyperParameters hps
  groupBetaPrior bv betaPrior
  return bv

groupModel (SB.StanVar bn bt) (Hierarchical stz hps (NonCentered rawPrior nonCenteredF)) = do
  brv@(SB.StanVar brn _) <- SB.inBlock SB.SBParameters $ SB.stanDeclare (rawName bn) bt ""
  sumToZero brv stz -- ?
  bv <- SB.inBlock SB.SBTransformedParameters $ SB.stanDeclareRHS bn bt "" (nonCenteredF $ SB.name brn)
  addHyperParameters hps
  groupBetaPrior brv rawPrior
  return bv
-}


groupBetaPrior :: SB.StanVar -> SB.StanExpr -> SB.StanBuilderM env d ()
groupBetaPrior bv@(SB.StanVar bn bt) priorE = do
  let loopFromDim :: SB.StanDim -> SB.StanBuilderM env d a -> SB.StanBuilderM env d a
      loopFromDim (SB.NamedDim ik) = SB.stanForLoopB ("n_" <> ik) Nothing ik
      loopFromDim _ = const $ SB.stanBuildError $ "groupBetaPrior:  given beta must have all named dimensions"
      loopsFromDims :: [SB.StanDim] -> SB.StanBuilderM env d a -> SB.StanBuilderM env d a
      loopsFromDims dims = foldr (\f g -> g . f) id $ fmap loopFromDim dims
  SB.inBlock SB.SBModel $ case bt of
    SB.StanReal -> SB.addExprLine "groupBetaPrior" $ SB.var bv `SB.vectorSample` priorE
    SB.StanVector _ -> SB.addExprLine "groupBetaPrior" $ SB.name bn `SB.vectorSample` priorE
    SB.StanArray dims SB.StanReal -> loopsFromDims dims $ SB.addExprLine "groupBetaPrior" $ SB.var bv `SB.vectorSample` priorE
    _ -> SB.stanBuildError $ "groupBetaPrior: " <> bn <> " has type " <> show bt <> "which is not real scalar, vector, or array of real."

addHyperParameters :: HyperParameters -> SB.StanBuilderM env d ()
addHyperParameters hps = do
   let f ((SB.StanVar sn st), (t, eF)) = do
         v <- SB.inBlock SB.SBParameters $ SB.stanDeclare sn st t
         SB.inBlock SB.SBModel $  SB.addExprLine "groupModel.addHyperParameters" $ eF v --SB.var v `SB.vectorSample` e
   traverse_ f $ Map.toList hps

hierarchicalCenteredFixedMeanNormal :: Double -> SB.StanName -> SB.StanExpr -> SumToZero -> GroupModel
hierarchicalCenteredFixedMeanNormal mean sigmaName sigmaPrior stz = Hierarchical stz hpps (Centered bp) where
  hpps = one (SB.StanVar sigmaName SB.StanReal, ("<lower=0>",\v -> SB.var v `SB.vectorSample` sigmaPrior))
  bp = SB.normal (Just $ SB.scalar $ show mean) (SB.name sigmaName)

hierarchicalNonCenteredFixedMeanNormal :: Double -> SB.StanName -> SB.StanExpr -> SumToZero -> GroupModel
hierarchicalNonCenteredFixedMeanNormal mean sigmaName sigmaPrior stz = Hierarchical stz hpps (NonCentered rp ncF) where
  hpps = one (SB.StanVar sigmaName SB.StanReal, ("<lower=0>",\v -> SB.var v `SB.vectorSample` sigmaPrior))
  rp = SB.stdNormal
  ncF brE = brE `SB.times` SB.name sigmaName

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

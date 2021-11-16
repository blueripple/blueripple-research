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

module Stan.ModelBuilder.SumToZero where

import Prelude hiding (All)
import qualified Data.Map as Map
import qualified Stan.ModelBuilder.Distributions as SD
import qualified Stan.ModelBuilder as SB

sumToZeroFunctions :: SB.StanBuilderM env d ()
sumToZeroFunctions = SB.addFunctionsOnce "sumToZeroQR" $ do
  SB.declareStanFunction "vector Q_sum_to_zero_QR(int N)" $ do
    SB.addStanLine "vector [2*N] Q_r"
    SB.stanForLoop "i" Nothing "N" $ const $ do
      SB.addStanLine "Q_r[i] = -sqrt((N-i)/(N-i+1.0))"
      SB.addStanLine "Q_r[i+N] = inv_sqrt((N-i) * (N-i+1))"
    SB.addStanLine "return Q_r"

  SB.declareStanFunction "vector sum_to_zero_QR(vector x_raw, vector Q_r)" $ do
    SB.addStanLine "int N = num_elements(x_raw) + 1"
    SB.addStanLine "vector [N] x"
    SB.addStanLine "real x_aux = 0"
    SB.addStanLine "real x_sigma = inv_sqrt(1 - inv(N))"
    SB.stanForLoop "i" Nothing "N-1" $ const $ do
      SB.addStanLine "x[i] = x_aux + x_raw[i] * Q_r[i]"
      SB.addStanLine "x_aux = x_aux + x_raw[i] * Q_r[i+N]"
    SB.addStanLine "x[N] = x_aux"
    SB.addStanLine "return (x_sigma * x)"

rawName :: Text -> Text
rawName t = t <> "_raw"


sumToZeroQR :: SB.StanVar -> SB.StanBuilderM env d ()
sumToZeroQR (SB.StanVar varName st@(SB.StanVector sd)) = do
  sumToZeroFunctions
  let vDim = SB.dimToText sd
  SB.inBlock SB.SBTransformedData $ do
    let dim = SB.scalar "2" `SB.times` SB.stanDimToExpr sd
    SB.stanDeclareRHS ("Q_r_" <> varName) (SB.StanVector $ SB.ExprDim dim) ""
      $ SB.function "Q_sum_to_zero_QR" (one $ SB.declaration $ SB.stanDimToExpr sd)
--    $ SB.addStanLine $ "vector[2*" <> vDim <> "] Q_r_" <> varName <> " = Q_sum_to_zero_QR(" <> vDim <> ")"
  SB.inBlock SB.SBParameters $ do
    let dim = SB.stanDimToExpr sd `SB.minus` SB.scalar "1"
    SB.stanDeclare (varName <> "_stz") (SB.StanVector $ SB.ExprDim dim) ""
--    $ SB.addStanLine $ "vector[" <> vDim <> " - 1] " <> varName <> "_stz"
  SB.inBlock SB.SBTransformedParameters
    $ SB.stanDeclareRHS varName st "" $ SB.function "sum_to_zero_QR" (SB.name (varName <> "_stz") :| [SB.name ("Q_r_" <> varName)])
--  SB.inBlock SB.SBModel
--    $ SB.addExprLines "sumToZeroQR" $ [SB.name varName `SB.vectorSample` SD.stdNormal]
--    $ SB.addStanLine $ varName <> "_stz ~ normal(0, 1)"
  return ()
sumToZeroQR (SB.StanVar varName _) = SB.stanBuildError $ "Non vector type given to sumToZeroQR (varName=" <> varName <> ")"

softSumToZero :: SB.StanVar -> SB.StanExpr -> SB.StanBuilderM env d ()
softSumToZero sv@(SB.StanVar varName st@(SB.StanVector (SB.NamedDim k))) sumToZeroPrior = do
  SB.inBlock SB.SBParameters $ SB.stanDeclare varName st ""
  SB.inBlock SB.SBModel $ do
    let expr = SB.vectorized (one k) (SB.function "sum" (one $ SB.var sv)) `SB.vectorSample` sumToZeroPrior
    SB.addExprLines "softSumToZero" [expr]
softSumToZero (SB.StanVar varName _) _ = SB.stanBuildError $ "Non vector type given to softSumToZero (varName=" <> varName <> ")"

weightedSoftSumToZero :: SB.StanVar -> SB.StanName -> SB.StanExpr -> SB.StanBuilderM env d ()
weightedSoftSumToZero sv@(SB.StanVar varName st@(SB.StanVector (SB.NamedDim k))) gn sumToZeroPrior = do
--  let dSize = SB.dimToText sd
  SB.inBlock SB.SBParameters $ SB.stanDeclare varName st ""
  weightsV <- SB.inBlock SB.SBTransformedData $ do
    weightsV' <- SB.stanDeclareRHS (varName <> "_weights") (SB.StanVector (SB.NamedDim k)) "<lower=0>"
      $ SB.function "rep_vector" (SB.scalar "0" :| [SB.stanDimToExpr $ SB.NamedDim k])
    SB.stanForLoopB "n" Nothing k
      $ SB.addExprLine "weightedSoftSumToZero"
      $ SB.var sv `SB.plusEq` (SB.scalar "1") --[" <> gn <> "[n]] += 1"
--      $ SB.binOp "+=" (SB.indexBy (SB.name $ varName <> "_weights") gn) (SB.scalar "1") --[" <> gn <> "[n]] += 1"
    SB.addStanLine $ varName <> "_weights /= N"
    return weightsV'
  SB.inBlock SB.SBModel $ do
--    let expr = SB.function "dot_product" (SB.name varName :| [SB.name $ varName <> "_weights"]) `SB.vectorSample` sumToZeroPrior
    let expr = SB.vectorized (one k) (SB.function "dot_product" (SB.var sv :| [SB.var weightsV])) `SB.vectorSample` sumToZeroPrior
    SB.addExprLines "softSumToZero" [expr]
--    SB.addStanLine $ "dot_product(" <> varName <> ", " <> varName <> "_weights) ~ normal(0, " <> show sumToZeroSD <> ")"
weightedSoftSumToZero (SB.StanVar varName _) _ _ = SB.stanBuildError $ "Non-vector (\"" <> varName <> "\") given to weightedSoftSumToZero"


data SumToZero = STZNone | STZSoft SB.StanExpr | STZSoftWeighted SB.StanName SB.StanExpr | STZQR deriving (Show, Eq)

sumToZero :: SB.StanVar -> SumToZero -> SB.StanBuilderM env d ()
sumToZero _ STZNone = return ()
sumToZero v (STZSoft p) = softSumToZero v p
sumToZero v (STZSoftWeighted gn p) = weightedSoftSumToZero v gn p
sumToZero v STZQR = sumToZeroQR v


type HyperParameters = Map SB.StanName (Text, SB.StanExpr) -- name, constraint for declaration, prior

data HierarchicalParameterization = Centered SB.StanExpr
                                  | NonCentered SB.StanExpr (SB.StanExpr -> SB.StanExpr)

data GroupModel = NonHierarchical SumToZero SB.StanExpr
                | Hierarchical SumToZero HyperParameters HierarchicalParameterization



groupModel :: SB.StanVar -> GroupModel -> SB.StanBuilderM env d SB.StanVar
groupModel bv gm = do
  gmRes <- groupModel' [bv] gm
  case gmRes of
    (x:[]) -> return x
    _ -> undefined -- this should really never happen

groupModel' :: [SB.StanVar] -> GroupModel -> SB.StanBuilderM env d [SB.StanVar]
groupModel' bvs (NonHierarchical stz priorE) = do
  let declareOne (SB.StanVar bn bt) = SB.inBlock SB.SBParameters $ SB.stanDeclare bn bt ""
  when (stz /= STZQR) $ traverse_ declareOne bvs --do { SB.inBlock SB.SBParameters $ SB.stanDeclare bn bt ""; return ()}
  traverse_ (\bv -> sumToZero bv stz) bvs
  traverse_ (\bv -> groupBetaPrior bv priorE) bvs
  return bvs

groupModel' bvs (Hierarchical stz hps (Centered betaPrior)) = do
  let declareOne (SB.StanVar bn bt) = SB.inBlock SB.SBParameters $ SB.stanDeclare bn bt ""
  addHyperParameters hps
  traverse_ declareOne bvs
  traverse_ (\bv -> sumToZero bv stz) bvs
  traverse_ (\bv -> groupBetaPrior bv betaPrior) bvs
  return bvs

groupModel' bvs (Hierarchical stz hps (NonCentered rawPrior nonCenteredF)) = do
  let declareRaw (SB.StanVar bn bt) = SB.inBlock SB.SBParameters $ SB.stanDeclare (rawName bn) bt ""
  brvs <- traverse declareRaw bvs
  traverse (\brv -> sumToZero brv stz) brvs -- ?
  let declareBeta (SB.StanVar bn bt) = SB.inBlock SB.SBTransformedParameters $ SB.stanDeclareRHS bn bt "" (nonCenteredF $ SB.name $ rawName bn)
  traverse_ declareBeta bvs
  addHyperParameters hps
  traverse (\brv -> groupBetaPrior brv rawPrior) brvs
  return bvs

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
   let f (n, (t, e)) = do
         v <- SB.inBlock SB.SBParameters $ SB.stanDeclare n SB.StanReal t
         SB.inBlock SB.SBModel $  SB.addExprLine "groupModel.addHyperParameters" $ SB.var v `SB.vectorSample` e
   traverse_ f $ Map.toList hps

hierarchicalCenteredFixedMeanNormal :: Double -> SB.StanName -> SB.StanExpr -> SumToZero -> GroupModel
hierarchicalCenteredFixedMeanNormal mean sigmaName sigmaPrior stz = Hierarchical stz hpps (Centered bp) where
  hpps = one (sigmaName, ("<lower=0>",sigmaPrior))
  bp = SB.normal (Just $ SB.scalar $ show mean) (SB.name sigmaName)

hierarchicalNonCenteredFixedMeanNormal :: Double -> SB.StanName -> SB.StanExpr -> SumToZero -> GroupModel
hierarchicalNonCenteredFixedMeanNormal mean sigmaName sigmaPrior stz = Hierarchical stz hpps (NonCentered rp ncF) where
  hpps = one (sigmaName, ("<lower=0>",sigmaPrior))
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

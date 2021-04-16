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
import qualified Stan.ModelBuilder.Distributions as SD
import qualified Stan.ModelBuilder as SB

sumToZeroFunctions :: SB.StanBuilderM env d r0 ()
sumToZeroFunctions = do
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

addSumToZeroFunctions ::SB.StanBuilderM env d r0 ()
addSumToZeroFunctions = SB.addFunctionsOnce "SumToZero" sumToZeroFunctions

rawName :: Text -> Text
rawName t = t <> "_raw"


sumToZeroQR :: SB.StanVar -> SB.StanBuilderM env d r0 ()
sumToZeroQR (SB.StanVar varName st@(SB.StanVector sd)) = do
  sumToZeroFunctions
  let vDim = SB.dimToText sd
  SB.inBlock SB.SBTransformedData $ do
    let dim = SB.scalar "2" `SB.times` SB.stanDimToExpr sd
    SB.stanDeclareRHS ("Q_r_" <> varName) (SB.StanVector $ SB.ExprDim dim) "" $ SB.function "Q_sum_to_zero_QR" (one $ SB.stanDimToExpr sd)
--    $ SB.addStanLine $ "vector[2*" <> vDim <> "] Q_r_" <> varName <> " = Q_sum_to_zero_QR(" <> vDim <> ")"
  SB.inBlock SB.SBParameters $ do
    let dim = SB.stanDimToExpr sd `SB.minus` SB.scalar "1"
    SB.stanDeclare (varName <> "_stz") (SB.StanVector $ SB.ExprDim dim) ""
--    $ SB.addStanLine $ "vector[" <> vDim <> " - 1] " <> varName <> "_stz"
  SB.inBlock SB.SBTransformedParameters
    $ SB.stanDeclareRHS varName st "" $ SB.function "sum_to_zero_QR" (SB.name (varName <> "_stz") :| [SB.name ("Q_r_" <> varName)])
  SB.inBlock SB.SBModel
    $ SB.addExprLines "sumToZeroQR" $ [SB.name varName `SB.vectorSample` SD.stdNormal]
--    $ SB.addStanLine $ varName <> "_stz ~ normal(0, 1)"
sumToZeroQR (SB.StanVar varName _) = SB.stanBuildError $ "Non vector type given to sumToZeroQR (varName=" <> varName <> ")"

softSumToZero :: SB.StanVar -> SB.StanExpr -> SB.StanBuilderM env d r0 ()
softSumToZero sv@(SB.StanVar varName st@(SB.StanVector sd)) sumToZeroPrior = do
  SB.inBlock SB.SBParameters $ SB.stanDeclare varName st ""
  SB.inBlock SB.SBModel $ do
    let expr = SB.function "sum" (one $ SB.name varName) `SB.vectorSample` sumToZeroPrior
    SB.addExprLines "softSumToZero" [expr]
softSumToZero (SB.StanVar varName _) _ = SB.stanBuildError $ "Non vector type given to softSumToZero (varName=" <> varName <> ")"

weightedSoftSumToZero :: SB.StanVar -> SB.StanName -> SB.StanExpr -> SB.StanBuilderM env d r0 ()
weightedSoftSumToZero (SB.StanVar varName st@(SB.StanVector sd)) gn sumToZeroPrior = do
  let dSize = SB.dimToText sd
  SB.inBlock SB.SBParameters $ SB.stanDeclare varName st ""
  SB.inBlock SB.SBTransformedData $ do
    SB.stanDeclareRHS (varName <> "_weights") (SB.StanVector sd) "<lower=0>"
      $ SB.function "rep_vector" (SB.scalar "0" :| [SB.stanDimToExpr sd])
    SB.stanForLoop "n" Nothing dSize $ const $ SB.addStanLine $ varName <> "_weights[" <> gn <> "[n]] += 1"
    SB.addStanLine $ varName <> "_weights /= N"
  SB.inBlock SB.SBModel $ do
    let expr = SB.function "dot_product" (SB.name varName :| [SB.name $ varName <> "_weights"]) `SB.vectorSample` sumToZeroPrior
    SB.addExprLines "softSumToZero" [expr]
--    SB.addStanLine $ "dot_product(" <> varName <> ", " <> varName <> "_weights) ~ normal(0, " <> show sumToZeroSD <> ")"
weightedSoftSumToZero (SB.StanVar varName _) _ _ = SB.stanBuildError $ "Non-vector (\"" <> varName <> "\") given to weightedSoftSumToZero"

data SumToZero = STZNone | STZSoft SB.StanExpr | STZSoftWeighted SB.StanName SB.StanExpr | STZQR

rescaledSumToZero :: SumToZero -> SB.StanVar ->  SB.StanVar -> SB.StanBuilderM env d r0 ()
rescaledSumToZero STZNone beta@(SB.StanVar bn bt) sigma@(SB.StanVar sn st) = do
  SB.inBlock SB.SBParameters $ SB.stanDeclare (rawName bn) bt ""
  SB.inBlock SB.SBTransformedParameters $ SB.stanDeclareRHS bn bt "" $ SB.name sn `SB.times` SB.name (rawName bn)
  SB.inBlock SB.SBModel $ do
     let betaRawPrior = SB.name (rawName bn) `SB.vectorSample` SB.stdNormal
     SB.addExprLines "rescaledSumToZero (No sum)" [betaRawPrior]
  return ()
rescaledSumToZero (STZSoft prior) beta@(SB.StanVar bn bt) sigma@(SB.StanVar sn _) = do
  softSumToZero (SB.StanVar (rawName bn) bt)  prior
  SB.inBlock SB.SBTransformedParameters $ SB.stanDeclareRHS bn bt "" $ SB.name sn `SB.times` SB.name (rawName bn)
  SB.inBlock SB.SBModel $ do
     let betaRawPrior = SB.name (rawName bn) `SB.vectorSample` SB.stdNormal
     SB.addExprLines "rescaledSumToZero (No sum)" [betaRawPrior]
  return ()
rescaledSumToZero (STZSoftWeighted gV prior) beta@(SB.StanVar bn bt) sigma@(SB.StanVar sn _) = do
  weightedSoftSumToZero (SB.StanVar (rawName bn) bt)  gV prior
  SB.inBlock SB.SBTransformedParameters $ SB.stanDeclareRHS bn bt "" $ SB.name sn `SB.times` SB.name (rawName bn)
  SB.inBlock SB.SBModel $ do
     let betaRawPrior = SB.name (rawName bn) `SB.vectorSample` SB.stdNormal
     SB.addExprLines "rescaledSumToZero (No sum)" [betaRawPrior]
  return ()
rescaledSumToZero STZQR beta@(SB.StanVar bn bt) sigma@(SB.StanVar sn _) = do
  sumToZeroQR (SB.StanVar (rawName bn) bt)
  SB.inBlock SB.SBTransformedParameters $ SB.stanDeclareRHS bn bt "" $ SB.name sn `SB.times` SB.name (rawName bn)
  return ()

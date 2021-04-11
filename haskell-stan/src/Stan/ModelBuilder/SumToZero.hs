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

sumToZeroQR :: SB.StanVar -> SB.StanBuilderM env d r0 ()
sumToZeroQR (SB.StanVar varName st@(SB.StanVector sd)) = do
  addSumToZeroFunctions
  let vDim = SB.dimToText sd
  SB.inBlock SB.SBTransformedData
    $ SB.addStanLine $ "vector[2*" <> vDim <> "] Q_r_" <> varName <> " = Q_sum_to_zero_QR(" <> vDim <> ")"
  SB.inBlock SB.SBParameters
    $ SB.addStanLine $ "vector[" <> vDim <> " - 1] " <> varName <> "_raw"
  SB.inBlock SB.SBTransformedParameters
    $ SB.stanDeclareRHS varName st "" $ "sum_to_zero_QR(" <> varName <> "_raw, Q_r_" <> varName <> ")"
  SB.inBlock SB.SBModel
    $ SB.addStanLine $ varName <> "_raw ~ normal(0, 1)"

sumToZeroQR (SB.StanVar varName _) = SB.stanBuildError $ "Non vector type given to sumToZeroQR (varName=" <> varName <> ")"

softSumToZero :: SB.StanVar -> SB.StanExpr -> SB.StanBuilderM env d r0 ()
softSumToZero (SB.StanVar varName st@(SB.StanVector sd)) sumToZeroPrior = do
  SB.inBlock SB.SBModel $ do
    let expr = SB.function "sum" [SB.name varName] `SB.vectorSample` sumToZeroPrior
    SB.printExprM "softSumToZero" (SB.fullyIndexedBindings mempty) (return expr) >>= SB.addStanLine
--    $ SB.addStanLine $ "sum(" <> varName <> ") ~ normal(0, " <> show sumToZeroSD <> ")"
softSumToZero (SB.StanVar varName _) _ = SB.stanBuildError $ "Non vector type given to softSumToZero (varName=" <> varName <> ")"

weightedSoftSumToZero :: SB.StanVar -> Text -> Text -> SB.StanExpr -> SB.StanBuilderM env d r0 ()
weightedSoftSumToZero (SB.StanVar varName st@(SB.StanVector sd)) gn dSizeVar sumToZeroPrior = do
  let gSize = SB.dimToText sd
  SB.inBlock SB.SBTransformedData $ do
    SB.stanDeclareRHS (varName <> "_weights") (SB.StanVector sd) "<lower=0>" ("rep_vector(0," <> gSize <> ")")
--    SB.stanForLoop "g" Nothing gSizeVar $ const $ SB.addStanLine $ gn <> "_weights[g] = 0"
    SB.stanForLoop "n" Nothing dSizeVar $ const $ SB.addStanLine $ varName <> "_weights[" <> gn <> "[n]] += 1"
    SB.addStanLine $ varName <> "_weights /= N"

  SB.inBlock SB.SBModel $ do
    let expr = SB.function "dot_product" [SB.name varName, SB.name $ varName <> "_weights"] `SB.vectorSample` sumToZeroPrior
    SB.printExprM "softSumToZero" (SB.fullyIndexedBindings mempty) (return expr) >>= SB.addStanLine
--    SB.addStanLine $ "dot_product(" <> varName <> ", " <> varName <> "_weights) ~ normal(0, " <> show sumToZeroSD <> ")"

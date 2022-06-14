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
import qualified Stan.ModelBuilder.TypedExpressions.Types as TE
import qualified Stan.ModelBuilder.TypedExpressions.Program as TE
import qualified Stan.ModelBuilder.TypedExpressions.Statements as TE
import qualified Stan.ModelBuilder.TypedExpressions.Expressions as TE
import qualified Stan.ModelBuilder.TypedExpressions.StanFunctions as TE
import qualified Stan.ModelBuilder.TypedExpressions.Indexing as TE
import Data.Maybe (fromJust)

qSumToZeroQRF :: TE.Function TE.ECVec '[TE.EInt]
qSumToZeroQRF = TE.Function "Q_sum_to_zero_QR" TE.SCVec (TE.oneArgType TE.SInt)

qSumToZeroQRBody :: TE.ArgList TE.UExpr '[TE.EInt] -> (NonEmpty TE.UStmt, TE.UExpr TE.ECVec)
qSumToZeroQRBody (n TE.:> TE.ArgNil) = fromJust $ TE.writerNE $ do
  qr <- TE.declareW "Q_r" (TE.vectorSpec (TE.intE 2 `TE.timesE` n) [])
  let
    xp1 x = x `TE.plusE` TE.realE 1
    fBody i =
      let nmi = n `TE.minusE` i
      in TE.assign (TE.sliceE TE.s0 i qr) (TE.negateE $ TE.functionE TE.sqrt (TE.oneArg $ nmi `TE.divideE` (xp1 nmi)))
      :| [TE.assign (TE.sliceE TE.s0 (i `TE.plusE` n) qr) (TE.functionE TE.inv_sqrt (TE.oneArg $ nmi `TE.timesE` (xp1 nmi)))]
  TE.addStmt $ TE.for "i" (TE.SpecificNumbered (TE.intE 1) n) fBody
  return qr

sumToZeroQRF :: TE.Function TE.ECVec '[TE.ECVec, TE.ECVec]
sumToZeroQRF = TE.Function "sum_to_zero_QR" TE.SCVec (TE.SCVec TE.::> TE.SCVec TE.::> TE.ArgTypeNil)


sumToZeroQRBody :: TE.ArgList TE.UExpr '[TE.ECVec, TE.ECVec] -> (NonEmpty TE.UStmt, TE.UExpr TE.ECVec)
sumToZeroQRBody (x_raw TE.:> qr TE.:> TE.ArgNil) = fromJust $ TE.writerNE $ do
  n <- TE.declareRHSW "N" (TE.intSpec []) (TE.functionE TE.vector_size (TE.oneArg x_raw) `TE.plusE` TE.intE 1)
  x <- TE.declareW "x" (TE.vectorSpec n [])
  x_aux <- TE.declareRHSW "x_aux" (TE.realSpec []) (TE.realE 0)
  x_sigma <- TE.declareRHSW "x_sigma" (TE.realSpec []) (TE.functionE TE.inv_sqrt (TE.oneArg $ TE.intE 1 `TE.minusE` (TE.realE 1 `TE.divideE` n)))
  let fBody i =
        let ati = TE.sliceE TE.s0 i
            atiPlusN = TE.sliceE TE.s0 (i `TE.plusE` n)
        in ati x `TE.assign` (x_aux `TE.plusE` ati x_raw `TE.plusE` ati qr)
           :| [x_aux `TE.assign` (x_aux `TE.plusE` ati x_raw `TE.plusE` atiPlusN qr)]
  TE.addStmt $ TE.for "i" (TE.SpecificNumbered (TE.intE 1) (n `TE.minusE` TE.intE 1)) fBody
  TE.addStmt $ TE.sliceE TE.s0 n x `TE.assign` x_aux
  return $ x_sigma `TE.timesE` x

sumToZeroFunctions :: SB.StanBuilderM env d ()
sumToZeroFunctions = SB.addFunctionsOnce "sumToZeroQR" $ do
  SB.addStmtsToCode
    $ [TE.function qSumToZeroQRF (TE.Arg "N" TE.:> TE.ArgNil) qSumToZeroQRBody
      , TE.function sumToZeroQRF (TE.Arg "x_raw" TE.:> TE.Arg "Q_r" TE.:> TE.ArgNil) sumToZeroQRBody
      ]
{-
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
-}

sumToZeroQR :: SB.StanName -> TE.UExpr TE.EInt -> SB.StanBuilderM env d (TE.UExpr TE.ECVec)
sumToZeroQR varName vecSizeE = do
  sumToZeroFunctions
  qr_v <- SB.inBlock SB.SBTransformedData
    $ SB.stanDeclareRHS ("Q_r_" <> varName) (TE.vectorSpec (TE.intE 2 `TE.timesE` vecSizeE) []) $ TE.functionE qSumToZeroQRF (TE.oneArg vecSizeE)
  v_stz <- SB.inBlock SB.SBParameters
    $ SB.stanDeclare (varName <> "_stz") (TE.vectorSpec (vecSizeE `TE.minusE` TE.intE 1) [])
  SB.inBlock SB.SBTransformedParameters
    $ SB.stanDeclareRHS varName (TE.vectorSpec vecSizeE []) $ TE.functionE sumToZeroQRF (v_stz TE.:> qr_v TE.:> TE.ArgNil)
{-
--  let vDim = SB.dimToText sd
  SB.inBlock SB.SBTransformedData $ do
    let dim = SB.scalar "2" `SB.times` SB.stanDimToExpr sd
    TE.stanDeclareRHS ("Q_r_" <> TE.varName v) (SB.StanVector $ SB.ExprDim dim) ""
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
-}
--sumToZeroQR (SB.StanVar varName _) = SB.stanBuildError $ "Non vector type given to sumToZeroQR (varName=" <> varName <> ")"

softSumToZero :: SB.StanName -> TE.UExpr TE.EInt -> TE.DensityWithArgs TE.EReal -> SB.StanBuilderM env d ()
softSumToZero varName vecSize dw = do
  v <- SB.inBlock SB.SBParameters $ SB.stanDeclare varName (TE.vectorSpec vecSize [])
  SB.inBlock SB.SBModel $ SB.addStmtToCode
    $ TE.functionE TE.vector_sum (TE.oneArg v) `TE.sampleW` dw
  pure ()

{-
softSumToZero :: SB.StanVar -> SB.StanExpr -> SB.StanBuilderM env d ()
softSumToZero sv@(SB.StanVar varName st@(SB.StanVector (SB.NamedDim k))) sumToZeroPrior = do
  SB.inBlock SB.SBParameters $ SB.stanDeclare varName st ""
  SB.inBlock SB.SBModel $ do
    let expr = SB.vectorized (one k) (SB.function "sum" (one $ SB.var sv)) `SB.vectorSample` sumToZeroPrior
    SB.addExprLines "softSumToZero" [expr]
softSumToZero (SB.StanVar varName _) _ = SB.stanBuildError $ "Non vector type given to softSumToZero (varName=" <> varName <> ")"
-}

weightedSoftSumToZero :: SB.StanName -> TE.IndexArrayU -> TE.DensityWithArgs TE.EReal -> SB.StanBuilderM env d ()
weightedSoftSumToZero varName wgtIndex prior = do
  let vecSize = TE.indexSize wgtIndex
  let vecSpec = TE.vectorSpec vecSize []
  v <- SB.inBlock SB.SBParameters $ SB.stanDeclare varName vecSpec
  weights <- SB.inBlock SB.SBTransformedData $ do
    w <- SB.stanDeclareRHS (varName <> "_wgts") vecSpec $ TE.functionE TE.rep_vector (TE.realE 0 TE.:> vecSize TE.:> TE.ArgNil)
    let fb n = TE.sliceE TE.s0 n (TE.indexE TE.s0 wgtIndex w) `TE.plusEq` TE.intE 1 :| []
    SB.addStmtToCode $ TE.for "n" (TE.SpecificNumbered (TE.intE 1) vecSize) fb
    SB.addStmtToCode $ w `TE.divEq` vecSize
    pure w
  SB.inBlock SB.SBModel $ SB.addStmtToCode
    $ TE.functionE TE.dot (v TE.:> weights TE.:> TE.ArgNil) `TE.sampleW` prior
  pure ()
{-
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
-}

data SumToZero = STZNone | STZSoft (TE.DensityWithArgs TE.EReal) | STZSoftWeighted TE.IndexArrayU (TE.DensityWithArgs TE.EReal) | STZQR

sumToZero :: SB.StanName -> TE.UExpr TE.EInt -> SumToZero -> SB.StanBuilderM env d (Maybe (TE.UExpr TE.ECVec))
sumToZero _ _ STZNone = pure Nothing
sumToZero vecName vecSize (STZSoft p) = Nothing <$ softSumToZero vecName vecSize p
sumToZero vecName _ (STZSoftWeighted gi p) = Nothing <$ weightedSoftSumToZero vecName gi p
sumToZero vecName vecSize STZQR = Just <$> sumToZeroQR vecName vecSize

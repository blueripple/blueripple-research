{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}

module Stan.ModelBuilder.SumToZero where

import Prelude hiding (All)
import qualified Data.Map as Map
import qualified Stan.ModelBuilder.Distributions as SD
import qualified Stan.ModelBuilder as SB
import qualified Stan.ModelBuilder.TypedExpressions.Types as TE
import qualified Stan.ModelBuilder.TypedExpressions.TypedList as TE
import qualified Stan.ModelBuilder.TypedExpressions.Program as TE
import qualified Stan.ModelBuilder.TypedExpressions.Statements as TE
import qualified Stan.ModelBuilder.TypedExpressions.Expressions as TE
import qualified Stan.ModelBuilder.TypedExpressions.StanFunctions as TE
import qualified Stan.ModelBuilder.TypedExpressions.Indexing as TE

qSumToZeroQRF :: TE.Function TE.ECVec '[TE.EInt]
qSumToZeroQRF = TE.simpleFunction "Q_sum_to_zero_QR"

qSumToZeroQRBody :: TE.TypedList TE.UExpr '[TE.EInt] -> ([TE.UStmt], TE.UExpr TE.ECVec)
qSumToZeroQRBody (n TE.:> TE.TNil) = TE.writerL $ do
  qr <- TE.declareW "Q_r" (TE.vectorSpec (TE.intE 2 `TE.timesE` n) [])
  let
    xp1 x = x `TE.plusE` TE.realE 1
    fBody i =
      let nmi = n `TE.minusE` i
      in TE.assign (TE.sliceE TE.s0 i qr) (TE.negateE $ TE.functionE TE.sqrt (TE.oneTyped $ nmi `TE.divideE` (xp1 nmi)))
      :| [TE.assign (TE.sliceE TE.s0 (i `TE.plusE` n) qr) (TE.functionE TE.inv_sqrt (TE.oneTyped $ nmi `TE.timesE` (xp1 nmi)))]
  TE.addStmt $ TE.for "i" (TE.SpecificNumbered (TE.intE 1) n) fBody
  return qr

sumToZeroQRF :: TE.Function TE.ECVec '[TE.ECVec, TE.ECVec]
sumToZeroQRF = TE.simpleFunction "sum_to_zero_QR"


sumToZeroQRBody :: TE.TypedList TE.UExpr '[TE.ECVec, TE.ECVec] -> ([TE.UStmt], TE.UExpr TE.ECVec)
sumToZeroQRBody (x_raw TE.:> qr TE.:> TE.TNil) = TE.writerL $ do
  n <- TE.declareRHSW "N" (TE.intSpec []) (TE.functionE TE.size (TE.oneTyped x_raw) `TE.plusE` TE.intE 1)
  x <- TE.declareW "x" (TE.vectorSpec n [])
  x_aux <- TE.declareRHSW "x_aux" (TE.realSpec []) (TE.realE 0)
  x_sigma <- TE.declareRHSW "x_sigma" (TE.realSpec []) (TE.functionE TE.inv_sqrt (TE.oneTyped $ TE.intE 1 `TE.minusE` (TE.realE 1 `TE.divideE` n)))
  let fBody i =
        let ati = TE.sliceE TE.s0 i
            atiPlusN = TE.sliceE TE.s0 (i `TE.plusE` n)
        in ati x `TE.assign` (x_aux `TE.plusE` ati x_raw `TE.plusE` ati qr)
           :| [x_aux `TE.assign` (x_aux `TE.plusE` ati x_raw `TE.plusE` atiPlusN qr)]
  TE.addStmt $ TE.for "i" (TE.SpecificNumbered (TE.intE 1) (n `TE.minusE` TE.intE 1)) fBody
  TE.addStmt $ TE.sliceE TE.s0 n x `TE.assign` x_aux
  return $ x_sigma `TE.timesE` x

sumToZeroFunctions :: SB.StanBuilderM md gq ()
sumToZeroFunctions = SB.addFunctionsOnce "sumToZeroQR" $ do
  SB.addStmtsToCode
    $ [TE.function qSumToZeroQRF (TE.Arg "N" TE.:> TE.TNil) qSumToZeroQRBody
      , TE.function sumToZeroQRF (TE.Arg "x_raw" TE.:> TE.Arg "Q_r" TE.:> TE.TNil) sumToZeroQRBody
      ]

sumToZeroQR :: TE.StanName -> TE.UExpr TE.ECVec -> SB.StanBuilderM md gq (TE.UExpr TE.ECVec)
sumToZeroQR vName v_stz = do
  sumToZeroFunctions
  let vecSizeE = TE.functionE TE.size (TE.oneTyped v_stz) `TE.plusE` TE.intE 1
  qr_v <- SB.inBlock SB.SBTransformedData
    $ SB.stanDeclareRHS ("Q_r_" <> vName) (TE.vectorSpec (TE.intE 2 `TE.timesE` vecSizeE) []) $ TE.functionE qSumToZeroQRF (TE.oneTyped vecSizeE)
--  v_stz <- SB.inBlock SB.SBParameters
--    $ SB.stanDeclare (vName <> "_stz") (TE.vectorSpec (vecSizeE `TE.minusE` TE.intE 1) [])
  SB.inBlock SB.SBTransformedParameters
    $ SB.stanDeclareRHS vName (TE.vectorSpec vecSizeE []) $ TE.functionE sumToZeroQRF (v_stz TE.:> qr_v TE.:> TE.TNil)

softSumToZero :: TE.UExpr TE.ECVec -> TE.DensityWithArgs TE.EReal -> SB.StanBuilderM md gq ()
softSumToZero v dw = SB.inBlock SB.SBModel $ SB.addStmtToCode
  $ TE.functionE TE.sum (TE.oneTyped v) `TE.sampleW` dw

-- up to user to insure IndexArray and vector have same size
weightedSoftSumToZero :: TE.StanName -> TE.UExpr TE.ECVec -> TE.IndexArrayU -> TE.DensityWithArgs TE.EReal -> SB.StanBuilderM md gq ()
weightedSoftSumToZero vName v wgtIndex prior = do
  let vecSize = TE.indexSize wgtIndex
  let vecSpec = TE.vectorSpec vecSize []
--  v <- SB.inBlock SB.SBParameters $ SB.stanDeclare varName vecSpec
  weights <- SB.inBlock SB.SBTransformedData $ do
    w <- SB.stanDeclareRHS (vName <> "_wgts") vecSpec $ TE.functionE TE.rep_vector (TE.realE 0 TE.:> vecSize TE.:> TE.TNil)
    let fb n = TE.sliceE TE.s0 n (TE.indexE TE.s0 wgtIndex w) `TE.plusEq` TE.intE 1 :| []
    SB.addStmtToCode $ TE.for "n" (TE.SpecificNumbered (TE.intE 1) vecSize) fb
    SB.addStmtToCode $ w `TE.divEq` vecSize
    pure w
  SB.inBlock SB.SBModel $ SB.addStmtToCode
    $ TE.functionE TE.dot (v TE.:> weights TE.:> TE.TNil) `TE.sampleW` prior
  pure ()

data SumToZero = STZNone
               | STZSoft (TE.DensityWithArgs TE.EReal)
               | STZSoftWeighted TE.IndexArrayU (TE.DensityWithArgs TE.EReal)
               | STZQR

{-
sumToZero :: TE.UExpr TE.ECVec -> SumToZero -> SB.StanBuilderM md gq (Maybe (TE.UExpr TE.ECVec))
sumToZero _ STZNone = pure Nothing
sumToZero v (STZSoft p) = Nothing <$ softSumToZero v p
sumToZero v (STZSoftWeighted vName gi p) = Nothing <$ weightedSoftSumToZero vName v gi p
sumToZero v (STZQR vName) = Just <$> sumToZeroQR vName v
-}

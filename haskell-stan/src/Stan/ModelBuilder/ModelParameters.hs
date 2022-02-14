{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}

module Stan.ModelBuilder.ModelParameters where

import qualified Stan.ModelBuilder as SB
import qualified Stan.ModelBuilder.Expressions as SME
import qualified Stan.ModelBuilder.Distributions as SMD
import qualified Stan.ModelBuilder.GroupModel as SGM

intercept :: forall md gq. (Typeable md, Typeable gq) => Text -> SME.StanExpr -> SB.StanBuilderM md gq SB.StanVar
intercept iName alphaPriorE = do
  iVar <- SB.inBlock SB.SBParameters $ SB.stanDeclare iName SB.StanReal ""
  let interceptE = SB.var iVar `SME.vectorSample` alphaPriorE
  SB.inBlock SB.SBModel $ SB.addExprLine "intercept" interceptE
  return iVar

--data Prior = SimplePrior SB.StanExpr
--           | FunctionPrior (SME.StanVar -> SME.StanExpr)

data Parameterization = Centered
                      | NonCentered (SME.StanVar -> SME.StanExpr) --deriving (Eq, Show)

isReal :: Text -> SB.StanVar ->  SB.StanBuilderM md gq ()
isReal _ (SME.StanVar _ SME.StanReal) = return ()
isReal errMsg (SME.StanVar n _) = SB.stanBuildError $ errMsg <> "(" <> n <> " is not of type StanReal in isReal)"

isVec :: Text -> SB.StanVar -> SB.StanBuilderM md gq SME.IndexKey
isVec _ (SME.StanVar _ (SME.StanVector (SME.NamedDim ik))) = return ik
isVec errMsg (SME.StanVar n (SME.StanVector _)) =
  SB.stanBuildError $ errMsg <> ": " <> n <> " is of type StanVec but dimension is not named in isVec."
isVec errMsg (SME.StanVar n _) =
  SB.stanBuildError $ errMsg <> ": " <> n <> " is not of type StanVec in isVec."

isCholeskyCorr :: Text -> SB.StanVar -> SB.StanBuilderM md gq SME.IndexKey
isCholeskyCorr errMsg (SME.StanVar _ (SME.StanCholeskyFactorCorr (SME.NamedDim ik))) = return ik
isCholeskyCorr errMsg (SME.StanVar n (SME.StanCholeskyFactorCorr _)) =
  SB.stanBuildError $ errMsg <> ": " <> n <> " is of type StanCholeskyCorr but dimension is not named in isCholeskyCorr."
isCholeskyCorr errMsg (SME.StanVar n _) =
  SB.stanBuildError $ errMsg <> ": " <> n <> " is not of type StanCholeskyCorr in isCholeskyCorr."

scalarNonCenteredF :: SME.StanVar -> SME.StanVar -> SB.StanBuilderM md gq (SME.StanVar -> SME.StanExpr)
scalarNonCenteredF mu sigma = do
  isReal "scalarNonCenteredF: " mu
  isReal "scalarNonCenteredF: " sigma
  return $ \v -> SME.var mu `SME.plus` (SME.paren (SME.var sigma `SME.times` SME.var v))

vectorNonCenteredF :: SME.IndexKey -> SME.StanVar -> SME.StanVar -> SME.StanVar -> SB.StanBuilderM md gq (SME.StanVar -> SME.StanExpr)
vectorNonCenteredF ik mu tau lCorr = do
  muIndex <- isVec "vectorNonCenteredF" mu
  tauIndex <-isVec "vectorNonCenteredF" tau
  corrIndex <- isCholeskyCorr "vectorNonCenteredF" lCorr
  when (muIndex /= tauIndex || muIndex /= corrIndex)
    $ SB.stanBuildError
    $ "vectorNonCenteredF: index mismatch in given input parameters. "
    <> "mu index=" <> muIndex <> "; tau index=" <> tauIndex <> "; corrIndex=" <> corrIndex
  let repMu = SB.function "rep_matrix" (SB.var mu :| [SB.indexSize ik])
      dpmE = SB.function "diag_pre_multiply" (SB.var tau :| [SB.var lCorr])
      f v = SB.vectorizedOne ik $ repMu `SB.plus` (dpmE `SB.times` SB.var v)
  return f

addParameter :: Text
             -> SME.StanType
             -> Text
             -> SME.PossiblyVectorized SME.StanExpr
             -> SB.StanBuilderM md gq SME.StanVar
addParameter pName pType pConstraint pvp = do
  pv <- SB.inBlock SB.SBParameters $ SB.stanDeclare pName pType pConstraint
  SB.inBlock SB.SBModel
    $ SB.addExprLine "addParameter"
    $ case pvp of
        SME.UnVectorized e -> SB.var pv `SB.vectorSample` e
        SME.Vectorized iks e -> SB.vectorized iks $ SB.var pv `SB.vectorSample` e
  return pv

lkjCorrelationMatrixParameter :: Text -> SME.IndexKey -> Double -> SB.StanBuilderM md gq SB.StanVar
lkjCorrelationMatrixParameter name lkjDim lkjParameter = addParameter name lkjType "" lkjPrior
  where
    lkjType = SB.StanCholeskyFactorCorr $ SME.NamedDim lkjDim
    lkjPrior = SME.UnVectorized $ SB.function "lkj_corr_cholesky" (SB.scalar (show lkjParameter) :| [])

addTransformedParameter :: Text
                        -> SB.StanType
                        -> SME.PossiblyVectorized (SB.StanVar -> SB.StanExpr)
                        -> SME.PossiblyVectorized SME.StanExpr
                        -> SB.StanBuilderM md gq SB.StanVar
addTransformedParameter name sType fromRawF rawPrior = do
  rawV <- addParameter (name <> "_raw") sType "" rawPrior
  SB.inBlock SB.SBTransformedParameters
    $ SB.stanDeclareRHS name sType ""
    $ case fromRawF of
        SME.UnVectorized f -> f rawV
        SME.Vectorized iks f -> SME.vectorized iks $ f rawV

addHierarchicalScalar :: Text
                      -> SB.GroupTypeTag k
                      -> Parameterization
                      -> SME.StanExpr
                      -> SB.StanBuilderM md gq SB.StanVar
addHierarchicalScalar name gtt parameterization prior = do
  let gName = SB.taggedGroupName gtt
      pType = SME.StanVector (SME.NamedDim gName)
  case parameterization of
    Centered -> addParameter name pType "" (SME.Vectorized (one gName) prior)
    NonCentered f -> do
      let transform = SME.Vectorized (one gName) f
      addTransformedParameter name pType transform (SME.UnVectorized prior)

centeredHierarchicalVectorPrior :: SME.StanVar -> SME.StanVar -> SME.StanVar -> SB.StanBuilderM md gq SME.StanExpr
centeredHierarchicalVectorPrior mu tau lCorr = do
  muIndex <- isVec "vectorNonCenteredF" mu
  tauIndex <-isVec "vectorNonCenteredF" tau
  corrIndex <- isCholeskyCorr "vectorNonCenteredF" lCorr
  when (muIndex /= tauIndex || muIndex /= corrIndex)
    $ SB.stanBuildError
    $ "vectorNonCenteredF: index mismatch in given input parameters. "
    <> "mu index=" <> muIndex <> "; tau index=" <> tauIndex <> "; corrIndex=" <> corrIndex
  let dpmE = SB.function "diag_pre_multiply" (SB.var tau :| [SB.var lCorr])
  return $ SB.function "multi_normal_cholesky" (SB.var mu :| [dpmE])

addHierarchicalVector :: Text
                      -> SME.IndexKey
                      -> SB.GroupTypeTag k
                      -> Parameterization
                      -> SME.StanExpr
                      -> SB.StanBuilderM md gq SB.StanVar
addHierarchicalVector name rowIndex gtt parameterization priorE = do
  let gName = SB.taggedGroupName gtt
      pType = SB.StanMatrix (SME.NamedDim rowIndex, SME.NamedDim gName)
--      vecSet = Set.fromList [gName, rowIndex]
      vecG = SB.vectorizedOne gName
      vecR = SB.vectorizedOne rowIndex
  case parameterization of
    Centered -> do
      pv <- SB.inBlock SB.SBParameters $ SB.stanDeclare name pType ""
      SB.inBlock SB.SBModel
        $ SB.addExprLine "addHierarchicalVector"
        $ vecG $ vecR $ SB.function "to_vector" (one $ SB.var pv) `SME.eq` priorE
      return pv
    NonCentered f -> do
      rpv <- SB.inBlock SB.SBParameters $ SB.stanDeclare (name <> "_raw") pType ""
      SB.inBlock SB.SBModel
        $ SB.addExprLine "addHierarchicalVector"
        $ vecG $ vecR $ SB.function "to_vector" (one $ SB.var rpv) `SME.eq` priorE
      SB.inBlock SB.SBTransformedParameters
        $ SB.stanDeclareRHS name pType "" $ vecG $ vecR $ f rpv

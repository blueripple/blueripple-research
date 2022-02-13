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

data Prior = SimplePrior SB.StanExpr
           | FunctionPrior (SME.StanVar -> SME.StanExpr)

data Parameterization = Centered
                      | NonCentered (SME.StanVar -> SME.StanExpr) --deriving (Eq, Show)

scalarNonCenteredF :: SME.StanVar -> SME.StanVar -> (SME.StanVar -> SME.StanExpr)
scalarNonCenteredF mu sigma v = SME.var mu `SME.plus` (SME.paren (SME.var sigma `SME.times` SME.var v))

vectorNonCenteredF :: SME.IndexKey -> SME.StanVar -> SME.StanVar -> SME.StanVar -> (SME.StanVar -> SME.StanExpr)
vectorNonCenteredF ik mu tau lCorr v = e
  where
    repMu = SB.function "rep_matrix" (SB.var mu :| [SB.indexSize ik])
    dpmE = SB.function "diag_pre_multiply" (SB.var tau :| [SB.var lCorr])
    e = SB.vectorizedOne ik $ repMu `SB.plus` (dpmE `SB.times` SB.var v)

addParameter :: Text
             -> SME.StanType
             -> Text
             -> SME.PossiblyVectorized Prior
             -> SB.StanBuilderM md gq SB.StanVar
addParameter pName pType pConstraint pPriorF = do
  pv <- SB.inBlock SB.SBParameters $ SB.stanDeclare pName pType pConstraint
  SB.inBlock SB.SBModel
    $ SB.addExprLine "addParameter"
    $ case pPriorF of
        SME.UnVectorized (SimplePrior e) -> SB.var pv `SB.vectorSample` e
        SME.Vectorized ik (SimplePrior e) -> SB.vectorizedOne ik $ SB.var pv `SB.vectorSample` e
        SME.UnVectorized (FunctionPrior f) -> SB.var pv `SB.vectorSample` f pv
        SME.Vectorized ik (FunctionPrior f) -> SB.vectorizedOne ik $ SB.var pv `SB.vectorSample` f pv
  return pv

addTransformedParameter :: Text
                        -> SB.StanType
                        -> SME.PossiblyVectorized (SB.StanVar -> SB.StanExpr)
                        -> SME.PossiblyVectorized Prior
                        -> SB.StanBuilderM md gq SB.StanVar
addTransformedParameter name sType fromRawF rawPrior = do
  rawV <- addParameter (name <> "_raw") sType "" rawPrior
  SB.inBlock SB.SBTransformedParameters
    $ SB.stanDeclareRHS name sType ""
    $ case fromRawF of
        SME.UnVectorized f -> f rawV
        SME.Vectorized ik f -> SME.vectorizedOne ik $ f rawV

addHierarchicalScalar :: Text
                      -> SB.GroupTypeTag k
                      -> Parameterization
                      -> Prior
                      -> SB.StanBuilderM md gq SB.StanVar
addHierarchicalScalar name gtt parameterization rawPrior = do
  let gName = SB.taggedGroupName gtt
      pType = SME.StanVector (SME.NamedDim gName)
  v <- case parameterization of
    Centered -> addParameter name pType "" (SME.Vectorized gName prior)
    NonCentered f -> do
      let transform = SME.Vectorized gName f
      addTransformedParameter name sType transform prior

addHierarchicalVector :: Text
                      -> SME.IndexKey
                      -> SB.GroupTypeTag k
                      -> Parameterization
                      -> SME.PossiblyVectorized Prior
                      -> SB.StanBuilderM md gq SB.StanVar
addHierarchicalVector name rowIndex gtt parameterization pvp = do
  let gName = SB.taggedGroupName gtt
      pType = SB.StanMatrix (SME.NamedDim rowIndex, SME.NamedDim gName)
  v <- case parameterization of
    Centered ->

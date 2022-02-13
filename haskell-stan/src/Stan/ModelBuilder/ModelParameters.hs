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

addParameter :: Text -> SB.StanType -> Text -> (SB.StanVar -> SB.StanBuilderM md gq ()) -> SB.StanBuilderM md gq SB.StanVar
addParameter pName pType pConstraint pPriorF = do
  pv <- SB.inBlock SB.SBParameters $ SB.stanDeclare pName pType pConstraint
  SB.inBlock SB.SBModel $ pPriorF pv
  return pv

addSimpleParameter :: Text -> SB.StanType -> SB.StanExpr -> SB.StanBuilderM md gq SB.StanVar
addSimpleParameter pName pType priorE = addParameter pName pType "" priorF
  where
    priorF v = SB.addExprLine "addSimpleParameter" $ SB.var v `SB.vectorSample` priorE

addParameterWithVectorizedPrior :: Text -> SB.StanType -> SB.StanExpr -> SB.IndexKey -> SB.StanBuilderM md gq SB.StanVar
addParameterWithVectorizedPrior pName pType priorE index = addParameter pName pType "" priorF
  where
    priorF v = SB.addExprLine "addSimpleParameter" $ SB.vectorizedOne index $ SB.var v `SB.vectorSample` priorE

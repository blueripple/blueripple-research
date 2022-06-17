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
import qualified Stan.ModelBuilder.TypedExpressions.Types as TE
import qualified Stan.ModelBuilder.TypedExpressions.TypedList as TE
import qualified Stan.ModelBuilder.TypedExpressions.Statements as TE
import Frames.Streamly.CSV (formatWithShowCSV)
import qualified Stan.ModelBuilder.TypedExpressions.Functions as TE
import Data.Hashable.Generic (HashArgs)

addParameter :: forall t md gq. (Typeable md, Typeable gq) => TE.NamedDeclSpec t -> TE.DensityWithArgs t -> SB.StanBuilderM md gq (TE.UExpr t)
addParameter (TE.NamedDeclSpec n ds) alphaPriorD = SGM.addParameter $ SGM.HyperParameter n ds alphaPriorD

intercept :: forall md gq. (Typeable md, Typeable gq) => TE.NamedDeclSpec TE.EReal -> TE.DensityWithArgs TE.EReal -> SB.StanBuilderM md gq (TE.UExpr TE.EReal)
intercept = addParameter

--data Prior = SimplePrior SB.StanExpr
--           | FunctionPrior (SME.StanVar -> SME.StanExpr)
{-
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


lkjCorrelationMatrixParameter :: Text -> SME.IndexKey -> Double -> SB.StanBuilderM md gq SB.StanVar
lkjCorrelationMatrixParameter name lkjDim lkjParameter = addParameter name lkjType "" lkjPrior
  where
    lkjType = SB.StanCholeskyFactorCorr $ SME.NamedDim lkjDim
    lkjPrior = SME.Vectorized (one lkjDim) $ SB.function "lkj_corr_cholesky" (SB.scalar (show lkjParameter) :| [])
-}

addTransformedParameter :: TE.NamedDeclSpec
                        -> Maybe [TE.VarModifier TE.UExpr t]
                        -> TE.DensityWithArgs t
                        -> (TE.UExpr t -> TE.UExpr t)
                        -> SB.StanBuilderM md gq (TE.UExpr t)
addTransformedParameter nds rawCsM rawPrior fromRawF = groupModel nds p where
  TE.DensityWithArgs dRaw pRaw = rawPrior
  TE.NamedDeclSpec n ds@(TE.DeclSpec st cs) = nds
  rawDS = maybe ds (TE.DeclSpec st) rawCsM
  p = SGM.TransformedDiffP rawDS pRaw dRaw TE.TNil (const fromRawF)

addHierarchicalScalar :: TE.NamedDeclSpec
                      -> SGM.Parameters args
                      -> TE.Density t args

{-
  do
  let (TE.NamedDeclSpec n ds@(TE.DeclSpec st cs)) = nds
      rawDS = maybe ds (TE.DeclSpec st) rawCsM
  rawV <- addParameter (TE.NamedDeclSpec (name <> "_raw") rawDS) rawPrior
  SB.inBlock SB.SBTransformedParameters $ SB.stanDeclareRHS name ds $ fromRawF rawV
-}
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
      addTransformedParameter name pType "" transform (SME.Vectorized (one gName) prior)

centeredHierarchicalVectorPrior :: SME.StanVar -> SME.StanVar -> SME.StanVar -> SB.StanBuilderM md gq SME.StanExpr
centeredHierarchicalVectorPrior mu tau lCorr = do
  muIndex <- isVec "centeredHierarchicalVectorPrior " mu
  tauIndex <-isVec "centeredHierarchicalVectorPrior" tau
  corrIndex <- isCholeskyCorr "centeredHierarchicalVectorPrior " lCorr
  when (muIndex /= tauIndex || muIndex /= corrIndex)
    $ SB.stanBuildError
    $ "centeredHierarchicalVectorPrior : index mismatch in given input parameters. "
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
        $ vecG $ vecR $ SB.function "to_vector" (one $ SB.var pv) `SME.vectorSample` priorE
      return pv
    NonCentered f -> do
      rpv <- SB.inBlock SB.SBParameters $ SB.stanDeclare (name <> "_raw") pType ""
      SB.inBlock SB.SBModel
        $ SB.addExprLine "addHierarchicalVector"
        $ vecG $ vecR $ SB.function "to_vector" (one $ SB.var rpv) `SME.vectorSample` priorE
      SB.inBlock SB.SBTransformedParameters
        $ SB.stanDeclareRHS name pType "" $ vecG $ vecR $ f rpv

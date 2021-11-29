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

module Stan.ModelBuilder.FixedEffects where

import qualified Stan.ModelBuilder as SB
import qualified Stan.ModelBuilder.Expressions as SME
import qualified Stan.ModelBuilder.Distributions as SMD
import qualified Stan.ModelBuilder.GroupModel as SGM
import qualified Stan.ModelBuilder.BuildingBlocks as SBB

import Prelude hiding (All)
import qualified Data.Map as Map
import qualified Data.Set as Set
import qualified Data.Text as T
import qualified Data.Vector as V

data FixedEffects row = FixedEffects Int (row -> V.Vector Double)

data FixedEffectsModel k = NonInteractingFE Bool SB.StanExpr
                         | InteractingFE Bool (SB.GroupTypeTag k) (SGM.GroupModel)


addFixedEffects :: forall k r1 r2 d env.(Typeable d)
                => FixedEffectsModel k
                -> SB.RowTypeTag r1
                -> SB.RowTypeTag r2
                -> Maybe SB.StanVar
                -> FixedEffects r1
                -> Maybe Text
                -> SB.StanBuilderM env d ( SB.StanExpr -- Q * theta (or X * beta)
                                         , SB.StanVar -> SB.StanBuilderM env d SB.StanVar -- Y -> Y - mean(X)
                                         , SB.StanVar ->  SB.StanBuilderM env d SB.StanExpr -- Y -> Y * beta
                                         )
addFixedEffects feModel rttFE rttModeled mWgtsV fe@(FixedEffects n vecF) mVarSuffix = do
  (xV, f) <- addFixedEffectsData rttFE mWgtsV fe
  (feExpr, betaVar) <- addFixedEffectsParametersAndPriors feModel xV rttFE rttModeled mVarSuffix -- ??
  return (feExpr, f, betaVar)

addFixedEffectsData :: forall r d env. (Typeable d)
                    => SB.RowTypeTag r
                    -> Maybe SME.StanVar
                    -> FixedEffects r
                    -> SB.StanBuilderM env d (SB.StanVar -- X
                                             , SB.StanVar -> SB.StanBuilderM env d SB.StanVar -- Y -> Y - mean(X)
                                             )
addFixedEffectsData rttFE mWgtsV (FixedEffects n vecF) = do
  let feDataSetName = SB.dataSetName rttFE
  xV <- SB.add2dMatrixJson rttFE "X" "" (SB.NamedDim feDataSetName) n vecF -- JSON/code declarations for matrix
  fixedEffectsQR_Data xV rttFE mWgtsV

xName :: SB.RowTypeTag r -> Text
xName ds = "X_" <> SB.dataSetName ds

qName :: SB.RowTypeTag r -> Text
qName ds = "Q_" <> SB.dataSetName ds <> "_ast"

rName :: SB.RowTypeTag r -> Text
rName ds = "R_" <> SB.dataSetName ds <> "_ast"

rInvName :: SB.RowTypeTag r -> Text
rInvName ds = "R_" <> SB.dataSetName ds <> "_ast_inverse"


matrixDims :: SME.StanType -> SB.StanBuilderM env d (SME.StanDim, SME.StanDim)
matrixDims t = case t of
  SME.StanMatrix (rd, cd) -> return (rd, cd)
  _ -> SB.stanBuildError "Non matrix given to FixedEffects.matrixDims"

fixedEffectsQR_Data :: SME.StanVar
                    -> SB.RowTypeTag r
                    -> Maybe SME.StanVar
                    -> SB.StanBuilderM env d (SME.StanVar -- Q
                                             , SME.StanVar -> SB.StanBuilderM env d SME.StanVar -- Y - mean(X)
                                             )
fixedEffectsQR_Data m@(SB.StanVar mName mType@(SB.StanMatrix (rowDim, colDim))) rttFE wgtsM = do
  colKey <- case colDim of
    SB.NamedDim k -> return k
    _ -> SB.stanBuildError $ "fixedEffectsQR_Data: Column dimension of matrix must be a named dimension."
  qVar <- SB.inBlock SB.SBTransformedData $ do
    meanFunction <- case wgtsM of
      Nothing -> return $ "mean(" <> mName <> "[,k])"
      Just (SB.StanVar wgtsName _) -> do
        SBB.weightedMeanFunction
        return $ "weighted_mean(to_vector(" <> wgtsName <> "), " <> mName <> "[,k])"
    meanXV <- SB.stanDeclare ("mean_" <> mName) (SME.StanVector colDim) ""
    centeredXV <- SB.stanDeclare ("centered_" <> mName) mType "" --(SME.StanMatrix (SME.NamedDim rowKey, SME.NamedDim colKey)) ""
    SB.stanForLoopB "k" Nothing colKey $ do
      SB.addStanLine $ "mean_" <> mName <> "[k] = " <> meanFunction --"mean(" <> matrix <> "[,k])"
      SB.addStanLine $ "centered_" <>  mName <> "[,k] = " <> mName <> "[,k] - mean_" <> mName <> "[k]"
    let srE =  SB.function "sqrt" (one $ SB.indexSize' rowDim `SB.minus` SB.scalar "1")
        qRHS = SB.function "qr_thin_Q" (one $ SB.varNameE centeredXV) `SB.times` srE
    qVar' <- SB.stanDeclareRHS (qName rttFE) mType "" qRHS
    let rType = SME.StanMatrix (colDim, colDim)
        rRHS = SB.function "qr_thin_R" (one $ SB.varNameE centeredXV) `SB.divide` srE
    rVar <- SB.stanDeclareRHS (rName rttFE) rType "" rRHS
    let riRHS = SB.function "inverse" (one $ SB.varNameE rVar)
    riVar <- SB.stanDeclareRHS (rInvName rttFE) rType "" riRHS
    return qVar'
  let centeredX mv@(SME.StanVar sn st) =
        case st of
          cmt@(SME.StanMatrix (_, SME.NamedDim colKey)) -> SB.inBlock SB.SBTransformedData $ do
            cv <- SB.stanDeclare ("centered_" <> sn) cmt ""
            SB.stanForLoopB "k" Nothing colKey $ do
              SB.addStanLine $ "centered_" <>  sn <> "[,k] = " <> sn <> "[,k] - mean_" <> mName <> "[k]"
            return cv
          _ -> SB.stanBuildError
               $ "centeredX (from fixedEffectsQR_Data called with x="
               <> show mType
               <> "): called with mv="
               <> show mv
               <> " which is not a matrix type with indexed column dimension."
  return (qVar, centeredX)

fixedEffectsQR_Data _ _ _ = SB.stanBuildError "fixedEffectsQR_Data: called with non-matrix argument."


addFixedEffectsParametersAndPriors :: forall k r1 r2 d env. (Typeable d)
                                   => FixedEffectsModel k
                                   -> SME.StanVar
                                   -> SB.RowTypeTag r1
                                   -> SB.RowTypeTag r2
                                   -> Maybe Text
                                   -> SB.StanBuilderM env d (SB.StanExpr -- Q * theta (or X * beta)
                                                            , SB.StanVar -> SB.StanBuilderM env d SME.StanExpr -- Y ->  m (Y * beta)
                                                            )
addFixedEffectsParametersAndPriors feModel mVar rttFE rttModeled mVarSuffix
  = fixedEffectsQR_Parameters feModel rttFE mVar mVarSuffix

fixedEffectsQR_Parameters :: FixedEffectsModel k
                          -> SB.RowTypeTag r
                          -> SME.StanVar
                          -> Maybe Text
                          -> SB.StanBuilderM env d (SME.StanExpr -- theta
                                                   , SME.StanVar -> SB.StanBuilderM env d SME.StanExpr -- X -> X * beta
                                                   )
fixedEffectsQR_Parameters feModel rttFE q mVarSuffix = do
  case feModel of
    NonInteractingFE thinQR fePrior -> feParametersNoInteraction thinQR q rttFE mVarSuffix
    InteractingFE thinQR gtt gm  -> feParametersWithInteraction thinQR q gtt gm rttFE

feParametersNoInteraction :: Bool
                          -> SME.StanVar
                          -> SB.RowTypeTag r
                          -> Maybe Text
                          -> SB.StanBuilderM env d (SME.StanExpr -- theta
                                                   , SME.StanVar -> SB.StanBuilderM env d SME.StanExpr -- X -> X * beta
                                                   )
feParametersNoInteraction thinQR m@(SB.StanVar matrixName mType) rttFE mVarSuffix = do
  (_, colDim) <- matrixDims mType -- also checks if this is a matrix
  colIndexKey <- case colDim of
    SME.NamedDim k -> return k
    _ -> SB.stanBuildError "feParametersNoInteraction: Non indexed column type"
  let varName x = x <> fromMaybe "" mVarSuffix <> "_" <> matrixName
  (qThetaE, betaVar) <- case thinQR of
    False -> do
      betaVar' <- SB.inBlock SB.SBParameters $ SB.stanDeclare (varName "beta") (SME.StanVector colDim) ""
      let xBetaE = m `SME.matMult` betaVar'
      return (xBetaE, betaVar')
    True -> SB.useDataSetForBindings rttFE $ do
      thetaVar <- SB.inBlock SB.SBParameters $ SB.stanDeclare (varName "theta") (SME.StanVector colDim) ""
      SB.inBlock SB.SBTransformedParameters $ do
        betaVar' <- SB.stanDeclare (varName "beta") (SME.StanVector colDim) ""
        let ri = SME.StanVar (rName rttFE) (SME.StanMatrix (colDim, colDim))
        SB.addExprLine "feParametersNoInteraction"
          $ SME.vectorizedOne colIndexKey
          $ SME.var betaVar' `SME.eq` (ri `SME.matMult` thetaVar)
        let rowIndexKey = SB.crosswalkIndexKey rttFE --SB.dataSetCrosswalkName rttModeled rttFE
            m' = SME.StanVar matrixName (SME.StanMatrix (SME.NamedDim rowIndexKey, colDim))
        let qThetaE' = m' `SME.matMult` thetaVar
        return (qThetaE', betaVar')
--        SB.addStanLine $ varName "beta" <> " = " <> ri <> " * " <> varName "theta"
  let vectorizedBetaMult x = case x of
        SB.StanVar mName (SB.StanMatrix (SB.NamedDim rowKey, _)) -> return $ x `SB.matMult` betaVar
        _ -> SB.stanBuildError
             $ "vectorizeMult x (from fixedEffectsQR_Parameters, noInteraction, matrix name="
             <> show matrixName
             <> ") called with non-matrix or matrix with non-indexed row-dimension. x="
             <> show x
  return (qThetaE, vectorizedBetaMult)

feParametersWithInteraction :: Bool
                            -> SME.StanVar
                            -> SB.GroupTypeTag k
                            -> SGM.GroupModel
                            -> SB.RowTypeTag r
                            -> SB.StanBuilderM env d (SME.StanExpr -- Q * theta (or X * beta)
                                                     , SME.StanVar -> SB.StanBuilderM env d SME.StanExpr -- X -> X * beta
                                                     )
feParametersWithInteraction thinQR qVar gtt gm rtt = do
  (SB.IntIndex groupSize _) <- SB.rowToGroupIndex <$> SB.indexMap rtt gtt
  if groupSize == 2 then feBinaryInteraction thinQR qVar gtt gm rtt else feNonBinaryInteraction thinQR qVar gtt gm rtt

feBinaryInteraction :: Bool
                    -> SME.StanVar
                    -> SB.GroupTypeTag k
                    -> SGM.GroupModel
                    -> SB.RowTypeTag r
                    -> SB.StanBuilderM env d (SME.StanExpr -- Q * Theta (or X * Beta)
                                             , SME.StanVar -> SB.StanBuilderM env d SME.StanExpr -- X -> X * beta
                                             )
feBinaryInteraction thinQR qVar gtt gm rtt = case gm of
  SGM.BinarySymmetric epsPriorE -> undefined
  SGM.Binary muPriorE epsPriorE -> undefined
  _ -> SB.stanBuildError $ "FixedEffects.feBinaryInteraction called with non-binary group model"



feNonBinaryInteraction :: Bool
                       -> SME.StanVar
                       -> SB.GroupTypeTag k
                       -> SGM.GroupModel
                       -> SB.RowTypeTag r
                       -> SB.StanBuilderM env d (SME.StanExpr -- Q * Theta (or X * Beta)
                                                , SME.StanVar -> SB.StanBuilderM env d SME.StanExpr -- X -> X * beta
                                                )
feNonBinaryInteraction thinQR qVar gtt gm rtt = case gm of
  SGM.NonHierarchical stz betaPriorE -> undefined
  SGM.Hierarchical stz hps hp -> undefined
  _ -> SB.stanBuildError $ "FixedEffects.feNonBinaryInteraction called with binary group model"

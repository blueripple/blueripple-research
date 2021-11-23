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

-- returns
-- 'X * beta' (or 'Q * theta') model term expression
-- VarX -> 'VarX * beta' and just 'beta' for post-stratification
-- The latter is for use in post-stratification at fixed values of the fixed effects.
addFixedEffects :: forall r1 r2 d env.(Typeable d)
                => Bool
                -> SB.StanExpr
                -> SB.RowTypeTag r1
                -> SB.RowTypeTag r2
                -> Maybe SB.StanVar
                -> FixedEffects r1
                -> SB.StanBuilderM env d ( SB.StanExpr
                              , SB.StanVar -> SB.StanBuilderM env d SB.StanVar
                              , SB.StanExpr)
addFixedEffects thinQR fePrior rttFE rttModeled mWgtsV fe@(FixedEffects n vecF) = do
  (qVar, f) <- addFixedEffectsData rttFE mWgtsV fe
  (feExpr, betaVar) <- addFixedEffectsParametersAndPriors thinQR fePrior rttFE rttModeled ??
{-
  let feDataSetName = SB.dataSetName rttFE
      uSuffix = SB.underscoredIf feDataSetName
      rowIndexKey = SB.crosswalkIndexKey rttFE --SB.dataSetsCrosswalkName rttModeled rttFE
      colKey = "X_" <> feDataSetName <> "_Cols"
  xV <- SB.add2dMatrixJson rttFE "X" "" (SB.NamedDim feDataSetName) n vecF -- JSON/code declarations for matrix
  (qVar, f) <- fixedEffectsQR_Data uSuffix xV mWgtsV
-}
  (thetaVar, betaVar) <- fixedEffectsQR_Parameters qVar Nothing

--  (qVar, thetaVar, betaVar, f) <- fixedEffectsQR uSuffix xV mWgtsV
  -- model
  SB.inBlock SB.SBModel $ do
    let e = SB.vectorized (one colKey) (SB.var thetaVar) `SB.vectorSample` fePrior
    SB.addExprLine "addFixedEffects" e
  let eQ = SB.var qVar
      eQTheta = SB.matMult qVar thetaVar
      xName = if T.null feDataSetName then "centered_X" else "centered_X" <> uSuffix
      xVar = SB.StanVar xName (SB.varType qVar)
      eX = SB.var xVar
      eBeta = SB.var betaVar --SB.name $ "betaX" <> uSuffix
      eXBeta = SB.matMult xVar betaVar
      feExpr = if thinQR then eQTheta else eXBeta
  return (feExpr, f, eBeta)

addFixedEffectsData :: forall r d env. (Typeable d)
                    => SB.RowTypeTag r
                    -> Maybe SME.StanVar
                    -> FixedEffects r
                    -> SB.StanBuilderM env d (SB.StanVar, SB.StanVar -> SB.StanBuilderM env d SB.StanVar)
addFixedEffectsData feRTT mWgtsV (FixedEffects n vecF) = do
  let feDataSetName = SB.dataSetName feRTT
      uSuffix = SB.underscoredIf feDataSetName
  xV <- SB.add2dMatrixJson feRTT "X" "" (SB.NamedDim feDataSetName) n vecF -- JSON/code declarations for matrix
  fixedEffectsQR_Data uSuffix xV mWgtsV

addFixedEffectsParametersAndPriors :: forall r1 r2 d env. (Typeable d)
                                   => Bool
                                   -> SB.StanExpr
                                   -> SB.RowTypeTag r1
                                   -> SB.RowTypeTag r2
                                   -> Maybe Text
                                   -> SB.StanBuilderM env d (SB.StanExpr, SB.StanVar)
addFixedEffectsParametersAndPriors thinQR fePrior rttFE rttModeled mVarSuffix = do
  let feDataSetName = SB.dataSetName rttFE
      modeledDataSetName = fromMaybe "" mVarSuffix
      pSuffix = SB.underscoredIf feDataSetName
      uSuffix = pSuffix <> SB.underscoredIf modeledDataSetName
      rowIndexKey = SB.crosswalkIndexKey rttFE --SB.dataSetCrosswalkName rttModeled rttFE
      colIndexKey =  "X" <> pSuffix <> "_Cols"
      xVar = SB.StanVar ("X" <> pSuffix) $ SB.StanMatrix (SB.NamedDim rowIndexKey, SB.NamedDim colIndexKey)
  (thetaVar, betaVar) <- fixedEffectsQR_Parameters xVar Nothing
  SB.inBlock SB.SBModel $ do
    let e = SB.vectorized (one colIndexKey) (SB.var thetaVar) `SB.vectorSample` fePrior
    SB.addExprLine "addFixedEffectsParametersAndPriors" e
  let xType = SB.StanMatrix (SB.NamedDim rowIndexKey, SB.NamedDim colIndexKey) -- it's weird to have to create this here...
      qName = "Q" <> pSuffix <> "_ast"
      qVar = SB.StanVar qName xType
      eQTheta = SB.matMult qVar thetaVar
      xName = "centered_X" <> pSuffix
      xVar = SB.StanVar xName xType
      eXBeta = SB.matMult xVar betaVar
  let feExpr = if thinQR then eQTheta else eXBeta
  return (feExpr, betaVar)

{-
fixedEffectsQR :: Text
               -> SME.StanVar
               -> Maybe SME.StanVar
               -> SB.StanBuilderM env d (SME.StanVar, SME.StanVar, SME.StanVar, SME.StanVar -> SB.StanBuilderM env d SME.StanVar)
fixedEffectsQR thinSuffix xVar wgtsM = do
  (qVar, f) <- fixedEffectsQR_Data thinSuffix xVar wgtsM --rowKey colKey
  (thetaVar, betaVar) <- fixedEffectsQR_Parameters qVar Nothing --thinSuffix matrix colKey
  return (qVar, thetaVar, betaVar, f)
-}
fixedEffectsQR_Data :: Text
                    -> SME.StanVar
                    -> Maybe SME.StanVar
                    -> SB.StanBuilderM env d (SME.StanVar -- Q matrix variable
                                             , SME.StanVar -> SB.StanBuilderM env d SME.StanVar -- function to center different data around these means, returning centered
                                             )
fixedEffectsQR_Data thinSuffix (SB.StanVar matrixName (SB.StanMatrix (rowDim, colDim))) wgtsM = do
  let mt = SB.StanMatrix (rowDim, colDim)
      ri = "R" <> thinSuffix <> "_ast_inverse"
      q = "Q" <> thinSuffix <> "_ast"
      r = "R" <> thinSuffix <> "_ast"
--      qMatrixType = SME.StanMatrix (SME.NamedDim rowKey, SME.NamedDim colKey)
  colKey <- case colDim of
    SB.NamedDim k -> return k
    _ -> SB.stanBuildError $ "fixedEffectsQR_Data: Column dimension of matrix must be a named dimension."
  qVar <- SB.inBlock SB.SBTransformedData $ do
    meanFunction <- case wgtsM of
      Nothing -> return $ "mean(" <> matrixName <> "[,k])"
      Just (SB.StanVar wgtsName _) -> do
        SBB.weightedMeanFunction
        return $ "weighted_mean(to_vector(" <> wgtsName <> "), " <> matrixName <> "[,k])"
    meanXV <- SB.stanDeclare ("mean_" <> matrixName) (SME.StanVector colDim) ""
    centeredXV <- SB.stanDeclare ("centered_" <> matrixName) mt "" --(SME.StanMatrix (SME.NamedDim rowKey, SME.NamedDim colKey)) ""
    SB.stanForLoopB "k" Nothing colKey $ do
      SB.addStanLine $ "mean_" <> matrixName <> "[k] = " <> meanFunction --"mean(" <> matrix <> "[,k])"
      SB.addStanLine $ "centered_" <>  matrixName <> "[,k] = " <> matrixName <> "[,k] - mean_" <> matrixName <> "[k]"
    let srE =  SB.function "sqrt" (one $ SB.indexSize' rowDim `SB.minus` SB.scalar "1")
        qRHS = SB.function "qr_thin_Q" (one $ SB.varNameE centeredXV) `SB.times` srE
    qVar' <- SB.stanDeclareRHS q mt "" qRHS
    let rType = SME.StanMatrix (colDim, colDim)
        rRHS = SB.function "qr_thin_R" (one $ SB.varNameE centeredXV) `SB.divide` srE
    rVar <- SB.stanDeclareRHS r rType "" rRHS
    let riRHS = SB.function "inverse" (one $ SB.varNameE rVar)
    riVar <- SB.stanDeclareRHS ri rType "" riRHS
    return qVar'
  let centeredX mv@(SME.StanVar sn st) =
        case st of
          (SME.StanMatrix (SME.NamedDim rk, SME.NamedDim colKey)) -> SB.inBlock SB.SBTransformedData $ do
            cv <- SB.stanDeclare ("centered_" <> sn) (SME.StanMatrix (SME.NamedDim rk, SME.NamedDim colKey)) ""
            SB.stanForLoopB "k" Nothing colKey $ do
              SB.addStanLine $ "centered_" <>  sn <> "[,k] = " <> sn <> "[,k] - mean_" <> matrixName <> "[k]"
            return cv
          _ -> SB.stanBuildError
               $ "fixedEffectsQR_Data (returned StanVar -> StanExpr): "
               <> " Given matrix doesn't have same dimensions as modeled fixed effects."
  return (qVar, centeredX)

fixedEffectsQR_Data _ _ _ = SB.stanBuildError "fixedEffectsQR_Data: called with non-matrix argument."

fixedEffectsQR_Parameters :: SME.StanVar
                          -> Maybe (SB.GroupTypeTag k, SGM.GroupModel, SB.RowTypeTag r)
                          -> SB.StanBuilderM env d (SME.StanVar -- theta
                                                   , SME.StanVar -- beta
                                                   )
fixedEffectsQR_Parameters q@(SB.StanVar matrixName (SB.StanMatrix (_, colDim))) mGroupInteraction = do
  let thinSuffix = snd $ T.breakOn "_" matrixName
      ri = "R" <> thinSuffix <> "_ast_inverse"
      noInteraction = do
        thetaVar <- SB.inBlock SB.SBParameters $ SB.stanDeclare ("theta" <> matrixName) (SME.StanVector colDim) ""
        betaVar <- SB.inBlock SB.SBTransformedParameters $ do
          betaVar' <- SB.stanDeclare ("beta" <> matrixName) (SME.StanVector colDim) ""
          SB.addStanLine $ "beta" <> matrixName <> " = " <> ri <> " * theta" <> matrixName
          return betaVar'
        return (thetaVar, betaVar)
  noInteraction
{-
      withInteraction gtt gm rtt = do
        (SB.IntIndex groupSize _) <- SB.rowToGroupIndex <$> SB.indexMap rtt gtt
        let binary gtt rtt = do
-}

fixedEffectsQR_Parameters _ _ = SB.stanBuildError "fixedEffectsQR_Parameters: called with non-matrix variable."

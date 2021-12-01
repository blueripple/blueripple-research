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


thinQR :: FixedEffectsModel k -> Bool
thinQR (NonInteractingFE b _) = b
thinQR (InteractingFE b _ _) = b

data QRMatrixes = QRMatrixes { qM :: SME.StanVar, rM :: SME.StanVar, rInvM :: SME.StanVar}
data FEMatrixes = NoQR SME.StanVar | ThinQR SME.StanVar QRMatrixes

xM :: FEMatrixes -> SME.StanVar
xM (NoQR x) = x
xM (ThinQR x _) = x

qrM :: (QRMatrixes -> SME.StanVar) -> FEMatrixes -> Either Text SME.StanVar
qrM _ (NoQR _) = Left "qrM: No QR matrix in NoQR"
qrM f (ThinQR _ qr) = Right $ f qr

qrQ :: FEMatrixes -> Either Text SME.StanVar
qrQ = qrM qM

qrR :: FEMatrixes -> Either Text SME.StanVar
qrR = qrM rM

qrRInv :: FEMatrixes -> Either Text SME.StanVar
qrRInv = qrM rInvM


addFixedEffects :: forall k r1 r2 d env.(Typeable d)
                => FixedEffectsModel k
                -> SB.RowTypeTag r1
                -> SB.RowTypeTag r2
                -> Maybe SB.StanVar
                -> FixedEffects r1
                -> Maybe Text
                -> SB.StanBuilderM env d ( SB.StanExpr -- Q * theta (or X * beta)
                                         , SB.StanVar -> SB.StanBuilderM env d SB.StanVar -- Y -> Y - mean(X)
                                         , SB.IndexKey -> SB.StanVar ->  SB.StanBuilderM env d SB.StanExpr -- rtt -> Y -> Y * beta
                                         )
addFixedEffects feModel rttFE rttModeled mWgtsV fe@(FixedEffects n vecF) mVarSuffix = do
  (xV, f) <- addFixedEffectsData rttFE mWgtsV fe
  (feExpr, betaVar) <- addFixedEffectsParametersAndPriors feModel xV rttFE rttModeled mVarSuffix -- ??
  return (feExpr, f, betaVar)



addFixedEffectsData :: forall r d env. (Typeable d)
                    => SB.RowTypeTag r
                    -> Maybe SME.StanVar
                    -> FixedEffects r
                    -> SB.StanBuilderM env d (FEMatrixes
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
                    -> SB.StanBuilderM env d (FEMatrixes
                                             , SME.StanVar -> SB.StanBuilderM env d SME.StanVar -- Y - mean(X)
                                             )
fixedEffectsQR_Data xVar@(SB.StanVar xName mType@(SB.StanMatrix (rowDim, colDim))) rttFE wgtsM = do
  colKey <- case colDim of
    SB.NamedDim k -> return k
    _ -> SB.stanBuildError $ "fixedEffectsQR_Data: Column dimension of matrix must be a named dimension."
  (qVar, rVar, rInvVar) <- SB.inBlock SB.SBTransformedData $ do
    meanFunction <- case wgtsM of
      Nothing -> return $ "mean(" <> xName <> "[,k])"
      Just (SB.StanVar wgtsName _) -> do
        SBB.weightedMeanFunction
        return $ "weighted_mean(to_vector(" <> wgtsName <> "), " <> xName <> "[,k])"
    meanXV <- SB.stanDeclare ("mean_" <> xName) (SME.StanVector colDim) ""
    centeredXV <- SB.stanDeclare ("centered_" <> xName) mType "" --(SME.StanMatrix (SME.NamedDim rowKey, SME.NamedDim colKey)) ""
    SB.stanForLoopB "k" Nothing colKey $ do
      SB.addStanLine $ "mean_" <> xName <> "[k] = " <> meanFunction --"mean(" <> matrix <> "[,k])"
      SB.addStanLine $ "centered_" <>  xName <> "[,k] = " <> xName <> "[,k] - mean_" <> xName <> "[k]"
    let srE =  SB.function "sqrt" (one $ SB.indexSize' rowDim `SB.minus` SB.scalar "1")
        qRHS = SB.function "qr_thin_Q" (one $ SB.varNameE centeredXV) `SB.times` srE
    qVar' <- SB.stanDeclareRHS (qName rttFE) mType "" qRHS
    let rType = SME.StanMatrix (colDim, colDim)
        rRHS = SB.function "qr_thin_R" (one $ SB.varNameE centeredXV) `SB.divide` srE
    rVar' <- SB.stanDeclareRHS (rName rttFE) rType "" rRHS
    let riRHS = SB.function "inverse" (one $ SB.varNameE rVar')
    rInvVar' <- SB.stanDeclareRHS (rInvName rttFE) rType "" riRHS
    return (qVar', rVar', rInvVar')
  let centeredX mv@(SME.StanVar sn st) =
        case st of
          cmt@(SME.StanMatrix (_, SME.NamedDim colKey)) -> SB.inBlock SB.SBTransformedData $ do
            cv <- SB.stanDeclare ("centered_" <> sn) cmt ""
            SB.stanForLoopB "k" Nothing colKey $ do
              SB.addStanLine $ "centered_" <>  sn <> "[,k] = " <> sn <> "[,k] - mean_" <> xName <> "[k]"
            return cv
          _ -> SB.stanBuildError
               $ "centeredX (from fixedEffectsQR_Data called with x="
               <> show mType
               <> "): called with mv="
               <> show mv
               <> " which is not a matrix type with indexed column dimension."
  return (ThinQR xVar (QRMatrixes qVar rVar rInvVar), centeredX)

fixedEffectsQR_Data _ _ _ = SB.stanBuildError "fixedEffectsQR_Data: called with non-matrix argument."


addFixedEffectsParametersAndPriors :: forall k r1 r2 d env. (Typeable d)
                                   => FixedEffectsModel k
                                   -> FEMatrixes
                                   -> SB.RowTypeTag r1
                                   -> SB.RowTypeTag r2
                                   -> Maybe Text
                                   -> SB.StanBuilderM env d (SB.StanExpr -- Q * theta (or X * beta)
                                                            , SB.IndexKey -> SB.StanVar -> SB.StanBuilderM env d SME.StanExpr -- Y ->  m (Y * beta)
                                                            )
addFixedEffectsParametersAndPriors feModel feMatrixes rttFE rttModeled mVarSuffix
  = fixedEffectsQR_Parameters feModel rttFE rttModeled feMatrixes mVarSuffix


checkModelDataConsistency :: FixedEffectsModel k -> FEMatrixes -> SB.StanBuilderM env d ()
checkModelDataConsistency model (NoQR _) = if thinQR model
                                           then SB.stanBuildError "thinQR is true in model but matrices are NoQR"
                                           else return ()

checkModelDataConsistency model (ThinQR _ _) = if thinQR model
                                               then return ()
                                               else SB.stanBuildError "thinQR is false in model but matrices are ThinQR"



fixedEffectsQR_Parameters :: FixedEffectsModel k
                          -> SB.RowTypeTag rFE
                          -> SB.RowTypeTag rM
                          -> FEMatrixes
                          -> Maybe Text
                          -> SB.StanBuilderM env d (SME.StanExpr -- theta
                                                   , SB.IndexKey -> SME.StanVar -> SB.StanBuilderM env d SME.StanExpr -- X -> X * beta
                                                   )
fixedEffectsQR_Parameters feModel rttFE rttModeled feMatrixes mVarSuffix = do
  checkModelDataConsistency feModel feMatrixes
  case feModel of
    NonInteractingFE _ fePrior -> feParametersNoInteraction feMatrixes rttFE mVarSuffix
    InteractingFE _ gtt gm  -> feParametersWithInteraction feMatrixes gtt gm rttFE rttModeled mVarSuffix

colDimAndDimKeys :: FEMatrixes -> SB.StanBuilderM env d (SME.StanDim, SME.IndexKey, SME.IndexKey)
colDimAndDimKeys feMatrixes =
  case SME.varType (xM feMatrixes) of
    SME.StanMatrix (SME.NamedDim rk, SME.NamedDim ck) -> return (SME.NamedDim ck, rk, ck)
    _ -> SB.stanBuildError $ "colDimAndDimKeys: Bad type (" <> show (SME.varType (xM feMatrixes)) <> ")"

reIndex :: SME.IndexKey -> SME.IndexKey -> SME.StanVar -> SB.StanBuilderM env d SME.StanVar
reIndex oldK newK sv =
  let (sv', hasChanged) = SME.varChangeIndexKey oldK newK sv
  in if hasChanged
     then return sv'
     else SB.stanBuildError $ "reIndex: Failed to re-index matrix with fixed-effects data crosswalk. "
          <> "oldKey=" <> show oldK <> "; newKey=" <> show newK <> "; M=" <> show sv

vectorizedBetaMult :: SME.StanVar -> SME.StanVar -> SB.StanBuilderM env d SME.StanExpr
vectorizedBetaMult betaVar x = case x of
  SB.StanVar mName (SB.StanMatrix (SB.NamedDim rowKey, _)) -> return $ x `SB.matMult` betaVar
  _ -> SB.stanBuildError
       $ "vectorizedBetaMult x (from fixedEffectsQR_Parameters)"
       <> " called with non-matrix or matrix with non-indexed row-dimension. x="
       <> show x

feParametersNoInteraction :: FEMatrixes
                          -> SB.RowTypeTag r
                          -> Maybe Text
                          -> SB.StanBuilderM env d (SME.StanExpr -- Q * theta (or X * beta)
                                                   , SB.IndexKey -> SME.StanVar -> SB.StanBuilderM env d SME.StanExpr -- X -> X * beta
                                                   )
feParametersNoInteraction feMatrixes rttFE mVarSuffix = do
  let xVar = xM feMatrixes
      xName = SME.varName xVar
      xWalkIndexKey = SB.crosswalkIndexKey rttFE
  (colDim, rowIndexKey, colIndexKey) <- colDimAndDimKeys feMatrixes
  let varName x = x <> fromMaybe "" mVarSuffix <> "_" <> xName
  case feMatrixes of
    NoQR xVar -> do
      betaVar <- SB.inBlock SB.SBParameters $ SB.stanDeclare (varName "beta") (SME.StanVector colDim) ""
      xVar' <- reIndex rowIndexKey xWalkIndexKey xVar
      return (xVar' `SME.matMult` betaVar, \_ -> vectorizedBetaMult betaVar)
    ThinQR _ (QRMatrixes qVar _ rInvVar) -> do
      thetaVar <- SB.inBlock SB.SBParameters $ SB.stanDeclare (varName "theta") (SME.StanVector colDim) ""
      SB.inBlock SB.SBTransformedParameters $ do
        betaVar <- SB.stanDeclare (varName "beta") (SME.StanVector colDim) ""
        SB.addExprLine "feParametersNoInteraction"
          $ SME.vectorizedOne colIndexKey
          $ SME.var betaVar `SME.eq` (rInvVar `SME.matMult` thetaVar)
        qVar' <- reIndex rowIndexKey xWalkIndexKey qVar
        return (qVar' `SME.matMult` thetaVar, \_ -> vectorizedBetaMult betaVar)

vectorizedMultE :: SME.StanName -> SME.IndexKey -> SME.StanExpr -> SME.StanVar -> SB.StanBuilderM env d SME.StanExpr
vectorizedMultE vecName vecIndex vecExpr x = case x of
  SB.StanVar mName (SB.StanMatrix (SB.NamedDim rowKey, _)) -> do
    vecVar <- SB.stanDeclare vecName (SME.StanVector $ SME.NamedDim vecIndex) ""
    SB.stanForLoopB "n" Nothing vecIndex
      $ SB.addExprLine "vectorizedMultE" $ SB.var vecVar `SME.eq` vecExpr
    return $ x `SB.matMult` vecVar
  _ -> SB.stanBuildError
       $ "vectorizedBetaMult x (from fixedEffectsQR_Parameters)"
       <> " called with non-matrix or matrix with non-indexed row-dimension. x="
       <> show x

feParametersWithInteraction :: FEMatrixes
                            -> SB.GroupTypeTag k
                            -> SGM.GroupModel
                            -> SB.RowTypeTag rFE
                            -> SB.RowTypeTag rM
                            -> Maybe Text
                            -> SB.StanBuilderM env d (SME.StanExpr -- Q * theta (or X * beta)
                                                     , SB.IndexKey -> SME.StanVar -> SB.StanBuilderM env d SME.StanExpr -- X -> X * beta
                                                     )
feParametersWithInteraction feMatrixes gtt gm rttFE rttModeled mVarSuffix = do
  (SB.IntIndex groupSize _) <- SB.rowToGroupIndex <$> SB.indexMap rttModeled gtt -- has to be vs modeled group since that is where groups are defined
  if groupSize == 2
    then feBinaryInteraction feMatrixes gtt gm rttFE rttModeled mVarSuffix
    else feNonBinaryInteraction feMatrixes gtt gm rttFE rttModeled mVarSuffix

feBinaryInteraction :: FEMatrixes
                    -> SB.GroupTypeTag k
                    -> SGM.GroupModel
                    -> SB.RowTypeTag rFE
                    -> SB.RowTypeTag rM
                    -> Maybe Text
                    -> SB.StanBuilderM env d (SME.StanExpr -- Q * {-eps, eps} (or X * {-eps, eps})
                                             , SB.IndexKey -> SME.StanVar -> SB.StanBuilderM env d SME.StanExpr -- X -> X * beta
                                             )
feBinaryInteraction feMatrixes gtt gm rttFE rttModeled mVarSuffix = do
  let xName = SME.varName $ xM feMatrixes
      varName x = x <> SB.taggedGroupName gtt <> fromMaybe "" mVarSuffix <> "_" <> xName
      modeledIndexKey = SB.dataSetName rttModeled
      xWalkIndexKey = SB.crosswalkIndexKey rttFE
      pmMatE sv = SME.indexBy (SME.bracket (SME.csExprs (SB.var sv :| [SME.negate $ SB.var sv]))) (SB.taggedGroupName gtt)
  (colDim, rowIndexKey, colIndexKey) <- colDimAndDimKeys feMatrixes
  let vecE x sv = SME.vectorizedOne colIndexKey $ SME.function "dot_product" $ SB.var x :| [pmMatE sv]
  case gm of
    SGM.BinarySymmetric epsPriorE -> case feMatrixes of
      NoQR xVar -> do
        xVar' <- reIndex rowIndexKey xWalkIndexKey xVar
        epsVar <- SB.inBlock SB.SBParameters $ SB.stanDeclare (varName "eps") (SME.StanVector colDim) ""
        xEpsVecVar <- SB.useDataSetForBindings rttModeled $ SB.inBlock SB.SBModel $ SBB.vectorizeExpr (varName "epsVec") (vecE xVar' epsVar) modeledIndexKey
        return (SB.var xEpsVecVar, \ik x -> fmap SB.var (SBB.vectorizeExpr (varName "epsVec") (vecE x epsVar) ik))
--                   vectorizedMultE (varName "epsVec") modeledIndexKey (pmVecE epsVar
      ThinQR _ (QRMatrixes qVar _ rInvVar) -> do
        qVar' <- reIndex rowIndexKey xWalkIndexKey qVar
        thetaVar <- SB.inBlock SB.SBParameters $ SB.stanDeclare (varName "theta") (SME.StanVector colDim) ""
        betaVar <- SB.inBlock SB.SBTransformedParameters $ do
          betaVar' <-  SB.stanDeclare (varName "beta") (SME.StanVector colDim) ""
          SB.addExprLine "feParametersNoInteraction"
            $ SME.vectorizedOne colIndexKey
            $ SME.var betaVar' `SME.eq` (rInvVar `SME.matMult` thetaVar)
          return betaVar'
        qThetaVecVar <-  SB.useDataSetForBindings rttModeled $ SB.inBlock SB.SBModel $ SBB.vectorizeExpr (varName "thetaVec") (vecE qVar' thetaVar) modeledIndexKey
        return (SB.var qThetaVecVar, \ik x -> fmap SB.var (SBB.vectorizeExpr (varName "thetaVec") (vecE x thetaVar) ik))
--                   vectorizedMultE (varName "betaVec") modeledIndexKey (pmVecE betaVar))
    SGM.Binary muPriorE epsPriorE -> undefined


feNonBinaryInteraction :: FEMatrixes
                       -> SB.GroupTypeTag k
                       -> SGM.GroupModel
                       -> SB.RowTypeTag rFE
                       -> SB.RowTypeTag rM
                       -> Maybe Text
                       -> SB.StanBuilderM env d (SME.StanExpr -- Q * Theta (or X * Beta)
                                                , SB.IndexKey -> SME.StanVar -> SB.StanBuilderM env d SME.StanExpr -- X -> X * beta
                                                )
feNonBinaryInteraction feMatrixes gtt gm rttFE rttModeled mVarSuffix = case gm of
  SGM.NonHierarchical stz betaPriorE -> undefined
  SGM.Hierarchical stz hps hp -> undefined
  _ -> SB.stanBuildError $ "FixedEffects.feNonBinaryInteraction called with binary group model"

{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}

module Stan.ModelBuilder.DesignMatrix where

import Prelude hiding (All)
import qualified Stan.ModelBuilder.BuildingBlocks as SBB
import qualified Stan.ModelBuilder.ModelParameters as SMP
import qualified Stan.ModelBuilder as SB

import qualified Control.Foldl as FL
import qualified Control.Scanl as SL
import Data.Functor.Contravariant (Contravariant(..))
import qualified Data.Array as Array
import qualified Data.Map as Map
import qualified Data.Set as Set
import qualified Data.Vector.Unboxed as V
import qualified CmdStan as SB
import qualified Stan.ModelConfig as SB

data DesignMatrixRowPart r = DesignMatrixRowPart { dmrpName :: Text
                                                 , dmrpLength :: Int
                                                 , dmrpVecF :: r -> V.Vector Double
                                                 }


instance Contravariant DesignMatrixRowPart where
  contramap g (DesignMatrixRowPart n l f) = DesignMatrixRowPart n l (f . g)


stackDesignMatrixRowParts :: DesignMatrixRowPart r1 -> DesignMatrixRowPart r2 -> Either Text (DesignMatrixRowPart (Either r1 r2))
stackDesignMatrixRowParts d1 d2 = do
  when (dmrpName d1 /= dmrpName d2) $ Left $ "stackDesignMatrixRowPart: Name mismatch! d1=" <> dmrpName d1 <> "; d2=" <> dmrpName d2
  when (dmrpLength d1 /= dmrpLength d2) $ Left $ "stackDesignMatrixRowPart: Length mismatch! l(d1)=" <> show (dmrpLength d1) <> "; l(d2)=" <> show (dmrpLength d2)
  pure $ DesignMatrixRowPart (dmrpName d1) (dmrpLength d1) (either (dmrpVecF d1) (dmrpVecF d2))



data DesignMatrixRow r = DesignMatrixRow { dmName :: Text
                                         , dmParts :: [DesignMatrixRowPart r]
                                         }

stackDesignMatrixRows :: DesignMatrixRow r1 -> DesignMatrixRow r2 -> Either Text (DesignMatrixRow (Either r1 r2))
stackDesignMatrixRows dm1 dm2 = do
  when (dmName dm1 /= dmName dm2) $ Left $ "stackDesignMatrixRows: Name mismatch! dm1=" <> dmName dm1 <> "; dm2=" <> dmName dm2
  newParts <- traverse (uncurry stackDesignMatrixRowParts) $ zip (dmParts dm1) (dmParts dm2)
  return $ DesignMatrixRow (dmName dm1) newParts

dmColIndexName :: DesignMatrixRow r -> Text
dmColIndexName dmr = dmName dmr <> "_Cols"

instance Contravariant DesignMatrixRow where
  contramap g (DesignMatrixRow n dmrps) = DesignMatrixRow n $ fmap (contramap g) dmrps


rowLengthF :: FL.Fold (DesignMatrixRowPart r) Int
rowLengthF = FL.premap dmrpLength FL.sum

rowFuncF :: FL.Fold (DesignMatrixRowPart r) (r -> V.Vector Double)
rowFuncF = appConcat . sequenceA <$> FL.premap dmrpVecF FL.list
  where appConcat g r = V.concat (g r)

matrixFromRowData :: DesignMatrixRow r -> SB.MatrixRowFromData r
matrixFromRowData (DesignMatrixRow name rowParts) = SB.MatrixRowFromData name length f
  where (length, f) = FL.fold ((,) <$> rowLengthF <*> rowFuncF) rowParts

{-
combineRowFuncs :: Foldable f => f (Int, r -> V.Vector Double) -> (Int, r -> V.Vector Double)
combineRowFuncs rFuncs =
  let nF = FL.premap fst FL.sum
      fF = (\r -> V.concat . fmap ($r)) <$> FL.premap snd FL.list
  in FL.fold ((,) <$> nF <*> fF) rFuncs
-}

boundedEnumRowFunc :: forall r k.(Enum k, Bounded k, Eq k) => (r -> k) -> (Int, r -> V.Vector Double)
boundedEnumRowFunc rToKey = case numKeys of
  1 -> error "Single element enum given to boundedEnumRowFunc"
  2 -> binary
  _ -> nonBinary
  where
    keys :: [k] = universe
    numKeys = length keys
    binary = (1, \r -> V.singleton $ realToFrac $ if rToKey r == minBound then -1 else 1)
    oneZero r x = if rToKey r == x then 1 else 0
    nonBinary = (numKeys, \r -> V.fromList $ fmap (oneZero r) keys)

boundedEnumRowPart :: (Enum k, Bounded k, Eq k) => Text -> (r -> k) -> DesignMatrixRowPart r
boundedEnumRowPart name f = DesignMatrixRowPart name n vf
  where (n, vf) = boundedEnumRowFunc f

rowPartFromFunctions :: Text -> [r -> Double] -> DesignMatrixRowPart r
rowPartFromFunctions name fs = DesignMatrixRowPart name (length fs) toVec
  where
    toVec r = V.fromList $ fmap ($r) fs

-- adds matrix (name_dataSetName)
-- adds K_name for col dimension (also <NamedDim name_Cols>)
-- row dimension should be N_dataSetNamer  (which is <NamedDim dataSetName)
-- E.g., if name="Design" and dataSetName="myDat"
-- In data
-- "Int N_myDat;" (was already there)
-- "Int K_Design;"
-- "matrix[N_myDat, K_Design] Design_myDat;"
-- with accompanying json
addDesignMatrix :: (Typeable md, Typeable gq) => SB.RowTypeTag r -> DesignMatrixRow r -> SB.StanBuilderM md gq SB.StanVar
addDesignMatrix rtt dmr = SB.add2dMatrixJson rtt (matrixFromRowData dmr) ""  (SB.NamedDim (SB.dataSetName rtt))

designMatrixIndexes :: DesignMatrixRow r -> [(Text, Int, Int)]
designMatrixIndexes (DesignMatrixRow _ dmps)= SL.scan rowPartScan dmps where
  rowPartScanStep rp = do
    curIndex <- get
    put $ curIndex + dmrpLength rp
    return (dmrpName rp, dmrpLength rp, curIndex)
  rowPartScan = SL.Scan rowPartScanStep 1

-- adds J_Group and Group_Design_Index for all parts of row
addDesignMatrixIndexes :: (Typeable md, Typeable gq) => SB.RowTypeTag r -> DesignMatrixRow r -> SB.StanBuilderM md gq ()
addDesignMatrixIndexes rtt dmr = do
  let addEach (gName, gSize, gStart) = do
        let sizeName = dmName dmr <> "_" <> gName
        sv <- SB.addFixedIntJson (SB.inputDataType rtt) sizeName Nothing gSize
        SB.addDeclBinding sizeName sv
        SB.addUseBindingToDataSet rtt sizeName sv
        SB.addFixedIntJson (SB.inputDataType rtt) (gName <> "_" <> dmName dmr <> "_Index") Nothing gStart
        pure ()
  traverse_ addEach $ designMatrixIndexes dmr

-- we assume we've already checked the dimension
splitToGroupVar :: Text -> Text -> SB.StanVar -> SB.StanBuilderM md gq SB.StanVar
splitToGroupVar dName gName v@(SB.StanVar n st) = do
  let newVarName = n <> "_" <> gName
      index = gName <> "_" <> dName <> "_Index"
      sizeName = dName <> "_" <> gName
      namedDimE x = SB.stanDimToExpr $ SB.NamedDim x
      vecDM = SB.vectorizedOne (dName <> "_Cols")
      segment x = vecDM $ SB.function "segment" $ SB.var x :| [SB.name index, namedDimE sizeName]
      block d x = vecDM $ SB.function "block" $ SB.var x :| [SB.scalar "1", SB.name index, namedDimE d, namedDimE sizeName]
  case st of
    SB.StanVector _ -> SB.stanDeclareRHS newVarName (SB.StanVector $ SB.NamedDim sizeName) "" $ segment v
    SB.StanArray [SB.NamedDim d] (SB.StanVector _) -> do
      nv <- SB.stanDeclare newVarName (SB.StanArray [SB.NamedDim d] $ SB.StanVector $ SB.NamedDim sizeName) ""
      SB.stanForLoopB "k" Nothing d $ SB.addExprLine "splitToGroupVec" $ SB.var nv `SB.eq` segment v
      return nv
    SB.StanMatrix (SB.NamedDim d, _) -> SB.stanDeclareRHS newVarName (SB.StanMatrix (SB.NamedDim d, SB.NamedDim sizeName)) "" $ block d v
    _ -> SB.stanBuildError "DesignMatrix.splitToGroupVar: Can only split vectors, arrays of vectors or matrices. And the latter, only with named row dimension."

-- take a stan vector, array, or matrix indexed by this design row
-- and split into the parts for each group
-- this doesn't depend on r
splitToGroupVars :: DesignMatrixRow r -> SB.StanVar -> SB.StanBuilderM md gq [SB.StanVar]
splitToGroupVars dmr@(DesignMatrixRow n _) v@(SB.StanVar _ st) = do
  let designColName = n <> "_Cols"
  case st of
    SB.StanVector d -> when (d /= SB.NamedDim designColName)
      $ SB.stanBuildError $ "DesignMatrix.splitTogroupVars: vector to split has wrong dimension: " <> show d
    SB.StanArray _ (SB.StanVector d) -> when (d /= SB.NamedDim designColName)
      $ SB.stanBuildError $ "DesignMatrix.splitTogroupVars: vectors in array of vectors to split has wrong dimension: " <> show d
    SB.StanMatrix (d, _)  -> when (d /= SB.NamedDim designColName)
      $ SB.stanBuildError $ "DesignMatrix.splitTogroupVars: matrix to split has wrong row-dimension: " <> show d
  traverse (\(g, _, _) -> splitToGroupVar n g v) $ designMatrixIndexes dmr


data DMParameterization = DMCentered | DMNonCentered deriving (Show, Eq)


addDMParametersAndPriors :: (Typeable md, Typeable gq)
                         => Text
                         -> SB.GroupTypeTag k -- exchangeable contexts
                         -> SB.StanName -- name for beta parameter (so we can use theta if QR)
                         -> DMParameterization
                         -> (SB.StanExpr, SB.StanExpr, Double) -- priors for mu and tau and lkj parameter
                         -> Maybe Text -- suffix for varnames
                         -> SB.StanBuilderM md gq (SB.StanVar
                                                  , SB.StanVar
                                                  , SB.StanVar
                                                  , SB.StanVar
                                                  , SB.StanVar
                                                  )
addDMParametersAndPriors designMatrixName g betaName parameterization (muPrior, tauPrior, lkjParameter) mS = do
  let dmDimName = designMatrixName <> "_Cols"
      dmDim = SB.NamedDim dmDimName
      dmVec = SB.StanVector dmDim
      vecDM = SB.vectorizedOne dmDimName
      gName =  SB.taggedGroupName g
      gDim = SB.NamedDim gName
      gVec = SB.StanVector gDim
      vecG = SB.vectorizedOne gName
      s = fromMaybe "" mS
      normal x = SB.normal Nothing (SB.scalar $ show x)
      dmBetaE dm beta = vecDM $ SB.function "dot_product" (SB.var dm :| [SB.var beta])
      lkjPriorE = SB.function "lkj_corr_cholesky" (SB.scalar (show lkjParameter) :| [])

  (alphaRaw, muAlpha, sigmaAlpha, mu, tau, lCorr, betaRaw) <- SB.inBlock SB.SBParameters $ do
    alphaRaw' <- case parameterization of
      DMCentered -> SB.stanDeclare ("alpha" <> s) gVec ""
      DMNonCentered -> SB.stanDeclare ("alpha_raw" <> s) gVec ""
    muAlpha' <- SB.stanDeclare ("mu_alpha" <> s) SB.StanReal ""
    sigmaAlpha' <- SB.stanDeclare ("sigma_alpha" <> s) SB.StanReal "<lower=0>"
    mu' <- SB.stanDeclare ("mu" <> s) dmVec ""
    tau' <- SB.stanDeclare ("tau" <> s) dmVec "<lower=0>"
    lCorr' <- SB.stanDeclare ("L" <> s) (SB.StanCholeskyFactorCorr dmDim) ""
    betaRaw' <- case parameterization of
      DMCentered -> SB.stanDeclare (betaName <> s) (SB.StanMatrix (dmDim, gDim)) ""
      DMNonCentered -> SB.stanDeclare (betaName <> s <> "_raw") (SB.StanMatrix (dmDim, gDim)) ""
    return (alphaRaw', muAlpha', sigmaAlpha', mu', tau', lCorr', betaRaw')
  let dpmE = SB.function "diag_pre_multiply" (SB.var tau :| [SB.var lCorr])
      repMu = SB.function "rep_matrix" (SB.var mu :| [SB.indexSize gName])
  beta <- case parameterization of
    DMCentered -> return betaRaw
    DMNonCentered -> SB.inBlock SB.SBTransformedParameters $
      SB.stanDeclareRHS (betaName <> s) (SB.StanMatrix (dmDim,gDim)) ""
        $ vecG $ vecDM $ repMu `SB.plus` (dpmE `SB.times` SB.var betaRaw)
  alpha <- case parameterization of
    DMCentered -> return alphaRaw
    DMNonCentered -> SB.inBlock SB.SBTransformedParameters $
      SB.stanDeclareRHS ("alpha" <> s) gVec ""
      $ vecG $ SB.var muAlpha `SB.plus` (SB.var sigmaAlpha `SB.times` SB.var alphaRaw)
  SB.inBlock SB.SBModel $ do
    case parameterization of
      DMCentered -> do
        SB.addExprLines "addDMParametersAnsPriors"
          [vecDM $ SB.var betaRaw `SB.vectorSample` SB.function "multi_normal_cholesky" (SB.var mu :| [dpmE])
          , vecG $ SB.var alphaRaw `SB.vectorSample` SB.function "normal" (SB.var muAlpha :| [SB.var sigmaAlpha])]
      DMNonCentered ->
--        SB.stanForLoopB "g" Nothing gName
        SB.addExprLines "addDMParametersAndPriors"
        [vecG $ vecDM $ SB.function "to_vector" (one $ SB.var betaRaw) `SB.vectorSample` SB.stdNormal
        , vecG $ SB.var alphaRaw `SB.vectorSample` SB.stdNormal]

    SB.addExprLines "addParametersAndPriors" $
      [ SB.var muAlpha `SB.vectorSample` muPrior
      , SB.var sigmaAlpha `SB.vectorSample` tauPrior
      , vecDM $ SB.var mu `SB.vectorSample` muPrior
      , vecDM $ SB.var tau `SB.vectorSample` tauPrior
      , vecDM $ SB.var lCorr `SB.vectorSample` lkjPriorE
      ]
  pure (alpha, beta, mu, tau, lCorr)


centerDataMatrix :: (Typeable md, Typeable gq)
                 => SB.StanVar -- matrix
                 -> Maybe SB.StanVar -- row weights
                 -> SB.StanBuilderM md gq (SB.StanVar -- centered matrix, X - row_mean(X)
                                          , SB.InputDataType -> SB.StanVar -> Maybe Text -> SB.StanBuilderM md gq SB.StanVar -- \Y -> Y - row_mean(X)
                                          )
centerDataMatrix mV@(SB.StanVar mn mt) mwgtsV = do
  (rowIndexKey, colIndexKey) <- case mt of
    SB.StanMatrix (rowDim, colDim) -> do
      rowIndex <- case rowDim of
        SB.NamedDim indexKey -> pure indexKey
        _ -> SB.stanBuildError "DesignMatrix.centerDataMatrix: row dimension of given matrix must be a named dimension"
      colIndex <- case colDim of
        SB.NamedDim indexKey -> pure indexKey
        _ -> SB.stanBuildError "DesignMatrix.centerDataMatrix: column dimension of given matrix must be a named dimension"
      return (rowIndex, colIndex)
    _ -> SB.stanBuildError "DesignMatrix.centerDataMatrix: Given argument must be a marix."
  (centeredXV, meanXV) <- SB.inBlock SB.SBTransformedData $ do
    meanFunction <- case mwgtsV of
      Nothing -> return $ SB.function "mean" (one $ SB.vectorizedOne rowIndexKey (SB.var mV))
      Just wgtsV -> do
        SBB.weightedMeanFunction
        let vectorizedWgts = SB.function "to_vector" (one $ SB.vectorizedOne rowIndexKey (SB.var wgtsV))
        return $ SB.function "weighted_mean" (vectorizedWgts :| [SB.vectorizedOne rowIndexKey (SB.var mV)])
    meanXV' <- SB.stanDeclareRHS ("mean_" <> mn) (SB.StanVector $ SB.NamedDim colIndexKey) ""
      $ SB.function "rep_vector"  (SB.scalar "0" :| [SB.indexSize colIndexKey])
    centeredXV' <- SB.stanDeclare ("centered_" <> mn) mt ""
    SB.stanForLoopB "k" Nothing colIndexKey $ do
      SB.addExprLine "centerDataMatrix" $ SB.var meanXV' `SB.eq` meanFunction
      SB.addExprLine "centerDataMatrix"
        $ SB.vectorizedOne rowIndexKey $ SB.var centeredXV' `SB.eq` (SB.var mV `SB.minus` SB.var meanXV')
    pure (centeredXV', meanXV')
  let centeredX idt mv@(SB.StanVar mn mt) mSuffix = do
        let codeBlock = if idt == SB.ModelData then SB.SBTransformedData else SB.SBTransformedDataGQ
        case mt of
          cmt@(SB.StanMatrix (SB.NamedDim rKey, SB.NamedDim cKey)) -> SB.inBlock codeBlock $ do
            cv <- SB.stanDeclare ("centered_" <> mn <> fromMaybe "" mSuffix) cmt ""
            SB.stanForLoopB "k" Nothing cKey $ do
              SB.addExprLine "centerData.Matrix.centeredX"$ SB.vectorizedOne rKey $ SB.var cv `SB.eq` (SB.var mv `SB.minus` SB.var meanXV)
            return cv
          _ -> SB.stanBuildError
               $ "centeredX (from DesignMatrix.centerDataMatrix called with x="
               <> show mt
               <> "): called with mv="
               <> show mv
               <> " which is not a matrix type with indexed row and column dimension."
  pure (centeredXV, centeredX)

-- take a matrix x and return (thin) Q, R and inv(R)
-- as Q_x, R_x, invR_x
-- see https://mc-stan.org/docs/2_28/stan-users-guide/QR-reparameterization.html
thinQR :: SB.StanVar -- matrix of predictors
       -> Maybe (SB.StanVar, SB.StanName) -- theta and name for beta
       -> SB.StanBuilderM md gq (SB.StanVar, SB.StanVar, SB.StanVar, Maybe SB.StanVar)
thinQR xVar@(SB.StanVar xName mType@(SB.StanMatrix (rowDim, SB.NamedDim colDimName))) mThetaBeta = do
  let colDim = SB.NamedDim colDimName
      vecDM = SB.vectorizedOne colDimName
      srE =  SB.function "sqrt" (one $ SB.indexSize' rowDim `SB.minus` SB.scalar "1")
      qRHS = SB.function "qr_thin_Q" (one $ SB.varNameE xVar) `SB.times` srE
  (q, r, rI) <- SB.inBlock SB.SBTransformedData $ do
    qV  <- SB.stanDeclareRHS ("Q_" <> xName) mType "" qRHS
    let rType = SB.StanMatrix (colDim, colDim)
        rRHS = SB.function "qr_thin_R" (one $ SB.varNameE xVar) `SB.divide` srE
    rV <- SB.stanDeclareRHS ("R_" <> xName) rType "" rRHS
    let riRHS = SB.function "inverse" (one $ SB.varNameE rV)
    rInvV <- SB.stanDeclareRHS ("invR_" <> xName) rType "" riRHS
    return (qV, rV, rInvV)
  mBeta <- case mThetaBeta of
    Nothing -> return Nothing
    Just (theta@(SB.StanVar _ betaType), betaName) -> case betaType of
      SB.StanMatrix (_, SB.NamedDim gDimName) ->
        fmap Just
           $ SB.inBlock SB.SBGeneratedQuantities
           $ SB.stanDeclareRHS betaName betaType ""
           $ SB.vectorizedOne gDimName
           $ vecDM $ rI `SB.matMult` theta
      SB.StanVector (SB.NamedDim gDimName) ->
        fmap Just
           $ SB.inBlock SB.SBGeneratedQuantities
           $ SB.stanDeclareRHS betaName betaType ""
           $ vecDM $ rI `SB.matMult` theta
      _ -> SB.stanBuildError $ "DesignMatrix.thinQR: bad type given for theta: type=" <> show betaType
  return (q, r, rI, mBeta)

thinQR x _ = SB.stanBuildError $ "Non matrix variable given to DesignMatrix.thinQR: v=" <> show x

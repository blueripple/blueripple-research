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
import qualified Stan.ModelBuilder as SB

import qualified Control.Foldl as FL
import qualified Control.Scanl as SL
import Data.Functor.Contravariant (Contravariant(..))
import qualified Data.Array as Array
import qualified Data.Map as Map
import qualified Data.Set as Set
import qualified Data.Vector.Unboxed as V

data DesignMatrixRowPart r = DesignMatrixRowPart { dmrpName :: Text
                                                 , dmrpLength :: Int
                                                 , dmrpVecF :: r -> V.Vector Double
                                                 }


instance Contravariant DesignMatrixRowPart where
  contramap g (DesignMatrixRowPart n l f) = DesignMatrixRowPart n l (f . g)

data DesignMatrixRow r = DesignMatrixRow { dmName :: Text
                                         , dmParts :: [DesignMatrixRowPart r]
                                         }

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
        sv <- SB.addFixedIntJson ("J_" <> gName) Nothing gSize
        SB.addDeclBinding gName sv
        SB.addUseBindingToDataSet rtt gName sv
        SB.addFixedIntJson (gName <> "_" <> dmName dmr <> "_Index") Nothing gStart
        pure ()
  traverse_ addEach $ designMatrixIndexes dmr

-- we assume we've already checked the dimension
splitToGroupVar :: Text -> Text -> SB.StanVar -> SB.StanBuilderM md gq SB.StanVar
splitToGroupVar dName gName v@(SB.StanVar n st) = do
  let newVarName = n <> "_" <> gName
      index = gName <> "_" <> dName <> "_Index"
      namedDimE x = SB.stanDimToExpr $ SB.NamedDim x
      vecDM = SB.vectorizedOne (dName <> "_Cols")
      segment x = vecDM $ SB.function "segment" $ SB.var x :| [SB.name index, namedDimE gName]
      block d x = vecDM $ SB.function "block" $ SB.var x :| [SB.scalar "1", SB.name index, namedDimE d, namedDimE gName]
  case st of
    SB.StanVector _ -> SB.stanDeclareRHS newVarName (SB.StanVector $ SB.NamedDim gName) "" $ segment v
    SB.StanArray [SB.NamedDim d] (SB.StanVector _) -> do
      nv <- SB.stanDeclare newVarName (SB.StanArray [SB.NamedDim d] $ SB.StanVector $ SB.NamedDim gName) ""
      SB.stanForLoopB "k" Nothing d $ SB.addExprLine "splitToGroupVec" $ SB.var nv `SB.eq` segment v
      return nv
    SB.StanMatrix (SB.NamedDim d, _) -> SB.stanDeclareRHS newVarName (SB.StanMatrix (SB.NamedDim d, SB.NamedDim gName)) "" $ block d v
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

addDMParametersAndPriors :: (Typeable md, Typeable gq)
                         => SB.RowTypeTag r -- for bindings
                         -> DesignMatrixRow r
                         -> SB.GroupTypeTag k -- exchangeable contexts
                         -> (Double, Double, Double) -- prior widths and lkj parameter
                         -> Maybe Text -- suffix for varnames
                         -> SB.StanBuilderM md gq (SB.StanVar, SB.StanVar, SB.StanVar, SB.StanVar)
addDMParametersAndPriors rtt (DesignMatrixRow n _) g (muSigma, tauSigma, lkjParameter) mS = SB.useDataSetForBindings rtt $ do
  let dmDimName = n <> "_Cols"
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
  (mu, tau, lCorr, betaRaw) <- SB.inBlock SB.SBParameters $ do
    mu' <- SB.stanDeclare ("mu" <> s) dmVec ""
    tau' <- SB.stanDeclare ("tau" <> s) dmVec "<lower=0>"
    lCorr' <- SB.stanDeclare ("L" <> s) (SB.StanCholeskyFactorCorr dmDim) ""
    betaRaw' <- SB.stanDeclare ("beta" <> s <> "_raw") (SB.StanArray [gDim] $ SB.StanVector dmDim) ""
    return (mu', tau', lCorr', betaRaw')
  beta <- SB.inBlock SB.SBTransformedParameters $ do
    beta' <- SB.stanDeclare ("beta" <> s) (SB.StanArray [gDim] dmVec) ""
    let dpmE = SB.function "diag_pre_multiply" (SB.var tau :| [SB.var lCorr])
    SB.stanForLoopB "s" Nothing gName
      $ SB.addExprLine "electionModelDM"
      $ vecDM $ SB.var beta' `SB.eq` (SB.var mu `SB.plus` (dpmE `SB.times` SB.var betaRaw))
    return beta'
  SB.inBlock SB.SBModel $ do
      SB.stanForLoopB "g" Nothing gName
        $ SB.addExprLine "addDMParametersAndPriors"
        $ vecDM $ SB.var betaRaw `SB.vectorSample` SB.stdNormal
      SB.addExprLines "addParametersAndPriors" $
        [vecDM $ SB.var mu `SB.vectorSample` normal muSigma
        , vecDM $ SB.var tau `SB.vectorSample` normal tauSigma
        , vecDM $ SB.var lCorr `SB.vectorSample` lkjPriorE
        ]
  pure (beta, mu, tau, lCorr)


centerDataMatrix :: (Typeable md, Typeable gq)
             => Bool
             -> SB.StanVar -- matrix
             -> Maybe SB.StanVar -- row weights
             -> SB.StanBuilderM md gq (SB.StanVar -- centered matrix, X - row_mean(X)
                                      , SB.StanVar -> SB.StanBuilderM md gq SB.StanVar -- \Y -> Y - row_mean(X)
                                      )
centerDataMatrix skipAlphaCol mV@(SB.StanVar mn mt) mwgtsV = do
  colIndexKey <- case mt of
    SB.StanMatrix (rowDim, colDim) -> case colDim of
      SB.NamedDim indexKey -> pure indexKey
      _ -> SB.stanBuildError "DesignMatrix.centerDataMatrix: column dimension of given matrix must be a named dimension"
    _ -> SB.stanBuildError "DesignMatrix.centerDataMatrix: Given argument must be a marix."
  let mStart = if skipAlphaCol then Just (SB.scalar "2") else Nothing
  centeredXV <- SB.inBlock SB.SBTransformedData $ do
    meanFunction <- case mwgtsV of
      Nothing -> return $ "mean(" <> mn <> "[,k])"
      Just (SB.StanVar wgtsName _) -> do
        SBB.weightedMeanFunction
        return $ "weighted_mean(to_vector(" <> wgtsName <> "), " <> mn <> "[,k])"
    meanXV <- SB.stanDeclareRHS ("mean_" <> mn) (SB.StanVector $ SB.NamedDim colIndexKey) ""
      $ SB.function "rep_vector"  (SB.scalar "0" :| [SB.indexSize colIndexKey])
    centeredXV' <- SB.stanDeclare ("centered_" <> mn) mt ""
    SB.stanForLoopB "k" mStart colIndexKey $ do
      SB.addStanLine $ "mean_" <> mn <> "[k] = " <> meanFunction
    SB.stanForLoopB "k" Nothing colIndexKey $ do
      SB.addStanLine $ "centered_" <>  mn <> "[,k] = " <> mn <> "[,k] - mean_" <> mn <> "[k]"
    pure centeredXV'
  let centeredX mv@(SB.StanVar sn st) =
        case st of
          cmt@(SB.StanMatrix (_, SB.NamedDim colIndexKey)) -> SB.inBlock SB.SBTransformedData $ do
            cv <- SB.stanDeclare ("centered_" <> sn) cmt ""
            SB.stanForLoopB "k" Nothing colIndexKey $ do
              SB.addStanLine $ "centered_" <> sn <> "[,k] = " <> sn <> "[,k] - mean_" <> mn <> "[k]"
            return cv
          _ -> SB.stanBuildError
               $ "centeredX (from DesignMatrix.centerDataMatrix called with x="
               <> show mt
               <> "): called with mv="
               <> show mv
               <> " which is not a matrix type with indexed column dimension."
  pure (centeredXV, centeredX)

-- take a matrix x and return (thin) Q, R and inv(R)
-- as Q_x, R_x, invR_x
-- see https://mc-stan.org/docs/2_28/stan-users-guide/QR-reparameterization.html
thinQR :: SB.StanVar -> SB.StanBuilderM md gq (SB.StanVar, SB.StanVar, SB.StanVar)
thinQR xVar@(SB.StanVar xName mType@(SB.StanMatrix (rowDim, colDim))) = do
   let srE =  SB.function "sqrt" (one $ SB.indexSize' rowDim `SB.minus` SB.scalar "1")
       qRHS = SB.function "qr_thin_Q" (one $ SB.varNameE xVar) `SB.times` srE
   qV  <- SB.stanDeclareRHS ("Q_" <> xName) mType "" qRHS
   let rType = SB.StanMatrix (colDim, colDim)
       rRHS = SB.function "qr_thin_R" (one $ SB.varNameE xVar) `SB.divide` srE
   rV <- SB.stanDeclareRHS ("R_" <> xName) rType "" rRHS
   let riRHS = SB.function "inverse" (one $ SB.varNameE rV)
   rInvV <- SB.stanDeclareRHS ("invR_" <> xName) rType "" riRHS
   return (qV, rV, rInvV)
thinQR x = SB.stanBuildError $ "Non matrix variable given to DesignMatrix.thinQR: v=" <> show x

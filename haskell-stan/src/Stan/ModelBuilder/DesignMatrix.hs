{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeApplications #-}
{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}
{-# HLINT ignore "Use for_" #-}

module Stan.ModelBuilder.DesignMatrix where

import Prelude hiding (All)

import qualified Stan.ModelBuilder.TypedExpressions.Types as TE
import qualified Stan.ModelBuilder.TypedExpressions.Indexing as TE
import qualified Stan.ModelBuilder.TypedExpressions.TypedList as TE
import Stan.ModelBuilder.TypedExpressions.TypedList (TypedList(..))
import qualified Stan.ModelBuilder.TypedExpressions.Expressions as TE
import qualified Stan.ModelBuilder.TypedExpressions.Statements as TE
import qualified Stan.ModelBuilder.TypedExpressions.StanFunctions as TE
import qualified Stan.ModelBuilder.TypedExpressions.DAG as DAG



import qualified Stan.ModelBuilder.BuildingBlocks as SBB
--import qualified Stan.ModelBuilder.Parameters as MP
import qualified Stan.ModelBuilder as SB

import qualified Control.Foldl as FL
import qualified Control.Scanl as SL
import Data.Functor.Contravariant (Contravariant(..))
import qualified Data.Array as Array
import qualified Data.List as List
import qualified Data.Map as Map
import qualified Data.Massiv.Array as MA
import qualified Data.Massiv.Vector as MV
import qualified Data.Massiv.Array.Numeric as MN
import qualified Data.Set as Set
import qualified Data.Vector.Unboxed as V
import qualified CmdStan as SB
import qualified Stan.ModelConfig as SB
import Stan.ModelBuilder (RowTypeTag(RowTypeTag))
import Stan.ModelBuilder.TypedExpressions.Operations (SBinaryOp(SBoolean))
import qualified Stan.ModelBuilder.TypedExpressions.Operations as TE
import qualified Stan.ModelBuilder as TE
import qualified Data.Type.Nat as DT
import qualified Data.Aeson as DT
import Data.Type.Equality ((:~:)(..), TestEquality (..))

data DesignMatrixRowPart r = DesignMatrixRowPart { dmrpName :: TE.StanName
                                                 , dmrpLength :: Int
                                                 , dmrpVecF :: r -> V.Vector Double
                                                 }

instance Contravariant DesignMatrixRowPart where
  contramap g (DesignMatrixRowPart n l f) = DesignMatrixRowPart n l (f . g)

-- a co-product
stackDesignMatrixRowParts :: DesignMatrixRowPart r1 -> DesignMatrixRowPart r2 -> Either Text (DesignMatrixRowPart (Either r1 r2))
stackDesignMatrixRowParts d1 d2 = do
  when (dmrpName d1 /= dmrpName d2) $ Left $ "stackDesignMatrixRowPart: Name mismatch! d1=" <> dmrpName d1 <> "; d2=" <> dmrpName d2
  when (dmrpLength d1 /= dmrpLength d2) $ Left $ "stackDesignMatrixRowPart: Length mismatch! l(d1)=" <> show (dmrpLength d1) <> "; l(d2)=" <> show (dmrpLength d2)
  pure $ DesignMatrixRowPart (dmrpName d1) (dmrpLength d1) (either (dmrpVecF d1) (dmrpVecF d2))
{-# INLINEABLE stackDesignMatrixRowParts #-}


data DesignMatrixRow r = DesignMatrixRow { dmName :: TE.StanName
                                         , dmParts :: [DesignMatrixRowPart r]
                                         }

stackDesignMatrixRows :: DesignMatrixRow r1 -> DesignMatrixRow r2 -> Either Text (DesignMatrixRow (Either r1 r2))
stackDesignMatrixRows dm1 dm2 = do
  when (dmName dm1 /= dmName dm2) $ Left $ "stackDesignMatrixRows: Name mismatch! dm1=" <> dmName dm1 <> "; dm2=" <> dmName dm2
  newParts <- traverse (uncurry stackDesignMatrixRowParts) $ zip (dmParts dm1) (dmParts dm2)
  return $ DesignMatrixRow (dmName dm1) newParts
{-# INLINEABLE stackDesignMatrixRows #-}

dmColIndexName :: DesignMatrixRow r -> Text
dmColIndexName dmr = dmName dmr <> "_Cols"
{-# INLINEABLE dmColIndexName #-}

instance Contravariant DesignMatrixRow where
  contramap g (DesignMatrixRow n dmrps) = DesignMatrixRow n $ fmap (contramap g) dmrps

rowLengthF :: FL.Fold (DesignMatrixRowPart r) Int
rowLengthF = FL.premap dmrpLength FL.sum
{-# INLINEABLE rowLengthF #-}

rowFuncF :: FL.Fold (DesignMatrixRowPart r) (r -> V.Vector Double)
rowFuncF = appConcat . sequenceA <$> FL.premap dmrpVecF FL.list
  where appConcat g r = V.concat (g r)
{-# INLINEABLE rowFuncF #-}

matrixFromRowData :: DesignMatrixRow r -> Maybe SB.IndexKey -> SB.MatrixRowFromData r
matrixFromRowData (DesignMatrixRow name rowParts) indexKeyM = SB.MatrixRowFromData name indexKeyM length f
  where (length, f) = FL.fold ((,) <$> rowLengthF <*> rowFuncF) rowParts
{-# INLINEABLE matrixFromRowData #-}

designMatrixRowPartFromMatrixRowData :: SB.MatrixRowFromData r -> DesignMatrixRowPart r
designMatrixRowPartFromMatrixRowData (SB.MatrixRowFromData name _ n rowFunc) = DesignMatrixRowPart name n rowFunc
{-# INLINEABLE designMatrixRowPartFromMatrixRowData #-}
{-
combineRowFuncs :: Foldable f => f (Int, r -> V.Vector Double) -> (Int, r -> V.Vector Double)
combineRowFuncs rFuncs =
  let nF = FL.premap fst FL.sum
      fF = (\r -> V.concat . fmap ($ r)) <$> FL.premap snd FL.list
  in FL.fold ((,) <$> nF <*> fF) rFuncs
-}

-- first argument, if set, will encode as that is all zeroes.
boundedEnumRowFunc :: forall r k.(Enum k, Bounded k, Eq k) => Maybe k -> (r -> k) -> (Int, r -> V.Vector Double)
boundedEnumRowFunc encodeAsZerosM rToKey = case numKeys of
  1 -> error "Single element enum given to boundedEnumRowFunc"
  2 -> binary
  _ -> nonBinary
  where
    binary = (1, \r -> V.singleton $ realToFrac $ if rToKey r == minBound then -1 else 1)
    keys :: [k] = maybe id List.delete encodeAsZerosM universe
    numKeys = length (oneHotKeys encodeAsZerosM)
    oneZero r x = if rToKey r == x then 1 else 0
    nonBinary = (numKeys, oneHotVector encodeAsZerosM . rToKey)
{-# INLINEABLE boundedEnumRowFunc #-}

oneHotKeys :: (Enum k, Bounded k, Eq k) => Maybe k -> [k]
oneHotKeys encodeAsZerosM =  maybe id List.delete encodeAsZerosM universe
{-# INLINEABLE oneHotKeys #-}

oneHotVector :: forall k.(Enum k, Bounded k, Eq k) => Maybe k -> k -> V.Vector Double
oneHotVector encodeAsZerosM k = V.fromList $ fmap (oneZero k) $ oneHotKeys encodeAsZerosM
  where
    oneZero k k' = if k == k' then 1 else 0
{-# INLINEABLE oneHotVector #-}

oneHotVectorMassiv :: forall k.(Enum k, Bounded k, Eq k) => Maybe k -> k -> MV.Vector MV.U Double
oneHotVectorMassiv encodeAsZerosM k = MA.compute $ MV.sfromList $ fmap (oneZero k) $ oneHotKeys encodeAsZerosM
  where
    oneZero k k' = if k == k' then 1 else 0
{-# INLINEABLE oneHotVectorMassiv #-}

boundedEnumRowPart :: (Enum k, Bounded k, Eq k) => Maybe k -> Text -> (r -> k) -> DesignMatrixRowPart r
boundedEnumRowPart encodeAsZerosM name f = DesignMatrixRowPart name n vf
  where (n, vf) = boundedEnumRowFunc encodeAsZerosM f
{-# INLINEABLE boundedEnumRowPart #-}

rowPartFromFunctions :: Text -> [r -> Double] -> DesignMatrixRowPart r
rowPartFromFunctions name fs = DesignMatrixRowPart name (length fs) toVec
  where
    toVec r = V.fromList $ fmap ($ r) fs
{-# INLINEABLE rowPartFromFunctions #-}

rowPartFromBoundedEnumFunctions :: forall k r.(Enum k, Bounded k, Eq k) => Maybe k -> Text -> (k -> r -> Double) -> DesignMatrixRowPart r
rowPartFromBoundedEnumFunctions encodeAsZerosM name f = DesignMatrixRowPart name vecSize (V.fromList . MV.stoList . sumScaledVecs)
  where keys :: [k] = universe
        vecSize :: Int = length keys - maybe 0 (const 1) encodeAsZerosM
        keyedVecs = zip keys $ fmap (oneHotVectorMassiv encodeAsZerosM) keys
        scaledVecs r = fmap (\(k, v) -> v MN..* f k r) keyedVecs
        sumScaledVecs r = FL.fold (FL.Fold (MN.!+!) (MA.compute $ MV.sreplicate (MA.Sz vecSize) 0) id) $ scaledVecs r
{-# INLINEABLE rowPartFromBoundedEnumFunctions #-}

-- adds matrix (name_dataSetName)
-- adds K_name (or given index) for col dimension (also <NamedDim name_Cols>)
-- row dimension should be N_dataSetName (which is <NamedDim dataSetName)
-- E.g., if name="Design" and dataSetName="myDat"
-- In data
-- "Int N_myDat;" (was already there)
-- "Int K_Design;"
-- "matrix[N_myDat, K_Design] Design_myDat;"
-- with accompanying json
addDesignMatrix :: (Typeable md, Typeable gq) => SB.RowTypeTag r -> DesignMatrixRow r -> Maybe SB.IndexKey -> SB.StanBuilderM md gq (TE.UExpr TE.EMat)
addDesignMatrix rtt dmr colIndexM = SB.add2dMatrixJson rtt (matrixFromRowData dmr colIndexM) []
{-# INLINEABLE addDesignMatrix #-}


designMatrixColDimBinding ::  DesignMatrixRow r -> Maybe SB.IndexKey -> (SB.IndexKey, TE.UExpr TE.EInt)
designMatrixColDimBinding dmr indexKeyM = (colIndex, colExpr)
  where
    ik = fromMaybe (dmName dmr) indexKeyM
    colIndex = ik  <> "_Cols"
    colExpr = TE.namedE ("K_" <> ik) TE.SInt
{-# INLINEABLE designMatrixColDimBinding #-}

designMatrixIndexes :: DesignMatrixRow r -> [(DesignMatrixRowPart r, Int, Int)]
designMatrixIndexes (DesignMatrixRow _ dmps)= SL.scan rowPartScan dmps where
  rowPartScanStep rp = do
    curIndex <- get
    put $ curIndex + dmrpLength rp
    return (rp, dmrpLength rp, curIndex)
  rowPartScan = SL.Scan rowPartScanStep 1

designMatrixPartSizeName :: DesignMatrixRow r -> DesignMatrixRowPart r -> TE.StanName
designMatrixPartSizeName dmr dmrp = "S_" <> dmName dmr <> "_" <> dmrpName dmrp
{-# INLINEABLE designMatrixPartSizeName #-}

designMatrixPartIndexName :: DesignMatrixRow r -> DesignMatrixRowPart r -> TE.StanName
designMatrixPartIndexName dmr dmrp = "I_" <> dmName dmr <> "_" <> dmrpName dmrp
{-# INLINEABLE designMatrixPartIndexName #-}


-- declares S_DesignName_PartName (size of part) and I_DesignName_PartName (starting index of part) and for all parts of design matrix row
addDesignMatrixIndexes :: (Typeable md, Typeable gq)
                       => SB.RowTypeTag r -> DesignMatrixRow r -> SB.StanBuilderM md gq [(DesignMatrixRowPart r, TE.UExpr TE.EInt, TE.UExpr TE.EInt)]
addDesignMatrixIndexes rtt dmr = do
  let addEach (rp, gSize, gStart) = do
--        let sizeName = dmName dmr <> "_" <> gName
        se <- SB.addFixedIntJson (SB.inputDataType rtt) (designMatrixPartSizeName dmr rp) Nothing gSize
        ie <- SB.addFixedIntJson (SB.inputDataType rtt) (designMatrixPartIndexName dmr rp) Nothing gStart
        pure (rp, se, ie)
  traverse addEach $ designMatrixIndexes dmr

splitToGroupVar :: forall t r md gq.(TE.IsContainer t, TE.GenSType t)
                => (DesignMatrixRowPart r, TE.UExpr TE.EInt, TE.UExpr TE.EInt)
                -> TE.UExpr t
                -> TE.StanName
                -> SB.StanBuilderM md gq (TE.UExpr t)
splitToGroupVar (dmrp, se, ie) tse sn = do
  let newVarName = sn <> "_" <> dmrpName dmrp
      splitVarRowsE = TE.functionE TE.size (tse :> TNil)
      segment :: TE.UExpr TE.ECVec -> TE.UExpr TE.ECVec
      segment x = TE.functionE TE.segment (x :> ie :> se :> TNil) --  $ SB.var x :| [SB.name index, namedDimE sizeName]
      block x = TE.functionE TE.block (x :> TE.intE 1 :> ie :> splitVarRowsE :> se :> TNil)

  case TE.genSType @t of
    TE.SCVec -> SB.stanDeclareRHSN (TE.NamedDeclSpec newVarName $ TE.vectorSpec se []) $ segment tse
    TE.SArray sn TE.SCVec -> case testEquality sn (DT.SS @DT.Nat0) of
      Just Refl -> do
        xe :: TE.UExpr (TE.EArray1 TE.ECVec) <- SB.stanDeclareN (TE.NamedDeclSpec newVarName $ TE.array1Spec splitVarRowsE (TE.vectorSpec se []))
        TE.addStmtToCode
          $ TE.for "k" (TE.SpecificNumbered (TE.intE 1) splitVarRowsE)
          $ \ke -> let atk = TE.sliceE TE.s0 ke
                   in [atk xe `TE.assign` segment (atk tse)]
        pure xe
      _ -> SB.stanBuildError "DesignMatrix.splitToGroupVar: Can only split vectors, 1d arrays of vectors, or matrices."
    TE.SMat -> SB.stanDeclareRHSN (TE.NamedDeclSpec newVarName $ TE.matrixSpec splitVarRowsE se []) $ block tse
    _ -> SB.stanBuildError "DesignMatrix.splitToGroupVar: Can only split vectors, 1d arrays of vectors, or matrices."

-- take a Stan vector, array, or matrix indexed by this design row
-- and split into the parts for each group
-- this doesn't depend on r
-- Do we want to
-- 1. Check dimensions? Don't know how from Haskell side.
-- 2. Should the name prefix here come from the design matrix name?
splitToGroupVars :: (TE.IsContainer t, TE.GenSType t)
                 => DesignMatrixRow r -> TE.UExpr t -> Maybe TE.StanName -> SB.StanBuilderM md gq [TE.UExpr t]
splitToGroupVars dmr tse nM =
  let n = fromMaybe (dmName dmr) nM
  in traverse (\(dmrp, k, l) -> splitToGroupVar (dmrp, TE.intE k, TE.intE l) tse n) $ designMatrixIndexes dmr
{-
  let designColName = n <> "_Cols"
  case st of
    SB.StanVector d -> when (d /= SB.NamedDim designColName)
      $ SB.stanBuildError $ "DesignMatrix.splitTogroupVars: vector to split has wrong dimension: " <> show d
    SB.StanArray _ (SB.StanVector d) -> when (d /= SB.NamedDim designColName)
      $ SB.stanBuildError $ "DesignMatrix.splitTogroupVars: vectors in array of vectors to split has wrong dimension: " <> show d
    SB.StanMatrix (d, _)  -> when (d /= SB.NamedDim designColName)
      $ SB.stanBuildError $ "DesignMatrix.splitTogroupVars: matrix to split has wrong row-dimension: " <> show d
  traverse (\(g, _, _) -> splitToGroupVar n g v) $ designMatrixIndexes dmr
-}

data DMParameterization = DMCentered | DMNonCentered deriving (Show, Eq)

{-
addDMParametersAndPriors :: (Typeable md, Typeable gq)
                         => DesignMatrixRow r
                         -> SB.GroupTypeTag k -- exchangeable contexts
                         -> TE.StanName -- name for beta parameter (so we can use theta if QR)
                         -> DMParameterization
                         -> (TE.DensityWithArgs TE.ECVec, TE.DensityWithArgs TE.ECVec, TE.DensityWithArgs TE.ESqMat) -- priors for mu and tau and lkj parameter
                         -> Maybe Text -- suffix for varnames
                         -> SB.StanBuilderM md gq (DAG.ParameterTag TE.ECVec -- alpha
                                                  , DAG.ParameterTag TE.EMat -- beta
                                                  , DAG.ParameterTag TE.ECVec -- mu
                                                  , DAG.ParameterTag TE.ECVec -- tau
                                                  , DAG.ParameterTag TE.ESqMat -- corr
                                                  )
addDMParametersAndPriors dmr gtt betaName parameterization (muPrior, tauPrior, lkjPrior) = do



addDMParametersAndPriors' :: (Typeable md, Typeable gq)
                         => DesignMatrixRow r
                         -> SB.GroupTypeTag k -- exchangeable contexts
                         -> TE.StanName -- name for beta parameter (so we can use theta if QR)
                         -> DMParameterization
                         -> (TE.DensityWithArgs TE.ECVec, TE.DensityWithArgs TE.ECVec, TE.DensityWithArgs TE.ESqMat) -- priors for mu and tau and lkj parameter
                         -> Maybe Text -- suffix for varnames
                         -> SB.StanBuilderM md gq (DAG.ParameterTag TE.ECVec -- alpha
                                                  , DAG.ParameterTag TE.EMat -- beta
                                                  , DAG.ParameterTag TE.ECVec -- mu
                                                  , DAG.ParameterTag TE.ECVec -- tau
                                                  , DAG.ParameterTag TE.ESqMat -- corr
                                                  )
addDMParametersAndPriors' dmr gtt betaName parameterization (muPrior, tauPrior, lkjPrior) = do
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

  (alphaRaw, muAlpha, sigmaAlpha, mu, tau, lCorr, betaRaw) <- do
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
-}
data DMStandardization = DMCenterOnly | DMCenterAndScale

fixSDZeroFunction :: SB.StanBuilderM md gq (TE.Function TE.EReal '[TE.EReal])
fixSDZeroFunction =  do
  let f :: TE.Function TE.EReal '[TE.EReal]
      f = TE.simpleFunction "fixSDZero"
  SB.addFunctionOnce f (TE.Arg "x" :> TNil)
    $ \(x :> TNil) -> TE.writerL $ return $ TE.condE (TE.binaryOpE (TE.SBoolean TE.SEq) x (TE.realE 0)) (TE.realE 1) x

shiftAndScaleDataMatrixFunction :: SB.StanBuilderM md gq (TE.Function TE.EMat '[TE.EMat, TE.ECVec, TE.ECVec])
shiftAndScaleDataMatrixFunction =  do
  let f :: TE.Function TE.EMat '[TE.EMat, TE.ECVec, TE.ECVec]
      f = TE.simpleFunction "shiftAndScaleDataMatrix"
  SB.addFunctionOnce f (TE.DataArg "m" :> TE.DataArg "means" :> TE.DataArg "sds" :> TNil)
    $ \(m :> means :> sds :> TNil) -> TE.writerL $ do
    newMatrix <- TE.declareNW (TE.NamedDeclSpec "shiftedAndScaled" $ TE.matrixSpec (SBB.mRowsE m) (SBB.mColsE m) [])
    TE.addStmt $ TE.for "k" (TE.SpecificNumbered (TE.intE 1) $ SBB.mColsE m)
      $ \ke ->
          let colk :: TE.UExpr q -> TE.UExpr (TE.Sliced TE.N1 q)
              colk = TE.sliceE TE.s1 ke
              atk = TE.sliceE TE.s0 ke
              elDivide = TE.binaryOpE (TE.SElementWise TE.SDivide)
          in [colk newMatrix `TE.assign` ((colk m `TE.minusE` atk means) `TE.divideE` atk sds)]
    return newMatrix


centerDataMatrix :: (Typeable md, Typeable gq)
                 => DMStandardization
                 -> TE.UExpr TE.EMat -- matrix
                 -> Maybe (TE.UExpr TE.ECVec)
                 -> TE.StanName -- prefix for names
                 -> SB.StanBuilderM md gq (TE.UExpr TE.EMat -- standardized matrix, X - row_mean(X) or (X - row_mean(X))/row_stddev(X)
                                          , SB.InputDataType -> TE.UExpr TE.EMat -> TE.StanName -> SB.StanBuilderM md gq (TE.UExpr TE.EMat) -- \Y -> standardized Y (via mean/var of X)
                                          )
centerDataMatrix dms m mwgtsV namePrefix = do
  vecMVF <- case mwgtsV of
    Nothing -> do
      mvF <- SBB.unWeightedMeanVarianceFunction
      let dummyVecE = TE.namedE "dummyVec" TE.SCVec
      return $ \mc -> TE.functionE mvF (dummyVecE :> mc :> TNil)
    Just wgtsV -> do
      mvF <- SBB.weightedMeanVarianceFunction
      return $ \mc -> TE.functionE mvF (wgtsV :> mc :> TNil)
  SB.inBlock SB.SBTransformedData $ do
    fixSDZero <- fixSDZeroFunction
    mVec <- SB.stanDeclareN $ TE.NamedDeclSpec (namePrefix <> "_means") $ TE.vectorSpec (SBB.mColsE m) []
    sVec <- SB.stanDeclareN $ TE.NamedDeclSpec (namePrefix <> "_variances") $ TE.vectorSpec (SBB.mColsE m) []
    SB.addStmtToCode
      $ TE.for "k" (TE.SpecificNumbered (TE.intE 1) $ SBB.mColsE m)
      $ \ke -> TE.writerL' $ do
      let kCol = TE.sliceE TE.s1 ke
          atk = TE.sliceE TE.s0 ke
      mv <- TE.declareRHSNW (TE.NamedDeclSpec "mv" $ TE.vectorSpec (TE.intE 2) []) $ vecMVF (kCol m)
      TE.addStmt $ atk mVec `TE.assign` (TE.sliceE TE.s0 (TE.intE 1) mv)
      TE.addStmt $ atk sVec `TE.assign` TE.functionE fixSDZero (TE.functionE TE.sqrt ((TE.sliceE TE.s0 (TE.intE 2) mv) :> TNil) :> TNil)
    shiftAndScaleF <- shiftAndScaleDataMatrixFunction
    let stdize x = TE.functionE shiftAndScaleF (x :> mVec :> sVec :> TNil)
    mStd <- SB.stanDeclareRHSN (TE.NamedDeclSpec (namePrefix <> "_standardized") $ TE.matrixSpec (SBB.mRowsE m) (SBB.mColsE m) [])
            $ stdize m
    let centerF idt m' n = do
          let block = if idt == SB.ModelData then SB.SBTransformedData else SB.SBTransformedDataGQ
          SB.inBlock block $ SB.stanDeclareRHSN (TE.NamedDeclSpec n $ TE.matrixSpec (SBB.mRowsE m') (SBB.mColsE m') []) $ stdize m'
    return (mStd, centerF)
{-
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
  (centeredXV, meanXV, sdXV) <- SB.inBlock SB.SBTransformedData $ do
    fixSDZeroFunction
    meanVarFunction <- case mwgtsV of
      Nothing -> do
        SBB.unWeightedMeanVarianceFunction
        return $ SB.function "unweighted_mean_variance" (one $ SB.vectorizedOne rowIndexKey (SB.var mV))
      Just wgtsV -> do
        SBB.weightedMeanVarianceFunction
        let vectorizedWgts = SB.function "to_vector" (one $ SB.vectorizedOne rowIndexKey (SB.var wgtsV))
        return $ SB.function "weighted_mean_variance" (vectorizedWgts :| [SB.vectorizedOne rowIndexKey (SB.var mV)])
    meanXV' <- SB.stanDeclareRHS ("mean_" <> mn) (SB.StanVector $ SB.NamedDim colIndexKey) ""
      $ SB.function "rep_vector"  (SB.scalar "0" :| [SB.indexSize colIndexKey])
    sdXV' <- SB.stanDeclareRHS ("sd_" <> mn) (SB.StanVector $ SB.NamedDim colIndexKey) ""
      $ SB.function "rep_vector"  (SB.scalar "0" :| [SB.indexSize colIndexKey])
    stdXV' <- SB.stanDeclare ("centered_" <> mn) mt ""
    SB.stanForLoopB "k" Nothing colIndexKey $ do
      meanVar <- SB.stanDeclareRHS "meanVar" (SB.StanVector $ SB.GivenDim 2) "" $ meanVarFunction
      SB.addExprLine "centerDataMatrix" $ SB.var meanXV' `SB.eq` SB.withIndexes (SB.varNameE meanVar) (one $ SB.GivenDim 1)
      SB.addExprLine "centerDataMatrix"
        $ SB.var sdXV' `SB.eq` SB.function "fixSDZero" (one $ SB.function "sqrt" (one $ SB.withIndexes (SB.varNameE meanVar) (one $ SB.GivenDim 2)))
      SB.addExprLine "centerDataMatrix"
        $ SB.vectorizedOne rowIndexKey
        $ case dms of
            DMCenterOnly -> SB.var stdXV' `SB.eq` (SB.var mV `SB.minus` SB.var meanXV')
            DMCenterAndScale -> SB.var stdXV' `SB.eq` SB.paren (SB.var mV `SB.minus` SB.var meanXV') `SB.divide` SB.var sdXV'
    SBB.printVar "" meanXV'
    SBB.printVar "" sdXV'
    pure (stdXV', meanXV', sdXV')
  let stdX idt mv@(SB.StanVar mn mt) mSuffix = do
        let codeBlock = if idt == SB.ModelData then SB.SBTransformedData else SB.SBTransformedDataGQ
        case mt of
          cmt@(SB.StanMatrix (SB.NamedDim rKey, SB.NamedDim cKey)) -> SB.inBlock codeBlock $ do
            cv <- SB.stanDeclare ("centered_" <> mn <> fromMaybe "" mSuffix) cmt ""
            SB.stanForLoopB "k" Nothing cKey $ do
              SB.addExprLine "centerData.Matrix.centeredX"
                $ SB.vectorizedOne rKey
                $ case dms of
                    DMCenterOnly -> SB.var cv `SB.eq` (SB.var mv `SB.minus` SB.var meanXV)
                    DMCenterAndScale -> SB.var cv `SB.eq` SB.paren (SB.var mv `SB.minus` SB.var meanXV) `SB.divide` SB.var sdXV
            return cv
          _ -> SB.stanBuildError
               $ "centeredX (from DesignMatrix.centerDataMatrix called with x="
               <> show mt
               <> "): called with mv="
               <> show mv
               <> " which is not a matrix type with indexed row and column dimension."
  pure (centeredXV, stdX)
-}
-- take a matrix x and return (thin) Q, R and inv(R)
-- as Q_x, R_x, invR_x
-- see https://mc-stan.org/docs/2_28/stan-users-guide/QR-reparameterization.html
thinQR :: forall t md gq.(TE.GenSType t, TE.TypeOneOf t [TE.ECVec, TE.EMat, TE.EArray (TE.S TE.Z) TE.ECVec])
       => TE.MatrixE -- matrix of predictors
       -> TE.StanName -- names prefix
       -> Maybe (TE.UExpr t, TE.NamedDeclSpec t) -- theta and name for beta
       -> SB.StanBuilderM md gq (TE.UExpr TE.EMat, TE.UExpr TE.EMat, TE.UExpr TE.EMat, Maybe (TE.UExpr t))
thinQR xE xName mThetaBeta = do
  (q, r, rI) <- SB.inBlock SB.SBTransformedData $ do
    qE  <- SB.stanDeclareRHSN (TE.NamedDeclSpec ("Q_" <> xName) $ TE.matrixSpec (SBB.mRowsE xE) (SBB.mColsE xE) [])
           $ TE.functionE TE.qr_thin_Q (xE :> TNil) `TE.divideE` TE.functionE TE.sqrt (SBB.mRowsE xE `TE.minusE` TE.realE 1 :> TNil)
    rE  <- SB.stanDeclareRHSN (TE.NamedDeclSpec ("R_" <> xName) $ TE.matrixSpec (SBB.mColsE xE) (SBB.mColsE xE) [])
           $ TE.functionE TE.qr_thin_R (xE :> TNil) `TE.divideE` TE.functionE TE.sqrt (SBB.mRowsE xE `TE.minusE` TE.realE 1 :> TNil)
    rInvE <- SB.stanDeclareRHSN (TE.NamedDeclSpec ("invR_" <> xName) $ TE.matrixSpec (SBB.mColsE xE) (SBB.mColsE xE) []) $ TE.functionE TE.inverse (rE :> TNil)
    return (qE, rE, rInvE)
  mBeta <-  SB.inBlock SB.SBGeneratedQuantities $ case mThetaBeta of
    Nothing -> return Nothing
    Just (theta, betaNDS) -> fmap Just $ case TE.genSType @t of
      TE.SMat ->
           SB.stanDeclareRHSN betaNDS $ rI `TE.timesE` theta
      TE.SCVec ->
           SB.stanDeclareRHSN betaNDS $ rI `TE.timesE` theta
      TE.SArray sn TE.SCVec -> case testEquality sn (DT.SS @DT.Nat0) of
        Just Refl -> do
          let arrSizeE = TE.functionE TE.size (theta :> TNil)
              vecSize = TE.functionE TE.rows (rI :> TNil)
          TE.addFromCodeWriter (do
                                   beta <- TE.declareNW betaNDS
                                   TE.addStmt $ TE.for "j" (TE.SpecificNumbered (TE.intE 0) arrSizeE)
                                           $ \j -> let atj = TE.sliceE TE.s0 j in [atj beta `TE.assign` (rI `TE.timesE` atj theta)]
                                   return beta
                               )
        Nothing -> SB.stanBuildError $ "DesignMatrix.thinQR array of dimension other than 1 given for theta."
      _ -> SB.stanBuildError $ "DesignMatrix.thinQR: bad type given for theta: type=" <> show (TE.genSType @t)
  return (q, r, rI, mBeta)

{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}
{-# HLINT ignore "Use for_" #-}
{-# HLINT ignore "Use camelCase" #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE QuantifiedConstraints #-}

module Stan.ModelBuilder.BuildingBlocks where


import qualified Stan.ModelBuilder.TypedExpressions.Types as TE
import qualified Stan.ModelBuilder.TypedExpressions.TypedList as TE
import Stan.ModelBuilder.TypedExpressions.TypedList (TypedList(..))
import Stan.ModelBuilder.TypedExpressions.Types (Nat(..))
import qualified Stan.ModelBuilder.TypedExpressions.Statements as TE
import qualified Stan.ModelBuilder.TypedExpressions.Indexing as TE
import qualified Stan.ModelBuilder.TypedExpressions.Operations as TE
import qualified Stan.ModelBuilder.TypedExpressions.StanFunctions as TE
import qualified Stan.ModelBuilder as SB
import qualified Stan.ModelBuilder.Expressions as SME
import qualified Stan.ModelBuilder.Distributions as SMD

import Prelude hiding (sum, All)
import qualified Data.List.NonEmpty as NE
import qualified Data.Dependent.HashMap as DHash
import qualified Data.Dependent.Sum as DSum
import qualified Data.Map as Map
import qualified Data.Set as Set
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Stan.ModelConfig as SB
import Stan.ModelBuilder.BuilderTypes (dataSetSizeName)
import qualified Stan.ModelBuilder.TypedExpressions.Types as TE
import qualified Stan.ModelBuilder.TypedExpressions.Operations as TE
import Stan.ModelBuilder (addFromCodeWriter)
import qualified Stan.ModelBuilder.TypedExpressions.Expressions as TE

{-
namedVectorIndex :: SB.StanVar -> SB.StanBuilderM md gq SB.IndexKey
namedVectorIndex x = case SME.varType x of
  SME.StanVector (SME.NamedDim ik) -> return ik
  _ -> SB.stanBuildError $ "namedVectorIndex: bad type=" <> show x

named1dArrayIndex :: SB.StanVar -> SB.StanBuilderM md gq SB.IndexKey
named1dArrayIndex x = case SME.varType x of
  SME.StanArray [SME.NamedDim ik] _ -> return ik
  _ -> SB.stanBuildError $ "named1dArrayIndex: bad type=" <> show x

namedMatrixRowIndex :: SB.StanVar -> SB.StanBuilderM md gq SB.IndexKey
namedMatrixRowIndex x = case SME.varType x of
  SME.StanMatrix (SME.NamedDim ik, _) -> return ik
  _ -> SB.stanBuildError $ "namedMatrixRowIndex: bad type=" <> show x

namedMatrixColIndex :: SB.StanVar -> SB.StanBuilderM md gq SB.IndexKey
namedMatrixColIndex x = case SME.varType x of
  SME.StanMatrix (_, SME.NamedDim ik) -> return ik
  _ -> SB.stanBuildError $ "namedMatrixColIndex: bad type=" <> show x
-}

addIntData :: (Typeable md, Typeable gq)
            => SB.RowTypeTag r
            -> TE.StanName
            -> Maybe Int
            -> Maybe Int
            -> (r -> Int)
            -> SB.StanBuilderM md gq TE.IntArrayE
addIntData rtt varName mLower mUpper f = do
  let cs = maybe [] (pure . TE.lowerM . TE.intE) mLower ++ maybe [] (pure . TE.upperM . TE.intE) mUpper
      ndsF lE = TE.NamedDeclSpec varName $ TE.intArraySpec lE cs
  SB.addColumnJson rtt ndsF f

addCountData :: forall r md gq.(Typeable md, Typeable gq)
             => SB.RowTypeTag r
             -> TE.StanName
             -> (r -> Int)
             -> SB.StanBuilderM md gq TE.IntArrayE
addCountData rtt varName f = addIntData rtt varName (Just 0) Nothing f

addRealData :: (Typeable md, Typeable gq)
            => SB.RowTypeTag r
            -> TE.StanName
            -> Maybe Double
            -> Maybe Double
            -> (r -> Double)
            -> SB.StanBuilderM md gq  TE.VectorE
addRealData rtt varName mLower mUpper f = do
  let cs = maybe [] (pure . TE.lowerM. TE.realE) mLower ++ maybe [] (pure . TE.upperM . TE.realE) mUpper
      ndsF lE = TE.NamedDeclSpec varName $ TE.vectorSpec lE cs
  SB.addColumnJson rtt ndsF f

add2dMatrixData :: (Typeable md, Typeable gq)
                => SB.RowTypeTag r
                -> SB.MatrixRowFromData r
                -> Maybe Double
                -> Maybe Double
            -> SB.StanBuilderM md gq TE.MatrixE
add2dMatrixData rtt matrixRowFromData mLower mUpper = do
  let cs = maybe [] (pure . TE.lowerM . TE.realE) mLower ++ maybe [] (pure . TE.upperM . TE.realE) mUpper
  SB.add2dMatrixJson rtt matrixRowFromData cs -- (SB.NamedDim $ SB.dataSetName rtt)  --stanType bounds f

sampleDistV :: SMD.StanDist t args -> TE.ExprList args -> TE.UExpr t -> SB.StanBuilderM md gq ()
sampleDistV sDist args yV =  SB.inBlock SB.SBModel $ SB.addStmtToCode $ SMD.familySample sDist yV args

printExpr :: Text -> TE.UExpr t -> SB.StanBuilderM md gq ()
printExpr t e = SB.addStmtToCode $ TE.print (TE.stringE ("\"" <> t <> "\"=") :> e :> TNil)

printTarget :: Text -> SB.StanBuilderM md gq ()
printTarget t = printExpr "target" (TE.functionE TE.targetVal TNil)

{-
parallelSampleDistV :: (Typeable md, Typeable gq) => Text -> SB.RowTypeTag r -> SMD.StanDist args -> args -> SB.StanVar -> SB.StanBuilderM md gq ()
parallelSampleDistV fPrefix rtt sDist args slicedVar@(SB.StanVar slicedName slicedType) = do
--  let rsExpr = SB.target `SB.plusEq` SB.function "reduce_sum"
  slicedType' <- SB.stanBuildEither $ SB.dropLastIndex slicedType
  let sliceIndex = "sliceIndex"
      dsName = SB.dataSetName rtt
      sliceName = slicedName <> "_slice"
      sVarDist = SB.StanVar sliceName slicedType'
      sVarArg = SB.StanVar sliceName slicedType
      samplingE = SMD.familyLUDF sDist args sVarDist
      fSuffix = if SB.distType sDist == SB.Discrete then "lpmf" else "lpdf"
      fuSuffix = if SB.distType sDist == SB.Discrete then "lupmf" else "lupdf"
      fNameDecl = "partial_sum" <> "_" <> fPrefix <> "_" <> fSuffix
      fNameUse = "partial_sum" <> "_" <> fPrefix <> "_" <> fuSuffix
  fnArgs' <- SB.exprVarsM $ SME.vectorizedOne dsName $ samplingE
  let fnArgs = Set.toList $ Set.delete sVarDist fnArgs' -- slicedVar is handled separately
--  SB.stanBuildError $ "HERE: \n" <> show fnArgs <> "\n" <> SB.prettyPrintSTree samplingE
  SB.addFixedIntJson' SB.ModelData "grainsize" Nothing 1
--  slicedType' <- SB.stanBuildEither $ SB.dropOuterIndex slicedType
  SB.addFunctionsOnce fNameDecl $ do
    let argList = sVarArg :|
                  [ SB.StanVar "start" SB.StanInt
                  , SB.StanVar "end" SB.StanInt] ++
                  fnArgs
        fnArgsExpr = SB.csExprs $ SB.varAsArgument <$> argList
    fnArgsExprT <- SB.stanBuildEither $  SB.printExpr SB.noBindings fnArgsExpr
    SB.declareStanFunction ("real " <> fNameDecl <> "(" <> fnArgsExprT <> ")") $ do
      SB.indexBindingScope $ do -- only add slice for index in this scope
        SB.addUseBinding' dsName $ SB.indexBy (SB.bare "start:end") sliceIndex -- index data-set with slice
        SB.addExprLine "parallelSampleDistV" $ SB.fReturn $ SB.vectorized (one sliceIndex) $ samplingE
  SB.inBlock SB.SBModel $ do
    let varName (SB.StanVar n _) = SB.name n
        argList = SB.bare fNameUse :|  [SB.name slicedName, SB.name "grainsize"] ++ (varName <$> fnArgs)
    SB.addExprLine "parallelSampleDistV" $ SB.target `SB.plusEq` SB.function "reduce_sum" argList
-}
generateLogLikelihood :: SB.RowTypeTag r
                      -> SMD.StanDist t pts
                      -> TE.CodeWriter (TE.IntE -> TE.ExprList pts)
                      -> (TE.IntE -> TE.UExpr t)
                      -> SB.StanBuilderM md gq ()
generateLogLikelihood rtt sDist slicedArgsFCW slicedYF =
  generateLogLikelihood' $ addToLLSet rtt (LLDetails sDist slicedArgsFCW slicedYF) emptyLLSet

-- 2nd arg returns something which might need slicing at the loop index for paramters that depend on the index
-- 3rd arg also
data LLDetails r = forall t pts.LLDetails (SMD.StanDist t pts) (TE.CodeWriter (TE.UExpr TE.EInt -> TE.ExprList pts)) (TE.UExpr TE.EInt -> TE.UExpr t)
--  LLDetails :: forall args.SMD.StanDist args -> SB.StanBuilderM md gq args -> SME.StanVar -> LLDetails md gq r

newtype LLDetailsList r = LLDetailsList [LLDetails r]

addDetailsLists :: LLDetailsList r -> LLDetailsList r -> LLDetailsList r
addDetailsLists (LLDetailsList x) (LLDetailsList y) = LLDetailsList (x <> y)

type LLSet = DHash.DHashMap SB.RowTypeTag LLDetailsList

emptyLLSet :: LLSet
emptyLLSet = DHash.empty

addToLLSet :: SB.RowTypeTag r -> LLDetails r -> LLSet  -> LLSet
addToLLSet rtt d llSet = DHash.insertWith addDetailsLists rtt (LLDetailsList [d]) llSet

mergeLLSets ::  LLSet  -> LLSet -> LLSet
mergeLLSets = DHash.unionWith addDetailsLists

-- we return RowTypeTag from doOne so that DHash traversal can infer types, I think.
generateLogLikelihood' :: LLSet -> SB.StanBuilderM md gq ()
generateLogLikelihood' llSet =  SB.inBlock SB.SBLogLikelihood $ do
  let prependSizeName rtt (LLDetailsList ds) ls = Prelude.replicate (Prelude.length ds) (SB.dataSetSizeName rtt) ++ ls
  llSizeListNE <- case nonEmpty (DHash.foldrWithKey prependSizeName [] llSet) of
    Nothing -> SB.stanBuildError "generateLogLikelihood': empty set of log-likelihood details given"
    Just x -> return x
  let namedIntE n = TE.namedE n TE.SInt
      llSizeE = TE.multiOpE TE.SAdd $ fmap namedIntE llSizeListNE
  logLikE <- SB.stanDeclareN $ TE.NamedDeclSpec "log_lik" $ TE.vectorSpec llSizeE []
  let doOne :: SB.RowTypeTag a -> LLDetails a -> StateT [TE.UExpr TE.EInt] (SB.StanBuilderM md gq) (SB.RowTypeTag a)
      doOne rtt (LLDetails d pFCW yF) = do
        prevSizes <- get
        let sizeE =  TE.multiOpE TE.SAdd $ namedIntE "n" :| prevSizes
        lift $ SB.addFromCodeWriter $ do
          pF <- pFCW
          TE.addStmt $ TE.for "n" (TE.SpecificNumbered (TE.intE 1) (namedIntE $ dataSetSizeName rtt))
            $ \nE -> [TE.sliceE TE.s0 nE logLikE `TE.assign` SMD.familyLDF d (yF nE) (pF nE)]
        put $ TE.namedSizeE (SB.dataSetSizeName rtt) : prevSizes
        pure rtt
      doList ::  SB.RowTypeTag a -> LLDetailsList a -> StateT [TE.UExpr TE.EInt] (SB.StanBuilderM md gq) (SB.RowTypeTag a)
      doList rtt (LLDetailsList lls) = traverse_ (doOne rtt) lls >> pure rtt
  _ <- evalStateT (DHash.traverseWithKey doList llSet) []
  pure ()

generatePosteriorPrediction :: SB.RowTypeTag r
                            -> TE.NamedDeclSpec (TE.EArray1 t)
                            -> SMD.StanDist t pts
                            -> TE.CodeWriter (TE.IntE -> TE.ExprList pts)
                            -> SB.StanBuilderM md gq (TE.ArrayE t)
generatePosteriorPrediction rtt nds sDist psFCW = generatePosteriorPrediction' rtt nds sDist psFCW id

generatePosteriorPrediction' :: SB.RowTypeTag r
                             -> TE.NamedDeclSpec (TE.EArray1 t)
                             -> SMD.StanDist t pts
                             -> TE.CodeWriter (TE.IntE -> TE.ExprList pts)
                             -> (TE.UExpr t -> TE.UExpr t)
                             -> SB.StanBuilderM md gq (TE.ArrayE t)
generatePosteriorPrediction' rtt nds sDist psFCW f = SB.inBlock SB.SBGeneratedQuantities $ do
  psF <- SB.addFromCodeWriter psFCW
  let rngE nE = SMD.familyRNG sDist (psF nE)
  ppE <- SB.stanDeclareN nds
  SB.addStmtToCode
    $ TE.for "n" (TE.SpecificNumbered (TE.intE 1) (TE.namedSizeE $ SB.dataSetSizeName rtt))
    $ \nE -> [TE.sliceE TE.s0 nE ppE `TE.assign` f (rngE nE)]
  return ppE

generatePosteriorPredictionV :: SB.RowTypeTag r
                             -> TE.NamedDeclSpec TE.ECVec
                             -> SMD.StanDist TE.ECVec pts
                             -> TE.ExprList pts
                             -> SB.StanBuilderM md gq TE.VectorE
generatePosteriorPredictionV rtt nds sDist ps =
  SB.inBlock SB.SBGeneratedQuantities
  $ SB.stanDeclareRHSN nds $ SMD.familyRNG sDist ps

diagVectorFunction :: SB.StanBuilderM md gq (TE.Function TE.ECVec '[TE.EArray1 TE.ECVec, TE.EInt])
diagVectorFunction = do
  let f :: TE.Function TE.ECVec '[TE.EArray1 TE.ECVec, TE.EInt]
      f = TE.simpleFunction "index_both"
      dsF :: TE.ExprList [TE.EArray1 TE.ECVec, TE.EInt] -> TE.DeclSpec TE.ECVec
      dsF (_ :> n :> TNil) = TE.vectorSpec n []
  SB.addFunctionOnce f (TE.Arg "vs" :> TE.Arg "N" :> TNil)
    $ TE.simpleFunctionBody f "out_vec" dsF
    $ \rvE (vs :> n :> TNil) ->
        [TE.for "n" (TE.SpecificNumbered (TE.intE 1) n) $ \nE ->
            let slice = TE.sliceE TE.s0 nE in [slice rvE `TE.assign` slice (TE.sliceE TE.s0 nE vs)]
        ]
  return f

vectorizeExpr :: TE.IntE -> TE.StanName -> (TE.IntE -> TE.RealE) -> TE.CodeWriter TE.VectorE
vectorizeExpr lE sn se = head <$> vectorizeExprT lE ((sn, se) :| [])

-- like vectorizeExpr but for multiple things in same loop
vectorizeExprT :: Traversable t
               => TE.IntE -> t (TE.StanName, TE.IntE -> TE.RealE) -> TE.CodeWriter (t (TE.UExpr TE.ECVec))
vectorizeExprT lengthE namedSrcs = do
  let vecVname sn = sn <> "_v"
      nds sn = TE.NamedDeclSpec (vecVname sn) $ TE.vectorSpec lengthE []
      declareVec (sn, ve) = do
        fe <- TE.declareNW $ nds sn
        return (fe, ve)
      fillVec ne (ve, se) = TE.sliceE TE.s0 ne ve `TE.assign` se ne
  varExps <- traverse declareVec namedSrcs
  TE.addStmt $ TE.for "n" (TE.SpecificNumbered (TE.intE 1) lengthE) $ \nE -> fmap (fillVec nE) varExps
  return $ fst <$> varExps

sum :: TE.UExpr TE.ECVec -> TE.UExpr TE.EReal
sum x = TE.functionE TE.sum (x :> TNil)

weightedMeanFunction :: SB.StanBuilderM md gq (TE.Function TE.EReal [TE.ECVec, TE.ECVec])
weightedMeanFunction = do
  let f :: TE.Function TE.EReal [TE.ECVec, TE.ECVec]
      f = TE.simpleFunction "weighted_mean"
  SB.addFunctionOnce f (TE.Arg "ws" :> TE.Arg "xs" :> TNil)
    $ \(ws :> xs :> TNil)  -> TE.writerL $ do
    wgtdXs <- TE.declareRHSNW (TE.NamedDeclSpec "wgtdXs" $ TE.vectorSpec (TE.functionE TE.size (xs :> TNil)) [])
              $ TE.binaryOpE (TE.SElementWise TE.SMultiply) ws xs
    return $ sum wgtdXs `TE.divideE` sum ws

weightedMeanVarianceFunction :: SB.StanBuilderM md gq (TE.Function TE.ECVec [TE.ECVec, TE.ECVec])
weightedMeanVarianceFunction = do
  let f :: TE.Function TE.ECVec [TE.ECVec, TE.ECVec]
      f = TE.simpleFunction "weighted_mean_variance"
      eTimes = TE.binaryOpE (TE.SElementWise TE.SMultiply)
  SB.addFunctionOnce f (TE.Arg "ws" :> TE.Arg "xs" :> TNil)
    $ \(ws :> xs :> TNil) -> TE.writerL $ do
    n <- TE.declareRHSW "N" (TE.intSpec []) $ TE.functionE TE.size (xs :> TNil)
    wgtdXs <- TE.declareRHSW "wgtdXs" (TE.vectorSpec n []) $ ws `eTimes` xs
    mv <- TE.declareW "meanVar" (TE.vectorSpec (TE.intE 2) [])
    let meanVar i = TE.sliceE TE.s0 (TE.intE i) mv
    TE.addStmt $ meanVar 1 `TE.assign` (sum wgtdXs `TE.divideE` sum ws)
    y <- TE.declareRHSW "y" (TE.vectorSpec n []) $ xs `TE.minusE` meanVar 1
    TE.addStmt $ meanVar 2 `TE.assign` (sum (ws `eTimes` y `eTimes` y) `TE.divideE` sum ws)
    return mv
{-
  SB.addFunctionsOnce "weighted_mean_variance"
                        $ SB.declareStanFunction "vector weighted_mean_variance(vector ws, vector xs)" $ do
  SB.addStanLine "int N = num_elements(xs)"
  SB.addStanLine "vector[N] wgtdXs = ws .* xs"
  SB.addStanLine "vector[2] meanVar"
  SB.addStanLine "meanVar[1] = sum(wgtdXs)/sum(ws)"
  SB.addStanLine "vector[N] y = (xs - meanVar[1])"
  SB.addStanLine "meanVar[2] = sum(ws .* y .* y)/sum(ws)"
  SB.addStanLine "return meanVar"
-}

unWeightedMeanVarianceFunction :: SB.StanBuilderM md gq (TE.Function TE.ECVec [TE.ECVec, TE.ECVec])
unWeightedMeanVarianceFunction = do
  let f :: TE.Function TE.ECVec [TE.ECVec, TE.ECVec]
      f = TE.simpleFunction "unweighted_mean_variance"
  SB.addFunctionOnce f (TE.Arg "ws" :> TE.Arg "xs" :> TNil)
    $ \(_ :> xs :> TNil) -> TE.writerL $ do
    n <- TE.declareRHSW "N" (TE.intSpec []) $ TE.functionE TE.size (xs :> TNil)
    mv <- TE.declareW "meanVar" (TE.vectorSpec (TE.intE 2) [])
    let meanVar i = TE.sliceE TE.s0 (TE.intE i) mv
    TE.addStmt $ meanVar 1 `TE.assign` TE.functionE TE.mean (xs :> TNil)
    TE.addStmt $ meanVar 2 `TE.assign` TE.functionE TE.variance (xs :> TNil)
    return mv
{-
unWeightedMeanVarianceFunction :: SB.StanBuilderM md gq ()
unWeightedMeanVarianceFunction =  SB.addFunctionsOnce "unweighted_mean_variance"
                        $ SB.declareStanFunction "vector unweighted_mean_variance(vector xs)" $ do
  SB.addStanLine "int N = num_elements(xs)"
  SB.addStanLine "vector[2] meanVar"
  SB.addStanLine "meanVar[1] = mean(xs)"
  SB.addStanLine "meanVar[2] = variance(xs)"
  SB.addStanLine "return meanVar"
-}


realIntRatio :: TE.UExpr TE.EInt -> TE.UExpr TE.EInt -> TE.UExpr TE.EReal
realIntRatio k l = let f x = (TE.realE 1 `TE.timesE` x) in f k `TE.divideE` f l

declTranspose :: TE.TypeOneOf t [TE.EMat, TE.ESqMat, TE.ECVec, TE.ERVec]
              => TE.NamedDeclSpec (TE.UnaryResultT TE.UTranspose t)
              -> TE.UExpr t
              -> SB.StanBuilderM md gq (TE.UExpr (TE.UnaryResultT TE.UTranspose t))
declTranspose nds m = do
  SB.stanDeclareRHSN nds $ TE.unaryOpE TE.STranspose m

indexedConstIntArray :: SB.RowTypeTag r -> Maybe Text -> TE.UExpr TE.EInt -> TE.UExpr TE.EInt -> SB.StanBuilderM md gq TE.IntArrayE
indexedConstIntArray rtt mSuffix lengthE nE =
  let dsName = SB.dataSetName rtt
      sizeName = SB.dataSetSizeName rtt
      nds = TE.NamedDeclSpec ("constIndex_" <> dsName <> maybe "" ("_" <>) mSuffix) $ TE.intArraySpec lengthE []
  in SB.inBlock SB.SBTransformedData
     $ SB.stanDeclareRHSN nds $ TE.functionE TE.rep_array (nE :> lengthE :> TNil)

zeroVectorE :: TE.IntE -> TE.VectorE
zeroVectorE lengthE = TE.functionE TE.rep_vector (TE.realE 0 :> lengthE :> TNil)

zeroMatrixE :: TE.IntE -> TE.IntE -> TE.MatrixE
zeroMatrixE rowsE colsE = TE.functionE TE.rep_matrix (TE.realE 0 :> rowsE :> colsE :> TNil)

ps_by_group :: TE.Function TE.ECVec [TE.EInt, TE.EInt, TE.EIndexArray, TE.ECVec, TE.ECVec]
ps_by_group = TE.simpleFunction "ps_by_group"

psByGroupFunction :: SB.StanBuilderM md gq (TE.Function TE.ECVec [TE.EInt, TE.EInt, TE.EIndexArray, TE.ECVec, TE.ECVec])
psByGroupFunction = do
  SB.addFunctionOnce ps_by_group (TE.DataArg "Nps" :> TE.DataArg "Ngrp" :> TE.DataArg "grpPSIndex" :> TE.DataArg "wgts" :> TE.Arg "probs" :> TNil)
    $ \(nPS :> nGrp :> grpPSIndex :> wgts :> probs :> TNil) -> TE.writerL $ do
    let vds = TE.vectorSpec nGrp []
        plusEq = TE.opAssign TE.SAdd
        elDivEq = TE.opAssign (TE.SElementWise TE.SDivide)
    sumByGroup <- TE.declareRHSW "SumByGroup" vds $ zeroVectorE nGrp
    sumWgts <- TE.declareRHSW "SumByGroup" vds $ zeroVectorE nGrp
    TE.addStmt $ TE.for "k" (TE.SpecificNumbered (TE.intE 1) nPS)
      $ \k ->
          let atk = TE.sliceE TE.s0 k
              indexByPS = TE.indexE TE.s0 grpPSIndex
          in [atk (indexByPS sumByGroup) `plusEq` (atk wgts `TE.timesE` atk (indexByPS probs))
             , atk (indexByPS sumWgts) `plusEq` atk wgts
             ]
    TE.addStmt $ sumByGroup `elDivEq` sumWgts
    return sumByGroup

{-
psByGroupFunction :: SB.StanBuilderM md gq ()
psByGroupFunction = SB.addFunctionsOnce "psByGroup"
                      $ SB.declareStanFunction "vector psByGroup(int Nps, int Ngrp, array[] int grpPSIndex, vector wgts, vector probs)" $ do
  SB.addStanLine "vector[Ngrp] SumByGroup = rep_vector(0, Ngrp)"
  SB.addStanLine "vector[Ngrp] SumWgts = rep_vector(0, Ngrp)"
  SB.addLine "for (k in 1:Nps) {\n"
  SB.addStanLine "  SumByGroup[grpPSIndex[k]] += wgts[k] * probs[grpPSIndex[k]]"
  SB.addStanLine "  SumWgts[grpPSIndex[k]] += wgts[k]"
  SB.addLine "}\n"
  SB.addStanLine "SumByGroup ./= SumWgts"
  SB.addStanLine "return SumByGroup"
-}

postStratifiedParameterF :: (Typeable md, Typeable gq)
                         => Bool
                         -> SB.StanBlock
                         -> Maybe Text
                         -> SB.RowTypeTag r -- data set to post-stratify
                         -> SB.GroupTypeTag k -- group by
                         -> TE.UExpr TE.EIndexArray -- PS Index for group
                         -> TE.UExpr TE.ECVec -- PS weight
                         -> TE.UExpr TE.ECVec --  expression of parameters to post-stratify
                         -> Maybe (SB.RowTypeTag r', TE.UExpr TE.EIndexArray) -- re-index?
                         -> SB.StanBuilderM md gq (TE.UExpr TE.ECVec)
postStratifiedParameterF prof block varNameM rtt gtt grpIndex wgtsV pV reIndexRttM = do
  psByGroupFunction
  let dsName = SB.dataSetName rtt
      gName = SB.taggedGroupName gtt
      dsSizeE = TE.namedSizeE $ SB.dataSetSizeName rtt
      grpSizeE = TE.namedSizeE $ SB.groupSizeName gtt
      psDataByGroupName = dsName <> "_By_" <> gName
      indexName = SB.dataSetName rtt <> "_" <> SB.taggedGroupName gtt
      varName = case reIndexRttM of
        Nothing -> fromMaybe psDataByGroupName varNameM
        Just (reIndexRtt, _) -> fromMaybe (dsName <> "_By_" <> SB.dataSetName reIndexRtt) varNameM
      grpVecDS =  TE.vectorSpec grpSizeE [] --SB.StanVector $ SB.NamedDim gName
      psVecDS =  TE.vectorSpec dsSizeE [] --SB.StanVector $ SB.NamedDim dsName
      scopeF :: [TE.UStmt] -> TE.UStmt
      scopeF stmts = if prof then TE.profile varName stmts else TE.scoped stmts
  SB.inBlock block $ case reIndexRttM of
    Nothing -> do
      probV <- SB.stanDeclare varName grpVecDS
      SB.addStmtToCode $ scopeF
        [probV `TE.assign` TE.functionE ps_by_group (dsSizeE :>  grpSizeE :> grpIndex :> wgtsV :> pV :> TNil)]
      pure probV
    Just (reIndexRtt, reIndex) -> do
      riProb <-  SB.stanDeclare varName $ TE.vectorSpec (TE.namedSizeE $ SB.dataSetSizeName reIndexRtt) [] --(SB.StanVector $ SB.NamedDim reIndexKey) ""
      SB.addStmtToCode $ scopeF $ TE.writerL' $ do
        gProb <- TE.declareRHSW psDataByGroupName grpVecDS $ TE.functionE ps_by_group (dsSizeE :>  grpSizeE :> grpIndex :> wgtsV :> pV :> TNil)
        TE.addStmt $ riProb `TE.assign` TE.indexE TE.s0 reIndex gProb
      pure riProb

mRowsE :: (TE.TypeOneOf t [TE.EMat, TE.ESqMat], TE.GenSType t) => TE.UExpr t -> TE.IntE
mRowsE m = TE.functionE TE.rows (m :> TNil)

mColsE :: (TE.TypeOneOf t [TE.EMat, TE.ESqMat], TE.GenSType t) => TE.UExpr t -> TE.IntE
mColsE m = TE.functionE TE.cols (m :> TNil)

vecLengthE :: (TE.TypeOneOf t [TE.ECVec, TE.ERVec], TE.GenSType t, TE.IsContainer t)
           => TE.UExpr t -> TE.IntE
vecLengthE v = TE.functionE TE.size (v :> TNil)

arr1LengthE :: TE.GenSType t => TE.UExpr (TE.EArray1 t) -> TE.IntE
arr1LengthE v = TE.functionE TE.size (v :> TNil)


reIndex :: TE.IntArrayE -> (TE.IntE -> TE.UExpr t) -> TE.IntE -> TE.UExpr t
reIndex ia eF ke = eF $ TE.sliceE TE.s0 ke ia




{-
postStratifiedParameter :: (Typeable md, Typeable gq)
                        => Bool
                        -> Maybe Text
                        -> SB.RowTypeTag r -- data set to post-stratify
                        -> SB.GroupTypeTag k -- group by
                        -> SB.StanExpr -- weight
                        -> SB.StanExpr -- expression of parameters to post-stratify
                        -> Maybe (SB.RowTypeTag r') -- re-index?
                        -> SB.StanBuilderM md gq SB.StanVar
postStratifiedParameter prof varNameM rtt gtt wgtE pE reIndexRttM = do
  let dsName = SB.dataSetName rtt
      gName = SB.taggedGroupName gtt
      psDataByGroupName = dsName <> "_By_" <> gName
      varName = case reIndexRttM of
        Nothing -> fromMaybe psDataByGroupName varNameM
        Just reIndexRtt -> fromMaybe (SB.dataSetName rtt <> "_By_" <> SB.dataSetName reIndexRtt) varNameM
      profF :: SB.StanBuilderM md gq a -> SB.StanBuilderM md gq a
      profF = if prof then SB.profile varName else SB.bracketed 2
      zeroVec indexKey varName = SB.stanDeclareRHS
                                 varName
                                 (SB.StanVector $ SB.NamedDim indexKey)
                                 ""
                                 (zeroVectorE indexKey)
      psLoops pV wV = do
        SB.stanForLoopB "k" Nothing dsName
          $ SB.addExprLines "postStratifiedParameter"
          [SB.var pV `SB.plusEq` (SB.paren wgtE `SB.times` SB.paren pE)
          , SB.var wV `SB.plusEq` SB.paren wgtE]
        SB.addExprLine "postStratifiedParameter" $ SB.vectorizedOne gName $ SB.binOp "./=" (SB.var pV) (SB.var wV)
  SB.inBlock SB.SBTransformedParameters $ case reIndexRttM of
    Nothing -> do
      gProb <- zeroVec gName varName
      gWeight <- zeroVec gName (varName <> "_wgts")
      profF $ SB.useDataSetForBindings rtt $ do
        psLoops gProb gWeight
        return gProb
    Just reIndexRtt -> do
      let reIndexKey = SB.dataSetName reIndexRtt
      riProb <-  SB.stanDeclare varName(SB.StanVector $ SB.NamedDim reIndexKey) ""
      profF $ SB.useDataSetForBindings rtt $ do
        gProb <- zeroVec gName psDataByGroupName
        gWeight <- zeroVec gName (psDataByGroupName <> "_wgts")
        psLoops gProb gWeight
        SB.useDataSetForBindings reIndexRtt
          $ SB.addExprLine "postStratifiedParameter"
          $ SB.vectorizedOne reIndexKey
          $ SB.var riProb `SB.eq` SB.var gProb
      return riProb

stackDataSets :: forall md gq r1 r2. (Typeable r1, Typeable r2)
                       => Text
                       -> SB.RowTypeTag r1
                       -> SB.RowTypeTag r2
                       -> SB.GroupSet -- groups to stack
                       -> SB.StanBuilderM md gq (SB.RowTypeTag (Either r1 r2) -- tag for combined loops
                                                , SB.StanName -> SME.StanVar -> SME.StanVar -> SB.StanBuilderM md gq SME.StanVar -- stack variables
                                             )
stackDataSets name rtt1 rtt2 groups = do
  let n1 = SB.dataSetName rtt1
      n2 = SB.dataSetName rtt2
      sizeName x = "N_" <> x
  listF1 <- SB.getModelDataFoldableAsListF rtt1
  listF2 <- SB.getModelDataFoldableAsListF rtt2
  rtt <- SB.addModelDataSet name (SB.ToFoldable $ \md -> (Left <$> listF1 md) ++ (Right <$> listF2 md))
  SB.addUseBindingToDataSet rtt n1 $ SB.StanVar ("N_" <> n1) SB.StanInt
  SB.addUseBindingToDataSet rtt n2 $ SB.StanVar ("N_" <> n2) SB.StanInt
  SB.inBlock SB.SBTransformedData $ do
    sizeV <- SB.stanDeclareRHS (sizeName name) SME.StanInt "<lower=0>"
             $ SME.name (sizeName n1) `SME.plus` SME.name (sizeName n2)
    SB.addDeclBinding name sizeV
    SB.addUseBindingToDataSet rtt name sizeV
  let copyUseBinding rttFrom rttTo dimName = do
        m <- SB.getDataSetBindings rttFrom
        case Map.lookup dimName m of
          Just e -> SB.addUseBindingToDataSet' rttTo dimName e
          Nothing -> SB.stanBuildError
                     $ "stackDataSets.copyUseBinding: " <> dimName <> " not found in data-set="
                     <> SB.dataSetName rtt <> " (inputeType=" <> show (SB.inputDataType rtt) <>")."
      copyIfNamed rttFrom rttTo d = case d of
        SB.NamedDim ik -> copyUseBinding rttFrom rttTo ik
        _ -> pure ()
      stackVars vName v1@(SB.StanVar _ t1) v2@(SB.StanVar _ t2) = do
        let stackTypesErr :: SB.StanBuilderM md gq a
            stackTypesErr = SB.stanBuildError $ "BuildingBlocks.stackDataSets.stackVars: Bad variables for stacking: v1=" <> show v1 <> "; v2=" <> show v2
        case (t1, t2) of
          (SB.StanVector (SB.NamedDim n1), SB.StanVector (SB.NamedDim n2)) ->
            SB.stanDeclareRHS vName (SB.StanVector $ SB.NamedDim name) "" $ SB.function "append_row" (SB.varNameE v1 :| [SB.varNameE v2])
          (SB.StanMatrix (SB.NamedDim n1, cd1), SB.StanMatrix (SB.NamedDim n2, cd2)) -> do
            when (cd1 /= cd2) stackTypesErr
            copyIfNamed rtt1 rtt cd1
            SB.useDataSetForBindings rtt
              $ SB.stanDeclareRHS vName (SB.StanMatrix (SB.NamedDim name, cd1)) "" $ SB.function "append_row" (SB.varNameE v1 :| [SB.varNameE v2])
          (SB.StanArray (SB.NamedDim n1 : ads1) at1, SB.StanArray (SB.NamedDim n2 : ads2) at2) -> do
            when ((ads1 /= ads2) || (at1 /= at2)) stackTypesErr
            mapM_ (copyIfNamed rtt1 rtt) (ads1 ++ SB.getDims at1)
            SB.useDataSetForBindings rtt
              $ SB.stanDeclareRHS vName (SB.StanArray (SB.NamedDim name : ads1) at1) "" $ SB.function "append_array" (SB.varNameE v1 :| [SB.varNameE v2])
          _ -> stackTypesErr
      stackGroupIndexes :: forall k. SB.GroupTypeTag k -> SB.Phantom k -> SB.StanBuilderM md gq (SB.Phantom k)
      stackGroupIndexes gtt _ = do
        let gName = SB.taggedGroupName gtt
        iv1 <- SB.getGroupIndexVar rtt1 gtt
        iv2 <- SB.getGroupIndexVar rtt2 gtt
        iv <- SB.inBlock SB.SBTransformedData $ do
          SB.setDataSetForBindings rtt
          stackVars (name <> "_" <> gName) iv1 iv2
        SB.addUseBindingToDataSet rtt gName iv
        return SB.Phantom
  _ <- DHash.traverseWithKey stackGroupIndexes groups
  return (rtt, stackVars)

{-
groupDataSetMembershipMatrix :: SB.IndexKey -> SB.RowTypeTag r -> SB.StanBuilderM env d SB.StanVar
groupDataSetMembershipMatrix groupIndexKey rttD = SB.inBlock SB.SBTransformedData $ SB.useDataSetForBindings rttD $ do
  let dsIndexKey = SB.dataSetName rttD
      mType = SB.StanMatrix (SB.NamedDim groupKey, SB.NamedDim dsIndexKey)
      mName = groupIndexKey <> "_" <> dsIndexKey <> "_MM"
  sv <- SB.stanDeclare mName mType "<lower=0, upper=1>"
  SB.stanForLoopB "n" Nothing dsIndexKey
    $ SB.stanForLoopB "g"
-}



{-
addMultiIndex :: SB.RowTypeTag r -> [DHash.Some GroupTypeTag] -> Maybe Text -> SB.StanBuilderM env d SB.StanVar
addMultiIndex rtt gtts mVarName = do
  -- check that all indices are present
  rowInfo <- SB.rowInfo rtt
  let checkGroup :: Some GroupTypeTag -> SB.StanBuilderM env d ()
      checkGroup sg = case sg of
        DHash.Some gtt -> case DHash.lookup gtt (SB.groupIndexes rowInfo) of
          Nothing -> SB.stanBuildError $ "addMultiIndex: group " <> Sb.taggedGroupName gtt <> " is missing from group indexes for data-set " <> SB.dataSetName rtt <> "."
          Just _ -> return ()
  mapM_ checkGroup gtts
-}
-}

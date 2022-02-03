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

module Stan.ModelBuilder.BuildingBlocks where

import qualified Stan.ModelBuilder as SB
import qualified Stan.ModelBuilder.Expressions as SME
import qualified Stan.ModelBuilder.Distributions as SMD
import qualified Stan.ModelBuilder.GroupModel as SGM

import Prelude hiding (All)
import Data.List.NonEmpty as NE
import qualified Data.Dependent.HashMap as DHash
import qualified Data.Dependent.Sum as DSum
import qualified Data.Map as Map
import qualified Data.Set as Set
import qualified Data.Text as T
import qualified Data.Vector as V

addIntData :: (Typeable md, Typeable gq)
            => SB.RowTypeTag r
            -> SME.StanName
            -> Maybe Int
            -> Maybe Int
            -> (r -> Int)
            -> SB.StanBuilderM md gq SME.StanVar
addIntData rtt varName mLower mUpper f = do
  let stanType =  SB.StanArray [SB.NamedDim $ SB.dataSetName rtt] SME.StanInt
      bounds = case (mLower, mUpper) of
                 (Nothing, Nothing) -> ""
                 (Just l, Nothing) -> "<lower=" <> show l <> ">"
                 (Nothing, Just u) -> "<upper=" <> show u <> ">"
                 (Just l, Just u) -> "<lower=" <> show l <> ", upper=" <> show u <> ">"
  SB.addColumnJson rtt varName stanType bounds f

addCountData :: forall r md gq.(Typeable md, Typeable gq)
             => SB.RowTypeTag r
             -> SME.StanName
             -> (r -> Int)
             -> SB.StanBuilderM md gq SME.StanVar
addCountData rtt varName f = addIntData rtt varName (Just 0) Nothing f

addRealData :: (Typeable md, Typeable gq)
            => SB.RowTypeTag r
            -> SME.StanName
            -> Maybe Double
            -> Maybe Double
            -> (r -> Double)
            -> SB.StanBuilderM md gq SME.StanVar
addRealData rtt varName mLower mUpper f = do
  let stanType =  SB.StanArray [SB.NamedDim $ SB.dataSetName rtt] SME.StanReal
      bounds = case (mLower, mUpper) of
                 (Nothing, Nothing) -> ""
                 (Just l, Nothing) -> "<lower=" <> show l <> ">"
                 (Nothing, Just u) -> "<upper=" <> show u <> ">"
                 (Just l, Just u) -> "<lower=" <> show l <> ", upper=" <> show u <> ">"
  SB.addColumnJson rtt varName stanType bounds f


add2dMatrixData :: (Typeable md, Typeable gq)
                => SB.RowTypeTag r
                -> SB.MatrixRowFromData r
--                -> SME.StanName
--            -> Int
                -> Maybe Double
                -> Maybe Double
--            -> (r -> V.Vector Double)
            -> SB.StanBuilderM md gq SME.StanVar
add2dMatrixData rtt matrixRowFromData mLower mUpper = do
  let --stanType =  SB.StanMatrix (SB.NamedDim $ SB.dataSetName rtt) (SB.NamedDim colName)
      bounds = case (mLower, mUpper) of
                 (Nothing, Nothing) -> ""
                 (Just l, Nothing) -> "<lower=" <> show l <> ">"
                 (Nothing, Just u) -> "<upper=" <> show u <> ">"
                 (Just l, Just u) -> "<lower=" <> show l <> ", upper=" <> show u <> ">"
  SB.add2dMatrixJson rtt matrixRowFromData bounds (SB.NamedDim $ SB.dataSetName rtt)  --stanType bounds f


intercept :: forall md gq. (Typeable md, Typeable gq) => Text -> SME.StanExpr -> SB.StanBuilderM md gq SB.StanVar
intercept iName alphaPriorE = do
  iVar <- SB.inBlock SB.SBParameters $ SB.stanDeclare iName SB.StanReal ""
  let --alphaE = SB.name iName
      interceptE = SB.var iVar `SME.vectorSample` alphaPriorE
  SB.inBlock SB.SBModel $ SB.addExprLine "intercept" interceptE
  return iVar

sampleDistV :: SB.RowTypeTag r -> SMD.StanDist args -> args -> SB.StanVar -> SB.StanBuilderM md gq ()
sampleDistV rtt sDist args yV =  SB.inBlock SB.SBModel $ do
  let dsName = SB.dataSetName rtt
      samplingE = SMD.familySampleF sDist args yV
  SB.addExprLine "sampleDistV" $ SME.vectorizedOne dsName samplingE


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
  SB.addFixedIntJson' "grainsize" Nothing 1
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

generateLogLikelihood :: SB.RowTypeTag r -> SMD.StanDist args -> SB.StanBuilderM md gq args -> SME.StanVar -> SB.StanBuilderM md gq ()
generateLogLikelihood rtt sDist args yV =  generateLogLikelihood' $ addToLLSet rtt (LLDetails sDist args yV) $ emptyLLSet

data LLDetails md gq args r = LLDetails (SMD.StanDist args) (SB.StanBuilderM md gq args) SME.StanVar
data LLDetailsList md gq args r = LLDetailsList [LLDetails md gq args r]

addDetailsLists :: LLDetailsList md gq args r -> LLDetailsList md gq args r -> LLDetailsList md gq args r
addDetailsLists (LLDetailsList x) (LLDetailsList y) = LLDetailsList (x <> y)

type LLSet md gq args = DHash.DHashMap SB.RowTypeTag (LLDetailsList md gq args)

emptyLLSet :: LLSet md gq args
emptyLLSet = DHash.empty

addToLLSet :: SB.RowTypeTag r -> LLDetails md gq args r -> LLSet md gq args -> LLSet md gq args
addToLLSet rtt d llSet = DHash.insertWith addDetailsLists rtt (LLDetailsList [d]) llSet
--data Res r = Res

generateLogLikelihood' :: LLSet md gq args -> SB.StanBuilderM md gq ()
generateLogLikelihood' llSet =  SB.inBlock SB.SBLogLikelihood $ do
  let prependSizeName rtt (LLDetailsList ds) ls = Prelude.replicate (Prelude.length ds) (SB.dataSetSizeName rtt) ++ ls
  llSizeListNE <- case nonEmpty (DHash.foldrWithKey prependSizeName [] llSet) of
    Nothing -> SB.stanBuildError "generateLogLikelihood': empty set of log-likelihood details given"
    Just x -> return x
  let llSizeE = SME.multiOp "+" $ fmap SB.name llSizeListNE
  SB.addDeclBinding' "LLIndex" llSizeE
  logLikV <- SB.stanDeclare "log_lik" (SME.StanVector $ SME.NamedDim "LLIndex") ""
  let doOne :: SB.RowTypeTag a -> LLDetails md gq args a -> StateT [SME.StanExpr] (SB.StanBuilderM md gq) (SB.RowTypeTag a)
      doOne rtt (LLDetails dist argsM yV) = do
        prevSizes <- get
        lift $ SB.bracketed 2 $ SB.useDataSetForBindings rtt $ SB.indexBindingScope $ do
          SB.addUseBinding' "LLIndex" $ SB.multiOp "+" (SB.name "n" :| prevSizes)
          let dsName = SB.dataSetName rtt
          args <- argsM
          SB.stanForLoopB "n" Nothing dsName
            $ SB.addExprLine "generateLogLikelihood'"
            $ SB.var logLikV `SB.eq` SMD.familyLDF dist args yV
        put $ SB.name (SB.dataSetSizeName rtt) : prevSizes
        pure rtt
      doList ::  SB.RowTypeTag a -> LLDetailsList md gq args a -> StateT [SME.StanExpr] (SB.StanBuilderM md gq) (SB.RowTypeTag a)
      doList rtt (LLDetailsList lls) = traverse_ (doOne rtt) lls >> pure rtt
  _ <- evalStateT (DHash.traverseWithKey doList llSet) []
  pure ()

{-

  doOne rtt (LLDetails dist bldr v) = do
    SB.bracketed 2 $ SB.useDataSetForBindings rtt $ do
      args <- bldr
      SB.stanForLoop "n" Nothing (SB.dataSetName rtt) $ do
        SB.addExprLine "generateLogLikelihood'" $ SB.var logLik

      let argsM (_, a, _) = a
      args <- sequence $ argsM <$> distsArgsM
      let distsAndArgs = NE.zipWith (\(d, _, y) a -> (d, a, y)) distsArgsM args
    SB.stanForLoopB "n" Nothing dsName $ do
      let lhsE = SME.var logLikV --SME.withIndexes (SME.name "log_lik") [dim]
          oneRhsE (sDist, args, yV) = SMD.familyLDF sDist args yV
          rhsE = SB.multiOp "+" $ fmap oneRhsE distsAndArgs
      SB.addExprLine "generateLogLikelihood" $ lhsE `SME.eq` rhsE
  let dsName = SB.dataSetName  rtt
      dim = SME.NamedDim dsName llSet

  SB.bracketed 2 $ SB.useDataSetForBindings rtt $ do
    let argsM (_, a, _) = a
    args <- sequence $ argsM <$> distsArgsM
    let distsAndArgs = NE.zipWith (\(d, _, y) a -> (d, a, y)) distsArgsM args
    SB.stanForLoopB "n" Nothing dsName $ do
      let lhsE = SME.var logLikV --SME.withIndexes (SME.name "log_lik") [dim]
          oneRhsE (sDist, args, yV) = SMD.familyLDF sDist args yV
          rhsE = SB.multiOp "+" $ fmap oneRhsE distsAndArgs
      SB.addExprLine "generateLogLikelihood" $ lhsE `SME.eq` rhsE
-}

generatePosteriorPrediction :: SB.RowTypeTag r -> SME.StanVar -> SMD.StanDist args -> args -> SB.StanBuilderM md gq SME.StanVar
generatePosteriorPrediction rtt sv@(SME.StanVar ppName t) sDist args = SB.inBlock SB.SBGeneratedQuantities $ do
  let rngE = SMD.familyRNG sDist args
--      ppVar = SME.StanVar ppName t
--      ppE = SME.var ppVar --SME.indexBy (SME.name ppName) k `SME.eq` rngE
  ppVar <- SB.stanDeclare ppName t ""
  SB.stanForLoopB "n" Nothing (SB.dataSetName rtt) $ SB.addExprLine "generatePosteriorPrediction" $ SME.var ppVar `SME.eq` rngE
  return sv

diagVectorFunction :: SB.StanBuilderM md gq Text
diagVectorFunction = SB.declareStanFunction "vector indexBoth(vector[] vs, int N)" $ do
--  SB.addStanLine "int vec_size = num_elements(ii)"
  SB.addStanLine "vector[N] out_vec"
  SB.stanForLoop "i" Nothing "N" $ const $ SB.addStanLine $ "out_vec[i] = vs[i, i]"
  SB.addStanLine "return out_vec"
  return "indexBoth"

-- given something indexed by things which can be indexed from a data-set,
-- create a vector which is a 1d alias
-- e.g., given beta_g1_g2[J_g1, J_g2]
-- declare beta_g1_g2_vD1[J_D1]
-- and set beta_g1_g2_vD1[n] = beta_g1_g2[g1_D1[n], g2_D1[n]]
vectorizeVar :: SB.StanVar -> SB.IndexKey -> SB.StanBuilderM md gq SB.StanVar
vectorizeVar v@(SB.StanVar vn _) = vectorizeExpr vn (SB.var v)

vectorizeExpr :: SB.StanName -> SB.StanExpr -> SB.IndexKey -> SB.StanBuilderM md gq SB.StanVar
vectorizeExpr sn se ik = do
  let vecVname = sn <> "_v"
  fv <- SB.stanDeclare vecVname (SB.StanVector (SB.NamedDim ik)) ""
  SB.stanForLoopB "n" Nothing ik $ SB.addExprLine "vectorizeExpr" $ SB.var fv `SB.eq` se
  return fv

weightedMeanFunction :: SB.StanBuilderM md gq ()
weightedMeanFunction =  SB.addFunctionsOnce "weighted_mean"
                        $ SB.declareStanFunction "real weighted_mean(vector ws, vector xs)" $ do
  SB.addStanLine "vector[num_elements(xs)] wgtdXs = ws .* xs"
  SB.addStanLine "return (sum(wgtdXs)/sum(ws))"


matrixTranspose :: SB.StanVar -> SB.StanBuilderM md gq SB.StanVar
matrixTranspose m@(SB.StanVar n (SB.StanMatrix (rd, cd))) = do
  SB.stanDeclareRHS (n <> "_Transpose") (SB.StanMatrix (cd, rd)) "" $ SB.matTranspose m

indexedConstIntArray :: SB.RowTypeTag r -> Int -> SB.StanBuilderM md gq SB.StanVar
indexedConstIntArray rtt n =
  let dsName = SB.dataSetName rtt
      sizeName = "N_" <> dsName
  in SB.inBlock SB.SBTransformedData
     $ SB.stanDeclareRHS ("constIndex_" <> dsName) (SB.StanArray [SB.NamedDim dsName] SB.StanInt) ""
     $ SB.function "rep_array" (SB.scalar (show n) :| [SB.name sizeName])

stackDataSets :: forall md gq r1 r2. ()
                       => Text
                       -> SB.RowTypeTag r1
                       -> SB.RowTypeTag r2
                       -> SB.GroupSet -- groups to stack
                       -> SB.StanBuilderM md gq (SB.RowTypeTag () -- tag for combined loops
                                                , SB.StanVar -- vector with data-set index
                                                , SB.StanName -> SME.StanVar -> SME.StanVar -> SB.StanBuilderM md gq SME.StanVar -- stack variables
                                             )
stackDataSets name rtt1 rtt2 groups = do
  let n1 = SB.dataSetName rtt1
      n2 = SB.dataSetName rtt2
      sizeName x = "N_" <> x
  rtt <- SB.addModelDataSet name (SB.ToFoldable $ const [()])
  SB.addUseBindingToDataSet rtt n1 $ SB.StanVar ("N_" <> n1) SB.StanInt
  SB.addUseBindingToDataSet rtt n2 $ SB.StanVar ("N_" <> n2) SB.StanInt
  indexV <- SB.inBlock SB.SBTransformedData $ do
    sizeV <- SB.stanDeclareRHS (sizeName name) SME.StanInt "<lower=0>"
             $ SME.name (sizeName n1) `SME.plus` SME.name (sizeName n2)
    SB.addDeclBinding name sizeV
    SB.addUseBindingToDataSet rtt name sizeV
    let iArray n s = SB.function "rep_array" (SB.scalar (show n) :| [SB.name $ sizeName s])
    SB.stanDeclareRHS "dataSetIndex" (SB.StanArray [SB.NamedDim name] SB.StanInt) "<lower=0>"
      $ SB.function "append_array" (iArray 0 n1 :| [iArray 1 n2])
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
  return (rtt, indexV, stackVars)

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

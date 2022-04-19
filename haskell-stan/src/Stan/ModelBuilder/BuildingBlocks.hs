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
import qualified Data.List.NonEmpty as NE
import qualified Data.Dependent.HashMap as DHash
import qualified Data.Dependent.Sum as DSum
import qualified Data.Map as Map
import qualified Data.Set as Set
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Stan.ModelConfig as SB
import Streamly.Data.Array.Foreign (getIndex)

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
                -> Maybe Double
                -> Maybe Double
            -> SB.StanBuilderM md gq SME.StanVar
add2dMatrixData rtt matrixRowFromData mLower mUpper = do
  let bounds = case (mLower, mUpper) of
                 (Nothing, Nothing) -> ""
                 (Just l, Nothing) -> "<lower=" <> show l <> ">"
                 (Nothing, Just u) -> "<upper=" <> show u <> ">"
                 (Just l, Just u) -> "<lower=" <> show l <> ", upper=" <> show u <> ">"
  SB.add2dMatrixJson rtt matrixRowFromData bounds (SB.NamedDim $ SB.dataSetName rtt)  --stanType bounds f

sampleDistV :: SB.RowTypeTag r -> SMD.StanDist args -> args -> SB.StanVar -> SB.StanBuilderM md gq ()
sampleDistV rtt sDist args yV =  SB.inBlock SB.SBModel $ do
  let dsName = SB.dataSetName rtt
      samplingE = SMD.familySampleF sDist args yV
  SB.addExprLine "sampleDistV" $ SME.vectorizedOne dsName samplingE


printVar :: Text -> SB.StanVar -> SB.StanBuilderM md gq ()
printVar t v@(SB.StanVar n _) = SB.addExprLine "printVar" $ SB.function "print" $ SB.bare ("\"" <> t <> n <> "=\"") :| [SB.varNameE v]

printTarget :: Text -> SB.StanBuilderM md gq ()
printTarget t = SB.addExprLine "printVar" $ SB.function "print" $ SB.bare ("\"" <> t <> "target=\"") :| [SB.bare "target()"]

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

generateLogLikelihood :: SB.RowTypeTag r -> SMD.StanDist args -> SB.StanBuilderM md gq args -> SME.StanVar -> SB.StanBuilderM md gq ()
generateLogLikelihood rtt sDist args yV =  generateLogLikelihood' $ addToLLSet rtt (LLDetails sDist args yV) $ emptyLLSet

data LLDetails md gq r = forall args.LLDetails (SMD.StanDist args) (SB.StanBuilderM md gq args) SME.StanVar
--  LLDetails :: forall args.SMD.StanDist args -> SB.StanBuilderM md gq args -> SME.StanVar -> LLDetails md gq r

data LLDetailsList md gq r = LLDetailsList [LLDetails md gq r]

addDetailsLists :: LLDetailsList md gq r -> LLDetailsList md gq r -> LLDetailsList md gq r
addDetailsLists (LLDetailsList x) (LLDetailsList y) = LLDetailsList (x <> y)

type LLSet md gq = DHash.DHashMap SB.RowTypeTag (LLDetailsList md gq)

emptyLLSet :: LLSet md gq
emptyLLSet = DHash.empty

addToLLSet :: SB.RowTypeTag r -> LLDetails md gq r -> LLSet md gq  -> LLSet md gq
addToLLSet rtt d llSet = DHash.insertWith addDetailsLists rtt (LLDetailsList [d]) llSet

mergeLLSets ::  LLSet md gq  -> LLSet md gq -> LLSet md gq
mergeLLSets = DHash.unionWith addDetailsLists
--data Res r = Res

generateLogLikelihood' :: LLSet md gq -> SB.StanBuilderM md gq ()
generateLogLikelihood' llSet =  SB.inBlock SB.SBLogLikelihood $ do
  let prependSizeName rtt (LLDetailsList ds) ls = Prelude.replicate (Prelude.length ds) (SB.dataSetSizeName rtt) ++ ls
  llSizeListNE <- case nonEmpty (DHash.foldrWithKey prependSizeName [] llSet) of
    Nothing -> SB.stanBuildError "generateLogLikelihood': empty set of log-likelihood details given"
    Just x -> return x
  let llSizeE = SME.multiOp "+" $ fmap SB.name llSizeListNE
  SB.addDeclBinding' "LLIndex" llSizeE
  logLikV <- SB.stanDeclare "log_lik" (SME.StanVector $ SME.NamedDim "LLIndex") ""
  let doOne :: SB.RowTypeTag a -> LLDetails md gq a -> StateT [SME.StanExpr] (SB.StanBuilderM md gq) (SB.RowTypeTag a)
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
      doList ::  SB.RowTypeTag a -> LLDetailsList md gq a -> StateT [SME.StanExpr] (SB.StanBuilderM md gq) (SB.RowTypeTag a)
      doList rtt (LLDetailsList lls) = traverse_ (doOne rtt) lls >> pure rtt
  _ <- evalStateT (DHash.traverseWithKey doList llSet) []
  pure ()

generatePosteriorPrediction :: SB.RowTypeTag r -> SME.StanVar -> SMD.StanDist args -> args -> SB.StanBuilderM md gq SME.StanVar
generatePosteriorPrediction rtt sv sDist args = generatePosteriorPrediction' rtt sv sDist args id

generatePosteriorPrediction' :: SB.RowTypeTag r -> SME.StanVar -> SMD.StanDist args -> args -> (SB.StanExpr -> SB.StanExpr) -> SB.StanBuilderM md gq SME.StanVar
generatePosteriorPrediction' rtt sv@(SME.StanVar ppName t) sDist args f = SB.inBlock SB.SBGeneratedQuantities $ do
  let rngE = SMD.familyRNG sDist args
  ppVar <- SB.stanDeclare ppName t ""
  SB.stanForLoopB "n" Nothing (SB.dataSetName rtt) $ SB.addExprLine "generatePosteriorPrediction" $ SME.var ppVar `SME.eq` f rngE
  return sv

diagVectorFunction :: SB.StanBuilderM md gq Text
diagVectorFunction = SB.declareStanFunction "vector indexBoth(vector[] vs, int N)" $ do
--  SB.addStanLine "int vec_size = num_elements(ii)"
  SB.addStanLine "vector[N] out_vec"
  SB.stanForLoop "i" Nothing "N" $ const $ SB.addStanLine $ "out_vec[i] = vs[i, i]"
  SB.addStanLine "return out_vec"
  pure "indexBoth"

-- given something indexed by things which can be indexed from a data-set,
-- create a vector which is a 1d alias
-- e.g., given beta_g1_g2[J_g1, J_g2]
-- declare beta_g1_g2_vD1[J_D1]
-- and set beta_g1_g2_vD1[n] = beta_g1_g2[g1_D1[n], g2_D1[n]]
vectorizeVar :: SB.StanVar -> SB.IndexKey -> SB.StanBuilderM md gq SB.StanVar
vectorizeVar v@(SB.StanVar vn _) = vectorizeExpr vn (SB.var v)

vectorizeExpr :: SB.StanName -> SB.StanExpr -> SB.IndexKey -> SB.StanBuilderM md gq SB.StanVar
vectorizeExpr sn se ik = head <$> vectorizeExprT ((sn, se) :| []) ik

-- like vectorizeExpr but for multiple things in same loop
vectorizeExprT :: Traversable t => t (SB.StanName, SB.StanExpr) -> SB.IndexKey -> SB.StanBuilderM md gq (t SB.StanVar)
vectorizeExprT namedExprs ik = do
  let vecVname sn = sn <> "_v"
      declareVec (sn, se) = do
        fv <- SB.stanDeclare (vecVname sn) (SB.StanVector (SB.NamedDim ik)) ""
        return (fv, se)
      fillVec (v, se) = (SB.addExprLine "vectorizeExprA" $ SB.var v `SB.eq` se) >> return v
  varExps <- traverse declareVec namedExprs
  SB.stanForLoopB "n" Nothing ik $ traverse fillVec varExps

weightedMeanFunction :: SB.StanBuilderM md gq ()
weightedMeanFunction =  SB.addFunctionsOnce "weighted_mean"
                        $ SB.declareStanFunction "real weighted_mean(vector ws, vector xs)" $ do
  SB.addStanLine "vector[num_elements(xs)] wgtdXs = ws .* xs"
  SB.addStanLine "return (sum(wgtdXs)/sum(ws))"

realIntRatio :: SME.StanVar -> SME.StanVar -> SME.StanExpr
realIntRatio k l = SB.binOp "/"
                   (SB.paren $ (SB.scalar "1.0" `SB.times` SB.var k))
                   (SB.paren $ (SB.scalar "1.0" `SB.times` SB.var l))

matrixTranspose :: SB.StanVar -> SB.StanBuilderM md gq SB.StanVar
matrixTranspose m@(SB.StanVar n (SB.StanMatrix (rd, cd))) = do
  SB.stanDeclareRHS (n <> "_Transpose") (SB.StanMatrix (cd, rd)) "" $ SB.matTranspose m

indexedConstIntArray :: SB.RowTypeTag r -> Maybe Text -> Int -> SB.StanBuilderM md gq SB.StanVar
indexedConstIntArray rtt mSuffix n =
  let dsName = SB.dataSetName rtt
      sizeName = "N_" <> dsName
  in SB.inBlock SB.SBTransformedData
     $ SB.stanDeclareRHS ("constIndex_" <> dsName <> maybe "" ("_" <>) mSuffix) (SB.StanArray [SB.NamedDim dsName] SB.StanInt) ""
     $ SB.function "rep_array" (SB.scalar (show n) :| [SB.name sizeName])

zeroVectorE :: SME.IndexKey -> SB.StanExpr
zeroVectorE indexKey = SB.function "rep_vector" ((SB.scalar $ show 0) :| [SB.indexSize indexKey])

zeroMatrixE :: SME.IndexKey -> SB.IndexKey -> SB.StanExpr
zeroMatrixE rowIndex colIndex = SB.function "rep_matrix" ((SB.scalar $ show 0) :| [SB.indexSize rowIndex, SB.indexSize colIndex])

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


postStratifiedParameterF :: (Typeable md, Typeable gq)
                         => Bool
                         -> SB.StanBlock
                         -> Maybe Text
                         -> SB.RowTypeTag r -- data set to post-stratify
                         -> SB.GroupTypeTag k -- group by
                         -> SB.StanVar -- weight
                         -> SB.StanVar --  expression of parameters to post-stratify
                         -> Maybe (SB.RowTypeTag r') -- re-index?
                         -> SB.StanBuilderM md gq SB.StanVar
postStratifiedParameterF prof block varNameM rtt gtt wgtsV pV reIndexRttM = do
  psByGroupFunction
  let dsName = SB.dataSetName rtt
      gName = SB.taggedGroupName gtt
      psDataByGroupName = dsName <> "_By_" <> gName
      indexName = SB.dataSetName rtt <> "_" <> SB.taggedGroupName gtt
      varName = case reIndexRttM of
        Nothing -> fromMaybe psDataByGroupName varNameM
        Just reIndexRtt -> fromMaybe (dsName <> "_By_" <> SB.dataSetName reIndexRtt) varNameM
      grpVecType =  SB.StanVector $ SB.NamedDim gName
      psVecType =  SB.StanVector $ SB.NamedDim dsName
      profF :: SB.StanBuilderM md gq a -> SB.StanBuilderM md gq a
      profF = if prof then SB.profile varName else SB.bracketed 2
  SB.inBlock block $ case reIndexRttM of
    Nothing -> do
      probV <- SB.stanDeclare varName grpVecType ""
      profF $ do
        wgtsAsVec <- SB.stanDeclareRHS ("wgtsVec") psVecType "" $ SB.vectorizedOne dsName $ SME.function "to_vector" (one $ SB.var wgtsV)
        SB.addExprLine "postStratifiedParameterF"
          $ SME.var probV `SME.eq` SME.function "psByGroup" (SME.indexSize dsName :|  [SME.indexSize gName, SME.bare indexName, SB.var wgtsAsVec, SB.var pV])
      return probV
    Just reIndexRtt -> do
      let reIndexKey = SB.dataSetName reIndexRtt
      riProb <-  SB.stanDeclare varName (SB.StanVector $ SB.NamedDim reIndexKey) ""
      profF $ SB.useDataSetForBindings rtt $ do
        wgtsAsVec <- SB.stanDeclareRHS ("wgtsVec") psVecType "" $ SB.vectorizedOne dsName $ SME.function "to_vector" (one $ SB.var wgtsV)
        gProb <- SB.stanDeclareRHS psDataByGroupName grpVecType ""
          $ SME.vectorizedOne dsName $ SME.function "psByGroup" (SME.indexSize dsName :|  [SME.indexSize gName, SME.bare indexName, SB.var wgtsAsVec, SB.var pV])
        SB.useDataSetForBindings reIndexRtt
          $ SB.addExprLine "postStratifiedParameter"
          $ SB.vectorizedOne reIndexKey
          $ SB.var riProb `SB.eq` SB.var gProb
      return riProb


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

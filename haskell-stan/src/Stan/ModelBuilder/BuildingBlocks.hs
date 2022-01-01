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
generateLogLikelihood rtt sDist args yV =  generateLogLikelihood' rtt (one (sDist, args, yV))

generateLogLikelihood' :: SB.RowTypeTag r -> NonEmpty (SMD.StanDist args, SB.StanBuilderM md gq args, SME.StanVar) -> SB.StanBuilderM md gq ()
generateLogLikelihood' rtt distsArgsM =  SB.inBlock SB.SBGeneratedQuantities $ do
  let dsName = SB.dataSetName rtt
      dim = SME.NamedDim dsName
  logLikV <- SB.stanDeclare "log_lik" (SME.StanVector dim) ""
  SB.bracketed 2 $ SB.useDataSetForBindings rtt $ do
    let argsM (_, a, _) = a
    args <- sequence $ argsM <$> distsArgsM
    let distsAndArgs = NE.zipWith (\(d, _, y) a -> (d, a, y)) distsArgsM args
    SB.stanForLoopB "n" Nothing dsName $ do
      let lhsE = SME.var logLikV --SME.withIndexes (SME.name "log_lik") [dim]
          oneRhsE (sDist, args, yV) = SMD.familyLDF sDist args yV
          rhsE = SB.multiOp "+" $ fmap oneRhsE distsAndArgs
      SB.addExprLine "generateLogLikelihood" $ lhsE `SME.eq` rhsE


generatePosteriorPrediction :: SB.RowTypeTag r -> SME.StanVar -> SMD.StanDist args -> args -> SB.StanBuilderM md gq SME.StanVar
generatePosteriorPrediction rtt sv@(SME.StanVar ppName t) sDist args = SB.inBlock SB.SBGeneratedQuantities $ do
  let rngE = SMD.familyRNG sDist args
--      ppVar = SME.StanVar ppName t
--      ppE = SME.var ppVar --SME.indexBy (SME.name ppName) k `SME.eq` rngE
  ppVar <- SB.stanDeclare ppName t ""
  SB.stanForLoopB "n" Nothing (SB.dataSetName rtt) $ SB.addExprLine "generatePosteriorPrediction" $ SME.var ppVar
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

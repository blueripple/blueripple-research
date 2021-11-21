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

import Prelude hiding (All)
import qualified Data.Map as Map
import qualified Data.Set as Set
import qualified Data.Vector as V

addIntData :: (Typeable d)
            => SB.RowTypeTag r
            -> SME.StanName
            -> Maybe Int
            -> Maybe Int
            -> (r -> Int)
            -> SB.StanBuilderM env d SME.StanVar
addIntData rtt varName mLower mUpper f = do
  let stanType =  SB.StanArray [SB.NamedDim $ SB.dataSetName rtt] SME.StanInt
      bounds = case (mLower, mUpper) of
                 (Nothing, Nothing) -> ""
                 (Just l, Nothing) -> "<lower=" <> show l <> ">"
                 (Nothing, Just u) -> "<upper=" <> show u <> ">"
                 (Just l, Just u) -> "<lower=" <> show l <> ", upper=" <> show u <> ">"
  SB.addColumnJson rtt varName stanType bounds f

addCountData :: forall r d env.(Typeable d)
             => SB.RowTypeTag r
             -> SME.StanName
             -> (r -> Int)
             -> SB.StanBuilderM env d SME.StanVar
addCountData rtt varName f = addIntData rtt varName (Just 0) Nothing f

addRealData :: (Typeable d)
            => SB.RowTypeTag r
            -> SME.StanName
            -> Maybe Double
            -> Maybe Double
            -> (r -> Double)
            -> SB.StanBuilderM env d SME.StanVar
addRealData rtt varName mLower mUpper f = do
  let stanType =  SB.StanArray [SB.NamedDim $ SB.dataSetName rtt] SME.StanReal
      bounds = case (mLower, mUpper) of
                 (Nothing, Nothing) -> ""
                 (Just l, Nothing) -> "<lower=" <> show l <> ">"
                 (Nothing, Just u) -> "<upper=" <> show u <> ">"
                 (Just l, Just u) -> "<lower=" <> show l <> ", upper=" <> show u <> ">"
  SB.addColumnJson rtt varName stanType bounds f


add2dMatrixData :: (Typeable d)
            => SB.RowTypeTag r
            -> SME.StanName
            -> Int
            -> Maybe Double
            -> Maybe Double
            -> (r -> V.Vector Double)
            -> SB.StanBuilderM env d SME.StanVar
add2dMatrixData rtt varName cols mLower mUpper f = do
  let --stanType =  SB.StanMatrix (SB.NamedDim $ SB.dataSetName rtt) (SB.NamedDim colName)
      bounds = case (mLower, mUpper) of
                 (Nothing, Nothing) -> ""
                 (Just l, Nothing) -> "<lower=" <> show l <> ">"
                 (Nothing, Just u) -> "<upper=" <> show u <> ">"
                 (Just l, Just u) -> "<lower=" <> show l <> ", upper=" <> show u <> ">"
  SB.add2dMatrixJson rtt varName bounds (SB.NamedDim $ SB.dataSetName rtt) cols f --stanType bounds f


intercept :: forall env d. (Typeable d) => Text -> SME.StanExpr -> SB.StanBuilderM env d SB.StanVar
intercept iName alphaPriorE = do
  iVar <- SB.inBlock SB.SBParameters $ SB.stanDeclare iName SB.StanReal ""
  let --alphaE = SB.name iName
      interceptE = SB.var iVar `SME.vectorSample` alphaPriorE
  SB.inBlock SB.SBModel $ SB.addExprLine "intercept" interceptE
  return iVar

sampleDistV :: SB.RowTypeTag r -> SMD.StanDist args -> args -> SB.StanVar -> SB.StanBuilderM env d ()
sampleDistV rtt sDist args yV =  SB.inBlock SB.SBModel $ do
  let dsName = SB.dataSetName rtt
      samplingE = SMD.familySampleF sDist args yV
  SB.addExprLine "sampleDistV" $ SME.vectorizedOne dsName samplingE


parallelSampleDistV :: Typeable d => Text -> SB.RowTypeTag r -> SMD.StanDist args -> args -> SB.StanVar -> SB.StanBuilderM env d ()
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


{-
generateLogLikelihood :: SB.RowTypeTag r -> SMD.StanDist args -> args -> SB.StanBuilderM env d ()
generateLogLikelihood rtt sDist args =  SB.inBlock SB.SBGeneratedQuantities $ do
  let dsName = SB.dataSetName rtt
      dim = SME.NamedDim dsName
  SB.stanDeclare "log_lik" (SME.StanVector dim) ""
  SB.stanForLoopB "n" Nothing dsName $ do
    let lhsE = SME.withIndexes (SME.name "log_lik") [dim]
        rhsE = SMD.familyLDF sDist dsName args
    SB.addExprLine "generateLogLikelihood" $ lhsE `SME.eq` rhsE
-}

generateLogLikelihood :: SB.RowTypeTag r -> SMD.StanDist args -> args -> SME.StanVar -> SB.StanBuilderM env d ()
generateLogLikelihood rtt sDist args yV =  generateLogLikelihood' rtt (one (sDist, args, yV))

generateLogLikelihood' :: SB.RowTypeTag r -> NonEmpty (SMD.StanDist args, args, SME.StanVar) -> SB.StanBuilderM env d ()
generateLogLikelihood' rtt distsAndArgs =  SB.inBlock SB.SBGeneratedQuantities $ do
  let dsName = SB.dataSetName rtt
      dim = SME.NamedDim dsName
  logLikV <- SB.stanDeclare "log_lik" (SME.StanVector dim) ""
  SB.stanForLoopB "n" Nothing dsName $ do
    let lhsE = SME.var logLikV --SME.withIndexes (SME.name "log_lik") [dim]
        oneRhsE (sDist, args, yV) = SMD.familyLDF sDist args yV
        rhsE = SB.multiOp "+" $ fmap oneRhsE distsAndArgs
    SB.addExprLine "generateLogLikelihood" $ lhsE `SME.eq` rhsE


generatePosteriorPrediction :: SB.RowTypeTag r -> SME.StanVar -> SMD.StanDist args -> args -> SB.StanBuilderM env d SME.StanVar
generatePosteriorPrediction rtt sv@(SME.StanVar ppName t) sDist args = SB.inBlock SB.SBGeneratedQuantities $ do
  let rngE = SMD.familyRNG sDist args
--      ppVar = SME.StanVar ppName t
--      ppE = SME.var ppVar --SME.indexBy (SME.name ppName) k `SME.eq` rngE
  ppVar <- SB.stanDeclare ppName t ""
  SB.stanForLoopB "n" Nothing (SB.dataSetName rtt) $ SB.addExprLine "generatePosteriorPrediction" $ SME.var ppVar
  return sv
--generatePosteriorPrediction _ _ _ _ = SB.stanBuildError "Variable argument to generatePosteriorPrediction must be a 1-d array with a named dimension"

fixedEffectsQR :: Text
               -> SME.StanName
               -> Maybe SME.StanVar
               -> SME.IndexKey
               -> SME.IndexKey
               -> SB.StanBuilderM env d (SME.StanVar, SME.StanVar, SME.StanVar, SME.StanVar -> SB.StanBuilderM env d SME.StanVar)
fixedEffectsQR thinSuffix matrix wgtsM rowKey colKey = do
  (qVar, f) <- fixedEffectsQR_Data thinSuffix matrix wgtsM rowKey colKey
  (thetaVar, betaVar) <- fixedEffectsQR_Parameters thinSuffix matrix colKey
  let qMatrixType = SME.StanMatrix (SME.NamedDim rowKey, SME.NamedDim colKey)
      q = "Q" <> thinSuffix <> "_ast"
      qMatrix = SME.StanVar q qMatrixType
  return (qVar, thetaVar, betaVar, f)

fixedEffectsQR_Data :: Text
                    -> SME.StanName
                    -> Maybe SME.StanVar
                    -> SME.IndexKey
                    -> SME.IndexKey
                    -> SB.StanBuilderM env d (SME.StanVar, SME.StanVar -> SB.StanBuilderM env d SME.StanVar)
fixedEffectsQR_Data thinSuffix matrix wgtsM rowKey colKey = do
  let ri = "R" <> thinSuffix <> "_ast_inverse"
      q = "Q" <> thinSuffix <> "_ast"
      r = "R" <> thinSuffix <> "_ast"
      qMatrixType = SME.StanMatrix (SME.NamedDim rowKey, SME.NamedDim colKey)
  qVar <- SB.inBlock SB.SBTransformedData $ do
    meanFunction <- case wgtsM of
      Nothing -> return $ "mean(" <> matrix <> "[,k])"
      Just (SB.StanVar wgtsName _) -> do
        weightedMeanFunction
        return $ "weighted_mean(to_vector(" <> wgtsName <> "), " <> matrix <> "[,k])"
    SB.stanDeclare ("mean_" <> matrix) (SME.StanVector (SME.NamedDim colKey)) ""
    SB.stanDeclare ("centered_" <> matrix) (SME.StanMatrix (SME.NamedDim rowKey, SME.NamedDim colKey)) ""
    SB.stanForLoopB "k" Nothing colKey $ do
      SB.addStanLine $ "mean_" <> matrix <> "[k] = " <> meanFunction --"mean(" <> matrix <> "[,k])"
      SB.addStanLine $ "centered_" <>  matrix <> "[,k] = " <> matrix <> "[,k] - mean_" <> matrix <> "[k]"
    let srE =  SB.function "sqrt" (one $ SB.indexSize rowKey `SB.minus` SB.scalar "1")
        qRHS = SB.function "qr_thin_Q" (one $ SB.name $ "centered_" <> matrix) `SB.times` srE
        rRHS = SB.function "qr_thin_R" (one $ SB.name $ "centered_" <> matrix) `SB.divide` srE
        riRHS = SB.function "inverse" (one $ SB.name r)
    qVar' <- SB.stanDeclareRHS q qMatrixType "" qRHS
    SB.stanDeclareRHS r (SME.StanMatrix (SME.NamedDim colKey, SME.NamedDim colKey)) "" rRHS
    SB.stanDeclareRHS ri (SME.StanMatrix (SME.NamedDim colKey, SME.NamedDim colKey)) "" riRHS
    return qVar'
  let centeredX mv@(SME.StanVar sn st) =
        case st of
          (SME.StanMatrix (SME.NamedDim rk, SME.NamedDim colKey)) -> SB.inBlock SB.SBTransformedData $ do
            cv <- SB.stanDeclare ("centered_" <> sn) (SME.StanMatrix (SME.NamedDim rk, SME.NamedDim colKey)) ""
            SB.stanForLoopB "k" Nothing colKey $ do
              SB.addStanLine $ "centered_" <>  sn <> "[,k] = " <> sn <> "[,k] - mean_" <> matrix <> "[k]"
            return cv
          _ -> SB.stanBuildError
               $ "fixedEffectsQR_Data (returned StanVar -> StanExpr): "
               <> " Given matrix doesn't have same dimensions as modeled fixed effects."
  return (qVar, centeredX)

fixedEffectsQR_Parameters :: Text -> SME.StanName -> SME.IndexKey -> SB.StanBuilderM env d (SME.StanVar, SME.StanVar)
fixedEffectsQR_Parameters thinSuffix matrix colKey = do
  let ri = "R" <> thinSuffix <> "_ast_inverse"
  thetaVar <- SB.inBlock SB.SBParameters $ SB.stanDeclare ("theta" <> matrix) (SME.StanVector $ SME.NamedDim colKey) ""
  betaVar <- SB.inBlock SB.SBTransformedParameters $ do
    betaVar' <- SB.stanDeclare ("beta" <> matrix) (SME.StanVector $ SME.NamedDim colKey) ""
    SB.addStanLine $ "beta" <> matrix <> " = " <> ri <> " * theta" <> matrix
    return betaVar'
  return (thetaVar, betaVar)


diagVectorFunction :: SB.StanBuilderM env d Text
diagVectorFunction = SB.declareStanFunction "vector indexBoth(vector[] vs, int N)" $ do
--  SB.addStanLine "int vec_size = num_elements(ii)"
  SB.addStanLine "vector[N] out_vec"
  SB.stanForLoop "i" Nothing "N" $ const $ SB.addStanLine $ "out_vec[i] = vs[i, i]"
  SB.addStanLine "return out_vec"
  return "indexBoth"

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
-- given something indexed by things which can indexed from a data-set,
-- create a vector which is a 1d alias
-- e.g., given beta_g1_g2[J_g1, J_g2]
-- declare beta_g1_g2_vD1[J_D1]
-- and set beta_g1_g2_vD1[n] = beta_g1_g2[g1_D1[n], g2_D1[n]]
vectorizeVar :: SB.StanVar -> SB.RowTypeTag r -> SB.StanBuilderM env d SB.StanVar
vectorizeVar v@(SB.StanVar vn vt) rtt = do
  let ik = SB.dataSetName rtt
      vecVname = vn <> "_v" <> ik
  fv <- SB.stanDeclare vecVname (SB.StanVector (SB.NamedDim ik)) ""
  SB.useDataSetForBindings rtt $ do
    SB.stanForLoopB "n" Nothing ik $ SB.addExprLine "vectorizeVar" $ SB.var fv `SB.eq` SB.var v
  return fv


weightedMeanFunction :: SB.StanBuilderM env d ()
weightedMeanFunction =  SB.addFunctionsOnce "weighted_mean"
                        $ SB.declareStanFunction "real weighted_mean(vector ws, vector xs)" $ do
  SB.addStanLine "vector[num_elements(xs)] wgtdXs = ws .* xs"
  SB.addStanLine "return (sum(wgtdXs)/sum(ws))"

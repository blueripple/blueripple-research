{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE UnicodeSyntax #-}

module BlueRipple.Model.Demographic.TableProducts
  (
    module BlueRipple.Model.Demographic.TableProducts
  )
where

import qualified BlueRipple.Model.Demographic.DataPrep as DDP
import qualified BlueRipple.Model.Demographic.EnrichData as DED

import qualified BlueRipple.Data.Keyed as BRK
import qualified BlueRipple.Data.DemographicTypes as DT

import qualified Knit.Report as K
--import qualified Knit.Effect.Logger as K
import qualified Knit.Effect.PandocMonad as KPM
import qualified Text.Pandoc.Error as PA
import qualified Polysemy as P
import qualified Polysemy.Error as PE

import qualified Control.MapReduce.Simple as MR
import qualified Frames.MapReduce as FMR
import qualified Frames.Folds as FF
import qualified Frames.Streamly.Transform as FST
import qualified Frames.Streamly.InCore as FSI
import qualified Streamly.Prelude as Streamly

import qualified Control.Foldl as FL
import qualified Data.IntMap as IM
import qualified Data.List as L
import qualified Data.Map.Strict as M
import qualified Data.Map.Merge.Strict as MM
import qualified Data.Set as S
import Data.Type.Equality (type (~))

import qualified Data.List as List
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V
import qualified Frames as F
import qualified Frames.Melt as F
import qualified Frames.Transform as FT
import qualified Numeric.LinearAlgebra as LA
import qualified Numeric.NLOPT as NLOPT
import qualified Numeric
import qualified Data.Vector.Storable as VS
import qualified Data.Vector.Unboxed as VU

import Control.Lens (view, (^.))
import Control.Monad.Except (throwError)
import GHC.TypeLits (Symbol)
import Graphics.Vega.VegaLite (density)

import qualified Stan.ModelBuilder as SMB
import qualified Stan.ModelConfig as SC
import qualified Stan.ModelBuilder.BuildingBlocks as SBB
import qualified Stan.ModelBuilder.DesignMatrix as DM
import qualified Stan.ModelBuilder.TypedExpressions.Types as TE
import qualified Stan.ModelBuilder.TypedExpressions.Statements as TE
import qualified Stan.ModelBuilder.TypedExpressions.Indexing as TEI
import qualified Stan.ModelBuilder.TypedExpressions.DAG as DAG
import qualified Stan.ModelBuilder.TypedExpressions.StanFunctions as SF
import Stan.ModelBuilder.TypedExpressions.TypedList (TypedList(..))

-- does a pure product
-- for each outerK we split each bs cell into as many cells as are represented by as
-- and we split in proportion to the counts in as
frameTableProduct :: forall outerK as bs count r .
                     (V.KnownField count
                     , DED.EnrichDataEffects r
                     , (bs V.++ (outerK V.++ as V.++ '[count])) ~ (((bs V.++ outerK) V.++ as) V.++ '[count])
                     , outerK F.⊆ (outerK V.++ as V.++ '[count])
                     , outerK F.⊆ (outerK V.++ bs V.++ '[count])
                     , bs F.⊆ (outerK V.++ bs V.++ '[count])
                     , FSI.RecVec (outerK V.++ as V.++ '[count])
                     , FSI.RecVec (bs V.++ (outerK V.++ as V.++ '[count]))
                     , F.ElemOf (outerK V.++ as V.++ '[count]) count
                     , F.ElemOf (outerK V.++ bs V.++ '[count]) count
                     , Show (F.Record outerK)
                     , BRK.FiniteSet (F.Record bs)
                     , Ord (F.Record bs)
                     , Ord (F.Record outerK)
                     , V.Snd count ~ Int
                     )
                  => F.FrameRec (outerK V.++ as V.++ '[count])
                  -> F.FrameRec (outerK V.++ bs V.++ '[count])
                  -> K.Sem r (F.FrameRec (bs V.++ outerK V.++ as V.++ '[count]))
frameTableProduct base splitUsing = DED.enrichFrameFromModel @count (fmap (DED.mapSplitModel round realToFrac) . splitFLookup) base
  where
    splitFLookup = FL.fold (DED.splitFLookupFld (F.rcast @outerK) (F.rcast @bs) (realToFrac @Int @Double . F.rgetField @count)) splitUsing


-- to get a set of counts from the product counts and null-space weights
-- take the weights and left-multiply them with the vectors
-- to get the weighted sum of those vectors and then add it to the
-- product counts

applyNSPWeights :: LA.Matrix LA.R -> LA.Vector LA.R -> LA.Vector LA.R -> LA.Vector LA.R
applyNSPWeights nsVs nsWs pV = pV + nsWs LA.<# nsVs

applyNSPWeightsO :: DED.EnrichDataEffects r => LA.Matrix LA.R -> LA.Vector LA.R -> LA.Vector LA.R -> K.Sem r (LA.Vector LA.R)
applyNSPWeightsO  nsVs nsWs pV = do
  K.logLE K.Info $ "Initial: pV + nsWs <.> nVs = " <> DED.prettyVector (pV + nsWs LA.<# nsVs)
  let n = VS.length nsWs
      objD v = (VS.sum $ VS.map (^ 2) (v - nsWs), 2 * (v - nsWs))
      obj2D v =
        let srNormV = Numeric.sqrt (LA.norm_2 v)
            srNormN = Numeric.sqrt (LA.norm_2 nsWs)
            nRatio = srNormN / srNormV
        in (v `LA.dot` nsWs - srNormV * srNormN, nsWs - (LA.scale nRatio v))
      obj v = fst . objD
      constraintData =  L.zip (VS.toList pV) (LA.toColumns nsVs)
      constraintF :: (Double, LA.Vector LA.R)-> LA.Vector LA.R -> (Double, LA.Vector LA.R)
      constraintF (p, nullC) v = (negate $ p + (v `LA.dot` nullC), negate nullC)
      constraintFs = fmap constraintF constraintData
      nlConstraintsD = fmap (\cf -> NLOPT.InequalityConstraint (NLOPT.Scalar cf) 1e-5) constraintFs
      nlConstraints = fmap (\cf -> NLOPT.InequalityConstraint (NLOPT.Scalar $ fst . cf) 1e-5) constraintFs
      maxIters = 500
      absTol = 1e-6
      absTolV = VS.fromList $ L.replicate n absTol
      nlStop = NLOPT.ParameterAbsoluteTolerance absTolV :| [NLOPT.MaximumEvaluations maxIters]
--      nlStop = NLOPT.ObjectiveAbsoluteTolerance absTol :| [NLOPT.MaximumEvaluations maxIters]
--      nlAlgo = NLOPT.SLSQP objD [] nlConstraintsD []
      nlAlgo = NLOPT.MMA objD nlConstraintsD
      nlProblem =  NLOPT.LocalProblem (fromIntegral n) nlStop nlAlgo
      nlSol = NLOPT.minimizeLocal nlProblem nsWs
  case nlSol of
    Left result -> PE.throw $ DED.TableMatchingException  $ "minConstrained: NLOPT solver failed: " <> show result
    Right solution -> case NLOPT.solutionResult solution of
      NLOPT.MAXEVAL_REACHED -> PE.throw $ DED.TableMatchingException $ "minConstrained: NLOPT Solver hit max evaluations (" <> show maxIters <> ")."
      NLOPT.MAXTIME_REACHED -> PE.throw $ DED.TableMatchingException $ "minConstrained: NLOPT Solver hit max time."
      _ -> do
        let oWs = NLOPT.solutionParams solution
        K.logLE K.Info $ "solution=" <> DED.prettyVector oWs
        K.logLE K.Info $ "Solution: pV + oWs <.> nVs = " <> DED.prettyVector (pV + oWs LA.<# nsVs)
        pure $ pV + oWs LA.<# nsVs

-- this aggregates over cells with the same given key
labeledRowsToVecFld :: (Ord k, BRK.FiniteSet k, VS.Storable a, Num a) => (row -> k) -> (row -> a) -> FL.Fold row (VS.Vector a)
labeledRowsToVecFld key dat = VS.fromList . M.elems . addZeroes <$> (FL.premap (\row -> (key row, dat row)) $ FL.foldByKeyMap FL.sum)
  where
    zeroMap = M.fromList $ zip (S.toList BRK.elements) $ repeat 0
    addZeroes m = M.union m zeroMap


labeledRowsToVec :: (Ord k, BRK.FiniteSet k, VS.Storable a, Foldable f, Num a) => (row -> k) -> (row -> a) -> f row -> VS.Vector a
labeledRowsToVec key dat = FL.fold (labeledRowsToVecFld key dat)

applyNSPWeightsFld :: forall outerKs ks count rs r .
                      ( DED.EnrichDataEffects r
                      , V.KnownField count
                      , outerKs V.++ (ks V.++ '[count]) ~ (outerKs V.++ ks) V.++ '[count]
                      , ks F.⊆ (ks V.++ '[count])
                      , Real (V.Snd count)
                      , Integral (V.Snd count)
                      , F.ElemOf (ks V.++ '[count]) count
                      , F.ElemOf rs count
                      , Ord (F.Record ks)
                      , BRK.FiniteSet (F.Record ks)
                      , Ord (F.Record outerKs)
                      , outerKs F.⊆ rs
                      , ks F.⊆ rs
                      , (ks V.++ '[count]) F.⊆ rs
                      , FSI.RecVec (outerKs V.++ (ks V.++ '[count]))
                      )
                   => (F.Record outerKs -> Maybe Text)
                   -> LA.Matrix LA.R
                   -> (F.Record outerKs -> FL.FoldM (K.Sem r) (F.Record (ks V.++ '[count])) (LA.Vector LA.R))
                   -> FL.FoldM (K.Sem r) (F.Record rs) (F.FrameRec (outerKs V.++ ks V.++ '[count]))
applyNSPWeightsFld logM nsVs nsWsFldF =
  let keysL = S.toList $ BRK.elements @(F.Record ks) -- these are the same for each outerK
      precomputeFld :: F.Record outerKs -> FL.FoldM (K.Sem r) (F.Record (ks V.++ '[count])) (LA.Vector LA.R, Double, LA.Vector LA.R)
      precomputeFld ok =
        let sumFld = FL.generalize $ FL.premap (realToFrac . F.rgetField @count) FL.sum
            vecFld = FL.generalize $ labeledRowsToVecFld (F.rcast @ks) (realToFrac . F.rgetField @count)
        in (,,) <$> nsWsFldF ok <*> sumFld <*> vecFld
      compute :: F.Record outerKs -> (LA.Vector LA.R, Double, LA.Vector LA.R) -> K.Sem r [F.Record (outerKs V.++ ks V.++ '[count])]
      compute ok (nsWs, vSum, v) = do
        maybe (pure ()) (\msg -> K.logLE K.Info $ msg <> " nsWs=" <> DED.prettyVector nsWs) $ logM ok
        optimalV <- fmap (VS.map (* vSum)) $ applyNSPWeightsO nsVs nsWs $ VS.map (/ vSum) v
        pure $ zipWith (\k c -> ok F.<+> k F.<+> FT.recordSingleton @count (round c)) keysL (VS.toList optimalV)
      innerFld ok = FMR.postMapM (compute ok) (precomputeFld ok)
  in  FMR.concatFoldM
        $ FMR.mapReduceFoldM
        (FMR.generalizeUnpack $ FMR.noUnpack)
        (FMR.generalizeAssign $ FMR.assignKeysAndData @outerKs @(ks V.++ '[count]))
        (FMR.ReduceFoldM $ \k -> F.toFrame <$> innerFld k)

nullSpaceVectors :: Int -> [DED.Stencil Int] -> LA.Matrix LA.R
nullSpaceVectors n stencils = LA.tr $ LA.nullspace $ DED.mMatrix n stencils

nullSpaceProjections :: (Ord k, Ord outerK, Show outerK, DED.EnrichDataEffects r, Foldable f)
                            => LA.Matrix LA.R
                            -> (row -> outerK)
                            -> (row -> k)
                            -> (row -> Int)
                            -> f row
                            -> f row
                            -> K.Sem r (Map outerK (Double, LA.Vector LA.R))
nullSpaceProjections nullVs outerKey key count actuals products = do
  let normalizedVec m s = VS.fromList $ (/ s)  <$> M.elems m
      mapData m s = (s, normalizedVec m s)
      toDatFld = mapData <$> FL.map <*> FL.premap snd FL.sum
      toMapFld = FL.premap (\r -> (outerKey r, (key r, realToFrac (count r)))) $ FL.foldByKeyMap toDatFld
      actualM = FL.fold toMapFld actuals
      prodM = FL.fold toMapFld products
      whenMatchedF _ (na, aV) (np, pV) = pure (na, nullVs LA.#> (aV - pV))
      whenMissingAF outerK _ = PE.throw $ DED.TableMatchingException $ "averageNullSpaceProjections: Missing actuals for outerKey=" <> show outerK
      whenMissingPF outerK _ = PE.throw $ DED.TableMatchingException $ "averageNullSpaceProjections: Missing product for outerKey=" <> show outerK
  MM.mergeA (MM.traverseMissing whenMissingAF) (MM.traverseMissing whenMissingPF) (MM.zipWithAMatched whenMatchedF) actualM prodM


avgNullSpaceProjections :: (Double -> Double) -> Map a (Double, LA.Vector LA.R) -> LA.Vector LA.R
avgNullSpaceProjections popWeightF m = VS.map (/ totalWeight) . VS.fromList . fmap (FL.fold FL.mean . VS.toList) . LA.toColumns . LA.fromRows . fmap weight $ M.elems m
  where
    totalWeight = FL.fold (FL.premap (popWeightF . fst) FL.sum) m / realToFrac (M.size m)
    weight (n, v) = VS.map (* popWeightF n) v



diffCovarianceFld :: forall outerK k row .
                     (Ord outerK, Ord k, BRK.FiniteSet k)
                  => (row -> outerK)
                  -> (row -> k)
                  -> (row -> LA.R)
                  -> [DED.Stencil Int]
                  -> FL.Fold row (LA.Herm LA.R)
diffCovarianceFld outerKey catKey dat stencils = snd . LA.meanCov . LA.fromRows . fmap diffProjections <$> vecsFld
  where
    allKs :: Set k = BRK.elements
    n = S.size allKs
    nullVecs = nullSpaceVectors n stencils
    mMatrix = DED.mMatrix n stencils
    mSumsV vec = mMatrix LA.#> vec
    tot = LA.sumElements
    prodV vec = LA.scale (tot vec) $ VS.fromList $ fmap (LA.prodElements . (* mSumsV vec)) $ LA.toColumns mMatrix
    diffProjections vec = nullVecs LA.#> (vec - prodV vec)
    innerFld = (\v -> LA.scale (1 / VS.sum v) v)  <$> labeledRowsToVecFld fst snd
--    vecsFld :: FL.Fold row [LA.Vector LA.R]
    vecsFld = MR.mapReduceFold
              MR.noUnpack
              (MR.assign outerKey (\row -> (catKey row, dat row)))
              (MR.foldAndLabel innerFld (\_ v -> v))


significantNullVecs :: Double
                    -> Int
                    -> [DED.Stencil Int]
                    -> LA.Herm LA.R
                    -> LA.Matrix LA.R
significantNullVecs fracCovToInclude n sts cov = LA.tr sigEVecs LA.<> nullVecs
  where
    (eigVals, eigVecs) = LA.eigSH cov
    totCov = VS.sum eigVals
    nSig = fst $ VS.foldl' (\(m, csf) c -> if csf / totCov < fracCovToInclude then (m + 1, csf + c) else (m, csf)) (0, 0) eigVals
    sigEVecs = LA.takeColumns nSig eigVecs
    nullVecs = nullSpaceVectors n sts


nullVecProjectionsModelDataFld ::  forall outerK k row d .
                                   (Ord outerK, Ord k, BRK.FiniteSet k)
                               => LA.Matrix LA.R
                               -> [DED.Stencil Int]
                               -> (row -> outerK)
                               -> (row -> k)
                               -> (row -> LA.R)
                               -> FL.Fold row d
                               -> FL.Fold row [(outerK, d, LA.Vector LA.R)]
nullVecProjectionsModelDataFld nullVecs stencils outerKey catKey count datFold =
  MR.mapReduceFold MR.noUnpack (MR.assign outerKey id) (MR.foldAndLabel innerFld (\ok (d, v) -> (ok, d, v)))
  where
    allKs :: Set k = BRK.elements
    n = S.size allKs
    mMatrix = DED.mMatrix n stencils
    mSumsV vec = mMatrix LA.#> vec
    tot = LA.sumElements
    prodV vec = LA.scale (tot vec) $ VS.fromList $ fmap (LA.prodElements . (* mSumsV vec)) $ LA.toColumns mMatrix
    projFld = (\v -> nullVecs LA.#> (v - prodV v)) <$> labeledRowsToVecFld catKey count
    innerFld = (,) <$> datFold <*> projFld


data Model1P = Model1P { m1pPWLogDensity :: Double, m1pFracGrad :: Double, m1pFracOfColor :: Double } deriving stock Show

model1DatFld :: (F.ElemOf rs DT.PWPopPerSqMile
                , F.ElemOf rs DT.Education4C
                , F.ElemOf rs DT.Race5C
                , F.ElemOf rs DT.PopCount
                )
             => FL.Fold (F.Record rs) Model1P
model1DatFld = Model1P <$> dFld <*> gFld <*> rFld
  where
    nPeople = realToFrac . view DT.popCount
    dens = view DT.pWPopPerSqMile
    wgtFld = FL.premap nPeople FL.sum
    wgtdFld f = (/) <$> FL.premap (\r -> nPeople r * f r) FL.sum <*> wgtFld
    dFld = wgtdFld dens
    fracFld f = (/) <$> FL.prefilter f wgtFld <*> wgtFld
    gFld = fracFld ((== DT.E4_CollegeGrad) . view DT.education4C)
    rFld = fracFld ((/= DT.R5_WhiteNonHispanic) . view DT.race5C)

stateG :: SMB.GroupTypeTag Text
stateG = SMB.GroupTypeTag "State"

stateGroupBuilder :: (Foldable g, Foldable f, Typeable row)
                  => (row -> Text) -> f Text -> SMB.StanGroupBuilderM (g row) () ()
stateGroupBuilder saF states = do
  projData <- SMB.addModelDataToGroupBuilder "ProjectionData" (SMB.ToFoldable id)
  SMB.addGroupIndexForData stateG projData $ SMB.makeIndexFromFoldable show saF states
  SMB.addGroupIntMapForDataSet stateG projData $ SMB.dataToIntMapFromFoldable saF states

designMatrixRow1 :: DM.DesignMatrixRow Model1P
designMatrixRow1 = DM.DesignMatrixRow "ProjModel1"
                   [DM.DesignMatrixRowPart "logDensity" 1 (VU.singleton . m1pPWLogDensity)
                   , DM.DesignMatrixRowPart "fracGrad" 1 (VU.singleton . m1pFracGrad)
                   , DM.DesignMatrixRowPart "fracOC" 1 (VU.singleton . m1pFracOfColor)
                   ]

data ProjModelData outerK md =
  ProjModelData
  {
    projDataTag :: SMB.RowTypeTag (outerK, md, VU.Vector Double)
  , nRowsE :: TE.IntE
  , nNullVecsE :: TE.IntE
  , nStatesE :: TE.IntE
  , nPredictorsE :: TE.IntE
  , predictorsE :: TE.MatrixE
  , projectionsE :: TE.MatrixE
  }

data ModelConfig md =
  ModelConfig
  {
    nRows :: Int
  , nNullVecs :: Int
  , nStates :: Int
  , designMatrixRow :: DM.DesignMatrixRow md
  , centerAlpha :: Bool
  }

projModelData :: forall md outerK . (Typeable outerK, Typeable md)
               => ModelConfig md
               -> SMB.StanBuilderM [(outerK, md, VU.Vector Double)] () (ProjModelData outerK md)
projModelData mc = do
  projData <- SMB.dataSetTag @(outerK, md, VU.Vector Double) SC.ModelData "ProjectionData"
  nRowsE' <- SBB.addFixedInt "N" mc.nRows
  nNullVecsE' <- SBB.addFixedInt "K" mc.nNullVecs
  nStatesE' <- SBB.addFixedInt "M" mc.nStates
  let projMER :: SMB.MatrixRowFromData (outerK, d, VU.Vector Double)
      projMER = SMB.MatrixRowFromData "nvp" Nothing mc.nNullVecs (\(_, _, v) -> v)
  pmE <- SBB.add2dMatrixData projData projMER Nothing Nothing
  let (_, nPredictorsE') = DM.designMatrixColDimBinding mc.designMatrixRow Nothing
  dmE <- DM.addDesignMatrix projData (contramap (\(_, md, _) -> md) mc.designMatrixRow) Nothing
  pure $ ProjModelData projData nRowsE' nNullVecsE' nStatesE' nPredictorsE' dmE pmE

-- given K null vectors, S states, and D predictors
data ProjModelParameters =
  ProjModelParameters
  {
    pmpAlpha :: TE.MatrixE -- Matrix(K,S)
--  , pmpMuAlpha :: TE.VectorE -- Vector(K)
--  , pmpSigmaAlpha :: TE.VectorE -- Vector(K)
  , pmpTheta :: TE.MatrixE -- Matrix(K,D)
--  , pmpMuBeta :: TE.MatrixE -- Matrix(K,D)
--  , pmpSigmaBeta :: TE.MatrixE -- Matrix(K,D)
  }

projModelParameters :: ModelConfig md -> ProjModelData outerK md -> SMB.StanBuilderM [(outerK, md, VU.Vector Double)] () ProjModelParameters
projModelParameters mc pmd = do
  let stdNormalDWA = TE.DensityWithArgs SF.normalS (TE.realE 0 :> TE.realE 1 :> TNil)
  muAlphaP <- DAG.simpleParameterWA
             (TE.NamedDeclSpec "muAlpha" $ TE.vectorSpec pmd.nNullVecsE [])
             stdNormalDWA
  sigmaAlphaP <-  DAG.simpleParameterWA
                  (TE.NamedDeclSpec "sigmaAlpha" $ TE.vectorSpec pmd.nNullVecsE [TE.lowerM $ TE.realE 0])
                  stdNormalDWA
  let alphaNDS = TE.NamedDeclSpec "alpha" $ TE.matrixSpec pmd.nNullVecsE pmd.nStatesE []
      alphaPs =  DAG.build muAlphaP :> DAG.build sigmaAlphaP :> TNil
      atNV x k = TE.sliceE TEI.s0 k x
      loopNVs = TE.for "k" (TE.SpecificNumbered (TE.intE 1) pmd.nNullVecsE)
      diagPreMult rv m = TE.functionE SF.diagPreMultiply (rv :> m :> TNil)
      rowsOf nColsE cv = diagPreMult (TE.transposeE cv) (TE.functionE SF.rep_matrix (TE.realE 1 :> TE.functionE SF.size (cv :> TNil) :> nColsE :> TNil))
  alphaP <- case mc.centerAlpha of
    True -> DAG.addBuildParameter
            $ DAG.UntransformedP alphaNDS [] alphaPs
            $ \(muAlphaE :> sigmaAlphaE :> TNil) m
              -> TE.addStmt
                 $ loopNVs
                 $ \k -> [TE.sample (m `atNV` k) SF.normalS (muAlphaE `atNV` k :> sigmaAlphaE `atNV` k :> TNil)]
    False -> DAG.withIIDRawMatrix alphaNDS Nothing stdNormalDWA alphaPs
            $ \(muAlphaE :> sigmaAlphaE :> TNil) rawM -> rowsOf pmd.nStatesE muAlphaE `TE.plusE` diagPreMult (TE.transposeE sigmaAlphaE) rawM
  -- for now all the thetas are iid std normals
  thetaP <- DAG.iidMatrixP
           (TE.NamedDeclSpec "theta" $ TE.matrixSpec pmd.nNullVecsE pmd.nPredictorsE [])
           []
           (DAG.given (TE.realE 0) :> DAG.given (TE.realE 1) :> TNil)
           SF.normalS
  let f = DAG.parameterTagExpr
  pure $ ProjModelParameters (f alphaP) (f thetaP)

projModel :: (Typeable outerK, Typeable md) => ModelConfig md -> SMB.StanBuilderM [(outerK, md, VU.Vector Double)] () ()
projModel mc = do
  mData <- projModelData mc
  mParams <- projModelParameters mc mData
  let betaTrNDS = TE.NamedDeclSpec "betaTr" $ TE.matrixSpec mData.nPredictorsE mData.nNullVecsE []
      betaNDS  = TE.NamedDeclSpec "beta" $ TE.matrixSpec mData.nNullVecsE mData.nPredictorsE []
      projectionsTrNDS = TE.NamedDeclSpec "nvpTr" $ TE.matrixSpec mData.nNullVecsE mData.nRowsE []
  (dmQ, dmR, dmRI, mBetaTr) <- DM.thinQR mData.predictorsE "DM" $ Just (TE.transposeE mParams.pmpTheta, betaTrNDS)
  betaTrE <- SMB.stanBuildMaybe "projModel: mBeta is set to Nothing in thinQR!" mBetaTr
  projectionsTrE <- SMB.inBlock SMB.SBTransformedData $ SMB.addFromCodeWriter $ TE.declareRHSNW projectionsTrNDS $ TE.transposeE mData.projectionsE
  SMB.inBlock SMB.SBModel $ SMB.addFromCodeWriter $ do
    betaE <- TE.declareRHSNW betaNDS $ TE.transposeE betaTrE
    let atNV x k = TE.sliceE TEI.s0 k x
        byState = TE.indexE TEI.s0 (SMB.byGroupIndexE mData.projDataTag stateG)
        loopNVs = TE.for "k" (TE.SpecificNumbered (TE.intE 1) mData.nNullVecsE)
        loopBody k = TE.writerL' $ do
          let muE = byState (mParams.pmpAlpha `atNV` k) `TE.plusE` (betaE `atNV` k `TE.timesE` dmQ)
              sigmaE = TE.functionE SF.rep_row_vector (TE.realE 1 :> mData.nRowsE :> TNil) -- this shouldn't be in model block
          TE.addStmt $ TE.sample (projectionsTrE `atNV` k) SF.normal (muE :> sigmaE :> TNil)
    TE.addStmt $ loopNVs loopBody

stencils ::  forall a b.
             (Ord a
             , Ord b
             , BRK.FiniteSet b
             )
         => (b -> a) -> [DED.Stencil Int]
stencils bFromA = M.elems $ FL.fold (DED.subgroupStencils bFromA) $ S.toList BRK.elements

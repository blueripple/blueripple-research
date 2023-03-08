{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE UnicodeSyntax #-}
{-# LANGUAGE StandaloneDeriving #-}

module BlueRipple.Model.Demographic.TableProducts
  (
    module BlueRipple.Model.Demographic.TableProducts
  )
where

import qualified BlueRipple.Configuration as BR
import qualified BlueRipple.Utilities.KnitUtils as BRKU
import qualified BlueRipple.Model.Demographic.DataPrep as DDP
import qualified BlueRipple.Model.Demographic.EnrichData as DED

import qualified BlueRipple.Data.Keyed as BRK
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.GeographicTypes as GT

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
import qualified Data.IntMap.Strict as IM
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
import qualified Data.Vector as V
import qualified Data.Vector.Storable as VS
import qualified Data.Vector.Unboxed as VU

import Control.Lens (view, (^.))
import Control.Monad.Except (throwError)
import GHC.TypeLits (Symbol)
import Graphics.Vega.VegaLite (density)

import qualified CmdStan as CS
import qualified Stan.ModelBuilder as SMB
import qualified Stan.ModelRunner as SMR
import qualified Stan.ModelConfig as SC
import qualified Stan.Parameters as SP
import qualified Stan.RScriptBuilder as SR
import qualified Stan.ModelBuilder.BuildingBlocks as SBB
import qualified Stan.ModelBuilder.DesignMatrix as DM
import qualified Stan.ModelBuilder.Distributions as SDI
import qualified Stan.ModelBuilder.TypedExpressions.Types as TE
import qualified Stan.ModelBuilder.TypedExpressions.Statements as TE
import qualified Stan.ModelBuilder.TypedExpressions.Indexing as TEI
import qualified Stan.ModelBuilder.TypedExpressions.Operations as TEO
import qualified Stan.ModelBuilder.TypedExpressions.DAG as DAG
import qualified Stan.ModelBuilder.TypedExpressions.StanFunctions as SF
import qualified Stan.ModelBuilder.Distributions as SD
import Stan.ModelBuilder.TypedExpressions.TypedList (TypedList(..))
import qualified Flat

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
    pim = LA.pinv mMatrix
    mProd = pim LA.<> mMatrix
--    mSumsV vec = mMatrix LA.#> vec
--    tot = LA.sumElements
--    prodV vec = LA.scale (tot vec) $ VS.fromList $ fmap (LA.prodElements . (* mSumsV vec)) $ LA.toColumns mMatrix
    diffProjections vec = nullVecs LA.#> (vec - mProd LA.#> vec)
    innerFld = (\v -> LA.scale (1 / VS.sum v) v) <$> labeledRowsToVecFld fst snd
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


type ProjDataRow outerK md = (outerK, md Double, LA.Vector LA.R)

data ProjData outerK md =
  ProjData
  {
    pdNNullVecs :: Int
  , pdNPredictors :: Int
  , pdRows :: [ProjDataRow outerK md]
  }

nullVecProjectionsModelDataFld ::  forall outerK k row md .
                                   (Ord outerK, Ord k, BRK.FiniteSet k)
                               => LA.Matrix LA.R
                               -> [DED.Stencil Int]
                               -> (row -> outerK)
                               -> (row -> k)
                               -> (row -> LA.R)
                               -> FL.Fold row md
                               -> FL.Fold row [(outerK, md, LA.Vector LA.R)]
nullVecProjectionsModelDataFld nullVecs stencils outerKey catKey count datFold =
  MR.mapReduceFold MR.noUnpack (MR.assign outerKey id) (MR.foldAndLabel innerFld (\ok (d, v) -> (ok, d, v)))
  where
    allKs :: Set k = BRK.elements
    n = S.size allKs
    mMatrix = DED.mMatrix n stencils
    pim = LA.pinv mMatrix
    mProd = pim LA.<> mMatrix
--    mSumsV vec = mMatrix LA.#> vec
--    tot = LA.sumElements
--    prodV vec = LA.scale (1 / tot vec) $ VS.fromList $ fmap (LA.prodElements . (* mSumsV vec)) $ LA.toColumns mMatrix
    projFld = (\v -> let w = VS.map (/ VS.sum v) v in nullVecs LA.#> (w - mProd LA.#> w)) <$> labeledRowsToVecFld catKey count
    innerFld = (,) <$> datFold <*> projFld


data Model1P a = Model1P { m1pPWLogDensity :: a, m1pFracGrad :: a, m1pFracOfColor :: a }
  deriving stock (Show, Generic)
  deriving anyclass Flat.Flat

model1DatFld :: (F.ElemOf rs DT.PWPopPerSqMile
                , F.ElemOf rs DT.Education4C
                , F.ElemOf rs DT.Race5C
                , F.ElemOf rs DT.PopCount
                )
             => FL.Fold (F.Record rs) (Model1P Double)
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

{-
model1MeanFld :: FL.Fold (Model1P Double) (Model1P Double)
model1MeanFld = Model1P
                <$> FL.premap m1pPWLogDensity FL.mean
                <*> FL.premap m1pFracGrad FL.mean
                <*> FL.premap m1pFracOfColor FL.mean
-}

model1ToList :: Model1P a -> [a]
model1ToList (Model1P x y z) = [x, y, z]

model1FromList :: [a] -> Either Text (Model1P a)
model1FromList as = case as of
  [x, y, z] -> Right $ Model1P x y z
  _ -> Left "model1FromList: wrong size list given (n /= 3)"

data ModelResult g (b :: Type -> Type) = ModelResult { mrGeoAlpha :: Map g [Double], mrSI :: b [(Double, Double)] }
  deriving stock (Generic)

deriving stock instance (Show g, Show (b [(Double, Double)])) => Show (ModelResult g b)
deriving anyclass instance (Ord g, Flat.Flat g, Flat.Flat (b [(Double, Double)])) => Flat.Flat (ModelResult g b)

--deriving
--deriving instance

--deriving instance (Flat.Flat g, Flat.Flat (b (Double, Double))) => Flat.Flat (ModelResult g b)

stateG :: SMB.GroupTypeTag Text
stateG = SMB.GroupTypeTag "State"

stateGroupBuilder :: (Foldable f, Typeable outerK, Typeable md)
                  => (outerK -> Text) -> f Text -> SMB.StanGroupBuilderM (ProjData outerK md) () ()
stateGroupBuilder saF states = do
  let ok (x, _, _) = x
  projData <- SMB.addModelDataToGroupBuilder "ProjectionData" (SMB.ToFoldable pdRows)
  SMB.addGroupIndexForData stateG projData $ SMB.makeIndexFromFoldable show (saF . ok) states
  SMB.addGroupIntMapForDataSet stateG projData $ SMB.dataToIntMapFromFoldable (saF . ok) states

emptyDM :: DM.DesignMatrixRow (Model1P Double)
emptyDM = DM.DesignMatrixRow "EDM" []

designMatrixRow1 :: DM.DesignMatrixRow (Model1P Double)
designMatrixRow1 = DM.DesignMatrixRow "PM1"
                   [DM.DesignMatrixRowPart "logDensity" 1 (VU.singleton . m1pPWLogDensity)
                   , DM.DesignMatrixRowPart "fracGrad" 1 (VU.singleton . m1pFracGrad)
                   , DM.DesignMatrixRowPart "fracOC" 1 (VU.singleton . m1pFracOfColor)
                   ]

data ProjModelData outerK md =
  ProjModelData
  {
    projDataTag :: SMB.RowTypeTag (ProjDataRow outerK md)
  , nNullVecsE :: TE.IntE
  , nPredictorsE :: TE.IntE
  , predictorsE :: TE.MatrixE
  , projectionsE :: TE.MatrixE
  }

data AlphaModel = AlphaSimple | AlphaHierCentered | AlphaHierNonCentered deriving stock (Show)

alphaModelText :: AlphaModel -> Text
alphaModelText AlphaSimple = "AS"
alphaModelText AlphaHierCentered = "AHC"
alphaModelText AlphaHierNonCentered = "AHNC"

data Distribution = NormalDist | CauchyDist | StudentTDist

distributionText :: Distribution -> Text
distributionText CauchyDist = "cauchy"
distributionText NormalDist = "normal"
distributionText StudentTDist = "studentT"

data ModelConfig md =
  ModelConfig
  {
    nNullVecs :: Int
  , standardizeNVs :: Bool
  , designMatrixRow :: DM.DesignMatrixRow (md Double)
  , alphaModel :: AlphaModel
  , distribution :: Distribution
  , mdToList :: md Double -> [Double]
  , mdFromList :: [[(Double, Double)]] -> Either Text (md [(Double, Double)])
  }

modelText :: ModelConfig md -> Text
modelText mc = distributionText mc.distribution <> "_" <> mc.designMatrixRow.dmName <> "_" <> alphaModelText mc.alphaModel

dataText :: ModelConfig md -> Text
dataText mc = mc.designMatrixRow.dmName <> "_NV" <> show mc.nNullVecs

projModelData :: forall md outerK . (Typeable outerK, Typeable md)
               => ModelConfig md
               -> SMB.StanBuilderM (ProjData outerK md) () (ProjModelData outerK md)
projModelData mc = do
  projData <- SMB.dataSetTag @(ProjDataRow outerK md) SC.ModelData "ProjectionData"
  let projMER :: SMB.MatrixRowFromData (ProjDataRow outerK md) --(outerK, md Double, VS.Vector Double)
      projMER = SMB.MatrixRowFromData "nvp" Nothing mc.nNullVecs (\(_, _, v) -> VU.convert v)
  pmE <- SBB.add2dMatrixData projData projMER Nothing Nothing
  let nNullVecsE' = SMB.mrfdColumnsE projMER
      (_, nPredictorsE') = DM.designMatrixColDimBinding mc.designMatrixRow Nothing
  dmE <- if DM.rowLength mc.designMatrixRow > 0
         then DM.addDesignMatrix projData (contramap (\(_, md, _) -> md) mc.designMatrixRow) Nothing
         else pure $ TE.namedE "ERROR" TE.SMat -- this shouldn't show up in stan code at all
  pure $ ProjModelData projData nNullVecsE' nPredictorsE' dmE pmE

-- given K null vectors, S states, and D predictors
-- alpha, theta, sigma
-- alpha is a K row-vector or S x K matrix
data Alpha = SimpleAlpha TE.RVectorE | HierarchicalAlpha TE.MatrixE
-- theta is a D x K matrix (or Nothing)
newtype Theta = Theta (Maybe TE.MatrixE)
-- sigma is a K row-vector
newtype Sigma = Sigma {unSigma :: TE.RVectorE }

newtype Nu = Nu { unNu :: TE.RVectorE }

data ProjModelParameters where
  NormalProjModelParameters :: Alpha -> Theta -> Sigma -> ProjModelParameters
  CauchyProjModelParameters :: Alpha -> Theta -> Sigma -> ProjModelParameters
  StudentTProjModelParameters :: Alpha -> Theta -> Sigma -> Nu -> ProjModelParameters

paramTheta :: ProjModelParameters -> Theta
paramTheta (NormalProjModelParameters _ t _) = t
paramTheta (CauchyProjModelParameters _ t _) = t
paramTheta (StudentTProjModelParameters _ t _ _) = t

projModelParameters :: ModelConfig md -> ProjModelData outerK md -> SMB.StanBuilderM (ProjData outerK md) () ProjModelParameters
projModelParameters mc pmd = do
  let stdNormalDWA :: (TE.TypeOneOf t [TE.EReal, TE.ECVec, TE.ERVec], TE.GenSType t) => TE.DensityWithArgs t
      stdNormalDWA = TE.DensityWithArgs SF.std_normal TNil --(TE.realE 0 :> TE.realE 1 :> TNil)
      f = DAG.parameterTagExpr
      numPredictors = DM.rowLength mc.designMatrixRow
  -- for now all the thetas are iid std normals

  theta <- if numPredictors > 0 then
               (Theta . Just . f)
               <$> DAG.iidMatrixP
               (TE.NamedDeclSpec "theta" $ TE.matrixSpec pmd.nPredictorsE pmd.nNullVecsE [])
               [] TNil
               SF.std_normal
             else pure $ Theta Nothing
  sigma <-  (Sigma . f)
             <$> DAG.simpleParameterWA
             (TE.NamedDeclSpec "sigma" $ TE.rowVectorSpec pmd.nNullVecsE [TE.lowerM $ TE.realE 0])
             stdNormalDWA
  let nStatesE = SMB.groupSizeE stateG
      hierAlphaNDS = TE.NamedDeclSpec "alpha" $ TE.matrixSpec nStatesE pmd.nNullVecsE []
      fstI x k = TE.sliceE TEI.s0 k x
      loopNVs = TE.for "k" (TE.SpecificNumbered (TE.intE 1) pmd.nNullVecsE)
      diagPostMult m cv = TE.functionE SF.diagPostMultiply (m :> cv :> TNil)
      rowsOf nRowsE rv = diagPostMult (TE.functionE SF.rep_matrix (TE.realE 1 :> nRowsE :> TE.functionE SF.size (rv :> TNil) :> TNil)) (TE.transposeE rv)
--      colsOf nColsE cv = diagPostMult (TE.functionE SF.rep_matrix (TE.realE 1 :> TE.functionE SF.size (cv :> TNil) :> nColsE) cv :> TNil)
      hierAlphaPs = do
        muAlphaP <- DAG.simpleParameterWA
                    (TE.NamedDeclSpec "muAlpha" $ TE.rowVectorSpec pmd.nNullVecsE [])
                    stdNormalDWA
        sigmaAlphaP <-  DAG.simpleParameterWA
                        (TE.NamedDeclSpec "sigmaAlpha" $ TE.rowVectorSpec pmd.nNullVecsE [TE.lowerM $ TE.realE 0])
                        stdNormalDWA
        pure (DAG.build muAlphaP :> DAG.build sigmaAlphaP :> TNil)
  alpha <- case mc.alphaModel of
    AlphaSimple -> do
      SimpleAlpha . f
        <$> DAG.simpleParameterWA
           (TE.NamedDeclSpec "alpha" $ TE.rowVectorSpec pmd.nNullVecsE [])
           stdNormalDWA
    AlphaHierCentered -> do
      alphaPs <- hierAlphaPs
      fmap (HierarchicalAlpha . f)
        $ DAG.addBuildParameter
        $ DAG.UntransformedP hierAlphaNDS [] alphaPs
        $ \(muAlphaE :> sigmaAlphaE :> TNil) m
          -> TE.addStmt
             $ loopNVs
             $ \k -> [TE.sample (m `fstI` k) SF.normalS (muAlphaE `fstI` k :> sigmaAlphaE `fstI` k :> TNil)]
    AlphaHierNonCentered -> do
      alphaPs <- hierAlphaPs
      fmap (HierarchicalAlpha . f)
        $ DAG.withIIDRawMatrix hierAlphaNDS Nothing stdNormalDWA alphaPs
        $ \(muAlphaE :> sigmaAlphaE :> TNil) rawM -> rowsOf nStatesE muAlphaE `TE.plusE` diagPostMult rawM (TE.transposeE sigmaAlphaE)
  case mc.distribution of
    NormalDist -> pure $ NormalProjModelParameters alpha theta sigma
    CauchyDist -> pure $ CauchyProjModelParameters alpha theta sigma
    StudentTDist -> do
      let kVectorOf x = TE.functionE SF.rep_row_vector (TE.realE x :> pmd.nNullVecsE :> TNil)
      nu <-  (Nu . f)
             <$> DAG.simpleParameterWA
             (TE.NamedDeclSpec "nu" $ TE.rowVectorSpec pmd.nNullVecsE [TE.lowerM $ TE.realE 0])
             (TE.DensityWithArgs SF.gamma (kVectorOf 2 :> kVectorOf 0.1 :> TNil))
      pure $ StudentTProjModelParameters alpha theta sigma nu

data RunConfig = RunConfig { rcIncludePPCheck :: Bool, rcIncludeLL :: Bool }

-- not returning anything for now
projModel :: (Typeable outerK, Typeable md)
          => RunConfig -> ModelConfig md -> SMB.StanBuilderM (ProjData outerK md) () ()
projModel rc mc = do
  mData <- projModelData mc
  mParams <- projModelParameters mc mData
  let betaNDS = TE.NamedDeclSpec "beta" $ TE.matrixSpec mData.nPredictorsE mData.nNullVecsE []
      nRowsE = SMB.dataSetSizeE mData.projDataTag
      fstI x k = TE.sliceE TEI.s0 k x
      sndI x k = TE.sliceE TEI.s1 k x
      loopNVs = TE.for "k" (TE.SpecificNumbered (TE.intE 1) mData.nNullVecsE)
  (predM, centerF, mBeta) <- case paramTheta mParams of
    Theta (Just thetaE) -> do
      (centeredPredictorsE, centerF) <- DM.centerDataMatrix DM.DMCenterOnly mData.predictorsE Nothing "DM"
      (dmQ, _, _, mBeta) <- DM.thinQR centeredPredictorsE "DM" $ Just (thetaE, betaNDS)
      pure (dmQ, centerF, mBeta)
    Theta Nothing -> pure (TE.namedE "ERROR" TE.SMat, \_ x _ -> pure x, Nothing)
  (nvps, inverseF) <- case mc.standardizeNVs of
    True -> SMB.inBlock SMB.SBTransformedData $ SMB.addFromCodeWriter $ do
      let nvVecDS t = TE.NamedDeclSpec t $ TE.rowVectorSpec mData.nNullVecsE []
      sds <- TE.declareNW (nvVecDS "nvpSDs")
      stdNVPs <- TE.declareNW (TE.NamedDeclSpec "stdNVPs" $ TE.matrixSpec nRowsE mData.nNullVecsE [])
      TE.addStmt
        $ loopNVs
        $ \k -> let colk :: TE.UExpr t -> TE.UExpr (TEI.Sliced (TE.S TE.Z) t)
                    colk = flip sndI k --TE.sliceE TEI.s1 k x
                in
                  [ (sds `fstI` k) `TE.assign` TE.functionE SF.sd (colk mData.projectionsE :> TNil)
                  , colk stdNVPs `TE.assign` (colk mData.projectionsE `TE.divideE` (sds `fstI` k))]
      let inverse :: (t ~ TEO.BinaryResultT TEO.BMultiply TE.EReal t) => TE.IntE -> TE.UExpr t -> TE.UExpr t --TE.UExpr (TEO.BinaryResultT TEO.BMultiply TE.EReal t)
          inverse k psCol = sds `fstI` k `TE.timesE` psCol
      pure (stdNVPs, inverse)
    False -> pure (mData.projectionsE, const id)

  -- model
  let reIndexByState = TE.indexE TEI.s0 (SMB.byGroupIndexE mData.projDataTag stateG)
      muE :: Alpha -> Theta -> TE.IntE -> TE.VectorE
      muE a t k =  case a of
       SimpleAlpha alpha -> case t of
         Theta Nothing -> TE.functionE SF.rep_vector (alpha `fstI` k :> nRowsE :> TNil)
         Theta (Just theta) -> alpha `fstI` k `TE.plusE` (predM `TE.timesE` (theta `sndI` k))
       HierarchicalAlpha alpha -> case t of
         Theta Nothing -> reIndexByState (alpha `sndI` k)
         Theta (Just theta) -> reIndexByState (alpha `sndI` k) `TE.plusE` (predM `TE.timesE` (theta `sndI` k))
      sigmaE :: Sigma -> TE.IntE -> TE.VectorE
      sigmaE s k = TE.functionE SF.rep_vector (unSigma s `fstI` k :> nRowsE :> TNil)

  let ppF :: Int
          -> ((TE.IntE -> TE.ExprList xs) -> TE.IntE -> TE.UExpr TE.EReal)
          -> (TE.IntE -> TE.CodeWriter (TE.IntE -> TE.ExprList xs))
          -> SMB.StanBuilderM (ProjData outerK md) () (TE.ArrayE TE.EReal)
      ppF k rngF rngPSCW =  SBB.generatePosteriorPrediction'
                            mData.projDataTag
                            (TE.NamedDeclSpec ("predProj_" <> show k) $ TE.array1Spec nRowsE $ TE.realSpec [])
                            rngF
                            (rngPSCW (TE.intE k))
                            --               (pure $ \nE -> muE kE `fstI` nE :> unSigma mParams.pSigma `fstI` kE :> TNil)
                            (\_ p -> inverseF (TE.intE k) p)
  let (sampleStmtF, ppStmtF) = case mParams of
        NormalProjModelParameters a t s ->
          let ssF e k = TE.sample e SF.normal (muE a t k :> sigmaE s k :> TNil)
              rF f nE = TE.functionE SF.normal_rng (f nE)
              rpF k = pure $ \nE -> muE a t k `fstI` nE :> sigmaE s k `fstI` nE :> TNil
          in (ssF, \n -> ppF n rF rpF)
        CauchyProjModelParameters a t s ->
          let ssF e k = TE.sample e SF.cauchy (muE a t k :> sigmaE s k :> TNil)
              rF f nE = TE.functionE SF.cauchy_rng (f nE)
              rpF k = pure $ \nE -> muE a t k `fstI` nE :> sigmaE s k `fstI` nE :> TNil
          in (ssF, \n -> ppF n rF rpF)
        StudentTProjModelParameters a t s n ->
          let nu :: Nu -> TE.IntE -> TE.VectorE
              nu n k = TE.functionE SF.rep_vector (unNu n `fstI` k :> nRowsE :> TNil)
              ssF e k = TE.sample e SF.student_t (nu n k :> muE a t k :> sigmaE s k :> TNil)
              rF f nE = TE.functionE SF.student_t_rng (f nE)
              rpF k  = pure $ \nE -> nu n k `fstI` nE :> muE a t k `fstI` nE :> sigmaE s k `fstI` nE :>  TNil
          in (ssF, \n -> ppF n rF rpF)

  SMB.inBlock SMB.SBModel $ SMB.addFromCodeWriter $ do
    let loopBody k = TE.writerL' $ TE.addStmt $ sampleStmtF (nvps `sndI` k) k
    TE.addStmt $ loopNVs loopBody
  -- generated quantities
  when rc.rcIncludePPCheck $ forM_ [1..mc.nNullVecs] ppStmtF
  pure ()

runProjModel :: forall (ks :: [(Symbol, Type)]) md r .
                (K.KnitEffects r
                , BRKU.CacheEffects r
                , ks F.⊆ DDP.ACSByPUMAR
                , Ord (F.Record ks)
                , BRK.FiniteSet (F.Record ks)
                , Typeable md
                , Flat.Flat (md [(Double, Double)])
                )
             => Bool
             -> BR.CommandLine
             -> RunConfig
             -> ModelConfig md
             -> LA.Matrix LA.R
             -> [DED.Stencil Int]
             -> FL.Fold (F.Record DDP.ACSByPUMAR) (md Double)
             -> K.Sem r (K.ActionWithCacheTime r (ModelResult Text md)) -- no returned result for now
runProjModel clearCaches cmdLine rc mc nullVecs stencils datFld = do
  let cacheDirE = (if clearCaches then Left else Right) "model/demographic/nullVecProjModel"
      dataName = "projectionData_" <> dataText mc
      runnerInputNames = SC.RunnerInputNames
                         ("br-2022-Demographics/stan/nullVecProj")
                         (modelText mc)
                         (Just $ SC.GQNames "pp" dataName) -- posterior prediction vars to wrap
                         dataName
  acsByPUMA_C <- DDP.cachedACSByPUMA
--  acsByPUMA <- K.ignoreCacheTime acsByPUMA_C
  let outerKey = F.rcast @[GT.StateAbbreviation, GT.PUMA]
      catKey = F.rcast @ks
      count = realToFrac . view DT.popCount
      projData acsByPUMA = ProjData mc.nNullVecs (DM.rowLength mc.designMatrixRow)
                           (FL.fold
                            (nullVecProjectionsModelDataFld nullVecs stencils outerKey catKey count datFld)
                            acsByPUMA
                           )
      modelData_C = projData <$> acsByPUMA_C
      meanSDFld :: FL.Fold Double (Double, Double) = (,) <$> FL.mean <*> FL.std
      meanSDFlds :: Int -> FL.Fold [Double] [(Double, Double)]
      meanSDFlds m = traverse (\n -> FL.premap (List.!! n) meanSDFld) [0..(m - 1)]
--        foldl' (\flds fld -> g <$> flds <*> fld) (pure []) $ replicate n meanSDFld
--        where g ls l = ls ++ [l]
  modelData <- K.ignoreCacheTime modelData_C
  let meanSDs = FL.fold (FL.premap (\(_, _, v) -> VS.toList v) $ meanSDFlds mc.nNullVecs) $ pdRows modelData
  K.logLE K.Info $ "meanSDs=" <> show meanSDs
  states <- FL.fold (FL.premap (view GT.stateAbbreviation) FL.set) <$> K.ignoreCacheTime acsByPUMA_C
  (dw, code) <-  SMR.dataWranglerAndCode modelData_C (pure ())
                (stateGroupBuilder (view GT.stateAbbreviation)  (S.toList states))
                (projModel rc mc)

  let (nNullVecs, _) = LA.size nullVecs
      unwraps = (\n -> SR.UnwrapExpr ("matrix(ncol="
                                       <> show nNullVecs
                                       <> ", byrow=TRUE, unlist(jsonData $ nvp_ProjectionData))[,"
                                       <> show n <> "]") ("obsNVP_" <> show n))
                <$> [1..nNullVecs]
  res_C <- SMR.runModel' @BRKU.SerializerC @BRKU.CacheData
           cacheDirE
           (Right runnerInputNames)
           dw
           code
           (projModelResultAction mc) --SC.DoNothing -- (stateModelResultAction mcWithId dmr)
           (SMR.ShinyStan unwraps) --(SMR.Both [SR.UnwrapNamed "successes" "yObserved"])
           modelData_C
           (pure ())
  K.logLE K.Info "projModel run complete."
  pure res_C


projModelResultAction :: forall outerK md r .
                         (K.KnitEffects r
                         , Typeable md
                         , Typeable outerK
                         )
                      => ModelConfig md
                      -> SC.ResultAction r (ProjData outerK md) () SMB.DataSetGroupIntMaps () (ModelResult Text md)
projModelResultAction mc = SC.UseSummary f where
  f summary _ modelDataAndIndexes_C _ = do
    (modelData, resultIndexesE) <- K.ignoreCacheTime modelDataAndIndexes_C
    -- compute means of predictors because model was zero-centered in them
    let mdMeansFld = FL.premap (\(_, md, _) -> mc.mdToList md)
                    $ traverse (\n -> FL.premap (List.!! n) FL.mean) [0..(mc.nNullVecs - 1)]
        mdMeansL = FL.fold mdMeansFld $ pdRows modelData
    stateIM <- K.knitEither
      $ resultIndexesE >>= SMB.getGroupIndex (SMB.RowTypeTag @(ProjData outerK md) SC.ModelData "ProjectionData") stateG
    let allStates = IM.elems stateIM
        getVector n = K.knitEither $ SP.getVector . fmap CS.mean <$> SP.parse1D n (CS.paramStats summary)
        getMatrix n = K.knitEither $ fmap CS.mean <$> SP.parse2D n (CS.paramStats summary)
    geoMap <- case mc.alphaModel of
      AlphaSimple -> do
        alphaV <- getVector "alpha" -- states by nNullvecs
        pure $ M.fromList $ zip allStates (repeat $ V.toList alphaV)
      _ -> do
        alphaVs <- getMatrix "alpha"
        let mRowToList cols row = fmap (\c -> SP.getIndexed alphaVs (row, c)) $ [0..(cols - 1)]
        pure $ M.fromList $ fmap (\(row, sa) -> (sa, mRowToList mc.nNullVecs row)) $ IM.toList stateIM
    betaSIL <- case DM.rowLength mc.designMatrixRow of
      0 -> pure $ replicate mc.nNullVecs []
      p -> do
        betaVs <- getMatrix "beta" -- ps by nNullVecs
        let mColToList rows col = fmap (\r -> SP.getIndexed betaVs (r, col)) [0..(rows - 1)]
        pure $ transp $ fmap (\m -> zip (mColToList p m) mdMeansL) [0..(mc.nNullVecs - 1)]
    betaSI <- K.knitEither $ mc.mdFromList betaSIL
    pure $ ModelResult geoMap betaSI

transp :: [[a]] -> [[a]]
transp = go [] where
  go x [] = fmap reverse x
  go [] (r : rs) = go (fmap (: []) r) rs
  go x (r :rs) = go (List.zipWith (:) r x) rs


stencils ::  forall a b.
             (Ord a
             , Ord b
             , BRK.FiniteSet b
             )
         => (b -> a) -> [DED.Stencil Int]
stencils bFromA = M.elems $ FL.fold (DED.subgroupStencils bFromA) $ S.toList BRK.elements

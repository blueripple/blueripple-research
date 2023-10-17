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

module BlueRipple.Model.Demographic.TPModel3
  (
    module BlueRipple.Model.Demographic.TPModel3
  )
where

import qualified BlueRipple.Configuration as BR
import qualified BlueRipple.Utilities.KnitUtils as BRKU
import qualified BlueRipple.Model.Demographic.DataPrep as DDP
import qualified BlueRipple.Model.Demographic.EnrichData as DED
import qualified BlueRipple.Model.Demographic.MarginalStructure as DMS
import qualified BlueRipple.Model.Demographic.TableProducts as DTP

import qualified BlueRipple.Data.Keyed as BRK
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.GeographicTypes as GT

import qualified Knit.Report as K hiding (elements)

import qualified Control.MapReduce.Simple as MR

import qualified Control.Foldl as FL
import qualified Data.IntMap.Strict as IM
import qualified Data.Map.Strict as M
import qualified Data.Set as S
import Data.Type.Equality (type (~))

import qualified Data.List as List
import qualified Frames as F
import qualified Frames.Melt as F
import qualified Frames.Serialize as FS
import qualified Numeric.LinearAlgebra as LA
import qualified Numeric
import qualified Data.Vinyl as Vinyl
import qualified Data.Vector as V
import qualified Data.Vector.Storable as VS
import qualified Data.Vector.Unboxed as VU
import qualified Text.Show
import Control.Lens (Lens', view, over, (^.), _2)
import GHC.TypeLits (Symbol)

import qualified CmdStan as CS
import qualified Stan.ModelBuilder as SMB
import qualified Stan.ModelRunner as SMR
import qualified Stan.ModelConfig as SC
import qualified Stan.Parameters as SP
import qualified Stan.RScriptBuilder as SR
import qualified Stan.ModelBuilder.BuildingBlocks as SBB
import qualified Stan.ModelBuilder.Distributions as SMD
import qualified Stan.ModelBuilder.DesignMatrix as DM
import qualified Stan.ModelBuilder.TypedExpressions.Types as TE
import qualified Stan.ModelBuilder.TypedExpressions.Statements as TE
import qualified Stan.ModelBuilder.TypedExpressions.Indexing as TEI
import qualified Stan.ModelBuilder.TypedExpressions.Operations as TEO
import qualified Stan.ModelBuilder.TypedExpressions.DAG as DAG
import qualified Stan.ModelBuilder.TypedExpressions.StanFunctions as SF
import Stan.ModelBuilder.TypedExpressions.TypedList (TypedList(..))
import qualified Flat
import Flat.Instances.Vector ()

data ProjDataRow outerK =
  ProjDataRow { pdKey :: outerK, pdCovariates :: VS.Vector Double, pdCoeff ::Double }

-- NB: nullVecs we use are not the ones from SVD but a subset of a rotation of those via
-- the eigenvectors of the covariance
nullVecProjectionsModelDataFld ::  forall outerK k row w .
                                   (Ord outerK)
                               => Lens' w Double
                               -> DMS.MarginalStructure w k
                               -> DTP.NullVectorProjections k
                               -> (row -> outerK)
                               -> (row -> k)
                               -> (row -> w)
                               -> FL.Fold row (VS.Vector Double) -- covariates
                               -> FL.Fold row [(outerK, VS.Vector Double, VS.Vector Double)]
nullVecProjectionsModelDataFld wl ms nvps outerKey catKey datF datFold = case ms of
  DMS.MarginalStructure _ ptFld -> MR.mapReduceFold
                                   MR.noUnpack
                                   (MR.assign outerKey id)
                                   (MR.foldAndLabel innerFld (\ok (d, v) -> (ok, d, v)))
    where
{-      allKs :: Set k = BRK.elements
      wgtVec = VS.fromList . fmap (view wl)
      pcF :: [w] -> VS.Vector Double
      pcF =  wgtVec . fmap snd . FL.fold ptFld . zip (S.toList allKs)
      projections ws = let ws' = DMS.normalize wl ws in DTP.fullToProj nvps (wgtVec ws' - pcF ws')
      projFld = fmap projections $ DTP.labeledRowsToListFld catKey datF
-}
      projFld = DTP.diffProjectionsFromJointFld ms wl (DTP.fullToProj nvps) catKey datF
      innerFld = (,) <$> datFold <*> projFld

newtype NVProjectionRowData ks =
  NVProjectionRowData { unNVProjectionRowData :: (F.Record ks, VS.Vector Double, VS.Vector Double)} deriving stock (Generic)

instance (FS.RecFlat ok, Vinyl.RMap ok) => Flat.Flat (NVProjectionRowData ok) where
  size (NVProjectionRowData (r, cvs, ps)) = Flat.size (FS.toS r, cvs, ps)
  encode (NVProjectionRowData (r, cvs, ps)) = Flat.encode (FS.toS r, cvs, ps)
  decode = fmap (\(sr, cvsL, psL) -> NVProjectionRowData (FS.fromS sr, cvsL, psL)) Flat.decode

toProjDataRow :: Int -> (k, VS.Vector Double, VS.Vector Double) -> ProjDataRow k
toProjDataRow n (k, covariates, projections) = ProjDataRow k covariates (projections VS.! n)

cwdInnerFld :: (Ord k, BRK.FiniteSet k)
            => (F.Record rs -> k)
            -> (F.Record rs -> DMS.CellWithDensity)
            -> FL.Fold (F.Record rs) [DMS.CellWithDensity]
cwdInnerFld keyF datF = fmap M.elems $ marginalVecFld keyF
  where
    marginalVecFld f = FL.premap (\r -> (f r, datF r)) (DMS.normalizeAndFillMapFld DMS.cwdWgtLens)
-- fmap (VS.fromList . fmap DMS.cwdWgt . M.elems)

bLogit :: Double -> Double -> Double
bLogit eps x
  | x < eps = Numeric.log eps
  | x > 1 - eps = Numeric.log (1 / eps)
  | otherwise = Numeric.log (x / (1 - x))

cwdListToLogitVec :: [DMS.CellWithDensity] -> VS.Vector Double
cwdListToLogitVec = VS.fromList . fmap (bLogit 1e-10 . DMS.cwdWgt)

cwdListToLogPWDensity :: [DMS.CellWithDensity] -> Double
cwdListToLogPWDensity = --FL.fold (safeLogDiv <$> FL.premap (\cw -> DMS.cwdWgt cw * DMS.cwdDensity cw) FL.sum <*> FL.premap DMS.cwdWgt FL.sum)
  posLog . snd . FL.fold (DT.densityAndPopFld' (const 1) DMS.cwdWgt DMS.cwdDensity)

posLog :: Double -> Double
posLog z = if z < 1 then 0 else Numeric.log z

{-
safeLogDiv :: Double -> Double -> Double
safeLogDiv x y = if y < 1e-10 then 0
                 else let z = x / y
                      in if z < 1 then 0 else Numeric.log z
-}
{-
cwdCovariatesFld :: (Ord k, BRK.FiniteSet k)
                 => (F.Record rs -> k)
                 -> (F.Record rs -> DMS.CellWithDensity)
                 -> FL.Fold (F.Record rs) (VS.Vector Double)
cwdCovariatesFld keyF datF = fmap (\cws -> VS.concat [VS.singleton (safeLog $ cwdListToPWDensity cws), cwdListToLogitVec cws]) $ cwdInnerFld keyF datF
-}

mergeInnerFlds :: [FL.Fold (F.Record rs) (VS.Vector Double)] -> FL.Fold (F.Record rs) (VS.Vector Double)
mergeInnerFlds = fmap VS.concat . sequenceA

dmr :: Text -> Int -> DM.DesignMatrixRow (VS.Vector Double)
dmr t n = DM.DesignMatrixRow t [DM.DesignMatrixRowPart t n VU.convert]

-- NB: nullVecs we use are not the ones from SVD but a subset of a rotation of those via
-- the eigenvectors of the covariance
nullVecProjectionsModelDataFldCheck ::  forall outerK k row w .
                                        (Ord outerK)
                                    => Lens' w Double
                                    -> DMS.MarginalStructure w k
                                    -> DTP.NullVectorProjections k
                                    -> (row -> outerK)
                                    -> (row -> k)
                                    -> (row -> w)
                                    -> FL.Fold row (VS.Vector Double) -- covariates
                                    -> FL.Fold row [(outerK
                                                    , VS.Vector Double -- covariates
                                                    , VS.Vector Double -- nvProjections
                                                    , [(k, w)] -- product table
                                                    , [(k, w)] -- orig table
                                                    )]
nullVecProjectionsModelDataFldCheck wl ms nvps outerKey catKey datF datFold = case ms of
  DMS.MarginalStructure _ ptFld -> MR.mapReduceFold
                                   MR.noUnpack
                                   (MR.assign outerKey id)
                                   (MR.foldAndLabel innerFld (\ok (d, (v, pKWs, oKWs)) -> (ok, d, v, pKWs, oKWs)))
    where
      pcF :: [(k, w)] -> [(k, w)]
      pcF =  FL.fold ptFld
      results kws = let kws' = DMS.normalize (_2 . wl) kws -- normalized original probs
--                        n = FL.fold (FL.premap (view $ _2 . wl) FL.sum) kws
                    in (DTP.diffProjectionsFromJointKeyedList ms wl (DTP.fullToProj nvps) kws'
                      , FL.fold (DMS.marginalProductFromJointFld wl ms) kws --fmap (over (_2 . wl) (* n)) $ pcF kws -- product at original size
                      , kws
                      )
      projFld = fmap results $ DTP.labeledRowsToKeyedListFld catKey datF
      innerFld = (,) <$> datFold <*> projFld

data ProjData outerK =
  ProjData
  {
    pdNPredictors :: Int
  , pdRows :: [ProjDataRow outerK]
  }
{-
newtype RecordKey ks = RecordKey (F.Record ks)

instance Text.Show.Show (F.Record ks) => Show (RecordKey ks) where
  show (RecordKey r) = "RecordKey " <> show r

instance Eq (F.Record ks) => Eq (RecordKey ks) where
  RecordKey r1 == RecordKey r2 = r1 == r2

instance Ord (F.Record ks) => Ord (RecordKey ks) where
  compare (RecordKey r1) (RecordKey r2) = compare r1 r2

instance FS.RecFlat ks => Flat.Flat (RecordKey ks) where
  size (RecordKey k) = Flat.size $ FS.toS k
  encode (RecordKey k) = Flat.encode $ FS.toS k
  decode = fmap (RecordKey . FS.fromS) $ Flat.decode

instance BRK.FiniteSet (F.Record k) => BRK.FiniteSet (RecordKey k) where
  elements = RecordKey <$> BRK.elements @(F.Record k)
-}
data ComponentPredictor g =
  ComponentPredictor { mrGeoAlpha :: Map g Double
                     , mrSI :: V.Vector (Double, Double)
                     }
  deriving stock (Generic)

deriving anyclass instance (Ord g, Flat.Flat g) => Flat.Flat (ComponentPredictor g)

data Predictor k g = Predictor {predPTD :: DTP.ProjectionsToDiff k, predCPs :: [ComponentPredictor g] } deriving stock (Generic)

deriving anyclass instance (Ord g, Ord k, BRK.FiniteSet k, Flat.Flat g) => Flat.Flat (Predictor k g)

{-
mapPredictor :: DMS.IsomorphicKeys a b -> Predictor a g -> Predictor b g
mapPredictor ik@(DMS.IsomorphicKeys abF _) (Predictor nvps predCPs) =
  Predictor (DTP.mapNullVectorProjections ik nvps) (M.mapKeys abF $ fmap (mapCPKey abF) predCPs)
-}

modelResultNVP :: (Show g, Ord g)
               => ComponentPredictor g
               -> g
               -> VS.Vector Double
               -> Either Text Double
modelResultNVP mr g md = do
    geoAlpha <- maybeToRight ("geoAlpha lookup failed for gKey=" <> show g <> ". mrGeoAlpha=" <> show mr.mrGeoAlpha) $ M.lookup g mr.mrGeoAlpha
    let
        applyOne x (b, m) = b * (x - m)
        beta = V.sum $ V.zipWith applyOne (VS.convert md) (mrSI mr)
    pure $ geoAlpha + beta

modelResultNVPs :: (Show g, Ord g) => Predictor k g -> g -> VS.Vector Double -> Either Text [Double]
modelResultNVPs p g cvs = traverse (\mr -> modelResultNVP mr g cvs) $ predCPs p

viaNearestOnSimplex :: DTP.ProjectionsToDiff k -> VS.Vector Double -> VS.Vector Double -> K.Sem r (VS.Vector Double)
viaNearestOnSimplex ptd projWs prodV = do
  let n = VS.sum prodV
  pure $ VS.map (* n) $ DTP.projectToSimplex $ DTP.applyNSPWeights ptd projWs (VS.map (/ n) prodV)

-- NB: This function assumes you give it an ordered and complete list of (k, w) pairs
predictedJoint :: forall g k w r . (Show g, Ord g, K.KnitEffects r)
               => DTP.OptimalOnSimplexF r --(DTP.ProjectionsToDiff k -> VS.Vector Double -> VS.Vector Double -> K.Sem r (VS.Vector Double))
               -> Lens' w Double
               -> Predictor k g
               -> g
               -> VS.Vector Double
               -> [(k, w)]
               -> K.Sem r [(k, w)]
predictedJoint onSimplexM wgtLens p gk covariates keyedProduct = do
  let n = FL.fold (FL.premap (view wgtLens . snd) FL.sum) keyedProduct
      prodV = VS.fromList $ fmap (view wgtLens . snd) keyedProduct

  nvpsPrediction <- K.knitEither $ VS.fromList <$> modelResultNVPs p gk covariates

  onSimplexWgts <- onSimplexM (predPTD p) nvpsPrediction prodV --wgts DTP.projectToSimplex $ DTP.applyNSPWeights (predNVP p) nvpsPrediction (VS.map (/ n) prodV)
--      newWeights = VS.map (* n) onSimplex
  let f (newWgt, (k, w)) = (k, over wgtLens (const newWgt) w)
      predictedTable = fmap f $ zip (VS.toList onSimplexWgts) keyedProduct
      predV = VS.fromList $ fmap (view wgtLens . snd) predictedTable
      checkV = DTP.nvpConstraints (DTP.nullVectorProjections $ predPTD p) LA.#> (predV - prodV)
  K.logLE (K.Debug 1)
    $ "Region=" <> show gk
    <> "\ncovariates = " <> DED.prettyVector covariates
    <> "\npredicted projections = " <> DED.prettyVector nvpsPrediction
    <> "\npredicted projections (onSimplex) = " <> DED.prettyVector onSimplexWgts
    <> "\npredicted result = " <> DED.prettyVector predV
    <> "\nC * (predicted - product) = " <> DED.prettyVector checkV
  pure predictedTable

stateG :: SMB.GroupTypeTag Text
stateG = SMB.GroupTypeTag "State"

stateGroupBuilder :: (Foldable f, Typeable outerK)
                  => (outerK -> Text) -> f Text -> SMB.StanGroupBuilderM (ProjData outerK) () ()
stateGroupBuilder saF states = do
  projData <- SMB.addModelDataToGroupBuilder "ProjectionData" (SMB.ToFoldable pdRows)
  SMB.addGroupIndexForData stateG projData $ SMB.makeIndexFromFoldable show (saF . pdKey) states
  SMB.addGroupIntMapForDataSet stateG projData $ SMB.dataToIntMapFromFoldable (saF . pdKey) states

data ProjModelData outerK =
  ProjModelData
  {
    projDataTag :: SMB.RowTypeTag (ProjDataRow outerK)
  , nPredictorsE :: TE.IntE
  , predictorsE :: TE.MatrixE
  , projectionsE :: TE.VectorE
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

data ModelConfig =
  ModelConfig
  {
    standardizeNVs :: Bool
  , designMatrixRow :: DM.DesignMatrixRow (VS.Vector Double)
  , alphaModel :: AlphaModel
  , distribution :: Distribution
  }

modelText :: ModelConfig -> Text
modelText mc = distributionText mc.distribution <> "_" <> mc.designMatrixRow.dmName <> "_" <> alphaModelText mc.alphaModel

dataText :: ModelConfig -> Text
dataText mc = mc.designMatrixRow.dmName

projModelData :: forall outerK . Typeable outerK
              => ModelConfig
              -> SMB.StanBuilderM (ProjData outerK) () (ProjModelData outerK)
projModelData mc = do
  projData <- SMB.dataSetTag @(ProjDataRow outerK) SC.ModelData "ProjectionData"
--  let projMER :: SMB.MatrixRowFromData (ProjDataRow outerK) --(outerK, md Double, VS.Vector Double)
--      projMER = SMB.MatrixRowFromData "nvp" Nothing (modelNumNullVecs mc) (\(_, _, v) -> VU.convert v)
  pmE <- SBB.addRealData projData "projection" Nothing Nothing pdCoeff
  let (_, nPredictorsE') = DM.designMatrixColDimBinding mc.designMatrixRow Nothing
  dmE <- if DM.rowLength mc.designMatrixRow > 0
         then DM.addDesignMatrix projData (contramap pdCovariates mc.designMatrixRow) Nothing
         else pure $ TE.namedE "ERROR" TE.SMat -- this shouldn't show up in stan code at all
  pure $ ProjModelData projData nPredictorsE' dmE pmE

-- given K null vectors, S states, and D predictors
-- alpha, theta, sigma
-- alpha is a K row-vector or S x K matrix
data Alpha = SimpleAlpha (DAG.Parameter TE.EReal) | HierarchicalAlpha (DAG.Parameter TE.ECVec)
-- theta is a D x K matrix (or Nothing)
newtype Theta = Theta (Maybe (DAG.Parameter TE.ECVec))
-- sigma is a K row-vector
newtype Sigma = Sigma {unSigma :: DAG.Parameter TE.EReal}

newtype Nu = Nu { unNu :: DAG.Parameter TE.EReal }

data ProjModelParameters where
  NormalProjModelParameters :: Alpha -> Theta -> Sigma -> ProjModelParameters
  CauchyProjModelParameters :: Alpha -> Theta -> Sigma -> ProjModelParameters
  StudentTProjModelParameters :: Alpha -> Theta -> Sigma -> Nu -> ProjModelParameters

paramTheta :: ProjModelParameters -> Theta
paramTheta (NormalProjModelParameters _ t _) = t
paramTheta (CauchyProjModelParameters _ t _) = t
paramTheta (StudentTProjModelParameters _ t _ _) = t

projModelParameters :: ModelConfig -> ProjModelData outerK -> SMB.StanBuilderM (ProjData outerK) () ProjModelParameters
projModelParameters mc pmd = do
  let stdNormalDWA :: (TE.TypeOneOf t [TE.EReal, TE.ECVec, TE.ERVec], TE.GenSType t) => TE.DensityWithArgs t
      stdNormalDWA = TE.DensityWithArgs SF.std_normal TNil
      numPredictors = DM.rowLength mc.designMatrixRow
  -- for now all the thetas are iid std normals

  theta <- if numPredictors > 0 then
               (Theta . Just)
               <$> DAG.simpleParameterWA
               (TE.NamedDeclSpec "theta" $ TE.vectorSpec pmd.nPredictorsE [])
               stdNormalDWA
             else pure $ Theta Nothing
  sigma <-  Sigma
             <$> DAG.simpleParameterWA
             (TE.NamedDeclSpec "sigma" $ TE.realSpec [TE.lowerM $ TE.realE 0])
             stdNormalDWA
  let nStatesE = SMB.groupSizeE stateG
      hierAlphaNDS = TE.NamedDeclSpec "alpha" $ TE.vectorSpec nStatesE []
      hierAlphaPs = do
        muAlphaP <- DAG.simpleParameterWA
                    (TE.NamedDeclSpec "muAlpha" $ TE.realSpec [])
                    stdNormalDWA
        sigmaAlphaP <-  DAG.simpleParameterWA
                        (TE.NamedDeclSpec "sigmaAlpha" $ TE.realSpec [TE.lowerM $ TE.realE 0])
                        stdNormalDWA
        pure (muAlphaP :> sigmaAlphaP :> TNil)
  alpha <- case mc.alphaModel of
    AlphaSimple -> do
      fmap SimpleAlpha
        $ DAG.simpleParameterWA
        (TE.NamedDeclSpec "alpha" $ TE.realSpec [])
        stdNormalDWA
    AlphaHierCentered -> do
      alphaPs <- hierAlphaPs
      fmap HierarchicalAlpha
        $ DAG.addBuildParameter
        $ DAG.UntransformedP hierAlphaNDS [] alphaPs
        $ \(muAlphaE :> sigmaAlphaE :> TNil) m
          -> TE.addStmt $ TE.sample m SF.normalS (muAlphaE :> sigmaAlphaE :> TNil)
    AlphaHierNonCentered -> do
      alphaPs <- hierAlphaPs
      let rawNDS = TE.NamedDeclSpec (TE.declName hierAlphaNDS <> "_raw") $ TE.decl hierAlphaNDS
      rawAlphaP <- DAG.simpleParameterWA rawNDS stdNormalDWA
      fmap HierarchicalAlpha
        $ DAG.addBuildParameter
        $ DAG.TransformedP hierAlphaNDS []
        (rawAlphaP :> alphaPs) DAG.TransformedParametersBlock
        (\(rawE :> muAlphaE :> muSigmaE :> TNil) -> DAG.DeclRHS $ muAlphaE `TE.plusE` (muSigmaE `TE.timesE` rawE))
        TNil (\_ _ -> pure ())
  case mc.distribution of
    NormalDist -> pure $ NormalProjModelParameters alpha theta sigma
    CauchyDist -> pure $ CauchyProjModelParameters alpha theta sigma
    StudentTDist -> do
--      let kVectorOf x = TE.functionE SF.rep_row_vector (TE.realE x :> pmd.nNullVecsE :> TNil)
      nu <-  fmap Nu
             $ DAG.simpleParameterWA
             (TE.NamedDeclSpec "nu" $ TE.realSpec [TE.lowerM $ TE.realE 0])
             (TE.DensityWithArgs SF.gamma (TE.realE 2 :> TE.realE 0.1 :> TNil))
      pure $ StudentTProjModelParameters alpha theta sigma nu

data RunConfig = RunConfig { nvIndex :: Int, rcIncludePPCheck :: Bool, rcIncludeLL :: Bool, statesM :: Maybe (Text, [Text]) }

-- not returning anything for now
projModel :: (Typeable outerK)
          => RunConfig
          -> ModelConfig
          -> SMB.StanBuilderM (ProjData outerK) () ()
projModel rc mc = do
  mData <- projModelData mc
  mParams <- projModelParameters mc mData
  let betaNDS = TE.NamedDeclSpec "beta" $ TE.vectorSpec mData.nPredictorsE []
      nRowsE = SMB.dataSetSizeE mData.projDataTag
      fstI x k = TE.sliceE TEI.s0 k x
      pExpr = DAG.parameterExpr

--      sndI x k = TE.sliceE TEI.s1 k x
--      loopNVs = TE.for "k" (TE.SpecificNumbered (TE.intE 1) mData.nNullVecsE)
  (predM, _centerF, _mBeta) <- case paramTheta mParams of
    Theta (Just thetaP) -> do
      (centeredPredictorsE, centerF) <- DM.centerDataMatrix DM.DMCenterOnly mData.predictorsE Nothing "DM"
      (dmQ, _, _, mBeta) <- DM.thinQR centeredPredictorsE "DM" $ Just (pExpr thetaP, betaNDS)
      pure (dmQ, centerF, mBeta)
    Theta Nothing -> pure (TE.namedE "ERROR" TE.SMat, \_ x _ -> pure x, Nothing)
  (stdNVP, inverseF) <- case mc.standardizeNVs of
    True -> do
      sdP <- DAG.addBuildParameter
             $ DAG.TransformedDataP
             $ DAG.TData
             (TE.NamedDeclSpec "nvpSD" $ TE.realSpec []) []
             TNil (\_ -> DAG.DeclRHS $  TE.functionE SF.sd (mData.projectionsE :> TNil))
      stdNVP <- SMB.inBlock SMB.SBTransformedData $ SMB.addFromCodeWriter
                $ TE.declareRHSNW (TE.NamedDeclSpec "stdNVP" $ TE.vectorSpec nRowsE [])
                $ mData.projectionsE `TE.divideE` pExpr sdP
      let inverse :: (t ~ TEO.BinaryResultT TEO.BMultiply TE.EReal t) => TE.UExpr t -> TE.UExpr t
          inverse psCol = pExpr sdP `TE.timesE` psCol
      pure (stdNVP, inverse)
    False -> pure (mData.projectionsE, id)

  -- model
  let reIndexByState = TE.indexE TEI.s0 (SMB.byGroupIndexE mData.projDataTag stateG)
      muE :: Alpha -> Theta -> TE.VectorE
      muE a t =  case a of
       SimpleAlpha alphaP -> case t of
         Theta Nothing -> TE.functionE SF.rep_vector (pExpr alphaP :> nRowsE :> TNil)
         Theta (Just thetaP) -> pExpr alphaP `TE.plusE` (predM `TE.timesE` pExpr thetaP)
       HierarchicalAlpha alpha -> case t of
         Theta Nothing -> reIndexByState $ pExpr alpha
         Theta (Just thetaP) -> reIndexByState (pExpr alpha) `TE.plusE` (predM `TE.timesE` pExpr thetaP)
      sigmaE :: Sigma -> TE.VectorE
      sigmaE s = TE.functionE SF.rep_vector (pExpr (unSigma s) :> nRowsE :> TNil)

  let ppF :: ((TE.IntE -> TE.ExprList xs) -> TE.IntE -> TE.UExpr TE.EReal)
          -> TE.CodeWriter (TE.IntE -> TE.ExprList xs)
          -> SMB.StanBuilderM (ProjData outerK) () (TE.ArrayE TE.EReal)
      ppF rngF rngPSCW = SBB.generatePosteriorPrediction'
                         mData.projDataTag
                         (TE.NamedDeclSpec "predProj" $ TE.array1Spec nRowsE $ TE.realSpec [])
                         rngF
                         rngPSCW
                         (\_ p -> inverseF p)
      llF :: SMD.StanDist t pts rts
          -> TE.CodeWriter (TE.IntE -> TE.ExprList pts)
          -> TE.CodeWriter (TE.IntE -> TE.UExpr t)
          -> SMB.StanBuilderM md gq ()
      llF = SBB.generateLogLikelihood mData.projDataTag

  let (sampleStmtF, pp, ll) = case mParams of
        NormalProjModelParameters a t s ->
          let ssF e = TE.sample e SF.normal (muE a t :> sigmaE s  :> TNil)
              rF f nE = TE.functionE SF.normal_rng (f nE)
              rpF = pure $ \nE -> muE a t `fstI` nE :> sigmaE s `fstI` nE :> TNil
              ll = llF SMD.normalDist rpF (pure $ \nE -> stdNVP `TE.at` nE)
          in (ssF, ppF rF rpF, ll)
        CauchyProjModelParameters a t s ->
          let ssF e = TE.sample e SF.cauchy (muE a t :> sigmaE s :> TNil)
              rF f nE = TE.functionE SF.cauchy_rng (f nE)
              rpF = pure $ \nE -> muE a t `fstI` nE :> sigmaE s `fstI` nE :> TNil
              ll = llF SMD.cauchyDist rpF (pure $ \nE -> stdNVP `TE.at` nE)
          in (ssF, ppF rF rpF, ll)
        StudentTProjModelParameters a t s n ->
          let nu :: Nu -> TE.VectorE
              nu n' = TE.functionE SF.rep_vector (pExpr (unNu n') :> nRowsE :> TNil)
              ssF e = TE.sample e SF.student_t (nu n  :> muE a t :> sigmaE s :> TNil)
              rF f nE = TE.functionE SF.student_t_rng (f nE)
              rpF = pure $ \nE -> nu n `fstI` nE :> muE a t `fstI` nE :> sigmaE s `fstI` nE :>  TNil
              ll = llF SMD.studentTDist rpF (pure $ \nE -> stdNVP `TE.at` nE)
          in (ssF, ppF rF rpF, ll)

  SMB.inBlock SMB.SBModel $ SMB.addFromCodeWriter $ TE.addStmt $ sampleStmtF stdNVP
  -- generated quantities
  when rc.rcIncludePPCheck $ void pp
  when rc.rcIncludeLL ll
  pure ()

cwdF :: (F.ElemOf rs DT.PopCount, F.ElemOf rs DT.PWPopPerSqMile) => F.Record rs -> DMS.CellWithDensity
cwdF r = DMS.CellWithDensity (realToFrac $ r ^. DT.popCount) (r ^. DT.pWPopPerSqMile)

model3A5CacheDir :: Text
model3A5CacheDir = "model/demographic/nullVecProjModel3_A5/"

runProjModel :: forall (ks :: [(Symbol, Type)]) rs r .
                (K.KnitEffects r
                , BRKU.CacheEffects r
                , ks F.⊆ rs
                , F.ElemOf rs GT.PUMA
                , F.ElemOf rs GT.StateAbbreviation
                , F.ElemOf rs DT.PopCount
                , F.ElemOf rs DT.PWPopPerSqMile
                )
             => Either Text Text
             -> BR.CommandLine
             -> RunConfig
             -> ModelConfig
             -> K.ActionWithCacheTime r (F.FrameRec rs)
             -> K.ActionWithCacheTime r (DTP.NullVectorProjections (F.Record ks))
             -> DMS.MarginalStructure DMS.CellWithDensity (F.Record ks)
             -> FL.Fold (F.Record rs) (VS.Vector Double)
             -> K.Sem r (K.ActionWithCacheTime r (ComponentPredictor Text))
runProjModel cacheDirE _cmdLine rc mc acs_C nvps_C ms datFld = K.wrapPrefix "TPModel3.runProjModel" $ do
  let dataName = "projectionData_" <> dataText mc <> "_N" <> show rc.nvIndex <> maybe "" fst rc.statesM
      runnerInputNames = SC.RunnerInputNames
                         ("br-2022-Demographics/stan/nullVecProj_M3_A5")
                         (modelText mc)
                         (Just $ SC.GQNames "pp" dataName) -- posterior prediction vars to wrap
                         dataName
--  acsByPUMA_C <- DDP.cachedACSa5ByPUMA
  nvpDataCacheKey <- BRKU.cacheFromDirE cacheDirE ("nvpRowData" <> dataText mc <> ".bin")
  let outerKey = F.rcast @[GT.StateAbbreviation, GT.PUMA]
      catKey = F.rcast @ks
      nvpDeps = (,) <$> acs_C <*>  nvps_C
  nvpData_C <- fmap (fmap (fmap unNVProjectionRowData))
               $ BRKU.retrieveOrMakeD nvpDataCacheKey nvpDeps
               $ \(acsByPUMA, nvps) -> (do
--                                           K.logLE K.Diagnostic $ toText $ LA.dispf 3 (DTP.fullToProjM nvps)
                                           let rawRows = FL.fold (nullVecProjectionsModelDataFld DMS.cwdWgtLens ms nvps outerKey catKey cwdF datFld) acsByPUMA
                                           pure $ fmap NVProjectionRowData rawRows
                               )
  let statesFilter = maybe id (\(_, sts) -> filter ((`elem` sts) . view GT.stateAbbreviation . pdKey)) rc.statesM
      rowData_C = fmap (statesFilter . fmap (toProjDataRow rc.nvIndex)) $ nvpData_C
      modelData_C = ProjData (DM.rowLength mc.designMatrixRow) <$> rowData_C
--  acsByPUMA <- K.ignoreCacheTime acsByPUMA_C

  let meanSDFld :: FL.Fold Double (Double, Double) = (,) <$> FL.mean <*> FL.std
  modelData <- K.ignoreCacheTime modelData_C
  let meanSD = FL.fold (FL.premap pdCoeff meanSDFld) $ pdRows modelData
  K.logLE K.Diagnostic $ "meanSD=" <> show meanSD
  states <- FL.fold (FL.premap (view GT.stateAbbreviation) FL.set) <$> K.ignoreCacheTime acs_C
  (dw, code) <-  SMR.dataWranglerAndCode modelData_C (pure ())
                (stateGroupBuilder (view GT.stateAbbreviation)  (S.toList states))
                (projModel rc mc)

  let unwraps = [SR.UnwrapNamed "projection" "yProjection"]

  res_C <- SMR.runModel' @BRKU.SerializerC @BRKU.CacheData
           cacheDirE
           (Right runnerInputNames)
           Nothing
           dw
           code
           (projModelResultAction mc) --SC.DoNothing -- (stateModelResultAction mcWithId dmr)
           (SMR.Both unwraps) --(SMR.Both [SR.UnwrapNamed "successes" "yObserved"])
           modelData_C
           (pure ())
  K.logLE K.Info "projModel run complete."
  pure res_C

--NB: parsed summary data has stan indexing, i.e., Arrays start at 1.
projModelResultAction :: forall outerK r ks .
                         (K.KnitEffects r
                         , Typeable outerK
                         )
                      => ModelConfig
                      -> SC.ResultAction r (ProjData outerK) () SMB.DataSetGroupIntMaps () (ComponentPredictor Text)
projModelResultAction mc = SC.UseSummary f where
  f summary _ modelDataAndIndexes_C _ = do
    (modelData, resultIndexesE) <- K.ignoreCacheTime modelDataAndIndexes_C
    -- compute means of predictors because model was zero-centered in them
    let nPredictors = DM.rowLength mc.designMatrixRow
        mdMeansFld = FL.premap (VS.toList . pdCovariates)
                    $ traverse (\n -> FL.premap (List.!! n) FL.mean) [0..(nPredictors - 1)]
        nvpSDFld = FL.premap pdCoeff FL.std
        (mdMeansL, nvpSD) = FL.fold ((,) <$> mdMeansFld <*> nvpSDFld) $ pdRows modelData
        rescaleAlphaBeta x = if mc.standardizeNVs then x * nvpSD else x
    stateIM <- K.knitEither
      $ resultIndexesE >>= SMB.getGroupIndex (SMB.RowTypeTag @(ProjDataRow outerK) SC.ModelData "ProjectionData") stateG
    let allStates = IM.elems stateIM
        getScalar n = K.knitEither $ SP.getScalar . fmap CS.mean <$> SP.parseScalar n (CS.paramStats summary)
        getVector n = K.knitEither $ SP.getVector . fmap CS.mean <$> SP.parse1D n (CS.paramStats summary)
--        getMatrix n = K.knitEither $ fmap CS.mean <$> SP.parse2D n (CS.paramStats summary)
    geoMap <- case mc.alphaModel of
      AlphaSimple -> do
        alpha <- getScalar "alpha" -- states by nNullvecs
        pure $ M.fromList $ fmap (, rescaleAlphaBeta alpha) allStates
      _ -> do
        alphaV <- getVector "alpha"
--        let mRowToList row = SP.getIndexed alphaV row
        pure $ M.fromList $ fmap (\(stateIdx, stateAbbr) -> (stateAbbr, rescaleAlphaBeta (alphaV V.! (stateIdx - 1)))) $ IM.toList stateIM
    betaSI <- case nPredictors of
      0 -> pure V.empty
      p -> do
        betaV <- getVector "beta"
        pure $ V.fromList $ zip (fmap rescaleAlphaBeta $ V.toList betaV) mdMeansL
    pure $ ComponentPredictor geoMap betaSI

{-
data ASERModelP a = ASERModelP { mASER_PWLogDensity :: a, mASER_FracOver45 :: a, mASER_FracGrad :: a, mASER_FracOfColor :: a , mASER_FracWNG :: a  }
  deriving stock (Show, Generic)
  deriving anyclass Flat.Flat

aserModelDatFld :: (F.ElemOf rs DT.PWPopPerSqMile
                   , F.ElemOf rs DT.Age4C
                   , F.ElemOf rs DT.Education4C
                   , F.ElemOf rs DT.Race5C
                   , F.ElemOf rs DT.PopCount
                   )
             => FL.Fold (F.Record rs) (ASERModelP Double)
aserModelDatFld = ASERModelP <$> dFld <*> aFld <*> gFld <*> rFld <*> wngFld
  where
    nPeople = realToFrac . view DT.popCount
    dens r = let x = view DT.pWPopPerSqMile r in if x > 1 then Numeric.log x else 0
    wgtFld = FL.premap nPeople FL.sum
    wgtdFld f = safeDiv <$> FL.premap (\r -> nPeople r * f r) FL.sum <*> wgtFld
    dFld = wgtdFld dens
    over45 = (`elem` [DT.A4_45To64, DT.A4_65AndOver]) . view DT.age4C
    grad = (== DT.E4_CollegeGrad) . view DT.education4C
    ofColor = (/= DT.R5_WhiteNonHispanic) . view DT.race5C
    fracFld f = safeDiv <$> FL.prefilter f wgtFld <*> wgtFld
    wng x = not (ofColor x) && not (grad x)
    aFld = fracFld over45
    gFld = fracFld grad
    rFld = fracFld ofColor
    wngFld = fracFld wng

aserModelFuncs :: ModelDataFuncs ASERModelP a
aserModelFuncs = ModelDataFuncs aserModelToList aserModelFromList where
  aserModelToList :: ASERModelP a -> [a]
  aserModelToList (ASERModelP x y z a b) = [x, y, z, a, b]

  aserModelFromList :: [a] -> Either Text (ASERModelP a)
  aserModelFromList as = case as of
    [x, y, z, a, b] -> Right $ ASERModelP x y z a b
    _ -> Left "aserModelFromList: wrong size list given (n /= 5)"

designMatrixRowASER :: DM.DesignMatrixRow (ASERModelP Double)
designMatrixRowASER = DM.DesignMatrixRow "ASER"
                      [DM.DesignMatrixRowPart "logDensity" 1 (VU.singleton . mASER_PWLogDensity)
                      , DM.DesignMatrixRowPart "fracOver45" 1 (VU.singleton . mASER_FracOver45)
                      , DM.DesignMatrixRowPart "fracGrad" 1 (VU.singleton . mASER_FracGrad)
                      , DM.DesignMatrixRowPart "fracOC" 1 (VU.singleton . mASER_FracOfColor)
                      , DM.DesignMatrixRowPart "fracWNG" 1 (VU.singleton . mASER_FracWNG)
                      ]


---

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
    dens = Numeric.log . view DT.pWPopPerSqMile
    wgtFld = FL.premap nPeople FL.sum
    wgtdFld f = (/) <$> FL.premap (\r -> nPeople r * f r) FL.sum <*> wgtFld
    dFld = wgtdFld dens
    fracFld f = (/) <$> FL.prefilter f wgtFld <*> wgtFld
    gFld = fracFld ((== DT.E4_CollegeGrad) . view DT.education4C)
    rFld = fracFld ((/= DT.R5_WhiteNonHispanic) . view DT.race5C)

model1Funcs :: ModelDataFuncs Model1P a
model1Funcs = ModelDataFuncs model1ToList model1FromList where
  model1ToList :: Model1P a -> [a]
  model1ToList (Model1P x y z) = [x, y, z]

  model1FromList :: [a] -> Either Text (Model1P a)
  model1FromList as = case as of
    [x, y, z] -> Right $ Model1P x y z
    _ -> Left "model1FromList: wrong size list given (n /= 3)"

emptyDM :: DM.DesignMatrixRow (Model1P Double)
emptyDM = DM.DesignMatrixRow "EDM" []

designMatrixRow1 :: DM.DesignMatrixRow (Model1P Double)
designMatrixRow1 = DM.DesignMatrixRow "PM1"
                   [DM.DesignMatrixRowPart "logDensity" 1 (VU.singleton . m1pPWLogDensity)
                   , DM.DesignMatrixRowPart "fracGrad" 1 (VU.singleton . m1pFracGrad)
                   , DM.DesignMatrixRowPart "fracOC" 1 (VU.singleton . m1pFracOfColor)
                   ]

data Model2P a = Model2P { m2pPWLogDensity :: a, m2pFracCit :: a, m2pFracGrad :: a, m2pFracOfColor :: a }
  deriving stock (Show, Generic)
  deriving anyclass Flat.Flat

safeDiv :: Double -> Double -> Double
safeDiv x y = if y /= 0 then x / y else 0
{-# INLINE safeDiv #-}

model2DatFld :: (F.ElemOf rs DT.PWPopPerSqMile
                , F.ElemOf rs DT.CitizenC
                , F.ElemOf rs DT.Education4C
                , F.ElemOf rs DT.Race5C
                , F.ElemOf rs DT.PopCount
                )
             => FL.Fold (F.Record rs) (Model2P Double)
model2DatFld = Model2P <$> dFld <*> cFld <*> gFld <*> rFld
  where
    nPeople = realToFrac . view DT.popCount
    dens r = let pwd = view DT.pWPopPerSqMile r in if pwd > 1 then Numeric.log pwd else 0
    wgtFld = FL.premap nPeople FL.sum
    wgtdFld f = safeDiv <$> FL.premap (\r -> nPeople r * f r) FL.sum <*> wgtFld
    dFld = wgtdFld dens
    fracFld f = (/) <$> FL.prefilter f wgtFld <*> wgtFld
    cFld = fracFld ((== DT.Citizen) . view DT.citizenC)
    gFld = fracFld ((== DT.E4_CollegeGrad) . view DT.education4C)
    rFld = fracFld ((/= DT.R5_WhiteNonHispanic) . view DT.race5C)

model2Funcs :: ModelDataFuncs Model2P a
model2Funcs = ModelDataFuncs model2ToList model2FromList where
  model2ToList :: Model2P a -> [a]
  model2ToList (Model2P x y z a) = [x, y, z, a]

  model2FromList :: [a] -> Either Text (Model2P a)
  model2FromList as = case as of
    [x, y, z, a] -> Right $ Model2P x y z a
    _ -> Left "model1FromList: wrong size list given (n /= 3)"

designMatrixRow2 :: DM.DesignMatrixRow (Model2P Double)
designMatrixRow2 = DM.DesignMatrixRow "PM2"
                   [DM.DesignMatrixRowPart "logDensity" 1 (VU.singleton . m2pPWLogDensity)
                   , DM.DesignMatrixRowPart "fracCit" 1 (VU.singleton . m2pFracCit)
                   , DM.DesignMatrixRowPart "fracGrad" 1 (VU.singleton . m2pFracGrad)
                   , DM.DesignMatrixRowPart "fracOC" 1 (VU.singleton . m2pFracOfColor)
                   ]
-}

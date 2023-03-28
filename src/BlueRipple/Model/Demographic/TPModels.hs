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

module BlueRipple.Model.Demographic.TPModels
  (
    module BlueRipple.Model.Demographic.TPModels
  )
where

import qualified BlueRipple.Configuration as BR
import qualified BlueRipple.Utilities.KnitUtils as BRKU
import qualified BlueRipple.Model.Demographic.DataPrep as DDP
import qualified BlueRipple.Model.Demographic.MarginalStructure as DMS
import qualified BlueRipple.Model.Demographic.TableProducts as DTP

import qualified BlueRipple.Data.Keyed as BRK
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.GeographicTypes as GT

import qualified Knit.Report as K

import qualified Control.MapReduce.Simple as MR

import qualified Control.Foldl as FL
import qualified Data.IntMap.Strict as IM
import qualified Data.Map.Strict as M
import qualified Data.Set as S
import Data.Type.Equality (type (~))

import qualified Data.List as List
import qualified Frames as F
import qualified Frames.Melt as F
import qualified Numeric.LinearAlgebra as LA
import qualified Numeric
import qualified Data.Vector as V
import qualified Data.Vector.Storable as VS
import qualified Data.Vector.Unboxed as VU

import Control.Lens (view)
import GHC.TypeLits (Symbol)

import qualified CmdStan as CS
import qualified Stan.ModelBuilder as SMB
import qualified Stan.ModelRunner as SMR
import qualified Stan.ModelConfig as SC
import qualified Stan.Parameters as SP
import qualified Stan.RScriptBuilder as SR
import qualified Stan.ModelBuilder.BuildingBlocks as SBB
import qualified Stan.ModelBuilder.DesignMatrix as DM
import qualified Stan.ModelBuilder.TypedExpressions.Types as TE
import qualified Stan.ModelBuilder.TypedExpressions.Statements as TE
import qualified Stan.ModelBuilder.TypedExpressions.Indexing as TEI
import qualified Stan.ModelBuilder.TypedExpressions.Operations as TEO
import qualified Stan.ModelBuilder.TypedExpressions.DAG as DAG
import qualified Stan.ModelBuilder.TypedExpressions.StanFunctions as SF
import Stan.ModelBuilder.TypedExpressions.TypedList (TypedList(..))
import qualified Flat

-- NB: nullVecs we use are not the ones from SVD but a subset of a rotation of those via
-- the eigenvectors of the covariance
nullVecProjectionsModelDataFld ::  forall outerK k row md .
                                   (Ord outerK)
                               => DMS.MarginalStructure k
                               -> DTP.NullVectorProjections
                               -> (row -> outerK)
                               -> (row -> k)
                               -> (row -> LA.R)
                               -> FL.Fold row md
                               -> FL.Fold row [(outerK, md, LA.Vector LA.R)]
nullVecProjectionsModelDataFld ms nvps outerKey catKey count datFold = case ms of
  DMS.MarginalStructure _ ptFld -> MR.mapReduceFold
                                 MR.noUnpack
                                 (MR.assign outerKey id)
                                 (MR.foldAndLabel innerFld (\ok (d, v) -> (ok, d, v)))
    where
      allKs :: Set k = BRK.elements
      pcF :: VS.Vector Double -> VS.Vector Double
      pcF =  VS.fromList . fmap snd . FL.fold ptFld . zip (S.toList allKs) . VS.toList
      projections v = let w = DTP.normalizedVec v in DTP.fullToProj nvps (w - pcF w)
      projFld = fmap projections $ DTP.labeledRowsToVecFld catKey count
      innerFld = (,) <$> datFold <*> projFld

-- NB: nullVecs we use are not the ones from SVD but a subset of a rotation of those via
-- the eigenvectors of the covariance
nullVecProjectionsModelDataFldCheck ::  forall outerK k row md .
                                        (Ord outerK)
                                    => DMS.MarginalStructure k
                                    -> DTP.NullVectorProjections
                                    -> (row -> outerK)
                                    -> (row -> k)
                                    -> (row -> Double)
                                    -> FL.Fold row md
                                    -> FL.Fold row [(outerK, md, LA.Vector Double, LA.Vector Double, LA.Vector Double)]
nullVecProjectionsModelDataFldCheck ms nvps outerKey catKey count datFold = case ms of
  DMS.MarginalStructure _ ptFld -> MR.mapReduceFold
                                   MR.noUnpack
                                   (MR.assign outerKey id)
                                   (MR.foldAndLabel innerFld (\ok (d, (v, pv, nv)) -> (ok, d, v, pv, nv)))
    where
      allKs :: Set k = BRK.elements
      pcF :: VS.Vector Double -> VS.Vector Double
      pcF =  VS.fromList . fmap snd . FL.fold ptFld . zip (S.toList allKs) . VS.toList
      results v = let w = DTP.normalizedVec v -- normalized original probs
                      nSum = VS.sum v -- num of people

                  in (DTP.fullToProj nvps (w - pcF w), VS.map (* nSum) (pcF v), v)
      projFld = fmap results $ DTP.labeledRowsToVecFld catKey count
      innerFld = (,) <$> datFold <*> projFld


type ProjDataRow outerK md = (outerK, md Double, LA.Vector LA.R)

data ProjData outerK md =
  ProjData
  {
    pdNNullVecs :: Int
  , pdNPredictors :: Int
  , pdRows :: [ProjDataRow outerK md]
  }

data ModelDataFuncs md a = ModelDataFuncs { mdfToList :: md a -> [a], mdfFromList :: [[(a, a)]] -> Either Text (md [(a, a)])}


data ModelResult g (b :: Type -> Type) = ModelResult { mrGeoAlpha :: Map g [Double], mrSI :: b [(Double, Double)] }
  deriving stock (Generic)

deriving stock instance (Show g, Show (b [(Double, Double)])) => Show (ModelResult g b)
deriving anyclass instance (Ord g, Flat.Flat g, Flat.Flat (b [(Double, Double)])) => Flat.Flat (ModelResult g b)

modelResultNVPs :: (Show g, Ord g) => (forall a . ModelDataFuncs md a) -> ModelResult g md -> g -> md Double -> Either Text [Double]
modelResultNVPs mdf mr g md = do
    geoAlphas <- maybeToRight ("geoAlpha lookup failed for gKey=" <> show g) $ M.lookup g mr.mrGeoAlpha
    let mdL = mdf.mdfToList md
        mrBetaL = mdf.mdfToList mr.mrSI
        applyOne x (b, m) = b * (x - m)
        applyToList x = fmap (applyOne x)
        eachBetaL = zipWith applyToList mdL mrBetaL
        betaL = fmap (FL.fold FL.sum) $ transp eachBetaL
    pure $ zipWith (+) geoAlphas betaL

stateG :: SMB.GroupTypeTag Text
stateG = SMB.GroupTypeTag "State"

stateGroupBuilder :: (Foldable f, Typeable outerK, Typeable md)
                  => (outerK -> Text) -> f Text -> SMB.StanGroupBuilderM (ProjData outerK md) () ()
stateGroupBuilder saF states = do
  let ok (x, _, _) = x
  projData <- SMB.addModelDataToGroupBuilder "ProjectionData" (SMB.ToFoldable pdRows)
  SMB.addGroupIndexForData stateG projData $ SMB.makeIndexFromFoldable show (saF . ok) states
  SMB.addGroupIntMapForDataSet stateG projData $ SMB.dataToIntMapFromFoldable (saF . ok) states



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
    projVecs :: DTP.NullVectorProjections
  , standardizeNVs :: Bool
  , designMatrixRow :: DM.DesignMatrixRow (md Double)
  , alphaModel :: AlphaModel
  , distribution :: Distribution
  , mdFuncs :: ModelDataFuncs md Double
  }

modelNumNullVecs :: ModelConfig md -> Int
modelNumNullVecs mc = fst $ LA.size $ DTP.nvpProj mc.projVecs

modelText :: ModelConfig md -> Text
modelText mc = distributionText mc.distribution <> "_" <> mc.designMatrixRow.dmName <> "_" <> alphaModelText mc.alphaModel

dataText :: ModelConfig md -> Text
dataText mc = mc.designMatrixRow.dmName <> "_NV" <> show (modelNumNullVecs mc)

projModelData :: forall md outerK . (Typeable outerK, Typeable md)
               => ModelConfig md
               -> SMB.StanBuilderM (ProjData outerK md) () (ProjModelData outerK md)
projModelData mc = do
  projData <- SMB.dataSetTag @(ProjDataRow outerK md) SC.ModelData "ProjectionData"
  let projMER :: SMB.MatrixRowFromData (ProjDataRow outerK md) --(outerK, md Double, VS.Vector Double)
      projMER = SMB.MatrixRowFromData "nvp" Nothing (modelNumNullVecs mc) (\(_, _, v) -> VU.convert v)
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
  (predM, _centerF, _mBeta) <- case paramTheta mParams of
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
              nu n' k = TE.functionE SF.rep_vector (unNu n' `fstI` k :> nRowsE :> TNil)
              ssF e k = TE.sample e SF.student_t (nu n k :> muE a t k :> sigmaE s k :> TNil)
              rF f nE = TE.functionE SF.student_t_rng (f nE)
              rpF k  = pure $ \nE -> nu n k `fstI` nE :> muE a t k `fstI` nE :> sigmaE s k `fstI` nE :>  TNil
          in (ssF, \n' -> ppF n' rF rpF)

  SMB.inBlock SMB.SBModel $ SMB.addFromCodeWriter $ do
    let loopBody k = TE.writerL' $ TE.addStmt $ sampleStmtF (nvps `sndI` k) k
    TE.addStmt $ loopNVs loopBody
  -- generated quantities
  when rc.rcIncludePPCheck $ forM_ [1..modelNumNullVecs mc] ppStmtF
  pure ()

runProjModel :: forall (ks :: [(Symbol, Type)]) md r .
                (K.KnitEffects r
                , BRKU.CacheEffects r
                , ks F.⊆ DDP.ACSByPUMAR
                , Typeable md
                , Flat.Flat (md [(Double, Double)])
                )
             => Bool
             -> BR.CommandLine
             -> RunConfig
             -> ModelConfig md
             -> DMS.MarginalStructure (F.Record ks)
             -> FL.Fold (F.Record DDP.ACSByPUMAR) (md Double)
             -> K.Sem r (K.ActionWithCacheTime r (ModelResult Text md)) -- no returned result for now
runProjModel clearCaches _cmdLine rc mc ms datFld = do
  let cacheDirE = (if clearCaches then Left else Right) "model/demographic/nullVecProjModel/"
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
      projData acsByPUMA =
        ProjData (modelNumNullVecs mc) (DM.rowLength mc.designMatrixRow)
        (FL.fold (nullVecProjectionsModelDataFld ms mc.projVecs outerKey catKey count datFld) acsByPUMA)
  let modelData_C = fmap projData acsByPUMA_C
      meanSDFld :: FL.Fold Double (Double, Double) = (,) <$> FL.mean <*> FL.std
      meanSDFlds :: Int -> FL.Fold [Double] [(Double, Double)]
      meanSDFlds m = traverse (\n -> FL.premap (List.!! n) meanSDFld) [0..(m - 1)]
--        foldl' (\flds fld -> g <$> flds <*> fld) (pure []) $ replicate n meanSDFld
--        where g ls l = ls ++ [l]
  modelData <- K.ignoreCacheTime modelData_C
  let meanSDs = FL.fold (FL.premap (\(_, _, v) -> VS.toList v) $ meanSDFlds (modelNumNullVecs mc)) $ pdRows modelData
  K.logLE K.Info $ "meanSDs=" <> show meanSDs
  states <- FL.fold (FL.premap (view GT.stateAbbreviation) FL.set) <$> K.ignoreCacheTime acsByPUMA_C
  (dw, code) <-  SMR.dataWranglerAndCode modelData_C (pure ())
                (stateGroupBuilder (view GT.stateAbbreviation)  (S.toList states))
                (projModel rc mc)

  let nNullVecs = modelNumNullVecs mc
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

--NB: parsed summary data has stan indexing, i.e., Arrays start at 1.
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
    let nPredictors = DM.rowLength mc.designMatrixRow
        mdMeansFld = FL.premap (\(_, md, _) -> mc.mdFuncs.mdfToList md)
                    $ traverse (\n -> FL.premap (List.!! n) FL.mean) [0..(nPredictors - 1)]
        nvpSDFld = FL.premap (\(_, _, v) -> VS.toList v)
                   $ traverse (\n -> FL.premap (List.!! n) FL.std) [0..((modelNumNullVecs mc) - 1)]
        (mdMeansL, nvpSDsL) = FL.fold ((,) <$> mdMeansFld <*> nvpSDFld) $ pdRows modelData
        rescaleAlphaBeta xs = if mc.standardizeNVs then zipWith (*) xs nvpSDsL else xs
    stateIM <- K.knitEither
      $ resultIndexesE >>= SMB.getGroupIndex (SMB.RowTypeTag @(ProjDataRow outerK md) SC.ModelData "ProjectionData") stateG
    let allStates = IM.elems stateIM
        getVector n = K.knitEither $ SP.getVector . fmap CS.mean <$> SP.parse1D n (CS.paramStats summary)
        getMatrix n = K.knitEither $ fmap CS.mean <$> SP.parse2D n (CS.paramStats summary)
    geoMap <- case mc.alphaModel of
      AlphaSimple -> do
        alphaV <- getVector "alpha" -- states by nNullvecs
        pure $ M.fromList $ fmap (, rescaleAlphaBeta $ V.toList alphaV) allStates
      _ -> do
        alphaVs <- getMatrix "alpha"
        let mRowToList cols row = fmap (\c -> SP.getIndexed alphaVs (row, c)) $ [1..cols]
        pure $ M.fromList $ fmap (\(row, sa) -> (sa, rescaleAlphaBeta $ mRowToList (modelNumNullVecs mc) row)) $ IM.toList stateIM
    betaSIL <- case nPredictors of
      0 -> pure $ replicate (modelNumNullVecs mc) []
      p -> do
        betaVs <- getMatrix "beta" -- ps by nNullVecs
        let mColToList rows col = fmap (\r -> SP.getIndexed betaVs (r, col)) [1..rows]
        pure $ transp $ fmap (\m -> zip (rescaleAlphaBeta $ mColToList p m) mdMeansL) [1..modelNumNullVecs mc]
    betaSI <- K.knitEither $ mc.mdFuncs.mdfFromList betaSIL
    pure $ ModelResult geoMap betaSI

transp :: [[a]] -> [[a]]
transp = go [] where
  go x [] = fmap reverse x
  go [] (r : rs) = go (fmap (: []) r) rs
  go x (r :rs) = go (List.zipWith (:) r x) rs


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

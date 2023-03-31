{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveFoldable #-}
{-# LANGUAGE DeriveTraversable #-}
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

module BlueRipple.Model.Demographic.TPModel2
  (
    module BlueRipple.Model.Demographic.TPModel2
  )
where

import Relude.Extra (traverseToSnd)

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
--import qualified Data.Distributive as DD
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
import qualified Data.Vector as V
import qualified Data.Vector.Storable as VS
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vinyl as V

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

productDistributionFld :: forall outerK k row .
                          (Ord outerK)
                       => DMS.MarginalStructure k
                       -> (row -> outerK)
                       -> (row -> k)
                       -> (row -> Double)
                       -> FL.Fold row (Map outerK (VS.Vector Double))
productDistributionFld marginalStructure outerKey catKey count = M.fromList <$> case marginalStructure of
  DMS.MarginalStructure _ ptFld -> MR.mapReduceFold
                                   MR.noUnpack
                                   (MR.assign outerKey id)
                                   (MR.foldAndLabel innerFld (,))
    where
      allKs :: Set k = BRK.elements
      pcF :: VS.Vector Double -> VS.Vector Double
      pcF =  VS.fromList . fmap snd . FL.fold ptFld . zip (S.toList allKs) . VS.toList
      innerFld = DTP.normalizedVec . pcF <$> DTP.labeledRowsToVecFld catKey count



-- produce the projections of the difference bewteen the distirbution of
-- probability 1 at k and the product distribution at outerK
rowDiffProjections ::  forall outerK k row .
                       (Ord outerK, Show outerK, Ord k, BRK.FiniteSet k)
                   => DTP.NullVectorProjections
                   -> Map outerK (VS.Vector Double) -- Product Distribution
                   -> (row -> outerK)
                   -> (row -> k)
                   -> row
                   -> Either Text (VS.Vector Double)
rowDiffProjections nvps pdMap outerKey catKey r = do
  let ok = outerKey r
      k = catKey r
      catDist = VS.fromList $ M.elems (M.singleton k 1 <> DMS.zeroMap)
  pd <- maybeToRight ("rowDiffProjections: " <> show ok <> " is missing from product structure map!") $ M.lookup ok pdMap
  pure $ DTP.fullToProj nvps (catDist - pd)

rowsWithProjectedDiffs :: (Traversable g
                          , Ord outerK
                          , Show outerK
                          , Ord k
                          , BRK.FiniteSet k)
                       =>  DTP.NullVectorProjections
                       -> Map outerK (VS.Vector Double) -- Product Distribution
                       -> (F.Record rs -> outerK)
                       -> (F.Record rs -> k)
                       -> g (F.Record rs)
                       -> Either Text (g (ProjDataRow rs))
rowsWithProjectedDiffs nvps pdMap outerKey catKey =
  fmap (fmap $ fmap (\(r, v) -> ProjDataRow r v))
  $ traverse (traverseToSnd $ rowDiffProjections nvps pdMap outerKey catKey)

data ProjDataRow rs = ProjDataRow (F.Record rs) (VS.Vector Double)

projRowRec :: ProjDataRow rs -> F.Record rs
projRowRec (ProjDataRow r _) = r

projRowVec :: ProjDataRow rs -> VS.Vector Double
projRowVec (ProjDataRow _ v) = v

instance (V.RMap rs, FS.RecFlat rs) => Flat.Flat (ProjDataRow rs) where
  size (ProjDataRow r v) = Flat.size (FS.toS r, VS.toList v)
  encode (ProjDataRow r v) = Flat.encode (FS.toS r, VS.toList v)
  decode = fmap (\(sr, l) -> ProjDataRow (FS.fromS sr) (VS.fromList l)) Flat.decode

data ProjData r = ProjData {pdNNullVecs :: Int, pdNPredictors :: Int, pdRows :: [ProjDataRow r]}

data SlopeIntercept = SlopeIntercept { siSlope :: Double, siIntercept :: Double} deriving stock (Show, Generic)

applySlopeIntercept :: SlopeIntercept -> Double -> Double
applySlopeIntercept (SlopeIntercept s i) x = i + s * x
{-# INLINEABLE applySlopeIntercept #-}

newtype ModelResult g k (pd :: Type -> Type) = ModelResult { unModelResult :: Map g (Map k [Double], pd [SlopeIntercept]) }
  deriving stock (Generic)

deriving stock instance (Show g, Show k, Show (b [SlopeIntercept])) => Show (ModelResult g k b)
deriving anyclass instance (Ord g, Flat.Flat g, Ord k, Flat.Flat k, Flat.Flat (b [SlopeIntercept])) => Flat.Flat (ModelResult g k b)

modelResultNVPs :: (Traversable pd, Applicative pd, Show g, Ord g, Show k, Ord k)
                => ModelResult g k pd
                -> (r -> g)
                -> (r -> k)
                -> (r -> pd Double)
                -> r -> Either Text (VS.Vector Double)
modelResultNVPs modelResult geoKey catKey pdF r = do
  let gk = geoKey r
      ck = catKey r
      pd = pdF r
  (gaM, pdSIs) <- maybeToRight ("modelResultNVPs: " <> show gk <> " not found in model result geo-alpha map!")
        $ M.lookup gk $ unModelResult modelResult
  alphaV <- maybeToRight ("modelResultNVPs: " <> show ck <> " not found in model result alpha map for " <> show gk <> "!")
            $ M.lookup ck gaM
  let pdSIL = sequenceA pdSIs
      applyTo si = applySlopeIntercept <$> si <*> pd
      betaV = VS.fromList $ fmap (getSum . foldMap Sum . applyTo) pdSIL
  pure $ VS.fromList alphaV + betaV

stateG :: SMB.GroupTypeTag Text
stateG = SMB.GroupTypeTag "State"

stateGroupBuilder :: (Foldable f, Typeable rs)
                  => (F.Record rs -> Text) -> f Text -> SMB.StanGroupBuilderM (ProjData rs) () ()
stateGroupBuilder saF states = do
  projData <- SMB.addModelDataToGroupBuilder "ProjectionData" (SMB.ToFoldable pdRows)
  SMB.addGroupIndexForData stateG projData $ SMB.makeIndexFromFoldable show (saF . projRowRec) states
  SMB.addGroupIntMapForDataSet stateG projData $ SMB.dataToIntMapFromFoldable (saF . projRowRec) states

data ProjModelData r =
  ProjModelData
  {
    projDataTag :: SMB.RowTypeTag (ProjDataRow r)
  , nNullVecsE :: TE.IntE
  , nAlphasE :: TE.IntE
  , alphasE :: TE.MatrixE
  , nPredictorsE :: TE.IntE
  , predictorsE :: TE.MatrixE
  , projectionsE :: TE.MatrixE
  }

data AlphaModel = AlphaSimple | AlphaHierCentered | AlphaHierNonCentered deriving stock (Show)

alphaModelText :: AlphaModel -> Text
alphaModelText AlphaSimple = "AS"
alphaModelText AlphaHierCentered = "AHC"
alphaModelText AlphaHierNonCentered = "AHNC"

data Distribution = NormalDist -- | CauchyDist | StudentTDist

distributionText :: Distribution -> Text
distributionText NormalDist = "normal"
--distributionText CauchyDist = "cauchy"
--distributionText StudentTDist = "studentT"

data ModelConfig alphaK pd where
  ModelConfig :: Traversable pd
              => { projVecs :: DTP.NullVectorProjections
                 , standardizeNVs :: Bool
                 , alphaDMR :: DM.DesignMatrixRow alphaK
                 , predDMR :: DM.DesignMatrixRow (pd Double)
                 , alphaModel :: AlphaModel
                 , distribution :: Distribution
                 } -> ModelConfig alphaK pd

modelNumNullVecs :: ModelConfig alphaK md -> Int
modelNumNullVecs mc = fst $ LA.size $ DTP.nvpProj mc.projVecs

modelText :: ModelConfig alphaK md -> Text
modelText mc = distributionText mc.distribution <> "_" <> mc.alphaDMR.dmName <> "_" <> mc.predDMR.dmName <> "_" <> alphaModelText mc.alphaModel

dataText :: ModelConfig alphaK md -> Text
dataText mc = mc.alphaDMR.dmName <> "_" <> mc.predDMR.dmName <> "_NV" <> show (modelNumNullVecs mc)

projModelData :: forall pd alphaK rs . (Typeable rs)
              =>  ModelConfig alphaK pd
              -> (F.Record rs -> alphaK)
              -> (F.Record rs -> pd Double)
              -> SMB.StanBuilderM (ProjData rs) () (ProjModelData rs)
projModelData mc catKey predF = do
  projData <- SMB.dataSetTag @(ProjDataRow rs) SC.ModelData "ProjectionData"
  let projMER :: SMB.MatrixRowFromData (ProjDataRow r) --(outerK, md Double, VS.Vector Double)
      projMER = SMB.MatrixRowFromData "nvp" Nothing (modelNumNullVecs mc) (\(ProjDataRow _ v) -> VU.convert v)
      -- convert is here because we want unboxed vectors for JSON but hmatix uses storable vectors for FFI
  pmE <- SBB.add2dMatrixData projData projMER Nothing Nothing
  let nNullVecsE' = SMB.mrfdColumnsE projMER
      (_, nAlphasE') = DM.designMatrixColDimBinding mc.alphaDMR Nothing
  alphaDME <- if DM.rowLength mc.alphaDMR > 0
              then DM.addDesignMatrix projData (contramap (catKey . projRowRec) mc.alphaDMR) Nothing
              else pure $ TE.namedE "ERROR" TE.SMat -- this shouldn't show up in stan code at all
  let (_, nPredictorsE') = DM.designMatrixColDimBinding mc.predDMR Nothing
  dmE <- if DM.rowLength mc.predDMR > 0
         then DM.addDesignMatrix projData (contramap (predF . projRowRec) mc.predDMR) Nothing
         else pure $ TE.namedE "ERROR" TE.SMat -- this shouldn't show up in stan code at all
  pure $ ProjModelData projData nNullVecsE' nAlphasE' alphaDME nPredictorsE' dmE pmE

-- S states
-- K projections
-- C categories
-- D predictors
-- either an K row-vector or S x K matrix
--data Alpha0 = SimpleAlpha0 TE.RVectorE | HierarchicalAlpha0 TE.MatrixE
-- C x K matrix or array[S] of C x K matrix
data Alpha = SimpleAlpha TE.MatrixE | HierarchicalAlpha (TE.ArrayE TE.EMat)
-- D x K matrix or Nothing
newtype Theta = Theta (Maybe TE.MatrixE)
-- sigma is K row-vector
newtype Sigma = Sigma { unSigma :: TE.RVectorE }

data ProjModelParameters where
  NormalParameters :: Alpha -> Theta -> Sigma -> ProjModelParameters

paramTheta :: ProjModelParameters -> Theta
paramTheta (NormalParameters _ t _) = t

projModelAlpha :: ModelConfig alphaK pd -> ProjModelData rs -> SMB.StanBuilderM (ProjData rs) () Alpha
projModelAlpha mc pmd = do
  let f = DAG.parameterTagExpr
      nStatesE = SMB.groupSizeE stateG
      hierAlphaSpec = TE.array1Spec nStatesE (TE.matrixSpec pmd.nAlphasE pmd.nNullVecsE [])
      hierAlphaNDS = TE.NamedDeclSpec "alpha" hierAlphaSpec
      indexAK a k = TE.slice0 k . TE.slice0 a
      indexSAK s a k = TE.slice0  s . indexAK a k
      loopSAK stmtsF =
        TE.nestedLoops (TE.vftSized "s" nStatesE :> TE.vftSized "a" pmd.nAlphasE :> TE.vftSized "k" pmd.nNullVecsE :> TNil)
        $ \(s :> a :> k :> TNil) -> stmtsF s a k

--      loopNVs = TE.loopSized pmd.nNullVecsE "k" --TE.for "k" (TE.SpecificNumbered (TE.intE 1) pmd.nNullVecsE)
--      loopStates = TE.loopSized nStatesE "s" -- "TE.for "s" (TE.SpecificNumbered (TE.intE 1) nStatesE)
--      loopAlphas = TE.loopSized pmd.nAlphasE "a" -- TE.for "a" (TE.SpecificNumbered (TE.intE 1) pmd.nAlphasE)
--                       $ \(sE :> aE :> kE :> TNil) -> stmtF s a k
--      loopSAK stmtsF = loopStates $ \s -> [loopAlphas $ \a -> [loopNVs $ \k -> stmtsF s a k]]

      diagPostMult m cv = TE.functionE SF.diagPostMultiply (m :> cv :> TNil)
      rowsOf nRowsE rv = diagPostMult (TE.functionE SF.rep_matrix (TE.realE 1 :> nRowsE :> TE.functionE SF.size (rv :> TNil) :> TNil)) (TE.transposeE rv)
--      colsOf nColsE cv = diagPostMult (TE.functionE SF.rep_matrix (TE.realE 1 :> TE.functionE SF.size (cv :> TNil) :> nColsE) cv :> TNil)
      hierAlphaPs = do
        muAlphaP <- DAG.iidMatrixP
                    (TE.NamedDeclSpec "muAlpha" $ TE.matrixSpec pmd.nAlphasE pmd.nNullVecsE [])
                    [] TNil SF.std_normal
        sigmaAlphaP <- DAG.iidMatrixP
                       (TE.NamedDeclSpec "sigmaAlpha" $ TE.matrixSpec pmd.nAlphasE pmd.nNullVecsE [TE.lowerM $ TE.realE 0])
                       [] TNil SF.std_normal
        pure (DAG.build muAlphaP :> DAG.build sigmaAlphaP :> TNil)

  case mc.alphaModel of
    AlphaSimple -> do
      SimpleAlpha . f
        <$>  DAG.iidMatrixP
        (TE.NamedDeclSpec "alpha" $ TE.matrixSpec pmd.nAlphasE pmd.nNullVecsE [])
        [] TNil SF.std_normal
    AlphaHierCentered -> do
      alphaPs <- hierAlphaPs
      fmap (HierarchicalAlpha . f)
        $ DAG.addBuildParameter
        $ DAG.UntransformedP hierAlphaNDS [] alphaPs
        $ \(muAlphaE :> sigmaAlphaE :> TNil) m
          -> TE.addStmt
             $ loopSAK $ \s a k -> [TE.sample (indexSAK s a k m)  SF.normalS (indexAK a k muAlphaE :> indexAK a k sigmaAlphaE :> TNil)]
    AlphaHierNonCentered -> do
      alphaPs <- hierAlphaPs
      let rawNDS = TE.NamedDeclSpec (DAG.rawName $ TE.declName hierAlphaNDS) hierAlphaSpec

      rawP <- DAG.addBuildParameter
              $ DAG.UntransformedP rawNDS [] TNil
              $ \_ m -> TE.addStmt $ loopSAK $ \s a k -> [TE.sample (indexSAK s a k m) SF.std_normal TNil]
      fmap (HierarchicalAlpha . f)
        $ DAG.addBuildParameter
        $ DAG.simpleTransformedP hierAlphaNDS [] (DAG.BuildP rawP :> alphaPs)
        $ \(rmE :> muE :> sigmaE :> TNil) ->
            let inner pE s a k = [indexSAK s a k pE `TE.assign` (indexAK a k muE `TE.plusE` (indexAK a k sigmaE `TE.timesE` indexSAK s a k rmE))]
            in DAG.DeclCodeF $ TE.addStmt . loopSAK . inner

projModelParameters :: ModelConfig alphaK pd -> ProjModelData rs -> SMB.StanBuilderM (ProjData rs) () ProjModelParameters
projModelParameters mc pmd = do
  let stdNormalDWA :: (TE.TypeOneOf t [TE.EReal, TE.ECVec, TE.ERVec], TE.GenSType t) => TE.DensityWithArgs t
      stdNormalDWA = TE.DensityWithArgs SF.std_normal TNil --(TE.realE 0 :> TE.realE 1 :> TNil)
      f = DAG.parameterTagExpr
      numPredictors = DM.rowLength mc.predDMR
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
  alpha <- projModelAlpha mc pmd
  pure $ NormalParameters alpha theta sigma

data RunConfig = RunConfig { rcIncludePPCheck :: Bool, rcIncludeLL :: Bool }

projModel :: Typeable rs => RunConfig -> (F.Record rs -> alphaK) -> (F.Record rs -> pd Double) -> ModelConfig alphaK pd -> SMB.StanBuilderM  (ProjData rs) () ()
projModel rc alphaKey predF mc = do
  mData <- projModelData mc alphaKey predF
  mParams <- projModelParameters mc mData
  let betaNDS = TE.NamedDeclSpec "beta" $ TE.matrixSpec mData.nPredictorsE mData.nNullVecsE []
      nRowsE = SMB.dataSetSizeE mData.projDataTag
      loopNVs = TE.loopSized mData.nNullVecsE "k" --TE.for "k" (TE.SpecificNumbered (TE.intE 1) mData.nNullVecsE)
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
        $ \k -> [ (sds `TE.at` k) `TE.assign` TE.functionE SF.sd (mData.projectionsE `TE.atCol` k :> TNil)
                , stdNVPs `TE.atCol` k `TE.assign` (mData.projectionsE `TE.atCol` k `TE.divideE` (sds `TE.at` k))]
      let inverse :: (t ~ TEO.BinaryResultT TEO.BMultiply TE.EReal t) => TE.IntE -> TE.UExpr t -> TE.UExpr t --TE.UExpr (TEO.BinaryResultT TEO.BMultiply TE.EReal t)
          inverse k psCol = sds `TE.at` k `TE.timesE` psCol
      pure (stdNVPs, inverse)
    False -> pure (mData.projectionsE, const id)

  -- model
  let reIndexByState = TE.indexE TEI.s0 (SMB.byGroupIndexE mData.projDataTag stateG)
      -- given alpha and theta return an nData x nNullVecs matrix
      muE :: Alpha -> Theta -> SMB.StanBuilderM (ProjData r) () TE.MatrixE
      muE a t =
        let thetaME = case t of
              Theta Nothing -> TE.functionE SF.rep_matrix (TE.realE 0 :> nRowsE :> mData.nNullVecsE :> TNil)
              Theta (Just theta) -> predM `TE.timesE` theta
        in case a of
             SimpleAlpha alpha -> pure $ mData.alphasE `TE.timesE` alpha  `TE.plusE` thetaME
             HierarchicalAlpha alpha -> SMB.addFromCodeWriter $ do
               mu <- TE.declareNW $ TE.NamedDeclSpec "mu" $ TE.matrixSpec nRowsE mData.nNullVecsE []
               TE.addStmt $ TE.loopSized nRowsE "n" $ \n ->
                 [(mu `TE.atRow` n)
                  `TE.assign`
                  ((mData.alphasE `TE.atRow` n) `TE.timesE` ((reIndexByState alpha) `TE.at` n) `TE.plusE` (thetaME `TE.atRow` n))]
               pure mu
      sigmaE :: Sigma -> TE.IntE -> TE.VectorE
      sigmaE s k = TE.functionE SF.rep_vector (unSigma s `TE.at` k :> nRowsE :> TNil)

      ppF :: TE.MatrixE
          -> Int
          -> ((TE.IntE -> TE.ExprList xs) -> TE.IntE -> TE.UExpr TE.EReal)
          -> (TE.MatrixE -> TE.IntE -> TE.CodeWriter (TE.IntE -> TE.ExprList xs))
          -> SMB.StanBuilderM (ProjData r) () (TE.ArrayE TE.EReal)
      ppF muMat k rngF rngPSCW = SBB.generatePosteriorPrediction'
                                 mData.projDataTag
                                 (TE.NamedDeclSpec ("predProj_" <> show k) $ TE.array1Spec nRowsE $ TE.realSpec [])
                                 rngF
                                 (rngPSCW muMat (TE.intE k))
                                 (\_ p -> inverseF (TE.intE k) p)

      (muMatBuilder, sampleStmtF, ppStmtF) = case mParams of
        NormalParameters a t s ->
          let ssF e muMat k = TE.sample e SF.normal (muMat `TE.atCol` k :> sigmaE s k :> TNil)
              rF f nE = TE.functionE SF.normal_rng (f nE)
              rpF muMat k = pure $ \nE -> muMat `TE.atCol` k `TE.at` nE :> sigmaE s k `TE.at` nE :> TNil
          in (muE a t, ssF, \muMat n -> ppF muMat n rF rpF)

  SMB.inBlock SMB.SBModel $ do
    muMat <- muMatBuilder
    SMB.addFromCodeWriter $ do
      let loopBody k = TE.writerL' $ TE.addStmt $ sampleStmtF (nvps `TE.atCol` k) muMat k
      TE.addStmt $ loopNVs loopBody
  -- generated quantities
  when rc.rcIncludePPCheck $ do
    muMat <- SMB.inBlock SMB.SBGeneratedQuantities muMatBuilder
    forM_ [1..modelNumNullVecs mc] (ppStmtF muMat)
  pure ()


runProjModel :: forall (ksO :: [(Symbol, Type)]) ksM pd r .
                (K.KnitEffects r
                , BRKU.CacheEffects r
                , ksM F.⊆ DDP.ACSModelRow
                , ksO F.⊆ DDP.ACSByPUMAR
                , ksO F.⊆ DDP.ACSModelRow
                , Typeable pd
                , Ord (F.Record ksO)
                , BRK.FiniteSet (F.Record ksO)
--                , Flat.Flat (pd [SlopeIntercept])
                )
             => Bool
             -> BR.CommandLine
             -> RunConfig
             -> ModelConfig (F.Record ksM) pd
             -> DMS.MarginalStructure (F.Record ksO)
             -> (F.Record DDP.ACSModelRow -> pd Double)
             -> K.Sem r (K.ActionWithCacheTime r ())
runProjModel clearCaches _cmdLine rc mc ms predF = do
  let cacheDirE = (if clearCaches then Left else Right) "model/demographic/nullVecProjModel/"
      dataName = "projectionData_" <> dataText mc
      runnerInputNames = SC.RunnerInputNames
                         ("br-2022-Demographics/stan/nullVecProj2")
                         (modelText mc)
                         (Just $ SC.GQNames "pp" dataName) -- posterior prediction vars to wrap
                         dataName
  acs_C <- DDP.cachedACS
  acsByPUMA_C <- DDP.cachedACSByPUMA
  let outerKey :: ([GT.StateAbbreviation, GT.PUMA] F.⊆ qs) => F.Record qs -> F.Record [GT.StateAbbreviation, GT.PUMA]
      outerKey = F.rcast
      catKeyO :: (ksO F.⊆ qs) => F.Record qs -> F.Record ksO
      catKeyO = F.rcast
      catKeyM :: (ksM F.⊆ qs) => F.Record qs -> F.Record ksM
      catKeyM = F.rcast
      count = realToFrac . view DT.popCount
      pdByPUMA_C = fmap (FL.fold (productDistributionFld ms outerKey catKeyO count)) acsByPUMA_C
      projDataE pdByPUMA acs =
        let projRowsE = rowsWithProjectedDiffs mc.projVecs pdByPUMA outerKey catKeyO $ FL.fold FL.list acs
        in ProjData (modelNumNullVecs mc) (DM.rowLength mc.predDMR) <$> projRowsE
      projDataE_C = projDataE <$> pdByPUMA_C <*> acs_C
      modelData_C = K.wctBind K.knitEither projDataE_C
      meanSDFld :: FL.Fold Double (Double, Double) = (,) <$> FL.mean <*> FL.std
      meanSDFlds :: Int -> FL.Fold [Double] [(Double, Double)]
      meanSDFlds m = traverse (\n -> FL.premap (List.!! n) meanSDFld) [0..(m - 1)]
  modelData <- K.ignoreCacheTime modelData_C
  let meanSDs = FL.fold (FL.premap (\(ProjDataRow _ v) -> VS.toList v) $ meanSDFlds (modelNumNullVecs mc)) $ pdRows modelData
  K.logLE K.Info $ "meanSDs=" <> show meanSDs
  states <-  FL.fold (FL.premap (view GT.stateAbbreviation) FL.set) <$> K.ignoreCacheTime acsByPUMA_C
  (dw, code) <-  SMR.dataWranglerAndCode modelData_C (pure ())
                (stateGroupBuilder (view GT.stateAbbreviation)  (S.toList states))
                (projModel rc catKeyM predF mc)

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
           SC.DoNothing
           (SMR.ShinyStan unwraps) --(SMR.Both [SR.UnwrapNamed "successes" "yObserved"])
           modelData_C
           (pure ())
  K.logLE K.Info "projModel run complete."
  pure res_C

newtype PModel1 a = PModel1 { pdLogDensity :: a }
  deriving stock (Show, Functor, Foldable, Traversable, Generic)
  deriving anyclass Flat.Flat

instance Applicative PModel1 where
  pure = PModel1
  (PModel1 f) <*> (PModel1 x) = PModel1 (f x)

data PModel0 a = PModel0
  deriving stock (Show, Functor, Foldable, Traversable, Generic)
  deriving anyclass Flat.Flat

instance Applicative PModel0 where
  pure _ = PModel0
  PModel0 <*> PModel0 = PModel0


designMatrixRow1 :: DM.DesignMatrixRow (PModel1 Double)
designMatrixRow1 = DM.DesignMatrixRow "Model1" [DM.DesignMatrixRowPart "logDensity" 1 (VU.singleton . pdLogDensity)]


designMatrixRow0 :: DM.DesignMatrixRow (PModel0 Double)
designMatrixRow0 = DM.DesignMatrixRow "PModel0" []


designMatrixRow_1_S_E_R :: DM.DesignMatrixRow (F.Record [DT.SexC, DT.Education4C, DT.Race5C])
designMatrixRow_1_S_E_R = DM.DesignMatrixRow "S_E_R" [cRP, sRP, eRP, rRP]
  where
    cRP = DM.DesignMatrixRowPart "Ones" 1 (const $ VU.singleton 1) -- for pure (state-level) alpha
    sRP = DM.boundedEnumRowPart Nothing "Sex" (view DT.sexC)
    eRP = DM.boundedEnumRowPart (Just DT.E4_HSGrad) "Edu" (view DT.education4C)
    rRP = DM.boundedEnumRowPart (Just DT.R5_WhiteNonHispanic) "Race" (view DT.race5C)

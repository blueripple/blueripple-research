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
                       -> (row -> outerK)
                       -> (row -> k)
                       -> g row
                       -> Either Text (g (row, VS.Vector Double))
rowsWithProjectedDiffs nvps pdMap outerKey catKey = traverse (traverseToSnd $ rowDiffProjections nvps pdMap outerKey catKey)

type ProjDataRow r = (r, VS.Vector Double)
data ProjData r = ProjData {pdNNullVecs :: Int, pdNPredictors :: Int, pdRows :: [ProjDataRow r]}

data SlopeIntercept = SlopeIntercept { siSlope :: Double, siIntercept :: Double}

applySlopeIntercept :: SlopeIntercept -> Double -> Double
applySlopeIntercept (SlopeIntercept s i) x = i + s * x
{-# INLINEABLE applySlopeIntercept #-}

newtype ModelResult g k (pd :: Type -> Type) = ModelResult { unModelResult :: Map g (Map k (VS.Vector Double), pd [SlopeIntercept]) }


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
  pure $ alphaV + betaV

stateG :: SMB.GroupTypeTag Text
stateG = SMB.GroupTypeTag "State"

stateGroupBuilder :: (Foldable f, Typeable r)
                  => (r -> Text) -> f Text -> SMB.StanGroupBuilderM (ProjData r) () ()
stateGroupBuilder saF states = do
  projData <- SMB.addModelDataToGroupBuilder "ProjectionData" (SMB.ToFoldable pdRows)
  SMB.addGroupIndexForData stateG projData $ SMB.makeIndexFromFoldable show (saF . fst) states
  SMB.addGroupIntMapForDataSet stateG projData $ SMB.dataToIntMapFromFoldable (saF . fst) states

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

projModelData :: forall pd alphaK r . (Typeable r, Typeable pd)
              =>  ModelConfig alphaK pd
              -> (r -> alphaK)
              -> (r -> pd Double)
              -> SMB.StanBuilderM (ProjData r) () (ProjModelData r)
projModelData mc catKey predF = do
  projData <- SMB.dataSetTag @(ProjDataRow r) SC.ModelData "ProjectionData"
  let projMER :: SMB.MatrixRowFromData (ProjDataRow r) --(outerK, md Double, VS.Vector Double)
      projMER = SMB.MatrixRowFromData "nvp" Nothing (modelNumNullVecs mc) (\(_, v) -> VU.convert v)
      -- convert is here because we want unboxed vectors for JSON but hmatix uses storable vectors for FFI
  pmE <- SBB.add2dMatrixData projData projMER Nothing Nothing
  let nNullVecsE' = SMB.mrfdColumnsE projMER
      (_, nAlphasE') = DM.designMatrixColDimBinding mc.alphaDMR Nothing
  alphaDME <- if DM.rowLength mc.alphaDMR > 0
              then DM.addDesignMatrix projData (contramap (catKey . fst) mc.alphaDMR) Nothing
              else pure $ TE.namedE "ERROR" TE.SMat -- this shouldn't show up in stan code at all
  let (_, nPredictorsE') = DM.designMatrixColDimBinding mc.predDMR Nothing
  dmE <- if DM.rowLength mc.predDMR > 0
         then DM.addDesignMatrix projData (contramap (predF . fst) mc.predDMR) Nothing
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

projModelAlpha :: ModelConfig alphaK pd -> ProjModelData r -> SMB.StanBuilderM (ProjData r) () Alpha
projModelAlpha mc pmd = do
  let stdNormalDWA :: (TE.TypeOneOf t [TE.EReal, TE.ECVec, TE.ERVec], TE.GenSType t) => TE.DensityWithArgs t
      stdNormalDWA = TE.DensityWithArgs SF.std_normal TNil --(TE.realE 0 :> TE.realE 1 :> TNil)
      f = DAG.parameterTagExpr
      nStatesE = SMB.groupSizeE stateG
      numCats = DM.rowLength mc.pred
      hierAlphaNDS = TE.NamedDeclSpec "alpha" $ TE.arraySpec nStatesE (TE.matrixSpec pmd.nAlphasE pmd.nNullVecsE) []
      fstI x k = TE.sliceE TEI.s0 k x
      loopNVs = TE.for "k" (TE.SpecificNumbered (TE.intE 1) pmd.nNullVecsE)
      diagPostMult m cv = TE.functionE SF.diagPostMultiply (m :> cv :> TNil)
      rowsOf nRowsE rv = diagPostMult (TE.functionE SF.rep_matrix (TE.realE 1 :> nRowsE :> TE.functionE SF.size (rv :> TNil) :> TNil)) (TE.transposeE rv)
--      colsOf nColsE cv = diagPostMult (TE.functionE SF.rep_matrix (TE.realE 1 :> TE.functionE SF.size (cv :> TNil) :> nColsE) cv :> TNil)
      hierAlphaPs = do
        muAlphaP <- DAG.iidMatrixP
                    (TE.NamedDeclSpec "muAlpha" $ TE.matrixSpec pmd.nAlphasE pmd.nNullVecsE [])
                    [] TNil (SF.Density SF.std_normal)
        sigmaAlphaP <- DAG.iidMatrixP
                       (TE.NamedDeclSpec "sigmaAlpha" $ TE.matrixSpec pmd.nAlphasE pmd.nNullVecsE [TE.lowerM $ TE.realE 0])
                       [] TNil (SF.Density SF.std_normal)
        pure (DAG.build muAlphaP :> DAG.build sigmaAlphaP :> TNil)

  case mc.alphaModel of
    AlphaSimple -> do
      SimpleAlpha . f
        <$>  DAG.iidMatrixP
        (TE.NamedDeclSpec "alpha" $ TE.matrixSpec pmd.nAlphasE pmd.nNullVecsE [])
        [] TNil (SF.Density SF.std_normal)
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
      fmap (HierarchicalAlpha0 . f)
        $ DAG.withIIDRawMatrix hierAlphaNDS Nothing stdNormalDWA alphaPs
        $ \(muAlphaE :> sigmaAlphaE :> TNil) rawM -> rowsOf nStatesE muAlphaE `TE.plusE` diagPostMult rawM (TE.transposeE sigmaAlphaE)


{-
projModelParameters :: ModelConfig alphaK pd -> ProjModelData r -> SMB.StanBuilderM (ProjData r) () ProjModelParameters
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

  let nStatesE = SMB.groupSizeE stateG
      hierAlpha0NDS = TE.NamedDeclSpec "alpha0" $ TE.matrixSpec nStatesE pmd.nNullVecsE []
      fstI x k = TE.sliceE TEI.s0 k x
      loopNVs = TE.for "k" (TE.SpecificNumbered (TE.intE 1) pmd.nNullVecsE)
      diagPostMult m cv = TE.functionE SF.diagPostMultiply (m :> cv :> TNil)
      rowsOf nRowsE rv = diagPostMult (TE.functionE SF.rep_matrix (TE.realE 1 :> nRowsE :> TE.functionE SF.size (rv :> TNil) :> TNil)) (TE.transposeE rv)
--      colsOf nColsE cv = diagPostMult (TE.functionE SF.rep_matrix (TE.realE 1 :> TE.functionE SF.size (cv :> TNil) :> nColsE) cv :> TNil)
      hierAlpha0Ps = do
        muAlphaP <- DAG.simpleParameterWA
                    (TE.NamedDeclSpec "muAlpha0" $ TE.rowVectorSpec pmd.nNullVecsE [])
                    stdNormalDWA
        sigmaAlphaP <-  DAG.simpleParameterWA
                        (TE.NamedDeclSpec "sigmaAlpha0" $ TE.rowVectorSpec pmd.nNullVecsE [TE.lowerM $ TE.realE 0])
                        stdNormalDWA
        pure (DAG.build muAlphaP :> DAG.build sigmaAlphaP :> TNil)

  alpha0 <- case mc.alphaModel of
    AlphaSimple -> do
      SimpleAlpha0 . f
        <$> DAG.simpleParameterWA
           (TE.NamedDeclSpec "alpha0" $ TE.rowVectorSpec pmd.nNullVecsE [])
           stdNormalDWA
    AlphaHierCentered -> do
      alphaPs <- hierAlphaPs
      fmap (HierarchicalAlpha0 . f)
        $ DAG.addBuildParameter
        $ DAG.UntransformedP hierAlphaNDS [] alphaPs
        $ \(muAlphaE :> sigmaAlphaE :> TNil) m
          -> TE.addStmt
             $ loopNVs
             $ \k -> [TE.sample (m `fstI` k) SF.normalS (muAlphaE `fstI` k :> sigmaAlphaE `fstI` k :> TNil)]
    AlphaHierNonCentered -> do
      alphaPs <- hierAlphaPs
      fmap (HierarchicalAlpha0 . f)
        $ DAG.withIIDRawMatrix hierAlphaNDS Nothing stdNormalDWA alphaPs
        $ \(muAlphaE :> sigmaAlphaE :> TNil) rawM -> rowsOf nStatesE muAlphaE `TE.plusE` diagPostMult rawM (TE.transposeE sigmaAlphaE)

  alphaCat <- case mc.alphaModel of
    AlphaSimple -> do
      SimpleAlpha . f
      <$> DAG.simpleParameterWA
        (TE.NamedDeclSpec)

      numAs = DM.rowLength mc.alphaDMR
-}
newtype PModel1 a = PModel1 { pdLogDensity :: a }
  deriving stock (Show, Functor, Foldable, Traversable, Generic)
  deriving anyclass Flat.Flat

instance Applicative PModel1 where
  pure = PModel1
  (PModel1 f) <*> (PModel1 x) = PModel1 (f x)

designMatrixRow1 :: DM.DesignMatrixRow (PModel1 Double)
designMatrixRow1 = DM.DesignMatrixRow "Model1" [DM.DesignMatrixRowPart "logDensity" 1 (VU.singleton . pdLogDensity)]


designMatrixRow_1_S_E_R :: DM.DesignMatrixRow (F.Record [DT.SexC, DT.Education4C, DT.Race5C])
designMatrixRow_1_S_E_R = DM.DesignMatrixRow "S_E_R" [cRP, sRP, eRP, rRP]
  where
    cRP = DM.DesignMatrixRowPart "Ones" 1 (const 1) -- for pure (state-level) alpha
    sRP = DM.boundedEnumRowPart Nothing "Sex" (view DT.sexC)
    eRP = DM.boundedEnumRowPart (Just DT.E4_HSGrad) "Edu" (view DT.education4C)
    rRP = DM.boundedEnumRowPart (Just DT.R5_WhiteNonHispanic) "Race" (view DT.race5C)

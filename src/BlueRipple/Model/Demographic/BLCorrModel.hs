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

module BlueRipple.Model.Demographic.BLCorrModel
  (
    module BlueRipple.Model.Demographic.BLCorrModel
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
import qualified Data.Vinyl.TypeLevel as V

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
import qualified Stan.ModelBuilder.Distributions as SD
import qualified Stan.ModelBuilder.TypedExpressions.Types as TE
import qualified Stan.ModelBuilder.TypedExpressions.Statements as TE
import qualified Stan.ModelBuilder.TypedExpressions.Indexing as TEI
import qualified Stan.ModelBuilder.TypedExpressions.Operations as TEO
import qualified Stan.ModelBuilder.TypedExpressions.DAG as DAG
import qualified Stan.ModelBuilder.TypedExpressions.StanFunctions as SF
import Stan.ModelBuilder.TypedExpressions.TypedList (TypedList(..))
import qualified Flat
import qualified BlueRipple.Utilities.KnitUtils as BRK


marginalFld :: Ord (F.Record ms) => (F.Record (rs V.++ ms) -> Int) -> FL.Fold (F.Record (rs V.++ ms)) [(F.Record rs, Map (F.Record ms) Int)]
marginalFld countF = MR.mapReduceFold
                     MR.noUnpack
                     (MR.assign F.rcast (\r -> (F.rcast r, countF r)))
                     (MR.foldAndLabel (FL.foldByKeyMap FL.sum) (,))



data DataRow rs = DataRow (F.Record rs) [Int]

dataRowRec :: DataRow rs -> F.Record rs
dataRowRec (DataRow r _) = r

dataRowCounts :: DataRow rs -> [Int]
dataRowCounts (DataRow _ ms) = ms

instance (V.RMap rs, FS.RecFlat rs) => Flat.Flat (DataRow rs) where
  size (DataRow r ms) = Flat.size (FS.toS r, ms)
  encode (DataRow r ms) = Flat.encode (FS.toS r, ms)
  decode = fmap (\(sr, ms) -> DataRow (FS.fromS sr) ms) Flat.decode

type DataRows rs = [DataRow rs]
--deriving anyclass instance (Flat.Flat (ProjDataRow rs)) => Flat.Flat (ProjData rs)
{-
--Figure out results section once we see if this model is well-behaved

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
-}

data AlphaModel = AlphaSimple | AlphaHierCentered | AlphaHierNonCentered deriving stock (Show, Eq, Generic)
data ParamCovarianceStructure = DiagonalCovariance | LKJCovariance Int deriving stock (Show, Eq, Generic)

alphaModelText :: AlphaModel -> Text
alphaModelText AlphaSimple = "AS"
alphaModelText AlphaHierCentered = "AHC"
alphaModelText AlphaHierNonCentered = "AHNC"

paramCovarianceText :: ParamCovariance -> Text
paramCovarianceText DiagonalCovariance = "diagCov"
paramCovarianceText (LKJCovariance n) = "lkj" <> show n

data ModelConfig alphaK pd where
  ModelConfig :: Traversable pd
              => { nCounts :: Int
                 , alphaDMR :: DM.DesignMatrixRow alphaK
                 , predDMR :: DM.DesignMatrixRow (pd Double)
                 , alphaModel :: AlphaModel
                 , pCov :: ParamCovariance
                 } -> ModelConfig alphaK pd

modelText :: ModelConfig alphaK md -> Text
modelText mc = mc.alphaDMR.dmName
               <> "_" <> mc.predDMR.dmName
               <> "_" <> alphaModelText mc.alphaModel
               <> "_" <> alphaCovarianceText mc.alphaCov

dataText :: ModelConfig alphaK md -> Text
dataText mc = mc.alphaDMR.dmName <> "_" <> mc.predDMR.dmName <> "_N" <> show (nCounts mc)

stateG :: SMB.GroupTypeTag Text
stateG = SMB.GroupTypeTag "State"

stateGroupBuilder :: (Foldable f, Typeable rs)
                  => (F.Record rs -> Text) -> f Text -> SMB.StanGroupBuilderM (DataRows rs) () ()
stateGroupBuilder saF states = do
  projData <- SMB.addModelDataToGroupBuilder "CountData" (SMB.ToFoldable id)
  SMB.addGroupIndexForData stateG projData $ SMB.makeIndexFromFoldable show (saF . projRowRec) states
  SMB.addGroupIntMapForDataSet stateG projData $ SMB.dataToIntMapFromFoldable (saF . projRowRec) states

data ModelData r =
  ModelData
  {
    dataTag :: SMB.RowTypeTag (DataRow r)
  , nCatsE :: TE.IntE
  , countsE :: TE.ArrayE (TE.EArray1 TE.EInt)
  , nAlphasE :: TE.IntE
  , alphasE :: TE.MatrixE
  , nPredictorsE :: TE.IntE
  , predictorsE :: TE.MatrixE
  }

modelData :: forall pd alphaK rs . (Typeable rs)
          =>  ModelConfig alphaK pd
          -> (F.Record rs -> alphaK)
          -> (F.Record rs -> pd Double)
          -> SMB.StanBuilderM (DataRows rs) () (ModelData rs)
modelData mc catKey predF = do
  dat <- SMB.dataSetTag @(DataRows rs) SC.ModelData "MarginalData"
  (countsE', nCatsE') <- SBB.addArrayOfIntArrays  dat "MCounts" Nothing mc.nCounts dataRowCounts (Just 0) Nothing
  let (_, nAlphasE') = DM.designMatrixColDimBinding mc.alphaDMR Nothing
  alphaDME <- if DM.rowLength mc.alphaDMR > 0
              then DM.addDesignMatrix dat (contramap (catKey . projRowRec) mc.alphaDMR) Nothing
              else pure $ TE.namedE "ERROR" TE.SMat -- this shouldn't show up in stan code at all
  let (_, nPredictorsE') = DM.designMatrixColDimBinding mc.predDMR Nothing
  dmE <- if DM.rowLength mc.predDMR > 0
         then DM.addDesignMatrix dat (contramap (predF . projRowRec) mc.predDMR) Nothing
         else pure $ TE.namedE "ERROR" TE.SMat -- this shouldn't show up in stan code at all
  pure $ ProjModelData dat nCatsE' countsE' nAlphasE' alphaDME nPredictorsE' dmE

-- S states
-- K categories to count
-- C categories to use for prediction
-- D predictors
-- M x K matrix or S array of M x K matrix. M=Number of cols is < 1 + C since we binary or one-hot encode all the categories
data Alpha = SimpleAlpha TE.MatrixE | HierarchicalAlpha (TE.ArrayE TE.EMat)
-- D X K vector or Nothing
newtype Theta = Theta (Maybe TE.MatrixE)

data ModelParameters where
  ModelParameters :: Alpha -> Theta -> ModelParameters

paramTheta :: ModelParameters -> Theta
paramTheta (ModelParameters _ t _) = t



modelAlpha :: ModelConfig alphaK pd -> ModelData rs -> SMB.StanBuilderM (DataRows rs) () Alpha
modelAlpha mc pmd = do
  let f = DAG.parameterTagExpr
  -- sigma structure for each column of alpha
  lkjCorrAlpha <- DAG.simpleParameter (TE.NamedDeclSpec "lkjAlpha" $ TE.choleskyFactorCorrSpec pmd.nAlphasE [])
                  (TE.realE 4 :> TNil) SF.lkj_corr_cholesky
  sigmaAlpha <- DAG.simpleParameter (TE.NamedDeclSpec "sigmaAlpha" $ TE.vectorSpec pmd.nAlphasE []) TNil SF.std_normal

  let nStatesE = SMB.groupSizeE stateG
      hierAlphaSpec =  TE.array1Spec nStatesE (TE.matrixSpec pmd.nAlphasE pmd.nCatsE [])
      hierAlphaNDS = TE.NamedDeclSpec "alpha" hierAlphaSpec
      hierAlphaPs = do
        muAlphaP <- DAG.simpleParameter
                    (TE.NamedDeclSpec "muAlpha" $ TE.matrixSpec pmd.nAlphasE pmd.nCatsE [])
                    TNil SF.std_normal
        sigmaAlphaP <- simpleParameter
                       (TE.NamedDeclSpec "sigmaAlpha" $ TE.matrixSpec pmd.nAlphasE pmd.nCatsE [TE.lowerM $ TE.realE 0])
                       TNil SF.std_normal
        pure (DAG.build muAlphaP :> DAG.build sigmaAlphaP :> TNil)

  case mc.alphaModel of
    AlphaSimple -> do
      SimpleAlpha . f
        <$>  DAG.simpleParameter
        (TE.NamedDeclSpec "alpha" $ TE.matrixSpec pmd.nAlphasE pmd.nCatsE [])
        TNil SF.std_normal
    AlphaHierCentered -> do
      alphaPs <- hierAlphaPs
      fmap (HierarchicalAlpha . f)
        $ DAG.addCenteredHierarchical
        hierAlphaNDS alphaPs SF.normal
    AlphaHierNonCentered -> do
      alphaPs <- hierAlphaPs
      let repMatrix v nRows = TE.functionE SF.repV_matrix (v :> nRows :> TNil)
          diagPostMultiply m v = TE.functionE SF.diagPostMultiply (m :> v :> TNil)
      fmap (HierarchicalAlpha . f)
      $ DAG.simpleNonCentered hierAlphaNDS hierAlphaSpec (TE.DensityWithArgs SF.std_normal TNil) alphaPs
      $ \(mu :> sigma :> TNil) rawM -> repMatrix mu mStatesE `TE.plusE` diagPostMultiply rawM sigma

modelParameters :: ModelConfig alphaK pd -> ModelData rs -> SMB.StanBuilderM (DataRows rs) () ModelParameters
modelParameters mc pmd = do
  let stdNormalDWA :: (TE.TypeOneOf t [TE.EReal, TE.ECVec, TE.ERVec], TE.GenSType t) => TE.DensityWithArgs t
      stdNormalDWA = TE.DensityWithArgs SF.std_normal TNil --(TE.realE 0 :> TE.realE 1 :> TNil)
      f = DAG.parameterTagExpr
      numPredictors = DM.rowLength mc.predDMR
  theta <- if numPredictors > 0 then
               (Theta . Just . f)
               <$> DAG.simpleParameter
               (TE.NamedDeclSpec "theta" $ TE.matrixSpec pmd.nPredictorsE [])
               TNil SF.std_normal
           else pure $ Theta Nothing

  alpha <- projModelAlpha mc pmd
  pure $ NormalParameters alpha theta

data RunConfig = RunConfig { rcIncludePPCheck :: Bool, rcIncludeLL :: Bool }

projModel :: Typeable rs
          => RunConfig
          -> (F.Record rs -> alphaK)
          -> (F.Record rs -> Int)
          -> (F.Record rs -> pd Double)
          -> ModelConfig alphaK pd
          -> SMB.StanBuilderM  (DataRows rs) () ()
projModel rc alphaKey countF predF mc = do
  mData <- modelData mc alphaKey predF
  mParams <- modelParameters mc mData
  let betaNDS = TE.NamedDeclSpec "beta" $ TE.matrixSpec mData.nPredictorsE mData.nNullVecsE []
  -- transformedData
  (predM, _centerF, _mBeta) <- case paramTheta mParams of
    Theta (Just thetaE) -> do
      (centeredPredictorsE, centerF) <- DM.centerDataMatrix DM.DMCenterOnly mData.predictorsE Nothing "DM"
      (dmQ, _, _, mBeta) <- DM.thinQR centeredPredictorsE "DM" $ Just (thetaE, betaNDS)
      pure (dmQ, centerF, mBeta)
    Theta Nothing -> pure (TE.namedE "ERROR" TE.SMat, \_ x _ -> pure x, Nothing)
  -- model
  let reIndexByState = TE.indexE TEI.s0 (SMB.byGroupIndexE mData.projDataTag stateG)
      -- given alpha and theta return an nData x nNullVecs matrix
      muE :: Alpha -> Theta -> SMB.StanBuilderM (ProjData r) () TE.MatrixE
      muE a t = SMB.addFromCodeWriter $ do
        let mTheta = case t of
              Theta x -> fmap (\t -> predM `TE.timesE` t) x
            muSpec = TE.NamedDeclSpec "mu" $ TE.matrixSpec nRowsE mData.nNullVecsE []
        case a of
          SimpleAlpha alpha -> case mTheta of
            Nothing -> TE.declareRHSNW muSpec (alphasE mData `TE.timesE` alpha)
            Just x -> TE.declareRHSNW muSpec (alphasE mData `TE.timesE` alpha `TE.plusE` x)
          HierarchicalAlpha alpha -> do
            mu <- TE.declareNW muSpec
            TE.addStmt $ TE.loopSized nRowsE "n" $ \n ->
              [(mu `TE.atRow` n)
                `TE.assign`
                (case mTheta of
                    Nothing -> (mData.alphasE `TE.atRow` n) `TE.timesE` ((reIndexByState alpha) `TE.at` n)
                    Just mt -> (mData.alphasE `TE.atRow` n) `TE.timesE` ((reIndexByState alpha) `TE.at` n) `TE.plusE` (mt `TE.atRow` n)
                )]
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
      eltTimes = TE.binaryOpE (TEO.SElementWise TEO.SMultiply)
      (muMatBuilder, sampleStmtF, ppStmtF) = case mParams of
        NormalParameters a t s ->
          let ssF e muMat k = SD.familySample SD.normalDist e (muMat `TE.atCol` k :> sigmaE s k :> TNil)
                --SD.familySample SD.countScaledNormalDist e (countsVec :> muMat `TE.atCol` k :> sigmaE s k :> TNil)
              rF f nE = SD.familyRNG SD.countScaledNormalDist (f nE) --TE.functionE SF.normal_rng (f nE)
              rpF muMat k = pure $ \nE -> countsVec `TE.at` nE :> muMat `TE.atCol` k `TE.at` nE :> sigmaE s k `TE.at` nE :> TNil
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
                , ksM F.⊆ DDP.ACSByPUMAR
                , ksO F.⊆ DDP.ACSByPUMAR
                , Typeable pd
                , Ord (F.Record ksO)
                , BRK.FiniteSet (F.Record ksO)
--                , Flat.Flat (pd [SlopeIntercept])
                )
             => Bool
             -> Maybe Int
             -> BR.CommandLine
             -> RunConfig
             -> ModelConfig (F.Record ksM) pd
             -> DMS.MarginalStructure (F.Record ksO)
             -> (F.Record DDP.ACSByPUMAR -> pd Double)
             -> K.Sem r (K.ActionWithCacheTime r ())
runProjModel clearCaches thinM _cmdLine rc mc ms predF = do
  let cacheRoot = "model/demographic/nullVecProjModel/"
      cacheDirE = (if clearCaches then Left else Right) cacheRoot
      dataName = "projectionData_" <> dataText mc
      runnerInputNames = SC.RunnerInputNames
                         ("br-2022-Demographics/stan/nullVecProj2")
                         (modelText mc)
                         (Just $ SC.GQNames "pp" dataName) -- posterior prediction vars to wrap
                         dataName
  acsByPUMA_C <- DDP.cachedACSByPUMA
  let outerKey :: ([GT.StateAbbreviation, GT.PUMA] F.⊆ qs) => F.Record qs -> F.Record [GT.StateAbbreviation, GT.PUMA]
      outerKey = F.rcast
      catKeyO :: (ksO F.⊆ qs) => F.Record qs -> F.Record ksO
      catKeyO = F.rcast
      catKeyM :: (ksM F.⊆ qs) => F.Record qs -> F.Record ksM
      catKeyM = F.rcast
      count = view DT.popCount
      takeEach n = fmap snd . List.filter ((== 0) . flip mod n . fst) . zip [0..]
      thin = maybe id takeEach thinM
      dataCacheKey = cacheRoot <> "/projModelData.bin"
  let projDataF acsByPUMA = do
        let pdByPUMA = FL.fold (productDistributionFld ms outerKey catKeyO (realToFrac . count)) acsByPUMA
        projRows <- K.knitEither $ rowsWithProjectedDiffs mc.projVecs pdByPUMA outerKey catKeyO $ thin $ FL.fold FL.list acsByPUMA
        pure $ ProjData (modelNumNullVecs mc) (DM.rowLength mc.predDMR) projRows
  when clearCaches $ BRK.clearIfPresentD dataCacheKey
  modelData_C <- BRKU.retrieveOrMakeD (cacheRoot <> "/projModelData.bin") acsByPUMA_C projDataF
  let meanSDFld :: FL.Fold Double (Double, Double) = (,) <$> FL.mean <*> FL.std
      meanSDFlds :: Int -> FL.Fold [Double] [(Double, Double)]
      meanSDFlds m = traverse (\n -> FL.premap (List.!! n) meanSDFld) [0..(m - 1)]
  modelData <- K.ignoreCacheTime modelData_C
  let meanSDs = FL.fold (FL.premap (\(ProjDataRow _ v) -> VS.toList v) $ meanSDFlds (modelNumNullVecs mc)) $ pdRows modelData
  K.logLE K.Info $ "meanSDs=" <> show meanSDs
  states <-  FL.fold (FL.premap (view GT.stateAbbreviation) FL.set) <$> K.ignoreCacheTime acsByPUMA_C
  (dw, code) <-  SMR.dataWranglerAndCode modelData_C (pure ())
                (stateGroupBuilder (view GT.stateAbbreviation)  (S.toList states))
                (projModel rc catKeyM count predF mc)

  let nNullVecs = modelNumNullVecs mc
      unwraps = (\n -> SR.UnwrapExpr ("matrix(ncol="
                                       <> show nNullVecs
                                       <> ", byrow=TRUE, unlist(jsonData $ nvp_ProjectionData))[,"
                                       <> show n <> "]") ("obsNVP_" <> show n))
                <$> [1..nNullVecs]
  res_C <- SMR.runModel' @BRKU.SerializerC @BRKU.CacheData
           cacheDirE
           (Right runnerInputNames)
           (Just $ SC.StanMCParameters 4 4 (Just 1000) (Just 1000) Nothing Nothing (Just 1))
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

designMatrixRow_1 :: DM.DesignMatrixRow (F.Record '[DT.Education4C])
designMatrixRow_1 = DM.DesignMatrixRow "Base" [cRP]
  where
    cRP = DM.DesignMatrixRowPart "Ones" 1 (const $ VU.singleton 1) -- for pure (state-level) alpha
--    eRP = DM.boundedEnumRowPart (Just DT.E4_HSGrad) "Edu" (view DT.education4C)


designMatrixRow_1_E :: DM.DesignMatrixRow (F.Record '[DT.Education4C])
designMatrixRow_1_E = DM.DesignMatrixRow "E" [cRP, eRP]
  where
    cRP = DM.DesignMatrixRowPart "Ones" 1 (const $ VU.singleton 1) -- for pure (state-level) alpha
    eRP = DM.boundedEnumRowPart (Just DT.E4_HSGrad) "Edu" (view DT.education4C)


designMatrixRow_1_S_E_R :: DM.DesignMatrixRow (F.Record [DT.SexC, DT.Education4C, DT.Race5C])
designMatrixRow_1_S_E_R = DM.DesignMatrixRow "S_E_R" [cRP, sRP, eRP, rRP]
  where
    cRP = DM.DesignMatrixRowPart "Ones" 1 (const $ VU.singleton 1) -- for pure (state-level) alpha
    sRP = DM.boundedEnumRowPart Nothing "Sex" (view DT.sexC)
    eRP = DM.boundedEnumRowPart (Just DT.E4_HSGrad) "Edu" (view DT.education4C)
    rRP = DM.boundedEnumRowPart (Just DT.R5_WhiteNonHispanic) "Race" (view DT.race5C)

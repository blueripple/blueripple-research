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
import qualified Frames.Transform as FT
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
import qualified Stan.ModelBuilder.BuildingBlocks.CovarianceStructure as SBC
import qualified Stan.ModelBuilder.BuildingBlocks.DirichletMultinomial as SBDM
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


data PopAndDensity = PopAndDensity { pop :: Int, pwDensity :: Double}
instance Semigroup PopAndDensity where
  (<>) (PopAndDensity pa pwda) (PopAndDensity pb pwdb) = PopAndDensity p pwd where
    p = pa + pb
    pwd = if p > 0 then (realToFrac pa * pwda + realToFrac pb * pwdb) / realToFrac p else 0

instance Monoid PopAndDensity where
  mempty = PopAndDensity 0 0
  mappend = (<>)

marginalFld :: (Ord ck, Ord mk, Monoid d)
            => (F.Record qs -> ck)
            -> (F.Record qs -> mk)
            -> (F.Record qs -> d)
            -> FL.Fold (F.Record qs) [(ck, Map mk d)]
marginalFld catKeyF mKeyF datF =
  MR.mapReduceFold
  MR.noUnpack
  (MR.assign catKeyF (\r -> (mKeyF r, datF r)))
  (MR.foldAndLabel (FL.foldByKeyMap FL.mconcat) (,))

data DataRow rs = DataRow (F.Record rs) [Int]
deriving instance (Show (F.Record rs)) => Show (DataRow rs)

makeRowFromPD :: F.Record cs -> [PopAndDensity] -> DataRow (cs V.++ '[DT.PWPopPerSqMile])
makeRowFromPD catR dats = DataRow (catR F.<+> FT.recordSingleton @DT.PWPopPerSqMile pwd) (fmap pop dats) where
  pwd = pwDensity $ mconcat dats

dataRowsFld :: forall rs ck k qs d .
               (Ord (F.Record rs)
               , Ord ck
               , Ord k
               , Monoid d
               , BRK.FiniteSet k
               )
            => (F.Record qs -> ck)
            -> (F.Record qs -> k)
            -> (F.Record qs -> d)
            -> (ck -> [d] -> DataRow rs)
            -> FL.Fold (F.Record qs) [DataRow rs]
dataRowsFld catKeyF mKeyF datF mkRow = fmap (\(qs, m) -> mkRow qs (M.elems $ M.union m zeroMap)) <$> marginalFld catKeyF mKeyF datF
  where
    zeroMap = M.fromList $ fmap (,mempty) (S.toList $ BRK.elements @k)

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

data CovarianceStructure = DiagonalCovariance | LKJCovariance Int deriving stock (Show, Eq, Generic)

covarianceText :: CovarianceStructure -> Text
covarianceText DiagonalCovariance = "diagCov"
covarianceText (LKJCovariance n) = "lkj" <> show n

data BetaModel = BetaSimple | BetaHierCentered CovarianceStructure | BetaHierNonCentered CovarianceStructure deriving stock (Show, Eq, Generic)

betaModelText :: BetaModel -> Text
betaModelText BetaSimple = "BS"
betaModelText (BetaHierCentered cs) = "BHC_" <> covarianceText cs
betaModelText (BetaHierNonCentered cs) = "BHNC_" <> covarianceText cs

data ModelConfig alphaK (pd :: Type -> Type) where
  ModelConfig :: ({-Traversable pd-})
              => { nCounts :: Int
                 , alphaDMR :: DM.DesignMatrixRow alphaK
--                 , predDMR :: DM.DesignMatrixRow (pd Double)
                 , betaModel :: BetaModel
                 , betaLastAsZero :: Bool
                 , dirichletPrior :: Bool
                 } -> ModelConfig alphaK pd

modelText :: ModelConfig alphaK md -> Text
modelText mc = mc.alphaDMR.dmName
--               <> "_" <> mc.predDMR.dmName
               <> "_" <> betaModelText mc.betaModel
               <> if mc.betaLastAsZero then "_l0" else ""
               <> if mc.dirichletPrior then "_dir" else ""

dataText :: ModelConfig alphaK md -> Text
dataText mc = mc.alphaDMR.dmName
--              <> "_" <> mc.predDMR.dmName

stateG :: SMB.GroupTypeTag Text
stateG = SMB.GroupTypeTag "State"

stateGroupBuilder :: (Foldable f, Typeable rs)
                  => (F.Record rs -> Text) -> f Text -> SMB.StanGroupBuilderM (DataRows rs) () ()
stateGroupBuilder saF states = do
  projData <- SMB.addModelDataToGroupBuilder "CountData" (SMB.ToFoldable id)
  SMB.addGroupIndexForData stateG projData $ SMB.makeIndexFromFoldable show (saF . dataRowRec) states
  SMB.addGroupIntMapForDataSet stateG projData $ SMB.dataToIntMapFromFoldable (saF . dataRowRec) states

data ModelData r =
  ModelData
  {
    dataTag :: SMB.RowTypeTag (DataRow r)
  , nCatsE :: TE.IntE
  , countsE :: TE.ArrayE (TE.EArray1 TE.EInt)
  , nCovariatesE :: TE.IntE
  , covariatesE :: TE.MatrixE
--  , nPredictorsE :: TE.IntE
--  , predictorsE :: TE.MatrixE
  }

--TODO: add predictors to alphas to make one matrix of covariates
modelData :: forall pd alphaK rs . (Typeable rs)
          =>  ModelConfig alphaK pd
          -> (F.Record rs -> alphaK)
          -> (F.Record rs -> pd Double)
          -> SMB.StanBuilderM (DataRows rs) () (ModelData rs)
modelData mc catKey predF = do
  dat <- SMB.dataSetTag @(DataRow rs) SC.ModelData "CountData"
  (countsE', nCatsE') <- SBB.addArrayOfIntArrays  dat "MCounts" Nothing mc.nCounts dataRowCounts (Just 0) Nothing
  let (_, nCovariatesE') = DM.designMatrixColDimBinding mc.alphaDMR Nothing
  covariatesDME <- if DM.rowLength mc.alphaDMR > 0
                   then DM.addDesignMatrix dat (contramap (catKey . dataRowRec) mc.alphaDMR) Nothing
                   else pure $ TE.namedE "ERROR" TE.SMat -- this shouldn't show up in stan code at all
{-  let (_, nPredictorsE') = DM.designMatrixColDimBinding mc.predDMR Nothing
  dmE <- if DM.rowLength mc.predDMR > 0
         then DM.addDesignMatrix dat (contramap (predF . projRowRec) mc.predDMR) Nothing
         else pure $ TE.namedE "ERROR" TE.SMat -- this shouldn't show up in stan code at all
-}
  pure $ ModelData dat nCatsE' countsE' nCovariatesE' covariatesDME --nPredictorsE' dmE

-- S states
-- K categories to count
-- C categories to use for prediction
-- D predictors. 0 for now.
-- M is number of one-hot encoded alphas
-- M x K matrix or S array of M x K matrix. M=Number of cols is < 1 + C + D since we binary or one-hot encode all the categories
data Beta = SimpleBeta (DAG.Parameter TE.EMat) | HierarchicalBeta (DAG.Parameter (TE.EArray1 TE.EMat))

data ModelParameters where
  ModelParameters :: Beta -> ModelParameters

modelBeta :: ModelConfig alphaK pd -> ModelData rs -> SMB.StanBuilderM (DataRows rs) () Beta
modelBeta mc pmd = do
  let nStatesE = SMB.groupSizeE stateG
      toVector x = TE.functionE SF.to_vector (x :> TNil)
      betaColsE = if mc.betaLastAsZero then pmd.nCatsE `TE.minusE` TE.intE 1 else pmd.nCatsE
      betaName = if mc.betaLastAsZero then "betaR" else "beta"
      betaShape = TE.matrixSpec pmd.nCovariatesE betaColsE
      hierBetaSpec =  TE.array1Spec nStatesE $ betaShape []
      hierBetaPs :: SMB.StanBuilderM (DataRows rs) () (DAG.Parameters [TE.EMat, TE.EMat])
      hierBetaPs = do
        muBetaP <- DAG.addBuildParameter
                    $ DAG.UntransformedP
                    (TE.NamedDeclSpec ("mu" <> betaName) $ betaShape [])
                    [] TNil
                    (\_ p -> TE.addStmt $ TE.sample (toVector p) SF.std_normal TNil)

        tauBetaP <- DAG.addBuildParameter
                     $ DAG.UntransformedP
                     (TE.NamedDeclSpec ("tau" <> betaName) $ betaShape [TE.lowerM $ TE.realE 0])
                     [] TNil
                     (\_ p -> TE.addStmt $ TE.sample (toVector p) SF.std_normal TNil)
        pure (muBetaP :> tauBetaP :> TNil)
      betaHier cs cent = do
        hierPs <- hierBetaPs
        case hierPs of
          (muBetaP :> tauBetaP :> TNil) -> do
            muBetaAP <- DAG.addBuildParameter
                       $ DAG.TransformedP
                       (TE.NamedDeclSpec ("mu" <> betaName <> "A") $ TE.array1Spec nStatesE $ betaShape [])
                       []
                       (muBetaP :> TNil)
                       DAG.TransformedParametersBlock
                       (\(muBetaE :> TNil) -> DAG.DeclRHS $ TE.functionE SF.rep_array (muBetaE :> (nStatesE :> TNil)))
                       TNil
                       (\_ _ -> pure ())

{-
              SMB.addFromCodeWriter
                        $ TE.declareRHSNW (TE.NamedDeclSpec ("mu" <> betaName <> "A") $ TE.array1Spec nStatesE $ betaShape [])
                        $ TE.functionE SF.rep_array (DAG.parameterExpr muBetaP :> (nStatesE :> TNil))
-}
            let tauBeta = DAG.parameterExpr tauBetaP
            case cs of
              DiagonalCovariance -> do
                fmap HierarchicalBeta
                  $ SBC.matrixMultiNormalParameter' SBC.Diagonal cent muBetaAP tauBetaP
                  (TE.NamedDeclSpec betaName $ hierBetaSpec)
              LKJCovariance lkjPriorP -> do
                lkjCorrBetaP <- DAG.simpleParameter
                                (TE.NamedDeclSpec ("lkj" <> betaName)
                                 $ TE.choleskyFactorCorrSpec (pmd.nCovariatesE `TE.timesE` betaColsE) [])
                                (DAG.given (TE.realE $ realToFrac lkjPriorP) :> TNil)
                                SF.lkj_corr_cholesky
                fmap HierarchicalBeta
                  $ SBC.matrixMultiNormalParameter' (SBC.Cholesky lkjCorrBetaP) cent muBetaAP tauBetaP
                  (TE.NamedDeclSpec betaName $ hierBetaSpec)
          _ -> SMB.stanBuildError "BLCorrModel.modelBeta: Pattern match error in hierarchical beta paramters. Yikes."
  betaRawP <- case mc.betaModel of
    BetaSimple -> fmap SimpleBeta
                  $ DAG.addBuildParameter
                  $ DAG.UntransformedP (TE.NamedDeclSpec betaName $ TE.matrixSpec pmd.nCovariatesE betaColsE [])
                  [] TNil (\_ muM -> TE.addStmt $ TE.sample (toVector muM) SF.std_normal TNil)
    BetaHierCentered cs -> betaHier cs SBC.Centered
    BetaHierNonCentered cs -> betaHier cs SBC.NonCentered
  case  mc.betaLastAsZero of
    False -> pure betaRawP
    True -> do
      let fullBetaShape = TE.matrixSpec pmd.nCovariatesE pmd.nCatsE
          zeroCol = TE.functionE SF.rep_vector (TE.realE 0 :> pmd.nCovariatesE :> TNil)
          appendZeroCol m = TE.functionE SF.append_col (m :> zeroCol :> TNil)
      case betaRawP of
        SimpleBeta brP ->
          fmap SimpleBeta
          $ DAG.addBuildParameter
          $ DAG.TransformedP (TE.NamedDeclSpec "beta" $ fullBetaShape []) []
          (brP :> TNil) DAG.TransformedParametersBlock
          (\(br :> TNil) -> DAG.DeclRHS $ appendZeroCol br)
          TNil
          (\_ _ -> pure ())
        HierarchicalBeta brP ->
          fmap HierarchicalBeta
          $ DAG.addBuildParameter
          $ DAG.TransformedP (TE.NamedDeclSpec "beta" $ TE.array1Spec nStatesE $ fullBetaShape []) []
          (brP :> TNil) DAG.TransformedParametersBlock
          (\(br :> TNil) -> DAG.DeclCodeF
            $ \b -> TE.addStmt $ TE.loopSized nStatesE "s"
                    $ \ns -> [b `TE.at` ns `TE.assign` appendZeroCol (br `TE.at` ns)])
          TNil
          (\_ _ -> pure ())

data RunConfig = RunConfig { rcIncludePPCheck :: Maybe Int, rcIncludeLL :: Bool, statesM :: Maybe (Text, [Text]) }

projModel :: Typeable rs
          => RunConfig
          -> (F.Record rs -> alphaK)
          -> (F.Record rs -> pd Double)
          -> ModelConfig alphaK pd
          -> SMB.StanBuilderM  (DataRows rs) () ()
projModel rc alphaKeyF predF mc = do
  mData <- modelData mc alphaKeyF predF
  let nRowsE = SMB.dataSetSizeE mData.dataTag
  -- transformed data
  totalCountE <- SMB.inBlock SMB.SBTransformedDataGQ $ SMB.addFromCodeWriter $ do
    tc <- TE.declareNW
      (TE.NamedDeclSpec "TCount" $ TE.array1Spec nRowsE $ TE.intSpec [TE.lowerM $ TE.intE 0])
    TE.addStmt $ TE.loopSized nRowsE "n" $ \n -> [(tc `TE.at` n) `TE.assign` (TE.functionE SF.sumInt (mData.countsE `TE.at` n :> TNil))]
    pure tc
  betaP <- modelBeta mc mData
  let reIndexByState = TE.indexE TEI.s0 (SMB.byGroupIndexE mData.dataTag stateG)
      betaByRow :: TE.IntE -> TE.MatrixE
      betaByRow ie = case betaP of
          SimpleBeta m ->  DAG.parameterExpr m
          HierarchicalBeta betaByState -> reIndexByState (DAG.parameterExpr betaByState) `TE.at` ie
      mnArgByRowE nE = TE.transposeE $ (mData.covariatesE `TE.at` nE) `TE.timesE` betaByRow nE


  case mc.dirichletPrior of
    False -> do
      SMB.inBlock SMB.SBModel
        $ SMB.addFromCodeWriter
        $ TE.addStmt
        $ TE.loopSized nRowsE "n"
        $ \nE -> [TE.target $ TE.densityE SF.multinomial_logit_lpmf (mData.countsE `TE.at` nE) (mnArgByRowE nE :> TNil)]

      case rc.rcIncludePPCheck of
        Just nChoices -> do
          let ppCheck n = SMB.inBlock SMB.SBGeneratedQuantities
                          $ SBB.generatePosteriorPrediction'
                          mData.dataTag
                          (TE.NamedDeclSpec "ppCounts"
                           $ TE.array1Spec nRowsE (TE.intSpec [])
                          )
                          (\f nE -> (TE.functionE SF.multinomial_logit_rng (mnArgByRowE nE :> f nE)) `TE.at` TE.intE n)
                          (pure $ \nE -> totalCountE `TE.at` nE :> TNil)
                          (const id)
          mapM_ ppCheck [1..nChoices]
        Nothing -> pure ()
      when rc.rcIncludeLL
        $ SBB.generateLogLikelihood
        mData.dataTag
        SD.multinomialLogitDist
        (pure $ \nE -> mnArgByRowE nE :> TNil)
        (pure $ \nE -> mData.countsE `TE.at` nE)

    True -> do
      (dirichlet_multinomial, dirichlet_multinomial_lpmf, dirichlet_multinomial_rng) <- SBDM.dirichletMultinomial @_ @TE.ECVec
      let softmax x = TE.functionE SF.softmax (x :> TNil)
          toVector x = TE.functionE SF.to_vector (x :> TNil)
      dPrecE <- fmap DAG.parameterExpr
                $ DAG.addBuildParameter
                $ DAG.UntransformedP
                (TE.NamedDeclSpec "dPrec" $ TE.realSpec [TE.lowerM $ TE.realE 0]) [] TNil
                (\_ dp -> TE.addStmt $ TE.sample dp SF.normal (TE.realE 5 :> TE.realE 25 :> TNil))
      let dmArgByRowE nE = dPrecE `TE.timesE` softmax (mnArgByRowE nE)
      SMB.inBlock SMB.SBModel
        $ SMB.addFromCodeWriter
        $ TE.addStmt
        $ TE.loopSized nRowsE "n"
        $ \nE -> TE.writerL'
                 $ (do
                       TE.addStmt $ TE.sample (mData.countsE `TE.at` nE) dirichlet_multinomial (dmArgByRowE nE :> TNil)
                   )
      case  rc.rcIncludePPCheck of
        Just nChoices -> do
          let ppCheck n = SMB.inBlock SMB.SBGeneratedQuantities
                          $ SBB.generatePosteriorPrediction'
                          mData.dataTag
                          (TE.NamedDeclSpec ("ppCounts_" <> show n)
                           $ TE.array1Spec nRowsE (TE.intSpec [])
                          )
                          (\f nE -> (TE.functionE dirichlet_multinomial_rng (dmArgByRowE nE :> f nE)) `TE.at` TE.intE n)
                          (pure $ \nE -> totalCountE `TE.at` nE :> TNil)
                          (const id)
          mapM_ ppCheck [1..nChoices]
        Nothing -> pure ()
      when rc.rcIncludeLL
        $ SBB.generateLogLikelihood'
        $ SBB.addToLLSet mData.dataTag
        (SBB.LLDetails
         (TE.densityE dirichlet_multinomial_lpmf)
         (pure $ \nE -> dmArgByRowE nE :> TNil)
         (pure $ \nE -> mData.countsE `TE.at` nE)
        )
        SBB.emptyLLSet


-- rs is stateAbbr ++ kP ++ PWPopPerSqMile
runProjModel :: forall kM pd kPs rs r .
                (K.KnitEffects r
                , BRKU.CacheEffects r
--                , kP F.⊆ DDP.ACSByPUMAR
                , Typeable pd
                , Ord kM
                , BRK.FiniteSet kM
                , rs ~ kPs V.++ '[DT.PWPopPerSqMile]
                , V.RMap rs
                , FS.RecFlat rs
                , Ord (F.Record rs)
                , Show (F.Record rs)
                , Typeable rs
                , F.ElemOf rs GT.StateAbbreviation
                , Ord (F.Record kPs)
                , kPs F.⊆ DDP.ACSa4ByPUMAR
                )
             => Bool
             -> BR.CommandLine
             -> RunConfig
             -> ModelConfig (F.Record rs) pd
             -> (F.Record DDP.ACSa4ByPUMAR -> kM)
             -> (F.Record DDP.ACSa4ByPUMAR -> F.Record kPs)
             -> (F.Record rs -> pd Double)
             -> K.Sem r (K.ActionWithCacheTime r ())
runProjModel clearCaches _cmdLine rc mc margKeyF predKeyF predF = do
  let cacheRoot = "model/demographic/nullVecProjModel/"
      cacheDirE = (if clearCaches then Left else Right) cacheRoot
      dataName = "blCorrData_" <> dataText mc <> maybe "" fst rc.statesM
      countF r = PopAndDensity (view DT.popCount r) (view DT.pWPopPerSqMile r)
      runnerInputNames = SC.RunnerInputNames
                         ("br-2022-Demographics/stan/blCorrModel")
                         (modelText mc)
                         (Just $ SC.GQNames "pp" dataName) -- posterior prediction vars to wrap
                         dataName
      statesFilter = maybe id (\(_, sts) -> F.filterFrame ((`elem` sts) . view GT.stateAbbreviation)) rc.statesM
  acsByPUMA_C <- fmap statesFilter <$> DDP.cachedACSa4ByPUMA
  let dataCacheKey = cacheRoot <> "/acsCounts_" <> mc.alphaDMR.dmName <> maybe "" fst rc.statesM
  when clearCaches $ BRK.clearIfPresentD dataCacheKey
  acsCountedByPUMA_C <- BRK.retrieveOrMakeD
                       dataCacheKey
                       acsByPUMA_C
                       $
                       \acsByPUMA -> do
                         let mkRow acsByPUMARow = makeRowFromPD (acsByPUMARow)
                             counted = FL.fold (dataRowsFld (F.rcast @kPs) margKeyF countF mkRow) acsByPUMA
--                         K.logLE K.Info $ "counted: " <> show counted
                         pure counted
  states <-  FL.fold (FL.premap (view GT.stateAbbreviation) FL.set) <$> K.ignoreCacheTime acsByPUMA_C
  (dw, code) <-  SMR.dataWranglerAndCode acsCountedByPUMA_C (pure ())
                (stateGroupBuilder (view GT.stateAbbreviation)  (S.toList states))
                (projModel rc id predF mc)

  let unwraps = case rc.rcIncludePPCheck of
        Just nChoices ->
          let f n = SR.UnwrapExpr ("matrix(ncol="
                                    <> show nChoices
                                    <> ", byrow=TRUE, unlist(jsonData $ MCounts))[,"
                                    <> show n <> "]") ("yCounts_" <> show n)
          in fmap f [1..nChoices]
        Nothing -> []
--      unwraps = [SR.UnwrapNamed "MCounts" "yCounts"]
  res_C <- SMR.runModel' @BRKU.SerializerC @BRKU.CacheData
           cacheDirE
           (Right runnerInputNames)
           (Just $ SC.StanMCParameters 4 4 (Just 1000) (Just 1000) Nothing Nothing (Just 1))
           dw
           code
           SC.DoNothing
           (SMR.Both unwraps) --(SMR.Both [SR.UnwrapNamed "successes" "yObserved"])
           acsCountedByPUMA_C
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

designMatrixRow_E :: DM.DesignMatrixRow (F.Record '[DT.Education4C])
designMatrixRow_E = DM.DesignMatrixRow "E" [eRP]
  where
    eRP = DM.boundedEnumRowPart (Just DT.E4_HSGrad) "Edu" (view DT.education4C)


designMatrixRow_1_E :: DM.DesignMatrixRow (F.Record '[DT.Education4C])
designMatrixRow_1_E = DM.DesignMatrixRow "I_E" [cRP, eRP]
  where
    cRP = DM.DesignMatrixRowPart "Ones" 1 (const $ VU.singleton 1) -- for pure (state-level) alpha
    eRP = DM.boundedEnumRowPart (Just DT.E4_HSGrad) "Edu" (view DT.education4C)

designMatrixRow_1_S_E :: DM.DesignMatrixRow (F.Record '[DT.SexC, DT.Education4C])
designMatrixRow_1_S_E = DM.DesignMatrixRow "I_S_E" [cRP, sRP, eRP]
  where
    cRP = DM.DesignMatrixRowPart "Ones" 1 (const $ VU.singleton 1) -- for pure (state-level) alpha
    sRP = DM.boundedEnumRowPart Nothing "Sex" (view DT.sexC)
    eRP = DM.boundedEnumRowPart (Just DT.E4_HSGrad) "Edu" (view DT.education4C)

designMatrixRow_1_S_E_R :: DM.DesignMatrixRow (F.Record [DT.SexC, DT.Education4C, DT.Race5C])
designMatrixRow_1_S_E_R = DM.DesignMatrixRow "I_S_E_R" [cRP, sRP, eRP, rRP]
  where
    cRP = DM.DesignMatrixRowPart "Ones" 1 (const $ VU.singleton 1) -- for pure (state-level) alpha
    sRP = DM.boundedEnumRowPart Nothing "Sex" (view DT.sexC)
    eRP = DM.boundedEnumRowPart (Just DT.E4_HSGrad) "Edu" (view DT.education4C)
    rRP = DM.boundedEnumRowPart (Just DT.R5_WhiteNonHispanic) "Race" (view DT.race5C)

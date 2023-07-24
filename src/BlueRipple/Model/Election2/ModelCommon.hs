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

module BlueRipple.Model.Election2.ModelCommon
  (
    module BlueRipple.Model.Election2.ModelCommon
  )
where

import qualified BlueRipple.Configuration as BR
import qualified BlueRipple.Utilities.KnitUtils as BRKU

import qualified BlueRipple.Model.Election2.DataPrep
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


stateG :: SMB.GroupTypeTag Text
stateG = SMB.GroupTypeTag "State"

{-
stateGroupBuilder :: (Foldable f, Typeable outerK)
                  => (outerK -> Text) -> f Text -> SMB.StanGroupBuilderM (ProjData outerK) () ()
stateGroupBuilder saF states = do
  projData <- SMB.addModelDataToGroupBuilder "ProjectionData" (SMB.ToFoldable pdRows)
  SMB.addGroupIndexForData stateG projData $ SMB.makeIndexFromFoldable show (saF . pdKey) states
  SMB.addGroupIntMapForDataSet stateG projData $ SMB.dataToIntMapFromFoldable (saF . pdKey) states
-}

data StateAlphaModel = StateAlphaSimple | StateAlphaHierCentered | StateAlphaHierNonCentered deriving stock (Show)

stateAlphaModelText :: StateAlphaModel -> Text
stateAlphaModelText StateAlphaSimple = "AS"
stateAlphaModelText StateAlphaHierCentered = "AHC"
stateAlphaModelText StateAlphaHierNonCentered = "AHNC"


-- we can use the designMatrixRow to get (a -> Vector Double) for prediction
data ModelConfig a =
  ModelConfig
  {
    stateAbbrF :: a -> Text
  , designMatrixRow :: DM.DesignMatrixRow a
  , trialsT :: Text
  , trialsF :: a -> Int
  , successesT :: Text
  , successesF :: a -> Int
  , stateAlphaModel :: StateAlphaModel
  }

-- for now we model only alpha hierarchically. Beta will be the same everywhere.
data PredictionData a = PredictionData { pdAlphaMap :: Map Text Double
                                       , pdBetaV :: VU.Vector Double
                                       , pdLogisticAdjMapM :: Maybe (Map Text Double)
                                       }

predictedP :: ModelConfig a -> PredictionData a -> a -> Either Text Double
predictedP mc pd a = do
  alpha <- case M.lookup (mc.stateAbbrF a) pd.pdAlphaMap of
    Nothing -> Left $ "Model.Election2.ModelCommon.predictedP: alphaMap lookup failed for k=" <> show (mc.stateAbbrF a)
    Just x -> pure x
  logisticAdj <- case pd.pdLogisticAdjMapM of
    Nothing -> pure 0
    Just m -> case M.lookup (mc.stateAbbrF a) m of
      Nothing -> Left $ "Model.Election2.ModelCommon.predictedP: pdLogisticAdjMap lookup failed for k=" <> show (mc.stateAbbrF a)
      Just x -> pure x
  let covariatesV = DM.designMatrixRowF mc.designMatrixRow a
      invLogit x = 1 / (1 + exp (negate x))
  pure $ invLogit (alpha + VU.sum (VU.zipWith (*) covariatesV pd.pdBetaV) + logisticAdj)

modelText :: ModelConfig a -> Text
modelText mc = mc.trialsT <> "_" <> mc.successesT <> "_" <> mc.designMatrixRow.dmName <> "_" <> stateAlphaModelText mc.stateAlphaModel

dataText :: ModelConfig a -> Text
dataText mc = mc.designMatrixRow.dmName

data ModelData a = ModelData
  {
    modelDataTag :: SMB.RowTypeTag a
  , nCovariatesE :: TE.IntE
  , covariatesE :: TE.MatrixE
  , trialsE :: TE.IntArrayE
  , successesE :: TE.IntArrayE
  }

modelData :: forall a md . (Typeable a)
          => ModelConfig a
          -> SMB.StanBuilderM md () (ModelData a)
modelData mc = do
  modelDataTag <- SMB.dataSetTag @a SC.ModelData "ElectionModelData"
  let (_, nCovariatesE') = DM.designMatrixColDimBinding mc.designMatrixRow Nothing
  dmE <- if DM.rowLength mc.designMatrixRow > 0
         then DM.addDesignMatrix modelDataTag mc.designMatrixRow Nothing
         else pure $ TE.namedE "ERROR" TE.SMat -- this shouldn't show up in stan code at all
  trialsE' <- SBB.addCountData modelDataTag mc.trialsT mc.trialsF
  successesE' <- SBB.addCountData modelDataTag mc.successesT mc.successesF
  pure $ ModelData modelDataTag nCovariatesE' dmE trialsE' successesE'

-- given S states
-- alpha is a scalar or S col-vector
data Alpha = SimpleAlpha (DAG.Parameter TE.EReal) | HierarchicalAlpha (DAG.Parameter TE.ECVec)

-- and K predictors
-- theta is K col-vector (or Nothing)
newtype Theta = Theta (Maybe (DAG.Parameter TE.ECVec))

data ModelParameters where
  BinomialLogitModelParameters :: Alpha -> Theta -> ModelParameters

paramAlpha :: ModelParameters -> Alpha
paramAlpha (BinomialLogitModelParameters a _) = a

paramTheta :: ModelParameters -> Theta
paramTheta (BinomialLogitModelParameters _ t) = t

modelParameters :: ModelConfig a -> ModelData a -> SMB.StanBuilderM md () ModelParameters
modelParameters mc pmd = do
  let stdNormalDWA :: (TE.TypeOneOf t [TE.EReal, TE.ECVec, TE.ERVec], TE.GenSType t) => TE.DensityWithArgs t
      stdNormalDWA = TE.DensityWithArgs SF.std_normal TNil
      numPredictors = DM.rowLength mc.designMatrixRow
  -- for now all the thetas are iid std normals

  theta <- if numPredictors > 0 then
               (Theta . Just)
               <$> DAG.simpleParameterWA
               (TE.NamedDeclSpec "theta" $ TE.vectorSpec pmd.nCovariatesE [])
               stdNormalDWA
             else pure $ Theta Nothing
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
  alpha <- case mc.stateAlphaModel of
    StateAlphaSimple -> do
      fmap SimpleAlpha
        $ DAG.simpleParameterWA
        (TE.NamedDeclSpec "alpha" $ TE.realSpec [])
        stdNormalDWA
    StateAlphaHierCentered -> do
      alphaPs <- hierAlphaPs
      fmap HierarchicalAlpha
        $ DAG.addBuildParameter
        $ DAG.UntransformedP hierAlphaNDS [] alphaPs
        $ \(muAlphaE :> sigmaAlphaE :> TNil) m
          -> TE.addStmt $ TE.sample m SF.normalS (muAlphaE :> sigmaAlphaE :> TNil)
    StateAlphaHierNonCentered -> do
      alphaPs <- hierAlphaPs
      let rawNDS = TE.NamedDeclSpec (TE.declName hierAlphaNDS <> "_raw") $ TE.decl hierAlphaNDS
      rawAlphaP <- DAG.simpleParameterWA rawNDS stdNormalDWA
      fmap HierarchicalAlpha
        $ DAG.addBuildParameter
        $ DAG.TransformedP hierAlphaNDS []
        (rawAlphaP :> alphaPs) DAG.TransformedParametersBlock
        (\(rawE :> muAlphaE :> muSigmaE :> TNil) -> DAG.DeclRHS $ muAlphaE `TE.plusE` (muSigmaE `TE.timesE` rawE))
        TNil (\_ _ -> pure ())
  pure $ BinomialLogitModelParameters alpha theta

data RunConfig = RunConfig { rcIncludePPCheck :: Bool, rcIncludeLL :: Bool }

-- not returning anything for now
model :: Typeable a
      => RunConfig
      -> ModelConfig a
      -> SMB.StanBuilderM md () ()
model rc mc = do
  mData <- modelData mc
  mParams <- modelParameters mc mData
  let betaNDS = TE.NamedDeclSpec "beta" $ TE.vectorSpec mData.nCovariatesE []
      nRowsE = SMB.dataSetSizeE mData.modelDataTag
      pExpr = DAG.parameterExpr

  (covariatesM, _centerF, _mBeta) <- case paramTheta mParams of
    Theta (Just thetaP) -> do
      (centeredCovariatesE, centerF) <- DM.centerDataMatrix DM.DMCenterOnly mData.covariatesE Nothing "DM"
      (dmQ, _, _, mBeta) <- DM.thinQR centeredCovariatesE "DM" $ Just (pExpr thetaP, betaNDS)
      pure (dmQ, centerF, mBeta)
    Theta Nothing -> pure (TE.namedE "ERROR" TE.SMat, \_ x _ -> pure x, Nothing)

  -- model
  let reIndexByState = TE.indexE TEI.s0 (SMB.byGroupIndexE mData.modelDataTag stateG)
      lpE :: Alpha -> Theta -> TE.VectorE
      lpE a t =  case a of
       SimpleAlpha alphaP -> case t of
         Theta Nothing -> TE.functionE SF.rep_vector (pExpr alphaP :> nRowsE :> TNil)
         Theta (Just thetaP) -> pExpr alphaP `TE.plusE` (covariatesM `TE.timesE` pExpr thetaP)
       HierarchicalAlpha alpha -> case t of
         Theta Nothing -> reIndexByState $ pExpr alpha
         Theta (Just thetaP) -> reIndexByState (pExpr alpha) `TE.plusE` (covariatesM `TE.timesE` pExpr thetaP)

  let ppF :: SMD.StanDist TE.EInt pts xs --((TE.IntE -> TE.ExprList xs) -> TE.IntE -> TE.UExpr TE.EReal)
          -> TE.CodeWriter (TE.IntE -> TE.ExprList xs)
          -> SMB.StanBuilderM md () (TE.ArrayE TE.EInt)
      ppF dist rngPSCW = SBB.generatePosteriorPrediction
                         mData.modelDataTag
                         (TE.NamedDeclSpec ("pred" <> mc.successesT) $ TE.array1Spec nRowsE $ TE.intSpec [])
                         dist --rngF
                         rngPSCW
--                         (\_ p -> p)
      llF :: SMD.StanDist t pts rts
          -> TE.CodeWriter (TE.IntE -> TE.ExprList pts)
          -> TE.CodeWriter (TE.IntE -> TE.UExpr t)
          -> SMB.StanBuilderM md gq ()
      llF = SBB.generateLogLikelihood mData.modelDataTag

  let (sampleStmtF, pp, ll) = case mParams of
        BinomialLogitModelParameters a t ->
          let ssF e = SMB.familySample SMD.binomialLogitDist e (mData.trialsE :> lpE a t :> TNil)
--              rF f nE = TE.functionE SF.normal_rng (f nE)
              rpF :: TE.CodeWriter (TE.IntE -> TE.ExprList '[TE.EInt, TE.EReal])
              rpF = pure $ \nE -> mData.trialsE `TE.at` nE :> lpE a t `TE.at` nE :> TNil
--              rpF = pure $ \nE -> lpE `fstI` nE :> mData.trialsE `fstI` nE :> TNil
              ll = llF SMD.binomialLogitDist rpF (pure $ \nE -> mData.successesE `TE.at` nE)
          in (ssF, ppF SMD.binomialLogitDist rpF, ll)

  SMB.inBlock SMB.SBModel $ SMB.addFromCodeWriter $ TE.addStmt $ sampleStmtF mData.successesE
  -- generated quantities
  when rc.rcIncludePPCheck $ void pp
  when rc.rcIncludeLL ll
  pure ()

--cwdF :: (F.ElemOf rs DT.PopCount, F.ElemOf rs DT.PWPopPerSqMile) => F.Record rs -> DMS.CellWithDensity
--cwdF r = DMS.CellWithDensity (realToFrac $ r ^. DT.popCount) (r ^. DT.pWPopPerSqMile)
{-
runModel :: forall (ks :: [(Symbol, Type)]) rs r .
                (K.KnitEffects r
                , BRKU.CacheEffects r
                )
             => Bool
             -> BR.CommandLine
             -> RunConfig
             -> ModelConfig a
             -> K.ActionWithCacheTime r (ModelData a)
             -> K.Sem r (K.ActionWithCacheTime r (PredictionData a))
runModel clearCaches _cmdLine rc mc acs_C nvps_C ms datFld = do
  let cacheRoot = "model/demographic/nullVecProjModel3_A5/"
      cacheDirE = (if clearCaches then Left else Right) cacheRoot
      dataName = "projectionData_" <> dataText mc <> "_N" <> show rc.nvIndex <> maybe "" fst rc.statesM
      runnerInputNames = SC.RunnerInputNames
                         ("br-2022-Demographics/stan/nullVecProj_M3_A5")
                         (modelText mc)
                         (Just $ SC.GQNames "pp" dataName) -- posterior prediction vars to wrap
                         dataName
--  acsByPUMA_C <- DDP.cachedACSa5ByPUMA
  let nvpDataCacheKey = cacheRoot <> "nvpRowData" <> dataText mc <> ".bin"
      outerKey = F.rcast @[GT.StateAbbreviation, GT.PUMA]
      catKey = F.rcast @ks
--      datF r = DMS.CellWithDensity (realToFrac $ r ^. DT.popCount) (r ^. DT.pWPopPerSqMile)
      nvpDeps = (,) <$> acs_C <*>  nvps_C
  nvpData_C <- fmap (fmap (fmap unNVProjectionRowData))
               $ BRKU.retrieveOrMakeD nvpDataCacheKey nvpDeps
               $ \(acsByPUMA, nvps) -> (do
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
  K.logLE K.Info $ "meanSD=" <> show meanSD
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
-}

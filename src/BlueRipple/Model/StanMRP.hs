{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-# OPTIONS_GHC  -O0 #-}

module BlueRipple.Model.StanMRP where

import qualified Control.Foldl as FL
import qualified Data.Aeson as A
import qualified Data.Array as Array
import qualified Data.Dependent.HashMap as DHash
import qualified Data.Dependent.Sum as DSum
import qualified Data.IntMap.Strict as IM
import qualified Data.List as List
import Data.List.Extra (nubOrd)
import qualified Data.Map as Map
import qualified Data.Maybe as Maybe
import qualified Data.Set as Set

import qualified Data.Text as T
import qualified Data.Vector as Vec
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V
import qualified Flat
import Flat.Instances.Vector()
import Flat.Instances.Containers()

import Frames.MapReduce (postMapM)
import qualified Control.MapReduce as MR

import qualified BlueRipple.Utilities.KnitUtils as BR
import qualified BlueRipple.Data.Keyed as BK
import qualified BlueRipple.Utilities.KnitUtils as BR

import qualified CmdStan as CS
import qualified CmdStan.Types as CS
import qualified Stan.JSON as SJ
import qualified Stan.Frames as SF
import qualified Stan.Parameters as SP
import qualified Stan.ModelRunner as SM
import qualified Stan.ModelBuilder as SB
import qualified Stan.ModelBuilder.Expressions as SME
import qualified Stan.ModelBuilder.SumToZero as SB
import qualified Stan.ModelBuilder.BuildingBlocks as SB
import qualified Stan.ModelBuilder.Distributions as SB
import qualified Stan.ModelConfig as SC
import qualified Stan.RScriptBuilder as SR
import qualified System.Environment as Env

import qualified Knit.Report as K
import qualified Knit.Effect.AtomicCache as K hiding (retrieveOrMake)
import Data.String.Here (here)

type BuilderM d = SB.StanBuilderM () d

buildDataWranglerAndCode :: (Typeable d)
                         => SB.StanGroupBuilderM d ()
                         -> env
                         -> SB.StanBuilderM env d ()
                         -> d
                         -> Either Text (SC.DataWrangler d SB.DataSetGroupIntMaps (), SB.StanCode)
buildDataWranglerAndCode groupM env builderM d =
  let builderWithWrangler = do
        SB.buildGroupIndexes
        builderM
        jsonF <- SB.buildJSONFromDataM
        intMapsBuilder <- SB.intMapsBuilder
        return
          $ SC.Wrangle SC.TransientIndex
          $ \d -> (intMapsBuilder d, jsonF)
      resE = SB.runStanBuilder d env groupM builderWithWrangler
  in fmap (\(SB.BuilderState _ _ _ _ _ c, dw) -> (dw, c)) resE

runMRPModel :: (K.KnitEffects r
               , BR.CacheEffects r
               , Flat.Flat c
               )
            => Bool
            -> Maybe Text
            -> Text
            -> Text
            -> SC.DataWrangler a b ()
            -> SB.StanCode
            -> Text
            -> SC.ResultAction r a b () c
            -> K.ActionWithCacheTime r a
            -> Maybe Int
            -> Maybe Double
            -> Maybe Int
            -> K.Sem r (K.ActionWithCacheTime r c)
runMRPModel clearCache mWorkDir modelName dataName dataWrangler stanCode ppName resultAction data_C mNSamples mAdaptDelta mMaxTreeDepth =
  K.wrapPrefix "BlueRipple.Model.StanMRP" $ do
  K.logLE K.Info "Running..."
  let workDir = fromMaybe ("stan/MRP/" <> modelName) mWorkDir
      outputLabel = modelName <> "_" <> dataName
      nSamples = fromMaybe 1000 mNSamples
      stancConfig =
        (SM.makeDefaultStancConfig (T.unpack $ workDir <> "/" <> modelName)) {CS.useOpenCL = False}
  stanConfig <-
    SC.setSigFigs 4
    . SC.noLogOfSummary
--    . SC.noDiagnose
    <$> SM.makeDefaultModelRunnerConfig
    workDir
    (modelName <> "_model")
    (Just (SB.All, SB.stanCodeToStanModel stanCode))
    (Just $ dataName <> ".json")
    (Just $ outputLabel)
    4
    (Just nSamples)
    (Just nSamples)
    mAdaptDelta
    mMaxTreeDepth
    (Just stancConfig)
  let resultCacheKey = "stan/MRP/result/" <> outputLabel <> ".bin"
  when clearCache $ do
    K.liftKnit $ SM.deleteStaleFiles stanConfig [SM.StaleData]
    BR.clearIfPresentD resultCacheKey
  modelDep <- SM.modelCacheTime stanConfig
  K.logLE K.Diagnostic $ "modelDep: " <> show (K.cacheTime modelDep)
  K.logLE K.Diagnostic $ "houseDataDep: " <> show (K.cacheTime data_C)
  let dataModelDep = const <$> modelDep <*> data_C
      getResults s () inputAndIndex_C = return ()
      unwraps = [SR.UnwrapNamed ppName ppName]
  BR.retrieveOrMakeD resultCacheKey dataModelDep $ \() -> do
    K.logLE K.Info "Data or model newer then last cached result. (Re)-running..."
    SM.runModel @BR.SerializerC @BR.CacheData
      stanConfig
      (SM.Both unwraps)
      dataWrangler
      SC.UnCacheable -- we cannot use a Cacheable index here
      resultAction
      ()
      data_C

-- Basic group declarations, indexes and Json are produced automatically
addGroup :: Typeable d
           => SB.RowTypeTag r
           -> SB.StanExpr
--           -> SB.StanExpr
--           -> SB.SumToZero
           -> SB.GroupModel
           -> SB.GroupTypeTag k
           -> Maybe Text
           -> SB.StanBuilderM env d SB.StanExpr
addGroup rtt binaryPrior gm gtt mVarSuffix = do
  SB.setDataSetForBindings rtt
  (SB.IntIndex indexSize _) <- SB.rowToGroupIndex <$> SB.indexMap rtt gtt
  let gn = SB.taggedGroupName gtt
      gs t = t <> fromMaybe "" mVarSuffix <> "_" <> gn
  when (indexSize < 2) $ SB.stanBuildError "Index with size <2 in MRGroup!"
  let binaryGroup = do
        let en = gs "eps"
            be = SB.bracket
                 $ SB.csExprs (SB.name en :| [SB.name $ "-" <> en])
        let e' = SB.indexBy be gn
            modelTerm = SB.vectorFunction "to_vector" e' []
        SB.inBlock SB.SBParameters $ SB.stanDeclare en SB.StanReal ""
        SB.inBlock SB.SBModel $ do
          let priorE = SB.name en `SB.vectorSample` binaryPrior
          SB.addExprLine "intercept" priorE
--          SB.printExprM "intercept" (SB.fullyIndexedBindings mempty) (return priorE) >>= SB.addStanLine
        return modelTerm
  let nonBinaryGroup = do
        let modelTerm = SB.indexBy (SB.name $ gs "beta") gn
--        sigmaVar <- SB.inBlock SB.SBParameters $ SB.stanDeclare (gs "sigma") SB.StanReal "<lower=0>"
--        SB.inBlock SB.SBModel $ do
--          let sigmaPriorE = SB.name (gs "sigma") `SB.vectorSample` sigmaPrior
--          SB.addExprLine "addMRGroup" sigmaPriorE
--          SB.printExprM "addMRGroup" (SB.fullyIndexedBindings mempty) (return sigmaPriorLine) >>= SB.addStanLine
        let betaVar = SB.StanVar (gs "beta") (SB.StanVector $ SB.NamedDim gn)
        SB.groupModel betaVar gm
--        SB.rescaledSumToZero stz pmp betaVar (gs "sigma") sigmaPrior
        return modelTerm
  if indexSize == 2 then binaryGroup else nonBinaryGroup

addInteractions2 :: Typeable d
                 => SB.RowTypeTag r
                 -> SB.GroupModel
                 -> SB.GroupTypeTag k1
                 -> SB.GroupTypeTag k2
                 -> Maybe Text
                 -> SB.StanBuilderM env d SB.StanVar
addInteractions2 rtt gm gtt1 gtt2 mSuffix = do
  SB.setDataSetForBindings rtt
  (SB.IntIndex indexSize1 _) <- SB.rowToGroupIndex <$> SB.indexMap rtt gtt1
  (SB.IntIndex indexSize2 _) <- SB.rowToGroupIndex <$> SB.indexMap rtt gtt2
  when (indexSize1 < 2 || indexSize2 < 2) $ SB.stanBuildError "addInteractions2: Index with size < 2"
  let ivn = "beta_" <> SB.taggedGroupName gtt1 <> "_" <> SB.taggedGroupName gtt2 <> maybe "" (("_" <>))  mSuffix
      ivt = SB.StanArray [SB.NamedDim $ SB.taggedGroupName gtt1, SB.NamedDim $ SB.taggedGroupName gtt2] SB.StanReal
      iv' = SB.StanVar ivn ivt
  SB.groupModel iv' gm


{-
--- return expression for sampling and one for everything else
addNestedMRGroup ::  Typeable d
                 => SB.RowTypeTag r
                 -> SB.GroupModel
                 -> SB.GroupModel
                 -> SB.GroupTypeTag k1 -- e.g., sex
                 -> SB.GroupTypeTag k2 -- partially pooled, e.g., state
                 -> Maybe Text
                 -> SB.StanBuilderM env d (SB.StanExpr, SB.StanExpr)
addNestedMRGroup rtt gm nonPooledGtt pooledGtt mVarSuffix = do
  let nonPooledGN = SB.taggedGroupName nonPooledGtt
      pooledGN = SB.taggedGroupName pooledGtt
      dsName = SB.dataSetName rtt
      suffix = nonPooledGN <> "_" <> pooledGN
      suffixed x = x <> fromMaybe "" mVarSuffix <> "_" <> suffix
  (SB.IntIndex pooledIndexSize _) <- SB.rowToGroupIndex <$> SB.indexMap rtt pooledGtt
  (SB.IntIndex nonPooledIndexSize _) <- SB.rowToGroupIndex <$> SB.indexMap rtt nonPooledGtt
  when (pooledIndexSize < 2) $ SB.stanBuildError $ "pooled index (" <> pooledGN <> ") with size <2 in nestedMRGroup!"
  when (nonPooledIndexSize < 2) $ SB.stanBuildError $ "non-pooled index (" <> nonPooledGN <> ") with size <2 in nestedMRGroup!"
  let nonPooledBinary = do
--        sigmaVar <- SB.inBlock SB.SBParameters $ SB.stanDeclare (suffixed "sigma") SB.StanReal "<lower=0>"
        let epsVar = SB.StanVar (suffixed "eps") (SB.StanVector $ SB.NamedDim pooledGN)
--        SB.inBlock SB.SBModel $ do
--          let e1 = SB.name (suffixed "sigma") `SB.vectorSample` sigmaPrior
--          SB.addExprLine "addNestedMRGroup (binary non-pooled)" e1
        SB.groupModel epsVar gm
--        SB.rescaledSumToZero stz pmp epsVar (suffixed "sigma") sigmaPrior
        (yE, epsE) <- SB.inBlock SB.SBTransformedParameters $ do
          yv <- SB.stanDeclare (suffixed "y") (SB.StanVector $ SB.NamedDim dsName) ""
          let yE = SB.useVar yv
              be = SB.bracket
                   $ SB.csExprs (SB.indexBy (SB.name $ suffixed "eps") pooledGN :|
                                  [SB.indexBy (SB.name $ suffixed "-eps") pooledGN]
                                )
              epsE =  SB.indexBy be nonPooledGN
          SB.stanForLoopB "n" Nothing dsName
            $ SB.addExprLine "addNestedMRGroup: y-loop" $ yE `SB.eq` epsE

          return (yE, epsE)
        return (yE, epsE)
      nonPooledNonBinary = do
        let betaType = SB.StanMatrix (SB.NamedDim nonPooledGN, SB.NamedDim pooledGN)
        (sigmaV, betaRawV) <- SB.inBlock SB.SBParameters $ do
          sv <- SB.stanDeclare (suffixed "sigma") (SB.StanVector (SB.NamedDim nonPooledGN)) "<lower=0>"
          brv <- SB.stanDeclare (suffixed "beta" <> "_raw") betaType ""
          return (sv, brv)
        (yV, betaV) <- SB.inBlock SB.SBTransformedParameters $ do
          let eDPM = SB.vectorized (Set.fromList [nonPooledGN, pooledGN])
                     $ SB.function "diag_pre_multiply" (SB.useVar sigmaV :| [SB.useVar betaRawV])
          b <- SB.stanDeclareRHS (suffixed "beta") betaType ""
               $ eDPM
          y <- SB.stanDeclare (suffixed "y") (SB.StanVector $ SB.NamedDim dsName) ""
          SB.stanForLoopB "n" Nothing dsName
            $ SB.addExprLine "nestedMRGroup (NB.TransformedParameters)"
            $ SB.useVar y `SB.eq` SB.useVar b
          return (y, b)
        SB.inBlock SB.SBModel $ do
          let eSigma = SB.vectorizedOne nonPooledGN
                       $ SB.useVar sigmaV `SB.vectorSample` sigmaPrior
          SB.addExprLines "addNestedMRGroup (NB.Model)" [eSigma]
          SB.stanForLoopB ("j" <> nonPooledGN) Nothing nonPooledGN
            $ SB.addExprLine "nestedMRGroup (NB.BetaLoop)"
            $ SB.vectorizedOne pooledGN
            $ SB.useVar betaRawV `SB.vectorSample` SB.stdNormal
--        SB.rescaledSumToZero SB.STZNone betaVar sigmaV  -- FIX, we can't sum to zero along cols or rows.
        let yE = SB.useVar yV --SB.indexed SB.modeledDataIndexName $ SB.name $ suffixed "y"
            betaE =  SB.useVar betaV --SB.indexed nonPooledGN $ SB.indexed pooledGN $ SB.name $ suffixed "beta"
        return (yE, betaE)

  if nonPooledIndexSize == 2 then nonPooledBinary else nonPooledNonBinary
-}

type GroupName = Text

data FixedEffects row = FixedEffects Int (row -> Vec.Vector Double)

emptyFixedEffects :: DHash.DHashMap SB.RowTypeTag FixedEffects
emptyFixedEffects = DHash.empty

-- returns
-- 'X * beta' (or 'Q * theta') model term expression
-- VarX -> 'VarX * beta' and just 'beta' for post-stratification
-- The latter is for use in post-stratification at fixed values of the fixed effects.
addFixedEffects :: forall r1 r2 d.(Typeable d)
                => Bool
                -> SB.StanExpr
                -> SB.RowTypeTag r1
                -> SB.RowTypeTag r2
                -> FixedEffects r1
                -> BuilderM d ( SB.StanExpr
                              , SB.StanVar -> BuilderM d SB.StanVar
                              , SB.StanExpr)
addFixedEffects thinQR fePrior rttFE rttModeled (FixedEffects n vecF) = do
  let feDataSetName = SB.dataSetName rttFE
      uSuffix = SB.underscoredIf feDataSetName
      rowIndexKey = SB.dataSetsCrosswalkName rttModeled rttFE
  SB.add2dMatrixJson rttFE "X" "" (SB.NamedDim feDataSetName) n vecF -- JSON/code declarations for matrix
  f <- SB.fixedEffectsQR uSuffix ("X" <> uSuffix) feDataSetName ("X_" <> feDataSetName <> "_Cols") -- code for parameters and transformed parameters
  -- model
  SB.inBlock SB.SBModel $ do
    let e = SB.name ("thetaX" <> uSuffix) `SB.vectorSample` fePrior
    SB.addExprLine "addFixedEffects" e
--    SB.addStanLine $ "thetaX" <> uSuffix <> " ~ normal(0," <> show fePriorSD <> ")"
  let eQ = if T.null feDataSetName
           then SB.indexBy (SB.name "Q_ast") rowIndexKey
           else SB.indexBy  (SB.name  $ "Q" <> uSuffix <> "_ast") rowIndexKey
      eTheta = SB.name $ "thetaX" <> uSuffix
      eQTheta = eQ `SB.times` eTheta
      eX = if T.null feDataSetName
           then SB.indexBy (SB.name "centered_X") rowIndexKey
           else SB.indexBy (SB.name ("centered_X" <> uSuffix)) rowIndexKey
      eBeta = SB.name $ "betaX" <> uSuffix
      eXBeta = eX `SB.times` eBeta
      feExpr = if thinQR then eQTheta else eXBeta
  return (feExpr, f, eBeta)

addFixedEffectsData :: forall r d. (Typeable d)
                    => SB.RowTypeTag r
                    -> FixedEffects r
                    -> BuilderM d (SB.StanVar -> BuilderM d SB.StanVar)
addFixedEffectsData feRTT (FixedEffects n vecF) = do
  let feDataSetName = SB.dataSetName feRTT
      uSuffix = SB.underscoredIf feDataSetName
  SB.add2dMatrixJson feRTT "X" "" (SB.NamedDim feDataSetName) n vecF -- JSON/code declarations for matrix
  SB.fixedEffectsQR_Data uSuffix ("X" <> uSuffix) feDataSetName ("X_" <> feDataSetName <> "_Cols") -- code for parameters and transformed parameters

addFixedEffectsParametersAndPriors :: forall r1 r2 d. (Typeable d)
                                   => Bool
                                   -> SB.StanExpr
                                   -> SB.RowTypeTag r1
                                   -> SB.RowTypeTag r2
                                   -> Maybe Text
                                   -> BuilderM d (SB.StanExpr, SB.StanExpr)
addFixedEffectsParametersAndPriors thinQR fePrior rttFE rttModeled mVarSuffix = do
  let feDataSetName = SB.dataSetName rttFE
      modeledDataSetName = fromMaybe "" mVarSuffix
      pSuffix = SB.underscoredIf feDataSetName
      uSuffix = pSuffix <> SB.underscoredIf modeledDataSetName
      rowIndexKey = SB.dataSetsCrosswalkName rttModeled rttFE
  SB.fixedEffectsQR_Parameters pSuffix ("X" <> uSuffix) ("X" <> pSuffix <> "_Cols")
  let eTheta = SB.name $ "thetaX" <> uSuffix
      eBeta  = SB.name $ "betaX" <> uSuffix
  SB.inBlock SB.SBModel $ do
    let e = eTheta `SB.vectorSample` fePrior
    SB.addExprLine "addFixedEffectsParametersAndPriors" e
--    SB.addStanLine $ "thetaX" <> uSuffix <> " ~ normal(0," <> show fePriorSD <> ")"
  let eQ = SB.indexBy  (SB.name  $ "Q" <> pSuffix <> "_ast") rowIndexKey
      eQTheta = eQ `SB.times` eTheta
      eX = SB.indexBy (SB.name ("centered_X" <> pSuffix)) rowIndexKey
      eXBeta = eX `SB.times` eBeta
      feExpr = if thinQR then eQTheta else eXBeta
  return (feExpr, eBeta)



buildIntMapBuilderF :: (k -> Either Text Int) -> (r -> k) -> FL.FoldM (Either Text) r (IM.IntMap k)
buildIntMapBuilderF eIntF keyF = FL.FoldM step (return IM.empty) return where
  step im r = case eIntF $ keyF r of
    Left msg -> Left $ "Indexing error when trying to build IntMap index: " <> msg
    Right n -> Right $ IM.insert n (keyF r) im

data PostStratificationType = PSRaw | PSShare (Maybe SB.StanExpr) deriving (Eq, Show)

-- TODO: order groups differently than the order coming from the built in group sets??
addPostStratification :: (Typeable d, Ord k) -- ,Typeable r, Typeable k)
                      => (SB.IndexKey -> BuilderM d SB.StanExpr) --NonEmpty (SB.StanDist args, args) -- e.g., sampling distribution connecting outcomes to model
--                      -> args -- required args, e.g., total counts for binomial
                      -> Maybe Text
                      -> SB.RowTypeTag rModel
                      -> SB.RowTypeTag rPS
                      -> SB.GroupRowMap rPS -- group mappings for PS data
                      -> Set.Set Text -- subset of groups to loop over
                      -> (rPS -> Double) -- PS weight
                      -> PostStratificationType -- raw or share
                      -> (Maybe (SB.GroupTypeTag k)) -- group to produce one PS per
                      -> BuilderM d SB.StanVar
addPostStratification psExprF mNameHead rttModel rttPS groupMaps modelGroups weightF psType mPSGroup = do
  -- check that all model groups in environment are accounted for in PS groups
  let showNames = T.intercalate "," . fmap (\(gtt DSum.:=> _) -> SB.taggedGroupName gtt) . DHash.toList
      psDataSetName = SB.dataSetName rttPS
      modelDataSetName = SB.dataSetName rttModel
      psGroupName = maybe "" SB.taggedGroupName mPSGroup
      uPSGroupName = maybe "" (\x -> "_" <> SB.taggedGroupName x) mPSGroup
      psSuffix = psDataSetName <> uPSGroupName
      namedPS = fromMaybe "PS" mNameHead <> "_" <> psSuffix
      sizeName = "N_" <> namedPS
      indexName = psDataSetName <> "_" <> psGroupName <> "_Index"
  SB.addDeclBinding namedPS (SB.name sizeName)
  rowInfos <- SB.rowBuilders <$> get
  modelGroupsDHM <- do
    case DHash.lookup rttModel rowInfos of
      Nothing -> SB.stanBuildError $ "Modeled data-set (\"" <> modelDataSetName <> "\") is not present in rowBuilders."
      Just (SB.RowInfo _ _ (SB.GroupIndexes gim) _ _) -> return gim
  -- allGroups <- SB.groupIndexByType <$> SB.askGroupEnv
  let usedGroups = DHash.filterWithKey (\(SB.GroupTypeTag n) _ -> n `Set.member` modelGroups) $ modelGroupsDHM
      checkGroupSubset n1 n2 gs1 gs2 = do
        let gDiff = DHash.difference gs1 gs2
        when (DHash.size gDiff /= 0)
          $ SB.stanBuildError
          $ n1 <> "(" <> showNames gs1 <> ") is not a subset of "
          <> n2 <> "(" <> showNames gs2 <> ")."
          <> "In " <> n1 <> " but not in " <> n2 <> ": " <> showNames gDiff <> "."
          <> " If this error appears entirely mysterious, try checking the *types* of your group key functions."
  checkGroupSubset "Modeling" "PS Spec" usedGroups groupMaps
  -- we don't need the PS group to be present on the RHS, but it could be
  let groupMapsToCheck = maybe groupMaps (\x -> DHash.delete x groupMaps) mPSGroup
  checkGroupSubset "PS Spec" "All Groups" groupMapsToCheck modelGroupsDHM
  toFoldable <- case DHash.lookup rttPS rowInfos of
    Nothing -> SB.stanBuildError $ "addPostStratification: RowTypeTag (" <> psDataSetName <> ") not found in rowBuilders."
    Just (SB.RowInfo tf _ _ _ _) -> return tf
  case mPSGroup of
    Nothing -> do
      SB.inBlock SB.SBData $ SB.stanDeclareRHS sizeName SB.StanInt "" $ SB.scalar "1" --SB.addJson rttPS sizeName SB.StanInt "<lower=0>"
      return ()
    Just gtt -> do
      rims <- SB.rowBuilders <$> get
      let psDataMissingErr = "addPostStratification: Post-stratification data-set "
                             <> psDataSetName
                             <> " is missing from rowBuilders."
          groupIndexMissingErr =  "addPostStratification: group "
                                  <> SB.taggedGroupName gtt
                                  <> " is missing from post-stratification data-set ("
                                  <> psDataSetName
                                  <> ")."
          groupRowMapMissingErr = "Specified group for PS sum (" <> SB.taggedGroupName gtt
                                  <> ") is not present in groupMaps: " <> showNames groupMaps

      (SB.GroupIndexes gis) <- SB.groupIndexes <$> (SB.stanBuildMaybe psDataMissingErr $ DHash.lookup rttPS rims)
      kToIntE <- SB.groupKeyToGroupIndex <$> (SB.stanBuildMaybe groupIndexMissingErr $ DHash.lookup gtt gis)
      SB.RowMap h <- SB.stanBuildMaybe groupRowMapMissingErr $ DHash.lookup gtt groupMaps
      SB.addIntMapBuilder rttPS gtt $ SB.buildIntMapBuilderF kToIntE h -- for extracting results
      SB.addColumnMJson rttPS indexName (SB.StanArray [SB.NamedDim psDataSetName] SB.StanInt) "<lower=0>" (kToIntE . h)
      SB.addJson rttPS sizeName SB.StanInt "<lower=0>"
        $ SJ.valueToPairF sizeName
        $ fmap (A.toJSON . Set.size)
        $ FL.premapM (kToIntE . h)
        $ FL.generalize FL.set
      SB.addDeclBinding indexName (SME.name sizeName)
      SB.addUseBindingToDataSet rttPS indexName $ SB.indexBy (SB.name indexName) psDataSetName
      return ()

  let usedGroupMaps = groupMaps `DHash.intersection` usedGroups
      ugNames = fmap (\(gtt DSum.:=> _) -> SB.taggedGroupName gtt) $ DHash.toList usedGroups
      weightArrayType = SB.StanVector $ SB.NamedDim psDataSetName  --SB.StanArray [SB.NamedDim namedPS] $ SB.StanArray groupDims SB.StanReal
  wgtsV <-  SB.addJson rttPS (namedPS <> "_wgts") weightArrayType ""
            $ SJ.valueToPairF (namedPS <> "_wgts")
            $ SJ.jsonArrayF weightF
  SB.inBlock SB.SBGeneratedQuantities $ do
    let errCtxt = "addPostStratification"
        divEq = SB.binOp "/="
    SB.useDataSetForBindings rttPS $ do
--      let eFromDistAndArgs (sDist, args) = SB.familyExp sDist psDataSetName args
      case mPSGroup of
        Nothing -> do
          psV <- SB.stanDeclareRHS namedPS SB.StanReal "" (SB.scalar "0")
          SB.bracketed 2 $ do
            wgtSumE <- case psType of
                         PSShare _ -> fmap SB.useVar
                                      $ SB.stanDeclareRHS (namedPS <> "_WgtSum") SB.StanReal "" (SB.scalar "0")
                         _ -> return SB.nullE
            SB.stanForLoopB "n" Nothing psDataSetName $ do
              psExpr <-  psExprF psDataSetName
              e <- SB.stanDeclareRHS ("e" <> namedPS) SB.StanReal "" psExpr --SB.multiOp "+" $ fmap eFromDistAndArgs sDistAndArgs
              SB.addExprLine errCtxt $ SB.useVar psV `SB.plusEq` (SB.useVar e `SB.times` SB.useVar wgtsV)
              case psType of
                PSShare Nothing -> SB.addExprLine errCtxt $ wgtSumE `SB.plusEq` SB.useVar wgtsV
                PSShare (Just e) -> SB.addExprLine errCtxt $ wgtSumE `SB.plusEq` (e `SB.times` SB.useVar wgtsV)
                _ -> return ()
            case psType of
              PSShare _ -> SB.addExprLine errCtxt $ SB.useVar psV `divEq` wgtSumE
              _ -> return ()
          return psV
        Just (SB.GroupTypeTag gn) -> do
          SB.addUseBinding namedPS $ SB.indexBy (SB.name namedPS) psDataSetName

          let zeroVec = SB.function "rep_vector" (SB.scalar "0" :| [SB.indexSize namedPS])
          psV <- SB.stanDeclareRHS namedPS (SB.StanVector $ SB.NamedDim indexName) "" zeroVec
          SB.bracketed 2 $ do
            wgtSumE <- case psType of
                         PSShare _ -> fmap SB.useVar
                                      $ SB.stanDeclareRHS (namedPS <> "_WgtSum") (SB.StanVector (SB.NamedDim indexName)) "" zeroVec
                         _ -> return SB.nullE
            SB.stanForLoopB "n" Nothing psDataSetName $ do
              psExpr <-  psExprF psDataSetName
              e <- SB.stanDeclareRHS ("e" <> namedPS) SB.StanReal "" psExpr --SB.multiOp "+" $ fmap eFromDistAndArgs sDistAndArgs
              SB.addExprLine errCtxt $ SB.useVar psV `SB.plusEq` (SB.useVar e `SB.times` SB.useVar wgtsV)
              case psType of
                PSShare Nothing -> SB.addExprLine errCtxt $ wgtSumE `SB.plusEq` SB.useVar wgtsV
                PSShare (Just e) -> SB.addExprLine errCtxt $ wgtSumE `SB.plusEq` (e `SB.times` SB.useVar wgtsV)
                _ -> return ()
            case psType of
              PSShare _ -> SB.stanForLoopB "n" Nothing indexName $ do
                SB.addExprLine errCtxt $ SB.useVar psV `divEq` wgtSumE
              _ -> return ()
          return psV

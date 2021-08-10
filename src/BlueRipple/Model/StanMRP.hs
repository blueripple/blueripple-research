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
import qualified Stan.ModelBuilder.SumToZero as SB
import qualified Stan.ModelBuilder.BuildingBlocks as SB
import qualified Stan.ModelBuilder.Distributions as SB
import qualified Stan.ModelConfig as SC
import qualified Stan.RScriptBuilder as SR
import qualified System.Environment as Env

import qualified Knit.Report as K
import qualified Knit.Effect.AtomicCache as K hiding (retrieveOrMake)
import Data.String.Here (here)

buildDataWranglerAndCode :: (Typeable d)
                         => SB.StanGroupBuilderM d ()
                         -> env
                         -> SB.StanBuilderM env d ()
                         -> d
                         -> SB.ToFoldable d modeledRow
                         -> Either Text (SC.DataWrangler d SB.DataSetGroupIntMaps (), SB.StanCode)
buildDataWranglerAndCode groupM env builderM d (SB.ToFoldable toFoldable) =
  let builderWithWrangler = do
        SB.addGroupIndexes
        _ <- builderM
        jsonRowBuilders <- SB.buildJSONF
        rowBuilders <- SB.rowBuilders <$> get
        intMapBuilders <- SB.indexBuilders <$> get
        return
          $ SC.Wrangle SC.TransientIndex
          $ \d -> (SB.buildIntMaps rowBuilders intMapBuilders d, SB.buildJSONFromRows jsonRowBuilders)
      resE = SB.runStanBuilder d toFoldable env groupM builderWithWrangler
  in fmap (\(SB.BuilderState _ _ _ _ c _, dw) -> (dw, c)) resE


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

type BuilderM modeledRow d = SB.StanBuilderM () d modeledRow

getIndexes :: BuilderM modeledRow d (Map Text (SB.IntIndex modeledRow))
getIndexes = SB.groupIndexByName <$> SB.askGroupEnv

getIndex :: GroupName -> BuilderM modeledRow d (SB.IntIndex modeledRow)
getIndex gn = do
  indexMap <- getIndexes
  case (Map.lookup gn indexMap) of
    Nothing -> SB.stanBuildError $ "No group index found for group with name=\"" <> gn <> "\""
    Just i -> return i

-- Basic group declarations, indexes and Json are produced automatically
addMRGroup :: (Typeable d, Typeable r) => SB.StanExpr -> SB.StanExpr -> SB.SumToZero -> GroupName -> BuilderM r d SB.StanExpr
addMRGroup binaryPrior sigmaPrior stz gn = do
  (SB.IntIndex indexSize _) <- getIndex gn
  when (indexSize < 2) $ SB.stanBuildError "Index with size <2 in MRGroup!"
  let binaryGroup = do
        let en = "eps_" <> gn
            be = SB.bracket
                 $ SB.csExprs (SB.name en :| [SB.name $ "-" <> en])
        let e' = SB.indexBy be gn
            modelTerm = SB.vectorFunction "to_vector" e' []
        SB.inBlock SB.SBParameters $ SB.stanDeclare ("eps_" <> gn) SB.StanReal ""
        SB.inBlock SB.SBModel $ do
          let priorE = SB.name en `SB.vectorSample` binaryPrior
          SB.addExprLine "intercept" priorE
--          SB.printExprM "intercept" (SB.fullyIndexedBindings mempty) (return priorE) >>= SB.addStanLine
        return modelTerm
  let nonBinaryGroup = do
        let gs t = t <> "_" <> gn
            modelTerm = SB.indexBy (SB.name $ gs "beta") gn
        sigmaVar <- SB.inBlock SB.SBParameters $ SB.stanDeclare (gs "sigma") SB.StanReal "<lower=0>"
        SB.inBlock SB.SBModel $ do
          let sigmaPriorE = SB.name (gs "sigma") `SB.vectorSample` sigmaPrior
          SB.addExprLine "addMRGroup" sigmaPriorE
--          SB.printExprM "addMRGroup" (SB.fullyIndexedBindings mempty) (return sigmaPriorLine) >>= SB.addStanLine
        let betaVar = SB.StanVar (gs "beta") (SB.StanVector $ SB.NamedDim gn)
        SB.rescaledSumToZero stz betaVar sigmaVar
        return modelTerm
  if indexSize == 2 then binaryGroup else nonBinaryGroup

--- return expression for sampling and one for everything else
addNestedMRGroup ::  (Typeable d, Typeable r)
                 => SB.StanExpr
                 -> SB.SumToZero
                 -> GroupName -- e.g., sex
                 -> GroupName -- partially pooled, e.g., state
                 -> BuilderM r d (SB.StanExpr, SB.StanExpr)
addNestedMRGroup  sigmaPrior stz nonPooledGN pooledGN = do
  let suffix = nonPooledGN <> "_" <> pooledGN
      suffixed x = x <> "_" <> suffix
  (SB.IntIndex pooledIndexSize _) <- getIndex pooledGN
  (SB.IntIndex nonPooledIndexSize _) <- getIndex nonPooledGN
  when (pooledIndexSize < 2) $ SB.stanBuildError $ "pooled index (" <> pooledGN <> ") with size <2 in nestedMRGroup!"
  when (nonPooledIndexSize < 2) $ SB.stanBuildError $ "non-pooled index (" <> nonPooledGN <> ") with size <2 in nestedMRGroup!"
  let nonPooledBinary = do
        sigmaVar <- SB.inBlock SB.SBParameters $ SB.stanDeclare (suffixed "sigma") SB.StanReal "<lower=0>"
        let epsVar = SB.StanVar (suffixed "eps") (SB.StanVector $ SB.NamedDim pooledGN)
        SB.inBlock SB.SBModel $ do
          let e1 = SB.name (suffixed "sigma") `SB.vectorSample` sigmaPrior
          SB.addExprLine "addNestedMRGroup (binary non-pooled)" e1
        SB.rescaledSumToZero stz epsVar sigmaVar
        (yE, epsE) <- SB.inBlock SB.SBTransformedParameters $ do
          yv <- SB.stanDeclare (suffixed "y") (SB.StanVector $ SB.NamedDim SB.modeledDataIndexName) ""
          let yE = SB.useVar yv
              be = SB.bracket
                   $ SB.csExprs (SB.indexBy (SB.name $ suffixed "eps") pooledGN :|
                                  [SB.indexBy (SB.name $ suffixed "-eps") pooledGN]
                                )
              epsE =  SB.indexBy be nonPooledGN
          SB.stanForLoopB "n" Nothing SB.modeledDataIndexName
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
          y <- SB.stanDeclare (suffixed "y") (SB.StanVector $ SB.NamedDim SB.modeledDataIndexName) ""
          SB.stanForLoopB "n" Nothing SB.modeledDataIndexName
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


type GroupName = Text

data FixedEffects row = FixedEffects Int (row -> Vec.Vector Double)

emptyFixedEffects :: DHash.DHashMap SB.RowTypeTag FixedEffects
emptyFixedEffects = DHash.empty

-- returns
-- 'X * beta' (or 'Q * theta') model term expression
-- 'X * beta' and just 'beta' for post-stratification
-- The latter is for use in post-stratification at fixed values of the fixed effects.
addFixedEffects :: forall r d r0.(Typeable d, Typeable r0)
                => Bool
                -> SB.StanExpr
                -> SB.RowTypeTag r
                -> FixedEffects r
                -> BuilderM r0 d (SB.StanExpr, SB.StanExpr, SB.StanExpr)
addFixedEffects thinQR fePrior feRTT (FixedEffects n vecF) = do
  let suffix = SB.dsSuffix feRTT
      uSuffix = SB.underscoredIf suffix
      feDataName = SB.dsName feRTT
  SB.add2dMatrixJson feRTT "X" (Just suffix) "" (SB.NamedDim feDataName) n vecF -- JSON/code declarations for matrix
  SB.fixedEffectsQR uSuffix ("X" <> uSuffix) feDataName ("X_" <> feDataName <> "_Cols") -- code for parameters and transformed parameters
  -- model
  SB.inBlock SB.SBModel $ do
    let e = SB.name ("thetaX" <> uSuffix) `SB.vectorSample` fePrior
    SB.addExprLine "addFixedEffects" e
--    SB.addStanLine $ "thetaX" <> uSuffix <> " ~ normal(0," <> show fePriorSD <> ")"
  let eQ = if T.null suffix
           then SB.indexBy (SB.name "Q_ast") feDataName
           else SB.indexBy  (SB.name  $ "Q" <> uSuffix <> "_ast") feDataName
      eTheta = SB.name $ "thetaX" <> uSuffix
      eQTheta = eQ `SB.times` eTheta
      eX = if T.null suffix
           then SB.indexBy (SB.name "centered_X") feDataName
           else SB.indexBy (SB.name ("centered_X" <> uSuffix)) feDataName
      eBeta = SB.name $ "betaX" <> uSuffix
      eXBeta = eX `SB.times` eBeta
      feExpr = if thinQR then eQTheta else eXBeta
  return (feExpr, eXBeta, eBeta)

addFixedEffectsData :: forall r d r0.(Typeable d, Typeable r0)
                    => SB.RowTypeTag r
                    -> FixedEffects r
                    -> BuilderM r0 d ()
addFixedEffectsData feRTT (FixedEffects n vecF) = do
  let suffix = SB.dsSuffix feRTT
      uSuffix = SB.underscoredIf suffix
      feDataName = SB.dsName feRTT
  SB.add2dMatrixJson feRTT "X" (Just suffix) "" (SB.NamedDim feDataName) n vecF -- JSON/code declarations for matrix
  SB.fixedEffectsQR_Data uSuffix ("X" <> uSuffix) feDataName ("X_" <> feDataName <> "_Cols") -- code for parameters and transformed parameters
  return ()

addFixedEffectsParametersAndPriors :: forall r1 r2 d r0.(Typeable d, Typeable r0)
                                   => Bool
                                   -> SB.StanExpr
                                   -> SB.RowTypeTag r1
                                   -> SB.RowTypeTag r2
                                   -> BuilderM r0 d (SB.StanExpr, SB.StanExpr, SB.StanExpr)
addFixedEffectsParametersAndPriors thinQR fePrior feRTT dataRTT = do
  let feSuffix = SB.dsSuffix feRTT
      dataSuffix = SB.dsSuffix dataRTT
      uSuffix = SB.underscoredIf feSuffix <> SB.underscoredIf dataSuffix
      feDataName = SB.dsName feRTT
  SB.fixedEffectsQR_Parameters uSuffix ("X" <> uSuffix) ("X_" <> feDataName <> "_Cols")
  -- model
  SB.inBlock SB.SBModel $ do
    let e = SB.name ("thetaX" <> uSuffix) `SB.vectorSample` fePrior
    SB.addExprLine "addFixedEffects" e
--    SB.addStanLine $ "thetaX" <> uSuffix <> " ~ normal(0," <> show fePriorSD <> ")"
  let eQ = SB.indexBy  (SB.name  $ "Q" <> uSuffix <> "_ast") feDataName
      eTheta = SB.name $ "thetaX" <> uSuffix
      eQTheta = eQ `SB.times` eTheta
      eX = SB.indexBy (SB.name ("centered_X" <> uSuffix)) feDataName
      eBeta = SB.name $ "betaX" <> uSuffix
      eXBeta = eX `SB.times` eBeta
      feExpr = if thinQR then eQTheta else eXBeta
  return (feExpr, eXBeta, eBeta)



buildIntMapBuilderF :: (k -> Either Text Int) -> (r -> k) -> FL.FoldM (Either Text) r (IM.IntMap k)
buildIntMapBuilderF eIntF keyF = FL.FoldM step (return IM.empty) return where
  step im r = case eIntF $ keyF r of
    Left msg -> Left $ "Indexing error when trying to build IntMap index: " <> msg
    Right n -> Right $ IM.insert n (keyF r) im

data PostStratificationType = PSRaw | PSShare deriving (Eq, Show)

-- TODO: order groups differently than the order coming from the built in group sets??
addPostStratification :: (Typeable d, Typeable r0, Typeable r, Typeable k)
                      => SB.StanDist args -- e.g., sampling distribution connecting outcomes to model
                      -> args -- required args, e.g., total counts for binomial
                      -> Maybe Text
                      -> SB.RowTypeTag r
                      -> SB.GroupRowMap r -- group mappings for PS data
                      -> Set.Set Text -- subset of groups to loop over
                      -> (r -> Double) -- PS weight
                      -> PostStratificationType -- raw or share
                      -> (Maybe (SB.GroupTypeTag k)) -- group to produce one PS per
                      -> BuilderM r0 d SB.StanVar
addPostStratification sDist args mNameHead rtt groupMaps modelGroups weightF psType mPSGroup = do
  -- check that all model groups in environment are accounted for in PS groups
  let showNames = T.intercalate "," . fmap (\(gtt DSum.:=> _) -> SB.taggedGroupName gtt) . DHash.toList
      dsName = SB.dsName rtt
      psGroupName = maybe "" SB.taggedGroupName mPSGroup
      uPSGroupName = maybe "" (\x -> "_" <> SB.taggedGroupName x) mPSGroup
      psSuffix = dsName <> uPSGroupName
      namedPS = fromMaybe "PS" mNameHead <> "_" <> psSuffix
      sizeName = "N_" <> namedPS
  SB.addDeclBinding namedPS (SB.name sizeName)
  allGroups <- SB.groupIndexByType <$> SB.askGroupEnv
  let usedGroups = DHash.filterWithKey (\(SB.GroupTypeTag n) _ -> n `Set.member` modelGroups) $ allGroups
  let checkGroupSubset n1 n2 gs1 gs2 = do
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
  checkGroupSubset "PS Spec" "All Groups" groupMapsToCheck allGroups
  rowBuilders <- SB.rowBuilders <$> get
  toFoldable <- case DHash.lookup rtt rowBuilders of
    Nothing -> SB.stanBuildError $ "addPostStratification: RowTypeTag (" <> SB.dsName rtt <> ") not found in rowBuilders."
    Just (SB.RowInfo tf _ _) -> return tf
  intKeyF  <- flip (maybe (return $ const $ Right 1)) mPSGroup $ \gtt -> do
    let errMsg tGrps = "Specified group for PS sum (" <> SB.taggedGroupName gtt
                       <> ") is not present in Builder groups: " <> tGrps
    SB.IndexMap _ eIntF _ <- SB.stanBuildMaybe (errMsg $ showNames allGroups) $ DHash.lookup gtt allGroups
    SB.RowMap h <- SB.stanBuildMaybe (errMsg $ showNames groupMaps) $  DHash.lookup gtt groupMaps
    SB.addIndexIntMapFld rtt gtt $ buildIntMapBuilderF eIntF h
    return $ eIntF . h
  -- add the data set for the json builders

  -- size of post-stratification result
  -- e.g., N_PS_ACS_State
  SB.addJson rtt sizeName SB.StanInt "<lower=0>"
    $ SJ.valueToPairF sizeName
    $ fmap (A.toJSON . Set.size)
    $ FL.premapM intKeyF
    $ FL.generalize FL.set
  let usedGroupMaps = groupMaps `DHash.intersection` usedGroups
      ugNames = fmap (\(gtt DSum.:=> _) -> SB.taggedGroupName gtt) $ DHash.toList usedGroups
      weightArrayType = SB.StanVector $ SB.NamedDim dsName  --SB.StanArray [SB.NamedDim namedPS] $ SB.StanArray groupDims SB.StanReal
  wgtsV <-  SB.addJson rtt (namedPS <> "_wgts") weightArrayType ""
            $ SJ.valueToPairF (namedPS <> "_wgts")
            $ SJ.jsonArrayF weightF
  SB.inBlock SB.SBGeneratedQuantities $ do
    let errCtxt = "addPostStratification"
        indexToPS x = SB.indexBy (SB.name $ x <> "_" <> dsName) dsName
        useBindings =  Map.fromList $ zip ugNames $ fmap indexToPS ugNames
        divEq = SB.binOp "/="
    case mPSGroup of
      Nothing -> do
        SB.withUseBindings useBindings $ do
          psV <- SB.stanDeclareRHS namedPS SB.StanReal "" (SB.scalar "0")
          SB.bracketed 2 $ do
            wgtSumE <- if psType == PSShare
                       then fmap SB.useVar $ SB.stanDeclareRHS (namedPS <> "_WgtSum") SB.StanReal "" (SB.scalar "0")
                       else return SB.nullE
            SB.stanForLoopB "n" Nothing dsName $ do
              e <- SB.stanDeclareRHS ("e" <> namedPS) SB.StanReal "" $ SB.familyExp sDist dsName args --expCode
              SB.addExprLine errCtxt $ SB.useVar psV `SB.plusEq` (SB.useVar e `SB.times` SB.useVar wgtsV)
              when (psType == PSShare) $ SB.addExprLine errCtxt $ wgtSumE `SB.plusEq` SB.useVar wgtsV
            when (psType == PSShare) $ SB.addExprLine errCtxt $ SB.useVar psV `divEq` wgtSumE
          return psV
      Just (SB.GroupTypeTag gn) -> do
        let useBindings' = Map.insert namedPS (SB.indexBy (SB.name $ gn <> "_" <> dsName) dsName) useBindings
        SB.withUseBindings useBindings' $ do
          let zeroVec = SB.function "rep_vector" (SB.scalar "0" :| [SB.indexSize namedPS])
          psV <- SB.stanDeclareRHS namedPS (SB.StanVector $ SB.NamedDim namedPS) "" zeroVec
          SB.bracketed 2 $ do
            wgtSumE <- if psType == PSShare
                       then fmap SB.useVar $ SB.stanDeclareRHS (namedPS <> "_WgtSum") (SB.StanVector (SB.NamedDim namedPS)) "" zeroVec
                       else return SB.nullE
            SB.stanForLoopB "n" Nothing dsName $ do
              e <- SB.stanDeclareRHS ("e" <> namedPS) SB.StanReal "" $ SB.familyExp sDist dsName args
              SB.addExprLine errCtxt $ SB.useVar psV `SB.plusEq` (SB.useVar e `SB.times` SB.useVar wgtsV)
              when (psType == PSShare) $ SB.addExprLine errCtxt $ wgtSumE `SB.plusEq` SB.useVar wgtsV
            when (psType == PSShare) $ SB.stanForLoopB "n" Nothing namedPS $ do
              SB.addExprLine errCtxt $ SB.useVar psV `divEq` wgtSumE
          return psV

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
  K.wrapPrefix "StanMRP" $ do
  K.logLE K.Info $ "Running: model=" <> modelName <> " using data=" <> dataName
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
  K.logLE (K.Debug 1) $ "modelDep: " <> show (K.cacheTime modelDep)
  K.logLE (K.Debug 1) $ "houseDataDep: " <> show (K.cacheTime data_C)
  let dataModelDep = const <$> modelDep <*> data_C
      getResults s () inputAndIndex_C = return ()
      unwraps = [SR.UnwrapNamed ppName ppName]
  BR.retrieveOrMakeD resultCacheKey dataModelDep $ \() -> do
    K.logLE K.Diagnostic "Data or model newer then last cached result. (Re)-running..."
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
           -> SB.GroupModel
           -> SB.GroupTypeTag k
           -> Maybe Text
           -> SB.StanBuilderM env d (SB.StanExpr, SB.StanVar)
addGroup rtt binaryPrior gm gtt mVarSuffix = do
  SB.setDataSetForBindings rtt
  (SB.IntIndex indexSize _) <- SB.rowToGroupIndex <$> SB.indexMap rtt gtt
  let gn = SB.taggedGroupName gtt
      gs t = t <> fromMaybe "" mVarSuffix <> "_" <> gn
  when (indexSize < 2) $ SB.stanBuildError "Index with size <2 in MRGroup!"
  let binaryGroup = do
        let en = gs "eps"
        epsVar <- SB.inBlock SB.SBParameters $ SB.stanDeclare en SB.StanReal ""
        let be = SB.bracket
                 $ SB.csExprs (SB.var epsVar :| [SB.negate $ SB.var epsVar])
        let e' = SB.indexBy be gn -- this is weird. But {a,-a} is an array and can be indexed
            modelTerm = SB.vectorFunction "to_vector" e' []
        SB.inBlock SB.SBModel $ do
          let priorE = SB.name en `SB.vectorSample` binaryPrior
          SB.addExprLine "intercept" priorE
        return (modelTerm, epsVar)
  let nonBinaryGroup = do
        let betaVar = SB.StanVar (gs "beta") (SB.StanVector $ SB.NamedDim gn)
            modelTerm = SB.var betaVar --SB.indexBy (SB.name $ gs "beta") gn
        SB.groupModel betaVar gm
        return (modelTerm, betaVar)
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


data Phantom k = Phantom
addGroupForInteractions :: SB.GroupTypeTag k
                        -> DHash.DHashMap SB.GroupTypeTag Phantom
                        -> DHash.DHashMap SB.GroupTypeTag Phantom
addGroupForInteractions gtt dhm = DHash.insert gtt Phantom dhm

withSome :: (forall k.SB.GroupTypeTag k -> a) -> DHash.Some SB.GroupTypeTag -> a
withSome f (DHash.Some gtt) = f gtt

addInteractions :: Typeable d
                 => SB.RowTypeTag r
                 -> SB.GroupModel
                 -> DHash.DHashMap SB.GroupTypeTag Phantom
                 -> Int
                 -> Maybe Text
                 -> SB.StanBuilderM env d [SB.StanVar]
addInteractions rtt gm groups nInteracting mSuffix = do
  SB.setDataSetForBindings rtt
  let groupList = DHash.keys groups -- [Some GroupTypeTag]
  when (nInteracting < 2) $ SB.stanBuildError $ "addInteractions: nInteracting must be at least 2"
  when (nInteracting > length groupList)
    $ SB.stanBuildError
    $ "addInteractions: nInteracting ("
    <> show nInteracting
    <> ") must be at most the number of groups ("
    <> show (length groupList)
    <> ")"
  let indexSize (DHash.Some gtt) = do
        (SB.IntIndex indexSize _) <- SB.rowToGroupIndex <$> SB.indexMap rtt gtt
        return indexSize
  iSizes <- traverse indexSize groupList
  minSize <- SB.stanBuildMaybe "addInteractions: empty groupList" $ FL.fold FL.minimum iSizes
  when (minSize < 2) $ SB.stanBuildError "addInteractions: Index with size < 2"
  let groupCombos = choices groupList nInteracting
      groupNameList = fmap (withSome SB.taggedGroupName)
      betaName l = "beta_" <> (T.intercalate "_" $ groupNameList l) <> maybe "" (("_" <>))  mSuffix
      betaType l = SB.StanArray (fmap SB.NamedDim $ groupNameList l) SB.StanReal
      betaVar l = SB.StanVar (betaName l) (betaType l)
      betaVars = betaVar <$> groupCombos
  SB.groupModel' betaVars gm

-- generate all possible collections of distinct elements of a given length from a list
choices :: [a] -> Int -> [[a]]
choices _ 0 = [[]]
choices [] _ = []
choices (x:xs) n = if (n > length (x:xs))
                   then []
                   else fmap (x:) (choices xs (n-1)) ++ choices xs n

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
      rowIndexKey = SB.crosswalkIndexKey rttFE --SB.dataSetsCrosswalkName rttModeled rttFE
      colKey = "X_" <> feDataSetName <> "_Cols"
--      rowIndexExpr =
  SB.add2dMatrixJson rttFE "X" "" (SB.NamedDim feDataSetName) n vecF -- JSON/code declarations for matrix
  (qVar, thetaVar, betaVar, f) <- SB.fixedEffectsQR uSuffix ("X" <> uSuffix) feDataSetName colKey -- code for parameters and transformed parameters
  -- model
  SB.inBlock SB.SBModel $ do
    let e = SB.vectorized (one colKey) (SB.var thetaVar) `SB.vectorSample` fePrior
    SB.addExprLine "addFixedEffects" e
--    SB.addStanLine $ "thetaX" <> uSuffix <> " ~ normal(0," <> show fePriorSD <> ")"
  let eQ = SB.var qVar
{-
        if T.null feDataSetName
           then SB.indexBy (SB.name "Q_ast") rowIndexKey
           else SB.indexBy  (SB.name  $ "Q" <> uSuffix <> "_ast") rowIndexKey
-}
  --    eTheta = SB.var thetaVar --SB.name $ "thetaX" <> uSuffix
      eQTheta = SB.matMult qVar thetaVar
      xName = if T.null feDataSetName then "centered_X" else "centered_X" <> uSuffix
      xVar = SB.StanVar xName (SB.varType qVar)
      eX = SB.var xVar
      eBeta = SB.var betaVar --SB.name $ "betaX" <> uSuffix
      eXBeta = SB.matMult xVar betaVar
      feExpr = if thinQR then eQTheta else eXBeta
  return (feExpr, f, eBeta)

addFixedEffectsData :: forall r d. (Typeable d)
                    => SB.RowTypeTag r
                    -> FixedEffects r
                    -> BuilderM d (SB.StanVar, SB.StanVar -> BuilderM d SB.StanVar)
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
                                   -> BuilderM d (SB.StanExpr, SB.StanVar)
addFixedEffectsParametersAndPriors thinQR fePrior rttFE rttModeled mVarSuffix = do
  let feDataSetName = SB.dataSetName rttFE
      modeledDataSetName = fromMaybe "" mVarSuffix
      pSuffix = SB.underscoredIf feDataSetName
      uSuffix = pSuffix <> SB.underscoredIf modeledDataSetName
      rowIndexKey = SB.crosswalkIndexKey rttFE --SB.dataSetCrosswalkName rttModeled rttFE
      colIndexKey =  "X" <> pSuffix <> "_Cols"
  (betaVar, thetaVar) <- SB.fixedEffectsQR_Parameters pSuffix ("X" <> uSuffix) colIndexKey
  let eTheta = SB.var thetaVar --SB.name $ "thetaX" <> uSuffix
      eBeta  = SB.var betaVar --SB.name $ "betaX" <> uSuffix
  SB.inBlock SB.SBModel $ do
    let e = SB.vectorized (one colIndexKey) eTheta `SB.vectorSample` fePrior
    SB.addExprLine "addFixedEffectsParametersAndPriors" e
--    SB.addStanLine $ "thetaX" <> uSuffix <> " ~ normal(0," <> show fePriorSD <> ")"
  let xType = SB.StanMatrix (SB.NamedDim rowIndexKey, SB.NamedDim colIndexKey) -- it's weird to have to create this here...
      qName = "Q" <> pSuffix <> "_ast"
      qVar = SB.StanVar qName xType
--      eQ = SB.var qVar --SB.indexBy  (SB.name  $ "Q" <> pSuffix <> "_ast") rowIndexKey
      eQTheta = SB.matMult qVar thetaVar
      xName = "centered_X" <> pSuffix
      xVar = SB.StanVar xName xType
--      eX = SB.var xVar --SB.indexBy (SB.name ("centered_X" <> pSuffix)) rowIndexKey
      eXBeta = SB.matMult xVar betaVar
  let feExpr = if thinQR then eQTheta else eXBeta
  return (feExpr, betaVar)



buildIntMapBuilderF :: (k -> Either Text Int) -> (r -> k) -> FL.FoldM (Either Text) r (IM.IntMap k)
buildIntMapBuilderF eIntF keyF = FL.FoldM step (return IM.empty) return where
  step im r = case eIntF $ keyF r of
    Left msg -> Left $ "Indexing error when trying to build IntMap index: " <> msg
    Right n -> Right $ IM.insert n (keyF r) im

data PostStratificationType = PSRaw | PSShare (Maybe SB.StanExpr) deriving (Eq, Show)

-- TODO: order groups differently than the order coming from the built in group sets??
addPostStratification :: (Typeable d, Ord k) -- ,Typeable r, Typeable k)
                      => (SB.IndexKey -> BuilderM d SB.StanExpr)
                      -> Maybe Text
                      -> SB.RowTypeTag rModel
                      -> SB.RowTypeTag rPS
                      -> SB.GroupSet -- groups to sum over
--                      -> Set.Set Text -- subset of groups to loop over
                      -> (rPS -> Double) -- PS weight
                      -> PostStratificationType -- raw or share
                      -> (Maybe (SB.GroupTypeTag k)) -- group to produce one PS per
                      -> BuilderM d SB.StanVar
addPostStratification psExprF mNameHead rttModel rttPS sumOverGroups {-sumOverGroups-} weightF psType mPSGroup = do
  -- check that all model groups in environment are accounted for in PS groups
  let psDataSetName = SB.dataSetName rttPS
      modelDataSetName = SB.dataSetName rttModel
      psGroupName = maybe "" SB.taggedGroupName mPSGroup
      uPSGroupName = maybe "" (\x -> "_" <> SB.taggedGroupName x) mPSGroup
      psSuffix = psDataSetName <> uPSGroupName
      namedPS = fromMaybe "PS" mNameHead <> "_" <> psSuffix
      sizeName = "N_" <> namedPS
      indexName = psDataSetName <> "_" <> psGroupName <> "_Index"
  SB.addDeclBinding namedPS $ SB.StanVar sizeName SME.StanInt
  rowInfos <- SB.rowBuilders <$> get
  modelGroupsDHM <- do
    case DHash.lookup rttModel rowInfos of
      Nothing -> SB.stanBuildError $ "Modeled data-set (\"" <> modelDataSetName <> "\") is not present in rowBuilders."
      Just (SB.RowInfo _ _ (SB.GroupIndexes gim) _ _) -> return gim
  psGroupsDHM <- do
     case DHash.lookup rttPS rowInfos of
       Nothing -> SB.stanBuildError $ "Post-stratification data-set (\"" <> psDataSetName <> "\") is not present in rowBuilders."
       Just (SB.RowInfo _ _ (SB.GroupIndexes gim) _ _) -> return gim
  checkGroupSubset "Sum Over" "Poststratification data-set" sumOverGroups psGroupsDHM
  checkGroupSubset "Sum Over" "Modeled data-set" sumOverGroups modelGroupsDHM
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
          psGroupMissingErr = "Specified group for PS sum (" <> SB.taggedGroupName gtt
                              <> ") is not present in post-stratification data-set: " <> showNames psGroupsDHM --Maps

      (SB.GroupIndexes gis) <- SB.groupIndexes <$> SB.stanBuildMaybe psDataMissingErr (DHash.lookup rttPS rims)
      kToIntE <- SB.groupKeyToGroupIndex <$> SB.stanBuildMaybe groupIndexMissingErr (DHash.lookup gtt gis)
      rowToK <- SB.rowToGroup <$> SB.stanBuildMaybe psGroupMissingErr (DHash.lookup gtt psGroupsDHM)
      SB.addIntMapBuilder rttPS gtt $ SB.buildIntMapBuilderF kToIntE rowToK -- for extracting results
      -- This is hacky.  We need a more principled way to know if re-adding same data is an issue.
      let indexType = SB.StanArray [SB.NamedDim psDataSetName] SB.StanInt
      SB.addColumnMJsonOnce rttPS indexName indexType "<lower=0>" (kToIntE . rowToK)
      SB.addJson rttPS sizeName SB.StanInt "<lower=0>"
        $ SJ.valueToPairF sizeName
        $ fmap (A.toJSON . Set.size)
        $ FL.premapM (kToIntE . rowToK)
        $ FL.generalize FL.set
      SB.addDeclBinding indexName $ SME.StanVar sizeName SME.StanInt
--      SB.addUseBindingToDataSet rttPS indexName $ SB.indexBy (SB.name indexName) psDataSetName
      SB.addUseBindingToDataSet rttPS indexName $ SB.StanVar indexName indexType
      return ()

  let weightArrayType = SB.StanVector $ SB.NamedDim psDataSetName  --SB.StanArray [SB.NamedDim namedPS] $ SB.StanArray groupDims SB.StanReal
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
                         PSShare _ -> fmap SB.var
                                      $ SB.stanDeclareRHS (namedPS <> "_WgtSum") SB.StanReal "" (SB.scalar "0")
                         _ -> return SB.nullE
            SB.stanForLoopB "n" Nothing psDataSetName $ do
              psExpr <- psExprF psDataSetName
              e <- SB.stanDeclareRHS ("e" <> namedPS) SB.StanReal "" psExpr --SB.multiOp "+" $ fmap eFromDistAndArgs sDistAndArgs
              SB.addExprLine errCtxt $ SB.var psV `SB.plusEq` (SB.var e `SB.times` SB.var wgtsV)
              case psType of
                PSShare Nothing -> SB.addExprLine errCtxt $ wgtSumE `SB.plusEq` SB.var wgtsV
                PSShare (Just e) -> SB.addExprLine errCtxt $ wgtSumE `SB.plusEq` (e `SB.times` SB.var wgtsV)
                _ -> return ()
            case psType of
              PSShare _ -> SB.addExprLine errCtxt $ SB.var psV `divEq` wgtSumE
              _ -> return ()
          return psV
        Just (SB.GroupTypeTag gn) -> do
--          SB.addUseBinding namedPS $ SB.indexBy (SB.name namedPS) psDataSetName
--          SB.addUseBinding namedPS $ SB.StanVar namedPS (SB.StanVector $ SB.NamedDim psDataSetName) --SB.indexBy (SB.name namedPS) psDataSetName

          let zeroVec = SB.function "rep_vector" (SB.scalar "0" :| [SB.indexSize namedPS])
          psV <- SB.stanDeclareRHS namedPS (SB.StanVector $ SB.NamedDim indexName) "" zeroVec
          SB.bracketed 2 $ do
            wgtSumE <- case psType of
                         PSShare _ -> fmap SB.var
                                      $ SB.stanDeclareRHS (namedPS <> "_WgtSum") (SB.StanVector (SB.NamedDim indexName)) "" zeroVec
                         _ -> return SB.nullE
            SB.stanForLoopB "n" Nothing psDataSetName $ do
              psExpr <-  psExprF psDataSetName
              e <- SB.stanDeclareRHS ("e" <> namedPS) SB.StanReal "" psExpr --SB.multiOp "+" $ fmap eFromDistAndArgs sDistAndArgs
              SB.addExprLine errCtxt $ SB.var psV `SB.plusEq` (SB.var e `SB.times` SB.var wgtsV)
              case psType of
                PSShare Nothing -> SB.addExprLine errCtxt $ wgtSumE `SB.plusEq` SB.var wgtsV
                PSShare (Just e) -> SB.addExprLine errCtxt $ wgtSumE `SB.plusEq` (e `SB.times` SB.var wgtsV)
                _ -> return ()
            case psType of
              PSShare _ -> SB.stanForLoopB "n" Nothing indexName $ do
                SB.addExprLine errCtxt $ SB.var psV `divEq` wgtSumE
              _ -> return ()
          return psV

showNames :: DHash.DHashMap SB.GroupTypeTag a -> Text
showNames = T.intercalate "," . fmap (\(gtt DSum.:=> _) -> SB.taggedGroupName gtt) . DHash.toList

checkGroupSubset :: Text
                 -> Text
                 -> DHash.DHashMap SB.GroupTypeTag a
                 -> DHash.DHashMap SB.GroupTypeTag b
                 -> SB.StanBuilderM env d ()
checkGroupSubset n1 n2 gs1 gs2 = do
  let gDiff = DHash.difference gs1 gs2
  when (DHash.size gDiff /= 0)
    $ SB.stanBuildError
    $ n1 <> "(" <> showNames gs1 <> ") is not a subset of "
    <> n2 <> "(" <> showNames gs2 <> ")."
    <> "In " <> n1 <> " but not in " <> n2 <> ": " <> showNames gDiff <> "."
    <> " If this error appears entirely mysterious, try checking the *types* of your group key functions."

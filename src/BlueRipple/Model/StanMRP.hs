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

buildDataWranglerAndCode :: (Typeable d, Typeable modeledRow)
                         => SB.StanGroupBuilderM modeledRow ()
                         -> env
                         -> SB.StanBuilderM env d modeledRow ()
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
          $ \d -> (SB.buildIntMaps rowBuilders intMapBuilders d, flip SB.buildJSONFromRows jsonRowBuilders)
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
        let e' = SB.uIndexed gn
                 $ SB.bracket
                 $ SB.args [SB.name en, SB.name $ "-" <> en]
            modelTerm = SB.vectorFunction "to_vector" e' []
        SB.inBlock SB.SBParameters $ SB.stanDeclare ("eps_" <> gn) SB.StanReal ""
        SB.inBlock SB.SBModel $ do
          let priorE = SB.name en `SB.vectorSample` binaryPrior
          SB.addExprLine "intercept" priorE
--          SB.printExprM "intercept" (SB.fullyIndexedBindings mempty) (return priorE) >>= SB.addStanLine
        return modelTerm
  let nonBinaryGroup = do
        let gs t = t <> "_" <> gn
            modelTerm = SB.uIndexed gn $ SB.name $ gs "beta"
        sigmaVar <- SB.inBlock SB.SBParameters $ SB.stanDeclare (gs "sigma") SB.StanReal "<lower=0>"
        SB.inBlock SB.SBModel $ do
          let sigmaPriorE = SB.name (gs "sigma") `SB.vectorSample` sigmaPrior
          SB.addExprLine "addMRGroup" sigmaPriorE
--          SB.printExprM "addMRGroup" (SB.fullyIndexedBindings mempty) (return sigmaPriorLine) >>= SB.addStanLine
        let betaVar = SB.StanVar (gs "beta") (SB.StanVector $ SB.NamedDim gn)
        SB.rescaledSumToZero stz betaVar sigmaVar
        return modelTerm
  if indexSize == 2 then binaryGroup else nonBinaryGroup


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
--          SB.printExprM "addNestedMRGroup" (SB.fullyIndexedBindings mempty) (return e1) >>= SB.addStanLine
        SB.rescaledSumToZero stz epsVar sigmaVar
        (yE, epsE) <- SB.inBlock SB.SBTransformedParameters $ do
          yv <- SB.stanDeclare (suffixed "y") (SB.StanVector $ SB.NamedDim SB.modeledDataIndexName) ""
          let yE = SB.useVar yv --SB.indexed SB.modeledDataIndexName $ SB.name $ suffixed "y"
              epsE =  SB.uIndexed nonPooledGN
                      $ SB.bracket
                      $ SB.args [SB.uIndexed pooledGN $ SB.name $ suffixed "eps"
                                ,SB.uIndexed pooledGN $ SB.name $ suffixed "-eps"
                                ]
          SB.stanForLoopB "n" Nothing SB.modeledDataIndexName $ SB.addExprLine "addNestedMRGroup: y-loop" $ yE `SB.eq` epsE
--            $ const
--            $ SB.addStanLine
--            $ suffixed "y" <> "[n] = {" <> suffixed "eps" <> "[" <> pooledGN <> "[n]], -" <> suffixed "eps" <> "[" <> pooledGN <> "[n]]}[" <> nonPooledGN <> "[n]]"
          return (yE, epsE)

        return (yE, epsE)
      nonPooledNonBinary = undefined
{-
        do
        let npGS = "N_" <> nonPooledGN
            pGS = "N_" <> pooledGN
        (sigmaV, lcorrV) <- SB.inBlock SB.SBParameters $ do
          sv <- SB.stanDeclare (suffixed "sigma") (SB.StanVector (SB.NamedDim npGS)) "<lower=0>"
          lv <- SB.stanDeclare (suffixed "lCorr") (SB.StanCholeskyFactorCorr (SB.NamedDim npGS)) ""
          return (sv, lv)

        let betaVar = SB.StanVar (suffixed "beta")  (SB.StanMatrix (SB.NamedDim npGS, SB.NamedDim pGS))
        SB.inBlock SB.SBModel $ do
          let e1 = SB.name (suffixed "sigma")  `SB.vectorSample` sigmaPrior -- NB this should prolly be some LKJ thing
          SB.printExprM "addNestedMRGroup" (SB.fullyIndexedBindings mempty) (return e1) >>= SB.addStanLine
        SB.rescaledSumToZero SB.STZNone betaVar sigmaVar  -- FIX, we can't sum to zero along cols or rows.
        yVar <- SB.inBlock SB.SBTransformedParameters $ do
          yv <- SB.stanDeclare (suffixed "y") (SB.StanVector $ SB.NamedDim "N") ""
          SB.stanForLoop "n" Nothing "N"
            $ const
            $ SB.addStanLine
            $ suffixed "y" <> "[n] = " <> suffixed "beta" <> "[" <> nonPooledGN <> "[n], " <> pooledGN <> "[n]]"
          return yv
        let yE = SB.indexed SB.modeledDataIndexName $ SB.name $ suffixed "y"
            betaE =  SB.indexed nonPooledGN $ SB.indexed pooledGN $ SB.name $ suffixed "beta"
        return (yE, betaE)
-}
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
addFixedEffects thinQR fePrior rtt (FixedEffects n vecF) = do
  let suffix = SB.dsSuffix rtt
      uSuffix = SB.underscoredIf suffix
  SB.add2dMatrixJson rtt "X" (Just suffix) "" (SB.NamedDim $ SB.dsName rtt) n vecF -- JSON/code declarations for matrix
  SB.fixedEffectsQR uSuffix ("X" <> uSuffix) (SB.dsName rtt) ("X_" <> SB.dsName rtt <> "_Cols") -- code for parameters and transformed parameters
  -- model
  SB.inBlock SB.SBModel $ do
    let e = SB.name ("thetaX" <> uSuffix) `SB.vectorSample` fePrior
    SB.addExprLine "addFixedEffects" e
--    SB.addStanLine $ "thetaX" <> uSuffix <> " ~ normal(0," <> show fePriorSD <> ")"
  let eQ = if T.null suffix then SB.uIndexed (SB.dsName rtt) $ SB.name "Q_ast" else SB.uIndexed (SB.dsName rtt) $ SB.name ("Q" <> uSuffix <> "_ast")
      eTheta = SB.name $ "thetaX" <> uSuffix
      eQTheta = eQ `SB.times` eTheta
      eX = if T.null suffix then SB.uIndexed (SB.dsName rtt) $ SB.name "centered_X" else SB.uIndexed (SB.dsName rtt) $ SB.name ("centered_X" <> uSuffix)
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
  checkGroupSubset "PS Spec" "All Groups" groupMaps allGroups
  rowBuilders <- SB.rowBuilders <$> get
  toFoldable <- case DHash.lookup rtt rowBuilders of
    Nothing -> SB.stanBuildError $ "RowTypeTag (" <> SB.dsName rtt <> ") not found in rowBuilders (in addPostStratification)."
    Just (SB.RowInfo tf _ _) -> return tf
  intKeyF  <- flip (maybe (return $ const $ Right 1)) mPSGroup $ \gtt -> do
    let errMsg tGrps = "Specified group for PS sum (" <> SB.taggedGroupName gtt
                       <> ") is not present in Builder groups: " <> tGrps
    SB.IndexMap _ eIntF _ <- SB.stanBuildMaybe (errMsg $ showNames allGroups) $ DHash.lookup gtt allGroups
    SB.RowMap h <- SB.stanBuildMaybe (errMsg $ showNames groupMaps) $  DHash.lookup gtt groupMaps
    SB.addIndexIntMapFld rtt gtt $ buildIntMapBuilderF eIntF h
    return $ eIntF . h
  -- add the data set for the json builders

--  SB.addUnIndexedDataSet namedPS toFoldable -- add this data set for JSON building
  SB.addJson rtt sizeName SB.StanInt "<lower=0>"
    $ SJ.valueToPairF sizeName
    $ fmap (A.toJSON . Set.size)
    $ FL.premapM intKeyF
    $ FL.generalize FL.set
  let usedGroupMaps = groupMaps `DHash.intersection` usedGroups
      ugNames = fmap (\(gtt DSum.:=> _) -> SB.taggedGroupName gtt) $ DHash.toList usedGroups
      groupBounds = fmap (\(_ DSum.:=> (SB.IndexMap (SB.IntIndex n _) _ _)) -> (1,n)) $ DHash.toList usedGroups
      groupDims = fmap SB.NamedDim ugNames
      weightArrayType = SB.StanArray [SB.NamedDim namedPS] $ SB.StanArray groupDims SB.StanReal
  let indexList :: SB.GroupRowMap r -> SB.GroupIndexDHM r0 -> Either Text (r -> Either Text [Int])
      indexList grm gim =
        let mCompose (gtt DSum.:=> SB.RowMap rTok) =
              case DHash.lookup gtt gim of
                Nothing -> Left $ "Failed lookup of group="
                           <> SB.taggedGroupName gtt
                           <> " in addPostStratification."
                Just (SB.IndexMap _ kToEitherInt _) -> Right (kToEitherInt . rTok)
            g :: [r -> Either Text Int] -> r -> Either Text [Int]
            g fs r = traverse ($r) fs
        in fmap g $ traverse mCompose $ DHash.toList grm
  indexF <- case indexList usedGroupMaps allGroups of
    Left msg -> SB.stanBuildError $ "Error producing PostStratification weight indexing function: " <> msg
    Right x -> return x
  let innerF r = case indexF r of
        Left msg -> Left $ "Error during post-stratification weight fold when applying group index function (index out of range?): " <> msg
        Right ls -> Right (ls, weightF r)
      sumInnerFold :: Ord k => FL.Fold (k, Double) [(k, Double)]
      sumInnerFold = MR.mapReduceFold MR.noUnpack (MR.Assign id) (MR.ReduceFold $ \k -> fmap (k,) FL.sum)
      assignM r = case intKeyF r of
        Left msg -> Left
                    $ "Error during post-stratification weight fold when indexing PS rows to result groups: " <> msg
        Right l -> Right (l, r)
      toIndexed x = SJ.prepareIndexed 0 groupBounds x
      reduceF = postMapM (\x -> (fmap (FL.fold sumInnerFold) $ traverse innerF x) >>= toIndexed)
                $ FL.generalize FL.list
      fldM = MR.mapReduceFoldM
             (MR.generalizeUnpack MR.noUnpack)
             (MR.AssignM assignM)
             (MR.ReduceFoldM $ const reduceF)
  SB.addJson rtt (namedPS <> "_wgts") weightArrayType ""
    $ SJ.valueToPairF (namedPS <> "_wgts")
    $ fmap A.toJSON fldM
  SB.inBlock SB.SBGeneratedQuantities $ do
    let groupCounters = fmap ("n_" <>) $ ugNames
--        indexBindings' =  Map.fromList $ zip ugNames $ fmap SB.name $ groupCounters
        innerLoopNames = case mPSGroup of
          Nothing -> ugNames -- (indexBindings', Map.keys indexBindings')
          Just (SB.GroupTypeTag gn) -> List.filter (/= gn) ugNames --(Map.insert gn (SB.name "n") indexBindings', Map.keys $ Map.delete gn indexBindings')
--        bindingStore = SB.fullyIndexedBindings indexBindings
        inner = do
          let psExpE = SB.familyExp sDist dsName args
--          expCode <- SB.printExprM "mrpPSStanCode" psExpE
          SB.stanDeclareRHS ("p" {-<> namedPS-}) SB.StanReal "" psExpE --expCode
          when (psType == PSShare)
            $ SB.addStanLine
            $ namedPS <> "_WgtSum += " <>  (namedPS <> "_wgts") <> "[n][" <> T.intercalate ", " groupCounters <> "]"
          SB.addStanLine
            $ namedPS <> "[n] += p" <> namedPS <> " * " <> (namedPS <> "_wgts") <> "[n][" <> T.intercalate ", " groupCounters <> "]"
        makeLoops [] = inner
        makeLoops (x : xs) = SB.stanForLoopB ("n_" <> x) Nothing x $ makeLoops xs
    SB.stanDeclareRHS namedPS (SB.StanVector $ SB.NamedDim sizeName) "" $ SB.function "rep_vector" [SB.scalar "0", SB.name sizeName]
--    SB.addStanLine $ namedPS <> " = rep_vector(0, " <> sizeName  <> ")"
    SB.stanForLoopB "n" Nothing sizeName $ do
      let wsn = namedPS <> "_WgtSum"
      when (psType == PSShare) $ (SB.stanDeclareRHS wsn SB.StanReal "" (SB.scalar "0") >> return ())
      makeLoops innerLoopNames
      when (psType == PSShare) $ SB.addStanLine $ namedPS <> "[n] /= " <> namedPS <> "_WgtSum"
    return $ SB.StanVar namedPS (SB.StanVector $ SB.NamedDim sizeName)

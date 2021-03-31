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
import qualified Stan.ModelConfig as SC
import qualified Stan.RScriptBuilder as SR
import qualified System.Environment as Env

import qualified Knit.Report as K
import qualified Knit.Effect.AtomicCache as K hiding (retrieveOrMake)
import Data.String.Here (here)

data MRPData f predRow modeledRow psRow =
  MRPData
  {
    modeled :: f modeledRow
  , postStratified ::Maybe (f psRow) -- if this is Nothing we don't do post-stratification
  }


buildDataWranglerAndCode :: (Typeable d, Typeable modeledRow)
                         => SB.StanGroupBuilderM modeledRow ()
                         -> Binomial_MRP_Model d modeledRow
                         -> SB.StanBuilderM (Binomial_MRP_Model d modeledRow) d modeledRow ()
                         -> d
                         -> SB.ToFoldable d modeledRow
                         -> Either Text (SC.DataWrangler d SB.GroupIntMaps (), SB.StanCode)
buildDataWranglerAndCode groupM model builderM d (SB.ToFoldable toFoldable) =
  let builderWithWrangler = do
        SB.addGroupIndexes
        _ <- builderM
        jsonRowBuilders <- SB.buildJSONF
        intMapBuilders <- SB.indexes <$> get
        return $ SC.Wrangle SC.TransientIndex $ \d -> (SB.buildIntMaps intMapBuilders d, flip SB.buildJSONFromRows jsonRowBuilders)
      resE = SB.runStanBuilder d toFoldable model groupM builderWithWrangler
  in fmap (\(SB.BuilderState _ _ _ c _, dw) -> (dw, c)) resE


runMRPModel2 :: (K.KnitEffects r
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
runMRPModel2 clearCache mWorkDir modelName dataName dataWrangler stanCode ppName resultAction data_C mNSamples mAdaptDelta mMaxTreeDepth =
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

data ProjectableRows f rowA rowB where
  ProjectableRows :: Functor f => f rowA -> (rowA -> rowB) -> ProjectableRows f rowA rowB
  ProjectedRows :: Functor f => f row -> ProjectableRows f row row

instance Functor (ProjectableRows f rowA) where
  fmap h (ProjectableRows rs g) = ProjectableRows rs (h . g)
  fmap h (ProjectedRows rs) = ProjectableRows rs h

projectableRows :: ProjectableRows f rowA rowB -> f rowA
projectableRows (ProjectableRows rs _) = rs
projectableRows (ProjectedRows rs) = rs

projectRow :: ProjectableRows f rowA rowB -> rowA -> rowB
projectRow (ProjectableRows _ g) = g
projectRow (ProjectedRows _) = id

projectRows :: ProjectableRows f rowA rowB -> ProjectableRows f rowB rowB
projectRows (ProjectableRows rs g) = ProjectedRows $ fmap g rs
projectRows (ProjectedRows rs) = ProjectedRows rs

projectedRows :: ProjectableRows f rowA rowB -> f rowB
projectedRows = projectableRows . projectRows

type MRData f modeledRows predRows = ProjectableRows f modeledRows predRows
type PSData f psRows predRows = ProjectableRows f psRows predRows
type LLData f modeledRows predRows = ProjectableRows f modeledRows predRows

data PostStratification k psRow predRow =
  PostStratification
  {
    psPrj :: psRow -> predRow
  , psWeight ::  psRow -> Double
  , psGroupKey :: psRow -> k
  }

type BuilderM modeledRow d = SB.StanBuilderM (Binomial_MRP_Model d modeledRow) d modeledRow

getModel ::  BuilderM modeledRow d (Binomial_MRP_Model d modeledRow)
getModel = SB.askUserEnv

getIndexes :: BuilderM modeledRow d (Map Text (SB.IntIndex modeledRow))
getIndexes = SB.groupIndexByName <$> SB.askGroupEnv

getIndex :: GroupName -> BuilderM modeledRow d (SB.IntIndex modeledRow)
getIndex gn = do
  indexMap <- getIndexes
  case (Map.lookup gn indexMap) of
    Nothing -> SB.stanBuildError $ "No group index found for group with name=\"" <> gn <> "\""
    Just i -> return i

-- Basic group declarations, indexes and Json are produced automatically
addMRGroup :: (Typeable d, Typeable r) => Double -> Double -> Double -> GroupName -> BuilderM r d SB.StanExpr
addMRGroup binarySD nonBinarySD sumGroupSD gn = do
  (SB.IntIndex indexSize _) <- getIndex gn
  when (indexSize < 2) $ SB.stanBuildError "Index with size <2 in MRGroup!"
  let binaryGroup = do
        let modelTerm = SB.VectorFunctionE "to_vector" $ SB.TermE $ SB.Indexed gn $ "{eps_" <> gn <> ", -eps_" <> gn <> "}"
        SB.addModelTerm modelTerm
        SB.inBlock SB.SBParameters $ SB.stanDeclare ("eps_" <> gn) SB.StanReal ""
        SB.inBlock SB.SBModel $  SB.addStanLine $ "eps_" <> gn <> " ~ normal(0, " <> show binarySD <> ")"
        return modelTerm
  let nonBinaryGroup = do
        let modelTerm = SB.TermE . SB.Indexed gn $ "beta_" <> gn
        SB.addModelTerm modelTerm
        SB.inBlock SB.SBTransformedData $ do
          SB.stanDeclare (gn <> "_weights") (SB.StanVector $ SB.NamedDim $ "N_" <> gn) "<lower=0>"
          SB.stanForLoop "g" Nothing ("N_" <> gn) $ const $ SB.addStanLine $ gn <> "_weights[g] = 0"
          SB.stanForLoop "n" Nothing "N" $ const $ SB.addStanLine $ gn <> "_weights[" <> gn <> "[n]] += 1"
          SB.addStanLine $ gn <> "_weights /= N"
        SB.inBlock SB.SBParameters $ do
          SB.stanDeclare ("sigma_" <> gn) SB.StanReal "<lower=0>"
--          SB.stanDeclare ("beta_" <> gn) (SB.StanVector $ SB.NamedDim ("N_" <> gn)) ("<multiplier=sigma_" <> gn <> ">")
          SB.stanDeclare ("beta_raw_" <> gn) (SB.StanVector $ SB.NamedDim ("N_" <> gn)) ""
        SB.inBlock SB.SBTransformedParameters $ do
          SB.stanDeclare ("beta_" <> gn) (SB.StanVector $ SB.NamedDim ("N_" <> gn)) ""
          SB.addStanLine $ "beta_" <> gn <> " = sigma_" <> gn <> " * beta_raw_" <> gn
        SB.inBlock SB.SBModel $ do
          SB.addStanLine $ "beta_raw_" <> gn <> " ~ normal(0, 1)" --" sigma_" <> gn <> ")"
          SB.addStanLine $ "sigma_" <> gn <> " ~ normal(0, " <> show nonBinarySD <> ")"
          SB.addStanLine $ "dot_product(beta_" <> gn <> ", " <> gn <> "_weights) ~ normal(0, " <> show sumGroupSD <> ")"
        return modelTerm
  if indexSize == 2 then binaryGroup else nonBinaryGroup

allMRGroups :: (Typeable d, Typeable r) => Double -> Double -> Double -> BuilderM r d ()
allMRGroups binarySD nonBinarySD sumGroupSD = do
  mrGroupNames <- bmm_MRGroups <$> getModel
  traverse_ (addMRGroup binarySD nonBinarySD sumGroupSD) mrGroupNames

type GroupName = Text

data FixedEffects row = FixedEffects Int (row -> Vec.Vector Double)

data Binomial_MRP_Model d modeledRow =
  Binomial_MRP_Model
  {
    bmm_Name :: Text -- we'll need this for (unique) file names
  , bmm_FixedEffects :: DHash.DHashMap (SB.RowTypeTag d) FixedEffects
  , bmm_MRGroups :: Set.Set GroupName
  , bmm_Total :: modeledRow -> Int
  , bmm_Success :: modeledRow -> Int
  }




emptyFixedEffects :: DHash.DHashMap (SB.RowTypeTag d) FixedEffects
emptyFixedEffects = DHash.empty
{-
addFixedEffects :: forall r d. SB.RowTypeTag d r
                -> FixedEffects r
                -> DHash.DHashMap (SB.RowTypeTag d) FixedEffects
                -> DHash.DHashMap (SB.RowTypeTag d) FixedEffects
addFixedEffects rtt fe dhm = DHash.insert rtt fe dhm
-}

feGroupNames :: forall modeledRow d. (Typeable d, Typeable modeledRow) => BuilderM modeledRow d (Set.Set GroupName)
feGroupNames = do
  feMap <- bmm_FixedEffects <$> getModel
  let feGroupMap = DHash.delete (SB.ModeledRowTag @d @modeledRow) feMap
      getName (rtt DSum.:=> _) = SB.dsName rtt
  return $ Set.fromList $ fmap getName $ DHash.toList feGroupMap

usedGroupNames :: (Typeable d, Typeable modeledRow) => BuilderM modeledRow d (Set.Set GroupName)
usedGroupNames = do
  model <- getModel
  feGNames <- feGroupNames
  return $ foldl' (\s gn -> Set.insert gn s) (bmm_MRGroups model) feGNames

checkEnv :: (Typeable d, Typeable modeledRow) => BuilderM modeledRow d ()
checkEnv = do
  model <- getModel
  allGroupNames <- Map.keys <$> getIndexes
  allFEGroupNames <- Set.toList <$> feGroupNames
  let allMRGroupNames = Set.toAscList $ bmm_MRGroups model
      hasAllFEGroups = List.isSubsequenceOf allFEGroupNames  allGroupNames
      hasAllMRGroups = List.isSubsequenceOf allMRGroupNames  allGroupNames
  when (not hasAllFEGroups) $ SB.stanBuildError $ "Missing group data! Given group data for " <> show allGroupNames <> ". FEGroups=" <> show allFEGroupNames
  when (not hasAllMRGroups) $ SB.stanBuildError $ "Missing group data! Given group data for " <> show allGroupNames <> ". MRGroups=" <> show allMRGroupNames
  return ()

--type PostStratificationWeight psRow = psRow -> Double
-- returns
-- 'X * beta' (or 'Q * theta') model term expression
-- 'X * beta' and just 'beta' for post-stratification
-- The latter is for use in post-stratification at fixed values of the fixed effects.
addFixedEffects :: forall r d r0.(Typeable d, Typeable r0)
                => Bool
                -> Double
                -> SB.RowTypeTag d r
                -> FixedEffects r
                -> BuilderM r0 d (SB.StanExpr, SB.StanExpr, SB.StanExpr)
addFixedEffects thinQR fePriorSD rtt (FixedEffects n vecF) = do
  let suffix = SB.dsSuffix rtt
      uSuffix = SB.underscoredIf suffix
  SB.add2dMatrixJson rtt "X" suffix "" (SB.NamedDim $ "N" <> uSuffix) n vecF -- JSON/code declarations for matrix
  SB.fixedEffectsQR uSuffix ("X" <> uSuffix) ("N" <> uSuffix) ("K" <> uSuffix) -- code for parameters and transformed parameters
  -- model
  SB.inBlock SB.SBModel $ SB.addStanLine $ "thetaX" <> uSuffix <> " ~ normal(0," <> show fePriorSD <> ")"
  let eQ = SB.TermE $ if T.null suffix then SB.Vectored "Q_ast" else SB.Indexed suffix ("Q" <> uSuffix <> "_ast") --[" <> suffix <> "]"
      eTheta = SB.TermE $ SB.Scalar $ "thetaX" <> uSuffix
      eQTheta = SB.BinOpE "*" eQ eTheta
      eX = SB.TermE $ if T.null suffix then SB.Vectored "centered_X" else SB.Indexed suffix ("centered_X" <> uSuffix) -- <> "[" <> suffix <> "]"
      eBeta = SB.TermE $ SB.Scalar $ "betaX" <> uSuffix
      eXBeta = SB.BinOpE "*" eX eBeta
      feExpr = if thinQR then eQTheta else eXBeta
  SB.addModelTerm feExpr
  return (feExpr, eXBeta, eBeta)

{-
allFixedEffects :: forall r d. (Typeable d, Typeable r) => Bool -> Double -> BuilderM r d ()
allFixedEffects thinQR fePriorSD = do
  model <- getModel
  let f :: (Typeable d, Typeable r) => DSum.DSum (SB.RowTypeTag d) (FixedEffects) -> BuilderM r d ()
      f (rtt DHash.:=> fe) = (addFixedEffects thinQR fePriorSD) rtt fe
  traverse_ f $ DHash.toList $ bmm_FixedEffects model
-}

intercept :: forall r d. (Typeable d, Typeable r) => Text -> Double -> BuilderM r d SB.StanExpr
intercept iName alphaPriorSD = do
  SB.inBlock SB.SBParameters $ SB.stanDeclare iName SB.StanReal ""
  SB.inBlock SB.SBModel $ SB.addStanLine $ iName <> " ~ normal(0, " <> show alphaPriorSD <> ")"
  let modelTerm = SB.TermE $ SB.Scalar "alpha"
  SB.addModelTerm modelTerm
  return modelTerm

buildIntMapBuilderF :: (k -> Either Text Int) -> (r -> k) -> FL.FoldM (Either Text) r (IM.IntMap k)
buildIntMapBuilderF eIntF keyF = FL.FoldM step (return IM.empty) return where
  step im r = case eIntF $ keyF r of
    Left msg -> Left $ "Indexing error when trying to build IntMap index: " <> msg
    Right n -> Right $ IM.insert n (keyF r) im

data PostStratificationType = PSRaw | PSShare deriving (Eq, Show)

-- TODO: order groups differently than the order coming from the built in group sets??
addPostStratification :: (Typeable d, Typeable r0, Typeable r, Typeable k)
                      => SB.StanDist args
                      -> args
                      -> SB.StanName
                      -> SB.ToFoldable d r
                      -> SB.GroupRowMap r
                      -> (r -> Double)
                      -> PostStratificationType
                      -> (Maybe (SB.GroupTypeTag k))
                      -> BuilderM r0 d ()
addPostStratification sDist args name toFoldable@(SB.ToFoldable rowsF) groupMaps weightF psType mPSGroup = do
  -- check that all model groups in environment are accounted for in PS groups
  let showNames = T.intercalate "," . fmap (\(gtt DSum.:=> _) -> SB.taggedGroupName gtt) . DHash.toList
  allGroups <- SB.groupIndexByType <$> SB.askGroupEnv
  usedGNames <- usedGroupNames
  let usedGroups = DHash.filterWithKey (\(SB.GroupTypeTag n) _ -> n `Set.member` usedGNames) $ allGroups
      checkGroupSubset n1 n2 gs1 gs2 = do
        let gDiff = DHash.difference gs1 gs2
        when (DHash.size gDiff /= 0)
          $ SB.stanBuildError
          $ n1 <> "(" <> showNames gs1 <> ") is not a subset of "
          <> n2 <> "(" <> showNames gs2 <> ")."
          <> "In " <> n1 <> " but not in " <> n2 <> ": " <> showNames gDiff <> "."
          <> " If this error appears entirely mysterious, try checking the *types* of your group key functions."
  checkGroupSubset "Modeling" "PS Spec" usedGroups groupMaps
  checkGroupSubset "PS Spec" "All Groups" groupMaps allGroups
  intKeyF  <- flip (maybe (return $ const $ Right 1)) mPSGroup $ \gtt -> do
    let errMsg tGrps = "Specified group for PS sum (" <> SB.taggedGroupName gtt
                       <> ") is not present in Builder groups: " <> tGrps
    SB.IndexMap _ eIntF <- SB.stanBuildMaybe (errMsg $ showNames allGroups) $ DHash.lookup gtt allGroups
    SB.RowMap h <- SB.stanBuildMaybe (errMsg $ showNames groupMaps) $  DHash.lookup gtt groupMaps
    SB.addIndexIntMap name (FL.foldM (buildIntMapBuilderF eIntF h) . rowsF)
    return $ eIntF . h
  -- add the data set for the json builders
  let namedPS = "PS_" <> name
  SB.addUnIndexedDataSet namedPS toFoldable
  -- "int<lower=0> N_PS_xxx;" number of post-stratified results
  SB.addJson (SB.RowTypeTag namedPS) ("N_" <> namedPS) SB.StanInt "<lower=0>"
    $ SJ.valueToPairF ("N_" <> namedPS)
    $ fmap (A.toJSON . Set.size)
    $ FL.premapM intKeyF
    $ FL.generalize FL.set
  let usedGroupMaps = groupMaps `DHash.intersection` usedGroups
      ugNames = fmap (\(gtt DSum.:=> _) -> SB.taggedGroupName gtt) $ DHash.toList usedGroups
      groupBounds = fmap (\(_ DSum.:=> (SB.IndexMap (SB.IntIndex n _) _)) -> (1,n)) $ DHash.toList usedGroups
      groupDims = fmap (\gn -> SB.NamedDim $ "N_" <> gn) ugNames
      weightArrayType = SB.StanArray [SB.NamedDim $ "N_" <> namedPS] $ SB.StanArray groupDims SB.StanReal
  let indexList :: SB.GroupRowMap r -> SB.GroupIndexDHM r0 -> Either Text (r -> Either Text [Int]) --[r -> Maybe Int]
      indexList grm gim =
        let mCompose (gtt DSum.:=> SB.RowMap rTok) =
              case DHash.lookup gtt gim of
                Nothing -> Left $ "Failed lookup of group=" <> SB.taggedGroupName gtt <> " in addPostStratification."
                Just (SB.IndexMap _ kToEitherInt) -> Right (kToEitherInt . rTok)
            g :: [r -> Either Text Int] -> r -> Either Text [Int]
            g fs r = traverse ($r) fs
        in fmap g $ traverse mCompose $ DHash.toList grm
  indexF <- case indexList usedGroupMaps allGroups of
    Left msg -> SB.stanBuildError $ "Error producing PostStratification weight indexing function: " <> msg
    Right x -> return x
  -- "real[N_G1, N_G2, ...] PS_xxx_wgt[N_PS_xxx];"  weight matrices for each PS result
  let innerF r = case indexF r of
        Left msg -> Left $ "Error during post-stratification weight fold when applying group index function (index out of range?): " <> msg
        Right ls -> Right (ls, weightF r)
      sumInnerFold :: Ord k => FL.Fold (k, Double) [(k, Double)]
      sumInnerFold = MR.mapReduceFold MR.noUnpack (MR.Assign id) (MR.ReduceFold $ \k -> fmap (k,) FL.sum)
      assignM r = case intKeyF r of
        Left msg -> Left $ "Error during post-stratification weight fold when indexing PS rows to result groups: " <> msg
        Right l -> Right (l, r)
      toIndexed x = SJ.prepareIndexed 0 groupBounds x
      reduceF = postMapM (\x -> (fmap (FL.fold sumInnerFold) $ traverse innerF x) >>= toIndexed) $ FL.generalize FL.list
      fldM = MR.mapReduceFoldM
             (MR.generalizeUnpack MR.noUnpack)
             (MR.AssignM assignM)
             (MR.ReduceFoldM $ const reduceF)
  SB.addJson (SB.RowTypeTag namedPS) (namedPS <> "_wgts") weightArrayType ""
    $ SJ.valueToPairF (namedPS <> "_wgts")
    $ fmap A.toJSON fldM
  SB.inBlock SB.SBGeneratedQuantities $ do
    let groupCounters = fmap ("n_" <>) $ ugNames
        im = Map.fromList $ zip ugNames groupCounters
        inner = do
          let psExpE = SB.familyExp sDist args
          expCode <- SB.printExprM "mrpPSStanCode" im (SB.NonVectorized "n") $ return psExpE
          SB.stanDeclareRHS "p" SB.StanReal "<lower=0, upper=1>" expCode
--          SB.addStanLine $ "real p = inv_logit(" <> modelTerms <> ")"
          when (psType == PSShare) $ SB.addStanLine $ namedPS <> "_WgtSum += " <>  (namedPS <> "_wgts") <> "[n][" <> T.intercalate "," groupCounters <> "]"
          SB.addStanLine $ namedPS <> "[n] += p * " <> (namedPS <> "_wgts") <> "[n][" <> T.intercalate "," groupCounters <> "]"
        makeLoops [] = inner
        makeLoops (x : xs) = SB.stanForLoop ("n_" <> x) Nothing ("N_" <> x) $ const $ makeLoops xs
    SB.stanDeclare namedPS (SB.StanArray [SB.NamedDim $ "N_" <> namedPS] SB.StanReal) ""
    SB.addStanLine $ namedPS <> " = rep_array(0, N_" <> namedPS <> ")"
    SB.stanForLoop "n" Nothing ("N_" <> namedPS) $ const $ do
      let wsn = namedPS <> "_WgtSum"
      when (psType == PSShare) $ SB.stanDeclareRHS wsn  SB.StanReal "<lower=0>" "0" --SB.addStanLine $ "real " <> namedPS <> "_WgtSum = 0"
      makeLoops ugNames
      when (psType == PSShare) $ SB.addStanLine $ wsn <> "[n] /= " <> namedPS <> "_WgtSum"

usedIndexes :: (Typeable d, Typeable modeledRow) => BuilderM modeledRow d (Map GroupName (SB.IntIndex modeledRow))
usedIndexes = do
  indexMap <- getIndexes -- SB.asksEnv sbe_groupIndices
  groupNames <- usedGroupNames
  return $ Map.restrictKeys indexMap groupNames

mrIndexes :: BuilderM modeledRow d (Map GroupName (SB.IntIndex modeledRow))
mrIndexes = do
  indexMap <- getIndexes --SB.asksEnv sbe_groupIndices
  groupNames <- bmm_MRGroups <$> getModel
  return $ Map.restrictKeys indexMap groupNames

dataBlockM :: (Typeable d, Typeable modeledRow) => BuilderM modeledRow d ()
dataBlockM = do
  model <- getModel
  SB.inBlock SB.SBData $ do
    SB.addColumnJson SB.ModeledRowTag "T" "" (SB.StanArray [SB.NamedDim "N"] SB.StanInt) "<lower=1>" (bmm_Total model)
    SB.addColumnJson SB.ModeledRowTag "S" "" (SB.StanArray [SB.NamedDim "N"] SB.StanInt) "<lower=0>" (bmm_Success model)
    return ()

mrpMainModelTerm :: BuilderM modeledRow d ()
mrpMainModelTerm = SB.inBlock SB.SBModel $ do
  indexMap <- getIndexes
  let im = Map.mapWithKey const indexMap
  modelTerms <- SB.printExprM "mrpModelBlock" im SB.Vectorized SB.getModelExpr
  SB.addStanLine $ "S ~ binomial_logit(T, " <> modelTerms <> ")"

mrpLogLikelihood :: BuilderM modeledRow d ()
mrpLogLikelihood = SB.inBlock SB.SBGeneratedQuantities $ do
  model <- getModel
  indexMap <- getIndexes --SB.asksEnv sbe_groupIndices
  SB.addStanLine $ "vector [N] log_lik"
  SB.stanForLoop "n" Nothing "N" $ \_ -> do
    let im = Map.mapWithKey (\k _ -> k <> "[n]") indexMap -- we need to index the groups.
    modelTerms <- SB.printExprM "mrpLogLikStanCode" im (SB.NonVectorized "n") $ SB.getModelExpr
    SB.addStanLine $ "log_lik[n] = binomial_logit_lpmf(S[n] | T[n], " <> modelTerms <> ")"

mrpPosteriorPrediction :: BuilderM modeledRow d ()
mrpPosteriorPrediction  = do
  SB.inBlock SB.SBGeneratedQuantities $ do
    indexMap <- getIndexes
    let im = Map.mapWithKey const indexMap
    model_terms <- SB.printExprM "SPred (Generated Quantities)" im SB.Vectorized SB.getModelExpr
--    SB.addStanLine $ "vector<lower=0, upper=1>[N] yPred = inv_logit(" <> model_terms <> ")"
    SB.addStanLine $ "vector<lower=0>[N] SPred =  to_vector(T) .* inv_logit(" <> model_terms <> ")"
--    SB.stanForLoop "n" Nothing "N" $ const $ SB.addStanLine "SPred[n] = yPred[n] * T[n]"

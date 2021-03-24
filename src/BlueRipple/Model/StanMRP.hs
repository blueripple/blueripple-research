{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
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
                         -> SB.ToFoldable d modeledRow --MRData modeledRow predRow
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
               , Flat.Flat b
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
            -> K.Sem r ()
runMRPModel2 clearCache mWorkDir modelName dataName dataWrangler stanCode ppName resultAction data_C mNSamples mAdaptDelta =
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
  res_C <- BR.retrieveOrMakeD resultCacheKey dataModelDep $ \() -> do
    K.logLE K.Info "Data or model newer then last cached result. (Re)-running..."
    SM.runModel @BR.SerializerC @BR.CacheData
      stanConfig
      (SM.Both unwraps)
      dataWrangler
      SC.UnCacheable -- we cannot use a Cacheable index here
      resultAction
      ()
      data_C
  return ()

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
mrGroup :: (Typeable d, Typeable r) => Double -> Double -> Double -> GroupName -> BuilderM r d ()
mrGroup binarySD nonBinarySD sumGroupSD gn = do
  (SB.IntIndex indexSize _) <- getIndex gn
  when (indexSize < 2) $ SB.stanBuildError "Index with size <2 in MRGroup!"
  let binaryGroup = do
        SB.addModelTerm $ SB.VectorFunctionE "to_vector" $ SB.TermE $ SB.Indexed gn $ "{eps_" <> gn <> ", -eps_" <> gn <> "}"
        SB.inBlock SB.SBParameters $ SB.stanDeclare ("eps_" <> gn) SB.StanReal ""
        SB.inBlock SB.SBModel $  SB.addStanLine $ "eps_" <> gn <> " ~ normal(0, " <> show binarySD <> ")"
  let nonBinaryGroup = do
        SB.addModelTerm $ SB.TermE . SB.Indexed gn $ "beta_" <> gn
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
  if indexSize == 2 then binaryGroup else nonBinaryGroup

allMRGroups :: (Typeable d, Typeable r) => Double -> Double -> Double -> BuilderM r d ()
allMRGroups binarySD nonBinarySD sumGroupSD = do
  mrGroupNames <- bmm_MRGroups <$> getModel
  traverse_ (mrGroup binarySD nonBinarySD sumGroupSD) mrGroupNames

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

addFixedEffects :: forall r d. SB.RowTypeTag d r
                -> FixedEffects r
                -> DHash.DHashMap (SB.RowTypeTag d) FixedEffects
                -> DHash.DHashMap (SB.RowTypeTag d) FixedEffects
addFixedEffects rtt fe dhm = DHash.insert rtt fe dhm

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

fixedEffects :: forall d r r0.(Typeable d, Typeable r0)
              => Bool -> Double -> SB.RowTypeTag d r -> FixedEffects r -> BuilderM r0 d ()
fixedEffects thinQR fePriorSD rtt (FixedEffects n vecF) = do
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

allFixedEffects :: forall r d. (Typeable d, Typeable r) => Bool -> Double -> BuilderM r d ()
allFixedEffects thinQR fePriorSD = do
  model <- getModel
  let f :: (Typeable d, Typeable r) => DSum.DSum (SB.RowTypeTag d) (FixedEffects) -> BuilderM r d ()
      f (rtt DHash.:=> fe) = (fixedEffects thinQR fePriorSD) rtt fe
  traverse_ f $ DHash.toList $ bmm_FixedEffects model

intercept :: forall r d. (Typeable d, Typeable r) => Text -> Double -> BuilderM r d ()
intercept iName alphaPriorSD = do
  SB.inBlock SB.SBParameters $ SB.stanDeclare iName SB.StanReal ""
  SB.inBlock SB.SBModel $ SB.addStanLine $ iName <> " ~ normal(0, " <> show alphaPriorSD <> ")"
  SB.addModelTerm $ SB.TermE $ SB.Scalar "alpha"

-- TODO: order groups differently than the order coming from the built in group sets??
addPostStratification :: (Typeable d, Typeable r0, Typeable r)
                      => SB.StanName
                      -> SB.ToFoldable d r
                      -> SB.GroupRowMap r
                      -> (r -> Double)
                      -> (Maybe (SB.GroupTypeTag k))
                      -> BuilderM r0 d ()
addPostStratification name toFoldable groupMaps weightF mPSGroup = do
  -- check that all model groups in environment are accounted for in PS groups
  let showNames = T.intercalate "," . fmap (\(gtt DSum.:=> _) -> SB.taggedGroupName gtt) . DHash.toList
  allGroups <- SB.groupIndexByType <$> SB.askGroupEnv
  usedGNames <- usedGroupNames
  let usedGroups = DHash.filterWithKey (\(SB.GroupTypeTag n) _ -> n `Set.member` usedGNames) $ allGroups
  when (DHash.size (DHash.difference usedGroups groupMaps) /= 0)
    $ SB.stanBuildError
    $ "A group used for modeling is missing from post-stratification specification!"
    <> "Modeling groups: " <> showNames usedGroups
    <> "PS Groups: " <> showNames groupMaps
  when (DHash.size (DHash.difference groupMaps allGroups) /= 0)
    $ SB.stanBuildError
    $ "A group(s) in the PS Groups is not present in the set of groups known to the model builder."
    <> "Unknown=" <> showNames (DHash.difference groupMaps allGroups) <> "; "
    <> "PS Groups=" <> showNames groupMaps <> "; "
    <> "Model Builder groups=" <> showNames allGroups
    <> "If this error appears entirely mysterious, try checking the types of your group key functions."
  -- keyF :: r -> Maybe Int
  keyF <- case mPSGroup of
            Nothing -> return $ const $ Just 1
            Just gtt -> case DHash.lookup gtt allGroups of
              Nothing -> SB.stanBuildError
                $ "Specified group for PS sum (" <> SB.taggedGroupName gtt
                <> ") is not present in Builder groups: " <> showNames allGroups
              Just (SB.IndexMap _ mIntF) -> case DHash.lookup gtt groupMaps of
                Nothing -> SB.stanBuildError
                  $ "Specified group for PS sum (" <> SB.taggedGroupName gtt
                  <> " is not in PS groups: "  <> showNames groupMaps
                Just (SB.RowMap h) -> return $ mIntF . h
  -- add the data set
  let namedPS = "PS_" <> name
  SB.addUnIndexedDataSet namedPS toFoldable
  -- "int<lower=0> N_PS_xxx;" number of post-stratified results
  SB.addJson (SB.RowTypeTag namedPS) ("N_" <> namedPS) SB.StanInt "<lower=0>"
    $ SJ.valueToPairF ("N_" <> namedPS)
    $ fmap (A.toJSON . Set.size)
    $ FL.premapM (maybe (Left "PS length fold failed due to indexing issue in r -> Maybe Int") Right . keyF)
    $ FL.generalize FL.set
  let usedGroupMaps = groupMaps `DHash.intersection` usedGroups
      ugNames = fmap (\(gtt DSum.:=> _) -> SB.taggedGroupName gtt) $ DHash.toList usedGroups
      groupBounds = fmap (\(_ DSum.:=> (SB.IndexMap (SB.IntIndex n _) _)) -> (1,n)) $ DHash.toList usedGroups
      groupDims = fmap (\gn -> SB.NamedDim $ "N_" <> gn) ugNames
--      weightArrayType = SB.StanArray ((SB.NamedDim $ "N_" <> namedPS) : groupDims) SB.StanReal -- (SB.StanArray [SB.NamedDim $ "N_" <> namedPS] SB.StanReal)
      weightArrayType = SB.StanArray [SB.NamedDim $ "N_" <> namedPS] $ SB.StanArray groupDims SB.StanReal
  let indexList :: SB.GroupRowMap r -> SB.GroupIndexDHM r0 -> Maybe (r -> Maybe [Int]) --[r -> Maybe Int]
      indexList grm gim =
        let mCompose (gtt DSum.:=> SB.RowMap rTok) =
              case DHash.lookup gtt gim of
                Nothing -> Nothing
                Just (SB.IndexMap _ kToMaybeInt) -> Just (kToMaybeInt . rTok)
            g :: [r -> Maybe Int] -> r -> Maybe [Int]
            g fs r = traverse ($r) fs
        in fmap g $ traverse mCompose $ DHash.toList grm
  indexF <- case indexList usedGroupMaps allGroups of
    Nothing -> SB.stanBuildError "Error producing PostStratification weight indexing function."
    Just x -> return x
  -- "real[N_G1, N_G2, ...] PS_xxx_wgt[N_PS_xxx];"  weight matrices for each PS result
  let innerF r = case indexF r of
        Nothing -> Left "Error during post-stratification weight fold when applying group index function. index out of range?"
        Just ls -> Right (ls, weightF r)
      assignM r = case keyF r of
        Nothing -> Left "Error during post-stratification weight fold when indexing PS rows to result groups."
        Just l -> Right (l, r)
      toIndexed x = SJ.prepareIndexed 0 groupBounds x
      reduceF = postMapM (\x -> traverse innerF x >>= toIndexed) $ FL.generalize FL.list
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
          modelTerms <- SB.printExprM "mrpPSStanCode" im (SB.NonVectorized "n") $ SB.getModelExpr
          SB.addStanLine $ "real  p = inv_logit(" <> modelTerms <> ")"
          SB.addStanLine $ namedPS <> "[n] += p * " <> (namedPS <> "_wgts") <> "[n][" <> T.intercalate "," groupCounters <> "]"
        makeLoops [] = inner
        makeLoops (x : xs) = SB.stanForLoop ("n_" <> x) Nothing ("N_" <> x) $ const $ makeLoops xs
    SB.stanDeclare namedPS (SB.StanArray [SB.NamedDim $ "N_" <> namedPS] SB.StanReal) ""
    SB.addStanLine $ namedPS <> " = rep_array(0, N_" <> namedPS <> ")"
    SB.stanForLoop "n" Nothing ("N_" <> namedPS) $ const $ makeLoops ugNames





{-
psRowsFld' :: Ord k
          => PostStratification k psRow predRow
          -> MRPBuilderM f predRow modeledRow psRow (FL.FoldM (Either Text) psRow [(k, (Vec.Vector Double, SJ.Indexed Double))])
psRowsFld' (PostStratification prj wgt key) = do
  model <- getModel
  toIndexedFld <- FL.premapM (return . snd) <$> predRowsToIndexed
  let fixedEffectsFld = postMapM (maybe (Left "Empty group in psRowsFld?") Right)
                        $ FL.generalize
                        $ FL.premap fst FL.last
      innerFld = (,) <$> fixedEffectsFld <*> toIndexedFld
      h pr = case bmm_FixedEffects model of
        Nothing -> Vec.empty
        Just (FixedEffects _ f) -> f pr
  return
    $ MR.mapReduceFoldM
    (MR.generalizeUnpack MR.noUnpack)
    (MR.generalizeAssign $ MR.assign key $ \psRow -> let pr = prj psRow in (h pr, (pr, wgt psRow)))
    (MR.foldAndLabelM innerFld (,))

psRowsFld :: Ord k
          => PostStratification k psRow predRow
          -> MRPBuilderM f predRow modeledRow psRow (FL.FoldM (Either Text) psRow [(k, Int, Vec.Vector Double, SJ.Indexed Double)])
psRowsFld ps = do
  fld' <- psRowsFld' ps
  let f (n, (k, (v, i))) = (k, n, v, i)
      g  = fmap f . zip [1..]
  return $ postMapM (return . g) fld'

psRowsJSONFld :: Text -> MRPBuilderM f modeledRow predRow psRow (SJ.StanJSONF (k, Int, Vec.Vector Double, SJ.Indexed Double) A.Series)
psRowsJSONFld psSuffix = do
  hasRowLevelFE <- isJust . bmm_FixedEffects <$> getModel
  let labeled x = x <> psSuffix
  return $ SJ.namedF (labeled "N") FL.length
    <> (if hasRowLevelFE then SJ.valueToPairF (labeled "X") (SJ.jsonArrayF $ \(_, _, v, _) -> v) else mempty)
    <> SJ.valueToPairF (labeled "W") (SJ.jsonArrayF $ \(_, _, _, ix) -> ix)

mrPSKeyMapFld ::  Ord k
           => PostStratification k psRow predRow
           -> FL.Fold psRow (IM.IntMap k)
mrPSKeyMapFld ps = fmap (IM.fromList . zip [1..] . sort . nubOrd . fmap (psGroupKey ps)) FL.list

mrPSDataJSONFold :: Ord k
                 => PostStratification k psRow predRow
                 -> Text
                 -> MRPBuilderM f predRow modeledRow psRow (SJ.StanJSONF psRow A.Series)
mrPSDataJSONFold psFuncs psSuffix = do
  psRowsJSONFld' <- psRowsJSONFld psSuffix
  psDataF <- psRowsFld psFuncs
  return $ postMapM (FL.foldM psRowsJSONFld') psDataF
-}

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

mrpMainModelTerm :: BuilderM modeledRow d ()
mrpMainModelTerm = SB.inBlock SB.SBModel $ do
  indexMap <- getIndexes
  let im = Map.mapWithKey const indexMap
  modelTerms <- SB.printExprM "mrpModelBlock" im SB.Vectorized SB.getModelExpr
  SB.addStanLine $ "S ~ binomial_logit(T, " <> modelTerms <> ")"

mrpLogLikStanCode :: BuilderM modeledRow d ()
mrpLogLikStanCode = SB.inBlock SB.SBGeneratedQuantities $ do
  model <- getModel
  indexMap <- getIndexes --SB.asksEnv sbe_groupIndices
  SB.addStanLine $ "vector [N] log_lik"
  SB.stanForLoop "n" Nothing "N" $ \_ -> do
    let im = Map.mapWithKey (\k _ -> k <> "[n]") indexMap -- we need to index the groups.
    modelTerms <- SB.printExprM "mrpLogLikStanCode" im (SB.NonVectorized "n") $ SB.getModelExpr
    SB.addStanLine $ "log_lik[n] = binomial_logit_lpmf(S[n] | T[n], " <> modelTerms <> ")"

{-
mrpPSStanCode :: forall f predRow modeledRow psRow.
                 Bool
              -> MRPBuilderM f predRow modeledRow psRow ()
mrpPSStanCode doPS = SB.inBlock SB.SBGeneratedQuantities $ do
  model <- getModel
  groupNames <- fmap fst <$> groupOrderedIntIndexes
  if doPS then (do
                   let groupCounters = fmap ("n_" <>) $ groupNames
                       im = Map.fromList $ zip groupNames groupCounters
                       inner = do
                         modelTerms <- SB.printExprM "mrpPSStanCode" im (SB.NonVectorized "n") $ modelExpr False "ps"
                         SB.addStanLine $ "real p<lower=0, upper=1> = inv_logit(" <> modelTerms <> ")"
                         SB.addStanLine $ "ps[n] += p * Wps[n][" <> T.intercalate "," groupCounters <> "]"
                       makeLoops :: [Text] -> MRPBuilderM f predRow modeledRow psRow ()
                       makeLoops []  = inner
                       makeLoops (x : xs) = SB.stanForLoop ("n_" <> x) Nothing ("J_" <> x) $ const $ makeLoops xs
                   SB.addStanLine $ "vector [Nps] ps"
                   SB.stanForLoop "n" Nothing "Nps" $ const $ makeLoops groupNames
               )
    else return ()
-}

mrpGeneratedQuantitiesBlock :: Bool
                            -> BuilderM modeledRow d ()
mrpGeneratedQuantitiesBlock doPS = do
  SB.inBlock SB.SBGeneratedQuantities $ do
    indexMap <- getIndexes
    let im = Map.mapWithKey const indexMap
    model_terms <- SB.printExprM "yPred (Generated Quantities)" im SB.Vectorized SB.getModelExpr
    SB.addStanLine $ "vector<lower=0, upper=1>[N] yPred = inv_logit(" <> model_terms <> ")"
    SB.addStanLine $ "real<lower=0> SPred[N]" --" = yPred * to_vector(T)"
    SB.stanForLoop "n" Nothing "N" $ const $ SB.addStanLine "SPred[n] = yPred[n] * T[n]"
  mrpLogLikStanCode

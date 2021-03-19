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
                         -> Either Text (SC.DataWrangler d () (), SB.StanCode)
buildDataWranglerAndCode groupM model builderM d (SB.ToFoldable toFoldable) =
  let builderWithWrangler = do
        _ <- builderM
        jsonRowBuilders <- SB.buildJSONF
        return $ SC.Wrangle SC.TransientIndex $ \d -> ((), flip SB.buildJSONFromRows jsonRowBuilders)
      resE = SB.runStanBuilder d toFoldable model groupM builderWithWrangler
  in fmap (\(SB.BuilderState _ _ c, dw) -> (dw, c)) resE


runMRPModel2 :: (K.KnitEffects r
               , BR.CacheEffects r
               , Foldable f
               , Functor f
               , Ord k
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
            -> K.Sem r ()
runMRPModel2 clearCache mWorkDir modelName dataName dataWrangler stanCode ppName resultAction data_C mNSamples =
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

-- basic group in modeled data. E.g., "J_age"
-- JSON for group indices are produced automatically
groupM :: Typeable d => GroupName -> BuilderM modeledRow d ()
groupM gn = do
  (SB.IntIndex indexSize _) <- getIndex gn
  SB.addFixedIntJson ("N_" <> gn) indexSize -- JSON: {J_Age : 2}
  SB.inBlock SB.SBData $ do
    SB.addStanLine $ "int<lower=2> N_" <> gn -- Code: int<lower=2> J_Age;
    SB.addStanLine $ "int<lower=1, upper=N_" <> gn <> "> " <> gn; -- Code: int<lower=1, upper=J_Age> Age;

allGroupsM :: (Typeable d, Typeable modeledRow) => BuilderM modeledRow d ()
allGroupsM = usedGroupNames >>= traverse_ groupM . Set.toList

{-
groupSizeJSONFold :: Text -> GroupName -> MRPBuilderM f modeledRow psRow (SJ.StanJSONF modeledRow A.Series)
groupSizeJSONFold prefix gn = do
  SB.IntIndex indexSize indexM <- getIndex gn
  return $ SJ.constDataF (prefix <> gn) indexSize


groupDataJSONFold :: Text -> GroupName -> MRPBuilderM f predRow modeledRow psRow (SJ.StanJSONF predRow A.Series)
groupDataJSONFold suffix gn = do
  SB.IntIndex indexSize indexM <- getIndex gn
  return $ SJ.valueToPairF (gn <> SB.underscoredIf suffix) (SJ.jsonArrayMF indexM)

groupsJSONFold :: Traversable f
               => (Text -> GroupName -> MRPBuilderM g predRow modeledRow psRow (SJ.StanJSONF predRow A.Series))
               -> Text
               -> f Text
               -> MRPBuilderM g predRow modeledRow psRow (SJ.StanJSONF predRow A.Series)
groupsJSONFold groupFold t =  fmap (foldMap id) . traverse (groupFold t)

groupsDataJSONFold :: Traversable f => Text -> f GroupName -> MRPBuilderM g predRow modeledRow psRow (SJ.StanJSONF predRow A.Series)
groupsDataJSONFold = groupsJSONFold groupDataJSONFold

groupsSizeFold :: Traversable f => f GroupName -> MRPBuilderM g predRow modeledRow psRow (SJ.StanJSONF predRow A.Series)
groupsSizeFold = groupsJSONFold groupSizeJSONFold "J_"
-}

type GroupName = Text

data FixedEffects row = FixedEffects Int (row -> Vec.Vector Double)

data Binomial_MRP_Model d modeledRow =
  Binomial_MRP_Model
  {
    bmm_Name :: Text -- we'll need this for (unique) file names
--  , bmm_FixedEffects :: Maybe (FixedEffects predRow) -- in case there are row-level fixed effects
  , bmm_FixedEffects :: DHash.DHashMap (SB.RowTypeTag d) FixedEffects
  , bmm_MRGroups :: Set.Set GroupName
--  , bmm_PrjPred :: modeledRow -> predRow
  , bmm_Total :: modeledRow -> Int
  , bmm_Success :: modeledRow -> Int
  }
{-
buildEnv :: Foldable f => [Group predRow] -> Binomial_MRP_Model predRow modeledRow -> MRData f modeledRow predRow  -> MRPBuilderEnv predRow modeledRow
buildEnv groups model modelDat = StanBuilderEnv groupIndexMap model
  where
    groupIndexMap = Map.fromList $ fmap (\g -> (groupName g, groupIndex g (projectedRows modelDat))) groups
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

type PostStratificationWeight psRow = psRow -> Double
{-}
-- The returned fold produces the "J_<GroupName>" data
mrGroupJSONFold :: MRData g modeledRow predRow
                -> MRPBuilderM g predRow modeledRow psRow (SJ.StanJSONF modeledRow A.Series)
mrGroupJSONFold modelDat = do
  groupNames <- Set.toList <$> usedGroupNames
  fGS <- groupsSizeFold groupNames
  return $ FL.premapM (return . projectRow modelDat) fGS
-}
fixedEffectsM :: forall d r modeledRow.(Typeable d)
              => SB.RowTypeTag d r -> FixedEffects r -> BuilderM modeledRow d ()
fixedEffectsM rtt (FixedEffects n vecF) = do
  let suffix = SB.underscoredIf $ SB.dsSuffix rtt
  SB.addFixedIntJson ("K" <> suffix) n -- JSON for K
  SB.add2dMatrixJson  rtt "X" suffix n vecF -- JSON for matrix
  SB.fixedEffectsQR suffix ("X" <> suffix) ("N" <> suffix) ("K" <> suffix) -- code for data, parameters and transformed parameters

allFixedEffectsM :: forall modeledRow d. Typeable d => BuilderM modeledRow d ()
allFixedEffectsM = do
  model <- getModel
  let f :: Typeable d => DSum.DSum (SB.RowTypeTag d) (FixedEffects) -> BuilderM modeledRow d ()
      f (rtt DHash.:=> fe) = fixedEffectsM rtt fe
  traverse_ f $ DHash.toList $ bmm_FixedEffects model

{-}
-- This fold produces:
-- the length of the projectable data
-- the group indexes for this data
predDataJSONFold :: Text
                 -> ProjectableRows f modeledRow predRow
                 -> MRPBuilderM f predRow modeledRow psRow (SJ.StanJSONF modeledRow A.Series)
predDataJSONFold label rows = do
  model <- getModel
  groupNames <- Set.toList <$> usedGroupNames
  fGD <- groupsDataJSONFold label groupNames
  let labeled t = t <> label
  return
    $ SJ.namedF (labeled "N") FL.length
--    <> (if (bmm_nFixedEffects model > 0) then SJ.valueToPairF (labeled "X") (SJ.jsonArrayF (bmm_FixedEffects model . projectRow rows)) else mempty)
    <> FL.premapM (return . projectRow rows) fGD

-- for one fixed effect level (all or group), produce K (columns) and the matrix of predictors
mrFixedEffectFold :: Maybe Text
                  -> ProjectableRows f modeledRow predRow
                  -> Maybe GroupName
                  -> FixedEffects predRow
                  -> MRPBuilderM f predRow modeledRow psRow (SJ.StanJSONF modeledRow A.Series)
mrFixedEffectFold mLabel rows mGN (FixedEffects n vecF) = do
  case mGN of
    Just gn -> do
      (SB.IntIndex _ mIntF) <- getIndex gn
      let h =  MR.postMapM (maybe (Left "Foldl.last returned Nothing") Right) . FL.generalize
          labeled t = maybe (t <> "_" <> gn) (\l -> t <> "_" <> l <> "_" <> gn) mLabel
          indexedDataToJSONSeries = (labeled "X" A..=) . A.toJSON . Vec.fromList . fmap snd . List.sortOn fst
          mFld =
            MR.mapReduceFoldM
            (MR.UnpackM $ \x -> maybe (Left "Missing group index when constructing a FixedEffect matrix fold") (\n -> Right [(n, x)]) $ mIntF x)
            (MR.generalizeAssign $ MR.Assign id)
            (MR.ReduceFoldM $ \k -> fmap (\d -> (k, vecF d)) $ h FL.last)
      return $ if n > 0
               then SJ.constDataF (labeled "K") n <> (fmap indexedDataToJSONSeries $ FL.premapM (return . projectRow rows) mFld)
               else mempty
    Nothing -> do
      let labeled t = maybe t (\l -> t <> "_" <> l) mLabel
      return $ if n > 0
               then SJ.constDataF (labeled "K") n
                    <> SJ.valueToPairF (labeled "X") (SJ.jsonArrayF (vecF . projectRow rows))
               else mempty

mrModelDataJSONFold  :: MRData g modeledRow predRow
                     -> MRPBuilderM g predRow modeledRow psRow (SJ.StanJSONF modeledRow A.Series)
mrModelDataJSONFold modelDat = do
  model <- getModel
  sizesF <- mrGroupJSONFold modelDat
  predDataF <- predDataJSONFold "" modelDat
  allFEFld <- maybe (return mempty) (\fe -> mrFixedEffectFold Nothing modelDat Nothing fe) $ bmm_FixedEffects model
  groupFEFld <- fmap mconcat <$> traverse (\(gn, fe) -> mrFixedEffectFold Nothing modelDat (Just gn) fe) $ Map.toList $ bmm_FEGroups model
  return
    $ sizesF
    <> predDataF
    <> allFEFld
    <> groupFEFld
    <> SJ.valueToPairF "T" (SJ.jsonArrayF $ bmm_Total model)
    <> SJ.valueToPairF "S" (SJ.jsonArrayF $ bmm_Success model)

mrGroupOrderedIntIndexes :: MRPBuilderM f predRow modeledRow psRow [(Text, SB.IntIndex predRow)]
mrGroupOrderedIntIndexes = do
  model <- getModel
  indexMap <- getIndexes
  let groupNames = Set.toAscList $ bmm_MRGroups model
  maybe
    (SB.stanBuildError "Error looking up a group name in ModelBuilderEnv")
    return
    $ fmap (zip groupNames)
    $ traverse (flip Map.lookup indexMap) groupNames

feGroupOrderedIntIndexes :: MRPBuilderM f predRow modeledRow psRow [(Text, SB.IntIndex predRow)]
feGroupOrderedIntIndexes = do
  model <- getModel
  indexMap <- getIndexes
  let groupNames = fmap fst $ Map.toAscList $ bmm_FEGroups model
  maybe
    (SB.stanBuildError "Error looking up a group name in ModelBuilderEnv")
    return
    $ fmap (zip groupNames)
    $ traverse (flip Map.lookup indexMap) groupNames


groupOrderedIntIndexes :: MRPBuilderM f predRow modeledRow psRow [(Text, SB.IntIndex predRow)]
groupOrderedIntIndexes = do
  fe <- feGroupOrderedIntIndexes
  mr <- mrGroupOrderedIntIndexes
  return $ fe ++ mr

predRowsToIndexed :: (Num a, Show a)
                  => MRPBuilderM f predRow modeledRow psRow (FL.FoldM (Either Text) (predRow, a) (SJ.Indexed a))
predRowsToIndexed = do
  indexes <- fmap snd <$> groupOrderedIntIndexes
  let bounds = zip (repeat 1) $ fmap SB.i_Size indexes
      indexers = fmap SB.i_Index indexes
      toIndices x = maybe (Left "Indexer error when building psRow fold") Right $ traverse ($x) indexers
      f (pr, a) = fmap (,a) $ toIndices pr
  return $ postMapM (\x -> traverse f x >>= SJ.prepareIndexed 0 bounds) $ FL.generalize FL.list


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

mrLLDataJSONFold :: LLData f modeledRow predRow
                 -> MRPBuilderM f predRow modeledRow psRow (SJ.StanJSONF modeledRow A.Series)
mrLLDataJSONFold llDat = do
  model <- getModel
  predDatF <- predDataJSONFold "ll" llDat
  return
    $ predDatF
    <> SJ.valueToPairF "Tll" (SJ.jsonArrayF $ bmm_Total model)
    <> SJ.valueToPairF "Sll" (SJ.jsonArrayF $ bmm_Success model)

ntMRPData :: (forall a.f a -> g a) -> MRPData f j k l -> MRPData g j k l
ntMRPData h (MRPData mod mPS) = MRPData (h mod) (h <$> mPS)
-}
{-
mrpDataWrangler :: forall k psRow f predRow modeledRow.
                   (Foldable f, Functor f, Ord k)
                => MRPData f predRow modeledRow psRow
                -> Maybe (PostStratification k psRow predRow)
                -> MRPBuilderM f predRow modeledRow psRow (SC.DataWrangler (MRPData f predRow modeledRow psRow) (IM.IntMap k) ())
mrpDataWrangler (MRPData modeled mPS) mPSFunctions = do
  model <- getModel
  modelDataFold <- mrModelDataJSONFold (ProjectableRows modeled $ bmm_PrjPred model)
  psDataFold <- case mPS of
    Nothing -> return mempty
    Just ps -> case mPSFunctions of
      Nothing -> SB.stanBuildError "PostStratification data given but post-stratification functions unset."
      Just ps -> mrPSDataJSONFold ps "ps"
  let psKeyMapFld = maybe mempty mrPSKeyMapFld mPSFunctions
  jsonRowBuilders <- SB.buildJSONF --SB.builders . SB.jsonBuilder <$> get
  let makeDataJsonE d@(MRPData modeled mPS mLL) = SB.buildJSONFromRows d jsonRowBuilders
{-
        builtJSON <- SB.buildJSONSeries jsonBuilders d
        modeledJSON <- SJ.frameToStanJSONSeries modelDataFold modeled
        psJSON <- maybe (Right mempty) (FL.foldM psDataFold) mPS
        llJSON <- maybe (Right mempty) (SJ.frameToStanJSONSeries llDataFold) mLL
        return $ builtJSON <> modeledJSON <> psJSON <>  llJSON
      psKeyMap = maybe mempty (FL.fold psKeyMapFld)
-}
      f (MRPData _ mPS _) = ((), makeDataJsonE)

  return $ SC.Wrangle SC.TransientIndex f
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
{-
feIndexes :: MRPBuilderM f predRow modeledRow psRow (Map GroupName (SB.IntIndex predRow))
feIndexes = do
  indexMap <- getIndexes -- SB.asksEnv sbe_groupIndices
  groupNames <- Set.fromList . Map.keys . bmm_FEGroups <$> getModel
  return $ Map.restrictKeys indexMap groupNames
-}
{-
groupSizesBlock :: MRPBuilderM f predRow modeledRow psRow ()
groupSizesBlock = do
  ui <- usedIndexes
  let groupSize x = SB.addStanLine $ "int<lower=2> J_" <> fst x
  traverse_ groupSize $ Map.toList ui

labeledDataBlockForRows :: Text -> MRPBuilderM f predRow modeledRow psRow ()
labeledDataBlockForRows suffix = do
  ui <- usedIndexes
  let groupIndex x = SB.addStanLine $ "int<lower=1, upper=J_" <> fst x <> "> " <> fst x <> SB.underscoredIf suffix <> "[N" <> suffix <> "]"
{-
        if i_Size (snd x) == 2
                     then SB.addStanLine $ "int<lower=1, upper=2> " <> fst x <> "_" <> suffix <> "[N" <> suffix <> "]"
                     else SB.addStanLine $ "int<lower=1, upper=J_" <> fst x <> "> " <> fst x <> "_" <> suffix <> "[N" <> suffix <> "]"
-}
  SB.addStanLine $ "int<lower=1> N" <> suffix
  traverse_ groupIndex $ Map.toList ui
-}

dataBlockM :: (Typeable d, Typeable modeledRow) => BuilderM modeledRow d ()
dataBlockM = do
  model <- getModel
  SB.inBlock SB.SBData $ SB.addLine $ "int<lower=0> N"
  allGroupsM
  allFixedEffectsM
  SB.inBlock SB.SBData $ do
    SB.addLine "int<lower=1> T[N]"
    SB.addLine "int<lower=0> S[N]"
    SB.addColumnJson SB.ModeledRowTag "T" "" (bmm_Total model)
    SB.addColumnJson SB.ModeledRowTag "S" "" (bmm_Success model)

{-
mrpDataBlock :: Bool
             -> Bool
             -> MRPBuilderM f predRow modeledRow psRow ()
mrpDataBlock postStratify diffLL = SB.inBlock SB.SBData $ do
  model <- getModel
  groupSizesBlock
  labeledDataBlockForRows ""
  case bmm_FixedEffects model of
    Just fe ->  SB.fixedEffectsQR "" "X" "N" "K"
    Nothing -> return ()
  let doGroupFE (gn, fe) = do
        let suffix = "_" <> gn
        SB.addStanLine $ "int<lower=1> N" <> suffix
        SB.fixedEffectsQR suffix ("X" <> suffix) ("N" <> suffix) ("K" <> suffix)
  traverse_ doGroupFE $ Map.toList $ bmm_FEGroups model
  SB.addStanLine $ "int<lower=1> T[N]"
  SB.addStanLine $ "int<lower=0> S[N]"
  case postStratify of
    False -> return ()
    True -> do
      SB.addStanLine $ "int<lower=0> Nps"
      case bmm_FixedEffects model of
        Just fe -> SB.addStanLine $ "matrix[Nps, K] Xps"
        Nothing -> return ()
      groupUpperBounds <- T.intercalate "," . fmap (show . SB.i_Size . snd) <$> groupOrderedIntIndexes
      SB.addStanLine $ "real<lower=0>[" <> groupUpperBounds <> "]" <> "W[Nps]" -- real[2,2,4] W[Nps];
  case diffLL of
    False -> return ()
    True -> do
      labeledDataBlockForRows "ll"
      case bmm_FixedEffects model of
        Just fe -> SB.addStanLine $ "matrix[Nll, K] Xll"
        Nothing -> return ()
      SB.addStanLine $ "int <lower=0>[Nll] Tll"
      SB.addStanLine $ "int <lower=0>[Nll] Sll"
-}

mrParametersBlock :: BuilderM modeledRow d ()
mrParametersBlock = SB.inBlock SB.SBParameters $ do
  ui <- mrIndexes
  let binaryParameter x = SB.addStanLine $ "real eps_" <> fst x
      nonBinaryParameter x = do
        let n = fst x
        SB.addStanLine ("real<lower=0> sigma_" <> n)
        SB.addStanLine ("vector<multiplier = sigma_" <> n <> ">[J_" <> n <> "] beta_" <> n)
      groupParameter x = if (SB.i_Size $ snd x) == 2
                         then binaryParameter x
                         else nonBinaryParameter x
  SB.addStanLine "real alpha"
  traverse_ groupParameter $ Map.toList ui


-- alpha + X * beta + beta_age[age] + ...
modelExpr :: Bool -> Text -> BuilderM modeledRow d SB.StanExpr
modelExpr thinQR suffix = do
  model <- getModel
  mrIndexMap <- mrIndexes
  let labeled x = x <> suffix
      binaryGroupExpr x = let n = fst x in SB.VectorFunctionE "to_vector" $ SB.TermE $ SB.Indexed n $ "{eps_" <> n <> ", -eps_" <> n <> "}"
      nonBinaryGroupExpr x = let n = fst x in SB.TermE . SB.Indexed n $ "beta_" <> n
      groupExpr x = if (SB.i_Size $ snd x) == 2 then binaryGroupExpr x else nonBinaryGroupExpr x
      eAlpha = SB.TermE $ SB.Scalar "alpha"
      eQ s = SB.TermE $ SB.Vectored $ "Q" <> SB.underscoredIf s <> "_ast"
      eTheta s = SB.TermE $ SB.Scalar $ "thetaX" <> SB.underscoredIf s
      eQTheta s = SB.BinOpE "*" (eQ s) (eTheta s)
      eX s = SB.TermE $ SB.Vectored $ "X" <> SB.underscoredIf s
      eBeta s = SB.TermE $ SB.Scalar $ "betaX" <> SB.underscoredIf s
      eXBeta s = SB.BinOpE "*" (eX s) (eBeta s)
      feExpr s = if thinQR then eQTheta s else eXBeta s
      feGroupsExpr = fmap (\(DHash.Some rtt) -> feExpr $ SB.dsSuffix rtt) $ DHash.keys $ bmm_FixedEffects model
--      lFEExpr = if (isJust $ bmm_FixedEffects model)
--                then feExpr "" : feGroupsExpr
--                else feGroupsExpr
      lMRGroupsExpr = maybe [] pure
                      $ viaNonEmpty (SB.multiOp "+" . fmap groupExpr) $ Map.toList $ mrIndexMap
  let neTerms = eAlpha :| (feGroupsExpr <> lMRGroupsExpr)
  return $ SB.multiOp "+" neTerms

mrpModelBlock :: Double -> Double -> Double -> BuilderM modeledRow d ()
mrpModelBlock priorSDAlpha priorSDBeta priorSDSigmas = SB.inBlock SB.SBModel $ do
  model <- getModel
  mrGroupIndexes <- mrIndexes
  indexMap <- getIndexes --SB.asksEnv sbe_groupIndices
  let binaryPrior x = SB.addStanLine $ "eps_" <> fst x <> " ~ normal(0, " <> show priorSDAlpha <> ")"
      nonBinaryPrior x = do
        SB.addStanLine $ "beta_" <> fst x <> " ~ normal(0, sigma_" <> fst x <> ")"
        SB.addStanLine $ "sigma_" <> fst x <> " ~ normal(0, " <> show priorSDSigmas <> ")"
      groupPrior x = if (SB.i_Size $ snd x) == 2
                     then binaryPrior x
                     else nonBinaryPrior x
      fePrior x = SB.addStanLine $ "thetaX" <> SB.underscoredIf x <> " ~ normal(0," <> show priorSDBeta <> ")"
  let im = Map.mapWithKey const indexMap
  modelTerms <- SB.printExprM "mrpModelBlock" im SB.Vectorized $ modelExpr True ""
  SB.addStanLine $ "alpha ~ normal(0," <> show priorSDAlpha <> ")"
--  when (isJust $ bmm_FixedEffects model) $ fePrior ""
  traverse_ (\(DHash.Some rtt) -> fePrior  $ SB.dsSuffix rtt) $ DHash.keys $ bmm_FixedEffects model
  mrIndexes >>= traverse_ groupPrior . Map.toList
  SB.addStanLine $ "S ~ binomial_logit(T, " <> modelTerms <> ")"

mrpLogLikStanCode :: BuilderM modeledRow d ()
mrpLogLikStanCode = SB.inBlock SB.SBGeneratedQuantities $ do
  model <- getModel
  indexMap <- getIndexes --SB.asksEnv sbe_groupIndices
--  let suffix = if difLL then "ll" else "" -- we use model data unless a different suffix is provided
  SB.addStanLine $ "vector [N] log_lik"
  SB.stanForLoop "n" Nothing "N" $ \_ -> do
    let im = Map.mapWithKey (\k _ -> k <> "[n]") indexMap -- we need to index the groups.
    modelTerms <- SB.printExprM "mrpLogLikStanCode" im (SB.NonVectorized "n") $ modelExpr False ""
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
--  mrpPSStanCode doPS
  mrpLogLikStanCode




--binomialMRPPostStratification
{-
mrpDataWrangler :: Text -> MRP_Model -> MRP_DataWrangler as bs ()
mrpDataWrangler cacheDir model =
  MRP_DataWrangler
  $ SC.WrangleWithPredictions (SC.CacheableIndex $ \c -> cacheDir <> "stan/index" <> SC.mrcOutputPrefix c <> ".bin") f g
  where

    enumStateF = FL.premap (F.rgetField @BR.StateAbbreviation) (SJ.enumerate 1)
    encodeAge = SF.toRecEncoding @DT.SimpleAgeC $ SJ.dummyEncodeEnum @DT.SimpleAge
    encodeSex = SF.toRecEncoding @DT.SexC $ SJ.dummyEncodeEnum @DT.Sex
  encodeEducation = SF.toRecEncoding @DT.CollegeGradC $ SJ.dummyEncodeEnum @DT.CollegeGrad
  encodeRace = SF.toRecEncoding @DT.Race5C $ SJ.dummyEncodeEnum @DT.Race5
  encodeCatCols :: SJ.Encoding SJ.IntVec (F.Record DT.CatColsASER5)
  encodeCatCols = SF.composeIntVecRecEncodings encodeAge
                  $ SF.composeIntVecRecEncodings encodeSex
                  $ SF.composeIntVecRecEncodings encodeEducation encodeRace
  (catColsIndexer, toCatCols) = encodeCatCols
  f cces = ((toState, fmap FS.toS toCatCols), makeJsonE) where
    (stateM, toState) = FL.fold enumStateF cces
    k = SJ.vecEncodingLength encodeCatCols
    makeJsonE x = SJ.frameToStanJSONSeries dataF cces where
      dataF = SJ.namedF "G" FL.length
              <> SJ.constDataF "J_state" (IM.size toState)
              <> SJ.constDataF "K" k
              <> SJ.valueToPairF "X" (SJ.jsonArrayMF (catColsIndexer . F.rcast @DT.CatColsASER5))
              <> SJ.valueToPairF "state" (SJ.jsonArrayMF (stateM . F.rgetField @BR.StateAbbreviation))
              <> SJ.valueToPairF "D_votes" (SJ.jsonArrayF (round @_ @Int . F.rgetField @BR.WeightedSuccesses))
              <> SJ.valueToPairF "Total_votes" (SJ.jsonArrayF (F.rgetField @BR.Count))
  g (toState, _) toPredict = SJ.frameToStanJSONSeries predictF toPredict where
    toStateIndexM sa = M.lookup sa $ SJ.flipIntIndex toState
    predictF = SJ.namedF "M" FL.length
               <> SJ.valueToPairF "predict_State" (SJ.jsonArrayMF (toStateIndexM . F.rgetField @BR.StateAbbreviation))
               <> SJ.valueToPairF "predict_X" (SJ.jsonArrayMF (catColsIndexer . F.rcast @DT.CatColsASER5))


extractResults :: Int
               -> ET.OfficeT
               -> CS.StanSummary
               -> F.FrameRec (CCES_KeyRow DT.CatColsASER5)
               -> Either T.Text (F.FrameRec (CCES_KeyRow DT.CatColsASER5 V.++ '[BR.Year, ET.Office, ET.DemVPV, BR.DemPref]))
extractResults year office summary toPredict = do
   predictProbs <- fmap CS.mean <$> SP.parse1D "predicted" (CS.paramStats summary)
   let yoRec :: F.Record '[BR.Year, ET.Office] = year F.&: office F.&: V.RNil
       probRec :: Double -> F.Record [ET.DemVPV, BR.DemPref]
       probRec x = (2*x -1 ) F.&: x F.&: V.RNil
       makeRow key prob = key F.<+> yoRec F.<+> probRec prob
   return $ F.toFrame $ uncurry makeRow <$> zip (FL.fold FL.list toPredict) (FL.fold FL.list predictProbs)

comparePredictions ::  K.KnitEffects r
                   => F.FrameRec (CCES_KeyRow DT.CatColsASER5 V.++ '[BR.Year, ET.Office, ET.DemVPV, BR.DemPref])
                   -> F.FrameRec (CCES_CountRow DT.CatColsASER5)
                   -> K.Sem r ()
comparePredictions predictions input = do
  joined <- K.knitEither
            $ either (Left. show) Right
            $ FJ.leftJoinE @(CCES_KeyRow DT.CatColsASER5) input predictions
  let p = F.rgetField @ET.DemPref
      n = F.rgetField @BR.Count
      s = realToFrac . round @_ @Int . F.rgetField @BR.WeightedSuccesses
      rowCountError r = abs (p r * realToFrac (n r) - s r)
      countErrorOfVotersF = (\x y -> x / realToFrac y) <$> FL.premap rowCountError FL.sum <*> FL.premap n FL.sum
      countErrorOfDemsF = (\x y -> x / realToFrac y) <$> FL.premap rowCountError FL.sum <*> FL.premap s FL.sum
      (countErrorV, countErrorD) = FL.fold ((,) <$> countErrorOfVotersF <*> countErrorOfDemsF) joined
  K.logLE K.Info $ "absolute count error (fraction of all votes): " <> show countErrorV
  K.logLE K.Info $ "absolute count error (fraction of D votes): " <> show countErrorD


count :: forall ks r.
              (K.KnitEffects r
              , Ord (F.Record ks)
              , FI.RecVec (ks V.++ BR.CountCols)
              , ks F.⊆ CCES.CCES_MRP
              )
      => (F.Record CCES.CCES_MRP -> F.Record ks)
      -> ET.OfficeT
      -> Int
      -> F.FrameRec CCES.CCES_MRP
      -> K.Sem r (F.FrameRec (ks V.++ BR.CountCols))
count getKey office year ccesMRP = do
  countFold <- K.knitEither $ case (office, year) of
    (ET.President, 2008) ->
      Right $ CCES.countDVotesF @CCES.Pres2008VoteParty getKey 2008
    (ET.President, 2012) ->
      Right $ CCES.countDVotesF @CCES.Pres2012VoteParty getKey 2012
    (ET.President, 2016) ->
      Right $ CCES.countDVotesF @CCES.Pres2016VoteParty getKey 2016
    (ET.House, y) ->
      Right $  CCES.countDVotesF @CCES.HouseVoteParty getKey y
    _ -> Left $ show office <> "/" <> show year <> " not available."
  let counted = FL.fold countFold ccesMRP
  return counted

prefASER5_MR :: (K.KnitEffects r,  BR.CacheEffects r, BR.SerializerC b)
             => (T.Text, CCESDataWrangler DT.CatColsASER5 b)
             -> (T.Text, SB.StanModel)
             -> ET.OfficeT
             -> Int
             -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec
                                                  (CCES_KeyRow DT.CatColsASER5
                                                    V.++
                                                    '[BR.Year, ET.Office, ET.DemVPV, BR.DemPref]
                                                  )))
prefASER5_MR (dataLabel, ccesDataWrangler) (modelName, model) office year = do
  -- count data
  let officeYearT = show office <> "_" <> show year
      countCacheKey = "data/stan/cces/stateVotesASER5_" <> officeYearT <> ".bin"
  allStatesL <- do
    stateXWalk <- K.ignoreCacheTimeM BR.stateAbbrCrosswalkLoader
    return $ fmap (F.rgetField @BR.StateAbbreviation) .  FL.fold FL.list . F.filterFrame ((<60) . F.rgetField @BR.StateFIPS) $ stateXWalk
  let toPredict :: F.FrameRec ('[BR.StateAbbreviation] V.++ DT.CatColsASER5)
      toPredict = F.toFrame [ s F.&: cat | s <- allStatesL, cat <- DT.allCatKeysASER5]
  cces_C <- CCES.ccesDataLoader
  ccesASER5_C <- BR.retrieveOrMakeFrame countCacheKey cces_C
                 $ count (F.rcast @(BR.StateAbbreviation ': DT.CatColsASER5)) office year
  let stancConfig = (SM.makeDefaultStancConfig (toString $ "stan/voterPref/" <> modelName)) { CS.useOpenCL = False }
  stanConfig <- SC.noLogOfSummary
                <$> SM.makeDefaultModelRunnerConfig
                "stan/voterPref"
                (modelName <> "_model")
                (Just (SB.NoLL, model))
                (Just $ "cces_" <> officeYearT <> "_" <> dataLabel <> ".json")
                (Just $ "cces_" <> officeYearT <> "_" <> modelName <> "_model")
                4
                (Just 1000)
                (Just 1000)
                (Just stancConfig)
  let resultCacheKey = "model/stan/cces/statePrefsASER5_" <> officeYearT <> "_" <> modelName <> ".bin"
  modelDep <- SM.modelCacheTime stanConfig
  let dataModelDep = const <$> modelDep <*> ccesASER5_C
      getResults s tp inputAndIndex_C = do
        (input, _) <- K.ignoreCacheTime inputAndIndex_C
        predictions <- K.knitEither $ extractResults year office s tp
        comparePredictions predictions input
        return predictions
  BR.retrieveOrMakeFrame resultCacheKey dataModelDep $ \() -> do
    K.logLE K.Info "Data or model newer than last cached result. Rerunning."
    SM.runModel @BR.SerializerC @BR.CacheData stanConfig (SM.ShinyStan [SR.UnwrapNamed "D_votes" "D_votes"]) ccesDataWrangler (SC.UseSummary getResults) toPredict ccesASER5_C


prefASER5_MR_Loo :: (K.KnitEffects r,  BR.CacheEffects r, BR.SerializerC b)
                 => (T.Text, CCESDataWrangler DT.CatColsASER5 b)
                 -> (T.Text, SB.StanModel)
                 -> ET.OfficeT
                 -> Int
                 -> K.Sem r ()
prefASER5_MR_Loo (dataLabel, ccesDataWrangler) (modelName, model) office year = do
  -- count data
  let officeYearT = show office <> "_" <> show year
      countCacheKey = "data/stan/cces/stateVotesASER5_" <> officeYearT <> ".bin"
  cces_C <- CCES.ccesDataLoader
  K.logLE K.Diagnostic "Finished loading (cached) CCES data"
  ccesASER5_C <- BR.retrieveOrMakeFrame countCacheKey cces_C
                 $ count (F.rcast  @(BR.StateAbbreviation ': DT.CatColsASER5)) office year
  let stancConfig = (SM.makeDefaultStancConfig $ toString $ "stan/voterPref/" <> modelName <> "_loo") { CS.useOpenCL = False }
  stanConfig <- SC.noLogOfSummary
                <$> SM.makeDefaultModelRunnerConfig
                "stan/voterPref"
                (modelName <> "_loo")
                (Just (SB.OnlyLL, model))
                (Just $ "cces_" <> officeYearT <> "_" <> dataLabel <> ".json")
                (Just $ "cces_" <> officeYearT <> "_" <> modelName <> "_loo")
                4
                (Just 1000)
                (Just 1000)
                (Just stancConfig)
  SM.runModel @BR.SerializerC @BR.CacheData stanConfig SM.Loo (SC.noPredictions ccesDataWrangler) SC.DoNothing () ccesASER5_C
-}

model_BinomialAllBuckets :: SB.StanModel
model_BinomialAllBuckets = SB.StanModel
                           binomialASER5_StateDataBlock
                           (Just binomialASER5_StateTransformedDataBlock)
                           binomialASER5_StateParametersBlock
                           Nothing
                           binomialASER5_StateModelBlock
                           (Just binomialASER5_StateGeneratedQuantitiesBlock)
                           binomialASER5_StateGQLLBlock

model_v2 :: SB.StanModel
model_v2 = SB.StanModel
           binomialASER5_StateDataBlock
           (Just binomialASER5_StateTransformedDataBlock)
           binomialASER5_v2_StateParametersBlock
           Nothing
           binomialASER5_v2_StateModelBlock
           (Just binomialASER5_v2_StateGeneratedQuantitiesBlock)
           binomialASER5_v2_StateGQLLBlock

model_v3 :: SB.StanModel
model_v3 = SB.StanModel
           binomialASER5_StateDataBlock
           (Just binomialASER5_StateTransformedDataBlock)
           binomialASER5_v3_ParametersBlock
           Nothing
           binomialASER5_v3_ModelBlock
           (Just binomialASER5_v3_GeneratedQuantitiesBlock)
           binomialASER5_v3_GQLLBlock

model_v4 :: SB.StanModel
model_v4 = SB.StanModel
           binomialASER5_v4_DataBlock
           Nothing
           binomialASER5_v4_ParametersBlock
           Nothing
           binomialASER5_v4_ModelBlock
           (Just binomialASER5_v4_GeneratedQuantitiesBlock)
           binomialASER5_v4_GQLLBlock


model_v5 :: SB.StanModel
model_v5 = SB.StanModel
           binomialASER5_v4_DataBlock
           Nothing
           binomialASER5_v5_ParametersBlock
           Nothing
           binomialASER5_v5_ModelBlock
           (Just binomialASER5_v5_GeneratedQuantitiesBlock)
           binomialASER5_v5_GQLLBlock

model_v6 :: SB.StanModel
model_v6 = SB.StanModel
           binomialASER5_v4_DataBlock
           (Just binomialASER5_v6_TransformedDataBlock)
           binomialASER5_v6_ParametersBlock
           Nothing
           binomialASER5_v6_ModelBlock
           (Just binomialASER5_v6_GeneratedQuantitiesBlock)
           binomialASER5_v6_GQLLBlock


model_v7 :: SB.StanModel
model_v7 = SB.StanModel
           binomialASER5_v4_DataBlock
           (Just binomialASER5_v6_TransformedDataBlock)
           binomialASER5_v7_ParametersBlock
           (Just binomialASER5_v7_TransformedParametersBlock)
           binomialASER5_v7_ModelBlock
           (Just binomialASER5_v7_GeneratedQuantitiesBlock)
           binomialASER5_v7_GQLLBlock


binomialASER5_StateDataBlock :: SB.DataBlock
binomialASER5_StateDataBlock = [here|
  int<lower = 0> G; // number of cells
  int<lower = 1> J_state; // number of states
  int<lower = 1> J_sex; // number of sex categories
  int<lower = 1> J_age; // number of age categories
  int<lower = 1> J_educ; // number of education categories
  int<lower = 1> J_race; // number of race categories
  int<lower = 1, upper = J_state> state[G];
  int<lower = 1, upper = J_age * J_sex * J_educ * J_race> category[G];
  int<lower = 0> D_votes[G];
  int<lower = 0> Total_votes[G];
  int<lower = 0> M; // number of predictions
  int<lower = 0> predict_State[M];
  int<lower = 0> predict_Category[M];
|]

binomialASER5_StateTransformedDataBlock :: SB.TransformedDataBlock
binomialASER5_StateTransformedDataBlock = [here|
  int <lower=1> nCat;
  nCat =  J_age * J_sex * J_educ * J_race;
|]

binomialASER5_StateParametersBlock :: SB.ParametersBlock
binomialASER5_StateParametersBlock = [here|
  vector[nCat] beta;
  real<lower=0> sigma_alpha;
  matrix<multiplier=sigma_alpha>[J_state, nCat] alpha;
|]

binomialASER5_StateModelBlock :: SB.ModelBlock
binomialASER5_StateModelBlock = [here|
  sigma_alpha ~ normal (0, 10);
  to_vector(alpha) ~ normal (0, sigma_alpha);
  for (g in 1:G) {
   D_votes[g] ~ binomial_logit(Total_votes[g], beta[category[g]] + alpha[state[g], category[g]]);
  }
|]

binomialASER5_StateGeneratedQuantitiesBlock :: SB.GeneratedQuantitiesBlock
binomialASER5_StateGeneratedQuantitiesBlock = [here|
  vector <lower = 0, upper = 1> [M] predicted;
  for (p in 1:M) {
    predicted[p] = inv_logit(beta[predict_Category[p]] + alpha[predict_State[p], predict_Category[p]]);
  }
|]

binomialASER5_StateGQLLBlock :: SB.GeneratedQuantitiesBlock
binomialASER5_StateGQLLBlock = [here|
  vector[G] log_lik;
  for (g in 1:G) {
      log_lik[g] =  binomial_logit_lpmf(D_votes[g] | Total_votes[g], beta[category[g]] + alpha[state[g], category[g]]);
  }
|]


binomialASER5_v2_StateParametersBlock :: SB.ParametersBlock
binomialASER5_v2_StateParametersBlock = [here|
  vector[nCat] beta;
  real<lower=0> sigma_alpha;
  vector<multiplier=sigma_alpha>[J_state] alpha;
|]

binomialASER5_v2_StateModelBlock :: SB.ModelBlock
binomialASER5_v2_StateModelBlock = [here|
  sigma_alpha ~ normal (0, 10);
  alpha ~ normal (0, sigma_alpha);
  D_votes ~ binomial_logit(Total_votes, beta[category] + alpha[state]);
|]

binomialASER5_v2_StateGeneratedQuantitiesBlock :: SB.GeneratedQuantitiesBlock
binomialASER5_v2_StateGeneratedQuantitiesBlock = [here|
  vector <lower = 0, upper = 1> [nCat] nationalProbs;
  matrix <lower = 0, upper = 1> [J_state, nCat] stateProbs;
  nationalProbs = inv_logit(beta[category]);
  stateProbs = inv_logit(beta[category] + alpha[state])
|]

binomialASER5_v2_StateGQLLBlock :: SB.GeneratedQuantitiesBlock
binomialASER5_v2_StateGQLLBlock = [here|
  vector[G] log_lik;
  for (g in 1:G) {
    log_lik[g] =  binomial_logit_lpmf(D_votes[g] | Total_votes[g], beta[category[g]] + alpha[state[g]]);
  }
|]


binomialASER5_v3_ParametersBlock :: SB.ParametersBlock
binomialASER5_v3_ParametersBlock = [here|
  vector[nCat] beta;
|]

binomialASER5_v3_ModelBlock :: SB.ModelBlock
binomialASER5_v3_ModelBlock = [here|
  D_votes ~ binomial_logit(Total_votes, beta[category]);
|]

binomialASER5_v3_GeneratedQuantitiesBlock :: SB.GeneratedQuantitiesBlock
binomialASER5_v3_GeneratedQuantitiesBlock = [here|
  vector <lower = 0, upper = 1> [nCat] nationalProbs;
  nationalProbs = inv_logit(beta[category]);
|]

binomialASER5_v3_GQLLBlock :: SB.GeneratedQuantitiesBlock
binomialASER5_v3_GQLLBlock = [here|
  vector[G] log_lik;
  for (g in 1:G) {
    log_lik[g] =  binomial_logit_lpmf(D_votes[g] | Total_votes[g], beta[category[g]]);
  }
|]

binomialASER5_v4_DataBlock :: SB.DataBlock
binomialASER5_v4_DataBlock = [here|
  int<lower = 0> G; // number of cells
  int<lower = 1> J_state; // number of states
  int<lower = 1, upper = J_state> state[G];
  int<lower = 1> K; // number of cols in predictor matrix
  matrix[G, K] X;
  int<lower = 0> D_votes[G];
  int<lower = 0> Total_votes[G];
  int<lower = 0> M;
  int<lower = 0> predict_State[M];
  matrix[M, K] predict_X;

|]



binomialASER5_v4_ParametersBlock :: SB.ParametersBlock
binomialASER5_v4_ParametersBlock = [here|
  real alpha;
  vector[K] beta;
|]

binomialASER5_v4_ModelBlock :: SB.ModelBlock
binomialASER5_v4_ModelBlock = [here|
    D_votes ~ binomial_logit(Total_votes, alpha + X * beta);
|]


binomialASER5_v4_GeneratedQuantitiesBlock :: SB.GeneratedQuantitiesBlock
binomialASER5_v4_GeneratedQuantitiesBlock = [here|
  vector<lower = 0, upper = 1>[M] predicted;
  for (p in 1:M) {
    real xBeta;
//    for (k in 1:K) {
//      xBeta = predict_X[p, k] * beta[k];
//    }
    predicted[p] = inv_logit(alpha + predict_X[p] * beta);
  }
|]


binomialASER5_v4_GQLLBlock :: SB.GeneratedQuantitiesBlock
binomialASER5_v4_GQLLBlock = [here|
  vector[G] log_lik;
  for (g in 1:G) {
    log_lik[g] =  binomial_logit_lpmf(D_votes[g] | Total_votes[g], alpha + X[g] * beta);
  }
|]


binomialASER5_v5_ParametersBlock :: SB.ParametersBlock
binomialASER5_v5_ParametersBlock = [here|
  real alpha;
  vector[K] beta;
  real<lower=0> sigma_aState;
  vector<multiplier=sigma_aState> [J_state] aState;
|]

binomialASER5_v5_ModelBlock :: SB.ModelBlock
binomialASER5_v5_ModelBlock = [here|
  alpha ~ normal(0,2);
  beta ~ normal(0,1);
  sigma_aState ~ normal(0, 10);
  aState ~ normal(0, sigma_aState);
  D_votes ~ binomial_logit(Total_votes, alpha + (X * beta) + aState[state]);
|]


binomialASER5_v5_GeneratedQuantitiesBlock :: SB.GeneratedQuantitiesBlock
binomialASER5_v5_GeneratedQuantitiesBlock = [here|
  vector<lower = 0, upper = 1>[M] predicted;
  predicted = inv_logit(alpha + (predict_X * beta) + aState[predict_State]);
|]


binomialASER5_v5_GQLLBlock :: SB.GeneratedQuantitiesBlock
binomialASER5_v5_GQLLBlock = [here|
  vector[G] log_lik;
  for (g in 1:G) {
    log_lik[g] =  binomial_logit_lpmf(D_votes[g] | Total_votes[g], alpha + X[g] * beta + aState[state[g]]);
  }
|]

binomialASER5_v6_TransformedDataBlock :: SB.TransformedDataBlock
binomialASER5_v6_TransformedDataBlock = [here|
  vector[G] intcpt;
  vector[M] predictIntcpt;
  matrix[G, K+1] XI; // add the intercept so the covariance matrix is easier to deal with
  matrix[M, K+1] predict_XI;
  for (g in 1:G)
    intcpt[g] = 1;
  XI = append_col(intcpt, X);
  for (m in 1:M)
    predictIntcpt[m] = 1;
  predict_XI = append_col(predictIntcpt, predict_X);
|]

binomialASER5_v6_ParametersBlock :: SB.ParametersBlock
binomialASER5_v6_ParametersBlock = [here|
  real alpha; // overall intercept
  vector[K] beta; // fixed effects
  vector<lower=0> [K+1] sigma;
  vector[K+1] betaState[J_state]; // state-level coefficients
|]

binomialASER5_v6_ModelBlock :: SB.ModelBlock
binomialASER5_v6_ModelBlock = [here|
  alpha ~ normal(0,2); // weak prior around 50%
  beta ~ normal(0,1);
  sigma ~ normal(0,10);
  for (s in 1:J_state)
    betaState[s] ~ normal(0, sigma);
  {
    vector[G] xiBetaState;
    for (g in 1:G)
      xiBetaState[g] = XI[g] * betaState[state[g]];
    D_votes ~ binomial_logit(Total_votes, alpha + (X * beta) + xiBetaState);
  }
|]


binomialASER5_v6_GeneratedQuantitiesBlock :: SB.GeneratedQuantitiesBlock
binomialASER5_v6_GeneratedQuantitiesBlock = [here|
  vector<lower = 0, upper = 1>[M] predicted;
  for (m in 1:M)
    predicted[m] = inv_logit(alpha + (predict_X[m] * beta) + (predict_XI[m] * betaState[predict_State[m]]));
|]


binomialASER5_v6_GQLLBlock :: SB.GeneratedQuantitiesBlock
binomialASER5_v6_GQLLBlock = [here|
  vector[G] log_lik;
  for (g in 1:G) {
    log_lik[g] =  binomial_logit_lpmf(D_votes[g] | Total_votes[g], alpha + X[g] * beta + XI[g] * betaState[state[g]]);
  }
|]


binomialASER5_v7_ParametersBlock :: SB.ParametersBlock
binomialASER5_v7_ParametersBlock = [here|
  real alpha; // overall intercept
  vector[K] beta; // fixed effects
  vector<lower=0, upper=pi()/2> [K+1] tau_unif; // group effects scales
  cholesky_factor_corr[K+1] L_Omega; // group effect correlations
  matrix[K+1, J_state] z; // state-level coefficients pre-transform
|]

binomialASER5_v7_TransformedParametersBlock :: SB.TransformedParametersBlock
binomialASER5_v7_TransformedParametersBlock = [here|
  vector<lower=0>[K+1] tau;
  matrix[J_state, K+1] betaState; // state-level coefficients
  for (k in 1:(K+1))
    tau[k] = 2.5 * tan(tau_unif[k]);
  betaState = (diag_pre_multiply(tau, L_Omega) * z)';
|]



binomialASER5_v7_ModelBlock :: SB.ModelBlock
binomialASER5_v7_ModelBlock = [here|
  alpha ~ normal(0,2); // weak prior around 50%
  beta ~ normal(0,1);
  to_vector(z) ~ std_normal();
  L_Omega ~ lkj_corr_cholesky(2);
  D_votes ~ binomial_logit(Total_votes, alpha + X * beta + rows_dot_product(betaState[state], XI));
|]


binomialASER5_v7_GeneratedQuantitiesBlock :: SB.GeneratedQuantitiesBlock
binomialASER5_v7_GeneratedQuantitiesBlock = [here|
  vector<lower = 0, upper = 1>[M] predicted;
  for (m in 1:M)
    predicted[m] = inv_logit(alpha + (predict_X[m] * beta) + dot_product(predict_XI[m], betaState[predict_State[m]]));
|]


binomialASER5_v7_GQLLBlock :: SB.GeneratedQuantitiesBlock
binomialASER5_v7_GQLLBlock = [here|
  vector[G] log_lik;
  for (g in 1:G) {
    log_lik[g] =  binomial_logit_lpmf(D_votes[g] | Total_votes[g], alpha + X[g] * beta + dot_product(XI[g], betaState[state[g]]));
  }
|]

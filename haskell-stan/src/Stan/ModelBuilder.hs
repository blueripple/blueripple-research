{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}

module Stan.ModelBuilder
(
  module Stan.ModelBuilder
  , module Stan.ModelBuilder.Expressions
  , module Stan.ModelBuilder.Distributions
  )
where

import qualified Stan.JSON as Stan
import qualified Stan.ModelBuilder.Expressions as SME
import Stan.ModelBuilder.Expressions
import Stan.ModelBuilder.Distributions

import Prelude hiding (All)
import qualified Control.Foldl as Foldl
import qualified Data.Aeson as Aeson
import qualified Data.Array as Array
import qualified Data.Dependent.HashMap as DHash
import qualified Data.Dependent.Sum as DSum
import qualified Data.GADT.Compare as GADT
import qualified Data.IntMap.Strict as IntMap
import qualified Data.List as List
import qualified Data.Map.Strict as Map
import qualified Data.Proxy as Proxy
import qualified Data.Sequence as Seq
import qualified Data.Set as Set
import qualified Data.Some as Some
import qualified Data.Text as T
import qualified Data.Text.IO as T
import qualified Data.Time.Clock as Time
import qualified Data.Vector as Vector
import qualified Data.Hashable as Hashable
import qualified Say
import qualified System.Directory as Dir
import qualified System.Environment as Env
import qualified Type.Reflection as Reflection

type FunctionsBlock = T.Text

type DataBlock = T.Text

type TransformedDataBlock = T.Text

type ParametersBlock = T.Text

type TransformedParametersBlock = T.Text

type ModelBlock = T.Text

type GeneratedQuantitiesBlock = T.Text

data GeneratedQuantities = NoLL | OnlyLL | All

data StanModel = StanModel
  { functionsBlock :: Maybe FunctionsBlock,
    dataBlock :: DataBlock,
    transformedDataBlockM :: Maybe TransformedDataBlock,
    parametersBlock :: ParametersBlock,
    transformedParametersBlockM :: Maybe TransformedParametersBlock,
    modelBlock :: ModelBlock,
    generatedQuantitiesBlockM :: Maybe GeneratedQuantitiesBlock,
    genLogLikelihoodBlock :: GeneratedQuantitiesBlock
  }
  deriving (Show, Eq, Ord)

data StanBlock = SBFunctions
               | SBData
               | SBTransformedData
               | SBParameters
               | SBTransformedParameters
               | SBModel
               | SBGeneratedQuantities
               | SBLogLikelihood deriving (Show, Eq, Ord, Enum, Bounded, Array.Ix)

data WithIndent = WithIndent Text Int

data StanCode = StanCode { curBlock :: StanBlock
                         , blocks :: Array.Array StanBlock WithIndent
                         }


stanCodeToStanModel :: StanCode -> StanModel
stanCodeToStanModel (StanCode _ a) =
  StanModel
  (f . g $ a Array.! SBFunctions)
  (g $ a Array.! SBData)
  (f . g  $ a Array.! SBTransformedData)
  (g $ a Array.! SBParameters)
  (f . g $ a Array.! SBTransformedParameters)
  (g $ a Array.! SBModel)
  (f . g $ a Array.! SBGeneratedQuantities)
  (g $ a Array.! SBLogLikelihood)
  where
    f x = if x == "" then Nothing else Just x
    g (WithIndent t _) = t

emptyStanCode :: StanCode
emptyStanCode = StanCode SBData (Array.listArray (minBound, maxBound) $ repeat (WithIndent mempty 2))

data JSONSeriesFold row where
  JSONSeriesFold :: Stan.StanJSONF row Aeson.Series -> JSONSeriesFold row

instance Semigroup (JSONSeriesFold row) where
  (JSONSeriesFold a) <> (JSONSeriesFold b) = JSONSeriesFold (a <> b)

instance Monoid (JSONSeriesFold row) where
  mempty = JSONSeriesFold $ pure mempty

-- f is existential here.  We supply the choice when we *construct* a ToFoldable
data ToFoldable d row where
  ToFoldable :: Foldable f => (d -> f row) -> ToFoldable d row

applyToFoldable :: Foldl.Fold row a -> ToFoldable d row -> d -> a
applyToFoldable fld (ToFoldable f) = Foldl.fold fld . f

applyToFoldableM :: Monad m => Foldl.FoldM m row a -> ToFoldable d row -> d -> m a
applyToFoldableM fldM (ToFoldable f) = Foldl.foldM fldM . f

{-
data ToEFoldable d row where
  ToEFoldable :: Foldable f => (d -> Either Text (f row)) -> ToEFoldable d row
-}

data GroupTypeTag k where
  GroupTypeTag :: Typeable k => Text -> GroupTypeTag k

taggedGroupName :: GroupTypeTag k -> Text
taggedGroupName (GroupTypeTag n) = n

instance GADT.GEq GroupTypeTag where
  geq gta@(GroupTypeTag n1) gtb@(GroupTypeTag n2) =
    case Reflection.eqTypeRep (Reflection.typeOf gta) (Reflection.typeOf gtb) of
      Just Reflection.HRefl -> if n1 == n2 then Just Reflection.Refl else Nothing
      _ -> Nothing

instance Hashable.Hashable (Some.Some GroupTypeTag) where
  hash (Some.Some (GroupTypeTag n)) = Hashable.hash n
  hashWithSalt m (Some.Some (GroupTypeTag n)) = hashWithSalt m n

data IntIndex row = IntIndex { i_Size :: Int, i_Index :: row -> Either Text Int }

data MakeIndex r k where
  GivenIndex :: Ord k => (Map k Int) -> (r -> k) -> MakeIndex r k
  FoldToIndex :: Ord k =>  (Foldl.Fold r (Map k Int)) -> (r -> k) -> MakeIndex r k

data IndexMap r k = IndexMap (IntIndex r) (k -> Either Text Int) (IntMap.IntMap k)

mapLookupE :: Ord k => (k -> Text) -> Map k a -> k -> Either Text a
mapLookupE errMsg m k = case Map.lookup k m of
  Just a -> Right a
  Nothing -> Left $ errMsg k

toIntMap :: Ord k => Map k Int -> IntMap k
toIntMap = IntMap.fromList . fmap (\(a, b) -> (b, a)) . Map.toList

mapToIndexMap :: Ord k => (r -> k) -> Map k Int -> IndexMap r k
mapToIndexMap h m = indexMap where
  lookupK = mapLookupE (const "key not found when building given index") m
  intIndex = IntIndex (Map.size m) (lookupK . h)
  indexMap = IndexMap intIndex lookupK (toIntMap m)

makeIndexMapF :: MakeIndex r k -> Foldl.Fold r (IndexMap r k)
makeIndexMapF (GivenIndex m h) = pure $ mapToIndexMap h m
makeIndexMapF (FoldToIndex fld h) = fmap (mapToIndexMap h) fld

makeIndexFromEnum :: forall k r.(Enum k, Bounded k, Ord k) => (r -> k) -> MakeIndex r k
makeIndexFromEnum h = GivenIndex m h where
  allKs = [minBound..maxBound]
  m = Map.fromList $ zip allKs [1..]

makeIndexFromFoldable :: (Foldable f, Ord k) => (k -> Text) -> (r -> k) -> f k -> MakeIndex r k
makeIndexFromFoldable _ h allKs = GivenIndex asMap h where
  listKs = ordNub $ Foldl.fold Foldl.list allKs
  asMap = Map.fromList $ zip listKs [1..]
{-
  size = Map.size asMap
  index k = case Map.lookup k asMap of
    Just n -> Right n
    Nothing -> Left $ "Index lookup failed for k=" <> printK k <> " in index built from foldable collection of k."
-}

makeIndexByCounting :: Ord k => (k -> Text) -> (r -> k) -> MakeIndex r k
makeIndexByCounting printK h = FoldToIndex (Foldl.premap h $ indexFold printK 1) h

indexFold :: Ord k => (k -> Text) -> Int -> Foldl.Fold k (Map k Int)
indexFold printK start =  Foldl.Fold step init done where
  step s k = Set.insert k s
  init = Set.empty
  done s = mapToInt where
    keyedList = zip (Set.toList s) [start..]
    mapToInt = Map.fromList keyedList
--    toIntE k = maybe (Left $ "Lookup failed for \"" <> printK k <> "\"") Right $ Map.lookup k mapToInt

type GroupIndexMakerDHM r = DHash.DHashMap GroupTypeTag (MakeIndex r)
type GroupIndexDHM r = DHash.DHashMap GroupTypeTag (IndexMap r)

-- For post-stratification
newtype RowMap r k = RowMap (r -> k)
type GroupRowMap r = DHash.DHashMap GroupTypeTag (RowMap r)

emptyGroupRowMap :: GroupRowMap r
emptyGroupRowMap = DHash.empty

addRowMap :: Typeable k => Text -> (r -> k) -> GroupRowMap r -> GroupRowMap r
addRowMap t f grm = DHash.insert (GroupTypeTag t) (RowMap f) grm

data DataToIntMap d r k = DataToIntMap (ToFoldable d r -> d -> Either Text (IntMap k))
newtype GroupIntMapBuilders d r = GroupIntMapBuilders (DHash.DHashMap GroupTypeTag (DataToIntMap d r))
type DataSetGroupIntMapBuilders d = DHash.DHashMap RowTypeTag (GroupIntMapBuilders d)

newtype GroupIntMaps r = GroupIntMaps (DHash.DHashMap GroupTypeTag IntMap.IntMap)
type DataSetGroupIntMaps = DHash.DHashMap RowTypeTag GroupIntMaps

getGroupIndex :: forall d r k. Typeable k
              => RowTypeTag r
              -> GroupTypeTag k
              -> DataSetGroupIntMaps
              -> Either Text (IntMap k)
getGroupIndex rtt gtt groupIndexes =
  case DHash.lookup rtt groupIndexes of
    Nothing -> Left $ "\"" <> dsName rtt <> "\" not found in data-set group int maps."
    Just (GroupIntMaps gim) -> case DHash.lookup gtt gim of
      Nothing -> Left $ "\"" <> taggedGroupName gtt <> "\" not found in Group int maps for data-set \"" <> dsName rtt <> "\""
      Just im -> Right im

emptyIntMapBuilders :: DataSetGroupIntMapBuilders d
emptyIntMapBuilders = DHash.empty

addIntMapBuilder :: forall d r k. (Typeable k)
                 => RowTypeTag r
                 -> GroupTypeTag k
                 -> (ToFoldable d r -> d -> Either Text (IntMap.IntMap k))
                 -> DataSetGroupIntMapBuilders d
                 -> Either Text (DataSetGroupIntMapBuilders d)
addIntMapBuilder rtt gtt imF dsgimb =
  case DHash.lookup rtt dsgimb of
    Nothing -> Right $ DHash.insert rtt (GroupIntMapBuilders $ DHash.singleton gtt (DataToIntMap imF)) dsgimb
    Just (GroupIntMapBuilders gimb) -> case DHash.lookup gtt gimb of
      Nothing -> Right $ DHash.insert rtt (GroupIntMapBuilders $ DHash.insert gtt (DataToIntMap imF) gimb) dsgimb
      Just _ -> Left $ taggedGroupName gtt <> " already has an index for data-set \"" <> dsName rtt <> "\""


buildIntMaps :: forall d r0. RowBuilderDHM d r0 -> DataSetGroupIntMapBuilders d -> d -> Either Text DataSetGroupIntMaps
buildIntMaps rowBuilders intMapBldrs d = DHash.traverseWithKey f intMapBldrs where
  f :: RowTypeTag r -> GroupIntMapBuilders d r -> Either Text (GroupIntMaps r)
  f rtt (GroupIntMapBuilders gimb) = case DHash.lookup rtt rowBuilders of
    Nothing -> Left $ "Data-set \"" <> dsName rtt <> "\" not found in row builders.  Maybe you forgot to add it?"
    Just (RowInfo tf _ _) -> GroupIntMaps <$> DHash.traverse (g tf) gimb
  g :: ToFoldable d r -> DataToIntMap d r k -> Either Text (IntMap k)
  g tf (DataToIntMap h) = h tf d

makeMainIndexes :: Foldable f => GroupIndexMakerDHM r -> f r -> (GroupIndexDHM r, Map Text (IntIndex r))
makeMainIndexes makerMap rs = (dhm, gm) where
  dhmF = DHash.traverse makeIndexMapF makerMap -- so we only fold once to make all the indices
  dhm = Foldl.fold dhmF rs
  namedIntIndex (GroupTypeTag n DSum.:=> IndexMap ii _ _) = (n, ii)
  gm = Map.fromList $ namedIntIndex <$> DHash.toList dhm


--modeledIndexIntMap :: GroupTypeTag k ->

groupIntMap :: IndexMap r k -> IntMap.IntMap k
groupIntMap (IndexMap _ _ im) = im

data RowInfo d r0 r = RowInfo { toFoldable :: ToFoldable d r
                              , jsonSeries :: JSONSeriesFold r
                              , indexMaker :: GroupIndexDHM r0 -> Either Text (r -> Either Text Int) }

indexedRowInfo :: forall d r0 r k. Typeable k => ToFoldable d r -> Text -> (r -> k) -> RowInfo d r0 r
indexedRowInfo tf groupName keyF = RowInfo tf mempty im where
  im :: GroupIndexDHM r0 -> Either Text (r -> Either Text Int)
  im gim = case DHash.lookup (GroupTypeTag @k groupName) gim of
             Nothing -> Left $ "indexedRowInfo: lookup of group=" <> groupName <> " failed."
             Just (IndexMap (IntIndex _ h) g _) -> Right (g . keyF)


unIndexedRowInfo :: forall d r0 r. ToFoldable d r -> Text -> RowInfo d r0 r
unIndexedRowInfo tf groupName  = RowInfo tf mempty (const $ Left $ groupName <> " is unindexed.")

  --emptyRowInfo :: RowInfo r
--emptyRowInfo = RowInfo mempty mempty

-- key for dependepent map.
data RowTypeTag r where
  ModeledRowTag :: Typeable r => RowTypeTag r
  RowTypeTag :: Typeable r => Text -> RowTypeTag r

modeledDataIndexName :: Text
modeledDataIndexName = "modeled"

dsName :: RowTypeTag r -> Text
dsName ModeledRowTag = modeledDataIndexName
dsName (RowTypeTag n) = n

dsSuffix :: RowTypeTag r -> Text
dsSuffix ModeledRowTag = ""
dsSuffix (RowTypeTag n) = n


-- we need the empty constructors here to bring in the Typeable constraints in the GADT
instance GADT.GEq RowTypeTag where
  geq a@(ModeledRowTag) b@(ModeledRowTag) =
    case Reflection.eqTypeRep (Reflection.typeOf a) (Reflection.typeOf b) of
       Just Reflection.HRefl -> Just Reflection.Refl
  geq ModeledRowTag (RowTypeTag _) = Nothing
  geq rta@(RowTypeTag n1) rtb@(RowTypeTag n2) =
    case Reflection.eqTypeRep (Reflection.typeOf rta) (Reflection.typeOf rtb) of
      Just Reflection.HRefl -> if (n1 == n2) then Just Reflection.Refl else Nothing
      _ -> Nothing

instance Hashable.Hashable (Some.Some RowTypeTag) where
  hash (Some.Some ModeledRowTag) = Hashable.hash ("modeled" :: Text)
  hash (Some.Some (RowTypeTag n)) = Hashable.hash n
  hashWithSalt m (Some.Some ModeledRowTag) = hashWithSalt m ("modeled" :: Text)
  hashWithSalt m (Some.Some (RowTypeTag n)) = hashWithSalt m n

-- the key is a name for the data-set.  The tag carries the toDataSet function
type RowBuilder d r0 = DSum.DSum RowTypeTag (RowInfo d r0)
type RowBuilderDHM d r0 = DHash.DHashMap RowTypeTag (RowInfo d r0)

initialRowBuilders :: (Typeable d, Typeable row, Foldable f) => (d -> f row) -> RowBuilderDHM d r0
initialRowBuilders toModeled =
  DHash.fromList [ModeledRowTag DSum.:=> (RowInfo (ToFoldable toModeled) mempty (const $ Left $ "Modeled data set needs no index but one was asked for."))]

addFoldToDBuilder :: forall d r0 r.(Typeable d)
                  => RowTypeTag r
                  -> Stan.StanJSONF r Aeson.Series
                  -> RowBuilderDHM d r0
                  -> Maybe (RowBuilderDHM d r0)
addFoldToDBuilder rtt fld rbm =
  case DHash.lookup rtt rbm of
    Nothing -> Nothing --DHash.insert rtt (RowInfo (JSONSeriesFold fld) (const Nothing)) rbm
    Just (RowInfo tf (JSONSeriesFold fld') gi)
      -> Just $ DHash.insert rtt (RowInfo tf (JSONSeriesFold $ fld' <> fld) gi) rbm

buildJSONSeries :: forall d r0 f. RowBuilderDHM d r0 -> d -> Either Text Aeson.Series
buildJSONSeries rbm d =
  let foldOne :: RowBuilder d r0 -> Either Text Aeson.Series
      foldOne ((RowTypeTag _ ) DSum.:=> (RowInfo (ToFoldable h) (JSONSeriesFold fld) _)) = Foldl.foldM fld (h d)
  in mconcat <$> (traverse foldOne $ DHash.toList rbm)

data JSONRowFold d r = JSONRowFold (ToFoldable d r) (Stan.StanJSONF r Aeson.Series)

buildJSONFromRows :: d -> DHash.DHashMap RowTypeTag (JSONRowFold d) -> Either Text Aeson.Series
buildJSONFromRows d rowFoldMap = do
  let toSeriesOne (rtt DSum.:=> JSONRowFold (ToFoldable tf) fld) = Foldl.foldM fld (tf d)
  fmap mconcat $ traverse toSeriesOne $ DHash.toList rowFoldMap

addGroupIndexes :: (Typeable d, Typeable r0) => StanBuilderM env d r0 ()
addGroupIndexes = do
  let buildIndexJSONFold :: (Typeable d, Typeable r0) => DSum.DSum GroupTypeTag (IndexMap r0) -> StanBuilderM env d r0 ()
      buildIndexJSONFold ((GroupTypeTag gName) DSum.:=> (IndexMap (IntIndex gSize mIntF) _ _)) = do
        addFixedIntJson' ("N_" <> gName) (Just 2) gSize
        _ <- addColumnMJson ModeledRowTag gName "" (SME.StanArray [SME.NamedDim "N"] SME.StanInt) "<lower=1>" mIntF
        return ()
  gim <- asks $ groupIndexByType . groupEnv
  traverse_ buildIndexJSONFold $ DHash.toList gim

buildJSONF :: forall env d r0 .(Typeable d, Typeable r0) => StanBuilderM env d r0 (DHash.DHashMap RowTypeTag (JSONRowFold d))
buildJSONF = do
  gim <- asks $ groupIndexByType . groupEnv
  rbs <- rowBuilders <$> get
  let buildRowJSONFolds :: RowInfo d r0 r -> StanBuilderM env d r0 (JSONRowFold d r)
      buildRowJSONFolds (RowInfo (ToFoldable toData) (JSONSeriesFold jsonF) getIndexF) = do
        case getIndexF gim of
          Right rowIndexF -> do
--            let intMapF = Foldl.FoldM (\im r -> fmap (\n -> IntMap.insert n r im) $ rowIndexF r) (return IntMap.empty) return
            -- FE data can have rows which are not in model.  We just ignore them
            let intMapF = Foldl.Fold (\im r -> either (const im) (\n -> IntMap.insert n r im) $ rowIndexF r) IntMap.empty id
            return $ JSONRowFold (ToFoldable $ Foldl.fold intMapF . toData) jsonF
          Left _ -> return $ JSONRowFold (ToFoldable toData) jsonF
  DHash.traverse buildRowJSONFolds rbs

data BuilderState d r0 = BuilderState { declaredVars :: !(Map SME.StanName SME.StanType)
                                      , rowBuilders :: !(RowBuilderDHM d r0)
                                      , hasFunctions :: !(Set.Set Text)
                                      , code :: !StanCode
                                      , indexBuilders :: !(DataSetGroupIntMapBuilders d)
                                      }
modifyRowBuildersA :: Applicative t => (RowBuilderDHM d r -> t (RowBuilderDHM d r)) -> BuilderState d r -> t (BuilderState d r)
modifyRowBuildersA f (BuilderState dv rb hf c ib) = (\x -> BuilderState dv x hf c ib) <$> f rb

modifyIndexBuildersA :: Applicative t => (DataSetGroupIntMapBuilders d -> t (DataSetGroupIntMapBuilders d)) -> BuilderState d r -> t (BuilderState d r)
modifyIndexBuildersA f (BuilderState dv rb hf c ib) = (\x -> BuilderState dv rb hf  c x) <$> f ib

initialBuilderState :: (Typeable d, Typeable r, Foldable f) => (d -> f r) -> BuilderState d r
initialBuilderState toModeled = BuilderState Map.empty (initialRowBuilders toModeled) Set.empty emptyStanCode emptyIntMapBuilders

newtype StanGroupBuilderM r0 a
  = StanGroupBuilderM { unStanGroupBuilderM :: ExceptT Text (State (GroupIndexMakerDHM r0)) a }
  deriving (Functor, Applicative, Monad, MonadState (GroupIndexMakerDHM r0))

data GroupEnv r0 = GroupEnv { groupIndexByType :: GroupIndexDHM r0
                            , groupIndexByName :: Map Text (IntIndex r0)
                            }
stanGroupBuildError :: Text -> StanGroupBuilderM r0 a
stanGroupBuildError t = StanGroupBuilderM $ ExceptT (pure $ Left t)

addGroup :: forall r k.Typeable k => Text -> MakeIndex r k -> StanGroupBuilderM r ()
addGroup gName mkIndex = do
  let gtt = GroupTypeTag @k gName
  gim <- get
  case DHash.lookup gtt gim of
    Nothing -> put $ DHash.insert gtt mkIndex gim
    Just _ -> stanGroupBuildError $ "Attempt to add a second group (\"" <> gName <> "\") at the same type as one already in group index builder map"

runStanGroupBuilder :: Foldable f => StanGroupBuilderM r () -> f r -> Either Text (GroupEnv r)
runStanGroupBuilder sgb rs =
  let (resE, gmi) = flip runState DHash.empty $ runExceptT $ unStanGroupBuilderM sgb
      (byType, byName) = makeMainIndexes gmi rs
  in fmap (const $ GroupEnv byType byName) resE

data StanEnv env r0 = StanEnv { userEnv :: !env, groupEnv :: !(GroupEnv r0) }

newtype StanBuilderM env d r0 a = StanBuilderM { unStanBuilderM :: ExceptT Text (ReaderT (StanEnv env r0) (State (BuilderState d r0))) a }
                                deriving (Functor, Applicative, Monad, MonadReader (StanEnv env r0), MonadState (BuilderState d r0))

askUserEnv :: StanBuilderM env d r0 env
askUserEnv = asks userEnv

askGroupEnv :: StanBuilderM env d r0 (GroupEnv r0)
askGroupEnv = asks groupEnv

addFunctionsOnce :: Text -> StanBuilderM env d r0 () -> StanBuilderM env d r0 ()
addFunctionsOnce functionsName fCode = do
  (BuilderState vars rowBuilders fsNames code ims) <- get
  case Set.member functionsName fsNames of
    True -> return ()
    False -> do
      let newFunctionNames = Set.insert functionsName fsNames
      fCode
      put (BuilderState vars rowBuilders newFunctionNames code ims)

addIndexedDataSet :: (Typeable r, Typeable d, Typeable k)
                  => Text
                  -> ToFoldable d r
                  -> (r -> k)
                  -> StanBuilderM env d r0 (RowTypeTag r)
addIndexedDataSet name toFoldable toKey = do
  let rtt = RowTypeTag name
  (BuilderState vars rowBuilders fsNames code ims) <- get
  case DHash.lookup rtt rowBuilders of
    Just _ -> stanBuildError "Attempt to add 2nd data set with same type and name"
    Nothing -> do
      let rowInfo = indexedRowInfo toFoldable name toKey
          newRowBuilders = DHash.insert rtt rowInfo rowBuilders
      put (BuilderState vars newRowBuilders fsNames code ims)
  return rtt

addUnIndexedDataSet :: (Typeable r, Typeable d)
                    => Text
                    -> ToFoldable d r
                    -> StanBuilderM env d r0 (RowTypeTag r)
addUnIndexedDataSet name toFoldable = do
  let rtt = RowTypeTag name
  (BuilderState vars rowBuilders modelExprs code ims) <- get
  case DHash.lookup rtt rowBuilders of
    Just _ -> stanBuildError "Attempt to add 2nd data set with same type and name"
    Nothing -> do
      let rowInfo = unIndexedRowInfo toFoldable name
          newRowBuilders = DHash.insert rtt rowInfo rowBuilders
      put (BuilderState vars newRowBuilders modelExprs code ims)
  return rtt

addIndexIntMapFld :: forall r k env d r0.
                  RowTypeTag r
                  -> GroupTypeTag k
                  -> (Foldl.FoldM (Either Text) r (IntMap k))
                  -> StanBuilderM env d r0 ()
addIndexIntMapFld rtt gtt imFld = do
  BuilderState vars rowBuilders fsNames code ibs <- get
  let   f :: ToFoldable d r -> d -> Either Text (IntMap k)
        f tf d = applyToFoldableM imFld tf d
  case gtt of
    GroupTypeTag _ ->
      case addIntMapBuilder rtt gtt f ibs of
        Left errMsg -> stanBuildError errMsg
        Right ibs' -> put $  BuilderState vars rowBuilders fsNames code ibs'


addIndexIntMap :: forall r k env d r0.
               RowTypeTag r
               -> GroupTypeTag k
               -> IntMap k
               -> StanBuilderM env d r0 ()
addIndexIntMap rtt gtt im = do
  BuilderState vars rowBuilders fsNames code ibs <- get
  let   f :: ToFoldable d r -> d -> Either Text (IntMap k)
        f _ _ = Right im
  case gtt of -- bring the Typeable constraint from the GroupTypeTag GADT into scope
    GroupTypeTag _ ->
      case addIntMapBuilder rtt gtt f ibs of
        Left errMsg -> stanBuildError errMsg
        Right ibs' -> put $  BuilderState vars rowBuilders fsNames code ibs'


runStanBuilder' :: forall f env d r0 a. (Typeable d, Typeable r0, Foldable f)
               => env -> GroupEnv r0 -> (d -> f r0) -> StanBuilderM env d r0 a -> Either Text (BuilderState d r0, a)
runStanBuilder' userEnv ge toModeled sb = res where
  (resE, bs) = flip runState (initialBuilderState toModeled) . flip runReaderT (StanEnv userEnv ge) . runExceptT $ unStanBuilderM sbWithIntMaps
  res = fmap (bs,) resE
  sbWithIntMaps :: StanBuilderM env d r0 a
  sbWithIntMaps = do
    let f :: (DHash.DSum GroupTypeTag (IndexMap r0)) -> StanBuilderM env d r0 ()
        f (gtt DSum.:=> IndexMap _ _ im) =  addIndexIntMap (ModeledRowTag @r0) gtt im
    traverse_ f $ DHash.toList $ groupIndexByType ge
    sb

runStanBuilder :: (Foldable f, Typeable d, Typeable r0)
               => d -> (d -> f r0) -> env -> StanGroupBuilderM r0 () -> StanBuilderM env d r0 a -> Either Text (BuilderState d r0, a)
runStanBuilder d toModeled userEnv sgb sb =
  let eGE = runStanGroupBuilder sgb (toModeled d)
  in case eGE of
    Left err -> Left $ "Error running group builder: " <> err
    Right ge -> runStanBuilder' userEnv ge toModeled sb

stanBuildError :: Text -> StanBuilderM env d r0 a
stanBuildError t = StanBuilderM $ ExceptT (pure $ Left t)

stanBuildMaybe :: Text -> Maybe a -> StanBuilderM env d r0 a
stanBuildMaybe msg = maybe (stanBuildError msg) return

stanBuildEither :: Either Text a -> StanBuilderM ev d r0 a
stanBuildEither = either stanBuildError return

declare :: SME.StanName -> SME.StanType -> StanBuilderM env d r0 ()
declare sn st = do
  (BuilderState vars rowBuilders modelExprs code ims) <- get
  case Map.lookup sn vars of
    Nothing -> put $ BuilderState (Map.insert sn st vars) rowBuilders modelExprs code ims
    Just dt -> if dt == st
               then stanBuildError $ "Attempt to re-declare " <> show sn <> " (" <> show dt <> ") with same type."
               else stanBuildError $ "Attempt to re-declare " <> show sn <> " (" <> show dt <> ") with new type (" <> show st <> ")!"

stanDeclare' :: SME.StanName -> SME.StanType -> Text -> Maybe Text -> StanBuilderM env d r SME.StanVar
stanDeclare' sn st sc mRHS = do
  declare sn st
  let dimsToText x = "[" <> T.intercalate ", " (SME.dimToText <$> x) <> "]"
  let typeToCode :: SME.StanName -> SME.StanType -> Text -> Text
      typeToCode sn st sc = case st of
        SME.StanInt -> "int" <> sc <> " " <> sn
        SME.StanReal -> "real" <> sc <> " " <> sn
        SME.StanArray dims st' -> case st' of
          SME.StanArray iDims st'' -> typeToCode sn (SME.StanArray (dims ++ iDims) st'') sc
          _ -> typeToCode sn st' sc <> dimsToText dims
        SME.StanVector dim -> "vector" <> sc <> "[" <> SME.dimToText dim <> "] " <> sn
        SME.StanMatrix (dimR, dimC) -> "matrix" <> sc <> "[" <> SME.dimToText dimR <> "," <> SME.dimToText dimC <> "] " <> sn
  let dimsToCheck st = case st of
        SME.StanVector d -> [d]
        SME.StanMatrix (d1, d2) -> [d1, d2]
        SME.StanArray dims st' -> dimsToCheck st' ++ dims
        _ -> []
  let checkDim d = case d of
        SME.NamedDim t -> checkName t
        SME.GivenDim _ -> return ()
  traverse_ checkDim $ dimsToCheck st
  case mRHS of
    Nothing -> addStanLine $ typeToCode sn st sc
    Just rhs -> addStanLine $ typeToCode sn st sc <> " = " <> rhs
  return $ SME.StanVar sn st

stanDeclare :: SME.StanName -> SME.StanType -> Text -> StanBuilderM env d r0 SME.StanVar
stanDeclare sn st sc = stanDeclare' sn st sc Nothing

stanDeclareRHS :: SME.StanName -> SME.StanType -> Text -> Text -> StanBuilderM env d r0 SME.StanVar
stanDeclareRHS sn st sc rhs = stanDeclare' sn st sc (Just rhs)


checkName :: SME.StanName -> StanBuilderM env d r0 ()
checkName sn = do
  declared <- declaredVars <$> get
  case Map.lookup sn declared of
    Nothing -> stanBuildError $ "name check failed for " <> show sn <> ". Missing declaration?"
    _ -> return ()

typeForName :: SME.StanName -> StanBuilderM env d r0 SME.StanType
typeForName sn = do
  declared <- declaredVars <$> get
  case Map.lookup sn declared of
    Nothing -> stanBuildError $ "type for name failed for " <> show sn <> ". Missing declaration?"
    Just x -> return x


isDeclared :: SME.StanName -> StanBuilderM env d r0 Bool
isDeclared sn  = do
  declared <- declaredVars <$> get
  case Map.lookup sn declared of
    Nothing -> return False
    Just _  -> return True

addJson :: (Typeable d)
        => RowTypeTag r
        -> SME.StanName
        -> SME.StanType
        -> Text
        -> Stan.StanJSONF r Aeson.Series
        -> StanBuilderM env d r0 SME.StanVar
addJson rtt name st sc fld = do
  inBlock SBData $ stanDeclare name st sc
  (BuilderState declared rowBuilders modelExprs code ims) <- get
--  when (Set.member name un) $ stanBuildError $ "Duplicate name in json builders: \"" <> name <> "\""
  newRowBuilders <- case addFoldToDBuilder rtt fld rowBuilders of
    Nothing -> stanBuildError $ "Attempt to add Json to an uninitialized dataset (" <> dsName rtt <> ")"
    Just x -> return x
  put $ BuilderState declared newRowBuilders modelExprs code ims
  return $ SME.StanVar name st

-- things like lengths may often be re-added
-- maybe work on a cleaner way...
addJsonUnchecked :: (Typeable d)
                 => RowTypeTag r
                 -> SME.StanName
                 -> SME.StanType
                 -> Text
                 -> Stan.StanJSONF r Aeson.Series
                 -> StanBuilderM env d r0 SME.StanVar
addJsonUnchecked rtt name st sc fld = do
  alreadyDeclared <- isDeclared name
  if not alreadyDeclared
    then addJson rtt name st sc fld
    else return $ SME.StanVar name st

addFixedIntJson :: forall r0 d env. (Typeable r0, Typeable d) => Text -> Maybe Int -> Int -> StanBuilderM env d r0 SME.StanVar
addFixedIntJson name mLower n = addJson (ModeledRowTag @r0) name SME.StanInt sc (Stan.constDataF name n) where
  sc = maybe "" (\l -> "<lower=" <> show l <> ">") $ mLower

addFixedIntJson' :: forall r0 d env. (Typeable r0, Typeable d) => Text -> Maybe Int -> Int -> StanBuilderM env d r0 SME.StanVar
addFixedIntJson' name mLower n = addJsonUnchecked (ModeledRowTag @r0) name SME.StanInt sc (Stan.constDataF name n) where
  sc = maybe "" (\l -> "<lower=" <> show l <> ">") $ mLower

-- These get re-added each time something adds a column built from the data-set.
-- But we only need it once per data set.
addLengthJson :: (Typeable d) => RowTypeTag r -> Text -> StanBuilderM env d r0 SME.StanVar
addLengthJson rtt name = addJsonUnchecked rtt name SME.StanInt "<lower=1>" (Stan.namedF name Foldl.length)

nameSuffixMsg :: SME.StanName -> Text -> Text
nameSuffixMsg n s = "name=\"" <> show n <> "\" suffix=\"" <> s <> "\""

addColumnJson :: (Typeable d, Aeson.ToJSON x)
              => RowTypeTag r
              -> Text
              -> Text
              -> SME.StanType
              -> Text
              -> (r -> x) -> StanBuilderM env d r0 SME.StanVar
addColumnJson rtt name suffix st sc toX = do
  case st of
    SME.StanInt -> stanBuildError $ "SME.StanInt (scalar) given as type in addColumnJson. " <> nameSuffixMsg name suffix
    SME.StanReal -> stanBuildError $ "SME.StanReal (scalar) given as type in addColumnJson. " <> nameSuffixMsg name suffix
    _ -> return ()
  addLengthJson rtt ("N" <> underscoredIf suffix)
  let fullName = name <> underscoredIf suffix
  addJson rtt fullName st sc (Stan.valueToPairF fullName $ Stan.jsonArrayF toX)

addColumnMJson :: (Typeable d, Typeable r, Aeson.ToJSON x)
              => RowTypeTag r
               -> Text
               -> Text
               -> SME.StanType
               -> Text
               -> (r -> Either Text x)
               -> StanBuilderM env d r0 SME.StanVar
addColumnMJson rtt name suffix st sc toMX = do
  case st of
    SME.StanInt -> stanBuildError $ "SME.StanInt (scalar) given as type in addColumnJson. " <> nameSuffixMsg name suffix
    SME.StanReal -> stanBuildError $ "SME.StanReal (scalar) given as type in addColumnJson. " <> nameSuffixMsg name suffix
    _ -> return ()
  addLengthJson rtt ("N" <> underscoredIf suffix)
  let fullName = name <> underscoredIf suffix
  addJson rtt fullName st sc (Stan.valueToPairF fullName $ Stan.jsonArrayEF toMX)


add2dMatrixJson :: (Typeable d, Typeable r0)
                => RowTypeTag r
                -> Text
                -> Text
                -> Text
                -> SME.StanDim
                -> Int
                -> (r -> Vector.Vector Double) -> StanBuilderM env d r0 SME.StanVar
add2dMatrixJson rtt name suffix sc rowDim cols vecF = do
  let colName = "K" <> underscoredIf suffix
  addFixedIntJson colName Nothing cols
  addColumnJson rtt name suffix (SME.StanMatrix (rowDim, SME.NamedDim colName)) sc vecF

modifyCode' :: (StanCode -> StanCode) -> BuilderState d r -> BuilderState d r
modifyCode' f bs = let newCode = f $ code bs in bs { code = newCode }

modifyCode :: (StanCode -> StanCode) -> StanBuilderM env d r ()
modifyCode f = modify $ modifyCode' f

setBlock' :: StanBlock -> StanCode -> StanCode
setBlock' b (StanCode _ blocks) = StanCode b blocks

setBlock :: StanBlock -> StanBuilderM env d r ()
setBlock = modifyCode . setBlock'

getBlock :: StanBuilderM env d r StanBlock
getBlock = do
  (BuilderState _ _ _ (StanCode b _) _) <- get
  return b

addLine' :: Text -> StanCode -> StanCode
addLine' t (StanCode b blocks) =
  let (WithIndent curCode curIndent) = blocks Array.! b
      newCode = curCode <> T.replicate curIndent " " <> t
  in StanCode b (blocks Array.// [(b, WithIndent newCode curIndent)])

addLine :: Text -> StanBuilderM env d r ()
addLine = modifyCode . addLine'

addStanLine :: Text -> StanBuilderM env d r ()
addStanLine t = addLine $ t <> ";\n"

declareStanFunction :: Text -> StanBuilderM env d r a -> StanBuilderM env d r a
declareStanFunction declaration body = inBlock SBFunctions $ do
  addLine declaration
  bracketed 2 body

indent :: Int -> StanCode -> StanCode
indent n (StanCode b blocks) =
  let (WithIndent curCode curIndent) = blocks Array.! b
  in StanCode b (blocks Array.// [(b, WithIndent curCode (curIndent + n))])

-- code inserted here will be indented n extra spaces
indented :: Int -> StanBuilderM env d r a -> StanBuilderM env d r a
indented n m = do
  modifyCode (indent n)
  x <- m
  modifyCode (indent $ negate n)
  return x

bracketed :: Int -> StanBuilderM env d r a -> StanBuilderM env d r a
bracketed n m = do
  addLine " {\n"
  x <- indented n m
  addLine "}\n"
  return x

stanForLoop :: Text -> Maybe Text -> Text -> (Text -> StanBuilderM env d r a) -> StanBuilderM env d r a
stanForLoop counter mStart end loopF = do
  let start = fromMaybe "1" mStart
  addLine $ "for (" <> counter <> " in " <> start <> ":" <> end <> ")"
  bracketed 2 $ loopF counter

data StanPrintable = StanLiteral Text | StanExpression Text

stanPrint :: [StanPrintable] -> StanBuilderM env d r ()
stanPrint ps =
  let f x = case x of
        StanLiteral x -> "\"" <> x <> "\""
        StanExpression x -> x
  in addStanLine $ "print(" <> T.intercalate ", " (fmap f ps) <> ")"

underscoredIf :: Text -> Text
underscoredIf t = if T.null t then "" else "_" <> t


stanIndented :: StanBuilderM env d r a -> StanBuilderM env d r a
stanIndented = indented 2

inBlock :: StanBlock -> StanBuilderM env d r a -> StanBuilderM env d r a
inBlock b m = do
  oldBlock <- getBlock
  setBlock b
  x <- m
  setBlock oldBlock
  return x


printExprM :: Text -> SME.VarBindingStore -> StanBuilderM env d r SME.StanExpr -> StanBuilderM env d r Text
printExprM context vbs me = do
  e <- me
  case SME.printExpr vbs e of
    Right t -> return t
    Left err -> stanBuildError err

{-
data StanPrior = NormalPrior Double Double | CauchyPrior Double Double | NoPrior
priorText :: StanPrior -> Text
priorText (NormalPrior m s) = "normal(m, s)"
stanPrior :: SME.StanName -> StanPrior -> StanBuilderM env d r ()
stanPrior sn sp = do
  st <- typeForName sn

  case
-}

stanModelAsText :: GeneratedQuantities -> StanModel -> T.Text
stanModelAsText gq sm =
  let section h b = h <> " {\n" <> b <> "}\n"
      maybeSection h = maybe "" (section h)
   in maybeSection "functions" (functionsBlock sm)
      <> section "data" (dataBlock sm)
      <> maybeSection "transformed data" (transformedDataBlockM sm)
      <> section "parameters" (parametersBlock sm)
      <> maybeSection "transformed parameters" (transformedParametersBlockM sm)
      <> section "model" (modelBlock sm)
      <> case gq of
           NoLL -> maybeSection "generated quantities" (generatedQuantitiesBlockM sm)
           OnlyLL -> section "generated quantities" (genLogLikelihoodBlock sm)
           All ->
             section "generated quantities"
             $ fromMaybe "" (generatedQuantitiesBlockM sm)
             <> "\n"
             <> genLogLikelihoodBlock sm
             <> "\n"

modelFile :: T.Text -> T.Text
modelFile modelNameT = modelNameT <> ".stan"

-- The file is either not there, there but the same, or there but different so we
-- need an available file name to proceed
data ModelState = New | Same | Updated T.Text deriving (Show)

renameAndWriteIfNotSame :: GeneratedQuantities -> StanModel -> T.Text -> T.Text -> IO ModelState
renameAndWriteIfNotSame gq m modelDir modelName = do
  let fileName d n = T.unpack $ d <> "/" <> n <> ".stan"
      curFile = fileName modelDir modelName
      findAvailableName modelDir modelName n = do
        let newName = fileName modelDir (modelName <> "_o" <> T.pack (show n))
        newExists <- Dir.doesFileExist newName
        if newExists then findAvailableName modelDir modelName (n + 1) else return $ T.pack newName
      newModel = stanModelAsText gq m
  exists <- Dir.doesFileExist curFile
  if exists then (do
    extant <- T.readFile curFile
    if extant == newModel then Say.say ("model file:" <> toText curFile <> " exists and is identical to model.") >> return Same else (do
      Say.say $ "model file:" <> T.pack curFile <> " exists and is different. Renaming and writing new model to file."
      newName <- findAvailableName modelDir modelName 1
      Dir.renameFile (fileName modelDir modelName) (T.unpack newName)
      T.writeFile (fileName modelDir modelName) newModel
      return $ Updated newName)) else (do
    Say.say $ "model file:" <> T.pack curFile <> " doesn't exist.  Writing new."
    T.writeFile (fileName modelDir modelName) newModel
    return New)

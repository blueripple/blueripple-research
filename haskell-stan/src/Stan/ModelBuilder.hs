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
import qualified Data.List.NonEmpty as NE
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
--  SupplementalIndex :: (Ord k, Foldable f) => f k -> MakeIndex r k

data IndexMap r k = IndexMap
                    { rowToGroupIndex :: IntIndex r,
                      groupKeyToGroupIndex :: k -> Either Text Int,
                      groupIndexToGroupKey :: IntMap.IntMap k
                    }

unusedIntIndex :: IntIndex r
unusedIntIndex = IntIndex 0 (const $ Left $ "Attempt to use an unused Int Index.")

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

makeIndexByCounting :: Ord k => (k -> Text) -> (r -> k) -> MakeIndex r k
makeIndexByCounting printK h = FoldToIndex (Foldl.premap h $ indexFold printK 1) h

indexFold :: Ord k => (k -> Text) -> Int -> Foldl.Fold k (Map k Int)
indexFold printK start =  Foldl.Fold step init done where
  step s k = Set.insert k s
  init = Set.empty
  done s = mapToInt where
    keyedList = zip (Set.toList s) [start..]
    mapToInt = Map.fromList keyedList

-- Index makers for one row type
newtype GroupIndexMakers r = GroupIndexMakers (DHash.DHashMap GroupTypeTag (MakeIndex r))
-- Indexes for one row type, made using the IndexMaker in GroupIndexMakerDHM and the rows of r from d
newtype GroupIndexes r = GroupIndexes (DHash.DHashMap GroupTypeTag (IndexMap r))

{-
-- Index makers stored by row type. NB: this has no type parameters.
type RowIndexMakersDHM = DHash.DHashMap RowTypeTag GroupIndexMakerDHM
-- Indexes by row type
type RowIndexesDHM = DHash.DHashMap RowTypeTag GroupIndexDHM
-}

-- For post-stratification
newtype RowMap r k = RowMap (r -> k)
type GroupRowMap r = DHash.DHashMap GroupTypeTag (RowMap r)

emptyGroupRowMap :: GroupRowMap r
emptyGroupRowMap = DHash.empty

addRowMap :: Typeable k => GroupTypeTag k -> (r -> k) -> GroupRowMap r -> GroupRowMap r
addRowMap gtt f grm = DHash.insert gtt (RowMap f) grm

newtype DataToIntMap r k = DataToIntMap { unDataToIntMap :: Foldl.FoldM (Either Text) r (IntMap k) }
newtype GroupIntMapBuilders r = GroupIntMapBuilders (DHash.DHashMap GroupTypeTag (DataToIntMap r))
--type DataSetGroupIntMapBuilders d = DHash.DHashMap RowTypeTag (GroupIntMapBuilders d)

-- r is a Phantom type here
newtype GroupIntMaps r = GroupIntMaps (DHash.DHashMap GroupTypeTag IntMap.IntMap)
type DataSetGroupIntMaps = DHash.DHashMap RowTypeTag GroupIntMaps



data GroupIndexAndIntMapMakers d r = GroupIndexAndIntMapMakers (ToFoldable d r) (GroupIndexMakers r) (GroupIntMapBuilders r)

--data GroupIndexesAndIntMaps d r = GroupIndexesAndIntMaps (ToFoldable d r) (GroupIndexes r) (GroupIntMaps r)

{-
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
-}

-- if it already exists, we ignore.  Which might lead to an error but we sometimes
-- want to post-stratify (or whatever) using the same data-set
-- Actually we error on already exists for now...
{-
addIntMapBuilder :: forall env d r k. (Typeable k)
                 => RowTypeTag r
                 -> GroupTypeTag k
                 -> Foldl.FoldM (Either Text) r (IntMap k)
                 -> StanBuilderM env d ()
addIntMapBuilder rtt gtt imF  = do
  (BuilderState dvs ibs rbs cj hfs c) <- get
  case DHash.lookup rtt rbs of
    Nothing -> stanBuildError
               $ "Adding IntMap builder for group "
               <> taggedGroupName gtt
               <> " to uninitialized data-set ("
               <> dataSetName rtt
               <> ") is not allowed. Initialize first."
    Just (RowInfo tf gims (GroupIntMapBuilders gimbs) js) -> do
      case DHash.lookup gtt gimbs of
        Just _ -> stanBuildError
                  $ "For data-set "
                  <> dataSetName rtt
                  <> " there is already an IntMap Buidler for group "
                  <> taggedGroupName gtt
                  <> "."
        Nothing -> do
          let gimbs' = DHash.insert gtt (DataToIntMap imF) gimbs
              ri' = RowInfo tf gims gimbs' js
              rbs' = DHash.insert rtt ri' rbs
          put $  BuilderState dvs ibs rbs' cj hfs c ibs
-}

{-
buildIntMapsForDataSet :: forall d r. GroupIntMapBuilders r -> d -> Either Text (GroupIntMaps r)
buildIntMapsForDataSet imbs d  = DHash.traverse f imbs where
  f :: DataToIntMap r k -> IntMap.IntMap k
  f (DataToIntMap fldM) = Fold.foldM fldM (tf d)
-}

intMapsForDataSetFoldM :: GroupIntMapBuilders r -> Foldl.FoldM (Either Text) r (GroupIntMaps r)
intMapsForDataSetFoldM (GroupIntMapBuilders imbs) = GroupIntMaps <$> DHash.traverse unDataToIntMap imbs

indexBuildersForDataSetFold :: GroupIndexMakers r -> Foldl.Fold r (GroupIndexes r)
indexBuildersForDataSetFold (GroupIndexMakers gims) = GroupIndexes <$> DHash.traverse makeIndexMapF gims

useBindingsFromGroupIndexMakers :: RowTypeTag r -> GroupIndexMakers r -> Map SME.IndexKey SME.StanExpr
useBindingsFromGroupIndexMakers rtt (GroupIndexMakers gims) = Map.fromList l where
  l = fmap g $ DHash.toList gims
  g (gtt DSum.:=> _) = let gn = taggedGroupName gtt in (gn, SME.indexBy (SME.name gn) $ dataSetName rtt)


-- build a new RowInfo from the row index and IntMap builders
buildGroupIndexesAndIntMaps :: d -> RowTypeTag r -> GroupIndexAndIntMapMakers d r -> Either Text (RowInfo d r)
buildGroupIndexesAndIntMaps d rtt (GroupIndexAndIntMapMakers tf@(ToFoldable f) ims imbs) = Foldl.foldM fldM $ f d  where
  gisFld = indexBuildersForDataSetFold ims
  imsFldM = intMapsForDataSetFoldM imbs
  useBindings = useBindingsFromGroupIndexMakers rtt ims
  fldM = RowInfo tf useBindings <$> Foldl.generalize gisFld <*> imsFldM <*> pure mempty


{-
buildIndexesForDataSet :: forall d r. RowInfo r -> d -> Either Text (GroupIndexes r)
buildIndexesForDataSet (RowInfo tf gims _ _) d = DHash.traverse f gims where
  f :: MakeIndex r k -> IndexMap r k
  f
-}
{-
  GroupIntMapBuilders d r -> Either Text (GroupIntMaps r)
  f rtt (GroupIntMapBuilders gimb) = case DHash.lookup rtt rowBuilders of
    Nothing -> Left $ "Data-set \"" <> dsName rtt <> "\" not found in row builders.  Maybe you forgot to add it?"
    Just (RowInfo tf _ _) -> GroupIntMaps <$> DHash.traverse (g tf) gimb
  g :: ToFoldable d r -> DataToIntMap d r k -> Either Text (IntMap k)
  g tf (DataToIntMap h) = h tf d
-}
{-
makeMainIndexes :: Foldable f => GroupIndexMakerDHM r -> f r -> (GroupIndexDHM r, Map Text (IntIndex r))
makeMainIndexes makerMap rs = (dhm, gm) where
  dhmF = DHash.traverse makeIndexMapF makerMap -- so we only fold once to make all the indices
  dhm = Foldl.fold dhmF rs
  namedIntIndex (GroupTypeTag n DSum.:=> IndexMap mII _ _) = fmap (n,) mII
  gm = Map.fromList $ catMaybes $ namedIntIndex <$> DHash.toList dhm
-}
groupIntMap :: IndexMap r k -> IntMap.IntMap k
groupIntMap (IndexMap _ _ im) = im


{-
data RowInfo d r = RowInfo
                   { toFoldable :: ToFoldable d r
                   , indexesAndIntMaps :: GroupIndexesAndIntMaps r
--                   , groupIndexMakers :: GroupIndexMakerDHM r
--                   , groupIntMapBuilders :: GroupIntMapBuilders d r
                   , jsonSeries :: JSONSeriesFold r
                   }
-}

data RowInfo d r = RowInfo
                   {
                     toFoldable    :: ToFoldable d r
                   , expressionBindings :: Map SME.IndexKey SME.StanExpr
                   , groupIndexes  :: GroupIndexes r
                   , groupIntMaps  :: GroupIntMaps r
                   , jsonSeries    :: JSONSeriesFold r
                   }


dataSetGroupIntMaps :: RowInfos d -> DataSetGroupIntMaps
dataSetGroupIntMaps = DHash.map groupIntMaps

dataSetGroupIntMapsM :: StanBuilderM env d DataSetGroupIntMaps
dataSetGroupIntMapsM = dataSetGroupIntMaps . rowBuilders <$> get

--newRowInfo :: forall d r. ToFoldable d r -> RowInfo d r
--newRowInfo tf groupName  = RowInfo tf mempty mempty

{-
indexedRowInfo :: forall d r0 r k. Typeable k => ToFoldable d r -> Text -> (r -> k) -> RowInfo d r0 r
indexedRowInfo tf groupName keyF = RowInfo tf mempty im where
  im :: GroupIndexDHM r0 -> Either Text (r -> Either Text Int)
  im gim = case DHash.lookup (GroupTypeTag @k groupName) gim of
             Nothing -> Left $ "indexedRowInfo: lookup of group=" <> groupName <> " failed."
             Just (IndexMap _ g _) -> Right (g . keyF)


unIndexedRowInfo :: forall d r0 r. ToFoldable d r -> Text -> RowInfo d r0 r
unIndexedRowInfo tf groupName  = RowInfo tf mempty (const $ Left $ groupName <> " is unindexed.")
-}

-- key for dependepent map.
data RowTypeTag r where
  RowTypeTag :: Typeable r => Text -> RowTypeTag r

dataSetName :: RowTypeTag r -> Text
dataSetName (RowTypeTag n) = n

-- we need the empty constructors here to bring in the Typeable constraints in the GADT
instance GADT.GEq RowTypeTag where
  geq rta@(RowTypeTag n1) rtb@(RowTypeTag n2) =
    case Reflection.eqTypeRep (Reflection.typeOf rta) (Reflection.typeOf rtb) of
      Just Reflection.HRefl -> if (n1 == n2) then Just Reflection.Refl else Nothing
      _ -> Nothing

instance Hashable.Hashable (Some.Some RowTypeTag) where
  hash (Some.Some (RowTypeTag n)) = Hashable.hash n
  hashWithSalt m (Some.Some (RowTypeTag n)) = hashWithSalt m n

-- the key is a name for the data-set.  The tag carries the toDataSet function
type RowBuilder d = DSum.DSum RowTypeTag (RowInfo d)
type RowInfos d = DHash.DHashMap RowTypeTag (RowInfo d)
--type IndexedRowInfos d = DHash.DHashMap RowTypeTag (IndexedRowInfo d)


{-
initialRowBuilders :: (Typeable d, Typeable row, Foldable f) => (d -> f row) -> RowBuilderDHM d
initialRowBuilders toModeled =
  DHash.fromList [ModeledRowTag DSum.:=> (RowInfo (ToFoldable toModeled) mempty (const $ Left $ "Modeled data set needs no index but one was asked for."))]
-}


addFoldToDBuilder :: forall d r.(Typeable d)
                  => RowTypeTag r
                  -> Stan.StanJSONF r Aeson.Series
                  -> RowInfos d
                  -> Maybe (RowInfos d)
addFoldToDBuilder rtt fld ris =
  case DHash.lookup rtt ris of
    Nothing -> Nothing --DHash.insert rtt (RowInfo (JSONSeriesFold fld) (const Nothing)) rbm
    Just (RowInfo x ubs y z (JSONSeriesFold fld'))
      -> Just $ DHash.insert rtt (RowInfo x ubs y z (JSONSeriesFold $ fld' <> fld)) ris

buildJSONSeries :: forall d f. RowInfos d -> d -> Either Text Aeson.Series
buildJSONSeries rbm d =
  let foldOne :: RowBuilder d -> Either Text Aeson.Series
      foldOne ((RowTypeTag _) DSum.:=> ri@(RowInfo (ToFoldable f) _ _ _ (JSONSeriesFold fld))) = Foldl.foldM fld (f d)
  in mconcat <$> (traverse foldOne $ DHash.toList rbm)

data JSONRowFold d r = JSONRowFold (ToFoldable d r) (Stan.StanJSONF r Aeson.Series)

buildJSONFromRows :: DHash.DHashMap RowTypeTag (JSONRowFold d) -> d -> Either Text Aeson.Series
buildJSONFromRows rowFoldMap d = do
  let toSeriesOne (rtt DSum.:=> JSONRowFold (ToFoldable tf) fld) = Foldl.foldM fld (tf d)
  fmap mconcat $ traverse toSeriesOne $ DHash.toList rowFoldMap

-- The Maybe return values are there just to satisfy the (returned) type of DHash.traverseWithKey
buildGroupIndexes :: (Typeable d) => StanBuilderM env d ()
buildGroupIndexes = do
  let buildIndexJSONFold :: (Typeable d) => RowTypeTag r -> GroupTypeTag k -> IndexMap r k -> StanBuilderM env d (Maybe k)
      buildIndexJSONFold rtt (GroupTypeTag gName) (IndexMap (IntIndex gSize mIntF) _ _) = do
        let dsName = dataSetName rtt
        addFixedIntJson ("J_" <> gName) (Just 2) gSize
        _ <- addColumnMJson rtt gName (SME.StanArray [SME.NamedDim dsName] SME.StanInt) "<lower=1>" mIntF
        addDeclBinding gName $ SME.name $ "J_" <> gName
--        addUseBinding dsName gName $ SME.indexBy (SME.name gName) dsName
        return Nothing
      buildRowFolds :: (Typeable d) => RowTypeTag r -> RowInfo d r -> StanBuilderM env d (Maybe r)
      buildRowFolds rtt (RowInfo _ _ (GroupIndexes gis) _ _) = do
        _ <- DHash.traverseWithKey (buildIndexJSONFold rtt) gis
        return Nothing
  rowInfos <- rowBuilders <$> get
  _ <- DHash.traverseWithKey buildRowFolds rowInfos
  return ()

buildJSONF :: forall env d.(Typeable d) => StanBuilderM env d (DHash.DHashMap RowTypeTag (JSONRowFold d))
buildJSONF = do
  rowInfos <- rowBuilders <$> get
  let buildRowJSONFolds :: RowInfo d r -> StanBuilderM env d (JSONRowFold d r)
      buildRowJSONFolds (RowInfo tf _ _ _ (JSONSeriesFold jsonF)) = return $ JSONRowFold tf jsonF
{-
      buildRowJSONFolds (RowInfo (ToFoldable toData) gis _ (JSONSeriesFold jsonF)) = do
        case getIndexF gim of
          Right rowIndexF -> do
            -- FE data can have rows which are not in model.  We just ignore them
            let intMapF = Foldl.Fold (\im r -> either (const im) (\n -> IntMap.insert n r im) $ rowIndexF r) IntMap.empty id
            return $ JSONRowFold (ToFoldable $ Foldl.fold intMapF . toData) jsonF
          Left _ -> return $ JSONRowFold (ToFoldable toData) jsonF
-}
  DHash.traverse buildRowJSONFolds rowInfos

newtype DeclarationMap = DeclarationMap (Map SME.StanName SME.StanType)
newtype ScopedDeclarations = ScopedDeclarations (NonEmpty DeclarationMap)

varLookup :: ScopedDeclarations -> SME.StanName -> Either Text StanVar
varLookup (ScopedDeclarations dms) sn = go $ toList dms where
  go [] = Left $ "\"" <> sn <> "\" not declared/in scope."
  go ((DeclarationMap x) : xs) = case Map.lookup sn x of
    Nothing -> go xs
    Just st -> return $ StanVar sn st

alreadyDeclared :: ScopedDeclarations -> SME.StanVar -> Either Text ()
alreadyDeclared sd@(ScopedDeclarations dms) (SME.StanVar sn st) =
  case varLookup sd sn of
    Right (StanVar _ st') ->  if st == st'
                              then Left $ sn <> " already declared (with same type)!"
                              else Left $ sn <> " already declared (with different type)!"
    Left _ -> return ()

addVarInScope :: SME.StanName -> SME.StanType -> StanBuilderM env d SME.StanVar
addVarInScope sn st = do
  let sv = SME.StanVar sn st
      newDVs dvs = do
        _ <- alreadyDeclared dvs sv
        let ScopedDeclarations dmNE = dvs
            DeclarationMap dm = head dmNE
            dmWithNew = DeclarationMap $ Map.insert sn st dm
        return  $ ScopedDeclarations $ dmWithNew :| tail dmNE
  bs <- get
  case modifyDeclaredVarsA newDVs bs of
    Left errMsg -> stanBuildError errMsg
    Right newBS -> put newBS >> return sv

getVar :: StanName -> StanBuilderM env d StanVar
getVar sn = do
  sd <- declaredVars <$> get
  case varLookup sd sn of
    Right sv -> return sv
    Left errMsg -> stanBuildError errMsg

addScope :: StanBuilderM env d ()
addScope = modify (modifyDeclaredVars f) where
  f (ScopedDeclarations dmNE) = ScopedDeclarations $ (DeclarationMap Map.empty) :| toList dmNE

dropScope :: StanBuilderM env d ()
dropScope = do
  ScopedDeclarations dmNE <- declaredVars <$> get
  let (_, mx) = NE.uncons dmNE
  case mx of
    Nothing -> stanBuildError $ "Attempt to drop outermost scope!"
    Just dmNE' -> modify (modifyDeclaredVars $ const $ ScopedDeclarations dmNE)

scoped :: StanBuilderM env d a -> StanBuilderM env d a
scoped x = do
  addScope
  a <- x
  dropScope
  return a

data BuilderState d = BuilderState { declaredVars :: !ScopedDeclarations
                                   , indexBindings :: !SME.VarBindingStore
                                   , rowBuilders :: !(RowInfos d)
                                   , constJSON :: JSONSeriesFold ()  -- json for things which are attached to no data set.
                                   , hasFunctions :: !(Set.Set Text)
                                   , code :: !StanCode
--                                   , indexBuilders :: !(DataSetGroupIntMapBuilders d)
                                   }

setDataSetForBindings :: RowTypeTag r -> StanBuilderM env d ()
setDataSetForBindings rtt = do
  rowInfos <- rowBuilders <$> get
  case DHash.lookup rtt rowInfos of
    Nothing -> stanBuildError $ "Data-set=\"" <> dataSetName rtt <> "\" is not present in rowBuilders."
    Just ri -> modify $ modifyIndexBindings (\(SME.VarBindingStore _ dbm) -> SME.VarBindingStore (expressionBindings ri) dbm)

modifyDeclaredVars :: (ScopedDeclarations -> ScopedDeclarations) -> BuilderState d -> BuilderState d
modifyDeclaredVars f (BuilderState dv vbs rb cj hf c) = BuilderState (f dv) vbs rb cj hf c

modifyDeclaredVarsA :: Applicative t
                    => (ScopedDeclarations -> t ScopedDeclarations)
                    -> BuilderState d
                    -> t (BuilderState d)
modifyDeclaredVarsA f (BuilderState dv vbs rb cj hf c) = (\x -> BuilderState x vbs rb cj hf c) <$> f dv

modifyIndexBindings :: (VarBindingStore -> VarBindingStore)
                    -> BuilderState d
                    -> BuilderState d
modifyIndexBindings f (BuilderState dv vbs rb cj hf c) = BuilderState dv (f vbs) rb cj hf c

modifyIndexBindingsA :: Applicative t
                     => (VarBindingStore -> t VarBindingStore)
                     -> BuilderState d
                     -> t (BuilderState d)
modifyIndexBindingsA f (BuilderState dv vbs rb cj hf c) = (\x -> BuilderState dv x rb cj hf c) <$> f vbs

withUseBindings :: Map SME.IndexKey SME.StanExpr -> StanBuilderM env d a -> StanBuilderM env d a
withUseBindings ubs m = do
  oldBindings <- indexBindings <$> get
  modify $ modifyIndexBindings (\(SME.VarBindingStore _ dbs) -> SME.VarBindingStore ubs dbs)
  a <- m
  modify $ modifyIndexBindings $ const oldBindings
  return a

modifyRowInfosA :: Applicative t
                   => (RowInfos d -> t (RowInfos d))
                   -> BuilderState d
                   -> t (BuilderState d)
modifyRowInfosA f (BuilderState dv vbs rb cj hf c) = (\x -> BuilderState dv vbs x cj hf c) <$> f rb

{-
modifyIndexBuildersA :: Applicative t
                     => (DataSetGroupIntMapBuilders d -> t (DataSetGroupIntMapBuilders d))
                     -> BuilderState d
                     -> t (BuilderState d)
modifyIndexBuildersA f (BuilderState dv vbs rb hf c ib) = (\x -> BuilderState dv vbs rb hf  c x) <$> f ib
-}

modifyConstJson :: (JSONSeriesFold () -> JSONSeriesFold ()) -> BuilderState d -> BuilderState d
modifyConstJson f (BuilderState dvs ibs rbs cj hfs c) = BuilderState dvs ibs rbs (f cj) hfs c

addConstJson :: JSONSeriesFold () -> BuilderState d -> BuilderState d
addConstJson jf = modifyConstJson (<> jf)

modifyFunctionNames :: (Set Text -> Set Text) -> BuilderState d -> BuilderState d
modifyFunctionNames f (BuilderState dv vbs rb cj hf c) = BuilderState dv vbs rb cj (f hf) c

initialBuilderState :: Typeable d => RowInfos d -> BuilderState d
initialBuilderState rowInfos =
  BuilderState
  (ScopedDeclarations $ (DeclarationMap Map.empty) :| [])
  SME.noBindings
  rowInfos
  mempty
  Set.empty
  emptyStanCode
--  emptyIntMapBuilders


type RowInfoMakers d = DHash.DHashMap RowTypeTag (GroupIndexAndIntMapMakers d)

newtype StanGroupBuilderM d a
  = StanGroupBuilderM { unStanGroupBuilderM :: ExceptT Text (State (RowInfoMakers d)) a }
  deriving (Functor, Applicative, Monad, MonadState (RowInfoMakers d))
{-
data GroupEnv = GroupEnv { groupIndexByType :: RowIndexesDHM
                         , groupIndexByName :: Map Text (IntIndex r0)

-}
stanGroupBuildError :: Text -> StanGroupBuilderM d a
stanGroupBuildError t = StanGroupBuilderM $ ExceptT (pure $ Left t)

addDataSetToGroupBuilder :: forall d r. Typeable r =>  Text -> ToFoldable d r -> StanGroupBuilderM d (RowTypeTag r)
addDataSetToGroupBuilder dsName tf = do
  rims <- get
  let rtt = RowTypeTag @r dsName
  case DHash.lookup rtt rims of
    Nothing -> do
      put $ DHash.insert rtt (GroupIndexAndIntMapMakers tf (GroupIndexMakers DHash.empty) (GroupIntMapBuilders DHash.empty)) rims
      return rtt
    Just _ -> stanGroupBuildError $ "Data-set \"" <> dataSetName rtt <> "\" already set up in Group Builder"


addGroupIndexForDataSet :: forall r k d.Typeable k => GroupTypeTag k -> RowTypeTag r -> MakeIndex r k -> StanGroupBuilderM d ()
addGroupIndexForDataSet gtt rtt mkIndex = do
  rims <- get
  case DHash.lookup rtt rims of
    Nothing -> stanGroupBuildError $ "Data-set \"" <> dataSetName rtt <> "\" needs to be added before groups can be added to it."
    Just (GroupIndexAndIntMapMakers tf (GroupIndexMakers gims) gimbs) -> case DHash.lookup gtt gims of
      Nothing -> put $ DHash.insert rtt (GroupIndexAndIntMapMakers tf (GroupIndexMakers $ DHash.insert gtt mkIndex gims) gimbs) rims
      Just _ -> stanGroupBuildError $ "Attempt to add a second group (\"" <> taggedGroupName gtt <> "\") at the same type for row=" <> dataSetName rtt

addGroupIntMapForDataSet :: forall r k d.Typeable k => GroupTypeTag k -> RowTypeTag r -> DataToIntMap r k -> StanGroupBuilderM d ()
addGroupIntMapForDataSet gtt rtt mkIntMap = do
  rims <- get
  case DHash.lookup rtt rims of
    Nothing -> stanGroupBuildError $ "Data-set \"" <> dataSetName rtt <> "\" needs to be added before groups can be added to it."
    Just (GroupIndexAndIntMapMakers tf gims (GroupIntMapBuilders gimbs)) -> case DHash.lookup gtt gimbs of
      Nothing -> put $ DHash.insert rtt (GroupIndexAndIntMapMakers tf gims (GroupIntMapBuilders $ DHash.insert gtt mkIntMap gimbs)) rims
      Just _ -> stanGroupBuildError $ "Attempt to add a second group (\"" <> taggedGroupName gtt <> "\") at the same type for row=" <> dataSetName rtt

runStanGroupBuilder :: Typeable d=> StanGroupBuilderM d () -> d -> Either Text (BuilderState d)
runStanGroupBuilder sgb d = do
  let (resE, rims) = usingState DHash.empty $ runExceptT $ unStanGroupBuilderM sgb
  rowInfos <- DHash.traverseWithKey (buildGroupIndexesAndIntMaps d) rims
  return $ initialBuilderState rowInfos

{-
data StanEnv env = StanEnv
                   { userEnv :: !env
                   , groupEnv :: !GroupEnv
                   }
-}

newtype StanBuilderM env d a = StanBuilderM { unStanBuilderM :: ExceptT Text (ReaderT env (State (BuilderState d))) a }
                             deriving (Functor, Applicative, Monad, MonadReader env, MonadState (BuilderState d))

askUserEnv :: StanBuilderM env d env
askUserEnv = ask

addFunctionsOnce :: Text -> StanBuilderM env d () -> StanBuilderM env d ()
addFunctionsOnce functionsName fCode = do
  (BuilderState vars ibs rowBuilders cj fsNames code) <- get
  if functionsName `Set.member` fsNames
    then return ()
    else (do
             fCode
             modify $ modifyFunctionNames $ Set.insert functionsName
         )

{-
addDataSet :: (Typeable r, Typeable d, Typeable k)
           => Text
           -> ToFoldable d r
           -> GroupIndexMakerDHM r
           -> StanBuilderM env d (RowTypeTag r)
addDataSet name toFoldable gims = do
  let rtt = RowTypeTag name
  (BuilderState vars ibs rowBuilders fsNames code ims) <- get
  case DHash.lookup rtt rowBuilders of
    Just _ -> stanBuildError "Attempt to add 2nd data set with same type and name"
    Nothing -> do
      let rowInfo = RowInfo toFoldable gims mempty
          newRowBuilders = DHash.insert rtt rowInfo rowBuilders
      put (BuilderState vars ibs newRowBuilders fsNames code ims)
  return rtt
-}
{-
addUnIndexedDataSet :: (Typeable r, Typeable d)
                    => Text
                    -> ToFoldable d r
                    -> StanBuilderM env d r0 (RowTypeTag r)
addUnIndexedDataSet name toFoldable = do
  let rtt = RowTypeTag name
  (BuilderState vars ibs rowBuilders modelExprs code ims) <- get
  case DHash.lookup rtt rowBuilders of
    Just _ -> stanBuildError "Attempt to add 2nd data set with same type and name"
    Nothing -> do
      let rowInfo = unIndexedRowInfo toFoldable name
          newRowBuilders = DHash.insert rtt rowInfo rowBuilders
      put (BuilderState vars ibs newRowBuilders modelExprs code ims)
  return rtt
-}
{-
addIndexIntMapFld :: forall r k env d r0.
                  RowTypeTag r
                  -> GroupTypeTag k
                  -> (Foldl.FoldM (Either Text) r (IntMap k))
                  -> StanBuilderM env d r0 ()
addIndexIntMapFld rtt gtt imFld = do
  BuilderState vars ib rowBuilders fsNames code ibs <- get
  let   f :: ToFoldable d r -> d -> Either Text (IntMap k)
        f tf d = applyToFoldableM imFld tf d
  case gtt of
    GroupTypeTag _ -> do
      let ibs' = addIntMapBuilder rtt gtt f ibs
      put $  BuilderState vars ib rowBuilders fsNames code ibs'


addIndexIntMap :: forall r k env d r0.
               RowTypeTag r
               -> GroupTypeTag k
               -> IntMap k
               -> StanBuilderM env d r0 ()
addIndexIntMap rtt gtt im = do
  BuilderState vars ib rowBuilders fsNames code ibs <- get
  let   f :: ToFoldable d r -> d -> Either Text (IntMap k)
        f _ _ = Right im
  case gtt of -- bring the Typeable constraint from the GroupTypeTag GADT into scope
    GroupTypeTag _ -> do
      let ibs' = addIntMapBuilder rtt gtt f ibs
      put $  BuilderState vars ib rowBuilders fsNames code ibs'


addDataSetIndexes ::  forall r k env d r0. (Typeable d)
                    => RowTypeTag r
                    -> GroupRowMap r
                    -> StanBuilderM env d r0 ()
addDataSetIndexes rtt grm = do
  git <- groupIndexByType <$> askGroupEnv
  let lengthName = "N_" <> dsName rtt
  addLengthJson rtt lengthName (dsName rtt)
  let f (gtt DSum.:=> (RowMap h)) =
        let name = taggedGroupName gtt <> "_" <> dsSuffix rtt
        in case DHash.lookup gtt git of
          Nothing -> stanBuildError $ "addDataSetIndexes: Group=" <> taggedGroupName gtt
                     <> " missing from GroupEnv. Perhaps a group index needs to be added, "
                     <> "either to modeled data or manually in the code-builder?"
          Just (IndexMap _ gE _) -> do
            addJson
              rtt
              name
              (SME.StanArray [SME.NamedDim $ dsName rtt] SME.StanInt)
              "<lower=1>"
              (Stan.valueToPairF name $ Stan.jsonArrayEF $ gE . h)
  traverse_ f $ DHash.toList grm
-}

{-
runStanBuilder' :: forall f env d a. (Typeable d)
               => env -> StanBuilderM env d a -> Either Text (BuilderState d, a)
runStanBuilder' userEnv sb = res where
  (resE, bs) = usingState (initialBuilderState toModeled) . usingReaderT (StanEnv userEnv ge) . runExceptT $ unStanBuilderM sbWithIntMaps
  res = fmap (bs,) resE
  sbWithIntMaps :: StanBuilderM env d r0 a
  sbWithIntMaps = do
    let f :: (DHash.DSum GroupTypeTag (IndexMap r0)) -> StanBuilderM env d r0 ()
        f (gtt DSum.:=> IndexMap _ _ im) =  addIndexIntMap (ModeledRowTag @r0) gtt im
    traverse_ f $ DHash.toList $ groupIndexByType ge
    addDeclBinding modeledDataIndexName (name $ "N")
    sb
-}

runStanBuilder :: (Typeable d)
               => d
               -> env
               -> StanGroupBuilderM d ()
               -> StanBuilderM env d a
               -> Either Text (BuilderState d, a)
runStanBuilder d userEnv sgb sb = do
  builderState <- runStanGroupBuilder sgb d
  let (resE, bs) = usingState builderState . usingReaderT userEnv . runExceptT $ unStanBuilderM $ sb
  fmap (bs,) resE

stanBuildError :: Text -> StanBuilderM env d a
stanBuildError t = StanBuilderM $ ExceptT (pure $ Left t)

stanBuildMaybe :: Text -> Maybe a -> StanBuilderM env d a
stanBuildMaybe msg = maybe (stanBuildError msg) return

stanBuildEither :: Either Text a -> StanBuilderM ev d a
stanBuildEither = either stanBuildError return

getDeclBinding :: IndexKey -> StanBuilderM env d SME.StanExpr
getDeclBinding k = do
  SME.VarBindingStore _ dbm <- indexBindings <$> get
  case Map.lookup k dbm of
    Nothing -> stanBuildError $ "declaration key (\"" <> k <> "\") not in binding store."
    Just e -> return e

addDeclBinding :: IndexKey -> SME.StanExpr -> StanBuilderM env d ()
addDeclBinding k e = modify $ modifyIndexBindings f where
  f (SME.VarBindingStore ubm dbm) = SME.VarBindingStore ubm (Map.insert k e dbm)

addUseBinding :: IndexKey -> SME.StanExpr -> StanBuilderM env d ()
addUseBinding k e = do
  --modify $ modifyIndexBindings f where
  let f (SME.VarBindingStore ubm dbm) = do
        case Map.lookup k ubm of
            Nothing -> Right $ SME.VarBindingStore (Map.insert k e ubm) dbm
            Just _ -> Left $ "Attempt to add a use binding where one already exists: index-key=" <> show k
  oldBuilderState <- get
  case modifyIndexBindingsA f oldBuilderState of
    Right newBuilderState -> put newBuilderState
    Left err -> stanBuildError err

indexBindingScope :: StanBuilderM env d a -> StanBuilderM env d a
indexBindingScope x = do
  curIB <- indexBindings <$> get
  a <- x
  modify (modifyIndexBindings $ const curIB)
  return a

isDeclared :: SME.StanName -> StanBuilderM env d Bool
isDeclared sn  = do
  sd <- declaredVars <$> get
  case varLookup sd sn of
    Left _ -> return False
    Right _ -> return True

-- return True if variable is new, False if already declared
declare :: SME.StanName -> SME.StanType -> StanBuilderM env d Bool
declare sn st = do
  let sv = SME.StanVar sn st
  sd <- declaredVars <$> get
  case varLookup sd sn of
    Left _ -> addVarInScope sn st >> return True
    Right (SME.StanVar _ st') -> if st' == st
                                 then return False
                                 else stanBuildError $ "Attempt to re-declare \"" <> sn <> "\" with different type. Previous=" <> show st' <> "; new=" <> show st

stanDeclare' :: SME.StanName -> SME.StanType -> Text -> Maybe StanExpr -> StanBuilderM env d SME.StanVar
stanDeclare' sn st sc mRHS = do
  isNew <- declare sn st
  let sv = SME.StanVar sn st
      lhs = declareVar sv sc
  _ <- if isNew
    then case mRHS of
           Nothing -> addExprLine "stanDeclare'" lhs
           Just rhs -> addExprLine "stanDeclare'" (SME.declaration lhs `SME.eq` rhs)
    else case mRHS of
           Nothing -> return ()
           Just _ -> stanBuildError $ "Attempt to re-declare variable with RHS (" <> sn <> ")"
  return sv

stanDeclare :: SME.StanName -> SME.StanType -> Text -> StanBuilderM env d SME.StanVar
stanDeclare sn st sc = stanDeclare' sn st sc Nothing

stanDeclareRHS :: SME.StanName -> SME.StanType -> Text -> SME.StanExpr -> StanBuilderM env d SME.StanVar
stanDeclareRHS sn st sc rhs = stanDeclare' sn st sc (Just rhs)

checkName :: SME.StanName -> StanBuilderM env d ()
checkName sn = do
  dvs <- declaredVars <$> get
  _ <- stanBuildEither $ varLookup dvs sn
  return ()
{-
  case Map.lookup sn declared of
    Nothing -> stanBuildError $ "name check failed for " <> show sn <> ". Missing declaration?"
    _ -> return ()
-}

typeForName :: SME.StanName -> StanBuilderM env d SME.StanType
typeForName sn = do
  dvs <- declaredVars <$> get
  stanBuildEither $ (\(SME.StanVar _ st) -> st) <$> varLookup dvs sn

addJson :: (Typeable d)
        => RowTypeTag r
        -> SME.StanName
        -> SME.StanType
        -> Text
        -> Stan.StanJSONF r Aeson.Series
        -> StanBuilderM env d SME.StanVar
addJson rtt name st sc fld = do
  inBlock SBData $ stanDeclare name st sc
  (BuilderState declared ib rowBuilders modelExprs code ims) <- get
--  when (Set.member name un) $ stanBuildError $ "Duplicate name in json builders: \"" <> name <> "\""
  newRowBuilders <- case addFoldToDBuilder rtt fld rowBuilders of
    Nothing -> stanBuildError $ "Attempt to add Json to an uninitialized dataset (" <> dataSetName rtt <> ")"
    Just x -> return x
  put $ BuilderState declared ib newRowBuilders modelExprs code ims
  return $ SME.StanVar name st

-- things like lengths may often be re-added
-- maybe work on a cleaner way...
addJsonUnchecked :: (Typeable d)
                 => RowTypeTag r
                 -> SME.StanName
                 -> SME.StanType
                 -> Text
                 -> Stan.StanJSONF r Aeson.Series
                 -> StanBuilderM env d SME.StanVar
addJsonUnchecked rtt name st sc fld = do
  alreadyDeclared <- isDeclared name
  if not alreadyDeclared
    then addJson rtt name st sc fld
    else return $ SME.StanVar name st

addFixedIntJson :: forall d env. (Typeable d) => Text -> Maybe Int -> Int -> StanBuilderM env d SME.StanVar
addFixedIntJson name mLower n = do
  let sc = maybe "" (\l -> "<lower=" <> show l <> ">") $ mLower
  inBlock SBData $ stanDeclare name SME.StanInt sc -- this will error if we already declared
  modify $ addConstJson (JSONSeriesFold $ Stan.constDataF name n)
  return $ SME.StanVar name SME.StanInt

addFixedIntJson' :: forall d env. (Typeable d) => Text -> Maybe Int -> Int -> StanBuilderM env d SME.StanVar
addFixedIntJson' name mLower n = do
  alreadyDeclared <- isDeclared name
  if not alreadyDeclared
    then addFixedIntJson name mLower n
    else return $ SME.StanVar name SME.StanInt

-- These get re-added each time something adds a column built from the data-set.
-- But we only need it once per data set.
addLengthJson :: (Typeable d) => RowTypeTag r -> Text -> SME.IndexKey -> StanBuilderM env d SME.StanVar
addLengthJson rtt name iKey = do
  addDeclBinding iKey (SME.name name)
  addJsonUnchecked rtt name SME.StanInt "<lower=1>" (Stan.namedF name Foldl.length)

nameSuffixMsg :: SME.StanName -> Text -> Text
nameSuffixMsg n dsName = "name=\"" <> show n <> "\" data-set=\"" <> show dsName <> "\""

addColumnJson :: (Typeable d, Aeson.ToJSON x)
              => RowTypeTag r
              -> Text
              -> SME.StanType
              -> Text
              -> (r -> x)
              -> StanBuilderM env d SME.StanVar
addColumnJson rtt name st sc toX = do
  let dsName = dataSetName rtt
  case st of
    SME.StanInt -> stanBuildError $ "SME.StanInt (scalar) given as type in addColumnJson. " <> nameSuffixMsg name dsName
    SME.StanReal -> stanBuildError $ "SME.StanReal (scalar) given as type in addColumnJson. " <> nameSuffixMsg name dsName
    _ -> return ()
  let sizeName = "N_" <> dsName
  addLengthJson rtt sizeName dsName -- ??
  let fullName = name <> "_" <> dsName
  addJson rtt fullName st sc (Stan.valueToPairF fullName $ Stan.jsonArrayF toX)

addColumnMJson :: (Typeable d, Aeson.ToJSON x)
               => RowTypeTag r
               -> Text
               -> SME.StanType
               -> Text
               -> (r -> Either Text x)
               -> StanBuilderM env d SME.StanVar
addColumnMJson rtt name st sc toMX = do
  let dsName = dataSetName rtt
  case st of
    SME.StanInt -> stanBuildError $ "SME.StanInt (scalar) given as type in addColumnJson. " <> nameSuffixMsg name dsName
    SME.StanReal -> stanBuildError $ "SME.StanReal (scalar) given as type in addColumnJson. " <> nameSuffixMsg name dsName
    _ -> return ()
  let sizeName = "N_" <> dsName
  addLengthJson rtt sizeName dsName -- ??
  let fullName = name <> "_" <> dsName
  addJson rtt fullName st sc (Stan.valueToPairF fullName $ Stan.jsonArrayEF toMX)

-- NB: name has to be unique so it can also be the suffix of the num columns.  Presumably the name carries the data-set suffix if nec.
add2dMatrixJson :: (Typeable d)
                => RowTypeTag r
                -> Text
                -> Text
                -> SME.StanDim
                -> Int
                -> (r -> Vector.Vector Double) -> StanBuilderM env d SME.StanVar
add2dMatrixJson rtt name sc rowDim cols vecF = do
  let dsName = dataSetName rtt
  let colName = "K" <> "_" <> name
      colDimName = name <> "_Cols"
  addFixedIntJson colName Nothing cols
  addDeclBinding colDimName (SME.name colName)
  addColumnJson rtt name (SME.StanMatrix (rowDim, SME.NamedDim colDimName)) sc vecF

modifyCode' :: (StanCode -> StanCode) -> BuilderState d -> BuilderState d
modifyCode' f bs = let newCode = f $ code bs in bs { code = newCode }

modifyCode :: (StanCode -> StanCode) -> StanBuilderM env d ()
modifyCode f = modify $ modifyCode' f

setBlock' :: StanBlock -> StanCode -> StanCode
setBlock' b (StanCode _ blocks) = StanCode b blocks

setBlock :: StanBlock -> StanBuilderM env d ()
setBlock = modifyCode . setBlock'

getBlock :: StanBuilderM env d StanBlock
getBlock = do
  (BuilderState _ _ _ _ _ (StanCode b _)) <- get
  return b

addInLine' :: Text -> StanCode -> StanCode
addInLine' t (StanCode b blocks) =
  let (WithIndent curCode curIndent) = blocks Array.! b
      newCode = curCode <> t
  in StanCode b (blocks Array.// [(b, WithIndent newCode curIndent)])


addLine' :: Text -> StanCode -> StanCode
addLine' t (StanCode b blocks) =
  let (WithIndent curCode curIndent) = blocks Array.! b
      newCode = curCode <> T.replicate curIndent " " <> t
  in StanCode b (blocks Array.// [(b, WithIndent newCode curIndent)])

addLine :: Text -> StanBuilderM env d ()
addLine = modifyCode . addLine'

addInLine :: Text -> StanBuilderM env d ()
addInLine = modifyCode . addInLine'

addStanLine :: Text -> StanBuilderM env d ()
addStanLine t = addLine $ t <> ";\n"

declareStanFunction :: Text -> StanBuilderM env d a -> StanBuilderM env d a
declareStanFunction declaration body = inBlock SBFunctions $ do
  addLine declaration
  bracketed 2 body

indent :: Int -> StanCode -> StanCode
indent n (StanCode b blocks) =
  let (WithIndent curCode curIndent) = blocks Array.! b
  in StanCode b (blocks Array.// [(b, WithIndent curCode (curIndent + n))])

-- code inserted here will be indented n extra spaces
indented :: Int -> StanBuilderM env d a -> StanBuilderM env d a
indented n m = do
  modifyCode (indent n)
  x <- m
  modifyCode (indent $ negate n)
  return x

bracketed :: Int -> StanBuilderM env d a -> StanBuilderM env d a
bracketed n m = do
  addInLine " {\n"
  x <- scoped $ indented n m
  addLine "}\n"
  return x

stanForLoop :: Text -- counter
            -> Maybe Text -- start, if not 1
            -> Text -- end, if
            -> (Text -> StanBuilderM env d a)
            -> StanBuilderM env d a
stanForLoop counter mStart end loopF = do
  let start = fromMaybe "1" mStart
  addLine $ "for (" <> counter <> " in " <> start <> ":" <> end <> ")"
  bracketed 2 $ loopF counter

stanForLoopB :: Text
             -> Maybe SME.StanExpr
             -> IndexKey
             -> StanBuilderM env d a
             -> StanBuilderM env d a
stanForLoopB counter mStartE k x = do
  endE <- getDeclBinding k
  let start = fromMaybe (SME.scalar "1") mStartE
      forE = SME.spaced (SME.name "for")
             $ SME.paren
             $ SME.spaced
             (SME.spaced (SME.name counter) (SME.name "in"))
             (start `SME.nextTo` (SME.bare ":") `SME.nextTo` endE)
  printExprM "forLoopB" forE >>= addLine
  indexBindingScope $ do
    addUseBinding k (SME.name counter)
    bracketed 2 x


data StanPrintable = StanLiteral Text | StanExpression Text

stanPrint :: [StanPrintable] -> StanBuilderM env d ()
stanPrint ps =
  let f x = case x of
        StanLiteral x -> "\"" <> x <> "\""
        StanExpression x -> x
  in addStanLine $ "print(" <> T.intercalate ", " (fmap f ps) <> ")"

underscoredIf :: Text -> Text
underscoredIf t = if T.null t then "" else "_" <> t

stanIndented :: StanBuilderM env d a -> StanBuilderM env d a
stanIndented = indented 2

inBlock :: StanBlock -> StanBuilderM env d a -> StanBuilderM env d a
inBlock b m = do
  oldBlock <- getBlock
  setBlock b
  x <- m
  setBlock oldBlock
  return x

printExprM :: Text -> SME.StanExpr -> StanBuilderM env d Text
printExprM context e = do
  vbs <- indexBindings <$> get
  case SME.printExpr vbs e of
    Right t -> return t
    Left err -> stanBuildError $ context <> ": " <> err

addExprLine :: Text -> SME.StanExpr -> StanBuilderM env d ()
addExprLine context  = printExprM context  >=> addStanLine

addExprLines :: Traversable t => Text -> t SME.StanExpr -> StanBuilderM env d ()
addExprLines context = traverse_ (addExprLine context)



--printUnindexedExprM :: Text -> SME.StanExpr -> StanBuilderM env d r Text
--printUnindexedExprM ctxt = printExprM ctxt SME.noBindings . return



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

{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
--{-# LANGUAGE PolyKinds #-}
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
import qualified Data.GADT.Show as GADT
import Frames.Streamly.TH (declareColumnType)
import Text.Printf (errorMissingArgument)
import Stan.ModelConfig (InputDataType(..))
import Knit.Report (crimson)

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
                      groupIndexToGroupKey :: IntMap.IntMap k,
                      rowToGroup :: r -> k
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
  indexMap = IndexMap intIndex lookupK (toIntMap m) h

makeIndexMapF :: MakeIndex r k -> Foldl.Fold r (IndexMap r k)
makeIndexMapF (GivenIndex m h) = pure $ mapToIndexMap h m
makeIndexMapF (FoldToIndex fld h) = fmap (mapToIndexMap h) fld

makeIndexFromEnum :: forall k r.(Enum k, Bounded k, Ord k) => (r -> k) -> MakeIndex r k
makeIndexFromEnum h = GivenIndex m h where
  allKs = [minBound..maxBound]
  m = Map.fromList $ zip allKs [1..]

makeIndexFromFoldable :: (Foldable f, Ord k)
  => (k -> Text)
  -> (r -> k)
  -> f k
  -> MakeIndex r k
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


-- For post-stratification
newtype RowMap r k = RowMap (r -> k)
type GroupRowMap r = DHash.DHashMap GroupTypeTag (RowMap r)

emptyGroupRowMap :: GroupRowMap r
emptyGroupRowMap = DHash.empty

addRowMap :: Typeable k => GroupTypeTag k -> (r -> k) -> GroupRowMap r -> GroupRowMap r
addRowMap gtt f grm = DHash.insert gtt (RowMap f) grm

data Phantom k = Phantom
type GroupSet = DHash.DHashMap GroupTypeTag Phantom

emptyGroupSet :: GroupSet
emptyGroupSet = DHash.empty

addGroupToSet :: Typeable k => GroupTypeTag k -> GroupSet -> GroupSet
addGroupToSet gtt gs = DHash.insert gtt Phantom gs

newtype DataToIntMap r k = DataToIntMap { unDataToIntMap :: Foldl.FoldM (Either Text) r (IntMap k) }
newtype GroupIntMapBuilders r = GroupIntMapBuilders (DHash.DHashMap GroupTypeTag (DataToIntMap r))

-- r is a Phantom type here
newtype GroupIntMaps r = GroupIntMaps (DHash.DHashMap GroupTypeTag IntMap.IntMap)
type DataSetGroupIntMaps = DHash.DHashMap RowTypeTag GroupIntMaps

data GroupIndexAndIntMapMakers d r = GroupIndexAndIntMapMakers DataSetUse (ToFoldable d r) (GroupIndexMakers r) (GroupIntMapBuilders r)

buildIntMapBuilderF :: (k -> Either Text Int) -> (r -> k) -> DataToIntMap r k --FL.FoldM (Either Text) r (IM.IntMap k)
buildIntMapBuilderF eIntF keyF = DataToIntMap $ Foldl.FoldM step (return IntMap.empty) return where
  step im r = case eIntF $ keyF r of
    Left msg -> Left $ "Indexing error when trying to build IntMap index: " <> msg
    Right n -> Right $ IntMap.insert n (keyF r) im

dataToIntMapFromFoldable :: forall k r f. (Show k, Ord k, Foldable f) => (r -> k) -> f k -> DataToIntMap r k
dataToIntMapFromFoldable keyF keys = buildIntMapBuilderF lkUp keyF where
  keysM :: Map k Int = Map.fromList $ zip (Foldl.fold Foldl.list keys) [1..]
  lkUp k = maybe (Left $ "dataToIntMapFromFoldable.lkUp: " <> show k <> " not found ") Right $ Map.lookup k keysM

dataToIntMapFromEnum :: forall k r f. (Show k, Enum k, Bounded k, Ord k) => (r -> k) -> DataToIntMap r k
dataToIntMapFromEnum keyF = dataToIntMapFromFoldable keyF [minBound..maxBound]

getGroupIndex :: forall d r k. Typeable k
              => RowTypeTag r
              -> GroupTypeTag k
              -> DataSetGroupIntMaps
              -> Either Text (IntMap k)
getGroupIndex rtt gtt groupIndexes =
  case DHash.lookup rtt groupIndexes of
    Nothing -> Left $ "\"" <> dataSetName rtt <> "\" not found in data-set group int maps."
    Just (GroupIntMaps gim) -> case DHash.lookup gtt gim of
      Nothing -> Left $ "\"" <> taggedGroupName gtt <> "\" not found in Group int maps for data-set \"" <> dataSetName rtt <> "\""
      Just im -> Right im

intMapsForDataSetFoldM :: GroupIntMapBuilders r -> Foldl.FoldM (Either Text) r (GroupIntMaps r)
intMapsForDataSetFoldM (GroupIntMapBuilders imbs) = GroupIntMaps <$> DHash.traverse unDataToIntMap imbs

indexBuildersForDataSetFold :: GroupIndexMakers r -> Foldl.Fold r (GroupIndexes r)
indexBuildersForDataSetFold (GroupIndexMakers gims) = GroupIndexes <$> DHash.traverse makeIndexMapF gims

indexType :: StanName -> SME.StanType
indexType indexOf = SME.StanArray [SME.NamedDim indexOf] SME.StanInt

useBindingsFromGroupIndexMakers :: RowTypeTag r -> GroupIndexMakers r -> Map SME.IndexKey SME.StanExpr
useBindingsFromGroupIndexMakers rtt (GroupIndexMakers gims) = Map.fromList l where
  l = fmap g $ DHash.toList gims
  g (gtt DSum.:=> _) =
    let gn = taggedGroupName gtt
        dsn = dataSetName rtt
        indexName = dsn <> "_" <> gn
        indexVar = SME.StanVar indexName $ indexType $ dsn
    in (gn, SME.var indexVar) --SME.indexBy (SME.name indexName) $ dataSetName rtt)


-- build a new RowInfo from the row index and IntMap builders
buildRowInfo :: d -> RowTypeTag r -> GroupIndexAndIntMapMakers d r -> RowInfo d r
buildRowInfo d rtt (GroupIndexAndIntMapMakers dsu tf@(ToFoldable f) ims imbs) = Foldl.fold fld $ f d  where
  gisFld = indexBuildersForDataSetFold $ ims
--  imsFldM = intMapsForDataSetFoldM imbs
  useBindings = Map.insert (dataSetName rtt) (SME.name $ "N_" <> dataSetName rtt) $ useBindingsFromGroupIndexMakers rtt ims
  fld = RowInfo tf useBindings <$> gisFld <*> pure imbs <*> pure mempty <*> pure dsu

groupIntMap :: IndexMap r k -> IntMap.IntMap k
groupIntMap (IndexMap _ _ im _) = im

data DataSetUse = ModelUse | GQOnlyUse deriving (Show, Eq, Ord, Enum, Bounded)

data RowInfo d r = RowInfo
                   {
                     toFoldable    :: ToFoldable d r
                   , expressionBindings :: Map SME.IndexKey SME.StanExpr
                   , groupIndexes  :: GroupIndexes r
                   , groupIntMapBuilders  :: GroupIntMapBuilders r
                   , jsonSeries    :: JSONSeriesFold r
                   , dataSetUse :: DataSetUse
                   }

unIndexedRowInfo :: forall d r. ToFoldable d r -> Text -> RowInfo d r
unIndexedRowInfo tf groupName  = RowInfo tf mempty (GroupIndexes DHash.empty) (GroupIntMapBuilders DHash.empty) mempty ModelUse


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

instance GADT.GShow RowTypeTag where
  gshowsPrec _ rtt s = s ++ toString (dataSetName rtt)

instance Hashable.Hashable (Some.Some RowTypeTag) where
  hash (Some.Some (RowTypeTag n)) = Hashable.hash n
  hashWithSalt m (Some.Some (RowTypeTag n)) = hashWithSalt m n

-- the key is a name for the data-set.  The tag carries the toDataSet function
type RowBuilder d = DSum.DSum RowTypeTag (RowInfo d)
type RowInfos d = DHash.DHashMap RowTypeTag (RowInfo d)


addFoldToDBuilder :: forall d r.(Typeable d)
                  => RowTypeTag r
                  -> Stan.StanJSONF r Aeson.Series
                  -> RowInfos d
                  -> Maybe (RowInfos d)
addFoldToDBuilder rtt fld ris =
  case DHash.lookup rtt ris of
    Nothing -> Nothing --DHash.insert rtt (RowInfo (JSONSeriesFold fld) (const Nothing)) rbm
    Just (RowInfo x ubs y z (JSONSeriesFold fld') dsu)
      -> Just $ DHash.insert rtt (RowInfo x ubs y z (JSONSeriesFold $ fld' <> fld) dsu) ris

buildJSONSeries :: forall d f. RowInfos d -> d -> Either Text Aeson.Series
buildJSONSeries rbm d =
  let foldOne :: RowBuilder d -> Either Text Aeson.Series
      foldOne ((RowTypeTag _) DSum.:=> ri@(RowInfo (ToFoldable f) _ _ _ (JSONSeriesFold fld) _)) = Foldl.foldM fld (f d)
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
      buildIndexJSONFold rtt (GroupTypeTag gName) (IndexMap (IntIndex gSize mIntF) _ _ _) = do
        let dsName = dataSetName rtt
            indexName = dsName <> "_" <> gName
        addFixedIntJson' ("J_" <> gName) (Just 1) gSize
        _ <- addColumnMJson rtt indexName (SME.StanArray [SME.NamedDim dsName] SME.StanInt) "<lower=1>" mIntF
--        addDeclBinding gName $ SME.name $ "J_" <> gName -- ??
        addDeclBinding gName $ SME.StanVar ("J_" <> gName) SME.StanInt
--        addUseBinding dsName gName $ SME.indexBy (SME.name gName) dsName
        return Nothing
      buildRowFolds :: (Typeable d) => RowTypeTag r -> RowInfo d r -> StanBuilderM env d (Maybe r)
      buildRowFolds rtt (RowInfo _ _ (GroupIndexes gis) _ _ _) = do
        _ <- DHash.traverseWithKey (buildIndexJSONFold rtt) gis
        return Nothing
  rowInfos <- rowBuilders <$> get
  _ <- DHash.traverseWithKey buildRowFolds rowInfos
  return ()

buildJSONF :: forall env d.(Typeable d) => StanBuilderM env d (DHash.DHashMap RowTypeTag (JSONRowFold d))
buildJSONF = do
  rowInfos <- rowBuilders <$> get
  let buildRowJSONFolds :: RowInfo d r -> StanBuilderM env d (JSONRowFold d r)
      buildRowJSONFolds (RowInfo tf _ _ _ (JSONSeriesFold jsonF) _) = return $ JSONRowFold tf jsonF
  DHash.traverse buildRowJSONFolds rowInfos

buildJSONFromDataM :: Typeable d => StanBuilderM env d (d -> Either Text Aeson.Series)
buildJSONFromDataM = do
  (JSONSeriesFold constJSONFld) <- constJSON <$> get
  dataSetJSON <- buildJSONF
  return $ \d ->
    let c = Foldl.foldM constJSONFld (Just ())
        ds =  buildJSONFromRows dataSetJSON d
    in (<>) <$> c <*> ds



data VariableScope = GlobalScope | ModelScope | GQScope deriving (Show, Eq, Ord)

newtype DeclarationMap = DeclarationMap (Map SME.StanName SME.StanType) deriving (Show)
data ScopedDeclarations = ScopedDeclarations { currentScope :: VariableScope
                                             , globalScope :: NonEmpty DeclarationMap
                                             , modelScope :: NonEmpty DeclarationMap
                                             , gqScope :: NonEmpty DeclarationMap
                                             } deriving (Show)

changeVarScope :: VariableScope -> ScopedDeclarations -> ScopedDeclarations
changeVarScope vs sd = sd { currentScope = vs}

initialScopedDeclarations :: ScopedDeclarations
initialScopedDeclarations = let x = one (DeclarationMap Map.empty) in ScopedDeclarations GlobalScope x x x

declarationsNE :: VariableScope -> ScopedDeclarations -> NonEmpty DeclarationMap
declarationsNE GlobalScope sd = globalScope sd
declarationsNE ModelScope sd = modelScope sd
declarationsNE GQScope sd = gqScope sd

setDeclarationsNE :: NonEmpty DeclarationMap -> VariableScope -> ScopedDeclarations -> ScopedDeclarations
setDeclarationsNE dmNE GlobalScope sd = sd { globalScope = dmNE}
setDeclarationsNE dmNE ModelScope sd = sd { modelScope = dmNE}
setDeclarationsNE dmNE GQScope sd = sd { gqScope = dmNE}

declarationsInScope :: ScopedDeclarations -> NonEmpty DeclarationMap
declarationsInScope sd = declarationsNE (currentScope sd) sd

addVarInScope :: SME.StanName -> SME.StanType -> StanBuilderM env d SME.StanVar
addVarInScope sn st = do
  let newSD sd = do
        _ <- alreadyDeclared sd (SME.StanVar sn st)
        let curScope = currentScope sd
            dmNE = declarationsNE curScope sd
            DeclarationMap m = head dmNE
            dm' = DeclarationMap $ Map.insert sn st m
            dmNE' = dm' :| tail dmNE
        return $ setDeclarationsNE dmNE' curScope sd
  bs <- get
  case modifyDeclaredVarsA newSD bs of
    Left errMsg -> stanBuildError errMsg
    Right newBS -> put newBS >> return (SME.StanVar sn st)

varLookupInScope :: ScopedDeclarations -> VariableScope -> SME.StanName -> Either Text StanVar
varLookupInScope sd sc sn = go $ toList dNE where
  dNE = declarationsNE sc sd
  go [] = Left $ "\"" <> sn <> "\" not declared/in scope (stan scope=" <> show sc <> ")."
  go ((DeclarationMap x) : xs) = case Map.lookup sn x of
    Nothing -> go xs
    Just st -> return $ StanVar sn st

varLookup :: ScopedDeclarations -> SME.StanName -> Either Text StanVar
varLookup sd = varLookupInScope sd (currentScope sd)

varLookupAllScopes :: ScopedDeclarations -> SME.StanName -> Either Text StanVar
varLookupAllScopes sd sn =
  case varLookupInScope sd GlobalScope sn of
    Right x -> Right x
    Left _ -> case varLookupInScope sd ModelScope sn of
      Right x -> Right x
      Left _ -> varLookupInScope sd GQScope sn


alreadyDeclared :: ScopedDeclarations -> SME.StanVar -> Either Text ()
alreadyDeclared sd (SME.StanVar sn st) =
  case varLookup sd sn of
    Right (StanVar _ st') ->  if st == st'
                              then Left $ sn <> " already declared (with same type)!"
                              else Left $ sn <> " already declared (with different type)!"
    Left _ -> return ()

alreadyDeclaredAllScopes :: ScopedDeclarations -> SME.StanVar -> Either Text ()
alreadyDeclaredAllScopes sd (SME.StanVar sn st) =
  case varLookupAllScopes sd sn of
    Right (StanVar _ st') ->  if st == st'
                              then Left $ sn <> " already declared (with same type)!"
                              else Left $ sn <> " already declared (with different type)!"
    Left _ -> return ()
{-
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
-}

getVar :: StanName -> StanBuilderM env d StanVar
getVar sn = do
  sd <- declaredVars <$> get
  case varLookup sd sn of
    Right sv -> return sv
    Left errMsg -> stanBuildError errMsg

addScope :: StanBuilderM env d ()
addScope = modify (modifyDeclaredVars f) where
  f sd = setDeclarationsNE dmNE' curScope sd where
    curScope = currentScope sd
    dmNE' = DeclarationMap Map.empty :| toList (declarationsNE curScope sd)

dropScope :: StanBuilderM env d ()
dropScope = do
  sd <- declaredVars <$> get
  let curScope = currentScope sd
      dmNE = declarationsNE curScope sd
      (_, mx) = NE.uncons dmNE
  case mx of
    Nothing -> stanBuildError $ "Attempt to drop outermost scope!"
    Just dmNE' -> modify (modifyDeclaredVars $ const $ setDeclarationsNE dmNE' curScope sd)

scoped :: StanBuilderM env d a -> StanBuilderM env d a
scoped x = do
  addScope
  a <- x
  dropScope
  return a

data BuilderState md gq = BuilderState { declaredVars :: !ScopedDeclarations
                                       , indexBindings :: !SME.VarBindingStore
                                       , modelRowBuilders :: !(RowInfos md)
                                       , gqRowBuilders :: !(RowInfos gq)
                                       , constJSON :: JSONSeriesFold ()  -- json for things which are attached to no data set.
                                       , hasFunctions :: !(Set.Set Text)
                                       , code :: !StanCode
                                       }

dumpBuilderState :: BuilderState md gq -> Text
dumpBuilderState bs = -- (BuilderState dvs ibs ris js hf c) =
  "Declared Vars: " <> show (declaredVars bs)
  <> "\n index-bindings: " <> show (indexBindinggs bs)
  <> "\n model row-info-keys: " <> show (DHash.keys $ modelRowBuilders bs)
  <> "\n gq row-info-keys: " <> show (DHash.keys $ gqRowBuilders bs)
  <> "\n functions: " <> show hf


--getRowInfo :: InputDataType -> RowTypeTag r

withRowInfo :: Text -> (forall x.RowInfo x r -> StanBuilderM md gq y) -> InputDataType -> RowTypeTag r -> StanBuilderM md gq y
withRowInfo ctxt f idt rtt =
  case idt of
    ModelData -> do
      rowInfos <- modelRowBuilders <$> get
      case DHash.lookup rtt rowInfos of
        Nothing -> stanBuildError $ "withRowInfo (" <> ctxt <> "): Data-set=\"" <> dataSetName rtt <> "\" is not present in modelRowBuilders."
        Just ri -> f ri
    GQData -> do
      rowInfos <- gqRowBuilders <$> get
      case DHash.lookup rtt rowInfos of
        Nothing -> stanBuildError $ "withRowInfo (" <> ctxt <> "): Data-set=\"" <> dataSetName rtt <> "\" is not present in modelRowBuilders."
        Just ri -> f ri

getDataSetBindings :: InputDataType -> RowTypeTag r -> StanBuilderM md gq (Map SME.IndexKey SME.StanExpr)
getDataSetBindings = withRowInfo "getDataSetBindings" (return . expressionBindings)

{-
getModelDataSetBindings :: RowTypeTag r -> StanBuilderM md gq (Map SME.IndexKey SME.StanExpr)
getModelDataSetBindings rtt = do
  rowInfos <- modelRowBuilders <$> get
  case DHash.lookup rtt rowInfos of
    Nothing -> stanBuildError $ "getModelDataSetBindings: Data-set=\"" <> dataSetName rtt <> "\" is not present in modelRowBuilders."
    Just ri -> return $ expressionBindings ri

getGQDataSetBindings :: RowTypeTag r -> StanBuilderM md gq (Map SME.IndexKey SME.StanExpr)
getGQDataSetBindings rtt = do
  rowInfos <- gqRowBuilders <$> get
  case DHash.lookup rtt rowInfos of
    Nothing -> stanBuildError $ "getGQDataSetFBindings: Data-set=\"" <> dataSetName rtt <> "\" is not present in gqRowBuilders."
    Just ri -> return $ expressionBindings ri
-}

setDataSetForBindings :: InputDataType -> RowTypeTag r -> StanBuilderM md gq ()
setDataSetForBindings = do
  newUseBindings <- getDataSetBindings idt rtt
  modify $ modifyIndexBindings (\(SME.VarBindingStore _ dbm) -> SME.VarBindingStore newUseBindings dbm)

useDataSetForBindings :: InputDataType -> RowTypeTag r -> StanBuilderM md gq a -> StanBuilderM md gq a
useDataSetForBindings idt rtt x = getDataSetBindings idt rtt >>= flip withUseBindings x

modifyDeclaredVars :: (ScopedDeclarations -> ScopedDeclarations) -> BuilderState md gq -> BuilderState md gq
modifyDeclaredVars f (BuilderState dv vbs mrb gqrb cj hf c) = BuilderState (f dv) vbs mrb gqrb cj hf c

modifyDeclaredVarsA :: Applicative t
                    => (ScopedDeclarations -> t ScopedDeclarations)
                    -> BuilderState md gq
                    -> t (BuilderState md gq)
modifyDeclaredVarsA f (BuilderState dv vbs mrb gqrb cj hf c) = (\x -> BuilderState x vbs mrb gqrb cj hf c) <$> f dv

modifyIndexBindings :: (VarBindingStore -> VarBindingStore)
                    -> BuilderState md gq
                    -> BuilderState md gq
modifyIndexBindings f (BuilderState dv vbs mrb gqrb cj hf c) = BuilderState dv (f vbs) mrb gqrb cj hf c

modifyIndexBindingsA :: Applicative t
                     => (VarBindingStore -> t VarBindingStore)
                     -> BuilderState md gq
                     -> t (BuilderState md gq)
modifyIndexBindingsA f (BuilderState dv vbs mrb gqrb cj hf c) = (\x -> BuilderState dv x mrb gqrb cj hf c) <$> f vbs

withUseBindings :: Map SME.IndexKey SME.StanExpr -> StanBuilderM md gq a -> StanBuilderM md gq a
withUseBindings ubs m = do
  oldBindings <- indexBindings <$> get
  modify $ modifyIndexBindings (\(SME.VarBindingStore _ dbs) -> SME.VarBindingStore ubs dbs)
  a <- m
  modify $ modifyIndexBindings $ const oldBindings
  return a

addScopedDeclBindings :: Map SME.IndexKey SME.StanExpr -> StanBuilderM env d a -> StanBuilderM env d a
addScopedDeclBindings dbs' m = do
  oldBindings <- indexBindings <$> get
  modify $ modifyIndexBindings (\(SME.VarBindingStore ubs dbs) -> SME.VarBindingStore ubs (Map.union dbs dbs))
  a <- m
  modify $ modifyIndexBindings $ const oldBindings
  return a


modifyModelRowInfosA :: Applicative t
                   => (RowInfos md -> t (RowInfos md))
                   -> BuilderState md gq
                   -> t (BuilderState md gq)
modifyModelRowInfosA f (BuilderState dv vbs mrb gqrb cj hf c) = (\x -> BuilderState dv vbs x gqrb cj hf c) <$> f mrb

modifyGQRowInfosA :: Applicative t
                   => (RowInfos gq -> t (RowInfos gq))
                   -> BuilderState md gq
                   -> t (BuilderState md gq)
modifyGQRowInfosA f (BuilderState dv vbs mrb gqrb cj hf c) = (\x -> BuilderState dv vbs mrb x cj hf c) <$> f gqrb


modifyConstJson :: (JSONSeriesFold () -> JSONSeriesFold ()) -> BuilderState md gq -> BuilderState md gq
modifyConstJson f (BuilderState dvs ibs rbs cj hfs c) = BuilderState dvs ibs rbs (f cj) hfs c

addConstJson :: JSONSeriesFold () -> BuilderState md gq -> BuilderState md gq
addConstJson jf = modifyConstJson (<> jf)

modifyFunctionNames :: (Set Text -> Set Text) -> BuilderState md gq -> BuilderState md gq
modifyFunctionNames f (BuilderState dv vbs mrb gqrb cj hf c) = BuilderState dv vbs mrb gqrb cj (f hf) c

initialBuilderState :: Typeable md => RowInfos md -> RowInfos gq -> BuilderState md gq
initialBuilderState modelRowInfos gqRowInfos =
  BuilderState
  initialScopedDeclarations
  SME.noBindings
  modelRowInfos
  gqRowInfos
  mempty
  Set.empty
  emptyStanCode

type RowInfoMakers d = DHash.DHashMap RowTypeTag (GroupIndexAndIntMapMakers d)

data GroupBuilderS md gq = GroupBuilderS { gbModelS :: RowInfoMakers md, gbGQS :: RowInfoMakers gq}

modifyGBModelS :: (RowInfoMakers md -> RowInfoMakers md) -> GroupBuilderS md gq -> GroupBuilderS md gq
modifyGBModelS f (GroupBuilderS mRims gqRims) = GroupBuilderS (f mRims) gqRims

modifyGBGQS :: (RowInfoMakers gq -> RowInfoMakers gq) -> GroupBuilderS md gq -> GroupBuilderS md gq
modifyGBGQS f (GroupBuilderS mRims gqRims) = GroupBuilderS mRims (f gqRims)

newtype StanGroupBuilderM md gq a
  = StanGroupBuilderM { unStanGroupBuilderM :: ExceptT Text (State (GroupBuilderS md gq)) a }
  deriving (Functor, Applicative, Monad, MonadState (GroupBuilderS md gq))

stanGroupBuildError :: Text -> StanGroupBuilderM md gq a
stanGroupBuildError t = StanGroupBuilderM $ ExceptT (pure $ Left t)

withRowInfoMakers :: (forall x. RowInfoMakers x -> StanGroupBuilderM md gq (Maybe RowInfoMakers x, y)) -> InputDataType -> StanGroupBuilderM md gq y
withRowInfoMakers f idt =
  case idt of
    ModelData -> do
      rims <- gets gbModelS
      (mRims, y) <- f rims
      case mRims of
        Nothing -> return ()
        Just newRims -> modify $ modifyGBModelS $ const newRims
      return y
    GQData -> do
      rims <- gets gbGQS
      (mRims, y) <- f rims
      case mRims of
        Nothing -> return ()
        Just newRims -> modify $ modifyGBModelS $ const newRims
      return y

getDataSetTag :: forall r md gq. Typeable r => InputDataType -> Text -> StanGroupBuilderM md gq (RowTypeTag r)
getDataSetTag idt t = withRowInfoMakers f idt where
  f infoMakers = do
    let rtt = RowTypeTag @r name
    case DHash.lookup rtt infoMakers of
      Nothing -> stanGroupBuildError $ "getModelDataSetTag: No data-set \"" <> name <> "\" found in group builder."
      Just _ -> return (Nothing, rtt)


-- HERE
--addDataToGroupBuilder ::

addModelDataToGroupBuilder :: forall md gq r. Typeable r
                         => Text
                         -> ToFoldable md r
                         -> StanGroupBuilderM md gq (RowTypeTag r)
addModelDataToGroupBuilder dsu dsName tf = do
  rims <- gets gbModelS
  let rtt = RowTypeTag @r dsName
  case DHash.lookup rtt rims of
    Nothing -> do
      modify $ modifyGBModelS $ DHash.insert rtt (GroupIndexAndIntMapMakers dsu tf (GroupIndexMakers DHash.empty) (GroupIntMapBuilders DHash.empty))
      return rtt
    Just _ -> stanGroupBuildError $ "Data-set \"" <> dataSetName rtt <> "\" already set up in Group Builder (Model)"


addGQDataToGroupBuilder :: forall md gq r. Typeable r
                         => Text
                         -> ToFoldable gq r
                         -> StanGroupBuilderM md gq (RowTypeTag r)
addGQDataToGroupBuilder dsu dsName tf = do
  rims <- gets gbGQS
  let rtt = RowTypeTag @r dsName
  case DHash.lookup rtt rims of
    Nothing -> do
      modify $ modifyGBGQS $ DHash.insert rtt (GroupIndexAndIntMapMakers dsu tf (GroupIndexMakers DHash.empty) (GroupIntMapBuilders DHash.empty))
      return rtt
    Just _ -> stanGroupBuildError $ "Data-set \"" <> dataSetName rtt <> "\" already set up in Group Builder (GQ)"

addGroupIndexForModelData :: forall r k md gq.Typeable k
                        => GroupTypeTag k
                        -> RowTypeTag r
                        -> MakeIndex r k
                        -> StanGroupBuilderM md gq ()
addGroupIndexForModelData gtt rtt mkIndex = do
  rims <- gets gbModelS
  case DHash.lookup rtt rims of
    Nothing -> stanGroupBuildError $ "Data-set \"" <> dataSetName rtt <> "\" needs to be added to Model data before groups can be added to it."
    Just (GroupIndexAndIntMapMakers dsu tf (GroupIndexMakers gims) gimbs) -> case DHash.lookup gtt gims of
      Nothing -> modify $ modifyGBModelS $ DHash.insert rtt (GroupIndexAndIntMapMakers dsu tf (GroupIndexMakers $ DHash.insert gtt mkIndex gims) gimbs)
      Just _ -> stanGroupBuildError $ "Attempt to add a second group (\"" <> taggedGroupName gtt <> "\") at the same type for model row=" <> dataSetName rtt

addGroupIndexForGQData :: forall r k md gq.Typeable k
                        => GroupTypeTag k
                        -> RowTypeTag r
                        -> MakeIndex r k
                        -> StanGroupBuilderM md gq ()
addGroupIndexForGQData gtt rtt mkIndex = do
  rims <- gets gbGQS
  case DHash.lookup rtt rims of
    Nothing -> stanGroupBuildError $ "Data-set \"" <> dataSetName rtt <> "\" needs to be added to GQ data before groups can be added to it."
    Just (GroupIndexAndIntMapMakers dsu tf (GroupIndexMakers gims) gimbs) -> case DHash.lookup gtt gims of
      Nothing -> modify $ modifyGBGQS $ DHash.insert rtt (GroupIndexAndIntMapMakers dsu tf (GroupIndexMakers $ DHash.insert gtt mkIndex gims) gimbs)
      Just _ -> stanGroupBuildError $ "Attempt to add a second group (\"" <> taggedGroupName gtt <> "\") at the same type for GQ row=" <> dataSetName rtt

addGroupIndexForModelCrosswalk :: forall k r md gq.
                                  Typeable k
                               => RowTypeTag r
                               -> MakeIndex r k
                               -> StanGroupBuilderM d ()
addGroupIndexForModelCrosswalk rtt mkIndex = do
  let gttX :: GroupTypeTag k = GroupTypeTag $ "I_" <> (dataSetName rtt)
  addGroupIndexForDataSet gttX rtt mkIndex


addGroupIntMapForDataSet :: forall r k d.Typeable k => GroupTypeTag k -> RowTypeTag r -> DataToIntMap r k -> StanGroupBuilderM d ()
addGroupIntMapForDataSet gtt rtt mkIntMap = do
  rims <- get
  case DHash.lookup rtt rims of
    Nothing -> stanGroupBuildError $ "Data-set \"" <> dataSetName rtt <> "\" needs to be added before groups can be added to it."
    Just (GroupIndexAndIntMapMakers dsu tf gims (GroupIntMapBuilders gimbs)) -> case DHash.lookup gtt gimbs of
      Nothing -> put $ DHash.insert rtt (GroupIndexAndIntMapMakers dsu tf gims (GroupIntMapBuilders $ DHash.insert gtt mkIntMap gimbs)) rims
      Just _ -> stanGroupBuildError $ "Attempt to add a second group (\"" <> taggedGroupName gtt <> "\") at the same type for row=" <> dataSetName rtt

-- This builds the indexes but not the IntMaps.  Those need to be built at the end.
runStanGroupBuilder :: Typeable d=> StanGroupBuilderM d () -> d -> BuilderState d
runStanGroupBuilder sgb d =
  let (resE, rims) = usingState DHash.empty $ runExceptT $ unStanGroupBuilderM sgb
      rowInfos = DHash.mapWithKey (buildRowInfo d) rims
  in initialBuilderState rowInfos


newtype StanBuilderM md gq a = StanBuilderM { unStanBuilderM :: ExceptT Text (State (BuilderState md gq)) a }
                             deriving (Functor, Applicative, Monad, MonadState (BuilderState md gq))

askUserEnv :: StanBuilderM env d env
askUserEnv = ask

dataSetTag :: forall r env d. Typeable r => Text -> StanBuilderM env d (RowTypeTag r)
dataSetTag name = do
  let rtt :: RowTypeTag r = RowTypeTag name
  rowInfos <- rowBuilders <$> get
  case DHash.lookup rtt rowInfos of
    Nothing -> stanBuildError
               $ "Requested data-set "
               <> name
               <> " is missing from row builders. Perhaps you forgot to add it in the groupBuilder or model code?"
               <> "\n data-sets=" <> show (DHash.keys rowInfos)
    Just _ -> return rtt

addFunctionsOnce :: Text -> StanBuilderM env d () -> StanBuilderM env d ()
addFunctionsOnce functionsName fCode = do
  (BuilderState vars ibs rowBuilders cj fsNames code) <- get
  if functionsName `Set.member` fsNames
    then return ()
    else (do
             fCode
             modify $ modifyFunctionNames $ Set.insert functionsName
         )

-- TODO: We should error if added twice but that doesn't quite work with post-stratifation code now.  Fix.
addIntMapBuilder :: RowTypeTag r
                 -> GroupTypeTag k
                 -> DataToIntMap r k
                 -> StanBuilderM env d ()
addIntMapBuilder rtt gtt dim = do
  BuilderState vars ib rowInfos jf hf code <- get
  case DHash.lookup rtt rowInfos of
    Nothing -> stanBuildError $ "addIntMapBuilders: Data-set \"" <> dataSetName rtt <> " not present in rowBuilders."
    Just (RowInfo tf ebs gis (GroupIntMapBuilders gimbs) js dsu) -> case DHash.lookup gtt gimbs of
      Just _ -> return () --stanBuildError $ "addIntMapBuilder: Group \"" <> taggedGroupName gtt <> "\" is already present for data-set \"" <> dataSetName rtt <> "\"."
      Nothing -> do
        let newRowInfo = RowInfo tf ebs gis (GroupIntMapBuilders $ DHash.insert gtt dim gimbs) js dsu
            newRowInfos = DHash.insert rtt newRowInfo rowInfos
        put $ BuilderState vars ib newRowInfos jf hf code

intMapsBuilder :: forall d env. StanBuilderM env d (d -> Either Text DataSetGroupIntMaps)
intMapsBuilder = intMapsFromRowInfos . rowBuilders <$> get

intMapsFromRowInfos :: RowInfos d -> d -> Either Text DataSetGroupIntMaps
intMapsFromRowInfos rowInfos d =
  let f :: d -> RowInfo d r -> Either Text (GroupIntMaps r)
      f d (RowInfo (ToFoldable h) _ _ gims _ _) = Foldl.foldM (intMapsForDataSetFoldM gims) (h d)
  in DHash.traverse (f d) rowInfos

addDataSet :: Typeable r
           => Text
           -> ToFoldable d r
           -> StanBuilderM env d (RowTypeTag r)
addDataSet name toFoldable = do
  let rtt = RowTypeTag name
  (BuilderState vars ibs rowBuilders modelExprs code ims) <- get
  case DHash.lookup rtt rowBuilders of
    Just _ -> stanBuildError "Attempt to add 2nd data set with same type and name"
    Nothing -> do
      let rowInfo = unIndexedRowInfo toFoldable name
          newRowBuilders = DHash.insert rtt rowInfo rowBuilders
      put (BuilderState vars ibs newRowBuilders modelExprs code ims)
  return rtt

dataSetCrosswalkName :: RowTypeTag rFrom -> RowTypeTag rTo -> SME.StanName
dataSetCrosswalkName rttFrom rttTo = "XWalk_" <> dataSetName rttFrom <> "_" <> dataSetName rttTo
{-# INLINEABLE dataSetCrosswalkName #-}

crosswalkIndexKey :: RowTypeTag rTo -> SME.StanName
crosswalkIndexKey rttTo = "XWalkTo_" <> dataSetName rttTo
{-# INLINEABLE crosswalkIndexKey #-}

-- build an index from each data-sets relationship to the common group.
-- add Json and use-binding
addDataSetsCrosswalk :: forall k rFrom rTo env d.
                        (Typeable d, Typeable k)
                     => RowTypeTag rFrom
                     -> RowTypeTag rTo
                     -> GroupTypeTag k
                     -> StanBuilderM env d ()
addDataSetsCrosswalk  rttFrom rttTo gtt = do
  let gttX :: GroupTypeTag k = GroupTypeTag $ "I_" <> (dataSetName rttTo)
  fromToGroup <- rowToGroup <$> indexMap rttFrom gtt -- rFrom -> k
  toGroupToIndexE <- groupKeyToGroupIndex <$> indexMap rttTo gttX -- k -> Either Text Int
  let xWalkName = dataSetCrosswalkName rttFrom rttTo
      xWalkIndexKey = crosswalkIndexKey rttTo
      xWalkType = SME.StanArray [SME.NamedDim $ dataSetName rttFrom] SME.StanInt
      xWalkVar = SME.StanVar xWalkName xWalkType
      xWalkF = toGroupToIndexE . fromToGroup
  addColumnMJson rttFrom xWalkName xWalkType "<lower=1>" xWalkF
--  addUseBindingToDataSet rttFrom xWalkIndexKey $ SME.indexBy (SME.name xWalkName) $ dataSetName rttFrom
  addUseBindingToDataSet rttFrom xWalkIndexKey xWalkVar
  addDeclBinding xWalkIndexKey $ SME.StanVar ("N_" <> dataSetName rttFrom) SME.StanInt
  return ()


duplicateDataSetBindings :: Foldable f => RowTypeTag r -> f (Text, Text) -> StanBuilderM env d ()
duplicateDataSetBindings rtt dups = do
  let doOne ebs (current, new) = case Map.lookup current ebs of
        Nothing -> stanBuildError $ "duplicateDataSetBindings: " <> current <> " is not in existing bindings."
        Just e -> return $ (new, e)
  (BuilderState vars ibs rims modelExprs code ims) <- get
  case DHash.lookup rtt rims of
    Nothing -> stanBuildError $ "duplicateDataSetBindings: Data-set " <> dataSetName rtt <> " is not in environment."
    Just (RowInfo tf ebs gis gimbs j dsu) -> do
      newKVs <- traverse (doOne ebs) (Foldl.fold Foldl.list dups)
      let newEbs = Map.union ebs $ Map.fromList newKVs
          newRowInfo = RowInfo tf newEbs gis gimbs j dsu
          newRowBuilders = DHash.insert rtt newRowInfo rims
      put (BuilderState vars ibs newRowBuilders modelExprs code ims)

rowInfo :: RowTypeTag r -> StanBuilderM env d (RowInfo d r)
rowInfo rtt = do
  rowInfos <- rowBuilders <$> get
  case DHash.lookup rtt rowInfos of
    Nothing -> stanBuildError $ "rowInfo: data-set " <> dataSetName rtt <> " not found in row-builders."
    Just ri -> return ri

indexMap :: RowTypeTag r -> GroupTypeTag k -> StanBuilderM env d (IndexMap r k)
indexMap rtt gtt = do
  rowInfos <- rowBuilders <$> get
  case DHash.lookup rtt rowInfos of
    Nothing -> stanBuildError $ "ModelBuilder.indexMap: \"" <> dataSetName rtt <> "\" not present in row builders."
    Just ri -> case DHash.lookup gtt ((\(GroupIndexes x) -> x) $ groupIndexes ri) of
                 Nothing -> stanBuildError
                            $ "ModelBuilder.indexMap: \""
                            <> taggedGroupName gtt
                            <> "\" not present in indexes for \""
                            <> dataSetName rtt <> "\""
                 Just im -> return im

runStanBuilder :: (Typeable d)
               => d
               -> env
               -> StanGroupBuilderM d ()
               -> StanBuilderM env d a
               -> Either Text (BuilderState d, a)
runStanBuilder d userEnv sgb sb =
  let builderState = runStanGroupBuilder sgb d
      (resE, bs) = usingState builderState . usingReaderT userEnv . runExceptT $ unStanBuilderM $ sb
  in fmap (bs,) resE

stanBuildError :: Text -> StanBuilderM env d a
stanBuildError t = do
  builderText <- dumpBuilderState <$> get
  StanBuilderM $ ExceptT (pure $ Left $ t <> "\nBuilder:\n" <> builderText)

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

addDeclBinding' :: IndexKey -> SME.StanExpr -> StanBuilderM env d ()
addDeclBinding' k e = modify $ modifyIndexBindings f where
  f (SME.VarBindingStore ubm dbm) = SME.VarBindingStore ubm (Map.insert k e dbm)

addDeclBinding :: IndexKey -> SME.StanVar -> StanBuilderM env d ()
addDeclBinding k sv = addDeclBinding' k (SME.var sv)

addUseBinding' :: IndexKey -> SME.StanExpr -> StanBuilderM env d ()
addUseBinding' k e = modify $ modifyIndexBindings f where
  f (SME.VarBindingStore ubm dbm) = SME.VarBindingStore (Map.insert k e ubm) dbm

addUseBinding :: IndexKey -> SME.StanVar -> StanBuilderM env d ()
addUseBinding k sv = addUseBinding' k (SME.var sv)

addUseBindingToDataSet' :: RowTypeTag r -> IndexKey -> SME.StanExpr -> StanBuilderM env d ()
addUseBindingToDataSet' rtt key e = do
  let dataNotFoundErr = "addUseBindingToDataSet: Data-set \"" <> dataSetName rtt <> "\" not found in StanBuilder.  Maybe you haven't added it yet?"
      bindingChanged newExpr oldExpr = when (newExpr /= oldExpr)
                                       $ Left
                                       $ "addUseBindingToDataSet: key="
                                       <> show key
                                       <> "\nAttempt to add different use binding to already-bound key. Old expression="
                                       <> show oldExpr
                                       <> "; new expression="
                                       <> show newExpr
      f rowInfos = do
        (RowInfo tf ebs gis gimbs js dsu) <- maybeToRight dataNotFoundErr $ DHash.lookup rtt rowInfos
        maybe (Right ()) (bindingChanged e) $ Map.lookup key ebs
        let ebs' = Map.insert key e ebs
        return $ DHash.insert rtt (RowInfo tf ebs' gis gimbs js dsu) rowInfos
  bs <- get
  bs' <- stanBuildEither $ modifyRowInfosA f bs
  put bs'

addUseBindingToDataSet :: RowTypeTag r -> IndexKey -> SME.StanVar -> StanBuilderM env d ()
addUseBindingToDataSet rtt key sv = addUseBindingToDataSet' rtt key (SME.var sv)

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

isDeclaredAllScopes :: SME.StanName -> StanBuilderM env d Bool
isDeclaredAllScopes sn  = do
  sd <- declaredVars <$> get
  case varLookupAllScopes sd sn of
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
addJsonOnce :: (Typeable d)
                 => RowTypeTag r
                 -> SME.StanName
                 -> SME.StanType
                 -> Text
                 -> Stan.StanJSONF r Aeson.Series
                 -> StanBuilderM env d SME.StanVar
addJsonOnce rtt name st sc fld = do
  alreadyDeclared <- isDeclaredAllScopes name
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
  alreadyDeclared <- isDeclaredAllScopes name
  if not alreadyDeclared
    then addFixedIntJson name mLower n
    else return $ SME.StanVar name SME.StanInt

-- These get re-added each time something adds a column built from the data-set.
-- But we only need it once per data set.
addLengthJson :: (Typeable d) => RowTypeTag r -> Text -> SME.IndexKey -> StanBuilderM env d SME.StanVar
addLengthJson rtt name iKey = do
--  addDeclBinding' iKey (SME.name name)
  addDeclBinding iKey $ SME.StanVar name SME.StanInt
  addJsonOnce rtt name SME.StanInt "<lower=1>" (Stan.namedF name Foldl.length)

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
  let sizeName = "N" <> underscoredIf dsName
  addLengthJson rtt sizeName dsName -- ??
--  let fullName = name <> "_" <> dsName
  addJson rtt name st sc (Stan.valueToPairF name $ Stan.jsonArrayF toX)

addColumnJsonOnce :: (Typeable d, Aeson.ToJSON x)
                  => RowTypeTag r
                  -> Text
                  -> SME.StanType
                  -> Text
                  -> (r -> x)
                  -> StanBuilderM env d SME.StanVar
addColumnJsonOnce rtt name st sc toX = do
  alreadyDeclared <- isDeclared name
  if not alreadyDeclared
    then addColumnJson rtt name st sc toX
    else return $ SME.StanVar name st

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
--  let fullName = name <> "_" <> dsName
  addJson rtt name st sc (Stan.valueToPairF name $ Stan.jsonArrayEF toMX)

addColumnMJsonOnce :: (Typeable d, Aeson.ToJSON x)
               => RowTypeTag r
               -> Text
               -> SME.StanType
               -> Text
               -> (r -> Either Text x)
               -> StanBuilderM env d SME.StanVar
addColumnMJsonOnce rtt name st sc toMX = do
  alreadyDeclared <- isDeclared name
  if not alreadyDeclared
    then addColumnMJson rtt name st sc toMX
    else return $ SME.StanVar name st

-- NB: name has to be unique so it can also be the suffix of the num columns.  Presumably the name carries the data-set suffix if nec.
data MatrixRowFromData r = MatrixRowFromData { rowName :: Text, rowLength :: Int, rowVec :: r -> Vector.Vector Double }

add2dMatrixJson :: (Typeable d)
                => RowTypeTag r
                -> MatrixRowFromData r
--                -> Text
                -> SME.DataConstraint
                -> SME.StanDim
--                -> Int
--                -> (r -> Vector.Vector Double)
                -> StanBuilderM env d SME.StanVar
add2dMatrixJson rtt (MatrixRowFromData name cols vecF) sc rowDim = do
  let dsName = dataSetName rtt
      wdName = name <> underscoredIf dsName
      colName = "K" <> "_" <> name
      colDimName = name <> "_Cols"
      colDimVar = SME.StanVar colDimName SME.StanInt
  addFixedIntJson' colName Nothing cols
--  addDeclBinding' colDimName (SME.name colName)
  addDeclBinding colDimName $ SME.StanVar colName SME.StanInt
--  addUseBindingToDataSet rtt colDimName (SME.name colName)
  addUseBindingToDataSet rtt colDimName colDimVar
  addColumnJson rtt wdName (SME.StanMatrix (rowDim, SME.NamedDim colDimName)) sc vecF

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
  modifyCode (indent $ Prelude.negate n)
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
--    addUseBinding' k (SME.name counter)
    addUseBinding k $ SME.StanVar counter SME.StanInt
    bracketed 2 x

loopOverNamedDims :: [SME.StanDim] -> StanBuilderM env d a -> StanBuilderM env d a
loopOverNamedDims dims = foldr (\f g -> g . f) id $ fmap loopFromDim dims where
  loopFromDim :: SME.StanDim -> StanBuilderM env d a -> StanBuilderM env d a
  loopFromDim (SME.NamedDim ik) = stanForLoopB ("n_" <> ik) Nothing ik
  loopFromDim _ = const $ stanBuildError $ "groupBetaPrior:  given beta must have all named dimensions"

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

varScopeBlock :: StanBlock -> StanBuilderM env d ()
varScopeBlock sb = case sb of
  SBModel -> modify (modifyDeclaredVars $ changeVarScope ModelScope)
  SBGeneratedQuantities -> modify (modifyDeclaredVars $ changeVarScope GQScope)
  _ -> modify (modifyDeclaredVars $ changeVarScope GlobalScope)

inBlock :: StanBlock -> StanBuilderM env d a -> StanBuilderM env d a
inBlock b m = do
  oldBlock <- getBlock
  setBlock b
  varScopeBlock b
  x <- m
  setBlock oldBlock
  varScopeBlock oldBlock
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

exprVarsM :: SME.StanExpr -> StanBuilderM env d (Set StanVar)
exprVarsM e = do
  vbs <- indexBindings <$> get
  case SME.exprVars vbs e of
    Left err -> stanBuildError $ "exprVarM: " <> err
    Right t -> return $ Set.fromList t -- get unique only

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

--modelFile :: T.Text -> T.Text
--modelFile modelNameT = modelNameT <> ".stan"

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

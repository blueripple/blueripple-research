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
  , module Stan.ModelBuilder.BuilderTypes
  , module Stan.ModelBuilder.Expressions
  , module Stan.ModelBuilder.Distributions
  )
where

import qualified Stan.JSON as Stan
import qualified Stan.ModelBuilder.Expressions as SME
import Stan.ModelBuilder.Expressions
import Stan.ModelBuilder.Distributions
import Stan.ModelBuilder.BuilderTypes

import Prelude hiding (All)
import qualified Control.Foldl as Foldl
import qualified Data.Aeson as Aeson
import qualified Data.Array as Array
import qualified Data.Constraint.Extras.TH as CE
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
import qualified Data.Vector.Unboxed as VU
import qualified Data.Hashable as Hashable
import qualified Say
import qualified System.Directory as Dir
import qualified System.Environment as Env
import qualified Type.Reflection as Reflection
import qualified Data.GADT.Show as GADT
import Frames.Streamly.TH (declareColumnType)
import Text.Printf (errorMissingArgument)
import Stan.ModelConfig (InputDataType(..))
import Knit.Report (crimson, getting)
import qualified Data.Hashable.Lifted as Hashable


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

applyToFoldable :: Foldl.Fold row a -> ToFoldable d row -> d -> a
applyToFoldable fld (ToFoldable f) = Foldl.fold fld . f

applyToFoldableM :: Monad m => Foldl.FoldM m row a -> ToFoldable d row -> d -> m a
applyToFoldableM fldM (ToFoldable f) = Foldl.foldM fldM . f

-- the key is a name for the data-set.  The tag carries the toDataSet function
type RowBuilder d = DSum.DSum RowTypeTag (RowInfo d)
type RowInfos d = DHash.DHashMap RowTypeTag (RowInfo d)

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

-- HERE
dataToIntMapFromKeyedRow :: (r -> k) -> DataToIntMap r k
dataToIntMapFromKeyedRow key = DataToIntMap $ Foldl.generalize fld where
  fld = fmap (IntMap.fromList . zip [1..]) $ Foldl.premap key Foldl.list


addRowKeyIntMapToGroupBuilder :: Typeable k => RowTypeTag r -> GroupTypeTag k -> (r -> k) ->  StanGroupBuilderM md gq ()
addRowKeyIntMapToGroupBuilder rtt gtt = addGroupIntMapForDataSet gtt rtt . dataToIntMapFromKeyedRow

addRowKeyIntMap :: Typeable k => RowTypeTag r -> GroupTypeTag k -> (r -> k) ->  StanBuilderM md gq ()
addRowKeyIntMap rtt gtt = addIntMapBuilder rtt gtt . dataToIntMapFromKeyedRow

addIntMapFromGroup :: RowTypeTag r -> GroupTypeTag k -> (r -> k) -> StanBuilderM md gq ()
addIntMapFromGroup rtt gtt rowToGroup = do
  let dataMissingErr = "addIntMapFromGroup: data-set "
                         <> dataSetName rtt
                         <> " is missing from " <> show (inputDataType rtt) <> " rowBuilders."
      groupIndexMissingErr =  "addIntMapFromGroup: group "
                              <> taggedGroupName gtt
                              <> " is missing from " <> show (inputDataType rtt)
                              <> "data-set ("
                              <> dataSetName rtt
                              <> ")."
  let f :: RowInfo x r -> StanBuilderM md gq ()
      f rowInfo = do
        let (GroupIndexes gis) = groupIndexes rowInfo
        kToIntE <- groupKeyToGroupIndex <$> stanBuildMaybe groupIndexMissingErr (DHash.lookup gtt gis)
        addIntMapBuilder rtt gtt $  buildIntMapBuilderF kToIntE rowToGroup -- for extracting results
  withRowInfo (stanBuildError dataMissingErr) f rtt

-- To HERE
getGroupIndex :: forall d r k. Typeable k
              => RowTypeTag r
              -> GroupTypeTag k
              -> DataSetGroupIntMaps
              -> Either Text (IntMap k)
getGroupIndex rtt gtt groupIndexes =
  case DHash.lookup rtt groupIndexes of
    Nothing -> Left
               $ dataSetName rtt <> " (idt="
               <> show (inputDataType rtt) <> ") not found in data-set group int maps: " <> displayDataSetGroupIntMaps groupIndexes
    Just (GroupIntMaps gim) -> case DHash.lookup gtt gim of
      Nothing -> Left $ "\"" <> taggedGroupName gtt <> "\" not found in Group int maps for data-set \"" <> dataSetName rtt <> "\""
      Just im -> Right im

groupIndexVarName :: RowTypeTag r -> GroupTypeTag k -> StanName
groupIndexVarName rtt gtt = dataSetName rtt <> "_" <> taggedGroupName gtt
{-# INLINEABLE groupIndexVarName #-}

getGroupIndexVar :: forall md gq r k.
                    RowTypeTag r
                 -> GroupTypeTag k
                 -> StanBuilderM md gq StanVar
getGroupIndexVar rtt gtt = do
  let varName = groupIndexVarName rtt gtt
      dsNotFoundErr = stanBuildError
                      $ "getGroupIndexVar: data-set=" <> dataSetName rtt <> " (input type=" <> show (inputDataType rtt) <> ") not found."
      varIfGroup :: forall x d.RowInfo d x -> StanBuilderM md gq SME.StanVar
      varIfGroup ri =
        let (GroupIndexes gis) = groupIndexes ri
        in case DHash.lookup gtt gis of
          Just _ -> return $ SME.StanVar varName (SME.StanArray [SME.NamedDim $ dataSetName rtt] SME.StanInt)
          Nothing -> stanBuildError
            $ "getGroupIndexVar: group=" <> taggedGroupName gtt
            <> " not found in data-set=" <> dataSetName rtt <> " (input type=" <> show (inputDataType rtt) <> ") not found."
  withRowInfo dsNotFoundErr varIfGroup rtt


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
buildRowInfo d rtt (GroupIndexAndIntMapMakers tf@(ToFoldable f) ims imbs) = Foldl.fold fld $ f d  where
  gisFld = indexBuildersForDataSetFold $ ims
--  imsFldM = intMapsForDataSetFoldM imbs
  useBindings = Map.insert (dataSetName rtt) (SME.name $ "N_" <> dataSetName rtt) $ useBindingsFromGroupIndexMakers rtt ims
  fld = RowInfo tf useBindings <$> gisFld <*> pure imbs <*> pure mempty

groupIntMap :: IndexMap r k -> IntMap.IntMap k
groupIntMap (IndexMap _ _ im _) = im

-- data DataSetUse = ModelUse | GQOnlyUse deriving (Show, Eq, Ord, Enum, Bounded)


unIndexedRowInfo :: forall d r. ToFoldable d r -> Text -> RowInfo d r
unIndexedRowInfo tf groupName  = RowInfo tf mempty (GroupIndexes DHash.empty) (GroupIntMapBuilders DHash.empty) mempty


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
      foldOne ((RowTypeTag _ _) DSum.:=> ri@(RowInfo (ToFoldable f) _ _ _ (JSONSeriesFold fld))) = Foldl.foldM fld (f d)
  in mconcat <$> (traverse foldOne $ DHash.toList rbm)

data JSONRowFold d r = JSONRowFold (ToFoldable d r) (Stan.StanJSONF r Aeson.Series)

buildJSONFromRows :: DHash.DHashMap RowTypeTag (JSONRowFold d) -> d -> Either Text Aeson.Series
buildJSONFromRows rowFoldMap d = do
  let toSeriesOne (rtt DSum.:=> JSONRowFold (ToFoldable tf) fld) = Foldl.foldM fld (tf d)
  fmap mconcat $ traverse toSeriesOne $ DHash.toList rowFoldMap

-- The Maybe return values are there just to satisfy the (returned) type of DHash.traverseWithKey
buildGroupIndexes :: (Typeable md, Typeable gq) => StanBuilderM md gq ()
buildGroupIndexes = do
  let buildIndexJSONFold :: (Typeable md, Typeable gq) => RowTypeTag r -> GroupTypeTag k -> IndexMap r k -> StanBuilderM md gq (Maybe k)
      buildIndexJSONFold rtt gtt@(GroupTypeTag gName) (IndexMap (IntIndex gSize mIntF) _ _ _) = do
        let dsName = dataSetName rtt
            indexName = groupIndexVarName rtt gtt --dsName <> "_" <> gName
        addFixedIntJson' ("J_" <> gName) (Just 1) gSize
        _ <- addColumnMJson rtt indexName (SME.StanArray [SME.NamedDim dsName] SME.StanInt) "<lower=1>" mIntF
--        addDeclBinding gName $ SME.name $ "J_" <> gName -- ??
        addDeclBinding gName $ SME.StanVar ("J_" <> gName) SME.StanInt
--        addUseBinding dsName gName $ SME.indexBy (SME.name gName) dsName
        return Nothing
      buildRowFolds :: (Typeable md, Typeable gq) => RowTypeTag r -> RowInfo d r -> StanBuilderM md gq (Maybe r)
      buildRowFolds rtt (RowInfo _ _ (GroupIndexes gis) _ _) = do
        _ <- DHash.traverseWithKey (buildIndexJSONFold rtt) gis
        return Nothing
  _ <- gets modelRowBuilders >>= DHash.traverseWithKey buildRowFolds
  _ <- gets gqRowBuilders >>= DHash.traverseWithKey buildRowFolds
  return ()

buildModelJSONF :: forall md gq.(Typeable md) => StanBuilderM md gq (DHash.DHashMap RowTypeTag (JSONRowFold md))
buildModelJSONF = do
  rowInfos <- modelRowBuilders <$> get
  let buildRowJSONFolds :: RowInfo md r -> StanBuilderM md gq (JSONRowFold md r)
      buildRowJSONFolds (RowInfo tf _ _ _ (JSONSeriesFold jsonF)) = return $ JSONRowFold tf jsonF
  DHash.traverse buildRowJSONFolds rowInfos

buildGQJSONF :: forall md gq.(Typeable gq) => StanBuilderM md gq (DHash.DHashMap RowTypeTag (JSONRowFold gq))
buildGQJSONF = do
  rowInfos <- gqRowBuilders <$> get
  DHash.traverse buildRowJSONFolds rowInfos

buildRowJSONFolds :: RowInfo d r -> StanBuilderM env d (JSONRowFold d r)
buildRowJSONFolds ri = return $ JSONRowFold (toFoldable ri) jsonF where
  JSONSeriesFold jsonF = jsonSeries ri

buildModelJSONFromDataM :: Typeable md => StanBuilderM md gq (md -> Either Text Aeson.Series)
buildModelJSONFromDataM = do
  (JSONSeriesFold constJSONFld) <- constJSON <$> get
  dataSetJSON <- buildModelJSONF
  return $ \d ->
    let c = Foldl.foldM constJSONFld (Just ())
        ds =  buildJSONFromRows dataSetJSON d
    in (<>) <$> c <*> ds

buildGQJSONFromDataM :: Typeable gq => StanBuilderM md gq (gq -> Either Text Aeson.Series)
buildGQJSONFromDataM = do
  dataSetJSON <- buildGQJSONF
  return $ \d -> buildJSONFromRows dataSetJSON d

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

addVarInScope :: SME.StanName -> SME.StanType -> StanBuilderM md gq SME.StanVar
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

getVar :: StanName -> StanBuilderM md gq StanVar
getVar sn = do
  sd <- declaredVars <$> get
  case varLookup sd sn of
    Right sv -> return sv
    Left errMsg -> stanBuildError errMsg

addScope :: StanBuilderM md gq ()
addScope = modify (modifyDeclaredVars f) where
  f sd = setDeclarationsNE dmNE' curScope sd where
    curScope = currentScope sd
    dmNE' = DeclarationMap Map.empty :| toList (declarationsNE curScope sd)

dropScope :: StanBuilderM md gq ()
dropScope = do
  sd <- declaredVars <$> get
  let curScope = currentScope sd
      dmNE = declarationsNE curScope sd
      (_, mx) = NE.uncons dmNE
  case mx of
    Nothing -> stanBuildError $ "Attempt to drop outermost scope!"
    Just dmNE' -> modify (modifyDeclaredVars $ const $ setDeclarationsNE dmNE' curScope sd)

scoped :: StanBuilderM md gq a -> StanBuilderM md gq a
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
  <> "\n index-bindings: " <> show (indexBindings bs)
  <> "\n model row-info-keys: " <> show (DHash.keys $ modelRowBuilders bs)
  <> "\n gq row-info-keys: " <> show (DHash.keys $ gqRowBuilders bs)
  <> "\n functions: " <> show (hasFunctions bs)


--getRowInfo :: InputDataType -> RowTypeTag r

withRowInfo :: StanBuilderM md gq y -> (forall x.RowInfo x r -> StanBuilderM md gq y) -> RowTypeTag r -> StanBuilderM md gq y
withRowInfo missing presentF rtt =
  case inputDataType rtt of
    ModelData -> do
      rowInfos <- modelRowBuilders <$> get
      maybe missing presentF $ DHash.lookup rtt rowInfos
    GQData -> do
      rowInfos <- gqRowBuilders <$> get
      maybe missing presentF $ DHash.lookup rtt rowInfos

getDataSetBindings :: RowTypeTag r -> StanBuilderM md gq (Map SME.IndexKey SME.StanExpr)
getDataSetBindings rtt = withRowInfo err (return . expressionBindings) rtt where
  idt = inputDataType rtt
  err = stanBuildError $ "getDataSetbindings: row-info=" <> dataSetName rtt <> " not found in " <> show idt

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

setDataSetForBindings :: RowTypeTag r -> StanBuilderM md gq ()
setDataSetForBindings rtt = do
  newUseBindings <- getDataSetBindings rtt
  modify $ modifyIndexBindings (\(SME.VarBindingStore _ dbm) -> SME.VarBindingStore newUseBindings dbm)

useDataSetForBindings :: RowTypeTag r -> StanBuilderM md gq a -> StanBuilderM md gq a
useDataSetForBindings rtt x = getDataSetBindings rtt >>= flip withUseBindings x

modifyDeclaredVars :: (ScopedDeclarations -> ScopedDeclarations) -> BuilderState md gq -> BuilderState md gq
modifyDeclaredVars f bs = bs {declaredVars = f (declaredVars bs)}
--(BuilderState dv vbs mrb gqrb cj hf c) = BuilderState (f dv) vbs mrb gqrb cj hf c

modifyDeclaredVarsA :: Applicative t
                    => (ScopedDeclarations -> t ScopedDeclarations)
                    -> BuilderState md gq
                    -> t (BuilderState md gq)
modifyDeclaredVarsA f bs = (\x -> bs { declaredVars = x}) <$> f (declaredVars bs)
-- (BuilderState dv vbs mrb gqrb cj hf c) = (\x -> BuilderState x vbs mrb gqrb cj hf c) <$> f dv

modifyIndexBindings :: (VarBindingStore -> VarBindingStore)
                    -> BuilderState md gq
                    -> BuilderState md gq
modifyIndexBindings f bs = bs {indexBindings = f (indexBindings bs)}
--(BuilderState dv vbs mrb gqrb cj hf c) = BuilderState dv (f vbs) mrb gqrb cj hf c

modifyIndexBindingsA :: Applicative t
                     => (VarBindingStore -> t VarBindingStore)
                     -> BuilderState md gq
                     -> t (BuilderState md gq)
modifyIndexBindingsA f bs = (\x -> bs {indexBindings = x}) <$> f (indexBindings bs)
--(BuilderState dv vbs mrb gqrb cj hf c) = (\x -> BuilderState dv x mrb gqrb cj hf c) <$> f vbs

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
modifyModelRowInfosA f bs = (\x -> bs {modelRowBuilders = x}) <$> f (modelRowBuilders bs)
--(BuilderState dv vbs mrb gqrb cj hf c) = (\x -> BuilderState dv vbs x gqrb cj hf c) <$> f mrb

modifyGQRowInfosA :: Applicative t
                   => (RowInfos gq -> t (RowInfos gq))
                   -> BuilderState md gq
                   -> t (BuilderState md gq)
modifyGQRowInfosA f bs = (\x -> bs {gqRowBuilders = x}) <$> f (gqRowBuilders bs)
--(BuilderState dv vbs mrb gqrb cj hf c) = (\x -> BuilderState dv vbs mrb x cj hf c) <$> f gqrb


modifyConstJson :: (JSONSeriesFold () -> JSONSeriesFold ()) -> BuilderState md gq -> BuilderState md gq
modifyConstJson f bs = bs { constJSON = f (constJSON bs)}
--(BuilderState dvs ibs mrbs gqrbs cj hfs c) = BuilderState dvs ibs mrbs gqrbs (f cj) hfs c

addConstJson :: JSONSeriesFold () -> BuilderState md gq -> BuilderState md gq
addConstJson jf = modifyConstJson (<> jf)

modifyFunctionNames :: (Set Text -> Set Text) -> BuilderState md gq -> BuilderState md gq
modifyFunctionNames f bs = bs { hasFunctions = f (hasFunctions bs)}
--(BuilderState dv vbs mrb gqrb cj hf c) = BuilderState dv vbs mrb gqrb cj (f hf) c

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

withRowInfoMakers :: (forall x. RowInfoMakers x -> StanGroupBuilderM md gq (Maybe (RowInfoMakers x), y)) -> InputDataType -> StanGroupBuilderM md gq y
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
        Just newRims -> modify $ modifyGBGQS $ const newRims
      return y

getDataSetTag :: forall r md gq. Typeable r => InputDataType -> Text -> StanGroupBuilderM md gq (RowTypeTag r)
getDataSetTag idt t = withRowInfoMakers f idt where
  f :: forall x. RowInfoMakers x -> StanGroupBuilderM md gq (Maybe (RowInfoMakers x), RowTypeTag r)
  f rowInfoMakers = do
    let rtt = RowTypeTag @r idt t
    case DHash.lookup rtt rowInfoMakers of
      Nothing -> stanGroupBuildError $ "getModelDataSetTag: No data-set \"" <> t <> "\" found in group builder."
      Just _ -> return (Nothing, rtt)

addModelDataToGroupBuilder :: forall md gq r. Typeable r
                           =>  Text
                           -> ToFoldable md r
                           -> StanGroupBuilderM md gq (RowTypeTag r)
addModelDataToGroupBuilder dsName tf = do
  let rtt = RowTypeTag @r ModelData dsName
  infoMakers <- gets gbModelS
  case DHash.lookup rtt infoMakers of
    Just _ -> stanGroupBuildError $ "Data-set \"" <> dataSetName rtt <> "\" already set up in Group Builder (Model)"
    Nothing -> do
      let newRims = DHash.insert rtt (GroupIndexAndIntMapMakers tf (GroupIndexMakers DHash.empty) (GroupIntMapBuilders DHash.empty)) infoMakers
      modify $ \x -> x { gbModelS = newRims}
      return rtt

addGQDataToGroupBuilder :: forall md gq r. Typeable r
                           => Text
                           -> ToFoldable gq r
                           -> StanGroupBuilderM md gq (RowTypeTag r)
addGQDataToGroupBuilder dsName tf = do
  let rtt = RowTypeTag @r GQData dsName
  infoMakers <- gets gbGQS
  case DHash.lookup rtt infoMakers of
    Just _ -> stanGroupBuildError $ "Data-set \"" <> dataSetName rtt <> "\" already set up in Group Builder (GQ)"
    Nothing -> do
      let newRims = DHash.insert rtt (GroupIndexAndIntMapMakers tf (GroupIndexMakers DHash.empty) (GroupIntMapBuilders DHash.empty)) infoMakers
      modify $ \x -> x { gbGQS = newRims}
      return rtt

addGroupIndexForData :: forall r k md gq.Typeable k
                        => GroupTypeTag k
                        -> RowTypeTag r
                        -> MakeIndex r k
                        -> StanGroupBuilderM md gq ()
addGroupIndexForData gtt rtt mkIndex = withRowInfoMakers f idt where
  idt = inputDataType rtt
  f :: forall x. RowInfoMakers x -> StanGroupBuilderM md gq (Maybe (RowInfoMakers x), ())
  f rowInfoMakers = do
    case DHash.lookup rtt rowInfoMakers of
      Nothing -> stanGroupBuildError $ "Data-set \"" <> dataSetName rtt <> "\" needs to be added to " <> show idt <> " before groups can be added to it."
      Just (GroupIndexAndIntMapMakers tf (GroupIndexMakers gims) gimbs) -> case DHash.lookup gtt gims of
        Just _ -> stanGroupBuildError
                  $ "Attempt to add a second group (\"" <> taggedGroupName gtt <> "\") at the same type for " <> show idt <> " row=" <> dataSetName rtt
        Nothing -> do
          let newRims = DHash.insert rtt (GroupIndexAndIntMapMakers tf (GroupIndexMakers $ DHash.insert gtt mkIndex gims) gimbs) rowInfoMakers
          return (Just newRims, ())

addGroupIndexForModelCrosswalk :: forall k r md gq.Typeable k
                               => RowTypeTag r
                               -> MakeIndex r k
                               -> StanGroupBuilderM md gq ()
addGroupIndexForModelCrosswalk rtt mkIndex = do
  let gttX :: GroupTypeTag k = GroupTypeTag $ "I_" <> (dataSetName rtt)
      idt = inputDataType rtt
  addGroupIndexForData gttX rtt mkIndex

addGroupIntMapForDataSet :: forall r k md gq.Typeable k
                         => GroupTypeTag k
                         -> RowTypeTag r
                         -> DataToIntMap r k
                         -> StanGroupBuilderM md gq ()
addGroupIntMapForDataSet gtt rtt mkIntMap = withRowInfoMakers f idt where
  idt = inputDataType rtt
  f :: forall x. RowInfoMakers x -> StanGroupBuilderM md gq (Maybe (RowInfoMakers x), ())
  f rowInfoMakers = do
    case DHash.lookup rtt rowInfoMakers of
      Nothing -> stanGroupBuildError $ "Data-set \"" <> dataSetName rtt <> "\" needs to be added before groups can be added to it."
      Just (GroupIndexAndIntMapMakers tf gims (GroupIntMapBuilders gimbs)) -> case DHash.lookup gtt gimbs of
        Just _ -> stanGroupBuildError $ "Attempt to add a second group (\"" <> taggedGroupName gtt <> "\") at the same type for row=" <> dataSetName rtt
        Nothing -> do
          let newRims = DHash.insert rtt (GroupIndexAndIntMapMakers tf gims (GroupIntMapBuilders $ DHash.insert gtt mkIntMap gimbs)) rowInfoMakers
          return (Just newRims, ())

-- This builds the indexes but not the IntMaps.  Those need to be built at the end.
runStanGroupBuilder :: (Typeable md, Typeable gq) => StanGroupBuilderM md gq () -> md -> gq -> BuilderState md gq
runStanGroupBuilder sgb md gq =
  let (resE, gbs) = usingState (GroupBuilderS DHash.empty DHash.empty) $ runExceptT $ unStanGroupBuilderM sgb
      modelRowInfos = DHash.mapWithKey (buildRowInfo md) $ gbModelS gbs
      gqRowInfos = DHash.mapWithKey (buildRowInfo gq) $ gbGQS gbs
  in initialBuilderState modelRowInfos gqRowInfos


newtype StanBuilderM md gq a = StanBuilderM { unStanBuilderM :: ExceptT Text (State (BuilderState md gq)) a }
                             deriving (Functor, Applicative, Monad, MonadState (BuilderState md gq))

dataSetTag :: forall r env md gq. Typeable r => InputDataType -> Text -> StanBuilderM md gq (RowTypeTag r)
dataSetTag idt name = do
  let rtt :: RowTypeTag r = RowTypeTag idt name
      err = stanBuildError $ "dataSetTag: Data-set " <> name <> " not found in " <> show idt <> " row builders."
  withRowInfo err (const $ return rtt) rtt
{-
  rowInfos <- rowBuilders <$> get
  case DHash.lookup rtt rowInfos of
    Nothing -> stanBuildError
               $ "Requested data-set "
               <> name
               <> " is missing from row builders. Perhaps you forgot to add it in the groupBuilder or model code?"
               <> "\n data-sets=" <> show (DHash.keys rowInfos)
    Just _ -> return rtt
-}

addFunctionsOnce :: Text -> StanBuilderM md gq () -> StanBuilderM md gq ()
addFunctionsOnce functionsName fCode = do
--  (BuilderState vars ibs rowBuilders cj fsNames code) <- get
  fsNames <- gets hasFunctions
  if functionsName `Set.member` fsNames
    then return ()
    else (do
             fCode
             modify $ modifyFunctionNames $ Set.insert functionsName
         )

-- TODO: We should error if added twice but that doesn't quite work with post-stratifation code now.  Fix.
addIntMapBuilder :: forall r k md gq.
                    RowTypeTag r
                 -> GroupTypeTag k
                 -> DataToIntMap r k
                 -> StanBuilderM md gq ()
addIntMapBuilder rtt gtt dim = do
  let idt = inputDataType rtt
      err :: StanBuilderM md gq a
      err = stanBuildError $ "addIntMapBuilder: Data-set \"" <> dataSetName rtt <> " not present in " <> show idt <> "rowBuilders."
  case idt of
    ModelData -> do
      rowInfo <- whenNothingM (DHash.lookup rtt <$> gets modelRowBuilders) err
      let (GroupIntMapBuilders gimbs) = groupIntMapBuilders rowInfo
      whenNothing_ (DHash.lookup gtt gimbs) $ do
        let newRowInfo = rowInfo {groupIntMapBuilders = GroupIntMapBuilders $ DHash.insert gtt dim gimbs}
            newRowInfoF = DHash.insert rtt newRowInfo
        modify $ runIdentity . modifyModelRowInfosA (Identity . newRowInfoF)
        return ()
    GQData -> do
      rowInfo <- whenNothingM (DHash.lookup rtt <$> gets gqRowBuilders) err
      let (GroupIntMapBuilders gimbs) = groupIntMapBuilders rowInfo
      whenNothing_ (DHash.lookup gtt gimbs) $ do
        let newRowInfo = rowInfo {groupIntMapBuilders = GroupIntMapBuilders $ DHash.insert gtt dim gimbs}
            newRowInfoF = DHash.insert rtt newRowInfo
        modify $ runIdentity . modifyGQRowInfosA (Identity . newRowInfoF)
        return ()

modelIntMapsBuilder :: forall md gq. StanBuilderM md gq (md -> Either Text DataSetGroupIntMaps)
modelIntMapsBuilder = intMapsFromRowInfos . modelRowBuilders <$> get

gqIntMapsBuilder :: forall md gq. StanBuilderM md gq (gq -> Either Text DataSetGroupIntMaps)
gqIntMapsBuilder = intMapsFromRowInfos . gqRowBuilders <$> get

intMapsFromRowInfos :: RowInfos d -> d -> Either Text DataSetGroupIntMaps
intMapsFromRowInfos rowInfos d =
  let f :: d -> RowInfo d r -> Either Text (GroupIntMaps r)
      f d (RowInfo (ToFoldable h) _ _ gims _) = Foldl.foldM (intMapsForDataSetFoldM gims) (h d)
  in DHash.traverse (f d) rowInfos

addModelDataSet :: forall md gq r.Typeable r
                => Text
                -> ToFoldable md r
                -> StanBuilderM md gq (RowTypeTag r)
addModelDataSet name toFoldable = do
  let rtt = RowTypeTag ModelData name
      alreadyPresent _ = stanBuildError "Attempt to add 2nd data set with same type and name in ModelData"
      add = do
        let rowInfo = unIndexedRowInfo toFoldable name
            newRowInfoF = DHash.insert rtt rowInfo
        modify $ runIdentity . modifyModelRowInfosA (Identity . newRowInfoF)
        return rtt
  withRowInfo add alreadyPresent rtt



addGQDataSet :: Typeable r
                => Text
                -> ToFoldable gq r
                -> StanBuilderM md gq (RowTypeTag r)
addGQDataSet name toFoldable = do
  let rtt = RowTypeTag GQData name
      alreadyPresent _ = stanBuildError "Attempt to add 2nd data set with same type and name in ModelData"
      add = do
        let rowInfo = unIndexedRowInfo toFoldable name
            newRowInfoF = DHash.insert rtt rowInfo
        modify $ runIdentity . modifyGQRowInfosA (Identity . newRowInfoF)
        return rtt
  withRowInfo add alreadyPresent rtt



{-
      put (BuilderState vars ibs newRowBuilders modelExprs code ims)
  return rtt

  withRowInfo "addModelDataSet" f ModelData rtt

  (BuilderState vars ibs rowBuilders modelExprs code ims) <- get
  case DHash.lookup rtt rowBuilders of
    Just _ ->
    Nothing -> do
      let rowInfo = unIndexedRowInfo toFoldable name
          newRowBuilders = DHash.insert rtt rowInfo rowBuilders
      put (BuilderState vars ibs newRowBuilders modelExprs code ims)
  return rtt
-}

dataSetCrosswalkName :: RowTypeTag rFrom -> RowTypeTag rTo -> SME.StanName
dataSetCrosswalkName rttFrom rttTo = "XWalk_" <> dataSetName rttFrom <> "_" <> dataSetName rttTo
{-# INLINEABLE dataSetCrosswalkName #-}

crosswalkIndexKey :: RowTypeTag rTo -> SME.StanName
crosswalkIndexKey rttTo = "XWalkTo_" <> dataSetName rttTo
{-# INLINEABLE crosswalkIndexKey #-}

-- build an index from each data-sets relationship to the common group.
-- only works when both data sets are model data or both are GQ data.
-- add Json and use-binding
addDataSetsCrosswalk :: forall k rFrom rTo md gq.(Typeable md, Typeable gq, Typeable k)
                     => RowTypeTag rFrom
                     -> RowTypeTag rTo
                     -> GroupTypeTag k
                     -> StanBuilderM md gq ()
addDataSetsCrosswalk  rttFrom rttTo gtt = do
{-
  when (inputDataType rttFrom /= inputDataType rttTo)
    $ stanBuildError
    $ "addDataSetsCrosswalk: From=" <> dataSetName rttFrom
    <> " (" <> show (inputDataType rttFrom) <> ")  and to="
    <> dataSetName rttTo <> " (" <> show (inputDataType rttTo)
    <> ") have different input data types (Model/GQ)"
-}
  let --idt = inputDataType rttFrom
      gttX :: GroupTypeTag k = GroupTypeTag $ "I_" <> (dataSetName rttTo)
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

{-
duplicateDataSetBindings :: Foldable f => RowTypeTag r -> f (Text, Text) -> StanBuilderM md gq ()
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
-}

modelRowInfo :: RowTypeTag r -> StanBuilderM md gq (RowInfo md r)
modelRowInfo rtt = do
  when (inputDataType rtt /= ModelData) $ stanBuildError "modelRowInfo: GQData row-tag given"
  rowInfos <- gets modelRowBuilders
  case DHash.lookup rtt rowInfos of
    Nothing -> stanBuildError $ "modelRowInfo: data-set=" <> dataSetName rtt <> " not found in " <> show (inputDataType rtt) <> " rowBuilders."
    Just ri -> return ri

gqRowInfo :: RowTypeTag r -> StanBuilderM md gq (RowInfo gq r)
gqRowInfo rtt = do
  when (inputDataType rtt /= GQData) $ stanBuildError "gqRowInfo: ModelData row-tag given"
  rowInfos <- gets gqRowBuilders
  case DHash.lookup rtt rowInfos of
    Nothing -> stanBuildError $ "gqRowInfo: data-set=" <> dataSetName rtt <> " not found in " <> show (inputDataType rtt) <> " rowBuilders."
    Just ri -> return ri

indexMap :: forall r k md gq.RowTypeTag r -> GroupTypeTag k -> StanBuilderM md gq (IndexMap r k)
indexMap rtt gtt = withRowInfo err f rtt where
  err = stanBuildError $ "ModelBuilder.indexMap: \"" <> dataSetName rtt <> "\" not present in row builders."
  f :: forall x. RowInfo x r -> StanBuilderM md gq (IndexMap r k)
  f rowInfo = do
    case DHash.lookup gtt ((\(GroupIndexes x) -> x) $ groupIndexes rowInfo) of
      Nothing -> stanBuildError
                 $ "ModelBuilder.indexMap: \""
                 <> taggedGroupName gtt
                 <> "\" not present in indexes for \""
                 <> dataSetName rtt <> "\" (" <> show (inputDataType rtt) <> ")"
      Just im -> return im
{-
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
-}

runStanBuilder :: (Typeable md, Typeable gq)
               => md
               -> gq
               -> StanGroupBuilderM md gq ()
               -> StanBuilderM md gq a
               -> Either Text (BuilderState md gq, a)
runStanBuilder md gq sgb sb =
  let builderState = runStanGroupBuilder sgb md gq
      (resE, bs) = usingState builderState . runExceptT $ unStanBuilderM $ sb
  in fmap (bs,) resE

stanBuildError :: Text -> StanBuilderM md gq a
stanBuildError t = do
  builderText <- dumpBuilderState <$> get
  StanBuilderM $ ExceptT (pure $ Left $ t <> "\nBuilder:\n" <> builderText)

stanBuildMaybe :: Text -> Maybe a -> StanBuilderM md gq a
stanBuildMaybe msg = maybe (stanBuildError msg) return

stanBuildEither :: Either Text a -> StanBuilderM md gq a
stanBuildEither = either stanBuildError return

getDeclBinding :: IndexKey -> StanBuilderM md gq SME.StanExpr
getDeclBinding k = do
  SME.VarBindingStore _ dbm <- indexBindings <$> get
  case Map.lookup k dbm of
    Nothing -> stanBuildError $ "declaration key (\"" <> k <> "\") not in binding store."
    Just e -> return e

addDeclBinding' :: IndexKey -> SME.StanExpr -> StanBuilderM md gq ()
addDeclBinding' k e = modify $ modifyIndexBindings f where
  f (SME.VarBindingStore ubm dbm) = SME.VarBindingStore ubm (Map.insert k e dbm)

addDeclBinding :: IndexKey -> SME.StanVar -> StanBuilderM md gq ()
addDeclBinding k sv = addDeclBinding' k (SME.var sv)

addUseBinding' :: IndexKey -> SME.StanExpr -> StanBuilderM md gq ()
addUseBinding' k e = modify $ modifyIndexBindings f where
  f (SME.VarBindingStore ubm dbm) = SME.VarBindingStore (Map.insert k e ubm) dbm

addUseBinding :: IndexKey -> SME.StanVar -> StanBuilderM md gq ()
addUseBinding k sv = addUseBinding' k (SME.var sv)

addUseBindingToDataSet' :: forall r md gq.RowTypeTag r -> IndexKey -> SME.StanExpr -> StanBuilderM md gq ()
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
      f :: forall x. RowInfos x -> Either Text (RowInfos x)
      f rowInfos = do
        (RowInfo tf ebs gis gimbs js) <- maybeToRight dataNotFoundErr $ DHash.lookup rtt rowInfos
        maybe (Right ()) (bindingChanged e) $ Map.lookup key ebs
        let ebs' = Map.insert key e ebs
        return $ DHash.insert rtt (RowInfo tf ebs' gis gimbs js) rowInfos
  bs <- get
  bs' <- stanBuildEither
         $ case inputDataType rtt of
             ModelData -> modifyModelRowInfosA f bs
             GQData -> modifyGQRowInfosA f bs
  put bs'

addUseBindingToDataSet :: RowTypeTag r -> IndexKey -> SME.StanVar -> StanBuilderM md gq ()
addUseBindingToDataSet rtt key sv = addUseBindingToDataSet' rtt key (SME.var sv)

indexBindingScope :: StanBuilderM md gq a -> StanBuilderM md gq a
indexBindingScope x = do
  curIB <- indexBindings <$> get
  a <- x
  modify (modifyIndexBindings $ const curIB)
  return a

isDeclared :: SME.StanName -> StanBuilderM md gq Bool
isDeclared sn  = do
  sd <- declaredVars <$> get
  case varLookup sd sn of
    Left _ -> return False
    Right _ -> return True

isDeclaredAllScopes :: SME.StanName -> StanBuilderM md gq Bool
isDeclaredAllScopes sn  = do
  sd <- declaredVars <$> get
  case varLookupAllScopes sd sn of
    Left _ -> return False
    Right _ -> return True


-- return True if variable is new, False if already declared
declare :: SME.StanName -> SME.StanType -> StanBuilderM md gq Bool
declare sn st = do
  let sv = SME.StanVar sn st
  sd <- declaredVars <$> get
  case varLookup sd sn of
    Left _ -> addVarInScope sn st >> return True
    Right (SME.StanVar _ st') -> if st' == st
                                 then return False
                                 else stanBuildError $ "Attempt to re-declare \"" <> sn <> "\" with different type. Previous=" <> show st' <> "; new=" <> show st

stanDeclare' :: SME.StanName -> SME.StanType -> Text -> Maybe StanExpr -> StanBuilderM md gq SME.StanVar
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

stanDeclare :: SME.StanName -> SME.StanType -> Text -> StanBuilderM md gq SME.StanVar
stanDeclare sn st sc = stanDeclare' sn st sc Nothing

stanDeclareRHS :: SME.StanName -> SME.StanType -> Text -> SME.StanExpr -> StanBuilderM md gq SME.StanVar
stanDeclareRHS sn st sc rhs = stanDeclare' sn st sc (Just rhs)

checkName :: SME.StanName -> StanBuilderM md gq ()
checkName sn = do
  dvs <- declaredVars <$> get
  _ <- stanBuildEither $ varLookup dvs sn
  return ()

typeForName :: SME.StanName -> StanBuilderM md gq SME.StanType
typeForName sn = do
  dvs <- declaredVars <$> get
  stanBuildEither $ (\(SME.StanVar _ st) -> st) <$> varLookup dvs sn

addJson :: forall r md gq. (Typeable md, Typeable gq)
        => RowTypeTag r
        -> SME.StanName
        -> SME.StanType
        -> Text
        -> Stan.StanJSONF r Aeson.Series
        -> StanBuilderM md gq SME.StanVar
addJson rtt name st sc fld = do
  v <- inBlock SBData $ stanDeclare name st sc
  let addFold :: Typeable x => RowInfos x -> StanBuilderM md gq (RowInfos x)
      addFold rowInfos = case addFoldToDBuilder rtt fld rowInfos of
        Nothing -> stanBuildError $ "Attempt to add Json to an uninitialized dataset (" <> dataSetName rtt <> ")"
        Just x -> return x
  bs <- get
  newBS <- case inputDataType rtt of
    ModelData -> modifyModelRowInfosA addFold bs
    GQData -> modifyGQRowInfosA addFold bs
  put newBS
  return v

{-
  (BuilderState declared ib rowBuilders modelExprs code ims) <- get
--  when (Set.member name un) $ stanBuildError $ "Duplicate name in json builders: \"" <> name <> "\""
  newRowBuilders <-
  put $ BuilderState declared ib newRowBuilders modelExprs code ims
  return $ SME.StanVar name st
-}

-- things like lengths may often be re-added
-- maybe work on a cleaner way...
addJsonOnce :: (Typeable md, Typeable gq)
                 => RowTypeTag r
                 -> SME.StanName
                 -> SME.StanType
                 -> Text
                 -> Stan.StanJSONF r Aeson.Series
                 -> StanBuilderM md gq SME.StanVar
addJsonOnce rtt name st sc fld = do
  alreadyDeclared <- isDeclaredAllScopes name
  if not alreadyDeclared
    then addJson rtt name st sc fld
    else return $ SME.StanVar name st

addFixedIntJson :: (Typeable md, Typeable gq) => Text -> Maybe Int -> Int -> StanBuilderM md gq SME.StanVar
addFixedIntJson name mLower n = do
  let sc = maybe "" (\l -> "<lower=" <> show l <> ">") $ mLower
  inBlock SBData $ stanDeclare name SME.StanInt sc -- this will error if we already declared
  modify $ addConstJson (JSONSeriesFold $ Stan.constDataF name n)
  return $ SME.StanVar name SME.StanInt

addFixedIntJson' :: (Typeable md, Typeable gq) => Text -> Maybe Int -> Int -> StanBuilderM md gq SME.StanVar
addFixedIntJson' name mLower n = do
  alreadyDeclared <- isDeclaredAllScopes name
  if not alreadyDeclared
    then addFixedIntJson name mLower n
    else return $ SME.StanVar name SME.StanInt

-- These get re-added each time something adds a column built from the data-set.
-- But we only need it once per data set.
addLengthJson :: (Typeable md, Typeable gq) => RowTypeTag r -> Text -> SME.IndexKey -> StanBuilderM md gq SME.StanVar
addLengthJson rtt name iKey = do
--  addDeclBinding' iKey (SME.name name)
  addDeclBinding iKey $ SME.StanVar name SME.StanInt
  addJsonOnce rtt name SME.StanInt "<lower=1>" (Stan.namedF name Foldl.length)

nameSuffixMsg :: SME.StanName -> Text -> Text
nameSuffixMsg n dsName = "name=\"" <> show n <> "\" data-set=\"" <> show dsName <> "\""

addColumnJson :: (Typeable md, Typeable gq, Aeson.ToJSON x)
              => RowTypeTag r
              -> Text
              -> SME.StanType
              -> Text
              -> (r -> x)
              -> StanBuilderM md gq SME.StanVar
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

addColumnJsonOnce :: (Typeable md, Typeable gq, Aeson.ToJSON x)
                  => RowTypeTag r
                  -> Text
                  -> SME.StanType
                  -> Text
                  -> (r -> x)
                  -> StanBuilderM md gq SME.StanVar
addColumnJsonOnce rtt name st sc toX = do
  alreadyDeclared <- isDeclared name
  if not alreadyDeclared
    then addColumnJson rtt name st sc toX
    else return $ SME.StanVar name st

addColumnMJson :: (Typeable md, Typeable gq, Aeson.ToJSON x)
               => RowTypeTag r
               -> Text
               -> SME.StanType
               -> Text
               -> (r -> Either Text x)
               -> StanBuilderM md gq SME.StanVar
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

addColumnMJsonOnce :: (Typeable md, Typeable gq, Aeson.ToJSON x)
               => RowTypeTag r
               -> Text
               -> SME.StanType
               -> Text
               -> (r -> Either Text x)
               -> StanBuilderM md gq SME.StanVar
addColumnMJsonOnce rtt name st sc toMX = do
  alreadyDeclared <- isDeclared name
  if not alreadyDeclared
    then addColumnMJson rtt name st sc toMX
    else return $ SME.StanVar name st

-- NB: name has to be unique so it can also be the suffix of the num columns.  Presumably the name carries the data-set suffix if nec.
data MatrixRowFromData r = MatrixRowFromData { rowName :: Text, rowLength :: Int, rowVec :: r -> VU.Vector Double }

add2dMatrixJson :: (Typeable md, Typeable gq)
                => RowTypeTag r
                -> MatrixRowFromData r
                -> SME.DataConstraint
                -> SME.StanDim
                -> StanBuilderM md gq SME.StanVar
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

modifyCode' :: (StanCode -> StanCode) -> BuilderState md gq -> BuilderState md gq
modifyCode' f bs = let newCode = f $ code bs in bs { code = newCode }

modifyCode :: (StanCode -> StanCode) -> StanBuilderM md gq ()
modifyCode f = modify $ modifyCode' f

setBlock' :: StanBlock -> StanCode -> StanCode
setBlock' b (StanCode _ blocks) = StanCode b blocks

setBlock :: StanBlock -> StanBuilderM md gq ()
setBlock = modifyCode . setBlock'

getBlock :: StanBuilderM md gq StanBlock
getBlock = do
  StanCode b _ <- gets code
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

addLine :: Text -> StanBuilderM md gq ()
addLine = modifyCode . addLine'

addInLine :: Text -> StanBuilderM md gq ()
addInLine = modifyCode . addInLine'

addStanLine :: Text -> StanBuilderM md gq ()
addStanLine t = addLine $ t <> ";\n"

declareStanFunction :: Text -> StanBuilderM md gq a -> StanBuilderM md gq a
declareStanFunction declaration body = inBlock SBFunctions $ do
  addLine declaration
  bracketed 2 body

indent :: Int -> StanCode -> StanCode
indent n (StanCode b blocks) =
  let (WithIndent curCode curIndent) = blocks Array.! b
  in StanCode b (blocks Array.// [(b, WithIndent curCode (curIndent + n))])

-- code inserted here will be indented n extra spaces
indented :: Int -> StanBuilderM md gq a -> StanBuilderM md gq a
indented n m = do
  modifyCode (indent n)
  x <- m
  modifyCode (indent $ Prelude.negate n)
  return x

bracketed :: Int -> StanBuilderM md gq a -> StanBuilderM md gq a
bracketed n m = do
  addInLine " {\n"
  x <- scoped $ indented n m
  addLine "}\n"
  return x

stanForLoop :: Text -- counter
            -> Maybe Text -- start, if not 1
            -> Text -- end, if
            -> (Text -> StanBuilderM md gq a)
            -> StanBuilderM md gq a
stanForLoop counter mStart end loopF = do
  let start = fromMaybe "1" mStart
  addLine $ "for (" <> counter <> " in " <> start <> ":" <> end <> ")"
  bracketed 2 $ loopF counter

stanForLoopB :: Text
             -> Maybe SME.StanExpr
             -> IndexKey
             -> StanBuilderM md gq a
             -> StanBuilderM md gq a
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

loopOverNamedDims :: [SME.StanDim] -> StanBuilderM md gq a -> StanBuilderM md gq a
loopOverNamedDims dims = foldr (\f g -> g . f) id $ fmap loopFromDim dims where
  loopFromDim :: SME.StanDim -> StanBuilderM env d a -> StanBuilderM env d a
  loopFromDim (SME.NamedDim ik) = stanForLoopB ("n_" <> ik) Nothing ik
  loopFromDim _ = const $ stanBuildError $ "groupBetaPrior:  given beta must have all named dimensions"

data StanPrintable = StanLiteral Text | StanExpression Text

stanPrint :: [StanPrintable] -> StanBuilderM md gq ()
stanPrint ps =
  let f x = case x of
        StanLiteral x -> "\"" <> x <> "\""
        StanExpression x -> x
  in addStanLine $ "print(" <> T.intercalate ", " (fmap f ps) <> ")"

underscoredIf :: Text -> Text
underscoredIf t = if T.null t then "" else "_" <> t

stanIndented :: StanBuilderM md gq a -> StanBuilderM md gq a
stanIndented = indented 2

varScopeBlock :: StanBlock -> StanBuilderM md gq ()
varScopeBlock sb = case sb of
  SBModel -> modify (modifyDeclaredVars $ changeVarScope ModelScope)
  SBGeneratedQuantities -> modify (modifyDeclaredVars $ changeVarScope GQScope)
  _ -> modify (modifyDeclaredVars $ changeVarScope GlobalScope)

inBlock :: StanBlock -> StanBuilderM md gq a -> StanBuilderM md gq a
inBlock b m = do
  oldBlock <- getBlock
  setBlock b
  varScopeBlock b
  x <- m
  setBlock oldBlock
  varScopeBlock oldBlock
  return x

printExprM :: Text -> SME.StanExpr -> StanBuilderM md gq Text
printExprM context e = do
  vbs <- indexBindings <$> get
  case SME.printExpr vbs e of
    Right t -> return t
    Left err -> stanBuildError $ context <> ": " <> err

addExprLine :: Text -> SME.StanExpr -> StanBuilderM md gq ()
addExprLine context  = printExprM context  >=> addStanLine

addExprLines :: Traversable t => Text -> t SME.StanExpr -> StanBuilderM md gq ()
addExprLines context = traverse_ (addExprLine context)

exprVarsM :: SME.StanExpr -> StanBuilderM md gq (Set StanVar)
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
           NoGQ -> ""
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

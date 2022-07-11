{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}

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
import qualified Stan.ModelBuilder.TypedExpressions.Program as TE
import qualified Stan.ModelBuilder.TypedExpressions.Statements as TE
import qualified Stan.ModelBuilder.TypedExpressions.Functions as TE
import qualified Stan.ModelBuilder.TypedExpressions.DAGTypes as DT
import qualified Data.Dependent.Map as DM
import Stan.ModelBuilder.TypedExpressions.Statements (StanName)
import Stan.ModelBuilder.TypedExpressions.Types (Nat(..))
import qualified Stan.ModelBuilder.TypedExpressions.Expressions as TE
import qualified Stan.ModelBuilder.TypedExpressions.Evaluate as TE
import qualified Stan.ModelBuilder.TypedExpressions.Format as TE
import qualified Stan.ModelBuilder.TypedExpressions.Types as TE

import qualified Prettyprinter.Render.Text as PP

import Stan.ModelBuilder.Expressions
import Stan.ModelBuilder.Distributions
import Stan.ModelBuilder.BuilderTypes

import Prelude hiding (All)
import Control.Monad.Writer.Strict as W
import qualified Control.Foldl as Foldl
import qualified Control.Exception as X
import qualified GHC.IO.Exception as X
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
import Stan.ModelConfig (InputDataType(..))
import qualified Stan.ModelConfig as SC
import qualified Data.Massiv.Array.Unsafe as X
import qualified Data.Vinyl as TE
import Data.Vector.Internal.Check (Checks(Bounds))

{-
stanCodeToStanModel :: StanCode -> StanModel
stanCodeToStanModel (StanCode _ a) =
  StanModel
  (f . g $ a Array.! SBFunctions)
  (g $ a Array.! SBData)
  (g $ a Array.! SBDataGQ)
  (f . g $ a Array.! SBTransformedData)
  (f . g $ a Array.! SBTransformedDataGQ)
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
-}

data StanCode = StanCode { curBlock :: StanBlock
                         , program :: TE.StanProgram
                         }

addStmtToCode :: TE.UStmt -> StanBuilderM md gq ()
addStmtToCode stmt = do
  cb <- getBlock
  f <- stanBuildEither $ TE.addStmtToBlock cb stmt
  modifyCode f

addStmtsToCode :: Traversable f => f TE.UStmt -> StanBuilderM md gq ()
addStmtsToCode stmts = do
  cb <- getBlock
  f <- stanBuildEither $ TE.addStmtsToBlock cb stmts
  modifyCode f

addStmtToCodeTop :: TE.UStmt -> StanBuilderM md gq ()
addStmtToCodeTop stmt = do
  cb <- getBlock
  f <- stanBuildEither $ TE.addStmtToBlockTop cb stmt
  modifyCode f

addStmtsToCodeTop :: Traversable f => f TE.UStmt -> StanBuilderM md gq ()
addStmtsToCodeTop stmts = do
  cb <- getBlock
  f <- stanBuildEither $ TE.addStmtsToBlockTop cb stmts
  modifyCode f

addFromCodeWriter :: TE.CodeWriter a -> StanBuilderM md gq a
addFromCodeWriter (TE.CodeWriter cw) = addStmtsToCode stmts >> return a
  where (a, stmts) = W.runWriter cw

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
               <> show (inputDataType rtt) <> ") not found in data-set group int maps: "
               <> displayDataSetGroupIntMaps groupIndexes <> "."
               <> " If this error is complaining about a data-set key that appears to be present, double check the *types* used when constructing the row-type-tags"

    Just (GroupIntMaps gim) -> case DHash.lookup gtt gim of
      Nothing -> Left $ "\"" <> taggedGroupName gtt <> "\" not found in Group int maps for data-set \"" <> dataSetName rtt <> "\""
      Just im -> Right im

groupIndexVarName :: RowTypeTag r -> GroupTypeTag k -> StanName
groupIndexVarName rtt gtt = dataSetName rtt <> "_" <> taggedGroupName gtt
{-# INLINEABLE groupIndexVarName #-}

getGroupIndexVar :: forall md gq r k.
                    RowTypeTag r
                 -> GroupTypeTag k
                 -> StanBuilderM md gq (TE.UExpr TE.EIndexArray)
getGroupIndexVar rtt gtt = do
  let varName = groupIndexVarName rtt gtt
      dsNotFoundErr = stanBuildError
                      $ "getGroupIndexVar: data-set=" <> dataSetName rtt <> " (input type=" <> show (inputDataType rtt) <> ") not found."
      varIfGroup :: forall x d.RowInfo d x -> StanBuilderM md gq (TE.UExpr TE.EIndexArray)
      varIfGroup ri =
        let (GroupIndexes gis) = groupIndexes ri
        in case DHash.lookup gtt gis of
          Just _ -> return $ TE.namedE varName TE.sIndexArray --SME.StanVar varName (SME.StanArray [SME.NamedDim $ dataSetName rtt] SME.StanInt)
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

useBindingsFromGroupIndexMakers :: RowTypeTag r -> GroupIndexMakers r -> TE.IndexArrayMap
useBindingsFromGroupIndexMakers rtt (GroupIndexMakers gims) = Map.fromList l where
  l = fmap g $ DHash.toList gims
  g (gtt DSum.:=> _) =
    let gn = taggedGroupName gtt
        dsn = dataSetName rtt
        indexName = dsn <> "_" <> gn
        indexExpr = TE.namedLIndex indexName
    in (gn, indexExpr)

-- build a new RowInfo from the row index and IntMap builders
buildRowInfo :: d -> RowTypeTag r -> GroupIndexAndIntMapMakers d r -> RowInfo d r
buildRowInfo d rtt (GroupIndexAndIntMapMakers tf@(ToFoldable f) ims imbs) = Foldl.fold fld $ f d  where
  gisFld = indexBuildersForDataSetFold $ ims
  useBindings = Map.insert (dataSetName rtt) (TE.namedLIndex ("N_" <> dataSetName rtt))
                $ useBindingsFromGroupIndexMakers rtt ims
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
--            nds = TE.NamedDeclSpec indexName $  TE.intArraySpec (TE.namedSizeE dsName) [TE.lowerM $ TE.intE 1]
            ndsF lE = TE.NamedDeclSpec indexName $  TE.intArraySpec lE [TE.lowerM $ TE.intE 1]
        addFixedIntJson' (inputDataType rtt) ("J_" <> gName) (Just 1) gSize
        _ <- addColumnMJson rtt ndsF mIntF
--        _ <- addColumnMJson rtt indexName (SME.StanArray [SME.NamedDim dsName] SME.StanInt) "<lower=1>" mIntF
        addDeclBinding gName $ "J_" <> gName
        return Nothing
      buildRowFolds :: (Typeable md, Typeable gq) => RowTypeTag r -> RowInfo d r -> StanBuilderM md gq (Maybe r)
      buildRowFolds rtt (RowInfo _ _ (GroupIndexes gis) _ _) = do
        _ <- DHash.traverseWithKey (buildIndexJSONFold rtt) gis
        return Nothing
  _ <- gets modelRowBuilders >>= DHash.traverseWithKey buildRowFolds
  _ <- gets gqRowBuilders >>= DHash.traverseWithKey buildRowFolds
  pure ()

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
  (JSONSeriesFold constJSONFld) <- constModelJSON <$> get
  dataSetJSON <- buildModelJSONF
  return $ \d ->
    let c = Foldl.foldM constJSONFld (Just ())
        ds =  buildJSONFromRows dataSetJSON d
    in (<>) <$> c <*> ds

buildGQJSONFromDataM :: Typeable gq => StanBuilderM md gq (gq -> Either Text Aeson.Series)
buildGQJSONFromDataM = do
  (JSONSeriesFold constJSONFld) <- constGQJSON <$> get
  dataSetJSON <- buildGQJSONF
  return $ \d ->
    let c = Foldl.foldM constJSONFld (Just ())
        ds =  buildJSONFromRows dataSetJSON d
    in (<>) <$> c <*> ds

data VariableScope = GlobalScope | ModelScope | GQScope deriving (Show, Eq, Ord)

newtype DeclarationMap = DeclarationMap (Map TE.StanName TE.EType) deriving (Show)
data ScopedDeclarations = ScopedDeclarations { currentScope :: VariableScope
                                             , globalScope :: NonEmpty DeclarationMap
                                             , modelScope :: NonEmpty DeclarationMap
                                             , gqScope :: NonEmpty DeclarationMap
                                             } deriving (Show)

changeVarScope :: VariableScope -> ScopedDeclarations -> ScopedDeclarations
changeVarScope vs sd = sd { currentScope = vs}

initialScopedDeclarations :: ScopedDeclarations
initialScopedDeclarations = let x = one (DeclarationMap Map.empty)
                            in ScopedDeclarations GlobalScope x x x

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

addVarInScope :: TE.StanName -> TE.StanType t -> StanBuilderM md gq (TE.UExpr t)
addVarInScope sn st = do
  let newSD sd = do
        _ <- alreadyDeclared sd sn st
        let curScope = currentScope sd
            dmNE = declarationsNE curScope sd
            DeclarationMap m = head dmNE
            dm' = DeclarationMap $ Map.insert sn (TE.eTypeFromStanType st) m
            dmNE' = dm' :| tail dmNE
        return $ setDeclarationsNE dmNE' curScope sd
  bs <- get
  case modifyDeclaredVarsA newSD bs of
    Left errMsg -> stanBuildError errMsg
    Right newBS -> do
      put newBS
      pure $ TE.namedE sn (TE.sTypeFromStanType st)

varLookupInScope :: ScopedDeclarations -> VariableScope -> TE.StanName -> Either Text TE.EType
varLookupInScope sd sc sn = go $ toList dNE where
  dNE = declarationsNE sc sd
  go [] = Left $ "\"" <> sn <> "\" not declared/in scope (stan scope=" <> show sc <> ")."
  go ((DeclarationMap x) : xs) = case Map.lookup sn x of
    Nothing -> go xs
    Just et -> pure et

varLookup :: ScopedDeclarations -> TE.StanName -> Either Text TE.EType
varLookup sd = varLookupInScope sd (currentScope sd)

varLookupAllScopes :: ScopedDeclarations -> TE.StanName -> Either Text TE.EType
varLookupAllScopes sd sn =
  case varLookupInScope sd GlobalScope sn of
    Right x -> Right x
    Left _ -> case varLookupInScope sd ModelScope sn of
      Right x -> Right x
      Left _ -> varLookupInScope sd GQScope sn


alreadyDeclared :: ScopedDeclarations -> TE.StanName -> TE.StanType t  -> Either Text ()
alreadyDeclared sd sn st =
  case varLookup sd sn of
    Right et ->  if et == TE.eTypeFromStanType st
                 then Left $ sn <> " already declared (with same type= " <> show et <> ")!"
                 else Left $ sn <> " (" <> show (TE.eTypeFromStanType st)
                      <> ")already declared (with different type="<> show et <> ")!"
    Left _ -> pure ()

alreadyDeclaredAllScopes :: ScopedDeclarations -> TE.StanName -> TE.StanType t -> Either Text ()
alreadyDeclaredAllScopes sd sn st =
  case varLookupAllScopes sd sn of
    Right et ->  if et == TE.eTypeFromStanType st
                 then Left $ sn <> " already declared (with same type= " <> show et <> ")!"
                 else Left $ sn <> " (" <> show (TE.eTypeFromStanType st)
                      <> ")already declared (with different type="<> show et <> ")!"
    Left _ -> pure ()
{-
addVarInScope :: TE.StanName -> SME.StanType -> StanBuilderM env d SME.StanVar
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
{-
getVar :: StanName -> StanBuilderM md gq StanVar
getVar sn = do
  sd <- declaredVars <$> get
  case varLookup sd sn of
    Right sv -> return sv
    Left errMsg -> stanBuildError errMsg
-}

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
  pure a

data BuilderState md gq = BuilderState { declaredVars :: !ScopedDeclarations
                                       , indexBindings :: !TE.IndexLookupCtxt
                                       , modelRowBuilders :: !(RowInfos md)
                                       , gqRowBuilders :: !(RowInfos gq)
                                       , constModelJSON :: JSONSeriesFold ()  -- json for things which are attached to no data set.
                                       , constGQJSON :: JSONSeriesFold ()
                                       , hasFunctions :: !(Set.Set Text)
                                       , parameterCollection :: DT.BParameterCollection
                                       , parameterCode :: TE.StanProgram
                                       , code :: !StanCode
                                       }

dumpBuilderState :: BuilderState md gq -> Text
dumpBuilderState bs = -- (BuilderState dvs ibs ris js hf c) =
  "Declared Vars: " <> show (declaredVars bs)
  <> "\n index-bindings: " <> TE.printLookupCtxt (indexBindings bs)
  <> "\n model row-info-keys: " <> show (DHash.keys $ modelRowBuilders bs)
  <> "\n gq row-info-keys: " <> show (DHash.keys $ gqRowBuilders bs)
  <> "\n functions: " <> show (hasFunctions bs)
  <> "\n parameterCollection (keys)" <> show (DM.keys $ DT.pdm $ parameterCollection bs)


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

getDataSetBindings :: RowTypeTag r -> StanBuilderM md gq TE.IndexArrayMap
getDataSetBindings rtt = withRowInfo err (return .  expressionBindings) rtt where
  idt = inputDataType rtt
  err = stanBuildError $ "getDataSetbindings: row-info=" <> dataSetName rtt <> " not found in " <> show idt

getModelDataFoldableAsListF :: RowTypeTag r -> StanBuilderM md gq (md -> [r])
getModelDataFoldableAsListF rtt = do
  let idt = inputDataType rtt
      errNF = stanBuildError $ "getModelDataFoldableAsList: row-info=" <> dataSetName rtt <> " not found in " <> show idt
      errGQ = stanBuildError $ "getModelDataFoldableAsList: row-info=" <> dataSetName rtt <> " is GQ data"
  case idt of
    ModelData -> do
      rowInfos <- modelRowBuilders <$> get
      case DHash.lookup rtt rowInfos of
        Nothing -> errNF
        Just ri -> return (applyToFoldable Foldl.list $ toFoldable ri)
    GQData -> errGQ

getGQDataFoldableAsListF :: RowTypeTag r -> StanBuilderM md gq (gq -> [r])
getGQDataFoldableAsListF rtt = do
  let idt = inputDataType rtt
      errNF = stanBuildError $ "getGQDataFoldableAsList: row-info=" <> dataSetName rtt <> " not found in " <> show idt
      errModelData = stanBuildError $ "getGQDataFoldableAsList: row-info=" <> dataSetName rtt <> " is Model data"
  case idt of
    GQData -> do
      rowInfos <- gqRowBuilders <$> get
      case DHash.lookup rtt rowInfos of
        Nothing -> errNF
        Just ri -> return (applyToFoldable Foldl.list $ toFoldable ri)
    ModelData -> errModelData


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
  modify $ modifyIndexBindings (\lc -> lc { TE.indexes = newUseBindings })

useDataSetForBindings :: RowTypeTag r -> StanBuilderM md gq a -> StanBuilderM md gq a
useDataSetForBindings rtt x = getDataSetBindings rtt >>= flip withUseBindings x

-- add anything not already present
addDataSetBindings :: RowTypeTag r -> StanBuilderM md gq a -> StanBuilderM md gq a
addDataSetBindings rtt x = getDataSetBindings rtt >>= flip extendUseBindings x

modifyDeclaredVars :: (ScopedDeclarations -> ScopedDeclarations) -> BuilderState md gq -> BuilderState md gq
modifyDeclaredVars f bs = bs {declaredVars = f (declaredVars bs)}
--(BuilderState dv vbs mrb gqrb cj hf c) = BuilderState (f dv) vbs mrb gqrb cj hf c

modifyDeclaredVarsA :: Applicative t
                    => (ScopedDeclarations -> t ScopedDeclarations)
                    -> BuilderState md gq
                    -> t (BuilderState md gq)
modifyDeclaredVarsA f bs = (\x -> bs { declaredVars = x}) <$> f (declaredVars bs)
-- (BuilderState dv vbs mrb gqrb cj hf c) = (\x -> BuilderState x vbs mrb gqrb cj hf c) <$> f dv

modifyIndexBindings :: (TE.IndexLookupCtxt -> TE.IndexLookupCtxt)
                    -> BuilderState md gq
                    -> BuilderState md gq
modifyIndexBindings f bs = bs {indexBindings = f (indexBindings bs)}
--(BuilderState dv vbs mrb gqrb cj hf c) = BuilderState dv (f vbs) mrb gqrb cj hf c

modifyIndexBindingsA :: Applicative t
                     => (TE.IndexLookupCtxt -> t TE.IndexLookupCtxt)
                     -> BuilderState md gq
                     -> t (BuilderState md gq)
modifyIndexBindingsA f bs = (\x -> bs {indexBindings = x}) <$> f (indexBindings bs)
--(BuilderState dv vbs mrb gqrb cj hf c) = (\x -> BuilderState dv x mrb gqrb cj hf c) <$> f vbs

withUseBindings :: TE.IndexArrayMap -> StanBuilderM md gq a -> StanBuilderM md gq a
withUseBindings ubs m = do
  oldBindings <- indexBindings <$> get
  modify $ modifyIndexBindings (\lc -> lc {TE.indexes = ubs})
  a <- m
  modify $ modifyIndexBindings $ const oldBindings
  return a

extendUseBindings :: TE.IndexArrayMap -> StanBuilderM md gq a -> StanBuilderM md gq a
extendUseBindings ubs' m = do
  oldBindings <- indexBindings <$> get
  modify $ modifyIndexBindings (\lc -> lc {TE.indexes = Map.union ubs' (TE.indexes lc)})
  a <- m
  modify $ modifyIndexBindings $ const oldBindings
  return a

withDeclBindings :: TE.IndexSizeMap -> StanBuilderM md gq a -> StanBuilderM md gq a
withDeclBindings dbs m = do
  oldBindings <- indexBindings <$> get
  modify $ modifyIndexBindings (\lc -> lc {TE.sizes = dbs})
  a <- m
  modify $ modifyIndexBindings $ const oldBindings
  return a

extendDeclBindings :: TE.IndexSizeMap -> StanBuilderM md gq a -> StanBuilderM md gq a
extendDeclBindings dbs' m = do
  oldBindings <- indexBindings <$> get
  modify $ modifyIndexBindings (\lc -> lc {TE.sizes = Map.union dbs' (TE.sizes lc)})
  a <- m
  modify $ modifyIndexBindings $ const oldBindings
  return a

addScopedDeclBindings :: TE.IndexSizeMap -> StanBuilderM env d a -> StanBuilderM env d a
addScopedDeclBindings dbs' m = do
  oldBindings <- indexBindings <$> get
  modify $ modifyIndexBindings (\lc -> lc {TE.sizes = Map.union dbs' (TE.sizes lc)})
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


modifyConstJson :: SC.InputDataType -> (JSONSeriesFold () -> JSONSeriesFold ()) -> BuilderState md gq -> BuilderState md gq
modifyConstJson idt f bs = case idt of
  SC.ModelData -> bs { constModelJSON = f (constModelJSON bs)}
  SC.GQData -> bs { constGQJSON = f (constGQJSON bs)}
--(BuilderState dvs ibs mrbs gqrbs cj hfs c) = BuilderState dvs ibs mrbs gqrbs (f cj) hfs c

addConstJson :: SC.InputDataType -> JSONSeriesFold () -> BuilderState md gq -> BuilderState md gq
addConstJson idt jf = modifyConstJson idt (<> jf)

modifyFunctionNames :: (Set Text -> Set Text) -> BuilderState md gq -> BuilderState md gq
modifyFunctionNames f bs = bs { hasFunctions = f (hasFunctions bs)}
--(BuilderState dv vbs mrb gqrb cj hf c) = BuilderState dv vbs mrb gqrb cj (f hf) c

initialBuilderState :: Typeable md => RowInfos md -> RowInfos gq -> BuilderState md gq
initialBuilderState modelRowInfos gqRowInfos =
  BuilderState
  initialScopedDeclarations
  TE.emptyLookupCtxt
  modelRowInfos
  gqRowInfos
  mempty
  mempty
  Set.empty
  (DT.BParameterCollection mempty mempty)
  TE.emptyStanProgram
  (StanCode SBData TE.emptyStanProgram)

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
             inBlock SBFunctions fCode
             modify $ modifyFunctionNames $ Set.insert functionsName
         )

addFunctionOnce :: (Traversable g, TE.GenSType rt)
                => TE.Function rt ats -> TE.TypedArgNames ats -> (TE.ExprList ats -> (g TE.UStmt, TE.UExpr rt)) -> StanBuilderM md gq (TE.Function rt ats)
addFunctionOnce f@(TE.Function fn _ _ _) argNames fBF = do
  fsNames <- gets hasFunctions
  when (not $  fn `Set.member` fsNames)
    $ inBlock SBFunctions
    $ addStmtToCode $ TE.function f argNames fBF
  return f

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

dataSetCrosswalkName :: RowTypeTag rFrom -> RowTypeTag rTo -> TE.StanName
dataSetCrosswalkName rttFrom rttTo = "XWalk_" <> dataSetName rttFrom <> "_" <> dataSetName rttTo
{-# INLINEABLE dataSetCrosswalkName #-}

crosswalkIndexKey :: RowTypeTag rTo -> TE.StanName
crosswalkIndexKey rttTo = "XWalkTo_" <> dataSetName rttTo
{-# INLINEABLE crosswalkIndexKey #-}

--dataSetSizeName :: RowTypeTag r -> Text
--dataSetSizeName rtt = "N_" <> dataSetName rtt

--groupSizeName :: GroupTypeTag k -> Text
--groupSizeName gtt = "J_" <> taggedGroupName gtt


dataSetSizeE :: RowTypeTag r -> TE.UExpr TE.EInt
dataSetSizeE rtt = TE.namedE (dataSetSizeName rtt) TE.SInt

groupSizeE :: GroupTypeTag k -> TE.UExpr TE.EInt
groupSizeE gtt = TE.namedE (groupSizeName gtt) TE.SInt

byGroupIndexE :: RowTypeTag r -> GroupTypeTag k -> TE.UExpr TE.EIndexArray
byGroupIndexE rtt gtt = TE.namedE (dataByGroupIndexName rtt gtt) TE.sIndexArray

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
--      xWalkType = SME.StanArray [SME.NamedDim $ dataSetName rttFrom] SME.StanInt
--      xWalkVar = SME.StanVar xWalkName xWalkType
      xWalkF = toGroupToIndexE . fromToGroup
--      xWalkNDS = TE.NamedDeclSpec xWalkName $ TE.intArraySpec (TE.namedSizeE $ dataSetName rttFrom) [TE.lowerM $ TE.intE 1]
      xWalkNDSF lE = TE.NamedDeclSpec xWalkName $ TE.intArraySpec lE [TE.lowerM $ TE.intE 1]
  addColumnMJson rttFrom xWalkNDSF xWalkF
--  addColumnMJson rttFrom xWalkName xWalkType "<lower=1>" xWalkF
--  addUseBindingToDataSet rttFrom xWalkIndexKey $ SME.indexBy (SME.name xWalkName) $ dataSetName rttFrom
  addUseBindingToDataSet rttFrom xWalkIndexKey xWalkName
  addDeclBinding xWalkIndexKey $ dataSetSizeName rttFrom
  pure ()

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
      (resE, bs) = usingState builderState . runExceptT $ unStanBuilderM sb
  in fmap (bs,) resE

stanBuildError :: Text -> StanBuilderM md gq a
stanBuildError t = do
  builderText <- dumpBuilderState <$> get
  StanBuilderM $ ExceptT (pure $ Left $ t <> "\nBuilder:\n" <> builderText)

stanBuildMaybe :: Text -> Maybe a -> StanBuilderM md gq a
stanBuildMaybe msg = maybe (stanBuildError msg) return

stanBuildEither :: Either Text a -> StanBuilderM md gq a
stanBuildEither = either stanBuildError return

getDeclBinding :: IndexKey -> StanBuilderM md gq (TE.LExpr TE.EInt)
getDeclBinding k = do
  dbm <- gets (TE.sizes . indexBindings)
  case Map.lookup k dbm of
    Nothing -> stanBuildError $ "declaration key (\"" <> k <> "\") not in binding store."
    Just e -> return e

addDeclBinding' :: IndexKey -> TE.LExpr TE.EInt -> StanBuilderM md gq ()
addDeclBinding' k e = modify $ modifyIndexBindings f where
  f lc = lc { TE.sizes = Map.insert k e $ TE.sizes lc}

addDeclBinding :: IndexKey -> TE.StanName -> StanBuilderM md gq ()
addDeclBinding k = addDeclBinding' k . TE.namedLSize

addUseBinding' :: IndexKey -> TE.IndexArrayL -> StanBuilderM md gq ()
addUseBinding' k e = modify $ modifyIndexBindings f where
  f lc = lc { TE.indexes = Map.insert k e $ TE.indexes lc }

addUseBinding :: IndexKey -> TE.StanName -> StanBuilderM md gq ()
addUseBinding k = addUseBinding' k . TE.namedLIndex

getUseBinding :: IndexKey -> StanBuilderM md gq TE.IndexArrayL
getUseBinding k = do
  ubm  <- gets (TE.indexes . indexBindings)
  case Map.lookup k ubm of
    Just e -> return e
    Nothing -> stanBuildError $ "getUseBinding: k=" <> show k <> " not found in use-binding map"

addUseBindingToDataSet' :: forall r md gq.RowTypeTag r -> IndexKey -> TE.IndexArrayL -> StanBuilderM md gq ()
addUseBindingToDataSet' rtt key e = do
  let dataNotFoundErr = "addUseBindingToDataSet: Data-set \"" <> dataSetName rtt <> "\" not found in StanBuilder.  Maybe you haven't added it yet?"
      bindingChanged newExpr oldExpr = when (not $ TE.eqLExpr newExpr oldExpr)
                                       $ Left
                                       $ "addUseBindingToDataSet: key="
                                       <> show key
                                       <> "\nAttempt to add different use binding to already-bound key. Old expression="
                                       <> TE.exprToText oldExpr
                                       <> "; new expression="
                                       <> TE.exprToText newExpr
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

addUseBindingToDataSet :: RowTypeTag r -> IndexKey -> TE.StanName -> StanBuilderM md gq ()
addUseBindingToDataSet rtt key = addUseBindingToDataSet' rtt key . TE.namedLIndex

indexBindingScope :: StanBuilderM md gq a -> StanBuilderM md gq a
indexBindingScope x = do
  curIB <- indexBindings <$> get
  a <- x
  modify (modifyIndexBindings $ const curIB)
  return a

isDeclared :: TE.StanName -> StanBuilderM md gq Bool
isDeclared sn  = do
  sd <- declaredVars <$> get
  case varLookup sd sn of
    Left _ -> return False
    Right _ -> return True

isDeclaredAllScopes :: TE.StanName -> StanBuilderM md gq Bool
isDeclaredAllScopes sn  = do
  sd <- declaredVars <$> get
  case varLookupAllScopes sd sn of
    Left _ -> return False
    Right _ -> return True


-- return True if variable is new, False if already declared
declare :: TE.StanName -> TE.StanType t -> StanBuilderM md gq Bool
declare sn st = do
--  let sv = SME.StanVar sn st
  sd <- declaredVars <$> get
  case varLookup sd sn of
    Left _ -> addVarInScope sn st >> return True
    Right et -> if et == TE.eTypeFromStanType st
                then return False
                else stanBuildError $ "Attempt to re-declare \"" <> sn <> "\" with different type. Previous="
                     <> show et <> "; new=" <> show (TE.eTypeFromStanType st)

stanDeclare' :: TE.StanName -> TE.DeclSpec t -> Maybe (TE.UExpr t) -> StanBuilderM md gq (TE.UExpr t)
stanDeclare' sn ds mRHS = do
  isNew <- declare sn $ TE.declType ds
  if isNew
    then addStmtToCode $ case mRHS of
                           Nothing -> TE.declare sn ds
                           Just re -> TE.declareAndAssign sn ds re
    else case mRHS of
           Nothing -> pure ()
           Just _ -> stanBuildError $ "Attempt to re-declare variable with RHS (" <> sn <> ")"
  pure $ TE.namedE sn (TE.sTypeFromStanType $ TE.declType ds)

{-

--  let sv = SME.StanVar sn st
  let lhs = declareVar sv sc
  _ <- if isNew
    then case mRHS of
           Nothing -> addExprLine "stanDeclare'" lhs
           Just rhs -> addExprLine "stanDeclare'" ((SME.declaration lhs) `SME.eq` rhs)
    else case mRHS of
           Nothing -> return ()

  return sv
-}

stanDeclare :: TE.StanName -> TE.DeclSpec t -> StanBuilderM md gq (TE.UExpr t)
stanDeclare sn ds = stanDeclare' sn ds Nothing

stanDeclareN :: TE.NamedDeclSpec t -> StanBuilderM md gq (TE.UExpr t)
stanDeclareN (TE.NamedDeclSpec sn ds) = stanDeclare' sn ds Nothing

stanDeclareRHS :: TE.StanName -> TE.DeclSpec t -> TE.UExpr t -> StanBuilderM md gq (TE.UExpr t)
stanDeclareRHS sn st rhs = stanDeclare' sn st (Just rhs)

stanDeclareRHSN :: TE.NamedDeclSpec t -> TE.UExpr t -> StanBuilderM md gq (TE.UExpr t)
stanDeclareRHSN (TE.NamedDeclSpec sn ds) rhs = stanDeclare' sn ds (Just rhs)


checkName :: TE.StanName -> StanBuilderM md gq ()
checkName sn = do
  dvs <- declaredVars <$> get
  _ <- stanBuildEither $ varLookup dvs sn
  pure ()

typeForName :: TE.StanName -> StanBuilderM md gq TE.EType
typeForName sn = do
  dvs <- declaredVars <$> get
  stanBuildEither $ varLookup dvs sn

addJson :: forall t r md gq. (Typeable md, Typeable gq)
        => RowTypeTag r
        -> TE.NamedDeclSpec t
        -> Stan.StanJSONF r Aeson.Series
        -> StanBuilderM md gq (TE.UExpr t)
addJson rtt nds fld = do
  let codeBlock = if inputDataType rtt == ModelData then SBData else SBDataGQ
  ve <- inBlock codeBlock $ stanDeclareN nds
  let addFold :: Typeable x => RowInfos x -> StanBuilderM md gq (RowInfos x)
      addFold rowInfos = case addFoldToDBuilder rtt fld rowInfos of
        Nothing -> stanBuildError $ "Attempt to add Json to an uninitialized dataset (" <> dataSetName rtt <> ")"
        Just x -> return x
  bs <- get
  newBS <- case inputDataType rtt of
    ModelData -> modifyModelRowInfosA addFold bs
    GQData -> modifyGQRowInfosA addFold bs
  put newBS
  return ve

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
            -> TE.NamedDeclSpec t
            -> Stan.StanJSONF r Aeson.Series
            -> StanBuilderM md gq (TE.UExpr t)
addJsonOnce rtt nds fld = do
  alreadyDeclared <- isDeclaredAllScopes $ TE.declName nds
  if not alreadyDeclared
    then addJson rtt nds fld
    else pure $ TE.namedE (TE.declName nds) (TE.sTypeFromStanType $ TE.declType $ TE.decl nds)

addFixedIntJson :: (Typeable md, Typeable gq) => InputDataType -> Text -> Maybe Int -> Int -> StanBuilderM md gq (TE.UExpr TE.EInt)
addFixedIntJson idt name mLower n = do
  let ds = TE.intSpec $ maybe [] (pure. TE.lowerM . TE.intE) $ mLower
      codeBlock = if idt == ModelData then SBData else SBDataGQ
  inBlock codeBlock $ stanDeclare name ds  -- this will error if we already declared
  modify $ addConstJson idt (JSONSeriesFold $ Stan.constDataF name n)
  return $ TE.namedE name TE.SInt

addFixedIntJson' :: (Typeable md, Typeable gq)
                 => InputDataType
                 -> Text
                 -> Maybe Int
                 -> Int
                 -> StanBuilderM md gq (TE.UExpr TE.EInt)
addFixedIntJson' idt name mLower n = do
  alreadyDeclared <- isDeclaredAllScopes name
  if not alreadyDeclared
    then addFixedIntJson idt name mLower n
    else return $ TE.namedE name TE.SInt

-- These get re-added each time something adds a column built from the data-set.
-- But we only need it once per data set.
addLengthJson :: (Typeable md, Typeable gq)
              => RowTypeTag r
              -> Text
              -> SME.IndexKey
              -> StanBuilderM md gq (TE.UExpr TE.EInt)
addLengthJson rtt name iKey = do
  addDeclBinding iKey name
  addJsonOnce rtt (TE.NamedDeclSpec name (TE.intSpec [TE.lowerM $ TE.intE 1])) (Stan.namedF name Foldl.length)

nameSuffixMsg :: TE.StanName -> Text -> Text
nameSuffixMsg n dsName = "name=\"" <> show n <> "\" data-set=\"" <> show dsName <> "\""

addColumnJson :: (Typeable md, Typeable gq, Aeson.ToJSON x
                 , TE.TypeOneOf t [TE.EArray (S Z) TE.EInt, TE.ECVec, TE.EMat]
                 )
              => RowTypeTag r
              -> (TE.UExpr TE.EInt -> TE.NamedDeclSpec t)
              -> (r -> x)
              -> StanBuilderM md gq (TE.UExpr t)
addColumnJson rtt ndsF toX = do
  let dsName = dataSetName rtt
      sizeName = dataSetName rtt --"N" <> underscoredIf dsName
  lE <- addLengthJson rtt sizeName dsName -- ??
  let nds = ndsF lE
  addJson rtt nds (Stan.valueToPairF (TE.declName nds) $ Stan.jsonArrayF toX)


addColumnJsonOnce :: (Typeable md, Typeable gq, Aeson.ToJSON x
                     , TE.TypeOneOf t [TE.EArray (S Z) TE.EInt, TE.ECVec, TE.EMat]
                     )
                  => RowTypeTag r
                  -> (TE.UExpr TE.EInt -> TE.NamedDeclSpec t)
                  -> (r -> x)
                  -> StanBuilderM md gq (TE.UExpr t)
addColumnJsonOnce rtt ndsF toX = do
  let lE = TE.namedE (dataSetSizeName rtt) TE.SInt
      nds = ndsF lE
      name = TE.declName nds
      sType = TE.sTypeFromStanType $ TE.declType $ TE.decl nds
  alreadyDeclared <- isDeclared name
  if not alreadyDeclared
    then addColumnJson rtt ndsF toX
    else pure $ TE.namedE name sType


addColumnMJson :: (Typeable md, Typeable gq, Aeson.ToJSON x
                  , TE.TypeOneOf t [TE.EArray (S Z) TE.EInt, TE.ECVec, TE.EMat]
                  )
               => RowTypeTag r
               -> (TE.UExpr TE.EInt -> TE.NamedDeclSpec t)
               -> (r -> Either Text x)
               -> StanBuilderM md gq (TE.UExpr t)
addColumnMJson rtt ndsF toMX = do
  let dsName = dataSetName rtt
  let sizeName = "N_" <> dsName
  lE <- addLengthJson rtt sizeName dsName -- ??
  let nds = ndsF lE
--  let fullName = name <> "_" <> dsName
  addJson rtt nds (Stan.valueToPairF (TE.declName nds) $ Stan.jsonArrayEF toMX)


addColumnMJsonOnce :: (Typeable md, Typeable gq, Aeson.ToJSON x
                      , TE.TypeOneOf t [TE.EArray (S Z) TE.EInt, TE.ECVec, TE.EMat]
                      )
               => RowTypeTag r
               -> (TE.UExpr TE.EInt -> TE.NamedDeclSpec t)
               -> (r -> Either Text x)
               -> StanBuilderM md gq (TE.UExpr t)
addColumnMJsonOnce rtt ndsF toMX = do
  let lE = TE.namedE (dataSetSizeName rtt) TE.SInt
      nds = ndsF lE
      name = TE.declName nds
      sType = TE.sTypeFromStanType $ TE.declType $ TE.decl nds
  alreadyDeclared <- isDeclared name
  if not alreadyDeclared
    then addColumnMJson rtt ndsF toMX
    else pure $ TE.namedE name sType

-- NB: name has to be unique so it can also be the suffix of the num columns.  Presumably the name carries the data-set suffix if nec.
data MatrixRowFromData r = MatrixRowFromData { rowName :: Text, colIndexM :: Maybe SME.IndexKey, rowLength :: Int, rowVec :: r -> VU.Vector Double }

add2dMatrixJson :: (Typeable md, Typeable gq)
                => RowTypeTag r
                -> MatrixRowFromData r
                -> [TE.VarModifier TE.UExpr TE.EReal]
--                -> TE.UExpr TE.EInt -- row dimension
                -> StanBuilderM md gq (TE.UExpr TE.EMat)
add2dMatrixJson rtt (MatrixRowFromData name colIndexM cols vecF) cs = do
  let dsName = dataSetName rtt
      wdName = name <> underscoredIf dsName
      ndsF rowsE = TE.NamedDeclSpec wdName $ TE.matrixSpec rowsE (TE.namedSizeE colName) cs
      colIndex = fromMaybe name colIndexM
      colName = "K" <> "_" <> colIndex
      colDimName = colIndex <> "_Cols"
--      colDimVar = SME.StanVar colIndex SME.StanInt
  _ <- addFixedIntJson' (inputDataType rtt) colName Nothing cols
--  addDeclBinding' colDimName (SME.name colName)
  addDeclBinding colDimName colName
--  addUseBindingToDataSet rtt colDimName (SME.name colName)
  addUseBindingToDataSet rtt colDimName colIndex
  addColumnJson rtt ndsF vecF

modifyCode' :: (TE.StanProgram -> TE.StanProgram) -> BuilderState md gq -> BuilderState md gq
modifyCode' f bs = let (StanCode curBlock oldProg) = code bs in bs { code = StanCode curBlock $ f oldProg }

modifyCode :: (TE.StanProgram -> TE.StanProgram) -> StanBuilderM md gq ()
modifyCode f = modify $ modifyCode' f

modifyCodeE :: Either Text (TE.StanProgram -> TE.StanProgram) -> StanBuilderM md gq ()
modifyCodeE fE = stanBuildEither fE >>= modifyCode

setBlock' :: StanBlock -> BuilderState md gq -> BuilderState md gq
setBlock' b bs = bs { code = (code bs) { curBlock = b} } -- lenses!

setBlock :: StanBlock -> StanBuilderM md gq ()
setBlock = modify . setBlock'

getBlock :: StanBuilderM md gq StanBlock
getBlock = gets (curBlock . code)


{-
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

declareStanFunction' :: Text -> StanBuilderM md gq ()
declareStanFunction' t = inBlock SBFunctions $ traverse_ addLine $ fmap (<> "\n") $ T.lines t

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

profileStmt :: Text -> Text
profileStmt tag = "profile (\"" <>  tag <> "\")"

profile :: Text -> StanBuilderM md gq a -> StanBuilderM md gq a
profile tag x = do
  b <- getBlock
  addLine $ profileStmt (stanProfileName b <> ": " <> tag)
  bracketed 2 x
-}
-- place profiling statement at beginning of the block
profileStanBlock :: StanBlock -> StanBuilderM md gq ()
profileStanBlock b = modifyCode g where
  g :: TE.StanProgram -> TE.StanProgram
  g sp = newProgram
    where
      curBlock = TE.unStanProgram sp Array.! b
      newBlock = TE.profile (stanProfileName b) [] : curBlock
      newProgram = TE.StanProgram $ TE.unStanProgram sp Array.// [(b, newBlock)]

stanProfileName :: StanBlock -> Text
stanProfileName SBFunctions = "functions"
stanProfileName SBData = "data"
stanProfileName SBDataGQ = "data (GQ)"
stanProfileName SBTransformedData = "transformed data"
stanProfileName SBTransformedDataGQ = "transformed data (GQ)"
stanProfileName SBParameters = "parameters"
stanProfileName SBTransformedParameters = "transformed data"
stanProfileName SBModel = "model"
stanProfileName SBGeneratedQuantities = "generated quantities"
stanProfileName SBLogLikelihood = "generated quantities (LL)"

{-
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
-}
underscoredIf :: Text -> Text
underscoredIf t = if T.null t then "" else "_" <> t

{-
stanIndented :: StanBuilderM md gq a -> StanBuilderM md gq a
stanIndented = indented 2
-}
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

{-
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
      needGQ = gq `elem` [All, NoLL]
      dataBlockWithGQ = dataBlock sm <> if needGQ then ("\n// For Generated Quantities\n" <> dataBlockGQ sm) else ""
      transformedDataBlockMWithGQ = transformedDataBlockM sm
                                   <> if needGQ then fmap ("\n// For Generated Quantities\n" <>) (transformedDataBlockMGQ sm) else Nothing
   in maybeSection "functions" (functionsBlock sm)
      <> section "data" dataBlockWithGQ
      <> maybeSection "transformed data" transformedDataBlockMWithGQ
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
-}
-- The file is either not there, there but the same, or there but different so we
-- need an available file name to proceed
data ModelState = New | Same | Updated T.Text deriving (Show)

renameAndWriteIfNotSame :: GeneratedQuantities -> TE.StanProgram -> T.Text -> T.Text -> IO ModelState
renameAndWriteIfNotSame gq p modelDir modelName = do
  let fileName d n = T.unpack $ d <> "/" <> n <> ".stan"
      curFile = fileName modelDir modelName
      findAvailableName modelDir modelName n = do
        let newName = fileName modelDir (modelName <> "_o" <> T.pack (show n))
        newExists <- Dir.doesFileExist newName
        if newExists then findAvailableName modelDir modelName (n + 1) else return $ T.pack newName
  newModel <- case TE.programAsText gq p of
    Right x -> pure x
    Left msg -> X.throwIO $ X.userError $ toString msg
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

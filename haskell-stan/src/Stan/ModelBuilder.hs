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

module Stan.ModelBuilder where

import qualified Stan.JSON as Stan

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
import qualified Data.Set as Set
import qualified Data.Some as Some
import qualified Data.Text as T
import qualified Data.Text.IO as T
--import Data.Typeable (typeOf, (:~:)(..))
--import Data.Type.Equality (TestEquality(..),apply, (:~~:)(..))
import qualified Data.Time.Clock as Time
import qualified Data.Vector as Vector
import qualified Data.Hashable as Hashable
import qualified Say
import qualified System.Directory as Dir
import qualified System.Environment as Env
import qualified Type.Reflection as Reflection


--Constraints.deriveArgDict ''RowTypeTag


type DataBlock = T.Text

type TransformedDataBlock = T.Text

type ParametersBlock = T.Text

type TransformedParametersBlock = T.Text

type ModelBlock = T.Text

type GeneratedQuantitiesBlock = T.Text

data GeneratedQuantities = NoLL | OnlyLL | All

data StanModel = StanModel
  { dataBlock :: DataBlock,
    transformedDataBlockM :: Maybe TransformedDataBlock,
    parametersBlock :: ParametersBlock,
    transformedParametersBlockM :: Maybe TransformedParametersBlock,
    modelBlock :: ModelBlock,
    generatedQuantitiesBlockM :: Maybe GeneratedQuantitiesBlock,
    genLogLikelihoodBlock :: GeneratedQuantitiesBlock
  }
  deriving (Show, Eq, Ord)

data StanBlock = SBData
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

data ToMFoldable d row where
  ToMFoldable :: Foldable f => (d -> Maybe (f row)) -> ToMFoldable d row


data GroupTypeTag k where
  GroupTypeTag :: Typeable k => Text -> GroupTypeTag k

instance GADT.GEq GroupTypeTag where
  geq gta@(GroupTypeTag n1) gtb@(GroupTypeTag n2) =
    case Reflection.eqTypeRep (Reflection.typeOf gta) (Reflection.typeOf gtb) of
      Just Reflection.HRefl -> if n1 == n2 then Just Reflection.Refl else Nothing
      _ -> Nothing

instance Hashable.Hashable (Some.Some GroupTypeTag) where
  hash (Some.Some (GroupTypeTag n)) = Hashable.hash n
  hashWithSalt m (Some.Some (GroupTypeTag n)) = hashWithSalt m n

data IntIndex row = IntIndex { i_Size :: Int, i_Index :: row -> Maybe Int }

data MakeIndex r0 k = GivenIndex Int (k -> Int) (r0 -> k) | FoldToIndex (Foldl.Fold r0 (Int, k -> Maybe Int)) (r0 -> k)
data IndexMap r0 k = IndexMap (IntIndex r0) (k -> Maybe Int)

makeIndexMapF :: MakeIndex r k -> Foldl.Fold r (IndexMap r k)
makeIndexMapF (GivenIndex n g h) = pure $ IndexMap (IntIndex n (Just . g . h)) (Just . g)
makeIndexMapF (FoldToIndex fld h) = fmap (\(n, g) -> IndexMap (IntIndex n (g . h)) g) fld
--  let (n, g) = Foldl.fold fld rs in IndexMap (IntIndex n (g . h)) g

makeIndexFromEnum :: forall k r.(Enum k, Bounded k) => (r -> k) -> MakeIndex r k
makeIndexFromEnum h = GivenIndex (1 + eMax - eMin) index h where
  index k = 1 + (fromEnum k - eMin)
  eMin = fromEnum $ minBound @k
  eMax = fromEnum $ maxBound @k

makeIndexByCounting :: Ord k => (r -> k) -> MakeIndex r k
makeIndexByCounting h = FoldToIndex (Foldl.premap h $ indexFold 1) h

indexFold :: Ord k => Int -> Foldl.Fold k (Int, k -> Maybe Int)
indexFold start =  Foldl.Fold step init done where
  step s k = Set.insert k s
  init = Set.empty
  done s = (Set.size s, toIntM) where
    keyedList = zip (Set.toList s) [start..]
    mapToInt = Map.fromList keyedList
    toIntM k = Map.lookup k mapToInt

type GroupIndexMakerDHM r = DHash.DHashMap GroupTypeTag (MakeIndex r)
type GroupIndexDHM r = DHash.DHashMap GroupTypeTag (IndexMap r)

makeMainIndexes :: Foldable f => GroupIndexMakerDHM r -> f r -> (GroupIndexDHM r, Map Text (IntIndex r))
makeMainIndexes makerMap rs = (dhm, gm) where
  dhmF = DHash.traverse makeIndexMapF makerMap -- so we only fold once to make all the indices
  dhm = Foldl.fold dhmF rs
  namedIntIndex (GroupTypeTag n DSum.:=> IndexMap ii _) = (n, ii)
  gm = Map.fromList $ namedIntIndex <$> DHash.toList dhm

data RowInfo d r0 r = RowInfo { toFoldable :: ToFoldable d r
                              , jsonSeries :: JSONSeriesFold r
                              , indexMaker :: GroupIndexDHM r0 -> Maybe (r -> Maybe Int, r0 -> Maybe Int) }

initialRowInfo :: forall d r0 r k. Typeable k => ToFoldable d r -> Text -> (r -> k) -> RowInfo d r0 r
initialRowInfo tf groupName keyF = RowInfo tf mempty im where
  im :: GroupIndexDHM r0 -> Maybe (r -> Maybe Int, r0 -> Maybe Int)
  im gim = case DHash.lookup (GroupTypeTag @k groupName) gim of
    Nothing -> Nothing
    Just (IndexMap (IntIndex _ h) g) -> Just (g . keyF, h)

--emptyRowInfo :: RowInfo r
--emptyRowInfo = RowInfo mempty mempty

-- key for dependepent map.
data RowTypeTag d r where
  ModeledRowTag :: (Typeable d, Typeable r) => RowTypeTag d r
  RowTypeTag :: (Typeable d, Typeable r) => Text -> RowTypeTag d r

dsName :: RowTypeTag d r -> Text
dsName ModeledRowTag = "modeled"
dsName (RowTypeTag n) = n

dsSuffix :: RowTypeTag d r -> Text
dsSuffix ModeledRowTag = ""
dsSuffix (RowTypeTag n) = n


-- we need the empty constructors here to bring in the Typeable constraints in the GADT
instance GADT.GEq (RowTypeTag f) where
  geq a@(ModeledRowTag) b@(ModeledRowTag) =
    case Reflection.eqTypeRep (Reflection.typeOf a) (Reflection.typeOf b) of
       Just Reflection.HRefl -> Just Reflection.Refl
  geq ModeledRowTag (RowTypeTag _) = Nothing
  geq rta@(RowTypeTag n1) rtb@(RowTypeTag n2) =
    case Reflection.eqTypeRep (Reflection.typeOf rta) (Reflection.typeOf rtb) of
      Just Reflection.HRefl -> if (n1 == n2) then Just Reflection.Refl else Nothing
      _ -> Nothing

instance Hashable.Hashable (Some.Some (RowTypeTag d)) where
  hash (Some.Some ModeledRowTag) = Hashable.hash ("modeled" :: Text)
  hash (Some.Some (RowTypeTag n)) = Hashable.hash n
  hashWithSalt m (Some.Some ModeledRowTag) = hashWithSalt m ("modeled" :: Text)
  hashWithSalt m (Some.Some (RowTypeTag n)) = hashWithSalt m n

-- the key is a name for the data-set.  The tag carries the toDataSet function
type RowBuilder d r0 = DSum.DSum (RowTypeTag d) (RowInfo d r0)
type RowBuilderDHM d r0 = DHash.DHashMap (RowTypeTag d) (RowInfo d r0)

initialRowBuilders :: (Typeable d, Typeable row, Foldable f) => (d -> f row) -> RowBuilderDHM d r0
initialRowBuilders toModeled = DHash.fromList [ModeledRowTag
                                                DSum.:=> (RowInfo (ToFoldable toModeled) mempty (const Nothing))
                                              ,(RowTypeTag @_ @() "Null")
                                                DSum.:=> (RowInfo (ToFoldable (const $ Identity ())) mempty (const $ Just (const Nothing, const Nothing)))
                                              ]

--data DataSet d f r = DataSet (RowTypeTag d r) (d -> f r)


nullDataSetTag :: Typeable d => RowTypeTag d ()
nullDataSetTag = RowTypeTag "Null"

addFoldToDBuilder :: forall d r0 r.(Typeable d)
                  => RowTypeTag d r
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

buildJSON :: forall env d r0 .(Typeable d, Typeable r0) => d -> StanBuilderM env d r0 Aeson.Series
buildJSON d = do
  let fm t = maybe (Left t) Right
  rbs <- rowBuilders <$> get
  (RowInfo (ToFoldable toModeled) (JSONSeriesFold modeledJsonF) _) <- case DHash.lookup ModeledRowTag rbs of
    Nothing -> stanBuildError "Failed to find ModeledRowTag in DataBuilderState!"
    Just x -> return x
  gim <- asks $ groupIndexByType . groupEnv
  let modeled = toModeled d
      buildRowJSON :: DSum.DSum (RowTypeTag d) (RowInfo d r0) -> StanBuilderM env d r0 (Aeson.Series, Stan.StanJSONF r0 Aeson.Series)
      buildRowJSON (rtt DSum.:=> (RowInfo (ToFoldable toData) (JSONSeriesFold jsonF) getIndexF)) = do
        (rowIndexF, modelRowIndexF) <- case getIndexF gim of
          Nothing -> stanBuildError ("Failed to build indexing function for " <> dsName rtt)
          Just x -> return x
        let rowData = toData d
            mAsIntMap = Foldl.foldM (Foldl.FoldM (\im r -> fmap (\n -> IntMap.insert n r im) $ rowIndexF r) (return IntMap.empty) return) rowData
        asIntMap <- case mAsIntMap of
          Nothing -> stanBuildError ("key in dataset (" <> dsName rtt <> ") missing from index.")
          Just x -> return x
        let indexFold = Stan.valueToPairF (dsName rtt) (Stan.jsonArrayMF modelRowIndexF)
        rowS <- case Foldl.foldM jsonF asIntMap of
          Left err -> stanBuildError err
          Right x -> return x
        return (rowS, indexFold)
  (allRowSeries, indexFolds) <- unzip <$> traverse buildRowJSON (DHash.toList $ DHash.delete (ModeledRowTag @d @r0) rbs)
  modeledSeries <- case Foldl.foldM (modeledJsonF <> mconcat indexFolds) modeled of
    Left err -> stanBuildError err
    Right x -> return x
  return $ modeledSeries <> (mconcat allRowSeries)

data JSONRowFold d r = JSONRowFold (ToMFoldable d r) (Stan.StanJSONF r Aeson.Series)

--instance Semigroup (JSONRowFold d r) where
--  (JSONRowFold tm a) <> (JSONRowFold _ b) = JSONRowFold tm (a <> b)

buildJSONFromRows :: d -> DHash.DHashMap (RowTypeTag d) (JSONRowFold d) -> Either Text Aeson.Series
buildJSONFromRows d rowFoldMap = do
  let toSeriesOne (Some.Some (JSONRowFold (ToMFoldable f) fld)) = case f d of
        Nothing -> Left ""
        Just x -> Foldl.foldM fld x
  fmap mconcat $ traverse toSeriesOne $ DHash.elems rowFoldMap


buildJSONF :: forall env d r0 .(Typeable d, Typeable r0) => StanBuilderM env d r0 (DHash.DHashMap (RowTypeTag d) (JSONRowFold d))
buildJSONF = do
  let fm t = maybe (Left t) Right
  rbs <- rowBuilders <$> get
  (RowInfo (ToFoldable toModeled) (JSONSeriesFold modeledJsonF) im) <- case DHash.lookup ModeledRowTag rbs of
    Nothing -> stanBuildError "Failed to find ModeledRowTag in DataBuilderState!"
    Just x -> return x
  gim <- asks $ groupIndexByType . groupEnv
--  let modeled = toModeled d
  let buildRowJSONFolds :: DSum.DSum (RowTypeTag d) (RowInfo d r0) -> StanBuilderM env d r0 (DSum.DSum (RowTypeTag d) (JSONRowFold d), Stan.StanJSONF r0 Aeson.Series)
      buildRowJSONFolds (rtt DSum.:=> (RowInfo (ToFoldable toData) (JSONSeriesFold jsonF) getIndexF)) = do
        (rowIndexF, modelRowIndexF) <- case getIndexF gim of
          Nothing -> stanBuildError ("Failed to build indexing function for " <> dsName rtt)
          Just x -> return x
        let intMapF = Foldl.FoldM (\im r -> fmap (\n -> IntMap.insert n r im) $ rowIndexF r) (return IntMap.empty) return
            indexFold = Stan.valueToPairF (dsName rtt) (Stan.jsonArrayMF modelRowIndexF)
        return (rtt DSum.:=> JSONRowFold (ToMFoldable $ Foldl.foldM intMapF . toData) jsonF, indexFold)
  (allRowDSums, indexFolds) <- unzip <$> traverse buildRowJSONFolds (DHash.toList $ DHash.delete (ModeledRowTag @d @r0) rbs)
  let modeledDSum = ModeledRowTag DSum.:=> JSONRowFold (ToMFoldable $ Just . toModeled) (modeledJsonF <> mconcat indexFolds)
  return $ DHash.fromList $ modeledDSum : allRowDSums --modeledSeries <> (mconcat allRowSeries)


data BuilderState d r0 = BuilderState { usedNames :: !(Set Text)
                                      , rowBuilders :: !(RowBuilderDHM d r0)
                                      , code :: !StanCode
                                      }

initialBuilderState :: (Typeable d, Typeable r, Foldable f) => (d -> f r) -> BuilderState d r
initialBuilderState toModeled = BuilderState Set.empty (initialRowBuilders toModeled) emptyStanCode

newtype StanGroupBuilderM r0 a
  = StanGroupBuilderM { unStanGroupBuilderM :: ExceptT Text (State (GroupIndexMakerDHM r0)) a }
  deriving (Functor, Applicative, Monad, MonadState (GroupIndexMakerDHM r0))

data GroupEnv r0 = GroupEnv { groupIndexByType :: GroupIndexDHM r0
                            , groupIndexByName :: Map Text (IntIndex r0)
                            }
stanGroupBuildError :: Text -> StanGroupBuilderM r0 a
stanGroupBuildError t = StanGroupBuilderM $ ExceptT (pure $ Left t)

addGroup :: forall r0 k.Typeable k => Text -> MakeIndex r0 k -> StanGroupBuilderM r0 ()
addGroup gName mkIndex = do
  let gtt = GroupTypeTag @k gName
  gim <- get
  case DHash.lookup gtt gim of
    Nothing -> put $ DHash.insert gtt mkIndex gim
    Just _ -> stanGroupBuildError $ "Attempt to add a second group (\"" <> gName <> "\") at the same type as one already in group index builder map"

runStanGroupBuilder :: Foldable f => StanGroupBuilderM r0 () -> f r0 -> Either Text (GroupEnv r0)
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

addDataSetBuilder :: (Typeable r, Typeable d, Typeable k) => Text -> ToFoldable d r -> (r -> k) -> StanBuilderM env d r0 ()
addDataSetBuilder name toFoldable toKey = do
  let rtt = RowTypeTag name
  (BuilderState un rbs c) <- get
  case DHash.lookup rtt rbs of
    Just _ -> stanBuildError "Attempt to add 2nd data set with same type and name"
    Nothing -> do
      let rowInfo = initialRowInfo toFoldable name toKey
          newBS = DHash.insert rtt rowInfo rbs
      put (BuilderState un newBS c)

runStanBuilder' :: (Typeable d, Typeable r0, Foldable f)
               => env -> GroupEnv r0 -> (d -> f r0) -> StanBuilderM env d r0 a -> Either Text (BuilderState d r0, a)
runStanBuilder' userEnv ge toModeled sb = res where
  (resE, bs) = flip runState (initialBuilderState toModeled) . flip runReaderT (StanEnv userEnv ge) . runExceptT $ unStanBuilderM sb
  res = fmap (bs,) resE

runStanBuilder :: (Foldable f, Typeable d, Typeable r0)
               => d -> (d -> f r0) -> env -> StanGroupBuilderM r0 () -> StanBuilderM env d r0 a -> Either Text (BuilderState d r0, a)
runStanBuilder d toModeled userEnv sgb sb =
  let eGE = runStanGroupBuilder sgb (toModeled d)
  in case eGE of
    Left err -> Left $ "Error running group builder: " <> err
    Right ge -> runStanBuilder' userEnv ge toModeled sb

stanBuildError :: Text -> StanBuilderM env d r0 a
stanBuildError t = StanBuilderM $ ExceptT (pure $ Left t)


addJson :: (Typeable d)
        => RowTypeTag d r
        -> Text
        -> Stan.StanJSONF r Aeson.Series
        -> StanBuilderM env d r0 ()
addJson rtt name fld = do
  (BuilderState un rbs code) <- get
  when (Set.member name un) $ stanBuildError $ "Duplicate name in json builders: \"" <> name <> "\""
  newBS <- case addFoldToDBuilder rtt fld rbs of
    Nothing -> stanBuildError $ "Attempt to add Json to an unitialized dataset (" <> dsName rtt <> ")"
    Just x -> return x
  put $ BuilderState (Set.insert name un)  newBS code

-- things like lengths may often be re-added
-- maybe work on a cleaner way...
addJsonUnchecked :: (Typeable d)
                 => RowTypeTag d r
                 -> Text
                 -> Stan.StanJSONF r Aeson.Series
                 -> StanBuilderM env d r0 ()
addJsonUnchecked rtt name fld = do
  (BuilderState un rbs code) <- get
  if Set.member name un
    then return ()
    else (do
             newBS <-  case addFoldToDBuilder rtt fld rbs of
               Nothing -> stanBuildError $ "Attempt to add Json to an unitialized dataset (" <> dsName rtt <> ")"
               Just x -> return x
             put $ BuilderState (Set.insert name un) newBS code
         )

addFixedIntJson :: Typeable d => Text -> Int -> StanBuilderM env d r0 ()
addFixedIntJson name n = addJson nullDataSetTag name (Stan.constDataF name n) -- const $ Right $ name Aeson..= n)

-- These get re-added each time something adds a column built from the data-set.
-- But we only need it once per data set.
addLengthJson :: (Typeable d) => RowTypeTag d r -> Text -> StanBuilderM env d r0 ()
addLengthJson rtt name = addJsonUnchecked rtt name (Stan.namedF name Foldl.length)

addColumnJson :: (Typeable d, Aeson.ToJSON x)
              => RowTypeTag d r -> Text -> Text -> (r -> x) -> StanBuilderM env d r0 ()
addColumnJson rtt name suffix toX = do
  addLengthJson rtt ("N" <> underscoredIf suffix)
  let fullName = name <> underscoredIf suffix
  addJson rtt fullName (Stan.valueToPairF fullName $ Stan.jsonArrayF toX)

addColumnMJson :: (Typeable d, Typeable r, Aeson.ToJSON x)
              => RowTypeTag d r -> Text -> Text -> (r -> Maybe x) -> StanBuilderM env d r0 ()
addColumnMJson rtt name suffix toMX = do
  addLengthJson rtt ("N" <> underscoredIf suffix)
  let fullName = name <> underscoredIf suffix
  addJson rtt fullName (Stan.valueToPairF fullName $ Stan.jsonArrayMF toMX)


add2dMatrixJson :: (Typeable d)
                => RowTypeTag d r -> Text -> Text -> Int -> (r -> Vector.Vector Double) -> StanBuilderM env d r0 ()
add2dMatrixJson rtt name suffix cols vecF = do
  addFixedIntJson ("K" <> suffix) cols
  addColumnJson rtt name suffix vecF

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
  (BuilderState _ _ (StanCode b _)) <- get
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

fixedEffectsQR :: Text -> Text -> Text -> Text -> StanBuilderM env d r ()
fixedEffectsQR thinSuffix matrix rows cols = do
  let ri = "R" <> thinSuffix <> "_ast_inverse"
  inBlock SBData $ do
    addStanLine $ "int<lower=1> " <> cols
    addStanLine $ "matrix[" <> rows <> ", " <> cols <> "] " <> matrix
  inBlock SBParameters $ addStanLine $ "vector[" <> cols <> "] theta" <> matrix
  inBlock SBTransformedData $ do
    let q = "Q" <> thinSuffix <> "_ast"
        r = "R" <> thinSuffix <> "_ast"
    addStanLine $ "matrix[" <> rows <> ", " <> cols <> "] " <> q
    addStanLine $ "matrix[" <> cols <> ", " <> cols <> "] " <> r
    addStanLine $ "matrix[" <> cols <> ", " <> cols <> "] " <> ri
    addStanLine $ q <> " = qr_thin_Q(" <> matrix <> ") * sqrt(" <> rows <> " - 1)"
    addStanLine $ r <> " = qr_thin_R(" <> matrix <> ") / sqrt(" <> rows <> " - 1)"
    addStanLine $ ri <> " = inverse(" <> r <> ")"
  inBlock SBTransformedParameters $ do
    addStanLine $ "vector[" <> cols <> "] beta" <> matrix
    addStanLine $ "beta" <> matrix <> " = " <> ri <> " * theta" <> matrix


stanIndented :: StanBuilderM env d r a -> StanBuilderM env d r a
stanIndented = indented 2

inBlock :: StanBlock -> StanBuilderM env d r a -> StanBuilderM env d r a
inBlock b m = do
  oldBlock <- getBlock
  setBlock b
  x <- m
  setBlock oldBlock
  return x

data VectorContext = Vectorized | NonVectorized Text

data StanModelTerm where
  Scalar :: Text -> StanModelTerm
  Vectored :: Text -> StanModelTerm
  Indexed :: Text -> Text -> StanModelTerm

printTerm :: Map Text Text -> VectorContext -> StanModelTerm -> Maybe Text
printTerm _ _ (Scalar t) = Just t
printTerm _ Vectorized (Vectored n) = Just n
printTerm _ (NonVectorized i) (Vectored n) = Just $ n <> "[" <> i <> "]"
printTerm indexMap _ (Indexed k t) = (\i -> t <> "[" <> i <> "]") <$> Map.lookup k indexMap

data StanExpr where
--  NullE :: StanExpr
  TermE :: StanModelTerm -> StanExpr --StanModelTerm
  BinOpE :: Text -> StanExpr -> StanExpr -> StanExpr
  FunctionE :: Text -> [StanExpr] -> StanExpr
  VectorFunctionE :: Text -> StanExpr -> StanExpr -- function to add when the context is vectorized
  GroupE :: StanExpr -> StanExpr

printExpr :: Map Text Text -> VectorContext -> StanExpr -> Maybe Text
--printExpr _ _ NullE = Just ""
printExpr im vc (TermE t) = printTerm im vc t
printExpr im vc (BinOpE o l r) = (\l' r' -> l' <> " " <> o <> " " <> r') <$> printExpr im vc l <*> printExpr im vc r
printExpr im vc (FunctionE f es) = (\es' -> f <> "(" <> T.intercalate ", " es' <> ")") <$> traverse (printExpr im vc) es
printExpr im Vectorized (VectorFunctionE f e) = (\e' -> f <> "(" <>  e' <> ")") <$> printExpr im Vectorized e
printExpr im vc@(NonVectorized _) (VectorFunctionE f e) = printExpr im vc e
printExpr im vc (GroupE e) = (\e' -> "(" <> e' <> ")") <$> printExpr im vc e

printExprM :: Text -> Map Text Text -> VectorContext -> StanBuilderM env d r StanExpr -> StanBuilderM env d r Text
printExprM context im vc me = do
  e <- me
  case printExpr im vc e of
    Nothing -> stanBuildError $ context <> ": Missing index while constructing model expression"
    Just t -> return t

multiOp :: Text -> NonEmpty StanExpr -> StanExpr
multiOp o es = foldl' (BinOpE o) (head es) (tail es)

stanModelAsText :: GeneratedQuantities -> StanModel -> T.Text
stanModelAsText gq sm =
  let section h b = h <> " {\n" <> b <> "}\n"
      maybeSection h = maybe "" (section h)
   in section "data" (dataBlock sm)
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

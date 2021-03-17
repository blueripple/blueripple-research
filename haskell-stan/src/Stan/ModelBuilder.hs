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

data MakeIndex r k = GivenIndex (k -> Int) (r -> k) | FoldToIndex (Foldl.Fold r (k -> Maybe Int)) (r -> k)
data IndexMap r k = IndexMap (k -> Maybe Int) (r -> k)

makeIndexMap :: Foldable f => MakeIndex r k -> f r -> IndexMap r k
makeIndexMap (GivenIndex g h) _ = IndexMap (Just . g) h
makeIndexMap (FoldToIndex fld h) rs = IndexMap (Foldl.fold fld rs) h

makeEnumeratedMakeIndex :: (r -> k) -> MakeIndex r k
makeEnumeratedMakeIndex h = undefined

type GroupIndexMakerDHM r = DHash.DHashMap GroupTypeTag (MakeIndex r)
type GroupIndexDHM r = DHash.DHashMap GroupTypeTag (IndexMap r)

makeMainIndexes :: Foldable f => GroupIndexMakerDHM r -> f r -> GroupIndexDHM r
makeMainIndexes makerMap rs = DHash.map (\m -> makeIndexMap m rs) makerMap

data RowInfo d r0 r = RowInfo { toFoldable :: ToFoldable d r
                             , jsonSeries :: JSONSeriesFold r
                             , indexMaker :: GroupIndexDHM r0 -> Maybe (r -> Maybe Int) }

initialRowInfo :: forall d r0 r k. Typeable k => ToFoldable d r -> Text -> (r -> k) -> RowInfo d r0 r
initialRowInfo tf groupName keyF = RowInfo tf mempty im where
  im :: GroupIndexDHM r0 -> Maybe (r -> Maybe Int)
  im gim = case DHash.lookup (GroupTypeTag @k groupName) gim of
    Nothing -> Nothing
    Just (IndexMap h _) -> Just (h . keyF)

--emptyRowInfo :: RowInfo r
--emptyRowInfo = RowInfo mempty mempty

-- key for dependepent map.
data RowTypeTag d r where
  ModeledRowTag :: (Typeable d, Typeable r) => RowTypeTag d r
  RowTypeTag :: (Typeable d, Typeable r) => Text -> RowTypeTag d r

dsName :: RowTypeTag d r -> Text
dsName ModeledRowTag = "modeled"
dsName (RowTypeTag n) = n

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
initialRowBuilders toModeled = DHash.singleton ModeledRowTag (RowInfo (ToFoldable toModeled) mempty (const Nothing))

addFoldToDBuilder :: forall d r0 r.(Typeable d, Typeable r)
                  => Text
                  -> Stan.StanJSONF r Aeson.Series
                  -> RowBuilderDHM d r0
                  -> Maybe (RowBuilderDHM d r0)
addFoldToDBuilder t fld rbm =
  let rtt = RowTypeTag @d @r t
  in case DHash.lookup rtt rbm of
    Nothing -> Nothing --DHash.insert rtt (RowInfo (JSONSeriesFold fld) (const Nothing)) rbm
    Just (RowInfo tf (JSONSeriesFold fld') gi)
      -> Just $ DHash.insert rtt (RowInfo tf (JSONSeriesFold $ fld' <> fld) gi) rbm


buildJSONSeries :: forall d r0 f. RowBuilderDHM d r0 -> d -> Either Text Aeson.Series
buildJSONSeries rbm d =
  let foldOne :: RowBuilder d r0 -> Either Text Aeson.Series
      foldOne ((RowTypeTag _ ) DSum.:=> (RowInfo (ToFoldable h) (JSONSeriesFold fld) _)) = Foldl.foldM fld (h d)
  in mconcat <$> (traverse foldOne $ DHash.toList rbm)


data DataBuilderState d r0
  = DataBuilderState { usedNames :: Set Text
                     , indexMakers :: GroupIndexMakerDHM r0
                     , builders :: RowBuilderDHM d r0
                    }

buildJSON :: forall d r.(Typeable d, Typeable r) => DataBuilderState d r -> d -> Either Text Aeson.Series
buildJSON dbs d = do
  let fm t = maybe (Left t) Right
  (RowInfo (ToFoldable toModeled) (JSONSeriesFold jsonF) _) <- fm "Failed to find ModeledRowTag in DataBuilderState!"
                                                               $ DHash.lookup ModeledRowTag (builders dbs)
  let modeled = toModeled d
  modeledSeries <- Foldl.foldM jsonF modeled
  let gim = makeMainIndexes (indexMakers dbs) modeled
      rowSeries (rtt DSum.:=> (RowInfo (ToFoldable toData) (JSONSeriesFold jsonF) getIndexF)) = do
        indexF <- fm ("Failed to build indexing function for " <> dsName rtt) $ getIndexF gim
        let rowData = toData d
        asIntMap <- fm ("key in dataset (" <> dsName rtt <> ") missing from index.")
                    $ Foldl.foldM (Foldl.FoldM (\im r -> fmap (\n -> IntMap.insert n r im) $ indexF r) (return IntMap.empty) return) rowData
        Foldl.foldM jsonF asIntMap
  allRowSeries <- mconcat <$> (traverse rowSeries $ DHash.toList $ DHash.delete (ModeledRowTag @d @r) $ builders dbs)
  return $ modeledSeries <> allRowSeries

data BuilderState d modeledRow = BuilderState { dataBuilder :: !(DataBuilderState d modeledRow)
                                              , code :: !StanCode
                                              }

initialBuilderState :: (Typeable d, Typeable r, Foldable f) => (d -> f r) -> BuilderState d r
initialBuilderState toModeled = BuilderState (DataBuilderState Set.empty DHash.empty $ initialRowBuilders toModeled) emptyStanCode

newtype StanBuilderM env d modeledRow a
  = StanBuilderM { unStanBuilderM :: ExceptT Text (ReaderT env (State (BuilderState d modeledRow))) a }
  deriving (Functor, Applicative, Monad, MonadReader env, MonadState (BuilderState d modeledRow))

runStanBuilder :: (Typeable d, Typeable modeledRow, Foldable f)
               => env -> (d -> f modeledRow) -> StanBuilderM env d modeledRow a -> Either Text (BuilderState d modeledRow, a)
runStanBuilder e toModeled sb = res where
  (resE, bs) = flip runState (initialBuilderState toModeled) . flip runReaderT e . runExceptT $ unStanBuilderM sb
  res = fmap (bs,) resE

stanBuildError :: Text -> StanBuilderM env d modeldRow a
stanBuildError t = StanBuilderM $ ExceptT (pure $ Left t)

askEnv :: StanBuilderM env d modeledRow env
askEnv = ask

asksEnv :: (env -> a) -> StanBuilderM env d modeledRow a
asksEnv = asks

data DataSet d f row = DataSet Text (d -> f row)

nullDataSet :: DataSet d Identity ()
nullDataSet = DataSet "Null" (const $ Identity ())

addJson :: (Typeable d, Typeable row, Foldable f)
        => DataSet d f row
        -> Text
        -> Stan.StanJSONF row Aeson.Series
        -> StanBuilderM env d modeledRow ()
addJson (DataSet dName toDataSet) name fld = do
  (BuilderState (DataBuilderState un ims rbs) code) <- get
  when (Set.member name un) $ stanBuildError $ "Duplicate name in json builders: \"" <> name <> "\""
  newBS <- case addFoldToDBuilder dName fld rbs of
    Nothing -> stanBuildError $ "Attempt to add Json to an unitialized dataset (" <> dName <> ")"
    Just x -> return x
  put $ BuilderState (DataBuilderState (Set.insert name un) ims newBS) code

-- things like lengths may often be re-added
-- maybe work on a cleaner way...
addJsonUnchecked :: (Typeable d, Typeable row, Foldable f)
                 => DataSet d f row
                 -> Text
                 -> Stan.StanJSONF row Aeson.Series
                 -> StanBuilderM env d modeledRow ()
addJsonUnchecked (DataSet dName _) name fld = do
  (BuilderState (DataBuilderState un ims rbs) code) <- get
  if Set.member name un
    then return ()
    else (do
             newBS <-  case addFoldToDBuilder dName fld rbs of
               Nothing -> stanBuildError $ "Attempt to add Json to an unitialized dataset (" <> dName <> ")"
               Just x -> return x
             put $ BuilderState (DataBuilderState (Set.insert name un) ims newBS) code
         )

addFixedIntJson :: Typeable d => Text -> Int -> StanBuilderM env d modeledRow ()
addFixedIntJson name n = addJson nullDataSet name (Stan.constDataF name n) -- const $ Right $ name Aeson..= n)

-- These get re-added each time something adds a column built from the data-set.
-- But we only need it once per data set.
addLengthJson :: (Typeable d, Foldable f, Typeable a) => DataSet d f a -> Text -> StanBuilderM env d modeledRow ()
addLengthJson dataSet name = addJsonUnchecked dataSet name (Stan.namedF name Foldl.length)

addColumnJson :: (Typeable d, Typeable a, Aeson.ToJSON x, Foldable f)
              => DataSet d f a -> Text -> Text -> (a -> x) -> StanBuilderM env d modeledRow ()
addColumnJson dataSet name suffix toX = do
  addLengthJson dataSet ("N" <> underscoredIf suffix)
  let fullName = name <> underscoredIf suffix
  addJson dataSet fullName (Stan.valueToPairF fullName $ Stan.jsonArrayF toX)

addColumnMJson :: (Typeable d, Typeable a, Aeson.ToJSON x, Foldable f)
              => DataSet d f a -> Text -> Text -> (a -> Maybe x) -> StanBuilderM env d modeledRow ()
addColumnMJson dataSet name suffix toMX = do
  addLengthJson dataSet ("N" <> underscoredIf suffix)
  let fullName = name <> underscoredIf suffix
  addJson dataSet fullName (Stan.valueToPairF fullName $ Stan.jsonArrayMF toMX)


add2dMatrixJson :: (Typeable d, Typeable a, Foldable f) => DataSet d f a -> Text -> Text -> Int -> (a -> Vector.Vector Double) -> StanBuilderM env d modeledRow ()
add2dMatrixJson dataSet name suffix cols vecF = do
  addFixedIntJson ("K" <> suffix) cols
  addColumnJson dataSet name suffix vecF

modifyCode' :: (StanCode -> StanCode) -> BuilderState d r -> BuilderState d r
modifyCode' f (BuilderState jb sc) = BuilderState jb (f sc)

modifyCode :: (StanCode -> StanCode) -> StanBuilderM env d r ()
modifyCode f = modify $ modifyCode' f

setBlock' :: StanBlock -> StanCode -> StanCode
setBlock' b (StanCode _ blocks) = StanCode b blocks

setBlock :: StanBlock -> StanBuilderM env d r ()
setBlock = modifyCode . setBlock'

getBlock :: StanBuilderM env d r StanBlock
getBlock = do
  (BuilderState _ (StanCode b _)) <- get
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

stanPrint :: [StanPrintable] -> StanBuilderM d env r ()
stanPrint ps =
  let f x = case x of
        StanLiteral x -> "\"" <> x <> "\""
        StanExpression x -> x
  in addStanLine $ "print(" <> T.intercalate ", " (fmap f ps) <> ")"

underscoredIf :: Text -> Text
underscoredIf t = if T.null t then "" else "_" <> t

fixedEffectsQR :: Text -> Text -> Text -> Text -> StanBuilderM d env r ()
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

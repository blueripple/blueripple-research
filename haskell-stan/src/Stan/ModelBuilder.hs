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
import qualified Data.List as List
import qualified Data.Map as Map
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

-- f is existential here.  We supply the choice when we construct a ToFoldable
data ToFoldable d row where
  ToFoldable :: Foldable f => (d -> f row) -> ToFoldable d row

-- key for dependepent map.
data RowTypeTag d row where
  RowTypeTag :: (Typeable d, Typeable row) => Text -> ToFoldable d row -> RowTypeTag d row
--  RowTypeTag :: (Typeable d, Typeable row) => Text -> (forall f. Foldable f => d -> f row) -> RowTypeTag d row

-- we need the empty constructors here to bring in the Typeable constraints in the GADT
instance GADT.GEq (RowTypeTag f) where
  geq rta@(RowTypeTag n1 _) rtb@(RowTypeTag n2 _) =
    case Reflection.eqTypeRep (Reflection.typeOf rta) (Reflection.typeOf rtb) of
      Just Reflection.HRefl -> if (n1 == n2) then Just Reflection.Refl else Nothing
      _ -> Nothing

instance Hashable.Hashable (Some.Some (RowTypeTag d)) where
  hash (Some.Some (RowTypeTag n _)) = Hashable.hash n
  hashWithSalt m (Some.Some (RowTypeTag n _)) = Hashable.hashWithSalt m n

-- the key is a name for the data-set.  The tag carries the toDataSet function
type JSONBuilder d = DSum.DSum (RowTypeTag d) JSONSeriesFold
type JSONBuilderDHM d = DHash.DHashMap (RowTypeTag d) JSONSeriesFold


addFoldToDBuilder :: (Typeable d, Typeable row, Foldable f)
                  => Text -> (d -> f row) -> Stan.StanJSONF row Aeson.Series -> JSONBuilderDHM d -> JSONBuilderDHM d
addFoldToDBuilder t f fld jbm =
  let rtt = (RowTypeTag t (ToFoldable f))
  in case DHash.lookup rtt jbm of
    Nothing -> DHash.insert rtt (JSONSeriesFold fld) jbm
    Just (JSONSeriesFold fld') -> DHash.insert rtt (JSONSeriesFold $ fld' <> fld) jbm


buildJSONSeries :: forall d f. JSONBuilderDHM d -> d -> Either Text Aeson.Series
buildJSONSeries jbm d =
  let foldOne :: JSONBuilder d -> Either Text Aeson.Series
      foldOne ((RowTypeTag _ (ToFoldable h)) DSum.:=> (JSONSeriesFold fld)) = Foldl.foldM fld (h d)
  in mconcat <$> (traverse foldOne $ DHash.toList jbm)


data JSONBuilderState d = JSONBuilderState { usedNames :: Set Text, builders :: JSONBuilderDHM d }

data BuilderState d = BuilderState { jsonBuilder :: JSONBuilderState d, code :: !StanCode }

initialBuilderState :: BuilderState d
initialBuilderState = BuilderState (JSONBuilderState Set.empty DHash.empty) emptyStanCode

newtype StanBuilderM env d a = StanBuilderM { unStanBuilderM :: ExceptT Text (ReaderT env (State (BuilderState d))) a }
  deriving (Functor, Applicative, Monad, MonadReader env, MonadState (BuilderState d))

runStanBuilder :: env -> StanBuilderM env d a -> Either Text (BuilderState d, a)
runStanBuilder e sb = res where
  (resE, bs) = flip runState initialBuilderState . flip runReaderT e . runExceptT $ unStanBuilderM sb
  res = fmap (bs,) resE

stanBuildError :: Text -> StanBuilderM row d a
stanBuildError t = StanBuilderM $ ExceptT (pure $ Left t)

askEnv :: StanBuilderM env d env
askEnv = ask

asksEnv :: (env -> a) -> StanBuilderM env d a
asksEnv = asks

data DataSet d f row = DataSet Text (d -> f row)

nullDataSet :: DataSet d Identity ()
nullDataSet = DataSet "Null" (const $ Identity ())

addJson :: (Typeable d, Typeable row, Foldable f) => DataSet d f row -> Text -> Stan.StanJSONF row Aeson.Series -> StanBuilderM env d ()
addJson (DataSet dName toDataSet) name fld = do
  (BuilderState (JSONBuilderState un jbs) code) <- get
  when (Set.member name un) $ stanBuildError $ "Duplicate name in json builders: \"" <> name <> "\""
  let newBS = addFoldToDBuilder dName toDataSet fld jbs
  put $ BuilderState (JSONBuilderState (Set.insert name un) newBS) code

-- things like lengths may often be re-added
-- maybe work on a cleaner way...
addJsonUnchecked :: (Typeable d, Typeable row, Foldable f) => DataSet d f row -> Text -> Stan.StanJSONF row Aeson.Series -> StanBuilderM env d ()
addJsonUnchecked (DataSet dName toDataSet) name fld = do
  (BuilderState (JSONBuilderState un jbs) code) <- get
  if Set.member name un
    then return ()
    else (do
             let newBS = addFoldToDBuilder dName toDataSet fld jbs
             put $ BuilderState (JSONBuilderState (Set.insert name un) newBS) code
         )

addFixedIntJson :: Typeable d => Text -> Int -> StanBuilderM env d ()
addFixedIntJson name n = addJson nullDataSet name (Stan.constDataF name n) -- const $ Right $ name Aeson..= n)

-- These get re-added each time something adds a column built from the data-set.
-- But we only need it once per data set.
addLengthJson :: (Typeable d, Foldable f, Typeable a) => DataSet d f a -> Text -> StanBuilderM env d ()
addLengthJson dataSet name = addJsonUnchecked dataSet name (Stan.namedF name Foldl.length)

addColumnJson :: (Typeable d, Typeable a, Aeson.ToJSON x) => Foldable f => DataSet d f a -> Text -> Text -> (a -> x) -> StanBuilderM env d ()
addColumnJson dataSet name suffix toX = do
  addLengthJson dataSet ("N" <> suffix)
  addJson dataSet (name <> suffix) (Stan.valueToPairF name $ Stan.jsonArrayF toX)

add2dMatrixJson :: (Typeable d, Typeable a, Foldable f) => DataSet d f a -> Text -> Text -> Int -> (a -> Vector.Vector Double) -> StanBuilderM env d ()
add2dMatrixJson dataSet name suffix cols vecF = do
  addFixedIntJson ("K" <> suffix) cols
  addColumnJson dataSet name suffix vecF

modifyCode' :: (StanCode -> StanCode) -> BuilderState d -> BuilderState d
modifyCode' f (BuilderState jb sc) = BuilderState jb (f sc)

modifyCode :: (StanCode -> StanCode) -> StanBuilderM env d ()
modifyCode f = modify $ modifyCode' f

setBlock' :: StanBlock -> StanCode -> StanCode
setBlock' b (StanCode _ blocks) = StanCode b blocks

setBlock :: StanBlock -> StanBuilderM env d ()
setBlock = modifyCode . setBlock'

getBlock :: StanBuilderM env d StanBlock
getBlock = do
  (BuilderState _ (StanCode b _)) <- get
  return b

addLine' :: Text -> StanCode -> StanCode
addLine' t (StanCode b blocks) =
  let (WithIndent curCode curIndent) = blocks Array.! b
      newCode = curCode <> T.replicate curIndent " " <> t
  in StanCode b (blocks Array.// [(b, WithIndent newCode curIndent)])

addLine :: Text -> StanBuilderM env d ()
addLine = modifyCode . addLine'

addStanLine :: Text -> StanBuilderM env d ()
addStanLine t = addLine $ t <> ";\n"

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
  addLine " {\n"
  x <- indented n m
  addLine "}\n"
  return x

stanForLoop :: Text -> Maybe Text -> Text -> (Text -> StanBuilderM env d a) -> StanBuilderM env d a
stanForLoop counter mStart end loopF = do
  let start = fromMaybe "1" mStart
  addLine $ "for (" <> counter <> " in " <> start <> ":" <> end <> ")"
  bracketed 2 $ loopF counter

data StanPrintable = StanLiteral Text | StanExpression Text

stanPrint :: [StanPrintable] -> StanBuilderM d env ()
stanPrint ps =
  let f x = case x of
        StanLiteral x -> "\"" <> x <> "\""
        StanExpression x -> x
  in addStanLine $ "print(" <> T.intercalate ", " (fmap f ps) <> ")"

underscoredIf :: Text -> Text
underscoredIf t = if T.null t then "" else "_" <> t

fixedEffectsQR :: Text -> Text -> Text -> Text -> StanBuilderM d env ()
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


stanIndented :: StanBuilderM env d a -> StanBuilderM env d a
stanIndented = indented 2

inBlock :: StanBlock -> StanBuilderM env d a -> StanBuilderM env d a
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

printExprM :: Text -> Map Text Text -> VectorContext -> StanBuilderM env d StanExpr -> StanBuilderM env d Text
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

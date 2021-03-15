{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeFamilies #-}

module Stan.ModelBuilder where

import qualified Stan.JSON as Stan

import Prelude hiding (All)
import qualified Control.Foldl as Foldl
import qualified Data.Aeson as Aeson
import qualified Data.Array as Array
import qualified Data.Map as Map
import qualified Data.Text as T
import qualified Data.Text.IO as T
import qualified Data.Time.Clock as Time
import qualified Data.Vector as Vector
import qualified Say
import qualified System.Directory as Dir
import qualified System.Environment as Env

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

initialBuilderState :: BuilderState d
initialBuilderState = BuilderState mempty emptyStanCode

data BuilderState a = BuilderState { jsonBuilder :: !(Map Text (a -> Either Text Aeson.Series)), code :: !StanCode }

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

addJsonBuilder' :: Map Text (d -> Either Text Aeson.Series) -> BuilderState d -> BuilderState d
addJsonBuilder' jsonB (BuilderState jsonB' sc) = BuilderState (jsonB' <> jsonB) sc

updateJsonBuilder' :: Map Text (d -> Either Text Aeson.Series) -> BuilderState d -> BuilderState d
updateJsonBuilder' jsonB (BuilderState _ sc) = BuilderState jsonB sc

addJsonBuilder :: Map Text (d -> Either Text Aeson.Series) -> StanBuilderM env d ()
addJsonBuilder = modify . addJsonBuilder'

addJson :: Text -> (d -> Either Text Aeson.Series) -> StanBuilderM env d ()
addJson name makeSeries = do
  bs <- get
  let jbMap = jsonBuilder bs
  case Map.lookup name jbMap of
    Nothing -> put $ updateJsonBuilder' (Map.insert name makeSeries jbMap) bs
    Just _ -> stanBuildError $ "\"" <> name <> "\" duplicated in jsonBuilderMap."

-- things like lengths may often be re-added
-- maybe work on a cleaner way...
addJsonUnchecked :: Text -> (d -> Either Text Aeson.Series) -> StanBuilderM env d ()
addJsonUnchecked name makeSeries = do
  bs <- get
  let jbMap = jsonBuilder bs
  put $ updateJsonBuilder' (Map.insert name makeSeries jbMap) bs


addFixedIntJson :: Text -> Int -> StanBuilderM env d ()
addFixedIntJson name n = addJson name (const $ Right $ name Aeson..= n)

-- These get re-added each time something adds a column built from the data-set.
-- But we only need it once per data set.
addLengthJson :: Foldable f => Text -> (d -> f a) -> StanBuilderM env d ()
addLengthJson name toDataSet = addJsonUnchecked name (Foldl.foldM (Stan.namedF name Foldl.length) . toDataSet)

addColumnJson :: Aeson.ToJSON x => Foldable f => Text -> Text -> (d -> f a) -> (a -> x) -> StanBuilderM env d ()
addColumnJson name suffix toDataSet toX = do
  addLengthJson ("N" <> suffix) toDataSet
  addJson (name <> suffix) (Foldl.foldM (Stan.valueToPairF name $ Stan.jsonArrayF toX) . toDataSet)

add2dMatrixJson :: Foldable f => Text -> Text -> Int -> (d -> f a) -> (a -> Vector.Vector Double) -> StanBuilderM env d ()
add2dMatrixJson name suffix cols toDataSet vecF = do
  addFixedIntJson ("K" <> suffix) cols
  addColumnJson name suffix toDataSet vecF




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

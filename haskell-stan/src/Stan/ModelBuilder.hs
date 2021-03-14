{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeFamilies #-}

module Stan.ModelBuilder where

import Prelude hiding (All)
import qualified Data.Array as Array
import qualified Data.Map as Map
import qualified Data.Text as T
import qualified Data.Text.IO as T
import qualified Data.Time.Clock as Time
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

newtype StanBuilderM env a = StanBuilderM { unStanBuilderM :: ExceptT Text (ReaderT env (State StanCode)) a }
  deriving (Functor, Applicative, Monad, MonadReader env, MonadState StanCode)

runStanBuilder :: env -> StanBuilderM env a -> Either Text (StanCode, a)
runStanBuilder e sb = res where
  (resE, sc) = flip runState emptyStanCode . flip runReaderT e . runExceptT $ unStanBuilderM sb
  res = fmap (sc,) resE

stanBuildError :: Text -> StanBuilderM row a
stanBuildError t = StanBuilderM $ ExceptT (pure $ Left t)

askEnv :: StanBuilderM env env
askEnv = ask

asksEnv :: (env -> a) -> StanBuilderM env a
asksEnv = asks

setBlock' :: StanBlock -> StanCode -> StanCode
setBlock' b (StanCode _ blocks) = StanCode b blocks

setBlock :: StanBlock -> StanBuilderM env ()
setBlock = modify . setBlock'

getBlock :: StanBuilderM env StanBlock
getBlock = do
  (StanCode b _) <- get
  return b

addLine' :: Text -> StanCode -> StanCode
addLine' t (StanCode b blocks) =
  let (WithIndent curCode curIndent) = blocks Array.! b
      newCode = curCode <> T.replicate curIndent " " <> t
  in StanCode b (blocks Array.// [(b, WithIndent newCode curIndent)])

addLine :: Text -> StanBuilderM env ()
addLine = modify . addLine'

addStanLine :: Text -> StanBuilderM env ()
addStanLine t = addLine $ t <> ";\n"

indent :: Int -> StanCode -> StanCode
indent n (StanCode b blocks) =
  let (WithIndent curCode curIndent) = blocks Array.! b
  in StanCode b (blocks Array.// [(b, WithIndent curCode (curIndent + n))])

-- code inserted here will be indented n extra spaces
indented :: Int -> StanBuilderM env a -> StanBuilderM env a
indented n m = do
  modify (indent n)
  x <- m
  modify (indent $ negate n)
  return x

bracketed :: Int -> StanBuilderM env a -> StanBuilderM env a
bracketed n m = do
  addLine " {\n"
  x <- indented n m
  addLine "}\n"
  return x

stanForLoop :: Text -> Maybe Text -> Text -> (Text -> StanBuilderM env a) -> StanBuilderM env a
stanForLoop counter mStart end loopF = do
  let start = fromMaybe "1" mStart
  addLine $ "for (" <> counter <> " in " <> start <> ":" <> end <> ")"
  bracketed 2 $ loopF counter

data StanPrintable = StanLiteral Text | StanExpression Text

stanPrint :: [StanPrintable] -> StanBuilderM env ()
stanPrint ps =
  let f x = case x of
        StanLiteral x -> "\"" <> x <> "\""
        StanExpression x -> x
  in addStanLine $ "print(" <> T.intercalate ", " (fmap f ps) <> ")"

fixedEffectsQR :: Text -> Text -> Text -> Text -> StanBuilderM env ()
fixedEffectsQR matrix suffix rows cols = do
  let ri = "R" <> suffix <> "_ast_inverse"
  inBlock SBData $ do
    addStanLine $ "int K" <> suffix
    addStanLine $ "matrix[" <> rows <> ", " <> cols <> "] " <> matrix <> suffix
  inBlock SBParameters $ addStanLine $ "vector[" <> cols <> "] theta" <> matrix <> suffix
  inBlock SBTransformedData $ do
    let q = "Q" <> suffix <> "_ast"
        r = "R" <> suffix <> "_ast"
    addStanLine $ "matrix[" <> rows <> ", " <> cols <> "] " <> q
    addStanLine $ "matrix[" <> cols <> ", " <> cols <> "] " <> r
    addStanLine $ "matrix[" <> cols <> ", " <> cols <> "] " <> ri
    addStanLine $ q <> " = qr_thin_Q(" <> matrix <> suffix <> ") * sqrt(" <> rows <> " - 1)"
    addStanLine $ r <> " = qr_thin_R(" <> matrix <> suffix <> ") / sqrt(" <> rows <> " - 1)"
    addStanLine $ ri <> " = inverse(" <> r <> ")"
  inBlock SBTransformedParameters $ do
    addStanLine $ "vector[" <> cols <> "] beta" <> matrix <> suffix
    addStanLine $ "beta" <> matrix <> suffix <> " = " <> ri <> " * theta" <> matrix <> suffix


stanIndented :: StanBuilderM env a -> StanBuilderM env a
stanIndented = indented 2

inBlock :: StanBlock -> StanBuilderM env a -> StanBuilderM env a
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

printExprM :: Text -> Map Text Text -> VectorContext -> StanBuilderM env StanExpr -> StanBuilderM env Text
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

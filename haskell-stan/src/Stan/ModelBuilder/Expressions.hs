{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveFoldable #-}
{-# LANGUAGE DeriveTraversable #-}
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

module Stan.ModelBuilder.Expressions where


import Prelude hiding (All, group)
import qualified Control.Foldl as Fold
import qualified Data.Functor.Foldable as Rec
import qualified Data.Functor.Foldable.TH as Rec

import qualified Data.Fix as Fix
import qualified Data.Map.Strict as Map
import qualified Data.Text as T

type StanName = Text
data StanDim = NamedDim StanName | GivenDim Int deriving (Show, Eq, Ord)

dimToText :: StanDim -> Text
dimToText (NamedDim t) = t
dimToText (GivenDim n) = show n

data StanType = StanInt
              | StanReal
              | StanArray [StanDim] StanType
              | StanVector StanDim
              | StanMatrix (StanDim, StanDim) deriving (Show, Eq, Ord)

data StanVar = StanVar StanName StanType

data VectorContext = Vectorized | NonVectorized Text deriving (Show, Eq)

data StanExprF a where
  ScalarF :: Text -> StanExprF a
  IndexedF :: Text -> StanExprF a
  ReIndexedF :: Text -> Text -> StanExprF a
  BinOpF :: Text -> a -> a -> StanExprF a
  FunctionF :: Text -> [a] -> StanExprF a
  VectorFunctionF :: Text -> a -> StanExprF a -- function to add when the context is vectorized
  GroupF :: Text -> Text -> a -> StanExprF a
  ArgsF :: [a] -> StanExprF a
--  ArrayF :: [a] -> StanExprF a
  deriving (Show, Eq, Functor, Foldable, Traversable)


type StanExpr = Fix.Fix StanExprF

scalar :: Text -> StanExpr
scalar = Fix.Fix . ScalarF

indexed :: Text -> StanExpr
indexed = Fix.Fix . IndexedF

reIndexed :: Text -> Text -> StanExpr
reIndexed k t = Fix.Fix $ ReIndexedF k t

binOp :: Text -> StanExpr -> StanExpr -> StanExpr
binOp op e1 e2 = Fix.Fix $ BinOpF op e1 e2

function :: Text -> [StanExpr] -> StanExpr
function fName es = Fix.Fix $ FunctionF fName es

functionWithGivens :: Text -> [StanExpr] -> [StanExpr] -> StanExpr
functionWithGivens fName gs as = function fName [binOp "|" (args gs) (args as)]

vectorFunction :: Text -> StanExpr -> StanExpr
vectorFunction t = Fix.Fix . VectorFunctionF t

group ::  Text -> Text -> StanExpr -> StanExpr
group ld rd = Fix.Fix . GroupF ld rd

args :: [StanExpr] -> StanExpr
args = Fix.Fix . ArgsF

paren :: StanExpr -> StanExpr
paren = group "(" ")"

bracket :: StanExpr -> StanExpr
bracket = group "{" "}"

--args :: [StanExpr] -> StanExpr
--args = Fix.Fix . ArgsE . fmap Fix.unFix

eqE ::  StanExpr -> StanExpr -> StanExpr
eqE = binOp "="

vectorSampleE :: StanExpr -> StanExpr -> StanExpr
vectorSampleE = binOp "~"

plusE ::  StanExpr -> StanExpr -> StanExpr
plusE = binOp "+"

timesE :: StanExpr -> StanExpr -> StanExpr
timesE = binOp "*"

multiOp :: Text -> NonEmpty StanExpr -> StanExpr
multiOp o es = foldl' (binOp o) (head es) (tail es)

-- This is wild
reIndexExpression :: Map Text Text -> StanExpr -> Either Text StanExpr
reIndexExpression im = Rec.cata alg where
  reIndex :: Map Text Text -> Text -> Text -> Either Text Text
  reIndex im key term = case Map.lookup key im of
    Nothing -> Left $ "re-indexing key \"" <> key <> "\" not found in index-map: " <> show im
    Just i -> Right $ term <> "[" <> i <> "]"
  alg :: StanExprF (Either Text StanExpr) -> Either Text StanExpr
  alg (ReIndexedF k t) = case Map.lookup k im of
    Nothing -> Left $ "re-indexing key \"" <> k <> "\" not found in index-map: " <> show im
    Just i -> Right $ indexed $ t <> "[" <> i <> "]"
  alg x = Fix.Fix <$> sequenceA x

printReIndexedExpr :: VectorContext -> StanExpr -> Text
printReIndexedExpr vc = Rec.cata alg where
  printVC :: VectorContext -> Text -> Text
  printVC Vectorized t = t
  printVC (NonVectorized i) t = t <> "[" <> i <> "]"
  csArgs es = T.intercalate ", " es
  alg :: StanExprF Text -> Text
  alg (ScalarF t) = t
  alg (IndexedF t) = printVC vc t
  alg (ReIndexedF k t) = error "Should not call printExpr' before reIndexing" --printVC vc <$> reIndex im k t
  alg (BinOpF op e1 e2) = e1 <> " " <> op <> " " <> e2
  alg (FunctionF f es) = f <> "(" <> csArgs es <> ")"
  alg (VectorFunctionF f e) = if vc == Vectorized then f <> "(" <> e <> ")" else e
  alg (GroupF ld rd e) = ld <> e <> rd
  alg (ArgsF es) = csArgs es

printExpr :: Map Text Text -> VectorContext -> StanExpr -> Either Text Text
printExpr im vc expr = printReIndexedExpr vc <$> reIndexExpression im expr

{-
printExpr :: Map Text Text -> VectorContext -> StanExpr -> Either Text Text
--printExpr _ _ NullE = Just ""
printExpr im vc (TermE t) = printTerm im vc t
printExpr im vc (BinOpE o l r) = (\l' r' -> l' <> " " <> o <> " " <> r') <$> printExpr im vc l <*> printExpr im vc r
printExpr im vc (FunctionE f es) = (\es' -> f <> "(" <> T.intercalate ", " es' <> ")") <$> traverse (printExpr im vc) es
printExpr im Vectorized (VectorFunctionE f e) = (\e' -> f <> "(" <>  e' <> ")") <$> printExpr im Vectorized e
printExpr im vc@(NonVectorized _) (VectorFunctionE f e) = printExpr im vc e
printExpr im vc (GroupE ld rd e) = (\e' -> ld <> e' <> rd) <$> printExpr im vc e
printExpr im vc (ArgsE es) = T.intercalate ", " <$> traverse (printExpr im vc) es
-}

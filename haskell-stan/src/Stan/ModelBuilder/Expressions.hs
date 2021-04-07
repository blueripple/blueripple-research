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
import qualified Data.Functor.Foldable.Monadic as Rec
import qualified Data.Functor.Foldable.TH as Rec

import qualified Data.Fix as Fix
import qualified Data.Map.Strict as Map
import qualified Data.Text as T

type StanName = Text
type StanIndexKey = Text
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

data StanExprF a where
  BareF :: Text -> StanExprF a
  IndexedF :: StanIndexKey -> a -> StanExprF a
  WithIndexF :: a -> a -> StanExprF a
  BinOpF :: Text -> a -> a -> StanExprF a
  FunctionF :: Text -> [a] -> StanExprF a
  VectorFunctionF :: Text -> a -> StanExprF a -- function to add when the context is vectorized
  GroupF :: Text -> Text -> a -> StanExprF a
  ArgsF :: [a] -> StanExprF a
  deriving (Show, Eq, Functor, Foldable, Traversable)

type StanExpr = Fix.Fix StanExprF

scalar :: Text -> StanExpr
scalar = Fix.Fix . BareF

name :: Text -> StanExpr
name = Fix.Fix . BareF

indexed :: StanIndexKey -> StanExpr -> StanExpr
indexed k e = Fix.Fix $ IndexedF k e

withIndex :: StanExpr -> StanExpr -> StanExpr
withIndex e1 e2 = Fix.Fix $ WithIndexF e1 e2

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

eq ::  StanExpr -> StanExpr -> StanExpr
eq = binOp "="

vectorSample :: StanExpr -> StanExpr -> StanExpr
vectorSample = binOp "~"

plus ::  StanExpr -> StanExpr -> StanExpr
plus = binOp "+"

times :: StanExpr -> StanExpr -> StanExpr
times = binOp "*"

multiOp :: Text -> NonEmpty StanExpr -> StanExpr
multiOp o es = foldl' (binOp o) (head es) (tail es)

data BindIndex = NoIndex | IndexE StanExpr --deriving (Show)
data VarBindingStore = VarBindingStore (Maybe StanIndexKey) (Map StanIndexKey StanExpr)

vectorizingBindings :: StanIndexKey -> Map StanIndexKey StanExpr -> VarBindingStore
vectorizingBindings vecIndex = VarBindingStore (Just vecIndex)

fullyIndexedBindings :: Map StanIndexKey StanExpr -> VarBindingStore
fullyIndexedBindings = VarBindingStore Nothing

noBindings :: VarBindingStore
noBindings = fullyIndexedBindings mempty

lookupBinding :: StanIndexKey -> VarBindingStore -> Maybe BindIndex
lookupBinding k (VarBindingStore (Just vk) bm) =
  if k == vk then Just NoIndex else IndexE <$> Map.lookup k bm
lookupBinding k (VarBindingStore Nothing bm) = IndexE <$> Map.lookup k bm

showKeys :: VarBindingStore -> Text
showKeys (VarBindingStore mvk bms) =
  maybe ("Non-vectorized ") ("Vectorized over " <>) mvk
  <> "Substution keys: " <> show (Map.keys bms)

vectorized :: VarBindingStore -> Bool
vectorized (VarBindingStore mvk _) = isJust mvk

-- this is wild
-- bind all indexes and do so recursively, from the top.
-- So your indexes can have indexes.
-- then fold from the bottom into Stan code
printExpr :: VarBindingStore -> StanExpr -> Either Text Text
printExpr vbs = Rec.hyloM (printIndexedAlg $ vectorized vbs) (bindIndexCoAlg vbs) -- bindIndexes vbs expr >>= printIndexedExpr (vectorized vbs)

bindIndexCoAlg ::  VarBindingStore -> StanExpr -> Either Text (StanExprF StanExpr)
bindIndexCoAlg vbs (Fix.Fix (IndexedF k e)) =
  case lookupBinding k vbs of
    Nothing -> Left $ "re-indexing key \"" <> k <> "\" not found in var-index-map: " <> showKeys vbs
    Just bi -> case bi of
      NoIndex  -> Right $ Fix.unFix e
      IndexE ei -> Right $ WithIndexF ei e
bindIndexCoAlg _ x = Right $ Fix.unFix x

csArgs :: [Text] -> Text
csArgs = T.intercalate ", "

printIndexedAlg :: Bool -> StanExprF Text -> Either Text Text
printIndexedAlg _ (BareF t) = Right t
printIndexedAlg _ (IndexedF _ _) = Left "Should not call printExpr' before binding indexes" --printVC vc <$> reIndex im k t
printIndexedAlg _ (WithIndexF ie e) = Right $ e <> "[" <> ie <> "]"
printIndexedAlg _ (BinOpF op e1 e2) = Right $ e1 <> " " <> op <> " " <> e2
printIndexedAlg _ (FunctionF f es) = Right $ f <> "(" <> csArgs es <> ")"
printIndexedAlg vc (VectorFunctionF f e) = Right $ if vc then f <> "(" <> e <> ")" else e
printIndexedAlg _ (GroupF ld rd e) = Right $ ld <> e <> rd
printIndexedAlg _ (ArgsF es) = Right $ csArgs es


bindIndexes :: VarBindingStore -> StanExpr -> Either Text StanExpr
bindIndexes vbs = Rec.anaM (bindIndexCoAlg vbs)
 -- also wild
-- build up Stan code recursively from the bottom
-- should only be called after index binding
printIndexedExpr :: Bool -> StanExpr -> Either Text Text
printIndexedExpr vc = Rec.cataM (printIndexedAlg vc)

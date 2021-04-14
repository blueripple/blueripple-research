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
data StanDim = NamedDim StanIndexKey | GivenDim Int deriving (Show, Eq, Ord)

dimToText :: StanDim -> Text
dimToText (NamedDim t) = t
dimToText (GivenDim n) = show n

data StanType = StanInt
              | StanReal
              | StanArray [StanDim] StanType
              | StanVector StanDim
              | StanMatrix (StanDim, StanDim)
              | StanCorrMatrix StanDim
              | StanCholeskyFactorCorr StanDim
              | StanCovMatrix StanDim
              deriving (Show, Eq, Ord)

data StanVar = StanVar StanName StanType deriving (Show, Eq)

getDims :: StanType -> [StanDim]
getDims StanInt = []
getDims StanReal = []
getDims (StanArray dims st) = dims ++ getDims st
getDims (StanVector d) = [d]
getDims (StanMatrix (d1, d2)) = [d1, d2]
getDims (StanCorrMatrix d) = [d]
getDims (StanCholeskyFactorCorr d) = [d]
getDims (StanCovMatrix d) = [d]

{-
dimsAsIndexes :: [StanDim]

varToExpr :: StanVar -> StanExpr
varToExpr (StanVar sn t) = case t of
  StanInt -> name sn
  StanReal -> name sn
  StanArray dims
-}

--printDeclaration :: VarBindingStore -> StanVar -> Either Text Text
--printDeclaration (VarBindingStore _ _ dms) (StanVar sn st) = do

indexesToExpr :: Text -> [StanDim] -> StanExpr
indexesToExpr t dims = withIndexes (go id dims) where
  go ef [] = ef (name t)
  go ef (x : xs) = case x of
    NamedDim k -> go (ef . dIndexed k) xs
    GivenDim d -> go (ef . withIndexes [d])

--indexOne :: Text -> StanDim -> StanExpr
--indexOne t (NamedDim k) = dIndexed k (name t)
--indexOne t (GivenDim d) = withIndexes (name t) [name d]

declarationToExpr :: VarBindingStore -> StanVar -> Text -> Either Text (StanExprF StanExpr)
declarationToExpr vbs (StanVar sn st) = case st of
  StanInt -> Right $ SpacedF (name "int") (name sn)
  StanReal -> Right $ SpacedF (name "real") (name sn)
  StanVector dim -> Right $ SpacedF (indexesToExpr "vector" [dim]) (name sn)
  StanCorrMatrix dim -> Right $ SpacedF (indexesToExpr "corr_matrix" [dim]) (name sn)
  StanCholeskyFactorCorr dim -> Right $ SpacedF (indexesToExpr "cholesky_factor_corr" [dim]) (name sn)
  StanCovMatrix dim -> Right $ SpacedF (indexesToExpr "cov_matrix" [dim]) (name sn)
  StanMatrix (d1, d2) -> Right $ SpacedF (indexesToExpr "matrix" [d1, d2]) (name sn)
  StanArray dims st -> ??


data StanExprF a where
  BareF :: Text -> StanExprF a
  SpacedF :: a -> a -> StanExprF a
  DeclareF :: StanVar -> Text -> StanExprF a
  DIndexedF :: StanIndexKey -> StanExprF
  IndexedF :: StanIndexKey -> a -> StanExprF a
  WithIndexesF :: a -> [a] -> StanExprF a
  BinOpF :: Text -> a -> a -> StanExprF a
  FunctionF :: Text -> [a] -> StanExprF a
  VectorFunctionF :: Text -> a -> [a] -> StanExprF a -- function to add when the context is vectorized
  GroupF :: Text -> Text -> a -> StanExprF a
  ArgsF :: [a] -> StanExprF a
  deriving (Show, Eq, Functor, Foldable, Traversable)

type StanExpr = Fix.Fix StanExprF

scalar :: Text -> StanExpr
scalar = Fix.Fix . BareF

name :: Text -> StanExpr
name = Fix.Fix . BareF

dIndexed :: StanIndexKey -> StanExpr -> StanExpr
dIndexed k e = Fix.Fix $ DIndexedF k e


indexed :: StanIndexKey -> StanExpr -> StanExpr
indexed k e = Fix.Fix $ IndexedF k e

withIndexes :: StanExpr -> [StanExpr] -> StanExpr
withIndexes e eis = Fix.Fix $ WithIndexesF e eis

binOp :: Text -> StanExpr -> StanExpr -> StanExpr
binOp op e1 e2 = Fix.Fix $ BinOpF op e1 e2

function :: Text -> [StanExpr] -> StanExpr
function fName es = Fix.Fix $ FunctionF fName es

functionWithGivens :: Text -> [StanExpr] -> [StanExpr] -> StanExpr
functionWithGivens fName gs as = function fName [binOp "|" (args gs) (args as)]

vectorFunction :: Text -> StanExpr -> [StanExpr] -> StanExpr
vectorFunction t e es = Fix.Fix $ VectorFunctionF t e es

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
data VarBindingStore = VarBindingStore { vectorizedIndex :: !(Maybe StanIndexKey)
                                       , useBindings :: Map StanIndexKey StanExpr
                                       , declarationBindings :: Map StanIndexKey StanExpr
                                       }

vectorizingBindings :: StanIndexKey -> Map StanIndexKey StanExpr -> Map StanIndexKey StanExpr -> VarBindingStore
vectorizingBindings vecIndex = VarBindingStore (Just vecIndex)

fullyIndexedBindings :: Map StanIndexKey StanExpr -> Map StanIndexKey StanExpr -> VarBindingStore
fullyIndexedBindings = VarBindingStore Nothing

noBindings :: VarBindingStore
noBindings = fullyIndexedBindings mempty mempty

lookupUseBinding :: StanIndexKey -> VarBindingStore -> Maybe BindIndex
lookupUseBinding k (VarBindingStore (Just vk) bm _) =
  if k == vk then Just NoIndex else IndexE <$> Map.lookup k bm
lookupUseBinding k (VarBindingStore Nothing bm _) = IndexE <$> Map.lookup k bm

lookupDeclarationBinding :: StanIndexKey -> VarBindingStore -> Maybe BindIndex
lookupDeclarationBinding k (VarBindingStore (Just vk) _ dms) =
  if k == vk then Just NoIndex else IndexE <$> Map.lookup k dms
lookupDeclarationBinding k (VarBindingStore _ _ dm) = IndexE <$> Map.lookup k dm

showKeys :: VarBindingStore -> Text
showKeys (VarBindingStore mvk bms dms) =
  maybe ("Non-vectorized ") ("Vectorized over " <>) mvk
  <> "Substitution keys: " <> show (Map.keys bms)
  <> "Declaration keys: " <> show (Map.keys dms)

vectorized :: VarBindingStore -> Bool
vectorized (VarBindingStore mvk _ _) = isJust mvk


-- this is wild
-- bind all indexes and do so recursively, from the top.
-- So your indexes can have indexes.
-- then fold from the bottom into Stan code
printExpr :: VarBindingStore -> StanExpr -> Either Text Text
printExpr vbs = Rec.hyloM (printIndexedAlg $ vectorized vbs) (bindIndexCoAlg vbs)

bindIndexCoAlg ::  VarBindingStore -> StanExpr -> Either Text (StanExprF StanExpr)
bindIndexCoAlg vbs (Fix.Fix (IndexedF k e)) =
  case lookupUseBinding k vbs of
    Nothing -> Left $ "re-indexing key \"" <> k <> "\" not found in var-index-map: " <> showKeys vbs
    Just bi -> case bi of
      NoIndex  -> Right $ Fix.unFix e
      IndexE ei -> Right $ WithIndexesF e [ei]
bindIndexCoAlg vbs (Fix.Fix (DIndexedF k e)) =
  case lookupDeclarationBinding k vbs of
    Nothing -> Left $ "re-indexing key \"" <> k <> "\" not found in declaration-index-map: " <> showKeys vbs
    Just bi -> case bi of
      NoIndex  -> Right $ Fix.unFix e
      IndexE ei -> Right $ WithIndexesF e [ei]
bindIndexCoAlg vbs (Fix.Fix (DeclareF sv c)) = declarationToExpr vbs sv c
bindIndexCoAlg _ x = Right $ Fix.unFix x

csArgs :: [Text] -> Text
csArgs = T.intercalate ", "

printIndexedAlg :: VarBindingStore -> StanExprF Text -> Either Text Text
printIndexedAlg _ (BareF t) = Right t
printIndexedAlg _ (SpacedF l r) = Right $ l <> " " <> r
printIndexedAlg vbs (DeclareF sv) = Left "Should not call printExpr before expanding declarations" --printVC vc <$> reIndex im k t
printIndexedAlg _ (DIndexedF _ _) = Left "Should not call printExpr before binding declation indexes" --printVC vc <$> reIndex im k t
printIndexedAlg _ (IndexedF _ _) = Left "Should not call printExpr before binding indexes" --printVC vc <$> reIndex im k t
printIndexedAlg _ (WithIndexesF e ies) = Right $ e <> "[" <> csArgs ies <> "]"
printIndexedAlg _ (BinOpF op e1 e2) = Right $ e1 <> " " <> op <> " " <> e2
printIndexedAlg _ (FunctionF f es) = Right $ f <> "(" <> csArgs es <> ")"
printIndexedAlg vbs (VectorFunctionF f e argEs) = Right $ if vectorized vbs then f <> "(" <> csArgs (e:argEs) <> ")" else e
printIndexedAlg _ (GroupF ld rd e) = Right $ ld <> e <> rd
printIndexedAlg _ (ArgsF es) = Right $ csArgs es


bindIndexes :: VarBindingStore -> StanExpr -> Either Text StanExpr
bindIndexes vbs = Rec.anaM (bindIndexCoAlg vbs)
 -- also wild
-- build up Stan code recursively from the bottom
-- should only be called after index binding
printIndexedExpr :: Bool -> StanExpr -> Either Text Text
printIndexedExpr vc = Rec.cataM (printIndexedAlg vc)

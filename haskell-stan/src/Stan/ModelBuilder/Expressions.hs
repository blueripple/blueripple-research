{-# LANGUAGE DataKinds #-}
--{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveFoldable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveTraversable #-}
{-# LANGUAGE DerivingVia #-}
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
import qualified Data.Functor.Classes as FC
import qualified Data.Functor.Classes.Generic as FC
import qualified Data.Functor.Foldable as Rec
import qualified Data.Functor.Foldable.Monadic as Rec
import qualified Data.Functor.Foldable.TH as Rec

import qualified Data.Fix as Fix
import qualified Data.Map.Strict as Map
import qualified Data.Set as Set
import qualified Data.Text as T

import GHC.Generics (Generic1)

type StanName = Text
type StanIndexKey = Text
data StanDim = NamedDim StanIndexKey
             | GivenDim Int
             | ExprDim StanExpr
             deriving (Show, Eq, Ord, Generic)

dimToText :: StanDim -> Text
dimToText (NamedDim t) = t
dimToText (GivenDim n) = show n
dimToText (ExprDim _) = error "Can't call dimToText on ExprDim"

data StanType = StanInt
              | StanReal
              | StanArray [StanDim] StanType
              | StanVector StanDim
              | StanMatrix (StanDim, StanDim)
              | StanCorrMatrix StanDim
              | StanCholeskyFactorCorr StanDim
              | StanCovMatrix StanDim
              deriving (Show, Eq, Ord, Generic)

data TExpr = TExpr StanExpr StanType deriving (Show, Eq, Ord, Generic)

data StanVar = StanVar StanName StanType deriving (Show, Eq, Ord, Generic)

tExprFromStanVar :: StanVar -> TExpr
tExprFromStanVar (StanVar sn st) = TExpr (name sn) st

indexTExpr :: TExpr -> StanExpr -> Either Text TExpr
indexTExpr (TExpr e (StanArray (x : xs) st)) ie =
  let st' = case xs of
        [] -> st
        ds -> StanArray ds st
  in Right $ TExpr (withIndexes e [ie]) st'
indexTExpr (TExpr e (StanVector d)) ie = Right $ TExpr (withIndexes e [ie]) StanReal
indexTExpr (TExpr e (StanMatrix (dr, dc))) ie = Right $ TExpr (withIndexes e [ie]) (StanVector dc)
indexTExpr te _ = Left $ "Attempt to index non-indexable TExpr: " <> show te

getDims :: StanType -> [StanDim]
getDims StanInt = []
getDims StanReal = []
getDims (StanArray dims st) = dims ++ getDims st
getDims (StanVector d) = [d]
getDims (StanMatrix (d1, d2)) = [d1, d2]
getDims (StanCorrMatrix d) = [d]
getDims (StanCholeskyFactorCorr d) = [d]
getDims (StanCovMatrix d) = [d]

indexKeys :: StanType -> [StanIndexKey]
indexKeys = catMaybes . fmap dimToIndexKey . getDims where
  dimToIndexKey (NamedDim k) = Just k
  dimToindexKey _ = Nothing

indexesToDExpr :: Text -> [StanDim] -> StanExpr
indexesToDExpr t = go id where
  go ef [] = ef (name t)
  go ef (x : xs) = case x of
    NamedDim k -> go (ef . dIndexed k) xs
    GivenDim d -> go (ef . flip withIndexes [name $ show d]) xs
    ExprDim e -> go (ef . flip withIndexes [e]) xs

indexesToUExpr :: StanExpr -> [StanDim] -> StanExpr
indexesToUExpr ne  = go id where
  go ef [] = ef ne
  go ef (x : xs) = case x of
    NamedDim k -> go (ef . uIndexed k) xs
    GivenDim d -> go (ef . flip withIndexes [name $ show d]) xs
    ExprDim e -> go (ef . flip withIndexes [e]) xs

collapseArray :: StanType -> StanType
collapseArray (StanArray dims st) = case st of
  StanArray dims' st -> collapseArray $ StanArray (dims ++ dims') st
  x -> StanArray dims st
collapseArray st = st

declarationPart :: StanType -> Text -> StanExpr
declarationPart  st c = case st of
  StanInt -> name $ "int" <> c
  StanReal -> name $ "real" <> c
  StanVector dim -> indexesToDExpr ("vector" <> c) [dim]
  StanCorrMatrix dim -> indexesToDExpr "corr_matrix" [dim]
  StanCholeskyFactorCorr dim -> indexesToDExpr "cholesky_factor_corr" [dim]
  StanCovMatrix dim -> indexesToDExpr "cov_matrix" [dim]
  StanMatrix (d1, d2) -> indexesToDExpr ("matrix" <> c) [d1, d2]
  StanArray dims st -> declarationPart st c

declarationExpr :: StanVar -> Text -> StanExpr
declarationExpr (StanVar sn st) c = case st of
  sa@(StanArray _ _) -> let (StanArray dims st) = collapseArray sa
                        in spaced (declarationPart st c) (indexesToDExpr sn dims)
  _ -> spaced (declarationPart st c) (name sn)

tExprToExpr :: TExpr -> StanExpr
tExprToExpr (TExpr ne st) = indexesToUExpr ne dims where
  dims = getDims st

data StanExprF a where
  BareF :: Text -> StanExprF a
  SpacedF :: a -> a -> StanExprF a
  DeclareF :: StanVar -> Text -> StanExprF a
  UseTExprF :: TExpr -> StanExprF a
  DIndexedF :: StanIndexKey -> a -> StanExprF a
  UIndexedF :: StanIndexKey -> a -> StanExprF a
  WithIndexesF :: a -> [a] -> StanExprF a
  BinOpF :: Text -> a -> a -> StanExprF a
  FunctionF :: Text -> [a] -> StanExprF a
  VectorFunctionF :: Text -> a -> [a] -> StanExprF a -- function to add when the context is vectorized
  GroupF :: Text -> Text -> a -> StanExprF a
  ArgsF :: [a] -> StanExprF a
  deriving  stock (Eq, Ord, Functor, Foldable, Traversable, Generic1)
  deriving   (FC.Show1, FC.Eq1, FC.Ord1) via FC.FunctorClassesDefault StanExprF

type StanExpr = Fix.Fix StanExprF

scalar :: Text -> StanExpr
scalar = Fix.Fix . BareF

name :: Text -> StanExpr
name = Fix.Fix . BareF

spaced :: StanExpr -> StanExpr -> StanExpr
spaced le re = Fix.Fix $ SpacedF le re

declareVar :: StanVar -> Text -> StanExpr
declareVar sv c = Fix.Fix $ DeclareF sv c

useVar :: StanVar -> StanExpr
useVar = useTExpr . tExprFromStanVar

useTExpr :: TExpr -> StanExpr
useTExpr = Fix.Fix . UseTExprF

dIndexed :: StanIndexKey -> StanExpr -> StanExpr
dIndexed k e = Fix.Fix $ DIndexedF k e

uIndexed :: StanIndexKey -> StanExpr -> StanExpr
uIndexed k e = Fix.Fix $ UIndexedF k e

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

minus ::  StanExpr -> StanExpr -> StanExpr
minus = binOp "-"

times :: StanExpr -> StanExpr -> StanExpr
times = binOp "*"

multiOp :: Text -> NonEmpty StanExpr -> StanExpr
multiOp o es = foldl' (binOp o) (head es) (tail es)

data BindIndex = NoIndex | IndexE StanExpr --deriving (Show)
data VarBindingStore = VarBindingStore { vectorizedIndexes :: !(Set StanIndexKey)
                                       , useBindings :: Map StanIndexKey StanExpr
                                       , declarationBindings :: Map StanIndexKey StanExpr
                                       }

vectorizingBindings :: StanIndexKey -> Map StanIndexKey StanExpr -> Map StanIndexKey StanExpr -> VarBindingStore
vectorizingBindings vecIndex = VarBindingStore (one vecIndex)

fullyIndexedBindings :: Map StanIndexKey StanExpr -> Map StanIndexKey StanExpr -> VarBindingStore
fullyIndexedBindings = VarBindingStore mempty

noBindings :: VarBindingStore
noBindings = fullyIndexedBindings mempty mempty

lookupUseBinding :: StanIndexKey -> VarBindingStore -> Maybe BindIndex
lookupUseBinding k (VarBindingStore vb bm _) =
  if Set.member k vb then Just NoIndex else IndexE <$> Map.lookup k bm

lookupDeclarationBinding :: StanIndexKey -> VarBindingStore -> Maybe BindIndex
lookupDeclarationBinding k (VarBindingStore vb _ dms) =
  if Set.member k vb then Just NoIndex else IndexE <$> Map.lookup k dms

showKeys :: VarBindingStore -> Text
showKeys (VarBindingStore vb bms dms) =
  (if Set.null vb then "Non-vectorized " else  "Vectorized over " <> show vb)
  <> "Substitution keys: " <> show (Map.keys bms)
  <> "Declaration keys: " <> show (Map.keys dms)

vectorized :: VarBindingStore -> Bool
vectorized (VarBindingStore vb _ _) = not $ Set.null vb


-- this is wild
-- bind all indexes and do so recursively, from the top.
-- So your indexes can have indexes.
-- then fold from the bottom into Stan code
printExpr :: VarBindingStore -> StanExpr -> Either Text Text
printExpr vbs = Rec.hyloM (printIndexedAlg $ vectorized vbs) (bindIndexCoAlg vbs)

bindIndexCoAlg ::  VarBindingStore -> StanExpr -> Either Text (StanExprF StanExpr)
bindIndexCoAlg vbs (Fix.Fix (UIndexedF k e)) =
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
bindIndexCoAlg vbs (Fix.Fix (DeclareF sv c)) = Right $ Fix.unFix $ declarationExpr sv c
bindIndexCoAlg vbs (Fix.Fix (UseTExprF te)) = Right $ Fix.unFix $ tExprToExpr te
bindIndexCoAlg _ x = Right $ Fix.unFix x

csArgs :: [Text] -> Text
csArgs = T.intercalate ", "

printIndexedAlg :: Bool -> StanExprF Text -> Either Text Text
printIndexedAlg _ (BareF t) = Right t
printIndexedAlg _ (SpacedF l r) = Right $ l <> " " <> r
printIndexedAlg _ (DeclareF _ _) = Left "Should not call printExpr before expanding declarations"
printIndexedAlg _ (UseTExprF _) = Left "Should not call printExpr before expanding var uses"
printIndexedAlg _ (DIndexedF _ _) = Left "Should not call printExpr before binding declaration indexes"
printIndexedAlg _ (UIndexedF _ _) = Left "Should not call printExpr before binding use indexes"
printIndexedAlg _ (WithIndexesF e ies) = Right $ e <> "[" <> csArgs ies <> "]"
printIndexedAlg _ (BinOpF op e1 e2) = Right $ e1 <> " " <> op <> " " <> e2
printIndexedAlg _ (FunctionF f es) = Right $ f <> "(" <> csArgs es <> ")"
printIndexedAlg vc (VectorFunctionF f e argEs) = Right $ if vc then f <> "(" <> csArgs (e:argEs) <> ")" else e
printIndexedAlg _ (GroupF ld rd e) = Right $ ld <> e <> rd
printIndexedAlg _ (ArgsF es) = Right $ csArgs es


bindIndexes :: VarBindingStore -> StanExpr -> Either Text StanExpr
bindIndexes vbs = Rec.anaM (bindIndexCoAlg vbs)
 -- also wild
-- build up Stan code recursively from the bottom
-- should only be called after index binding
printIndexedExpr :: Bool -> StanExpr -> Either Text Text
printIndexedExpr vc = Rec.cataM (printIndexedAlg vc)

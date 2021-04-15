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

{-
indexTExpr :: TExpr -> StanExpr -> Either Text TExpr
indexTExpr (TExpr e (StanArray (x : xs) st)) ie =
  let st' = case xs of
        [] -> st
        ds -> StanArray ds st
  in Right $ TExpr (withIndexes e [ie]) st'
indexTExpr (TExpr e (StanVector d)) ie = Right $ TExpr (withIndexes e [ie]) StanReal
indexTExpr (TExpr e (StanMatrix (dr, dc))) ie = Right $ TExpr (withIndexes e [ie]) (StanVector dc)
indexTExpr te _ = Left $ "Attempt to index non-indexable TExpr: " <> show te
-}

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

indexesToDExprs :: [StanDim] -> [(Maybe StanIndexKey, StanExpr)]
indexesToDExprs = fmap go  where
  go (NamedDim k) = (Just k, dIndex k)
  go (GivenDim n) = (Nothing, name $ show n)
  go (ExprDim e) = (Nothing, e)

indexesToUExprs :: [StanDim] -> [(Maybe StanIndexKey, StanExpr)]
indexesToUExprs = fmap go  where
  go (NamedDim k) = (Just k, uIndex k)
  go (GivenDim n) = (Nothing, name $ show n)
  go (ExprDim e) = (Nothing, e)


collapseArray :: StanType -> StanType
collapseArray (StanArray dims st) = case st of
  StanArray dims' st -> collapseArray $ StanArray (dims ++ dims') st
  x -> StanArray dims st
collapseArray st = st

declarationPart :: StanType -> Text -> StanExpr
declarationPart  st c = case st of
  StanInt -> name $ "int" <> c
  StanReal -> name $ "real" <> c
  StanVector dim -> withIndexes (name $ "vector" <> c) $ indexesToDExprs [dim]
  StanCorrMatrix dim -> withIndexes (name "corr_matrix") $ indexesToDExprs [dim]
  StanCholeskyFactorCorr dim -> withIndexes (name "cholesky_factor_corr") $ indexesToDExprs [dim]
  StanCovMatrix dim -> withIndexes (name "cov_matrix") $ indexesToDExprs  [dim]
  StanMatrix (d1, d2) -> withIndexes (name $ "matrix" <> c) $ indexesToDExprs [d1, d2]
  StanArray dims st -> declarationPart st c

declarationExpr :: StanVar -> Text -> StanExpr
declarationExpr (StanVar sn st) c = case st of
  sa@(StanArray _ _) -> let (StanArray dims st) = collapseArray sa
                        in spaced (declarationPart st c) (withIndexes (name sn) $ indexesToDExprs dims)
  _ -> spaced (declarationPart st c) (name sn)

tExprToExpr :: TExpr -> StanExpr
tExprToExpr (TExpr ne st) = withIndexes ne $ indexesToUExprs dims where
  dims = getDims st

--data IndexedExpr = Keyed StanIndex

data StanExprF a where
  NullF :: StanExprF a
  BareF :: Text -> StanExprF a
  NextToF :: a -> a -> StanExprF a
  DeclareF :: StanVar -> Text -> StanExprF a
  UseTExprF :: TExpr -> StanExprF a
  DIndexF :: StanIndexKey -> StanExprF a -- pre-lookup
  UIndexF :: StanIndexKey -> StanExprF a -- pre-lookup
--  IndexedF :: StanIndexKey -> a -> StanExprF a -- post-lookup
  VectorizedF :: Set StanIndexKey -> a -> StanExprF a
  IndexesF :: [(Maybe StanIndexKey, a)] -> StanExprF a
--  NextToF :: Text -> a -> a -> StanExprF a
--  FunctionF :: Text -> [a] -> StanExprF a
  VectorFunctionF :: Text -> a -> [a] -> StanExprF a -- function to add when the context is vectorized
--  GroupF :: Text -> Text -> a -> StanExprF a
--  ArgsF :: [a] -> StanExprF a
  deriving  stock (Eq, Ord, Functor, Foldable, Traversable, Generic1)
  deriving   (FC.Show1, FC.Eq1, FC.Ord1) via FC.FunctorClassesDefault StanExprF

type StanExpr = Fix.Fix StanExprF

nullE :: StanExpr
nullE = Fix.Fix NullF

bare :: Text -> StanExpr
bare = Fix.Fix . BareF

scalar :: Text -> StanExpr
scalar = Fix.Fix . BareF

name :: Text -> StanExpr
name = Fix.Fix . BareF

nextTo :: StanExpr -> StanExpr -> StanExpr
nextTo le re = Fix.Fix $ NextToF le re

spaced :: StanExpr -> StanExpr -> StanExpr
spaced le re = le `nextTo` bare " " `nextTo` re

declareVar :: StanVar -> Text -> StanExpr
declareVar sv c = Fix.Fix $ DeclareF sv c

useVar :: StanVar -> StanExpr
useVar = useTExpr . tExprFromStanVar

useTExpr :: TExpr -> StanExpr
useTExpr = Fix.Fix . UseTExprF

dIndex :: StanIndexKey -> StanExpr
dIndex k = Fix.Fix $ DIndexF k

uIndex :: StanIndexKey -> StanExpr
uIndex k = Fix.Fix $ UIndexF k

vectorized :: Set StanIndexKey -> StanExpr -> StanExpr
vectorized ks = Fix.Fix . VectorizedF ks

vectorizedOne :: StanIndexKey -> StanExpr -> StanExpr
vectorizedOne k = vectorized (one k)

uIndexBy :: StanExpr -> StanIndexKey -> StanExpr
uIndexBy e k = withIndexes e [(Just k, uIndex k)]

withIndexes :: StanExpr -> [(Maybe StanIndexKey, StanExpr)] -> StanExpr
withIndexes e eis = e `nextTo` indexes eis --``Fix.Fix $ WithIndexesF e eis

indexes :: [(Maybe StanIndexKey, StanExpr)] -> StanExpr
indexes = Fix.Fix . IndexesF --group "[" "]" . args

binOp :: Text -> StanExpr -> StanExpr -> StanExpr
binOp op e1 e2 = e1 `spaced` bare op `spaced` e2 --``Fix.Fix $ BinOpF op e1 e2

sep :: Text -> StanExpr -> StanExpr -> StanExpr
sep t le re = le `nextTo` bare t `nextTo` re

csExprs :: NonEmpty StanExpr -> StanExpr
csExprs es = foldl' (sep ",") (head es) (tail es)

function :: Text -> NonEmpty StanExpr -> StanExpr
function fName es = name fName `nextTo` (paren $ csExprs es)  --  Fix.Fix $ FunctionF fName es

functionWithGivens :: Text -> NonEmpty StanExpr -> NonEmpty StanExpr -> StanExpr
functionWithGivens fName gs as = name fName `nextTo` (paren $ binOp "|" (csExprs gs) (csExprs as))

vectorFunction :: Text -> StanExpr -> [StanExpr] -> StanExpr
vectorFunction t e es = Fix.Fix $ VectorFunctionF t e es

group ::  Text -> Text -> StanExpr -> StanExpr
group ld rd e = bare ld `nextTo` e `nextTo` bare rd --Fix.Fix . GroupF ld rd

--args :: [StanExpr] -> StanExpr
--args = Fix.Fix . ArgsF

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

--data BindIndex = NoIndex | IndexE StanExpr --deriving (Show)
data VarBindingStore = VarBindingStore { useBindings :: Map StanIndexKey StanExpr
                                       , declarationBindings :: Map StanIndexKey StanExpr
                                       }

--vectorizingBindings :: StanIndexKey -> Map StanIndexKey StanExpr -> Map StanIndexKey StanExpr -> VarBindingStore
--vectorizingBindings vecIndex = VarBindingStore (one vecIndex)

bindings :: Map StanIndexKey StanExpr -> Map StanIndexKey StanExpr -> VarBindingStore
bindings = VarBindingStore

noBindings :: VarBindingStore
noBindings = bindings mempty mempty

lookupUseBinding :: StanIndexKey -> VarBindingStore -> Maybe StanExpr
lookupUseBinding k (VarBindingStore bm _) = Map.lookup k bm

lookupDeclarationBinding :: StanIndexKey -> VarBindingStore -> Maybe StanExpr
lookupDeclarationBinding k (VarBindingStore _ dms) = Map.lookup k dms

showKeys :: VarBindingStore -> Text
showKeys (VarBindingStore bms dms) =
  "Use keys: " <> show (Map.keys bms)
  <> "; Declaration keys: " <> show (Map.keys dms)

-- this is wild
-- bind all indexes and do so recursively, from the top.
-- So your indexes can have indexes.
-- then fold from the bottom into Stan code
printExpr :: VarBindingStore -> StanExpr -> Either Text Text
printExpr vbs e = do
  unfolded <- usingCoalgM Set.empty $ Rec.anaM @_ @StanExpr (bindIndexCoAlg vbs) e
  Rec.cataM printIndexedAlg unfolded

--  Rec.hyloM (printIndexedAlg $ vectorized vbs) (bindIndexCoAlg vbs)

keepIndex :: Set StanIndexKey -> Maybe StanIndexKey -> Bool
keepIndex _ Nothing = True
keepIndex vks (Just k) = not $ Set.member k vks

newtype CoalgM a = CoalgM { unCoalgM :: StateT (Set StanIndexKey) (Either Text) a}
  deriving (Functor, Applicative, Monad, MonadState (Set StanIndexKey))

usingCoalgM :: Set StanIndexKey -> CoalgM a -> Either Text a
usingCoalgM vks x = evalStateT (unCoalgM x) vks

coalgLeft :: Text -> CoalgM a
coalgLeft = CoalgM . lift . Left

bindIndexCoAlg ::  VarBindingStore -> StanExpr -> CoalgM (StanExprF StanExpr)
bindIndexCoAlg vbs (Fix.Fix (UIndexF k)) =
  case lookupUseBinding k vbs of
    Nothing -> coalgLeft $ "re-indexing key \"" <> k <> "\" not found in var-index-map: " <> showKeys vbs
    Just ie -> return $ Fix.unFix ie
bindIndexCoAlg vbs (Fix.Fix (DIndexF k)) =
  case lookupDeclarationBinding k vbs of
    Nothing -> coalgLeft $ "re-indexing key \"" <> k <> "\" not found in declaration-index-map: " <> showKeys vbs
    Just ie -> return $ Fix.unFix ie
bindIndexCoAlg _ (Fix.Fix (DeclareF sv c)) = return $ Fix.unFix $ declarationExpr sv c
bindIndexCoAlg _ (Fix.Fix (UseTExprF te)) = return $ Fix.unFix $ tExprToExpr te
bindIndexCoAlg _ (Fix.Fix (VectorizedF vks e)) = do
  modify $ (<> vks)
  return $ Fix.unFix e
bindIndexCoAlg _ (Fix.Fix (VectorFunctionF f e es)) = do
  vks <- get
  return $ Fix.unFix $ if Set.null vks then e else function f (e :| es)
bindIndexCoAlg _ (Fix.Fix (IndexesF xs)) = do
  vks <- get
  let ies = snd <$> filter (keepIndex vks . fst) xs
  case nonEmpty ies of
    Nothing -> return NullF
    Just x -> return $ Fix.unFix $ group "[" "]" $ csExprs x
bindIndexCoAlg _ x = return $ Fix.unFix x


csArgs :: [Text] -> Text
csArgs = T.intercalate ", "

printIndexedAlg :: StanExprF Text -> Either Text Text
printIndexedAlg NullF = Right ""
printIndexedAlg (BareF t) = Right t
printIndexedAlg (NextToF l r) = Right $ l <> r
printIndexedAlg (DeclareF _ _) = Left "Should not call printExpr before expanding declarations"
printIndexedAlg (UseTExprF _) = Left "Should not call printExpr before expanding var uses"
printIndexedAlg (DIndexF _) = Left "Should not call printExpr before binding declaration indexes"
printIndexedAlg (UIndexF _) = Left "Should not call printExpr before binding use indexes"
printIndexedAlg (VectorizedF _ _) = Left "Should not call printExpr before resolving vectorization use"
printIndexedAlg (IndexesF _) = Left "Should not call printExpr before resolving indexes use"
--printIndexedAlg _ (WithIndexesF e ies) = Right $ e <> "[" <> csArgs ies <> "]"
--printIndexedAlg _ (BinOpF op e1 e2) = Right $ e1 <> " " <> op <> " " <> e2
--printIndexedAlg _ (FunctionF f es) = Right $ f <> "(" <> csArgs es <> ")"
printIndexedAlg (VectorFunctionF f e argEs) = Left "Should not call printExpr before resolving vectorizatio nuse"
--printIndexedAlg _ (GroupF ld rd e) = Right $ ld <> e <> rd
--printIndexedAlg _ (ArgsF es) = Right $ csArgs es

{-
bindIndexes :: VarBindingStore -> StanExpr -> Either Text StanExpr
bindIndexes vbs = Rec.anaM (bindIndexCoAlg vbs)
 -- also wild
-- build up Stan code recursively from the bottom
-- should only be called after index binding
printIndexedExpr :: Bool -> StanExpr -> Either Text Text
printIndexedExpr vc = Rec.cataM (printIndexedAlg vc)
-}


{-
bindIndexCoAlg' ::  VarBindingStore -> StanExpr -> Either Text (StanExprF StanExpr)
bindIndexCoAlg' vbs (Fix.Fix (UIndexF k)) =
  case lookupUseBinding k vbs of
    Nothing -> Left $ "re-indexing key \"" <> k <> "\" not found in var-index-map: " <> showKeys vbs
    Just ie -> Right $ IndexedF k ie
bindIndexCoAlg' vbs (Fix.Fix (DIndexF k)) =
  case lookupDeclarationBinding k vbs of
    Nothing -> Left $ "re-indexing key \"" <> k <> "\" not found in declaration-index-map: " <> showKeys vbs
    Just ie -> Right $ IndexedF k ie
bindIndexCoAlg' vbs (Fix.Fix (DeclareF sv c)) = Right $ Fix.unFix $ declarationExpr sv c
bindIndexCoAlg' vbs (Fix.Fix (UseTExprF te)) = Right $ Fix.unFix $ tExprToExpr te
bindIndexCoAlg' _ x = Right $ Fix.unFix x
-}

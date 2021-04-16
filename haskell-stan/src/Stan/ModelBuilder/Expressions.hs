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

stanDimToExpr :: StanDim -> StanExpr
stanDimToExpr (NamedDim k) = index k
stanDimToExpr (GivenDim n) = name $ show n
stanDimToExpr (ExprDim e) = e

indexesToExprs :: [StanDim] -> [StanExpr]
indexesToExprs = fmap stanDimToExpr

{-
indexesToUExprs :: [StanDim] -> [(Maybe StanIndexKey, StanExpr)]
indexesToUExprs = fmap go  where
  go (NamedDim k) = (Just k, uIndex k)
  go (GivenDim n) = (Nothing, name $ show n)
  go (ExprDim e) = (Nothing, e)
-}

collapseArray :: StanType -> StanType
collapseArray (StanArray dims st) = case st of
  StanArray dims' st -> collapseArray $ StanArray (dims ++ dims') st
  x -> StanArray dims st
collapseArray st = st

declarationPart :: StanType -> Text -> StanExpr
declarationPart  st c = case st of
  StanInt -> name $ "int" <> c
  StanReal -> name $ "real" <> c
  StanVector dim -> withIndexes (name $ "vector" <> c) [dim]
  StanCorrMatrix dim -> withIndexes (name "corr_matrix") [dim]
  StanCholeskyFactorCorr dim -> withIndexes (name "cholesky_factor_corr") [dim]
  StanCovMatrix dim -> withIndexes (name "cov_matrix") [dim]
  StanMatrix (d1, d2) -> withIndexes (name $ "matrix" <> c) [d1, d2]
  StanArray dims st -> declarationPart st c

declarationExpr :: StanVar -> Text -> StanExpr
declarationExpr (StanVar sn st) c = case st of
  sa@(StanArray _ _) -> let (StanArray dims st) = collapseArray sa
                        in spaced (declarationPart st c) (withIndexes (name sn) dims)
  _ -> spaced (declarationPart st c) (name sn)

tExprToExpr :: TExpr -> StanExpr
tExprToExpr (TExpr ne st) = withIndexes ne dims where
  dims = getDims st

data StanExprF a where
  NullF :: StanExprF a
  BareF :: Text -> StanExprF a
  NextToF :: a -> a -> StanExprF a
  DeclCtxtF :: a -> StanExprF a
  UseTExprF :: TExpr -> StanExprF a
  IndexF :: StanIndexKey -> StanExprF a -- pre-lookup
  VectorizedF :: Set StanIndexKey -> a -> StanExprF a
  IndexesF :: [StanDim] -> StanExprF a
  VectorFunctionF :: Text -> a -> [a] -> StanExprF a -- function to add when the context is vectorized
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

declaration :: StanExpr -> StanExpr
declaration = Fix.Fix . DeclCtxtF

declareVar :: StanVar -> Text -> StanExpr
declareVar sv c = Fix.Fix $ DeclCtxtF $ declarationExpr sv c

useVar :: StanVar -> StanExpr
useVar = useTExpr . tExprFromStanVar

useTExpr :: TExpr -> StanExpr
useTExpr = Fix.Fix . UseTExprF

index :: StanIndexKey -> StanExpr
index k = Fix.Fix $ IndexF k

vectorized :: Set StanIndexKey -> StanExpr -> StanExpr
vectorized ks = Fix.Fix . VectorizedF ks

vectorizedOne :: StanIndexKey -> StanExpr -> StanExpr
vectorizedOne k = vectorized (one k)

indexBy :: StanExpr -> StanIndexKey -> StanExpr
indexBy e k = withIndexes e [NamedDim k]

withIndexes :: StanExpr -> [StanDim] -> StanExpr
withIndexes e eis = e `nextTo` indexes eis --``Fix.Fix $ WithIndexesF e eis

indexes :: [StanDim] -> StanExpr
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

divide :: StanExpr -> StanExpr -> StanExpr
divide = binOp "/"

multiOp :: Text -> NonEmpty StanExpr -> StanExpr
multiOp o es = foldl' (binOp o) (head es) (tail es)

--data BindIndex = NoIndex | IndexE StanExpr --deriving (Show)
data VarBindingStore = VarBindingStore { useBindings :: Map StanIndexKey StanExpr
                                       , declarationBindings :: Map StanIndexKey StanExpr
                                       }

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
  case usingCoalgM $ Rec.anaM @_ @StanExpr (bindIndexCoAlg vbs) e of
    Left err -> Left $ err <> "\nTree:\n" <> show e
    Right ue -> Rec.cataM printIndexedAlg ue

--  Rec.hyloM (printIndexedAlg $ vectorized vbs) (bindIndexCoAlg vbs)

keepIndex :: Set StanIndexKey -> StanExpr -> Bool
keepIndex vks (Fix.Fix (IndexF k)) = not $ Set.member k vks
keepIndex _ _ = True

data LookupContext = Use | Declare

data AnaS = AnaS { lContext :: LookupContext, vectorizedIndexes :: Set StanIndexKey}

setLookupContext :: LookupContext -> AnaS -> AnaS
setLookupContext lc (AnaS _ x) = AnaS lc x

addVectorizedIndexes :: Set StanIndexKey -> AnaS -> AnaS
addVectorizedIndexes vks (AnaS lc vks') = AnaS lc (Set.union vks vks')

newtype CoalgM a = CoalgM { unCoalgM :: StateT AnaS (Either Text) a}
  deriving (Functor, Applicative, Monad, MonadState AnaS)

usingCoalgM :: CoalgM a -> Either Text a
usingCoalgM x = evalStateT (unCoalgM x) (AnaS Use Set.empty)

coalgLeft :: Text -> CoalgM a
coalgLeft = CoalgM . lift . Left

bindIndexCoAlg ::  VarBindingStore -> StanExpr -> CoalgM (StanExprF StanExpr)
bindIndexCoAlg vbs (Fix.Fix (IndexF k)) = do
  lc <- lContext <$> get
  case lc of
    Use -> case lookupUseBinding k vbs of
      Nothing -> coalgLeft $ "re-indexing key \"" <> k <> "\" not found in var-index-map: " <> showKeys vbs
      Just ie -> return $ Fix.unFix ie
    Declare ->   case lookupDeclarationBinding k vbs of
      Nothing -> coalgLeft $ "re-indexing key \"" <> k <> "\" not found in declaration-index-map: " <> showKeys vbs
      Just ie -> return $ Fix.unFix ie
bindIndexCoAlg _ (Fix.Fix (DeclCtxtF e)) = do
  modify $ setLookupContext Declare
  return $ Fix.unFix $ e --declarationExpr sv c
bindIndexCoAlg _ (Fix.Fix (UseTExprF te)) = return $ Fix.unFix $ tExprToExpr te
bindIndexCoAlg _ (Fix.Fix (VectorizedF vks e)) = do
  modify $ addVectorizedIndexes vks
  return $ Fix.unFix e
bindIndexCoAlg _ (Fix.Fix (VectorFunctionF f e es)) = do
  vks <- vectorizedIndexes <$> get
  return $ Fix.unFix $ if Set.null vks then e else function f (e :| es)
bindIndexCoAlg _ (Fix.Fix (IndexesF xs)) = do
  vks <- vectorizedIndexes <$> get
  let ies = filter (keepIndex vks) $ indexesToExprs xs
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
printIndexedAlg (DeclCtxtF _) = Left "Should not call printExpr before expanding declarations"
printIndexedAlg (UseTExprF _) = Left "Should not call printExpr before expanding var uses"
printIndexedAlg (IndexF _) = Left "Should not call printExpr before binding declaration indexes"
printIndexedAlg (VectorizedF _ _) = Left "Should not call printExpr before resolving vectorization use"
printIndexedAlg (IndexesF _) = Left "Should not call printExpr before resolving indexes use"
printIndexedAlg (VectorFunctionF f e argEs) = Left "Should not call printExpr before resolving vectorizatio nuse"

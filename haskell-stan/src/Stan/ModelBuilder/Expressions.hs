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
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}

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

import qualified Text.PrettyPrint as Pretty

import GHC.Generics (Generic1)
import Data.ByteString.Lazy (foldlChunks)
import Knit.Report (perspectiveX1)

type StanName = Text
type IndexKey = Text
type DataSetKey = Text
data StanDim = NamedDim IndexKey
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

data TExpr a = TExpr a StanType
  deriving  stock (Eq, Ord, Functor, Foldable, Traversable, Generic1)
  deriving   (FC.Show1, FC.Eq1, FC.Ord1) via FC.FunctorClassesDefault TExpr

data StanVar = StanVar StanName StanType deriving (Show, Eq, Ord, Generic)

varName :: StanVar -> StanName
varName (StanVar n _) = n

varType :: StanVar -> StanType
varType (StanVar _ t) = t


-- for matrix multiplication
dropLastIndex :: StanType -> Either Text StanType
dropLastIndex StanInt = Left "Can't drop outer index from an Int"
dropLastIndex StanReal = Left "Can't drop outer index from a Real"
dropLastIndex (StanArray ds t) = case dropLastIndex t of
  Right t' -> Right $ StanArray ds t'
  Left _ -> case ds of
    [] -> Left $ "Can't drop last index of a zero-dimensional array of scalars"
    _ -> Right $ StanArray (reverse $ drop 1 $ reverse ds) t
dropLastIndex (StanVector _) = Right StanReal
dropLastIndex (StanMatrix (rd, _)) = Right $ StanVector rd
dropLastIndex (StanCorrMatrix d) = Right $ StanVector d
dropLastIndex (StanCholeskyFactorCorr d) = Right $ StanVector d
dropLastIndex (StanCovMatrix d) = Right $ StanVector d

dropFirstIndex :: StanType -> Either Text StanType
dropFirstIndex StanInt = Left "Can't drop inner index from an Int"
dropFirstIndex StanReal = Left "Can't drop inner index from a Real"
dropFirstIndex (StanArray [] t) = dropFirstIndex t
dropFirstIndex (StanArray (x:xs) t) = Right $ if null xs then t else StanArray xs t
dropFirstIndex (StanVector _) = Right StanReal
dropFirstIndex (StanMatrix (_, rc)) = Right $ StanVector rc
dropFirstIndex (StanCorrMatrix d) = Right $ StanVector d
dropFirstIndex (StanCholeskyFactorCorr d) = Right $ StanVector d
dropFirstIndex (StanCovMatrix d) = Right $ StanVector d


tExprFromStanVar :: StanVar -> TExpr StanExpr
tExprFromStanVar (StanVar sn st) = TExpr (name sn) st

getDims :: StanType -> [StanDim]
getDims StanInt = []
getDims StanReal = []
getDims (StanArray dims st) = dims ++ getDims st
getDims (StanVector d) = [d]
getDims (StanMatrix (d1, d2)) = [d1, d2]
getDims (StanCorrMatrix d) = [d]
getDims (StanCholeskyFactorCorr d) = [d]
getDims (StanCovMatrix d) = [d]

getVarDims :: StanVar -> [StanDim]
getVarDims = getDims . varType

indexKeys :: StanType -> [IndexKey]
indexKeys = catMaybes . fmap dimToIndexKey . getDims where
  dimToIndexKey (NamedDim k) = Just k
  dimToindexKey _ = Nothing

stanDimToExpr :: StanDim -> StanExpr
stanDimToExpr (NamedDim k) = index k
stanDimToExpr (GivenDim n) = name $ show n
stanDimToExpr (ExprDim e) = e

indexesToExprs :: [StanDim] -> [StanExpr]
indexesToExprs = fmap stanDimToExpr

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

varAsArgument :: StanVar -> StanExpr
varAsArgument (StanVar sn st) = bare $ (typePart st) <> " " <> sn where
  typePart :: StanType -> Text
  typePart x = case x of
    StanInt -> "int"
    StanReal -> "real"
    StanVector _ -> "vector"
    StanMatrix _ -> "matrix"
    StanArray _ y -> case y of
      StanArray _ z -> typePart z
      _ -> typePart y <> "[]"
    _ -> error $ "varAsArgument: type=" <> show st <> " not supported as function argument"
  --  StanCorrMatrix dim -> withIndexes (name "corr_matrix") [dim]
--  StanCholeskyFactorCorr dim -> withIndexes (name "cholesky_factor_corr") [dim]
--  StanCovMatrix dim -> withIndexes (name "cov_matrix") [dim]

declarationExpr :: StanVar -> Text -> StanExpr
declarationExpr (StanVar sn st) c = case st of
  sa@(StanArray _ _) -> let (StanArray dims st) = collapseArray sa
                        in spaced (declarationPart st c) (withIndexes (name sn) dims)
  _ -> spaced (declarationPart st c) (name sn)

tExprToExpr :: TExpr StanExpr -> StanExpr
tExprToExpr (TExpr ne st) = withIndexes ne dims where
  dims = getDims st


data StanExprF a where
  NullF :: StanExprF a
  BareF :: Text -> StanExprF a
  AsIsF :: a -> StanExprF a -- required to rewrap Declarations
  NextToF :: a -> a -> StanExprF a
  LookupCtxtF :: LookupContext -> a -> StanExprF a
  NamedVarF :: StanVar -> StanExprF a
  MatMultF :: StanVar -> StanVar -> StanExprF a -- yeesh.  This is too specific?
  UseTExprF :: TExpr a -> StanExprF a
  IndexF :: IndexKey -> StanExprF a -- pre-lookup
  VectorizedF :: Set IndexKey -> a -> StanExprF a
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
declaration = Fix.Fix . LookupCtxtF Declare

declareVar :: StanVar -> Text -> StanExpr
declareVar sv c = declaration $ declarationExpr sv c

useTExpr :: TExpr StanExpr -> StanExpr
useTExpr = Fix.Fix . UseTExprF

var :: StanVar -> StanExpr
var = Fix.Fix . NamedVarF

--useVar :: StanVar -> StanExpr
--useVar = useTExpr . tExprFromStanVar

index :: IndexKey -> StanExpr
index k = Fix.Fix $ IndexF k

indexSize :: IndexKey -> StanExpr
indexSize = declaration . index

vectorized :: Set IndexKey -> StanExpr -> StanExpr
vectorized ks = Fix.Fix . VectorizedF ks

vectorizedOne :: IndexKey -> StanExpr -> StanExpr
vectorizedOne k = vectorized (one k)

indexBy :: StanExpr -> IndexKey -> StanExpr
indexBy e k = withIndexes e [NamedDim k]

withIndexes :: StanExpr -> [StanDim] -> StanExpr
withIndexes e eis = e `nextTo` indexes eis --``Fix.Fix $ WithIndexesF e eis

indexes :: [StanDim] -> StanExpr
indexes dims = Fix.Fix $ IndexesF dims

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

plusEq ::  StanExpr -> StanExpr -> StanExpr
plusEq = binOp "+="

minus ::  StanExpr -> StanExpr -> StanExpr
minus = binOp "-"

negate :: StanExpr -> StanExpr
negate e = bare "-" `nextTo` e

times :: StanExpr -> StanExpr -> StanExpr
times = binOp "*"

matMult :: StanVar -> StanVar -> StanExpr
matMult v1 v2 = Fix.Fix $ MatMultF v1 v2

{-
sv1 sv2 = do
  t1 <- dropLastIndex $ varType sv1
  t2 <- dropFirstIndex $ varType sv2
  let e1 = var $ StanVar (varName sv1) t1
      e2 = var $ StanVar (varName sv2) t2
  return $ times e1 e2
-}
divide :: StanExpr -> StanExpr -> StanExpr
divide = binOp "/"

multiOp :: Text -> NonEmpty StanExpr -> StanExpr
multiOp o es = foldl' (binOp o) (head es) (tail es)

-- special case for stan targets
target :: StanExpr
target = name "target"

-- special case for function return
fReturn :: StanExpr -> StanExpr
fReturn e = bare "return" `spaced` e

--data BindIndex = NoIndex | IndexE StanExpr --deriving (Show)
data VarBindingStore = VarBindingStore { useBindings :: Map IndexKey StanExpr
                                       , declarationBindings :: Map IndexKey StanExpr
                                       } deriving (Show)

bindings :: Map IndexKey StanExpr -> Map IndexKey StanExpr -> VarBindingStore
bindings = VarBindingStore

noBindings :: VarBindingStore
noBindings = bindings mempty mempty

lookupUseBinding :: IndexKey -> VarBindingStore -> Maybe StanExpr
lookupUseBinding k (VarBindingStore ubm _) = Map.lookup k ubm

lookupDeclarationBinding :: IndexKey -> VarBindingStore -> Maybe StanExpr
lookupDeclarationBinding k (VarBindingStore _ dbm) = Map.lookup k dbm

showKeys :: VarBindingStore -> Text
showKeys (VarBindingStore bms dms) =
  "Use keys: " <> show (Map.keys bms)
  <> "; Declaration keys: " <> show (Map.keys dms)

prettyPrintSTree :: StanExpr -> Text
prettyPrintSTree = toText  . Pretty.render . Rec.cata prettyPrintSExpr

prettyPrintCTree :: CodeExpr -> Text
prettyPrintCTree = toText  . Pretty.render . Rec.cata prettyPrintCExpr

-- this is wild
-- bind all indexes and do so recursively, from the top.
-- So your indexes can have indexes.
-- then fold from the bottom into Stan code
-- Requires a dataSetName for use-binding lookup
printExprWithLookupContext :: VarBindingStore -> LookupContext -> StanExpr -> Either Text Text
printExprWithLookupContext vbs lc e = do
  case Rec.anaM @_ @CodeExpr (toCodeCoAlg vbs) (AnaS lc mempty, e) of
    Left err -> Left $ err <> "\nTree:\n" <> prettyPrintSTree e
    Right ue -> Right $ Rec.cata writeCodeAlg ue

printExpr :: VarBindingStore -> StanExpr -> Either Text Text
printExpr vbs = printExprWithLookupContext vbs Use

printExpr' :: VarBindingStore -> StanExpr -> Either Text Text
printExpr' vbs e = Rec.hyloM (return . writeCodeAlg) (toCodeCoAlg vbs) (AnaS Use mempty, e)
{-
printExpr :: VarBindingStore -> StanExpr -> Either Text Text
printExpr vbs e = do
  case usingRecM $ Rec.anaM @_ @StanExpr (bindIndexCoAlg vbs) e of
    Left err -> Left $ err <> "\nTree:\n" <> prettyPrintTree e
    Right ue -> case Rec.cataM printIndexedAlg ue of
      Left err -> Left $ err <> "\nOrig Tree:\n" <> prettyPrintTree e <> "after binding:" <> prettyPrintTree ue
      Right x -> Right x

printExpr' :: VarBindingStore -> StanExpr -> Either Text Text
printExpr' vbs e = usingRecM $ Rec.hyloM (RecM . lift . printIndexedAlg) (bindIndexCoAlg vbs) e
-}

keepIndex :: Set IndexKey -> StanExpr -> Bool
keepIndex vks (Fix.Fix (IndexF k)) = not $ Set.member k vks
keepIndex _ _ = True

data LookupContext = Use | Declare deriving (Show, Eq, Ord)

data AnaS = AnaS { lContext :: LookupContext, vectorizedIndexes :: Set IndexKey}

setLookupContext :: LookupContext -> AnaS -> AnaS
setLookupContext lc (AnaS _ x) = AnaS lc x

addVectorizedIndexes :: Set IndexKey -> AnaS -> AnaS
addVectorizedIndexes vks (AnaS lc vks') = AnaS lc (Set.union vks vks')

data CodeExprF a where
  NullCF :: CodeExprF a
  BareCF :: Text -> CodeExprF a
  AsIsCF :: a -> CodeExprF a -- required to rewrap Declarations
  NextToCF :: a -> a -> CodeExprF a
  deriving  stock (Eq, Ord, Functor, Foldable, Traversable, Generic1)
  deriving   (FC.Show1, FC.Eq1, FC.Ord1) via FC.FunctorClassesDefault CodeExprF

type CodeExpr = Fix.Fix CodeExprF

toCodeCoAlg ::  VarBindingStore -> (AnaS, StanExpr) -> Either Text (CodeExprF (AnaS, StanExpr))
toCodeCoAlg vbs (as, Fix.Fix (IndexF k)) = do
  case lContext as of
    Use -> case lookupUseBinding k vbs of
      Nothing -> Left $ if Map.null (useBindings vbs)
                        then "var-index map is empty.  Maybe you forgot to choose which data set is used for binding via setDataSetForBindings ?"
                        else "re-indexing key \"" <> k <> "\" not found in useBindings: " <> show vbs

      Just ie -> return $ AsIsCF (as, ie)
    Declare ->   case lookupDeclarationBinding k vbs of
      Nothing -> Left $ "re-indexing key \"" <> k <> "\" not found in declaration-index-map: " <> show vbs
      Just ie -> return $ AsIsCF (as, ie)
toCodeCoAlg _ (AnaS _ vks, Fix.Fix (LookupCtxtF lc e)) = do
  return $ AsIsCF (AnaS lc vks, e)
toCodeCoAlg _ (as, Fix.Fix (UseTExprF te)) = return $ AsIsCF $ (as, tExprToExpr te)
toCodeCoAlg _ (as, Fix.Fix (NamedVarF sv)) = return $ AsIsCF $ (as, tExprToExpr $ tExprFromStanVar sv)
toCodeCoAlg _ (as, Fix.Fix (MatMultF sv1 sv2)) = do
  t1 <- dropLastIndex $ varType sv1
  t2 <- dropFirstIndex $ varType sv2
  let e1 = var $ StanVar (varName sv1) t1
      e2 = var $ StanVar (varName sv2) t2
  return $ AsIsCF $ (as, times e1 e2)
--  return $ AsIsCF $ (as, tExprToExpr $ tExprFromStanVar sv)
toCodeCoAlg _ (AnaS lc vks, Fix.Fix (VectorizedF vks' e)) = do
  return $ AsIsCF (AnaS lc (vks <> vks'), e)
toCodeCoAlg _ (as@(AnaS _ vks), Fix.Fix (VectorFunctionF f e es)) = do
  return $ AsIsCF $ (as, if Set.null vks then e else function f (e :| es))
toCodeCoAlg _ (as@(AnaS _ vks), Fix.Fix (IndexesF xs)) = do
  let ies = filter (keepIndex vks) $ indexesToExprs xs
  case nonEmpty ies of
    Nothing -> return NullCF
    Just x -> return $ AsIsCF (as, group "[" "]" $ csExprs x)
toCodeCoAlg _ (_, Fix.Fix NullF) = return $ NullCF
toCodeCoAlg _ (_, Fix.Fix (BareF t)) = return $ BareCF t
toCodeCoAlg _ (as, Fix.Fix (AsIsF x)) = return $ AsIsCF (as, x)
toCodeCoAlg _ (as, Fix.Fix (NextToF l r)) = return $ NextToCF (as, l) (as, r)

data VarF a where
  VarsVF :: [StanVar] -> a -> VarF a
  ExprVF :: a -> VarF a
  NullVF :: VarF a
  PairVF :: a -> a -> VarF a
  deriving  stock (Eq, Ord, Functor, Foldable, Traversable, Generic1)
  deriving   (FC.Show1, FC.Eq1, FC.Ord1) via FC.FunctorClassesDefault VarF

varsCoAlg :: VarBindingStore -> (AnaS, StanExpr) -> Either Text (VarF (AnaS, StanExpr))
varsCoAlg vbs (as, Fix.Fix (IndexF k)) =
  case lContext as of
    Use -> case lookupUseBinding k vbs of
      Nothing -> Left $ if Map.null (useBindings vbs)
                        then "varsCoAlg: var-index map is empty.  Maybe you forgot to choose which data set is used for binding via setDataSetForBindings ?"
                        else "varsCoAlg: re-indexing key \"" <> k <> "\" not found in useBindings: " <> show vbs
      Just ie -> return $  ExprVF (as, ie)
    Declare ->   case lookupDeclarationBinding k vbs of
      Nothing -> Left $ "varsCoAlg: re-indexing key \"" <> k <> "\" not found in declaration-index-map: " <> show vbs
      Just ie -> return $ ExprVF (as, ie)
varsCoAlg _ (AnaS _ vks, Fix.Fix (LookupCtxtF lc e)) = return $ ExprVF (AnaS lc vks, e)
varsCoAlg _ (as, Fix.Fix (UseTExprF te)) = return $ ExprVF (as, tExprToExpr te)
varsCoAlg _ (as, Fix.Fix (NamedVarF sv)) = return $ VarsVF [sv] $ (as, tExprToExpr $ tExprFromStanVar sv)
varsCoAlg _ (as, Fix.Fix (MatMultF sv1 sv2)) = do
  t1 <- dropLastIndex $ varType sv1
  t2 <- dropFirstIndex $ varType sv2
  return $ VarsVF [sv1, sv2] $ (as, indexes $ getDims t1 ++ getDims t2) -- we still need any vars in the indexes
varsCoAlg _ (AnaS lc vks, Fix.Fix (VectorizedF vks' e)) = return $ ExprVF (AnaS lc (vks <> vks'), e)
varsCoAlg _ (as@(AnaS _ vks), Fix.Fix (VectorFunctionF f e es)) = return $ ExprVF $ (as, if Set.null vks then e else function f (e :| es))
varsCoAlg _ (as@(AnaS _ vks), Fix.Fix (IndexesF xs)) = do
  let ies = filter (keepIndex vks) $ indexesToExprs xs
  case nonEmpty ies of
    Nothing -> return NullVF
    Just x -> return $ ExprVF (as, group "[" "]" $ csExprs x)
varsCoAlg _ (_, Fix.Fix NullF) = return NullVF
varsCoAlg _ (_, Fix.Fix (BareF t)) = return NullVF
varsCoAlg _ (as, Fix.Fix (AsIsF x)) = return $ ExprVF (as, x)
varsCoAlg _ (as, Fix.Fix (NextToF l r)) = return $ PairVF (as, l) (as, r)

varsAlg :: VarF [StanVar] -> [StanVar]
varsAlg (VarsVF svs vars) = svs ++ vars
varsAlg (ExprVF vars) = vars
varsAlg NullVF = []
varsAlg (PairVF vars1 vars2) = vars1 ++ vars2

exprVars :: VarBindingStore -> StanExpr -> Either Text [StanVar]
exprVars vbs e = Rec.hyloM (return . varsAlg) (varsCoAlg vbs) (AnaS Use mempty, e)

csArgs :: [Text] -> Text
csArgs = T.intercalate ", "

writeCodeAlg :: CodeExprF Text -> Text
writeCodeAlg NullCF = ""
writeCodeAlg (BareCF t) = t
writeCodeAlg (AsIsCF e) = e
writeCodeAlg (NextToCF l r) = l <> r

prettyPrintSExpr :: StanExprF Pretty.Doc -> Pretty.Doc
prettyPrintSExpr NullF = mempty
prettyPrintSExpr (BareF t) = Pretty.text (toString t)
prettyPrintSExpr (AsIsF t) = t
prettyPrintSExpr (NextToF l r) = l <> r
prettyPrintSExpr (LookupCtxtF lc e) = Pretty.text ("LookupCtxt->" <> show lc <> ": ") <>  Pretty.parens e
prettyPrintSExpr (NamedVarF sv) = Pretty.text ("var(" <> show sv <> ")")
prettyPrintSExpr (MatMultF sv1 sv2) = Pretty.text ("var(" <> show sv1 <> ") <*> var(" <> show sv2 <> ")")
prettyPrintSExpr (UseTExprF (TExpr e st)) = Pretty.text ("use [of " <> show st <> "]")  <> Pretty.parens e
prettyPrintSExpr (IndexF k) = Pretty.braces (Pretty.text $ toString k)
prettyPrintSExpr (VectorizedF ks e) = Pretty.text "vec" <> Pretty.brackets (Pretty.text $ show  $ Set.toList ks) <> Pretty.parens e
prettyPrintSExpr (IndexesF ds) = Pretty.braces (show ds)
prettyPrintSExpr (VectorFunctionF f e es) = e <> Pretty.text "?" <> Pretty.text (toString f) <> Pretty.parens (mconcat (Pretty.punctuate "," es))

prettyPrintCExpr :: CodeExprF Pretty.Doc -> Pretty.Doc
prettyPrintCExpr NullCF = mempty
prettyPrintCExpr (BareCF t) = Pretty.text (toString t)
prettyPrintCExpr (AsIsCF t) = t
prettyPrintCExpr (NextToCF l r) = l <> r
---

printIndexedAlg :: StanExprF Text -> Either Text Text
printIndexedAlg NullF = Right ""
printIndexedAlg (BareF t) = Right t
printIndexedAlg (AsIsF t) = Right t
printIndexedAlg (NextToF l r) = Right $ l <> r
printIndexedAlg (LookupCtxtF _ _) = Left "Should not call printExpr before performing lookups (LookupCtxtF)"
printIndexedAlg (NamedVarF _) = Left "Should not call printExpr before expanding variables. (NamedVarF)"
printIndexedAlg (MatMultF _ _) = Left "Should not call printExpr before expanding variables. (MatMultF)"
printIndexedAlg (UseTExprF _) = Left "Should not call printExpr before expanding texpr/var uses (UseTExprF)"
printIndexedAlg (IndexF _) = Left "Should not call printExpr before binding declaration indexes (IndexF)"
printIndexedAlg (VectorizedF _ _) = Left "Should not call printExpr before resolving vectorization use (VectorizedF)"
printIndexedAlg (IndexesF _) = Left "Should not call printExpr before resolving indexes use (IndexesF)"
printIndexedAlg (VectorFunctionF f e argEs) = Left "Should not call printExpr before resolving vectorization use (VectorFunctionF)"

{-
bindIndexCoAlg ::  VarBindingStore -> StanExpr -> RecM (StanExprF StanExpr)
bindIndexCoAlg vbs (Fix.Fix (IndexF k)) = do
  lc <- lContext <$> get
  case lc of
    Use -> case lookupUseBinding k vbs of
      Nothing -> recLeft $ "re-indexing key \"" <> k <> "\" not found in var-index-map: " <> showKeys vbs
      Just ie -> return $ AsIsF ie
    Declare ->   case lookupDeclarationBinding k vbs of
      Nothing -> recLeft $ "re-indexing key \"" <> k <> "\" not found in declaration-index-map: " <> showKeys vbs
      Just ie -> return $ AsIsF ie
bindIndexCoAlg _ (Fix.Fix (DeclCtxtF e)) = do
  modify $ setLookupContext Declare
  return $ AsIsF e
bindIndexCoAlg _ (Fix.Fix (UseTExprF te)) = return $ AsIsF $ tExprToExpr te
bindIndexCoAlg _ (Fix.Fix (VectorizedF vks e)) = do
  modify $ addVectorizedIndexes vks
  return $ AsIsF e
bindIndexCoAlg _ (Fix.Fix (VectorFunctionF f e es)) = do
  vks <- vectorizedIndexes <$> get
  return $ AsIsF $ if Set.null vks then e else function f (e :| es)
bindIndexCoAlg _ (Fix.Fix (IndexesF xs)) = do
  vks <- vectorizedIndexes <$> get
  let ies = filter (keepIndex vks) $ indexesToExprs xs
  case nonEmpty ies of
    Nothing -> return NullF
    Just x -> return $ AsIsF $ group "[" "]" $ csExprs x
bindIndexCoAlg _ x = return $ AsIsF x
-}

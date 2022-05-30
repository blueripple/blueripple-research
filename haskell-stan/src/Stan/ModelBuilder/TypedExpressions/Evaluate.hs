{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveTraversable #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}
{-# LANGUAGE TypeApplications #-}

module Stan.ModelBuilder.TypedExpressions.Evaluate where
import qualified Stan.ModelBuilder.Expressions as SME
import Prelude hiding (Nat)

import Stan.ModelBuilder.TypedExpressions.Types
import Stan.ModelBuilder.TypedExpressions.Indexing
import Stan.ModelBuilder.TypedExpressions.Operations
import Stan.ModelBuilder.TypedExpressions.Expressions
import Stan.ModelBuilder.TypedExpressions.Statements
import Stan.ModelBuilder.TypedExpressions.Recursion

import qualified Data.Functor.Foldable.Monadic as RS

import Control.Monad.State.Strict (modify, gets, withStateT)
import Data.Type.Nat (Nat(..), SNat(..))
import Data.Foldable (maximum)
import qualified Data.Functor.Classes as FC
import qualified Data.Functor.Classes.Generic as FC
import qualified Data.Functor.Foldable as Rec
import qualified Data.Functor.Foldable.Monadic as Rec
import GHC.Generics (Generic1)

import qualified Data.IntMap.Strict as IM
import qualified Data.Map.Strict as Map

import          Data.Kind (Type)
import          Data.List ((!!),(++))
import qualified Data.Fix as Fix
import qualified Data.Text as T

--import qualified Text.PrettyPrint as Pretty
import qualified Data.Type.Nat as DT
import Stan.ModelBuilder.Expressions (LookupContext)
import GHC.RTS.Flags (GCFlags(oldGenFactor))

{- Evaluation passes
1a. NatM LookupM UExpr LExpr (forall x. UExpr x -> LookupM LExpr x)
 - Do any lookups required to turn a UExpr into an LExpr
 - Since these are each fixed-points of HFunctors (parameterized by (EType -> Type)),
   this requires HFunctor recursion machinery.
1b. CStmt -> LookupM LStmt
 - CStmt has index expressions to look up within statement and context changes in the lookup environment.
 - LStmt has all things looked up.  This is now a tree of statements with expressions.

2. Optimization?

3a. LExpr ~> FormattedText (expressions to Text but in HFunctor form)
3b. LStmt -> Stmt (FormattedText) (Statement tree with expressions to statement tree with Text for expressions.)
3c. FormattedText -> Text
3c. Stmt (FormattedText) -> Text (Produce code for statement tree)
-}

type LookupM = StateT IndexLookupCtxt (Either Text)

lookupUse :: SME.IndexKey -> LookupM (LExpr EInt)
lookupUse k = do
  im <- gets indices
  case Map.lookup k im of
    Just e -> pure e
    Nothing -> lift $ Left $ "lookupUse: \"" <> k <> "\" not found in use map."

lookupSize :: SME.IndexKey -> LookupM (LExpr EInt)
lookupSize k = do
  sm <- gets sizes
  case Map.lookup k sm of
    Just e -> pure e
    Nothing -> lift $ Left $ "lookupDecl: \"" <> k <> "\" not found in decl map."

toLExprAlg :: IAlgM LookupM UExprF LExpr
toLExprAlg = \case
  UL x -> pure $ IFix x
  UNamedIndex ik -> lookupUse ik
  UNamedSize ik -> lookupSize ik

doLookups :: NatM LookupM UExpr LExpr
doLookups = iCataM toLExprAlg


-- Coalgebra to unfold a statement requiring lookups from the top down so that
-- context
doLookupsInStatementC :: Stmt UExpr -> LookupM (StmtF LExpr (Stmt UExpr))
doLookupsInStatementC = \case
  SDeclare txt st divf -> SDeclareF txt st <$> htraverse f divf
  SDeclAssign txt st divf if' -> SDeclAssignF txt st <$> htraverse f divf <*> f if'
  SAssign if' if1 -> SAssignF <$> f if' <*> f if1
  STarget if' -> STargetF <$> f if'
  SSample if' dis al -> SSampleF <$> f if' <*> pure dis <*> htraverse f al
  SFor txt if' if1 sts -> SForF txt <$> f if' <*> f if1 <*> pure sts
  SForEach txt if' sts -> SForEachF txt <$> f if' <*> pure sts
  SIfElse x0 st -> SIfElseF <$> traverse (\(c, s) -> (,) <$> f c <*> pure s) x0 <*> pure st
  SWhile if' sts -> SWhileF <$> f if' <*> pure sts
  SFunction func al sts -> pure $ SFunctionF func al sts
  SScoped b sts -> pure $ SScopedF b sts
  where
    f :: NatM LookupM UExpr LExpr
    f = doLookups

doLookupsInCStatementC :: CStmt -> LookupM (StmtF LExpr CStmt)
doLookupsInCStatementC = \case
  Extant st -> fmap Extant <$> doLookupsInStatementC st
  Change f cs ->  SScopedF False . pure <$> withStateT f (pure cs)

doLookupsInCStatement :: CStmt -> LookupM LStmt
doLookupsInCStatement = RS.anaM doLookupsInCStatementC

doLookupsInStatementE :: IndexLookupCtxt -> CStmt -> Either Text LStmt
doLookupsInStatementE ctxt0 = flip evalStateT ctxt0 . doLookupsInCStatement


{-
  DeclareEF st divf -> Right $ IFix $ LDeclareF st divf
  NamedEF txt st -> Right $ IFix $ LNamedF txt st
  IntEF n -> Right $ IFix $ LIntF n
  RealEF x -> Right $ IFix $ LRealF x
  ComplexEF x y -> Right $ IFix $ LComplexF x y
  BinaryOpEF sbo lhe rhe -> Right $ IFix $ LBinaryOpF sbo lhe rhe
  SliceEF sn ie e -> Right $ IFix $ LSliceF sn ie e
  NamedIndexEF key -> case Map.lookup key varMap of
    Just e -> Right e
    Nothing -> Left $ "index lookup failed for key=" <> key
-}
{-
data IExprF :: Type -> Type where
  IIndexed :: Text -> IM.IntMap Text -> [Int] -> IExprF a
  ILookup :: Text -> IExprF a
  deriving stock (Eq, Ord, Functor, Foldable, Traversable, Generic1)
  deriving (FC.Show1, FC.Eq1, FC.Ord1) via FC.FunctorClassesDefault IExprF
-}
{-
data IExpr = IExpr { atom :: Text,  indexed :: IM.IntMap Text, unusedIndices :: [Int] }

writeIExpr :: IExpr -> Text
writeIExpr (IExpr a is _) = a <> indexPart
  where
    maxUsed = maximum $ IM.keys is
    is' = is <> IM.fromList (zip [0..maxUsed] (repeat ""))
    indexPart = if IM.size is == 0
                then ""
                else "[" <> T.intercalate ", " (IM.elems is') <> "]"

cIndexed :: Text -> Int -> IExpr
cIndexed t nIndices = IExpr t IM.empty newIndices
  where
    newIndices = take nIndices [0..]

cAppendIndex :: SNat n -> Text -> IExpr -> IExpr
cAppendIndex n it (IExpr t im is) = IExpr t im' is'
  where
    nInt :: Int = fromIntegral $ DT.snatToNatural n
    (i, is') = (is !! nInt, take nInt is ++ drop (nInt + 1) is)
    im' = IM.insert i it im

inParen :: Text -> Text
inParen t = "(" <> t <> ")"

toIExprAlg :: IAlg LExprF (K IExpr)
toIExprAlg x = K $ case x of
  LDeclareF st divf -> undefined
  LNamedF txt _ -> cIndexed txt 0
  LIntF n -> cIndexed (show n) 0
  LRealF x -> cIndexed (show x) 0
  LComplexF rp ip -> cIndexed (inParen $ show rp <> " + i" <> show ip) 0
  LBinaryOpF sbo le re -> cIndexed (inParen $ writeIExpr (unK le) <> " " <> opText sbo <> " " <> writeIExpr (unK re)) 0
  LSliceF sn ie e -> cAppendIndex sn (writeIExpr $ unK ie) (unK e)


stanDeclHead :: StanType t -> DeclIndexVec t -> Text
stanDeclHead st iv = case st of
  StanInt -> "int"
  StanReal -> "real"
  StanArray sn st -> "array" <> indexText (take )
  StanVector -> "vector" <> indexText iv
  StanMatrix -> "matrix" <> indexText iv
  StanCorrMatrix -> "corr_matrix" <> indexText iv
  StanCholeskyFactorCorr -> "cholesky_factor_corr" indexText iv
  StanCovMatrix -> "cov_matrix" <> indexText iv

indexText :: Vec (UExprF EInt) n -> Text
indexText VNil = ""
indexText (i ::: v) = "[" <> go i v <> "]" where
  printIndex :: UExprF EInt -> Text
  printIndex = undefined
  go :: UExprF EInt -> Vec (UExprF EInt) m -> Text
  go ie VNil = printIndex ie
  go ie (ie' ::: v') = printIndex ie <> "," <> go ie' v'


opText :: SBinaryOp op -> Text
opText = \case
  SEqual -> "="
  SAdd -> "+"
  SSubtract -> "-"
  SMultiply -> "*"
  SDivide -> "/"
  SElementWise sbo -> "." <> opText sbo
  SAndEqual sbo -> opText sbo <> "="
-}

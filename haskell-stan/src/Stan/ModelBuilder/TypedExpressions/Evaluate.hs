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


type LookupM = StateT IndexLookupCtxt (Either Text)

lookupUse :: SME.IndexKey -> LookupM (LExpr EInt)
lookupUse k = do
  im <- gets indices
  case Map.lookup k im of
    Just e -> pure e
    Nothing -> lift $ Left $ "lookupUse: " <> k <> " not found in use map."

lookupSize :: SME.IndexKey -> LookupM (LExpr EInt)
lookupSize k = do
  sm <- gets sizes
  case Map.lookup k sm of
    Just e -> pure e
    Nothing -> lift $ Left $ "lookupDecl: " <> k <> " not found in decl map."

toLExprAlg :: IAlgM LookupM UExprF LExpr
toLExprAlg = \case
  UL x -> pure $ IFix x
  UNamedIndex ik -> lookupUse ik
  UNamedSize ik -> lookupSize ik

doLookups :: NatM LookupM UExpr LExpr
doLookups = iCataM toLExprAlg

doLookupsInStatementA :: StmtF UExpr (Stmt LExpr) -> LookupM (Stmt LExpr)
doLookupsInStatementA = \case
  SDeclareF txt st divf -> SDeclare txt st <$> htraverse f divf
  SDeclAssignF txt st divf if' -> SDeclAssign txt st <$> htraverse f divf <*> f if'
  SAssignF if' if1 -> SAssign <$> f if' <*> f if1
  STargetF if' -> STarget <$> f if'
  SSampleF if' dis al -> SSample <$> f if' <*> pure dis <*> htraverse f al
  SForF txt if' if1 sts -> SFor txt <$> f if' <*> f if1 <*> pure sts
  SForEachF txt if' sts -> SForEach txt <$> f if' <*> pure sts
  SIfElseF x0 st -> SIfElse <$> traverse (\(c, s) -> (,) <$> f c <*> pure s) x0 <*> pure st
  SWhileF if' sts -> SWhile <$> f if' <*> pure sts
  SFunctionF func al sts -> pure $ SFunction func al sts
  SScopeF sts -> pure $ SScope sts
  where
    f :: NatM LookupM UExpr LExpr
    f = doLookups

doLookupsInStatement :: Stmt UExpr -> LookupM LStmt
doLookupsInStatement = RS.cataM doLookupsInStatementA

doLookupsInUStatementA :: UStmtF (Stmt LExpr) -> LookupM LStmt
doLookupsInUStatementA = \case
  USF st -> pure st
  CSF f us -> withStateT f (doLookupsInStatement us)

doLookupsInUStatement :: UStmt -> LookupM LStmt
doLookupsInUStatement = RS.cataM doLookupsInUStatementA

doLookupsInStatementE :: IndexLookupCtxt -> UStmt -> Either Text LStmt
doLookupsInStatementE ctxt0 = flip evalStateT ctxt0 . doLookupsInUStatement


{-
wcDoLookupsInStatementA :: WithContextF UStmt LStmt -> LookupM LStmt
wcDoLookupsInStatementA = \case
  ExtantF st -> pure st
  ChangeF f us -> withStateT f (doLookupsInStatement us)
{-  ChangeF f us -> do
    old <- get
    modify f
    ls <- doLookupsInStatement us
    put old
    return ls
-}
wcDoLookupsInStatement :: WithContext UStmt -> LookupM LStmt
wcDoLookupsInStatement = RS.cataM wcDoLookupsInStatementA

wcDoLookupsInStatementE :: IndexLookupCtxt -> WithContext UStmt -> Either Text LStmt
wcDoLookupsInStatementE ctxt0  = flip evalStateT ctxt0 . wcDoLookupsInStatement
-}


{-
1. UCStmt -> m LStmt
  = Fix UCStmtF -> LookupM LStmt
  = cataM (algM :: AlgM LookupM UCStmtF LStmt)
2. UStmtF -> LookupM (Fix LStmtF)
  = iAnaM (iCoAlgM :: ICoAlgM LookupM LStmtF UStmtF)
         = NatM LookupM UStmtF
-}

{-
alg1 :: AlgM LookupM UCStmtF LStmt
alg1 = \case
  UContext f sf -> modify f >> _
-}

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

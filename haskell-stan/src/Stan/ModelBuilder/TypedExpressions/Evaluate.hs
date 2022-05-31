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
import Stan.ModelBuilder.TypedExpressions.Format

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
import Knit.Report (boundaryFrom)

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
  SBreak -> pure SBreakF
  SContinue -> pure SContinueF
  SFunction func al sts re -> SFunctionF func al sts <$> f re
  SPrint args -> SPrintF <$> htraverse f args
  SReject args -> SRejectF <$> htraverse f args
  SScoped sts -> pure $ SScopedF sts
  SContext mf body -> case mf of
    Nothing -> pure $ SContextF Nothing body
    Just f -> SContextF Nothing <$> withStateT f (pure body)
  where
    f :: NatM LookupM UExpr LExpr
    f = doLookups


doLookupsInCStatement :: UStmt -> LookupM LStmt
doLookupsInCStatement = RS.anaM doLookupsInStatementC

doLookupsInStatementE :: IndexLookupCtxt -> UStmt -> Either Text LStmt
doLookupsInStatementE ctxt0 = flip evalStateT ctxt0 . doLookupsInCStatement

statementToCodeE :: IndexLookupCtxt -> UStmt -> Either Text CodePP
statementToCodeE ctxt0 x = doLookupsInStatementE ctxt0 x >>= stmtToCodeE

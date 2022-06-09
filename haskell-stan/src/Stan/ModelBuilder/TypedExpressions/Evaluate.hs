{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}
{-# LANGUAGE TypeOperators #-}

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
import qualified Data.Functor.Foldable as RS

import Control.Monad.State.Strict (modify, withStateT)
import Data.Type.Nat (Nat(..), SNat(..))
import Data.Foldable (maximum)
import qualified Data.Functor.Classes as FC
import qualified Data.Functor.Classes.Generic as FC
import qualified Data.Functor.Foldable as Rec
import qualified Data.Functor.Foldable.Monadic as Rec

import qualified Control.Error as X

import qualified Data.IntMap.Strict as IM
import qualified Data.Map.Strict as Map

import          Data.Kind (Type)
import          Data.List ((!!),(++))
import qualified Data.Fix as Fix
import qualified Data.Text as T


import qualified Prettyprinter as PP
import qualified Data.Type.Nat as DT
import Stan.ModelBuilder.Expressions (LookupContext)


{- Evaluation passes
1a. NatM LookupM UExpr LExpr (forall x. UExpr x -> LookupM LExpr x)
 - Do any lookups required to turn a UExpr into an LExpr
 - Since these are each fixed-points of HFunctors (parameterized by (EType -> Type)),
   this requires HFunctor recursion machinery.
1b. CStmt -> LookupM LStmt
 - CStmt has index expressions to look up within statement and context changes in the lookup environment.
 - LStmt has all things looked up.  This is now a tree of statements with expressions.

2a. (Runtime) error checks?
2b. Optimization (vectorization? CSE? Loop splitting or consolidating?)

3a. LExpr ~> FormattedText (expressions to Text but in HFunctor form)
3b. LStmt -> Stmt (FormattedText) (Statement tree with expressions to statement tree with Text for expressions.)
3c. FormattedText -> Text
3c. Stmt (FormattedText) -> Text (Produce code for statement tree)
-}

type LookupM = StateT IndexLookupCtxt (Either Text)

lookupIndex :: SME.IndexKey -> LookupM (LExpr EInt)
lookupIndex k = do
  im <- gets indexes
  case Map.lookup k im of
    Just e -> pure e
    Nothing -> lift $ Left $ "lookupIndex: \"" <> k <> "\" not found in index map."

lookupSize :: SME.IndexKey -> LookupM (LExpr EInt)
lookupSize k = do
  sm <- gets sizes
  case Map.lookup k sm of
    Just e -> pure e
    Nothing -> lift $ Left $ "lookupSize: \"" <> k <> "\" not found in size map."

toLExprAlg :: IAlgM LookupM UExprF LExpr
toLExprAlg = \case
  UL x -> pure $ IFix x
  UNamedIndex ik -> lookupIndex ik
  UNamedSize ik -> lookupSize ik

doLookups :: NatM LookupM UExpr LExpr
doLookups = iCataM toLExprAlg


handleContext :: StmtF r a -> LookupM (StmtF r a)
handleContext = \case
  SContextF mf body -> case mf of
    Nothing -> pure $ SContextF Nothing body
    Just f -> SContextF Nothing <$> withStateT f (pure body)
  x -> pure x

doLookupsInCStatement :: UStmt -> LookupM LStmt
doLookupsInCStatement = RS.anaM (\x -> htraverse doLookups (RS.project x) >>= handleContext)

doLookupsInStatementE :: IndexLookupCtxt -> UStmt -> Either Text LStmt
doLookupsInStatementE ctxt0 = flip evalStateT ctxt0 . doLookupsInCStatement

statementToCodeE :: IndexLookupCtxt -> UStmt -> Either Text CodePP
statementToCodeE ctxt0 x = doLookupsInStatementE ctxt0 x >>= stmtToCodeE

data EExprF :: (EType -> Type) -> EType -> Type where
  EL :: LExprF r t -> EExprF r t
  EE :: Text -> EExprF r t

instance HFunctor EExprF where
  hfmap nat = \case
    EL x -> EL $ hfmap nat x
    EE t -> EE t

instance HTraversable EExprF where
  htraverse natM = \case
    EL x -> EL <$> htraverse natM x
    EE t -> pure $ EE t
  hmapM = htraverse

type EExpr = IFix EExprF

lExprToEExpr :: LExpr t -> EExpr t
lExprToEExpr = iCata (IFix . EL)

lookupIndexE :: SME.IndexKey -> LookupM (EExpr EInt)
lookupIndexE k =  do
  im <- gets indexes
  case Map.lookup k im of
    Just e -> pure $ lExprToEExpr e
    Nothing -> pure $ IFix $ EE $ "#index: " <> k <> "#"

lookupSizeE :: SME.IndexKey -> LookupM (EExpr EInt)
lookupSizeE k =  do
  im <- gets sizes
  case Map.lookup k im of
    Just e -> pure $ lExprToEExpr e
    Nothing -> pure $ IFix $ EE $ "#size: " <> k <> "#"

type EStmt = Stmt EExpr

doLookupsEInStatement :: UStmt -> LookupM EStmt
doLookupsEInStatement = RS.anaM (\x -> htraverse doLookupsE (RS.project x) >>= handleContext)

doLookupsEInStatementE :: IndexLookupCtxt -> UStmt -> Either Text EStmt
doLookupsEInStatementE ctxt0 = flip evalStateT ctxt0 . doLookupsEInStatement

doLookupsE :: NatM LookupM UExpr EExpr
doLookupsE = iCataM $ \case
  UL x -> pure $ IFix $ EL x
  UNamedIndex ik -> lookupIndexE ik
  UNamedSize ik -> lookupSizeE ik


eExprToIExprCode :: EExpr ~> K IExprCode
eExprToIExprCode = iCata $ \case
  EL x -> exprToDocAlg x
  EE t -> K $ Unsliced $ PP.pretty t

eExprToCode :: EExpr ~> K CodePP
eExprToCode = K . iExprToCode . unK . eExprToIExprCode

eStmtToCode :: EStmt -> Either Text CodePP
eStmtToCode = RS.hylo stmtToCodeAlg (hfmap eExprToCode . RS.project)

eStatementToCodeE :: IndexLookupCtxt -> UStmt -> Either Text CodePP
eStatementToCodeE ctxt0 x = doLookupsEInStatementE ctxt0 x >>= eStmtToCode


{-
g :: NatM LookupM UExpr EExpr --UExpr t -> LookupM (EExpr t)
g = \case
  UL x -> _

  iCataM $ \case
  UL x ->

--h :: NatM LookupM UExpr (EExprF UExpr)
--h = iCataM _
--  UL x -> pure $ IFix $ EL x


toEExprAlg :: IAlgM LookupM UExprF EExpr
toEExprAlg = \case
  UL x -> pure $ EL x
  UNamedIndex ik -> lookupUseE ik
  UNamedSize ik -> lookupSizeE ik

doLookupsE :: NatM LookupM UExpr EExpr
doLookupsE = iCataM toEExprAlg

--doLookupsEInCStatement :: UStmt -> LookupM EStmt
--doLookupsEInCStatement = RS.anaM (htraverse )
-}

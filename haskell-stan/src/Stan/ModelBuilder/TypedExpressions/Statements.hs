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
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE UndecidableInstances #-}
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}

module Stan.ModelBuilder.TypedExpressions.Statements
  (
    module Stan.ModelBuilder.TypedExpressions.Statements
  , module Stan.ModelBuilder.TypedExpressions.Expressions
  )
  where

import qualified Stan.ModelBuilder.TypedExpressions.Recursion as TR
import Stan.ModelBuilder.TypedExpressions.Expressions
import Stan.ModelBuilder.TypedExpressions.Types
import Stan.ModelBuilder.TypedExpressions.Indexing
import Stan.ModelBuilder.TypedExpressions.Operations
import Stan.ModelBuilder.TypedExpressions.Functions
import qualified Stan.ModelBuilder.Expressions as SME
import Prelude hiding (Nat)
import Relude.Extra
import qualified Data.Map.Strict as Map
import Data.Vector.Generic (unsafeCopy)

import qualified Data.Functor.Foldable as RS

--data Block f = Block { blockName :: Text, statements :: [Stmt f]}

-- Statements
data Stmt :: (EType -> Type) -> Type where
  SDeclare ::  Text -> StanType et -> DeclIndexVecF r et -> Stmt r
  SDeclAssign :: Text -> StanType et -> DeclIndexVecF r et -> r et -> Stmt r
  SAssign :: r t -> r t -> Stmt r
  STarget :: r EReal -> Stmt r
  SSample :: r st -> Distribution st args -> ArgList r args -> Stmt r
  SFor :: Text -> r EInt -> r EInt -> [Stmt r] -> Stmt r
  SForEach :: Text -> r t -> [Stmt r] -> Stmt r
  SIfElse :: [(r EBool, Stmt r)] -> Stmt r -> Stmt r -- [(condition, ifTrue)] -> ifAllFalse
  SWhile :: r EBool -> [Stmt r] -> Stmt r
  SBreak :: Stmt r
  SContinue :: Stmt r
  SFunction :: Function rt args -> ArgList (FuncArg Text) args -> [Stmt r] -> r rt -> Stmt r
  SPrint :: ArgList r args -> Stmt r
  SReject :: ArgList r args -> Stmt r
  SScoped :: [Stmt r] -> Stmt r
  SContext :: Maybe (IndexLookupCtxt -> IndexLookupCtxt) -> [Stmt r] -> Stmt r

data StmtF :: (EType -> Type) -> Type -> Type where
  SDeclareF ::  Text -> StanType et -> DeclIndexVecF r et -> StmtF r a
  SDeclAssignF :: Text -> StanType et -> DeclIndexVecF r et -> r et -> StmtF r a
  SAssignF :: r t -> r t -> StmtF r a
  STargetF :: r EReal -> StmtF r a
  SSampleF :: r st -> Distribution st args -> ArgList r args -> StmtF r a
  SForF :: Text -> r EInt -> r EInt -> [a] -> StmtF r a
  SForEachF :: Text -> r t -> [a] -> StmtF r a
  SIfElseF :: [(r EBool, a)] -> a -> StmtF r a -- [(condition, ifTrue)] -> ifAllFalse
  SWhileF :: r EBool -> [a] -> StmtF r a
  SBreakF :: StmtF r a
  SContinueF :: StmtF r a
  SFunctionF :: Function rt args -> ArgList (FuncArg Text) args -> [a] -> r rt -> StmtF r a
  SPrintF :: ArgList r args -> StmtF r a
  SRejectF :: ArgList r args -> StmtF r a
  SScopedF :: [a] -> StmtF r a
  SContextF :: Maybe (IndexLookupCtxt -> IndexLookupCtxt) -> [a] -> StmtF r a


type instance RS.Base (Stmt f) = StmtF f

type LStmt = Stmt LExpr
type UStmt = Stmt UExpr

data IndexLookupCtxt = IndexLookupCtxt { sizes :: Map SME.IndexKey (LExpr EInt), indices :: Map SME.IndexKey (LExpr EInt) }

{-
declare :: Text -> StanType t -> DeclIndexVecF UExpr t -> UStmt
declare = US $ SDeclare

declareAndAssign :: Text -> StanType t -> DeclIndexVecF UExpr t -> UExpr t -> UStmt
declareAndAssign = SDeclAssign

assign :: UExpr t -> UExpr t -> UStmt
assign = SAssign

target :: UExpr EReal -> UStmt
target = STarget

sample :: UExpr t -> Distribution t args -> ArgList UExpr args -> UStmt
sample = SSample

for :: Text -> UExpr EInt -> UExpr EInt -> [UStmt] -> UStmt
for = SFor
-}

instance Functor (StmtF f) where
  fmap f x = case x of
    SDeclareF txt st divf -> SDeclareF txt st divf
    SDeclAssignF txt st divf fet -> SDeclAssignF txt st divf fet
    SAssignF ft ft' -> SAssignF ft ft'
    STargetF f' -> STargetF f'
    SSampleF fst dis al -> SSampleF fst dis al
    SForF ctr startE endE body -> SForF ctr startE endE (f <$> body)
    SForEachF ctr fromE body -> SForEachF ctr fromE (f <$> body)
    SIfElseF x1 sf -> SIfElseF (secondF f x1) (f sf)
    SWhileF cond sfs -> SWhileF cond (f <$> sfs)
    SBreakF -> SBreakF
    SContinueF -> SContinueF
    SFunctionF func al sfs re -> SFunctionF func al (f <$> sfs) re
    SPrintF args -> SPrintF args
    SRejectF args -> SPrintF args
    SScopedF sfs -> SScopedF $ f <$> sfs
    SContextF mf sts -> SContextF mf $  f <$> sts

instance Foldable (StmtF f) where
  foldMap f = \case
    SDeclareF {} -> mempty
    SDeclAssignF {} -> mempty
    SAssignF {} -> mempty
    STargetF _ -> mempty
    SSampleF {} -> mempty
    SForF _ _ _ body -> foldMap f body
    SForEachF _ _ body -> foldMap f body
    SIfElseF ifConds sf -> foldMap (f . snd) ifConds <> f sf
    SWhileF _ body -> foldMap f body
    SBreakF -> mempty
    SContinueF -> mempty
    SFunctionF _ _ body _ -> foldMap f body
    SPrintF _ -> mempty
    SRejectF _ -> mempty
    SScopedF body -> foldMap f body
    SContextF _ body -> foldMap f body

instance Traversable (StmtF f) where
  traverse g = \case
    SDeclareF txt st divf -> pure $ SDeclareF txt st divf
    SDeclAssignF txt st divf fet -> pure $ SDeclAssignF txt st divf fet
    SAssignF ft ft' -> pure $ SAssignF ft ft'
    STargetF f -> pure $ STargetF f
    SSampleF fst dis al -> pure $ SSampleF fst dis al
    SForF txt f f' sfs -> SForF txt f f' <$> traverse g sfs
    SForEachF txt ft sfs -> SForEachF txt ft <$> traverse g sfs
    SIfElseF x0 sf -> SIfElseF <$> traverse (\(c, s) -> pure ((,) c) <*> g s) x0 <*> g sf
    SWhileF f body -> SWhileF f <$> traverse g body
    SBreakF -> pure SBreakF
    SContinueF -> pure SContinueF
    SFunctionF func al sfs re -> SFunctionF func al <$> traverse g sfs <*> pure re
    SPrintF args -> pure $ SPrintF args
    SRejectF args -> pure $ SRejectF args
    SScopedF sfs -> SScopedF <$> traverse g sfs
    SContextF mf sfs -> SContextF mf <$> traverse g sfs

instance Functor (RS.Base (Stmt f)) => RS.Recursive (Stmt f) where
  project = \case
    SDeclare txt st divf -> SDeclareF txt st divf
    SDeclAssign txt st divf fet -> SDeclAssignF txt st divf fet
    SAssign ft ft' -> SAssignF ft ft'
    STarget f -> STargetF f
    SSample fst dis al -> SSampleF fst dis al
    SFor txt f f' sts -> SForF txt f f' sts
    SForEach txt ft sts -> SForEachF txt ft sts
    SIfElse x0 st -> SIfElseF x0 st
    SWhile f sts -> SWhileF f sts
    SBreak -> SBreakF
    SContinue -> SContinueF
    SFunction func al sts re -> SFunctionF func al sts re
    SPrint args -> SPrintF args
    SReject args -> SRejectF args
    SScoped sts -> SScopedF sts
    SContext mf sts -> SContextF mf sts

instance Functor (RS.Base (Stmt f)) => RS.Corecursive (Stmt f) where
  embed = \case
    SDeclareF txt st divf -> SDeclare txt st divf
    SDeclAssignF txt st divf fet -> SDeclAssign txt st divf fet
    SAssignF ft ft' -> SAssign ft ft'
    STargetF f -> STarget f
    SSampleF fst dis al -> SSample fst dis al
    SForF txt f f' sts -> SFor txt f f' sts
    SForEachF txt ft sts -> SForEach txt ft sts
    SIfElseF x0 st -> SIfElse x0 st
    SWhileF f sts -> SWhile f sts
    SBreakF -> SBreak
    SContinueF -> SContinue
    SFunctionF func al sts re -> SFunction func al sts re
    SPrintF args -> SPrint args
    SRejectF args -> SReject args
    SScopedF sts -> SScoped sts
    SContextF mf sts -> SContext mf sts



{-
instance TR.HFunctor StmtF where
  hfmap nat = \case
    SDeclare txt st divf -> SDeclare txt st (TR.hfmap nat divf)
    SDeclAssign txt st divf rhe -> SDeclAssign txt st (TR.hfmap nat divf) (nat rhe)
    SAssign lhe rhe -> SAssign (nat lhe) (nat rhe)
    STarget rhe -> STarget (nat rhe)
    SSample gst dis al -> SSample (nat gst) dis (TR.hfmap nat al)
    SFor txt se ee body -> SFor txt (nat se) (nat se) body
    SForEach txt gt body -> SForEach txt (nat gt) (body)
    SIfElse x0 sf -> SIfElse (firstF nat x0) sf
    SWhile g body -> SWhile (nat g) body
    SFunction func al body -> SFunction func al body
    SScope body -> SScope body

instance TR.HTraversable StmtF where
  htraverse natM = \case
    SDeclare txt st indexEs -> SDeclare txt st <$> TR.htraverse natM indexEs
    SDeclAssign txt st indexEs rhe -> SDeclAssign txt st <$> TR.htraverse natM indexEs <*> natM rhe
    SAssign lhe rhe -> SAssign <$> natM lhe <*> natM rhe
    STarget re -> STarget <$> natM re
    SSample ste dist al -> SSample <$> natM ste <*> pure dist <*> TR.htraverse natM al
    SFor txt se ee body -> SFor txt <$> natM se <*> natM ee <*> pure body
    SForEach txt at body -> SForEach txt <$> natM at <*> pure body
    SIfElse x0 sf -> SIfElse <$> traverse (\(c, s) -> (,) <$> natM c <*> pure s) x0 <*> pure sf
    SWhile cond body -> SWhile <$> natM cond <*> pure body
    SFunction func al body -> pure $ SFunction func al body
    SScope body -> pure $ SScope body
  hmapM = TR.htraverse


-}

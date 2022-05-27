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
  SFunction :: Function rt args -> ArgList (TR.K Text) args -> [Stmt r] -> Stmt r
  SScope :: [Stmt r] -> Stmt r

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
  SFunctionF :: Function rt args -> ArgList (TR.K Text) args -> [a] -> StmtF r a
  SScopeF :: [a] -> StmtF r a

type LStmt = Stmt LExpr

data UStmt :: Type where
  US :: UStmt -> UStmt
  CS :: (IndexLookupCtxt -> IndexLookupCtxt) -> Stmt UExpr -> UStmt

data UStmtF :: Type -> Type where
  USF :: a -> UStmtF a
  CSF :: (IndexLookupCtxt -> IndexLookupCtxt) -> Stmt UExpr -> UStmtF a

instance Functor UStmtF where
  fmap f = \case
    USF st -> USF (f st)
    CSF g a -> CSF g a

instance Foldable UStmtF where
  foldMap f = \case
    USF st -> f st
    CSF g a -> mempty

instance Traversable UStmtF where
  traverse g = \case
    USF st -> USF <$> g st
    CSF f a -> pure $ CSF f a

instance RS.Recursive UStmt where
  project = \case
    US st -> USF st
    CS f us -> CSF f us

instance RS.Corecursive UStmt where
  embed = \case
    USF st -> US st
    CSF f us -> CS f us



type instance RS.Base LStmt = StmtF LExpr
type instance RS.Base UStmt = UStmtF

type instance RS.Base (Stmt f) = StmtF f

data IndexLookupCtxt = IndexLookupCtxt { sizes :: Map SME.IndexKey (LExpr EInt), indices :: Map SME.IndexKey (LExpr EInt) }

data WithContext :: Type -> Type where
  Extant :: WithContext a -> WithContext a
  Change :: (IndexLookupCtxt -> IndexLookupCtxt) -> a -> WithContext a

data WithContextF :: Type -> Type -> Type where
  ExtantF :: b -> WithContextF a b
  ChangeF :: (IndexLookupCtxt -> IndexLookupCtxt) -> a -> WithContextF a b

type instance RS.Base (WithContext a) = WithContextF a

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
    SFunctionF func al sfs -> SFunctionF func al (f <$> sfs)
    SScopeF sfs -> SScopeF $ f <$> sfs

instance Foldable (StmtF f) where
  foldMap f = \case
    SDeclareF txt st divf -> mempty
    SDeclAssignF txt st divf fet -> mempty
    SAssignF ft ft' -> mempty
    STargetF f' -> mempty
    SSampleF fst dis al -> mempty
    SForF txt f' f3 body -> foldMap f body
    SForEachF txt ft body -> foldMap f body
    SIfElseF ifConds sf -> foldMap (f . snd) ifConds <> f sf
    SWhileF f' body -> foldMap f body
    SFunctionF func al body -> foldMap f body
    SScopeF body -> foldMap f body

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
    SFunctionF func al sfs -> SFunctionF func al <$> traverse g sfs
    SScopeF sfs -> SScopeF <$> traverse g sfs

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
    SFunction func al sts -> SFunctionF func al sts
    SScope sts -> SScopeF sts


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
    SFunctionF func al sts -> SFunction func al sts
    SScopeF sts -> SScope sts


instance Functor (WithContextF a) where
  fmap f = \case
    ExtantF wc -> ExtantF (f wc)
    ChangeF g a -> ChangeF g a

instance Foldable (WithContextF a) where
  foldMap f = \case
    ExtantF wc -> f wc
    ChangeF _ _ -> mempty

instance Traversable (WithContextF a) where
  traverse g = \case
    ExtantF wc -> ExtantF <$> g wc
    ChangeF f a -> pure $ ChangeF f a

instance RS.Recursive (WithContext a) where
  project = \case
    Extant st -> ExtantF st
    Change f st -> ChangeF f st

instance RS.Corecursive (WithContext a) where
  embed = \case
    ExtantF st -> Extant st
    ChangeF f st -> Change f st



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

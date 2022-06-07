{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE FlexibleContexts #-}
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
import qualified Stan.ModelBuilder.BuilderTypes as SBT
import qualified Data.Vec.Lazy as Vec

import Prelude hiding (Nat)
import Relude.Extra
import qualified Data.Map.Strict as Map

import qualified Data.Functor.Foldable as RS
import Stan.ModelBuilder (TransformedParametersBlock)

data DeclSpec t = DeclSpec (StanType t) (Vec (DeclDimension t) (UExpr EInt)) [VarModifier UExpr (InternalType t)]

intSpec :: [VarModifier UExpr EInt] -> DeclSpec EInt
intSpec = DeclSpec StanInt VNil

realSpec :: [VarModifier UExpr EReal] -> DeclSpec EReal
realSpec = DeclSpec StanReal VNil

complexSpec :: [VarModifier UExpr EComplex] -> DeclSpec EComplex
complexSpec = DeclSpec StanComplex VNil

vectorSpec :: UExpr EInt -> [VarModifier UExpr EReal] -> DeclSpec ECVec
vectorSpec ie = DeclSpec StanVector (ie ::: VNil)

matrixSpec :: UExpr EInt -> UExpr EInt -> [VarModifier UExpr EReal] -> DeclSpec EMat
matrixSpec re ce = DeclSpec StanMatrix (re ::: ce ::: VNil)

arraySpec :: SNat n -> Vec n (UExpr EInt) -> DeclSpec t -> DeclSpec (EArray n t)
arraySpec n arrIndices (DeclSpec t tIndices vms) = DeclSpec (StanArray n t) (arrIndices Vec.++ tIndices) vms

-- functions for ease of use and exporting.  Monomorphised to UStmt, etc.
declare' :: Text -> StanType t -> Vec (DeclDimension t) (UExpr EInt) -> [VarModifier UExpr (InternalType t)] -> UStmt
declare' vn vt iDecls = SDeclare vn vt (DeclIndexVecF iDecls)

declare :: Text -> DeclSpec t -> UStmt
declare vn (DeclSpec st indices vms) = declare' vn st indices vms

declareAndAssign' :: Text -> StanType t -> Vec (DeclDimension t) (UExpr EInt) -> [VarModifier UExpr (InternalType t)] -> UExpr t -> UStmt
declareAndAssign' vn vt iDecls vms = SDeclAssign vn vt (DeclIndexVecF iDecls) vms

declareAndAssign :: Text -> DeclSpec t -> UExpr t -> UStmt
declareAndAssign vn (DeclSpec vt indices vms) = declareAndAssign' vn vt indices vms

addToTarget :: UExpr EReal -> UStmt
addToTarget = STarget

assign :: UExpr t -> UExpr t -> UStmt
assign = SAssign

sample :: UExpr t -> Density t args -> ArgList UExpr args -> UStmt
sample = SSample

data ForType t = SpecificNumbered (UExpr EInt) (UExpr EInt)
               | IndexedLoop SME.IndexKey
               | SpecificIn (UExpr t)
               | IndexedIn SME.IndexKey (UExpr t)

for :: Text -> ForType t -> NonEmpty UStmt -> UStmt
for loopCounter ft body = case ft of
  SpecificNumbered se' ee' -> SFor loopCounter se' ee' body
  IndexedLoop ik -> SFor loopCounter (intE 1) (namedSizeE ik) $ bodyWithLoopCounterContext ik
  SpecificIn e -> SForEach loopCounter e body
  IndexedIn ik e -> SForEach loopCounter e $ bodyWithLoopCounterContext ik
  where
    bodyWithLoopCounterContext ik = SContext (Just $ insertUseBinding ik (lNamedE loopCounter SInt)) body :| []

ifThen :: UExpr EBool -> UStmt -> UStmt -> UStmt
ifThen ce sTrue = SIfElse $ (ce, sTrue) :| []

ifThenElse :: NonEmpty (UExpr EBool, UStmt) -> UStmt -> UStmt
ifThenElse = SIfElse

while :: UExpr EBool -> NonEmpty UStmt -> UStmt
while = SWhile

break :: UStmt
break = SBreak

continue :: UStmt
continue = SContinue

function :: Function rt args -> ArgList (FuncArg Text) args -> (ArgList UExpr args -> (NonEmpty UStmt, UExpr rt)) -> UStmt
function fd argNames bodyF = SFunction fd argNames bodyS ret
  where
    argTypes = argTypesToArgListOfTypes $ functionArgTypes fd
    argExprs = zipArgListsWith (namedE . funcArgName) argNames argTypes
    (bodyS, ret) = bodyF argExprs

comment :: NonEmpty Text -> UStmt
comment = SComment

print :: ArgList UExpr args -> UStmt
print = SPrint

reject :: ArgList UExpr args -> UStmt
reject = SReject

scoped :: NonEmpty UStmt -> UStmt
scoped = SScoped

context :: (IndexLookupCtxt -> IndexLookupCtxt) -> NonEmpty UStmt -> UStmt
context cf = SContext (Just cf)

insertUseBinding :: SME.IndexKey -> LExpr EInt -> IndexLookupCtxt -> IndexLookupCtxt
insertUseBinding k ie (IndexLookupCtxt a b) = IndexLookupCtxt a (Map.insert k ie b)

insertSizeBinding :: SME.IndexKey -> LExpr EInt -> IndexLookupCtxt -> IndexLookupCtxt
insertSizeBinding k ie (IndexLookupCtxt a b) = IndexLookupCtxt (Map.insert k ie a) b

data VarModifier :: (EType -> Type) -> EType -> Type where
  VarLower :: r t -> VarModifier r t
  VarUpper :: r t -> VarModifier r t
  VarOffset :: r t -> VarModifier r t
  VarMultiplier :: r t -> VarModifier r t

lowerM :: UExpr t -> VarModifier UExpr t
lowerM = VarLower

upperM :: UExpr t -> VarModifier UExpr t
upperM = VarUpper

offsetM :: UExpr t -> VarModifier UExpr t
offsetM = VarOffset

multiplierM :: UExpr t -> VarModifier UExpr t
multiplierM = VarMultiplier


instance TR.HFunctor VarModifier where
  hfmap f = \case
    VarLower x -> VarLower $ f x
    VarUpper x -> VarUpper $ f x
    VarOffset x -> VarOffset $ f x
    VarMultiplier x -> VarMultiplier $ f x

instance TR.HTraversable VarModifier where
  htraverse nat = \case
    VarLower x -> VarLower <$> nat x
    VarUpper x -> VarUpper <$> nat x
    VarOffset x -> VarOffset <$> nat x
    VarMultiplier x -> VarMultiplier <$> nat x
  hmapM = TR.htraverse

data StmtBlock = FunctionsStmts
               | DataStmts
               | TDataStmts
               | ParametersStmts
               | TParametersStmts
               | ModelStmts
               | GeneratedQuantitiesStmts

-- Statements
data Stmt :: (EType -> Type) -> Type where
  SDeclare ::  Text -> StanType et -> DeclIndexVecF r et -> [VarModifier r (InternalType et)] -> Stmt r
  SDeclAssign :: Text -> StanType et -> DeclIndexVecF r et -> [VarModifier r (InternalType et)] -> r et -> Stmt r
  SAssign :: r t -> r t -> Stmt r
  STarget :: r EReal -> Stmt r
  SSample :: r st -> Density st args -> ArgList r args -> Stmt r
  SFor :: Text -> r EInt -> r EInt -> NonEmpty (Stmt r) -> Stmt r
  SForEach :: Text -> r t -> NonEmpty (Stmt r) -> Stmt r
  SIfElse :: NonEmpty (r EBool, Stmt r) -> Stmt r -> Stmt r -- [(condition, ifTrue)] -> ifAllFalse
  SWhile :: r EBool -> NonEmpty (Stmt r) -> Stmt r
  SBreak :: Stmt r
  SContinue :: Stmt r
  SFunction :: Function rt args -> ArgList (FuncArg Text) args -> NonEmpty (Stmt r) -> r rt -> Stmt r
  SComment :: NonEmpty Text -> Stmt r
  SPrint :: ArgList r args -> Stmt r
  SReject :: ArgList r args -> Stmt r
  SScoped :: NonEmpty (Stmt r) -> Stmt r
  SBlock :: StmtBlock -> [Stmt r] -> Stmt r
  SContext :: Maybe (IndexLookupCtxt -> IndexLookupCtxt) -> NonEmpty (Stmt r) -> Stmt r

data StmtF :: (EType -> Type) -> Type -> Type where
  SDeclareF ::  Text -> StanType et -> DeclIndexVecF r et -> [VarModifier r (InternalType et)] -> StmtF r a
  SDeclAssignF :: Text -> StanType et -> DeclIndexVecF r et -> [VarModifier r (InternalType et)] -> r et -> StmtF r a
  SAssignF :: r t -> r t -> StmtF r a
  STargetF :: r EReal -> StmtF r a
  SSampleF :: r st -> Density st args -> ArgList r args -> StmtF r a
  SForF :: Text -> r EInt -> r EInt -> NonEmpty a -> StmtF r a
  SForEachF :: Text -> r t -> NonEmpty a -> StmtF r a
  SIfElseF :: NonEmpty (r EBool, a) -> a -> StmtF r a -- [(condition, ifTrue)] -> ifAllFalse
  SWhileF :: r EBool -> NonEmpty a -> StmtF r a
  SBreakF :: StmtF r a
  SContinueF :: StmtF r a
  SFunctionF :: Function rt args -> ArgList (FuncArg Text) args -> NonEmpty a -> r rt -> StmtF r a
  SCommentF :: NonEmpty Text -> StmtF r a
  SPrintF :: ArgList r args -> StmtF r a
  SRejectF :: ArgList r args -> StmtF r a
  SScopedF :: NonEmpty a -> StmtF r a
  SBlockF :: StmtBlock -> [a] -> StmtF r a
  SContextF :: Maybe (IndexLookupCtxt -> IndexLookupCtxt) -> NonEmpty a -> StmtF r a

type instance RS.Base (Stmt f) = StmtF f

type LStmt = Stmt LExpr
type UStmt = Stmt UExpr

data IndexLookupCtxt = IndexLookupCtxt { sizes :: Map SME.IndexKey (LExpr EInt), indices :: Map SME.IndexKey (LExpr EInt) }

instance Functor (StmtF f) where
  fmap f x = case x of
    SDeclareF txt st divf vms -> SDeclareF txt st divf vms
    SDeclAssignF txt st divf vms rhse -> SDeclAssignF txt st divf vms rhse
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
    SCommentF t -> SCommentF t
    SPrintF args -> SPrintF args
    SRejectF args -> SRejectF args
    SScopedF sfs -> SScopedF $ f <$> sfs
    SBlockF bl stmts -> SBlockF bl (f <$> stmts)
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
    SCommentF _ -> mempty
    SPrintF _ -> mempty
    SRejectF _ -> mempty
    SScopedF body -> foldMap f body
    SBlockF _ body -> foldMap f body
    SContextF _ body -> foldMap f body

instance Traversable (StmtF f) where
  traverse g = \case
    SDeclareF txt st divf vms -> pure $ SDeclareF txt st divf vms
    SDeclAssignF txt st divf vms fet -> pure $ SDeclAssignF txt st divf vms fet
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
    SCommentF t -> pure $ SCommentF t
    SPrintF args -> pure $ SPrintF args
    SRejectF args -> pure $ SRejectF args
    SScopedF sfs -> SScopedF <$> traverse g sfs
    SBlockF bl stmts -> SBlockF bl <$> traverse g stmts
    SContextF mf sfs -> SContextF mf <$> traverse g sfs

instance Functor (RS.Base (Stmt f)) => RS.Recursive (Stmt f) where
  project = \case
    SDeclare txt st divf vms -> SDeclareF txt st divf vms
    SDeclAssign txt st divf vms fet -> SDeclAssignF txt st divf vms fet
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
    SComment t -> SCommentF t
    SPrint args -> SPrintF args
    SReject args -> SRejectF args
    SScoped sts -> SScopedF sts
    SBlock bl sts -> SBlockF bl sts
    SContext mf sts -> SContextF mf sts

instance Functor (RS.Base (Stmt f)) => RS.Corecursive (Stmt f) where
  embed = \case
    SDeclareF txt st divf vms -> SDeclare txt st divf vms
    SDeclAssignF txt st divf vms fet -> SDeclAssign txt st divf vms fet
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
    SCommentF t -> SComment t
    SPrintF args -> SPrint args
    SRejectF args -> SReject args
    SScopedF sts -> SScoped sts
    SBlockF bl sts -> SBlock bl sts
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

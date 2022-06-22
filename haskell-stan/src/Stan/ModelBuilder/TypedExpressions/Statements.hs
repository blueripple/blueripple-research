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
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Stan.ModelBuilder.TypedExpressions.Statements
  (
    module Stan.ModelBuilder.TypedExpressions.Statements
  , module Stan.ModelBuilder.TypedExpressions.Expressions
  )
  where

import qualified Stan.ModelBuilder.TypedExpressions.Recursion as TR
import Stan.ModelBuilder.TypedExpressions.Expressions
import Stan.ModelBuilder.TypedExpressions.Types
import Stan.ModelBuilder.TypedExpressions.TypedList
import Stan.ModelBuilder.TypedExpressions.Indexing
import Stan.ModelBuilder.TypedExpressions.Operations
import Stan.ModelBuilder.TypedExpressions.Functions
import Stan.ModelBuilder.TypedExpressions.StanFunctions
import qualified Data.Vec.Lazy as Vec

import Control.Monad.Writer.Strict as W

import Prelude hiding (Nat)
import Relude.Extra
import qualified Data.Map.Strict as Map

import qualified Data.Functor.Foldable as RS
--import qualified Stan.ModelBuilder.Expressions as SB

type StanName = Text

data DeclSpec t = DeclSpec (StanType t) (Vec (DeclDimension t) (UExpr EInt)) [VarModifier UExpr (ScalarType t)]

data NamedDeclSpec t = NamedDeclSpec StanName (DeclSpec t)

declName :: NamedDeclSpec t -> StanName
declName (NamedDeclSpec n _) = n

decl :: NamedDeclSpec t -> DeclSpec t
decl (NamedDeclSpec _ ds) = ds

declType :: DeclSpec t -> StanType t
declType (DeclSpec st _ _) = st

declDims :: DeclSpec t -> Vec (DeclDimension t) (UExpr EInt)
declDims (DeclSpec _ dims _) = dims

intSpec :: [VarModifier UExpr EInt] -> DeclSpec EInt
intSpec = DeclSpec StanInt VNil

realSpec :: [VarModifier UExpr EReal] -> DeclSpec EReal
realSpec = DeclSpec StanReal VNil

complexSpec :: [VarModifier UExpr EComplex] -> DeclSpec EComplex
complexSpec = DeclSpec StanComplex VNil

vectorSpec :: UExpr EInt -> [VarModifier UExpr EReal] -> DeclSpec ECVec
vectorSpec ie = DeclSpec StanVector (ie ::: VNil)

orderedSpec :: UExpr EInt -> [VarModifier UExpr EReal] -> DeclSpec ECVec
orderedSpec ie = DeclSpec StanOrdered (ie ::: VNil)

positiveOrderedSpec :: UExpr EInt -> [VarModifier UExpr EReal] -> DeclSpec ECVec
positiveOrderedSpec ie = DeclSpec StanPositiveOrdered (ie ::: VNil)

simplexSpec :: UExpr EInt -> [VarModifier UExpr EReal] -> DeclSpec ECVec
simplexSpec ie = DeclSpec StanSimplex (ie ::: VNil)

unitVectorSpec :: UExpr EInt -> [VarModifier UExpr EReal] -> DeclSpec ECVec
unitVectorSpec ie = DeclSpec StanUnitVector (ie ::: VNil)

rowVectorSpec :: UExpr EInt -> [VarModifier UExpr EReal] -> DeclSpec ERVec
rowVectorSpec ie = DeclSpec StanRowVector (ie ::: VNil)

matrixSpec :: UExpr EInt -> UExpr EInt -> [VarModifier UExpr EReal] -> DeclSpec EMat
matrixSpec re ce = DeclSpec StanMatrix (re ::: ce ::: VNil)

corrMatrixSpec :: UExpr EInt -> [VarModifier UExpr EReal] -> DeclSpec ESqMat
corrMatrixSpec rce = DeclSpec StanCorrMatrix (rce ::: VNil)

covMatrixSpec :: UExpr EInt -> [VarModifier UExpr EReal] -> DeclSpec ESqMat
covMatrixSpec rce = DeclSpec StanCovMatrix (rce ::: VNil)

choleskyFactorCorrSpec :: UExpr EInt -> [VarModifier UExpr EReal] -> DeclSpec ESqMat
choleskyFactorCorrSpec rce = DeclSpec StanCholeskyFactorCorr (rce ::: VNil)

choleskyFactorCovSpec :: UExpr EInt -> [VarModifier UExpr EReal] -> DeclSpec ESqMat
choleskyFactorCovSpec rce = DeclSpec StanCholeskyFactorCov (rce ::: VNil)

arraySpec :: SNat n -> Vec n (UExpr EInt) -> DeclSpec t -> DeclSpec (EArray n t)
arraySpec n arrIndices (DeclSpec t tIndices vms) = DeclSpec (StanArray n t) (arrIndices Vec.++ tIndices) vms

intArraySpec :: UExpr EInt -> [VarModifier UExpr EInt] -> DeclSpec EIndexArray
intArraySpec se cs = arraySpec s1 (se ::: VNil) (intSpec cs)

indexArraySpec :: UExpr EInt -> [VarModifier UExpr EInt] -> DeclSpec EIndexArray
indexArraySpec = intArraySpec


-- functions for ease of use and exporting.  Monomorphised to UStmt, etc.
declare' :: Text -> StanType t -> Vec (DeclDimension t) (UExpr EInt) -> [VarModifier UExpr (ScalarType t)] -> UStmt
declare' vn vt iDecls = SDeclare vn vt (DeclIndexVecF iDecls)

declare :: Text -> DeclSpec t -> UStmt
declare vn (DeclSpec st indices vms) = declare' vn st indices vms

declareN :: NamedDeclSpec t -> UStmt
declareN (NamedDeclSpec n ds) = declare n ds

declareAndAssign' :: Text -> StanType t -> Vec (DeclDimension t) (UExpr EInt) -> [VarModifier UExpr (ScalarType t)] -> UExpr t -> UStmt
declareAndAssign' vn vt iDecls vms = SDeclAssign vn vt (DeclIndexVecF iDecls) vms

declareAndAssign :: Text -> DeclSpec t -> UExpr t -> UStmt
declareAndAssign vn (DeclSpec vt indices vms) = declareAndAssign' vn vt indices vms

declareAndAssignN :: NamedDeclSpec t -> UExpr t -> UStmt
declareAndAssignN (NamedDeclSpec vn (DeclSpec vt indices vms)) = declareAndAssign' vn vt indices vms

addToTarget :: UExpr EReal -> UStmt
addToTarget = STarget

assign :: UExpr t -> UExpr t -> UStmt
assign = SAssign

-- doing it this way avoids using Stans += syntax.  I just expand.
-- to do otherwise I would have to add a constructor to Stmt
opAssign :: (ta ~ BinaryResultT bop ta tb) => SBinaryOp bop -> UExpr ta -> UExpr tb -> UStmt
opAssign op ea eb = assign ea $ binaryOpE op ea eb

plusEq :: (ta ~ BinaryResultT BAdd ta tb) => UExpr ta -> UExpr tb -> UStmt
plusEq = opAssign SAdd

divEq :: (ta ~ BinaryResultT BDivide ta tb) => UExpr ta -> UExpr tb -> UStmt
divEq = opAssign SDivide

data DensityWithArgs g where
  DensityWithArgs :: Density g args -> TypedList UExpr args -> DensityWithArgs g

withDWA :: (forall args.Density g args -> TypedList UExpr args -> r) -> DensityWithArgs g -> r
withDWA f (DensityWithArgs d args) = f d args

target :: UExpr EReal -> UStmt
target = STarget

sample :: UExpr t -> Density t args -> TypedList UExpr args -> UStmt
sample = SSample

sampleW :: UExpr t -> DensityWithArgs t  -> UStmt
sampleW ue (DensityWithArgs d al)= SSample ue d al


data ForType t = SpecificNumbered (UExpr EInt) (UExpr EInt)
               | IndexedLoop IndexKey
               | SpecificIn (UExpr t)
               | IndexedIn IndexKey (UExpr t)

for :: Traversable f => Text -> ForType t -> (UExpr EInt -> f UStmt) -> UStmt
for loopCounter ft bodyF = case ft of
  SpecificNumbered se' ee' -> SFor loopCounter se' ee' $ bodyF loopCounterE
  IndexedLoop ik -> SFor loopCounter (intE 1) (namedSizeE ik) $ bodyF loopCounterE --bodyWithLoopCounterContext ik
  SpecificIn e -> SForEach loopCounter e $ bodyF loopCounterE
  IndexedIn ik e -> SForEach loopCounter e $ bodyF loopCounterE --bodyWithLoopCounterContext ik
  where
    loopCounterE = namedE loopCounter SInt
--    bodyWithLoopCounterContext ik = SContext (Just $ insertUseBinding ik (lNamedE loopCounter SInt)) body :| []

ifThen :: UExpr EBool -> UStmt -> UStmt -> UStmt
ifThen ce sTrue = SIfElse $ (ce, sTrue) :| []

ifThenElse :: NonEmpty (UExpr EBool, UStmt) -> UStmt -> UStmt
ifThenElse = SIfElse

while :: Traversable f => UExpr EBool -> f UStmt -> UStmt
while = SWhile

break :: UStmt
break = SBreak

continue :: UStmt
continue = SContinue

function :: Traversable f => Function rt args -> TypedList (FuncArg Text) args -> (TypedList UExpr args -> (f UStmt, UExpr rt)) -> UStmt
function fd argNames bodyF = SFunction fd argNames bodyS ret
  where
    argTypes = typeListToTypedListOfTypes $ functionArgTypes fd
    argExprs = zipTypedListsWith (namedE . funcArgName) argNames argTypes
    (bodyS, ret) = bodyF argExprs

simpleFunctionBody :: Function rt pts
                   -> StanName
                   -> (ExprList pts -> DeclSpec rt)
                   -> (UExpr rt -> ExprList pts -> [UStmt])
                   -> ExprList pts
                   -> (NonEmpty UStmt, UExpr rt)
simpleFunctionBody f n retDSF bF args = let rE = namedE n st in  (declare n (retDSF args) :| bF rE args, rE)
  where
    st = sTypeFromStanType $ declType $ retDSF args

comment :: NonEmpty Text -> UStmt
comment = SComment

profile :: Text -> UStmt
profile = SProfile

print :: TypedList UExpr args -> UStmt
print = SPrint

reject :: TypedList UExpr args -> UStmt
reject = SReject

scoped :: Traversable f => f UStmt -> UStmt
scoped = SScoped

context :: Traversable f => (IndexLookupCtxt -> IndexLookupCtxt) -> f UStmt -> UStmt
context cf = SContext (Just cf)

insertIndexBinding :: IndexKey -> LExpr EIndexArray -> IndexLookupCtxt -> IndexLookupCtxt
insertIndexBinding k ie (IndexLookupCtxt a b) = IndexLookupCtxt a (Map.insert k ie b)

insertSizeBinding :: IndexKey -> LExpr EInt -> IndexLookupCtxt -> IndexLookupCtxt
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

newtype CodeWriter a = CodeWriter { unCodeWriter :: W.Writer [UStmt] a } deriving (Functor, Applicative, Monad, W.MonadWriter [UStmt])

{-
writerNE :: CodeWriter a -> Maybe (NonEmpty UStmt, a)
writerNE cw = do
  let (stmts, a) = writerL cw
  stmtsNE <- nonEmpty stmts
  return (stmtsNE, a)

writerNE' :: CodeWriter a -> Maybe (NonEmpty UStmt)
writerNE' = fmap fst . writerNE
-}

writerL :: CodeWriter a -> ([UStmt], a)
writerL (CodeWriter w) = (stmts, a)
  where (a, stmts) = W.runWriter w

writerL' :: CodeWriter a -> [UStmt]
writerL' = fst . writerL

addStmt :: UStmt -> CodeWriter ()
addStmt = W.tell . pure

declareW :: Text -> DeclSpec t -> CodeWriter (UExpr t)
declareW t ds = do
  addStmt $ declare t ds
  return $ namedE t (sTypeFromStanType $ declType ds)

declareNW :: NamedDeclSpec t -> CodeWriter (UExpr t)
declareNW nds = do
  addStmt $ declareN nds
  return $ namedE (declName nds) (sTypeFromStanType $ declType $ decl nds)

declareRHSW :: Text -> DeclSpec t -> UExpr t -> CodeWriter (UExpr t)
declareRHSW t ds rhs = do
  addStmt $ declareAndAssign t ds rhs
  return $ namedE t (sTypeFromStanType $ declType ds)

declareRHSNW :: NamedDeclSpec t -> UExpr t -> CodeWriter (UExpr t)
declareRHSNW nds rhs = do
  addStmt $ declareAndAssignN nds rhs
  return $ namedE (declName nds) (sTypeFromStanType $ declType $ decl nds)


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
  SDeclare ::  Text -> StanType et -> DeclIndexVecF r et -> [VarModifier r (ScalarType et)] -> Stmt r
  SDeclAssign :: Text -> StanType et -> DeclIndexVecF r et -> [VarModifier r (ScalarType et)] -> r et -> Stmt r
  SAssign :: r t -> r t -> Stmt r
  STarget :: r EReal -> Stmt r
  SSample :: r st -> Density st args -> TypedList r args -> Stmt r
  SFor :: Traversable f => Text -> r EInt -> r EInt -> f (Stmt r) -> Stmt r
  SForEach :: Traversable f => Text -> r t -> f (Stmt r) -> Stmt r
  SIfElse :: NonEmpty (r EBool, Stmt r) -> Stmt r -> Stmt r -- [(condition, ifTrue)] -> ifAllFalse
  SWhile :: Traversable f => r EBool -> f (Stmt r) -> Stmt r
  SBreak :: Stmt r
  SContinue :: Stmt r
  SFunction :: Traversable f => Function rt args -> TypedList (FuncArg Text) args -> f (Stmt r) -> r rt -> Stmt r
  SComment :: Traversable f => f Text -> Stmt r
  SProfile :: Text -> Stmt r
  SPrint :: TypedList r args -> Stmt r
  SReject :: TypedList r args -> Stmt r
  SScoped :: Traversable f => f (Stmt r) -> Stmt r
  SBlock :: Traversable f => StmtBlock -> f (Stmt r) -> Stmt r
  SContext :: Traversable f => Maybe (IndexLookupCtxt -> IndexLookupCtxt) -> f (Stmt r) -> Stmt r

data StmtF :: (EType -> Type) -> Type -> Type where
  SDeclareF ::  Text -> StanType et -> DeclIndexVecF r et -> [VarModifier r (ScalarType et)] -> StmtF r a
  SDeclAssignF :: Text -> StanType et -> DeclIndexVecF r et -> [VarModifier r (ScalarType et)] -> r et -> StmtF r a
  SAssignF :: r t -> r t -> StmtF r a
  STargetF :: r EReal -> StmtF r a
  SSampleF :: r st -> Density st args -> TypedList r args -> StmtF r a
  SForF :: Traversable f => Text -> r EInt -> r EInt -> f a -> StmtF r a
  SForEachF :: Traversable f => Text -> r t -> f a -> StmtF r a
  SIfElseF :: NonEmpty (r EBool, a) -> a -> StmtF r a -- [(condition, ifTrue)] -> ifAllFalse
  SWhileF :: Traversable f => r EBool -> f a -> StmtF r a
  SBreakF :: StmtF r a
  SContinueF :: StmtF r a
  SFunctionF :: Traversable f => Function rt args -> TypedList (FuncArg Text) args -> f a -> r rt -> StmtF r a
  SCommentF :: Traversable f => f Text -> StmtF r a
  SProfileF :: Text -> StmtF r a
  SPrintF :: TypedList r args -> StmtF r a
  SRejectF :: TypedList r args -> StmtF r a
  SScopedF :: Traversable f => f a -> StmtF r a
  SBlockF :: Traversable f => StmtBlock -> f a -> StmtF r a
  SContextF :: Traversable f => Maybe (IndexLookupCtxt -> IndexLookupCtxt) -> f a -> StmtF r a

type instance RS.Base (Stmt f) = StmtF f

type LStmt = Stmt LExpr
type UStmt = Stmt UExpr
type IndexArrayU = UExpr (EArray (S Z) EInt)
type IndexArrayL = LExpr (EArray (S Z) EInt)
--type IndexKey = Text
type IndexSizeMap = Map IndexKey (LExpr EInt)
type IndexArrayMap = Map IndexKey IndexArrayL

indexSize :: IndexArrayU -> UExpr EInt
indexSize = functionE array_num_elements . oneTyped

data IndexLookupCtxt = IndexLookupCtxt { sizes :: IndexSizeMap, indexes :: IndexArrayMap }

emptyLookupCtxt :: IndexLookupCtxt
emptyLookupCtxt = IndexLookupCtxt mempty mempty


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
    SProfileF t -> SProfileF t
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
    STargetF {} -> mempty
    SSampleF {} -> mempty
    SForF _ _ _ body -> foldMap f body
    SForEachF _ _ body -> foldMap f body
    SIfElseF ifConds sf -> foldMap (f . snd) ifConds <> f sf
    SWhileF _ body -> foldMap f body
    SBreakF -> mempty
    SContinueF -> mempty
    SFunctionF _ _ body _ -> foldMap f body
    SCommentF _ -> mempty
    SProfileF _ -> mempty
    SPrintF {} -> mempty
    SRejectF {} -> mempty
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
    SProfileF t -> pure $ SProfileF t
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
    SProfile t -> SProfileF t
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
    SProfileF t -> SProfile t
    SPrintF args -> SPrint args
    SRejectF args -> SReject args
    SScopedF sts -> SScoped sts
    SBlockF bl sts -> SBlock bl sts
    SContextF mf sts -> SContext mf sts

instance TR.HFunctor StmtF where
  hfmap nat = \case
    SDeclareF txt st divf vms -> SDeclareF txt st (TR.hfmap nat divf) (fmap (TR.hfmap nat) vms)
    SDeclAssignF txt st divf vms rhe -> SDeclAssignF txt st (TR.hfmap nat divf) (fmap (TR.hfmap nat) vms) (nat rhe)
    SAssignF lhe rhe -> SAssignF (nat lhe) (nat rhe)
    STargetF rhe -> STargetF (nat rhe)
    SSampleF gst dis al -> SSampleF (nat gst) dis (TR.hfmap nat al)
    SForF txt se ee body -> SForF txt (nat se) (nat ee) body
    SForEachF txt gt body -> SForEachF txt (nat gt) body
    SIfElseF x0 sf -> SIfElseF (firstF nat x0) sf
    SWhileF g body -> SWhileF (nat g) body
    SBreakF -> SBreakF
    SContinueF -> SContinueF
    SFunctionF func al body re -> SFunctionF func al body (nat re)
    SCommentF x -> SCommentF x
    SProfileF x -> SProfileF x
    SPrintF args -> SPrintF (TR.hfmap nat args)
    SRejectF args -> SRejectF (TR.hfmap nat args)
    SScopedF body -> SScopedF body
    SBlockF bl body -> SBlockF bl body
    SContextF mf body -> SContextF mf body

instance TR.HTraversable StmtF where
  htraverse natM = \case
    SDeclareF txt st indexEs vms -> SDeclareF txt st <$> TR.htraverse natM indexEs <*> traverse (TR.htraverse natM) vms
    SDeclAssignF txt st indexEs vms rhe -> SDeclAssignF txt st <$> TR.htraverse natM indexEs <*> traverse (TR.htraverse natM) vms <*> natM rhe
    SAssignF lhe rhe -> SAssignF <$> natM lhe <*> natM rhe
    STargetF re -> STargetF <$> natM re
    SSampleF ste dist al -> SSampleF <$> natM ste <*> pure dist <*> TR.htraverse natM al
    SForF txt se ee body -> SForF txt <$> natM se <*> natM ee <*> pure body
    SForEachF txt at body -> SForEachF txt <$> natM at <*> pure body
    SIfElseF x0 sf -> SIfElseF <$> traverse (\(c, s) -> (,) <$> natM c <*> pure s) x0 <*> pure sf
    SWhileF cond body -> SWhileF <$> natM cond <*> pure body
    SBreakF -> pure SBreakF
    SContinueF -> pure SContinueF
    SFunctionF func al body re -> SFunctionF func al body <$> natM re
    SCommentF x -> pure $ SCommentF x
    SProfileF x -> pure $ SProfileF x
    SPrintF args -> SPrintF <$> TR.htraverse natM args
    SRejectF args -> SRejectF <$> TR.htraverse natM args
    SScopedF body -> pure $ SScopedF body
    SBlockF bl body -> pure $ SBlockF bl body
    SContextF mf body -> pure $ SContextF mf body
  hmapM = TR.htraverse

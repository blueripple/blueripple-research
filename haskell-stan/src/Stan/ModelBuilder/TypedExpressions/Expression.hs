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

module Stan.ModelBuilder.TypedExpressions.Expression
  (
    module Stan.ModelBuilder.TypedExpressions.Expression
  )
  where

import qualified Stan.ModelBuilder.TypedExpressions.Recursion as TR
import Stan.ModelBuilder.TypedExpressions.Types
import Stan.ModelBuilder.TypedExpressions.Indexing
import Stan.ModelBuilder.TypedExpressions.Operations
import Stan.ModelBuilder.TypedExpressions.Functions
import qualified Stan.ModelBuilder.Expressions as SME
import Prelude hiding (Nat)
import Relude.Extra
import qualified Data.Map.Strict as Map
import Data.Vector.Generic (unsafeCopy)

--data Block f = Block { blockName :: Text, statements :: [Stmt f]}

-- Statements
data StmtF :: (EType -> Type) -> Type -> Type where
  SDeclare ::  Text -> StanType et -> DeclIndexVecF r et -> StmtF r a
  SDeclAssign :: Text -> StanType et -> DeclIndexVecF r et -> r et -> StmtF r a
  SAssign :: r t -> r t -> StmtF r a
  STarget :: r EReal -> StmtF r a
  SSample :: r st -> Distribution st args -> ArgList r args -> StmtF r a
  SFor :: Text -> r EInt -> r EInt -> [StmtF r a] -> StmtF r a
  SForEach :: Text -> r t -> [StmtF r a] -> StmtF r a
  SIfElse :: [(r EBool, StmtF r a)] -> StmtF r a -> StmtF r a -- [(condition, ifTrue)] -> ifAllFalse
  SWhile :: r EBool -> [StmtF r a] -> StmtF r a
  SFunction :: Function rt args -> ArgList (TR.K Text) args -> [StmtF r a] -> StmtF r a
  SScope :: [StmtF r a] -> StmtF r a

instance Functor (StmtF f) where
  fmap f x = case x of
    SDeclare txt st divf -> SDeclare txt st divf
    SDeclAssign txt st divf fet -> SDeclAssign txt st divf fet
    SAssign ft ft' -> SAssign ft ft'
    STarget f' -> STarget f'
    SSample fst dis al -> SSample fst dis al
    SFor ctr startE endE body -> SFor ctr startE endE (fmap f <$> body)
    SForEach ctr fromE body -> SForEach ctr fromE (fmap f <$> body)
    SIfElse x1 sf -> SIfElse (secondF (fmap f) x1) (fmap f sf)
    SWhile cond sfs -> SWhile cond (fmap f <$> sfs)
    SFunction func al sfs -> SFunction func al (fmap f <$> sfs)
    SScope sfs -> SScope $ fmap f <$> sfs

instance Foldable (StmtF f) where
  foldMap f = \case
    SDeclare txt st divf -> mempty
    SDeclAssign txt st divf fet -> mempty
    SAssign ft ft' -> mempty
    STarget f' -> mempty
    SSample fst dis al -> mempty
    SFor txt f' f3 body -> mconcat $ fmap (foldMap f) body
    SForEach txt ft body -> mconcat $ fmap (foldMap f) body
    SIfElse ifConds sf -> mconcat (fmap (foldMap f . snd) ifConds) <> foldMap f sf
    SWhile f' body -> mconcat $ fmap (foldMap f) body
    SFunction func al body -> mconcat $ fmap (foldMap f) body
    SScope body -> mconcat $ fmap (foldMap f) body

instance Traversable (StmtF f) where
  traverse g = \case
    SDeclare txt st divf -> pure $ SDeclare txt st divf
    SDeclAssign txt st divf fet -> pure $ SDeclAssign txt st divf fet
    SAssign ft ft' -> pure $ SAssign ft ft'
    STarget f -> pure $ STarget f
    SSample fst dis al -> pure $ SSample fst dis al
    SFor txt f f' sfs -> SFor txt f f' <$> traverse (traverse g) sfs
    SForEach txt ft sfs -> SForEach txt ft <$> traverse (traverse g) sfs
    SIfElse x0 sf -> SIfElse <$> traverse (\(c, s) -> pure ((,) c) <*> traverse g s) x0 <*> traverse g sf
    SWhile f body -> SWhile f <$> traverse (traverse g) body
    SFunction func al sfs -> SFunction func al <$> traverse (traverse g) sfs
    SScope sfs -> SScope <$> traverse (traverse g) sfs

instance TR.HFunctor StmtF where
  hfmap nat = \case
    SDeclare txt st divf -> SDeclare txt st (TR.hfmap nat divf)
    SDeclAssign txt st divf rhe -> SDeclAssign txt st (TR.hfmap nat divf) (nat rhe)
    SAssign lhe rhe -> SAssign (nat lhe) (nat rhe)
    STarget rhe -> STarget (nat rhe)
    SSample gst dis al -> SSample (nat gst) dis (TR.hfmap nat al)
    SFor txt se ee body -> SFor txt (nat se) (nat se) (TR.hfmap nat <$> body)
    SForEach txt gt body -> SForEach txt (nat gt) (TR.hfmap nat <$> body)
    SIfElse x0 sf -> SIfElse (fmap (\(c, s)  -> (nat c, TR.hfmap nat s)) x0) (TR.hfmap nat sf)
    SWhile g body -> SWhile (nat g) (TR.hfmap nat <$> body)
    SFunction func al body -> SFunction func al (TR.hfmap nat <$> body)
    SScope body -> SScope (TR.hfmap nat <$> body)

instance TR.HTraversable StmtF where
  htraverse natM = \case
    SDeclare txt st indexEs -> SDeclare txt st <$> TR.htraverse natM indexEs
    SDeclAssign txt st indexEs rhe -> SDeclAssign txt st <$> TR.htraverse natM indexEs <*> natM rhe
    SAssign lhe rhe -> SAssign <$> natM lhe <*> natM rhe
    STarget re -> STarget <$> natM re
    SSample ste dist al -> SSample <$> natM ste <*> pure dist <*> TR.htraverse natM al
    SFor txt se ee body -> SFor txt <$> natM se <*> natM ee <*> traverse (TR.htraverse natM) body
    SForEach txt at body -> SForEach txt <$> natM at <*> traverse (TR.htraverse natM) body
    SIfElse x0 sf -> SIfElse <$> traverse (\(c, s) -> (,) <$> natM c <*> TR.htraverse natM s) x0 <*> TR.htraverse natM sf
    SWhile cond body -> SWhile <$> natM cond <*> traverse (TR.htraverse natM) body
    SFunction func al body -> SFunction func al <$> traverse (TR.htraverse natM) body
    SScope body -> SScope <$> traverse (TR.htraverse natM) body
  hmapM = TR.htraverse

type LStmtF = StmtF LExpr
type LStmt = TR.Fix LStmtF

data UStmtF :: Type -> Type where
  SU :: StmtF UExpr t -> UStmtF t
  SContext :: (IndexLookupCtxt -> IndexLookupCtxt) -> UStmtF a -> UStmtF a

type UStmt = TR.Fix UStmtF

instance Functor UStmtF where
  fmap f = \case
    SU sf -> SU $ fmap f sf
    SContext g usf -> SContext g (fmap f usf)

instance Foldable UStmtF where
  foldMap f = \case
    SU sf -> foldMap f sf
    SContext g usf -> foldMap f usf

instance Traversable UStmtF where
  traverse g = \case
    SU sf -> SU <$> traverse g sf
    SContext f usf -> SContext f <$> traverse g usf

-- Expressions
data LExprF :: (EType -> Type) -> EType -> Type where
  LNamed :: Text -> SType t -> LExprF r t
  LInt :: Int -> LExprF r EInt
  LReal :: Double -> LExprF r EReal
  LComplex :: Double -> Double -> LExprF r EComplex
  LFunction :: Function rt args -> ArgList r args -> LExprF r rt
  LDistribution :: Distribution st args -> r st -> ArgList r args -> LExprF r EReal -- e.g., binomial_lupmf(st | ns, p)
  LBinaryOp :: SBinaryOp op -> r ta -> r tb -> LExprF r (BinaryResultT op ta tb)
  LCond :: r EBool -> r t -> r t -> LExprF r t
  LSlice :: SNat n -> r EInt -> r t -> LExprF r (Sliced n t)

type LExpr = TR.IFix LExprF

instance TR.HFunctor LExprF where
  hfmap nat = \case
    LNamed txt st -> LNamed txt st
    LInt n -> LInt n
    LReal x -> LReal x
    LComplex rp ip -> LComplex rp ip
    LFunction f al -> LFunction f (TR.hfmap nat al)
    LDistribution d st al -> LDistribution d (nat st) (TR.hfmap nat al)
    LBinaryOp sbo gta gtb -> LBinaryOp sbo (nat gta) (nat gtb)
    LCond c ifTrue ifFalse -> LCond (nat c) (nat ifTrue) (nat ifFalse)
    LSlice sn g gt -> LSlice sn (nat g) (nat gt)

instance TR.HTraversable LExprF where
  htraverse nat = \case
    LNamed txt st -> pure $ LNamed txt st
    LInt n -> pure $ LInt n
    LReal x -> pure $ LReal x
    LComplex x y -> pure $ LComplex x y
    LFunction f al -> LFunction f <$> TR.htraverse nat al
    LDistribution d st al -> LDistribution d <$> nat st <*> TR.htraverse nat al
    LBinaryOp sbo ata atb -> LBinaryOp sbo <$> nat ata <*> nat atb
    LCond c ifTrue ifFalse -> LCond <$> nat c <*> nat ifTrue <*> nat ifFalse
    LSlice sn a at -> LSlice sn <$> nat a <*> nat at
  hmapM = TR.htraverse

type DeclIndexVec et = DeclIndexVecF UExpr et--Vec (UExpr EInt) (DeclDimension et)

type IndexVec et = IndexVecF UExpr et -- (UExpr EInt) (Dimension et)

-- UEXpr represents expressions with un-looked-up indices
data IndexLookupCtxt = IndexLookupCtxt { declIndices :: Map SME.IndexKey (LExpr EInt), useIndices :: Map SME.IndexKey (LExpr EInt) }

lookupUse :: IndexLookupCtxt -> SME.IndexKey -> Either Text (LExpr EInt)
lookupUse (IndexLookupCtxt _ um) k =
  case Map.lookup k um of
    Just e -> Right e
    Nothing -> Left $ "lookupUse: " <> k <> " not found in use map."

lookupDecl :: IndexLookupCtxt -> SME.IndexKey -> Either Text (LExpr EInt)
lookupDecl (IndexLookupCtxt dm _) k =
  case Map.lookup k dm of
    Just e -> Right e
    Nothing -> Left $ "lookupDecl: " <> k <> " not found in decl map."

data UExprF :: (EType -> Type) -> EType -> Type where
  UL :: LExprF r et -> UExprF r et
  UNamedIndex :: SME.IndexKey -> UExprF r EInt

type UExpr = TR.IFix UExprF

instance TR.HFunctor UExprF where
  hfmap nat = \case
    UL le -> UL $ TR.hfmap nat le
    UNamedIndex txt -> UNamedIndex txt

instance TR.HTraversable UExprF where
  htraverse nat = \case
    UL le -> UL <$> TR.htraverse nat le
    UNamedIndex txt -> pure $ UNamedIndex txt
  hmapM = TR.htraverse

{-
instance TR.HFoldable UExprF where
  hfoldMap co = \case
    DeclareEF st divf -> mempty
    NamedEF txt st -> mempty
    IntEF n -> mempty
    RealEF x -> mempty
    ComplexEF x y -> mempty
    BinaryOpEF sbo ata atb -> co ata <> co atb
    SliceEF sn a at -> co a <> co at
    NamedIndexEF txt -> mempty
-}


namedE :: Text -> SType t -> UExpr t
namedE name  = TR.IFix . UL . LNamed name

intE :: Int -> UExpr EInt
intE = TR.IFix . UL . LInt

realE :: Double -> UExpr EReal
realE = TR.IFix . UL . LReal

complexE :: Double -> Double -> UExpr EComplex
complexE rp ip = TR.IFix $ UL $ LComplex rp ip

binaryOpE :: SBinaryOp op -> UExpr ta -> UExpr tb -> UExpr (BinaryResultT op ta tb)
binaryOpE op ea eb = TR.IFix $ UL $ LBinaryOp op ea eb

sliceE :: SNat n -> UExpr EInt -> UExpr t -> UExpr (Sliced n t)
sliceE sn ie e = TR.IFix $ UL $ LSlice sn ie e

namedIndexE :: Text -> UExpr EInt
namedIndexE = TR.IFix . UNamedIndex

{-

indexInner :: UExprF t a -> UExprF EInt a -> UExprF (SliceInnerN (S Z) t) a
indexInner e i = SliceE SZ i e

-- NB: We need the "go" here to add the SNat to the steps so GHC can convince itself that the lengths match up
-- This will yield a compile-time error if we try to index past the end or, same same, index something scalar.
-- That is, if n > Dimension a, this cannot be compiled.
indexInnerN :: UExprF t a -> Vec (UExprF EInt a) n -> UExprF (SliceInnerN n t) a
indexInnerN e v = go (sLength v) e v where
  go :: SNat n -> UExprF tb b -> Vec (UExprF EInt b) n -> UExprF (SliceInnerN n tb) b
  go SZ e _ = e
  go (SS m) e (i :> v') = go (sLength v') (indexInner e i) v'

indexAll :: UExprF t a -> IndexVec t a -> UExprF (SliceInnerN (Dimension t) t) a
indexAll = indexInnerN

plusE :: UExprF ta a -> UExprF tb a -> UExprF (BinaryResultT BAdd ta tb) a
plusE = BinaryOpE SAdd
-}

{-
useVar :: forall r.SME.StanVar -> (forall t.UExpr t -> r) -> r
useVar (SME.StanVar n x) k = case x of
  StanInt -> k $ NamedE n x SInt
  StanReal -> k $ NamedE n x SReal
  StanArray _ st -> toSType (fromStanType st) (k . NamedE n x . SArray)
  StanVector _ -> k $ NamedE n x SCVec
  StanMatrix _ -> k $ NamedE n x SMat
  StanCorrMatrix sd -> k $ NamedE n x SMat
  StanCholeskyFactorCorr sd -> k $ NamedE n x SMat
  StanCovMatrix sd -> k $ NamedE n x SMat
-}
{-
data StanType = StanInt
              | StanReal
              | StanArray [SME.StanDim] StanType
              | StanVector SME.StanDim
              | StanMatrix (SME.StanDim, SME.StanDim)
              | StanCorrMatrix SME.StanDim
              | StanCholeskyFactorCorr SME.StanDim
              | StanCovMatrix SME.StanDim
              deriving (Show, Eq, Ord, Generic)

fromStanType :: StanType -> EType
fromStanType = \case
  StanInt -> EInt
  StanReal -> EReal
  StanArray _ st -> EArray (fromStanType st)
  StanVector _ -> ECVec
  StanMatrix _ -> EMat
  StanCorrMatrix _ -> EMat
  StanCholeskyFactorCorr _ -> EMat
  StanCovMatrix _ -> EMat
-}
{-
class ToEType a where
  toEType :: EType

instance ToEType SInt where
  toEType = EInt
instance ToEType SInt where

  toEType SReal = EReal
  toEType SCVec = ECVec
  toEType SRVec = ERVec
  toEType SMat = EMat
  toEType (SArray st) = EArray (toEType st)
  toEType (sa ::-> sb) = toEType sa :-> toEType sb
-}
{-
  toSType (fromStanType x) f where
  f :: forall u.SType u -> r
  f = case x of
    StanInt -> k $ UNamedE n x SInt
    StanReal -> k $ UNamedE n x SReal
    StanArray _ st -> k $ toSType (fromStanType st) $ UNamedE n x . SArray
    StanVector sd -> _
    StanMatrix x1 -> _
    StanCorrMatrix sd -> _
    StanCholeskyFactorCorr sd -> _
    StanCovMatrix sd -> _
-}



{-
intE :: Int -> UExpr EInt
intE = IntE

realE :: Double -> UExpr EReal
realE = RealE

varE :: SType t -> Text -> UExpr t
varE _ = VarE

plusOpE :: UExpr t -> UExpr (t :-> t)
plusOpE a = FunE ()
-}

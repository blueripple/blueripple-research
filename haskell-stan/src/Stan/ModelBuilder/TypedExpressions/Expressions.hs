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

module Stan.ModelBuilder.TypedExpressions.Expressions
  (
    module Stan.ModelBuilder.TypedExpressions.Expressions
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
import qualified Data.Vec.Lazy as Vec
import qualified Data.Type.Nat as DT

-- Expressions
data LExprF :: (EType -> Type) -> EType -> Type where
  LNamed :: Text -> SType t -> LExprF r t
  LInt :: Int -> LExprF r EInt
  LReal :: Double -> LExprF r EReal
  LComplex :: Double -> Double -> LExprF r EComplex
  LString :: Text -> LExprF r EString
  LVector :: [Double] -> LExprF r ECVec
  LMatrix :: [Vec n Double] -> LExprF r EMat
  LArray :: NestedVec n (r t) -> LExprF r (EArray n t)
  LIntRange :: r EInt -> r EInt -> LExprF r (EArray (S Z) EInt)
  LFunction :: Function rt args -> ArgList r args -> LExprF r rt
  LDensity :: Density st args -> r st -> ArgList r args -> LExprF r EReal -- e.g., binomial_lupmf(st | ns, p)
  LUnaryOp :: SUnaryOp op -> r t -> LExprF r (UnaryResultT op t)
  LBinaryOp :: SBinaryOp op -> r ta -> r tb -> LExprF r (BinaryResultT op ta tb)
  LCond :: r EBool -> r t -> r t -> LExprF r t
  LSlice :: SNat n -> r EInt -> r t -> LExprF r (Sliced n t)
  LIndex :: SNat n -> r (EArray (S Z) EInt) -> r t -> LExprF r (Indexed n t)

type LExpr = TR.IFix LExprF

lNamedE :: Text -> SType t -> LExpr t
lNamedE name  = TR.IFix . LNamed name

lIntE :: Int -> LExpr EInt
lIntE = TR.IFix . LInt


-- UEXpr represents expressions with un-looked-up indices/sizes
data UExprF :: (EType -> Type) -> EType -> Type where
  UL :: LExprF r et -> UExprF r et
  UNamedIndex :: SME.IndexKey -> UExprF r EInt
  UNamedSize :: SME.IndexKey -> UExprF r EInt

type UExpr = TR.IFix UExprF

instance TR.HFunctor LExprF where
  hfmap nat = \case
    LNamed txt st -> LNamed txt st
    LInt n -> LInt n
    LReal x -> LReal x
    LComplex rp ip -> LComplex rp ip
    LString t -> LString t
    LVector xs -> LVector xs
    LMatrix ms -> LMatrix ms
    LArray nv -> LArray (fmap nat nv)
    LIntRange le ue -> LIntRange (nat le) (nat ue)
    LFunction f al -> LFunction f (TR.hfmap nat al)
    LDensity d st al -> LDensity d (nat st) (TR.hfmap nat al)
    LUnaryOp suo gta -> LUnaryOp suo (nat gta)
    LBinaryOp sbo gta gtb -> LBinaryOp sbo (nat gta) (nat gtb)
    LCond c ifTrue ifFalse -> LCond (nat c) (nat ifTrue) (nat ifFalse)
    LSlice sn g gt -> LSlice sn (nat g) (nat gt)
    LIndex n re e -> LIndex n (nat re) (nat e)

instance TR.HTraversable LExprF where
  htraverse nat = \case
    LNamed txt st -> pure $ LNamed txt st
    LInt n -> pure $ LInt n
    LReal x -> pure $ LReal x
    LComplex x y -> pure $ LComplex x y
    LString t -> pure $ LString t
    LVector xs -> pure $ LVector xs
    LMatrix ms -> pure $ LMatrix ms
    LArray nv -> LArray <$> traverse nat nv
    LIntRange le ue -> LIntRange <$> nat le <*> nat ue
    LFunction f al -> LFunction f <$> TR.htraverse nat al
    LDensity d st al -> LDensity d <$> nat st <*> TR.htraverse nat al
    LUnaryOp suo ata -> LUnaryOp suo <$> nat ata
    LBinaryOp sbo ata atb -> LBinaryOp sbo <$> nat ata <*> nat atb
    LCond c ifTrue ifFalse -> LCond <$> nat c <*> nat ifTrue <*> nat ifFalse
    LSlice sn a at -> LSlice sn <$> nat a <*> nat at
    LIndex n re e -> LIndex n <$> nat re <*> nat e
  hmapM = TR.htraverse

instance TR.HFunctor UExprF where
  hfmap nat = \case
    UL le -> UL $ TR.hfmap nat le
    UNamedIndex txt -> UNamedIndex txt
    UNamedSize txt -> UNamedSize txt

instance TR.HTraversable UExprF where
  htraverse nat = \case
    UL le -> UL <$> TR.htraverse nat le
    UNamedIndex txt -> pure $ UNamedIndex txt
    UNamedSize txt -> pure $ UNamedSize txt
  hmapM = TR.htraverse

namedE :: Text -> SType t -> UExpr t
namedE name  = TR.IFix . UL . LNamed name

intE :: Int -> UExpr EInt
intE = TR.IFix . UL . LInt

realE :: Double -> UExpr EReal
realE = TR.IFix . UL . LReal

complexE :: Double -> Double -> UExpr EComplex
complexE rp ip = TR.IFix $ UL $ LComplex rp ip

stringE :: Text -> UExpr EString
stringE = TR.IFix . UL . LString

vectorE :: [Double] -> UExpr ECVec
vectorE = TR.IFix . UL . LVector

matrixE :: [Vec n Double] -> UExpr EMat
matrixE = TR.IFix . UL . LMatrix

arrayE :: NestedVec n (UExpr t) -> UExpr (EArray n t)
arrayE = TR.IFix . UL . LArray

intRangeE :: UExpr EInt -> UExpr EInt -> UExpr (EArray (S Z) EInt)
intRangeE le ue = TR.IFix $ UL $ LIntRange le ue

functionE :: Function rt args -> ArgList UExpr args -> UExpr rt
functionE f al = TR.IFix $ UL $ LFunction f al

densityE :: Density gt args -> UExpr gt -> ArgList UExpr args -> UExpr EReal
densityE d ge al = TR.IFix $ UL $ LDensity d ge al

unaryOpE :: SUnaryOp op -> UExpr t -> UExpr (UnaryResultT op t)
unaryOpE op e = TR.IFix $ UL $ LUnaryOp op e

binaryOpE :: SBinaryOp op -> UExpr ta -> UExpr tb -> UExpr (BinaryResultT op ta tb)
binaryOpE op ea eb = TR.IFix $ UL $ LBinaryOp op ea eb

boolOpE :: SBoolOp op -> UExpr ta -> UExpr tb -> UExpr (BoolResultT op ta tb)
boolOpE bop ea eb = TR.IFix $ UL $ LBinaryOp (SBoolean bop) ea eb

condE :: UExpr EBool -> UExpr t -> UExpr t -> UExpr t
condE ce te fe = TR.IFix $ UL $ LCond ce te fe

sliceE :: SNat n -> UExpr EInt -> UExpr t -> UExpr (Sliced n t)
sliceE sn ie e = TR.IFix $ UL $ LSlice sn ie e

indexE :: SNat n -> UExpr (EArray (S Z) EInt) -> UExpr t -> UExpr (Indexed n t)
indexE sn ie e = TR.IFix $ UL $ LIndex sn ie e

namedIndexE :: Text -> UExpr EInt
namedIndexE = TR.IFix . UNamedIndex

namedSizeE :: Text -> UExpr EInt
namedSizeE = TR.IFix . UNamedSize

sliceInner :: UExpr t -> UExpr EInt -> UExpr (SliceInnerN (S Z) t)
sliceInner e i = sliceE SZ i e

-- NB: We need the "go" here to add the SNat to the steps so GHC can convince itself that the lengths match up
-- This will yield a compile-time error if we try to index past the end or, same same, index something scalar.
-- That is, if n > Dimension a, this cannot be compiled.
sliceInnerN :: UExpr t -> Vec n (UExpr EInt) -> UExpr (SliceInnerN n t)
sliceInnerN e v = Vec.withDict v $ go e v where
  go :: DT.SNatI m => UExpr u -> Vec m (UExpr EInt) -> UExpr (SliceInnerN m u)
  go = go' DT.snat
  go' :: DT.SNat k -> UExpr a -> Vec k (UExpr EInt) -> UExpr (SliceInnerN k a)
  go' SZ e _ = e
  go' SS e (i ::: v') = go' DT.snat (sliceInner e i) v'

sliceAll :: UExpr t -> Vec (Dimension t) (UExpr EInt) -> UExpr (SliceInnerN (Dimension t) t)
sliceAll = sliceInnerN

{-
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

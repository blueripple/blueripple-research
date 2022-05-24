{-# LANGUAGE DataKinds #-}
--{-# LANGUAGE DeriveTraversable #-}
--{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE StandaloneKindSignatures #-}
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}
{-# LANGUAGE TypeSynonymInstances #-}

module Stan.ModelBuilder.TypedExpressions.Expression
  (
    module Stan.ModelBuilder.TypedExpressions.Expression
  , Nat(..)
  , Fin(..)
  , Vec(..)
  )
  where

import qualified Stan.ModelBuilder.TypedExpressions.Recursion as TR

import Prelude hiding (reverse, Nat, (+))
import qualified Control.Foldl as Fold
import qualified Data.Comp.Multi as DC
import qualified Data.Functor.Classes as FC
import qualified Data.Functor.Classes.Generic as FC
import qualified Data.Functor.Foldable as Rec
import qualified Data.Functor.Foldable.Monadic as Rec
import qualified Data.Functor.Foldable.TH as Rec
import           Data.Kind (Type)
import qualified Data.Fix as Fix
import qualified Data.Map.Strict as Map
import qualified Data.Set as Set
import qualified Data.Text as T

import Data.Type.Nat (Nat(..), SNat(..))
import Data.Fin (Fin(..))
import Data.Vec.Lazy (Vec(..))

import qualified Data.Nat as DT
import qualified Data.Type.Nat as DT
import qualified Data.Fin as DT
import qualified Data.Vec.Lazy as DT

import qualified Text.PrettyPrint as Pretty

import GHC.Generics (Generic1)
import qualified GHC.TypeLits as TE
import GHC.TypeLits (ErrorMessage((:<>:)))
import qualified Stan.ModelBuilder.Expressions as SME
import Stan.ModelBuilder.Expressions (StanVar(..))
import Stan.ModelBuilder (DataSetGroupIntMaps)
import Frames.Streamly.CSV (accToMaybe)

-- to simplify most indexing
n0 :: Nat = Z
n1 :: Nat = S Z
n2 :: Nat = S n1
n3 :: Nat = S n2


-- to simplify most indexing
i0 :: SNat Z = DT.SZ
i1 :: SNat (S Z) = DT.SS
i2 :: SNat (S (S Z)) = DT.SS
i3 :: SNat (S (S (S Z))) = DT.SS
{-
toSNat :: Nat -> (forall n. SNat n -> r) -> r
toSNat Zero f = f SZero
toSNat (Succ n) f = toSNat n $ f . SSucc

data Fin :: Nat -> Type where
  FZ :: Fin (Succ n)
  FS :: Fin n -> Fin (Succ n)

data Vec :: Type -> Nat -> Type where
  VNil :: Vec a Zero
  (:>) ::a -> Vec a n -> Vec a (Succ n)

type family n + m where
  Zero + m = m
  Succ n + m = Succ (n + m)
  n + m = m + n
--  m + Zero = m
--  n + Succ m = Succ (n + m)
--  n + Succ m = Succ n + m

(+) :: Vec a n -> Vec a m -> Vec a (n + m)
VNil + ys = ys
(x :> xs) + ys = x :> (xs + ys)

sLength :: Vec a n -> SNat n
sLength VNil = SZero
sLength (_ :> v) = SSucc (sLength v)

vMap :: (a -> b) -> Vec a n -> Vec b n
vMap _ VNil = VNil
vMap f (a :> v) = f a :> vMap f v

(!!!) :: Vec a n -> Fin n -> a
(a :> v) !!! FZ = a
(a :> v) !!! (FS m) = v !!! m

take :: forall a n m.SNat m -> Vec a (m + n) -> Vec a m
take i v = go VNil i v
  where
    go :: Vec a l -> SNat k -> Vec a (k + n) -> Vec a (k + l)
    go v SZero _ = v
    go v (SSucc q) (a :> v') = go (a :> v) q v'

{-
popRandom :: Vec a (Succ n) -> Fin (Succ n) -> (a, Vec a n)
popRandom vOrig i = (a, v') where
  v' = reverse vLeft + vRight
  (a :> vLeft, vRight) = go VNil vOrig i
  go :: Vec a m -> Vec a (Succ l) -> Fin (Succ l) -> (Vec a (Succ m), Vec a l)
  go vAcc (a :> vR) FZ = (a :> vAcc, vR)
  go vAcc (a :> vR) (FS i') = go (a :> vAcc) vR i'

reverse :: Vec a n -> Vec a n
reverse = go VNil where
  go :: Vec a k -> Vec a l -> Vec a (l + k)
  go acc VNil = acc
  go acc (a :> v) = go (a :> acc) v
-}
-}

type family DiffOrZero (n :: Nat) (m :: Nat) :: Nat where
  DiffOrZero Z Z = Z
  DiffOrZero (S n) Z = S n
  DiffOrZero Z (S m) = Z
  DiffOrZero (S n) (S m) = DiffOrZero n m
-- possible types of terms
data EType = EInt | EReal | ECVec | ERVec | EMat | ESqMat | EArray Nat EType

data StanType :: EType -> Type where
  StanInt :: StanType EInt
  StanReal :: StanType EReal
  StanArray :: SNat n -> StanType et -> StanType (EArray n et)
  StanVector :: StanType ECVec
  StanMatrix :: StanType EMat
  StanCorrMatrix :: StanType ESqMat
  StanCholeskyFactorCorr :: StanType ESqMat
  StanCovMatrix :: StanType ESqMat

type family DeclDimension (e :: EType) :: Nat where
  DeclDimension EInt = Z
  DeclDimension EReal = Z
  DeclDimension ECVec = S Z
  DeclDimension ERVec = S Z
  DeclDimension EMat = S (S Z)
  DeclDimension ESqMat = (S Z)
  DeclDimension (EArray n t) = n `DT.Plus` Dimension t

type family Dimension (e :: EType) :: Nat where
  Dimension EInt = Z
  Dimension EReal = Z
  Dimension ECVec = S Z
  Dimension ERVec = S Z
  Dimension EMat = S (S Z)
  Dimension (EArray n t) = n `DT.Plus` Dimension t

-- singleton so that term types may be used at type-level
data SType :: EType -> Type where
  SInt :: SType EInt
  SReal :: SType EReal
  SCVec :: SType ECVec
  SRVec :: SType ERVec
  SMat :: SType EMat
  SSqMat :: SType ESqMat
  SArray :: SNat n -> SType t -> SType (EArray n t)

toSType :: forall t r.EType -> (forall t. SType t -> r) -> r
toSType EInt k = k SInt
toSType EReal k = k SReal
toSType ERVec k = k SRVec
toSType ECVec k = k SCVec
toSType EMat k = k SMat
toSType ESqMat k = k SSqMat
toSType (EArray n t) k = DT.reify n f
  where
    f :: forall n. DT.SNatI n => Proxy n -> r
    f _ = toSType t $ \st -> k (SArray (DT.snat @n)  st)
{-
data (a :: k) :~: (b :: k) where
  Refl :: a :~: a

class TestEquality (t :: k -> Type) where
  testEquality :: t a -> t b -> Maybe (a :~: b)

instance TestEquality SNat where
  testEquality SZ SZ = Just Refl
  testEquality (SS sn) (SS sm) = do
    Refl <- testEquality sn sm
    pure Refl
  testEquality _ _ = Nothing

instance TestEquality SType where
  testEquality SInt SInt = Just Refl
  testEquality SReal SReal = Just Refl
  testEquality SCVec SCVec = Just Refl
  testEquality SRVec SRVec = Just Refl
  testEquality SMat SMat = Just Refl
  testEquality (SArray sn sa) (SArray sm sb) = do
    Refl <- testEquality sa sb
    Refl <- testEquality sn sm
    pure Refl
  testEquality _ _ = Nothing
-}

type family ApplyDiffOrZeroToEType (n :: Nat) (e :: EType) :: EType where
  ApplyDiffOrZeroToEType Z (EArray Z t) = t
  ApplyDiffOrZeroToEType Z (EArray (S n) t) = EArray n t
  ApplyDiffOrZeroToEType d (EArray m t) = EArray m (Sliced d t)
  ApplyDiffOrZeroToEType _ x = x

type family Sliced (n :: Nat) (a :: EType) :: EType where
  Sliced _ EInt = TE.TypeError (TE.Text "Cannot slice (index) a scalar int.")
  Sliced _ EReal = TE.TypeError (TE.Text "Cannot slice (index) a scalar real.")
  Sliced Z ERVec = EReal
  Sliced _ ERVec = TE.TypeError (TE.Text "Cannot slice (index) a vector at a position other than 0.")
  Sliced Z ECVec = EReal
  Sliced _ ECVec = TE.TypeError (TE.Text "Cannot slice (index) a vector at a position other than 0.")
  Sliced Z EMat = ERVec
  Sliced (S Z) EMat = ECVec
  Sliced _ EMat = TE.TypeError (TE.Text "Cannot slice (index) a matrix at a position other than 0 or 1.")
  Sliced n (EArray m t) = ApplyDiffOrZeroToEType (DiffOrZero m n) (EArray m t)

type family SliceInnerN (n :: Nat) (a :: EType) :: EType where
  SliceInnerN Z a = a
  SliceInnerN (S n) a = SliceInnerN n (Sliced Z a)

newtype DeclIndexVecF (r :: EType -> Type) (et :: EType) = DeclIndexVecF { unDeclIndexVecF :: Vec (DeclDimension et) (r EInt)}

instance TR.NFunctor DeclIndexVecF where
  nmap nat (DeclIndexVecF v) = DeclIndexVecF $ DT.map nat v

type DeclIndexVec et = DeclIndexVecF UExpr et--Vec (UExpr EInt) (DeclDimension et)

newtype IndexVecF (r :: EType -> Type) (et :: EType) = IndexVecF { unIndexVecF :: Vec (Dimension et) (r EInt) }

instance TR.NFunctor IndexVecF where
  nmap nat (IndexVecF v) = IndexVecF $ DT.map nat v

type IndexVec et = IndexVecF UExpr et -- (UExpr EInt) (Dimension et)


data BinaryOp = BEqual | BAdd | BSubtract | BMultiply | BDivide | BElementWise BinaryOp | BAndEqual BinaryOp

data SBinaryOp :: BinaryOp -> Type where
  SEqual :: SBinaryOp BEqual
  SAdd :: SBinaryOp BAdd
  SSubtract :: SBinaryOp BSubtract
  SMultiply :: SBinaryOp BMultiply
  SDivide :: SBinaryOp BDivide
  SElementWise :: SBinaryOp op -> SBinaryOp (BElementWise op)
  SAndEqual :: SBinaryOp op -> SBinaryOp (BElementWise op)

type family BinaryResultT (bo :: BinaryOp) (a :: EType) (b :: EType) :: EType where
  BinaryResultT BEqual a a = a -- ?
  BinaryResultT BEqual a b = TE.TypeError (TE.ShowType a :<>: TE.Text " and " :<>: TE.ShowType b :<>: TE.Text " cannot be set equal." )
  BinaryResultT BAdd a a = a
  BinaryResultT BAdd EInt EReal = EReal
  BinaryResultT BAdd EReal EInt = EReal
  BinaryResultT BAdd a b = TE.TypeError (TE.ShowType a :<>: TE.Text " and " :<>: TE.ShowType b :<>: TE.Text " cannot be added." )
  BinaryResultT BSubtract a a = a
  BinaryResultT BSubtract EInt EReal = EReal
  BinaryResultT BSubtract EReal EInt = EReal
  BinaryResultT BSubtract a b = TE.TypeError (TE.ShowType a :<>: TE.Text " and " :<>: TE.ShowType b :<>: TE.Text " cannot be subtracted." )
  BinaryResultT BMultiply EInt a = a
  BinaryResultT BMultiply a EInt = a
  BinaryResultT BMultiply EReal a = a
  BinaryResultT BMultiply a EReal = a
  BinaryResultT BMultiply ERVec ECVec = EReal -- dot product
  BinaryResultT BMultiply EMat EMat = EMat
  BinaryResultT BMultiply EMat ESqMat = EMat
  BinaryResultT BMultiply ESqMat EMat = EMat
  BinaryResultT BMultiply ESqMat ESqMat = EMat
  BinaryResultT BMultiply EMat ECVec = ECVec
  BinaryResultT BMultiply ESqMat ECVec = ECVec
  BinaryResultT BMultiply ERVec EMat = ERVec
  BinaryResultT BMultiply ERVec ESqMat = ERVec
  BinaryResultT BMultiply EMat ERVec = TE.TypeError (TE.Text "Cannot do matrix * row-vector.  Perhaps you meant to flip the order?")
  BinaryResultT BMultiply ESqMat ERVec = TE.TypeError (TE.Text "Cannot do matrix * row-vector.  Perhaps you meant to flip the order?")
  BinaryResultT BMultiply ECVec EMat = TE.TypeError (TE.Text "Cannot do col-vector * matrix.  Perhaps you meant to flip the order?")
  BinaryResultT BMultiply ECVec ESqMat = TE.TypeError (TE.Text "Cannot do col-vector * matrix.  Perhaps you meant to flip the order?")
  BinaryResultT BMultiply a (EArray n b) = EArray n (BinaryResultT BMultiply a b)
  BinaryResultT BMultiply a b = TE.TypeError (TE.ShowType a :<>: TE.Text " and " :<>: TE.ShowType b :<>: TE.Text " cannot be multiplied." )
  BinaryResultT BDivide EInt EInt = EReal
  BinaryResultT BDivide EInt EReal = EReal
  BinaryResultT BDivide a EInt = a
--  BinaryResultT BDivide (EArray a) b = EArray (BinaryResultT BDivide a b)
  BinaryResultT BDivide a b = TE.TypeError (TE.ShowType a :<>: TE.Text " and " :<>: TE.ShowType b :<>: TE.Text " cannot be divided." )
  BinaryResultT (BElementWise op) (EArray n a) (EArray n b) = EArray n (BinaryResultT op a b)
  BinaryResultT (BElementWise op) ECVec ECVec = ECVec
  BinaryResultT (BElementWise op) ERVec ERVec = ERVec
  BinaryResultT (BElementWise _) a b = TE.TypeError (TE.ShowType a :<>: TE.Text " and " :<>: TE.ShowType b :<>: TE.Text " cannot be combined elementwise." )
  BinaryResultT (BAndEqual op) a b = BinaryResultT op a b

{-
data UExpr :: EType -> Type where
  DeclareE :: StanType t -> DeclIndexVec t -> UExpr t
  NamedE :: Text -> SType t -> UExpr t
  IntE :: Int -> UExpr EInt
  RealE :: Double -> UExpr EReal
  BinaryOpE :: SBinaryOp op -> UExpr ta -> UExpr tb -> UExpr (BinaryResultT op ta tb)
  SliceE :: SNat n -> UExpr EInt -> UExpr t -> UExpr (Sliced n t)
  NamedIndexE :: SME.IndexKey -> UExpr EInt
-}

data UExprF :: (EType -> Type) -> EType -> Type where
  DeclareEF :: StanType t -> DeclIndexVecF r t -> UExprF r t
  NamedEF :: Text -> SType t -> UExprF r t
  IntEF :: Int -> UExprF r EInt
  RealEF :: Double -> UExprF r EReal
  BinaryOpEF :: SBinaryOp op -> r ta -> r tb -> UExprF r (BinaryResultT op ta tb)
  SliceEF :: SNat n -> r EInt -> r t -> UExprF r (Sliced n t)
  NamedIndexEF :: SME.IndexKey -> UExprF r EInt

instance TR.NFunctor UExprF where
  nmap nat = \case
    DeclareEF st vec -> DeclareEF st (TR.nmap nat vec)
    NamedEF txt st -> NamedEF txt st
    IntEF n -> IntEF n
    RealEF x -> RealEF x
    BinaryOpEF sbo gta gtb -> BinaryOpEF sbo (nat gta) (nat gtb)
    SliceEF sn g gt -> SliceEF sn (nat g) (nat gt)
    NamedIndexEF txt -> NamedIndexEF txt

type UExpr = TR.IFix UExprF

declareE :: StanType t -> DeclIndexVec t -> UExpr t
declareE st = TR.IFix . DeclareEF st

namedE :: Text -> SType t -> UExpr t
namedE name  = TR.IFix . NamedEF name

intE :: Int -> UExpr EInt
intE = TR.IFix . IntEF

realE :: Double -> UExpr EReal
realE = TR.IFix . RealEF

binaryOpE :: SBinaryOp op -> UExpr ta -> UExpr tb -> UExpr (BinaryResultT op ta tb)
binaryOpE op ea eb = TR.IFix $ BinaryOpEF op ea eb

sliceE :: SNat n -> UExpr EInt -> UExpr t -> UExpr (Sliced n t)
sliceE sn ie e = TR.IFix $ SliceEF sn ie e

namedIndexE :: Text -> UExpr EInt
namedIndexE = TR.IFix . NamedIndexEF

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

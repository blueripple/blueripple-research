{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneKindSignatures #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE UndecidableInstances #-}
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}

module Stan.ModelBuilder.TypedExpressions.Indexing
  ( module Stan.ModelBuilder.TypedExpressions.Indexing,
    Fin (..),
    Vec (..),
  )
where

import Data.Fin (Fin (..))
import Data.Kind (Type)
import qualified Data.Type.Nat as DT
import Data.Vec.Lazy (Vec (..))
import qualified Data.Vec.Lazy as DT
import GHC.TypeLits (ErrorMessage ((:<>:)))
import qualified GHC.TypeLits as TE
import qualified Stan.ModelBuilder.TypedExpressions.Recursion as TR
import Stan.ModelBuilder.TypedExpressions.Types
import Prelude hiding (Nat)
import Data.Functor.Classes (eq1)
import Data.Type.Equality ((:~:)(Refl), (:~~:)(HRefl), TestEquality(testEquality))
import Data.GADT.Compare (GEq(geq))
import Type.Reflection (Typeable(..),typeRep)
import qualified Data.Monoid as Mon

-- to simplify most indexing
-- term level
n0 :: Nat = Z

n1 :: Nat = S Z

n2 :: Nat = S n1

n3 :: Nat = S n2

-- type level
type N0 :: Nat
type N0 = Z

type N1 :: Nat
type N1 = S Z

type N2 :: Nat
type N2 = S (S Z)

type N3 :: Nat
type N3 = S (S (S Z))

s0 :: SNat Z = DT.SZ

s1 :: SNat (S Z) = DT.SS

s2 :: SNat (S (S Z)) = DT.SS

s3 :: SNat (S (S (S Z))) = DT.SS

s4 :: SNat (S (S (S (S Z)))) = DT.SS

-- popRandom :: forall n m a. (DT.SNatI n, DT.SNatI m) => Vec (DT.Plus n (S m)) a -> (a, Vec (DT.Plus n m) a)
-- popRandom v = (a, vL DT.++ vR)
--  where (vL, a ::: vR) = DT.split v :: (Vec n a, Vec (S m) a)

data DiffHolder = PosDiff Nat | Same | NegDiff Nat

type family Diff (n :: Nat) (m :: Nat) :: DiffHolder where
  Diff Z Z = Same
  Diff (S n) Z = PosDiff (S n)
  Diff Z (S n) = NegDiff (S n) --TE.TypeError (TE.Text "Diff: attempt to take diff of m and n where n is larger than m.")
  Diff (S n) (S m) = Diff n m

type family DeclDimension (e :: EType) :: Nat where
  DeclDimension EInt = Z
  DeclDimension EReal = Z
  DeclDimension EComplex = Z
  DeclDimension ECVec = S Z
  DeclDimension ERVec = S Z
  DeclDimension EMat = S (S Z)
  DeclDimension ESqMat = S Z
  DeclDimension (EArray n t) = n `DT.Plus` DeclDimension t

type family Dimension (e :: EType) :: Nat where
  Dimension EInt = Z
  Dimension EReal = Z
  Dimension EComplex = Z
  Dimension ECVec = S Z
  Dimension ERVec = S Z
  Dimension EMat = S (S Z)
  Dimension ESqMat = S (S Z)
  Dimension (EArray n t) = n `DT.Plus` Dimension t

type family ApplyDiffToEType (n :: DiffHolder) (e :: EType) :: EType where
  ApplyDiffToEType _ (EArray Z t) = TE.TypeError (TE.Text "Attempt to slice a zero-dimensional array.  Which means you had a zero dimensional array?")
  ApplyDiffToEType (PosDiff _) (EArray (S Z) t) = TE.TypeError (TE.Text "ApplyDiffToEType: Impossible case of PosDiff but array of dimension 1.")
  ApplyDiffToEType (PosDiff _) (EArray (S n) t) = EArray n t -- slice is in the array
  ApplyDiffToEType Same (EArray (S Z) t) = t -- Slice a 1-d array case.
  ApplyDiffToEType Same (EArray (S n) t) = EArray n t -- Slice a 1-d array case.
  ApplyDiffToEType (NegDiff (S n)) (EArray o t) = EArray o (Sliced n t) -- array doesn not have enough dimensions.  Slice the rest from the contained type.
  ApplyDiffToEType _ x = TE.TypeError (TE.Text "ApplyDiffToEtype to type other than EArray.")

type family Sliced (n :: Nat) (a :: EType) :: EType where
  Sliced _ EInt = TE.TypeError (TE.Text "Cannot slice (index) a scalar int.")
  Sliced _ EReal = TE.TypeError (TE.Text "Cannot slice (index) a scalar real.")
  Sliced _ EComplex = TE.TypeError (TE.Text "Cannot slice (index) a scalar complex.")
  Sliced Z ERVec = EReal
  Sliced _ ERVec = TE.TypeError (TE.Text "Cannot slice (index) a row-vector at a position other than 0.")
  Sliced Z ECVec = EReal
  Sliced _ ECVec = TE.TypeError (TE.Text "Cannot slice (index) a vector at a position other than 0.")
  Sliced Z EMat = ERVec
  Sliced (S Z) EMat = ECVec
  Sliced _ EMat = TE.TypeError (TE.Text "Cannot slice (index) a matrix at a position other than 0 or 1.")
  Sliced Z ESqMat = ERVec
  Sliced (S Z) ESqMat = ECVec
  Sliced _ ESqMat = TE.TypeError (TE.Text "Cannot slice (index) a matrix at a position other than 0 or 1.")
  Sliced n (EArray m t) = ApplyDiffToEType (Diff m (S n)) (EArray m t)

type family SliceInnerN (n :: Nat) (a :: EType) :: EType where
  SliceInnerN Z a = a
  SliceInnerN (S n) a = SliceInnerN n (Sliced Z a)

type family IfLessOrEq (n :: Nat) (m :: Nat) (a :: EType) (b :: EType) :: EType where
  IfLessOrEq Z Z a _ = a
  IfLessOrEq Z (S n) a _ = a
  IfLessOrEq (S n) Z _ b = b
  IfLessOrEq (S n) (S m) a b = IfLessOrEq n m a b

type family Indexed (n :: Nat) (a :: EType) :: EType where
  Indexed Z ECVec = ECVec
  Indexed _ ECVec = TE.TypeError (TE.Text "Attempt to index a vector at a position other than 0.")
  Indexed Z ERVec = ERVec
  Indexed _ ERVec = TE.TypeError (TE.Text "Attempt to index a row_vector at a position other than 0.")
  Indexed Z EMat = EMat
  Indexed (S Z) EMat = EMat
  Indexed _ EMat = TE.TypeError (TE.Text "Attempt to index a matrix at a position other than 0 or 1.")
  Indexed Z ESqMat = EMat
  Indexed (S Z) ESqMat = EMat
  Indexed _ ESqMat = TE.TypeError (TE.Text "Attempt to index a (square) matrix at a position other than 0 or 1.")
  Indexed n (EArray m t) = IfLessOrEq (S n) (Dimension (EArray m t)) (EArray m t) (TE.TypeError (TE.Text "Attempt to index an array at too high an index."))
  Indexed _ _ = TE.TypeError (TE.Text "Cannot index a scalar.")

newtype DeclIndexVecF (r :: EType -> Type) (et :: EType) = DeclIndexVecF {unDeclIndexVecF :: Vec (DeclDimension et) (r EInt)}

instance TR.HFunctor DeclIndexVecF where
  hfmap nat (DeclIndexVecF v) = DeclIndexVecF $ DT.map nat v

instance TR.HTraversable DeclIndexVecF where
  hmapM natM = fmap DeclIndexVecF . traverse natM . unDeclIndexVecF
  htraverse natM = fmap DeclIndexVecF . traverse natM . unDeclIndexVecF

newtype IndexVecF (r :: EType -> Type) (et :: EType) = IndexVecF {unIndexVecF :: Vec (Dimension et) (r EInt)}

instance TR.HFunctor IndexVecF where
  hfmap nat (IndexVecF v) = IndexVecF $ DT.map nat v

instance TR.HTraversable IndexVecF where
  hmapM natM = fmap IndexVecF . traverse natM . unIndexVecF
  htraverse natM = fmap IndexVecF . traverse natM . unIndexVecF

newtype IndexVecM (r :: EType -> Type) (et :: EType) = IndexVecM {unIndexVecM :: Vec (Dimension et) (Maybe (r EInt))}

instance TR.HFunctor IndexVecM where
  hfmap nat (IndexVecM v) = IndexVecM $ DT.map (fmap nat) v

instance TR.HTraversable IndexVecM where
  htraverse natM = fmap IndexVecM . traverse (traverse natM) . unIndexVecM
  hmapM = TR.htraverse

data NestedVec :: Nat -> Type -> Type where
  NestedVec1 :: Vec (S n) a -> NestedVec (S Z) a
  NestedVec2 :: Vec (S n) (Vec (S m) a) -> NestedVec (S (S Z)) a
  NestedVec3 :: Vec (S n) (Vec (S m) (Vec (S k) a)) -> NestedVec (S (S (S Z))) a

instance Functor (NestedVec n) where
  fmap f = \case
    NestedVec1 v -> NestedVec1 $ DT.map f v
    NestedVec2 v -> NestedVec2 $ DT.map (DT.map f) v
    NestedVec3 v -> NestedVec3 $ DT.map (DT.map (DT.map f)) v

instance Foldable (NestedVec n) where
  foldMap f = \case
    NestedVec1 v -> foldMap f v
    NestedVec2 v -> mconcat $ DT.toList $ fmap (foldMap f) v
    NestedVec3 v -> mconcat $ concatMap DT.toList $ DT.toList $ fmap (foldMap f) <$> v

instance Traversable (NestedVec n) where
  traverse f = \case
    NestedVec1 v -> NestedVec1 <$> DT.traverse f v
    NestedVec2 v -> NestedVec2 <$> DT.traverse (DT.traverse f) v
    NestedVec3 v -> NestedVec3 <$> DT.traverse (DT.traverse (DT.traverse f)) v

nestedVecHead :: NestedVec n a -> a
nestedVecHead (NestedVec1 (a ::: _)) = a
nestedVecHead (NestedVec2 ((a ::: _) ::: _)) = a
nestedVecHead (NestedVec3 (((a ::: _) ::: _) ::: _)) = a

eqSizeNestedVec :: NestedVec n a -> NestedVec m b -> Maybe (n :~: m)
eqSizeNestedVec (NestedVec1 _) (NestedVec1 _) = Just Refl
eqSizeNestedVec (NestedVec2 _) (NestedVec2 _) = Just Refl
eqSizeNestedVec (NestedVec3 _) (NestedVec3 _) = Just Refl
eqSizeNestedVec _ _ = Nothing

eqNestedVec :: (a -> a -> Bool) -> NestedVec n a -> NestedVec n a -> Bool
eqNestedVec f nva nvb =
  let (sa, eltsA) = unNest nva
      (sb, eltsB) = unNest nvb
      eltsSame = getAll $ mconcat $ All <$> zipWith f eltsA eltsB
  in sa == sb && eltsSame

eqVecLength :: Vec n a -> Vec m b -> Maybe (n :~: m)
eqVecLength = go
  where
    go :: forall n m a b.Vec n a -> Vec m b -> Maybe (n :~: m)
    go VNil VNil = Just Refl
    go (_ ::: as) (_ ::: bs) = case go as bs of
      Just Refl -> Just Refl
      Nothing -> Nothing
    go _ _ = Nothing

eqVecEltType :: forall a b m n.(Typeable a, Typeable b) => Vec n a -> Vec m b -> Maybe (a :~: b)
eqVecEltType _ _ = testEquality (typeRep @a) (typeRep @b)

eqVec :: (Typeable a, Typeable b, Eq a) => Vec n a -> Vec m b -> Bool
eqVec v1 v2 = case eqVecLength v1 v2 of
  Just Refl -> case eqVecEltType v1 v2 of
    Just Refl -> DT.toList v1 == DT.toList v2
    Nothing -> False
  Nothing -> False


unNest :: NestedVec n a -> ([Int], [a])
unNest (NestedVec1 v) = ([DT.length v], DT.toList v)
unNest (NestedVec2 v) = ([DT.length v, DT.length (DT.head v)], concatMap DT.toList $ DT.toList v)
unNest (NestedVec3 v) = ([DT.length v, DT.length (DT.head v), DT.length (DT.head (DT.head v))], concat $ concatMap (fmap DT.toList . DT.toList) (DT.toList v))

{-
eTypeDim :: SType e -> Nat
eTypeDim = \case
  SInt -> n0
  SReal -> n0
  SComplex -> n0
  SCVec -> n1
  SRVec -> n1
  SMat -> n2
  SSqMat -> n2
  SArray sn st -> DT.snatToNat sn + eTypeDim st
  SBool -> n0
-}

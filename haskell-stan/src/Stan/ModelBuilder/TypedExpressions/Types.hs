{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}

module Stan.ModelBuilder.TypedExpressions.Types
  (
    module Stan.ModelBuilder.TypedExpressions.Types
  , Nat(..)
  , SNat(..)
  )
  where

import qualified Stan.ModelBuilder.TypedExpressions.Recursion as TR

import Prelude hiding (Nat)
import           Data.Kind (Type)

import Data.Type.Nat (Nat(..), SNat(..))
import qualified Data.Type.Nat as DT


-- possible structure of expressions
data EStructure = EVar | ELit | ECompound | ELookup deriving (Show)

-- EStructure Singleton
data SStructure :: EStructure -> Type where
  SVar :: SStructure EVar
  SLit :: SStructure ELit
  SCompound :: SStructure ECompound
  SLookup :: SStructure ELookup

withStructure :: EStructure -> (forall s.SStructure s -> r) -> r
withStructure EVar k = k SVar
withStructure ELit k = k SLit
withStructure ECompound k = k SCompound
withStructure ELookup k = k SLookup

-- possible types of terms
data EType = EInt | EReal | EComplex | ECVec | ERVec | EMat | ESqMat | EArray Nat EType

data Ty = Ty EStructure EType

type family TyStructure (a :: Ty) :: EStructure where
  TyStructure ('Ty s _) = s

type family TyType (a :: Ty) :: EType where
  TyType ('Ty _ et) = et

-- EType singleton
data SType :: EType -> Type where
  SInt :: SType EInt
  SReal :: SType EReal
  SComplex :: SType EComplex
  SCVec :: SType ECVec
  SRVec :: SType ERVec
  SMat :: SType EMat
  SSqMat :: SType ESqMat
  SArray :: SNat n -> SType t -> SType (EArray n t)

withSType :: forall t r.EType -> (forall t. SType t -> r) -> r
withSType EInt k = k SInt
withSType EReal k = k SReal
withSType EComplex k = k SComplex
withSType ERVec k = k SRVec
withSType ECVec k = k SCVec
withSType EMat k = k SMat
withSType ESqMat k = k SSqMat
withSType (EArray n t) k = DT.reify n f
  where
    f :: forall n. DT.SNatI n => Proxy n -> r
    f _ = withSType t $ \st -> k (SArray (DT.snat @n)  st)

data StanType :: EType -> Type where
  StanInt :: StanType EInt
  StanReal :: StanType EReal
  StanComp :: StanType EComplex
  StanArray :: SNat n -> StanType et -> StanType (EArray n et)
  StanVector :: StanType ECVec
  StanMatrix :: StanType EMat
  StanCorrMatrix :: StanType ESqMat
  StanCholeskyFactorCorr :: StanType ESqMat
  StanCovMatrix :: StanType ESqMat


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

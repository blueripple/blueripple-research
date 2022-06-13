{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}
{-# LANGUAGE LambdaCase #-}

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

import Data.Type.Equality ((:~:)(Refl), TestEquality(testEquality))
import Data.Type.Nat (Nat(..), SNat(..))
import qualified Data.Type.Nat as DT

import qualified GHC.TypeLits as TE
import GHC.TypeLits (ErrorMessage((:<>:)))
--import Data.GADT.Compare (GEq(geq))

-- possible types of terms
-- NB: zero dimensional array will be treated as the underlying type
data EType = EVoid | EString | EBool | EInt | EReal | EComplex | ECVec | ERVec | EMat | ESqMat | EArray Nat EType deriving (Eq, Show)

type family IfNumber (et :: EType) (a :: k) (b :: k) :: k where
  IfNumber EInt a _ = a
  IfNumber EReal a _ = a
  IfNumber EComplex a _ = a
  IfNumber _ _ b = b

type family IfNumbers (a :: EType) (b :: EType) (c :: k) (d :: k) where
  IfNumbers a b c d = IfNumber a (IfNumber b c d) d

type family Promoted (a :: EType) (b :: EType) :: EType where
  Promoted a a = a
  Promoted EInt EReal = EReal
  Promoted EReal EInt = EReal
  Promoted EInt EComplex = EComplex
  Promoted EComplex EInt = EComplex
  Promoted EReal EComplex = EComplex
  Promoted EComplex EReal = EComplex
  Promoted a b = TE.TypeError (TE.Text "One of " :<>: TE.ShowType a :<>: TE.Text " and " :<>: TE.ShowType b :<>: TE.Text " isn't a promotable (number) type.")

--Stan's modifiers (e.g. "<lower=2>" apply to the internal type in an array.)
type family ScalarType (et :: EType) :: EType where
  ScalarType (EArray _ t) = ScalarType t
  ScalarType ECVec = EReal
  ScalarType ERVec = EReal
  ScalarType EMat = EReal
  ScalarType ESqMat = EReal
  ScalarType a = a

-- EType singleton
data SType :: EType -> Type where
  SVoid :: SType EVoid
  SString :: SType EString
  SBool :: SType EBool
  SInt :: SType EInt
  SReal :: SType EReal
  SComplex :: SType EComplex
  SCVec :: SType ECVec
  SRVec :: SType ERVec
  SMat :: SType EMat
  SSqMat :: SType ESqMat
  SArray :: SNat n -> SType t -> SType (EArray n t)

instance Eq (SType t) where
  SVoid == SVoid = True
  SString == SString = True
  SBool == SBool = True
  SInt == SInt = True
  SReal == SReal = True
  SComplex == SComplex = True
  SCVec == SCVec = True
  SRVec == SRVec = True
  SMat == SMat = True
  SSqMat == SSqMat = True
  SArray n st == SArray n' st' = (DT.snatToNat n == DT.snatToNat n') && (st == st')


sTypeToEType :: SType t -> EType
sTypeToEType = \case
  SVoid -> EVoid
  SString -> EString
  SBool -> EBool
  SInt -> EInt
  SReal -> EReal
  SComplex -> EComplex
  SCVec -> ECVec
  SRVec -> ERVec
  SMat -> EMat
  SSqMat -> ESqMat
  SArray sn st -> case DT.snatToNat sn of
    Z -> sTypeToEType st
    S n -> EArray (S n) $ sTypeToEType st

withSType :: forall t r.EType -> (forall t. SType t -> r) -> r
withSType EVoid k = k SVoid
withSType EString k = k SString
withSType EBool k = k SBool
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

sTypeName :: SType t -> Text
sTypeName = \case
  SVoid -> "void"
  SString -> "string"
  SBool -> "bool"
  SInt -> "int"
  SReal -> "real"
  SComplex -> "complex"
  SCVec -> "vector"
  SRVec -> "row_vector"
  SMat -> "matrix"
  SSqMat -> "matrix"
  SArray _ _ -> "array"

data StanType :: EType -> Type where
--  StanVoid :: StanType EVoid
--  StanString :: StanType EString
--  StanBool :: StanType EBool
  StanInt :: StanType EInt
  StanReal :: StanType EReal
  StanComplex :: StanType EComplex
  StanArray :: SNat n -> StanType et -> StanType (EArray n et)
  StanVector :: StanType ECVec
  StanOrdered :: StanType ECVec
  StanPositiveOrdered :: StanType ECVec
  StanSimplex :: StanType ECVec
  StanUnitVector :: StanType ECVec
  StanRowVector :: StanType ERVec
  StanMatrix :: StanType EMat
  StanCorrMatrix :: StanType ESqMat
  StanCholeskyFactorCorr :: StanType ESqMat
  StanCovMatrix :: StanType ESqMat
  StanCholeskyFactorCov :: StanType ESqMat

stanTypeName :: StanType t -> Text
stanTypeName = \case
  StanInt -> "int"
  StanReal -> "real"
  StanComplex -> "complex"
  StanArray sn st -> "array"
  StanVector -> "vector"
  StanOrdered -> "ordered"
  StanPositiveOrdered -> "positive_ordered"
  StanSimplex -> "simplex"
  StanUnitVector -> "unit_vector"
  StanRowVector -> "row_vector"
  StanMatrix -> "matrix"
  StanCorrMatrix -> "corr_matrix"
  StanCholeskyFactorCorr -> "cholesky_factor_corr"
  StanCovMatrix -> "cov_matrix"
  StanCholeskyFactorCov -> "cholesky_factor_cov"

eTypeFromStanType :: StanType t -> EType
eTypeFromStanType = \case
  StanInt -> EInt
  StanReal -> EReal
  StanComplex -> EComplex
  StanArray sn st -> EArray (DT.snatToNat sn) (eTypeFromStanType st)
  StanVector -> ECVec
  StanOrdered -> ECVec
  StanPositiveOrdered -> ECVec
  StanSimplex -> ECVec
  StanUnitVector -> ECVec
  StanRowVector -> ERVec
  StanMatrix -> EMat
  StanCorrMatrix -> ESqMat
  StanCholeskyFactorCorr -> ESqMat
  StanCovMatrix -> ESqMat
  StanCholeskyFactorCov -> ESqMat

sTypeFromStanType :: StanType t -> SType t
sTypeFromStanType = \case
  StanInt -> SInt
  StanReal -> SReal
  StanComplex -> SComplex
  StanArray sn st -> SArray sn (sTypeFromStanType st)
  StanVector -> SCVec
  StanOrdered -> SCVec
  StanPositiveOrdered -> SCVec
  StanSimplex -> SCVec
  StanUnitVector -> SCVec
  StanRowVector -> SRVec
  StanMatrix -> SMat
  StanCorrMatrix -> SSqMat
  StanCholeskyFactorCorr -> SSqMat
  StanCovMatrix -> SSqMat
  StanCholeskyFactorCov -> SSqMat

instance TestEquality SType where
  testEquality SVoid SVoid = Just Refl
  testEquality SString SString = Just Refl
  testEquality SBool SBool = Just Refl
  testEquality SInt SInt = Just Refl
  testEquality SReal SReal = Just Refl
  testEquality SComplex SComplex = Just Refl
  testEquality SCVec SCVec = Just Refl
  testEquality SRVec SRVec = Just Refl
  testEquality SMat SMat = Just Refl
  testEquality SSqMat SSqMat = Just Refl
  testEquality (SArray sn sa) (SArray sm sb) = do
    Refl <- testEquality sa sb
    Refl <- testEquality sn sm
    pure Refl
  testEquality _ _ = Nothing


{-
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


data Ty = Ty EStructure EType

type family TyStructure (a :: Ty) :: EStructure where
  TyStructure ('Ty s _) = s

type family TyType (a :: Ty) :: EType where
  TyType ('Ty _ et) = et
-}

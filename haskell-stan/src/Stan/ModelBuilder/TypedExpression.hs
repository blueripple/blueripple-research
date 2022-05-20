{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}

module Stan.ModelBuilder.TypedExpression
  (

  )
  where

import Prelude hiding (All, group)
import qualified Control.Foldl as Fold
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

import qualified Text.PrettyPrint as Pretty

import GHC.Generics (Generic1)

import Stan.ModelBuilder.Expressions as SME
import Stan.ModelBuilder.Expressions (StanDim())
import Stan.ModelBuilder (DataSetGroupIntMaps)

data SStanDim :: StanDim -> Type where
  SNamedDim :: SME.IndexKey -> SStanDim NamedDim

-- possible types of terms
data EType = ENull | EInt | EReal | EVec StanDim | EMat (StanDim, StanDim) | ESqMat StanDim | EArray EType | EType :-> EType

-- singleton so that term types may be used at type-level
data SType :: EType -> Type where
  SNull :: SType ENull
  SInt :: SType EInt
  SReal :: SType EReal
  SVec :: SType EVec
  SMat :: SType EMat
  SSqMat :: SType ESqMat
  SArray :: SType t -> SType (EArray t)
  (::->) :: SType arg -> SType res -> SType (arg :-> res)

toSType :: EType -> (forall t. SType t -> r) -> r
toSType ENull k = k SNull
toSType EInt k = k SInt
toSType EReal k = k SReal
toSType EVec k = k SVec
toSType EMat k = k SMat
toSType ESqMat k = k SSqMat
toSType (EArray t) k = toSType t $ \st -> k (SArray st)
toSType (a :-> b) k = toSType a $ \sa -> toSType b $ \sb -> k (sa ::-> sb)

data (a :: k) :~: (b :: k) where
  Refl :: a :~: a

class TestEquality (t :: k -> Type) where
  testEquality :: t a -> t b -> Maybe (a :~: b)

instance TestEquality SType where
  testEquality SNull SNull = Just Refl
  testEquality SInt SInt = Just Refl
  testEquality SReal SReal = Just Refl
  testEquality SVec SVec = Just Refl
  testEquality SMat SMat = Just Refl
  testEquality SSqMat SSqMat = Just Refl
  testEquality (SArray sa) (SArray sb) = do
    Refl <- testEquality sa sb
    pure Refl
  testEquality (a1 ::-> r1) (a2 ::-> r2) = do
    Refl <- testEquality a1 a2
    Refl <- testEquality r1 r2
    return Refl
  testEquality _ _ = Nothing

{-
toEType :: SME.StanType -> EType
toEType

data TStanVar :: EType -> Type where
  TStanInt :: SME.StanName -> TStanVar EInt
  TStanReal :: SME.StanName -> TStanVar EReal
  TStanVector :: SME.StanName -> SME.StanDim -> TStanVar EVec
  TStanArray :: SME.StanName -> [SME.StanDim] -> SType t -> TStanVar (EArray t)

tStanVar :: SME.StanName -> SME.StanType -> TStanVar t
tStanVar n SME.StanInt = (SME.StanVar )

data TStanExprF (t :: EType) a where
  NullF :: TStanExprF ENull a
  BareF :: Text -> SType t -> TStanExprF t a
  AsIsF :: a -> SType t -> TStanExprF t a -- required to rewrap Declarations
--  NextToF :: a -> a -> TStanExprF a
  LookupCtxtF :: LookupContext -> a -> SType t -> TStanExprF t a
  NamedVarF :: StanVar -> TStanExprF t a
  MatMultF :: StanVar -> StanVar -> StanExprF a -- yeesh.  This is too specific?
  UseTExprF :: TExpr a -> StanExprF a
  IndexF :: IndexKey -> StanExprF a -- pre-lookup
  VectorizedF :: Set IndexKey -> a -> StanExprF a
  IndexesF :: [StanDim] -> StanExprF a
  VectorFunctionF :: Text -> a -> [a] -> StanExprF a -- function to add when the context is vectorized
  deriving  stock (Eq, Ord, Functor, Foldable, Traversable, Generic1)
  deriving   (FC.Show1, FC.Eq1, FC.Ord1) via FC.FunctorClassesDefault StanExprF
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

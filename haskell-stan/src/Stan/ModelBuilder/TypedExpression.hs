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
{-# LANGUAGE LambdaCase #-}

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
import Stan.ModelBuilder.Expressions (StanVar(..), StanType(..))
import Stan.ModelBuilder (DataSetGroupIntMaps)

-- possible types of terms
data EType = ENull | EInt | EReal | ECVec | ERVec | EMat | EArray EType | EType :-> EType

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

-- singleton so that term types may be used at type-level
data SType :: EType -> Type where
  SNull :: SType ENull
  SInt :: SType EInt
  SReal :: SType EReal
  SCVec :: SType ECVec
  SRVec :: SType ERVec
  SMat :: SType EMat
  SArray :: SType t -> SType (EArray t)
  (::->) :: SType arg -> SType res -> SType (arg :-> res)


toSType :: EType -> (forall t. SType t -> r) -> r
toSType ENull k = k SNull
toSType EInt k = k SInt
toSType EReal k = k SReal
toSType ERVec k = k SRVec
toSType ECVec k = k SCVec
toSType EMat k = k SMat
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
  testEquality SCVec SCVec = Just Refl
  testEquality SRVec SRVec = Just Refl
  testEquality SMat SMat = Just Refl
  testEquality (SArray sa) (SArray sb) = do
    Refl <- testEquality sa sb
    pure Refl
  testEquality (a1 ::-> r1) (a2 ::-> r2) = do
    Refl <- testEquality a1 a2
    Refl <- testEquality r1 r2
    return Refl
  testEquality _ _ = Nothing

data UExpr :: EType -> Type where
  UNamedE :: Text -> SME.StanType -> SType t -> UExpr t

makeNamed :: Text -> SME.StanType -> SType t -> UExpr t
makeNamed = UNamedE

fromVar :: SME.StanVar -> UExpr t
fromVar (SME.StanVar n x) = case x of
  StanInt -> UNamedE n x SInt
  StanReal -> _
  StanArray sds st -> _
  StanVector sd -> _
  StanMatrix x1 -> _
  StanCorrMatrix sd -> _
  StanCholeskyFactorCorr sd -> _
  StanCovMatrix sd -> _




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

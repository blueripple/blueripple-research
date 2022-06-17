{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}
{-# HLINT ignore "Use camelCase" #-}

module Stan.ModelBuilder.TypedExpressions.TypedList
  (
    module Stan.ModelBuilder.TypedExpressions.TypedList
  )
  where

import Stan.ModelBuilder.TypedExpressions.Types
import Stan.ModelBuilder.TypedExpressions.Recursion

import Prelude hiding (Nat)
import           Data.Kind (Type)

import qualified GHC.TypeLits as TE
import GHC.TypeLits (ErrorMessage((:<>:)))
import Data.Hashable.Generic (HashArgs)
import Data.Type.Equality ((:~:)(Refl), TestEquality(testEquality))

-- singleton for a list of arguments
data TypeList :: [EType] -> Type where
  TypeNil :: TypeList '[]
  (::>) :: SType et -> TypeList ets -> TypeList (et ': ets)

infixr 2 ::>

instance TestEquality TypeList where
  testEquality TypeNil TypeNil = Just Refl
  testEquality (sta ::> as) (stb ::> bs) = do
    Refl <- testEquality sta stb
    Refl <- testEquality as bs
    pure Refl
  testEquality _ _ = Nothing

eqTypeList :: TypeList es -> TypeList es' -> Bool
eqTypeList = go
  where
    go :: TypeList es -> TypeList es' -> Bool
    go TypeNil TypeNil = True
    go (sta ::> as) (stb ::> bs) = case testEquality sta stb of
      Just Refl -> go as bs
      Nothing -> False
    go _ _ = False

typesToList ::  (forall t.SType t -> a) -> TypeList args -> [a]
typesToList _ TypeNil = []
typesToList f (st ::> ats) = f st : typesToList f ats

typeListToTypedListOfTypes :: TypeList args -> TypedList SType args
typeListToTypedListOfTypes TypeNil = TNil
typeListToTypedListOfTypes (st ::> atl) = st :> typeListToTypedListOfTypes atl

oneType :: SType et -> TypeList '[et]
oneType st = st ::> TypeNil

-- list of arguments.  Parameterized by an expression type and the list of arguments
data TypedList ::  (EType -> Type) -> [EType] -> Type where
  TNil :: TypedList f '[]
  (:>) :: f et -> TypedList f ets -> TypedList f (et ': ets)

infixr 2 :>

instance HFunctor TypedList where
  hfmap nat = \case
    TNil -> TNil
    (:>) get al -> nat get :> hfmap nat al

instance HTraversable TypedList where
  htraverse natM = \case
    TNil -> pure TNil
    (:>) aet al -> (:>) <$> natM aet <*> htraverse natM al
  hmapM = htraverse

type family (as :: [k]) ++ (bs :: [k]) :: [k] where
  '[] ++ bs = bs
  (a ': as) ++ bs = a ': (as ++ bs)

appendTypedLists :: TypedList u as -> TypedList u bs -> TypedList u (as ++ bs)
appendTypedLists TNil b = b
appendTypedLists (a :> as) b = a :> appendTypedLists as b

zipTypedListsWith :: (forall x. a x -> b x -> c x) -> TypedList a args -> TypedList b args -> TypedList c args
zipTypedListsWith _ TNil TNil = TNil
zipTypedListsWith f (a :> as) (b :> bs) = f a b :> zipTypedListsWith f as bs

eqTypedLists :: forall (t ::EType -> Type) es. (forall a.t a -> t a -> Bool) -> TypedList t es -> TypedList t es -> Bool
eqTypedLists f a b = getAll $ mconcat $ All <$> typedKToList (zipTypedListsWith (\x y -> K $ f x y) a b)

{-
eqArgLists :: forall (t ::EType -> Type) es es'. (forall a.t a -> t a -> Bool) -> ArgList t es -> ArgList t es' -> Bool
eqArgLists f = go
  where
    go :: ArgList t ls -> ArgList t ls' -> Bool
    go ArgNil ArgNil = True
    go (x :> xs) (y :> ys) = f x y && go xs ys
-}

typedKToList :: TypedList (K a) ts -> [a]
typedKToList TNil = []
typedKToList (a :> al) = unK a : typedKToList al

oneTyped :: f et -> TypedList f '[et]
oneTyped e = e :> TNil

typeListToSTypeList :: TypeList args -> TypedList SType args
typeListToSTypeList TypeNil = TNil
typeListToSTypeList (st ::> atl) = st :> typeListToSTypeList atl

typedSTypeListToTypeList :: TypedList SType args -> TypeList args
typedSTypeListToTypeList TNil = TypeNil
typedSTypeListToTypeList (st :> xs) = st ::> typedSTypeListToTypeList xs

applyTypedListFunctionToTypeList :: (forall u.TypedList u args -> TypedList u args') -> TypeList args -> TypeList args'
applyTypedListFunctionToTypeList f = typedSTypeListToTypeList . f . typeListToSTypeList

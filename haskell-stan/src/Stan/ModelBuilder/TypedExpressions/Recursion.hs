{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE StandaloneKindSignatures #-}
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}

module Stan.ModelBuilder.TypedExpressions.Recursion where

import Data.Kind (Type)

newtype IFix (f :: (k -> Type) -> (k -> Type)) (t :: k) = IFix { unIFix :: f (IFix f) t }

type (~>)  :: (k -> Type) -> (k -> Type) -> Type
type (f ~> g) = forall x. f x -> g x

--type IAlg ::  _ --(k -> Type) -> (k -> Type) -> Type
type IAlg f g = f g ~> g

type ICoAlg f g = g ~> f g

class NFunctor f where
  nmap :: (g ~> h) -> (f g ~> f h)

-- poly-kinded const functor
newtype K (a :: Type) (t :: k) = K { unK :: a }

iCata :: forall f g. NFunctor f => IAlg f g -> (IFix f ~> g)
iCata alg  = c
  where
    c :: IFix f ~> g
    c = alg . nmap c . unIFix

iAna :: forall f g.NFunctor f => ICoAlg f g -> (g ~> IFix f)
iAna coalg = a
  where
    a :: g ~> IFix f
    a = IFix . nmap a . coalg

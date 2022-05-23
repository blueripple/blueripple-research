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

newtype IFix (f :: (k -> Type) -> (k -> Type)) (t :: k) = IFix (f (IFix f) t)

type (f ~> g) = forall x. f x -> g x

class NFunctor f where
  nmap :: (g ~> h) -> (f g ~> f h)

cata :: NFunctor f => (f g ~> g) -> (IFix f ~> g)
cata alg (IFix f) = alg (nmap (cata alg) f)

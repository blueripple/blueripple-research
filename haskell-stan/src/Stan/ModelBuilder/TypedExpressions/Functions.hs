{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}
{-# HLINT ignore "Use camelCase" #-}

module Stan.ModelBuilder.TypedExpressions.Functions
  (
    module Stan.ModelBuilder.TypedExpressions.Functions
  )
  where

import Stan.ModelBuilder.TypedExpressions.Types
import Stan.ModelBuilder.TypedExpressions.TypedList
import Stan.ModelBuilder.TypedExpressions.Recursion


import Prelude hiding (Nat)
import           Data.Kind (Type)

import qualified GHC.TypeLits as TE
import GHC.TypeLits (ErrorMessage((:<>:)))
import Data.Hashable.Generic (HashArgs)
import Data.Type.Equality ((:~:)(Refl), TestEquality(testEquality))


data Function :: EType -> [EType] -> Type  where
  Function :: Text
           -> SType t
           -> TypeList args
           -> (forall u.TypedList u args -> TypedList u args') -- allows remapping of args at function application
           -> Function t args
  IdentityFunction :: SType t -> Function t '[t]

--curryOneF :: Function t ts -> Function (t ::-> LastType ts) (AllButLast ts)
--curryOneF (Function n st tl tF) = Function n

-- Can't pattern match on the arg-mapping function in "where" or "let" since then args' would escape its scope.
-- But we can do this
withFunction :: (forall args'. Text -> SType t -> TypeList args -> (forall u.TypedList u args -> TypedList u args') -> r)
                -> Function t args
                -> r
withFunction f (Function t st tl g) = f t st tl g
withFunction f (IdentityFunction st) = f "" st (st ::> TypeNil) id

--simpleFunction :: (GenSType t, AllGenTypes args) => Text -> SType t -> TypeList args -> Function t args
--simpleFunction fn st args = Function fn st args id

simpleFunction :: (GenSType t, GenTypeList args) => Text -> Function t args
simpleFunction fn  = Function fn genSType genTypeList id


functionArgTypes :: Function rt args -> TypeList args
functionArgTypes (Function _ _ al _) = al
functionArgTypes (IdentityFunction t) = t ::> TypeNil

data Density :: EType -> [EType] -> Type where
  Density :: Text -- name
          -> SType t -- givens type
          -> TypeList args -- argument types
          -> (forall u.TypedList u args -> TypedList u args') -- allows remapping of args at function applicatio
          -> Density t args

withDensity :: (forall args' . Text -> SType t -> TypeList args -> (forall u.TypedList u args -> TypedList u args') -> r)
            -> Density t args
            -> r
withDensity f (Density dn st tl g) = f dn st tl g

simpleDensity :: (GenSType t, AllGenTypes ts, GenTypeList ts) => Text -> Density t ts
simpleDensity t  = Density t genSType genTypeList id

-- const functor for holding arguments to functions
data FuncArg :: Type -> k -> Type where
  Arg :: a -> FuncArg a r
  DataArg :: a -> FuncArg a r

funcArgName :: FuncArg Text a -> Text
funcArgName = \case
  Arg txt -> txt
  DataArg txt -> txt

mapFuncArg :: (a -> b) -> FuncArg a r -> FuncArg b r
mapFuncArg f = \case
  Arg a -> Arg $ f a
  DataArg a -> DataArg $ f a

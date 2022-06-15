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
  Function :: Text -- name
           -> SType r -- return type
           -> TypeList args -- argument types
           -> Function r args
  IdentityFunction :: SType t -> Function t '[t]

functionArgTypes :: Function rt args -> TypeList args
functionArgTypes (Function _ _ al) = al
functionArgTypes (IdentityFunction t) = t ::> TypeNil

data Density :: EType -> [EType] -> Type where
  Density :: Text -- name
          -> SType g -- givens type
          -> TypeList args -- argument types
          -> Density g args

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

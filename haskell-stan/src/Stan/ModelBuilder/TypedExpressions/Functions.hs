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

module Stan.ModelBuilder.TypedExpressions.Functions
  (
    module Stan.ModelBuilder.TypedExpressions.Functions
  )
  where

import Stan.ModelBuilder.TypedExpressions.Types
import Stan.ModelBuilder.TypedExpressions.Recursion

import Prelude hiding (Nat)
import           Data.Kind (Type)

import qualified GHC.TypeLits as TE
import GHC.TypeLits (ErrorMessage((:<>:)))
import Data.Hashable.Generic (HashArgs)


-- singleton for a list of arguments
data ArgTypeList :: [EType] -> Type where
  ArgTypeNil :: ArgTypeList '[]
  (::>) :: SType et -> ArgTypeList ets -> ArgTypeList (et ': ets)


{- ?? Maybe not poly-kinded enough?
-- list of types to parameterize a list of arguments
data family ArgTypeList (ts :: [EType])
data instance ArgTypeList '[] = ArgTypeNil
data instance ArgTypeList (et ': ets) = (::>) et (ArgTypeList ets)

deriving instance Eq (ArgList '[])
deriving instance (Eq x, Eq (ArgList xs)) => Eq (ArgList (x ': xs))
-}

oneArgType :: SType et -> ArgTypeList '[et]
oneArgType st = st ::> ArgTypeNil

-- list of arguments.  Parameterized by an expression type and the list of arguments
data ArgList ::  (EType -> Type) -> [EType] -> Type where
  ArgNil :: ArgList f '[]
  (:>) :: f et -> ArgList f ets -> ArgList f (et ': ets)

instance HFunctor ArgList where
  hfmap nat = \case
    ArgNil -> ArgNil
    (:>) get al -> nat get :> hfmap nat al

instance HTraversable ArgList where
  htraverse natM = \case
    ArgNil -> pure ArgNil
    (:>) aet al -> (:>) <$> natM aet <*> htraverse natM al
  hmapM = htraverse

oneArg :: f et -> ArgList f '[et]
oneArg e = e :> ArgNil

data Function :: EType -> [EType] -> Type  where
  Function :: Text -> SType r -> ArgTypeList args -> Function r args

data Distribution :: EType -> [EType] -> Type where
  Distribution :: Text -> SType g -> ArgTypeList args -> Distribution g args


logit :: Function EReal '[EReal]
logit = Function "logit" SReal (oneArgType SReal)

invLogit :: Function EReal '[EReal]
invLogit = Function "inv_logit" SReal (oneArgType SReal)

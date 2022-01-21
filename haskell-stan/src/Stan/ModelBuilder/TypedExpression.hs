{-# LANGUAGE DataKinds #-}
--{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveFoldable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveTraversable #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TupleSections #-}
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

import qualified Data.Fix as Fix
import qualified Data.Map.Strict as Map
import qualified Data.Set as Set
import qualified Data.Text as T

import qualified Text.PrettyPrint as Pretty

import GHC.Generics (Generic1)

import Stan.ModelBuilder.Expressions as SME

type TypedExpression = TypedExpression { stanType :: SME.StanType, stanExpr :: SME.StanExpression }

int :: TypedExpression
int n = TypedExpression (Show n) SME.StanInt

real x :: TypedExpression
real x = TypedExpression (Show x) SME.StanReal

binOp :: (SME.StanType -> SME.StanType -> Either Text SME.StanType)
         -> (SME.StanExpr -> SME.StanExpr -> SME.StanExpr)
         -> TypedExpression
         -> TypedExpression
         -> Either Text TypedExpression
binOp newTypeE newExpr te1 te2 = do
  typ <- newTypeE (stanType te1) (stanType te2)
  let expr = newExpr (stanExpr te1) (stanExpr te2)
  return $ TypedExpression typ expr

sameType :: Text -> SME.StanType -> SME.StanType -> Either Text SME.StanType
sameType ctxt t1 t2 = if t1 == t2 then Right t1 else Left ("Mismatched types in " <> ctxt)

--allowedTypes ::

times :: TypedExpression -> TypedExpression
times = binOp (sameType "times") SME.binOp "*"

plus :: TypedExpression -> TypedExpression
plus = binOp (sameType "times") SME.binOp "+"

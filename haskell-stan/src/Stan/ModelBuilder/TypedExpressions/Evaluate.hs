{-# LANGUAGE DataKinds #-}
--{-# LANGUAGE DeriveTraversable #-}
--{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE StandaloneKindSignatures #-}
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}

module Stan.ModelBuilder.TypedExpressions.Evaluate where

import Stan.ModelBuilder.TypedExpressions.Expression

import Prelude hiding (Nat, (+))
import qualified Data.Comp.Multi as DC
import           Data.Kind (Type)
import qualified Data.Fix as Fix
import qualified Data.Text as T

import qualified Text.PrettyPrint as Pretty
import GHC.TypeLits (ErrorMessage(Text))

stanDeclHead :: StanType t -> DeclIndexVec t -> Text
stanDeclHead st iv = case st of
  StanInt -> "int"
  StanReal -> "real"
  StanArray sn st -> "array" <> indexText (take )
  StanVector -> "vector" <> indexText iv
  StanMatrix -> "matrix" <> indexText iv
  StanCorrMatrix -> "corr_matrix" <> indexText iv
  StanCholeskyFactorCorr -> "cholesky_factor_corr" indexText iv
  StanCovMatrix -> "cov_matrix" <> indexText iv

indexText :: Vec (UExprF EInt) n -> Text
indexText VNil = ""
indexText (i :> v) = "[" <> go i v <> "]" where
  printIndex :: UExprF EInt -> Text
  printIndex = undefined
  go :: UExprF EInt -> Vec (UExprF EInt) m -> Text
  go ie VNil = printIndex ie
  go ie (ie' :> v') = printIndex ie <> "," <> go ie' v'


toStanCode :: UExprF t -> Text
toStanCode = \case
  DeclareE st vec -> _
  NamedE txt st -> txt_
  IntE n -> show n
  RealE x -> show x
  BinaryOpE sbo uef uef' -> _
  SliceE sn uef uef' -> _
  NamedIndexE txt -> _

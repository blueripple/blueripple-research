{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveTraversable #-}
{-# LANGUAGE DerivingVia #-}
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

import Prelude hiding (Nat, (+))

import Stan.ModelBuilder.TypedExpressions.Expression
import Stan.ModelBuilder.TypedExpressions.Recursion

import qualified Data.Functor.Classes as FC
import qualified Data.Functor.Classes.Generic as FC
import qualified Data.Functor.Foldable as Rec
import qualified Data.Functor.Foldable.Monadic as Rec
--import qualified Data.Fix as Fix
import GHC.Generics (Generic1)

import qualified Data.IntMap.Strict as IM

import qualified Data.Comp.Multi as DC
import           Data.Kind (Type)
import qualified Data.Fix as Fix
import qualified Data.Text as T

import qualified Text.PrettyPrint as Pretty
--import GHC.TypeLits (ErrorMessage(Text))
--import GHC.IO.Encoding (TextDecoder)

data CExprF :: Type -> Type where
  CText :: Text -> CExprF a
  CIndexed :: Text -> IM.IntMap Text -> CExprF a
  deriving  stock (Eq, Ord, Functor, Foldable, Traversable, Generic1)
  deriving   (FC.Show1, FC.Eq1, FC.Ord1) via FC.FunctorClassesDefault CExprF

type CExpr = Fix.Fix CExprF

cText :: Text -> CExpr
cText = Fix.Fix . CText

cIndexed :: Text -> Int -> Text -> CExpr
cIndexed t n it = Fix.Fix $ CIndexed t (IM.singleton n it)

cAppendIndexF :: Int -> Text -> CExprF a -> CExprF a
cAppendIndexF n it (CText t) = CIndexed t (IM.singleton n it)
cAppendIndexF n it (CIndexed t im) = CIndexed t im'
  where
    unIndexed :: Int -> [Int]
    unIndexed 0 _ = []
    unIndexed n im =
toCExprAlg :: IAlg UExprF (K CExpr)
toCExprAlg x = K $ case x of
  DeclareEF st divf -> undefined
  NamedEF txt _ -> txt
  IntEF n -> show n
  RealEF x -> show x
  BinaryOpEF sbo k k' -> unK k <> " " <> opText sbo <> " " <> unK k'
  SliceEF sn k k' -> undefined
  NamedIndexEF txt -> txt

{-
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

-}


opText :: SBinaryOp op -> Text
opText = \case
  SEqual -> "="
  SAdd -> "+"
  SSubtract -> "-"
  SMultiply -> "*"
  SDivide -> "/"
  SElementWise sbo -> "." <> opText sbo
  SAndEqual sbo -> opText sbo <> "="

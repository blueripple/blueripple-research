{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}
{-# LANGUAGE TypeSynonymInstances #-}

module Stan.ModelBuilder.TypedExpressions.Arithmetic
  (
    module Stan.ModelBuilder.TypedExpressions.Arithmetic
  )
  where

import qualified Stan.ModelBuilder.TypedExpressions.Recursion as TR
import Stan.ModelBuilder.TypedExpressions.Types

import Prelude hiding (Nat)
import           Data.Kind (Type)

import qualified GHC.TypeLits as TE
import GHC.TypeLits (ErrorMessage((:<>:)))


data BinaryOp = BEqual | BAdd | BSubtract | BMultiply | BDivide | BModulo | BElementWise BinaryOp | BAndEqual BinaryOp

data SBinaryOp :: BinaryOp -> Type where
  SEqual :: SBinaryOp BEqual
  SAdd :: SBinaryOp BAdd
  SSubtract :: SBinaryOp BSubtract
  SMultiply :: SBinaryOp BMultiply
  SDivide :: SBinaryOp BDivide
  SModulo :: SBinaryOp BModulo
  SElementWise :: SBinaryOp op -> SBinaryOp (BElementWise op)
  SAndEqual :: SBinaryOp op -> SBinaryOp (BElementWise op)

type family BinaryResultT (bo :: BinaryOp) (a :: EType) (b :: EType) :: EType where
  BinaryResultT BEqual a a = a -- ?
  BinaryResultT BEqual a b = TE.TypeError (TE.ShowType a :<>: TE.Text " and " :<>: TE.ShowType b :<>: TE.Text " cannot be set equal." )
  BinaryResultT BAdd a a = a
  BinaryResultT BAdd EInt EReal = EReal
  BinaryResultT BAdd EReal EInt = EReal
  BinaryResultT BAdd EInt EComplex = EComplex
  BinaryResultT BAdd EReal EComplex = EComplex
  BinaryResultT BAdd EComplex EInt = EComplex
  BinaryResultT BAdd EComplex EReal= EComplex
  BinaryResultT BAdd a b = TE.TypeError (TE.ShowType a :<>: TE.Text " and " :<>: TE.ShowType b :<>: TE.Text " cannot be added." )
  BinaryResultT BSubtract a a = a
  BinaryResultT BSubtract EInt EReal = EReal
  BinaryResultT BSubtract EReal EInt = EReal
  BinaryResultT BSubtract EInt EComplex = EComplex
  BinaryResultT BSubtract EReal EComplex = EComplex
  BinaryResultT BSubtract EComplex EInt = EComplex
  BinaryResultT BSubtract EComplex EReal= EComplex
  BinaryResultT BSubtract a b = TE.TypeError (TE.ShowType a :<>: TE.Text " and " :<>: TE.ShowType b :<>: TE.Text " cannot be subtracted." )
  BinaryResultT BMultiply EInt a = a
  BinaryResultT BMultiply a EInt = a
  BinaryResultT BMultiply EReal a = a
  BinaryResultT BMultiply a EReal = a
  BinaryResultT BMultiply EComplex a = a
  BinaryResultT BMultiply a EComplex = a
  BinaryResultT BMultiply ERVec ECVec = EReal -- dot product
  BinaryResultT BMultiply EMat EMat = EMat
  BinaryResultT BMultiply EMat ESqMat = EMat
  BinaryResultT BMultiply ESqMat EMat = EMat
  BinaryResultT BMultiply ESqMat ESqMat = EMat
  BinaryResultT BMultiply EMat ECVec = ECVec
  BinaryResultT BMultiply ESqMat ECVec = ECVec
  BinaryResultT BMultiply ERVec EMat = ERVec
  BinaryResultT BMultiply ERVec ESqMat = ERVec
  BinaryResultT BMultiply EMat ERVec = TE.TypeError (TE.Text "Cannot do matrix * row-vector.  Perhaps you meant to flip the order?")
  BinaryResultT BMultiply ESqMat ERVec = TE.TypeError (TE.Text "Cannot do matrix * row-vector.  Perhaps you meant to flip the order?")
  BinaryResultT BMultiply ECVec EMat = TE.TypeError (TE.Text "Cannot do col-vector * matrix.  Perhaps you meant to flip the order?")
  BinaryResultT BMultiply ECVec ESqMat = TE.TypeError (TE.Text "Cannot do col-vector * matrix.  Perhaps you meant to flip the order?")
  BinaryResultT BMultiply a (EArray n b) = EArray n (BinaryResultT BMultiply a b)
  BinaryResultT BMultiply a b = TE.TypeError (TE.ShowType a :<>: TE.Text " and " :<>: TE.ShowType b :<>: TE.Text " cannot be multiplied." )
  BinaryResultT BDivide EInt EInt = EReal
  BinaryResultT BDivide EInt EReal = EReal
  BinaryResultT BDivide EReal EInt = EReal
  BinaryResultT BDivide EComplex EComplex = EReal
  BinaryResultT BDivide EInt EComplex = EComplex
  BinaryResultT BDivide EReal EComplex = EComplex
  BinaryResultT BDivide EComplex EInt = EComplex
  BinaryResultT BDivide EComplex EReal = EComplex
  BinaryResultT BDivide a b = TE.TypeError (TE.ShowType a :<>: TE.Text " and " :<>: TE.ShowType b :<>: TE.Text " cannot be divided." )
  BinaryResultT (BElementWise op) (EArray n a) (EArray n b) = EArray n (BinaryResultT op a b)
  BinaryResultT (BElementWise op) ECVec ECVec = ECVec
  BinaryResultT (BElementWise op) ERVec ERVec = ERVec
  BinaryResultT (BElementWise op) EMat EMat = EMat
  BinaryResultT (BElementWise op) ESqMat ESqMat = EMat
  BinaryResultT (BElementWise op) EMat ESqMat = EMat
  BinaryResultT (BElementWise op) ESqMat EMat = EMat
  BinaryResultT (BElementWise _) a b = TE.TypeError (TE.ShowType a :<>: TE.Text " and " :<>: TE.ShowType b :<>: TE.Text " cannot be combined elementwise." )
  BinaryResultT (BAndEqual op) a b = BinaryResultT op a b

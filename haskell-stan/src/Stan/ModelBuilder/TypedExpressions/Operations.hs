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
{-# LANGUAGE LambdaCase #-}

module Stan.ModelBuilder.TypedExpressions.Operations
  (
    module Stan.ModelBuilder.TypedExpressions.Operations
  )
  where

import qualified Stan.ModelBuilder.TypedExpressions.Recursion as TR
import Stan.ModelBuilder.TypedExpressions.Types

import Prelude hiding (Nat)
import           Data.Kind (Type)

import qualified GHC.TypeLits as TE
import GHC.TypeLits (ErrorMessage((:<>:)))


data UnaryOp = UNegate | UTranspose

data SUnaryOp :: UnaryOp -> Type where
  SNegate :: SUnaryOp UNegate
  STranspose :: SUnaryOp UTranspose

type family UnaryResultT (uo :: UnaryOp) (a :: EType) :: EType where
  UnaryResultT UNegate a = a
  UnaryResultT UTranspose ECVec = ERVec
  UnaryResultT UTranspose ERVec = ECVec
  UnaryResultT UTranspose EMat = EMat
  UnaryResultT UTranspose ESqMat = ESqMat
  UnaryResultT UTranspose x = TE.TypeError (TE.ShowType x :<>:  TE.Text " cannot be set transposed.")

data BoolOp = BEq | BNEq | BLT | BLEq | BGT | BGEq | BAnd | BOr

data SBoolOp :: BoolOp -> Type where
  SEq :: SBoolOp BEq
  SNEq :: SBoolOp BNEq
  SLT :: SBoolOp BLT
  SLEq :: SBoolOp BLEq
  SGT :: SBoolOp BGT
  SGEq :: SBoolOp BGEq
  SAnd :: SBoolOp BAnd
  SOr :: SBoolOp BOr

type family BoolOpResultT (bo :: BoolOp) (a :: EType) (b :: EType) :: EType where
  BoolOpResultT BEq EString EString = EBool
  BoolOpResultT BNEq EString EString = EBool
  BoolOpResultT BLT EString EString = EBool
  BoolOpResultT BLEq EString EString = EBool
  BoolOpResultT BGT EString EString = EBool
  BoolOpResultT BGEq EString EString = EBool
  BoolOpResultT BEq a b = IfNumbers a b EBool (TE.TypeError (TE.ShowType a :<>: TE.Text " and " :<>: TE.ShowType b :<>: TE.Text " cannot be compared." ))
  BoolOpResultT BNEq a b = IfNumbers a b EBool (TE.TypeError (TE.ShowType a :<>: TE.Text " and " :<>: TE.ShowType b :<>: TE.Text " cannot be compared." ))
  BoolOpResultT BLT a b = IfNumbers a b EBool (TE.TypeError (TE.ShowType a :<>: TE.Text " and " :<>: TE.ShowType b :<>: TE.Text " cannot be compared." ))
  BoolOpResultT BLEq a b = IfNumbers a b EBool (TE.TypeError (TE.ShowType a :<>: TE.Text " and " :<>: TE.ShowType b :<>: TE.Text " cannot be compared." ))
  BoolOpResultT BGT a b = IfNumbers a b EBool (TE.TypeError (TE.ShowType a :<>: TE.Text " and " :<>: TE.ShowType b :<>: TE.Text " cannot be compared." ))
  BoolOpResultT BGEq a b = IfNumbers a b EBool (TE.TypeError (TE.ShowType a :<>: TE.Text " and " :<>: TE.ShowType b :<>: TE.Text " cannot be compared." ))
  BoolOpResultT BAnd EBool EBool = EBool
  BoolOpResultT BAnd a b = TE.TypeError (TE.Text "\"" :<>: TE.ShowType a :<>: TE.Text " && " :<>: TE.ShowType b :<>: TE.Text "\" is not defined." )
  BoolOpResultT BOr EBool EBool = EBool
  BoolOpResultT BOr a b = TE.TypeError (TE.Text "\"" :<>: TE.ShowType a :<>: TE.Text " || " :<>: TE.ShowType b :<>: TE.Text "\" is not defined." )

data BinaryOp = BAdd | BSubtract | BMultiply | BDivide | BPow | BModulo | BElementWise BinaryOp | BAndEqual BinaryOp

data SBinaryOp :: BinaryOp -> Type where
  SAdd :: SBinaryOp BAdd
  SSubtract :: SBinaryOp BSubtract
  SMultiply :: SBinaryOp BMultiply
  SDivide :: SBinaryOp BDivide
  SPow :: SBinaryOp BPow
  SModulo :: SBinaryOp BModulo
  SElementWise :: SBinaryOp op -> SBinaryOp (BElementWise op)
  SAndEqual :: SBinaryOp op -> SBinaryOp (BElementWise op)

type family BinaryResultT (bo :: BinaryOp) (a :: EType) (b :: EType) :: EType where
  BinaryResultT BAdd a a = a
  BinaryResultT BAdd a b = IfNumbers a b (Promoted a b) (TE.TypeError (TE.ShowType a :<>: TE.Text " and " :<>: TE.ShowType b :<>: TE.Text " cannot be added." ))
  BinaryResultT BSubtract EString EString = TE.TypeError (TE.Text "Stan strings cannot be subtracted!")
  BinaryResultT BSubtract a a = a
  BinaryResultT BSubtract a b = IfNumbers a b (Promoted a b) (TE.TypeError (TE.ShowType a :<>: TE.Text " and " :<>: TE.ShowType b :<>: TE.Text " cannot be subtracted." ))
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
  BinaryResultT BDivide a b = IfNumbers a b (Promoted a b) (TE.TypeError (TE.ShowType a :<>: TE.Text " and " :<>: TE.ShowType b :<>: TE.Text " cannot be divided." ))
  BinaryResultT BPow EInt EInt = EReal -- Stan spec
  BinaryResultT BPow EReal EReal = EReal
  BinaryResultT BPow EReal EInt = EReal
  BinaryResultT BPow EInt EReal = EReal
  BinaryResultT BPow x y = TE.TypeError (TE.ShowType x :<>: TE.Text " ^ " :<>: TE.ShowType y :<>: TE.Text " not allowed." )
  BinaryResultT BModulo EInt EInt = EInt
  BinaryResultT BModulo x y = TE.TypeError (TE.ShowType x :<>: TE.Text " % " :<>: TE.ShowType y :<>: TE.Text " not allowed." )
  BinaryResultT (BElementWise op) (EArray n a) (EArray n b) = EArray n (BinaryResultT op a b)
  BinaryResultT (BElementWise op) ECVec ECVec = ECVec
  BinaryResultT (BElementWise op) ERVec ERVec = ERVec
  BinaryResultT (BElementWise op) EMat EMat = EMat
  BinaryResultT (BElementWise op) ESqMat ESqMat = EMat
  BinaryResultT (BElementWise op) EMat ESqMat = EMat
  BinaryResultT (BElementWise op) ESqMat EMat = EMat
  BinaryResultT (BElementWise _) a b = TE.TypeError (TE.ShowType a :<>: TE.Text " and " :<>: TE.ShowType b :<>: TE.Text " cannot be combined elementwise." )
  BinaryResultT (BAndEqual op) a b = BinaryResultT op a b

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
{-# LANGUAGE TypeApplications #-}

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
import Data.Type.Equality ((:~:)(Refl), TestEquality(testEquality))

data UnaryOp = UNegate | UTranspose

data SUnaryOp :: UnaryOp -> Type where
  SNegate :: SUnaryOp UNegate
  STranspose :: SUnaryOp UTranspose

instance TestEquality SUnaryOp where
  testEquality SNegate SNegate = Just Refl
  testEquality STranspose STranspose = Just Refl
  testEquality _ _ = Nothing

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

instance TestEquality SBoolOp where
  testEquality SEq SEq = Just Refl
  testEquality SNEq SNEq = Just Refl
  testEquality SLT SLT = Just Refl
  testEquality SLEq SLEq = Just Refl
  testEquality SGT SGT = Just Refl
  testEquality SGEq SGEq = Just Refl
  testEquality SAnd SAnd = Just Refl
  testEquality SOr SOr = Just Refl
  testEquality _ _ = Nothing

type family BoolResultT (bo :: BoolOp) (a :: EType) (b :: EType) :: EType where
  BoolResultT BEq EString EString = EBool
  BoolResultT BNEq EString EString = EBool
  BoolResultT BLT EString EString = EBool
  BoolResultT BLEq EString EString = EBool
  BoolResultT BGT EString EString = EBool
  BoolResultT BGEq EString EString = EBool
  BoolResultT BEq a b = IfNumbers a b EBool (TE.TypeError (TE.ShowType a :<>: TE.Text " and " :<>: TE.ShowType b :<>: TE.Text " cannot be compared." ))
  BoolResultT BNEq a b = IfNumbers a b EBool (TE.TypeError (TE.ShowType a :<>: TE.Text " and " :<>: TE.ShowType b :<>: TE.Text " cannot be compared." ))
  BoolResultT BLT a b = IfNumbers a b EBool (TE.TypeError (TE.ShowType a :<>: TE.Text " and " :<>: TE.ShowType b :<>: TE.Text " cannot be compared." ))
  BoolResultT BLEq a b = IfNumbers a b EBool (TE.TypeError (TE.ShowType a :<>: TE.Text " and " :<>: TE.ShowType b :<>: TE.Text " cannot be compared." ))
  BoolResultT BGT a b = IfNumbers a b EBool (TE.TypeError (TE.ShowType a :<>: TE.Text " and " :<>: TE.ShowType b :<>: TE.Text " cannot be compared." ))
  BoolResultT BGEq a b = IfNumbers a b EBool (TE.TypeError (TE.ShowType a :<>: TE.Text " and " :<>: TE.ShowType b :<>: TE.Text " cannot be compared." ))
  BoolResultT BAnd EBool EBool = EBool
  BoolResultT BAnd a b = TE.TypeError (TE.Text "\"" :<>: TE.ShowType a :<>: TE.Text " && " :<>: TE.ShowType b :<>: TE.Text "\" is not defined." )
  BoolResultT BOr EBool EBool = EBool
  BoolResultT BOr a b = TE.TypeError (TE.Text "\"" :<>: TE.ShowType a :<>: TE.Text " || " :<>: TE.ShowType b :<>: TE.Text "\" is not defined." )

data BinaryOp = BAdd | BSubtract | BMultiply | BDivide | BPow | BModulo | BElementWise BinaryOp | BAndEqual BinaryOp | BBoolean BoolOp

data SBinaryOp :: BinaryOp -> Type where
  SAdd :: SBinaryOp BAdd
  SSubtract :: SBinaryOp BSubtract
  SMultiply :: SBinaryOp BMultiply
  SDivide :: SBinaryOp BDivide
  SPow :: SBinaryOp BPow
  SModulo :: SBinaryOp BModulo
  SElementWise :: SBinaryOp op -> SBinaryOp (BElementWise op)
  SBoolean :: SBoolOp bop -> SBinaryOp (BBoolean bop)

type family CanAndEqual (bo :: BinaryOp) :: BinaryOp where
  CanAndEqual (BBoolean _) =  TE.TypeError (TE.Text "Cannot AndEqual a boolean operator. That is, something like =/= makes no sense.")
  CanAndEqual a = a

instance TestEquality SBinaryOp where
  testEquality SAdd SAdd = Just Refl
  testEquality SSubtract SSubtract = Just Refl
  testEquality SMultiply SMultiply = Just Refl
  testEquality SDivide SDivide = Just Refl
  testEquality SPow SPow = Just Refl
  testEquality SModulo SModulo = Just Refl
  testEquality (SElementWise opa) (SElementWise opb) = case testEquality opa opb of
    Just Refl -> Just Refl
    Nothing -> Nothing
  testEquality (SBoolean bopa) (SBoolean bopb) = case testEquality @SBoolOp bopa bopb of
    Just Refl -> Just Refl
    Nothing -> Nothing
  testEquality _ _ = Nothing

type family BinaryResultT (bo :: BinaryOp) (a :: EType) (b :: EType) :: EType where
  BinaryResultT BAdd a a = a
  BinaryResultT BAdd a ECVec = IfRealNumber a ECVec (TE.TypeError (TE.ShowType a :<>: TE.Text " and " :<>: TE.ShowType ECVec :<>: TE.Text " cannot be added." ))
  BinaryResultT BAdd ECVec a = IfRealNumber a ECVec (TE.TypeError (TE.ShowType ECVec :<>: TE.Text " and " :<>: TE.ShowType a :<>: TE.Text " cannot be added." ))
  BinaryResultT BAdd a ERVec = IfRealNumber a ERVec (TE.TypeError (TE.ShowType a :<>: TE.Text " and " :<>: TE.ShowType ECVec :<>: TE.Text " cannot be added." ))
  BinaryResultT BAdd ERVec a = IfRealNumber a ERVec (TE.TypeError (TE.ShowType ECVec :<>: TE.Text " and " :<>: TE.ShowType a :<>: TE.Text " cannot be added." ))
  BinaryResultT BAdd a EMat = IfRealNumber a EMat (TE.TypeError (TE.ShowType a :<>: TE.Text " and " :<>: TE.ShowType ECVec :<>: TE.Text " cannot be added." ))
  BinaryResultT BAdd EMat a = IfRealNumber a EMat (TE.TypeError (TE.ShowType ECVec :<>: TE.Text " and " :<>: TE.ShowType a :<>: TE.Text " cannot be added." ))
  BinaryResultT BAdd a ESqMat = IfRealNumber a ESqMat (TE.TypeError (TE.ShowType a :<>: TE.Text " and " :<>: TE.ShowType ECVec :<>: TE.Text " cannot be added." ))
  BinaryResultT BAdd ESqMat a = IfRealNumber a ESqMat (TE.TypeError (TE.ShowType ECVec :<>: TE.Text " and " :<>: TE.ShowType a :<>: TE.Text " cannot be added." ))
  BinaryResultT BAdd a b = IfNumbers a b (Promoted a b) (TE.TypeError (TE.ShowType a :<>: TE.Text " and " :<>: TE.ShowType b :<>: TE.Text " cannot be added." ))
  BinaryResultT BSubtract a ECVec = IfRealNumber a ECVec (TE.TypeError (TE.ShowType a :<>: TE.Text " and " :<>: TE.ShowType ECVec :<>: TE.Text " cannot be subtracted." ))
  BinaryResultT BSubtract ECVec a = IfRealNumber a ECVec (TE.TypeError (TE.ShowType ECVec :<>: TE.Text " and " :<>: TE.ShowType a :<>: TE.Text " cannot be subtracted." ))
  BinaryResultT BSubtract a ERVec = IfRealNumber a ERVec (TE.TypeError (TE.ShowType a :<>: TE.Text " and " :<>: TE.ShowType ECVec :<>: TE.Text " cannot be subtracted." ))
  BinaryResultT BSubtract ERVec a = IfRealNumber a ERVec (TE.TypeError (TE.ShowType ECVec :<>: TE.Text " and " :<>: TE.ShowType a :<>: TE.Text " cannot be subtracted." ))
  BinaryResultT BSubtract a EMat = IfRealNumber a EMat (TE.TypeError (TE.ShowType a :<>: TE.Text " and " :<>: TE.ShowType ECVec :<>: TE.Text " cannot be subtracted." ))
  BinaryResultT BSubtract EMat a = IfRealNumber a EMat (TE.TypeError (TE.ShowType ECVec :<>: TE.Text " and " :<>: TE.ShowType a :<>: TE.Text " cannot be subtracted." ))
  BinaryResultT BSubtract a ESqMat = IfRealNumber a ESqMat (TE.TypeError (TE.ShowType a :<>: TE.Text " and " :<>: TE.ShowType ECVec :<>: TE.Text " cannot be subtracted." ))
  BinaryResultT BSubtract ESqMat a = IfRealNumber a ESqMat (TE.TypeError (TE.ShowType ECVec :<>: TE.Text " and " :<>: TE.ShowType a :<>: TE.Text " cannot be subtracted." ))

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
  BinaryResultT BMultiply a ECVec = IfRealNumber a ECVec (TE.TypeError (TE.ShowType a :<>: TE.Text " and " :<>: TE.ShowType ECVec :<>: TE.Text " cannot be multiplied." ))
  BinaryResultT BMultiply ECVec a = IfRealNumber a ECVec (TE.TypeError (TE.ShowType ECVec :<>: TE.Text " and " :<>: TE.ShowType a :<>: TE.Text " cannot be multiplied." ))
  BinaryResultT BMultiply a ERVec = IfRealNumber a ERVec (TE.TypeError (TE.ShowType a :<>: TE.Text " and " :<>: TE.ShowType ECVec :<>: TE.Text " cannot be multiplied." ))
  BinaryResultT BMultiply ERVec a = IfRealNumber a ERVec (TE.TypeError (TE.ShowType ECVec :<>: TE.Text " and " :<>: TE.ShowType a :<>: TE.Text " cannot be multiplied." ))
  BinaryResultT BMultiply a EMat = IfRealNumber a EMat (TE.TypeError (TE.ShowType a :<>: TE.Text " and " :<>: TE.ShowType ECVec :<>: TE.Text " cannot be multiplied." ))
  BinaryResultT BMultiply EMat a = IfRealNumber a EMat (TE.TypeError (TE.ShowType ECVec :<>: TE.Text " and " :<>: TE.ShowType a :<>: TE.Text " cannot be multiplied." ))
  BinaryResultT BMultiply a ESqMat = IfRealNumber a ESqMat (TE.TypeError (TE.ShowType a :<>: TE.Text " and " :<>: TE.ShowType ECVec :<>: TE.Text " cannot be multiplied." ))
  BinaryResultT BMultiply ESqMat a = IfRealNumber a ESqMat (TE.TypeError (TE.ShowType ECVec :<>: TE.Text " and " :<>: TE.ShowType a :<>: TE.Text " cannot be multiplied." ))
  BinaryResultT BMultiply a (EArray n b) = EArray n (BinaryResultT BMultiply a b)
  BinaryResultT BMultiply a b = TE.TypeError (TE.ShowType a :<>: TE.Text " and " :<>: TE.ShowType b :<>: TE.Text " cannot be multiplied." )
  BinaryResultT BDivide ECVec a = IfRealNumber a ECVec (TE.TypeError (TE.ShowType ECVec :<>: TE.Text " and " :<>: TE.ShowType a :<>: TE.Text " cannot be divided." ))
  BinaryResultT BDivide ERVec a = IfRealNumber a ERVec (TE.TypeError (TE.ShowType ERVec :<>: TE.Text " and " :<>: TE.ShowType a :<>: TE.Text " cannot be divided." ))
  BinaryResultT BDivide EMat a = IfRealNumber a EMat (TE.TypeError (TE.ShowType EMat :<>: TE.Text " and " :<>: TE.ShowType a :<>: TE.Text " cannot be divided." ))
  BinaryResultT BDivide ESqMat a = IfRealNumber a ESqMat (TE.TypeError (TE.ShowType ESqMat :<>: TE.Text " and " :<>: TE.ShowType a :<>: TE.Text " cannot be divided." ))
  BinaryResultT BDivide (EArray n t) a = IfNumber a (BinaryResultT BDivide t a) (TE.TypeError (TE.ShowType (EArray n t) :<>: TE.Text " and " :<>: TE.ShowType a :<>: TE.Text " cannot be divided." ))
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
  BinaryResultT (BBoolean bop) a b = BoolResultT bop a b

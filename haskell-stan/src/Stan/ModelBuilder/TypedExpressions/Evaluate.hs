{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveTraversable #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}

module Stan.ModelBuilder.TypedExpressions.Evaluate where

import Prelude hiding (Nat)

import Stan.ModelBuilder.TypedExpressions.Types
import Stan.ModelBuilder.TypedExpressions.Indexing
import Stan.ModelBuilder.TypedExpressions.Arithmetic
import Stan.ModelBuilder.TypedExpressions.Expression
import Stan.ModelBuilder.TypedExpressions.Recursion

import Data.Type.Nat (Nat(..), SNat(..))
import Data.Foldable(maximum)
import qualified Data.Functor.Classes as FC
import qualified Data.Functor.Classes.Generic as FC
import qualified Data.Functor.Foldable as Rec
import qualified Data.Functor.Foldable.Monadic as Rec
import GHC.Generics (Generic1)

import qualified Data.IntMap.Strict as IM

import          Data.Kind (Type)
import          Data.List ((!!),(++))
import qualified Data.Fix as Fix
import qualified Data.Text as T

--import qualified Text.PrettyPrint as Pretty
import qualified Data.Type.Nat as DT


toLExprAlg :: Map Text Text -> IAlg UExprF LExpr
toLExprAlg varMap s = case s of
  DeclareEF st iv -> LDeclareF st iv




data IExprF :: Type -> Type where
  IIndexed :: Text -> IM.IntMap Text -> [Int] -> IExprF a
  ILookup :: Text -> IExprF a
  deriving stock (Eq, Ord, Functor, Foldable, Traversable, Generic1)
  deriving (FC.Show1, FC.Eq1, FC.Ord1) via FC.FunctorClassesDefault IExprF


--data IExpr = IExpr { atom :: Text,  indexed :: IM.IntMap Text, unusedIndices :: [Int] }

writeIExpr :: IExpr -> Text
writeIExpr (IExpr a is _) = a <> indexPart
  where
    maxUsed = maximum $ IM.keys is
    is' = is <> (IM.fromList $ zip [0..maxUsed] (repeat ""))
    indexPart = if IM.size is == 0
                then ""
                else "[" <> T.intercalate ", " (IM.elems is') <> "]"

cIndexed :: Text -> Int -> IExpr
cIndexed t nIndices = IExpr t IM.empty newIndices
  where
    newIndices = take nIndices [0..]

cAppendIndex :: SNat n -> Text -> IExpr -> IExpr
cAppendIndex n it (IExpr t im is) = IExpr t im' is'
  where
    nInt :: Int = fromIntegral DT.snatToNatural
    (i, is') = (is !! nInt, take nInt is ++ drop (nInt + 1) is)
    im' = IM.insert i it im

toIExprAlg :: IAlg UExprF (K IExpr)
toIExprAlg x = K $ case x of
  DeclareEF st divf -> undefined
  NamedEF txt _ -> cIndexed txt 0
  IntEF n -> cIndexed (show n) 0
  RealEF x -> cIndexed (show x) 0
  ComplexEF rp ip -> cIndexed (show rp <> " + i" <> show ip) 0
  BinaryOpEF sbo le re -> cIndexed (writeIExpr (unK le) <> " " <> opText sbo <> " " <> writeIExpr (unK re)) 0
  SliceEF sn ie e -> cAppendIndex sn (writeIExpr $ unK ie) (unK e)
  NamedIndexEF txt -> undefined

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

{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}
{-# HLINT ignore "Use camelCase" #-}

module Stan.ModelBuilder.TypedExpressions.StanFunctions
  (
    module Stan.ModelBuilder.TypedExpressions.StanFunctions
  , module Stan.ModelBuilder.TypedExpressions.Functions
  )
  where

import Stan.ModelBuilder.TypedExpressions.Types
import Stan.ModelBuilder.TypedExpressions.TypedList
import Stan.ModelBuilder.TypedExpressions.Functions
import Stan.ModelBuilder.TypedExpressions.Expressions
import Stan.ModelBuilder.TypedExpressions.Indexing

import Data.Vec.Lazy (Vec)
import qualified GHC.TypeLits as TE
import GHC.TypeLits (ErrorMessage((:<>:)))
import Data.Type.Nat (SNatI)
import Prelude hiding (Nat)

-- this needs fixingf for higher dimensi
type VectorizedReal t = (GenSType t, ScalarType t ~ EReal)

logit :: VectorizedReal t => Function t '[t]
logit = simpleFunction "logit"

--logit :: TE.ExprList '[EReal] -> TE

invLogit :: VectorizedReal t => Function t '[t]
invLogit = simpleFunction "inv_logit"

sqrt :: VectorizedReal t => Function t '[t]
sqrt = simpleFunction "sqrt"

inv_sqrt :: VectorizedReal t => Function t '[t]
inv_sqrt = simpleFunction "inv_sqrt"

inv :: VectorizedReal t => Function t '[t]
inv = simpleFunction "inv"

mean :: (TypeOneOf t [ECVec, ERVec, EMat, ESqMat], GenSType t) => Function EReal '[t]
mean = simpleFunction "mean"

variance :: (TypeOneOf t [ECVec, ERVec, EMat, ESqMat], GenSType t) => Function EReal '[t]
variance = simpleFunction "variance"

targetVal :: Function EReal '[]
targetVal = simpleFunction "target"

array_num_elements :: (SNatI n, GenSType t) => Function EInt '[EArray n t]
array_num_elements = simpleFunction "inv"

to_vector :: (TypeOneOf t [ECVec, ERVec, EArray1 EInt, EArray1 EReal], GenSType t)
          => Function ECVec '[t]
to_vector = simpleFunction "to_vector"

size :: (IsContainer t, GenSType t) => Function EInt '[t]
size = simpleFunction "size"

sum :: (TypeOneOf t '[ECVec, ERVec, EMat, ESqMat], GenSType t) => Function EReal '[t]
sum = simpleFunction "sum"

rep_vector :: Function ECVec '[EReal, EInt]
rep_vector = simpleFunction "rep_vector"

type family NInts (n :: Nat) :: [EType] where
  NInts Z = '[]
  NInts (S n) = EInt ': NInts n

-- this pleases me
rep_array :: (GenSType t, GenTypeList (NInts n), SNatI n) => Function (EArray n t) (t ': NInts n)
rep_array = simpleFunction "rep_array"

dot :: Function EReal '[ECVec, ECVec]
dot = simpleFunction "dot"

type family RepArgs (t :: EType) :: [EType] where
  RepArgs ECVec = [EReal, EInt]
  RepArgs EMat = [EReal, EInt, EInt]
  RepArgs ESqMat = [EReal, EInt]
  RepArgs t = TE.TypeError (TE.Text "Cannot fill " :<>: TE.ShowType t :<>: TE.Text " like a container (e.g., vector, matrix)")

-- this might not be so useful because GHC/Haskell cannot neccessarily see through the constraints
rep_container :: (TypeOneOf t '[ECVec, EMat, ESqMat]) => SType t -> Function t (RepArgs t)
rep_container st = case st of
  SCVec -> rep_vector
  SMat -> rep_matrix
  SSqMat -> rep_sq_matrix
  _ -> undefined -- constraint means this should never get called??

rep_matrix :: Function EMat '[EReal, EInt, EInt]
rep_matrix = simpleFunction "rep_matrix"

rep_sq_matrix :: Function ESqMat '[EReal, EInt]
rep_sq_matrix = Function "rep_matrix" SSqMat (SReal ::> SInt ::> TypeNil) f
  where
    f :: TypedList u '[EReal, EInt] -> TypedList u '[EReal, EInt, EInt]
    f (a :> b :> TNil) = a :> b :> b :> TNil

segment :: (IsContainer t, GenSType t) => Function t [t, EInt, EInt]
segment = simpleFunction "segment"

inverse :: (TypeOneOf t [EMat, ESqMat], GenSType t) => Function t '[t]
inverse = simpleFunction "inverse"

qr_thin_Q :: Function EMat '[EMat]
qr_thin_Q = simpleFunction "qr_thin_Q"

qr_thin_R :: Function EMat '[EMat]
qr_thin_R = simpleFunction "qr_thin_R"

block :: Function EMat [EMat, EInt, EInt, EInt, EInt]
block = simpleFunction "block"

sub_col :: Function ECVec [EMat, EInt, EInt, EInt]
sub_col = simpleFunction "sub_col"

sub_row :: Function ERVec [EMat, EInt, EInt, EInt]
sub_row = simpleFunction "sub_row"

rows :: (TypeOneOf t [EMat, ESqMat], GenSType t) => Function EInt '[t]
rows = simpleFunction "rows"

cols :: (TypeOneOf t [EMat, ESqMat], GenSType t) => Function EInt '[t]
cols = simpleFunction "cols"


-- Densities & RNGs

normalDensity :: (TypeOneOf t [EReal, ECVec], GenSType t) => Density t '[t, t]
normalDensity = simpleDensity "normal"

normalLPDF :: (TypeOneOf t [EReal, ECVec], GenSType t) => Density t '[t, t]
normalLPDF = simpleDensity "normal_lpdf"

normalLUPDF :: (TypeOneOf t [EReal, ECVec], GenSType t) => Density t '[t, t]
normalLUPDF = simpleDensity "normal_lupdf"

normalRNG :: (TypeOneOf t [EReal, ECVec], GenSType t) => Function t '[t, t]
normalRNG = simpleFunction "normal_rng"

stdNormalDensity :: (TypeOneOf t [EReal, ECVec], GenSType t) => Density t '[]
stdNormalDensity = simpleDensity "std_normal"

stdNormalLPDF :: (TypeOneOf t [EReal, ECVec], GenSType t) => Density t '[]
stdNormalLPDF = simpleDensity "std_normal_lpdf"

stdNormalLUPDF :: (TypeOneOf t [EReal, ECVec], GenSType t) => Density t '[]
stdNormalLUPDF = simpleDensity "std_normal_lupdf"

stdNormalRNG :: (TypeOneOf t [EReal, ECVec], GenSType t) => Function t '[]
stdNormalRNG = simpleFunction "std_normal_rng"

cauchyDensity :: (TypeOneOf t [EReal, ECVec], GenSType t) => Density t '[t, t]
cauchyDensity = simpleDensity "cauchy"

cauchyLPDF :: (TypeOneOf t [EReal, ECVec], GenSType t) => Density t '[t, t]
cauchyLPDF = simpleDensity "cauchy_lpdf"

cauchyLUPDF :: (TypeOneOf t [EReal, ECVec], GenSType t) => Density t '[t, t]
cauchyLUPDF = simpleDensity "cauchy_lupdf"

cauchyRNG :: (TypeOneOf t [EReal, ECVec], GenSType t) => Function t '[t, t]
cauchyRNG = simpleFunction "cauchy_rng"

type BinDensityC t t' = (TypeOneOf t [EArray1 EInt, EInt], GenSType t
                        , TypeOneOf t' [EArray1 EReal, ECVec, EReal], ScalarType t' ~ EReal, GenSType t'
                        , Dimension t ~ Dimension t')

binomialDensity :: BinDensityC t t' => Density t '[t, t']
binomialDensity = simpleDensity "binomial"

binomialLPMF :: BinDensityC t t' => Density t '[t, t']
binomialLPMF = simpleDensity "binomial_lpmf"

binomialLUPMF :: BinDensityC t t' => Density t '[t, t']
binomialLUPMF = simpleDensity "binomial_lupmf"

binomialRNG ::BinDensityC t t' => Function t '[t, t']
binomialRNG = simpleFunction "binomial_rng"

binomialLogitDensity :: BinDensityC t t' => Density t '[t, t']
binomialLogitDensity = simpleDensity "binomial_logit"

binomialLogitLPMF :: BinDensityC t t' => Density t '[t, t']
binomialLogitLPMF = simpleDensity "binomial_logit_lpmf"

binomialLogitLUPMF :: BinDensityC t t' => Density t '[t, t']
binomialLogitLUPMF = simpleDensity "binomial_logit_lupmf"

betaDensity :: (TypeOneOf t [EReal, ECVec], GenSType t) => Density t '[t, t]
betaDensity = simpleDensity "beta"

betaLPDF :: (TypeOneOf t [EReal, ECVec], GenSType t) => Density t '[t, t]
betaLPDF = simpleDensity "beta_lpdf"

betaLUPDF :: (TypeOneOf t [EReal, ECVec], GenSType t) => Density t '[t, t]
betaLUPDF = simpleDensity "beta_lupdf"

betaRNG :: (TypeOneOf t [EReal, ECVec], GenSType t) => Function t '[t, t]
betaRNG = simpleFunction "beta_rng"

betaProportionDensity ::  (TypeOneOf t [EReal, ECVec], GenSType t) => Density t '[t, t]
betaProportionDensity = simpleDensity "beta_proportion"

betaProportionLPDF :: (TypeOneOf t [EReal, ECVec], GenSType t) => Density t '[t, t]
betaProportionLPDF = simpleDensity "beta_proportion_lpdf"

betaProportionLUPDF ::  (TypeOneOf t [EReal, ECVec], GenSType t) => Density t '[t, t]
betaProportionLUPDF = simpleDensity "beta_proportion_lupdf"

betaProportionRNG ::  (TypeOneOf t [EReal, ECVec], GenSType t) => Function t '[t, t]
betaProportionRNG = simpleFunction "beta_proportion_rng"

betaBinomialDensity :: BinDensityC t t' => Density t '[t, t', t']
betaBinomialDensity = simpleDensity "beta_binomial"

betaBinomialLPMF :: BinDensityC t t' => Density t '[t, t', t']
betaBinomialLPMF = simpleDensity "beta_binomial_lpmf"

betaBinomialLUPMF :: BinDensityC t t' => Density t '[t, t', t']
betaBinomialLUPMF = simpleDensity "binomial_lupmf"

betaBinomialRNG :: BinDensityC t t' => Function t '[t, t', t']
betaBinomialRNG = simpleFunction "beta_binomial_rng"

lkjDensity :: Density ESqMat '[EReal]
lkjDensity = simpleDensity "lkj_corr_cholesky"

multi_normal_cholesky :: Density ECVec '[ECVec, ECVec, ESqMat]
multi_normal_cholesky = simpleDensity "multi_normal_cholesky"

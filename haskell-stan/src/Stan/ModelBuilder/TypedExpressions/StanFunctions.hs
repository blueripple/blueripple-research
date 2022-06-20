{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}
{-# HLINT ignore "Use camelCase" #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}

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


logit :: Function EReal '[EReal]
logit = simpleFunction "logit"

invLogit :: Function EReal '[EReal]
invLogit = simpleFunction "inv_logit"

sqrt :: (TypeOneOf t [EReal, ECVec], GenSType t) => Function t '[t]
sqrt = simpleFunction "sqrt"

inv_sqrt :: Function EReal '[EReal]
inv_sqrt = simpleFunction "inv_sqrt"

inv :: Function EReal '[EReal]
inv = simpleFunction "inv"

array_num_elements :: (SNatI n, GenSType t) => Function EInt '[EArray n t]
array_num_elements = simpleFunction "inv"

to_vector :: (TypeOneOf t [ECVec, ERVec, EArray1 EInt, EArray1 EReal], GenSType t)
          => Function ECVec '[t]
to_vector = simpleFunction "to_vector"

size :: (TypeOneOf t '[ECVec, ERVec, EArray1 EInt, EArray1 EReal, EArray1 EComplex], GenSType t) => Function EInt '[t]
size = simpleFunction "size"

vector_sum :: (TypeOneOf t '[ECVec, ERVec], GenSType t) => Function EReal '[t]
vector_sum = simpleFunction "sum"

row_vector_sum :: Function EReal '[ERVec]
row_vector_sum = simpleFunction "sum"

matrix_sum :: Function EReal '[EMat]
matrix_sum = simpleFunction "sum"

rep_vector :: Function ECVec '[EReal, EInt]
rep_vector = simpleFunction "rep_vector"

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

binomialDensity :: Density (EArray1 EInt) '[EArray1 EInt, EReal]
binomialDensity = simpleDensity "binomial"

binomialLPMF :: Density (EArray1 EInt) '[EArray1 EInt, EReal]
binomialLPMF = simpleDensity "binomial_lpmf"

binomialLUPMF :: Density (EArray1 EInt) '[EArray1 EInt, EReal]
binomialLUPMF = simpleDensity "binomial_lupmf"

binomialRNG :: Function (EArray1 EInt) '[EArray1 EInt, EReal]
binomialRNG = simpleFunction "binomial_rng"

binomialLogitDensity :: Density (EArray1 EInt) '[EArray1 EInt, EReal]
binomialLogitDensity = simpleDensity "binomial_logit"

binomialLogitLPMF :: Density (EArray1 EInt) '[EArray1 EInt, EReal]
binomialLogitLPMF = simpleDensity "binomial_logit_lpmf"

binomialLogitLUPMF :: Density (EArray1 EInt) '[EArray1 EInt, EReal]
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

betaBinomialDensity ::   Density (EArray1 EInt) '[EArray1 EInt, ECVec, ECVec]
betaBinomialDensity = simpleDensity "beta_binomial"

betaBinomialLPMF :: Density (EArray1 EInt) '[EArray1 EInt, ECVec, ECVec]
betaBinomialLPMF = simpleDensity "beta_binomial_lpmf"

betaBinomialLUPMF :: Density (EArray1 EInt) '[EArray1 EInt, ECVec, ECVec]
betaBinomialLUPMF = simpleDensity "binomial_lupmf"

betaBinomialRNG :: Function (EArray1 EInt) '[EArray1 EInt, ECVec, ECVec]
betaBinomialRNG = simpleFunction "beta_binomial_rng"

lkjDensity :: Density ESqMat '[EReal]
lkjDensity = simpleDensity "lkj_corr_cholesky"

multi_normal_cholesky :: Density ECVec '[ECVec, ECVec, ESqMat]
multi_normal_cholesky = simpleDensity "multi_normal_cholesky"

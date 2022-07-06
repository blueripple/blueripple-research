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
{-# INLINEABLE logit #-}
--logit :: TE.ExprList '[EReal] -> TE

inv_logit :: VectorizedReal t => Function t '[t]
inv_logit = simpleFunction "inv_logit"
{-# INLINEABLE inv_logit #-}

sqrt :: VectorizedReal t => Function t '[t]
sqrt = simpleFunction "sqrt"
{-# INLINEABLE sqrt #-}

inv_sqrt :: VectorizedReal t => Function t '[t]
inv_sqrt = simpleFunction "inv_sqrt"
{-# INLINEABLE inv_sqrt #-}

inv :: VectorizedReal t => Function t '[t]
inv = simpleFunction "inv"
{-# INLINEABLE inv #-}

mean :: (TypeOneOf t [ECVec, ERVec, EMat, ESqMat], GenSType t) => Function EReal '[t]
mean = simpleFunction "mean"
{-# INLINEABLE mean #-}

variance :: (TypeOneOf t [ECVec, ERVec, EMat, ESqMat], GenSType t) => Function EReal '[t]
variance = simpleFunction "variance"
{-# INLINEABLE variance #-}

targetVal :: Function EReal '[]
targetVal = simpleFunction "target"
{-# INLINEABLE targetVal #-}

array_num_elements :: (SNatI n, GenSType t) => Function EInt '[EArray n t]
array_num_elements = simpleFunction "inv"
{-# INLINEABLE array_num_elements #-}

to_vector :: (TypeOneOf t [ECVec, ERVec, EArray1 EInt, EArray1 EReal, EMat, ESqMat], GenSType t)
          => Function ECVec '[t]
to_vector = simpleFunction "to_vector"
{-# INLINEABLE to_vector #-}

to_row_vector :: (TypeOneOf t [ECVec, ERVec, EArray1 EInt, EArray1 EReal, EMat, ESqMat], GenSType t)
          => Function ERVec '[t]
to_row_vector = simpleFunction "to_row_vector"
{-# INLINEABLE to_row_vector #-}

size :: (IsContainer t, GenSType t) => Function EInt '[t]
size = simpleFunction "size"
{-# INLINEABLE size #-}

sum :: (TypeOneOf t '[ECVec, ERVec, EMat, ESqMat], GenSType t) => Function EReal '[t]
sum = simpleFunction "sum"
{-# INLINEABLE sum #-}

rep_vector :: Function ECVec '[EReal, EInt]
rep_vector = simpleFunction "rep_vector"
{-# INLINEABLE rep_vector #-}

type family NInts (n :: Nat) :: [EType] where
  NInts Z = '[]
  NInts (S n) = EInt ': NInts n

-- this pleases me
rep_array :: (GenSType t, GenTypeList (NInts n), SNatI n) => Function (EArray n t) (t ': NInts n)
rep_array = simpleFunction "rep_array"
{-# INLINEABLE rep_array #-}

dot_product :: (TypeOneOf t '[ECVec, ERVec], GenSType t, TypeOneOf t' '[ECVec, ERVec], GenSType t')
            => Function EReal '[t, t']
dot_product = simpleFunction "dot_product"
{-# INLINEABLE dot_product #-}

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
{-# INLINEABLE rep_container #-}

rep_matrix :: Function EMat '[EReal, EInt, EInt]
rep_matrix = simpleFunction "rep_matrix"
{-# INLINEABLE rep_matrix #-}

repV_matrix :: (TypeOneOf t '[ECVec, ERVec], GenSType t) => Function EMat '[t, EInt]
repV_matrix = simpleFunction "rep_matrix"
{-# INLINEABLE repV_matrix #-}


rep_sq_matrix :: Function ESqMat '[EReal, EInt]
rep_sq_matrix = Function "rep_matrix" SSqMat (SReal ::> SInt ::> TypeNil) f
  where
    f :: TypedList u '[EReal, EInt] -> TypedList u '[EReal, EInt, EInt]
    f (a :> b :> TNil) = a :> b :> b :> TNil
{-# INLINEABLE rep_sq_matrix #-}

vecArrayToMatrix :: (TypeOneOf t [ECVec, ERVec], GenSType t) => Function EMat '[EArray1 t]
vecArrayToMatrix = simpleFunction "to_matrix"
{-# INLINEABLE vecArrayToMatrix #-}

segment :: (IsContainer t, GenSType t) => Function t [t, EInt, EInt]
segment = simpleFunction "segment"
{-# INLINEABLE segment #-}

inverse :: (TypeOneOf t [EMat, ESqMat], GenSType t) => Function t '[t]
inverse = simpleFunction "inverse"
{-# INLINEABLE inverse #-}

qr_thin_Q :: Function EMat '[EMat]
qr_thin_Q = simpleFunction "qr_thin_Q"
{-# INLINEABLE qr_thin_Q #-}

qr_thin_R :: Function EMat '[EMat]
qr_thin_R = simpleFunction "qr_thin_R"
{-# INLINEABLE qr_thin_R #-}

block :: Function EMat [EMat, EInt, EInt, EInt, EInt]
block = simpleFunction "block"
{-# INLINEABLE block #-}

sub_col :: Function ECVec [EMat, EInt, EInt, EInt]
sub_col = simpleFunction "sub_col"
{-# INLINEABLE sub_col #-}

sub_row :: Function ERVec [EMat, EInt, EInt, EInt]
sub_row = simpleFunction "sub_row"
{-# INLINEABLE sub_row #-}

rows :: (TypeOneOf t [EMat, ESqMat], GenSType t) => Function EInt '[t]
rows = simpleFunction "rows"
{-# INLINEABLE rows #-}

cols :: (TypeOneOf t [EMat, ESqMat], GenSType t) => Function EInt '[t]
cols = simpleFunction "cols"
{-# INLINEABLE cols #-}

diag_pre_multiply :: (TypeOneOf t [ECVec, ERVec], GenSType t) => Function EMat [t, EMat]
diag_pre_multiply = simpleFunction "diag_pre_multiply"

-- Densities & RNGs

normal :: (TypeOneOf t [EReal, ECVec, ERVec], GenSType t) => Density t '[t, t]
normal = simpleDensity "normal"
{-# INLINEABLE normal #-}

normal_lpdf :: (TypeOneOf t [EReal, ECVec, ERVec], GenSType t) => Density t '[t, t]
normal_lpdf = simpleDensity "normal_lpdf"
{-# INLINEABLE normal_lpdf #-}

normal_lupdf :: (TypeOneOf t [EReal, ECVec, ERVec], GenSType t) => Density t '[t, t]
normal_lupdf = simpleDensity "normal_lupdf"
{-# INLINEABLE normal_lupdf #-}

normal_rng :: (TypeOneOf t [EReal, ECVec, ERVec], GenSType t) => Function t '[t, t]
normal_rng = simpleFunction "normal_rng"
{-# INLINEABLE normal_rng #-}

normalS :: (TypeOneOf t [EReal, ECVec, ERVec], GenSType t) => Density t '[EReal, EReal]
normalS = simpleDensity "normal"
{-# INLINEABLE normalS #-}

normalS_lpdf :: (TypeOneOf t [EReal, ECVec, ERVec], GenSType t) => Density t '[EReal, EReal]
normalS_lpdf = simpleDensity "normal_lpdf"
{-# INLINEABLE normalS_lpdf #-}

normalS_lupdf :: (TypeOneOf t [EReal, ECVec, ERVec], GenSType t) => Density t '[EReal, EReal]
normalS_lupdf = simpleDensity "normal_lupdf"
{-# INLINEABLE normalS_lupdf #-}

normalS_rng :: (TypeOneOf t [EReal, ECVec, ERVec], GenSType t) => Function t '[EReal, EReal]
normalS_rng = simpleFunction "normal_rng"
{-# INLINEABLE normalS_rng #-}

std_normal :: (TypeOneOf t [EReal, ECVec, ERVec], GenSType t) => Density t '[]
std_normal = simpleDensity "std_normal"
{-# INLINEABLE std_normal #-}

std_normal_lpdf :: (TypeOneOf t [EReal, ECVec, ERVec], GenSType t) => Density t '[]
std_normal_lpdf = simpleDensity "std_normal_lpdf"
{-# INLINEABLE std_normal_lpdf #-}

std_normal_lupdf :: (TypeOneOf t [EReal, ECVec, ERVec], GenSType t) => Density t '[]
std_normal_lupdf = simpleDensity "std_normal_lupdf"
{-# INLINEABLE std_normal_lupdf #-}

std_normal_rng :: (TypeOneOf t [EReal, ECVec, ERVec], GenSType t) => Function t '[]
std_normal_rng = simpleFunction "std_normal_rng"
{-# INLINEABLE std_normal_rng #-}

lognormal :: (TypeOneOf t [EReal, ECVec, ERVec], GenSType t) => Density t '[t, t]
lognormal = simpleDensity "lognormal"
{-# INLINEABLE lognormal #-}

lognormal_lpdf :: (TypeOneOf t [EReal, ECVec, ERVec], GenSType t) => Density t '[t, t]
lognormal_lpdf = simpleDensity "lognormal_lpdf"
{-# INLINEABLE lognormal_lpdf #-}

lognormal_lupdf :: (TypeOneOf t [EReal, ECVec, ERVec], GenSType t) => Density t '[t, t]
lognormal_lupdf = simpleDensity "lognormal_lupdf"
{-# INLINEABLE lognormal_lupdf #-}

lognormal_rng :: (TypeOneOf t [EReal, ECVec, ERVec], GenSType t) => Function t '[t, t]
lognormal_rng = simpleFunction "lognormal_rng"
{-# INLINEABLE lognormal_rng #-}

lognormalS :: (TypeOneOf t [EReal, ECVec, ERVec], GenSType t) => Density t '[EReal, EReal]
lognormalS = simpleDensity "lognormal"
{-# INLINEABLE lognormalS #-}

lognormalS_lpdf :: (TypeOneOf t [EReal, ECVec, ERVec], GenSType t) => Density t '[EReal, EReal]
lognormalS_lpdf = simpleDensity "lognormal_lpdf"
{-# INLINEABLE lognormalS_lpdf #-}

lognormalS_lupdf :: (TypeOneOf t [EReal, ECVec, ERVec], GenSType t) => Density t '[EReal, EReal]
lognormalS_lupdf = simpleDensity "lognormal_lupdf"
{-# INLINEABLE lognormalS_lupdf #-}

lognormalS_rng :: (TypeOneOf t [EReal, ECVec, ERVec], GenSType t) => Function t '[EReal, EReal]
lognormalS_rng = simpleFunction "lognormal_rng"
{-# INLINEABLE lognormalS_rng #-}


cauchy :: (TypeOneOf t [EReal, ECVec], GenSType t) => Density t '[t, t]
cauchy = simpleDensity "cauchy"
{-# INLINEABLE cauchy #-}

cauchy_lpdf :: (TypeOneOf t [EReal, ECVec], GenSType t) => Density t '[t, t]
cauchy_lpdf = simpleDensity "cauchy_lpdf"
{-# INLINEABLE cauchy_lpdf #-}

cauchy_lupdf :: (TypeOneOf t [EReal, ECVec], GenSType t) => Density t '[t, t]
cauchy_lupdf = simpleDensity "cauchy_lupdf"
{-# INLINEABLE cauchy_lupdf #-}

cauchy_rng :: (TypeOneOf t [EReal, ECVec], GenSType t) => Function t '[t, t]
cauchy_rng = simpleFunction "cauchy_rng"
{-# INLINEABLE cauchy_rng #-}

type BinDensityC t t' = (TypeOneOf t [EArray1 EInt, EInt], GenSType t
                        , TypeOneOf t' [EArray1 EReal, ECVec, EReal], ScalarType t' ~ EReal, GenSType t'
                        , Dimension t ~ Dimension t')

binomial :: BinDensityC t t' => Density t '[t, t']
binomial = simpleDensity "binomial"
{-# INLINEABLE binomial #-}

binomial_lpmf :: BinDensityC t t' => Density t '[t, t']
binomial_lpmf = simpleDensity "binomial_lpmf"
{-# INLINEABLE binomial_lpmf #-}

binomial_lupmf :: BinDensityC t t' => Density t '[t, t']
binomial_lupmf = simpleDensity "binomial_lupmf"
{-# INLINEABLE binomial_lupmf #-}

binomial_rng ::BinDensityC t t' => Function t '[t, t']
binomial_rng = simpleFunction "binomial_rng"
{-# INLINEABLE binomial_rng #-}

binomial_logit :: BinDensityC t t' => Density t '[t, t']
binomial_logit = simpleDensity "binomial_logit"
{-# INLINEABLE binomial_logit #-}

binomial_logit_lpmf :: BinDensityC t t' => Density t '[t, t']
binomial_logit_lpmf = simpleDensity "binomial_logit_lpmf"
{-# INLINEABLE binomial_logit_lpmf #-}

binomial_logit_lupmf :: BinDensityC t t' => Density t '[t, t']
binomial_logit_lupmf = simpleDensity "binomial_logit_lupmf"
{-# INLINEABLE binomial_logit_lupmf #-}

beta :: (TypeOneOf t [EReal, ECVec], GenSType t) => Density t '[t, t]
beta = simpleDensity "beta"
{-# INLINEABLE beta #-}

beta_lpdf :: (TypeOneOf t [EReal, ECVec], GenSType t) => Density t '[t, t]
beta_lpdf = simpleDensity "beta_lpdf"
{-# INLINEABLE beta_lpdf #-}

beta_lupdf :: (TypeOneOf t [EReal, ECVec], GenSType t) => Density t '[t, t]
beta_lupdf = simpleDensity "beta_lupdf"
{-# INLINEABLE beta_lupdf #-}

beta_rng :: (TypeOneOf t [EReal, ECVec], GenSType t) => Function t '[t, t]
beta_rng = simpleFunction "beta_rng"
{-# INLINEABLE beta_rng #-}

beta_proportion ::  (TypeOneOf t [EReal, ECVec], GenSType t) => Density t '[t, t]
beta_proportion = simpleDensity "beta_proportion"
{-# INLINEABLE beta_proportion #-}

beta_proportion_lpdf :: (TypeOneOf t [EReal, ECVec], GenSType t) => Density t '[t, t]
beta_proportion_lpdf = simpleDensity "beta_proportion_lpdf"
{-# INLINEABLE beta_proportion_lpdf #-}

beta_proportion_lupdf ::  (TypeOneOf t [EReal, ECVec], GenSType t) => Density t '[t, t]
beta_proportion_lupdf = simpleDensity "beta_proportion_lupdf"
{-# INLINEABLE beta_proportion_lupdf #-}

beta_proportion_rng ::  (TypeOneOf t [EReal, ECVec], GenSType t) => Function t '[t, t]
beta_proportion_rng = simpleFunction "beta_proportion_rng"
{-# INLINEABLE beta_proportion_rng #-}

beta_binomial :: BinDensityC t t' => Density t '[t, t', t']
beta_binomial = simpleDensity "beta_binomial"
{-# INLINEABLE beta_binomial #-}

beta_binomial_lpmf :: BinDensityC t t' => Density t '[t, t', t']
beta_binomial_lpmf = simpleDensity "beta_binomial_lpmf"
{-# INLINEABLE beta_binomial_lpmf #-}

beta_binomial_lupmf :: BinDensityC t t' => Density t '[t, t', t']
beta_binomial_lupmf = simpleDensity "beta_binomial_lupmf"
{-# INLINEABLE beta_binomial_lupmf #-}

beta_binomial_rng :: BinDensityC t t' => Function t '[t, t', t']
beta_binomial_rng = simpleFunction "beta_binomial_rng"
{-# INLINEABLE beta_binomial_rng #-}

lkj_corr_cholesky :: Density ESqMat '[EReal]
lkj_corr_cholesky = simpleDensity "lkj_corr_cholesky"
{-# INLINEABLE lkj_corr_cholesky #-}

multi_normal_cholesky :: Density ECVec '[ECVec, ECVec, ESqMat]
multi_normal_cholesky = simpleDensity "multi_normal_cholesky"
{-# INLINEABLE multi_normal_cholesky #-}

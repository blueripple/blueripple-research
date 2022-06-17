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


logit :: Function EReal '[EReal]
logit = simpleFunction "logit" SReal (oneType SReal)

invLogit :: Function EReal '[EReal]
invLogit = simpleFunction "inv_logit" SReal (oneType SReal)

sqrt :: Function EReal '[EReal]
sqrt = simpleFunction "sqrt" SReal (oneType SReal)

inv_sqrt :: Function EReal '[EReal]
inv_sqrt = simpleFunction "inv_sqrt" SReal (oneType SReal)

inv :: Function EReal '[EReal]
inv = simpleFunction "inv" SReal (oneType SReal)

array_num_elements :: SNat n -> SType t -> Function EInt '[EArray n t]
array_num_elements sn st = simpleFunction "inv" SInt (oneType $ SArray sn st)

vector_size :: Function EInt '[ECVec]
vector_size = simpleFunction "size" SInt (oneType SCVec)

row_vector_size :: Function EInt '[ERVec]
row_vector_size = simpleFunction "size" SInt (oneType SRVec)

vector_sum :: Function EReal '[ECVec]
vector_sum = simpleFunction "sum" SReal (oneType SCVec)

row_vector_sum :: Function EReal '[ERVec]
row_vector_sum = simpleFunction "sum" SReal (oneType SRVec)

matrix_sum :: Function EReal '[EMat]
matrix_sum = simpleFunction "sum" SReal (oneType SMat)

rep_vector :: Function ECVec '[EReal, EInt]
rep_vector = simpleFunction "rep_vector" SCVec (SReal ::> SInt ::> TypeNil)

dot :: Function EReal '[ECVec, ECVec]
dot = simpleFunction "dot" SReal (SCVec ::> SCVec ::> TypeNil)


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
  _ -> undefined -- TO silence the warning.  But the constraint and type family in return should mean this never gets called

rep_matrix :: Function EMat '[EReal, EInt, EInt]
rep_matrix = simpleFunction "rep_matrix" SMat (SReal ::> SInt ::> SInt ::> TypeNil)

rep_sq_matrix :: Function ESqMat '[EReal, EInt]
rep_sq_matrix = Function "rep_matrix" SSqMat (SReal ::> SInt ::> TypeNil) f
  where
    f :: TypedList u '[EReal, EInt] -> TypedList u '[EReal, EInt, EInt]
    f (a :> b :> TNil) = a :> b :> b :> TNil
--
normalDensity :: SType t -> Density t '[EReal, EReal]
normalDensity st = simpleDensity "normal" st (SReal ::> SReal ::> TypeNil)

lkjDensity :: Density ESqMat '[EReal]
lkjDensity = simpleDensity "lkj_corr_cholesky" SSqMat (SReal ::> TypeNil)

multi_normal_cholesky :: Density ECVec '[ECVec, ECVec, ESqMat]
multi_normal_cholesky = simpleDensity "multi_normal_cholesky" SCVec (SCVec ::> SCVec ::> SSqMat ::> TypeNil)

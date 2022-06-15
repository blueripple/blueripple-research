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

module Stan.ModelBuilder.TypedExpressions.StanFunctions
  (
    module Stan.ModelBuilder.TypedExpressions.StanFunctions
  , module Stan.ModelBuilder.TypedExpressions.Functions
  )
  where

import Stan.ModelBuilder.TypedExpressions.Types
import Stan.ModelBuilder.TypedExpressions.TypedList
import Stan.ModelBuilder.TypedExpressions.Functions



logit :: Function EReal '[EReal]
logit = Function "logit" SReal (oneType SReal)

invLogit :: Function EReal '[EReal]
invLogit = Function "inv_logit" SReal (oneType SReal)

sqrt :: Function EReal '[EReal]
sqrt = Function "sqrt" SReal (oneType SReal)

inv_sqrt :: Function EReal '[EReal]
inv_sqrt = Function "inv_sqrt" SReal (oneType SReal)

inv :: Function EReal '[EReal]
inv = Function "inv" SReal (oneType SReal)

array_num_elements :: SNat n -> SType t -> Function EInt '[EArray n t]
array_num_elements sn st = Function "inv" SInt (oneType $ SArray sn st)

vector_size :: Function EInt '[ECVec]
vector_size = Function "size" SInt (oneType SCVec)

row_vector_size :: Function EInt '[ERVec]
row_vector_size = Function "size" SInt (oneType SRVec)

vector_sum :: Function EReal '[ECVec]
vector_sum = Function "sum" SReal (oneType SCVec)

row_vector_sum :: Function EReal '[ERVec]
row_vector_sum = Function "sum" SReal (oneType SRVec)

matrix_sum :: Function EReal '[EMat]
matrix_sum = Function "sum" SReal (oneType SMat)

rep_vector :: Function ECVec '[EReal, EInt]
rep_vector = Function "sum" SCVec (SReal ::> SInt ::> TypeNil)

dot :: Function EReal '[ECVec, ECVec]
dot = Function "dot" SReal (SCVec ::> SCVec ::> TypeNil)

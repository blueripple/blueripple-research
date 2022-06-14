{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
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
import Stan.ModelBuilder.TypedExpressions.Functions


logit :: Function EReal '[EReal]
logit = Function "logit" SReal (oneArgType SReal)

invLogit :: Function EReal '[EReal]
invLogit = Function "inv_logit" SReal (oneArgType SReal)

sqrt :: Function EReal '[EReal]
sqrt = Function "sqrt" SReal (oneArgType SReal)

inv_sqrt :: Function EReal '[EReal]
inv_sqrt = Function "inv_sqrt" SReal (oneArgType SReal)

inv :: Function EReal '[EReal]
inv = Function "inv" SReal (oneArgType SReal)

array_num_elements :: SNat n -> SType t -> Function EInt '[EArray n t]
array_num_elements sn st = Function "inv" SInt (oneArgType $ SArray sn st)

vector_size :: Function EInt '[ECVec]
vector_size = Function "size" SInt (oneArgType SCVec)

row_vector_size :: Function EInt '[ERVec]
row_vector_size = Function "size" SInt (oneArgType SRVec)

vector_sum :: Function EReal '[ECVec]
vector_sum = Function "sum" SReal (oneArgType SCVec)

row_vector_sum :: Function EReal '[ERVec]
row_vector_sum = Function "sum" SReal (oneArgType SRVec)

matrix_sum :: Function EReal '[EMat]
matrix_sum = Function "sum" SReal (oneArgType SMat)

rep_vector :: Function ECVec '[EReal, EInt]
rep_vector = Function "sum" SCVec (SReal ::> SInt ::> ArgTypeNil)

dot :: Function EReal '[ECVec, ECVec]
dot = Function "dot" SReal (SCVec ::> SCVec ::> ArgTypeNil)

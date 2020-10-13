{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
module Stan.Parameters where

import qualified Data.Map as M
import qualified Data.Vector as V

import qualified GHC.TypeNats as Nat

type family NTuple (n :: Nat.Nat) where
  NTuple 0 = ()
  NTuple 1 = Int
  NTuple 2 = (Int, Int)
  NTuple 3 = (Int, Int, Int)
  NTuple 4 = (Int, Int, Int, Int)

class Nat.KnownNat n => IndexFunction n where
  tupleToIndex :: NTuple n -> NTuple n -> Int
  indexToTuple :: NTuple n -> Int -> NTuple n

instance (IndexFunction 0) where
  tupleToIndex = undefined
  indexToTuple = undefined

instance (IndexFunction 1) where
  tupleToIndex _ = id
  indexToTuple _ = id 

instance (IndexFunction 2) where
  tupleToIndex (_, nj) (i, j) = i * nj + j  
  indexToTuple (_, nj) n = let j = n `mod` nj in ((n - j) `div` nj, j)

instance (IndexFunction 3) where
  tupleToIndex (ni, nj, nk) (i, j, k) =  tupleToIndex @2 (ni, nj) (i, j) * nk + k
  indexToTuple (ni, nj, nk) n =
    let k = n `mod` nk
        m = (n - k) `div` nk
        (i, j) = indexToTuple @2 (ni, nj) m
    in (i, j, k)

--instance (IndexFunction 4) where
--  tupleToIndex (_, nj, nk, nl) = ((i * nj + j) * nk + k) * nl + l
--  indexToTuple 


{-
data ParameterStatistics a where
  ScalarStatistics :: a -> ParameterStatistics
  IndexedStatistics :: KnownNat n => Int -> (

-}

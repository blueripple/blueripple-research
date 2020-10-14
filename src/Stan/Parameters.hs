{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
module Stan.Parameters where

import qualified Data.Map as M
import qualified Data.Vector as V

--import qualified GHC.TypeNats as Nat

data Dim = D0 | D1 | D2 | D3 | D4

type family NTuple (n :: Dim) where
  NTuple D0 = ()
  NTuple D1 = Int
  NTuple D2 = (Int, Int)
  NTuple D3 = (Int, Int, Int)
  NTuple D4 = (Int, Int, Int, Int)

class IndexFunction (n :: Dim) where
  tupleToIndex :: NTuple n -> NTuple n -> Int
  indexToTuple :: NTuple n -> Int -> NTuple n

instance IndexFunction D0 where
  tupleToIndex _ _ = 0
  indexToTuple _ _ = ()
  
instance IndexFunction D1 where
  tupleToIndex _ = id
  indexToTuple _ = id 

instance IndexFunction D2 where
  tupleToIndex (_, nj) (i, j) = i * nj + j  
  indexToTuple (_, nj) n = let j = n `mod` nj in ((n - j) `div` nj, j)

instance IndexFunction D3 where
  tupleToIndex (ni, nj, nk) (i, j, k) =  tupleToIndex @D2 (ni, nj) (i, j) * nk + k
  indexToTuple (ni, nj, nk) n =
    let k = n `mod` nk
        m = (n - k) `div` nk
        (i, j) = indexToTuple @D2 (ni, nj) m
    in (i, j, k)

instance IndexFunction D4 where
  tupleToIndex (ni, nj, nk, nl) (i, j, k, l) = tupleToIndex @D3 (ni, nj, nk) (i, j, k) * nl + l
  indexToTuple (ni, nj, nk, nl) n =
    let l = n `mod` nl
        m = (n - l) `div` nl
        (i, j, k) = indexToTuple @D3 (ni, nj, nk) m
    in (i, j, k, l)


data ParameterStatistics (dim :: Dim) a where
  ParameterStatistics :: NTuple dim -> V.Vector a -> ParameterStatistics dim a
  
getIndexed :: forall dim a. IndexFunction dim => ParameterStatistics dim a -> NTuple dim -> a
getIndexed (ParameterStatistics dims vec) index = vec V.! tupleToIndex @dim dims index 

getScalar :: ParameterStatistics D0 a -> a
getScalar p@(ParameterStatistics _ v) = getIndexed p ()

data ParameterStatisticsA a where
  ParameterStatisticsA :: (forall dim. ParameterStatistics dim a) -> ParameterStatisticsA a

--parameterStatistics :: ParameterStatistics a -> NTuple

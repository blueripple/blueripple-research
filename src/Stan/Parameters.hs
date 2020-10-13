{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}
module Stan.Parameters where

import qualified Data.Map as M
import qualified Data.Vector as V

type family KnownNat n => NIndex n where
  NTuple 0 = ()
  NTuple 1 = Int
  NTuple 2 = (Int, Int)
  NTuple 3 = (Int, Int, Int)
  


data ParameterStatistics a where
  ScalarStatistics :: a -> ParameterStatistics
  IndexedStatistics :: KnownNat n => Int -> (
  




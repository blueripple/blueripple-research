{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}
module Stan.ModelCompiler.Core where

import qualified Data.Text as T
import qualified Data.Vector as V

{-
I want to write code that looks like

do
  x <- stanData StanInt "x" 12
  ys <- stanData (StanVector x)  Nothing "y" dataVec

  sigma <- stanParam StanReal
  stanModel ys (stanNormal 0 sigma)

and produce

data {
  real x;
  vector[x] y;
}

parameters {
  real sigma;
}

model {
 ys ~ normal(0, sigma);
}

as well as the correct json to set x and ys from the given sources.

-}

data NumberType = SInt | SReal

type family HNumber (a :: NumberType) :: Type where
  HNumber SInt = Int
  HNumber SReal = Double

data Structure = Scalar
               | ColVector
               | RowVector
               | UnitVector
               | OrderedVector
               | PositiveOrderedVector
               | Simplex
               | Array
               | Matrix deriving (Show, Eq, Ord)

data ArrayIndex = V.Vector Int

data ArrayDimensions = V.Vector Int

data NumberConstraint (a :: Number) where
  Upper :: HNumber a -> NumberConstraint a
  Lower :: HNumber a -> NumberConstraint a
  Multiplier :: HNumber a -> NumberConstraint a
  Offset :: HNumber a -> NumberConstraint a

data Value (a :: Structure) (b :: Number) where
  IntV :: Value Scalar StanInt
  RealV :: Value Scalar StanReal
  ColVectorV :: Int -> Value ColVector StanReal
  RowVectorV :: Int -> Value RowVector StanReal
  OrderedVectorV :: Int -> Value OrderedVector StanReal
  PositiveOrderedVectorV :: Int -> Value PositiveOrderedVector StanReal
  UnitVectorV :: Int -> Value UnitVector StanReal
  SimplexV :: Int -> Value Simplex StanReal
  MatrixV :: Int -> Int -> Value Matrix StanReal
  ArrayV :: ArrayDimensions -> Value Array b


  

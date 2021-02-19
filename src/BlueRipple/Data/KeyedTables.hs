{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE ScopedTypeVariables #-}

module BlueRipple.Data.KeyedTables where

import qualified BlueRipple.Data.Keyed as BRK
import qualified Control.Foldl as FL
import qualified Data.Array as Array
import qualified Data.Set as Set

{-
newtype TableKey a = TableKey { unTableKey :: a }

data Table k a where
  Table :: Array.Ix (TableKey k) => Array.Array (TableKey k) a -> Table k a

deriving stock Functor (Table k)
deriving stock Foldable (Table k)

data Partition a b where
  Partition :: (BRK.FiniteSet a, BRK.FiniteSet b) => (b -> Set a) -> Partition a b
-}
-- This will error if a has no elements
finiteSetMinMax :: BRK.FiniteSet a => (a, a)
finiteSetMinMax = let se = BRK.elements in (Set.findMin se, Set.findMax se)

rePartition :: (Array.Ix k1, Array.Ix k2, BRK.FiniteSet k2, Monoid x)
            => (k2 -> Set.Set k1) -> Array.Array k1 x -> Array.Array k2 x
rePartition f t =
  let setToIntF :: Set a -> a -> Int
      setToIntF s a = if a `Set.member` s then 1 else 0
      af :: BRK.AggF Bool k2 k1
      af = BRK.AggF (flip Set.member . f)
      rekeyF = BRK.aggFoldAll af BRK.dataFoldCollapseBool
  in Array.array finiteSetMinMax $ FL.fold rekeyF $ Array.assocs t

unNest :: (Array.Ix k1, Array.Ix k2, BRK.FiniteSet k1, BRK.FiniteSet k2)
       => (Array.Array k1 (Array.Array k2 x)) -> Array.Array (k1, k2) x
unNest (Table t) =
  let rekeyOne :: k1 -> (k2, x) -> ((k1, k2), x)
      rekeyOne k1 (k2, x) = ((k1, k2), x)
      rekey :: (k1, [(k2, x)]) -> [(k1, k2), x]
      rekey (k1, xs) = fmap (rekeyOne k1) xs
  in Array.array finiteSetMinMax $ fmap (rekey . second Array.assocs) $ Array.assocs t

nest :: (Array.Ix k1, Array.Ix k2, BRK.FiniteSet k1, BRK.FiniteSet k2)
         Array.Array (k1, k2) x ->  (Array.Array k1 (Array.Array k2 x))

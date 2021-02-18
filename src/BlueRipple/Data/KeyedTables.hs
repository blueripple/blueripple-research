{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE ScopedTypeVariables #-}

module BlueRipple.Data.KeyedTables where

import qualified BlueRipple.Data.Keyed as BRK
import qualified Control.Foldl as FL
import qualified Data.Array as Array

newtype TableKey a = TableKey { unTableKey :: a }

data Table k a where
  Table :: Array.Ix (TableKey k) => Array.Array (TableKey k) a -> Table k a

deriving stock Functor (Table k)
deriving stock Foldable (Table k)


-- Any element of k2 maps to a subset of k1?

simplify :: (BRK.FiniteSet k1, BRK.FiniteSet k2, Monoid x) => (k2 -> ) -> Table k1 x -> Table k2 x

{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE UndecidableInstances #-}
module BlueRipple.Data.KeyedTables where

import qualified BlueRipple.Data.Keyed as BRK
import qualified Control.Foldl as FL
import qualified Data.Array as Array
import qualified Data.ByteString.Lazy as LBS
import qualified Data.Set as Set
import qualified Data.Csv as CSV
import           Data.Csv ((.:))
import qualified Data.Vector as Vec

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
{-
rekey :: (Array.Ix k1, Array.Ix k2, BRK.FiniteSet k2, Monoid x)
      => (k2 -> Set.Set k1) -> Array.Array k1 x -> Array.Array k2 x
rekey f t =
  let setToIntF :: Set a -> a -> Int
      setToIntF s a = if a `Set.member` s then 1 else 0
      af :: BRK.AggF Bool k2 k1
      af = BRK.AggF (flip Set.member . f)
      rekeyF = BRK.aggFoldAll af BRK.dataFoldCollapseBool
  in Array.array BRK.finiteSetMinMax $ FL.fold rekeyF $ Array.assocs t

unNest :: (Array.Ix k1, Array.Ix k2, BRK.FiniteSet k1, BRK.FiniteSet k2)
       => (Array.Array k1 (Array.Array k2 x)) -> Array.Array (k1, k2) x
unNest (Table t) =
  let rekeyOne :: k1 -> (k2, x) -> ((k1, k2), x)
      rekeyOne k1 (k2, x) = ((k1, k2), x)
      rekey :: (k1, [(k2, x)]) -> [(k1, k2), x]
      rekey (k1, xs) = fmap (rekeyOne k1) xs
  in Array.array finiteSetMinMax $ fmap (rekey . second Array.assocs) $ Array.assocs t
-}
-- What we want is to parse a csv file which contains some given prefix cols and then census labeled counts
-- and produce a (long) frame with the data from that table
-- Better, the table part should be a fold so we can compose this for multiple tables.

data TablesRow f a = TablesRow { prefix :: a, counts :: f (Vec.Vector Int)}

deriving instance (Show a, Show (f (Vec.Vector Int))) => Show (TablesRow f a)

parseTablesRow :: (Traversable f, CSV.FromNamedRecord a)  => f [Text] -> CSV.NamedRecord -> CSV.Parser (TablesRow f a)
parseTablesRow tableHeaders r = TablesRow <$> CSV.parseNamedRecord r <*> traverse (parseTable r) tableHeaders where
  lookupOne :: CSV.NamedRecord -> Text -> CSV.Parser Int
  lookupOne r = CSV.lookup r . encodeUtf8
  parseTable :: CSV.NamedRecord -> [Text] -> CSV.Parser (Vec.Vector Int)
  parseTable r headers = Vec.fromList <$> traverse (lookupOne r) headers

decodeCSVTables :: forall a f.(Traversable f, CSV.FromNamedRecord a) => f [Text] -> LByteString -> Either Text (CSV.Header, Vec.Vector (TablesRow f a))
decodeCSVTables tableHeaders = first toText . CSV.decodeByNameWithP (parseTablesRow tableHeaders) CSV.defaultDecodeOptions

decodeCSVTablesFromFile :: forall a f.(Traversable f, CSV.FromNamedRecord a) => f [Text] -> FilePath -> IO (Either Text (CSV.Header, Vec.Vector (TablesRow f a)))
decodeCSVTablesFromFile tableHeaders fp = decodeCSVTables tableHeaders <$> LBS.readFile fp

{-# LANGUAGE TupleSections #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE NoMonomorphismRestriction #-}

module Graphics.Vega.VegaLite.MapRow where

import qualified Control.Foldl as Foldl
import qualified Data.Map as Map
import qualified Data.Text as Text
import qualified Graphics.Vega.VegaLite as GV

type MapRow a = Map.Map Text.Text a

fromList :: [Text.Text] -> MapRow ()
fromList = Map.fromList . fmap (, ())

withNames :: (Foldable f, Foldable g, Show a) => f Text.Text -> g a -> Either Text.Text (MapRow a)
withNames names values = fmap Map.fromList namedValues
  where
    toList = Foldl.fold Foldl.list
    namedValues =
      if Foldl.fold Foldl.length names == Foldl.fold Foldl.length values
        then Right $ zip (toList names) (toList values)
        else
          Left
            ( "Names ("
                <> show (toList names)
                <> ") and values ("
                <> show (toList values)
                <> ") have different lengths in nameRow."
            )

type VLDataField = (GV.FieldName, GV.DataValue)

toVLDataFields :: (a -> GV.DataValue) -> MapRow a -> [VLDataField]
toVLDataFields vlDataVal = Map.toList . fmap vlDataVal

toVLData :: (Functor f, Foldable f) => (MapRow a -> [(GV.FieldName, GV.DataValue)]) -> [GV.Format] -> f (MapRow a) -> GV.Data
toVLData vlFields fmt mapRows =
  ( GV.dataFromRows fmt
      . Foldl.fold dataRowF (
            fmap vlFields mapRows)
  )
    []
  where
    dataRowF = Foldl.Fold (\rows tupleList -> rows . GV.dataRow tupleList) id id

dataValueText :: GV.DataValue -> Text.Text
dataValueText (GV.Boolean b) = "Boolean: " <> show b
dataValueText (GV.DateTime ds) = "DateTime"
dataValueText (GV.Number n) = "Number: " <> show n
dataValueText (GV.Str s) = "Str: " <> s
dataValueText GV.NullValue = "NullValue"

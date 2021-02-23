{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE DerivingVia #-}
module Stan.Predictor where

import qualified Control.Foldl as Foldl
import qualified Data.Profunctor as Profunctor
import qualified Stan.JSON as Stan
import qualified Data.Vector as Vector


data Predictor a b = Predictor { predictorName :: Text
                               , getPredictor :: a -> b
                               }

instance Functor (Predictor a) where
  fmap f (Predictor n g) = Predictor n (f . g)

instance Profunctor.ProFunctor Predictor where
  dimap l r (Predictor n f) = Predictor n (r . f . l)

mapName :: (Text -> Text) -> Predictor a b -> Predictor a b
mapName f (Predictor n g) = Predictor (f $ n) g

predictorsRowF  :: (Functor f, Foldable f) => Foldl.Fold (Predictor a b) (Vector.Vector b)
predictorsRowF = FL.premap getPredictor Foldl.vector

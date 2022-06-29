{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
module BlueRipple.Data.Quantiles where

import qualified Control.Foldl as FL

import qualified Data.List as List

import qualified Control.MapReduce             as MR
import qualified Frames.Transform              as FT
import qualified Frames.Folds                  as FF
import qualified Frames.MapReduce              as FMR
import qualified Frames.Enumerations           as FE

import qualified Frames                        as F
import qualified Frames.Melt                   as F
import qualified Frames.InCore                 as FI
import qualified Data.Vinyl                    as V
import qualified Data.Vinyl.TypeLevel          as V


quantileLookup :: (Foldable f, Ord a) => (x -> a) -> Int -> f x -> x -> Either Text Int
quantileLookup getA bins rows x =
  let sortedData  = sort $ FL.fold (FL.premap getA FL.list) rows
      nData = List.length sortedData
      quantileSize = nData `div` bins
      quantilesExtra = nData `rem` bins
      quantileMaxIndex k = quantilesExtra + k * quantileSize - 1 -- puts extra in 1st bucket
      quantileBreaks =  fmap (\k -> sortedData List.!! quantileMaxIndex k) [1..bins]
      indexedBreaks = zip quantileBreaks [1..bins]
      go a [] = Left "Given number is above all data given to build quantiles"
      go a ((y, k): xs) = if a < y then Right k else go a xs
  in go (getA x) indexedBreaks

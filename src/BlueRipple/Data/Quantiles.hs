{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

module BlueRipple.Data.Quantiles
  (
    module BlueRipple.Data.Quantiles
  )
where

import qualified Control.Foldl as FL

import qualified Data.List as List

quantileBreaks :: (Foldable f, Ord a) => (x -> a) -> Int -> f x -> [(a, Int)]
quantileBreaks getA bins rows =
  let sortedData  = sort $ FL.fold (FL.premap getA FL.list) rows
      nData = List.length sortedData
      quantileSize = nData `div` bins
      quantilesExtra = nData `rem` bins
      quantileMaxIndex k = quantilesExtra + k * quantileSize - 1 -- puts extra in 1st bucket
      quantileBreaks' =  fmap (\k -> sortedData List.!! quantileMaxIndex k) [1..bins]
 in  zip quantileBreaks' [1..bins]

quantileLookup :: (Foldable f, Ord a, Show a) => (x -> a) -> Int -> f x -> x -> Either Text Int
quantileLookup getA bins rows = quantileLookup' getA (quantileBreaks getA bins rows)

quantileLookup' :: (Ord a, Show a) => (x -> a) -> [(a, Int)] -> x -> Either Text Int
quantileLookup' getA indexedBreaks x =
  let go a [] = Left $ "Given number " <> show a <> " is above all quantiles maximums " <> show (fst <$> indexedBreaks)
      go a ((y, k): xs) = if a <= y then Right k else go a xs
  in go (getA x) indexedBreaks

median :: (Foldable f, RealFrac a) => (x -> a) -> f x -> a
median getA rows =
  let sortedData = sort $ FL.fold (FL.premap getA FL.list) rows
      l = length sortedData
      ld2 = l `div` 2
  in if even (length sortedData)
     then (sortedData List.!! (ld2 - 1) + sortedData List.!! ld2) / 2
     else sortedData List.!! ld2


medianE :: (Foldable f, RealFrac a) => (x -> Either Text a) -> f x -> Either Text a
medianE getA rows = do
  sortedData <- sort <$> FL.foldM (FL.premapM getA $ FL.generalize FL.list) rows
  let l = length sortedData
      ld2 = l `div` 2
  return $ if even (length sortedData)
           then (sortedData List.!! (ld2 - 1) + sortedData List.!! ld2) / 2
           else sortedData List.!! ld2

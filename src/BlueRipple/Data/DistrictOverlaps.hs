{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module BlueRipple.Data.DistrictOverlaps where

import qualified BlueRipple.Data.ElectionTypes as ET
import qualified Data.Vector as Vec
import qualified Data.Csv as CSV
import qualified Knit.Report as K


data DistrictOverlaps a = DistrictOverlaps { stateAbbreviation :: Text
                                           , rowDistrictType :: ET.DistrictType
                                           , colDistrictType :: ET.DistrictType
                                           , populations :: Vec.Vector Int
                                           , overlaps :: Vec.Vector (Vec.Vector a) -- indexed by district number
                                           } deriving (Show, Eq, Ord, Generic, Functor)


data OverlapCSVRow = OverlapCSVRow { rowName :: Text,  pop :: Int,  overlapVec :: (Vec.Vector Int) } deriving (Show, Eq, Ord, Generic)

instance CSV.FromRecord OverlapCSVRow where
  parseRecord v = OverlapCSVRow
                  <$> v CSV..! 0
                  <*> v CSV..! 1
                  <*> fmap (Vec.fromList) (traverse (\n -> v CSV..! n) [2..(Vec.length v - 1)])

loadOverlapsFromCSV :: K.KnitEffects r => FilePath -> Text -> ET.DistrictType -> ET.DistrictType -> K.Sem r (DistrictOverlaps Int)
loadOverlapsFromCSV fp stateAbbr rowDType colDType = do
  rowsE <-  CSV.decode CSV.HasHeader <$> K.liftKnit @IO (readFileLBS fp)
  rows <- K.knitEither $ first toText rowsE
  pure $ DistrictOverlaps stateAbbr rowDType colDType (fmap pop rows) (fmap overlapVec rows)


overlapFractionsForRow :: DistrictOverlaps Int -> Int -> Vec.Vector Double
overlapFractionsForRow (DistrictOverlaps _ _ _ p ols) n = fmap (\x -> realToFrac x/realToFrac (p Vec.! (n - 1))) $ ols Vec.! (n - 1)

overlapsOverThresholdForRow :: Double -> DistrictOverlaps Int -> Int -> [(Int, Double)]
overlapsOverThresholdForRow threshold x n = filter ((>= threshold) . snd) $ zip [1..] (Vec.toList (overlapFractionsForRow x n))

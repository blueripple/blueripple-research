{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}

module BlueRipple.Data.DistrictOverlaps where

import qualified BlueRipple.Data.ElectionTypes as ET
import qualified Data.HashMap.Strict as HM
import qualified Data.Map as Map
import qualified Data.Text.Encoding as T
import qualified Data.Text.Read as T

import qualified Data.Vector as Vec
import qualified Data.Csv as CSV hiding (header)
import qualified Data.Csv.Parser as CSV
import qualified Knit.Report as K
import Database.SQLite3.Direct (errmsg)


data DistrictOverlaps a = DistrictOverlaps { stateAbbreviation :: Text
                                           , rowDistrictType :: ET.DistrictType
                                           , colDistrictType :: ET.DistrictType
                                           , populations :: Vec.Vector Int
                                           , overlaps :: Vec.Vector (Map Text Int) -- indexed by row number
                                           , rowByName :: Map Text Int
                                           } deriving (Show, Eq, Ord, Generic, Functor)


data OverlapCSVRow = OverlapCSVRow { rowName :: Text,  pop :: Int,  overlapMap :: Map Text Int } deriving (Show, Eq, Ord, Generic)


instance CSV.FromNamedRecord OverlapCSVRow where
  parseNamedRecord m =
    let overlapHM =  HM.delete "Name" $ HM.delete "TotalPopulation" m
    in  OverlapCSVRow
        <$> m CSV..: "NAME"
        <*> m CSV..: "TotalPopulation"
        <*> CSV.parseNamedRecord overlapHM

loadOverlapsFromCSV :: K.KnitEffects r => FilePath -> Text -> ET.DistrictType -> ET.DistrictType -> K.Sem r (DistrictOverlaps Int)
loadOverlapsFromCSV fp stateAbbr rowDType colDType = do
--  let options = CSV.defaultDecodeOptions
  fileLBS <- K.liftKnit @IO (readFileLBS fp)
  (header, rows) < - K.knitEither $ CSV.decodeByName fileLBS
  let populationsV = fmap pop rows
      overlapsV =  fmap overlapMap rows
      namesV = fmap rowName rows
  let rowByName = Map.fromList $ zip [0..] (Vec.toList namesV)
  pure $ DistrictOverlaps stateAbbr rowDType colDType populationsV overlapsV rowByName


overlapFractionsForRowByNumber :: DistrictOverlaps Int -> Int -> Vec.Vector Double
overlapFractionsForRowByNumber (DistrictOverlaps _ _ _ p ols _) n = fmap (\x -> realToFrac x/realToFrac (p Vec.! (n - 1))) $ ols Vec.! (n - 1)

overlapFractionsForRowByName :: DistrictOverlaps Int -> Text -> Maybe (Vec.Vector Double)
overlapFractionsForRowByName  (DistrictOverlaps _ _ _ _ _ rm) t =
  fmap (\(p, v) -> Vec.map (\x -> realToFrac x/realToFrac p) v) $ Map.lookup t rm

overlapsOverThresholdForRowByNumber :: Double -> DistrictOverlaps Int -> Int -> [(Int, Double)]
overlapsOverThresholdForRowByNumber threshold x n = filter ((>= threshold) . snd) $ zip [1..] (Vec.toList (overlapFractionsForRowByNumber x n))

overlapsOverThresholdForRowByName :: Double -> DistrictOverlaps Int -> Text -> [(Text, Double)]
overlapsOverThresholdForRowByName threshold x t = filter ((>= threshold) . snd) $ zip [1..] (Vec.toList (overlapFractionsForRowByName x t))

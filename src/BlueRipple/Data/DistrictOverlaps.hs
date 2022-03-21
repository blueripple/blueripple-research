{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}

module BlueRipple.Data.DistrictOverlaps where

import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Data.ElectionTypes as ET
import qualified BlueRipple.Data.Loaders as BR
import qualified BlueRipple.Utilities.KnitUtils as BR
import BlueRipple.Data.CensusLoaders (noMaps)
import qualified Control.Foldl as FL
import qualified Data.HashMap.Strict as HM
import qualified Data.Map as Map
import qualified Data.Set as Set
import qualified Data.Text.Encoding as T
import qualified Data.Text.Read as T
import qualified Frames as F
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
    let overlapHM =  HM.delete "NAME" $ HM.delete "TotalPopulation" m
    in  OverlapCSVRow
        <$> m CSV..: "NAME"
        <*> m CSV..: "TotalPopulation"
        <*> CSV.parseNamedRecord overlapHM

loadOverlapsFromCSV :: K.KnitEffects r => FilePath -> Text -> ET.DistrictType -> ET.DistrictType -> K.Sem r (DistrictOverlaps Int)
loadOverlapsFromCSV fp stateAbbr rowDType colDType = do
--  let options = CSV.defaultDecodeOptions
  fileLBS <- K.liftKnit @IO (readFileLBS fp)
  (header, rows) <- K.knitEither $ first toText $ CSV.decodeByName fileLBS
  let populationsV = fmap pop rows
      overlapsV =  fmap overlapMap rows
      namesV = fmap rowName rows
  let rowByName = Map.fromList $ zip (Vec.toList namesV) [0..]
  pure $ DistrictOverlaps stateAbbr rowDType colDType populationsV overlapsV rowByName

overlapFractionsForRowByNumber :: DistrictOverlaps Int -> Int -> Map Text Double
overlapFractionsForRowByNumber (DistrictOverlaps _ _ _ p ols _) n = fmap (\x -> realToFrac x/realToFrac (p Vec.! n)) $ ols Vec.! n

overlapFractionsForRowByName :: DistrictOverlaps Int -> Text -> Maybe (Map Text Double)
overlapFractionsForRowByName  x t = overlapFractionsForRowByNumber x <$> Map.lookup t (rowByName x)

overlapsOverThresholdForRowByNumber :: Double -> DistrictOverlaps Int -> Int -> Map Text Double
overlapsOverThresholdForRowByNumber threshold x n = Map.filter (>= threshold) $ overlapFractionsForRowByNumber x n

overlapsOverThresholdForRowByName :: Double -> DistrictOverlaps Int -> Text -> Maybe (Map Text Double)
overlapsOverThresholdForRowByName threshold x t = overlapsOverThresholdForRowByNumber threshold x <$>  Map.lookup t (rowByName x)


overlapCollection :: K.KnitEffects r => Set Text -> (Text -> FilePath) -> ET.DistrictType -> ET.DistrictType -> K.Sem r (Map Text (DistrictOverlaps Int))
overlapCollection stateAbbreviations abbrToOverlapFile rowDType colDType = do
  let loadOne sa = loadOverlapsFromCSV  (abbrToOverlapFile sa) sa rowDType colDType >>= return . (sa,)
  fmap Map.fromList $ traverse loadOne $ Set.toList stateAbbreviations

oldCDOverlapCollection :: (K.KnitEffects r, BR.CacheEffects r) => K.Sem r (Map Text (DistrictOverlaps Int))
oldCDOverlapCollection = do
  stateInfo <- K.ignoreCacheTimeM BR.stateAbbrCrosswalkLoader
  let states = FL.fold (FL.premap (F.rgetField @BR.StateAbbreviation) FL.set)
               $ F.filterFrame (\r -> (F.rgetField @BR.StateFIPS r < 60)
                                 && not (F.rgetField @BR.OneDistrict r)
                                 && not (F.rgetField @BR.StateAbbreviation r `Set.member` noMaps)
                               ) stateInfo
  overlapCollection states (\sa -> toString $ "data/cdOverlaps/" <> sa <> ".csv") ET.Congressional ET.Congressional

{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}

module BlueRipple.Data.DistrictOverlaps
  (
    module BlueRipple.Data.DistrictOverlaps
  )
where

import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Data.ElectionTypes as ET
import qualified BlueRipple.Data.GeographicTypes as GT
import qualified BlueRipple.Data.Loaders as BR
import qualified BlueRipple.Utilities.KnitUtils as BR
import qualified BlueRipple.Data.Loaders.Redistricting as DRA

import BlueRipple.Data.CensusLoaders (noMaps)
import qualified Control.Foldl as FL
import qualified Data.HashMap.Strict as HM
import qualified Data.Map as Map
import qualified Data.Set as Set
import qualified Frames as F
import qualified Frames.Transform as FT
import qualified Frames.Streamly.TH as FTH
import qualified Data.Vector as Vec
import qualified Data.Csv as CSV hiding (header)
import Control.Lens (view, (^.))
import qualified Data.Vinyl as V
import qualified Text.Read as TR

import qualified Frames.MapReduce as FMR

import qualified Knit.Report as K
import qualified BlueRipple.Utilities.KnitUtils as BRK

FTH.declareColumn "Overlap" ''Double
FTH.declareColumn "CongressionalPPL" ''Double

data DistrictOverlaps a = DistrictOverlaps { stateAbbreviation :: Text
                                           , rowDistrictType :: GT.DistrictType
                                           , colDistrictType :: GT.DistrictType
                                           , populations :: Vec.Vector Int
                                           , overlaps :: Vec.Vector (Map Text Int) -- indexed by row number
                                           , rowByName :: Map Text Int
                                           } deriving stock (Show, Eq, Ord, Generic, Functor)


data OverlapCSVRow = OverlapCSVRow { rowName :: Text,  pop :: Int,  overlapMap :: Map Text Int } deriving stock (Show, Eq, Ord, Generic)


instance CSV.FromNamedRecord OverlapCSVRow where
  parseNamedRecord m =
    let overlapHM =  HM.delete "NAME" $ HM.delete "TotalPopulation" m
    in  OverlapCSVRow
        <$> m CSV..: "NAME"
        <*> m CSV..: "TotalPopulation"
        <*> CSV.parseNamedRecord overlapHM

loadOverlapsFromCSV :: K.KnitEffects r => FilePath -> Text -> GT.DistrictType -> GT.DistrictType -> K.Sem r (DistrictOverlaps Int)
loadOverlapsFromCSV fp stateAbbr rowDType colDType = do
--  let options = CSV.defaultDecodeOptions
  fileLBS <- K.liftKnit @IO (readFileLBS fp)
  (_, rows) <- K.knitEither $ first toText $ CSV.decodeByName fileLBS
  let populationsV = fmap pop rows
      overlapsV =  fmap overlapMap rows
      namesV = fmap rowName rows
  let rowByName' = Map.fromList $ zip (Vec.toList namesV) [0..]
  pure $ DistrictOverlaps stateAbbr rowDType colDType populationsV overlapsV rowByName'

overlapFractionsForRowByNumber :: DistrictOverlaps Int -> Int -> Map Text Double
overlapFractionsForRowByNumber (DistrictOverlaps _ _ _ p ols _) n = fmap (\x -> realToFrac x / realToFrac (p Vec.! n)) $ ols Vec.! n

overlapFractionsForRowByName :: DistrictOverlaps Int -> Text -> Maybe (Map Text Double)
overlapFractionsForRowByName  x t = overlapFractionsForRowByNumber x <$> Map.lookup t (rowByName x)

overlapsOverThresholdForRowByNumber :: Double -> DistrictOverlaps Int -> Int -> Map Text Double
overlapsOverThresholdForRowByNumber threshold x n = Map.filter (>= threshold) $ overlapFractionsForRowByNumber x n

overlapsOverThresholdForRowByName :: Double -> DistrictOverlaps Int -> Text -> Maybe (Map Text Double)
overlapsOverThresholdForRowByName threshold x t = overlapsOverThresholdForRowByNumber threshold x <$>  Map.lookup t (rowByName x)


overlapCollection :: K.KnitEffects r => Set Text -> (Text -> FilePath) -> GT.DistrictType -> GT.DistrictType -> K.Sem r (Map Text (DistrictOverlaps Int))
overlapCollection stateAbbreviations abbrToOverlapFile rowDType colDType = do
  let loadOne sa = (sa, ) <$> loadOverlapsFromCSV  (abbrToOverlapFile sa) sa rowDType colDType
  fmap Map.fromList $ traverse loadOne $ Set.toList stateAbbreviations

oldCDOverlapCollection :: (K.KnitEffects r, BR.CacheEffects r) => K.Sem r (Map Text (DistrictOverlaps Int))
oldCDOverlapCollection = do
  stateInfo <- K.ignoreCacheTimeM BR.stateAbbrCrosswalkLoader
  let states = FL.fold (FL.premap (F.rgetField @GT.StateAbbreviation) FL.set)
               $ F.filterFrame (\r -> (F.rgetField @GT.StateFIPS r < 60)
                                 && not (F.rgetField @BR.OneDistrict r)
                                 && not (F.rgetField @GT.StateAbbreviation r `Set.member` noMaps)
                               ) stateInfo
  overlapCollection states (\sa -> toString $ "data/cdOverlaps/" <> sa <> ".csv") GT.Congressional GT.Congressional


-- keep only the max overlap for each SLD
newtype CompareOverlap = CompareOverlap  {unCompareOverlap :: F.Record [Overlap, GT.CongressionalDistrict, CongressionalPPL] } deriving (Eq)
instance Ord CompareOverlap where
  compare (CompareOverlap a) (CompareOverlap b) = compare (a ^. overlap) (b ^. overlap)

maxSLD_CDOverlaps :: F.FrameRec [GT.DistrictTypeC, GT.DistrictName, Overlap, GT.CongressionalDistrict, CongressionalPPL]
                  -> Maybe (F.FrameRec [GT.DistrictTypeC, GT.DistrictName, Overlap, GT.CongressionalDistrict, CongressionalPPL])
maxSLD_CDOverlaps = FL.foldM outerFldM
  where
    outerFldM = FMR.concatFoldM
                $ FMR.mapReduceFoldM
                (FMR.generalizeUnpack FMR.noUnpack)
                (FMR.generalizeAssign $ FMR.assignKeysAndData @[GT.DistrictTypeC, GT.DistrictName] @[Overlap, GT.CongressionalDistrict, CongressionalPPL])
                (FMR.makeRecsWithKeyM id $ FMR.ReduceFoldM $ const $ fmap (pure @[]) $ foldToFoldM innerFldM)
    innerFldM = fmap (fmap unCompareOverlap) $ FL.premap CompareOverlap FL.maximum

-- overlaps along with congressionalPPL
sldCDOverlaps :: (K.KnitEffects r, BRK.CacheEffects r) =>  Map Text Bool -> Text
              -> K.Sem r (F.FrameRec [GT.DistrictTypeC, GT.DistrictName, Overlap, GT.CongressionalDistrict, CongressionalPPL])
sldCDOverlaps stateUpperOnlyMap sa = do
  upperOnly <- K.knitMaybe ("sldCDOverlaps: " <> sa <> " missing from stateUpperOnlyMap") $ Map.lookup sa stateUpperOnlyMap
--  allSLDPlansMap <- DRA.allPassedSLDPlans
  allCDPlansMap <- DRA.allPassedCongressionalPlans
  draCD <- K.ignoreCacheTimeM $ DRA.lookupAndLoadRedistrictingPlanAnalysis allCDPlansMap (DRA.redistrictingPlanId sa "Passed" GT.Congressional)
  let cdRow r = not $ (r ^. GT.districtName) `elem` ["Summary", "Un","\"\""]
--      sLDs = F.filterFrame nonSummary draSLD
      cDs = F.filterFrame cdRow draCD
  overlapsL <- overlapCollection (Set.singleton sa) (\x -> toString $ "data/districtOverlaps/" <> x <> "_SLDL" <> "_CD.csv") GT.StateLower GT.Congressional
  overlapsU <- overlapCollection (Set.singleton sa) (\x -> toString $ "data/districtOverlaps/" <> x <> "_SLDU" <> "_CD.csv") GT.StateUpper GT.Congressional
  let overlappingSLDs' cdn overlaps' = do
        let
          pwo = Vec.zip (populations overlaps') (overlaps overlaps')
          nameByRow = fmap fst $ sortOn snd $ Map.toList $ rowByName overlaps'
          intDiv x y = realToFrac x / realToFrac y
          olFracM (pop', ols) = fmap (\x-> intDiv x pop') $ Map.lookup cdn ols
          olFracsM = traverse olFracM pwo
        olFracs <- K.knitMaybe ("sldCDOverlaps: Missing CD in overlap data for " <> cdn) olFracsM
        let namedOlFracs = zip nameByRow (Vec.toList olFracs)
        pure namedOlFracs

      overlappingSLDs cdn = do
        overlapsU' <- K.knitMaybe ("sldCDOverlap: Failed to find overlap data for upper chamber of " <> sa) $ Map.lookup sa overlapsU
        namedOverU <- overlappingSLDs' cdn overlapsU'
        let upperRes =  fmap (\(n, x) -> (GT.StateUpper, n, x)) namedOverU
        case upperOnly of
          True -> pure upperRes
          False -> do
            overlapsL' <- K.knitMaybe ("sldCDOverlap: Failed to find overlap data for lower chamber of " <> sa) $ Map.lookup sa overlapsL
            namedOverL <- overlappingSLDs' cdn overlapsL'
            pure $ upperRes <> fmap (\(n, x) -> (GT.StateLower, n, x)) namedOverL
      dName = view GT.districtName
--      sld r = (r ^. GT.districtTypeC, dName r)
      f r = fmap (\ols -> (dName r, r ^. ET.demShare, ols)) $ overlappingSLDs (dName r)
  BRK.logFrame cDs
  overlapsByCD <- traverse f $ FL.fold FL.list cDs
  let toRecInner :: (GT.DistrictType, Text, Double) -> F.Record [GT.DistrictTypeC, GT.DistrictName, Overlap]
      toRecInner (dt, dn, ol) = dt F.&: dn F.&: ol F.&: V.RNil
      toRec :: Double -> F.Record '[GT.CongressionalDistrict] -> F.Record [GT.DistrictTypeC, GT.DistrictName, Overlap]
            -> F.Record [GT.DistrictTypeC, GT.DistrictName, Overlap, GT.CongressionalDistrict, CongressionalPPL]
      toRec cdPPL cdRec ri = ri F.<+> cdRec F.<+> FT.recordSingleton @CongressionalPPL cdPPL
      toRecs :: (Text, Double, [(GT.DistrictType, Text, Double)])
             -> Either Text [F.Record [GT.DistrictTypeC, GT.DistrictName, Overlap, GT.CongressionalDistrict, CongressionalPPL]]
      toRecs (cdT, cdPPL, slds) = fmap (\cdRec ->  fmap (toRec cdPPL cdRec) $ fmap toRecInner slds) $ cdNameToCDRec cdT
  F.toFrame . mconcat <$> (K.knitEither $ traverse toRecs overlapsByCD)

foldToFoldM :: Monad m => FL.Fold a (m b) -> FL.FoldM m a b
foldToFoldM = FMR.postMapM id . FL.generalize

cdNameToCDRec :: Text -> Either Text (F.Record '[GT.CongressionalDistrict])
cdNameToCDRec t =
  fmap (FT.recordSingleton @GT.CongressionalDistrict)
  $ maybe (Left $ "cdNameTOCDRec: Failed to parse " <> t <> " as an Int") Right
  $ TR.readMaybe $ toString t

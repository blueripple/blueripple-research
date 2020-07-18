{-# LANGUAGE AllowAmbiguousTypes       #-}
{-# LANGUAGE DataKinds                 #-}
{-# LANGUAGE DeriveDataTypeable        #-}
{-# LANGUAGE DeriveGeneric             #-}
{-# LANGUAGE DerivingVia               #-}
{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE FlexibleInstances         #-}
{-# LANGUAGE GADTs                     #-}

{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE PolyKinds                 #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE TypeApplications          #-}
{-# LANGUAGE TypeFamilies              #-}
{-# LANGUAGE TypeOperators             #-}
{-# LANGUAGE QuasiQuotes               #-}
{-# LANGUAGE StandaloneDeriving        #-}
{-# LANGUAGE TupleSections             #-}
{-# LANGUAGE UndecidableInstances      #-}
{-# OPTIONS_GHC  -fplugin=Polysemy.Plugin  #-}

module MRP.DistrictClusters where

import qualified Control.Foldl                 as FL
import           Data.Discrimination            ( Grouping )
import qualified Data.List as List
import qualified Data.Map as M
import           Data.Maybe (catMaybes, fromMaybe)
import qualified Data.Text                     as T
import qualified Data.Serialize                as Serialize
import qualified Data.Vector                   as Vec
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V

import GHC.Generics (Generic)

import           Graphics.Vega.VegaLite.Configuration as FV
import qualified Graphics.Vega.VegaLite.Compat as FV
import qualified Frames as F
import qualified Frames.Melt as F
import qualified Frames.CSV as F
import qualified Frames.InCore                 as FI
import qualified Polysemy.Error as P
import qualified Polysemy.RandomFu             as PRF
import qualified Polysemy.ConstraintAbsorber.MonadRandom as PMR

import qualified Control.MapReduce             as MR
import qualified Frames.Transform              as FT
import qualified Frames.Folds                  as FF
import qualified Frames.MapReduce              as FMR
import qualified Frames.SimpleJoins            as FJ
import qualified Frames.Misc                   as FM
import qualified Frames.Serialize              as FS
import qualified Frames.KMeans                 as FK
import qualified Math.Rescale                  as MR

import qualified Frames.Visualization.VegaLite.Data
                                               as FV

import qualified Graphics.Vega.VegaLite        as GV
import qualified Knit.Report                   as K

  
import           Data.String.Here               ( i, here )

import qualified Text.Blaze.Html               as BH
import qualified Text.Blaze.Html5.Attributes   as BHA

import           BlueRipple.Configuration 
import           BlueRipple.Utilities.KnitUtils 
import qualified BlueRipple.Utilities.TableUtils as BR

import qualified Numeric.GLM.ProblemTypes      as GLM
import qualified Numeric.GLM.Bootstrap            as GLM
import qualified Numeric.GLM.MixedModel            as GLM

import qualified Data.Time.Calendar            as Time
import qualified Data.Time.Clock               as Time

import qualified Data.Datamining.Clustering.SGM as SGM
import qualified Data.Datamining.Clustering.Classifier as SOM
import qualified Data.Datamining.Clustering.SOM as SOM
import qualified Math.Geometry.Grid as Grid
import qualified Math.Geometry.Grid.Square as Grid
import qualified Math.Geometry.GridMap as GridMap
import qualified Math.Geometry.GridMap.Lazy as GridMap 
import qualified Data.Random.Distribution.Uniform as RandomFu
import qualified Data.Random as RandomFu

import qualified Data.Word as Word

import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Data.Loaders as BR
import qualified BlueRipple.Data.ACS_PUMS as PUMS
import qualified BlueRipple.Data.CPSVoterPUMS as CPS
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.ElectionTypes as ET

import qualified BlueRipple.Model.MRP as BR
import qualified BlueRipple.Model.Turnout_MRP as BR

import qualified BlueRipple.Data.UsefulDataJoins as BR
import qualified MRP.CCES_MRP_Analysis as BR
import qualified MRP.CachedModels as BR
import qualified BlueRipple.Utilities.KnitUtils as BR
import qualified BlueRipple.Data.Keyed         as Keyed
import MRP.Common
import MRP.CCES
import qualified MRP.CCES as CCES

text1 :: T.Text
text1 = [i|

|]

text2 :: T.Text = [here|

|]
  

type ASER5CD as = ('[BR.Year, BR.StateAbbreviation, BR.StateFIPS, BR.CongressionalDistrict] V.++ DT.CatColsASER5) V.++ as

addPUMSZerosF :: FL.Fold (F.Record (ASER5CD '[PUMS.Citizens, PUMS.NonCitizens])) (F.FrameRec (ASER5CD '[PUMS.Citizens, PUMS.NonCitizens]))
addPUMSZerosF =
  let zeroPop ::  F.Record '[PUMS.Citizens, PUMS.NonCitizens]
      zeroPop = 0 F.&: 0 F.&: V.RNil
  in FMR.concatFold
     $ FMR.mapReduceFold
     FMR.noUnpack
     (FMR.assignKeysAndData @'[BR.Year, BR.StateAbbreviation, BR.StateFIPS, BR.CongressionalDistrict])
     (FMR.makeRecsWithKey id
       $ FMR.ReduceFold
       $ const
       $ Keyed.addDefaultRec @DT.CatColsASER5 zeroPop)
                                                                  
type PctWWC = "PctWWC" F.:-> Double
type PctBlack = "PctBlack" F.:-> Double
type PctNonWhite = "PctNonWhite" F.:-> Double
type PctYoung = "PctYoung" F.:-> Double
type VoteShare2016 = "2016Share" F.:-> Double
type Cluster = "Cluster" F.:-> (Int, Int)

type instance FI.VectorFor (Int, Int) = K.Vector

isWWC r = F.rgetField @DT.Race5C r == DT.R5_WhiteNonLatinx && F.rgetField @DT.CollegeGradC r == DT.Grad
isBlack r =  F.rgetField @DT.Race5C r == DT.R5_Black
isNonWhite r =  F.rgetField @DT.Race5C r /= DT.R5_WhiteNonLatinx
isYoung r = F.rgetField @DT.SimpleAgeC r == DT.Under

type W = "w" F.:-> Double

districtToWWCBlack :: FL.Fold
                        (F.Record (PUMS.CDCounts DT.CatColsASER5))
                        (F.FrameRec '[BR.Year, BR.StateAbbreviation, BR.CongressionalDistrict, PctWWC, PctBlack, W])
districtToWWCBlack =
  let districtToXYW :: FL.Fold (F.Record (DT.CatColsASER5 V.++ '[PUMS.Citizens])) (F.Record [PctWWC, PctBlack, W])
      districtToXYW =
        let --isWWC r = F.rgetField @DT.Race5C r == DT.R5_WhiteNonLatinx && F.rgetField @DT.CollegeGradC r == DT.Grad
            --isBlack r =  F.rgetField @DT.Race5C r == DT.R5_Black
            citizens = realToFrac . F.rgetField @PUMS.Citizens        
            citizensF = FL.premap citizens FL.sum
            pctWWCF = (/) <$> FL.prefilter isWWC (FL.premap citizens FL.sum) <*> citizensF
            pctBlackF = (/) <$> FL.prefilter isBlack (FL.premap citizens FL.sum) <*> citizensF
        in (\x y w -> x F.&: y F.&: w F.&: V.RNil) <$> pctWWCF <*> pctBlackF <*> citizensF
  in FMR.concatFold
     $ FMR.mapReduceFold
     FMR.noUnpack
     (FMR.assignKeysAndData @'[BR.Year, BR.StateAbbreviation, BR.CongressionalDistrict] @(DT.CatColsASER5 V.++ '[PUMS.Citizens]))
     (FMR.foldAndAddKey districtToXYW)

votesToVoteShareF :: FL.Fold (F.Record [ET.Party, ET.Votes]) (F.Record '[ET.PrefType, BR.DemPref])
votesToVoteShareF =
  let
    party = F.rgetField @ET.Party
    votes = F.rgetField @ET.Votes
    demVotesF = FL.prefilter (\r -> party r == ET.Democratic) $ FL.premap votes FL.sum
    demRepVotesF = FL.prefilter (\r -> let p = party r in (p == ET.Democratic || p == ET.Republican)) $ FL.premap votes FL.sum
    demPref d dr = if dr > 0 then realToFrac d/realToFrac dr else 0
    demPrefF = demPref <$> demVotesF <*> demRepVotesF
  in fmap (\x -> FT.recordSingleton ET.VoteShare `V.rappend` FT.recordSingleton @BR.DemPref x) demPrefF


data DistrictP as rs = DistrictP { districtId :: F.Record as, isModel :: Bool, districtData :: [F.Record rs] } deriving (Generic)
deriving instance (Show (F.Record as), Show (F.Record rs)) => Show (DistrictP as rs)
--instance S.Serialize (DistrictP as rs)

absMetric :: [Double] -> [Double] -> Double
absMetric xs ys =  FL.fold FL.sum $ fmap abs $ zipWith (-) xs ys

meanAbsMetric :: [Double] -> [Double] -> Double
meanAbsMetric xs ys =  FL.fold ((/) <$> fmap realToFrac FL.sum <*> fmap realToFrac FL.length)  $ fmap abs $ zipWith (-) xs ys

districtDiff :: forall ks d as rs.(Ord (F.Record ks)
                                  , ks F.⊆ rs
                                  , F.ElemOf rs d
                                  , V.KnownField d
                                  , Real (V.Snd d)
                                  )
             => ([Double] -> [Double] -> Double) 
             -> DistrictP as rs
             -> DistrictP as rs
             -> Double
districtDiff metric d1 d2 =
  let asMap = FL.fold (FL.premap (\r-> (F.rcast @ks r, F.rgetField @d r)) FL.map)
      r1Ns = fmap snd $ M.toList $ asMap $ districtData d1      
      r2Ns = fmap snd $ M.toList $ asMap $ districtData d2
      r1T = realToFrac $ FL.fold FL.sum r1Ns
      r2T = realToFrac $ FL.fold FL.sum r2Ns
      r1xs = fmap (\n -> realToFrac n/r1T) r1Ns
      r2xs = fmap (\n -> realToFrac n/r2T) r2Ns
  in FL.fold FL.sum $ fmap abs $ zipWith (-) r1xs r2xs


districtMakeSimilar :: forall ks d as.(Ord (F.Record ks)
                                      , ks F.⊆ (ks V.++ '[d])
                                      , F.ElemOf (ks V.++ '[d]) d
                                      , V.KnownField d
                                      , Real (V.Snd d)
                                      )
                    => (Double -> V.Snd d)
                    -> DistrictP as (ks V.++ '[d])
                    -> Double
                    -> DistrictP as (ks V.++ '[d])
                    -> DistrictP as (ks V.++ '[d])
districtMakeSimilar toD tgtD x patD =
  let asMap = FL.fold (FL.premap (\r-> (F.rcast @ks r, F.rgetField @d r)) FL.map)
      tgtNs = fmap snd $ M.toList $ asMap $ districtData tgtD      
      patKNs = M.toList $ asMap $ districtData patD
      f nt (kp, np) = (kp, realToFrac np * (1 - x) + realToFrac nt * x)
      newPatKNs = zipWith f tgtNs patKNs
      makeD :: Double -> F.Record '[d]
      makeD x = toD x F.&: V.RNil
      newPatRecs = fmap (\(k, x) -> V.rappend k  (makeD x)) newPatKNs
  in DistrictP (districtId patD) True  newPatRecs

buildSOM :: forall ks d as r.
  (K.KnitEffects r
  , K.Member PRF.RandomFu r
  , ks F.⊆ (ks V.++ '[d])
  , V.KnownField d
  , F.ElemOf (ks V.++ '[d]) d
  , Real (V.Snd d)
  , Ord (F.Record ks)
  )
  => (Int, Int) -- rectangular grid sizes
  -> [DistrictP as (ks V.++ '[d])]
  -> K.Sem r (SOM.SOM Double Double (GridMap.LGridMap Grid.RectSquareGrid) Double (Int, Int) (DistrictP as (ks V.++ '[d])))
buildSOM (gridRows, gridCols) sampleFrom = do
  let g = Grid.rectSquareGrid gridRows gridCols
      numSamples = gridRows * gridCols
      sampleUniqueIndex is = do
        newIndex <- PRF.sampleRVar (RandomFu.uniform 0 (length sampleFrom - 1))
        if newIndex `elem` is then sampleUniqueIndex is else return (newIndex : is)
  sampleIndices <- FL.foldM (FL.FoldM (\is _ -> sampleUniqueIndex is) (return []) return) [1..numSamples]
  let samples = fmap (\n -> sampleFrom !! n) sampleIndices
      gm = GridMap.lazyGridMap g samples
      n' = fromIntegral (length sampleFrom)
      lrf = SOM.decayingGaussian 0.5 0.1 0.3 0.1 n'
      dDiff = districtDiff @ks @d meanAbsMetric
      dMakeSimilar = districtMakeSimilar @ks @d (fromIntegral . round)
  return $ SOM.SOM gm lrf dDiff dMakeSimilar 0
     
post :: forall r.(K.KnitMany r, K.CacheEffectsD r, K.Member GLM.RandomFu r) => Bool -> K.Sem r ()
post updated = P.mapError BR.glmErrorToPandocError $ K.wrapPrefix "BidenVsWWC" $ do

  let clusterRowsToS (cs, rs) = (fmap FS.toS cs, fmap FS.toS rs)
      clusterRowsFromS (cs', rs') = (fmap FS.fromS cs', fmap FS.fromS rs')

  pums2018ByCD_C <- do
    demo_C <- PUMS.pumsLoader
    BR.retrieveOrMakeFrame "mrp/DistrictClusters/pums2018ByCD.bin" demo_C $ \pumsRaw -> do
      let pums2018Raw = F.filterFrame ((== 2018) . F.rgetField @BR.Year) pumsRaw
      pumsCDRollup <- PUMS.pumsCDRollup (PUMS.pumsKeysToASER5 True . F.rcast) pums2018Raw
      return $ FL.fold addPUMSZerosF pumsCDRollup

  houseVoteShare2018_C <- do
    houseResults_C <- BR.houseElectionsLoader
    BR.retrieveOrMakeFrame "mrp/DistrictClusters/houseVoteShare2018.bin" houseResults_C $ \houseResults -> do
      let filterHR r = F.rgetField @BR.Year r == 2018
                       && F.rgetField @BR.Stage r == "gen"
                       && F.rgetField @BR.Runoff r == False
                       && F.rgetField @BR.Special r == False
                       && (F.rgetField @ET.Party r == ET.Democratic || F.rgetField @ET.Party r == ET.Republican)
          houseResults2018 = fmap (F.rcast @[BR.Year, BR.StateAbbreviation, BR.CongressionalDistrict, ET.Party, ET.Votes, ET.TotalVotes])
                         $ F.filterFrame filterHR houseResults
          houseResults2018F = FMR.concatFold $ FMR.mapReduceFold
                              FMR.noUnpack
                              (FMR.assignKeysAndData @[BR.Year, BR.StateAbbreviation, BR.CongressionalDistrict])
                              (FMR.foldAndAddKey votesToVoteShareF)
      return $ FL.fold houseResults2018F houseResults2018

  clusteredDistricts <- K.ignoreCacheTimeM $ do
    let cachedDeps = (,) <$> pums2018ByCD_C <*> houseVoteShare2018_C
    K.retrieveOrMakeTransformed
      clusterRowsToS
      clusterRowsFromS
      "mrp/DistrictClusters/clusteredDistricts.bin"
      cachedDeps $ \(pumsByCD, houseVoteShare) -> do      
        let labelCD r = F.rgetField @BR.StateAbbreviation r <> "-" <> (T.pack $ show $ F.rgetField @BR.CongressionalDistrict r)          
            forClustering = F.toFrame
                            $ catMaybes
                            $ fmap F.recMaybe
                            $ F.leftJoin @[BR.Year, BR.StateAbbreviation, BR.CongressionalDistrict]
                            (FL.fold districtToWWCBlack pumsByCD)
                            houseVoteShare
        let initialCentroidsF n 
              = FMR.functionToFoldM $ \hx -> PMR.absorbMonadRandom $ FK.kMeansPPCentroids @PctWWC @PctBlack @W FK.euclidSq n hx            
        rawClusters <- FK.kMeansOneWithClusters @PctWWC @PctBlack @W
                       (FL.premap (\r -> (F.rgetField @PctWWC r, F.rgetField @W r)) $ MR.weightedScaleAndUnscale (MR.RescaleNormalize 1) (MR.RescaleNormalize 1) id)
                       (FL.premap (\r -> (F.rgetField @PctBlack r, F.rgetField @W r)) $ MR.weightedScaleAndUnscale (MR.RescaleNormalize 1) (MR.RescaleNormalize 1) id)
                       20
                       10
                       initialCentroidsF
                       (FK.weighted2DRecord @PctWWC @PctBlack @W)
                       FK.euclidSq
                       forClustering
        return $ FK.clusteredRowsFull @PctWWC @PctBlack @W labelCD $ M.fromList [((2018 F.&: V.RNil) :: F.Record '[BR.Year], rawClusters)]

-- SOM
  districtsForSOM :: [DistrictP [BR.StateAbbreviation, BR.CongressionalDistrict, ET.DemPref] (DT.CatColsASER5 V.++ '[PUMS.Citizens])] <- do
    pumsByCD <- K.ignoreCacheTime pums2018ByCD_C
    houseVoteShare <- K.ignoreCacheTime houseVoteShare2018_C
    let (pumsByCDWithVoteShare, missing) = FJ.leftJoinWithMissing  @([BR.Year, BR.StateAbbreviation, BR.CongressionalDistrict])
                                           pumsByCD
                                           houseVoteShare
    K.logLE K.Info $ "Districts with missing house results (dropped from analysis):" <> (T.pack $ show $ List.nub $ missing)
    return
      $ FL.fold
      (FMR.mapReduceFold
       FMR.noUnpack
       (FMR.assignKeysAndData @[BR.StateAbbreviation, BR.CongressionalDistrict, ET.DemPref] @(DT.CatColsASER5 V.++ '[PUMS.Citizens]))
       (FMR.ReduceFold $ \k -> fmap (\rs -> DistrictP k False rs) FL.list)
      )
      pumsByCDWithVoteShare

  K.logLE K.Info $ "Working with " <> (T.pack $ show (FL.fold FL.length districtsForSOM)) <> " districts." 
{-      
  let dDiff = districtDiff @DT.CatColsASER5 @PUMS.Citizens meanAbsMetric
      dMakeSimilar = districtMakeSimilar @DT.CatColsASER5 @PUMS.Citizens @[BR.StateAbbreviation, BR.CongressionalDistrict, ET.DemPref] (fromIntegral . round)
      sgm0 :: SGM.SGM Int Double Word (DistrictP [BR.StateAbbreviation, BR.CongressionalDistrict, ET.DemPref] (DT.CatColsASER5 V.++ '[PUMS.Citizens]))
      sgm0 = SGM.makeSGM (SGM.exponential 0.1 0.5) 20 0.1 True dDiff dMakeSimilar 
      sgm = SGM.trainBatch sgm0 districtsForSOM
  K.logLE K.Info $ "SGM model map:" <> (T.pack $ show $ fmap districtId $ SGM.modelMap sgm)
-}
  let gridRows = 3
      gridCols = 3
  randomOrderDistricts <- PRF.sampleRVar $ RandomFu.shuffle districtsForSOM 
  som0 <- buildSOM @DT.CatColsASER5 @PUMS.Citizens (gridRows, gridCols) districtsForSOM
  let som = SOM.trainBatch som0 randomOrderDistricts

  K.logLE K.Info $ "Building SOM heatmap of DemPref"    
  let districtPref = F.rgetField @ET.DemPref . districtId
      heatMap0 :: GridMap.LGridMap Grid.RectSquareGrid (Int, Double)
      heatMap0  = GridMap.lazyGridMap (Grid.rectSquareGrid gridRows gridCols) $ replicate (gridRows * gridCols) (0, 0)
      adjustOne pref (districts, avgPref)  = (districts + 1, ((realToFrac districts * avgPref) + pref)/(realToFrac $ districts + 1))
      heatMap = FL.fold (FL.Fold (\gm d -> let pref = districtPref d in GridMap.adjust (adjustOne pref) (SOM.classify som d) gm) heatMap0 id) districtsForSOM
  K.logLE K.Info $ "Building cluster table"
  let distStats :: DistrictP [BR.StateAbbreviation, BR.CongressionalDistrict, ET.DemPref] (DT.CatColsASER5 V.++ '[PUMS.Citizens])
                -> F.Record [BR.StateAbbreviation, BR.CongressionalDistrict, ET.DemPref, PctWWC, PctNonWhite, PctYoung, Cluster]
      distStats d =
        let vac = F.rgetField @PUMS.Citizens
            vacF = fmap realToFrac $ FL.premap vac FL.sum
            pctWWCF = (/) <$> FL.prefilter isWWC vacF <*> vacF
            pctNonWhite = (/) <$> FL.prefilter isNonWhite vacF <*> vacF
            pctYoung = (/) <$> FL.prefilter isYoung vacF <*> vacF
            statsRec :: [F.Record (DT.CatColsASER5 V.++ '[PUMS.Citizens])] -> F.Record [PctWWC, PctNonWhite, PctYoung]
            statsRec rs = FL.fold ((\x y z -> x F.&: y F.&: z F.&: V.RNil) <$> pctWWCF <*> pctNonWhite <*> pctYoung) rs
            clusterRec :: F.Record '[Cluster] = SOM.classify som d F.&: V.RNil
        in districtId d F.<+> statsRec (districtData d) F.<+> clusterRec
      districtsWithStatsF = F.toFrame $ fmap distStats districtsForSOM      
--  logFrame districtsWithStatsF
  let dwsCollonnade :: BR.CellStyle (F.Record [BR.StateAbbreviation, BR.CongressionalDistrict, ET.DemPref, PctWWC, PctNonWhite, PctYoung, Cluster]) T.Text
                    -> K.Colonnade K.Headed (F.Record [BR.StateAbbreviation, BR.CongressionalDistrict, ET.DemPref, PctWWC, PctNonWhite, PctYoung, Cluster]) K.Cell
      dwsCollonnade cas =
        let x = 2
        in K.headed "State" (BR.toCell cas "State" "State" (BR.textToStyledHtml . F.rgetField @BR.StateAbbreviation))
        <> K.headed "District" (BR.toCell cas "District" "District" (BR.numberToStyledHtml "%d" . F.rgetField @BR.CongressionalDistrict))
        <> K.headed "Cluster" (BR.toCell cas "% Under 45" "% Under 45" (BR.textToStyledHtml  . T.pack . show . F.rgetField @Cluster))
        <> K.headed "% WWC" (BR.toCell cas "% WWC" "% WWC" (BR.numberToStyledHtml "%2f" . (*100) . F.rgetField @PctWWC))
        <> K.headed "% Non-White" (BR.toCell cas "% Non-White" "% Non-White" (BR.numberToStyledHtml "%2f" . (*100) . F.rgetField @PctNonWhite))
        <> K.headed "% Under 45" (BR.toCell cas "% Under 45" "% Under 45" (BR.numberToStyledHtml "%2f" . (*100) . F.rgetField @PctNonWhite))
        <> K.headed "2018 D Vote Share" (BR.toCell cas "2018 D" "2018 D" (BR.numberToStyledHtml "%2f" . (*100) . F.rgetField @ET.DemPref))
       
--  K.liftKnit $ F.writeCSV "districtStats.csv" withStatsF
  K.logLE K.Info $ "Building neighbor plot for SOM"
  let gm = SOM.toGridMap som
      bmus :: forall k p x t d gm. (Grid.Index (gm p) ~ k
                                   , Grid.Index (GridMap.BaseGrid gm p) ~ k
                                   , Grid.Index (GridMap.BaseGrid gm x) ~ k
                                   , GridMap.GridMap gm p
                                   , GridMap.GridMap gm x
                                   , Grid.Grid (gm p)
                                   , Num t
                                   , Num x
                                   , Num d
                                   , Ord x
                                   , Ord k                                   
                                   ) => (p -> p -> x) -> SOM.SOM t d gm x k p -> p -> Maybe [(k, x)]
      bmus diff som p = do
        let gm = SOM.toGridMap som
            k1 = SOM.classify som p
        n1 <- GridMap.lookup k1 gm
        let neighbors = Grid.neighbours (GridMap.toGrid gm) k1
            neighborIndexAndDiff :: k -> Maybe (k, x)
            neighborIndexAndDiff k = do
              nn <- GridMap.lookup k gm
              return $ (k, diff p nn)
        neighborDiffs <- traverse neighborIndexAndDiff neighbors
        return $ (k1, diff n1 p) : neighborDiffs
      coords :: [((Int, Int), Double)] -> (Double, Double)
      coords ns =
        let bmu = head ns
            bmuD = snd bmu
            wgt = snd
            sumWgtF = FL.premap (\(_,d) -> 1/d) FL.sum
            avgXF = (/) <$> (FL.premap (\((x,_),d) -> realToFrac x/d) FL.sum) <*> sumWgtF
            avgYF = (/) <$> (FL.premap (\((_,y),d) -> realToFrac y/d) FL.sum) <*> sumWgtF
        in case bmuD of
          0 -> (\(x, y) -> (realToFrac x, realToFrac y)) $ fst bmu          
          _ -> FL.fold ((,) <$> avgXF <*> avgYF) ns
      districtSOMCoords :: DistrictP [BR.StateAbbreviation, BR.CongressionalDistrict, ET.DemPref] (DT.CatColsASER5 V.++ '[PUMS.Citizens])
                    -> Maybe (F.Record [BR.StateAbbreviation, BR.CongressionalDistrict, ET.DemPref, '("X",Double), '("Y", Double)])
      districtSOMCoords d = do
        let dDiff = districtDiff @DT.CatColsASER5 @PUMS.Citizens meanAbsMetric
        (x, y) <- coords <$> bmus dDiff som d
        let xyRec :: F.Record ['("X", Double), '("Y", Double)] = x F.&: y F.&: V.RNil
        return $ V.rappend (districtId d) xyRec

  districtsOnMap <- F.toFrame <$> (K.knitMaybe "Error adding SOM coords to districts" $ traverse districtSOMCoords districtsForSOM)  
--  logFrame districtsOnMap
        
  curDate <-  (\(Time.UTCTime d _) -> d) <$> K.getCurrentTime
  let pubDateDistrictClusters =  Time.fromGregorian 2020 7 25
  K.newPandoc
    (K.PandocInfo ((postRoute PostDistrictClusters) <> "main")
      (brAddDates updated pubDateDistrictClusters curDate
       $ M.fromList [("pagetitle", "Looking for Flippable House Districts")
                    ,("title","Looking For Flippable House Districts")
                    ]
      ))
      $ do        
        brAddMarkDown text1
        _ <- K.addHvega Nothing Nothing
             $ clusterVL @PctWWC @PctBlack
             "2018 House Districts Clustered By %WWC and %Black"
             (FV.ViewConfig 800 800 10)
             (fmap F.rcast $ fst clusteredDistricts)
             (fmap F.rcast $ snd clusteredDistricts)
        _ <- K.addHvega Nothing Nothing
             $ somRectHeatMap
             "District SOM Heat Map"
             (FV.ViewConfig 800 800 10)
             heatMap
             districtsOnMap
        BR.brAddRawHtmlTable
          ("Districts With Stats")
          (BHA.class_ "brTable")
          (dwsCollonnade mempty)
          districtsWithStatsF
        brAddMarkDown brReadMore


somRectHeatMap :: Foldable f
               => T.Text
               -> FV.ViewConfig
               -> GridMap.LGridMap Grid.RectSquareGrid (Int, Double)
               -> f (F.Record [BR.StateAbbreviation, BR.CongressionalDistrict, ET.DemPref, '("X",Double), '("Y", Double)])
               -> GV.VegaLite
somRectHeatMap title vc gm distRows =
  let asList = List.sortOn fst $ GridMap.toList gm
      (nRows, nCols) = FL.fold ((,) <$> (fmap (fromMaybe 0) (FL.premap (fst . fst) FL.maximum)) <*> (fmap (fromMaybe 0) (FL.premap (snd . fst) FL.maximum))) asList  
      dataRows = fmap (\((r, c),(n, p)) -> GV.dataRow [("Row", GV.Number $ realToFrac r)
                                                      , ("Col", GV.Number $ realToFrac c)
                                                      , ("Num Districts", GV.Number $ realToFrac n)
                                                      , ("Avg D Margin", GV.Number (p - 0.5))
                                                      ])
                 asList
      somDat = GV.dataFromRows [] . FL.fold (FL.Fold (\f g -> f . g) id id) dataRows      
      encX = GV.position GV.X [GV.PName "Col", GV.PmType GV.Quantitative]
      encY = GV.position GV.Y [GV.PName "Row", GV.PmType GV.Quantitative]
      encColor = GV.color [GV.MName "Avg D Margin", GV.MmType GV.Quantitative]
      encSize = GV.size [GV.MName "Num Districts", GV.MmType GV.Quantitative]
      enc = (GV.encoding . encX . encY . encColor) []
      mark = GV.mark GV.Rect []
      hmSpec = GV.asSpec [enc, mark, somDat []]      
      distDat = FV.recordsToVLData id FV.defaultParse distRows
      makeLabel = GV.calculateAs "datum.state_abbreviation + \"-\" + datum.congressional_district" "District"
      makeToggle = GV.calculateAs "\"Off\"" "Labels"
      prefToShare = GV.calculateAs "datum.DemPref - 0.5" "D Vote Share"
      encDX = GV.position GV.X [GV.PName "X", GV.PmType GV.Quantitative, GV.PAxis [GV.AxValues $ GV.Numbers $  fmap realToFrac [0..nRows]]]
      encDY = GV.position GV.Y [GV.PName "Y", GV.PmType GV.Quantitative, GV.PAxis [GV.AxValues $ GV.Numbers $  fmap realToFrac [0..nCols]]]
      encDColor = GV.color [GV.MName "D Vote Share", GV.MmType GV.Quantitative, GV.MScale [GV.SScheme "redblue" []]]
      encD = (GV.encoding . encDX . encDY . encDColor) []
      markD = GV.mark GV.Circle [GV.MTooltip GV.TTData]
      selectionD = GV.selection
                . GV.select "scalesD" GV.Interval [GV.BindScales, GV.Clear "click[event.shiftKey]"]
      selectionL = GV.selection . GV.select "LabelsS" GV.Single [GV.Fields ["Labels"], GV.Bind [GV.ICheckbox "Labels" [GV.InName "District Labels"]]]
      distSpec = GV.asSpec [encD, markD, (GV.transform . prefToShare) [], selectionD [],  distDat]
      encT = GV.text [GV.TSelectionCondition (GV.SelectionName "LabelsS") [] [GV.TName "District", GV.TmType GV.Nominal]] 
      lSpec = GV.asSpec [(GV.encoding . encDX . encDY . encT) [], (GV.transform . makeLabel)  [], GV.mark GV.Text [], selectionL [] , distDat]
  in FV.configuredVegaLite vc [FV.title title, GV.layer [distSpec, lSpec]]
      

           
clusterVL :: forall x y f.
             (Foldable f
             , FV.ToVLDataValue (F.ElField x)
             , FV.ToVLDataValue (F.ElField y)
             , F.ColumnHeaders '[x]
             , F.ColumnHeaders '[y]
             )
          => T.Text
          -> FV.ViewConfig
          -> f (F.Record [BR.Year, FK.ClusterId, x, y, W])
          -> f (F.Record ([BR.Year, FK.ClusterId, FK.MarkLabel, x, y, W, ET.DemPref]))
          -> GV.VegaLite
clusterVL title vc centroidRows districtRows =
  let datCentroids = FV.recordsToVLData id FV.defaultParse centroidRows
      datDistricts = FV.recordsToVLData id FV.defaultParse districtRows
      makeShare = GV.calculateAs "datum.DemPref - 0.5" "Dem Vote Share"
      makeCentroidColor = GV.calculateAs "0" "CentroidColor"
      centroidMark = GV.mark GV.Circle [GV.MColor "grey", GV.MTooltip GV.TTEncoding]
      districtMark = GV.mark GV.Circle [ GV.MTooltip GV.TTData]
      encX = GV.position GV.X [FV.pName @x, GV.PmType GV.Quantitative]
      encY = GV.position GV.Y [FV.pName @y, GV.PmType GV.Quantitative]
      encColorD = GV.color [GV.MName "Dem Vote Share", GV.MmType GV.Quantitative, GV.MScale [GV.SScheme "redblue" []]]
      encSizeC = GV.size [FV.mName @W, GV.MmType GV.Quantitative]
      centroidSpec = GV.asSpec [(GV.encoding . encX . encY . encSizeC) [], centroidMark, (GV.transform . makeCentroidColor) [], datCentroids]
      districtSpec = GV.asSpec [(GV.encoding . encX . encY . encColorD) [], districtMark, (GV.transform . makeShare) [], datDistricts]
  in FV.configuredVegaLite  vc [FV.title title, GV.layer [centroidSpec, districtSpec]]
  


--clusterVL :: T.Text -> [(Double, Double, Double)

{-
vlRallyWWC :: Foldable f
                 => T.Text
                 -> FV.ViewConfig
                 -> f (F.Record [BR.StateAbbreviation, PollMargin, ExcessWWCPer, ExcessBasePer])
                 -> GV.VegaLite
vlRallyWWC title vc rows =
  let dat = FV.recordsToVLData id FV.defaultParse rows
      makeWWC = GV.calculateAs "datum.PollMargin * datum.ExcessWWCPer" "Excess WWC %"
      makeBase = GV.calculateAs "datum.PollMargin * datum.ExcessBasePer" "Excess Base %"
      renameMargin = GV.calculateAs "datum.PollMargin" "Poll Margin %"
      renameSA = GV.calculateAs "datum.state_abbreviation" "State"      
      doFold = GV.foldAs ["Excess WWC %", "Excess Base %"] "Type" "Pct"
      encY = GV.position GV.Y [GV.PName "Type", GV.PmType GV.Nominal, GV.PAxis [GV.AxNoTitle]
                              , GV.PSort [GV.CustomSort $ GV.Strings ["Excess WWC %", "Excess Base %"]]
                              ]
      encX = GV.position GV.X [GV.PName "Pct"
                              , GV.PmType GV.Quantitative
                              ]
      encFacet = GV.row [GV.FName "State", GV.FmType GV.Nominal]
      encColor = GV.color [GV.MName "Type"
                          , GV.MmType GV.Nominal
                          , GV.MSort [GV.CustomSort $ GV.Strings ["Excess WWC %", "Excess Base %"]]
                          ]
      enc = GV.encoding . encX . encY . encColor . encFacet
      transform = GV.transform .  makeWWC . makeBase . renameSA . renameMargin . doFold
  in FV.configuredVegaLite vc [FV.title title, enc [], transform [], GV.mark GV.Bar [], dat]
-}

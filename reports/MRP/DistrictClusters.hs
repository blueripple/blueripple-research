{-# LANGUAGE AllowAmbiguousTypes       #-}
{-# LANGUAGE ConstraintKinds #-}
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
import qualified Control.Monad.State           as State
import           Data.Discrimination            ( Grouping )
import qualified Data.List as List
import qualified Data.Map as M
import           Data.Maybe (catMaybes, fromMaybe)
import qualified Data.Text                     as T
import qualified Data.Serialize                as Serialize
import qualified Data.Set as Set
import qualified Data.Vector                   as Vec
import qualified Data.Vector.Unboxed           as UVec
import qualified Data.Vector.Storable           as SVec
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
import qualified Math.KMeans                   as MK
import qualified Math.Rescale                  as MR
import qualified Numeric.LinearAlgebra as LA

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
import qualified Data.Sparse.SpMatrix as SLA


import qualified Data.Word as Word

import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Data.Loaders as BR
import qualified BlueRipple.Data.ACS_PUMS as PUMS
import qualified BlueRipple.Data.CPSVoterPUMS as CPS
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.ElectionTypes as ET

import qualified BlueRipple.Model.MRP as BR
import qualified BlueRipple.Model.Turnout_MRP as BR
import qualified BlueRipple.Model.TSNE as BR

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
type Cluster k = "Cluster" F.:-> k
type SOM_Cluster = Cluster (Int, Int)
type K_Cluster = Cluster Int
type PCA1 = "PCA1" F.:-> Double
type PCA2 = "PCA2" F.:-> Double
type TSNE1 = "TSNE1" F.:-> Double
type TSNE2 = "TSNE2" F.:-> Double
type TSNEIters = "TSNE_iterations" F.:-> Int
type TSNEPerplexity = "TSNE_Perplexity" F.:-> Int
type TSNELearningRate = "TSNE_LearningRate" F.:-> Double

type instance FI.VectorFor (Int, Int) = K.Vector

instance FV.ToVLDataValue (F.ElField SOM_Cluster) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Str $ T.pack $ show $ V.getField x)

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


data DistrictP as rs = DistrictP { districtId :: F.Record as, isModel :: Bool, districtPop :: Int, districtVec :: UVec.Vector Double } deriving (Generic)
deriving instance Show (F.Record as) => Show (DistrictP as rs)
deriving instance Eq (F.Record as) => Eq (DistrictP as rs)
--instance S.Serialize (DistrictP as rs)

vecDemoAsRecs :: forall ks d.
                  (Ord (F.Record ks)
                  , Keyed.FiniteSet (F.Record ks)
                  , V.KnownField d
--                  , Real (V.Snd d)
                  )
              => (Int -> Double -> V.Snd d) -> Int -> UVec.Vector Double -> [F.Record (ks V.++ '[d])]
vecDemoAsRecs toD pop vecDemo =
  let keyRecs = Set.toAscList $ Keyed.elements @(F.Record ks)
      values :: [Double] = UVec.toList vecDemo
      makeD :: Double -> F.Record '[d]
      makeD x = toD pop x F.&: V.RNil
  in zipWith (\k v -> k F.<+> (makeD v)) keyRecs values 

districtAsRecs :: forall ks d as rs.
                  (Ord (F.Record ks)
                  , ks F.⊆ rs
                  , Keyed.FiniteSet (F.Record ks)
                  , F.ElemOf rs d
                  , V.KnownField d
                  , Real (V.Snd d)
                  , rs ~ (ks V.++ '[d])
                  )
               => (Int -> Double -> V.Snd d) -> DistrictP as rs -> [F.Record (as V.++ rs)]
districtAsRecs toD (DistrictP id _ pop vec) = fmap (\r -> id F.<+> r) $ vecDemoAsRecs @ks @d toD pop vec 
      

absMetric :: UVec.Vector Double -> UVec.Vector Double -> Double
absMetric xs ys =  UVec.sum $ UVec.map abs $ UVec.zipWith (-) xs ys

--meanAbsMetric :: UVec.Vector Double -> UVec.Vector Double -> Double
--meanAbsMetric xs ys =  FL.fold FL.mean  $ fmap abs $ Vec.zipWith (-) xs ys

euclideanMetric :: UVec.Vector Double -> UVec.Vector Double -> Double
euclideanMetric xs ys = UVec.sum $ UVec.map (\x -> x * x) $ UVec.zipWith (-) xs ys

districtDiff :: forall ks d as rs.(Ord (F.Record ks)
                                  , ks F.⊆ rs
                                  , F.ElemOf rs d
                                  , V.KnownField d
                                  , Real (V.Snd d)
                                  )
             => (UVec.Vector Double -> UVec.Vector Double -> Double) 
             -> DistrictP as rs
             -> DistrictP as rs
             -> Double
districtDiff metric d1 d2 = metric (districtVec d1) (districtVec d2)

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
  let f t p = p * (1 - x) + t * x
      newPatVec = UVec.zipWith f (districtVec tgtD) (districtVec patD)
      newPatPop = round $ f (realToFrac $ districtPop tgtD) (realToFrac $ districtPop patD) 
  in DistrictP (districtId patD) True newPatPop newPatVec

buildSOM :: forall ks d as r.
  (K.KnitEffects r
  , K.Member PRF.RandomFu r
  , ks F.⊆ (ks V.++ '[d])
  , V.KnownField d
  , F.ElemOf (ks V.++ '[d]) d
  , Real (V.Snd d)
  , Ord (F.Record ks)
  )
  => (UVec.Vector Double -> UVec.Vector Double -> Double) -- ^ metric for SOM
  -> (Int, Int) -- rectangular grid sizes
  -> [DistrictP as (ks V.++ '[d])]
  -> K.Sem r (SOM.SOM Double Double (GridMap.LGridMap Grid.RectSquareGrid) Double (Int, Int) (DistrictP as (ks V.++ '[d])))
buildSOM metric (gridRows, gridCols) sampleFrom = do
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
      dDiff = districtDiff @ks @d metric
      dMakeSimilar = districtMakeSimilar @ks @d (fromIntegral . round)
  return $ SOM.SOM gm lrf dDiff dMakeSimilar 0


-- NB: Weight is chosen so that for exactly the same and equidistributed,
-- tr (M'M) = (numPats * numPats)/numClusters
sameClusterMatrixSOM :: forall c v k p dk. (SOM.Classifier c v k p, Ord dk, Eq k, Ord v) => (p -> dk) -> c v k p -> [p] -> SLA.SpMatrix Int
sameClusterMatrixSOM getKey som ps = sameClusterMatrix getKey (SOM.classify som) ps

sameClusterMatrix :: forall k p dk. (Ord dk, Eq k) => (p -> dk) -> (p -> k) -> [p] -> SLA.SpMatrix Int
sameClusterMatrix getKey cluster ps =
  let numPats = length ps
      getCluster :: p -> State.State (M.Map dk k) k
      getCluster p = do
        let k = getKey p
        m <- State.get
        case M.lookup k m of
          Just c -> return c
          Nothing -> do
            let c = cluster p
            State.put (M.insert k c m)
            return c      
      go :: (Int, [p]) -> State.State (M.Map dk k) [(Int, Int, Int)]
      go (n, ps) = do
        ks <- traverse getCluster ps
        let f k1 (k2, m) = if k1 == k2 then Just (n, m, 1) else Nothing
            swapIndices (a, b, h) = (b, a, h) 
        case ks of
          [] -> return []
          (kh : kt) -> do
            let upper = catMaybes (fmap (f kh) $ zip kt [(n+1)..])
                lower = fmap swapIndices upper
            return $ (n,n,1) : (upper ++ lower)
      scM = List.concat <$> (traverse go (zip [0..] $ List.tails ps))
  in SLA.fromListSM (numPats, numPats) $ State.evalState scM M.empty


randomSCM :: K.Member GLM.RandomFu r => Int -> Int -> K.Sem r (SLA.SpMatrix Int)
randomSCM nPats nClusters = do
  clustered <- traverse (const $ PRF.sampleRVar (RandomFu.uniform 1 nClusters)) [1..nPats]
  let go :: (Int, [Int]) -> [(Int, Int, Int)]
      go (n, ks) =
        let f k1 (k2, m) = if k1 == k2 then Just (n, m, 1) else Nothing
            swapIndices (a, b, h) = (b, a, h)
        in case ks of
          [] -> []
          (kh : kt) ->
            let upper = catMaybes (fmap (f kh) $ zip kt [(n+1)..])
                lower = fmap swapIndices upper
            in (n,n,1) : (upper ++ lower)
      scM = List.concat $ fmap go $ zip [0..] $ List.tails clustered
  return $ SLA.fromListSM (nPats, nPats) scM
  
post :: forall r.(K.KnitMany r, K.CacheEffectsD r, K.Member GLM.RandomFu r) => Bool -> K.Sem r ()
post updated = P.mapError BR.glmErrorToPandocError $ K.wrapPrefix "DistrictClustering" $ do

  let clusterRowsToS (cs, rs) = (fmap FS.toS cs, fmap FS.toS rs)
      clusterRowsFromS (cs', rs') = (fmap FS.fromS cs', fmap FS.fromS rs')

  pums2018ByCD_C <- do
    demo_C <- PUMS.pumsLoader
    BR.retrieveOrMakeFrame "mrp/DistrictClusters/pums2018ByCD.bin" demo_C $ \pumsRaw -> do
      let pums2018Raw = F.filterFrame ((== 2018) . F.rgetField @BR.Year) pumsRaw
      pumsCDRollup <- PUMS.pumsCDRollup (PUMS.pumsKeysToASER5 True . F.rcast) pums2018Raw
      return $ FL.fold addPUMSZerosF pumsCDRollup

  houseVoteShare_C <- do
    houseResults_C <- BR.houseElectionsLoader
    BR.retrieveOrMakeFrame "mrp/DistrictClusters/houseVoteShare.bin" houseResults_C $ \houseResultsRaw -> do
      let filterHR r = F.rgetField @BR.Year r `elem` [2016, 2018]
                       && F.rgetField @BR.Stage r == "gen"
                       && F.rgetField @BR.Runoff r == False
                       && F.rgetField @BR.Special r == False
                       && (F.rgetField @ET.Party r == ET.Democratic || F.rgetField @ET.Party r == ET.Republican)
          houseResults = fmap (F.rcast @[BR.Year, BR.StateAbbreviation, BR.CongressionalDistrict, ET.Party, ET.Votes, ET.TotalVotes])
                         $ F.filterFrame filterHR houseResultsRaw
          houseResultsF = FMR.concatFold $ FMR.mapReduceFold
                              FMR.noUnpack
                              (FMR.assignKeysAndData @[BR.Year, BR.StateAbbreviation, BR.CongressionalDistrict])
                              (FMR.foldAndAddKey votesToVoteShareF)
      return $ FL.fold houseResultsF houseResults

  pumsByCDWithVoteShare_C <- do
    let cachedDeps = (,) <$> pums2018ByCD_C <*> houseVoteShare_C
    BR.retrieveOrMakeFrame "mrp/DistrictClusters/pumsByCDWithVoteShare.bin" cachedDeps $ \(pumsByCD, houseVoteShare) -> do
      K.logLE K.Info $ "Joining demographics with vote share data"
      let (pumsByCDWithVoteShare, missing) = FJ.leftJoinWithMissing  @([BR.Year, BR.StateAbbreviation, BR.CongressionalDistrict])
                                             pumsByCD
                                             houseVoteShare
      K.logLE K.Info $ "Districts with missing house results (dropped from analysis):" <> (T.pack $ show $ List.nub $ missing)
      return pumsByCDWithVoteShare

  K.logLE K.Info $ "Building clustering input"
  districtsForClustering :: [DistrictP [BR.StateAbbreviation, BR.CongressionalDistrict, ET.DemPref, PUMS.Citizens] (DT.CatColsASER5 V.++ '[PUMS.Citizens])] <- do
    pumsByCDWithVoteShare <- K.ignoreCacheTime pumsByCDWithVoteShare_C
    let asVec rs = UVec.fromList
                   $ fmap snd
                   $ M.toAscList
                   $ FL.fold (FL.premap (\r-> (F.rcast @DT.CatColsASER5 r, realToFrac $ F.rgetField @PUMS.Citizens r)) FL.map) rs
        pop rs = FL.fold (FL.premap (F.rgetField @PUMS.Citizens) FL.sum) rs
        popRec :: Int -> F.Record '[PUMS.Citizens] = \n -> n F.&: V.RNil
    return
      $ FL.fold
      (FMR.mapReduceFold
       FMR.noUnpack
       (FMR.assignKeysAndData @[BR.StateAbbreviation, BR.CongressionalDistrict, ET.DemPref] @(DT.CatColsASER5 V.++ '[PUMS.Citizens]))
       (FMR.ReduceFold $ \k -> fmap (\rs -> DistrictP (k F.<+> popRec (pop rs)) False (pop rs) (asVec rs)) FL.list)
      )
      pumsByCDWithVoteShare
  K.logLE K.Info $ "After joining with 2018 voteshare info, working with " <> (T.pack $ show $ length districtsForClustering) <> " districts."
      
  let districtsForClustering_C = fmap (const districtsForClustering) pumsByCDWithVoteShare_C
  K.logLE K.Info $ "Computing top 2 PCA components"
  (pca1v, pca2v) <- do
    let asSVecs = fmap (SVec.convert . districtVec) districtsForClustering
        asMat = LA.fromRows asSVecs
        mTm = (LA.tr asMat) LA.<> asMat
        (eigVals, eigVecs) = LA.eigSH (LA.trustSym mTm)
        pc1Vec = SVec.convert $ (LA.toColumns eigVecs) List.!! 0
        pc2Vec = SVec.convert $ (LA.toColumns eigVecs) List.!! 1
    K.logLE K.Diagnostic $ "Eigenvalues: " <> (T.pack $ show eigVals)
    K.logLE K.Diagnostic $ "PC 1:" <> (T.pack $ show (vecDemoAsRecs @DT.CatColsASER5 @PUMS.Citizens (\pop pct -> round $ pct * realToFrac pop) 100 $ pc1Vec))
    K.logLE K.Diagnostic $ "PC 2:" <> (T.pack $ show (vecDemoAsRecs @DT.CatColsASER5 @PUMS.Citizens (\pop pct -> round $ pct * realToFrac pop) 100 $ pc2Vec))
    return (pc1Vec, pc2Vec)

  let numComps = 3
      pairsWithFirst l = case l of
        [] -> []
        (x : []) -> []
        (x : xs) -> fmap (x,) xs
      allDiffPairs = List.concat . fmap pairsWithFirst . List.tails

  let kClusterDistricts = do
        K.logLE K.Info $ "Computing K-means"
        let labelCD r = F.rgetField @BR.StateAbbreviation r <> "-" <> (T.pack $ show $ F.rgetField @BR.CongressionalDistrict r)            
            distWeighted :: MK.Weighted (DistrictP [BR.StateAbbreviation, BR.CongressionalDistrict, ET.DemPref, PUMS.Citizens] (DT.CatColsASER5 V.++ '[PUMS.Citizens])) Double
            distWeighted = MK.Weighted
                           40
                           districtVec
                           (realToFrac . districtPop)
            metric = euclideanMetric
        initialCentroids <- PMR.absorbMonadRandom $ MK.kMeansPPCentroids metric 10 (fmap districtVec districtsForClustering)
        (MK.Clusters kmClusters, iters) <- MK.weightedKMeans (MK.Centroids $ Vec.fromList initialCentroids) distWeighted metric districtsForClustering
        let distRec :: DistrictP [BR.StateAbbreviation, BR.CongressionalDistrict, ET.DemPref, PUMS.Citizens] (DT.CatColsASER5 V.++ '[PUMS.Citizens])
                    -> F.Record [BR.StateAbbreviation, BR.CongressionalDistrict, ET.DemPref, PUMS.Citizens, W, PCA1, PCA2]
            distRec d =
              let pca1 = UVec.sum $ UVec.zipWith (*) pca1v (districtVec d)
                  pca2 = UVec.sum $ UVec.zipWith (*) pca2v (districtVec d)
                  w = districtPop d
              in (districtId d) F.<+> (realToFrac w F.&: pca1 F.&: pca2 F.&: V.RNil)
            processOne c = do
              (centroidUVec, cW) <- MK.centroid distWeighted (MK.members c)
              let cPCA1 = UVec.sum $ UVec.zipWith (*) pca1v centroidUVec
                  cPCA2 = UVec.sum $ UVec.zipWith (*) pca2v centroidUVec
                  withProjs = fmap distRec (MK.members c)
              return $ ((cPCA1, cPCA2, cW), withProjs)
        rawClusters <- K.knitMaybe "Empty Cluster in K_means." $ traverse processOne  (Vec.toList kmClusters) 
        return $ FK.clusteredRowsFull @PCA1 @PCA2 @W labelCD $ M.fromList [((2018 F.&: V.RNil) :: F.Record '[BR.Year], rawClusters)]
  
  kClusteredDistricts <- kClusterDistricts
  K.logLE K.Info $ "K-means diagnostics"
  let kClustered = snd kClusteredDistricts
      numKDists = length kClustered 
      kSCM = sameClusterMatrix (F.rcast @'[FK.MarkLabel]) (F.rcast @'[FK.ClusterId]) kClustered
      kEffClusters = realToFrac (numKDists * numKDists)/ (realToFrac $ SLA.trace $ SLA.matMat_ SLA.ABt kSCM kSCM)
      kCompFactor = kEffClusters / realToFrac (numKDists * numKDists)
  K.logLE K.Info $ "Effective clusters=" <> (T.pack $ show kEffClusters)  
  K.logLE K.Info $ "Making " <> (T.pack $ show numComps) <> " other k-means clusterings for comparison..."
  kComps <- fmap snd <$> traverse (const kClusterDistricts) [1..numComps]
  let kSCMs = fmap  (sameClusterMatrix (F.rcast @'[FK.MarkLabel]) (F.rcast @'[FK.ClusterId])) kComps
      allKSCMPairs = allDiffPairs kSCMs
      allDiffKTraces = fmap (\(scm1, scm2) -> kCompFactor * (realToFrac $ SLA.trace $ SLA.matMat_ SLA.ABt scm1 scm2)) allKSCMPairs
      (meanKC, stdKC) = FL.fold ((,) <$> FL.mean <*> FL.std) allDiffKTraces
  K.logLE K.Info $ "K-meansL <comp> = " <> (T.pack $ show meanKC) <> "; sigma(comp)=" <> (T.pack $ show stdKC)

-- tSNE
  K.logLE K.Info $ "tSNE embedding..."
  let tsneIters = [1000]
      tsnePerplexities = [40]
      tsneLearningRates = [10]
--  K.clearIfPresent "mrp/DistrictClusters/tsne.bin"
  (tSNE_ClustersF, tSNE_ClusteredF) <- K.ignoreCacheTimeM
            $ BR.retrieveOrMake2Frames "mrp/DistrictClusters/tsne.bin" districtsForClustering_C $ \dfc -> do
    K.logLE K.Info $ "Running tSNE gradient descent for " <> (T.pack $ show tsneIters) <> " iterations."    
    tsneMs <- BR.runTSNE
              (Just 1)
              districtId
              (UVec.toList . districtVec)
              tsnePerplexities
              tsneLearningRates
              tsneIters
              (\x -> (BR.tsneIteration2D_M x, BR.tsneCost2D_M x, BR.solutionToList $ BR.tsneSolution2D_M x))
              BR.tsne2D_S
              dfc
           
    let tSNERec :: BR.TSNEParams -> (Double, Double) -> F.Record [TSNEPerplexity, TSNELearningRate, TSNEIters, TSNE1, TSNE2] 
        tSNERec (BR.TSNEParams p lr n) (x, y) = p F.&: lr F.&: n F.&: x F.&: y F.&: V.RNil
        tSNERecs p = fmap (\(k, tSNEXY) -> k F.<+> tSNERec p tSNEXY) . M.toList
        fullTSNEResult =  mconcat $ fmap (\(p, solM) -> F.toFrame $ tSNERecs p solM) $ tsneMs

    -- do k-means clustering on TSNE
        labelCD r = F.rgetField @BR.StateAbbreviation r <> "-" <> (T.pack $ show $ F.rgetField @BR.CongressionalDistrict r)
        getVec r = UVec.fromList $ [F.rgetField @TSNE1 r, F.rgetField @TSNE2 r]
        distWeighted :: MK.Weighted
                        (F.Record [BR.StateAbbreviation, BR.CongressionalDistrict, ET.DemPref, PUMS.Citizens, TSNEPerplexity, TSNELearningRate, TSNEIters, TSNE1, TSNE2]) Double
        distWeighted = MK.Weighted
                       2
                       getVec
                       (realToFrac . F.rgetField @PUMS.Citizens)
        metric = euclideanMetric
    initialCentroids <- PMR.absorbMonadRandom $ MK.kMeansPPCentroids metric 10 (fmap getVec $ FL.fold FL.list fullTSNEResult)
    (MK.Clusters kmClusters, iters) <- MK.weightedKMeans (MK.Centroids $ Vec.fromList initialCentroids) distWeighted metric fullTSNEResult
    let processOne c = do
          (centroidUVec, cW) <- MK.centroid distWeighted (MK.members c)
          let wRec :: Double -> F.Record '[W] = \x -> x F.&: V.RNil
              addW r = r F.<+> wRec (realToFrac $ F.rgetField @PUMS.Citizens r)
              withWeights = fmap addW $ MK.members c
          return ((centroidUVec UVec.! 0, centroidUVec UVec.! 1, cW), withWeights)
    rawClusters <- K.knitMaybe "Empty Cluster in tSNE K-means." $ traverse processOne (Vec.toList kmClusters)
    let (clustersL, clusteredDistrictsL)
          = FK.clusteredRowsFull @TSNE1 @TSNE2 @W labelCD $ M.fromList [((2018 F.&: V.RNil) :: F.Record '[BR.Year], rawClusters)]
        
    return (F.toFrame clustersL, F.toFrame clusteredDistrictsL)
  logFrame tSNE_ClustersF
-- SOM      
  K.logLE K.Info $ "SOM clustering..."
  let gridRows = 3
      gridCols = 3
      numDists = length districtsForClustering
      metric = euclideanMetric
      somResultsKey = "mrp/DistrictClusters/SOM_Results.bin"
  K.clearIfPresent somResultsKey
  (districtsWithStatsF, districtsOnMap) <- K.ignoreCacheTimeM $ do
    retrieveOrMake2Frames somResultsKey districtsForClustering_C $ \dfc -> do
      randomOrderDistricts <- PRF.sampleRVar $ RandomFu.shuffle dfc
      som0 <- buildSOM @DT.CatColsASER5 @PUMS.Citizens metric (gridRows, gridCols) dfc
      let som = SOM.trainBatch som0 randomOrderDistricts  
      K.logLE K.Info $ "SOM Diagnostics"

      let scm = sameClusterMatrixSOM districtId som districtsForClustering
          effClusters = realToFrac (numDists * numDists)/ (realToFrac $ SLA.trace $ SLA.matMat_ SLA.ABt scm scm)
      K.logLE K.Info $ "Effective clusters=" <> (T.pack $ show effClusters)  
      let compFactor = effClusters / realToFrac (numDists * numDists)
      K.logLE K.Info $ "Making " <> (T.pack $ show numComps) <> " different SOMs"
      som0s <- traverse (const $ buildSOM @DT.CatColsASER5 @PUMS.Citizens metric (gridRows, gridCols) districtsForClustering) [1..numComps]
      let soms = fmap (\s -> SOM.trainBatch s randomOrderDistricts) som0s
          scms = fmap (\s -> sameClusterMatrixSOM districtId s districtsForClustering) soms

          allDiffSCMPairs = allDiffPairs scms
          allDiffTraces = fmap (\(scm1, scm2) -> compFactor * (realToFrac $ SLA.trace $ SLA.matMat_ SLA.ABt scm1 scm2)) allDiffSCMPairs
          (meanC, stdC) = FL.fold ((,) <$> FL.mean <*> FL.std) allDiffTraces
      K.logLE K.Info $ "<comp> = " <> (T.pack $ show meanC) <> "; sigma(comp)=" <> (T.pack $ show stdC)

      K.logLE K.Info $ "Generating random ones for comparison"
      randomSCMs <- traverse (const $ randomSCM numDists (gridRows * gridCols)) [1..numComps]
      let allRandomTraces = fmap (\scm ->  SLA.trace $ SLA.matMat_ SLA.ABt scm scm) randomSCMs
      let allSameEffClusters = fmap (\scm ->  realToFrac (numDists * numDists)/ (realToFrac $ SLA.trace $ SLA.matMat_ SLA.ABt scm scm)) randomSCMs
          (meanRCs, stdRCs) = FL.fold ((,) <$> FL.mean <*> FL.std) allSameEffClusters
      K.logLE K.Info $ "Random SCMs: <eff Clusters> = " <> (T.pack $ show meanRCs) <> "; sigma(eff Clusters)=" <> (T.pack $ show stdRCs)      
      let allDiffPairsRandom = allDiffPairs randomSCMs
          randomCompFactor = meanRCs / realToFrac (numDists * numDists)
          allRandomDiffTraces = fmap (\(scm1, scm2) -> randomCompFactor * (realToFrac $ SLA.trace $ SLA.matMat_ SLA.ABt scm1 scm2)) allDiffPairsRandom
          (meanRS, stdRS) = FL.fold ((,) <$> FL.mean <*> FL.std) allRandomDiffTraces
      K.logLE K.Info $ "Random SCs: <comp> = " <> (T.pack $ show meanRS) <> "; sigma(comp)=" <> (T.pack $ show stdRS)

      K.logLE K.Info $ "Building SOM heatmap of DemPref"    
      let districtPref = F.rgetField @ET.DemPref . districtId
          heatMap0 :: GridMap.LGridMap Grid.RectSquareGrid (Int, Double)
          heatMap0  = GridMap.lazyGridMap (Grid.rectSquareGrid gridRows gridCols) $ replicate (gridRows * gridCols) (0, 0)
          adjustOne pref (districts, avgPref)  = (districts + 1, ((realToFrac districts * avgPref) + pref)/(realToFrac $ districts + 1))
          heatMap = FL.fold (FL.Fold (\gm d -> let pref = districtPref d in GridMap.adjust (adjustOne pref) (SOM.classify som d) gm) heatMap0 id) districtsForClustering
      K.logLE K.Info $ "Building cluster table"
      let distStats :: DistrictP [BR.StateAbbreviation, BR.CongressionalDistrict, ET.DemPref, PUMS.Citizens] (DT.CatColsASER5 V.++ '[PUMS.Citizens])
                    -> F.Record [BR.StateAbbreviation, BR.CongressionalDistrict, ET.DemPref, PUMS.Citizens, PctWWC, PctNonWhite, PctYoung, SOM_Cluster]
          distStats d =
            let vac = F.rgetField @PUMS.Citizens
                vacF = fmap realToFrac $ FL.premap vac FL.sum
                pctWWCF = (/) <$> FL.prefilter isWWC vacF <*> vacF
                pctNonWhite = (/) <$> FL.prefilter isNonWhite vacF <*> vacF
                pctYoung = (/) <$> FL.prefilter isYoung vacF <*> vacF
                statsRec :: [F.Record (DT.CatColsASER5 V.++ '[PUMS.Citizens])] -> F.Record [PctWWC, PctNonWhite, PctYoung]
                statsRec rs = FL.fold ((\x y z -> x F.&: y F.&: z F.&: V.RNil) <$> pctWWCF <*> pctNonWhite <*> pctYoung) rs
                clusterRec :: F.Record '[SOM_Cluster] = SOM.classify som d F.&: V.RNil
            in districtId d F.<+> statsRec (fmap F.rcast $ districtAsRecs @DT.CatColsASER5 @PUMS.Citizens (\pop pct -> round $ pct * realToFrac pop) d) F.<+> clusterRec
      districtsWithStatsF :: F.FrameRec DWSRow <- do
        houseVoteShare <- K.ignoreCacheTime houseVoteShare_C    
        let voteShare2016 = fmap (FT.replaceColumn @ET.DemPref @VoteShare2016 id . F.rcast @[BR.StateAbbreviation, BR.CongressionalDistrict, ET.DemPref])
                            $ F.filterFrame ((==2016) . F.rgetField @BR.Year) houseVoteShare
            (districtsWithStats, missing2016Share)  = FJ.leftJoinWithMissing @[BR.StateAbbreviation, BR.CongressionalDistrict]
                                                      (F.toFrame $ fmap distStats districtsForClustering)
                                                      voteShare2016
        K.logLE K.Info $ "Districts with missing 2016 house results (dropped from analysis):" <> (T.pack $ show $ List.nub $ missing2016Share)
        return $ F.toFrame districtsWithStats
-- logFrame districtsWithStatsF
      
  
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
          districtSOMCoords :: DistrictP [BR.StateAbbreviation, BR.CongressionalDistrict, ET.DemPref, PUMS.Citizens] (DT.CatColsASER5 V.++ '[PUMS.Citizens])
                            -> Maybe (F.Record [BR.StateAbbreviation, BR.CongressionalDistrict, ET.DemPref, PUMS.Citizens, SOM_Cluster, '("X",Double), '("Y", Double)])
          districtSOMCoords d = do
            let dDiff = districtDiff @DT.CatColsASER5 @PUMS.Citizens metric        
            (c, (x, y)) <- do
              districtBMUs <- bmus dDiff som d
              let (dX, dY) =  coords districtBMUs
                  dC = fst $ head districtBMUs
              return (dC, (dX, dY))
            let cxyRec :: F.Record [SOM_Cluster, '("X", Double), '("Y", Double)] = c F.&: x F.&: y F.&: V.RNil
            return $ V.rappend (districtId d) cxyRec

      districtsOnMap <- F.toFrame <$> (K.knitMaybe "Error adding SOM coords to districts" $ traverse districtSOMCoords districtsForClustering)
      return (districtsWithStatsF, districtsOnMap)
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
             $ clusterVL @PCA1 @PCA2
             "2018 House Districts: K-Means"
             (FV.ViewConfig 800 800 10)
             (fmap F.rcast $ fst kClusteredDistricts)
             (fmap F.rcast $ snd kClusteredDistricts)
        _ <- K.addHvega Nothing Nothing
             $ kMeansBoxes @PCA1 @PCA2
             "2018 House Districts: K-Means"
             (FV.ViewConfig 800 800 10)
             (fmap F.rcast $ fst kClusteredDistricts)
             (fmap F.rcast $ snd kClusteredDistricts)
        _ <- K.addHvega Nothing Nothing
             $ tsneVL
             "tSNE Embedding"
             (FV.ViewConfig 800 800 10)
             (fmap F.rcast tSNE_ClusteredF)
        _ <- K.addHvega Nothing Nothing
             $ somRectHeatMap
             "District SOM Heat Map"
             (FV.ViewConfig 800 800 10)
--             heatMap
             (fmap F.rcast districtsOnMap)
        _ <- K.addHvega Nothing Nothing
             $ somBoxes
             "District SOM Box & Whisker"
             (FV.ViewConfig 800 800 10)
             (fmap F.rcast districtsOnMap)
        BR.brAddRawHtmlTable
          ("Districts With Stats")
          (BHA.class_ "brTable")
          (dwsCollonnade mempty)
          districtsWithStatsF
        brAddMarkDown brReadMore


tsneVL ::  Foldable f
       => T.Text
       -> FV.ViewConfig
       -> f (F.Record [BR.StateAbbreviation, BR.CongressionalDistrict, ET.DemPref, TSNEPerplexity, TSNELearningRate, TSNEIters, TSNE1, TSNE2])
       -> GV.VegaLite
tsneVL title vc rows =
  let dat = FV.recordsToVLData id FV.defaultParse rows
      listF getField = fmap List.nub $ FL.premap getField FL.list
      (perpL, lrL, iterL) = FL.fold ((,,)
                            <$> listF (F.rgetField @TSNEPerplexity)
                            <*> listF (F.rgetField @TSNELearningRate)
                            <*> listF (F.rgetField @TSNEIters)) rows
      encX = GV.position GV.X [FV.pName @TSNE1, GV.PmType GV.Quantitative]
      encY = GV.position GV.Y [FV.pName @TSNE2, GV.PmType GV.Quantitative]
      encColor = GV.color [GV.MName "Dem Vote Share", GV.MmType GV.Quantitative, GV.MScale [GV.SScheme "redblue" []]]
      encCol = GV.column [FV.fName @TSNEIters, GV.FmType GV.Ordinal]
      encRow = GV.row [FV.fName @TSNEPerplexity, GV.FmType GV.Ordinal]
      makeShare = GV.calculateAs "datum.DemPref - 0.5" "Dem Vote Share"
      makeDistrict = GV.calculateAs "datum.state_abbreviation + \"-\" + datum.congressional_district" "District"
      mark = GV.mark GV.Circle [GV.MTooltip GV.TTData]
      bindScales =  GV.select "scalesD" GV.Interval [GV.BindScales, GV.Clear "click[event.shiftKey]"]
{-
      bindPerplexity = GV.select "sPerplexity" GV.Single [GV.Fields ["TSNE_Perplexity"]
                                                         , GV.Bind [ GV.ISelect "TSNE_Perplexity" [GV.InName "",GV.InOptions $ fmap (T.pack . show) perpL]]]
      bindLearningRate = GV.select "sLearningRate" GV.Single [GV.Fields ["TSNE_LearningRate"]
                                                            , GV.Bind [ GV.ISelect "TSNE_LearningRate" [GV.InName "",GV.InOptions $ fmap (T.pack . show) lrL]]]
      bindIters = GV.select "sIters" GV.Single [GV.Fields ["TSNE_iterations"]
                                              , GV.Bind [ GV.ISelect "TSNE_iterations" [GV.InName "",GV.InOptions $ fmap (T.pack . show) iterL]]]
      filterSelections = GV.filter (GV.FSelection "sPerplexity") . GV.filter (GV.FSelection "sLearningRate") . GV.filter (GV.FSelection "sIters")
-}
      enc = (GV.encoding . encX . encY . encRow . encColor) []
      transform = (GV.transform . makeShare . makeDistrict) []
      selection = (GV.selection . bindScales) []
      resolve = (GV.resolve . GV.resolution (GV.RScale [ (GV.ChY, GV.Independent), (GV.ChX, GV.Independent)])) []
  in FV.configuredVegaLite vc [FV.title title, enc, mark, transform, selection, resolve, dat]
  
somRectHeatMap :: (Foldable f, Functor f)
               => T.Text
               -> FV.ViewConfig
               -> f (F.Record [BR.StateAbbreviation, BR.CongressionalDistrict, ET.DemPref, SOM_Cluster, '("X",Double), '("Y", Double)])
               -> GV.VegaLite
somRectHeatMap title vc distRows =
  let (nRows, nCols) = FL.fold ((,)
                                 <$> (fmap (fromMaybe 0) (FL.premap (fst . F.rgetField @SOM_Cluster) FL.maximum))
                                 <*> (fmap (fromMaybe 0) (FL.premap (snd . F.rgetField @SOM_Cluster) FL.maximum))) distRows
      distDat = FV.recordsToVLData id FV.defaultParse
                $ fmap (F.rcast @[BR.StateAbbreviation, BR.CongressionalDistrict, ET.DemPref, '("X",Double), '("Y", Double)]) distRows
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
      

somBoxes ::  Foldable f
         => T.Text
         -> FV.ViewConfig
         -> f (F.Record [BR.StateAbbreviation, BR.CongressionalDistrict, ET.DemPref, SOM_Cluster])
         -> GV.VegaLite
somBoxes title vc rows =
  let somDat = FV.recordsToVLData id FV.defaultParse rows
      makeShare = GV.calculateAs "datum.DemPref - 0.5" "Dem Vote Share"
      makeLabel = GV.calculateAs "datum.state_abbreviation + \"-\" + datum.congressional_district" "District"
      boxPlotMark = GV.mark GV.Boxplot [GV.MExtent $ GV.IqrScale 1.0, GV.MNoOutliers]
      encX = GV.position GV.X [GV.PName "Dem Vote Share", GV.PmType GV.Quantitative]
      encY = GV.position GV.Y [FV.pName @SOM_Cluster, GV.PmType GV.Ordinal]
      encColor = GV.color [GV.MName "Dem Vote Share", GV.MmType GV.Quantitative,  GV.MScale [GV.SScheme "redblue" []]]
      encTooltip = GV.tooltip [GV.TName "District", GV.TmType GV.Nominal]
      boxSpec = GV.asSpec [(GV.encoding . encX . encY) []
                          , boxPlotMark
                          , (GV.transform . makeShare . makeLabel) []
                          ]
      pointsSpec = GV.asSpec [(GV.encoding . encX . encY . encColor . encTooltip) []
                             , GV.mark GV.Circle []
                             ,(GV.transform . makeShare . makeLabel) []
                             ]
  in FV.configuredVegaLite vc [FV.title title
                              , GV.layer [boxSpec, pointsSpec]
                              , somDat]


           
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
      centroidMark = GV.mark GV.Point [GV.MColor "grey", GV.MTooltip GV.TTEncoding]
      districtMark = GV.mark GV.Point [ GV.MTooltip GV.TTData]
      encShape = GV.shape [FV.mName @FK.ClusterId, GV.MmType GV.Nominal]
      encX = GV.position GV.X [FV.pName @x, GV.PmType GV.Quantitative]
      encY = GV.position GV.Y [FV.pName @y, GV.PmType GV.Quantitative]
      encColorD = GV.color [GV.MName "Dem Vote Share", GV.MmType GV.Quantitative, GV.MScale [GV.SScheme "redblue" []]]
      encSizeC = GV.size [FV.mName @W, GV.MmType GV.Quantitative]
      selectionD = GV.selection
                   . GV.select "scalesD" GV.Interval [GV.BindScales, GV.Clear "click[event.shiftKey]"]
      centroidSpec = GV.asSpec [(GV.encoding . encX . encY . encSizeC . encShape) [], centroidMark, (GV.transform . makeCentroidColor) [], datCentroids]
      districtSpec = GV.asSpec [(GV.encoding . encX . encY . encColorD . encShape) [], districtMark, (GV.transform . makeShare) [], selectionD [], datDistricts]
  in FV.configuredVegaLite  vc [FV.title title, GV.layer [centroidSpec, districtSpec]]
  

kMeansBoxes :: forall x y f.
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
kMeansBoxes title vc centroidRows districtRows =
  let datCentroids = FV.recordsToVLData id FV.defaultParse centroidRows
      datDistricts = FV.recordsToVLData id FV.defaultParse districtRows
      makeShare = GV.calculateAs "datum.DemPref - 0.5" "Dem Vote Share"
      boxPlotMark = GV.mark GV.Boxplot [GV.MExtent $ GV.IqrScale 1.0]
      encX = GV.position GV.X [GV.PName "Dem Vote Share", GV.PmType GV.Quantitative]
      encY = GV.position GV.Y [FV.pName @FK.ClusterId, GV.PmType GV.Ordinal]
      encColor = GV.color [GV.MName "Dem Vote Share", GV.MmType GV.Quantitative]
      encTooltip = GV.tooltip [GV.TName "mark_label", GV.TmType GV.Nominal]
  in FV.configuredVegaLite vc [FV.title title, (GV.encoding . encX . encY . encTooltip) [], boxPlotMark, (GV.transform . makeShare) [], datDistricts]

type DWSRow = [BR.StateAbbreviation
              , BR.CongressionalDistrict
              , ET.DemPref
              , PUMS.Citizens
              , PctWWC
              , PctNonWhite
              , PctYoung
              , SOM_Cluster
              , VoteShare2016]

dwsCollonnade :: BR.CellStyle (F.Record DWSRow) T.Text -> K.Colonnade K.Headed (F.Record DWSRow) K.Cell
dwsCollonnade cas =
  let x = 2
  in K.headed "State" (BR.toCell cas "State" "State" (BR.textToStyledHtml . F.rgetField @BR.StateAbbreviation))
     <> K.headed "District" (BR.toCell cas "District" "District" (BR.numberToStyledHtml "%d" . F.rgetField @BR.CongressionalDistrict))
     <> K.headed "Cluster" (BR.toCell cas "% Under 45" "% Under 45" (BR.textToStyledHtml  . T.pack . show . F.rgetField @SOM_Cluster))
     <> K.headed "% WWC" (BR.toCell cas "% WWC" "% WWC" (BR.numberToStyledHtml "%.1f" . (*100) . F.rgetField @PctWWC))
     <> K.headed "% Non-White" (BR.toCell cas "% Non-White" "% Non-White" (BR.numberToStyledHtml "%.1f" . (*100) . F.rgetField @PctNonWhite))
     <> K.headed "% Under 45" (BR.toCell cas "% Under 45" "% Under 45" (BR.numberToStyledHtml "%.1f" . (*100) . F.rgetField @PctNonWhite))
     <> K.headed "2018 D Vote Share" (BR.toCell cas "2018 D" "2018 D" (BR.numberToStyledHtml "%.1f" . (*100) . F.rgetField @ET.DemPref))
     <> K.headed "2016 D Vote Share" (BR.toCell cas "2016 D" "2016 D" (BR.numberToStyledHtml "%.1f" . (*100) . F.rgetField @VoteShare2016))
       

{-
-- SGM      
  let dDiff = districtDiff @DT.CatColsASER5 @PUMS.Citizens meanAbsMetric
      dMakeSimilar = districtMakeSimilar @DT.CatColsASER5 @PUMS.Citizens @[BR.StateAbbreviation, BR.CongressionalDistrict, ET.DemPref] (fromIntegral . round)
      sgm0 :: SGM.SGM Int Double Word (DistrictP [BR.StateAbbreviation, BR.CongressionalDistrict, ET.DemPref] (DT.CatColsASER5 V.++ '[PUMS.Citizens]))
      sgm0 = SGM.makeSGM (SGM.exponential 0.1 0.5) 20 0.1 True dDiff dMakeSimilar 
      sgm = SGM.trainBatch sgm0 districtsForClustering
  K.logLE K.Info $ "SGM model map:" <> (T.pack $ show $ fmap districtId $ SGM.modelMap sgm)
-}

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

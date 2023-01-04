{-# LANGUAGE AllowAmbiguousTypes       #-}
{-# LANGUAGE ConstraintKinds           #-}
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
{-# LANGUAGE QuasiQuotes               #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE TemplateHaskell           #-}
{-# LANGUAGE TypeApplications          #-}
{-# LANGUAGE TypeFamilies              #-}
{-# LANGUAGE TypeOperators             #-}
{-# LANGUAGE StandaloneDeriving        #-}
{-# LANGUAGE TupleSections             #-}
{-# LANGUAGE UndecidableInstances      #-}

module BlueRipple.Model.DistrictClusters where

import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.ElectionTypes as ET
import qualified BlueRipple.Data.ACS_PUMS as PUMS
import qualified BlueRipple.Data.Keyed         as Keyed

import qualified Knit.Report                   as K
import qualified Polysemy.RandomFu             as PRF
import qualified Frames.MapReduce as FMR

import qualified Flat
import qualified Frames.Serialize as FS

import qualified Data.Vector.Unboxed           as UVec
import qualified Frames as F
import qualified Frames.Melt as F
import qualified Frames.Streamly.InCore as FI
import qualified Frames.Streamly.TH as FS
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V

import qualified Control.Foldl as FL
import qualified Control.Foldl.Statistics      as FLS
import qualified Control.Monad.State as State
import qualified Data.Set as Set
import qualified Data.Map as M
import qualified Data.List as List
import Data.List ((!!))
import qualified Data.Text as T

import qualified Data.Massiv.Array as MA

import qualified BlueRipple.Model.TSNE as BR
import qualified Data.Algorithm.TSNE.Utils as TSNE
import qualified Numeric.Clustering.Perplexity as Perplexity

import qualified Data.Datamining.Clustering.SGM as SGM
import qualified Data.Datamining.Clustering.Classifier as SOM
import qualified Data.Datamining.Clustering.SOM as SOM
import qualified Math.Geometry.Grid as Grid
import qualified Math.Geometry.Grid.Square as Grid
import qualified Math.Geometry.GridMap as GridMap
import qualified Math.Geometry.GridMap.Lazy as GridMap
import qualified Data.Random.Distribution.Uniform as RandomFu
import qualified Data.Random as RandomFu
--import qualified Data.Sparse.SpMatrix as SLA
import GHC.TypeLits (Symbol)
import qualified Graphics.Vega.VegaLite.Configuration as FV
import Numeric.LinearAlgebra (rows)
import qualified Frames.Visualization.VegaLite.Data as FVD
import qualified Graphics.Vega.VegaLite as GV
import qualified Graphics.Vega.VegaLite.Compat as FV


data DistrictP as rs = DistrictP { districtId :: F.Record as, isModel :: Bool, districtPop :: Int, districtVec :: UVec.Vector Double } deriving (Generic)
deriving instance Show (F.Record as) => Show (DistrictP as rs)
deriving instance Eq (F.Record as) => Eq (DistrictP as rs)

instance (V.RMap as, FS.RecFlat as) => Flat.Flat (DistrictP as rs) where
  size (DistrictP id im dp dv) n = Flat.size (FS.toS id, im, dp, UVec.toList dv) n
  encode (DistrictP id im dp dv) = Flat.encode (FS.toS id, im, dp, UVec.toList dv)
  decode = (\(idS, im, dp, dvL) -> DistrictP (FS.fromS idS) im dp (UVec.fromList dvL)) <$> Flat.decode


districtsForClustering :: forall dcs c.
                          (V.KnownField c
                          , V.Snd c ~ Int
                          , dcs F.⊆  (dcs V.++ '[c])
                          , (dcs V.++ '[c]) F.⊆ ([BR.StateAbbreviation, ET.DistrictTypeC, ET.DistrictName, c] V.++ dcs)
                          , F.ElemOf (dcs V.++ '[c]) c
                          , Ord (F.Record dcs))
                          => F.FrameRec ([BR.StateAbbreviation, ET.DistrictTypeC, ET.DistrictName, c] V.++ dcs)
                       -> [DistrictP [BR.StateAbbreviation, ET.DistrictTypeC, ET.DistrictName, c] (dcs V.++ '[c])]
districtsForClustering dists =
--  pumsByCDWithVoteShare <- K.ignoreCacheTime pumsByCDWithVoteShare_C
  let asVec rs = UVec.fromList
                 $ fmap snd
                 $ M.toAscList
                 $ FL.fold (FL.premap (\r-> (F.rcast @dcs r, realToFrac $ F.rgetField @c r)) FL.map) rs
      pop rs = FL.fold (FL.premap (F.rgetField @c) FL.sum) rs
      popRec :: Int -> F.Record '[c] = \n -> n F.&: V.RNil
  in FL.fold
    (FMR.mapReduceFold
     FMR.noUnpack
     (FMR.assignKeysAndData @[BR.StateAbbreviation, ET.DistrictTypeC, ET.DistrictName] @(dcs V.++ '[c]))
     (FMR.ReduceFold $ \k -> fmap (\rs -> DistrictP (k F.<+> popRec (pop rs)) False (pop rs) (asVec rs)) FL.list)
    )
    dists


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

districtDiff
  :: forall ks d as rs.
     (UVec.Vector Double -> UVec.Vector Double -> Double)
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

type SparseMatrix a = [(Int, Int, a)]

-- NB: Weight is chosen so that for exactly the same and equidistributed,
-- tr (M'M) = (numPats * numPats)/numClusters
sameClusterMatrixSOM :: forall c v k p dk. (SOM.Classifier c v k p, Ord dk, Eq k, Ord v) => (p -> dk) -> c v k p -> [p] -> SparseMatrix Int
sameClusterMatrixSOM getKey som ps = sameClusterMatrix getKey (SOM.classify som) ps

sameClusterMatrix :: forall k p dk. (Ord dk, Eq k) => (p -> dk) -> (p -> k) -> [p] -> SparseMatrix Int
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
  in {- SLA.fromListSM (numPats, numPats) $-} State.evalState scM M.empty


randomSCM :: K.Member PRF.RandomFu r => Int -> Int -> K.Sem r (SparseMatrix Int)
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
  return $ {-SLA.fromListSM (nPats, nPats)-} scM

type Method = "Method" F.:-> T.Text
type Mean = "mean" F.:-> Double
type Sigma = "sigma" F.:-> Double
type ScaledDelta = "scaled_delta" F.:-> Double
type FlipIndex = "flip_index" F.:-> Double



computeScaledDelta
  :: forall ks a f r.
  (K.KnitEffects r
  , Foldable f
  , FI.RecVec (ks V.++ [Method, Mean, Sigma, ScaledDelta, FlipIndex])
  )
  => T.Text
  -> (a -> a -> Double)
  -> Double
  -> (a -> Double)
  -> (a -> F.Record ks)
  -> f a
  -> K.Sem r (F.FrameRec (ks V.++ [Method, Mean, Sigma, ScaledDelta, FlipIndex]))
computeScaledDelta methodName distance perplexity qty key rows = do
  let length = FL.fold FL.length rows
      datV = MA.fromList @MA.B MA.Seq $ FL.fold FL.list rows
      xs = MA.map qty datV
      rawDistances
        = MA.computeAs MA.U $ TSNE.symmetric MA.Seq (MA.Sz1 length)
          $ \(i MA.:. j) -> distance (datV MA.! i) (datV MA.! j)
      meanDistance = MA.sum rawDistances / (realToFrac $ length * length)
      distances = MA.computeAs MA.U $ MA.map (/meanDistance) rawDistances
  K.logLE K.Info "Computing probabilities from distances.."
--  K.logLE K.Info $ T.pack $ show distances
  probabilities <- K.knitEither
                   $ either (Left . T.pack . show) Right
                   $ Perplexity.probabilities perplexity distances
--  K.logLE K.Info $ T.pack $ show probabilities
  K.logLE K.Info "Finished computing probabilities from distances."
  let dist v =
        let xw = MA.computeAs MA.B $ MA.zip xs v
            mean = FL.fold FLS.meanWeighted xw
            var = FL.fold (FLS.varianceWeighted mean) xw
        in (mean, var)
      dists = MA.map dist (MA.outerSlices probabilities)
      asRec :: a -> (Double, Double) -> F.Record (ks V.++ [Method, Mean, Sigma, ScaledDelta, FlipIndex])
      asRec a (m, v) =
        let sigma = sqrt v
            scaledDelta = (qty a - m)/sigma
            flipIndex = if (qty a - 0.5) * (m - 0.5) < 0 then abs scaledDelta else 0
            suffixRec :: F.Record [Method, Mean, Sigma, ScaledDelta, FlipIndex]
            suffixRec = methodName F.&: m F.&: sigma F.&: scaledDelta F.&: flipIndex F.&: V.RNil
        in key a F.<+> suffixRec
  return $ F.toFrame $ MA.computeAs MA.B $ MA.zipWith asRec datV dists

FS.declareColumn "TSNE1" ''Double
FS.declareColumn "TSNE2" ''Double
FS.declareColumn "TSNEIters" ''Int
FS.declareColumn "TSNEPerplexity" ''Int
FS.declareColumn "TSNELearningRate" ''Double

tsneChartCat :: forall t f.(V.KnownField t, Foldable f, Show (V.Snd t))
             => T.Text
             -> T.Text
             -> FV.ViewConfig
             -> f (F.Record [BR.StateAbbreviation, ET.DistrictTypeC, ET.DistrictName, t, TSNE1, TSNE2])
             -> GV.VegaLite
tsneChartCat title labelName vc rows =
  let toVLDataRec = FVD.asVLData GV.Str "State"
                    V.:& FVD.asVLData (GV.Str . show) "DType"
                    V.:& FVD.asVLData GV.Str "District"
                    V.:& FVD.asVLData (GV.Str . show) labelName
                    V.:& FVD.asVLData GV.Number "TSNE-x"
                    V.:& FVD.asVLData GV.Number "TSNE-y"
                    V.:& V.RNil
      vlData = FVD.recordsToData toVLDataRec rows
      makeDistrictName = GV.transform . GV.calculateAs "datum.State + '-' + datum.District" "District Name"
      encodeTSNEx = GV.position GV.X [GV.PName "TSNE-x", GV.PmType GV.Quantitative, GV.PScale [GV.SZero False]]
      encodeTSNEy = GV.position GV.Y [GV.PName "TSNE-y", GV.PmType GV.Quantitative, GV.PScale [GV.SZero False]]
      encodeLabel = GV.color [GV.MName labelName, GV.MmType GV.Nominal]
      encDistrictName = GV.text [GV.TName "District Name", GV.TmType GV.Nominal]
      encToolTips = GV.tooltips [[GV.TName "District Name", GV.TmType GV.Nominal], [GV.TName labelName, GV.TmType GV.Nominal]]
      ptEnc = GV.encoding . encodeTSNEx . encodeTSNEy . encodeLabel
      labelEnc = ptEnc . encDistrictName . encToolTips
      ptSpec = GV.asSpec [ptEnc [], GV.mark GV.Circle []]
      labelSpec = GV.asSpec [labelEnc [],  GV.mark GV.Text [GV.MdX 20], makeDistrictName [] ]
  in FV.configuredVegaLite vc [FV.title title, GV.layer [ptSpec, labelSpec], vlData]


tsneChartNum :: forall t f.(V.KnownField t, Foldable f, Real (V.Snd t), F.ElemOf [BR.StateAbbreviation, ET.DistrictTypeC, ET.DistrictName, t, TSNE1, TSNE2] t)
             => T.Text
             -> T.Text
             -> (V.Snd t -> V.Snd t)
             -> FV.ViewConfig
             -> f (F.Record [BR.StateAbbreviation, ET.DistrictTypeC, ET.DistrictName, t, TSNE1, TSNE2])
             -> GV.VegaLite
tsneChartNum title numName f vc rows =
  let toVLDataRec = FVD.asVLData GV.Str "State"
                    V.:& FVD.asVLData (GV.Str . show) "DType"
                    V.:& FVD.asVLData GV.Str "District"
                    V.:& FVD.asVLData (GV.Number . realToFrac . f) numName
                    V.:& FVD.asVLData GV.Number "TSNE-x"
                    V.:& FVD.asVLData GV.Number "TSNE-y"
                    V.:& V.RNil
      maxF = fmap (fromMaybe 0) $ FL.maximum
      absMinF = fmap (abs . fromMaybe 0) $ FL.minimum
      colorExtent = FL.fold (FL.premap (realToFrac . f . F.rgetField @t) (max <$> maxF <*> absMinF)) rows
      vlData = FVD.recordsToData toVLDataRec rows
      makeDistrictName = GV.transform . GV.calculateAs "datum.State + '-' + datum.District" "District Name"
      encodeTSNEx = GV.position GV.X [GV.PName "TSNE-x", GV.PmType GV.Quantitative, GV.PScale [GV.SZero False]]
      encodeTSNEy = GV.position GV.Y [GV.PName "TSNE-y", GV.PmType GV.Quantitative, GV.PScale [GV.SZero False]]
      encodeLabel = GV.color [GV.MName numName, GV.MmType GV.Quantitative, GV.MScale [GV.SDomain (GV.DNumbers [negate colorExtent, colorExtent]), GV.SScheme "redblue" []]]
      encDistrictName = GV.text [GV.TName "District Name", GV.TmType GV.Nominal]
      encToolTips = GV.tooltips [[GV.TName "District Name", GV.TmType GV.Nominal], [GV.TName numName, GV.TmType GV.Nominal]]
      ptEnc = GV.encoding . encodeTSNEx . encodeTSNEy . encodeLabel
      labelEnc = ptEnc . encDistrictName . encToolTips
      ptSpec = GV.asSpec [ptEnc [], GV.mark GV.Circle []]
      labelSpec = GV.asSpec [labelEnc [],  GV.mark GV.Text [GV.MdX 20], makeDistrictName [] ]
  in FV.configuredVegaLite vc [FV.title title, GV.layer [ptSpec, labelSpec], vlData]


tsneChart' :: forall f.(Foldable f)
          => T.Text
          -> FV.ViewConfig
          -> f (F.Record [BR.StateAbbreviation, ET.DistrictTypeC, ET.DistrictName, TSNE1, TSNE2])
          -> GV.VegaLite
tsneChart' title vc rows =
  let toVLDataRec = FVD.asVLData GV.Str "State"
                    V.:& FVD.asVLData (GV.Str . show) "DType"
                    V.:& FVD.asVLData GV.Str "District"
                    V.:& FVD.asVLData GV.Number "TSNE-x"
                    V.:& FVD.asVLData GV.Number "TSNE-y"
                    V.:& V.RNil
      vlData = FVD.recordsToData toVLDataRec rows
      makeDistrictName = GV.transform . GV.calculateAs "datum.State + '-' + datum.District" "District Name"
      encodeTSNEx = GV.position GV.X [GV.PName "TSNE-x", GV.PmType GV.Quantitative, GV.PScale [GV.SZero False]]
      encodeTSNEy = GV.position GV.Y [GV.PName "TSNE-y", GV.PmType GV.Quantitative, GV.PScale [GV.SZero False]]
      encDistrictName = GV.text [GV.TName "District Name", GV.TmType GV.Nominal]
      encToolTips = GV.tooltips [[GV.TName "District Name", GV.TmType GV.Nominal], [GV.TmType GV.Nominal]]
      ptEnc = GV.encoding . encodeTSNEx . encodeTSNEy
      labelEnc = ptEnc . encDistrictName . encToolTips
      ptSpec = GV.asSpec [ptEnc [], GV.mark GV.Circle []]
      labelSpec = GV.asSpec [labelEnc [],  GV.mark GV.Text [GV.MdX 20], makeDistrictName [] ]
  in FV.configuredVegaLite vc [FV.title title, GV.layer [ptSpec, labelSpec], vlData]

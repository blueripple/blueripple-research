{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
module BlueRipple.Data.CountFolds where

import qualified Control.Foldl as FL

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


  -- map reduce folds for counting
type Count = "Count" F.:-> Int
type WeightedCount = "WeightedCount" F.:-> Double
type Successes = "Successes" F.:-> Int
type MeanWeight = "MeanWeight" F.:-> Double
type VarWeight = "VarWeight" F.:-> Double
type WeightedSuccesses = "WeightedSuccesses" F.:-> Double
--type UnweightedSuccesses =  "UnweightedSuccesses" F.:-> Int

binomialFold
  :: (F.Record r -> Bool) -> FL.Fold (F.Record r) (F.Record '[Count, Successes])
binomialFold testRow =
  let successesF = FL.premap (\r -> if testRow r then 1 else 0) FL.sum
  in  (\n s -> n F.&: s F.&: V.RNil) <$> FL.length <*> successesF

countFold
  :: forall k r d
   . ( Ord (F.Record k)
     , FI.RecVec (k V.++ '[Count, Successes])
     , k F.⊆ r
     , d F.⊆ r
     )
  => (F.Record d -> Bool)
  -> FL.Fold (F.Record r) [F.FrameRec (k V.++ '[Count, Successes])]
countFold testData = MR.mapReduceFold
  MR.noUnpack
  (FMR.assignKeysAndData @k)
  (FMR.foldAndAddKey $ binomialFold testData)

type CountCols = '[Count, Successes, WeightedCount, WeightedSuccesses, MeanWeight, VarWeight]

zeroCount :: F.Record CountCols
zeroCount = 0 F.&: 0 F.&: 0 F.&: 0 F.&: 1 F.&: 0 F.&: V.RNil

weightedBinomialFold
  :: (F.Record r -> Bool)
  -> (F.Record r -> Double)
  -> FL.Fold (F.Record r) (F.Record CountCols)
weightedBinomialFold testRow weightRow =
  let wCountF = FL.premap weightRow FL.sum
      wSuccessesF = FL.prefilter testRow wCountF
      successesF  = FL.prefilter testRow FL.length
      meanWeightF = FL.premap weightRow FL.mean
      varWeightF  = FL.premap weightRow FL.variance
      f s ws mw = if mw < 1e-6 then realToFrac s else ws / mw -- if meanweight is 0 but
  in  (\n s wc ws mw vw -> n F.&: s F.&: f n wc mw F.&: f s ws mw F.&: mw F.&: vw F.&: V.RNil)
      <$> FL.length
      <*> successesF
      <*> wCountF
      <*> wSuccessesF
      <*> meanWeightF
      <*> varWeightF
{-
weightedCountFold
  :: forall k r d
   . (Ord (F.Record k)
     , FI.RecVec (k V.++ CountCols)
     , k F.⊆ r
     , k F.⊆ (k V.++ r)
     , d F.⊆ (k V.++ r)
     )
  => (F.Record r -> Bool) -- ^ count this row?
  -> (F.Record d -> Bool) -- ^ success ?
  -> (F.Record d -> Double) -- ^ weight
  -> FL.Fold (F.Record r) (F.FrameRec (k V.++ CountCols))
weightedCountFold {- filterData testData weightData-} = weightedCountFoldGeneral (F.rcast @k)
{-  FMR.concatFold $ FMR.mapReduceFold
    (MR.filterUnpack filterData)
    (FMR.assignKeysAndData @k)
    (FMR.foldAndAddKey $ weightedBinomialFold testData weightData)
-}
-}
weightedCountFold
  :: forall k r d
   . (Ord (F.Record k)
     , FI.RecVec (k V.++ CountCols)
     )
  => (F.Record r -> F.Record k)
  -> (F.Record r -> F.Record d)
  -> (F.Record r -> Bool) -- ^ include this row?
  -> (F.Record d -> Bool) -- ^ success ?
  -> (F.Record d -> Double) -- ^ weight
  -> FL.Fold (F.Record r) (F.FrameRec (k V.++ CountCols))
weightedCountFold getKey getData filterRow testData weightData =
  FL.prefilter filterRow $ FMR.concatFold $ FMR.mapReduceFold
    FMR.noUnpack
    (FMR.Assign $ \r ->  (getKey r, getData r))
    (FMR.foldAndAddKey $ weightedBinomialFold testData weightData)

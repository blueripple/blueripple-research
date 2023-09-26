{-# LANGUAGE AllowAmbiguousTypes       #-}
{-# LANGUAGE DataKinds                 #-}
{-# LANGUAGE PolyKinds                 #-}
{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE FlexibleInstances         #-}
{-# LANGUAGE GADTs                     #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE QuantifiedConstraints     #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE TypeApplications          #-}
{-# LANGUAGE TypeFamilies              #-}
{-# LANGUAGE TypeOperators             #-}
{-# OPTIONS_GHC  -O0 #-}

module BlueRipple.Data.UsefulDataJoins
  (
    module BlueRipple.Data.UsefulDataJoins
  )
where

import qualified Control.Foldl                 as FL
import qualified Data.Text                     as T
import           Data.Serialize.Text            ( )
import qualified Frames                        as F
import qualified Frames.Melt                   as F
import qualified Frames.InCore                 as FI
import Data.Type.Equality            (type (~))
import qualified Data.Vinyl                    as V
import qualified Data.Vinyl.TypeLevel          as V
import qualified Data.Discrimination.Grouping  as G

import qualified Frames.Transform              as FT
import qualified Frames.Folds                  as FF
import qualified Frames.MapReduce              as FMR
import qualified Frames.SimpleJoins            as FJ

import qualified Knit.Report                   as K

import qualified BlueRipple.Utilities.KnitUtils as BR
import qualified BlueRipple.Data.DataFrames    as BR
import qualified BlueRipple.Data.Loaders as BRL
import qualified BlueRipple.Data.DemographicTypes
                                               as DT

import qualified BlueRipple.Data.GeographicTypes
                                               as GT

import qualified BlueRipple.Data.ElectionTypes
                                               as ET

import qualified BlueRipple.Model.TurnoutAdjustment
                                               as BR

rollupF :: forall as bs ds. (Ord (F.Record bs)
                           , bs F.⊆ (as V.++ bs V.++ ds)
                           , ds F.⊆ (as V.++ bs V.++ ds)
                           , FI.RecVec (bs V.++ ds)
                           ) => FL.Fold (F.Record ds) (F.Record ds) -> FL.Fold (F.Record (as V.++ bs V.++ ds)) (F.FrameRec (bs V.++ ds))
rollupF dataF = FMR.concatFold $ FMR.mapReduceFold
         FMR.noUnpack
         (FMR.assignKeysAndData @bs)
         (FMR.foldAndAddKey dataF)


rollupSumF :: forall as bs ds. (Ord (F.Record bs)
                               , bs F.⊆ (as V.++ bs V.++ ds)
                               , ds F.⊆ (as V.++ bs V.++ ds)
                               , FI.RecVec (bs V.++ ds)
                               , FF.ConstrainedFoldable Num ds
                               )
        => FL.Fold (F.Record (as V.++ bs V.++ ds)) (F.FrameRec (bs V.++ ds))
rollupSumF = rollupF @as @bs @ds (FF.foldAllConstrained @Num FL.sum)

addElectoralWeight ::
  ET.ElectoralWeightSourceT
  -> ET.ElectoralWeightOfT
  -> (F.Record rs -> Double)
  -> F.Record rs
  -> F.Record (rs V.++ [ET.ElectoralWeightSource, ET.ElectoralWeightOf, ET.ElectoralWeight])
addElectoralWeight ews ewof calcEW r =
  let ewcs :: F.Record [ET.ElectoralWeightSource, ET.ElectoralWeightOf, ET.ElectoralWeight] = ews F.&: ewof F.&: calcEW r F.&: V.RNil
  in r `V.rappend` ewcs



type PCols p = [DT.PopCountOf, p]
type PEWCols p = PCols p V.++ ET.EWCols --[DT.PopCountOf, p, ET.ElectoralWeightSource, ET.ElectoralWeightOf, ET.ElectoralWeight]

joinDemoAndWeights
  :: forall js ks p
  . ( FJ.CanLeftJoinM  js (ks V.++ PCols p) (js V.++ ET.EWCols)
    , ((ks V.++ PCols p) V.++ F.RDeleteAll js (js V.++ ET.EWCols)) ~ ((ks V.++ PCols p) V.++ ET.EWCols)
    )
  => F.FrameRec (ks V.++ (PCols p))
  -> F.FrameRec (js V.++ ET.EWCols)
  -> Either [F.Record js] (F.FrameRec (ks V.++ PCols p V.++ ET.EWCols))
joinDemoAndWeights d w = FJ.leftJoinE @js d w
{-F.toFrame
                         $ fmap F.rcast
                         $ catMaybes
                         $ fmap F.recMaybe
                         $ F.leftJoin @js d w
-}

{-
--defaultPopRec :: DT.PopCountOfT -> V.Snd p -> F.Record (PCols p)
--defaultPopRec pco x = pco F.&: x F.&: V.RNil


joinDemoAndWeightsWithDefaults
  :: forall js ks p
  . (js F.⊆ ks
    , Keyed.FiniteSet (F.Record ks)
    , FI.RecVec (ks V.++ PCols p V.++ ET.EWCols)
    , (ks V.++ PCols p V.++ ET.EWCols) F.⊆ ((ks V.++ PCols p) V.++ F.RDeleteAll js (js V.++ ET.EWCols))
    , js F.⊆  (ks V.++ PCols p)
    , js F.⊆  (js V.++ ET.EWCols)
    , (ks V.++ PCols p) F.⊆ ((ks V.++ PCols p) V.++ F.RDeleteAll js (js V.++ ET.EWCols))
    , (F.RDeleteAll js (js V.++ ET.EWCols)) F.⊆  (js V.++ ET.EWCols)
    , V.RMap (ks V.++ PCols p)
    , V.RMap  ((ks V.++ PCols p) V.++ F.RDeleteAll js (js V.++ ET.EWCols))
    , V.RecApplicative  (F.RDeleteAll js (js V.++ ET.EWCols))
    , G.Grouping (F.Record js)
    , FI.RecVec  (ks V.++ PCols p)
    , FI.RecVec (F.RDeleteAll js (js V.++ ET.EWCols))
    , FI.RecVec ((ks V.++ PCols p) V.++ F.RDeleteAll js (js V.++ ET.EWCols))
    , V.KnownField p
    , F.ElemOf (ks V.++ (PCols p)) p
    , F.ElemOf (ks V.++ (PCols p)) DT.PopCountOf
    , ks F.⊆ (ks V.++ (PCols p))
    , (ks V.++ PCols p V.++ ET.EWCols) F.⊆ ((js V.++ ET.EWCols) V.++ F.RDeleteAll js (ks V.++ PCols p))
    , (js V.++ ET.EWCols) F.⊆ ((js V.++ ET.EWCols) V.++ F.RDeleteAll js (ks V.++ PCols p))
    , (F.RDeleteAll js (ks V.++ PCols p)) F.⊆ (ks V.++ PCols p)
    , V.RMap (js V.++ ET.EWCols)
    , V.RMap ((js V.++ ET.EWCols) V.++ F.RDeleteAll js (ks V.++ PCols p))
    , V.RecApplicative (F.RDeleteAll js (ks V.++ PCols p))
    , FI.RecVec (js V.++ ET.EWCols)
    , FI.RecVec (F.RDeleteAll js (ks V.++ PCols p))
    , FI.RecVec ((js V.++ ET.EWCols) V.++ F.RDeleteAll js (ks V.++ PCols p))
    )
  => DT.PopCountOfT
  -> V.Snd p
  -> F.FrameRec (ks V.++ (PCols p))
  -> F.FrameRec (js V.++ ET.EWCols)
  -> F.FrameRec (ks V.++ PCols p V.++ ET.EWCols)
joinDemoAndWeightsWithDefaults pco x d w =
  let demoWithDefaults :: F.FrameRec (ks V.++ (PCols p))
      demoWithDefaults =  F.toFrame $ FL.fold (Keyed.addDefaultRec @ks @(PCols p) (pco F.&: x F.&: V.RNil)) d
  in F.toFrame
     $ fmap F.rcast
     $ catMaybes
     $ fmap F.recMaybe
     $ F.leftJoin @js w demoWithDefaults
-}

adjustWeightsForStateTotals
  :: forall ks p r
  . (K.KnitEffects r
    , V.KnownField p
    , V.Snd p ~ Int
    , F.ElemOf (BR.WithYS ks) GT.StateAbbreviation
    , (ks V.++ PCols p V.++ ET.EWCols) F.⊆ BR.WithYS (ks V.++ PCols p V.++ ET.EWCols)
    , F.ElemOf (ks V.++ PCols p V.++ ET.EWCols) p
    , F.ElemOf (ks V.++ PCols p V.++ ET.EWCols) ET.ElectoralWeight
    , FI.RecVec (ks V.++ PCols p V.++ ET.EWCols)
--    , Show (F.Record ((ks V.++ PCols '(V.Fst p, Int)) V.++ ET.EWCols))
    )
  => F.Frame (F.Record BRL.StateTurnoutCols)
  -> F.FrameRec (BR.WithYS ks V.++ PCols p V.++ ET.EWCols)
  -> K.Sem r (F.FrameRec (BR.WithYS ks V.++ PCols p  V.++ ET.EWCols))
adjustWeightsForStateTotals stateTurnout unadj =
  FL.foldM
    (BR.adjTurnoutFold @p @ET.ElectoralWeight stateTurnout)
    unadj


demographicsWithAdjTurnoutByState
  :: forall catCols p ks js effs
  . ( K.KnitEffects effs
    , FJ.CanLeftJoinM (js V.++ catCols) ((BR.WithYS ks) V.++ catCols V.++ (PCols p)) (js V.++ catCols V.++ ET.EWCols)
    , (((ks V.++ catCols) V.++ PCols p) V.++ F.RDeleteAll (js V.++ catCols) ((js V.++ catCols) V.++ ET.EWCols))
      ~ (((ks V.++ catCols) V.++ PCols p) V.++ ET.EWCols)
    , V.RMap (js V.++ catCols)
    , V.ReifyConstraint Show V.ElField (js V.++ catCols)
    , V.RecordToList (js V.++ catCols)
    , V.KnownField p
    , V.Snd p ~ Int
    , F.ElemOf (ks V.++ catCols V.++ PEWCols p) p
    , F.ElemOf (ks V.++ catCols V.++ PEWCols p) ET.ElectoralWeight
--    , (ks V.++ catCols V.++ PEWCols p) F.⊆ BR.WithYS (ks V.++ catCols V.++ PEWCols p)
    , FI.RecVec (ks V.++ catCols V.++ PEWCols p)
    , (ks V.++ catCols V.++ PEWCols p) F.⊆ (BR.WithYS (ks V.++ catCols V.++ PCols p) V.++ F.RDeleteAll (js V.++ catCols) (js V.++ catCols V.++ ET.EWCols))
--    , (js V.++ catCols) F.⊆ ((js V.++ catCols V.++ ET.EWCols))
--    , (js V.++ catCols) F.⊆ BR.WithYS (ks V.++ catCols)
--    , (js V.++ catCols) F.⊆ BR.WithYS (ks V.++ catCols V.++ (PCols p))
--    , (ks V.++ catCols V.++ (PCols p)) F.⊆ BR.WithYS (ks V.++ catCols V.++ (PCols p) V.++ F.RDeleteAll (js V.++ catCols) ((js V.++ catCols) V.++ ET.EWCols))
--    , (F.RDeleteAll (js V.++ catCols) ((js V.++ catCols) V.++ ET.EWCols)) F.⊆ ((js V.++ catCols) V.++ ET.EWCols)
--    , V.RMap ((ks V.++ catCols) V.++ PCols p)
--    , V.RMap ((((ks V.++ catCols) V.++ PCols p) V.++ F.RDeleteAll (js V.++ catCols) ((js V.++ catCols) V.++ ET.EWCols)))
--    , V.RecApplicative (F.RDeleteAll (js V.++ catCols) ((js V.++ catCols) V.++ ET.EWCols))
--    , G.Grouping (F.Record (js V.++ catCols))
--    , FI.RecVec ((ks V.++ catCols) V.++ PCols p)
--    , FI.RecVec (F.RDeleteAll (js V.++ catCols) ((js V.++ catCols) V.++ ET.EWCols))
--    , FI.RecVec (((ks V.++ catCols) V.++ PCols p) V.++ F.RDeleteAll (js V.++ catCols) ((js V.++ catCols) V.++ ET.EWCols))
    , (ks V.++ catCols V.++ PCols p V.++ ET.EWCols) ~ (ks V.++ catCols V.++ (PCols p V.++ ET.EWCols))
--    , Show (F.Record ((ks V.++ catCols)
--                           V.++ '[ '("PopCountOf", DT.PopCountOfT), p,
--                                   ET.ElectoralWeightSource, ET.ElectoralWeightOf,
--                                   ET.ElectoralWeight]))
    )
  => F.Frame (F.Record BRL.StateTurnoutCols)
  -> F.FrameRec ((BR.WithYS ks) V.++ catCols V.++ (PCols p))
  -> F.FrameRec (js V.++ catCols V.++ ET.EWCols)
  -> K.Sem effs (F.FrameRec ((BR.WithYS ks) V.++ catCols V.++ (PEWCols p)))
demographicsWithAdjTurnoutByState stateTurnout demos ews = do
  let joinedE :: Either [F.Record (js V.++ catCols)] (F.FrameRec ((BR.WithYS ks) V.++ catCols V.++ (PEWCols p)))
      joinedE = joinDemoAndWeights @(js V.++ catCols) @((BR.WithYS ks) V.++ catCols) @p demos ews
  joined <- K.knitEither
            $ either (\missingKeys -> Left $ "missing keys in electoral weights in demographicsWithAdjTurnoutByState: " <> show missingKeys) Right
            $ joinedE
  adjustWeightsForStateTotals @(ks V.++ catCols) @p stateTurnout joined

{-
demographicsWithDefaultsWithAdjTurnoutByState
  :: forall catCols p ks js effs
  . ( K.KnitEffects effs
    , V.KnownField p
    , V.Snd p ~ Int
    , F.ElemOf (ks V.++ catCols V.++ PEWCols p) p
    , F.ElemOf (ks V.++ catCols V.++ PEWCols p) ET.ElectoralWeight
    , (ks V.++ catCols V.++ PEWCols p) F.⊆ BR.WithYS (ks V.++ catCols V.++ PEWCols p)
    , FI.RecVec (ks V.++ catCols V.++ PEWCols p)
    , (ks V.++ catCols V.++ PEWCols p) F.⊆ (BR.WithYS (ks V.++ catCols V.++ PCols p) V.++ F.RDeleteAll (js V.++ catCols) (js V.++ catCols V.++ ET.EWCols))
    , (js V.++ catCols) F.⊆ ((js V.++ catCols V.++ ET.EWCols))
    , (js V.++ catCols) F.⊆ BR.WithYS (ks V.++ catCols)
    , (js V.++ catCols) F.⊆ BR.WithYS (ks V.++ catCols V.++ (PCols p))
    , (ks V.++ catCols V.++ (PCols p)) F.⊆ BR.WithYS (ks V.++ catCols V.++ (PCols p) V.++ F.RDeleteAll (js V.++ catCols) ((js V.++ catCols) V.++ ET.EWCols))
    , (F.RDeleteAll (js V.++ catCols) ((js V.++ catCols) V.++ ET.EWCols)) F.⊆ ((js V.++ catCols) V.++ ET.EWCols)
    , V.RMap ((ks V.++ catCols) V.++ PCols p)
    , V.RMap ((((ks V.++ catCols) V.++ PCols p) V.++ F.RDeleteAll (js V.++ catCols) ((js V.++ catCols) V.++ ET.EWCols)))
    , V.RecApplicative (F.RDeleteAll (js V.++ catCols) ((js V.++ catCols) V.++ ET.EWCols))
    , G.Grouping (F.Record (js V.++ catCols))
    , FI.RecVec ((ks V.++ catCols) V.++ PCols p)
    , FI.RecVec (F.RDeleteAll (js V.++ catCols) ((js V.++ catCols) V.++ ET.EWCols))
    , FI.RecVec (((ks V.++ catCols) V.++ PCols p) V.++ F.RDeleteAll (js V.++ catCols) ((js V.++ catCols) V.++ ET.EWCols))
    , (ks V.++ catCols V.++ PCols p V.++ ET.EWCols) ~ (ks V.++ catCols V.++ (PCols p V.++ ET.EWCols))
    )
  => DT.PopCountOfT
  -> V.Snd p
  -> F.Frame BR.StateTurnout
  -> F.FrameRec ((BR.WithYS ks) V.++ catCols V.++ (PCols p))
  -> F.FrameRec (js V.++ catCols V.++ ET.EWCols)
  -> K.Sem effs (F.FrameRec ((BR.WithYS ks) V.++ catCols V.++ (PEWCols p)))
demographicsWithDefaultsWithAdjTurnoutByState pco x stateTurnout demos ews = do
  let joined :: F.FrameRec ((BR.WithYS ks) V.++ catCols V.++ (PEWCols p))
      joined = joinDemoAndWeightsWithDefaults @(js V.++ catCols) @((BR.WithYS ks) V.++ catCols) @p pco x demos ews
  adjustWeightsForStateTotals @(ks V.++ catCols) @p stateTurnout joined

-}

-- This is monstrous
rollupAdjustAndJoin
  :: forall as catCols p ks js effs
  .(K.KnitEffects effs
   , FJ.CanLeftJoinM (js V.++ catCols) ((BR.WithYS ks) V.++ catCols V.++ (PCols p)) (js V.++ catCols V.++ ET.EWCols)
   , (((ks V.++ catCols) V.++ PCols p) V.++ F.RDeleteAll (js V.++ catCols) ((js V.++ catCols) V.++ ET.EWCols)) ~ (((ks V.++ catCols) V.++ PCols p) V.++ ET.EWCols)
   , V.RMap (js V.++ catCols)
   , V.ReifyConstraint Show V.ElField (js V.++ catCols)
   , V.RecordToList (js V.++ catCols)
   , V.KnownField p
   , V.Snd p ~ Int
   , F.ElemOf (ks V.++ catCols V.++ (PEWCols p)) p
   , F.ElemOf (ks V.++ catCols V.++ (PEWCols p)) ET.ElectoralWeight
   , FI.RecVec (ks V.++ catCols V.++ (PEWCols p))
   , (ks V.++ catCols V.++ (PEWCols p)) F.⊆ BR.WithYS (ks V.++ catCols V.++ (PCols p) V.++ F.RDeleteAll (js V.++ catCols) ((js V.++ catCols V.++ ET.EWCols)))
   , (as V.++ BR.WithYS ks V.++ catCols V.++ PCols p) ~ (as V.++ BR.WithYS (ks V.++ catCols V.++ '[DT.PopCountOf]) V.++ '[p])
   , (((ks V.++ catCols) V.++ '[DT.PopCountOf]) V.++ '[p]) ~ ((ks V.++ catCols) V.++ PCols p)
   , Ord (F.Record ((ks V.++ catCols) V.++ '[DT.PopCountOf]))
   , F.ElemOf  (as V.++ BR.WithYS (ks V.++ catCols V.++ '[DT.PopCountOf]) V.++ '[p]) p
   , F.ElemOf  (as V.++ BR.WithYS (ks V.++ catCols V.++ '[DT.PopCountOf]) V.++ '[p]) GT.StateAbbreviation
   , F.ElemOf  (as V.++ BR.WithYS (ks V.++ catCols V.++ '[DT.PopCountOf]) V.++ '[p]) BR.Year
   , ((ks V.++ catCols) V.++ '[DT.PopCountOf]) F.⊆ (as V.++ BR.WithYS (ks V.++ catCols V.++ '[DT.PopCountOf]) V.++ '[p])
   , FI.RecVec (as V.++ BR.WithYS (ks V.++ catCols V.++ '[DT.PopCountOf]) V.++ '[p] V.++  F.RDeleteAll (ks V.++ catCols) ((ks V.++ catCols) V.++ ET.EWCols))
   , V.AllConstrained G.Grouping (F.UnColumn (ks V.++ catCols))
   , (ks V.++ catCols) F.⊆ (as V.++ BR.WithYS (ks V.++ catCols V.++ '[DT.PopCountOf]) V.++ '[p])
   , (as V.++ BR.WithYS (ks V.++ catCols V.++ '[DT.PopCountOf]) V.++ '[p]) F.⊆ (as V.++ BR.WithYS (ks V.++ catCols V.++ '[DT.PopCountOf]) V.++ '[p] V.++  F.RDeleteAll (ks V.++ catCols) ((ks V.++ catCols) V.++ ET.EWCols))
   , V.RMap (as V.++ BR.WithYS (ks V.++ catCols V.++ '[DT.PopCountOf]) V.++ '[p])
   , V.RMap (as V.++ BR.WithYS (ks V.++ catCols V.++ '[DT.PopCountOf]) V.++ '[p] V.++  F.RDeleteAll (ks V.++ catCols) ((ks V.++ catCols) V.++ ET.EWCols))
   , G.Grouping (F.Record (ks V.++ catCols))
   , FI.RecVec (as V.++ BR.WithYS (ks V.++ catCols V.++ '[DT.PopCountOf]) V.++ '[p])
   , (ks V.++ catCols V.++ PCols p V.++ ET.EWCols) ~ (ks V.++ catCols V.++ (PCols p V.++ ET.EWCols))
   , (as V.++ BR.WithYS (ks V.++ catCols V.++ '[DT.PopCountOf]) V.++ '[p] V.++ ET.EWCols)
    ~ ((as V.++ BR.WithYS (ks V.++ catCols V.++ '[DT.PopCountOf]) V.++ '[p]) V.++ F.RDeleteAll (ks V.++ catCols) (ks V.++ catCols V.++ ET.EWCols))
   , (ks V.++ catCols V.++ ET.EWCols) F.⊆ BR.WithYS (ks V.++ catCols V.++ PEWCols p)
   , (ks V.++ catCols) F.⊆ BR.WithYS (ks V.++ catCols V.++ ET.EWCols)
   , (F.RDeleteAll (ks V.++ catCols) ((ks V.++ catCols) V.++ ET.EWCols)) F.⊆ BR.WithYS (ks V.++ catCols V.++ ET.EWCols)
   , V.RecApplicative (F.RDeleteAll (ks V.++ catCols) ((ks V.++ catCols) V.++ ET.EWCols))
   , FI.RecVec (F.RDeleteAll (ks V.++ catCols) ((ks V.++ catCols) V.++ ET.EWCols))
--   , Show (F.Record ((ks V.++ catCols)
--                           V.++ '[ '("PopCountOf", DT.PopCountOfT), p,
--                                   ET.ElectoralWeightSource, ET.ElectoralWeightOf,
--                                   ET.ElectoralWeight]))
   )
  => F.Frame (F.Record BRL.StateTurnoutCols)
  -> F.FrameRec (as V.++ (BR.WithYS ks) V.++ catCols V.++ (PCols p))
  -> F.FrameRec (js V.++ catCols V.++ ET.EWCols)
  -> K.Sem effs (F.FrameRec (as V.++ (BR.WithYS ks) V.++ catCols V.++ PCols p V.++ ET.EWCols))
rollupAdjustAndJoin stateTurnout demos ews = do
  let rolledUpDemos :: F.FrameRec ((BR.WithYS ks) V.++ catCols V.++ (PCols p))
      rolledUpDemos = FL.fold (rollupSumF @as @((BR.WithYS ks) V.++ catCols V.++ '[DT.PopCountOf]) @'[p]) demos
  adjusted :: F.FrameRec (BR.WithYS ks V.++ catCols V.++ PEWCols p) <- demographicsWithAdjTurnoutByState @catCols @p @ks @js  stateTurnout rolledUpDemos ews
  let adjustedWithoutDemo = fmap (F.rcast @(BR.WithYS ks V.++ catCols V.++ ET.EWCols)) adjusted
  return
      $ F.toFrame
      $ catMaybes
      $ fmap F.recMaybe
      $ F.leftJoin @(BR.WithYS ks V.++ catCols) demos adjustedWithoutDemo

type TurnoutCols = [BR.Population, BR.Citizen, BR.Registered, BR.Voted]
type ACSColsCD = [GT.CongressionalDistrict, BR.Year, GT.StateAbbreviation, BR.StateFIPS, BR.StateName]
type ACSCols = [BR.Year, GT.StateAbbreviation, BR.StateFIPS, BR.StateName]

{-
-- This is also monstrous.  Which is surprising??
acsDemographicsWithAdjCensusTurnoutByCD
  :: forall catCols r
  . (K.KnitEffects r
    , BR.CacheEffects r
    , V.RMap catCols
    , V.ReifyConstraint Show V.ElField catCols
    , V.RecordToList catCols
    , ((catCols V.++ PCols BR.ACSCount) V.++ ET.EWCols) ~ (catCols V.++ PEWCols BR.ACSCount)
    , ((catCols V.++ '[DT.PopCountOf]) V.++ '[BR.ACSCount]) ~ (catCols V.++ [DT.PopCountOf, BR.ACSCount])
    , ((catCols V.++ PCols BR.ACSCount) V.++ F.RDeleteAll catCols (catCols V.++ ET.EWCols)) ~ (catCols V.++ PEWCols BR.ACSCount)
    , BR.RecSerializerC (DT.ACSKeys V.++ catCols V.++ '[BR.ACSCount, BR.VotedPctOfAll])
    , V.RMap (catCols V.++ '[BR.ACSCount, BR.VotedPctOfAll])
    , FI.RecVec (catCols V.++ '[BR.ACSCount, BR.VotedPctOfAll])
    , (catCols V.++ PCols BR.ACSCount) F.⊆ (DT.ACSKeys V.++ (catCols V.++ '[BR.ACSCount]) V.++ '[DT.PopCountOf])
    , (catCols V.++ ET.EWCols) F.⊆ (BR.Year : (catCols V.++ TurnoutCols V.++ ET.EWCols))
    , F.ElemOf (catCols V.++ TurnoutCols) BR.Population
    , F.ElemOf (catCols V.++ TurnoutCols) BR.Voted
    , V.AllConstrained G.Grouping (F.UnColumn catCols)
    , V.RMap (catCols V.++ PCols BR.ACSCount)
    , V.RMap (catCols V.++ PEWCols BR.ACSCount)
    , V.RecApplicative (F.RDeleteAll catCols (catCols V.++ ET.EWCols))
    , G.Grouping (F.Record catCols)
    , FI.RecVec (catCols V.++ PEWCols BR.ACSCount)
    , FI.RecVec (catCols V.++ PCols BR.ACSCount)
    , FI.RecVec (F.RDeleteAll catCols (catCols V.++ ET.EWCols))
    , Ord (F.Record (catCols V.++ '[DT.PopCountOf]))
    , F.ElemOf (catCols V.++ PEWCols BR.ACSCount) BR.ACSCount
    , F.ElemOf (catCols V.++ PCols BR.ACSCount) BR.ACSCount
    , F.ElemOf (catCols V.++ PEWCols BR.ACSCount) ET.ElectoralWeight
    , (catCols V.++ PEWCols BR.ACSCount) F.⊆ (ACSCols V.++ catCols V.++ PEWCols BR.ACSCount)
    , (catCols V.++ '[DT.PopCountOf]) F.⊆ (ACSColsCD V.++ catCols V.++ PCols BR.ACSCount)
    , (catCols V.++ PCols BR.ACSCount) F.⊆ (ACSCols V.++ catCols V.++ PEWCols BR.ACSCount)
    , (catCols V.++ PCols BR.ACSCount) F.⊆ (ACSColsCD V.++ catCols V.++ PEWCols BR.ACSCount)
    , (catCols V.++ ET.EWCols) F.⊆ (ACSCols V.++ catCols V.++ PEWCols BR.ACSCount)
    , catCols F.⊆ (ACSColsCD V.++ catCols V.++ PCols BR.ACSCount)
    , catCols F.⊆ (BR.Year ': (catCols V.++ ET.EWCols))
    , catCols F.⊆ (ACSCols V.++ catCols V.++ PCols BR.ACSCount)
    , catCols F.⊆ (ACSCols V.++ catCols V.++ ET.EWCols)
    , (F.RDeleteAll catCols (catCols V.++ ET.EWCols))  F.⊆ (BR.Year ': (catCols V.++ ET.EWCols))
    , (F.RDeleteAll catCols (catCols V.++ ET.EWCols))  F.⊆ (ACSCols V.++ (catCols V.++ ET.EWCols))
    , (catCols V.++ '[BR.ACSCount, BR.VotedPctOfAll]) F.⊆ (ACSColsCD V.++ (F.RDelete ET.ElectoralWeight (catCols V.++ PEWCols BR.ACSCount)) V.++ '[BR.VotedPctOfAll])
    , (F.RDelete ET.ElectoralWeight (catCols V.++ PEWCols BR.ACSCount)) F.⊆ (ACSColsCD V.++ catCols V.++ PEWCols BR.ACSCount)
--    , V.ReifyConstraint Show V.ElField (catCols
--                                        V.++ '[ '("PopCountOf", DT.PopCountOfT), '("ACSCount", Int),
--                                                ET.ElectoralWeightSource, ET.ElectoralWeightOf,
--                                                ET.ElectoralWeight])
--    , V.RecordToList (catCols
--                       V.++ '[ '("PopCountOf", DT.PopCountOfT), '("ACSCount", Int),
--                               ET.ElectoralWeightSource, ET.ElectoralWeightOf,
--                               ET.ElectoralWeight])
    )
  => T.Text
  -> K.ActionWithCacheTime r (F.FrameRec (DT.ACSKeys V.++ catCols V.++ '[BR.ACSCount]))
  -> K.ActionWithCacheTime r (F.FrameRec ('[BR.Year] V.++ catCols V.++ '[BR.Population, BR.Citizen, BR.Registered, BR.Voted]))
  -> K.ActionWithCacheTime r (F.FrameRec BRL.StateTurnoutCols)
  -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec (DT.ACSKeys V.++ catCols V.++ '[BR.ACSCount, BR.VotedPctOfAll])))
acsDemographicsWithAdjCensusTurnoutByCD cacheKey cachedDemo cachedTurnout cachedStateTurnout = do
  let cachedDeps = (,,) <$> cachedDemo <*> cachedTurnout <*> cachedStateTurnout
  BR.retrieveOrMakeFrame cacheKey cachedDeps $ \(demoF, turnoutF, stateTurnoutF) -> do
    let demo' = fmap (FT.mutate $ const $ FT.recordSingleton @DT.PopCountOf DT.PC_All) demoF
        demo'' = fmap (F.rcast @([GT.CongressionalDistrict, BR.Year, GT.StateAbbreviation, BR.StateFIPS, BR.StateName] V.++ catCols V.++ (PCols BR.ACSCount))) demo'
        vpa r = realToFrac (F.rgetField @BR.Voted r) / realToFrac (F.rgetField @BR.Population r)
    let turnout' = fmap (F.rcast @('[BR.Year] V.++ catCols V.++ ET.EWCols) . addElectoralWeight ET.EW_Census ET.EW_All vpa) turnoutF
    result <- rollupAdjustAndJoin @'[GT.CongressionalDistrict] @catCols @BR.ACSCount @[BR.StateFIPS, BR.StateName] @'[BR.Year] stateTurnoutF demo'' turnout'
    return $ F.toFrame $ fmap (F.rcast . FT.retypeColumn @ET.ElectoralWeight @BR.VotedPctOfAll) result


cachedASEDemographicsWithAdjTurnoutByCD
  :: (K.KnitEffects r
     , BR.CacheEffects r
     )
  => K.ActionWithCacheTime r (F.FrameRec (DT.ACSKeys V.++ DT.CatColsASE V.++ '[BR.ACSCount]))
  -> K.ActionWithCacheTime r (F.FrameRec ( '[BR.Year] V.++ DT.CatColsASE V.++ '[BR.Population, BR.Citizen, BR.Registered, BR.Voted]))
  -> K.ActionWithCacheTime r (F.FrameRec BRL.StateTurnoutCols)
  -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec (DT.ACSKeys V.++ DT.CatColsASE V.++'[BR.ACSCount, BR.VotedPctOfAll])))
cachedASEDemographicsWithAdjTurnoutByCD = acsDemographicsWithAdjCensusTurnoutByCD @DT.CatColsASE "turnout/aseDemoWithStateAdjTurnoutByCD.bin"

cachedASRDemographicsWithAdjTurnoutByCD
  :: (K.KnitEffects r
     , BR.CacheEffects r
     )
  => K.ActionWithCacheTime r (F.FrameRec (DT.ACSKeys V.++ DT.CatColsASR V.++ '[BR.ACSCount]))
  -> K.ActionWithCacheTime r (F.FrameRec ( '[BR.Year] V.++ DT.CatColsASR V.++ '[BR.Population, BR.Citizen, BR.Registered, BR.Voted]))
  -> K.ActionWithCacheTime r (F.FrameRec BRL.StateTurnoutCols)
  -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec (DT.ACSKeys V.++ DT.CatColsASR V.++'[BR.ACSCount, BR.VotedPctOfAll])))
cachedASRDemographicsWithAdjTurnoutByCD = acsDemographicsWithAdjCensusTurnoutByCD @DT.CatColsASR "turnout/asrDemoWithStateAdjTurnoutByCD.bin"
-}

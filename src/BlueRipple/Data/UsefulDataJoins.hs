{-# LANGUAGE AllowAmbiguousTypes       #-}
{-# LANGUAGE DataKinds                 #-}
{-# LANGUAGE DeriveDataTypeable        #-}
{-# LANGUAGE DeriveGeneric             #-}
{-# LANGUAGE PolyKinds                 #-}
{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE FlexibleInstances         #-}
{-# LANGUAGE GADTs                     #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE QuantifiedConstraints     #-}
{-# LANGUAGE QuasiQuotes               #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE TypeApplications          #-}
{-# LANGUAGE TypeFamilies              #-}
{-# LANGUAGE TypeOperators             #-}
{-# LANGUAGE StandaloneDeriving        #-}
{-# LANGUAGE TupleSections             #-}

{-# OPTIONS_GHC  -fplugin=Polysemy.Plugin  -O0 #-}

module BlueRipple.Data.UsefulDataJoins where

import qualified Control.Foldl                 as FL
import           Control.Monad                  ( join )
import qualified Data.Array                    as A
import           Data.Function                  ( on )
import qualified Data.List                     as L
import qualified Data.Set                      as S
import qualified Data.Map                      as M
import           Data.Maybe                     ( isJust
                                                , catMaybes
                                                )

import qualified Data.Text                     as T
import qualified Data.Serialize                as SE
import           Data.Serialize.Text            ( )
import qualified Frames                        as F
import qualified Frames.Melt                   as F
import qualified Frames.InCore                 as FI
import qualified Data.Vinyl                    as V
import qualified Data.Vinyl.TypeLevel          as V
import qualified Data.Discrimination.Grouping  as G


import qualified Control.MapReduce             as MR
import qualified Frames.Transform              as FT
import qualified Frames.Folds                  as FF
import qualified Frames.MapReduce              as FMR
import qualified Frames.Enumerations           as FE
import qualified Frames.Utils                  as FU
import qualified Frames.Serialize              as FS

import qualified Graphics.Vega.VegaLite        as GV
import qualified Knit.Report                   as K

import           BlueRipple.Utilities.KnitUtils

import qualified Statistics.Types              as ST
import           GHC.Generics                   ( Generic )

import qualified BlueRipple.Data.DataFrames    as BR
import qualified BlueRipple.Data.DemographicTypes
                                               as BR
                                               
import qualified BlueRipple.Data.ElectionTypes
                                               as BR

import qualified BlueRipple.Data.PrefModel     as BR
import qualified BlueRipple.Model.TurnoutAdjustment
                                               as BR

import qualified BlueRipple.Utilities.KnitUtils
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
  BR.ElectoralWeightSourceT
  -> BR.ElectoralWeightOfT
  -> (F.Record rs -> Double)
  -> F.Record rs
  -> F.Record (rs V.++ [BR.ElectoralWeightSource, BR.ElectoralWeightOf, BR.ElectoralWeight])
addElectoralWeight ews ewof calcEW r =
  let ewcs :: F.Record [BR.ElectoralWeightSource, BR.ElectoralWeightOf, BR.ElectoralWeight] = ews F.&: ewof F.&: calcEW r F.&: V.RNil
  in r `V.rappend` ewcs


type EWCols = [BR.ElectoralWeightSource, BR.ElectoralWeightOf, BR.ElectoralWeight]
type PCols p = [BR.PopCountOf, p]
type PEWCols p = PCols p V.++ EWCols --[BR.PopCountOf, p, BR.ElectoralWeightSource, BR.ElectoralWeightOf, BR.ElectoralWeight]

joinDemoAndWeights
  :: forall js ks p
  . (js F.⊆ ks
    , FI.RecVec (ks V.++ PCols p V.++ EWCols)
    , (ks V.++ PCols p V.++ EWCols) F.⊆ ((ks V.++ PCols p) V.++ F.RDeleteAll js (js V.++ EWCols))
    , js F.⊆  (ks V.++ PCols p)
    , js F.⊆  (js V.++ EWCols)
    , (ks V.++ PCols p) F.⊆ ((ks V.++ PCols p) V.++ F.RDeleteAll js (js V.++ EWCols))
    , (F.RDeleteAll js (js V.++ EWCols)) F.⊆  (js V.++ EWCols)
    , V.RMap (ks V.++ PCols p)
    , V.RMap  ((ks V.++ PCols p) V.++ F.RDeleteAll js (js V.++ EWCols))
    , V.RecApplicative  (F.RDeleteAll js (js V.++ EWCols))
    , G.Grouping (F.Record js)
    , FI.RecVec  (ks V.++ PCols p)
    , FI.RecVec (F.RDeleteAll js (js V.++ EWCols))
    , FI.RecVec ((ks V.++ PCols p) V.++ F.RDeleteAll js (js V.++ EWCols))
    )
  => F.FrameRec (ks V.++ (PCols p))
  -> F.FrameRec (js V.++ EWCols)
  -> F.FrameRec (ks V.++ PCols p V.++ EWCols)
joinDemoAndWeights d w = F.toFrame
                         $ fmap F.rcast
                         $ catMaybes
                         $ fmap F.recMaybe
                         $ F.leftJoin @js d w

adjustWeightsForStateTotals
  :: forall ks p r
  . (K.KnitEffects r
    , V.KnownField p
    , V.Snd p ~ Int
    , F.ElemOf (BR.WithYS ks) BR.Year
    , F.ElemOf (BR.WithYS ks) BR.StateAbbreviation
    , (ks V.++ PCols p V.++ EWCols) F.⊆ (BR.WithYS (ks V.++ PCols p V.++ EWCols))
    , F.ElemOf (ks V.++ PCols p V.++ EWCols) p
    , F.ElemOf (ks V.++ PCols p V.++ EWCols) BR.ElectoralWeight
    , FI.RecVec (ks V.++ PCols p V.++ EWCols)
    )
  => F.Frame BR.StateTurnout
  -> F.FrameRec ((BR.WithYS ks) V.++ PCols p V.++ EWCols)
  -> K.Sem r (F.FrameRec ((BR.WithYS ks) V.++ PCols p  V.++ EWCols))
adjustWeightsForStateTotals stateTurnout unadj = 
  FL.foldM
    (BR.adjTurnoutFold @p @BR.ElectoralWeight stateTurnout)
    unadj


demographicsWithAdjTurnoutByState
  :: forall catCols p ks js effs
  . ( K.KnitEffects effs
    , V.KnownField p
    , V.Snd p ~ Int
    , F.ElemOf (ks V.++ catCols V.++ PEWCols p) p
    , F.ElemOf (ks V.++ catCols V.++ PEWCols p) BR.ElectoralWeight
    , (ks V.++ catCols V.++ PEWCols p) F.⊆ BR.WithYS (ks V.++ catCols V.++ PEWCols p)
    , FI.RecVec (ks V.++ catCols V.++ PEWCols p)
    , (ks V.++ catCols V.++ PEWCols p) F.⊆ (BR.WithYS (ks V.++ catCols V.++ PCols p) V.++ F.RDeleteAll (js V.++ catCols) (js V.++ catCols V.++ EWCols))
    , (js V.++ catCols) F.⊆ ((js V.++ catCols V.++ EWCols))
    , (js V.++ catCols) F.⊆ BR.WithYS (ks V.++ catCols)
    , (js V.++ catCols) F.⊆ BR.WithYS (ks V.++ catCols V.++ (PCols p))
    , (ks V.++ catCols V.++ (PCols p)) F.⊆ BR.WithYS (ks V.++ catCols V.++ (PCols p) V.++ F.RDeleteAll (js V.++ catCols) ((js V.++ catCols) V.++ EWCols))
    , (F.RDeleteAll (js V.++ catCols) ((js V.++ catCols) V.++ EWCols)) F.⊆ ((js V.++ catCols) V.++ EWCols)
    , V.RMap ((ks V.++ catCols) V.++ PCols p)
    , V.RMap ((((ks V.++ catCols) V.++ PCols p) V.++ F.RDeleteAll (js V.++ catCols) ((js V.++ catCols) V.++ EWCols)))
    , V.RecApplicative (F.RDeleteAll (js V.++ catCols) ((js V.++ catCols) V.++ EWCols))
    , G.Grouping (F.Record (js V.++ catCols))
    , FI.RecVec ((ks V.++ catCols) V.++ PCols p)
    , FI.RecVec (F.RDeleteAll (js V.++ catCols) ((js V.++ catCols) V.++ EWCols))
    , FI.RecVec (((ks V.++ catCols) V.++ PCols p) V.++ F.RDeleteAll (js V.++ catCols) ((js V.++ catCols) V.++ EWCols))
    , (ks V.++ catCols V.++ PCols p V.++ EWCols) ~ (ks V.++ catCols V.++ (PCols p V.++ EWCols))
    )
  => F.Frame BR.StateTurnout
  -> F.FrameRec ((BR.WithYS ks) V.++ catCols V.++ (PCols p))
  -> F.FrameRec (js V.++ catCols V.++ EWCols)
  -> K.Sem effs (F.FrameRec ((BR.WithYS ks) V.++ catCols V.++ (PEWCols p)))
demographicsWithAdjTurnoutByState stateTurnout demos ews = do
  let joined :: F.FrameRec ((BR.WithYS ks) V.++ catCols V.++ (PEWCols p))
      joined = joinDemoAndWeights @(js V.++ catCols) @((BR.WithYS ks) V.++ catCols) @p demos ews
  adjustWeightsForStateTotals @(ks V.++ catCols) @p stateTurnout joined

leftJoinM
  :: forall ks as bs.
  (
    FI.RecVec (as V.++ (F.RDeleteAll ks bs))
  , ks F.⊆ as
  , ks F.⊆ bs
  , as F.⊆ (as V.++ (F.RDeleteAll ks bs))
  , (F.RDeleteAll ks bs) F.⊆ bs
  , V.RMap as
  , V.RMap (as V.++ (F.RDeleteAll ks bs))
  , V.RecApplicative (F.RDeleteAll ks bs)
  , G.Grouping (F.Record ks)
  , FI.RecVec as
  , FI.RecVec (F.RDeleteAll ks bs)
  )
  => F.FrameRec as
  -> F.FrameRec bs
  -> Maybe (F.FrameRec (as V.++ (F.RDeleteAll ks bs)))
leftJoinM fa fb = fmap F.toFrame $ sequence $ fmap F.recMaybe $ F.leftJoin @ks fa fb
  

leftJoinM3
  :: forall ks as bs cs.
  (
    FI.RecVec (as V.++ (F.RDeleteAll ks bs))
  , ks F.⊆ as
  , ks F.⊆ bs
  , as F.⊆ (as V.++ (F.RDeleteAll ks bs))
  , (F.RDeleteAll ks bs) F.⊆ bs
  , V.RMap as
  , V.RMap (as V.++ (F.RDeleteAll ks bs))
  , V.RecApplicative (F.RDeleteAll ks bs)
  , G.Grouping (F.Record ks)
  , FI.RecVec as
  , FI.RecVec (F.RDeleteAll ks bs)
  , FI.RecVec ((as V.++ F.RDeleteAll ks bs) V.++ F.RDeleteAll ks cs)
  , ks F.⊆ (as V.++ (F.RDeleteAll ks bs))
  , ks F.⊆ cs
  , (as V.++ F.RDeleteAll ks bs) F.⊆ (as V.++ (F.RDeleteAll ks bs) V.++ (F.RDeleteAll ks cs))
  , (F.RDeleteAll ks cs) F.⊆ cs
  , V.RMap (as V.++ (F.RDeleteAll ks bs) V.++ (F.RDeleteAll ks cs))
  , V.RecApplicative (F.RDeleteAll ks cs)
  , FI.RecVec (F.RDeleteAll ks cs)
  )
  => F.FrameRec as
  -> F.FrameRec bs
  -> F.FrameRec cs
  -> Maybe (F.FrameRec (as V.++ (F.RDeleteAll ks bs) V.++ (F.RDeleteAll ks cs)))
leftJoinM3 fa fb fc = do
  fab <-  leftJoinM @ks fa fb
  fmap F.toFrame $ sequence $ fmap F.recMaybe $ F.leftJoin @ks fab fc
  
-- This is monstrous
rollupAdjustAndJoin
  :: forall as catCols p ks js effs
  .(K.KnitEffects effs
  , V.KnownField p
  , V.Snd p ~ Int
  , F.ElemOf (ks V.++ catCols V.++ (PEWCols p)) p
  , F.ElemOf (ks V.++ catCols V.++ (PEWCols p)) BR.ElectoralWeight
  , (ks V.++ catCols V.++ (PEWCols p)) F.⊆ BR.WithYS (ks V.++ catCols V.++ (PEWCols p))
  , FI.RecVec (ks V.++ catCols V.++ (PEWCols p))
  , (ks V.++ catCols V.++ (PEWCols p)) F.⊆ BR.WithYS (ks V.++ catCols V.++ (PCols p) V.++ F.RDeleteAll (js V.++ catCols) ((js V.++ catCols V.++ EWCols)))
  , (js V.++ catCols) F.⊆ ((js V.++ catCols V.++ EWCols))
  , (js V.++ catCols) F.⊆ BR.WithYS (ks V.++ catCols)
  , (js V.++ catCols) F.⊆ BR.WithYS (ks V.++ catCols V.++ (PCols p))
  , (ks V.++ catCols V.++ (PCols p)) F.⊆ BR.WithYS (ks V.++ catCols V.++ (PCols p) V.++ F.RDeleteAll (js V.++ catCols) ((js V.++ catCols) V.++ EWCols))
  , (F.RDeleteAll (js V.++ catCols) ((js V.++ catCols) V.++ EWCols)) F.⊆ ((js V.++ catCols) V.++ EWCols)
  , V.RMap ((ks V.++ catCols) V.++ PCols p)
  , V.RMap ((((ks V.++ catCols) V.++ PCols p) V.++ F.RDeleteAll (js V.++ catCols) ((js V.++ catCols) V.++ EWCols)))
  , V.RecApplicative (F.RDeleteAll (js V.++ catCols) ((js V.++ catCols) V.++ EWCols))
  , G.Grouping (F.Record (js V.++ catCols))
  , FI.RecVec ((ks V.++ catCols) V.++ PCols p)
  , FI.RecVec (F.RDeleteAll (js V.++ catCols) ((js V.++ catCols) V.++ EWCols))
  , FI.RecVec (((ks V.++ catCols) V.++ PCols p) V.++ F.RDeleteAll (js V.++ catCols) ((js V.++ catCols) V.++ EWCols))
  , (as V.++ BR.WithYS ks V.++ catCols V.++ PCols p) ~ (as V.++ BR.WithYS (ks V.++ catCols V.++ '[BR.PopCountOf]) V.++ '[p])
  , (((ks V.++ catCols) V.++ '[BR.PopCountOf]) V.++ '[p]) ~ ((ks V.++ catCols) V.++ PCols p)
  , Ord (F.Record ((ks V.++ catCols) V.++ '[BR.PopCountOf]))
  , F.ElemOf  (as V.++ BR.WithYS (ks V.++ catCols V.++ '[BR.PopCountOf]) V.++ '[p]) p
  , F.ElemOf  (as V.++ BR.WithYS (ks V.++ catCols V.++ '[BR.PopCountOf]) V.++ '[p]) BR.StateAbbreviation
  , F.ElemOf  (as V.++ BR.WithYS (ks V.++ catCols V.++ '[BR.PopCountOf]) V.++ '[p]) BR.Year
  , ((ks V.++ catCols) V.++ '[BR.PopCountOf]) F.⊆ (as V.++ BR.WithYS (ks V.++ catCols V.++ '[BR.PopCountOf]) V.++ '[p])
  , FI.RecVec (as V.++ BR.WithYS (ks V.++ catCols V.++ '[BR.PopCountOf]) V.++ '[p] V.++  F.RDeleteAll (ks V.++ catCols) ((ks V.++ catCols) V.++ EWCols))
  , V.AllConstrained G.Grouping (F.UnColumn (ks V.++ catCols))
  , (ks V.++ catCols) F.⊆ (as V.++ BR.WithYS (ks V.++ catCols V.++ '[BR.PopCountOf]) V.++ '[p])
  , (as V.++ BR.WithYS (ks V.++ catCols V.++ '[BR.PopCountOf]) V.++ '[p]) F.⊆ (as V.++ BR.WithYS (ks V.++ catCols V.++ '[BR.PopCountOf]) V.++ '[p] V.++  F.RDeleteAll (ks V.++ catCols) ((ks V.++ catCols) V.++ EWCols))
  , V.RMap (as V.++ BR.WithYS (ks V.++ catCols V.++ '[BR.PopCountOf]) V.++ '[p])
  , V.RMap (as V.++ BR.WithYS (ks V.++ catCols V.++ '[BR.PopCountOf]) V.++ '[p] V.++  F.RDeleteAll (ks V.++ catCols) ((ks V.++ catCols) V.++ EWCols))
  , G.Grouping (F.Record (ks V.++ catCols))
  , FI.RecVec (as V.++ BR.WithYS (ks V.++ catCols V.++ '[BR.PopCountOf]) V.++ '[p])
  , (ks V.++ catCols V.++ PCols p V.++ EWCols) ~ (ks V.++ catCols V.++ (PCols p V.++ EWCols))
  , (as V.++ BR.WithYS (ks V.++ catCols V.++ '[BR.PopCountOf]) V.++ '[p] V.++ EWCols)
    ~ ((as V.++ BR.WithYS (ks V.++ catCols V.++ '[BR.PopCountOf]) V.++ '[p]) V.++ F.RDeleteAll (ks V.++ catCols) (ks V.++ catCols V.++ EWCols))
  , (ks V.++ catCols V.++ EWCols) F.⊆ BR.WithYS (ks V.++ catCols V.++ PEWCols p)
  , (ks V.++ catCols) F.⊆ BR.WithYS (ks V.++ catCols V.++ EWCols)
  , (F.RDeleteAll (ks V.++ catCols) ((ks V.++ catCols) V.++ EWCols)) F.⊆ BR.WithYS (ks V.++ catCols V.++ EWCols)
  , V.RecApplicative (F.RDeleteAll (ks V.++ catCols) ((ks V.++ catCols) V.++ EWCols))
  , FI.RecVec (F.RDeleteAll (ks V.++ catCols) ((ks V.++ catCols) V.++ EWCols))
  )
  => F.Frame BR.StateTurnout
  -> F.FrameRec (as V.++ (BR.WithYS ks) V.++ catCols V.++ (PCols p))
  -> F.FrameRec (js V.++ catCols V.++ EWCols)
  -> K.Sem effs (F.FrameRec (as V.++ (BR.WithYS ks) V.++ catCols V.++ PCols p V.++ EWCols))
rollupAdjustAndJoin stateTurnout demos ews = do
  let rolledUpDemos :: F.FrameRec ((BR.WithYS ks) V.++ catCols V.++ (PCols p))
      rolledUpDemos = FL.fold (rollupSumF @as @((BR.WithYS ks) V.++ catCols V.++ '[BR.PopCountOf]) @'[p]) demos
  adjusted :: F.FrameRec (BR.WithYS ks V.++ catCols V.++ PEWCols p) <- demographicsWithAdjTurnoutByState @catCols @p @ks @js  stateTurnout rolledUpDemos ews
  let adjustedWithoutDemo = fmap (F.rcast @(BR.WithYS ks V.++ catCols V.++ EWCols)) adjusted
  return
      $ F.toFrame
      $ catMaybes
      $ fmap F.recMaybe
      $ F.leftJoin @(BR.WithYS ks V.++ catCols) demos adjustedWithoutDemo

type TurnoutCols = [BR.Population, BR.Citizen, BR.Registered, BR.Voted]
type ACSColsCD = [BR.CongressionalDistrict, BR.Year, BR.StateAbbreviation, BR.StateFIPS, BR.StateName]
type ACSCols = [BR.Year, BR.StateAbbreviation, BR.StateFIPS, BR.StateName]

-- This is also monstrous.  Which is surprising??
acsDemographicsWithAdjCensusTurnoutByCD
  :: forall catCols r
  . (K.KnitEffects r
    , ((catCols V.++ PCols BR.ACSCount) V.++ EWCols) ~ (catCols V.++ PEWCols BR.ACSCount)
    , ((catCols V.++ '[BR.PopCountOf]) V.++ '[BR.ACSCount]) ~ (catCols V.++ [BR.PopCountOf, BR.ACSCount])
    , ((catCols V.++ PCols BR.ACSCount) V.++ F.RDeleteAll catCols (catCols V.++ EWCols)) ~ (catCols V.++ PEWCols BR.ACSCount)
    , FS.RecSerialize (BR.ACSKeys V.++ catCols V.++ '[BR.ACSCount, BR.VotedPctOfAll])
    , V.RMap (catCols V.++ '[BR.ACSCount, BR.VotedPctOfAll])
    , FI.RecVec (catCols V.++ '[BR.ACSCount, BR.VotedPctOfAll])
    , (catCols V.++ PCols BR.ACSCount) F.⊆ (BR.ACSKeys V.++ (catCols V.++ '[BR.ACSCount]) V.++ '[BR.PopCountOf])
    , (catCols V.++ EWCols) F.⊆ (BR.Year : (catCols V.++ TurnoutCols V.++ EWCols))
    , F.ElemOf (catCols V.++ TurnoutCols) BR.Population
    , F.ElemOf (catCols V.++ TurnoutCols) BR.Voted
    , V.AllConstrained G.Grouping (F.UnColumn catCols)
    , V.RMap (catCols V.++ PCols BR.ACSCount)
    , V.RMap (catCols V.++ PEWCols BR.ACSCount)
    , V.RecApplicative (F.RDeleteAll catCols (catCols V.++ EWCols))
    , G.Grouping (F.Record catCols)
    , FI.RecVec (catCols V.++ PEWCols BR.ACSCount)
    , FI.RecVec (catCols V.++ PCols BR.ACSCount)
    , FI.RecVec (F.RDeleteAll catCols (catCols V.++ EWCols))
    , Ord (F.Record (catCols V.++ '[BR.PopCountOf]))
    , F.ElemOf (catCols V.++ PEWCols BR.ACSCount) BR.ACSCount
    , F.ElemOf (catCols V.++ PCols BR.ACSCount) BR.ACSCount
    , F.ElemOf (catCols V.++ PEWCols BR.ACSCount) BR.ElectoralWeight
    , (catCols V.++ PEWCols BR.ACSCount) F.⊆ (ACSCols V.++ catCols V.++ PEWCols BR.ACSCount)
    , (catCols V.++ '[BR.PopCountOf]) F.⊆ (ACSColsCD V.++ catCols V.++ PCols BR.ACSCount)
    , (catCols V.++ PCols BR.ACSCount) F.⊆ (ACSCols V.++ catCols V.++ PEWCols BR.ACSCount)
    , (catCols V.++ PCols BR.ACSCount) F.⊆ (ACSColsCD V.++ catCols V.++ PEWCols BR.ACSCount)
    , (catCols V.++ EWCols) F.⊆ (ACSCols V.++ catCols V.++ PEWCols BR.ACSCount)
    , catCols F.⊆ (ACSColsCD V.++ catCols V.++ PCols BR.ACSCount)
    , catCols F.⊆ (BR.Year ': (catCols V.++ EWCols))
    , catCols F.⊆ (ACSCols V.++ catCols)
    , catCols F.⊆ (ACSCols V.++ catCols V.++ PCols BR.ACSCount)
    , catCols F.⊆ (ACSCols V.++ catCols V.++ EWCols)
    , (F.RDeleteAll catCols (catCols V.++ EWCols))  F.⊆ (BR.Year ': (catCols V.++ EWCols))
    , (F.RDeleteAll catCols (catCols V.++ EWCols))  F.⊆ (ACSCols V.++ (catCols V.++ EWCols))
    , (catCols V.++ '[BR.ACSCount, BR.VotedPctOfAll]) F.⊆ (ACSColsCD V.++ (F.RDelete BR.ElectoralWeight (catCols V.++ PEWCols BR.ACSCount)) V.++ '[BR.VotedPctOfAll])
    , (F.RDelete BR.ElectoralWeight (catCols V.++ PEWCols BR.ACSCount)) F.⊆ (ACSColsCD V.++ catCols V.++ PEWCols BR.ACSCount) 
    )
  => T.Text
  -> K.Sem r (F.FrameRec (BR.ACSKeys V.++ catCols V.++ '[BR.ACSCount]))
  -> K.Sem r (F.FrameRec ('[BR.Year] V.++ catCols V.++ '[BR.Population, BR.Citizen, BR.Registered, BR.Voted]))
  -> K.Sem r (F.Frame BR.StateTurnout)
  -> K.Sem r (F.FrameRec (BR.ACSKeys V.++ catCols V.++ '[BR.ACSCount, BR.VotedPctOfAll]))
acsDemographicsWithAdjCensusTurnoutByCD cacheKey demoA turnoutA stateTurnoutA =
  BR.retrieveOrMakeFrame cacheKey $ do
    demo         <- demoA
    turnout      <- turnoutA
    stateTurnout <- stateTurnoutA
    let demo' = fmap (FT.mutate $ const $ FT.recordSingleton @BR.PopCountOf BR.PC_All) demo
        demo'' = fmap (F.rcast @([BR.CongressionalDistrict, BR.Year, BR.StateAbbreviation, BR.StateFIPS, BR.StateName] V.++ catCols V.++ (PCols BR.ACSCount))) demo'
        vpa r = realToFrac (F.rgetField @BR.Voted r) / realToFrac (F.rgetField @BR.Population r)
    let turnout' = fmap (F.rcast @('[BR.Year] V.++ catCols V.++ EWCols) . addElectoralWeight BR.EW_Census BR.EW_All vpa) turnout
    result <- rollupAdjustAndJoin @'[BR.CongressionalDistrict] @catCols @BR.ACSCount @[BR.StateFIPS, BR.StateName] @'[BR.Year] stateTurnout demo'' turnout'
    return $ F.toFrame $ fmap (F.rcast . FT.retypeColumn @BR.ElectoralWeight @BR.VotedPctOfAll) result


cachedASEDemographicsWithAdjTurnoutByCD
  :: forall r 
   . (K.KnitEffects r)
  => K.Sem r (F.FrameRec (BR.ACSKeys V.++ BR.CatColsASE V.++ '[BR.ACSCount]))
  -> K.Sem r
       ( F.FrameRec
           ( '[BR.Year]
               V.++
               BR.CatColsASE
               V.++
               '[BR.Population, BR.Citizen, BR.Registered, BR.Voted]
           )
       )
  -> K.Sem r (F.Frame BR.StateTurnout)
  -> K.Sem
       r
       ( F.FrameRec
           ( BR.ACSKeys
               V.++
               BR.CatColsASE
               V.++
               '[BR.ACSCount, BR.VotedPctOfAll]
           )
       )
cachedASEDemographicsWithAdjTurnoutByCD = acsDemographicsWithAdjCensusTurnoutByCD @BR.CatColsASE "turnout/aseDemoWithStateAdjTurnoutByCD.bin"

cachedASRDemographicsWithAdjTurnoutByCD
  :: forall r
   . (K.KnitEffects r)
  => K.Sem r (F.FrameRec (BR.ACSKeys V.++ BR.CatColsASR V.++ '[BR.ACSCount]))
  -> K.Sem
       r
       ( F.FrameRec
           ( '[BR.Year]
               V.++
               BR.CatColsASR
               V.++
               '[BR.Population, BR.Citizen, BR.Registered, BR.Voted]
           )
       )
  -> K.Sem r (F.Frame BR.StateTurnout)
  -> K.Sem
       r
       ( F.FrameRec
           ( BR.ACSKeys
               V.++
               BR.CatColsASR
               V.++
               '[BR.ACSCount, BR.VotedPctOfAll]
           )
       )
cachedASRDemographicsWithAdjTurnoutByCD =  acsDemographicsWithAdjCensusTurnoutByCD @BR.CatColsASR "turnout/asrDemoWithStateAdjTurnoutByCD.bin"

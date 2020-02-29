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
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE TypeApplications          #-}
{-# LANGUAGE TypeFamilies              #-}
{-# LANGUAGE TypeOperators             #-}
{-# LANGUAGE QuasiQuotes               #-}
{-# LANGUAGE StandaloneDeriving        #-}
{-# LANGUAGE TupleSections             #-}

{-# OPTIONS_GHC  -fplugin=Polysemy.Plugin  #-}

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
type PEWCols p = [BR.PopCountOf, p, BR.ElectoralWeightSource, BR.ElectoralWeightOf, BR.ElectoralWeight]

joinDemoAndWeights
  :: forall js ks p
  . (js F.⊆ ks
    , FI.RecVec (ks V.++ (PEWCols p))
    , (ks V.++ (PEWCols p)) F.⊆ ((ks V.++ PCols p) V.++ F.RDeleteAll js (js V.++ EWCols))
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
  -> F.FrameRec (ks V.++ (PEWCols p))
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
    , (ks V.++ PEWCols p) F.⊆ (BR.WithYS (ks V.++ PEWCols p))
    , F.ElemOf (ks V.++ PEWCols p) p
    , F.ElemOf (ks V.++ PEWCols p) BR.ElectoralWeight
    , FI.RecVec (ks V.++ PEWCols p)
    )
  => F.Frame BR.StateTurnout
  -> F.FrameRec ((BR.WithYS ks) V.++ (PEWCols p))
  -> K.Sem r (F.FrameRec ((BR.WithYS ks) V.++ (PEWCols p)))
adjustWeightsForStateTotals stateTurnout unadj = 
  FL.foldM
    (BR.adjTurnoutFold @p @BR.ElectoralWeight stateTurnout)
    unadj


demographicsWithAdjTurnoutByState
  :: forall catCols p ks js effs
  . ( K.KnitEffects effs
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
    )
  => F.Frame BR.StateTurnout
  -> F.FrameRec ((BR.WithYS ks) V.++ catCols V.++ (PCols p))
  -> F.FrameRec (js V.++ catCols V.++ EWCols)
  -> K.Sem effs (F.FrameRec ((BR.WithYS ks) V.++ catCols V.++ (PEWCols p)))
demographicsWithAdjTurnoutByState stateTurnout demos ews =
  adjustWeightsForStateTotals @(ks V.++ catCols) @p stateTurnout (joinDemoAndWeights @(js V.++ catCols) @((BR.WithYS ks) V.++ catCols) @p demos ews)

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
  , (as V.++ BR.WithYS ks V.++ catCols V.++ PEWCols p) ~ (as V.++ BR.WithYS (ks V.++ catCols V.++ '[BR.PopCountOf]) V.++ '[p] V.++ F.RDeleteAll (ks V.++ catCols) ((ks V.++ catCols V.++ PEWCols p)))
  , (((ks V.++ catCols) V.++ '[BR.PopCountOf]) V.++ '[p]) ~ ((ks V.++ catCols) V.++ PCols p)
  , Ord (F.Record ((ks V.++ catCols) V.++ '[BR.PopCountOf]))
  , F.ElemOf  (as V.++ BR.WithYS (ks V.++ catCols V.++ '[BR.PopCountOf]) V.++ '[p]) p
  , F.ElemOf  (as V.++ BR.WithYS (ks V.++ catCols V.++ '[BR.PopCountOf]) V.++ '[p]) BR.StateAbbreviation
  , F.ElemOf  (as V.++ BR.WithYS (ks V.++ catCols V.++ '[BR.PopCountOf]) V.++ '[p]) BR.Year
  , ((ks V.++ catCols) V.++ '[BR.PopCountOf]) F.⊆ (as V.++ BR.WithYS (ks V.++ catCols V.++ '[BR.PopCountOf]) V.++ '[p])
  , FI.RecVec (as V.++ BR.WithYS (ks V.++ catCols V.++ '[BR.PopCountOf]) V.++ '[p] V.++  F.RDeleteAll (ks V.++ catCols) ((ks V.++ catCols) V.++ PEWCols p))
  , V.AllConstrained G.Grouping (F.UnColumn (ks V.++ catCols))
  , (ks V.++ catCols) F.⊆ (as V.++ BR.WithYS (ks V.++ catCols V.++ '[BR.PopCountOf]) V.++ '[p])
  , (ks V.++ catCols) F.⊆ BR.WithYS (ks V.++ catCols V.++ (PEWCols p))
  , (as V.++ BR.WithYS (ks V.++ catCols V.++ '[BR.PopCountOf]) V.++ '[p]) F.⊆ (as V.++ BR.WithYS (ks V.++ catCols V.++ '[BR.PopCountOf]) V.++ '[p] V.++  F.RDeleteAll (ks V.++ catCols) ((ks V.++ catCols) V.++ PEWCols p))
  , (F.RDeleteAll (ks V.++ catCols) ((ks V.++ catCols) V.++ PEWCols p)) F.⊆ BR.WithYS (ks V.++ catCols V.++ PEWCols p)
  , V.RMap (as V.++ BR.WithYS (ks V.++ catCols V.++ '[BR.PopCountOf]) V.++ '[p])
  , V.RMap (as V.++ BR.WithYS (ks V.++ catCols V.++ '[BR.PopCountOf]) V.++ '[p] V.++  F.RDeleteAll (ks V.++ catCols) ((ks V.++ catCols) V.++ PEWCols p))
  , V.RecApplicative (F.RDeleteAll (ks V.++ catCols) ((ks V.++ catCols) V.++ PEWCols p))
  , G.Grouping (F.Record (ks V.++ catCols))
  , FI.RecVec (as V.++ BR.WithYS (ks V.++ catCols V.++ '[BR.PopCountOf]) V.++ '[p])
  , FI.RecVec (F.RDeleteAll (ks V.++ catCols) ((ks V.++ catCols) V.++ PEWCols p))
  )
  => F.Frame BR.StateTurnout
  -> F.FrameRec (as V.++ (BR.WithYS ks) V.++ catCols V.++ (PCols p))
  -> F.FrameRec (js V.++ catCols V.++ EWCols)
  -> K.Sem effs (F.FrameRec (as V.++ (BR.WithYS ks) V.++ catCols V.++ (PEWCols p)))
rollupAdjustAndJoin stateTurnout demos ews = do
  let rolledUpDemos = FL.fold (rollupSumF @as @((BR.WithYS ks) V.++ catCols V.++ '[BR.PopCountOf]) @'[p]) demos
  adjusted <- demographicsWithAdjTurnoutByState @catCols @p @ks @js @effs stateTurnout rolledUpDemos ews
  return
      $ F.toFrame
      $ catMaybes
      $ fmap F.recMaybe
      $ F.leftJoin @(BR.WithYS ks V.++ catCols) demos adjusted

{-
acsDemographicsWithAdjCensusTurnoutByCD
  :: forall catCols r. (KnitEffects r)
  => K.Sem r (F.FrameRec (BR.ACSKeys V.++ catCols V.++ '[BR.ACSCount]))
  -> K.Sem r (F.FrameRec ('[BR.Year] V.++ catCols V.++ '[BR.Population, BR.Citizen, BR.Registered, BR.Voted]))
  -> K.Sem r (F.Frame BR.StateTurnout)
  -> K.Sem r (F.FrameRec (BR.ACSKeys V.++ catCols V.++ '[BR.ACSCount, BR.VotedPctOfAll]))
cachedASEDemographicsWithAdjTurnoutByCD' demoA turnoutA stateTurnoutA =
  BR.retrieveOrMakeFrame "turnout/aseDemoWithStateAdjTurnoutByCD.bin" $ do
    demo         <- demoA
    turnout      <- turnoutA
    stateTurnout <- stateTurnoutA
    let turnout' = fmap (addElectoralWeight BR.EW_Census BR.EW_All (F.rgetField 
    result <- rollupAdjustAndJoin @BR.CongressionalDistrict @catCols @BR.ACSCount @[BR.Year, BR.StateFIPS, BR.StateName] @'[BR.Year] stateTurnout demo turnout' 
-}

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
cachedASEDemographicsWithAdjTurnoutByCD demoA turnoutA stateTurnoutA =
  BR.retrieveOrMakeFrame "turnout/aseDemoWithStateAdjTurnoutByCD.bin" $ do
    demo         <- demoA
    turnout      <- turnoutA
    stateTurnout <- stateTurnoutA
    let
      rollupCDF = FL.premap F.rcast $ rollupSumF @'[BR.CongressionalDistrict] @('[BR.Year, BR.StateAbbreviation] V.++ BR.CatColsASE) @'[BR.ACSCount]
      demoByState = FL.fold rollupCDF demo
      votedPctOfAll r = realToFrac (F.rgetField @BR.Voted r)
                        / realToFrac (F.rgetField @BR.Population r)
      turnoutWithPct = fmap
        (FT.mutate $ FT.recordSingleton @BR.VotedPctOfAll . votedPctOfAll)
        turnout
      demoWithUnAdjTurnoutByState =
        catMaybes $ fmap F.recMaybe $ F.leftJoin @('[BR.Year] V.++ BR.CatColsASE)
        demoByState
        turnoutWithPct
    demoWithAdjTurnoutByState <- FL.foldM
      (BR.adjTurnoutFold @BR.ACSCount @BR.VotedPctOfAll stateTurnout)
      demoWithUnAdjTurnoutByState
    return
      $ F.toFrame
      $ fmap F.rcast
      $ catMaybes
      $ fmap F.recMaybe
      $ F.leftJoin @('[BR.Year, BR.StateAbbreviation] V.++ BR.CatColsASE) demo demoWithAdjTurnoutByState
  
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
cachedASRDemographicsWithAdjTurnoutByCD demoA turnoutA stateTurnoutA =
  BR.retrieveOrMakeFrame  "turnout/asrDemoWithStateAdjTurnoutByCD.bin" $ do
    demo         <- demoA
    turnout      <- turnoutA
    stateTurnout <- stateTurnoutA
    let
      rollupCDF = FL.premap F.rcast $ rollupSumF @'[BR.CongressionalDistrict] @('[BR.Year, BR.StateAbbreviation] V.++ BR.CatColsASR) @'[BR.ACSCount]
      demoByState = FL.fold rollupCDF demo
      votedPct r = realToFrac (F.rgetField @BR.Voted r)
                   / realToFrac (F.rgetField @BR.Population r)
      turnoutWithPct = fmap
        (FT.mutate $ FT.recordSingleton @BR.VotedPctOfAll . votedPct)
        turnout
      demoWithUnAdjTurnoutByState =
        catMaybes $ fmap F.recMaybe $ F.leftJoin @('[BR.Year] V.++ BR.CatColsASR)
        demoByState
        turnoutWithPct
    demoWithAdjTurnoutByState <- FL.foldM
      (BR.adjTurnoutFold @BR.ACSCount @BR.VotedPctOfAll stateTurnout)
      demoWithUnAdjTurnoutByState
    return
      $ F.toFrame
      $ fmap F.rcast
      $ catMaybes
      $ fmap F.recMaybe
      $ F.leftJoin @('[BR.Year, BR.StateAbbreviation] V.++ BR.CatColsASR) demo demoWithAdjTurnoutByState

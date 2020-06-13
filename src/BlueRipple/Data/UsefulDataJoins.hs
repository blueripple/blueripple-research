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

{-# OPTIONS_GHC  -O0 #-}

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
import qualified Frames.SimpleJoins            as FJ
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
import qualified BlueRipple.Data.Keyed as Keyed                                               



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



type PCols p = [BR.PopCountOf, p]
type PEWCols p = PCols p V.++ BR.EWCols --[BR.PopCountOf, p, BR.ElectoralWeightSource, BR.ElectoralWeightOf, BR.ElectoralWeight]

joinDemoAndWeights
  :: forall js ks p
  . ( FJ.CanLeftJoinM  js (ks V.++ PCols p) (js V.++ BR.EWCols)
    , ((ks V.++ PCols p) V.++ F.RDeleteAll js (js V.++ BR.EWCols)) ~ ((ks V.++ PCols p) V.++ BR.EWCols)
    , js F.⊆ ks
    , FI.RecVec (ks V.++ PCols p V.++ BR.EWCols)
    , (ks V.++ PCols p V.++ BR.EWCols) F.⊆ ((ks V.++ PCols p) V.++ F.RDeleteAll js (js V.++ BR.EWCols))
    , js F.⊆  (ks V.++ PCols p)
    , js F.⊆  (js V.++ BR.EWCols)
    , (ks V.++ PCols p) F.⊆ ((ks V.++ PCols p) V.++ F.RDeleteAll js (js V.++ BR.EWCols))
    , (F.RDeleteAll js (js V.++ BR.EWCols)) F.⊆  (js V.++ BR.EWCols)
    , V.RMap (ks V.++ PCols p)
    , V.RMap  ((ks V.++ PCols p) V.++ F.RDeleteAll js (js V.++ BR.EWCols))
    , V.RecApplicative  (F.RDeleteAll js (js V.++ BR.EWCols))
    , G.Grouping (F.Record js)
    , FI.RecVec  (ks V.++ PCols p)
    , FI.RecVec (F.RDeleteAll js (js V.++ BR.EWCols))
    , FI.RecVec ((ks V.++ PCols p) V.++ F.RDeleteAll js (js V.++ BR.EWCols))
    )
  => F.FrameRec (ks V.++ (PCols p))
  -> F.FrameRec (js V.++ BR.EWCols)
  -> Either [F.Record js] (F.FrameRec (ks V.++ PCols p V.++ BR.EWCols))
joinDemoAndWeights d w = FJ.leftJoinE @js d w
{-F.toFrame
                         $ fmap F.rcast
                         $ catMaybes
                         $ fmap F.recMaybe
                         $ F.leftJoin @js d w
-}

{-
--defaultPopRec :: BR.PopCountOfT -> V.Snd p -> F.Record (PCols p)
--defaultPopRec pco x = pco F.&: x F.&: V.RNil

  
joinDemoAndWeightsWithDefaults
  :: forall js ks p
  . (js F.⊆ ks
    , Keyed.FiniteSet (F.Record ks)
    , FI.RecVec (ks V.++ PCols p V.++ BR.EWCols)
    , (ks V.++ PCols p V.++ BR.EWCols) F.⊆ ((ks V.++ PCols p) V.++ F.RDeleteAll js (js V.++ BR.EWCols))
    , js F.⊆  (ks V.++ PCols p)
    , js F.⊆  (js V.++ BR.EWCols)
    , (ks V.++ PCols p) F.⊆ ((ks V.++ PCols p) V.++ F.RDeleteAll js (js V.++ BR.EWCols))
    , (F.RDeleteAll js (js V.++ BR.EWCols)) F.⊆  (js V.++ BR.EWCols)
    , V.RMap (ks V.++ PCols p)
    , V.RMap  ((ks V.++ PCols p) V.++ F.RDeleteAll js (js V.++ BR.EWCols))
    , V.RecApplicative  (F.RDeleteAll js (js V.++ BR.EWCols))
    , G.Grouping (F.Record js)
    , FI.RecVec  (ks V.++ PCols p)
    , FI.RecVec (F.RDeleteAll js (js V.++ BR.EWCols))
    , FI.RecVec ((ks V.++ PCols p) V.++ F.RDeleteAll js (js V.++ BR.EWCols))
    , V.KnownField p
    , F.ElemOf (ks V.++ (PCols p)) p
    , F.ElemOf (ks V.++ (PCols p)) BR.PopCountOf
    , ks F.⊆ (ks V.++ (PCols p))
    , (ks V.++ PCols p V.++ BR.EWCols) F.⊆ ((js V.++ BR.EWCols) V.++ F.RDeleteAll js (ks V.++ PCols p))
    , (js V.++ BR.EWCols) F.⊆ ((js V.++ BR.EWCols) V.++ F.RDeleteAll js (ks V.++ PCols p))
    , (F.RDeleteAll js (ks V.++ PCols p)) F.⊆ (ks V.++ PCols p)
    , V.RMap (js V.++ BR.EWCols)
    , V.RMap ((js V.++ BR.EWCols) V.++ F.RDeleteAll js (ks V.++ PCols p))
    , V.RecApplicative (F.RDeleteAll js (ks V.++ PCols p))
    , FI.RecVec (js V.++ BR.EWCols)
    , FI.RecVec (F.RDeleteAll js (ks V.++ PCols p))
    , FI.RecVec ((js V.++ BR.EWCols) V.++ F.RDeleteAll js (ks V.++ PCols p))
    )
  => BR.PopCountOfT
  -> V.Snd p
  -> F.FrameRec (ks V.++ (PCols p))
  -> F.FrameRec (js V.++ BR.EWCols)
  -> F.FrameRec (ks V.++ PCols p V.++ BR.EWCols)
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
    , F.ElemOf (BR.WithYS ks) BR.Year
    , F.ElemOf (BR.WithYS ks) BR.StateAbbreviation
    , (ks V.++ PCols p V.++ BR.EWCols) F.⊆ (BR.WithYS (ks V.++ PCols p V.++ BR.EWCols))
    , F.ElemOf (ks V.++ PCols p V.++ BR.EWCols) p
    , F.ElemOf (ks V.++ PCols p V.++ BR.EWCols) BR.ElectoralWeight
    , FI.RecVec (ks V.++ PCols p V.++ BR.EWCols)
    )
  => F.Frame BR.StateTurnout
  -> F.FrameRec ((BR.WithYS ks) V.++ PCols p V.++ BR.EWCols)
  -> K.Sem r (F.FrameRec ((BR.WithYS ks) V.++ PCols p  V.++ BR.EWCols))
adjustWeightsForStateTotals stateTurnout unadj = 
  FL.foldM
    (BR.adjTurnoutFold @p @BR.ElectoralWeight stateTurnout)
    unadj


demographicsWithAdjTurnoutByState
  :: forall catCols p ks js effs
  . ( K.KnitEffects effs
    , FJ.CanLeftJoinM (js V.++ catCols) ((BR.WithYS ks) V.++ catCols V.++ (PCols p)) (js V.++ catCols V.++ BR.EWCols)
    , (((ks V.++ catCols) V.++ PCols p) V.++ F.RDeleteAll (js V.++ catCols) ((js V.++ catCols) V.++ BR.EWCols)) ~ (((ks V.++ catCols) V.++ PCols p) V.++ BR.EWCols)
    , V.RMap (js V.++ catCols)
    , V.ReifyConstraint Show V.ElField (js V.++ catCols)
    , V.RecordToList (js V.++ catCols)
    , V.KnownField p
    , V.Snd p ~ Int
    , F.ElemOf (ks V.++ catCols V.++ PEWCols p) p
    , F.ElemOf (ks V.++ catCols V.++ PEWCols p) BR.ElectoralWeight
    , (ks V.++ catCols V.++ PEWCols p) F.⊆ BR.WithYS (ks V.++ catCols V.++ PEWCols p)
    , FI.RecVec (ks V.++ catCols V.++ PEWCols p)
    , (ks V.++ catCols V.++ PEWCols p) F.⊆ (BR.WithYS (ks V.++ catCols V.++ PCols p) V.++ F.RDeleteAll (js V.++ catCols) (js V.++ catCols V.++ BR.EWCols))
    , (js V.++ catCols) F.⊆ ((js V.++ catCols V.++ BR.EWCols))
    , (js V.++ catCols) F.⊆ BR.WithYS (ks V.++ catCols)
    , (js V.++ catCols) F.⊆ BR.WithYS (ks V.++ catCols V.++ (PCols p))
    , (ks V.++ catCols V.++ (PCols p)) F.⊆ BR.WithYS (ks V.++ catCols V.++ (PCols p) V.++ F.RDeleteAll (js V.++ catCols) ((js V.++ catCols) V.++ BR.EWCols))
    , (F.RDeleteAll (js V.++ catCols) ((js V.++ catCols) V.++ BR.EWCols)) F.⊆ ((js V.++ catCols) V.++ BR.EWCols)
    , V.RMap ((ks V.++ catCols) V.++ PCols p)
    , V.RMap ((((ks V.++ catCols) V.++ PCols p) V.++ F.RDeleteAll (js V.++ catCols) ((js V.++ catCols) V.++ BR.EWCols)))
    , V.RecApplicative (F.RDeleteAll (js V.++ catCols) ((js V.++ catCols) V.++ BR.EWCols))
    , G.Grouping (F.Record (js V.++ catCols))
    , FI.RecVec ((ks V.++ catCols) V.++ PCols p)
    , FI.RecVec (F.RDeleteAll (js V.++ catCols) ((js V.++ catCols) V.++ BR.EWCols))
    , FI.RecVec (((ks V.++ catCols) V.++ PCols p) V.++ F.RDeleteAll (js V.++ catCols) ((js V.++ catCols) V.++ BR.EWCols))
    , (ks V.++ catCols V.++ PCols p V.++ BR.EWCols) ~ (ks V.++ catCols V.++ (PCols p V.++ BR.EWCols))
    )
  => F.Frame BR.StateTurnout
  -> F.FrameRec ((BR.WithYS ks) V.++ catCols V.++ (PCols p))
  -> F.FrameRec (js V.++ catCols V.++ BR.EWCols)
  -> K.Sem effs (F.FrameRec ((BR.WithYS ks) V.++ catCols V.++ (PEWCols p)))
demographicsWithAdjTurnoutByState stateTurnout demos ews = do
  let joinedE :: Either [F.Record (js V.++ catCols)] (F.FrameRec ((BR.WithYS ks) V.++ catCols V.++ (PEWCols p)))
      joinedE = joinDemoAndWeights @(js V.++ catCols) @((BR.WithYS ks) V.++ catCols) @p demos ews
  joined <- K.knitEither
            $ either (\missingKeys -> Left $ "missing keys in electoral weights in demographicsWithAdjTurnoutByState: " <> (T.pack $ show missingKeys)) Right
            $ joinedE
  adjustWeightsForStateTotals @(ks V.++ catCols) @p stateTurnout joined

{-
demographicsWithDefaultsWithAdjTurnoutByState
  :: forall catCols p ks js effs
  . ( K.KnitEffects effs
    , V.KnownField p
    , V.Snd p ~ Int
    , F.ElemOf (ks V.++ catCols V.++ PEWCols p) p
    , F.ElemOf (ks V.++ catCols V.++ PEWCols p) BR.ElectoralWeight
    , (ks V.++ catCols V.++ PEWCols p) F.⊆ BR.WithYS (ks V.++ catCols V.++ PEWCols p)
    , FI.RecVec (ks V.++ catCols V.++ PEWCols p)
    , (ks V.++ catCols V.++ PEWCols p) F.⊆ (BR.WithYS (ks V.++ catCols V.++ PCols p) V.++ F.RDeleteAll (js V.++ catCols) (js V.++ catCols V.++ BR.EWCols))
    , (js V.++ catCols) F.⊆ ((js V.++ catCols V.++ BR.EWCols))
    , (js V.++ catCols) F.⊆ BR.WithYS (ks V.++ catCols)
    , (js V.++ catCols) F.⊆ BR.WithYS (ks V.++ catCols V.++ (PCols p))
    , (ks V.++ catCols V.++ (PCols p)) F.⊆ BR.WithYS (ks V.++ catCols V.++ (PCols p) V.++ F.RDeleteAll (js V.++ catCols) ((js V.++ catCols) V.++ BR.EWCols))
    , (F.RDeleteAll (js V.++ catCols) ((js V.++ catCols) V.++ BR.EWCols)) F.⊆ ((js V.++ catCols) V.++ BR.EWCols)
    , V.RMap ((ks V.++ catCols) V.++ PCols p)
    , V.RMap ((((ks V.++ catCols) V.++ PCols p) V.++ F.RDeleteAll (js V.++ catCols) ((js V.++ catCols) V.++ BR.EWCols)))
    , V.RecApplicative (F.RDeleteAll (js V.++ catCols) ((js V.++ catCols) V.++ BR.EWCols))
    , G.Grouping (F.Record (js V.++ catCols))
    , FI.RecVec ((ks V.++ catCols) V.++ PCols p)
    , FI.RecVec (F.RDeleteAll (js V.++ catCols) ((js V.++ catCols) V.++ BR.EWCols))
    , FI.RecVec (((ks V.++ catCols) V.++ PCols p) V.++ F.RDeleteAll (js V.++ catCols) ((js V.++ catCols) V.++ BR.EWCols))
    , (ks V.++ catCols V.++ PCols p V.++ BR.EWCols) ~ (ks V.++ catCols V.++ (PCols p V.++ BR.EWCols))
    )
  => BR.PopCountOfT
  -> V.Snd p
  -> F.Frame BR.StateTurnout
  -> F.FrameRec ((BR.WithYS ks) V.++ catCols V.++ (PCols p))
  -> F.FrameRec (js V.++ catCols V.++ BR.EWCols)
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
   , FJ.CanLeftJoinM (js V.++ catCols) ((BR.WithYS ks) V.++ catCols V.++ (PCols p)) (js V.++ catCols V.++ BR.EWCols)
   , (((ks V.++ catCols) V.++ PCols p) V.++ F.RDeleteAll (js V.++ catCols) ((js V.++ catCols) V.++ BR.EWCols)) ~ (((ks V.++ catCols) V.++ PCols p) V.++ BR.EWCols)
   , V.RMap (js V.++ catCols)
   , V.ReifyConstraint Show V.ElField (js V.++ catCols)
   , V.RecordToList (js V.++ catCols)
   , V.KnownField p
   , V.Snd p ~ Int
   , F.ElemOf (ks V.++ catCols V.++ (PEWCols p)) p
   , F.ElemOf (ks V.++ catCols V.++ (PEWCols p)) BR.ElectoralWeight
   , (ks V.++ catCols V.++ (PEWCols p)) F.⊆ BR.WithYS (ks V.++ catCols V.++ (PEWCols p))
   , FI.RecVec (ks V.++ catCols V.++ (PEWCols p))
   , (ks V.++ catCols V.++ (PEWCols p)) F.⊆ BR.WithYS (ks V.++ catCols V.++ (PCols p) V.++ F.RDeleteAll (js V.++ catCols) ((js V.++ catCols V.++ BR.EWCols)))
   , (js V.++ catCols) F.⊆ ((js V.++ catCols V.++ BR.EWCols))
   , (js V.++ catCols) F.⊆ BR.WithYS (ks V.++ catCols)
   , (js V.++ catCols) F.⊆ BR.WithYS (ks V.++ catCols V.++ (PCols p))
   , (ks V.++ catCols V.++ (PCols p)) F.⊆ BR.WithYS (ks V.++ catCols V.++ (PCols p) V.++ F.RDeleteAll (js V.++ catCols) ((js V.++ catCols) V.++ BR.EWCols))
   , (F.RDeleteAll (js V.++ catCols) ((js V.++ catCols) V.++ BR.EWCols)) F.⊆ ((js V.++ catCols) V.++ BR.EWCols)
   , V.RMap ((ks V.++ catCols) V.++ PCols p)
   , V.RMap ((((ks V.++ catCols) V.++ PCols p) V.++ F.RDeleteAll (js V.++ catCols) ((js V.++ catCols) V.++ BR.EWCols)))
   , V.RecApplicative (F.RDeleteAll (js V.++ catCols) ((js V.++ catCols) V.++ BR.EWCols))
   , G.Grouping (F.Record (js V.++ catCols))
   , FI.RecVec ((ks V.++ catCols) V.++ PCols p)
   , FI.RecVec (F.RDeleteAll (js V.++ catCols) ((js V.++ catCols) V.++ BR.EWCols))
   , FI.RecVec (((ks V.++ catCols) V.++ PCols p) V.++ F.RDeleteAll (js V.++ catCols) ((js V.++ catCols) V.++ BR.EWCols))
   , (as V.++ BR.WithYS ks V.++ catCols V.++ PCols p) ~ (as V.++ BR.WithYS (ks V.++ catCols V.++ '[BR.PopCountOf]) V.++ '[p])
   , (((ks V.++ catCols) V.++ '[BR.PopCountOf]) V.++ '[p]) ~ ((ks V.++ catCols) V.++ PCols p)
   , Ord (F.Record ((ks V.++ catCols) V.++ '[BR.PopCountOf]))
   , F.ElemOf  (as V.++ BR.WithYS (ks V.++ catCols V.++ '[BR.PopCountOf]) V.++ '[p]) p
   , F.ElemOf  (as V.++ BR.WithYS (ks V.++ catCols V.++ '[BR.PopCountOf]) V.++ '[p]) BR.StateAbbreviation
   , F.ElemOf  (as V.++ BR.WithYS (ks V.++ catCols V.++ '[BR.PopCountOf]) V.++ '[p]) BR.Year
   , ((ks V.++ catCols) V.++ '[BR.PopCountOf]) F.⊆ (as V.++ BR.WithYS (ks V.++ catCols V.++ '[BR.PopCountOf]) V.++ '[p])
   , FI.RecVec (as V.++ BR.WithYS (ks V.++ catCols V.++ '[BR.PopCountOf]) V.++ '[p] V.++  F.RDeleteAll (ks V.++ catCols) ((ks V.++ catCols) V.++ BR.EWCols))
   , V.AllConstrained G.Grouping (F.UnColumn (ks V.++ catCols))
   , (ks V.++ catCols) F.⊆ (as V.++ BR.WithYS (ks V.++ catCols V.++ '[BR.PopCountOf]) V.++ '[p])
   , (as V.++ BR.WithYS (ks V.++ catCols V.++ '[BR.PopCountOf]) V.++ '[p]) F.⊆ (as V.++ BR.WithYS (ks V.++ catCols V.++ '[BR.PopCountOf]) V.++ '[p] V.++  F.RDeleteAll (ks V.++ catCols) ((ks V.++ catCols) V.++ BR.EWCols))
   , V.RMap (as V.++ BR.WithYS (ks V.++ catCols V.++ '[BR.PopCountOf]) V.++ '[p])
   , V.RMap (as V.++ BR.WithYS (ks V.++ catCols V.++ '[BR.PopCountOf]) V.++ '[p] V.++  F.RDeleteAll (ks V.++ catCols) ((ks V.++ catCols) V.++ BR.EWCols))
   , G.Grouping (F.Record (ks V.++ catCols))
   , FI.RecVec (as V.++ BR.WithYS (ks V.++ catCols V.++ '[BR.PopCountOf]) V.++ '[p])
   , (ks V.++ catCols V.++ PCols p V.++ BR.EWCols) ~ (ks V.++ catCols V.++ (PCols p V.++ BR.EWCols))
   , (as V.++ BR.WithYS (ks V.++ catCols V.++ '[BR.PopCountOf]) V.++ '[p] V.++ BR.EWCols)
    ~ ((as V.++ BR.WithYS (ks V.++ catCols V.++ '[BR.PopCountOf]) V.++ '[p]) V.++ F.RDeleteAll (ks V.++ catCols) (ks V.++ catCols V.++ BR.EWCols))
   , (ks V.++ catCols V.++ BR.EWCols) F.⊆ BR.WithYS (ks V.++ catCols V.++ PEWCols p)
   , (ks V.++ catCols) F.⊆ BR.WithYS (ks V.++ catCols V.++ BR.EWCols)
   , (F.RDeleteAll (ks V.++ catCols) ((ks V.++ catCols) V.++ BR.EWCols)) F.⊆ BR.WithYS (ks V.++ catCols V.++ BR.EWCols)
   , V.RecApplicative (F.RDeleteAll (ks V.++ catCols) ((ks V.++ catCols) V.++ BR.EWCols))
   , FI.RecVec (F.RDeleteAll (ks V.++ catCols) ((ks V.++ catCols) V.++ BR.EWCols))
   )
  => F.Frame BR.StateTurnout
  -> F.FrameRec (as V.++ (BR.WithYS ks) V.++ catCols V.++ (PCols p))
  -> F.FrameRec (js V.++ catCols V.++ BR.EWCols)
  -> K.Sem effs (F.FrameRec (as V.++ (BR.WithYS ks) V.++ catCols V.++ PCols p V.++ BR.EWCols))
rollupAdjustAndJoin stateTurnout demos ews = do
  let rolledUpDemos :: F.FrameRec ((BR.WithYS ks) V.++ catCols V.++ (PCols p))
      rolledUpDemos = FL.fold (rollupSumF @as @((BR.WithYS ks) V.++ catCols V.++ '[BR.PopCountOf]) @'[p]) demos
  adjusted :: F.FrameRec (BR.WithYS ks V.++ catCols V.++ PEWCols p) <- demographicsWithAdjTurnoutByState @catCols @p @ks @js  stateTurnout rolledUpDemos ews
  let adjustedWithoutDemo = fmap (F.rcast @(BR.WithYS ks V.++ catCols V.++ BR.EWCols)) adjusted
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
    , V.RMap catCols
    , V.ReifyConstraint Show V.ElField catCols
    , V.RecordToList catCols
    , ((catCols V.++ PCols BR.ACSCount) V.++ BR.EWCols) ~ (catCols V.++ PEWCols BR.ACSCount)
    , ((catCols V.++ '[BR.PopCountOf]) V.++ '[BR.ACSCount]) ~ (catCols V.++ [BR.PopCountOf, BR.ACSCount])
    , ((catCols V.++ PCols BR.ACSCount) V.++ F.RDeleteAll catCols (catCols V.++ BR.EWCols)) ~ (catCols V.++ PEWCols BR.ACSCount)
    , FS.RecSerialize (BR.ACSKeys V.++ catCols V.++ '[BR.ACSCount, BR.VotedPctOfAll])
    , V.RMap (catCols V.++ '[BR.ACSCount, BR.VotedPctOfAll])
    , FI.RecVec (catCols V.++ '[BR.ACSCount, BR.VotedPctOfAll])
    , (catCols V.++ PCols BR.ACSCount) F.⊆ (BR.ACSKeys V.++ (catCols V.++ '[BR.ACSCount]) V.++ '[BR.PopCountOf])
    , (catCols V.++ BR.EWCols) F.⊆ (BR.Year : (catCols V.++ TurnoutCols V.++ BR.EWCols))
    , F.ElemOf (catCols V.++ TurnoutCols) BR.Population
    , F.ElemOf (catCols V.++ TurnoutCols) BR.Voted
    , V.AllConstrained G.Grouping (F.UnColumn catCols)
    , V.RMap (catCols V.++ PCols BR.ACSCount)
    , V.RMap (catCols V.++ PEWCols BR.ACSCount)
    , V.RecApplicative (F.RDeleteAll catCols (catCols V.++ BR.EWCols))
    , G.Grouping (F.Record catCols)
    , FI.RecVec (catCols V.++ PEWCols BR.ACSCount)
    , FI.RecVec (catCols V.++ PCols BR.ACSCount)
    , FI.RecVec (F.RDeleteAll catCols (catCols V.++ BR.EWCols))
    , Ord (F.Record (catCols V.++ '[BR.PopCountOf]))
    , F.ElemOf (catCols V.++ PEWCols BR.ACSCount) BR.ACSCount
    , F.ElemOf (catCols V.++ PCols BR.ACSCount) BR.ACSCount
    , F.ElemOf (catCols V.++ PEWCols BR.ACSCount) BR.ElectoralWeight
    , (catCols V.++ PEWCols BR.ACSCount) F.⊆ (ACSCols V.++ catCols V.++ PEWCols BR.ACSCount)
    , (catCols V.++ '[BR.PopCountOf]) F.⊆ (ACSColsCD V.++ catCols V.++ PCols BR.ACSCount)
    , (catCols V.++ PCols BR.ACSCount) F.⊆ (ACSCols V.++ catCols V.++ PEWCols BR.ACSCount)
    , (catCols V.++ PCols BR.ACSCount) F.⊆ (ACSColsCD V.++ catCols V.++ PEWCols BR.ACSCount)
    , (catCols V.++ BR.EWCols) F.⊆ (ACSCols V.++ catCols V.++ PEWCols BR.ACSCount)
    , catCols F.⊆ (ACSColsCD V.++ catCols V.++ PCols BR.ACSCount)
    , catCols F.⊆ (BR.Year ': (catCols V.++ BR.EWCols))
    , catCols F.⊆ (ACSCols V.++ catCols)
    , catCols F.⊆ (ACSCols V.++ catCols V.++ PCols BR.ACSCount)
    , catCols F.⊆ (ACSCols V.++ catCols V.++ BR.EWCols)
    , (F.RDeleteAll catCols (catCols V.++ BR.EWCols))  F.⊆ (BR.Year ': (catCols V.++ BR.EWCols))
    , (F.RDeleteAll catCols (catCols V.++ BR.EWCols))  F.⊆ (ACSCols V.++ (catCols V.++ BR.EWCols))
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
    let turnout' = fmap (F.rcast @('[BR.Year] V.++ catCols V.++ BR.EWCols) . addElectoralWeight BR.EW_Census BR.EW_All vpa) turnout
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

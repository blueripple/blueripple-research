{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}

module Visualizations.StatePrefs where

import qualified Control.Foldl                 as FL
import qualified Frames                        as F
import qualified Data.Text                     as T
import qualified Data.List                     as L

import qualified Frames.Visualization.VegaLite.Data
                                               as FV
import qualified Graphics.Vega.VegaLite.Configuration
                                               as FV
import qualified Graphics.Vega.VegaLite.Compat as FV

import qualified Graphics.Vega.VegaLite        as GV
import           MRP.DeltaVPV                   ( DemVPV
                                                , DemPref
                                                )

import qualified BlueRipple.Data.DemographicTypes
                                               as BR
import qualified BlueRipple.Data.DataFrames    as BR
                                         hiding ( Office )
import qualified MRP.CCES                      as BR

-- Voter Pref vs time.  House vs. Prez, State vs. National.  Facet by type of voter.
vlPrefVsTime
  :: Foldable f
  => T.Text -- title
  -> T.Text -- state abbreviation
  -> FV.ViewConfig
  -> f
       ( F.Record
           '[BR.StateAbbreviation, BR.Office, BR.Year, BR.SimpleAgeC, BR.SexC, BR.CollegeGradC, BR.SimpleRaceC, DemPref]
       )
  -> GV.VegaLite
vlPrefVsTime title stateAbbr vc@(FV.ViewConfig w h _) rows
  = let
      rowFilter r =
        let sa = F.rgetField @BR.StateAbbreviation r
        in  (sa == stateAbbr) || (sa == "National")
      fRows = L.filter rowFilter $ FL.fold FL.list rows
      dat = FV.recordsToVLData id FV.defaultParse fRows
      encX = GV.position GV.X [FV.pName @BR.Year, GV.PmType GV.Ordinal]
      encY = GV.position GV.Y [FV.pName @BR.DemPref, GV.PmType GV.Quantitative]
      addRegionOffice = GV.calculateAs
        "datum.state_abbreviation + '-' + datum.Office"
        "Region-Office"
      encColor = GV.color [GV.MName "Region-Office", GV.MmType GV.Nominal]
      encDetail = GV.detail [GV.DName "Region-Office", GV.DmType GV.Nominal]
      addDemographic = GV.calculateAs
        "(datum.SimpleAge == 'Under' ? 'Under 45' : '45 And Over') + '-' + datum.Sex +'-' + datum.CollegeGrad + '-' + datum.SimpleRace"
        "Demographic"
      linesSpec = GV.asSpec
        [ (GV.encoding . encX . encY . encColor . encDetail) []
        , GV.mark GV.Line []
        , GV.width (w / 4)
        , GV.height (h / 4)
        , (GV.transform . addRegionOffice) []
        ]
      facet = GV.facetFlow [GV.FName "Demographic"]
    in
      FV.configuredVegaLite
        vc
        [ FV.title title
        , GV.specification linesSpec
        , facet
        , (GV.transform . addDemographic) []
        , dat
        , GV.columns 4
        ]


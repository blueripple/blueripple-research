{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}
{-# LANGUAGE TypeOperators       #-}
module Visualizations.StatePrefs where

import qualified Control.Foldl                 as FL
import qualified Frames                        as F
import qualified Frames.Melt                   as F
import qualified Frames.Transform              as FT
--import           Data.Maybe                     ( maybe )
import qualified Data.Text                     as T
import qualified Data.List                     as L
import qualified Data.Vinyl                    as V

import qualified Frames.Visualization.VegaLite.Data
                                               as FV
import qualified Graphics.Vega.VegaLite.Configuration
                                               as FV
import qualified Graphics.Vega.VegaLite.Compat as FV

import qualified Graphics.Vega.VegaLite        as GV
import           MRP.DeltaVPV                   ( DemPref
                                                )

import qualified BlueRipple.Data.DemographicTypes
                                               as BR
import qualified BlueRipple.Data.ElectionTypes as ET
import qualified BlueRipple.Data.DataFrames    as BR
                                         hiding ( Office )
--import qualified MRP.CCES                      as BR

-- Voter Pref vs time.  House vs. Prez, State vs. National.  Facet by type of voter.
vlPrefVsTime
  :: Foldable f
  => T.Text -- title
  -> T.Text -- state abbreviation
  -> FV.ViewConfig
  -> f
       ( F.Record
           '[BR.StateAbbreviation, ET.Office, BR.Year, BR.SimpleAgeC, BR.SexC, BR.CollegeGradC, BR.SimpleRaceC, DemPref]
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

--stateCDsGeoJSON :: T.Text -> T.Text
--stateCDsGeoJSON stateAbbr = "https://theunitedstates.io/districts/states/" <> stateAbbr <> "/shape.geojson"

usDistrictsTopoJSONUrl
  = "https://raw.githubusercontent.com/blueripple/data-sets/master/data/geo/cd116_2018.topojson"

type DistrictGeoId = "DistrictGeoId" F.:-> T.Text
addDistrictGeoId
  :: (F.ElemOf rs BR.StateFIPS, F.ElemOf rs BR.CongressionalDistrict)
  => F.Record rs
  -> F.Record (DistrictGeoId ': rs)
addDistrictGeoId r =
  let
    sf       = F.rgetField @BR.StateFIPS r
    cd       = F.rgetField @BR.CongressionalDistrict r
    dgidInt  = 100 * sf + cd
    dgidText = (if dgidInt < 1000 then "0" else "") <> (T.pack $ show dgidInt)
  in
    (FT.recordSingleton @DistrictGeoId dgidText) `V.rappend` r

vpvChoroColorScale :: Double -> Double -> GV.MarkChannel
vpvChoroColorScale lo hi =
  GV.MScale [GV.SScheme "redblue" [], GV.SDomain (GV.DNumbers [lo, hi])]

vlByCD
  :: forall c f rs
   . ( F.ElemOf rs c
     , F.ElemOf rs BR.StateFIPS
     , F.ElemOf rs BR.CongressionalDistrict
     , Foldable f
     , Functor f
     , V.RMap rs
     , V.ReifyConstraint FV.ToVLDataValue V.ElField rs
     , V.RecordToList rs
     , F.ColumnHeaders '[c]
     )
  => T.Text -- title
  -> Maybe GV.MarkChannel -- color spec
  -> FV.ViewConfig
  -> f (F.Record rs)
  -> GV.VegaLite
vlByCD title colorScaleM vc rows =
  let datGeo =
        GV.dataFromUrl usDistrictsTopoJSONUrl [GV.TopojsonFeature "cd116"]
      projection     = GV.projection [GV.PrType GV.AlbersUsa]
      rowsWithGeoIds = fmap addDistrictGeoId rows
      datVal         = FV.recordsToVLData id FV.defaultParse rowsWithGeoIds
      lookup = GV.lookupAs "DistrictGeoId" datGeo "properties.GEOID" "geo"
      mark           = GV.mark GV.Geoshape []
      shapeEnc       = GV.shape [GV.MName "geo", GV.MmType GV.GeoFeature]
      colorEnc =
        GV.color
          $  [FV.mName @c, GV.MmType GV.Quantitative]
          ++ (maybe [] (pure @[]) colorScaleM)

      enc = GV.encoding . shapeEnc . colorEnc
  in  FV.configuredVegaLite
        vc
        [ FV.title title
        , datVal
        , (GV.transform . lookup) []
        , enc []
        , mark
        , projection
        ]


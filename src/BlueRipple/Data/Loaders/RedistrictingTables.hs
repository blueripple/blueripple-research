{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE Rank2Types #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}

module BlueRipple.Data.Loaders.RedistrictingTables where

import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.ElectionTypes as ET
import BlueRipple.Data.LoadersCore
import qualified BlueRipple.Utilities.KnitUtils as BR
import qualified Control.Foldl as FL
import Control.Lens ((%~))
import qualified Data.Map as M
import qualified Data.Set as S
import qualified Data.Sequence as Sequence
import Data.Serialize.Text ()
import qualified Data.Text as T
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V
import qualified Frames as F
import qualified Frames.Streamly.LoadInCore as FS
import qualified Frames.Streamly.TH as FS
import qualified Frames.Melt as F
import qualified Frames.MaybeUtils as FM
import qualified Frames.Serialize as FS
import qualified Frames.Transform as FT
import qualified Knit.Report as K


redistrictingPath :: Text
redistrictingPath = "data/redistricting"

type PlanName = "PlanName" F.:-> Text

type RedistrictingPlanIdR = [DT.StateAbbreviation, PlanName, ET.DistrictTypeC]
type RedistrictingPlanId = F.Record RedistrictingPlanIdR
data RedistrictingPlanFiles = RedistrictingPlanFiles { districtDemographicsFP :: Text, districtAnalysisFP :: Text } deriving (Show)

redistrictingPlanId :: Text -> Text -> ET.DistrictType -> RedistrictingPlanId
redistrictingPlanId sa name dt = sa F.&: name F.&: dt F.&: V.RNil

plans :: Map RedistrictingPlanId RedistrictingPlanFiles
plans = M.fromList
  [
    (redistrictingPlanId "NC" "CST-13" ET.Congressional, RedistrictingPlanFiles "../bigData/Census/cd117_NC.csv" "data/redistricting/NC-CST-13.csv")
  , (redistrictingPlanId "NC" "Passed" ET.Congressional, RedistrictingPlanFiles "../bigData/Census/cd117_NC.csv" "data/redistricting/NC-CST-13.csv")
  , (redistrictingPlanId "TX" "Passed" ET.Congressional, RedistrictingPlanFiles "../bigData/Census/cd117_TX.csv" "data/redistricting/TX-proposed.csv")
  ]

redistrictingAnalysisCols :: Set FS.HeaderText
redistrictingAnalysisCols = S.fromList $ FS.HeaderText <$> ["ID","Total Pop","Dem","Rep","Oth","Total VAP","White","Minority","Hispanic","Black","Asian"]

redistrictingAnalysisRenames :: Map FS.HeaderText FS.ColTypeName
redistrictingAnalysisRenames = M.fromList [(FS.HeaderText "ID", FS.ColTypeName "DistrictNumber")
                                          ,(FS.HeaderText "Total Pop", FS.ColTypeName "Population")
                                          ,(FS.HeaderText "Total VAP", FS.ColTypeName "VAP")
                                          ,(FS.HeaderText "Dem", FS.ColTypeName "DemShare")
                                          ,(FS.HeaderText "Rep", FS.ColTypeName "RepShare")
                                          ,(FS.HeaderText "Oth", FS.ColTypeName "OthShare")
                                          ,(FS.HeaderText "White", FS.ColTypeName "WhiteFrac")
                                          ,(FS.HeaderText "Minority", FS.ColTypeName "MinorityFrac")
                                          ,(FS.HeaderText "Hispanic", FS.ColTypeName "HispanicFrac")
                                          ,(FS.HeaderText "Black", FS.ColTypeName "BlackFrac")
                                          ,(FS.HeaderText "Asian", FS.ColTypeName "AsianFrac")
                                          ]

-- this assumes these files are all like this one
redistrictingAnalysisRowGen = FS.modifyColumnSelector modF rg where
  rg = (FS.rowGen "data/redistricting/NC-CST-13.csv") { FS.tablePrefix = ""
                                                      , FS.separator = FS.CharSeparator ','
                                                      , FS.rowTypeName = "DRAnalysisRaw"
                                                      }
  modF = FS.renameSomeUsingNames redistrictingAnalysisRenames
         . FS.columnSubset redistrictingAnalysisCols

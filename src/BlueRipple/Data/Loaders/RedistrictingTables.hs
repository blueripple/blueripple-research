{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE Rank2Types #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}

module BlueRipple.Data.Loaders.RedistrictingTables
  (
    module BlueRipple.Data.Loaders.RedistrictingTables
  )
where

import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.ElectionTypes as ET
import qualified BlueRipple.Data.Loaders as BR
import BlueRipple.Data.CensusLoaders (noMaps)
import qualified BlueRipple.Utilities.KnitUtils as BR
import qualified Control.Foldl as FL
import qualified Data.Map as M
import qualified Data.Set as S
import Data.Serialize.Text ()
import qualified Data.Vinyl as V
import qualified Frames as F
import qualified Frames.Streamly.TH as FS
import qualified Frames.Streamly.ColumnUniverse as FCU
import qualified Knit.Report as K


redistrictingPath :: Text
redistrictingPath = "data/redistricting"

type PlanName = "PlanName" F.:-> Text

type RedistrictingPlanIdR = [DT.StateAbbreviation, PlanName, ET.DistrictTypeC]
type RedistrictingPlanId = F.Record RedistrictingPlanIdR
data RedistrictingPlanFiles = RedistrictingPlanFiles { districtDemographicsFP :: Text, districtAnalysisFP :: Text } deriving stock Show

redistrictingPlanId :: Text -> Text -> ET.DistrictType -> RedistrictingPlanId
redistrictingPlanId sa name dt = sa F.&: name F.&: dt F.&: V.RNil

{-
plans :: Map RedistrictingPlanId RedistrictingPlanFiles
plans = M.fromList
  [
    (redistrictingPlanId "NC" "Passed" ET.Congressional, RedistrictingPlanFiles "../bigData/Census/cd117_NC.csv" "data/redistricting/NC_congressional.csv")
  , (redistrictingPlanId "NC" "Passed" ET.StateUpper, RedistrictingPlanFiles "../bigData/Census/nc_2022_sldu.csv" "data/redistricting/nc_2022_sldu.csv")
  , (redistrictingPlanId "NC" "Passed" ET.StateLower, RedistrictingPlanFiles "../bigData/Census/nc_2022_sldl.csv" "data/redistricting/nc_2022_sldl.csv")
  , (redistrictingPlanId "TX" "Passed" ET.Congressional, RedistrictingPlanFiles "../bigData/Census/cd117_TX.csv" "data/redistricting/TX-proposed.csv")
  , (redistrictingPlanId "AZ" "Passed" ET.Congressional, RedistrictingPlanFiles "../bigData/Census/az_cd117.csv" "data/redistricting/az_congressional.csv")
  , (redistrictingPlanId "AZ" "Passed" ET.StateUpper, RedistrictingPlanFiles "../bigData/Census/az_sld.csv" "data/redistricting/az_SLD.csv")
  ]
-}

allPassedCongressionalPlans ::  (K.KnitEffects r, BR.CacheEffects r) => K.Sem r  (Map RedistrictingPlanId RedistrictingPlanFiles)
allPassedCongressionalPlans = do
  stateInfo <- K.ignoreCacheTimeM BR.stateAbbrCrosswalkLoader
  let states = FL.fold (FL.premap (F.rgetField @BR.StateAbbreviation) FL.list)
               $ F.filterFrame (\r -> (F.rgetField @BR.StateFIPS r < 60)
                                 && not (F.rgetField @BR.OneDistrict r)
                                 && not (F.rgetField @BR.StateAbbreviation r `S.member` noMaps)
                               ) stateInfo
      planId sa = redistrictingPlanId sa "Passed" ET.Congressional
      planFiles sa = RedistrictingPlanFiles ("../bigData/Census/cd117_" <> sa <> ".csv") ("data/redistricting/" <> sa <> "_congressional.csv")
      mapPair sa = (planId sa, planFiles sa)
  return $ M.fromList $ fmap mapPair states

allPassedSLDPlans :: (K.KnitEffects r, BR.CacheEffects r) => K.Sem r  (Map RedistrictingPlanId RedistrictingPlanFiles)
allPassedSLDPlans = do
  stateInfo <- K.ignoreCacheTimeM BR.stateAbbrCrosswalkLoader
  let statesAnd = FL.fold (FL.premap (\r -> (F.rgetField @BR.StateAbbreviation r, F.rgetField @BR.SLDUpperOnly r)) FL.list)
                  $ F.filterFrame (\r -> (F.rgetField @BR.StateFIPS r < 60)
                                         && not (F.rgetField @BR.StateAbbreviation r `S.member` S.insert "DC" noMaps)
                                  ) stateInfo
      planUpper sa = redistrictingPlanId sa "Passed" ET.StateUpper
      planUpperFiles sa = RedistrictingPlanFiles ("../bigData/Census/" <> sa <> "_2022_sldu.csv") ("data/redistricting/" <> sa <> "_sldu.csv")
      mapPairUpper sa = (planUpper sa, planUpperFiles sa)
      planLower sa = redistrictingPlanId sa "Passed" ET.StateLower
      planLowerFiles sa = RedistrictingPlanFiles ("../bigData/Census/" <> sa <> "_2022_sldl.csv") ("data/redistricting/" <> sa <> "_sldl.csv")
      mapPairLower sa = (planLower sa, planLowerFiles sa)
      mapPairs (sa, upperOnly) = mapPairUpper sa : if upperOnly then [] else [mapPairLower sa]
  return $ M.fromList $ concat $ fmap mapPairs statesAnd

redistrictingAnalysisCols :: Set FS.HeaderText
redistrictingAnalysisCols = S.fromList $ FS.HeaderText <$> ["ID","Total Pop","Dem","Rep","Oth","Total VAP","White","Minority","Hispanic","Black","Asian"]

redistrictingAnalysisRenames :: Map FS.HeaderText FS.ColTypeName
redistrictingAnalysisRenames = M.fromList [(FS.HeaderText "ID", FS.ColTypeName "DistrictName")
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
redistrictingAnalysisRowGen :: FS.RowGen FS.DefaultStream 'FS.ColumnByName FCU.CommonColumns
redistrictingAnalysisRowGen = FS.modifyColumnSelector modF rg where
  rg = (FS.rowGen "data/redistricting/NC-CST-13.csv") { FS.tablePrefix = ""
                                                      , FS.separator = FS.CharSeparator ','
                                                      , FS.rowTypeName = "DRAnalysisRaw"
                                                      }
  modF = FS.renameSomeUsingNames redistrictingAnalysisRenames
         . FS.columnSubset redistrictingAnalysisCols

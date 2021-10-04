{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
module BlueRipple.Data.CCESPath where

import qualified Frames.Streamly.TH                     as FS
import qualified Frames.Streamly.TH as FS

import qualified Data.Set as S
import qualified Data.Map as M
--import BlueRipple.Data.Loaders (presidentialElectionsWithIncumbency, presidentialByStateFrame)


dataDir :: FilePath
dataDir = "../bigData/CCES/"

cces2018C_CSV :: FilePath = dataDir ++ "CCES_cumulative_2006_2018.csv"
cces2020C_CSV :: FilePath = dataDir ++ "CES_cumulative_2006-2020.csv"

ccesCols2018C :: S.Set FS.HeaderText
ccesCols2018C = S.fromList (FS.HeaderText <$> ["year"
                                              , "case_id"
                                              , "weight"
                                              , "weight_cumulative"
                                              , "st"
                                              , "dist_up"
                                              , "gender"
                                              , "age"
                                              , "educ"
                                              , "race"
                                              , "hispanic"
                                              , "pid3"
                                              , "pid7"
                                              , "pid3_leaner"
                                              , "vv_regstatus"
                                              , "vv_turnout_gvm"
                                              , "voted_rep_party"
                                              , "voted_pres_08"
                                              , "voted_pres_12"
                                              , "voted_pres_16"
                                              ])
ccesCols2020C :: S.Set FS.HeaderText
ccesCols2020C = S.insert (FS.HeaderText "voted_pres_20") ccesCols2018C

-- the things I would make Categorical are already ints. :(
ccesRowGen2020CAllCols = (FS.rowGen cces2020C_CSV) { FS.tablePrefix = "CCES"
                                                   , FS.separator   = ","
                                                   , FS.rowTypeName = "CCES"
                                                   }

ccesRowGen2018CAllCols = (FS.rowGen cces2018C_CSV) { FS.tablePrefix = "CCES"
                                                   , FS.separator   = ","
                                                   , FS.rowTypeName = "CCES"
                                                   }

ccesRowGen2018C = FS.modifyColumnSelector colSubset ccesRowGen2018CAllCols where
  colSubset = FS.columnSubset ccesCols2018C

ccesRowGen2020C = FS.modifyColumnSelector colSubset ccesRowGen2020CAllCols where
  colSubset = FS.columnSubset ccesCols2020C


ces2020CSV :: FilePath = dataDir ++ "CES20_Common_OUTPUT_vv.csv"

ces2020Cols :: S.Set FS.HeaderText
ces2020Cols = S.fromList (FS.HeaderText <$> ["caseid"
                                           , "commonweight"
                                           , "vvweight"
                                           , "inputstate"
                                           , "cdid116"
                                           , "gender"
                                           , "birthyr"
                                           , "educ"
                                           , "race"                                           , "hispanic"
                                           , "pid3"
                                           , "pid7"
                                           , "CL_voter_status" -- registration, Catalist
                                           , "CL_2020gvm" -- how voted and thus turnout, Catalist
                                           , "CC20_410" -- 2020 pres vote
                                           , "CC20_412_t" -- 2020 house vote party (?)
                                           , "HouseCand1Party"
                                           , "HouseCand2Party"
                                           , "HouseCand3Party"
                                           , "HouseCand4Party"
                                           ])

ces2020Renames :: Map FS.HeaderText FS.ColTypeName
ces2020Renames = M.fromList [ (FS.HeaderText "caseid", FS.ColTypeName "CaseId")
                            , (FS.HeaderText "commonweight", FS.ColTypeName "Weight")
                            , (FS.HeaderText "vvweight", FS.ColTypeName "RegisteredWeight")
                            , (FS.HeaderText "inputstate", FS.ColTypeName "StateFips")
                            , (FS.HeaderText "cdid116", FS.ColTypeName "CD")
                            , (FS.HeaderText "CL_2020gvm", FS.ColTypeName "CTurnout")
                            , (FS.HeaderText "CC20_412", FS.ColTypeName "HouseVote")
                            , (FS.HeaderText "CC20_410", FS.ColTypeName "PresVote2020")
                            ]

ccesRowGen2020AllCols = (FS.rowGen ces2020CSV) { FS.tablePrefix = "CES"
                                               , FS.separator   = ","
                                               , FS.rowTypeName = "CES20"
                                               }

cesRowGen2020 = FS.modifyColumnSelector modF ccesRowGen2020AllCols where
  modF = FS.renameSomeUsingNames ces2020Renames . FS.columnSubset ces2020Cols

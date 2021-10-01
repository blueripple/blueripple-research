{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
module BlueRipple.Data.CCESPath where

import qualified Frames.Streamly.TH                     as FS
import qualified Frames.Streamly.TH as FS

import qualified Data.Set as S
--import BlueRipple.Data.Loaders (presidentialElectionsWithIncumbency, presidentialByStateFrame)


dataDir :: FilePath
dataDir = "../bigData/CCES/"

cces2018CSV :: FilePath = dataDir ++ "CCES_cumulative_2006_2018.csv"
cces2020CSV :: FilePath = dataDir ++ "CES_cumulative_2006-2020.csv"

ccesCols2018 :: S.Set FS.HeaderText
ccesCols2018 = S.fromList (FS.HeaderText <$> ["year"
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
ccesCols2020 :: S.Set FS.HeaderText
ccesCols2020 = S.insert (FS.HeaderText "voted_pres_20") ccesCols2018

-- the things I would make Categorical are already ints. :(
ccesRowGen2020AllCols = (FS.rowGen cces2020CSV) { FS.tablePrefix = "CCES"
                                                , FS.separator   = ","
                                                , FS.rowTypeName = "CCES"
                                                }

ccesRowGen2018AllCols = (FS.rowGen cces2018CSV) { FS.tablePrefix = "CCES"
                                                , FS.separator   = ","
                                                , FS.rowTypeName = "CCES"
                                                }
ccesRowGen2018 = FS.modifyColumnSelector colSubset ccesRowGen2018AllCols where
  colSubset = FS.columnSubset ccesCols2018

ccesRowGen2020 = FS.modifyColumnSelector colSubset ccesRowGen2020AllCols where
  colSubset = FS.columnSubset ccesCols2020

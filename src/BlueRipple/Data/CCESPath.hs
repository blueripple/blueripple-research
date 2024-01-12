{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module BlueRipple.Data.CCESPath
  (
    module BlueRipple.Data.CCESPath
  )
where

import qualified Frames.Streamly.TH                     as FS
import qualified Frames.Streamly.ColumnUniverse         as FCU

import qualified Data.Set as S
import qualified Data.Map as M
--import BlueRipple.Data.Loaders (presidentialElectionsWithIncumbency, presidentialByStateFrame)

data ValidationSource = Catalist | TargetSmart deriving stock (Show, Eq)
data ElectionYearType = Presidential Text | Midterm

data CESConfig = CESConfig
                 {
                   configFP :: FilePath
                 , configVS :: ValidationSource
                 , configEYT :: ElectionYearType
                 , configCongress :: Int
                 , configYrSuffix :: Int
                 , configCLAsInt :: Bool
--                 , configCols :: S.Set FS.HeaderText
--                 , congigColRenames :: Map FS.HeaderText FS.ColTypeName
                 }
data CESYear = CES2016 | CES2018 | CES2020 | CES2022 deriving (Show, Eq)

cesConfig :: CESYear -> CESConfig
cesConfig CES2016 = CESConfig ces2016CSV Catalist (Presidential "CC16_410a") 115 16 False
cesConfig CES2018 = CESConfig ces2018CSV Catalist Midterm 116 18 True
cesConfig CES2020 = CESConfig ces2020CSV Catalist (Presidential "CC20_410") 116 20 True
cesConfig CES2022 = CESConfig ces2022CSV TargetSmart Midterm 118 22 True

cols :: CESYear -> S.Set FS.HeaderText
cols cy =
  let config = cesConfig cy
      yrSuffix = configYrSuffix config
  in commonCols yrSuffix (configCongress config)
        <> case (configVS config) of
             Catalist -> catalistCols yrSuffix
             TargetSmart -> targetSmartCols yrSuffix

renames :: CESYear -> Map FS.HeaderText FS.ColTypeName
renames cy =
  let config = cesConfig cy
      yrSuffix = configYrSuffix config
      clAsInt = configCLAsInt config
  in commonRenames clAsInt yrSuffix (configCongress config)
     <> case (configVS config) of
          Catalist -> catalistRenames clAsInt yrSuffix
          TargetSmart -> targetSmartRenames clAsInt yrSuffix

baseColsAndRenames :: CESYear -> (S.Set FS.HeaderText, Map FS.HeaderText FS.ColTypeName)
baseColsAndRenames cy = case (configEYT $ cesConfig cy) of
  Presidential presVoteHeader -> addPresVote (FS.HeaderText presVoteHeader) (cols cy) (renames cy)
  Midterm -> (cols cy, renames cy)

colsAndRenames :: CESYear -> (S.Set FS.HeaderText, Map FS.HeaderText FS.ColTypeName)
colsAndRenames CES2016 = f $ baseColsAndRenames CES2016 where
  f = unRenameHeader (FS.HeaderText "caseid") (FS.HeaderText "V101") .
      unRenameHeader (FS.HeaderText "CL_voter_status") (FS.HeaderText "CL_voterstatus") .
      unRenameHeader (FS.HeaderText "commonpostweight") (FS.HeaderText "commonweight_post") .
      unRenameHeader (FS.HeaderText "CL_2016gvm") (FS.HeaderText "CL_E2016GVM")
colsAndRenames CES2018 = baseColsAndRenames CES2018
colsAndRenames CES2020 = baseColsAndRenames CES2020
colsAndRenames CES2022 = baseColsAndRenames CES2022

cesRowGen2022 :: FS.RowGen FS.DefaultStream 'FS.ColumnByName FCU.CommonColumns
cesRowGen2022 = FS.modifyColumnSelector modF ccesRowGen2022AllCols where
  (cols, renames) =  colsAndRenames CES2022
  modF = FS.renameSomeUsingNames renames . FS.columnSubset cols

cesRowGen2020 :: FS.RowGen FS.DefaultStream 'FS.ColumnByName FCU.CommonColumns
cesRowGen2020 = FS.modifyColumnSelector modF ccesRowGen2020AllCols where
  (cols, renames) = colsAndRenames CES2020
  modF = FS.renameSomeUsingNames renames . FS.columnSubset cols

cesRowGen2018 :: FS.RowGen FS.DefaultStream 'FS.ColumnByName FCU.CommonColumns
cesRowGen2018 = FS.modifyColumnSelector modF ccesRowGen2018AllCols where
  (cols, renames) = colsAndRenames CES2018
  modF = FS.renameSomeUsingNames renames . FS.columnSubset cols

cesRowGen2016 :: FS.RowGen FS.DefaultStream 'FS.ColumnByName FCU.CommonColumns
cesRowGen2016 = FS.modifyColumnSelector modF ccesRowGen2016AllCols where
  (cols, renames) = colsAndRenames CES2016
  modF = FS.renameSomeUsingNames renames . FS.columnSubset cols

ccesRowGen2022AllCols :: FS.RowGen FS.DefaultStream 'FS.ColumnByName FCU.CommonColumns
ccesRowGen2022AllCols = (FS.rowGen ces2022CSV) { FS.tablePrefix = "CES"
                                               , FS.separator   = FS.CharSeparator ','
                                               , FS.rowTypeName = "CES22"
                                               }

ccesRowGen2020AllCols :: FS.RowGen FS.DefaultStream 'FS.ColumnByName FCU.CommonColumns
ccesRowGen2020AllCols = (FS.rowGen ces2020CSV) { FS.tablePrefix = "CES"
                                               , FS.separator   = FS.CharSeparator ','
                                               , FS.rowTypeName = "CES20"
                                               }

ccesRowGen2018AllCols :: FS.RowGen FS.DefaultStream 'FS.ColumnByName FCU.CommonColumns
ccesRowGen2018AllCols = (FS.rowGen ces2018CSV) { FS.tablePrefix = "CES"
                                               , FS.separator   = FS.CharSeparator ','
                                               , FS.rowTypeName = "CES18"
                                               }

ccesRowGen2016AllCols :: FS.RowGen FS.DefaultStream 'FS.ColumnByName FCU.CommonColumns
ccesRowGen2016AllCols = (FS.rowGen ces2016CSV) { FS.tablePrefix = "CES"
                                               , FS.separator   = FS.CharSeparator '\t'
                                               , FS.rowTypeName = "CES16"
                                               }

dataDir :: FilePath
dataDir = "../bigData/CCES/"

ces2016CSV :: FilePath
ces2016CSV = dataDir ++ "CCES16_Common_OUTPUT_Feb2018_VV.tab"

ces2018CSV :: FilePath
ces2018CSV = dataDir ++ "cces18_common_vv.csv"

ces2020CSV :: FilePath
ces2020CSV = dataDir ++ "CES20_Common_OUTPUT_vv.csv"

ces2022CSV :: FilePath
ces2022CSV = dataDir ++ "CCES22_Common_OUTPUT_vv_topost.csv"

commonCols :: Int -> Int -> S.Set FS.HeaderText
commonCols yrSuffix congress =
  S.fromList (FS.HeaderText <$> ["caseid"
                                , "commonpostweight"
                                , "inputstate"
                                , "cdid" <> show congress
                                , "gender"
                                , "birthyr"
                                , "educ"
                                , "race"
                                , "hispanic"
                                , "pew_bornagain"
                                , "pid3"
                                , "pid7"
                                , "CC" <> show yrSuffix <> "_412" -- house candidate
                                , "HouseCand1Party"
                                , "HouseCand2Party"
                                , "HouseCand3Party"
                                , "HouseCand4Party"
                                ])

catalistCols :: Int -> S.Set FS.HeaderText
catalistCols yrSuffix =
  S.fromList (FS.HeaderText <$>
              [ "CL_voter_status" -- registration, Catalist
              , "CL_20" <> show yrSuffix <> "gvm" -- how voted and thus turnout, Catalist
              ]
             )

catalistRenames :: Bool -> Int ->  Map FS.HeaderText FS.ColTypeName
catalistRenames clAsInt yrSuffix =
  let tSuffix = if clAsInt then "" else "T"
  in M.fromList
     [ (FS.HeaderText ("CL_20" <> show yrSuffix <> "gvm"), FS.ColTypeName $ "VTurnout" <> tSuffix)
     , (FS.HeaderText ("CL_voter_status"), FS.ColTypeName $ "VVoterStatus" <> tSuffix)
     ]

targetSmartCols :: Int -> S.Set FS.HeaderText
targetSmartCols yrSuffix =
  S.fromList (FS.HeaderText <$>
              [ "TS_voterstatus" -- registration, TargetSmart
              , "TS_g20" <> show yrSuffix  -- how voted and thus turnout, TargetSmart
              ]
             )

targetSmartRenames :: Bool -> Int ->  Map FS.HeaderText FS.ColTypeName
targetSmartRenames clAsInt yrSuffix =
  let tSuffix = if clAsInt then "" else "T"
  in M.fromList
   [ (FS.HeaderText ("TS_g20" <> show yrSuffix <> "gvm"), FS.ColTypeName $ "VTurnout" <> tSuffix)
   , (FS.HeaderText ("TS_voterstatus"), FS.ColTypeName $ "VVoterStatus" <> tSuffix)
   ]

commonRenames :: Bool -> Int -> Int -> Map FS.HeaderText FS.ColTypeName
commonRenames clAsInt yrSuffix congress =
  let tSuffix = if clAsInt then "" else "T"
  in M.fromList
     [ (FS.HeaderText "caseid", FS.ColTypeName "CaseId")
     , (FS.HeaderText "commonpostweight", FS.ColTypeName "Weight")
     , (FS.HeaderText "inputstate", FS.ColTypeName "StateFips")
     , (FS.HeaderText ("cdid" <> show congress), FS.ColTypeName "CD")
     , (FS.HeaderText ("CC" <> show yrSuffix <> "_412"), FS.ColTypeName "HouseVote")
     ]

unRenameHeader :: FS.HeaderText
               -> FS.HeaderText
               -> (S.Set FS.HeaderText, M.Map FS.HeaderText FS.ColTypeName)
               -> (S.Set FS.HeaderText, M.Map FS.HeaderText FS.ColTypeName)
unRenameHeader shouldBe is (cols, renames) = (newCols, newRenames) where
  asColTypeName (FS.HeaderText ht) = FS.ColTypeName ht
  newCols = S.insert is $ S.delete shouldBe cols
  newRenames = case M.lookup shouldBe renames of
    Nothing -> M.insert is (asColTypeName shouldBe) renames
    Just v -> M.insert is v $ M.delete shouldBe renames

addPresVote :: FS.HeaderText -> S.Set FS.HeaderText ->  Map FS.HeaderText FS.ColTypeName -> (S.Set FS.HeaderText, Map FS.HeaderText FS.ColTypeName)
addPresVote header cols renames = (S.insert header cols, M.insert header (FS.ColTypeName "PresVote") renames)

{-
cesCols22 :: Int -> Int -> S.Set FS.HeaderText
cesCols22 yrSuffix congress =$ cesCols yrSuffix congress

S.fromList (FS.HeaderText <$> ["caseid"
                                                            , "commonpostweight"
                                                            , "inputstate"
                                                            , "cdid" <> show congress
                                                            , "gender4"
                                                            , "birthyr"
                                                            , "educ"
                                                            , "race"
                                                            , "hispanic"
                                                            , "pew_bornagain"
                                                            , "pid3"
                                                            , "pid7"
                                                            , "TS_voterstatus" -- registration, TargetSmart
                                                            , "TS_g20" <> show yrSuffix  -- how voted and thus turnout, TargetSmart
                                                            , "CC" <> show yrSuffix <> "_412" -- house candidate
                                                            , "HouseCand1Party"
                                                            , "HouseCand2Party"
                                                            , "HouseCand3Party"
                                                            , "HouseCand4Party"
                                                            ])

cesRenames22 :: Bool -> Int -> Int -> Map FS.HeaderText FS.ColTypeName
cesRenames22 clAsInt yrSuffix congress =
  let tSuffix = if clAsInt then "" else "T"
  in M.fromList
     [ (FS.HeaderText "caseid", FS.ColTypeName "CaseId")
     , (FS.HeaderText "commonpostweight", FS.ColTypeName "Weight")
     , (FS.HeaderText "inputstate", FS.ColTypeName "StateFips")
     , (FS.HeaderText ("cdid" <> show congress), FS.ColTypeName "CD")
     , (FS.HeaderText ("CC" <> show yrSuffix <> "_412"), FS.ColTypeName "HouseVote")
     , (FS.HeaderText ("TS_g20" <> show yrSuffix), FS.ColTypeName $ "VTurnout" <> tSuffix)
     , (FS.HeaderText ("TS_voterstatus"), FS.ColTypeName $ "VVoterStatus" <> tSuffix)
     ]
--
-}




{-
ccesRowGen2018AllCols :: FS.RowGen FS.DefaultStream 'FS.ColumnByName FCU.CommonColumns
ccesRowGen2018AllCols = (FS.rowGen ces2018CSV) { FS.tablePrefix = "CES"
                                               , FS.separator   = FS.CharSeparator ','
                                               , FS.rowTypeName = "CES18"
                                               }

cesRowGen2018 :: FS.RowGen FS.DefaultStream 'FS.ColumnByName FCU.CommonColumns
cesRowGen2018 = FS.modifyColumnSelector modF ccesRowGen2018AllCols where
  modF = FS.renameSomeUsingNames (cesRenames True 18 115) . FS.columnSubset (cesCols 18 115)

ces2016CSV :: FilePath
ces2016CSV = dataDir ++ "CCES16_Common_OUTPUT_Feb2018_VV.tab"

renames2016 :: Map FS.HeaderText FS.ColTypeName
cols2016 :: Set FS.HeaderText
(cols2016, renames2016) = f (cesCols 16 115, cesRenames False 16 115) where
  f = unRenameHeader (FS.HeaderText "caseid") (FS.HeaderText "V101") .
      unRenameHeader (FS.HeaderText "CL_voter_status") (FS.HeaderText "CL_voterstatus") .
      unRenameHeader (FS.HeaderText "commonpostweight") (FS.HeaderText "commonweight_post") .
      unRenameHeader (FS.HeaderText "CL_2016gvm") (FS.HeaderText "CL_E2016GVM")

-}
-- Cumulative versions

cces2018C_CSV :: FilePath
cces2018C_CSV = dataDir ++ "CCES_cumulative_2006_2018.csv"

cces2020C_CSV :: FilePath
cces2020C_CSV = dataDir ++ "CES_cumulative_2006-2020.csv"

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
--                                              , "pew_bornagain"
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
ccesRowGen2020CAllCols :: FS.RowGen FS.DefaultStream 'FS.ColumnByName FCU.CommonColumns
ccesRowGen2020CAllCols = (FS.rowGen cces2020C_CSV) { FS.tablePrefix = "CCES"
                                                   , FS.separator   = FS.CharSeparator ','
                                                   , FS.rowTypeName = "CCES"
                                                   }

ccesRowGen2018CAllCols :: FS.RowGen FS.DefaultStream 'FS.ColumnByName FCU.CommonColumns
ccesRowGen2018CAllCols = (FS.rowGen cces2018C_CSV) { FS.tablePrefix = "CCES"
                                                   , FS.separator   = FS.CharSeparator ','
                                                   , FS.rowTypeName = "CCES"
                                                   }

ccesRowGen2018C :: FS.RowGen FS.DefaultStream 'FS.ColumnByName FCU.CommonColumns
ccesRowGen2018C = FS.modifyColumnSelector colSubset ccesRowGen2018CAllCols where
  colSubset = FS.columnSubset ccesCols2018C

ccesRowGen2020C :: FS.RowGen FS.DefaultStream 'FS.ColumnByName FCU.CommonColumns
ccesRowGen2020C = FS.modifyColumnSelector colSubset ccesRowGen2020CAllCols where
  colSubset = FS.columnSubset ccesCols2020C

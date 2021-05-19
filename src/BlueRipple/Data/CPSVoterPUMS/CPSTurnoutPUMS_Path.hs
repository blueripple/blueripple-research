{-# LANGUAGE OverloadedStrings   #-}
module BlueRipple.Data.CPSTurnoutPUMS.CPSTurnoutPUMS_Path where

dataDir = "../bigData/IPUMS/"

cpsTurnoutPUMSCSV :: FilePath
cpsTurnoutPUMSCSV = dataDir ++ "CPS_Voting_2006to2020.csv"

cpsTurnoutPUMSRowGen = (F.rowGen cpsTurnoutPUMSCSV) { F.tablePrefix = "CPS"
                                                    , F.separator = ","
                                                    , F.rowTypeName = "CPSTurnoutPUMS_"
                                                    }

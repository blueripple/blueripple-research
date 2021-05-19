{-# LANGUAGE OverloadedStrings   #-}
module BlueRipple.Data.CPSVoterPUMS.CPSVoterPUMS_Path where

import qualified Frames.TH as F

dataDir = "../bigData/IPUMS/"

cpsVoterPUMSCSV :: FilePath
cpsVoterPUMSCSV = dataDir ++ "CPS_Voting_2006to2020.csv"

cpsVoterPUMSRowGen = (F.rowGen cpsVoterPUMSCSV) { F.tablePrefix = "CPS"
                                                , F.separator = ","
                                                , F.rowTypeName = "CPSVoterPUMS_Raw"
                                                }

{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings   #-}

module BlueRipple.Data.CPSVoterPUMS.CPSVoterPUMS_Path
  (
    module BlueRipple.Data.CPSVoterPUMS.CPSVoterPUMS_Path
  )
where

import qualified Frames.Streamly.TH as F
import qualified Frames.Streamly.ColumnUniverse         as FCU

dataDir :: [Char]
dataDir = "../bigData/IPUMS/"

cpsVoterPUMSCSV :: FilePath
cpsVoterPUMSCSV = dataDir ++ "CPS_Voting_2006to2020.csv"

cpsVoterPUMSRowGen :: F.RowGen F.DefaultStream 'F.ColumnByName FCU.CommonColumns
cpsVoterPUMSRowGen = (F.rowGen cpsVoterPUMSCSV) { F.tablePrefix = "CPS"
                                                , F.separator = F.CharSeparator ','
                                                , F.rowTypeName = "CPSVoterPUMS_Raw"
                                                }

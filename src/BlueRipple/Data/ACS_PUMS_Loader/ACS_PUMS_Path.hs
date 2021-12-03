{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeOperators #-}
module BlueRipple.Data.ACS_PUMS_Loader.ACS_PUMS_Path where

import qualified Frames as F
import qualified Frames.Streamly.TH                     as F
import qualified Data.Text as T

dataDir :: T.Text
dataDir = "../bigData/IPUMS/"

testDir :: T.Text
testDir = "../bigData/test/"

pumsACS1YrCSV :: FilePath
pumsACS1YrCSV = toString $ dataDir <> "/acsSelected2006To2018.csv"


pumsACS1YrRowGen = (F.rowGen pumsACS1YrCSV) { F.tablePrefix = "PUMS"
                                            , F.separator   = F.CharSeparator ','
                                            , F.rowTypeName = "PUMS_Raw"
                                            }



pumsACS1YrCSV' :: FilePath
pumsACS1YrCSV' = toString $ dataDir <> "/acsByPUMA_2010To2019.csv"


{-
pumsACS1YrCSV' :: FilePath
pumsACS1YrCSV' = T.unpack $ testDir <> "/acs1MM.csv"
-}

pumsACS1YrRowGen' = (F.rowGen pumsACS1YrCSV') { F.tablePrefix = "PUMS"
                                             , F.separator   = F.CharSeparator ','
                                             , F.rowTypeName = "PUMS_Raw2"
                                             }




type PUMSSTATEFIP = "STATEFIP" F.:-> Int

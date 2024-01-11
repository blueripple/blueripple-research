{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeOperators #-}

module BlueRipple.Data.ACS_PUMS_Loader.ACS_PUMS_Path
  (
    module BlueRipple.Data.ACS_PUMS_Loader.ACS_PUMS_Path
  )
where

import qualified Frames as F
import qualified Frames.Streamly.TH                     as F
import qualified Frames.Streamly.ColumnUniverse         as FCU
import qualified Data.Text as T

dataDir :: T.Text
dataDir = "../bigData/IPUMS/"

testDir :: T.Text
testDir = "../bigData/test/"

pumsACS1YrCSV :: FilePath
pumsACS1YrCSV = toString $ dataDir <> "/acsSelected2006To2018.csv"

pumsACS1YrRowGen :: F.RowGen
                    F.DefaultStream
                    'F.ColumnByName
                    FCU.CommonColumns
pumsACS1YrRowGen = (F.rowGen pumsACS1YrCSV) { F.tablePrefix = "PUMS"
                                            , F.separator   = F.CharSeparator ','
                                            , F.rowTypeName = "PUMS_Raw"
                                            }



pumsACS1YrCSV' :: FilePath
pumsACS1YrCSV' = toString $ dataDir <> "/acsByPUMA_2010To2020.csv"

pumsACS1Yr2010_20CSV :: FilePath
pumsACS1Yr2010_20CSV = toString $ dataDir <> "/acsByPUMA_2010To2020.csv"

pumsACS1Yr2012_21CSV :: FilePath
pumsACS1Yr2012_21CSV = toString $ dataDir <> "/acsByPUMA_2012To2021.csv"

pumsACS1Yr2012_22CSV :: FilePath
pumsACS1Yr2012_22CSV = toString $ dataDir <> "/acsByPUMA_2012To2022.csv"


{-
pumsACS1YrCSV' :: FilePath
pumsACS1YrCSV' = T.unpack $ testDir <> "/acs1MM.csv"
-}
pumsACS1YrRowGen' :: F.RowGen
                     F.DefaultStream
                     'F.ColumnByName
                     FCU.CommonColumns
pumsACS1YrRowGen' = (F.rowGen pumsACS1YrCSV') { F.tablePrefix = "PUMS"
                                             , F.separator   = F.CharSeparator ','
                                             , F.rowTypeName = "PUMS_Raw2"
                                             }

pumsACS1Yr2012_21RowGen' :: F.RowGen
                            F.DefaultStream
                            'F.ColumnByName
                            FCU.CommonColumns
pumsACS1Yr2012_21RowGen' = (F.rowGen pumsACS1Yr2012_21CSV) { F.tablePrefix = "PUMS"
                                                           , F.separator   = F.CharSeparator ','
                                                           , F.rowTypeName = "PUMS_Raw2"
                                                           }

pumsACS1Yr2012_22RowGen :: F.RowGen
                           F.DefaultStream
                           'F.ColumnByName
                           FCU.CommonColumns
pumsACS1Yr2012_22RowGen = (F.rowGen pumsACS1Yr2012_22CSV) { F.tablePrefix = "PUMS"
                                                           , F.separator   = F.CharSeparator ','
                                                           , F.rowTypeName = "PUMS_Raw2"
                                                           }



type PUMSSTATEFIP = "STATEFIP" F.:-> Int

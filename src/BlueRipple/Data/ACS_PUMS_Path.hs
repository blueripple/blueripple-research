{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
module BlueRipple.Data.ACS_PUMS_Path where

import qualified Frames.TH                     as F
import qualified Data.Text as T

dataDir = "./bigData/PUMS/"

pums1YrCSV :: Int -> T.Text -> FilePath
pums1YrCSV y ft = dataDir ++ show y ++ "/1-Year/psam_p" ++ (T.unpack ft) ++ ".csv"

-- the things I would make Categorical are already ints. :(
pums1YrRowGen = (F.rowGen $ pums1YrCSV 2018 "usa") { F.tablePrefix = "PUMS"
                                                   , F.separator   = ","
                                                   , F.rowTypeName = "PUMS"
                                                 }


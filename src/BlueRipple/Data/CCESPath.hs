{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
module BlueRipple.Data.CCESPath where

import qualified Frames.TH                     as F

dataDir :: FilePath
dataDir = "../bigData/CCES/"

ccesCSV :: FilePath = dataDir ++ "CCES_cumulative_2006_2018.csv"

-- the things I would make Categorical are already ints. :(
ccesRowGen = (F.rowGen ccesCSV) { F.tablePrefix = "CCES"
                                , F.separator   = ","
                                , F.rowTypeName = "CCES"
                                }

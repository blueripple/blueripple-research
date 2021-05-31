{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
module BlueRipple.Data.CCESPath where

import qualified Frames.TH                     as F

dataDir :: FilePath
dataDir = "../bigData/CCES/"

cces2018CSV :: FilePath = dataDir ++ "CCES_cumulative_2006_2018.csv"
cces2020CSV :: FilePath = dataDir ++ "CCES_cumulative_2006_2020.csv"

-- the things I would make Categorical are already ints. :(
ccesRowGen = (F.rowGen cces2020CSV) { F.tablePrefix = "CCES"
                                    , F.separator   = ","
                                    , F.rowTypeName = "CCES"
                                    }

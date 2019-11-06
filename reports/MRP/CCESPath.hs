{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
module MRP.CCESPath where

import qualified Frames.TH                     as F

dataDir = "./reports/MRP/data/"

ccesTSV :: FilePath = dataDir ++ "CCES_cumulative_2006_2018.csv"

-- the things I would make Categorical are already ints. :(
ccesRowGen = (F.rowGen ccesTSV) { F.tablePrefix = "CCES"
                                , F.separator   = ","
                                , F.rowTypeName = "CCES"
                                }

{- From DataFrames, for later -}
{-

-- these columns are parsed wrong so we fix them before parsing
--F.declareColumn "CCESVvRegstatus" ''Int  
--F.declareColumn "CCESHispanic"    ''Int
F.tableTypes' ccesRowGen
-}

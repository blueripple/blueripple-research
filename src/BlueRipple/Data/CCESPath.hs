{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
module BlueRipple.Data.CCESPath where

import qualified Frames.TH                     as F

dataDir = "./data/"

ccesTSV :: FilePath = dataDir ++ "CCES_cumulative_2006_2018.txt"

-- the things I would make Categorical are already ints. :(
ccesRowGen = (F.rowGen ccesTSV) { F.tablePrefix = "CCES"
                                , F.separator   = "\t"
                                , F.rowTypeName = "CCES"
                                }

{- From DataFrames, for later -}
{-

-- these columns are parsed wrong so we fix them before parsing
--F.declareColumn "CCESVvRegstatus" ''Int  
--F.declareColumn "CCESHispanic"    ''Int
F.tableTypes' ccesRowGen
-}

{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeOperators #-}
module BlueRipple.Data.ACS_PUMS_Loader.ACS_PUMS_Path where

import qualified Frames as F
import qualified Frames.TH                     as F
import qualified Data.Text as T

dataDir :: T.Text
dataDir = "../bigData/IPUMS/"

pumsACS1YrCSV :: FilePath
pumsACS1YrCSV = T.unpack $ dataDir <> "/acsSelected2006To2018.csv"


pumsACS1YrRowGen = (F.rowGen pumsACS1YrCSV) { F.tablePrefix = "PUMS"
                                            , F.separator   = ","
                                            , F.rowTypeName = "PUMS_Raw"
                                            }

type PUMSSTATEFIP = "STATEFIP" F.:-> Int


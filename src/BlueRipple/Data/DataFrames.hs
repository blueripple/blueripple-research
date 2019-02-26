{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes       #-}
{-# LANGUAGE TemplateHaskell   #-}
{-# LANGUAGE TypeFamilies      #-}
{-# LANGUAGE TypeOperators     #-}
module BlueRipple.Data.DataFrames
  (
    module BlueRipple.Data.DataSourcePaths
  , module BlueRipple.Data.DataFrames
  ) where

import           BlueRipple.Data.DataSourcePaths



import           Data.Proxy                      (Proxy (..))
import           Data.Text                       (Text)
import qualified Frames                          as F
import qualified Frames.CSV                      as F
import qualified Frames.TH                       as F

import qualified Frames.ParseableTypes as FP

-- pre-declare cols with non-standard types
F.declareColumn "Date" ''FP.FrameDay

F.tableTypes "TotalSpending" totalSpendingCSV

F.tableTypes' (F.rowGen forecastAndSpendingCSV) { F.rowTypeName = "ForecastAndSpending"
                                                , F.columnUniverse = Proxy :: Proxy FP.ColumnsWithDayAndLocalTime
                                                }
                                                
F.tableTypes "ElectionResults" electionResultsCSV
F.tableTypes "TotalSpendingBefore" totalSpendingBeforeCSV
F.tableTypes "Demographics" demographicsCSV

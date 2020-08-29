{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import qualified Data.Text as T
import qualified BlueRipple.Data.FromDB as BR.FromDB
import qualified Data.Time.Calendar as Calendar
main :: IO ()
main= do
--  BR.FromDB.netSpendingBySenateCandidatesBetweenCSV Nothing (Calendar.fromGregorian 2020 8 27)
--  BR.FromDB.netSpendingByHouseCandidatesBetweenCSV Nothing (Calendar.fromGregorian 2020 8 27)
  BR.FromDB.moneySummaryCSV

{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import qualified Data.Text as T
import qualified FromDB as FromDB
import qualified Data.Time.Calendar as Calendar
main :: IO ()
main= do
--  BR.FromDB.netSpendingBySenateCandidatesBetweenCSV Nothing (Calendar.fromGregorian 2020 8 27)
  BR.FromDB.netSpendingByHouseCandidatesBetweenCSV (Just $ Calendar.fromGregorian 2021 01 01) (Calendar.fromGregorian 2022 1 15)
--  FromDB.moneySummaryCSV

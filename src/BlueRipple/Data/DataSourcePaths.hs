{-# LANGUAGE ScopedTypeVariables #-}
module BlueRipple.Data.DataSourcePaths where

dataDir = "./data/"

totalSpendingCSV :: FilePath = dataDir ++ "allSpendingThrough20181106.csv"
forecastAndSpendingCSV :: FilePath = dataDir ++ "forecastAndSpending.csv"
electionResultsCSV :: FilePath = dataDir ++ "electionResult2018.csv"
totalSpendingBeforeCSV :: FilePath = dataDir ++ "allSpendingThrough20180731.csv"
demographicsCSV :: FilePath = dataDir ++ "censusDataByDistrict.csv"

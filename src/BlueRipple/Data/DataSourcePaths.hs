{-# LANGUAGE ScopedTypeVariables #-}
module BlueRipple.Data.DataSourcePaths where

dataDir = "./data/"

totalSpendingCSV :: FilePath = dataDir ++ "allSpendingThrough20181106.csv"
forecastAndSpendingCSV :: FilePath = dataDir ++ "forecastAndSpending.csv"
electionResultsCSV :: FilePath = dataDir ++ "electionResult2018.csv"
totalSpendingBeforeCSV :: FilePath = dataDir ++ "allSpendingThrough20180731.csv"
totalSpendingDuringCSV :: FilePath = dataDir ++ "allSpendingFrom20180801Through20181106.csv"
demographicsCSV :: FilePath = dataDir ++ "censusDataByDistrict.csv"
angryDemsCSV :: FilePath = dataDir ++ "angryDemsContributions20181203.csv"

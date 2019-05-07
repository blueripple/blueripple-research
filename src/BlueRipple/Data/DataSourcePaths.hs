{-# LANGUAGE ScopedTypeVariables #-}
module BlueRipple.Data.DataSourcePaths where

dataDir = "./data/"

totalSpendingCSV :: FilePath = dataDir ++ "allSpendingThrough20181106.csv"
forecastAndSpendingCSV :: FilePath = dataDir ++ "forecastAndSpending.csv"
electionResultsCSV :: FilePath = dataDir ++ "electionResult2018.csv"
totalSpendingBeforeCSV :: FilePath =
  dataDir ++ "allSpendingThrough20180731.csv"
totalSpendingDuringCSV :: FilePath =
  dataDir ++ "allSpendingFrom20180801Through20181106.csv"
contextDemographicsCSV :: FilePath =
  dataDir ++ "contextDemographicsByDistrict.csv"
identityDemographics2016CSV :: FilePath =
  dataDir ++ "identityDemographicsByDistrict2016.csv"
identityDemographics2017CSV :: FilePath =
  dataDir ++ "identityDemographicsByDistrict2017.csv"
angryDemsCSV :: FilePath = dataDir ++ "angryDemsContributions20181203.csv"
houseElectionsCSV :: FilePath = dataDir ++ "1976-2018-house.csv"
turnout2016CSV :: FilePath = dataDir ++ "2016Turnout.csv"
turnout2018CSV :: FilePath = dataDir ++ "2018Turnout.csv"
turnoutCSV :: FilePath = dataDir ++ "Turnout2016-2018.csv"

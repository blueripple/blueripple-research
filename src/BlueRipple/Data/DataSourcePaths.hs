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
identityDemographics2012CSV :: FilePath =
  dataDir ++ "identityDemographicsByDistrict2012.csv"
identityDemographics2014CSV :: FilePath =
  dataDir ++ "identityDemographicsByDistrict2014.csv"
identityDemographics2016CSV :: FilePath =
  dataDir ++ "identityDemographicsByDistrict2016.csv"
identityDemographics2017CSV :: FilePath =
  dataDir ++ "identityDemographicsByDistrict2017.csv"
identityDemographicsLongCSV :: FilePath =
  dataDir ++ "identityDemographicsLong2010-2017.csv"
angryDemsCSV :: FilePath = dataDir ++ "angryDemsContributions20181203.csv"
houseElectionsCSV :: FilePath = dataDir ++ "1976-2018-house.csv"
turnoutCSV :: FilePath = dataDir ++ "Turnout2012-2018.csv"

module Main where

import           BlueRipple.Data.FromDB

main :: IO ()
main = do
  allHouseCSV
  netSpendingByHouseCandidatesBetweenCSV Nothing beforeForecast
  netSpendingByHouseCandidatesBetweenCSV Nothing electionDay
  netSpendingByHouseCandidatesBetweenCSV (Just startForecast) electionDay

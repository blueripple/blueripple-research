module Main where

import           BlueRipple.Data.FromDB

main :: IO ()
main = do
  allHouseCSV
  netSpendingByHouseCandidatesBeforeCSV beforeForecast
  netSpendingByHouseCandidatesBeforeCSV electionDay

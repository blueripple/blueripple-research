{-# LANGUAGE TypeFamilies #-}

module BlueRipple.Data.CensusTypes where

import qualified BlueRipple.Data.DemographicTypes as DT

import qualified Data.Map as Map

data TotalPopulation = TotalPopulation
data Citizenship = Native | Naturalized | NonCitizen

data UnderOver18 = Under | Over

type TotalPopulationKey = TotalPopulation

acsTotalPopulationTable :: Map Text TotalPopulationKey
acsTotalPopulationTable = Map.fromList [("JMAE001",TotalPopulation)]

type CitizenshipTableKey = (DT.Sex, UnderOver18, Citizenship)

citizenshipTable :: Map Text CitizenshipTableKey
citizenshipTable = Map.fromList [("JWBE004",(DT.Male, Under, Native))
                                ,("JWBE006",(DT.Male, Under, Naturalized))
                                ,("JWBE007",(DT.Male, Under, NonCitizen))
                                ,("JWBE009",(DT.Male, Over, Native))
                                ,("JWBE011",(DT.Male, Over, Naturalized))
                                ,("JWBE012",(DT.Male, Over, NonCitizen))
                                ,("JWBE015",(DT.Female, Under, Native))
                                ,("JWBE017",(DT.Female, Under, Naturalized))
                                ,("JWBE018",(DT.Female, Under, NonCitizen))
                                ,("JWBE020",(DT.Female, Over, Native))
                                ,("JWBE022",(DT.Female, Over, Naturalized))
                                ,("JWBE023",(DT.Female, Over, NonCitizen))
                                ]

type

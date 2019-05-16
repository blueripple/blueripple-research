{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE PolyKinds         #-}
{-# LANGUAGE TypeOperators     #-}
{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes       #-}
{-# LANGUAGE TemplateHaskell   #-}
{-# LANGUAGE TypeFamilies      #-}
{-# LANGUAGE TypeOperators     #-}
{-# LANGUAGE TypeApplications  #-}
{-# LANGUAGE DeriveGeneric #-}
module BlueRipple.Data.DataFrames
  ( module BlueRipple.Data.DataSourcePaths
  , module BlueRipple.Data.DataFrames
  )
where

import           BlueRipple.Data.DataSourcePaths



import           Data.Proxy                     ( Proxy(..) )
import           Data.Text                      ( Text )
import           Data.Ix                        ( Ix )
import qualified Frames                        as F
import qualified Frames.CSV                    as F
import qualified Frames.InCore                 as FI
import qualified Frames.TH                     as F
import qualified Data.Vinyl.TypeLevel          as V

import qualified Frames.ParseableTypes         as FP
import qualified Frames.Transform              as FT

import           Data.Hashable                  ( Hashable )
import qualified Data.Vector                   as V
import           GHC.Generics                   ( Generic )

-- pre-declare cols with non-standard types
F.declareColumn "Date" ''FP.FrameDay

F.tableTypes "TotalSpending" totalSpendingCSV

F.tableTypes' (F.rowGen forecastAndSpendingCSV) { F.rowTypeName = "ForecastAndSpending"
                                                , F.columnUniverse = Proxy :: Proxy FP.ColumnsWithDayAndLocalTime
                                                }

F.tableTypes "ElectionResults" electionResultsCSV
F.tableTypes "AngryDems" angryDemsCSV
F.tableTypes "HouseElections" houseElectionsCSV
F.tableTypes "ContextDemographics" contextDemographicsCSV
F.tableTypes "Turnout"          turnoutCSV
F.tableTypes' (F.rowGen identityDemographics2016CSV) {F.rowTypeName = "AgeSexRaceByDistrict", F.tablePrefix = "Census" }


F.declareColumn "PopCount" ''Int
--F.declareColumn "CongressionalDistrict" ''Int
--F.declareColumn "StateAbbreviation" ''Text

type DemographicCategory b = "DemographicCategory" F.:-> b

type DemographicIds = '[StateAbbreviation,CongressionalDistrict]
type DemographicLong b = DemographicIds V.++ [DemographicCategory b,PopCount]

data DemographicStructure a b = DemographicStructure
  {
    dsReshape :: a -> [F.Record (DemographicLong b)] -- wide to long
  , dsCategories :: [b]
  }

data SimpleASR = OldNonWhiteFemale
               | YoungNonWhiteFemale
               | OldNonWhiteMale
               | YoungNonWhiteMale
               | OldWhiteFemale
               | YoungWhiteFemale
               | OldWhiteMale
               | YoungWhiteMale deriving (Show,Read,Enum,Bounded,Eq,Ord,Ix,Generic)

type instance FI.VectorFor SimpleASR = V.Vector
instance Hashable SimpleASR


simpleAgeSexRace :: DemographicStructure AgeSexRaceByDistrict SimpleASR
simpleAgeSexRace = DemographicStructure reshape [minBound ..]
 where
  reshapeEach :: SimpleASR -> AgeSexRaceByDistrict -> F.Record '[PopCount]
  reshapeEach c r = FT.recordSingleton @PopCount $ case c of
    OldNonWhiteFemale   -> F.rgetField @CensusOldNonWhiteFemale r
    YoungNonWhiteFemale -> F.rgetField @CensusYoungNonWhiteFemale r
    OldNonWhiteMale     -> F.rgetField @CensusOldNonWhiteMale r
    YoungNonWhiteMale   -> F.rgetField @CensusYoungNonWhiteMale r
    OldWhiteFemale      -> F.rgetField @CensusOldNonWhiteFemale r
    YoungWhiteFemale    -> F.rgetField @CensusYoungNonWhiteFemale r
    OldWhiteMale        -> F.rgetField @CensusOldNonWhiteMale r
    YoungWhiteMale      -> F.rgetField @CensusYoungNonWhiteMale r
  reshape :: AgeSexRaceByDistrict -> [F.Record (DemographicLong SimpleASR)]
  reshape =
    fmap
        ( F.rcast -- this is required to rearrange cols into expected order, I think?
        . FT.retypeColumn @CensusStateAbbreviation @StateAbbreviation
        . FT.retypeColumn @CensusCongressionalDistrict @CongressionalDistrict
        )
      . FT.reshapeRowSimpleOnOne
          @'[CensusStateAbbreviation, CensusCongressionalDistrict]
          @(DemographicCategory SimpleASR)
          [minBound ..]
          reshapeEach




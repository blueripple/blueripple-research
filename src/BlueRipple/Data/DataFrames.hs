{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE PolyKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE TypeOperators       #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE QuasiQuotes         #-}
{-# LANGUAGE TemplateHaskell     #-}
{-# LANGUAGE TypeFamilies        #-}
{-# LANGUAGE TypeOperators       #-}
{-# LANGUAGE TypeApplications    #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE Rank2Types          #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections       #-}

module BlueRipple.Data.DataFrames
  ( module BlueRipple.Data.DataSourcePaths
  , module BlueRipple.Data.DataFrames
  )
where

import           BlueRipple.Data.DataSourcePaths

import qualified Control.Foldl                 as FL
import qualified Data.Array                    as A
import qualified Data.Map                      as M
import           Data.Maybe                     ( fromMaybe)
import           Data.Proxy                     ( Proxy(..) )
import qualified Data.Text                     as T
import           Data.Text                      ( Text )
import           Data.Ix                        ( Ix )
import qualified Data.Vinyl                    as V
import qualified Data.Vinyl.TypeLevel          as V
import qualified Data.Vinyl.Functor            as V
import qualified Frames                        as F
import           Frames                         ( (:.)(..) )
import qualified Frames.CSV                    as F
import qualified Frames.InCore                 as FI
import qualified Frames.TH                     as F
import qualified Frames.Melt                   as F
import qualified Text.Read                     as TR

import qualified Frames.Folds                  as FF
import qualified Frames.ParseableTypes         as FP
import qualified Frames.Transform              as FT
import qualified Frames.MapReduce              as MR
import qualified Frames.Enumerations           as FE

import           Data.Hashable                  ( Hashable )
import qualified Data.Vector                   as V
import           GHC.Generics                   ( Generic )

import GHC.TypeLits (Symbol)
import Data.Kind (Type)

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

-- This one might be different for different breakdowns
F.tableTypes' (F.rowGen identityDemographics2016CSV) {F.rowTypeName = "AgeSexRaceByDistrict", F.tablePrefix = "Census" }

F.declareColumn "PopCount" ''Int

type DemographicCategory b = "DemographicCategory" F.:-> b  -- general holder
type LocationKey = '[StateAbbreviation, CongressionalDistrict]

type DemographicCounts b = LocationKey V.++ [DemographicCategory b, PopCount]

data DemographicStructure demographicDataRow electionDataRow demographicCategories = DemographicStructure
  {
    dsReshapeDemographicData :: demographicDataRow -> [F.Record (DemographicCounts demographicCategories)] -- wide to long
  , dsMapAndTypeTurnout :: F.Frame Turnout
                        -> Maybe (F.FrameRec ((F.RDelete Identity (F.RecordColumns Turnout)) V.++ '[DemographicCategory demographicCategories]))
  , dsMapElectionData :: Int -> F.Frame electionDataRow -> F.FrameRec (LocationKey V.++ [DVotes, RVotes, Totalvotes])
  , dsCategories :: [demographicCategories]
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
type instance FI.VectorFor (A.Array b Int) = V.Vector
instance Hashable SimpleASR


simpleAgeSexRace :: DemographicStructure AgeSexRaceByDistrict HouseElections SimpleASR
simpleAgeSexRace = DemographicStructure reshape typeTurnout mapElectionData [minBound ..]
 where
  reshapeEach :: SimpleASR -> AgeSexRaceByDistrict -> F.Record '[PopCount]
  reshapeEach c r = FT.recordSingleton @PopCount $ case c of
    OldNonWhiteFemale   -> F.rgetField @CensusOldNonWhiteFemale r
    YoungNonWhiteFemale -> F.rgetField @CensusYoungNonWhiteFemale r
    OldNonWhiteMale     -> F.rgetField @CensusOldNonWhiteMale r
    YoungNonWhiteMale   -> F.rgetField @CensusYoungNonWhiteMale r
    OldWhiteFemale      -> F.rgetField @CensusOldWhiteFemale r
    YoungWhiteFemale    -> F.rgetField @CensusYoungWhiteFemale r
    OldWhiteMale        -> F.rgetField @CensusOldWhiteMale r
    YoungWhiteMale      -> F.rgetField @CensusYoungWhiteMale r

  reshape :: AgeSexRaceByDistrict -> [F.Record (DemographicCounts SimpleASR)]
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

  typeTurnout
    :: F.Frame Turnout
    -> Maybe (F.FrameRec
         ( (F.RDelete Identity (F.RecordColumns Turnout))
             V.++
             '[DemographicCategory SimpleASR]
         ))
  typeTurnout f =
    let filtered = F.filterFrame (\r -> let i = F.rgetField @Identity r in (i /= "YoungMale")
                                                                           && (i /= "OldMale")
                                                                           && (i /= "YoungFemale")
                                                                           && (i /= "OldFemale")) f
    in fmap F.toFrame $ sequenceA $ fmap
    (F.rtraverse V.getCompose . FT.transformMaybe typeIdentity) (FL.fold FL.list filtered)

  mapElectionData :: Int -> F.Frame HouseElections
                  -> F.FrameRec (LocationKey V.++ [DVotes, RVotes, Totalvotes])
  mapElectionData year eData =
    let xDat = fmap (F.rcast @('[Year] V.++ LocationKey V.++ [Party, Candidatevotes, Totalvotes])
                     . FT.retypeColumn @District @CongressionalDistrict
                     . FT.retypeColumn @StatePo @StateAbbreviation
                    ) eData
        unpack = MR.unpackFilterOnField @Year (== year)
        assign = MR.assignKeysAndData @LocationKey @'[Party, Candidatevotes, Totalvotes]
        reduce = MR.foldAndAddKey flattenVotes
    in FL.fold (MR.concatFold $ MR.mapReduceFold unpack assign reduce) xDat



type DVotes = "DVotes" F.:-> Int
type RVotes = "RVotes" F.:-> Int
flattenVotes
  :: FL.Fold
       (F.Record '[Party, Candidatevotes, Totalvotes])
       (F.Record '[DVotes, RVotes, Totalvotes])
flattenVotes =
  FF.sequenceRecFold
    $    FF.recFieldF
           FL.sum
           (\r -> if F.rgetField @Party r == "democrat"
             then F.rgetField @Candidatevotes r
             else 0
           )
    V.:& FF.recFieldF
           FL.sum
           (\r -> if F.rgetField @Party r == "republican"
             then F.rgetField @Candidatevotes r
             else 0
           )
    V.:& FF.recFieldF (fmap (fromMaybe 0) $ FL.last) (F.rgetField @Totalvotes)
    V.:& V.RNil
      

typeIdentity
  :: forall b. (Read b)
   => F.Record '[Identity]
  -> F.Rec (Maybe F.:. F.ElField) '[DemographicCategory b]
typeIdentity x =
  (V.Compose $ fmap V.Field $ TR.readMaybe @b (T.unpack $ F.rgetField @Identity x)) V.:& V.RNil

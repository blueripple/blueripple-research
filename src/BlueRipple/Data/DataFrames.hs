{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE PolyKinds         #-}
{-# LANGUAGE GADTs             #-}
{-# LANGUAGE TypeOperators     #-}
{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes       #-}
{-# LANGUAGE TemplateHaskell   #-}
{-# LANGUAGE TypeFamilies      #-}
{-# LANGUAGE TypeOperators     #-}
{-# LANGUAGE TypeApplications  #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE Rank2Types    #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
module BlueRipple.Data.DataFrames
  ( module BlueRipple.Data.DataSourcePaths
  , module BlueRipple.Data.DataFrames
  )
where

import           BlueRipple.Data.DataSourcePaths

import qualified Control.Foldl                 as FL
import qualified Data.Array                    as A
import qualified Data.Map                      as M
import           Data.Maybe                     ( fromJust )
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

import qualified Frames.ParseableTypes         as FP
import qualified Frames.Transform              as FT
import qualified Frames.MapReduce              as MR

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
  , dsMapAndTypeTurnout :: F.Frame Turnout -> Maybe (F.FrameRec ((F.RDelete Identity (F.RecordColumns Turnout)) V.++ '[DemographicCategory b]))
  , dsCategories :: [b]
  }

-- some utilities for mapping between records and arrays (total Maps, basically)
mapAscListAll
  :: forall k v . (Enum k, Ord k, Bounded k) => M.Map k v -> Maybe [(k, v)]
mapAscListAll m = if (length (M.keys m) == length [minBound @k ..])
  then Just $ M.toAscList m
  else Nothing

makeArrayGeneralMF
  :: forall x k v
   . (Enum k, Ord k, Bounded k, A.Ix k)
  => M.Map k v
  -> (x -> k)
  -> (x -> v)
  -> (v -> v -> v)
  -> FL.FoldM Maybe x (A.Array k v)
makeArrayGeneralMF m0 getKey getVal combine =
  fmap (A.array (minBound, maxBound))
  $ MR.postMapM mapAscListAll
  $ FL.generalize
  $ FL.Fold
  (\m x -> M.insertWith combine (getKey x) (getVal x) m)
  m0
  id

makeArrayMF
  :: forall x k v
   . (Enum k, Ord k, Bounded k, A.Ix k)
  => (x -> k)
  -> (x -> v)
  -> (v -> v -> v)
  -> FL.FoldM Maybe x (A.Array k v)
makeArrayMF = makeArrayGeneralMF M.empty

makeArrayWithDefaultF
  :: forall x k v
   . (Enum k, Ord k, Bounded k, A.Ix k)
  => v
  -> (x -> k)
  -> (x -> v)
  -> (v -> v -> v)
  -> FL.Fold x (A.Array k v)
makeArrayWithDefaultF d getKey getVal combine =
  FL.Fold
  (\m x -> M.insertWith combine (getKey x) (getVal x) m)
  (M.fromList $ fmap (, d) [minBound ..])
  (A.array (minBound,maxBound) . M.toAscList)

-- This function requires at least one record of each type or else it returns Nothing
recordsToArrayMF
  :: forall b rs
   . (F.ElemOf rs (DemographicCategory b)
     , F.ElemOf rs PopCount
     , Enum b
     , Ord b
     , Bounded b
     , Ix b)
  => FL.FoldM Maybe (F.Record rs) (A.Array b Int)
recordsToArrayMF =
  makeArrayMF (F.rgetField @(DemographicCategory b))
  (F.rgetField @PopCount)
  (flip const)

foldNumArray :: (Num a, A.Ix k, Bounded k) => FL.Fold (A.Array k a) (A.Array k a)
foldNumArray = FL.Fold (\as a -> A.accum (+) as (A.assocs a)) (A.listArray (minBound,maxBound) $ repeat 0) id

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


simpleAgeSexRace :: DemographicStructure AgeSexRaceByDistrict SimpleASR
simpleAgeSexRace = DemographicStructure reshape typeTurnout [minBound ..]
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

typeIdentity
  :: forall b. (Read b)
   => F.Record '[Identity]
  -> F.Rec (Maybe F.:. F.ElField) '[DemographicCategory b]
typeIdentity x =
  (V.Compose $ fmap V.Field $ TR.readMaybe @b (T.unpack $ F.rgetField @Identity x)) V.:& V.RNil

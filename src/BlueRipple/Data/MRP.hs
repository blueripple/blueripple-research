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
{-# OPTIONS_GHC -O0              #-}
module BlueRipple.Data.MRP where


import           BlueRipple.Data.DataFrames

import qualified Control.Foldl                 as FL
import           Control.Lens                   ((%~))
import qualified Control.Monad.Except          as X
import qualified Data.Array                    as A
import qualified Data.List                     as L
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
import qualified Frames.MaybeUtils             as FM
import qualified Frames.MapReduce              as MR
import qualified Frames.Enumerations           as FE

import           Data.Hashable                  ( Hashable )
import qualified Data.Vector                   as V
import           GHC.Generics                   ( Generic )

import GHC.TypeLits (Symbol)
import Data.Kind (Type)

type CCES_MRP_Raw = '[ CCESYear
                     , CCESSt
                     , CCESDist
                     , CCESDistUp
                     , CCESCountyFips
                     , CCESGender
                     , CCESAge
                     , CCESEduc
                     , CCESRace
                     , CCESHispanic -- 1 for yes, 2 for no.  Missing is no. hispanic race + no = hispanic.  any race + yes = hispanic (?)
                     , CCESVvRegstatus
                     , CCESVvTurnoutGvm
                     , CCESVotedRepParty]
                    
type CCES_MRP = '[ Year
                 , StateFIPS
                 , CongressionalDistrict
                 , Gender
                 , Age
                 , Under45
                 , Education
                 , CollegeGrad
                 , Race
                 , WhiteNonHispanic
                 , Registration
                 , Turnout
                 , HouseVoteParty
                 ]
                 

-- first try, order these consistently with the data and use (toEnum . (-1)) when possible
minus1 x = x - 1
data GenderT = Male | Female deriving (Show, Enum, Bounded, Eq, Ord)

intToGenderT :: Int -> GenderT
intToGenderT = toEnum . minus1

type Gender = "Gender" F.:-> GenderT

data EducationT = NoHS | HighSchool | SomeCollege | TwoYear | FourYear | PostGrad deriving (Show, Enum, Bounded, Eq, Ord)

intToEducationT :: Int -> EducationT
intToEducationT = toEnum . minus1

intToCollegeGrad :: Int -> Bool
intToCollegeGrad n = n >= 4 

type Education = "Education" F.:-> EducationT
type CollegeGrad = "CollegeGrad" F.:-> Bool

data RaceT = White | Black | Hispanic | Asian | NativeAmerican | Mixed | Other | MiddleEastern deriving (Show, Enum, Bounded, Eq, Ord)

intToRaceT :: Int -> RaceT
intToRaceT = toEnum . minus1

type Race = "Race" F.:-> RaceT
type WhiteNonHispanic = "WhiteNonHispanic" F.:-> Bool


data AgeT = A18To24 | A25To44 | A45To64 | A65To74 | A75AndOver deriving (Show, Enum, Bounded, Eq, Ord)
intToAgeT :: Real a => a -> AgeT
intToAgeT x 
  | x < 25 = A18To24
  | x < 45 = A25To44
  | x < 65 = A45To64
  | x < 75 = A65To74
  | otherwise = A75AndOver

intToUnder45 :: Int -> Bool
intToUnder45 n = n < 45

type Age = "Age" F.:-> AgeT
type Under45 = "Under45" F.:-> Bool

data RegistrationT = Active | NoRecordReg | UnRegistered | Dropped | Inactive | Multiple deriving (Show, Enum, Bounded, Eq, Ord)

intToRegistrationT :: Int -> RegistrationT
intToRegistrationT = toEnum . minus1

type Registration = "Registration" F.:-> RegistrationT

data TurnoutT = Voted | NoRecordVote | NoFile deriving (Show, Enum, Bounded, Eq, Ord)

intToTurnoutT :: Int -> TurnoutT
intToTurnoutT = toEnum . minus1

type Turnout = "Turnout" F.:-> TurnoutT

data PartyT = Democrat | Republican | OtherParty deriving (Show, Enum, Bounded, Eq, Ord)

intToPartyT :: Int -> PartyT
intToPartyT x
  | x == 1 = Democrat
  | x == 2 = Republican
  | otherwise = OtherParty

type HouseVoteParty = "HouseVoteParty" F.:-> PartyT

-- to use in maybeRecsToFrame
fixCCESRow :: F.Rec (Maybe F.:. F.ElField) CCES_MRP_Raw -> F.Rec (Maybe F.:. F.ElField) CCES_MRP_Raw
fixCCESRow r = (F.rsubset %~ missingHispanicToNo)
               $ (F.rsubset %~ missingPartyToOther)
               $ (F.rsubset %~ missingRegstatusToNoRecord)
               $ (F.rsubset %~ missingTurnoutToNoFile)
               $ r where
  missingHispanicToNo :: F.Rec (Maybe :. F.ElField) '[CCESHispanic] -> F.Rec (Maybe :. F.ElField) '[CCESHispanic]
  missingHispanicToNo = FM.fromMaybeMono 2
  missingPartyToOther :: F.Rec (Maybe :. F.ElField) '[CCESVotedRepParty] -> F.Rec (Maybe :. F.ElField) '[CCESVotedRepParty]
  missingPartyToOther = FM.fromMaybeMono 3
  missingRegstatusToNoRecord :: F.Rec (Maybe :. F.ElField) '[CCESVvRegstatus] -> F.Rec (Maybe :. F.ElField) '[CCESVvRegstatus] -- ??
  missingRegstatusToNoRecord = FM.fromMaybeMono 2
  missingTurnoutToNoFile :: F.Rec (Maybe :. F.ElField) '[CCESVvTurnoutGvm] -> F.Rec (Maybe :. F.ElField) '[CCESVvTurnoutGvm] -- ??
  missingTurnoutToNoFile = FM.fromMaybeMono 3
  
-- fmap over Frame after load and throwing out bad rows
transformCCESRow :: F.Record CCES_MRP_Raw -> F.Record CCES_MRP
transformCCESRow r = F.rcast @CCES_MRP (mutate r) where
  addGender = FT.recordSingleton @Gender . intToGenderT . F.rgetField @CCESGender
  addEducation = FT.recordSingleton @Education . intToEducationT . F.rgetField @CCESEduc
  addCollegeGrad = FT.recordSingleton @CollegeGrad . intToCollegeGrad . F.rgetField @CCESEduc
  rInt q = F.rgetField @CCESRace q
  hInt q = F.rgetField @CCESHispanic q
  race q = if (hInt q == 1) then Hispanic else intToRaceT (rInt q)
  addRace = FT.recordSingleton @Race . race 
  addWhiteNonHispanic = FT.recordSingleton @WhiteNonHispanic . (== White) . race 
  addAge = FT.recordSingleton @Age . intToAgeT . F.rgetField @CCESAge
  addUnder45 = FT.recordSingleton @Under45 . intToUnder45 . F.rgetField @CCESAge
  addRegistration = FT.recordSingleton @Registration . intToRegistrationT . F.rgetField @CCESVvRegstatus
  addTurnout = FT.recordSingleton @Turnout . intToTurnoutT . F.rgetField @CCESVvTurnoutGvm
  addHouseVoteParty = FT.recordSingleton @HouseVoteParty . intToPartyT . F.rgetField @CCESVotedRepParty
  mutate = FT.retypeColumn @CCESYear @Year
           . FT.retypeColumn @CCESSt @StateFIPS
           . FT.retypeColumn @CCESDistUp @CongressionalDistrict -- could be CCES_Dist or CCES_DistUp
           . FT.mutate addGender
           . FT.mutate addEducation
           . FT.mutate addCollegeGrad
           . FT.mutate addRace
           . FT.mutate addWhiteNonHispanic
           . FT.mutate addAge
           . FT.mutate addUnder45
           . FT.mutate addRegistration
           . FT.mutate addTurnout
           . FT.mutate addHouseVoteParty

{-
F.declareColumn "PopCount" ''Int
type DemographicCategory b = "DemographicCategory" F.:-> b  -- general holder
type LocationKey = '[StateAbbreviation, CongressionalDistrict]

type DemographicCounts b = LocationKey V.++ [DemographicCategory b, PopCount]

data DemographicStructure demographicDataRow turnoutDataRow electionDataRow demographicCategories = DemographicStructure
  {
    dsPreprocessDemographicData :: (forall m. Monad m => Int -> F.Frame demographicDataRow -> X.ExceptT Text m (F.FrameRec (DemographicCounts demographicCategories)))
  , dsPreprocessTurnoutData :: (forall m. Monad m => Int -> F.Frame turnoutDataRow -> X.ExceptT Text m (F.FrameRec '[DemographicCategory demographicCategories, Population, VotedPctOfAll]))
  , dsPreprocessElectionData :: (forall m. Monad m => Int -> F.Frame electionDataRow -> X.ExceptT Text m (F.FrameRec (LocationKey V.++ [DVotes, RVotes, Totalvotes])))
  , dsCategories :: [demographicCategories]
  }

type instance FI.VectorFor (A.Array b Int) = V.Vector
type instance FI.VectorFor (A.Array b Double) = V.Vector
-}

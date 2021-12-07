{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE DeriveAnyClass    #-}
{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs             #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes       #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell   #-}
{-# LANGUAGE TypeApplications  #-}
{-# LANGUAGE TypeFamilies      #-}
{-# LANGUAGE TypeOperators     #-}
module BlueRipple.Data.DemographicTypes
  (
    module BlueRipple.Data.DemographicTypes
  , module BlueRipple.Data.DataFrames
  )
where

import qualified BlueRipple.Data.DataFrames    as BR
import BlueRipple.Data.DataFrames (StateAbbreviation, StateFIPS, CountyFIPS)
import qualified BlueRipple.Data.Keyed         as K

import           Control.Arrow                  ( (>>>) )
import qualified Control.Foldl                 as FL
import qualified Control.MapReduce             as MR
import qualified Data.Array                    as A
import qualified Data.Binary as B
import Data.Hashable (Hashable)
import qualified Data.Ix as Ix
import qualified Data.Map                      as M
import qualified Data.Monoid                   as Mon
import qualified Data.Text                     as T
import qualified Data.Serialize                as S
import qualified Data.Set                      as Set
import qualified Flat
import qualified Frames                        as F
import qualified Frames.TH
import qualified Frames.Melt                   as F
import qualified Frames.InCore                 as FI
import qualified Frames.Folds                  as FF
import qualified Frames.MapReduce              as FMR
import qualified Frames.Transform              as FT
import qualified Data.Vector.Unboxed           as UVec
import           Data.Vector.Unboxed.Deriving   (derivingUnbox)
import           Data.Vinyl.TypeLevel           (type (++))
import           Data.Vinyl.Lens                (type (⊆))
import qualified Data.Vinyl                    as V
import qualified Data.Vinyl.TypeLevel          as V
import qualified Data.Vinyl.XRec               as V
import           Data.Word                      (Word8)
import           GHC.Generics                   ( Generic )
import           Data.Discrimination            ( Grouping )
import qualified Frames.Visualization.VegaLite.Data
                                               as FV
import qualified Graphics.Vega.VegaLite        as GV
import qualified Relude.Extra as Relude
-- Serialize for caching
-- Binary for caching
-- Flat for caching
-- FI.VectorFor for frames
-- Grouping for leftJoin
-- FiniteSet for composition of aggregations

data DemographicGrouping = ASE | ASR | ASER | ASER4 | ASER5 deriving (Enum, Bounded, Eq, Ord, A.Ix, Show, Generic)
instance S.Serialize DemographicGrouping
instance B.Binary DemographicGrouping
instance Flat.Flat DemographicGrouping
instance Grouping DemographicGrouping
instance K.FiniteSet DemographicGrouping

derivingUnbox "DemographicGrouping"
  [t|DemographicGrouping -> Word8|]
  [|toEnum . fromEnum|]
  [|toEnum . fromEnum|]
type instance FI.VectorFor DemographicGrouping = UVec.Vector

type DemographicGroupingC = "DemographicGrouping" F.:-> DemographicGrouping

instance FV.ToVLDataValue (F.ElField DemographicGroupingC) where
  toVLDataValue x = (toText $ V.getLabel x, GV.Str $ show $ V.getField x)

data PopCountOfT = PC_All | PC_Citizen | PC_VAP deriving (Enum, Bounded, Eq, Ord, A.Ix, Show, Generic)
instance S.Serialize PopCountOfT
instance B.Binary PopCountOfT
instance Flat.Flat PopCountOfT
instance Grouping PopCountOfT
instance K.FiniteSet PopCountOfT

derivingUnbox "PopCountOfT"
  [t|PopCountOfT -> Word8|]
  [|toEnum . fromEnum|]
  [|toEnum . fromEnum|]
type instance FI.VectorFor PopCountOfT = UVec.Vector

type PopCountOf = "PopCountOf" F.:-> PopCountOfT
instance FV.ToVLDataValue (F.ElField PopCountOf) where
  toVLDataValue x = (toText $ V.getLabel x, GV.Str $ show $ V.getField x)

type PopCount = "PopCount" F.:-> Int

data Sex = Female | Male deriving (Enum, Bounded, Eq, Ord, A.Ix, Show, Generic, Hashable)
instance S.Serialize Sex
instance B.Binary Sex
instance Flat.Flat Sex
instance Grouping Sex
instance K.FiniteSet Sex
derivingUnbox "Sex"
  [t|Sex -> Word8|]
  [|toEnum . fromEnum|]
  [|toEnum . fromEnum|]
type instance FI.VectorFor Sex = UVec.Vector

type SexC = "Sex" F.:-> Sex

instance FV.ToVLDataValue (F.ElField SexC) where
  toVLDataValue x = (toText $ V.getLabel x, GV.Str $ show $ V.getField x)

--
data SimpleRace = NonWhite | White deriving (Eq, Ord, Enum, Bounded, A.Ix, Show, Generic)
instance S.Serialize SimpleRace
instance B.Binary SimpleRace
instance Flat.Flat SimpleRace
instance Grouping SimpleRace
instance K.FiniteSet SimpleRace
derivingUnbox "SimpleRace"
  [t|SimpleRace -> Word8|]
  [|toEnum . fromEnum|]
  [|toEnum . fromEnum|]
type instance FI.VectorFor SimpleRace = UVec.Vector

type SimpleRaceC = "SimpleRace" F.:-> SimpleRace

instance FV.ToVLDataValue (F.ElField SimpleRaceC) where
  toVLDataValue x = (toText $ V.getLabel x, GV.Str $ show $ V.getField x)

type IsCitizen = "IsCitizen" F.:-> Bool

data CollegeGrad = NonGrad | Grad deriving (Eq, Ord, Enum, Bounded, A.Ix, Show, Generic, Hashable)
instance S.Serialize CollegeGrad
instance B.Binary CollegeGrad
instance Flat.Flat CollegeGrad
instance Grouping CollegeGrad
instance K.FiniteSet CollegeGrad
derivingUnbox "CollegeGrad"
  [t|CollegeGrad -> Word8|]
  [|toEnum . fromEnum|]
  [|toEnum . fromEnum|]
type instance FI.VectorFor CollegeGrad = UVec.Vector

type CollegeGradC = "CollegeGrad" F.:-> CollegeGrad

instance FV.ToVLDataValue (F.ElField CollegeGradC) where
  toVLDataValue x = (toText $ V.getLabel x, GV.Str $ show $ V.getField x)

type InCollege = "InCollege" F.:-> Bool

data SimpleAge = Under | EqualOrOver deriving (Eq, Ord, Enum, Bounded, A.Ix, Show, Generic)
instance S.Serialize SimpleAge
instance B.Binary SimpleAge
instance Flat.Flat SimpleAge
instance Grouping SimpleAge
instance K.FiniteSet SimpleAge
derivingUnbox "SimpleAge"
  [t|SimpleAge -> Word8|]
  [|toEnum . fromEnum|]
  [|toEnum . fromEnum|]
type instance FI.VectorFor SimpleAge = UVec.Vector

type SimpleAgeC = "SimpleAge" F.:-> SimpleAge
instance FV.ToVLDataValue (F.ElField SimpleAgeC) where
  toVLDataValue x = (toText $ V.getLabel x, GV.Str $ show $ V.getField x)

data Age4 = A4_18To24 | A4_25To44 | A4_45To64 | A4_65AndOver deriving (Enum, Bounded, Eq, Ord, Show, Generic)
instance S.Serialize Age4
instance B.Binary Age4
instance Flat.Flat Age4
instance Grouping Age4
instance K.FiniteSet Age4
derivingUnbox "Age4"
  [t|Age4 -> Word8|]
  [|toEnum . fromEnum|]
  [|toEnum . fromEnum|]
type instance FI.VectorFor Age4 = UVec.Vector

type Age4C = "Age4" F.:-> Age4

simpleAgeFrom4 :: SimpleAge -> [Age4]
simpleAgeFrom4 Under       = [A4_18To24, A4_25To44]
simpleAgeFrom4 EqualOrOver = [A4_45To64, A4_65AndOver]

age4ToSimple :: Age4 -> SimpleAge
age4ToSimple A4_18To24 = Under
age4ToSimple A4_25To44 = Under
age4ToSimple _ = EqualOrOver

data Age5F = A5F_Under18 | A5F_18To24 | A5F_25To44 | A5F_45To64 | A5F_65AndOver deriving (Enum, Bounded, Eq, Ord, Show, Generic, Hashable, Ix.Ix)
instance S.Serialize Age5F
instance B.Binary Age5F
instance Flat.Flat Age5F
instance Grouping Age5F
--instance Ix.Ix Age5F
instance K.FiniteSet Age5F
derivingUnbox "Age5F"
  [t|Age5F -> Word8|]
  [|toEnum . fromEnum|]
  [|toEnum . fromEnum|]
type instance FI.VectorFor Age5F = UVec.Vector

type Age5FC = "Age5F" F.:-> Age5F

simpleAgeFrom5F :: SimpleAge -> [Age5F]
simpleAgeFrom5F Under       = [A5F_Under18, A5F_18To24, A5F_25To44]
simpleAgeFrom5F EqualOrOver = [A5F_45To64, A5F_65AndOver]

--age5FToAge4 :: Age5F -> Age4

age5FToSimple :: Age5F -> SimpleAge
age5FToSimple A5F_Under18 = Under
age5FToSimple A5F_18To24 = Under
age5FToSimple A5F_25To44 = Under
age5FToSimple _ = EqualOrOver


data Age5 = A5_18To24 | A5_25To44 | A5_45To64 | A5_65To74 | A5_75AndOver deriving (Enum, Bounded, Eq, Ord, Show, Generic)
instance S.Serialize Age5
instance B.Binary Age5
instance Flat.Flat Age5
instance Grouping Age5
instance K.FiniteSet Age5
derivingUnbox "Age5"
  [t|Age5 -> Word8|]
  [|toEnum . fromEnum|]
  [|toEnum . fromEnum|]
type instance FI.VectorFor Age5 = UVec.Vector

type Age5C = "Age5" F.:-> Age5

simpleAgeFrom5 :: SimpleAge -> [Age5]
simpleAgeFrom5 Under       = [A5_18To24, A5_25To44]
simpleAgeFrom5 EqualOrOver = [A5_45To64, A5_65To74, A5_75AndOver]

data Education = L9 | L12 | HS | SC | AS | BA | AD deriving (Enum, Bounded, Eq, Ord, Show, Generic)
instance S.Serialize Education
instance B.Binary Education
instance Flat.Flat Education
instance Grouping Education
instance K.FiniteSet Education
derivingUnbox "Education"
  [t|Education -> Word8|]
  [|toEnum . fromEnum|]
  [|toEnum . fromEnum|]
type instance FI.VectorFor Education = UVec.Vector

type EducationC = "Education" F.:-> Education

acsLevels :: CollegeGrad -> [Education]
acsLevels NonGrad = [L9, L12, HS, SC, AS]
acsLevels Grad    = [BA, AD]

turnoutLevels :: CollegeGrad -> [Education]
turnoutLevels NonGrad = [L9, L12, HS, SC] -- NB: Turnout data did not contain an Associates Degree row
turnoutLevels Grad    = [BA, AD]

aseACSLabel :: (Age4, Sex, Education) -> T.Text
aseACSLabel (a, s, e) = sexLabel s <> age4Label a <> acsEducationLabel e

age4Label :: Age4 -> T.Text
age4Label A4_18To24    = "18To24"
age4Label A4_25To44    = "25To44"
age4Label A4_45To64    = "45To64"
age4Label A4_65AndOver = "65AndOver"

sexLabel :: Sex -> T.Text
sexLabel Female = "Female"
sexLabel Male   = "Male"

acsEducationLabel :: Education -> T.Text
acsEducationLabel L9  = "LessThan9th"
acsEducationLabel L12 = "LessThan12th"
acsEducationLabel HS  = "HighSchool"
acsEducationLabel SC  = "SomeCollege"
acsEducationLabel AS  = "Associates"
acsEducationLabel BA  = "Bachelors"
acsEducationLabel AD  = "AdvancedDegree"

aseTurnoutLabel :: (Age5, Sex, Education) -> T.Text
aseTurnoutLabel (a, s, e) =
  turnoutSexLabel s <> age5Label a <> turnoutEducationLabel e

age5Label :: Age5 -> T.Text
age5Label A5_18To24    = "18To24"
age5Label A5_25To44    = "25To44"
age5Label A5_45To64    = "45To64"
age5Label A5_65To74    = "65To74"
age5Label A5_75AndOver = "75AndOver"

turnoutSexLabel :: Sex -> T.Text
turnoutSexLabel Female = "F"
turnoutSexLabel Male   = "M"

turnoutEducationLabel :: Education -> T.Text
turnoutEducationLabel L9  = "L9"
turnoutEducationLabel L12 = "L12"
turnoutEducationLabel HS  = "HS"
turnoutEducationLabel SC  = "SC"
turnoutEducationLabel AS =
  error "No associates degree category in turnout data."
turnoutEducationLabel BA = "BA"
turnoutEducationLabel AD = "AD"


data ACSRace = ACS_All | ACS_WhiteNonHispanic | ACS_NonWhite deriving (Enum, Bounded, Eq, Ord, Show, Generic)
instance S.Serialize ACSRace
instance B.Binary ACSRace
instance Flat.Flat ACSRace
instance Grouping ACSRace
instance K.FiniteSet ACSRace
derivingUnbox "ACSRace"
  [t|ACSRace -> Word8|]
  [|toEnum . fromEnum|]
  [|toEnum . fromEnum|]
type instance FI.VectorFor ACSRace = UVec.Vector

type ACSRaceC = "ACSRace" F.:-> ACSRace


acsRaceLabel :: ACSRace -> T.Text
acsRaceLabel ACS_All              = "All"
acsRaceLabel ACS_WhiteNonHispanic = "WhiteNonHispanic"
acsRaceLabel ACS_NonWhite         = "NonWhite"

asrACSLabel :: (Age5, Sex, ACSRace) -> T.Text
asrACSLabel (a, s, r) = sexLabel s <> acsRaceLabel r <> age5Label a

acsRaceLabel' :: ACSRace -> T.Text
acsRaceLabel' ACS_All              = ""
acsRaceLabel' ACS_WhiteNonHispanic = "WhiteNonHispanic"
acsRaceLabel' ACS_NonWhite         = "NonWhite"

asrACSLabel' :: (Age5, Sex, ACSRace) -> T.Text
asrACSLabel' (a, s, r) = sexLabel s <> acsRaceLabel' r <> age5Label a


asACSLabel :: (Age5, Sex) -> T.Text
asACSLabel (a, s) = sexLabel s <> age5Label a


data TurnoutRace = Turnout_All | Turnout_WhiteNonHispanic | Turnout_Black | Turnout_Asian | Turnout_Hispanic deriving (Enum, Bounded, Eq, Ord, Show, Generic)
instance S.Serialize TurnoutRace
instance B.Binary TurnoutRace
instance Flat.Flat TurnoutRace
instance Grouping TurnoutRace
instance K.FiniteSet TurnoutRace
derivingUnbox "TurnoutRace"
  [t|TurnoutRace -> Word8|]
  [|toEnum . fromEnum|]
  [|toEnum . fromEnum|]
type instance FI.VectorFor TurnoutRace = UVec.Vector

type TurnoutRaceC = "TurnoutRace" F.:-> TurnoutRace

data Race5 = R5_Other | R5_Black | R5_Hispanic | R5_Asian | R5_WhiteNonHispanic deriving (Enum, Bounded, Eq, Ord, Show, Generic, Hashable)
instance S.Serialize Race5
instance B.Binary Race5
instance Flat.Flat Race5
instance Grouping Race5
instance K.FiniteSet Race5
derivingUnbox "Race5"
  [t|Race5 -> Word8|]
  [|toEnum . fromEnum|]
  [|toEnum . fromEnum|]
type instance FI.VectorFor Race5 = UVec.Vector

type Race5C = "Race5" F.:-> Race5
instance FV.ToVLDataValue (F.ElField Race5C) where
  toVLDataValue x = (toText $ V.getLabel x, GV.Str $ show $ V.getField x)

simpleRaceFromRace5 :: Race5 -> SimpleRace
simpleRaceFromRace5 R5_Other = NonWhite
simpleRaceFromRace5 R5_Black = NonWhite
simpleRaceFromRace5 R5_Hispanic = NonWhite
simpleRaceFromRace5 R5_Asian = NonWhite
simpleRaceFromRace5 R5_WhiteNonHispanic = White


data Race4 = R4_Other | R4_Black | R4_Hispanic | R4_WhiteNonHispanic deriving (Enum, Bounded, Eq, Ord, Show, Generic)
instance S.Serialize Race4
instance B.Binary Race4
instance Flat.Flat Race4
instance Grouping Race4
instance K.FiniteSet Race4
derivingUnbox "Race4"
  [t|Race4 -> Word8|]
  [|toEnum . fromEnum|]
  [|toEnum . fromEnum|]
type instance FI.VectorFor Race4 = UVec.Vector

type Race4C = "Race4" F.:-> Race4
instance FV.ToVLDataValue (F.ElField Race4C) where
  toVLDataValue x = (toText $ V.getLabel x, GV.Str $ show $ V.getField x)


data Hisp = NonHispanic | Hispanic deriving (Enum, Bounded, Eq, Ord, Show, Generic, Hashable)
instance S.Serialize Hisp
instance B.Binary Hisp
instance Flat.Flat Hisp
instance Grouping Hisp
instance K.FiniteSet Hisp
derivingUnbox "Hisp"
  [t|Hisp -> Word8|]
  [|toEnum . fromEnum|]
  [|toEnum . fromEnum|]
type instance FI.VectorFor Hisp = UVec.Vector
type HispC = "Hisp" F.:-> Hisp
instance FV.ToVLDataValue (F.ElField HispC) where
  toVLDataValue x = (toText $ V.getLabel x, GV.Str $ show $ V.getField x)


data RaceAlone4 = RA4_White | RA4_Black | RA4_Asian | RA4_Other deriving (Enum, Bounded, Eq, Ord, Show, Generic, Hashable)
instance S.Serialize RaceAlone4
instance B.Binary RaceAlone4
instance Flat.Flat RaceAlone4
instance Grouping RaceAlone4
instance K.FiniteSet RaceAlone4
derivingUnbox "RaceAlone4"
  [t|RaceAlone4 -> Word8|]
  [|toEnum . fromEnum|]
  [|toEnum . fromEnum|]
type instance FI.VectorFor RaceAlone4 = UVec.Vector
type RaceAlone4C = "RaceAlone4" F.:-> RaceAlone4
instance FV.ToVLDataValue (F.ElField RaceAlone4C) where
  toVLDataValue x = (toText $ V.getLabel x, GV.Str $ show $ V.getField x)

simpleRaceFromRace4 :: Race4 -> SimpleRace
simpleRaceFromRace4 R4_Other = NonWhite
simpleRaceFromRace4 R4_Black = NonWhite
simpleRaceFromRace4 R4_Hispanic = NonWhite
simpleRaceFromRace4 R4_WhiteNonHispanic = White

race4FromRace5 :: Race5 -> Race4
race4FromRace5 R5_Other = R4_Other
race4FromRace5 R5_Asian = R4_Other
race4FromRace5 R5_Black = R4_Black
race4FromRace5 R5_Hispanic = R4_Hispanic
race4FromRace5 R5_WhiteNonHispanic = R4_WhiteNonHispanic

simpleRaceFromRaceAlone4 :: RaceAlone4 -> SimpleRace
simpleRaceFromRaceAlone4 RA4_White = White
simpleRaceFromRaceAlone4 _ = NonWhite

simpleRaceFromRaceAlone4AndHisp :: Bool -> RaceAlone4 -> Hisp -> SimpleRace
simpleRaceFromRaceAlone4AndHisp True RA4_White Hispanic = White
simpleRaceFromRaceAlone4AndHisp True _ _ = NonWhite
simpleRaceFromRaceAlone4AndHisp False x _ = simpleRaceFromRaceAlone4 x

race5FromRaceAlone4AndHisp :: Bool -> RaceAlone4 -> Hisp -> Race5
race5FromRaceAlone4AndHisp True _ Hispanic = R5_Hispanic
race5FromRaceAlone4AndHisp True RA4_White NonHispanic = R5_WhiteNonHispanic
race5FromRaceAlone4AndHisp True RA4_Black NonHispanic = R5_Black
race5FromRaceAlone4AndHisp True RA4_Asian NonHispanic = R5_Asian
race5FromRaceAlone4AndHisp True RA4_Other NonHispanic = R5_Other
race5FromRaceAlone4AndHisp False RA4_White _ = R5_WhiteNonHispanic
race5FromRaceAlone4AndHisp False RA4_Black _ = R5_Black
race5FromRaceAlone4AndHisp False RA4_Asian _ = R5_Asian
race5FromRaceAlone4AndHisp False RA4_Other _ = R5_Other


race4FromRaceAlone4AndHisp :: Bool -> RaceAlone4 -> Hisp -> Race4
race4FromRaceAlone4AndHisp True _ Hispanic = R4_Hispanic
race4FromRaceAlone4AndHisp True RA4_White NonHispanic = R4_WhiteNonHispanic
race4FromRaceAlone4AndHisp True RA4_Black NonHispanic = R4_Black
race4FromRaceAlone4AndHisp True _ NonHispanic = R4_Other
race4FromRaceAlone4AndHisp False RA4_White _ = R4_WhiteNonHispanic
race4FromRaceAlone4AndHisp False RA4_Black _ = R4_Black
race4FromRaceAlone4AndHisp False _ _ = R4_Other

turnoutRaceLabel :: TurnoutRace -> T.Text
turnoutRaceLabel Turnout_All              = "All"
turnoutRaceLabel Turnout_WhiteNonHispanic = "WhiteNonHispanic"
turnoutRaceLabel Turnout_Black            = "Black"
turnoutRaceLabel Turnout_Asian            = "Asian"
turnoutRaceLabel Turnout_Hispanic         = "Hispanic"


data Language = English
              | German
              | Spanish
              | Chinese
              | Tagalog
              | Vietnamese
              | French
              | Arabic
              | Korean
              | Russian
              | FrenchCreole
              | LangOther deriving (Show, Enum, Bounded, Eq, Ord, Generic, Hashable)

instance S.Serialize Language
instance B.Binary Language
instance Flat.Flat Language
instance Grouping Language
instance K.FiniteSet Language
derivingUnbox "Language"
  [t|Language -> Word8|]
  [|toEnum . fromEnum|]
  [|toEnum . fromEnum|]
type instance FI.VectorFor Language = UVec.Vector

type LanguageC = "Language" F.:-> Language
instance FV.ToVLDataValue (F.ElField LanguageC) where
  toVLDataValue x = (toText $ V.getLabel x, GV.Str $ show $ V.getField x)


data SpeaksEnglish = SE_Yes | SE_No | SE_Some deriving (Show, Enum, Bounded, Eq, Ord, Generic, Hashable)
instance S.Serialize SpeaksEnglish
instance B.Binary SpeaksEnglish
instance Flat.Flat SpeaksEnglish
instance Grouping SpeaksEnglish
instance K.FiniteSet SpeaksEnglish
derivingUnbox "SpeaksEnglish"
  [t|SpeaksEnglish -> Word8|]
  [|toEnum . fromEnum|]
  [|toEnum . fromEnum|]
type instance FI.VectorFor SpeaksEnglish = UVec.Vector

type SpeaksEnglishC = "SpeaksEnglish" F.:-> SpeaksEnglish
instance FV.ToVLDataValue (F.ElField SpeaksEnglishC) where
  toVLDataValue x = (toText $ V.getLabel x, GV.Str $ show $ V.getField x)

type PctNativeEnglish = "PctNativeEnglish" F.:-> Double
type PctNoEnglish = "PctNoEnglish" F.:-> Double

type Income = "Income" F.:-> Double
type AvgIncome = "AvgIncome" F.:-> Double
type MedianIncome = "MedianIncome" F.:-> Double

type SocSecIncome = "SocSecIncome" F.:-> Double
type AvgSocSecIncome = "AvgSocSecIncome" F.:-> Double

type PctOfPovertyLine = "PctOfPovertyLine" F.:-> Double
type PctUnderPovertyLine = "PctUnderPovertyLine" F.:-> Double
type PctUnder2xPovertyLine = "PctUnder2xPovertyLine" F.:-> Double

data CensusRegion = Northeast | Midwest | South | West | OtherRegion deriving (Show, Enum, Bounded, Eq, Ord, Generic, Hashable)

instance S.Serialize CensusRegion
instance B.Binary CensusRegion
instance Flat.Flat CensusRegion
instance Grouping CensusRegion
instance K.FiniteSet CensusRegion
derivingUnbox "CensusRegion"
  [t|CensusRegion -> Word8|]
  [|toEnum . fromEnum|]
  [|toEnum . fromEnum|]
type instance FI.VectorFor CensusRegion = UVec.Vector

type CensusRegionC = "CensusRegion" F.:-> CensusRegion

data CensusDivision = NewEngland
                    | MiddleAtlantic
                    | EastNorthCentral
                    | WestNorthCentral
                    | SouthAtlantic
                    | EastSouthCentral
                    | WestSouthCentral
                    | Mountain
                    | Pacific
                    | OtherDivision
                  deriving (Show, Enum, Bounded, Eq, Ord, Generic, Hashable)

instance S.Serialize CensusDivision
instance B.Binary CensusDivision
instance Flat.Flat CensusDivision
instance Grouping CensusDivision
instance K.FiniteSet CensusDivision
derivingUnbox "CensusDivision"
  [t|CensusDivision -> Word8|]
  [|toEnum . fromEnum|]
  [|toEnum . fromEnum|]
type instance FI.VectorFor CensusDivision = UVec.Vector

type CensusDivisionC = "CensusDivision" F.:-> CensusDivision

censusDivisionToRegion :: CensusDivision -> CensusRegion
censusDivisionToRegion NewEngland = Northeast
censusDivisionToRegion MiddleAtlantic = Northeast
censusDivisionToRegion EastNorthCentral = Midwest
censusDivisionToRegion WestNorthCentral = Midwest
censusDivisionToRegion SouthAtlantic = South
censusDivisionToRegion EastSouthCentral = South
censusDivisionToRegion WestSouthCentral = South
censusDivisionToRegion Mountain = West
censusDivisionToRegion Pacific = West
censusDivisionToRegion OtherDivision = OtherRegion

type PopPerSqMile = "PopPerSqMile" F.:-> Double


data CensusMetro = MetroUnknown
                 | NonMetro
                 | MetroPrincipal
                 | MetroOther
                 | MetroMixed deriving (Show, Enum, Bounded, Eq, Ord, Generic, Hashable)


instance S.Serialize CensusMetro
instance B.Binary CensusMetro
instance Flat.Flat CensusMetro
instance Grouping CensusMetro
instance K.FiniteSet CensusMetro
derivingUnbox "CensusMetro"
  [t|CensusMetro -> Word8|]
  [|toEnum . fromEnum|]
  [|toEnum . fromEnum|]
type instance FI.VectorFor CensusMetro = UVec.Vector

type CensusMetroC = "CensusMetro" F.:-> CensusMetro
type PctInMetro = "PctIntMetro" F.:-> Double

data EmploymentStatus = Employed
                      | Unemployed
                      | NotInLaborForce
                      | EmploymentUnknown deriving (Show, Enum, Bounded, Eq, Ord, Generic, Hashable)

instance S.Serialize EmploymentStatus
instance B.Binary EmploymentStatus
instance Flat.Flat EmploymentStatus
instance Grouping EmploymentStatus
instance K.FiniteSet EmploymentStatus
derivingUnbox "EmploymentStatus"
  [t|EmploymentStatus -> Word8|]
  [|toEnum . fromEnum|]
  [|toEnum . fromEnum|]
type instance FI.VectorFor EmploymentStatus = UVec.Vector

type EmploymentStatusC = "EmploymentStatus" F.:-> EmploymentStatus
type PctUnemployed = "PctUnemployed" F.:-> Double

type CatColsASER = '[SimpleAgeC, SexC, CollegeGradC, SimpleRaceC]
catKeyASER :: SimpleAge -> Sex -> CollegeGrad -> SimpleRace -> F.Record CatColsASER
catKeyASER a s e r = a F.&: s F.&: e F.&: r F.&: V.RNil

allCatKeysASER = [catKeyASER a s e r | a <- [EqualOrOver, Under], e <- [NonGrad, Grad], s <- [Female, Male], r <- [NonWhite, White]]

type CatColsASE = '[SimpleAgeC, SexC, CollegeGradC]
catKeyASE :: SimpleAge -> Sex -> CollegeGrad -> F.Record CatColsASE
catKeyASE a s e = a F.&: s F.&: e F.&: V.RNil

allCatKeysASE = [catKeyASE a s e | a <- [EqualOrOver, Under], s <- [Female, Male], e <- [NonGrad, Grad]]

type CatColsASR = '[SimpleAgeC, SexC, SimpleRaceC]
catKeyASR :: SimpleAge -> Sex -> SimpleRace -> F.Record CatColsASR
catKeyASR a s r = a F.&: s F.&: r F.&: V.RNil

allCatKeysASR = [catKeyASR a s r | a <- [EqualOrOver, Under], s <- [Female, Male], r <- [NonWhite, White]]

type CatColsASER5 = '[SimpleAgeC, SexC, CollegeGradC, Race5C]
catKeyASER5 :: SimpleAge -> Sex -> CollegeGrad -> Race5 -> F.Record CatColsASER5
catKeyASER5 a s e r = a F.&: s F.&: e F.&: r F.&: V.RNil

allCatKeysASER5 = [catKeyASER5 a s e r | a <- [EqualOrOver, Under], e <- [NonGrad, Grad], s <- [Female, Male], r <- [minBound..]]

type CatColsASER5H = '[SimpleAgeC, SexC, CollegeGradC, Race5C, HispC]
catKeyASER5H :: SimpleAge -> Sex -> CollegeGrad -> Race5 -> Hisp -> F.Record CatColsASER5H
catKeyASER5H a s e r h = a F.&: s F.&: e F.&: r F.&: h F.&: V.RNil

allCatKeysASER5H =
  [catKeyASER5H a s e r h | a <- [EqualOrOver, Under], e <- [NonGrad, Grad], s <- [Female, Male], r <- [minBound..], h <- [Hispanic, NonHispanic]]

type CatColsASER4 = '[SimpleAgeC, SexC, CollegeGradC, Race4C]
catKeyASER4 :: SimpleAge -> Sex -> CollegeGrad -> Race4 -> F.Record CatColsASER4
catKeyASER4 a s e r = a F.&: s F.&: e F.&: r F.&: V.RNil

allCatKeysASER4 = [catKeyASER4 a s e r | a <- [EqualOrOver, Under], e <- [NonGrad, Grad], s <- [Female, Male], r <- [minBound..]]

type CatColsLanguage = '[LanguageC, SpeaksEnglishC]


asrTurnoutLabel' :: (Age5, Sex, TurnoutRace) -> T.Text
asrTurnoutLabel' (a, s, r) = turnoutRaceLabel r <> sexLabel s <> age5Label a


asrTurnoutLabel :: (Age5, Sex, ACSRace) -> T.Text
asrTurnoutLabel (a, s, r) = acsRaceLabel r <> sexLabel s <> age5Label a


acsASELabelMap :: M.Map T.Text (Age4, Sex, Education)
acsASELabelMap =
  M.fromList
    . Relude.fmapToFst aseACSLabel
    $ [ (a, s, e)
      | a <- [(minBound :: Age4) ..]
      , s <- [(minBound :: Sex) ..]
      , e <- [(minBound :: Education) ..]
      ]

allTextKeys
  :: forall t
   . (V.KnownField t, V.Snd t ~ T.Text)
  => [T.Text]
  -> Set.Set (F.Record '[t])
allTextKeys = Set.fromList . fmap (F.&: V.RNil)

allASE_ACSKeys = allTextKeys @BR.ACSKey $ fmap
  aseACSLabel
  [ (a, s, e)
  | a <- [(minBound :: Age4) ..]
  , s <- [(minBound :: Sex) ..]
  , e <- [(minBound :: Education) ..]
  ]

allASR_ACSKeys = allTextKeys @BR.ACSKey $ fmap
  asrACSLabel'
  [ (a, s, r)
  | a <- [(minBound :: Age5) ..]
  , s <- [(minBound :: Sex) ..]
  , r <- [ACS_All, ACS_WhiteNonHispanic]
  ] -- this is how we have the data.  We infer ACS_White from .this

allASR_TurnoutKeys = allTextKeys @BR.Group $ fmap
  asrTurnoutLabel'
  [ (a, s, r)
  | a <- [(minBound :: Age5) ..]
  , s <- [(minBound :: Sex) ..]
  , r <- [(minBound :: TurnoutRace) ..]
  ]

allASE_TurnoutKeys = allTextKeys @BR.Group $ fmap
  aseTurnoutLabel
  [ (a, s, e)
  | a <- [(minBound :: Age5) ..]
  , s <- [(minBound :: Sex) ..]
  , e <- [L9, L12, HS, SC, BA, AD]
  ] -- no associates degree in turnout data

typedASEDemographics
  :: (F.ElemOf rs BR.ACSKey)
  => F.Record rs
  -> Either T.Text (F.Record (rs ++ '[Age4C, SexC, EducationC]))
typedASEDemographics r = do
  let key     = F.rgetField @BR.ACSKey r
      textMap = show acsASELabelMap
      errMsg  = key <> " not found in " <> textMap
      toRec :: (Age4, Sex, Education) -> F.Record '[Age4C, SexC, EducationC]
      toRec (a, s, e) = a F.&: s F.&: e F.&: V.RNil
  typedCols <- fmap toRec $ maybeToRight errMsg $ M.lookup
    key
    acsASELabelMap
  return $ r `V.rappend` typedCols

demographicsFold :: FL.Fold (F.Record '[BR.ACSCount]) (F.Record '[BR.ACSCount])
demographicsFold = FF.foldAllConstrained @Num FL.sum

-- aggregate ACSKey to as
aggACS_ASEKey :: K.AggListRec Int '[Age4C, SexC, EducationC] '[BR.ACSKey]
aggACS_ASEKey =
  K.AggList
    $ K.aggList
    . (\x -> [x F.&: V.RNil])
    . aseACSLabel
    . (\r ->
        (F.rgetField @Age4C r, F.rgetField @SexC r, F.rgetField @EducationC r)
      )

aggACS_ASRKey :: K.AggListRec Int '[Age5C, SexC, ACSRaceC] '[BR.ACSKey]
aggACS_ASRKey =
  K.AggList
    $ K.aggList
    . (\x -> [x F.&: V.RNil])
    . asrACSLabel'
    . (\r ->
        (F.rgetField @Age5C r, F.rgetField @SexC r, F.rgetField @ACSRaceC r)
      )

type ACSKeys = [BR.Year, BR.StateFIPS, BR.CongressionalDistrict, BR.StateName, BR.StateAbbreviation]

acsCountMonoidOps :: K.MonoidOps (F.Record '[BR.ACSCount])
acsCountMonoidOps = K.monoidOpsFromFold $ FF.foldAllConstrained @Num FL.sum

acsCountGroupOps :: K.GroupOps (F.Record '[BR.ACSCount])
acsCountGroupOps = K.GroupOps
  acsCountMonoidOps
  (FT.recordSingleton . negate . F.rgetField @BR.ACSCount)

-- We can't check these for exactness because T.Text is not finite.
-- Maybe we should add checks for a finite lits of values and implement
-- exact and complete in terms of that?
simplifyACS_ASEFold
  :: FL.FoldM
       (Either T.Text)
       BR.ASEDemographics
       ( F.FrameRec
           (ACSKeys ++ '[SimpleAgeC, SexC, CollegeGradC, BR.ACSCount])
       )
simplifyACS_ASEFold =
  let
    aggAge4         = K.toAggListRec $ K.liftAggList simpleAgeFrom4
    aggACSEducation = K.toAggListRec $ K.liftAggList acsLevels
    aggSex          = K.toAggListRec $ K.liftAggList pure
    aggASE =
      aggAge4 `K.aggListProductRec` aggSex `K.aggListProductRec` aggACSEducation
    agg = aggASE >>> aggACS_ASEKey -- when all is said and done, aggregations compose nicely
  in
    FMR.concatFoldM $ FMR.mapReduceFoldM
      (FMR.generalizeUnpack MR.noUnpack)
      ( FMR.generalizeAssign
      $ FMR.assignKeysAndData @ACSKeys @'[BR.ACSKey, BR.ACSCount]
      )
      (FMR.makeRecsWithKeyM id $ MR.ReduceFoldM $ const $ K.aggFoldAllCheckedRec
        (K.hasOneOfEachRec allASE_ACSKeys)
        (K.functionalize agg)
        (K.groupCollapse acsCountGroupOps)
      )

simplifyACS_ASRFold
  :: FL.FoldM
       (Either T.Text)
       BR.ASEDemographics
       (F.FrameRec (ACSKeys ++ '[SimpleAgeC, SexC, SimpleRaceC, BR.ACSCount]))
simplifyACS_ASRFold
  = let
      aggAge5    = K.toAggListRec $ K.liftAggList simpleAgeFrom5
      aggACSRace = K.toAggListRec $ K.AggList $ \sr -> case sr of
        NonWhite -> K.IndexedList [(1, ACS_All), (-1, ACS_WhiteNonHispanic)]
        White    -> K.aggList [ACS_WhiteNonHispanic]
      aggSex = K.toAggListRec $ K.liftAggList pure
      aggASR =
        aggAge5 `K.aggListProductRec` aggSex `K.aggListProductRec` aggACSRace
      agg = aggASR >>> aggACS_ASRKey -- when all is said and done, aggregations compose nicely
    in
      FMR.concatFoldM $ FMR.mapReduceFoldM
        (FMR.generalizeUnpack MR.noUnpack)
        ( FMR.generalizeAssign
        $ FMR.assignKeysAndData @ACSKeys @'[BR.ACSKey, BR.ACSCount]
        )
        ( FMR.makeRecsWithKeyM id
        $ MR.ReduceFoldM
        $ const
        $ K.aggFoldAllCheckedRec (K.hasOneOfEachRec allASR_ACSKeys)
                                 (K.functionalize agg)
                                 (K.groupCollapse acsCountGroupOps)
        )

turnoutMonoidOps
  :: K.MonoidOps
       (F.Record '[BR.Population, BR.Citizen, BR.Registered, BR.Voted])
turnoutMonoidOps = K.monoidOpsFromFold $ FF.foldAllConstrained @Num FL.sum

turnoutGroupOps
  :: K.GroupOps (F.Record '[BR.Population, BR.Citizen, BR.Registered, BR.Voted])
turnoutGroupOps =
  let pop  = F.rgetField @BR.Population
      cit  = F.rgetField @BR.Population
      reg  = F.rgetField @BR.Registered
      voted  = F.rgetField @BR.Voted
      invert r =
        negate (pop r)
          F.&: negate (cit r)
          F.&: negate (reg r)
          F.&: negate (voted r)
          F.&: V.RNil
  in  K.GroupOps turnoutMonoidOps invert

aggTurnout_ASEKey :: K.AggListRec Int '[Age5C, SexC, EducationC] '[BR.Group]
aggTurnout_ASEKey =
  K.AggList
    $ K.aggList
    . (\x -> [x F.&: V.RNil])
    . aseTurnoutLabel
    . (\r ->
        (F.rgetField @Age5C r, F.rgetField @SexC r, F.rgetField @EducationC r)
      )

turnoutSimpleASEAgg
  :: K.AggListRec Int '[SimpleAgeC, SexC, CollegeGradC] '[BR.Group]
turnoutSimpleASEAgg =
  let
    aggAge5      = K.toAggListRec $ K.liftAggList simpleAgeFrom5
    aggSex       = K.toAggListRec $ K.liftAggList pure
    aggEducation = K.toAggListRec $ K.liftAggList turnoutLevels
    aggASE =
      aggAge5 `K.aggListProductRec` aggSex `K.aggListProductRec` aggEducation
  in
    aggASE >>> aggTurnout_ASEKey

simplifyTurnoutASEFold
  :: FL.FoldM
       (Either T.Text)
       BR.TurnoutASE
       ( F.FrameRec
           '[BR.Year, SimpleAgeC, SexC, CollegeGradC, BR.Population, BR.Citizen, BR.Registered, BR.Voted]
       )
simplifyTurnoutASEFold = FMR.concatFoldM $ FMR.mapReduceFoldM
  (MR.generalizeUnpack MR.noUnpack)
  ( MR.generalizeAssign
  $ FMR.assignKeysAndData @'[BR.Year]
    @'[BR.Group, BR.Population, BR.Citizen, BR.Registered, BR.Voted]
  )
  (FMR.makeRecsWithKeyM id $ MR.ReduceFoldM $ const $ K.aggFoldAllCheckedRec
    (K.hasOneOfEachRec allASE_TurnoutKeys)
    (K.functionalize turnoutSimpleASEAgg)
    (K.groupCollapse turnoutGroupOps)
  )

aggTurnout_ASRKey :: K.AggListRec Int '[Age5C, SexC, TurnoutRaceC] '[BR.Group]
aggTurnout_ASRKey =
  K.AggList
    $ K.aggList
    . (\x -> [x F.&: V.RNil])
    . asrTurnoutLabel'
    . (\r ->
        (F.rgetField @Age5C r, F.rgetField @SexC r, F.rgetField @TurnoutRaceC r)
      )

turnoutSimpleASRAgg
  :: K.AggListRec Int '[SimpleAgeC, SexC, SimpleRaceC] '[BR.Group]
turnoutSimpleASRAgg
  = let
      aggAge5 = K.toAggListRec $ K.liftAggList simpleAgeFrom5
      aggSex  = K.toAggListRec $ K.liftAggList pure
      aggRace = K.toAggListRec $ K.AggList $ \sr -> case sr of
        NonWhite ->
          K.IndexedList [(1, Turnout_All), (-1, Turnout_WhiteNonHispanic)]
        White -> K.aggList [Turnout_WhiteNonHispanic]
      aggASR =
        aggAge5 `K.aggListProductRec` aggSex `K.aggListProductRec` aggRace
    in
      aggASR >>> aggTurnout_ASRKey

simplifyTurnoutASRFold
  :: FL.FoldM
       (Either T.Text)
       BR.TurnoutASR
       ( F.FrameRec
           '[BR.Year, SimpleAgeC, SexC, SimpleRaceC, BR.Population, BR.Citizen, BR.Registered, BR.Voted]
       )
simplifyTurnoutASRFold = FMR.concatFoldM $ FMR.mapReduceFoldM
  (MR.generalizeUnpack MR.noUnpack)
  ( MR.generalizeAssign
  $ FMR.assignKeysAndData @'[BR.Year]
    @'[BR.Group, BR.Population, BR.Citizen, BR.Registered, BR.Voted]
  )
  (FMR.makeRecsWithKeyM id $ MR.ReduceFoldM $ const $ K.aggFoldAllCheckedRec
    (K.hasOneOfEachRec allASR_TurnoutKeys)
    (K.functionalize turnoutSimpleASRAgg)
    (K.groupCollapse turnoutGroupOps)
  )

turnoutASELabelMap :: M.Map T.Text (Age5, Sex, Education)
turnoutASELabelMap =
  M.fromList
    . Relude.fmapToFst aseTurnoutLabel
    $ [ (a, s, e)
      | a <- [(minBound :: Age5) ..]
      , s <- [(minBound :: Sex) ..]
      , e <- [(minBound :: Education) ..]
      ]

typedASETurnout
  :: (F.ElemOf rs BR.Group)
  => F.Record rs
  -> Either T.Text (F.Record (rs ++ '[Age5C, SexC, EducationC]))
typedASETurnout r = do
  let key     = F.rgetField @BR.Group r
      textMap = show turnoutASELabelMap
      errMsg  = key <> " not found in " <> textMap
      toRec :: (Age5, Sex, Education) -> F.Record '[Age5C, SexC, EducationC]
      toRec (a, s, e) = a F.&: s F.&: e F.&: V.RNil
  typedCols <- fmap toRec $ maybeToRight errMsg $ M.lookup
    key
    turnoutASELabelMap
  return $ r `V.rappend` typedCols

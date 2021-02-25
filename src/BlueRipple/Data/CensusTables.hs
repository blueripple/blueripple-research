{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
module BlueRipple.Data.CensusTables where

import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Data.KeyedTables as KT
import qualified BlueRipple.Data.Keyed as K

import qualified Data.Binary as B
import qualified Data.Serialize as S
import qualified Flat

import qualified Data.Array as Array
import qualified Data.Csv as CSV
import           Data.Csv ((.:))
import qualified Data.Map as Map
import qualified Data.Set as Set
import qualified Frames                        as F
import qualified Frames.TH as F
import qualified Frames.InCore                 as FI
import           Data.Discrimination            ( Grouping )
import qualified Data.Vinyl as V
import qualified Data.Vector.Unboxed           as UVec
import           Data.Vector.Unboxed.Deriving   (derivingUnbox)

F.declareColumn "SqMiles" ''Double
F.declareColumn "SqKm" ''Double

type CDPrefixR = [BR.StateFips, BR.CongressionalDistrict, SqMiles, SqKm]

--newtype CensusPrefix rs = CensusPrefix { unCensusPrefix :: F.Record rs }
newtype CDPrefix = CDPrefix { unCDPrefix :: F.Record CDPrefixR } deriving (Show)
toCDPrefix :: Int -> Int -> Double -> Double -> CDPrefix
toCDPrefix sf cd sm sk = CDPrefix $ sf F.&: cd F.&: sm F.&: sk F.&: V.RNil

instance CSV.FromNamedRecord CDPrefix where
  parseNamedRecord r = toCDPrefix
                       <$> r .: "StateFIPS"
                       <*> r .: "CongressionalDistrict"
                       <*> r .: "SqMiles"
                       <*> r .: "SqKm"

-- types for tables

data Age14 = A14_Under5 | A14_5To9 | A14_10To14 | A14_15To17 | A14_18To19 | A14_20To24
           | A14_25To29 | A14_30To34 | A14_35To44 | A14_45To54
           | A14_55To64 | A14_65To74 | A14_75To84 | A14_85AndOver deriving (Show, Enum, Bounded, Eq, Ord, Array.Ix, Generic)

instance S.Serialize Age14
instance B.Binary Age14
instance Flat.Flat Age14
instance Grouping Age14
instance K.FiniteSet Age14

derivingUnbox "Age14"
  [t|Age14 -> Word8|]
  [|toEnum . fromEnum|]
  [|toEnum . fromEnum|]
type instance FI.VectorFor Age14 = UVec.Vector
type Age14C = "Age14" F.:-> Age14

age14FromAge5F :: DT.Age5F -> [Age14]
age14FromAge5F DT.A5F_Under18 = [A14_Under5, A14_5To9, A14_10To14, A14_15To17]
age14FromAge5F DT.A5F_18To24 = [A14_18To19, A14_20To24]
age14FromAge5F DT.A5F_25To44 = [A14_25To29, A14_30To34, A14_35To44]
age14FromAge5F DT.A5F_45To64 = [A14_45To54, A14_55To64]
age14FromAge5F DT.A5F_65AndOver = [A14_65To74, A14_75To84, A14_85AndOver]

reKeyAgeBySex :: (DT.Sex, DT.Age5F) -> [(DT.Sex, Age14)]
reKeyAgeBySex (s, a) = fmap (s, ) $ age14FromAge5F a

data Citizenship = Native | Naturalized | NonCitizen  deriving (Show, Enum, Bounded, Eq, Ord, Array.Ix, Generic)
instance S.Serialize Citizenship
instance B.Binary Citizenship
instance Flat.Flat Citizenship
instance Grouping Citizenship
instance K.FiniteSet Citizenship
derivingUnbox "Citizenship"
  [t|Citizenship -> Word8|]
  [|toEnum . fromEnum|]
  [|toEnum . fromEnum|]
type instance FI.VectorFor Citizenship = UVec.Vector
--F.declareColumn "AgeACS" ''AgeACS
type CitizenshipC = "Citizenship" F.:-> Citizenship

citizenshipFromIsCitizen :: Bool -> [Citizenship]
citizenshipFromIsCitizen True = [Native, Naturalized]
citizenshipFromIsCitizen False = [NonCitizen]

data Education4 = E4_NonHSGrad | E4_HSGrad | E4_SomeCollege | E4_CollegeGrad deriving (Show, Enum, Bounded, Eq, Ord, Array.Ix, Generic)
instance S.Serialize Education4
instance B.Binary Education4
instance Flat.Flat Education4
instance Grouping Education4
instance K.FiniteSet Education4
derivingUnbox "Education4"
  [t|Education4 -> Word8|]
  [|toEnum . fromEnum|]
  [|toEnum . fromEnum|]
type instance FI.VectorFor Education4 = UVec.Vector
type Education4C = "Education" F.:-> Education4

education4FromCollegeGrad :: DT.CollegeGrad -> [Education4]
education4FromCollegeGrad DT.NonGrad = [E4_NonHSGrad, E4_HSGrad, E4_SomeCollege]
education4FromCollegeGrad DT.Grad = [E4_CollegeGrad]

data CensusTable = SexByAge | SexByCitizenship | SexByEducation deriving (Show, Eq, Ord)

type family CensusTableKey (c :: CensusTable) :: Type where
  CensusTableKey 'SexByAge = (DT.Sex, Age14)
  CensusTableKey 'SexByCitizenship = (DT.Sex, Citizenship)
  CensusTableKey 'SexByEducation = (DT.Sex, Education4)

newtype NHGISPrefix = NHGISPrefix { unNHGISPrefix :: Text } deriving (Eq, Ord, Show)
data TableYear = TY2018 | TY2016

tableYear :: TableYear -> Int
tableYear TY2018 = 2018
tableYear TY2016 = 2016

sexByAgePrefix :: TableYear -> DT.RaceAlone4 -> [NHGISPrefix]
sexByAgePrefix TY2018 DT.RA4_White = [NHGISPrefix "AJ6O"]
sexByAgePrefix TY2018 DT.RA4_Black = [NHGISPrefix "AJ6P"]
sexByAgePrefix TY2018 DT.RA4_Asian = [NHGISPrefix "AJ6R"]
sexByAgePrefix TY2018 DT.RA4_Other = NHGISPrefix <$> ["AJ6Q", "AJ6S", "AJ6T", "AJ6U"]
sexByAgePrefix TY2016 DT.RA4_White = [NHGISPrefix "AGBT"]
sexByAgePrefix TY2016 DT.RA4_Black = [NHGISPrefix "AGBU"]
sexByAgePrefix TY2016 DT.RA4_Asian = [NHGISPrefix "AGBW"]
sexByAgePrefix TY2016 DT.RA4_Other = NHGISPrefix <$> ["AGBV", "AGBX", "AGBY", "AGBZ"]

hispanicSexByAgePrefix :: TableYear -> NHGISPrefix
hispanicSexByAgePrefix TY2018 = NHGISPrefix "AJ6W"
hispanicSexByAgePrefix TY2016 = NHGISPrefix "AGB1"

whiteNonHispanicSexByAgePrefix :: TableYear -> NHGISPrefix
whiteNonHispanicSexByAgePrefix TY2018 = NHGISPrefix "AJ6V"
whiteNonHispanicSexByAgePrefix TY2016 = NHGISPrefix "AGB0"

sexByAge :: NHGISPrefix -> Map (DT.Sex, Age14) Text
sexByAge (NHGISPrefix p) =
  Map.fromList [((DT.Male, A14_Under5), p <> "E003")
               ,((DT.Male, A14_5To9), p <> "E004")
               ,((DT.Male, A14_10To14), p <> "E005")
               ,((DT.Male, A14_15To17), p <> "E006")
               ,((DT.Male, A14_18To19), p <> "E007")
               ,((DT.Male, A14_20To24), p <> "E008")
               ,((DT.Male, A14_25To29), p <> "E009")
               ,((DT.Male, A14_30To34), p <> "E010")
               ,((DT.Male, A14_35To44), p <> "E011")
               ,((DT.Male, A14_45To54), p <> "E012")
               ,((DT.Male, A14_55To64), p <> "E013")
               ,((DT.Male, A14_65To74), p <> "E014")
               ,((DT.Male, A14_75To84), p <> "E015")
               ,((DT.Male, A14_85AndOver), p <> "E016")
               ,((DT.Female, A14_Under5), p <> "E018")
               ,((DT.Female, A14_5To9), p <> "E019")
               ,((DT.Female, A14_10To14), p <> "E020")
               ,((DT.Female, A14_15To17), p <> "E021")
               ,((DT.Female, A14_18To19), p <> "E022")
               ,((DT.Female, A14_20To24), p <> "E023")
               ,((DT.Female, A14_25To29), p <> "E024")
               ,((DT.Female, A14_30To34), p <> "E025")
               ,((DT.Female, A14_35To44), p <> "E026")
               ,((DT.Female, A14_45To54), p <> "E027")
               ,((DT.Female, A14_55To64), p <> "E028")
               ,((DT.Female, A14_65To74), p <> "E029")
               ,((DT.Female, A14_75To84), p <> "E030")
               ,((DT.Female, A14_85AndOver), p <> "E031")
               ]

sexByCitizenshipPrefix :: TableYear -> DT.RaceAlone4 -> [NHGISPrefix]
sexByCitizenshipPrefix TY2018 DT.RA4_White = [NHGISPrefix "AJ7C"]
sexByCitizenshipPrefix TY2018 DT.RA4_Black = [NHGISPrefix "AJ7D"]
sexByCitizenshipPrefix TY2018 DT.RA4_Asian = [NHGISPrefix "AJ7F"]
sexByCitizenshipPrefix TY2018 DT.RA4_Other = NHGISPrefix <$> ["AJ7E", "AJ7G", "AJ7H", "AJ7I"]
sexByCitizenshipPrefix TY2016 DT.RA4_White = [NHGISPrefix "AGCH"]
sexByCitizenshipPrefix TY2016 DT.RA4_Black = [NHGISPrefix "AGCI"]
sexByCitizenshipPrefix TY2016 DT.RA4_Asian = [NHGISPrefix "AGCK"]
sexByCitizenshipPrefix TY2016cccccc DT.RA4_Other = NHGISPrefix <$> ["AGCJ", "AGCL", "AGCM", "AGCN"]

hispanicSexByCitizenshipPrefix :: TableYear -> NHGISPrefix
hispanicSexByCitizenshipPrefix TY2018  = NHGISPrefix "AJ7K"
hispanicSexByCitizenshipPrefix TY2016  = NHGISPrefix "AGCP"

whiteNonHispanicSexByCitizenshipPrefix :: TableYear -> NHGISPrefix
whiteNonHispanicSexByCitizenshipPrefix TY2018 = NHGISPrefix "AJ7J"
whiteNonHispanicSexByCitizenshipPrefix TY2016 = NHGISPrefix "AGCO"


sexByCitizenship :: NHGISPrefix -> Map (DT.Sex, Citizenship) Text
sexByCitizenship (NHGISPrefix p) = Map.fromList [((DT.Male, Native), p <> "E009")
                                                   ,((DT.Male, Naturalized), p <> "E011")
                                                   ,((DT.Male, NonCitizen), p <> "E012")
                                                   ,((DT.Female, Native), p <> "E020")
                                                   ,((DT.Female, Naturalized), p <> "E022")
                                                   ,((DT.Female, NonCitizen), p <> "E023")
                                                   ]

sexByEducationPrefix :: TableYear -> DT.RaceAlone4 -> [NHGISPrefix]
sexByEducationPrefix TY2018 DT.RA4_White = [NHGISPrefix "AKDA"]
sexByEducationPrefix TY2018 DT.RA4_Black = [NHGISPrefix "AKDB"]
sexByEducationPrefix TY2018 DT.RA4_Asian = [NHGISPrefix "AKDD"]
sexByEducationPrefix TY2018 DT.RA4_Other = NHGISPrefix <$> ["AKDC", "AKDE", "AKDF", "AKDG"]
sexByEducationPrefix TY2016 DT.RA4_White = [NHGISPrefix "AGIF"]
sexByEducationPrefix TY2016 DT.RA4_Black = [NHGISPrefix "AGIG"]
sexByEducationPrefix TY2016 DT.RA4_Asian = [NHGISPrefix "AGII"]
sexByEducationPrefix TY2016 DT.RA4_Other = NHGISPrefix <$> ["AGIH", "AGIJ", "AGIK", "AGIL"]

hispanicSexByEducationPrefix :: TableYear -> NHGISPrefix
hispanicSexByEducationPrefix TY2018  = NHGISPrefix "AKDI"
hispanicSexByEducationPrefix TY2016  = NHGISPrefix "AGIN"

whiteNonHispanicSexByEducationPrefix :: TableYear -> NHGISPrefix
whiteNonHispanicSexByEducationPrefix TY2018 = NHGISPrefix "AKDH"
whiteNonHispanicSexByEducationPrefix TY2016 = NHGISPrefix "AGIM"

sexByEducation :: NHGISPrefix -> Map (DT.Sex, Education4) Text
sexByEducation (NHGISPrefix p) = Map.fromList [((DT.Male, E4_NonHSGrad), p <> "E003")
                                              ,((DT.Male, E4_HSGrad), p <> "E004")
                                              ,((DT.Male, E4_SomeCollege), p <> "E005")
                                              ,((DT.Male, E4_CollegeGrad), p <> "E006")
                                              ,((DT.Female, E4_NonHSGrad), p <> "E008")
                                              ,((DT.Female, E4_HSGrad), p <> "E009")
                                              ,((DT.Female, E4_SomeCollege), p <> "E010")
                                              ,((DT.Female, E4_CollegeGrad), p <> "E011")
                                              ]

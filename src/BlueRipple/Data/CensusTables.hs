{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
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
--F.declareColumn "AgeACS" ''AgeACS
type Age14C = "Age14" F.:-> Age14

age5FFromAge14 :: DT.Age5F -> [AgeACS]
age5FFromAge14 DT.A5F_Under18 = [A14_Under5, A14_5To9, A14_10To14, A14_15To17]
age5FFromAge14 DT.A5F_18To24 = [A14_18To19, A14_20To24]
age5FFromAge14 DT.A5F_25To44 = [A14_25To29, A14_30To34, AA_35To44]
age5FFromAge14 DT.A5F_45To64 = [A14_45To54, A14_55To64]
age5FFromAge14 DT.A5F_65AndOver = [A14_65To74, A1475To84, A14_85AndOver]

reKeyAgeBySex :: (DT.Sex, DT.Age5F) -> [(DT.Sex, Age14)]
reKeyAgeBySex (s, a) = fmap (s, ) $ age5FFromAgeACS a

data Citizenship = Native | Naturalized | NonCitizen  deriving (Show, Enum, Bounded, Eq, Ord, Array.Ix, Generic)
data instance S.Serialize Citizenship
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
type CitizenShipC = "Citizenship" F.:-> Citizenship

data CensusTable = SexByAge | SexByCitizenship deriving (Show, Eq, Ord)

type family CensusTableKey (c :: CensusTable) :: Type where
  CensusTableKey 'SexByAge = (DT.Sex, AgeACS)
  CensusTableKey 'SexByCitizenship = (DT.Sex, Citizenship)

totalSexByCitizenshipPrefix :: Text = "AJ7B"
whiteSexByCitizenshipPrefix :: Text = "AJ7C"
blackSexByCitizenshipPrefix :: Text = "AJ7D"
asianSexByCitizenshipPrefix :: Text = "AJ7F"
hispanicSexByCitizenshipPrefix :: Text = "AJ7K"
acsSexByCitizenship :: Text -> Map (DT.Sex, Citizenship) Text
acsSexByCitizenship p = Map.fromList [((DT.Male, Native), p <> "E009")
                                              ,((DT.Male, Naturalized), p <> "E011")
                                              ,((DT.Male, NonCitizen), p <> "E012")
                                              ,((DT.Female, Native), p <> "E020")
                                              ,((DT.Female, Naturalized), p <> "E022")
                                              ,((DT.Female, NonCitizen), p <> "E023")
                                              ]

acsSexByAge :: Text -> Map (DT.Sex, AgeACS) Text
acsSexByAge p =
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

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

data AgeACS = AA_Under5 | AA_5To9 | AA_10To14 | AA_15To17 | AA_18To19 | AA_20
            | AA_21 | AA_22To24 | AA_25To29 | AA_30To34 | AA_35To39 | AA_40To44
            | AA_45To49 | AA_50To54 | AA_55To59 | AA_60To61 | AA_62To64 | AA_65To66
            | AA_67To69 | AA_70To74 | AA_75To79 | AA_80To84 | AA_85AndOver deriving (Show, Enum, Bounded, Eq, Ord, Array.Ix, Generic)

instance S.Serialize AgeACS
instance B.Binary AgeACS
instance Flat.Flat AgeACS
instance Grouping AgeACS
instance K.FiniteSet AgeACS

derivingUnbox "AgeACS"
  [t|AgeACS -> Word8|]
  [|toEnum . fromEnum|]
  [|toEnum . fromEnum|]
type instance FI.VectorFor AgeACS = UVec.Vector
--F.declareColumn "AgeACS" ''AgeACS
type AgeACS_C = "AgeACS" F.:-> AgeACS

age5FFromAgeACS :: DT.Age5F -> Set.Set AgeACS
age5FFromAgeACS DT.A5F_Under18 = Set.fromList [AA_Under5, AA_5To9, AA_10To14, AA_15To17]
age5FFromAgeACS DT.A5F_18To24 = Set.fromList [AA_18To19, AA_20, AA_21, AA_22To24]
age5FFromAgeACS DT.A5F_25To44 = Set.fromList [AA_25To29, AA_30To34, AA_35To39, AA_40To44]
age5FFromAgeACS DT.A5F_45To64 = Set.fromList [AA_45To49, AA_50To54, AA_55To59, AA_60To61, AA_62To64]
age5FFromAgeACS DT.A5F_65AndOver = Set.fromList [AA_65To66, AA_67To69, AA_70To74, AA_75To79, AA_80To84, AA_85AndOver]

reKeyAgeBySex :: (DT.Sex, DT.Age5F) -> Set.Set (DT.Sex, AgeACS)
reKeyAgeBySex (s, a) = Set.map (s, ) $ age5FFromAgeACS a

data CensusTable = SexByAge deriving (Show, Eq, Ord)

type family CensusTableKey (c :: CensusTable) :: Type where
  CensusTableKey 'SexByAge = (DT.Sex, AgeACS)

acsSexByAge :: Map (DT.Sex, AgeACS) Text
acsSexByAge = Map.fromList [((DT.Male, AA_Under5), "JLZE003")
                           ,((DT.Male, AA_5To9), "JLZE004")
                           ,((DT.Male, AA_10To14), "JLZE005")
                           ,((DT.Male, AA_15To17), "JLZE006")
                           ,((DT.Male, AA_18To19), "JLZE007")
                           ,((DT.Male, AA_20), "JLZE008")
                           ,((DT.Male, AA_21), "JLZE009")
                           ,((DT.Male, AA_22To24), "JLZE010")
                           ,((DT.Male, AA_25To29), "JLZE011")
                           ,((DT.Male, AA_30To34), "JLZE012")
                           ,((DT.Male, AA_35To39), "JLZE013")
                           ,((DT.Male, AA_40To44), "JLZE014")
                           ,((DT.Male, AA_45To49), "JLZE015")
                           ,((DT.Male, AA_50To54), "JLZE016")
                           ,((DT.Male, AA_55To59), "JLZE017")
                           ,((DT.Male, AA_60To61), "JLZE018")
                           ,((DT.Male, AA_62To64), "JLZE019")
                           ,((DT.Male, AA_65To66), "JLZE020")
                           ,((DT.Male, AA_67To69), "JLZE021")
                           ,((DT.Male, AA_70To74), "JLZE022")
                           ,((DT.Male, AA_75To79), "JLZE023")
                           ,((DT.Male, AA_80To84), "JLZE024")
                           ,((DT.Male, AA_85AndOver), "JLZE025")
                           ,((DT.Female, AA_Under5), "JLZE027")
                           ,((DT.Female, AA_5To9), "JLZE028")
                           ,((DT.Female, AA_10To14), "JLZE029")
                           ,((DT.Female, AA_15To17), "JLZE030")
                           ,((DT.Female, AA_18To19), "JLZE031")
                           ,((DT.Female, AA_20), "JLZE032")
                           ,((DT.Female, AA_21), "JLZE033")
                           ,((DT.Female, AA_22To24), "JLZE034")
                           ,((DT.Female, AA_25To29), "JLZE035")
                           ,((DT.Female, AA_30To34), "JLZE036")
                           ,((DT.Female, AA_35To39), "JLZE037")
                           ,((DT.Female, AA_40To44), "JLZE038")
                           ,((DT.Female, AA_45To49), "JLZE039")
                           ,((DT.Female, AA_50To54), "JLZE040")
                           ,((DT.Female, AA_55To59), "JLZE041")
                           ,((DT.Female, AA_60To61), "JLZE042")
                           ,((DT.Female, AA_62To64), "JLZE043")
                           ,((DT.Female, AA_65To66), "JLZE044")
                           ,((DT.Female, AA_67To69), "JLZE045")
                           ,((DT.Female, AA_70To74), "JLZE046")
                           ,((DT.Female, AA_75To79), "JLZE047")
                           ,((DT.Female, AA_80To84), "JLZE048")
                           ,((DT.Female, AA_85AndOver), "JLZE049")
                           ]

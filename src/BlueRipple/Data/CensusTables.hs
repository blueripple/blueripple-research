{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
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

acsSexByAge :: Map Text (DT.Sex, AgeACS)
acsSexByAge = Map.fromList [("JLZE003",(DT.Male, AA_Under5))
                           ,("JLZE004",(DT.Male, AA_5To9))
                           ,("JLZE005",(DT.Male, AA_10To14))
                           ,("JLZE006",(DT.Male, AA_15To17))
                           ,("JLZE007",(DT.Male, AA_18To19))
                           ,("JLZE008",(DT.Male, AA_20))
                           ,("JLZE009",(DT.Male, AA_21))
                           ,("JLZE010",(DT.Male, AA_22To24))
                           ,("JLZE011",(DT.Male, AA_25To29))
                           ,("JLZE012",(DT.Male, AA_30To34))
                           ,("JLZE013",(DT.Male, AA_35To39))
                           ,("JLZE014",(DT.Male, AA_40To44))
                           ,("JLZE015",(DT.Male, AA_45To49))
                           ,("JLZE016",(DT.Male, AA_50To54))
                           ,("JLZE017",(DT.Male, AA_55To59))
                           ,("JLZE018",(DT.Male, AA_60To61))
                           ,("JLZE019",(DT.Male, AA_62To64))
                           ,("JLZE020",(DT.Male, AA_65To66))
                           ,("JLZE021",(DT.Male, AA_67To69))
                           ,("JLZE022",(DT.Male, AA_70To74))
                           ,("JLZE023",(DT.Male, AA_75To79))
                           ,("JLZE024",(DT.Male, AA_80To84))
                           ,("JLZE025",(DT.Male, AA_85AndOver))
                           ,("JLZE027",(DT.Female, AA_Under5))
                           ,("JLZE028",(DT.Female, AA_5To9))
                           ,("JLZE029",(DT.Female, AA_10To14))
                           ,("JLZE030",(DT.Female, AA_15To17))
                           ,("JLZE031",(DT.Female, AA_18To19))
                           ,("JLZE032",(DT.Female, AA_20))
                           ,("JLZE033",(DT.Female, AA_21))
                           ,("JLZE034",(DT.Female, AA_22To24))
                           ,("JLZE035",(DT.Female, AA_25To29))
                           ,("JLZE036",(DT.Female, AA_30To34))
                           ,("JLZE037",(DT.Female, AA_35To39))
                           ,("JLZE038",(DT.Female, AA_40To44))
                           ,("JLZE039",(DT.Female, AA_45To49))
                           ,("JLZE040",(DT.Female, AA_50To54))
                           ,("JLZE041",(DT.Female, AA_55To59))
                           ,("JLZE042",(DT.Female, AA_60To61))
                           ,("JLZE043",(DT.Female, AA_62To64))
                           ,("JLZE044",(DT.Female, AA_65To66))
                           ,("JLZE045",(DT.Female, AA_67To69))
                           ,("JLZE046",(DT.Female, AA_70To74))
                           ,("JLZE047",(DT.Female, AA_75To79))
                           ,("JLZE048",(DT.Female, AA_80To84))
                           ,("JLZE049",(DT.Female, AA_85AndOver))
                           ]

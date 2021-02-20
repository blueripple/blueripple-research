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
newtype CDPrefix = CDPrefix { unCDPrefix :: F.Record CDPrefixR }
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
acsSexByAge = Map.fromList [("JLZ003",(DT.Male, AA_Under5))
                           ,("JLZ004",(DT.Male, AA_5To9))
                           ,("JLZ005",(DT.Male, AA_10To14))
                           ,("JLZ006",(DT.Male, AA_15To17))
                           ,("JLZ007",(DT.Male, AA_18To19))
                           ,("JLZ008",(DT.Male, AA_20))
                           ,("JLZ009",(DT.Male, AA_21))
                           ,("JLZ010",(DT.Male, AA_22To24))
                           ,("JLZ011",(DT.Male, AA_25To29))
                           ,("JLZ012",(DT.Male, AA_30To34))
                           ,("JLZ013",(DT.Male, AA_35To39))
                           ,("JLZ014",(DT.Male, AA_40To44))
                           ,("JLZ015",(DT.Male, AA_45To49))
                           ,("JLZ016",(DT.Male, AA_50To54))
                           ,("JLZ017",(DT.Male, AA_55To59))
                           ,("JLZ018",(DT.Male, AA_60To61))
                           ,("JLZ019",(DT.Male, AA_62To64))
                           ,("JLZ020",(DT.Male, AA_65To66))
                           ,("JLZ021",(DT.Male, AA_67To69))
                           ,("JLZ022",(DT.Male, AA_70To74))
                           ,("JLZ023",(DT.Male, AA_75To79))
                           ,("JLZ024",(DT.Male, AA_80To84))
                           ,("JLZ025",(DT.Male, AA_85AndOver))
                           ,("JLZ027",(DT.Female, AA_Under5))
                           ,("JLZ028",(DT.Female, AA_5To9))
                           ,("JLZ029",(DT.Female, AA_10To14))
                           ,("JLZ030",(DT.Female, AA_15To17))
                           ,("JLZ031",(DT.Female, AA_18To19))
                           ,("JLZ032",(DT.Female, AA_20))
                           ,("JLZ033",(DT.Female, AA_21))
                           ,("JLZ034",(DT.Female, AA_22To24))
                           ,("JLZ035",(DT.Female, AA_25To29))
                           ,("JLZ036",(DT.Female, AA_30To34))
                           ,("JLZ037",(DT.Female, AA_35To39))
                           ,("JLZ038",(DT.Female, AA_40To44))
                           ,("JLZ039",(DT.Female, AA_45To49))
                           ,("JLZ040",(DT.Female, AA_50To54))
                           ,("JLZ041",(DT.Female, AA_55To59))
                           ,("JLZ042",(DT.Female, AA_60To61))
                           ,("JLZ043",(DT.Female, AA_62To64))
                           ,("JLZ044",(DT.Female, AA_65To66))
                           ,("JLZ045",(DT.Female, AA_67To69))
                           ,("JLZ046",(DT.Female, AA_70To74))
                           ,("JLZ047",(DT.Female, AA_75To79))
                           ,("JLZ048",(DT.Female, AA_80To84))
                           ,("JLZ049",(DT.Female, AA_85AndOver))
                           ]

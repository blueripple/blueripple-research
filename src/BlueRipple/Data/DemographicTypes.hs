{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE DeriveFunctor     #-}
{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs             #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications  #-}
{-# LANGUAGE TypeFamilies      #-}
{-# LANGUAGE TypeOperators     #-}
{-# LANGUAGE TupleSections     #-}
module BlueRipple.Data.DemographicTypes where

import qualified BlueRipple.Data.DataFrames    as BR

import qualified Control.Foldl                 as FL
import qualified Data.Array                    as A
import qualified Data.Map                      as M
import qualified Data.Text                     as T
import qualified Data.Serialize                as S
import qualified Data.Set                      as Set
import qualified Frames                        as F
import qualified Frames.Melt                   as F
import qualified Frames.InCore                 as FI
import qualified Frames.Folds                  as FF
import qualified Frames.MapReduce              as FMR
import qualified Data.Vector                   as Vec
import qualified Data.Vinyl                    as V
import qualified Data.Vinyl.TypeLevel          as V
import           GHC.Generics                   ( Generic )
import           Data.Discrimination            ( Grouping )
import qualified Frames.Visualization.VegaLite.Data
                                               as FV
import qualified Graphics.Vega.VegaLite        as GV

-- Serialize for caching
-- FI.VectorFor for frames
-- Grouping for leftJoin

data Sex = Female | Male deriving (Enum, Bounded, Eq, Ord, A.Ix, Show, Generic)

instance S.Serialize Sex

type instance FI.VectorFor Sex = Vec.Vector

instance Grouping Sex

type SexC = "Sex" F.:-> Sex

instance FV.ToVLDataValue (F.ElField SexC) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Str $ T.pack $ show $ V.getField x)

--
data SimpleRace = NonWhite | White deriving (Eq, Ord, Enum, Bounded, A.Ix, Show, Generic)

instance S.Serialize SimpleRace

type instance FI.VectorFor SimpleRace = Vec.Vector

instance Grouping SimpleRace

type SimpleRaceC = "SimpleRace" F.:-> SimpleRace

instance FV.ToVLDataValue (F.ElField SimpleRaceC) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Str $ T.pack $ show $ V.getField x)

data CollegeGrad = NonGrad | Grad deriving (Eq, Ord, Enum, Bounded, A.Ix, Show, Generic)

instance S.Serialize CollegeGrad

type instance FI.VectorFor CollegeGrad = Vec.Vector

instance Grouping CollegeGrad

type CollegeGradC = "CollegeGrad" F.:-> CollegeGrad

instance FV.ToVLDataValue (F.ElField CollegeGradC) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Str $ T.pack $ show $ V.getField x)

data SimpleAge = Under | EqualOrOver deriving (Eq, Ord, Enum, Bounded, A.Ix, Show, Generic)

instance S.Serialize SimpleAge

type instance FI.VectorFor SimpleAge = Vec.Vector

instance Grouping SimpleAge

type SimpleAgeC = "SimpleAge" F.:-> SimpleAge
instance FV.ToVLDataValue (F.ElField SimpleAgeC) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Str $ T.pack $ show $ V.getField x)


data Age4 = A4_18To24 | A4_25To44 | A4_45To64 | A4_65AndOver deriving (Enum, Bounded, Eq, Ord, Show, Generic)
instance S.Serialize Age4
type instance FI.VectorFor Age4 = Vec.Vector
instance Grouping Age4

type Age4C = "Age4" F.:-> Age4

simpleAgeFrom4 :: SimpleAge -> [Age4]
simpleAgeFrom4 Under       = [A4_18To24, A4_25To44]
simpleAgeFrom4 EqualOrOver = [A4_45To64, A4_65AndOver]

data Age5 = A5_18To24 | A5_25To44 | A5_45To64 | A5_65To74 | A5_75AndOver deriving (Enum, Bounded, Eq, Ord, Show, Generic)
instance S.Serialize Age5
type instance FI.VectorFor Age5 = Vec.Vector
instance Grouping Age5

type Age5C = "Age5" F.:-> Age5

simpleAgeFrom5 :: SimpleAge -> [Age5]
simpleAgeFrom5 Under       = [A5_18To24, A5_25To44]
simpleAgeFrom5 EqualOrOver = [A5_45To64, A5_65To74, A5_75AndOver]

data Education = L9 | L12 | HS | SC | AS | BA | AD deriving (Enum, Bounded, Eq, Ord, Show, Generic)
instance S.Serialize Education
type instance FI.VectorFor Education = Vec.Vector
instance Grouping Education

type EducationC = "Education" F.:-> Education

acsLevels :: CollegeGrad -> [Education]
acsLevels NonGrad = [L9, L12, HS, SC, AS]
acsLevels Grad    = [BA, AD]

turnoutLevels :: CollegeGrad -> [Education]
turnoutLevels NonGrad = [L9, L12, HS, SC] -- NB: Turnout data did not contain an Associates Degree row
turnoutLevels Grad    = [BA, AD]

aseACSLabel :: (Age4, Sex, Education) -> T.Text
aseACSLabel (a, s, e) = acsSexLabel s <> age4Label a <> acsEducationLabel e

age4Label :: Age4 -> T.Text
age4Label A4_18To24    = "18To24"
age4Label A4_25To44    = "25To44"
age4Label A4_45To64    = "45To64"
age4Label A4_65AndOver = "65AndOver"

acsSexLabel :: Sex -> T.Text
acsSexLabel Female = "Female"
acsSexLabel Male   = "Male"

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


data ACSRace = ACS_All | ACS_WhiteNonHispanic | ACS_NonWhite deriving (Enum, Bounded, Eq, Ord, Show)
data TurnoutRace = Turnout_White | Turnout_Black | Turnout_Asian | Turnout_Hispanic

acsRaceLabel :: ACSRace -> T.Text
acsRaceLabel ACS_All              = "All"
acsRaceLabel ACS_WhiteNonHispanic = "WhiteNonHispanic"
acsRaceLabel ACS_NonWhite         = "NonWhite"

asrACSLabel :: (Age5, Sex, ACSRace) -> T.Text
asrACSLabel (a, s, r) = acsSexLabel s <> acsRaceLabel r <> age5Label a

asACSLabel :: (Age5, Sex) -> T.Text
asACSLabel (a, s) = acsSexLabel s <> age5Label a

asrTurnoutLabel :: (Age5, Sex, ACSRace) -> T.Text
asrTurnoutLabel (a, s, r) = acsRaceLabel r <> acsSexLabel s <> age5Label a


acsASELabelMap :: M.Map T.Text (Age4, Sex, Education)
acsASELabelMap =
  M.fromList
    . fmap (\x -> (aseACSLabel x, x))
    $ [ (a, s, e)
      | a <- [(minBound :: Age4) ..]
      , s <- [(minBound :: Sex) ..]
      , e <- [(minBound :: Education) ..]
      ]

typedASEDemographics
  :: (F.ElemOf rs BR.ACSKey)
  => F.Record rs
  -> Either T.Text (F.Record (rs V.++ '[Age4C, SexC, EducationC]))
typedASEDemographics r = do
  let key     = F.rgetField @BR.ACSKey r
      textMap = T.pack $ show acsASELabelMap
      errMsg  = key <> " not found in " <> textMap
      toRec :: (Age4, Sex, Education) -> F.Record '[Age4C, SexC, EducationC]
      toRec (a, s, e) = a F.&: s F.&: e F.&: V.RNil
  typedCols <- fmap toRec $ maybe (Left errMsg) Right $ M.lookup
    key
    acsASELabelMap
  return $ r `V.rappend` typedCols

demographicsFold :: FL.Fold (F.Record '[BR.ACSCount]) (F.Record '[BR.ACSCount])
demographicsFold = FF.foldAllConstrained @Num FL.sum

turnoutASELabelMap :: M.Map T.Text (Age5, Sex, Education)
turnoutASELabelMap =
  M.fromList
    . fmap (\x -> (aseTurnoutLabel x, x))
    $ [ (a, s, e)
      | a <- [(minBound :: Age5) ..]
      , s <- [(minBound :: Sex) ..]
      , e <- [(minBound :: Education) ..]
      ]

typedASETurnout
  :: (F.ElemOf rs BR.Group)
  => F.Record rs
  -> Either T.Text (F.Record (rs V.++ '[Age5C, SexC, EducationC]))
typedASETurnout r = do
  let key     = F.rgetField @BR.Group r
      textMap = T.pack $ show turnoutASELabelMap
      errMsg  = key <> " not found in " <> textMap
      toRec :: (Age5, Sex, Education) -> F.Record '[Age5C, SexC, EducationC]
      toRec (a, s, e) = a F.&: s F.&: e F.&: V.RNil
  typedCols <- fmap toRec $ maybe (Left errMsg) Right $ M.lookup
    key
    turnoutASELabelMap
  return $ r `V.rappend` typedCols



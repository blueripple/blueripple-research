{-# LANGUAGE OverloadedStrings #-}
module BlueRipple.Data.PrefModel.ASETypes where

import qualified Data.Text                     as T

data ACSAge = A18To24 | A25To44 | A45To64 | A65AndOver deriving (Enum, Bounded, Eq, Ord, Show)
data TurnoutAge = T18To24 | T25To44 | T45To64 | T65To74 | T75AndOver deriving (Enum, Bounded, Eq, Ord, Show)
data Sex = Female | Male deriving (Enum, Bounded, Eq, Ord, Show)
data Education = L9 | L12 | HS | SC | AS | BA | AD

aseACSLabel :: (ACSAge, Sex, Education) -> T.Text
aseACSLabel (a, s, e) = acsSexLabel s <> acsAgeLabel a <> acsEducationLabel e

acsAgeLabel :: ACSAge -> T.Text
acsAgeLabel A18To24    = "18To24"
acsAgeLabel A25To44    = "25To44"
acsAgeLabel A45To64    = "45To64"
acsAgeLabel A65AndOver = "65AndOver"

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


aseTurnoutLabel :: (TurnoutAge, Sex, Education) -> T.Text
aseTurnoutLabel (a, s, e) =
  turnoutSexLabel s <> turnoutAgeLabel a <> turnoutEducationLabel e

turnoutAgeLabel :: TurnoutAge -> T.Text
turnoutAgeLabel T18To24    = "18To24"
turnoutAgeLabel T25To44    = "25To44"
turnoutAgeLabel T45To64    = "45To64"
turnoutAgeLabel T65To74    = "65To74"
turnoutAgeLabel T75AndOver = "75AndOver"

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



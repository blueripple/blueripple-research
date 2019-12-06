{-# LANGUAGE OverloadedStrings #-}
module BlueRipple.Data.PrefModel.ASRTypes where

import qualified BlueRipple.Data.DemographicTypes
                                               as BR
import qualified Data.Text                     as T

data Age = A18To24 | A25To44 | A45To64 | A65To74 | A75AndOver deriving (Enum, Bounded, Eq, Ord, Show)
data ACSRace = All | WhiteNonHispanic | NonWhite deriving (Enum, Bounded, Eq, Ord, Show)
data TurnoutRace = White | Black | Asian | Hispanic
ageLabel :: Age -> T.Text
ageLabel A18To24    = "18To24"
ageLabel A25To44    = "25To44"
ageLabel A45To64    = "45To64"
ageLabel A65To74    = "65To74"
ageLabel A75AndOver = "75AndOver"

sexLabel :: BR.Sex -> T.Text
sexLabel BR.Female = "Female"
sexLabel BR.Male   = "Male"

acsRaceLabel :: ACSRace -> T.Text
acsRaceLabel All              = "All"
acsRaceLabel WhiteNonHispanic = "WhiteNonHispanic"
acsRaceLabel NonWhite         = "NonWhite"

asrACSLabel :: (Age, BR.Sex, ACSRace) -> T.Text
asrACSLabel (a, s, r) = sexLabel s <> acsRaceLabel r <> ageLabel a

asACSLabel :: (Age, BR.Sex) -> T.Text
asACSLabel (a, s) = sexLabel s <> ageLabel a

asrTurnoutLabel :: (Age, BR.Sex, ACSRace) -> T.Text
asrTurnoutLabel (a, s, r) = acsRaceLabel r <> sexLabel s <> ageLabel a

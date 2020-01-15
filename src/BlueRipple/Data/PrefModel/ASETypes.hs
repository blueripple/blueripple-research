{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications  #-}
{-# LANGUAGE TypeFamilies      #-}
{-# LANGUAGE TypeOperators     #-}
module BlueRipple.Data.PrefModel.ASETypes where

import           BlueRipple.Data.DemographicTypes
import qualified BlueRipple.Data.DataFrames    as BR

import qualified Data.Discrimination           as D
import qualified Data.Text                     as T
import qualified Data.Serialize                as S
import qualified Control.Foldl                 as FL
import qualified Frames                        as F
import qualified Frames.Melt                   as F
import qualified Frames.InCore                 as FI
import qualified Data.Map                      as M
import qualified Data.Vector                   as Vec
import qualified Data.Vinyl                    as V
import qualified Data.Vinyl.TypeLevel          as V
import           GHC.Generics                   ( Generic )


data ACSAge = A18To24 | A25To44 | A45To64 | A65AndOver deriving (Enum, Bounded, Eq, Ord, Show, Generic)
instance S.Serialize ACSAge
type instance FI.VectorFor ACSAge = Vec.Vector
instance D.Grouping ACSAge

type ACSAgeC = "ACSAge" F.:-> ACSAge

simpleAgeACS :: SimpleAge -> [ACSAge]
simpleAgeACS Under = [A18To24, A25To44]
simpleAgeCS EqualOrOver = [A45To64, A65AndOver]

data TurnoutAge = T18To24 | T25To44 | T45To64 | T65To74 | T75AndOver deriving (Enum, Bounded, Eq, Ord, Show, Generic)
instance S.Serialize TurnoutAge
type instance FI.VectorFor TurnoutAge = Vec.Vector
instance D.Grouping TurnoutAge

type TurnoutAgeC = "TurnoutAge" F.:-> TurnoutAge

simpleAgeTurnout :: SimpleAge -> [TurnoutAge]
simpleAgeTurnout Under       = [T18To24, T25To44]
simpleAgeTurnout EqualOrOver = [T45To64, T65To74, T75AndOver]

data Education = L9 | L12 | HS | SC | AS | BA | AD deriving (Enum, Bounded, Eq, Ord, Show, Generic)
instance S.Serialize Education
type instance FI.VectorFor Education = Vec.Vector
instance D.Grouping Education

type EducationC = "Education" F.:-> Education

acsLevels :: CollegeGrad -> [Education]
acsLevels NonGrad = [L9, L12, HS, SC, AS]
acsLevels Grad    = [BA, AD]

turnoutLevels :: CollegeGrad -> [Education]
turnoutLevels NonGrad = [L9, L12, HS, SC] -- NB: Turnout data did not contain an Associates Degree row
turnoutLevels Grad    = [BA, AD]

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

demographicLabelMap :: M.Map T.Text (ACSAge, Sex, Education)
demographicLabelMap =
  M.fromList
    . fmap (\x -> (aseACSLabel x, x))
    $ [ (a, s, e)
      | a <- [(minBound :: ACSAge) ..]
      , s <- [(minBound :: Sex) ..]
      , e <- [(minBound :: Education) ..]
      ]

typedASEDemographics
  :: (F.ElemOf rs BR.ACSKey)
  => F.Record rs
  -> Either T.Text (F.Record (rs V.++ '[ACSAgeC, SexC, EducationC]))
typedASEDemographics r = do
  let key     = F.rgetField @BR.ACSKey r
      textMap = T.pack $ show demographicLabelMap
      errMsg  = key <> " not found in " <> textMap
      toRec :: (ACSAge, Sex, Education) -> F.Record '[ACSAgeC, SexC, EducationC]
      toRec (a, s, e) = a F.&: s F.&: e F.&: V.RNil
  typedCols <- fmap toRec $ maybe (Left errMsg) Right $ M.lookup
    key
    demographicLabelMap
  return $ r `V.rappend` typedCols

turnoutLabelMap :: M.Map T.Text (TurnoutAge, Sex, Education)
turnoutLabelMap =
  M.fromList
    . fmap (\x -> (aseTurnoutLabel x, x))
    $ [ (a, s, e)
      | a <- [(minBound :: TurnoutAge) ..]
      , s <- [(minBound :: Sex) ..]
      , e <- [(minBound :: Education) ..]
      ]

typedASETurnout
  :: (F.ElemOf rs BR.Group)
  => F.Record rs
  -> Either T.Text (F.Record (rs V.++ '[TurnoutAgeC, SexC, EducationC]))
typedASETurnout r = do
  let
    key     = F.rgetField @BR.Group r
    textMap = T.pack $ show turnoutLabelMap
    errMsg  = key <> " not found in " <> textMap
    toRec
      :: (TurnoutAge, Sex, Education)
      -> F.Record '[TurnoutAgeC, SexC, EducationC]
    toRec (a, s, e) = a F.&: s F.&: e F.&: V.RNil
  typedCols <- fmap toRec $ maybe (Left errMsg) Right $ M.lookup
    key
    turnoutLabelMap
  return $ r `V.rappend` typedCols

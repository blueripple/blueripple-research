{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE DerivingVia         #-}
{-# LANGUAGE PolyKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE TypeOperators       #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE TypeFamilies        #-}
{-# LANGUAGE TypeOperators       #-}
{-# LANGUAGE TypeApplications    #-}
{-# LANGUAGE Rank2Types          #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE UndecidableInstances #-}
{-# OPTIONS_GHC -O0              #-}
module BlueRipple.Data.CCES
  (
    module BlueRipple.Data.CCES
  , module BlueRipple.Data.CCESFrame
  )
  where

import           BlueRipple.Data.DataFrames
import qualified BlueRipple.Data.DataFrames    as BR
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.ElectionTypes as ET
import qualified BlueRipple.Data.CountFolds as BR
import qualified BlueRipple.Data.LoadersCore as BR
import qualified BlueRipple.Data.Loaders as BR
import qualified BlueRipple.Data.Keyed as Keyed
import qualified BlueRipple.Data.CountFolds as BR
import           BlueRipple.Data.CCESFrame
import qualified BlueRipple.Utilities.KnitUtils as BR

import qualified Control.Foldl                 as FL
import           Control.Lens                   ((%~))
import qualified Data.Serialize                as S
import qualified Data.IntMap                   as IM
import qualified Data.Map                      as M
import           Data.Maybe                     ( fromJust)
import qualified Data.Set as Set
import qualified Data.Text                     as T
import qualified Data.Text.Read as Text.Read

import qualified Data.Vinyl                    as V
import qualified Data.Vinyl.TypeLevel          as V

import qualified Flat

import qualified Frames                        as F
import           Frames                         ( (:.)(..) )
import qualified Frames.InCore                 as FI
import qualified Frames.Melt                   as F
import qualified Frames.MapReduce              as FMR
import qualified Frames.Transform              as FT
import qualified Frames.MaybeUtils             as FM

import qualified Frames.Serialize              as FS
import qualified Frames.Visualization.VegaLite.Data
                                               as FV
import qualified Graphics.Vega.VegaLite        as GV

--import qualified Numeric.GLM.ProblemTypes      as GLM
--import qualified Numeric.GLM.Predict            as GLM

import qualified Data.Vector                   as V
import           GHC.Generics                   ( Generic, Rep )

import qualified Knit.Report as K
import qualified Polysemy.Error                as P (mapError)
import qualified Polysemy                as P (raise)
import qualified Relude.Extra as Relude


ccesDataLoader :: (K.KnitEffects r, BR.CacheEffects r) => K.Sem r (K.ActionWithCacheTime r (F.FrameRec CCES_MRP))
ccesDataLoader = K.wrapPrefix "ccesDataLoader" $ do
  K.logLE K.Info "Loading/Building CCES data"
  BR.cachedMaybeFrameLoader @(F.RecordColumns CCES) @CCES_MRP_Raw @CCES_MRP
    (BR.LocalData $ toText cces2020CSV)
    Nothing
    Nothing
    fixCCESRow
    transformCCESRow
    Nothing
    "cces_2006_2020.bin"

type CCES_MRP_Raw = '[ CCESYear
                     , CCESCaseId
                     , CCESWeight
                     , CCESWeightCumulative
                     , CCESSt
                     , CCESDistUp
                     , CCESGender
                     , CCESAge
                     , CCESEduc
                     , CCESRace
                     , CCESHispanic -- 1 for yes, 2 for no.  Missing is no. hispanic race + no = hispanic.  any race + yes = hispanic (?)
                     , CCESPid3
                     , CCESPid7
                     , CCESPid3Leaner
                     , CCESVvRegstatus
                     , CCESVvTurnoutGvm
                     , CCESVotedRepParty
                     , CCESVotedPres16
                     , CCESVotedPres12
                     , CCESVotedPres08
                     ]

type CCES_MRP = '[ Year
                 , CCESCaseId
                 , CCESWeight
                 , CCESWeightCumulative
                 , StateAbbreviation
                 , CongressionalDistrict
                 , DT.SexC
                 , DT.Age5C
                 , DT.SimpleAgeC
                 , DT.EducationC
                 , DT.CollegeGradC
                 , DT.Race5C
                 , DT.HispC
                 , DT.SimpleRaceC
                 , PartisanId3
                 , PartisanId7
                 , PartisanIdLeaner
                 , Registration
                 , Turnout
                 , HouseVoteParty
                 , Pres2016VoteParty
                 , Pres2012VoteParty
                 , Pres2008VoteParty
                 ]


-- first try, order these consistently with the data and use (toEnum . (-1)) when possible
minus1 x = x - 1

-- can't do toEnum here because we have Female first
intToSex :: Int -> DT.Sex
intToSex n = case n of
  1 -> DT.Male
  2 -> DT.Female
  _ -> error "Int other 1 or 2 in \"intToSex\""

intToEducation :: Int -> DT.Education
intToEducation 1 = DT.L9
intToEducation 2 = DT.L12
intToEducation 3 = DT.HS
intToEducation 4 = DT.AS
intToEducation 5 = DT.BA
intToEducation 6 = DT.AD

intToCollegeGrad :: Int -> DT.CollegeGrad
intToCollegeGrad n = if n >= 4 then DT.Grad else DT.NonGrad

data RaceT = White | Black | Hispanic | Asian | NativeAmerican | Mixed | Other | MiddleEastern deriving (Show, Enum, Bounded, Eq, Ord, Generic)
type instance FI.VectorFor RaceT = V.Vector
instance S.Serialize RaceT
instance Flat.Flat RaceT

intToRaceT :: Int -> RaceT
intToRaceT = fromMaybe Other . Relude.safeToEnum . minus1

type Race = "Race" F.:-> RaceT
instance FV.ToVLDataValue (F.ElField Race) where
  toVLDataValue x = (toText $ V.getLabel x, GV.Str $ show $ V.getField x)

raceToSimpleRace :: RaceT -> DT.SimpleRace
raceToSimpleRace White = DT.White
raceToSimpleRace _ = DT.NonWhite


raceToRace5 :: RaceT -> DT.Race5
raceToRace5 White = DT.R5_WhiteNonLatinx
raceToRace5 Black = DT.R5_Black
raceToRace5 Hispanic = DT.R5_Latinx
raceToRace5 Asian = DT.R5_Asian
raceToRace5 _ = DT.R5_Other

intToHisp :: Int -> DT.Hisp
intToHisp n
  | n == 1 = DT.Hispanic
  | otherwise = DT.NonHispanic


intToAgeT :: Real a => a -> DT.Age5
intToAgeT x
  | x < 25 = DT.A5_18To24
  | x < 45 = DT.A5_25To44
  | x < 65 = DT.A5_45To64
  | x < 75 = DT.A5_65To74
  | otherwise = DT.A5_75AndOver

intToSimpleAge :: Int -> DT.SimpleAge
intToSimpleAge n = if n < 45 then DT.Under else DT.EqualOrOver

data RegistrationT = R_Active
                   | R_NoRecord
                   | R_Unregistered
                   | R_Dropped
                   | R_Inactive
                   | R_Multiple
                   | R_Missing deriving (Show, Enum, Bounded, Eq, Ord, Generic)
type instance FI.VectorFor RegistrationT = V.Vector
instance S.Serialize RegistrationT
instance Flat.Flat RegistrationT


parseRegistration :: T.Text -> RegistrationT
parseRegistration "Active" = R_Active
parseRegistration "No Record Of Registration" = R_NoRecord
parseRegistration "Unregistered" = R_Unregistered
parseRegistration "Dropped" = R_Dropped
parseRegistration "Inactive" = R_Inactive
parseRegistration "Multiple Appearances" = R_Multiple
parseRegistration _ = R_Missing

type Registration = "Registration" F.:-> RegistrationT
instance FV.ToVLDataValue (F.ElField Registration) where
  toVLDataValue x = (toText $ V.getLabel x, GV.Str $ show $ V.getField x)

data RegPartyT = RP_NoRecord
               | RP_Unknown
               | RP_Democratic
               | RP_Republican
               | RP_Green
               | RP_Independent
               | RP_Libertarian
               | RP_Other deriving (Show, Enum, Bounded, Eq, Ord, Generic)
type instance FI.VectorFor RegPartyT = V.Vector
instance S.Serialize RegPartyT
instance Flat.Flat RegPartyT

parseRegParty :: T.Text -> RegPartyT
parseRegParty "No Record Of Party Registration" = RP_NoRecord
parseRegParty "Unknown" = RP_Unknown
parseRegParty "Democratic" = RP_Democratic
parseRegParty "Republican" = RP_Republican
parseRegParty "Green" = RP_Green
parseRegParty "Independent" = RP_Independent
parseRegParty "Libertarian" = RP_Libertarian
parseRegParty _ = RP_Other

data TurnoutT = T_Voted
              | T_NoRecord
              | T_NoFile
              | T_Missing deriving (Show, Enum, Bounded, Eq, Ord, Generic)
type instance FI.VectorFor TurnoutT = V.Vector
instance S.Serialize TurnoutT
instance Flat.Flat TurnoutT

parseTurnout :: T.Text -> TurnoutT
parseTurnout "Voted" = T_Voted
parseTurnout "No Record Of Voting" = T_NoRecord
parseTurnout "No Voter File" = T_NoFile
parseTurnout _ = T_Missing

type Turnout = "Turnout" F.:-> TurnoutT
instance FV.ToVLDataValue (F.ElField Turnout) where
  toVLDataValue x = (toText $ V.getLabel x, GV.Str $ show $ V.getField x)


data PartisanIdentity3 = PI3_Democrat
                       | PI3_Republican
                       | PI3_Independent
                       | PI3_Other
                       | PI3_NotSure
                       | PI3_Missing deriving (Show, Enum, Bounded, Eq, Ord, Generic)
type instance FI.VectorFor PartisanIdentity3 = V.Vector
instance S.Serialize PartisanIdentity3
instance Flat.Flat PartisanIdentity3

parsePartisanIdentity3 :: Int -> PartisanIdentity3
parsePartisanIdentity3 = fromMaybe PI3_Missing . Relude.safeToEnum . minus1 . min 6

type PartisanId3 = "PartisanId3" F.:-> PartisanIdentity3
instance FV.ToVLDataValue (F.ElField PartisanId3) where
  toVLDataValue x = (toText $ V.getLabel x, GV.Str $ show $ V.getField x)


data PartisanIdentity7 = PI7_StrongDem
                       | PI7_WeakDem
                       | PI7_LeanDem
                       | PI7_Independent
                       | PI7_LeanRep
                       | PI7_WeakRep
                       | PI7_StrongRep
                       | PI7_NotSure
                       | PI7_Missing deriving (Show, Enum, Bounded, Eq, Ord, Generic)
type instance FI.VectorFor PartisanIdentity7 = V.Vector
instance S.Serialize PartisanIdentity7
instance Flat.Flat PartisanIdentity7

parsePartisanIdentity7 :: Int -> PartisanIdentity7
parsePartisanIdentity7 = fromMaybe PI7_Missing . Relude.safeToEnum . minus1 . min 9

type PartisanId7 = "PartisanId7" F.:-> PartisanIdentity7
instance FV.ToVLDataValue (F.ElField PartisanId7) where
  toVLDataValue x = (toText $ V.getLabel x, GV.Str $ show $ V.getField x)


data PartisanIdentityLeaner = PIL_Democrat
                            | PIL_Republican
                            | PIL_Independent
                            | PIL_NotSure
                            | PIL_Missing deriving (Show, Enum, Bounded, Eq, Ord, Generic)
type instance FI.VectorFor PartisanIdentityLeaner = V.Vector
instance S.Serialize PartisanIdentityLeaner
instance Flat.Flat PartisanIdentityLeaner

parsePartisanIdentityLeaner :: Int -> PartisanIdentityLeaner
parsePartisanIdentityLeaner = fromMaybe PIL_Missing . Relude.safeToEnum . minus1 . min 5

type PartisanIdLeaner = "PartisanIdLeaner" F.:-> PartisanIdentityLeaner
instance FV.ToVLDataValue (F.ElField PartisanIdLeaner) where
  toVLDataValue x = (toText $ V.getLabel x, GV.Str $ show $ V.getField x)

parseHouseVoteParty :: T.Text -> ET.PartyT
parseHouseVoteParty "Democratic" = ET.Democratic
parseHouseVoteParty "Republican" = ET.Republican
parseHouseVoteParty _ = ET.Other

type HouseVoteParty = "HouseVoteParty" F.:-> ET.PartyT

parsePres2016VoteParty :: T.Text -> ET.PartyT
parsePres2016VoteParty "Hilary Clinton" = ET.Democratic
parsePres2016VoteParty "Donald Trump" = ET.Republican
parsePres2016VoteParty _ = ET.Other

parsePres2012VoteParty :: T.Text -> ET.PartyT
parsePres2012VoteParty "Barack Obama" = ET.Democratic
parsePres2012VoteParty "Mitt Romney" = ET.Republican
parsePres2012VoteParty _ = ET.Other

parsePres2008VoteParty :: T.Text -> ET.PartyT
parsePres2008VoteParty t = if T.isInfixOf "Barack Obama" t
                           then ET.Democratic
                           else if T.isInfixOf "John McCain" t
                                then ET.Republican
                                     else ET.Other

type Pres2016VoteParty = "Pres2016VoteParty" F.:-> ET.PartyT
type Pres2012VoteParty = "Pres2012VoteParty" F.:-> ET.PartyT
type Pres2008VoteParty = "Pres2008VoteParty" F.:-> ET.PartyT

-- to use in maybeRecsToFrame
fixCCESRow :: F.Rec (Maybe F.:. F.ElField) CCES_MRP_Raw -> F.Rec (Maybe F.:. F.ElField) CCES_MRP_Raw
fixCCESRow r = (F.rsubset %~ missingHispanicToNo)
               $ (F.rsubset %~ missingPID3)
               $ (F.rsubset %~ missingPID7)
               $ (F.rsubset %~ missingPIDLeaner)
               $ (F.rsubset %~ missingEducation)
               r where
  missingHispanicToNo :: F.Rec (Maybe :. F.ElField) '[CCESHispanic] -> F.Rec (Maybe :. F.ElField) '[CCESHispanic]
  missingHispanicToNo = FM.fromMaybeMono 2
  missingPID3 :: F.Rec (Maybe :. F.ElField) '[CCESPid3] -> F.Rec (Maybe :. F.ElField) '[CCESPid3]
  missingPID3 = FM.fromMaybeMono 6
  missingPID7 :: F.Rec (Maybe :. F.ElField) '[CCESPid7] -> F.Rec (Maybe :. F.ElField) '[CCESPid7]
  missingPID7 = FM.fromMaybeMono 9
  missingPIDLeaner :: F.Rec (Maybe :. F.ElField) '[CCESPid3Leaner] -> F.Rec (Maybe :. F.ElField) '[CCESPid3Leaner]
  missingPIDLeaner = FM.fromMaybeMono 5
  missingEducation :: F.Rec (Maybe :. F.ElField) '[CCESEduc] -> F.Rec (Maybe :. F.ElField) '[CCESEduc]
  missingEducation = FM.fromMaybeMono 5

--textToInt :: T.Text -> Int
--textToInt = either (const 0) fst . Text.Read.decimal

-- fmap over Frame after load and throwing out bad rows

transformCCESRow :: F.Record CCES_MRP_Raw -> F.Record CCES_MRP
transformCCESRow = F.rcast . addCols where
  addCols = (FT.addName  @CCESYear @Year)
            . (FT.addName @CCESSt @StateAbbreviation)
            . (FT.addName @CCESDistUp @CongressionalDistrict)
            . (FT.addOneFromOne @CCESGender @DT.SexC intToSex)
            . (FT.addOneFromOne @CCESEduc @DT.EducationC intToEducation)
            . (FT.addOneFromOne @CCESEduc @DT.CollegeGradC intToCollegeGrad)
            . (FT.addOneFromOne @CCESRace @DT.Race5C (raceToRace5 . intToRaceT))
            . (FT.addOneFromOne @CCESRace @DT.SimpleRaceC (raceToSimpleRace . intToRaceT))
            . (FT.addOneFromOne @CCESHispanic @DT.HispC intToHisp)
            . (FT.addOneFromOne @CCESAge @DT.Age5C intToAgeT)
            . (FT.addOneFromOne @CCESAge @DT.SimpleAgeC intToSimpleAge)
            . (FT.addOneFromOne @CCESVvRegstatus @Registration parseRegistration)
            . (FT.addOneFromOne @CCESVvTurnoutGvm @Turnout parseTurnout)
            . (FT.addOneFromOne @CCESVotedRepParty @HouseVoteParty parseHouseVoteParty)
            . (FT.addOneFromOne @CCESVotedPres08 @Pres2008VoteParty parsePres2008VoteParty)
            . (FT.addOneFromOne @CCESVotedPres12 @Pres2012VoteParty parsePres2012VoteParty)
            . (FT.addOneFromOne @CCESVotedPres16 @Pres2016VoteParty parsePres2016VoteParty)
            . (FT.addOneFromOne @CCESPid3 @PartisanId3 parsePartisanIdentity3)
            . (FT.addOneFromOne @CCESPid7 @PartisanId7 parsePartisanIdentity7)
            . (FT.addOneFromOne @CCESPid3Leaner @PartisanIdLeaner parsePartisanIdentityLeaner)



-- map reduce folds for counting

-- some keys for aggregation
type ByCCESPredictors = '[StateAbbreviation, DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.SimpleRaceC]


--
countDVotesF
  :: forall vc cs
  . (Ord (F.Record cs)
    , FI.RecVec (cs V.++ BR.CountCols)
    , F.ElemOf CCES_MRP vc
    , V.KnownField vc
    , V.Snd vc ~ ET.PartyT
    )
  => (F.Record CCES_MRP -> F.Record cs)
  -> Int
  -> FMR.Fold (F.Record CCES_MRP) (F.FrameRec (cs V.++ BR.CountCols))
countDVotesF getKeys y =
  BR.weightedCountFold
  getKeys
  (F.rcast @[vc, CCESWeightCumulative])
  (\r ->
     (F.rgetField @BR.Year r == y)
     && (F.rgetField @vc r `elem` [ET.Republican, ET.Democratic])
  )
  ((== ET.Democratic) . F.rgetField @vc)
  (F.rgetField @CCESWeightCumulative)

{-
countDemHouseVotesF
  :: forall cs
  . (Ord (F.Record cs)
    , FI.RecVec (cs V.++ BR.CountCols)
    )
  => Int
  -> FMR.Fold
  (F.Record CCES_MRP)
  (F.FrameRec (cs V.++ BR.CountCols))
countDemHouseVotesF = countDVotesF @HouseVoteParty

countDemPres2008VotesF
  :: forall cs
  . (Ord (F.Record cs)
    , FI.RecVec (cs V.++ BR.CountCols)
    )
  => FMR.Fold
  (F.Record CCES_MRP)
  (F.FrameRec (cs V.++ BR.CountCols))
countDemPres2008VotesF = countDVotesF @Pres2008VoteParty 2008

countDemPres2012VotesF
  :: forall cs
  . (Ord (F.Record cs)
    , FI.RecVec (cs V.++ BR.CountCols)
    , cs F.⊆ CCES_MRP
    , cs F.⊆ ('[BR.StateAbbreviation] V.++ cs V.++ CCES_MRP)
    , F.ElemOf (cs V.++ CCES_MRP) Pres2012VoteParty
    , F.ElemOf (cs V.++ CCES_MRP) CCESWeightCumulative
    )
  => FMR.Fold
  (F.Record CCES_MRP)
  (F.FrameRec ('[BR.StateAbbreviation] V.++ cs V.++ BR.CountCols))
countDemPres2012VotesF =
  BR.weightedCountFold @('[BR.StateAbbreviation] V.++ cs) @CCES_MRP
    @'[Pres2012VoteParty, CCESWeightCumulative]
    (\r ->
      (F.rgetField @BR.Year r == 2012)
        && (      F.rgetField @Pres2012VoteParty r
           `elem` [ET.Republican, ET.Democratic]
           )
    )
    ((== ET.Democratic) . F.rgetField @Pres2012VoteParty)
    (F.rgetField @CCESWeightCumulative)

countDemPres2016VotesF
  :: forall cs
  . (Ord (F.Record cs)
    , FI.RecVec (cs V.++ BR.CountCols)
    , cs F.⊆ CCES_MRP
    , cs F.⊆ ('[BR.StateAbbreviation] V.++ cs V.++ CCES_MRP)
    , F.ElemOf (cs V.++ CCES_MRP) Pres2016VoteParty
    , F.ElemOf (cs V.++ CCES_MRP) CCESWeightCumulative
    )
  => FMR.Fold
  (F.Record CCES_MRP)
  (F.FrameRec ('[BR.StateAbbreviation] V.++ cs V.++ BR.CountCols))
countDemPres2016VotesF =
  BR.weightedCountFold @('[BR.StateAbbreviation] V.++ cs) @CCES_MRP
    @'[Pres2016VoteParty, CCESWeightCumulative]
    (\r ->
      (F.rgetField @BR.Year r == 2016)
        && (      F.rgetField @Pres2016VoteParty r
           `elem` [ET.Republican, ET.Democratic]
           )
    )
    ((== ET.Democratic) . F.rgetField @Pres2016VoteParty)
    (F.rgetField @CCESWeightCumulative)
-}

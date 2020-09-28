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
{-# LANGUAGE QuasiQuotes         #-}
{-# LANGUAGE TemplateHaskell     #-}
{-# LANGUAGE TypeFamilies        #-}
{-# LANGUAGE TypeOperators       #-}
{-# LANGUAGE TypeApplications    #-}
{-# LANGUAGE Rank2Types          #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TupleSections       #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE UndecidableInstances #-}
{-# OPTIONS_GHC -O0              #-}
module BlueRipple.Data.CCES
  (
    module BlueRipple.Data.CCES
  , module BlueRipple.Data.CCESFrame
  )
  where

import           BlueRipple.Data.DataFrames
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.ElectionTypes as ET
import qualified BlueRipple.Model.MRP as BR
import qualified BlueRipple.Data.LoadersCore as BR
import qualified BlueRipple.Data.Loaders as BR
import qualified BlueRipple.Data.Keyed as Keyed

import           BlueRipple.Data.CCESFrame

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

import qualified Frames                        as F
import           Frames                         ( (:.)(..) )
import qualified Frames.InCore                 as FI
import qualified Frames.Melt                   as F

import qualified Frames.Transform              as FT
import qualified Frames.MaybeUtils             as FM

import qualified Frames.Serialize              as FS
import qualified Frames.Visualization.VegaLite.Data
                                               as FV
import qualified Graphics.Vega.VegaLite        as GV

import qualified Numeric.GLM.ProblemTypes      as GLM
import qualified Numeric.GLM.Predict            as GLM

import qualified Data.Vector                   as V
import           GHC.Generics                   ( Generic, Rep )

import qualified Knit.Report as K
import qualified Polysemy.Error                as P (mapError)
import qualified Polysemy                as P (raise)


ccesDataLoader :: (K.KnitEffects r, K.CacheEffectsD r) => K.Sem r (K.ActionWithCacheTime r (F.FrameRec CCES_MRP))
ccesDataLoader = K.wrapPrefix "ccesDataLoader"
                 $ BR.cachedMaybeFrameLoader @(F.RecordColumns CCES) @CCES_MRP_Raw @CCES_MRP
                 (BR.LocalData $ T.pack ccesCSV)
                 Nothing
                 Nothing
                 fixCCESRow
                 transformCCESRow
                 Nothing
                 "ccesMRP.sbin"
                          
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

-- these are orphans but where could they go?
-- I guess we could newtype "ElField" somehow, just for serialization? Then coerce back and forth...
--instance (S.Serialize (V.Snd t), V.KnownField t) => S.Serialize (F.ElField t)
--instance S.Serialize (F.Record CCES_MRP)

-- first try, order these consistently with the data and use (toEnum . (-1)) when possible
minus1 x = x - 1

-- can't do toEnum here because we have Female first
intToSex :: Int -> DT.Sex
intToSex n = case n of
  1 -> DT.Male
  2 -> DT.Female
  _ -> undefined

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

intToRaceT :: Int -> RaceT
intToRaceT = toEnum . minus1

type Race = "Race" F.:-> RaceT
instance FV.ToVLDataValue (F.ElField Race) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Str $ T.pack $ show $ V.getField x)

raceToSimpleRace :: RaceT -> DT.SimpleRace
raceToSimpleRace White = DT.White
raceToSimpleRace _ = DT.NonWhite


raceToRace5 :: RaceT -> DT.Race5
raceToRace5 White = DT.R5_WhiteNonLatinx
raceToRace5 Black = DT.R5_Black
raceToRace5 Hispanic = DT.R5_Latinx
raceToRace5 Asian = DT.R5_Asian
raceToRace5 _ = DT.R5_Other


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
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Str $ T.pack $ show $ V.getField x)

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

parseTurnout :: T.Text -> TurnoutT
parseTurnout "Voted" = T_Voted
parseTurnout "No Record Of Voting" = T_NoRecord
parseTurnout "No Voter File" = T_NoFile
parseTurnout _ = T_Missing

type Turnout = "Turnout" F.:-> TurnoutT
instance FV.ToVLDataValue (F.ElField Turnout) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Str $ T.pack $ show $ V.getField x)


data PartisanIdentity3 = PI3_Democrat
                       | PI3_Republican
                       | PI3_Independent
                       | PI3_Other
                       | PI3_NotSure
                       | PI3_Missing deriving (Show, Enum, Bounded, Eq, Ord, Generic)
type instance FI.VectorFor PartisanIdentity3 = V.Vector
instance S.Serialize PartisanIdentity3

parsePartisanIdentity3 :: Int -> PartisanIdentity3
parsePartisanIdentity3 = toEnum . minus1 . min 6

type PartisanId3 = "PartisanId3" F.:-> PartisanIdentity3
instance FV.ToVLDataValue (F.ElField PartisanId3) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Str $ T.pack $ show $ V.getField x)


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

parsePartisanIdentity7 :: Int -> PartisanIdentity7
parsePartisanIdentity7 = toEnum . minus1 . min 9

type PartisanId7 = "PartisanId7" F.:-> PartisanIdentity7
instance FV.ToVLDataValue (F.ElField PartisanId7) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Str $ T.pack $ show $ V.getField x)


data PartisanIdentityLeaner = PIL_Democrat
                            | PIL_Republican
                            | PIL_Independent
                            | PIL_NotSure
                            | PIL_Missing deriving (Show, Enum, Bounded, Eq, Ord, Generic)
type instance FI.VectorFor PartisanIdentityLeaner = V.Vector
instance S.Serialize PartisanIdentityLeaner

parsePartisanIdentityLeaner :: Int -> PartisanIdentityLeaner
parsePartisanIdentityLeaner = toEnum . minus1 . min 5

type PartisanIdLeaner = "PartisanIdLeaner" F.:-> PartisanIdentityLeaner
instance FV.ToVLDataValue (F.ElField PartisanIdLeaner) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Str $ T.pack $ show $ V.getField x)

{-
data VotePartyT = VP_Democratic | VP_Republican | VP_Other deriving (Show, Enum, Bounded, Eq, Ord, Generic)
type instance FI.VectorFor VotePartyT = V.Vector
instance S.Serialize VotePartyT
-}

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

{-                                          
parsePres2008VoteParty "Barack Obama" = VP_Democratic
parsePres2008VoteParty "John McCain" = VP_Republican
parsePres2008VoteParty _ = VP_Other
-}

type Pres2016VoteParty = "Pres2016VoteParty" F.:-> ET.PartyT
type Pres2012VoteParty = "Pres2012VoteParty" F.:-> ET.PartyT 
type Pres2008VoteParty = "Pres2008VoteParty" F.:-> ET.PartyT

{-
data OfficeT = House | Senate | President deriving (Show,  Enum, Bounded, Eq, Ord, Generic)
type instance FI.VectorFor OfficeT = V.Vector
instance S.Serialize OfficeT


type Office = "Office" F.:-> OfficeT
instance FV.ToVLDataValue (F.ElField MRP.CCES.Office) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Str $ T.pack $ show $ V.getField x)
-}

-- to use in maybeRecsToFrame
fixCCESRow :: F.Rec (Maybe F.:. F.ElField) CCES_MRP_Raw -> F.Rec (Maybe F.:. F.ElField) CCES_MRP_Raw
fixCCESRow r = (F.rsubset %~ missingHispanicToNo)
               $ (F.rsubset %~ missingPID3)
               $ (F.rsubset %~ missingPID7)
               $ (F.rsubset %~ missingPIDLeaner)
               $ (F.rsubset %~ missingEducation)
               $ r where
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
  intsToRace h r = if (h == 1) then Hispanic else intToRaceT r
  addCols = (FT.addName  @CCESYear @Year)
            . (FT.addName @CCESSt @StateAbbreviation)
            . (FT.addName @CCESDistUp @CongressionalDistrict)
            . (FT.addOneFromOne @CCESGender @DT.SexC intToSex)
            . (FT.addOneFromOne @CCESEduc @DT.EducationC intToEducation)
            . (FT.addOneFromOne @CCESEduc @DT.CollegeGradC intToCollegeGrad)
            . (FT.addOneFrom @[CCESHispanic, CCESRace] @DT.Race5C (\x y -> raceToRace5 $ intsToRace x y))
            . (FT.addOneFrom @[CCESHispanic, CCESRace] @DT.SimpleRaceC (\h r -> raceToSimpleRace $ intsToRace h r))
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
{-
type ByStateSex = '[StateAbbreviation, BR.SexC]
type ByStateSexRace = '[StateAbbreviation, BR.SexC, BR.SimpleRaceC]
type ByStateSexRaceAge = '[StateAbbreviation, BR.SexC, BR.SimpleRaceC, BR.SimpleAgeC]
type ByStateSexEducationAge = '[StateAbbreviation, BR.SexC, BR.CollegeGradC, BR.SimpleAgeC]
type ByStateSexRaceEducation = '[StateAbbreviation, BR.SexC, BR.SimpleRaceC, BR.CollegeGradC]
type ByStateSexRaceEducationAge = '[StateAbbreviation, BR.SexC, BR.SimpleRaceC, BR.CollegeGradC, BR.SimpleAgeC]
type ByStateRaceEducation = '[StateAbbreviation, BR.SimpleRaceC, BR.CollegeGradC]



data CCESPredictor = P_Sex | P_WWC | P_Race | P_Education | P_Age deriving (Show, Eq, Ord, Enum, Bounded)
type CCESEffect = GLM.WithIntercept CCESPredictor

ccesPredictor :: forall r. (F.ElemOf r BR.SexC
                           , F.ElemOf r BR.SimpleRaceC
                           , F.ElemOf r BR.CollegeGradC
                           , F.ElemOf r BR.SimpleAgeC) => F.Record r -> CCESPredictor -> Double
ccesPredictor r P_Sex       = if F.rgetField @BR.SexC r == BR.Female then 0 else 1
ccesPredictor r P_Race      = if F.rgetField @BR.SimpleRaceC r == BR.NonWhite then 0 else 1 -- non-white is baseline
ccesPredictor r P_Education = if F.rgetField @BR.CollegeGradC r == BR.NonGrad then 0 else 1 -- non-college is baseline
ccesPredictor r P_Age       = if F.rgetField @BR.SimpleAgeC r == BR.EqualOrOver then 0 else 1 -- >= 45  is baseline
ccesPredictor r P_WWC       = if (F.rgetField @BR.SimpleRaceC r == BR.White) && (F.rgetField @BR.CollegeGradC r == BR.NonGrad) then 1 else 0
-}
{-    
-- we newtype this so we can derive instances for all the things
newtype  SimplePredictor ps = CCESSimplePredictor { unSimplePredictor :: F.Record ps }

instance (Show (F.Record ps)) => Show (SimplePredictor ps) where
  show (SimplePredictor x) = "SimplePredictor " ++ (show x)
  
instance (Eq (F.Record ps)) => Eq (SimplePredictor ps) where
  (SimplePredictor x) == (SimplePredictor y) = x == y
  
instance (Ord (F.Record ps)) => Ord (SimplePredictor ps) where
  compare (SimplePredictor x) (SimplePredictor y) = compare x y

instance (Ord (F.Record ps), Keyed.FiniteSet (F.Record ps)) => Enum (SimplePredictor ps) where
  toEnum n =
    let im = IM.fromList $ zip [0..] $ fmap SimplePredictor $ Set.toAscList Keyed.elements
    in fromJust $ IM.lookup n im
  fromEnum a =
    let m = M.fromList $ zip (fmap SimplePredictor $ Set.toAscList Keyed.elements) [0..]
    in fromJust $ M.lookup a m

instance (Keyed.FiniteSet (F.Record ps)) => Bounded (SimplePredictor ps) where
  minBound = head $ fmap SimplePredictor $ Set.toList $ Keyed.elements
  maxBound = last $ fmap SimplePredictor $ Set.toList $ Keyed.elements


allSimplePredictors :: Keyed.FiniteSet (F.Record ps) => [SimplePredictor ps]
allSimplePredictors = fmap SimplePredictor $ Set.toList Keyed.elements 

type SimpleEffect ps = GLM.WithIntercept (SimplePredictor ps)

simplePredictor :: forall ps rs. (ps F.⊆ rs
                                     , Eq (F.Record ps)
                                     )
                    => F.Record rs -> SimplePredictor ps -> Double
simplePredictor r p = if (F.rcast @ps r == unCCESSimplePredictor p) then 1 else 0

predMap :: forall cs. (Keyed.FiniteSet (F.Record cs), Ord (F.Record cs), cs F.⊆ cs)
  => F.Record cs -> M.Map (SimplePredictor cs) Double
predMap r =  M.fromList $ fmap (\p -> (p, simplePredictor r p)) allSimplePredictors

catPredMaps :: forall cs.  (Keyed.FiniteSet (F.Record cs), Ord (F.Record cs), cs F.⊆ cs)
  => M.Map (F.Record cs) (M.Map (SimplePredictor cs) Double)
catPredMaps = M.fromList $ fmap (\k -> (unSimplePredictor k,predMap (unSimplePredictor k))) allSimplePredictors  

data  LocationHolder c f a =  LocationHolder { locName :: T.Text
                                             , locKey :: Maybe (F.Rec f LocationCols)
                                             , catData :: M.Map (F.Rec f c) a
                                             } deriving (Generic)

deriving instance (V.RMap c
                  , V.ReifyConstraint Show F.ElField c
                  , V.RecordToList c
                  , Show a) => Show (LocationHolder c F.ElField a)
                  
instance (S.Serialize a
         , Ord (F.Rec FS.SElField c)
         , S.GSerializePut
           (Rep (F.Rec FS.SElField c))
         , S.GSerializeGet (Rep (F.Rec FS.SElField c))
         , (Generic (F.Rec FS.SElField c))
         ) => S.Serialize (LocationHolder c FS.SElField a)

lhToS :: (Ord (F.Rec FS.SElField c)
         , V.RMap c
         )
      => LocationHolder c F.ElField a -> LocationHolder c FS.SElField a
lhToS (LocationHolder n lkM cdm) = LocationHolder n (fmap FS.toS lkM) (M.mapKeys FS.toS cdm)

lhFromS :: (Ord (F.Rec F.ElField c)
           , V.RMap c
         ) => LocationHolder c FS.SElField a -> LocationHolder c F.ElField a
lhFromS (LocationHolder n lkM cdm) = LocationHolder n (fmap FS.fromS lkM) (M.mapKeys FS.fromS cdm)

type LocationCols = '[StateAbbreviation]
locKeyPretty :: F.Record LocationCols -> T.Text
locKeyPretty r =
  let stateAbbr = F.rgetField @StateAbbreviation r
  in stateAbbr

--type ASER = '[BR.SimpleAgeC, BR.SexC, BR.CollegeGradC, BR.SimpleRaceC]
predictionsByLocation ::
  forall ps r rs. ( V.RMap ps
                  , V.ReifyConstraint Show V.ElField ps
                  , V.RecordToList ps
                  , Ord (F.Record ps)
                  , Ord (SimplePredictor ps)               
                  , FI.RecVec (ps V.++ BR.CountCols)
                  , F.ElemOf (ps V.++ BR.CountCols) BR.Count
                  , F.ElemOf (ps V.++ BR.CountCols) BR.MeanWeight
                  , F.ElemOf (ps V.++ BR.CountCols) BR.UnweightedSuccesses
                  , F.ElemOf (ps V.++ BR.CountCols) BR.VarWeight
                  , F.ElemOf (ps V.++ BR.CountCols) BR.WeightedSuccesses
                  , Keyed.FiniteSet (F.Record ps)
                  , Show (F.Record (LocationCols V.++ ps V.++ BR.CountCols))
                  , Show (F.Record ps)
                  , Enum (SimplePredictor ps)
                  , Bounded (SimplePredictor ps)
                  , (ps V.++ BR.CountCols) F.⊆ (LocationCols V.++ ps V.++ BR.CountCols)
                  , ps F.⊆ (ps V.++ BR.CountCols)
                  , ps F.⊆ (LocationCols V.++ ps V.++ BR.CountCols)
                  , K.KnitEffects r
               )
  => K.Sem r (F.FrameRec rs)
  -> FL.Fold (F.Record rs) (F.FrameRec (LocationCols V.++ ps V.++ BR.CountCols))  
  -> [SimpleEffect ps]
  -> M.Map (F.Record ps) (M.Map (SimplePredictor ps) Double)
  -> K.Sem r [LocationHolder ps V.ElField Double]
predictionsByLocation ccesFrameAction countFold predictors catPredMap = P.mapError BR.glmErrorToPandocError $ K.wrapPrefix "predictionsByLocation" $ do
  K.logLE K.Diagnostic "Starting (getting CCES data)"
  ccesFrame <- P.raise ccesFrameAction --F.toFrame <$> P.raise (K.useCached ccesRecordListAllCA)
  K.logLE K.Diagnostic ("Inferring")
  (mm, rc, ebg, bu, vb, bs) <- BR.inferMR @LocationCols @ps @ps
                               countFold
                               predictors                                                     
                               simplePredictor
                               ccesFrame
  
  let states = FL.fold FL.set $ fmap (F.rgetField @StateAbbreviation) ccesFrame
      allStateKeys = fmap (\s -> s F.&: V.RNil) $ FL.fold FL.list states
      predictLoc l = LocationHolder (locKeyPretty l) (Just l) catPredMap
      toPredict = [LocationHolder "National" Nothing catPredMap] <> fmap predictLoc allStateKeys                           
      predict (LocationHolder n lkM cpms) = P.mapError BR.glmErrorToPandocError $ do
        let predictFrom catKey predMap =
              let groupKeyM = fmap (`V.rappend` catKey) lkM --lkM >>= \lk -> return $ lk `V.rappend` catKey
                  emptyAsNationalGKM = case groupKeyM of
                                         Nothing -> Nothing
                                         Just k -> fmap (const k) $ GLM.categoryNumberFromKey rc k (BR.RecordColsProxy @(LocationCols V.++ ps))
              in GLM.predictFromBetaUB mm (flip M.lookup predMap) (const emptyAsNationalGKM) rc ebg bu vb
        cpreds <- M.traverseWithKey predictFrom cpms
        return $ LocationHolder n lkM cpreds
  traverse predict toPredict
-}

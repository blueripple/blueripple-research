{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE DeriveGeneric       #-}
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
module MRP.CCES
  (
    module MRP.CCES
  , module MRP.CCESFrame
  )
  where

import           BlueRipple.Data.DataFrames
import qualified BlueRipple.Data.DemographicTypes as BR
import qualified BlueRipple.Data.ElectionTypes as ET
import qualified BlueRipple.Data.PrefModel.SimpleAgeSexEducation as BR
import qualified BlueRipple.Data.PrefModel.SimpleAgeSexRace as BR
import qualified BlueRipple.Model.MRP_Pref as BR
import qualified BlueRipple.Data.Loaders as BR

import           MRP.CCESFrame

import qualified Control.Foldl                 as FL
import           Control.Lens                   ((%~))
import qualified Control.Monad.Except          as X
import qualified Control.Monad.State           as ST
import qualified Data.Array                    as A
import qualified Data.Serialize                as S
import qualified Data.Serialize.Text           as S
import qualified Data.List                     as L
import qualified Data.Map                      as M
import           Data.Maybe                     ( fromMaybe)
import           Data.Proxy                     ( Proxy(..) )
import qualified Data.Text                     as T
import           Data.Text                      ( Text )
import           Text.Read                      (readMaybe)
import           Data.Ix                        ( Ix )
import qualified Data.Vinyl                    as V
import qualified Data.Vinyl.TypeLevel          as V
import qualified Data.Vinyl.Functor            as V
import qualified Frames                        as F
import           Frames                         ( (:.)(..) )
import qualified Frames.CSV                    as F
import qualified Frames.InCore                 as FI
import qualified Frames.TH                     as F
import qualified Frames.Melt                   as F
import qualified Text.Read                     as TR

import qualified Frames.Folds                  as FF
import qualified Frames.ParseableTypes         as FP
import qualified Frames.Transform              as FT
import qualified Frames.MaybeUtils             as FM
import qualified Frames.MapReduce              as MR
import qualified Frames.Enumerations           as FE
import qualified Frames.Serialize              as FS
import qualified Frames.Visualization.VegaLite.Data
                                               as FV
import qualified Graphics.Vega.VegaLite        as GV

import qualified Data.IndexedSet               as IS
import qualified Numeric.GLM.ProblemTypes      as GLM
import qualified Numeric.GLM.ModelTypes      as GLM
import qualified Numeric.GLM.Predict            as GLM
import qualified Numeric.LinearAlgebra         as LA

import           Data.Hashable                  ( Hashable )
import qualified Data.Vector                   as V
--import qualified Data.Vector.Boxed             as VB
import           GHC.Generics                   ( Generic, Rep )

import qualified Knit.Report as K
import qualified Polysemy.Error                as P (mapError, Error)
import qualified Polysemy                as P (raise)


import GHC.TypeLits (Symbol)
import Data.Kind (Type)

ccesDataLoader :: K.KnitEffects r => K.Sem r (F.FrameRec CCES_MRP)
ccesDataLoader = BR.cachedMaybeFrameLoader @CCES_MRP_Raw @CCES_MRP_Raw @CCES_MRP
                 (BR.LocalData $ T.pack ccesCSV)
                 Nothing
                 (const True)
                 fixCCESRow
                 transformCCESRow
                 Nothing
                 "ccesMRP.bin"
                          
type CCES_MRP_Raw = '[ CCESYear
                     , CCESCaseId
                     , CCESWeight
                     , CCESWeightCumulative
                     , CCESSt
                     , CCESDist
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
                 , BR.SexC
                 , Age
                 , BR.SimpleAgeC
                 , Education
                 , BR.CollegeGradC
                 , Race
                 , BR.SimpleRaceC
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
intToSex :: Int -> BR.Sex
intToSex n = case n of
  1 -> BR.Male
  2 -> BR.Female
  _ -> undefined

{-
type Sex = "Sex" F.:-> BR.Sex
instance FV.ToVLDataValue (F.ElField Sex) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Str $ T.pack $ show $ V.getField x)
-}

data EducationT = E_NoHS
                | E_HighSchool
                | E_SomeCollege
                | E_TwoYear
                | E_FourYear
                | E_PostGrad
                | E_Missing deriving (Show, Enum, Bounded, Eq, Ord, Generic)
type instance FI.VectorFor EducationT = V.Vector
instance S.Serialize EducationT

intToEducationT :: Int -> EducationT
intToEducationT = toEnum . minus1 . min 7

intToCollegeGrad :: Int -> BR.CollegeGrad
intToCollegeGrad n = if n >= 4 then BR.Grad else BR.NonGrad

type Education = "Education" F.:-> EducationT
{-
type CollegeGrad = "CollegeGrad" F.:-> BR.CollegeGrad
instance FV.ToVLDataValue (F.ElField CollegeGrad) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Str $ T.pack $ show $ V.getField x)
-}

data RaceT = White | Black | Hispanic | Asian | NativeAmerican | Mixed | Other | MiddleEastern deriving (Show, Enum, Bounded, Eq, Ord, Generic)
type instance FI.VectorFor RaceT = V.Vector
instance S.Serialize RaceT

intToRaceT :: Int -> RaceT
intToRaceT = toEnum . minus1

type Race = "Race" F.:-> RaceT
instance FV.ToVLDataValue (F.ElField Race) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Str $ T.pack $ show $ V.getField x)

raceToSimpleRace :: RaceT -> BR.SimpleRace
raceToSimpleRace White = BR.White
raceToSimpleRace _ = BR.NonWhite

{-
type SimpleRace = "SimpleRace" F.:-> BR.SimpleRace
instance FV.ToVLDataValue (F.ElField SimpleRace) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Str $ T.pack $ show $ V.getField x)
-}

data AgeT = A18To24 | A25To44 | A45To64 | A65To74 | A75AndOver deriving (Show, Enum, Bounded, Eq, Ord, Generic)
type instance FI.VectorFor AgeT = V.Vector
instance S.Serialize AgeT

intToAgeT :: Real a => a -> AgeT
intToAgeT x 
  | x < 25 = A18To24
  | x < 45 = A25To44
  | x < 65 = A45To64
  | x < 75 = A65To74
  | otherwise = A75AndOver

intToSimpleAge :: Int -> BR.SimpleAge
intToSimpleAge n = if n < 45 then BR.Under else BR.EqualOrOver

type Age = "Age" F.:-> AgeT

{-
type SimpleAge = "Under45" F.:-> BR.SimpleAge
instance FV.ToVLDataValue (F.ElField SimpleAge) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Str $ T.pack $ show $ V.getField x)
-}

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
  
-- fmap over Frame after load and throwing out bad rows
transformCCESRow :: F.Record CCES_MRP_Raw -> F.Record CCES_MRP
transformCCESRow r = F.rcast @CCES_MRP (mutate r) where
  addGender = FT.recordSingleton @BR.SexC . intToSex . F.rgetField @CCESGender
  addEducation = FT.recordSingleton @Education . intToEducationT . F.rgetField @CCESEduc
  addCollegeGrad = FT.recordSingleton @BR.CollegeGradC . intToCollegeGrad . F.rgetField @CCESEduc
  rInt q = F.rgetField @CCESRace q
  hInt q = F.rgetField @CCESHispanic q
  race q = if (hInt q == 1) then Hispanic else intToRaceT (rInt q)
  addRace = FT.recordSingleton @Race . race 
  addSimpleRace = FT.recordSingleton @BR.SimpleRaceC . raceToSimpleRace . race 
  addAge = FT.recordSingleton @Age . intToAgeT . F.rgetField @CCESAge
  addSimpleAge = FT.recordSingleton @BR.SimpleAgeC . intToSimpleAge . F.rgetField @CCESAge
  addRegistration = FT.recordSingleton @Registration . parseRegistration  . F.rgetField @CCESVvRegstatus
  addTurnout = FT.recordSingleton @Turnout . parseTurnout . F.rgetField @CCESVvTurnoutGvm
  addHouseVoteParty = FT.recordSingleton @HouseVoteParty . parseHouseVoteParty . F.rgetField @CCESVotedRepParty
  addPres2008VoteParty = FT.recordSingleton @Pres2008VoteParty . parsePres2008VoteParty . F.rgetField @CCESVotedPres08
  addPres2012VoteParty = FT.recordSingleton @Pres2012VoteParty . parsePres2012VoteParty . F.rgetField @CCESVotedPres12
  addPres2016VoteParty = FT.recordSingleton @Pres2016VoteParty . parsePres2016VoteParty . F.rgetField @CCESVotedPres16
  addPID3 = FT.recordSingleton @PartisanId3 . parsePartisanIdentity3 . F.rgetField @CCESPid3
  addPID7 = FT.recordSingleton @PartisanId7 . parsePartisanIdentity7 . F.rgetField @CCESPid7
  addPIDLeaner = FT.recordSingleton @PartisanIdLeaner . parsePartisanIdentityLeaner . F.rgetField @CCESPid3Leaner
  mutate = FT.retypeColumn @CCESYear @Year
           . FT.retypeColumn @CCESSt @StateAbbreviation
           . FT.retypeColumn @CCESDistUp @CongressionalDistrict -- could be CCES_Dist or CCES_DistUp
           . FT.mutate addGender
           . FT.mutate addEducation
           . FT.mutate addCollegeGrad
           . FT.mutate addRace
           . FT.mutate addSimpleRace
           . FT.mutate addAge
           . FT.mutate addSimpleAge
           . FT.mutate addRegistration
           . FT.mutate addTurnout
           . FT.mutate addHouseVoteParty
           . FT.mutate addPres2008VoteParty
           . FT.mutate addPres2012VoteParty
           . FT.mutate addPres2016VoteParty           
           . FT.mutate addPID3
           . FT.mutate addPID7
           . FT.mutate addPIDLeaner



-- map reduce folds for counting

-- some keys for aggregation
type ByStateSex = '[StateAbbreviation, BR.SexC]
type ByStateSexRace = '[StateAbbreviation, BR.SexC, BR.SimpleRaceC]
type ByStateSexRaceAge = '[StateAbbreviation, BR.SexC, BR.SimpleRaceC, BR.SimpleAgeC]
type ByStateSexEducationAge = '[StateAbbreviation, BR.SexC, BR.CollegeGradC, BR.SimpleAgeC]
type ByStateSexRaceEducation = '[StateAbbreviation, BR.SexC, BR.SimpleRaceC, BR.CollegeGradC]
type ByStateSexRaceEducationAge = '[StateAbbreviation, BR.SexC, BR.SimpleRaceC, BR.CollegeGradC, BR.SimpleAgeC]
type ByStateRaceEducation = '[StateAbbreviation, BR.SimpleRaceC, BR.CollegeGradC]


type ByCCESPredictors = '[StateAbbreviation, BR.SimpleAgeC, BR.SexC, BR.CollegeGradC, BR.SimpleRaceC]
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

type ASER = '[BR.SimpleAgeC, BR.SexC, BR.CollegeGradC, BR.SimpleRaceC]
predictionsByLocation ::
  forall cc r. (cc F.⊆ (LocationCols V.++ ASER V.++ BR.CountCols)
               , Show (F.Record cc)
               , V.RMap cc
               , V.ReifyConstraint Show V.ElField cc
               , V.RecordToList cc
               , Ord (F.Record cc)
               , K.KnitEffects r
             )
  => K.Sem r (F.FrameRec CCES_MRP)
  -> FL.Fold (F.Record CCES_MRP) (F.FrameRec (LocationCols V.++ ASER V.++ BR.CountCols))  
  -> [GLM.WithIntercept CCESPredictor]
  -> M.Map (F.Record cc) (M.Map CCESPredictor Double)
  -> K.Sem r [LocationHolder cc V.ElField Double]
predictionsByLocation ccesFrameAction countFold predictors catPredMap = P.mapError BR.glmErrorToPandocError $ do
  ccesFrame <- P.raise ccesFrameAction --F.toFrame <$> P.raise (K.useCached ccesRecordListAllCA)
  (mm, rc, ebg, bu, vb, bs) <- BR.inferMR @LocationCols @cc @[BR.SimpleAgeC
                                                             ,BR.SexC
                                                             ,BR.CollegeGradC
                                                             ,BR.SimpleRaceC]
                                                             countFold
                                                             predictors                                                     
                                                             ccesPredictor
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
                                         Just k -> fmap (const k) $ GLM.categoryNumberFromKey rc k (BR.RecordColsProxy @(LocationCols V.++ cc))
              in GLM.predictFromBetaUB mm (flip M.lookup predMap) (const emptyAsNationalGKM) rc ebg bu vb
        cpreds <- M.traverseWithKey predictFrom cpms
        return $ LocationHolder n lkM cpreds
  traverse predict toPredict

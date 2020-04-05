{-# LANGUAGE AllowAmbiguousTypes  #-}
{-# LANGUAGE ConstraintKinds      #-}
{-# LANGUAGE DataKinds            #-}
{-# LANGUAGE PolyKinds            #-}
{-# LANGUAGE GADTs                #-}
{-# LANGUAGE FlexibleContexts     #-}
{-# LANGUAGE FlexibleInstances    #-}
{-# LANGUAGE OverloadedStrings    #-}
{-# LANGUAGE TypeFamilies         #-}
{-# LANGUAGE TypeOperators        #-}
{-# LANGUAGE TypeApplications     #-}
{-# LANGUAGE Rank2Types           #-}
{-# LANGUAGE ScopedTypeVariables  #-}
{-# LANGUAGE UndecidableInstances       #-}
{-# OPTIONS_GHC -O0 -freduction-depth=0 #-}
module BlueRipple.Data.CPSVoterPUMS
  (
    cpsVoterPUMSLoader 
  ) where


import qualified BlueRipple.Data.CPSVoterPUMS.CPSVoterPUMS_Frame as BR
import qualified BlueRipple.Data.DemographicTypes as BR
import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Data.Loaders as BR
import qualified BlueRipple.Data.Keyed as BR
import qualified BlueRipple.Utilities.KnitUtils as BR

import qualified Control.Foldl                 as FL
import           Control.Lens                   ((%~))
import qualified Control.Monad.Except          as X
import qualified Control.Monad.State           as ST
import qualified Data.Array                    as A
import qualified Data.Serialize                as S
import qualified Data.Serialize.Text           as S
import qualified Data.List                     as L
import qualified Data.Map                      as M
import           Data.Maybe                     ( fromMaybe, catMaybes)
import qualified Data.Text                     as T
import           Data.Text                      ( Text )
import           Text.Read                      (readMaybe)
import qualified Data.Vinyl                    as V
import           Data.Vinyl.TypeLevel                     (type (++))
import qualified Data.Vinyl.TypeLevel          as V
import qualified Data.Vinyl.Functor            as V
import qualified Frames                        as F
import           Data.Vinyl.Lens               (type (⊆))
import           Frames                         ( (:.)(..) )
import qualified Frames.CSV                    as F
import qualified Frames.InCore                 as FI
import qualified Frames.TH                     as F
import qualified Frames.Melt                   as F
import qualified Text.Read                     as TR

import qualified Frames.Folds                  as FF
import qualified Frames.MapReduce              as FMR
import qualified Frames.ParseableTypes         as FP
import qualified Frames.Transform              as FT
import qualified Frames.MaybeUtils             as FM
import qualified Frames.Utils                  as FU
import qualified Frames.MapReduce              as MR
import qualified Frames.Enumerations           as FE
import qualified Frames.Serialize              as FS
import qualified Frames.SimpleJoins            as FJ
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

cpsVoterPUMSLoader :: K.KnitEffects r => K.Sem r (F.FrameRec CPSVoterPUMS)
cpsVoterPUMSLoader = BR.cachedMaybeFrameLoader @CPSVoterPUMS_Raw @(F.RecordColumns @CPSVoterPUMS_All) @CPSVoterPUMS
                       (BR.LocalData $ T.pack BR.cpsVoterPUMSCSV)
                       Nothing
                       (const True)
                       fixCPSVoterPUMSRow
                       transformCPSVoterPUMSRow
                       Nothing
                       "cpsVoterPUMS.bin"


type CPSVoterPUMS_Raw = '[ CPSYEAR
                         , CPSSTATEFIP
                         , CPSCOUNTY
                         , CPSAGE
                         , CPSSEX
                         , CPSRACE
                         , CPSEDUC
                         , CPSSCHLCOLL
                         , CPSVOWHYNOT
                         , CPSVOTEHOW
                         , CPSVOTEWHEN
                         , CPSVOTED
                         , CPSVOREG
                         , CPSVOSUPPWT
                         ]

type CPSVoterPUMSWeight = "CPSVoterPUMSWeight" F.:-> Double

type CPSVoterPUMS = '[ Year
                     , StateFIPS
                     , CountyFIPS
                     , BR.Age4C
                     , BR.SexC
                     , BR.Race5C
                     , BR.CollegeGradC
                     , BR.InCollege
                     , BR.VoteWhyNotC
                     , BR.VoteHowC
                     , BR.VoteWhenC
                     , BR.VotedC
                     , BR.Registered
                     , CPSVoterPUMSWeight
                     ]


cpsKeysToASER :: Bool -> F.Record '[BR.Age4C, BR.SexC, BR.CollegeGradC, BR.InCollege, BR.Race5C] -> F.Record BR.CatColsASER
cpsKeysToASER addInCollegeToGrads r =
  let cg = F.rgetField @BR.CollegeGradC r
      ic = addInCollegeToGrads && F.rgetField @InCollege r
  in (BR.age4ToSimple $ F.rgetField @BR.Age4C r)
     F.&: (F.rgetField @BR.SexC r)
     F.&: (if (cg == BR.Grad || ic) then BR.Grad else BR.NonGrad)
     F.&: (BR.simpleRaceFromRace5 $ F.rgetField @BR.Race5C r)
     F.&: V.RNil

cpsKeysToASE :: Bool -> F.Record '[BR.Age4C, BR.SexC, BR.CollegeGradC, BR.InCollege, BR.Race5C] -> F.Record BR.CatColsASE
cpsKeysToASE addInCollegeToGrads r =
  let cg = F.rgetField @BR.CollegeGradC r
      ic = addInCollegeToGrads && F.rgetField @InCollege r
  in (BR.age4ToSimple $ F.rgetField @BR.Age4C r)
     F.&: (F.rgetField @BR.SexC r)
     F.&: (if (cg == BR.Grad || ic) then BR.Grad else BR.NonGrad)
     F.&: V.RNil     

cpsKeysToASR :: F.Record '[BR.Age4C, BR.SexC, BR.CollegeGradC, BR.InCollege, BR.Race5C] -> F.Record BR.CatColsASR
cpsKeysToASR r =
  (BR.age4ToSimple $ F.rgetField @BR.Age4C r)
  F.&: (F.rgetField @BR.SexC r)
  F.&: (BR.simpleRaceFromRace5 $ F.rgetField @BR.Race5C r)
  F.&: V.RNil

cpsKeysToIdentity :: F.Record '[BR.Age4C, BR.SexC, BR.CollegeGradC, BR.InCollege, BR.Race5C] -> F.Record '[]
cpsKeysToIdentity = const V.RNil

-- we have to drop all records with age < 18
intToAge4 :: Int -> BR.Age4
intToAge4 n
  | n < 18 = error "CPS Voter record with age < 18"
  | n < 24 = BR.A4_18To24
  | n < 45 = BR.A4_25To44
  | n < 65 = BR.A4_45To64
  | otherwise = BR.A4_65AndOver

intToCollegeGrad :: Int -> BR.CollegeGrad
intToCollegeGrad n = if n < 111 then BR.NonGrad else BR.Grad

intToInCollege :: Int -> Bool
intToInCollege n = (n == 4) 

intToSex :: Int -> BR.Sex
intToSex n = if n == 1 then BR.Male else BR.Female

intsToRace5 :: Int -> Int -> BR.Race5
intsToRace5 hN rN 
  | hN >= 100 = BR.R5_Latinx
  | rN == 100 = BR.R5_WhiteNonLatinx
  | rN == 200 = BR.R5_Black
  | rN == 651 = BR.R5_Asian
  | otherwise = BR.R5_Other

intToVoteWhyNot :: Int -> BR.VoteWhyNot
intToVoteWhyNot n
  | n == 1 = BR.VWN_PhysicallyUnable
  | n == 2 = BR.VWN_Away
  | n == 3 = BR.VWN_Forgot
  | n == 4 = BR.VWN_NotInterested
  | n == 5 = BR.VWN_Busy
  | n == 6 = BR.VWN_Transport
  | n == 7 = BR.VWN_DislikeChoices
  | n == 8 = BR.VWN_RegIssue
  | n == 9 = BR.VWN.Weather
  | n == 10 = BR.VWN_BadPollingPlace
  | otherwide = BR.VWN_Other

intToRegWhyNot :: Int -> BR.RegWhyNot
intToRegWhyNot n
  | n == 1 = BR.RWN_MissedDeadline
  | n == 2 = BR.RWN_DidntKnowHow
  | n == 3 = BR.RWN_Residency
  | n == 4 = BR.RWN_PhysicallyUnable
  | n == 5 = BR.RWN_Language
  | n == 6 = BR.RWN_NotInterested
  | n == 7 = BR.RWN_MyVoteIrrelevant
  | otherwise = BR.RWN_Other
  

intToVoteHow :: Int -> BR.VoteHow
intToVoteHow n
  | n == 1 = BR.VH_InPerson
  | n == 2 = BR.VH_ByMail
  | otherwise = BR.VH_Other

intToVoteWhen :: Int -> BR.VoteWhen
intToVoteWhen n
  | n == 1 = BR.VW_ElectionDay
  | n == 2 = BR.VW_BeforeElectionDay
  | otherwise = BR.VW_Other

intToVoted :: Int -> BR.Voted
intToVoted n
  | n == 1 = BR.V_DidNotVote
  | n == 2 = BR.V_Voted
  | otherwise = BR.V_Other

intToRegistered :: Int -> BR.Registered
intToRegistered n
  | n == 1 = BR.R_NotRegistered
  | n == 2 = BR.R_Registered
  | otherwise = BR.R_Other


transformCPSVoterPUMSRow :: F.Record CPSVoterPUMS_Raw -> F.Record CPSVoterPUMS
transformCPSVoterPUMSRow = undefined

-- to use in maybeRecsToFrame
-- if SCHG indicates not in school we map to 0 so we will interpret as "Not In College"
fixPUMSRow :: F.Rec (Maybe F.:. F.ElField) PUMS_Raw -> F.Rec (Maybe F.:. F.ElField) PUMS_Raw
fixPUMSRow r = (F.rsubset %~ missingInCollegeTo0)
               $ r where
  missingInCollegeTo0 :: F.Rec (Maybe :. F.ElField) '[BR.PUMSSCHG] -> F.Rec (Maybe :. F.ElField) '[BR.PUMSSCHG]
  missingInCollegeTo0 = FM.fromMaybeMono 0

-- fmap over Frame after load and throwing out bad rows
transformPUMSRow :: Int -> F.Record PUMS_Raw -> F.Record PUMS_Typed
transformPUMSRow y r = F.rcast @PUMS_Typed (mutate r) where
--  addState = FT.recordSingleton @BR.StateFIPS . F.rgetField @PUMSST
  addCitizen = FT.recordSingleton @Citizen . intToCitizen . F.rgetField @BR.PUMSCIT
  addWeight = FT.recordSingleton @PUMSWeight . F.rgetField @BR.PUMSPWGTP
  addAge4 = FT.recordSingleton @BR.Age4C . intToAge4 . F.rgetField @BR.PUMSAGEP
  addSex = FT.recordSingleton @BR.SexC . intToSex . F.rgetField @BR.PUMSSEX
  addEducation = FT.recordSingleton @BR.CollegeGradC . intToCollegeGrad . F.rgetField @BR.PUMSSCHL
  addInCollege = FT.recordSingleton @InCollege . intToInCollege . F.rgetField @BR.PUMSSCHG
  hN = F.rgetField @BR.PUMSHISP
  rN = F.rgetField @BR.PUMSRAC1P
  addRace r = FT.recordSingleton @BR.Race5C (intsToRace5 (hN r) (rN r))
  addYear = const $ FT.recordSingleton @BR.Year y
  mutate = FT.retypeColumn @BR.PUMSST @BR.StateFIPS
           . FT.retypeColumn @BR.PUMSPUMA @BR.PUMA
           . FT.mutate addYear
           . FT.mutate addCitizen
           . FT.mutate addWeight
           . FT.mutate addAge4
           . FT.mutate addSex
           . FT.mutate addEducation
           . FT.mutate addInCollege
           . FT.mutate addRace

{-
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
  forall cc r. (cc ⊆ (LocationCols ++ ASER ++ BR.CountCols)
               , Show (F.Record cc)
               , V.RMap cc
               , V.ReifyConstraint Show V.ElField cc
               , V.RecordToList cc
               , Ord (F.Record cc)
               , K.KnitEffects r
             )
  => K.Sem r (F.FrameRec CCES_MRP)
  -> FL.Fold (F.Record CCES_MRP) (F.FrameRec (LocationCols ++ ASER ++ BR.CountCols))  
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
                                         Just k -> fmap (const k) $ GLM.categoryNumberFromKey rc k (BR.RecordColsProxy @(LocationCols ++ cc))
              in GLM.predictFromBetaUB mm (flip M.lookup predMap) (const emptyAsNationalGKM) rc ebg bu vb
        cpreds <- M.traverseWithKey predictFrom cpms
        return $ LocationHolder n lkM cpreds
  traverse predict toPredict

-}

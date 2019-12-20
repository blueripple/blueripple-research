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
--import qualified BlueRipple.Data.PrefModel.ASETypes as BR
import qualified BlueRipple.Data.PrefModel.SimpleAgeSexEducation as BR
--import qualified BlueRipple.Data.PrefModel.SimpleAgeSexRace as BR

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

import qualified Data.IndexedSet               as IS
import qualified Numeric.GLM.ProblemTypes      as GLM
import qualified Numeric.LinearAlgebra         as LA

import           Data.Hashable                  ( Hashable )
import qualified Data.Vector                   as V
--import qualified Data.Vector.Boxed             as VB
import           GHC.Generics                   ( Generic )

import GHC.TypeLits (Symbol)
import Data.Kind (Type)

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
                     , CCESVotedPres12]
                    
type CCES_MRP = '[ Year
                 , CCESCaseId
                 , CCESWeight
                 , CCESWeightCumulative
                 , StateAbbreviation
                 , CongressionalDistrict
                 , Sex
                 , Age
                 , SimpleAge
                 , Education
                 , SimpleEducation
                 , Race
                 , SimpleRace
                 , PartisanId3
                 , PartisanId7
                 , PartisanIdLeaner
                 , Registration
                 , Turnout
                 , HouseVoteParty
                 , Pres2016VoteParty
                 , Pres2012VoteParty
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

type Sex = "Sex" F.:-> BR.Sex

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

intToSimpleEducation :: Int -> BR.SimpleEducation
intToSimpleEducation n = if n >= 4 then BR.Grad else BR.NonGrad

type Education = "Education" F.:-> EducationT
type SimpleEducation = "CollegeGrad" F.:-> BR.SimpleEducation

data RaceT = White | Black | Hispanic | Asian | NativeAmerican | Mixed | Other | MiddleEastern deriving (Show, Enum, Bounded, Eq, Ord, Generic)
type instance FI.VectorFor RaceT = V.Vector
instance S.Serialize RaceT

intToRaceT :: Int -> RaceT
intToRaceT = toEnum . minus1

type Race = "Race" F.:-> RaceT

raceToSimpleRace :: RaceT -> BR.SimpleRace
raceToSimpleRace White = BR.White
raceToSimpleRace _ = BR.NonWhite

type SimpleRace = "SimpleRace" F.:-> BR.SimpleRace


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
intToSimpleAge n = if n < 45 then BR.Young else BR.Old

type Age = "Age" F.:-> AgeT
type SimpleAge = "Under45" F.:-> BR.SimpleAge

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

data VotePartyT = VP_Democratic | VP_Republican | VP_Other deriving (Show, Enum, Bounded, Eq, Ord, Generic)
type instance FI.VectorFor VotePartyT = V.Vector
instance S.Serialize VotePartyT

parseHouseVoteParty :: T.Text -> VotePartyT
parseHouseVoteParty "Democratic" = VP_Democratic
parseHouseVoteParty "Republican" = VP_Republican
parseHouseVoteParty _ = VP_Other

type HouseVoteParty = "HouseVoteParty" F.:-> VotePartyT

parsePres2016VoteParty :: T.Text -> VotePartyT
parsePres2016VoteParty "Hilary Clinton" = VP_Democratic
parsePres2016VoteParty "Donald Trump" = VP_Republican
parsePres2016VoteParty _ = VP_Other

parsePres2012VoteParty :: T.Text -> VotePartyT
parsePres2012VoteParty "Barack Obama" = VP_Democratic
parsePres2012VoteParty "Mitt Romney" = VP_Republican
parsePres2012VoteParty _ = VP_Other

type Pres2016VoteParty = "Pres2016VoteParty" F.:-> VotePartyT
type Pres2012VoteParty = "Pres2012VoteParty" F.:-> VotePartyT 

data OfficeT = House | Senate | President deriving (Show,  Enum, Bounded, Eq, Ord, Generic)
type instance FI.VectorFor OfficeT = V.Vector
instance S.Serialize OfficeT

type Office = "Office" F.:-> OfficeT

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
  addGender = FT.recordSingleton @Sex . intToSex . F.rgetField @CCESGender
  addEducation = FT.recordSingleton @Education . intToEducationT . F.rgetField @CCESEduc
  addSimpleEducation = FT.recordSingleton @SimpleEducation . intToSimpleEducation . F.rgetField @CCESEduc
  rInt q = F.rgetField @CCESRace q
  hInt q = F.rgetField @CCESHispanic q
  race q = if (hInt q == 1) then Hispanic else intToRaceT (rInt q)
  addRace = FT.recordSingleton @Race . race 
  addSimpleRace = FT.recordSingleton @SimpleRace . raceToSimpleRace . race 
  addAge = FT.recordSingleton @Age . intToAgeT . F.rgetField @CCESAge
  addSimpleAge = FT.recordSingleton @SimpleAge . intToSimpleAge . F.rgetField @CCESAge
  addRegistration = FT.recordSingleton @Registration . parseRegistration  . F.rgetField @CCESVvRegstatus
  addTurnout = FT.recordSingleton @Turnout . parseTurnout . F.rgetField @CCESVvTurnoutGvm
  addHouseVoteParty = FT.recordSingleton @HouseVoteParty . parseHouseVoteParty . F.rgetField @CCESVotedRepParty
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
           . FT.mutate addSimpleEducation
           . FT.mutate addRace
           . FT.mutate addSimpleRace
           . FT.mutate addAge
           . FT.mutate addSimpleAge
           . FT.mutate addRegistration
           . FT.mutate addTurnout
           . FT.mutate addHouseVoteParty
           . FT.mutate addPres2012VoteParty
           . FT.mutate addPres2016VoteParty           
           . FT.mutate addPID3
           . FT.mutate addPID7
           . FT.mutate addPIDLeaner



-- map reduce folds for counting
type Count = "Count" F.:-> Int
type Successes = "Successes" F.:-> Int

-- some keys for aggregation
type ByStateSex = '[StateAbbreviation, Sex]
type ByStateSexRace = '[StateAbbreviation, Sex, SimpleRace]
type ByStateSexRaceAge = '[StateAbbreviation, Sex, SimpleRace, SimpleAge]
type ByStateSexEducationAge = '[StateAbbreviation, Sex, SimpleEducation, SimpleAge]
type ByStateSexRaceEducation = '[StateAbbreviation, Sex, SimpleRace, SimpleEducation]
type ByStateSexRaceEducationAge = '[StateAbbreviation, Sex, SimpleRace, SimpleEducation, SimpleAge]
type ByStateRaceEducation = '[StateAbbreviation, SimpleRace, SimpleEducation]

binomialFold :: (F.Record r -> Bool) -> FL.Fold (F.Record r) (F.Record '[Count, Successes])
binomialFold testRow =
  let successesF = FL.premap (\r -> if testRow r then 1 else 0) FL.sum
  in  (\n s -> n F.&: s F.&: V.RNil) <$> FL.length <*> successesF

countFold :: forall k r d.(Ord (F.Record k)
                          , FI.RecVec (k V.++ '[Count, Successes])
                          , k F.⊆ r
                          , d F.⊆ r)
          => (F.Record d -> Bool)
          -> FL.Fold (F.Record r) [F.FrameRec (k V.++ [Count,Successes])]
countFold testData = MR.mapReduceFold MR.noUnpack (MR.assignKeysAndData @k)  (MR.foldAndAddKey $ binomialFold testData)


type MeanWeight = "MeanWeight" F.:-> Double
type VarWeight = "VarWeight" F.:-> Double
type WeightedSuccesses = "WeightedSuccesses" F.:-> Double

weightedBinomialFold :: (F.Record r -> Bool) -> (F.Record r -> Double) -> FL.Fold (F.Record r) (F.Record '[Count, WeightedSuccesses, MeanWeight, VarWeight])
weightedBinomialFold testRow weightRow =
  let successesF = FL.premap (\r -> if testRow r then weightRow r else 0) FL.sum
      meanWeightF = FL.premap weightRow FL.mean
      varWeightF    = FL.premap weightRow FL.variance
  in  (\n s mw vw -> n F.&: (s/mw) F.&: mw F.&: vw F.&: V.RNil) <$> FL.length <*> successesF <*> meanWeightF <*> varWeightF

weightedCountFold :: forall k r d.(Ord (F.Record k)
                                  , FI.RecVec (k V.++ '[Count, WeightedSuccesses, MeanWeight, VarWeight])
                                  , k F.⊆ r
                                  , d F.⊆ r)
                  => (F.Record d -> Bool)
                  -> (F.Record d -> Double)
                  -> FL.Fold (F.Record r) [F.FrameRec (k V.++ [Count, WeightedSuccesses, MeanWeight, VarWeight])]
weightedCountFold testData weightData =
  MR.mapReduceFold
  MR.noUnpack
  (MR.assignKeysAndData @k)
  (MR.foldAndAddKey $ weightedBinomialFold testData weightData)

type ByCCESPredictors = '[StateAbbreviation, Sex, SimpleRace, SimpleEducation, SimpleAge]
data CCESPredictor = P_Sex | P_WWC | P_Race | P_Education | P_Age deriving (Show, Eq, Ord, Enum, Bounded)
type CCESEffect = GLM.WithIntercept CCESPredictor
ccesPredictor :: forall r. (F.ElemOf r Sex
                           , F.ElemOf r SimpleRace
                           , F.ElemOf r SimpleEducation
                           , F.ElemOf r SimpleAge) => F.Record r -> CCESPredictor -> Double
ccesPredictor r P_Sex = if (F.rgetField @Sex r == BR.Male) then 1 else 0
ccesPredictor r P_WWC    = if (F.rgetField @SimpleRace r == BR.White) && (F.rgetField @SimpleEducation r == BR.NonGrad) then 1 else 0
ccesPredictor r P_Race    = if F.rgetField @SimpleRace r == BR.NonWhite then 0 else 1 -- non-white is baseline
ccesPredictor r P_Education    = if F.rgetField @SimpleEducation r == BR.NonGrad then 0 else 1 -- non-college is baseline
ccesPredictor r P_Age    = if F.rgetField @SimpleAge r == BR.Old then 0 else 1 -- >= 45  is baseline

type instance GLM.GroupKey (Proxy k) = F.Record k
recordToGroupKey :: forall k r. (k F.⊆ r) => F.Record r -> Proxy k -> F.Record k
recordToGroupKey r _ = F.rcast r

{-
data GroupByState = GroupByState deriving (Show, Eq, Ord)
type instance GLM.GroupKey GroupByState = F.Record '[StateAbbreviation]
groupByStateKey :: forall r.  (F.ElemOf r StateAbbreviation) => F.Record r -> GroupByState -> GLM.GroupKey GroupByState
groupByStateKey r _ = F.rcast r

data GroupByStateAndGender = GroupByStateAndGender deriving (Show, Eq, Ord)
type instance GLM.GroupKey GroupByStateAndGender = (T.Text, GenderT)
groupByStateAndGenderKey :: forall r.  (F.ElemOf r StateAbbreviation
                                       , F.ElemOf r Gender
                                       )
                         => F.Record r -> GroupByStateAndGender -> GLM.GroupKey GroupByStateAndGender
groupByStateAndGenderKey r _ = (F.rgetField                          


ccesGroupLabels ::forall r.  (F.ElemOf r StateAbbreviation
                             ,F.ElemOf r Gender
                             , F.ElemOf r BR.SimpleRace
                             , F.ElemOf r BR.SimpleEducation
                             , F.ElemOf r BR.SimpleAge)
                => F.Record r -> CCESGroup -> T.Text
ccesGroupLabels r G_State = F.rgetField @StateAbbreviation r
ccesGroupLabels r G_SG = F.rgetField @StateAbbreviation r <> "-" <> (T.pack $ show $ F.rgetField @Gender r)
-}
{-ccesGroupLabels r G_District =
  let sa = F.rgetField @StateAbbreviation r
      cd = F.rgetField @CongressionalDistrict r
  in sa <> "-" <> (T.pack $ show cd)
-}
getFraction r = (realToFrac $ F.rgetField @Successes r)/(realToFrac $ F.rgetField @Count r)
getFractionWeighted r = (F.rgetField @WeightedSuccesses r)/(realToFrac $ F.rgetField @Count r)
--fixedEffects :: GLM.FixedEffects CCESPredictor
--fixedEffects = GLM.allFixedEffects True

--groups = IS.fromList [CCES_State]

-- This really really needs to be someplace central...

lmePrepFrame
  :: forall p g rs
   . (GLM.PredictorC p, GLM.GroupC g)
  => (F.Record rs -> Double) -- ^ observations
  -> GLM.FixedEffects p
  -> IS.IndexedSet g
  -> (F.Record rs -> p -> Double) -- ^ predictors
  -> (F.Record rs -> g -> GLM.GroupKey g)  -- ^ classifiers
  -> FL.Fold
       (F.Record rs)
       ( LA.Vector Double
       , LA.Matrix Double
       , Either Text (GLM.RowClassifier g)
       ) -- ^ (X,y,(row-classifier, size of class))
lmePrepFrame observationF fe groupIndices getPredictorF classifierLabelF
  = let
      makeInfoVector
        :: M.Map g (M.Map (GLM.GroupKey g) Int)
        -> M.Map g (GLM.GroupKey g)
        -> Either Text (V.Vector (GLM.ItemInfo g))
      makeInfoVector indexMaps groupKeys =
        let
          g (grp, groupKey) =
            GLM.ItemInfo
              <$> (   maybe
                      (Left $ "Failed on " <> (T.pack $ show (grp, groupKey)))
                      Right
                  $   M.lookup grp indexMaps
                  >>= M.lookup groupKey
                  )
              <*> pure groupKey
        in  fmap V.fromList $ traverse g $ M.toList groupKeys
      makeRowClassifier
        :: Traversable f
        => M.Map g (M.Map (GLM.GroupKey g) Int)
        -> f (M.Map g (GLM.GroupKey g))
        -> Either Text (GLM.RowClassifier g)
      makeRowClassifier indexMaps labels = do
        let sizes = fmap M.size indexMaps
        indexed <- traverse (makeInfoVector indexMaps) labels
        return $ GLM.RowClassifier groupIndices
                                   sizes
                                   (V.fromList $ FL.fold FL.list indexed)
                                   indexMaps
      getPredictorF' _   GLM.Intercept     = 1
      getPredictorF' row (GLM.Predictor x) = getPredictorF row x
      predictorF row = LA.fromList $ case fe of
        GLM.FixedEffects indexedFixedEffects ->
          fmap (getPredictorF' row) $ IS.members indexedFixedEffects
        GLM.InterceptOnly -> [1]
      getClassifierLabels :: F.Record rs -> M.Map g (GLM.GroupKey g)
      getClassifierLabels r =
        M.fromList $ fmap (\g -> (g, classifierLabelF r g)) $ IS.members
          groupIndices
      foldObs   = fmap LA.fromList $ FL.premap observationF FL.list
      foldPred  = fmap LA.fromRows $ FL.premap predictorF FL.list
      foldClass = FL.premap getClassifierLabels FL.list
      g (vY, mX, ls) =
        ( vY
        , mX
        , makeRowClassifier
          (snd $ ST.execState (addAll ls) (M.empty, M.empty))
          ls
        )
    in
      fmap g $ ((,,) <$> foldObs <*> foldPred <*> foldClass)



addOne
  :: GLM.GroupC g
  => (g, GLM.GroupKey g)
  -> ST.State (M.Map g Int, M.Map g (M.Map (GLM.GroupKey g) Int)) ()
addOne (grp, label) = do
  (nextIndexMap, groupIndexMaps) <- ST.get
  let groupIndexMap = fromMaybe M.empty $ M.lookup grp groupIndexMaps
  case M.lookup label groupIndexMap of
    Nothing -> do
      let index         = fromMaybe 0 $ M.lookup grp nextIndexMap
          nextIndexMap' = M.insert grp (index + 1) nextIndexMap
          groupIndexMaps' =
            M.insert grp (M.insert label index groupIndexMap) groupIndexMaps
      ST.put (nextIndexMap', groupIndexMaps')
      return ()
    _ -> return ()

addMany
  :: (GLM.GroupC g, Traversable h)
  => h (g, GLM.GroupKey g)
  -> ST.State (M.Map g Int, M.Map g (M.Map (GLM.GroupKey g) Int)) ()
addMany x = traverse addOne x >> return ()

addAll
  :: GLM.GroupC g
  => [M.Map g (GLM.GroupKey g)]
  -> ST.State (M.Map g Int, M.Map g (M.Map (GLM.GroupKey g) Int)) ()
addAll x = traverse (addMany . M.toList) x >> return ()



{-
lmePrepFrame
  :: forall p g rs
   . (Bounded p, Enum p, Ord g, Show g)
  => (F.Record rs -> Double) -- ^ observations
  -> GLM.FixedEffects p
  -> IS.IndexedSet g
  -> (F.Record rs -> p -> Double) -- ^ predictors
  -> (F.Record rs -> g -> T.Text)  -- ^ classifiers
  -> FL.Fold
       (F.Record rs)
       ( LA.Vector Double
       , LA.Matrix Double
       , Either T.Text (GLM.RowClassifier g)
       ) -- ^ (X,y,(row-classifier, size of class))
lmePrepFrame observationF fe groupIndices getPredictorF classifierLabelF
  = let
      makeInfoVector
        :: M.Map g (M.Map T.Text Int)
        -> M.Map g T.Text
        -> Either T.Text (V.Vector GLM.ItemInfo)
      makeInfoVector indexMaps labels =
        let
          g (grp, label) =
            GLM.ItemInfo
              <$> (maybe (Left $ "Failed on " <> (T.pack $ show (grp, label)))
                         Right
                  $ M.lookup grp indexMaps
                  >>= M.lookup label
                  )
              <*> pure label
        in  fmap V.fromList $ traverse g $ M.toList labels
      makeRowClassifier
        :: Traversable f
        => M.Map g (M.Map T.Text Int)
        -> f (M.Map g T.Text)
        -> Either T.Text (GLM.RowClassifier g)
      makeRowClassifier indexMaps labels = do
        let sizes = fmap M.size indexMaps
        indexed <- traverse (makeInfoVector indexMaps) labels
        return $ GLM.RowClassifier groupIndices
                                   sizes
                                   (V.fromList $ FL.fold FL.list indexed)
                                   indexMaps
      getPredictorF' _   GLM.Intercept     = 1
      getPredictorF' row (GLM.Predictor x) = getPredictorF row x
      predictorF row = LA.fromList $ case fe of
        GLM.FixedEffects indexedFixedEffects ->
          fmap (getPredictorF' row) $ IS.members indexedFixedEffects
        GLM.InterceptOnly -> [1]
      getClassifierLabels :: F.Record rs -> M.Map g T.Text
      getClassifierLabels r =
        M.fromList $ fmap (\g -> (g, classifierLabelF r g)) $ IS.members
          groupIndices
      foldObs   = fmap LA.fromList $ FL.premap observationF FL.list
      foldPred  = fmap LA.fromRows $ FL.premap predictorF FL.list
      foldClass = FL.premap getClassifierLabels FL.list
      g (vY, mX, ls) =
        ( vY
        , mX
        , makeRowClassifier
          (snd $ ST.execState (addAll ls) (M.empty, M.empty))
          ls
        )
    in
      fmap g $ ((,,) <$> foldObs <*> foldPred <*> foldClass)

addOne
  :: Ord g
  => (g, T.Text)
  -> ST.State (M.Map g Int, M.Map g (M.Map T.Text Int)) ()
addOne (grp, label) = do
  (nextIndexMap, groupIndexMaps) <- ST.get
  let groupIndexMap = fromMaybe M.empty $ M.lookup grp groupIndexMaps
  case M.lookup label groupIndexMap of
    Nothing -> do
      let index         = fromMaybe 0 $ M.lookup grp nextIndexMap
          nextIndexMap' = M.insert grp (index + 1) nextIndexMap
          groupIndexMaps' =
            M.insert grp (M.insert label index groupIndexMap) groupIndexMaps
      ST.put (nextIndexMap', groupIndexMaps')
      return ()
    _ -> return ()

addMany
  :: (Ord g, Traversable h)
  => h (g, T.Text)
  -> ST.State (M.Map g Int, M.Map g (M.Map T.Text Int)) ()
addMany x = traverse addOne x >> return ()

addAll
  :: Ord g
  => [M.Map g T.Text]
  -> ST.State (M.Map g Int, M.Map g (M.Map T.Text Int)) ()
addAll x = traverse (addMany . M.toList) x >> return ()
-}

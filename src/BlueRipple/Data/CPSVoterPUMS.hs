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
import qualified BlueRipple.Data.ElectionTypes as BR
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
cpsVoterPUMSLoader = BR.cachedFrameLoader @(F.RecordColumns BR.CPSVoterPUMS_Raw) @CPSVoterPUMS
                       (BR.LocalData $ T.pack BR.cpsVoterPUMSCSV)
                       Nothing
                       transformCPSVoterPUMSRow
                       Nothing
                       "cpsVoterPUMS.bin"

type CPSVoterPUMSWeight = "CPSVoterPUMSWeight" F.:-> Double

type CPSVoterPUMS = '[ BR.Year
                     , BR.StateFIPS
                     , BR.CountyFIPS
                     , BR.Age4C
                     , BR.SexC
                     , BR.Race5C
                     , BR.IsCitizen
                     , BR.CollegeGradC
                     , BR.InCollege
                     , BR.VoteWhyNotC
                     , BR.RegWhyNotC
                     , BR.VoteHowC
                     , BR.VoteWhenC
                     , BR.VotedYNC
                     , BR.RegisteredYNC
                     , CPSVoterPUMSWeight
                     ]


--cpsVoterPUMSElectoralWeights



cpsKeysToASER :: Bool -> F.Record '[BR.Age4C, BR.SexC, BR.CollegeGradC, BR.InCollege, BR.Race5C] -> F.Record BR.CatColsASER
cpsKeysToASER addInCollegeToGrads r =
  let cg = F.rgetField @BR.CollegeGradC r
      ic = addInCollegeToGrads && F.rgetField @BR.InCollege r
  in (BR.age4ToSimple $ F.rgetField @BR.Age4C r)
     F.&: (F.rgetField @BR.SexC r)
     F.&: (if (cg == BR.Grad || ic) then BR.Grad else BR.NonGrad)
     F.&: (BR.simpleRaceFromRace5 $ F.rgetField @BR.Race5C r)
     F.&: V.RNil

cpsKeysToASE :: Bool -> F.Record '[BR.Age4C, BR.SexC, BR.CollegeGradC, BR.InCollege, BR.Race5C] -> F.Record BR.CatColsASE
cpsKeysToASE addInCollegeToGrads r =
  let cg = F.rgetField @BR.CollegeGradC r
      ic = addInCollegeToGrads && F.rgetField @BR.InCollege r
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

intToIsCitizen :: Int -> Bool
intToIsCitizen n = (n <= 4)

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
  | n == 9 = BR.VWN_Weather
  | n == 10 = BR.VWN_BadPollingPlace
  | otherwise = BR.VWN_Other

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

intToVotedYN :: Int -> BR.VotedYN
intToVotedYN n
  | n == 1 = BR.VYN_DidNotVote
  | n == 2 = BR.VYN_Voted
  | otherwise = BR.VYN_Other

intToRegisteredYN :: Int -> BR.RegisteredYN
intToRegisteredYN n
  | n == 1 = BR.RYN_NotRegistered
  | n == 2 = BR.RYN_Registered
  | otherwise = BR.RYN_Other

transformCPSVoterPUMSRow :: BR.CPSVoterPUMS_Raw -> F.Record CPSVoterPUMS
transformCPSVoterPUMSRow r = F.rcast @CPSVoterPUMS (mutate r) where
  addAge = FT.recordSingleton @BR.Age4C . intToAge4 . F.rgetField @BR.CPSAGE
  addSex =  FT.recordSingleton @BR.SexC . intToSex . F.rgetField @BR.CPSSEX
  hN = F.rgetField @BR.CPSHISPAN
  rN = F.rgetField @BR.CPSRACE
  addRace r = FT.recordSingleton @BR.Race5C $ intsToRace5 (hN r) (rN r)
  addIsCitizen = FT.recordSingleton @BR.IsCitizen . intToIsCitizen . F.rgetField @BR.CPSCITIZEN
  addCollegeGrad = FT.recordSingleton @BR.CollegeGradC . intToCollegeGrad . F.rgetField @BR.CPSEDUC
  addInCollege = FT.recordSingleton @BR.InCollege . intToInCollege . F.rgetField @BR.CPSSCHLCOLL
  addVoteWhyNot = FT.recordSingleton @BR.VoteWhyNotC . intToVoteWhyNot . F.rgetField @BR.CPSVOWHYNOT
  addRegWhyNot = FT.recordSingleton @BR.RegWhyNotC . intToRegWhyNot . F.rgetField @BR.CPSVOYNOTREG
  addVoteHow = FT.recordSingleton @BR.VoteHowC . intToVoteHow . F.rgetField @BR.CPSVOTEHOW
  addVoteWhen = FT.recordSingleton @BR.VoteWhenC . intToVoteWhen . F.rgetField @BR.CPSVOTEWHEN
  addVotedYN = FT.recordSingleton @BR.VotedYNC . intToVotedYN . F.rgetField @BR.CPSVOTED
  addRegisteredYN = FT.recordSingleton @BR.RegisteredYNC . intToRegisteredYN . F.rgetField @BR.CPSVOREG
  mutate = FT.retypeColumn @BR.CPSYEAR @BR.Year
           . FT.retypeColumn @BR.CPSSTATEFIP @BR.StateFIPS
           . FT.retypeColumn @BR.CPSCOUNTY @BR.CountyFIPS
           . FT.mutate addAge
           . FT.mutate addSex
           . FT.mutate addRace
           . FT.mutate addIsCitizen
           . FT.mutate addCollegeGrad
           . FT.mutate addInCollege
           . FT.mutate addVoteWhyNot
           . FT.mutate addRegWhyNot
           . FT.mutate addVoteHow
           . FT.mutate addVoteWhen
           . FT.mutate addVotedYN
           . FT.mutate addRegisteredYN
           . FT.retypeColumn @BR.CPSVOSUPPWT @CPSVoterPUMSWeight

-- to use in maybeRecsToFrame
-- if SCHG indicates not in school we map to 0 so we will interpret as "Not In College"
{-
fixPUMSRow :: F.Rec (Maybe F.:. F.ElField) PUMS_Raw -> F.Rec (Maybe F.:. F.ElField) PUMS_Raw
fixPUMSRow r = (F.rsubset %~ missingInCollegeTo0)
               $ r where
  missingInCollegeTo0 :: F.Rec (Maybe :. F.ElField) '[BR.PUMSSCHG] -> F.Rec (Maybe :. F.ElField) '[BR.PUMSSCHG]
  missingInCollegeTo0 = FM.fromMaybeMono 0
-}

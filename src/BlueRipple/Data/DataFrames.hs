{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE PolyKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE TypeOperators       #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE QuasiQuotes         #-}
{-# LANGUAGE TemplateHaskell     #-}
{-# LANGUAGE TypeFamilies        #-}
{-# LANGUAGE TypeOperators       #-}
{-# LANGUAGE TypeApplications    #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE Rank2Types          #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections       #-}

module BlueRipple.Data.DataFrames
  ( module BlueRipple.Data.DataSourcePaths
  , module BlueRipple.Data.DataFrames
  )
where

import           BlueRipple.Data.DataSourcePaths

import qualified Control.Foldl                 as FL
import qualified Control.Monad.Except          as X
import qualified Data.Array                    as A
import qualified Data.List                     as L
import qualified Data.Map                      as M
import           Data.Maybe                     ( fromMaybe)
import           Data.Proxy                     ( Proxy(..) )
import qualified Data.Text                     as T
import           Data.Text                      ( Text )
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
import qualified Frames.MapReduce              as MR
import qualified Frames.Enumerations           as FE

import           Data.Hashable                  ( Hashable )
import qualified Data.Vector                   as V
import           GHC.Generics                   ( Generic )

import GHC.TypeLits (Symbol)
import Data.Kind (Type)

-- pre-declare cols with non-standard types
F.declareColumn "Date" ''FP.FrameDay

F.tableTypes "TotalSpending" totalSpendingCSV

F.tableTypes' (F.rowGen forecastAndSpendingCSV) { F.rowTypeName = "ForecastAndSpending"
                                                , F.columnUniverse = Proxy :: Proxy FP.ColumnsWithDayAndLocalTime
                                                }

F.tableTypes "ElectionResults" electionResultsCSV
F.tableTypes "AngryDems" angryDemsCSV
F.tableTypes "HouseElections" houseElectionsCSV
F.tableTypes "ContextDemographics" contextDemographicsCSV
--F.tableTypes "Turnout"          turnoutCSV
F.tableTypes "TurnoutASR"          detailedASRTurnoutCSV
F.tableTypes "TurnoutASE"          detailedASETurnoutCSV
F.tableTypes "ASRDemographics" ageSexRaceDemographicsLongCSV
F.tableTypes "ASEDemographics" ageSexEducationDemographicsLongCSV

-- This one might be different for different breakdowns
--F.tableTypes' (F.rowGen identityDemographics2016CSV) {F.rowTypeName = "AgeSexRaceByDistrict", F.tablePrefix = "Census" }


F.declareColumn "PopCount" ''Int

type DemographicCategory b = "DemographicCategory" F.:-> b  -- general holder
type LocationKey = '[StateAbbreviation, CongressionalDistrict]

type DemographicCounts b = LocationKey V.++ [DemographicCategory b, PopCount]

data DemographicStructure demographicDataRow turnoutDataRow electionDataRow demographicCategories = DemographicStructure
  {
    dsPreprocessDemographicData :: (forall m. Monad m => Int -> F.Frame demographicDataRow -> X.ExceptT Text m (F.FrameRec (DemographicCounts demographicCategories)))
  , dsPreprocessTurnoutData :: (forall m. Monad m => Int -> F.Frame turnoutDataRow -> X.ExceptT Text m (F.FrameRec '[DemographicCategory demographicCategories, Population, VotedPctOfAll]))
  , dsPreprocessElectionData :: (forall m. Monad m => Int -> F.Frame electionDataRow -> X.ExceptT Text m (F.FrameRec (LocationKey V.++ [DVotes, RVotes, Totalvotes])))
  , dsCategories :: [demographicCategories]
  }

type instance FI.VectorFor (A.Array b Int) = V.Vector
type instance FI.VectorFor (A.Array b Double) = V.Vector

data SimpleASR = OldNonWhiteFemale
               | YoungNonWhiteFemale
               | OldNonWhiteMale
               | YoungNonWhiteMale
               | OldWhiteFemale
               | YoungWhiteFemale
               | OldWhiteMale
               | YoungWhiteMale deriving (Show,Read,Enum,Bounded,Eq,Ord,Ix,Generic)

type instance FI.VectorFor SimpleASR = V.Vector
instance Hashable SimpleASR

data SimpleASE = OldFemaleNonGrad
               | YoungFemaleNonGrad
               | OldMaleNonGrad
               | YoungMaleNonGrad
               | OldFemaleCollegeGrad
               | YoungFemaleCollegeGrad
               | OldMaleCollegeGrad
               | YoungMaleCollegeGrad deriving (Show,Read,Enum,Bounded,Eq,Ord,Ix,Generic)

type instance FI.VectorFor SimpleASE = V.Vector
instance Hashable SimpleASE

type instance FI.VectorFor (A.Array b Int) = V.Vector


data ASR = WhiteFemale18To24
         | WhiteFemale25To44
         | WhiteFemale45To64
         | WhiteFemale65To74
         | WhiteFemale75AndOver
         | WhiteMale18To24
         | WhiteMale25To44
         | WhiteMale45To64
         | WhiteMale65To74
         | WhiteMale75AndOver
         | NonWhiteFemale18To24
         | NonWhiteFemale25To44
         | NonWhiteFemale45To64
         | NonWhiteFemale65To74
         | NonWhiteFemale75AndOver
         | NonWhiteMale18To24
         | NonWhiteMale25To44
         | NonWhiteMale45To64
         | NonWhiteMale65To74
         | NonWhiteMale75AndOver deriving (Show,Read,Enum,Bounded,Eq,Ord,Ix,Generic)

type instance FI.VectorFor ASR = V.Vector
instance Hashable ASR

          
processElectionData :: Monad m => Int -> F.Frame HouseElections
                -> X.ExceptT Text m (F.FrameRec (LocationKey V.++ [DVotes, RVotes, Totalvotes]))
processElectionData year eData = do
  let xDat = fmap (F.rcast @('[Year] V.++ LocationKey V.++ [Candidate, Party, Candidatevotes, Totalvotes])
                   . FT.retypeColumn @District @CongressionalDistrict
                   . FT.retypeColumn @StatePo @StateAbbreviation
                  ) eData
      unpack = MR.generalizeUnpack $ MR.unpackFilterOnField @Year (== year)
      assign = MR.generalizeAssign $ MR.assignKeysAndData @LocationKey @'[Candidate, Party, Candidatevotes, Totalvotes]
      reduce = MR.makeRecsWithKeyM id $ MR.ReduceFoldM (const $ fmap (pure @[]) flattenVotes')
  either X.throwError return $ FL.foldM (MR.concatFoldM $ MR.mapReduceFoldM unpack assign reduce) xDat

data Cands = NoCands | OneCand Int | Multi Int
candsToVotes NoCands = 0
candsToVotes (OneCand x) = x
candsToVotes (Multi x) = x

type DVotes = "DVotes" F.:-> Int
type RVotes = "RVotes" F.:-> Int
flattenVotes'
  :: FL.FoldM (Either Text)
       (F.Record '[Candidate, Party, Candidatevotes, Totalvotes])
       (F.Record '[DVotes, RVotes, Totalvotes]) -- Map Name (Map Party Votes)
flattenVotes' =
  MR.postMapM mapToRec
  $ FL.generalize
  $ FL.Fold step (M.empty,0) id where
  step (m,_) r =
    let cand = F.rgetField @Candidate r
        party = F.rgetField @Party r
        cVotes = F.rgetField @Candidatevotes r
        tVotes = F.rgetField @Totalvotes r
        updateInner :: Maybe (M.Map Text Int) -> Maybe (M.Map Text Int)
        updateInner pmM = Just $ M.insert party cVotes $ fromMaybe M.empty pmM -- if same candidate appears with same party, this replaces
    in (M.alter updateInner cand m, tVotes)
  mapToRec :: (M.Map Text (M.Map Text Int),Int) -> Either Text (F.Record [DVotes, RVotes, Totalvotes])
  mapToRec (m, tVotes) = do
    let findByParty p =
          let cs = L.filter (L.elem p . M.keys) $ fmap snd $ M.toList m  
          in case cs of
            [] -> NoCands --Right 0 -- Left  $  "No cands when finding " <> p <> " in " <> (T.pack $ show m)
            [cm] -> OneCand $ FL.fold FL.sum cm 
            cms -> Multi $ FL.fold FL.sum $ fmap (FL.fold FL.sum) cms --Left $ "More than one candidate with party=" <> p <> ": " <> (T.pack $ show m)
    let dCand = findByParty "democrat"
        rCand = findByParty "republican"
    return $ candsToVotes dCand F.&: candsToVotes rCand  F.&: tVotes F.&: V.RNil   

flattenVotes
  :: FL.Fold
       (F.Record '[Party, Candidatevotes, Totalvotes])
       (F.Record '[DVotes, RVotes, Totalvotes])
flattenVotes =
  FF.sequenceRecFold
    $    FF.recFieldF
           FL.sum
           (\r -> if F.rgetField @Party r == "democrat"
             then F.rgetField @Candidatevotes r
             else 0
           )
    V.:& FF.recFieldF
           FL.sum
           (\r -> if F.rgetField @Party r == "republican"
             then F.rgetField @Candidatevotes r
             else 0
           )
    V.:& FF.recFieldF (fmap (fromMaybe 0) $ FL.last) (F.rgetField @Totalvotes)
    V.:& V.RNil

---
simpleAgeSexEducation :: DemographicStructure ASEDemographics TurnoutASE HouseElections SimpleASE
simpleAgeSexEducation = DemographicStructure processDemographicData processTurnoutData processElectionData [minBound ..]
 where
   mergeACSCounts :: Monad m => M.Map T.Text Int -> X.ExceptT Text m [(SimpleASE, Int)]
   mergeACSCounts m = do
     let lookupX k = maybe (X.throwError $ "(mergeACSCounts) lookup failed for key=\"" <> k <> "\"") return . M.lookup k
     f18To24L9 <- lookupX "Female18To24LessThan9th" m
     f25To44L9 <- lookupX "Female25To44LessThan9th" m
     f18To24LHS <- lookupX "Female18To24LessThan12th" m
     f25To44LHS <- lookupX "Female25To44LessThan12th" m
     f18To24HS <- lookupX "Female18To24HighSchool" m
     f25To44HS <- lookupX "Female25To44HighSchool" m  
     f18To24SC <- lookupX "Female18To24SomeCollege" m
     f25To44SC <- lookupX "Female25To44SomeCollege" m
     f18To24AS <- lookupX "Female18To24Associates" m
     f25To44AS <- lookupX "Female25To44Associates" m
     let youngFemaleNonGrad = f18To24L9 + f25To44L9 + f18To24LHS + f25To44LHS + f18To24HS + f25To44HS + f18To24SC + f25To44SC + f18To24AS + f25To44AS
     f18To24BA <- lookupX "Female18To24Bachelors" m
     f25To44BA <- lookupX "Female25To44Bachelors" m
     f18To24AD <- lookupX "Female18To24AdvancedDegree" m
     f25To44AD <- lookupX "Female25To44AdvancedDegree" m
     let youngFemaleGrad = f18To24BA + f25To44BA + f18To24AD + f25To44AD
     f45To64L9 <- lookupX "Female45To64LessThan9th" m
     f65AndOverL9 <- lookupX "Female65AndOverLessThan9th" m
     f45To64LHS <- lookupX "Female45To64LessThan12th" m
     f65AndOverLHS <- lookupX "Female65AndOverLessThan12th" m
     f45To64HS <- lookupX "Female45To64HighSchool" m
     f65AndOverHS <- lookupX "Female65AndOverHighSchool" m  
     f45To64SC <- lookupX "Female45To64SomeCollege" m
     f65AndOverSC <- lookupX "Female65AndOverSomeCollege" m
     f45To64AS <- lookupX "Female45To64Associates" m
     f65AndOverAS <- lookupX "Female65AndOverAssociates" m
     let oldFemaleNonGrad = f45To64L9 + f65AndOverL9 + f45To64LHS + f65AndOverLHS + f45To64HS + f65AndOverHS + f45To64SC + f65AndOverSC + f45To64AS + f65AndOverAS
     f45To64BA <- lookupX "Female45To64Bachelors" m
     f65AndOverBA <- lookupX "Female65AndOverBachelors" m
     f45To64AD <- lookupX "Female45To64AdvancedDegree" m
     f65AndOverAD <- lookupX "Female65AndOverAdvancedDegree" m
     let oldFemaleGrad = f45To64BA + f65AndOverBA + f45To64AD + f65AndOverAD
     m18To24L9 <- lookupX "Male18To24LessThan9th" m
     m25To44L9 <- lookupX "Male25To44LessThan9th" m
     m18To24LHS <- lookupX "Male18To24LessThan12th" m
     m25To44LHS <- lookupX "Male25To44LessThan12th" m
     m18To24HS <- lookupX "Male18To24HighSchool" m
     m25To44HS <- lookupX "Male25To44HighSchool" m  
     m18To24SC <- lookupX "Male18To24SomeCollege" m
     m25To44SC <- lookupX "Male25To44SomeCollege" m
     m18To24AS <- lookupX "Male18To24Associates" m
     m25To44AS <- lookupX "Male25To44Associates" m
     let youngMaleNonGrad = m18To24L9 + m25To44L9 + m18To24LHS + m25To44LHS + m18To24HS + m25To44HS + m18To24SC + m25To44SC + m18To24AS + m25To44AS
     m18To24BA <- lookupX "Male18To24Bachelors" m
     m25To44BA <- lookupX "Male25To44Bachelors" m
     m18To24AD <- lookupX "Male18To24AdvancedDegree" m
     m25To44AD <- lookupX "Male25To44AdvancedDegree" m
     let youngMaleGrad = m18To24BA + m25To44BA + m18To24AD + m25To44AD
     m45To64L9 <- lookupX "Male45To64LessThan9th" m
     m65AndOverL9 <- lookupX "Male65AndOverLessThan9th" m
     m45To64LHS <- lookupX "Male45To64LessThan12th" m
     m65AndOverLHS <- lookupX "Male65AndOverLessThan12th" m
     m45To64HS <- lookupX "Male45To64HighSchool" m
     m65AndOverHS <- lookupX "Male65AndOverHighSchool" m  
     m45To64SC <- lookupX "Male45To64SomeCollege" m
     m65AndOverSC <- lookupX "Male65AndOverSomeCollege" m
     m45To64AS <- lookupX "Male45To64Associates" m
     m65AndOverAS <- lookupX "Male65AndOverAssociates" m
     let oldMaleNonGrad = m45To64L9 + m65AndOverL9 + m45To64LHS + m65AndOverLHS + m45To64HS + m65AndOverHS + m45To64SC + m65AndOverSC + m45To64AS + m65AndOverAS 
     m45To64BA <- lookupX "Male45To64Bachelors" m
     m65AndOverBA <- lookupX "Male65AndOverBachelors" m
     m45To64AD <- lookupX "Male45To64AdvancedDegree" m
     m65AndOverAD <- lookupX "Male65AndOverAdvancedDegree" m
     let oldMaleGrad = m45To64BA + m65AndOverBA + m45To64AD + m65AndOverAD     
         result = 
           [ (OldFemaleNonGrad, oldFemaleNonGrad)
           , (YoungFemaleNonGrad, youngFemaleNonGrad)
           , (OldMaleNonGrad, oldMaleNonGrad)
           , (YoungMaleNonGrad, youngMaleNonGrad)
           , (OldFemaleCollegeGrad, oldFemaleGrad)
           , (YoungFemaleCollegeGrad, youngFemaleGrad)
           , (OldMaleCollegeGrad, oldMaleGrad)
           , (YoungMaleCollegeGrad, youngMaleGrad)
           ]
         totalInput = FL.fold FL.sum $ fmap snd $ M.toList m
         totalResult = FL.fold FL.sum $ fmap snd $ result
     X.when (totalInput /= totalResult) $ X.throwError ("Totals don't match in mergeACSCounts (SimpleASE)")
     return result
     
   processDemographicData :: Monad m => Int -> F.Frame ASEDemographics -> X.ExceptT Text m (F.FrameRec (DemographicCounts SimpleASE))
   processDemographicData year dd = 
     let makeRec :: (SimpleASE , Int) ->  F.Record [DemographicCategory SimpleASE, PopCount]
         makeRec (b, n) = b F.&: n F.&: V.RNil
         fromRec r = (F.rgetField @ACSKey r, F.rgetField @ACSCount r)
         unpack = MR.generalizeUnpack $ MR.unpackFilterOnField @Year (==year)
         assign = MR.generalizeAssign $ MR.assignKeysAndData @[StateAbbreviation,CongressionalDistrict] @[ACSKey,ACSCount]
         reduce = MR.makeRecsWithKeyM makeRec $ MR.ReduceFoldM (const $ MR.postMapM mergeACSCounts $ FL.generalize $ FL.premap fromRec FL.map) 
     in FL.foldM (MR.concatFoldM $ MR.mapReduceFoldM unpack assign reduce) dd
   
   mergeTurnoutRows :: Monad m => M.Map T.Text (Int, Int) -> X.ExceptT Text m [(SimpleASE, Int, Int)]
   mergeTurnoutRows m = do
     let lookupX k = maybe (X.throwError $ "(mergeTurnoutRows) lookup failed for key=\"" <> k <> "\"") return . M.lookup k
     (f18To24L9P, f18To24L9V) <- lookupX "F18To24L9" m
     (f25To44L9P, f25To44L9V) <- lookupX "F25To44L9" m
     (f45To64L9P, f45To64L9V) <- lookupX "F45To64L9" m
     (f65To74L9P, f65To74L9V) <- lookupX "F65To74L9" m
     (f75AndOverL9P, f75AndOverL9V) <- lookupX "F75AndOverL9" m
     (f18To24L12P, f18To24L12V) <- lookupX "F18To24L12" m
     (f25To44L12P, f25To44L12V) <- lookupX "F25To44L12" m
     (f45To64L12P, f45To64L12V) <- lookupX "F45To64L12" m
     (f65To74L12P, f65To74L12V) <- lookupX "F65To74L12" m
     (f75AndOverL12P, f75AndOverL12V) <- lookupX "F75AndOverL12" m
     (f18To24HSP, f18To24HSV) <- lookupX "F18To24HS" m
     (f25To44HSP, f25To44HSV) <- lookupX "F25To44HS" m
     (f45To64HSP, f45To64HSV) <- lookupX "F45To64HS" m
     (f65To74HSP, f65To74HSV) <- lookupX "F65To74HS" m
     (f75AndOverHSP, f75AndOverHSV) <- lookupX "F75AndOverHS" m
     (f18To24SCP, f18To24SCV) <- lookupX "F18To24SC" m
     (f25To44SCP, f25To44SCV) <- lookupX "F25To44SC" m
     (f45To64SCP, f45To64SCV) <- lookupX "F45To64SC" m
     (f65To74SCP, f65To74SCV) <- lookupX "F65To74SC" m
     (f75AndOverSCP, f75AndOverSCV) <- lookupX "F75AndOverSC" m
     (f18To24BAP, f18To24BAV) <- lookupX "F18To24BA" m
     (f25To44BAP, f25To44BAV) <- lookupX "F25To44BA" m
     (f45To64BAP, f45To64BAV) <- lookupX "F45To64BA" m
     (f65To74BAP, f65To74BAV) <- lookupX "F65To74BA" m
     (f75AndOverBAP, f75AndOverBAV) <- lookupX "F75AndOverBA" m
     (f18To24ADP, f18To24ADV) <- lookupX "F18To24AD" m
     (f25To44ADP, f25To44ADV) <- lookupX "F25To44AD" m
     (f45To64ADP, f45To64ADV) <- lookupX "F45To64AD" m
     (f65To74ADP, f65To74ADV) <- lookupX "F65To74AD" m
     (f75AndOverADP, f75AndOverADV) <- lookupX "F75AndOverAD" m
     (m18To24L9P, m18To24L9V) <- lookupX "M18To24L9" m
     (m25To44L9P, m25To44L9V) <- lookupX "M25To44L9" m
     (m45To64L9P, m45To64L9V) <- lookupX "M45To64L9" m
     (m65To74L9P, m65To74L9V) <- lookupX "M65To74L9" m
     (m75AndOverL9P, m75AndOverL9V) <- lookupX "M75AndOverL9" m
     (m18To24L12P, m18To24L12V) <- lookupX "M18To24L12" m
     (m25To44L12P, m25To44L12V) <- lookupX "M25To44L12" m
     (m45To64L12P, m45To64L12V) <- lookupX "M45To64L12" m
     (m65To74L12P, m65To74L12V) <- lookupX "M65To74L12" m
     (m75AndOverL12P, m75AndOverL12V) <- lookupX "M75AndOverL12" m
     (m18To24HSP, m18To24HSV) <- lookupX "M18To24HS" m
     (m25To44HSP, m25To44HSV) <- lookupX "M25To44HS" m
     (m45To64HSP, m45To64HSV) <- lookupX "M45To64HS" m
     (m65To74HSP, m65To74HSV) <- lookupX "M65To74HS" m
     (m75AndOverHSP, m75AndOverHSV) <- lookupX "M75AndOverHS" m
     (m18To24SCP, m18To24SCV) <- lookupX "M18To24SC" m
     (m25To44SCP, m25To44SCV) <- lookupX "M25To44SC" m
     (m45To64SCP, m45To64SCV) <- lookupX "M45To64SC" m
     (m65To74SCP, m65To74SCV) <- lookupX "M65To74SC" m
     (m75AndOverSCP, m75AndOverSCV) <- lookupX "M75AndOverSC" m
     (m18To24BAP, m18To24BAV) <- lookupX "M18To24BA" m
     (m25To44BAP, m25To44BAV) <- lookupX "M25To44BA" m
     (m45To64BAP, m45To64BAV) <- lookupX "M45To64BA" m
     (m65To74BAP, m65To74BAV) <- lookupX "M65To74BA" m
     (m75AndOverBAP, m75AndOverBAV) <- lookupX "M75AndOverBA" m
     (m18To24ADP, m18To24ADV) <- lookupX "M18To24AD" m
     (m25To44ADP, m25To44ADV) <- lookupX "M25To44AD" m
     (m45To64ADP, m45To64ADV) <- lookupX "M45To64AD" m
     (m65To74ADP, m65To74ADV) <- lookupX "M65To74AD" m
     (m75AndOverADP, m75AndOverADV) <- lookupX "M75AndOverAD" m
     let (ofngP, ofngV) = (f45To64L9P + f45To64L12P + f45To64HSP + f45To64SCP +
                           f65To74L9P + f65To74L12P + f65To74HSP + f65To74SCP +
                           f75AndOverL9P + f75AndOverL12P + f75AndOverHSP + f75AndOverSCP,
                           f45To64L9V + f45To64L12V + f45To64HSV + f45To64SCV +
                           f65To74L9V + f65To74L12V + f65To74HSV + f65To74SCV +
                           f75AndOverL9V + f75AndOverL12V + f75AndOverHSV + f75AndOverSCV)
         (yfngP, yfngV) = (f18To24L9P + f18To24L12P + f18To24HSP + f18To24SCP +
                           f25To44L9P + f25To44L12P + f25To44HSP + f25To44SCP,
                           f18To24L9V + f18To24L12V + f18To24HSV + f18To24SCV +
                           f25To44L9V + f25To44L12V + f25To44HSV + f25To44SCV)
         (omngP, omngV) = (m45To64L9P + m45To64L12P + m45To64HSP + m45To64SCP +
                           m65To74L9P + m65To74L12P + m65To74HSP + m65To74SCP +
                           m75AndOverL9P + m75AndOverL12P + m75AndOverHSP + m75AndOverSCP,
                           m45To64L9V + m45To64L12V + m45To64HSV + m45To64SCV +
                           m65To74L9V + m65To74L12V + m65To74HSV + m65To74SCV +
                           m75AndOverL9V + m75AndOverL12V + m75AndOverHSV + m75AndOverSCV)
         (ymngP, ymngV) = (m18To24L9P + m18To24L12P + m18To24HSP + m18To24SCP +
                           m25To44L9P + m25To44L12P + m25To44HSP + m25To44SCP,
                           m18To24L9V + m18To24L12V + m18To24HSV + m18To24SCV +
                           m25To44L9V + m25To44L12V + m25To44HSV + m25To44SCV)
         (ofcgP, ofcgV) = (f45To64BAP + f45To64ADP + f65To74BAP + f65To74ADP + f75AndOverBAP + f75AndOverADP,
                           f45To64BAV + f45To64ADV + f65To74BAV + f65To74ADV + f75AndOverBAV + f75AndOverADV)
         (yfcgP, yfcgV) = (f18To24BAP + f18To24ADP + f25To44BAP + f25To44ADP,
                           f18To24BAV + f18To24ADV + f25To44BAV + f25To44ADV)
         (omcgP, omcgV) = (m45To64BAP + m45To64ADP + m65To74BAP + m65To74ADP + m75AndOverBAP + m75AndOverADP,
                           m45To64BAV + m45To64ADV + m65To74BAV + m65To74ADV + m75AndOverBAV + m75AndOverADV)
         (ymcgP, ymcgV) = (m18To24BAP + m18To24ADP + m25To44BAP + m25To44ADP,
                           m18To24BAV + m18To24ADV + m25To44BAV + m25To44ADV)                                    
         result =
           [ (OldFemaleNonGrad, ofngP, ofngV)
           , (YoungFemaleNonGrad, yfngP, yfngV)
           , (OldMaleNonGrad, omngP, omngV)
           , (YoungMaleNonGrad, ymngP, ymngV)
           , (OldFemaleCollegeGrad, ofcgP, ofcgV)
           , (YoungFemaleCollegeGrad, yfcgP, yfcgV)
           , (OldMaleCollegeGrad, omcgP, omcgV)
           , (YoungMaleCollegeGrad, ymcgP, ymcgV)
           ]
         (inputP, inputV) = FL.fold ((,) <$> FL.premap fst FL.sum <*> FL.premap snd FL.sum) m
         (resultP, resultV) = FL.fold ((,) <$> FL.premap (\(_,x,_) -> x) FL.sum <*> FL.premap (\(_,_,x) -> x) FL.sum) result
     X.when (inputP /= resultP) $ X.throwError "Population totals don't agree in mergeTurnoutRows (SimpleASE)"
     X.when (inputV /= resultV) $ X.throwError "Voted totals don't agree in mergeTurnoutRows (SimpleASE)"
     return result
         
   processTurnoutData :: Monad m
     => Int
     -> F.Frame TurnoutASE
     -> X.ExceptT Text m (F.FrameRec '[DemographicCategory SimpleASE, Population, VotedPctOfAll])
   processTurnoutData year td = 
    let makeRec :: (SimpleASE,Int,Int) -> F.Record [DemographicCategory SimpleASE, Population, VotedPctOfAll]
        makeRec (b,p,v) = b F.&: p F.&: (realToFrac v/realToFrac p) F.&: V.RNil
        fromRec r = (F.rgetField @Group r, (F.rgetField @Population r, F.rgetField @Voted r))
        unpack = MR.generalizeUnpack $ MR.unpackFilterOnField @Year (==year)
        assign = MR.generalizeAssign $ MR.assignKeysAndData @'[] @[Group,Population,Voted]
        reduce = MR.makeRecsWithKeyM makeRec $ MR.ReduceFoldM (const $ MR.postMapM mergeTurnoutRows $ FL.generalize $ FL.premap fromRec FL.map) 
    in FL.foldM (MR.concatFoldM $ MR.mapReduceFoldM unpack assign reduce) td

---

simpleAgeSexRace :: DemographicStructure ASRDemographics TurnoutASR HouseElections SimpleASR
simpleAgeSexRace = DemographicStructure processDemographicData processTurnoutData processElectionData [minBound ..]
 where
   mergeACSCounts :: Monad m => M.Map T.Text Int -> X.ExceptT Text m [(SimpleASR, Int)]
   mergeACSCounts m = do
     let lookupX k = maybe (X.throwError $ "(mergeACSCounts) lookup failed for key=\"" <> k <> "\"") return . M.lookup k
     f18To24 <- lookupX "Female18To24" m
     f25To44 <- lookupX "Female25To44" m
     f45To64 <- lookupX "Female45To64" m
     f65To74 <- lookupX "Female65To74" m
     f75AndOver <- lookupX "Female75AndOver" m
     m18To24 <- lookupX "Male18To24" m
     m25To44 <- lookupX "Male25To44" m
     m45To64 <- lookupX "Male45To64" m
     m65To74 <- lookupX "Male65To74" m
     m75AndOver <- lookupX "Male75AndOver" m
     fWNH18To24 <- lookupX "FemaleWhiteNonHispanic18To24" m
     fWNH25To44 <- lookupX "FemaleWhiteNonHispanic25To44" m
     fWNH45To64 <- lookupX "FemaleWhiteNonHispanic45To64" m
     fWNH65To74 <- lookupX "FemaleWhiteNonHispanic65To74" m
     fWNH75AndOver <- lookupX "FemaleWhiteNonHispanic75AndOver" m
     mWNH18To24 <- lookupX "MaleWhiteNonHispanic18To24" m
     mWNH25To44 <- lookupX "MaleWhiteNonHispanic25To44" m
     mWNH45To64 <- lookupX "MaleWhiteNonHispanic45To64" m
     mWNH65To74 <- lookupX "MaleWhiteNonHispanic65To74" m
     mWNH75AndOver <- lookupX "MaleWhiteNonHispanic75AndOver" m
     let owf = fWNH45To64 + fWNH65To74 + fWNH75AndOver
         ywf = fWNH18To24 + fWNH25To44
         owm = mWNH45To64 + mWNH65To74 + mWNH75AndOver
         ywm = mWNH18To24 + mWNH25To44
         result = 
           [ (OldNonWhiteFemale, (f45To64 + f65To74 + f75AndOver - owf))
           , (YoungNonWhiteFemale, (f18To24 + f25To44 - ywf))
           , (OldNonWhiteMale, (m45To64 + m65To74 + m75AndOver - owm))
           , (YoungNonWhiteMale, (m18To24 + m25To44 - ywm))
           , (OldWhiteFemale, owf)
           , (YoungWhiteFemale, ywf)
           , (OldWhiteMale, owm)
           , (YoungWhiteMale, ywm)
           ]
         totalInput = FL.fold FL.sum $ fmap snd $ L.filter (not . L.isSubsequenceOf "Hispanic" . T.unpack . fst) $ M.toList m
         totalResult = FL.fold FL.sum $ fmap snd $ result
     X.when (totalInput /= totalResult) $ X.throwError ("Totals don't match in mergeACSCounts")
     return result
     
   processDemographicData :: Monad m => Int -> F.Frame ASRDemographics -> X.ExceptT Text m (F.FrameRec (DemographicCounts SimpleASR))
   processDemographicData year dd = 
     let makeRec :: (SimpleASR , Int) ->  F.Record [DemographicCategory SimpleASR, PopCount]
         makeRec (b, n) = b F.&: n F.&: V.RNil
         fromRec r = (F.rgetField @ACSKey r, F.rgetField @ACSCount r)
         unpack = MR.generalizeUnpack $ MR.unpackFilterOnField @Year (==year)
         assign = MR.generalizeAssign $ MR.assignKeysAndData @[StateAbbreviation,CongressionalDistrict] @[ACSKey,ACSCount]
         reduce = MR.makeRecsWithKeyM makeRec $ MR.ReduceFoldM (const $ MR.postMapM mergeACSCounts $ FL.generalize $ FL.premap fromRec FL.map) 
     in FL.foldM (MR.concatFoldM $ MR.mapReduceFoldM unpack assign reduce) dd
   
   mergeTurnoutRows :: Monad m => M.Map T.Text (Int, Int) -> X.ExceptT Text m [(SimpleASR, Int, Int)]
   mergeTurnoutRows m = do
     let lookupX k = maybe (X.throwError $ "(mergeTurnoutRows) lookup failed for key=\"" <> k <> "\"") return . M.lookup k
     (wm18To24P, wm18To24V) <- lookupX "WhiteMale18To24" m
     (wm25To44P, wm25To44V) <- lookupX "WhiteMale25To44" m
     (wm45To64P, wm45To64V) <- lookupX "WhiteMale45To64" m
     (wm65To74P, wm65To74V) <- lookupX "WhiteMale65To74" m
     (wm75AndOverP, wm75AndOverV) <- lookupX "WhiteMale75AndOver" m
     (wf18To24P, wf18To24V) <- lookupX "WhiteFemale18To24" m
     (wf25To44P, wf25To44V) <- lookupX "WhiteFemale25To44" m
     (wf45To64P, wf45To64V) <- lookupX "WhiteFemale45To64" m
     (wf65To74P, wf65To74V) <- lookupX "WhiteFemale65To74" m
     (wf75AndOverP, wf75AndOverV) <- lookupX "WhiteFemale75AndOver" m
     (bm18To24P, bm18To24V) <- lookupX "BlackMale18To24" m
     (bm25To44P, bm25To44V) <- lookupX "BlackMale25To44" m
     (bm45To64P, bm45To64V) <- lookupX "BlackMale45To64" m
     (bm65To74P, bm65To74V) <- lookupX "BlackMale65To74" m
     (bm75AndOverP, bm75AndOverV) <- lookupX "BlackMale75AndOver"  m
     (bf18To24P, bf18To24V) <- lookupX "BlackFemale18To24"  m
     (bf25To44P, bf25To44V) <- lookupX "BlackFemale25To44" m
     (bf45To64P, bf45To64V) <- lookupX "BlackFemale45To64" m
     (bf65To74P, bf65To74V) <- lookupX "BlackFemale65To74" m
     (bf75AndOverP, bf75AndOverV) <- lookupX "BlackFemale75AndOver" m
     (am18To24P, am18To24V) <- lookupX "AsianMale18To24" m
     (am25To44P, am25To44V) <- lookupX "AsianMale25To44" m
     (am45To64P, am45To64V) <- lookupX "AsianMale45To64" m
     (am65To74P, am65To74V) <- lookupX "AsianMale65To74" m
     (am75AndOverP, am75AndOverV) <- lookupX "AsianMale75AndOver" m
     (af18To24P, af18To24V) <- lookupX "AsianFemale18To24" m
     (af25To44P, af25To44V) <- lookupX "AsianFemale25To44" m
     (af45To64P, af45To64V) <- lookupX "AsianFemale45To64" m
     (af65To74P, af65To74V) <- lookupX "AsianFemale65To74" m
     (af75AndOverP, af75AndOverV) <- lookupX "AsianFemale75AndOver" m
     (hm18To24P, hm18To24V) <- lookupX "HispanicMale18To24" m
     (hm25To44P, hm25To44V) <- lookupX "HispanicMale25To44" m
     (hm45To64P, hm45To64V) <- lookupX "HispanicMale45To64" m
     (hm65To74P, hm65To74V) <- lookupX "HispanicMale65To74" m
     (hm75AndOverP, hm75AndOverV) <- lookupX "HispanicMale75AndOver" m
     (hf18To24P, hf18To24V) <- lookupX "HispanicFemale18To24" m
     (hf25To44P, hf25To44V) <- lookupX "HispanicFemale25To44" m
     (hf45To64P, hf45To64V) <- lookupX "HispanicFemale45To64" m
     (hf65To74P, hf65To74V) <- lookupX "HispanicFemale65To74" m
     (hf75AndOverP, hf75AndOverV) <- lookupX "HispanicFemale75AndOver" m
     let (ywmP, ywmV) = (wm18To24P + wm25To44P, wm18To24V + wm25To44V)
         (ywfP, ywfV) = (wf18To24P + wf25To44P, wf18To24V + wf25To44V)
         (owmP, owmV) = (wm45To64P + wm65To74P + wm75AndOverP, wm45To64V + wm65To74V + wm75AndOverV)
         (owfP, owfV) = (wf45To64P + wf65To74P + wf75AndOverP, wf45To64V + wf65To74V + wf75AndOverV)
         (ybmP, ybmV) = (bm18To24P + bm25To44P, bm18To24V + bm25To44V)
         (ybfP, ybfV) = (bf18To24P + bf25To44P, bf18To24V + bf25To44V)
         (obmP, obmV) = (bm45To64P + bm65To74P + bm75AndOverP, bm45To64V + bm65To74V + bm75AndOverV)
         (obfP, obfV) = (bf45To64P + bf65To74P + bf75AndOverP, bf45To64V + bf65To74V + bf75AndOverV)
         (yamP, yamV) = (am18To24P + am25To44P, am18To24V + am25To44V)
         (yafP, yafV) = (af18To24P + af25To44P, af18To24V + af25To44V)
         (oamP, oamV) = (am45To64P + am65To74P + am75AndOverP, am45To64V + am65To74V + am75AndOverV)
         (oafP, oafV) = (af45To64P + af65To74P + af75AndOverP, af45To64V + af65To74V + af75AndOverV)
         (yhmP, yhmV) = (hm18To24P + hm25To44P, hm18To24V + hm25To44V)
         (yhfP, yhfV) = (hf18To24P + hf25To44P, hf18To24V + hf25To44V)
         (ohmP, ohmV) = (hm45To64P + hm65To74P + hm75AndOverP, hm45To64V + hm65To74V + hm75AndOverV)
         (ohfP, ohfV) = (hf45To64P + hf65To74P + hf75AndOverP, hf45To64V + hf65To74V + hf75AndOverV)
         (ynwmP, ynwmV) = (ybmP + yamP + yhmP, ybmV + yamV + yhmV)
         (onwmP, onwmV) = (obmP + oamP + ohmP, obmV + oamV + ohmV)
         (ynwfP, ynwfV) = (ybfP + yafP + yhfP, ybfV + yafV + yhfV)
         (onwfP, onwfV) = (obfP + oafP + ohfP, obfV + oafV + ohfV)
         result =
           [ (OldNonWhiteFemale, onwfP, onwfV)
           , (YoungNonWhiteFemale, ynwfP, ynwfV)
           , (OldNonWhiteMale, onwmP, onwmV)
           , (YoungNonWhiteMale, ynwmP, ynwmV)
           , (OldWhiteFemale, owfP, owfV)
           , (YoungWhiteFemale, ywfP, ywfV)
           , (OldWhiteMale, owmP, owmV)
           , (YoungWhiteMale, ywmP, ywmV)
           ]
         (inputP, inputV) = FL.fold ((,) <$> FL.premap fst FL.sum <*> FL.premap snd FL.sum) m
         (resultP, resultV) = FL.fold ((,) <$> FL.premap (\(_,x,_) -> x) FL.sum <*> FL.premap (\(_,_,x) -> x) FL.sum) result
     X.when (inputP /= resultP) $ X.throwError "Population totals don't agree in mergeTurnoutRows"
     X.when (inputV /= resultV) $ X.throwError "Voted totals don't agree in mergeTurnoutRows"
     return result
         
   processTurnoutData :: Monad m
     => Int
     -> F.Frame TurnoutASR
     -> X.ExceptT Text m (F.FrameRec '[DemographicCategory SimpleASR, Population, VotedPctOfAll])
   processTurnoutData year td = 
    let makeRec :: (SimpleASR,Int,Int) -> F.Record [DemographicCategory SimpleASR, Population, VotedPctOfAll]
        makeRec (b,p,v) = b F.&: p F.&: (realToFrac v/realToFrac p) F.&: V.RNil
        fromRec r = (F.rgetField @Group r, (F.rgetField @Population r, F.rgetField @Voted r))
        unpack = MR.generalizeUnpack $ MR.unpackFilterOnField @Year (==year)
        assign = MR.generalizeAssign $ MR.assignKeysAndData @'[] @[Group,Population,Voted]
        reduce = MR.makeRecsWithKeyM makeRec $ MR.ReduceFoldM (const $ MR.postMapM mergeTurnoutRows $ FL.generalize $ FL.premap fromRec FL.map) 
    in FL.foldM (MR.concatFoldM $ MR.mapReduceFoldM unpack assign reduce) td


ageSexRace :: DemographicStructure ASRDemographics TurnoutASR HouseElections ASR
ageSexRace = DemographicStructure processDemographicData processTurnoutData processElectionData [minBound ..]
 where
   mergeACSCounts :: Monad m => M.Map T.Text Int -> X.ExceptT Text m [(ASR, Int)]
   mergeACSCounts m = do
     let lookupX k = maybe (X.throwError $ "(mergeACSCounts) lookup failed for key=\"" <> k <> "\"") return . M.lookup k
     f18To24 <- lookupX "Female18To24" m
     f25To44 <- lookupX "Female25To44" m
     f45To64 <- lookupX "Female45To64" m
     f65To74 <- lookupX "Female65To74" m
     f75AndOver <- lookupX "Female75AndOver" m
     m18To24 <- lookupX "Male18To24" m
     m25To44 <- lookupX "Male25To44" m
     m45To64 <- lookupX "Male45To64" m
     m65To74 <- lookupX "Male65To74" m
     m75AndOver <- lookupX "Male75AndOver" m
     fWNH18To24 <- lookupX "FemaleWhiteNonHispanic18To24" m
     fWNH25To44 <- lookupX "FemaleWhiteNonHispanic25To44" m
     fWNH45To64 <- lookupX "FemaleWhiteNonHispanic45To64" m
     fWNH65To74 <- lookupX "FemaleWhiteNonHispanic65To74" m
     fWNH75AndOver <- lookupX "FemaleWhiteNonHispanic75AndOver" m
     mWNH18To24 <- lookupX "MaleWhiteNonHispanic18To24" m
     mWNH25To44 <- lookupX "MaleWhiteNonHispanic25To44" m
     mWNH45To64 <- lookupX "MaleWhiteNonHispanic45To64" m
     mWNH65To74 <- lookupX "MaleWhiteNonHispanic65To74" m
     mWNH75AndOver <- lookupX "MaleWhiteNonHispanic75AndOver" m
     let result = 
           [
             (WhiteFemale18To24, fWNH18To24)
           , (WhiteFemale25To44, fWNH25To44)
           , (WhiteFemale45To64, fWNH45To64)           
           , (WhiteFemale65To74, fWNH65To74)
           , (WhiteFemale75AndOver, fWNH75AndOver)
           , (WhiteMale18To24, mWNH18To24)
           , (WhiteMale25To44, mWNH25To44)
           , (WhiteMale45To64, mWNH45To64)           
           , (WhiteMale65To74, mWNH65To74)
           , (WhiteMale75AndOver, mWNH75AndOver)
           , (NonWhiteFemale18To24, f18To24 - fWNH18To24)
           , (NonWhiteFemale25To44, f25To44 - fWNH25To44)
           , (NonWhiteFemale45To64, f45To64 - fWNH45To64)           
           , (NonWhiteFemale65To74, f65To74 - fWNH65To74)
           , (NonWhiteFemale75AndOver, f75AndOver - fWNH75AndOver)
           , (NonWhiteMale18To24, m18To24 - mWNH18To24)
           , (NonWhiteMale25To44, m25To44 - mWNH25To44)
           , (NonWhiteMale45To64, m45To64 - mWNH45To64)           
           , (NonWhiteMale65To74, m65To74 - mWNH65To74)
           , (NonWhiteMale75AndOver, m75AndOver - mWNH75AndOver)
           ]
         totalInput = FL.fold FL.sum $ fmap snd $ L.filter (not . L.isSubsequenceOf "Hispanic" . T.unpack . fst) $ M.toList m
         totalResult = FL.fold FL.sum $ fmap snd $ result
     X.when (totalInput /= totalResult) $ X.throwError ("Totals don't match in mergeACSCounts")
     return result
     
   processDemographicData :: Monad m => Int -> F.Frame ASRDemographics -> X.ExceptT Text m (F.FrameRec (DemographicCounts ASR))
   processDemographicData year dd = 
     let makeRec :: (ASR , Int) ->  F.Record [DemographicCategory ASR, PopCount]
         makeRec (b, n) = b F.&: n F.&: V.RNil
         fromRec r = (F.rgetField @ACSKey r, F.rgetField @ACSCount r)
         unpack = MR.generalizeUnpack $ MR.unpackFilterOnField @Year (==year)
         assign = MR.generalizeAssign $ MR.assignKeysAndData @[StateAbbreviation,CongressionalDistrict] @[ACSKey,ACSCount]
         reduce = MR.makeRecsWithKeyM makeRec $ MR.ReduceFoldM (const $ MR.postMapM mergeACSCounts $ FL.generalize $ FL.premap fromRec FL.map) 
     in FL.foldM (MR.concatFoldM $ MR.mapReduceFoldM unpack assign reduce) dd
   
   mergeTurnoutRows :: Monad m => M.Map T.Text (Int, Int) -> X.ExceptT Text m [(ASR, Int, Int)]
   mergeTurnoutRows m = do
     let lookupX k = maybe (X.throwError $ "(mergeTurnoutRows) lookup failed for key=\"" <> k <> "\"") return . M.lookup k
     (wm18To24P, wm18To24V) <- lookupX "WhiteMale18To24" m
     (wm25To44P, wm25To44V) <- lookupX "WhiteMale25To44" m
     (wm45To64P, wm45To64V) <- lookupX "WhiteMale45To64" m
     (wm65To74P, wm65To74V) <- lookupX "WhiteMale65To74" m
     (wm75AndOverP, wm75AndOverV) <- lookupX "WhiteMale75AndOver" m
     (wf18To24P, wf18To24V) <- lookupX "WhiteFemale18To24" m
     (wf25To44P, wf25To44V) <- lookupX "WhiteFemale25To44" m
     (wf45To64P, wf45To64V) <- lookupX "WhiteFemale45To64" m
     (wf65To74P, wf65To74V) <- lookupX "WhiteFemale65To74" m
     (wf75AndOverP, wf75AndOverV) <- lookupX "WhiteFemale75AndOver" m
     (bm18To24P, bm18To24V) <- lookupX "BlackMale18To24" m
     (bm25To44P, bm25To44V) <- lookupX "BlackMale25To44" m
     (bm45To64P, bm45To64V) <- lookupX "BlackMale45To64" m
     (bm65To74P, bm65To74V) <- lookupX "BlackMale65To74" m
     (bm75AndOverP, bm75AndOverV) <- lookupX "BlackMale75AndOver"  m
     (bf18To24P, bf18To24V) <- lookupX "BlackFemale18To24"  m
     (bf25To44P, bf25To44V) <- lookupX "BlackFemale25To44" m
     (bf45To64P, bf45To64V) <- lookupX "BlackFemale45To64" m
     (bf65To74P, bf65To74V) <- lookupX "BlackFemale65To74" m
     (bf75AndOverP, bf75AndOverV) <- lookupX "BlackFemale75AndOver" m
     (am18To24P, am18To24V) <- lookupX "AsianMale18To24" m
     (am25To44P, am25To44V) <- lookupX "AsianMale25To44" m
     (am45To64P, am45To64V) <- lookupX "AsianMale45To64" m
     (am65To74P, am65To74V) <- lookupX "AsianMale65To74" m
     (am75AndOverP, am75AndOverV) <- lookupX "AsianMale75AndOver" m
     (af18To24P, af18To24V) <- lookupX "AsianFemale18To24" m
     (af25To44P, af25To44V) <- lookupX "AsianFemale25To44" m
     (af45To64P, af45To64V) <- lookupX "AsianFemale45To64" m
     (af65To74P, af65To74V) <- lookupX "AsianFemale65To74" m
     (af75AndOverP, af75AndOverV) <- lookupX "AsianFemale75AndOver" m
     (hm18To24P, hm18To24V) <- lookupX "HispanicMale18To24" m
     (hm25To44P, hm25To44V) <- lookupX "HispanicMale25To44" m
     (hm45To64P, hm45To64V) <- lookupX "HispanicMale45To64" m
     (hm65To74P, hm65To74V) <- lookupX "HispanicMale65To74" m
     (hm75AndOverP, hm75AndOverV) <- lookupX "HispanicMale75AndOver" m
     (hf18To24P, hf18To24V) <- lookupX "HispanicFemale18To24" m
     (hf25To44P, hf25To44V) <- lookupX "HispanicFemale25To44" m
     (hf45To64P, hf45To64V) <- lookupX "HispanicFemale45To64" m
     (hf65To74P, hf65To74V) <- lookupX "HispanicFemale65To74" m
     (hf75AndOverP, hf75AndOverV) <- lookupX "HispanicFemale75AndOver" m
     let result =
           [
             (WhiteFemale18To24, wf18To24P, wf18To24V)
           , (WhiteFemale25To44, wf25To44P, wf25To44V)
           , (WhiteFemale45To64, wf45To64P, wf45To64V)           
           , (WhiteFemale65To74, wf65To74P, wf65To74V)
           , (WhiteFemale75AndOver, wf75AndOverP, wf75AndOverV)
           , (WhiteMale18To24, wm18To24P, wm18To24V)
           , (WhiteMale25To44, wm25To44P, wm25To44V)
           , (WhiteMale45To64, wm45To64P, wm45To64V)           
           , (WhiteMale65To74, wm65To74P, wm65To74V)
           , (WhiteMale75AndOver, wm75AndOverP, wm75AndOverV)
           , (NonWhiteFemale18To24, bf18To24P + af18To24P + hf18To24P, bf18To24V + af18To24V + hf18To24V)
           , (NonWhiteFemale25To44, bf25To44P + af25To44P + hf25To44P, bf25To44V + af25To44V + hf25To44V)
           , (NonWhiteFemale45To64, bf45To64P + af45To64P + hf45To64P, bf45To64V + af45To64V + hf45To64V)
           , (NonWhiteFemale65To74, bf65To74P + af65To74P + hf65To74P, bf65To74V + af65To74V + hf65To74V)
           , (NonWhiteFemale75AndOver, bf75AndOverP + af75AndOverP + hf75AndOverP, bf75AndOverV + af75AndOverV + hf75AndOverV)
           , (NonWhiteMale18To24, bm18To24P + am18To24P + hm18To24P, bm18To24V + am18To24V + hm18To24V)
           , (NonWhiteMale25To44, bm25To44P + am25To44P + hm25To44P, bm25To44V + am25To44V + hm25To44V)
           , (NonWhiteMale45To64, bm45To64P + am45To64P + hm45To64P, bm45To64V + am45To64V + hm45To64V)
           , (NonWhiteMale65To74, bm65To74P + am65To74P + hm65To74P, bm65To74V + am65To74V + hm65To74V)
           , (NonWhiteMale75AndOver, bm75AndOverP + am75AndOverP + hm75AndOverP, bm75AndOverV + am75AndOverV + hm75AndOverV)
           ]
         (inputP, inputV) = FL.fold ((,) <$> FL.premap fst FL.sum <*> FL.premap snd FL.sum) m
         (resultP, resultV) = FL.fold ((,) <$> FL.premap (\(_,x,_) -> x) FL.sum <*> FL.premap (\(_,_,x) -> x) FL.sum) result
     X.when (inputP /= resultP) $ X.throwError "Population totals don't agree in mergeTurnoutRows"
     X.when (inputV /= resultV) $ X.throwError "Voted totals don't agree in mergeTurnoutRows"
     return result
         
   processTurnoutData :: Monad m
     => Int
     -> F.Frame TurnoutASR
     -> X.ExceptT Text m (F.FrameRec '[DemographicCategory ASR, Population, VotedPctOfAll])
   processTurnoutData year td = 
    let makeRec :: (ASR,Int,Int) -> F.Record [DemographicCategory ASR, Population, VotedPctOfAll]
        makeRec (b,p,v) = b F.&: p F.&: (realToFrac v/realToFrac p) F.&: V.RNil
        fromRec r = (F.rgetField @Group r, (F.rgetField @Population r, F.rgetField @Voted r))
        unpack = MR.generalizeUnpack $ MR.unpackFilterOnField @Year (==year)
        assign = MR.generalizeAssign $ MR.assignKeysAndData @'[] @[Group,Population,Voted]
        reduce = MR.makeRecsWithKeyM makeRec $ MR.ReduceFoldM (const $ MR.postMapM mergeTurnoutRows $ FL.generalize $ FL.premap fromRec FL.map) 
    in FL.foldM (MR.concatFoldM $ MR.mapReduceFoldM unpack assign reduce) td

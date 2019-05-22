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
F.tableTypes "TurnoutRSA"          detailedRSATurnoutCSV
F.tableTypes "IdentityDemographics" identityDemographicsLongCSV

-- This one might be different for different breakdowns
--F.tableTypes' (F.rowGen identityDemographics2016CSV) {F.rowTypeName = "AgeSexRaceByDistrict", F.tablePrefix = "Census" }


F.declareColumn "PopCount" ''Int

type DemographicCategory b = "DemographicCategory" F.:-> b  -- general holder
type LocationKey = '[StateAbbreviation, CongressionalDistrict]

type DemographicCounts b = LocationKey V.++ [DemographicCategory b, PopCount]

data DemographicStructure demographicDataRow electionDataRow demographicCategories = DemographicStructure
  {
    dsPreprocessDemographicData :: (forall m. Monad m => Int -> F.Frame demographicDataRow -> X.ExceptT Text m (F.FrameRec (DemographicCounts demographicCategories)))
  , dsPreprocessTurnoutData :: (forall m. Monad m => Int -> F.Frame TurnoutRSA -> X.ExceptT Text m (F.FrameRec '[DemographicCategory demographicCategories, Population, VotedPctOfAll]))
  , dsPreprocessElectionData :: (forall m. Monad m => Int -> F.Frame electionDataRow -> X.ExceptT Text m (F.FrameRec (LocationKey V.++ [DVotes, RVotes, Totalvotes])))
  , dsCategories :: [demographicCategories]
  }

data SimpleASR = OldNonWhiteFemale
               | YoungNonWhiteFemale
               | OldNonWhiteMale
               | YoungNonWhiteMale
               | OldWhiteFemale
               | YoungWhiteFemale
               | OldWhiteMale
               | YoungWhiteMale deriving (Show,Read,Enum,Bounded,Eq,Ord,Ix,Generic)



type instance FI.VectorFor SimpleASR = V.Vector
type instance FI.VectorFor (A.Array b Int) = V.Vector
instance Hashable SimpleASR


simpleAgeSexRace :: DemographicStructure IdentityDemographics HouseElections SimpleASR
simpleAgeSexRace = DemographicStructure processDemographicData processTurnoutData mapElectionData [minBound ..]
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
     
   processDemographicData :: Monad m => Int -> F.Frame IdentityDemographics -> X.ExceptT Text m (F.FrameRec (DemographicCounts SimpleASR))
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
     -> F.Frame TurnoutRSA
     -> X.ExceptT Text m (F.FrameRec '[DemographicCategory SimpleASR, Population, VotedPctOfAll])
   processTurnoutData year td = 
    let makeRec :: (SimpleASR,Int,Int) -> F.Record [DemographicCategory SimpleASR, Population, VotedPctOfAll]
        makeRec (b,p,v) = b F.&: p F.&: (realToFrac v/realToFrac p) F.&: V.RNil
--        fromRec :: F.Record (F.RecordColumns TurnoutRSA) -> (Text, (Int, Int))
        fromRec r = (F.rgetField @Group r, (F.rgetField @Population r, F.rgetField @Voted r))
        unpack = MR.generalizeUnpack $ MR.unpackFilterOnField @Year (==year)
        assign = MR.generalizeAssign $ MR.assignKeysAndData @'[] @[Group,Population,Voted]
        reduce = MR.makeRecsWithKeyM makeRec $ MR.ReduceFoldM (const $ MR.postMapM mergeTurnoutRows $ FL.generalize $ FL.premap fromRec FL.map) 
    in FL.foldM (MR.concatFoldM $ MR.mapReduceFoldM unpack assign reduce) td
     


   mapElectionData :: Monad m => Int -> F.Frame HouseElections
                   -> X.ExceptT Text m (F.FrameRec (LocationKey V.++ [DVotes, RVotes, Totalvotes]))
   mapElectionData year eData = do
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
{-
    case (dCand, rCand) of
       (Multi _, OneCand _) -> Left $ "More than one democrat in competitive election: " <> (T.pack $ show m)
       (OneCand _,Multi _) -> Left $ "More than one republican in competitive election: " <> (T.pack $ show m)
       (Multi _, Multi _) -> Left $ "More than one democrat and more than one republican: " <> (T.pack $ show m)
       (dem, rep) -> Right $ 
-}

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

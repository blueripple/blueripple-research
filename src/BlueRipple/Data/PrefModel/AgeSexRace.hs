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

module BlueRipple.Data.PrefModel.AgeSexRace where

import           BlueRipple.Data.DataFrames
import           BlueRipple.Data.DemographicTypes
import           BlueRipple.Data.PrefModel
import           BlueRipple.Data.PrefModel.ASRTypes

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
         getPair x = lookupX (asrTurnoutLabel x) m
     (am18To24P, am18To24V) <- getPair (A18To24,Male,All) 
     (am25To44P, am25To44V) <- getPair (A25To44,Male,All)
     (am45To64P, am45To64V) <- getPair (A45To64,Male,All)
     (am65To74P, am65To74V) <- getPair (A65To74,Male,All)
     (am75AndOverP, am75AndOverV) <- getPair (A75AndOver,Male,All)
     (af18To24P, af18To24V) <- getPair (A18To24,Female,All)
     (af25To44P, af25To44V) <- getPair (A25To44,Female,All)
     (af45To64P, af45To64V) <- getPair (A45To64,Female,All)
     (af65To74P, af65To74V) <- getPair (A65To74,Female,All)
     (af75AndOverP, af75AndOverV) <- getPair (A75AndOver,Female,All)
     (wnhm18To24P, wnhm18To24V) <- getPair (A18To24,Male,WhiteNonHispanic) 
     (wnhm25To44P, wnhm25To44V) <- getPair (A25To44,Male,WhiteNonHispanic)
     (wnhm45To64P, wnhm45To64V) <- getPair (A45To64,Male,WhiteNonHispanic)
     (wnhm65To74P, wnhm65To74V) <- getPair (A65To74,Male,WhiteNonHispanic)
     (wnhm75AndOverP, wnhm75AndOverV) <- getPair (A75AndOver,Male,WhiteNonHispanic)
     (wnhf18To24P, wnhf18To24V) <- getPair (A18To24,Female,WhiteNonHispanic)
     (wnhf25To44P, wnhf25To44V) <- getPair (A25To44,Female,WhiteNonHispanic)
     (wnhf45To64P, wnhf45To64V) <- getPair (A45To64,Female,WhiteNonHispanic)
     (wnhf65To74P, wnhf65To74V) <- getPair (A65To74,Female,WhiteNonHispanic)
     (wnhf75AndOverP, wnhf75AndOverV) <- getPair (A75AndOver,Female,WhiteNonHispanic)
     let result =
           [
             (WhiteFemale18To24, wnhf18To24P, wnhf18To24V)
           , (WhiteFemale25To44, wnhf25To44P, wnhf25To44V)
           , (WhiteFemale45To64, wnhf45To64P, wnhf45To64V)           
           , (WhiteFemale65To74, wnhf65To74P, wnhf65To74V)
           , (WhiteFemale75AndOver, wnhf75AndOverP, wnhf75AndOverV)
           , (WhiteMale18To24, wnhm18To24P, wnhm18To24V)
           , (WhiteMale25To44, wnhm25To44P, wnhm25To44V)
           , (WhiteMale45To64, wnhm45To64P, wnhm45To64V)           
           , (WhiteMale65To74, wnhm65To74P, wnhm65To74V)
           , (WhiteMale75AndOver, wnhm75AndOverP, wnhm75AndOverV)           
           , (NonWhiteFemale18To24, af18To24P - wnhf18To24P,af18To24V - wnhf18To24V)
           , (NonWhiteFemale25To44, af25To44P - wnhf25To44P,af25To44V - wnhf25To44V)
           , (NonWhiteFemale45To64, af45To64P - wnhf45To64P,af45To64V - wnhf45To64V)
           , (NonWhiteFemale65To74, af65To74P - wnhf65To74P,af65To74V - wnhf65To74V)
           , (NonWhiteFemale75AndOver, af75AndOverP - wnhf75AndOverP,af75AndOverV - wnhf75AndOverV)
           , (NonWhiteMale18To24, am18To24P - wnhm18To24P, am18To24V - wnhm18To24V)
           , (NonWhiteMale25To44, am25To44P - wnhm25To44P, am25To44V - wnhm25To44V)
           , (NonWhiteMale45To64, am45To64P - wnhm45To64P, am45To64V - wnhm45To64V)
           , (NonWhiteMale65To74, am65To74P - wnhm65To74P, am65To74V - wnhm65To74V)
           , (NonWhiteMale75AndOver, am75AndOverP - wnhm75AndOverP, am75AndOverV - wnhm75AndOverV)          
           ]
         (inputP, inputV) = FL.fold ((,) <$> FL.premap fst FL.sum <*> FL.premap snd FL.sum)
                            $ fmap snd
                            $ L.filter (L.isSubsequenceOf "All" . T.unpack . fst)
                            $ M.toList m
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

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

module BlueRipple.Data.PrefModel.SimpleAgeSexRace where

import BlueRipple.Data.DataFrames
import BlueRipple.Data.PrefModel
import BlueRipple.Data.PrefModel.ASRTypes

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

data SimpleAge = Old | Young
ages :: SimpleAge -> [Age]
ages Old = [A45To64,A65To74,A75AndOver]
ages Young = [A18To24,A25To44]

--foldLookup :: Monad m => (b -> m c) -> FL.Fold c d -> FL.FoldM m b d 
--foldLookup g fld = FL.premapM g $ FL.generalize fld

simpleAgeSexRace :: DemographicStructure ASRDemographics TurnoutASR HouseElections SimpleASR
simpleAgeSexRace = DemographicStructure processDemographicData processTurnoutData processElectionData [minBound ..]
 where
   mergeACSCounts :: Monad m => M.Map T.Text Int -> X.ExceptT Text m [(SimpleASR, Int)]
   mergeACSCounts m = do
     let lookupX k = maybe (X.throwError $ "(mergeACSCounts) lookup failed for key=\"" <> k <> "\"") return . M.lookup k
         sumLookup g = FL.foldM (FL.premapM (flip lookupX m) (FL.generalize FL.sum)) g
     oldFemale <- sumLookup $ fmap asACSLabel  [(a,Female) | a<-ages Old]
     youngFemale <- sumLookup $ fmap asACSLabel  [(a,Female) | a<-ages Young]
     oldMale <- sumLookup $ fmap asACSLabel  [(a,Male) | a<-ages Old]
     youngMale <- sumLookup $ fmap asACSLabel  [(a,Male) | a<-ages Young]
     oldWNHFemale <- sumLookup $ fmap asrACSLabel [(a,Female,WhiteNonHispanic) | a<-ages Old]
     youngWNHFemale <- sumLookup $ fmap asrACSLabel [(a,Female,WhiteNonHispanic) | a<-ages Young]
     oldWNHMale <- sumLookup $ fmap asrACSLabel [(a,Male,WhiteNonHispanic) | a<-ages Old]
     youngWNHMale <- sumLookup $ fmap asrACSLabel [(a,Male,WhiteNonHispanic) | a<-ages Young]
     let result =
           [ (OldNonWhiteFemale, oldFemale - oldWNHFemale) 
           , (YoungNonWhiteFemale, youngFemale - youngWNHFemale)
           , (OldNonWhiteMale, oldMale - oldWNHMale)
           , (YoungNonWhiteMale, youngMale - youngWNHMale)
           , (OldWhiteFemale, oldWNHFemale)
           , (YoungWhiteFemale, youngWNHFemale)
           , (OldWhiteMale, oldWNHMale)
           , (YoungWhiteMale, youngWNHMale)
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
         sumPair = FL.Fold (\(tA, tB) (a, b) -> (tA + a, tB +b)) (0, 0) id 
         sumLookup g = FL.foldM (FL.premapM (flip lookupX m) (FL.generalize sumPair)) g
     (oldFemaleP, oldFemaleV) <- sumLookup $ fmap asrTurnoutLabel  [(a,Female,All) | a<-ages Old]
     (youngFemaleP, youngFemaleV) <- sumLookup $ fmap asrTurnoutLabel  [(a,Female,All) | a<-ages Young]
     (oldMaleP, oldMaleV) <- sumLookup $ fmap asrTurnoutLabel  [(a,Male,All) | a<-ages Old]
     (youngMaleP, youngMaleV) <- sumLookup $ fmap asrTurnoutLabel  [(a,Male,All) | a<-ages Young]
     (oldWNHFemaleP, oldWNHFemaleV) <- sumLookup $ fmap asrTurnoutLabel [(a,Female,WhiteNonHispanic) | a<-ages Old]
     (youngWNHFemaleP, youngWNHFemaleV) <- sumLookup $ fmap asrTurnoutLabel [(a,Female,WhiteNonHispanic) | a<-ages Young]
     (oldWNHMaleP, oldWNHMaleV) <- sumLookup $ fmap asrTurnoutLabel [(a,Male,WhiteNonHispanic) | a<-ages Old]
     (youngWNHMaleP, youngWNHMaleV) <- sumLookup $ fmap asrTurnoutLabel [(a,Male,WhiteNonHispanic) | a<-ages Young]
     let result =
           [ (OldNonWhiteFemale, oldFemaleP - oldWNHFemaleP, oldFemaleV - oldWNHFemaleV)
           , (YoungNonWhiteFemale, youngFemaleP - youngWNHFemaleP, youngFemaleV - youngWNHFemaleV)
           , (OldNonWhiteMale, oldMaleP - oldWNHMaleP, oldMaleV -oldWNHMaleV)
           , (YoungNonWhiteMale, youngMaleP - youngWNHMaleP, youngMaleV - youngWNHMaleV)
           , (OldWhiteFemale, oldWNHFemaleP, oldWNHFemaleV)
           , (YoungWhiteFemale, youngWNHFemaleP, youngWNHFemaleV)
           , (OldWhiteMale, oldWNHMaleP, oldWNHMaleV)
           , (YoungWhiteMale, youngWNHMaleP, youngWNHMaleV)
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
     -> X.ExceptT Text m (F.FrameRec '[DemographicCategory SimpleASR, Population, VotedPctOfAll])
   processTurnoutData year td = 
    let makeRec :: (SimpleASR,Int,Int) -> F.Record [DemographicCategory SimpleASR, Population, VotedPctOfAll]
        makeRec (b,p,v) = b F.&: p F.&: (realToFrac v/realToFrac p) F.&: V.RNil
        fromRec r = (F.rgetField @Group r, (F.rgetField @Population r, F.rgetField @Voted r))
        unpack = MR.generalizeUnpack $ MR.unpackFilterOnField @Year (==year)
        assign = MR.generalizeAssign $ MR.assignKeysAndData @'[] @[Group,Population,Voted]
        reduce = MR.makeRecsWithKeyM makeRec $ MR.ReduceFoldM (const $ MR.postMapM mergeTurnoutRows $ FL.generalize $ FL.premap fromRec FL.map) 
    in FL.foldM (MR.concatFoldM $ MR.mapReduceFoldM unpack assign reduce) td


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

import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Data.DemographicTypes as BR
import qualified BlueRipple.Data.PrefModel as BR
--import BlueRipple.Data.PrefModel.ASRTypes

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

import           GHC.TypeLits (Symbol)
import qualified Data.Serialize as S
import           Data.Kind (Type)

data SimpleASR = OldNonWhiteFemale
               | YoungNonWhiteFemale
               | OldNonWhiteMale
               | YoungNonWhiteMale
               | OldWhiteFemale
               | YoungWhiteFemale
               | OldWhiteMale
               | YoungWhiteMale deriving (Show,Read,Enum,Bounded,Eq,Ord,Ix,Generic)

data SimpleSR = NonWhiteFemale | WhiteFemale | NonWhiteMale | WhiteMale deriving (Show, Read, Enum, Bounded, Eq, Ord,Ix, Generic)

simpleASR2SimpleSR :: Num a => A.Array SimpleASR a -> A.Array SimpleSR a
simpleASR2SimpleSR x = A.array (minBound,maxBound) [(NonWhiteFemale, x A.! OldNonWhiteFemale + x A.! YoungNonWhiteFemale)
                                                   ,(WhiteFemale, x A.! OldWhiteFemale + x A.! YoungWhiteFemale)
                                                   ,(NonWhiteMale, x A.! OldNonWhiteMale + x A.! YoungNonWhiteMale)
                                                   ,(WhiteMale, x A.! OldWhiteMale + x A.! YoungWhiteMale)
                                                   ]
                       
data SimpleAR = OldNonWhite | YoungNonWhite | OldWhite | YoungWhite deriving (Show, Read, Enum, Bounded, Eq, Ord,Ix, Generic)

simpleASR2SimpleAR :: Num a => A.Array SimpleASR a -> A.Array SimpleAR a
simpleASR2SimpleAR x = A.array (minBound,maxBound) [(OldNonWhite, x A.! OldNonWhiteFemale + x A.! OldNonWhiteMale)
                                                   ,(YoungNonWhite, x A.! YoungNonWhiteFemale + x A.! YoungNonWhiteMale)
                                                   ,(OldWhite, x A.! OldWhiteFemale + x A.! OldWhiteMale)
                                                   ,(YoungWhite, x A.! YoungWhiteFemale + x A.! YoungWhiteMale)
                                                   ]

type instance FI.VectorFor SimpleASR = V.Vector
instance Hashable SimpleASR

--foldLookup :: Monad m => (b -> m c) -> FL.Fold c d -> FL.FoldM m b d 
--foldLookup g fld = FL.premapM g $ FL.generalize fld

simpleAgeSexRace :: BR.DemographicStructure BR.ASRDemographics BR.TurnoutASR BR.HouseElections SimpleASR
simpleAgeSexRace = BR.DemographicStructure processDemographicData processTurnoutData BR.processElectionData [minBound ..]
 where
   mergeACSCounts :: Monad m => M.Map T.Text Int -> X.ExceptT Text m [(SimpleASR, Int)]
   mergeACSCounts m = do
     let lookupX k = maybe (X.throwError $ "(mergeACSCounts) lookup failed for key=\"" <> k <> "\"") return . M.lookup k
         sumLookup g = FL.foldM (FL.premapM (flip lookupX m) (FL.generalize FL.sum)) g
     oldFemale <- sumLookup $ fmap BR.asACSLabel  [(a, BR.Female) | a<-BR.simpleAgeFrom5 BR.EqualOrOver]
     youngFemale <- sumLookup $ fmap BR.asACSLabel  [(a, BR.Female) | a<-BR.simpleAgeFrom5 BR.Under]
     oldMale <- sumLookup $ fmap BR.asACSLabel  [(a, BR.Male) | a<-BR.simpleAgeFrom5 BR.EqualOrOver]
     youngMale <- sumLookup $ fmap BR.asACSLabel  [(a, BR.Male) | a<-BR.simpleAgeFrom5 BR.Under]
     oldWNHFemale <- sumLookup $ fmap BR.asrACSLabel [(a, BR.Female, BR.ACS_WhiteNonHispanic) | a<-BR.simpleAgeFrom5 BR.EqualOrOver]
     youngWNHFemale <- sumLookup $ fmap BR.asrACSLabel [(a, BR.Female, BR.ACS_WhiteNonHispanic) | a<-BR.simpleAgeFrom5 BR.Under]
     oldWNHMale <- sumLookup $ fmap BR.asrACSLabel [(a, BR.Male, BR.ACS_WhiteNonHispanic) | a<-BR.simpleAgeFrom5 BR.EqualOrOver]
     youngWNHMale <- sumLookup $ fmap BR.asrACSLabel [(a, BR.Male, BR.ACS_WhiteNonHispanic) | a<-BR.simpleAgeFrom5 BR.Under]
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
     
   processDemographicData :: Monad m => Int -> F.Frame BR.ASRDemographics -> X.ExceptT Text m (F.FrameRec (BR.DemographicCounts SimpleASR))
   processDemographicData year dd = 
     let makeRec :: (SimpleASR , Int) ->  F.Record [BR.DemographicCategory SimpleASR, BR.PopCount]
         makeRec (b, n) = b F.&: n F.&: V.RNil
         fromRec r = (F.rgetField @BR.ACSKey r, F.rgetField @BR.ACSCount r)
         unpack = MR.generalizeUnpack $ MR.unpackFilterOnField @BR.Year (==year)
         assign = MR.generalizeAssign $ MR.assignKeysAndData @[BR.StateAbbreviation, BR.CongressionalDistrict] @[BR.ACSKey, BR.ACSCount]
         reduce = MR.makeRecsWithKeyM makeRec $ MR.ReduceFoldM (const $ MR.postMapM mergeACSCounts $ FL.generalize $ FL.premap fromRec FL.map) 
     in FL.foldM (MR.concatFoldM $ MR.mapReduceFoldM unpack assign reduce) dd
   
   mergeTurnoutRows :: Monad m => M.Map T.Text (Int, Int) -> X.ExceptT Text m [(SimpleASR, Int, Int)]
   mergeTurnoutRows m = do     
     let lookupX k = maybe (X.throwError $ "(mergeTurnoutRows) lookup failed for key=\"" <> k <> "\"") return . M.lookup k
         sumPair = FL.Fold (\(tA, tB) (a, b) -> (tA + a, tB +b)) (0, 0) id 
         sumLookup g = FL.foldM (FL.premapM (flip lookupX m) (FL.generalize sumPair)) g
     (oldFemaleP, oldFemaleV) <- sumLookup $ fmap BR.asrTurnoutLabel  [(a, BR.Female, BR.ACS_All) | a<-BR.simpleAgeFrom5 BR.EqualOrOver]
     (youngFemaleP, youngFemaleV) <- sumLookup $ fmap BR.asrTurnoutLabel  [(a, BR.Female, BR.ACS_All) | a<-BR.simpleAgeFrom5 BR.Under]
     (oldMaleP, oldMaleV) <- sumLookup $ fmap BR.asrTurnoutLabel  [(a, BR.Male, BR.ACS_All) | a<-BR.simpleAgeFrom5 BR.EqualOrOver]
     (youngMaleP, youngMaleV) <- sumLookup $ fmap BR.asrTurnoutLabel  [(a, BR.Male, BR.ACS_All) | a<-BR.simpleAgeFrom5 BR.Under]
     (oldWNHFemaleP, oldWNHFemaleV) <- sumLookup $ fmap BR.asrTurnoutLabel [(a, BR.Female, BR.ACS_WhiteNonHispanic) | a<-BR.simpleAgeFrom5 BR.EqualOrOver]
     (youngWNHFemaleP, youngWNHFemaleV) <- sumLookup $ fmap BR.asrTurnoutLabel [(a, BR.Female, BR.ACS_WhiteNonHispanic) | a<-BR.simpleAgeFrom5 BR.Under]
     (oldWNHMaleP, oldWNHMaleV) <- sumLookup $ fmap BR.asrTurnoutLabel [(a, BR.Male, BR.ACS_WhiteNonHispanic) | a<-BR.simpleAgeFrom5 BR.EqualOrOver]
     (youngWNHMaleP, youngWNHMaleV) <- sumLookup $ fmap BR.asrTurnoutLabel [(a, BR.Male, BR.ACS_WhiteNonHispanic) | a<-BR.simpleAgeFrom5 BR.Under]
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
     -> F.Frame BR.TurnoutASR
     -> X.ExceptT Text m (F.FrameRec '[BR.DemographicCategory SimpleASR, BR.Population, BR.VotedPctOfAll])
   processTurnoutData year td = 
    let makeRec :: (SimpleASR,Int,Int) -> F.Record [BR.DemographicCategory SimpleASR, BR.Population, BR.VotedPctOfAll]
        makeRec (b,p,v) = b F.&: p F.&: (realToFrac v/realToFrac p) F.&: V.RNil
        fromRec r = (F.rgetField @BR.Group r, (F.rgetField @BR.Population r, F.rgetField @BR.Voted r))
        unpack = MR.generalizeUnpack $ MR.unpackFilterOnField @BR.Year (==year)
        assign = MR.generalizeAssign $ MR.assignKeysAndData @'[] @[BR.Group, BR.Population, BR.Voted]
        reduce = MR.makeRecsWithKeyM makeRec $ MR.ReduceFoldM (const $ MR.postMapM mergeTurnoutRows $ FL.generalize $ FL.premap fromRec FL.map) 
    in FL.foldM (MR.concatFoldM $ MR.mapReduceFoldM unpack assign reduce) td


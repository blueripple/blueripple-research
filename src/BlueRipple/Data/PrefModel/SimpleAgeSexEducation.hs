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

module BlueRipple.Data.PrefModel.SimpleAgeSexEducation where

import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Data.DemographicTypes as BR
import qualified BlueRipple.Data.PrefModel as BR

import qualified Control.Foldl                 as FL
import qualified Control.Monad.Except          as X
import qualified Data.Array                    as A
import qualified Data.List                     as L
import qualified Data.Map                      as M
import           Data.Maybe                     ( fromMaybe)
import           Data.Proxy                     ( Proxy(..) )
import qualified Data.Serialize                as S
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


data SimpleASE = OldFemaleNonGrad
               | YoungFemaleNonGrad
               | OldMaleNonGrad
               | YoungMaleNonGrad
               | OldFemaleCollegeGrad
               | YoungFemaleCollegeGrad
               | OldMaleCollegeGrad
               | YoungMaleCollegeGrad deriving (Show,Read,Enum,Bounded,Eq,Ord,Ix,Generic)

data SimpleSE = FemaleNonGrad | FemaleCollegeGrad | MaleNonGrad | MaleCollegeGrad deriving (Show, Read, Enum, Bounded, Eq, Ord,Ix, Generic)

simpleASE2SimpleSE :: Num a => A.Array SimpleASE a -> A.Array SimpleSE a
simpleASE2SimpleSE x = A.array (minBound,maxBound) [(FemaleNonGrad, x A.! OldFemaleNonGrad + x A.! YoungFemaleNonGrad)
                                                   ,(FemaleCollegeGrad, x A.! OldFemaleCollegeGrad + x A.! YoungFemaleCollegeGrad)
                                                   ,(MaleNonGrad, x A.! OldMaleNonGrad + x A.! YoungMaleNonGrad)
                                                   ,(MaleCollegeGrad, x A.! OldMaleCollegeGrad + x A.! YoungMaleCollegeGrad)
                                                   ]


type instance FI.VectorFor SimpleASE = V.Vector
instance Hashable SimpleASE

-- map SimpleASE to triples (which then get mapped to labels from the data) of Age, Sex, Education

acsASE :: SimpleASE -> [(BR.Age4, BR.Sex, BR.Education)]
acsASE YoungFemaleNonGrad = [(a,BR.Female,e) | a <- BR.simpleAgeFrom4 BR.Under, e <- BR.acsLevels BR.NonGrad]
acsASE YoungFemaleCollegeGrad = [(a,BR.Female,e) | a <- BR.simpleAgeFrom4 BR.Under, e <- BR.acsLevels BR.Grad]
acsASE OldFemaleNonGrad = [(a,BR.Female,e) | a <- BR.simpleAgeFrom4 BR.EqualOrOver, e <- BR.acsLevels BR.NonGrad]
acsASE OldFemaleCollegeGrad = [(a,BR.Female,e) | a <- BR.simpleAgeFrom4 BR.EqualOrOver, e <- BR.acsLevels BR.Grad]
acsASE YoungMaleNonGrad = [(a,BR.Male,e) | a <- BR.simpleAgeFrom4 BR.Under, e <- BR.acsLevels BR.NonGrad]
acsASE YoungMaleCollegeGrad = [(a,BR.Male,e) | a <- BR.simpleAgeFrom4 BR.Under, e <- BR.acsLevels BR.Grad]
acsASE OldMaleNonGrad = [(a,BR.Male,e) | a <- BR.simpleAgeFrom4 BR.EqualOrOver, e <- BR.acsLevels BR.NonGrad]
acsASE OldMaleCollegeGrad = [(a,BR.Male,e) | a <- BR.simpleAgeFrom4 BR.EqualOrOver, e <- BR.acsLevels BR.Grad]

turnoutASE :: SimpleASE -> [(BR.Age5, BR.Sex, BR.Education)]
turnoutASE YoungFemaleNonGrad = [(a,BR.Female,e) | a <- BR.simpleAgeFrom5 BR.Under, e <- BR.turnoutLevels BR.NonGrad]
turnoutASE YoungFemaleCollegeGrad = [(a,BR.Female,e) | a <- BR.simpleAgeFrom5 BR.Under, e <- BR.turnoutLevels BR.Grad]
turnoutASE OldFemaleNonGrad = [(a,BR.Female,e) | a <- BR.simpleAgeFrom5 BR.EqualOrOver, e <- BR.turnoutLevels BR.NonGrad]
turnoutASE OldFemaleCollegeGrad = [(a,BR.Female,e) | a <- BR.simpleAgeFrom5 BR.EqualOrOver, e <- BR.turnoutLevels BR.Grad]
turnoutASE YoungMaleNonGrad = [(a,BR.Male,e) | a <- BR.simpleAgeFrom5 BR.Under, e <- BR.turnoutLevels BR.NonGrad]
turnoutASE YoungMaleCollegeGrad = [(a,BR.Male,e) | a <- BR.simpleAgeFrom5 BR.Under, e <- BR.turnoutLevels BR.Grad]
turnoutASE OldMaleNonGrad = [(a,BR.Male,e) | a <- BR.simpleAgeFrom5 BR.EqualOrOver, e <- BR.turnoutLevels BR.NonGrad]
turnoutASE OldMaleCollegeGrad = [(a,BR.Male,e) | a <- BR.simpleAgeFrom5 BR.EqualOrOver, e <- BR.turnoutLevels BR.Grad]   


---
simpleAgeSexEducation :: BR.DemographicStructure BR.ASEDemographics BR.TurnoutASE BR.HouseElections SimpleASE
simpleAgeSexEducation = BR.DemographicStructure processDemographicData processTurnoutData BR.processElectionData [minBound ..]
 where
   mergeACSCounts :: Monad m => M.Map T.Text Int -> X.ExceptT Text m [(SimpleASE, Int)]
   mergeACSCounts m = do
     let lookupX k = maybe (X.throwError $ "(mergeACSCounts) lookup failed for key=\"" <> k <> "\"") return . M.lookup k
         sumLookup = FL.foldM (FL.premapM (flip lookupX m) (FL.generalize FL.sum)) . fmap BR.aseACSLabel
         allCats = [(minBound :: SimpleASE)..maxBound]
     resultSums <-  traverse (sumLookup . acsASE) allCats
     let result = zip allCats resultSums
         totalInput = FL.fold FL.sum $ fmap snd $ M.toList m
         totalResult = FL.fold FL.sum $ fmap snd $ result
     X.when (totalInput /= totalResult) $ X.throwError ("Totals don't match in mergeACSCounts (SimpleASE)")
     return result
     
   processDemographicData :: Monad m => Int -> F.Frame BR.ASEDemographics -> X.ExceptT Text m (F.FrameRec (BR.DemographicCounts SimpleASE))
   processDemographicData year dd = 
     let makeRec :: (SimpleASE , Int) ->  F.Record [BR.DemographicCategory SimpleASE, BR.PopCount]
         makeRec (b, n) = b F.&: n F.&: V.RNil
         fromRec r = (F.rgetField @BR.ACSKey r, F.rgetField @BR.ACSCount r)
         unpack = MR.generalizeUnpack $ MR.unpackFilterOnField @BR.Year (==year)
         assign = MR.generalizeAssign $ MR.assignKeysAndData @[BR.StateAbbreviation, BR.CongressionalDistrict] @[BR.ACSKey, BR.ACSCount]
         reduce = MR.makeRecsWithKeyM makeRec $ MR.ReduceFoldM (const $ MR.postMapM mergeACSCounts $ FL.generalize $ FL.premap fromRec FL.map) 
     in FL.foldM (MR.concatFoldM $ MR.mapReduceFoldM unpack assign reduce) dd
   
   mergeTurnoutRows :: Monad m => M.Map T.Text (Int, Int) -> X.ExceptT Text m [(SimpleASE, Int, Int)]
   mergeTurnoutRows m = do
     let lookupX k = maybe (X.throwError $ "(mergeTurnoutRows) lookup failed for key=\"" <> k <> "\"") return . M.lookup k
         sumPair = FL.Fold (\(tA, tB) (a, b) -> (tA + a, tB +b)) (0, 0) id 
         sumLookup = FL.foldM (FL.premapM (flip lookupX m) (FL.generalize sumPair)) . fmap BR.aseTurnoutLabel
         allCats = [(minBound :: SimpleASE)..maxBound]
     resultSums <-  traverse (sumLookup . turnoutASE) allCats
     let result = zipWith (\c (p,v) -> (c,p,v)) allCats resultSums
         (inputP, inputV) = FL.fold ((,) <$> FL.premap fst FL.sum <*> FL.premap snd FL.sum) m
         (resultP, resultV) = FL.fold ((,) <$> FL.premap (\(_,x,_) -> x) FL.sum <*> FL.premap (\(_,_,x) -> x) FL.sum) result
     X.when (inputP /= resultP) $ X.throwError "Population totals don't agree in mergeTurnoutRows (SimpleASE)"
     X.when (inputV /= resultV) $ X.throwError "Voted totals don't agree in mergeTurnoutRows (SimpleASE)"
     return result
         
   processTurnoutData :: Monad m
     => Int
     -> F.Frame BR.TurnoutASE
     -> X.ExceptT Text m (F.FrameRec '[BR.DemographicCategory SimpleASE, BR.Population, BR.VotedPctOfAll])
   processTurnoutData year td = 
    let makeRec :: (SimpleASE,Int,Int) -> F.Record [BR.DemographicCategory SimpleASE, BR.Population, BR.VotedPctOfAll]
        makeRec (b,p,v) = b F.&: p F.&: (realToFrac v/realToFrac p) F.&: V.RNil
        fromRec r = (F.rgetField @BR.Group r, (F.rgetField @BR.Population r, F.rgetField @BR.Voted r))
        unpack = MR.generalizeUnpack $ MR.unpackFilterOnField @BR.Year (==year)
        assign = MR.generalizeAssign $ MR.assignKeysAndData @'[] @[BR.Group, BR.Population, BR.Voted]
        reduce = MR.makeRecsWithKeyM makeRec $ MR.ReduceFoldM (const $ MR.postMapM mergeTurnoutRows $ FL.generalize $ FL.premap fromRec FL.map) 
    in FL.foldM (MR.concatFoldM $ MR.mapReduceFoldM unpack assign reduce) td


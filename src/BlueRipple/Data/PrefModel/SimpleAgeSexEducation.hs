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

import           BlueRipple.Data.DataFrames
import           BlueRipple.Data.PrefModel
import           BlueRipple.Data.PrefModel.ASETypes

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

data SimpleEducation = NonGrad | Grad
data SimpleAge = Old | Young

-- map SimpleASE to triples (which then get mapped to labels from the data) of Age, Sex, Education

acsASE :: SimpleASE -> [(ACSAge, Sex, Education)]
acsASE YoungFemaleNonGrad = [(a,Female,e) | a <- acsAges Young, e <- acsLevels NonGrad]
acsASE YoungFemaleCollegeGrad = [(a,Female,e) | a <- acsAges Young, e <- acsLevels Grad]
acsASE OldFemaleNonGrad = [(a,Female,e) | a <- acsAges Old, e <- acsLevels NonGrad]
acsASE OldFemaleCollegeGrad = [(a,Female,e) | a <- acsAges Old, e <- acsLevels Grad]
acsASE YoungMaleNonGrad = [(a,Male,e) | a <- acsAges Young, e <- acsLevels NonGrad]
acsASE YoungMaleCollegeGrad = [(a,Male,e) | a <- acsAges Young, e <- acsLevels Grad]
acsASE OldMaleNonGrad = [(a,Male,e) | a <- acsAges Old, e <- acsLevels NonGrad]
acsASE OldMaleCollegeGrad = [(a,Male,e) | a <- acsAges Old, e <- acsLevels Grad]

turnoutASE :: SimpleASE -> [(TurnoutAge, Sex, Education)]
turnoutASE YoungFemaleNonGrad = [(a,Female,e) | a <- turnoutAges Young, e <- turnoutLevels NonGrad]
turnoutASE YoungFemaleCollegeGrad = [(a,Female,e) | a <- turnoutAges Young, e <- turnoutLevels Grad]
turnoutASE OldFemaleNonGrad = [(a,Female,e) | a <- turnoutAges Old, e <- turnoutLevels NonGrad]
turnoutASE OldFemaleCollegeGrad = [(a,Female,e) | a <- turnoutAges Old, e <- turnoutLevels Grad]
turnoutASE YoungMaleNonGrad = [(a,Male,e) | a <- turnoutAges Young, e <- turnoutLevels NonGrad]
turnoutASE YoungMaleCollegeGrad = [(a,Male,e) | a <- turnoutAges Young, e <- turnoutLevels Grad]
turnoutASE OldMaleNonGrad = [(a,Male,e) | a <- turnoutAges Old, e <- turnoutLevels NonGrad]
turnoutASE OldMaleCollegeGrad = [(a,Male,e) | a <- turnoutAges Old, e <- turnoutLevels Grad]   

acsLevels :: SimpleEducation -> [Education]
acsLevels NonGrad = [L9, L12, HS, SC, AS]
acsLevels Grad = [BA, AD]

turnoutLevels :: SimpleEducation -> [Education]
turnoutLevels NonGrad = [L9, L12, HS, SC] -- NB: Turnout data did not contain an Associates Degree row
turnoutLevels Grad = [BA, AD]

acsAges :: SimpleAge -> [ACSAge]
acsAges Old = [A45To64,A65AndOver]
acsAges Young = [A18To24,A25To44]

turnoutAges :: SimpleAge -> [TurnoutAge]
turnoutAges Old = [T45To64,T65To74,T75AndOver]
turnoutAges Young = [T18To24,T25To44]

---
simpleAgeSexEducation :: DemographicStructure ASEDemographics TurnoutASE HouseElections SimpleASE
simpleAgeSexEducation = DemographicStructure processDemographicData processTurnoutData processElectionData [minBound ..]
 where
   mergeACSCounts :: Monad m => M.Map T.Text Int -> X.ExceptT Text m [(SimpleASE, Int)]
   mergeACSCounts m = do
     let lookupX k = maybe (X.throwError $ "(mergeACSCounts) lookup failed for key=\"" <> k <> "\"") return . M.lookup k
         sumLookup = FL.foldM (FL.premapM (flip lookupX m) (FL.generalize FL.sum)) . fmap aseACSLabel
         allCats = [(minBound :: SimpleASE)..maxBound]
     resultSums <-  traverse (sumLookup . acsASE) allCats
     let result = zip allCats resultSums
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
         sumPair = FL.Fold (\(tA, tB) (a, b) -> (tA + a, tB +b)) (0, 0) id 
         sumLookup = FL.foldM (FL.premapM (flip lookupX m) (FL.generalize sumPair)) . fmap aseTurnoutLabel
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


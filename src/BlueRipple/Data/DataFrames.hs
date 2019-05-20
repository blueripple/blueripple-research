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
F.tableTypes "Turnout"          turnoutCSV
F.tableTypes "TurnoutRSA"          detailedRSAturnoutCSV
F.tableTypes "IdentityDemographics" identityDemographicsLongCSV

-- This one might be different for different breakdowns
--F.tableTypes' (F.rowGen identityDemographics2016CSV) {F.rowTypeName = "AgeSexRaceByDistrict", F.tablePrefix = "Census" }


F.declareColumn "PopCount" ''Int

type DemographicCategory b = "DemographicCategory" F.:-> b  -- general holder
type LocationKey = '[StateAbbreviation, CongressionalDistrict]

type DemographicCounts b = LocationKey V.++ [DemographicCategory b, PopCount]

data DemographicStructure demographicDataRow electionDataRow demographicCategories = DemographicStructure
  {
    dsPreprocessDemographicData :: Monad m => F.Frame demographicDataRow -> X.ExceptT Text m (F.FrameRec (DemographicCounts demographicCategories))
  , dsPreprocessTurnout :: Int -> F.Frame TurnoutRSA
                        -> X.ExceptT Text m ((F.FrameRec '[DemographicCategory demographicCategories, VotedPctOfAll]))
  , dsPreprocessElectionData :: Int -> F.Frame electionDataRow -> F.FrameRec (LocationKey V.++ [DVotes, RVotes, Totalvotes])
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
simpleAgeSexRace = DemographicStructure processDemograpicData typeTurnout mapElectionData [minBound ..]
 where
   mergeACSCounts :: Monad m => M.Map T.Text Int -> X.ExceptT m [(SimpleASR, Int)]
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
     return $
       [ (OldNonWhiteFemale, (f45To64 + f65To74 + f75AndOver - owf))
       , (YoungNonWhiteFemale, (f18To24 + f25To44 - ywf))
       , (OldNonWhiteMale, (m45To64 + m65To74 + m75AndOver - owm))
       , (YoungNonWhiteMale, (m18To24 + m25To44 - ywm))
       , (OldWhiteFemale, owf)
       , (YoungWhiteFemale, ywf)
       , (OldWhiteMale, owm)
       , (YoungWhiteMale, ywm)
       ]
     
   processDemographicData :: Monad m => Int -> F.Frame IdentityDemographics -> X.ExceptT Text m (F.FrameRec (DemographicCounts demographicCategories))
   processDemographicData year dd =
    let makeRec b n :: F.Record [[DemographicCategory SimpleASR, PopCount] = b F.&: n F.&: V.RNil 
        unpack = MR.generalizeUnpack $ MR.filterOnField @Year (==year)
        assign = MR.generalizeAssign $ MR.assignKeysAndData @[StateAbbreviation,CongressionalDistrict] @[ACSKey,ACSCount]
        reduce = MR.makeRecsWithKey (uncurry makeRec) $ MR.ReduceFoldM (const $ MR.postMapM mergeACSCounts $ FL.generalize FL.map) 
    in FL.foldM (MR.concatFold $ MR.mapReduceFold unpack assign reduce) dd
  
   mergeTurnoutRows :: Monad m => M.Map T.Text (Int, Int) -> X.ExceptT Text m (F.FrameRec '[DemographicCategory SimpleASR, VotedPctOfAll])
   mergeTurnoutRows m = do
     let lookupX = maybe (X.throwError $ "(mergeTurnoutRows) lookup failed for key=\"" <> k <> "\"") return . M.lookup k
     (wm18To24P, wm18To24V) <- lookupX "WhiteMale18To24"
     (wm25To34P, wm25To24V) <- lookupX "WhiteMale25To34"
     



typeTurnout
    :: F.Frame Turnout
    -> Maybe (F.FrameRec
         ( (F.RDelete Identity (F.RecordColumns Turnout))
             V.++
             '[DemographicCategory SimpleASR]
         ))
  typeTurnout f =
    let filtered = F.filterFrame (\r -> let i = F.rgetField @Identity r in (i /= "YoungMale")
                                                                           && (i /= "OldMale")
                                                                           && (i /= "YoungFemale")
                                                                           && (i /= "OldFemale")) f
    in fmap F.toFrame $ sequenceA $ fmap
    (F.rtraverse V.getCompose . FT.transformMaybe typeIdentity) (FL.fold FL.list filtered)

  mapElectionData :: Int -> F.Frame HouseElections
                  -> F.FrameRec (LocationKey V.++ [DVotes, RVotes, Totalvotes])
  mapElectionData year eData =
    let xDat = fmap (F.rcast @('[Year] V.++ LocationKey V.++ [Party, Candidatevotes, Totalvotes])
                     . FT.retypeColumn @District @CongressionalDistrict
                     . FT.retypeColumn @StatePo @StateAbbreviation
                    ) eData
        unpack = MR.unpackFilterOnField @Year (== year)
        assign = MR.assignKeysAndData @LocationKey @'[Party, Candidatevotes, Totalvotes]
        reduce = MR.foldAndAddKey flattenVotes
    in FL.fold (MR.concatFold $ MR.mapReduceFold unpack assign reduce) xDat



type DVotes = "DVotes" F.:-> Int
type RVotes = "RVotes" F.:-> Int
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
      

typeIdentity
  :: forall b. (Read b)
   => F.Record '[Identity]
  -> F.Rec (Maybe F.:. F.ElField) '[DemographicCategory b]
typeIdentity x =
  (V.Compose $ fmap V.Field $ TR.readMaybe @b (T.unpack $ F.rgetField @Identity x)) V.:& V.RNil

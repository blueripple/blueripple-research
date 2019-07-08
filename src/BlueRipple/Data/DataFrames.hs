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

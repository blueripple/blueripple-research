{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE PolyKinds           #-}
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
import qualified Knit.Report as K

import qualified Control.Foldl                 as FL
import           Control.Monad.IO.Class         ( MonadIO(liftIO) )
import qualified Control.Monad.Except          as X
import qualified Data.Array                    as A
import qualified Data.List                     as L
import qualified Data.Map                      as M
import           Data.Maybe                     ( fromMaybe, catMaybes)
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

import qualified Pipes                         as P
import qualified Pipes.Prelude                 as P

import qualified Frames.Folds                  as FF
import qualified Frames.ParseableTypes         as FP
import qualified Frames.Transform              as FT
import qualified Frames.MapReduce              as MR
import qualified Frames.Enumerations           as FE
import qualified Frames.MaybeUtils             as FM

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
F.tableTypes "EdisonExit2018" exitPoll2018CSV

-- these columns are parsed wrong so we fix them before parsing
--F.declareColumn "CCESVvRegstatus" ''Int  
--F.declareColumn "CCESHispanic"    ''Int
F.tableTypes' ccesRowGen

loadToFrame
  :: forall rs effs
   . ( MonadIO (K.Sem effs)
     , K.LogWithPrefixesLE effs
     , F.ReadRec rs
     , FI.RecVec rs
     , V.RMap rs
     )
  => F.ParserOptions
  -> FilePath
  -> (F.Record rs -> Bool)
  -> K.Sem effs (F.FrameRec rs)
loadToFrame po fp filterF = do
  let producer = F.readTableOpt po fp P.>-> P.filter filterF
  frame <- liftIO $ F.inCoreAoS producer
  let reportRows :: Foldable f => f x -> FilePath -> K.Sem effs ()
      reportRows f fn =
        K.logLE K.Diagnostic
          $  T.pack (show $ FL.fold FL.length f)
          <> " rows in "
          <> T.pack fn
  reportRows frame fp
  return frame

loadToMaybeRecs
  :: forall rs rs' effs
   . ( MonadIO (K.Sem effs)
     , K.LogWithPrefixesLE effs
     , F.ReadRec rs'
     , FI.RecVec rs'
     , V.RMap rs'
     , rs F.⊆ rs'
     )
  => F.ParserOptions
  -> (F.Rec (Maybe F.:. F.ElField) rs -> Bool)
  -> FilePath
  -> K.Sem effs [F.Rec (Maybe F.:. F.ElField) rs]
loadToMaybeRecs po filterF fp  = do
  let producerM = F.readTableMaybeOpt @_ @rs' po fp --P.>-> P.filter filterF
  listM :: [F.Rec (Maybe F.:. F.ElField) rs] <- liftIO $ F.runSafeEffect $ P.toListM $ producerM P.>-> P.map F.rcast P.>-> P.filter filterF  
  let reportRows :: Foldable f => f x -> FilePath -> K.Sem effs ()
      reportRows f fn =
        K.logLE K.Diagnostic
          $  T.pack (show $ FL.fold FL.length f)
          <> " rows in "
          <> T.pack fn
  reportRows listM fp
  return listM

maybeRecsToFrame :: (K.LogWithPrefixesLE effs
                    , FI.RecVec rs
                    , V.RFoldMap rs
                    , V.RPureConstrained V.KnownField rs
                    , V.RecApplicative rs
                    , V.RApply rs
                    )
                 => (F.Rec (Maybe F.:. F.ElField) rs -> (F.Rec (Maybe F.:. F.ElField) rs)) -- fix any Nothings you need to/can
                 -> (F.Record rs -> Bool) -- filter after removing Nothings
                 -> [F.Rec (Maybe F.:. F.ElField) rs]
                 -> K.Sem effs (F.FrameRec rs)
maybeRecsToFrame fixMissing filterRows maybeRecs = K.wrapPrefix "maybeRecsToFrame" $ do
  K.logLE K.Diagnostic $ "Input rows: " <> (T.pack $ show $ length maybeRecs)
  let missingPre = FM.whatsMissing maybeRecs
  K.logLE K.Diagnostic $ "Missing Data before fixing: \n" <> (T.pack $ show missingPre)
  let fixed = fmap fixMissing maybeRecs
      missingPost = FM.whatsMissing fixed
  K.logLE K.Diagnostic $ "Missing Data after fixing: \n" <> (T.pack $ show missingPost)      
  let droppedMissing = catMaybes $ fmap F.recMaybe fixed
  K.logLE K.Diagnostic $ "Rows after fixing and dropping missing: " <> (T.pack $ show $ length droppedMissing)
  let  filtered = L.filter filterRows droppedMissing
  K.logLE K.Diagnostic $ "Rows after filtering: " <> (T.pack $ show $ length filtered)                 
  return $ F.toFrame filtered

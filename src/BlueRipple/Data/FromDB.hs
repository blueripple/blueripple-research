{-# LANGUAGE DataKinds                 #-}
{-# LANGUAGE ExplicitForAll            #-}
{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE FlexibleInstances         #-}
{-# LANGUAGE GADTs                     #-}
{-# LANGUAGE NoMonoLocalBinds          #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE RankNTypes                #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE TemplateHaskell           #-}
{-# LANGUAGE TypeApplications          #-}
{-# LANGUAGE TypeFamilies              #-}
module BlueRipple.Data.FromDB where

--import           ExploreFEC.Data.Spending                 (CandidateSpending (..),
--                                                           describeSpending)

import           OpenFEC.Beam.Sqlite.CustomFields         ()
import qualified OpenFEC.Beam.Types                       as FEC


import qualified Database.Beam                            as B
import qualified Database.Beam.Backend.SQL.BeamExtensions as BE
import qualified Database.Beam.Sqlite                     as B
import qualified Database.SQLite.Simple                   as SL

import           Control.Arrow                            ((&&&))
import qualified Control.Foldl                            as FL
import           Control.Lens                             (to, (.=), (.~), (^.))
import           Control.Monad                            (forM_)
import           Data.Default                             (Default)
import qualified Data.Foldable                            as F
import qualified Data.List                                as L
import qualified Data.Map                                 as M
import           Data.Maybe                               (fromMaybe)
import qualified Data.Sequence                            as Seq
import qualified Data.Set                                 as S
import qualified Data.Text                                as T
import qualified Data.Text.IO                             as T
import           Data.Time.Calendar.WeekDate              (toWeekDate)
import           Data.Time.LocalTime                      (LocalTime (..))
import           Data.Tuple.Select                        (sel1, sel2, sel3,
                                                           sel4, sel5, sel6,
                                                           sel7, sel8, sel9)
import           Data.Tuple.Update                        (upd5, upd6, upd7,
                                                           upd8)
import           Formattable.NumFormat                    (formatNum, usdFmt)
import qualified System.IO                                as Sys
import qualified Text.Printf                              as TP

data Forecast = Forecast { winP :: Double, voteShare :: Double {-, voteShare10 :: Double, voteShare90 :: Double -} } deriving (Show)
data Spending = Spending { disbursement :: FEC.Amount, indSupport :: FEC.Amount, indOppose :: FEC.Amount, party :: FEC.Amount } deriving (Show)

allRaces :: SL.Connection -> FEC.Office -> IO [(FEC.State, FEC.District)]
allRaces dbConn office = do
  let allCandidates =  B.all_ (FEC._openFEC_DB_candidate FEC.openFEC_DB)
      allHouseCandidates = B.filter_ (\c -> FEC._candidate_office c B.==. B.val_ office) $ allCandidates
      allElections = B.nub_ $ fmap (\c -> (FEC._candidate_state c, FEC._candidate_district c)) $ allHouseCandidates
  B.runBeamSqlite dbConn $ B.runSelectReturningList $ B.select $ allElections


electionResults :: SL.Connection -> IO (M.Map FEC.CandidateID Double)
electionResults dbConn = do
  asList <- B.runBeamSqlite dbConn $ B.runSelectReturningList $ B.select $ do
    result <- B.all_ (FEC._openFEC_DB_electionResult FEC.openFEC_DB)
    pure (result ^. FEC.electionResult_candidate_id, result ^. FEC.electionResult_voteshare)
  return $ M.fromList asList

spendingAndForecastByRace :: SL.Connection
                          -> FEC.Office
                          -> Maybe FEC.State
                          -> Maybe FEC.District
                          -> IO [(FEC.CandidateID, T.Text, LocalTime, Double, Double, FEC.Amount, FEC.Amount, FEC.Amount, FEC.Amount)]
spendingAndForecastByRace dbConn office stateM districtM = do
  let allCandidates = B.all_ (FEC._openFEC_DB_candidate FEC.openFEC_DB)
      allForecasts = B.all_ (FEC._openFEC_DB_forecast538 FEC.openFEC_DB)
      allDisbursements = B.all_ (FEC._openFEC_DB_disbursement FEC.openFEC_DB)
      allIndExpenditures = B.all_ (FEC._openFEC_DB_indExpenditure FEC.openFEC_DB)
      allPartyExpenditures = B.all_ (FEC._openFEC_DB_partyExpenditure FEC.openFEC_DB)
      districtM' = if office /= FEC.House then Just 0 else districtM -- the 0 default here is bad.  I should throw an error...
      stateFilter c = maybe (B.val_ True) (\state -> (FEC._candidate_state c B.==. B.val_ state)) stateM
      districtFilter c = maybe (B.val_ True) (\district -> (FEC._candidate_district c B.==. B.val_ district)) districtM'
      candidatesInElection = B.filter_ (\c -> ((stateFilter c)
                                               B.&&. (FEC._candidate_office c B.==. B.val_ office)
                                               B.&&. (districtFilter c))) $ allCandidates
      orderedForecasts = B.orderBy_ (\f -> (B.asc_ (FEC._forecast538_candidate_name f), B.asc_ (FEC._forecast538_forecast_date f))) $ allForecasts
      aggregatedDisbursements = B.aggregate_ (\(id, date, amount) -> (B.group_ id, B.group_ date, B.sum_ amount)) $ do
        d <- allDisbursements
        pure (FEC._disbursement_candidate_id d, FEC._disbursement_date d, FEC._disbursement_amount_adj d)
      aggregatedIndExpenditures = B.aggregate_ (\(id, date, supportOpposeFlag, amount) -> (B.group_ id, B.group_ date, B.group_ supportOpposeFlag, B.sum_ amount)) $ do
        ie <- allIndExpenditures
        pure (FEC._indExpenditure_candidate_id ie, FEC._indExpenditure_date ie, FEC._indExpenditure_support_oppose_indicator ie, FEC._indExpenditure_amount ie)
      aggregatedIESupport = B.filter_ (\x -> sel3 x B.==. B.val_ FEC.Support) $  aggregatedIndExpenditures
      aggregatedIEOppose = B.filter_ (\x -> sel3 x B.==. B.val_ FEC.Oppose) $  aggregatedIndExpenditures
      aggregatedPartyExpenditures = B.aggregate_ (\(id, date, amount) -> (B.group_ id, B.group_ date, B.sum_ amount)) $ do
        pe <- allPartyExpenditures
        pure (FEC._partyExpenditure_candidate_id pe, FEC._partyExpenditure_date pe, FEC._partyExpenditure_amount pe)

  forecasts' <- B.runBeamSqlite dbConn $ B.runSelectReturningList $ B.select $ do
    candidate <- candidatesInElection
    forecast <- orderedForecasts
    disbursement <- B.leftJoin_ aggregatedDisbursements (\(id,date,_) -> (id `B.references_` candidate) B.&&. (date B.==. forecast ^. FEC.forecast538_forecast_date))
    indSupport <- B.leftJoin_ aggregatedIESupport (\(id,date,_,_) -> (id `B.references_` candidate) B.&&. (date B.==. forecast ^. FEC.forecast538_forecast_date))
    indOppose <- B.leftJoin_ aggregatedIEOppose (\(id,date,_,_) -> (id `B.references_` candidate) B.&&. (date B.==. forecast ^. FEC.forecast538_forecast_date))
    partyExpenditures <- B.leftJoin_ aggregatedPartyExpenditures (\(id,date,_) -> (id `B.references_` candidate) B.&&. (date B.==. forecast ^. FEC.forecast538_forecast_date))
    B.guard_ ((FEC._forecast538_candidate_id forecast `B.references_` candidate)
              B.&&. (forecast ^. FEC.forecast538_model B.==. B.val_ "deluxe"))
    pure ( (candidate ^. FEC.candidate_id --forecast ^. FEC.forecast538_candidate_id
         , forecast ^. FEC.forecast538_candidate_name)
         , forecast ^. FEC.forecast538_forecast_date
         , forecast ^. FEC.forecast538_winP
         , forecast ^. FEC.forecast538_voteshare
         , sel3 disbursement
         , sel4 indSupport
         , sel4 indOppose
         , sel3 partyExpenditures)
  let g = maybe 0 (maybe 0 id)
  return $ fmap (\((x0, x1), x2, x3, x4, x5, x6, x7, x8) -> (x0, x1, x2, x3, x4, g x5, g x6, g x7, g x8)) forecasts'

allHouseCSV :: IO ()
allHouseCSV = do
  let tupleToCSV (id,n,d,wp,vs,db,is,io,pe)
        = id <> ","
          <> n <> ","
          <> T.pack (show $ localDay d) <> ","
          <> T.pack (TP.printf "%.2g" wp) <> ","
          <> T.pack (TP.printf "%.2g" vs) <> ","
          <> T.pack (TP.printf "%.0g" db) <> ","
          <> T.pack (TP.printf "%.0g" is) <> ","
          <> T.pack (TP.printf "%.0g" io) <> ","
          <> T.pack (TP.printf "%.0g" pe)

  dbConn <- SL.open "/Users/adam/Google Drive/FEC.db"
  let header = "id,name,date,win_percentage,voteshare,disbursement,ind_support,ind_oppose,party_expenditures"
  rows <- spendingAndForecastByRace dbConn FEC.House Nothing Nothing -- this will be BIG
  handle <- Sys.openFile "/Users/adam/DataScience/FEC/BlueRipple/data/allSpending.csv" Sys.WriteMode
  T.hPutStrLn handle header
  mapM (T.hPutStrLn handle . tupleToCSV) rows
  Sys.hClose handle





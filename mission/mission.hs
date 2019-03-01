{-# LANGUAGE DataKinds                 #-}
{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE GADTs                     #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE TypeApplications          #-}
{-# LANGUAGE TypeOperators             #-}
{-# LANGUAGE QuasiQuotes               #-}
module Main where

import           Control.Lens                    ((^.))
import qualified Control.Foldl                   as FL
import           Control.Monad.IO.Class          (MonadIO(liftIO))
import qualified Control.Monad.Freer.Logger      as Log
import qualified Control.Monad.Freer             as FR
import qualified Control.Monad.Freer.PandocMonad as FR
import qualified Control.Monad.Freer.Pandoc      as P
import           Control.Monad.Freer.Random      (runRandomIOPureMT)
import           Control.Monad.Freer.Docs        (toNamedDocListWithM)
import           Data.Functor.Identity           (Identity(..))
import qualified Data.List                       as List
import qualified Data.Map                        as M
import           Data.Maybe                      (catMaybes)
import           Data.Proxy                      (Proxy(..))
import qualified Data.Text                       as T
import qualified Data.Text.IO                    as T
import qualified Data.Text.Lazy                  as TL
import qualified Data.Time.Calendar              as Time
import qualified Data.Vinyl                      as V
import qualified Data.Vinyl.Class.Method         as V
import qualified Frames                          as F
import           Frames                          ((:->),(<+>),(&:))
import qualified Frames.CSV                      as F
--import qualified Frames.InCore                   as F
import qualified Pipes                           as P
import qualified Pipes.Prelude                   as P
import qualified Statistics.Types               as S

import qualified Html.Blaze.Report            as H
import qualified Text.Pandoc.Report              as P
import qualified Text.Blaze.Html.Renderer.Text   as BH
import           Control.Monad.Freer.Html        (Blaze, blaze, blazeToText, blazeHtml)

import qualified Frames.ParseableTypes as FP
import qualified Frames.VegaLite as FV
import qualified Frames.Transform as FT
import qualified Frames.Aggregations as FA 
import qualified Frames.Regression as FR
import           Data.String.Here (here)

import           BlueRipple.Data.DataFrames

templateVars = M.fromList
  [
    ("lang", "English")
  , ("author", "Adam Conner-Sax")
  , ("pagetitle", "BlueRipple Adam Mission")
--  , ("tufte","True")
  ]

main :: IO ()
main = do
  let writeNamedHtml (P.NamedDoc n lt) = T.writeFile (T.unpack $ "mission/html/" <> n <> ".html") $ TL.toStrict lt
      writeAllHtml = fmap (const ()) . traverse writeNamedHtml
      pandocToBlaze = fmap BH.renderHtml . P.toBlazeDocument (Just "pandoc-templates/minWithVega-pandoc.html") templateVars P.mindocOptionsF
  let runAll = FR.runPandocAndLoggingToIO Log.logAll
--               . runRandomIOPureMT (pureMT 1)
               . toNamedDocListWithM pandocToBlaze
               . Log.wrapPrefix "Main" 
  eitherDocs <- runAll $ do --  P.pandocWriterToBlazeDocument (Just "pandoc-templates/minWithVega-pandoc.html") templateVars P.mindocOptionsF $ do
    -- load the data
    
    Log.log Log.Info "Creating data Producers from csv files..."    
    let parserOptions = F.defaultParser { F.quotingMode =  F.RFC4180Quoting ' ' }
        totalSpendingP :: F.MonadSafe m => P.Producer TotalSpending m ()
        totalSpendingP = F.readTableOpt parserOptions totalSpendingCSV
        totalSpendingBeforeP :: F.MonadSafe m => P.Producer TotalSpendingBefore m ()
        totalSpendingBeforeP = F.readTableOpt parserOptions totalSpendingBeforeCSV
        forecastAndSpendingP ::  F.MonadSafe m => P.Producer ForecastAndSpending m ()
        forecastAndSpendingP = F.readTableOpt parserOptions forecastAndSpendingCSV
        electionResultsP :: F.MonadSafe m => P.Producer ElectionResults m ()
        electionResultsP = F.readTableOpt parserOptions electionResultsCSV
        demographicsP :: F.MonadSafe m => P.Producer Demographics m ()
        demographicsP = F.readTableOpt parserOptions demographicsCSV
    Log.log Log.Info "Loading data into memory..."
    let reportRows f fn = Log.log Log.Diagnostic $ (T.pack $ show $ FL.fold FL.length f) <> " rows in " <> fn
    totalSpendingFrame <- liftIO $ F.inCoreAoS totalSpendingP
    reportRows totalSpendingFrame "totalSpending"
    totalSpendingBeforeFrame <- liftIO $ F.inCoreAoS totalSpendingBeforeP
    reportRows totalSpendingBeforeFrame "totalSpendingBefore"
    forecastAndSpendingFrame <- liftIO $ F.inCoreAoS forecastAndSpendingP
    reportRows forecastAndSpendingFrame "forecastAndSpending"
    electionResultsFrame <- liftIO $ F.inCoreAoS electionResultsP
    reportRows electionResultsFrame "electionResults"
    demographicsFrame <- liftIO $ F.inCoreAoS demographicsP
    reportRows demographicsFrame "demographics"
    Log.log Log.Info "Knitting..."
    P.newPandoc "mission" $ do
      P.addMarkDown spendingHistNotes
      totalSpendingHistograms totalSpendingFrame
      P.addMarkDown differentialSpendNotes
      spendVsChangeInVoteShare totalSpendingFrame forecastAndSpendingFrame electionResultsFrame
  case eitherDocs of
    Right namedDocs -> writeAllHtml namedDocs --T.writeFile "mission/html/mission.html" $ TL.toStrict  $ htmlAsText
    Left err -> putStrLn $ "pandoc error: " ++ show err


-- 
spendFor r = (r ^. disbursement) + (r ^. indSupport) + (realToFrac $ r ^. partyExpenditures)
--
--type CandidateTotalSpend = "candidate_total_spend" :-> Double
type RaceTotalFor = "race_total_for" :-> Double
type RaceTotalAgainst = "race_total_against" :-> Double
type RaceTotalCands = "race_total_cands" :-> Int
type CandidateDiffSpend  = "candidate_diff_spend" :-> Double
type ResultVsForecast = "result_vs_forecast" :-> Double

differentialSpendNotes
  = [here|
## Differential Spending and changes in the race
* Candidates, committees and parties spend money on behalf of candidates.  But Committees (Super-PACs) etc. can also spend money *against* candidates.  That makes it confusing to define exactly how much was spent on behalf of each candidate.  In the below, when analyzing differences in spending, we take the simplistic approach of assuming that money spend against a candidate is exactly like money spent for the opposing candidate(s).  If there is more than one opposing candidate, we divide it evenly among them.
* In order to look at races with very different overall levels of spending, we divide each candidates total spending by the total spent on the entire race by all candidates.  This number varies between 0 (none of the money spent in the race was for this candidate) to 1 (all of the money spent in the race was for this candidate.)
|]         

-- differential spend vs (result - 8/1 forecast)
spendVsChangeInVoteShare :: (MonadIO (FR.Eff effs), FR.Members '[Log.Logger, P.ToPandoc] effs, FR.PandocEffects effs)
  => F.Frame TotalSpending -> F.Frame ForecastAndSpending -> F.Frame ElectionResults -> FR.Eff effs ()
spendVsChangeInVoteShare totalSpendingFrame fcastAndSpendFrame eResultsFrame = Log.wrapPrefix "spendVsChangeInVoteShare" $ do
  -- create a Frame with each candidate, candidate total, race total, candidate differential
  -- use aggregateFs by aggregating by [StateAbbreviation,CongressionalDistrict] and then use extract to turn those records into the ones we want
  Log.log Log.Info "Transforming spending before, forecasts and spending during, and election results..."
  let firstForecastFrame = fmap (F.rcast @[CandidateId, StateAbbreviation, CongressionalDistrict, CandidateParty, Voteshare]) $
        F.filterFrame (\r -> r ^. date == FP.FrameDay (Time.fromGregorian 2018 08 01)) fcastAndSpendFrame
  Log.log Log.Info "Filtered forecasts for first voteshare forecast on 8/1/2018 to get first forecast by candidateId"
  
  let foldAllSpending = FA.aggregateFs (Proxy @'[CandidateId]) Identity addSpending (0 &: 0 &: 0 &: 0 &: V.RNil) Identity where
        addSpending = flip $ V.recAdd . F.rcast @[Disbursement, IndSupport, IndOppose, PartyExpenditures]
      spendingDuringFrame = FL.fold foldAllSpending fcastAndSpendFrame
  Log.log Log.Info "Summed spending during forecasting period to get spending 8/1/2018-election day by candidateId"
  
  let proxyRace = Proxy :: Proxy '[StateAbbreviation, CongressionalDistrict]
      spendAgainst r = (r ^. indOppose)      
      raceTotalsF = FL.Fold (\(n,for,against) r -> (n+1, for + spendFor r, against + spendAgainst r)) (0, 0, 0) id
--      addRaceTotal (_, for, against) = FT.recordSingleton @RaceTotalSpend $ realToFrac (for + against)
      raceTotalFrameF = FA.aggregateFs proxyRace Identity (flip (:)) [] extract where
        extract candsInRace =
          let (n , for, against) = FL.fold raceTotalsF candsInRace
              addF = FT.recordSingleton @RaceTotalFor for
              addA = FT.recordSingleton @RaceTotalAgainst against
              addN = FT.recordSingleton @RaceTotalCands n
              addAll _ = addF <+> addA <+> addN
          in fmap (F.rcast @[CandidateId, RaceTotalFor, RaceTotalAgainst, RaceTotalCands] . FT.mutate addAll) candsInRace
      raceTotalFrame = FL.fold raceTotalFrameF totalSpendingFrame 
  Log.log Log.Info "aggregated by race (state and district) to get total spending by race and then attached that to each candidateId"
  -- all ur joins belong to us
  let selectResults = fmap (F.rcast @[CandidateId, FinalVoteshare])
      fRFrame = F.toFrame $ catMaybes $ fmap F.recMaybe $ F.leftJoin @'[CandidateId] firstForecastFrame (selectResults eResultsFrame)
      fRTFrame = F.toFrame $ catMaybes $ fmap F.recMaybe $ F.leftJoin @'[CandidateId] fRFrame raceTotalFrame
      fRTDFrame = F.toFrame $ catMaybes $ fmap F.recMaybe $ F.leftJoin @'[CandidateId] fRTFrame spendingDuringFrame
  Log.log Log.Info $ "Did all the joins. Final frame has " <> (T.pack $ show $ FL.fold FL.length fRTDFrame) <> " rows."
  let addCandDiff r = FT.recordSingleton @CandidateDiffSpend $ candSpend/totalSpend where
        allCandsFor = F.rgetField @RaceTotalFor r
        allCandsAgainst = F.rgetField @RaceTotalAgainst r
        nCands = F.rgetField @RaceTotalCands r
        candFor = spendFor r
        candAgainst = spendAgainst r
        candSpend = candFor + (allCandsAgainst - realToFrac candAgainst)/(realToFrac nCands - 1)
        totalSpend = allCandsFor + allCandsAgainst
      addResultVsForecast r = FT.recordSingleton @ResultVsForecast $ (finalVS - forecastVS) where
        forecastVS = F.rgetField @Voteshare r
        finalVS = F.rgetField @FinalVoteshare r
      addAll r = addCandDiff r <+> addResultVsForecast r
      candidatesInContestedRacesFrame = F.filterFrame ((> 1) . F.rgetField @RaceTotalCands) $ fmap (FT.mutate addAll) fRTDFrame
  liftIO $ F.writeCSV "data/allTogetherFrame.csv" candidatesInContestedRacesFrame
  let filterCloseAndCheap r =
        let vs = F.rgetField @Voteshare r
            rtf = F.rgetField @RaceTotalFor r
            rta = F.rgetField @RaceTotalAgainst r
            party = F.rgetField @CandidateParty r
        in (vs > 30) && (vs < 70) && ((rtf + rta) < 1000000) -- && (party == "Republican")
      toAnalyzeFrame = F.filterFrame filterCloseAndCheap candidatesInContestedRacesFrame
  Log.log Log.Info $ "Added normalized differential spend and filtered to cheap and close races. " <> (T.pack $ show (FL.fold FL.length toAnalyzeFrame)) <> " rows left."
  P.addBlaze $ H.placeVisualization "DiffSpendHistogram"  $
    FV.singleHistogram @CandidateDiffSpend "Distribution of Differential Spending" (Just "# Candidates") 10 Nothing Nothing True toAnalyzeFrame
  diffSpendVsdiffVs <- FR.ordinaryLeastSquares @ResultVsForecast @True @'[CandidateDiffSpend] toAnalyzeFrame
  P.addBlaze $ FR.prettyPrintRegressionResultBlaze (\y _ -> "Explaining " <> y) diffSpendVsdiffVs S.cl95 
  P.addBlaze $ H.placeVisualization ("dsVsdvsfit") $ FV.frameScatterWithFit "Differential Spending vs Change in Voteshare" (Just "regression") diffSpendVsdiffVs S.cl95 toAnalyzeFrame
  P.addBlaze $ H.placeVisualization ("dsVsdvsRegresssionCoeffs") $ FV.regressionCoefficientPlot "Parameters" ["intercept","differential spend"] (FR.regressionResult diffSpendVsdiffVs) S.cl95
-- Spending histograms
spendingHistNotes
  = [here|
## Spending By Party in 2018 House Races
* The bar charts below summarize the distribution of per-candidate spending in 2018 house races.  The top chart shows all the candidates, the one below zooms in on candidates who spent less than $1,000,000 and the one below that zooms in further to candidates who spent less than $100,000.
* These were produced with data from the [FEC][FECMain], using the open-data [API][FECAPI].
* This chart includes candidate expenditures, independent expenditures in support of the candidate and party expenditures on behalf of the candidate.  It ignores independent expenditures *against* the candidate, which can be considerable in some races. See the [FEC web-site for more information][FECDefs] about all of these categories.

[FECMain]: <https://www.fec.gov/>
[FECAPI]: <https://api.open.fec.gov/developers/>
[FECDefs]: <https://www.fec.gov/data/browse-data/?tab=spending>
|]

type AllSpending = "all_spending" F.:-> Double
sumSpending r =
  let db = realToFrac $ F.rgetField @Disbursement r
      is = realToFrac $ F.rgetField @IndSupport r      
      pe = realToFrac $ F.rgetField @PartyExpenditures r
  in FT.recordSingleton @AllSpending (db + is + pe)
     
totalSpendingHistograms :: (FR.Members '[Log.Logger, P.ToPandoc] effs, FR.PandocEffects effs)
  => F.Frame TotalSpending -> FR.Eff effs ()
totalSpendingHistograms tsFrame = do
  let frameWithSum = F.filterFrame ((>0). F.rgetField @AllSpending) $ fmap (FT.mutate sumSpending) tsFrame
      mergeOtherParties :: F.Record '[CandidateParty] -> F.Record '[CandidateParty]
      mergeOtherParties r =
        let p = F.rgetField @CandidateParty r
            np = case p of
              "Republican" -> "Republican"
              "Democrat" -> "Democrat"
              _ -> "_AllOthers"
        in FT.recordSingleton @CandidateParty np
      frameWithMergedOtherParties = fmap (FT.transform mergeOtherParties) frameWithSum 
--  Log.log Log.Diagnostic $ T.pack $ show $ fmap (show . F.rcast @[CandidateId, AllSpending]) $ FL.fold FL.list frameWithSum
  P.addBlaze $ H.placeVisualization "SpendingHistogramByPartyAll"  $
    FV.multiHistogram @AllSpending @CandidateParty "Distribution of Spending By Party" (Just "# Candidates") 10 (Just 0) (Just 1e7) True FV.AdjacentBar frameWithMergedOtherParties
  P.addBlaze $ H.placeVisualization "SpendingHistogramByParty1MM"  $
    FV.multiHistogram @AllSpending @CandidateParty "Distribution of Spending By Party (< $1,000,000)" (Just "# Candidates") 10 (Just 0) (Just 1e6) False FV.AdjacentBar frameWithMergedOtherParties
  P.addBlaze $ H.placeVisualization "SpendingHistogramByParty1M"  $
    FV.multiHistogram @AllSpending @CandidateParty "Distribution of Spending By Party (< $100,000)" (Just "# Candidates") 10 (Just 0) (Just 1e5) False FV.AdjacentBar frameWithMergedOtherParties
  

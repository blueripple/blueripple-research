{-# LANGUAGE DataKinds                 #-}
{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE GADTs                     #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE TypeApplications          #-}
{-# LANGUAGE TypeOperators             #-}
{-# LANGUAGE QuasiQuotes               #-}
{-# LANGUAGE AllowAmbiguousTypes       #-}
{-# LANGUAGE PolyKinds                 #-}
{-# LANGUAGE PartialTypeSignatures     #-}
module Main where

import           Control.Lens                    ((^.))
import qualified Control.Foldl                   as FL
import           Control.Monad.IO.Class          (MonadIO(liftIO))
import qualified Data.Map                        as M
import           Data.Maybe                      (catMaybes)
import qualified Data.Monoid                     as MO
import           Data.Proxy                      (Proxy(..))
import qualified Data.Text                       as T
import qualified Data.Text.IO                    as T
import qualified Data.Text.Lazy                  as TL
import qualified Data.Time.Calendar              as Time
import qualified Data.Vinyl                      as V
import qualified Data.Vinyl.TypeLevel            as V
import qualified Frames                          as F
import           Frames                          ((:->),(<+>),(&:))
import qualified Frames.CSV                      as F
import qualified Frames.InCore                   as F hiding (inCoreAoS)
import qualified Pipes                           as P
import qualified Pipes.Prelude                   as P
import qualified Statistics.Types               as S

import qualified Text.Blaze.Html.Renderer.Text  as BH

import qualified Frames.ParseableTypes as FP
import qualified Frames.VegaLite as FV
import qualified Frames.Transform as FT
import qualified Frames.Folds as FF 
import qualified Frames.Regression as FR
import qualified Frames.MapReduce as MR
import qualified Frames.Table as Table

import qualified Frames.Visualization.VegaLite.Data        as FHV
import qualified Graphics.Vega.VegaLite.Configuration as VLC
import qualified Frames.Visualization.VegaLite.Histogram as FHV

import qualified Knit.Report                    as K
import qualified Knit.Report.Other.Blaze        as KB

import           Data.String.Here (here)

import           BlueRipple.Data.DataFrames

templateVars = M.fromList
  [
    ("lang", "English")
  , ("author", "Adam Conner-Sax")
  , ("pagetitle", "BlueRipple Adam Mission")
--  , ("tufte","True")
  ]

loadCSVToFrame :: forall rs effs. ( MonadIO (K.Sem effs)
                                  , K.LogWithPrefixesLE effs
                                  , F.ReadRec rs
                                  , F.RecVec rs
                                  , V.RMap rs)
               => F.ParserOptions -> FilePath -> (F.Record rs -> Bool) -> K.Sem effs (F.FrameRec rs)
loadCSVToFrame po fp filterF = do
  let --producer :: F.MonadSafe m => P.Producer rs m ()
      producer = F.readTableOpt po fp P.>-> P.filter filterF 
  frame <- liftIO $ F.inCoreAoS producer
  let reportRows :: Foldable f => f x -> FilePath -> K.Sem effs ()
      reportRows f fn = K.logLE K.Diagnostic $ T.pack (show $ FL.fold FL.length f) <> " rows in " <> T.pack fn
  reportRows frame fp
  return frame
  
main :: IO ()
main = do
  let template = K.FromIncludedTemplateDir "mindoc-pandoc-KH.html"
  pandocWriterConfig <- K.mkPandocWriterConfig template
                                               templateVars
                                               K.mindocOptionsF
  let knitConfig = K.defaultKnitConfig
        { K.outerLogPrefix = Just "mission.Main"
        , K.logIf = K.logAll
        , K.pandocWriterConfig = pandocWriterConfig
        }
  eitherDocs <- K.knitHtmls knitConfig $ do
    -- load the data   
    let parserOptions = F.defaultParser { F.quotingMode =  F.RFC4180Quoting ' ' }
    K.logLE K.Info "Loading data..."
    totalSpendingFrame :: F.Frame TotalSpending <- loadCSVToFrame parserOptions totalSpendingCSV (const True)
    totalSpendingBeforeFrame :: F.Frame TotalSpending <- loadCSVToFrame parserOptions totalSpendingBeforeCSV (const True)
    totalSpendingDuringFrame :: F.Frame TotalSpending <- loadCSVToFrame parserOptions totalSpendingDuringCSV (const True)
    forecastAndSpendingFrame :: F.Frame ForecastAndSpending <- loadCSVToFrame parserOptions forecastAndSpendingCSV (const True)
    electionResultsFrame :: F.Frame ElectionResults <- loadCSVToFrame parserOptions electionResultsCSV (const True)
    contextDemographicsFrame :: F.Frame ContextDemographics <- loadCSVToFrame parserOptions contextDemographicsCSV (const True)
    angryDemsFrame :: F.Frame AngryDems <- loadCSVToFrame parserOptions angryDemsCSV (const True)
--    reportRows angryDemsFrame "angryDems"
    K.logLE K.Info "Knitting..."
    K.newPandoc (K.PandocInfo "mission" M.empty) $ do
      totalSpendingHistograms totalSpendingFrame
      spendVsChangeInVoteShare totalSpendingDuringFrame totalSpendingFrame forecastAndSpendingFrame electionResultsFrame
      angryDemsAnalysis angryDemsFrame
  case eitherDocs of
    Right namedDocs ->   K.writeAllPandocResultsWithInfoAsHtml "mission/html" namedDocs --T.writeFile "mission/html/mission.html" $ TL.toStrict  $ htmlAsText
    Left err -> putStrLn $ "pandoc error: " ++ show err
--
{-
TODO
** Trends
** Registration (see Census)
* Repeat Runners
* local races?
* Ballot initiatives?
* GOTV
-}
--
angryDemsNotes :: T.Text
angryDemsNotes
  = [here|
## Angry Democrats Donations
* The [angry democrats post][AngryDemPost] generated a variety of donation size.
* As the histogram below shows, donations were mostly small with a few much larger ones.

[AngryDemPost]: <https://medium.com/@frank_s_david/angrydems-cc7e8caefe7b>
|]
  
angryDemsAnalysis :: K.KnitOne effs => F.Frame AngryDems -> K.Sem effs ()
angryDemsAnalysis angryDemsFrame = do
  -- aggregate by ReceiptID
  K.addMarkDown angryDemsNotes
  let byDonationFrame = FL.fold (MR.concatFold $ MR.mapReduceFold
                                 MR.noUnpack
                                 (MR.assignKeysAndData @'[ReceiptID] @'[Amount])
                                 (MR.foldAndAddKey (FF.foldAllMonoid @MO.Sum)))
                        angryDemsFrame
      vc = VLC.ViewConfig 800 400 50
  _ <- K.addHvega Nothing Nothing $
    FHV.singleHistogram @Amount "Angry Democrats Donations" (Just "# Donations") 10 VLC.Default False vc byDonationFrame
  _ <- K.addHvega Nothing Nothing $
    FHV.singleHistogram @Amount "Angry Democrats Donations (<$3000)" (Just "# Donations") 10 (VLC.GivenMinMax 0 3000) True vc byDonationFrame
  _ <- K.addHvega Nothing Nothing $
    FHV.singleHistogram @Amount "Angry Democrats Donations (<$200)" (Just "# Donations") 10 (VLC.GivenMinMax 0 200) True vc byDonationFrame
  return ()
  

-- 
spendFor r = (r ^. disbursement) + (r ^. indSupport) + realToFrac (r ^. partyExpenditures)
spendAgainst r = r ^. indOppose     
proxyRace = Proxy :: Proxy '[StateAbbreviation, CongressionalDistrict]

type RaceTotalFor = "race_total_for" :-> Double
type RaceTotalAgainst = "race_total_against" :-> Double
type RaceTotalCands = "race_total_cands" :-> Int
type CandidateDiffSpend  = "candidate_diff_spend" :-> Double
type ResultVsForecast = "result_vs_forecast" :-> Double

differentialSpendNotes :: T.Text
differentialSpendNotes
  = [here|
## Differential Spending and changes in the race
* Below we look at the spending of each candidate from 8/1/2018 (the earliest date for which we have any polling/forecast data) through election day 2018.  Since we are interested in whether that spending affects the outcome of a race, we pair the spending data with an estimated change in vote share, calculated using the [538][538_house] forecast on 8/1 and the actual election result.
* Candidates, committees and parties spend money on behalf of candidates.  But Committees (Super-PACs) etc. can also spend money *against* candidates.  That makes it hard to determine how much was spent on behalf of each candidate.  We take the simplistic approach of assuming that money spent against a candidate is exactly like money spent for the opposing candidate(s).  If there is more than one opposing candidate, we divide it evenly among them.
* We divide each candidates total spending by the total spent on the entire race by all candidates, considering % of spending by each candidate rather than the actual dollars spent.  We do this so we can compare races with very different levels of spending.  
* We visualize this data a couple of ways.  First, a histogram of the differential spending and then a scatter-plot of differential spending vs change in vote-share, along with a simple regression. If anything, this regression shows that extra spending has a slightly negative correlation with changes in vote-share; in line with [this][538_electionMoney] 538 story about how money affects elections.

[538_house]:<https://projects.fivethirtyeight.com/2018-midterm-election-forecast/house/>
[538_electionMoney]: <https://fivethirtyeight.com/features/money-and-elections-a-complicated-love-story/>
|]         

setF :: V.KnownField t => V.Snd t -> V.ElField t
setF = V.Field
  
-- differential spend vs (result - 8/1 forecast)
spendVsChangeInVoteShare :: K.KnitOne effs
  => F.Frame TotalSpending -> F.Frame TotalSpending -> F.Frame ForecastAndSpending -> F.Frame ElectionResults -> K.Sem effs ()
spendVsChangeInVoteShare spendingDuringFrame totalSpendingFrame fcastAndSpendFrame eResultsFrame = K.wrapPrefix "spendVsChangeInVoteShare" $ do
  -- create a Frame with each candidate, candidate total, race total, candidate differential
  -- use aggregateFs by aggregating by [StateAbbreviation,CongressionalDistrict] and then use extract to turn those records into the ones we want
  K.logLE K.Info "Transforming spending before, forecasts and spending during, and election results..."
  let firstForecastFrame = fmap (F.rcast @[CandidateId, StateAbbreviation, CongressionalDistrict, CandidateParty, Voteshare]) $
        F.filterFrame (\r -> r ^. date == FP.FrameDay (Time.fromGregorian 2018 08 01)) fcastAndSpendFrame
  K.logLE K.Info "Filtered forecasts for first voteshare forecast on 8/1/2018 to get first forecast by candidateId"
  let proxyRace = Proxy :: Proxy '[StateAbbreviation, CongressionalDistrict]
      spendAgainst r = (r ^. indOppose)
      raceTotalsF = FF.sequenceRecFold (FF.recFieldF @RaceTotalCands FL.length id 
                                        V.:& FF.recFieldF @RaceTotalFor FL.sum spendFor
                                        V.:& FF.recFieldF @RaceTotalAgainst FL.sum spendAgainst 
                                        V.:& V.RNil)             
      raceTotalFrameF = (MR.concatFold $ MR.mapReduceFold
                          MR.noUnpack
                          (MR.assignKeys @[StateAbbreviation, CongressionalDistrict])
                          (MR.ReduceFold $ \_ -> let f t c = fmap (V.rappend t) c in f <$> raceTotalsF <*> FL.list ))

      raceTotalFrame = F.toFrame $ fmap (F.rcast @[CandidateId,RaceTotalFor,RaceTotalAgainst,RaceTotalCands]) $ FL.fold raceTotalFrameF totalSpendingFrame

      retypeCols = FT.retypeColumn @RaceTotalFor @("race_during_for" :-> Double)
                   . FT.retypeColumn @RaceTotalAgainst @("race_during_against" :-> Double)
      raceDuringFrame = F.toFrame $ fmap (retypeCols . F.rcast @[CandidateId,RaceTotalFor,RaceTotalAgainst,Disbursement,IndSupport,IndOppose,PartyExpenditures])
                        $ FL.fold raceTotalFrameF spendingDuringFrame
  K.logLE K.Info "aggregated by race (state and district) to get total spending by race and then attached that to each candidateId" 
  -- all ur joins r belong to us
  let selectResults = fmap (F.rcast @[CandidateId, FinalVoteshare])
      fRFrame = F.toFrame $ catMaybes $ fmap F.recMaybe $ F.leftJoin @'[CandidateId] firstForecastFrame (selectResults eResultsFrame)
      fRTFrame = F.toFrame $ catMaybes $ fmap F.recMaybe $ F.leftJoin @'[CandidateId] fRFrame raceTotalFrame      
      fRTBFrame = F.toFrame $ catMaybes $ fmap F.recMaybe $ F.leftJoin @'[CandidateId] fRTFrame raceDuringFrame
  K.logLE K.Info $ "Did all the joins. Final frame has " <> T.pack (show $ FL.fold FL.length fRTBFrame) <> " rows."
  let addCandDiff r = FT.recordSingleton @CandidateDiffSpend $ 100*candSpend/totalSpend where
        allCandsFor = F.rgetField @RaceTotalFor r
        allCandsAgainst = F.rgetField @RaceTotalAgainst r
        allCandsAgainstDuring = F.rgetField @("race_during_against" :-> Double) r
        allCandsForDuring = F.rgetField @("race_during_for" :-> Double) r
        nCands = F.rgetField @RaceTotalCands r
        candForDuring = spendFor r
        candAgainstDuring = spendAgainst r
        candSpend = candForDuring + (allCandsAgainstDuring - realToFrac candAgainstDuring)/(realToFrac nCands - 1)
        totalSpend = allCandsForDuring + allCandsAgainstDuring
      addResultVsForecast r = FT.recordSingleton @ResultVsForecast $ (finalVS - forecastVS) where
        forecastVS = F.rgetField @Voteshare r
        finalVS = F.rgetField @FinalVoteshare r
      addAll r = addCandDiff r <+> addResultVsForecast r
      contestedRacesFrame = F.filterFrame ((==2) . F.rgetField @RaceTotalCands) $ fmap (FT.mutate addAll) fRTBFrame
  K.logLE K.Info $ "Added normalized differential spend and filtered to cheap and close races. " <> (T.pack $ show (FL.fold FL.length contestedRacesFrame)) <> " rows left."
  K.addMarkDown differentialSpendNotes
  K.addMarkDown "### All Races With 2 or more candidates"
  _ <- K.addHvega Nothing Nothing  $
    FHV.singleHistogram @CandidateDiffSpend "Distribution of Differential Spending (8/1/2018-11/6/2018)" (Just "# Candidates") 10 VLC.Default True (VLC.ViewConfig 800 400 50) contestedRacesFrame
  diffSpendVsdiffVs <- FR.ordinaryLeastSquares @_ @ResultVsForecast @True @'[CandidateDiffSpend] contestedRacesFrame
  _ <- K.addHvega Nothing Nothing $ FV.frameScatterWithFit "Differential Spending vs Change in Voteshare (8/1/208-11/6/2018)" (Just "regression") diffSpendVsdiffVs S.cl95 contestedRacesFrame
  K.addBlaze $ FR.prettyPrintRegressionResultBlaze (\y _ -> "Regression Details") diffSpendVsdiffVs S.cl95 
  _ <- K.addHvega Nothing Nothing $ FV.regressionCoefficientPlot "Parameters" ["intercept","differential spend"] (FR.regressionResult diffSpendVsdiffVs) S.cl95
  let filterCloseAndCheap r =
        let vs = F.rgetField @Voteshare r
            rtf = F.rgetField @RaceTotalFor r
            rta = F.rgetField @RaceTotalAgainst r
            party = F.rgetField @CandidateParty r
        in (vs > 40) && (vs < 60) && ((rtf + rta) < 2000000) -- && (party == "Republican")
      cheapAndCloseFrame = F.filterFrame filterCloseAndCheap  contestedRacesFrame
  K.addMarkDown "### All Races With 2 or more candidates, total spending below $2,000,000, and first forecast closer than 60/40."
  _ <- K.addHvega Nothing Nothing  $
    FHV.singleHistogram @CandidateDiffSpend "Distribution of Differential Spending (8/1/2018-11/6/2018)" (Just "# Candidates") 10 VLC.Default True (VLC.ViewConfig 800 400 50) cheapAndCloseFrame
  diffSpendVsdiffVs <- FR.ordinaryLeastSquares @_ @ResultVsForecast @True @'[CandidateDiffSpend] cheapAndCloseFrame
  _ <- K.addHvega Nothing Nothing $ FV.frameScatterWithFit "Differential Spending vs Change in Voteshare (8/1/208-11/6/2018)" (Just "regression") diffSpendVsdiffVs S.cl95 cheapAndCloseFrame
  K.addBlaze $ FR.prettyPrintRegressionResultBlaze (\y _ -> "Regression Details") diffSpendVsdiffVs S.cl95 
  _ <- K.addHvega Nothing Nothing $ FV.regressionCoefficientPlot "Parameters" ["intercept","differential spend"] (FR.regressionResult diffSpendVsdiffVs) S.cl95
  K.addMarkDown "### All Races With 2 or more candidates, total spending below $2,000,000, and first forecast closer than 60/40."
  
  

-- Spending histograms
spendingHistNotes :: T.Text
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
     
totalSpendingHistograms :: K.KnitOne effs
  => F.Frame TotalSpending -> K.Sem effs ()
totalSpendingHistograms tsFrame = do
  K.addMarkDown spendingHistNotes
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
      vc = VLC.ViewConfig 800 400 50
--  K.log K.Diagnostic $ T.pack $ show $ fmap (show . F.rcast @[CandidateId, AllSpending]) $ FL.fold FL.list frameWithSum
  _ <- K.addHvega Nothing Nothing  $
    FHV.multiHistogram @AllSpending @CandidateParty "Distribution of Spending By Party" (Just "# Candidates") 10 (VLC.GivenMinMax 0 1e7) True FHV.AdjacentBar vc frameWithMergedOtherParties
  _ <- K.addHvega Nothing Nothing  $
    FHV.multiHistogram @AllSpending @CandidateParty "Distribution of Spending By Party (< $1,000,000)" (Just "# Candidates") 10 (VLC.GivenMinMax 0 1e6) False FHV.AdjacentBar vc frameWithMergedOtherParties
  _ <- K.addHvega Nothing Nothing  $
    FHV.multiHistogram @AllSpending @CandidateParty "Distribution of Spending By Party (< $100,000)" (Just "# Candidates") 10 (VLC.GivenMinMax 0 1e5) False FHV.AdjacentBar vc frameWithMergedOtherParties
  return ()

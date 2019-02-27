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

import qualified Control.Foldl                   as FL
import           Control.Monad.IO.Class          (MonadIO(liftIO))
import qualified Control.Monad.Freer.Logger      as Log
import qualified Control.Monad.Freer             as FR
import qualified Control.Monad.Freer.PandocMonad as FR
import qualified Control.Monad.Freer.Pandoc      as P
import           Control.Monad.Freer.Random      (runRandomIOPureMT)
import           Control.Monad.Freer.Docs        (toNamedDocListWithM)
import qualified Data.List                       as List
import qualified Data.Map                        as M
import qualified Data.Text                       as T
import qualified Data.Text.IO                    as T
import qualified Data.Text.Lazy                  as TL
import qualified Data.Time.Calendar              as Time
import qualified Frames                          as F
import qualified Frames.CSV                      as F
--import qualified Frames.InCore                   as F
import qualified Pipes                           as P
import qualified Pipes.Prelude                   as P

import qualified Html.Blaze.Report            as H
import qualified Text.Pandoc.Report              as P
import qualified Text.Blaze.Html.Renderer.Text   as BH
import           Control.Monad.Freer.Html        (Blaze, blaze, blazeToText, blazeHtml)

import qualified Frames.ParseableTypes as FP
import qualified Frames.VegaLite as FV
import qualified Frames.Transform as FT
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
      totalSpendingHistogram totalSpendingFrame
  case eitherDocs of
    Right namedDocs -> writeAllHtml namedDocs --T.writeFile "mission/html/mission.html" $ TL.toStrict  $ htmlAsText
    Left err -> putStrLn $ "pandoc error: " ++ show err

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

     
totalSpendingHistogram :: (FR.Members '[Log.Logger, P.ToPandoc] effs, FR.PandocEffects effs)
  => F.Frame TotalSpending -> FR.Eff effs ()
totalSpendingHistogram tsFrame = do
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
  

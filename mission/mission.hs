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
import qualified Text.Pandoc.Report              as P
import qualified Text.Blaze.Html.Renderer.Text   as BH
import           Control.Monad.Freer.Html        (Blaze, blaze, blazeToText, blazeHtml)

import qualified Frames.ParseableTypes as FP

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
  let runAll = FR.runPandocAndLoggingToIO Log.logAll . Log.wrapPrefix "Main" . fmap BH.renderHtml      
  htmlAsTextE <- runAll $ P.pandocWriterToBlazeDocument (Just "pandoc-templates/minWithVega-pandoc.html") templateVars P.mindocOptionsF $ do
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
    return ()
  case htmlAsTextE of
    Right htmlAsText -> T.writeFile "mission/html/mission.html" $ TL.toStrict  $ htmlAsText
    Left err -> putStrLn $ "pandoc error: " ++ show err

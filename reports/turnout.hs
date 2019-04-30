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
module Main where

import           Control.Lens                   ( (^.) )
import qualified Control.Foldl                 as FL
import           Control.Monad.IO.Class         ( MonadIO(liftIO) )
import qualified Data.Map                      as M
import           Data.Maybe                     ( catMaybes )
import qualified Data.Monoid                   as MO
import           Data.Proxy                     ( Proxy(..) )
import qualified Data.Text                     as T
import qualified Data.Text.IO                  as T
import qualified Data.Text.Lazy                as TL
import qualified Data.Time.Calendar            as Time
import qualified Data.Vinyl                    as V
import qualified Data.Vinyl.TypeLevel          as V
import qualified Frames                        as F
import           Frames                         ( (:->)
                                                , (<+>)
                                                , (&:)
                                                )
import qualified Frames.CSV                    as F
import qualified Frames.InCore                 as F
                                         hiding ( inCoreAoS )
import qualified Pipes                         as P
import qualified Pipes.Prelude                 as P
import qualified Statistics.Types              as S

import qualified Text.Blaze.Html.Renderer.Text as BH

import qualified Frames.ParseableTypes         as FP
import qualified Frames.VegaLite               as FV
import qualified Frames.Transform              as FT
import qualified Frames.Folds                  as FF
import qualified Frames.Regression             as FR
import qualified Frames.MapReduce              as MR
import qualified Frames.Table                  as Table

import qualified Knit.Report                   as K
import qualified Knit.Report.Other.Blaze       as KB
import qualified Knit.Effect.Pandoc            as K
                                                ( newPandoc
                                                , NamedDoc(..)
                                                )

import           Data.String.Here               ( here )

import           BlueRipple.Data.DataFrames

templateVars = M.fromList
  [ ("lang"     , "English")
  , ("author"   , "Adam Conner-Sax")
  , ("pagetitle", "Turnout Model & Predictions")
--  , ("tufte","True")
  ]

loadCSVToFrame
  :: forall rs effs
   . ( MonadIO (K.Sem effs)
     , K.LogWithPrefixesLE effs
     , F.ReadRec rs
     , F.RecVec rs
     , V.RMap rs
     )
  => F.ParserOptions
  -> FilePath
  -> (F.Record rs -> Bool)
  -> K.Sem effs (F.FrameRec rs)
loadCSVToFrame po fp filterF = do
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

main :: IO ()
main = do
  let writeNamedHtml (K.NamedDoc n lt) =
        T.writeFile (T.unpack $ "reports/html/" <> n <> ".html")
          $ TL.toStrict lt
      writeAllHtml       = fmap (const ()) . traverse writeNamedHtml
      pandocWriterConfig = K.PandocWriterConfig
        (Just "pandoc-templates/minWithVega-pandoc.html")
        templateVars
        K.mindocOptionsF
  eitherDocs <-
    K.knitHtmls (Just "turnout.Main") K.logAll pandocWriterConfig $ do
    -- load the data   
      let parserOptions =
            F.defaultParser { F.quotingMode = F.RFC4180Quoting ' ' }
      K.logLE K.Info "Loading data..."
  --    totalSpendingFrame :: F.Frame TotalSpending <- loadCSVToFrame parserOptions totalSpendingCSV (const True)
  --    totalSpendingBeforeFrame :: F.Frame TotalSpending <- loadCSVToFrame parserOptions totalSpendingBeforeCSV (const True)
  --    totalSpendingDuringFrame :: F.Frame TotalSpending <- loadCSVToFrame parserOptions totalSpendingDuringCSV (const True)
  --    forecastAndSpendingFrame :: F.Frame ForecastAndSpending <- loadCSVToFrame parserOptions forecastAndSpendingCSV (const True)
  --    electionResultsFrame :: F.Frame ElectionResults <- loadCSVToFrame parserOptions electionResultsCSV (const True)
  --    angryDemsFrame :: F.Frame AngryDems <- loadCSVToFrame parserOptions angryDemsCSV (const True)
      contextDemographicsFrame :: F.Frame ContextDemographics <- loadCSVToFrame
        parserOptions
        contextDemographicsCSV
        (const True)
      identityDemographicsFrame :: F.Frame IdentityDemographics <-
        loadCSVToFrame parserOptions identityDemographicsCSV (const True)
      houseElectionsFrame :: F.Frame HouseElections <- loadCSVToFrame
        parserOptions
        houseElectionsCSV
        (const True)
      turnout2016Frame :: F.Frame Turnout <- loadCSVToFrame parserOptions
                                                            turnout2016CSV
                                                            (const True)
      K.logLE K.Info "Knitting..."
      K.newPandoc "turnout" $ do
        turnoutModel identityDemographicsFrame
                     houseElectionsFrame
                     turnout2016Frame
  case eitherDocs of
    Right namedDocs -> writeAllHtml namedDocs --T.writeFile "mission/html/mission.html" $ TL.toStrict  $ htmlAsText
    Left  err       -> putStrLn $ "pandoc error: " ++ show err

turnoutModel
  :: (K.Member K.ToPandoc r, K.PandocEffects r)
  => F.Frame IdentityDemographics
  -> F.Frame HouseElections
  -> F.Frame Turnout
  -> K.Sem r ()
turnoutModel identityDFrame houseElexFrame turnout2016Frame = do
  -- rename some cols in houseElex
  let houseElexF = fmap
        ( FT.retypeColumn @StateFips @StateFIPS
        . FT.retypeColumn @StatePo @StateAbbreviation
        )
        houseElexFrame
  K.logLE K.Diagnostic $ T.pack $ show (take 5 $ FL.fold FL.list houseElexF)
  K.logLE K.Diagnostic $ T.pack $ show (FL.fold FL.list turnout2016Frame)

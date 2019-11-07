{-# LANGUAGE DataKinds                 #-}
{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE GADTs                     #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE PolyKinds                 #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE TypeApplications          #-}
{-# LANGUAGE TypeOperators             #-}
{-# LANGUAGE QuasiQuotes               #-}
--{-# LANGUAGE AllowAmbiguousTypes       #-}

import qualified Control.Foldl                 as FL

import qualified Data.List                     as L
import qualified Data.Map                      as M
import qualified Data.Vector                   as VB
import qualified Data.Text                     as T

import qualified Text.Printf                   as PF

import qualified Data.Vinyl                    as V
import qualified Frames                        as F
import qualified Frames.CSV                    as F

import qualified Pipes                         as P
import qualified Pipes.Prelude                 as P

import qualified Numeric.LinearAlgebra         as LA

import qualified Frames.Visualization.VegaLite.Data
                                               as FV
import qualified Frames.Visualization.VegaLite.StackedArea
                                               as FV
import qualified Frames.Visualization.VegaLite.LineVsTime
                                               as FV
import qualified Frames.Visualization.VegaLite.ParameterPlots
                                               as FV
import qualified Frames.Visualization.VegaLite.Correlation
                                               as FV

import qualified Frames.Transform              as FT
import qualified Frames.Folds                  as FF
import qualified Frames.MapReduce              as MR
import qualified Frames.Enumerations           as FE
import qualified Frames.Utils                  as FU

import qualified Knit.Report                   as K
import           Polysemy.Error                 ( Error )

import           Data.String.Here               ( here )

import           BlueRipple.Data.DataFrames
import           MRP.CCES
import qualified BlueRipple.Model.TurnoutAdjustment
                                               as TA

templateVars =
  M.fromList [("lang", "English")
--  , ("author"   , "Adam Conner-Sax & Frank David")
--  , ("pagetitle", "Preference Model & Predictions")
--  , ("tufte","True")
                                 ]

{- TODO
1. Why are rows still being dropped?  Which col is missing?  In general, write a diagnostic for showing what is missing...
Some answers:
The boring:  CountyFIPS.  I don't want this anyway.  Dropped
The weird:  Missing voted_rep_party.  These are non-voters or voters who didn't vote in house race.  A bit more than 1/3 of
survey responses.  Maybe that's not weird??
-}

main :: IO ()
main = do
  let template = K.FromIncludedTemplateDir "mindoc-pandoc-KH.html"
--  let template = K.FullySpecifiedTemplatePath "pandoc-templates/minWithVega-pandoc.html"
  pandocWriterConfig <- K.mkPandocWriterConfig template
                                               templateVars
                                               K.mindocOptionsF
  eitherDocs <-
    K.knitHtmls (Just "MRP_Basics.Main") K.logAll pandocWriterConfig $ do
      K.logLE K.Info "Loading data..."
      let csvParserOptions =
            F.defaultParser { F.quotingMode = F.RFC4180Quoting ' ' }
          tsvParserOptions = csvParserOptions { F.columnSeparator = "," }
          preFilterYears   = FU.filterOnMaybeField @Year (`L.elem` [2016])
      ccesMaybeRecs <- loadToMaybeRecs @CCES_MRP_Raw @(F.RecordColumns CCES)
        tsvParserOptions
        preFilterYears
        ccesTSV
      ccesFrame <-
        fmap transformCCESRow
          <$> maybeRecsToFrame fixCCESRow (const True) ccesMaybeRecs
      let
        firstFew = take 1000 $ FL.fold
          FL.list
          (fmap
            (F.rcast
              @'[CCESCaseId, StateAbbreviation, Registration, Turnout, HouseVoteParty, PartisanId3]
            )
            ccesFrame
          )
      K.logLE K.Diagnostic
        $  "ccesFrame (first 100 rows):\n"
        <> (T.intercalate "\n" $ fmap (T.pack . show) firstFew)
      K.logLE K.Info "Inferring..."
      K.logLE K.Info "Knitting docs..."
  case eitherDocs of
    Right namedDocs ->
      K.writeAllPandocResultsWithInfoAsHtml "reports/html/MRP_Basics" namedDocs
    Left err -> putStrLn $ "pandoc error: " ++ show err

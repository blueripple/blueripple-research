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
import           BlueRipple.Data.MRP
import qualified BlueRipple.Model.TurnoutAdjustment
                                               as TA

templateVars = M.fromList
  [ ("lang"     , "English")
  , ("author"   , "Adam Conner-Sax & Frank David")
  , ("pagetitle", "Preference Model & Predictions")
--  , ("tufte","True")
  ]

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
      let
        csvParserOptions =
          F.defaultParser { F.quotingMode = F.RFC4180Quoting ' ' }
        tsvParserOptions = csvParserOptions { F.columnSeparator = "\t" }
        preFilterYears   = FU.filterOnMaybeField @CCESYear
          (`L.elem` [2010, 2012, 2014, 2016, 2018])
      ccesMaybeRecs <- loadToMaybeRecs @CCES_MRP_Raw @(F.RecordColumns CCES)
        tsvParserOptions
        preFilterYears
        ccesTSV
      ccesFrame <-
        fmap transformCCESRow
          <$> maybeRecsToFrame
                fixCCESRow
                ((`L.elem` [2010, 2012, 2014, 2016, 2018]) . F.rgetField @Year)
                ccesMaybeRecs
      let firstFew = take 4 $ FL.fold FL.list ccesFrame
      K.logLE K.Diagnostic
        $  "ccesFrame (first 4 rows):\n"
        <> (T.pack $ show firstFew)
      K.logLE K.Info "Inferring..."
      K.logLE K.Info "Knitting docs..."
  case eitherDocs of
    Right namedDocs ->
      K.writeAllPandocResultsWithInfoAsHtml "reports/html/MRP_Basics" namedDocs
    Left err -> putStrLn $ "pandoc error: " ++ show err

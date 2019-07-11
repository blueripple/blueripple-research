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

import qualified Data.Vector                   as VB

import qualified Data.Text                     as T

import qualified Text.Printf                   as PF

import qualified Data.Vinyl                    as V
import qualified Frames                        as F

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
    K.knitHtmls (Just "MRP_Basics.Main") K.nonDiagnostic pandocWriterConfig $ do
      K.logLE K.Info "Loading data..."
      K.logLE K.Info "Inferring..."
      K.logLE K.Info "Knitting docs..."
  case eitherDocs of
    Right namedDocs ->
      K.writeAllPandocResultsWithInfoAsHtml "reports/html/MRP_Basics" namedDocs
    Left err -> putStrLn $ "pandoc error: " ++ show err

{-# LANGUAGE AllowAmbiguousTypes       #-}
{-# LANGUAGE DataKinds                 #-}
{-# LANGUAGE DeriveDataTypeable        #-}
{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE GADTs                     #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE PolyKinds                 #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE TypeApplications          #-}
{-# LANGUAGE TypeOperators             #-}
{-# LANGUAGE QuasiQuotes               #-}
--{-# LANGUAGE AllowAmbiguousTypes       #-}

import qualified Control.Foldl                 as FL
import           Control.Lens                   ((%~))
import qualified Control.Monad.State           as ST
import Control.Monad (when)
import           Control.Monad.IO.Class         ( MonadIO(liftIO) )

import qualified Data.Time.Calendar            as Time
import qualified Data.Time.Clock               as Time
import qualified Data.Time.Format              as Time

import qualified Data.Array as A
import qualified Data.List                     as L
import qualified Data.Map                      as M
import           Data.Maybe (fromMaybe, isJust)
import qualified Data.Vector                   as VB
import qualified Data.Vector.Storable          as VS
import qualified Data.Text                     as T
import           Data.Data                      ( Data )
import           Data.Typeable                  ( Typeable )

import qualified Text.Printf                   as PF

import qualified Data.Vinyl                    as V
import qualified Data.Vinyl.TypeLevel          as V
import qualified Frames                        as F
import qualified Frames.CSV                    as F
import qualified Frames.Melt                   as F

import qualified Pipes                         as P
import qualified Pipes.Prelude                 as P

import qualified Numeric.LinearAlgebra         as LA

import qualified Frames                        as F
import qualified Frames.InCore                 as F
import qualified Data.Vinyl                    as V

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
import qualified Frames.Serialize              as FS
import qualified Frames.MaybeUtils             as FM

import qualified Knit.Report                   as K
import qualified Knit.Report.Cache             as K
import qualified Polysemy                      as P
import           Polysemy.Error                 ( Error, mapError, throw )
import           Polysemy.Async                 (Async, asyncToIO, asyncToIOFinal) -- can't use final with this version of Knit-Haskell
import           Polysemy.RandomFu              (RandomFu, runRandomIO)
import           Text.Pandoc.Error             as PE

import           Data.String.Here               ( here )

import qualified System.Console.CmdArgs.Implicit as CA
import System.Console.CmdArgs.Implicit ((&=))


import           BlueRipple.Data.DataFrames as BR
import           BlueRipple.Data.Loaders as BR
import           BlueRipple.Data.PrefModel as BR

import           BlueRipple.Utilities.KnitUtils

import qualified BlueRipple.Model.TurnoutAdjustment
                                               as TA
                                               
import           MRP.CCES
import           MRP.Common
import qualified MRP.WWC as WWC
import qualified MRP.Pools as Pools
import qualified MRP.DeltaVPV as DeltaVPV
import qualified MRP.Kentucky as Kentucky
import qualified MRP.Wisconsin as Wisconsin
import qualified MRP.TurnoutGaps as TurnoutGaps
import qualified MRP.ElectoralWeights as ElectoralWeights

yamlAuthor :: T.Text
yamlAuthor = [here|
- name: Adam Conner-Sax
- name: Frank David
|]

templateVars =
  M.fromList [("lang", "English")
             , ("site-title", "Blue Ripple Politics")
             , ("home-url", "https://www.blueripplepolitics.org")             
--  , ("author"   , T.unpack yamlAuthor)
             ]

{- TODO
1. Why are rows still being dropped?  Which col is missing?  In general, write a diagnostic for showing what is missing...
Some answers:
The boring:  CountyFIPS.  I don't want this anyway.  Dropped
The weird:  Missing voted_rep_party.  These are non-voters or voters who didn't vote in house race.  A bit more than 1/3 of
survey responses.  Maybe that's not weird??
-}

pandocTemplate = K.FullySpecifiedTemplatePath "pandoc-templates/blueripple_basic.html"

data PostArgs = PostArgs { posts :: [Post], updated :: Bool, diagnostics :: Bool } deriving (Show, Data, Typeable)

postArgs = PostArgs { posts = CA.enum [[] &= CA.ignore,
                                        [PostWWC] &= CA.name "wwc" &= CA.help "knit \"WWC\"",
                                        [PostPools] &= CA.name "pools" &= CA.help "knit \"Pools\"",
                                        [PostDeltaVPV] &= CA.name "dVPV" &= CA.help "knit \"Delta VPV\"",
                                        [PostMethods] &= CA.name "methods" &= CA.help "knit \"Methods\"",
                                        [PostKentucky] &= CA.name "KY" &= CA.help "knit \"Kentucky\"",
                                        [PostWisconsin] &= CA.name "WI" &= CA.help "knit \"Wisconsin\"",
                                        [PostTurnoutGaps] &= CA.name "turnout" &= CA.help "knit \"Turnout\"",
                                        [PostElectoralWeights] &= CA.name "weights" &= CA.help "knit \"Electoral Weights\"", 
                                        [(minBound :: Post).. ] &= CA.name "all" &= CA.help "knit all"
                                      ]
                    , updated = CA.def
                      &= CA.name "u"
                      &= CA.help "Flag to set whether the post gets an updated date annotation.  Defaults to False."
                      &= CA.typ "Bool"
                    , diagnostics = CA.def
                      &= CA.name "d"
                      &= CA.help "Show diagnostic info.  Defaults to False."
                      &= CA.typ "Bool"                    
                    }
           &= CA.verbosity
           &= CA.help "Produce MRP Model Blue Ripple Politics Posts"
           &= CA.summary "mrp-model v0.1.0.0, (C) 2019 Adam Conner-Sax"

main :: IO ()
main = do
  args <- CA.cmdArgs postArgs
  putStrLn $ show args
  pandocWriterConfig <- K.mkPandocWriterConfig pandocTemplate
                                               templateVars
                                               brWriterOptionsF
  let logFilter = case (diagnostics args) of
        False -> K.nonDiagnostic
        True -> K.logAll
      knitConfig = K.defaultKnitConfig
        { K.outerLogPrefix = Just "MRP.Main"
        , K.logIf = logFilter
        , K.pandocWriterConfig = pandocWriterConfig
        }      
  eitherDocs <-
    K.knitHtmls knitConfig $ runRandomIO $ do
      stateCrossWalkFrame <- BR.stateAbbrCrosswalkLoader
      let statesFromAbbreviations = M.fromList $ fmap (\r -> (F.rgetField @StateAbbreviation r, F.rgetField @StateName r)) $ FL.fold FL.list stateCrossWalkFrame
      K.logLE K.Info "Knitting docs..."      
      curDate <- (\(Time.UTCTime d _) -> d) <$> K.getCurrentTime
      let pubDateIntro = Time.fromGregorian 2019 12 9      
      when (PostWWC `elem` (posts args)) $ K.newPandoc
        (K.PandocInfo
         (postPath PostWWC)
         (brAddDates (updated args) pubDateIntro curDate
          $ M.fromList [("pagetitle", "How did WWCV voter preference change from 2016 to 2018?")
                        ,("title","How did WWCV voter preference change from 2016 to 2018?")
                        ]
          )
        )
        $ WWC.post statesFromAbbreviations 
      let pubDatePools = Time.fromGregorian 2019 12 9        
      when (PostPools `elem` (posts args)) $ K.newPandoc
        (K.PandocInfo
         (postPath PostPools)
         (brAddDates (updated args) pubDatePools curDate
          $ M.fromList [("pagetitle", "Where Do We Look for Democratic Votes in 2020?")
                        ,("title","Where Do We Look For Democratic Votes in 2020?")
                        ]
          )
        )
        $ Pools.post statesFromAbbreviations
      let pubDateDeltaVPV = Time.fromGregorian 2020 1 5                
      when (PostDeltaVPV `elem` (posts args)) $ K.newPandoc
        (K.PandocInfo
         (postPath PostDeltaVPV)
         (brAddDates (updated args) pubDateDeltaVPV curDate
          $ M.fromList [("pagetitle", "Deep Dive on Variations in Democratic VPV")
                        ,("title","Deep Dive on Variations in Democratic VPV")
                        ]
          )
        )
        $ DeltaVPV.post stateCrossWalkFrame 
      let pubDateKentucky = Time.fromGregorian 2020 1 5                
      when (PostKentucky `elem` (posts args)) $ K.newPandoc
        (K.PandocInfo
         (postPath PostKentucky)
         (brAddDates (updated args) pubDateKentucky curDate
          $ M.fromList [("pagetitle", "Explore Kentucky")
                        ,("title","Explore Kentucky")
                        ]
          )
        )
        $ Kentucky.post
      let pubDateWisconsin = Time.fromGregorian 2020 2 5                
      when (PostWisconsin `elem` (posts args)) $ K.newPandoc
        (K.PandocInfo
         (postPath PostWisconsin)
         (brAddDates (updated args) pubDateWisconsin curDate
          $ M.fromList [("pagetitle", "Explore Wisconsin")
                        ,("title","Explore Wisconsin")
                        ]
          )
        )
        $ Wisconsin.post 
      when (PostTurnoutGaps `elem` (posts args))
        $ TurnoutGaps.post
        (updated args)
      when (PostElectoralWeights `elem` (posts args))
        $ ElectoralWeights.post
        (updated args)

  case eitherDocs of
    Right namedDocs ->
      K.writeAllPandocResultsWithInfoAsHtml "posts" namedDocs
    Left err -> putStrLn $ "pandoc error: " ++ show err


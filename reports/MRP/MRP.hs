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
import Control.Monad (when)

import qualified Data.Time.Calendar            as Time
import qualified Data.Time.Clock               as Time
import qualified Data.Map                      as M

import qualified Data.Text                     as T
import           Data.Data                      ( Data )
import           Data.Typeable                  ( Typeable )
import qualified Data.Random.Source.PureMT     as PureMT

import qualified Frames                        as F

import qualified Knit.Report                   as K
import           Polysemy.RandomFu              (runRandomIO, runRandomIOPureMT)

import           Data.String.Here               ( here )

import qualified System.Console.CmdArgs.Implicit as CA
import System.Console.CmdArgs.Implicit ((&=))


import           BlueRipple.Data.DataFrames as BR
import           BlueRipple.Data.Loaders as BR

import           BlueRipple.Utilities.KnitUtils

import           MRP.Common
import qualified MRP.WWC as WWC
import qualified MRP.Pools as Pools
import qualified MRP.DeltaVPV as DeltaVPV
import qualified MRP.Kentucky as Kentucky
import qualified MRP.Wisconsin as Wisconsin
import qualified MRP.TurnoutGaps as TurnoutGaps
import qualified MRP.ElectoralWeights as ElectoralWeights
import qualified MRP.Language as Language
import qualified MRP.BidenVsWWC as BidenVsWWC
import qualified MRP.DistrictClusters as DistrictClusters

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

data PostArgs = PostArgs { posts :: [Post], updated :: Bool, diagnostics :: Bool, debug :: Int  } deriving (Show, Data, Typeable)

postArgs = PostArgs { posts = CA.enum [[] &= CA.ignore,
                                        [PostWWC] &= CA.name "wwc" &= CA.help "knit \"WWC\"",
                                        [PostPools] &= CA.name "pools" &= CA.help "knit \"Pools\"",
                                        [PostDeltaVPV] &= CA.name "dVPV" &= CA.help "knit \"Delta VPV\"",
                                        [PostMethods] &= CA.name "methods" &= CA.help "knit \"Methods\"",
                                        [PostKentucky] &= CA.name "KY" &= CA.help "knit \"Kentucky\"",
                                        [PostWisconsin] &= CA.name "WI" &= CA.help "knit \"Wisconsin\"",
                                        [PostTurnoutGaps] &= CA.name "turnout" &= CA.help "knit \"Turnout\"",
                                        [PostElectoralWeights] &= CA.name "weights" &= CA.help "knit \"Electoral Weights\"",
                                        [PostLanguage] &= CA.name "language" &= CA.help "knit \"Language\"",
                                        [PostBidenVsWWC] &= CA.name "bidenVsWWC" &= CA.help "knit \"BidenVsWWC\"",
                                        [PostDistrictClusters] &= CA.name "clusters" &= CA.help "knit \"DistrictClusters\"", 
                                        [(minBound :: Post).. ] &= CA.name "all" &= CA.help "knit all"
                                      ]
                    , updated = CA.def
                      &= CA.name "u"
                      &= CA.help "Flag to set whether the post gets an updated date annotation.  Defaults to False."
                      &= CA.typ "Bool"
                    , diagnostics = CA.def
                      &= CA.name "d"
                      &= CA.help "Show diagnostic info.  Defaults to False."
                      &= CA.typ "BOOL"
                    , debug = CA.def
                      &= CA.name "debug"
                      &= CA.help "Show debug info at given level (higher is more info).  If present, overrides and sets \"diagnostic\" to True."
                      &= CA.typ "INT"
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
  let pureMTseed = PureMT.pureMT 1
  let logFilter = if debug args > 0
                  then K.logDebug (debug args)
                  else case (diagnostics args) of                         
                         False -> K.nonDiagnostic
                         True -> K.logDiagnostic
      knitConfig = (K.defaultKnitConfig Nothing)
        { K.outerLogPrefix = Just "MRP.Main"
        , K.logIf = logFilter
        , K.pandocWriterConfig = pandocWriterConfig
        }      
  eitherDocs <-
    K.knitHtmls knitConfig $ runRandomIOPureMT pureMTseed $ do
      stateCrossWalkFrame <- K.ignoreCacheTimeM BR.stateAbbrCrosswalkLoader
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
      when (PostLanguage `elem` (posts args))
        $ Language.post
        (updated args)
      when (PostBidenVsWWC `elem` (posts args))
        $ BidenVsWWC.post
        (updated args)
      when (PostDistrictClusters `elem` (posts args))
        $ DistrictClusters.post
        (updated args)


  case eitherDocs of
    Right namedDocs ->
      K.writeAllPandocResultsWithInfoAsHtml "posts" namedDocs
    Left err -> putStrLn $ "pandoc error: " ++ show err


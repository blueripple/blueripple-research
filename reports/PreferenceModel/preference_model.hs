{-# LANGUAGE DataKinds                 #-}
{-# LANGUAGE DeriveDataTypeable        #-}
{-# LANGUAGE PolyKinds                 #-}
{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE GADTs                     #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE TypeApplications          #-}
{-# LANGUAGE TypeOperators             #-}
{-# LANGUAGE QuasiQuotes               #-}
{-# LANGUAGE AllowAmbiguousTypes       #-}
{-# LANGUAGE TupleSections             #-}
{-# OPTIONS_GHC  -fplugin=Polysemy.Plugin  #-}

module Main where

import qualified Control.Foldl                 as FL
import           Control.Monad.IO.Class         ( MonadIO(liftIO) )
import           Control.Monad (when)
import qualified Data.List                     as L
import qualified Data.Map                      as M
import qualified Data.Array                    as A
import qualified Data.Set                      as S
import qualified Data.Text                     as T
import qualified Data.Time.Calendar            as Time
import qualified Data.Time.Clock               as Time
import qualified Data.Time.Format              as Time
import Data.Data (Data)
import Data.Typeable (Typeable)

import qualified Frames                        as F
import qualified Frames.CSV                    as F

import           Graphics.Vega.VegaLite.Configuration as FV

import qualified Frames.Visualization.VegaLite.Data
                                               as FV
import qualified Frames.Visualization.VegaLite.StackedArea
                                               as FV
import qualified Frames.Visualization.VegaLite.LineVsTime
                                               as FV
import qualified Frames.Visualization.VegaLite.ParameterPlots
                                               as FV                                               

import qualified Knit.Report                   as K
import qualified Knit.Report.Input.MarkDown.PandocMarkDown    as K
import qualified Text.Pandoc.Options           as PA

import           Data.String.Here               ( here, i )


import qualified System.Console.CmdArgs.Implicit as CA
import System.Console.CmdArgs.Implicit ((&=))


import           BlueRipple.Configuration
import           BlueRipple.Utilities.KnitUtils
import           BlueRipple.Data.DataFrames
import           BlueRipple.Data.PrefModel
import           BlueRipple.Data.PrefModel.SimpleAgeSexRace
import           BlueRipple.Data.PrefModel.SimpleAgeSexEducation
import qualified BlueRipple.Model.Preference as PM

import PreferenceModel.Common
import qualified PreferenceModel.Methods as Methods
import qualified PreferenceModel.PM2018 as PM2018
import qualified PreferenceModel.ExitPolls as ExitPolls
import qualified Explainer.WWCV as WWCV
import qualified PreferenceModel.AcrossTime as AcrossTime

yamlAuthor :: T.Text
yamlAuthor = [here|
- name: Adam Conner-Sax
- name: Frank David
|]

templateVars = M.fromList
  [ ("lang"     , "English")
--  , ("author"   , T.unpack yamlAuthor)
--  , ("pagetitle", "Preference Model & Predictions")
  , ("site-title", "Blue Ripple Politics")
  , ("home-url", "https://www.blueripplepolitics.org")
--  , ("tufte","True")
  ]

--pandocTemplate = K.FromIncludedTemplateDir "mindoc-pandoc-KH.html"
pandocTemplate = K.FullySpecifiedTemplatePath "pandoc-templates/blueripple_basic.html"


--data PostName = P1 | P1A | P2 | P3 | Methods  deriving (Show, Data, Typeable, Enum, Bounded, Eq, Ord)
data PostsArgs = PostArgs { posts :: [Post],  updated :: Bool } deriving (Show, Data, Typeable)

postArgs = PostArgs { posts = CA.enum [[] &= CA.ignore,
                                        [Post2018] &= CA.name "p2018" &= CA.help "knit \"2018\"",
                                        [PostExitPolls] &= CA.name "pExitPolls" &= CA.help "knit \"ExitPolls\"",
                                        [PostWWCV] &= CA.name "wwcv" &= CA.help "knit \"WWCV\"",
                                        [PostAcrossTime] &= CA.name "pAcrossTime" &= CA.help "knit \"AcrossTime\"",
                                        [PostMethods] &= CA.name "methods" &= CA.help "knit \"Methods\"",
                                        [(minBound :: Post).. ] &= CA.name "all" &= CA.help "knit all"
                                      ]
                    , updated = CA.def
                      &= CA.name "u"
                      &= CA.help "Flag to set whether the post gets an updated date annotation.  Defaults to False."
                      &= CA.typ "Bool"
                    }
           &= CA.verbosity
           &= CA.help "Produce Preference Model Blue Ripple Politics Posts"
           &= CA.summary "preference_model v0.3.0.0, (C) 2019 Adam Conner-Sax"
           
  
main :: IO ()
main = do
  args <- CA.cmdArgs postArgs
  putStrLn $ show args
  pandocWriterConfig <- K.mkPandocWriterConfig pandocTemplate
                                               templateVars
                                               brWriterOptionsF
  let knitConfig = K.defaultKnitConfig
                   { K.outerLogPrefix = Just "preference-model.Main"
                   , K.logIf = K.nonDiagnostic
                   , K.pandocWriterConfig = pandocWriterConfig
                   }                                               
  eitherDocs <-
    K.knitHtmls knitConfig $ do
    -- load the data   
      let parserOptions =
            F.defaultParser --{ F.quotingMode = F.RFC4180Quoting '\"' }
      K.logLE K.Info "Loading data..."
      contextDemographicsPath <- liftIO $ usePath contextDemographicsCSV
      contextDemographicsFrame :: F.Frame ContextDemographics <- loadToFrame
        parserOptions
        contextDemographicsPath
        (const True)
      asrDemographicsPath <- liftIO $ usePath ageSexRaceDemographicsLongCSV
      asrDemographicsFrame :: F.Frame ASRDemographics <-
        loadToFrame parserOptions  asrDemographicsPath (const True)
      aseDemographicsPath <- liftIO $ usePath ageSexEducationDemographicsLongCSV  
      aseDemographicsFrame :: F.Frame ASEDemographics <-
        loadToFrame parserOptions aseDemographicsPath (const True)
      houseElectionsPath <- liftIO $ usePath houseElectionsCSV  
      houseElectionsFrame :: F.Frame HouseElections <- loadToFrame
        parserOptions
        houseElectionsPath
        (const True)
      asrTurnoutPath <- liftIO $ usePath detailedASRTurnoutCSV  
      asrTurnoutFrame :: F.Frame TurnoutASR <- loadToFrame
        parserOptions
        asrTurnoutPath
        (const True)
      aseTurnoutPath <- liftIO $ usePath detailedASETurnoutCSV
      aseTurnoutFrame :: F.Frame TurnoutASE <- loadToFrame
        parserOptions
        aseTurnoutPath
        (const True)
      edisonExitPath <- liftIO $ usePath exitPoll2018CSV  
      edisonExit2018Frame :: F.Frame EdisonExit2018 <- loadToFrame
        parserOptions
        edisonExitPath
        (const True)
      K.logLE K.Info "Inferring..."
      let yearSets :: M.Map Post (S.Set Int) =
            M.fromList [(Post2018,S.fromList [2018]),
                        (PostExitPolls,S.fromList [2018]),
                        (PostWWCV,S.fromList []),
                        (PostAcrossTime,S.fromList [2010, 2012, 2014, 2016, 2018]),
                        (PostMethods,S.empty)]
      yearList <- knitMaybe "Failed to find a yearlist for some post name!" $ fmap (S.toList . S.unions) $ traverse (\pn -> M.lookup pn yearSets) $ posts args          
      let years      = M.fromList $ fmap (\x -> (x, x)) yearList
      
      modeledResultsASR <- PM.modeledResults simpleAgeSexRace (const True) asrDemographicsFrame asrTurnoutFrame houseElectionsFrame years 
      modeledResultsASE <- PM.modeledResults simpleAgeSexEducation (const True) aseDemographicsFrame aseTurnoutFrame houseElectionsFrame years
      let
        battlegroundStates =
          [ "NH"
          , "PA"
          , "VA"
          , "NC"
          , "FL"
          , "OH"
          , "MI"
          , "WI"
          , "IA"
          , "CO"
          , "AZ"
          , "NV"
          ]
--        bgOnly r =
--          L.elem (F.rgetField @StateAbbreviation r) battlegroundStates      
--      modeledResultsBG_ASR <- PM.modeledResults simpleAgeSexRace bgOnly asrDemographicsFrame asrTurnoutFrame houseElectionsFrame years 
--      modeledResultsBG_ASE <- PM.modeledResults simpleAgeSexEducation bgOnly aseDemographicsFrame aseTurnoutFrame houseElectionsFrame years 

      K.logLE K.Info "Knitting docs..."
      curDate <- (\(Time.UTCTime d _) -> d) <$> K.getCurrentTime
      let pubDateP1 = Time.fromGregorian 2019 9 2
      when (Post2018 `elem` (posts args)) $ K.newPandoc
        (K.PandocInfo
          (postPath Post2018)
          (brAddDates (updated args) pubDateP1 curDate -- (Just curDate)
            $ M.fromList [("pagetitle", "Digging into 2018 -  National Voter Preference")
                         ,("title", "Digging into the 2018 House Election Results")             
                         ]
          )
        )
        $ PM2018.post modeledResultsASR modeledResultsASE
      when (PostExitPolls `elem` (posts args)) $ K.newPandoc
        (K.PandocInfo
         (postPath PostExitPolls)
          (brAddDates (updated args) pubDateP1 curDate -- (Just curDate)
           $ M.fromList [("pagetitle", "Comparison of inferred preference model to Edison exit polls.")
                        ,("title","Comparing the Inferred Preference Model to the Exit Polls")
                        ]
          )
        )
        $ ExitPolls.post modeledResultsASR modeledResultsASE edisonExit2018Frame
      when (PostMethods `elem` (posts args)) $ K.newPandoc
        (K.PandocInfo
          (postPath PostMethods)
          (brAddDates (updated args) pubDateP1 curDate -- (Just curDate)
            $ M.fromList [("pagetitle", "Inferred Preference Model: Methods & Sources")
                         ]
          )
        )
        $ Methods.post
      let pubP2 = Time.fromGregorian 2019 10 5
          titleP2 = "What's All This Talk About The White Working Class?"
      when (PostWWCV `elem` (posts args)) $ K.newPandoc
        (K.PandocInfo
          (postPath PostWWCV)
          (brAddDates (updated args) pubP2 curDate
            $ M.fromList [("pagetitle", titleP2)
                         ,("title", titleP2)
                         ]
          )
        )
        $ WWCV.post --modeledResultsASR modeledResultsASE houseElectionsFrame
        
      let pubP3 = Time.fromGregorian 2019 10 26  
      when (PostAcrossTime `elem` (posts args)) $ K.newPandoc
        (K.PandocInfo
          (postPath PostAcrossTime)
          (brAddDates (updated args) pubP3 curDate
            $ M.fromList [("pagetitle", "Where Did The Blue Wave Come From?")
                         ,("title", "Where Did The Blue Wave Come From?")
                         ]
          )
        )
        $ AcrossTime.post modeledResultsASR modeledResultsASE {-modeledResultsBG_ASR modeledResultsBG_ASE-} houseElectionsFrame
  case eitherDocs of
    Right namedDocs -> K.writeAllPandocResultsWithInfoAsHtml
      "posts"
      namedDocs
    Left err -> putStrLn $ "pandoc error: " ++ show err
          

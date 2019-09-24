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

import Data.Data (Data)
import Data.Typeable (Typeable)
import qualified System.Console.CmdArgs.Implicit as CA
import System.Console.CmdArgs.Implicit ((&=))


import           BlueRipple.Configuration
import           BlueRipple.Utilities.KnitUtils
import           BlueRipple.Data.DataFrames
import           BlueRipple.Data.PrefModel
import           BlueRipple.Data.PrefModel.SimpleAgeSexRace
import           BlueRipple.Data.PrefModel.SimpleAgeSexEducation
import qualified BlueRipple.Model.Preference as PM

import PrefCommon
import Methods
import P1
import P1A
import P2
import P3

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


data PostName = P1 | P1A | P2 | P3 | Methods  deriving (Show, Data, Typeable, Enum, Bounded, Eq, Ord)
data PostsArgs = PostArgs { posts :: [PostName],  updated :: Bool } deriving (Show, Data, Typeable)



postArgs = PostArgs { posts = CA.enum [[] &= CA.ignore,
                                        [P1] &= CA.name "p1" &= CA.help "knit P1",
                                        [P1A] &= CA.name "p1a" &= CA.help "knit P1A (ExitPolls)",
                                        [P2] &= CA.name "p2" &= CA.help "knit P2",
                                        [P3] &= CA.name "p3" &= CA.help "knit P2",
                                        [Methods] &= CA.name "methods" &= CA.help "knit Methods post",
                                        [(minBound :: PostName).. ] &= CA.name "all" &= CA.help "knit all"
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
  let brMarkDownReaderOptions =
        let exts = PA.readerExtensions K.markDownReaderOptions
        in PA.def
           { PA.readerStandalone = True
           , PA.readerExtensions = PA.enableExtension PA.Ext_smart exts
           }
      brAddMarkDown :: K.KnitOne r => T.Text -> K.Sem r ()
      brAddMarkDown = K.addMarkDownWithOptions brMarkDownReaderOptions
      brWriterOptionsF :: PA.WriterOptions -> PA.WriterOptions
      brWriterOptionsF o =
        let exts = PA.writerExtensions o
        in o { PA.writerExtensions = PA.enableExtension PA.Ext_smart exts
             , PA.writerSectionDivs = True
             }
  pandocWriterConfig <- K.mkPandocWriterConfig pandocTemplate
                                               templateVars
                                               brWriterOptionsF
  eitherDocs <-
    K.knitHtmls (Just "preference_model.Main") K.nonDiagnostic pandocWriterConfig $ do
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
      let yearSets :: M.Map PostName (S.Set Int) =
            M.fromList [(P1,S.fromList [2018]),
                        (P1A,S.fromList [2018]),
                        (P2,S.fromList []),
                        (P3,S.fromList [2010, 2012, 2014, 2016, 2018]),
                        (Methods,S.empty)]
      yearList <- knitMaybe "Failed to find a yearlist for some post name!" $ fmap (S.toList . S.unions) $ traverse (\pn -> M.lookup pn yearSets) $ posts args          
      let years      = M.fromList $ fmap (\x -> (x, x)) yearList
      
      modeledResultsASR <- PM.modeledResults simpleAgeSexRace asrDemographicsFrame asrTurnoutFrame houseElectionsFrame years 
      modeledResultsASE <- PM.modeledResults simpleAgeSexEducation aseDemographicsFrame aseTurnoutFrame houseElectionsFrame years 

      K.logLE K.Info "Knitting docs..."
      curDate <- (\(Time.UTCTime d _) -> d) <$> K.getCurrentTime
      let formatTime t = Time.formatTime Time.defaultTimeLocale "%B %e, %Y" t
          addDates :: Time.Day -> Time.Day -> M.Map String String -> M.Map String String
          addDates pubDate updateDate tMap =
            let pubT = M.singleton "published" $ formatTime pubDate
                updT = case updated args of
                         True -> if (updateDate > pubDate) then M.singleton "updated" (formatTime updateDate) else M.empty
                         False -> M.empty
            in tMap <> pubT <> updT
          -- group name, voting pop, turnout fraction, inferred dem vote fraction  
          pubDateP1 = Time.fromGregorian 2019 9 2
      when (P1 `elem` (posts args)) $ K.newPandoc
        (K.PandocInfo
          "p1/main"
          (addDates pubDateP1 curDate -- (Just curDate)
            $ M.fromList [("pagetitle", "Digging into 2018 -  National Voter Preference")
                         ,("title", "Digging into the 2018 House Election Results")             
                         ]
          )
        )
        $ p1 modeledResultsASR modeledResultsASE
      when (P1A `elem` (posts args)) $ K.newPandoc
        (K.PandocInfo
         "p1/ExitPolls"
          (addDates pubDateP1 curDate -- (Just curDate)
           $ M.fromList [("pagetitle", "Comparison of inferred preference model to Edison exit polls.")
                        ,("title","Comparing the Inferred Preference Model to the Exit Polls")
                        ]
          )
        )
        $ p1a modeledResultsASR modeledResultsASE edisonExit2018Frame
      when (Methods `elem` (posts args)) $ K.newPandoc
        (K.PandocInfo
          "methods/main"
          (addDates pubDateP1 curDate -- (Just curDate)
            $ M.fromList [("pagetitle", "Inferred Preference Model: Methods & Sources")
                         ]
          )
        )
        $ methods
      let pubP2 = Time.fromGregorian 2019 9 27
          titleP2 = "What's All This Talk About The White Working Class?"
      when (P2 `elem` (posts args)) $ K.newPandoc
        (K.PandocInfo
          "p2/main"
          (addDates pubP2 curDate
            $ M.fromList [("pagetitle", titleP2)
                         ,("title", titleP2)
                         ]
          )
        )
        $ p2 "p2" --modeledResultsASR modeledResultsASE houseElectionsFrame
        
      let pubP3 = Time.fromGregorian 2019 9 19  
      when (P3 `elem` (posts args)) $ K.newPandoc
        (K.PandocInfo
          "p3/main"
          (addDates pubP3 curDate
            $ M.fromList [("pagetitle", "Voter Preference: 2010-2018")
                         ,("title", "Voter Preference: 2010-2018")
                         ]
          )
        )
        $ p3 modeledResultsASR modeledResultsASE houseElectionsFrame
  case eitherDocs of
    Right namedDocs -> K.writeAllPandocResultsWithInfoAsHtml
      "posts/preference-model"
      namedDocs
    Left err -> putStrLn $ "pandoc error: " ++ show err
          

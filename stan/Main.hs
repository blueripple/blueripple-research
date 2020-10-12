{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
module Main where

import qualified Control.Foldl as FL
import qualified Data.IntMap.Strict as IM
import qualified Data.Map as M
import qualified Data.Maybe as Maybe

import qualified Data.Text as T
import qualified Frames as F

import qualified Frames.MapReduce as FMR

import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Utilities.KnitUtils as BR
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Model.MRP as BR
import qualified BlueRipple.Data.CCES as CCES
import qualified BlueRipple.Model.CCES_MRP_Analysis as CCES
import qualified BlueRipple.Data.Keyed as BK

import qualified CmdStan as CS
import qualified Stan.Frames.JSON as SJ


import qualified Knit.Report as K
import Data.String.Here (here)


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


pandocTemplate = K.FullySpecifiedTemplatePath "pandoc-templates/blueripple_basic.html"


main :: IO ()
main= do
  pandocWriterConfig <- K.mkPandocWriterConfig pandocTemplate
                                               templateVars
                                               K.mindocOptionsF
  let  knitConfig = (K.defaultKnitConfig Nothing)
        { K.outerLogPrefix = Just "stan.Main"
        , K.logIf = K.logDiagnostic
        , K.pandocWriterConfig = pandocWriterConfig
        }
  resE <- K.knitHtml knitConfig $ makeDoc
  case resE of
    Right htmlAsText ->
      K.writeAndMakePathLT "stan.html" htmlAsText
    Left err -> putStrLn $ "Pandoc Error: " ++ show err


makeDoc :: forall r.(K.KnitOne r,  K.CacheEffectsD r) => K.Sem r ()
makeDoc = do
  cces <- K.ignoreCacheTimeM $ CCES.ccesDataLoader
  let addZerosF =  FMR.concatFold $ FMR.mapReduceFold
             (FMR.noUnpack)
             (FMR.splitOnKeys @'[BR.StateAbbreviation])
             ( FMR.makeRecsWithKey id
               $ FMR.ReduceFold
               $ const
               $ BK.addDefaultRec @DT.CatColsASER5 BR.zeroCount )
  let pres2016 = FL.fold (CCES.countDemPres2016VotesF @DT.CatColsASER5) cces
      pres2016WithZeros = FL.fold addZerosF pres2016 
  BR.logFrame pres2016
  -- build the enumerating fold
  let enumSexF = SJ.enumerateField (T.pack . show) (SJ.enumerate 1) (F.rgetField @DT.SexC)
      enumAgeF = SJ.enumerateField (T.pack . show) (SJ.enumerate 1) (F.rgetField @DT.SimpleAgeC)
      enumEducationF = SJ.enumerateField (T.pack . show) (SJ.enumerate 1) (F.rgetField @DT.CollegeGradC)
      enumRaceF = SJ.enumerateField (T.pack . show) (SJ.enumerate 1) (F.rgetField @DT.Race5C)
      enumStateF = SJ.enumerateField id (SJ.enumerate 1) (F.rgetField @BR.StateAbbreviation)
      enumF = (,,,,) <$> enumSexF <*> enumAgeF <*> enumEducationF <*> enumRaceF <*> enumStateF
      ((sexF, toSex), (ageF, toAge), (educationF, toEducation), (raceF, toRace), (stateF, toState)) = FL.fold enumF pres2016WithZeros
      dataF = SJ.namedF "G" FL.length
              <> SJ.constDataF "J_state" (IM.size toState)
              <> SJ.constDataF "J_sex" (IM.size toSex)
              <> SJ.constDataF "J_age" (IM.size toAge)
              <> SJ.constDataF "J_educ" (IM.size toEducation)
              <> SJ.constDataF "J_race" (IM.size toRace)
              <> SJ.valueToPairF "sex" sexF
              <> SJ.valueToPairF "age" ageF
              <> SJ.valueToPairF "education" educationF
              <> SJ.valueToPairF "race" raceF
              <> SJ.valueToPairF "state" stateF
              <> SJ.valueToPairF "D_votes" (SJ.jsonArrayF $ F.rgetField @BR.UnweightedSuccesses)
              <> SJ.valueToPairF "Total_votes" (SJ.jsonArrayF $ F.rgetField @BR.Count)
  K.liftKnit $ do
    let model = "stan/prefMRP"
        datFile = "stan/ccesTest.json"
    SJ.frameToStanJSONFile datFile dataF pres2016WithZeros
    CS.make =<< CS.makeDefaultMakeConfig model
    let config = (CS.makeDefaultSample model 1) { CS.inputData = Just datFile }
    CS.stan model config
    outputFile <- pure $ Maybe.fromMaybe (error "impossible") $ CS.output config
--    let outputFile = output.csv
    putStrLn . CS.unparsed =<< CS.stansummary (CS.makeDefaultSummaryConfig [outputFile])

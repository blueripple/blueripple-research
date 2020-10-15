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
import qualified BlueRipple.Utilities.KnitUtils as BR

import qualified CmdStan as CS
import qualified CmdStan.Types as CS
import qualified Stan.JSON as SJ
import qualified Stan.Parameters as SP
import qualified Stan.ModelRunner as SM
import qualified System.Environment as Env

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


runPrefMRPModels :: forall r.(K.KnitEffects r,  K.CacheEffectsD r) => K.Sem r ()
runPrefMRPModels = do
  -- wrangle data  
  cces_C <- CCES.ccesDataLoader
  ccesPres2016ASER5_C <- BR.retrieveOrMakeFrame "stan/ccesPres2016.bin" cces_C $ \cces -> do
    K.logLE K.Info "CCES data.  Rebuilding presidential 2016 ASER5 rollup."
    let addZerosF =  FMR.concatFold $ FMR.mapReduceFold
                     (FMR.noUnpack)
                     (FMR.splitOnKeys @'[BR.StateAbbreviation])
                     ( FMR.makeRecsWithKey id
                       $ FMR.ReduceFold
                       $ const
                       $ BK.addDefaultRec @DT.CatColsASER5 BR.zeroCount )
        pres2016 = FL.fold (CCES.countDemPres2016VotesF @DT.CatColsASER5) cces
        pres2016WithZeros = FL.fold addZerosF pres2016        
    return pres2016WithZeros
  -- build enumeration folds
  let enumSexF = SJ.enumerateField (T.pack . show) (SJ.enumerate 1) (F.rgetField @DT.SexC)
      enumAgeF = SJ.enumerateField (T.pack . show) (SJ.enumerate 1) (F.rgetField @DT.SimpleAgeC)
      enumEducationF = SJ.enumerateField (T.pack . show) (SJ.enumerate 1) (F.rgetField @DT.CollegeGradC)
      enumRaceF = SJ.enumerateField (T.pack . show) (SJ.enumerate 1) (F.rgetField @DT.Race5C)
      enumStateF = SJ.enumerateField id (SJ.enumerate 1) (F.rgetField @BR.StateAbbreviation)
      enumCategoryF = SJ.enumerateField (T.pack . show) (SJ.enumerate 1) (F.rcast @DT.CatColsASER5)
  -- do outer enumeration fold for indices
  pres2016WithZeros <- K.ignoreCacheTime ccesPres2016ASER5_C
  let enumF = (,,,,,)
        <$> enumSexF
        <*> enumAgeF
        <*> enumEducationF
        <*> enumRaceF
        <*> enumStateF
        <*> enumCategoryF
      ((sexF, toSex)
        , (ageF, toAge)
        , (educationF, toEducation)
        , (raceF, toRace)
        , (stateF, toState)
        , (categoryF, toCategory)) = FL.fold enumF pres2016WithZeros
  -- create model runner actions        
  let makeJson ccesASER5 = do
        let dataF = SJ.namedF "G" FL.length
                    <> SJ.constDataF "J_state" (IM.size toState)
                    <> SJ.constDataF "J_sex" (IM.size toSex)
                    <> SJ.constDataF "J_age" (IM.size toAge)
                    <> SJ.constDataF "J_educ" (IM.size toEducation)
                    <> SJ.constDataF "J_race" (IM.size toRace)
--                  <> SJ.valueToPairF "sex" sexF
--                  <> SJ.valueToPairF "age" ageF
--                  <> SJ.valueToPairF "education" educationF
--                  <> SJ.valueToPairF "race" raceF
                    <> SJ.valueToPairF "category" categoryF
                    <> SJ.valueToPairF "state" stateF
                    <> SJ.valueToPairF "D_votes" (SJ.jsonArrayF $ F.rgetField @BR.UnweightedSuccesses)
                    <> SJ.valueToPairF "Total_votes" (SJ.jsonArrayF $ F.rgetField @BR.Count)
        K.knitEither $ SJ.frameToStanJSONEncoding dataF ccesASER5
      resultsNational summary _ = do
        probs <- fmap CS.mean <$> (K.knitEither $ SP.parse1D "probs" (CS.paramStats summary)) 
        K.logLE K.Info $ "With Categories: " <> (T.pack . show . fmap (\(i, c) -> (SP.getIndexed probs i, c))  $ IM.toList toCategory)
  stanConfigNational <- SM.makeDefaultModelRunnerConfig "stan" "prefR" 4 (Just 1000) (Just 1000) Nothing
  SM.runModel stanConfigNational makeJson resultsNational ccesPres2016ASER5_C
--  let resultsWithStates summary _ = do
        
        
makeDoc :: forall r.(K.KnitOne r,  K.CacheEffectsD r) => K.Sem r ()
makeDoc = runPrefMRPModels
{-
  clangBinDirM <- K.liftKnit $ Env.lookupEnv "CLANG_BINDIR"
  case clangBinDirM of
    Nothing -> K.logLE K.Info "CLANG_BINDIR not set. Using exisiting path is correct for clang."
    Just clangBinDir -> do
      curPath <- K.liftKnit $ Env.getEnv "PATH"
      K.logLE K.Info $ "Current path: " <> (T.pack $ show curPath) <> ".  Adding " <> (T.pack $ show clangBinDir) <> " for llvm clang."
      K.liftKnit $ Env.setEnv "PATH" (clangBinDir ++ ":" ++ curPath)
  cces_C <- CCES.ccesDataLoader
  ccesPres2016ASER5_C <- BR.retrieveOrMakeFrame "stan/ccesPres2016.bin" cces_C $ \cces -> do
    K.logLE K.Info "CCES data.  Rebuilding presidential 2016 ASER5 rollup."
    let addZerosF =  FMR.concatFold $ FMR.mapReduceFold
                     (FMR.noUnpack)
                     (FMR.splitOnKeys @'[BR.StateAbbreviation])
                     ( FMR.makeRecsWithKey id
                       $ FMR.ReduceFold
                       $ const
                       $ BK.addDefaultRec @DT.CatColsASER5 BR.zeroCount )
        pres2016 = FL.fold (CCES.countDemPres2016VotesF @DT.CatColsASER5) cces
        pres2016WithZeros = FL.fold addZerosF pres2016        
    return pres2016WithZeros
  let addDir d fp = d ++ "/" ++ fp
      modelDir = "stan"
      model = "prefMRP"
      modelFile =  model ++ ".stan"
      outputFile n =  model ++ "_" ++ show n ++ ".csv"
      datFile = model ++ "_dat.json"      
      numChains = 4

  let enumSexF = SJ.enumerateField (T.pack . show) (SJ.enumerate 1) (F.rgetField @DT.SexC)
      enumAgeF = SJ.enumerateField (T.pack . show) (SJ.enumerate 1) (F.rgetField @DT.SimpleAgeC)
      enumEducationF = SJ.enumerateField (T.pack . show) (SJ.enumerate 1) (F.rgetField @DT.CollegeGradC)
      enumRaceF = SJ.enumerateField (T.pack . show) (SJ.enumerate 1) (F.rgetField @DT.Race5C)
      enumStateF = SJ.enumerateField id (SJ.enumerate 1) (F.rgetField @BR.StateAbbreviation)
      enumCategoryF = SJ.enumerateField (T.pack . show) (SJ.enumerate 1) (F.rcast @DT.CatColsASER5)
      
  json_C <- do
    let jsonFP = addDir modelDir datFile
    curJSON_C <- BR.fileDependency jsonFP
    BR.updateIf curJSON_C ccesPres2016ASER5_C $ \pres2016WithZeros -> do
      K.logLE K.Info $ "CCES rollup changed.  Rebuilding Stan JSON input at \"" <> (T.pack jsonFP) <> "\"."
      -- build and do the enumerating fold
      let enumF = (,,,,,) <$> enumSexF <*> enumAgeF <*> enumEducationF <*> enumRaceF <*> enumStateF <*> enumCategoryF
          ((sexF, toSex), (ageF, toAge), (educationF, toEducation), (raceF, toRace), (stateF, toState), (categoryF, toCategory)) = FL.fold enumF pres2016WithZeros
      let dataF = SJ.namedF "G" FL.length
                  <> SJ.constDataF "J_state" (IM.size toState)
                  <> SJ.constDataF "J_sex" (IM.size toSex)
                  <> SJ.constDataF "J_age" (IM.size toAge)
                  <> SJ.constDataF "J_educ" (IM.size toEducation)
                  <> SJ.constDataF "J_race" (IM.size toRace)
--                  <> SJ.valueToPairF "sex" sexF
--                  <> SJ.valueToPairF "age" ageF
--                  <> SJ.valueToPairF "education" educationF
--                  <> SJ.valueToPairF "race" raceF
                  <> SJ.valueToPairF "category" categoryF
                  <> SJ.valueToPairF "state" stateF
                  <> SJ.valueToPairF "D_votes" (SJ.jsonArrayF $ F.rgetField @BR.UnweightedSuccesses)
                  <> SJ.valueToPairF "Total_votes" (SJ.jsonArrayF $ F.rgetField @BR.Count)
      K.liftKnit $ SJ.frameToStanJSONFile jsonFP dataF pres2016WithZeros
  let stanOutputFiles = fmap (\n -> outputFile n) [1..numChains]
  stanMakeConfig <- K.liftKnit $ CS.makeDefaultMakeConfig (addDir modelDir model)
  stanOutput_C <- do    
    curStanOutputs_C <- fmap BR.oldestUnit $ traverse (BR.fileDependency . addDir modelDir) stanOutputFiles
    curModel_C <- BR.fileDependency (addDir modelDir modelFile)
    let runStanDeps = (,) <$> json_C <*> curModel_C
        runOneChain chainIndex = do 
          let config = (CS.makeDefaultSample model chainIndex) { CS.inputData = Just (addDir modelDir datFile)
                                                               , CS.output = Just (addDir modelDir $ outputFile chainIndex) 
                                                               , CS.numSamples = Just 1000
                                                               , CS.numWarmup = Just 1000
                                                               }
          K.logLE K.Info $ "Running " <> T.pack model <> " for chain " <> (T.pack $ show chainIndex)
          K.logLE K.Diagnostic $ "Command: " <> T.pack (CS.toStanExeCmdLine config)
          K.liftKnit $ CS.stan (addDir modelDir model) config
          K.logLE K.Info $ "Finished chain " <> (T.pack $ show chainIndex)
          
    res <- K.ignoreCacheTimeM $ BR.updateIf (fmap Just curStanOutputs_C) runStanDeps $ \_ ->  do
      K.logLE K.Info "Stan outputs older than input data or model.  Rebuilding Stan exe and running."
      K.liftKnit $ CS.make stanMakeConfig
      maybe Nothing (const $ Just ()) . sequence <$> (K.sequenceConcurrently $ fmap runOneChain [1..numChains])
    K.knitMaybe "THere was an error running an MCMC chain." res    
  summaryConfig <- K.liftKnit $ CS.useCmdStanDirForStansummary (CS.makeDefaultSummaryConfig $ fmap (addDir modelDir) stanOutputFiles)
  K.logLE K.Diagnostic $ "Summary command: " <> (T.pack $ (CS.cmdStanDir stanMakeConfig) ++ "/bin/stansummary") <> " " <> T.intercalate " " (fmap T.pack (CS.stansummaryConfigToCmdLine summaryConfig))
  summary <- K.liftKnit $ CS.stansummary summaryConfig
  K.logLE K.Info $ "Stan Summary:\n" <> (T.pack $ CS.unparsed summary)
  probs <- fmap CS.mean <$> (K.knitEither $ SP.parse1D "probs" (CS.paramStats summary))
  K.logLE K.Info $ "Probs: " <> (T.pack . show $ probs)
  -- rebuild category index
  cces <- K.ignoreCacheTime ccesPres2016ASER5_C
  let (_, toCategory) = FL.fold enumCategoryF cces
  K.logLE K.Info $ "With Categories: " <> (T.pack . show . fmap (\(i, c) -> (SP.getIndexed probs i, c))  $ IM.toList toCategory)
-}  

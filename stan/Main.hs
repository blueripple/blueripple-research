{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
module Main where

import qualified Control.Foldl as FL
import qualified Data.IntMap.Strict as IM
import qualified Data.Map as M
import qualified Data.Maybe as Maybe

import qualified Data.Random.Source.PureMT     as PureMT
import qualified Data.Text as T
import qualified Frames as F
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V

import qualified Frames.MapReduce as FMR

import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Utilities.KnitUtils as BR
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.ElectionTypes as ET
import qualified BlueRipple.Model.MRP as BR
import qualified BlueRipple.Data.CCES as CCES
import qualified BlueRipple.Model.CCES_MRP_Analysis as CCES
import qualified BlueRipple.Data.Keyed as BK
import qualified BlueRipple.Utilities.KnitUtils as BR

import qualified BlueRipple.Model.CachedModels as BRC
import qualified BlueRipple.Model.StanCCES as BRS

import qualified CmdStan as CS
import qualified CmdStan.Types as CS
import qualified Stan.JSON as SJ
import qualified Stan.Parameters as SP
import qualified Stan.ModelRunner as SM
import qualified System.Environment as Env

import qualified Knit.Report as K
import           Polysemy.RandomFu              (RandomFu, runRandomIO, runRandomIOPureMT)
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
  let pureMTseed = PureMT.pureMT 1
  resE <- K.knitHtml knitConfig $ runRandomIOPureMT pureMTseed $ makeDoc
  case resE of
    Right htmlAsText ->
      K.writeAndMakePathLT "stan.html" htmlAsText
    Left err -> putStrLn $ "Pandoc Error: " ++ show err


        
        
makeDoc :: forall r.(K.KnitOne r,  K.CacheEffectsD r, K.Member RandomFu r) => K.Sem r ()
makeDoc = do
  K.logLE K.Info "Stan model fit for 2016 presidential votes:"
  stan <- K.ignoreCacheTimeM $ BRS.prefASER5_MR ET.President 2016
  BR.logFrame stan
  K.logLE K.Info "glm-haskell model fit for 2016 presidential votes:"
  let g r = (F.rgetField @BR.Year r == 2016) && (F.rgetField @ET.Office r == ET.President)
  glmHaskell <- F.filterFrame g <$> (K.ignoreCacheTimeM $ BRC.ccesPreferencesASER5_MRP)
  BR.logFrame glmHaskell

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
      resultsWithStates summary _ = do
        stateProbs <- fmap CS.mean <$> (K.knitEither $ SP.parse2D "stateProbs" (CS.paramStats summary))
        -- build rows to left join
        let (states, cats) = SP.getDims stateProbs
            indices = [(stateI, catI) | stateI <- [1..states], catI <- [1..cats]]
            makeRow (stateI, catI) = do
              abbr <- IM.lookup stateI toState 
              catRec <- IM.lookup catI toCategory
              let prob = SP.getIndexed stateProbs (stateI, catI)
                  vpv = 2 * prob - 1
                  x :: F.Record ('[BR.StateAbbreviation] V.++ DT.CatColsASER5 V.++ [BR.Year, ET.Office, ET.DemVPV, BR.DemPref])
                  x = (abbr F.&: catRec) F.<+> (2016 F.&: ET.President F.&: vpv F.&: prob F.&: V.RNil)
              return x
        probRows <- K.knitMaybe "Error looking up indices.  Should be impossible!"
                    $ traverse makeRow indices
        BR.logFrame probRows
        return probRows       
  let stancConfig = (SM.makeDefaultStancConfig "/Users/adam/BlueRipple/research/stan/voterPref/binomial_ASER5_state_model") { CS.useOpenCL = False }
  stanConfig <- SM.makeDefaultModelRunnerConfig
    "stan/voterPref"
    "binomial_ASER5_state_model"
    Nothing
    (Just "cces_President_2016.json")
    (Just "cces_President_2016_binomial_ASER5_state")
    4
    (Just 1000)
    (Just 1000)
    (Just stancConfig)
  _ <- SM.runModel stanConfig makeJson resultsWithStates ccesPres2016ASER5_C
  return ()
--  let resultsWithStates summary _ = do

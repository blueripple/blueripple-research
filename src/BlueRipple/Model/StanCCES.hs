{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}

module BlueRipple.Model.StanCCES where

import qualified Control.Foldl as FL
import qualified Data.IntMap.Strict as IM
import qualified Data.Map as M
import qualified Data.Maybe as Maybe

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

import qualified CmdStan as CS
import qualified CmdStan.Types as CS
import qualified Stan.JSON as SJ
import qualified Stan.Parameters as SP
import qualified Stan.ModelRunner as SM
import qualified System.Environment as Env

import qualified Knit.Report as K

prefASER5_MR :: forall r.(K.KnitEffects r,  K.CacheEffectsD r)
             => ET.Office
             -> Int
             -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec
                                                  ( '[BR.StateAbbreviation]
                                                    V.++
                                                    DT.CatColsASER5
                                                    V.++
                                                    '[BR.Year, ET.Office, ET.DemVPV, BR.DemPref]
                                                  )))
prefMR office year = do
  -- wrangle data  
  cces_C <- CCES.ccesDataLoader
  countFold <- K.knitEither $ case (office, year) of
    (ET.President, 2008) -> Right $ CCES.countDemPres2008VotesF @DT.CatColsASER5
    (ET.President, 2012) -> Right $ CCES.countDemPres2012VotesF @DT.CatColsASER5
    (ET.President, 2016) -> Right $ CCES.countDemPres2016VotesF @DT.CatColsASER5
    (ET.House, y) -> Right $  CCES.countDemHouseVotesF @DT.CatColsASER5 y
    _ -> Left $ T.pack (show office) <> "/" <> (T.pack $ show year) <> " not available."
  let officeYear = (T.pack $ show office) <> "_" <> (T.pack $ show year)
      countCacheKey = "data/stan/ccesCount_" <> officeYear <> ".bin"  
  ccesASER5_C <- BR.retrieveOrMakeFrame countCacheKey cces_C $ \cces -> do
    K.logLE K.Info $ "CCES data:  Rebuilding " <> countCacheKey
    let addZerosF =  FMR.concatFold $ FMR.mapReduceFold
                     (FMR.noUnpack)
                     (FMR.splitOnKeys @'[BR.StateAbbreviation])
                     ( FMR.makeRecsWithKey id
                       $ FMR.ReduceFold
                       $ const
                       $ BK.addDefaultRec @DT.CatColsASER5 BR.zeroCount )
        counted = FL.fold countFold cces
        countedWithZeros = FL.fold addZerosF pres2016        
    return countedWithZeros
  -- build enumeration folds
  let enumSexF = SJ.enumerateField (T.pack . show) (SJ.enumerate 1) (F.rgetField @DT.SexC)
      enumAgeF = SJ.enumerateField (T.pack . show) (SJ.enumerate 1) (F.rgetField @DT.SimpleAgeC)
      enumEducationF = SJ.enumerateField (T.pack . show) (SJ.enumerate 1) (F.rgetField @DT.CollegeGradC)
      enumRaceF = SJ.enumerateField (T.pack . show) (SJ.enumerate 1) (F.rgetField @DT.Race5C)
      enumStateF = SJ.enumerateField id (SJ.enumerate 1) (F.rgetField @BR.StateAbbreviation)
      enumCategoryF = SJ.enumerateField (T.pack . show) (SJ.enumerate 1) (F.rcast @DT.CatColsASER5)
  -- do outer enumeration fold for indices
  countedWithZeros <- K.ignoreCacheTime ccesASER5_C
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
                  x = (abbr F.&: catRec) F.<+> (y F.&: office F.&: vpv F.&: prob F.&: V.RNil)
              return x
        probRows <- K.knitMaybe "Error looking up indices.  Should be impossible!"
                    $ traverse makeRow indices
        BR.logFrame probRows
        return probRows       
  let stancConfig = (SM.makeDefaultStancConfig "/Users/adam/BlueRipple/research/stan/ccesPref/prefMR") { CS.useOpenCL = False }
  stanConfig <- SM.makeDefaultModelRunnerConfig "stan/ccesPref" "prefMR" (Just "ccesPref.json") (Just yearOffice) 4 (Just 1000) (Just 1000) (Just stancConfig)
  _ <- SM.runModel stanConfig makeJson resultsWithStates ccesASER5_C
  return ()

{-# LANGUAGE AllowAmbiguousTypes       #-}
{-# LANGUAGE DataKinds                 #-}
{-# LANGUAGE DeriveDataTypeable        #-}
{-# LANGUAGE DeriveGeneric             #-}
{-# LANGUAGE PolyKinds                 #-}
{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE FlexibleInstances         #-}
{-# LANGUAGE GADTs                     #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE TypeApplications          #-}
{-# LANGUAGE TypeFamilies              #-}
{-# LANGUAGE TypeOperators             #-}
{-# LANGUAGE QuasiQuotes               #-}
{-# LANGUAGE StandaloneDeriving        #-}
{-# LANGUAGE TupleSections             #-}
{-# OPTIONS_GHC  -fplugin=Polysemy.Plugin  #-}

module MRP.Wisconsin where

import qualified Control.Foldl                 as FL
import           Control.Monad (join)
import qualified Data.Array                    as A
import           Data.Function (on)
import qualified Data.List as L
import qualified Data.Set as S
import qualified Data.Map                      as M
import           Data.Maybe (isJust, catMaybes, fromMaybe)
import           Data.Proxy (Proxy(..))
--import  Data.Ord (Compare)

import qualified Data.Text                     as T
import qualified Data.Serialize                as SE
import qualified Data.Vector as V
import qualified Data.Vector.Storable               as VS


import           Graphics.Vega.VegaLite.Configuration as FV
import qualified Graphics.Vega.VegaLite.Compat as FV
import qualified Frames as F
import qualified Frames.Melt as F
import qualified Frames.InCore as FI
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V


import qualified Control.MapReduce             as MR
import qualified Frames.Transform              as FT
import qualified Frames.Folds                  as FF
import qualified Frames.MapReduce              as FMR
import qualified Frames.Enumerations           as FE
import qualified Frames.Utils                  as FU
import qualified Frames.Serialize              as FS

import qualified Frames.Visualization.VegaLite.Data
                                               as FV
import qualified Frames.Visualization.VegaLite.ParameterPlots
                                               as FV                                               

import qualified Graphics.Vega.VegaLite        as GV
import qualified Knit.Report                   as K
import qualified Polysemy.Error                as P (mapError, Error)
import qualified Polysemy                      as P (raise)
import           Text.Pandoc.Error             as PE
import qualified Text.Blaze.Colonnade          as BC

import           Data.String.Here               ( here, i )

import qualified Colonnade                     as C
import qualified Text.Blaze.Colonnade          as BC
import qualified Text.Blaze.Html               as BH
import qualified Text.Blaze.Html5.Attributes   as BHA

import           BlueRipple.Configuration 
import           BlueRipple.Utilities.KnitUtils 
import           BlueRipple.Utilities.TableUtils 
--import           BlueRipple.Data.DataFrames 

import qualified Data.IndexedSet               as IS
import qualified Numeric.GLM.ProblemTypes      as GLM
import qualified Numeric.GLM.ModelTypes      as GLM
import qualified Numeric.GLM.FunctionFamily    as GLM
import qualified Numeric.GLM.MixedModel        as GLM
import qualified Numeric.GLM.Bootstrap            as GLM
import qualified Numeric.GLM.Report            as GLM
import qualified Numeric.GLM.Predict            as GLM
import qualified Numeric.GLM.Confidence            as GLM
import qualified Numeric.SparseDenseConversions as SD

import qualified Statistics.Types              as ST
import GHC.Generics (Generic)


import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Data.Loaders as BR
import qualified BlueRipple.Data.DemographicTypes as BR
import qualified BlueRipple.Data.ElectionTypes as ET
import qualified BlueRipple.Data.HouseElectionTotals as BR
import qualified BlueRipple.Data.PrefModel as BR
import qualified BlueRipple.Data.PrefModel.SimpleAgeSexEducation as BR
import qualified BlueRipple.Model.TurnoutAdjustment as BR
import qualified BlueRipple.Model.MRP_Pref as BR
import qualified BlueRipple.Model.PostStratify as BR
import qualified BlueRipple.Data.UsefulDataJoins as BR
import qualified MRP.CCES_MRP_Analysis as BR
import qualified BlueRipple.Utilities.KnitUtils as BR
import MRP.Common
import MRP.CCES
import MRP.DeltaVPV (DemVPV)

import qualified PreferenceModel.Common as PrefModel
import qualified BlueRipple.Data.Keyed as BR

import qualified Visualizations.StatePrefs as BR

text1 :: T.Text
text1 = [i|
|]

--type LocationCols = '[BR.StateAbbreviation]
type CatColsASER = '[BR.SimpleAgeC, BR.SexC, BR.CollegeGradC, BR.SimpleRaceC]
catKeyASER :: BR.SimpleAge -> BR.Sex -> BR.CollegeGrad -> BR.SimpleRace -> F.Record CatColsASER
catKeyASER a s e r = a F.&: s F.&: e F.&: r F.&: V.RNil

predMapASER :: F.Record CatColsASER -> M.Map CCESPredictor Double
predMapASER r = M.fromList [(P_Sex, if F.rgetField @BR.SexC r == BR.Female then 0 else 1)
                       ,(P_Race, if F.rgetField @BR.SimpleRaceC r == BR.NonWhite then 0 else 1)
                       ,(P_Education, if F.rgetField @BR.CollegeGradC r == BR.NonGrad then 0 else 1)
                       ,(P_Age, if F.rgetField @BR.SimpleAgeC r == BR.EqualOrOver then 0 else 1)
                       ]

allCatKeysASER = [catKeyASER a s e r | a <- [BR.EqualOrOver, BR.Under], e <- [BR.NonGrad, BR.Grad], s <- [BR.Female, BR.Male], r <- [BR.White, BR.NonWhite]]


type CatColsASE = '[BR.SimpleAgeC, BR.SexC, BR.CollegeGradC]
catKeyASE :: BR.SimpleAge -> BR.Sex -> BR.CollegeGrad -> F.Record CatColsASE
catKeyASE a s e = a F.&: s F.&: e F.&: V.RNil

predMapASE :: F.Record CatColsASE -> M.Map CCESPredictor Double
predMapASE r = M.fromList [(P_Sex, if F.rgetField @BR.SexC r == BR.Female then 0 else 1)         
                       ,(P_Education, if F.rgetField @BR.CollegeGradC r == BR.NonGrad then 0 else 1)
                       ,(P_Age, if F.rgetField @BR.SimpleAgeC r == BR.EqualOrOver then 0 else 1)
                       ]

allCatKeysASE = [catKeyASE a s e | a <- [BR.EqualOrOver, BR.Under], s <- [BR.Female, BR.Male], e <- [BR.NonGrad, BR.Grad]]

type CatColsASR = '[BR.SimpleAgeC, BR.SexC, BR.SimpleRaceC]
catKeyASR :: BR.SimpleAge -> BR.Sex -> BR.SimpleRace -> F.Record CatColsASR
catKeyASR a s r = a F.&: s F.&: r F.&: V.RNil

predMapASR :: F.Record CatColsASR -> M.Map CCESPredictor Double
predMapASR r = M.fromList [(P_Sex, if F.rgetField @BR.SexC r == BR.Female then 0 else 1)
                          ,(P_Race, if F.rgetField @BR.SimpleRaceC r == BR.NonWhite then 0 else 1)
                          ,(P_Age, if F.rgetField @BR.SimpleAgeC r == BR.EqualOrOver then 0 else 1)
                          ]

allCatKeysASR = [catKeyASR a s r | a <- [BR.EqualOrOver, BR.Under], s <- [BR.Female, BR.Male], r <- [BR.White, BR.NonWhite]]

catPredMap pmF acks = M.fromList $ fmap (\k -> (k, pmF k)) acks
catPredMapASER = catPredMap predMapASER allCatKeysASER
catPredMapASE = catPredMap predMapASE allCatKeysASE
catPredMapASR = catPredMap predMapASR allCatKeysASR

foldPrefAndTurnoutData :: FF.EndoFold (F.Record '[BR.ACSCount, BR.VotedPctOfAll, DemVPV, BR.DemPref])
foldPrefAndTurnoutData =  FF.sequenceRecFold
                          $ FF.toFoldRecord (FL.premap (F.rgetField @BR.ACSCount) FL.sum)
                          V.:& FF.toFoldRecord (BR.weightedSumRecF @BR.ACSCount @BR.VotedPctOfAll)
                          V.:& FF.toFoldRecord (BR.weightedSumRecF @BR.ACSCount @DemVPV)
                          V.:& FF.toFoldRecord (BR.weightedSumRecF @BR.ACSCount @BR.DemPref)
                          V.:& V.RNil

post :: forall es r.(K.KnitOne r , K.Member GLM.RandomFu r) => K.Sem r ()
post = P.mapError BR.glmErrorToPandocError $ K.wrapPrefix "Wisconsin" $ do
  let stateAbbr = "WI"
      stateOnly = F.filterFrame (\r -> F.rgetField @BR.StateAbbreviation r == stateAbbr)
      stateAndNation = F.filterFrame (\r -> F.rgetField @BR.StateAbbreviation r `L.elem` [stateAbbr, "National"])
  stateTurnoutRaw <- BR.stateTurnoutLoader -- P.raise $ K.useCached stateTurnoutCA
  aseACS <- BR.simpleASEDemographicsLoader 
  asrACS <- BR.simpleASRDemographicsLoader 
  aseTurnout <- BR.simpleASETurnoutLoader 
  asrTurnout <- BR.simpleASRTurnoutLoader 
  let showRecs = T.intercalate "\n" . fmap (T.pack . show) . FL.fold FL.list
  let predictorsASER = [GLM.Intercept, GLM.Predictor P_Sex , GLM.Predictor P_Age, GLM.Predictor P_Education, GLM.Predictor P_Race]
      predictorsASE = [GLM.Intercept, GLM.Predictor P_Sex , GLM.Predictor P_Age, GLM.Predictor P_Education]
      predictorsASR = [GLM.Intercept, GLM.Predictor P_Sex , GLM.Predictor P_Age, GLM.Predictor P_Race]
  inferredPrefsASER <-  stateAndNation <$> K.retrieveOrMakeTransformed (fmap FS.toS . FL.fold FL.list) (F.toFrame . fmap FS.fromS) "mrp/simpleASER_MR.bin"
                        (P.raise $ BR.mrpPrefs @CatColsASER (Just "ASER") ccesDataLoader predictorsASER catPredMapASER) 
  inferredPrefsASE <-  stateAndNation <$> K.retrieveOrMakeTransformed (fmap FS.toS . FL.fold FL.list) (F.toFrame . fmap FS.fromS) "mrp/simpleASE_MR.bin"
                       (P.raise $ BR.mrpPrefs @CatColsASE (Just "ASE") ccesDataLoader predictorsASE catPredMapASE) 
  inferredPrefsASR <-  stateAndNation <$> K.retrieveOrMakeTransformed (fmap FS.toS . FL.fold FL.list) (F.toFrame . fmap FS.fromS) "mrp/simpleASR_MR.bin"
                       (P.raise $ BR.mrpPrefs @CatColsASR (Just "ASR") ccesDataLoader predictorsASR catPredMapASR) 
  brAddMarkDown text1
  _ <- K.addHvega Nothing Nothing $ BR.vlPrefVsTime "Dem Preference By Demographic Split" stateAbbr (FV.ViewConfig 800 800 10) $ fmap F.rcast inferredPrefsASER  
  -- get adjusted turnouts (national rates, adj by state) for each CD
  demographicsAndTurnoutASE <- stateOnly <$> BR.cachedASEDemographicsWithAdjTurnoutByCD (return aseACS) (return aseTurnout) (return stateTurnoutRaw)
  demographicsAndTurnoutASR <- stateOnly <$> BR.cachedASRDemographicsWithAdjTurnoutByCD (return asrACS) (return asrTurnout) (return stateTurnoutRaw)
  -- join with the prefs
  K.logLE K.Info "Joining turnout by CD and prefs"
  let aseTurnoutAndPrefs = catMaybes
                           $ fmap F.recMaybe
                           $ F.leftJoin @([BR.StateAbbreviation, BR.Year] V.++ CatColsASE) demographicsAndTurnoutASE inferredPrefsASE
      asrTurnoutAndPrefs = catMaybes
                           $ fmap F.recMaybe
                           $ F.leftJoin @([BR.StateAbbreviation, BR.Year] V.++ CatColsASR) demographicsAndTurnoutASR inferredPrefsASR
      labelPSBy x = V.rappend (FT.recordSingleton @ET.PrefType x)
      psCellVPVByBothF =  (<>)
                          <$> fmap pure (fmap (labelPSBy ET.PSByVAP)
                                         $ BR.postStratifyCell @DemVPV
                                         (realToFrac . F.rgetField @BR.ACSCount)
                                         (realToFrac . F.rgetField @DemVPV))
                          <*> fmap pure (fmap (labelPSBy ET.PSByVoted)
                                         $ BR.postStratifyCell @DemVPV
                                         (\r -> realToFrac (F.rgetField @BR.ACSCount r) * F.rgetField @BR.VotedPctOfAll r)
                                         (realToFrac . F.rgetField @DemVPV))
      psVPVByDistrictF =  BR.postStratifyF
                          @[BR.Year, ET.Office, BR.StateAbbreviation, BR.StateFIPS, BR.CongressionalDistrict]
                          @[DemVPV, BR.ACSCount, BR.VotedPctOfAll]
                          @[ET.PrefType, DemVPV]
                          psCellVPVByBothF
      vpvPostStratifiedByASE = FL.fold psVPVByDistrictF aseTurnoutAndPrefs
      vpvPostStratifiedByASR = FL.fold psVPVByDistrictF asrTurnoutAndPrefs
--      plotEmAll :: Int -> ET.OfficeT -> K.Sem r ()
      plotEmAll y o r = do
        let tStart = (T.pack $ show y) <> " " <> (T.pack $ show o)
            cs = Just $ BR.vpvChoroColorScale (negate r) r
            vc = FV.ViewConfig 800 800 10
        _ <- K.addHvega Nothing Nothing
             $ BR.vlByCD @DemVPV (tStart <> " District VPV (Voting Age Pop, Post-Stratified by Age, Sex, Education)") cs vc
             $ F.filterFrame (\r -> (F.rgetField @BR.Year r == y)
                                    && (F.rgetField @ET.Office r == o)
                                    && (F.rgetField @ET.PrefType r == ET.PSByVAP)
                             ) vpvPostStratifiedByASE
        return ()                     
        _ <- K.addHvega Nothing Nothing
             $ BR.vlByCD @DemVPV (tStart <> " District VPV (Voting Age Pop, Post-Stratified by Age, Sex, Race)") cs vc
             $ F.filterFrame (\r -> (F.rgetField @BR.Year r == y)
                               && (F.rgetField @ET.Office r == o)
                               && (F.rgetField @ET.PrefType r == ET.PSByVAP)
                             ) vpvPostStratifiedByASR
        _ <- K.addHvega Nothing Nothing
          $ BR.vlByCD @DemVPV (tStart <> " District VPV (Voted, Post-Stratified by Age, Sex, Education)") cs vc
          $ F.filterFrame (\r -> (F.rgetField @BR.Year r == 2016)
                                 && (F.rgetField @ET.Office r == ET.President)
                                 && (F.rgetField @ET.PrefType r == ET.PSByVoted)
                          ) vpvPostStratifiedByASE
        _ <- K.addHvega Nothing Nothing
          $ BR.vlByCD @DemVPV (tStart <> " District VPV (Voted, Post-Stratified by Age, Sex, Race)") cs vc
          $ F.filterFrame (\r -> (F.rgetField @BR.Year r == 2016)
                            && (F.rgetField @ET.Office r == ET.President)
                            && (F.rgetField @ET.PrefType r == ET.PSByVoted)
                          ) vpvPostStratifiedByASR
        return ()
  plotEmAll 2016 ET.President 0.25
  plotEmAll 2018 ET.House 0.35
  let aseDemoF = FMR.concatFold $ FMR.mapReduceFold
              (FMR.unpackFilterRow ((== 2018) . F.rgetField @BR.Year))
              (FMR.assignKeysAndData @CatColsASE @[BR.ACSCount, BR.VotedPctOfAll, DemVPV, BR.DemPref])
              (FMR.foldAndAddKey foldPrefAndTurnoutData)
  let asrDemoF = FMR.concatFold $ FMR.mapReduceFold
              (FMR.unpackFilterRow ((== 2018) . F.rgetField @BR.Year))
              (FMR.assignKeysAndData @CatColsASR @[BR.ACSCount, BR.VotedPctOfAll, DemVPV, BR.DemPref])
              (FMR.foldAndAddKey foldPrefAndTurnoutData)              
      asrSums = FL.fold asrDemoF asrTurnoutAndPrefs
      aseSums = FL.fold aseDemoF aseTurnoutAndPrefs
  K.logLE K.Info $ T.intercalate "\n" $ fmap (T.pack . show) $ FL.fold FL.list aseSums
  K.logLE K.Info $ T.intercalate "\n" $ fmap (T.pack . show) $ FL.fold FL.list asrSums
  brAddMarkDown brReadMore

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
import qualified BlueRipple.Data.DemographicTypes as BR
import qualified BlueRipple.Data.HouseElectionTotals as BR
import qualified BlueRipple.Data.PrefModel as BR
import qualified BlueRipple.Data.PrefModel.SimpleAgeSexEducation as BR
import qualified BlueRipple.Model.TurnoutAdjustment as BR
import qualified BlueRipple.Model.MRP_Pref as BR

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


post :: forall es r.(K.KnitOne r
        , K.Members es r
        , K.Member GLM.RandomFu r
        )
     => K.Cached es [BR.ASEDemographics]
     -> K.Cached es [BR.ASRDemographics]
     -> K.Cached es [BR.TurnoutASE]
     -> K.Cached es [BR.TurnoutASR]
     -> K.Cached es [BR.StateTurnout]
     -> K.Cached es [F.Record CCES_MRP]
     -> K.Sem r ()
post aseDemoCA asrDemoCA aseTurnoutCA asrTurnoutCA stateTurnoutCA ccesRecordListAllCA = P.mapError BR.glmErrorToPandocError $ K.wrapPrefix "Wisconsin" $ do
  let stateAbbr = "WI"
      stateOnly = F.filterFrame (\r -> F.rgetField @BR.StateAbbreviation r == stateAbbr)      
  aseACSRaw <- P.raise $ K.useCached aseDemoCA
  asrACSRaw <- P.raise $ K.useCached asrDemoCA
  aseTurnoutRaw <- P.raise $ K.useCached aseTurnoutCA
  asrTurnoutRaw <- P.raise $ K.useCached asrTurnoutCA
  stateTurnoutRaw <- P.raise $ K.useCached stateTurnoutCA
  aseACS <- K.retrieveOrMakeTransformed (fmap FS.toS . FL.fold FL.list) (F.toFrame . fmap FS.fromS) "mrp/acs_simpleASE.bin"
            $ K.logLE K.Diagnostic "re-keying aseACS" >> (K.knitEither $ FL.foldM BR.simplifyACS_ASEFold aseACSRaw)

  asrACS <- K.retrieveOrMakeTransformed (fmap FS.toS . FL.fold FL.list) (F.toFrame . fmap FS.fromS) "mrp/acs_simpleASR.bin"
            $   K.logLE K.Diagnostic "re-keying asrACS" >> (K.knitEither $ FL.foldM BR.simplifyACS_ASRFold asrACSRaw)
            
  aseTurnout <- K.retrieveOrMakeTransformed (fmap FS.toS . FL.fold FL.list) (F.toFrame . fmap FS.fromS) "mrp/turnout_simpleASE.bin"
                $   K.logLE K.Diagnostic "re-keying aseTurnout" >> (K.knitEither $ FL.foldM BR.simplifyTurnoutASEFold aseTurnoutRaw)

  asrTurnout <- K.retrieveOrMakeTransformed (fmap FS.toS . FL.fold FL.list) (F.toFrame . fmap FS.fromS) "mrp/turnout_simpleASR.bin"
                $   K.logLE K.Diagnostic "re-keying asrTurnout" >> (K.knitEither $ FL.foldM BR.simplifyTurnoutASRFold asrTurnoutRaw)
  
  let showRecs = T.intercalate "\n" . fmap (T.pack . show) . FL.fold FL.list        
      countDemHouseVotesF y =  BR.weightedCountFold @ByCCESPredictors @CCES_MRP @'[HouseVoteParty,CCESWeightCumulative]
                           (\r -> (F.rgetField @BR.Year r == y)
                                  && (F.rgetField @HouseVoteParty r `elem` [VP_Republican, VP_Democratic]))
                           ((== VP_Democratic) . F.rgetField @HouseVoteParty)
                           (F.rgetField @CCESWeightCumulative)
      countDemPres2008VotesF =  BR.weightedCountFold @ByCCESPredictors @CCES_MRP @'[Pres2008VoteParty,CCESWeightCumulative]
                                (\r -> (F.rgetField @BR.Year r == 2008)
                                   && (F.rgetField @Pres2008VoteParty r `elem` [VP_Republican, VP_Democratic]))
                            ((== VP_Democratic) . F.rgetField @Pres2008VoteParty)
                              (F.rgetField @CCESWeightCumulative)
      countDemPres2012VotesF =  BR.weightedCountFold @ByCCESPredictors @CCES_MRP @'[Pres2012VoteParty,CCESWeightCumulative]
                              (\r -> (F.rgetField @BR.Year r == 2012)
                                     && (F.rgetField @Pres2012VoteParty r `elem` [VP_Republican, VP_Democratic]))
                                ((== VP_Democratic) . F.rgetField @Pres2012VoteParty)
                              (F.rgetField @CCESWeightCumulative)
      countDemPres2016VotesF =  BR.weightedCountFold @ByCCESPredictors @CCES_MRP @'[Pres2016VoteParty,CCESWeightCumulative]
                              (\r -> (F.rgetField @BR.Year r == 2016)
                                     && (F.rgetField @Pres2016VoteParty r `elem` [VP_Republican, VP_Democratic]))
                              ((== VP_Democratic) . F.rgetField @Pres2016VoteParty)
                              (F.rgetField @CCESWeightCumulative)
      vpv x = 2*x - 1
      lhToRecs year office (LocationHolder lp lkM predMap) =
        let addCols p = FT.mutate (const $ FT.recordSingleton @BR.DemPref p) .
                        FT.mutate (const $ FT.recordSingleton @DemVPV (vpv p)) .
                        FT.mutate (const $ FT.recordSingleton @Office office).
                        FT.mutate (const $ FT.recordSingleton @BR.Year year)                        
            g lkM = let lk = fromMaybe (lp F.&: V.RNil) lkM in fmap (\(ck,p) -> addCols p (lk `V.rappend` ck )) $ M.toList predMap
        in g lkM
      lhsToFrame y o = F.toFrame . concat . fmap (lhToRecs y o) 
      doASER = do
        K.logLE K.Info "Doing ASER MR..."
        let cacheIt cn fa = K.retrieveOrMakeTransformed (fmap FS.toS . FL.fold FL.list) (F.toFrame . fmap FS.fromS) cn fa
        let predictorsASER = [GLM.Intercept
                             , GLM.Predictor P_Sex
                             , GLM.Predictor P_Age
                             , GLM.Predictor P_Education
                             , GLM.Predictor P_Race
                             ]
        predsByLocationPres2008 <- cacheIt "mrp/tmp/pres2008" (lhsToFrame 2008 President <$> P.raise (predictionsByLocation ccesRecordListAllCA countDemPres2008VotesF predictorsASER catPredMapASER))
        predsByLocationPres2012 <- cacheIt "mrp/tmp/pres2012" (lhsToFrame 2012 President <$> P.raise (predictionsByLocation ccesRecordListAllCA countDemPres2012VotesF predictorsASER catPredMapASER))
        predsByLocationPres2016 <- cacheIt "mrp/tmp/pres2016" (lhsToFrame 2016 President <$> P.raise (predictionsByLocation ccesRecordListAllCA countDemPres2016VotesF predictorsASER catPredMapASER))
        predsByLocationHouse <- traverse (\y -> cacheIt ("mrp/tmp/house" <> T.pack (show y)) (lhsToFrame y House <$> P.raise (predictionsByLocation ccesRecordListAllCA (countDemHouseVotesF y) predictorsASER catPredMapASER))) [2008,2010,2012,2014,2016,2018]
        return $ predsByLocationPres2008 <> predsByLocationPres2012 <> predsByLocationPres2016 <> mconcat predsByLocationHouse
      doASE = do
        K.logLE K.Info "Doing ASE MR..."
        let cacheIt cn fa = K.retrieveOrMakeTransformed (fmap FS.toS . FL.fold FL.list) (F.toFrame . fmap FS.fromS) cn fa
        let predictorsASE = [GLM.Intercept
                             , GLM.Predictor P_Age
                             , GLM.Predictor P_Sex
                             , GLM.Predictor P_Education
                             ]
        predsByLocationPres2008 <- cacheIt "mrp/tmp/ase/pres2008" (lhsToFrame 2008 President <$> P.raise (predictionsByLocation ccesRecordListAllCA countDemPres2008VotesF predictorsASE catPredMapASE))
        predsByLocationPres2012 <- cacheIt "mrp/tmp/ase/pres2012" (lhsToFrame 2012 President <$> P.raise (predictionsByLocation ccesRecordListAllCA countDemPres2012VotesF predictorsASE catPredMapASE))
        predsByLocationPres2016 <- cacheIt "mrp/tmp/ase/pres2016" (lhsToFrame 2016 President <$> P.raise (predictionsByLocation ccesRecordListAllCA countDemPres2016VotesF predictorsASE catPredMapASE))
        predsByLocationHouse <- traverse (\y -> cacheIt ("mrp/tmp/ase/house" <> T.pack (show y)) (lhsToFrame y House <$> P.raise (predictionsByLocation ccesRecordListAllCA (countDemHouseVotesF y) predictorsASE catPredMapASE))) [2008,2010,2012,2014,2016,2018]
        return $ predsByLocationPres2008 <> predsByLocationPres2012 <> predsByLocationPres2016 <> mconcat predsByLocationHouse
      doASR = do
        K.logLE K.Info "Doing ASR MR..."
        let cacheIt cn fa = K.retrieveOrMakeTransformed (fmap FS.toS . FL.fold FL.list) (F.toFrame . fmap FS.fromS) cn fa
        let predictorsASR = [GLM.Intercept
                             , GLM.Predictor P_Age
                             , GLM.Predictor P_Sex
                             , GLM.Predictor P_Race
                             ]
        predsByLocationPres2008 <- cacheIt "mrp/tmp/asr/pres2008" (lhsToFrame 2008 President <$> P.raise (predictionsByLocation ccesRecordListAllCA countDemPres2008VotesF predictorsASR catPredMapASR))
        predsByLocationPres2012 <- cacheIt "mrp/tmp/asr/pres2012" (lhsToFrame 2012 President <$> P.raise (predictionsByLocation ccesRecordListAllCA countDemPres2012VotesF predictorsASR catPredMapASR))
        predsByLocationPres2016 <- cacheIt "mrp/tmp/asr/pres2016" (lhsToFrame 2016 President <$> P.raise (predictionsByLocation ccesRecordListAllCA countDemPres2016VotesF predictorsASR catPredMapASR))
        predsByLocationHouse <- traverse (\y -> cacheIt ("mrp/tmp/asr/house" <> T.pack (show y)) (lhsToFrame y House <$> P.raise (predictionsByLocation ccesRecordListAllCA (countDemHouseVotesF y) predictorsASR catPredMapASR))) [2008,2010,2012,2014,2016,2018]
        return $ predsByLocationPres2008 <> predsByLocationPres2012 <> predsByLocationPres2016 <> mconcat predsByLocationHouse        
  inferredPrefsASER <-  K.retrieveOrMakeTransformed (fmap FS.toS . FL.fold FL.list) (F.toFrame . fmap FS.fromS) "mrp/simpleASER_MR.bin" doASER
  inferredPrefsASE <-  K.retrieveOrMakeTransformed (fmap FS.toS . FL.fold FL.list) (F.toFrame . fmap FS.fromS) "mrp/simpleASE_MR.bin" doASE
  inferredPrefsASR <-  K.retrieveOrMakeTransformed (fmap FS.toS . FL.fold FL.list) (F.toFrame . fmap FS.fromS) "mrp/simpleASR_MR.bin" doASR
  -- post-stratify 
  brAddMarkDown text1
  _ <- K.addHvega Nothing Nothing $ BR.vlPrefVsTime "Test" "WI" (FV.ViewConfig 800 800 10) $ fmap F.rcast inferredPrefsASER
  brAddMarkDown brReadMore


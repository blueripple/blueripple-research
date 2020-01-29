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
import           Data.Maybe (isJust, catMaybes)
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
import qualified Polysemy.Error                as P (mapError)
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
import           Numeric.GLM.MixedModel        as GLM
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
import MRP.DeltaVPV hiding (post)

import qualified PreferenceModel.Common as PrefModel
import qualified BlueRipple.Data.Keyed as BR

text1 :: T.Text
text1 = [i|
|]

type LocationCols = '[BR.StateAbbreviation]
type CatCols = '[BR.SexC, BR.CollegeGradC, BR.SimpleAgeC, BR.SimpleRaceC]

post :: (K.KnitOne r
        , K.Members es r
        )
     => K.Cached es [BR.ASEDemographics]
     -> K.Cached es [BR.ASRDemographics]
     -> K.Cached es [BR.TurnoutASE]
     -> K.Cached es [BR.TurnoutASR]
     -> K.Cached es [BR.StateTurnout]
     -> K.Cached es [F.Record CCES_MRP]
     -> K.Sem r ()
post aseDemoCA  asrDemoCA aseTurnoutCA asrTurnoutCA stateTurnoutCA ccesMRP = K.wrapPrefix "Wisconsin" $ do
  let stateAbbr = "WI"
      stateOnly = F.filterFrame (\r -> F.rgetField @BR.StateAbbreviation r == stateAbbr)      
  aseACSRaw <- K.useCached aseDemoCA
  asrACSRaw <- K.useCached asrDemoCA
  aseTurnoutRaw <- K.useCached aseTurnoutCA
  asrTurnoutRaw <- K.useCached asrTurnoutCA
  stateTurnoutRaw <- K.useCached stateTurnoutCA
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
                           (\r -> (F.rgetField @Turnout r == T_Voted)
                                  && (F.rgetField @BR.Year r == y)
                                  && (F.rgetField @HouseVoteParty r `elem` [VP_Republican, VP_Democratic]))
                           ((== VP_Democratic) . F.rgetField @HouseVoteParty)
                           (F.rgetField @CCESWeightCumulative)
      countDemPres2008VotesF =  BR.weightedCountFold @ByCCESPredictors @CCES_MRP @'[Pres2008VoteParty,CCESWeightCumulative]
                              (\r -> (F.rgetField @Turnout r == T_Voted)
                                     && (F.rgetField @BR.Year r == 2008)
                                     && (F.rgetField @Pres2008VoteParty r `elem` [VP_Republican, VP_Democratic]))
                            ((== VP_Democratic) . F.rgetField @Pres2008VoteParty)
                              (F.rgetField @CCESWeightCumulative)
      counDemtPres2012VotesF =  BR.weightedCountFold @ByCCESPredictors @CCES_MRP @'[Pres2012VoteParty,CCESWeightCumulative]
                              (\r -> (F.rgetField @Turnout r == T_Voted)
                                     && (F.rgetField @BR.Year r == 2012)
                                     && (F.rgetField @Pres2012VoteParty r `elem` [VP_Republican, VP_Democratic]))
                              ((== VP_Democratic) . F.rgetField @Pres2012VoteParty)
                              (F.rgetField @CCESWeightCumulative)
      countDemPres2016VotesF =  BR.weightedCountFold @ByCCESPredictors @CCES_MRP @'[Pres2016VoteParty,CCESWeightCumulative]
                              (\r -> (F.rgetField @Turnout r == T_Voted)
                                     && (F.rgetField @BR.Year r == 2016)
                                     && (F.rgetField @Pres2016VoteParty r `elem` [VP_Republican, VP_Democratic]))
                              ((== VP_Democratic) . F.rgetField @Pres2016VoteParty)
                              (F.rgetField @CCESWeightCumulative)
      states = FL.fold FL.set $ fmap (F.rgetField @BR.StateAbbreviation) ccesFrameAll
      predictionsByLocation countFold = do
        ccesFrameAll <- F.toFrame <$> P.raise (K.useCached ccesRecordListAllCA)
        (mm2016p, rc2016p, ebg2016p, bu2016p, vb2016p, bs2016p) <- BR.inferMR @LocationCols @CatCols @[BR.StateAbbreviation
                                                                                                      ,BR.SexC
                                                                                                      ,BR.SimpleRaceC
                                                                                                      ,BR.CollegeGradC
                                                                                                      ,BR.SimpleAgeC]
                                                                   countFold 
                                                                   [GLM.Intercept
                                                                   , GLM.Predictor P_Sex
                                                                   , GLM.Predictor P_Age
                                                                   , GLM.Predictor P_Education
                                                                   , GLM.Predictor P_Race
                                                                   ]
                                                                   ccesPredictor
                                                                   ccesFrameAll

            let allStateKeys = fmap (\s -> s F.&: V.RNil) $ FL.fold FL.list states            
                predictLoc l = LocationHolder (locKeyPretty l) (Just l) catPredMaps
                toPredict = [LocationHolder "National" Nothing catPredMaps] <> fmap predictLoc allStateKeys                           
                predict (LocationHolder n lkM cpms) = P.mapError glmErrorToPandocError $ do
                  let predictFrom catKey predMap =
                        let groupKeyM = lkM >>= \lk -> return $ lk `V.rappend` catKey
                            emptyAsNationalGKM = case groupKeyM of
                              Nothing -> Nothing
                              Just k -> fmap (const k) $ GLM.categoryNumberFromKey rc2016p k Proxy
                        in GLM.predictFromBetaUB mm2016p (flip M.lookup predMap) (const emptyAsNationalGKM) rc2016p ebg2016p bu2016p vb2016p     
                  cpreds <- M.traverseWithKey predictFrom cpms
                  return $ LocationHolder n lkM cpreds
        traverse predict toPredict
      vpv x = 2*x - 1
      lhToRecs year office = (LocationHolder _ lkM predMap) =
        let addCols p = FT.mutate (const $ FT.recordSingleton @DemPref p) .
                        FT.mutate (const $ FT.recordSingleton @DemVPV (vpv p)) .
                        FT.mutate (const $ FT.recordSingleton @Office office).
                        FT.mutate (const $ FT.recordSingleton @BR.Year year)                        
            g lk = fmap (\(ck,p) -> addCols p (lk `V.rappend` ck )) $ M.toList predMap
        in fmap g lkM
      doMR = do        
        predsByLocationPres2008 <- fmap (lhToRecs 2008 President) <$> predictionsByLocation countDemPres2008VotesF
        predsByLocationPres2012 <- predictionsByLocation countDemPres2012VotesF
        predsByLocationPres2016 <- predictionsByLocation countDemPres2016VotesF
        predsByLocationHouse <- traverse (\y <- fmap (y,) $ predictionsByLocation countDemHouseVotes y) [2008,2010,2012,2014,2016,2018]
        
  brAddMarkDown text1
  brAddMarkDown brReadMore


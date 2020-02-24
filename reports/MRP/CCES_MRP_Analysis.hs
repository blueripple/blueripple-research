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

module MRP.CCES_MRP_Analysis where

import qualified Control.Foldl                 as FL
import           Control.Monad                  ( join )
import qualified Data.Array                    as A
import           Data.Function                  ( on )
import qualified Data.List                     as L
import qualified Data.Set                      as S
import qualified Data.Map                      as M
import           Data.Maybe                     ( isJust
                                                , catMaybes
                                                , fromMaybe
                                                )
import           Data.Proxy                     ( Proxy(..) )
--import  Data.Ord (Compare)

import qualified Data.Text                     as T
import qualified Data.Serialize                as SE
import qualified Data.Vector                   as V
import qualified Data.Vector.Storable          as VS


import           Graphics.Vega.VegaLite.Configuration
                                               as FV
import qualified Graphics.Vega.VegaLite.Compat as FV
import qualified Frames                        as F
import qualified Frames.Melt                   as F
import qualified Frames.InCore                 as FI
import qualified Data.Vinyl                    as V
import qualified Data.Vinyl.TypeLevel          as V


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
import qualified Polysemy.Error                as P
                                                ( mapError
                                                , Error
                                                )
import qualified Polysemy                      as P
                                                ( raise )
import           Text.Pandoc.Error             as PE
import qualified Text.Blaze.Colonnade          as BC

import           Data.String.Here               ( here
                                                , i
                                                )

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
import qualified Numeric.GLM.ModelTypes        as GLM
import qualified Numeric.GLM.FunctionFamily    as GLM
import qualified Numeric.GLM.MixedModel        as GLM
import qualified Numeric.GLM.Bootstrap         as GLM
import qualified Numeric.GLM.Report            as GLM
import qualified Numeric.GLM.Predict           as GLM
import qualified Numeric.GLM.Confidence        as GLM
import qualified Numeric.SparseDenseConversions
                                               as SD

import qualified Statistics.Types              as ST
import           GHC.Generics                   ( Generic
                                                , Rep
                                                )


import qualified BlueRipple.Data.DataFrames    as BR
import qualified BlueRipple.Data.DemographicTypes
                                               as BR
import qualified BlueRipple.Data.ElectionTypes as ET
import qualified BlueRipple.Data.HouseElectionTotals
                                               as BR
import qualified BlueRipple.Data.PrefModel     as BR
import qualified BlueRipple.Data.PrefModel.SimpleAgeSexEducation
                                               as BR
import qualified BlueRipple.Model.TurnoutAdjustment
                                               as BR
import qualified BlueRipple.Model.MRP_Pref     as BR
import qualified BlueRipple.Model.PostStratify as BR
import qualified BlueRipple.Data.UsefulDataJoins
                                               as BR

import qualified BlueRipple.Utilities.KnitUtils
                                               as BR
import           MRP.Common
import           MRP.CCES
import           MRP.DeltaVPV                   ( DemVPV )

import qualified PreferenceModel.Common        as PrefModel
import qualified BlueRipple.Data.Keyed         as BR

import qualified Visualizations.StatePrefs     as BR

countDemHouseVotesF y =
  BR.weightedCountFold @ByCCESPredictors @CCES_MRP
    @'[HouseVoteParty, CCESWeightCumulative]
    (\r ->
      (F.rgetField @BR.Year r == y)
        && (F.rgetField @HouseVoteParty r `elem` [ET.Republican, ET.Democratic])
    )
    ((== ET.Democratic) . F.rgetField @HouseVoteParty)
    (F.rgetField @CCESWeightCumulative)

countDemPres2008VotesF =
  BR.weightedCountFold @ByCCESPredictors @CCES_MRP
    @'[Pres2008VoteParty, CCESWeightCumulative]
    (\r ->
      (F.rgetField @BR.Year r == 2008)
        && (      F.rgetField @Pres2008VoteParty r
           `elem` [ET.Republican, ET.Democratic]
           )
    )
    ((== ET.Democratic) . F.rgetField @Pres2008VoteParty)
    (F.rgetField @CCESWeightCumulative)

countDemPres2012VotesF =
  BR.weightedCountFold @ByCCESPredictors @CCES_MRP
    @'[Pres2012VoteParty, CCESWeightCumulative]
    (\r ->
      (F.rgetField @BR.Year r == 2012)
        && (      F.rgetField @Pres2012VoteParty r
           `elem` [ET.Republican, ET.Democratic]
           )
    )
    ((== ET.Democratic) . F.rgetField @Pres2012VoteParty)
    (F.rgetField @CCESWeightCumulative)

countDemPres2016VotesF =
  BR.weightedCountFold @ByCCESPredictors @CCES_MRP
    @'[Pres2016VoteParty, CCESWeightCumulative]
    (\r ->
      (F.rgetField @BR.Year r == 2016)
        && (      F.rgetField @Pres2016VoteParty r
           `elem` [ET.Republican, ET.Democratic]
           )
    )
    ((== ET.Democratic) . F.rgetField @Pres2016VoteParty)
    (F.rgetField @CCESWeightCumulative)

mrpPrefs
  :: forall cc r
   . ( K.KnitEffects r
     , K.Member GLM.RandomFu r
     , ( (((cc V.++ '[BR.Year]) V.++ '[ET.Office]) V.++ '[DemVPV])
           V.++
           '[BR.DemPref]
       )
         ~
         (cc V.++ '[BR.Year, ET.Office, DemVPV, BR.DemPref])
     , FS.RecSerialize (cc V.++ '[BR.Year, ET.Office, DemVPV, BR.DemPref])
     , FI.RecVec (cc V.++ '[BR.Year, ET.Office, DemVPV, BR.DemPref])
     , V.RMap (cc V.++ '[BR.Year, ET.Office, DemVPV, BR.DemPref])
     , cc F.⊆ (LocationCols V.++ ASER V.++ BR.CountCols)
     , V.RMap cc
     , V.ReifyConstraint Show V.ElField cc
     , V.RecordToList cc
     , Ord (F.Record cc)
     )
  => Maybe T.Text
  -> K.Sem r (F.FrameRec CCES_MRP)
  -> [GLM.WithIntercept CCESPredictor]
  -> M.Map (F.Record cc) (M.Map CCESPredictor Double)
  -> K.Sem
       r
       ( F.FrameRec
           ( '[BR.StateAbbreviation]
               V.++
               cc
               V.++
               '[BR.Year, ET.Office, DemVPV, BR.DemPref]
           )
       )
mrpPrefs cacheTmpDirM ccesDataAction predictor catPredMap = do
  let vpv x = 2 * x - 1
      lhToRecs year office (LocationHolder lp lkM predMap) =
        let addCols p =
              FT.mutate (const $ FT.recordSingleton @BR.DemPref p)
                . FT.mutate (const $ FT.recordSingleton @DemVPV (vpv p))
                . FT.mutate (const $ FT.recordSingleton @ET.Office office)
                . FT.mutate (const $ FT.recordSingleton @BR.Year year)
            g lkM =
              let lk = fromMaybe (lp F.&: V.RNil) lkM
              in  fmap (\(ck, p) -> addCols p (lk `V.rappend` ck))
                    $ M.toList predMap
        in  g lkM
      lhsToFrame y o = F.toFrame . concat . fmap (lhToRecs y o)
  K.logLE K.Info "Doing ASER MR..."
  let cacheIt cn fa = case cacheTmpDirM of
        Nothing -> fa
        Just tmpDir -> K.retrieveOrMakeTransformed
                         (fmap FS.toS . FL.fold FL.list)
                         (F.toFrame . fmap FS.fromS)
                         ("mrp/tmp/" <> tmpDir <> "/" <> cn)
                         fa
  predsByLocationPres2008 <- cacheIt
    "pres2008"
    (   lhsToFrame 2008 ET.President
    <$> (predictionsByLocation ccesDataAction
                               countDemPres2008VotesF
                               predictor
                               catPredMap
        )
    )
  predsByLocationPres2012 <- cacheIt
    "pres2012"
    (   lhsToFrame 2012 ET.President
    <$> (predictionsByLocation ccesDataAction
                               countDemPres2012VotesF
                               predictor
                               catPredMap
        )
    )
  predsByLocationPres2016 <- cacheIt
    "pres2016"
    (   lhsToFrame 2016 ET.President
    <$> (predictionsByLocation ccesDataAction
                               countDemPres2016VotesF
                               predictor
                               catPredMap
        )
    )
  predsByLocationHouse <- traverse
    (\y -> cacheIt
      ("house" <> T.pack (show y))
      (   lhsToFrame y ET.House
      <$> (predictionsByLocation ccesDataAction
                                 (countDemHouseVotesF y)
                                 predictor
                                 catPredMap
          )
      )
    )
    [2008, 2010, 2012, 2014, 2016, 2018]
  return
    $  predsByLocationPres2008
    <> predsByLocationPres2012
    <> predsByLocationPres2016
    <> mconcat predsByLocationHouse

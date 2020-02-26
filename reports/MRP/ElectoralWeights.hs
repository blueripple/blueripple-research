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

module MRP.ElectoralWeights where

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
import qualified BlueRipple.Utilities.TableUtils as BR
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

import qualified Data.Time.Calendar            as Time
import qualified Data.Time.Clock               as Time
import qualified Data.Time.Format              as Time

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
When pollsters or academics go from estimating voter preference to electoral outcomes, things get
complicated.  Voter preference is complicated too!  But the approach is straightforward:
take survey data or voter-file data or some data that has individual preference and
use that to infer voter preference for a group of people.  There are many choices
for how to group the data: by demographic charactersitics?  by location?  both?

But let's suppose we accept the preferences, however derived.  How do we go from those
to a modeled election result?  To do that we need a model of the *electorate*, that is
how many people of each type (in the country or each state or district) are likely to vote
in the election.  That question is usually broken down into two parts:
knowing the demographics of the country/state/region, that is knowing how many people
are in each group and then knowing how many of those people will vote on election day.
The number of people in each group is usually straightforwardly derived from census data.
But the second part---what fraction of each group will vote, aka the electoral weights---
that's extremely hard to figure out.

There are a number of standard approaches:

1. Ask them! Some surveys ask the respondent if they intend to vote and then use that
(or some modeled fraction of that) to create a set of weights with as much specificity
as the survey allows.

2. Assume the next election will be like the last election:
- The census tracks nationwide demographically split turnout for previous
elections and we can use those weights, though they lack any geographic specificity.
- Some surveys match respondents with voter-file data and thus turnout.  These can then be used
to build a set of weights, however specific the survey allows.

3. Model the electorate using some combination of the above data---previous elections,
surveys---and whatever other variables, e.g., negative partisanship of a district,
to predict how the current electorate might differ from the past.

Rather than opine on the best model, which is not our expertise,
in this piece we want to show how important the choice of electoral weights turns out to be.
We want to help you become a more informed reader of polls and academic work
which predicts electoral results.

This piece was partially inspired by a recent [Vox article][Vox:BernieYouth], which used
a novel survey to look at how each candidate in the democratic primary might fare against
Donald Trump.

[Vox:BernieYouth]: <https://www.vox.com/policy-and-politics/2020/2/25/21152538/bernie-sanders-electability-president-moderates-data>
|]
  
type CatColsASER = '[BR.SimpleAgeC, BR.SexC, BR.CollegeGradC, BR.SimpleRaceC]
catKeyASER :: BR.SimpleAge -> BR.Sex -> BR.CollegeGrad -> BR.SimpleRace -> F.Record CatColsASER
catKeyASER a s e r = a F.&: s F.&: e F.&: r F.&: V.RNil

predMapASER :: F.Record CatColsASER -> M.Map CCESPredictor Double
predMapASER r = M.fromList [(P_Sex, if F.rgetField @BR.SexC r == BR.Female then 0 else 1)
                       ,(P_Race, if F.rgetField @BR.SimpleRaceC r == BR.NonWhite then 0 else 1)
                       ,(P_Education, if F.rgetField @BR.CollegeGradC r == BR.NonGrad then 0 else 1)
                       ,(P_Age, if F.rgetField @BR.SimpleAgeC r == BR.EqualOrOver then 0 else 1)
                       ]

allCatKeysASER = [catKeyASER a s e r | a <- [BR.EqualOrOver, BR.Under], e <- [BR.NonGrad, BR.Grad], s <- [BR.Female, BR.Male], r <- [BR.NonWhite, BR.White]]


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
predMapASR r = M.fromList [(P_Age, if F.rgetField @BR.SimpleAgeC r == BR.EqualOrOver then 0 else 1)
                          ,(P_Sex, if F.rgetField @BR.SexC r == BR.Female then 0 else 1)
                          ,(P_Race, if F.rgetField @BR.SimpleRaceC r == BR.NonWhite then 0 else 1)
                          ]

allCatKeysASR = [catKeyASR a s r | a <- [BR.EqualOrOver, BR.Under], s <- [BR.Female, BR.Male], r <- [BR.NonWhite, BR.White]]

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

votesToVoteShareF :: FL.Fold (F.Record [ET.Party, ET.Votes]) (F.Record '[ET.PrefType, BR.DemPref])
votesToVoteShareF =
  let
    party = F.rgetField @ET.Party
    votes = F.rgetField @ET.Votes
    demVotesF = FL.prefilter (\r -> party r == ET.Democratic) $ FL.premap votes FL.sum
    demRepVotesF = FL.prefilter (\r -> let p = party r in (p == ET.Democratic || p == ET.Republican)) $ FL.premap votes FL.sum
    demPref d dr = if dr > 0 then realToFrac d/realToFrac dr else 0
    demPrefF = demPref <$> demVotesF <*> demRepVotesF
  in fmap (\x -> FT.recordSingleton ET.VoteShare `V.rappend` FT.recordSingleton @BR.DemPref x) demPrefF


post :: forall r.(K.KnitMany r, K.Member GLM.RandomFu r) => Bool -> K.Sem r ()
post updated = P.mapError BR.glmErrorToPandocError $ K.wrapPrefix "TurnoutScenarios" $ do
  stateTurnoutRaw <- BR.stateTurnoutLoader 
  aseACS <- BR.simpleASEDemographicsLoader 
  asrACS <- BR.simpleASRDemographicsLoader 
  aseTurnout <- BR.simpleASETurnoutLoader 
  asrTurnout <- BR.simpleASRTurnoutLoader 
--  logFrame aseTurnout
  let predictorsASER = [GLM.Intercept, GLM.Predictor P_Sex , GLM.Predictor P_Age, GLM.Predictor P_Education, GLM.Predictor P_Race]
      predictorsASE = [GLM.Intercept, GLM.Predictor P_Age , GLM.Predictor P_Sex, GLM.Predictor P_Education]
      predictorsASR = [GLM.Intercept, GLM.Predictor P_Age , GLM.Predictor P_Sex, GLM.Predictor P_Race]
  inferredPrefsASER <-  BR.retrieveOrMakeFrame "mrp/simpleASER_MR.bin"
                        (P.raise $ BR.mrpPrefs @CatColsASER (Just "ASER") ccesDataLoader predictorsASER catPredMapASER) 
  inferredPrefsASE <-  BR.retrieveOrMakeFrame "mrp/simpleASE_MR.bin"
                       (P.raise $ BR.mrpPrefs @CatColsASE (Just "ASE") ccesDataLoader predictorsASE catPredMapASE) 
  inferredPrefsASR <-  BR.retrieveOrMakeFrame "mrp/simpleASR_MR.bin"
                       (P.raise $ BR.mrpPrefs @CatColsASR (Just "ASR") ccesDataLoader predictorsASR catPredMapASR) 

  -- get adjusted turnouts (national rates, adj by state) for each CD
  demographicsAndTurnoutASE <- BR.cachedASEDemographicsWithAdjTurnoutByCD (return aseACS) (return aseTurnout) (return stateTurnoutRaw)
  demographicsAndTurnoutASR <- BR.cachedASRDemographicsWithAdjTurnoutByCD (return asrACS) (return asrTurnout) (return stateTurnoutRaw)
  K.logLE K.Info "Computing pres-election 2-party vote-share"
  presPrefByStateFrame <- do
    let fld = FMR.concatFold $ FMR.mapReduceFold
              MR.noUnpack
              (FMR.assignKeysAndData @[BR.Year, BR.State, BR.StateAbbreviation, BR.StateFIPS, ET.Office])
              (FMR.foldAndAddKey votesToVoteShareF)
    presByStateFrame <- BR.presidentialByStateFrame
    return $ FL.fold fld presByStateFrame    
  K.logLE K.Info "Computing house election 2-party vote share"
  let houseElectionFilter r = (F.rgetField @BR.Stage r == "gen")
                              && (F.rgetField @BR.Runoff r == False)
                              && (F.rgetField @BR.Special r == False)
                              && (F.rgetField @ET.Party r == ET.Democratic || F.rgetField @ET.Party r == ET.Republican)
  houseElectionFrame <- F.filterFrame houseElectionFilter <$> BR.houseElectionsLoader
  let houseVoteShareF = FMR.concatFold $ FMR.mapReduceFold
                        FMR.noUnpack
                        (FMR.assignKeysAndData @[BR.Year, BR.StateAbbreviation, BR.StateFIPS, BR.CongressionalDistrict, ET.Office])
                        (FMR.foldAndAddKey votesToVoteShareF)
      houseVoteShareFrame = FL.fold houseVoteShareF houseElectionFrame
  K.logLE K.Info "Joining turnout by CD and prefs"
  let aseTurnoutAndPrefs = catMaybes
                           $ fmap F.recMaybe
                           $ F.leftJoin @([BR.StateAbbreviation, BR.Year] V.++ CatColsASE) demographicsAndTurnoutASE inferredPrefsASE
      asrTurnoutAndPrefs = catMaybes
                           $ fmap F.recMaybe
                           $ F.leftJoin @([BR.StateAbbreviation, BR.Year] V.++ CatColsASR) demographicsAndTurnoutASR inferredPrefsASR
      -- fold these to state level
  let justPres2016 r = (F.rgetField @BR.Year r == 2016) && (F.rgetField @ET.Office r == ET.President)    
      aseDemoF = FMR.concatFold $ FMR.mapReduceFold
                 (FMR.unpackFilterRow justPres2016)
                 (FMR.assignKeysAndData @(CatColsASE V.++ '[BR.StateAbbreviation, BR.Year, ET.Office]) @[BR.ACSCount, BR.VotedPctOfAll, DemVPV, BR.DemPref])
                 (FMR.foldAndAddKey foldPrefAndTurnoutData)
      asrDemoF = FMR.concatFold $ FMR.mapReduceFold
                 (FMR.unpackFilterRow justPres2016)
                 (FMR.assignKeysAndData @(CatColsASR V.++ '[BR.StateAbbreviation, BR.Year, ET.Office]) @[BR.ACSCount, BR.VotedPctOfAll, DemVPV, BR.DemPref])
                 (FMR.foldAndAddKey foldPrefAndTurnoutData)              
      asrByState = FL.fold asrDemoF asrTurnoutAndPrefs
      aseByState = FL.fold aseDemoF aseTurnoutAndPrefs
      labelPSBy x = V.rappend (FT.recordSingleton @ET.PrefType x)
      psCellVPVByBothF =  (<>)
                          <$> fmap pure (fmap (labelPSBy ET.PSByVAP)
                                         $ BR.postStratifyCell @BR.DemPref
                                         (realToFrac . F.rgetField @BR.ACSCount)
                                         (realToFrac . F.rgetField @BR.DemPref))
                          <*> fmap pure (fmap (labelPSBy ET.PSByVoted)
                                         $ BR.postStratifyCell @BR.DemPref
                                         (\r -> realToFrac (F.rgetField @BR.ACSCount r) * F.rgetField @BR.VotedPctOfAll r)
                                         (realToFrac . F.rgetField @BR.DemPref))
      psVPVByStateF =  BR.postStratifyF
                          @[BR.Year, ET.Office, BR.StateAbbreviation]
                          @[BR.DemPref, BR.ACSCount, BR.VotedPctOfAll]
                          @[ET.PrefType, BR.DemPref]
                          psCellVPVByBothF
      vpvPostStratifiedByASE = fmap (`V.rappend` FT.recordSingleton @BR.DemographicGroupingC BR.ASE) $ FL.fold psVPVByStateF aseByState
      vpvPostStratifiedByASR =  fmap (`V.rappend` FT.recordSingleton @BR.DemographicGroupingC BR.ASR) $ FL.fold psVPVByStateF asrByState
  let vpvPostStratified = vpvPostStratifiedByASE <> vpvPostStratifiedByASR

  curDate <-  (\(Time.UTCTime d _) -> d) <$> K.getCurrentTime
  let pubDateElectoralWeights =  Time.fromGregorian 2020 2 21
  K.newPandoc
    (K.PandocInfo ((postRoute PostElectoralWeights) <> "main")
      (brAddDates updated pubDateElectoralWeights curDate
       $ M.fromList [("pagetitle", "What Are We Talking About When We Talk About Electoral Weights?")
                    ,("title","What Are We Talking About When We Talk About Electoral Weights?")
                    ]
      ))
      $ do        
        brAddMarkDown text1
        brAddMarkDown brReadMore
     


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
import qualified Frames.Constraints            as FC
import qualified Frames.SimpleJoins            as FJ

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
import qualified BlueRipple.Data.ACS_PUMS as PUMS
import qualified BlueRipple.Data.ACS_PUMS_Loaders as PUMS
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
2. Assume the next election will be like the last election. The census tracks nationwide
demographically split turnout for previous elections and we can use those weights,
though they lack any geographic specificity. Also, some surveys match respondents with
voter-file data and thus turnout.  These can then be used to build a set of weights,
however specific the survey allows.
3. Model the electorate using some combination of the above data---previous elections,
surveys---and whatever other variables, e.g., negative partisanship of a district,
to predict how the current electorate might differ from the past.

Rather than opine on the best model, which is not our expertise,
in this piece we want to show how important the choice of electoral weights turns out to be.
We want to help you become a more informed reader of polls and academic work
which predicts electoral results.

This piece was partially inspired by a recent [Vox article][Vox:BernieYouth], which used
a novel survey to look at how each candidate in the democratic primary might fare against
Donald Trump. The paper on which the article is based is interesting and worth a read. But
some of the conclusions of the paper and article are based on using electoral weights derived
from the 2016 election.  It's very hard to evaluate these conclusions without understanding
how sensitive they are to the choice of electoral weights.

So let's explore how much difference these weights can make.  We'll begin by picking a few
possible sets of weights and comparing the projected popular vote and electoral college results
that they predict from the *same set of preferences*---in this case, preferences derived from
our [MRP model][BR:MRP] based on the [CCES][CCES].  Here are the weights we'll explore:

- Census2012: Nationwide weights from the 2012 election, tabulated by the census.
- Census2016: Nationwide weights from the 2016 election, tabulated by the census.
- Census2018: Nationwide weights from the 2018 (mid-term) election, tabulated by the census.
- CCES2016a: Nationwide weights derived from turnout data in the CCES survey itself.
- CCES2016b: State-level weights derived from turnout data in the CCES survey itself.

[BR:MRP]: <https://blueripple.github.io/research/mrp-model/p1/main.html#data-and-methods>
[CCES]: <https://cces.gov.harvard.edu/>
[Vox:BernieYouth]: <https://www.vox.com/policy-and-politics/2020/2/25/21152538/bernie-sanders-electability-president-moderates-data>
|]
  
foldPrefAndTurnoutData :: FF.EndoFold (F.Record '[PUMS.Citizens, ET.ElectoralWeight, DemVPV, BR.DemPref])
foldPrefAndTurnoutData =  FF.sequenceRecFold
                          $ FF.toFoldRecord (FL.premap (F.rgetField @PUMS.Citizens) FL.sum)
                          V.:& FF.toFoldRecord (BR.weightedSumRecF @PUMS.Citizens @ET.ElectoralWeight)
                          V.:& FF.toFoldRecord (BR.weightedSumRecF @PUMS.Citizens @DemVPV)
                          V.:& FF.toFoldRecord (BR.weightedSumRecF @PUMS.Citizens @BR.DemPref)
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
  K.logLE K.Info "Loading re-keyed demographic and turnout data."
  -- state totals and national demographic splits
  stateTurnoutRaw <- BR.stateTurnoutLoader  
  aseTurnout <- BR.simpleASETurnoutLoader 
  asrTurnout <- BR.simpleASRTurnoutLoader

  -- preferences
  let predictorsASER = fmap GLM.Predictor (allCCESSimplePredictors @BR.CatColsASER)
      predictorsASE =  fmap GLM.Predictor (allCCESSimplePredictors @BR.CatColsASE)
      predictorsASR = fmap GLM.Predictor (allCCESSimplePredictors @BR.CatColsASR)
      statesAfter y r = F.rgetField @BR.Year r > y && F.rgetField @BR.StateAbbreviation r /= "National"
  inferredPrefsASER <-  F.filterFrame (statesAfter 2010) <$> BR.retrieveOrMakeFrame "mrp/simpleASER_MR.bin"
                        (P.raise $ BR.mrpPrefs @BR.CatColsASER (Just "ASER") ccesDataLoader predictorsASER catPredMaps) 
  inferredPrefsASE <-  F.filterFrame (statesAfter 2010) <$> BR.retrieveOrMakeFrame "mrp/simpleASE_MR.bin"
                       (P.raise $ BR.mrpPrefs @BR.CatColsASE (Just "ASE") ccesDataLoader predictorsASE catPredMaps) 
  inferredPrefsASR <-  F.filterFrame (statesAfter 2010) <$> BR.retrieveOrMakeFrame "mrp/simpleASR_MR.bin"
                       (P.raise $ BR.mrpPrefs @BR.CatColsASR (Just "ASR") ccesDataLoader predictorsASR catPredMaps) 

  -- inferred turnout
  inferredTurnoutASER <- BR.retrieveOrMakeFrame "mrp/turnout/simpleASER_MR.bin"
                         (P.raise $ BR.mrpTurnout @BR.CatColsASER (Just "T_ASER") ccesDataLoader predictorsASER catPredMaps)
  inferredTurnoutASR <-  BR.retrieveOrMakeFrame "mrp/turnout/simpleASR_MR.bin"
                         (P.raise $ BR.mrpTurnout @BR.CatColsASR (Just "T_ASR") ccesDataLoader predictorsASR catPredMaps)
  inferredTurnoutASE <-  BR.retrieveOrMakeFrame "mrp/turnout/simpleASE_MR.bin"
                         (P.raise $ BR.mrpTurnout @BR.CatColsASE (Just "T_ASE") ccesDataLoader predictorsASE catPredMaps)
--  logFrame inferredTurnoutASER
  -- demographics
  pumsDemographics <- PUMS.pumsLoadAll
  let pumsASRByState = fmap (FT.mutate $ const $ FT.recordSingleton @BR.PopCountOf BR.PC_Citizen)
                       $ FL.fold (PUMS.pumsStateRollupF $ PUMS.pumsKeysToASR) pumsDemographics
      pumsASEByState = fmap (FT.mutate $ const $ FT.recordSingleton @BR.PopCountOf BR.PC_Citizen)
                       $ FL.fold (PUMS.pumsStateRollupF $ PUMS.pumsKeysToASE True) pumsDemographics
      addElectoralWeight :: (F.ElemOf rs BR.Citizen, F.ElemOf rs BR.Voted)
                         => F.Record rs
                         -> F.Record [ET.ElectoralWeightSource, ET.ElectoralWeightOf, ET.ElectoralWeight] 
      addElectoralWeight r = ET.EW_Census F.&: ET.EW_Citizen F.&: (realToFrac $ F.rgetField @BR.Voted r)/(realToFrac $ F.rgetField @BR.Citizen r) F.&: V.RNil
      ewASR = fmap (FT.mutate addElectoralWeight) asrTurnout
      ewASE = fmap (FT.mutate addElectoralWeight) aseTurnout
  K.logLE K.Info "Adjusting turnout via PUMS demographics and census turnout"      
  let asrDemoAndAdjEW_action = BR.demographicsWithAdjTurnoutByState
                        @BR.CatColsASR
                        @PUMS.Citizens
                        @'[PUMS.NonCitizens, BR.PopCountOf, BR.StateFIPS]
                        @'[BR.Year] stateTurnoutRaw (fmap F.rcast pumsASRByState) (fmap F.rcast ewASR)
      aseDemoAndAdjEW_action = BR.demographicsWithAdjTurnoutByState
                        @BR.CatColsASE
                        @PUMS.Citizens
                        @'[PUMS.NonCitizens, BR.PopCountOf, BR.StateFIPS]
                        @'[BR.Year] stateTurnoutRaw (fmap F.rcast pumsASEByState) (fmap F.rcast ewASE)
  asrDemoAndAdjEW <- BR.retrieveOrMakeFrame "turnout/asrPumsDemoAndAdjEW.bin" asrDemoAndAdjEW_action
  aseDemoAndAdjEW <- BR.retrieveOrMakeFrame "turnout/asePumsDemoAndAdjEW.bin" aseDemoAndAdjEW_action


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
  aseAllByState <- BR.retrieveOrMakeFrame "mrp/weights/aseAllByState.bin"
                   (K.knitMaybe "Missing key (stateAbbr, year, catColsASE) when joining inferredPrefsASE and aseDemoAndAdjEW"
                    $ FJ.leftJoinM @('[BR.StateAbbreviation, BR.Year] V.++ BR.CatColsASE) inferredPrefsASE aseDemoAndAdjEW)
  asrAllByState <- BR.retrieveOrMakeFrame "mrp/weights/asrAllByState.bin"
                   (K.knitMaybe "Missing key when joining inferredPrefsASR and asrDemoAndAdjEW"
                    $ FJ.leftJoinM @('[BR.StateAbbreviation, BR.Year] V.++ BR.CatColsASR) inferredPrefsASR asrDemoAndAdjEW)
  -- fold these to state level
  let aseDemoF = FMR.concatFold $ FMR.mapReduceFold
                 FMR.noUnpack
                 (FMR.assignKeysAndData @(BR.CatColsASE V.++ '[BR.StateAbbreviation, BR.Year, ET.Office]) @[PUMS.Citizens, ET.ElectoralWeight, DemVPV, BR.DemPref])
                 (FMR.foldAndAddKey foldPrefAndTurnoutData)
      asrDemoF = FMR.concatFold $ FMR.mapReduceFold
                 FMR.noUnpack
                 (FMR.assignKeysAndData @(BR.CatColsASR V.++ '[BR.StateAbbreviation, BR.Year, ET.Office]) @[PUMS.Citizens, ET.ElectoralWeight, DemVPV, BR.DemPref])
                 (FMR.foldAndAddKey foldPrefAndTurnoutData)              
      asrByState = FL.fold asrDemoF asrAllByState
      aseByState = FL.fold aseDemoF aseAllByState
  electoralVotesByStateFrame <- F.filterFrame ((==2020) . F.rgetField @BR.Year) <$> BR.electoralCollegeFrame
  let yearOfficeFilter y o r =  F.rgetField @BR.Year r == y && F.rgetField @ET.Office r == o

      asrPrefs y o = fmap (F.rcast @('[BR.StateAbbreviation] V.++ BR.CatColsASR V.++ '[BR.DemPref]))
                     $ F.filterFrame (yearOfficeFilter y o) asrByState
      asrDemo y o = fmap (F.rcast @('[BR.StateAbbreviation] V.++ BR.CatColsASR V.++ '[PUMS.Citizens]))
                    $ F.filterFrame (yearOfficeFilter y o) asrByState
      asrCensusEW y o = fmap (F.rcast @('[BR.StateAbbreviation] V.++ BR.CatColsASR V.++ '[ET.ElectoralWeight]))
                        $ F.filterFrame (yearOfficeFilter y o) asrByState                   
      asrWgtdByCensus2012 = mergeASRElectionData
                            (asrDemo 2018 ET.House)
                            (asrPrefs 2016 ET.President)
                            "Census2012"
                            (asrCensusEW 2012 ET.President)
                            electoralVotesByStateFrame
      asrWgtdByCensus2016 = mergeASRElectionData
                            (asrDemo 2018 ET.House)
                            (asrPrefs 2016 ET.President)
                            "Census2016"
                            (asrCensusEW 2016 ET.President)
                            electoralVotesByStateFrame
      asrWgtd = asrWgtdByCensus2012 <> asrWgtdByCensus2016
      
      ewResultsF = FMR.concatFold $ FMR.mapReduceFold
                   FMR.noUnpack
                   (FMR.assignKeysAndData @'[WeightSource])
                   (FMR.foldAndAddKey mergedElectionDataToResultF)
      asrEwResults = FL.fold ewResultsF asrWgtd
--  logFrame asrWgtd
  logFrame asrEwResults
  let asePrefs y o = fmap (F.rcast @('[BR.StateAbbreviation] V.++ BR.CatColsASE V.++ '[BR.DemPref]))
                     $ F.filterFrame (yearOfficeFilter y o) aseByState
      aseDemo y o = fmap (F.rcast @('[BR.StateAbbreviation] V.++ BR.CatColsASE V.++ '[PUMS.Citizens]))
                    $ F.filterFrame (yearOfficeFilter y o) aseByState
      aseCensusEW y o = fmap (F.rcast @('[BR.StateAbbreviation] V.++ BR.CatColsASE V.++ '[ET.ElectoralWeight]))
                        $ F.filterFrame (yearOfficeFilter y o) aseByState
      aseWgtdByCensus2012 = mergeASEElectionData
                            (aseDemo 2018 ET.House)
                            (asePrefs 2016 ET.President)
                            "Census2012"
                            (aseCensusEW 2012 ET.President)
                            electoralVotesByStateFrame
      aseWgtdByCensus2016 = mergeASEElectionData
                            (aseDemo 2018 ET.House)
                            (asePrefs 2016 ET.President)
                            "Census2016"
                            (aseCensusEW 2016 ET.President)
                            electoralVotesByStateFrame
      aseWgtd = aseWgtdByCensus2012 <> aseWgtdByCensus2016
      
      aseEwResults = FL.fold ewResultsF aseWgtd
--  logFrame aseWgtd
  logFrame aseEwResults
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
     
type WeightSource = "WeightSource" F.:-> T.Text
-- I'd like this to be CatCols generic but that leads to a nightmare of constraints
-- maybe a @LeftJoinC@ ?? Tried that.  Still messy.
mergeASRElectionData :: 
  F.FrameRec ('[BR.StateAbbreviation] V.++ BR.CatColsASR V.++  '[PUMS.Citizens])
  -> F.FrameRec ('[BR.StateAbbreviation] V.++ BR.CatColsASR V.++  '[BR.DemPref])
  -> T.Text
  -> F.FrameRec ('[BR.StateAbbreviation] V.++ BR.CatColsASR V.++  '[ET.ElectoralWeight])
  -> F.Frame BR.ElectoralCollege
  -> F.FrameRec [WeightSource, BR.StateAbbreviation, BR.Electors, PUMS.Citizens, BR.DemPref, ET.ElectoralWeight]
mergeASRElectionData demographics prefs ewType ews eCollege =
  -- join and fold
  let demoPref = F.toFrame $ catMaybes $ fmap F.recMaybe $ F.leftJoin @('[BR.StateAbbreviation] V.++ BR.CatColsASR) demographics prefs
      demoPrefWeight = catMaybes $ fmap F.recMaybe $ F.leftJoin @('[BR.StateAbbreviation] V.++ BR.CatColsASR) demoPref ews
      catFold :: FL.Fold (F.Record (BR.CatColsASR V.++ [PUMS.Citizens, BR.DemPref, ET.ElectoralWeight]))
                           (F.Record [PUMS.Citizens, BR.DemPref, ET.ElectoralWeight])
      catFold =
        let pop = F.rgetField @PUMS.Citizens
            pref = F.rgetField @BR.DemPref
            wgt = F.rgetField @ET.ElectoralWeight
            popSumF = FL.premap pop FL.sum
            voteSumF = FL.premap (\r -> realToFrac (pop r) * wgt r) FL.sum
            wgtdPrefF = FL.premap (\r -> realToFrac (pop r) * wgt r * pref r) FL.sum
        in  FF.sequenceRecFold
            $ FF.toFoldRecord (FL.premap pop FL.sum)
            V.:& FF.toFoldRecord ((/) <$> wgtdPrefF <*> voteSumF)
            V.:& FF.toFoldRecord ((/) <$> voteSumF <*> (fmap realToFrac popSumF))
            V.:& V.RNil
      psFold = FMR.concatFold $ FMR.mapReduceFold
               FMR.noUnpack
               (FMR.assignKeysAndData @'[BR.StateAbbreviation])
               (FMR.foldAndAddKey catFold)
      postStratified = FL.fold psFold demoPrefWeight
      psWithElectors = F.toFrame $ catMaybes $ fmap F.recMaybe $ F.leftJoin @'[BR.StateAbbreviation] postStratified eCollege
  in fmap (F.rcast . FT.mutate (const $ FT.recordSingleton @WeightSource ewType)) psWithElectors

mergeASEElectionData :: 
  F.FrameRec ('[BR.StateAbbreviation] V.++ BR.CatColsASE V.++  '[PUMS.Citizens])
  -> F.FrameRec ('[BR.StateAbbreviation] V.++ BR.CatColsASE V.++  '[BR.DemPref])
  -> T.Text
  -> F.FrameRec ('[BR.StateAbbreviation] V.++ BR.CatColsASE V.++  '[ET.ElectoralWeight])
  -> F.Frame BR.ElectoralCollege
  -> F.FrameRec [WeightSource, BR.StateAbbreviation, BR.Electors, PUMS.Citizens, BR.DemPref, ET.ElectoralWeight]
mergeASEElectionData demographics prefs ewType ews eCollege =
  -- join and fold
  let demoPref = F.toFrame $ catMaybes $ fmap F.recMaybe $ F.leftJoin @('[BR.StateAbbreviation] V.++ BR.CatColsASE) demographics prefs
      demoPrefWeight = catMaybes $ fmap F.recMaybe $ F.leftJoin @('[BR.StateAbbreviation] V.++ BR.CatColsASE) demoPref ews
      catFold :: FL.Fold (F.Record (BR.CatColsASE V.++ [PUMS.Citizens, BR.DemPref, ET.ElectoralWeight]))
                           (F.Record [PUMS.Citizens, BR.DemPref, ET.ElectoralWeight])
      catFold =
        let pop = F.rgetField @PUMS.Citizens
            pref = F.rgetField @BR.DemPref
            wgt = F.rgetField @ET.ElectoralWeight
            popSumF = FL.premap pop FL.sum
            voteSumF = FL.premap (\r -> realToFrac (pop r) * wgt r) FL.sum
            wgtdPrefF = FL.premap (\r -> realToFrac (pop r) * wgt r * pref r) FL.sum
        in  FF.sequenceRecFold
            $ FF.toFoldRecord (FL.premap pop FL.sum)
            V.:& FF.toFoldRecord ((/) <$> wgtdPrefF <*> voteSumF)
            V.:& FF.toFoldRecord ((/) <$> voteSumF <*> (fmap realToFrac popSumF))
            V.:& V.RNil
      psFold = FMR.concatFold $ FMR.mapReduceFold
               FMR.noUnpack
               (FMR.assignKeysAndData @'[BR.StateAbbreviation])
               (FMR.foldAndAddKey catFold)
      postStratified = FL.fold psFold demoPrefWeight
      psWithElectors = F.toFrame $ catMaybes $ fmap F.recMaybe $ F.leftJoin @'[BR.StateAbbreviation] postStratified eCollege
  in fmap (F.rcast . FT.mutate (const $ FT.recordSingleton @WeightSource ewType)) psWithElectors



type ElectorsD = "ElectorsD" F.:-> Int
type ElectorsR = "ElectorsR" F.:-> Int

mergedElectionDataToResultF :: FL.Fold (F.Record [BR.StateAbbreviation, BR.Electors, PUMS.Citizens, BR.DemPref, ET.ElectoralWeight])
                               (F.Record [ElectorsD, ElectorsR, BR.DemPref])
                              
mergedElectionDataToResultF =
  let es = F.rgetField @BR.Electors
      pop = F.rgetField @PUMS.Citizens
      dShare = F.rgetField @BR.DemPref
      voteWgt = F.rgetField @ET.ElectoralWeight
      popSumF = FL.premap pop FL.sum
      electorF t = FL.prefilter t $ FL.premap es FL.sum
      votersF = FL.premap (\r -> realToFrac (pop r) * voteWgt r) FL.sum
      dVotersF = FL.premap (\r -> realToFrac (pop r) * voteWgt r * dShare r) FL.sum
  in  FF.sequenceRecFold
      $ FF.toFoldRecord (electorF ((>0.5) . dShare))
      V.:& FF.toFoldRecord (electorF ((<=0.5) . dShare))
      V.:& FF.toFoldRecord ((/) <$> dVotersF <*> votersF)
      V.:& V.RNil


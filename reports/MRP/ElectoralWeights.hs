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
import qualified Data.Map                      as M
import qualified Data.Text                     as T

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
import qualified Frames.SimpleJoins            as FJ

import qualified Frames.Visualization.VegaLite.Data
                                               as FV

import qualified Graphics.Vega.VegaLite        as GV
import qualified Knit.Report                   as K
import qualified Polysemy.Error                as P (mapError)
import qualified Polysemy                      as P (raise)

import           Data.String.Here               ( i )

import           BlueRipple.Configuration 
import           BlueRipple.Utilities.KnitUtils 

import qualified Numeric.GLM.ProblemTypes      as GLM
import qualified Numeric.GLM.Bootstrap            as GLM

import qualified Data.Time.Calendar            as Time
import qualified Data.Time.Clock               as Time

import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Data.Loaders as BR
import qualified BlueRipple.Data.ACS_PUMS as PUMS
import qualified BlueRipple.Data.ACS_PUMS_Loaders as PUMS
import qualified BlueRipple.Data.CPSVoterPUMS as CPS
import qualified BlueRipple.Data.DemographicTypes as BR
import qualified BlueRipple.Data.ElectionTypes as ET

import qualified BlueRipple.Model.MRP as BR
import qualified BlueRipple.Model.Turnout_MRP as BR

import qualified BlueRipple.Data.UsefulDataJoins as BR
import qualified MRP.CCES_MRP_Analysis as BR
import qualified BlueRipple.Utilities.KnitUtils as BR
import MRP.Common
import MRP.CCES
import MRP.DeltaVPV (DemVPV)

text1 :: T.Text
text1 = [i|
Our original plan for this post was to delve into the modeling issues surrounding electoral
weighting in polling and predicitive modeling of election outcomes.  But the country
is a very different place then when we started this work.  As important as the upcoming
elections may be, the corona-virus pandemic is urgent and exhausting.

But there is still an election coming and we still want to be a source of thoughtful ideas
for participation for fellow progressives and Democrats.  So we are going to continue this
work, and try to understand along with you, how our landscape has changed.

--

Predicting election outcomes via polling or survey data is hard, primarily because the people you survey
may not be representative of the electorate, and the composition of the electorate in each state is a moving target.
People don't always remember whether they voted or who they voted for and,
for a variety of reasons, they may not be honest about their voting history.

- Getting accurate answers, is a key part of a pollsters job.  Some surveys, e.g., the [CCES
survey][CCES], one we use a lot,
tackle this issue by verifying some data externally, for example via state voter-files.
- Surveys and polls assign *weights* to responses, numbers that allow users of the data to
reconstruct a representative sample of the population from the survey.  For example, if
the country has roughly the same number of adults above and below 45 but the survey has
fewer responses from older people, those responses will be assigned higher weights.  Weighting
is complex, not least because there are many demographic variables to be considered.
- But we truly *do not know* who will vote on election day.  You could
have extremely accurate estimates of how each kind of person (older or younger,
male or female, Black, White, Latinx, Asian, Texan or Californian, etc.) *would* vote
and not really know *how many of them will* vote.  This gets even harder to predict in
smaller regions, like states, congressional districts or state legislative districts.

The recent elections in Wisconsin are a reminder of all this.  The last minute surge in
mail-in ballots, the closure of polling places and resulting long lines at polling
places all likely shifted the composition of the electorate.

We have very accurate information about who lives in each of these places.  The census
tracks this and updates the data annually.  So what we need to know is the
probability that a voter of a certain demographic type, in a certain place, will cast a
vote. There are lots of reasons why people do and don't
vote: see [our piece][BR:ID] on voter ID or Fair Vote's [rundown][FV:Turnout] on
voter turnout, for example.  For the purposes of *this post* we're going to ignore
the reasons and focus on the modeling problem.  For another point of view,
[here][UpshotModel] is an explanation from the New York Times' Upshot about their
election modeling.

When pollsters and academics talk about this problem, they use the term "weighting" or
"weights." The weighting of a survey to match the electorate is just like what we
talked about above, when polls are weighted to match the population, except here the
weights are chosen to match some estimate of what the electorate will look like.
But we will often talk instead about probabilities, the chance that a given voter will vote.
These probabilities, multiplied by the number of people in a given group,
a number we know quite accurately, provide the "electoral weight".
for example, if 60% of Texan Females over 45 will vote (a probability),
and you know how many Texan Females over 45 there are (about 1.2 million)
you can figure out how to weight your survey responses so that the weighted tally
contains about 738,000 female Texan voters over 45.

So how do we figure out the probability that someone will vote?
There are a number of standard approaches:

1. Ask them! Some surveys ask the respondent if they intend to vote, or, more often,
a [bunch of questions][Pew:LikelyVoter] used to assess their likelihood of voting, and then use that
(or some modeled fraction of that) to create a set of weights with as much specificity
as the survey allows. This is almost always what pollsters do.
2. Assume the next election will be like the last election. The census tracks nationwide
demographically split turnout for previous elections and we can use those weights,
though they lack any geographic specificity. Also, some surveys match respondents with
voter-file data and thus turnout.  These can then be used to build a set of weights.
3. Model the electorate using some combination of the above data---previous elections,
surveys---and whatever other variables, e.g., negative partisanship of a district,
to predict how the current electorate might differ from the past.

Rather than opine on the best model, which is not our expertise,
we want to demonstrate how much a prediction can depend on the model
of electoral weights. We want you to become a more informed (and skeptical!)
reader of polls and academic work predicting electoral results.

This piece was partially inspired by a recent [Vox article][Vox:BernieYouth], which used
a novel survey to look at how each candidate in the Democratic primary might fare against
Donald Trump. The [paper][Paper:BernieYouth] on which the article is based
is interesting and worth a read.  Some of the conclusions of the paper and article are
based on electoral weights derived
from the 2016 election (using the CCES survey).
It's very hard to evaluate these conclusions without understanding
how sensitive they are to the choice of electoral weights.

So let's explore how much difference these weights can make!  We'll fix our voter preferences to
an estimate from the 2016 presidential election (using a variation of the [MR model][BR:MRP]
we've used in the last couple of posts), and use 2016 
[demographics][Census:PUMS] from the census.  The only thing we'll be varying here
are the electoral weights, that is, the likelihood that a given type of voter,
in a given place, will vote

We'll begin by picking a few
possible sets of weights and comparing the projected popular vote and electoral college results
that they predict from the *same set of preferences*---in this case, preferences derived from
our [MRP model][BR:MRP] based on the [CCES][CCES].  
One note: each set of weights has been adjusted on a per state basis so that the weights
and demographics (in the same election year) correctly give the total number of votes cast in
the state that year--if you're interested in the details, we followed a simplified version
of the techniques described in [this paper][DeepInteractions]. We used
State-level probabilities inferred from either the CCES survey for the 2008,
2012 and 2016 general elections, or the Census CPS survey (using Microdata),
each broken down by age (45 or over/under 45),
sex (Female/Male),
education (College Grad and in College/otherwise)
and race (Non-white/White Non-hispanic). 

We're not recommending any of these weights in particular,
but each might be a reasonable model of the 2020 electorate since each is derived from a validated source and a
recent election.

The chart below shows the Democratic share of the 2-party popular vote and the number of electors won by the Democratic candidate
using (modeled) 2016 preferences, 2016 demographics and each set of electoral weights.  The results vary quite a bit,
from a popular vote share below 49.5% and 190 electoral votes to a popular vote share over 53% and over
370 electoral votes! That's a swing of almost 4% in the popular vote (~6 million votes)
and the difference between losing and winning the election.

[UpshotModel]: <https://www.nytimes.com/2016/06/10/upshot/how-we-built-our-model.html>
[DeepInteractions]: <http://www.stat.columbia.edu/~gelman/research/unpublished/deep-inter.pdf>
[PEW:LikelyVoter]: <https://www.pewresearch.org/methods/2016/01/07/measuring-the-likelihood-to-vote/>
[Census:PUMS]: <https://www.census.gov/programs-surveys/acs/technical-documentation/pums.html>
[FV:Turnout]: <https://www.fairvote.org/voter_turnout#voter_turnout_101>
[BR:ID]: <https://blueripplepolitics.org/blog/voter-ids>
[BR:MRP]: <https://blueripple.github.io/research/mrp-model/p1/main.html#data-and-methods>
[CCES]: <https://cces.gov.harvard.edu/>
[Vox:BernieYouth]: <https://www.vox.com/policy-and-politics/2020/2/25/21152538/bernie-sanders-electability-president-moderates-data>
[Paper:BernieYouth]: <https://osf.io/25wm9/>
[Rumsfeld]: <https://en.wikipedia.org/wiki/There_are_known_knowns>
|]


text2 :: T.Text
text2 = [i|
There is substantially more variation among the models using CCES weights
than the model using weights from the census.
The CCES models also look more like the actual election outcomes,
though that could just be a coincidence.
The census-sourced electoral weights predict higher turnout from younger voters
and minority voters than does the CCES data,
and is thus much more Democratic-candidate-friendly than the CCES data. 

[Vox:EducationWeighting]: <https://www.vox.com/policy-and-politics/2019/11/14/20961794/education-weight-state-polls-trump-2020-election>
|]

modelingNotes :: T.Text
modelingNotes = [i|
## Model Notes
As we continue to work on this data we try to refine and improve our modeling.
In this post we have shifted from using the census summary of the CPS voter supplement
to using the CPS microdata itself, as harmonized via the [IPUMS][IPUMS-CPS] web portal.
This has several advantages.  Firstly, the micro-data
allows us to get turnout numbers at the *state* level.
Actually, the data is given at the county level,
but since counties don't map neatly onto congressional
districts or state legislative districts, this is less useful.
Also useful is that the microdata has more demographic information.  For example,
the summary tables allow us to explore the variables of age, sex and race *or*
ag, sex, and education but not age, sex, education, and race.  This is important since
the combination of education and race is necessary to explore the voting and turnout
patterns of the so-called "White Working Class," a crucial voting block in the past few
elections.
Another nice benefit is that the microdata contains more information about voting.  It includes
data about whether people voted by mail or voted early and some information about why
registered non-voters didn't vote.
For all those reasons, going forward we will be using the mocro-data sourced CPS turnout data instead of
the previous nationally-aggregated summaries of that data.

[IPUMS-CPS]: <https://cps.ipums.org/cps/>
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


type ByMailPct = "ByMailPct" F.:-> Double
type EarlyPct = "EarlyPct" F.:-> Double

alternateVoteF :: FL.Fold (F.Record CPS.CPSVoterPUMS) (F.FrameRec [BR.Year, BR.StateAbbreviation, ByMailPct, EarlyPct])
alternateVoteF =
  let possible r = CPS.cpsPossibleVoter (F.rgetField @ET.VotedYNC r) && F.rgetField @BR.IsCitizen r
      nonCat = F.rcast @[BR.Year, BR.StateAbbreviation]
      catKey = CPS.cpsKeysToIdentity . F.rcast
      innerF :: FL.Fold (F.Record '[ ET.VoteHowC
                                   , ET.VoteWhenC
                                   , ET.VotedYNC
                                   , BR.IsCitizen
                                   , CPS.CPSVoterPUMSWeight
                                   ])
                (F.Record [ByMailPct, EarlyPct])
      innerF =
        let vbm r = F.rgetField @ET.VoteHowC r == ET.VH_ByMail
            early r = F.rgetField @ET.VoteWhenC r == ET.VW_BeforeElectionDay
            wgt r = F.rgetField @CPS.CPSVoterPUMSWeight r
            wgtdCountF f = FL.prefilter (\r -> f r && possible r) $ FL.premap wgt FL.sum
        in (\bm e wgt -> bm/wgt F.&: e/wgt F.&: V.RNil) <$> wgtdCountF vbm <*> wgtdCountF early <*> wgtdCountF (const True)            
  in CPS.cpsVoterPUMSRollup nonCat catKey innerF

post :: forall r.(K.KnitMany r, K.Member GLM.RandomFu r) => Bool -> K.Sem r ()
post updated = P.mapError BR.glmErrorToPandocError $ K.wrapPrefix "TurnoutScenarios" $ do
  K.logLE K.Info "Loading re-keyed demographic and turnout data."

  -- state totals and national demographic splits
  stateTurnoutRaw <- BR.stateTurnoutLoader

  let addElectoralWeight :: (F.ElemOf rs BR.Citizen, F.ElemOf rs BR.Voted)
                         => F.Record rs
                         -> F.Record [ET.ElectoralWeightSource, ET.ElectoralWeightOf, ET.ElectoralWeight] 
      addElectoralWeight r = ET.EW_Census F.&: ET.EW_Citizen F.&: (realToFrac $ F.rgetField @BR.Voted r)/(realToFrac $ F.rgetField @BR.Citizen r) F.&: V.RNil

--      ewASR = fmap (FT.mutate addElectoralWeight) asrTurnout
--      ewASE = fmap (FT.mutate addElectoralWeight) aseTurnout
  cpsVoterPUMS <- CPS.cpsVoterPUMSLoader
  -- vote by mail and early voting by state
  let countAlternateCPS = FL.fold alternateVoteF cpsVoterPUMS
  logFrame $ F.filterFrame (\r -> F.rgetField @BR.Year r == 2018) countAlternateCPS      
  let cpsASERTurnoutByState = FL.fold (CPS.cpsVoterPUMSElectoralWeightsByState (CPS.cpsKeysToASER True . F.rcast)) cpsVoterPUMS
      cpsASERTurnout = FL.fold (CPS.cpsVoterPUMSNationalElectoralWeights (CPS.cpsKeysToASER True . F.rcast)) cpsVoterPUMS
      cpsASERCounted2008 = FL.fold (CPS.cpsCountVotersByStateF (CPS.cpsKeysToASER True . F.rcast) 2008) cpsVoterPUMS  
      predictorsASER = fmap GLM.Predictor (BR.allSimplePredictors @BR.CatColsASER)
      statesAfter y r = F.rgetField @BR.Year r > y && F.rgetField @BR.StateAbbreviation r /= "National"

  inferredCensusTurnoutASER <- F.filterFrame (statesAfter 2007) <$> BR.retrieveOrMakeFrame "mrp/turnout/censusSimpleASER_MR.bin"
                               (do
                                   P.raise
                                     $ BR.mrpTurnout @BR.CatColsASER
                                     (Just "T_CensusASER")
                                     ET.EW_Census
                                     ET.EW_Citizen
                                     cpsVoterPUMS
                                     (CPS.cpsCountVotersByStateF $ CPS.cpsKeysToASER True . F.rcast)
                                     predictorsASER
                                     BR.catPredMaps)

  -- preferences
  inferredPrefsASER <-  F.filterFrame (statesAfter 2007) <$> BR.retrieveOrMakeFrame "mrp/simpleASER_MR.bin"
                        (P.raise $ BR.mrpPrefs @BR.CatColsASER (Just "ASER") ccesDataLoader predictorsASER BR.catPredMaps) 

  -- inferred turnout
  inferredCCESTurnoutASER <- F.filterFrame (statesAfter 2007) <$> BR.retrieveOrMakeFrame "mrp/turnout/ccesSimpleASER_MR.bin"
                             (do
                                 ccesData <- ccesDataLoader
                                 P.raise
                                   $ BR.mrpTurnout @BR.CatColsASER
                                   (Just "T_ASER")
                                   ET.EW_CCES
                                   ET.EW_Citizen
                                   ccesData
                                   (BR.countVotersF @BR.CatColsASER)
                                   predictorsASER
                                   BR.catPredMaps)
  --  logFrame inferredTurnoutASE
  -- demographics
  pumsASERByState <- BR.retrieveOrMakeFrame "mrp/weight/pumsASERByState.bin"
                     (do
                         pumsDemographics <- PUMS.pumsLoadAll
                         return $ fmap (FT.mutate $ const $ FT.recordSingleton @BR.PopCountOf BR.PC_Citizen)
                           $ FL.fold (PUMS.pumsStateRollupF $ PUMS.pumsKeysToASER True) pumsDemographics
                     )

  let addElectoralWeight :: (F.ElemOf rs BR.Citizen, F.ElemOf rs BR.Voted)
                         => F.Record rs
                         -> F.Record [ET.ElectoralWeightSource, ET.ElectoralWeightOf, ET.ElectoralWeight] 
      addElectoralWeight r = ET.EW_Census F.&: ET.EW_Citizen F.&: (realToFrac $ F.rgetField @BR.Voted r)/(realToFrac $ F.rgetField @BR.Citizen r) F.&: V.RNil

  K.logLE K.Info "Adjusting national census turnout via PUMS demographics and total recorded turnout"      
  let aserDemoAndAdjCensusEW_action = BR.demographicsWithAdjTurnoutByState
                                @BR.CatColsASER
                                @PUMS.Citizens
                                @'[PUMS.NonCitizens, BR.PopCountOf, BR.StateFIPS]
                                @'[BR.Year, BR.StateAbbreviation]
                                stateTurnoutRaw
                                (fmap F.rcast pumsASERByState)
                                (fmap F.rcast inferredCensusTurnoutASER)
  let filterForAdjTurnout r = F.rgetField @BR.StateAbbreviation r == "WI" && F.rgetField @BR.Year r == 2016
 
  aserDemoAndAdjCensusEW <- BR.retrieveOrMakeFrame "turnout/aserPumsDemoAndAdjCensusEW.bin" aserDemoAndAdjCensusEW_action
{-  K.logLE K.Diagnostic $ "pumsASEByState has " <> (T.pack . show $ FL.fold FL.length pumsASEByState) <> " rows."
  K.logLE K.Diagnostic $ "cpsASETurnoutByState has " <> (T.pack . show $ FL.fold FL.length cpsASETurnoutByState) <> " rows."
  K.logLE K.Diagnostic $ "aseDemoAndAdjEW has " <> (T.pack . show $ FL.fold FL.length aseDemoAndAdjEW) <> " rows." -}
--  logFrame $ F.filterFrame filterForAdjTurnout aserDemoAndAdjCensusEW
  K.logLE K.Info "Adjusting CCES inferred turnout via PUMS demographics and total recorded turnout."  
  let aserDemoAndAdjCCESEW_action = BR.demographicsWithAdjTurnoutByState
                                    @BR.CatColsASER
                                    @PUMS.Citizens
                                    @'[PUMS.NonCitizens, BR.PopCountOf, BR.StateFIPS]
                                    @'[BR.Year, BR.StateAbbreviation] stateTurnoutRaw (fmap F.rcast pumsASERByState) (fmap F.rcast inferredCCESTurnoutASER)
  
  aserDemoAndAdjCCESEW <- BR.retrieveOrMakeFrame "turnout/aserPumsDemoAndAdjCCESEW.bin" aserDemoAndAdjCCESEW_action
  K.logLE K.Diagnostic "comparison of Census then CCES for 2016 in WI after adjustment."
--  logFrame $ F.filterFrame filterForAdjTurnout aserDemoAndAdjCensusEW
--  logFrame $ F.filterFrame filterForAdjTurnout aserDemoAndAdjCCESEW
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
  aserAllByState <- BR.retrieveOrMakeFrame "mrp/weights/aserAllByState.bin"
                    (K.knitMaybe "Missing key when joining inferredPrefsASR and aserDemoAndAdjCensusEW"
                     $ FJ.leftJoinM @('[BR.StateAbbreviation, BR.Year] V.++ BR.CatColsASER) inferredPrefsASER aserDemoAndAdjCensusEW)                   
  -- fold these to state level
  let aserDemoF = FMR.concatFold $ FMR.mapReduceFold
                  FMR.noUnpack
                  (FMR.assignKeysAndData @(BR.CatColsASER V.++ '[BR.StateAbbreviation, BR.Year, ET.Office, ET.ElectoralWeightSource, ET.ElectoralWeightOf])
                    @[PUMS.Citizens, ET.ElectoralWeight, DemVPV, BR.DemPref])
                  (FMR.foldAndAddKey foldPrefAndTurnoutData)                               
      aserByState = FL.fold aserDemoF aserAllByState
  electoralVotesByStateFrame <- F.filterFrame ((==2020) . F.rgetField @BR.Year) <$> BR.electoralCollegeFrame
  let yearFilter y r =  F.rgetField @BR.Year r == y 
      yearOfficeFilter y o r =  yearFilter y r && F.rgetField @ET.Office r == o
      prependYearSourceAndGrouping :: Int
                                   -> ET.ElectoralWeightSourceT
                                   -> BR.DemographicGrouping
                                   -> F.Record rs
                                   -> F.Record (BR.Year ': (ET.ElectoralWeightSource ': (BR.DemographicGroupingC ': rs)))
      prependYearSourceAndGrouping y src grp r =  y F.&: src F.&: grp F.&: r
      prepRows y src grp = F.toFrame . fmap (prependYearSourceAndGrouping y src grp) . FL.fold FL.list  

  let ewResultsF = FMR.concatFold $ FMR.mapReduceFold
                   FMR.noUnpack
                   (FMR.assignKeysAndData @'[BR.Year, ET.ElectoralWeightSource, BR.DemographicGroupingC, WeightSource])
                   (FMR.foldAndAddKey mergedElectionDataToResultF)
    
  let aserPrefs y o = fmap (F.rcast @('[BR.StateAbbreviation] V.++ BR.CatColsASER V.++ '[BR.DemPref]))
                      $ F.filterFrame (yearOfficeFilter y o) inferredPrefsASER
      aserDemo y o = fmap (F.rcast @('[BR.StateAbbreviation] V.++ BR.CatColsASER V.++ '[PUMS.Citizens]))
                    $ F.filterFrame (yearFilter y) pumsASERByState
      aserCensusEW y o = fmap (F.rcast @('[BR.StateAbbreviation] V.++ BR.CatColsASER V.++ '[ET.ElectoralWeightSource, ET.ElectoralWeightOf, ET.ElectoralWeight]))
                        $ F.filterFrame (yearFilter y)  aserDemoAndAdjCensusEW
      aserCCESEW y = fmap (F.rcast @('[BR.StateAbbreviation] V.++ BR.CatColsASER V.++ '[ET.ElectoralWeightSource, ET.ElectoralWeightOf, ET.ElectoralWeight]))
                     $ F.filterFrame (yearFilter y) aserDemoAndAdjCCESEW

      aserWgtdByCensus2008 = prepRows 2008 ET.EW_Census BR.ASER
                             <$> mergeASERElectionData
                             (aserDemo 2016 ET.President)
                             (aserPrefs 2016 ET.President)
                             "Census (ASER) 2008"
                             (aserCensusEW 2008 ET.President)
                             electoralVotesByStateFrame 
      aserWgtdByCensus2012 = prepRows 2012 ET.EW_Census BR.ASER
                            <$> mergeASERElectionData
                            (aserDemo 2016 ET.President)
                            (aserPrefs 2016 ET.President)
                            "Census (ASER) 2012"
                            (aserCensusEW 2012 ET.President)
                            electoralVotesByStateFrame
      aserWgtdByCensus2016 = prepRows 2016 ET.EW_Census BR.ASER
                            <$> mergeASERElectionData
                            (aserDemo 2016 ET.President)
                            (aserPrefs 2016 ET.President)
                            "Census (ASER) 2016"
                            (aserCensusEW 2016 ET.President)
                            electoralVotesByStateFrame
      aserWgtdByCensus2018 = prepRows 2018 ET.EW_Census BR.ASER
                            <$> mergeASERElectionData
                            (aserDemo 2018 ET.House)
                            (aserPrefs 2016 ET.President)
                            "Census (ASER) 2018"
                            (aserCensusEW 2018 ET.House)
                            electoralVotesByStateFrame                            
                     
      aserWgtdByCCES yDemo oDemo yTurnout = prepRows yTurnout ET.EW_CCES BR.ASER
                                            <$> mergeASERElectionData
                                            (aserDemo yDemo oDemo)
                                            (aserPrefs 2016 ET.President)
                                            ("CCES (ASER) " <> (T.pack $ show yTurnout))
                                            (aserCCESEW yTurnout)
                                            electoralVotesByStateFrame
      aserCCES = mconcat <$> traverse (aserWgtdByCCES 2016 ET.President) [2008, 2012, 2016]
      aserCCES2 = mconcat <$> sequence [aserWgtdByCCES 2016 ET.President 2016, aserWgtdByCCES 2018 ET.House 2018]      
  aserWgtd <- BR.retrieveOrMakeFrame "mrp/weights/aserWgtd.bin" (mconcat <$> sequence [aserWgtdByCensus2008, aserWgtdByCensus2012, aserWgtdByCensus2016, aserCCES])  
  aserWgtd2 <- BR.retrieveOrMakeFrame "mrp/weights/aserWgtd2.bin" (mconcat <$> sequence [aserWgtdByCensus2016, aserWgtdByCensus2018, aserCCES2])      
  let aserEwResults = FL.fold ewResultsF aserWgtd
      aserEwResults2 = FL.fold ewResultsF aserWgtd2
  logFrame aserEwResults

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
        _ <-  K.addHvega Nothing Nothing
              $ vlWeights
              "Popular vote share and Electoral Votes for 2016 preferences and demographics and various electoral weights"
              (FV.ViewConfig 800 800 10)
              aserEwResults
        brAddMarkDown text2
        _ <-  K.addHvega Nothing Nothing
              $ vlWeights
              "Popular vote share and Electoral Votes: 2016 weightings vs. 2018 weightings"
              (FV.ViewConfig 800 800 10)
              aserEwResults2
        brAddMarkDown modelingNotes
        brAddMarkDown brReadMore



vlWeights :: (Functor f, Foldable f)
  => T.Text
  -> FV.ViewConfig
  -> f (F.Record [BR.Year, ET.ElectoralWeightSource, BR.DemographicGroupingC, WeightSource, ElectorsD, ElectorsR, BR.DemPref])
  -> GV.VegaLite
vlWeights title vc rows =
  let dat = FV.recordsToVLData id FV.defaultParse rows
      makeVS =  GV.calculateAs "100 * datum.DemPref" "Vote Share (%)"
      makeVSRuleVal = GV.calculateAs "50" "50%"
      encVSRuleX = GV.position GV.X [GV.PName "50%", GV.PmType GV.Quantitative, GV.PScale [GV.SDomain $ GV.DNumbers [48, 53]], GV.PNoTitle]
      makeEVRuleVal = GV.calculateAs "269" "Evenly Split"
      encEVRuleY = GV.position GV.Y [GV.PName "Evenly Split", GV.PmType GV.Quantitative, GV.PScale [GV.SDomain $ GV.DNumbers [170, 370]], GV.PNoTitle]
      makeSourceType = GV.calculateAs "datum.ElectoralWeightSource + '/' + datum.DemographicGrouping" "Weight Source"
      encX = GV.position GV.X [GV.PName "Vote Share (%)"
                              , GV.PmType GV.Quantitative
                              , GV.PScale [GV.SDomain $ GV.DNumbers [48.5, 54]]
                              , GV.PTitle "D Vote Share (%)"]
      encY = GV.position GV.Y [FV.pName @ElectorsD
                              , GV.PmType GV.Quantitative
                              , GV.PScale [GV.SDomain $ GV.DNumbers [170, 370]]
                              , GV.PTitle "# D Electoral Votes"
                              ]
      encColor = GV.color [FV.mName @BR.Year, GV.MmType GV.Nominal]
      encShape = GV.shape [GV.MName "Weight Source"]
--      filterToCensus = GV.filter (GV.FExpr "datum.ElectoralWeightSource==EW_Census")
--      filterToCCCES = GV.filter (GV.FExpr "datum.ElectoralWeightSource==EW_CCES")
      dotSpec  = GV.asSpec [(GV.encoding . encX . encY . encColor . encShape) [], (GV.transform . makeVS . makeSourceType) []
                              , GV.mark GV.Point [GV.MTooltip GV.TTData, GV.MFilled True, GV.MSize 100]]
      vsRuleSpec = GV.asSpec [(GV.encoding . encVSRuleX) [], (GV.transform . makeVSRuleVal) [], GV.mark GV.Rule []]
      evRuleSpec = GV.asSpec [(GV.encoding . encEVRuleY) [], (GV.transform . makeEVRuleVal) [], GV.mark GV.Rule []]
  in FV.configuredVegaLite vc [FV.title title, GV.layer [dotSpec, vsRuleSpec, evRuleSpec], dat]
  


type WeightSource = "WeightSource" F.:-> T.Text        
mergeElectionData ::
  forall ks cs xs ys zs r
  . (K.KnitEffects r
    , F.ElemOf ((xs V.++ F.RDeleteAll (ks V.++ cs) ys) V.++ F.RDeleteAll (ks V.++ cs) zs) PUMS.Citizens
    , F.ElemOf ((xs V.++ F.RDeleteAll (ks V.++ cs) ys) V.++ F.RDeleteAll (ks V.++ cs) zs) BR.DemPref
    , F.ElemOf ((xs V.++ F.RDeleteAll (ks V.++ cs) ys) V.++ F.RDeleteAll (ks V.++ cs) zs) ET.ElectoralWeight
    , ks F.⊆ ((xs V.++ F.RDeleteAll (ks V.++ cs) ys) V.++ F.RDeleteAll (ks V.++ cs) zs)
    , FI.RecVec (ks V.++ [PUMS.Citizens, BR.DemPref, ET.ElectoralWeight])
    , FJ.CanLeftJoinM3 (ks V.++ cs) xs ys zs
    , Ord (F.Record ks)
    , (ks V.++ '[PUMS.Citizens, BR.DemPref, WeightSource, ET.ElectoralWeight])  F.⊆
      (WeightSource ': (ks V.++ [PUMS.Citizens, BR.DemPref, ET.ElectoralWeight]))
    , Show (F.Record xs)
    , Show (F.Record ys)
    , Show (F.Record zs)
    )
  => F.FrameRec xs
  -> F.FrameRec ys
  -> T.Text
  -> F.FrameRec zs
  -> K.Sem r (F.FrameRec (ks V.++ '[PUMS.Citizens, BR.DemPref, WeightSource, ET.ElectoralWeight]))
mergeElectionData demographics prefs wSource wgts = K.wrapPrefix "mergeElectionData" $ do    
  let catFold =
        let pop = F.rgetField @PUMS.Citizens
            pref = F.rgetField @BR.DemPref
            wgt = F.rgetField @ET.ElectoralWeight
            popSumF = FL.premap pop FL.sum
            voteSumF = FL.premap (\r -> realToFrac (pop r) * wgt r) FL.sum
            wgtdPrefF = FL.premap (\r -> realToFrac (pop r) * wgt r * pref r) FL.sum
        in  FF.sequenceRecFold @_ @[PUMS.Citizens, BR.DemPref, ET.ElectoralWeight]
            $ FF.toFoldRecord (FL.premap pop FL.sum)
            V.:& FF.toFoldRecord ((/) <$> wgtdPrefF <*> voteSumF)
            V.:& FF.toFoldRecord ((/) <$> voteSumF <*> (fmap realToFrac popSumF))
            V.:& V.RNil
      psFold = FMR.concatFold $ FMR.mapReduceFold
               FMR.noUnpack
               (FMR.assignKeysAndData @ks @[PUMS.Citizens, BR.DemPref, ET.ElectoralWeight])
               (FMR.foldAndAddKey catFold)
      demoPrefWeightM = FJ.leftJoinM3 @(ks V.++ cs) demographics prefs wgts
  case demoPrefWeightM of
    Nothing -> do
      K.logLE K.Diagnostic "Missing key in mergeElectionData"
      K.logLE K.Diagnostic $ "demographics (" <> (T.pack . show $ FL.fold FL.length demographics) <> " rows)"
      logFrame demographics
      K.logLE K.Diagnostic $ "prefs (" <> (T.pack . show $ FL.fold FL.length prefs) <> " rows)"
      logFrame prefs
      K.logLE K.Diagnostic $ "wgts (" <> (T.pack . show $ FL.fold FL.length wgts) <> " rows)"
      logFrame wgts
      K.knitError "Missing key in mergeElectionData"
    Just demoPrefWeight -> do
      let postStratified :: F.FrameRec (ks V.++ [PUMS.Citizens, BR.DemPref, ET.ElectoralWeight])
          postStratified = FL.fold psFold demoPrefWeight
      return $ fmap (F.rcast . FT.addColumn @WeightSource wSource) postStratified

addEVs ::
  ( K.KnitEffects r
  , FJ.CanLeftJoinM '[BR.StateAbbreviation] rs (F.RecordColumns BR.ElectoralCollege)
   )
  => F.Frame BR.ElectoralCollege
  -> F.FrameRec rs
  -> K.Sem r (F.FrameRec (rs V.++ F.RDeleteAll '[BR.StateAbbreviation] (F.RecordColumns BR.ElectoralCollege)))
addEVs electors x = K.knitMaybe "Missing stateabbreviation key present in input but not in electors frame."
                    $ FJ.leftJoinM @'[BR.StateAbbreviation] x electors
  
mergeASRElectionData :: K.KnitEffects r
  =>  F.FrameRec ('[BR.StateAbbreviation] V.++ BR.CatColsASR V.++  '[PUMS.Citizens])
  -> F.FrameRec ('[BR.StateAbbreviation] V.++ BR.CatColsASR V.++  '[BR.DemPref])
  -> T.Text
  -> F.FrameRec ('[BR.StateAbbreviation] V.++ BR.CatColsASR V.++  '[ET.ElectoralWeightSource, ET.ElectoralWeightOf, ET.ElectoralWeight])
  -> F.Frame BR.ElectoralCollege
  -> K.Sem r (F.FrameRec [WeightSource, BR.StateAbbreviation, BR.Electors, PUMS.Citizens, BR.DemPref, ET.ElectoralWeight])
mergeASRElectionData demographics prefs wSource ews eCollege = K.wrapPrefix "ASR" $ do
  K.logLE K.Diagnostic $ "Merging for " <> wSource
  merged <- mergeElectionData @'[BR.StateAbbreviation] @BR.CatColsASR demographics prefs wSource ews
  fmap F.rcast <$> addEVs eCollege merged

  
mergeASEElectionData :: K.KnitEffects r
  => F.FrameRec ('[BR.StateAbbreviation] V.++ BR.CatColsASE V.++  '[PUMS.Citizens])
  -> F.FrameRec ('[BR.StateAbbreviation] V.++ BR.CatColsASE V.++  '[BR.DemPref])
  -> T.Text
  -> F.FrameRec ('[BR.StateAbbreviation] V.++ BR.CatColsASE V.++  '[ET.ElectoralWeightSource, ET.ElectoralWeightOf, ET.ElectoralWeight])
  -> F.Frame BR.ElectoralCollege
  -> K.Sem r (F.FrameRec [WeightSource, BR.StateAbbreviation, BR.Electors, PUMS.Citizens, BR.DemPref, ET.ElectoralWeight])
mergeASEElectionData demographics prefs wSource ews eCollege =  K.wrapPrefix "ASE" $ do
  K.logLE K.Diagnostic $ "Merging for " <> wSource
  merged <- mergeElectionData @'[BR.StateAbbreviation] @BR.CatColsASE demographics prefs wSource ews
  fmap F.rcast <$> addEVs eCollege merged

mergeASERElectionData :: K.KnitEffects r
  => F.FrameRec ('[BR.StateAbbreviation] V.++ BR.CatColsASER V.++  '[PUMS.Citizens])
  -> F.FrameRec ('[BR.StateAbbreviation] V.++ BR.CatColsASER V.++  '[BR.DemPref])
  -> T.Text
  -> F.FrameRec ('[BR.StateAbbreviation] V.++ BR.CatColsASER V.++  '[ET.ElectoralWeightSource, ET.ElectoralWeightOf, ET.ElectoralWeight])
  -> F.Frame BR.ElectoralCollege
  -> K.Sem r (F.FrameRec [WeightSource, BR.StateAbbreviation, BR.Electors, PUMS.Citizens, BR.DemPref, ET.ElectoralWeight])
mergeASERElectionData demographics prefs wSource ews eCollege = K.wrapPrefix "ASER" $ do
  K.logLE K.Diagnostic $ "Merging for " <> wSource
  merged <- mergeElectionData @'[BR.StateAbbreviation] @BR.CatColsASER demographics prefs wSource ews
  fmap F.rcast <$> addEVs eCollege merged

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


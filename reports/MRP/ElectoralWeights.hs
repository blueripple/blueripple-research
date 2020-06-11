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
import Control.Monad (join)
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
import qualified Numeric.GLM.MixedModel            as GLM

import qualified Data.Time.Calendar            as Time
import qualified Data.Time.Clock               as Time

import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Data.Loaders as BR
import qualified BlueRipple.Data.ACS_PUMS as PUMS
import qualified BlueRipple.Data.CPSVoterPUMS as CPS
import qualified BlueRipple.Data.DemographicTypes as BR
import qualified BlueRipple.Data.ElectionTypes as ET

import qualified BlueRipple.Model.MRP as BR
import qualified BlueRipple.Model.Turnout_MRP as BR

import qualified BlueRipple.Data.UsefulDataJoins as BR
import qualified MRP.CCES_MRP_Analysis as BR
import qualified BlueRipple.Utilities.KnitUtils as BR
import qualified BlueRipple.Data.Keyed         as Keyed
import MRP.Common
import MRP.CCES
import qualified MRP.CCES as CCES


text1 :: T.Text
text1 = [i|
Why are political polls so confusing?  Why do the same set of polls often lead to very different predictions?
There are a number of things that make polls confusing and difficult to interpret.  Among them, choosing
how to survey people (land-line phones?  Cell-phones?  Facebook survey?),
selecting which questions to ask and how to phrase them, and then interpreting the data to reach some
conclusion, e.g., to predict the outcome of an upcoming election. All these complexities are worth
talking about but here we're going to focus on the last question: how do we go from polling-data to
election prediction?

1. **Polling and Prediction 101**
2. **Estimating Electorate Composition**
3. **Example: The 2016 Presidential Election**
4. **Conclusion**

## Polling and Prediction 101
Predicting election outcomes via polling or survey data is hard: the people you survey
may not be representative of the electorate, and the composition of
the electorate in each state changes election-to-election.
When you conduct a poll or survey, you cannot control who responds.  So your sample will be
different in myriad ways from the population as a whole,  and also different from the subset
of the population who will vote.

Reconstructing the opinion of the population from a poll is done via weighting,
assigning a weight to each response that allow users of the data to
reconstruct a representative sample of the population from the survey.  For example, if
a state has roughly the same number of adults above and below 45 but the survey has
fewer responses from people over 45, those responses will be assigned higher weights.  Weighting
is complex, not least because there are many demographic variables to be considered.

So now we have an estimate of how a person in a particular place is likely to vote given
various demographic information about them.  And, from the census and other sources, we
know the demographics of each county/district/state.  But if we want to predict an election
outcome, or decide where we need to focus resources because an election is likely to be close,
we need to know who is going to cast a ballot in the election.  

There are lots of reasons why people do and don't
vote: see [our piece][BR:ID] on voter ID or Fair Vote's [rundown][FV:Turnout] on
voter turnout, for example.  For the purposes of this post we're going to ignore
the reasons and focus on the modeling problem.  For another point of view,
[here][UpshotModel] is an explanation from the New York Times' Upshot about their
election modeling.

Figuring out how to weight opinion data to predict voting is, arguably, the
most difficult part of determining out which elections/states are likely to be close and
thus where to prioritize work and the allocation of resources.

It is also deeply contentious, As an extreme example,
CNN recently published a [poll][CNN:20200608_Poll] showing Biden up 14 points
over Trump.  The Trump campaign hired a pollster to "analyze" the CNN poll and then
[demanded CNN retract the poll][CNN:Demand] which CNN promptly
[refused][CNN:RefuseRetract] to do so.  The substantive objection made by Trump's
campaign was that CNN should have weighted their poll so that the fraction
of Republicans, Democrats and Independents was the same as in 2016.  It's not hard to
see why this is a bad idea.  As public opinion shifts, so does people's partisan
identification.  Just to confirm this, below we plot partisan identity as reported
in the CCES survey from 2006-2018 for both Presidential and House elections just
to see how much it shifts election to election.

[CNN:RefuseRetract]: <https://www.cnn.com/2020/06/10/politics/cnn-letter-to-trump-over-poll/index.html>
[CNN:Demand]: <https://www.cnn.com/2020/06/10/politics/trump-campaign-cnn-poll/index.html>
[CNN:20200608_Poll]: <https://www.cnn.com/2020/06/08/politics/cnn-poll-trump-biden-chaotic-week/index.html>
[AAPOR:LikelyVoters]: <https://www.aapor.org/Education-Resources/Election-Polling-Resources/Likely-Voters.aspx>
[ElectProject:CPSOverReport]: <http://www.electproject.org/home/voter-turnout/cps-methodology>
[Census:CPSVoter]: <https://www.census.gov/topics/public-sector/voting.html>
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
  
text2 :: T.Text = [i|
When pollsters and academics talk about this problem, they use the term "weighting."
The weighting of a survey to match the electorate is just like what we
talked about above, when polls are weighted to match the population, except here the
weights are chosen or inferred to match some estimate of who will vote.

We will often talk instead about probabilities, the chance that a given voter will vote.
These probabilities, multiplied by the number of people in a given group,
provide the "electoral weight".
for example, if Female Texans over 45 have a 60% chance of voting,
and you know how many Texan Females over 45 there are (about 1.2 million)
you can figure out how to weight your survey responses so that the weighted tally
contains about 738,000 female Texan voters over 45. 

So how do we figure out the probability that someone will vote?
There are a number of standard approaches:

1. Ask them! Some surveys ask the respondent if they intend to vote, or, more often,
a [set of questions][Pew:LikelyVoter] is used to assess their likelihood of voting.
Those answers are used to create a set of weights with as much specificity
as the survey allows. 
2. Assume the next election will be like the last election. The census tracks nationwide
demographically split turnout for previous elections and we can use those weights.  Because
of the size of the [CPS voter survey][Census:CPSVoter], this is often a trusted source.  But
it lacks any verification and there is [significant evidence that people over-report their
voting][ElectProject:CPSOverReport].
2b. Alternatively, some surveys match respondents with
voter-file data and from there compute turnout probabilities. Acessing, standardizing and
processing voter file data is a big and expensive job.  There is no public data-set using
all the voter file info, though some market-research companies make subsets of this data
available to academics and non-profits at reduced rates.
3. Model the electorate using some combination of the above data---previous elections,
[surveys---and whatever other variables, e.g., negative partisanship of a district,
to predict how the current electorate might differ from the past.

Once you know the demographics of a place,
what probability each group has to vote for either candidate
(in a 2-party race) and the probability that eligible voters in each
group will cast a ballot on election day, you can predict outcomes.  The simplest way
is to simply multiply the number of people in each group, by the probability that they will vote
and then by the probability that, given that they will vote, they will vote for each candidate.
We add up all those numbers and get estimated votes for each candidate.
This "post-stratification" yields an estimate of the two-party vote.

We can stop here if all we care about is how close the election is likely to be.
Election modelers typically go further by estimating the uncertainty of those
predictions, often by simulation.  In one of these simulations, each person
in a district or state is simulated by two tosses of unfair coins, one to decide
if they vote, and for those that do, the other to decide for whom they voted.  This
simulation is then run a few thousand times and the various results plotted to
visualize uncertainty. So when a site like 538.com says that a candidate has a
60% chance of winning the election, they are saying that the candidate wins in
60% of the simulations.  Some election simulations are more complex than this
because they attempt to account for correlations among the regions, basically
to acknowledge that if the polling is wrong, it's probably wrong the same way
in every place.

Rather than opine on the best model, which is not our expertise,
we want to demonstrate that a prediction can depend a lot on the model
of electoral weights. We want you to become a more informed (and skeptical!)
reader of polls and academic work predicting electoral results.

So let's explore how much difference these weights can make.
We'll fix our voter preferences to
an estimate from the 2016 presidential election (using a variation of the [MR model][BR:MRP]
we've used in the last couple of posts), and use 2016 
[demographics][Census:PUMS] from the census.  The only thing we'll be varying here
are the electoral weights, that is, the likelihood that a given type of voter,
in a given place, will vote.  We'll use two data sources (the [CCES][CCES] and the
[Census CPS Voter survey][Census:CPSVoter]) and two different models of each data-set.
Both models group voters by sex (M/F), age (Under 45/45 and Over),
and education (No 4-year college/In college or college graduate).  The two models differ
in how they handle race.  The simpler model, "ASER", uses
the two-category classification we've been using in earlier posts (Non-White/White-Non-Latinx).
"ASER5" uses a five-category classification for race (Black/Latinx/Asian/White/Other).
Using MRP, we infer preferences and turnout for each group (16 groups for ASER, 40 for ASER5)
in each state and the District of Columbia. 
From these, we'll estimate the two-party-vote share in each state and the national vote share
and electoral college outcome.

One note: each set of weights has been adjusted on a per state basis so that the weights
and demographics correctly give the total number of votes cast in
the state that year--if you're interested in the details, we followed a simplified version
of the techniques described in [this paper][DeepInteractions].

We're not recommending any of these weights in particular,
but each comes from reliable and oft-used data-sources and
fairly common modeling assumptions about which categories are relevant.

The chart below shows the Democratic share of the 2-party popular vote and the number of electors won by the Democratic candidate
using (modeled) 2016 preferences, 2016 demographics and each set of electoral weights.  The results vary quite a bit,
from a popular vote share below 49.5% and 190 electoral votes to a popular vote share over 53% and over
340 electoral votes! That's a swing of almost 4% in the popular vote (~6 million votes)
and the difference between losing and winning the election.

[CNN:RefuseRetract]: <https://www.cnn.com/2020/06/10/politics/cnn-letter-to-trump-over-poll/index.html>
[CNN:Demand]: <https://www.cnn.com/2020/06/10/politics/trump-campaign-cnn-poll/index.html>
[CNN:20200608_Poll]: <https://www.cnn.com/2020/06/08/politics/cnn-poll-trump-biden-chaotic-week/index.html>
[AAPOR:LikelyVoters]: <https://www.aapor.org/Education-Resources/Election-Polling-Resources/Likely-Voters.aspx>
[ElectProject:CPSOverReport]: <http://www.electproject.org/home/voter-turnout/cps-methodology>
[Census:CPSVoter]: <https://www.census.gov/topics/public-sector/voting.html>
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


text3 :: T.Text
text3 = [i|
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
As we continue to work with this data we refine and improve our modeling.
In this post we have shifted from using the census summary of the CPS voter supplement
to using the CPS microdata itself, as harmonized via the [IPUMS][IPUMS-CPS] web portal.
This has several advantages.  Firstly, the micro-data
allows us to get turnout numbers at the *state* level.
Also useful is that the microdata has more demographic information.  For example,
the summary tables allow us to explore the variables of age, sex and race *or*
age, sex, and education but not age, sex, education, *and* race.  This is important since
the combination of education and race is necessary to explore the voting and turnout
patterns of the so-called "White Working Class," a crucial voting block in the past few
elections.
Another nice benefit is that the microdata contains more information about voting.  It includes
data about whether people voted by mail or voted early and some information about why
registered non-voters didn't vote.
For all those reasons, going forward we will be using the micro-data sourced CPS turnout data instead of
the previous nationally-aggregated summaries of that data.

[IPUMS-CPS]: <https://cps.ipums.org/cps/>
|]
  
foldPrefAndTurnoutData :: FF.EndoFold (F.Record '[PUMS.Citizens, ET.ElectoralWeight, ET.DemVPV, BR.DemPref])
foldPrefAndTurnoutData =  FF.sequenceRecFold
                          $ FF.toFoldRecord (FL.premap (F.rgetField @PUMS.Citizens) FL.sum)
                          V.:& FF.toFoldRecord (BR.weightedSumRecF @PUMS.Citizens @ET.ElectoralWeight)
                          V.:& FF.toFoldRecord (BR.weightedSumRecF @PUMS.Citizens @ET.DemVPV)
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
  let possible r = let vyn = F.rgetField @ET.VotedYNC r in CPS.cpsPossibleVoter vyn && CPS.cpsVoted vyn && F.rgetField @BR.IsCitizen r
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
  in CPS.cpsVoterPUMSRollup (\r -> nonCat r `V.rappend` catKey r) innerF

post :: forall r.(K.KnitMany r, K.Member GLM.RandomFu r) => Bool -> K.Sem r ()
post updated = P.mapError BR.glmErrorToPandocError $ K.wrapPrefix "ElectoralWeights" $ do
  K.logLE K.Info "Loading re-keyed demographic and turnout data."

  -- state totals and national demographic splits
  stateTurnoutRaw <- BR.stateTurnoutLoader

{-  let addElectoralWeight :: (F.ElemOf rs BR.Citizen, F.ElemOf rs BR.Voted)
                         => F.Record rs
                         -> F.Record [ET.ElectoralWeightSource, ET.ElectoralWeightOf, ET.ElectoralWeight] 
      addElectoralWeight r = ET.EW_Census F.&: ET.EW_Citizen F.&: (realToFrac $ F.rgetField @BR.Voted r)/(realToFrac $ F.rgetField @BR.Citizen r) F.&: V.RNil
-}

  let predictorsASER = fmap GLM.Predictor (BR.allSimplePredictors @BR.CatColsASER)
      predictorsASER5 = fmap GLM.Predictor (BR.allSimplePredictors @BR.CatColsASER5)
      statesAfter y r = F.rgetField @BR.Year r > y && F.rgetField @BR.StateAbbreviation r /= "National"


  partisanId <- BR.retrieveOrMakeFrame "mrp/weights/parstisanID" $ do
    ccesData <- ccesDataLoader
    let voted r = F.rgetField @CCES.Turnout r == CCES.T_Voted
        withPartyId r = F.rgetField @CCES.PartisanId3 r `elem` [CCES.PI3_Democrat, CCES.PI3_Republican, CCES.PI3_Independent]
        votedWithId r = voted r && withPartyId r 
        partisanIdF = FMR.concatFold
                          $ FMR.mapReduceFold
                          (FMR.unpackFilterRow votedWithId)
                          (FMR.assignKeysAndData @(BR.Year ': (CCES.PartisanId3 ': BR.CatColsASER5)) @'[CCES.CCESWeightCumulative])
                          (FMR.foldAndAddKey $ FF.foldAllConstrained @Num FL.sum)
        partisanId = FL.fold partisanIdF ccesData
        totalByYearF = FMR.concatFold
                       $ FMR.mapReduceFold
                       FMR.noUnpack
                       (FMR.assignKeysAndData @(BR.Year ': BR.CatColsASER5) @'[CCES.CCESWeightCumulative])
                       (FMR.foldAndAddKey $ FF.foldAllConstrained @Num FL.sum)
        totalWeight = fmap (FT.retypeColumn @CCES.CCESWeightCumulative @'("TotalWeight", Double))
                      $ FL.fold totalByYearF partisanId
    partisanIdFraction <- do
      withWeights <- K.knitEither
                     $ either (Left . T.pack . show) Right
                     $ FJ.leftJoinE @(BR.Year ': BR.CatColsASER5) partisanId totalWeight
      let w = F.rgetField @CCES.CCESWeightCumulative
          t = F.rgetField @'("TotalWeight", Double)
          getFraction :: F.Record '[CCES.CCESWeightCumulative, '("TotalWeight", Double)] -> F.Record '[ '("Fraction", Double)]
          getFraction r = (w r/ t r) F.&: V.RNil
      return $ fmap (FT.transform getFraction) withWeights
    logFrame partisanIdFraction

    pumsData <- PUMS.pumsLoader
    let pumsToCountsF = FMR.concatFold
                        $ FMR.mapReduceFold
                        (FMR.Unpack (pure @[] . FT.transform (PUMS.pumsKeysToASER5 True)))
                        (FMR.assignKeysAndData @(BR.Year ': BR.CatColsASER5) @'[PUMS.Citizens])
                        (FMR.foldAndAddKey $ FF.foldAllConstrained @Num FL.sum)
        pumsCounts = FL.fold pumsToCountsF pumsData
    partisanCitizens <- do
      withCitizens <- K.knitEither
                      $ either (Left . T.pack . show) Right
                      $ FJ.leftJoinE @(BR.Year ': BR.CatColsASER5) partisanIdFraction pumsCounts
      let getPartisans :: F.Record '[PUMS.Citizens, '("Fraction", Double)] -> F.Record '[ '("Partisans", Double)]
          getPartisans r = (realToFrac (F.rgetField @PUMS.Citizens r) * F.rgetField @'("Fraction", Double) r) F.&: V.RNil
      return $ fmap (FT.mutate $  getPartisans . F.rcast) withCitizens
    logFrame partisanCitizens
    let combineDemographicsF = FMR.concatFold
                               $ FMR.mapReduceFold
                               FMR.noUnpack
                               (FMR.assignKeysAndData @[BR.Year, CCES.PartisanId3] @'[ '("Partisans", Double)])
                               (FMR.foldAndAddKey $ FF.foldAllConstrained @Num FL.sum)
        partisansByYear = FL.fold combineDemographicsF partisanCitizens
        totalPartisansByYearF = FMR.concatFold
                                $ FMR.mapReduceFold
                                FMR.noUnpack
                                (FMR.assignKeysAndData @'[BR.Year] @'[ '("Partisans", Double)])
                                (FMR.foldAndAddKey $ FF.foldAllConstrained @Num FL.sum)
        totalPartisansByYear = fmap (FT.retypeColumn @'("Partisans", Double) @'("TotalPartisans", Double))
                               $ FL.fold totalPartisansByYearF partisansByYear
    withTotalPartisans <- K.knitEither
                          $ either (Left . T.pack . show) Right
                          $ FJ.leftJoinE @'[BR.Year] partisansByYear totalPartisansByYear
    let getPartisanFraction :: F.Record ['("Partisans", Double), '("TotalPartisans", Double)] -> F.Record '[ '("PartisanFraction", Double)]
        getPartisanFraction r = (F.rgetField @'("Partisans", Double) r / F.rgetField @'("TotalPartisans", Double) r) F.&: V.RNil
        withPartisanFraction = fmap (FT.mutate $ getPartisanFraction . F.rcast) withTotalPartisans
    logFrame withPartisanFraction
    return withPartisanFraction
    
  inferredCensusTurnoutASER <- F.filterFrame (statesAfter 2007) <$> BR.retrieveOrMakeFrame "mrp/turnout/censusSimpleASER_MR.bin"
                               (do
                                   cpsVoterPUMS <- CPS.cpsVoterPUMSLoader
                                   P.raise
                                     $ BR.mrpTurnout @BR.CatColsASER
                                     GLM.MDVNone
                                     (Just "T_CensusASER")
                                     ET.EW_Census
                                     ET.EW_Citizen
                                     cpsVoterPUMS
                                     (CPS.cpsCountVotersByStateF $ CPS.cpsKeysToASER True . F.rcast)
                                     predictorsASER
                                     BR.catPredMaps
                               )


  inferredCensusTurnoutASER5 <- F.filterFrame (statesAfter 2007) <$> BR.retrieveOrMakeFrame "mrp/turnout/censusASER5_MR.bin"
                               (do
                                   cpsVoterPUMS <- CPS.cpsVoterPUMSLoader
                                   P.raise
                                     $ BR.mrpTurnout @BR.CatColsASER5
                                     GLM.MDVNone
                                     (Just "T_CensusASER5")
                                     ET.EW_Census
                                     ET.EW_Citizen
                                     cpsVoterPUMS
                                     (CPS.cpsCountVotersByStateF $ CPS.cpsKeysToASER5 True . F.rcast)
                                     predictorsASER5
                                     BR.catPredMaps
                               )                               
  -- preferences
  inferredPrefsASER <-  F.filterFrame (statesAfter 2007) <$> BR.retrieveOrMakeFrame "mrp/simpleASER_MR.bin"
                        (P.raise $ BR.mrpPrefs @BR.CatColsASER GLM.MDVNone (Just "ASER") ccesDataLoader predictorsASER BR.catPredMaps)
  let
    addZeroCountsF = FMR.concatFold $ FMR.mapReduceFold
                     (FMR.noUnpack)
                     (FMR.splitOnKeys @'[BR.StateAbbreviation])
                     ( FMR.makeRecsWithKey id
                       $ FMR.ReduceFold
                       $ const
                       $ Keyed.addDefaultRec @BR.CatColsASER5 BR.zeroCount
                     )
  inferredPrefsASER5 <-  F.filterFrame (statesAfter 2007) <$> BR.retrieveOrMakeFrame "mrp/prefsASER5_MR.bin"
                         (P.raise $ BR.mrpPrefs @BR.CatColsASER5 GLM.MDVSimple (Just "T_prefsASER5") ccesDataLoader predictorsASER5 BR.catPredMaps)                         

  -- inferred turnout
  inferredCCESTurnoutOfAllASER <- F.filterFrame (statesAfter 2007) <$> BR.retrieveOrMakeFrame "mrp/turnout/ccesOfAllSimpleASER_MR.bin"
                                  (do
                                      ccesData <- ccesDataLoader
                                      P.raise
                                        $ BR.mrpTurnout @BR.CatColsASER
                                        GLM.MDVNone
                                        (Just "T_OfAllASER")
                                        ET.EW_CCES
                                        ET.EW_Citizen
                                        ccesData
                                        (BR.countVotersOfAllF @BR.CatColsASER)
                                        predictorsASER
                                        BR.catPredMaps
                                  )


  inferredCCESTurnoutOfAllASER5 <- F.filterFrame (statesAfter 2007) <$> BR.retrieveOrMakeFrame "mrp/turnout/ccesOfAllASER5_MR.bin"
                                  (do
                                      ccesData <- ccesDataLoader
                                      P.raise
                                        $ BR.mrpTurnout @BR.CatColsASER5
                                        GLM.MDVNone
                                        (Just "T_OfAllASER5")
                                        ET.EW_CCES
                                        ET.EW_Citizen
                                        ccesData
                                        (BR.countVotersOfAllF @BR.CatColsASER5)
                                        predictorsASER5
                                        BR.catPredMaps
                                  )
{-                                  
  K.logLE K.Info $ "GA CCES Turnout (inferred)"
  logFrame $ F.filterFrame gaFilter inferredCCESTurnoutOfAllASER5
-}                           
  --  logFrame inferredTurnoutASE
  -- demographics
  pumsASERByState <- BR.retrieveOrMakeFrame "mrp/weight/pumsASERByState.bin"
                     (do
                         pumsDemographics <- PUMS.pumsLoader
                         return $ fmap (FT.mutate $ const $ FT.recordSingleton @BR.PopCountOf BR.PC_Citizen)
                           $ FL.fold (PUMS.pumsStateRollupF $ PUMS.pumsKeysToASER True . F.rcast) pumsDemographics
                     )

  pumsASER5ByState <- BR.retrieveOrMakeFrame "mrp/weight/pumsASER5ByState.bin"
                      (do
                          pumsDemographics <- PUMS.pumsLoader
                          let rollup = fmap (FT.mutate $ const $ FT.recordSingleton @BR.PopCountOf BR.PC_Citizen)                              
                                       $ FL.fold (PUMS.pumsStateRollupF $ PUMS.pumsKeysToASER5 True . F.rcast) pumsDemographics
                              addDefaultsOneF :: FL.Fold (F.Record (BR.CatColsASER5 V.++ [PUMS.NonCitizens, BR.PopCountOf, PUMS.Citizens]))
                                                 (F.FrameRec (BR.CatColsASER5 V.++ [PUMS.NonCitizens, BR.PopCountOf, PUMS.Citizens]))
                              addDefaultsOneF = fmap F.toFrame $ Keyed.addDefaultRec @BR.CatColsASER5 (0 F.&: BR.PC_Citizen F.&: 0 F.&: V.RNil)
                              addDefaultsF = FMR.concatFold
                                             $ FMR.mapReduceFold
                                             FMR.noUnpack
                                             (FMR.assignKeysAndData @[BR.Year, BR.StateAbbreviation, BR.StateFIPS]) 
                                             (FMR.makeRecsWithKey id $ FMR.ReduceFold $ const addDefaultsOneF)
                          return $ FL.fold addDefaultsF rollup
                      )
{-                      
  K.logLE K.Info $ "GA Demographics (PUMS, ASER5)"
  logFrame $ F.filterFrame gaFilter pumsASER5ByState
-}

  K.logLE K.Info "Adjusting national census turnout via PUMS demographics and total recorded turnout"      
  let aserDemoAndAdjCensusEW_action = BR.demographicsWithAdjTurnoutByState
                                      @BR.CatColsASER
                                      @PUMS.Citizens
                                      @'[PUMS.NonCitizens, BR.PopCountOf, BR.StateFIPS]
                                      @'[BR.Year, BR.StateAbbreviation]
                                      stateTurnoutRaw
                                      (fmap F.rcast pumsASERByState)
                                      (fmap F.rcast inferredCensusTurnoutASER)

      aser5DemoAndAdjCensusEW_action = BR.demographicsWithAdjTurnoutByState
                                       @BR.CatColsASER5
                                       @PUMS.Citizens
                                       @'[PUMS.NonCitizens, BR.PopCountOf, BR.StateFIPS]
                                       @'[BR.Year, BR.StateAbbreviation]
                                       stateTurnoutRaw
                                       (fmap F.rcast pumsASER5ByState)
                                       (fmap F.rcast inferredCensusTurnoutASER5)

      filterForAdjTurnout r = F.rgetField @BR.StateAbbreviation r == "WI" && F.rgetField @BR.Year r == 2016
 
  aserDemoAndAdjCensusEW <- BR.retrieveOrMakeFrame "turnout/aserPumsDemoAndAdjCensusEW.bin" aserDemoAndAdjCensusEW_action
  aser5DemoAndAdjCensusEW <- BR.retrieveOrMakeFrame "turnout/aser5PumsDemoAndAdjCensusEW.bin" aser5DemoAndAdjCensusEW_action
  K.logLE K.Info "Adjusting CCES inferred turnout via PUMS demographics and total recorded turnout."  
  let aserDemoAndAdjCCESEW_action = BR.demographicsWithAdjTurnoutByState
                                    @BR.CatColsASER
                                    @PUMS.Citizens
                                    @'[PUMS.NonCitizens, BR.PopCountOf, BR.StateFIPS]
                                    @'[BR.Year, BR.StateAbbreviation] stateTurnoutRaw (fmap F.rcast pumsASERByState) (fmap F.rcast inferredCCESTurnoutOfAllASER)
      aser5DemoAndAdjCCESEW_action = BR.demographicsWithAdjTurnoutByState
                                     @BR.CatColsASER5
                                     @PUMS.Citizens
                                     @'[PUMS.NonCitizens, BR.PopCountOf, BR.StateFIPS]
                                     @'[BR.Year, BR.StateAbbreviation] stateTurnoutRaw (fmap F.rcast pumsASER5ByState) (fmap F.rcast inferredCCESTurnoutOfAllASER5)                                    
  
  aserDemoAndAdjCCESEW <- BR.retrieveOrMakeFrame "turnout/aserPumsDemoAndAdjCCESEW.bin" aserDemoAndAdjCCESEW_action
  aser5DemoAndAdjCCESEW <- BR.retrieveOrMakeFrame "turnout/aser5PumsDemoAndAdjCCESEW.bin" aser5DemoAndAdjCCESEW_action
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
                    $ K.knitEither
                    ( either (Left . T.pack . show) Right 
                    $ FJ.leftJoinE @('[BR.StateAbbreviation, BR.Year] V.++ BR.CatColsASER) inferredPrefsASER aserDemoAndAdjCensusEW)
  aser5AllByState <- BR.retrieveOrMakeFrame "mrp/weights/aser5AllByState.bin"
                     $ K.knitEither
                     ( either (Left . T.pack . show) Right  
                     $ FJ.leftJoinE @('[BR.StateAbbreviation, BR.Year] V.++ BR.CatColsASER5) inferredPrefsASER5 aser5DemoAndAdjCensusEW)
  -- fold these to state level
  let aserDemoF = FMR.concatFold $ FMR.mapReduceFold
                  FMR.noUnpack
                  (FMR.assignKeysAndData @(BR.CatColsASER V.++ '[BR.StateAbbreviation, BR.Year, ET.Office, ET.ElectoralWeightSource, ET.ElectoralWeightOf])
                   @[PUMS.Citizens, ET.ElectoralWeight, ET.DemVPV, BR.DemPref])
                  (FMR.foldAndAddKey foldPrefAndTurnoutData)
      aser5DemoF = FMR.concatFold $ FMR.mapReduceFold
                   FMR.noUnpack
                   (FMR.assignKeysAndData @(BR.CatColsASER5 V.++ '[BR.StateAbbreviation, BR.Year, ET.Office, ET.ElectoralWeightSource, ET.ElectoralWeightOf])
                     @[PUMS.Citizens, ET.ElectoralWeight, ET.DemVPV, BR.DemPref])
                   (FMR.foldAndAddKey foldPrefAndTurnoutData)
                   
      aserByState = FL.fold aserDemoF aserAllByState
      aser5ByState = FL.fold aser5DemoF aser5AllByState
      
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

      aser5Prefs y o = fmap (F.rcast @('[BR.StateAbbreviation] V.++ BR.CatColsASER5 V.++ '[BR.DemPref]))
                      $ F.filterFrame (yearOfficeFilter y o) inferredPrefsASER5
      aser5Demo y o = fmap (F.rcast @('[BR.StateAbbreviation] V.++ BR.CatColsASER5 V.++ '[PUMS.Citizens]))
                    $ F.filterFrame (yearFilter y) pumsASER5ByState
                      
      aser5CensusEW y o = fmap (F.rcast @('[BR.StateAbbreviation] V.++ BR.CatColsASER5 V.++ '[ET.ElectoralWeightSource, ET.ElectoralWeightOf, ET.ElectoralWeight]))
                        $ F.filterFrame (yearFilter y)  aser5DemoAndAdjCensusEW

      aser5CCESEW y = fmap (F.rcast @('[BR.StateAbbreviation] V.++ BR.CatColsASER5 V.++ '[ET.ElectoralWeightSource, ET.ElectoralWeightOf, ET.ElectoralWeight]))
                     $ F.filterFrame (yearFilter y) aser5DemoAndAdjCCESEW                     

      aserWgtdByCensus2008 = prepRows 2008 ET.EW_Census BR.ASER
                             <$> mergeASERElectionData
                             (aserDemo 2016 ET.President)
                             (aserPrefs 2016 ET.President)
                             "2008 Census "
                             (aserCensusEW 2008 ET.President)
                             electoralVotesByStateFrame 
      aserWgtdByCensus2012 = prepRows 2012 ET.EW_Census BR.ASER
                            <$> mergeASERElectionData
                            (aserDemo 2016 ET.President)
                            (aserPrefs 2016 ET.President)
                            "2012 Census"
                            (aserCensusEW 2012 ET.President)
                            electoralVotesByStateFrame
      aserWgtdByCensus2016 = prepRows 2016 ET.EW_Census BR.ASER
                            <$> mergeASERElectionData
                            (aserDemo 2016 ET.President)
                            (aserPrefs 2016 ET.President)
                            "2016 Census"
                            (aserCensusEW 2016 ET.President)
                            electoralVotesByStateFrame
      aserWgtdByCensus2018 = prepRows 2018 ET.EW_Census BR.ASER
                            <$> mergeASERElectionData
                            (aserDemo 2018 ET.House)
                            (aserPrefs 2016 ET.President)
                            "2018 Census"
                            (aserCensusEW 2018 ET.House)
                            electoralVotesByStateFrame                            
                     
      aserWgtdByCCES yDemo oDemo yTurnout = prepRows yTurnout ET.EW_CCES BR.ASER
                                            <$> mergeASERElectionData
                                            (aserDemo yDemo oDemo)
                                            (aserPrefs 2016 ET.President)
                                            ((T.pack $ show yTurnout) <> " CCES")
                                            (aserCCESEW yTurnout)
                                            electoralVotesByStateFrame
      aserCCES = mconcat <$> traverse (aserWgtdByCCES 2016 ET.President) [2008, 2012, 2016]
      aserCCES2 = mconcat <$> sequence [aserWgtdByCCES 2016 ET.President 2016, aserWgtdByCCES 2018 ET.House 2018]      
  aserWgtd <- BR.retrieveOrMakeFrame "mrp/weights/aserWgtd.bin" (mconcat <$> sequence [aserWgtdByCensus2008, aserWgtdByCensus2012, aserWgtdByCensus2016, aserCCES])  
  aserWgtd2 <- BR.retrieveOrMakeFrame "mrp/weights/aserWgtd2.bin" (mconcat <$> sequence [aserWgtdByCensus2016, aserWgtdByCensus2018, aserCCES2])      
  let aserEwResults = FL.fold ewResultsF aserWgtd
      aserEwResults2 = FL.fold ewResultsF aserWgtd2
  logFrame aserEwResults

---
  let aser5WgtdByCensus2008 = prepRows 2008 ET.EW_Census BR.ASER5
                             <$> mergeASER5ElectionData
                             (aser5Demo 2016 ET.President)
                             (aser5Prefs 2016 ET.President)
                             "2008 Census"
                             (aser5CensusEW 2008 ET.President)
                             electoralVotesByStateFrame 
      aser5WgtdByCensus2012 = prepRows 2012 ET.EW_Census BR.ASER5
                            <$> mergeASER5ElectionData
                            (aser5Demo 2016 ET.President)
                            (aser5Prefs 2016 ET.President)
                            "2012 Census"
                            (aser5CensusEW 2012 ET.President)
                            electoralVotesByStateFrame
      aser5WgtdByCensus2016 = prepRows 2016 ET.EW_Census BR.ASER5
                            <$> mergeASER5ElectionData
                            (aser5Demo 2016 ET.President)
                            (aser5Prefs 2016 ET.President)
                            "2016 Census"
                            (aser5CensusEW 2016 ET.President)
                            electoralVotesByStateFrame
      aser5WgtdByCensus2018 = prepRows 2018 ET.EW_Census BR.ASER5
                            <$> mergeASER5ElectionData
                            (aser5Demo 2018 ET.House)
                            (aser5Prefs 2016 ET.President)
                            "2018 Census"
                            (aser5CensusEW 2018 ET.House)
                            electoralVotesByStateFrame                            
                     
      aser5WgtdByCCES yDemo oDemo yTurnout = prepRows yTurnout ET.EW_CCES BR.ASER5
                                            <$> mergeASER5ElectionData
                                            (aser5Demo yDemo oDemo)
                                            (aser5Prefs 2016 ET.President)
                                            ((T.pack $ show yTurnout) <> " CCES")
                                            (aser5CCESEW yTurnout)
                                            electoralVotesByStateFrame
      aser5CCES = mconcat <$> traverse (aser5WgtdByCCES 2016 ET.President) [2008, 2012, 2016]
      aser5CCES2 = mconcat <$> sequence [aser5WgtdByCCES 2016 ET.President 2016, aserWgtdByCCES 2018 ET.House 2018]      
  aser5Wgtd <- BR.retrieveOrMakeFrame "mrp/weights/aser5Wgtd.bin" (mconcat <$> sequence [aser5WgtdByCensus2008, aser5WgtdByCensus2012, aser5WgtdByCensus2016, aser5CCES])  
  aser5Wgtd2 <- BR.retrieveOrMakeFrame "mrp/weights/aser5Wgtd2.bin" (mconcat <$> sequence [aser5WgtdByCensus2016, aser5WgtdByCensus2018, aser5CCES2])      
  let aser5EwResults = FL.fold ewResultsF aser5Wgtd 
      aser5EwResults2 = FL.fold ewResultsF aser5Wgtd2
  logFrame aser5EwResults

---

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
        brAddMarkDown text2
        _ <-  K.addHvega Nothing Nothing
              $ vlWeights
              "2016 Vote share and EVs for various electoral weights"
              (FV.ViewConfig 400 400 5)
              (F.filterFrame ((==2016) . F.rgetField @BR.Year) $ aserEwResults <> aser5EwResults)
        brAddMarkDown text3
        _ <-  K.addHvega Nothing Nothing
              $ vlWeights
              "Vote share and EVs: 2016 weightings vs. 2018 weightings"
              (FV.ViewConfig 400 400 5)
              (aserEwResults2 <> aser5EwResults2)
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
      voteShareDomain = GV.SDomain $ GV.DNumbers [48.5, 53.5]
      evDomain = GV.SDomain $ GV.DNumbers [180, 350]
      encVSRuleX = GV.position GV.X [GV.PName "50%", GV.PmType GV.Quantitative, GV.PScale [voteShareDomain], GV.PNoTitle]
      makeEVRuleVal = GV.calculateAs "269" "Evenly Split"
      encEVRuleY = GV.position GV.Y [GV.PName "Evenly Split", GV.PmType GV.Quantitative, GV.PScale [evDomain], GV.PNoTitle]
      makeSourceType = GV.calculateAs "datum.ElectoralWeightSource + '/' + datum.DemographicGrouping" "Weight Source"
      encX = GV.position GV.X [GV.PName "Vote Share (%)"
                              , GV.PmType GV.Quantitative
                              , GV.PScale [voteShareDomain]
                              , GV.PTitle "D Vote Share (%)"]
      encY = GV.position GV.Y [FV.pName @ElectorsD
                              , GV.PmType GV.Quantitative
                              , GV.PScale [evDomain]
                              , GV.PTitle "# D Electoral Votes"
                              ]
      encColor = GV.color [FV.mName @WeightSource, GV.MmType GV.Nominal]
      encShape = GV.shape [FV.mName @BR.DemographicGroupingC, GV.MmType GV.Nominal]
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

mergeASER5ElectionData :: K.KnitEffects r
  => F.FrameRec ('[BR.StateAbbreviation] V.++ BR.CatColsASER5 V.++  '[PUMS.Citizens])
  -> F.FrameRec ('[BR.StateAbbreviation] V.++ BR.CatColsASER5 V.++  '[BR.DemPref])
  -> T.Text
  -> F.FrameRec ('[BR.StateAbbreviation] V.++ BR.CatColsASER5 V.++  '[ET.ElectoralWeightSource, ET.ElectoralWeightOf, ET.ElectoralWeight])
  -> F.Frame BR.ElectoralCollege
  -> K.Sem r (F.FrameRec [WeightSource, BR.StateAbbreviation, BR.Electors, PUMS.Citizens, BR.DemPref, ET.ElectoralWeight])
mergeASER5ElectionData demographics prefs wSource ews eCollege = K.wrapPrefix "ASER5" $ do
  K.logLE K.Diagnostic $ "Merging for " <> wSource
  merged <- mergeElectionData @'[BR.StateAbbreviation] @BR.CatColsASER5 demographics prefs wSource ews
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


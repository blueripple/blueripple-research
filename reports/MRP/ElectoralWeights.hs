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
import qualified Data.List as L
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
Why did the Trump campaign ask CNN to retract a poll and why did CNN refuse?

There are a number of things that make polls confusing and difficult to interpret.  Among them, choosing
how to survey people (land-line phones?  Cell-phones?  Facebook survey?),
selecting which questions to ask and how to phrase them, and then interpreting the data to reach some
conclusion, e.g., to predict the outcome of an upcoming election. All these complexities are worth
talking about, but here we're going to focus on the last question: how do we go from polling-data to
prediction?

1. **Polling and Prediction 101**
2. **CNN vs. Trump**
3. **Example: The 2016 Presidential Election**
4. **Model Notes**

## Polling and Prediction 101
Predicting election outcomes via polling or survey data is hard: the people you survey
may not be representative of the electorate, and the composition of
the electorate in each state changes election-to-election.
When you conduct a poll or survey, you cannot control who responds.  So your sample will be
different in myriad ways from the population as a whole,  and also different from the subset
of the population who will vote.

Reconstructing the opinion of the population from a poll is done via weighting,
assigning a number to each response that allow users of the data to
reconstruct a representative sample of the population from the survey.  For example, if
a state has roughly the same number of adults above and below 45 but the survey has
fewer responses from people over 45, those responses will be assigned higher weights.  Weighting
is complex, not least because there are many demographic variables to be considered.

With weighting, we have an estimate of how a person in a particular place is likely to vote given
various demographic information about them.  And, from the census and other sources, we
know the demographics of each county/district/state.  But if we want to predict an election
outcome, or decide where we need to focus resources because an election is likely to be close,
we need to know who is going to cast a ballot in the election.  

There are a lot of reasons people do and don't
vote: see [our piece][BR:ID] on voter ID, or Fair Vote's [rundown][FV:Turnout] on
voter turnout, for example.  For the purposes of this post we're going to ignore
the reasons, and focus on the data problem of uncertain turnout.

Figuring out how to weight opinion data to predict voting is among the
most difficult parts of determining which elections/states are likely to be close and
thus where to prioritize work and the allocation of resources.

## CNN vs. Trump
Weighting polls is also deeply contentious. As an extreme example,
CNN recently published a [poll][CNN:20200608_Poll] showing Biden up 14 points
over Trump.  The Trump campaign hired a pollster to "analyze" the CNN poll and then
[demanded CNN retract the poll][CNN:Demand], which CNN promptly
[refused][CNN:RefuseRetract] to do.  The substantive objection made by Trump's
campaign was that CNN should have *weighted their poll differently*, such that the fraction
of Republicans, Democrats and Independents was the same as among voters in 2016.  It's not hard to
see why this is a bad idea.  Firstly, as people's views on candidates change,
so may their reported party.  So weighting to fix partisan identity will tend to obscure
exactly the thing you are trying to measure.  If partisan identity were a stable feature of
the electorate, it might still make sense to weight this way, just to control for a bad polling
sample.  But partisan identification is not particularly stable!
In the chart below we plot partisan identity as reported
in the CCES survey from 2006-2018 to see how much it shifts election to election.
From the chart we see that though voters are roughly equally distributed between
Democrats, Independents and Republicans, they shift around among those groups
a significant amount.


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

##Example: The 2016 Presidential Election
So, does the last election provide any useful guidance for weighting?
Using partisan identity is particularly bad since it is so heavily correlated
the thing a poll is trying to measure.  But perhaps if we just weight
demographically, that is assume that the turnout in any given demographic group
in any state will be similar to 2016, this might provide reasonable weights?
This doesn't have the correlation
issue of partisan identity but turnout by demographic group is also
variable election-to-election.  And there are two other issues:
data sources on turnout vary
dramatically and how one breaks down the demographics can change the results as well.
To illustrate both these points, we built a few simple electorate models from
2016 data, either from the census [CPS voter survey][Census:CPSVoter]
or the [CCES survey data][CCES].
For each data-set, we used [multi-level regression][BR:MRP] to infer
turnout probabilities in each state for 2 demographic groupings, differing only in the
level of detail we use when considering a person's race:

- **ASER**: we infer turnout for 16 groups per state,
categorizing everyone by
age (45 and Over/Under 45), sex (Female/Male),
education (Non-College-Grad/College Grad or In College),
and race (Non-White/White-Non-Latinx).

- **ASER5**:  we infer turnout for 40 groups per state,
categorizing everyone by
age (45 and Over/Under 45), sex (Female/Male),
education (Non-College-Grad/College Grad or In College),
and race (Black/Latinx/Asian/Other/White-Non-Latinx).

We further adjust these inferred turnout numbers such that, when
multiplied by the number of people in each group in the state, we reproduce
the actual number of ballots cast on election day.
If you're interested in the details, we followed a simplified version
of the techniques described in [this paper][DeepInteractions].

To illustrate how different these electorate weights are,
we apply them to the 2016 election itself, using the
census demographics in each state and voter preferences inferred
from the CCES data using the same multi-level regression methods.
From that we compute the two-party vote share in each state and get 
4 different 2016 outcomes in terms of national popular vote and electoral
votes.  This is not something that would have been an actual
forecast.  For that, we would want to simulate many elections with the
given set of probabilities.  But these correspond to the average outcome
with each of these electorate compositions and so give a picture of
how different they are.

The results are charted below and vary,
from a Dem popular vote share below 48.5% and 180 electoral votes to a popular vote share of about 50.5% and
almost 280 electoral votes! That's a swing of almost 3% in the popular vote (~4 million votes)
and the difference between losing and winning the election.  

[CNN:RefuseRetract]: <https://www.cnn.com/2020/06/10/politics/cnn-letter-to-trump-over-poll/index.html>
[CNN:Demand]: <https://www.cnn.com/2020/06/10/politics/trump-campaign-cnn-poll/index.html>
[CNN:20200608_Poll]: <https://www.cnn.com/2020/06/08/politics/cnn-poll-trump-biden-chaotic-week/index.html>
[AAPOR:LikelyVoters]: <https://www.aapor.org/Education-Resources/Election-Polling-Resources/Likely-Voters.aspx>
[ElectProject:CPSOverReport]: <http://www.electproject.org/home/voter-turnout/cps-methodology>
[Census:CPSVoter]: <https://www.census.gov/topics/public-sector/voting.html>

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
The Census based models look more like the actual election outcomes,
though that could just be a coincidence.
The census-sourced electoral weights indicate higher turnout from younger
and minority voters than does the CCES data,
and is thus more Democratic-candidate-friendly than the CCES data.

For a given data-set, there are significant differences in the ASER and
ASER5 models.  Popular vote difference comes from correlation between
turnout and preference among the more specific race categories.
For instance, in 2016, Black voters were more likely to vote for Democratic
candidates *and* turned out to vote at higher rates than Latinx voters.
So separating those groups out, results in a model with higher
Dem vote-share.  The difference in electoral college outcome is even
larger--electoral votes jump by more than you might expect from just the
shift in popular vote--suggesting that the specific places that
people of various non-white racial backgrounds live is advantageous to a
Democratic Presidential candidate. In other words, the extra vote-share
we see from looking at race in greater detail is disproportionately
located in some close (battleground) states.

None of these models is particularly close to the actual election we had,
and this is using data from an election to model the same election.
Blindly using that data to model the 2020 election would be foolhardy.

What do good pollsters actually do? They ask respondents a variety of questions to gauge
how likely they are to vote.  They combine this with demographic weighting (like
the examples above) to estimate the makeup of the electorate. This is complicated
and hard to get right, and is part of what makes some pollsters more reliable than
others. Previous elections may be used as sanity checks or as the
baseline for non-poll-based models.

None of these issues is simple or settled.  For example, a [pollster in Utah
recently decided to begin using education in their weighting][UTPT:Weighting], something
they had not been using before, because they realized they had made errors
in 2016 by ignoring it.

Election modelers then use these polls as well as fundamentals---economic trends,
incumbency, etc.---to predict the probabilities of various outcomes. For a particularly
thorough explanation of one such model,
see the [Economist's summary][Economist:ElectionModel] of their work for 2020.

[UTPT:Weighting]: <https://www.utpoliticaltrends.com/thedeeperstate/the-importance-of-weighting>
[Economist:ElectionModel]: <https://projects.economist.com/us-2020-forecast/president/how-this-works>
[UpshotModel]: <https://www.nytimes.com/2016/06/10/upshot/how-we-built-our-model.html>
[Vox:EducationWeighting]: <https://www.vox.com/policy-and-politics/2019/11/14/20961794/education-weight-state-polls-trump-2020-election>
|]

modelingNotes :: T.Text
modelingNotes = [i|
## Model Notes
As we continue to work with this data we refine and improve our modeling.
In this post we have shifted from using the census summary of the CPS voter supplement
to using the CPS micro-data itself, as harmonized via the [IPUMS][IPUMS-CPS] web portal.
This has several advantages.  Firstly, the micro-data
allows us to get turnout numbers at the *state* level.
Also useful is that the micro-data has more demographic information.  For example,
the summary tables allow us to explore the variables of age, sex and race *or*
age, sex, and education but not age, sex, education, *and* race.  This is important since
the combination of education and race is necessary to explore the voting and turnout
patterns of the so-called "White Working Class," a crucial voting block in the past few
elections.
Another nice benefit is that the micro-data contains more information about voting.  It includes
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

post :: forall r.(K.KnitMany r, K.CacheEffectsD r, K.Member GLM.RandomFu r) => Bool -> K.Sem r ()
post updated = P.mapError BR.glmErrorToPandocError $ K.wrapPrefix "ElectoralWeights" $ do
  K.logLE K.Info "Loading re-keyed demographic and turnout data."

  -- state totals and national demographic splits
--  stateTurnoutRaw <- BR.stateTurnoutLoader

  let predictorsASER = fmap GLM.Predictor (BR.allSimplePredictors @BR.CatColsASER)
      predictorsASER5 = fmap GLM.Predictor (BR.allSimplePredictors @BR.CatColsASER5)
      statesAfter y r = F.rgetField @BR.Year r > y && F.rgetField @BR.StateAbbreviation r /= "National"

  cachedCCES_Data <- CCES.ccesDataLoader
  partisanIdC <-     
    BR.retrieveOrMakeFrame "mrp/weights/partisanId.bin" cachedCCES_Data $ \ccesData -> do
      let voted r = F.rgetField @CCES.Turnout r == CCES.T_Voted
          withPartyId r = F.rgetField @CCES.PartisanId3 r `elem` [CCES.PI3_Democrat, CCES.PI3_Republican, CCES.PI3_Independent]
          votedWithId r = voted r && withPartyId r 
          partisanIdF = FMR.concatFold
                        $ FMR.mapReduceFold
                        (FMR.unpackFilterRow votedWithId)
                        (FMR.assignKeysAndData @[BR.Year, CCES.PartisanId3] @'[CCES.CCESWeightCumulative])
                        (FMR.foldAndAddKey $ FF.foldAllConstrained @Num FL.sum)
          partisanId = FL.fold partisanIdF ccesData
          totalByYearF = FMR.concatFold
                         $ FMR.mapReduceFold
                         FMR.noUnpack
                         (FMR.assignKeysAndData @'[BR.Year] @'[CCES.CCESWeightCumulative])
                         (FMR.foldAndAddKey $ FF.foldAllConstrained @Num FL.sum)
          totalWeight = fmap (FT.retypeColumn @CCES.CCESWeightCumulative @'("TotalWeight", Double))
                        $ FL.fold totalByYearF partisanId
      partisanIdFraction <- do
        withWeights <- K.knitEither
                       $ either (Left . T.pack . show) Right
                       $ FJ.leftJoinE @'[BR.Year] partisanId totalWeight
        let w = F.rgetField @CCES.CCESWeightCumulative
            t = F.rgetField @'("TotalWeight", Double)
            getFraction :: F.Record '[CCES.CCESWeightCumulative, '("TotalWeight", Double)] -> F.Record '[ '("Fraction", Double)]
            getFraction r = (w r/ t r) F.&: V.RNil
        return $ fmap (FT.transform getFraction) withWeights
      return partisanIdFraction

--  let (K.WithCacheTime _ partisanIdA) = partisanIdC
--  partisanId <- partisanIdA
  K.ignoreCacheTime partisanIdC >>= logFrame 

  cachedCPS_VoterPUMS_Data <- CPS.cpsVoterPUMSLoader
  inferredCensusTurnoutASER_C <- fmap (fmap (F.filterFrame (statesAfter 2007))) . 
    BR.retrieveOrMakeFrame "mrp/turnout/censusSimpleASER_MR.bin" cachedCPS_VoterPUMS_Data $ const $                                
      BR.mrpTurnout @BR.CatColsASER
      GLM.MDVNone
      (Just "T_CensusASER")
      ET.EW_Census
      ET.EW_Citizen
      cachedCPS_VoterPUMS_Data
      (CPS.cpsCountVotersByStateF $ CPS.cpsKeysToASER True . F.rcast)
      predictorsASER
      BR.catPredMaps
                                                          
  inferredCensusTurnoutASER5_C <- fmap (fmap (F.filterFrame (statesAfter 2007))) . 
    BR.retrieveOrMakeFrame "mrp/turnout/censusASER5_MR.bin" cachedCPS_VoterPUMS_Data $ const $ 
      BR.mrpTurnout @BR.CatColsASER5
        GLM.MDVNone
        (Just "T_CensusASER5")
        ET.EW_Census
        ET.EW_Citizen
        cachedCPS_VoterPUMS_Data
        (CPS.cpsCountVotersByStateF $ CPS.cpsKeysToASER5 True . F.rcast)
        predictorsASER5
        BR.catPredMaps

  -- preferences
  inferredPrefsASER_C <-  (fmap (F.filterFrame (statesAfter 2007))) <$> do
    BR.retrieveOrMakeFrame "mrp/simpleASER_MR.bin" cachedCCES_Data $ const $ 
      BR.mrpPrefs @BR.CatColsASER GLM.MDVNone (Just "ASER") cachedCCES_Data predictorsASER BR.catPredMaps

      
  inferredPrefsASER5_C <-  (fmap (F.filterFrame (statesAfter 2007))) <$> do
    BR.retrieveOrMakeFrame "mrp/prefsASER5_MR.bin" cachedCCES_Data $ const $
      BR.mrpPrefs @BR.CatColsASER5 GLM.MDVSimple (Just "T_prefsASER5") cachedCCES_Data predictorsASER5 BR.catPredMaps

  -- inferred turnout
  inferredCCESTurnoutOfAllASER_C <- (fmap (F.filterFrame (statesAfter 2007))) <$> do
    BR.retrieveOrMakeFrame "mrp/turnout/ccesOfAllSimpleASER_MR.bin" cachedCCES_Data $ const $
      BR.mrpTurnout @BR.CatColsASER
        GLM.MDVNone
        (Just "T_OfAllASER")
        ET.EW_CCES
        ET.EW_Citizen
        cachedCCES_Data
        (BR.countVotersOfAllF @BR.CatColsASER)
        predictorsASER
        BR.catPredMaps


  inferredCCESTurnoutOfAllASER5_C <- (fmap (F.filterFrame (statesAfter 2007))) <$> do
    BR.retrieveOrMakeFrame "mrp/turnout/ccesOfAllASER5_MR.bin" cachedCCES_Data $ const $
      BR.mrpTurnout @BR.CatColsASER5
        GLM.MDVNone
        (Just "T_OfAllASER5")
        ET.EW_CCES
        ET.EW_Citizen
        cachedCCES_Data
        (BR.countVotersOfAllF @BR.CatColsASER5)
        predictorsASER5
        BR.catPredMaps

{-                                  
  K.logLE K.Info $ "GA CCES Turnout (inferred)"
  logFrame $ F.filterFrame gaFilter inferredCCESTurnoutOfAllASER5
-}                           
  --  logFrame inferredTurnoutASE
  -- demographics
  cachedPUMS_Demographics <- PUMS.pumsLoader
  pumsASERByStateC <- (fmap (F.filterFrame (statesAfter 2007))) <$> do
    BR.retrieveOrMakeFrame "mrp/weights/pumsASERByState.bin" cachedPUMS_Demographics $ \pumsDemographics -> do
      let rollup = fmap (FT.mutate $ const $ FT.recordSingleton @BR.PopCountOf BR.PC_Citizen)                         
                   $ FL.fold (PUMS.pumsStateRollupF $ PUMS.pumsKeysToASER True . F.rcast) pumsDemographics
          addDefaultsOneF :: FL.Fold (F.Record (BR.CatColsASER V.++ [PUMS.NonCitizens, BR.PopCountOf, PUMS.Citizens]))
                             (F.FrameRec (BR.CatColsASER V.++ [PUMS.NonCitizens, BR.PopCountOf, PUMS.Citizens]))
          addDefaultsOneF = fmap F.toFrame $ Keyed.addDefaultRec @BR.CatColsASER (0 F.&: BR.PC_Citizen F.&: 0 F.&: V.RNil)
          addDefaultsF = FMR.concatFold
                         $ FMR.mapReduceFold
                         FMR.noUnpack
                         (FMR.assignKeysAndData @[BR.Year, BR.StateAbbreviation, BR.StateFIPS]) 
                         (FMR.makeRecsWithKey id $ FMR.ReduceFold $ const addDefaultsOneF)
      return $ FL.fold addDefaultsF rollup
  
  pumsASER5ByStateC <- (fmap (F.filterFrame (statesAfter 2007))) <$> do
    BR.retrieveOrMakeFrame "mrp/weights/pumsASER5ByState.bin" cachedPUMS_Demographics $ \pumsDemographics -> do
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
{-                      
  K.logLE K.Info $ "GA Demographics (PUMS, ASER5)"
  logFrame $ F.filterFrame gaFilter pumsASER5ByState
-}

  K.logLE K.Info "Adjusting national census turnout via PUMS demographics and total recorded turnout"
  aserDemoAndAdjCensusEW_C <- do
    cachedStateTurnout <- BR.stateTurnoutLoader
    let cachedDeps = (,,) <$> cachedStateTurnout <*> pumsASERByStateC <*> inferredCensusTurnoutASER_C
    BR.retrieveOrMakeFrame "turnout/aserPumsDemoAndAdjCensusEW.bin" cachedDeps $ \(stateTurnout, pumsASERByState, inferredCensusTurnoutASER) -> 
      BR.demographicsWithAdjTurnoutByState
        @BR.CatColsASER
        @PUMS.Citizens
        @'[PUMS.NonCitizens, BR.PopCountOf, BR.StateFIPS]
        @'[BR.Year, BR.StateAbbreviation]
        stateTurnout
        (fmap F.rcast pumsASERByState)
        (fmap F.rcast inferredCensusTurnoutASER)

  aser5DemoAndAdjCensusEW_C <- do
    cachedStateTurnout <- BR.stateTurnoutLoader
    let cachedDeps = (,,) <$> cachedStateTurnout <*> pumsASER5ByStateC <*> inferredCensusTurnoutASER5_C
    BR.retrieveOrMakeFrame "turnout/aser5PumsDemoAndAdjCensusEW.bin" cachedDeps $ \(stateTurnout, pumsASER5ByState, inferredCensusTurnoutASER5) -> 
      BR.demographicsWithAdjTurnoutByState
        @BR.CatColsASER5
        @PUMS.Citizens
        @'[PUMS.NonCitizens, BR.PopCountOf, BR.StateFIPS]
        @'[BR.Year, BR.StateAbbreviation]
        stateTurnout
        (fmap F.rcast pumsASER5ByState)
        (fmap F.rcast inferredCensusTurnoutASER5)

  let filterForAdjTurnout r = F.rgetField @BR.StateAbbreviation r == "WI" && F.rgetField @BR.Year r == 2016

  K.logLE K.Info "Adjusting CCES inferred turnout via PUMS demographics and total recorded turnout."
  aserDemoAndAdjCCESEW_C <- do
    cachedStateTurnout <- BR.stateTurnoutLoader
    let cachedDeps = (,,) <$> cachedStateTurnout <*> pumsASERByStateC <*> inferredCCESTurnoutOfAllASER_C
    BR.retrieveOrMakeFrame "turnout/aserPumsDemoAndAdjCCESEW.bin" cachedDeps $ \(stateTurnout, demographics, prefs) -> 
      BR.demographicsWithAdjTurnoutByState
        @BR.CatColsASER
        @PUMS.Citizens
        @'[PUMS.NonCitizens, BR.PopCountOf, BR.StateFIPS]
        @'[BR.Year, BR.StateAbbreviation] stateTurnout (fmap F.rcast demographics) (fmap F.rcast prefs)

  aser5DemoAndAdjCCESEW_C <- do
    cachedStateTurnout <- BR.stateTurnoutLoader
    let cachedDeps = (,,) <$> cachedStateTurnout <*> pumsASER5ByStateC <*> inferredCCESTurnoutOfAllASER5_C
    BR.retrieveOrMakeFrame "turnout/aser5PumsDemoAndAdjCCESEW.bin" cachedDeps $ \(stateTurnout, demographics, prefs) -> 
      BR.demographicsWithAdjTurnoutByState
        @BR.CatColsASER5
        @PUMS.Citizens
        @'[PUMS.NonCitizens, BR.PopCountOf, BR.StateFIPS]
        @'[BR.Year, BR.StateAbbreviation] stateTurnout (fmap F.rcast demographics) (fmap F.rcast prefs)
  
  K.logLE K.Info "Computing pres-election 2-party vote-share"
  presPrefByStateFrame <- do
    let fld = FMR.concatFold $ FMR.mapReduceFold
              MR.noUnpack
              (FMR.assignKeysAndData @[BR.Year, BR.State, BR.StateAbbreviation, BR.StateFIPS, ET.Office])
              (FMR.foldAndAddKey votesToVoteShareF)
    presByStateFrame <- K.ignoreCacheTimeM $ BR.presidentialByStateFrame
    return $ FL.fold fld presByStateFrame

    
  K.logLE K.Info "Computing house election 2-party vote share"
  let houseElectionFilter r = (F.rgetField @BR.Stage r == "gen")
                              && (F.rgetField @BR.Runoff r == False)
                              && (F.rgetField @BR.Special r == False)
                              && (F.rgetField @ET.Party r == ET.Democratic || F.rgetField @ET.Party r == ET.Republican)
  houseElectionFrame <- F.filterFrame houseElectionFilter <$> K.ignoreCacheTimeM BR.houseElectionsLoader
  let houseVoteShareF = FMR.concatFold $ FMR.mapReduceFold
                        FMR.noUnpack
                        (FMR.assignKeysAndData @[BR.Year, BR.StateAbbreviation, BR.StateFIPS, BR.CongressionalDistrict, ET.Office])
                        (FMR.foldAndAddKey votesToVoteShareF)
      houseVoteShareFrame = FL.fold houseVoteShareF houseElectionFrame
  K.logLE K.Info "Joining turnout by CD and prefs"
  electoralVotesByStateFrame <- F.filterFrame ((==2020) . F.rgetField @BR.Year) <$> K.ignoreCacheTimeM BR.electoralCollegeFrame
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

      ewGroupingAsText :: ET.ElectoralWeightSourceT -> T.Text
      ewGroupingAsText ET.EW_CCES = "CCES"
      ewGroupingAsText ET.EW_Census = "Census"
      
  aserWgtd <- K.ignoreCacheTimeM $ do
    let cachedDeps = (,,,) <$> inferredPrefsASER_C <*> pumsASERByStateC <*> aserDemoAndAdjCensusEW_C <*> aserDemoAndAdjCCESEW_C

        aserPrefs pFrame y o = fmap (F.rcast @('[BR.StateAbbreviation] V.++ BR.CatColsASER V.++ '[BR.DemPref]))
                               $ F.filterFrame (yearOfficeFilter y o) pFrame
        aserDemo demoFrame y = fmap (F.rcast @('[BR.StateAbbreviation] V.++ BR.CatColsASER V.++ '[PUMS.Citizens]))
                                 $ F.filterFrame (yearFilter y) demoFrame
        aserEW ewFrame y  = fmap (F.rcast @('[BR.StateAbbreviation] V.++ BR.CatColsASER V.++ '[ET.ElectoralWeightSource, ET.ElectoralWeightOf, ET.ElectoralWeight]))
                            $ F.filterFrame (yearFilter y)  ewFrame
                            
        aserWgtd pFrame demoFrame ewFrame ewGrouping yDemo yPrefs oPrefs yTurnout =
          prepRows yTurnout ewGrouping BR.ASER
          <$> mergeASERElectionData
          (aserDemo demoFrame yDemo)
          (aserPrefs pFrame yPrefs oPrefs)
          ((T.pack $ show yTurnout) <> " " <>  (ewGroupingAsText ewGrouping))
          (aserEW ewFrame yTurnout)
          electoralVotesByStateFrame                                                
                                                      
    BR.retrieveOrMakeFrame "mrp/weights/aserWgtd.bin" cachedDeps $ \(inferredPrefsASER, pumsASERByState, aserDemoAndAdjCensusEW, aserDemoAndAdjCCESEW) -> do
      let aserCCES = mconcat <$> traverse (aserWgtd inferredPrefsASER pumsASERByState aserDemoAndAdjCCESEW ET.EW_CCES 2016 2016 ET.President) [2008, 2012, 2016]
          aserCensus = mconcat <$> traverse (aserWgtd inferredPrefsASER pumsASERByState aserDemoAndAdjCensusEW ET.EW_Census 2016 2016 ET.President) [2008, 2012, 2016]
      mconcat <$> sequence [aserCCES, aserCensus]

  let aserEwResults = FL.fold ewResultsF aserWgtd
  logFrame aserEwResults


  aser5Wgtd <- K.ignoreCacheTimeM $ do
    let cachedDeps = (,,,) <$> inferredPrefsASER5_C <*> pumsASER5ByStateC <*> aser5DemoAndAdjCensusEW_C <*> aser5DemoAndAdjCCESEW_C

        aser5Prefs pFrame y o = fmap (F.rcast @('[BR.StateAbbreviation] V.++ BR.CatColsASER5 V.++ '[BR.DemPref]))
                                $ F.filterFrame (yearOfficeFilter y o) pFrame
        aser5Demo demoFrame y = fmap (F.rcast @('[BR.StateAbbreviation] V.++ BR.CatColsASER5 V.++ '[PUMS.Citizens]))
                                $ F.filterFrame (yearFilter y) demoFrame
        aser5EW ewFrame y  = fmap (F.rcast @('[BR.StateAbbreviation] V.++ BR.CatColsASER5 V.++ '[ET.ElectoralWeightSource, ET.ElectoralWeightOf, ET.ElectoralWeight]))
                             $ F.filterFrame (yearFilter y)  ewFrame
                            
        aser5Wgtd pFrame demoFrame ewFrame ewGrouping yDemo yPrefs oPrefs yTurnout =
          prepRows yTurnout ewGrouping BR.ASER5
          <$> mergeASER5ElectionData
          (aser5Demo demoFrame yDemo)
          (aser5Prefs pFrame yPrefs oPrefs)
          ((T.pack $ show yTurnout) <> " " <>  (ewGroupingAsText ewGrouping))
          (aser5EW ewFrame yTurnout)
          electoralVotesByStateFrame                                                
                                                      
    BR.retrieveOrMakeFrame "mrp/weights/aser5Wgtd.bin" cachedDeps $ \(inferredPrefsASER5, pumsASER5ByState, aser5DemoAndAdjCensusEW, aser5DemoAndAdjCCESEW) -> do
      let aser5CCES = mconcat <$> traverse (aser5Wgtd inferredPrefsASER5 pumsASER5ByState aser5DemoAndAdjCCESEW ET.EW_CCES 2016 2016 ET.President) [2008, 2012, 2016]
          aser5Census = mconcat <$> traverse (aser5Wgtd inferredPrefsASER5 pumsASER5ByState aser5DemoAndAdjCensusEW ET.EW_Census 2016 2016 ET.President) [2008, 2012, 2016]

      mconcat <$> sequence [aser5CCES, aser5Census]

  let aser5EwResults = FL.fold ewResultsF aser5Wgtd 
      cces2016 r = F.rgetField @BR.Year r == 2016 && F.rgetField @ET.ElectoralWeightSource r == ET.EW_CCES
      prefsFilter st r = F.rgetField @BR.Year r == 2016 && F.rgetField @BR.StateAbbreviation r == st && F.rgetField @ET.Office r == ET.President
      turnoutFilter st r = F.rgetField @BR.Year r == 2016 && F.rgetField @BR.StateAbbreviation r == st
  
  logFrame aser5EwResults

---
  partisanId <- K.ignoreCacheTime partisanIdC
  curDate <-  (\(Time.UTCTime d _) -> d) <$> K.getCurrentTime
  let pubDateElectoralWeights =  Time.fromGregorian 2020 7 11
  K.newPandoc
    (K.PandocInfo ((postRoute PostElectoralWeights) <> "main")
      (brAddDates updated pubDateElectoralWeights curDate
       $ M.fromList [("pagetitle", "Polling Explainer: CNN vs. Trump")
                    ,("title","Polling Explainer: CNN vs. Trump")
                    ]
      ))
      $ do        
        brAddMarkDown text1

        _ <- K.addHvega Nothing Nothing
          $ vlPartisanIdOverTime
          "Partisan Identification Over Time"
          (FV.ViewConfig 80 200 5)
          partisanId
        brAddMarkDown text2
        _ <-  K.addHvega Nothing Nothing
              $ vlWeights
              "2016 Vote share and EVs for various electoral weights"
              (FV.ViewConfig 400 400 5)
              (F.filterFrame ((==2016) . F.rgetField @BR.Year) $ aserEwResults <> aser5EwResults)
        brAddMarkDown text3
{-        
        _ <-  K.addHvega Nothing Nothing
              $ vlWeights
              "Vote share and EVs: 2016 weightings vs. 2018 weightings"
              (FV.ViewConfig 400 400 5)
              (aserEwResults2 <> aser5EwResults2)
-}
        brAddMarkDown modelingNotes
        brAddMarkDown brReadMore


data PartisanId = Democrat | Independent | Republican deriving (Show)
mapPID :: CCES.PartisanIdentity3 -> PartisanId
mapPID CCES.PI3_Democrat = Democrat
mapPID CCES.PI3_Republican = Republican
mapPID _ = Independent
type PID = "Partisan Identity" F.:-> PartisanId

instance FV.ToVLDataValue (F.ElField PID) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Str $ T.pack $ show $ V.getField x)

vlPartisanIdOverTime :: (Functor f, Foldable f)
  => T.Text
  -> FV.ViewConfig
  -> f (F.Record [BR.Year, CCES.PartisanId3, '("Fraction", Double)])
  -> GV.VegaLite
vlPartisanIdOverTime title vc rows =
  let dat = FV.recordsToVLData id FV.defaultParse (FV.addMappedColumn @CCES.PartisanId3 @PID  mapPID rows)
      fractionAsPct = GV.calculateAs "100 * datum.Fraction" "% of Voters" 
      encX = GV.position GV.X [FV.pName @PID, GV.PmType GV.Nominal, GV.PAxis [GV.AxNoTitle]]
      encY = GV.position GV.Y [GV.PName "% of Voters"
                              , GV.PmType GV.Quantitative
                              , GV.PScale [GV.SDomain $ GV.DNumbers [0, 50]]
                              ]
      encColor = GV.color [FV.mName @PID, GV.MmType GV.Nominal]
      encColumn = GV.column [FV.fName @BR.Year, GV.FmType GV.Ordinal]
      encoding = (GV.encoding . encColumn . encX . encY . encColor) []
      mark = GV.mark GV.Bar []
      transform = (GV.transform . fractionAsPct) []
  in FV.configuredVegaLite vc [FV.title title, encoding, mark, transform, dat]



vlWeights :: (Functor f, Foldable f)
  => T.Text
  -> FV.ViewConfig
  -> f (F.Record [BR.Year, ET.ElectoralWeightSource, BR.DemographicGroupingC, WeightSource, ElectorsD, ElectorsR, BR.DemPref])
  -> GV.VegaLite
vlWeights title vc rows =
  let dat = FV.recordsToVLData id FV.defaultParse rows
      makeVS =  GV.calculateAs "100 * datum.DemPref" "Vote Share (%)"
      makeVSRuleVal = GV.calculateAs "50" "50%"
      voteShareDomain = GV.SDomain $ GV.DNumbers [48.5, 51.5]
      evDomain = GV.SDomain $ GV.DNumbers [180, 300]
      encVSRuleX = GV.position GV.X [GV.PName "50%", GV.PmType GV.Quantitative, GV.PScale [voteShareDomain]]
      makeEVRuleVal = GV.calculateAs "269" "Evenly Split"
      encEVRuleY = GV.position GV.Y [GV.PName "Evenly Split", GV.PmType GV.Quantitative, GV.PScale [evDomain]]
      makeSourceType = GV.calculateAs "datum.ElectoralWeightSource + '/' + datum.DemographicGrouping" "Weight Source"
      encX = GV.position GV.X [GV.PName "Vote Share (%)"
                              , GV.PmType GV.Quantitative
                              , GV.PScale [voteShareDomain]
                              , GV.PAxis [GV.AxTitle "D Vote Share (%)"]
                              ]
      encY = GV.position GV.Y [FV.pName @ElectorsD
                              , GV.PmType GV.Quantitative
                              , GV.PScale [evDomain]
                              , GV.PAxis[GV.AxTitle "# D Electoral Votes"]
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
    , V.RMap (ks V.++ cs)
    , V.ReifyConstraint Show V.ElField (ks V.++ cs)
    , V.RecordToList (ks V.++ cs)
    , FI.RecVec (ks V.++ cs)
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
      demoPrefWeightE = FJ.leftJoinE3 @(ks V.++ cs) demographics prefs wgts
  case demoPrefWeightE of
    Left (FJ.FirstJoin mKeys) -> do
      K.logLE K.Error "key(s) present in demographics but not prefs: "
      logFrame $ F.toFrame mKeys
      K.knitError "Error in mergeElectionData"
    Left (FJ.SecondJoin mKeys) -> do
      K.logLE K.Error "key(s) present in demographics and prefs but not wgts:"
      logFrame $ F.toFrame mKeys
      K.knitError "Error in mergeElectionData"
    Right demoPrefWeight -> do 
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


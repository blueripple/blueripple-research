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

module MRP.TurnoutGaps where

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

text1a :: T.Text
text1a = [i|
In 2016, Trump won states that Clinton would likely have won if every demographic group voted in equal proportion. This reflects
a "turnout gap" between likely R voters and likely D voters. As we've talked about in [earlier pieces][BR:BlueWave],
one simplistic but useful way to look at an election is to consider some
demographic breakdown of voters (e.g., Age, Sex and Race), estimate how many eligible voters are in each group,
how likely each is to vote, and who they are likely to vote for.  We can look at this nationwide or in a single congressional district:
any region where we can estimate the demographics, turnout and preferences.

In this piece we focus on turnout gaps in the battleground states.  We're not going to talk about why these gaps exist,
but there's a great rundown at [FairVote][FV:Turnout].
In our [last post][BR:TurnoutHowHigh], we looked at how high turnout could go and the most efficient ways to boost turnout.
Here we take up a related question: what would changes in turnout do to the electoral landscape in the race for the White House? 

1. **Turnout Gaps**
2. **What If Everybody Voted?**
3. **Battleground States**
4. **How Much Turnout Do We Need?**
5. **Key Takeaways**
6. **Take Action**

## Demographic Turnout Gaps {#TurnoutGaps}
As a starting point, let's look at the demographic turnout gap in some battleground states.
We're defining the *demographic turnout gap* as the difference in turnout between groups that lean
Democratic vs the turnout of groups that lean Republican.  This is *not* the turnout gap between Republicans
and Democrats.  Each demographic group has some Democratic and some Republican voters.  Here we're
imagining targeting GOTV by age (for instance) and so we consider turnout by demographic groups. 

In table below we illustrate these gaps for the 2012 and 2016 presidential
elections---we've included 2012 just to indicate that while these numbers do fluctuate,
the numbers we see in 2016 are fairly typical of recent presidential elections. The gaps
in this table are large and that can seem discouraging. But instead we think it shows an opportunity
to use GOTV work to generate votes for Democratic candidates.  In what follows we will try to
focus that opportunity; to see where it might be most productive in the upcoming presidential election.
It's important to remember that these states were very close in the 2016 election
(except for TX and GA). How can this be if the turnout gaps are so large? 
Among the groups we are looking at,
Democratic leaning groups are much more likely to vote for Democrats than the 
Republican leaners are to vote for Republicans. This also means, as we'll explore
in detail below, that we do not have to close those gaps to win these
states.  Just shrinking the gaps slightly is enough in many of the battleground states.

We've split the electorate in to 8 groups: age (45 or over/under 45), sex (F/M),
and race (non-white, white non-Hispanic).
We compute turnout as a percentage of the *voting-age-population* (VAP) rather than 
*voting-eligible-population* (VEP) because we don't have a source for
VEP split by demographic groups.
Using VAP lowers all of our turnout rates, especially in
AZ, FL and TX where there are large numbers of immigrants who are voting age but not citizens and
thus not eligible to vote. As a demographic group, those voters tend to
vote for Democrats so using VAP also has the effect of
showing a higher turnout gap than actually exists among eligible voters. 

We should not read this table as meaning that, for example, in AZ in 2016, 24% fewer Democrats
showed up at the polls. Instead, the table says that in 2016,
turnout among people most likely (by age, sex, and race) to vote for Democrats
was 24% lower than it was among the folks most likely to vote Republican.



[BR:BlueWave]: <${brGithubUrl (PrefModel.postPath PrefModel.PostAcrossTime)}>
[FV:Turnout]: <https://www.fairvote.org/voter_turnout#voter_turnout_101>
[BR:TurnoutHowHigh]: <https://blueripplepolitics.org/blog/gotv-data>
|]

text1b :: T.Text
text1b = [i|
## What If Everybody Voted? {#EverybodyVoted}
Last year, The Economist ran a fascinating data-journalism article entitled
["Would Donald Trump be president if all Americans actually voted?"][Economist:EveryoneVoted].  Using
a variety of sophisticated methods (explained in the linked piece) they attempted to simulate the
2016 presidential election under the assumption that every eligible voter cast a vote.  They concluded that
Clinton would have won the election.  Though the model is complex, the result comes down to understanding
the demographics in each state and the voter preferences of those groups.  The idea that everyone would vote in
a country where roughly 65% of eligible voters actually vote seems far-fetched.  So it's important to
realize that what the Economist modeled is the result we'd expect if everybody voted *in equal proportion*.
It doesn't matter if everyone votes, just that people in every
demographic group are equally likely to vote in each state.

Here we're going to perform a similar analysis, focused only on the battleground states.
Our model of voter preference is substantially simpler than the one used by the Economist.
They broke the electorate into many groups by sex, age, education and race.  Here we are just going to look
at age (45 or Over/Under 45), sex (F/M), and race (Non-White/White-Non-Hispanic).  That's more than
enough to see the central point: in many crucial states, the gaps in turnout between likely R and likely D
voters were enough to cost Clinton the election.

Though we're focusing on 2016, we're not interested in re-hashing that election!  We want to know where
to focus our energies now.  Where can efforts to raise turnout make the most difference?

## Battleground States {#Battlegrounds}
Below we show a chart of the modeled Democratic preference of the electorate in each battleground state.
Our [MR model][BR:Pools:DataMethods] allows us to infer the preference of each demographic group in each state.  To
get an overall preference for the state, we have to weigh those groups by their relative sizes.
That step is called
"post-stratification" (the "P" in "MRP").  But do we post-stratify by the voting-age-population
or the number of *likely voters* in each group? Weighting by population corresponds to a
scenario where every demographic group votes in equal proportion.   

In the chart below we compare the 2-party Democratic vote-share in the 2016
presidential elections with the post-stratified preference assuming everybody votes in
equal proportion. This gives us a window into the effects of turnout
gaps in various states.  The effect of the gap in an individual state depends on
the demographics and preferences in that particular state.

There are a few things to note in the chart. While all of these states have
higher Democratic Vote Share in the everybody-votes scenario, some shift by
more than others.  This reflects the different sizes of the groups and the
intensity of the D lean among the D leaners and the R lean among the R leaners.
Except for OH, all the states here would move to the Democratic column if 

Is the difference sufficient to push the state to a preference
above 50%? Then turnout gaps alone might make the difference between winning and losing the state.
In AZ, FL, GA, NC, PA, and TX, turnout gaps create differences of over 3% in final preference.
While driving a smaller gap in MI, and WI, those states were both very close in 2016 and those smaller
gaps were enough to explain all three losses. OH is the only one of these states where even if all voters voted
in equal proportion, Democrats would still likely lose the state.

[BR:Pools:DataMethods]: <${brGithubUrl (postPath PostPools)}/#DataAndMethods>
[BR:Home]: <https://blueripplepolitics.org>
[Economist:EveryoneVoted]: <https://medium.economist.com/would-donald-trump-be-president-if-all-americans-actually-voted-95c4f960798>
|]

text2 :: T.Text
text2 = [i|
Let's return to GA and TX for a minute.  These are not traditional battleground states. TX
voted for Trump by almost a 10% margin in 2016. But changing
demographics---racial and educational---are making Texas more purple.  And the large shift
in preference we see from just the turnout gap suggests that TX is already more purple than
recent outcomes suggest.  This year, because Texas has a number
of retirements in the house, the local house races will be more competitive, which
often drives higher and more balanced turnout. GA, also traditionally a red state,
was closer than TX in 2016 with Trump winning by about 5%. And GA, by our simple model, could
shift about 6% bluer if there were no turnout gaps.

## How Much Turnout Do We Need?
Hopefully by now we've convinced you that the turnout gap exists and that shrinking or closing it could
be enough to flip some states.  Looking at that chart, you can see that some states don't need to move much from
the actual 2016 outcome towards the everybody-voted scenario to flip.  Let's quantify that.
To put this more concretely,
let's consider two different plans for boosting turnout in each of these states:

- Option 1: Boost turnout for everyone in the state
- Option 2: Focus GOTV efforts exclusively on Dem leaning voters

In either case,
is GOTV work alone enough to flip any of these states? Using the voter preferences and turnout
from the 2016 presidential election and the demographics from 2018, we compute the turnout boost
required to flip each battleground state. The math is straightforward but messy so we've put the
details [here][BR:BoostNotes].

In the table below we summarize the results.  With just a glance, it's apparent that efforts
to improve Dem turnout would be particularly valuable in FL, MI, PA, and WI.  Those are states
where less than a 6% increase in Dem turnout could flip the state.  In MI, boosting turnout
among *all* voters just 3.5% would also likely flip the state.  It's remarkable that in most
of these states, boosting turnout among all voters could flip them.  This is a result of the
fact that in Republican leaning voters are usually less Republican leaning than Democratic
leaning voters are Democratic leaning.

In our [previous post][BR:TurnoutHowHigh] we examined some history and scholarship about
turnout, concluding that 5% boosts in turnout were plausible, given high levels of
voter intensity and strong GOTV work.  Looking at the table, that puts MI and PA in the
"achievable" range, FL at the edge of that and WI just barely above. If you want to do GOTV
work, or donate to groups doing that work, these are the best places to start.

One last point: these
states are very different sizes, so the number of "extra" voters needed to increase
Dem turnout by, e.g.,  1% can vary greatly.  But the larger states are also larger electoral
prizes---they have more electoral votes.  That is, it's much more work to boost turnout 4.4%
among Dems in FL than to boost it 5.5% in WI, but FL is worth almost 3 times as many electoral votes.
So the difference in work may be worth it.  It's a little more complicated than that
because each state has a different proportion and intensity of Dem leaners.
The bottom line: in terms of bang-for-buck, the best bets for GOTV work are MI, WI, PA and then FL.


[BR:Home]: <https://blueripplepolitics.org>
[BR:TurnoutHowHigh]: <https://blueripplepolitics.org/blog/gotv-data>
[BR:BoostNotes]: <${brGithubUrl ((postRoute PostTurnoutGaps) <> "turnoutBoostExplainer")}#>
[Economist:EveryoneVoted]: <https://medium.economist.com/would-donald-trump-be-president-if-all-americans-actually-voted-95c4f960798>
[FV:Turnout]: <https://www.fairvote.org/voter_turnout#voter_turnout_101>
|]

text3 :: T.Text
text3 = [i|

## Key Takeaways
* Demographic turnout gaps are large, which means there's room for GOTV work to generate Democratic votes.
* Of the battlegrounds, broad-based GOTV efforts in MI are likely to flip the state blue,
and increasing turnout within Dem-leaning groups will likely close the gap in WI and PA and maybe FL.
* GA and TX could turn into battleground states if we significantly boost turnout among Dem-leaning groups.

## Take Action
One of our themes at [Blue Ripple Politics][BR:Home]
is that we believe that democracy works better if everyone votes, and that we should prioritize laws and policies that
make voting easier for everyone, e.g., same-day-registration, vote-by-mail, making election day a federal holiday, 
having more polling places, etc.  We've identified 4 states where GOTV work should have the highest payoff
in terms of the Presidential election.  We encourage you to get involved with an organization doing GOTV work in
those states.

Below, we highlight national organizations that are targeting the states we think are easiest
to flip and/or demographic groups which are crucial in those states.
We're also interested in local organizations in any of these states, so let us know of any you
come across.

* [For Our Future Action Fund][FOFAF] organizes GOTV drives in FL, NV, OH, PA, WI, VA and MI. You can donate
money or volunteer in the states where they are active.
* [Progressive Turnout Project][PTP] also runs state-level GOTV in these states (as well as others).
* [Voto Latino][VL] organizes Latinx voters nationwide.  While this is not as single-state focused, the Latinx
vote is crucial in many of the states which are flippable in the 2020 election and plays a crucial
role in many a close house district.  

## Update {#Update}
G. Elliot Morris (@gelliotmorris) and others [point out][GEMTweet] that we should not interpret these results as
indicating a path to victory in these states.  We agree!  We should've been more clear:  we don't think
only Dems will attempt to raise turnout. Nor do we think that GOTV efforts can successfully target only
Dems---though here we should note that our imagined targeting is demographic, for example targeting only
young voters, which is slightly more plausible than targeting only Dems.  Our goal is here is twofold:
to figure out *where GOTV work is most valuable* and to observe that the necessary numbers in those places are
in the realm of turnout shifts we've seen in the past.

One way to reframe this:  the numbers we calculated are
very approximate amounts by which a D leaning turnout boost needs
to exceed an R leaning turnout boost to close the 2016 vote-share gap.
E.g., in MI, we would need a 1% greater boost in Dem leaning turnout than
R leaning turnout. 

Some questions we didn't/couldn't answer but are interested in:

- What are the *relative* (D leaning vs. R leaning)
shifts in turnout in these states over the past few presidential elections?
- How probable is any given shift in turnout?
- How much of a difference does GOTV work make in that distribution?

A very partial answer to the first question is contained in the table at the beginning of this post.
It shows the approximate Dem leaning vs. R leaning turnout in each battleground state in 2012 and 2016.
The shifts in the D/R gap between 2012 and 2016 vary but several are over 2% and GA is over 3%.
So 3% net swings in favor of Dems are not impossible, election to election.
That is not to say that we know how to *produce* those shifts, but that such shifts are not implausible.
We'll try to look a bit further back to get a better sense of those numbers over more elections.

[GEMTweet]: <https://twitter.com/gelliottmorris/status/1230877174493786114>
[VL]: <https://votolatino.org/>
[FOFAF]: <https://forourfuturefund.org/>
[PTP]: <https://www.turnoutpac.org/>
[BR:Home]: <https://blueripplepolitics.org>
[BR:TurnoutHowHigh]: <https://blueripplepolitics.org/blog/gotv-data>
[Economist:EveryoneVoted]: <https://medium.economist.com/would-donald-trump-be-president-if-all-americans-actually-voted-95c4f960798>
[FV:Turnout]: <https://www.fairvote.org/voter_turnout#voter_turnout_101>
|]

  {-
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
-}
  
foldPrefAndTurnoutData :: FF.EndoFold (F.Record '[BR.ACSCount, BR.VotedPctOfAll, DemVPV, BR.DemPref])
foldPrefAndTurnoutData =  FF.sequenceRecFold
                          $ FF.toFoldRecord (FL.premap (F.rgetField @BR.ACSCount) FL.sum)
                          V.:& FF.toFoldRecord (BR.weightedSumRecF @BR.ACSCount @BR.VotedPctOfAll)
                          V.:& FF.toFoldRecord (BR.weightedSumRecF @BR.ACSCount @DemVPV)
                          V.:& FF.toFoldRecord (BR.weightedSumRecF @BR.ACSCount @BR.DemPref)
                          V.:& V.RNil

type BoostA = "boostA" F.:-> Double
type BoostB = "boostB" F.:-> Double
type BoostPop = "boostPop" F.:-> Int
type ToFlip = "ToFlip" F.:-> Double

foldPrefAndTurnoutDataBoost :: (Double -> Bool)
                            -> FL.Fold (F.Record [BR.ACSCount, BR.VotedPctOfAll, DemVPV, BR.DemPref])
                            (F.Record [BR.ACSCount, BR.VotedPctOfAll, BoostA, BoostB, BoostPop])
foldPrefAndTurnoutDataBoost prefTest =
  let t r = prefTest $ F.rgetField @BR.DemPref r
      dVotesF = FL.premap (\r -> realToFrac (F.rgetField @BR.ACSCount r) * F.rgetField @BR.DemPref r * F.rgetField @BR.VotedPctOfAll r) FL.sum
      votesF = FL.premap (\r -> realToFrac (F.rgetField @BR.ACSCount r) * F.rgetField @BR.VotedPctOfAll r) FL.sum 
      popF = FL.prefilter t $ FL.premap (F.rgetField @BR.ACSCount) FL.sum
      aNF = FL.prefilter t $ FL.premap (\r -> realToFrac (F.rgetField @BR.ACSCount r) * realToFrac (F.rgetField @BR.DemPref r)) FL.sum
      aDF = dVotesF
      aF = (/) <$> aNF <*> aDF
      bNF = popF
      bDF = votesF
      bF = (/) <$> fmap realToFrac bNF <*> bDF
      prefF = (/) <$> dVotesF <*> votesF
--      toFlipF = (\p a b -> let d = (0.5 - p)/p in d/(a - (1.0 + d)*b)) <$> prefF <*> aF <*> bF
    in FF.sequenceRecFold 
       $ FF.toFoldRecord (FL.premap (F.rgetField @BR.ACSCount) FL.sum)
       V.:& FF.toFoldRecord (BR.weightedSumRecF @BR.ACSCount @BR.VotedPctOfAll)
       V.:& FF.toFoldRecord aF
       V.:& FF.toFoldRecord bF
       V.:& FF.toFoldRecord bNF
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
  let --states = ["AZ", "FL", "GA", "ME", "NC", "OH", "MI", "WI", "PA", "CO", "NH", "NV", "TX", "VA"]
    states = ["AZ", "FL", "GA", "NC", "OH", "MI", "WI", "PA", "TX"]
    statesOnly = F.filterFrame (\r -> F.rgetField @BR.StateAbbreviation r `L.elem` states)
    stateAndNation = F.filterFrame (\r -> F.rgetField @BR.StateAbbreviation r `L.elem` "National" : states)
  stateTurnoutRaw <- BR.stateTurnoutLoader 
  aseACS <- BR.simpleASEDemographicsLoader 
  asrACS <- BR.simpleASRDemographicsLoader 
  let acsASRByStateF = FMR.concatFold $ FMR.mapReduceFold
                       FMR.noUnpack
                       (FMR.assignKeysAndData @([BR.Year, BR.StateAbbreviation] V.++ BR.CatColsASR) @'[BR.ACSCount])
                       (FMR.foldAndAddKey $ FF.foldAllConstrained @Num FL.sum)  
  aseTurnout <- BR.simpleASETurnoutLoader 
  asrTurnout <- BR.simpleASRTurnoutLoader 
--  logFrame aseTurnout
  let showRecs = T.intercalate "\n" . fmap (T.pack . show) . FL.fold FL.list
  let predictorsASER = GLM.Intercept : fmap GLM.Predictor (allCCESSimplePredictors @BR.CatColsASER)
      predictorsASE =  GLM.Intercept : fmap GLM.Predictor (allCCESSimplePredictors @BR.CatColsASE)
      predictorsASR = GLM.Intercept : fmap GLM.Predictor (allCCESSimplePredictors @BR.CatColsASR)
--  inferredPrefsASER <-  stateAndNation <$> BR.retrieveOrMakeFrame "mrp/simpleASER_MR.bin"
--                        (P.raise $ BR.mrpPrefs @BR.CatColsASER (Just "ASER") ccesDataLoader predictorsASER (catPredMaps @BR.CatColsASER)) 
  inferredPrefsASE <-  stateAndNation <$> BR.retrieveOrMakeFrame "mrp/simpleASE_MR.bin"
                       (P.raise $ BR.mrpPrefs @BR.CatColsASE (Just "ASE") ccesDataLoader predictorsASE (catPredMaps @BR.CatColsASE)) 
  inferredPrefsASR <-  stateAndNation <$> BR.retrieveOrMakeFrame "mrp/simpleASR_MR.bin"
                       (P.raise $ BR.mrpPrefs @BR.CatColsASR (Just "ASR") ccesDataLoader predictorsASR (catPredMaps @BR.CatColsASR)) 

  -- get adjusted turnouts (national rates, adj by state) for each CD
  demographicsAndTurnoutASE <- statesOnly <$> BR.cachedASEDemographicsWithAdjTurnoutByCD (return aseACS) (return aseTurnout) (return stateTurnoutRaw)
  demographicsAndTurnoutASR <- statesOnly <$> BR.cachedASRDemographicsWithAdjTurnoutByCD (return asrACS) (return asrTurnout) (return stateTurnoutRaw)
  K.logLE K.Info "Comparing 2012 turnout to 2016 turnout among 2016 Dem leaners..."
  let asrTurnoutComparison yPrefs yPop years =
        let isYear y r = (F.rgetField @BR.Year r == y)
            isYearPres y r = isYear y r && (F.rgetField @ET.Office r == ET.President)

            turnoutByCD y = fmap (F.rcast @('[BR.StateAbbreviation, BR.CongressionalDistrict] V.++ BR.CatColsASR V.++ '[BR.VotedPctOfAll]))
                               $ F.filterFrame (isYear y)  demographicsAndTurnoutASR
            popByCD y = fmap (F.rcast @('[BR.StateAbbreviation, BR.CongressionalDistrict] V.++ BR.CatColsASR V.++ '[BR.ACSCount]))
                           $ F.filterFrame (isYear y)  demographicsAndTurnoutASR
            prefsByState y =  fmap (F.rcast @('[BR.StateAbbreviation] V.++ BR.CatColsASR V.++ '[BR.DemPref]))
                              $ F.filterFrame (isYearPres y) inferredPrefsASR
            turnoutAndPopToStateF :: FL.Fold (F.Record ([BR.StateAbbreviation, BR.CongressionalDistrict] V.++ BR.CatColsASR V.++ [BR.ACSCount, BR.VotedPctOfAll]))
                                             (F.FrameRec ('[BR.StateAbbreviation] V.++ BR.CatColsASR V.++ [BR.ACSCount, BR.VotedPctOfAll]))
            turnoutAndPopToStateF = FMR.concatFold $ FMR.mapReduceFold
                                    FMR.noUnpack
                                    (FMR.splitOnKeys @('[BR.StateAbbreviation] V.++ BR.CatColsASR))
                                    (FMR.foldAndAddKey
                                      (FF.sequenceRecFold
                                      $ FF.toFoldRecord (FL.premap (F.rgetField @BR.ACSCount) FL.sum)
                                      V.:& FF.toFoldRecord (BR.weightedSumRecF @BR.ACSCount @BR.VotedPctOfAll)
                                      V.:& V.RNil)
                                    )
            turnoutAndPopByState y = fmap (F.rcast @('[BR.StateAbbreviation] V.++ BR.CatColsASR V.++ [BR.ACSCount, BR.VotedPctOfAll]))
                                     $ FL.fold turnoutAndPopToStateF
                                     (catMaybes
                                       $ fmap F.recMaybe
                                       $ F.leftJoin @('[BR.StateAbbreviation, BR.CongressionalDistrict] V.++ BR.CatColsASR) (popByCD yPop) (turnoutByCD y))
            allJoined y =  fmap ((FT.recordSingleton @BR.Year y `V.rappend`) . F.rcast @('[BR.StateAbbreviation] V.++ BR.CatColsASR V.++ [BR.ACSCount, BR.VotedPctOfAll, BR.DemPref]))
                           $ catMaybes
                           $ fmap F.recMaybe
                           $ F.leftJoin @('[BR.StateAbbreviation] V.++ BR.CatColsASR) (prefsByState yPrefs) (turnoutAndPopByState y) 
            turnoutF = FF.sequenceRecFold 
                          $ FF.toFoldRecord (FL.prefilter ((< 0.5) . F.rgetField @BR.DemPref) $ BR.weightedSumRecF @BR.ACSCount @BR.VotedPctOfAll)
                          V.:& FF.toFoldRecord (FL.prefilter ((< 0.5) . F.rgetField @BR.DemPref) $ FL.premap (F.rgetField @BR.ACSCount) FL.sum)
                          V.:& FF.toFoldRecord (FL.prefilter ((>= 0.5) . F.rgetField @BR.DemPref) $ BR.weightedSumRecF @BR.ACSCount @BR.VotedPctOfAll)
                          V.:& FF.toFoldRecord (FL.prefilter ((>= 0.5) . F.rgetField @BR.DemPref) $ FL.premap (F.rgetField @BR.ACSCount) FL.sum)
                          V.:& V.RNil
            comparisonF :: FL.Fold (F.Record ('[BR.Year, BR.StateAbbreviation] V.++ BR.CatColsASR V.++ [BR.ACSCount, BR.VotedPctOfAll, BR.DemPref]))
                           (F.FrameRec [BR.Year, BR.StateAbbreviation, '("RTurnout", Double), '("RPop",Int),'("DTurnout", Double), '("DPop", Int)])
            comparisonF = FMR.concatFold $ FMR.mapReduceFold
                          FMR.noUnpack
                          (FMR.splitOnKeys @'[BR.Year, BR.StateAbbreviation])
                          (FMR.foldAndAddKey turnoutF)
            allRows = mconcat $ fmap ( FL.fold (FL.premap F.rcast comparisonF) .  allJoined) years
        in allRows

  
  let turnoutTableYears = [2012, 2016]
      turnoutGaps = asrTurnoutComparison 2016 2018 turnoutTableYears
--  logFrame turnoutGaps
  let turnoutGapsForTable = FL.fold (FMR.mapReduceFold
                            FMR.noUnpack
                            (FMR.splitOnKeys @'[BR.StateAbbreviation])
                            (FMR.ReduceFold $ \k -> fmap (k,) FL.list))
                            turnoutGaps    
--  K.logLE K.Info $ T.pack $ show turnoutGapsForTable  
  let turnoutCompColonnade years cas =
        let dVAP = F.rgetField @'("DPop",Int)
            dTurnout = F.rgetField @'("DTurnout",Double)
            rVAP = F.rgetField @'("RPop",Int)
            rTurnout = F.rgetField @'("RTurnout",Double)
            st = F.rgetField @BR.StateAbbreviation . fst            
            getForYear y = L.find ((== y) . F.rgetField @BR.Year) . snd
            colsForYear y =
              let ty = T.pack $ show y
                  hDVAP = (ty <> " Dem VAP")
                  hDTurnout = (ty <> " Dem Turnout")
                  hRVAP = (ty <> " Rep VAP")
                  hRTurnout = (ty <> " Rep Turnout")
              in C.headed (BC.Cell mempty $ BH.toHtml hDTurnout) (BR.toCell cas hDTurnout hDTurnout (BR.maybeNumberToStyledHtml "%2.2f" . fmap ((*100.0) . dTurnout) . getForYear y))
--                 <> C.headed (BC.Cell mempty $ BH.toHtml hRVAP) (BR.toCell cas hRVAP hRVAP (BR.maybeNumberToStyledHtml "%d" . fmap rVAP . getForYear y))
                 <> C.headed (BC.Cell mempty $ BH.toHtml hRTurnout) (BR.toCell cas hRTurnout hRTurnout (BR.maybeNumberToStyledHtml "%2.2f" . fmap ((*100.0) . rTurnout) . getForYear y))
                 <> C.headed "Gap" (BR.toCell cas "Gap" "Gap" (BR.maybeNumberToStyledHtml "%2.2f" . fmap ((*100) . (\r -> dTurnout r - rTurnout r)) . getForYear y))
        in C.headed "State" (BR.toCell cas "State" "State" (BR.textToStyledHtml . st))
           <> mconcat (fmap colsForYear years)

  
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
--  logFrame houseVoteShareFrame
  K.logLE K.Info "Joining turnout by CD and prefs"
  let aseTurnoutAndPrefs = catMaybes
                           $ fmap F.recMaybe
                           $ F.leftJoin @([BR.StateAbbreviation, BR.Year] V.++ BR.CatColsASE) demographicsAndTurnoutASE inferredPrefsASE
      asrTurnoutAndPrefs = catMaybes
                           $ fmap F.recMaybe
                           $ F.leftJoin @([BR.StateAbbreviation, BR.Year] V.++ BR.CatColsASR) demographicsAndTurnoutASR inferredPrefsASR

      -- fold these to state level
  let justPres2016 r = (F.rgetField @BR.Year r == 2016) && (F.rgetField @ET.Office r == ET.President)    
      aseDemoF = FMR.concatFold $ FMR.mapReduceFold
                 (FMR.unpackFilterRow justPres2016)
                 (FMR.assignKeysAndData @(BR.CatColsASE V.++ '[BR.StateAbbreviation, BR.Year, ET.Office]) @[BR.ACSCount, BR.VotedPctOfAll, DemVPV, BR.DemPref])
                 (FMR.foldAndAddKey foldPrefAndTurnoutData)
      asrDemoF = FMR.concatFold $ FMR.mapReduceFold
                 (FMR.unpackFilterRow justPres2016)
                 (FMR.assignKeysAndData @(BR.CatColsASR V.++ '[BR.StateAbbreviation, BR.Year, ET.Office]) @[BR.ACSCount, BR.VotedPctOfAll, DemVPV, BR.DemPref])
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
      -- assemble set with updated demographics
      toFlip r =
        let a = F.rgetField @BoostA r
            b = F.rgetField @BoostB r
            p = F.rgetField @BR.DemPref r
            d = (0.5 - p)/p
        in FT.recordSingleton @ToFlip $ d/(a - (1.0 + d) * b)

      toFlipASRByState x =
        let filter y r = F.rgetField @BR.Year r == y && F.rgetField @ET.Office r == ET.President
            asrPrefTurnout2016 = fmap (F.rcast @('[BR.StateAbbreviation] V.++ BR.CatColsASR V.++ [BR.VotedPctOfAll, DemVPV, BR.DemPref]))
                                 $ F.filterFrame (filter 2016) asrByState
            asrDemo2018 = fmap (F.rcast @('[BR.StateAbbreviation] V.++ BR.CatColsASR V.++ '[BR.ACSCount]))
                          $ FL.fold acsASRByStateF $ statesOnly $ F.filterFrame (\r -> F.rgetField @BR.Year r == 2018) asrACS
            asrUpdatedDemo = catMaybes
                             $ fmap F.recMaybe
                             $ F.leftJoin @('[BR.StateAbbreviation] V.++ BR.CatColsASR) asrPrefTurnout2016 asrDemo2018
            asrWithBoosts = FL.fold
                            (FMR.concatFold $ FMR.mapReduceFold
                              MR.noUnpack
                              (FMR.assignKeysAndData @'[BR.StateAbbreviation])
                              (FMR.foldAndAddKey $ foldPrefAndTurnoutDataBoost (>x))
                            )
                            asrUpdatedDemo
                            
            presPref2016 = fmap (F.rcast @[BR.StateAbbreviation, BR.DemPref]) $ F.filterFrame (filter 2016) presPrefByStateFrame
            asrUpdated =  catMaybes
                                $ fmap F.recMaybe
                                $ F.leftJoin @'[BR.StateAbbreviation] asrWithBoosts
                                $ presPref2016
        in fmap (FT.mutate toFlip) asrUpdated
      toFlipAll = F.toFrame $ fmap (FT.retypeColumn @ToFlip @'("ToFlipAll",Double)
                                    . FT.retypeColumn @BoostPop @'("BoostPopAll",Int)
                                    . F.rcast @[BR.StateAbbreviation, BR.ACSCount, BR.VotedPctOfAll, BR.DemPref, BoostPop, ToFlip]) $  toFlipASRByState 0.0
      toFlipDems = F.toFrame $ fmap  (FT.retypeColumn @ToFlip @'("ToFlipDems",Double)
                                      . FT.retypeColumn @BoostPop @'("BoostPopDems",Int)
                                      . F.rcast @[BR.StateAbbreviation, BoostPop, ToFlip]) $  toFlipASRByState 0.5
      toFlipJoined =   F.toFrame
                       $ catMaybes
                       $ fmap F.recMaybe
                       $ F.leftJoin @'[BR.StateAbbreviation] toFlipAll toFlipDems
  evFrame <- fmap (F.rcast @[BR.StateAbbreviation, BR.Electors]) . F.filterFrame (\r -> F.rgetField @BR.Year r == 2020) <$> BR.electoralCollegeFrame
  let toFlipWithEV =  catMaybes
                      $ fmap F.recMaybe
                      $ F.leftJoin @'[BR.StateAbbreviation] toFlipJoined evFrame
--  logFrame toFlipWithEV
  let boostColonnade cas =
        let toFlipDM r = let x = F.rgetField @'("ToFlipDems",Double) r in if x > 0 then Just x else Nothing
            toFlipAllM r = let x = F.rgetField @'("ToFlipAll",Double) r in if x > 0 then Just x else Nothing
            evs = F.rgetField @BR.Electors
            pop  = F.rgetField @BR.ACSCount 
            bPop = F.rgetField @'("BoostPopDems",Int)
            votersPerEvM r = fmap (\x -> (round (x * realToFrac (bPop r) / realToFrac (evs r))):: Int) (toFlipDM r)
        in C.headed "State" (BR.toCell cas "State" "State" (BR.textToStyledHtml . F.rgetField @BR.StateAbbreviation))
           <> C.headed "Electoral Votes" (BR.toCell cas "Electoral Votes" "EVs" (BR.numberToStyledHtml "%d". evs))
           <> C.headed "Population (000s)" (BR.toCell cas "Population" "Pop" (BR.numberToStyledHtml "%d". (\r -> (round (realToFrac (pop r)/1000)) :: Int)))
           <> C.headed "Dem Leaners (000s)" (BR.toCell cas "Leaners" "Leaners" (BR.numberToStyledHtml "%d". (\r -> (round (realToFrac (bPop r)/1000)) :: Int)))
           <> C.headed "Boosting All Requires (%)" (BR.toCell cas "BoostAll" "% Of All" (BR.maybeNumberToStyledHtml "%2.2f" . fmap (*100) . toFlipAllM))
           <> C.headed "Boosting Dems Requires (%)" (BR.toCell cas "BoostDem" "% Of Dems" (BR.maybeNumberToStyledHtml "%2.2f" . fmap (*100) . toFlipDM))
--           <> C.headed "New Voters/Ev" (BR.toCell cas "VotersPerEv" "Voters/EV" (BR.maybeNumberToStyledHtml "%d" . votersPerEvM))
        
  
  
{-
      postStratifiedWithBoostByASR = FL.fold
                                     (FMR.concatFold $ FMR.mapReduceFold
                                      MR.noUnpack
                                      (FMR.assignKeysAndData @[BR.Year, ET.Office, BR.StateAbbreviation])
                                      (FMR.foldAndAddKey $ foldPrefAndTurnoutDataBoost 0.5)
                                     )
                                  asrByState
      asrByStateWithPresPref =  catMaybes
                                $ fmap F.recMaybe
                                $ F.leftJoin @[BR.StateAbbreviation, BR.Year] postStratifiedWithBoostByASR
                                $ fmap (F.rcast @[BR.Year, BR.StateAbbreviation, PresPrefDem]) presPrefByStateFrame
      asrBoosts = fmap (FT.mutate toFlip) asrByStateWithPresPref          
  logFrame asrBoosts
-}
  curDate <-  (\(Time.UTCTime d _) -> d) <$> K.getCurrentTime
  let pubDateTurnoutGaps =  Time.fromGregorian 2020 2 21
  K.newPandoc
    (K.PandocInfo ((postRoute PostTurnoutGaps) <> "main")
      (brAddDates updated pubDateTurnoutGaps curDate
       $ M.fromList [("pagetitle", "Voter Turnout in Battleground States: Where Can We Win?")
                    ,("title","Voter Turnout in Battleground States: Where Can We Win?")
                    ]
      ))
      $ do        
        brAddMarkDown text1a
        BR.brAddRawHtmlTable "Turnout Comparison" (BHA.class_ "brTable")  (turnoutCompColonnade turnoutTableYears mempty) turnoutGapsForTable        
        brAddMarkDown text1b
        let presVoteShareFrame = statesOnly $ F.filterFrame ((== 2016) . F.rgetField @BR.Year) presPrefByStateFrame
            asrPostStratifiedFrame = F.filterFrame ((== ET.PSByVAP) . F.rgetField @ET.PrefType) vpvPostStratifiedByASR
        _ <-  K.addHvega Nothing Nothing
          $ vlTurnoutGap
          "Democratic Vote Share in the Battleground States"
          (FV.ViewConfig 800 400 10)
          $ (fmap F.rcast asrPostStratifiedFrame <> fmap F.rcast presVoteShareFrame)
        brAddMarkDown text2
        BR.brAddRawHtmlTable "Turnout Boosts to Flip Battlegrounds" (BHA.class_ "brTable") (boostColonnade mempty) toFlipWithEV
        brAddMarkDown text3
        brAddMarkDown brReadMore
     
  K.newPandoc
    (K.PandocInfo ((postRoute PostTurnoutGaps) <> "turnoutBoostExplainer")
      (brAddDates updated pubDateTurnoutGaps curDate
       $ M.fromList [("pagetitle", "Turnout Boost Details")
                    ,("title","Turnout Boost Details")
                    ]
      ))
      $ brAddMarkDown turnoutBoostExplainerMD
        
  let toFlipASRHouse t =
        let filter y r = F.rgetField @BR.Year r == y && F.rgetField @ET.Office r == ET.House
            asrPrefTurnout2018 = fmap (F.rcast @('[BR.StateAbbreviation, BR.CongressionalDistrict] V.++ BR.CatColsASR V.++ [BR.VotedPctOfAll, DemVPV, BR.DemPref]))
                                 $ F.filterFrame (filter 2018) $ F.toFrame $ asrTurnoutAndPrefs
            asrDemo2018 = fmap (F.rcast @('[BR.StateAbbreviation, BR.CongressionalDistrict] V.++ BR.CatColsASR V.++ '[BR.ACSCount]))
                          $ statesOnly $ F.filterFrame (\r -> F.rgetField @BR.Year r == 2018) asrACS
            asrUpdatedDemo = catMaybes
                             $ fmap F.recMaybe
                             $ F.leftJoin @('[BR.StateAbbreviation, BR.CongressionalDistrict] V.++ BR.CatColsASR) asrPrefTurnout2018 asrDemo2018
            asrWithBoosts = FL.fold
                            (FMR.concatFold $ FMR.mapReduceFold
                             MR.noUnpack
                             (FMR.assignKeysAndData @'[BR.StateAbbreviation, BR.CongressionalDistrict])
                              (FMR.foldAndAddKey $ foldPrefAndTurnoutDataBoost t)
                            )
                            asrUpdatedDemo
                            
            houseVoteShare2018 = fmap (F.rcast @[BR.StateAbbreviation, BR.CongressionalDistrict, BR.DemPref]) $ F.filterFrame (filter 2018) houseVoteShareFrame
            asrUpdated =  catMaybes
                          $ fmap F.recMaybe
                          $ F.leftJoin @'[BR.StateAbbreviation, BR.CongressionalDistrict] asrWithBoosts
                          $ houseVoteShare2018
        in fmap (FT.mutate toFlip) asrUpdated
  let prefFilter l u r = let p = F.rgetField @BR.DemPref r in p >= l && p <= u
      demFlippable = F.toFrame $ L.sortOn (F.rgetField @ToFlip) $ L.filter (prefFilter 0.45 0.5) $ toFlipASRHouse (>0.5)
      demDefend = F.toFrame $ L.sortOn (F.rgetField @ToFlip) $ L.filter (prefFilter 0.5 0.55) $ toFlipASRHouse (<0.5) 
  logFrame demFlippable
  logFrame demDefend
  let houseFlipColonnade cas =
        let toFlip r = F.rgetField @ToFlip r
            dPref r = F.rgetField @BR.DemPref r
            pop  = F.rgetField @BR.ACSCount 
            bPop = F.rgetField @'("BoostPop",Int)
        in C.headed "State" (BR.toCell cas "State" "State" (BR.textToStyledHtml . F.rgetField @BR.StateAbbreviation))
           <> C.headed "District" (BR.toCell cas "District" "District" (BR.numberToStyledHtml "%d" . F.rgetField @BR.CongressionalDistrict))
           <> C.headed "2018 2-party D Vote Share (%)" (BR.toCell cas "DVoteSharePct" "D Vote Share (%)" (BR.numberToStyledHtml "%2.2f" . (*100) . dPref))
           <> C.headed "Turnout Differential to Flip (%)" (BR.toCell cas "TurnoutDiffToFlipPct" "Turnout Diff To Flip (%)" (BR.numberToStyledHtml "%2.2f" . (*100) . toFlip))
{-    
  let toFlipAll = F.toFrame $ fmap (FT.retypeColumn @ToFlip @'("ToFlipAll",Double)
                                    . FT.retypeColumn @BoostPop @'("BoostPopAll",Int)
                                    . F.rcast @[BR.StateAbbreviation, BR.ACSCount, BR.VotedPctOfAll, BR.DemPref, BoostPop, ToFlip]) $  toFlipASRByState 0.0
      toFlipDems = F.toFrame $ fmap  (FT.retypeColumn @ToFlip @'("ToFlipDems",Double)
                                      . FT.retypeColumn @BoostPop @'("BoostPopDems",Int)
                                      . F.rcast @[BR.StateAbbreviation, BoostPop, ToFlip]) $  toFlipASRByState 0.5
      toFlipJoined =   F.toFrame
                       $ catMaybes
                       $ fmap F.recMaybe
                       $ F.leftJoin @'[BR.StateAbbreviation] toFlipAll toFlipDems
  evFrame <- fmap (F.rcast @[BR.StateAbbreviation, BR.Electors]) . F.filterFrame (\r -> F.rgetField @BR.Year r == 2020) <$> BR.electoralCollegeFrame
  let toFlipWithEV =  catMaybes
                      $ fmap F.recMaybe
                      $ F.leftJoin @'[BR.StateAbbreviation] toFlipJoined evFrame
-}
  let pubDateTurnoutGapsCD =  Time.fromGregorian 2020 3 1  
  K.newPandoc
    (K.PandocInfo ((postRoute PostTurnoutGapsCD) <> "main" )
    (brAddDates updated pubDateTurnoutGapsCD curDate
       $ M.fromList [("pagetitle", "Turnout Gap Analysis: House District Edition")
                    ,("title","Turnout Gap Analysis: House District Edition")
                    ]
      ))
      $ do
        brAddMarkDown cdText1
        BR.brAddRawHtmlTable "Turnout Diff to Gain" (BHA.class_ "brTable") (houseFlipColonnade mempty) demFlippable
        _ <-  K.addHvega Nothing Nothing
          $ vlFlip
          "Turnout Differentials Required to Flip Close R Districts in Battleground States"
          True
          (FV.ViewConfig 800 400 10)
          $ (fmap F.rcast demFlippable)
        BR.brAddRawHtmlTable "Turnout Diff to Lose" (BHA.class_ "brTable") (houseFlipColonnade mempty) demDefend
        _ <-  K.addHvega Nothing Nothing
          $ vlFlip
          "Turnout Differentials Required to Flip Close D Districts in Battleground States"
          False
          (FV.ViewConfig 800 400 10)
          $ (fmap F.rcast demDefend)
        return ()  
        
    

cdText1 :: T.Text
cdText1 = [i|
|]


vlFlip :: (Functor f, Foldable f)
       => T.Text
       -> Bool -- true for demFlip, false for demDefend
       -> FV.ViewConfig
       -> f (F.Record [BR.StateAbbreviation, BR.CongressionalDistrict, BR.DemPref, ToFlip])
       -> GV.VegaLite
vlFlip title demFlip vc rows =
  let dat = FV.recordsToVLData id FV.defaultParse rows
      makeVS = GV.calculateAs (if demFlip then "50 - 100 * datum.DemPref" else "100 * datum.DemPref - 50") "Vote Share"
      makeFP = GV.calculateAs "100 * datum.ToFlip" "To Flip"
      xLabel = if demFlip then "D Lost By (%)" else "D Won By %"
      encX = GV.position GV.X [GV.PName "Vote Share", GV.PmType GV.Quantitative, GV.PTitle xLabel]
      encY = GV.position GV.Y [GV.PName "To Flip", GV.PmType GV.Quantitative, GV.PTitle "% Turnout Differential to Flip"]
      transform = GV.transform . makeVS . makeFP
      encoding = GV.encoding . encX . encY
      dotSpec = GV.asSpec [(GV.encoding . encX . encY) [], transform [], GV.mark GV.Point [GV.MTooltip GV.TTData]]
  in FV.configuredVegaLite vc [FV.title title, GV.layer [dotSpec], dat]
  
vlTurnoutGap :: (Functor f, Foldable f)
             => T.Text -- title
             -> FV.ViewConfig
             -> f (F.Record [BR.StateAbbreviation, ET.Office, BR.Year, ET.PrefType, BR.DemPref])
             -> GV.VegaLite
vlTurnoutGap title vc rows =
  let mapPrefs p = case p of
        ET.PSByVAP -> "Voted in Equal Proportion"
        ET.VoteShare -> "Actual 2016 Vote"
        _ -> "N/A"
      dat = FV.recordsToVLData id FV.defaultParse (FV.addMappedColumn @ET.PrefType @'("Vote Share Type",T.Text) mapPrefs rows)
--      makeYVal = GV.calculateAs "datum.state_abbreviation + '-' + datum.year + '/' + datum.ET.Office + ' (' + datum.DemographicGrouping + ')'" "State/Race"
      makeYVal = GV.calculateAs "datum.state_abbreviation" "State"
      makeRuleVal = GV.calculateAs "50" "Evenly Split"
      makeVS = GV.calculateAs "100 * datum.DemPref" "Vote Share"
      encX = GV.position GV.X [GV.PName "Vote Share", GV.PmType GV.Quantitative, GV.PScale [GV.SDomain $ GV.DNumbers [40, 60]], GV.PTitle "2 Party Vote Share (%)"]
      encRuleX = GV.position GV.X [GV.PName "Evenly Split", GV.PmType GV.Quantitative, GV.PScale [GV.SDomain $ GV.DNumbers [40, 60]], GV.PNoTitle]
      encY = GV.position GV.Y [GV.PName "State", GV.PmType GV.Nominal]
      encColor = GV.color [GV.MName "Vote Share Type", GV.MmType GV.Nominal, GV.MNoTitle]
      encDetail = GV.detail [GV.DName "State", GV.DmType GV.Nominal]
      encoding = GV.encoding . encDetail . encX . encY
      transform = GV.transform . makeYVal . makeVS
      config = FV.viewConfigAsHvega vc
      lineSpec = GV.asSpec [(GV.encoding . encDetail . encX . encY) [], transform [], GV.mark GV.Line []]
      dotSpec = GV.asSpec [(GV.encoding . encX . encY . encColor) [], transform [], GV.mark GV.Point []]
      ruleSpec = GV.asSpec [(GV.encoding . encRuleX) [], (GV.transform . makeRuleVal) [], GV.mark GV.Rule []]      
  in
    FV.configuredVegaLite vc [FV.title title, GV.layer [lineSpec, dotSpec, ruleSpec], dat]




turnoutBoostExplainerMD :: T.Text
turnoutBoostExplainerMD = [here|
Let's label our groups by $g$, with turnout $T_g$, population $N_g$ and
Dem preference $P_g$. If the votes cast for dems are $V_D$ out of the total votes $V$,
then post-stratified preference is

$\begin{equation}
P = \frac{V_D}{V}=\frac{\sum_g T_g N_g P_g}{\sum_g T_g N_g}
\end{equation}$

splitting into dem leaners (denoted by $g\in D$) and others ($g \in R$):

$\begin{equation}
P = \frac{\sum_{g\in D} T_g N_g P_g + \sum_{g \in R} T_g N_g P_g}{\sum_{g \in D} T_g N_g + \sum_{g \in R} T_g N_g}
\end{equation}$

Suppose we boost turnout in Dem leaning groups by x (so for a 1% boost, x would be 0.01):

$\begin{equation}
P(x) = \frac{\sum_{g\in D} (T_g + x) N_g P_g + \sum_{g \in R} T_g N_g P_g}{\sum_{g \in D} (T_g + x) N_g + \sum_{g \in R} T_g N_g}
=\frac{x\sum_{g \in D}N_g P_g + V_D}{x\sum_{g \in D} N_g + V}
=\frac{V_D}{V}\frac{1 + x\sum_{g \in D}N_g P_g/V_D}{1 + x\sum_{g \in D} N_g/V} = P \frac{1 + x\sum_{g \in D}N_g P_g/V_D}{1 + x\sum_{g \in D} N_g/V}
\end{equation}$

Usually, we're curious about what $x$ we need for a certain $P(x)$.  For example, $P(x)=0.5$ is the level required to "flip" a state.  So let's
call the $P(x)$ we're hoping for $P_h$ and write $P_h =P\times(1 + \delta)$ or $\delta = \frac{P_h - P}{P}$.
Also, just to simplify things, we'll define $a = \sum_{g \in D}N_g P_g/V_D$ and $b =  \sum_{g \in D} N_g/V$.  So we have

$\begin{equation}
P\times(1 + \delta) = P \frac {1 + ax}{1 + bx}
\end{equation}$

which we can solve for $x$:

$\begin{equation}
x = \frac{\delta}{a - (1+\delta)b}
\end{equation}$

We can understand this formula a bit. We need a boost that is proportional to $\delta$, the gap we need to make up.
Making up ground is easier when Dem preference is high in the groups we are boosting ($a - b$, more or less).

Let's look at a simple example. Imagine 800 voters in group A with 50% voter turnout and leaning 75/25 toward Dems
and 1000 voters in group B with 60% voter turnout and leaning 65/35 toward Republicans.
A Democrat would get 280 votes from the Dem leaners and 210 votes from
the R leaners, for a total of 490.
The Republican would get 120 votes from the Dem leaners and 390 from the R leaners, for a total of 510. So
$P_0=\frac{V_D}{V} = \frac{490}{1000} = 0.49$

How much do we need to boost turnout in group A to flip the state, that is to get P_h = 0.5?
Plugging the numbers above into the equation for $P(x)$ ($\delta = 0.02$; $a = 560/510 = 1.1$; $b = 800/1000 = 0.8$),
we get $x = 0.11 = 11\%$. That is, we'd need to boost turnout in group A by 11%, from 50% to 61%.
|]


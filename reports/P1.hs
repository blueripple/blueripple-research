{-# LANGUAGE DataKinds                 #-}
{-# LANGUAGE DeriveDataTypeable        #-}
{-# LANGUAGE PolyKinds                 #-}
{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE GADTs                     #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE TypeApplications          #-}
{-# LANGUAGE TypeOperators             #-}
{-# LANGUAGE QuasiQuotes               #-}
{-# LANGUAGE AllowAmbiguousTypes       #-}
{-# LANGUAGE TupleSections             #-}
{-# OPTIONS_GHC  -fplugin=Polysemy.Plugin  #-}

module P1 (p1) where

import qualified Control.Foldl                 as FL
import qualified Data.Map                      as M
import qualified Data.Array                    as A

import qualified Data.Text                     as T


import           Graphics.Vega.VegaLite.Configuration as FV

import qualified Frames.Visualization.VegaLite.Data
                                               as FV
import qualified Frames.Visualization.VegaLite.ParameterPlots
                                               as FV                                               

import qualified Knit.Report                   as K
--import qualified Knit.Report.Input.MarkDown.PandocMarkDown    as K
--import qualified Text.Pandoc.Options           as PA

import           Data.String.Here               ( here, i )

import           BlueRipple.Configuration
import           BlueRipple.Utilities.KnitUtils
import           BlueRipple.Data.PrefModel.SimpleAgeSexRace
import           BlueRipple.Data.PrefModel.SimpleAgeSexEducation
import qualified BlueRipple.Model.Preference as PM

import PrefCommon


p1 :: K.KnitOne r
   => M.Map Int (PM.PreferenceResults SimpleASR FV.NamedParameterEstimate)
   -> M.Map Int (PM.PreferenceResults SimpleASE FV.NamedParameterEstimate)
   -> K.Sem r ()
p1 modeledResultsASR modeledResultsASE = do
  brAddMarkDown br2018Intro
  let groupData :: (A.Ix b, Show b)
        => PM.PreferenceResults b FV.NamedParameterEstimate
        -> b
        -> (T.Text, Int, Double, Double)
      groupData pr x = (T.pack (show x)
                       , PM.nationalVoters pr A.! x
                       , PM.nationalTurnout pr A.! x
                       , FV.value $ FV.pEstimate  $ PM.modeled pr A.! x
                       )
      groupDataList :: (Enum b, Bounded b, A.Ix b, Show b)
                    => PM.PreferenceResults b FV.NamedParameterEstimate
                    -> [(T.Text, Int, Double, Double)]
      groupDataList pr = fmap (groupData pr) [minBound..maxBound]
      vRowBuilderGD =
        FV.addRowBuilder @'("Group",T.Text) (\(g,_,_,_) -> g)
        $ FV.addRowBuilder @'("VotingAgePop",Int) (\(_,v,_,_) -> v)
        $ FV.addRowBuilder @'("Turnout",Double) (\(_,_,t,_) -> t)
        $ FV.addRowBuilder @'("Voters",Int) (\(_,v, t :: Double ,_) -> round (t * realToFrac v))
        $ FV.addRowBuilder @'("D Voter Preference",Double) (\(_,_,_,p) -> 100*p)
        $ FV.emptyRowBuilder
      perGroupingChart :: forall b r. (A.Ix b, Enum b, Show b, Ord b, Bounded b, K.KnitOne r)
                       => T.Text
                       -> Int
                       -> M.Map Int (PM.PreferenceResults b FV.NamedParameterEstimate)
                       -> K.Sem r ()
      perGroupingChart title yr mr = do
        pr <-
          knitMaybe ("Failed to find " <> (T.pack $ show yr) <> " in modelResults.")
          $ M.lookup yr mr
        let groupDataL = groupDataList pr
            gdRows = FV.vinylRows vRowBuilderGD groupDataL
        _ <- K.addHvega Nothing Nothing
             $ PM.vlGroupingChart
             title
             (FV.ViewConfig 650 325 0)
             gdRows
        return ()
  _ <- perGroupingChart "Grouped by Age, Race, and Sex" 2018 modeledResultsASR
  brAddMarkDown br2018BetweenFigs
  _ <- perGroupingChart "Grouped by Age, Sex and Education" 2018 modeledResultsASE
  brAddMarkDown br2018AfterFigs
  brAddMarkDown brReadMore
  

--------------------------------------------------------------------------------
br2018Intro :: T.Text
br2018Intro = [i|
In our first deep-dive into some modeling, we take a quick look at the 2018
house elections.  What can we do with data from previous elections to guide
our use of time and money in 2019 and 2020?

1. Estimating voter preference
2. Who voted for Democrats and Progressives?
3. What does this mean for 2019 and 2020?
4. Ways to take action
5. A note about exit polls

## Estimating Voter Preference: What we're doing and why
The 2018 house races were generally good for Democrats and progressives---but why?
Virtually every plausible theory has at least some support:
depending on which pundits and researchers you follow,
you could credibly argue that
[turnout of young voters](https://www.vox.com/2019/4/26/18516645/2018-midterms-voter-turnout-census)
, or [white women abandoning Trump](https://www.vox.com/policy-and-politics/2018/11/7/18064260/midterm-elections-turnout-women-trump-exit-polls>)
, or an
[underlying demographic shift toward non-white voters](https://www.pewresearch.org/fact-tank/2018/11/08/the-2018-midterm-vote-divisions-by-race-gender-education)
was the main factor that propelled the
Blue Wave in the midterms.

But if Democrats want to solidify and extend their gains, we really want to know
the relative importance of each of these factors---in other words,
we want to understand election outcomes in a district or state in terms of:

- Demographics: What are the age, sex, education level, race, etc. of the eligible voters?
- Turnout: For each demographic group, what fraction of those eligible to vote cast votes on election day?
- Voter Preference: When people from each group vote, who are they likely to vote for?

It turns out that breaking this down is difficult because we don't have all the data.

- Demographics: [Census data](https://www.census.gov/programs-surveys/acs.html)
exists for the demographic breakdown of each house district.
- Turnout: The [Census](https://www.census.gov/topics/public-sector/voting/data/tables.2018.html)
also publishes data containing the *national* turnout of each group in each election.
This is helpful but not as geographically specific as we'd like.
- Preference: The [election results](https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/IG0UN2)
tell us how all those groups voted *in aggregate* in each district.

In short, we have geographically specific information on demographics,
demographically detailed but geographically general data on turnout,
and demographically general but geographically specific data on voter preference.

[Edison Research](https://www.edisonresearch.com/election-polling/#one)
did extensive exit-polling in 2018
and, along with others,
[CNN analyzed](https://www.cnn.com/election/2018/exit-polls)
this data, providing some breakdowns of voter preference in various demographic categories.
[Much reporting](https://www.vox.com/policy-and-politics/2018/11/7/18064260/midterm-elections-turnout-women-trump-exit-polls)
has used these results to
[discuss](https://www.nytimes.com/interactive/2018/11/07/us/elections/house-exit-polls-analysis.html)
the voter preferences of the 
[2018 electorate](https://www.brookings.edu/blog/the-avenue/2018/11/08/2018-exit-polls-show-greater-white-support-for-democrats/).
However, there is
[widespread criticism](https://www.nytimes.com/2014/11/05/upshot/exit-polls-why-they-so-often-mislead.html) of exit-polling,
and it may lead us astray in several ways.

Is there a different way to find out which groups voted in what numbers for Democrats, without relying on exit polls?

In the rest of this post, and some subsequent posts, we'll explore
one approach for using the data we have to learn something demographically specific about
voter preference.  That is, we'll infer a national-level voter preference
for each demographic group using the data described above.
We describe the methods in detail
**[here](${brPrefModelUrl brMethods})**,
and the [data and code](${brGithub <> "/preference-model"})
are available at the Blue Ripple [github](${brGithubLanding}).

What are the odds of a voter in any of these demographic groups voting for
a Democrat? Any set of voter preferences (those odds), give rise to
a probability, given the turnout and demographics, of the election result we
observe in a specific district.
In 2018, we had 382 districts with at least one Democrat and one Republican
running (and we ignore the rest,
since we are only interested in the choices people make between Democrats
and Republicans). We can combine all those to get the chance that
a particular set of voter preferences explains all the results.  From this, we
can infer the most likely voter preferences.
The details are spelled out in a 
**[separate post](${brPrefModelUrl brMethods})**.

## Who Voted For Democrats and Progressives?
There are many ways to partition the electorate, e.g., age, educational
attainment, race or sex.  We can also group people by answers to specific
questions about issues or previous votes.  In subsequent posts we will look at all
these possibilities, or point to other sources that do.  For our first look we're
going to stick with some standard demographic groupings that originate with the
[Census ACS data](https://www.census.gov/programs-surveys/acs.html). We'll look first
at grouping the electorate by age (old/young = over/under 45), sex (female or male),
and race (non-white and white), as well as the same age and sex categories but
with a split on education (non-college-grad or college-grad) instead of race.

We know that these categories are oversimplifications and problematic in
various ways.  But we are limited to the categories the census provides and
we want the number of groups to be reasonably small for this analysis to be useful.

In the charts below, we show our inferred voter preference split by demographic group.
We use size to indicate the number of voters in each group and the color to
signify turnout. Hovering over a data point will show the numbers in detail.
|]
    
br2018BetweenFigs :: T.Text
br2018BetweenFigs = [i|
The most striking observation is the gap between white and non-white voters’
inferred support for Democrats in 2018. Non-whites
have over 75% preference for Dems regardless of age or gender,
though support is stronger among non-white female voters than
non-white male voters. Inferred support from white voters in 2018
is substantially lower, roughly 40-50% across age groups and genders.

Splitting instead by age, sex and education:
|]

br2018AfterFigs :: T.Text
br2018AfterFigs = [i|
Here we see a large (>15% point) gap between more Democratic-leaning
college-educated voters and voters without a
four-year degree.  We also see a similar gap with age, where younger
voters are 15% or so more likely to vote for Democrats. 

In both charts, the size of the circles reflects the number of people of
voting age in each group.  In 2018
Democrats won elections by winning smaller demographic groups
by a large margin and losing larger demographic groups by a small
margin.

Often those demographically small groups also had lower turnout.
One thing to focus on now is boosting turnout among the groups that
are more likely to vote for Democrats:

- Non-white voters of any age, with or without a college degree
- Young voters of any race, especially those with a college degree or in college
- College educated voters of any race or age

## What does this mean for 2019 and 2020?
These charts can be viewed as pictures of the so-called Democratic
"base"--voters of color and younger white voters, particularly those
with a college degree.
Winning more votes comes from raising turnout among that
coalition *and* trying to improve Democratic voter preference
with everyone else.

What we really want to know is demographic voter preference
at the *district* or *state*
level. Most candidates
win local races and, because of the
[electoral college](https://www.nationalpopularvote.com/),
presidential elections also
hinge on very local outcomes. So, in the near future,
we plan to investigate the issue of turnout and changing
voter-preference for single
states or congressional districts.  We hope to use that
analysis to help interested progressives discover where
turnout is key vs. places where changing voter preference
might be more important. Both are important everywhere, of course.
But in some places, one might be a much larger source of
Democratic votes.

## Ways to take Action
Democrats and progressives will benefit
from get-out-the-vote work and work fighting voter suppression.
Both improve turnout among the groups most likely to vote
for blue candidates.

We suggest that you donate your time or money to organizations fighting to improve voter
access and turnout. In later posts we'll dig into who's doing this work at a state and local
level, but here are some groups that are operating nationally.

- [Demand The Vote](https://www.demandthevote.com/) is an information
clearinghouse for voter access and voting rights legislation in all 50 states.
- [When We All Vote](https://www.whenweallvote.org/), Michelle Obama's new
organization, works to increase voter turnout nationally and has opportunities to
volunteer and donate money.
- The [Campus Vote Project](https://www.campusvoteproject.org/why-student-voters-matter) works
to improve registration and turnout among college students.
Students can [sign up](https://www.campusvoteproject.org/sign-up) to work on their own campuses.
- [Next Gen America](https://nextgenamerica.org/) works nationally to improve youth turnout.
They have a [variety of ways](https://nextgenamerica.org/act/) you can donate time or money to the cause. 

## A Note about Exit Polls
These results are similar but markedly different from the exit polls.
**[This post](${brPrefModelUrl brP1ExitPolls})**
has more details on that comparison.

### Updates
- 9/26/2019: Updated demographic data from the census to the just released 2018 ACS data.
|]  
  
------------- unused
intro2018 :: T.Text
intro2018 = [i|
## 2018 Voter Preference
The 2018 house races were generally good for Democrats and progressives--but why?
Virtually every plausible theory has at least some support –
depending on which pundits and researchers you follow,
you could credibly argue that
turnout of young voters[^VoxYouthTurnout], or white women abandoning Trump[^VoxWhiteWomen], or an underlying
demographic shift toward non-white voters[^Pew2018] was the main factor that propelled the
Blue Wave in the midterms.

If Democrats want to solidify and extend their gains, what we really want to know
is the relative importance of each of these factors – in other words,
how much of last year’s outcome was due to changes in demographics vs.
voter turnout vs. voters changing their party preferences?
It turns out that answering
this is difficult. We have good data on the country’s changing demographics,
and also on who showed up to the polls, broken down by gender, age, and race.
But in terms of how each sub-group voted, we only have exit polls and
post-election surveys, as well as the final election results in aggregate.

* We consider only "competitive" districts, defined as those that had
a democrat and republican candidate. Of the 435 House districts, 
382 districts were competitive in 2018.

* Our demographic groupings are limited by the the categories recognized
and tabulated by the census and by our desire to balance specificity
(using more groups so that we might recognize people's identity more precisely)
with a need to keep the model small enough to make inference possible.
Thus for now we split the electorate into "white" (non-hispanic) and "non-white",
"male" and "female" and "young" (<45) and "old".

* Our inference model uses Bayesian techniques
that are described in more detail in a separate
[Preference-Model Notes](${brPrefModelUrl brMethods})
post.

Several folks have done related work, inferring voter behavior using the
election results and other data combined with census data about demographics.
in U.S. elections. In particular:

* [Andrew Gelman](http://www.stat.columbia.edu/~gelman/)
and various collaborators have used Bayesian and other inference techniques to
look at exit-poll and other survey data to examine turnout and voting patterns.  In particular
the technique I use here to adjust the census turnout numbers to best match the actual recorded
vote totals in each district comes from
[Ghitza & Gelman, 2013](http://www.stat.columbia.edu/~gelman/research/published/misterp.pdf).

* [This blog post](http://tomvladeck.com/2016/12/31/unpacking-the-election-results-using-bayesian-inference/)
uses bayesian inference and a beta-binomial
voter model to look at the 2016 election and various subsets of the
electorate. The more sophisticated model allows inference on voter polarity 
within groups as well as the voter preference of each group.

* [This paper](https://arxiv.org/pdf/1611.03787.pdf)
uses different techniques but similar data to
look at the 2016 election and infer voter behavior by demographic group.
The model uses county-level data and exit-polls
and is able to draw inferences about turnout and voter preference and to do so
for *specific geographical areas*.

* This [post](https://medium.com/@yghitza_48326/revisiting-what-happened-in-the-2018-election-c532feb51c0)
is asking many of the same questions but using much more specific data, gathered from
voter files[^VoterFiles].  That data is not publicly available, at least not for free.

Each of these studies is limited to the 2016 presidential election. Still,
each has much to offer in terms of ideas for
pushing this work forward, especially where county-level election returns are
available, as they are for 2016 and 2018[^MITElectionLabData].

As a first pass, we modeled the voting preferences of our
8 demographic sub-groups in the 2018 election,
so we could compare our results with data from exit polls and surveys.
The results are presented in the figure below:

[^VoxYouthTurnout]: <https://www.vox.com/2019/4/26/18516645/2018-midterms-voter-turnout-census>
[^VoxWhiteWomen]: <https://www.vox.com/policy-and-politics/2018/11/7/18064260/midterm-elections-turnout-women-trump-exit-polls>
[^Pew2018]: <https://www.pewresearch.org/fact-tank/2018/11/08/the-2018-midterm-vote-divisions-by-race-gender-education/>
speaks to this, though it addresses turnout and opinion shifts as well.
[^VoterFiles]: <https://www.pewresearch.org/fact-tank/2018/02/15/voter-files-study-qa/>
[^MITElectionLabData]: <https://electionlab.mit.edu/data>
|]

  
postFig2018 :: T.Text
postFig2018 = [i|
The most striking observation is the chasm between white and non-white voters’
inferred support for Democrats in 2018. Non-whites were modeled to
have over 75% preference for Dems regardless of age or gender,
though support is even a bit stronger among non-white female voters than
non-white male voters5. Inferred support from white voters in 2018
is substantially lower, roughly 35-45% across age groups and genders.
In contrast, differences in inferred preferences by age
(matching for gender and race) or gender (matching for age and race) are not
particularly striking or consistent
(e.g., comparing white males in the under-25 and over-75 groups).
Overall, we’re heartened that our model seems to work pretty well,
because the results are broadly consistent with exit polls and surveys[^ExitPolls2018][^Surveys2018]. 
Thus, our model confirmed prior work suggesting that non-white support for
Democrats in 2018 was much higher than that by whites, across all
genders and age groups. But it still doesn’t tell us what happened in 2018
compared with prior years. To what extent did Democrats’ gains over 2016 come from
underlying growth in the non-white population, higher turnout among non-whites,
increased preference for Democrats (among whites or non-whites), or some combination
of these and other factors? That requires comparing these data to results
from earlier elections – which is what we’ll do in subsequent posts. Stay tuned. 

[^ExitPolls2018]: <https://www.nytimes.com/interactive/2018/11/07/us/elections/house-exit-polls-analysis.html>,
<https://www.brookings.edu/blog/the-avenue/2018/11/08/2018-exit-polls-show-greater-white-support-for-democrats/>
[^Surveys2018]: <https://www.pewresearch.org/fact-tank/2018/11/29/in-midterm-voting-decisions-policies-took-a-back-seat-to-partisanship/>
|]

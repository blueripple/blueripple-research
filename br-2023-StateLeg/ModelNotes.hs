{-# LANGUAGE QuasiQuotes #-}

module ModelNotes where

import Data.String.Here (here)

part1 :: Text
part1 = [here|
### Modeling State Legislative Elections
As donors have become more willing to give money to state-legislative candidates
it’s become more important to know as much as possible about the competitiveness
of each seat, especially in states where the control or supermajority is potentially
up for grabs.

Local expertise in the particular district or set of districts is often
the best way to figure out which districts are close-but-winnable.
But that’s often hard to find and requires vetting. So it’s useful to have ways of
understanding state-legislative districts that we can apply to the country as a whole.

Polling would, of course, also be useful, but there are almost never polls for
state-legislative races. Polls are expensive and state-legislative-races
don’t usually generate enough news interest to field one. Also, one puzzle of
working in geographies as small as state-legislative-districts is that there is
less information available about the demographic characteristics of the people who live there,
making it hard to weight the polls correctly.

## Historical Partisan Lean
The most straightforward way to find close-but-winnable races is to look at what happened
in previous elections, either for the same office or others.
That’s straightforward but not easy.
[Dave’s Redistricting](https://davesredistricting.org/maps#home)
does a spectacular job of joining district maps and precinct-level data from previous
elections to create an estimate of the historical partisan lean[^pl] (HPL) of every
state-legislative-district in the country. Their rich interface allows the user to choose various
previous elections and combinations of them to estimate the partisan lean.

[^pl]: By “partisan lean” we mean the 2-party vote share of Democratic votes, that
is, given $D$ democratic votes and $R$ Republican votes and $O$ third-party votes,
we will report a partisan lean of $\frac{D}{D+R}$, ignoring the third-party votes.
For guessing who might win a district in the future, this is the most useful partisan
lean number.

Any state-legislative-district
which has an HPL that is very Democratic or very Republican is likely not a race the other
party can win in this cycle. As an example, here is chart of the VA house HPL[^hplVA].
As with many such maps, it looks mainly Republican (red) but that is because the
districts with Democratic leaning HPL are often geographically smaller, in
places, like cities, with higher population density.

[^hplVA]: Using 2020 ACS population estimates and a composite of presidential and statewide
elections from 2016-2021: 2018 and 2020 senator as well as Governor and AG from 2021.
|]

part2 :: Text
part2 = [here|
There are some questions this approach leaves unanswered. Why does
each district have the HPL it does? Are there districts with HPLs that are surprising given
their location and demographic makeup? This sort of analysis provides opportunities to
add a possibly flippable or safe-looking-but-vulnerable district to a list for donors or
remove districts that were already marginal but seem more so when looking at the underlying
demographics.

For example, imagine an SLD with HPL just below your cutoff for winnable.
Supose a demographic analysis shows that it’s
demographic partisan lean (DPL, something we’ll explain below) is over 50%. Might that be a district
where a good and well-resourced candidate can win a race or, in losing a close race, change the
narrative going forward? Or might it be useful to be alerted to a district which looks
easily winnable by HPL standards but with DPL much closer to 50%? That district might be vulnerable
to the right opposition candidate, especailly in a tough political environment.

HPL also doesn’t help much with scenario analysis. E.g., what would happen in a
given seat if women turned out at higher rates and were more likely to vote
for the democratic candidate? This is a simple way to consider the effect
of the Dobbs ruling on current elections. With a demographic
breakdown and model of turnout and partisan lean we can estimate these
sorts of effects.

## Demographic Partisan Lean
Rather than consider how people in a specific place have voted in previous elections,
Demographic Partisan Lean (DPL) instead categorizes people demographically, in our case
by state, population-density, age, sex, educational-attainment and race/ethnicity.
Using large surveys of turnout and party-preference
we model expected turnout and party-preference for each of those categories.
Then, given the numbers of people in each of those categories in a district, compute
the (modeled) partisan lean among expected voters. Here’s what this looks like in VA:
|]

part3 :: Text
part3 = [here|
These maps are, unsurprisingly, very similar but there are some large differences which
are clearer on the chart of differences below
|]

part4 :: Text
part4 = [here|
Just like HPL, DPL involves choices about which data to use. Which election(s) should we use
as sources for voter turnout and party preference? Even for a given election, what sources of
data should we use?

DPL has data issues all its own as well, e.g., how do we know how many of each category of people
live in the district? DPL requires a large number of modeling choices, including which
categories to include in the first place, how to break them down into buckets for modeling,
what sort of model to use, how to handle uncertainty in that model, etc.  We’ll discuss all this
in more detail below.

One way to think of DPL is as a very detailed analysis of
voting patterns based on race or education or age. Each of those can be a valuable predictor
of turnout and party-preference. But sometimes the combination of categories is essential
for understanding. For example, the Republican lean of white-non-college educated voters
is greater than you expect from the Republican lean of white voters and the Republican lean
of non-college-educated voters combined. This gets even more complex and harder to keep track of
once you add age and sex. Further, these things vary from state to state. Population density
affects party-preference quite strongly in ways not captured by any of those categories.
All of these things vary differently for turnout and party-preference. And turnout and
preference both matter when it comes to figuring out which districts are close enough to
be worth donor investment.

DPL is the partisan lean of a district if,
within each demographic group, each person’s choice to vote and whom to vote for
was more or less the same as everyone else in the state living at similar population density.
This is likely less
predictive than knowing who those same people voted for in the previous few elections. But
it’s also different information, and particularly interesting when it’s inconsistent with
the HPL.

## Scenario Analysis
Since the DPL is built from an estimate of who lives in a district and how likely each of them is
to turn out and vote for the Democratic candidate, we can use it to answer some
“what would happen if...” sorts of questions. If you imagine some voters are
more energized and thus likely to vote and/or more likley to vote for the Democratic
candidate, that might change which seats are in play and which are safe. This might
also be useful when considering a targeted intervention. E.g., how much would you have to
boost turnout among people 18-35 to meaninfully shift the likely vote-share in competitive
districts? Let’s look at a couple of examples, again using VA.

Imagine we think we can boost youth turnout by 5% across the state.
How much would that change
|]

part5 :: Text
part5 = [here|
## DPL: our model
We’re going to explain what we do in steps, expanding on each one further down in the document
and putting some of the mathematical details in other, linked documents. Our purpose here
is to present a thorough idea of what we do without spending too much time on any of the
technical details.

We begin with the Cooperative Election Study ([CES](https://cces.gov.harvard.edu)),
a highly-regarded survey which
runs every 2 years, speaks to people before and after the general election and validate’s
people’s responses about turnout via a third-party which uses voter-file data[^vf]. The
CES survey includes approximately 60,000 people per election cycle. It gathers demographic and
geographic information about each person as well as information about voting, party preference
as well as various other opinion and issue questions.

[^vf]: CES has used different validation partners in different years. Whoever parteners with the
CES in a particular year attempts to match a CES interviewee with a voter-file record in that state
to validate the survey responses about registration and turnout.

The CES data includes state and congressional district of each person interviewed. We use that
to join the data to population-density data from the 5-year American Community
Survey ([ACS](https://www.census.gov/programs-surveys/acs/microdata.html)) data, using
the sample ending the same year as the CES survey was done.

We then fit multi-level regressions of that data, one for
turnout, one for party-preference and one for both at the same time.
To compute expected turnout, party-preference of voters or DPL, we then
“post-stratify” the model using the demographics of a given SLD. That
demographic data is also sourced from the ACS, though via a different path
because the microdata is not available at SLD size geographies. Due
to the method we use for modeling (Hamiltonian Monte Carlo), the
result of our post-stratification is actually a distribution of possible
turnout, party-preference or DPL, giving us an expectation and various
ways to look at uncertainty.

Let’s dig into the details a bit!

# Choosing Our Sources
Our present model uses the 2020 CES survey as its source. We will have the
2022 CES survey as a possible source available to us soon. It’s not clear
which is more appropriate for thinking about 2024. 2022 is more recent and so
might better capture current demographic voting trends,
but 2020 is the most recent *presidential* election year.
Using the 2020 survey, we choose presidential vote as our
party-preference indicator. If we use 2022, we will
switch to house-candidate vote and include incumbency in the regression model
for party-preference. We’re thinking about ways to combine these but it’s not
straightforward for a variety of reasons. We could also model using both and
have two DPL numbers to look at per district.

ACS is the best available public data for figuring out the demographic breakdowns
of who lives in a district. But that data is limited at the scale of a
state-legislative district. We get ACS population tables at the census-tract
level and aggregate them to the district level. Then we use our own statistical methods^[nsm] to
combine those tables to produce a population table with all of our
demographic categories in each district.

[nsm]: We will produce another explainer about just this part of the DPL. Basically,
the ACS data is provided in tables which cover a maximum of 3-categories at a time,
for example citizenship, sex and race/ethnicity. To get the table we want,
citizenship x age x sex x education x race/ethnicity–citizenship is there so we
can get the final table for citizens only since only citizens can vote–we need
to “combine” 3 of those tables. We use data from larger geographies and a model
of how those tables fit together based on the data in each of them, to estimate
the correct combination in each district. There is a large and confusing literature
full of techniques for doing this. The one we’ve developed has some advantages
in terms of accuracy and statistical bias and the disadvantage of being somewhat more
complex.

There are various companies that use voter-file data
(which is public but costs $$ and requires work to standardize) and various other data and
modeling (e.g., using names to predict race and sex) to estimate a population table
for any specific geography. We’ve not had a chance to compare the accuracy of our methods
to theirs but we imagine those methods are capable of being quite accurate as well.

# Modeling the Turnout and Party-Preference
The CES survey provides data for each person interviewed. The demographic data is provided,
along with a weight, designed so that subgroups by age, race, etc. are correctly represented once
weights are accounted for. For example, if you know that there are equal numbers of men and women
(CES has just begun tracking gender categories other than male or female but the ACS does not)
in a congressional district but your survey has twice as many women as men, you would adjust the
weights so that those interviews have equal representation in a weighted sum. Because our demographic
categories are more coarse-grained than what the CES provides (e.g., CES give age in years but we want
to model with 5 age buckets) we need to aggregate the data. We use the weights when aggregating and this
means that in the aggregated data we have non-whole numbers of voters and voters preferring one party or the other.

We imagine each person in a demographic category and state has a
specific fixed probablilty of voting and each voter a different probability of voting
for the Democratic candidate. This would lead to a binomial model of vote
counts. This is obviously a simplification but a fairly standard one, and a reasonable fit
to the data. As mentioned above, we have non-whole counts. So we use a generalization of the
binomial model^[bg] which allows for this.

[bg]: Specifically, we use the binomial density but just allow non-integer “successes” and “failures”.
This is not an actual probability density and gives slightly lower (check this!) likelihood
to very low and very high counts than it should. Fixing this is one project for our
next version!

Our specific probability is a linear function of the log-density^[lpd] plus a number for each of the categories
and some of their combinations. In particular we estimate using “alphas” for
state, age, sex, education, race/ethnicity, the combination of age and education, age and race/ethnicity,
education and race, and state and race. For the state factor and all the combination factors,
we use “partial-pooling” which means we allow the model itself to estimate how big a factor these variations
should be.

[lpd]: For modeling, we use logarithmic population density.
Population density in the US varies by several orders of magnitude, from 10s
of people per square mile in the most rural places to over 100,000 per square mile
in the densest places, like New York City. That makes it difficult to use as a
predictor. There are various approaches to “fixing” this. We can classify places
by density, e.g., as rural, suburban and urban. Or we can divide density into
quantiles and use those for modeling. We choose a log-transform to compress
the range, somewhat like using quantiles, but preserve the continuous variation.

We use [Stan](https://mc-stan.org), which then runs a
[Hamiltonian Monte Carlo](https://en.wikipedia.org/wiki/Hamiltonian_Monte_Carlo)
to estimate the parameters. Because oh how monte-carlo methods work, we end up with
not only our expected parameter values but also their distributions, allowing us to
capture uncertainties. This is also true of post-stratifications, which also come with
distributions of outcomes and thus things like confidence intervals.

There’s an important last step. We post-stratify these modeled probabilities
across an entire state, giving the expected number of votes in that state. But
we know the actual number of votes recorded in the state and our number won’t usually match
exactly. So we adjust each probability using a technique pioneered by
[Hur & Achen](https://www.aramhur.com/uploads/6/0/1/8/60187785/2013._poq_coding_cps.pdf),
and explained in more detail on pages 9-10 of
[this paper](http://www.stat.columbia.edu/~gelman/research/published/mrp_voterfile_20181030.pdf).
We can apply the same adjustment to our confidence intervals giving us an approximate
confidence interval for the adjusted parameter or post-stratification result.

We do this for turnout alone, party-preference of voters, where we match to known
vote totals for the candidate of each party, and both together.

The final result of all this work is an estimate of the DPL for any SDL in the country.
|]

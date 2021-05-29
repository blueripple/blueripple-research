Demographic differences account for some but not all of the variations in turnout
gaps among states. That is, turnout gaps vary significantly by state
*even once we account for the demographic differences among them.* For example,
only 8 of the 32 point difference between the AL and WA turnout gaps is demographic in origin.
The other 24 is state-specific, i.e., has something to do with AL and WA in particular.

Why might there be state-specific effects on turnout?

- Specific state policies may encourage or discourage voting by regulating early
  voting, vote-by-mail, poll hours, ID requirements and other aspects of voter access.
  The policies often disproportionately affects certain voters.
- Grassroots organizing and campaigning, both largely local,
  boosts turnout in demographically specific ways.
- State and local elections have their own dynamics, energizing certain groups of voters.

There’s been a great deal of focus on voter
organizing and suppression and how that affected turnout in 2020,
particularly in GA, where Dems prevailed, and FL and TX, where we did not.
This prompts our interest in a data-informed view of state-specific turnout.

Modeling this is somewhat complicated!
First we need a baseline view of state-level turnout across demographic groups.
Our model uses data from the Current Population Survey Voter and Registration Supplement (CPS-VRS)
and the American Community Survey (ACS), both produced by the US Census bureau and we'll
explain a bit about those below. We're also going to
talk briefly about MRP modeling
([Click here](https://www.youtube.com/watch?v=bq9c1zsR9NM>)
for a good 3 minute explanatory video about MRP),
anchoring this and further analyses based on similar models.

Three quick caveats before we dig in.

1. It is our firm belief
that voting should be easy for everyone,
regardless of demographics or partisan identity,
and all measures to suppress voting are wrong.
We support vote-by-mail and more polling places, open for longer hours on more days, etc.
2. Lumping all VOC together is simplistic and reductive,
because it ignores many other non-racial/ethnic factors that affect turnout.
But we are particularly interested in the interplay of voter suppression and organizing,
where race/ethnicity is a big factor.
3. Any conclusions linking turnout to state voting policies
will be suggestive at best, because these analyses are
[notoriously difficult](<https://scholar.princeton.edu/sites/default/files/jmummolo/files/jop_voterid_print.pdf>).
Standard surveys don’t give us enough reliable data to make strong conclusions.

Let’s dive into the analysis.
We’re going to start with two detailed sections about the underlying data and approach,
describe our model, and then pose and answer two initial questions about the
VOC/WHNV turnout gap.

### Post Roadmap

- Intro (1): Our Data—The CPS-VRS and ACS
- Intro (2): MRP—A Quick Primer
- Our Basic Model
- Initial questions (and answers) about voter turnout and race.
- What’s Next?

## Intro (1): Our Data—The American Community Survey and the CPS Voting and Registration Supplement

Each year, the U.S. Census Bureau (USCB) conducts the
[“American Community Survey”](https://www.census.gov/programs-surveys/acs)
which, for our purposes,
is an update to the decennial census.  Surveys are sent to ~3.5 million
households (of about 125 million) so it’s not a complete count.
There is also less geographic specificity
to the reported results than the decennial census.
The decennial census reports results down to the “block” level, whereas
much of the ACS data is available only at the “Public Use Micro-data Area” (PUMA) level—
each PUMA has about 100,000 people.  Still, this is enough granularity for most work
at the state or congressional-district level.
We use 2018 ACS data rather than the 2010 decennial census because it provides a more up-to-date
picture of the demographics of each state.
(We’ll re-run this analysis once we have full 2020 census results or the 2020 ACS.)

In addition, each election year,
the USCB produces the
[Voting and Registration Supplement](https://www.census.gov/topics/public-sector/voting.html)
to the Current Population Survey (CPS-VRS),
asking approximately 100,000 people nationwide
if they voted in the general election.
Responses are paired with county of residence, demographic information
(age, sex, race, ethnicity, education, etc.),
allowing estimation of voter turnout among various groups and in various places.

We make one important tweak to the CPS-VRS data:
The survey responses are “self-reported” and not independently validated,
so there are
[reporting errors](http://www.electproject.org/home/voter-turnout/cps-methodology)
that tend to overestimate turnout, and in a way which differs systematically
among states. To account for this, we adjust the turnout probabilities from the CPS-VRS
so that when they are post-stratified across the voting eligible population (VEP)
of each state, we get the correct total turnout.  This was first suggested by
[Achen and Hur](https://www.aramhur.com/uploads/6/0/1/8/60187785/2013._poq_coding_cps.pdf)
and we follow the procedure outlined by
[Ghitza and Gelman](http://www.stat.columbia.edu/~gelman/research/published/misterp.pdf)
(p. 769), to compute the adjustment for each state, using the vote totals from
[United States Election Project](http://www.electproject.org/home/voter-turnout/voter-turnout-data)
and group populations from the ACS.

Crucially, CPS-VRS data
[seems to under-report the turnout gaps between white and non-white voters.
](https://static1.squarespace.com/static/5fac72852ca67743c720d6a1/t/5ff8a986c87fc6090567c6d0/1610131850413/CPS_AFS_2021.pdf)
So all of our results looking at race-specific turnout
should be viewed skeptically: the gaps are likely larger than we see in the data,
though it's unclear if this comes from over-estimating VOC turnout or under-estimating
WNH turnout.
There are other publicly available
surveys which, when possible, validate survey responses via state voter files,
for example the
[CCES](https://cces.gov.harvard.edu).  That survey is smaller: approximately
50,000 people surveyed each year, with about 40,000 validated voters. For this post,
we stick to the CPS-VRS because it's bigger. When the data is available,
we may repeat this analysis with the 2020 CCES survey.

The 100,000 people surveyed by the CPS-VRS are distributed throughout the country, so there
will be a limited number of people in each state, particularly less populous ones.
Once you start breaking those people down by demographic groups, the number of people
per group gets quite small.  For example, our model has binary groupings for age, sex and
education and a 4-category grouping for race and ethnicity. Considering
each of the 50 states plus DC, we have $2\times  2 \times 2 \times 4 \times 51 = 1632$ groups.
If people were distributed evenly among those groups, we might have 60 or so people in each.
But people are not distributed equally among those groups!
Some states are smaller and some may not have
very many people in some of those categories.  So how can we hope to understand any
state and race effects in turnout?

That's where MRP comes in.

## Intro(2): MRP—A Quick Primer

Though there might not be many people in any one Age/Sex/Education/Race/State group, each person
surveyed has many things in common with many people in other groups.  The **MR** part of **MRP** stands
for **M**ulti-level **R**egression,
a modeling technique which allows “partial-pooling” of the data. This means that the estimation
of the turnout probability in each group is built partially from the data in that group and partially
from the data in the shared categories.

Consider a young, female, Black, college-graduate in MI.  We
could estimate her turnout probability using just young, female, Black, college-educated voters in MI.
But there won't be that many potential voters like her in the CPS-VRS,
which would make the estimate very uncertain. However, that
potential voter presumably has much in common with other young, female, Black, college-educated voters *in
other states.*  So we could use all of them to estimate her chance of voting.
Unfortunately, then we lose whatever information is specific about such voters in MI!
MR models allow us to use *both*.

Models can be constructed to partially-pool along different groupings.
The MR technique and tools we use
(namely, [Hamiltonian Monte Carlo](https://en.wikipedia.org/wiki/Hamiltonian_Monte_Carlo)
via [Stan](https://mc-stan.org/about/))
allow the data itself to determine how much partial-pooling leads
to the best estimates.

Once we have estimates for every group in every state, we turn them into
turnout numbers or probabilities via **P**ost-stratification: multiplying
the estimated probabilities by the actual number of people in each group,
and adding these up to figure out how many people are likely to vote. Without
post-stratification, we'd need to weight the CPS-VRS to match the population in each
state and that creates thorny modeling issues all by itself[^dataWeight].
Instead, we use CPS-VRS to estimate
group-level probabilities and then
post-stratify them using the actual populations in each state.

[^dataWeight]: Using weights in the data input to the model raises the question of the uncertainty
    of the weights themselves, something which might require its own model!

The Monte-Carlo modeling produces confidence intervals for the parameters,
*and* the post-stratifications that use them.
The fact that some groups are very small, making probabilistic inference difficult,
will show up in our results as wide confidence intervals.
Partial-pooling helps, but only so much.

## Our Basic Model

Our basic model includes age (under 45 or 45-and-over),
sex (female or male), education (non-college-graduate or college-graduate),
race/ethnicity (Black-non-Hispanic, Hispanic, Asian/Other, and white-non-Hispanic) and state.
We recognize that these categories are reductive.  In the case of sex
we are limited to categories provided by the CPS data. For age and education
we've chosen to simplify the categories to keep the modeling simple.
For race/ethnicity, we‘re using a slightly richer set of categories,
since turnout varies widely among these groups.

We add a congressional-district-level population-density
factor and interactions between education and VOC/WHNV,
a term in the model that estimates the effect of being, e.g.,
white-non-Hispanic (WNH) *and* college-educated over and above the
effects of being in either category separately. Crucially,
we also include an interaction between state and VOC/WNHV,
a term which estimates the *state-dependent* portion of the turnout gap.

We fit a binomial model, estimating the probability that voters in each subgroup will vote.
The model uses partial-pooling in the national turnout by race,
turnout probability in each state,
and for the interaction between state and race, allowing the data to determine the best
balance among these for estimating the turnout of a particular subgroup.

A more complex model might:

- Expand the categories: e.g., including religion or marital status.
- Allow partial pooling for more of the categories.
- Add more interactions between categories, for example modeling the effect of race and gender taken together[^interactions].

[^interactions]:  We ran our model with some of these interactions and found that, other than the ones we already had,
    they appeared to add little information.

Because we are interested in local organizing and state-level voter suppression,
we focus on the state-specific portion of the turnout gap,
in particular how much the gap
in each state differs from what we would expect based on the demographics
(age, sex, education, race/ethnicity, local population density) of those voters.
So we post-stratify—using the ACS data[^acsAvail]—on VOC and WHNV separately in each state,
with and without state/race interactions.

[^acsAvail]: We don't yet have ACS data for 2020,
    so for now we are using 2018 ACS data in our post-stratifications.

## Two initial questions (and answers) about voter turnout and race

Now we’re in a position to answer two questions about each state’s VOC turnout:

- *How much better (or worse) was each state’s turnout gap
  in 2020 compared with what one would have expected from demographics alone?*
- *Which states have significant (positive or negative) impact on the turnout gap?*

\(1) Observed vs. expected turnout gaps in 2020 by state:
Below we look only at the *state-specific* turnout gap, taking
the difference of the full turnout gap in a state
([figure 1](#figure_fullGap))
and the demographic turnout gap
([figure 2](#figure_demographicOnly)).
We chart these gaps on the same scale as the
previous two charts so comparing magnitudes is straightforward.
CO and AL are at the top with 8 point better-than-expected
turnout gaps while WA, something of an outlier, is at the bottom,
with a 16 point worse-than-expected turnout gap.

Demographic differences account for some but not all of the variations in VOC turnout
among states. That is, VOC turnout varies significantly by state
*even once we account for the demographic differences among the states.* For example,
only 8 of the 32 point difference between AL and WA is demographic.  The other 24
is state-specific.

Why might there be state-specific effects on turnout?

- Specific state policies may encourage or discourage voting by regulating early
  voting, vote-by-mail, poll hours, ID requirements and other aspects of voter access.
  The policies often disproportiantely affects certain voters.
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
talk briefly about [MRP modeling](https://www.youtube.com/watch?v=bq9c1zsR9NM>),
anchoring this and further analyses based on similar models.

Three quick caveats before we dig in.

1. It is our firm belief
that voting should be easy for everyone,
regardless of demographics or partisan identity,
and all measures to suppress voting are wrong. Under all circumstances
we support vote-by-mail and more polling places, open for longer hours on more days, etc.
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
describe our model, and then pose and answer two initial questions about voter turnout and race.

### Post Roadmap

- Intro (1): Our Data—The CPS-VRS and ACS
- Intro (2): MRP—A Quick Primer
- Our Basic Model
- Initial questions (and answers) about voter turnout and race.
- Conclusions

## Intro (1): Our Data–The American Community Survey and the CPS Voting and Registration Supplement

Each year, the U.S. Census Bureau (USCB) conducts the
[“American Community Survey” ](https://www.census.gov/programs-surveys/acs)
which, for our purposes,
is an update to the decennial census.  Surveys are sent to ~3.5 million
households; it’s not a complete count.  There is also less geographic specificity
to the reported results than the decennial census.
The decennial census reports results down to the “block” level, whereas
much of the ACS data is available only at the "Public Use Microdata Area" (PUMA) level—
—each PUMA has about 100,000 people.  Still, this is enough granularity for most work
at the state or congressional-district level.
We use ACS data rather than the 2010 decennial census because it provides a more up-to-date
picture of the demographics of each state.
(We’ll re-run this analysis once we have full 2020 census results.)

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
that tend to overestimate turnout in a way which differs systematically
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
[seems to under-report the gaps between White and non-White voters.](https://static1.squarespace.com/static/5fac72852ca67743c720d6a1/t/5ff8a986c87fc6090567c6d0/1610131850413/CPS_AFS_2021.pdf)
So all of our results looking at race-specific turnout
should be viewed skeptically: the gaps are likely larger than we see in the data,
though it's unclear if this comes from over-estimating VOC turnout or under-estimating
WNH turnout.
There are other publically available
surveys which, when possible, validate survey reponses via state voter files,
primarily the
[CCES](https://cces.gov.harvard.edu).  That survey is smaller: approximately
50,000 people surveyed each year, with about 40,000 validated voters. For this post,
we stick to the CPS-VRS because it's bigger. In a future post, we may repeat this analysis
with the 2020 CCES survey.

The 100,000 people surveyed by the CPS-VRS are distributed throughout the country, so there
will only be a limited number of people in each state, particularly less populous ones.
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
a modeling technique which allows "partial-pooling" of the data. This means that the estimation
of the turnout probability in each group is built partially from the data in that group and partially
from the data in the shared categories.  For example, the turnout probability for
young, female, college-educated, Black voters in MI is fit partially from just those voters,
partially from all young, female, college-educated, Black voters in the
entire country, and partially from all voters in MI.  Models can be constructed to allow
partial-pooling along different groupings.  The **MR** technique and tools we use
(namely, [Hamiltonian Monte Carlo](https://en.wikipedia.org/wiki/Hamiltonian_Monte_Carlo)
via [Stan](https://mc-stan.org/about/))
allow the data itself to determine how much partial-pooling leads
to the best estimates.

Once we have estimates for every group in every state, we turn them into
turnout numbers via post-stratification: multiplying
the estimated probabilities by the actual number of people in each group,
and adding these up to figure out how many people are likely to vote. Without
post-stratification, we'd need to analyze where the CPS-VRS was or was not
representative of the population.  Instead, we use CPS-VRS to estimate
group level probabilities and then weight them using the actual populations
in each state.

The Monte-Carlo modeling produces confidence intervals for the parameters,
*and* the post-stratifications that use them.
The fact that some groups are very small, making probabilistic inference difficult,
will show up in our results as wide confidence intervals.
Partial-pooling helps, but only so much.

## Our Basic Model

Our basic model includes age (under 45 or 45-and-over),
sex (female or male), education (non-college-graduate or college-graduate),
race/ethnicity (Black-Non-Hispanic, Hispanic, Asian/Other, and White-Non-Hispanic) and state.
We recognize that these categories are reductive.  In the case of sex
we are limited to categories provided by the CPS data. For age and education
we've chosen to simplify the categories to keep the modeling simple.
For race/ethnicity, we‘re using a slightly richer set of categories,
since turnout varies widely among these groups.

We add a congressional-district-level population-density
factor and interactions between education and a binary race term—a simplification
of the race categories to White-non-Hispanic (WNH) and non-WNH:
a term in the model that estimates the effect of being, e.g.,
White-non-Hispanic (WNH) *and* college-educated over and above the
effects of being in either category separately. Crucially,
we also include an interaction between state and VOC/WNHV,
a term which estimates the *state-dependent* portion of the VOC turnout.

We fit a multi-level model, allowing partial-pooling in the estimate of
the national turnout by race, turnout probability in each state,
and for the interaction between state and race.
A more complex model might:

- Expand the categories: e.g., including religion or marital status.
- Allow partial pooling for more of the categories.
- Add more interactions between categories, for example modeling the effect of race and gender taken together.
(We ran our model with more interactions and found that, other than the ones we already had,
they added little information to the model.)

Because we are interested in local organizing and state-level voter suppression,
we focus on the state-specific portion of VOC turnout, in particular how much VOC turnout
in each state differs from what we would expect based on the demographics
(age, sex, education, local population density) of those voters.
So we post-stratify on VOC in each state, with and without state/race interactions.
We don't yet have ACS data for 2020,
so for now we are using 2018 ACS data in our post-stratifications.

## Two initial questions (and answers) about voter turnout and race

Now we’re in a position to answer two questions about each state’s VOC turnout:

- *How much worse (or better) was each state’s turnout gap
  in 2016 compared with what one would have expected from demographics alone?*
- *Which states have significant (positive or negative) state-specific effects?*

\(1) Observed vs. expected turnout gaps in 2020 by state:
Below we look *only* at the “State-Specific Turnout gap”, taking
the difference of the estimated turnout gap in a state and
the estimated demographic turnout gap.
CO is at the top with 7 point better-than-expected
turnout gap and WA is at the bottom, with 16 point
worse-than-expected turnout gap.

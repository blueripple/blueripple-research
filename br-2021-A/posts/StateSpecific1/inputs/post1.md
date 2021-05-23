# State-Level Voter Turnout And Race

### Key Points In This Post

- Voters-of-color--all but White-Non-Hispanic (WNH)--often vote at lower rates
than
- We model 2020 general election turnout,
concluding that the state-to-state variations in the
turnout of voters-of-color (VOC) is unlikely to be
explained by demographics alone.
- State-specific VOC turnout was high in CO, AL, PA, GA and MI and low in
WA, IL, MA, NH and FL.
- The state-specific component of VOC turnout is usually smaller than
the demographic component.  Neverthelesss, it's important: elections
can be very close and because it reflects what is happening at the state level.


## Focusing on State-Specific Turnout Effects

As we've written about [before](https://blueripple.github.io/research/mrp-model/p3/main.html),
voter turnout varies widely among demographic groups.
Younger voters and voters of color
tend to vote for Democrats, but smaller fractions of those voters cast a vote in most elections.
Similarly, White voters without a college degree, who
are more likely to vote for Republicans, are less likely to vote than White voters
with a college degree.

Unsurprisingly, group-level turnout varies by state:
each state has a different mix of eligible voters, with different
distributions of age and education, etc.

But group-level turnout varies widely by state
*even once we account for the demographic differences among them*,
which we are calling the "state-specific" effect and explore in detail below.
Some possible reasons include:

- Specific state policies which encourage or discourage voting by regulating early
  voting, vote-by-mail, poll hours, ID requirements and other aspects of voter access.
- Grassroots organizing and campaigning, both largely local,
  boosts turnout in demographically specific ways.
- State and local elections have their own dynamics, energizing certain groups of voters.

There’s been a great deal of focus on understanding how much voter
organizing and suppression affected turnout in 2020,
particularly in GA, where Dems prevailed, and FL and TX, where we did not.
In order to answer that question,
we first need a baseline view of state-level turnout across demographic groups.
Our starting point in this post is to focus on the turnout difference
between White-non-Hispanic (WNH) voters and VOC,
using data from the Current Population Survey Voter and Registration Supplement (CPS-VRS)
and the American Community Survey (ACS), both produced by the US Census bureau. We're also going to
explain a bit about [MRP modeling](https://www.youtube.com/watch?v=bq9c1zsR9NM>),
anchoring this and further analyses based on similar models.

Three quick caveats before we dig in.

1. We acknowledge that considering only the WNH/VOC gap is simplistic and reductive,
because it ignores many other non-racial factors that affect turnout.
But we are particularly interested in the interplay of voter suppression and organizing,
where race is a big factor.
2. It is our firm belief
that voting should be easy for everyone,
regardless of demographics or partisan identity,
and all measures to suppress voting are wrong.
3. Any conclusions we can make linking turnout to state voting policies
will be suggestive at best, because these analyses are
[notoriously difficult](<https://scholar.princeton.edu/sites/default/files/jmummolo/files/jop_voterid_print.pdf>).
Standard surveys don’t collect the right type or amount of data to track within-state
differences over time among subsets of voters with enough consistency
to do anything more than generate interesting hypotheses.

With those points out of the way,
let’s dive into the analysis.
We’re going to start with two detailed sections about the underlying data and approach,
describe our model, and then pose and answer three initial questions about voter turnout and race

### Post Roadmap

- Intro (1): Our Data—The CPS-VRS and ACS
- Intro (2): MRP—A Quick Primer
- Our Basic Model
- Four initial questions (and answers) about voter turnout and race.
- Conclusions

## Intro (1): Our Data–The American Community Survey and the CPS Voting and Registration Supplement

Each year, the U.S. Census Bureau (USCB) conducts the
[“American Community Survey” ](https://www.census.gov/programs-surveys/acs)
which, for our purposes,
is an update to the decennial census.  Surveys are sent to ~3.5 million
households, so it’s not a complete count.  There is also less geographic specificity
to the reported results than the decennial census.
The decennial census reports results down to the “block” level, whereas
much of the ACS data is available only at the "Public Use Microdata Area" (PUMA) level—
—each PUMA has about 100,000 people.  Still, this is enough granularity for work
at the state or congressional-district level.
We use ACS data rather than the 2010 decennial census because it provides a more up-to-date
picture of the demographics. (We’ll re-run this analysis once we have full 2020 census results.)

In addition, each election year,
the USCB produces the
[Voting and Registration Supplement](https://www.census.gov/topics/public-sector/voting.html)
to the Current Population Survey (CPS-VRS),
by asking approximately 100,000 people nationwide
for their registration status and if they voted in the general election.
Responses are paired with county of residence, demographic information
(age, sex, race, ethnicity, education, etc.),
allowing estimation of voter turnout among various groups and in various places.

We make one important tweak to the CPS-VRS data:
The survey responses are “self-reported” and not independently validated,
so there are
[reporting errors](http://www.electproject.org/home/voter-turnout/cps-methodology)
that tend to overestimate turnout in a way which differs systematically
among states. To account for this, we adjust the turnout probabilities from the CPS
so that when they are post-stratified across the voting eligible population (VEP)
of each state, we get the correct total turnout.  This was first suggested by
[Achen and Hur](https://www.aramhur.com/uploads/6/0/1/8/60187785/2013._poq_coding_cps.pdf)
and we follow the procedure outlined by
[Ghitza and Gelman](http://www.stat.columbia.edu/~gelman/research/published/misterp.pdf)
(p. 769), to compute the adjustment for each state/year, using the vote totals from
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
50,000 people surveyed each year, with about 40,000 validated voters. For the sake of a
first analysis, we will start with the CPS data.

The 100,000 people surveyed by the CPS-VRS are distributed throughout the country, so there
will only be a limited number of people in each state, particularly less populous ones.
Once you start breaking those people down by demographic groups, the number of people
per group gets quite small.  For example, our model has binary groupings for age, sex and
education and a 4-category grouping for race. Considering
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
to the best estimation.

Once we have estimates for every group in every state, we turn them into
turnout numbers via post-stratification: multiplying
the estimated probabilities by the actual number of people in each group,
and adding these up to figure out how many people are likely to vote.

The Monte-Carlo modeling produces confidence intervals of the parameters,
*and* the post-stratifications that use them.
The fact that some groups are very small, making probabilistic inference difficult,
will show up in our results as large confidence intervals.
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
of the race categories to White-non-Hispanic (WNH) and non-White-non-Hispanic (NWNH):
a term in the model that estimates the effect of being, e.g.,
White-non-Hispanic (WNH) *and* college-educated over and above the
effects of being in either category separately. Crucially,
we also include an interaction between state and WNH/VOC,
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


It’s also interesting to look at turnout "gaps": differences in the turnout rate
between VOC and WNH voters.  Rather than showing differences among states,
these might highlight how policies and organizing within a state affect the
VOC population and WNH population differently. To save space here,
we look at the gaps in a [separate note][gapNote_link].

## Four initial questions (and answers) about voter turnout and race

Now we’re in a position to answer four questions about each state’s VOC turnout:

- *How much worse (or better) was each state’s VOC turnout in 2016 compared with what one would have expected from demographics alone?*
- *Which states have significant (positive or negative) state-specific effects?*
- *Is the state-specific factor a reflection of election integrity?*
- *In those states, how do the demographic and state-specific components compare in size?*

Let's see if the  data and model can shed some light!

\(1) Observed vs. expected VOC turnout in 2020 by state:
Below we look *only* at the “State-Specific VOC Turnout”, taking
the difference of the estimated VOC turnout in a state and
the estimated VOC turnout *based on the demographics alone*.
CO is at the top with 4 point better-than-expected
turnout of VOC and WA is at the bottom, with 8 point
worse-than-expected turnout of VOC.

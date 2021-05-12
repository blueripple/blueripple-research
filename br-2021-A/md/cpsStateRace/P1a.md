# Voter Turnout And Race

As we've written about [before](https://blueripple.github.io/research/mrp-model/p3/main.html)
voter turnout varies widely among demographic groups.
Younger voters and voters of color
tend to vote for Democrats, but smaller fractions of those voters cast a vote in most elections.
Similarly, White voters without a college degree, who
are more likely to vote for Republicans, are less likely to vote than White voters
with a college degree.

Unsurprisingly, these turnout differences
also vary by state, because each state has a different mix of voters, with different
distributions of age and education, etc.

But turnout gaps vary widely by state
*even once we account for the demographic differences among them*,
something we explore in detail below.
Some possible reasons include:

- Specific state policies which encourage or discourage voting by regulating early
  voting, vote-by-mail, poll hours, ID requirements and other aspects of voter access.
- Grassroots organizing and campaigning, both largely local,
  may also drive turnout and shrink turnout gaps.
- Specific national and local elections have their own dynamics, energizing certain groups of voters.

There’s been a great deal of focus on understanding how much voter
organizing and suppression affected turnout in 2020,
particularly in GA, where Dems prevailed, and FL and TX,
where we did not.
But in order to answer that question,
we first need a baseline view of state-level turnout gaps across demographic groups.
Our starting point in this post is to focus on turnout differences
between White-non-Hispanic (WNH) voters and everyone else
(Non-White-Non-Hispanic voters, NWNH),
using data from the Current Population Survey Voter and Registration Supplement (CPS-VRS)
and the American Community Survey (ACS), both produced by the US Census bureau. We're also going to
explain a bit about [MRP modeling](https://www.youtube.com/watch?v=bq9c1zsR9NM>),
anchoring this and further analyses based on similar models.

Three quick caveats before we dig in.
First, we acknowledge that considering only the WNH/NWNH gap is simplistic and reductive,
because it ignores many other non-race factors that affect turnout.
But we are particularly interested in the interplay of voter suppression and organizing,
where race is a big factor, and this simplification also helps us to the analysis.
Second, this analysis should in no way take away from our firm belief
that voting should be easy for everyone,
regardless of demographics or partisan identity,
and all measures to suppress voting are wrong.
And third, any conclusions we can make linking turnout to state voting policies
will be suggestive at best, because these analyses are
[notoriously difficult](<https://scholar.princeton.edu/sites/default/files/jmummolo/files/jop_voterid_print.pdf>).
Standard surveys don’t collect the right type or amount of data to track within-state
differences over time among subsets of voters with enough consistency
to do anything more than generate interesting hypotheses.

With those points out of the way,
let’s dive into the analysis.
We’re going to start with two detailed sections about the underlying data and approach,
describe our model, and then pose and answer three initial questions about voter turnout and race

### In This Post

- Intro (1): Our Data—The CPS-VRS and ACS
- Intro (2): MRP—A Quick Primer
- Our Basic Model
- Three initial questions (and answers) about voter turnout and race.
- Conclusions

## Intro (1): Our Data–The American Community Survey and the CPS Voting and Registration Supplement

Each year, the U.S. Census Bureau (USCB) conducts the “American Community Survey” which, for our purposes,
is an update to the decennial census.  Surveys are sent to ~3.5 million
households, so it’s not a complete count.  There is also less geographic specificity
to the reported results than the decennial census.
The census reports results down to the “block” level, whereas
much of the ACS data is available only at the PUMA (Public Use Microdata Area) level—
—each PUMA has about 100,000 people.  Still, this is enough granularity for work
at the state or congressional-district level.  For precise demographics at the state-legislative
or voting precint level, we need to use either decennial census results or the subset of
the ACS which is reported at the census tract level.
We use ACS data rather than the 2010 decennial census because it provides a more up-to-date
picture of the demographics. (We’ll re-run this analysis once we have full 2020 census results.)

In addition, each election year,
the USCB publishes the
[Voting and Registration Supplement](https://www.census.gov/topics/public-sector/voting.html)
to the Current Population Survey (CPS-VRS),
which asks approximately 100,000 people nationwide
about their registration status and if they voted in the general election.
Responses are paired with county of residence, demographic information
(age, sex, race, ethnicity, education, etc.),
allowing estimation of voter turnout among various groups and in various places.

We make two important tweaks to the COS-VRS data.
First, the survey responses are “self-reported” and not independently validated,
so there are
[reporting errors](http://www.electproject.org/home/voter-turnout/cps-methodology)
that tend to overestimate turnout in a way which differs systematically
among states. To account for this, we adjust the turnout probabilities from the CPS
so that when they are post-stratified across the citizen population of each state, we get
the correct total turnout.  This was first suggested by
[Achen and Hur](https://www.aramhur.com/uploads/6/0/1/8/60187785/2013._poq_coding_cps.pdf)
and we follow the procedure outlined by
[Ghitza and Gelman](http://www.stat.columbia.edu/~gelman/research/published/misterp.pdf)
(p. 769), to compute the adjustment for each state/year, using the vote totals from
[United States Election Project](http://www.electproject.org/home/voter-turnout/voter-turnout-data)


Second, CPS-VRS data
[seems to under-report](https://static1.squarespace.com/static/5fac72852ca67743c720d6a1/t/5ff8a986c87fc6090567c6d0/1610131850413/CPS_AFS_2021.pdf)
the gaps between White and non-White voters.  So all of our results looking at turnout gaps
should be viewed skeptically: the gaps are likely larger than we see in the data.
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
each of the 50 states plus DC, we have $32 \times 51 = 1632$ groups.  If people were
distributed evenly among those groups, we might have 60 or so people in each. But people
are not distributed equally among those groups! Some states are smaller and some may not have
very many people in some of those categories.  So how can we hope to understand any state-level
subtleties in turnout?

That's where MRP comes in.

## Intro(2): MRP—A Quick Primer

Though there might not be many people in any one Age/Sex/Education/Race/State group, each person
surveyed has many things in common with many people in other groups.  The **MR** part of **MRP** stands
for **M**\ ulti-level **R**\ egression,
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
we also include an interaction between state and WNH/NWNH,
a term which estimates the *state-dependent* portion of the NWNH turnout.

We fit a multi-level model, allowing partial-pooling in the estimate of
the overall turnout probability in each state and for the interaction between state and race.
A more complex model might expand the categories,
allow partial pooling for more of the categories,
or add more interactions between categories.

Because we are interested in local organizing and state-level voter suppression,
we focus on the state-specific portion of NWNH turnout, in particular how much NWNH turnout
in each state differs from what we would expect based on the demographics
(age, sex, education, local population density) of those voters. So we post stratify on NWNH
voters in each state, with and without state/race interactions.

It’s also interesting to look at turnout "gaps": differences in the turnout rate
between NWNH voters and WNH voters.  Rather than showing differences among states,
these might highlight how policies and organizing within a state affect the
NWNH population and WNH population differently. To save space here,
we look at the gaps in a [separate note][note_link].

## Three initial questions (and answers) about voter turnout and race

Now we’re positioned to be able to answer three questions about each state’s NWNH turnout:

1. How much worse (or better) was each state’s NWNH turnout in 2016 compared with what one would have expected?
2. How did each state’s NWNH turnout change from 2012 to 2016?

(1) Observed vs. expected NWNH turnout in 2016 by state:
Below we look *only* at the “State NWNH Turnout Effect”, taking
the difference of the estimated NWNH turnout in a state and subtracting
the estimated NWNH turnout *based on the demographics alone*.
PA is at the top with 4 point better-than-expected
turnout of NWNH voters and AZ is at the bottom, with 5 point
worse-than-expected turnout of NWNH voters.

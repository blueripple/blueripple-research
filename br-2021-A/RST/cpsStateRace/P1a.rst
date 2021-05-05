Voter Turnout And Race
++++++++++++++++++++++

As we've written about `before <https://blueripple.github.io/research/mrp-model/p3/main.html>`_,
younger voters and voters of color
tend to vote for Democrats, but smaller fractions of those voters cast a vote in most elections.
White voters without a college degree
are more likely to vote for Republicans, and are similarly less likely to vote than White voters
with a college degree. These turnout differences
vary by state.  In a sense, that’s no surprise: each state has a different mix of voters, with different
distributions of age and education, etc.  But turnout gaps vary widely by state
*even once we account for the demographic differences among them*, something we explore in detail below.
Some possible reasons incude:

- Various state policies which encourage or discourage voting: more days of early
  voting, easier vote-by-mail, having more polling places, or keeping polls open for more hours makes voting easier and may shrink
  turnout gaps.  ID requirements, voter purges and unduly strict requirements around provisional ballots can
  widen these gaps.
- Grassroots organizing and campaigning, both largely local,
  may also drive turnout and shrink turnout gaps.
- Specific national and local elections have their own dynamics, energizing certain groups of voters.

Democratic successes in AL in 2016 (the election of Doug Jones to the Senate),
and GA in 2020, as well as 2020 Dem struggles in FL and TX,
echo important themes about the role of organizing and voter
suppression in the turnout of Democratic voters.  Before we dig into that story—something we plan to do
later this year—we set the stage, exploring the variations in state-level turnout gaps
between White-non-Hispanic (WNH) voters and everyone else (Non-White-Non-Hispanic voters, NWNH),
using data from the Current Population Survey Voter and Registration Supplement (CPS-VRS)
and the American Community Survey (ACS), both produced by the US Census bureau. We're also going to
explain a bit about `MRP modeling <https://www.youtube.com/watch?v=bq9c1zsR9NM>`_,
anchoring this and further analyses based on similar models.

Considering only the WNH/NWNH gap is simplistic and reductive.  There are turnout gaps
by level of education and age, by population density and income, by religion, family
structure and occupation.  But we are particularly interested in the interplay of
voter suppression and organizing, and a lot of this plays out on the field
of race. In order to look state by state, which complicates the modeling,
we must simplify as much else as we can.

Any attempt to analyze voter turnout and connect it to government policy or
grasroots organizing is fraught,
running the risk of minimizing the harms done by suppressive policies or
asserting that organizing is some sort of solution to voter suppression.
We at Blue Ripple Politics are unequivocally
in support of nearly all measures to make voting easier:
no-excuse voting by mail,
same day registration,
more polling places,
more days of early voting,
longer hours at polling places,
the elimination of voter ID requirements,
an end to the aggressive purging of voter lists,
support and allowance of provisional ballots,
re-enfranchisement of people in
prison and the formerly incarcerated, etc.
And regardless of whether suppression is succesful, it is wrong.  It is morally repugnant and
it requires energy and resources to combat.  Forcing people to spend those energies and resources
is itself an outrage. Voting should be easy for everyone and we want everyone to vote!

Trying to measure the effect of suppressive
voting policy (requiring voter ID, shrinking the available polling places/hours, etc.)
`is difficult. <https://scholar.princeton.edu/sites/default/files/jmummolo/files/jop_voterid_print.pdf>`_
It requires measuring within-state differences over-time among subsets of voters.  The
standard surveys used to look at these effects do not have sufficient data within groups or enough
consistency over time for these analyses. The inspiring and remarkable successes of
organizing in the face of these restrictions further complicates any simple measuring of their effects.

In This Post
____________

- Our Data: The CPS-VRS and ACS
- MRP: A Quick Primer
- The Basic Model
- State-Level effects
- Conclusions

*Data: The American Community Survey and the CPS Voting and Registration Supplement*
____________________________________________________________________________________

Each year, the U.S. Census Bureau (USCB) conducts the “American Community Survey” which, for our purposes,
is an update to the decennial census.  Surveys are sent to ~3.5 million
households, so it’s not as complete a count.  There is also less geographic specificity
to the reported results.  The census reports results down to the “block” level, whereas
much of the ACS data is available only at the PUMA (Public Use Microdata Area) level—
—each PUMA has about 100,000 people.  Still, this is enough granularity for work
at the state or congressional-district level.  For precise demographics at the state-legislative
or voting precint level, we need to use either decennial census results or the subset of
the ACS which is reported at the census tract level.
We use ACS data rather than the 2010 decennial census because it provides a more up-to-date
picture of the demographics.  When the 2020 census
results are available we will have an extremely precise and current set of demographic data.

and each election year, via the Current Population Survey,
the USCB publishes the
`Voting and Registration Supplement <https://www.census.gov/topics/public-sector/voting.html>`_,
the result of asking approximately 100,000 people nationwide
about their registration status and if they voted in the general election.
In addition to county of residence, demographic information
(age, sex, race, ethnicity, education, etc.) is paired with the responses,
allowing estimation of voter turnout among various groups and in various places.

The CPS-VRS responses are “self-reported”: the survey does not validate the registration
or turnout of the people
surveyed, and thus there are
`reporting errors <http://www.electproject.org/home/voter-turnout/cps-methodology>`_,
ones which tend to overestimate turnout in a way which differs systematically
among states. To account for this, we adjust the turnout probabilities from the CPS
so that when they are post-stratified across the citizen population of each state, we get
the correct total turnout.  This was first suggested by
`Achen and Hur <https://www.aramhur.com/uploads/6/0/1/8/60187785/2013._poq_coding_cps.pdf>`_
and we follow the procedure outlined by
`Ghitza and Gelman <http://www.stat.columbia.edu/~gelman/research/published/misterp.pdf>`_
(p. 769), to compute the adjustment for each state/year, using the vote totals from
`United States Election Project <http://www.electproject.org/home/voter-turnout/voter-turnout-data>`_.


Also, CPS-VRS data
`seems to under-report
<https://static1.squarespace.com/static/5fac72852ca67743c720d6a1/t/5ff8a986c87fc6090567c6d0/1610131850413/CPS_AFS_2021.pdf>`_
the gaps between White and non-White voters.  So all of our results looking at turnout gaps
should be viewed skeptically: the gaps are likely larger than we see in the data.
There are other publically available
surveys which, when possible, validate survey reponses via state voter files,
primarily the
`CCES <https://cces.gov.harvard.edu>`_.  That survey is smaller: approximately
50,000 people surveyed each year, with about 40,000 validated voters. For the sake of a
first analysis, we will start with the CPS data.

The 100,000 people surveyed by the CPS-VRS are distributed throughout the country, so there
will only be a limited number of people in each state, particularly less populous ones.
Once you start breaking those people down by demographic groups, the number of people
per group gets quite small.  For example, our model has binary groupings for age, sex and
education and a 4-category grouping for race. Considering
each of the 50 states plus DC, we have :math:`32 \times 51 = 1632` groups.  If people were
distributed evenly among those groups, we might have 60 or so people in each. But people
are not distributed equally among those groups! Some states are smaller and some may not have
very many people in some of those categories.  So how can we hope to understand any state-level
subtleties in turnout?

That's where MRP comes in.

*MRP: A Quick Primer*
_____________________

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
(namely, `Hamiltonian Monte Carlo <https://en.wikipedia.org/wiki/Hamiltonian_Monte_Carlo>`_
via `Stan <https://mc-stan.org/about/>`_)
allow the data itself to determine how much partial-pooling leads
to the best estimation.

Once we have estimates for every group in every state, we turn them into
turnout numbers via post-stratification: multiplying
the estimated probabilities by the actual number of people in each group,
and adding these up to figure out how many people are likely to vote.

The Monte-Carlo modeling produces confidence intervals of the parameters,
*and* the post-stratifications which use them.
The fact that some groups are very small, making probabilistic inference difficult,
will show up in our results as large confidence intervals.
Partial-pooling helps, but only so much.

*The Basic Model*
_________________

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
a term which estimates the *state-dependent* portion of the turnout gap.

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

It’s also interesting to look at turnout gaps between NWNH voters and WNH voters.
Clicking the link below will bring you some more information about those.
click the link below for more detail about the demographics-only gap and the total gap.

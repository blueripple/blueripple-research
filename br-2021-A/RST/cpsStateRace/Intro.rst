Voter Turnout And Race
++++++++++++++++++++++


As we've written about `before <https://blueripple.github.io/research/mrp-model/p3/main.html>`_,
younger voters and voters of color
tend to vote for Democrats, but smaller fractions of those voters cast a vote in most elections.
White voters without a college degree
are more likely to vote for Republicans, and are similarly less likely to vote than White voters
with a college degree. These turnout differences
vary by state, though it’s not clear why.  Some possibilities:

- Each state has different policies which encourage or discourage voting: more days of early
  voting, having more polling places, or keeping polls open for more hours makes voting easier and may shrink
  the turnout gap.
- Grassroots organizing and campaigning, both largely local,
  may also drive turnout and shrink turnout gaps.
- Specific national and local elections have their own dynamics, energizing certain groups of voters.

The Democratic successes in AL in 2016 (the election of Doug Jones),
and GA in 2020, as well as 2020 Dem struggles in FL and TX,
echo important themes about the role of organizing and voter
suppression in the turnout of Democratic voters.  Before we dig into that story—something we plan to do
later this year—we set the stage, exploring the variations in state-level turnout gaps, using data
from the Current Population Survey (CPS) Voter Supplement and the
American Community Survey (ACS), both produced by the US Census bureau. We're also going to
explain a bit about `MRP modeling <https://www.youtube.com/watch?v=bq9c1zsR9NM>`_,
anchoring this and further analyses based on similar models.

Before we dive in, we should note that trying to measure the effect of suppressive
voting policy (requiring voter ID, shrinking the available polling places/hours, etc.) using
`data-science <https://scholar.princeton.edu/sites/default/files/jmummolo/files/jop_voterid_print.pdf>`_
is difficult.  It requires measuring within-state differences over-time among subsets of voters.  The
standard surveys used to look at these effects do not have sufficient data within groups or enough
consistency over time for these analyses. The inspiring and remarkable successes of
organizing in the face of these restrictions further complicates measuring effects.  We want to be clear
that whatever the consequence of these laws and policies on turnout, we at Blue Ripple Politics are against them.
Every state should make it *easier* to vote, particularly via early voting, same-day registration and the
(re-)enfranchisement of the formerly incarcerated and people im jail and prison. We also believe that
even succesfully overcome voter suppression represents wasted time and energy on behalf of the organizers and
voters which should not be minimized just because the electoral outcome is one we favor.

In This Post
____________

- The CPS Voter Supplement
- MRP: A Quick Primer
- The Basic Model
- State-Level effects
- Conclusions

*Turnout Data: The CPS Voter Supplement*
________________________________________

Each election year, the census bureau, via the Current Population Survey,
conducts the `Voter Survey <https://www.census.gov/topics/public-sector/voting.html>`_,
asking approximately 100,000 people nationwide
about their registration status and if they voted in the general election.
In addition to county of residence, demographic information
(age, sex, race, ethnicity, education, etc.) is paired with the responses,
allowing estimation of voter turnout among various groups and in various places.

The CPS responses are “self-reported”: the survey does not validate the registration
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
(p. 769), to compute the adjustment for each state/year.

Also, CPS data
`seems to under-report
<https://static1.squarespace.com/static/5fac72852ca67743c720d6a1/t/5ff8a986c87fc6090567c6d0/1610131850413/CPS_AFS_2021.pdf>`_
the gaps between White and non-White voters.  So all of our results looking at turnout gaps
should be viewed skeptically: the gaps are likely larger than we see in the data.
There are other publically available
surveys which do validate these reponses, primarily the
`CCES <https://cces.gov.harvard.edu>`_.  That survey is smaller: approximately
50,000 people surveyed each year, with about 40,000 validated voters. For the sake of a
first analysis, we will start with the CPS data.

The 100,000 people surveyed by the CPS are distributed throughout the country, so there
will only be a limited number of people in each state, particularly less populous ones.
Once you start breaking those people down by demographic groups, the number of people
per group gets quite small.  For example, our model has binary groupings for age, sex and
education and a 5-category grouping for race. Considering
each of the 50 states plus DC, we have :math:`40 \times 51 = 2040` groups.  If people were
distributed evenly among those groups, we might have 50 people or so in each. But people
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
entire country, and partially from all voters in MI.  Models can be constructed various ways to allow
different sorts of partial-pooling.  The **MR** technique and tools we use
(namely, `Hamiltonian Monte Carlo <https://en.wikipedia.org/wiki/Hamiltonian_Monte_Carlo>`_
via `Stan <https://mc-stan.org/about/>`_)
allow the data itself to determine how much partial-pooling leads
to the best estimation.

Once we have estimates for every group in every state, we turn them into
turnout numbers via post-stratification: multiplying
the estimated probabilities by the actual number of people in each group,
and adding these up to figure out how many people are likely to vote.

Confidence intervals of the parameters,
*and* post-stratifications which use them,
are produced naturally by the Monte-Carlo modeling.
The fact that some groups are very small, and thus hard to estimate,
will show up in our results as large error bars.  Partial-pooling helps,
but only so much.

*The Basic Model*
_________________

Our basic model includes age (under 45 or 45-and-over),
sex (female or male), education (non-college-graduate or college-graduate),
race/ethnicity (Black, Hispanic, Asian, White-non-Hispanic and other) and state.
We recognize that all these categories are reductive.  In the case of sex
we are limited to categories provided by the CPS data. For age and education
we've chosen to simplify the categories to keep the modeling simple.
For race/ethnicity, we‘re using a slightly richer set of categories,
since turnout varies widely among these groups.

We add a congressional-district-level population-density
factor and interactions between education and a binary race term—a simplification
of the race categories to White-non-Hispanic (WNH) and non-White-non-Hispanic (NWNH):
a term in the model that estimates the effect of being, e.g.,
White-non-Hispanic (WNH) *and* college-educated over and above the
effects of being in either category separately. Finally,
we include an interaction between state and WNH/NWNH,
a term which estimates the *state-dependent* portion of the turnout gap.

We fit a multi-level model, allowing partial-pooling in the estimate of
the overall turnout probability in each state and for the interaction between state and race.
A more complex model might expand the categories,
allow partial pooling for more of the categories,
or add more interactions between categories.

For the purposes of this post, we are interested specifically in the turnout difference
between White-Non-Hispanic voters, who lean R, and everyone else, who lean D.  Separately
Post-stratifying across the WNH and the NWNH populations
produces modeled turnout rates for each and taking the difference produces the turnout gap.

The turnout gap in each state is partly due to the specific mix of people in that state,
their mix of ages, sexes, etc. and partly due to things specific to the state itself, such
as history, organizing and voter suppression.
As an example, in the chart below, we look at the 2016 state turnout gaps
(along with 90% percent confidence intervals) predicted
by our model, first without the state-race interaction.
These gaps come from the *national* turnout gap between WNH and NWNH voters and the
differences among states come entirely from different distributions of ages,
gender and education among the WNH and NWNH populations in that state.  These gaps
average about 9 pts, ranging from a bit more than 4 points in TN to 15 points in HI.
The average gap can be quite different year-to-year and it was, for example, close to 0 in 2012.
In each of the following charts, the zero-line is marked in blue and the mean of the
turnout gaps in orange.

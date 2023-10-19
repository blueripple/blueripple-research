### Modeling State Legislative Elections (updated October 19, 2023)
There are a number of reasons to analyze elections in state-legislative districts (SLDs).
Our primary goal is to help donors decide where to give money. The amount of money
people have to give is limited and we hope they will focus that giving to long-term
development of organizational infrastructure or on specific close-but-winnable (CBW) races.
Our work focuses on the latter sort of giving, finding winnable races.

We take as given that local expertise in the particular district or set of districts is likely
to be the best way to figure out which districts are CBW. But there are more than 5000 SLDs!
Also, Local expertise is hard to find and requires vetting. So we think it’s useful to have ways of
understanding SLDs that we can apply to the country as a whole, perhaps narrowing the
set of districts where on-the-ground expertise is important.

The most straightforward way to select CBW races is to look at what happened in
a district in previous elections, eitehr for the same office or others.
That’s straightforward but not at all easy. [Dave’s Redistricting](??)
does a spectacular job of joining district maps and precinct-level data from previous
elections to create an estimate of the historical partisan lean (HPL) of every SLD
in the country. Their rich interface allows the user to choose various
previous elections and combinations of them to estimate the partisan lean.

We think this is absolutely the right starting place when figuring out where to donate. Any SLD
which has an HPL that is very Democratic or very Republican is likely not a race the other
party can win in this cycle.

Nevertheless, we think there are some useful questions this approach leaves unanswered. Why does
each district have the HPL it does? Are there districts with HPLs that are surprising given
their location and demographic makeup? We think this sort of analysis provides opportunities to
usefully adjust a list of CBWs or the allocation among them. For example, imagine an SLD with HPL
1 point below your cutoff for winnable by the Dem. But a demographic analysis shows that it’s
demographic partisan lean (DPL, something we’ll explain below) is over 50%. That might be a place
where a good and well-resourced candidate can win a race or in losing a close-race, change the
narrative going forward. It might be equally useful to be alerted to a district which looks
easily winnable by HPL standards but with DPL much closer to 50%. That district might be vulnerable
to the right opposition candidate.

## Demographic Partisan Lean
Rather than consider how people in a specific place have voted in previous elections,
Demographic Partisan Lean (DPL) instead categorizes people demographically, in our case
by state, population-density, age, sex, educational-attainment and race/ethnicity.
Using large surveys of turnout and party-preference
we model expected turnout and party-preference for each of those categories.
Then, given the numbers of people in each of those categories in a district, compute
the (modeled) partisan-lean among expected-voters.

Just like HPL, this involves some choices about which data to use. Which election(s) should we use
as sources for voter turnout and party preference? Even for a given election, what sources of
data should we use?

DPL has data issues of its own, e.g., how do we know how many of each category of people
live in the district? And DPL requires a large number of modeling choices, including which
categories to include in the first place, how to break them down into buckets for modeling,
what sort of model to use, how to handle uncertainty in that model, etc.  We’ll discuss all this
in much more detail below as we describe our specific modeling process.

One way to think of DPL is as a more detailed version of what people are doing when they look at
voting patterns based on race or education or age. Each of those can be a valuable predictor
of turnout and party-preference. But sometimes the combination of categories is essential
for understanding. For example, the Republican lean of white-non-college educated voters
is greater than you expect from the Republican lean of white voters and the Republican lean
of non-college-educated voters combined. This gets even more complex and harder to keep track of
once you add age and sex. Further, these things vary from state to state. Population density
affects party-preference quite strongly in ways not captured by any of those categories.
All of these things vary differently for turnout and party-preference.

You can think of DPL as the partisan lean of a district if,
within each demographic group, each person acted more or less the same as
everyone else in the state living at similar population density
in terms of turning out to vote and who they choose to vote for. This is clearly less
predictive than knowing who those same people voted for in the previous few elections. But
it’s also different information, and particularly interesting when it’s inconsistent with
the HPL.


## DPL: our model
We’re going to explain what we do in steps, expanding on each one further down in the document
and putting some of the mathematical details in other, linked documents. Our purpose here
is to give you a solid idea of what we do but without too much detail.

We begin with the Cooperative Election Study ([CES](https://cces.gov.harvard.edu)),
a highly-regarded survey which
runs every 2 years, speaks to people before and after the general election and attempts to
validate people’s responses about turnout via a third-party using voter-file data. The
CES survey includes approximately 60,000 people per election cycle. It gathers demographic and
geographic information about each person as well as information about voting, party preference
as well as various other opinion and issue questions.

The CES data includes state and congressional district of each person interviewed. We use that
to join the data to population-density data from the 5-year American Community
Survey ([ACS](https://www.census.gov/programs-surveys/acs/microdata.html)) data, using
the sample ending the same year as the CES survey was done.

We then build and fit multi-level regressions of that data, one for
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
which is more appropriate for thinking about 2024. 2022 is clearly more recent
but 2020 is the most recent *presidential* election year.
Using the 2020 survey, we choose presidential vote as our
party-preference indicator. If we use 2022, we will
switch to house-candidate vote and include incumbency in the regression model
for party-preference. We’re thinking about ways to combine these but it’s not
straightforward for a variety of reasons. We could also just model using both and
have two DPL numbers to look at per district.

ACS is the best available public data for figuring out the demographic breakdowns
of who lives in a district. But that data is somewhat limited at the scale of a
state-legislative district. We get ACS population tables at the census-tract level and aggregate
them to the district level. Then we use our own statistical methods^[nsm] to
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
to theirs but we imagine those methods are capable of being quite accurate.

# Modeling the Turnout and Party-Preference
The CES survey provides data for each person interviewed. The demographic data is provided,
along with a weight, designed so that subgroups by age, race, etc. are correctly represented once
weights are accounted for. For example, if you know that there are equal numbers of men and women
(CES has just begun tracking gender categories other than male or female but the ACS does not)
in a congressional district but your survey has twice as many women as men, you would adjust the
weights so that those interviews have equal representation in a weighted sum. Because our demographic
categories are more coarse-grained than what the CES provides (e.g., they give age in years but we want
to model with 5 age groups) we need to aggregate the data. We use the weights when aggregating and this
means that in the aggregated data we have non-whole numbers of voters and voters preferring one party or the other.

We imagine each person in a demographic category and state has a
specific fixed probablilty of voting. This would lead to a binomial model of vote
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
education and race and state and race. For the state factor and all the combination factors,
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

There’s an important last step. When we post-stratify these modeled probabilities
across an entire state, that gives the expected number of votes in that state. But
we know the number of votes recorded in the state and our number won’t usually match
exactly. So we adjust each probability using a technique pioneered by
[Hur & Achen](https://www.aramhur.com/uploads/6/0/1/8/60187785/2013._poq_coding_cps.pdf),
and explained in more detail on pages 9-10 of
[this paper](http://www.stat.columbia.edu/~gelman/research/published/mrp_voterfile_20181030.pdf).
We can apply the same adjustment to our confidence intervals giving us an approximate
confidence interval for the adjusted parameter or post-stratification result.

We do this for turnout alone, party-preference of voters, where we match to known
vote totals for the candidate of each party, and both together which we use just to
estimate confidence intervals pre-adjustment.

The final result of all this work is an estimate of the DPL for any SDL in the country.

As an example, let’s look at the HPL and DPL for Virginia’s house elections in 2023:







We want to build a reasonable but simple demographic model of
voter turnout and preference which we
can use to estimate the outcome of an election
in a state legislative district.  We want to build something
we can apply fairly easily to any state.
And, since this is a redistricting year,
we want something that we can apply to newly drawn
districts as well as existing ones.

One possibility is that we work bottom-up, using voting precincts as building blocks:

1. Get the geographic boundary and election results for each precinct,
e.g., from [openprecincts](https://openprecincts.org).
2. Build a demographic profile of the precinct using overlaps[^arealInterpolation]
of census “blocks” or “block
groups”[^censusGeographies].
3. Use the total number of votes and votes cast for the Democratic candidate
in each precinct to *infer* a demographic model of turnout and voter preference in the
entire set of precincts.
4. Apply that model to the demographics of a SLD to generate a rough
estimate of the likely election result.

For a given SLD (or set of SLD’s), what precincts do we include in the model?
In order to keep
things simple we want a model that covers multiple districts. We
could model using every precinct in the country or at least the state.
Using every precinct in the country is hard: some of that data is unavailable
and there are a lot of precincts (about
[175,000](https://arxiv.org/ftp/arxiv/papers/1410/1410.8868.pdf)
of them)!
Using data only from the state risks being
too influenced by the local election history.

So instead, we work top-down from national-level data:

1. Use a national survey of voter turnout, e.g., the
Current Population Survey Voter Registration Supplement
([CPSVRS](https://www.census.gov/data/datasets/time-series/demo/cps/cps-supp_cps-repwgt/cps-voting.html)),
and/or the
Cooperative Election Survey
([CES](https://cces.gov.harvard.edu),
formerly the CCES) and a national survey
of voter preference, like the CES, to build demographically stratified turnout
and preference data at the state or congressional-district (CD) level.
2. Use that data to infer a demographic model of turnout and voter preference,
possibly with state or CD-level effects.
3. Apply[^postStratify] that model to the demographics of a SLD to generate
a rough estimate of the likely election result.

This approach might be too general: voters in different regions might not
be well described by a national model. The data we use is organized by CD,
so we can add some geographic specificity to the model. Even better, we can
compare the quality of models with different levels of geographic detail.

This data is considerably simpler
to work with and more comprehensive precinct-level data is not available for all states
and all election years.  But we remain interested in the bottom-up approach as well,
and we might implement some version of it for comparison.

Some details:

1. For turnout, we have the CPSVRS and/or the CES.  The CPSVRS is self-reported whereas the
CES validates the turnout data via voter files.  All other things equal, we’d prefer the validated
data.  But there’s some evidence that the
[validation process used by the CES introduces bias](https://agadjanianpolitics.wordpress.com/2018/02/19/vote-validation-and-possible-underestimates-of-turnout-among-younger-americans/)
because it tends to miss people who move between elections,
and they are disproportionately likely to be young and poor.
Because it the more used source, we use the CPSVRS as a source of turnout data,
though we also run the models using the CES as a turnout source to see
if it makes a large difference.
People tend to over-report their own turnout, which presents a problem for non-validated sources.
So we use some standard adjustments
to the turnout
data, first suggested by
[Hur & Achen](https://www.aramhur.com/uploads/6/0/1/8/60187785/2013._poq_coding_cps.pdf),
which adjusts the turnout probabilities from
CPSVRS so that the actual recorded total turnout matches the CPSVRS post-stratified
on the geography in question. The Hur & Achen paper doesn’t address how to re-weight
among various demographic groups within the same geography.  For this we follow the
procedure outlined (in a slightly different context) on pages 9-10 of
[this paper](http://www.stat.columbia.edu/~gelman/research/published/mrp_voterfile_20181030.pdf)
by Ghitza and Gelman.

2. We choose congressional districts (CD’s) as our basic geographic unit for
constructing the model of turnout and voter preference.
We use CD’s as our modeling units for two reasons:
CD’s are a core interest at BlueRipple politics
and they are the smallest geography easily available in all the required data.
We can get more fine grained census data for demographics but the turnout and
voter preference data that we use (the CPSVRS and CES) is not available at the SLD level.

For each of the 436 districts (435 CDs plus DC) in the U.S., we have:

- The CPSVRS, containing
self-reported voter turnout for about 1% of eligible voters in each district,
broken down by sex (female or male), education (non-college-grad or college-grad)
and race (Black, Latinx, Asian, white-non-Latinx, other).

- The CES, containing validated turnout and voter preference–specifically
the political party of the voter’s choice for congressional representative, broken down by the same
demographic categories.

- Population density, computed by aggregating data from the Census Bureau’s American Community Survey
(ACS) at the Public-Use-Microdata-Area (PUMA) level.

For each State Legislative District (SLD) we have data from the ACS, which we aggregate from
the block-group level using
[areal interpolation](https://medium.com/spatial-data-science/spatial-interpolation-with-python-a60b52f16cbb).
Our shapefiles for the SLDs and the census block-groups come from the Census Bureau.
The result of this aggregation is a breakdown
of the citizens in each district by the same demographic variables as the CPSVRS and CES data, as well as
an estimate of population density in the SLD.

Modeling proceeds as follows:

1. We combine the CPSVRS, CES, and CD-level population density into one data-set, with rows for each combination
   of demographic variables within each CD.  That’s 2 x 2 x 5 x 436 = 8720 rows, each with turnout data from both
   CPSVRS and CES, party preference from the CES and population density from the ACS.

2. We model turnout and vote choice as binomially distributed with probabilities determined by the various
   demographic parameters.  That is, given a district,
   $d$, with population density $\rho$, and the subset of people with sex $s$, education-level $e$ and race $r$,
   we assume that the turnout ($T$) of eligible voters ($E$)
   and the votes for the Democratic candidate ($D$),
   out of all validated voters ($V$), both follow binomial distributions:

    $T^{(d)}_{ser}(\rho) \thicksim B\big(E^{(d)}_{ser}(\rho);t^{(d)}_{ser}(\rho)\big)$

    $D^{(d)}_{ser}(\rho) \thicksim B\big(V^{(d)}_{ser}(\rho);p^{(d)}_{ser}(\rho)\big)$

    where $B(n;p)$ is the distribution of successful outcomes from $n$-trials with
    fixed probability of success $p$.
    NB: If we were using the same data for turnout and preference, $T^{(d)}_{ser}(\rho)$
    would be the same as $V^{(d)}_{ser}(\rho)$,
    but since we are using different data sets, we need to model them separately.

    The probabilities $t$ and $p$ must be between 0 and 1 and are modeled via
    [logistic functions](https://en.wikipedia.org/wiki/Logistic_function)
    of the parameters so that the parameters themselves are unconstrained.
    This allows the fitting to proceed more easily.
    The logistic then maps the unconstrained
    sum of the parameters to a probability.

    $\begin{equation}
    t^{(d)}_{ser}(\rho) = \textrm{logit}(X\cdot\tilde{\rho} + A_s + B_e + C_r + D_{S(d)})
    \end{equation}$

    $\begin{equation}
    p^{(d)}_{ser}(\rho) = \textrm{logit}(x\cdot\tilde{\rho} + \alpha_s + \beta_e + \gamma_r + \delta_{S(d)})
    \end{equation}$

    where $S(d)$ is the state in which the district $d$ is located,
    and $\tilde{\rho} = \textrm{log}(\rho) - \textrm{mean}(\textrm{log}(\rho))$

    - $X$ and $x$ are single coefficients, given broad, normal, zero-centered priors.

    - $A_s$, $B_e$, $\alpha_s$, and $\beta_e$ are constrained to be $\pm$ one number each,
      since the categories are binary.
      E.g., $B_\textrm{college-grad} = -B_\textrm{non-grad}$.
      They are given broad, normal, zero-centered priors.

    - $C_r$ and $\gamma_r$ represent one number per race category and are partially pooled:
      we add hyper-parameters representing the mean value among all races and the variance
      of the specific race coefficients around that mean.  The mean acts like the constant term
      in the fit, setting an average turnout/preference among all people,
      and the variance adjusts how much the race-specific levels
      of turnout/D-preference are derived from each racial group separately.

    - $D_s$ and $\delta_s$ take on one value per state and are also partially pooled.
      Here we assume zero mean and use one
      hyper-parameter for the variance, allowing the fit to determine
      how much the state-by-state variance is determined by the demographics of the state
      vs. something intrinsic to the state itself, an issue we looked at in some detail in
      a [previous post](https://blueripple.github.io/research/Turnout/StateSpecific1/post.html).

3. We use
   [Haskell](https://www.haskell.org) to parse the data, reorganize it and produce
   code for
   [Stan](https://mc-stan.org), which then runs the
   [Hamiltonian Monte Carlo](https://en.wikipedia.org/wiki/Hamiltonian_Monte_Carlo)
   to estimate the posterior distribution of all the parameters and thus
   posterior distributions of $t^{(d)}_{ser}(\rho)$ and $p^{(d)}_{ser}(\rho)$.

4. Using those distributions and the breakdown of the demographics of each SLD,
   which amounts to a population density ($\rho*$), and numbers, $N_{ser}$
   of eligible voters for each combination of sex, education-level and race, we
   [post-stratify](https://en.wikipedia.org/wiki/Multilevel_regression_with_poststratification)
   to get a distribution of votes, $V$, and democratic votes, $D$:

    $\begin{equation}
    V = \sum_{ser} N_{ser} t_{ser}(\rho*)
    \end{equation}$

    $\begin{equation}
    D = \sum_{ser} N_{ser} t_{ser}(\rho*) p_{ser}(\rho*)
    \end{equation}$

    where it’s understood that we are setting the state $S=\textrm{Virginia}$.
    From here it’s straightforward to compute the distribution
    of democratic vote share $D/V$.

One advantage of estimating these things via Monte Carlo, is that we compute each quantity,
including the post-stratified ones, on *each* Monte Carlo iteration.  So rather than just
getting an estimate of the value of each quantity, we get some large number of samples from
its posterior distribution.  So we have good
diagnostics around these quantities: we can see if we’ve done enough sampling for
their distributions to have converged and we can extract informative confidence
intervals–even of derived quantities like the post-stratifications–rather
than just crude estimates of standard deviation as you might get from various methods which
estimate parameters via optimization of the maximum-likelihood function.

[^censusGeographies]: For the decennial census there is detailed data available at the block
level, where a block has ~100 people.  For the American Community Survey, which collects
data every year and thus is more up to date than the decennial census, data is available
only at the “block-group” level, consisting of a few thousand people.

[^arealInterpolation]: By “overlap” we mean that we weight a census geography in a
SLD by computing how much the area of that geography overlaps the district, a
technique called
[“areal interpolation”](https://www.spatialanalysisonline.com/HTML/areal_interpolation.htm).
Areal interpolation assumes that people are distributed evenly over the source geographies
(the census blocks or block-groups).  This assumption is relatively harmless if the
census geographies are very small compared to the target geography.  More accuracy can
be achieved using sources of population density data within the census geographies, for
example, the
[National Land Cover Database (NLCD)](https://www.usgs.gov/centers/eros/science/national-land-cover-database?qt-science_center_objects=0#qt-science_center_objects).

[^postStratify]: Given a model giving a probability that any person,
described by their demography, will vote and
the probability that they will vote for the Democratic candidate, we
can break each SLD into buckets of each sort of person in our model
and then multiply the number of people in each group by the those
probabilities to figure out the total number of votes in each group and
how many of those votes were for the Democratic candidate.  We add
all those numbers up and that yields turnout and Dem vote share.  Applying
the model to the composition of the SLD in this way is called
“post-stratification”.

[^otherData]: Using precinct-level data would be interesting.  At the
precinct-level we can build demographic profiles from census data and get election
returns and then *infer* the demographic components of turnout and voter preference.
This is challenging in a number of ways.  E.g., The geography of precincts can be difficult
to get accurately and thus building demographic profiles can be tricky as well. It can also
be difficult to get the precinct-level returns in a standard format.
If you are interested in precinct data with geography,
[Openprecincts.org](https://openprecincts.org) does great work assembling this state-by-state.

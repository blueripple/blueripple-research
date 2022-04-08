# Blue Ripple’s Demographic Election Model

## Introduction

We want to build a reasonable but simple demographic model of
voter turnout and party preference which we
can use to predict the outcome of an election
in a geography with known demographics, like a congressional or
state-legisltive district.
And, since this is a redistricting year,
we want something that we can apply to newly drawn
districts as well as existing ones.

When we say “predict” above we don’t mean in the sense
of actually trying to handicap the outcome. Instead we intend
to estimate the outcome based only on geographic and demographic variables.
So we ignore things like current national sentiment toward either
party, whether or not the candidate is an incumbent, etc. Our goal
is to isolate something like an intrinsic partisan lean, though we
readily acknowledge that this can shift over time as voter turnout
and preferences shift. We’re trying to strike a balance between an
estimate which just assembles voting history for all the precincts
in a district, e.g., [Dave’s Redistricting][DavesR] and a full-blown
prediction model, using demographics, poll averages,
and economic-indicators, e.g.,
the [Economist Model][EconM].

[DavesR]: https://davesredistricting.org/maps#aboutus
[EconM]: https://projects.economist.com/us-2020-forecast/president

We don’t expect our model to be more accurate than a historical or
full-blown predictive model. Instead, we hope to spot places where
the demographic expectation and historical results don’t line up,
and to do so well in advance of any polling or in places where polls
don’t exist, like state-legislative races.

We could work bottom-up, using voting precincts as building blocks:

1. Get the geographic boundary and election results for each precinct,
e.g., from [openprecincts](https://openprecincts.org).
2. Build a demographic profile of the precinct using overlaps[^arealInterpolation]
of the precinct and census “blocks” or “block groups”[^censusGeographies].
3. Use the total number of votes and votes cast for the Democratic candidate
in each precinct to *infer* a demographic and geographic model of turnout and voter preference in the
entire set of precincts.
4. Apply that model to the demographics of a district to generate a rough
estimate of the likely election result.

For a given district (or set of districts), what precincts do we include in the model?
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
and the
Cooperative Election Survey
([CES](https://cces.gov.harvard.edu),
formerly the CCES) and a national survey
of voter preference, like the CES, to build demographically stratified turnout
and preference data at the state or congressional-district (CD) level.
We add federal election result data at the Congressional-district and state-wide level.
2. Use that data to *infer* a demographic model of turnout and voter preference,
with state-level effects.
3. Apply[^postStratify] that model to the demographics of a given district to generate
a rough estimate of the likely election result.

## Definitions and Notation
Some important labels:

  Name   Description
 ------  -----------------------------------------------------------------
 $g$     Demographic Group (combination of sex, education, race/ethnicity)
 $s$     State
 $d$     District

And quantities:

 Name   Description
------  ------------------------------------------------------------------------
 $N$    Citizen Voting Age Population (CVAP)
 $V$    Number of votes cast (for either Democrat or Republican)
 $t$    Turnout ($t=\textrm{Pr{Voting age citizen votes in the election}}=V/N$)
 $D$    Number of votes cast for the Democratic candidate
 $p$    Democratic voter preference ($p=\textrm{Pr{Votes for the Democrat|Voted}}=D/V$)

A couple of things are worth pointing out here:

- We've chosen to ignore third-party candidates and compute two-party preference (the probability that
  a voting age citizen who chooses either the Democrat or Republican chooses the Democrat) and
  two-party share (the probability that a voter who chooses either the Democrat or Republican chooses the
  Democrat). This is simpler and almost always what we care most about.

- We model the voter preference of *people who vote*. We are interested in election outcomes
  and those are driven by voters.  But there are certainly interesting questions to address about
  whether voters and non-voters have the same preferences.

Some useful relations among these quantities:

- $N^{(s)}=\sum_g N_g^{(s)}$
-



1. For turnout, we have the CPSVRS, the CES and the elections themselves.
The CPSVRS is self-reported whereas the
CES validates the turnout data via voter files.  All other things equal, we’d prefer only validated
data.  But there’s some evidence that the
[validation process used by the CES introduces bias](https://agadjanianpolitics.wordpress.com/2018/02/19/vote-validation-and-possible-underestimates-of-turnout-among-younger-americans/)
because it tends to miss people who move between elections,
and they are disproportionately likely to be young and poor.
So we use both sources, along with actual reported turnout, though
for that we have only aggregate data for the state or Congressional district.
People tend to over-report their own turnout, which presents a problem for non-validated sources.
This is a problem discussed in detail in
[Hur & Achen](https://www.aramhur.com/uploads/6/0/1/8/60187785/2013._poq_coding_cps.pdf).
Their suggestion is to reweight the CPS so that the weighted post-stratification matches
known total turnout in each geography. The standard approach for doing that (explained on pps. 9-10 of
this [paper][GelmanGhitza2018]) is to perform multi-level regression on your survey data
and, once the parameters have been estimated, apply the smallest possible correction to each of them
such that when post-stratified across an election geography, the vote totals match.

[GelmanGhitza2018]: http://www.stat.columbia.edu/~gelman/research/published/mrp_voterfile_20181030.pdf

Our model works slightly differently. We perform multi-level regression on the surveys and known vote
totals *simultaneously*, by adding the implied post-stratified vote total as a derived parameter and
including a term in the model for that as well.

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

And we have election results: at the state-level for presidential and senate elections and at the
CD level for congressional elections.

For each Legislative District (LD) we have data from the ACS, which we aggregate from
the block-group level using
[areal interpolation](https://medium.com/spatial-data-science/spatial-interpolation-with-python-a60b52f16cbb).
Our shapefiles for the LDs and the census block-groups come from the Census Bureau, or, in the case of new
legislative maps, from the excellent [Dave’s Redistricting][DavesR].
The result of this aggregation is a breakdown
of the citizens in each district by the same demographic variables as the CPSVRS and CES data, as well as
an estimate of population density in the SLD.

[DavesR]: https://davesredistricting.org/maps#aboutus

Modeling proceeds as follows:

1. We add population density to each of our turnout/voter preference data sets.  This requires some care
   when aggregating to larger areas. We use a population-weighted average of the log of the density to aggregate,
   which is equivalent to the population-weighted geomtric mean of the density. We use population weighting
   so that we capture the density in which people are actually living: imagine a district with a very dense
   city and then a lot of empty land.  The non-weighted density would be something centered between the city
   density and the very small rural density. But most people live in the city! So the correct density for modeling
   behavior is very close to the density of the city, something captured by population-weighting. We use the
   geometric mean because it is more robust to outliers, and population-density has a lot of outliers!

2. We model turnout and vote choice as [binomially][WPBinom]
   distributed with success probabilities determined by the various
   demographic parameters.  That is, given a district,
   $d$, and the subset (by sex, education-level and race) of people $g$,
   we assume that the turnout, $T$, of eligible voters, $E$,
   and the votes for the Democratic candidate, $D$,
   out of all validated voters $V$, both follow binomial distributions:

    $T_g^{(d)} \thicksim B\Big(E^{(d)}_g\Big|t_g^{(d)}\Big)$

    $D_g^{(d)} \thicksim B\Big(V^{(d)}_g\Big|p_g^{(d)}\Big)$

    where $B(n|p)$ is the distribution of successful outcomes from $n$-trials with
    probability of success $p$.

    NB: If we were using the same data for turnout and preference, $T^{(d)}$
    would be the same as $V^{(d)}$,
    but since we are using different data sets, we need to model them separately.

    The $t$ and $p$ parameters must be between 0 and 1 and are modeled via
    [logistic functions](https://en.wikipedia.org/wiki/Logistic_function)
    so that the parameters themselves are unconstrained.
    This allows the fitting to proceed more easily.
    The logistic function then maps the unconstrained
    sum of the parameters to a probability.

    $\begin{equation}
    t_{(d,g)} = \textrm{logit}\big(\alpha_T^{S(d)} + \vec{X}^{(d,g)}\cdot\vec{\beta}_T\big)
    \end{equation}$

    $\begin{equation}
    p_{(d,g)} = \textrm{logit}\big(\alpha_P^{S(d)} + \vec{X}^{(d,g)}\cdot\vec{\beta}_P\big)
    \end{equation}$

    where $S(d)$ is the state in which the district $d$ is located and

    - $\vec{X}$ is a vector carrying the information about the demographics of a subset of a
      district or state.

    - $\alpha$ is a hierarchical parameter, partially pooling the information from all states
      as to overall level of turnout and preference.

    - $\vec{\beta}$ is a vector of hierarchical parameters relating turnout and
      preference to all of our demographic variables (sex, education,
      race/ethnicity and population density)

    - Population density is binned into 10 quantiles. We’ve also tried using the log of the population
      density as a continuous variable, but even using the logarithm, there are outliers which are
      better modeled using the binning.

    - There is one additional complication. For turnout we are combining 3 data-sets (CPSVRS, CES and
      election results) and for preference we are combining two (CES and election results). In the actual
      fit, we add one scalar parameter to the non-election data-sets to allow each data set a different
      average level of turnout or preference.

[WPBinom]: https://en.wikipedia.org/wiki/Binomial_distribution

3. We use
   [Haskell](https://www.haskell.org) to parse the data, reorganize it and produce
   code for
   [Stan](https://mc-stan.org), which then runs the
   [Hamiltonian Monte Carlo](https://en.wikipedia.org/wiki/Hamiltonian_Monte_Carlo)
   to estimate the posterior distribution of all the parameters and thus
   posterior distributions of $\mu_T^{(d,g)}$ and $\mu_P^{(d,g)}$.

4. Using those distributions and the breakdown of the demographics of each SLD,
   which amounts to numbers, $N^{(g,d)}$
   of eligible voters for each combination of sex, education-level and race, we
   [post-stratify](https://en.wikipedia.org/wiki/Multilevel_regression_with_poststratification)
   to get a distribution of votes, $V$, and democratic votes, $D$:

    $\begin{equation}
    V^{(d)} = \sum_g N^{(g,d)} \mu_T^{(d,g)}
    \end{equation}$

    $\begin{equation}
    D^{(d)} = \sum_g N^{(g,d)} \mu_P^{(d,g)}
    \end{equation}$

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

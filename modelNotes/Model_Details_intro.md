# Blue Ripple’s Demographic Election Model

1. Introduction
2. Definitions and Notation
3. Model Dependencies
4. Model Paramters and Structure

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
Some important categories:

   Name              Description
 ------------------  ---------------------------------------------------------------------------------
 $\mathcal{K}        A specific set of demographic groupings (e.g., sex, education and race/ethnicity)
 $k$                 A specific demographic grouping, i.e., $\mathcal{K} = \{k\}$
 $l$                 A location (E.g., a state $S$, or congressional district $d$)
 $\mathcal{D}(S)$    The set of congressional districts in state $S$

And quantities:

 Name   Description
--------  ------------------------------------------------------------------------
 $N$      Citizen Voting Age Population (CVAP)
 $V$      Number of votes cast (for either Democrat or Republican)
 $t$      Turnout ($t=\textrm{Pr{Voting age citizen votes in the election}}=V/N$)
 $D$      Number of votes cast for the Democratic candidate
 $p$      Democratic voter preference ($p=\textrm{Pr{Votes for the Democrat|Voted}}=D/V$)
 $\rho$   Population Density

A couple of things are worth pointing out here:

- We've chosen to ignore third-party candidates and compute two-party preference (the probability that
  a voting age citizen who chooses either the Democrat or Republican chooses the Democrat) and
  two-party share (the probability that a voter who chooses either the Democrat or Republican chooses the
  Democrat). This is simpler and almost always what we care most about.

- We model *voter* preference not voting-age-citizen preference. We are interested in election outcomes
  and those are driven by voters.  But there are certainly interesting questions to address about
  whether voters and non-voting adult citizens have the same candidate preferences.

- We will indicate the subset of each quantity in each demographic group via subscript,
  as in $N_k(g)$ is the number of voting age citizens from demographic group $k$ in geography $g$.

- The capitalized quantities can be summed directly over demographic groupings.
  E.g., $N(g)=\sum_k N_k(g)$, $V(g)=\sum_k V_k(g)$, $D(g)=\sum_k D_k(g)$
  The lowercase quantities are probabilities so they aggregate differently.

    - $$t(l) = V(l)/N(l) = \sum_k N_k(l) t_k(l) / N(l) = \frac{\sum_k N_k(l) t_k(l)}{\sum_k N_k(l)}$$

    - $$t(S) = V(S)/N(S) = \frac{\sum_{d\in \mathcal{D}(S)}\sum_k N_k(d)t_k(d)}{N(S)}$$

    - $$p(l) = D(l)/V(l) = \frac{\sum_k N_k(l) t_k(g) p_k(l)}{V(l)} =\frac{\sum_k N_k(l) t_k(l) p_k(l)}{\sum_k N_k(l) t_k(l)}$$

    - $$p(S) = D(S)/V(S) = \frac{\sum_{d\in \mathcal{D}(S)}\sum_k N_k(d) t_k(d) p_k(d)}{\sum_{d\in \mathcal{D}(S)} \sum_k N_k(d)t_k(d)}$$

We *observe* $t$ and $p$ in any location where we have survey or election data. In the
CES survey data, we observe $t_k(g)$ and $p_k(g)$ for demographic variables including age,
sex, education, race and ethnicity and at the geographic level of congressional districts.
In the CPSVRS survey, we observe $t_k(s)$, i.e., just turnout and only at the level of states[^cpsState].
An election result is an observation of $t(s)$ and $p(s)$ or $t(d)$ and $p(d)$, that is
turnout and preference but without any demographic breakdown[^electionDemographics].

[^cpsState]: The CPSVRS is reported with county-level geography.  Counties can be smaller
or larger than a congressional district and in practice, these geographies are hard to
convert.  Rather than deal with that, we aggregate the CPSVRS to the state-level.

[^electionDemographics]: It's possible, via exit polls or the voter file
(a public record in each state
matching voters to names and addresses) to try and estimate the demographics of actual
election turnout. In practice, these methods are unreliable (exit polls) or expensive
and hard to get right (purchasing, cleaning and matching voter-file data).

Our goal is to model $t$ and $p$ as functions of the location and demographics
so we can understand them better–how does race affect voter preference in Texas?–
and look for anomalies, districts where the modeled expectation and the historical
election results are very different. Such mismatches might indicate issues in the model,
but they might also be indications of interesting political stories, and places to
look for flippable or vulnerable districts.

Modeled Quantities

Name           Description
-------------  -----------------------------------
$u_k(l;\rho)$  Modeled turnout probability
$q_k(l;\rho)$  Modeled Democratic voter preference

So, for a location $l$ and demographic group $k$, $u_k(l;\rho)$ is the estimated
probability that a voting-age citizen in that location, with population density
$\rho$ and with those demographics, will turn out to vote.  And $q_k(l;\rho)$
is the probability that any such voter will choose the democratic candidate[^density].

[^density]: The explicit presence of the population density $\rho$ here is
potentially confusing.
It might seem like population density should simply be part of what varies with the
location, $l$.  In our particular setup, we use population density data at a level more
granular than our modeled set of locations. Most of our data is state-level. And what data
we have at the congressional district level (CES survey and federal house elections)
is for the districts which pertained from the 2012-2020 elections.  Many of those
districts have now changed. And we are interested in state-legislative districts as well.
So we use states as locations rather than congressional districts.
But, like demographic variations, population density is known
to be strongly predictive of voter preference–people in cities are much more likely to
vote for Democrats than people in suburbs or rural areas and this effect persists
even when accounting for differences in age and race between those groups. It’s
relatively easy to gather population density information for any geography of interest:
states, house districts and state-legislative districts.
We handle the density differently from the other demographic variables because density
is a continuous or ordinal predictor rather than categorical, like sex, education or race.

Demographic Model Dependencies (collectively indexed by $k$)

Name            Description
-------------   -------------------------------------------------------------------------------
Sex             Female or Male (surveys and the ACS data only record binary gender information)
Education       Non-graduate from college or college graduate
Race/Ethnicity  Black, Hispanic, Asian, White-non-Hispanic, or Other

The obvious missing category here is age.  Our input data has age information but when we
construct demographic breakdowns of new districts we are limited to what's provided by the
ACS survey. That information is published by the census in various tables and it's not
possible to extract age, education and race/ethnicity simultaneously[^acsAge]. So we
cannot post-stratify using the age information, even if it's present in the model. Since
the model is simpler to fit without it, we drop age information for now.

[^acsAge]: Some ACS tables do have age information, but then we lose education or
race/ethnicity.  There are interesting techniques using those tables together to
extract probability distributions of age given sex, education and race/ethnicity.
We plan to apply these techniques at some point to add age into the model.

## Basic Model Structure

In order to use our data to estimate turnout probability and voter preference, we
need a model of how survey or election results depend on those quantities. In fact,
even asserting that what we need to estimate is only those two probabilities
and not also, e.g., some dispersion in those quantities, implies a particular model.
We are assuming that for each person of type $k$ in location $l$ with
population density $\rho$, the choice to vote is like a coin flip where the odds of
that coin coming up "voted" is $u_k(l;\rho)$ and coming up "didn't vote" is
$1-u_k(l;\rho)$.  Similarly, for any voter of type  $k$ in location $l$ with
population density $\rho$, the chance of their vote being for the Democrat is
"q_k(l;\rho)" and the Republican $1-q_k(l;\rho)$. If we modeled each voter
separately, these would be [Bernoulli][WPBernoulli] trials and when we take
groups of Bernoulli trials with the same probability, we get a
[Binomial][WPBinomial] distribution.

[WPBernoulli]: https://en.wikipedia.org/wiki/Bernoulli_distribution
[WPBinomial]: https://en.wikipedia.org/wiki/Binomial_distribution

Specifically, given $u_k(l;\rho)$ and $q_k(l;\rho)$ we assume that the
probability of observing $n$ voters in a group of $N$ voting-age citizens
of type $k$ in location $l$ with density $\rho$ is $B\big(n|N,u_k(l;\rho)\big)$
and the probability of observing $d$ votes for the democratic candidate among
$V$ voters of type $k$ in location $l$ with density $\rho$ is $B\big(d|V,q_k(l;\rho)\big)$.

We need to parameterize $u$ and $q$ in terms of our observed demographic
variables. We use the [logit][WPlogit] function to map an unbounded
range into a probability (and thus it's inverse to map back to probability):

[WPlogit]: https://en.wikipedia.org/wiki/Logit

$$u_k(l;\rho) = \textrm{logit}^{-1}(\alpha_l + \vec{X}_k(\rho)\cdot\vec{\beta_l})$$
$$q_k(l;\rho) = \textrm{logit}^{-1}(\gamma_l + \vec{Y}_k(\rho)\cdot\vec{\theta_l})$$

where $\vec{X}_k(\rho)$ is a vector containing the density and demographic information.
Population density for any region is computed via a population-weighted geometric mean:
population-weighted to better reflect the lived density of the people in the region
and geometric mean because it is more robust to outliers which are a common feature
of population densities. In our model we bin those densities into 10 quantiles and use
the quantile index in place of the actual density to further reduce the influence of
outliers. The demographic information $k$ is encoded via "one-hot" encoding of the
category: using -1 or 1 for a binary category like college-educated and $M-1$ 0s or 1s
for an $M$-valued category like race/ethnicity.

$\vec{Y}_k(\rho)$ is the same as $\vec{X}_k(\rho)$ except it also contains
an element representing incumbency information for the relevant election:
a $1$ for a Democratic incumbent, $0$ for no-incumbent and $-1$ for a Republican
incumbent.

# Meta-analysis details
Since we are using multiple surveys and the election data–a sort of
meta-analysis–we need to account for the possibility of systematic
differences between these sources of data.
Our (admittedly simplistic) approach to this is to add a single extra parameter
per data-set for each of $u$ and $q$ for each of the non-election sources and for
each election source other than house elections.
This allows each to have a different
overall level of turnout or voter preference while requiring the location and demographic
variation to be the same.

We are, in some sense, treating
house elections as “neutral” and allowing all other data-sets to vary. We chose
this since we intend to apply the model to house elections and that simplifies
post-stratification.

# Hierarchical Model
Rather than estimating $u_k(l;\rho)$ and $q_k(l;\rho)$ directly in each state from data in
only that state, we use a [multi-level model][WPmultilevel], allowing partial-pooling of the national
data to inform the estimates in each state.  This introduces more parameters to the model
but allows for a more robust and informative fit. In our case, we model the $\alpha$, $\gamma$,
$\beta$ and $theta$ hierarchically.  There various ways to parameterize this and we
choose a [non-centered][BANonCentered] parameterization since we expect
our “contexts” (the various states), to be relatively similar which might lead to problems
fitting using a centered paramterization.

[WPmultilevel]: https://en.wikipedia.org/wiki/Multilevel_model
[BANonCentered]: https://betanalpha.github.io/assets/case_studies/hierarchical_modeling.html#51_Multivariate_Centered_and_Non-Centered_Parameterizations

So we set

$$\alpha_l = \alpha + \sigma_\alpha a_l$$
$$\vec{\beta}_l = \vec{\beta} + \vec{\tau}_{\beta} \cdot \textbf{C}_{\beta} \cdot \vec{b}_l$$
$$\gamma_l = \gamma + \sigma_\gamma c_l$$
$$\vec{\theta}_l = \vec{\theta} + \vec{\tau}_{\theta} \cdot \textbf{C}_{\theta} \cdot \vec{d}_l$$

where $\sigma$,and $\vec{\tau}$ are standard-deviation parameters which control
the amount of pooling of national data with state data. If those parameters are
small, then the fit is using mostly unpooled, national data. If those parameters are
large, then the state-level data is mostly informing the fit. We fit those parameters
along with everything else. $\textbf{C}$ is a correlation matrix, fit to the correlation
among the density and demographic parameters in the data.

# Modeling Election Data
So far we’ve dodged one subtlety. Unlike the survey data, the election data
does not come with attached demographic information. One standard approach
to this is to remove it from the multi-level model and then adjust the
model-parameters such that post-stratification matches
some set of aggregates from the election data, for example,
[turnout in each state][GGAdj].

One challenge of modeling
this data is that they are observed over different sorts of geographies (states and districts)
though, since these geographies are nested that is not so complex.
More challenging is the issue of how to incorporate the election data, which is the most
informative in the sense that there are many more voters than survey takers, but lacks
demographic specificity. We’ll come back to this shortly when

only the surveys give us observations for each demographic group, $k$, separately.

For any geography, e.g., a district, we know the demographic breakdown, $\{N_{dg}\}$,
so, given a model fro which allows us, via *post-stratification*[^postStrat], to e



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

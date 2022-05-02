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

By “predict,” we don’t mean in the sense
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
of the precinct and census “blocks,” “block groups” or “tracts”[^censusGeographies].
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
We also add federal election result data at the Congressional-district and state-wide
(President and Senate) level.
2. Use that data to *infer* a demographic model of turnout and voter preference,
with state-level effects.
3. Apply[^postStratify] that model to the demographics of a given district to generate
a rough estimate of the likely election result.

## Definitions and Notation
Some important categories:

   Name              Description
 ------------------  ---------------------------------------------------------------------------------
 $\mathcal{K}$       A specific set of demographic groupings (e.g., sex, education and race/ethnicity)
 $k$                 A specific demographic grouping, i.e., $\mathcal{K} = \{k\}$
 $l$                 A location (E.g., a state $S$, or congressional district $d$)
 $\mathcal{D}(S)$    The set of congressional districts in state $S$

And quantities:

 Name     Description
--------  ------------------------------------------------------------------------
 $N$      Citizen Voting Age Population (CVAP)
 $V$      Number of votes cast (for either a Democrat or Republican)
 $t$      Turnout probability ($t=\textrm{Pr{Voting age citizen votes in the election}}=V/N$)
 $D$      Number of votes cast for the Democratic candidate
 $p$      Democratic voter preference ($p=\textrm{Pr{Votes for the Democrat|Voted}}=D/V$)
 $\rho$   Population Density

A couple of things are worth pointing out here:

- We've chosen to ignore third-party candidates and compute two-party preference (the probability that
  a voting age citizen who chooses either the Democrat or Republican chooses the Democrat) and
  two-party share (the probability that a voter who chooses either the Democrat or Republican chooses the
  Democrat). This is simpler and almost always what we care most about.

- We model *voter* preference not voting-age-citizen preference. We are interested in election outcomes
  and those are created by voters.  But there are certainly interesting questions to address about
  whether voters and non-voting adult citizens have the same candidate preferences.

- We will indicate the subset of each quantity in each demographic group via subscript,
  as in $N_k(g)$ is the number of voting age citizens from demographic group $k$ in geography $g$.

- The capitalized quantities can be summed directly over demographic groupings.
  E.g., $N(g)=\sum_k N_k(g)$, $V(g)=\sum_k V_k(g)$, $D(g)=\sum_k D_k(g)$
  The lowercase quantities are probabilities, so they aggregate differently.

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
so we can better understand them–how does race affect voter preference in Texas?–
and look for anomalies, districts where the modeled expectation and the historical
election results are very different. Such mismatches might indicate issues in the model,
but they might also be indications of interesting political stories, and places to
look for flippable or vulnerable districts. We use $u$ for the modeled $t$, and $q$
for the modeled $p$.

Modeled Quantities

Name           Description
-------------  ----------------------------------------------------------------------------------
$u_k(l;\rho)$  Modeled turnout probability in location $l$ with population density $\rho$
$q_k(l;\rho)$  Modeled Democratic voter preference in location $l$ with population density $\rho$

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
is for the districts which existed from the 2012-2020 elections.  Many of those
districts have now changed. And we are interested in state-legislative districts as well.
So we use states as modeling locations rather than congressional districts.
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
need a model of how survey or election results depend on those quantities. The
standard choices for turnout and voter preference models is
[Bernoulli][WPBernoulli]/[Binomial][WPBinomial]–which assumes that
for each person of type $k$ in location $l$ with
population density $\rho$, the choice to vote is like a coin flip where the odds of
that coin coming up "voted" is $u_k(l;\rho)$ and coming up "didn't vote" is
$1-u_k(l;\rho)$.  Similarly, for any voter of type  $k$ in location $l$ with
population density $\rho$, the chance of their vote being for the Democrat is
$q_k(l;\rho)$ and the Republican $1-q_k(l;\rho)$. If we modeled each voter
separately, these would be [Bernoulli][WPBernoulli] trials and when we take
groups of Bernoulli trials with the same probability of success, we get a
[Binomial][WPBinomial] distribution.

The Bernoulli/Binomial is a reasonable fit
for each data-set alone, but had insufficient dispersion,
even allowing for various offsets in the parameters, to
account for the different data sources all in one model.
The [Beta-Binomial][WPBetaBinomial],
on the other hand, allows a reasonable fit to all the data.  The cost
of using this slightly more complex model, is the introduction of
a parameter (one each for turnout and vote choice) which reflects
the dispersion in the observed probabilities.

[WPBernoulli]: https://en.wikipedia.org/wiki/Bernoulli_distribution
[WPBinomial]: https://en.wikipedia.org/wiki/Binomial_distribution
[WPBetaBinomial]: https://en.wikipedia.org/wiki/Beta-binomial_distribution

Specifically, given $u_k(l;\rho)$ and $q_k(l;\rho)$, we assume that the
probability of observing $n$ voters in a group of $N$ voting-age citizens
of type $k$ in location $l$ with density $\rho$ is
$BB\big(n|N,u_k(l;\rho)\phi_T,(1-u_k(l;\rho))\phi_T\big)$
and the probability of observing $d$ votes for the democratic candidate among
$V$ voters of type $k$ in location $l$ with density $\rho$ is
$BB\big(d|V,q_k(l;\rho)\phi_P,(1-q_k(l;\rho))\phi_P\big)$, where the
$\phi$’s parameterize the dispersion in probabilities.

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
category: using -1 or 1 for a binary category like college-educated and $(M-1)$ 0s or 1s
for an $M$-valued category like race/ethnicity.

$\vec{Y}_k(\rho)$ is the same as $\vec{X}_k(\rho)$ except it also contains
an element representing incumbency information for the relevant election:
a $1$ for a Democratic incumbent, $0$ for no-incumbent and $-1$ for a Republican
incumbent.

# Hierarchical Model
Rather than estimating $u_k(l;\rho)$ and $q_k(l;\rho)$ directly in each state from data in
only that state, we use a [multi-level model][WPmultilevel], allowing partial-pooling of the national
data to inform the estimates in each state.  This introduces more parameters to the model
but allows for a more robust and informative fit. In our case, we model the $\alpha$, $\gamma$,
$\vec{\beta}$ and $\vec{\theta}$ hierarchically.  There various ways to parameterize this and we
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

where $\sigma_\alpha$, $\sigma_\gamma$, $\vec{\tau}_\beta$, and $\vec{\tau}_\theta$
are standard-deviation parameters which control
the amount of pooling of national data with state data. If those parameters are
small, then the fit is using mostly unpooled, national data. If those parameters are
large, then the state-level data is mostly informing the fit. We fit those parameters
along with everything else. $\textbf{C}$ is a correlation matrix, fit to the correlation
among the density and demographic parameters in the data.

# Meta-analysis details
Since we are using multiple surveys and the election data–a sort of
meta-analysis–we need to account for the possibility of systematic
differences between these sources of data.
Our approach to this is to add a hierarchical (logistic) $\alpha$
centered at 0 and varying among the data-sets, as well as a non-hierarchical
data-set-specific offsets for each demographic category $\vec{\beta}$ in each
data-set except the house election results[^dataSetBeta].

[^dataSetBeta]: There are two things to explain here. Why isn’t the data-set
$\beta$ also hierarchical? And why not have a $\vec{\beta}$ offset for all data-sets?
We tried a hierarchical data-set-$\beta$ but it was difficult to fit
and, we think, requires a mix of centered/non-centered parameterizations.
So we switched to a non-hierarchical set of offsets for $\vec{\beta}$.
But once the parameter
is non-hierarchical, having an offset for each data-set would introduce a
redundancy (technically, the parameters would be
“[non-identifiable][WPIdentifiability]”) because a fixed amount could be added
to each data-set $\vec{\beta}$ and removed from the shared $\vec{\beta}$
without changing the estimated
probabilities. To fix that, we pick one data-set as “neutral” and don’t add
the demographic offset there. Since we are interested in house
and state-legislative elections, using the house election as the “neutral”
data-set makes the most sense.

[WPIdentifiability]: https://en.wikipedia.org/wiki/Identifiability

# Modeling Election Data
So far we’ve dodged one subtlety. Unlike the survey data, the election data
does not come with attached demographic information. One approach
is to remove it from the multi-level model and then, after fitting
the multi-level model to the survey data, adjusting the
model-parameters such that post-stratification matches
some set of aggregates from the election data, for example,
turnout in each state. This is done by choosing a
“smallest” such adjustment in some well-specified sense. See,
e.g., [“Deep Interactions With MRP...][GGDeep2013], pp. 769-770.

[GGDeep2013]: http://www.stat.columbia.edu/~gelman/research/published/misterp.pdf

As discussed in that paper, such an adjustment assumes small correlations between
whatever leads to mismatches between the survey and the election results
and the demographic variables in question. It’s not clear why that should be the case.
In the case of the CPS at least, there is [evidence][AFS2021] that this assumption doesn’t hold.

[AFS2021]: https://static1.squarespace.com/static/5fac72852ca67743c720d6a1/t/5ff8a986c87fc6090567c6d0/1610131850413/CPS_AFS_2021.pdf

And there is also a technical issue: When adjusting the parameters this way
it is unclear what becomes of their distributions. Depending on how we do the
estimation (see next section), we may have more information about each parameter
than its likeliest value, including some information about uncertainty or a
confidence interval. But it’s not at all clear how that information should be
adjusted via the procedure outlined in the paper above.

There is, however, an alternative. We can add the likelihood of the parameters explaining
the election data to the model itself. We do this by constructing new parameters for each election $i$:

$$\hat{u}^{(i)} = \frac{\sum_k N^{(i)}_k u_k(l^{(i)};\rho^{(i)})}{N^{(i)}}$$
$$\hat{q}^{(i)} = \frac{\sum_k N^{(i)}_k u_k(l^{(i)};\rho^{(i)}) q_k(l^{(i)},\rho^{(i)})}{\sum_k N^{(i)}_k u_k(l^{(i)};\rho^{(i)})}$$

and then asserting that the election is governed by the same beta-binomial process as the surveys.
That is given parameters $u$ and $q$, the probability that,
in election $i$, given $N$ voting-age citizens in
the election location $l^{(i)}$, the probability that there were $V^{(i)}$ voters,
$D^{(i)}$ of whom voted for the Democratic candidate are
$BB\big(V^{(i)}|N^{(i)},\hat{u}^{(i)}\phi_T,(1-\hat{u}^{(i)})\phi_T\big)$ and
$BB\big(D^{(i)}|V^{(i)},\hat{q}^{(i)}\phi_P,(1-\hat{q}^{(i)})\phi_P\big)$ respectively.

## Practical Considerations

At this point, we are, theoretically, done. We have parameterized our probabilities via our data,
and asserted that each piece of data, survey or election, has a likelihood expressed by a Beta-Binomial
distribution using those parameters. So we can construct the joint likelihood
and attempt to maximize it in order to estimate our parameters.
Or we can use a Bayesian approach, choose priors for our various parameters,
combine those with the probability of the data given the parameters (the likelihood)
and use that to find the joint posterior distribution of our parameters given our data, etc.

Let’s recall that our goal is to apply these estimations to new geographical regions via
post-stratification. So, optimally, we’d like some way of computing a distribution of
post-stratification results that is consistent with the distribution of parameter values,
since the uncertainties we are ultimately interested in are those of the post-stratifications.

Monte-Carlo simulation is ideal here, since it allows computation of derived quantities on each path,
resulting in a distribution of post-stratified results.  Further, Monte-Carlo simulation does not
require that we analytically maximize this very complex likelihood, though, depending on the method,
it does require gradients of the likelihood.

If we only want to model the surveys, this is straightforward and fast. However, once we include the
election results, things get harder since the $\hat{u}$ and $\hat{q}$,
being sums over many $\textrm{logit}^{-1}$ functions, make the gradients of
the likelihood computationally burdensome. In practice, these models run in a few
hours on a reasonably powerful laptop.

We use [Haskell](https://www.haskell.org) to parse the data, reorganize it and produce
code for [Stan](https://mc-stan.org), which then runs the
[Hamiltonian Monte Carlo](https://en.wikipedia.org/wiki/Hamiltonian_Monte_Carlo)
to estimate the posterior distribution of all the parameters and thus
posterior distributions of $u_k(l,\rho)$ and $q_k(l,\rho)$.

To produce demographic profiles of new districts, we get shapefiles from
[Dave’s Redistricting][DRA], ACS data from the Census Bureau via [NHGIS][NHGIS]
and use areal interpolation[^arealInterpolation] to compute the overlap of each
census geography (tracts[^censusGeographies], in this case) with our
new districts and then aggregate that census information for the district.

[DRA]: https://davesredistricting.org
[NHGIS]: https://www.nhgis.org

Using those distributions and demographic breakdowns (which amounts to the numbers, $N_k(l)$
of eligible voters for each combination of sex, education-level and race), we
[post-stratify](https://en.wikipedia.org/wiki/Multilevel_regression_with_poststratification)
to get from our parameter values to votes $V$, and democratic votes, $D$:

$$V(d) = \sum_k N_k(d) u_k(l,\rho)$$
$$D(d) = \sum_k N_k(l) u_k(l,\rho) q_k(l,\rho)$$

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
district by computing how much the area of that geography overlaps the district, a
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

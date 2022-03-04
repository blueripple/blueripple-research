### Modeling State Legislative Elections
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

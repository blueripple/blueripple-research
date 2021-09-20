### Modeling State Legislative Elections
We want to build a reasonable but simple model of voter turnout and preference which we
can apply to estimating outcomes in state legislative districts.  We want to build something
we can apply fairly easily to any state.  And, since this is a redistricting year,
we want something that can apply to new districts as well as exisiting ones.

One possibility works bottom-up, using voting precincts as building blocks:

1. For each precinct get the geographic boundary and election results from
[openprecincts](https://openprecincts.org).
2. Build a demographic profile of the precinct using overlaps of census “blocks” or “block
groups”[^censusGeographies].
3. Use the total number of votes and votes cast for the Democratic candidate
to *infer* a demographic model of turnout and voter preference.
4. Apply (post-stratify) that model to the demographics of a SLD to generate a rough
estimate of the likely election result.

For a given SLD, what precincts do we include in the model?  In order to keep
things simple we want a model that we can use on multiple districts, so we
would want one model using every precinct in the country or at least the state.
Using every precinct in the country is hard, some of that data is unavailable
and there are a lot of them!  Using just data from each state risks being a little
too influenced by the local election history.

Another possibility works top-down from national-level data:

1. Use a national survey of voter turnout, e.g., the
Current Population Survey Voter Registration Supplement (CPSVRS) and/or the
Cooperative Election Survey (CES, formerly the CCES) and a national survey
of voter preference, like the CES, to build demographically stratified turnout
and preference data at the state or congressional-district (CD) level.
2. Use that data to infer a demographic model of turnout and voter preference,
possibly with state or CD-level effects.
3. Apply (post-stratify) that model to the demographics of a SLD to generate
a rough estimate of the likely election result.

This approach might be too general: voters in different regions might not
be well described by a national model.  The data we use is organized by CD,
so we can choose a model with some geographic specificity. Even better, we can
compare the quality of models with different levels of geographic detail.

For now, we are working with the top-down approach.  That data is considerably simpler
to work with and more comprehensive–precinct level data is not available for all states
and all election years.  But we remain interested in the bottom-up approach as well
and we might implement some version of it for comparison.

[^censusGeographies]: For the decennial census there is detailed data avaialable at the block
level, where a block has ~100 people.  For the American Community Survey, which collects
data every year and thus is more up to date than the decennial census, data is available
only at the “block-group” level, consisting of a few thousand people.


What data is available?


For turnout, we have the
Current Population Survey Voter Registration Supplement (CPSVRS) and/or the
Cooperative Election Survey (CES, formerly the CCES).  The CPSVRS is self-reported whereas the
CES validates the turnout data via voter files.  All other things equal, we’d prefer the validated
data.  But the CPSVRS is a considerably larger survey.  And there’s some evidence that the
validation process introduces bias because it tends to miss people who move between elections
and they are disproportionately likely to be young and poor.  For both reasons of size
and bias, we use the CPSVRS as a source of turnout data.

We model turnout and voter preference using demographic variables by congressional district (CD).
We use congressional districts as our modeling units for two reasons:
CDs are a core interest at BlueRipple politics
and CDs are the smallest geography easily available in all the required data.
We can get more fine grained census data for demographics but the turnout and
voter preference data that we use[^otherData] is not available at the SLD level.

[^otherData]: Using precinct-level data would be interesting.  At the
precinct-level we can build demographic profiles from census data and get election
returns and then *infer* the demographic components of turnout and voter preference.
This is challenging in a number of ways.  E.g., The geography of precincts can be difficult
to get accurately and thus building demographic profiles can be tricky as well. It can also
be difficult to get the precinct-level returns in a standard format.
If you are interested in precinct data with geography,
[Openprecincts.org](https://openprecincts.org) does great work assmebling this state-by-state.

For each of the 436 districts (435 CDs plus DC) in the U.S., we have:

- The Census Bureau’s Current Population Survey Voter Registration Supplement (CPSVRS)
which contains self-reported voter turnout for about 1% of eligible voters in each district,
broken down by sex (female or male), education (non-college-grad or college-grad)
and race (Black, Latinx, Asian, white-non-Latinx, other).

- The Cooperative Election Study (CES), containing validated turnout and voter preference–specifically
the political party of the voter’s choice for congressional representative, broken down by the same
demographic categories.

- Population density, computed by aggregating from the Census Bureau’s American Community Survey
(ACS) at the Public-Use-Microdata-Area level.

For each State Legislative District (SLD) we have data from the ACS, which we aggregate from
the block-group level using
[areal interpolation](https://medium.com/spatial-data-science/spatial-interpolation-with-python-a60b52f16cbb).
Our shapefiles for the SLDs come from the Census Bureau.  The result of this aggregation is a breakdown
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
    t^{(d)}_{ser}(\rho) = \textrm{logit}(X\tilde{\rho} + A_s + B_e + C_r + D_{S(d)})
    \end{equation}$

    $\begin{equation}
    p^{(d)}_{ser}(\rho) = \textrm{logit}(x\tilde{\rho} + \alpha_s + \beta_e + \gamma_r + \delta_{S(d)})
    \end{equation}$

    where $S(d)$ is the state in which the district $d$ is located,
    and $\tilde{\rho} = \textrm{log}(\rho) - \textrm{mean}(\textrm{log}(\rho))$

    - $X$ and $x$ are single coefficients, given broad, normal, zero-centered priors.

    - $A$, $B$, $\alpha$, and $\beta$ are constrained to be $\pm$ one number each,
      since the categories are binary.
      E.g., $B_\textrm{college-grad} = -B_\textrm{non-grad}$.
      They are given broad, normal, zero-centered priors.

    - $C$ and $\gamma$ represent one number per race category and are partially pooled:
      we add hyper-parameters representing the mean value among all races and the variance
      of the specific race coefficients around that mean.  The mean acts like the constant term
      in the fit, setting an average turnout/preference among all people,
      and the variance adjusts how much the race-specific levels
      of turnout/D preference are derived from each racial group separately.

    - $D$ and $\delta$ take on one value per state and are also partially pooled.
      Here we assume zero mean and use one
      hyper-parameter for the variance, allowing the fit to determine
      how much the state-by-state variance is determined by the demographics of the state
      vs. something intrinsic to the state itself, an issue we looked at in some detail in
      a [previous post](https://blueripple.github.io/research/Turnout/StateSpecific1/post.html).

3. We use
   [Haskell](https://www.haskell.org) to parse the data, reorganize it and write code for
   [Stan](https://mc-stan.org), which then runs the
   [Hamiltonian Monte Carlo](https://en.wikipedia.org/wiki/Hamiltonian_Monte_Carlo)
   to estimate the posterior distribution of all the parameters and thus
   posterior distributions of $t^{(S)}_{ser}(d)$ and $p^{(S)}_{ser}(d)$.

4. Using those distributions and the breakdown of the demographics of each SLD,
   which amounts to a population density ($\rho*$), and numbers, $N_{ser}$
   of eligible voters for each combination of sex, education-level and race, we
   [post-stratify](https://en.wikipedia.org/wiki/Multilevel_regression_with_poststratification)
   to get a distribution of votes, $V$, and democratic votes, $D$:

    $\begin{equation}
    V = \sum_{ser} N_{ser} t_{ser}
    \end{equation}$

    $\begin{equation}
    D = \sum_{ser} N_{ser} t_{ser} p_{ser}
    \end{equation}$

    where it’s understood that we are setting the state $S=\textrm{Virginia}$
    and $\rho=\rho*$.
    From here it’s straightforward to compute the distribution
    of democratic vote share $D/V$.

One advantage of estimating these things via Monte Carlo, is that we compute each quantity,
including the post-stratified ones, on *each* Monte Carlo iteration.  So rather than just
getting an estimate of the value of each quantity, we get some large number of samples from
its posterior distribution.  The most straightforward advantages of this are that we have good
diagnostics around these quantities: we can see if we’ve done enough sampling for
their distributions to have converged and we can extract informative confidence
intervals–even of derived quantities like the post-stratifications–rather
than just crude estimates of standard deviation as you might get from various methods which
estimate parameters via optimization of the maximum-likelihood function.

### Modeling State Legislative Elections
For each of the 435 *congressional* districts (CDs) in the U.S., we have:

- The Census Bureau’s Current Population Survey Voter Registration Supplement (CPSVRS)
which contains self-reported voter turnout for about 1% of eligible voters in each district,
broken down by sex (female or male), education (non-college-grad or college-grad)
and race (Black, Latinx, Asian, white-non-Latinx, other).

- The Cooperative Election Study, containing validated turnout and voter preference–specifically
the political party of the voters choice for congressional representative, broken down by the same
demographic categories.

- Population density, computed by aggregating from the Census Bureau’s American Community Survey
(ACS) at the Public-Use-Microdata-Area level.

For each *State Legislative District* (SLD) we have data from the ACS, which we aggregate from
the block-group level using
[areal interpolation](https://medium.com/spatial-data-science/spatial-interpolation-with-python-a60b52f16cbb).
Our shapefiles for the SLDs come from the Census Bureau.  The result of this aggregation is a breakdown
of the citizens in each district by the same demographic variables as the CPSVRS and CES data, as well as
an estimate of population density in the SLD.

Modeling proceeds as follows:

- We combine the CPSVRS, CES, and CD-level population density into one data-set, with rows for each combination
of demographic variables within each CD.  That’s 2 x 2 x 5 x 435 = 8700 rows, each with turnout data from both
CPSVRS and CES, party preference from the CES and population density from the ACS.

- We model turnout and voter preference as binomially distributed with probabilities determined by the various
demographic parameters.  That is, we assume that the turnout, $T$ of eligible voters, $E$ in a given state, $S$,
in a district with population density $d$,
with sex $s$, education-level $e$ and race $r$, denoted, and the number of actual voters, $V$, who
vote for the democratic candidate follow binomial distributions:
$T^{(S)}_{ser}(d) \thicksim B(E^{(S)}_{ser}(d),t^{(S)}_{ser}(d))$
and $D^{(S)}_{ser}(d) \thicksim B(V^{(S)}_{ser}(d),p^{(S)}_{ser}(d))$.
where $B(n,p)$ is the distribution of successful outcomes from $n$-trials with probability of success $p$.
If we were using the same data for turnout and preference, $T^{(S)}_{ser}(d)$
would be the same as $V^{(S)}_{ser}(d)$,
but since we are using different data sets, we need to model them separately.

The probabilities. $t$ and $p$, are assumed to be inverse logistic functions:

$\begin{equation}
t^{(S)}_{ser}(d) = logit^{-1}(X \ln(d) + A_s + B_e + C_r + D_S)
\end{equation}$

$\begin{equation}
p^{(S)}_{ser}(d) = logit^{-1}(x \ln(d) + \alpha_s + \beta_e + \gamma_r + \delta_S)
\end{equation}$

- $X$ and $x$ are single numbers, given a broad normal prior
$A$, $B$, $\alpha$, and $\beta$ are $\pm$ one number since the categories are binary,
and given broad normal priors.

$C$ and $\gamma$ represent one number per race category and are partially pooled:
we add hyper-parameters representing the mean value among all races and the std deviation
of the specific race coefficients around that mean.  The mean acts like the constant term
in an ordinary fit and the std deviation adjusts how much pooling is used to determine the
race coefficients.

- $D$ and $delta$ are also partially pooled but we assume a mean of 0 and then allow the
fitting to determine the optimal amount the state parameters should be able to vary.

- We use
[Haskell](https://www.haskell.org) to parse the data, reorganize it and write code for
[Stan](https://mc-stan.org), which then runs the Markov Chain Monte Carlo to estimate
the posterior distribution of all the parameters and thus
posterior distributions of $t^{(S)}_{ser}(d)$ and $p^{(S)}_{ser}(d)$.

- Using those distributions and the breakdown of the demographics of each SLD,
which amounts to a population density, and number, $N_{ser}$ of eligible voters for each
sex, education-level and race, we
[post-stratify](https://en.wikipedia.org/wiki/Multilevel_regression_with_poststratification)
to get a distribution of votes, $V$, and democratic votes, $D$:
$\begin{equation}
V = \sum_{ser} N_{ser} t_{ser}
\end{equation}$

$\begin{equation}
D = \sum_{ser} N_{ser} t_{ser} p_{ser}
\end{equation}$

where it’s understood that we are setting the state $S=\textrm{Virginia}$ and $d = d*$.
From here it’s straightforward to compute the democratic vote share $D/V$.

*A Critical Look at the Blue Ripple Voter Model*

Over the past year we’ve developed our first version of a model to estimate voter turnout and
voter preference based on geography (State) and demographics (population density, race, education and sex).
A summary of the technical details of the model, please see [here][modelExplainer]. In brief, the model
uses surveys ([CES][CES] and the [CPS Voter Supplement][CPSVRS]) and statewide election results
(for House, Senate and President) to estimate the probability that an eligible voter will choose
to vote and the probability that will vote for the democratic candidate. We then use those probabilities
(acutally, their joint distributions) to estimate vote share in a given place with known population density
and demographics.

We’ve structured the model
to balance the national and state level data as well as to balance the various sources
of information using hierarchical modeling techniques[^hierarchical]. And we use Monte-Carlo methods to
find the posterior distribution of the parameters (i.e., fit the model)[^montecarlo]
so that we keep as much information about uncertainties as possible throughout.

[^hierarchical]: We add a so-called “hyper-parameter” which controls the balance between the
state and national level influence on various parameters. As an example, consider the
parameter which reflects the effect of being a college-graduate on voter preference. In our model,
that parameter has a national component–which is the same across all states–and a state component.
Our hyper-parameter, the standard deviation of the state-components,
controls how likely the state-components are to be zero. If that parameter is small, the
parameter will be dominated by the national effect, as if we didn’t use state-level data.
If instead that parameter is large, the state-level information will dominate. The hyper-parameter
is estimated along with all the other parameters, so we allow the data to tell us how this balance
is best achieved. For more on hierarchical modeling, see this
excellent (and quite technical) [article][hierarchical].

[hierarchical]: https://betanalpha.github.io/assets/case_studies/hierarchical_modeling.html

[^montecarlo]: Given a model of data and data to use for estimation, there are a variety
of techniques we could use to estimate the values–or, better, the distributions, of the
parameters. One set of methods uses numerical techniques to find the set of parameter values
that give the maximum likelihood of observing the data given the model. These then use
the local curvature of the maximum likelihood function to estimate uncertainties in the
parameters. The other option is to use “Monte-Carlo” techniques, which, more-or-less,
generate an ensemble of samples that can be used as an approximation
of the posterior distribution. For some models with large numbers of parameters, this can
be one of the only ways to estimate the parameters ina reasonable amount of time because
clever Monte-Carlo algorithms are very good at sampling efficiently, even in high-dimensional
spaces. An important additional benefit is that the results of the Monte-Carlo is an ensemble
of parameter values that can then be used to calculate dsitributions of *functions*
of those parameters. E.g., when we use the parameters in our election models to estimate
voter turnout in a given congressional district, we actually compute that for every ensemble
from our Monte-Carlo and the result is a distribution of possible voter turnout, including
useful confidence intervals. Also, since we compute things directly from the ensemble, the
effect of correlations is included in those results. For more about Monte-Carlo methods,
see [this article][montecarlo].

[montecarlo]: https://betanalpha.github.io/assets/case_studies/sampling.html

We have a couple of goals: understanding more about districts without being overburdened by local history,
so that we are able to see when a district might either be on the cusp of change (e.g., the old GA-7,
which almost flipped in 2018 and then did flip in 2020) and
spotting large mis-matches between our demographic expectations and election outcomes. This may indicate
a district where a better campaign or organizing efforts could make a large difference. We’re also particularly
interested in state-legislative districts.  Unlike U.S. House races, which,
when perceived as potentially close get analyzed and polled
over and over again, state-leg districts can seem like a poll- and data-free zone.

But even within  modest ambitions, the model has an assortment of weaknesses that we’d like to improve on
for the next cycle. These fall broadly into
four categories. We are ignoring some potentially important demographic variables,
most obviously age, but also religion, income, marital/parental status, and more detailed
breakdowns of race and ethnicity. We don’t adjust at all
for the current political climate, e.g., using presidential approval ratings or polls. We don’t adjust for
candidate quality or, in applying the model, to incumbency (though we do take incumbency into account
when fitting to past elections). Finally, we have made a number of choices within the model itself, using specific
distributions and priors and making choices about which parameters are hierarchical. While we’ve made some
attempts to check that our choices are reasonable, we could do much more.

For the remainder of this post, we’ll look at some congressional districts where the model gives
suprising results and see if that points us toward some improvements. As a starting point, let’s look
at the U.S. house districts where our model has the biggest mis-matches with the results of recent elections
(provided by the excellent [Dave’s Redistricting web-site][DavesR].)

[modelExplainer]: https://blueripple.github.io/explainer/model/ElectionModel/post.html
[CPSVRS]: https://www.census.gov/data/datasets/time-series/demo/cps/cps-supp_cps-repwgt/cps-voting.html
[CES]: https://cces.gov.harvard.edu
[DavesR]: https://davesredistricting.org/maps#aboutus

**NB: AK, DE, MT, ND, SD, VT and WY are missing since each has only one district and so no redistricting data was available.

# New Maps, New Dem Strategy: Our NC Analysis

**Updates 3/4/2022:** This post has been updated to reflect the [new NC maps][NCRuling] as well
as using a slightly newer version of our model.

[NCRuling]: https://www.nytimes.com/2022/02/23/us/politics/north-carolina-maps-democrats.html

The 2022 Congressional elections will involve new district maps in
all 50 states.
Districts will be reshaped, created, and eliminated,
which will change Dems’ odds in each race.
How can donors figure out which races have the biggest “bang for buck”
for flipping tenuous GOP seats or protecting vulnerable Dems?

To help answer that question, we’ve been building and refining a demographic
model that predicts Democratic lean in each district based on its
makeup in terms of race, sex, education, and population density.
We then compare those results to an existing model based on historical
data to help Dem donors identify races that we think deserve support
for “offense” or “defense”.

Update: As promised in our original post, as we improve the model[^modelChanges], we’ll update
these recommendations.

In this post, we’re focusing on North Carolina. Here’s what we’ll cover:

1. Dem-lean by district in NC: our demographic model vs. historical data (*updated*)
2. Districts worthy of Dem donor support
3. Coming next from Blue Ripple
4. Coda #1: Demographics of the new NC districts
5. Coda #2: Brief intro to our methods (for non-experts)

## 1. Dem-lean by district in NC: our demographic model vs. historical data

Our “demographic” model forecasts the potential Democratic lean of each
new district in NC based on attributes like race, education, age, and
population density. In the graph and table below,
we compare our predictions to a “historical” model (from the excellent
[Dave’s Redistricting (DR) web-site][DavesR]) built up from precinct-level
results in prior elections[^voteShare]. (See methods at the end of this post for more details.)
The axes show the projected 2-party Dem vote share with each model.
The diagonal line represents where districts would fall on this scatter-plot
if the two models agreed precisely. In districts to the left of the line,
our demographic model thinks the D vote share is higher than historical results,
and to the right of the line, we think it’s lower than the historical model predicts[^old].

[^old]: We’ve also done this modeling for the old districts and compared that
result to the actual 2020 election results. See [here][oldDistricts].

NB: For this and all scatter charts to follow, you
can pan & zoom by dragging with the mouse or moving the scroll wheel.  To reset the chart,
hold shift and click with the mouse.

[DavesR]: https://davesredistricting.org/maps#aboutus

[^voteShare]: One important note about the numbers. Dave’s Redistricting gives
estimates of Democratic candidate votes, Republican candidate votes and votes
for other candidates.  We’ve taken those numbers and computed 2-party vote share
for the Democratic candidate, that is, D Votes/(D Votes + R Votes). That makes it
comparable with the Demographic model which also produces 2-party vote share.

[^modelChanges]: Our initial model was a relatively simple Bayesian Monte-Carlo
estimation of binomial models for turnout and party preference in each of our
demographic categories. In particular, we did *not* use the state-specific information
in the data to inform the predictions in a particular state, instead pooling all
the data for fitting.
In the new version, we are using partial-pooling at the state level, allowing the
fitting procedure itself to decide how much to allow the state-level probabilities
to vary from the nationwide ones. We’ve switched from using a binomial model to
a beta-binomial, which we think is a better match for the data.
We’ve also introduced some ability for the distributions of coefficients to have
correlation structure, meaning that, for example, if within the data it was more likely
that in states with higher-than-average turnout of white voters there was also
higher-than-average turnout of Hispanic voters, the fit adjusts accordingly.
In addition we’ve replaced our use of the log of the population density with 10 quantiles
of population density. This is very similar but tames some odd results for extremely sparse
or dense districts, something we see more often at the State Legislative level.

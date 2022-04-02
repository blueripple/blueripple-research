# Arizona’s New State-Legislative Map

Like all states, Arizona is locking down new state-legislative maps,
which affect Democrat’s odds in 2022 races.
In this reshaped landscape,
how can donors focus their attention on races with the biggest “bang for buck”
for flipping tenuous GOP seats or protecting vulnerable Dems?

To help answer that question, we’ve continued to build and refine
a demographic model that predicts Democratic lean in each district based on its
makeup in terms of race, sex, education, and population density.
We then compare those results to an existing model based on historical
data to help Dem donors identify races that we think deserve support, either
for “offense” or “defense”.

This post focuses on the Arizona state-legislature districts.
We’ve done a similar analyses for Congressional districts
in [Texas][TXPost] and [North Carolina][NCPost].
Here’s what we’ll cover:

[TXPost]: https://blueripple.github.io/research/NewMaps/TX_Congressional/post.html
[NCPost]: https://blueripple.github.io/research/NewMaps/NC_Congressional/post.html

1. Dem-lean by district in AZ: our demographic model vs. historical data
2. Districts worthy of Dem donor support
3. Coming next from Blue Ripple
4. Coda #1: Demographics of the new AZ districts
5. Coda #2: Brief intro to our methods (for non-experts)

## 1. Dem-lean by district in AZ: our demographic model vs. historical data

The new AZ map has 30 State-Legislative districts. AZ is unusual in that
the upper and lower state-house seats use the same districts: one state senator
and two state representatives are elected in each district.
Our “demographic” model forecasts the potential Democratic lean of each
new district based on attributes like race, education, age, and
population density, with some adjustments to fit past results in each state.
In the table below,
we compare our predictions to a “historical” model (from the excellent
[Dave’s Redistricting (DR) web-site][DavesR]) built up from precinct-level
results in prior elections[^voteShare]. The “BR Stance” column uses a combination
of the models to classify districts into categories that we use to make donation
recommendations.
We also include significant overlaps (by population) with AZ’s congressional
districts and highlight those we think are competitive.
Given two equally appealing districts, we’d prefer to donate
to one where that money and work might also help in a competitive congressional district.
(See methods at the end of this post for more details.)

[DavesR]: https://davesredistricting.org/maps#aboutus

[^voteShare]: One important note about the numbers. Dave’s Redistricting gives
estimates of Democratic candidate votes, Republican candidate votes and votes
for other candidates.  We’ve taken those numbers and computed 2-party vote share
for the Democratic candidate, that is, D Votes/(D Votes + R Votes). That makes it
comparable with the Demographic model which also produces 2-party vote share.

One crucial note before we dive into the findings.  Our model is not meant to be predictive
and where we differ strongly from the historical model,
the historical model is likely more accurate.
We hope to unearth districts where the demographics tell
us that a different outcome is more likely than we might think.  This can cut both
ways: In AZ-27 our model sees D+7 in a place that is historically R+5. We think that makes
that district worth a longer look, especially if a strong candidate is running and/or there is
strong local organizing.  Conversely, in AZ-23, we see R+4 where the historical model sees D+8.
There, we think it might be important to make sure the D candidate is well supported because the
demographics might support a stronger than expected R performance.

Some quick observations before we look at the table:

- Our model is more “optimistic” than the historical model, seeing more D leaning
districts (17 vs 13)

- Our result in AZ-6 is obviously wrong. AZ-6 is (according to DRA) 60% Native American.
Our model, which doesn’t have
a specific race/ethnicity category for Native Americans,
is very wrong about their likely voter preference. Also,
we use population density as one
of our predictors and AZ-6, with the extraordinarily low density of
20 people/sq mile, is an outlier
and that may be making the results lean more R than is correct. We’re working
on it!  In the meantime, we would ignore that model result.

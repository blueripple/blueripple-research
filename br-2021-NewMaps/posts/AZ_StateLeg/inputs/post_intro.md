# AZ New State-Legislative Map

Like all states, Arizona is locking down new state-legislative maps,
which affect
Democrat’s odds in 2022 races.
In this reshaped landscape,
how can donors focus their attention on races with the biggest “bang for buck”
for flipping tenuous GOP seats or protecting vulnerable Dems?

To help answer that question, we’ve continued to build and refine
a demographic
model that predicts Democratic lean in each district based on its
makeup in terms of race, sex, education, and population density.
We then compare those results to an existing model based on historical
data to help Dem donors identify races that we think deserve support
for “offense” or “defense”.

This post focuses on Arizona. We’ve done a similar
analysis for Congressional districts in [Texas][TXPost] and
[North Carolina][NCPost] (though the NC map is in flux due to litigation and we
will update that post once a new map seems clear)
Here’s what we’ll cover:

[TXPost]: https://blueripple.github.io/research/NewMaps/TX_Congressional/post.html
[NCPost]: https://blueripple.github.io/research/NewMaps/NC_Congressional/post.html

1. Dem-lean by district in AZ: our demographic model vs. historical data
2. Districts worthy of Dem donor support
3. Coming next from Blue Ripple
4. Coda #1: Demographics of the new AZ districts
5. Coda #2: Brief intro to our methods (for non-experts)

## 1. Dem-lean by district in AZ: our demographic model vs. historical data

The new AZ map has 30 State-Legislative districts.  AZ is unusual in that
the upper and lower state-house seats use the same districts: one state senator
and two state representatives are elected in each district.
Our “demographic” model forecasts the potential Democratic lean of each
new district based on attributes like race, education, age, and
population density, with some adjustments to fit past results in each state.
In the table below,
we compare our predictions to a “historical” model (from the excellent
[Dave’s Redistricting (DR) web-site][DavesR]) built up from precinct-level
results in prior elections[^voteShare]. (See methods at the end of this post for more details.)

[DavesR]: https://davesredistricting.org/maps#aboutus

[^voteShare]: One important note about the numbers. Dave’s Redistricting gives
estimates of Democratic candidate votes, Republican candidate votes and votes
for other candidates.  We’ve taken those numbers and computed 2-party vote share
for the Democratic candidate, that is, D Votes/(D Votes + R Votes). That makes it
comparable with the Demographic model which also produces 2-party vote share.

One crucial note before we dive into the findings.  Our model is not meant to be predictive
and where we differ strongly from the historical model, we think the historical model is more
likely to be accurate.  But what we hope to unearth, are districts where the demographics tell
us that a different outcome is plausible or more likely than we might think.  This can cut both
ways: our model might see D+6 in a place that is historically R+5 (AZ-27) and we think that makes
that district worth a longer look, especially if a strong candidate is running and/or there is
strong local organizing.  Conversely, we may see R+3 where the historical model sees D+8 (AZ-23).
There, we think it might be important to make sure the D candidate is well supported because the
demographics would support an R win.

- Our model is more “optimistic” than the historical model, seeing more D leaning
districts (18)

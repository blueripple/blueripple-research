# Michigan’s New State-Legislative Map

Like all states, Michigan has new state-legislative maps,
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

This post focuses on the Pennsylvania state-legislature districts.
We’ve done a similar analyses for Congressional districts
in
[Arizona][AZPost],
[Georgia][GAPost],
[Michigan][MIPost],
[North Carolina][NCPost],
[New York][NYPost],
[Pennsylvania][PAPost],
and [Texas][TXPost].
as well as state-legislature districts in
[Arizona][AZSLD]
and [Pennsylvania][PASLD]
Here’s what we’ll cover:

[AZPost]: https://blueripple.github.io/research/NewMaps/AZ_Congressional/post.html
[GAPost]: https://blueripple.github.io/research/NewMaps/GA_Congressional/post.html
[TXPost]: https://blueripple.github.io/research/NewMaps/TX_Congressional/post.html
[NCPost]: https://blueripple.github.io/research/NewMaps/NC_Congressional/post.html
[NYPost]: https://blueripple.github.io/research/NewMaps/NY_Congressional/post.html
[PAPost]: https://blueripple.github.io/research/NewMaps/PA_Congressional/post.html
[MIPost]: https://blueripple.github.io/research/NewMaps/MI_Congressional/post.html
[AZSLD]: https://blueripple.github.io/research/NewMaps/AZ_StateLeg/post.html
[MISLD]: https://blueripple.github.io/research/NewMaps/MI_StateLeg/post.html
[PASLD]: https://blueripple.github.io/research/NewMaps/PA_StateLeg/post.html

1. Dem-lean by district in MI: our demographic model vs. historical data
2. Coda #1: Demographics of the new MI districts
3. Coda #2: Brief intro to our methods (for non-experts)

## 1. Dem-lean by district in MI: our demographic model vs. historical data
The MI legislature has 110 lower house seats
(the “Michigan House of Representatives”)
and representatives serve two-year terms.  So each seat is contested at every
election. The upper house (the “Michigan State Senate”) has 38 seats, with
senators serving 4-year terms; all of these seats are contested in *mid-term* years
and none in presidential election years. So all are up for grabs in this election.

Our demographic model forecasts the potential Democratic lean of each
new district based on attributes like race, education, age, and
population density, with some adjustments to fit past results in each state.
In the table below,
we compare our predictions to a “historical” model (from the excellent
[Dave’s Redistricting (DR) web-site][DavesR]) built up from precinct-level
results in prior elections[^voteShare]. The “BR Stance” column uses a combination
of the models to classify districts into categories that we use to make donation
recommendations.
We also include significant overlaps (by population) with MI’s congressional
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

There are 148 districts to analyze
but most are safe D or safe R.  We model every district but we only
present results for 56 “interesting” districts, ones where:

1. BR model sees the district as between R+5 and D+5 or
2. Dave’s Redistricting sees the district as between R+3 and D+3 or
3. BR model sees the district as D+5 or more but Dave’s sees it as R+3 or more or
4. BR model sees the district as R+5 or more but Dave’s sees it as D+3 or more.

We drop the rest of the districts because the tables would get too long and the charts too crowded.
This will make the correspondence between the BR model and the historical one look very tenuous,
mostly because we aren’t looking at the 92 districts where they strongly agree.
It’s also important to keep in mind that state-legislative districts are relatively
small (approximately 250,000 residents per state senate seat
and 85,000 residents per representative seat)
which makes the BR model more uncertain.

Our model is not meant to be predictive, here we differ strongly from the historical model,
the historical model is likely more accurate.
We hope to unearth districts where the demographics tell
us that a different outcome is more likely than we might otherwise think.

For an example of where the demographic and historical leans are very different,
consider lower house district 59. MI-L-59 is a relatively dense
(slightly under 2500 residents per square mile) majority White district,
whose voting age citizens are just over 8% non-White and about
37% college graduates. The model sees this as D+2, indicating that,
even allowing for the particularities of MI,
districts similar to this are D-leaning toss-ups.
But the voting history predicts R+13. Since this looks
out-of-reach historically, we wouldn’t recommend devoting resources to this
district in hopes of winning. But we do think it might be worth longer-term
investment, or at least some work understanding how the history of this
particular area is so different from demographic expectations. Why has
this density and level of education not led to a more competitive district?

In order to simplify the results we’ve split them into four categories:

1. “Both Models Close” lists districts where both models agree that the race is close. These are always good places for donors.
2. “Plausible Surprises” includes districts whiche are historically close and where our model suggests an outcome different
  from the historical expectation. These might be interesting as long-shot picks, bench-building or for purposes of defense in a tough year.
3. “Differences of degree” lists districts where our models agree but we think it’s less close than the historical model. We’re less interested
  in driving donations to these districts since we think they are safe or out of reach, especially in this cycle.
4. “Implausible Suprises” includes districts where we disagree strongly with the historical model but the difference is so large as to be more
  likely a place where the model is a bad fit and the historical model does not leave much room for a supriose outcome.

We ignore the remaining districts, ones where both models consider the district safe for either Ds or Rs. While there might be great
long term reasons to invest in candidates there, we don’t see the value in the near-term.  Even for bench-building work, we’d much rather see
it in districts on the “plausible suprises” list. Also, some categories in some states are empty, so we omit those tables altogether.

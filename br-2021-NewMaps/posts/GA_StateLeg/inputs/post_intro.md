# Georgia’s New State-Legislative Map

Like all states, Georgia has set new state-legislative
maps in response to the 2020 census,
and these affect Democrat’s odds in 2022 races.
In this reshaped landscape,
how can donors focus their attention on races with the biggest “bang for buck”
for flipping tenuous GOP seats or protecting vulnerable Dems?

To help answer that question, we’ve continued to build and refine
a demographic model that predicts Democratic lean in each district based on its
makeup in terms of race, sex, education, and population density.
We then compare those results to an existing model based on historical
data to help Dem donors identify races that we think deserve support, either
for “offense” or “defense”.

This post focuses on the Georgia state-legislature districts.
We’ve done a similar analyses for Congressional districts
in
[Arizona][AZPost],
[Michigan][MIPost],
[North Carolina][NCPost],
[Pennsylvania][PAPost],
and [Texas][TXPost].
as well as state-legislature districts in
[Arizona][AZSLD]
and [Pennsylvania][PASLD]
Here’s what we’ll cover:

[AZPost]: https://blueripple.github.io/research/NewMaps/AZ_Congressional/post.html
[TXPost]: https://blueripple.github.io/research/NewMaps/TX_Congressional/post.html
[NCPost]: https://blueripple.github.io/research/NewMaps/NC_Congressional/post.html
[PAPost]: https://blueripple.github.io/research/NewMaps/PA_Congressional/post.html
[MIPost]: https://blueripple.github.io/research/NewMaps/MI_Congressional/post.html
[AZSLD]: https://blueripple.github.io/research/NewMaps/AZ_StateLeg/post.html
[MISLD]: https://blueripple.github.io/research/NewMaps/MI_StateLeg/post.html
[PASLD]: https://blueripple.github.io/research/NewMaps/PA_StateLeg/post.html

1. Dem-lean by district in GA: our demographic model vs. historical data
2. Coda #1: Demographics of the new GA districts
3. Coda #2: Brief intro to our methods (for non-experts)

## 1. Dem-lean by district in GA: our demographic model vs. historical data
The lower house in GA (the “House of Representatives”) has 180 seats.
The upper house (the “Senate”) has 56 seats.  All seats are up for election
every two years.

Our “demographic” model forecasts the potential Democratic lean of each
new district based on attributes like race, education, age, and
population density, with some adjustments to fit past results in each state.
In the table below,
we compare our predictions to a “historical” model (from the excellent
[Dave’s Redistricting (DR) web-site][DavesR]) built up from precinct-level
results in prior elections[^voteShare]. The “BR Stance” column uses a combination
of the models to classify districts into categories that we use to make donation
recommendations.
We also include significant overlaps (by population) with GA’s congressional
districts and highlight those we think are competitive.
Given two equally appealing districts, we’d prefer to donate
to one where that money and work might also help in a competitive congressional district,
what we call a “double word score”. Since there is also a crucial Governor’s
race–Stacey Abrams is trying to unseat Brian
Kemp–and a federal Senate seat in
play–Raphael Warnock is trying to hold his seat against Herschel Walker,
each of these is more like a quadruple word score.
(See methods at the end of this post for more details.)

[DavesR]: https://davesredistricting.org/maps#aboutus

[^voteShare]: One important note about the numbers. Dave’s Redistricting gives
estimates of Democratic candidate votes, Republican candidate votes and votes
for other candidates.  We’ve taken those numbers and computed 2-party vote share
for the Democratic candidate, that is, D Votes/(D Votes + R Votes). That makes it
comparable with the Demographic model which also produces 2-party vote share.

There are 236 seats to analyze but most are safe D or safe R.
We model every district but we only
present results only for 42 “interesting” districts, ones where:

1. The BR model sees the district as between R+5 and D+5 or
2. Dave’s Redistricting sees the district as between R+3 and D+3 or
3. The BR model sees the district as D+5 or more but Dave’s sees it as R+3 or more or
4. The BR model sees the district as R+5 or more but Dave’s sees it as D+3 or more.

We drop the rest of the districts because the tables would get too long and the charts too crowded.
This will make the correspondence between the BR model and the historical one look very tenuous
but that’s mostly because we aren’t looking at the 168 districts where they strongly agree.
It’s also important to keep in mind that state-legislative districts are relatively
small (about 200,000 residents per state senate seat and 60,000 residents per representative seat)
which makes the BR model more uncertain.

Our model is not meant to be predictive and where we differ strongly from the historical model,
the historical model is likely more accurate. We hope to unearth districts where the demographics tell
us that a different outcome is more likely than we might otherwise think.

Across nearly all the “interesting” districts in GA, we think there is more potential for
Dems than the historical voting patterns suggest. Our model is responding to a
combination of relatively high levels of education among the White voters in majority White
districts–most of these districts have over 50% college graduates among
voting-age-citizens–and relatively high densities–most of these districts have more
than 1000 people per square mile. Both of these demographic factors usually lead
to more D-leaning districts. So what is happening in GA?

There could be many explanations: perhaps the same things that lead to more D voting in
other states operate differently in GA, and college-educated White voters in moderate
density towns are much more heavily Republican than in other places. It’s also possible
that there are GA-specific turnout/voter-suppression issues that
skew the results in these districts.

In federal elections, GA has recent history of previously R districts flipping
when D candidates recognized latent potential: Lucy McBath flipped the old GA-6
in 2018 and and Carolyn Bordeaux flipped the old GA-7 in 2020.
So perhaps our model is just seeing this same
pattern and there is potential for Dem upside in a lot of these districts.

[^raceOther]: It’s statistically and politically tricky to decide how to categorize people. If we
had more data, we’d love to add more race/ethnicity categories. In practice, we are limited both by
the categories provided by our data sources and the limited numbers of people surveyed overall,
which means groups composing small fractions of the U.S. population are often not surveyed in
every state and definitely no in every state, age and education group. So we make the common
choice of splitting our race/ethnicity categories into
Black, Asian, Hispanic, White-non-Hispanic and other.

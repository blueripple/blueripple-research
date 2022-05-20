# Pennsylvania’s New State-Legislative Map

Like all states, Pennsylvania has new state-legislative maps,
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
[Michigan][MIPost],
[North Carolina][NCPost],
[Pennsylvania][PAPost],
and [Texas][TXPost].
as well as state-legislature districts in
[Arizona][AZSLD]
and [Michigan][MISLD]
Here’s what we’ll cover:

[AZPost]: https://blueripple.github.io/research/NewMaps/AZ_Congressional/post.html
[TXPost]: https://blueripple.github.io/research/NewMaps/TX_Congressional/post.html
[NCPost]: https://blueripple.github.io/research/NewMaps/NC_Congressional/post.html
[PAPost]: https://blueripple.github.io/research/NewMaps/PA_Congressional/post.html
[MIPost]: https://blueripple.github.io/research/NewMaps/MI_Congressional/post.html
[AZSLD]: https://blueripple.github.io/research/NewMaps/AZ_StateLeg/post.html
[MISLD]: https://blueripple.github.io/research/NewMaps/MI_StateLeg/post.html
[PASLD]: https://blueripple.github.io/research/NewMaps/PA_StateLeg/post.html

1. Dem-lean by district in PA: our demographic model vs. historical data
2. Coda #1: Demographics of the new PA districts
3. Coda #2: Brief intro to our methods (for non-experts)

## 1. Dem-lean by district in PA: our demographic model vs. historical data
The new PA map has 203 lower house seats (the “Pennsylvania House of Representatives”)
and representatives serve two-year terms.  So each seat is contested at every
election. The upper house (the “Pennsylvania State Senate”) has 50 seats, with
senators serving staggered 4-year terms; half of these seats are contested in
each two-year cycle. In 2022, the even-numbered senate seats are up for grabs.

Our “demographic” model forecasts the potential Democratic lean of each
new district based on attributes like race, education, age, and
population density, with some adjustments to fit past results in each state.
In the table below,
we compare our predictions to a “historical” model (from the excellent
[Dave’s Redistricting (DR) web-site][DavesR]) built up from precinct-level
results in prior elections[^voteShare]. The “BR Stance” column uses a combination
of the models to classify districts into categories that we use to make donation
recommendations.
We also include significant overlaps (by population) with PA’s congressional
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

There are 253 seats to analyze (though only 228 are contested in this election)
but most are safe D or safe R.  We model every district but we only
present results for 60 “interesting” districts, ones where:

1. BR model sees the district as between R+5 and D+5 or
2. Dave’s Redistricting sees the district as between R+3 and D+3 or
3. BR model sees the district as D+5 or more but Dave’s sees it as R+3 or more or
4. BR model sees the district as R+5 or more but Dave’s sees it as D+3 or more.

We drop the rest of the districts because the tables would get too long and the charts too crowded.
This will make the correspondence between the BR model and the historical one look very tenuous
but that’s mostly because we aren’t looking at the 168 districts where they strongly agree.
It’s also important to keep in mind that state-legislative districts are relatively
small (250,000 residents per state senate seat and 60,000 residents per representative seat)
which makes the BR model more uncertain.

Our model is not meant to be predictive and where we differ strongly from the historical model,
the historical model is likely more accurate. We hope to unearth districts where the demographics tell
us that a different outcome is more likely than we might otherwise think.

For an example of where the demographic and historical lean are very different,
consider district 112 in the House of Representatives (“PA-L112” in our charts).
This is a medium-density (990 people per square mile), very White district with
a fairly low percentage of college-graduates (27%). That is, PA-112 is a very
“White Working Class” district. So our model thinks it is
strongly R (R+12). But historically it’s D+9. If any of our readers know local
PA politics, we would love to know what is working so well for Dems in PA-112!
When the demographic and historical model differ this strongly, we think the
historical model is going to be a better predictor.  The discrepancy here does
point us to an interesting district, but it doesn’t mean we think that district
is going to go red in this cycle.

Some notes to specific to PA:

- Our model struggles to predict the voter turnout and preference
  of voters whose race/ethnicity we classify as “Other”,
  which is other than Black, Hispanic, Asian and White-non-Hispanic[^raceOther].
  There are not a lot of such voters in the survey data which results in large uncertainties. For
  districts where such voters are a larger fraction of the population (e.g., PA-189, PA-115).
  We are not suprised to see the model diverge strongly from the historical result and we would trust
  history over our model, especially in a safe district.

- Our model is somewhat more pessimistic about Hispanic voter preference than the historical model
  we’re comparing to here.  That might be because we use only 2020 data and the typical
  historical model uses the past 2 or 3 elections. This shifts our estimate downwards in any
  district which has a large number of Hispanic voters, something true in several PA districts.
  In this case, what you think is correct depends on whether you think the Dems can return to
  their pre-2020 levels of support from Hispanic voters or instead that 2020 is an indication
  of things to come. This is also playing a role in our much-too-low estimate in PA-115.

[^raceOther]: It’s statistically and politically tricky to decide how to categorize people. If we
had more data, we’d love to add more race/ethnicity categories. In practice, we are limited both by
the categories provided by our data sources and the limited numbers of people surveyed overall,
which means groups composing small fractions of the U.S. population are often not surveyed in
every state and definitely no in every state, age and education group. So we make the common
choice of splitting our race/ethnicity categories into
Black, Asian, Hispanic, White-non-Hispanic and other.

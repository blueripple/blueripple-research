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
in [Texas][TXPost] and [North Carolina][NCPost].
Here’s what we’ll cover:

[TXPost]: https://blueripple.github.io/research/NewMaps/TX_Congressional/post.html
[NCPost]: https://blueripple.github.io/research/NewMaps/NC_Congressional/post.html

1. Dem-lean by district in PA: our demographic model vs. historical data
2. Coming next from Blue Ripple
3. Coda #1: Demographics of the new AZ districts
4. Coda #2: Brief intro to our methods (for non-experts)

## 1. Dem-lean by district in PA: our demographic model vs. historical data
The new PA map has 203 lower house seats (the “Pennsylvania House of Representatives”)
and representatives serve two-year terms.  So each seat is contested at every
election. The upper house (the “Pennsylvania State Senate”) has 50 seats, with
senators serving staggered 4-year terms so half of these seats are contested in
each two-year cycle. In 2022, only the even-numbered senate seats are up for grabs.

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

There are 253 seats to analyze (though only 228 are contested in this election)
but most are safe D or safe R.  We model every district but we only
present results for “interesting” districts:

    1. Either model sees the district as between R+5 and D+5.
    2. One model sees the district as D+5 or higher and the other as R+5 or higher.

We drop the rest of the districts because the tables would get too long and the charts too crowded.

Our model is not meant to be predictive
and where we differ strongly from the historical model,
the historical model is likely more accurate.
We hope to unearth districts where the demographics tell
us that a different outcome is more likely than we might otherwise think.

For a good example of how the demographics tell us interesting things but are clearly
insufficient, consider two lower house districts: PA-2 and PA-79.  Both are more than 85% white
and have relatively low college graduation rates.  But both are also relatively dense, around
2000 people per square mile. In the demographic model, these look similar: R leaning toss-ups.
But historically, PA-79 is very safe R (R+15) and PA-2 is safe D (D+8). Is either vulnerable to
the other party in this cycle? Our model suggests a longer look if an incumbent is
stepping down or and/or a strong challenger is running.

Some notes to specific to PA:

- Our model struggles to predict the voter turnout and preference
  of voters whose race/ethnicity is other than Black, Hispanic, Asian and White-non-Hispanic.
  There are not a lot of such voters in the survey data which results in large uncertainties. For
  districts where such voters are a larger fraction of the population (e.g., PA-74, PA-189, PA-115).
  We are not suprised to see the model diverge strongly from the historical result and we would trust
  history over our model, especially in a safe district.

- Our model is somewhat more pessimistic about Hispanic voter preference than the historical model
  we’re comparing to here.  That might be mostly because we use only 2020 data and the typical
  historical model uses the past 2 or 3 elections. This shifts our estimate downwards in any
  district which has a large number of Hispanic voters, something true in several AZ districts.
  In this case, what you think is correct depends on whether you think the Dems can return to
  their pre-2020 levels of support from Hispanic voters or instead that 2020 is an indication
  of things to come. This is also playing a role in our estimates in PA-115.

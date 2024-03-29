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
We’ve done a similar analyses for Congressional districts in
[Arizona][AZPost],
[Michigan][MIPost],
[North Carolina][NCPost],
[Pennsylvania][PAPost],
and [Texas][TXPost].

Here’s what we’ll cover:

[AZPost]: https://blueripple.github.io/research/NewMaps/AZ_Congressional/post.html
[TXPost]: https://blueripple.github.io/research/NewMaps/TX_Congressional/post.html
[NCPost]: https://blueripple.github.io/research/NewMaps/NC_Congressional/post.html
[PAPost]: https://blueripple.github.io/research/NewMaps/PA_Congressional/post.html
[MIPost]: https://blueripple.github.io/research/NewMaps/MI_Congressional/post.html
[AZSLD]: https://blueripple.github.io/research/NewMaps/AZ_StateLeg/post.html
[MISLD]: https://blueripple.github.io/research/NewMaps/MI_StateLeg/post.html
[PASLD]: https://blueripple.github.io/research/NewMaps/PA_StateLeg/post.html

1. Dem-lean by district in AZ: our demographic model vs. historical data
2. Coda #1: Demographics of the new AZ districts
3. Coda #2: Brief intro to our methods (for non-experts)

## 1. Dem-lean by district in AZ: our demographic model vs. historical data

The new AZ map has 30 State-Legislative districts. AZ is unusual in that
the upper and lower state-house seats use the same districts: one state senator
and two state representatives are elected in each district.
Our “demographic” model forecasts the potential Democratic lean of each
new district based on the district makeup in terms of race/ethnicity,
educational-attainment, and population density. The model uses
national-level and state-level data
(from surveys and 2020 election results), using the national data
to bolster the local when data there is sparse.

In the tables below,
we compare our predictions to a “historical” model (from the excellent
[Dave’s Redistricting (DR) web-site][DavesR]) built up from precinct-level
results in prior elections[^voteShare].

In order to simplify the results we’ve split them into four categories:

1. *Both Models Close* lists districts where both models agree that the race is close. These are always good places for donors.
2. *Plausible Surprises*–there are none of these in AZ this year!–
   includes districts which are historically close and where our model suggests an outcome different
  from the historical expectation. These might be interesting as long-shot picks, bench-building or for purposes of defense in a tough year.
3. *Differences of Degree* lists districts where our models agree but we think it’s less close than the historical model. We’re less interested
  in driving donations to these districts since we think they are safe or out of reach, especially in this cycle.
4. *Implausible Suprises* includes districts where we disagree strongly with the historical model and the difference is so large as to be more
  likely a place where the model is a bad fit. The historical model does not leave much room for a suprise outcome. This includes places where
  our models agree on the likely outcome but the historical model is safe enough that we don’t think our model should be cause for concern.

We ignore the remaining districts, ones where both models consider the district safe for either Ds or Rs. While there might be great
long term reasons to invest in candidates there, we don’t see the value in the near-term.  Even for bench-building work, we’d much rather see
it in districts on the “plausible suprises” list. Also, some categories in some states are empty, so we omit those tables altogether.

The “BR Stance” column uses a combination
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
us that a different outcome is more likely than we might otherwise think.
This can cut both ways:
In AZ-14 our model sees a tossup in a place that is historically R+10. We think that makes
that district worth a longer look, especially if a strong candidate is running and/or there is
strong local organizing.  Conversely, we see AZ-18 as a tossup but the historical model sees D+10.
There, we think it might be important to make sure the D candidate is well supported because the
demographics might support a stronger than expected R performance.

Some notes specific to AZ:

- Our model struggles to predict the voter turnout and preference
  of voters whose race/ethnicity is other than Black, Hispanic, Asian and White-non-Hispanic.
  There are not a lot of such voters in the survey data which results in large uncertainties
  in the model. For
  districts where such voters are a large fraction of the population (e.g., AZ-6), we are not
  suprised to see the model diverge strongly from the historical result and we would trust
  history over our model, especially in a safe district.

- Our model is somewhat more pessimistic about Hispanic voter preference than the historical model
  we’re comparing to here.  That might be mostly because we use only 2020 data and the typical
  historical model uses the past 2 or 3 elections. This shifts our estimate downwards in any
  district which has a large number of Hispanic voters, something true in several AZ districts.
  In this case, what you think is correct depends on whether you think the Dems can return to
  their pre-2020 levels of support from Hispanic voters or instead that 2020 is an indication
  of things to come.

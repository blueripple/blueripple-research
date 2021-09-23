# Modeling State House Races: VA edition

We’ve
[written before](https://blueripplepolitics.org/blog/state-races-2019)
about the importance of state legislative elections and,
if anything, the situation for Dems and progressives has only become more
urgent since then. Republican control of state-houses has opened the door to partisan and race-based
[gerrymandering](https://www.washingtonpost.com/news/wonk/wp/2015/03/01/this-is-the-best-explanation-of-gerrymandering-you-will-ever-see/)
and
[voter suppression](https://www.aclu.org/issues/voting-rights/fighting-voter-suppression),
which allows the Republicans to hold state-houses and a disproportionate share of the
U.S. house. In addition,
on hundreds of substantive issues, including
[abortion access](https://www.washingtonpost.com/politics/2021/09/01/texas-abortion-law-faq/),
[COVID-safety](https://apnews.com/article/health-government-and-politics-coronavirus-pandemic-michigan-laws-eeb73e92d5af8b46f6a1e70d8a5cbe81),
[Medicaid expansion](https://apnews.com/article/wisconsin-medicaid-business-health-government-and-politics-1ab60e341674584c3059511d35ec7c21),
and [education policy](https://thehill.com/changing-america/respect/equality/558927-texas-passes-law-banning-critical-race-theory-in-schools),
Republican control is an ongoing disaster for Democratic policy positions and, more
importantly, for the people of these states.

Virginia is the rare
[good-news story](https://slate.com/news-and-politics/2019/11/democrats-win-virginia-legislature.html)
in this regard: the state’s House of Delegates (lower house) and Senate went blue in 2019,
after 20 years of Republican control
(VA state-house elections occur in odd-numbered years).
We’d like to keep it that way–which is why we are trying
to figure out the highest-impact races to support this fall.

But modeling State-legislative elections to figure out which ones need the most help from
donors is tricky. That’s mainly because the data are extremely challenging:
polling is almost non-existent
and detailed demographic information gets more difficult to find for smaller regions,
making any kind of forecasting difficult.

Despite the challenges, we’ll do our best to identify the upcoming
Virginia state legislature races that the Dem candidate *can* win,
but is *not certain* to win. Below we’ll look at our
first attempt to model expected closeness of elections in the VA lower house.
In this post we’ll feed our model 2018 data and compare the predictions to
the 2019 election outcome.
To be very clear: we’re not interested in predicting outcomes. We are interested in
determining which races are likely to be close and are thus flippable or in need of defending.

### Key Points In This Post

- Using voter-turnout and voter-preference data we can model expected
turnout and preference for specific state-legislative-districts (SLDs).
- Demographic information for state-legislative districts (SLDs) is available from the
ACS (American Community Survey) and the decennial census, but there is “some assembly required.”
- Combining these pieces of information allows us to estimate
a range of likely outcomes for an SLD election based on intrinsic factors, without having to include any
history of local election results.
- Our test model, which compares forecasted results using 2018 data to 2019 results,
is extremely encouraging, so we are excited to apply it to the upcoming 2021 races!

## How We Modeled State Legislature Election Results from Demographic Information
When people use election-data and demographics to
craft new districts[^redistricting]
for congressional
or state legislative elections, they focus almost entirely on past election results:
breaking past results down to their smallest available geographies, usually
“precincts”, further splitting those into census blocks, then reassembling
them in different ways to build new districts with predictable political leanings.
We don’t doubt the accuracy of these methods, especially given the financial and
computational resources given over to them during redistricting.

[^redistricting]: See, for example
[this](https://districtr.org) free redistricting tool. Or
[this](https://www.districtbuilder.org).  Information about all
things redistricting is available
[here](https://redistricting.lls.edu).

But what if past results are inconsistent with what we might expect from a
purely demographic model?  While we expect the past-results model to
be highly predictive, we also believe that mismatches between past results
and intrinsic demographics might be extremely informative.
These differences might highlight the places
where the right candidate, campaign or organizing
effort might produce a surprising result by activating Dem-leaning voters,
or alerting us to districts
where an incumbent Democrat might need more help defending a seat than
one might expect from past results alone.

To do this,  we’re going to model state-legislative elections using only
demographic variables: population density,
sex (female or male are the only classifications in the data we have),
education level (non-college-grad or college-grad),
and race (Black, Latinx, Asian, white-non-Latinx, other).
We would very much like to have an age factor as well but the tables
made available by the census at the SLD level preclude this[^whyNoAge].

We’ve built very similar models before, most-recently to look at the
[turnout gap between voters-of-color and white voters](https://blueripple.github.io/research/Turnout/StateSpecific1/post.html)
. So, for more details on our
[demographic and turnout data](https://blueripple.github.io/research/Turnout/StateSpecific1/post.html#intro-1-our-datathe-american-community-survey-and-the-cps-voting-and-registration-supplement),
or
[multi-level-regression and post-stratification (MRP)](https://blueripple.github.io/research/Turnout/StateSpecific1/post.html#intro2-mrpa-quick-primer),
please click the links above.

This model has a few key differences from what we did before:

- In addition to modeling voter turnout, we also model voter preference
and we build those models jointly since there are likely correlations
between turnout and preference that might get lost in separate models[^jointModel].

- We need demographic information for each SLD which is harder to
assemble from census data than the demographics of. e.g.,
a congressional district.

[^jointModel]: Imagine, for example, that college-educated Asian voters
are more likely to vote for Dems.  But that in the places
where their turnout is highest, their preference for Dems is lowest.
If we modeled turnout and preference separately, we would miss this correlation,
and estimate a higher number of Dem votes than we should in places
where we expect high turnout among college-educated Asian voters, and
a lower number of Dem votes than we should in places were we expect
low turnout of those voters.

We assemble SLD-level demographic information using census provided
[shapefiles](https://en.wikipedia.org/wiki/Shapefile)
for each district. The shapefile is used to find
all the census block-groups inside the district and those are
aggregated[^demographicCode] to construct SLD-level demographic
breakdowns of population density, sex, education and race.

A complication to our approach is that our favored source for turnout data, the census
bureau’s Current Population Survey Voting and Registration Supplement
([CPSVRS](https://www.census.gov/data/datasets/time-series/demo/cps/cps-supp_cps-repwgt/cps-voting.html)),
has no data about who voters chose in the election, just whether or not they
voted.  To get voter preference information we use
the Cooperative Election Survey
([CES](https://cces.gov.harvard.edu)) which does the work of validating
survey respondents self-reported turnout with voter files[^whyCPS].  We use
each voter’s party choice for their U.S. House district as a proxy for
their likely vote for state legislature.

[^whyCPS]: The CES also has turnout information and it
has the advantage of being validated.  But that comes with
[its own issues](https://agadjanianpolitics.wordpress.com/2018/02/19/vote-validation-and-possible-underestimates-of-turnout-among-younger-americans/).
We generally run our estimations with both CPSVRS and CES as
a turnout source to make sure the results are similar but rely
on a
[slightly-corrected](https://www.aramhur.com/uploads/6/0/1/8/60187785/2013._poq_coding_cps.pdf)
version of the CPSVRS, as that seems to be the most common approach.

Our model combines those data sets at the congressional district level
and jointly estimates turnout probabilities using counts of voters
from the CPSVRS and
counts of Dem votes for U.S. House from the CES.
We then post-stratify the estimates across
the demographic data we built for each VA lower house district. The result is
a prediction for the expected Dem vote share in each district. More detail
about the model and data-sources can be found [here][model_description].

Cutting to the chase: in the chart below we plot the model estimate
(using 2018 data) vs. the results of the 2019 election. In blue,
we also plot the “model=result” line,
where every dot would fall if the model was exactly predictive of each race.
The
[R-squared](https://en.wikipedia.org/wiki/Coefficient_of_determination)
value indicates that our model explains 74% of the variance
among contested races[^uncontested].

[^uncontested]: Races uncontested by one of the major parties fall on the sides of the chart
(except HD-63, where a
[well-known](https://ballotpedia.org/Larry_Haake)
local independent
mounted a strong challenge to the incumbent Dem)
and we can see that these are predictably one-sided in the model,
with the exception of district
78 (a swingy district that was uncontested by the Democrats).

[^electionModel]: See, for example,
[this description](https://hdsr.mitpress.mit.edu/pub/nw1dzd02/release/1)
of the Economist magazine’s U.S. presidential election forecast.

[^whyNoAge]: For larger geographic areas
it’s possible to get ACS “micro-data,”
which provides specific information for many factors.
The smallest of these, Public-Use-Microdata-Areas
or “PUMA”s, contain about 100,000 people which is too big
to use to get the demographics of a SLD.
Census block-groups only contain a few thousand people, but for those
micro-data is not available.
We’re using census tables and they
do not provide 4-factor breakdowns.
So we we’re limited to sex, education and
race. Once additional 2020 decennial data is available,
we may be able to improve on this.

[^demographicCode]: We built a
[python script](https://github.com/blueripple/GeoData/blob/main/code/aggregateRaw.py)
to automate most
of this process. We download shapefiles and block-group-data for the
state and the script merges those into SLD-level demographics.  The
code is available on our
[github site](https://github.com/blueripple)

[^modelDetails]: We’ve written a much more detailed
description of the model
[here]()

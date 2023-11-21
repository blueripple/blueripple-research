{-# LANGUAGE QuasiQuotes #-}

module ModelNotes where

import Data.String.Here (here)

part1 :: Text
part1 = [here|
# Efficient Giving to State Legislative Elections
Democratic donors are more interested than ever in state-legislative races. So
it’s increasingly important to know how to allocate money among those races.

Ideally, knowing as much as possible about the competitiveness
of each seat, especially in states where control of, or a supermajority in, a chamber
is potentially up for grabs,
allows us to focus our efforts on the ones where we can have the greatest “bang for the buck.”

In the first part of this post, we’ll discuss what we think our modeling and data-driven approaches
can contribute to this analysis and the refinement and framing of slates of races for donors.
In the second part, we’ll dig into the technical details of our model.

### It’s Hard to Analyze State Legislative Races!

Local expertise in a particular district or set of districts is often
the best way to figure out which races are close-but-winnable.
However, such knowledge is often hard to find and it’s useful to have ways of
looking at state-legislative districts that we can
apply to the state or country as a whole.

Polling would be extremely useful, but polls are expensive and
state-legislative-races don’t usually generate enough news interest
to field a poll. Additionally, there’s less information available about
the demographic characteristics of the people who live in geographic
regions as small as a state-legislative district. This makes the work of
weighting a poll–adjusting the responses you get to
estimate the responses you would’ve gotten from a representative sample–quite
difficult.

That leaves us with various options for using historical data to help Democratic donors
figure out which state legislature candidates to support. The primary strategy
is to use historical results to identify “close” races.
We think our modeling approach–which incorporates data on the recent turnout and
partisan lean of various demographic groups–provides an extra set of helpful
tools for looking at these races.

### Past Partisan Lean: Let history be your guide
The most straightforward way to find close-but-winnable races is to look at what happened
in previous elections, either for the same districts or statewide.
[Dave’s Redistricting](https://davesredistricting.org/maps#home)
does a spectacular job of joining district maps and precinct-level data from previous
elections to create an estimate of the past-partisan-lean[^ppl] (PPL) of every
state-legislative-district in the country. Their rich interface allows the user to choose various
previous elections and combinations of them to estimate the partisan lean.

[^ppl]: By “partisan lean” we mean the 2-party vote share of Democratic votes, that
is, given $D$ democratic votes and $R$ Republican votes,
we will report a partisan lean of $\frac{D}{D+R}$, ignoring the third-party votes.

As an example, here is chart of the PPL in the VA house[^pplVA], the lower
chamber of the VA state-legislature. As with many such maps,
it looks mainly Republican (Red) but that is because the
districts with Democratic leaning PPL are often geographically smaller, in
places, like cities, with higher population density. As the 2023 election shows, VA
has slightly more D leaning districts than R leaning ones.

[^pplVA]: Using 2020 ACS population estimates and a composite of presidential and statewide
elections from 2016-2021: 2018 and 2020 senate as well as Governor and AG from 2021.
|]
part1b :: Text
part1b = [here|
However, this backward-looking approach doesn’t help us find contests
where focused efforts might shift the outcome toward Democrats or be needed to protect a previously
safe seat.
|]

part2 :: Text
part2 = [here|
The limitation of PPL is that it tells you nothing about *why* a district has the lean it does.
For that you need local knowledge and/or some analysis of the demographics.
You can look at the demographic composition of a district and make some educated guesses
but it’s useful to do that more systematically, via a detailed demographic model applied to
robust estimates of the demographic composition of each district.

This sort of analysis can help identify opportunities and vulnerabilities for Democrats.
District PPLs inconsistent with their location and demographic makeup may
be opportunities to spot a possibly flippable or safe-looking-but-vulnerable district
or uncover districts that might be ruled out as donation targets because the underlying
demographics make them likely less close than history suggests.

For example, imagine a state-legislative-district with PPL just below some cutoff for winnable.
A demographic analysis shows that its “expected” partisan-lean is over 50%.
Might that be a district where a good and well-resourced candidate can win a race or,
in losing a close race, change the
narrative going forward? Might it be useful to be alerted to a district which looks
easily winnable by PPL standards but with “expected” partisan lean much closer to 50%?
That district might be vulnerable to the right opposition candidate,
especially in a tough political environment.

### Demographic Partisan Lean: Using the demographics of state legislative districts
Rather than consider how people in a specific place have voted in previous elections,
Demographic Partisan Lean (DPL) instead categorizes people demographically, in our case
by state, age, sex, educational-attainment, race/ethnicity, and population-density[^buckets].
Using large surveys of turnout and party-preference
we model expected turnout and party-preference for each of those categories.
Then, given the numbers of people in each of those categories in a district,
we combine them to compute
the (modeled) partisan lean among expected voters. Here’s what this looks like in VA:

[^buckets]: For age we use five categories: 18-24, 25-34, 35-44, 45-64, and 65 and over. For sex we use two: male and female.
Almost none of our source data tracks sex in a more fine-grained way. For educational attainment,
we use four categories: non high-school graduate, high-school graduate, some college, and college-graduate.
And for race/ethnicity we use five categories:
Black, Hispanic, Asian American/Pacific Islander (AAPI), White-Non-Hispanic and Other.
With all of these categories there would be value to finer but
that would also create computational and data difficulties.]
|]


{-
However, a major drawback of PPL is that it doesn’t help much with scenario analysis.
E.g., what would happen in a
given seat if women turned out at higher rates and were more likely to vote
for the democratic candidate? With a demographic
breakdown and model of turnout and partisan lean we can estimate these
sorts of effects.
-}


part3 :: Text
part3 = [here|
The maps of PPL and DPL are, unsurprisingly, very similar but there are some large differences which
are clearer on the chart of the difference (DPL - PPL) below.
|]

part3b :: Text
part3b = [here|
We can see
that there are a few districts which might be interesting to look at. This is clearer
in table form: below we list some districts which are not close in PPL but, when looked at
demographically, *ought* to be close.
These are districts that might be flippable or look like safe seats but need defending.
It’s not that these seats *are* flippable (or in need of extra defense) but that they might
be worth a second look to figure out why they have been voting in ways so different from what we
might expect based on the demographics.
|]

part4 :: Text
part4 = [here|
A quick note: We did this analysis pre-election and all of the contested districts in the list above played
out pretty much as history (PPL) would suggest. For example, House district 52 (Lower-52) was won by the R candidate
55-45. Looking at our model, we see a district right between the median R and D districts in terms of population
density and %voters-of-color and slightly closer to the median R district in terms of the % of white voters
who have graduated from college. So why did our model think this might be a D district? The voters in it are significantly
*younger* than a typical district. Almost 60% of the voters are under 45. The model, even in VA, suggests that younger
voters are significantly more D leaning. It might be interesting to try to campaign in this district with that
in mind.

One way to think of DPL is as a very detailed analysis of
voting patterns based on race or education or age. Each of those can be a valuable predictor
of turnout and party-preference. But sometimes the combination of categories is essential
for understanding. For example, the Republican lean of white-non-college educated voters
is greater than you expect from the Republican lean of white voters and the Republican lean
of non-college-educated voters combined. This gets even more complex and harder to keep track of
once you add age and sex. All of these things may vary from state to state. Population density
affects party-preference quite strongly in ways not well captured by any of those categories.

DPL is the partisan lean of a district if,
within each demographic group, each person’s choice to vote and whom to vote for
was more or less the same as everyone else in the state living at similar population density.
This is less
*predictive* than knowing who those same people voted for in the previous few elections. But
it’s different information, and particularly interesting when it’s inconsistent with
the PPL.

### Scenario Analysis: Asking “what if...?” questions
Since the DPL is built from an estimate of who lives in a district and how likely each of them is
to turn out and vote for the Democratic candidate, we can use it to answer some
“what would happen if...” sorts of questions. If you imagine some voters are
more energized and thus likely to vote and/or more likely to vote for the Democratic
candidate, that might change which seats are in play and which are safe. For example,
suppose we think the Dobbs decision overturning Roe v. Wade will raise turnout among women
by 5% and also pushes their party preference 5 points towards Democratic candidates[^scenario].
What would this mean for the 20 closest (by PPL) house districts in VA?

[^scenario]: A technical note: we don't actually move the probabilities by 5% (or whatever) for a couple of reasons.
We don't want to end up with probabilities above 1 or below 0 which could happen with larger shifts and/or
probabilities already closer to 0 or 1. And, intuitively, very low and very high probabilities are likely to
shift less than probabilities closer to 50%. So we shift using the logistic function in such a way that
for a shift of $x$, we would shift a probability of $\frac{1}{2} - \frac{x}{2}$ to $\frac{1}{2} + \frac{x}{2}$
but smoothly apply slightly smaller shifts as the probability moves away from $\frac{1}{2}$.
|]

part4b :: Text
part4b = [here|
We can see that it makes a bunch of safe R districts into Lean R and a couple of Lean D become safe.
This would be helpful when advising donors or allocating donor funds.

Of course, you don’t need any sort of model to figure out that shifting the
turnout and preference of female voters by 5% would shift the resulting vote share
by a bit more than 2.5%. Women make up slightly more than half the electorate in most
districts so a 5 point preference shift among women will be a slightly more than 2.5 point
shift in vote share, with another slight boost coming from the turnout shift.

But what if you thought the preference shift was only among women with a college degree?
This is a tricker thing to map out in VA. Here’s the same table but with that scenario:
|]

part4c :: Text
part4c = [here|
In this case the shift varies from under one point to over 1.5 points, which, using our example,
makes HD-69 and HD-30 competitive (but not HD-49) and pushes HD-97 into safe territory.

This might also be useful when considering a targeted intervention. E.g., how much would you have to
boost turnout among people 18-35 to meaningfully shift the likely vote-share in competitive
districts? Imagine we think we can boost youth turnout by 5% across the state.
How much would that change the final vote-share across the state? It turns out that makes very
little difference in close districts in VA, primarily because the typical under 35 voter in VA is not
overwhelmingly more likely to vote for Democrats. So it makes a small difference and one that
can be positive or negative, depending on the district.
|]

part5 :: Text
part5 = [here|
When doing these analyses, we’ve chosen PPL as our baseline. But one could just as easily use
some other framework or model to come up with a baseline and still use DPL based scenario
analysis to understand how much and where things might change under various circumstances.

### Geographical District Overlaps: Double-word scores
Once we have PPL to make allocation recommendations for donors, e.g., give to any race within 5 points of 50/50,
how can we narrow (or broaden) that list? Are we missing any districts worth an investment of resources?
Are we including any that are too hard or too easy to win?
Among all the districts we consider close, are some better “investments” than others?

Sometimes it’s helpful to think about how the district relates to other races on the same
ticket, the so-called “reverse-coattails” effect (“reverse” because we are referring to races
for smaller offices helping a larger race on the same ballot).

When a district overlaps geographically with another important election we call that a
a “double-word score.” For instance, in a presidential election year, any close
state-legislative district in a swing state might be a good or appealing place to direct donor dollars.
Close senate races also generate these sorts of opportunities.

Looking at competitive *congressional* districts,
gives “double-word score” opportunities for some state-legislative-districts
and not others. These are trickier to find than statewide elections since we need to analyze the population overlaps
of the congressional and state-legislative districts.
As an example, let’s consider the 2024 election in Wisconsin. The chart below
contains all the competitive state-legislative districts (PPL between 45% and 55%) and whichever congressional
district contains most of the state-legislative-district. In green, we’ve highlighted the districts where the
overlapping congressional districts are also competitive by PPL. The WI congressional races are already
triple-word-scores in ‘24 because WI is a swing state and has a senate race as well.
So the state-legislative-districts in green in the chart below are actually quadruple-word-scores!

This can be a nice frame for driving donor money aimed at the bigger races into the state-legislative races.
|]


part6 :: Text
part6 = [here|
## DPL: our model
We’re going to explain what we do in steps, expanding on each one further down in the document
and putting some of the mathematical details in other, linked documents. Our purpose here
is to present a thorough idea of what we do without spending too much time on any of the
technical details.

Our survey data comes from the Cooperative Election Study ([CES](https://cces.gov.harvard.edu)),
a highly-regarded survey which runs every 2 years and validates
people’s responses about turnout via a third-party which uses voter-file data[^vf]. The
CES survey includes approximately 60,000 people per election cycle. It gathers demographic and
geographic information about each person as well as information about voting and party preference.

[^vf]: CES has used different validation partners in different years. Whoever partners with the
CES in a particular year attempts to match a CES interviewee with a voter-file record in that state
to validate the survey responses about registration and turnout.

The CES data includes state and congressional district of each person interviewed. We use that
to join the data to population-density data from the 5-year American Community
Survey ([ACS](https://www.census.gov/programs-surveys/acs/microdata.html)) data, using
the sample ending the same year as the CES survey was done.

We then fit a multi-level regressions of that data, one for
turnout, one for party-preference and one for both at the same time.
To compute expected turnout, party-preference of voters or DPL in a district,
we  “post-stratify” the model using the demographics of the district. That
demographic data is also sourced from the ACS, though via a different path
because the microdata is not available at SLD size geographies. Due
to the method we use for modeling (Hamiltonian Monte Carlo), the
result of post-stratification is a distribution of
turnout, party-preference or DPL, giving us an expectation and various
ways to look at uncertainty.

Let’s dig into the details a bit!

### Choosing Our Sources
Our present model uses the 2020 CES survey as its source. We will have the
2022 CES survey as a possible source available to us soon. It’s not clear
which is more appropriate for thinking about 2024. 2022 is more recent and so
might better capture current demographic voting trends,
but 2020 is the most recent *presidential* election year.
Using the 2020 survey, we choose presidential vote as our
party-preference indicator. If we use 2022, we will
switch to house-candidate vote and include incumbency in the regression model
for party-preference. We’re thinking about ways to combine these but it’s not
straightforward for a variety of reasons. We could also model using both and
have two DPL numbers to look at per district.

ACS is the best available public data for figuring out the demographic breakdowns
of who lives in a district. We get ACS population tables at the census-tract
level and aggregate them to the district level. Unfortunately, none of these
tables alone has all the categories we want for post-stratification.
So We use statistical methods[^nsm] to
combine those tables, producing an estimated population table with all of our
demographic categories in each district.

[^nsm]: We will produce another explainer about just this part of the DPL. Basically,
the ACS data is provided in tables which typically cover 3-categories at a time,
for example citizenship, sex and race/ethnicity. To get the table we want,
citizenship x age x sex x education x race/ethnicity–citizenship is there so we
can get the final table for citizens only since only citizens can vote–we need
to “combine” 3 of those tables. We use data from larger geographies and a model
of how those tables fit together based on the data in each of them, to estimate
the correct combination in each district. There is a large and confusing literature
full of techniques for doing this. The one we’ve developed has some advantages
in terms of accuracy and statistical bias and the disadvantage of being somewhat more
complex.

There are various companies that use voter-file data
(which is public but costs $$ and requires work to standardize) and various other data and
modeling (e.g., using names to predict race and sex) to estimate a population table
for any specific geography. We’ve not had a chance to compare the accuracy of our methods
to theirs but we imagine those methods are capable of being quite accurate as well.

### Modeling the Turnout and Party-Preference
The CES survey provides data for each person interviewed. The demographic data is provided,
along with a weight, designed so that subgroups by age, race, etc. are correctly represented once
weights are accounted for. For example, if you know that there are equal numbers of men and women
(CES has just begun tracking gender categories other than male or female but the ACS does not)
in a congressional district but your survey has twice as many women as men, you would adjust the
weights so that those interviews have equal representation in a weighted sum. Because our demographic
categories are more coarse-grained than what the CES provides (e.g., CES give age in years but we want
to model with 5 age buckets) we need to aggregate the data. We use the weights when aggregating and this
means that in the aggregated data we have non-whole numbers of voters and voters preferring one party or the other.

We imagine each person in a demographic category and state has a
specific fixed probability of voting and each voter a different probability of voting
for the Democratic candidate. This would lead to a binomial model of vote
counts. This is obviously a simplification but a fairly standard one, and a reasonable fit
to the data. As mentioned above, we have non-whole counts. So we use a generalization of the
binomial model^[bg] which allows for this.

[bg]: Specifically, we use the binomial density but just allow non-integer “successes” and “failures”.
This is not an actual probability density and gives slightly lower likelihood
to very low and very high counts than it should. Fixing this is one project for our
next version!

Our specific probability is a linear function of the log-density^[lpd] plus a number for each of the categories
and some of their combinations. In particular we estimate using “alphas” for
state, age, sex, education, race/ethnicity, the combination of age and education, age and race/ethnicity,
education and race, and state and race. For the state factor and all the combination factors,
we use “partial-pooling” which means we allow the model itself to estimate how big an overall
factor these variations should be.

[lpd]: For modeling, we use logarithmic population density.
Population density in the US varies by several orders of magnitude, from 10s
of people per square mile in the most rural places to over 100,000 per square mile
in the densest places, like New York City. That makes it difficult to use as a
predictor. There are various approaches to “fixing” this. We can classify places
by density, e.g., as rural, suburban and urban. Or we can divide density into
quantiles and use those for modeling. We choose a log-transform to compress
the range, somewhat like using quantiles, but preserve the continuous variation.

We use [Stan](https://mc-stan.org), which then runs a
[Hamiltonian Monte Carlo](https://en.wikipedia.org/wiki/Hamiltonian_Monte_Carlo)
to estimate the parameters. Because of how Monte-Carlo methods work, we end up with
not only our expected parameter values but also their distributions, allowing us to
capture uncertainties. This is also true of post-stratifications,
which then provide with distributions of outcomes and thus things like confidence intervals.

There’s an important last step. We post-stratify these modeled probabilities
across an entire state, giving the expected number of votes in that state. But
we *know* the actual number of votes recorded in the state and our number won’t usually match
exactly. So we adjust each probability using a technique pioneered by
[Hur & Achen](https://www.aramhur.com/uploads/6/0/1/8/60187785/2013._poq_coding_cps.pdf),
and explained in more detail on pages 9-10 of
[this paper](http://www.stat.columbia.edu/~gelman/research/published/mrp_voterfile_20181030.pdf).
We can apply the same adjustment to our confidence intervals giving us an approximate
confidence interval for the adjusted parameter or post-stratification result.

We do this for turnout and party-preference of voters, where we match to known
vote totals for the candidate of each party.

The final result of all this work is an estimate of the DPL for any SLD in the country.
|]

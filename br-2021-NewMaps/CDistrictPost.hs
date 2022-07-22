{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

module CDistrictPost where

import Data.String.Here (here, i)

import qualified Path
import Data.Massiv.Array (Load(makeArray))

cdPostDir = [Path.reldir|cdPostComponents|]

cdPostTop :: Path.Path Path.Abs Path.Dir -> Text -> Text
cdPostTop p cd =
  [i|The BlueRipple Politics model focuses on demographics and location as a predictor of election results.
We intentionally leave out things like district specific partisan history[^partisan] and incumbency[^incumbency].
Those things are important for *predicting* election outcomes, so why do we leave them out? We are more interested
in the potential of a district than what happens in any one election. Focusing on the electoral history or
incumbency risks missing demographic changes or conditions that might be favorable or unfavorable to Dem
candidates. There is plenty of good predictive modeling (e.g. FiveThirtyEight)
and plenty of good district specific analysis (e.g., Cook or Sabato),
all summarized at [270ToWin][ratings].
We're trying to provide something
a little different, looking for possible surprises and districts on the cusp of change.

For Sawbuck Patriots[sawbuck] we're using our methodology to provde color commentary, to give you, the hardy
daily donors, a different way to look at Alex's picks.
So below we're giving you the demographic data we
think is most relevant in any district: the population density, what percentage of the eligible-to-vote White
people have graduated from college and what percent of the eligible-to-vote population are voters of color.

But that data doesn't mean much without context. So we've done a few of things:

- We've ranked the data on a scale of 1-10 among all districts and we plot rank instead of the number.
  This puts all the numbers on the same scale and footing.

- We've included the national median (as a circle) for *historically* D leaning (blue circle)
  and R leaning (red circle) districts.

- And we've added a blue/red line to indicate the D/R range of districts. That line shows the middle
  50% of D or R districts.

- Where sensible, we show another chart with the national medians and ranges replaced by state medians and ranges.
  Each state is different and seeing that context helps as well.  Where we don't have those, it's because a state is
  small and lacks at least 1 of each sort of district.

- One more chart with a comparison district added.  This is as similar a district as we can find on our demographic
  measures and, when possible, in the same state, but often with a more D voting history, as further evidence that
  the district we are looking at can stay/go D.

[^partisan]: Our model uses election results and surveys to estimate turnout and voter preference according to
various demographic variables. But we don't use previous election results as a *predictor*, though that is clearly
crucial if your goal is prediction.
[^incumbency]: We do use incumbency as a predictor otherwise the strong effect of incumbency would make
the rest of the fit less precise.  But we do not apply it when analyzing districts because our goal is
to look at the potential for districts to change, not to predict a specific outcome.
[ratings]: https://www.270towin.com/2022-house-election-predictions/
[sawbuck]: https://www.sawbuckpatriots.com

|]


cdPostBottom :: Path.Path Path.Abs Path.Dir -> Text -> Text
cdPostBottom p cd =
  [i| For the intrepid, a bit more about our key metrics:

- Population Density is usually measured in people per square mile. We use a slightly more people-centered
  version, so a district with a very dense city and then a lot of empty land around it will basically have
  the density of the city in our calculations since that's the density all the voters actually live in.
  We do this by breaking each district into census blocks and then doing a people-weighted geometric average.
  There is ample [evidence][pDensity] that population density has a high correlation with partisan lean, wit
  the crossover point from R to D at about 800 people per square mile.

- The past few years have seen massive education polarization in US electoral politics, with college-educated
  voters swinging more towards D candidates and non-college voters swinging toward R candidates. This
  is true amoing White voters and voters of color but voters of color are substantially more D leaning
  to begin with so their educational attainment is less useful as a district predictor. It's not clear
  if education is explanatory or a proxy for other issues (for a good discussion of this, check out
  [SplitTicket][edPol]).

- As we noted above, voters of color tend to lean strongly D though this varies across racial/ethnic groups.
  Therr is some recent drift toward the Republican party, particularly among non-college-educated
  Hispanic voters. Still, the fraction of a district which is non-White is heavily predictive.

[pDensity]: https://engaging-data.com/election-population-density/
[edPol]: https://split-ticket.org/2022/01/03/the-white-vote-and-educational-polarization/
|]

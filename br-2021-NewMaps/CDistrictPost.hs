{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE ScopedTypeVariables #-}

module CDistrictPost where

import qualified BlueRipple.Utilities.KnitUtils as BR

import qualified Knit.Report as K
import qualified Polysemy as P
import Data.String.Here (i)
import qualified Data.Text as T
--import qualified Data.Map as Map
import qualified Path

cdPostDir :: Path.Path Path.Rel Path.Dir
cdPostDir = [Path.reldir|cdPostComponents|]


cdTopSBDefault :: Text
cdTopSBDefault =
  [i|
For [Sawbuck Patriots][sawbuck] we're using our methodology to provide color commentary, to give you, the hardy
daily donors, a different way to look at Alex's picks.
So below we're giving you the demographic data we
think is most relevant in any district: the population density, what percentage of the eligible-to-vote White
people have graduated from college and what percent of the eligible-to-vote population are voters of color.

[sawbuck]: https://www.sawbuckpatriots.com

|]

cdTopSB :: (K.KnitEffects r, P.Member (P.Embed IO) r) => Path.Path Path.Abs Path.Dir -> Text -> K.Sem r Text
cdTopSB p cd = do
  fp <- K.liftKnit @IO $ Path.parseRelFile $ toString cd <> ".md"
  let districtSpecificPath = p Path.</> cdPostDir Path.</> fp
  tM <- BR.brGetTextFromFile districtSpecificPath
  let --link = "https://blueripple.github.io/SawbuckPatriots/" <> cd <> "_sawbuck.html"
      --linkTxt = "More details of this analysis are available at the  [Blue Ripple Web Site](" <> link <> ")."
      checkTxt = "Checking " <> show districtSpecificPath <> " for blurb..."
  case tM of
    Nothing -> K.logLE K.Warning $ checkTxt <>  "not found. Using default."
    Just _ -> K.logLE K.Info $ checkTxt <> "found. Using provided blurb."
  return $ fromMaybe cdTopSBDefault tM

cdPostTop :: K.KnitEffects r =>  Path.Path Path.Abs Path.Dir -> Text -> K.Sem r Text
cdPostTop p cd = do
  sbText <- cdTopSB p cd
  return
    $ [i|Blue Ripple Politics focuses on demographics and location as an indicator of partisan lean,
leaving out things like district specific partisan history[^partisan] and incumbency[^incumbency].
There is plenty of good predictive modeling (e.g. FiveThirtyEight)
and plenty of good district specific analysis (e.g., Cook or Sabato),
all well summarized at [270ToWin][ratings].
We're trying to provide something
different, looking for possible surprises and districts on the cusp of change. For
more details about the model, please visit the
[model-details section of our web-site][modelDetails].

[modelDetails]: https://blueripple.github.io/explainer/model/ElectionModel/post.html

|] <> sbText
   <> [i|
Our key data is summarized in the chart below.

- The data for all districts is put in order and binned into 10 equally populated bins.

- We've included the national median (as a circle) for *historically* D leaning (blue)
  and R leaning (red) districts and we've added a blue/red line to indicate
  the middle 50% D/R range of districts.

[^partisan]: Our model uses election results and surveys to estimate turnout and voter preference according to
various demographic variables. But we don't use previous election results as a *predictor*, though that is clearly
crucial if your goal is prediction.
[^incumbency]: We do use incumbency as a predictor otherwise the strong effect of incumbency would make
the rest of the fit less precise.  But we do not apply it when analyzing districts because our goal is
to look at the potential for districts to change, not to predict a specific outcome.

[ratings]: https://www.270towin.com/2022-house-election-predictions/

|]

cdPostStateChart :: Path.Path Path.Abs Path.Dir -> Text -> Text
cdPostStateChart _ _ =
  [i|Below, we replace the national medians with state medians
(while keeping the *national* ranges).
Each state is different and seeing that context helps as well.
|]

cdPostCompChart :: Path.Path Path.Abs Path.Dir -> Text -> NonEmpty Text -> Text
cdPostCompChart _ _ compCDs =
  let x1 :: Text = if length compCDs == 1 then "One final chart" else "Some final charts"
      x2 :: Text = if length compCDs == 1 then head compCDs else T.intercalate ", " (init compCDs)  <> ", and " <> last compCDs
  in [i|
${x1}, this time adding ${x2} for comparison.
|]

cdPostBottom :: Path.Path Path.Abs Path.Dir -> Text -> Text
cdPostBottom _ _ =
  [i| For the curious, a bit more about our key metrics:

- Population Density is usually measured in people per square mile. We use a people-weighted
  version, so a district with a very dense city and mostly empty land around it will basically have
  the density of the city in our calculations.
  We do this by breaking each district into census blocks and then doing a people-weighted geometric average.
  There is ample [evidence][pDensity] that population density has a high correlation with partisan lean, with
  the crossover point from R to D at about 800 people per square mile.

- The past few years have seen massive education polarization in US electoral politics, with college-educated
  voters swinging more towards D candidates and non-college voters swinging toward R candidates. This
  is true amoing White voters and voters of color, but voters of color are substantially more D leaning
  to begin with so their educational attainment is less useful as a district predictor. It's not clear
  if education is explanatory or a proxy for other issues (for a good discussion of this, check out
  [SplitTicket][edPol]).

- As we noted above, voters of color tend to lean strongly D though this varies across racial/ethnic groups.
  Their is some recent drift toward the Republican party, particularly among non-college-educated
  Hispanic voters. Still, the fraction of a district which is non-White is heavily predictive.

[pDensity]: https://engaging-data.com/election-population-density/
[edPol]: https://split-ticket.org/2022/01/03/the-white-vote-and-educational-polarization/
|]

{-# LANGUAGE DataKinds                 #-}
{-# LANGUAGE DeriveDataTypeable        #-}
{-# LANGUAGE PolyKinds                 #-}
{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE GADTs                     #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE TypeApplications          #-}
{-# LANGUAGE TypeOperators             #-}
{-# LANGUAGE QuasiQuotes               #-}
{-# LANGUAGE AllowAmbiguousTypes       #-}
{-# LANGUAGE TupleSections             #-}
{-# OPTIONS_GHC  -fplugin=Polysemy.Plugin  #-}

module P2 (p2) where

import qualified Control.Foldl                 as FL
import qualified Data.Map                      as M
import qualified Data.Array                    as A

import qualified Data.Text                     as T


import           Graphics.Vega.VegaLite.Configuration as FV

import qualified Frames.Visualization.VegaLite.Data
                                               as FV
import qualified Frames.Visualization.VegaLite.ParameterPlots
                                               as FV                                               

import qualified Knit.Report                   as K
--import qualified Knit.Report.Input.MarkDown.PandocMarkDown    as K
--import qualified Text.Pandoc.Options           as PA

import           Data.String.Here               ( here, i )

import           BlueRipple.Configuration
import           BlueRipple.Utilities.KnitUtils
import           BlueRipple.Data.PrefModel.SimpleAgeSexRace
import           BlueRipple.Data.PrefModel.SimpleAgeSexEducation
import qualified BlueRipple.Model.Preference as PM

import PrefCommon
import Paths_preference_model

p2 :: K.KnitOne r
   => T.Text
--   -> M.Map Int (PM.PreferenceResults SimpleASR FV.NamedParameterEstimate)
--   -> M.Map Int (PM.PreferenceResults SimpleASE FV.NamedParameterEstimate)
   -> K.Sem r ()
p2 assetPath = do
  dataDir <- T.pack <$> K.liftKnit getDataDir
  let tweetPath = dataDir <> "/images/WWCV_tweet.png"
  copyAsset tweetPath (brPrefModelLocalPath <> assetPath <> "/images")
  brAddMarkDown $ brP2Intro assetPath
  brAddMarkDown brReadMore

--------------------------------------------------------------------------------
brP2Intro :: T.Text -> T.Text
brP2Intro assetPath = [i|
Political strategists and pundits are split on how Democrats ought
to address "White Working Class" (WWC) voters:
[convince them](https://www.realclearpolitics.com/articles/2019/04/07/after_2016_loss_democrats_know_they_need_white_male_voters_139987.html)
or [ignore them](https://www.rollcall.com/news/campaigns/learning-lessons-about-working-class-whites)?
We think they're both half-right.  WWC voters are, as a group,
too attached to the Republican party to convince, but also too large to ignore.
We think the key is energizing the progressive base of the Democratic party, while *also*
finding common ground with some WWC voters on issues they care about.

1. Why Is There Controversy About WWC voters?
2. How Democrats Win In Heavily WWC States and Districts
3. What You Can Do Now To Help

## Why Is There Controversy About White Working Class Voters?
First, when people talk about WWC voters, they usually mean white voters without a 4-year college degree.
For more details, see
[this paper](https://www.demos.org/research/understanding-working-class).

With that in mind, consider the following recent, much discussed tweet:

![@sahilkapur tweet](images/WWCV_tweet.png)\_

Let's focus on "meaningful gains." As a group,
WWC voters are more likely to vote for Republicans and that
will almost certainly continue be true in 2020.
But there are a lot of WWC voters! So the difference between the 37% (66% - 29%) loss in 2016
and the 24% (61% - 37%) loss in 2018 represents a *lot* of votes. For instance,
WWC voters make up about
59% of the electorate in Wisconsin. If there had been a 13% swing to 
Clinton instead of Trump---the swing from 2016 to 2018---in 2016,
Wisconsin would have gone blue.
Michigan (53% WWC) and Pennsylvania (55% WWC) are also states where a 13% shift
in the 2016 WWC vote would have flipped them blue.
But there's more: those states were very very close---so even a 2% shift in the WWC vote
would have flipped them *all* blue. 

Where does this leave us? On the one hand, the WWC is a large
(though [shrinking](https://www.stlouisfed.org/on-the-economy/2019/may/why-white-working-class-decline))
group and even small swings in their likelihood of voting for Democrats can make a
difference, particularly in WWC-heavy midwestern battleground states.  But the gap
we need to close in those states is small, so centering
a campaign on winning WWC votes isn't the only---or best---strategy.

To make that point even clearer,
let's do an experiment: what change in turnout
among non-WWC voters in each state would have generated enough votes in 2016 to win three
key battlegrounds?
In all three states, it turns out that a pretty small turnout
boost among non-WWC---less than 3%---would have made the difference.
And these are all states that saw, e.g.,
drops in turnout from 2012 to 2016 of >5% among non-white voters. 

This is summarized in the table below (votes in thousands):

-------------------------------------------------------------------------------------------------
State D Votes R Votes Total  WWC Pop  WWC D Pref  Non-WWC D Pref  Pref Shift in  Turnout Boost in
                                                                  WWC needed     non-WWC needed
                                                                  to win         to win 

----- ------- ------- -----  -------  ----------  --------------  -------------  ----------------
PA    2926    2971    6167   53%      29%         70%             1.4%           2.2%

MI    2269    2280    4799   54%      29%         68%             0.4%           0.6%

WI    1383    1405    2953   59%      29%         74%             1.3%           2.6%          
-------------------------------------------------------------------------------------------------


NB: The turnout boost % assumes an approximate turnout of 50% among Non-WWC voters in all 3 states. 
Our source for the percentage of WWC voters in these states is
[this article.](https://www.americanprogress.org/issues/democracy/reports/2017/11/01/441926/voter-trends-in-2016/).
Our vote totals are from
[Wikipedia](https://www.wikipedia.org/) and our Democratic voter preference (29%) is taken from the
tweet above.  The Non-WWC Democratic voter preference is calculated from the WWC voter preference, the
WWC percentage of the populations and the vote totals, assuming that turnout is similar among WWC and
non-WWC voters.

This little thought experiment makes an important point about the 2016 election, turnout and voter rights.
In all three of these states, non-WWC turnout fell by over 5% from 2012 to 2016.
If even half of those folks (or even far less, in some cases)
were able to and chose to show up on election day,
that also would have been enough to turn the tide and win the presidency. 

## How Democrats Can Win In Heavily WWC States
It's important to re-iterate how close the midwest battleground states
were in 2016, and how difficult it would be for a Republican candidate to win them
with numbers anything like what we saw in 2018. So Democrats may be justified
in not making any special appeal to WWC voters.  However, as progressives
often point out, many progressive policies are good for almost everyone, including
the WWC. So we think the approach should be two-fold:

1. Do what we ought do everywhere else: work hard at registration and turnout among everyone,
particularly the Democratic base.  This involves defense of voting rights:
fighting unfair poll purges and voter ID laws,
working for automatic and same-day registration,
and working to keep more polling places open and for more days and hours.
And this also requires the traditional tools for improving turnout:
registration drives, knocking on doors, etc.  This idea inspires part of 
Blue Ripple Politics' support for local candidates in places where
their voter outreach will also help candidates running for other offices.

2. Identify which parts of the WWC can be convinced, hopefully by their willingness
to embrace parts of the progressive agenda.
[Data For Progress](https://www.dataforprogress.org/) has done amazing
[work](https://www.dataforprogress.org/the-new-progressive-agenda) looking
at progressive policies with broad support.  For instance, capping
Credit Card interest rates polls positively among Trump voters in all 50 states,
and Marijuana legalization polls positively among Trump voters in all but AK, MS and AL.
Ending the war in Yemen is more mixed among Trump voters, but polls well among them
in PA, MI and WI.  All of these policies are supported by many progressive
candidates, including several vying for the Democratic Nomination.

3. In the short-term, some WWC voters may choose to vote Democratic
because of their disgust with Trump and his enablers.  For example
[non-evangelical WWC women](https://nymag.com/intelligencer/2018/12/democrats-won-with-non-evangelical-white-working-class-women.html)
are more than 10% of the national electorate and were strong *Democratic* voters (57% D vs. 41% R) in 2018.
Trump's approval rating with non-evangelical WWC women
is now about 35% so there's no hint that they would vote any differently in 2020
than they did in 2018. We note that this is a *tactical* consideration and so ranks low for us.
But it's a reminder that the "against Trump" message is also worthwhile in this election.

In other words: yes, we should rally and support the voters who
are reliable Democratic supporters while also minimizing losses among the WWC.
Working hardest for the people who support Democrats is the right thing to do.
But we should also find common ground with WWC voters that share many of our core values
and encourage them to show up in November and pull the Democratic lever, because
progressivism is good for almost
everyone, and especially good for the working class. This two-pronged strategy will
energize and broaden our base---which is key for both the long-term success
of progressive policies and also
the tactical, short-term goal of winning the 2020 presidential election and
getting Trump out of office.

## Take Action
Give your time or money to  organizations that work on registration and turnout,
especially among groups that tend to support Democrats. 

- [Rock The Vote](https://www.rockthevote.org) is "nonpartisan nonprofit
dedicated to building the political power of young people."
Through them you can
[organize](https://www.rockthevote.org/action-center/host-your-own-event/)
your own registration drive or
[volunteer](https://www.rockthevote.org/action-center/volunteer/) 
to register and engage voters or
[donate](https://www.rockthevote.org/donate/)
money to further that work.
- [Voto Latino](https://votolatino.org/) works to register voters,
and "empower Latinos to be agents of change."
- [Vote Riders](https://www.voteriders.org/) works specifically on
[voter ID](https://www.voteriders.org/real-myths-about-voter-id/),
with a "mission to ensure that all citizens are able to exercise their right to vote."
You can [volunteer](https://www.voteriders.org/get-involved/volunteer/)
to help voters acquire appropriate ID or
[donate money](https://voteriders.salsalabs.org/donate/index.html) to
help with their work.

We tried to find local
organizations working specifically in battleground states but didn't discover any.
Please [email](mailto:adam@blueripplepolitics.org) us if you know of
local organizations doing this work and we'll update this post! 
|]
  

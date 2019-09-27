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
too attached to the Republican party to convince, and too large to ignore.
We think the key is energizing the progressive base of the Democratic party while also
finding common ground on issues progressives and *some* WWC voters care about.

1. Why Is There Controversy About WWC voters?
2. How Democrats Win In Heavily WWC States and Districts
3. What You Can Do Now To Help

## Why Is There Controversy About White Working Class Voters?
First, when people talk about WWC voters people usually mean white voters without a 4-year college degree.
For more details, see
[this paper](https://www.demos.org/research/understanding-working-class)).

With that in mind, consider the following recent tweet:

![@sahilkapur tweet](images/WWCV_tweet.png)\_

This tweet generated a fair amount of discussion! Let's focus on "meaningful gains."
WWC voters are more likely to vote for Republicans and that
will almost certainly continue be true in 2020.
But there are a lot of WWC voters! So the difference between the 37% loss in 2016
and the 24% loss in 2018 represents a *lot* of votes. For instance,
WWC voters make up about
58% of the electorate in Wisconsin. If there had been a 13% swing to 
Clinton instead of Trump in 2016, Wisconsin would have gone blue.
Michigan (53% WWC) and Pennsylvania (55% WWC) are also states where a 13% shift
in the 2016 WWC vote would have flipped them blue.
But those states were very very close! Even a 2% preference swing in the WWC vote
would have flipped them *all* blue. 

Where does this leave us? On the one hand, the WWC is a large
(though [shrinking](https://www.stlouisfed.org/on-the-economy/2019/may/why-white-working-class-decline))
group and even small swings in their likelihood of voting for Democrats can make a
difference, particularly in WWC-heavy mid-western battleground states.  But the number
of votes required is not nearly large enough to justify centering
a campaign on winning WWC votes. To make that point even clearer,
let's ask what change in turnout
among the rest of the voters in each state would have generated enough votes in 2016.
In all three states, it would require less than a 3% turnout boost to bridge the 2016 gap.
And these are all states that saw, e.g.,
drops in turnout from 2012 to 2016 of >5% among non-white voters. 

This is summarized in the table below (votes in thousands):

---------------------------------------------------------------------------------------------
State D Votes R Votes Total  WWC Pop  WWC D Pref  Non-WWC D Pref  Pref Swing  Turnout Boost 
----- ------- ------- -----  -------  ----------  --------------  ----------  -------------
PA    2926    2971    6167   53%      29%         70%             1.4%        2.2%

MI    2269    2280    4799   54%      29%         68%             0.4%        0.6%

WI    1383    1405    2953   59%      29%         74%             1.3%        2.6%          
---------------------------------------------------------------------------------------------

NB: The turnout swing % assumes an approximate turnout of 50% among Non-WWC voters in all 3 states.
Our source for the percentage of WWC voters in these states is
[this article.](https://www.americanprogress.org/issues/democracy/reports/2017/11/01/441926/voter-trends-in-2016/).
Our vote totals are from
[Wikipedia.](https://www.wikipedia.org/)

## How Democrats Can Win In Heavily WWC States
First, it's important to re-iterate how close the mid-west battleground states
were in 2016, and how difficult it would be for a Republican candidate to win them
with numbers anything like what we saw in 2018. So Democrats may be justified
in not making any special appeal to WWC voters.  However, as progressives
often point out, many progressive policies are good for almost everyone, including
the WWC. So we think the approach should be two-fold:

1. Do what we ought do everywhere else: work hard at registration and turnout among everyone,
particularly the Democratic base.  This involves defense of voting rights:
fighting poll purges and voter ID laws, working for automatic and same-day registration,
and working to keep more polling places open and for more days and hours.
And this also requires the traditional tools for improving turnout:
registration drives, knocking on doors, etc.  This idea inspires part of 
Blue Ripple Politics' method for choosing local candidates in places where
their voter outreach will also help candidates running for other offices.

2. Identify which parts of the WWC can be convinced, either by their willingness
to embrace parts of the progressive agenda or their disgust with Trump and his enablers.  For example
[non-evangelical WWC women](https://nymag.com/intelligencer/2018/12/democrats-won-with-non-evangelical-white-working-class-women.html)
are more than 10% of the national electorate and were strong *Democratic* voters (57/41) in 2018.
Trunp's approval rating with non-evangelical WWC women is about 35% so there's no hint that they would vote any differently in 2020
than they did in 2018. 

In other words, rally and support the voters who
are reliable Democratic supporters while also minimizing losses among the WWC.
Working hardest for the people who support Democrats is the right thing to do.
Working to convince the portion of the WWC that
is tired of Trump is also the right thing to do, progressivism is good for almost
everyone, and especially good for the working class. Both of these things are also
good strategy: building turnout among the progressive base and broadening that base
are core to the long-term success of progressive policies.  Those things are also
tactically important right now, for winning this election and getting Trump out
of office.

## Take Action
Give your time or money to  organizations that work on registration and turnout,
especially among groups that tend to support Democrats. 

- 
[Rock The Vote](https://www.rockthevote.org) is "nonpartisan nonprofit
dedicated to building the political power of young people."
Through them you may
[organize](https://www.rockthevote.org/action-center/host-your-own-event/)
your own registration drive or
[volunteer](https://www.rockthevote.org/action-center/volunteer/) 
to register and engage voters or
[donate](https://www.rockthevote.org/donate/)
money to further that work.
- [Voto Latino](https://votolatino.org/) works to register voters,
and "empower Latinos to be agents of change."

Some of these states have voter ID laws.  Organizations that do work
to mitigate the effects of these laws on turnout are also important.

- [Vote Riders](https://www.voteriders.org/) works specifically on
[voter ID](https://www.voteriders.org/real-myths-about-voter-id/),
with a "mission to ensure that all citizens are able to exercise their right to vote."
You can [volunteer](https://www.voteriders.org/get-involved/volunteer/)
to help voters acquire appropriate ID or
[donate money](https://voteriders.salsalabs.org/donate/index.html) to
help with their work.

We tried to find local
organizations working specifically in these states but didn't discover any.
Please [email](mailto:adam@blueripplepolitics.org) us if you know of
local organizations doing this work and we'll update this post! 
|]
  

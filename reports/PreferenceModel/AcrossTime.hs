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

module PreferenceModel.AcrossTime (post) where

import qualified Control.Foldl                 as FL
import qualified Data.Map                      as M
import qualified Data.Array                    as A

import qualified Text.Blaze.Colonnade          as BC
import qualified Text.Blaze.Html.Renderer.Text as B
import qualified Text.Blaze.Html5.Attributes   as BHA

import qualified Data.List as L
import qualified Data.Text                     as T
import qualified Data.Text.Lazy                     as TL

import qualified Frames as F
import           Graphics.Vega.VegaLite.Configuration as FV

import qualified Frames.Visualization.VegaLite.Data
                                               as FV
import qualified Frames.Visualization.VegaLite.ParameterPlots
                                               as FV
import qualified Frames.Visualization.VegaLite.LineVsTime as FV                                               
import qualified Frames.Visualization.VegaLite.StackedArea as FV
import qualified Knit.Report                   as K

import           Data.String.Here               ( here, i )

import           BlueRipple.Configuration
import           BlueRipple.Utilities.KnitUtils
import qualified BlueRipple.Utilities.TableUtils as BT
import           BlueRipple.Data.DataFrames 
import           BlueRipple.Data.PrefModel.SimpleAgeSexRace
import           BlueRipple.Data.PrefModel.SimpleAgeSexEducation
import qualified BlueRipple.Model.Preference as PM

import PreferenceModel.Common


data JustRace = NonWhite | White deriving (Enum, Bounded, Eq, Ord, A.Ix, Show)
aggregateToJustRace :: PM.Aggregation JustRace SimpleASR
aggregateToJustRace = PM.Aggregation children where
  children :: JustRace -> [SimpleASR]
  children x = case x of
    NonWhite -> [OldNonWhiteFemale, YoungNonWhiteFemale, OldNonWhiteMale, YoungNonWhiteMale]
    White -> [OldWhiteFemale, YoungWhiteFemale, OldWhiteMale, YoungWhiteMale]

aggregateToSimpleEducation :: PM.Aggregation SimpleEducation SimpleASE
aggregateToSimpleEducation = PM.Aggregation children where
  children :: SimpleEducation -> [SimpleASE]
  children x = case x of
    NonGrad -> [OldFemaleNonGrad, YoungFemaleNonGrad, OldMaleNonGrad, YoungMaleNonGrad]
    Grad -> [OldFemaleCollegeGrad, YoungFemaleCollegeGrad, OldMaleCollegeGrad, YoungMaleCollegeGrad]

post :: K.KnitOne r
     => M.Map Int (PM.PreferenceResults SimpleASR FV.ParameterEstimate)
     -> M.Map Int (PM.PreferenceResults SimpleASE FV.ParameterEstimate)
{-     -> M.Map Int (PM.PreferenceResults SimpleASR FV.NamedParameterEstimate)
     -> M.Map Int (PM.PreferenceResults SimpleASE FV.NamedParameterEstimate)
-}
     -> F.Frame HouseElections
     -> K.Sem r ()
post modeledResultsASR modeledResultsASE {-modeledResultBG_ASR modeledResultBG_ASE-} houseElectionsFrame = do  
            -- arrange data for vs time plot
  let flattenOneF :: Show b => Int -> FL.Fold (b,FV.ParameterEstimate) [(T.Text,Int,Double)]
      flattenOneF y = FL.Fold
        (\l (b,a) -> (T.pack (show b), y, FV.value a) : l)
        []
        reverse
      flattenF :: (A.Ix b, Show b) => FL.Fold (Int, PM.PreferenceResults b FV.ParameterEstimate) [(T.Text,Int,Double)]  
      flattenF = FL.Fold
        (\l (y, pr) -> FL.fold (flattenOneF y) (A.assocs $ PM.modeled pr) : l)
        []
        (concat . reverse)
      vRowBuilderPVsT =
        FV.addRowBuilder @'("Group",T.Text) (\(g, _, _) -> g)
        $ FV.addRowBuilder @'("Election Year",Int) (\(_, y, _) -> y)
        $ FV.addRowBuilder @'("D Voter Preference (%)",Double) (\(_, _, vp) -> 100*vp)
        $ FV.emptyRowBuilder
      vDatPVsT :: (Show b, A.Ix b) => M.Map Int (PM.PreferenceResults b FV.ParameterEstimate)
               -> [FV.Row
                    '[ '("Group", F.Text), '("Election Year", Int),
                       '("D Voter Preference (%)", Double)]] 
      vDatPVsT pr =
        FV.vinylRows vRowBuilderPVsT $ FL.fold flattenF $ M.toList pr
      addParametersVsTime :: (Show b, A.Ix b, K.KnitOne r)
                          => M.Map Int (PM.PreferenceResults b FV.ParameterEstimate)
                          -> K.Sem r ()
      addParametersVsTime pr = do 
        let vl =
              FV.multiLineVsTime @'("Group",T.Text) @'("Election Year",Int)
              @'("D Voter Preference (%)",Double)
              "D Voter Preference Vs. Election Year"
              FV.DataMinMax
              FV.intYear
--              (FV.TimeEncoding "%Y" FV.Year)
              (FV.ViewConfig 800 400 10)
              (vDatPVsT pr)
        _ <- K.addHvega Nothing Nothing vl
        return ()

      -- arrange data for stacked area share of electorate
  let
    f1 :: [(x, [(y, z)])] -> [(x, y, z)]
    f1 = concat . fmap (\(x, yzs) -> fmap (\(y, z) -> (x, y, z)) yzs)
    vRowBuilderSVS =
      FV.addRowBuilder @'("Group",T.Text) (\(_, y, _) -> y)
      $ FV.addRowBuilder @'("Election Year",Int) (\(x, _, _) -> x)
      $ FV.addRowBuilder @'("Voteshare of D Votes (%)",Double)
      (\(_, _, z) -> 100*z)
      FV.emptyRowBuilder
    vDatSVS prMap = FV.vinylRows vRowBuilderSVS $ f1 $ M.toList $ fmap
                    (PM.modeledDVotes PM.ShareOfD)
                    prMap
    addStackedArea :: (K.KnitOne r, A.Ix b, Bounded b, Enum b, Show b)
                     => M.Map Int (PM.PreferenceResults b Double)
                     -> K.Sem r ()
    addStackedArea prMap = do
      let vl = FV.stackedAreaVsTime @'("Group",T.Text) @'("Election Year",Int)
               @'("Voteshare of D Votes (%)",Double)
               "Voteshare in Competitive Districts vs. Election Year"
               (FV.GivenMinMax 0 100)
               FV.intYear
--               (FV.TimeEncoding "%Y" FV.Year)
               (FV.ViewConfig 750 200 10)
               (vDatSVS prMap)
      _ <- K.addHvega Nothing Nothing vl
      return ()
    mkDeltaTableASR mr locFilter (y1, y2) = do
      let y1T = T.pack $ show y1
          y2T = T.pack $ show y2
      mry1 <- knitMaybe "lookup failure in mwu"
              $ M.lookup y1 mr
      mry2 <- knitMaybe "lookup failure in mwu"
              $ M.lookup y2 mr
      (table, (mD1, mR1), (mD2, mR2)) <-
        PM.deltaTable simpleAgeSexRace locFilter houseElectionsFrame y1 y2 mry1 mry2
      return table
    mkDeltaTableASE mr locFilter  (y1, y2) = do
      let y1T = T.pack $ show y1
          y2T = T.pack $ show y2
      mry1 <- knitMaybe "lookup failure in mwu"
              $ M.lookup y1 mr
      mry2 <- knitMaybe "lookup failure in mwu"
              $ M.lookup y2 mr
      (table, (mD1, mR1), (mD2, mR2)) <-
        PM.deltaTable simpleAgeSexEducation locFilter houseElectionsFrame y1 y2 mry1 mry2
      return table
{-      
      let table' = case mRows of
            Nothing -> table
            Just n -> take n $ FL.fold FL.list table
      let greenOpinion g = g `elem` greenOpinionGroups
      brAddRawHtmlTable (BHA.class_ "br_table") (PM.deltaTableColonnadeBlaze greenOpinion) table
-}
  let emphasizeTotalRow =
        BT.CellStyle (\r _ -> if  (PM.dtrGroup r == "Total") then BT.totalCell else "")
      highlightRows groups =
        BT.CellStyle (\r _ -> if (PM.dtrGroup r `elem` groups) then BT.highlightCellBlue else "")      
      highlightOpinionCellFor groups =
        BT.CellStyle (\r c -> if (c == "FromOpinion") && (PM.dtrGroup r `elem` groups) then BT.highlightCellBlue else "")      

      cStyles hgs ogs = mconcat [emphasizeTotalRow, highlightRows hgs, highlightOpinionCellFor ogs]      

  ase20122016Table <- mkDeltaTableASE  modeledResultsASE (const True) (2012, 2016)
  ase20162018Table <- mkDeltaTableASE  modeledResultsASE (const True) (2016, 2018)
  ase20142018Table <- mkDeltaTableASE  modeledResultsASE (const True) (2014, 2018)
  ase20102018Table <- mkDeltaTableASE  modeledResultsASE (const True) (2010, 2018)
  asr20102018Table <- mkDeltaTableASR  modeledResultsASR (const True) (2010, 2018)
  asr20142018Table <- mkDeltaTableASR  modeledResultsASR (const True) (2014, 2018)  

  brAddMarkDown brAcrossTimeIntro
  addParametersVsTime  modeledResultsASR
  brAddMarkDown brAcrossTimeASRPref
  addStackedArea $ fmap (PM.aggregatePreferenceResults aggregateToJustRace . fmap FV.value) modeledResultsASR
  brAddMarkDown brAcrossTimeASRVoteShare           
  addParametersVsTime  modeledResultsASE
  brAddMarkDown brAcrossTimeASEPref
  addStackedArea $ fmap (PM.aggregatePreferenceResults aggregateToSimpleEducation . fmap FV.value) modeledResultsASE
  brAddMarkDown brBreakingDownTheChanges
  BT.brAddRawHtmlTable "2016 to 2018"
    (BHA.class_ "brTable")
    (PM.deltaTableColonnadeBlaze (cStyles [] []))
    (take 1 $ FL.fold FL.list ase20162018Table)
  brAddMarkDown brAcrossTimeASRRow
  BT.brAddRawHtmlTable
    "2016 to 2018"
    (BHA.class_ "brTable")
    (PM.deltaTableColonnadeBlaze (cStyles [] []))
    ase20162018Table
  brAddMarkDown brAcrossTimeTables
  BT.brAddRawHtmlTable
    "2010 to 2018"
    (BHA.class_ "brTable")
    (PM.deltaTableColonnadeBlaze (cStyles [] []))
    ase20102018Table
  brAddMarkDown brAcrossTimeASRTable
  BT.brAddRawHtmlTable
    "2010 to 2018"
    (BHA.class_ "brTable")
    (PM.deltaTableColonnadeBlaze (cStyles [] []))
    asr20102018Table
  brAddMarkDown brAcrossTimeTableSummary
  brAddMarkDown brAcrossTimeTakeAction
  brAddMarkDown brReadMore
  
  {-
  brAddMarkDown brAcrossTimeAfterASE
  -- one row to talk about
  -- analyze results
  
  brAddRawHtmlTable (BHA.class_ "br_table") (PM.deltaTableColonnadeBlaze (cStyles [] [])) asr20122016Table
  brAddMarkDown brAcrossTimeASR2012To2016
  ase20122016Table <- mkDeltaTableASE modeledResultsASE  (const True) (2012, 2016)
  brAddRawHtmlTable (BHA.class_ "br_table") (PM.deltaTableColonnadeBlaze (cStyles ["OldMaleNonGrad"] ["YoungFemaleCollegeGrad"])) ase20122016Table
  brAddMarkDown brAcrossTimeASE2012To2016   
{-  brAddMarkDown voteShifts
  _ <-
    traverse (mkDeltaTableASR (const True))
    $ [ (2012, 2016)
      , (2014, 2018)
      , (2014, 2016)
      , (2016, 2018)
      , (2010, 2018)
      ]
  brAddMarkDown voteShiftObservations
-}
  let
    battlegroundStates =
      [ "NH"
      , "PA"
      , "VA"
      , "NC"
      , "FL"
      , "OH"
      , "MI"
      , "WI"
      , "IA"
      , "CO"
      , "AZ"
      , "NV"
      ]
    bgOnly r =
      L.elem (F.rgetField @StateAbbreviation r) battlegroundStates
--  brAddMarkDown "### Presidential Battleground States"
--  _ <- mkDeltaTableASR bgOnly (2010, 2018)
--  _ <- mkDeltaTableASE bgOnly (2010, 2018)
  
  brAddRawHtmlTable (BHA.class_ "br_table") (PM.deltaTableColonnadeBlaze (cStyles [] [])) bgASE20122016Table
  brAddRawHtmlTable (BHA.class_ "br_table") (PM.deltaTableColonnadeBlaze (cStyles [] [])) bgASE20142018Table
  brAddMarkDown brAcrossTimeAfterBattleground
  brAddMarkDown brReadMore
-}
brAcrossTimeIntro :: T.Text
brAcrossTimeIntro = [i|

In our previous [post][BR:2018] we introduced a
[model][BR:Methods] using census data and election results to infer voter
preference for various demographic groups in the 2018 house elections.
In this post we'll look back to 2010, examining how Democrats gained and lost votes.

From our analysis it appears that most of the election-to-election change in vote-share
is driven by changes in voter preference rather than demographics or turnout.
We do see a long-term demographic and turnout shift which currently favors Democrats. 

Whatever happened in the past is likely not predictive.
2020 will not be a replay of 2016 or 2018.
However, understanding what happened in previous elections
is a good place to start as we look to hold and expand on the gains made in 2018.

1. **Turnout vs. Preference:** Context and Caveats
2. **The Evolving Democratic Coalition**
3. **Breaking Down the Changes**
4. **What Does It Mean?**
5. **Take Action**

## Turnout vs. Preference: Context and Caveats
Swirling around many analyses and think-pieces about elections is a central
question:  do Democrats/Republicans win elections by getting the people who
agree with them to vote (turnout) or by convincing
people who disagree with them to change their minds (preference)? Campaigns
try to do both, of course, but is one more important in a given race?

We think preference shifts were a big deal in 2018.  Before explaining our data,
it's important to address three key issues that arise with these sorts of analyses:

1. **Who are the "people" we're studying?** Many excellent prior analyses,
like those by [Rachel Bitecofer][RB:TurnoutTweet]  and
[Yair Ghitza][YG:WhatHappened2018], use voter files to look at
voters' underlying party affiliation, to understand the behaviors of registered
Dems and Republicans (and independents) from election to election. We use a
different approach (described in more detail [here][BR:Methods]) that subdivides
people only by demographics, e.g., age, race, degree of education, etc. That
means that if, within the same demographic group, some previously Republican voters
stay home and some new Democratic voters show up, we will see that as
a *preference shift* though from a party affiliation perspective,
it could also be seen as a turnout shift. 

2. **What's the geographic unit of measure?** These analyses depend
a lot on the geographical boundaries and the demographic categorizations.
For instance, if college-educated voters move from one district to another in a state,
that’s demographic change at the district level but not at the state or national level.
In our work so far, we've looked at voters' behaviors only at the national level.

3. **What's the comparator year?** If there's a change in demographics, turnout, or preference,
the key question is: compared with what?
Using 2014 as a baseline—comparing mid-term to mid-term—then there is a Democratic turnout surge,
as [Rachel Bitecofer argues][RB:TurnoutTweet]. If you look instead at the changes from 2016 to 2018,
focusing on the blue wave, voter preference was the more pronounced shift,
as seen in this [Yair Ghitza article][YG:WhatHappened2018]. We take
a few different perspectives, as noted below.

[RB:TurnoutTweet]: <https://twitter.com/RachelBitecofer/status/1185515355088867328>
[YG:WhatHappened2018]: <https://medium.com/@yghitza_48326/revisiting-what-happened-in-the-2018-election-c532feb51c0>
[BR:Methods]: <${brGithubUrl (postPath PostMethods)}#>

## The Evolving Democratic Coalition

From 2010 to 2018, non-white voters were highly likely to vote for Democrats,
with a >75% Democratic voter preference throughout all those elections. 
By contrast, white voters are more likely to vote for Republicans,
remaining between 40% and 45% throughout, with a low in 2016 and an encouraging uptick in
2018.

[BR:2018]: <${brGithubUrl (postPath Post2018)}#>
[BR:Methods]: <${brGithubUrl (postPath PostMethods)}#>
|]
  
brAcrossTimeASRPref :: T.Text
brAcrossTimeASRPref = [i|
There are more white than non-white
eligible voters and white voters have higher overall turnout.  We can see
this clearly in a chart of the shares of the Democratic electorate,
that is, fractions of Democratic votes.  Though non-white voters are overwhelmingly democratic,
they make up at most 40% of the votes Democrats get in any election.
|]

brAcrossTimeASRVoteShare :: T.Text
brAcrossTimeASRVoteShare = [i|
Grouping by educational attainment, age and sex, we a different picture of the
Democratic electorate.  Young college graduates and older female college
graduates are consistently democratic, but the younger voters are more Democratic
leaning by about 10 percentage points.  Older college-educated
men were Republican leaning until 2018 when they shifted to slight support
of the Democrats. We also see a very distinct shift:
beginning sometime after the 2012 election,
college-educated voters become more likely to vote for Democrats, and
non-college-educated voters become more likely to vote for Republicans.

When we looked at the breakdown by race rather than educational attainment, it
looked as if Democrats had gained with all groups from 2016 to 2018.  But looking
at educational attainment, it's clear that Democrats lost ground with
non-college-educated voters. Though the census data is not granular enough for us to
do the same analysis broken down by race *and* educational attainment, it seems likely
that the Democratic losses between 2016 and 2018 are largely
with white non-college-educated voters, the
so-called "White Working Class", something we discuss in some
detail in [another post][BR:WWC].

[BR:WWC]: <${brGithubUrl (postPath PostWWCV)}#>
|]

brAcrossTimeASEPref :: T.Text
brAcrossTimeASEPref = [i|
The movement of college-educated voters toward Democrats, while non-college-educated
voters move toward Republicans, is also clear in the Democratic vote-share.  In 2010,
Democrats got 60% of their votes from non-college-educated voters but that is down to
49% in 2018.
|]

brBreakingDownTheChanges :: T.Text
brBreakingDownTheChanges = [i|
## Breaking Down The Changes

People tell various stories about how elections are won or lost.  For example,
one story about the 2018 blue wave is that disaffected Republican voters stayed home,
while energized Democratic voters turned out in unexpected numbers.  Another story
is that significant numbers of voters shifted from voting Republican to Democratic.

We think there is a grain of truth in the first story but that the second is more
convincing.  To see why, we're going to look at some tables that break down the changes
in Democratic votes by our three factors: demographics, turnout and preference.  Before
we dive in, let's consider just one row of such a table,
looking at changes from 2016 to 2018:
|]

brAcrossTimeASRRow :: T.Text
brAcrossTimeASRRow = [i|
We have:

1. The name of the demographic group (older female non-college-graduates).
2. The population (44.7 million) in the ending year.
3. The change in Democratic votes coming from changes in population (-120,000 votes).
One important note here:  if this is a group that votes
for Democrats on average, increases in population lead to increases in Democratic votes
and a positive number in this column.  But if this is a group
that votes *against* Democrats, an increase in population is a *net loss*
of Democratic votes and the number in this column will be negative.  Older female non-college
educated voters tend to vote against Democrats so this net *loss* of votes comes from an *increase*
in population.
4.  The change in votes coming from changes in turnout (+693,000 votes).
Again, increased turnout may lead to a positive or
negative number here, depending on whether the group is more likely to vote for Democrats or Republicans.
In this case, *lower* turnout is leading to a net *gain* of Democratic votes.
5. The next column indicates votes gained or lost from shifts in voter preference (-745,000).
6. The total votes gained or lost in this group over the time period (-171,000 votes, the sum of columns 3, 4, and 5).
7. The total votes gained or lost (column 6) as a percentage of the total electorate (-0.08%).

Looking at all the groups together tells a more complete story. Overall,
changes in turnout does *not* appear to be a big net mover of votes.  Some Democratic
votes were gained by a drop
in turnout among older non-college-educated voters, but a similar number of Democratic
votes were lost by lower turnout among young college-educated voters.  Similarly,
demographic shifts play a *small* role---net gains among college-graduates outpace the aging of the
non-college-educated groups. Nearly all the Democratic vote gains arise
from changes in voter preference.
|]

brAcrossTimeTables :: T.Text
brAcrossTimeTables = [i|
It's instructive to look at the same groups but this time examining the changes from
2010 through 2018.  Here we see that voter preference also plays the largest role.  Over this
time range, demographics begins to play a larger role as well.  More people are getting
college degrees and that increases the size of the college-educated electorate, which
benefits Democrats on average.  Again, the turnout changes are fairly small across
all groups.  
|]

brAcrossTimeASRTable :: T.Text
brAcrossTimeASRTable = [i|
And it's worthwhile to look at these longer-term changes by race as well as by educational attainment.
Again we see that voter preference plays the largest role and that demographics is significant.
One distinction here is a noticeable trend to higher turnout
but one which generates more net votes among non-white
voters than are lost among white voters, leading to a net gain of Democratic votes.
|]

brAcrossTimeTableSummary :: T.Text
brAcrossTimeTableSummary = [i|
In our analysis, the blue
wave---the electorate shift just between 2016 and 2018---was
produced by voters who preferred Republicans in 2016,
shifting toward Democrats in 2018. We also see that current demographic shifts,
both in average educational attainment
and non-white vs. white voters, lead to more Democratic votes.  Turnout shifts are also
largely good for Democrats over the long-term.

It's important to note that this analysis is at the national level.
Each district has its own version of this story and we plan to dig into
those in more detail in the coming months.
|]
  
brAcrossTimeTakeAction :: T.Text
brAcrossTimeTakeAction = [i|
## Take Action
What's the takeaway here, in terms of concrete actions you can take?
Firstly, as always,
it's important to support organizations that fight for voting rights and work for
higher turnout among minority communities and young voters.  Though
turnout was not the driver of the blue wave, it was the base beneath it.  All
those "extra" votes don't matter, if the Democratic base doesn't (or can't) show
up on election day. So consider supporting:

- [Fair Fight][Org:FairFight], Stacey Abrams' organization dedicated to fighting for
voting rights in Georgia and beyond.
- [BLOC][Org:BLOC], community organizers in Milwaukee, WI,
working on [improving black voter turnout][WP:BLOC].
- [Campus Vote Project][Org:CampusVoteProject], an organization working on campuses
across the country to register college students, an
[important and suppressed][NYT:Students] source of
Democratic votes.

And stay focused on progressive policies where there is crossover support from independents and
centrist Republicans.
[Data For Progress][Org:DataForProgress] does extremely good work in this area and
has identified policies that progressives support which also poll well with centrist Democrats
and Republicans, e.g., capping credit-card interest rates or ending the war in Yemen.

Lastly, support *local* candidates who will rally turnout and engage undecided voters who may then
support progressives up and down the ticket. This is part of how Blue Ripple Politics looks at all
elections. Our current pieces on
[Louisiana][BR:LA],
[Kentucky and Mississippi][BR:KYandMS] highlight many great examples of this.


[Org:BLOC]: <https://www.blocbybloc.org>
[WP:BLOC]: <https://www.washingtonpost.com/politics/in-milwaukee-an-inner-city-group-tackles-a-key-democratic-need-turning-out-black-voters/2019/10/23/ceee17ea-e9d4-11e9-9c6d-436a0df4f31d_story.html>
[Org:DataForProgress]: <https://www.dataforprogress.org/>
[NYT:Students]: <https://www.nytimes.com/2019/10/24/us/voting-college-suppression.html>
[Org:FairFight]: <https://fairfight.com/>
[Org:CampusVoteProject]: <https://www.campusvoteproject.org>
[BR:LA]: <https://blueripplepolitics.org/blog/2019-la-leg>
[BR:KYandMS]: <https://blueripplepolitics.org/blog/2019-la-ky-ms>
|]
  
brAcrossTimeASR2012To2016 :: T.Text  
brAcrossTimeASR2012To2016 = [i|
The loss of Democratic votes comes almost entirely from shifts in opinion among
white voters, only slightly offset by demographic and turnout trends among
non-white voters.

The age/sex/education breakdown tells a related story, a waning
of Democratic preference among non-college graduates, especially older ones:
|]
  
brAcrossTimeASE2012To2016 :: T.Text
brAcrossTimeASE2012To2016 = [i|
One common thread in these tables is that the loss of votes comes
largely from shifts in opinion rather than demographics or turnout.
This is what often leaves Democrats feeling like they must appeal
more to white working class voters, as we talked about in
[this post][BR:WWCV].

## What Happened in the Battleground States?

One question we can ask is what happened to those voters in 2018?
In particular, let's consider just the battleground states of
NH, PA, VA, NV, FL, OH, MI, WI, IA, CO, AZ, and NV. First we'll
look again at the loss of votes from 2012 to 2016 and then
the blue wave of 2018.

[BR:WWCV]: <${brGithubUrl (postPath PostWWCV)}#>
|]

brAcrossTimeAfterBattleground :: T.Text
brAcrossTimeAfterBattleground = [i|
All of the approximately 2 million votes lost across those states
from 2012 to 2016 are
regained in 2018.  What's more, the gain comes from changes in
preference *not* changes in turnout.  This was not Republican voters staying
home, it was an electorate with similar turnout but which became
substantially more Democratic between 2016 and 2018. This happened
in an interesting way: from 2012 to 2016 Democrats lost share with
non-college educated voters while holding relatively steady with
college-educated voters.  The Blue wave in 2018 was *not* powered by
gaining back ground with the voters lost in 2016
but instead making 
inroads with college-educated white voters while maintaining turnout and
preference among the rest of the coalition.  That is, from 2012
to 2018, Democrats are about even with white voters but they have lost
non-college educated voters and gained college educated ones.

# Take Action

|]

  
  
brAcrossTimeXXXX :: T.Text
brAcrossTimeXXXX = [i|
Last war, etc.


Our interest in this model really comes from looking at the results across time
in an attempt to distinguish changes coming from demographic shifts, voter turnout
and voter preference.
The results are presented below. As in 2018, what stands out immediately is
the strong support of non-white voters for democratic candidates,
running at or above 75%, regardless of age or sex,
though support is somewhat stronger among non-white female voters
than non-white male voters[^2014]. Support from white voters is
substantially lower, between 37% and 49% across
both age groups and both sexes, though people
under 45 are about 4% more likely to vote democratic than their older
counterparts.  
As we move from 2016 to 2018, the non-white support holds,
maybe increasing slightly from its already high level,
and white support *grows* substantially across all ages and sexes,
though it remains below 50%. These results are broadly consistent with
exit-polling[^ExitPolls2012][^ExitPolls2014][^ExitPolls2016][^ExitPolls2018],
though there are some notable differences as well.

[BR:2018]: <${brGithubUrl (postPath Post2018)}#>
[BR:Methods]: <${brGithubUrl (postPath PostMethods)}#>
[BR:WWCV]: <${brGithubUrl (postPath PostWWCV)}$>
[^2014]: We note that there is a non-white swing towards republicans in 2014.
That is consistent with exit-polls that show a huge swing in the Asian vote:
from approximately 75% likely to vote democratic in 2012 to slightly *republican* leaning in 2014 and then
back to about 67% likely to vote democratic in 2016 and higher than 75% in 2018.
See, e.g., <https://www.nytimes.com/interactive/2018/11/07/us/elections/house-exit-polls-analysis.html>
[^ExitPolls2012]: <https://www.nytimes.com/interactive/2014/11/04/us/politics/2014-exit-polls.html#us/2012>
[^ExitPolls2014]: <https://www.nytimes.com/interactive/2014/11/04/us/politics/2014-exit-polls.html#us/2014>
[^ExitPolls2016]:  <https://www.nytimes.com/interactive/2016/11/08/us/politics/election-exit-polls.html>
[^ExitPolls2018]: <https://www.nytimes.com/interactive/2018/11/07/us/elections/house-exit-polls-analysis.html>,
<https://www.brookings.edu/blog/the-avenue/2018/11/08/2018-exit-polls-show-greater-white-support-for-democrats/>
|]

brAcrossTimeAfterASE :: T.Text
brAcrossTimeAfterASE = [i|
|]
--------------------------------------------------------------------------------  
voteShifts :: T.Text
voteShifts = [i|
So *some* of the 2018 democratic house votes came from
existing white voters changing their votes
while non-white support remained intensely high. Is that
the whole story?

Now we have an estimate of how peoples' choices changed between 2012 and 2018.
But that's only one part of the story.  Voting shifts are also driven by
changes in demographics (people move, get older, become eligible to vote
and people die) and different changes in voter turnout among different
demographic groups. In our simplistic model, we can look at these separately.

Below, we compare these changes (nationally) for each group for
2012 -> 2016 (both presidential elections),
2014 -> 2018 (both midterm elections) and
2016 -> 2018 (to look at the "Trump" effect). In each table the columns with "+/-" on
them indicate a net change in the (Democratic - Republican) vote totals coming from
that factor.  For example, if the "From Population" column is positive, that means
the change in population of that group between those years resulted in a net gain of
D votes.  NB: If that group was a net republican voting group then a rise in population
would lead to negative net vote change[^TableNote].

[^TableNote]: One tricky aspect of ascribing changes to one factor is that some of
the change comes from changes in two or more of the factors.  In this table, the
changes due to any pair of factors is split evenly between that pair and the
changes coming from all three are divvied up equally among all three.

|]
--------------------------------------------------------------------------------



voteShiftObservations :: T.Text
voteShiftObservations = [i|

The total changes are broadly in-line with the popular house vote totals
(all in thousands of votes)[^WikipediaHouse]:

Year   Democrats    Republicans   D - R
----- ----------   ------------  ------
2010  38,980       44,827        -4,847
2012  59,646       58,228        +1,418
2014  35,624       40,081        -4,457
2016  61,417       62,772        -1,355
2018  60,320       50,467        +9,853

when we look only at competitive districts, this via official result data:

Year   Democrats    Republicans   D - R
----- ----------   ------------  ------
2010  37,961       41,165        -3,204
2012  55,213       52,650        +2,563
2014  30,534       34,936        -4,402
2016  53,840       56,409        -2,569
2018  58,544       52,162        +6,382


These numbers tie out fairly well with the model.
This is by design: the model's turnout percentages are
adjusted in each district
so that the total votes in the district add up correctly.

* This model indicates a -4,700k shift (toward **republicans**)
2012 -> 2016 and the competitive popular house vote shifted -5,100k.
* This model indicates a +9,600k shift (toward **democrats**)
2014 -> 2018 and the competitive popular house vote shifted +10,800k.
* This model indicates a +6,800k shift (toward **democrats**)
2016 -> 2018 and the competitive popular house vote shifted +8,900k.
* This model indicates a +8,300k shift (toward **democrats**)
2010 -> 2018 and the competitive popular house vote shifted +9,600k. 

[^WikipediaHouse]: Sources:
<https://en.wikipedia.org/wiki/2010_United_States_House_of_Representatives_elections>
<https://en.wikipedia.org/wiki/2012_United_States_House_of_Representatives_elections>,
<https://en.wikipedia.org/wiki/2014_United_States_House_of_Representatives_elections>,
<https://en.wikipedia.org/wiki/2016_United_States_House_of_Representatives_elections>,
<https://en.wikipedia.org/wiki/2018_United_States_House_of_Representatives_elections>
|]



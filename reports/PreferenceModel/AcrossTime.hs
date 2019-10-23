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
import           BlueRipple.Data.DataFrames 
import           BlueRipple.Data.PrefModel.SimpleAgeSexRace
import           BlueRipple.Data.PrefModel.SimpleAgeSexEducation
import qualified BlueRipple.Model.Preference as PM

import PreferenceModel.Common


post :: K.KnitOne r
     => M.Map Int (PM.PreferenceResults SimpleASR FV.NamedParameterEstimate)
     -> M.Map Int (PM.PreferenceResults SimpleASE FV.NamedParameterEstimate)
     -> M.Map Int (PM.PreferenceResults SimpleASR FV.NamedParameterEstimate)
     -> M.Map Int (PM.PreferenceResults SimpleASE FV.NamedParameterEstimate)
     -> F.Frame HouseElections
     -> K.Sem r ()
post modeledResultsASR modeledResultsASE modeledResultBG_ASR modeledResultBG_ASE houseElectionsFrame = do  
            -- arrange data for vs time plot
  let flattenOneF y = FL.Fold
        (\l a -> (FV.name a, y, FV.value $ FV.pEstimate a) : l)
        []
        reverse
      flattenF = FL.Fold
        (\l (y, pr) -> FL.fold (flattenOneF y) (PM.modeled pr) : l)
        []
        (concat . reverse)
      vRowBuilderPVsT =
        FV.addRowBuilder @'("Group",T.Text) (\(g, _, _) -> g)
        $ FV.addRowBuilder @'("Election Year",Int) (\(_, y, _) -> y)
        $ FV.addRowBuilder @'("D Voter Preference (%)",Double) (\(_, _, vp) -> 100*vp)
        $ FV.emptyRowBuilder
      vDatPVsT :: M.Map Int (PM.PreferenceResults b FV.NamedParameterEstimate)
               -> [FV.Row
                    '[ '("Group", F.Text), '("Election Year", Int),
                       '("D Voter Preference (%)", Double)]] 
      vDatPVsT pr =
        FV.vinylRows vRowBuilderPVsT $ FL.fold flattenF $ M.toList pr
      addParametersVsTime :: K.KnitOne r
                          => M.Map Int (PM.PreferenceResults b FV.NamedParameterEstimate)
                          -> K.Sem r ()
      addParametersVsTime pr = do 
        let vl =
              FV.multiLineVsTime @'("Group",T.Text) @'("Election Year",Int)
              @'("D Voter Preference (%)",Double)
              "D Voter Preference Vs. Election Year"
              FV.DataMinMax
              (FV.TimeEncoding "%Y" FV.Year)
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
                     => M.Map Int (PM.PreferenceResults b FV.NamedParameterEstimate)
                     -> K.Sem r ()
    addStackedArea prMap = do
      let vl = FV.stackedAreaVsTime @'("Group",T.Text) @'("Election Year",Int)
               @'("Voteshare of D Votes (%)",Double)
               "Voteshare in Competitive Districts vs. Election Year"
               (FV.TimeEncoding "%Y" FV.Year)
               (FV.ViewConfig 800 400 10)
               (vDatSVS prMap)
      _ <- K.addHvega Nothing Nothing vl
      return ()
    mkDeltaTableASR mr locFilter (y1, y2) mRows greenOpinionGroups = do
      let y1T = T.pack $ show y1
          y2T = T.pack $ show y2
      brAddMarkDown $ "### " <> y1T <> "->" <> y2T
      mry1 <- knitMaybe "lookup failure in mwu"
              $ M.lookup y1 mr
      mry2 <- knitMaybe "lookup failure in mwu"
              $ M.lookup y2 mr
      (table, (mD1, mR1), (mD2, mR2)) <-
        PM.deltaTable simpleAgeSexRace locFilter houseElectionsFrame y1 y2 mry1 mry2
      let table' = case mRows of
            Nothing -> table
            Just n -> take n $ FL.fold FL.list table
      let greenOpinion g = g `elem` greenOpinionGroups           
      brAddRawHtmlTable (BHA.class_ "br_table") (PM.deltaTableColonnadeBlaze greenOpinion)  $ table'
    mkDeltaTableASE mr locFilter  (y1, y2) mRows greenOpinionGroups = do
      let y1T = T.pack $ show y1
          y2T = T.pack $ show y2
      brAddMarkDown $ "### " <> y1T <> "->" <> y2T
      mry1 <- knitMaybe "lookup failure in mwu"
              $ M.lookup y1 mr
      mry2 <- knitMaybe "lookup failure in mwu"
              $ M.lookup y2 mr
      (table, (mD1, mR1), (mD2, mR2)) <-
        PM.deltaTable simpleAgeSexEducation locFilter houseElectionsFrame y1 y2 mry1 mry2        
      let table' = case mRows of
            Nothing -> table
            Just n -> take n $ FL.fold FL.list table
      let greenOpinion g = g `elem` greenOpinionGroups
      brAddRawHtmlTable (BHA.class_ "br_table") (PM.deltaTableColonnadeBlaze greenOpinion) table
        
  brAddMarkDown brAcrossTimeIntro
  addParametersVsTime  modeledResultsASR
  brAddMarkDown brAcrossTimeASRPref
  addStackedArea modeledResultsASR
  brAddMarkDown brAcrossTimeASRVoteShare           
  addParametersVsTime  modeledResultsASE
  brAddMarkDown brAcrossTimeASEPref
  addStackedArea modeledResultsASE
  brAddMarkDown brAcrossTimeASEVoteShare           
  brAddMarkDown brAcrossTimeAfterASE
  -- one row to talk about
  mkDeltaTableASR modeledResultsASR  (const True) (2012, 2016) (Just 1) []
  -- analyze results
  mkDeltaTableASR  modeledResultsASR  (const True) (2012, 2016) Nothing []
  brAddMarkDown brAcrossTimeASR2012To2016
  mkDeltaTableASE  modeledResultsASE (const True) (2012, 2016) Nothing ["YoungFemaleCollegeGrad"]
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
  _ <- mkDeltaTableASE  modeledResultBG_ASE bgOnly (2012, 2016) Nothing []
  _ <- mkDeltaTableASE  modeledResultBG_ASE bgOnly (2016, 2018) Nothing []
  brAddMarkDown brAcrossTimeAfterBattleground
  brAddMarkDown brReadMore

brAcrossTimeIntro :: T.Text
brAcrossTimeIntro = [i|

In our previous [post][BR:2018] we introduced a
[model][BR:Methods] using census data and election results to infer voter
preference for various demographic groups in the 2018 house elections.
In this post we'll look back at the past few elections and examine
how did Democrats gained and lost votes. Our key takeaway is that most
of the change has been from people changing their minds. Demographic changes
have created Democratic votes and there is a lot of room to gain votes via turnout as well.
But the primary driver of election-to-election change has been voter preference.

Whatever happened in the past is not a simple guide
to the future. How we won in 2018 may not be how we win in 2019 or 2020.
However, understanding what happened in previous elections
is a good place to start as we look to hold and expand on the gains made in 2018.

1. The Evolving Democratic Coalition
2. Breaking Down the Changes
3. What Happened in the Battleground States?
4. Take Action

## The Evolving Democratic Coalition

As before, we try to understand election results by considering voter preference and turnout in various
demographic groups.  In particular, we look at

- Demographics: What are the age, sex, education level, race, etc. of the eligible voters?
- Turnout: For each demographic group, what fraction of those eligible to vote cast votes on election day?
- Voter Preference: How likely are people from each group to vote for the Democratic candidate?

Once we have some estimate of all three components and can see how they change over
time, we can look at how votes were lost and gained by Democrats from 2010 to 2018.
Consider a break down by age, sex and race. As was true in 2018, from 2010 to 2018, non-white
voters vote for Democrats more than 70% of the time whereas white voters vote for Democrats
less than 50% of the time. 

[BR:2018]: <${brGithubUrl (postPath Post2018)}#>
[BR:Methods]: <${brGithubUrl (postPath PostMethods)}#>
|]
  
brAcrossTimeASRPref :: T.Text
brAcrossTimeASRPref = [i|
Also useful is to look at shares of the electorate rather than preference numbers.  Electorate shares
put all three components together but make it clearer how mnay votes come from each group.
|]

brAcrossTimeASRVoteShare :: T.Text
brAcrossTimeASRVoteShare = [i|
This chart reminds us that though non-white voters are overwhelmingly democratic,
they make up only about 40% of the Democratic votes.
This is a combination of smaller
numbers of possible voters and lower turnout.  The latter is
something there is ongoing work to change via anti-voter-suppression efforts,
registration drives and get-out-the-vote (GOTV) work.

As before, we also group things by age, sex and educational attainment.  First
the voter preferences through time:
|]

brAcrossTimeASEPref :: T.Text
brAcrossTimeASEPref = [i|
This shows a very distinct shift toward Democrats of college-educated voters in 2018
and an equally distinct shift toward Republicans of non-college-educated voters in 2014.  

In terms of vote-share:
|]

brAcrossTimeASEVoteShare :: T.Text
brAcrossTimeASEVoteShare = [i|
Again we see that, in terms of these groups,
the Democratic coalition is made up of a large number
of votes from groups that are not majority democratic voters and smaller
numbers of votes from many groups which do tend to vote for Democrats.

Also of note in both vote-share charts is the tendency of Democratic vote-share
to fall off in mid-term years, except for 2018.  That's something to keep in
mind and work against in 2022!

## Breaking Down The Changes

One way to look at all this data is to break-down the changes in
total Democratic votes in each group into our 3 categories: demographics, turnout
and preference. 
For example, the following table illustrates the age/sex/race break-down in the loss of
democratic votes between 2012 and 2016:
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



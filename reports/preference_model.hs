{-# LANGUAGE DataKinds                 #-}
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

module Main where

import qualified Control.Foldl                 as FL
import qualified Control.Monad.Except          as X
import           Control.Monad.IO.Class         ( MonadIO(liftIO) )
import qualified Colonnade                     as C
import qualified Data.List                     as L
import qualified Data.Map                      as M
import qualified Data.Array                    as A
import           Data.Maybe                     ( catMaybes
                                                )
import qualified Data.Vector                   as VB
import qualified Data.Vector.Storable          as VS                 

import qualified Text.Pandoc.Error             as PA

import qualified Data.Profunctor               as PF
import qualified Data.Text                     as T
import qualified Data.Time.Calendar            as Time
import qualified Data.Time.Clock               as Time
import qualified Data.Time.Format              as Time
import qualified Data.Vinyl                    as V
import qualified Text.Printf                   as PF
import qualified Frames                        as F
import qualified Frames.Melt                   as F
import qualified Frames.CSV                    as F
import qualified Frames.InCore                 as F
                                         hiding ( inCoreAoS )

import qualified Pipes                         as P
import qualified Pipes.Prelude                 as P
import qualified Statistics.Types              as S
import qualified Statistics.Distribution       as S
import qualified Statistics.Distribution.StudentT      as S


import           Numeric.MCMC.Diagnostics       ( summarize
                                                , ExpectationSummary(..)
--                                                , mpsrf
--                                                , mannWhitneyUTest
                                                )
import qualified Numeric.LinearAlgebra         as LA

import qualified Graphics.Vega.VegaLite        as GV
import           Graphics.Vega.VegaLite.Configuration as FV
import qualified Graphics.Vega.VegaLite.Compat as FV

import qualified Frames.Visualization.VegaLite.Data
                                               as FV
import qualified Frames.Visualization.VegaLite.StackedArea
                                               as FV
import qualified Frames.Visualization.VegaLite.LineVsTime
                                               as FV
import qualified Frames.Visualization.VegaLite.ParameterPlots
                                               as FV                                               

import qualified Frames.Visualization.VegaLite.Correlation
                                               as FV                                               
                                               
import qualified Frames.Transform              as FT
import qualified Frames.Folds                  as FF
import qualified Frames.MapReduce              as MR
import qualified Frames.Enumerations           as FE

import qualified Knit.Report                   as K
import qualified Knit.Report.Input.MarkDown.PandocMarkDown    as K
import           Polysemy.Error                 (Error)
import qualified Text.Pandoc.Options           as PA

import           Data.String.Here               ( here )

import           BlueRipple.Utilities.KnitUtils
import           BlueRipple.Data.DataFrames
import           BlueRipple.Data.PrefModel
import           BlueRipple.Data.PrefModel.SimpleAgeSexRace
import           BlueRipple.Data.PrefModel.SimpleAgeSexEducation
import qualified BlueRipple.Model.PreferenceBayes as PB
import qualified BlueRipple.Model.TurnoutAdjustment as TA

yamlAuthor :: T.Text
yamlAuthor = [here|
- name: Adam Conner-Sax
- name: Frank David
|]

templateVars = M.fromList
  [ ("lang"     , "English")
--  , ("author"   , T.unpack yamlAuthor)
--  , ("pagetitle", "Preference Model & Predictions")
  , ("site-title", "Blue Ripple Politics")
  , ("home-url", "https://www.blueripplepolitics.org")
--  , ("tufte","True")
  ]

--pandocTemplate = K.FromIncludedTemplateDir "mindoc-pandoc-KH.html"
pandocTemplate = K.FullySpecifiedTemplatePath "pandoc-templates/blueripple_basic.html"

--------------------------------------------------------------------------------
br2018Intro :: T.Text
br2018Intro = [here|
In our first deep-dive into some modeling, we take a quick look at the 2018
house elections.  What can we do with data from previous elections to guide
our use of time and money in 2019 and 2020?

1. Estimating voter preference
2. Who voted for Democrats and Progressives?
3. What does this mean for 2019 and 2020?
5. Action

## Estimating Voter Preference
There are many ways to partition the electorate, e.g., age, educational
attainment, race or sex.  We can also group people by answers to specific
questions about issues or previous votes.  In subsequent posts we will look at all
these possibilities, or point to other sources which do.  For our first look we're
going to stick with some standard demographic groupings which originate with the
[Census ACS data](https://www.census.gov/programs-surveys/acs.html). We'll look first
at grouping the electorate by age (old/young = over/under 45), sex (female or male),
and race (non-white and white), as well as the same age and sex categories but
with a split on education (non-college-grad or college-grad) instead of race.

We know that these categories are oversimplifications and problematic in
various ways.  But we are limited to the categories the census provides and
we want the number of groups to be reasonably small for this analysis to be useful.

[Edison Research](https://www.edisonresearch.com/election-polling/#one)
did extensive exit-polling in 2018
and, along with others,
[CNN analyzed](https://www.cnn.com/election/2018/exit-polls)
this data, providing some breakdowns along these categories.
[Much reporting](https://www.vox.com/policy-and-politics/2018/11/7/18064260/midterm-elections-turnout-women-trump-exit-polls)
has used these results to
[discuss](https://www.nytimes.com/interactive/2018/11/07/us/elections/house-exit-polls-analysis.html)
the
[2018 electorate](https://www.brookings.edu/blog/the-avenue/2018/11/08/2018-exit-polls-show-greater-white-support-for-democrats/).
However, there is
[widespread criticism](https://www.nytimes.com/2014/11/05/upshot/exit-polls-why-they-so-often-mislead.html) of exit-polling.

Is there a different way to see which groups voted in what numbers for Democrats?
In the rest of this post, and some subsequent posts, we'll explore a
[different approach](https://blueripple.github.io/PreferenceModel/MethodsAndSources.html).
What if, rather than polling, we just used the *election results* themselves?
How would that work? Here's the date used:

- [Census data](https://www.census.gov/topics/public-sector/voting/data/tables.2018.html)
for (national) turnout of each group.
- [Census data](https://www.census.gov/programs-surveys/acs.html)
for the demographic breakdown of each house district.
- The [election result](https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/IG0UN2)
in each comptetive house district.

What are the odds of a voter in any of these demographic groups voting for
a Democrat? Any set of voter preferences (those odds), give rise to
a probability, given the turnout and demographics, of the election result we
observe in a specific district.
In 2018, We had 382 districts with at least one democrat and one republican
running (we ignore the rest), and we can combine all those to get the chance that
a particular set of voter preferences explains all the results.  From this, we
infer the most-likley voter preferences.
The details are spelled out in a 
[separate post](https://blueripple.github.io/PreferenceModel/MethodsAndSources.html).

## Who Voted For Democrats and Progressives?
In the charts below, we show our inferred voter preference split by demographic group.
We use size to indicate the number of voters in each group and the color to
signify turnout.
|]
    
br2018BetweenFigs :: T.Text
br2018BetweenFigs = [here|
The most striking observation is the gap between white and non-white voters’
inferred support for Democrats in 2018. Non-whites
have over 75% preference for Dems regardless of age or gender,
though support is stronger among non-white female voters than
non-white male voters. Inferred support from white voters in 2018
is substantially lower, roughly 40-50% across age groups and genders.

Splitting instead by age, sex and education:
|]

br2018AfterFigs :: T.Text
br2018AfterFigs = [here|
Here we see a large (>15% point) gap between more-democratic-leaning
college-educated voters and voters without a
four-year degree.  We also see a similar gap with age, where younger
voters are 15% or so more likely to vote for democrats. 

In both charts, the size of the circles reflects the number of people of
voting age in each group (within the competitive districts).  In 2018
democrats won almost all the groups we consider here but the groups where
they lag have the largest number of actual voters.  The charts also make
clear that voter turnout among young voters is relatively low and
raising that would generally be a boon for democrats.

These results are similar but markedly different from the exit-polls.
[This post](https://blueripple.github.io/PreferenceModel/ExitPolls.html)
has more details on that comparison.

## What does this mean for 2019 and 2020?
These charts can be viewed as pictures of the so-called Democratic
"base"--voters of color and younger white voters, particularly those
with a college degree.
Winning more votes comes from raising turnout among that
coalition *and* trying to improve Democratic voter preference
with everyone else.

But this isn't really a national question!  Most candidates
win local races and, because of the
[electoral college](https://www.nationalpopularvote.com/),
presidential elections also
hinge on very local outcomes. So, in the near future,
we plan to investigate the issue of turnout and changing
voter-preference for single
states or congressional districts.  We hope to use that
analysis to help interested progressives discover where
turnout is key vs. places where changing minds might be
more important. Both are important everywhere, of course.
But in some places, one might be a much larger source of
democratic votes.

## Action
Democrats and progressives will benefit
from get-out-the-vote work and work fighting voter suppression.
Both improve turnout among the groups most likely to vote
for Dems and progressives.

Donate your time or money to organizations fighting to improve voter
access and turnout:

- [Demand The Vote](https://www.demandthevote.com/) is an information
clearinghouse for voter access and voting rights legislation in all 50 states.
and link to variety of ways to put your time or money into voter access and turnout.
- [When We All Vote](https://www.whenweallvote.org/), Michelle Obama's new
organization, works to increase voter turnout nationally and has opportunities to
volunteer and donate money.
- The [Campus Vote Project](https://www.campusvoteproject.org/why-student-voters-matter) works
to improve registration and turnout among college students.
Students can [sign up](https://www.campusvoteproject.org/sign-up) to work on their own campuses.
- [Next Gen America](https://nextgenamerica.org/) works nationally to improve youth turnout.
They have a [variety of ways](https://nextgenamerica.org/act/) you can donate time or money to the cause. 

And here's our take on some crucial state races coming up in 2019.  State
legislatures, Governors and state Secretaries-of-State have a large impact on
voter access:

- [Crucial 2019 legislative races in VA and LA](http://www.blueripplepolitics.org/blog/ky-la-2019)
- [Other important 2019 state races](http://www.blueripplepolitics.org/blog/state-level-races)
|]                
--------------------------------------------------------------------------------
brExitPolls :: T.Text
brExitPolls = [here|
The press has various breakdowns of the 2018 exit polling done by
Edison Research.  None split things into the same categories we
looked at in our
[post on inferred voter preference in 2018 house elections](https://blueripple.github.io/PreferenceModel/2018.html). But
we can merge some of our categories and then compare. We see rough agreement but also some very
large discrepancies that we have yet to explain.
|]

brExitAR :: T.Text
brExitAR = [here|
After merging genders, our preference model tracks the exit polls quite well. 
|]

brExitSR :: T.Text
brExitSR = [here|
A fairly large discrepancy appears when we look at sex and race, merging ages.
Our model infers similar voting preferences for white men and women and non-white
men and women whereas the exit polls show women about 10% more likely to vote
for Democrats.
|]

brExitSE :: T.Text
brExitSE = [here|
An even larger discrepancy appears when we look at sex and education, merging
our race categories.
Our inferred result for male college graduates is a full 15% higher than the
exit polls, while our result for female non-college graduates is almost 10%
below the exit-polls.
|]

  
--------------------------------------------------------------------------------
brAcrossTimeIntro :: T.Text
brAcrossTimeIntro = [here|

In our previous [post][BR:2018] we introduced a
[model][BR:Methods] which we used to infer voter
preference for various demographic groups in the 2018 house elections, using census
data and election results. Here we look
at how those preferences have shifted since 2010.

1. The Evolving Democratic Coalition
2. Breaking Down the Changes
3. Thoughts on 2020 (and Beyond)

## The Evolving Democratic Coalition

Democratic votes can come from turnout---getting a Democratic voter registered and/or to
the polls---or from changes in voter preference---basically, changing the
opinion of a non-Democratic voter.  Further, the
demographics of the voting population in a district change over time which shifts
how these factors add up to an election result. 

Consider the voter preference broken down by age, sex and race from 2010 to 2018:

[BR:2018]: <https://blueripple.github.io/PreferenceModel/2018.html#>
[BR:Methods]: <https://blueripple.github.io/PreferenceModel/MethodsAndSources.html#>
|]
  
brAcrossTimeASRPref :: T.Text
brAcrossTimeASRPref = [here|
As before, the difference in voter preference between non-white and white voters is stark.
There is an encouraging move in white voter preference toward Democrats in 2018.
We can also look at how those numbers translate to the composition of the democratic electorate:
|]

brAcrossTimeASRVoteShare :: T.Text
brAcrossTimeASRVoteShare = [here|
This chart reminds us that though non-white voters are overwhelmingly democratic,
they only make up roughly 40% of the Democratic votes.
This is a combination of smaller
numbers of possible voters and lower turnout.  The latter is
something there is ongoing work to change via anti-voter-suppression efforts
and registration and GOTV drives.

As before we can also group things by age, sex and educational attainment.  First
the voter preferences through time:
|]

brAcrossTimeASEPref :: T.Text
brAcrossTimeASEPref = [here|
This shows a very distinct shift toward Democrats of all groups except older
non-college graduates, who have become more Republican since 2012. Younger
non-college graduates shifted republican in 2012 but shifted back in 2018.

Again, we can also look at this data in terms of vote-share:
|]

brAcrossTimeASEVoteShare :: T.Text
brAcrossTimeASEVoteShare = [here|
Here we see again that the Democratic coalition is made up of a large number
of voters from groups that are not majority democratic voters and smaller
numbers of votes from many groups which do tend to vote for Democrats.

Also of note in both vote-share charts is the tendency of Democratic vote-share
to fall off in mid-term years, except for 2018.  That's something to keep in
mind and work against in 2022!

One interesting way to look at all this data is to break-down the changes in
total Democratic votes in each group into 3 categories: demographics, turnout
and preference.  Demographics tracks the changes in the size of these different
groups, whereas turnout and preference track changes in the behavior of each group.
For example, the following table illustrates the age/sex/race break-down in the loss of
democratic votes between 2012 and 2016:
|]

brAcrossTimeASR2012To2016 :: T.Text  
brAcrossTimeASR2012To2016 = [here|
The loss of Democratic votes comes almost entirely from shifts in opinion among
white voters, only slightly offset by demographic and turnout trends among
non-white voters.

The age/sex/education breakdown tells a related story, a waning
of Democratic preference
among non-college graduates, especially older ones:
|]
  
brAcrossTimeASE2012To2016 :: T.Text
brAcrossTimeASE2012To2016 = [here|
One common thread in these tables is that the loss of votes comes
largely from shifts in opinion rather than demographics or turnout.
This is what often leaves Democrats feeling like they must appeal
more to white voters, particularly white, non-college educated
Democrats.  From this data it's clear that those are the voters
that were lost between 2012 and 2016.  Did they come back in 2018?

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

[BR:2018]: <https://blueripple.github.io/PreferenceModel/2018.html#>
[BR:Methods]: <https://blueripple.github.io/PreferenceModel/MethodsAndSources.html#>
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
brAcrossTimeAfterASE = [here|
|]
--------------------------------------------------------------------------------  
voteShifts :: T.Text
voteShifts = [here|
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
voteShiftObservations = [here|

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

  
main :: IO ()
main = do
--  let template = K.FromIncludedTemplateDir "mindoc-pandoc-KH.html"
--  let template = K.FullySpecifiedTemplatePath "pandoc-templates/minWithVega-pandoc.html"
  let brMarkDownReaderOptions =
        let exts = PA.readerExtensions K.markDownReaderOptions
        in PA.def
           { PA.readerStandalone = True
           , PA.readerExtensions = PA.enableExtension PA.Ext_smart exts
           }
      brAddMarkDown :: K.KnitOne r => T.Text -> K.Sem r ()
      brAddMarkDown = K.addMarkDownWithOptions brMarkDownReaderOptions
      brWriterOptionsF :: PA.WriterOptions -> PA.WriterOptions
      brWriterOptionsF o =
        let exts = PA.writerExtensions o
        in o { PA.writerExtensions = PA.enableExtension PA.Ext_smart exts
             , PA.writerSectionDivs = True
             }
  pandocWriterConfig <- K.mkPandocWriterConfig pandocTemplate
                                               templateVars
                                               brWriterOptionsF
  eitherDocs <-
    K.knitHtmls (Just "preference_model.Main") K.nonDiagnostic pandocWriterConfig $ do
    -- load the data   
      let parserOptions =
            F.defaultParser { F.quotingMode = F.RFC4180Quoting ' ' }
      K.logLE K.Info "Loading data..."
      contextDemographicsFrame :: F.Frame ContextDemographics <- loadToFrame
        parserOptions
        contextDemographicsCSV
        (const True)
      asrDemographicsFrame :: F.Frame ASRDemographics <-
        loadToFrame parserOptions ageSexRaceDemographicsLongCSV (const True)
      aseDemographicsFrame :: F.Frame ASEDemographics <-
        loadToFrame parserOptions ageSexEducationDemographicsLongCSV (const True)        
      houseElectionsFrame :: F.Frame HouseElections <- loadToFrame
        parserOptions
        houseElectionsCSV
        (const True)
      asrTurnoutFrame :: F.Frame TurnoutASR <- loadToFrame
        parserOptions
        detailedASRTurnoutCSV
        (const True)
      aseTurnoutFrame :: F.Frame TurnoutASE <- loadToFrame
        parserOptions
        detailedASETurnoutCSV
        (const True)
      edisonExit2018Frame :: F.Frame EdisonExit2018 <- loadToFrame
        parserOptions
        exitPoll2018CSV
        (const True)
--      edisonExit2018Array <- knitMaybe "Failed to make array from 2018 Edison exit-poll data!" $
--        FL.foldM (FE.makeArrayMF (read @SimpleASR . T.unpack . F.rgetField @Group) (F.rgetField @DemPrefPct) const) edisonExit2018Frame
      K.logLE K.Info "Inferring..."
      let yearList :: [Int]   = [2010, 2012, 2014, 2016, 2018]
          years      = M.fromList $ fmap (\x -> (x, x)) yearList
          categoriesASR = fmap (T.pack . show) $ dsCategories simpleAgeSexRace
          categoriesASE = fmap (T.pack . show) $ dsCategories simpleAgeSexEducation
      
      modeledResultsASR <- modeledResults simpleAgeSexRace asrDemographicsFrame asrTurnoutFrame houseElectionsFrame years 
      modeledResultsASE <- modeledResults simpleAgeSexEducation aseDemographicsFrame aseTurnoutFrame houseElectionsFrame years 

      K.logLE K.Info "Knitting docs..."
      curDate <- (\(Time.UTCTime d _) -> d) <$> K.getCurrentTime
      let formatTime t = Time.formatTime Time.defaultTimeLocale "%B %e, %Y" t
          curDateString = formatTime curDate
          addDates :: Time.Day -> Time.Day -> M.Map String String -> M.Map String String
          addDates pubDate updateDate tMap =
            let pubT = M.singleton "published" $ formatTime pubDate
                updT = M.singleton "updated" $ formatTime updateDate
            in tMap <> pubT <> if (updateDate > pubDate) then updT else M.empty
          -- group name, voting pop, turnout fraction, inferred dem vote fraction  
          pubDate2018 = Time.fromGregorian 2019 9 5
      K.newPandoc
          (K.PandocInfo
            "2018"
            (addDates pubDate2018 curDate
             $ M.fromList [("pagetitle", "Digging into 2018 -  National Voter Preference")
                          ,("title", "Digging into the 2018 House Election Results")             
                          ]
            )
          )
        $ do
            brAddMarkDown br2018Intro
            let groupData :: (A.Ix b, Show b)
                          => PreferenceResults b FV.NamedParameterEstimate
                          -> b
                          -> (T.Text, Int, Double, Double)
                groupData pr x = (T.pack (show x)
                                 , nationalVoters pr A.! x
                                 , nationalTurnout pr A.! x
                                 , FV.value $ FV.pEstimate  $ modeled pr A.! x
                                 )
                groupDataList :: (Enum b, Bounded b, A.Ix b, Show b)
                              => PreferenceResults b FV.NamedParameterEstimate
                              -> [(T.Text, Int, Double, Double)]
                groupDataList pr = fmap (groupData pr) [minBound..maxBound]
                vRowBuilderGD =
                  FV.addRowBuilder @'("Group",T.Text) (\(g,_,_,_) -> g)
                  $ FV.addRowBuilder @'("VotingAgePop",Int) (\(_,v,_,_) -> v)
                  $ FV.addRowBuilder @'("Turnout",Double) (\(_,_,t,_) -> t)
                  $ FV.addRowBuilder @'("Voters",Int) (\(_,v, t :: Double ,_) -> round (t * realToFrac v))
                  $ FV.addRowBuilder @'("D Voter Preference",Double) (\(_,_,_,p) -> 100*p)
                  $ FV.emptyRowBuilder
                perGroupingChart :: forall b r. (A.Ix b, Enum b, Show b, Ord b, Bounded b, K.KnitOne r)
                                 => T.Text
                                 -> Int
                                 -> M.Map Int (PreferenceResults b FV.NamedParameterEstimate)
                                 -> K.Sem r ()
                perGroupingChart title yr mr = do
                  pr <-
                    knitMaybe ("Failed to find " <> (T.pack $ show yr) <> " in modelResults.")
                    $ M.lookup yr mr
                  let groupDataL = groupDataList pr
                      gdRows = FV.vinylRows vRowBuilderGD groupDataL
                  _ <- K.addHvega Nothing Nothing
                       $ vlGroupingChart
                       title
                       (FV.ViewConfig 650 325 0)
                       gdRows
                  return ()
            prGDASR2018 <- perGroupingChart "Grouped by Age, Race, and Sex" 2018 modeledResultsASR
            brAddMarkDown br2018BetweenFigs
            prGDASE2018 <- perGroupingChart "Grouped by Age, Sex and Education" 2018 modeledResultsASE
            brAddMarkDown br2018AfterFigs
            return ()

      K.newPandoc
        (K.PandocInfo
         "ExitPolls"
          (addDates pubDate2018 curDate
           $ M.fromList [("pagetitle", "Comparison of inferred preference model to Edison exit polls.")
                        ,("title","Comparing the Inferred Preference Model to the Exit Polls")
                        ]
          )
        )
        $ do
          brAddMarkDown brExitPolls
          let mergedPrefs :: (A.Ix b, Enum b, Bounded b, A.Ix c, Enum c, Bounded c)
                          => PreferenceResults b FV.NamedParameterEstimate
                          -> (A.Array b Double -> A.Array c Double)
                          -> A.Array c Double
              mergedPrefs pr mergeF =
                let mergedVAP = mergeF (fmap realToFrac $ nationalVoters pr)
                    voters = totalArrayZipWith (*) (fmap realToFrac $ nationalVoters pr) (nationalTurnout pr)
                    mergedVoters = mergeF voters
                    mergedTurnout = totalArrayZipWith (/) mergedVoters mergedVAP
                    dVotes = totalArrayZipWith (*) (fmap (FV.value . FV.pEstimate) $ modeled pr) voters
                    mergedDVotes = mergeF dVotes
                in totalArrayZipWith (/) mergedDVotes mergedVoters                
              groupDataMerged :: forall b c r. (Enum b , Bounded b, A.Ix b, Show b
                                               ,Enum c, Bounded c, A.Ix c, Show c, K.KnitOne r)
                             => PreferenceResults b FV.NamedParameterEstimate
                             -> F.Frame EdisonExit2018
                             -> (A.Array b Double -> A.Array c Double)
                             -> K.Sem r [(T.Text, Double, Double)]
              groupDataMerged pr exits mergeF = do
                   let allGroups :: [c] = [minBound..maxBound]
                       modeledPrefs = mergedPrefs pr mergeF
                       exitsRow c = knitEither $
                         case FL.fold FL.list (F.filterFrame ((== T.pack (show c)) . F.rgetField @Group) exits) of
                           [] -> Left ("No rows in exit-poll data for group=" <> (T.pack $ show c))
                           [x] -> Right x
                           _ -> Left (">1 in exit-poll data for group=" <> (T.pack $ show c))
                       dPrefFromExitsRow r =
                         let dFrac = F.rgetField @DemPref r
                             rFrac = F.rgetField @RepPref r
                         in dFrac/(dFrac + rFrac)
                   exitPrefs <- fmap dPrefFromExitsRow <$> traverse exitsRow allGroups
                   return $ zip3 (fmap (T.pack .show) allGroups) (A.elems modeledPrefs) exitPrefs
              withExitsRowBuilder =
                FV.addRowBuilder @'("Group",T.Text) (\(g,_,_) -> g)
                $ FV.addRowBuilder @'("Model Dem Pref",Double) (\(_,mp,_) -> mp)
                $ FV.addRowBuilder @'("ModelvsExit",Double) (\(_,mp,ep) -> mp - ep)
                $ FV.emptyRowBuilder
              compareChart :: (A.Ix b, Enum b, Bounded b, Show b, A.Ix c, Enum c, Bounded c, Show c, K.KnitOne r)
                           => T.Text
                           -> Maybe T.Text
                           -> M.Map Int (PreferenceResults b FV.NamedParameterEstimate)                           
                           -> F.Frame EdisonExit2018
                           -> (A.Array b Double -> A.Array c Double)
                           -> K.Sem r ()
              compareChart title captionM mr exits mergeF = do
                pr <-
                  knitMaybe ("Failed to find 2018 in modelResults.")
                  $ M.lookup 2018 mr
                gdm <- groupDataMerged pr exits mergeF
                let ecRows = FV.vinylRows withExitsRowBuilder gdm 
                _ <- K.addHvega Nothing captionM $ exitCompareChart title (FV.ViewConfig 650 325 0) ecRows
                return ()
          compareChart "Age and Race" (Just brExitAR) modeledResultsASR edisonExit2018Frame simpleASR2SimpleAR
          compareChart "Sex and Race" (Just brExitSR) modeledResultsASR edisonExit2018Frame simpleASR2SimpleSR
          compareChart "Sex and Education" (Just brExitSE) modeledResultsASE edisonExit2018Frame simpleASE2SimpleSE
          return ()
      K.newPandoc
          (K.PandocInfo
            "MethodsAndSources"
            (addDates pubDate2018 curDate
              $ M.fromList [("pagetitle", "Inferred Preference Model: Methods & Sources")
                           ]
            )
          )
        $ brAddMarkDown modelNotesBayes
      let pubDateAcrossTime = Time.fromGregorian 2019 9 19  
      K.newPandoc
          (K.PandocInfo
            "AcrossTime"
            (addDates pubDateAcrossTime curDate
            $ M.fromList [("pagetitle", "Voter Preference: 2010-2018")
                         ,("title", "Voter Preference: 2010-2018")
                         ]
            )
          )
        $ do

            -- arrange data for vs time plot
            let flattenOneF y = FL.Fold
                  (\l a -> (FV.name a, y, FV.value $ FV.pEstimate a) : l)
                  []
                  reverse
                flattenF = FL.Fold
                  (\l (y, pr) -> FL.fold (flattenOneF y) (modeled pr) : l)
                  []
                  (concat . reverse)
                vRowBuilderPVsT =
                  FV.addRowBuilder @'("Group",T.Text) (\(g, _, _) -> g)
                  $ FV.addRowBuilder @'("Election Year",Int) (\(_, y, _) -> y)
                  $ FV.addRowBuilder @'("D Voter Preference (%)",Double) (\(_, _, vp) -> 100*vp)
                  $ FV.emptyRowBuilder
                vRowBuilderPR =
                  FV.addRowBuilder @'("PEst",FV.NamedParameterEstimate) id
                  $ FV.emptyRowBuilder
                vDatPVsT :: M.Map Int (PreferenceResults b FV.NamedParameterEstimate)
                         -> [FV.Row
                             '[ '("Group", F.Text), '("Election Year", Int),
                                '("D Voter Preference (%)", Double)]] 
                vDatPVsT pr =
                  FV.vinylRows vRowBuilderPVsT $ FL.fold flattenF $ M.toList pr
                addParametersVsTime :: K.KnitOne r
                                    => M.Map Int (PreferenceResults b FV.NamedParameterEstimate)
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
                  $ FV.addRowBuilder @'("D Voteshare of D+R Votes (%)",Double)
                      (\(_, _, z) -> 100*z)
                      FV.emptyRowBuilder
              vDatSVS prMap = FV.vinylRows vRowBuilderSVS $ f1 $ M.toList $ fmap
                modeledDVotes
                prMap
              addStackedArea :: (K.KnitOne r, A.Ix b, Bounded b, Enum b, Show b)
                             => M.Map Int (PreferenceResults b FV.NamedParameterEstimate)
                             -> K.Sem r ()
              addStackedArea prMap = do
                let vl = FV.stackedAreaVsTime @'("Group",T.Text) @'("Election Year",Int)
                         @'("D Voteshare of D+R Votes (%)",Double)
                         "D Voteshare of D+R votes in Competitive Districts vs. Election Year"
                         (FV.TimeEncoding "%Y" FV.Year)
                         (FV.ViewConfig 800 400 10)
                         (vDatSVS prMap)
                _ <- K.addHvega Nothing Nothing vl
                return ()
              mkDeltaTableASR locFilter  (y1, y2) = do
                let y1T = T.pack $ show y1
                    y2T = T.pack $ show y2
                brAddMarkDown $ "### " <> y1T <> "->" <> y2T
                mry1 <- knitMaybe "lookup failure in mwu"
                  $ M.lookup y1 modeledResultsASR
                mry2 <- knitMaybe "lookup failure in mwu"
                  $ M.lookup y2 modeledResultsASR
                (table, (mD1, mR1), (mD2, mR2)) <-
                      deltaTable simpleAgeSexRace locFilter houseElectionsFrame y1 y2 mry1 mry2
                K.addColonnadeTextTable deltaTableColonnade $ table
              mkDeltaTableASE locFilter  (y1, y2) = do
                let y1T = T.pack $ show y1
                    y2T = T.pack $ show y2
                brAddMarkDown $ "### " <> y1T <> "->" <> y2T
                mry1 <- knitMaybe "lookup failure in mwu"
                  $ M.lookup y1 modeledResultsASE
                mry2 <- knitMaybe "lookup failure in mwu"
                  $ M.lookup y2 modeledResultsASE
                (table, (mD1, mR1), (mD2, mR2)) <-
                      deltaTable simpleAgeSexEducation locFilter houseElectionsFrame y1 y2 mry1 mry2
                K.addColonnadeTextTable deltaTableColonnade $ table
                
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
            -- analyze results
            mkDeltaTableASR (const True) (2012, 2016)
            brAddMarkDown brAcrossTimeASR2012To2016
            mkDeltaTableASE (const True) (2012, 2016)

            brAddMarkDown voteShifts
{-            _ <-
              traverse (mkDeltaTable (const True))
                $ [ (2012, 2016)
                  , (2014, 2018)
                  , (2014, 2016)
                  , (2016, 2018)
                  , (2010, 2018)
                  ]
-}
            brAddMarkDown voteShiftObservations
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
            brAddMarkDown "### Presidential Battleground States"
            _ <- mkDeltaTableASR bgOnly (2010, 2018)
            return ()
  case eitherDocs of
    Right namedDocs -> K.writeAllPandocResultsWithInfoAsHtml
      "reports/html/preference_model"
      namedDocs
    Left err -> putStrLn $ "pandoc error: " ++ show err


            
modeledResults :: ( MonadIO (K.Sem r)
                  , K.KnitEffects r
                  , Show tr
                  , Show b
                  , Enum b
                  , Bounded b
                  , A.Ix b
                  , FL.Vector (F.VectorFor b) b)
               => DemographicStructure dr tr HouseElections b
               -> F.Frame dr
               -> F.Frame tr
               -> F.Frame HouseElections 
               -> M.Map Int Int
               -> K.Sem r (M.Map Int (PreferenceResults b FV.NamedParameterEstimate))
modeledResults ds dFrame tFrame eFrame years = flip traverse years $ \y -> do
  K.logLE K.Info $ "inferring " <> T.pack (show $ dsCategories ds) <> " for " <> (T.pack $ show y)
  preferenceModel ds y dFrame eFrame tFrame

-- PreferenceResults to list of group names and predicted D votes
-- But we want them as a fraction of D/D+R
modeledDVotes :: forall b. (A.Ix b, Bounded b, Enum b, Show b)
  => PreferenceResults b FV.NamedParameterEstimate -> [(T.Text, Double)]
modeledDVotes pr =
  let
    summed = FL.fold
             (votesAndPopByDistrictF @b)
             (fmap F.rcast $ votesAndPopByDistrict pr)
    popArray =
      F.rgetField @(PopArray b) summed
    turnoutArray =
      F.rgetField @(TurnoutArray b) summed
    predVoters = zipWith (*) (A.elems turnoutArray) $ fmap realToFrac (A.elems popArray)
    allDVotes  = F.rgetField @DVotes summed
    allRVotes  = F.rgetField @RVotes summed
    dVotes b =
      realToFrac (popArray A.! b)
      * (turnoutArray A.! b)
      * (FV.value . FV.pEstimate $ (modeled pr) A.! b)
    allPredictedD = FL.fold FL.sum $ fmap dVotes [minBound..maxBound]
    scale = (realToFrac allDVotes/realToFrac (allDVotes + allRVotes))/allPredictedD      
  in
    fmap (\b -> (T.pack $ show b, scale * dVotes b))
    [(minBound :: b) .. maxBound]


data DeltaTableRow =
  DeltaTableRow
  { dtrGroup :: T.Text
  , dtrPop :: Int
  , dtrFromPop :: Int
  , dtrFromTurnout :: Int
  , dtrFromOpinion :: Int
  , dtrTotal :: Int
  , dtrPct :: Double
  } deriving (Show)

deltaTable
  :: forall dr tr e b r
   . (A.Ix b
     , Bounded b
     , Enum b
     , Show b
     , MonadIO (K.Sem r)
     , K.KnitEffects r
     )
  => DemographicStructure dr tr e b
  -> (F.Record LocationKey -> Bool)
  -> F.Frame e
  -> Int -- ^ year A
  -> Int -- ^ year B
  -> PreferenceResults b FV.NamedParameterEstimate
  -> PreferenceResults b FV.NamedParameterEstimate
  -> K.Sem r ([DeltaTableRow], (Int, Int), (Int, Int))
deltaTable ds locFilter electionResultsFrame yA yB trA trB = do
  let
    groupNames = fmap (T.pack . show) $ dsCategories ds
    getPopAndTurnout
      :: Int -> PreferenceResults b FV.NamedParameterEstimate -> K.Sem r (A.Array b Int, A.Array b Double)
    getPopAndTurnout y tr = do
      resultsFrame <- knitX $ (dsPreprocessElectionData ds) y electionResultsFrame
      let
        totalDRVotes =             
          let filteredResultsF = F.filterFrame (locFilter . F.rcast) resultsFrame
          in FL.fold (FL.premap (\r -> F.rgetField @DVotes r + F.rgetField @RVotes r) FL.sum) filteredResultsF
        totalRec = FL.fold
          votesAndPopByDistrictF
          ( fmap
              (F.rcast
                @'[PopArray b, TurnoutArray b, DVotes, RVotes]
              )
          $ F.filterFrame (locFilter . F.rcast)
          $ F.toFrame
          $ votesAndPopByDistrict tr
          )
        totalCounts = F.rgetField @(PopArray b) totalRec
        unAdjTurnout = nationalTurnout tr
      tDelta <- liftIO $ TA.findDelta totalDRVotes totalCounts unAdjTurnout
      let adjTurnout = TA.adjTurnoutP tDelta unAdjTurnout        
      return (totalCounts, adjTurnout)
      
  (popA, turnoutA) <- getPopAndTurnout yA trA
  (popB, turnoutB) <- getPopAndTurnout yB trB
  let
    pop        = FL.fold FL.sum popA
    probsArray = fmap (FV.value . FV.pEstimate) . modeled
    probA      = probsArray trA
    probB      = probsArray trB
    modeledVotes popArray turnoutArray probArray =
      let dVotes b =
              round
                $ realToFrac (popArray A.! b)
                * (turnoutArray A.! b)
                * (probArray A.! b)
          rVotes b =
              round
                $ realToFrac (popArray A.! b)
                * (turnoutArray A.! b)
                * (1.0 - probArray A.! b)
      in  FL.fold
            ((,) <$> FL.premap dVotes FL.sum <*> FL.premap rVotes FL.sum)
            [minBound .. maxBound]
    makeDTR b =
      let pop0     = realToFrac $ popA A.! b
          dPop     = realToFrac $ (popB A.! b) - (popA A.! b)
          turnout0 = realToFrac $ turnoutA A.! b
          dTurnout = realToFrac $ (turnoutB A.! b) - (turnoutA A.! b)
          prob0    = realToFrac $ (probA A.! b)
          dProb    = realToFrac $ (probB A.! b) - (probA A.! b)
          dtrCombo = dPop * dTurnout * (2 * dProb) / 4 -- the rest is accounted for in other terms, we spread this among them
          dtrN =
              round
                $ dPop
                * (turnout0 + dTurnout / 2)
                * (2 * (prob0 + dProb / 2) - 1)
                + (dtrCombo / 3)
          dtrT =
              round
                $ (pop0 + dPop / 2)
                * dTurnout
                * (2 * (prob0 + dProb / 2) - 1)
                + (dtrCombo / 3)
          dtrO =
              round
                $ (pop0 + dPop / 2)
                * (turnout0 + dTurnout / 2)
                * (2 * dProb)
                + (dtrCombo / 3)
          dtrTotal = dtrN + dtrT + dtrO
      in  DeltaTableRow (T.pack $ show b)
                        (popB A.! b)
                        dtrN
                        dtrT
                        dtrO
                        dtrTotal
                        (realToFrac dtrTotal / realToFrac pop)
    groupRows = fmap makeDTR [minBound ..]
    addRow (DeltaTableRow g p fp ft fo t _) (DeltaTableRow _ p' fp' ft' fo' t' _)
      = DeltaTableRow g
                      (p + p')
                      (fp + fp')
                      (ft + ft')
                      (fo + fo')
                      (t + t')
                      (realToFrac (t + t') / realToFrac (p + p'))
    totalRow = FL.fold
      (FL.Fold addRow (DeltaTableRow "Total" 0 0 0 0 0 0) id)
      groupRows
    dVotesA = modeledVotes popA turnoutA probA
    dVotesB = modeledVotes popB turnoutB probB
  return (groupRows ++ [totalRow], dVotesA, dVotesB)

deltaTableColonnade :: C.Colonnade C.Headed DeltaTableRow T.Text
deltaTableColonnade =
  C.headed "Group" dtrGroup
    <> C.headed "Population (k)" (T.pack . show . (`div` 1000) . dtrPop)
    <> C.headed "+/- From Population (k)"
                (T.pack . show . (`div` 1000) . dtrFromPop)
    <> C.headed "+/- From Turnout (k)"
                (T.pack . show . (`div` 1000) . dtrFromTurnout)
    <> C.headed "+/- From Opinion (k)"
                (T.pack . show . (`div` 1000) . dtrFromOpinion)
    <> C.headed "+/- Total (k)" (T.pack . show . (`div` 1000) . dtrTotal)
    <> C.headed "+/- %Vote" (T.pack . PF.printf "%2.2f" . (* 100) . dtrPct)

--------------------------------------------------------------------------------
modelNotesPreface :: T.Text
modelNotesPreface = [here|
## Preference-Model Notes
Our goal is to use the house election results[^ResultsData] to fit a very
simple model of the electorate.  We consider the electorate as having some number
of "identity" groups. For example we could divide by sex
(the census only records this as a F/M binary),
age, "old" (45 or older) and "young" (under 45) and
education (college graduates vs. non-college graduate)
or racial identity (white vs. non-white). 
We recognize that these categories are limiting and much too simple.
But we believe it's a reasonable starting point, a balance
between inclusiveness and having way too many variables.

For each congressional district where both major parties ran candidates, we have
census estimates of the number of people in each of our
demographic categories[^CensusDemographics].
And from the census we have national-level turnout estimates for each of these
groups as well[^CensusTurnout].

All we can observe is the **sum** of all the votes in the district,
not the ones cast by each group separately.
But each district has a different demographic makeup and so each is a
distinct piece of data about how each group is likely to vote.

The turnout numbers from the census are national averages and
aren't correct in any particular district.  Since we don't have more
detailed turnout data, there's not much we can do.  But we do know the
total number of votes observed in each district, and we should at least
adjust the turnout numbers so that the total number of votes predicted
by the turnout numbers and populations is close to the observed number
of votes. For more on this adjustment, see below.

How likely is a voter in each group to vote for the
democratic candidate in a contested race?

For each district, $d$, we have the set of expected voters
(the number of people in each group in that region, $N^{(d)}_i$,
multiplied by the turnout, $t_i$ for that group),
$V^{(d)}_i$, the number of democratic votes, $D^{(d)}$,
republican votes, $R^{(d)}$ and total votes, $T^{(d)}$, which may exceed $D^{(d)} + R^{(d)}$,
since there may be third party candidates. For the sake of simplicity,
we assume that all groups are equally likely to vote for a third party candidate.
We want to estimate $p_i$, the probability that
a voter (in any district) in the $i$th group--given that they voted
for a republican or democrat--will vote for the democratic candidate.                     

the turnout numbers from the census, , multiplied by the
poopulations of each group will *not* add up to the number of votes observed,
since turnout varies district to district.
We adjust these turnout numbers via a technique[^GGCorrection] from
[Ghitza and Gelman, 2013](http://www.stat.columbia.edu/~gelman/research/published/misterp.pdf).


[^ResultsData]: MIT Election Data and Science Lab, 2017
, "U.S. House 1976–2018"
, https://doi.org/10.7910/DVN/IG0UN2
, Harvard Dataverse, V3
, UNF:6:KlGyqtI+H+vGh2pDCVp7cA== [fileUNF]
[^ResultsDataV2]:MIT Election Data and Science Lab, 2017
, "U.S. House 1976–2018"
, https://doi.org/10.7910/DVN/IG0UN2
, Harvard Dataverse, V4
, UNF:6:M0873g1/8Ee6570GIaIKlQ== [fileUNF]
[^CensusDemographics]: Source: US Census, American Community Survey <https://www.census.gov/programs-surveys/acs.html> 
[^CensusTurnout]: Source: US Census, Voting and Registration Tables <https://www.census.gov/topics/public-sector/voting/data/tables.2014.html>. NB: We are using 2017 demographic population data for our 2018 analysis,
since that is the latest available from the census.
We will update this once the census publishes updated 2018 American Community Survey data.
[^GGCorrection]: We note that there is an error in the 2013 Ghitza and Gelman paper, one which is
corrected in a more recent working paper <http://www.stat.columbia.edu/~gelman/research/published/mrp_voterfile_20181030.pdf>.
by the same authors.  In the 2013 paper, a correction is derived
for turnout in each region by find the $\delta^{(d)}$ which minimizes
$\big|T^{(d)} -\sum_i N^{(d)}_i logit^{-1}(logit(t_i) + \delta^{(d)})\big|$. The authors then
state that the adjusted turnout in region $d$ is $\hat{t}^{(d)}_i = t_i + \delta^{(d)}$ which
doesn't make sense since $\delta^{(d)}$ is not a probability.  This is corrected in the working
paper to $\hat{t}^{(d)}_i = logit^{-1}(logit(t_i) + \delta^{(d)})$.

|]

--------------------------------------------------------------------------------
modelNotesBayes :: T.Text
modelNotesBayes = modelNotesPreface <> "\n\n" <> [here|

* [Bayes theorem](<https://en.wikipedia.org/wiki/Bayes%27_theorem)
relates the probability of a model
(our demographic voting probabilities $\{p_i\}$),
given the observed data (the number of democratic votes recorded in each
district, $\{D_k\}$) to the likelihood of observing that data given the model
and some prior knowledge about the unconditional probability of the model itself
$P(\{p_i\})$, as well as $P(\{D_k\})$, the unconditional probability of observing
the "evidence":
$\begin{equation}
P(\{p_i\}|\{D_k\})P(\{D_k\}) = P(\{D_k\}|\{p_i\})P(\{p_i\})
\end{equation}$
In this situation, the thing we wish to compute, $P(\{p_i\}|\{D_k\})$,
is referred to as the "posterior" distribution.

* $P(\{p_i\})$ is called a "prior" and amounts to an assertion about
what we think we know about the parameters before we have seen any of the data.
In practice, this can often be set to something very boring, in our case,
we will assume that our prior is just that any $p_i \in [0,1]$ is equally likely.

* $P(\{D_k\})$ is the unconditional probability of observing
the specific outcome $\{D_k\}$.
This is difficult to compute! Sometimes we can compute it by observing:
$\begin{equation}
P(\{D_k\}) = \sum_{\{p_i\}} P(\{D_k\}|{p_i}) P(\{p_i\})
\end{equation}$.  But in general, we'd like to compute the posterior in
some way that avoids needing the probability of the evidence.

* $P(\{D_k\}|\{p_i\})$, the probability that we
observed our evidence, *given* a specific set of $\{p_i\}$ is a thing
we can calculate:
Our $p_i$ are the probability that one voter of type $i$, who votes for
a democrat or republican, chooses
the democrat.  We *assume*, for the sake of simplicity,
that for each demographic group $i$, each voter's vote is like a coin
flip where the coin comes up "Democrat" with probability $p_i$ and
"Republican" with probability $1-p_i$. This distribution of single
voter outcomes is known as the [Bernoulli distribution.][WP:Bernoulli].
Given $V_i$ voters of that type, the distribution of democratic votes
*from that type of voter*
is [Binomial][WP:Binomial] with $V_i$ trials and $p_i$ probability of success.
But $V_i$ is quite large! So we can approximate this with a normal
distribution with mean $V_i p_i$ and variance $V_i p_i (1 - p_i)$
(see [Wikipedia][WP:BinomialApprox]).  However, we can't observe the number
of votes from just one type of voter. We can only observe the sum over all types.
Luckily, the sum of normally distributed random variables follows a  normal
distribution as well.
So the distribution of democratic votes across all types of voters is also approximately
normal,
with mean $\sum_i V_i p_i$ and variance $\sum_i V_i p_i (1 - p_i)$
(again, see [Wikipedia][WP:SumNormal]). Thus we have $P(D_k|\{p_i\})$, or,
what amounts to the same thing, its probability density.
But that means we also know the probability density of all the evidence
given $\{p_i\}$, $\rho(\{D_k\}|\{p_i\})$, since that is just the
product of the densities for each $D_k$:
$\begin{equation}
\mu_k(\{p_i\}) = \sum_i V_i p_i\\
v_k(\{p_i\}) = \sum_i V_i p_i (1 - p_i)\\
\rho(D_k|\{p_i\}) = \frac{1}{\sqrt{2\pi v_k}}e^{-\frac{(D_k -\mu_k(\{p_i\}))^2}{2v_k(\{p_i\})}}\\
\rho(\{D_k\}|\{p_i\}) = \Pi_k \rho(D_k|\{p_i\})
\end{equation}$

* Now that we have this probability density, we want to look for the set of
voter preferences which maximizes it.  There are many methods to do this but in
this case, because the distribution has a simple shape, and we can compute its
gradient, a good numerical optimizer is all we need.  That gives us
maximum-likelihood estimates and covariances among the estimated parameters.

[WP:Bernoulli]: <https://en.wikipedia.org/wiki/Bernoulli_distribution>

[WP:Binomial]: <https://en.wikipedia.org/wiki/Binomial_distribution>

[WP:BinomialApprox]: <https://en.wikipedia.org/wiki/Binomial_distribution#Normal_approximation>

[WP:SumNormal]: <https://en.wikipedia.org/wiki/Sum_of_normally_distributed_random_variables>
|]

modelNotesMCMC :: T.Text
modelNotesMCMC = [here|
* In order to compute expectations on this distribution we use
Markov Chain Monte Carlo (MCMC). MCMC creates "chains" of samples
from the the posterior
distribution given a prior, $P(\{p_i\})$, the conditional
$P(\{D_k\}|\{p_i\})$, and a starting $\{p_i\}$.
Note that this doesn't require knowing $P(\{D_k\})$, basically
because the *relative* likelihood of any $\{p_i\}$
doesn't depend on it.
Those samples are then used to compute expectations of
various quantities of interest.
In practice, it's hard to know when you have "enough" samples
to have confidence in your expectations.
Here we use an interval based "potential scale reduction factor"
([PSRF][Ref:Convergence]) to check the convergence of any one
expectation, e,g, each $p_i$ in $\{p_i\}$, and a
"multivariate potential scale reduction factor" ([MPSRF][Ref:MPSRF]) to
make sure that the convergence holds for all possible linear combinations
of the $\{p_i\}$.
Calculating either PSRF or MPSRF entails starting several chains from
different (random) starting locations, then comparing something like
a variance on each chain to the same quantity on the combined chains. 
This converges to one as the chains converge[^rhat] and a value below 1.1 is,
conventionally, taken to indicate that the chains have converged
"enough".

[Ref:Convergence]: <http://www2.stat.duke.edu/~scs/Courses/Stat376/Papers/ConvergeDiagnostics/BrooksGelman.pdf>

[Ref:MPSRF]: <https://www.ets.org/Media/Research/pdf/RR-03-07-Sinharay.pdf>

[^rhat]: The details of this convergence are beyond our scope but just to get an intuition:
consider a PSRF computed by using (maximum - minimum) of some quantity.
The mean of these intervals is also the mean maximum minus the mean minimum.
And the mean maximum is clearly less than the maximum across all chains while the
mean minimum is clearly larger than than the absolute minimum across
all chains. So their ratio gets closer to 1 as the individual chains
look more and more like the combined chain, which we take to mean that the chains
have converged.
|]

type X = "X" F.:-> Double
type ScaledDVotes = "ScaledDVotes" F.:-> Int
type ScaledRVotes = "ScaledRVotes" F.:-> Int
type PopArray b = "PopArray" F.:-> A.Array b Int
type TurnoutArray b = "TurnoutArray" F.:-> A.Array b Double

votesAndPopByDistrictF
  :: forall b
   . (A.Ix b, Bounded b, Enum b)
  => FL.Fold
       (F.Record '[PopArray b, TurnoutArray b, DVotes, RVotes])
       (F.Record '[PopArray b, TurnoutArray b, DVotes, RVotes])
votesAndPopByDistrictF =
  let voters r = A.listArray (minBound, maxBound)
                 $ zipWith (*) (A.elems $ F.rgetField @(TurnoutArray b) r) (fmap realToFrac $ A.elems $ F.rgetField @(PopArray b) r)
      g r = A.listArray (minBound, maxBound)
            $ zipWith (/) (A.elems $ F.rgetField @(TurnoutArray b) r) (fmap realToFrac $ A.elems $ F.rgetField @(PopArray b) r)
      recomputeTurnout r = F.rputField @(TurnoutArray b) (g r) r                            
  in PF.dimap (F.rcast @'[PopArray b, TurnoutArray b, DVotes, RVotes]) recomputeTurnout
    $    FF.sequenceRecFold
    $    FF.FoldRecord (PF.dimap (F.rgetField @(PopArray b)) V.Field FE.sumTotalNumArray)
    V.:& FF.FoldRecord (PF.dimap voters V.Field FE.sumTotalNumArray)
    V.:& FF.FoldRecord (PF.dimap (F.rgetField @DVotes) V.Field FL.sum)
    V.:& FF.FoldRecord (PF.dimap (F.rgetField @RVotes) V.Field FL.sum)
    V.:& V.RNil

data PreferenceResults b a = PreferenceResults
  {
    votesAndPopByDistrict :: [F.Record [ StateAbbreviation
                                       , CongressionalDistrict
                                       , PopArray b -- population by group
                                       , TurnoutArray b -- adjusted turnout by group
                                       , DVotes
                                       , RVotes
                                       ]]
    , nationalTurnout :: A.Array b Double
    , nationalVoters :: A.Array b Int
    , modeled :: A.Array b a
    , correlations :: LA.Matrix Double
  }

preferenceModel
  :: forall dr tr b r
   . ( Show tr
     , Show b
     , Enum b
     , Bounded b
     , A.Ix b
     , FL.Vector (F.VectorFor b) b
     , K.KnitEffects r
     , MonadIO (K.Sem r)
     )
  => DemographicStructure dr tr HouseElections b
  -> Int
  -> F.Frame dr
  -> F.Frame HouseElections
  -> F.Frame tr
  -> K.Sem
       r
       (PreferenceResults b FV.NamedParameterEstimate)
preferenceModel ds year identityDFrame houseElexFrame turnoutFrame =
  do
    -- reorganize data from loaded Frames
    resultsFlattenedFrame <- knitX
      $ (dsPreprocessElectionData ds) year houseElexFrame
    filteredTurnoutFrame <- knitX
      $ (dsPreprocessTurnoutData ds) year turnoutFrame
    let year' = if (year == 2018) then 2017 else year -- we're using 2017 for now, until census updated ACS data
    longByDCategoryFrame <- knitX
      $ (dsPreprocessDemographicData ds) year' identityDFrame

    -- turn long-format data into Arrays by demographic category, beginning with national turnout
    turnoutByGroupArray <-
      knitMaybe "Missing or extra group in turnout data?" $ FL.foldM
        (FE.makeArrayMF (F.rgetField @(DemographicCategory b))
                        (F.rgetField @VotedPctOfAll)
                        (flip const)
        )
        filteredTurnoutFrame

    -- now the populations in each district
    let votersArrayMF = MR.mapReduceFoldM
          (MR.generalizeUnpack $ MR.noUnpack)
          (MR.generalizeAssign $ MR.splitOnKeys @LocationKey)
          (MR.foldAndLabelM
            (fmap (FT.recordSingleton @(PopArray b))
                  (FE.recordsToArrayMF @(DemographicCategory b) @PopCount)
            )
            V.rappend
          )
    -- F.Frame (LocationKey V.++ (PopArray b))      
    populationsFrame <-
      knitMaybe "Error converting long demographic data to arrays!"
      $   F.toFrame
      <$> FL.foldM votersArrayMF longByDCategoryFrame

    -- and the total populations in each group
    let addArray :: (A.Ix k, Num a) => A.Array k a -> A.Array k a -> A.Array k a
        addArray a1 a2 = A.accum (+) a1 (A.assocs a2)
        zeroArray :: (A.Ix k, Bounded k, Enum k, Num a) => A.Array k a
        zeroArray = A.listArray (minBound, maxBound) $ L.repeat 0
        popByGroupArray = FL.fold (FL.premap (F.rgetField @(PopArray b)) (FL.Fold addArray zeroArray id)) populationsFrame

    let
      resultsWithPopulationsFrame =
        catMaybes $ fmap F.recMaybe $ F.leftJoin @LocationKey resultsFlattenedFrame
                                                              populationsFrame

    K.logLE K.Info $ "Computing Ghitza-Gelman turnout adjustment for each district so turnouts produce correct number D+R votes."
    resultsWithPopulationsAndGGAdjFrame <- fmap F.toFrame $ flip traverse resultsWithPopulationsFrame $ \r -> do
      let tVotesF x = F.rgetField @DVotes x + F.rgetField @RVotes x -- Should this be D + R or total?
      ggDelta <- ggTurnoutAdj r tVotesF turnoutByGroupArray
      K.logLE K.Diagnostic $
        "Ghitza-Gelman turnout adj="
        <> (T.pack $ show ggDelta)
        <> "; Adj Turnout=" <> (T.pack $ show $ TA.adjTurnoutP ggDelta turnoutByGroupArray)
      return $ FT.mutate (const $ FT.recordSingleton @(TurnoutArray b) $ TA.adjTurnoutP ggDelta turnoutByGroupArray) r

    let onlyOpposed r =
          (F.rgetField @DVotes r > 0) && (F.rgetField @RVotes r > 0)
        opposedFrame = F.filterFrame onlyOpposed resultsWithPopulationsAndGGAdjFrame
        numCompetitiveRaces = FL.fold FL.length opposedFrame

    K.logLE K.Info
      $ "After removing races where someone is running unopposed we have "
      <> (T.pack $ show numCompetitiveRaces)
      <> " contested races."
      
    totalVoteDiagnostics @b resultsWithPopulationsAndGGAdjFrame opposedFrame
    

    let 
      scaleInt s n = round $ s * realToFrac n
      mcmcData =
        fmap
        (\r ->
           ( (F.rgetField @DVotes r)
           , VB.fromList $ fmap round (adjVotersL (F.rgetField @(TurnoutArray b) r) (F.rgetField @(PopArray b) r))
           )
        )
        $ FL.fold FL.list opposedFrame
      numParams = length $ dsCategories ds
    (cgRes, _, _) <- liftIO $ PB.cgOptimizeAD mcmcData (VB.fromList $ fmap (const 0.5) $ dsCategories ds)
    let cgParamsA = A.listArray (minBound :: b, maxBound) $ VB.toList cgRes
        cgVarsA = A.listArray (minBound :: b, maxBound) $ VS.toList $ PB.variances mcmcData cgRes
        npe cl b =
          let
            x = cgParamsA A.! b
            sigma = sqrt $ cgVarsA A.! b
            dof = realToFrac $ numCompetitiveRaces - L.length (A.elems cgParamsA)
            interval = S.quantile (S.studentTUnstandardized dof 0 sigma) (1.0 - (S.significanceLevel cl/2))
            pEstimate = FV.ParameterEstimate x (x - interval/2.0, x + interval/2.0)
          in FV.NamedParameterEstimate (T.pack $ show b) pEstimate
        parameterEstimatesA = A.listArray (minBound :: b, maxBound) $ fmap (npe S.cl95) $ [minBound :: b .. maxBound]

    K.logLE K.Info $ "MLE results: " <> (T.pack $ show $ A.elems parameterEstimatesA)     
-- For now this bit is diagnostic.  But we should chart the correlations
-- and, perhaps, the eigenvectors of the covariance??    
    let cgCorrel = PB.correl mcmcData cgRes -- TODO: make a chart out of this
        (cgEv, cgEvs) = PB.mleCovEigens mcmcData cgRes
    K.logLE K.Diagnostic $ "sigma = " <> (T.pack $ show $ fmap sqrt $ cgVarsA)
    K.logLE K.Diagnostic $ "Correlation=" <> (T.pack $ PB.disps 3 cgCorrel)
    K.logLE K.Diagnostic $ "Eigenvalues=" <> (T.pack $ show cgEv)
    K.logLE K.Diagnostic $ "Eigenvectors=" <> (T.pack $ PB.disps 3 cgEvs)
    
    return $ PreferenceResults
      (fmap F.rcast $ FL.fold FL.list opposedFrame)
      turnoutByGroupArray
      popByGroupArray
      parameterEstimatesA
      cgCorrel

ggTurnoutAdj :: forall b rs r. (A.Ix b
                               , F.ElemOf rs (PopArray b)
                               , MonadIO (K.Sem r)
                               ) => F.Record rs -> (F.Record rs -> Int) -> A.Array b Double -> K.Sem r Double
ggTurnoutAdj r totalVotesF unadjTurnoutP = do
  let population = F.rgetField @(PopArray b) r
      totalVotes = totalVotesF r
  liftIO $ TA.findDelta totalVotes population unadjTurnoutP

adjVotersL :: A.Array b Double -> A.Array b Int -> [Double]
adjVotersL turnoutPA popA = zipWith (*) (A.elems turnoutPA) (fmap realToFrac $ A.elems popA)

totalVoteDiagnostics :: forall b rs f r
                        . (A.Ix b
                          , Foldable f
                          , F.ElemOf rs (PopArray b)
                          , F.ElemOf rs (TurnoutArray b)
                          , F.ElemOf rs Totalvotes
                          , F.ElemOf rs DVotes
                          , F.ElemOf rs RVotes
                          , K.KnitEffects r
                        )
  => f (F.Record rs) -- ^ frame with all rows
  -> f (F.Record rs) -- ^ frame with only rows from competitive races
  -> K.Sem r ()
totalVoteDiagnostics allFrame opposedFrame = K.wrapPrefix "VoteSummary" $ do
  let allVoters r = FL.fold FL.sum
                    $ zipWith (*) (A.elems $ F.rgetField @(TurnoutArray b) r) (fmap realToFrac $ A.elems $ F.rgetField @(PopArray b) r)
      allVotersF = FL.premap allVoters FL.sum
      allVotesF  = FL.premap (F.rgetField @Totalvotes) FL.sum
      allDVotesF = FL.premap (F.rgetField @DVotes) FL.sum
      allRVotesF = FL.premap (F.rgetField @RVotes) FL.sum
  --      allDRVotesF = FL.premap (\r -> F.rgetField @DVotes r + F.rgetField @RVotes r) FL.sum
      (totalVoters, totalVotes, totalDVotes, totalRVotes) = FL.fold
        ((,,,) <$> allVotersF <*> allVotesF <*> allDVotesF <*> allRVotesF)
        allFrame
      (totalVotersCD, totalVotesCD, totalDVotesCD, totalRVotesCD) = FL.fold
        ((,,,) <$> allVotersF <*> allVotesF <*> allDVotesF <*> allRVotesF)
        opposedFrame
  K.logLE K.Info $ "voters=" <> (T.pack $ show totalVoters)
  K.logLE K.Info $ "house votes=" <> (T.pack $ show totalVotes)
  K.logLE K.Info
    $  "D/R/D+R house votes="
    <> (T.pack $ show totalDVotes)
    <> "/"
    <> (T.pack $ show totalRVotes)
    <> "/"
    <> (T.pack $ show (totalDVotes + totalRVotes))
  K.logLE K.Info
    $  "voters (competitive districts)="
    <> (T.pack $ show totalVotersCD)
  K.logLE K.Info
    $  "house votes (competitive districts)="
    <> (T.pack $ show totalVotesCD)
  K.logLE K.Info
    $  "D/R/D+R house votes (competitive districts)="
    <> (T.pack $ show totalDVotesCD)
    <> "/"
    <> (T.pack $ show totalRVotesCD)
    <> "/"
    <> (T.pack $ show (totalDVotesCD + totalRVotesCD))

modelNotesRegression :: T.Text
modelNotesRegression = modelNotesPreface <> [here|

Given $T' = \sum_i V_i$, the predicted number of votes in the district and that $\frac{D+R}{T}$ is the probability that a voter in this district will vote for either major party candidate, we define $Q=\frac{T}{T'}\frac{D+R}{T} = \frac{D+R}{T'}$ and have:

$\begin{equation}
D = Q\sum_i p_i V_i\\
R = Q\sum_i (1-p_i) V_i
\end{equation}$

combining then simplfying:

$\begin{equation}
D - R =  Q\sum_i p_i V_i - Q\sum_i (1-p_i) V_i\\
\frac{D-R}{Q} = \sum_i (2p_i - 1) V_i\\
\frac{D-R}{Q} = 2\sum_i p_i V_i - \sum_i V_i\\
\frac{D-R}{Q} = 2\sum_i p_i V_i - T'\\
\frac{D-R}{Q} + T' = 2\sum_i p_i V_i
\end{equation}$

and substituting $\frac{D+R}{T'}$ for $Q$ and simplifying, we get

$\begin{equation}
\sum_i p_i V_i = \frac{T'}{2}(\frac{D-R}{D+R} + 1)
\end{equation}$

We can simplify this a bit more if we define $d$ and $r$ as the percentage of the major party vote that goes for each party, that is $d = D/(D+R)$ and $r = R/(D+R)$.
Now $\frac{D-R}{D+R} = d-r$ and so $\sum_i p_i V_i = \frac{T'}{2}(1 + (d-r))$

This is now in a form amenable for regression, estimating the $p_i$ that best fit the 369 results in 2016.

Except it's not!! Because these parameters are probabilities and
classic regression is not a good method here.
So we turn to Bayesian inference.  Which was more appropriate from the start.
|]


totalArrayZipWith :: (A.Ix b, Enum b, Bounded b)
                  => (x -> y -> z)
                  -> A.Array b x
                  -> A.Array b y
                  -> A.Array b z
totalArrayZipWith f xs ys = A.listArray (minBound, maxBound) $ zipWith f (A.elems xs) (A.elems ys)

vlGroupingChart :: Foldable f
                => T.Text
                -> FV.ViewConfig
                -> f (F.Record ['("Group", T.Text)
                               ,'("VotingAgePop", Int)
                               ,'("Turnout",Double)
                               ,'("Voters", Int)
                               ,'("D Voter Preference", Double)
                               ])
                -> GV.VegaLite
vlGroupingChart title vc rows =
  let dat = FV.recordsToVLData id FV.defaultParse rows
      xLabel = "Inferred (%) Likelihood of Voting Democratic"
      estimateXenc = GV.position GV.X [FV.pName @'("D Voter Preference", Double)
                                      ,GV.PmType GV.Quantitative
                                      ,GV.PAxis [GV.AxTitle xLabel]
                                      ]
      estimateYenc = GV.position GV.Y [FV.pName @'("Group",T.Text)
                                      ,GV.PmType GV.Ordinal
                                      ,GV.PAxis [GV.AxTitle "Demographic Group"]
                                      ]
      estimateSizeEnc = GV.size [FV.mName @'("Voters",Int)
                                , GV.MmType GV.Quantitative
                                , GV.MScale [GV.SDomain $ GV.DNumbers [5e6,30e6]]
                                , GV.MLegend [GV.LFormatAsNum]
                                
                                ]
      estimateColorEnc = GV.color [FV.mName @'("Turnout", Double)
                                  , GV.MmType GV.Quantitative
                                  , GV.MScale [GV.SDomain $ GV.DNumbers [0.2,0.8]]
                                  , GV.MLegend [GV.LGradientLength (vcHeight vc / 3)
                                               , GV.LFormatAsNum
                                               , GV.LFormat "%"
                                               ]
                                  ]
      estEnc = estimateXenc . estimateYenc . estimateSizeEnc . estimateColorEnc
      estSpec = GV.asSpec [(GV.encoding . estEnc) [], GV.mark GV.Point []]
  in
    FV.configuredVegaLite vc [FV.title title, GV.layer [estSpec], dat]

exitCompareChart :: Foldable f
                 => T.Text
                 -> FV.ViewConfig
                 -> f (F.Record ['("Group", T.Text)
                                ,'("Model Dem Pref", Double)
                                ,'("ModelvsExit",Double)
                               ])
                -> GV.VegaLite
exitCompareChart title vc rows =
  let dat = FV.recordsToVLData id FV.defaultParse rows
      xLabel = "Modeled % Likelihood of Voting Democratic"
      xEnc =  GV.position GV.X [FV.pName @'("Model Dem Pref", Double)
                               ,GV.PmType GV.Quantitative
                               ,GV.PAxis [GV.AxTitle xLabel
                                         , GV.AxFormatAsNum
                                         , GV.AxFormat "%"
                                         ]
                               ]
      yEnc = GV.position GV.Y [FV.pName @'("ModelvsExit", Double)
                              ,GV.PmType GV.Quantitative
                              ,GV.PScale [GV.SDomain $ GV.DNumbers [negate 0.15,0.15]]
                              ,GV.PAxis [GV.AxTitle "Model - Exit Poll"
                                        , GV.AxFormatAsNum
                                        , GV.AxFormat "%"
                                        ]
                              ]
      colorEnc = GV.color [FV.mName @'("Group", T.Text)
                          , GV.MmType GV.Nominal                          
                          ]
      enc = xEnc . yEnc . colorEnc
      spec = GV.asSpec [(GV.encoding . enc) [], GV.mark GV.Point []]
  in
    FV.configuredVegaLite vc [FV.title title, GV.layer [spec], dat]
             


vlGroupingChartExit :: Foldable f
                    => T.Text
                    -> FV.ViewConfig
                    -> f (F.Record ['("Group", T.Text)
                                   ,'("VotingAgePop", Int)
                                   ,'("Voters", Int)
                                   ,'("D Voter Preference", Double)
                                   ,'("InfMinusExit", Double)
                                   ])
                    -> GV.VegaLite
vlGroupingChartExit title vc rows =
  let dat = FV.recordsToVLData id FV.defaultParse rows
      xLabel = "Inferred Likelihood of Voting Democratic"
      estimateXenc = GV.position GV.X [FV.pName @'("D Voter Preference", Double)
                                      ,GV.PmType GV.Quantitative
                                      ,GV.PAxis [GV.AxTitle xLabel]
                                      ]
      estimateYenc = GV.position GV.Y [FV.pName @'("Group",T.Text)
                                      ,GV.PmType GV.Ordinal
                                      ]
      estimateSizeEnc = GV.size [FV.mName @'("VotingAgePop",Int)
                                , GV.MmType GV.Quantitative]
      estimateColorEnc = GV.color [FV.mName @'("InfMinusExit", Double)
                                  , GV.MmType GV.Quantitative] 
      estEnc = estimateXenc . estimateYenc . estimateSizeEnc . estimateColorEnc
      estSpec = GV.asSpec [(GV.encoding . estEnc) [], GV.mark GV.Point []]
  in
    FV.configuredVegaLite vc [FV.title title, GV.layer [estSpec], dat]


intro2018 :: T.Text
intro2018 = [here|
## 2018 Voter Preference
The 2018 house races were generally good for Democrats and progressives--but why?
Virtually every plausible theory has at least some support –
depending on which pundits and researchers you follow,
you could credibly argue that
turnout of young voters[^VoxYouthTurnout], or white women abandoning Trump[^VoxWhiteWomen], or an underlying
demographic shift toward non-white voters[^Pew2018] was the main factor that propelled the
Blue Wave in the midterms.

If Democrats want to solidify and extend their gains, what we really want to know
is the relative importance of each of these factors – in other words,
how much of last year’s outcome was due to changes in demographics vs.
voter turnout vs. voters changing their party preferences?
It turns out that answering
this is difficult. We have good data on the country’s changing demographics,
and also on who showed up to the polls, broken down by gender, age, and race.
But in terms of how each sub-group voted, we only have exit polls and
post-election surveys, as well as the final election results in aggregate.

* We consider only "competitive" districts, defined as those that had
a democrat and republican candidate. Of the 435 House districts, 
382 districts were competitive in 2018.

* Our demographic groupings are limited by the the categories recognized
and tabulated by the census and by our desire to balance specificity
(using more groups so that we might recognize people's identity more precisely)
with a need to keep the model small enough to make inference possible.
Thus for now we split the electorate into "white" (non-hispanic) and "non-white",
"male" and "female" and "young" (<45) and "old".

* Our inference model uses Bayesian techniques
that are described in more detail in a separate
[Preference-Model Notes](https://blueripple.github.io/PreferenceModel/MethodsAndSources.html)
post.

Several folks have done related work, inferring voter behavior using the
election results and other data combined with census data about demographics.
in U.S. elections. In particular:

* [Andrew Gelman](http://www.stat.columbia.edu/~gelman/)
and various collaborators have used Bayesian and other inference techniques to
look at exit-poll and other survey data to examine turnout and voting patterns.  In particular
the technique I use here to adjust the census turnout numbers to best match the actual recorded
vote totals in each district comes from
[Ghitza & Gelman, 2013](http://www.stat.columbia.edu/~gelman/research/published/misterp.pdf).

* [This blog post](http://tomvladeck.com/2016/12/31/unpacking-the-election-results-using-bayesian-inference/)
uses bayesian inference and a beta-binomial
voter model to look at the 2016 election and various subsets of the
electorate. The more sophisticated model allows inference on voter polarity 
within groups as well as the voter preference of each group.

* [This paper](https://arxiv.org/pdf/1611.03787.pdf)
uses different techniques but similar data to
look at the 2016 election and infer voter behavior by demographic group.
The model uses county-level data and exit-polls
and is able to draw inferences about turnout and voter preference and to do so
for *specific geographical areas*.

* This [post](https://medium.com/@yghitza_48326/revisiting-what-happened-in-the-2018-election-c532feb51c0)
is asking many of the same questions but using much more specific data, gathered from
voter files[^VoterFiles].  That data is not publicly available, at least not for free.

Each of these studies is limited to the 2016 presidential election. Still,
each has much to offer in terms of ideas for
pushing this work forward, especially where county-level election returns are
available, as they are for 2016 and 2018[^MITElectionLabData].

As a first pass, we modeled the voting preferences of our
8 demographic sub-groups in the 2018 election,
so we could compare our results with data from exit polls and surveys.
The results are presented in the figure below:

[^VoxYouthTurnout]: <https://www.vox.com/2019/4/26/18516645/2018-midterms-voter-turnout-census>
[^VoxWhiteWomen]: <https://www.vox.com/policy-and-politics/2018/11/7/18064260/midterm-elections-turnout-women-trump-exit-polls>
[^Pew2018]: <https://www.pewresearch.org/fact-tank/2018/11/08/the-2018-midterm-vote-divisions-by-race-gender-education/>
speaks to this, though it addresses turnout and opinion shifts as well.
[^VoterFiles]: <https://www.pewresearch.org/fact-tank/2018/02/15/voter-files-study-qa/>
[^MITElectionLabData]: <https://electionlab.mit.edu/data>
|]

  
postFig2018 :: T.Text
postFig2018 = [here|
The most striking observation is the chasm between white and non-white voters’
inferred support for Democrats in 2018. Non-whites were modeled to
have over 75% preference for Dems regardless of age or gender,
though support is even a bit stronger among non-white female voters than
non-white male voters5. Inferred support from white voters in 2018
is substantially lower, roughly 35-45% across age groups and genders.
In contrast, differences in inferred preferences by age
(matching for gender and race) or gender (matching for age and race) are not
particularly striking or consistent
(e.g., comparing white males in the under-25 and over-75 groups).
Overall, we’re heartened that our model seems to work pretty well,
because the results are broadly consistent with exit polls and surveys[^ExitPolls2018][^Surveys2018]. 
Thus, our model confirmed prior work suggesting that non-white support for
Democrats in 2018 was much higher than that by whites, across all
genders and age groups. But it still doesn’t tell us what happened in 2018
compared with prior years. To what extent did Democrats’ gains over 2016 come from
underlying growth in the non-white population, higher turnout among non-whites,
increased preference for Democrats (among whites or non-whites), or some combination
of these and other factors? That requires comparing these data to results
from earlier elections – which is what we’ll do in subsequent posts. Stay tuned. 

[^ExitPolls2018]: <https://www.nytimes.com/interactive/2018/11/07/us/elections/house-exit-polls-analysis.html>,
<https://www.brookings.edu/blog/the-avenue/2018/11/08/2018-exit-polls-show-greater-white-support-for-democrats/>
[^Surveys2018]: <https://www.pewresearch.org/fact-tank/2018/11/29/in-midterm-voting-decisions-policies-took-a-back-seat-to-partisanship/>
|]

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
module Main where

import           Control.Arrow                  ( first
                                                , second
                                                , (***)
                                                , (&&&)
                                                )
import           Control.Lens                   ( (^.)
                                                , over
                                                , (&)
                                                , (%~)
                                                )
import qualified Control.Foldl                 as FL
import           Control.Monad                  ( when )
import qualified Control.Monad.Except          as X
import           Data.Traversable               ( sequenceA )
import           Control.Monad.IO.Class         ( MonadIO(liftIO) )
import qualified Colonnade                     as C
import qualified Text.Blaze.Colonnade          as C
import qualified Data.Functor.Identity         as I
import qualified Data.Either                   as E
import qualified Data.List                     as L
import qualified Data.Map                      as M
import qualified Data.Array                    as A
import           Data.Maybe                     ( catMaybes
                                                , fromMaybe
                                                , isNothing
                                                , isJust
                                                , fromJust
                                                )

import qualified Text.Pandoc.Error             as PA

import qualified Text.Read                     as TR
import qualified Data.Monoid                   as MO
import           Data.Proxy                     ( Proxy(..) )
import qualified Data.Profunctor               as PF
import qualified Data.Text                     as T
import qualified Data.Text.IO                  as T
import qualified Data.Text.Lazy                as TL
import qualified Data.Time.Calendar            as Time
import qualified Data.Vinyl                    as V
import qualified Data.Vinyl.TypeLevel          as V
import qualified Data.Vinyl.Functor            as V
import qualified Text.Printf                   as PF
import qualified Frames                        as F
import           Frames                         ( (:->)
                                                , (<+>)
                                                , (&:)
                                                )
import qualified Frames.CSV                    as F
import qualified Frames.InCore                 as F
                                         hiding ( inCoreAoS )
import qualified Frames.Melt                   as F
                                                ( ElemOf
                                                , RDeleteAll
                                                )

import qualified Pipes                         as P
import qualified Pipes.Prelude                 as P
import qualified Statistics.Types              as S
import           System.Random                  ( randomRIO )
import qualified System.Directory              as SD
import qualified Statistics.Types              as S

import qualified Text.Blaze.Html.Renderer.Text as BH

import           Numeric.MCMC.Diagnostics       ( summarize
                                                , ExpectationSummary(..)
                                                , mpsrf
                                                , mannWhitneyUTest
                                                )

-- to be removed once all things are converted to Frames-hvega
import qualified Graphics.Visualization.GOG.Data
                                               as GG
import qualified Graphics.Visualization.VegaLite.ParameterPlot
                                               as VV
import qualified Graphics.Visualization.VegaLite.Common
                                               as VV
--import qualified Graphics.Visualization.VegaLite.StackedArea as VV

import qualified Frames.Visualization.VegaLite.Data
                                               as FV
import qualified Frames.Visualization.VegaLite.StackedArea
                                               as FV
import qualified Frames.Visualization.VegaLite.LineVsTime
                                               as FV

import qualified Frames.ParseableTypes         as FP
import qualified Frames.Constraints            as FCon
import qualified Frames.VegaLite               as FV
import qualified Frames.Transform              as FT
import qualified Frames.Conversion             as FC
import qualified Frames.Folds                  as FF
import qualified Frames.Regression             as FR
import qualified Frames.MapReduce              as MR
import qualified Frames.Enumerations           as FE
import qualified Frames.Table                  as Table

import qualified Knit.Report                   as K
import           Polysemy.Error                 ( throw
                                                , Error
                                                )
import qualified Knit.Report.Other.Blaze       as KB
import qualified Knit.Effect.Pandoc            as K
                                                ( newPandoc
                                                , DocWithInfo(..)
                                                )

import           Data.String.Here               ( here )

import           BlueRipple.Data.DataFrames
import qualified BlueRipple.Model.TurnoutBayes as TB

templateVars = M.fromList
  [ ("lang"     , "English")
  , ("author"   , "Adam Conner-Sax & Frank David")
  , ("pagetitle", "Preference Model & Predictions")
--  , ("tufte","True")
  ]

loadCSVToFrame
  :: forall rs effs
   . ( MonadIO (K.Sem effs)
     , K.LogWithPrefixesLE effs
     , F.ReadRec rs
     , F.RecVec rs
     , V.RMap rs
     )
  => F.ParserOptions
  -> FilePath
  -> (F.Record rs -> Bool)
  -> K.Sem effs (F.FrameRec rs)
loadCSVToFrame po fp filterF = do
  let producer = F.readTableOpt po fp P.>-> P.filter filterF
  frame <- liftIO $ F.inCoreAoS producer
  let reportRows :: Foldable f => f x -> FilePath -> K.Sem effs ()
      reportRows f fn =
        K.logLE K.Diagnostic
          $  T.pack (show $ FL.fold FL.length f)
          <> " rows in "
          <> T.pack fn
  reportRows frame fp
  return frame


--------------------------------------------------------------------------------
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

--------------------------------------------------------------------------------
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

  --------------------------------------------------------------------------------
acrossTime :: T.Text
acrossTime = [here|
## Where Did the 2018 Votes Come From?
In our previous [post][BR:2018] we introduced a model which we used to infer voter
preference for various demographic groups in the 2018 house elections.
That model requires 2 inputs for each congressional district (CD):
an estimated number of voters in each demographic group and the democratic vs
republican vote totals.  We estimate the voter numbers via census data on population
and turnout by demographic grouping and we use the MIT election lab data on election
returns for the result data.
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
This is by design: the model's population numbers are scaled so
that the total votes in each district
and the total voters in each category add up correctly.
That means that the total number of D+R votes in each election
will be very close to what
we see in the data:

* This model indicates a - 4,800k shift (toward **republicans**)
2012 -> 2016 and the competitive popular house vote shifted -5,100k.
* This model indicates a +10,200k shift (toward **democrats**)
2014 -> 2018 and the competitive popular house vote shifted +10,800k.
* This model indicates a + 8,500k shift (toward **democrats**)
2016 -> 2018 and the competitive popular house vote shifted +8,900k.
* This model indicates a + 9,100k shift (toward **democrats**)
2010 -> 2018 and the competitive popular house vote shifted +9,600k. 

[^WikipediaHouse]: Sources:
<https://en.wikipedia.org/wiki/2010_United_States_House_of_Representatives_elections>
<https://en.wikipedia.org/wiki/2012_United_States_House_of_Representatives_elections>,
<https://en.wikipedia.org/wiki/2014_United_States_House_of_Representatives_elections>,
<https://en.wikipedia.org/wiki/2016_United_States_House_of_Representatives_elections>,
<https://en.wikipedia.org/wiki/2018_United_States_House_of_Representatives_elections>
|]

knitX
  :: forall r a
   . K.Member (Error PA.PandocError) r
  => X.ExceptT T.Text (K.Sem r) a
  -> K.Sem r a
knitX ma = X.runExceptT ma >>= (knitEither @r)

knitMaybe
  :: forall r a
   . K.Member (Error K.PandocError) r
  => T.Text
  -> Maybe a
  -> K.Sem r a
knitMaybe msg ma = maybe (K.knitError msg) return ma

knitEither
  :: forall r a
   . K.Member (Error K.PandocError) r
  => Either T.Text a
  -> K.Sem r a
knitEither = either K.knitError return

quick = RunParams 2 500 50
justEnough = RunParams 5 5000 500
goToTown = RunParams 10 10000 1000

type DemographicCategories = SimpleASR

main :: IO ()
main = do
  let template = K.FromIncludedTemplateDir "pandoc-mindoc-KH.html"
--  let template = K.FullySpecifiedTemplatePath "pandoc-templates/minWithVega-pandoc.html"
  pandocWriterConfig <- K.mkPandocWriterConfig template
                                               templateVars
                                               K.mindocOptionsF
  eitherDocs <-
    K.knitHtmls (Just "preference_model.Main") K.logAll pandocWriterConfig $ do
    -- load the data   
      let parserOptions =
            F.defaultParser { F.quotingMode = F.RFC4180Quoting ' ' }
      K.logLE K.Info "Loading data..."
      contextDemographicsFrame :: F.Frame ContextDemographics <- loadCSVToFrame
        parserOptions
        contextDemographicsCSV
        (const True)
      asrDemographicsFrame :: F.Frame ASRDemographics <-
        loadCSVToFrame parserOptions ageSexRaceDemographicsLongCSV (const True)
      aseDemographicsFrame :: F.Frame ASEDemographics <-
        loadCSVToFrame parserOptions ageSexEducationDemographicsLongCSV (const True)        
      houseElectionsFrame :: F.Frame HouseElections <- loadCSVToFrame
        parserOptions
        houseElectionsCSV
        (const True)
      asrTurnoutFrame :: F.Frame TurnoutASR <- loadCSVToFrame
        parserOptions
        detailedASRTurnoutCSV
        (const True)
      aseTurnoutFrame :: F.Frame TurnoutASE <- loadCSVToFrame
        parserOptions
        detailedASETurnoutCSV
        (const True)        
      K.logLE K.Info "Inferring..."
      let rp = quick
          yearList :: [Int]   = [2010, 2012, 2014, 2016, 2018]
          years      = M.fromList $ fmap (\x -> (x, x)) yearList
          categoriesASR = fmap (T.pack . show) $ dsCategories simpleAgeSexRace
          categoriesASE = fmap (T.pack . show) $ dsCategories simpleAgeSexEducation
          toNPE (category, (ExpectationSummary m (lo, hi) _)) =
            VV.NamedParameterEstimate category (VV.ParameterEstimate m (lo, hi))

      
      let modeledResults :: ( MonadIO (K.Sem r)
                            , K.KnitEffects r
                            , Show tr
                            , Show b
                            , Enum b
                            , Bounded b
                            , A.Ix b
                            , FL.Vector (F.VectorFor b) b)
            => DemographicStructure dr tr HouseElections b
            -> F.Frame dr -> F.Frame tr -> K.Sem r (M.Map Int (PreferenceResults b VV.NamedParameterEstimate)) 
          modeledResults ds dFrame tFrame = flip traverse years $ \y -> do
            K.logLE K.Info $ "inferring (SimpleASR) for " <> (T.pack $ show y)
            pr <- preferenceModel ds rp y dFrame houseElectionsFrame tFrame
            let pd =
                  A.array (minBound, maxBound)
                  $ fmap (\(b, es) -> (b, toNPE (T.pack $ show b, es)))
                  $ A.assocs
                  $ modeled pr
            return $ pr { modeled = pd }
            
      modeledResultsASR <- modeledResults simpleAgeSexRace asrDemographicsFrame asrTurnoutFrame
      modeledResultsASE <- modeledResults simpleAgeSexEducation aseDemographicsFrame aseTurnoutFrame

      let pdsWithYear x pr =
            let mapName pd@(VV.NamedParameterEstimate n _) =
                    pd { VV.name = n <> "-" <> x }
            in  fmap mapName $ modeled pr
          f x = fmap (\y -> (x, y))

      K.logLE K.Info "Knitting docs..."
      let flattenOneF y = FL.Fold
            (\l a -> (VV.name a, y, VV.value $ VV.pEstimate a) : l)
            []
            reverse
          flattenF = FL.Fold
            (\l (y, pr) -> FL.fold (flattenOneF y) (modeled pr) : l)
            []
            (concat . reverse)
          vRowBuilderPVsT =
            FV.addRowBuilder @'("Group",T.Text) (\(g, _, _) -> g)
              $ FV.addRowBuilder @'("Election Year",Int) (\(_, y, _) -> y)
              $ FV.addRowBuilder @'("D Voter Preference",Double)
                  (\(_, _, vp) -> vp)
                  FV.emptyRowBuilder
      K.newPandoc
          (K.PandocInfo
            "2018"
            (M.singleton "pagetitle" "2018 Preference Model Intro")
          )
        $ do            
            K.addMarkDown intro2018
            let prefsOneYear :: K.KnitOne r
                  => Int
                  -> M.Map Int (PreferenceResults b VV.NamedParameterEstimate)
                  -> K.Sem r ()
                prefsOneYear y mr = do
                  pr <-
                    knitMaybe "Failed to find 2018 in modelResults (SimpleASR)."
                    $ M.lookup y mr
                  _ <- K.addHvega Nothing Nothing $ VV.parameterPlotMany
                    id
                    "Modeled probability of voting Democratic in (competitive) 2018 house races"
                    S.cl95
                    (VV.ViewConfig 800 400 50)
                    (f (T.pack $ show y) $ pdsWithYear (T.pack $ show y) pr)
                  return ()
                  
            prASR_2018 <- prefsOneYear 2018 modeledResultsASR
            prASE_2018 <- prefsOneYear 2018 modeledResultsASE
            K.addMarkDown postFig2018
      K.newPandoc
          (K.PandocInfo
            "MethodsAndSources"
            (M.singleton "pagetitle"
                         "Inferred Preference Model: Methods & Sources"
            )
          )
        $ K.addMarkDown modelNotesBayes
      K.newPandoc
          (K.PandocInfo
            "AcrossTime"
            (M.singleton "pagetitle" "Preference Model Across Time")
          )
        $ do
            K.addMarkDown acrossTime
            -- arrange data for vs time plot

            let vDatPVsT_ASR =
                  FV.vinylRows vRowBuilderPVsT $ FL.fold flattenF $ M.toList
                    modeledResultsASR
                vParametersVsTime_ASR =
                  FV.multiLineVsTime @'("Group",T.Text) @'("Election Year",Int)
                    @'("D Voter Preference",Double)
                    "D Voter Preference Vs. Election Year"
                    FV.DataMinMax
                    (FV.TimeEncoding "%Y" FV.Year)
                    (FV.ViewConfig 800 400 50)
                    vDatPVsT_ASR
            _ <- K.addHvega Nothing Nothing vParametersVsTime_ASR

            let vDatPVsT_ASE =
                  FV.vinylRows vRowBuilderPVsT $ FL.fold flattenF $ M.toList
                    modeledResultsASE
                vParametersVsTime_ASE =
                  FV.multiLineVsTime @'("Group",T.Text) @'("Election Year",Int)
                    @'("D Voter Preference",Double)
                    "D Voter Preference Vs. Election Year"
                    FV.DataMinMax
                    (FV.TimeEncoding "%Y" FV.Year)
                    (FV.ViewConfig 800 400 50)
                    vDatPVsT_ASE
            _ <- K.addHvega Nothing Nothing vParametersVsTime_ASE

            -- arrange data for stacked area share of electorate
            let
              f1 :: [(x, [(y, z)])] -> [(x, y, z)]
              f1 = concat . fmap (\(x, yzs) -> fmap (\(y, z) -> (x, y, z)) yzs)
              vRowBuilderSVS =
                FV.addRowBuilder @'("Group",T.Text) (\(_, y, _) -> y)
                  $ FV.addRowBuilder @'("Election Year",Int) (\(x, _, _) -> x)
                  $ FV.addRowBuilder @'("D Voteshare of D+R Votes",Double)
                      (\(_, _, z) -> z)
                      FV.emptyRowBuilder
              vDatSVS_ASR = FV.vinylRows vRowBuilderSVS $ f1 $ M.toList $ fmap
                modeledDVotes
                modeledResultsASR
              vStackedArea_ASR =
                FV.stackedAreaVsTime @'("Group",T.Text) @'("Election Year",Int)
                  @'("D Voteshare of D+R Votes",Double)
                  "D Voteshare of D+R votes in Competitive Districts vs. Election Year"
                  (FV.TimeEncoding "%Y" FV.Year)
                  (FV.ViewConfig 800 400 50)
                  vDatSVS_ASR
            _ <- K.addHvega Nothing Nothing vStackedArea_ASR
            
            let vDatSVS_ASE = FV.vinylRows vRowBuilderSVS $ f1 $ M.toList $ fmap
                              modeledDVotes
                              modeledResultsASE
                vStackedArea_ASE =
                  FV.stackedAreaVsTime @'("Group",T.Text) @'("Election Year",Int)
                  @'("D Voteshare of D+R Votes",Double)
                  "D Voteshare of D+R votes in Competitive Districts vs. Election Year"
                  (FV.TimeEncoding "%Y" FV.Year)
                  (FV.ViewConfig 800 400 50)
                  vDatSVS_ASE
            _ <- K.addHvega Nothing Nothing vStackedArea_ASE

            -- analyze results
            -- Quick Mann-Whitney
            let
              mkDeltaTable locFilter (y1, y2) = do
                let y1T = T.pack $ show y1
                    y2T = T.pack $ show y2
                K.addMarkDown $ "### " <> y1T <> "->" <> y2T
                mry1 <- knitMaybe "lookup failure in mwu"
                  $ M.lookup y1 modeledResultsASR
                mry2 <- knitMaybe "lookup failure in mwu"
                  $ M.lookup y2 modeledResultsASR
                let
                  mwU =
                    fmap
                        (\f -> mannWhitneyUTest (S.mkPValue 0.05)
                                                f
                                                (mcmcChain mry1)
                                                (mcmcChain mry2)
                        )
                      $ fmap
                          (\n -> (!! n))
                          [0 .. (length (dsCategories simpleAgeSexRace) - 1)]
                K.logLE K.Info
                  $  "Mann-Whitney U  "
                  <> y1T
                  <> "->"
                  <> y2T
                  <> ": "
                  <> (T.pack $ show mwU)
                let (table, (mD1, mR1), (mD2, mR2)) =
                      deltaTable simpleAgeSexRace locFilter mry1 mry2
                K.addColonnadeTextTable deltaTableColonnade $ table
                K.addMarkDown
                  $  "In "
                  <> y1T
                  <> " the model expects "
                  <> (T.pack $ show mD1)
                  <> " total D votes, "
                  <> (T.pack $ show mR1)
                  <> " total R votes, so modeled D-R is "
                  <> (T.pack $ show (mD1 - mR1))
                K.addMarkDown
                  $  "In "
                  <> y2T
                  <> " the model expects "
                  <> (T.pack $ show mD2)
                  <> " total D votes, "
                  <> (T.pack $ show mR2)
                  <> " total R votes, so modeled D-R is "
                  <> (T.pack $ show (mD2 - mR2))
            K.addMarkDown voteShifts
            _ <-
              traverse (mkDeltaTable (const True))
                $ [ (2012, 2016)
                  , (2014, 2018)
                  , (2014, 2016)
                  , (2016, 2018)
                  , (2010, 2018)
                  ]
            K.addMarkDown voteShiftObservations
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
            K.addMarkDown "### Presidential Battleground States"
            _ <- mkDeltaTable bgOnly (2010, 2018)
            return ()
  case eitherDocs of
    Right namedDocs -> K.writeAllPandocResultsWithInfoAsHtml
      "reports/html/preference_model"
      namedDocs
    Left err -> putStrLn $ "pandoc error: " ++ show err

modeledDVotes :: forall b. (A.Ix b, Bounded b, Enum b, Show b)
  => PreferenceResults b VV.NamedParameterEstimate -> [(T.Text, Double)]
modeledDVotes pr =
  let
    summed = FL.fold
             (votesAndPopByDistrictF @b)
             (fmap F.rcast $ votesAndPopByDistrict pr)
    popArray =
      F.rgetField @(CountArray b) summed
    popScale   = F.rgetField @PopScale summed
    predVoters = F.rgetField @PredictedVoters summed
    allDVotes  = F.rgetField @DVotes summed
    allRVotes  = F.rgetField @RVotes summed
    shareOfDScale =
      popScale
      * ( realToFrac predVoters
          / realToFrac (allDVotes + allRVotes)
        )
      / realToFrac allDVotes
    shareOfDRScale =
      shareOfDScale
      * ( realToFrac allDVotes
          / realToFrac (allDVotes + allRVotes)
        )
    dVotes b =
      realToFrac (popArray A.! b)
      * ((nationalTurnout pr) A.! b)
      * (VV.value . VV.pEstimate $ (modeled pr) A.! b)
      * shareOfDRScale
  in
    fmap (\b -> (T.pack $ show b, dVotes b))
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
  :: forall dr tr e b
   . (A.Ix b, Bounded b, Enum b, Show b)
  => DemographicStructure dr tr e b
  -> (F.Record LocationKey -> Bool)
  -> PreferenceResults b VV.NamedParameterEstimate
  -> PreferenceResults b VV.NamedParameterEstimate
  -> ([DeltaTableRow], (Int, Int), (Int, Int))
deltaTable ds locFilter trA trB =
  let
    groupNames = fmap (T.pack . show) $ dsCategories ds
    getScaledPop
      :: PreferenceResults b VV.NamedParameterEstimate -> A.Array b Int
    getScaledPop tr =
      let
        totalRec = FL.fold
          votesAndPopByDistrictF
          ( fmap
              (F.rcast
                @'[CountArray b, DVotes, RVotes, PredictedVoters, PopScale]
              )
          $ F.filterFrame (locFilter . F.rcast)
          $ F.toFrame
          $ votesAndPopByDistrict tr
          )
        totalCounts = F.rgetField @(CountArray b) totalRec
      in
        totalCounts
    popA       = getScaledPop trA
    popB       = getScaledPop trB
    pop        = FL.fold FL.sum popA
    turnoutA   = nationalTurnout trA
    turnoutB   = nationalTurnout trB
    probsArray = fmap (VV.value . VV.pEstimate) . modeled
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
  in
    (groupRows ++ [totalRow], dVotesA, dVotesB)

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
racial identity (white-non-hispanic or non-white).
We recognize that these categories are limiting and much too simple.
But we believe it's a reasonable starting point, as a balance
between inclusiveness and having way too many variables.

For each congressional district where both major parties ran candidates, we have
census estimates of the number of people in each of our
demographic categories[^CensusDemographics].
And from the census we have national-level turnout estimates for each of these
groups as well[^CensusTurnout]. We assume that these turnout percentages
hold exactly in each district, giving a number of voters,
$N$, in each group, $i$, for each district.

All we can observe is the **sum** of all the votes in the district,
not the ones cast by each group separately.
But each district has a different demographic makeup and so each is a
distinct piece of data about how each group is likely to vote.

What we want to estimate, is how likely a voter in
each group is of voting for the
democratic candidate in a contested race.

For each district, $d$, we have the set of expected voters
(the number of people in each group, multiplied by the turnout for that group),
$\{V_i\}_d$, the number of democratic votes, $D_d$,
republican votes, $R_d$ and total votes, $T_d$, which may exceed $D_d + R_d$,
since there may be third party candidates. For the sake of simplicity,
we assume that all groups are equally likely to vote for a third party candidate.
We want to estimate $p_i$, the probability that
a voter (in any district) in the $i$th group--given that they voted
for a republican or democrat--will vote for the democratic candidate.                     

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

|]

--------------------------------------------------------------------------------
modelNotesBayes :: T.Text
modelNotesBayes = modelNotesPreface <> "\n\n" <> [here|

* Bayes theorem[^WP:BayesTheorem] relates the probability of a model
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
the specific outcome $\{D_k\}$
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
So the distribution of democratic votes across all types of voters is also normal,
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

[^WP:BayesTheorem]: <https://en.wikipedia.org/wiki/Bayes%27_theorem>

[WP:Bernoulli]: <https://en.wikipedia.org/wiki/Bernoulli_distribution>

[WP:Binomial]: <https://en.wikipedia.org/wiki/Binomial_distribution>

[WP:BinomialApprox]: <https://en.wikipedia.org/wiki/Binomial_distribution#Normal_approximation>

[WP:SumNormal]: <https://en.wikipedia.org/wiki/Sum_of_normally_distributed_random_variables>

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
type PredictedVoters = "PredictedVoters" F.:-> Int
type ScaledDVotes = "ScaledDVotes" F.:-> Int
type ScaledRVotes = "ScaledRVotes" F.:-> Int
type PopScale = "PopScale" F.:-> Double -- (DVotes + RVotes)/sum (pop_i * turnout_i)
type CountArray b = "CountArray" F.:-> A.Array b Int

addPopScale r =
  FT.recordSingleton @PopScale
    $ realToFrac (F.rgetField @DVotes r + F.rgetField @RVotes r)
    / realToFrac (F.rgetField @PredictedVoters r)

votesAndPopByDistrictF
  :: forall b
   . (A.Ix b, Bounded b, Enum b)
  => FL.Fold
       (F.Record '[CountArray b, DVotes, RVotes, PredictedVoters, PopScale])
       (F.Record '[CountArray b, DVotes, RVotes, PredictedVoters, PopScale])
votesAndPopByDistrictF =
  PF.dimap (F.rcast @'[CountArray b, DVotes, RVotes, PredictedVoters])
           (FT.mutate addPopScale)
    $    FF.sequenceEndoFolds
    $    FF.FoldEndo FE.sumTotalNumArray
    V.:& FF.FoldEndo FL.sum
    V.:& FF.FoldEndo FL.sum
    V.:& FF.FoldEndo FL.sum
    V.:& V.RNil

data PreferenceResults b a = PreferenceResults
  {
    votesAndPopByDistrict :: [F.Record [ StateAbbreviation
                                       , CongressionalDistrict
                                       , CountArray b
                                       , DVotes
                                       , RVotes
                                       , PredictedVoters
                                       , PopScale
                                       ]]
    , nationalTurnout :: A.Array b Double
    , modeled :: A.Array b a
    , mcmcChain :: TB.Chain -- exposed for significance testing of differences between years
  }

data RunParams = RunParams { nChains :: Int, nSamplesPerChain :: Int, nBurnPerChain :: Int }


preferenceModel
  :: forall dr tr b r
   . ( Show tr
     , Show b
     , Enum b
     , Bounded b
     , A.Ix b
     , FL.Vector (F.VectorFor b) b
--                , K.Member K.ToPandoc r
     , K.PandocEffects r
     , MonadIO (K.Sem r)
     )
  => DemographicStructure dr tr HouseElections b
  -> RunParams
  -> Int
  -> F.Frame dr
  -> F.Frame HouseElections
  -> F.Frame tr
  -> K.Sem
       r
       (PreferenceResults b (ExpectationSummary Double))
preferenceModel ds runParams year identityDFrame houseElexFrame turnoutFrame =
  do
    resultsFlattenedFrame <- knitX
      $ (dsPreprocessElectionData ds) year houseElexFrame
    filteredTurnoutFrame <- knitX
      $ (dsPreprocessTurnoutData ds) year turnoutFrame
    let year' = if (year == 2018) then 2017 else year -- we're using 2017 for now, until census updated ACS data
    longByDCategoryFrame <- knitX
      $ (dsPreprocessDemographicData ds) year' identityDFrame

    turnoutByGroupArray <-
      knitMaybe "Missing or extra group in turnout data?" $ FL.foldM
        (FE.makeArrayMF (F.rgetField @(DemographicCategory b))
                        (F.rgetField @VotedPctOfAll)
                        (flip const)
        )
        filteredTurnoutFrame
    turnoutPopByGroupArray <-
      knitMaybe "Missing or extra group in turnout data?" $ FL.foldM
        (FE.makeArrayMF (F.rgetField @(DemographicCategory b))
                        ((* 1000) . F.rgetField @Population)
                        (flip const)
        )
        filteredTurnoutFrame
    acsPopByGroupArray <-
      knitMaybe "Problem making array from summed ACS population"
      $ FL.foldM
          (FE.makeArrayMF (F.rgetField @(DemographicCategory b))
                          (F.rgetField @PopCount)
                          (flip const)
          )
      $ FL.fold
          (MR.concatFold $ MR.mapReduceFold
            MR.noUnpack
            (MR.assignKeysAndData @'[DemographicCategory b] @'[PopCount])
            (MR.foldAndAddKey $ FF.foldAllConstrained @Num FL.sum)
          )
          longByDCategoryFrame
    K.logLE K.Diagnostic
      $  "Pop From Turnout: "
      <> (T.pack $ show turnoutPopByGroupArray)
    K.logLE K.Diagnostic
      $  "Pop From ACS: "
      <> (T.pack $ show acsPopByGroupArray)
    let turnoutVsACSAdjustment b = realToFrac (turnoutPopByGroupArray A.! b)
          / realToFrac (acsPopByGroupArray A.! b)
    let turnoutByGroup b = turnoutByGroupArray A.! b
        scaleByTurnoutAndAdj b n =
          round $ turnoutByGroup b * turnoutVsACSAdjustment b * realToFrac n

        sumVotersF = PF.dimap
          (\r -> scaleByTurnoutAndAdj (F.rgetField @(DemographicCategory b) r)
                                      (F.rgetField @PopCount r)
          )
          (FT.recordSingleton @PredictedVoters)
          FL.sum
        predictedVotersF = MR.concatFold $ MR.mapReduceFold
          MR.noUnpack
          (MR.splitOnKeys @LocationKey)
          (MR.foldAndAddKey sumVotersF)
        predictedVotersFrame = MR.fold predictedVotersF longByDCategoryFrame

        resultsWPVFrame =
          F.toFrame
            $ fmap (FT.mutate addPopScale)
            $ catMaybes
            $ fmap F.recMaybe
            $ F.leftJoin @LocationKey resultsFlattenedFrame predictedVotersFrame

    let onlyOpposed r =
          (F.rgetField @DVotes r > 0) && (F.rgetField @RVotes r > 0)
        opposedRWPVFrame    = F.filterFrame onlyOpposed resultsWPVFrame
        numCompetitiveRaces = FL.fold FL.length opposedRWPVFrame
    K.logLE K.Info
      $ "After removing races where someone is running unopposed and scaling each group by turnout we have "
      <> (T.pack $ show numCompetitiveRaces)
      <> " contested races."
    -- some diagnostics here
    let allVotersF = FL.premap (F.rgetField @PredictedVoters) FL.sum
        allVotesF  = FL.premap (F.rgetField @Totalvotes) FL.sum
        allDVotesF = FL.premap (F.rgetField @DVotes) FL.sum
        allRVotesF = FL.premap (F.rgetField @RVotes) FL.sum
  --      allDRVotesF = FL.premap (\r -> F.rgetField @DVotes r + F.rgetField @RVotes r) FL.sum
        (totalVoters, totalVotes, totalDVotes, totalRVotes) = FL.fold
          ((,,,) <$> allVotersF <*> allVotesF <*> allDVotesF <*> allRVotesF)
          resultsWPVFrame
        (totalVotersCD, totalVotesCD, totalDVotesCD, totalRVotesCD) = FL.fold
          ((,,,) <$> allVotersF <*> allVotesF <*> allDVotesF <*> allRVotesF)
          opposedRWPVFrame
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
    -- flatten long data into arrays
    let votersArrayMF = MR.mapReduceFoldM
          (MR.generalizeUnpack $ MR.noUnpack)
          (MR.generalizeAssign $ MR.splitOnKeys @LocationKey)
          (MR.foldAndLabelM
            (fmap (FT.recordSingleton @(CountArray b))
                  (FE.recordsToArrayMF @(DemographicCategory b) @PopCount)
            )
            V.rappend
          )
    arrayCountsFrame <-
      knitMaybe "Error converting long demographic data to arrays for MCMC"
      $   F.toFrame
      <$> FL.foldM votersArrayMF longByDCategoryFrame
    let
      opposedRWPVWithArrayCountsFrame =
        catMaybes $ fmap F.recMaybe $ F.leftJoin @LocationKey opposedRWPVFrame
                                                              arrayCountsFrame
      scaleArrayCounts popScale =
        A.array (minBound, maxBound)
          . fmap
              (\(b, c) ->
                (b, round $ popScale * turnoutVsACSAdjustment b * realToFrac c)
              )
          . A.assocs
      opposedRWPVWithScaledArrayCountsFrame = fmap
        (\r -> F.rputField @(CountArray b)
          (scaleArrayCounts (F.rgetField @PopScale r)
                            (F.rgetField @(CountArray b) r)
          )
          r
        )
        opposedRWPVWithArrayCountsFrame
      popArrayToVotersList :: A.Array b Int -> [Int]
      popArrayToVotersList =
        fmap (\(b, c) -> round $ turnoutByGroup b * realToFrac c) . A.assocs
      scaleInt s n = round $ s * realToFrac n
      mcmcData =
        fmap
            (\r ->
              ( (F.rgetField @DVotes r)
              , popArrayToVotersList (F.rgetField @(CountArray b) r)
              )
            )
          $ opposedRWPVWithScaledArrayCountsFrame
      numParams = length $ dsCategories ds
    mcmcResults <- liftIO $ TB.runMany mcmcData
                                       numParams
                                       (nChains runParams)
                                       (nSamplesPerChain runParams)
                                       (nBurnPerChain runParams)
    -- this use of [0..(numParams - 1)] is bad.
    let conf = S.cl95
    summaries <- knitMaybe "mcmc \"summarize\" produced Nothing." $ traverse
      (\n -> summarize conf (!! n) mcmcResults)
      [0 .. (numParams - 1)]
    let summaryA = A.listArray (minBound :: b, maxBound) summaries
    K.logLE K.Info $ "summaries: " <> (T.pack $ show summaries)
    K.logLE K.Info
      $  "mpsrf="
      <> (T.pack $ show $ mpsrf (fmap (\n -> (!! n)) [0 .. (numParams - 1)])
                                mcmcResults
         )
    return $ PreferenceResults
      (fmap F.rcast opposedRWPVWithScaledArrayCountsFrame)
      turnoutByGroupArray
      summaryA
      (L.concat mcmcResults)

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


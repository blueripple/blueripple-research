{-# LANGUAGE DataKinds                 #-}
{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE GADTs                     #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE TypeApplications          #-}
{-# LANGUAGE TypeOperators             #-}
{-# LANGUAGE QuasiQuotes               #-}
{-# LANGUAGE AllowAmbiguousTypes       #-}
{-# LANGUAGE PolyKinds                 #-}
module Main where

import           Control.Lens                   ( (^.)
                                                , over
                                                , (&)
                                                , (%~)
                                                )
import qualified Control.Foldl                 as FL
import           Control.Monad                  ( when )
import           Control.Monad.IO.Class         ( MonadIO(liftIO) )
import qualified Colonnade                     as C
import qualified Text.Blaze.Colonnade          as C
import qualified Data.Functor.Identity         as I
import qualified Data.List as L
import qualified Data.Map                      as M
import           Data.Maybe                     ( catMaybes
                                                , fromMaybe
                                                , isNothing
                                                , isJust
                                                , fromJust
                                                )                 
import qualified Data.Monoid                   as MO
import           Data.Proxy                     ( Proxy(..) )
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
import qualified Pipes                         as P
import qualified Pipes.Prelude                 as P
import qualified Statistics.Types              as S
import           System.Random (randomRIO)
import qualified Statistics.Types              as S

import qualified Text.Blaze.Html.Renderer.Text as BH

import Numeric.MCMC.Diagnostics (summarize, ExpectationSummary (..), mpsrf, mannWhitneyUTest)
import Graphics.VegaLite.ParameterPlot (ParameterDetails (..), parameterPlot, parameterPlotMany)

import qualified Frames.ParseableTypes         as FP
import qualified Frames.VegaLite               as FV
import qualified Frames.Transform              as FT
import qualified Frames.Conversion             as FC
import qualified Frames.Folds                  as FF
import qualified Frames.Regression             as FR
import qualified Frames.MapReduce              as MR
import qualified Frames.Table                  as Table

import qualified Knit.Report                   as K
import           Polysemy.Error (throw)
import qualified Knit.Report.Other.Blaze       as KB
import qualified Knit.Effect.Pandoc            as K
                                                ( newPandoc
                                                , NamedDoc(..)
                                                )

import           Data.String.Here               ( here )

import           BlueRipple.Data.DataFrames 
import qualified BlueRipple.Model.TurnoutBayes as TB

templateVars = M.fromList
  [ ("lang"     , "English")
  , ("author"   , "Adam Conner-Sax")
  , ("pagetitle", "Turnout Model & Predictions")
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
introduction :: T.Text
introduction = [here|
## Where Did The 2018 Votes Come From?
The 2018 house races were generally good for Democrats and Progressives.
As we look to hold those gains and build on them, there are an assortment of
questions to ask about the 2018 results.  As an example, how much of the change
from 2016 to 2018 was the result of changes in demographics vs. voter turnout vs.
voters changing their votes?  Answering this is difficult. We don't have
granular turnout data and we have only exit-poll and post-election survey data
to look at for data about how people voted in each election.  In what follows, 
we attempt to use the election results themselves[^ResultsData], combined with
demographic data about the
populations in each house district[^CensusDemographics],
and census data regarding the turnout[^CensusTurnout]
of various demographic groups
to *infer* the likelihood of a given person voting
for the democratic candidate in a house race.
We'll interpret changes in that inferred probability
as people "changing their minds".

In order to get a more complete picture, we'll do the same work for the house
elections in 2012, 2014, 2016 and 2018 so we can look over time as well as
compare like-with-like in terms of presidential and non-presidential elections.

* In each year, we consider only "competitive" districts, that is those that had
a democrat and republican candidate. Of the possible 435 districts, 
In 2012 385 were competitive, in 2014 351 districts were competitive,
in 2016 369 districts were competitive, and 382 districts were competitive in 2018.

* Our demographic groupings are limited by the the categories recognized
and tabulated by the census and by our desire to balance specificity
(using more groups so that we might recognize people's identity more precisely)
with a need to keep the model small enough to make inference possible.
Thus for now we split the electorate into White (Non-Hispanic) and Non-White,
Male and Female and "Young" (<45) and "Old".

* More detail about the voter preference model and the techniques
used to perform inference
are in the [Preference-Model Notes](#preference-model-notes) section below.
Please note, though, these results are inferred from other data,
rather than measured directly through polling or surveys and as such
may not match those sorts of analyses.

The results are presented below. What stands out immediately is how strong
the support of non-white voters is for democratic candidates,
running at or above 75% (and often above 85%), regardless of age or sex,
though support is somewhat stronger among non-white female voters
than non-white male voters[^2014]. Support from white voters is substantially lower,
about between 35% and 45% across both age groups and both sexes, though people
under 45 are slightly more likely to vote democratic than their older
counterparts.  This is particularly noticeable in 2016,
when, perhaps because of Trump,
young white people became slightly more likely to vote for democratic
house candidates while older
white people became more likely to vote for republicans in the house.
As we move from 2016 to 2018, the non-white support holds,
maybe increasing slightly from its already high level,
and white support *grows* substantially across all ages and sexes,
though it remains below 50%. These results are broadly consistent with
exit-polling[^ExitPolls2012][^ExitPolls2014][^ExitPolls2016][^ExitPolls2018]. 

So we conclude that *some* of the 2018 democratic house votes came from
existing white voters changing their votes
while non-white support remained intensely high. Is that
the whole story?


[^ResultsData]: Source: Mit Election Lab <https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/IG0UN2> 
[^CensusDemographics]: Source: US Census, American Community Survey <https://www.census.gov/programs-surveys/acs.html> 
[^CensusTurnout]: Source: US Census, Voting and Registration Tables <https://www.census.gov/topics/public-sector/voting/data/tables.2014.html>
[^4]: We use 2017 demographic population data for our 2018 analysis, since that is the latest available from the census.
We will update this once the census publishes updated 2018 American Community Survey data.
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

--------------------------------------------------------------------------------  
voteShifts :: T.Text
voteShifts = [here|
Now we have an estimate of how peoples' choices changed between 2012 and 2018.
But that's only one part of the story.  Voting shifts are also driven by
changes in demographics (people move, get older, become eligible to vote
and people die) and different changes in voter turnout among different
demographic groups. In our simplistic model, we can look at these separately,
nationally and in any congressional district.

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
2012  59,646       58,228        +1,418
2014  35,624       40,081        -4,457
2016  61,417       62,772        -1,355
2018  60,320       50,467        +9,853

* Our model indicates a -4,140k shift toward republicans 2012 -> 2016 and the popular house vote shifted -2,773k.
* Our model indicates a +8,426 shift toward democrats 2014 -> 2018 and the popular house vote shifted +14,310k.
* Our model indicates a +5,567 shift toward democrats 2016 -> 2018 and the popular house vote shifted +11,208k.

We don't expect these numbers to match since we are only counting competitive house districts.  Still ???.

[^WikipediaHouse]: Sources: <https://en.wikipedia.org/wiki/2012_United_States_House_of_Representatives_elections>,
<https://en.wikipedia.org/wiki/2014_United_States_House_of_Representatives_elections>,
<https://en.wikipedia.org/wiki/2016_United_States_House_of_Representatives_elections>,
<https://en.wikipedia.org/wiki/2018_United_States_House_of_Representatives_elections>

|]
  
quick = RunParams 2 500 50
justEnough = RunParams 5 3000 300
thorough = RunParams 5 5000 500
goToTown = RunParams 10 10000 1000

  
main :: IO ()
main = do
  let writeNamedHtml (K.NamedDoc n lt) =
        T.writeFile (T.unpack $ "reports/html/" <> n <> ".html")
          $ TL.toStrict lt
      writeAllHtml       = fmap (const ()) . traverse writeNamedHtml
      pandocWriterConfig = K.PandocWriterConfig
        (Just "pandoc-templates/minWithVega-pandoc.html")
        templateVars
        K.mindocOptionsF
  eitherDocs <-
    K.knitHtmls (Just "turnout.Main") K.nonDiagnostic pandocWriterConfig $ do
    -- load the data   
      let parserOptions =
            F.defaultParser { F.quotingMode = F.RFC4180Quoting ' ' }
      K.logLE K.Info "Loading data..."
      contextDemographicsFrame :: F.Frame ContextDemographics <- loadCSVToFrame
        parserOptions
        contextDemographicsCSV
        (const True)
      identityDemographics2012Frame :: F.Frame IdentityDemographics <-
        loadCSVToFrame parserOptions identityDemographics2016CSV (const True)
      identityDemographics2014Frame :: F.Frame IdentityDemographics <-
        loadCSVToFrame parserOptions identityDemographics2017CSV (const True)        
      identityDemographics2016Frame :: F.Frame IdentityDemographics <-
        loadCSVToFrame parserOptions identityDemographics2016CSV (const True)
      identityDemographics2017Frame :: F.Frame IdentityDemographics <-
        loadCSVToFrame parserOptions identityDemographics2017CSV (const True)
      houseElectionsFrame :: F.Frame HouseElections <- loadCSVToFrame
        parserOptions
        houseElectionsCSV
        (const True)
      turnoutFrame :: F.Frame Turnout <- loadCSVToFrame parserOptions
                                         turnoutCSV
                                         (const True)
      K.logLE K.Info "Knitting..."
      K.newPandoc "turnout" $ do
        let rp = goToTown
        K.addMarkDown introduction
        K.logLE K.Info $ "inferring for 2012"
        res2012 <- turnoutModel rp 2012 identityDemographics2012Frame
                   houseElectionsFrame
                   turnoutFrame
        K.logLE K.Info $ "inferring for 2014"
        res2014 <- turnoutModel rp 2014 identityDemographics2014Frame
                   houseElectionsFrame
                   turnoutFrame
        K.logLE K.Info $ "inferring for 2016"
        res2016 <- turnoutModel rp 2016 identityDemographics2016Frame
                   houseElectionsFrame
                   turnoutFrame
        K.logLE K.Info $ "inferring for 2018"
        res2018 <- turnoutModel rp 2018 identityDemographics2017Frame
                    houseElectionsFrame
                    turnoutFrame
        let names = fmap T.pack $ F.columnHeaders (Proxy :: Proxy (F.Record IdentityCounts))
            toPD (name, (ExpectationSummary m (lo,hi) _)) = ParameterDetails name m (lo,hi)
        when (isJust (modeled res2012)
              && isJust (modeled res2014)
              && isJust (modeled res2016)
              && isJust (modeled res2018)) $ do          
          let tr2012 = res2012 { modeled = I.Identity $ fmap toPD $ zip names (fromJust $ modeled res2012) }
              tr2014 = res2014 { modeled = I.Identity $ fmap toPD $ zip names (fromJust $ modeled res2014) }
              tr2016 = res2016 { modeled = I.Identity $ fmap toPD $ zip names (fromJust $ modeled res2016) }
              tr2018 = res2018 { modeled = I.Identity $ fmap toPD $ zip names (fromJust $ modeled res2018) }
              pdsWithYear x tr =
                let mapName pd@(ParameterDetails n _ _) = pd {name = n <> "-" <> x}
                in fmap mapName $ I.runIdentity $ modeled tr
              f x = fmap (\y -> (x,y))
          _ <- K.addHvega Nothing Nothing $ parameterPlotMany id
            "Modeled Probability of Voting Democratic in competitive house races"
            S.cl95
            (concat
             $ [f "2012" $ pdsWithYear "2012" tr2012]
              ++ [f "2014" $ pdsWithYear "2014" tr2014]
              ++ [f "2016" $ pdsWithYear "2016" tr2016]
              ++ [f "2018" $ pdsWithYear "2018" tr2018]
            )
            -- analyze results
            -- Mann-Whitney
          let mwU = fmap (\f -> mannWhitneyUTest (S.mkPValue 0.05) f (mcmcChain res2016) (mcmcChain res2018)) $ fmap (\n-> (!!n)) [0..7]
          K.logLE K.Info $ "Mann-Whitney U  2016->2018: " <> (T.pack $ show mwU)          
          K.addMarkDown voteShifts
          K.addMarkDown "### 2012 -> 2016"
          K.addColonnadeTextTable deltaTableColonnade $ deltaTable tr2012 tr2016
          K.addMarkDown "### 2014 -> 2018"
          K.addColonnadeTextTable deltaTableColonnade $ deltaTable tr2014 tr2018
          K.addMarkDown "### 2016 -> 2018"
          K.addColonnadeTextTable deltaTableColonnade $ deltaTable tr2016 tr2018
          K.addMarkDown voteShiftObservations
        K.addMarkDown modelNotesBayes
  case eitherDocs of
    Right namedDocs -> writeAllHtml namedDocs --T.writeFile "mission/html/mission.html" $ TL.toStrict  $ htmlAsText
    Left  err       -> putStrLn $ "pandoc error: " ++ show err

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

deltaTable :: 
           TurnoutResults I.Identity ParameterDetails
           -> TurnoutResults I.Identity ParameterDetails
           -> [DeltaTableRow]
deltaTable trA trB = 
  let groups = fmap T.pack $ F.columnHeaders (Proxy :: Proxy (F.Record IdentityCounts))
      getScaledPop :: TurnoutResults I.Identity ParameterDetails -> [Int]
      getScaledPop tr =
        let popRec = popTotal tr
            scale = F.rgetField @PopScale popRec
            rescale = round . (* scale) . realToFrac
        in fmap rescale $ FC.toList @Int @IdentityCounts popRec
      popA = getScaledPop trA
      popB = getScaledPop trB
      pop = FL.fold FL.sum popA
      makeTurnoutList m = catMaybes $ fmap (\g -> fmap ((/1000.0) . realToFrac) (M.lookup g m <*> (Just 1000))) groups
      turnoutA = makeTurnoutList $ nationalTurnout trA
      turnoutB = makeTurnoutList $ nationalTurnout trB
      probA = fmap value $ I.runIdentity $ modeled trA
      probB = fmap value $ I.runIdentity $ modeled trB
      makeDTR n =
        let pop0 = realToFrac $ popA !! n
            dPop = realToFrac $ (popB !! n) - (popA !! n)
            turnout0 = realToFrac $ turnoutA !! n
            dTurnout = realToFrac $ (turnoutB !! n) - (turnoutA !! n)
            prob0 = realToFrac $ (probA !! n)
            dProb = realToFrac $ (probB !! n) - (probA !! n)
            dtrCombo = dPop * dTurnout * (2 * dProb)/4 -- the rest is accounted for in other terms, we spread this among them
            dtrN = round $ dPop * (turnout0 + dTurnout/2) * (2*(prob0 + dProb/2) - 1) + (dtrCombo/3)
            dtrT = round $ (pop0 + dPop/2) * dTurnout * (2*(prob0 + dProb/2) - 1) + (dtrCombo/3)
            dtrO = round $ (pop0 + dPop/2) * (turnout0 + dTurnout/2) * (2 * dProb) + (dtrCombo/3)
            dtrTotal = dtrN + dtrT + dtrO 
        in DeltaTableRow (groups !! n) (popB !! n) dtrN dtrT dtrO dtrTotal (realToFrac dtrTotal/realToFrac pop)
      groupRows = fmap makeDTR [0..7]
      addRow (DeltaTableRow g p fp ft fo t _) (DeltaTableRow _ p' fp' ft' fo' t' _) =
        DeltaTableRow g (p+p') (fp + fp') (ft +ft') (fo +fo') (t + t') (realToFrac (t + t')/realToFrac (p + p'))
      totalRow = FL.fold (FL.Fold addRow (DeltaTableRow "Total" 0 0 0 0 0 0) id) groupRows
  in groupRows ++ [totalRow]

deltaTableColonnade :: C.Colonnade C.Headed DeltaTableRow T.Text
deltaTableColonnade =  
  C.headed "Group" dtrGroup
  <> C.headed "Population (k)" (T.pack . show . (`div` 1000) . dtrPop)
  <> C.headed "+/- From Population (k)" (T.pack . show . (`div` 1000) . dtrFromPop)
  <> C.headed "+/- From Turnout (k)" (T.pack . show . (`div` 1000). dtrFromTurnout)
  <> C.headed "+/- From Opinion (k)" (T.pack . show . (`div` 1000) . dtrFromOpinion)
  <> C.headed "+/- Total (k)" (T.pack . show . (`div` 1000) . dtrTotal)
  <> C.headed "+/- %Vote" (T.pack . PF.printf "%2.2f" . (*100) . dtrPct) 
--  K.logLE K.Info $ T.pack $ show deltaTableRows
             
type DVotes = "DVotes" F.:-> Int
type RVotes = "RVotes" F.:-> Int
flattenVotes
  :: FL.Fold
       (F.Record '[Party, Candidatevotes, Totalvotes])
       (F.Record '[DVotes, RVotes, Totalvotes])
flattenVotes =
  FF.sequenceRecFold
    $    FF.recFieldF
           FL.sum
           (\r -> if F.rgetField @Party r == "democrat"
             then F.rgetField @Candidatevotes r
             else 0
           )
    V.:& FF.recFieldF
           FL.sum
           (\r -> if F.rgetField @Party r == "republican"
             then F.rgetField @Candidatevotes r
             else 0
           )
    V.:& FF.recFieldF (fmap (fromMaybe 0) $ FL.last) (F.rgetField @Totalvotes)
    V.:& V.RNil


{-
We'll be using expected voters to explain vote difference.  But if overall expected vote is less than actual D+R,
we had high turnout and we need to scale up or else that difference will cause probs to be artificially elevated
-}

--------------------------------------------------------------------------------
modelNotesPreface :: T.Text
modelNotesPreface = [here|
## Preference-Model Notes
Our goal is to use the house election results to fit a very simple model of the
electorate.  We consider the electorate as having eight
"identity" groups, split by sex (the census only records this as a F/M binary),
age, "old" (45 or older) and "young" (under 45) and
racial identity (white-non-hispanic or non-white).
We recognize that these categories are limiting and much too simple.
But we believe it's a reasonable starting point, as a balance
between inclusiveness and having way too many variables.

For each congressional district where both major parties ran candidates, we have
census estimates of the number of people in each of our demographic categories.
And from the census we have national-level turnout estimates for each of these
groups as well. We assume that these turnout percentages
hold exactly in each district, giving a number of voters,
$N$, in each group, $i$, for each district.

All we can observe is the **sum** of all the votes in the district,
not the ones cast by each
group separately.
But each district has a different demographic makeup and so each is a
distinct piece of data about how each group is likely to vote.

What we want to estimate, is how likely a voter in each group is of voting for the
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
(See [Wikipedia][WP:BinomialApprox]).  However, we can't observe the number
of votes from just one type of voter. We can only observe the sum over all types.
Luckily, the sum of normally distributed random variables follows a  normal
distribution as well.
So the distribution of democratic votes across all types of voters is also normal,
with mean $\sum_i V_i p_i$ and variance $\sum_i V_i p_i (1 - p_i)$
(See [Wikipedia][WP:SumNormal]). Thus we have $P(D_k|\{p_i\})$, or,
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
distribution given a prior, $P(\{p_i\})$,
the conditional $P(\{D_k\}|\{p_i\})$, and a starting $\{p_i\}$.
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
type PredictedVotes = "PredictedVotes" F.:-> Int
type ScaledDVotes = "ScaledDVotes" F.:-> Int
type ScaledRVotes = "ScaledRVotes" F.:-> Int
type PopScale    = "PopScale" F.:-> Double -- (DVotes + RVotes)/sum (pop_i * turnout_i)

type IdentityCounts = '[ OldNonWhiteFemale
                       , OldNonWhiteMale
                       , YoungNonWhiteFemale
                       , YoungNonWhiteMale
                       , OldWhiteFemale
                       , OldWhiteMale
                       , YoungWhiteFemale
                       , YoungWhiteMale
                       ]



data TurnoutResults f a = TurnoutResults
  {
    votesAndPopByDistrictF :: F.FrameRec ([ StateAbbreviation
                                          , CongressionalDistrict
                                          , PopScale
                                          , DVotes
                                          , RVotes
                                          ] V.++ IdentityCounts),
    popTotal :: F.Record (IdentityCounts V.++ '[DVotes,RVotes,PredictedVotes,PopScale]), -- F.Record ('[PopScale,DVotes,RVotes] V.++ IdentityCounts),
    nationalTurnout :: M.Map T.Text (Int -> Int),
    modeled :: f [a],
    mcmcChain :: TB.Chain -- exposed for significance testing of differences between years
  }
    
data RunParams = RunParams { nChains :: Int, nSamplesPerChain :: Int, nBurnPerChain :: Int }                        

turnoutModel
  :: (K.Member K.ToPandoc r, K.PandocEffects r, MonadIO (K.Sem r))
  => RunParams
  -> Int
  -> F.Frame IdentityDemographics
  -> F.Frame HouseElections
  -> F.Frame Turnout
  -> K.Sem r (TurnoutResults Maybe (ExpectationSummary Double)) --(Maybe [ExpectationSummary Double], TB.Chain)
turnoutModel runParams year identityDFrame houseElexFrame turnoutFrame = do
  -- rename some cols in houseElex

  let
    houseElexF = fmap
      ( F.rcast
        @'[Year, StateAbbreviation, CongressionalDistrict, Party, Candidatevotes, Totalvotes]
      . FT.retypeColumn @District @CongressionalDistrict
      . FT.retypeColumn @StatePo @StateAbbreviation
      )
      houseElexFrame
    unpack = MR.unpackFilterOnField @Year (== year)
    assign =
      MR.assignKeysAndData @'[StateAbbreviation, CongressionalDistrict]
        @'[Party, Candidatevotes, Totalvotes]
    reduce = MR.foldAndAddKey flattenVotes
    houseElexFlattenedF
      :: F.FrameRec
           '[StateAbbreviation, CongressionalDistrict, DVotes, RVotes, Totalvotes]
    houseElexFlattenedF =
      FL.fold (MR.concatFold $ MR.mapReduceFold unpack assign reduce) houseElexF
  K.logLE K.Diagnostic
    $  "before mapReduce: "
    <> (T.pack $ show (take 5 $ reverse $ FL.fold FL.list houseElexF))
  K.logLE K.Diagnostic
    $  "after mapReduce: "
    <> (T.pack $ show (take 5 $ FL.fold FL.list houseElexFlattenedF))
  let filteredTurnoutFrame = F.filterFrame ((== year) . F.rgetField @Year) turnoutFrame  
  K.logLE K.Diagnostic $ T.pack $ show (FL.fold FL.list filteredTurnoutFrame)
  K.logLE K.Diagnostic
    $  "Before scaling by turnout: "
    <> (T.pack $ show (take 5 $ FL.fold FL.list identityDFrame))
-- Use turnout so we have # voters of each type.
  let pVotes r = (r ^. youngWhiteMale) + (r ^. oldWhiteMale) + (r ^. youngWhiteFemale) + (r ^. oldWhiteFemale)
                 + (r ^. youngNonWhiteMale) + (r ^. oldNonWhiteMale) + (r ^. youngNonWhiteFemale) + (r ^. oldNonWhiteFemale)
  let scaleInt :: Double -> Int -> Int
      scaleInt s = round . (* s) . realToFrac
      scaleMap = FL.fold
        (FL.Fold
          (\m r -> M.insert (F.rgetField @Identity r)
                            (scaleInt (F.rgetField @VotedFraction r))
                            m
          )
          M.empty
          id
        )
        filteredTurnoutFrame
      popToVotedM = do -- Maybe Monad
        scaleYWM  <- M.lookup "YoungWhiteMale" scaleMap
        scaleOWM  <- M.lookup "OldWhiteMale" scaleMap
        scaleYWF  <- M.lookup "YoungWhiteFemale" scaleMap
        scaleOWF  <- M.lookup "OldWhiteFemale" scaleMap
        scaleYNWM <- M.lookup "YoungNonWhiteMale" scaleMap
        scaleONWM <- M.lookup "OldNonWhiteMale" scaleMap
        scaleYNWF <- M.lookup "YoungNonWhiteFemale" scaleMap
        scaleONWF <- M.lookup "OldNonWhiteFemale" scaleMap
        return
          $ (\r ->
              r
                & (youngWhiteMale %~ scaleYWM)
                & (oldWhiteMale %~ scaleOWM)
                & (youngWhiteFemale %~ scaleYWF)
                & (oldWhiteFemale %~ scaleOWM)
                & (youngNonWhiteMale %~ scaleYNWM)
                & (oldNonWhiteMale %~ scaleONWM)
                & (youngNonWhiteFemale %~ scaleYNWF)
                & (oldNonWhiteFemale %~ scaleONWF)
            , \r -> ( scaleYWM $ r ^. youngWhiteMale)
                    +(scaleOWM $ r ^. oldWhiteMale)
                    +(scaleYWF $ r ^. youngWhiteFemale)
                    +(scaleOWF$ r ^. oldWhiteFemale)
                    +(scaleYNWM $ r ^. youngNonWhiteMale)
                    +(scaleONWM $ r ^. oldNonWhiteMale) 
                    +(scaleYNWF $ r ^. youngNonWhiteFemale)
                    +(scaleONWF $ r ^. oldNonWhiteFemale) 
            
            )
  when (isNothing popToVotedM)
    $ K.logLE K.Error "popToVoted was not constructed!" -- we should throw something here
  let (popToVoted, sumOfVoted) = fromMaybe (id,const 0) popToVotedM
      popByIdentityAndResultsF =
        F.toFrame
          $ catMaybes
          $ fmap F.recMaybe
          $ F.leftJoin @'[StateAbbreviation, CongressionalDistrict]
              identityDFrame
              houseElexFlattenedF
  K.logLE K.Diagnostic
    $  "After joining: "
    <> (T.pack $ show (take 5 $ FL.fold FL.list popByIdentityAndResultsF))
  K.logLE K.Info
    $  "Joined identity and result data has "
    <> (T.pack $ show $ FL.fold FL.length popByIdentityAndResultsF)
    <> " rows."
  let opposedPBIRF = F.filterFrame
        (\r -> (F.rgetField @DVotes r > 0) && (F.rgetField @RVotes r > 0))
        popByIdentityAndResultsF
      opposedVBIRF = fmap popToVoted opposedPBIRF        
  K.logLE K.Info
    $  "After removing races where someone is running unopposed and scaling each group by turnout we have "
    <> (T.pack $ show $ FL.fold FL.length opposedVBIRF)
    <> " rows. (contested races)"
  let sumOfPeople r = (r ^. youngWhiteMale) + (r ^. oldWhiteMale) + (r ^. youngWhiteFemale) + (r ^. oldWhiteFemale)
                      + (r ^. youngNonWhiteMale) + (r ^. oldNonWhiteMale) + (r ^. youngNonWhiteFemale) + (r ^. oldNonWhiteFemale)
  let predictedVotesField = FT.recordSingleton @PredictedVotes . sumOfVoted
      vD = F.rgetField @DVotes
      vR = F.rgetField @RVotes
      popScaleV r = FT.recordSingleton @PopScale $ (realToFrac (vD r + vR r)/realToFrac (sumOfPeople r)) -- we've already scaled by turnout here
      popScaleP r = FT.recordSingleton @PopScale $ (realToFrac (vD r + vR r)/realToFrac (sumOfVoted r)) -- here we need to rescale as we sum
      opposedVBIRWithTargetF = fmap (FT.mutate popScaleV) opposedVBIRF
      opposedPBIRWithScaleF = fmap (FT.mutate (\r -> popScaleP r F.<+> predictedVotesField r)) opposedPBIRF
  K.logLE K.Diagnostic
    $  "Final frame: "
    <> (T.pack $ show (take 5 $ FL.fold FL.list opposedVBIRWithTargetF))
  let forMCMC r =
        let dVotes = F.rgetField @DVotes r
            rVotes = F.rgetField @RVotes r
            voteScale = 1/ F.rgetField @PopScale r
            scaledDVotes = realToFrac dVotes * voteScale
            scaledRVotes = realToFrac rVotes * voteScale 
        in (round scaledDVotes, FC.toList @Int @IdentityCounts r)
      mcmcData = fmap forMCMC $ FL.fold FL.list opposedVBIRWithTargetF
  -- generate some random starting points
  mcmcResults <- liftIO $ TB.runMany mcmcData 8 (nChains runParams) (nSamplesPerChain runParams) (nBurnPerChain runParams)
  let conf = S.cl95
      summaries = traverse (\n->summarize conf (!!n) mcmcResults) [0..7]         
  K.logLE K.Info $ "summaries: " <> (T.pack $ show summaries)
  K.logLE K.Info $ "mpsrf=" <> (T.pack $ show $ mpsrf (fmap (\n-> (!!n)) [0..7]) mcmcResults)
  let resFrame = fmap (F.rcast @([StateAbbreviation, CongressionalDistrict, PopScale, DVotes, RVotes] V.++ IdentityCounts)) opposedPBIRWithScaleF      
      totalPopRec = FL.fold (FF.foldAllConstrained @Num FL.sum) $ fmap (F.rcast @(IdentityCounts V.++ [DVotes,RVotes,PredictedVotes])) opposedPBIRWithScaleF
      totalScale = let r = totalPopRec in realToFrac (F.rgetField @DVotes r + F.rgetField @RVotes r)/(realToFrac $ F.rgetField @PredictedVotes r)
      totalPopWithScaleRec = V.rappend totalPopRec $ FT.recordSingleton @PopScale totalScale
  return $ TurnoutResults resFrame totalPopWithScaleRec scaleMap summaries (L.concat mcmcResults)  


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

Except it's not!! Because these parameters are probabilities and classic regression is not a good method here.  So we turn to Bayesian inference.  Which was more appropriate from the start.
|]


sumScaledPopF :: FL.Fold (F.Record ('[PopScale] V.++ IdentityCounts)) (F.Record IdentityCounts)
sumScaledPopF =
  FF.sequenceRecFold 
  $ FF.recFieldF FL.sum (\r -> round $ F.rgetField @PopScale r * realToFrac (F.rgetField @OldNonWhiteFemale r))
  V.:& FF.recFieldF FL.sum (\r -> round $ F.rgetField @PopScale r * realToFrac (F.rgetField @OldNonWhiteMale r))
  V.:& FF.recFieldF FL.sum (\r -> round $ F.rgetField @PopScale r * realToFrac (F.rgetField @YoungNonWhiteFemale r))
  V.:& FF.recFieldF FL.sum (\r -> round $ F.rgetField @PopScale r * realToFrac (F.rgetField @YoungNonWhiteMale r))
  V.:& FF.recFieldF FL.sum (\r -> round $ F.rgetField @PopScale r * realToFrac (F.rgetField @OldWhiteFemale r))
  V.:& FF.recFieldF FL.sum (\r -> round $ F.rgetField @PopScale r * realToFrac (F.rgetField @OldWhiteMale r))
  V.:& FF.recFieldF FL.sum (\r -> round $ F.rgetField @PopScale r * realToFrac (F.rgetField @YoungWhiteFemale r))
  V.:& FF.recFieldF FL.sum (\r -> round $F.rgetField @PopScale r * realToFrac (F.rgetField @YoungWhiteMale r))
  V.:& V.RNil

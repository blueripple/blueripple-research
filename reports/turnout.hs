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

import Numeric.MCMC.Diagnostics (summarize, ExpectationSummary (..))
import Graphics.VegaLite.ParameterPlot (ParameterDetails (..), parameterPlot, parameterPlotMany)

import qualified Frames.ParseableTypes         as FP
import qualified Frames.VegaLite               as FV
import qualified Frames.Transform              as FT
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

beforeProbs :: T.Text
beforeProbs = [here|
## Explaining the Blue Wave of 2018
The 2018 house races were generally good for Democrats and Progressives.
As we look to hold those gains and build on them there are an assortment of questions to ask about
the 2018 results.  As an example, what can we figure out about how much of the 2016 -> 2018 changes were the result of
changes in voter turnout vs. voters changing their minds?  This is a difficult question to answer since we don't have
very granular turnout data and we only have exit poll and post-election survey data to look at for data about how people
voted in each election.  Here, we attempt to use the election results themselves combined with demographic data about the
populations in each district and turnout of various demographic groups to *infer* the likelihood of a given person voting
for the democratic candidate in a house race.  We perform this inference using election results[^1] and demographic data[^2] from 2016 and then
repeat for 2018[^3].

* In each year, we consider only districts that had a democrat and republican candidates.
In 2018 that was 369 (of 438) districts, growing to 382 districts in 2018.

* Our demographic groupings are limited by the the categories recognized by the census
and by our desire to balance specificity (using more groups so that we might recognize people's identity more precisely) with
a need to keep the model small enough to make inference possible.  Thus for now we split the electorate into White (Non-Hispanic) and Non-White,
Male and Female and Young (<45) and Old. Those categories are denoted below as in the following table:

Label  Group
------ ------------------------
ONWF   Old Non-White Females
YNWF   Young Non-White Females
ONWM   Old Non-White Males
YNWM   Young Non-White Males
OWF    Old White Females
YWF    Young White Females
OWM    Old White Males
YWM    Young White Males

* More detail about the model and the techniques used to perform inference are in the "Model Notes" section below.

The results are presented below. What stands out immediately is how strong the support of non-white voters is for democratic candidates,
running at about 80% regardless of age or sex, though support is somewhat stronger among non-white female voters than non-white male voters.
Support from white voters is substantially lower, about 40% across all ages and sexes.
As we move from 2016 to 2018, the non-white support holds, maybe increasing slightly from its already high level, and white support *grows*
substantially across all ages and sexes, though it remains below 50%.


[^1]: Source: Mit Election Lab <https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/IG0UN2> 
[^2]: Source: US Census, American Community Survey <https://www.census.gov/programs-surveys/acs.html> 
[^3]: We use 2017 demographic population data for our 2018 analysis, since that is the latest available from the census.
We will update this whenever the census publishes updated 2018 American Community Survey data.  
|]


afterProbs :: T.Text
afterProbs = [here|
These results are broadly consistent with exit-polling from 2016 and 2018[^4]. But there is one confusing thing about these results:

* The numbers are not always consistent with exit-polling[^5] and after-election surveys[^6], especially when it comes to young white females.
Exit-polls and after-election surveys show YWF voting for Hillary Clinton at slightly over 50% (??).
but this model shows a much lower number, closer to 40%.  The model may be wrong, YWF's may have been more likely to split their ticket,
voting for Clinton but also for a Republican for the house seat, or the exit-poll or after-election survey data may be incorrect.


[^4]: <https://www.brookings.edu/blog/the-avenue/2018/11/08/2018-exit-polls-show-greater-white-support-for-democrats/>
[^5]: <https://www.nbcnews.com/storyline/2016-election-day/election-polls-nbc-news-analysis-2016-votes-voters-n680466>
[^6]: <http://www.rasmussenreports.com/public_content/political_commentary/commentary_by_geoffrey_skelley/another_look_back_at_2016>
|]

whatMatters :: T.Text
whatMatters = [here|
Now we have an estimate of how people's choices changed between 2016 and 2018.  But that's only one part of the story.  The other change is voter turnout.
Nationally, 2018 house races[^7] moved about 9 points towards the democrats in polls[^8] and XX points in the electoral results.  Was that driven more by turnout
 (lower turnout by whites and more by non-whites, e.g.,) or by changing minds (e.g., the move in white democratic voting probability we see in the above results)?

[^7]: <https://www.realclearpolitics.com/epolls/other/2016_generic_congressional_vote-5279.html>
[^8]: <https://www.realclearpolitics.com/epolls/other/2018_generic_congressional_vote-6185.html>
|]
  
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
    K.knitHtmls (Just "turnout.Main") K.logAll pandocWriterConfig $ do
    -- load the data   
      let parserOptions =
            F.defaultParser { F.quotingMode = F.RFC4180Quoting ' ' }
      K.logLE K.Info "Loading data..."
      contextDemographicsFrame :: F.Frame ContextDemographics <- loadCSVToFrame
        parserOptions
        contextDemographicsCSV
        (const True)
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
        K.addMarkDown beforeProbs
        results2016M <- turnoutModel 2016 identityDemographics2016Frame
                        houseElectionsFrame
                        turnoutFrame
        
        results2018M <- turnoutModel 2018 identityDemographics2017Frame
                       houseElectionsFrame
                       turnoutFrame
        let names = ["YWM","OWM","YWF","OWF","YNWM","ONWM","YNWF","ONWF"]
            toPD (name, (ExpectationSummary m (lo,hi) _)) = ParameterDetails name m (lo,hi)
        when (isJust results2016M && isJust results2018M) $ do
          let pds2016 = fmap toPD $ zip (fmap (<> "-2016") names) (fromJust results2016M)
              pds2018 = fmap toPD $ zip (fmap (<> "-2018") names) (fromJust results2018M)
          K.addHvega "VoteProbs" $ parameterPlotMany id
            "Modeled Probability of Voting Democratic in competitive house races"
            S.cl95
            (concat $ [fmap (\pd -> ("2016",pd)) pds2016] ++ [fmap (\pd -> ("2018",pd)) pds2018])
        K.addMarkDown afterProbs
        K.addMarkDown whatMatters
        K.addMarkDown modelNotesBayes
  case eitherDocs of
    Right namedDocs -> writeAllHtml namedDocs --T.writeFile "mission/html/mission.html" $ TL.toStrict  $ htmlAsText
    Left  err       -> putStrLn $ "pandoc error: " ++ show err

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


type X = "X" F.:-> Double
type PredictedVotes = "PredictedVotes" F.:-> Int

modelNotesPreface :: T.Text
modelNotesPreface = [here|
## Model Notes
Our goal is to use the 2016 house results to fit a very simple model of the electorate.  We consider the electorate as having eight
"identity" groups, split by sex (the census only records this as the F/M binary), age, young (<45) and old (>45) and racial identity (white or non-white). We recognize that these categories are limiting and much too simple. But we believe it's a reasonable starting point, as a balance between inclusiveness and having way too many variables.

For each congressional district where both major parties ran candidates (369 out of 438), we have census estimates of the number of people in each of our demographic categories.  And from the census we have national-level turnout estimates for each of these groups as well.
What we want to estimate, is how likely a voter in each group is of voting for the democratic candidate in a contested race.

We label our identity groups by a subscript $i$ and so, for each district, we have the set of expected voters (the number of people in each group, multiplied by the turnout for that group), $\{V_i\}$, the number of democratic votes, $D$,
republican votes, $R$ and total votes, $T$, which may exceed $D + R$ since there may be third party candidates. 
For the sake of simplicity, we assume that all groups are equally likely to vote for a third party candidate. And now we want to estimate $p_i$, the probability that
a voter in the $i$th group--who votes for a republican or democrat!!--will vote for the democratic candidate.                     

|]


modelNotesBayes :: T.Text
modelNotesBayes = modelNotesPreface <> "\n\n" <> [here|

* Bayes theorem relates the probability of a model (our probabilities $\{p_i\}$), given the observed data (the number of democratic votes recorded in each district, $\{D_k\}$) to the likelihood of observing that data given the model and our prior knowledge about the model:
$\begin{equation}
P(\{p_i\}|\{D_k\}) = \frac{P(\{D_k\}|\{p_i\})P(\{p_i\})}{P(\{D_k\})}
\end{equation}$

* What makes this useful is that $P(\{D_k\}|\{p_i\})$ is a thing we can compute. More on that later. $P(\{p_i\})$ is called a "prior" and amounts to an assumption about what we think we know about the parameters before we have seen any data.  In practice, this can often be set to something very boring, in our case, we will assume that our prior is just that any $p_i \in [0,1]$ is equally likely.

* $P(\{D_k\})$ is the unconditional probability that we observed our data.  This is difficult to compute! But, thankfully, what we are usually interested in is just finding the $\{p_i\}$ which maximize $P(\{p_i\}|\{D_k\})$ and, since $P(\{D_k\})$ doesn't depend on $\{p_i\}$, we don't need to know what it is.

* Back to the computation of $P(\{D_k\}|\{p_i\})$, the probability that we observed our evidence, given a specific set of $\{p_i\}$.  Our $p_i$ are the probability that one voter of type $i$ votes for the democrat.  Given $V_i$ voters of that type, the distribution of democratic votes *from that type of voter* is Bernoulli, with $V_i$ trials and $p_i$ probability of success.  But $V_i$ is quite large! So we can approximate this with a normal distribution with mean $V_i p_i$ and variance $V_i p_i (1 - p_i)$ (See [Wikipedia][WP:Binomial]).  However, we can't observe the number of votes from just one type of voter. We can only observe the sum over all types.  Luckily, the sum of normally distributed random variables is also normal.  So the distribution of democratic votes across all types of voters is also normal, with mean $\sum_i V_i p_i$ and variance $\sum_i V_i p_i (1 - p_i)$ (See [Wikipedia][WP:SumNormal]). Thus we have $P(D_k|\{p_i\})$, or, what amounts to the same thing, its density. But that means we also know $P(\{D_k\}|\{p_i\})$ since that is just the product of the normal distribution for each $D_k$:

$\begin{equation}
\mu_k(\{p_i\}) = \sum_i V_i p_i\\
v_k(\{p_i\}) = \sum_i V_i p_i (1 - p_i)\\
p(D_k|\{p_i\}) = \frac{1}{\sqrt{2\pi v_k}}e^{-\frac{(D_k -\mu_k(\{p_i\}))^2}{2v_k(\{p_i\})}}\\
p(\{D_k\}|\{p_i\}) = \Pi_k p(D_k|\{p_i\})
\end{equation}$


[WP:Binomial]: <https://en.wikipedia.org/wiki/Binomial_distribution#Normal_approximation>
[WP:SumNormal]: <https://en.wikipedia.org/wiki/Sum_of_normally_distributed_random_variables>

|]
  
turnoutModel
  :: (K.Member K.ToPandoc r, K.PandocEffects r, MonadIO (K.Sem r))
  => Int
  -> F.Frame IdentityDemographics
  -> F.Frame HouseElections
  -> F.Frame Turnout
  -> K.Sem r (Maybe [ExpectationSummary Double])
turnoutModel year identityDFrame houseElexFrame turnoutFrame = do
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
            )
  when (isNothing popToVotedM)
    $ K.logLE K.Error "popToVoted was not constructed!" -- we should throw something here
  let popToVoted       = fromMaybe id popToVotedM
      votedByIdentityF = fmap popToVoted identityDFrame
  K.logLE K.Diagnostic
    $  "After scaling by turnout: "
    <> (T.pack $ show (take 5 $ FL.fold FL.list votedByIdentityF))
  let votedByIdentityAndResultsF =
        F.toFrame
          $ catMaybes
          $ fmap F.recMaybe
          $ F.leftJoin @'[StateAbbreviation, CongressionalDistrict]
              votedByIdentityF
              houseElexFlattenedF
  K.logLE K.Diagnostic
    $  "After joining: "
    <> (T.pack $ show (take 5 $ FL.fold FL.list votedByIdentityAndResultsF))
  K.logLE K.Info
    $  "Joined identity and result data has "
    <> (T.pack $ show $ FL.fold FL.length votedByIdentityAndResultsF)
    <> " rows."
  let opposedVBIRF = F.filterFrame
        (\r -> (F.rgetField @DVotes r > 0) && (F.rgetField @RVotes r > 0))
        votedByIdentityAndResultsF
  K.logLE K.Info
    $  "After removing races where someone is running unopposed we have "
    <> (T.pack $ show $ FL.fold FL.length opposedVBIRF)
    <> " rows."
  let predictedVotes r = (r ^. youngWhiteMale) + (r ^. oldWhiteMale) + (r ^. youngWhiteFemale) + (r ^. oldWhiteFemale)
                         + (r ^. youngNonWhiteMale) + (r ^. oldNonWhiteMale) + (r ^. youngNonWhiteFemale) + (r ^. oldNonWhiteFemale)
      scaledDVotes r =
        let vD = F.rgetField @DVotes r
            vR = F.rgetField @RVotes r
        in FT.recordSingleton @X $ 0.5 * (realToFrac $ predictedVotes r) * (1+ (realToFrac $ vD - vR)/(realToFrac $ vD + vR))
      opposedVBIRWithTargetF = fmap (FT.mutate scaledDVotes . FT.mutate (\r -> FT.recordSingleton @PredictedVotes $ predictedVotes r)) opposedVBIRF      
  K.logLE K.Diagnostic
    $  "Final frame: "
    <> (T.pack $ show (take 5 $ FL.fold FL.list opposedVBIRWithTargetF))
  let forMCMC r =
        let dVotes = F.rgetField @DVotes r
            rVotes = F.rgetField @RVotes r
            totalVotes = F.rgetField @Totalvotes r
            predictedVotes = F.rgetField @PredictedVotes r
            scaledDVotes = realToFrac dVotes * (realToFrac predictedVotes/realToFrac totalVotes)
            scaledRVotes = realToFrac rVotes * (realToFrac predictedVotes/realToFrac totalVotes)
        in (round scaledDVotes,
             [
               F.rgetField @YoungWhiteMale r
             , F.rgetField @OldWhiteMale r
             , F.rgetField @YoungWhiteFemale r
             , F.rgetField @OldWhiteFemale r
             , F.rgetField @YoungNonWhiteMale r
             , F.rgetField @OldNonWhiteMale r
             , F.rgetField @YoungNonWhiteFemale r
             , F.rgetField @OldNonWhiteFemale r
             ])
      mcmcData = fmap forMCMC $ FL.fold FL.list opposedVBIRWithTargetF
  -- generate some random starting points
  let randomStart :: Int -> IO [Double]
      randomStart n = sequence $ replicate n (randomRIO (0,1))
      randomStarts :: Int -> Int -> IO [[Double]]
      randomStarts n m = sequence $ replicate m (randomStart n)
  starts <- liftIO $ randomStarts 8 50 -- should be 8 50
  mcmcResults <- liftIO $ traverse (fmap (drop 100) . TB.runMCMC mcmcData 1500) starts -- should be 1500
  let conf = S.cl95
      summaries = traverse (\n->summarize conf (!!n) mcmcResults) [0..7]         
  K.logLE K.Diagnostic $ "summaries: " <> (T.pack $ show summaries)
  return summaries


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

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

import qualified Text.Blaze.Html.Renderer.Text as BH

import qualified Frames.ParseableTypes         as FP
import qualified Frames.VegaLite               as FV
import qualified Frames.Transform              as FT
import qualified Frames.Folds                  as FF
import qualified Frames.Regression             as FR
import qualified Frames.MapReduce              as MR
import qualified Frames.Table                  as Table

import qualified Knit.Report                   as K
import qualified Knit.Report.Other.Blaze       as KB
import qualified Knit.Effect.Pandoc            as K
                                                ( newPandoc
                                                , NamedDoc(..)
                                                )

import           Data.String.Here               ( here )

import           BlueRipple.Data.DataFrames
import           BlueRipple.Model.TurnoutBayes (runMCMC)

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
  --    totalSpendingFrame :: F.Frame TotalSpending <- loadCSVToFrame parserOptions totalSpendingCSV (const True)
  --    totalSpendingBeforeFrame :: F.Frame TotalSpending <- loadCSVToFrame parserOptions totalSpendingBeforeCSV (const True)
  --    totalSpendingDuringFrame :: F.Frame TotalSpending <- loadCSVToFrame parserOptions totalSpendingDuringCSV (const True)
  --    forecastAndSpendingFrame :: F.Frame ForecastAndSpending <- loadCSVToFrame parserOptions forecastAndSpendingCSV (const True)
  --    electionResultsFrame :: F.Frame ElectionResults <- loadCSVToFrame parserOptions electionResultsCSV (const True)
  --    angryDemsFrame :: F.Frame AngryDems <- loadCSVToFrame parserOptions angryDemsCSV (const True)
      contextDemographicsFrame :: F.Frame ContextDemographics <- loadCSVToFrame
        parserOptions
        contextDemographicsCSV
        (const True)
      identityDemographicsFrame :: F.Frame IdentityDemographics <-
        loadCSVToFrame parserOptions identityDemographicsCSV (const True)
      houseElectionsFrame :: F.Frame HouseElections <- loadCSVToFrame
        parserOptions
        houseElectionsCSV
        (const True)
      turnout2016Frame :: F.Frame Turnout <- loadCSVToFrame parserOptions
                                                            turnout2016CSV
                                                            (const True)
      K.logLE K.Info "Knitting..."
      K.newPandoc "turnout" $ do
        turnoutModel identityDemographicsFrame
                     houseElectionsFrame
                     turnout2016Frame
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

modelNotesBayes :: T.Text
modelNotesBayes = modelNotesPreface <> "\n\n" <> [here|

* Bayes theorem relates the probability of a model (our probabilities $\{p_i\}$), given the observed data (the number of democratic votes recorded in each district, $\{D_k\}$) to the likelihood of observing that data given the model and our prior knowledge about the model:
$\begin{equation}
P(\{p_i\}|\{D_k\}) = \frac{P(\{D_k\}|\{p_i\})P(\{p_i\})}{P(\{D_k\})}
\end{equation}$

* What makes this useful is that $P(\{D_k\}|\{p_i\})$ is a thing we can compute. More on that later. $P(\{p_i\})$ is called a "prior" and amounts to an assumption about what we think we know about the parameters before we have seen any data.  In practice, this can often be set to something very boring, in our case, we will assume that our prior is just that any $p_i \in [0,1]$ is equally likely.

* $P(\{D_k\})$ is the unconditional probability that we observed our data.  This is difficult to compute! But, thankfully, what we are usually interested in is just finding the $\{p_i\}$ which maximize $P(\{p_i\}|\{D_k\})$ and, since $P(\{D_k\})$ doesn't depend on $\{p_i\}$, we don't need to know what it is.

* Back to the computation of $P(\{D_k\}|\{p_i\})$, the probability that we observed our evidence, given a specific set of $\{p_i\}$.  Our $p_i$ are the probability that one voter of type $i$ votes for the democrat.  Given $V_i$ voters of that type, the distribution of democratic votes *from that type of voter* is Bernoulli, with $V_i$ trials and $p_i$ probability of success.  But $V_i$ is quite large! So we can approximate this with a normal distribution with mean $V_i p_i$ and variance $V_i p_i (1 - p_i)$ (See [Wikipedia][WP:Binomial]).  However, we can't observe the number of votes from just one type of voter. We can only observe the sum over all types.  Luckily, the sum of normally distributed random variables is also normal.  So the distribution of democratic votes across all types of voters is also normal, with mean $\sum_i V_i p_i$ and variance $\sum_i V_i p_i (1 - p_i)$ (See [Wikipedia][WP:SumNormal]). Thus we have $P(\{D_k\}|\{p_i\})$, or, what amounts to the same thing, its density.

[WP:Binomial]: <https://en.wikipedia.org/wiki/Binomial_distribution#Normal_approximation>
[WP:SumNormal]: <https://en.wikipedia.org/wiki/Sum_of_normally_distributed_random_variables>

|]
  
turnoutModel
  :: (K.Member K.ToPandoc r, K.PandocEffects r, MonadIO (K.Sem r))
  => F.Frame IdentityDemographics
  -> F.Frame HouseElections
  -> F.Frame Turnout
  -> K.Sem r ()
turnoutModel identityDFrame houseElexFrame turnout2016Frame = do
  -- rename some cols in houseElex
  K.addMarkDown modelNotesBayes
  let
    houseElexF = fmap
      ( F.rcast
        @'[Year, StateAbbreviation, CongressionalDistrict, Party, Candidatevotes, Totalvotes]
      . FT.retypeColumn @District @CongressionalDistrict
      . FT.retypeColumn @StatePo @StateAbbreviation
      )
      houseElexFrame
    unpack = MR.unpackFilterOnField @Year (== 2016)
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
  K.logLE K.Diagnostic $ T.pack $ show (FL.fold FL.list turnout2016Frame)
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
        turnout2016Frame
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
      opposedVBIRWithTargetF = fmap (FT.mutate scaledDVotes) opposedVBIRF
  K.logLE K.Diagnostic
    $  "Final frame: "
    <> (T.pack $ show (take 5 $ FL.fold FL.list opposedVBIRWithTargetF))
{-
  let forMCMC r = (F.rgetField @DVotes r,
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
      mcmcData = take 5 $ fmap forMCMC $ FL.fold FL.list opposedVBIRF
  liftIO $ runMCMC mcmcData   
 -}  
-- AARGH!  WE need logistic regression or something because these p's are probabilities.    
  turnoutRegression <- FR.ordinaryLeastSquares @_ @X @False @[YoungWhiteMale,OldWhiteMale,YoungWhiteFemale,OldWhiteFemale,YoungNonWhiteMale,OldNonWhiteMale,YoungNonWhiteFemale,OldNonWhiteFemale] opposedVBIRWithTargetF
  K.addBlaze $ FR.prettyPrintRegressionResultBlaze (\y _ -> "Regression Details") turnoutRegression S.cl95 
  K.addHvega "turnoutRegressionCoeffs" $ FV.regressionCoefficientPlot "Parameters" ["YWM","OWM","YWF","OWF","YNWM","ONWM","YNWF","ONWF"] (FR.regressionResult turnoutRegression) S.cl95
  

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

module PreferenceModel.Methods (post) where

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

import PreferenceModel.Common


post :: K.KnitOne r
        => K.Sem r ()
post = do
 brAddMarkDown modelNotesBayes
 brAddMarkDown brReadMore

--------------------------------------------------------------------------------
modelNotesPreface :: T.Text
modelNotesPreface = [i|
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
(the number of people in each group, $i$ in that region, $N^{(d)}_i$,
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
for turnout in each region by finding the $\\delta^{(d)}$ which minimizes
$|T^{(d)} - \\sum_i N^{(d)}_i logit^{-1}(logit(t_i) + \\delta^{(d)})|$.
The authors then
state that the adjusted turnout in region $d$ is $\\hat{t}^{(d)}_i = t_i + \\delta^{(d)}$ which
doesn't make sense since $\\delta^{(d)}$ is not a probability.  This is corrected in the working
paper to $\\hat{t}^{(d)}_i = logit^{-1}(logit(t_i) + \\delta^{(d)})$.

|]

--------------------------------------------------------------------------------
modelNotesBayes :: T.Text
modelNotesBayes = modelNotesPreface <> "\n\n" <> [here|

* [Bayes theorem](<https://en.wikipedia.org/wiki/Bayes%27_theorem)
relates the probability of a model
(our demographic voting probabilities $\{p_i\}$),
given the observed data (the number of democratic votes recorded in each
district, $\{D^{(d)}\}$) to the likelihood of observing that data given the model
and some prior knowledge about the unconditional probability of the model itself
$P(\{p_i\})$, as well as $P(\{D^{(d)}\})$, the unconditional probability of observing
the "evidence":
$\begin{equation}
P(\{p_i\}|\{D^{(d)})P(\{D^{(d)}\}) = P(\{D^{(d)}\}|\{p_i\})P(\{p_i\})
\end{equation}$
In this situation, the thing we wish to compute, $P(\{p_i\}|\{D^{(d)}\})$,
is referred to as the "posterior" distribution.

* $P(\{p_i\})$ is called a "prior" and amounts to an assertion about
what we think we know about the parameters before we have seen any of the data.
In practice, this can often be set to something very boring, in our case,
we will assume that our prior is just that any $p_i \in [0,1]$ is equally likely.

* $P(\{D^{(d)}\})$ is the unconditional probability of observing
the specific outcome $\{D^{(d)}\}$, that is, the specific set of election results
we observe.
This is difficult to compute! Sometimes we can compute it by observing:
$\begin{equation}
P(\{D^{(d)}\}) = \sum_{\{p_i\}} P(\{D^{(d)}\}|\{p_i\}) P(\{p_i\})
\end{equation}$.  But in general, we'd like to compute the posterior in
some way that avoids needing the probability of the evidence.

* $P(\{D^{(d)}\}|\{p_i\})$, the probability that we
observed our evidence (the election results),
*given* a specific set $\{p_i\}$ of voter preferences is a thing
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
(again, see [Wikipedia][WP:SumNormal]). Thus we have $P(D^{(d)}|\{p_i\})$, or,
more precisely, its probability density.
But that means we also know the probability density of all the evidence
given $\{p_i\}$, $\rho(\{D^{(d)}\}|\{p_i\})$, since that is just the
product of the densities for each district:
$\begin{equation}
\mu^{(d)}(\{p_i\}) = \sum_i V_i p_i\\
v^{(d)}(\{p_i\}) = \sum_i V_i p_i (1 - p_i)\\
\rho(D^{(d)}|\{p_i\}) = \frac{1}{\sqrt{2\pi v^{(d)}}}e^{-\frac{(D^{(d)} -\mu^{(d)}(\{p_i\}))^2}{2v^{(d)}(\{p_i\})}}\\
\rho(\{D^{(d)}\}|\{p_i\}) = \Pi^{(d)} \rho(D^{(d)}|\{p_i\})
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
modelNotesMCMC = [i|
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

modelNotesRegression :: T.Text
modelNotesRegression = modelNotesPreface <> [i|

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


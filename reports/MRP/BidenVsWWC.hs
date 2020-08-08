{-# LANGUAGE AllowAmbiguousTypes       #-}
{-# LANGUAGE DataKinds                 #-}
{-# LANGUAGE DeriveDataTypeable        #-}
{-# LANGUAGE DeriveGeneric             #-}
{-# LANGUAGE PolyKinds                 #-}
{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE FlexibleInstances         #-}
{-# LANGUAGE GADTs                     #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE TypeApplications          #-}
{-# LANGUAGE TypeFamilies              #-}
{-# LANGUAGE TypeOperators             #-}
{-# LANGUAGE QuasiQuotes               #-}
{-# LANGUAGE StandaloneDeriving        #-}
{-# LANGUAGE TupleSections             #-}
{-# OPTIONS_GHC  -fplugin=Polysemy.Plugin  #-}

module MRP.BidenVsWWC where

import qualified Control.Foldl                 as FL
import           Data.Discrimination            ( Grouping )
import qualified Data.Map as M
import qualified Data.Text                     as T
import qualified Data.Serialize                as Serialize
import qualified Data.Vector                   as Vec
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V

import GHC.Generics (Generic)

import           Graphics.Vega.VegaLite.Configuration as FV
import qualified Graphics.Vega.VegaLite.Compat as FV
import qualified Frames as F
import qualified Frames.Melt as F
import qualified Frames.InCore                 as FI
import qualified Polysemy.Error as P

import qualified Control.MapReduce             as MR
import qualified Frames.Transform              as FT
import qualified Frames.Folds                  as FF
import qualified Frames.MapReduce              as FMR
import qualified Frames.SimpleJoins            as FJ
import qualified Frames.Misc                   as FM

import qualified Frames.Visualization.VegaLite.Data
                                               as FV

import qualified Graphics.Vega.VegaLite        as GV
import qualified Knit.Report                   as K

import           Data.String.Here               ( i, here )

import qualified Text.Blaze.Html               as BH
import qualified Text.Blaze.Html5.Attributes   as BHA

import           BlueRipple.Configuration 
import           BlueRipple.Utilities.KnitUtils 
import qualified BlueRipple.Utilities.TableUtils as BR

import qualified Numeric.GLM.ProblemTypes      as GLM
import qualified Numeric.GLM.Bootstrap            as GLM
import qualified Numeric.GLM.MixedModel            as GLM

import qualified Data.Time.Calendar            as Time
import qualified Data.Time.Clock               as Time

import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Data.Loaders as BR
import qualified BlueRipple.Data.ACS_PUMS as PUMS
import qualified BlueRipple.Data.CPSVoterPUMS as CPS
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.ElectionTypes as ET

import qualified BlueRipple.Model.MRP as BR
import qualified BlueRipple.Model.Turnout_MRP as BR

import qualified BlueRipple.Data.UsefulDataJoins as BR
import qualified MRP.CCES_MRP_Analysis as BR
import qualified MRP.CachedModels as BR
import qualified BlueRipple.Utilities.KnitUtils as BR
import qualified BlueRipple.Data.Keyed         as Keyed
import MRP.Common
import MRP.CCES
import qualified MRP.CCES as CCES

pollAvgDate = Time.fromGregorian 2020 8 7
pollAvgDateText = (\(y,m,d) ->  (T.pack $ show m)
                                <> "/" <> (T.pack $ show d)
                                <> "/" <> (T.pack $ show y)) $ Time.toGregorian pollAvgDate


bgPolls = [StatePoll "AK" (-4)
          ,StatePoll "AR" (-2)
          ,StatePoll "AZ" 5 
          ,StatePoll "FL" 4
          ,StatePoll "GA" 0
          ,StatePoll "IA" 0
          ,StatePoll "MI" 8
          ,StatePoll "MO" (-7)
          ,StatePoll "MT" (-7)
          ,StatePoll "NC" 1
          ,StatePoll "NH" 13
          ,StatePoll "NM" 14
          ,StatePoll "NV" 4
          ,StatePoll "OH" 0
          ,StatePoll "PA" 6
          ,StatePoll "TN" (-9)
          ,StatePoll "TX" (-1)
          ,StatePoll "UT" (-19)
          ,StatePoll "VA" 11
          ,StatePoll "WI" 8
          ]

text1 :: T.Text
text1 = [i|
In a [previous post][BR:BattlegroundTurnout],
we analyzed how changes in turnout might help Democrats win in various battleground states.
Meanwhile, Trump's campaign is doing the same math. As Trump has fallen in polls of battleground
states, it's becoming increasingly clear that his re-elections strategy is to "rally the base."
But can that strategy actually work?

Short answer: no.  Trump might win a few battleground states by boosting turnout among his
most die-hard supporters, but he'd still lose the rest, as well as the election overall.

1. **Two Possible GOP GOTV Strategies**
2. **Results Of Our Analysis**
3. **What Does It All Mean?**
4. **Take Action**
5. **Sidebar: The Math**

## Two Possible GOP GOTV Strategies
According to the [270toWin][BGPolls] polling average,
as of ${pollAvgDateText}, Biden leads in the polls in several battleground states.
Since we've spoken so much about demographically specific turnout boosts
, e.g., GOTV work among college-educated or young voters, we thought it would be
interesting to look at that strategy from the GOP side.  Whose turnout helps
Trump?  The only group that was strongly pro-Trump in 2016 was White, non-Latinx
voters without a 4-year college-degree (and not in-college), i.e.,
the so-called WWC.
Polling so far this cycle
shows their support for Trump is waning, both among
[female WWC voters][LAT:WWCWomen]
and
[younger WWC voters][Brookings:WWCYouth],
but overall, WWC voters are still, on average, Trump voters. This leads
to two possible options for the Republicans:

1. Focus on WWC: The WWC is
a big fraction of the US population, and is particularly concentrated
in a few key states. So one plausible Trump campaign strategy in the
battleground states might be to work at boosting turnout
among all WWC voters. 

2. Focus on the "Base": Another strategy, which seems closer to what is actually happening
right now, is to try to boost turnout only among "the base"--the portion of the
WWC that voted for Trump in 2016, and has stuck by Trump since then.

Could either of these approaches lead Trump to victory in 2020?

We'll look at the results shortly, but first a few quick notes about how we analyzed the problem:

- We assume that WWC voters in the battleground states remain like their 2016 selves,
both in their likelihood
to vote and in their preference for Trump.  These are very conservative (in the modeling
sense) assumptions.  All the polling suggests that the WWC has shifted toward the Democrats.  So we
firmly believe that the amount the GOP would need to boost
WWC turnout to make up for their current polling deficit
is probably higher than what we calculate below.
- For each state, we used our MRP model to estimate both the likelihood of voting and the preference
for Trump among all voters, grouped by state and membership in the WWC. For each state, we then used
the 2016 WWC preference for Trump, the total voting age population, and WWC voting age population
to figure out what extra percentage of the WWC would have to vote in order to negate Biden's current
lead in that state. 
- We also considered a turnout boost solely among the fraction of WWC people in
Trump's "base," which we took to be the fraction of the WWC that voted for Trump in 2016.
We assumed all extra turnout in that group will vote 100% for Trump.
- See the sidebar at the end of this post for more details on the math.

## Results Of Our Analysis
In each battleground state where Biden currently has a lead, we show the turnout boost that
Trump would need in either the WWC overall or his "base" to close the current statewide gap.
The boost he'd need from his base is always smaller because, as we discussed earlier, that group
has a higher preference for Trump than the WWC overall.

[BGPolls]: <https://www.270towin.com/2020-polls-biden-trump/>
[BR:BattlegroundTurnout]: <https://blueripple.github.io/research/mrp-model/p3/main.html>
[LAT:WWCWomen]: <https://www.latimes.com/politics/story/2020-06-26/behind-trumps-sharp-slump-white-women-who-stuck-with-him-before-are-abandoning-him-now>
[Brookings:WWCYouth]: <https://www.brookings.edu/blog/fixgov/2020/07/06/president-trump-is-losing-support-among-young-white-working-class-voters/>
|]

text2 :: T.Text = [here|
For context, typical election-to-election swings in WWC turnout are well under 5%.
10% turnout surges are virtually unheard of in recent electoral history. 
[This paper][SociologicalScience:WWCTurnout] breaks this down for the four presidential elections between 2004 and 2016,
showing almost no shifts in battleground state voting for White non-Latinx voters with a high school diploma
or less and 5% or less variability in the “Some college” group.

Thus, we make three observations from our analysis:

1. If Trump focuses on the WWC broadly,  NC is winnable, but nothing else.

2. If Trump focuses on his hard-core “base,” none of these states is winnable under realistic assumptions based on historical turnout variability.

3. If Trump focuses on his "base" and somehow manages an unprecedented 10% turnout boost there, 
he could plausibly also win AZ, FL, NV and PA. But, as noted, the odds of this happening from a change
in turnout are negligible.

If we do the electoral college math,
Biden wins the election unless the Trump campaign pulls off that unprecedented 10% turnout boost in the base
*and* everything else breaks his way.
First, let’s make a conservative assumption that Biden loses all the currently close states
where he’s trailing or tied right now (AK, AR, GA, IA, OH, TX).
(In reality, we believe Biden could win most of these.)
Under that assumption, not counting the states on the chart above,
Biden would have 226 Electoral votes (EVs) and Trump 204 of the 270 EVs needed to win.
If Biden wins all the states in the chart, he would end up winning the election with 334 EVs.
If Trump gets a 5% turnout boost in the WWC and/or his “base” in all of those states, he wins NC,
but Biden still wins the election with 319 EVs.
In the black swan scenario in which Trump increases base turnout by 10%,
he also takes AZ, FL, NV, and PA, and wins,
though it's important to note how unprecedented such a turnout shift would be.
 
So, our analysis of this table is that Trump can’t plausibly win the election
just by focusing on turnout of either the WWC or his most die-hard base.
However, there are some important caveats:

- These calculations assume that the rest of the electorate behaves as expected.
If Republicans simultaneously engage in voter suppression and other shenanigans to
reduce Dem turnout at the same time that they focus on their core voters,
then that could deliver the election for Trump.
Thus it is especially important that we fight to preserve voters’ rights in the run-up to November.

- These polls can and will shift between now and election day, so no one ought to be
complacent.  Progressives and Dems should work as hard as they can,
to insure winning the presidential election and as many down-ticket races as possible.
Fighting hard in places like GA, which may not be necessary to take the White House but
where 2 senate seats are up for grabs,
could well lead to Dems taking control of the Senate.

- The polls themselves are weighted by some reasonable calculation of who is likely to vote.
So whatever change in turnout the GOP needs,
it must happen on top of whatever those voters are saying now.
That makes the big shifts discussed above even less likely.

- One response to this is to
[argue that Trump voters are "shy" when responding to polls][WP:ShyTrumpVoter]
and thus polls under count Trump voters.  There is no evidence of this.  The shock of 2016 was
produced by last-minute changes which polling averages were slow to pick up and some issues
around [weighting the electorate for education][Upshot:EdWeighting],
the latter of which have been [addressed by the better pollsters][NBC:BetterPolling] since.

## What Does It All Mean?

- Dems could get complacent but we shouldn't.  Just as with
[recent modeling based on fundamentals and polls][Econ:2020Model], you could make a case that
Dems should be confident of winning the presidential election.  We hope that's not what you take away
from this. Trump's campaign sees the same data and will look to rally the base, the WWC at large,
change minds *and* suppress Dem votes.  And there are Senate and House races to win as well, many of them
being fought in the same places that matter for the presidential election.

- This analysis can explain Trump's current campaign strategy:
[overt racist appeals][NYT:TrumpConfederateFlag] to his base,
using [various attacks as wedge issues][NYT:TrumpAttack] to enhance WWC support
and win over suburban white college graduates, and
[fighting to suppress Vote-At-Home/Vote-By-Mail][NPR:TrumpVBM], which Trump
perceives, [incorrectly][PNAS:NonPartisanVBM], to favor Dem voters.

So, we think Dems need be especially focused on turning out their base and combating
voter suppression, particularly in FL, PA, and MI. Trump must hold his slim leads in
OH, TX, and GA, win all the closer states in the table above *and* win 1 out of 3 of
FL, PA or MI. If he loses *any* of the other states in our list, MI would no longer
be enough for Trump and he'd have to win FL or PA.  

But FL, PA and MI currently look out of reach via
traditional tactics.
If these polling margins hold up as the election gets closer, the primary
remaining strategy the Trump campaign will have is voter suppression.
COVID-19 [creates various problems][BR:COVID] for local election officials, and the Republicans at the
national and local level will not hesitate to use them to try and make voting more difficult
in Democrat supporting regions, particularly cities.
Expect demographically targeted social media, playing up the unreliability of Vote-By-Mail and
the danger of voting in-person.  Expect various targeted efforts to close polling
places and/or make them operate more slowly in Dem heavy areas,
forcing voters to wait in long lines.

## Take Action
With these things in mind, we recommend giving your time and/or money to the following organizations:

- Support [All Voting is Local][AllVotingIsLocal], a non-profit focused on voting-rights and enfranchisement
in many of the same states we are highlighting above, as well as GA, which is important this cycle
as well.

- Support the [Brennan Center][Brennan], working to improve voting systems, fight
voter suppression and restore voting rights to formerly incarcerated
citizens.

[AllVotingIsLocal]: <https://allvotingislocal.org/>
[NBC:BetterPolling]: <https://www.nbcnews.com/politics/meet-the-press/here-s-how-nbc-news-working-improve-its-state-polling-n1234716>
[Upshot:EdWeighting]: <https://www.nytimes.com/2017/05/31/upshot/a-2016-review-why-key-state-polls-were-wrong-about-trump.html>
[WP:ShyTrumpVoter]: <https://www.washingtonpost.com/politics/2020/05/13/theory-that-trumps-support-is-hampered-by-shy-voters-returns-another-election-cycle/>
[RCP:GA1]: <https://www.realclearpolitics.com/epolls/2020/senate/ga/georgia_senate_perdue_vs_ossoff-7067.html>
[RCP:GA2]: <https://www.realclearpolitics.com/epolls/2020/senate/ga/georgia_senate_special_election_open_primary-7069.html>
[FlipTXHouse]: <https://flipthetxhouse.com/>
[BR:COVID]: <https://blueripplepolitics.org/blog/coronavirus-1>
[BR:GA]: <https://blueripplepolitics.org/blog/ga-leg-2020>
[Brennan]: <https://www.brennancenter.org/issues/ensure-every-american-can-vote>
[FairFight]: <https://fairfight.com/latest-news/>
[TXSenate]: <https://www.realclearpolitics.com/epolls/2020/senate/tx/texas_senate_cornyn_vs_hegar-7047.html>
[Bitecofer:TX]: <https://www.reddit.com/r/VoteBlue/comments/gxze1l/rachel_bitecofer_rates_7_texas_house_races_likely/>
[PNAS:NonPartisanVBM]: <https://www.pnas.org/content/117/25/14052>
[NPR:TrumpVBM]: <https://www.npr.org/2020/06/22/881598655/fact-check-trump-spreads-unfounded-claims-about-voting-by-mail>
[NYT:TrumpConfederateFlag]: <https://www.nytimes.com/2020/07/06/us/politics/trump-bubba-wallace-nascar.html>
[NYT:TrumpAttack]: <https://www.nytimes.com/2020/07/17/us/trump-biden-2020-election.html>
[Econ:2020Model]: <https://projects.economist.com/us-2020-forecast/president>
[SociologicalScience:WWCTurnout]: <https://sociologicalscience.com/download/vol-4/november/SocSci_v4_656to685.pdf>
|]
  
sidebarMD :: T.Text = [here|
## Sidebar: The Math
For those of you interested in how the calculation works in more detail, here you go.
Let's call the number of people of voting age $N$, using $N_w$ for the number of
WWC voting-age people and $N_x$ for the non-WWC number, so $N=N_w+N_x$.
Similarly, we'll use
$V=V_w+V_x$ for the number of voters and $W_w$ and $W_x$
to work in percentages: $V_w = W_w N_w$ and $V_x = W_x N_x$. $P_w$ denotes the likelihood
that a member of the WWC votes for Trump (Technical note: $P_w$ is, more precisely, the
probability that they voted R *given* that they voted D or R. It's possible the non-voting
or third-party voting members of the WWC have different preferences).

So the number of votes for the Democrat, $V_D$, can be expressed as
$V_D =  P_w W_w N_w + P_x W_x N_x$ and the Republican vote as
$V_R = (1-P_w) W_w N_w + (1-P_x) W_x N_x$ and thus
$V_D - V_R = (2P_w-1) W_w N_w + (2P_x-1) W_x N_x$.  You may recognize $2P-1$ as a
quantity we called "Votes Per Voter" or "VPV" in an [earlier piece][BR:DeltaVPV].
We are given a polling lead, $M$ in percentage terms, so

$MN = (2P_w-1) W_w N_w + (2P_x-1) W_x N_x$

Now imagine a boost, $y_w$, in WWC turnout, which equalizes the votes. So

$\begin{equation}
0 = (2P_w-1) (W_w + y_w) N_w + (2P_x-1) W_x N_x\\
0 = (2P_w-1) W_w N_w + (2P_x-1) W_x N_x + y_w (2P_w-1) N_w
\end{equation}$

and, substituting $MN$ for the first part, $0 = M N + y_w (2P_w-1) N_w$. So $y_w = -\frac{M N}{(2P_w-1) N_w}$. This is the boost
required of the entire WWC. If, instead, we are just boosting turnout among members of the WWC
who will vote for Trump 100% of the time, we replace $2P_w-1$ by $1$ (always vote for Trump)
and $N_w$ by $(1-P_w)N_w$ ($1-P_w$ is the fraction of the WWC that votes for Trump 100% of the time) , yielding
$y_b = \frac{M N}{(1-P_w)Nw}$ ("b" for "base" in the subscript).

[BR:DeltaVPV]: <https://blueripple.github.io/research/mrp-model/p1/main.html>
|]


text3 :: T.Text
text3 = [i|

|]

data IsWWC_T = WWC | NonWWC deriving (Show, Generic, Eq, Ord)  
wwc r = if (F.rgetField @DT.Race5C r == DT.R5_WhiteNonLatinx) && (F.rgetField @DT.CollegeGradC r == DT.NonGrad)
        then WWC
        else NonWWC

type IsWWC = "IsWWC" F.:-> IsWWC_T
type ExcessWWCPer = "ExcessWWCPer" F.:-> Double
type ExcessBasePer = "ExcessBasePer" F.:-> Double

prefAndTurnoutF :: FF.EndoFold (F.Record '[PUMS.Citizens, ET.ElectoralWeight, ET.DemVPV, BR.DemPref])
prefAndTurnoutF =  FF.sequenceRecFold
                   $ FF.toFoldRecord (FL.premap (F.rgetField @PUMS.Citizens) FL.sum)
                   V.:& FF.toFoldRecord (BR.weightedSumRecF @PUMS.Citizens @ET.ElectoralWeight)
                   V.:& FF.toFoldRecord (BR.weightedSumRecF @PUMS.Citizens @ET.DemVPV)
                   V.:& FF.toFoldRecord (BR.weightedSumRecF @PUMS.Citizens @BR.DemPref)
                   V.:& V.RNil

addIsWWC :: (F.ElemOf rs DT.CollegeGradC 
            ,F.ElemOf rs DT.Race5C)
         => F.Record rs -> F.Record (IsWWC ': rs)
addIsWWC r = wwc r F.&: r

requiredExcessTurnoutF :: FL.FoldM
  Maybe
  (F.Record [IsWWC,PUMS.Citizens, ET.ElectoralWeight, ET.DemVPV, ET.DemPref])
  (F.Record '[ExcessWWCPer, ExcessBasePer])
requiredExcessTurnoutF =
  let requiredExcessTurnout :: M.Map IsWWC_T (F.Record [PUMS.Citizens, ET.ElectoralWeight, ET.DemVPV, ET.DemPref])
                            -> Maybe (F.Record '[ExcessWWCPer, ExcessBasePer])
      requiredExcessTurnout m = do
        wwcRow <- M.lookup WWC m
        nonWWCRow <- M.lookup NonWWC m
        let vals r = (F.rgetField @PUMS.Citizens r, F.rgetField @ET.ElectoralWeight r, F.rgetField @ET.DemVPV r, F.rgetField @ET.DemPref r)
            (wwcVAC, wwcEW, wwcDVPV, wwcDPref) = vals wwcRow
            (nonVAC, nonEW, _, _) = vals nonWWCRow
            voters = (realToFrac wwcVAC * wwcEW) + (realToFrac nonVAC * nonEW)
            excessWWCPer = negate voters / (realToFrac wwcVAC * wwcDVPV)
            excessBasePer =  voters / (realToFrac wwcVAC * (1 - wwcDPref))
        return $ excessWWCPer F.&: excessBasePer F.&: V.RNil
      
  in FM.widenAndCalcF
     (\r -> (F.rgetField @IsWWC r, F.rcast @[PUMS.Citizens, ET.ElectoralWeight, ET.DemVPV, ET.DemPref] r))
     prefAndTurnoutF
     requiredExcessTurnout


wwcFold :: FL.FoldM
           Maybe
           (F.Record (BR.StateAbbreviation ': (DT.CatColsASER5 V.++ [PUMS.Citizens, ET.ElectoralWeight, ET.DemVPV, ET.DemPref])))
           (F.FrameRec [BR.StateAbbreviation, ExcessWWCPer, ExcessBasePer])
wwcFold = FMR.concatFoldM
          $ FMR.mapReduceFoldM
          (FMR.generalizeUnpack $ FMR.Unpack $ pure @[] . addIsWWC)
          (FMR.generalizeAssign $ FMR.assignKeysAndData @'[BR.StateAbbreviation] @[IsWWC, PUMS.Citizens, ET.ElectoralWeight, ET.DemVPV, ET.DemPref])
          (FMR.makeRecsWithKeyM id $ FMR.ReduceFoldM $ const $ (fmap (pure @[]) requiredExcessTurnoutF))

type MaxTurnoutChange = "Max Turnout Change" F.:-> Double

--turnoutChangeF :: FL.Fold (F.Record ([BR.Year, BR.StateAbbreviation] V.++ DT.CatColsASER5 V.++ '[ET.ElectoralWeight]))
--                  (F.Record [BR.StateAbbreviation, MaxTurnoutChange])

data StatePoll = StatePoll { abbreviation :: T.Text, demMargin :: Double}

type PollMargin = "PollMargin" F.:-> Double  

type BoostResRow = F.Record [BR.StateAbbreviation, PollMargin, ExcessWWCPer, ExcessBasePer, BR.Electors]

statePollCollonade :: BR.CellStyle BoostResRow T.Text -> K.Colonnade K.Headed BoostResRow K.Cell
statePollCollonade cas =
  let h c = K.Cell (BHA.class_ "brTableHeader") $ BH.toHtml c
      margin = F.rgetField @PollMargin
      wwcBoost r = margin r * F.rgetField @ExcessWWCPer r
      baseBoost r = margin r * F.rgetField @ExcessBasePer r      
  in K.headed "State" (BR.toCell cas "State" "State" (BR.textToStyledHtml . F.rgetField @BR.StateAbbreviation))
     <> K.headed "Projected current D lead (% of Total)" (BR.toCell cas "D Margin" "D Margin" (BR.numberToStyledHtml "%2.1f" . margin))
     <> K.headed "Trump boost needed to tie (%WWC)" (BR.toCell cas "% WWC" "% WWC" (BR.numberToStyledHtml "%2.1f" . wwcBoost))
     <> K.headed "Trump boost needed to tie (% of \"base\")" (BR.toCell cas "% Base" "% Base" (BR.numberToStyledHtml "%2.1f" . baseBoost))
     <> K.headed "Electoral Votes" (BR.toCell cas "EVs" "EVs" (BR.textToStyledHtml . T.pack . show . F.rgetField @BR.Electors))

type  ASER5State as = (BR.StateAbbreviation ': DT.CatColsASER5) V.++ as

addPUMSZerosF :: FL.Fold (F.Record (ASER5State '[PUMS.Citizens])) (F.FrameRec (ASER5State '[PUMS.Citizens]))
addPUMSZerosF =
  let zeroPop ::  F.Record '[PUMS.Citizens]
      zeroPop = 0 F.&: V.RNil
  in FMR.concatFold
     $ FMR.mapReduceFold
     FMR.noUnpack
     (FMR.assignKeysAndData @'[BR.StateAbbreviation])
     (FMR.makeRecsWithKey id
       $ FMR.ReduceFold
       $ const
       $ Keyed.addDefaultRec @DT.CatColsASER5 zeroPop)
                                                                  
                    
post :: forall r.(K.KnitMany r, K.CacheEffectsD r, K.Member GLM.RandomFu r) => Bool -> K.Sem r ()
post updated = P.mapError BR.glmErrorToPandocError $ K.wrapPrefix "BidenVsWWC" $ do
  
  requiredExcessTurnout <- K.ignoreCacheTimeM $ do
    electoralWeights_C <- BR.adjCensusElectoralWeightsMRP_ASER5
    prefs_C <- BR.ccesPreferencesASER5_MRP
    demo_C <- PUMS.pumsLoader
    let cachedDeps = (,,) <$> electoralWeights_C <*> prefs_C <*> demo_C
--  K.clear "mrp/BidenVsWWC/requiredExcessTurnout.bin"
    BR.retrieveOrMakeFrame "mrp/BidenVsWWC/requiredExcessTurnout.bin" cachedDeps $ \(adjStateEW, statePrefs, demo) -> do
      let isYear y r = F.rgetField @BR.Year r == y
          isPres r = F.rgetField @ET.Office r == ET.President  
          ewFrame = fmap (F.rcast @(ASER5State '[ET.ElectoralWeight]))
                    $ F.filterFrame (isYear 2016) adjStateEW
          prefsFrame = fmap (F.rcast @(ASER5State '[ET.DemVPV, ET.DemPref]))
                       $ F.filterFrame (\r -> isYear 2016 r && isPres r) statePrefs
          demoFrame = FL.fold addPUMSZerosF
                      $ fmap (F.rcast @(ASER5State '[PUMS.Citizens]))
                      $ FL.fold
                      (PUMS.pumsStateRollupF (PUMS.pumsKeysToASER5 True . F.rcast))
                      $ F.filterFrame (isYear 2018) demo
      
      ewAndPrefs <- K.knitEither $ either (Left . T.pack . show) Right
                    $ FJ.leftJoinE3 @(ASER5State '[]) ewFrame prefsFrame demoFrame
      K.knitMaybe "Error computing requiredExcessTurnout (missing WWC or NonWWC for some state?)" (FL.foldM wwcFold (fmap F.rcast ewAndPrefs))

  logFrame requiredExcessTurnout
 
  let bgPollsD = filter (\(StatePoll _ m) -> m > 0 && m < 10) bgPolls
      pollF :: F.FrameRec '[BR.StateAbbreviation, PollMargin] = F.toFrame $ fmap (\(StatePoll sa dm) -> sa F.&: dm F.&: V.RNil) bgPollsD
  electoralVotes <- fmap (F.rcast @[BR.StateAbbreviation, BR.Electors])
                    . F.filterFrame ((== 2020). F.rgetField @BR.Year)
                    <$> (K.ignoreCacheTimeM $ BR.electoralCollegeFrame)
  withPollF <- K.knitEither $ either (Left . T.pack . show) Right $ FJ.leftJoinE3 @'[BR.StateAbbreviation] pollF requiredExcessTurnout electoralVotes
  logFrame withPollF
  curDate <-  (\(Time.UTCTime d _) -> d) <$> K.getCurrentTime
  let pubDateBidenVsWWC =  Time.fromGregorian 2020 8 8
  K.newPandoc
    (K.PandocInfo ((postRoute PostBidenVsWWC) <> "main")
      (brAddDates updated pubDateBidenVsWWC curDate
       $ M.fromList [("pagetitle", "White Working Class Turnout: Trump's Last Stand?")
                    ,("title","White Working Class Turnout: Trump's Last Stand?")
                    ]
      ))
      $ do        
        brAddMarkDown text1
        BR.brAddRawHtmlTable
          ("Needed Trump Boosts in Battleground States (" <> pollAvgDateText <> ")")
          (BHA.class_ "brTable")
          (statePollCollonade mempty)
          (fmap F.rcast withPollF)
        brAddMarkDown text2
{-        _ <- K.addHvega Nothing Nothing
             $ vlRallyWWC
             "Excess WWC/Base Turnout to Make Up Polling Gap"
             (FV.ViewConfig 200 (600 / (realToFrac $ length bgPolls)) 10)
             withPollF
-}
        brAddMarkDown sidebarMD
        brAddMarkDown brReadMore


vlRallyWWC :: Foldable f
                 => T.Text
                 -> FV.ViewConfig
                 -> f (F.Record [BR.StateAbbreviation, PollMargin, ExcessWWCPer, ExcessBasePer])
                 -> GV.VegaLite
vlRallyWWC title vc rows =
  let dat = FV.recordsToVLData id FV.defaultParse rows
      makeWWC = GV.calculateAs "datum.PollMargin * datum.ExcessWWCPer" "Excess WWC %"
      makeBase = GV.calculateAs "datum.PollMargin * datum.ExcessBasePer" "Excess Base %"
      renameMargin = GV.calculateAs "datum.PollMargin" "Poll Margin %"
      renameSA = GV.calculateAs "datum.state_abbreviation" "State"      
      doFold = GV.foldAs ["Excess WWC %", "Excess Base %"] "Type" "Pct"
      encY = GV.position GV.Y [GV.PName "Type", GV.PmType GV.Nominal, GV.PAxis [GV.AxNoTitle]
                              , GV.PSort [GV.CustomSort $ GV.Strings ["Excess WWC %", "Excess Base %"]]
                              ]
      encX = GV.position GV.X [GV.PName "Pct"
                              , GV.PmType GV.Quantitative
                              ]
      encFacet = GV.row [GV.FName "State", GV.FmType GV.Nominal]
      encColor = GV.color [GV.MName "Type"
                          , GV.MmType GV.Nominal
                          , GV.MSort [GV.CustomSort $ GV.Strings ["Excess WWC %", "Excess Base %"]]
                          ]
      enc = GV.encoding . encX . encY . encColor . encFacet
      transform = GV.transform .  makeWWC . makeBase . renameSA . renameMargin . doFold
  in FV.configuredVegaLite vc [FV.title title, enc [], transform [], GV.mark GV.Bar [], dat]

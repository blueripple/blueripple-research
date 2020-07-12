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

pollAvgDate = Time.fromGregorian 2020 7 11
pollAvgDateText = (\(y,m,d) ->  (T.pack $ show m)
                                <> "/" <> (T.pack $ show d)
                                <> "/" <> (T.pack $ show y)) $ Time.toGregorian pollAvgDate

text1 :: T.Text
text1 = [i|
In a [previous post][BR:BattlegroundTurnout],
we analyzed how changes in turnout might help Democrats win in various battleground states.
Meabwhile, Trump's campaign is doing the same math. AS Trump has fallen in polls of battleground
states, it's becoming increasingly clear that his re-elections strategy is to "rally the base."
But can that strategy actually work?

Short answer: no.  Trump might win up to two battleground states by boosting turnout among his
most diehard supporters, but he'd still lose the rest, as well as the election overall.

1. **Two Possible GOP GOTV Strategies**
2. **Results Of Our Analysis**
3. **Sidebar: The Math**
4. **What Does It All Mean?**

## Two Possible GOP GOTV Strategies
According to the [270toWin][BGPolls] polling average,
as of $pollAvgDateText$, Biden leads in the polls in several battleground states.
Since we've spoken so much about demographically specific turnout boosts
, e.g., GOTV work among college-educated or young voters, we thought it would be
interesting to look at that strategy from the GOP side.  Whose turnout helps
Trump?  The only group that was strongly pro-trump in 2016 was White, non-Latinx
voters without a 4-year college-degree (and not in-college), i.e.,
the so-called WWC.
Polling so far this cycle
shows their support for Trump is waning, both among
[female WWC voters][LAT:WWCWomen]
and
[younger WWC voters][Brookings:WWCYouth],
but overall, WWC voters are still, on average, Trump voters. This leads
to two possible options for the Republicans:

1. The WWC is
a big fraction of the US population, and is particularly concentrated
in a few key states. So one plausible Trump campaign strategy in the
battleground states might be to work at boosting turnout
among all WWC voters. 

2. Another strategy, which seems closer to what is actually happening
right now, is to try to boost turnout only among "the base" that has stuck by Trump since the
2016 election.

Could either of these approaches lead Trump to victory in 2020?

We'll look at the results shortly, but first a few quick notes about how we analyzed the problem:

- We assume that WWC voters in the battleground states remain like their 2016 selves,
both in their likelihood
to vote and in their preference for Trump.  These are very conservative (in the modeling
sense) assumptions.  All the polling suggests that the WWC has shifted toward the Democrats.  So we
firmly believe that the amount the GOP would need to boost
WWC turnout to make up for their current polling deficit,
is probably higher than what we calculate below.
- For each state, we used our MRP model to estimate both the likelihood of voting and the preference
for Trump among all voters, grouped by state and membership in the WWC. For each state, we then used
the 2016 WWC preference for Trump, the total voting age population, and WWC voting age population
to figure out what extra percentage of the WWC would have to vote in order to negate Biden's current
lead in that state. 
- We also considered a turnout boost solely among the fraction of WWC people in
Trump's "base," which we took to be the fraction of the WWC which voted for Trump in 2016.
We assumed all extra turnout in that group will vote 100% for Trump.
- See the sidebar at the end of this post for more details on the math.

## Results Of Our Analysis
In each battleground state where Biden currently has a a lead, we show the turnout boost that
Trump would need in either the WWC overall or his "base" to close the current statewide gap.
The boost he'd need from his base is always smaller because, as we discussed earlier, that group
has a higher preference for Trump than the WWC overall.

[BGPolls]: <https://www.270towin.com/2020-polls-biden-trump/>
[BR:BattlegroundTurnout]: <https://blueripple.github.io/research/mrp-model/p3/main.html>
[LAT:WWCWomen]: <https://www.latimes.com/politics/story/2020-06-26/behind-trumps-sharp-slump-white-women-who-stuck-with-him-before-are-abandoning-him-now>
[Brookings:WWCYouth]: <https://www.brookings.edu/blog/fixgov/2020/07/06/president-trump-is-losing-support-among-young-white-working-class-voters/>
|]

text2 :: T.Text = [here|
With the exception of GA and OH, Trump would need more than a 10% surge in voting among the WWC
to win any of those states, something that has not happened in recent history.
Typical election-to-election swings in WWC voting are well under 5%.
[This paper][SociologicalScience:WWCTurnout] breaks this down for the four
presidential elections between 2004 and 2016, showing almost no shifts in battleground
state voting for White non-Latinx voters with a high school diploma or less and 5% or
less variability in the "Some college" group.

It's harder to analyze turnout in
Trump's "base," but there's no a-priori reason to think it would be possible to drive
turnout there by more than 5% either. An unlikely 10% turnout shift in the "base"
could bring AZ, FL, GA, NC, NV, and OH back to the GOP, which still leaves Biden
winning the election, though not by much.

So, our analysis of this table is that Trump might be able to close the gap
with Biden in GA and OH with a general appeal to the WWC and that there is a small chance
of winning a few more states if he can mobilize his most ardent followers within the WWC.
But in the other battleground states, “rally the base” will be woefully insufficient.
And none of this is enough to swing the election to Trump.

Two important caveats:

-These polls can and will shift between now and election day, so no one ought to be
complacent.  Progerssives and Dems should work as hard as they can,
to insure winning the presidential election and as many down-ticket races as possible.
Fighting hard in places like GA, which may not be necessary to take the White House but
where 2 senate seats are up for grabs,
could well lead to Dems taking control of the Senate.

-The polls themselves are weighted by some reasonable calculation of who is likely to vote.
So whatever change in turnout the GOP needs,
it must happen on top of whatever those voters are saying now.
That makes big shifts less likely.

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
that a member of the WWC votes for Trump (Technical note: $P_w$ is, more precisly, the
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

type BoostResRow = F.Record [BR.StateAbbreviation, PollMargin, ExcessWWCPer, ExcessBasePer]

statePollCollonade :: BR.CellStyle BoostResRow T.Text -> K.Colonnade K.Headed BoostResRow K.Cell
statePollCollonade cas =
  let h c = K.Cell (BHA.class_ "brTableHeader") $ BH.toHtml c
      margin = F.rgetField @PollMargin
      wwcBoost r = margin r * F.rgetField @ExcessWWCPer r
      baseBoost r = margin r * F.rgetField @ExcessBasePer r      
  in K.headed "State" (BR.toCell cas "State" "State" (BR.textToStyledHtml . F.rgetField @BR.StateAbbreviation))
     <> K.headed "Biden Lead" (BR.toCell cas "D Margin" "D Margin" (BR.numberToStyledHtml "%2.1f" . margin))
     <> K.headed "% WWC" (BR.toCell cas "% WWC" "% WWC" (BR.numberToStyledHtml "%2.1f" . wwcBoost))
     <> K.headed "% Base" (BR.toCell cas "% Base" "% Base" (BR.numberToStyledHtml "%2.1f" . baseBoost))

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
    --K.clear "mrp/BidenVsWWC/requiredExcessTurnout.bin"
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
  let bgPolls = [StatePoll "AZ" 5 
                ,StatePoll "FL" 4
                ,StatePoll "GA" 3
                ,StatePoll "IA" (-1)
                ,StatePoll "MI" 7
                ,StatePoll "NC" 5
                ,StatePoll "NM" 14
                ,StatePoll "NV" 4
                ,StatePoll "OH" 1
                ,StatePoll "PA" 8
                ,StatePoll "TX" 0
                ,StatePoll "VA" 12
                ,StatePoll "WI" 7
                ]
      bgPollsD = filter (\(StatePoll _ m) -> m > 0) bgPolls
      pollF :: F.FrameRec '[BR.StateAbbreviation, PollMargin] = F.toFrame $ fmap (\(StatePoll sa dm) -> sa F.&: dm F.&: V.RNil) bgPollsD
  withPollF <- K.knitEither $ either (Left . T.pack . show) Right $ FJ.leftJoinE @'[BR.StateAbbreviation] pollF requiredExcessTurnout
  logFrame withPollF
  curDate <-  (\(Time.UTCTime d _) -> d) <$> K.getCurrentTime
  let pubDateBidenVsWWC =  Time.fromGregorian 2020 7 9
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
          ("Needed WWC Boosts in Battleground States (" <> pollAvgDateText <> ")")
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
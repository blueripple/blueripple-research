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

module MRP.TurnoutGaps where

import qualified Control.Foldl                 as FL
import           Control.Monad (join)
import qualified Data.Array                    as A
import           Data.Function (on)
import qualified Data.List as L
import qualified Data.Set as S
import qualified Data.Map                      as M
import           Data.Maybe (isJust, catMaybes, fromMaybe)
import           Data.Proxy (Proxy(..))
--import  Data.Ord (Compare)

import qualified Data.Text                     as T
import qualified Data.Serialize                as SE
import qualified Data.Vector as V
import qualified Data.Vector.Storable               as VS


import           Graphics.Vega.VegaLite.Configuration as FV
import qualified Graphics.Vega.VegaLite.Compat as FV
import qualified Frames as F
import qualified Frames.Melt as F
import qualified Frames.InCore as FI
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V


import qualified Control.MapReduce             as MR
import qualified Frames.Transform              as FT
import qualified Frames.Folds                  as FF
import qualified Frames.MapReduce              as FMR
import qualified Frames.Enumerations           as FE
import qualified Frames.Utils                  as FU
import qualified Frames.Serialize              as FS

import qualified Frames.Visualization.VegaLite.Data
                                               as FV
import qualified Frames.Visualization.VegaLite.ParameterPlots
                                               as FV                                               

import qualified Graphics.Vega.VegaLite        as GV
import qualified Knit.Report                   as K
import qualified Polysemy.Error                as P (mapError, Error)
import qualified Polysemy                      as P (raise)
import           Text.Pandoc.Error             as PE
import qualified Text.Blaze.Colonnade          as BC

import           Data.String.Here               ( here, i )

import qualified Colonnade                     as C
import qualified Text.Blaze.Colonnade          as BC
import qualified Text.Blaze.Html               as BH
import qualified Text.Blaze.Html5.Attributes   as BHA

import           BlueRipple.Configuration 
import           BlueRipple.Utilities.KnitUtils 
import qualified BlueRipple.Utilities.TableUtils as BR
--import           BlueRipple.Data.DataFrames 

import qualified Data.IndexedSet               as IS
import qualified Numeric.GLM.ProblemTypes      as GLM
import qualified Numeric.GLM.ModelTypes      as GLM
import qualified Numeric.GLM.FunctionFamily    as GLM
import qualified Numeric.GLM.MixedModel        as GLM
import qualified Numeric.GLM.Bootstrap            as GLM
import qualified Numeric.GLM.Report            as GLM
import qualified Numeric.GLM.Predict            as GLM
import qualified Numeric.GLM.Confidence            as GLM
import qualified Numeric.SparseDenseConversions as SD

import qualified Statistics.Types              as ST
import GHC.Generics (Generic)

import qualified Data.Time.Calendar            as Time
import qualified Data.Time.Clock               as Time
import qualified Data.Time.Format              as Time

import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Data.Loaders as BR
import qualified BlueRipple.Data.DemographicTypes as BR
import qualified BlueRipple.Data.ElectionTypes as ET
import qualified BlueRipple.Data.HouseElectionTotals as BR
import qualified BlueRipple.Data.PrefModel as BR
import qualified BlueRipple.Data.PrefModel.SimpleAgeSexEducation as BR
import qualified BlueRipple.Model.TurnoutAdjustment as BR
import qualified BlueRipple.Model.MRP_Pref as BR
import qualified BlueRipple.Model.PostStratify as BR
import qualified BlueRipple.Data.UsefulDataJoins as BR
import qualified MRP.CCES_MRP_Analysis as BR
import qualified BlueRipple.Utilities.KnitUtils as BR
import MRP.Common
import MRP.CCES
import MRP.DeltaVPV (DemVPV)

import qualified PreferenceModel.Common as PrefModel
import qualified BlueRipple.Data.Keyed as BR

import qualified Visualizations.StatePrefs as BR

text1 :: T.Text
text1 = [i|
In 2016, Trump won states that Clinton would likely have won if every demographic group voted in equal proportion. This reflects
a "turnout gap" between likely R voters and likely D voters. As we've talked about in earlier pieces [LINKS],
one simplistic but useful way to look at an election is to consider some
demographic breakdown of voters (e.g., Age, Sex and Race), estimate how many eligible voters are in each group,
how likely each is to vote, and who they are likely to vote for.  We can look at this nationwide or in a single congressional district:
any region where we can estimate the demographics, turnout and preferences.

In this piece we focus on turnout gaps in the battleground states.  We're not going to talk about why these gaps exist, though
we'll address this a bit at the end when we discuss actions you can take, but there's a great rundown at [FairVote][FV:Turnout].
In our [last post][BR:TurnoutHowHigh], we looked at how high turnout could go and the most efficient ways to boost turnout.
Here we take up a related question: what would changes in turnout do to the electoral landscape in the race for the White House? 

1. **What If Everybody Voted?**
2. **Battleground States**

## What If Everybody Voted? {#EverybodyVoted}
Last year, The Economist ran a fascinating data-journalism article entitled
["Would Donald Trump be president if all Americans actually voted?"][Economist:EveryoneVoted].  Using
a variety of sophisticated methods (exaplined in the linked piece) they attempted to simulate the
2016 presidential election under the assumption that every eligible voter cast a vote.  They concluded that
Clinton would have won the election.  Though the model is complex, the result comes down to understanding
the demographics in each state and the voter preferences of those groups.  The idea that everyone would vote in
a country where roughly 65% of eligible voters actually vote seems far-fetched.  So it's important to
realize that what the Economist modeled is the result we'd expect if everybody voted *in equal proportion*.
It doesn't matter if everyone votes, just that every demographic group is equally likely to vote in each state.

Here we're going to perform a similar analysis, but just focused on the battleground states.
Our model of voter preference is substantially simpler than the Economist.  They broke the
electorate into many groups by sex, age, education and race.  Here we are just going to look
at Age (45 or Over/Under 45), Sex (F/M), and Race (Non-White/White-Non-Hispanic).  That's more than
enough to see the central point: in many crucial states, the gaps in turnout between likely R and likely D
voters were enough to cost Clinton many battleground states.

Though we're focusing on 2016, we're not interested in re-hashing that election!  We want to know where
to focus our energies now.  Where can efforts to raise turnout make the most difference?

## Battleground States {#Battlegrounds}
Below we show a chart of the modeled Democratic preference of the electorate in each battleground state.
Our MR model [LINK] allows us to infer the preference of each demographic group in each state.  To
get an overall preference for the state, we have to weigh those groups.  That step is called
"post-stratification" (the "P" in "MRP").  But do we post-stratify by the number of *eligible voters*
or the number of *likely voters* in each group? Weighting by eligible voters corresponds to a
scenario where every demographic group votes in equal proportion.  Weighting by likely voters
corresponds to what happened, turnout-wise, in the 2016 election. 

In the chart below we do both weightings and compare.  This gives us a window into the effects of turnout
gaps in varioius states.  The effect of the gap in an individual state depends on turnout of course, but also
the demographics and preferences in that particular state.

There are a few things to note in the chart.  Some states have large differences between the equal-proportion
scenario and the likely-voter scenario.  For those the turnout gaps
are significant for electoral outcomes.  Is the difference sufficient to push the state to a preference
above 50%?  Then turnout gaps alone might make the difference between winning and losing the state.
In AZ, FL, GA, NC, NV, TX an VA, turnout gaps create differences of over 3% in final preference.
While driving a smaller gap in MI, PA and WI, those states were both very close in 2016 and those smaller
gaps made a big difference.


[BR:Home]: <https://blueripplepolitics.org>
[BR:TurnoutHowHigh]: <https://blueripplepolitics.org/blog/gotv-data>
[Economist:EveryoneVoted]: <https://medium.economist.com/would-donald-trump-be-president-if-all-americans-actually-voted-95c4f960798>
[FV:Turnout]: <https://www.fairvote.org/voter_turnout#voter_turnout_101>
|]

text2 :: T.Text
text2 = [i|
Let's return to GA and TX for a minute.  These are not traditional battleground states. TX
voted for Trump by almost a 10% margin in 2016. But changing
demographics---racial and educational---are making Texas more purple.  And the large shift
in preference we see from just the turnout gap suggests that TX is already more purple than
recent outcomes suggest.  This year, because Texas has a number
of retirements in the house, the local house races will be more competitive house and that
often drives higher and more balanced turnout. GA, also traditionally a red state,
was closer than TX in 2016 with Trump winning by about 5%. And GA, by our simple model, could
shift about 6pts bluer if there were no turnout gaps.

What we see so far is that turnout gaps are different in different states.  This
can already help you focus your efforts: if enfranchisement and turnout are something you care about,
this analysis suggests that putting your effort (or money) into groups working in AZ, FL, NV and PA, or
specific house or state legislative candidates in those same states, might be the best choice in terms of
affecting the outcome of the presidential race.
Devoting energy to closing turnout gaps in GA and TX is also likely to be worthwhile, helping
to win house and state legislative races and putting pressure on Republican efforts to hold the
state in the Presidential race (and, in the case of TX, the senate race).
Once the primaries are done, we will start focusing on the house races in those states to see
which might have useful overlap with turnout efforts.

Now we've identified some battleground states where turnout gaps are important to presidential vote outcomes.
But we haven't addressed how much we would need to boost turnout in order to flip these states. As our previous
piece discussed, boosting turnout a few % is possible but boosting it 20% isn't.  To put this more concretely,
let's consider two different plans for boosting turnout in each of these states.  In one, we focus on efforts
that increase turnout for everyone and in the other, we focus exclusively on Dem leaning voters. In either case,
is GOTV work alone enought to flip any of these states? Using the voter preferences and turnout
from the 2016 presidential election and the demographics from 2018, we compute the turnout boost, if possible,
required to flip each battleground state. The math is straightforward but messy so we've put the
details [here][BR:BoostNotes].

In the chart below, we summarize the results.

[BR:Home]: <https://blueripplepolitics.org>
[BR:TurnoutHowHigh]: <https://blueripplepolitics.org/blog/gotv-data>
[BR:BoostNotes]: <${brGithubUrl ((postRoute PostTurnoutGaps) <> "turnoutBoostExplainer")}#>
[Economist:EveryoneVoted]: <https://medium.economist.com/would-donald-trump-be-president-if-all-americans-actually-voted-95c4f960798>
[FV:Turnout]: <https://www.fairvote.org/voter_turnout#voter_turnout_101>
|]

text3 :: T.Text
text3 = [i|

One of our themes at [Blue Ripple Politics][BR:Home]
is that we believe that Democracy works better if everyone votes, and that we should prioritize laws and policies that
make voting easier for everyone, e.g., same-day-registration, vote-by-mail, making election day a federal holiday, 
having more polling places, etc.  



[BR:Home]: <https://blueripplepolitics.org>
[BR:TurnoutHowHigh]: <https://blueripplepolitics.org/blog/gotv-data>
[Economist:EveryoneVoted]: <https://medium.economist.com/would-donald-trump-be-president-if-all-americans-actually-voted-95c4f960798>
[FV:Turnout]: <https://www.fairvote.org/voter_turnout#voter_turnout_101>
|]

--type LocationCols = '[BR.StateAbbreviation]
type CatColsASER = '[BR.SimpleAgeC, BR.SexC, BR.CollegeGradC, BR.SimpleRaceC]
catKeyASER :: BR.SimpleAge -> BR.Sex -> BR.CollegeGrad -> BR.SimpleRace -> F.Record CatColsASER
catKeyASER a s e r = a F.&: s F.&: e F.&: r F.&: V.RNil

predMapASER :: F.Record CatColsASER -> M.Map CCESPredictor Double
predMapASER r = M.fromList [(P_Sex, if F.rgetField @BR.SexC r == BR.Female then 0 else 1)
                       ,(P_Race, if F.rgetField @BR.SimpleRaceC r == BR.NonWhite then 0 else 1)
                       ,(P_Education, if F.rgetField @BR.CollegeGradC r == BR.NonGrad then 0 else 1)
                       ,(P_Age, if F.rgetField @BR.SimpleAgeC r == BR.EqualOrOver then 0 else 1)
                       ]

allCatKeysASER = [catKeyASER a s e r | a <- [BR.EqualOrOver, BR.Under], e <- [BR.NonGrad, BR.Grad], s <- [BR.Female, BR.Male], r <- [BR.White, BR.NonWhite]]


type CatColsASE = '[BR.SimpleAgeC, BR.SexC, BR.CollegeGradC]
catKeyASE :: BR.SimpleAge -> BR.Sex -> BR.CollegeGrad -> F.Record CatColsASE
catKeyASE a s e = a F.&: s F.&: e F.&: V.RNil

predMapASE :: F.Record CatColsASE -> M.Map CCESPredictor Double
predMapASE r = M.fromList [(P_Sex, if F.rgetField @BR.SexC r == BR.Female then 0 else 1)         
                       ,(P_Education, if F.rgetField @BR.CollegeGradC r == BR.NonGrad then 0 else 1)
                       ,(P_Age, if F.rgetField @BR.SimpleAgeC r == BR.EqualOrOver then 0 else 1)
                       ]

allCatKeysASE = [catKeyASE a s e | a <- [BR.EqualOrOver, BR.Under], s <- [BR.Female, BR.Male], e <- [BR.NonGrad, BR.Grad]]

type CatColsASR = '[BR.SimpleAgeC, BR.SexC, BR.SimpleRaceC]
catKeyASR :: BR.SimpleAge -> BR.Sex -> BR.SimpleRace -> F.Record CatColsASR
catKeyASR a s r = a F.&: s F.&: r F.&: V.RNil

predMapASR :: F.Record CatColsASR -> M.Map CCESPredictor Double
predMapASR r = M.fromList [(P_Sex, if F.rgetField @BR.SexC r == BR.Female then 0 else 1)
                          ,(P_Race, if F.rgetField @BR.SimpleRaceC r == BR.NonWhite then 0 else 1)
                          ,(P_Age, if F.rgetField @BR.SimpleAgeC r == BR.EqualOrOver then 0 else 1)
                          ]

allCatKeysASR = [catKeyASR a s r | a <- [BR.EqualOrOver, BR.Under], s <- [BR.Female, BR.Male], r <- [BR.White, BR.NonWhite]]

catPredMap pmF acks = M.fromList $ fmap (\k -> (k, pmF k)) acks
catPredMapASER = catPredMap predMapASER allCatKeysASER
catPredMapASE = catPredMap predMapASE allCatKeysASE
catPredMapASR = catPredMap predMapASR allCatKeysASR

foldPrefAndTurnoutData :: FF.EndoFold (F.Record '[BR.ACSCount, BR.VotedPctOfAll, DemVPV, BR.DemPref])
foldPrefAndTurnoutData =  FF.sequenceRecFold
                          $ FF.toFoldRecord (FL.premap (F.rgetField @BR.ACSCount) FL.sum)
                          V.:& FF.toFoldRecord (BR.weightedSumRecF @BR.ACSCount @BR.VotedPctOfAll)
                          V.:& FF.toFoldRecord (BR.weightedSumRecF @BR.ACSCount @DemVPV)
                          V.:& FF.toFoldRecord (BR.weightedSumRecF @BR.ACSCount @BR.DemPref)
                          V.:& V.RNil

type BoostA = "boostA" F.:-> Double
type BoostB = "boostB" F.:-> Double
type BoostPop = "boostPop" F.:-> Int
type ToFlip = "ToFlip" F.:-> Double

foldPrefAndTurnoutDataBoost :: Double
                            -> FL.Fold (F.Record [BR.ACSCount, BR.VotedPctOfAll, DemVPV, BR.DemPref])
                            (F.Record [BR.ACSCount, BR.VotedPctOfAll, DemVPV, BR.DemPref, BoostA, BoostB, BoostPop])
foldPrefAndTurnoutDataBoost thresh =
  let t r = F.rgetField @BR.DemPref r > thresh
      dVotesF = FL.premap (\r -> realToFrac (F.rgetField @BR.ACSCount r) * F.rgetField @BR.DemPref r * F.rgetField @BR.VotedPctOfAll r) FL.sum
      votesF = FL.premap (\r -> realToFrac (F.rgetField @BR.ACSCount r) * F.rgetField @BR.VotedPctOfAll r) FL.sum 
      popF = FL.prefilter t $ FL.premap (F.rgetField @BR.ACSCount) FL.sum
      aNF = FL.prefilter t $ FL.premap (\r -> realToFrac (F.rgetField @BR.ACSCount r) * realToFrac (F.rgetField @BR.DemPref r)) FL.sum
      aDF = dVotesF
      aF = (/) <$> aNF <*> aDF
      bNF = popF
      bDF = votesF
      bF = (/) <$> fmap realToFrac bNF <*> bDF
      prefF = (/) <$> dVotesF <*> votesF
--      toFlipF = (\p a b -> let d = (0.5 - p)/p in d/(a - (1.0 + d)*b)) <$> prefF <*> aF <*> bF
    in FF.sequenceRecFold
       $ FF.toFoldRecord (FL.premap (F.rgetField @BR.ACSCount) FL.sum)
       V.:& FF.toFoldRecord (BR.weightedSumRecF @BR.ACSCount @BR.VotedPctOfAll)
       V.:& FF.toFoldRecord (BR.weightedSumRecF @BR.ACSCount @DemVPV)
       V.:& FF.toFoldRecord prefF
       V.:& FF.toFoldRecord aF
       V.:& FF.toFoldRecord bF
       V.:& FF.toFoldRecord bNF
       V.:& V.RNil                          

type PresPrefDem = "PresPrefDem" F.:-> Double

presByStateToDemPrefF :: FL.Fold (F.Record [ET.Party, ET.Votes]) (F.Record '[PresPrefDem])
presByStateToDemPrefF =
  let
    party = F.rgetField @ET.Party
    votes = F.rgetField @ET.Votes
    demVotesF = FL.prefilter (\r -> party r == ET.Democratic) $ FL.premap votes FL.sum
    demRepVotesF = FL.prefilter (\r -> let p = party r in (p == ET.Democratic || p == ET.Republican)) $ FL.premap votes FL.sum
    demPref d dr = if dr > 0 then realToFrac d/realToFrac dr else 0
    demPrefF = demPref <$> demVotesF <*> demRepVotesF
  in fmap (FT.recordSingleton @PresPrefDem) demPrefF


post :: forall es r.(K.KnitMany r
        , K.Members es r
        , K.Member GLM.RandomFu r
        )
     => Bool
     -> K.Cached es [BR.ASEDemographics]
     -> K.Cached es [BR.ASRDemographics]
     -> K.Cached es [BR.TurnoutASE]
     -> K.Cached es [BR.TurnoutASR]
     -> K.Cached es [BR.StateTurnout]
     -> K.Cached es [F.Record CCES_MRP]
     -> K.Sem r ()
post updated aseDemoCA asrDemoCA aseTurnoutCA asrTurnoutCA stateTurnoutCA ccesRecordListAllCA = P.mapError BR.glmErrorToPandocError $ K.wrapPrefix "TurnoutScenarios" $ do
  let --states = ["AZ", "FL", "GA", "ME", "NC", "OH", "MI", "WI", "PA", "CO", "NH", "NV", "TX", "VA"]
    states = ["AZ", "FL", "GA", "NC", "OH", "MI", "WI", "PA", "TX"]
    statesOnly = F.filterFrame (\r -> F.rgetField @BR.StateAbbreviation r `L.elem` states)
    stateAndNation = F.filterFrame (\r -> F.rgetField @BR.StateAbbreviation r `L.elem` "National" : states)
  aseACSRaw <- P.raise $ K.useCached aseDemoCA
  asrACSRaw <- P.raise $ K.useCached asrDemoCA
  aseTurnoutRaw <- P.raise $ K.useCached aseTurnoutCA
  asrTurnoutRaw <- P.raise $ K.useCached asrTurnoutCA
  stateTurnoutRaw <- P.raise $ K.useCached stateTurnoutCA
  aseACS <- K.retrieveOrMakeTransformed (fmap FS.toS . FL.fold FL.list) (F.toFrame . fmap FS.fromS) "mrp/acs_simpleASE.bin"
            $ K.logLE K.Diagnostic "re-keying aseACS" >> (K.knitEither $ FL.foldM BR.simplifyACS_ASEFold aseACSRaw)

  asrACS <- K.retrieveOrMakeTransformed (fmap FS.toS . FL.fold FL.list) (F.toFrame . fmap FS.fromS) "mrp/acs_simpleASR.bin"
            $   K.logLE K.Diagnostic "re-keying asrACS" >> (K.knitEither $ FL.foldM BR.simplifyACS_ASRFold asrACSRaw)

  let acsASRByStateF = FMR.concatFold $ FMR.mapReduceFold
                       FMR.noUnpack
                       (FMR.assignKeysAndData @([BR.Year, BR.StateAbbreviation] V.++ CatColsASR) @'[BR.ACSCount])
                       (FMR.foldAndAddKey $ FF.foldAllConstrained @Num FL.sum)
  
  aseTurnout <- K.retrieveOrMakeTransformed (fmap FS.toS . FL.fold FL.list) (F.toFrame . fmap FS.fromS) "mrp/turnout_simpleASE.bin"
                $   K.logLE K.Diagnostic "re-keying aseTurnout" >> (K.knitEither $ FL.foldM BR.simplifyTurnoutASEFold aseTurnoutRaw)

  asrTurnout <- K.retrieveOrMakeTransformed (fmap FS.toS . FL.fold FL.list) (F.toFrame . fmap FS.fromS) "mrp/turnout_simpleASR.bin"
                $   K.logLE K.Diagnostic "re-keying asrTurnout" >> (K.knitEither $ FL.foldM BR.simplifyTurnoutASRFold asrTurnoutRaw)
  
  let showRecs = T.intercalate "\n" . fmap (T.pack . show) . FL.fold FL.list
  let predictorsASER = [GLM.Intercept, GLM.Predictor P_Sex , GLM.Predictor P_Age, GLM.Predictor P_Education, GLM.Predictor P_Race]
      predictorsASE = [GLM.Intercept, GLM.Predictor P_Sex , GLM.Predictor P_Age, GLM.Predictor P_Education]
      predictorsASR = [GLM.Intercept, GLM.Predictor P_Sex , GLM.Predictor P_Age, GLM.Predictor P_Race]
  inferredPrefsASER <-  stateAndNation <$> K.retrieveOrMakeTransformed (fmap FS.toS . FL.fold FL.list) (F.toFrame . fmap FS.fromS) "mrp/simpleASER_MR.bin"
                        (P.raise $ BR.mrpPrefs @CatColsASER ccesRecordListAllCA predictorsASER catPredMapASER) 
  inferredPrefsASE <-  stateAndNation <$> K.retrieveOrMakeTransformed (fmap FS.toS . FL.fold FL.list) (F.toFrame . fmap FS.fromS) "mrp/simpleASE_MR.bin"
                       (P.raise $ BR.mrpPrefs @CatColsASE ccesRecordListAllCA predictorsASE catPredMapASE) 
  inferredPrefsASR <-  stateAndNation <$> K.retrieveOrMakeTransformed (fmap FS.toS . FL.fold FL.list) (F.toFrame . fmap FS.fromS) "mrp/simpleASR_MR.bin"
                       (P.raise $ BR.mrpPrefs @CatColsASR ccesRecordListAllCA predictorsASR catPredMapASR) 

  -- get adjusted turnouts (national rates, adj by state) for each CD
  demographicsAndTurnoutASE <- statesOnly <$> BR.aseDemographicsWithAdjTurnoutByCD (K.asCached aseACS) (K.asCached aseTurnout) (K.asCached stateTurnoutRaw)
  demographicsAndTurnoutASR <- statesOnly <$> BR.asrDemographicsWithAdjTurnoutByCD (K.asCached asrACS) (K.asCached asrTurnout) (K.asCached stateTurnoutRaw)
  -- join with the prefs
  K.logLE K.Info "Computing pres-election based prefs"
  presPrefByStateFrame <- do
    let fld = FMR.concatFold $ FMR.mapReduceFold
              MR.noUnpack
              (FMR.assignKeysAndData @[BR.Year, BR.State, BR.StateAbbreviation, BR.StateFIPS, ET.Office])
              (FMR.foldAndAddKey presByStateToDemPrefF)
    presByStateFrame <- BR.presidentialByStateFrame
    return $ FL.fold fld
      presByStateFrame
  K.logLE K.Info "Joining turnout by CD and prefs"
  let aseTurnoutAndPrefs = catMaybes
                           $ fmap F.recMaybe
                           $ F.leftJoin @([BR.StateAbbreviation, BR.Year] V.++ CatColsASE) demographicsAndTurnoutASE inferredPrefsASE
      asrTurnoutAndPrefs = catMaybes
                           $ fmap F.recMaybe
                           $ F.leftJoin @([BR.StateAbbreviation, BR.Year] V.++ CatColsASR) demographicsAndTurnoutASR inferredPrefsASR

      -- fold these to state level
  let justPres2016 r = (F.rgetField @BR.Year r == 2016) && (F.rgetField @ET.Office r == ET.President)    
      aseDemoF = FMR.concatFold $ FMR.mapReduceFold
                 (FMR.unpackFilterRow justPres2016)
                 (FMR.assignKeysAndData @(CatColsASE V.++ '[BR.StateAbbreviation, BR.Year, ET.Office]) @[BR.ACSCount, BR.VotedPctOfAll, DemVPV, BR.DemPref])
                 (FMR.foldAndAddKey foldPrefAndTurnoutData)
      asrDemoF = FMR.concatFold $ FMR.mapReduceFold
                 (FMR.unpackFilterRow justPres2016)
                 (FMR.assignKeysAndData @(CatColsASR V.++ '[BR.StateAbbreviation, BR.Year, ET.Office]) @[BR.ACSCount, BR.VotedPctOfAll, DemVPV, BR.DemPref])
                 (FMR.foldAndAddKey foldPrefAndTurnoutData)              
      asrByState = FL.fold asrDemoF asrTurnoutAndPrefs
      aseByState = FL.fold aseDemoF aseTurnoutAndPrefs
      labelPSBy x = V.rappend (FT.recordSingleton @BR.PostStratifiedBy x)
      psCellVPVByBothF =  (<>)
                          <$> fmap pure (fmap (labelPSBy BR.VAP)
                                         $ BR.postStratifyCell @BR.DemPref
                                         (realToFrac . F.rgetField @BR.ACSCount)
                                         (realToFrac . F.rgetField @BR.DemPref))
                          <*> fmap pure (fmap (labelPSBy BR.Voted)
                                         $ BR.postStratifyCell @BR.DemPref
                                         (\r -> realToFrac (F.rgetField @BR.ACSCount r) * F.rgetField @BR.VotedPctOfAll r)
                                         (realToFrac . F.rgetField @BR.DemPref))
      psVPVByDistrictF =  BR.postStratifyF
                          @[BR.Year, ET.Office, BR.StateAbbreviation]
                          @[BR.DemPref, BR.ACSCount, BR.VotedPctOfAll]
                          @[BR.PostStratifiedBy, BR.DemPref]
                          psCellVPVByBothF
      vpvPostStratifiedByASE = fmap (`V.rappend` FT.recordSingleton @BR.DemographicGroupingC BR.ASE) $ FL.fold psVPVByDistrictF aseByState
      vpvPostStratifiedByASR =  fmap (`V.rappend` FT.recordSingleton @BR.DemographicGroupingC BR.ASR) $ FL.fold psVPVByDistrictF asrByState
      vpvPostStratified = vpvPostStratifiedByASE <> vpvPostStratifiedByASR
      -- assemble set with updated demographics
      toFlip r =
        let a = F.rgetField @BoostA r
            b = F.rgetField @BoostB r
            p = F.rgetField @PresPrefDem r
            d = (0.5 - p)/p
        in FT.recordSingleton @ToFlip $ d/(a - (1.0 + d) * b)

      toFlipASR x =
        let filter y r = F.rgetField @BR.Year r == y && F.rgetField @ET.Office r == ET.President
            asrPrefTurnout2016 = fmap (F.rcast @('[BR.StateAbbreviation] V.++ CatColsASR V.++ [BR.VotedPctOfAll, DemVPV, BR.DemPref]))
                                 $ F.filterFrame (filter 2016) asrByState
            asrDemo2018 = fmap (F.rcast @('[BR.StateAbbreviation] V.++ CatColsASR V.++ '[BR.ACSCount]))
                          $ FL.fold acsASRByStateF $ statesOnly $ F.filterFrame (\r -> F.rgetField @BR.Year r == 2018) asrACS
            asrUpdatedDemo = catMaybes
                             $ fmap F.recMaybe
                             $ F.leftJoin @('[BR.StateAbbreviation] V.++ CatColsASR) asrPrefTurnout2016 asrDemo2018
            asrWithBoosts = FL.fold
                            (FMR.concatFold $ FMR.mapReduceFold
                              MR.noUnpack
                              (FMR.assignKeysAndData @'[BR.StateAbbreviation])
                              (FMR.foldAndAddKey $ foldPrefAndTurnoutDataBoost x)
                            )
                            asrUpdatedDemo
                            
            presPref2016 = fmap (F.rcast @[BR.StateAbbreviation, PresPrefDem]) $ F.filterFrame (filter 2016) presPrefByStateFrame
            asrUpdated =  catMaybes
                                $ fmap F.recMaybe
                                $ F.leftJoin @'[BR.StateAbbreviation] asrWithBoosts
                                $ presPref2016
        in fmap (FT.mutate toFlip) asrUpdated
      toFlipAll = F.toFrame $ fmap (FT.retypeColumn @ToFlip @'("ToFlipAll",Double)
                                    . FT.retypeColumn @BoostPop @'("BoostPopAll",Int)
                                    . F.rcast @[BR.StateAbbreviation, BR.ACSCount, BR.VotedPctOfAll, PresPrefDem, BoostPop, ToFlip]) $  toFlipASR 0.0
      toFlipDems = F.toFrame $ fmap  (FT.retypeColumn @ToFlip @'("ToFlipDems",Double)
                                      . FT.retypeColumn @BoostPop @'("BoostPopDems",Int)
                                      . F.rcast @[BR.StateAbbreviation, BoostPop, ToFlip]) $  toFlipASR 0.5
      toFlipJoined =   F.toFrame
                       $ catMaybes
                       $ fmap F.recMaybe
                       $ F.leftJoin @'[BR.StateAbbreviation] toFlipAll toFlipDems
  evFrame <- fmap (F.rcast @[BR.StateAbbreviation, BR.Electors]) . F.filterFrame (\r -> F.rgetField @BR.Year r == 2020) <$> BR.electoralCollegeFrame
  let toFlipWithEV =  catMaybes
                      $ fmap F.recMaybe
                      $ F.leftJoin @'[BR.StateAbbreviation] toFlipJoined evFrame
  logFrame toFlipWithEV
  let boostColonnade cas =
        let toFlipD = F.rgetField @'("ToFlipDems",Double)
            toFlipAll = F.rgetField @'("ToFlipAll",Double)
            evs = F.rgetField @BR.Electors
            pop  = F.rgetField @BR.ACSCount 
            bPop = F.rgetField @'("BoostPopDems",Int)
        in C.headed "State" (BR.toCell cas "State" "State" (BR.textToStyledHtml . F.rgetField @BR.StateAbbreviation))
           <> C.headed "Electoral Votes" (BR.toCell cas "Electoral Votes" "EVs" (BR.numberToStyledHtml "%d". evs))
           <> C.headed "Population (000s)" (BR.toCell cas "Population" "Pop" (BR.numberToStyledHtml "%d". (\r -> (round (realToFrac (pop r)/1000)) :: Int)))
           <> C.headed "Dem Leaners (000s)" (BR.toCell cas "Leaners" "Leaners" (BR.numberToStyledHtml "%d". (\r -> (round (realToFrac (bPop r)/1000)) :: Int)))
           <> C.headed "Boosting All Requires (%)" (BR.toCell cas "BoostAll" "% Of All" (BR.numberToStyledHtml "%2.2f" . (*100) . toFlipAll))
           <> C.headed "Boosting Dems Requires (%)" (BR.toCell cas "BoostDem" "% Of Dems" (BR.numberToStyledHtml "%2.2f" . (*100) . toFlipD))
           <> C.headed "New Voters/Ev" (BR.toCell cas "VotersPerEv" "Voters/EV" (BR.numberToStyledHtml "%d" . (\r -> (round (realToFrac (bPop r) * toFlipD r / realToFrac (evs r))) :: Int)))
        
  
  
{-
      postStratifiedWithBoostByASR = FL.fold
                                     (FMR.concatFold $ FMR.mapReduceFold
                                      MR.noUnpack
                                      (FMR.assignKeysAndData @[BR.Year, ET.Office, BR.StateAbbreviation])
                                      (FMR.foldAndAddKey $ foldPrefAndTurnoutDataBoost 0.5)
                                     )
                                  asrByState
      asrByStateWithPresPref =  catMaybes
                                $ fmap F.recMaybe
                                $ F.leftJoin @[BR.StateAbbreviation, BR.Year] postStratifiedWithBoostByASR
                                $ fmap (F.rcast @[BR.Year, BR.StateAbbreviation, PresPrefDem]) presPrefByStateFrame
      asrBoosts = fmap (FT.mutate toFlip) asrByStateWithPresPref          
  logFrame asrBoosts
-}
  curDate <-  (\(Time.UTCTime d _) -> d) <$> K.getCurrentTime
  let pubDateTurnoutGaps =  Time.fromGregorian 2020 2 15
  K.newPandoc
    (K.PandocInfo ((postRoute PostTurnoutGaps) <> "main")
      (brAddDates updated pubDateTurnoutGaps curDate
       $ M.fromList [("pagetitle", "What If Everybody Voted, Redux")
                    ,("title","What If Everybody Voted, Redux")
                    ]
      ))
      $ do        
        brAddMarkDown text1
        _ <-  K.addHvega Nothing (Just $ "\"Voted\" is Actual Turnout and \"VAP\" (Voting Age Population) is equal proportion.")
          $ vlTurnoutGap
          "Battleground Preference Post-Stratified by Age, Sex and Race"
          (FV.ViewConfig 800 800 10)
          $ fmap F.rcast vpvPostStratifiedByASR
        brAddMarkDown text2
--        K.addColonnadeTextTable boostColonnade toFlipWithEV
        BR.brAddRawHtmlTable "Turnout Boosts to Flip Battlegrounds" (BHA.class_ "brTable") (boostColonnade mempty) toFlipWithEV
        brAddMarkDown text3
        brAddMarkDown brReadMore
     
  K.newPandoc
    (K.PandocInfo ((postRoute PostTurnoutGaps) <> "turnoutBoostExplainer")
      (brAddDates updated pubDateTurnoutGaps curDate
       $ M.fromList [("pagetitle", "Turnout Boost Details")
                    ,("title","Turnout Boost Details")
                    ]
      ))
    $ do
    brAddMarkDown turnoutBoostExplainerMD
         
vlTurnoutGap :: Foldable f
             => T.Text -- title
             -> FV.ViewConfig
             -> f (F.Record [BR.StateAbbreviation, ET.Office, BR.Year, BR.DemographicGroupingC, BR.PostStratifiedBy, BR.DemPref])
             -> GV.VegaLite
vlTurnoutGap title vc rows =
  let dat = FV.recordsToVLData id FV.defaultParse rows
--      makeYVal = GV.calculateAs "datum.state_abbreviation + '-' + datum.year + '/' + datum.ET.Office + ' (' + datum.DemographicGrouping + ')'" "State/Race"
      makeYVal = GV.calculateAs "datum.state_abbreviation + '-' + datum.year + '/' + datum.Office" "State/Race" 
      encX = GV.position GV.X [FV.pName @BR.DemPref, GV.PmType GV.Quantitative, GV.PScale [GV.SDomain $ GV.DNumbers [0.40, 0.60]]]
      encY = GV.position GV.Y [GV.PName "State/Race", GV.PmType GV.Nominal]
      encColor = GV.color [FV.mName @BR.PostStratifiedBy, GV.MmType GV.Nominal]
      encDetail = GV.detail [GV.DName "State/Race", GV.DmType GV.Nominal]
      encoding = GV.encoding . encDetail . encX . encY
      transform = GV.transform . makeYVal
      lineSpec = GV.asSpec [(GV.encoding . encDetail . encX . encY) [], transform [], GV.mark GV.Line []]
      dotSpec = GV.asSpec [(GV.encoding . encX . encY . encColor) [], transform [], GV.mark GV.Point []]
  in
    FV.configuredVegaLite vc [FV.title title, GV.layer [lineSpec, dotSpec], dat]


turnoutBoostExplainerMD :: T.Text
turnoutBoostExplainerMD = [here|
Let's label our groups by $g$, with turnout $T_g$, population $N_g$ and
Dem preference $P_g$. If the votes cast for dems are $V_D$ out of the total votes $V$,
then post-stratified preference is

$\begin{equation}
P = \frac{V_D}{V}=\frac{\sum_g T_g N_g P_g}{\sum_g T_g N_g}
\end{equation}$

splitting into dem leaners (denoted by $g\in D$) and others ($g \in R$):

$\begin{equation}
P = \frac{\sum_{g\in D} T_g N_g P_g + \sum_{g \in R} T_g N_g P_g}{\sum_{g \in D} T_g N_g + \sum_{g \in R} T_g N_g}
\end{equation}$

Suppose we boost turnout in Dem leaning groups by x (so for a 1% boost, x would be 0.01):

$\begin{equation}
P(x) = \frac{\sum_{g\in D} (T_g + x) N_g P_g + \sum_{g \in R} T_g N_g P_g}{\sum_{g \in D} (T_g + x) N_g + \sum_{g \in R} T_g N_g}
=\frac{x\sum_{g \in D}N_g P_g + V_D}{x\sum_{g \in D} N_g + V}
=\frac{V_D}{V}\frac{1 + x\sum_{g \in D}N_g P_g/V_D}{1 + x\sum_{g \in D} N_g/V} = P \frac{1 + x\sum_{g \in D}N_g P_g/V_D}{1 + x\sum_{g \in D} N_g/V}
\end{equation}$

Usually, we're curious about what $x$ we need for a certain $P(x)$.  For example, $P(x)=0.5$ is the level required to "flip" a state.  So let's
call the $P(x)$ we're hoping for $P_h$ and write $P_h =P\times(1 + \delta)$ or $\delta = \frac{P_h - P}{P}$.
Also, just to simplify things, we'll define $a = \sum_{g \in D}N_g P_g/V_D$ and $b =  \sum_{g \in D} N_g/V$.  So we have

$\begin{equation}
P\times(1 + \delta) = P \frac {1 + ax}{1 + bx}
\end{equation}$

which we can solve for $x$:

$\begin{equation}
x = \frac{\delta}{a - (1+\delta)b}
\end{equation}$

We can understand this formula a bit. We need a boost that is proportional to $\delta$, the gap we need to make up.
Making up ground is easier when Dem preference is high in the groups we are boosting ($a - b$, more or less).

Let's look at a simple example. Imagine 800 voters in group A with 50% voter turnout and leaning 75/25 toward Dems
and 1000 voters in group B with 60% voter turnout and leaning 65/35 toward Republicans.
A Democrat would get 280 votes from the Dem leaners and 210 votes from
the R leaners, for a total of 490.
The Republican would get 120 votes from the Dem leaners and 390 from the R leaners, for a total of 510. So
$P_0=\frac{V_D}{V} = \frac{490}{1000} = 0.49$

How much do we need to boost turnout in group A to flip the state, that is to get P_h = 0.5?
Plugging the numbers above into the equation for $P(x)$ ($\delta = 0.02$; $a = 560/510 = 1.1$; $b = 800/1000 = 0.8$),
we get $x = 0.11 = 11\%$. That is, we'd need to boost turnout in group A by 11%, from 50% to 61%.
|]


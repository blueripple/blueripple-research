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
import           BlueRipple.Utilities.TableUtils 
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


import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Data.DemographicTypes as BR
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
"post-stratification" (the "P" in "MRP").  But do we post-stratify by the number of eligible voters
in each group or the number of likely voters in each group?  Below we do both: post-stratifying by
the number of eligible voters (Voting-Age-Population or "VAP") reflects the "everybody votes in equal
propotion" scenario while post-stratifying by likely voters ("Voted") reflects something more like what actually
happened.  The result is two preferences for each state.

There are a few things worth looking for in the chart.  Is the difference large?  Then turnout gaps
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
voted for Trump by almost 10 pts in 2016. But changing
demographics---racial and educational---are making Texas more purple.  And the large shift
in preference we see from just the turnout gap suggests that TX is already more purple than
recent outcomes suggest.  This is particularly interesting this year because Texas has a number
of retirements in the house, making a number of more competitive house elections and those
can drive higher and more balanced turnout. This is consistent with the Economist piece,
which shows TX going blue under the everybody votes scenario.

GA, also traditionally a red state,  was closer than TX in 2016.
Trump won by 5 pts. And GA, by our simple model, could
shift about 6pts bluer if there were no turnout gaps.  Like TX, the
Economist model has GA going blue in an everybody votes in equal propotion
scenario.


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

post :: forall es r.(K.KnitOne r
        , K.Members es r
        , K.Member GLM.RandomFu r
        )
     => K.Cached es [BR.ASEDemographics]
     -> K.Cached es [BR.ASRDemographics]
     -> K.Cached es [BR.TurnoutASE]
     -> K.Cached es [BR.TurnoutASR]
     -> K.Cached es [BR.StateTurnout]
     -> K.Cached es [F.Record CCES_MRP]
     -> K.Sem r ()
post aseDemoCA asrDemoCA aseTurnoutCA asrTurnoutCA stateTurnoutCA ccesRecordListAllCA = P.mapError BR.glmErrorToPandocError $ K.wrapPrefix "TurnoutScenarios" $ do
  let states = ["AZ", "FL", "GA", "IA", "NC", "OH", "MI", "WI", "PA", "CO", "NH", "NV", "TX", "VA"]
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
  K.logLE K.Info "Joining turnout by CD and prefs"
  let aseTurnoutAndPrefs = catMaybes
                           $ fmap F.recMaybe
                           $ F.leftJoin @([BR.StateAbbreviation, BR.Year] V.++ CatColsASE) demographicsAndTurnoutASE inferredPrefsASE
      asrTurnoutAndPrefs = catMaybes
                           $ fmap F.recMaybe
                           $ F.leftJoin @([BR.StateAbbreviation, BR.Year] V.++ CatColsASR) demographicsAndTurnoutASR inferredPrefsASR

      -- fold these to state level
  let justPres2016 r = (F.rgetField @BR.Year r == 2016) && (F.rgetField @Office r == President)    
      aseDemoF = FMR.concatFold $ FMR.mapReduceFold
                 (FMR.unpackFilterRow justPres2016)
                 (FMR.assignKeysAndData @(CatColsASE V.++ '[BR.StateAbbreviation, BR.Year, Office]) @[BR.ACSCount, BR.VotedPctOfAll, DemVPV, BR.DemPref])
                 (FMR.foldAndAddKey foldPrefAndTurnoutData)
      asrDemoF = FMR.concatFold $ FMR.mapReduceFold
                 (FMR.unpackFilterRow justPres2016)
                 (FMR.assignKeysAndData @(CatColsASR V.++ '[BR.StateAbbreviation, BR.Year, Office]) @[BR.ACSCount, BR.VotedPctOfAll, DemVPV, BR.DemPref])
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
                          @[BR.Year, Office, BR.StateAbbreviation]
                          @[BR.DemPref, BR.ACSCount, BR.VotedPctOfAll]
                          @[BR.PostStratifiedBy, BR.DemPref]
                          psCellVPVByBothF
      vpvPostStratifiedByASE = fmap (`V.rappend` FT.recordSingleton @BR.DemographicGroupingC BR.ASE) $ FL.fold psVPVByDistrictF aseByState
      vpvPostStratifiedByASR =  fmap (`V.rappend` FT.recordSingleton @BR.DemographicGroupingC BR.ASR) $ FL.fold psVPVByDistrictF asrByState
      vpvPostStratified = vpvPostStratifiedByASE <> vpvPostStratifiedByASR
{-      
      vpvPostStratifiedVAPByASE = FL.fold
                                  (BR.aggFoldRec
                                   (BR.aggFReduceRec @Bool @[BR.Year, Office, BR.StateAbbreviation] @(CatColsASE V.++ '[BR.CongressionalDistrict]))
                                   (BR.dataFoldCollapseBool foldPrefAndTurnoutData)                                
                                   (FL.fold (K.dimap F.rcast S.toList FL.set) aseTurnoutAndPrefs)
                                  )
                                  $ fmap F.rcast aseTurnoutAndPrefs
  logFrame  vpvPostStratifiedVAPByASE
-}
  brAddMarkDown text1
  _ <-  K.addHvega Nothing (Just $ "\"Voted\" is Actual Turnout and \"VAP\" (Voting Age Population) is equal proportion.")
        $ vlTurnoutGap
        "Battleground Preference Post-Stratified by Age, Sex and Race"
        (FV.ViewConfig 800 800 10)
        $ fmap F.rcast vpvPostStratifiedByASR
  brAddMarkDown text2
{-  _ <-  K.addHvega Nothing Nothing
        $ vlTurnoutGap
        "Battleground Preference Post-Stratified by Age, Sex and Race (Voted=Actual Turnout, VAP=Equal Proportion)"
        (FV.ViewConfig 800 800 10)
        $ fmap F.rcast vpvPostStratifiedByASR
-}
--  logFrame vpvPostStratifiedByASE
  brAddMarkDown brReadMore

vlTurnoutGap :: Foldable f
             => T.Text -- title
             -> FV.ViewConfig
             -> f (F.Record [BR.StateAbbreviation, Office, BR.Year, BR.DemographicGroupingC, BR.PostStratifiedBy, BR.DemPref])
             -> GV.VegaLite
vlTurnoutGap title vc rows =
  let dat = FV.recordsToVLData id FV.defaultParse rows
--      makeYVal = GV.calculateAs "datum.state_abbreviation + '-' + datum.year + '/' + datum.Office + ' (' + datum.DemographicGrouping + ')'" "State/Race"
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

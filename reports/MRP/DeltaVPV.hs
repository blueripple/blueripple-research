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

module MRP.DeltaVPV where

import qualified Control.Foldl                 as FL
import qualified Data.List as L
import qualified Data.Map                      as M
import           Data.Maybe (catMaybes)

import qualified Data.Text                     as T

import           Graphics.Vega.VegaLite.Configuration as FV
import qualified Graphics.Vega.VegaLite.Compat as FV
import qualified Frames as F
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V


import qualified Control.MapReduce             as MR
import qualified Frames.Transform              as FT
import qualified Frames.Folds                  as FF
import qualified Frames.MapReduce              as FMR
import qualified Frames.Serialize              as FS

import qualified Frames.Visualization.VegaLite.Data
                                               as FV

import qualified Graphics.Vega.VegaLite        as GV
import qualified Knit.Report                   as K
import qualified Polysemy.Error                as P (mapError)
import qualified Polysemy                      as P (raise)
import           Text.Pandoc.Error             as PE
import qualified Text.Blaze.Colonnade          as BC

import           Data.String.Here               ( i )

import qualified Colonnade                     as C

import qualified Text.Blaze.Html               as BH
import qualified Text.Blaze.Html5.Attributes   as BHA

import           BlueRipple.Configuration 
import           BlueRipple.Utilities.KnitUtils 
import           BlueRipple.Utilities.TableUtils 

import qualified Numeric.GLM.ProblemTypes      as GLM
import qualified Numeric.GLM.ModelTypes      as GLM
import qualified Numeric.GLM.Bootstrap            as GLM
import qualified Numeric.GLM.MixedModel            as GLM

import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Data.Loaders as BR
import qualified BlueRipple.Data.DemographicTypes as BR
import qualified BlueRipple.Data.ElectionTypes as ET

import qualified BlueRipple.Data.PrefModel as BR
import qualified BlueRipple.Data.PrefModel.SimpleAgeSexEducation as BR
import qualified BlueRipple.Model.TurnoutAdjustment as BR
import qualified BlueRipple.Model.MRP as BR
import qualified BlueRipple.Model.PostStratify as BR


import qualified BlueRipple.Utilities.KnitUtils as BR
import MRP.Common
import MRP.CCES
import qualified MRP.CCES_MRP_Analysis as BR
import qualified PreferenceModel.Common as PrefModel
import qualified Frames.Melt as F
import qualified BlueRipple.Data.ACS_PUMS as PUMS
import qualified BlueRipple.Data.CPSVoterPUMS as CPS
import qualified BlueRipple.Data.UsefulDataJoins as BR
import qualified Frames.SimpleJoins as FJ

brText1 :: T.Text
brText1 = [i|
In our [last research post][BR:Pools] we looked at college-educated-voter preference, measured
by "Votes Per Voter" ([VPV][BR:Pools:VPV]). VPV captures the value of boosting turnout
among some group of voters. In that post, we looked exclusively at the 2016 Presidential
election. But is the 2016 Presidential election the right baseline for the 2020 Presidential
election?  Or should we think of the 2018 House elections as a baseline?  Mid-terms are different
from Presidential years and House elections are different from Presidential elections.
Let's dive into those differences, looking first at
2016 Presidential vs. House races,  and then at the 2016 vs. 2018 House races. 

In the last post we were struck by how different
some of the VPV numbers were in different states.
We're going to hone in on that here as well,
looking at our data on [choropleth-maps][Wikipedia:choropleth]
so we can see the geographic variation clearly.

1. **Review: MRP and VPV**
2. **2016: Presidential Race vs. House Races**
3. **2018 House Races vs. 2016 House Races**
4. **Key Takeaways**
5. **Take Action**

NB: As part of routine data maintenance,
we've updated this post using newer and better data sources.  The numbers are quite similar
and all the text remains the same.

##MRP and VPV
To recap:  we have data from the [CCES][CCES] survey which gives us information about
individual voters, including their location and reported votes for President in 2016 and the House
in 2016 and 2018.  We use a technique called Multi-level Regression (MR) to combine the local
data (e.g., college-educated female voters over 45 in a particular state) in a sensible way
with the data for the same group in all the states, and estimate Democratic voter preference
(D votes/(D votes + R votes) among voters who voted D or R)
of each group in each state.  From these numbers we compute VPV, which makes the value of
turnout among these voters a bit clearer than voter preference.

To combine group estimates into an estimate of VPV for a state,
we "Post-stratify" (the "P" in MRP) them, which means weighting the group VPV estimates
by the number of people in each group.  For the post-stratified charts below, we will
be post-stratifying by estimated numbers of voters, obtained from
census estimates of voting age population in each group and state
and national voter turnout rates in each group. We adjust the turnout rates in
each state so that the total number of number of votes cast in the
state---a number we get from the great work of the
[United States Election Project][USEP:Turnout]---
is the product of the turnout rates and the population from the census in
that district.  See [here][BR:Methods:Turnout] for more details.

##2016: Presidential Race vs. House Races
Was Clinton more or less popular than other Democratic candidates running for office in 2016?
That turns out to depend on which voters you look at and where they live.
In the charts below we look
at the *change* in Democratic VPV from 2016 Presidential voting to 2016 House voting, a measure
of the difference in popularity of Clinton and local House candidates. We
estimate VPV for Presidential and House votes separately, keeping the same groups as in
our last post: Non-College-Grad/College-Grad, Female/Male, and Over 45/Under 45. In each chart,
a blue color indicates a place where Clinton was *more* popular than the Democrats running
for the local House seats and a red color indicates the opposite. Since we are considering
only voters who voted D or R, anyplace Clinton out-performed local House candidates is also
a place where Trump under-performed local House candidates. From this data there is no way to
know what is underlying the differences between Presidential and House voting.

It's important to remember that these charts are showing *change* in VPV.  Red doesn't
mean that the state voted Republican for President or House in 2016, just that it was
*more* Republican in its Presidential voting than House voting.

We plot all eight groups below, first college graduates and then non-graduates.
Among college-educated young female voters, and to a lesser extent,
college-educated young male voters,
Clinton was clearly more popular than the local Democrat. Older college-educated voters
are a mix, though with wide variation among states.
Among older non-college-educated voters, though, Clinton is *less* popular than the
local Democrat, and younger non-college-educated voters are a widely varying mix.

[CCES]: <https://cces.gov.harvard.edu/>
[USEP:Turnout]: <http://www.electproject.org/home/voter-turnout/voter-turnout-data>
[MRP:Summary]: <https://en.wikipedia.org/wiki/Multilevel_regression_with_poststratification>
[MRP:Methods]: <${brGithubUrl (postPath PostMethods)}>
[BR:Pools]: <${brGithubUrl (postPath PostPools)}>
[BR:Pools:VPV]: <${brGithubUrl (postPath PostPools)}/#VPV>
[PrefModel:WWCV]: <${brGithubUrl (PrefModel.postPath PrefModel.PostWWCV)}>
[PrefModel:AcrossTime]: <${brGithubUrl (PrefModel.postPath PrefModel.PostAcrossTime)}>
[BR:PM2018]: <https://blueripple.github.io/research/preference-model/p1/main.html>
[BR:PMAcrossTIme]: <https://blueripple.github.io/research/preference-model/p2/main.html>
[BR:AboutUs]: <https://blueripplepolitics.org/about-us>
[BR:Methods:Turnout]: <https://blueripple.github.io/research/preference-model/methods/main.html#turnout>
[Bitecofer:Bio]: <http://cnu.edu/people/rachelbitecofer/>
[Bitecofer:2018House]: <https://cnu.edu/wasoncenter/2018/09/26-signs-democrats-win-big/>
[Bitecofer:2020Pres]: <https://cnu.edu/wasoncenter/2019/07/01-2020-election-forecast/>
[NPR:CollegeWomen]: <https://www.npr.org/2018/09/24/650447848/the-womens-wave-backlash-to-trump-persists-reshaping-politics-in-2018>
[Wikipedia:choropleth]: <https://en.wikipedia.org/wiki/Choropleth_map>
|]

brText2 :: T.Text
brText2 = [i|
Putting these all together, via post-stratification in each state,
gives the map below. Of particular note:

- Clinton under-performed local candidates significantly in the battleground state
of WI, but outperformed significantly in PA and a smaller amount in MI, OH and FL.
- Clinton outperformed local candidates in TX and GA.
This is interesting since Clinton *outperforming* the local Democrat looks the same
(in this data) as Trump *under-performing* the local Republican.
Trump under-performance in ever-less-Republican
TX---where there has been a wave of [Republican retirements][TX:RR]---and
GA---where Stacey Abrams [barely lost][GA:SA] the Governor's race in 2018---would
be very interesting in 2020.
- Clinton's biggest under-performances were in heavily Democratic VT and MA.
Her biggest out-performance was in heavily Republican AK. We'd have to look at
other races to be sure, but we imagine Democratic Presidential candidates often
under-perform House voting in heavily blue states and out-perform in heavily red ones
(and vice-versa with Republican Presidential candidates) because voters don't
think their Presidential votes matter so they are less inclined to vote the party line.

[TX:RR]: <https://www.theatlantic.com/politics/archive/2019/09/house-republicans-texas-are-retiring-2020/597406/>
[GA:SA]: <https://www.nytimes.com/2018/11/16/us/elections/georgia-governor-race-kemp-abrams.html>
|]

brText3 :: T.Text
brText3 = [i|
Now that we've looked at Presidential vs. House voting in 2016, let's look at the
2018 Blue Wave in the House and compare that to House voting in 2016. We do the
same breakdown as before. Young college-educated voters were noticeably
more Democratic in their voting in 2018, as were older college-educated female voters.
Among non-college-educated voters, things are mixed but with great variation among
the states.  It's interesting and encouraging that young non-college-educated female voters
shifted strongly Democratic in their House voting between 2016 and 2018.
|]

brText4 :: T.Text
brText4 = [i|
Again, we combine all these views via post-stratification, and we get a picture of the
House voting shift from 2016 to 2018. Except for WI, which was similar in 2018 and 2016,
all the mid-west battleground states, as well as FL, TX, NM, CO and AZ shifted
toward the Dems in their House voting. So did KY, an encouraging thing when we consider
unseating Mitch McConnell in the 2020 Senate race.
|]  

brEnd :: T.Text
brEnd = [i|
## Key Takeaways
- In the Presidential battleground states,
non-college-educated female voters under 45
shifted much more strongly toward Dems as compared to other
non-college-educated voters (except in
Wisconsin). How do Dems hold on to or increase this shift in 2020?
Was something really so different in Wisconsin?

- Wisconsin is actually different from the other mid-western battlegrounds on
nearly all these maps! Wisconsin voters of all sorts were less likely to vote for Clinton
than for their local House candidates. And while young WI voters, with the
exception of non-college-educated young males, were more likely to vote for
Dems in the 2018 House races than in 2016, older *college-educated* voters shifted
*Republican* from 2016 to 2018.  This makes Wisconsin look quite different
from Ohio and Pennsylvania which moved solidly Democratic in their House voting
from 2016 to 2018.  So Wisconsin might not be so much a
["Tipping Point"][WI:TIP] state as an exception among the battlegrounds. 

- Texas! Among nearly all groups in Texas,
Clinton was more popular than local Dem House candidates in 2016. This appears
as an 11% blue VPV shift on the post-stratified map.  A similar shift happens in
the 2016 to 2018 House vote, this time about a 9% VPV shift toward Dem House candidates.
But the House vote shift comes from combining *hugely different* shifts among groups:
college-educated
young females shifted slightly *Republican* in Texas House voting while young
non-college educated males shifted heavily Democratic.  This seems worth
trying to understand better. Texas has many opportunities for House pickups,
a possible Senate flip and is shifting slowly to becoming a Presidential battleground,
though that remains unlikely in 2020.

Sometimes exploring the data leaves us with more questions than answers!

At Blue Ripple Politics we are keenly interested in where this data leads in terms
of what you can do with your time or money to help elect progressives, take back the
White House and Senate and hold the House.  As far as national strategy goes, we think
the data we examined in this post suggests a continued and targeted effort at registering
and GOTV among young female voters, particularly those with a college education.

We don't mean to imply that this is the most important focus for turnout work.
Voters-of-color, particularly black women, are hugely Democratic leaning
and any work to support registration, turnout and to eliminate voter-suppression
for those voters is crucial as well. 

## Take Action
Here are some organizations that work specifically to encourage registration and
turnout among young female voters.

- The [League of Women Voters][LOWV] works on registration and turnout among female voters
in general.  But they are keenly aware of how important the youth vote is and how much work
remains to be done.
- [Higher Heights for America][HHFA] works to support black women running for office
*and* works on voter registration
and turnout among black women.  Though we haven't addressed race in this post, women-of-color
are much more Dem leaning in their voting than any other large demographic group.
- [Get Her Elected][GHE] connects people willing to volunteer services to progressive female
candidates with candidates in need of those services.  Progressive female
candidates running for office and in office
are likely to drive turnout and registration among young women. 

[LOWV]: < https://www.lwv.org/blog/league-volunteers-track-register-voters-200-schools-spring>
[HHFA]: <https://www.higherheightsforamerica.org/>
[GHE]: <https://www.getherelected.com/>
[WI:TIP]: <https://www.washingtonpost.com/lifestyle/all-eyes-are-on-wisconsin-the-state-thats-gearing-up-to-define-the-presidential-election/2019/11/04/abf643dc-f40d-11e9-a285-882a8e386a96_story.html>
|]
  

glmErrorToPandocError :: GLM.GLMError -> PE.PandocError
glmErrorToPandocError x = PE.PandocSomeError $ T.pack $ show x

{-
type BR.LocationCols = '[BR.StateAbbreviation]
locKeyPretty :: F.Record BR.LocationCols -> T.Text
locKeyPretty r =
  let stateAbbr = F.rgetField @BR.StateAbbreviation r
  in stateAbbr
-}

type CatCols = '[BR.SexC, BR.CollegeGradC, BR.SimpleAgeC]
catKey :: BR.Sex -> BR.CollegeGrad -> BR.SimpleAge -> F.Record CatCols
catKey s e a = s F.&: e F.&: a F.&: V.RNil

unCatKey :: F.Record CatCols -> (BR.Sex, BR.CollegeGrad, BR.SimpleAge)
unCatKey r =
  let s = F.rgetField @BR.SexC r
      e = F.rgetField @BR.CollegeGradC r
      a = F.rgetField @BR.SimpleAgeC r
  in (s,e,a)

simpleASEToCatKey :: BR.SimpleASE -> F.Record CatCols
simpleASEToCatKey BR.OldFemaleNonGrad = catKey BR.Female BR.NonGrad BR.EqualOrOver
simpleASEToCatKey BR.YoungFemaleNonGrad = catKey BR.Female BR.NonGrad BR.Under
simpleASEToCatKey BR.OldMaleNonGrad = catKey BR.Male BR.NonGrad BR.EqualOrOver
simpleASEToCatKey BR.YoungMaleNonGrad = catKey BR.Male BR.NonGrad BR.Under
simpleASEToCatKey BR.OldFemaleCollegeGrad = catKey BR.Female BR.Grad BR.EqualOrOver
simpleASEToCatKey BR.YoungFemaleCollegeGrad = catKey BR.Female BR.Grad BR.Under
simpleASEToCatKey BR.OldMaleCollegeGrad = catKey BR.Male BR.Grad BR.EqualOrOver
simpleASEToCatKey BR.YoungMaleCollegeGrad = catKey BR.Male BR.Grad BR.Under


--predMap :: F.Record CatCols -> M.Map (CCESSimplePredictor CatCols) Double
--predMap r = M.fromList $ fmap (\p -> (p, ccesSimplePredictor r p)) allCCESSimplePredictors
{-
predMap r = M.fromList [(P_Sex, if F.rgetField @BR.SexC r == BR.Female then 0 else 1)
--                       ,(P_Race, if F.rgetField @BR.NonWhite r == True then 0 else 1)
                       ,(P_Education, if F.rgetField @BR.CollegeGradC r == BR.NonGrad then 0 else 1)
                       ,(P_Age, if F.rgetField @BR.SimpleAgeC r == BR.EqualOrOver then 0 else 1)
                       ]

allCatKeys = allCCESSimplePredictors --[catKey s e a | a <- [BR.EqualOrOver, BR.Under], e <- [BR.NonGrad, BR.Grad], s <- [BR.Female, BR.Male]]
catPredMaps = M.fromList $ fmap (\k -> (unCCESSimplePredictor k, predMap (unCCESSimplePredictor k))) allCatKeys
-}

catKeyColHeader :: F.Record CatCols -> T.Text
catKeyColHeader r =
  let g = T.pack $ show $ F.rgetField @BR.SexC r
      a = T.pack $ show $ F.rgetField @BR.SimpleAgeC r
      e = T.pack $ show $ F.rgetField @BR.CollegeGradC r
  in a <> "-" <> e <> "-" <> g


type DistrictGeoId = "DistrictGeoId" F.:-> T.Text

type GroupCols = BR.LocationCols V.++ CatCols --StateAbbreviation, Gender] -- this is always location ++ Categories
type MRGroup = BR.RecordColsProxy GroupCols 

  
post :: (K.KnitOne r
        , K.Member GLM.RandomFu r
        )
     => F.Frame BR.States  -- state names from state abbreviations
     -> K.Sem r ()
post stateCrossWalkFrame = P.mapError glmErrorToPandocError $ K.wrapPrefix "DeltaVPV" $ do
  K.logLE K.Info $ "Working on DeltaVPV post..."
  let stateNameByAbbreviation = M.fromList $ fmap (\r -> (F.rgetField @BR.StateAbbreviation r, F.rgetField @BR.StateName r)) $ FL.fold FL.list stateCrossWalkFrame
      isWWC r = (F.rgetField @BR.SimpleRaceC r == BR.White) && (F.rgetField @BR.CollegeGradC r == BR.NonGrad)
  let predictorsASE = fmap GLM.Predictor (BR.allSimplePredictors @BR.CatColsASE)     
      narrowCountFold = fmap (fmap (F.rcast @(BR.LocationCols V.++ CatCols V.++ BR.CountCols)))
      
  predsByLocation2016pC <- do
    K.WithCacheTime ccesTime ccesDataA <- ccesDataLoader    
    K.retrieveOrMakeTransformed (fmap BR.lhToS) (fmap BR.lhFromS) "mrp/pools/predsByLocation" (Just ccesTime) $ do
      ccesData <- ccesDataA
      P.raise (BR.predictionsByLocation @BR.CatColsASE GLM.MDVNone ccesData ({- narrowCountFold -} BR.countDemPres2016VotesF @BR.CatColsASE) predictorsASE  BR.catPredMaps)
      
  predsByLocation2016hC <- do
    K.WithCacheTime ccesTime ccesDataA <- ccesDataLoader    
    K.retrieveOrMakeTransformed (fmap BR.lhToS) (fmap BR.lhFromS) "mrp/deltaVPV/predsByLocation2016h" (Just ccesTime) $ do
      ccesData <- ccesDataA
      P.raise (BR.predictionsByLocation @BR.CatColsASE GLM.MDVNone ccesData (BR.countDemHouseVotesF @BR.CatColsASE 2016) predictorsASE BR.catPredMaps)
      
  predsByLocation2018hC <-  do
    K.WithCacheTime ccesTime ccesDataA <- ccesDataLoader    
    K.retrieveOrMakeTransformed (fmap BR.lhToS) (fmap BR.lhFromS) "mrp/deltaVPV/predsByLocation2018h" (Just ccesTime) $ do
      ccesData <- ccesDataA
      P.raise (BR.predictionsByLocation @BR.CatColsASE GLM.MDVNone ccesData (BR.countDemHouseVotesF @BR.CatColsASE 2018) predictorsASE BR.catPredMaps)

  let vpv x = 2*x - 1
      lhToRecsM year office (BR.LocationHolder _ lkM predMap) =
        let addCols p = FT.mutate (const $ FT.recordSingleton @ET.DemPref p) .
                        FT.mutate (const $ FT.recordSingleton @ET.DemVPV (vpv p)) .
                        FT.mutate (const $ FT.recordSingleton @ET.Office office).
                        FT.mutate (const $ FT.recordSingleton @BR.Year year)                        
            g lk = fmap (\(ck,p) -> addCols p (lk `V.rappend` ck )) $ M.toList predMap
        in fmap g lkM
      pblToFrameFM year office = MR.concatFoldM $ MR.mapReduceFoldM
          (MR.UnpackM $ lhToRecsM year office)
          (MR.generalizeAssign $ MR.Assign (\x -> ((),x)))
          (MR.generalizeReduce $ MR.ReduceFold (const FL.list))
      pblToFrame (year, office, pbl) = FL.foldM (pblToFrameFM year office) $ L.filter ((/= "National") . BR.locName) pbl


  longFrameC <- do
    let (K.WithCacheTime pbl2016pTime pbl2016pA) = predsByLocation2016pC
        (K.WithCacheTime pbl2016hTime pbl2016hA) = predsByLocation2016hC
        (K.WithCacheTime pbl2018hTime pbl2018hA) = predsByLocation2018hC
        newestDepTime = maximum [pbl2016pTime, pbl2016hTime, pbl2018hTime]
    BR.retrieveOrMakeFrame "mrp/dvpv/longFrame.sbin" (Just newestDepTime) $ do
      predsByLocation2016p <- pbl2016pA
      predsByLocation2016h <- pbl2016hA
      predsByLocation2018h <- pbl2018hA      
      K.knitMaybe "Failed to make long-frame"
        $ fmap (F.toFrame . concat)
        $ traverse pblToFrame [(2016, ET.President, predsByLocation2016p)
                              ,(2016, ET.House, predsByLocation2016h)
                              ,(2018, ET.House, predsByLocation2018h)
                              ]
  
  pumsASEByStateC <- do
    (K.WithCacheTime pumsACS_Time pumsACS_A) <- PUMS.pumsLoader
    BR.retrieveOrMakeFrame "mrp/dvpv/pumsASEByState.sbin" (Just pumsACS_Time) $ do
      pumsACSFrame <- pumsACS_A
      let yFilter r = let yr = F.rgetField @BR.Year r in (yr == 2016) || (yr == 2018)        
          processF = FL.prefilter yFilter $ PUMS.pumsStateRollupF (PUMS.pumsKeysToASE True . F.rcast)
      return
        $ fmap (FT.mutate $ const $ FT.recordSingleton @BR.PopCountOf BR.PC_Citizen)
        $ FL.fold processF pumsACSFrame
    
  cpsTurnoutASEByStateC <- do
    K.WithCacheTime cpsVoterTime cpsVoterA <- CPS.cpsVoterPUMSLoader
    BR.retrieveOrMakeFrame "mrp/dvpv/cpsTurnoutASEByState.sbin" (Just cpsVoterTime) $ do
      cpsVoterPUMS <- cpsVoterA
      return
        $ FL.fold (CPS.cpsVoterPUMSElectoralWeightsByState (CPS.cpsKeysToASE True . F.rcast)) cpsVoterPUMS

  aseDemoAndAdjCensusEW_C <- do
    K.WithCacheTime stateTurnoutTime stateTurnoutA <- BR.stateTurnoutLoader
    let (K.WithCacheTime pumsASE_Time pumsASE_A) = pumsASEByStateC
        (K.WithCacheTime cpsTurnoutASEByStateTime cpsTurnoutASEByStateA) = cpsTurnoutASEByStateC
        newestDepTime = maximum [stateTurnoutTime, pumsASE_Time, cpsTurnoutASEByStateTime]
    BR.retrieveOrMakeFrame "mrp/dvpv/aseDemoAndAdjCensusEW.sbin" (Just newestDepTime) $ do
      stateTurnout <- stateTurnoutA
      pumsASEByState <- pumsASE_A
      cpsTurnoutASEByState <- cpsTurnoutASEByStateA
      BR.demographicsWithAdjTurnoutByState
        @BR.CatColsASE
        @PUMS.Citizens
        @'[PUMS.NonCitizens, BR.PopCountOf, BR.StateFIPS]
        @'[BR.Year, BR.StateAbbreviation]
        stateTurnout
        (fmap F.rcast pumsASEByState)
        (fmap F.rcast cpsTurnoutASEByState)
      
  longFrame <- K.ignoreCacheTime longFrameC
  aseDemoAndAdjCensusEW <- K.ignoreCacheTime aseDemoAndAdjCensusEW_C 
  let longFrameWithState = catMaybes $ fmap F.recMaybe $ (F.leftJoin @'[BR.StateAbbreviation]) longFrame stateCrossWalkFrame

  withDemoAndAdjTurnoutFrame <- K.knitEither
                                $ either (Left . T.pack . show) Right
                                $ FJ.leftJoinE @[BR.StateAbbreviation, BR.SexC, BR.CollegeGradC, BR.SimpleAgeC, BR.Year]
                                aseDemoAndAdjCensusEW
                                (F.toFrame longFrameWithState)
  let labelCell x = V.rappend (FT.recordSingleton @ET.PrefType x)
      psCellVPVByBothF =  (<>)
                          <$> fmap pure (fmap (labelCell ET.PSByVAP) $ BR.postStratifyCell @ET.DemVPV (realToFrac . F.rgetField @PUMS.Citizens) (realToFrac . F.rgetField @ET.DemVPV))
                          <*> fmap pure (fmap (labelCell ET.PSByVoted) $ BR.postStratifyCell @ET.DemVPV (\r -> realToFrac (F.rgetField @PUMS.Citizens r) * F.rgetField @ET.ElectoralWeight r) (realToFrac . F.rgetField @ET.DemVPV))
      psVPVByStateF = BR.postStratifyF
                      @[BR.Year, ET.Office, BR.StateAbbreviation, BR.StateName]
                      @[ET.DemVPV, PUMS.Citizens, ET.ElectoralWeight]
                      @[ET.PrefType, ET.DemVPV]
                      psCellVPVByBothF 
        
      psVPVByBoth = FL.fold psVPVByStateF withDemoAndAdjTurnoutFrame
  brAddMarkDown brText1
--  _ <- K.addHvega Nothing Nothing $ vlVPVByDistrict "Test" (FV.ViewConfig 800 800 10) (fmap F.rcast psVPVByDistrictPres2016ByVoted)
{-  _ <- K.addHvega Nothing Nothing $ vlStateScatterVsElection
       "VPV: 2016 House vs. President"
       (FV.ViewConfig 800 800 10)
       ("House 2016", "President 2016")
       (fmap F.rcast longFrame)
-}

  _ <- K.addHvega Nothing Nothing $ vldVPVByState
    "Change in College-Graduate VPV: President 2016 - House 2016"
    (FV.ViewConfig 300 300 10)
    ("President2016","House2016")
    BR.Grad
    (fmap F.rcast longFrameWithState) --presMinusHouse2016
  brLineBreak
--  K.addStrictTextHtml "<br>"
  _ <- K.addHvega Nothing Nothing $ vldVPVByState
    "Change in Non-College-Graduate VPV: President 2016 - House 2016"
    (FV.ViewConfig 300 300 10)
    ("President2016","House2016")
    BR.NonGrad
    (fmap F.rcast longFrameWithState) --presMinusHouse2016
  brAddMarkDown brText2
  _ <- K.addHvega Nothing Nothing $ vldVPVByStatePS
    "Post-Stratified Dem VPV: President 2016 - House 2016"
    (FV.ViewConfig 800 800 10)
    ("President2016","House2016")
    (fmap F.rcast $ F.filterFrame ((== ET.PSByVoted) . F.rgetField @ET.PrefType) psVPVByBoth)
  brAddMarkDown brText3
{-  K.addHvega Nothing Nothing
    $ vlPostStratScatter
    "States: VPV 2016 House vs 2016 President"
    (FV.ViewConfig 800 800 10)
    ("House 2016","President 2016")
    (fmap F.rcast $ psVPVByBoth)
  _ <- K.addHvega Nothing Nothing $ vlStateScatterVsElection
                         "Dem VPV: 2016 House vs 2018 House"
                          (FV.ViewConfig 800 800 10)
                          ("House 2016", "House 2018")
                          (fmap F.rcast longFrame)
-}
  _ <- K.addHvega Nothing Nothing $ vldVPVByState
    "Change in College-Grad VPV: House 2018 - House 2016"
    (FV.ViewConfig 300 300 10)
    ("House2018","House2016")
    BR.Grad
    (fmap F.rcast longFrameWithState) -- house2018MinusHouse2016
  brLineBreak
  _ <- K.addHvega Nothing Nothing $ vldVPVByState
    "Change in Non-College-Grad VPV: House 2018 - House 2016"
    (FV.ViewConfig 300 300 10)
    ("House2018","House2016")
    BR.NonGrad
    (fmap F.rcast longFrameWithState) -- house2018MinusHouse2016
  brAddMarkDown brText4
  _ <- K.addHvega Nothing Nothing $ vldVPVByStatePS
    "Post-Stratified Dem VPV: House 2018 - House 2016"
    (FV.ViewConfig 800 800 10)
    ("House2018","House2016")
    (fmap F.rcast $ F.filterFrame ((== ET.PSByVoted) . F.rgetField @ET.PrefType) psVPVByBoth)
{-  K.addHvega Nothing Nothing
    $ vlPostStratScatter
    "States: VPV 2016 House vs 2018 House"
    (FV.ViewConfig 800 800 10)
    ("House 2016","House 2018")
    (fmap F.rcast $ psVPVByBoth)
-}
--  _ <- K.addHvega Nothing Nothing $ vlVPVChoropleth "VPV: College Graduates" (FV.ViewConfig 400 200 10)  $ fmap F.rcast $ withTurnoutFrame 
  let battlegroundStates =
        [ "AZ"
        , "FL"
        , "GA"
        , "IA"
        , "NC"
        , "OH"
        , "MI"
        , "WI"
        , "PA"
        , "CO"
        , "NH"
        , "NV"
        , "TX"
        , "VA"
        ]
      sortedBGNat = "National" : L.sort battlegroundStates      
  brAddMarkDown brEnd       
  brAddMarkDown brReadMore


educationGap :: BR.Sex -> BR.SimpleAge -> BR.LocationHolder CatCols F.ElField Double -> Maybe (Double, Double)
educationGap s a (BR.LocationHolder _ _ cd) = do  
  datGrad <- M.lookup (catKey s BR.Grad a) cd
  datNonGrad <- M.lookup (catKey s BR.NonGrad a) cd
  return (datNonGrad, datGrad)

ageGap :: BR.Sex -> BR.CollegeGrad -> BR.LocationHolder CatCols F.ElField Double -> Maybe (Double, Double)
ageGap s e (BR.LocationHolder _ _ cd) = do  
  datYoung <- M.lookup (catKey s e BR.Under) cd
  datOld <- M.lookup (catKey s e BR.EqualOrOver) cd
  return (datOld, datYoung)
                
--emphasizeStates s = CellStyle (\tr _ -> if inStates s tr then highlightCellBlue else "")
emphasizeNational = CellStyle (\x _ -> if BR.locName x == "National" then highlightCellPurple else "")

significantGivenCI :: (Double, Double) -> (Double, Double) -> Double
significantGivenCI (loA, hiA) (loB, hiB) =
  let maxLo = max loA loB
      minHi = min hiA hiB
      overlap = max 0 (minHi - maxLo)
      shortest = min (hiA - loA) (hiB - loB)
  in overlap / shortest
                    
colPrefByLocation
  :: [F.Record CatCols]
  -> CellStyle (BR.LocationHolder CatCols F.ElField Double) T.Text
  -> C.Colonnade C.Headed (BR.LocationHolder CatCols F.ElField Double) BC.Cell
colPrefByLocation cats cas =
  let h = catKeyColHeader
      hc c = BC.Cell (BHA.class_ "brTableHeader") $ BH.toHtml c
      rowFromCatKey :: F.Record CatCols -> C.Colonnade C.Headed (BR.LocationHolder CatCols F.ElField Double) BC.Cell
      rowFromCatKey r =
        C.headed (hc $ h r) (toCell cas (h r) (h r) (maybeNumberToStyledHtml "%2.1f" . fmap (*100) . M.lookup r . BR.catData))
  in C.headed "Location" (toCell cas "Location" "Location" (textToStyledHtml . BR.locName))
     <> mconcat (fmap rowFromCatKey cats)


vlPostStratScatter :: Foldable f
                   => T.Text
                   -> FV.ViewConfig
                   -> (T.Text, T.Text)
                   -> f (F.Record [BR.StateAbbreviation, ET.Office, BR.Year, ET.PrefType, ET.DemVPV])
                   -> GV.VegaLite
vlPostStratScatter title vc (race1, race2) rows =
  let pivotFold = FV.simplePivotFold @[ET.Office, BR.Year] @'[ET.DemVPV]
        (\keyLabel dataLabel -> dataLabel <> "-" <> keyLabel)
        (\r -> (T.pack $ show $ F.rgetField @ET.Office r)
               <> " "
               <> (T.pack $ show $ F.rgetField @BR.Year r))
        (\r -> [("Dem VPV",GV.Number $ F.rgetField @ET.DemVPV r)])        
      dat = GV.dataFromRows [] $ FV.pivotedRecordsToVLDataRows @'[BR.StateAbbreviation,ET.PrefType]
            pivotFold rows
      vpvCol x = "Dem VPV" <> "-" <> x
      encX = GV.position GV.X [GV.PName (vpvCol race1), GV.PmType GV.Quantitative, GV.PAxis [GV.AxTitle race1]]
      encY = GV.position GV.Y [GV.PName (vpvCol race2), GV.PmType GV.Quantitative, GV.PAxis [GV.AxTitle race2]]
      encColor = GV.color [FV.mName @ET.PrefType]
      encX2 = GV.position GV.X [GV.PName (vpvCol race2), GV.PmType GV.Quantitative, GV.PAxis [GV.AxTitle ""]]
      encY2 = GV.position GV.Y [GV.PName (vpvCol race1), GV.PmType GV.Quantitative, GV.PAxis [GV.AxTitle ""]]
      encScatter = GV.encoding . encX . encY . encColor
      scatterSpec = GV.asSpec [encScatter [], GV.mark GV.Point [GV.MFilled True,GV.MTooltip GV.TTData]]
      lineSpec1 = GV.asSpec [(GV.encoding . encX . encY2) [], GV.mark GV.Line []]
      lineSpec2 = GV.asSpec [(GV.encoding . encX2 . encY) [], GV.mark GV.Line []]
  in FV.configuredVegaLite vc [FV.title title, GV.layer [scatterSpec, lineSpec1, lineSpec2], dat]
  

vlStateScatterVsElection :: Foldable f
                         => T.Text                         
                         -> FV.ViewConfig
                         -> (T.Text, T.Text)
                         -> f (F.Record [BR.StateAbbreviation, ET.Office, BR.Year, BR.SexC, BR.CollegeGradC, BR.SimpleAgeC, ET.DemVPV])
                         -> GV.VegaLite
vlStateScatterVsElection title vc@(FV.ViewConfig w h _) (race1, race2) rows = 
  let pivotFold = FV.simplePivotFold @[ET.Office, BR.Year] @'[ET.DemVPV]
        (\keyLabel dataLabel -> dataLabel <> "-" <> keyLabel)
        (\r -> (T.pack $ show $ F.rgetField @ET.Office r)
               <> " "
               <> (T.pack $ show $ F.rgetField @BR.Year r))
        (\r -> [("Dem VPV",GV.Number $ F.rgetField @ET.DemVPV r)])        
      dat = GV.dataFromRows [] $ FV.pivotedRecordsToVLDataRows @'[BR.StateAbbreviation, BR.SexC, BR.CollegeGradC, BR.SimpleAgeC]
            pivotFold rows
      vpvCol x = "Dem VPV" <> "-" <> x
      encX = GV.position GV.X [GV.PName (vpvCol race1), GV.PmType GV.Quantitative, GV.PAxis [GV.AxTitle race1]]                              
      encY = GV.position GV.Y [GV.PName (vpvCol race2), GV.PmType GV.Quantitative, GV.PAxis [GV.AxTitle race2]]
      encY2 = GV.position GV.Y [GV.PName (vpvCol race1), GV.PmType GV.Quantitative, GV.PAxis [GV.AxTitle ""]]
      encX2 = GV.position GV.X [GV.PName (vpvCol race2), GV.PmType GV.Quantitative, GV.PAxis [GV.AxTitle ""]]
      facet = GV.facet [GV.ColumnBy [FV.fName @BR.CollegeGradC, GV.FmType GV.Nominal]]--, GV.RowBy [GV.FName "Age", GV.FmType GV.Nominal]]
      encFacetRow = GV.row [FV.fName @BR.CollegeGradC, GV.FmType GV.Nominal]
      addSexAge = GV.calculateAs "datum.Sex + '/' + datum.Under45" "Sex/Age"
      encColor = GV.color [GV.MName "Sex/Age", GV.MmType GV.Nominal]
      dotSpec = GV.asSpec [(GV.encoding . encX . encY . encColor) []
                          , GV.mark GV.Point [GV.MTooltip GV.TTData]
                          , GV.width (w/2)
                          , GV.height h
                          , (GV.transform . addSexAge) []
                          ]
      refLineSpec1 = GV.asSpec [(GV.encoding . encX . encY2) [], GV.mark GV.Line [], GV.width (w/2), GV.height h]
      refLineSpec2 = GV.asSpec [(GV.encoding . encX2 . encY) [], GV.mark GV.Line [], GV.width (w/2), GV.height h]
      layers = GV.layer [refLineSpec1, refLineSpec2, dotSpec]
      allProps = [layers]
    in FV.configuredVegaLite vc [FV.title title ,GV.specification (GV.asSpec [layers]), facet , dat]



usStatesTopoJSONUrl = "https://cdn.jsdelivr.net/npm/us-atlas@3/states-10m.json"
usStatesAlbersTopoJSONUrl = "https://cdn.jsdelivr.net/npm/us-atlas@3/states-albers-10m.json"
usDistrictsTopoJSONUrl = "https://raw.githubusercontent.com/blueripple/data-sets/master/data/geo/cd116_2018.topojson"

choroColorScale = GV.MScale [GV.SScheme "redblue" [], GV.SDomain (GV.DNumbers [-0.7,0.7])]

vldVPVByState :: Foldable f
                 => T.Text
                 -> FV.ViewConfig
                 -> (T.Text, T.Text)
                 -> BR.CollegeGrad
                 -> f (F.Record [BR.StateName, ET.Office, BR.Year, BR.SexC, BR.CollegeGradC, BR.SimpleAgeC, ET.DemVPV])
                 -> GV.VegaLite
vldVPVByState title vc (race1, race2) edFilter rows =
  let datGeo = GV.dataFromUrl usStatesTopoJSONUrl [GV.TopojsonFeature "states"]
      pivotFold = FV.simplePivotFold @[ET.Office, BR.Year] @'[ET.DemVPV]
                  (\keyLabel dataLabel -> keyLabel)
                  (\r -> (T.pack $ show $ F.rgetField @ET.Office r)
                    <> (T.pack $ show $ F.rgetField @BR.Year r))
                  (\r -> [("Dem VPV",GV.Number $ F.rgetField @ET.DemVPV r)])        
      datVal = GV.dataFromRows [] $ FV.pivotedRecordsToVLDataRows @'[BR.StateName, BR.SexC, BR.CollegeGradC, BR.SimpleAgeC]
               pivotFold rows
      dataSets = GV.datasets [("stateDat",datVal)]
      encFacetRow = GV.row [FV.fName @BR.SexC, GV.FmType GV.Nominal, GV.FHeader [GV.HNoTitle]]
      encFacetCol = GV.column [FV.fName @BR.SimpleAgeC, GV.FmType GV.Nominal, GV.FHeader [GV.HNoTitle]]
      filter = GV.filter (GV.FExpr $ "datum.CollegeGrad == '" <> (T.pack $ show edFilter) <> "'") -- && datum.Election == '2016 President' && datum.Age == 'Under'")
      vpvCol x = x
      calcDiff = GV.calculateAs ("datum." <> vpvCol race1 <> "-" <> "datum." <> vpvCol race2) "Change in VPV"
      projection = GV.projection [GV.PrType GV.AlbersUsa]
      transform2 = GV.transform . GV.lookupAs (FV.colName @BR.StateName) datGeo "properties.name" "geo" . calcDiff . filter
      mark = GV.mark GV.Geoshape []
      colorEnc = GV.color [GV.MName "Change in VPV"
                          , GV.MmType GV.Quantitative
                          , choroColorScale
                          ]
      shapeEnc = GV.shape [GV.MName "geo", GV.MmType GV.GeoFeature]
      tooltip = GV.tooltips [[GV.TName (FV.colName @BR.StateName), GV.TmType GV.Nominal]
                            ,[GV.TName "Change in VPV", GV.TmType GV.Quantitative, GV.TFormat ".0%"]
                            ]      
      enc = GV.encoding .  colorEnc . shapeEnc . encFacetRow . encFacetCol . tooltip 
      cSpec = GV.asSpec [datVal, transform2 [], enc [], mark, projection]
  in FV.configuredVegaLite vc [FV.title title,  datVal, transform2 [], enc [], mark, projection]


vlVPVChoropleth :: Foldable f
                => T.Text
                -> FV.ViewConfig
                -> f (F.Record [BR.StateName, BR.StateFIPS, BR.Year, ET.Office, BR.SexC, BR.SimpleAgeC, BR.CollegeGradC, ET.DemVPV])
                -> GV.VegaLite
vlVPVChoropleth title vc rows =
  let datGeo = GV.dataFromUrl usStatesTopoJSONUrl [GV.TopojsonFeature "states"]
      datVal = FV.recordsToVLData id FV.defaultParse rows
      encFacetRow = GV.row [FV.fName @ET.Office, GV.FmType GV.Nominal]
      encFacetCol = GV.column [FV.fName @BR.SimpleAgeC, GV.FmType GV.Nominal]
      filter = GV.filter (GV.FExpr $ "datum.CollegeGrad == 'Grad' && datum.Sex == 'Female'")
      projection = GV.projection [GV.PrType GV.AlbersUsa]
      transform2 = GV.transform . GV.lookupAs "StateName" datGeo "properties.name" "geo" . filter
      mark = GV.mark GV.Geoshape []
      colorEnc = GV.color [FV.mName @ET.DemVPV
                          , GV.MmType GV.Quantitative
                          , choroColorScale
                          ]
      shapeEnc = GV.shape [GV.MName "geo", GV.MmType GV.GeoFeature]
      tooltip = GV.tooltips [[FV.tName @BR.StateName, GV.TmType GV.Nominal]
                            ,[FV.tName @ET.DemVPV, GV.TmType GV.Quantitative, GV.TFormat ".0%"]]      
      enc = GV.encoding .  colorEnc . shapeEnc . encFacetRow . encFacetCol . tooltip 
  in FV.configuredVegaLite vc [FV.title title,  datVal, transform2 [], enc [], mark, projection]

vldVPVByStatePS :: Foldable f
                => T.Text
                -> FV.ViewConfig
                -> (T.Text, T.Text)
                -> f (F.Record [BR.StateName, BR.Year, ET.Office, ET.DemVPV])
                -> GV.VegaLite
vldVPVByStatePS title vc (race1, race2) rows =
  let datGeo = GV.dataFromUrl usStatesTopoJSONUrl [GV.TopojsonFeature "states"]
      pivotFold = FV.simplePivotFold @[ET.Office, BR.Year] @'[ET.DemVPV]
                  (\keyLabel dataLabel -> keyLabel)
                  (\r -> (T.pack $ show $ F.rgetField @ET.Office r)
                         <> (T.pack $ show $ F.rgetField @BR.Year r))
                  (\r -> [("Dem VPV",GV.Number $ F.rgetField @ET.DemVPV r)])        
      datVal = GV.dataFromRows [] $ FV.pivotedRecordsToVLDataRows @'[BR.StateName]
               pivotFold rows
      projection = GV.projection [GV.PrType GV.AlbersUsa]
      lookup = GV.lookupAs "StateName" datGeo "properties.name" "geo"
      calculate = GV.calculateAs ("datum." <> race1 <> " - " <> "datum." <> race2) "Change in VPV"
      mark = GV.mark GV.Geoshape []
      shapeEnc = GV.shape [GV.MName "geo", GV.MmType GV.GeoFeature]
      colorEnc = GV.color [GV.MName "Change in VPV"
                          , GV.MmType GV.Quantitative
                          , choroColorScale
                          ]
      tooltipEnc = GV.tooltips [[FV.tName @BR.StateName, GV.TmType GV.Nominal]
                               ,[GV.TName "Change in VPV", GV.TmType GV.Quantitative, GV.TFormat ".0%"]
                               ]
      enc = GV.encoding . shapeEnc . colorEnc . tooltipEnc
  in FV.configuredVegaLite vc [FV.title title, datVal, (GV.transform . calculate . lookup) [], enc [], mark, projection]



vlVPVByDistrict :: Foldable f
                => T.Text
                -> FV.ViewConfig
                -> f (F.Record [DistrictGeoId, ET.DemVPV])
                -> GV.VegaLite
vlVPVByDistrict title vc rows =
  let datGeo = GV.dataFromUrl usDistrictsTopoJSONUrl [GV.TopojsonFeature "cd116"]
      datVal = FV.recordsToVLData id FV.defaultParse rows
      projection = GV.projection [GV.PrType GV.AlbersUsa]
      lookup = GV.lookupAs "DistrictGeoId" datGeo "properties.GEOID" "geo"
      mark = GV.mark GV.Geoshape []
      shapeEnc = GV.shape [GV.MName "geo", GV.MmType GV.GeoFeature]
      colorEnc = GV.color [FV.mName @ET.DemVPV, GV.MmType GV.Quantitative, choroColorScale]
      enc = GV.encoding . shapeEnc . colorEnc
  in FV.configuredVegaLite vc [FV.title title, datVal, (GV.transform . lookup) [], enc [], mark, projection]


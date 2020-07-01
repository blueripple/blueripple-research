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
{-# LANGUAGE TypeOperators             #-}
{-# LANGUAGE QuasiQuotes               #-}
{-# LANGUAGE StandaloneDeriving        #-}
{-# LANGUAGE TupleSections             #-}

{-# OPTIONS_GHC  -fplugin=Polysemy.Plugin  #-}

module MRP.Pools (post) where

import qualified Control.Foldl                 as FL
import           Data.Function (on)
import qualified Data.List as L
import qualified Data.Map                      as M

import qualified Data.Text                     as T

import           Graphics.Vega.VegaLite.Configuration as FV
import qualified Graphics.Vega.VegaLite.Compat as FV
import qualified Frames as F
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V

import qualified Graphics.Vega.VegaLite        as GV
import qualified Knit.Report                   as K
import qualified Polysemy.Error                as P (mapError)
import qualified Polysemy                      as P (raise)
import           Text.Pandoc.Error             as PE
import qualified Text.Blaze.Colonnade          as BC

import           Data.String.Here               ( i )

import qualified Colonnade                     as C
import qualified Text.Blaze.Colonnade          as BC
import qualified Text.Blaze.Html               as BH
import qualified Text.Blaze.Html5.Attributes   as BHA

import           BlueRipple.Configuration 
import           BlueRipple.Utilities.KnitUtils 
import           BlueRipple.Utilities.TableUtils 

import qualified Numeric.GLM.ProblemTypes      as GLM
import qualified Numeric.GLM.ModelTypes      as GLM
import qualified Numeric.GLM.Bootstrap            as GLM
import qualified Numeric.GLM.MixedModel as GLM

import BlueRipple.Data.DataFrames
import qualified BlueRipple.Data.DemographicTypes as BR
import qualified BlueRipple.Data.ElectionTypes as ET
import qualified BlueRipple.Model.MRP as BR
import MRP.Common
import MRP.CCES

import qualified PreferenceModel.Common as PrefModel

brText1 :: T.Text
brText1 = [i|
In prior analyses ([here][BR:PM2018], focusing on 2018, and [here][BR:PMAcrossTime],
considering trends 2010-2018),
we asked a key question about Democratic strategy in 2020:
should the focus be on turning out likely Dem voters, or changing the minds of folks
who are less likely to vote for Team Blue? (TL;DR: We concluded that we should
definitely get our base to turn out, but we shouldn't neglect subgroups outside
of that group that align with us on key issues).

Here, we ask an obvious follow-up question about turnout: if we want to mobilize
likely Democratic voters, on whom should we focus? Is that answer the same
in every state or district?  There is a tendency to imagine that the voting
preferences of a demographic group are the same everywhere. That, for example,
young voters in Texas are the same as young voters in Michigan.  But looking only
at national averages obscures lots of interesting
regional variation.  That regional variation *matters*, since house and senate
campaigns contend for voters in specific places and, as long as the path to the
presidency goes through the electoral college, the presidential race also has
a strong geographic component.

In this post, we focus specifically on college-educated voters, and mostly on female
college-educated voters.
We were inspired to begin our analysis here by one of our favorite election modelers,
[Professor Rachel Bitecofer][Bitecofer:Bio], who generously spent some time chatting with us
last month about our related interests.
She reminded us of a core idea in her [spot-on analysis][Bitecofer:2018House] of the 2018
house races: the presence of "large pools of untapped Democratic voters,"
including college-educated voters, makes for places where Democrats can (and did in 2018!)
outperform compared to 2016.

In our work, we always try to keep an eye on how progressives and Democrats can best
use their time and/or money in upcoming elections. Our [values][BR:AboutUs]
make us particularly
interested in work that emphasizes registration and voter turnout. In this post we
examine if and *where* a college-educated-voter turnout drive might be most effective.

1. **Data and Methods**
2. **How Many Votes is a New Voter Worth?**
3. **College Educated Voters and Age**
4. **Key Takeaways**
5. **Take Action**

## Data and Methods {#DataAndMethods}
In our research pieces so far, we've looked only at aggregate data, that
is data which comes from adding together a large number of people: census
counts or election results, for example.  In this piece we look at some
per-person data, namely the Cooperative Congressional Election Study, or 
[CCES][CCES],
which surveys about 60,000 people
in every election cycle. For each survey response,
the CCES includes geographic and demographic information along with opinion about
various political questions, whether the person is registered to vote, and whether they
voted and for whom in elections for Governor, House, Senate and President, whenever
each is applicable. 

The geographic information allows
us to start estimating a variety of things at the state level, something that isn't possible
using only aggregate data.  We do this using Multi-level Regression
(the "MR" of "MRP", which stands for Multi-level Regression with Post-stratification), a technique
explained in more detail [here][MRP:Summary] and about which we will have an explainer in the
coming months. Very briefly: this method allows you to use all the data
(e.g., all female-college-educated voters over 45 who voted for a Democrat or Republican)
to get a baseline and then use
the data in a specific "bucket" (e.g., those same voters, but just in Texas)
to make an improved
inference for that set of people. In other words,
it balances the abundance of the non-specific data with the
sparser local information for improved insight.

If we are hoping to boost turnout among Democratic
leaning voters in battleground states or crucial races, it helps to know which voters are
most likely to be Democratic leaning *in that particular state or district.*  This data
and these techniques allow us to do just that.

(Quick aside for data geeks: Our specific model uses a survey-weighted, logistic MR to fit a
binomial distribution to the likelihood of a Democratic vote in the 2016 presidential election,
among voters who voted D or R,
in each of 408 = 51 x 2 x 2 x 2 groups:
(states + DC) x (Female or Male) x (Non-College Grad or College Grad) x (Under 45 or 45 and over).
Also, as we've said in earlier posts, we recognize that these categories are vast oversimplifications
of the electorate.  We are limited by what information is present in the survey---Sex is limited
to "Female" and "Male" in the CCES---and by computational complexity, which is why we've limited
our education and age breakdown to two categories each.  Its also important to note that the accuracy
of these inferences depends on the number of voters in each category.  So the estimates for more
populous states are likely to be more accurate.)

In this post, we analyze the 2016 presidential election to measure Democratic voter preference
among college-educated voters in each state (and DC). Given the results in the house
races in 2018, we think that most college-educated voters have become *more likely* to vote
for Democrats since 2016.  But there are inevitable differences between presidential elections
and house races and we want to start with a straightforward question:  If the 2020 electorate
was like the 2016 electorate, where might a college-educated-voter turnout drive be useful?

## How Many Votes is a New Voter Worth? {#VPV}
Before getting into the results, let's introduce a metric that we call "Votes Per Voter" (VPV),
which reflects the "value" to Democrats of getting a single person in a particular group
to show up on election day.

Here's how VPV works: Let's say that our model tells us that in a certain state,
young, female, college-educated people vote Democratic about 72% of the time.
If we increased turnout among them by 100,000
voters for 2020 and they were exactly as likely to vote Democratic as in 2016,
how many votes would that extra 100,000 voters net the Democratic candidate?
Of that 100,000, 72,000 (72% x 100,000)
would vote for the democrat and 28,000 ((100% - 72%) x 100,000) for the Republican.
So Dems would net 72,000 - 28,000 = 44,000 votes. In general, if $x\\%$ of voters will vote for
the Democrat, each such voter is "worth" $2x-100\\%$ votes. In other words, the VPV for this
group is 0.44 (44,000 *net* Democratic votes out of 100,000 new voters in the group who show up to
cast ballots).
Note that a group with a Democratic voter preference of 50% has a VPV of 0, a group that votes
Democratic 60% of the time has a VPV of 0.2. And a group which votes for Democrats less
than 50% of the time has a negative VPV.

This metric is useful because it highlights the connection between voter preference and
votes gained by additional turnout.
A group which leans only slightly Democratic is not a great place to invest resources
on turnout since each voter is only slightly more likely to vote blue.  This is
reflected by the near 0 VPV.

As a side note, this is why changing people's minds can seem a more appealing avenue to getting
votes: changing a Republican vote to a Democratic vote has a VPV of 2 (200%):
one vote is lost by the Republican and one is gained by the Democrat, so each such voter is "worth" 2 votes.
(More on that in a later analysis---stay tuned!)

## Female College Educated Voters and Age 
Let's look first at female college-educated voters and how their VPV varies by state and by age.
This demographic, one that has
been [trending strongly Democratic since the early 2000s][NPR:CollegeWomen],
is particularly important for Democrats.
In the chart below we show the VPV of female college-educated voters, split by age at 45,
for each state (and DC and the Nation as a whole).
We've ordered the states (plus DC and the nation as a whole) by increasing VPV of
female college-educated voters under 45.

[CCES]: <https://cces.gov.harvard.edu/>
[MRP:Summary]: <https://en.wikipedia.org/wiki/Multilevel_regression_with_poststratification>
[MRP:Methods]: <${brGithubUrl (postPath PostMethods)}>
[PrefModel:WWCV]: <${brGithubUrl (PrefModel.postPath PrefModel.PostWWCV)}>
[PrefModel:AcrossTime]: <${brGithubUrl (PrefModel.postPath PrefModel.PostAcrossTime)}>
[BR:PM2018]: <https://blueripple.github.io/research/preference-model/p1/main.html>
[BR:PMAcrossTIme]: <https://blueripple.github.io/research/preference-model/p2/main.html>
[BR:AboutUs]: <https://blueripplepolitics.org/about-us>
[Bitecofer:Bio]: <http://cnu.edu/people/rachelbitecofer/>
[Bitecofer:2018House]: <https://cnu.edu/wasoncenter/2018/09/26-signs-democrats-win-big/>
[Bitecofer:2020Pres]: <https://cnu.edu/wasoncenter/2019/07/01-2020-election-forecast/>
[NPR:CollegeWomen]: <https://www.npr.org/2018/09/24/650447848/the-womens-wave-backlash-to-trump-persists-reshaping-politics-in-2018>
|]
  
brText2 :: T.Text
brText2 = [i|
We also looked specifically at some states we think are worth highlighting, including several
classic presidential "battlegrounds" and others (like Texas and Georgia) that are moving towards
battleground status and have important house seats in play. we've added male voters here as well,
in order to clarify the focus on *female* college-educated voters as a good source of Democratic votes:
|]

brText3 :: T.Text
brText3 = [i|
## Key Takeaways
There's a lot of detail here, but we'd like to make a few main observations:

- College-educated voters are not a monolithic group.  There's a lot of variation in
VPV by state, by sex and by age. A GOTV strategy targeting **all** college-educated voters
is not a good use of resources for Dems since it will target lots of voters with VPV below
or near 0.
- Should Dems turnout-efforts target *female* college-educated voters? It depends.
They skew more Democratic (positive VPV),
but the "yield" of targeting this group probably depends on the age distribution, because the over-45s are less blue.
- What about *young* college-educated voters? If we compare males and females in this group across states,
there's a pretty wide variability.
Female college-educated voters under 45 all have positive VPV,
but a strategy targeting young college-educated voters (of either sex)
in battleground states would have different degrees of effectiveness between states.
- Finally: should we micro-target *female* college-educated voters *under 45* for GOTV efforts?
They're the most Dem-friendly subgroup we studied here,
but their VPVs across the states are widely distributed,
from under 0.1 in Indiana, to almost 0.65 in California.
That variability is also pronounced in the battleground states:
female college-educated voters under 45 have a much higher VPV in some places (TX, NH, NV) than others (PA, MI).
- BOTTOM LINE: A GOTV strategy targeting young female college grads would likely help Dems,
but may yield many more net Dem votes in some places than others.
The success of broader strategies (e.g., targeting college grads more broadly)
would depend on the relative sizes of different sub-groups -- which is a topic we'll explore in a subsequent post.

## Take Action
This early in the cycle it can be difficult to find organizations focused on specific groups within
states.  But there are some groups which are clearly working in the same vein. If you know
of local organizations doing work on youth/college-educated voter turnout, please email us with
that information and we'll update this post.

- [MOVE Texas][Org:MOVETexas], does GOTV work with young people in Texas.
- [The Sunrise Movement][Org:SunriseMovement], a youth-focused environmental advocacy group
has joined forces with various local organizations to work on registration and turnout among young voters.
- [NextGen America][Org:NextGenAmerica] works on youth turnout nationally, holding registration events
at many colleges and universities.

[PrefModel:WWCV]: <${brGithubUrl (PrefModel.postPath PrefModel.PostWWCV)}>
[PrefModel:AcrossTime]: <${brGithubUrl (PrefModel.postPath PrefModel.PostAcrossTime)}>
[Org:MOVETexas]: <https://movetexas.org/>
[Org:TurnPABlue]: <https://turnpablue.org/>
[Org:WisDems]: <https://wisdems.org/>
[Org:NextGenAmerica]: <https://nextgenamerica.org/>
[Org:OhioStudentOrganization]: <https://ohiostudentassociation.org/campaign/voter-registration/>
[Org:SunriseMovement]: <https://www.sunrisemovement.org/>
|]
  

glmErrorToPandocError :: GLM.GLMError -> PE.PandocError
glmErrorToPandocError x = PE.PandocSomeError $ T.pack $ show x

{-
type BR.LocationCols = '[StateAbbreviation]
locKeyPretty :: F.Record BR.LocationCols -> T.Text
locKeyPretty r =
  let stateAbbr = F.rgetField @StateAbbreviation r
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

--predMap :: F.Record CatCols -> M.Map (CCESSimplePredictor CatCols) Double
--predMap r = M.fromList $ fmap (\p -> (p, ccesSimplePredictor r p)) allCCESSimplePredictors
{-
  M.fromList [(P_Sex, if F.rgetField @BR.SexC r == BR.Female then 0 else 1)
                       ,(P_Education, if F.rgetField @BR.CollegeGradC r == BR.NonGrad then 0 else 1)
                       ,(P_Age, if F.rgetField @BR.SimpleAgeC r == BR.EqualOrOver then 0 else 1)
                       ]

allCatKeys = allCCESSimplePredictors --[catKey s e a | a <- [BR.EqualOrOver, BR.Under], e <- [BR.NonGrad, BR.Grad], s <- [BR.Female, BR.Male]]
catPredMaps = M.fromList $ fmap (\k -> (unCCESSimplePredictor k,predMap (unCCESSimplePredictor k))) allCatKeys
-}

catKeyColHeader :: F.Record CatCols -> T.Text
catKeyColHeader r =
  let g = T.pack $ show $ F.rgetField @BR.SexC r
--      wnh = if F.rgetField @WhiteNonHispanic r then "White" else "NonWhite"
      a = T.pack $ show $ F.rgetField @BR.SimpleAgeC r
      e = T.pack $ show $ F.rgetField @BR.CollegeGradC r
  in a <> "-" <> e <> "-" <> g


type GroupCols = BR.LocationCols V.++ CatCols --StateAbbreviation, Gender] -- this is always location ++ Categories
type MRGroup = BR.RecordColsProxy GroupCols 

  
post :: forall r.(K.KnitOne r, K.CacheEffectsD r, K.Member GLM.RandomFu r, K.Member GLM.Async r)
     => M.Map T.Text T.Text -- state names from state abbreviations
     -> K.Sem r ()
post stateNameByAbbreviation = P.mapError glmErrorToPandocError $ K.wrapPrefix "Pools" $ do
  K.logLE K.Info $ "Working on Pools post..."
  let isWWC r = (F.rgetField @BR.SimpleRaceC r == BR.White) && (F.rgetField @BR.CollegeGradC r == BR.NonGrad)
      countDemPres2016VotesF = BR.weightedCountFold @ByCCESPredictors @CCES_MRP @'[Pres2016VoteParty,CCESWeightCumulative]
                                (\r -> (F.rgetField @Turnout r == T_Voted)
                                      && (F.rgetField @Year r == 2016)
                                      && (F.rgetField @Pres2016VoteParty r `elem` [ET.Republican, ET.Democratic]))
                               ((== ET.Democratic) . F.rgetField @Pres2016VoteParty)
                               (F.rgetField @CCESWeightCumulative)

  let preds =  GLM.Intercept : fmap GLM.Predictor BR.allSimplePredictors --[GLM.Intercept, GLM.Predictor P_Sex, GLM.Predictor P_Age, GLM.Predictor P_Education]
      narrowCountFold = fmap (fmap (F.rcast @(BR.LocationCols V.++ CatCols V.++ BR.CountCols)))
  cachedCCES_Data <- ccesDataLoader
  ccesFrame <- K.ignoreCacheTime cachedCCES_Data
  logFrame $ F.filterFrame (\r -> F.rgetField @Year r == 2018 && F.rgetField @StateAbbreviation r == "WY") ccesFrame
  predsByLocation <-  K.ignoreCacheTimeM $ K.retrieveOrMakeTransformed (fmap BR.lhToS) (fmap BR.lhFromS)  "mrp/pools/predsByLocation" cachedCCES_Data $ \ccesData -> 
    P.raise (BR.predictionsByLocation @CatCols GLM.MDVNone ccesData (narrowCountFold countDemPres2016VotesF) preds BR.catPredMaps)
    

  K.logLE K.Diagnostic $ T.pack $ show predsByLocation  
  brAddMarkDown brText1
  let dvpv x = 2*x - 1
      melt (BR.LocationHolder n _ cdM) = fmap (\(ck, x) -> (n,unCatKey ck, dvpv x)) $ M.toList cdM 
      longPrefs = concat $ fmap melt predsByLocation
      sortedStates sex = K.knitMaybe "Error sorting locationHolders" $ do
        let f lh@(BR.LocationHolder n _ cdM) = do
              yng <-  dvpv <$> M.lookup (catKey sex BR.Grad BR.Under) cdM
              old <- dvpv <$> M.lookup (catKey sex BR.Grad BR.EqualOrOver) cdM
              let grp = if yng < 0
                        then -1
                        else if old > 0
                             then 1
                             else 0                              
              return (n, (grp, yng))
        fmap fst . L.sortBy ((compare `on` snd.snd)) <$> traverse f predsByLocation
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
  sortedByYoungWomen <- sortedStates BR.Female
  _ <- K.addHvega Nothing Nothing $
       vlPrefGapByState
       "2016 Presidential Election: Preference Gap Between Older and Younger College Educated Women"
       (FV.ViewConfig 800 800 10)
       sortedByYoungWomen
       BR.Female
       longPrefs       
  brAddMarkDown brText2     
  _ <- K.addHvega Nothing Nothing $
       vlPrefGapByStateBoth
       "2016 Presidential Election: Preference Gap Between Older and Younger College Educated Voters"
       (FV.ViewConfig 800 800 10)
       sortedBGNat
       (L.filter (\(s,_,_) -> s `elem` sortedBGNat) $ longPrefs)
  brAddMarkDown brText3     
       
{-
  brAddRawHtmlTable
    "Democratic Voter Preference (%) by State and Category"
    (BHA.class_ "brTable")
    (colPrefByLocation allCatKeys emphasizeNational)
    predsByLocation
-}
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

vlPrefGapByState :: Foldable f
                 => T.Text
                 -> FV.ViewConfig
                 -> [T.Text]
                 -> BR.Sex
                 -> f (T.Text, (BR.Sex, BR.CollegeGrad, BR.SimpleAge), Double) -> GV.VegaLite
vlPrefGapByState title vc sortedStates sex rows =
  let datRow (n, (s,e,a), p) = GV.dataRow [("State", GV.Str n)
                                          , ("Sex", GV.Str $ T.pack $ show s)
                                          , ("Education", GV.Str $ T.pack $ show e)
                                          , ("Age", GV.Str $ T.pack $ show a)
                                          , ("D VPV", GV.Number p)
                                          ] []
      dat = GV.dataFromRows [] $ concat $ fmap datRow $ FL.fold FL.list rows
      encY = GV.position GV.Y [GV.PName "State", GV.PmType GV.Nominal, GV.PSort [GV.CustomSort $ GV.Strings sortedStates]]      
      encX = GV.position GV.X [GV.PName "D VPV", GV.PmType GV.Quantitative]
      filter = GV.transform . GV.filter (GV.FExpr $ "datum.Sex == '" <> (T.pack $ show sex) <> "' && datum.Education == 'Grad'")
      encDetail = GV.detail [GV.DName "State", GV.DmType GV.Nominal]
      encColor = GV.color [GV.MName "Age", GV.MmType GV.Nominal]
      dotSpec = GV.asSpec [(GV.encoding . encY . encX . encColor) [], GV.mark GV.Point [], filter []]
      lineSpec = GV.asSpec [(GV.encoding . encDetail . encX . encY) [], GV.mark GV.Line [], filter []]
  in
    FV.configuredVegaLite vc [FV.title title ,GV.layer [dotSpec, lineSpec], dat]

-- each state is it's own plot and we facet those
vlPrefGapByStateBoth :: Foldable f
                     => T.Text
                     -> FV.ViewConfig
                     -> [T.Text]
                     -> f (T.Text, (BR.Sex, BR.CollegeGrad, BR.SimpleAge), Double) -> GV.VegaLite
vlPrefGapByStateBoth title vc sortedStates rows =
  let datRow (n, (s,e,a), p) = GV.dataRow [("State", GV.Str n)
                                          , ("Age", GV.Str $ (T.pack $ show a))
                                          , ("Sex",GV.Str $ (T.pack $ show s))
                                          , ("Education", GV.Str $ T.pack $ show e)
                                          , ("D VPV", GV.Number p)
                                          , ("Ref0", GV.Number 0)
                                          ] []
      dat = GV.dataFromRows [] $ concat $ fmap datRow $ FL.fold FL.list rows
      encY = GV.position GV.Y [GV.PName "Sex", GV.PmType GV.Nominal, GV.PAxis [GV.AxTitle ""]]      
      encX = GV.position GV.X [GV.PName "D VPV"
                              , GV.PmType GV.Quantitative
                              , GV.PAxis [GV.AxGrid False]
                              , GV.PScale [GV.SDomain $ GV.DNumbers [-0.5,0.5]]]
      facetFlow = GV.facetFlow [GV.FName "State", GV.FmType GV.Nominal, GV.FSort [GV.CustomSort $ GV.Strings sortedStates] ]
      filter = GV.transform . GV.filter (GV.FExpr $ "datum.Education == 'Grad'")
      encDetail = GV.detail [GV.DName "Sex", GV.DmType GV.Nominal]
      encDColor = GV.color [GV.MName "Age", GV.MmType GV.Nominal]
      encLColor = GV.color [GV.MName "Sex", GV.MmType GV.Nominal]
      encRuleX = GV.position GV.X [GV.PName "Ref0", GV.PmType GV.Quantitative, GV.PAxis [GV.AxTitle "D VPV"]]
      dotPropS = [(GV.encoding . encX . encY . encDColor) [], GV.mark GV.Point [], filter []]
      linePropS = [(GV.encoding . encX . encY . encDetail) [], GV.mark GV.Line [], filter []]
      gridPropS = [(GV.encoding . encRuleX) [], GV.mark GV.Rule [GV.MOpacity 0.05]]
      allPropS = [GV.layer [GV.asSpec dotPropS, GV.asSpec linePropS, GV.asSpec gridPropS]]
  in
    FV.configuredVegaLite vc [FV.title title ,GV.columns 3, GV.specification (GV.asSpec allPropS), facetFlow, dat]

{-
vlVPVDistribution :: Foldable f => T.Text -> FV.ViewConfig -> f (T.Text, (BR.Sex, BR.SimpleEducation, BR.SimpleAge), Double) -> GV.VegaLite
vlVPVDistribution title vc sortedStates rows =
  let datRow (n, (s,e,a), p) = GV.dataRow [("State", GV.Str n)
                                          , ("Age", GV.Str $ (T.pack $ show a))
                                          , ("Sex",GV.Str $ (T.pack $ show s))
                                          , ("Education", GV.Str $ T.pack $ show e)
                                          , ("D VPV", GV.Number p)
                                          ] []
      dat = GV.dataFromRows [] $ concat $ fmap datRow $ FL.fold FL.list rows
-}      
{-
usStatesTopoJSONUrl = "https://cdn.jsdelivr.net/npm/us-atlas@3/states-10m.json"
usStatesAlbersTopoJSONUrl = "https://cdn.jsdelivr.net/npm/us-atlas@3/states-albers-10m.json"

-- Lessons learned
-- The data is the geography
-- transform with lookup (properties.name) to add data
-- to figure out the right lookup name you can just plot the geography (no transform, no encoding) and look at loaded data
-- in vega editor
vlPctStateChoropleth :: Foldable f => T.Text -> FV.ViewConfig -> f (T.Text, Double) -> GV.VegaLite
vlPctStateChoropleth title vc stateData =
  let datGeo = GV.dataFromUrl usStatesTopoJSONUrl [GV.TopojsonFeature "states"]
      datVal = GV.dataFromRows [] $ concat $ fmap (\(s,x) -> GV.dataRow [("state", GV.Str s),("value", GV.Number x)] []) $ FL.fold FL.list stateData
      dataSets = GV.datasets [("stateNums",datVal)]
      projection = GV.projection [GV.PrType GV.AlbersUsa]
      transform = GV.transform . GV.lookup "properties.name" datVal "state" ["state","value"]
      mark = GV.mark GV.Geoshape []
      colorEnc = GV.color [GV.MName "value", GV.MmType GV.Quantitative, GV.MScale [GV.SScheme "redyellowgreen" [], GV.SDomain (GV.DNumbers [-0.5,0.5])]]
      tooltip = GV.tooltips [[GV.TName "state", GV.TmType GV.Nominal],[GV.TName "value", GV.TmType GV.Quantitative, GV.TFormat ".0%"]]      
      enc = GV.encoding .  colorEnc . tooltip
  in FV.configuredVegaLite vc [FV.title title, datGeo, mark, projection, transform [], enc []]

vlTest :: FV.ViewConfig -> GV.VegaLite
vlTest vc =
  let
    datGeo = GV.dataFromUrl usStatesTopoJSONUrl [GV.TopojsonFeature "states"]
    projection = GV.projection [GV.PrType GV.AlbersUsa]
    mark = GV.mark GV.Geoshape [GV.MFill "lightgrey" ]
  in FV.configuredVegaLite vc [datGeo, mark, projection]
-}


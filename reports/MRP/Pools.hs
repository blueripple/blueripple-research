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
import           Control.Monad (join)
import qualified Data.Array                    as A
import           Data.Function (on)
import qualified Data.List as L
import qualified Data.Set as S
import qualified Data.Map                      as M
import           Data.Maybe (isJust)
import           Data.Proxy (Proxy(..))
--import  Data.Ord (Compare)

import qualified Data.Text                     as T
import qualified Data.Serialize                as SE
import qualified Data.Vector.Storable               as VS


import           Graphics.Vega.VegaLite.Configuration as FV
import qualified Graphics.Vega.VegaLite.Compat as FV
import qualified Frames as F
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V

import qualified Frames.Transform              as FT
import qualified Frames.Folds                  as FF
import qualified Frames.MapReduce              as MR
import qualified Frames.Enumerations           as FE
import qualified Frames.Utils                  as FU
import qualified Frames.Serialize              as FS

import qualified Frames.Visualization.VegaLite.Data
                                               as FV
import qualified Frames.Visualization.VegaLite.ParameterPlots
                                               as FV                                               

import qualified Graphics.Vega.VegaLite        as GV
import qualified Knit.Report                   as K
import qualified Polysemy.Error                as P (mapError)
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
import           Numeric.GLM.MixedModel        as GLM
import qualified Numeric.GLM.Bootstrap            as GLM
import qualified Numeric.GLM.Report            as GLM
import qualified Numeric.GLM.Predict            as GLM
import qualified Numeric.GLM.Confidence            as GLM
import qualified Numeric.SparseDenseConversions as SD

import qualified Statistics.Types              as ST
import GHC.Generics (Generic)


import BlueRipple.Data.DataFrames
import qualified BlueRipple.Data.DemographicTypes as BR
import qualified BlueRipple.Data.PrefModel as BR
import qualified BlueRipple.Data.PrefModel.SimpleAgeSexEducation as BR
import MRP.Common
import MRP.CCES

import qualified PreferenceModel.Common as PrefModel

brText1 :: T.Text
brText1 = [i|
One benefit of working on Blue Ripple Politics is that we get
to talk with very smart analysts who point
us in interesting directions.  A recent conversation with one of our favorite election modelers,
Professor Rachel Bitecofer (LINK), reminded us of her spot-on analysis (LINK) of the 2018
house races, one which focused on turnout among "pools of untapped Democratic voters" (CHECK QUOTE).
In this post we look a bit at one of the pools that Professor Bitecofer is focused on in the 2020
elections: college educated voters. 

1. **Data and Methods**
2. **How Many Votes is a New Voter Worth?**
3. **Female College Educated Voters and Age**
4. **Battleground States**
5. **What Does It Mean?**
6. **Take Action**

## Data and Methods
In our research pieces so far, we've looked only at aggregate data, that
is data which comes from adding together a large number of people: census
counts or election results, for example.  In this piece we look at some
per-person data, namely the Cooperative Congressional Election Study
[CCES][CCES],
which surveys about 60,000 people
in every election cycle. For each survey response,
the CCES includes geographic and demographic information along with opinion about
various political questions, whether the person is registered to vote, and whether they
voted and who for in elections for Governor, House, Senate and President, whenever
each is applicable. 

The geographic information allows
us to start estimating a variety of things at the state level, something we couldn't do using
only aggregate data.  We do this using multi-level regression
(the "MR" of "MRP", which stands for Multi-level Regression with Post-stratification), a technique
explained in more detail [here][MRP:Summary] and about which we will have an explainer in the
coming months. Very briefly: MR allows you to use all the data to get a baseline and then use
the data in a specific "bucket" (e.g., college educated older women in Texas) to make an improved
inference for that set of people; it balances the abundance of the data for everywhere with the need to
use the local information for improved insight.

Our specific model uses a survey-weighted logistic MR to fit a
binomial distribution to the Democratic votes, among voters who voted D or R,
in each 408 groups:
(states + DC) x (Female or Male) x (Non-College Grad or College Grad) x (Under 45 or 45 and over).

As we've said in earlier posts, we recognize that these categories are vast oversimplifications
of the electorate.  We are limited by what information is present in the survey--Gender is limited
to "Female" and "Male" in the CCES, and by computational complexity, which is why we've limited
our education and age breakdown to two categories each.

If we are hoping to boost turnout among Democratic
leaning voters in battleground states or crucial races, it helps to know which voters are
most likely to be Democratic leaning *in that particular state or district.*  This data
and these techniques allow us to do just that.

## How Many Votes is a New Voter Worth?
According to our model, young college-educated women in Texas voted Democratic in the 2016
presidential election about 72% of the time.  If we increased turnout among them by 100,000
voters for 2020 and they were exactly as likely to vote Democratic as in 2016,
how many votes would that extar 100,000 voters net the Democratic candidate?
Of that 100,000, 72,000 (72% x 100,000)
would vote for the democrat and 38,000 ((100% - 72%) x 100,000) for the Republican.
So Dems would net 72,000 - 38,000 = 34,000 votes. In general, if $x\\%$ of voters will vote for
the Democrat, each such voter is "worth" $2x-100\\%$ votes.  We'll call that number "Votes Per Voter" or
VPV. Note that a group with a voter preference of 50% has a VPV of 0. A group that votes
Democratic 60% of the time has a VPV of 20%. And a group which votes for Democrats less
than 50% of the time has a negative VPV.

As a side note, this is why changing people's minds can seem a more appealing avenue to getting
votes: changing a Republican vote to a Democratic vote has a VPV of 2 votes (200%):
the one lost by the Republican and the one gained by the Democrat.

## Female College Educated Voters and Age
So let's look at college-educated women and how their VPV varies by state and by age.
In the chart below we show the VPV of college-educated women, split by age at 45, for
each state (and DC and the Nation as a whole).  The states are ordered by the VPV of
young college-educated women.

[CCES]: <https://cces.gov.harvard.edu/>
[MRP:Summary]: <https://en.wikipedia.org/wiki/Multilevel_regression_with_poststratification>
[MRP:Methods]: <${brGithubUrl (postPath PostMethods)}>
[PrefModel:WWCV]: <${brGithubUrl (PrefModel.postPath PrefModel.PostWWCV)}>
[PrefModel:AcrossTime]: <${brGithubUrl (PrefModel.postPath PrefModel.PostAcrossTime)}>
|]
  
brText2 :: T.Text
brText2 = [i|
Some quick observations:

- Except in PA and NC, college-educated women under 45
are more likely than their older counterparts to vote for Democrats.
- The VPV, even of college-educated women under 45, is widely distributed, from under 10% in Indiana,
to almost 65% in California.
- The spread varies *a lot*: From an almost 50% VPV spread in Texas to almost no spread at all
in North Carolina.
- In a few states (TX, SD, AZ, UT, AR, SC, AL, ND), college-educated women 
over 45 have negative VPV.

Those last two observations lead to a tactical suggestion:  anyone working on
registration or GOTV of college-educated women in Texas, ought to skew their efforts
sharply toward younger voters.  Note: We strongly believe that everyone should
vote and that all states should make that easy.  But if we were spending money
or time on that effort in Texas in 2020, we would target *younger* college-educated
women (and men, as we'll see below).

Texas is interesting, maybe as a long-shot battleground in the presidential race, but
more because there are many house seats in play.  The presidential battleground
states are all worth focusing on here, and below we chart just those.  We've added men
here as well, in order to clarify why people focus on college-educated *women* as a good
source of Democratic votes.


[PrefModel:WWCV]: <${brGithubUrl (PrefModel.postPath PrefModel.PostWWCV)}>
[PrefModel:AcrossTime]: <${brGithubUrl (PrefModel.postPath PrefModel.PostAcrossTime)}>
|]

brText3 :: T.Text
brText3 = [i|
Looking at each of these states, it seems clear that a battleground state turnout drive
focused on all college-educated voters should skew young, especially one that targets men
as well as women.  In all the battleground states except PA, college-educated men over 45
have negative VPV.
|]
  

glmErrorToPandocError :: GLM.GLMError -> PE.PandocError
glmErrorToPandocError x = PE.PandocSomeError $ show x

type LocationCols = '[StateAbbreviation]
locKeyPretty :: F.Record LocationCols -> T.Text
locKeyPretty r =
  let stateAbbr = F.rgetField @StateAbbreviation r
  in stateAbbr

type CatCols = '[Sex, SimpleEducation, SimpleAge]
catKey :: BR.Sex -> BR.SimpleEducation -> BR.SimpleAge -> F.Record CatCols
catKey s e a = s F.&: e F.&: a F.&: V.RNil

unCatKey :: F.Record CatCols -> (BR.Sex, BR.SimpleEducation, BR.SimpleAge)
unCatKey r =
  let s = F.rgetField @Sex r
      e = F.rgetField @SimpleEducation r
      a = F.rgetField @SimpleAge r
  in (s,e,a)

predMap :: F.Record CatCols -> M.Map CCESPredictor Double
predMap r = M.fromList [(P_Sex, if F.rgetField @Sex r == BR.Female then 0 else 1)
--                       ,(P_Race, if F.rgetField @WhiteNonHispanic r == True then 1 else 0)
                       ,(P_Education, if F.rgetField @SimpleEducation r == BR.NonGrad then 0 else 1)
                       ,(P_Age, if F.rgetField @SimpleAge r == BR.Old then 0 else 1)
                       ]
allCatKeys = [catKey s e a | a <- [BR.Old, BR.Young], e <- [BR.NonGrad, BR.Grad], s <- [BR.Female, BR.Male]]
catPredMaps = M.fromList $ fmap (\k -> (k,predMap k)) allCatKeys

catKeyColHeader :: F.Record CatCols -> T.Text
catKeyColHeader r =
  let g = T.pack $ show $ F.rgetField @Sex r
--      wnh = if F.rgetField @WhiteNonHispanic r then "White" else "NonWhite"
      a = T.pack $ show $ F.rgetField @SimpleAge r
      e = T.pack $ show $ F.rgetField @SimpleEducation r
  in a <> "-" <> e <> "-" <> g


type GroupCols = LocationCols V.++ CatCols --StateAbbreviation, Gender] -- this is always location ++ Categories
type MRGroup = Proxy GroupCols 

  
post :: (K.KnitOne r, K.Member GLM.RandomFu r, K.Member GLM.Async r)
     => M.Map T.Text T.Text -- state names from state abbreviations
     -> K.CachedRunnable r [F.Record CCES_MRP]
     -> K.Sem r ()
post stateNameByAbbreviation ccesRecordListAllCR = P.mapError glmErrorToPandocError $ K.wrapPrefix "Intro" $ do
  K.logLE K.Info $ "Working on Intro post..."
{-  
  let (BR.DemographicStructure processDemoData processTurnoutData _ _) = BR.simpleAgeSexEducation  
      makeASEDemographics y aseDemoFrame = do
        knitX $ processDemoData y aseDemoFrame
      makeASETurnout y aseTurnoutFrame = do
        knitX $ processTurnoutData y
-}
  let isWWC r = (F.rgetField @SimpleRace r == BR.White) && (F.rgetField @SimpleEducation r == BR.NonGrad)
{-      countDemHouseVotesF = MR.concatFold
                            $ weightedCountFold @ByCCESPredictors @CCES_MRP @'[HouseVoteParty,CCESWeightCumulative]
                            ((== VP_Democratic) . F.rgetField @HouseVoteParty)
                            (F.rgetField @CCESWeightCumulative) -}
                
      countDemPres2016VotesF = MR.concatFold
                               $ weightedCountFold @ByCCESPredictors @CCES_MRP @'[Pres2016VoteParty,CCESWeightCumulative]
                               ((== VP_Democratic) . F.rgetField @Pres2016VoteParty)
                               (F.rgetField @CCESWeightCumulative)
      inferMR :: (K.KnitOne r, K.Member GLM.RandomFu r, K.Member GLM.Async r)
              => FL.Fold (F.Record CCES_MRP) (F.FrameRec (ByCCESPredictors V.++ '[Count, WeightedSuccesses, MeanWeight, VarWeight]))
              -> Int
              -> F.FrameRec CCES_MRP
              -> K.Sem r (GLM.MixedModel CCESPredictor MRGroup
                         , GLM.RowClassifier MRGroup
                         , GLM.EffectsByGroup MRGroup CCESPredictor
                         , GLM.BetaU
                         , VS.Vector Double
                         , [(GLM.BetaU, VS.Vector Double)]) 
      inferMR cf y ccesFrameAll = P.mapError glmErrorToPandocError $ K.wrapPrefix ("inferMR " <> (T.pack $ show y) <> ":") $ do
        let recFilter r =
              let party = F.rgetField @Pres2016VoteParty r
              in (F.rgetField @Turnout r == T_Voted) && (F.rgetField @Year r == y) && ((party == VP_Republican) || (party == VP_Democratic))
            ccesFrame = F.filterFrame recFilter ccesFrameAll
            counted = FL.fold FL.list $ FL.fold cf (fmap F.rcast ccesFrame)
            vCounts  = VS.fromList $ fmap (F.rgetField @Count) counted
            designEffect mw vw = 1 + (vw / (mw * mw))
            vWeights  = VS.fromList $ fmap (\r ->
                                               let mw = F.rgetField @MeanWeight r
                                                   vw = F.rgetField @VarWeight r
                                               in 1 / sqrt (designEffect mw vw) 
                                           ) counted -- VS.replicate (VS.length vCounts) 1.0
            fixedEffects = GLM.FixedEffects $ IS.fromList [GLM.Intercept
                                                          , GLM.Predictor P_Sex
                                                          , GLM.Predictor P_Age
                                                          , GLM.Predictor P_Education
                                                          ]
            groups = IS.fromList [Proxy]
            (observations, fixedEffectsModelMatrix, rcM) = FL.fold
              (lmePrepFrame getFractionWeighted fixedEffects groups ccesPredictor (recordToGroupKey @GroupCols)) counted
            regressionModelSpec = GLM.RegressionModelSpec fixedEffects fixedEffectsModelMatrix observations
        rowClassifier <- case rcM of
          Left msg -> K.knitError msg
          Right x -> return x
        let effectsByGroup = M.fromList [(Proxy, IS.fromList [GLM.Intercept])]
        fitSpecByGroup <- GLM.fitSpecByGroup fixedEffects effectsByGroup rowClassifier        
        let lmmControls = GLM.LMMControls GLM.LMM_BOBYQA 1e-6
            lmmSpec = GLM.LinearMixedModelSpec (GLM.MixedModelSpec regressionModelSpec fitSpecByGroup) lmmControls
            cc = GLM.PIRLSConvergenceCriterion GLM.PCT_Deviance 1e-6 20
            glmmControls = GLM.GLMMControls GLM.UseCanonical 10 cc
            glmmSpec = GLM.GeneralizedLinearMixedModelSpec lmmSpec vWeights (GLM.Binomial vCounts) glmmControls
            mixedModel = GLM.GeneralizedLinearMixedModel glmmSpec
        randomEffectsModelMatrix <- GLM.makeZ fixedEffectsModelMatrix fitSpecByGroup rowClassifier
        let randomEffectCalc = GLM.RandomEffectCalculated randomEffectsModelMatrix (GLM.makeLambda fitSpecByGroup)
            th0 = GLM.setCovarianceVector fitSpecByGroup 1 0
            mdVerbosity = MDVNone
        GLM.checkProblem mixedModel randomEffectCalc
        K.logLE K.Info "Fitting data..."
        ((th, pd, sigma2, betaU, vb, cs), vMuSol, cf) <- GLM.minimizeDeviance mdVerbosity ML mixedModel randomEffectCalc th0
        GLM.report mixedModel randomEffectsModelMatrix (GLM.bu_vBeta betaU) (SD.toSparseVector vb)          
        let fes = GLM.fixedEffectStatistics mixedModel sigma2 cs betaU
        K.logLE K.Diagnostic $ "FixedEffectStatistics: " <> (T.pack $ show fes)
        epg <- GLM.effectParametersByGroup @MRGroup @CCESPredictor rowClassifier effectsByGroup vb
        K.logLE K.Diagnostic $ "EffectParametersByGroup: " <> (T.pack $ show epg)
        gec <- GLM.effectCovariancesByGroup effectsByGroup mixedModel sigma2 th      
        K.logLE K.Diagnostic $ "EffectCovariancesByGroup: " <> (T.pack $ show gec)
        rebl <- GLM.randomEffectsByLabel epg rowClassifier
        K.logLE K.Diagnostic
          $  "Random Effects:\n"
          <> GLM.printRandomEffectsByLabel rebl
        smCondVar <- GLM.conditionalCovariances mixedModel
                     cf
                     randomEffectCalc
                     th
                     betaU
        let bootstraps = []
{-                     
        K.logLE K.Info "Bootstrappping for confidence intervals..."
        bootstraps <- GLM.parametricBootstrap mdVerbosity
                      ML
                      mixedModel
                      randomEffectCalc
                      cf
                      th
                      vMuSol
                      (sqrt sigma2)
                      200
                      True
-}
        let GLM.FixedEffectStatistics _ mBetaCov = fes

        let f r = do
              let obs = getFractionWeighted r
{-
              bootWCI <- GLM.predictWithCI
                         mixedModel
                         (Just . ccesPredictor r)
                         (Just . recordToGroupKey @GroupCols r)
                         rowClassifier
                         effectsByGroup
                         betaU
                         vb
                         (ST.mkCL 0.95)
                         (GLM.BootstrapCI GLM.BCI_Accelerated bootstraps)
-}                
              predictCVCI <- GLM.predictWithCI
                             mixedModel
                             (Just . ccesPredictor r)
                             (Just . recordToGroupKey @GroupCols r)
                             rowClassifier
                             effectsByGroup
                             betaU
                             vb
                             (ST.mkCL 0.95)
                             (GLM.NaiveCondVarCI mBetaCov smCondVar)

              return (r, obs, predictCVCI)
        fitted <- traverse f (FL.fold FL.list counted)
        K.logLE K.Diagnostic $ "Fitted:\n" <> (T.intercalate "\n" $ fmap (T.pack . show) fitted)
        fixedEffectTable <- GLM.printFixedEffects fes
        K.logLE K.Diagnostic $ "FixedEffects:\n" <> fixedEffectTable
{-        
        let toPredict = [("WWC (all States)", M.fromList [(P_WWC, 1)], M.empty)
                        ,("Non-WWC (all States)", M.fromList [(P_WWC, 0)], M.empty)
                        ]
-}
        let GLM.FixedEffectStatistics fep _ = fes            
        return (mixedModel, rowClassifier, effectsByGroup, betaU, vb, bootstraps) -- fes, epg, rowClassifier, bootstraps)
  let predictionsByLocation = do
        ccesFrameAll <- F.toFrame <$> P.raise (K.useCached ccesRecordListAllCR)
        (mm2016p, rc2016p, ebg2016p, bu2016p, vb2016p, bs2016p) <- inferMR countDemPres2016VotesF 2016 ccesFrameAll
        let states = FL.fold FL.set $ fmap (F.rgetField @StateAbbreviation) ccesFrameAll
            allStateKeys = fmap (\s -> s F.&: V.RNil) $ FL.fold FL.list states            
            predictLoc l = LocationHolder (locKeyPretty l) (Just l) catPredMaps
            toPredict = [LocationHolder "National" Nothing catPredMaps] <> fmap predictLoc allStateKeys                           
            predict (LocationHolder n lkM cpms) = P.mapError glmErrorToPandocError $ do
              let predictFrom catKey predMap =
                    let groupKeyM = lkM >>= \lk -> return $ lk `V.rappend` catKey
                        emptyAsNationalGKM = case groupKeyM of
                          Nothing -> Nothing
                          Just k -> fmap (const k) $ GLM.categoryNumberFromKey rc2016p k Proxy
                    in GLM.predictFromBetaUB mm2016p (flip M.lookup predMap) (const emptyAsNationalGKM) rc2016p ebg2016p bu2016p vb2016p     
              cpreds <- M.traverseWithKey predictFrom cpms
              return $ LocationHolder n lkM cpreds
        traverse predict toPredict
  predsByLocation <-  K.retrieveOrMakeTransformed (fmap lhToS) (fmap lhFromS)  "mrp/pools/predsByLocation" predictionsByLocation
  
{-
  forWWCChart <-  GLM.eitherToSem $ traverse (\tr -> do
                                            let sa = stateAbbr tr
                                            fullState <- maybe (Left $ "Couldn't find " <> sa) Right $ M.lookup sa stateNameByAbbreviation
                                            return (fullState, significantDeltaHouse tr)) $ filter (\tr -> (stateAbbr tr) /= "National") forWWCTable
_ <- K.addHvega Nothing Nothing $ (vlPctStateChoropleth "Significant Change in WWC Dem Voter Preference 2016 to 2018" (FV.ViewConfig 800 400 10) forWWCChart)
-}
  K.logLE K.Diagnostic $ T.pack $ show predsByLocation  
  brAddMarkDown brText1
  let dvpv x = 2*x - 1
      melt (LocationHolder n _ cdM) = fmap (\(ck, x) -> (n,unCatKey ck, dvpv x)) $ M.toList cdM 
      longPrefs = concat $ fmap melt predsByLocation
      sortedStates sex = K.knitMaybe "Error sorting locationHolders" $ do
        let f lh@(LocationHolder n _ cdM) = do
              yng <-  dvpv <$> M.lookup (catKey sex BR.Grad BR.Young) cdM
              old <- dvpv <$> M.lookup (catKey sex BR.Grad BR.Old) cdM
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
  sortedByYoungMen <- sortedStates BR.Male
  _ <- K.addHvega Nothing Nothing $
       vlPrefGapByState
       "2016 Presidential Election: Preference Gap Between Older and Younger College Educated Men"
       (FV.ViewConfig 800 800 10)       
       sortedByYoungMen
       BR.Male
       longPrefs

  brAddRawHtmlTable
    "Democratic Voter Preference (%) by State and Category"
    (BHA.class_ "brTable")
    (colPrefByLocation allCatKeys emphasizeNational)
    predsByLocation
-}
  brAddMarkDown brReadMore

-- TODO: make this traversable
data  LocationHolder f a =  LocationHolder { locName :: T.Text
                                           , locKey :: Maybe (F.Rec f LocationCols)
                                           , catData :: M.Map (F.Rec f CatCols) a
                                           } deriving (Generic)


educationGap :: BR.Sex -> BR.SimpleAge -> LocationHolder F.ElField Double -> Maybe (Double, Double)
educationGap s a (LocationHolder _ _ cd) = do  
  datGrad <- M.lookup (catKey s BR.Grad a) cd
  datNonGrad <- M.lookup (catKey s BR.NonGrad a) cd
  return (datNonGrad, datGrad)

ageGap :: BR.Sex -> BR.SimpleEducation -> LocationHolder F.ElField Double -> Maybe (Double, Double)
ageGap s e (LocationHolder _ _ cd) = do  
  datYoung <- M.lookup (catKey s e BR.Young) cd
  datOld <- M.lookup (catKey s e BR.Old) cd
  return (datOld, datYoung)


deriving instance Show a => Show (LocationHolder F.ElField a)
                  
instance SE.Serialize a => SE.Serialize (LocationHolder FS.SElField a)

lhToS :: LocationHolder F.ElField a -> LocationHolder FS.SElField a
lhToS (LocationHolder n lkM cdm) = LocationHolder n (fmap FS.toS lkM) (M.mapKeys FS.toS cdm)

lhFromS :: LocationHolder FS.SElField a -> LocationHolder F.ElField a
lhFromS (LocationHolder n lkM cdm) = LocationHolder n (fmap FS.fromS lkM) (M.mapKeys FS.fromS cdm)

                
--emphasizeStates s = CellStyle (\tr _ -> if inStates s tr then highlightCellBlue else "")
emphasizeNational = CellStyle (\x _ -> if  locName x == "National" then highlightCellPurple else "")

significantGivenCI :: (Double, Double) -> (Double, Double) -> Double
significantGivenCI (loA, hiA) (loB, hiB) =
  let maxLo = max loA loB
      minHi = min hiA hiB
      overlap = max 0 (minHi - maxLo)
      shortest = min (hiA - loA) (hiB - loB)
  in overlap / shortest
                    
colPrefByLocation
  :: [F.Record CatCols]
  -> CellStyle (LocationHolder F.ElField Double) T.Text
  -> C.Colonnade C.Headed (LocationHolder F.ElField Double) BC.Cell
colPrefByLocation cats cas =
  let h = catKeyColHeader
      hc c = BC.Cell (BHA.class_ "brTableHeader") $ BH.toHtml c
      rowFromCatKey :: F.Record CatCols -> C.Colonnade C.Headed (LocationHolder F.ElField Double) BC.Cell
      rowFromCatKey r =
        C.headed (hc $ h r) (toCell cas (h r) (h r) (maybeNumberToStyledHtml "%2.1f" . fmap (*100) . M.lookup r . catData))
  in C.headed "Location" (toCell cas "Location" "Location" (textToStyledHtml . locName))
     <> mconcat (fmap rowFromCatKey cats)

vlPrefGapByState :: Foldable f => T.Text -> FV.ViewConfig -> [T.Text] -> BR.Sex -> f (T.Text, (BR.Sex, BR.SimpleEducation, BR.SimpleAge), Double) -> GV.VegaLite
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
vlPrefGapByStateBoth :: Foldable f => T.Text -> FV.ViewConfig -> [T.Text] -> f (T.Text, (BR.Sex, BR.SimpleEducation, BR.SimpleAge), Double) -> GV.VegaLite
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


vlVPVDistribution :: Foldable f => T.Text -> FV.ViewConfig -> f (T.Text, (BR.Sex, BR.SimpleEducation, BR.SimpleAge), Double) -> GV.VegaLite
vlVPVDistribution title vc sortedStates rows =
  let datRow (n, (s,e,a), p) = GV.dataRow [("State", GV.Str n)
                                          , ("Age", GV.Str $ (T.pack $ show a))
                                          , ("Sex",GV.Str $ (T.pack $ show s))
                                          , ("Education", GV.Str $ T.pack $ show e)
                                          , ("D VPV", GV.Number p)
                                          ] []
      dat = GV.dataFromRows [] $ concat $ fmap datRow $ FL.fold FL.list rows
      
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

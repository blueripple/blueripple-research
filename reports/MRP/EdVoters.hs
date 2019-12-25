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

module MRP.EdVoters (post) where

import qualified Control.Foldl                 as FL
import           Control.Monad (join)
import qualified Data.Array                    as A
import           Data.Function (on)
import qualified Data.List as L
import qualified Data.Set as S
import qualified Data.Map                      as M
import           Data.Maybe (isJust, catMaybes)
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


import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Data.DemographicTypes as BR
import qualified BlueRipple.Data.PrefModel as BR
import qualified BlueRipple.Data.PrefModel.SimpleAgeSexEducation as BR
import qualified BlueRipple.Utilities.KnitUtils as BR
import MRP.Common
import MRP.CCES

import qualified PreferenceModel.Common as PrefModel

brText1 :: T.Text
brText1 = [i|
In our [last research post][BR:Pools] we looked at college-educated-voter preference, measured
by "Votes Per Voter" ([VPV][BR:Pools:VPV]). VPV is
a number which captures the value of boosting turnout
among some group of voters. In that post we looked exclusively at the 2016 presidential
election.  Here we broaden our view, looking at the 2016 house races to shed some light
on the "Trump effect" among voters and then the 2018 house races to consider how the
electorate might be shifting since 2016.

1. **Review: MRP and VPV**
2. **2016: Presidential Race vs. House Races**
3. **2018 House Races vs. 2016 House Races**
4. **List 4**
5. **List 5**


##MRP and VPV
To recap:  we have data from the [CCES][CCES] survey which gives us information about
individual voters: their location and reported votes for president in 2016 and the house
in 2016 and 2018.  We use a technique called Multi-level Regression (MR) to combine the local
data (e.g., college-educated female voters over 45 in a particular state) with the data for
the same group in all the states in a sensible way and estimate Democratic voter preference
of each group in each state.  From these numbers we compute VPV, which makes the value of
turnout among these voters a bit clearer than voter preference.  In particular, a group which
is equally vote for the Democrat or the Republican in an election has a VPV of 0.

When we want o combine group estimates into an estimate of VPV for a congressional
district or state, we "Post-stratify" (the "P" in MRP) them, which means weighting them
by the population (from the census, e.g.) of that group in that region.
In the post-stratified charts
below we post-stratify using both the Voting Age Populaiton (VAP) and the number of votes
(VAP * turnout for each group).

##2016: Presidential Race vs. House Races
Though Trump won in 2016, it turns out he was almost uniformly less popular
among college-educated voters than whatever Republican house candidate was running.
In other words, in many states, college-educated voters were more likely to vote for Clinton
for president than for the Dem in their local house race.
Below we plot the Democratic VPV of the 2016 house
vote vs. the 2016 presidential vote for each state. Each dot is a particular group
(divided by Sex and Age) in a particular state.  There are separate charts for voters
who graduated from college and voters who did not. A group which was more likely to vote
for the Dem in a house race has a dot further to the right and a group which was more likely
to vote for Clinton in the presidential race has a dot which is further up. Dots to the left of the
diagonal line are groups which were more Dem leaning in the presidential voting than in their
house voting and vice-versa for dots to the right.

A few things are immediately clear:

- A group with high odds of voting for a Democrat in a house race
is also likely to have preferred Clinton. This is exactly what we would expect.
- Voters under 45 (orange and light blue circles) are more likely to vote for Democrats than
their older counterparts.
- Among college graduates, most voters--and almost all voters under 45--were more Dem leaning
in the presidential race than in their local house races.
- Conversely, among non-college graduates, voters over 45 were more Republican leaning in their
votes for president than for their local house candidates.


[CCES]: <https://cces.gov.harvard.edu/>
[MRP:Summary]: <https://en.wikipedia.org/wiki/Multilevel_regression_with_poststratification>
[MRP:Methods]: <${brGithubUrl (postPath PostMethods)}>
[BR:Pools]: <${brGithubUrl (postPath PostPools)}>
[BR:Pools:VPV]: <${brGithubUrl (postPath PostPools)}/#VPV>
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
Another way to look at this data is to plot the difference of VPV in the presidential election
and the house election for different groups and overlay that on a map. Looking at just college
graduates, we create a ["choropleth map"][Wikipedia:choropleth] for each combination of
sex and age in the previous chart.

In each of these maps, bluer indicates more Democratic presidential voting than
house voting and green the opposite.  Here we also see that college-educated
women under 45 had the biggest gap between their presidential voting and house
voting and that this effect was fairly unifo


[Wikipedia:choropleth]: <https://en.wikipedia.org/wiki/Choropleth_map>
|]
  
brEnd :: T.Text
brEnd = [i|

## Take Action
|]
  

glmErrorToPandocError :: GLM.GLMError -> PE.PandocError
glmErrorToPandocError x = PE.PandocSomeError $ T.pack $ show x

type LocationCols = '[BR.StateAbbreviation]
locKeyPretty :: F.Record LocationCols -> T.Text
locKeyPretty r =
  let stateAbbr = F.rgetField @BR.StateAbbreviation r
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

simpleASEToCatKey :: BR.SimpleASE -> F.Record CatCols
simpleASEToCatKey BR.OldFemaleNonGrad = catKey BR.Female BR.NonGrad BR.Old
simpleASEToCatKey BR.YoungFemaleNonGrad = catKey BR.Female BR.NonGrad BR.Young
simpleASEToCatKey BR.OldMaleNonGrad = catKey BR.Male BR.NonGrad BR.Old
simpleASEToCatKey BR.YoungMaleNonGrad = catKey BR.Male BR.NonGrad BR.Young
simpleASEToCatKey BR.OldFemaleCollegeGrad = catKey BR.Female BR.Grad BR.Old
simpleASEToCatKey BR.YoungFemaleCollegeGrad = catKey BR.Female BR.Grad BR.Young
simpleASEToCatKey BR.OldMaleCollegeGrad = catKey BR.Male BR.Grad BR.Old
simpleASEToCatKey BR.YoungMaleCollegeGrad = catKey BR.Male BR.Grad BR.Young


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
      a = T.pack $ show $ F.rgetField @SimpleAge r
      e = T.pack $ show $ F.rgetField @SimpleEducation r
  in a <> "-" <> e <> "-" <> g

type DemPref    = "DemPref"    F.:-> Double
type DemVPV     = "DemVPV"     F.:-> Double
type DistrictGeoId = "DistrictGeoId" F.:-> T.Text

data PostStratifiedByT = Voted | VAP deriving (Enum, Bounded, Eq , Ord, Show)
type PostStratifiedBy = "PostStratifiedBy" F.:-> PostStratifiedByT
type instance FI.VectorFor PostStratifiedByT = V.Vector
instance FV.ToVLDataValue (F.ElField PostStratifiedBy) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Str $ T.pack $ show $ V.getField x)

type GroupCols = LocationCols V.++ CatCols --StateAbbreviation, Gender] -- this is always location ++ Categories
type MRGroup = Proxy GroupCols 

  
post :: (K.KnitOne r
        , K.Member GLM.RandomFu r
        , K.Members es r
        , K.Members es1 r
        , K.Members es2 r)
     => F.Frame BR.States  -- state names from state abbreviations
     -> K.Cached es [F.Record CCES_MRP]
     -> K.Cached es1 [BR.ASEDemographics]
     -> K.Cached es2 [BR.TurnoutASE]
     -> K.Sem r ()
post stateCrossWalkFrame ccesRecordListAllCA aseDemoCA aseTurnoutCA = P.mapError glmErrorToPandocError $ K.wrapPrefix "EdVoters" $ do
  K.logLE K.Info $ "Working on EdVoters post..."
  let stateNameByAbbreviation = M.fromList $ fmap (\r -> (F.rgetField @BR.StateAbbreviation r, F.rgetField @BR.StateName r)) $ FL.fold FL.list stateCrossWalkFrame
      isWWC r = (F.rgetField @SimpleRace r == BR.White) && (F.rgetField @SimpleEducation r == BR.NonGrad)
      countDemPres2016VotesF = FMR.concatFold
                               $ weightedCountFold @ByCCESPredictors @CCES_MRP @'[Pres2016VoteParty,CCESWeightCumulative]
                               (\r -> (F.rgetField @Turnout r == T_Voted)
                                      && (F.rgetField @BR.Year r == 2016)
                                      && (F.rgetField @Pres2016VoteParty r `elem` [VP_Republican, VP_Democratic]))
                               ((== VP_Democratic) . F.rgetField @Pres2016VoteParty)
                               (F.rgetField @CCESWeightCumulative)
      countDemHouseVotesF y = FMR.concatFold
                              $ weightedCountFold @ByCCESPredictors @CCES_MRP @'[HouseVoteParty,CCESWeightCumulative]
                              (\r -> (F.rgetField @Turnout r == T_Voted)
                                     && (F.rgetField @BR.Year r == y)
                                     && (F.rgetField @HouseVoteParty r `elem` [VP_Republican, VP_Democratic]))
                              ((== VP_Democratic) . F.rgetField @HouseVoteParty)
                              (F.rgetField @CCESWeightCumulative)                               
      inferMR cf y ccesFrameAll = P.mapError glmErrorToPandocError $ K.wrapPrefix ("inferMR " <> (T.pack $ show y) <> ":") $ do
        let counted = FL.fold FL.list $ FL.fold cf (fmap F.rcast ccesFrameAll)
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
        let GLM.FixedEffectStatistics _ mBetaCov = fes

        let f r = do
              let obs = getFractionWeighted r
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
        let GLM.FixedEffectStatistics fep _ = fes            
        return (mixedModel, rowClassifier, effectsByGroup, betaU, vb, bootstraps) -- fes, epg, rowClassifier, bootstraps)
  let predictionsByLocation countFold y = do
        ccesFrameAll <- F.toFrame <$> P.raise (K.useCached ccesRecordListAllCA)
        (mm2016p, rc2016p, ebg2016p, bu2016p, vb2016p, bs2016p) <- inferMR countFold y ccesFrameAll
        let states = FL.fold FL.set $ fmap (F.rgetField @BR.StateAbbreviation) ccesFrameAll
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
  predsByLocation2016p <-  K.retrieveOrMakeTransformed (fmap lhToS) (fmap lhFromS)  "mrp/pools/predsByLocation" (predictionsByLocation countDemPres2016VotesF 2016)
--  predsByLocation2014h <-  K.retrieveOrMakeTransformed (fmap lhToS) (fmap lhFromS)  "mrp/edVoters/predsByLocation2014h" (predictionsByLocation countDemHouseVotesF 2014)
  predsByLocation2016h <-  K.retrieveOrMakeTransformed (fmap lhToS) (fmap lhFromS)  "mrp/edVoters/predsByLocation2016h" (predictionsByLocation (countDemHouseVotesF 2016) 2016)
  predsByLocation2018h <-  K.retrieveOrMakeTransformed (fmap lhToS) (fmap lhFromS)  "mrp/edVoters/predsByLocation2018h" (predictionsByLocation (countDemHouseVotesF 2018) 2018)

--  K.logLE K.Diagnostic $ T.pack $ show predsByLocation  

{-  
  ccesFrameAll <- F.toFrame <$> P.raise (K.useCached ccesRecordListAllCA)  
  let recFilter r =
        let party = F.rgetField @HouseVoteParty r
        in (F.rgetField @Turnout r == T_Voted) && (F.rgetField @BR.Year r == 2018) && ((party == VP_Republican) || (party == VP_Democratic))
  let countHouse2018Frame = FL.fold countDemHouseVotesF $ F.filterFrame recFilter ccesFrameAll
  K.logLE K.Info $ "\n" <> T.intercalate "\n" (fmap (T.pack . show) $ FL.fold FL.list $ countHouse2018Frame)
-}
  let vpv x = 2*x - 1
      lhToRecsM year office (LocationHolder _ lkM predMap) =
        let addCols p = FT.mutate (const $ FT.recordSingleton @DemPref p) .
                        FT.mutate (const $ FT.recordSingleton @DemVPV (vpv p)) .
                        FT.mutate (const $ FT.recordSingleton @Office office).
                        FT.mutate (const $ FT.recordSingleton @BR.Year year)                        
            g lk = fmap (\(ck,p) -> addCols p (lk `V.rappend` ck )) $ M.toList predMap
        in fmap g lkM
      pblToFrameFM year office = MR.concatFoldM $ MR.mapReduceFoldM
          (MR.UnpackM $ lhToRecsM year office)
          (MR.generalizeAssign $ MR.Assign (\x -> ((),x)))
          (MR.generalizeReduce $ MR.ReduceFold (const FL.list))
      pblToFrame (year, office, pbl) = FL.foldM (pblToFrameFM year office) $ L.filter ((/= "National") . locName) pbl
      
  longFrame <- K.knitMaybe "Failed to make long-frame"
               $ fmap (F.toFrame . concat)
               $ traverse pblToFrame [(2016, President, predsByLocation2016p)
                                     ,(2016, House, predsByLocation2016h)
                                     ,(2018, House, predsByLocation2018h)
                                     ]
  demographicsFrameRaw <- F.toFrame <$> P.raise (K.useCached aseDemoCA)
  turnoutFrameRaw <- F.toFrame <$> P.raise (K.useCached aseTurnoutCA)
  let longFrameWithState = catMaybes $ fmap F.recMaybe $ (F.leftJoin @'[BR.StateAbbreviation]) longFrame stateCrossWalkFrame
  -- Arrange and join demo/turnout
      BR.DemographicStructure pDD pTD _ _ =  BR.simpleAgeSexEducation
      years = [2016,2018]      
      expandCategories = FT.mutate (simpleASEToCatKey . F.rgetField @(BR.DemographicCategory BR.SimpleASE))
      demographicsFrameAdapt y = fmap (FT.mutate (const $ FT.recordSingleton @BR.Year y) . expandCategories) <$> (BR.knitX $ pDD y demographicsFrameRaw)
      turnoutFrameAdapt y = fmap (FT.mutate (const $ FT.recordSingleton @BR.Year y) . expandCategories) <$> (BR.knitX $ pTD y turnoutFrameRaw)
  demographicsFrame <- mconcat <$> traverse demographicsFrameAdapt years    
  turnoutFrame <- mconcat <$> traverse turnoutFrameAdapt years    
  let withDemographicsFrame = catMaybes
                               $ fmap F.recMaybe
                               $ (F.leftJoin @[BR.StateAbbreviation,Sex,SimpleEducation,SimpleAge,BR.Year]) demographicsFrame (F.toFrame longFrameWithState)
      withTurnoutFrame = catMaybes
                         $ fmap F.recMaybe
                         $ (F.leftJoin @[Sex,SimpleEducation,SimpleAge,BR.Year]) (F.toFrame withDemographicsFrame) turnoutFrame
      postStratifyCell :: forall t q.(V.KnownField t, V.Snd t ~ Double)
              => PostStratifiedByT -> (q -> Double) -> (q -> Double) -> FL.Fold q (F.Record '[PostStratifiedBy,t])
      postStratifyCell psb weight count = (\sdw sw -> FT.recordSingleton @PostStratifiedBy psb `V.rappend` FT.recordSingleton @t (sdw/sw))
                                          <$>  FL.premap (\x -> weight x * count x) FL.sum
                                          <*> FL.premap weight FL.sum
      postStratifyF :: forall k cs ts rs . (k F.⊆ rs
                                           , cs F.⊆ rs
                                           , Ord (F.Record k)
                                           , FI.RecVec (k V.++ (PostStratifiedBy ': ts))
                                           
                                          )
                    => FL.Fold (F.Record cs) [F.Record (PostStratifiedBy ': ts)]
                    -> FL.Fold (F.Record rs) (F.FrameRec (k V.++ (PostStratifiedBy ': ts)))
      postStratifyF cellFold = fmap (F.toFrame . concat) $ MR.mapReduceFold
                               MR.noUnpack
                               (FMR.assignKeysAndData @k @cs)
                               (MR.ReduceFold (\k -> fmap (fmap (k `V.rappend`)) cellFold))
      psCellVPVByBothF =  (<>)
                          <$> fmap pure (postStratifyCell @DemVPV VAP (realToFrac . F.rgetField @BR.PopCount) (realToFrac . F.rgetField @DemVPV))
                          <*> fmap pure (postStratifyCell @DemVPV Voted (\r -> realToFrac (F.rgetField @BR.PopCount r) * F.rgetField @BR.VotedPctOfAll r) (realToFrac . F.rgetField @DemVPV))
      psVPVByStateF = postStratifyF @[BR.Year, Office, BR.StateAbbreviation, BR.StateName] @[DemVPV,BR.PopCount,BR.VotedPctOfAll] @'[DemVPV] psCellVPVByBothF 
        
      psVPVByBoth = FL.fold psVPVByStateF withTurnoutFrame
      psVPVByDistrictF = postStratifyF @[BR.Year, Office, BR.StateAbbreviation, BR.StateFIPS, BR.CongressionalDistrict] @[DemVPV, BR.PopCount, BR.VotedPctOfAll] @'[DemVPV] psCellVPVByBothF
--      toDistrictGeoId :: F.Record '[BR.StateFIPS, BR.CongressionalDistrict] -> F.Record '[DistrictGeoId]
      toDistrictGeoId r =
        let sf = F.rgetField @BR.StateFIPS r
            cd = F.rgetField @BR.CongressionalDistrict r
            dgidInt = 100*sf + cd
            dgidText = (if dgidInt < 1000 then "0" else "" ) <> (T.pack $ show dgidInt)
        in FT.recordSingleton @DistrictGeoId dgidText
      psVPVByDistrict = fmap (FT.mutate toDistrictGeoId) $ FL.fold psVPVByDistrictF withTurnoutFrame
      psVPVByDistrictPres2016ByVoted = F.filterFrame (\r -> (F.rgetField @BR.Year r == 2018)
                                                            && (F.rgetField @Office r == House)
                                                            && (F.rgetField @PostStratifiedBy r == Voted)
                                                            && (F.rgetField @BR.StateAbbreviation r == "TX")) psVPVByDistrict
  -- K.logLE K.Info $ T.intercalate "\n" $ fmap (T.pack . show) $ FL.fold FL.list psVPVByDistrictPres2016ByVoted 

  brAddMarkDown brText1
--  _ <- K.addHvega Nothing Nothing $ vlVPVByDistrict "Test" (FV.ViewConfig 800 800 10) (fmap F.rcast psVPVByDistrictPres2016ByVoted)
  _ <- K.addHvega Nothing Nothing $ vlStateScatterVsElection
       "VPV: 2016 House vs. President"
       (FV.ViewConfig 800 800 10)
       ("House 2016", "President 2016")
       (fmap F.rcast longFrame)
  brAddMarkDown brText2
  _ <- K.addHvega Nothing Nothing $ vldVPVByState
    "Change in VPV: President 2016 - House 2016"
    (FV.ViewConfig 400 400 10)
    ("President2016","House2016")
    (fmap F.rcast longFrameWithState) --presMinusHouse2016
  K.addHvega Nothing Nothing
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
  _ <- K.addHvega Nothing Nothing $ vldVPVByState
    "Change in VPV: House 2018 - House 2016"
    (FV.ViewConfig 400 400 10)
    ("House2018","House2016")
    (fmap F.rcast longFrameWithState) -- house2018MinusHouse2016
  K.addHvega Nothing Nothing
    $ vlPostStratScatter
    "States: VPV 2016 House vs 2018 House"
    (FV.ViewConfig 800 800 10)
    ("House 2016","House 2018")
    (fmap F.rcast $ psVPVByBoth)    
  _ <- K.addHvega Nothing Nothing $ vlVPVChoropleth "VPV: College Graduates" (FV.ViewConfig 400 200 10)  $ fmap F.rcast $ withTurnoutFrame 
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


vlPostStratScatter :: Foldable f
                   => T.Text
                   -> FV.ViewConfig
                   -> (T.Text, T.Text)
                   -> f (F.Record [BR.StateAbbreviation, Office, BR.Year, PostStratifiedBy, DemVPV])
                   -> GV.VegaLite
vlPostStratScatter title vc (race1, race2) rows =
  let pivotFold = FV.simplePivotFold @[Office, BR.Year] @'[DemVPV]
        (\keyLabel dataLabel -> dataLabel <> "-" <> keyLabel)
        (\r -> (T.pack $ show $ F.rgetField @Office r)
               <> " "
               <> (T.pack $ show $ F.rgetField @BR.Year r))
        (\r -> [("Dem VPV",GV.Number $ F.rgetField @DemVPV r)])        
      dat = GV.dataFromRows [] $ FV.pivotedRecordsToVLDataRows @'[BR.StateAbbreviation,PostStratifiedBy]
            pivotFold rows
      vpvCol x = "Dem VPV" <> "-" <> x
      encX = GV.position GV.X [GV.PName (vpvCol race1), GV.PmType GV.Quantitative, GV.PAxis [GV.AxTitle race1]]
      encY = GV.position GV.Y [GV.PName (vpvCol race2), GV.PmType GV.Quantitative, GV.PAxis [GV.AxTitle race2]]
      encColor = GV.color [FV.mName @PostStratifiedBy]
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
                         -> f (F.Record [BR.StateAbbreviation, Office, BR.Year, Sex, SimpleEducation, SimpleAge, DemVPV])
                         -> GV.VegaLite
vlStateScatterVsElection title vc@(FV.ViewConfig w h _) (race1, race2) rows = 
  let pivotFold = FV.simplePivotFold @[Office, BR.Year] @'[DemVPV]
        (\keyLabel dataLabel -> dataLabel <> "-" <> keyLabel)
        (\r -> (T.pack $ show $ F.rgetField @Office r)
               <> " "
               <> (T.pack $ show $ F.rgetField @BR.Year r))
        (\r -> [("Dem VPV",GV.Number $ F.rgetField @DemVPV r)])        
      dat = GV.dataFromRows [] $ FV.pivotedRecordsToVLDataRows @'[BR.StateAbbreviation,Sex,SimpleEducation,SimpleAge]
            pivotFold rows
      vpvCol x = "Dem VPV" <> "-" <> x
      encX = GV.position GV.X [GV.PName (vpvCol race1), GV.PmType GV.Quantitative, GV.PAxis [GV.AxTitle race1]]                              
      encY = GV.position GV.Y [GV.PName (vpvCol race2), GV.PmType GV.Quantitative, GV.PAxis [GV.AxTitle race2]]
      encY2 = GV.position GV.Y [GV.PName (vpvCol race1), GV.PmType GV.Quantitative, GV.PAxis [GV.AxTitle ""]]
      encX2 = GV.position GV.X [GV.PName (vpvCol race2), GV.PmType GV.Quantitative, GV.PAxis [GV.AxTitle ""]]
      facet = GV.facet [GV.ColumnBy [FV.fName @SimpleEducation, GV.FmType GV.Nominal]]--, GV.RowBy [GV.FName "Age", GV.FmType GV.Nominal]]
      encFacetRow = GV.row [FV.fName @SimpleEducation, GV.FmType GV.Nominal]
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

vldVPVByState :: Foldable f
                 => T.Text
                 -> FV.ViewConfig
                 -> (T.Text, T.Text)
                 -> f (F.Record [BR.StateName, Office, BR.Year, Sex, SimpleEducation, SimpleAge, DemVPV])
                 -> GV.VegaLite
vldVPVByState title vc (race1, race2) rows =
  let datGeo = GV.dataFromUrl usStatesTopoJSONUrl [GV.TopojsonFeature "states"]
      pivotFold = FV.simplePivotFold @[Office, BR.Year] @'[DemVPV]
                  (\keyLabel dataLabel -> keyLabel)
                  (\r -> (T.pack $ show $ F.rgetField @Office r)
                    <> (T.pack $ show $ F.rgetField @BR.Year r))
                  (\r -> [("Dem VPV",GV.Number $ F.rgetField @DemVPV r)])        
      datVal = GV.dataFromRows [] $ FV.pivotedRecordsToVLDataRows @'[BR.StateName,Sex,SimpleEducation,SimpleAge]
               pivotFold rows
      dataSets = GV.datasets [("stateDat",datVal)]
      encFacetRow = GV.row [FV.fName @Sex, GV.FmType GV.Nominal]
      encFacetCol = GV.column [FV.fName @SimpleAge, GV.FmType GV.Nominal]
      filter = GV.filter (GV.FExpr $ "datum.CollegeGrad == 'Grad'") -- && datum.Election == '2016 President' && datum.Age == 'Young'")
      vpvCol x = x
      calcDiff = GV.calculateAs ("datum." <> vpvCol race1 <> "-" <> "datum." <> vpvCol race2) "Change in VPV"
      projection = GV.projection [GV.PrType GV.AlbersUsa]
      transform2 = GV.transform . GV.lookupAs (FV.colName @BR.StateName) datGeo "properties.name" "geo" . calcDiff . filter
      mark = GV.mark GV.Geoshape []
      colorEnc = GV.color [GV.MName "Change in VPV", GV.MmType GV.Quantitative]--, GV.MScale [GV.SScheme "redyellowgreen" [], GV.SDomain (GV.DNumbers [-0.5,0.5])]]
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
                -> f (F.Record [BR.StateName, BR.StateFIPS, BR.Year, Office, Sex, SimpleAge, SimpleEducation, DemVPV])
                -> GV.VegaLite
vlVPVChoropleth title vc rows =
  let datGeo = GV.dataFromUrl usStatesTopoJSONUrl [GV.TopojsonFeature "states"]
      datVal = FV.recordsToVLData id FV.defaultParse rows
      encFacetRow = GV.row [FV.fName @Office, GV.FmType GV.Nominal]
      encFacetCol = GV.column [FV.fName @SimpleAge, GV.FmType GV.Nominal]
      filter = GV.filter (GV.FExpr $ "datum.CollegeGrad == 'Grad' && datum.Sex == 'Female'")
      projection = GV.projection [GV.PrType GV.AlbersUsa]
      transform2 = GV.transform . GV.lookupAs "StateName" datGeo "properties.name" "geo" . filter
      mark = GV.mark GV.Geoshape []
      colorEnc = GV.color [FV.mName @DemVPV, GV.MmType GV.Quantitative]
      shapeEnc = GV.shape [GV.MName "geo", GV.MmType GV.GeoFeature]
      tooltip = GV.tooltips [[FV.tName @BR.StateName, GV.TmType GV.Nominal],[FV.tName @DemVPV, GV.TmType GV.Quantitative, GV.TFormat ".0%"]]      
      enc = GV.encoding .  colorEnc . shapeEnc . encFacetRow . encFacetCol . tooltip 
--      cSpec = GV.asSpec [datVal, transform2 [], enc [], mark, projection]
  in FV.configuredVegaLite vc [FV.title title,  datVal, transform2 [], enc [], mark, projection]

vlVPVByDistrict :: Foldable f
                => T.Text
                -> FV.ViewConfig
                -> f (F.Record [DistrictGeoId, DemVPV])
                -> GV.VegaLite
vlVPVByDistrict title vc rows =
  let datGeo = GV.dataFromUrl usDistrictsTopoJSONUrl [GV.TopojsonFeature "cd116"]
      datVal = FV.recordsToVLData id FV.defaultParse rows
      projection = GV.projection [GV.PrType GV.AlbersUsa]
      lookup = GV.lookupAs "DistrictGeoId" datGeo "properties.GEOID" "geo"
      mark = GV.mark GV.Geoshape []
      shapeEnc = GV.shape [GV.MName "geo", GV.MmType GV.GeoFeature]
      colorEnc = GV.color [FV.mName @DemVPV, GV.MmType GV.Quantitative]
      enc = GV.encoding . shapeEnc . colorEnc
  in FV.configuredVegaLite vc [FV.title title, datVal, (GV.transform . lookup) [], enc [], mark, projection]


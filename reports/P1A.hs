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

module P1A (p1a) where

import qualified Control.Foldl                 as FL
import qualified Data.Map                      as M
import qualified Data.Array                    as A

import qualified Data.Text                     as T


import           Graphics.Vega.VegaLite.Configuration as FV
import qualified Frames as F

import qualified Frames.Visualization.VegaLite.Data
                                               as FV
import qualified Frames.Visualization.VegaLite.ParameterPlots
                                               as FV                                               

import qualified Knit.Report                   as K

import           Data.String.Here               ( here, i )

import           BlueRipple.Configuration
import           BlueRipple.Utilities.KnitUtils
import           BlueRipple.Data.DataFrames 
import           BlueRipple.Data.PrefModel.SimpleAgeSexRace
import           BlueRipple.Data.PrefModel.SimpleAgeSexEducation
import qualified BlueRipple.Model.Preference as PM

import PrefCommon

brExitPolls :: T.Text
brExitPolls = [i|
The press has various breakdowns of the 2018 exit polling done by
Edison Research.  None split things into the same categories we
looked at in our
**[post on inferred voter preference in 2018 house elections](${brPrefModelUrl brP1Main})**.
But we can merge some of our categories and then compare. For comparison we chose a
[Fox News post](https://www.foxnews.com/midterms-2018/voter-analysis)
because it had the most detailed demographic splits we found.

We see rough agreement but also some very
large discrepancies, particularly around voter preference of men and women
that we have yet to explain. NB: In each of the following charts, perfect agreement
with the exit polls would leave all the dots in the vertical middle of the chart, at 0%.
|]

brExitAR :: T.Text
brExitAR = [i|
After merging men and women, our preference model tracks the exit polls quite well as seen in the chart below.
|]

brExitSR :: T.Text
brExitSR = [i|
A fairly large discrepancy appears when we look at sex and race, merging ages.
Our model infers similar voting preferences for white men and women and non-white
men and women whereas the exit polls show women about 10% more likely to vote
for Democrats. This is illustrated in the chart below.
|]

brExitSE :: T.Text
brExitSE = [i|
An even larger discrepancy appears when we look at sex and education, merging
our age categories.
Our inferred result for male college graduates is a full 15% higher than the
exit polls, while our result for female non-college graduates is almost 10%
below the exit-polls. This is laid out in the chart below.
|]
  
brExitConclusion :: T.Text
brExitConclusion = [i|
We're continuing to investigate these differences and we hope that using other
data and methods we can figure out why this model infers higher Democratic
voter preference for men and lower for women than exit-poll or other post-election
analytics.  We welcome your
ideas, via [email](mailto:adam@blueripplepolitics.org),
[Twitter](${brTwitter}) or
via a [github issue](${brGithub <> "/preference-model/issues"}).
|]


p1a :: K.KnitOne r
   => M.Map Int (PM.PreferenceResults SimpleASR FV.NamedParameterEstimate)
   -> M.Map Int (PM.PreferenceResults SimpleASE FV.NamedParameterEstimate)
   -> F.Frame EdisonExit2018
   -> K.Sem r ()
p1a modeledResultsASR modeledResultsASE edisonExit2018Frame = do
  brAddMarkDown brExitPolls
  let mergedPrefs :: (A.Ix b, Enum b, Bounded b, A.Ix c, Enum c, Bounded c)
                  => PM.PreferenceResults b FV.NamedParameterEstimate
                  -> (A.Array b Double -> A.Array c Double)
                  -> A.Array c Double
      mergedPrefs pr mergeF =
        let voters = PM.totalArrayZipWith (*) (fmap realToFrac $ PM.nationalVoters pr) (PM.nationalTurnout pr)
            mergedVoters = mergeF voters
            dVotes = PM.totalArrayZipWith (*) (fmap (FV.value . FV.pEstimate) $ PM.modeled pr) voters
            mergedDVotes = mergeF dVotes
        in PM.totalArrayZipWith (/) mergedDVotes mergedVoters                
      groupDataMerged :: forall b c r. (Enum b , Bounded b, A.Ix b, Show b
                                       ,Enum c, Bounded c, A.Ix c, Show c, K.KnitOne r)
                      => PM.PreferenceResults b FV.NamedParameterEstimate
                      -> F.Frame EdisonExit2018
                      -> (A.Array b Double -> A.Array c Double)
                      -> K.Sem r [(T.Text, Double, Double)]
      groupDataMerged pr exits mergeF = do
        let allGroups :: [c] = [minBound..maxBound]
            modeledPrefs = mergedPrefs pr mergeF
            exitsRow c = knitEither $
              case FL.fold FL.list (F.filterFrame ((== T.pack (show c)) . F.rgetField @Group) exits) of
                [] -> Left ("No rows in exit-poll data for group=" <> (T.pack $ show c))
                [x] -> Right x
                _ -> Left (">1 in exit-poll data for group=" <> (T.pack $ show c))
            dPrefFromExitsRow r =
              let dFrac = F.rgetField @DemPref r
                  rFrac = F.rgetField @RepPref r
              in dFrac/(dFrac + rFrac)
        exitPrefs <- fmap dPrefFromExitsRow <$> traverse exitsRow allGroups
        return $ zip3 (fmap (T.pack .show) allGroups) (A.elems modeledPrefs) exitPrefs
      withExitsRowBuilder =
        FV.addRowBuilder @'("Group",T.Text) (\(g,_,_) -> g)
        $ FV.addRowBuilder @'("Model Dem Pref",Double) (\(_,mp,_) -> mp)
        $ FV.addRowBuilder @'("ModelvsExit",Double) (\(_,mp,ep) -> mp - ep)
        $ FV.emptyRowBuilder
      compareChart :: (A.Ix b, Enum b, Bounded b, Show b, A.Ix c, Enum c, Bounded c, Show c, K.KnitOne r)
                   => T.Text
                   -> Maybe T.Text
                   -> M.Map Int (PM.PreferenceResults b FV.NamedParameterEstimate)                           
                   -> F.Frame EdisonExit2018
                   -> (A.Array b Double -> A.Array c Double)
                   -> K.Sem r ()
      compareChart title captionM mr exits mergeF = do
        pr <-
          knitMaybe ("Failed to find 2018 in modelResults.")
          $ M.lookup 2018 mr
        gdm <- groupDataMerged pr exits mergeF
        let ecRows = FV.vinylRows withExitsRowBuilder gdm 
        _ <- K.addHvega Nothing captionM $ PM.exitCompareChart title (FV.ViewConfig 650 325 0) ecRows
        return ()
  brAddMarkDown brExitAR
  compareChart "Age and Race" Nothing modeledResultsASR edisonExit2018Frame simpleASR2SimpleAR
  brAddMarkDown brExitSR
  compareChart "Sex and Race" Nothing modeledResultsASR edisonExit2018Frame simpleASR2SimpleSR
  brAddMarkDown brExitSE
  compareChart "Sex and Education" Nothing modeledResultsASE edisonExit2018Frame simpleASE2SimpleSE
  brAddMarkDown brExitConclusion
  brAddMarkDown brReadMore

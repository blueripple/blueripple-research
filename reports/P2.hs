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

module P2 (p2) where

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

import PrefCommon
import Paths_preference_model

p2 :: K.KnitOne r
   => T.Text
--   -> M.Map Int (PM.PreferenceResults SimpleASR FV.NamedParameterEstimate)
--   -> M.Map Int (PM.PreferenceResults SimpleASE FV.NamedParameterEstimate)
   -> K.Sem r ()
p2 assetPath = do
  dataDir <- K.liftKnit getDataDir
  let tweetPath = (T.pack dataDir) <> "/images/WWCV_tweet.png"
  copyAsset tweetPath (brPrefModelLocalPath <> assetPath <> "/images")
  brAddMarkDown $ brP2Intro assetPath

--------------------------------------------------------------------------------
brP2Intro :: T.Text -> T.Text
brP2Intro assetPath = [i|
Democratic strategists are split on how to address "White Working Class" (WWC) voters
(voters without a 4-year college degree): convince them or ignore them?
We think they're both half-right.  WWC voters are too conservative to convince
and too large to ignore.
We think the key is finding overlaps between the issues progressives and WWC voters
care about, thus convincing some WWC voters while also rallying the progressive base.

1. Why Is There Controversy About WWC voters?
2. How Democrats Win In Heavily WWC States and Districts
3. What You Can Do Now To Help

## Why Is There Controversy About White Working Class Voters?
![@sahilkapur tweet](images/WWCV_tweet.png)\_

This tweet generated a fair amount of discussion! What you make of it
hinges entirely on the meaning you attach to "meaningful gains." It's true
that WWC voters are more likely to vote for Republicans than Democrats and that
will likely continue to be true in the near future, certainly in 2020.
But there are a lot of WWC voters! For instance, WWC voters make up about
60% of the electorate in Wisconsin (ref?). If 7% had voted for
Clinton instead of Trump in 2016, Wisconsin would have gone blue.
Michigan (53% WWC) and Pennsylvania (55% WWC) are also states where a 7% shift
in the 2016 WWC vote would have flipped them blue.

Where does this leave us? Clinton barely lost those three states in 2016, so
perhaps we shouldn't worry about WWC voters at all and instead should work
on registration and turnout of the Democratic base. On the other hand, in 2018
those voters swung back toward Democrats in the house races and keeping them at
37% Democratic makes winning in the mid-west, and elsewhere, a whole lot
easier than if they are 30% Democratic.
|]

  

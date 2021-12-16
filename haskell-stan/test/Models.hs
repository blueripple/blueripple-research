{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell     #-}

module Models where

import qualified Stan.ModelBuilder as S
import qualified Stan.ModelBuilder.BuildingBlocks as SBB
import qualified Stan.ModelBuilder.Expressions as SE
import qualified Stan.ModelBuilder.Distributions as SD
import qualified Stan.ModelConfig as SC

import qualified Frames.Streamly.TH as F
import qualified Frames.Streamly.CSV as F

import qualified Knit.Report as K

relDir :: Text = "haskell-stan/" -- change to "" if this becomes it's own project
F.tableTypes "FB_Results" (relDir <> "data/football.csv")
F.tableTypes "FB_Matchups" (relDir <> "data/matchups1.csv")

data HomeField = FavoriteField | UnderdogField

homeField :: F.Record FB_Results -> HomeField
homeField r = if (F.rgetField @Home == 1) then FavoriteField else UnderdogField

homeFieldG :: S.GroupTypeTag HomeField
homeFieldG = S.GroupTypeTag "HomeField"

favoriteG :: S.GroupTypeTag Text
favoriteG = S.GroupTypeTag "Favorite"

underdogG :: S.GroupTypeTag Text
underdogG = S.GroupTypeTag "Underdog"

spread :: F.Record FB_Results -> Double
spread = F.rgetField @Spread

scoreDiff :: F.Record FB_Results -> Double
scoreDiff r = realToFrac (F.rgetField @Favorite r) - realToFrac (F.rgetField @Underdog r)

groupBuilder :: S.StanGroupBuilderM (F.Frame FB_Results, F.Frame FB_Matchups) ()
groupBuilder = do
  resultsData <- SB.addDataSetToGroupBuilder "Results" (SB.ToFoldable fst)
  S.addGroupIndexForDataSet homeFieldG resultsData $ S.makeIndexFromEnum homeField
--  S.addGroupIndexForDataSet favoriteG resultsData $ S.makeIndexFromFoldable show (F.rgetField @FavoriteName) teams
--  S.addGroupIndexForDataSet underdogG resultsData $ S.makeIndexFromFoldable show (F.rgetField @UnderdogName) teams
  return ()


dataAndCodeBuilder :: S.StanBuilderM () (F.Frame FB_Results, F.Frame FB_Matchups) ()
dataAndCodeBuilder = do
  resultsData <- S.dataSetTag @FB_Results "Results"
  spreadsV <- SBB.addRealData resultsData "spr" (Just 0) Nothing spread
  sDiffV <- SBB.addRealData resultsData "diff" (Just 0) Nothing scoreDiff
  let normal x = SD.normal Nothing $ SE.scalar $ show x
      muPrior = normal 5
      sigmaPrior = normal 10
      dist = SD.normalDist
  (muV, sigmaV) <- S.inBlock S.SBParametersBlock $ do
    muV' <- SB.stanDeclare "mu" SB.StanReal ""
    sigmaV' <- SB.stanDeclare "sigma" SB.StanReal ""
    return (muV', sigmaV')
  S.sampleDistV resultsData SD.normalDist (muV, sigmaV) sDiff

extractResults :: K.KnitEffects r => SC.ResultAction r d S.DataSetGroupIntMaps () ([Double], [Double])
extractResults = SC.UseSummary f where
  f summary _ _ = do

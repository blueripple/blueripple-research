{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell     #-}
{-# LANGUAGE TypeApplications     #-}

module Models where

import qualified Stan.ModelBuilder as S
import qualified Stan.ModelBuilder.BuildingBlocks as SBB
import qualified Stan.ModelBuilder.Expressions as SE
import qualified Stan.ModelBuilder.Distributions as SD
import qualified Stan.ModelConfig as SC
import qualified Stan.ModelRunner as SMR
import qualified Stan.Parameters as SP
import qualified CmdStan as CS

import qualified Frames as F hiding (tableTypes)
import qualified Frames.Streamly.TH as F
import qualified Frames.Streamly.CSV as F

import qualified Knit.Report as K

import Control.Lens ((^.), view)
import qualified Stan.ModelRunner as CS

-- remove "haskell-stan" from these paths if this becomes it's own project
F.tableTypes "FB_Result" ("haskell-stan/test/data/football.csv")
F.tableTypes "FB_Matchup" ("haskell-stan/test/data/matchups1.csv")

data HomeField = FavoriteField | UnderdogField deriving (Show, Eq, Ord, Enum, Bounded)

homeField :: FB_Result -> HomeField
homeField r = if r ^. home then FavoriteField else UnderdogField

homeFieldG :: S.GroupTypeTag HomeField = S.GroupTypeTag "HomeField"

favoriteG :: S.GroupTypeTag Text = S.GroupTypeTag "Favorite"

underdogG :: S.GroupTypeTag Text = S.GroupTypeTag "Underdog"

-- spread :: F.Record FB_Results -> Double
-- spread = F.rgetField @Spread

scoreDiff :: FB_Result -> Double
scoreDiff r = realToFrac (r ^. favorite) - realToFrac (r ^. underdog)

spreadDiff :: FB_Result -> Double
spreadDiff r = r ^. spread - scoreDiff r

groupBuilder :: S.StanGroupBuilderM (F.Frame FB_Result, F.Frame FB_Matchup) ()
groupBuilder = do
  resultsData <- S.addDataSetToGroupBuilder "Results" (S.ToFoldable fst)
  S.addGroupIndexForDataSet homeFieldG resultsData $ S.makeIndexFromEnum homeField
--  S.addGroupIndexForDataSet favoriteG resultsData $ S.makeIndexFromFoldable show (F.rgetField @FavoriteName) teams
--  S.addGroupIndexForDataSet underdogG resultsData $ S.makeIndexFromFoldable show (F.rgetField @UnderdogName) teams
  return ()


dataAndCodeBuilder :: S.StanBuilderM () (F.Frame FB_Result, F.Frame FB_Matchup) ()
dataAndCodeBuilder = do
  resultsData <- S.dataSetTag @FB_Result "Results"
  spreadDiffV <- SBB.addRealData resultsData "diff" (Just 0) Nothing spreadDiff
  let normal x = SD.normal Nothing $ SE.scalar $ show x
  (muV, sigmaV) <- S.inBlock S.SBParameters $ do
    muV' <- S.stanDeclare "mu" S.StanReal ""
    sigmaV' <- S.stanDeclare "sigma" S.StanReal ""
    return (muV', sigmaV')
  S.inBlock S.SBModel $ S.addExprLines "priors"
    [
      SE.var muV `SE.eq` normal 5
    , SE.var sigmaV `SE.eq` normal 10
    ]
  SBB.sampleDistV resultsData SD.normalDist (S.var muV, S.var sigmaV) spreadDiffV

-- the getParameter function feels like an incantation.  Need to simplify.
extractResults :: K.KnitEffects r => SC.ResultAction r d S.DataSetGroupIntMaps () ([Double], [Double])
extractResults = SC.UseSummary f where
  f summary _ _ = do
    let getParameter n = K.knitEither $ SP.getScalar . fmap CS.percents <$> SP.parseScalar n (CS.paramStats summary)
    (,) <$> getParameter "mu" <*> getParameter "sigma"

-- This whole thing should be wrapped in the core for this very common variation.
dataWranglerAndCode :: (K.KnitEffects r, Typeable a)
                    => K.ActionWithCacheTime r a --(F.Frame FB_Result, F.Frame FB_Matchup)
                    -> S.StanGroupBuilderM a ()
                    -> S.StanBuilderM () a ()
                    -> K.Sem r (SC.DataWrangler a S.DataSetGroupIntMaps (), S.StanCode)
dataWranglerAndCode data_C gb sb = do
  dat <- K.ignoreCacheTime data_C
  let builderWithWrangler = do
        S.buildGroupIndexes
        sb
        jsonF <- S.buildJSONFromDataM
        intMapsBuilder <- S.intMapsBuilder
        return
          $ SC.Wrangle SC.TransientIndex
          $ \d -> (intMapsBuilder d, jsonF)
      resE = S.runStanBuilder dat () gb builderWithWrangler
  K.knitEither $ fmap (\(bs, dw) -> (dw, S.code bs)) resE

runModel :: (K.KnitEffects r, Typeable a)
         => Bool
         -> Maybe Text
         -> Text
         -> Text
         -> SC.DataWrangler a b ()
         -> S.StanCode
         -> Text
         -> SC.ResultAction r a b () c
         -> K.ActionWithCacheTime r a
         -> K.Sem r (K.ActionWithCacheTime r c)
runModel clearCaches mWorkDir modelName dataName dataWrangler stanCode ppName resultAction data_C =
  K.wrapPrefix "haskell-stan-test.runModel" $ do
  K.logLE K.Info $ "Running: model=" <> modelName <> " using data=" <> dataName
  let workDir = fromMaybe "stan" mWorkDir
      outputLabel = modelName <> "_" <> dataName
      nSamples = 1000
      stancConfig = CS.makeDefaultStancConfig (toString $ workDir <> "/" <> modelName) {CS.useOpenCL = False}
      threadsM = Just (-1)
  stanConfig <-
    SC.setSigFigs 4
    . SC.noLogOfSummary
    <$> SMR.makeDefaultModelRunnerConfig

{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell     #-}
{-# LANGUAGE TypeApplications     #-}

module Models where

import qualified KnitEnvironment as KE

import qualified Stan.ModelBuilder as S
import qualified Stan.ModelBuilder.TypedExpressions.DAG as DAG
import qualified Stan.ModelBuilder.TypedExpressions.Indexing as TE
import qualified Stan.ModelBuilder.TypedExpressions.Types as TE
import qualified Stan.ModelBuilder.TypedExpressions.TypedList as TE
import Stan.ModelBuilder.TypedExpressions.TypedList (TypedList(..))
import qualified Stan.ModelBuilder.TypedExpressions.Expressions as TE
import qualified Stan.ModelBuilder.TypedExpressions.Statements as TE
import qualified Stan.ModelBuilder.TypedExpressions.StanFunctions as TE

import qualified Stan.ModelBuilder.BuildingBlocks as SBB
import qualified Stan.ModelBuilder.Expressions as SE
import qualified Stan.ModelBuilder.Distributions as SD
--import qualified Stan.ModelBuilder.GroupModel as SGM
import qualified Stan.ModelConfig as SC
import qualified Stan.Parameters as SP
import qualified CmdStan as CS

import qualified Frames as F hiding (tableTypes)
import qualified Frames.Streamly.TH as F
import qualified Frames.Streamly.LoadInCore as F
import qualified Frames.Streamly.Streaming.Class as FSC
import qualified Frames.Streamly.Streaming.Streamly as FS

import qualified Knit.Report as K

import qualified Data.IntMap as IM
import qualified Data.Vector as Vec
import Control.Lens ((^.), view)
import qualified Stan.ModelBuilder.TypedExpressions.DAG as SB
import qualified Stan.ModelBuilder.TypedExpressions.DAG as DAG
import Stan.ModelBuilder (groupSizeE)
import qualified Stan.ModelBuilder as TE

-- remove "haskell-stan" from these paths if this becomes it's own project
F.tableTypes "FB_Result" ("haskell-stan/test/data/football.csv")
F.tableTypes "FB_Matchup" ("haskell-stan/test/data/matchups1.csv")


fbResults :: forall r.(K.KnitEffects r, KE.CacheEffects r) => K.Sem r (K.ActionWithCacheTime r (F.Frame FB_Result))
fbResults = do
  let cacheKey :: Text = "data/fbResults.bin"
      fp = "haskell-stan/test/data/football.csv"
  fileDep <- K.fileDependency fp
  sf <- K.retrieveOrMake @KE.SerializerC @KE.CacheData cacheKey fileDep
        $ const
        $ fmap KE.fromFrame
        $ K.liftKnit -- we have to run this in IO since K.Sem r does not support Monad Control
        $ FSC.runSafe @F.DefaultStream
        $ F.loadInCore @F.DefaultStream @IO fB_ResultParser fp Just
  return $ fmap KE.toFrame sf

fbMatchups :: forall r.(K.KnitEffects r, KE.CacheEffects r) => Int -> K.Sem r (K.ActionWithCacheTime r (F.Frame FB_Matchup))
fbMatchups n = do
  let cacheKey :: Text = "data/fbMatchups" <> show n <> ".bin"
      fp = "haskell-stan/test/data/matchups" <> show n <> ".csv"
  fileDep <- K.fileDependency fp
  sf <- K.retrieveOrMake @KE.SerializerC @KE.CacheData cacheKey fileDep
        $ const
        $ fmap KE.fromFrame
        $ K.liftKnit -- we have to run this in IO since K.Sem r does not support Monad Control
        $ FSC.runSafe @F.DefaultStream
        $ F.loadInCore @F.DefaultStream @IO fB_MatchupParser fp Just
  return $ fmap KE.toFrame sf


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

groupBuilder :: Foldable f => f Text -> S.StanGroupBuilderM (F.Frame FB_Result) (F.Frame FB_Matchup) ()
groupBuilder teams = do
  resultsData <- S.addModelDataToGroupBuilder "Results" (S.ToFoldable id)
  S.addGroupIndexForData homeFieldG resultsData $ S.makeIndexFromEnum homeField
  S.addGroupIndexForData favoriteG resultsData $ S.makeIndexFromFoldable show (F.rgetField @FavoriteName) teams
  S.addGroupIntMapForDataSet favoriteG resultsData $ S.dataToIntMapFromFoldable (F.rgetField @FavoriteName) teams
  matchupData <- S.addGQDataToGroupBuilder "Matchups" (S.ToFoldable id)
  S.addGroupIndexForData favoriteG matchupData $ S.makeIndexFromFoldable show (F.rgetField @FavoriteName) teams
--  S.addRowKeyIntMapToGroupBuilder matchupData favoriteG (F.rgetField @FavoriteName)
--  S.addGroupIndexForDataSet underdogG resultsData $ S.makeIndexFromFoldable show (F.rgetField @UnderdogName) teams
  return ()

spreadDiffNormal :: S.StanBuilderM (F.Frame FB_Result) (F.Frame FB_Matchup) ()
spreadDiffNormal = do
  resultsData <- S.dataSetTag @FB_Result SC.ModelData "Results"
  S.setDataSetForBindings resultsData
  spreadDiffE <- SBB.addRealData resultsData "diff" Nothing Nothing spreadDiff
  sigmaMuP <- DAG.addBuildParameter
              $ DAG.simpleParameter
              (TE.NamedDeclSpec "sigma_mu_fav" $ TE.realSpec [TE.lowerM $ TE.realE 0])
              (DAG.GivenP (TE.realE 0) :> DAG.GivenP (TE.realE 3) :> TNil)
              TE.normalDensity
  sigmaP <- DAG.addBuildParameter
         $ DAG.simpleParameter
         (TE.NamedDeclSpec "sigma" $ TE.realSpec [TE.lowerM $ TE.realE 0])
         (DAG.GivenP (TE.realE 13) :> DAG.GivenP (TE.realE 15) :> TNil)
         $ TE.normalDensity
  muVP <- DAG.addBuildParameter
          $ DAG.simpleParameter
          (TE.NamedDeclSpec "mu_fav" $ TE.vectorSpec (S.groupSizeE favoriteG) [])
          (DAG.GivenP (TE.realE 0) :> DAG.BuildP sigmaMuP :> TNil)
         $ TE.normalDensityS
  let toVec x = TE.functionE TE.rep_vector (x :> S.dataSetSizeE resultsData :> TNil)
      vecSigmaP = DAG.mapParameter toVec sigmaP
      indexedMuP = DAG.mapParameter (TE.indexE TE.s0 (TE.byGroupIndexE resultsData favoriteG)) muVP
  S.inBlock S.SBModel
    $ S.addStmtToCode
    $ TE.sample spreadDiffE TE.normalDensity (indexedMuP :> vecSigmaP :> TNil)
--  SBB.sampleDistV resultsData SD.normalDist (S.var mu_favV, S.var sigmaV) spreadDiffV
{-  S.inBlock S.SBGeneratedQuantities $ do
    matchups <- S.dataSetTag @FB_Matchup SC.GQData "Matchups"
    S.addRowKeyIntMap matchups favoriteG (F.rgetField @FavoriteName)
    S.useDataSetForBindings matchups $ do
      S.stanDeclareRHS "eScoreDiff" (S.StanVector $ S.NamedDim "Matchups") ""
        $ SE.vectorizedOne "Matchups"
        $ SD.familyExp SD.normalDist (S.var mu_favV, S.var sigmaV)
      return ()
  SBB.generateLogLikelihood resultsData SD.normalDist (return (S.var mu_favV, S.var sigmaV)) spreadDiffV
-}

-- the getParameter function feels like an incantation.  Need to simplify.
type ModelReturn = ([(Text, [Double])],[Double], [Double],[(Text, [Double])])
normalParamCIs :: K.KnitEffects r => SC.ResultAction r md gq S.DataSetGroupIntMaps () ModelReturn
normalParamCIs = SC.UseSummary f where
  f summary _ modelDataAndIndexes_C mGQDataAndIndexes_C = do
    resultIndexesE <- K.ignoreCacheTime $ fmap snd modelDataAndIndexes_C
    teamResultIM <- K.knitEither
      $  resultIndexesE >>= S.getGroupIndex (S.RowTypeTag @FB_Result SC.ModelData "Results") favoriteG


    gqDataAndIndexes_C <- K.knitMaybe "normalParamCIs: No GQ data/indices provided!" mGQDataAndIndexes_C
    matchupIndexesE <- K.ignoreCacheTime $ fmap snd gqDataAndIndexes_C
    teamMatchupIM <- K.knitEither
                     $ matchupIndexesE >>= S.getGroupIndex (S.RowTypeTag @FB_Matchup SC.GQData "Matchups") favoriteG
    K.logLE K.Diagnostic $ "MatchupIM: " <> show teamMatchupIM
    let resultsTeamList = fmap snd $ IM.toAscList teamResultIM
        matchupsTeamList = fmap snd $ IM.toAscList teamMatchupIM
    let getScalar n = K.knitEither $ SP.getScalar . fmap CS.percents <$> SP.parseScalar n (CS.paramStats summary)
        getVector n = K.knitEither $ SP.getVector . fmap CS.percents <$> SP.parse1D n (CS.paramStats summary)
        addTeams t = fmap (zip t . Vec.toList)
    (,,,) <$> addTeams resultsTeamList (getVector "mu_fav")
      <*> getScalar "sigma_mu_fav"
      <*> getScalar "sigma"
      <*> addTeams matchupsTeamList (getVector "eScoreDiff")

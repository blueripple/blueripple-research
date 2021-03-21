{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-# OPTIONS_GHC  -O0 #-}

module BlueRipple.Model.StanMRP where

import qualified Control.Foldl as FL
import qualified Data.Aeson as A
import qualified Data.Array as Array
import qualified Data.Dependent.HashMap as DHash
import qualified Data.Dependent.Sum as DSum
import qualified Data.IntMap.Strict as IM
import qualified Data.List as List
import Data.List.Extra (nubOrd)
import qualified Data.Map as Map
import qualified Data.Maybe as Maybe
import qualified Data.Set as Set

import qualified Data.Text as T
import qualified Data.Vector as Vec
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V
import qualified Flat
import Flat.Instances.Vector()
import Flat.Instances.Containers()

import Frames.MapReduce (postMapM)
import qualified Control.MapReduce as MR

import qualified BlueRipple.Utilities.KnitUtils as BR
import qualified BlueRipple.Data.Keyed as BK
import qualified BlueRipple.Utilities.KnitUtils as BR

import qualified CmdStan as CS
import qualified CmdStan.Types as CS
import qualified Stan.JSON as SJ
import qualified Stan.Frames as SF
import qualified Stan.Parameters as SP
import qualified Stan.ModelRunner as SM
import qualified Stan.ModelBuilder as SB
import qualified Stan.ModelConfig as SC
import qualified Stan.RScriptBuilder as SR
import qualified System.Environment as Env

import qualified Knit.Report as K
import qualified Knit.Effect.AtomicCache as K hiding (retrieveOrMake)
import Data.String.Here (here)

data MRPData f predRow modeledRow psRow =
  MRPData
  {
    modeled :: f modeledRow
  , postStratified ::Maybe (f psRow) -- if this is Nothing we don't do post-stratification
  }


buildDataWranglerAndCode :: (Typeable d, Typeable modeledRow)
                         => SB.StanGroupBuilderM modeledRow ()
                         -> Binomial_MRP_Model d modeledRow
                         -> SB.StanBuilderM (Binomial_MRP_Model d modeledRow) d modeledRow ()
                         -> d
                         -> SB.ToFoldable d modeledRow --MRData modeledRow predRow
                         -> Either Text (SC.DataWrangler d () (), SB.StanCode)
buildDataWranglerAndCode groupM model builderM d (SB.ToFoldable toFoldable) =
  let builderWithWrangler = do
        jsonRowBuilders <- SB.buildJSONF
        _ <- builderM
        return $ SC.Wrangle SC.TransientIndex $ \d -> ((), flip SB.buildJSONFromRows jsonRowBuilders)
      resE = SB.runStanBuilder d toFoldable model groupM builderWithWrangler
  in fmap (\(SB.BuilderState _ _ c, dw) -> (dw, c)) resE


runMRPModel2 :: (K.KnitEffects r
               , BR.CacheEffects r
               , Flat.Flat c
               , Flat.Flat b
               )
            => Bool
            -> Maybe Text
            -> Text
            -> Text
            -> SC.DataWrangler a b ()
            -> SB.StanCode
            -> Text
            -> SC.ResultAction r a b () c
            -> K.ActionWithCacheTime r a
            -> Maybe Int
            -> K.Sem r ()
runMRPModel2 clearCache mWorkDir modelName dataName dataWrangler stanCode ppName resultAction data_C mNSamples =
  K.wrapPrefix "BlueRipple.Model.StanMRP" $ do
  K.logLE K.Info "Running..."
  let workDir = fromMaybe ("stan/MRP/" <> modelName) mWorkDir
      outputLabel = modelName <> "_" <> dataName
      nSamples = fromMaybe 1000 mNSamples
      stancConfig =
        (SM.makeDefaultStancConfig (T.unpack $ workDir <> "/" <> modelName)) {CS.useOpenCL = False}
  stanConfig <-
    SC.setSigFigs 4
    . SC.noLogOfSummary
    <$> SM.makeDefaultModelRunnerConfig
    workDir
    (modelName <> "_model")
    (Just (SB.All, SB.stanCodeToStanModel stanCode))
    (Just $ dataName <> ".json")
    (Just $ outputLabel)
    4
    (Just nSamples)
    (Just nSamples)
    (Just stancConfig)
  let resultCacheKey = "stan/MRP/result/" <> outputLabel <> ".bin"
  when clearCache $ do
    K.liftKnit $ SM.deleteStaleFiles stanConfig [SM.StaleData]
    BR.clearIfPresentD resultCacheKey
  modelDep <- SM.modelCacheTime stanConfig
  K.logLE K.Diagnostic $ "modelDep: " <> show (K.cacheTime modelDep)
  K.logLE K.Diagnostic $ "houseDataDep: " <> show (K.cacheTime data_C)
  let dataModelDep = const <$> modelDep <*> data_C
      getResults s () inputAndIndex_C = return ()
      unwraps = [SR.UnwrapNamed ppName ppName]
  res_C <- BR.retrieveOrMakeD resultCacheKey dataModelDep $ \() -> do
    K.logLE K.Info "Data or model newer then last cached result. (Re)-running..."
    SM.runModel @BR.SerializerC @BR.CacheData
      stanConfig
      (SM.Both unwraps)
      dataWrangler
      resultAction
      ()
      data_C
  return ()

data ProjectableRows f rowA rowB where
  ProjectableRows :: Functor f => f rowA -> (rowA -> rowB) -> ProjectableRows f rowA rowB
  ProjectedRows :: Functor f => f row -> ProjectableRows f row row

instance Functor (ProjectableRows f rowA) where
  fmap h (ProjectableRows rs g) = ProjectableRows rs (h . g)
  fmap h (ProjectedRows rs) = ProjectableRows rs h

projectableRows :: ProjectableRows f rowA rowB -> f rowA
projectableRows (ProjectableRows rs _) = rs
projectableRows (ProjectedRows rs) = rs

projectRow :: ProjectableRows f rowA rowB -> rowA -> rowB
projectRow (ProjectableRows _ g) = g
projectRow (ProjectedRows _) = id

projectRows :: ProjectableRows f rowA rowB -> ProjectableRows f rowB rowB
projectRows (ProjectableRows rs g) = ProjectedRows $ fmap g rs
projectRows (ProjectedRows rs) = ProjectedRows rs

projectedRows :: ProjectableRows f rowA rowB -> f rowB
projectedRows = projectableRows . projectRows

type MRData f modeledRows predRows = ProjectableRows f modeledRows predRows
type PSData f psRows predRows = ProjectableRows f psRows predRows
type LLData f modeledRows predRows = ProjectableRows f modeledRows predRows

data PostStratification k psRow predRow =
  PostStratification
  {
    psPrj :: psRow -> predRow
  , psWeight ::  psRow -> Double
  , psGroupKey :: psRow -> k
  }

type BuilderM modeledRow d = SB.StanBuilderM (Binomial_MRP_Model d modeledRow) d modeledRow

getModel ::  BuilderM modeledRow d (Binomial_MRP_Model d modeledRow)
getModel = SB.askUserEnv

getIndexes :: BuilderM modeledRow d (Map Text (SB.IntIndex modeledRow))
getIndexes = SB.groupIndexByName <$> SB.askGroupEnv

getIndex :: GroupName -> BuilderM modeledRow d (SB.IntIndex modeledRow)
getIndex gn = do
  indexMap <- getIndexes
  case (Map.lookup gn indexMap) of
    Nothing -> SB.stanBuildError $ "No group index found for group with name=\"" <> gn <> "\""
    Just i -> return i

-- basic group in modeled data. E.g., "J_age"
-- JSON for group indices are produced automatically
groupM :: (Typeable d, Typeable r) => GroupName -> BuilderM r d ()
groupM gn = do
  (SB.IntIndex indexSize _) <- getIndex gn
  SB.addFixedIntJson ("N_" <> gn) (Just 2) indexSize -- JSON: {N_Age : 2}

allGroupsM :: (Typeable d, Typeable r) => BuilderM r d ()
allGroupsM = usedGroupNames >>= traverse_ groupM . Set.toList

type GroupName = Text

data FixedEffects row = FixedEffects Int (row -> Vec.Vector Double)

data Binomial_MRP_Model d modeledRow =
  Binomial_MRP_Model
  {
    bmm_Name :: Text -- we'll need this for (unique) file names
  , bmm_FixedEffects :: DHash.DHashMap (SB.RowTypeTag d) FixedEffects
  , bmm_MRGroups :: Set.Set GroupName
  , bmm_Total :: modeledRow -> Int
  , bmm_Success :: modeledRow -> Int
  }

emptyFixedEffects :: DHash.DHashMap (SB.RowTypeTag d) FixedEffects
emptyFixedEffects = DHash.empty

addFixedEffects :: forall r d. SB.RowTypeTag d r
                -> FixedEffects r
                -> DHash.DHashMap (SB.RowTypeTag d) FixedEffects
                -> DHash.DHashMap (SB.RowTypeTag d) FixedEffects
addFixedEffects rtt fe dhm = DHash.insert rtt fe dhm

feGroupNames :: forall modeledRow d. (Typeable d, Typeable modeledRow) => BuilderM modeledRow d (Set.Set GroupName)
feGroupNames = do
  feMap <- bmm_FixedEffects <$> getModel
  let feGroupMap = DHash.delete (SB.ModeledRowTag @d @modeledRow) feMap
      getName (rtt DSum.:=> _) = SB.dsName rtt
  return $ Set.fromList $ fmap getName $ DHash.toList feGroupMap

usedGroupNames :: (Typeable d, Typeable modeledRow) => BuilderM modeledRow d (Set.Set GroupName)
usedGroupNames = do
  model <- getModel
  feGNames <- feGroupNames
  return $ foldl' (\s gn -> Set.insert gn s) (bmm_MRGroups model) feGNames

checkEnv :: (Typeable d, Typeable modeledRow) => BuilderM modeledRow d ()
checkEnv = do
  model <- getModel
  allGroupNames <- Map.keys <$> getIndexes
  allFEGroupNames <- Set.toList <$> feGroupNames
  let allMRGroupNames = Set.toAscList $ bmm_MRGroups model
      hasAllFEGroups = List.isSubsequenceOf allFEGroupNames  allGroupNames
      hasAllMRGroups = List.isSubsequenceOf allMRGroupNames  allGroupNames
  when (not hasAllFEGroups) $ SB.stanBuildError $ "Missing group data! Given group data for " <> show allGroupNames <> ". FEGroups=" <> show allFEGroupNames
  when (not hasAllMRGroups) $ SB.stanBuildError $ "Missing group data! Given group data for " <> show allGroupNames <> ". MRGroups=" <> show allMRGroupNames
  return ()

type PostStratificationWeight psRow = psRow -> Double

fixedEffectsM :: forall d r r0.(Typeable d, Typeable r0)
              => SB.RowTypeTag d r -> FixedEffects r -> BuilderM r0 d ()
fixedEffectsM rtt (FixedEffects n vecF) = do
  let suffix = SB.dsSuffix rtt
      uSuffix = SB.underscoredIf suffix
  SB.add2dMatrixJson rtt "X" suffix "" (SB.NamedDim $ "N" <> uSuffix) n vecF -- JSON/code declarations for matrix
  SB.fixedEffectsQR uSuffix ("X" <> uSuffix) ("N" <> uSuffix) ("K" <> uSuffix) -- code for parameters and transformed parameters

allFixedEffectsM :: forall r d. (Typeable d, Typeable r) => BuilderM r d ()
allFixedEffectsM = do
  model <- getModel
  let f :: (Typeable d, Typeable r) => DSum.DSum (SB.RowTypeTag d) (FixedEffects) -> BuilderM r d ()
      f (rtt DHash.:=> fe) = fixedEffectsM rtt fe
  traverse_ f $ DHash.toList $ bmm_FixedEffects model

{-
psRowsFld' :: Ord k
          => PostStratification k psRow predRow
          -> MRPBuilderM f predRow modeledRow psRow (FL.FoldM (Either Text) psRow [(k, (Vec.Vector Double, SJ.Indexed Double))])
psRowsFld' (PostStratification prj wgt key) = do
  model <- getModel
  toIndexedFld <- FL.premapM (return . snd) <$> predRowsToIndexed
  let fixedEffectsFld = postMapM (maybe (Left "Empty group in psRowsFld?") Right)
                        $ FL.generalize
                        $ FL.premap fst FL.last
      innerFld = (,) <$> fixedEffectsFld <*> toIndexedFld
      h pr = case bmm_FixedEffects model of
        Nothing -> Vec.empty
        Just (FixedEffects _ f) -> f pr
  return
    $ MR.mapReduceFoldM
    (MR.generalizeUnpack MR.noUnpack)
    (MR.generalizeAssign $ MR.assign key $ \psRow -> let pr = prj psRow in (h pr, (pr, wgt psRow)))
    (MR.foldAndLabelM innerFld (,))

psRowsFld :: Ord k
          => PostStratification k psRow predRow
          -> MRPBuilderM f predRow modeledRow psRow (FL.FoldM (Either Text) psRow [(k, Int, Vec.Vector Double, SJ.Indexed Double)])
psRowsFld ps = do
  fld' <- psRowsFld' ps
  let f (n, (k, (v, i))) = (k, n, v, i)
      g  = fmap f . zip [1..]
  return $ postMapM (return . g) fld'

psRowsJSONFld :: Text -> MRPBuilderM f modeledRow predRow psRow (SJ.StanJSONF (k, Int, Vec.Vector Double, SJ.Indexed Double) A.Series)
psRowsJSONFld psSuffix = do
  hasRowLevelFE <- isJust . bmm_FixedEffects <$> getModel
  let labeled x = x <> psSuffix
  return $ SJ.namedF (labeled "N") FL.length
    <> (if hasRowLevelFE then SJ.valueToPairF (labeled "X") (SJ.jsonArrayF $ \(_, _, v, _) -> v) else mempty)
    <> SJ.valueToPairF (labeled "W") (SJ.jsonArrayF $ \(_, _, _, ix) -> ix)

mrPSKeyMapFld ::  Ord k
           => PostStratification k psRow predRow
           -> FL.Fold psRow (IM.IntMap k)
mrPSKeyMapFld ps = fmap (IM.fromList . zip [1..] . sort . nubOrd . fmap (psGroupKey ps)) FL.list

mrPSDataJSONFold :: Ord k
                 => PostStratification k psRow predRow
                 -> Text
                 -> MRPBuilderM f predRow modeledRow psRow (SJ.StanJSONF psRow A.Series)
mrPSDataJSONFold psFuncs psSuffix = do
  psRowsJSONFld' <- psRowsJSONFld psSuffix
  psDataF <- psRowsFld psFuncs
  return $ postMapM (FL.foldM psRowsJSONFld') psDataF
-}

usedIndexes :: (Typeable d, Typeable modeledRow) => BuilderM modeledRow d (Map GroupName (SB.IntIndex modeledRow))
usedIndexes = do
  indexMap <- getIndexes -- SB.asksEnv sbe_groupIndices
  groupNames <- usedGroupNames
  return $ Map.restrictKeys indexMap groupNames

mrIndexes :: BuilderM modeledRow d (Map GroupName (SB.IntIndex modeledRow))
mrIndexes = do
  indexMap <- getIndexes --SB.asksEnv sbe_groupIndices
  groupNames <- bmm_MRGroups <$> getModel
  return $ Map.restrictKeys indexMap groupNames

dataBlockM :: (Typeable d, Typeable modeledRow) => BuilderM modeledRow d ()
dataBlockM = do
  model <- getModel
--  SB.inBlock SB.SBData $ SB.addStanLine $ "int<lower=0> N"
--  allGroupsM
  allFixedEffectsM
  SB.inBlock SB.SBData $ do
--    SB.addStanLine "int<lower=1> T[N]"
--    SB.addStanLine "int<lower=0> S[N]"
    SB.addColumnJson SB.ModeledRowTag "T" "" (SB.StanArray [SB.NamedDim "N"] SB.StanInt) "<lower=1>" (bmm_Total model)
    SB.addColumnJson SB.ModeledRowTag "S" "" (SB.StanArray [SB.NamedDim "N"] SB.StanInt) "<lower=0>" (bmm_Success model)

mrParametersBlock :: BuilderM modeledRow d ()
mrParametersBlock = SB.inBlock SB.SBParameters $ do
  ui <- mrIndexes
  let binaryParameter x = SB.addStanLine $ "real eps_" <> fst x
      nonBinaryParameter x = do
        let n = fst x
        SB.stanDeclare ("sigma_" <> n) SB.StanReal "<lower=0>"
        SB.stanDeclare ("beta_" <> n) (SB.StanVector $ SB.NamedDim ("N_" <> n)) ("<multiplier=sigma_" <> n <> ">")
--        SB.addStanLine ("real<lower=0> sigma_" <> n)
--        SB.addStanLine ("vector<multiplier = sigma_" <> n <> ">[N_" <> n <> "] beta_" <> n)
      groupParameter x = if (SB.i_Size $ snd x) == 2
                         then binaryParameter x
                         else nonBinaryParameter x
  SB.stanDeclare "alpha" SB.StanReal ""
--  SB.addStanLine "real alpha"
  traverse_ groupParameter $ Map.toList ui


-- alpha + X * beta + beta_age[age] + ...
modelExpr :: Bool -> Text -> BuilderM modeledRow d SB.StanExpr
modelExpr thinQR _ = do
  model <- getModel
  mrIndexMap <- mrIndexes
--  let labeled x = x <> suffix
  let binaryGroupExpr x = let n = fst x in SB.VectorFunctionE "to_vector" $ SB.TermE $ SB.Indexed n $ "{eps_" <> n <> ", -eps_" <> n <> "}"
      nonBinaryGroupExpr x = let n = fst x in SB.TermE . SB.Indexed n $ "beta_" <> n
      groupExpr x = if (SB.i_Size $ snd x) == 2 then binaryGroupExpr x else nonBinaryGroupExpr x
      eAlpha = SB.TermE $ SB.Scalar "alpha"
      eQ s = if T.null s
             then SB.TermE $ SB.Vectored $ "Q_ast"
             else SB.TermE $ SB.Vectored $ "Q" <> SB.underscoredIf s <> "_ast[" <> s <> "]"
      eTheta s = SB.TermE $ SB.Scalar $ "thetaX" <> SB.underscoredIf s
      eQTheta s = SB.BinOpE "*" (eQ s) (eTheta s)
      eX s = if T.null s
             then SB.TermE $ SB.Vectored $ "X"
             else SB.TermE $ SB.Vectored $ "X" <> SB.underscoredIf s <> "[" <> s <> "]"
      eBeta s = SB.TermE $ SB.Scalar $ "betaX" <> SB.underscoredIf s
      eXBeta s = SB.BinOpE "*" (eX s) (eBeta s)
      feExpr s = if thinQR then eQTheta s else eXBeta s
      feGroupsExpr = fmap (\(DHash.Some rtt) -> feExpr $ SB.dsSuffix rtt) $ DHash.keys $ bmm_FixedEffects model
--      lFEExpr = if (isJust $ bmm_FixedEffects model)
--                then feExpr "" : feGroupsExpr
--                else feGroupsExpr
      lMRGroupsExpr = maybe [] pure
                      $ viaNonEmpty (SB.multiOp "+" . fmap groupExpr) $ Map.toList $ mrIndexMap
  let neTerms = eAlpha :| (feGroupsExpr <> lMRGroupsExpr)
  return $ SB.multiOp "+" neTerms

mrpModelBlock :: Double -> Double -> Double -> Double -> BuilderM modeledRow d ()
mrpModelBlock priorSDAlpha priorSDBeta priorSDSigmas sumSigma = SB.inBlock SB.SBModel $ do
  model <- getModel
  mrGroupIndexes <- mrIndexes
  indexMap <- getIndexes --SB.asksEnv sbe_groupIndices
  let binaryPrior x = do
        SB.addStanLine $ "eps_" <> fst x <> " ~ cauchy(0, " <> show priorSDAlpha <> ")"
      nonBinaryPrior x = do
        SB.addStanLine $ "beta_" <> fst x <> " ~ cauchy(0, sigma_" <> fst x <> ")"
        SB.addStanLine $ "sigma_" <> fst x <> " ~ cauchy(0, " <> show priorSDSigmas <> ")"
        SB.addStanLine $ "sum(beta_" <> fst x <> ") ~ cauchy(0, " <> show sumSigma <> ")"
      groupPrior x = if (SB.i_Size $ snd x) == 2
                     then binaryPrior x
                     else nonBinaryPrior x
      fePrior x = SB.addStanLine $ "thetaX" <> SB.underscoredIf x <> " ~ cauchy(0," <> show priorSDBeta <> ")"
  let im = Map.mapWithKey const indexMap
  modelTerms <- SB.printExprM "mrpModelBlock" im SB.Vectorized $ modelExpr True ""
  SB.addStanLine $ "alpha ~ cauchy(0," <> show priorSDAlpha <> ")"
--  when (isJust $ bmm_FixedEffects model) $ fePrior ""
  traverse_ (\(DHash.Some rtt) -> fePrior  $ SB.dsSuffix rtt) $ DHash.keys $ bmm_FixedEffects model
  mrIndexes >>= traverse_ groupPrior . Map.toList
  SB.addStanLine $ "S ~ binomial_logit(T, " <> modelTerms <> ")"

mrpLogLikStanCode :: BuilderM modeledRow d ()
mrpLogLikStanCode = SB.inBlock SB.SBGeneratedQuantities $ do
  model <- getModel
  indexMap <- getIndexes --SB.asksEnv sbe_groupIndices
--  let suffix = if difLL then "ll" else "" -- we use model data unless a different suffix is provided
  SB.addStanLine $ "vector [N] log_lik"
  SB.stanForLoop "n" Nothing "N" $ \_ -> do
    let im = Map.mapWithKey (\k _ -> k <> "[n]") indexMap -- we need to index the groups.
    modelTerms <- SB.printExprM "mrpLogLikStanCode" im (SB.NonVectorized "n") $ modelExpr False ""
    SB.addStanLine $ "log_lik[n] = binomial_logit_lpmf(S[n] | T[n], " <> modelTerms <> ")"

{-
mrpPSStanCode :: forall f predRow modeledRow psRow.
                 Bool
              -> MRPBuilderM f predRow modeledRow psRow ()
mrpPSStanCode doPS = SB.inBlock SB.SBGeneratedQuantities $ do
  model <- getModel
  groupNames <- fmap fst <$> groupOrderedIntIndexes
  if doPS then (do
                   let groupCounters = fmap ("n_" <>) $ groupNames
                       im = Map.fromList $ zip groupNames groupCounters
                       inner = do
                         modelTerms <- SB.printExprM "mrpPSStanCode" im (SB.NonVectorized "n") $ modelExpr False "ps"
                         SB.addStanLine $ "real p<lower=0, upper=1> = inv_logit(" <> modelTerms <> ")"
                         SB.addStanLine $ "ps[n] += p * Wps[n][" <> T.intercalate "," groupCounters <> "]"
                       makeLoops :: [Text] -> MRPBuilderM f predRow modeledRow psRow ()
                       makeLoops []  = inner
                       makeLoops (x : xs) = SB.stanForLoop ("n_" <> x) Nothing ("J_" <> x) $ const $ makeLoops xs
                   SB.addStanLine $ "vector [Nps] ps"
                   SB.stanForLoop "n" Nothing "Nps" $ const $ makeLoops groupNames
               )
    else return ()
-}

mrpGeneratedQuantitiesBlock :: Bool
                            -> BuilderM modeledRow d ()
mrpGeneratedQuantitiesBlock doPS = do
  SB.inBlock SB.SBGeneratedQuantities $ do
--    mrGroupIndexes <- mrIndexes
    indexMap <- getIndexes
    let im = Map.mapWithKey const indexMap
    model_terms <- SB.printExprM "yPred (Generated Quantities)" im SB.Vectorized $ modelExpr False ""
    SB.addStanLine $ "vector<lower=0, upper=1>[N] yPred = inv_logit(" <> model_terms <> ")"
    SB.addStanLine $ "real<lower=0> SPred[N]" --" = yPred * to_vector(T)"
    SB.stanForLoop "n" Nothing "N" $ const $ SB.addStanLine "SPred[n] = yPred[n] * T[n]"
  mrpLogLikStanCode

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
import qualified Data.IntMap.Strict as IM
import Data.List.Extra (nubOrd)
import qualified Data.Map as Map
import qualified Data.Maybe as Maybe

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


runMRPModel :: forall k psRow modeledRow predRow f r.
               (K.KnitEffects r
               , BR.CacheEffects r
               , Foldable f
               , Functor f
               , Ord k
               , Flat.Flat k)
            => Bool
            -> Maybe Text
            -> Text
            -> Binomial_MRP_Model predRow modeledRow
            -> Maybe (PostStratification k psRow predRow)
            -> Maybe Text
            -> Text
            -> K.ActionWithCacheTime r (MRPData f predRow modeledRow psRow)
            -> Maybe Int
            -> K.Sem r ()
runMRPModel clearCache mWorkDir modelName model mPSFunctions mLLSuffix dataName mrpData_C mNSamples =
  K.wrapPrefix "BlueRipple.Model.StanMRP" $ do
  K.logLE K.Info "Building dataWrangler and model code"
  mrpData <- K.ignoreCacheTime mrpData_C
  let builderEnv = buildEnv model $ ProjectableRows (modeled mrpData) (bmm_PrjPred model)
      mPSSuffix = fmap (const "ps") mPSFunctions
  (stanCode, dataWrangler) <- K.knitEither $ SB.runStanBuilder builderEnv $ do
    mrpDataBlock model mPSSuffix mLLSuffix
    mrpParametersBlock model
    mrpModelBlock model 2 2 1
    mrpGeneratedQuantitiesBlock model mPSSuffix mLLSuffix
    mrpDataWrangler model mrpData mPSFunctions
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
    1
    (Just nSamples)
    (Just nSamples)
    (Just stancConfig)
  let resultCacheKey = "stan/MRP/result/" <> outputLabel <> ".bin"
  when clearCache $ do
    K.liftKnit $ SM.deleteStaleFiles stanConfig [SM.StaleData]
    BR.clearIfPresentD resultCacheKey
  modelDep <- SM.modelCacheTime stanConfig
  K.logLE K.Diagnostic $ "modelDep: " <> show (K.cacheTime modelDep)
  K.logLE K.Diagnostic $ "houseDataDep: " <> show (K.cacheTime mrpData_C)
  let dataModelDep = const <$> modelDep <*> mrpData_C
      getResults s () inputAndIndex_C = return ()
      -- we need something for PP checks here.  Probably counts
      unwraps = [SR.UnwrapNamed "Tm" "Tm", SR.UnwrapNamed "Sm" "Sm"]
  res_C <- BR.retrieveOrMakeD resultCacheKey dataModelDep $ \() -> do
    K.logLE K.Info "Data or model newer then last cached result. (Re)-running..."
    SM.runModel @BR.SerializerC @BR.CacheData
      stanConfig
      (SM.Both unwraps)
      dataWrangler
      (SC.UseSummary getResults)
      ()
      mrpData_C
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

--data EncodePS k psRow = EncodePS

data IntIndex row = IntIndex { i_Size :: Int, i_Index :: row -> Maybe Int }

intEncoderFoldToIntIndexFold :: SJ.IntEncoderF row -> FL.Fold row (IntIndex row)
intEncoderFoldToIntIndexFold = fmap (\(f, km) -> IntIndex (IM.size km) f)

data MRPBuilderEnv predRow  =
  StanBuilderEnv
  {
    sbe_groupIndices :: Map Text (IntIndex predRow)
--  , sbe_Model :: Binomial_MRP_Model predRow modelRow
  }

type MRPBuilderM a = SB.StanBuilderM (MRPBuilderEnv a)

getIndex :: Group row -> MRPBuilderM row (IntIndex row)
getIndex g = do
  indexMap <- SB.asksEnv sbe_groupIndices
  case (Map.lookup (groupName g) indexMap) of
    Nothing -> SB.stanBuildError $ "No group index found for group with name=\"" <> (groupName g) <> "\""
    Just i -> return i

getIndexes :: MRPBuilderM row (Map Text (IntIndex row))
getIndexes = SB.asksEnv sbe_groupIndices

data Group row where
  EnumeratedGroup :: Text -> IntIndex row -> Group row
  LabeledGroup :: Text -> FL.Fold row (IntIndex row) -> Group row

groupName :: Group row -> Text
groupName (EnumeratedGroup n _) = n
groupName (LabeledGroup n _) = n

groupIndex :: Foldable f => Group row -> f row -> IntIndex row
groupIndex (EnumeratedGroup _ i) _ = i
groupIndex (LabeledGroup _ fld) rows = FL.fold fld rows

groupSizeJSONFold :: Text -> Group row -> MRPBuilderM row (SJ.StanJSONF row A.Series)
groupSizeJSONFold prefix g = do
  let name = groupName g
  IntIndex indexSize indexM <- getIndex g
  return $ SJ.constDataF (prefix <> name) indexSize

groupDataJSONFold :: Text -> Group row -> MRPBuilderM row (SJ.StanJSONF row A.Series)
groupDataJSONFold suffix g = do
  let name = groupName g
  IntIndex indexSize indexM <- getIndex g
  return $ SJ.valueToPairF (name <> "_" <> suffix) (SJ.jsonArrayMF indexM)

groupsJSONFold :: Traversable f
               => (Text -> Group row -> MRPBuilderM row (SJ.StanJSONF row A.Series))
               -> Text
               -> f (Group row)
               -> MRPBuilderM row (SJ.StanJSONF row A.Series)
groupsJSONFold groupFold t =  fmap (foldMap id) . traverse (groupFold t)

groupsDataJSONFold :: Traversable f => Text -> f (Group row) -> MRPBuilderM row (SJ.StanJSONF row A.Series)
groupsDataJSONFold = groupsJSONFold groupDataJSONFold

groupsSizeFold :: Traversable f => f (Group row) -> MRPBuilderM row (SJ.StanJSONF row A.Series)
groupsSizeFold = groupsJSONFold groupSizeJSONFold "J_"

data Binomial_MRP_Model predRow modelRow =
  Binomial_MRP_Model
  {
    bmm_Name :: Text -- we'll need this for (unique) file names
  , bmm_nFixedEffects :: Int
  , bmm_FixedEffects :: predRow -> Vec.Vector Double
  , bmm_Groups :: [Group predRow]
  , bmm_PrjPred :: modelRow -> predRow
  , bmm_Total :: modelRow -> Int
  , bmm_Success :: modelRow -> Int
  }

buildEnv :: Foldable f => Binomial_MRP_Model predRow modelRow -> MRData f modeledRow predRow  -> MRPBuilderEnv predRow
buildEnv model modelDat = StanBuilderEnv groupIndexMap
  where
    groupIndexMap = Map.fromList $ fmap (\g -> (groupName g, groupIndex g (projectedRows modelDat))) (bmm_Groups model)

type PostStratificationWeight psRow = psRow -> Double

mrGroupJSONFold :: () --Foldable g
                    => Binomial_MRP_Model predRow modeledRow
                    -> MRData g modeledRow predRow
                    -> MRPBuilderM predRow (SJ.StanJSONF modeledRow A.Series)
mrGroupJSONFold model modelDat = do
   fGS <- groupsSizeFold (bmm_Groups model)
   return $ FL.premapM (return . projectRow modelDat) fGS

predDataJSONFold :: Text
                 -> Binomial_MRP_Model predRow modeledRow
                 -> ProjectableRows f row predRow
                 -> MRPBuilderM predRow (SJ.StanJSONF row A.Series)
predDataJSONFold label model rows = do
  fGD <- groupsDataJSONFold label (bmm_Groups model)
  let labeled t = t <> label
  return
    $ SJ.namedF (labeled "N") FL.length
    <> (if (bmm_nFixedEffects model > 0) then SJ.valueToPairF (labeled "X") (SJ.jsonArrayF (bmm_FixedEffects model . projectRow rows)) else mempty)
    <> FL.premapM (return . projectRow rows) fGD

mrModelDataJSONFold  :: Binomial_MRP_Model predRow modeledRow
                     -> MRData g modeledRow predRow
                     -> MRPBuilderM predRow (SJ.StanJSONF modeledRow A.Series)
mrModelDataJSONFold model modelDat = do
  sizesF <- mrGroupJSONFold model modelDat
  predDataF <- predDataJSONFold "m" model modelDat
  return
    $ (if (bmm_nFixedEffects model > 0) then SJ.constDataF "K" (bmm_nFixedEffects model) else mempty)
    <> sizesF
    <> predDataF
    <> SJ.valueToPairF "Tm" (SJ.jsonArrayF $ bmm_Total model)
    <> SJ.valueToPairF "Sm" (SJ.jsonArrayF $ bmm_Success model)

groupOrderedIntIndexes :: Binomial_MRP_Model predRow modelRow
                    -> MRPBuilderM predRow [IntIndex predRow]
groupOrderedIntIndexes model = do
  indexMap <- SB.asksEnv sbe_groupIndices
  let groups = bmm_Groups model
  maybe
    (SB.stanBuildError "Error looking up a group name in ModelBuilderEnv")
    return
    $ traverse (flip Map.lookup indexMap) $ fmap groupName groups

predRowsToIndexed :: (Num a, Show a)
                  => Binomial_MRP_Model predRow modelRow
                  -> MRPBuilderM predRow (FL.FoldM (Either Text) (predRow, a) (SJ.Indexed a))
predRowsToIndexed model = do
  indexes <- groupOrderedIntIndexes model
  let bounds = zip (repeat 1) $ fmap i_Size indexes
      indexers = fmap i_Index indexes
      toIndices x = maybe (Left "Indexer error when building psRow fold") Right $ traverse ($x) indexers
      f (pr, a) = fmap (,a) $ toIndices pr
  return $ postMapM (\x -> traverse f x >>= SJ.prepareIndexed 0 bounds) $ FL.generalize FL.list

psRowsFld' :: Ord k
          => Binomial_MRP_Model predRow modelRow
          -> PostStratification k psRow predRow
          -> MRPBuilderM predRow (FL.FoldM (Either Text) psRow [(k, (Vec.Vector Double, SJ.Indexed Double))])
psRowsFld' model (PostStratification prj wgt key) = do
  toIndexedFld <- FL.premapM (return . snd) <$> predRowsToIndexed model
  let fixedEffectsFld = postMapM (maybe (Left "Empty group in psRowsFld?") Right)
                        $ FL.generalize
                        $ FL.premap fst FL.last
      innerFld = (,) <$> fixedEffectsFld <*> toIndexedFld
  return
    $ MR.mapReduceFoldM
    (MR.generalizeUnpack MR.noUnpack)
    (MR.generalizeAssign $ MR.assign key $ \psRow -> let pr = prj psRow in (bmm_FixedEffects model pr, (pr, wgt psRow)))
    (MR.foldAndLabelM innerFld (,))

psRowsFld :: Ord k
          => Binomial_MRP_Model predRow modelRow
          -> PostStratification k psRow predRow
          -> MRPBuilderM predRow (FL.FoldM (Either Text) psRow [(k, Int, Vec.Vector Double, SJ.Indexed Double)])
psRowsFld model ps = do
  fld' <- psRowsFld' model ps
  let f (n, (k, (v, i))) = (k, n, v, i)
      g  = fmap f . zip [1..]
  return $ postMapM (return . g) fld'

psRowsJSONFld :: Text -> SJ.StanJSONF (k, Int, Vec.Vector Double, SJ.Indexed Double) A.Series
psRowsJSONFld psSuffix =
  let labeled x = x <> psSuffix
  in SJ.namedF (labeled "N") FL.length
     <> SJ.valueToPairF (labeled "X") (SJ.jsonArrayF $ \(_, _, v, _) -> v)
     <> SJ.valueToPairF (labeled "W") (SJ.jsonArrayF $ \(_, _, _, ix) -> ix)

mrPSKeyMapFld ::  Ord k
           => PostStratification k psRow predRow
           -> FL.Fold psRow (IM.IntMap k)
mrPSKeyMapFld ps = fmap (IM.fromList . zip [1..] . sort . nubOrd . fmap (psGroupKey ps)) FL.list

mrPSDataJSONFold :: Ord k
                 => Binomial_MRP_Model predRow modeledRow
                 -> PostStratification k psRow predRow
                 -> Text
                 -> MRPBuilderM predRow (SJ.StanJSONF psRow A.Series)
mrPSDataJSONFold model psFuncs psSuffix = do
  psDataF <- psRowsFld model psFuncs
--  let fld = (,) <$> FL.generalize mrPSKeyMapFld <*> psRowsJSONFld
  return $ postMapM (FL.foldM $ psRowsJSONFld psSuffix) psDataF

mrLLDataJSONFold :: ()
                 => Binomial_MRP_Model predRow modeledRow
                 -> LLData f modeledRow predRow
                 -> MRPBuilderM predRow (SJ.StanJSONF modeledRow A.Series)
mrLLDataJSONFold model llDat = do
  predDatF <- predDataJSONFold "ll" model llDat
  return
    $ predDatF
    <> SJ.valueToPairF "Total" (SJ.jsonArrayF $ bmm_Total model)
    <> SJ.valueToPairF "Success" (SJ.jsonArrayF $ bmm_Success model)

data MRPData f predRow modeledRow psRow =
  MRPData
  {
    modeled :: f modeledRow
  , postStratified ::Maybe (f psRow) -- if this is Nothing we don't do post-stratification
  , logLikelihood :: Maybe (f modeledRow) -- if this is Nothing, we use the modeled data instead
  }

ntMRPData :: (forall a.f a -> g a) -> MRPData f j k l -> MRPData g j k l
ntMRPData h (MRPData mod mPS mLL) = MRPData (h mod) (h <$> mPS) (h <$> mLL)

mrpDataWrangler :: forall k psRow f predRow modeledRow.
                   (Foldable f, Functor f, Ord k)
                => Binomial_MRP_Model predRow modeledRow
                -> MRPData f predRow modeledRow psRow
                -> Maybe (PostStratification k psRow predRow)
                -> MRPBuilderM predRow (SC.DataWrangler (MRPData f predRow modeledRow psRow) (IM.IntMap k) ())
mrpDataWrangler model (MRPData modeled mPS mLL) mPSFunctions = do
  modelDataFold <- mrModelDataJSONFold model (ProjectableRows modeled $ bmm_PrjPred model)
  psDataFold <- case mPS of
    Nothing -> return mempty
    Just ps -> case mPSFunctions of
      Nothing -> SB.stanBuildError "PostStratification data given but post-stratification functions unset."
      Just ps -> mrPSDataJSONFold model ps "ps"
  llDataFold <- case mLL of
    Nothing -> return mempty
    Just ll -> mrLLDataJSONFold model (ProjectableRows ll $ bmm_PrjPred model)
  let psKeyMapFld = maybe mempty mrPSKeyMapFld mPSFunctions
  let makeDataJsonE (MRPData modeled mPS mLL) = do
        modeledJSON <- SJ.frameToStanJSONSeries modelDataFold modeled
        psJSON <- maybe (Right mempty) (FL.foldM psDataFold) mPS
        llJSON <- maybe (Right mempty) (SJ.frameToStanJSONSeries llDataFold) mLL
        return $ modeledJSON <> psJSON <>  llJSON
      psKeyMap = maybe mempty (FL.fold psKeyMapFld)
      f (MRPData _ mPS _) = (psKeyMap mPS, makeDataJsonE)
  return $ SC.Wrangle SC.TransientIndex f

bFixedEffects :: Binomial_MRP_Model predRow modeledRow -> Bool
bFixedEffects model = bmm_nFixedEffects model > 0

groupSizesBlock :: MRPBuilderM predRow ()
groupSizesBlock = do
  indexMap <- SB.asksEnv sbe_groupIndices
  let groupSize x = SB.addStanLine $ "int<lower=2> J_" <> fst x
  traverse_ groupSize $ Map.toList indexMap

labeledDataBlockForRows :: Binomial_MRP_Model predRow modeledRow -> Text -> MRPBuilderM predRow ()
labeledDataBlockForRows model suffix = do
  indexMap <- SB.asksEnv sbe_groupIndices
  let groupIndex x = if i_Size (snd x) == 2
                     then SB.addStanLine $ "int<lower=1, upper=2> " <> fst x <> "_" <> suffix <> "[N" <> suffix <> "]"
--                      then SB.addStanLine $ "int<lower=1, upper=2>[N" <> suffix <> "] " <> fst x
                     else SB.addStanLine $ "int<lower=1, upper=J_" <> fst x <> "> " <> fst x <> "_" <> suffix <> "[N" <> suffix <> "]"
  SB.addStanLine $ "int<lower=1> N" <> suffix
  when (bFixedEffects model) $ SB.addStanLine $ "matrix[N" <> suffix <> ", K] X" <> suffix
  traverse_ groupIndex $ Map.toList indexMap

mrpDataBlock :: Binomial_MRP_Model predRow modeledRow
             -> Maybe Text
             -> Maybe Text
             -> MRPBuilderM predRow ()
mrpDataBlock model mPSSuffix mLLSuffix = SB.inBlock SB.SBData $ do
  groupSizesBlock
  when (bFixedEffects model) $ SB.addStanLine "int<lower=1> K"
  labeledDataBlockForRows model "m"
  SB.addStanLine $ "int<lower=1> Tm[Nm]"
  SB.addStanLine $ "int<lower=0> Sm[Nm]"
  case mPSSuffix of
    Nothing -> return ()
    Just psSuffix -> do
      SB.addStanLine $ "int N" <> psSuffix
      when (bFixedEffects model) $ SB.addStanLine $ "matrix[N" <> psSuffix <> ", K] X" <> psSuffix
      groupUpperBounds <- T.intercalate "," . fmap (show . i_Size) <$> groupOrderedIntIndexes model
      SB.addStanLine $ "real<lower=0>[" <> groupUpperBounds <> "]" <> "W[N" <> psSuffix <> "]" -- real[2,2,4] W[Nps];
  case mLLSuffix of
    Nothing -> return ()
    Just llSuffix -> do
      labeledDataBlockForRows model llSuffix
      SB.addStanLine $ "int <lower=0>[N" <> llSuffix <> "] T" <> llSuffix
      SB.addStanLine $ "int <lower=0>[N" <> llSuffix <> "] S" <> llSuffix

mrpParametersBlock :: Binomial_MRP_Model predRow modeledRow -> MRPBuilderM predRow ()
mrpParametersBlock model = SB.inBlock SB.SBParameters $ do
  indexMap <- SB.asksEnv sbe_groupIndices
  let binaryParameter x = SB.addStanLine $ "real eps_" <> fst x
      nonBinaryParameter x = do
        let n = fst x
        SB.addStanLine ("real<lower=0> sigma_" <> n)
        SB.addStanLine ("vector<multiplier = sigma_" <> n <> ">[J_" <> n <> "] beta_" <> n)
      groupParameter x = if (i_Size $ snd x) == 2
                         then binaryParameter x
                         else nonBinaryParameter x
  SB.addStanLine "real alpha"
  when (bFixedEffects model) $ SB.addStanLine "vector[K] beta"
  traverse_ groupParameter $ Map.toList indexMap

-- standardize (and center??) the fixed effects
mrpTransformedParametersBlock :: Binomial_MRP_Model predRow modeledRow -> MRPBuilderM predRow ()
mrpTransformedParametersBlock model = SB.inBlock SB.SBParameters $ do


-- alpha + X * beta + beta_age[age] + ...
modelExpr :: Binomial_MRP_Model predRow modeledRow -> Text -> MRPBuilderM predRow SB.StanExpr
modelExpr model suffix = do
  indexMap <- SB.asksEnv sbe_groupIndices
  let labeled x = x <> suffix
      binaryGroupExpr x = let n = fst x in SB.VectorFunctionE "to_vector" $ SB.TermE $ SB.Indexed n $ "{eps_" <> n <> ", -eps_" <> n <> "}"
      nonBinaryGroupExpr x = let n = fst x in SB.TermE . SB.Indexed n $ "beta_" <> n
      groupExpr x = if (i_Size $ snd x) == 2 then binaryGroupExpr x else nonBinaryGroupExpr x
      eAlpha = SB.TermE $ SB.Scalar "alpha"
      eX = SB.TermE $ SB.Vectored $ labeled "X"
      eBeta = SB.TermE $ SB.Scalar "beta"
      eXBeta = SB.BinOpE "*" eX eBeta
      lXBetaExpr = if bFixedEffects model then [eXBeta] else []
      lGroupsExpr = maybe [] pure
                    $ viaNonEmpty (SB.multiOp "+" . fmap groupExpr) $ Map.toList indexMap
  let neTerms = eAlpha :| (lXBetaExpr <> lGroupsExpr)
  return $ SB.multiOp "+" neTerms

mrpModelBlock :: Binomial_MRP_Model predRow modeledRow -> Double -> Double -> Double -> MRPBuilderM predRow ()
mrpModelBlock model priorSDAlpha priorSDBeta priorSDSigmas = SB.inBlock SB.SBModel $ do
  indexMap <- SB.asksEnv sbe_groupIndices
  let binaryPrior x = SB.addStanLine $ "eps_" <> fst x <> " ~ normal(0, " <> show priorSDAlpha <> ")"
      nonBinaryPrior x = do
        SB.addStanLine $ "beta_" <> fst x <> " ~ normal(0, sigma_" <> fst x <> ")"
        SB.addStanLine $ "sigma_" <> fst x <> " ~ normal(0, " <> show priorSDSigmas <> ")"
      groupPrior x = if (i_Size $ snd x) == 2
                     then binaryPrior x
                     else nonBinaryPrior x
  let im = Map.mapWithKey (\k _ -> k <> "_m") indexMap
  modelTerms <- SB.printExprM "mrpModelBlock" im SB.Vectorized $ modelExpr model "m"
  SB.addStanLine $ "alpha ~ normal(0," <> show priorSDAlpha <> ")"
  when (bFixedEffects model) $ SB.addStanLine $ "beta ~ normal(0," <> show priorSDBeta <> ")"
  traverse groupPrior $ Map.toList indexMap
  SB.stanPrint [SB.StanLiteral "alpha=", SB.StanExpression "alpha"]
  SB.stanPrint [SB.StanLiteral "beta=", SB.StanExpression "beta"]
  SB.stanPrint [SB.StanLiteral "target before=", SB.StanExpression "target()"]
  SB.addStanLine $ "Sm ~ binomial_logit(Tm, " <> modelTerms <> ")"
  SB.stanPrint [SB.StanLiteral "target after=", SB.StanExpression "target()"]
--  SB.addStanLine $ "print(\"alpha \",alpha)"
--  SB.addStanLine $ "print(\"beta \",beta)"

-- we need to general log_lik using modeled or given data

mrpLogLikStanCode :: Binomial_MRP_Model predRow modeledRow
                  -> Maybe Text
                  -> MRPBuilderM predRow ()
mrpLogLikStanCode model mLLSuffix = SB.inBlock SB.SBGeneratedQuantities $ do
  indexMap <- SB.asksEnv sbe_groupIndices
  let suffix = fromMaybe "m" mLLSuffix -- we use model data unless a different suffix is provided
  SB.addStanLine $ "vector [N" <> suffix <> "] log_lik"
  SB.stanForLoop "n" Nothing ("N" <> suffix) $ \_ -> do
    let im = Map.mapWithKey (\k _ -> k <> "_" <> suffix <> "[n]") indexMap -- we need to index the groups.
    modelTerms <- SB.printExprM "mrpLogLikStanCode" im (SB.NonVectorized "n") $ modelExpr model suffix
    SB.addStanLine $ "log_lik[n] = binomial_logit_lpmf(S" <> suffix <> "[n]| T" <> suffix <> "[n], " <> modelTerms <> ")"


mrpPSStanCode :: forall predRow modeledRow.
                 Binomial_MRP_Model predRow modeledRow
              -> Maybe Text
              -> MRPBuilderM predRow ()
mrpPSStanCode model mPSSuffix = SB.inBlock SB.SBGeneratedQuantities $ do
  case mPSSuffix of
    Nothing -> return ()
    Just suffix -> do
      let groupNames = fmap groupName (bmm_Groups model)
          groupCounters = fmap ("n_" <>) groupNames
          im = Map.fromList $ zip groupNames groupCounters
          inner = do
            modelTerms <- SB.printExprM "mrpPSStanCode" im (SB.NonVectorized "n") $ modelExpr model suffix
            SB.addStanLine $ "real p<lower=0, upper=1> = inv_logit(" <> modelTerms <> ")"
            SB.addStanLine $ "ps[n] += p * W" <> suffix <> "[n][" <> T.intercalate "," groupCounters <> "]"
          makeLoops :: [Text] -> MRPBuilderM predRow ()
          makeLoops []  = inner
          makeLoops (x : xs) = SB.stanForLoop ("n_" <> x) Nothing ("J_" <> x) $ const $ makeLoops xs
      SB.addStanLine $ "vector [N" <> suffix <> "] ps"
      SB.stanForLoop "n" Nothing ("N" <> suffix) $ const $ makeLoops groupNames

mrpGeneratedQuantitiesBlock :: Binomial_MRP_Model predRow modeledRow
                            -> Maybe Text
                            -> Maybe Text
                            -> MRPBuilderM predRow ()
mrpGeneratedQuantitiesBlock model mPSSuffix mLLSuffix = do
  mrpPSStanCode model mPSSuffix
  mrpLogLikStanCode model mLLSuffix




--binomialMRPPostStratification
{-
mrpDataWrangler :: Text -> MRP_Model -> MRP_DataWrangler as bs ()
mrpDataWrangler cacheDir model =
  MRP_DataWrangler
  $ SC.WrangleWithPredictions (SC.CacheableIndex $ \c -> cacheDir <> "stan/index" <> SC.mrcOutputPrefix c <> ".bin") f g
  where

    enumStateF = FL.premap (F.rgetField @BR.StateAbbreviation) (SJ.enumerate 1)
    encodeAge = SF.toRecEncoding @DT.SimpleAgeC $ SJ.dummyEncodeEnum @DT.SimpleAge
    encodeSex = SF.toRecEncoding @DT.SexC $ SJ.dummyEncodeEnum @DT.Sex
  encodeEducation = SF.toRecEncoding @DT.CollegeGradC $ SJ.dummyEncodeEnum @DT.CollegeGrad
  encodeRace = SF.toRecEncoding @DT.Race5C $ SJ.dummyEncodeEnum @DT.Race5
  encodeCatCols :: SJ.Encoding SJ.IntVec (F.Record DT.CatColsASER5)
  encodeCatCols = SF.composeIntVecRecEncodings encodeAge
                  $ SF.composeIntVecRecEncodings encodeSex
                  $ SF.composeIntVecRecEncodings encodeEducation encodeRace
  (catColsIndexer, toCatCols) = encodeCatCols
  f cces = ((toState, fmap FS.toS toCatCols), makeJsonE) where
    (stateM, toState) = FL.fold enumStateF cces
    k = SJ.vecEncodingLength encodeCatCols
    makeJsonE x = SJ.frameToStanJSONSeries dataF cces where
      dataF = SJ.namedF "G" FL.length
              <> SJ.constDataF "J_state" (IM.size toState)
              <> SJ.constDataF "K" k
              <> SJ.valueToPairF "X" (SJ.jsonArrayMF (catColsIndexer . F.rcast @DT.CatColsASER5))
              <> SJ.valueToPairF "state" (SJ.jsonArrayMF (stateM . F.rgetField @BR.StateAbbreviation))
              <> SJ.valueToPairF "D_votes" (SJ.jsonArrayF (round @_ @Int . F.rgetField @BR.WeightedSuccesses))
              <> SJ.valueToPairF "Total_votes" (SJ.jsonArrayF (F.rgetField @BR.Count))
  g (toState, _) toPredict = SJ.frameToStanJSONSeries predictF toPredict where
    toStateIndexM sa = M.lookup sa $ SJ.flipIntIndex toState
    predictF = SJ.namedF "M" FL.length
               <> SJ.valueToPairF "predict_State" (SJ.jsonArrayMF (toStateIndexM . F.rgetField @BR.StateAbbreviation))
               <> SJ.valueToPairF "predict_X" (SJ.jsonArrayMF (catColsIndexer . F.rcast @DT.CatColsASER5))


extractResults :: Int
               -> ET.OfficeT
               -> CS.StanSummary
               -> F.FrameRec (CCES_KeyRow DT.CatColsASER5)
               -> Either T.Text (F.FrameRec (CCES_KeyRow DT.CatColsASER5 V.++ '[BR.Year, ET.Office, ET.DemVPV, BR.DemPref]))
extractResults year office summary toPredict = do
   predictProbs <- fmap CS.mean <$> SP.parse1D "predicted" (CS.paramStats summary)
   let yoRec :: F.Record '[BR.Year, ET.Office] = year F.&: office F.&: V.RNil
       probRec :: Double -> F.Record [ET.DemVPV, BR.DemPref]
       probRec x = (2*x -1 ) F.&: x F.&: V.RNil
       makeRow key prob = key F.<+> yoRec F.<+> probRec prob
   return $ F.toFrame $ uncurry makeRow <$> zip (FL.fold FL.list toPredict) (FL.fold FL.list predictProbs)

comparePredictions ::  K.KnitEffects r
                   => F.FrameRec (CCES_KeyRow DT.CatColsASER5 V.++ '[BR.Year, ET.Office, ET.DemVPV, BR.DemPref])
                   -> F.FrameRec (CCES_CountRow DT.CatColsASER5)
                   -> K.Sem r ()
comparePredictions predictions input = do
  joined <- K.knitEither
            $ either (Left. show) Right
            $ FJ.leftJoinE @(CCES_KeyRow DT.CatColsASER5) input predictions
  let p = F.rgetField @ET.DemPref
      n = F.rgetField @BR.Count
      s = realToFrac . round @_ @Int . F.rgetField @BR.WeightedSuccesses
      rowCountError r = abs (p r * realToFrac (n r) - s r)
      countErrorOfVotersF = (\x y -> x / realToFrac y) <$> FL.premap rowCountError FL.sum <*> FL.premap n FL.sum
      countErrorOfDemsF = (\x y -> x / realToFrac y) <$> FL.premap rowCountError FL.sum <*> FL.premap s FL.sum
      (countErrorV, countErrorD) = FL.fold ((,) <$> countErrorOfVotersF <*> countErrorOfDemsF) joined
  K.logLE K.Info $ "absolute count error (fraction of all votes): " <> show countErrorV
  K.logLE K.Info $ "absolute count error (fraction of D votes): " <> show countErrorD


count :: forall ks r.
              (K.KnitEffects r
              , Ord (F.Record ks)
              , FI.RecVec (ks V.++ BR.CountCols)
              , ks F.⊆ CCES.CCES_MRP
              )
      => (F.Record CCES.CCES_MRP -> F.Record ks)
      -> ET.OfficeT
      -> Int
      -> F.FrameRec CCES.CCES_MRP
      -> K.Sem r (F.FrameRec (ks V.++ BR.CountCols))
count getKey office year ccesMRP = do
  countFold <- K.knitEither $ case (office, year) of
    (ET.President, 2008) ->
      Right $ CCES.countDVotesF @CCES.Pres2008VoteParty getKey 2008
    (ET.President, 2012) ->
      Right $ CCES.countDVotesF @CCES.Pres2012VoteParty getKey 2012
    (ET.President, 2016) ->
      Right $ CCES.countDVotesF @CCES.Pres2016VoteParty getKey 2016
    (ET.House, y) ->
      Right $  CCES.countDVotesF @CCES.HouseVoteParty getKey y
    _ -> Left $ show office <> "/" <> show year <> " not available."
  let counted = FL.fold countFold ccesMRP
  return counted

prefASER5_MR :: (K.KnitEffects r,  BR.CacheEffects r, BR.SerializerC b)
             => (T.Text, CCESDataWrangler DT.CatColsASER5 b)
             -> (T.Text, SB.StanModel)
             -> ET.OfficeT
             -> Int
             -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec
                                                  (CCES_KeyRow DT.CatColsASER5
                                                    V.++
                                                    '[BR.Year, ET.Office, ET.DemVPV, BR.DemPref]
                                                  )))
prefASER5_MR (dataLabel, ccesDataWrangler) (modelName, model) office year = do
  -- count data
  let officeYearT = show office <> "_" <> show year
      countCacheKey = "data/stan/cces/stateVotesASER5_" <> officeYearT <> ".bin"
  allStatesL <- do
    stateXWalk <- K.ignoreCacheTimeM BR.stateAbbrCrosswalkLoader
    return $ fmap (F.rgetField @BR.StateAbbreviation) .  FL.fold FL.list . F.filterFrame ((<60) . F.rgetField @BR.StateFIPS) $ stateXWalk
  let toPredict :: F.FrameRec ('[BR.StateAbbreviation] V.++ DT.CatColsASER5)
      toPredict = F.toFrame [ s F.&: cat | s <- allStatesL, cat <- DT.allCatKeysASER5]
  cces_C <- CCES.ccesDataLoader
  ccesASER5_C <- BR.retrieveOrMakeFrame countCacheKey cces_C
                 $ count (F.rcast @(BR.StateAbbreviation ': DT.CatColsASER5)) office year
  let stancConfig = (SM.makeDefaultStancConfig (toString $ "stan/voterPref/" <> modelName)) { CS.useOpenCL = False }
  stanConfig <- SC.noLogOfSummary
                <$> SM.makeDefaultModelRunnerConfig
                "stan/voterPref"
                (modelName <> "_model")
                (Just (SB.NoLL, model))
                (Just $ "cces_" <> officeYearT <> "_" <> dataLabel <> ".json")
                (Just $ "cces_" <> officeYearT <> "_" <> modelName <> "_model")
                4
                (Just 1000)
                (Just 1000)
                (Just stancConfig)
  let resultCacheKey = "model/stan/cces/statePrefsASER5_" <> officeYearT <> "_" <> modelName <> ".bin"
  modelDep <- SM.modelCacheTime stanConfig
  let dataModelDep = const <$> modelDep <*> ccesASER5_C
      getResults s tp inputAndIndex_C = do
        (input, _) <- K.ignoreCacheTime inputAndIndex_C
        predictions <- K.knitEither $ extractResults year office s tp
        comparePredictions predictions input
        return predictions
  BR.retrieveOrMakeFrame resultCacheKey dataModelDep $ \() -> do
    K.logLE K.Info "Data or model newer than last cached result. Rerunning."
    SM.runModel @BR.SerializerC @BR.CacheData stanConfig (SM.ShinyStan [SR.UnwrapNamed "D_votes" "D_votes"]) ccesDataWrangler (SC.UseSummary getResults) toPredict ccesASER5_C


prefASER5_MR_Loo :: (K.KnitEffects r,  BR.CacheEffects r, BR.SerializerC b)
                 => (T.Text, CCESDataWrangler DT.CatColsASER5 b)
                 -> (T.Text, SB.StanModel)
                 -> ET.OfficeT
                 -> Int
                 -> K.Sem r ()
prefASER5_MR_Loo (dataLabel, ccesDataWrangler) (modelName, model) office year = do
  -- count data
  let officeYearT = show office <> "_" <> show year
      countCacheKey = "data/stan/cces/stateVotesASER5_" <> officeYearT <> ".bin"
  cces_C <- CCES.ccesDataLoader
  K.logLE K.Diagnostic "Finished loading (cached) CCES data"
  ccesASER5_C <- BR.retrieveOrMakeFrame countCacheKey cces_C
                 $ count (F.rcast  @(BR.StateAbbreviation ': DT.CatColsASER5)) office year
  let stancConfig = (SM.makeDefaultStancConfig $ toString $ "stan/voterPref/" <> modelName <> "_loo") { CS.useOpenCL = False }
  stanConfig <- SC.noLogOfSummary
                <$> SM.makeDefaultModelRunnerConfig
                "stan/voterPref"
                (modelName <> "_loo")
                (Just (SB.OnlyLL, model))
                (Just $ "cces_" <> officeYearT <> "_" <> dataLabel <> ".json")
                (Just $ "cces_" <> officeYearT <> "_" <> modelName <> "_loo")
                4
                (Just 1000)
                (Just 1000)
                (Just stancConfig)
  SM.runModel @BR.SerializerC @BR.CacheData stanConfig SM.Loo (SC.noPredictions ccesDataWrangler) SC.DoNothing () ccesASER5_C
-}

model_BinomialAllBuckets :: SB.StanModel
model_BinomialAllBuckets = SB.StanModel
                           binomialASER5_StateDataBlock
                           (Just binomialASER5_StateTransformedDataBlock)
                           binomialASER5_StateParametersBlock
                           Nothing
                           binomialASER5_StateModelBlock
                           (Just binomialASER5_StateGeneratedQuantitiesBlock)
                           binomialASER5_StateGQLLBlock

model_v2 :: SB.StanModel
model_v2 = SB.StanModel
           binomialASER5_StateDataBlock
           (Just binomialASER5_StateTransformedDataBlock)
           binomialASER5_v2_StateParametersBlock
           Nothing
           binomialASER5_v2_StateModelBlock
           (Just binomialASER5_v2_StateGeneratedQuantitiesBlock)
           binomialASER5_v2_StateGQLLBlock

model_v3 :: SB.StanModel
model_v3 = SB.StanModel
           binomialASER5_StateDataBlock
           (Just binomialASER5_StateTransformedDataBlock)
           binomialASER5_v3_ParametersBlock
           Nothing
           binomialASER5_v3_ModelBlock
           (Just binomialASER5_v3_GeneratedQuantitiesBlock)
           binomialASER5_v3_GQLLBlock

model_v4 :: SB.StanModel
model_v4 = SB.StanModel
           binomialASER5_v4_DataBlock
           Nothing
           binomialASER5_v4_ParametersBlock
           Nothing
           binomialASER5_v4_ModelBlock
           (Just binomialASER5_v4_GeneratedQuantitiesBlock)
           binomialASER5_v4_GQLLBlock


model_v5 :: SB.StanModel
model_v5 = SB.StanModel
           binomialASER5_v4_DataBlock
           Nothing
           binomialASER5_v5_ParametersBlock
           Nothing
           binomialASER5_v5_ModelBlock
           (Just binomialASER5_v5_GeneratedQuantitiesBlock)
           binomialASER5_v5_GQLLBlock

model_v6 :: SB.StanModel
model_v6 = SB.StanModel
           binomialASER5_v4_DataBlock
           (Just binomialASER5_v6_TransformedDataBlock)
           binomialASER5_v6_ParametersBlock
           Nothing
           binomialASER5_v6_ModelBlock
           (Just binomialASER5_v6_GeneratedQuantitiesBlock)
           binomialASER5_v6_GQLLBlock


model_v7 :: SB.StanModel
model_v7 = SB.StanModel
           binomialASER5_v4_DataBlock
           (Just binomialASER5_v6_TransformedDataBlock)
           binomialASER5_v7_ParametersBlock
           (Just binomialASER5_v7_TransformedParametersBlock)
           binomialASER5_v7_ModelBlock
           (Just binomialASER5_v7_GeneratedQuantitiesBlock)
           binomialASER5_v7_GQLLBlock


binomialASER5_StateDataBlock :: SB.DataBlock
binomialASER5_StateDataBlock = [here|
  int<lower = 0> G; // number of cells
  int<lower = 1> J_state; // number of states
  int<lower = 1> J_sex; // number of sex categories
  int<lower = 1> J_age; // number of age categories
  int<lower = 1> J_educ; // number of education categories
  int<lower = 1> J_race; // number of race categories
  int<lower = 1, upper = J_state> state[G];
  int<lower = 1, upper = J_age * J_sex * J_educ * J_race> category[G];
  int<lower = 0> D_votes[G];
  int<lower = 0> Total_votes[G];
  int<lower = 0> M; // number of predictions
  int<lower = 0> predict_State[M];
  int<lower = 0> predict_Category[M];
|]

binomialASER5_StateTransformedDataBlock :: SB.TransformedDataBlock
binomialASER5_StateTransformedDataBlock = [here|
  int <lower=1> nCat;
  nCat =  J_age * J_sex * J_educ * J_race;
|]

binomialASER5_StateParametersBlock :: SB.ParametersBlock
binomialASER5_StateParametersBlock = [here|
  vector[nCat] beta;
  real<lower=0> sigma_alpha;
  matrix<multiplier=sigma_alpha>[J_state, nCat] alpha;
|]

binomialASER5_StateModelBlock :: SB.ModelBlock
binomialASER5_StateModelBlock = [here|
  sigma_alpha ~ normal (0, 10);
  to_vector(alpha) ~ normal (0, sigma_alpha);
  for (g in 1:G) {
   D_votes[g] ~ binomial_logit(Total_votes[g], beta[category[g]] + alpha[state[g], category[g]]);
  }
|]

binomialASER5_StateGeneratedQuantitiesBlock :: SB.GeneratedQuantitiesBlock
binomialASER5_StateGeneratedQuantitiesBlock = [here|
  vector <lower = 0, upper = 1> [M] predicted;
  for (p in 1:M) {
    predicted[p] = inv_logit(beta[predict_Category[p]] + alpha[predict_State[p], predict_Category[p]]);
  }
|]

binomialASER5_StateGQLLBlock :: SB.GeneratedQuantitiesBlock
binomialASER5_StateGQLLBlock = [here|
  vector[G] log_lik;
  for (g in 1:G) {
      log_lik[g] =  binomial_logit_lpmf(D_votes[g] | Total_votes[g], beta[category[g]] + alpha[state[g], category[g]]);
  }
|]


binomialASER5_v2_StateParametersBlock :: SB.ParametersBlock
binomialASER5_v2_StateParametersBlock = [here|
  vector[nCat] beta;
  real<lower=0> sigma_alpha;
  vector<multiplier=sigma_alpha>[J_state] alpha;
|]

binomialASER5_v2_StateModelBlock :: SB.ModelBlock
binomialASER5_v2_StateModelBlock = [here|
  sigma_alpha ~ normal (0, 10);
  alpha ~ normal (0, sigma_alpha);
  D_votes ~ binomial_logit(Total_votes, beta[category] + alpha[state]);
|]

binomialASER5_v2_StateGeneratedQuantitiesBlock :: SB.GeneratedQuantitiesBlock
binomialASER5_v2_StateGeneratedQuantitiesBlock = [here|
  vector <lower = 0, upper = 1> [nCat] nationalProbs;
  matrix <lower = 0, upper = 1> [J_state, nCat] stateProbs;
  nationalProbs = inv_logit(beta[category]);
  stateProbs = inv_logit(beta[category] + alpha[state])
|]

binomialASER5_v2_StateGQLLBlock :: SB.GeneratedQuantitiesBlock
binomialASER5_v2_StateGQLLBlock = [here|
  vector[G] log_lik;
  for (g in 1:G) {
    log_lik[g] =  binomial_logit_lpmf(D_votes[g] | Total_votes[g], beta[category[g]] + alpha[state[g]]);
  }
|]


binomialASER5_v3_ParametersBlock :: SB.ParametersBlock
binomialASER5_v3_ParametersBlock = [here|
  vector[nCat] beta;
|]

binomialASER5_v3_ModelBlock :: SB.ModelBlock
binomialASER5_v3_ModelBlock = [here|
  D_votes ~ binomial_logit(Total_votes, beta[category]);
|]

binomialASER5_v3_GeneratedQuantitiesBlock :: SB.GeneratedQuantitiesBlock
binomialASER5_v3_GeneratedQuantitiesBlock = [here|
  vector <lower = 0, upper = 1> [nCat] nationalProbs;
  nationalProbs = inv_logit(beta[category]);
|]

binomialASER5_v3_GQLLBlock :: SB.GeneratedQuantitiesBlock
binomialASER5_v3_GQLLBlock = [here|
  vector[G] log_lik;
  for (g in 1:G) {
    log_lik[g] =  binomial_logit_lpmf(D_votes[g] | Total_votes[g], beta[category[g]]);
  }
|]

binomialASER5_v4_DataBlock :: SB.DataBlock
binomialASER5_v4_DataBlock = [here|
  int<lower = 0> G; // number of cells
  int<lower = 1> J_state; // number of states
  int<lower = 1, upper = J_state> state[G];
  int<lower = 1> K; // number of cols in predictor matrix
  matrix[G, K] X;
  int<lower = 0> D_votes[G];
  int<lower = 0> Total_votes[G];
  int<lower = 0> M;
  int<lower = 0> predict_State[M];
  matrix[M, K] predict_X;

|]



binomialASER5_v4_ParametersBlock :: SB.ParametersBlock
binomialASER5_v4_ParametersBlock = [here|
  real alpha;
  vector[K] beta;
|]

binomialASER5_v4_ModelBlock :: SB.ModelBlock
binomialASER5_v4_ModelBlock = [here|
    D_votes ~ binomial_logit(Total_votes, alpha + X * beta);
|]


binomialASER5_v4_GeneratedQuantitiesBlock :: SB.GeneratedQuantitiesBlock
binomialASER5_v4_GeneratedQuantitiesBlock = [here|
  vector<lower = 0, upper = 1>[M] predicted;
  for (p in 1:M) {
    real xBeta;
//    for (k in 1:K) {
//      xBeta = predict_X[p, k] * beta[k];
//    }
    predicted[p] = inv_logit(alpha + predict_X[p] * beta);
  }
|]


binomialASER5_v4_GQLLBlock :: SB.GeneratedQuantitiesBlock
binomialASER5_v4_GQLLBlock = [here|
  vector[G] log_lik;
  for (g in 1:G) {
    log_lik[g] =  binomial_logit_lpmf(D_votes[g] | Total_votes[g], alpha + X[g] * beta);
  }
|]


binomialASER5_v5_ParametersBlock :: SB.ParametersBlock
binomialASER5_v5_ParametersBlock = [here|
  real alpha;
  vector[K] beta;
  real<lower=0> sigma_aState;
  vector<multiplier=sigma_aState> [J_state] aState;
|]

binomialASER5_v5_ModelBlock :: SB.ModelBlock
binomialASER5_v5_ModelBlock = [here|
  alpha ~ normal(0,2);
  beta ~ normal(0,1);
  sigma_aState ~ normal(0, 10);
  aState ~ normal(0, sigma_aState);
  D_votes ~ binomial_logit(Total_votes, alpha + (X * beta) + aState[state]);
|]


binomialASER5_v5_GeneratedQuantitiesBlock :: SB.GeneratedQuantitiesBlock
binomialASER5_v5_GeneratedQuantitiesBlock = [here|
  vector<lower = 0, upper = 1>[M] predicted;
  predicted = inv_logit(alpha + (predict_X * beta) + aState[predict_State]);
|]


binomialASER5_v5_GQLLBlock :: SB.GeneratedQuantitiesBlock
binomialASER5_v5_GQLLBlock = [here|
  vector[G] log_lik;
  for (g in 1:G) {
    log_lik[g] =  binomial_logit_lpmf(D_votes[g] | Total_votes[g], alpha + X[g] * beta + aState[state[g]]);
  }
|]

binomialASER5_v6_TransformedDataBlock :: SB.TransformedDataBlock
binomialASER5_v6_TransformedDataBlock = [here|
  vector[G] intcpt;
  vector[M] predictIntcpt;
  matrix[G, K+1] XI; // add the intercept so the covariance matrix is easier to deal with
  matrix[M, K+1] predict_XI;
  for (g in 1:G)
    intcpt[g] = 1;
  XI = append_col(intcpt, X);
  for (m in 1:M)
    predictIntcpt[m] = 1;
  predict_XI = append_col(predictIntcpt, predict_X);
|]

binomialASER5_v6_ParametersBlock :: SB.ParametersBlock
binomialASER5_v6_ParametersBlock = [here|
  real alpha; // overall intercept
  vector[K] beta; // fixed effects
  vector<lower=0> [K+1] sigma;
  vector[K+1] betaState[J_state]; // state-level coefficients
|]

binomialASER5_v6_ModelBlock :: SB.ModelBlock
binomialASER5_v6_ModelBlock = [here|
  alpha ~ normal(0,2); // weak prior around 50%
  beta ~ normal(0,1);
  sigma ~ normal(0,10);
  for (s in 1:J_state)
    betaState[s] ~ normal(0, sigma);
  {
    vector[G] xiBetaState;
    for (g in 1:G)
      xiBetaState[g] = XI[g] * betaState[state[g]];
    D_votes ~ binomial_logit(Total_votes, alpha + (X * beta) + xiBetaState);
  }
|]


binomialASER5_v6_GeneratedQuantitiesBlock :: SB.GeneratedQuantitiesBlock
binomialASER5_v6_GeneratedQuantitiesBlock = [here|
  vector<lower = 0, upper = 1>[M] predicted;
  for (m in 1:M)
    predicted[m] = inv_logit(alpha + (predict_X[m] * beta) + (predict_XI[m] * betaState[predict_State[m]]));
|]


binomialASER5_v6_GQLLBlock :: SB.GeneratedQuantitiesBlock
binomialASER5_v6_GQLLBlock = [here|
  vector[G] log_lik;
  for (g in 1:G) {
    log_lik[g] =  binomial_logit_lpmf(D_votes[g] | Total_votes[g], alpha + X[g] * beta + XI[g] * betaState[state[g]]);
  }
|]


binomialASER5_v7_ParametersBlock :: SB.ParametersBlock
binomialASER5_v7_ParametersBlock = [here|
  real alpha; // overall intercept
  vector[K] beta; // fixed effects
  vector<lower=0, upper=pi()/2> [K+1] tau_unif; // group effects scales
  cholesky_factor_corr[K+1] L_Omega; // group effect correlations
  matrix[K+1, J_state] z; // state-level coefficients pre-transform
|]

binomialASER5_v7_TransformedParametersBlock :: SB.TransformedParametersBlock
binomialASER5_v7_TransformedParametersBlock = [here|
  vector<lower=0>[K+1] tau;
  matrix[J_state, K+1] betaState; // state-level coefficients
  for (k in 1:(K+1))
    tau[k] = 2.5 * tan(tau_unif[k]);
  betaState = (diag_pre_multiply(tau, L_Omega) * z)';
|]



binomialASER5_v7_ModelBlock :: SB.ModelBlock
binomialASER5_v7_ModelBlock = [here|
  alpha ~ normal(0,2); // weak prior around 50%
  beta ~ normal(0,1);
  to_vector(z) ~ std_normal();
  L_Omega ~ lkj_corr_cholesky(2);
  D_votes ~ binomial_logit(Total_votes, alpha + X * beta + rows_dot_product(betaState[state], XI));
|]


binomialASER5_v7_GeneratedQuantitiesBlock :: SB.GeneratedQuantitiesBlock
binomialASER5_v7_GeneratedQuantitiesBlock = [here|
  vector<lower = 0, upper = 1>[M] predicted;
  for (m in 1:M)
    predicted[m] = inv_logit(alpha + (predict_X[m] * beta) + dot_product(predict_XI[m], betaState[predict_State[m]]));
|]


binomialASER5_v7_GQLLBlock :: SB.GeneratedQuantitiesBlock
binomialASER5_v7_GQLLBlock = [here|
  vector[G] log_lik;
  for (g in 1:G) {
    log_lik[g] =  binomial_logit_lpmf(D_votes[g] | Total_votes[g], alpha + X[g] * beta + dot_product(XI[g], betaState[state[g]]));
  }
|]

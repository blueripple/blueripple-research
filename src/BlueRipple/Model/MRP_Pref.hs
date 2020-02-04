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

module BlueRipple.Model.MRP_Pref where

import qualified BlueRipple.Data.Keyed         as K

import qualified Control.Foldl                 as FL
import           Control.Monad                  ( join )
import qualified Control.Monad.State           as State
import qualified Data.Array                    as A
import           Data.Function                  ( on )
import qualified Data.List                     as L
import qualified Data.Set                      as S
import qualified Data.Map                      as M
import           Data.Maybe                     ( isJust
                                                , catMaybes
                                                , fromMaybe
                                                )
--import           Data.Proxy                     ( Proxy(..) )
--import  Data.Ord (Compare)

import qualified Data.Text                     as T
import qualified Data.Serialize                as SE
import qualified Data.Vector                   as V
import qualified Data.Vector.Storable          as VS


import qualified Frames                        as F
import qualified Frames.Melt                   as F
import qualified Frames.InCore                 as FI
import qualified Data.Vinyl                    as V
import qualified Data.Vinyl.TypeLevel          as V


import qualified Control.MapReduce             as MR
import qualified Frames.Transform              as FT
import qualified Frames.Folds                  as FF
import qualified Frames.MapReduce              as FMR
import qualified Frames.Enumerations           as FE
import qualified Frames.Utils                  as FU
import qualified Frames.Serialize              as FS

import qualified Knit.Report                   as K
import qualified Polysemy.Error                as P
                                                ( mapError )
import qualified Polysemy                      as P
                                                ( raise )
import           Text.Pandoc.Error             as PE
import qualified Text.Blaze.Colonnade          as BC

import qualified Numeric.LinearAlgebra         as LA

import qualified Data.IndexedSet               as IS
import qualified Numeric.GLM.ProblemTypes      as GLM
import qualified Numeric.GLM.ModelTypes        as GLM
import qualified Numeric.GLM.FunctionFamily    as GLM
import           Numeric.GLM.MixedModel        as GLM
import qualified Numeric.GLM.Bootstrap         as GLM
import qualified Numeric.GLM.Report            as GLM
import qualified Numeric.GLM.Predict           as GLM
import qualified Numeric.GLM.Confidence        as GLM
import qualified Numeric.SparseDenseConversions
                                               as SD

import qualified Statistics.Types              as ST
import           GHC.Generics                   ( Generic )

-- map reduce folds for counting
type Count = "Count" F.:-> Int
type Successes = "Successes" F.:-> Int
type MeanWeight = "MeanWeight" F.:-> Double
type VarWeight = "VarWeight" F.:-> Double
type WeightedSuccesses = "WeightedSuccesses" F.:-> Double
type UnweightedSuccesses =  "UnweightedSuccesses" F.:-> Int

binomialFold
  :: (F.Record r -> Bool) -> FL.Fold (F.Record r) (F.Record '[Count, Successes])
binomialFold testRow =
  let successesF = FL.premap (\r -> if testRow r then 1 else 0) FL.sum
  in  (\n s -> n F.&: s F.&: V.RNil) <$> FL.length <*> successesF

countFold
  :: forall k r d
   . ( Ord (F.Record k)
     , FI.RecVec (k V.++ '[Count, Successes])
     , k F.⊆ r
     , d F.⊆ r
     )
  => (F.Record d -> Bool)
  -> FL.Fold (F.Record r) [F.FrameRec (k V.++ '[Count, Successes])]
countFold testData = MR.mapReduceFold
  MR.noUnpack
  (FMR.assignKeysAndData @k)
  (FMR.foldAndAddKey $ binomialFold testData)

type CountCols = '[Count, UnweightedSuccesses, WeightedSuccesses, MeanWeight, VarWeight]

weightedBinomialFold
  :: (F.Record r -> Bool)
  -> (F.Record r -> Double)
  -> FL.Fold (F.Record r) (F.Record CountCols)
weightedBinomialFold testRow weightRow =
  let wSuccessesF =
        FL.premap (\r -> if testRow r then weightRow r else 0) FL.sum
      successesF  = FL.premap (\r -> if testRow r then 1 else 0) FL.sum
      meanWeightF = FL.premap weightRow FL.mean
      varWeightF  = FL.premap weightRow FL.variance
  in  (\n s ws mw vw -> n F.&: s F.&: (ws / mw) F.&: mw F.&: vw F.&: V.RNil)
      <$> FL.length
      <*> successesF
      <*> wSuccessesF
      <*> meanWeightF
      <*> varWeightF

weightedCountFold
  :: forall k r d
   . (Ord (F.Record k), FI.RecVec (k V.++ CountCols), k F.⊆ r, d F.⊆ r)
  => (F.Record r -> Bool) -- ^ count this row?
  -> (F.Record d -> Bool) -- ^ success ?
  -> (F.Record d -> Double) -- ^ weight
  -> FL.Fold (F.Record r) (F.FrameRec (k V.++ CountCols))
weightedCountFold filterData testData weightData =
  FMR.concatFold $ FMR.mapReduceFold
    (MR.filterUnpack filterData)
    (FMR.assignKeysAndData @k)
    (FMR.foldAndAddKey $ weightedBinomialFold testData weightData)

zeroCount :: F.Record CountCols
zeroCount = 0 F.&: 0 F.&: 0 F.&: 1 F.&: 0 F.&: V.RNil

getFraction r =
  let n = F.rgetField @Count r
  in  if n == 0
        then 0
        else (realToFrac $ F.rgetField @Successes r) / (realToFrac n)

getFractionWeighted r =
  let n = F.rgetField @Count r
  in  if n == 0 then 0 else (F.rgetField @WeightedSuccesses r) / (realToFrac n)

data RecordColsProxy k = RecordColsProxy deriving (Show, Enum, Bounded, A.Ix, Eq, Ord)
type instance GLM.GroupKey (RecordColsProxy k) = F.Record k

--type instance GLM.GroupKey (Proxy k) = F.Record k

recordToGroupKey
  :: forall k r . (k F.⊆ r) => F.Record r -> RecordColsProxy k -> F.Record k
recordToGroupKey r _ = F.rcast @k r

glmErrorToPandocError :: GLM.GLMError -> PE.PandocError
glmErrorToPandocError x = PE.PandocSomeError $ T.pack $ show x


type GroupCols gs cs = gs V.++ cs
type CountedCols gs cs = (GroupCols gs cs) V.++ CountCols

-- TODO: Add bootstraps back in, make optional
-- Maybe set up way to serialize all this at this level??

-- ls: are keys for locations (groups with no matching fixed-effect)
-- cs: are keys for fixedEffects, one for each constructor in b
-- b: Fixed Effect type
-- g: group type
inferMR
  :: forall ls cs ks b g f rs effs
   . ( Foldable f
     , Functor f
     , K.KnitEffects effs
     , F.RDeleteAll ls (ls V.++ ks V.++ CountCols) ~ (ks V.++ CountCols)
     , (ls V.++ ks V.++ CountCols) ~ (ls V.++ (ks V.++ CountCols))
     , Ord (F.Record ls)
     , ls F.⊆ (ls V.++ ks V.++ CountCols)
     , (ks V.++ CountCols) F.⊆ (ls V.++ ks V.++ CountCols)
     , ks F.⊆ (ks V.++ CountCols)
     , (ls V.++ cs) F.⊆ (ls V.++ (ks V.++ CountCols))
     , Show (F.Record (ls V.++ ks V.++ CountCols))
     , FI.RecVec (ls V.++ (ks V.++ CountCols))
     , F.ElemOf (ks V.++ CountCols) Count
     , F.ElemOf (ks V.++ CountCols) MeanWeight
     , F.ElemOf (ks V.++ CountCols) VarWeight
     , F.ElemOf (ks V.++ CountCols) WeightedSuccesses
     , F.ElemOf (ks V.++ CountCols) UnweightedSuccesses
     , F.ElemOf (ls V.++ ks V.++ CountCols) Count
     , F.ElemOf (ls V.++ ks V.++ CountCols) MeanWeight
     , F.ElemOf (ls V.++ ks V.++ CountCols) VarWeight
     , F.ElemOf (ls V.++ ks V.++ CountCols) WeightedSuccesses
     , F.ElemOf (ls V.++ ks V.++ CountCols) UnweightedSuccesses
     , Show (F.Record (ls V.++ cs))
     , K.FiniteSet (F.Record ks)
     , Ord b
     , Show b
     , Enum b
     , Bounded b
     , Ord (F.Record (GroupCols ls cs))
     , g ~ RecordColsProxy (GroupCols ls cs)
     )
  => FL.Fold (F.Record rs) (F.FrameRec (ls V.++ ks V.++ CountCols))
  -> [GLM.WithIntercept b] -- fixed effects to fit
  -> (F.Record (ls V.++ ks V.++ CountCols) -> b -> Double)  -- how to get a fixed effect value from a record
  -> f (F.Record rs)
  -> K.Sem
       effs
       ( GLM.MixedModel b g
       , GLM.RowClassifier g
       , GLM.EffectsByGroup g b
       , GLM.BetaU
       , LA.Vector Double
       , [(GLM.BetaU, LA.Vector Double)]
       )
inferMR cf fixedEffectList getFixedEffect rows =
  P.mapError glmErrorToPandocError
    $ K.wrapPrefix ("inferMR")
    $ do
        let
          addZeroCountsF = FMR.concatFold $ FMR.mapReduceFold
            (FMR.noUnpack)
            (FMR.splitOnKeys @ls)
            ( FMR.makeRecsWithKey id
            $ FMR.ReduceFold
            $ const
            $ K.addDefaultRec @ks zeroCount
            )
          counted = FL.fold FL.list $ FL.fold addZeroCountsF $ FL.fold cf rows
--        K.logLE K.Diagnostic $ T.intercalate "\n" $ fmap (T.pack . show) counted
        let
          vCounts = VS.fromList $ fmap (F.rgetField @Count) counted
          designEffect mw vw = 1 + (vw / (mw * mw))
          vWeights = VS.fromList $ fmap
            (\r ->
              let mw = F.rgetField @MeanWeight r
                  vw = F.rgetField @VarWeight r
              in  1 / sqrt (designEffect mw vw)
            )
            counted -- VS.replicate (VS.length vCounts) 1.0
          fixedEffects = GLM.FixedEffects $ IS.fromList fixedEffectList
          groups       = IS.fromList [RecordColsProxy]
          (observations, fixedEffectsModelMatrix, rcM) = FL.fold
            (lmePrepFrame getFractionWeighted
                          fixedEffects
                          groups
                          getFixedEffect
                          (recordToGroupKey @(GroupCols ls cs))
            )
            counted
          regressionModelSpec = GLM.RegressionModelSpec
            fixedEffects
            fixedEffectsModelMatrix
            observations
        rowClassifier <- case rcM of
          Left  msg -> K.knitError msg
          Right x   -> return x
        let effectsByGroup =
              M.fromList [(RecordColsProxy, IS.fromList [GLM.Intercept])]
        fitSpecByGroup <- GLM.fitSpecByGroup @b @g fixedEffects
                                                   effectsByGroup
                                                   rowClassifier
        let lmmControls = GLM.LMMControls GLM.LMM_BOBYQA 1e-6
            lmmSpec     = GLM.LinearMixedModelSpec
              (GLM.MixedModelSpec regressionModelSpec fitSpecByGroup)
              lmmControls
            cc = GLM.PIRLSConvergenceCriterion GLM.PCT_Deviance 1e-6 20
            glmmControls = GLM.GLMMControls GLM.UseCanonical 10 cc
            glmmSpec = GLM.GeneralizedLinearMixedModelSpec
              lmmSpec
              vWeights
              (GLM.Binomial vCounts)
              glmmControls
            mixedModel = GLM.GeneralizedLinearMixedModel glmmSpec
        randomEffectsModelMatrix <- GLM.makeZ fixedEffectsModelMatrix
                                              fitSpecByGroup
                                              rowClassifier
        let randomEffectCalc = GLM.RandomEffectCalculated
              randomEffectsModelMatrix
              (GLM.makeLambda fitSpecByGroup)
            th0         = GLM.setCovarianceVector fitSpecByGroup 1 0
            mdVerbosity = MDVNone
        GLM.checkProblem mixedModel randomEffectCalc
        K.logLE K.Info "Fitting data..."
        ((th, pd, sigma2, betaU, vb, cs), vMuSol, cf) <- GLM.minimizeDeviance
          mdVerbosity
          ML
          mixedModel
          randomEffectCalc
          th0
        GLM.report mixedModel
                   randomEffectsModelMatrix
                   (GLM.bu_vBeta betaU)
                   (SD.toSparseVector vb)
        let fes = GLM.fixedEffectStatistics mixedModel sigma2 cs betaU
        K.logLE K.Diagnostic $ "FixedEffectStatistics: " <> (T.pack $ show fes)
        epg <- GLM.effectParametersByGroup @g @b rowClassifier effectsByGroup vb
        K.logLE K.Diagnostic
          $  "EffectParametersByGroup: "
          <> (T.pack $ show epg)
        gec <- GLM.effectCovariancesByGroup effectsByGroup mixedModel sigma2 th
        K.logLE K.Diagnostic
          $  "EffectCovariancesByGroup: "
          <> (T.pack $ show gec)
        rebl <- GLM.randomEffectsByLabel epg rowClassifier
        K.logLE K.Diagnostic
          $  "Random Effects:\n"
          <> GLM.printRandomEffectsByLabel rebl
        smCondVar <- GLM.conditionalCovariances mixedModel
                                                cf
                                                randomEffectCalc
                                                th
                                                betaU
        let bootstraps                           = []
        let GLM.FixedEffectStatistics _ mBetaCov = fes
        let f r = do
              let obs = getFractionWeighted r
              predictCVCI <- GLM.predictWithCI
                mixedModel
                (Just . getFixedEffect r)
                (Just . recordToGroupKey @(GroupCols ls cs) r)
                rowClassifier
                effectsByGroup
                betaU
                vb
                (ST.mkCL 0.95)
                (GLM.NaiveCondVarCI mBetaCov smCondVar)
              return (r, obs, predictCVCI)
        fitted <- traverse f (FL.fold FL.list counted)
        K.logLE K.Diagnostic
          $  "Fitted:\n"
          <> (T.intercalate "\n" $ fmap (T.pack . show) fitted)
        fixedEffectTable <- GLM.printFixedEffects fes
        K.logLE K.Diagnostic $ "FixedEffects:\n" <> fixedEffectTable
        let GLM.FixedEffectStatistics fep _ = fes
        return
          (mixedModel, rowClassifier, effectsByGroup, betaU, vb, bootstraps) -- fes, epg, rowClassifier, bootstraps)

lmePrepFrame
  :: forall p g rs
   . (GLM.PredictorC p, GLM.GroupC g)
  => (F.Record rs -> Double) -- ^ observations
  -> GLM.FixedEffects p
  -> IS.IndexedSet g
  -> (F.Record rs -> p -> Double) -- ^ predictors
  -> (F.Record rs -> g -> GLM.GroupKey g)  -- ^ classifiers
  -> FL.Fold
       (F.Record rs)
       ( LA.Vector Double
       , LA.Matrix Double
       , Either T.Text (GLM.RowClassifier g)
       ) -- ^ (X,y,(row-classifier, size of class))
lmePrepFrame observationF fe groupIndices getPredictorF classifierLabelF
  = let
      makeInfoVector
        :: M.Map g (M.Map (GLM.GroupKey g) Int)
        -> M.Map g (GLM.GroupKey g)
        -> Either T.Text (V.Vector (GLM.ItemInfo g))
      makeInfoVector indexMaps groupKeys =
        let
          g (grp, groupKey) =
            GLM.ItemInfo
              <$> (   maybe
                      (Left $ "Failed on " <> (T.pack $ show (grp, groupKey)))
                      Right
                  $   M.lookup grp indexMaps
                  >>= M.lookup groupKey
                  )
              <*> pure groupKey
        in  fmap V.fromList $ traverse g $ M.toList groupKeys
      makeRowClassifier
        :: Traversable f
        => M.Map g (M.Map (GLM.GroupKey g) Int)
        -> f (M.Map g (GLM.GroupKey g))
        -> Either T.Text (GLM.RowClassifier g)
      makeRowClassifier indexMaps labels = do
        let sizes = fmap M.size indexMaps
        indexed <- traverse (makeInfoVector indexMaps) labels
        return $ GLM.RowClassifier groupIndices
                                   sizes
                                   (V.fromList $ FL.fold FL.list indexed)
                                   indexMaps
      getPredictorF' _   GLM.Intercept     = 1
      getPredictorF' row (GLM.Predictor x) = getPredictorF row x
      predictorF row = LA.fromList $ case fe of
        GLM.FixedEffects indexedFixedEffects ->
          fmap (getPredictorF' row) $ IS.members indexedFixedEffects
        GLM.InterceptOnly -> [1]
      getClassifierLabels :: F.Record rs -> M.Map g (GLM.GroupKey g)
      getClassifierLabels r =
        M.fromList $ fmap (\g -> (g, classifierLabelF r g)) $ IS.members
          groupIndices
      foldObs   = fmap LA.fromList $ FL.premap observationF FL.list
      foldPred  = fmap LA.fromRows $ FL.premap predictorF FL.list
      foldClass = FL.premap getClassifierLabels FL.list
      g (vY, mX, ls) =
        ( vY
        , mX
        , makeRowClassifier
          (snd $ State.execState (addAll ls) (M.empty, M.empty))
          ls
        )
    in
      fmap g $ ((,,) <$> foldObs <*> foldPred <*> foldClass)



addOne
  :: GLM.GroupC g
  => (g, GLM.GroupKey g)
  -> State.State (M.Map g Int, M.Map g (M.Map (GLM.GroupKey g) Int)) ()
addOne (grp, label) = do
  (nextIndexMap, groupIndexMaps) <- State.get
  let groupIndexMap = fromMaybe M.empty $ M.lookup grp groupIndexMaps
  case M.lookup label groupIndexMap of
    Nothing -> do
      let index         = fromMaybe 0 $ M.lookup grp nextIndexMap
          nextIndexMap' = M.insert grp (index + 1) nextIndexMap
          groupIndexMaps' =
            M.insert grp (M.insert label index groupIndexMap) groupIndexMaps
      State.put (nextIndexMap', groupIndexMaps')
      return ()
    _ -> return ()

addMany
  :: (GLM.GroupC g, Traversable h)
  => h (g, GLM.GroupKey g)
  -> State.State (M.Map g Int, M.Map g (M.Map (GLM.GroupKey g) Int)) ()
addMany x = traverse addOne x >> return ()

addAll
  :: GLM.GroupC g
  => [M.Map g (GLM.GroupKey g)]
  -> State.State (M.Map g Int, M.Map g (M.Map (GLM.GroupKey g) Int)) ()
addAll x = traverse (addMany . M.toList) x >> return ()

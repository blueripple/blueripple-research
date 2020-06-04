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
{-# LANGUAGE QuasiQuotes               #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE StandaloneDeriving        #-}
{-# LANGUAGE TypeApplications          #-}
{-# LANGUAGE TypeFamilies              #-}
{-# LANGUAGE TypeOperators             #-}
{-# LANGUAGE StandaloneDeriving        #-}
{-# LANGUAGE TupleSections             #-}
{-# LANGUAGE UndecidableInstances        #-}

module BlueRipple.Model.MRP where

import qualified BlueRipple.Data.Keyed         as K
import qualified BlueRipple.Data.DataFrames    as BR

import qualified Control.Foldl                 as FL
import           Control.Monad                  ( join )
import qualified Control.Monad.State           as State
import qualified Data.Array                    as A
import           Data.Function                  ( on )
import qualified Data.List                     as L
import qualified Data.Set                      as S
import qualified Data.IntMap                   as IM
import qualified Data.Map                      as M
import           Data.Maybe                     ( isJust
                                                , catMaybes
                                                , fromMaybe
                                                , fromJust
                                                )
import qualified Data.Text                     as T
import qualified Data.Profunctor               as P
import qualified Data.Serialize                as SE
import           Data.Serialize.Text ()
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
--import qualified Frames.Misc                  as FU
import qualified Frames.Serialize              as FS

import qualified Knit.Report                   as K hiding (elements)
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
import           GHC.Generics                   ( Generic, Rep )

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

zeroCount :: F.Record CountCols
zeroCount = 0 F.&: 0 F.&: 0 F.&: 1 F.&: 0 F.&: V.RNil

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
      f s ws mw = if mw < 1e-6 then realToFrac s else ws / mw -- if meanweight is 0 but 
  in  (\n s ws mw vw -> n F.&: s F.&: (f s ws mw) F.&: mw F.&: vw F.&: V.RNil)
      <$> FL.length
      <*> successesF
      <*> wSuccessesF
      <*> meanWeightF
      <*> varWeightF

weightedCountFold
  :: forall k r d
   . (Ord (F.Record k)
     , FI.RecVec (k V.++ CountCols)
     , k F.⊆ r
     , k F.⊆ (k V.++ r)
     , d F.⊆ (k V.++ r)
     )
  => (F.Record r -> Bool) -- ^ count this row?
  -> (F.Record d -> Bool) -- ^ success ?
  -> (F.Record d -> Double) -- ^ weight
  -> FL.Fold (F.Record r) (F.FrameRec (k V.++ CountCols))
weightedCountFold {- filterData testData weightData-} = weightedCountFoldGeneral (F.rcast @k)
{-  FMR.concatFold $ FMR.mapReduceFold
    (MR.filterUnpack filterData)
    (FMR.assignKeysAndData @k)
    (FMR.foldAndAddKey $ weightedBinomialFold testData weightData)
-}

weightedCountFoldGeneral
  :: forall k r d
   . (Ord (F.Record k)
     , FI.RecVec (k V.++ CountCols)
     , k F.⊆ (k V.++ r)
     , d F.⊆ (k V.++ r)
     )
  => (F.Record r -> F.Record k)
  -> (F.Record r -> Bool) -- ^ include this row?
  -> (F.Record d -> Bool) -- ^ success ?
  -> (F.Record d -> Double) -- ^ weight
  -> FL.Fold (F.Record r) (F.FrameRec (k V.++ CountCols))
weightedCountFoldGeneral getKey filterData testData weightData =
  FL.prefilter filterData $ FMR.concatFold $ FMR.mapReduceFold
    (MR.Unpack $ \r ->  [getKey r `V.rappend` r])
    (FMR.assignKeysAndData @k @d)
    (FMR.foldAndAddKey $ weightedBinomialFold testData weightData)

  

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



---
newtype  SimplePredictor ps = SimplePredictor { unSimplePredictor :: F.Record ps }

instance (Show (F.Record ps)) => Show (SimplePredictor ps) where
  show (SimplePredictor x) = "SimplePredictor " ++ (show x)
  
instance (Eq (F.Record ps)) => Eq (SimplePredictor ps) where
  (SimplePredictor x) == (SimplePredictor y) = x == y
  
instance (Ord (F.Record ps)) => Ord (SimplePredictor ps) where
  compare (SimplePredictor x) (SimplePredictor y) = compare x y

instance (Ord (F.Record ps), K.FiniteSet (F.Record ps)) => Enum (SimplePredictor ps) where
  toEnum n =
    let im = IM.fromList $ zip [0..] $ fmap SimplePredictor $ S.toAscList K.elements
    in fromJust $ IM.lookup n im
  fromEnum a =
    let m = M.fromList $ zip (fmap SimplePredictor $ S.toAscList K.elements) [0..]
    in fromJust $ M.lookup a m

instance (K.FiniteSet (F.Record ps)) => Bounded (SimplePredictor ps) where
  minBound = head $ fmap SimplePredictor $ S.toList $ K.elements
  maxBound = last $ fmap SimplePredictor $ S.toList $ K.elements


allSimplePredictors :: K.FiniteSet (F.Record ps) => [SimplePredictor ps]
allSimplePredictors = fmap SimplePredictor $ S.toList K.elements 

type SimpleEffect ps = GLM.WithIntercept (SimplePredictor ps)

simplePredictor :: forall ps rs. (ps F.⊆ rs
                                     , Eq (F.Record ps)
                                     )
                    => F.Record rs -> SimplePredictor ps -> Double
simplePredictor r p = if (F.rcast @ps r == unSimplePredictor p) then 1 else 0

predMap :: forall cs. (K.FiniteSet (F.Record cs), Ord (F.Record cs), cs F.⊆ cs)
  => F.Record cs -> M.Map (SimplePredictor cs) Double
predMap r =  M.fromList $ fmap (\p -> (p, simplePredictor r p)) allSimplePredictors

catPredMaps :: forall cs.  (K.FiniteSet (F.Record cs), Ord (F.Record cs), cs F.⊆ cs)
  => M.Map (F.Record cs) (M.Map (SimplePredictor cs) Double)
catPredMaps = M.fromList $ fmap (\k -> (unSimplePredictor k,predMap (unSimplePredictor k))) allSimplePredictors  

data  LocationHolder c f a =  LocationHolder { locName :: T.Text
                                             , locKey :: Maybe (F.Rec f LocationCols)
                                             , catData :: M.Map (F.Rec f c) a
                                             } deriving (Generic)

deriving instance (V.RMap c
                  , V.ReifyConstraint Show F.ElField c
                  , V.RecordToList c
                  , Show a) => Show (LocationHolder c F.ElField a)
                  
instance (SE.Serialize a
         , Ord (F.Rec FS.SElField c)
         , SE.GSerializePut
           (Rep (F.Rec FS.SElField c))
         , SE.GSerializeGet (Rep (F.Rec FS.SElField c))
         , (Generic (F.Rec FS.SElField c))
         ) => SE.Serialize (LocationHolder c FS.SElField a)

lhToS :: (Ord (F.Rec FS.SElField c)
         , V.RMap c
         )
      => LocationHolder c F.ElField a -> LocationHolder c FS.SElField a
lhToS (LocationHolder n lkM cdm) = LocationHolder n (fmap FS.toS lkM) (M.mapKeys FS.toS cdm)

lhFromS :: (Ord (F.Rec F.ElField c)
           , V.RMap c
         ) => LocationHolder c FS.SElField a -> LocationHolder c F.ElField a
lhFromS (LocationHolder n lkM cdm) = LocationHolder n (fmap FS.fromS lkM) (M.mapKeys FS.fromS cdm)

type LocationCols = '[BR.StateAbbreviation]
locKeyPretty :: F.Record LocationCols -> T.Text
locKeyPretty r =
  let stateAbbr = F.rgetField @BR.StateAbbreviation r
  in stateAbbr

--type ASER = '[BR.SimpleAgeC, BR.SexC, BR.CollegeGradC, BR.SimpleRaceC]
predictionsByLocation ::
  forall ps r rs. ( V.RMap ps
                  , V.ReifyConstraint Show V.ElField ps
                  , V.RecordToList ps
                  , Ord (F.Record ps)
                  , Ord (SimplePredictor ps)               
                  , FI.RecVec (ps V.++ CountCols)
                  , F.ElemOf (ps V.++ CountCols) Count
                  , F.ElemOf (ps V.++ CountCols) MeanWeight
                  , F.ElemOf (ps V.++ CountCols) UnweightedSuccesses
                  , F.ElemOf (ps V.++ CountCols) VarWeight
                  , F.ElemOf (ps V.++ CountCols) WeightedSuccesses
                  , K.FiniteSet (F.Record ps)
                  , Show (F.Record (LocationCols V.++ ps V.++ CountCols))
                  , Show (F.Record ps)
                  , Enum (SimplePredictor ps)
                  , Bounded (SimplePredictor ps)
                  , (ps V.++ CountCols) F.⊆ (LocationCols V.++ ps V.++ CountCols)
                  , ps F.⊆ (ps V.++ CountCols)
                  , ps F.⊆ (LocationCols V.++ ps V.++ CountCols)
                  , F.ElemOf rs BR.StateAbbreviation
                  , K.KnitEffects r
               )
  => GLM.MinimizeDevianceVerbosity
  -> K.Sem r (F.FrameRec rs)
  -> FL.Fold (F.Record rs) (F.FrameRec (LocationCols V.++ ps V.++ CountCols))  
  -> [SimpleEffect ps]
  -> M.Map (F.Record ps) (M.Map (SimplePredictor ps) Double)
  -> K.Sem r [LocationHolder ps V.ElField Double]
predictionsByLocation verbosity ccesFrameAction countFold predictors catPredMap =
  P.mapError glmErrorToPandocError  $ K.wrapPrefix "predictionsByLocation" $ do
    K.logLE K.Diagnostic "Starting (getting data )"
    ccesFrame <- P.raise ccesFrameAction --F.toFrame <$> P.raise (K.useCached ccesRecordListAllCA)
    K.logLE K.Diagnostic ("Inferring")
    (mm, rc, ebg, bu, vb, bs) <- inferMR @LocationCols @ps @ps
                                 verbosity
                                 countFold
                                 predictors                                                     
                                 simplePredictor
                                 ccesFrame
  
    let states = FL.fold FL.set $ fmap (F.rgetField @BR.StateAbbreviation) ccesFrame
        allStateKeys = fmap (\s -> s F.&: V.RNil) $ FL.fold FL.list states
        predictLoc l = LocationHolder (locKeyPretty l) (Just l) catPredMap
        toPredict = [LocationHolder "National" Nothing catPredMap] <> fmap predictLoc allStateKeys                           
        predict (LocationHolder n lkM cpms) = P.mapError glmErrorToPandocError $ do
          let predictFrom catKey predMap =
                let groupKeyM = fmap (`V.rappend` catKey) lkM --lkM >>= \lk -> return $ lk `V.rappend` catKey
                    emptyAsNationalGKM = case groupKeyM of
                                           Nothing -> Nothing
                                           Just k -> fmap (const k) $ GLM.categoryNumberFromKey rc k (RecordColsProxy @(LocationCols V.++ ps))
                in GLM.runLogOnGLMException $ GLM.predictFromBetaB mm (flip M.lookup predMap) (const emptyAsNationalGKM) rc ebg bu vb
          cpreds <- M.traverseWithKey predictFrom cpms
          return $ LocationHolder n lkM cpreds
    traverse predict toPredict

---


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
  => GLM.MinimizeDevianceVerbosity
  -> FL.Fold (F.Record rs) (F.FrameRec (ls V.++ ks V.++ CountCols))
  -> [GLM.WithIntercept b] -- fixed effects to fit
  -> (F.Record (ls V.++ ks V.++ CountCols) -> b -> Double)  -- how to get a fixed effect value from a record
  -> f (F.Record rs)
  -> K.Sem
       effs
       ( GLM.MixedModel b g
       , GLM.RowClassifier g
       , GLM.EffectsByGroup g b
       , GLM.BetaVec
       , LA.Vector Double
       , [(GLM.BetaVec, LA.Vector Double)]
       )
inferMR verbosity cf fixedEffectList getFixedEffect rows =
  P.mapError glmErrorToPandocError
  $ GLM.runLogOnGLMException
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
--    K.logLE K.Diagnostic $ T.intercalate "\n" $ fmap (T.pack . show) counted
    let
      vCounts = VS.fromList $ fmap (F.rgetField @Count) counted
      designEffect mw vw = if (mw > 1e-12) then 1 + (vw / (mw * mw)) else 1
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
--        K.logLE K.Diagnostic $ "vCounts=" <> (T.pack $ show vCounts)
--        K.logLE K.Diagnostic $ "vWeights=" <> (T.pack $ show vWeights)
--        K.logLE K.Diagnostic $ "mX=" <> (T.pack $ show fixedEffectsModelMatrix)
--        K.logLE K.Diagnostic $ "vY=" <> (T.pack $ show observations)
    let regressionModelSpec = GLM.RegressionModelSpec
          fixedEffects
          fixedEffectsModelMatrix
          observations
    rowClassifier <- case rcM of
      Left  msg -> K.knitError msg
      Right x   -> return x
--        K.logLE K.Diagnostic $ "rc=" <> (T.pack $ show rowClassifier)          
    let effectsByGroup =
          M.fromList [(RecordColsProxy, IS.fromList [GLM.Intercept])]
    fitSpecByGroup <- GLM.fitSpecByGroup @b @g fixedEffects
                      effectsByGroup
                      rowClassifier
    let lmmControls = GLM.LMMControls GLM.LMM_BOBYQA 1e-6 Nothing
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
--        K.logLE K.Diagnostic $ "smZ=" <> (T.pack $ show randomEffectsModelMatrix) 
    let randomEffectCalc = GLM.RandomEffectCalculated
          randomEffectsModelMatrix
          (GLM.makeLambda fitSpecByGroup)
        th0         = GLM.setCovarianceVector fitSpecByGroup 1 0
        -- mdVerbosity = MDVNone
    GLM.checkProblem mixedModel randomEffectCalc
    K.logLE K.Info "Fitting data..."
    ((th, pd, sigma2, beta, mzvu, mzvb, cs), vMuSol, cf) <- GLM.minimizeDeviance
                                                            verbosity
                                                            ML
                                                            mixedModel
                                                            randomEffectCalc
                                                            th0
    vb <- K.knitMaybe "b-vector (random effects coefficients) is zero in MRP.inferMR" $ GLM.useMaybeZeroVec Nothing Just mzvb
    GLM.report mixedModel
      randomEffectsModelMatrix
      beta
      (SD.toSparseVector vb)
    let fes = GLM.fixedEffectStatistics mixedModel sigma2 cs beta
    K.logLE K.Diagnostic $ "FixedEffectStatistics: " <> (T.pack $ show fes)
    epg <- GLM.effectParametersByGroup @g @b rowClassifier effectsByGroup vb
--        K.logLE K.Diagnostic
--          $  "EffectParametersByGroup: "
--          <> (T.pack $ show epg)
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
                 beta
                 mzvu
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
                  beta
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
      (mixedModel, rowClassifier, effectsByGroup, beta, vb, bootstraps) -- fes, epg, rowClassifier, bootstraps)

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

-- useful data folds
weightedSumF :: (Real w, Real v, Fractional y) => FL.Fold (w, v) y
weightedSumF =
  (/)
    <$> FL.premap (\(w, v) -> realToFrac w * realToFrac v) FL.sum
    <*> P.dimap fst realToFrac FL.sum

weightedSumRecF
  :: forall w v y rs
   . ( V.KnownField w
     , V.KnownField v
     , Fractional y
     , F.ElemOf rs w
     , F.ElemOf rs v
     , Real (V.Snd w)
     , Real (V.Snd v)
     )
  => FL.Fold (F.Record rs) y
weightedSumRecF =
  FL.premap (\r -> (F.rgetField @w r, F.rgetField @v r)) weightedSumF


sumProdIfRecF
  :: forall w v t y rs
   . ( V.KnownField w
     , V.KnownField v
     , V.KnownField t
     , Fractional y
     , F.ElemOf rs w
     , F.ElemOf rs v
     , F.ElemOf rs t
     , Real (V.Snd w)
     , Real (V.Snd v)
     )
  => (V.Snd t -> Bool)
  -> FL.Fold (F.Record rs) y
sumProdIfRecF test = FL.prefilter (test . F.rgetField @t) $ FL.premap
  (\r -> (realToFrac (F.rgetField @w r) * realToFrac (F.rgetField @v r)))
  FL.sum

{-# LANGUAGE AllowAmbiguousTypes       #-}
{-# LANGUAGE DataKinds                 #-}
{-# LANGUAGE DeriveDataTypeable        #-}
{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE GADTs                     #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE PolyKinds                 #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE TypeApplications          #-}
{-# LANGUAGE TypeOperators             #-}
{-# LANGUAGE QuasiQuotes               #-}
--{-# LANGUAGE AllowAmbiguousTypes       #-}

import qualified Control.Foldl                 as FL
import qualified Control.Monad.State           as ST

import qualified Data.Array as A
import qualified Data.List                     as L
import qualified Data.Map                      as M
import           Data.Maybe (fromMaybe)
import qualified Data.Vector                   as VB
import qualified Data.Vector.Storable          as VS
import qualified Data.Text                     as T
import           Data.Data                      ( Data )
import           Data.Typeable                  ( Typeable )

import qualified Text.Printf                   as PF

import qualified Data.Vinyl                    as V
import qualified Data.Vinyl.TypeLevel          as V
import qualified Frames                        as F
import qualified Frames.CSV                    as F
import qualified Frames.Melt                   as F

import qualified Pipes                         as P
import qualified Pipes.Prelude                 as P

import qualified Numeric.LinearAlgebra         as LA

import qualified Frames                        as F
import qualified Frames.InCore                 as F
import qualified Data.Vinyl                    as V

import qualified Frames.Visualization.VegaLite.Data
                                               as FV
import qualified Frames.Visualization.VegaLite.StackedArea
                                               as FV
import qualified Frames.Visualization.VegaLite.LineVsTime
                                               as FV
import qualified Frames.Visualization.VegaLite.ParameterPlots
                                               as FV
import qualified Frames.Visualization.VegaLite.Correlation
                                               as FV

import qualified Frames.Transform              as FT
import qualified Frames.Folds                  as FF
import qualified Frames.MapReduce              as MR
import qualified Frames.Enumerations           as FE
import qualified Frames.Utils                  as FU

import qualified Knit.Report                   as K
import           Polysemy.Error                 ( Error, mapError, throw )
import           Text.Pandoc.Error             as PE

import           Data.String.Here               ( here )

import qualified System.Console.CmdArgs.Implicit as CA
import System.Console.CmdArgs.Implicit ((&=))


import           BlueRipple.Data.DataFrames
import           BlueRipple.Utilities.KnitUtils

import qualified BlueRipple.Model.TurnoutAdjustment
                                               as TA
                                               
import           MRP.CCES
import           MRP.Common

import qualified Data.IndexedSet               as IS
import qualified Numeric.GLM.ProblemTypes      as GLM
import qualified Numeric.GLM.FunctionFamily    as GLM
import           Numeric.GLM.MixedModel        as GLM  
import qualified Numeric.GLM.Report            as GLM
import qualified Numeric.SparseDenseConversions
                                               as SD

yamlAuthor :: T.Text
yamlAuthor = [here|
- name: Adam Conner-Sax
- name: Frank David
|]

templateVars =
  M.fromList [("lang", "English")
             , ("site-title", "Blue Ripple Politics")
             , ("home-url", "https://www.blueripplepolitics.org")             
--  , ("author"   , T.unpack yamlAuthor)
             ]

{- TODO
1. Why are rows still being dropped?  Which col is missing?  In general, write a diagnostic for showing what is missing...
Some answers:
The boring:  CountyFIPS.  I don't want this anyway.  Dropped
The weird:  Missing voted_rep_party.  These are non-voters or voters who didn't vote in house race.  A bit more than 1/3 of
survey responses.  Maybe that's not weird??
-}

pandocTemplate = K.FullySpecifiedTemplatePath "pandoc-templates/blueripple_basic.html"

data PostArgs = PostArgs { posts :: [Post], updated :: Bool } deriving (Show, Data, Typeable)

postArgs = PostArgs { posts = CA.enum [[] &= CA.ignore,
                                        [PostIntro] &= CA.name "intro" &= CA.help "knit \"Intro\"",
                                        [PostMethods] &= CA.name "methods" &= CA.help "knit \"Methods\"",
                                        [(minBound :: Post).. ] &= CA.name "all" &= CA.help "knit all"
                                      ]
                    , updated = CA.def
                      &= CA.name "u"
                      &= CA.help "Flag to set whether the post gets an updated date annotation.  Defaults to False."
                      &= CA.typ "Bool"
                    }
           &= CA.verbosity
           &= CA.help "Produce MRP Model Blue Ripple Politics Posts"
           &= CA.summary "mrp-model v0.1.0.0, (C) 2019 Adam Conner-Sax"

glmErrorToPandocError :: GLM.GLMError -> PE.PandocError
glmErrorToPandocError x = PE.PandocSomeError $ show x

main :: IO ()
main = do
  args <- CA.cmdArgs postArgs
  putStrLn $ show args
  pandocWriterConfig <- K.mkPandocWriterConfig pandocTemplate
                                               templateVars
                                               brWriterOptionsF
  eitherDocs <-
    K.knitHtmls (Just "MRP_Basics.Main") K.nonDiagnostic pandocWriterConfig $ mapError glmErrorToPandocError $ do
      K.logLE K.Info "Loading data..."
      let csvParserOptions =
            F.defaultParser { F.quotingMode = F.RFC4180Quoting ' ' }
          tsvParserOptions = csvParserOptions { F.columnSeparator = "," }
          preFilterYears   = FU.filterOnMaybeField @Year (`L.elem` [2016])
      ccesMaybeRecs <- loadToMaybeRecs @CCES_MRP_Raw @(F.RecordColumns CCES)
        tsvParserOptions
        preFilterYears
        ccesTSV
      ccesFrame <-
        fmap transformCCESRow
          <$> maybeRecsToFrame fixCCESRow (const True) ccesMaybeRecs
{-          
      let
        firstFew = take 1000 $ FL.fold
          FL.list
          (fmap
            (F.rcast
              @'[CCESCaseId, StateAbbreviation, Registration, Turnout, HouseVoteParty, PartisanId3]
            )
            ccesFrame
          )
      K.logLE K.Diagnostic      
        $  "ccesFrame (first 100 rows):\n"
        <> (T.intercalate "\n" $ fmap (T.pack . show) firstFew)
-}
      let countVotedByStateGenderF = MR.concatFold $ countFold @ByStateGender @[StateAbbreviation, Gender, Turnout] @'[Turnout] ((== T_Voted) . F.rgetField @Turnout)
      let counted = FL.fold FL.list $ FL.fold countVotedByStateGenderF (fmap F.rcast ccesFrame)
      K.logLE K.Diagnostic      
        $  "counted:\n"
        <> (T.intercalate "\n" $ fmap (T.pack . show) counted)
      K.logLE K.Info "Inferring..."
      let counts = VS.fromList $ fmap (F.rgetField @Count) counted
          weights :: VS.Vector Double = VS.replicate (VS.length counts) 1.0
          (observations, fixedEffectsModelMatrix, rcM) = FL.fold
            (lmePrepFrame getFraction fixedEffects groups ccesPredictor ccesGroupLabels) counted
          regressionModelSpec = GLM.RegressionModelSpec fixedEffects fixedEffectsModelMatrix observations
      rowClassifier  <- case rcM of
        Left msg -> throw $ GLM.OtherGLMError msg
        Right x -> return x --K.knitError "GLMK.throwEither $ either (Left . GLM.OtherGLMError) Right rcM
      let effectsByGroup = M.fromList [(CCES_State, IS.fromList [GLM.Intercept])]
      fitSpecByGroup <- GLM.fitSpecByGroup fixedEffects effectsByGroup rowClassifier        
      let lmmControls = GLM.LMMControls LMM_NELDERMEAD 1e-6
          lmmSpec = GLM.LinearMixedModelSpec (GLM.MixedModelSpec regressionModelSpec fitSpecByGroup) lmmControls
          cc = GLM.PIRLSConvergenceCriterion GLM.PCT_Deviance 1e-6 20
          glmmControls = GLM.GLMMControls GLM.UseCanonical 10 cc
          glmmSpec = GLM.GeneralizedLinearMixedModelSpec lmmSpec weights (GLM.Binomial counts) glmmControls
          mixedModel = GLM.GeneralizedLinearMixedModel glmmSpec
      randomEffectsModelMatrix <- GLM.makeZ fixedEffectsModelMatrix fitSpecByGroup rowClassifier
      let randomEffectCalc = GLM.RandomEffectCalculated randomEffectsModelMatrix (GLM.makeLambda fitSpecByGroup)
          th0 = GLM.setCovarianceVector fitSpecByGroup 1 0
          mdVerbosity = MDVNone
      GLM.checkProblem mixedModel randomEffectCalc
      (th, pd, sigma2, betaU, b, cs) <- GLM.minimizeDeviance mdVerbosity ML mixedModel randomEffectCalc th0
      _ <- GLM.minimizeDeviance mdVerbosity ML mixedModel randomEffectCalc th0
      _ <- GLM.minimizeDeviance mdVerbosity ML mixedModel randomEffectCalc th0
      _ <- GLM.minimizeDeviance mdVerbosity ML mixedModel randomEffectCalc th0
      _ <- GLM.minimizeDeviance mdVerbosity ML mixedModel randomEffectCalc th0
      _ <- GLM.minimizeDeviance mdVerbosity ML mixedModel randomEffectCalc th0
      _ <- GLM.minimizeDeviance mdVerbosity ML mixedModel randomEffectCalc th0
      _ <- GLM.minimizeDeviance mdVerbosity ML mixedModel randomEffectCalc th0
      _ <- GLM.minimizeDeviance mdVerbosity ML mixedModel randomEffectCalc th0
      _ <- GLM.minimizeDeviance mdVerbosity ML mixedModel randomEffectCalc th0                                                                                                
      GLM.report mixedModel randomEffectsModelMatrix (GLM.bu_vBeta betaU) (SD.toSparseVector b)

      let fes = GLM.fixedEffectStatistics mixedModel sigma2 cs betaU
      K.logLE K.Info $ "FixedEffectStatistics: " <> (T.pack $ show fes)
      epg <- GLM.effectParametersByGroup rowClassifier effectsByGroup b
      K.logLE K.Info $ "EffectParametersByGroup: " <> (T.pack $ show epg)
      gec <- GLM.effectCovariancesByGroup effectsByGroup mixedModel sigma2 th      
      K.logLE K.Info $ "EffectCovariancesByGroup: " <> (T.pack $ show gec)
      rebl <- GLM.randomEffectsByLabel epg rowClassifier
      K.logLE K.Info
        $  "Random Effects:\n"
        <> GLM.printRandomEffectsByLabel rebl
      let f r = do
            let obs = getFraction r
            fitted <- GLM.fitted mixedModel
                      ccesPredictor
                      ccesGroupLabels
                      fes
                      epg
                      rowClassifier
                      r
            return (obs, fitted)
      fitted <- traverse f (FL.fold FL.list counted)
      K.logLE K.Info $ "Fitted:\n" <> (T.pack $ show fitted)
      
      K.logLE K.Info "Knitting docs..."
--      curDate <- (\(Time.UTCTime d _) -> d) <$> K.getCurrentTime
        
  case eitherDocs of
    Right namedDocs ->
      K.writeAllPandocResultsWithInfoAsHtml "reports/html/MRP_Basics" namedDocs
    Left err -> putStrLn $ "pandoc error: " ++ show err


-- map reduce
type Count = "Count" F.:-> Int
type Successes = "Successes" F.:-> Int

-- some keys for aggregation
type ByStateGender = '[StateAbbreviation, Gender]
type ByStateGenderRace = '[StateAbbreviation, Gender, WhiteNonHispanic]
type ByStateGenderRaceAge = '[StateAbbreviation, Gender, WhiteNonHispanic, Under45]
type ByStateGenderEducationAge = '[StateAbbreviation, Gender, CollegeGrad, Under45]
type ByStateGenderRaceEducation = '[StateAbbreviation, Gender, WhiteNonHispanic, CollegeGrad]

binomialFold :: (F.Record r -> Bool) -> FL.Fold (F.Record r) (F.Record '[Count, Successes])
binomialFold testRow =
  let successesF = FL.premap (\r -> if testRow r then 1 else 0) FL.sum
  in  (\s n -> s F.&: n F.&: V.RNil) <$> FL.length <*> successesF

countFold :: forall k r d.(Ord (F.Record k)
                          , F.RecVec (k V.++ '[Count, Successes])
                          , k F.⊆ r
                          , d F.⊆ r)
          => (F.Record d -> Bool)
          -> FL.Fold (F.Record r) [F.FrameRec (k V.++ [Count,Successes])]
countFold testData = MR.mapReduceFold MR.noUnpack (MR.assignKeysAndData @k)  (MR.foldAndAddKey $ binomialFold testData)
 
data CCESPredictor = P_Gender deriving (Show, Eq, Ord, Enum, Bounded)
type CCESEffect = GLM.WithIntercept CCESPredictor
ccesPredictor :: forall r. (F.ElemOf r Gender) => F.Record r -> CCESPredictor -> Double
ccesPredictor r P_Gender = if (F.rgetField @Gender r == Female) then 0 else 1

data CCESGroup = CCES_State deriving (Show, Eq, Ord, Enum, Bounded, A.Ix)
ccesGroupLabels ::forall r.  F.ElemOf r StateAbbreviation => F.Record r -> CCESGroup -> T.Text
ccesGroupLabels r CCES_State = F.rgetField @StateAbbreviation r

getFraction r = (realToFrac $ F.rgetField @Successes r)/(realToFrac $ F.rgetField @Count r)
fixedEffects :: GLM.FixedEffects CCESPredictor
fixedEffects = GLM.allFixedEffects True

groups = IS.fromList [CCES_State]

lmePrepFrame
  :: forall p g rs
   . (Bounded p, Enum p, Ord g, Show g)
  => (F.Record rs -> Double) -- ^ observations
  -> GLM.FixedEffects p
  -> IS.IndexedSet g
  -> (F.Record rs -> p -> Double) -- ^ predictors
  -> (F.Record rs -> g -> T.Text)  -- ^ classifiers
  -> FL.Fold
       (F.Record rs)
       ( LA.Vector Double
       , LA.Matrix Double
       , Either T.Text (GLM.RowClassifier g)
       ) -- ^ (X,y,(row-classifier, size of class))
lmePrepFrame observationF fe groupIndices getPredictorF classifierLabelF
  = let
      makeInfoVector
        :: M.Map g (M.Map T.Text Int)
        -> M.Map g T.Text
        -> Either T.Text (VB.Vector GLM.ItemInfo)
      makeInfoVector indexMaps labels =
        let
          g (grp, label) =
            GLM.ItemInfo
              <$> (maybe (Left $ "Failed on " <> (T.pack $ show (grp, label)))
                         Right
                  $ M.lookup grp indexMaps
                  >>= M.lookup label
                  )
              <*> pure label
        in  fmap VB.fromList $ traverse g $ M.toList labels
      makeRowClassifier
        :: Traversable f
        => M.Map g (M.Map T.Text Int)
        -> f (M.Map g T.Text)
        -> Either T.Text (GLM.RowClassifier g)
      makeRowClassifier indexMaps labels = do
        let sizes = fmap M.size indexMaps
        indexed <- traverse (makeInfoVector indexMaps) labels
        return $ GLM.RowClassifier groupIndices
                                   sizes
                                   (VB.fromList $ FL.fold FL.list indexed)
                                   indexMaps
      getPredictorF' _   GLM.Intercept     = 1
      getPredictorF' row (GLM.Predictor x) = getPredictorF row x
      predictorF row = LA.fromList $ case fe of
        GLM.FixedEffects indexedFixedEffects ->
          fmap (getPredictorF' row) $ IS.members indexedFixedEffects
        GLM.InterceptOnly -> [1]
      getClassifierLabels :: F.Record rs -> M.Map g T.Text
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
          (snd $ ST.execState (addAll ls) (M.empty, M.empty))
          ls
        )
    in
      fmap g $ ((,,) <$> foldObs <*> foldPred <*> foldClass)

addOne
  :: Ord g
  => (g, T.Text)
  -> ST.State (M.Map g Int, M.Map g (M.Map T.Text Int)) ()
addOne (grp, label) = do
  (nextIndexMap, groupIndexMaps) <- ST.get
  let groupIndexMap = fromMaybe M.empty $ M.lookup grp groupIndexMaps
  case M.lookup label groupIndexMap of
    Nothing -> do
      let index         = fromMaybe 0 $ M.lookup grp nextIndexMap
          nextIndexMap' = M.insert grp (index + 1) nextIndexMap
          groupIndexMaps' =
            M.insert grp (M.insert label index groupIndexMap) groupIndexMaps
      ST.put (nextIndexMap', groupIndexMaps')
      return ()
    _ -> return ()

addMany
  :: (Ord g, Traversable h)
  => h (g, T.Text)
  -> ST.State (M.Map g Int, M.Map g (M.Map T.Text Int)) ()
addMany x = traverse addOne x >> return ()

addAll
  :: Ord g
  => [M.Map g T.Text]
  -> ST.State (M.Map g Int, M.Map g (M.Map T.Text Int)) ()
addAll x = traverse (addMany . M.toList) x >> return ()


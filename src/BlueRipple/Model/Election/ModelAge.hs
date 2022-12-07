{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UnicodeSyntax #-}

module BlueRipple.Model.Election.ModelAge where

import qualified BlueRipple.Data.ACS_PUMS as PUMS
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.ElectionTypes as ET
import qualified BlueRipple.Data.Loaders as BRL
import qualified BlueRipple.Utilities.KnitUtils as BRK
import qualified BlueRipple.Data.DataFrames as BRDF
import qualified BlueRipple.Data.Keyed as BRK

import qualified Stan.ModelBuilder as S
import qualified Stan.ModelBuilder.DesignMatrix as DM
import qualified Stan.ModelBuilder.BuildingBlocks as SB
import qualified Stan.ModelConfig as SC
import qualified Stan.ModelBuilder.TypedExpressions.Statements as TE
import qualified Stan.ModelBuilder.TypedExpressions.Expressions as TE
import qualified Stan.ModelBuilder.TypedExpressions.Operations as TEO
import qualified Stan.ModelBuilder.TypedExpressions.Indexing as TEI
import qualified Stan.ModelBuilder.TypedExpressions.StanFunctions as SF
import qualified Stan.ModelBuilder.Distributions as SD
import qualified Stan.ModelBuilder.TypedExpressions.DAG as DAG
import Stan.ModelBuilder.TypedExpressions.TypedList (TypedList(..))

import qualified Control.MapReduce.Simple as MR
import qualified Frames.MapReduce as FMR
import qualified Frames.Transform as FT

import qualified Control.Foldl as FL
import qualified Data.Map as M
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V
import qualified Data.Vector.Unboxed as VU
import qualified Frames as F
import qualified Frames.Melt as F
import qualified Knit.Report as K
import qualified Numeric
import qualified Graphics.Vega.VegaLite as DAG

categoricalAgeModel :: S.StanBuilderM [ACSByStateMN] () ()
categoricalAgeModel = do
  let dmr = designMatrixRowACS logDensityDMRP
  -- data
  acsData <- S.dataSetTag @ACSByStateMN SC.ModelData "ACS"
  let numAgeCats = length [(minBound :: DT.Age5F)..]
      nDataE = S.dataSetSizeE acsData
  nAgeCatsE <- SB.addFixedInt "K" numAgeCats
  ageCountsE <- SB.addIntArrayData acsData "ageCounts" nAgeCatsE (Just 0) Nothing snd
  acsMatE <- DM.addDesignMatrix acsData dmr Nothing
  let (_, nPredictorsE) = DM.designMatrixColDimBinding dmr Nothing
  -- parameters
  -- zero vector for identifiability trick
  zvP <- DAG.addBuildParameter
         $ DAG.TransformedDataP
         $ DAG.TData
         (TE.NamedDeclSpec "zeroes" $ TE.vectorSpec nPredictorsE [])
         []
         TNil
         (const $ DAG.DeclRHS $ TE.functionE SF.rep_vector (TE.realE 0 :> nPredictorsE :> TNil))

  betaRawP <- DAG.addBuildParameter
              $ DAG.UntransformedP
              (TE.NamedDeclSpec "beta_raw" $ TE.matrixSpec nPredictorsE (nAgeCatsE `TE.minusE` TE.intE 1) [])
              []
              TNil
              (\_ _ -> pure ())

  betaP <- DAG.addBuildParameter
           $ DAG.TransformedP
           (TE.NamedDeclSpec "beta" $ TE.matrixSpec nPredictorsE nAgeCatsE [])
           []
           (DAG.build betaRawP :> DAG.build zvP :> TNil)
           (\ps -> DAG.DeclRHS $ TE.functionE SF.append_col ps)
           (DAG.given (TE.realE 0) :> DAG.given (TE.realE 2) :> TNil)
           (\normalPS x -> TE.addStmt $ TE.sample (TE.functionE SF.to_vector (x :> TNil)) SF.normalS normalPS)

  let betaE = DAG.parameterTagExpr betaP
      betaXD = TE.declareRHSNW
               (TE.NamedDeclSpec "beta_x" $ TE.matrixSpec nDataE nAgeCatsE [])
               (acsMatE `TE.timesE` betaE)
      at x n = TE.sliceE TEI.s0 n x

  S.inBlock S.SBModel $ S.addStmtsToCode $ TE.writerL' $ do
    let sizeE e = TE.functionE SF.size (e :> TNil)
    betaX <- betaXD
    TE.addStmt $ TE.for "n" (TE.SpecificNumbered (TE.intE 1) nDataE) $ \n ->
      [TE.target $ TE.densityE SF.multinomial_logit_lupmf (ageCountsE `at` n) (TE.transposeE (betaX `at` n) :> TNil)]

  gqBetaX <- S.inBlock S.SBGeneratedQuantities $ S.addFromCodeWriter betaXD
  SB.generateLogLikelihood
    acsData
    SD.multinomialLogitDist
    (pure $ \nE -> TE.transposeE (gqBetaX `at` nE) :> TNil)
    (\nE -> ageCountsE `at` nE)




designMatrixRowACS :: forall rs a.(F.ElemOf rs DT.CollegeGradC
                                  , F.ElemOf rs DT.SexC
                                  , F.ElemOf rs DT.RaceAlone4C
                                  , F.ElemOf rs DT.HispC
                                  , F.ElemOf rs DT.PopPerSqMile
                                  --                                , F.ElemOf rs BRDF.StateAbbreviation
                                  --                                , F.ElemOf rs BRDF.CongressionalDistrict
                                  )
                   => DM.DesignMatrixRowPart (F.Record rs, a)
                   -> DM.DesignMatrixRow (F.Record rs, a)
designMatrixRowACS densRP = DM.DesignMatrixRow "ACSDMRow" [densRP, sexRP, eduRP, raceRP]
  where
    sexRP = DM.boundedEnumRowPart Nothing "Sex" (F.rgetField @DT.SexC . fst)
    eduRP = DM.boundedEnumRowPart Nothing "Education" (collegeGrad . fst)
    race5Census r = DT.race5FromRaceAlone4AndHisp True (F.rgetField @DT.RaceAlone4C $ fst r) (F.rgetField @DT.HispC $ fst r)
    raceRP = DM.boundedEnumRowPart (Just DT.R5_WhiteNonHispanic) "Race" race5Census

groupBuilderState :: [Text] -> S.StanGroupBuilderM [ACSByStateMN] () ()
groupBuilderState states = do
  acsData <- S.addModelDataToGroupBuilder "ACS" (S.ToFoldable id)
  S.addGroupIndexForData stateGroup acsData $ S.makeIndexFromFoldable show (F.rgetField @DT.StateAbbreviation . fst) states

groupBuilderCD :: [Text] -> [Text] -> S.StanGroupBuilderM (F.FrameRec ACSByCD) () ()
groupBuilderCD states cds = do
  acsData <- S.addModelDataToGroupBuilder "ACS" (S.ToFoldable id)
  S.addGroupIndexForData stateGroup acsData $ S.makeIndexFromFoldable show (F.rgetField @DT.StateAbbreviation) states
  S.addGroupIndexForData cdGroup acsData $ S.makeIndexFromFoldable show districtKey cds

{-
setupACSRows :: (Typeable md, Typeable gq)
             => DM.DesignMatrixRowPart (F.Record ACSByCD)
             -> S.StanBuilderM md gq (S.RowTypeTag (F.Record ACSByCD)
                                     , TE.IntArrayE
                                     , TE.MatrixE)
setupACSRows densRP = do
  let dmRow = designMatrixRowACS densRP
  acsData <- S.dataSetTag @(F.Record ACSByCD) SC.ModelData "ACS"
  acsCit <- SB.addCountData acsData "ACS_CVAP" (F.rgetField @PUMS.Citizens)
  dmACS <- DM.addDesignMatrix acsData dmRow (Just "DM")
  return (acsData, acsCit, dmACS)
-}

cdGroup :: S.GroupTypeTag Text
cdGroup = S.GroupTypeTag "CD"

stateGroup :: S.GroupTypeTag Text
stateGroup = S.GroupTypeTag "State"

type Predictors = [DT.SexC, DT.CollegeGradC, DT.RaceAlone4C, DT.HispC]
type Categoricals = DT.Age5FC ': Predictors
type ACSByCD = PUMS.CDCounts Categoricals
type ACSByState = PUMS.StateCounts Categoricals

acsByCD ∷ (K.KnitEffects r, BRK.CacheEffects r)
        ⇒ F.FrameRec PUMS.PUMS
        → F.FrameRec BRL.DatedCDFromPUMA2012
        → K.Sem r (F.FrameRec ACSByCD)
acsByCD acsByPUMA cdFromPUMA = fmap F.rcast <$> PUMS.pumsCDRollup (earliest earliestYear) (acsReKey . F.rcast) cdFromPUMA acsByPUMA
 where
  earliestYear = 2016
  earliest year = (>= year) . F.rgetField @BRDF.Year

cachedACSByCD
  ∷ ∀ r
   . (K.KnitEffects r, BRK.CacheEffects r)
  ⇒ K.ActionWithCacheTime r (F.FrameRec PUMS.PUMS)
  → K.ActionWithCacheTime r (F.FrameRec BRL.DatedCDFromPUMA2012)
  → K.Sem r (K.ActionWithCacheTime r (F.FrameRec ACSByCD))
cachedACSByCD acs_C cdFromPUMA_C = do
  let acsByCDDeps = (,) <$> acs_C <*> cdFromPUMA_C
  BRK.retrieveOrMakeFrame "model/age/acsByCD.bin" acsByCDDeps $
    \(acsByPUMA, cdFromPUMA) → acsByCD acsByPUMA cdFromPUMA

acsByState ∷ F.FrameRec PUMS.PUMS → F.FrameRec ACSByState
acsByState acsByPUMA = F.rcast <$> FL.fold (PUMS.pumsStateRollupF (acsReKey . F.rcast)) filteredACSByPUMA
 where
  earliestYear = 2016
  earliest year = (>= year) . F.rgetField @BRDF.Year
  filteredACSByPUMA = F.filterFrame (earliest earliestYear) acsByPUMA

cachedACSByState
  ∷ ∀ r
   . (K.KnitEffects r, BRK.CacheEffects r)
  ⇒ K.ActionWithCacheTime r (F.FrameRec PUMS.PUMS)
  → K.Sem r (K.ActionWithCacheTime r (F.FrameRec ACSByState))
cachedACSByState acs_C = do
  BRK.retrieveOrMakeFrame "model/age/acsByState.bin" acs_C $
    pure . acsByState

acsReKey
  ∷ F.Record '[DT.Age5FC, DT.SexC, DT.CollegeGradC, DT.InCollege, DT.RaceAlone4C, DT.HispC]
  → F.Record Categoricals
acsReKey r =
  F.rgetField @DT.Age5FC r
  F.&: F.rgetField @DT.SexC r
  F.&: (if collegeGrad r || inCollege r then DT.Grad else DT.NonGrad)
  F.&: F.rgetField @DT.RaceAlone4C r
  F.&: F.rgetField @DT.HispC r
  F.&: V.RNil


forMultinomial :: forall ks as bs rs l. (ks F.⊆ rs, as F.⊆ rs, Ord (F.Record ks), Enum l, Bounded l, Ord l)
               => (F.Record rs -> l) -- label
               -> (F.Record rs -> Int) -- count
               -> FL.Fold (F.Record as) (F.Record bs)
               -> FL.Fold (F.Record rs) [(F.Record (ks V.++ bs), VU.Vector Int)]
forMultinomial label count extraF =
  let vecF :: FL.Fold (l, Int) (VU.Vector Int)
      vecF = let zeroMap = M.fromList $ zip [(minBound :: l)..] $ repeat 0
             in VU.fromList . fmap snd . M.toList . M.unionWith (+) zeroMap <$> FL.foldByKeyMap FL.sum
--      lastMF :: FL.FoldM Maybe a a
--      lastMF = FL.FoldM (\_ a -> Just a) Nothing Just
      datF :: FL.Fold (F.Record as, (l, Int)) (F.Record bs, VU.Vector Int)
      datF = (,) <$> FL.premap fst extraF <*> FL.premap snd vecF
  in MR.concatFold
     $ MR.mapReduceFold
     MR.noUnpack
     (MR.assign (F.rcast @ks) (\r -> (F.rcast @as r, (label r, count r))))
     (MR.foldAndLabel datF (\ks (bs, v) -> [(ks F.<+> bs, v)]))


type ACSByStateMN = (F.Record ([BRDF.Year, BRDF.StateAbbreviation, BRDF.StateFIPS] V.++ Predictors V.++ '[DT.PopPerSqMile]), VU.Vector Int)

densityF :: FL.Fold (F.Record [PUMS.Citizens, PUMS.NonCitizens, DT.PopPerSqMile]) (F.Record '[DT.PopPerSqMile])
densityF =
  let nPeople r = F.rgetField @PUMS.Citizens r + F.rgetField @PUMS.NonCitizens r
      density r = F.rgetField @DT.PopPerSqMile r
      f r = (realToFrac $ nPeople r, density r)
  in FT.recordSingleton @DT.PopPerSqMile <$> FL.premap f PUMS.densityF

geomDensityF :: FL.Fold (Double, Double) Double
geomDensityF =
  let wgtF = FL.premap fst FL.sum
      wgtSumF = Numeric.exp <$> FL.premap (\(w, d) -> w * safeLog d) FL.sum
  in (/) <$> wgtSumF <*> wgtF
{-# INLINE geomDensityF #-}

acsByStateMN :: F.FrameRec ACSByState -> [ACSByStateMN]
acsByStateMN = FL.fold (forMultinomial @([BRDF.Year, BRDF.StateAbbreviation, BRDF.StateFIPS] V.++ Predictors)
                        (F.rgetField @DT.Age5FC)
                        (F.rgetField @PUMS.Citizens)
                        densityF
                       )

collegeGrad :: F.ElemOf rs DT.CollegeGradC => F.Record rs -> Bool
collegeGrad r = F.rgetField @DT.CollegeGradC r == DT.Grad

inCollege :: F.ElemOf rs DT.InCollege => F.Record rs -> Bool
inCollege = F.rgetField @DT.InCollege

districtKey :: (F.ElemOf rs BRDF.StateAbbreviation, F.ElemOf rs BRDF.CongressionalDistrict) => F.Record rs -> Text
districtKey r = F.rgetField @BRDF.StateAbbreviation r <> "-" <> show (F.rgetField @BRDF.CongressionalDistrict r)

logDensityPredictor :: F.ElemOf rs DT.PopPerSqMile => F.Record rs -> VU.Vector Double
logDensityPredictor = safeLogV . F.rgetField @DT.PopPerSqMile

safeLog x =  if x < 1e-12 then 0 else Numeric.log x -- won't matter because Pop will be 0 here
safeLogV x =  VU.singleton $ safeLog x

logDensityDMRP :: F.ElemOf rs DT.PopPerSqMile => DM.DesignMatrixRowPart (F.Record rs, a)
logDensityDMRP = DM.DesignMatrixRowPart "Density" 1 (logDensityPredictor . fst)

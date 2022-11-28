{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE UnicodeSyntax #-}

module BlueRipple.Model.Election.ModelAge where

import qualified BlueRipple.Data.ACS_PUMS as PUMS
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.ElectionTypes as ET
import qualified BlueRipple.Data.Loaders as BRL

import qualified Stan.ModelBuilder as S
import qualified Stan.ModelBuilder.DesignMatrix as DM
import qualified Stan.ModelBuilder.BuildingBlocks as SB
import qualified Stan.ModelConfig as SC
--import qualified Stan.ModelBuilder.TypedExpressions.Types as TE
import qualified Stan.ModelBuilder.TypedExpressions.Expressions as TE

import qualified Frames as F
import qualified Frames.Melt as F

designMatrixRowACS :: forall rs.(F.ElemOf rs DT.CollegeGradC
                                , F.ElemOf rs DT.InCollege
                                , F.ElemOf rs DT.SexC
                                , F.ElemOf rs DT.RaceAlone4C
                                , F.ElemOf rs DT.HispC
                                , F.ElemOf rs DT.PopPerSqMile
                                )
                   => DM.DesignMatrixRowPart (F.Record rs)
                   -> DM.DesignMatrixRow (F.Record rs)
designMatrixRowACS densRP = DM.DesignMatrixRow (dmName dmType) [densRP, sexRP, eduRP, raceRP]
  where
    sexRP = DM.boundedEnumRowPart Nothing "Sex" (F.rgetField @DT.SexC)
    eduRP = DM.boundedEnumRowPart Nothing "Education" (\r -> F.rgetField @DT.CollegeGradC r || F.rgetField @DT.InCollege r)
    race5Census r = DT.race5FromRaceAlone4AndHisp True (F.rgetField @DT.RaceAlone4C r) (F.rgetField @DT.HispC r)
    raceRP = DM.boundedEnumRowPart (Just DT.R5_WhiteNonHispanic) "Race" race5Census

groupBuilder :: [Text] -> [Text] -> S.StanGroupBuilderM (F.FrameRec PUMS.PUMS) () ()
groupBuilder psGroup states cds = do
  acsData <- S.addModelDataToGroupBuilder "ACS" (S.ToFoldable id)
  S.addGroupIndexForData stateGroup acsData $ S.makeIndexFromFoldable show (F.rgetField @DT.StateAbbreviation) states
  S.addGroupIndexForData cdGroup acsData $ S.makeIndexFromFoldable show districtKey cds

setupACSRows :: (Typeable md, Typeable gq)
             => DM.DesignMatrixRowPart (F.Record ACSByCD)
             -> (ET.OfficeT -> F.Record ACSByCD -> Double)
             -> S.StanBuilderM md gq (S.RowTypeTag (F.Record ACSByCD)
                                     , TE.IntArrayE
                                     , TE.MatrixE)
setupACSRows densRP = do
  let dmRow = designMatrixRowACS densRP
  acsData <- S.dataSetTag @(F.Record ACSByCD) SC.ModelData "ACS"
  acsCit <- SB.addCountData acsData "ACS_CVAP" (F.rgetField @PUMS.Citizens)
  dmACS <- DM.addDesignMatrix acsData dmRow (Just "DM")
  return (acsData, acsCit, dmACS)

cdGroup :: SB.GroupTypeTag Text
cdGroup = SB.GroupTypeTag "CD"

stateGroup :: SB.GroupTypeTag Text
stateGroup = SB.GroupTypeTag "State"

type Categoricals = [DT.Age5FC, DT.SexC, DT.CollegeGradC, DT.RaceAlone4C, DT.HispC]
type ACSByCD = PUMS.CDCounts Categoricals
type ACSByState = PUMS.StateCounts Categoricals

acsByCD ∷ (K.KnitEffects r, BR.CacheEffects r)
        ⇒ F.FrameRec PUMS.PUMS
        → F.FrameRec BR.DatedCDFromPUMA2012
        → K.Sem r (F.FrameRec ACSByCD)
acsByCD acsByPUMA cdFromPUMA = fmap F.rcast <$> PUMS.pumsCDRollup (earliest earliestYear) (acsReKey . F.rcast) cdFromPUMA acsByPUMA
 where
  earliestYear = 2016
  earliest year = (>= year) . F.rgetField @BR.Year

cachedACSByCD
  ∷ ∀ r
   . (K.KnitEffects r, BR.CacheEffects r)
  ⇒ K.ActionWithCacheTime r (F.FrameRec PUMS.PUMS)
  → K.ActionWithCacheTime r (F.FrameRec BR.DatedCDFromPUMA2012)
  → K.Sem r (K.ActionWithCacheTime r (F.FrameRec ACSByCD))
cachedACSByCD acs_C cdFromPUMA_C = do
  let acsByCDDeps = (,) <$> pums_C <*> cdFromPUMA_C
  BR.retrieveOrMakeFrame "model/age/acsByCD.bin" acsByCDDeps $
    \(acsByPUMA, cdFromPUMA) → acsByCD acsByPUMA cdFromPUMA

acsByState ∷ F.FrameRec PUMS.PUMS → F.FrameRec PUMSByStateR
acsByState acsByPUMA = F.rcast <$> FL.fold (PUMS.pumsStateRollupF (acsReKey . F.rcast)) filteredACSByPUMA
 where
  earliestYear = 2016
  earliest year = (>= year) . F.rgetField @BR.Year
  filteredACSByPUMA = F.filterFrame (earliest earliestYear) acsByPUMA

cachedACSByState
  ∷ ∀ r
   . (K.KnitEffects r, BR.CacheEffects r)
  ⇒ K.ActionWithCacheTime r (F.FrameRec PUMS.PUMS)
  → K.Sem r (K.ActionWithCacheTime r (F.FrameRec (StateKeyR V.++ CensusPredictorR V.++ '[PUMS.Citizens])))
cachedACSByState pums_C = do
  let zeroCount ∷ F.Record '[PUMS.Citizens]
      zeroCount = 0 F.&: V.RNil
      addZeroF =
        FMR.concatFold $
          FMR.mapReduceFold
            FMR.noUnpack
            (FMR.assignKeysAndData @StateKeyR @(Categoricals V.++ '[PUMS.Citizens]))
            ( FMR.makeRecsWithKey id $
                FMR.ReduceFold $
                  const $
                    BRK.addDefaultRec @Categoricals zeroCount
            )
  BR.retrieveOrMakeFrame "model/age/acsByState.bin" pums_C $
    pure . FL.fold addZeroF . pumsByState

acsReKey
  ∷ F.Record '[DT.Age5FC, DT.SexC, DT.CollegeGradC, DT.InCollege, DT.RaceAlone4C, DT.HispC]
  → F.Record Categoricals
acsReKey r =
  let cg = F.rgetField @DT.CollegeGradC r
      ic = F.rgetField @DT.InCollege r
  in F.rgetField @DT.Age5FC r
     F.&: F.rgetField @DT.SexC r
     F.&: (if cg == DT.Grad || ic then DT.Grad else DT.NonGrad)
     F.&: F.rgetField @DT.RaceAlone4C r
     F.&: F.rgetField @DT.HispC r
     F.&: V.RNil

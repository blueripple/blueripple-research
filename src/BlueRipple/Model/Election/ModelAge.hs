{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE UnicodeSyntax #-}

module BlueRipple.Model.Election.ModelAge where

import qualified BlueRipple.Data.ACS_PUMS as PUMS
import qualified BlueRipple.Data.DemographicTypes as DT

import qualified Stan.ModelBuilder.DesignMatrix as DM
import qualified Stan.ModelBuilder.BuildingBlocks as SB

import qualified Frames as F

designMatrixRowACS :: forall rs k.(F.ElemOf rs DT.CollegeGradC
                                  , F.ElemOf rs DT.InCollege
                                  , F.ElemOf rs DT.SexC
                                  , F.ElemOf rs DT.RaceAline4C
                                  , F.ElemOf rs DT.HispC
                                  , F.ElemOf rs DT.PopPerSqMile
                                  )
                   => Model k
                   -> DM.DesignMatrixRowPart (F.Record rs)
                   -> DM.DesignMatrixRow (F.Record rs)
designMatrixRowACS m densRP = DM.DesignMatrixRow (dmName dmType) [densRP, sexRP, eduRP, raceRP]
  where
    sexRP = DM.boundedEnumRowPart Nothing "Sex" (F.rgetField @DT.SexC)
    eduRP = DM.boundedEnumRowPart Nothing "Education" (\r -> F.rgetField @DT.CollegeGradC r || F.rgetField @DT.InCollege r)
    race5Census r = DT.race5FromRaceAlone4AndHisp True (F.rgetField @DT.RaceAlone4C r) (F.rgetField @DT.HispC r)
    raceRP = DM.boundedEnumRowPart (Just DT.R5_WhiteNonHispanic) "Race" race5Census

groupBuilder :: [Text] -> [Text] -> SB.StanGroupBuilderM (F.FrameRec PUMS.PUMS) () ()
groupBuilder psGroup states cds = do
  acsData <- SB.addModelDataToGroupBuilder "ACS" (SB.ToFoldable id)
  SB.addGroupIndexForData stateGroup acsData $ SB.makeIndexFromFoldable show (F.rgetField @BR.StateAbbreviation) states
  SB.addGroupIndexForData cdGroup acsData $ SB.makeIndexFromFoldable show districtKey cds

setupACSRows :: (Typeable md, Typeable gq)
             => Model k
             -> DM.DesignMatrixRowPart (F.Record ACSWithDensityEM)
             -> (ET.OfficeT -> F.Record ACSWithDensityEM -> Double)
             -> SB.StanBuilderM md gq (SB.RowTypeTag (F.Record ACSWithDensityEM)
                                      , TE.IntArrayE
                                      , TE.MatrixE
                                      , Map ET.OfficeT TE.MatrixE)
setupACSRows m densRP incF = do
  let include = modelComponents m
      offices = votesFrom m
      dmRowT = designMatrixRowPS m densRP DMTurnout
      dmRowP o = designMatrixRowPS' m densRP (DMPref o) (incF o)
      officeMap = M.fromList $ let x = Set.toList offices in zip x x
  acsPSData <- SB.dataSetTag @(F.Record ACSWithDensityEM) SC.ModelData "ACS"
  acsWgts <- SB.addCountData acsPSData "ACS_CVAP_WGT" (F.rgetField @PUMS.Citizens)
  dmACST <- DM.addDesignMatrix acsPSData dmRowT (Just "DMTurnout")
  dmACSPs <- traverse (\o -> DM.addDesignMatrix acsPSData (dmRowP o) (Just "DMPref")) officeMap
  return (acsPSData, acsWgts, dmACST, dmACSPs)

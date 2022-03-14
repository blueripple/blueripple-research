{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}
--{-# OPTIONS_GHC -O0 #-}

module BlueRipple.Model.Election.StanModel where

import Prelude hiding (pred)
import Relude.Extra (secondF)
import qualified BlueRipple.Configuration as BR
import qualified BlueRipple.Data.ACS_PUMS as PUMS
import qualified BlueRipple.Data.CPSVoterPUMS as CPS
import qualified BlueRipple.Data.CensusTables as Census
import qualified BlueRipple.Data.CensusLoaders as Census
import qualified BlueRipple.Data.CountFolds as BRCF
import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Data.ElectionTypes as ET
import qualified BlueRipple.Data.ModelingTypes as MT
import qualified BlueRipple.Data.Keyed as BRK
import qualified BlueRipple.Data.Loaders as BR
import qualified BlueRipple.Model.TurnoutAdjustment as BRTA
import qualified BlueRipple.Model.PostStratify as BRPS
import qualified BlueRipple.Utilities.KnitUtils as BR
import qualified BlueRipple.Utilities.FramesUtils as BRF
import qualified BlueRipple.Model.StanMRP as MRP
import qualified Numeric
import qualified CmdStan as CS
import qualified Control.Foldl as FL
import qualified Data.Aeson as A
import qualified Data.List as List
import qualified Data.Map.Strict as M
import qualified Data.Map.Merge.Strict as M
import qualified Data.IntMap as IM
import qualified Data.Set as Set
import Data.String.Here (here)
import qualified Data.Serialize                as S
import qualified Data.Text as T
import qualified Data.Vector as Vec
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V
import qualified Data.Vector as Vector
import qualified Data.Vector.Unboxed as VU
import qualified Flat
import qualified Frames as F
import qualified Frames.Conversion as FC
import qualified Frames.Melt as F
import qualified Frames.Streamly.InCore as FI
import qualified Frames.Streamly.TH as FS
import qualified Frames.Serialize as FS
import qualified Frames.SimpleJoins as FJ
import qualified Frames.Transform as FT
import GHC.Generics (Generic)
import qualified Data.MapRow as MapRow
import qualified Knit.Effect.AtomicCache as K hiding (retrieveOrMake)
import qualified Knit.Report as K
import qualified Knit.Utilities.Streamly as K
import qualified Numeric.Foldl as NFL
import qualified Optics
import qualified Stan.JSON as SJ
import qualified Stan.ModelBuilder.ModelParameters as SMP
import qualified Stan.ModelBuilder.GroupModel as SB
import qualified Stan.ModelBuilder.FixedEffects as SFE
import qualified Stan.ModelConfig as SC
import qualified Stan.ModelRunner as SM
import qualified Stan.Parameters as SP
import qualified Stan.RScriptBuilder as SR
import qualified BlueRipple.Data.CCES as CCES
import BlueRipple.Data.CCESFrame (cces2018C_CSV)
import BlueRipple.Data.ElectionTypes (CVAP)
import qualified Frames.MapReduce as FMR
import qualified Stan.ModelBuilder.DesignMatrix as DM
import qualified Stan.ModelBuilder as SB
import qualified Control.MapReduce as FMR
import qualified Frames.Folds as FF
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified Stan.ModelBuilder.BuildingBlocks as SB

import BlueRipple.Model.Election.DataPrep

groupBuilderDM :: forall rs ks tr pr.
                  (F.ElemOf rs BR.StateAbbreviation
                  , F.ElemOf rs Census.Count
                  , Typeable rs
                  , Typeable ks
                  , V.RMap ks
                  , V.ReifyConstraint Show F.ElField ks
                  , V.RecordToList ks
                  , FI.RecVec rs
                  , ks F.⊆ rs
                  , Ord (F.Record ks)
                  )
               => Model
               -> SB.GroupTypeTag (F.Record ks)
               -> [Text]
               -> [F.Record ks]
               -> SB.StanGroupBuilderM CCESAndCPSEM (F.FrameRec rs) ()
groupBuilderDM model psGroup states psKeys = do
  let loadCPSTurnoutData :: SB.StanGroupBuilderM CCESAndCPSEM (F.FrameRec rs) () = do
        cpsData <- SB.addModelDataToGroupBuilder "CPS" (SB.ToFoldable $ F.filterFrame ((/=0) . F.rgetField @BRCF.Count) . cpsVEMRows)
        SB.addGroupIndexForData stateGroup cpsData $ SB.makeIndexFromFoldable show (F.rgetField @BR.StateAbbreviation) states
      officeFilterF offices r = Set.member (F.rgetField @ET.Office r) offices
      elexRows :: CCESAndCPSEM -> F.FrameRec (ElectionResultWithDemographicsR '[BR.Year, BR.StateAbbreviation])
      elexRows x = F.filterFrame ((>0) . F.rgetField @TVotes) -- filter out anything with no votes!
                   $ F.filterFrame (officeFilterF $ votesFrom model)
                   $ stateElectionRows x <> (fmap F.rcast $ cdElectionRows x)
      loadElexTurnoutData :: SB.StanGroupBuilderM CCESAndCPSEM (F.FrameRec rs) ()
      loadElexTurnoutData = do
        elexTurnoutData <- SB.addModelDataToGroupBuilder "ElectionsT" (SB.ToFoldable elexRows)
        SB.addGroupIndexForData stateGroup elexTurnoutData $ SB.makeIndexFromFoldable show (F.rgetField @BR.StateAbbreviation) states
      loadElexPrefData :: SB.StanGroupBuilderM CCESAndCPSEM (F.FrameRec rs) ()
      loadElexPrefData = do
        elexPrefData <- SB.addModelDataToGroupBuilder "ElectionsP" (SB.ToFoldable elexRows)
        SB.addGroupIndexForData stateGroup elexPrefData $ SB.makeIndexFromFoldable show (F.rgetField @BR.StateAbbreviation) states
      loadCCESTurnoutData :: SB.StanGroupBuilderM CCESAndCPSEM (F.FrameRec rs) () = do
        ccesTurnoutData <- SB.addModelDataToGroupBuilder "CCEST" (SB.ToFoldable ccesEMRows)
        SB.addGroupIndexForData stateGroup ccesTurnoutData $ SB.makeIndexFromFoldable show (F.rgetField @BR.StateAbbreviation) states
      loadCCESPrefData :: ET.OfficeT -> ET.VoteShareType -> SB.StanGroupBuilderM CCESAndCPSEM (F.FrameRec rs) ()
      loadCCESPrefData office vst = do
        ccesPrefData <- SB.addModelDataToGroupBuilder ("CCESP_" <> show office)
                        (SB.ToFoldable $ F.filterFrame (not . zeroCCESVotes office vst) . ccesEMRows)
        SB.addGroupIndexForData stateGroup ccesPrefData $ SB.makeIndexFromFoldable show (F.rgetField @BR.StateAbbreviation) states
  loadElexTurnoutData
  loadCPSTurnoutData
  loadCCESTurnoutData
  loadElexPrefData
  when (Set.member ET.President $ votesFrom model) $ loadCCESPrefData ET.President (voteShareType model)
  when (Set.member ET.House $ votesFrom model) $ loadCCESPrefData ET.House (voteShareType model)

  psData <- SB.addGQDataToGroupBuilder "PSData" (SB.ToFoldable $ F.filterFrame ((/=0) . F.rgetField @Census.Count))
  SB.addGroupIndexForData stateGroup psData $ SB.makeIndexFromFoldable show (F.rgetField @BR.StateAbbreviation) states
  SB.addGroupIndexForData psGroup psData $ SB.makeIndexFromFoldable show F.rcast psKeys

-- If only presidential eleciton is included, incumbency is not useful since it's a constant
data DMType = DMTurnout | DMPref | DMPresOnlyPref deriving (Show, Eq)

designMatrixRowPS :: forall rs.(F.ElemOf rs DT.CollegeGradC
                             , F.ElemOf rs DT.SexC
                             , F.ElemOf rs DT.Race5C
                             , F.ElemOf rs DT.HispC
                             , F.ElemOf rs DT.PopPerSqMile
                             )
                  => DM.DesignMatrixRowPart (F.Record rs)
                  -> DMType
                  -> DM.DesignMatrixRow (F.Record rs)
designMatrixRowPS densRP dmType = DM.DesignMatrixRow (show dmType)
                                  $ case dmType of
                                      DMTurnout -> [densRP, sexRP, eduRP, raceRP, wngRP]
                                      DMPref -> [densRP, incRP, sexRP, eduRP, raceRP, wngRP]
                                      DMPresOnlyPref -> [densRP, sexRP, eduRP, raceRP, wngRP]
  where
    incRP = DM.DesignMatrixRowPart "Incumbency" 1 (const $ VU.replicate 1 0) -- we set incumbency to 0 for PS
    sexRP = DM.boundedEnumRowPart "Sex" (F.rgetField @DT.SexC)
    eduRP = DM.boundedEnumRowPart "Education" (F.rgetField @DT.CollegeGradC)
    raceRP = DM.boundedEnumRowPart "Race" mergeRace5AndHispanic
    wngRP = DM.boundedEnumRowPart "WhiteNonGrad" wnhNonGradCCES

designMatrixRowCCES :: DM.DesignMatrixRowPart (F.Record CCESWithDensityEM)
                    -> DMType
                    -> (F.Record  CCESWithDensityEM -> Double)
                    -> DM.DesignMatrixRow (F.Record CCESWithDensityEM)
designMatrixRowCCES densRP dmType incF = DM.DesignMatrixRow (show dmType)
                                         $ case dmType of
                                             DMTurnout -> [densRP, sexRP, eduRP, raceRP, wngRP]
                                             DMPref -> [densRP, incRP, sexRP, eduRP, raceRP, wngRP]
                                             DMPresOnlyPref -> [densRP, sexRP, eduRP, raceRP, wngRP]
  where
    incRP = DM.rowPartFromFunctions "Incumbency" [incF]
    sexRP = DM.boundedEnumRowPart "Sex" (F.rgetField @DT.SexC)
    eduRP = DM.boundedEnumRowPart "Education" (F.rgetField @DT.CollegeGradC)
    raceRP = DM.boundedEnumRowPart "Race" mergeRace5AndHispanic
    wngRP = DM.boundedEnumRowPart "WhiteNonGrad" wnhNonGradCCES

designMatrixRowCPS :: DM.DesignMatrixRowPart (F.Record CPSVWithDensityEM) -> DM.DesignMatrixRow (F.Record CPSVWithDensityEM)
designMatrixRowCPS densRP = DM.DesignMatrixRow (show DMTurnout) $ [densRP, sexRP, eduRP, raceRP, wngRP]
 where
    sexRP = DM.boundedEnumRowPart "Sex" (F.rgetField @DT.SexC)
    eduRP = DM.boundedEnumRowPart "Education" (F.rgetField @DT.CollegeGradC)
    raceRP = DM.boundedEnumRowPart "Race" race5Census
    wngRP = DM.boundedEnumRowPart "WhiteNonGrad" wnhNonGradCensus

designMatrixRowACS :: DM.DesignMatrixRowPart (F.Record PUMSWithDensityEM)
                   -> DMType
                   -> DM.DesignMatrixRow (F.Record PUMSWithDensityEM)
designMatrixRowACS densRP dmType = DM.DesignMatrixRow (show dmType)
                                   $ case dmType of
                                       DMTurnout -> [densRP, sexRP, eduRP, raceRP, wngRP]
                                       DMPref -> [densRP, incRP, sexRP, eduRP, raceRP, wngRP]
                                       DMPresOnlyPref -> [densRP, sexRP, eduRP, raceRP, wngRP]

  where
    incRP = DM.DesignMatrixRowPart "Incumbency" 1 (const $ VU.replicate 1 0) -- we set incumbency to 0 for ACS/diagnostics
    sexRP = DM.boundedEnumRowPart "Sex" (F.rgetField @DT.SexC)
    eduRP = DM.boundedEnumRowPart "Education" (F.rgetField @DT.CollegeGradC)
    raceRP = DM.boundedEnumRowPart "Race" race5Census
    wngRP = DM.boundedEnumRowPart "WhiteNonGrad" wnhNonGradCensus

designMatrixRowElex :: DM.DesignMatrixRowPart (F.Record ElectionWithDemographicsR)
                    -> DMType
                    -> DM.DesignMatrixRow (F.Record ElectionWithDemographicsR)
designMatrixRowElex densRP dmType = DM.DesignMatrixRow (show dmType)
                                    $ case dmType of
                                        DMTurnout -> [densRP, sexRP, eduRP, raceRP, wngRP]
                                        DMPref -> [densRP, incRP, sexRP, eduRP, raceRP, wngRP]
                                        DMPresOnlyPref -> [densRP, sexRP, eduRP, raceRP, wngRP]
  where
    incRP = DM.rowPartFromFunctions "Incumbency" [realToFrac . F.rgetField @Incumbency]
    fracWhite r = F.rgetField @FracWhiteNonHispanic r + F.rgetField @FracWhiteHispanic r
    fracHispanic r =  F.rgetField @FracWhiteHispanic r + F.rgetField @FracNonWhiteHispanic r
    fracWhiteNonGrad r = fracWhite r - F.rgetField @FracWhiteGrad r
    sexRP = DM.rowPartFromFunctions "Sex" [\r ->  1 - (2 * F.rgetField @FracFemale r)] -- Female is -1 since 1st in enum
    eduRP = DM.rowPartFromFunctions "Education" [\r ->  (2 * F.rgetField @FracGrad r) - 1] -- Grad is +1 since 2nd in enum
    raceRP = DM.rowPartFromFunctions "Race" [F.rgetField @FracOther
                                            , F.rgetField @FracBlack
                                            , fracHispanic
                                            , F.rgetField @FracAsian
                                            , F.rgetField @FracWhiteNonHispanic
                                            ]
    wngRP = DM.rowPartFromFunctions "WhiteNonGrad" [\r ->  (2 * fracWhiteNonGrad r) - 1] -- white non-grad is 1 since True in Bool, thus 2nd in enum


race5Census r = DT.race5FromRaceAlone4AndHisp True (F.rgetField @DT.RaceAlone4C r) (F.rgetField @DT.HispC r)
wnhCensus r = F.rgetField @DT.RaceAlone4C r == DT.RA4_White && F.rgetField @DT.HispC r == DT.NonHispanic
wnhNonGradCensus r = wnhCensus r && F.rgetField @DT.CollegeGradC r == DT.NonGrad

predictorFunctions :: SB.RowTypeTag r
                   -> DM.DesignMatrixRow r
                   -> Maybe Text
                   -> SB.IndexKey
                   -> SB.StanVar
                   -> SB.StanBuilderM md gq (SB.StanVar -> SB.StanVar -> SB.StanVar -> SB.StanExpr
                                            , SB.StanVar -> SB.StanVar -> SB.StanVar -> SB.StanExpr
                                            )
predictorFunctions rtt dmr suffixM dmColIndex dsIndexV = do
  let dmBetaE dmE betaE = SB.vectorizedOne dmColIndex $ SB.function "dot_product" (dmE :| [betaE])
      predE aE dmE betaE = aE `SB.plus` dmBetaE dmE betaE
      pred a dm beta = predE (SB.var a) (SB.var dm) (SB.var beta)
      suffix = fromMaybe "" suffixM
      iPredF :: SB.StanBuilderM md gq (SB.StanExpr -> SB.StanExpr -> SB.StanExpr -> SB.StanExpr)
      iPredF = SB.useDataSetForBindings rtt $ do
        dsAlpha <- SMP.addParameter ("dsAplha" <> suffix) SB.StanReal "" (SB.UnVectorized SB.stdNormal)
        dsPhi <- SMP.addParameter ("dsPhi" <> suffix) (SB.StanVector $ SB.NamedDim dmColIndex) "" (SB.Vectorized (one dmColIndex) SB.stdNormal)
        SB.inBlock SB.SBGeneratedQuantities $ SB.useDataSetForBindings rtt $ DM.splitToGroupVars dmr dsPhi
        return $ \aE dmE betaE -> predE (aE `SB.plus` SB.paren (SB.var dsAlpha `SB.times` SB.var dsIndexV))
                                         dmE
                                         (betaE `SB.plus` SB.paren (SB.var dsPhi `SB.times` SB.var dsIndexV))
  iPredE <- iPredF
  let iPred a dm beta = iPredE (SB.var a) (SB.var dm) (SB.var beta)
  return (pred, iPred)

setupCCESTData :: (Typeable md, Typeable gq)
               => DM.DesignMatrixRowPart (F.Record CCESWithDensityEM)
               -> SB.StanBuilderM md gq (SB.RowTypeTag (F.Record CCESWithDensityEM)
                                        , DM.DesignMatrixRow (F.Record CCESWithDensityEM)
                                        , SB.StanVar, SB.StanVar, SB.StanVar)
setupCCESTData densRP = do
  ccesTData <- SB.dataSetTag @(F.Record CCESWithDensityEM) SC.ModelData "CCEST"
  dmCCES <- DM.addDesignMatrix ccesTData (designMatrixRowCCES densRP DMTurnout (const 0))
  cvapCCES <- SB.addCountData ccesTData "CVAP_CCES" (F.rgetField @Surveyed)
  votedCCES <- SB.addCountData ccesTData "Voted_CCES" (round . F.rgetField @AHVoted)
  return (ccesTData, designMatrixRowCCES densRP DMTurnout (const 0), cvapCCES, votedCCES, dmCCES)


setupCPSData ::  (Typeable md, Typeable gq)
             => DM.DesignMatrixRowPart (F.Record CPSVWithDensityEM)
             -> SB.StanBuilderM md gq (SB.RowTypeTag (F.Record CPSVWithDensityEM)
                                      , DM.DesignMatrixRow (F.Record CPSVWithDensityEM)
                                      , SB.StanVar, SB.StanVar, SB.StanVar)
setupCPSData densRP = do
  cpsData <- SB.dataSetTag @(F.Record CPSVWithDensityEM) SC.ModelData "CPS"
  cvapCPS <- SB.addCountData cpsData "CVAP_CPS" (F.rgetField @BRCF.Count)
  votedCPS <- SB.addCountData cpsData "Voted_CPS"  (round . F.rgetField @AHSuccesses)
  dmCPS <- DM.addDesignMatrix cpsData (designMatrixRowCPS densRP)
  return (cpsData, designMatrixRowCPS densRP, cvapCPS, votedCPS, dmCPS)


setupElexTData :: (Typeable md, Typeable gq)
               => DM.DesignMatrixRowPart (F.Record ElectionWithDemographicsR)
               -> SB.StanBuilderM md gq (SB.RowTypeTag (F.Record ElectionWithDemographicsR)
                                        , DM.DesignMatrixRow (F.Record ElectionWithDemographicsR)
                                        , SB.StanVar, SB.StanVar, SB.StanVar)
setupElexTData densRP = do
  elexTData <- SB.dataSetTag @(F.Record ElectionWithDemographicsR) SC.ModelData "ElectionsT"
  cvapElex <- SB.addCountData elexTData "CVAP_Elections" (F.rgetField @PUMS.Citizens)
  votedElex <- SB.addCountData elexTData "Voted_Elections" (F.rgetField @TVotes)
  dmElexT <- DM.addDesignMatrix elexTData (designMatrixRowElex densRP DMTurnout)
  return (elexTData, designMatrixRowElex densRP DMTurnout, cvapElex, votedElex, dmElexT)


setupCCESPData :: (Typeable md, Typeable gq)
               => DM.DesignMatrixRowPart (F.Record CCESWithDensityEM)
               -> DMType
               -> (ET.OfficeT -> F.Record CCESWithDensityEM -> Double)
               -> ET.OfficeT
               -> ET.VoteShareType
               -> SB.StanBuilderM md gq (SB.RowTypeTag (F.Record CCESWithDensityEM)
                                        , DM.DesignMatrixRow (F.Record CCESWithDensityEM)
                                        , SB.StanVar, SB.StanVar, SB.StanVar)
setupCCESPData densRP dmPrefType incF office vst = do
  let (votesF, dVotesF) = getCCESVotes office vst
  ccesPData <- SB.dataSetTag @(F.Record CCESWithDensityEM) SC.ModelData ("CCESP_" <> show office)
--  ccesPIndex <- SB.indexedConstIntArray ccesPData (Just "P") n
  dmCCESP <- DM.addDesignMatrix ccesPData (designMatrixRowCCES densRP dmPrefType (incF office))
  raceVotesCCES <- SB.addCountData ccesPData ("VotesInRace_CCES_" <> show office) (round . votesF)
  dVotesInRaceCCES <- SB.addCountData ccesPData ("DVotesInRace_CCES_" <> show office) (round . dVotesF)
  return (ccesPData, designMatrixRowCCES densRP DMPref (incF office), raceVotesCCES, dVotesInRaceCCES, dmCCESP)

setupElexPData :: (Typeable md, Typeable gq)
               => DM.DesignMatrixRowPart (F.Record ElectionWithDemographicsR)
               -> DMType
               -> ET.VoteShareType
               -> SB.StanBuilderM md gq (SB.RowTypeTag (F.Record ElectionWithDemographicsR)
                                        , DM.DesignMatrixRow (F.Record ElectionWithDemographicsR)
                                        , SB.StanVar, SB.StanVar, SB.StanVar)
setupElexPData densRP dmPrefType vst = do
  elexPData <- SB.dataSetTag @(F.Record ElectionWithDemographicsR) SC.ModelData "ElectionsP"
--  elexPIndex <- SB.indexedConstIntArray elexPData Nothing dsIndex
  dmElexP <- DM.addDesignMatrix elexPData (designMatrixRowElex densRP dmPrefType)
  raceVotesElex <- SB.addCountData elexPData "VotesInRace_Elections"
    $ case vst of
        ET.TwoPartyShare -> (\r -> F.rgetField @DVotes r + F.rgetField @RVotes r)
        ET.FullShare -> F.rgetField @TVotes
  dVotesInRaceElex <- SB.addCountData elexPData "DVotesInRace_Elections" (F.rgetField @DVotes)
  return (elexPData, designMatrixRowElex densRP DMPref, raceVotesElex, dVotesInRaceElex, dmElexP)

data DataSetAlpha = DataSetAlpha | NoDataSetAlpha deriving (Show, Eq)

addModelForDataSet :: (Typeable md, Typeable gq)
                   => Text
                   -> Bool
                   -> SB.StanBuilderM md gq (SB.RowTypeTag r, DM.DesignMatrixRow r, SB.StanVar, SB.StanVar, SB.StanVar)
                   -> DataSetAlpha
                   -> Maybe (SC.InputDataType -> SB.StanVar -> Maybe Text -> SB.StanBuilderM md gq SB.StanVar)
                   -> SB.StanVar
                   -> SB.StanVar
                   -> SB.LLSet md gq
                   -> SB.StanBuilderM md gq (SC.InputDataType -> SB.StanVar -> Maybe Text -> SB.StanBuilderM md gq SB.StanVar, SB.LLSet md gq)
addModelForDataSet dataSetLabel includePP dataSetupM dataSetAlpha centerM alpha beta llSet = do
  let addLabel x = x <> "_" <> dataSetLabel
  (rtt, designMatrixRow, counts, successes, dm) <- dataSetupM
  dmColIndex <- case dm of
    (SB.StanVar _ (SB.StanMatrix (_, SB.NamedDim ik))) -> return ik
    (SB.StanVar m _) -> SB.stanBuildError $ "addModelForData: dm is not a matrix with named row index"
  invSamples <- SMP.addParameter (addLabel "invSamples") SB.StanReal "<lower=0>" (SB.UnVectorized SB.stdNormal)
  dsIxM <-  case dataSetAlpha of
              NoDataSetAlpha -> return Nothing
              DataSetAlpha -> do
                ix <- SMP.addParameter (addLabel "ix") SB.StanReal "" (SB.UnVectorized SB.stdNormal)
                return $ Just ix
  (dmC, centerF) <- case centerM of
    Nothing -> DM.centerDataMatrix dm Nothing
    Just f -> do
      dmC' <- f SC.ModelData dm Nothing --(Just dataSetLabel)
      return (dmC', f)
  let dist = SB.betaBinomialDist True counts
      dmBetaE dmE betaE = SB.vectorizedOne dmColIndex $ SB.function "dot_product" (dmE :| [betaE])
      muE aE dmE betaE = SB.function "inv_logit" $ one $ aE `SB.plus` dmBetaE dmE betaE
      muT ixM dm = case ixM of
        Nothing -> muE (SB.var alpha) (SB.var dm) (SB.var beta)
        Just ixV -> muE (SB.var ixV `SB.plus` SB.var alpha) (SB.var dm) (SB.var beta)
      betaA ixM is dm = muT ixM dm `SB.divide` SB.var is
      betaB ixM is dm = SB.paren (SB.scalar "1.0" `SB.minus` muT ixM dm) `SB.divide` SB.var is
      vecBetaA = SB.vectorizeExpr (addLabel "betaA") (betaA dsIxM invSamples dmC) (SB.dataSetName rtt)
      vecBetaB = SB.vectorizeExpr (addLabel "betaB") (betaB dsIxM invSamples dmC) (SB.dataSetName rtt)
  SB.inBlock SB.SBModel $ do
    SB.useDataSetForBindings rtt $ do
      betaA <- vecBetaA
      betaB <- vecBetaB
      SB.sampleDistV rtt dist (SB.var betaA, SB.var betaB) successes
  let llDetails =  SB.LLDetails dist (pure (betaA dsIxM invSamples dmC, betaB dsIxM invSamples dmC)) successes
      llSet' = SB.addToLLSet rtt llDetails llSet
      pp = SB.StanVar (addLabel "PP") (SB.StanVector $ SB.NamedDim $ SB.dataSetName rtt)
  when includePP $ do
    SB.useDataSetForBindings rtt
      $ SB.generatePosteriorPrediction rtt pp dist (betaA dsIxM invSamples dmC, betaB dsIxM invSamples dmC)
    pure ()
  return (centerF, llSet')

type ModelKeyC ks = (V.ReifyConstraint Show F.ElField ks
                    , V.RecordToList ks
                    , V.RMap ks
                    , FI.RecVec (ks V.++ '[ModelDesc, ModeledTurnout, ModeledPref, ModeledShare])
                    , V.RMap (ks V.++ '[ModelDesc, ModeledTurnout, ModeledPref, ModeledShare])
                    , Show (F.Record ks)
                    , Typeable ks
                    , Ord (F.Record ks)
                    , Flat.GFlatDecode
                      (Flat.Rep
                       (F.Rec FS.SElField (ks V.++ '[ModelDesc, ModeledTurnout, ModeledPref, ModeledShare])))
                    , Flat.GFlatEncode
                      (Flat.Rep
                       (F.Rec FS.SElField (ks V.++ '[ModelDesc, ModeledTurnout, ModeledPref, ModeledShare])))
                    , Flat.GFlatSize
                      (Flat.Rep
                        (F.Rec FS.SElField (ks V.++ '[ModelDesc, ModeledTurnout, ModeledPref, ModeledShare])))
                    , Generic
                      (F.Rec FS.SElField (ks V.++ '[ModelDesc, ModeledTurnout, ModeledPref, ModeledShare]))
                    )

electionModelDM :: forall rs ks r tr pr.
                   (K.KnitEffects r
                   , BR.CacheEffects r
                   , ModelKeyC ks
                   , F.ElemOf rs BR.StateAbbreviation
                   , F.ElemOf rs DT.CollegeGradC
                   , F.ElemOf rs DT.SexC
                   , F.ElemOf rs DT.Race5C
                   , F.ElemOf rs DT.HispC
                   , F.ElemOf rs DT.PopPerSqMile
                   , F.ElemOf rs Census.Count
                   , FI.RecVec rs
                   , ks F.⊆ rs
                   , Typeable rs
                 )
                => Bool
                -> BR.CommandLine
                -> Bool
                -> Maybe SC.StanMCParameters
                -> Text
                -> Model
                -> Int
                -> (SB.GroupTypeTag (F.Record ks), Text)
                -> K.ActionWithCacheTime r CCESAndCPSEM
                -> K.ActionWithCacheTime r (F.FrameRec rs)
                -> K.Sem r (K.ActionWithCacheTime r (ModelResults ks))
electionModelDM clearCaches cmdLine includePP mStanParams modelDir model datYear (psGroup, psDataSetName) dat_C psDat_C = K.wrapPrefix "stateLegModel" $ do
  K.logLE K.Info $ "(Re-)running DM turnout/pref model if necessary."
  ccesDataRows <- K.ignoreCacheTime $ fmap ccesEMRows dat_C
  let densityMatrixRowPart :: forall x. F.ElemOf x DT.PopPerSqMile => DM.DesignMatrixRowPart (F.Record x)
      densityMatrixRowPart = densityMatrixRowPartFromData (densityTransform model) ccesDataRows
      reportZeroRows :: K.Sem r ()
      reportZeroRows = do
        let numZeroHouseRows = countCCESZeroVoteRows ET.House (voteShareType model) ccesDataRows
            numZeroPresRows = countCCESZeroVoteRows ET.President (voteShareType model) ccesDataRows
        K.logLE K.Diagnostic $ "CCES data has " <> show numZeroHouseRows <> " rows with no house votes and "
          <> show numZeroPresRows <> " rows with no votes for president."
  reportZeroRows
  stElexRows <- K.ignoreCacheTime $ fmap stateElectionRows dat_C
  cdElexRows <- K.ignoreCacheTime $ fmap cdElectionRows dat_C
  let stIncPair r = (F.rcast @[BR.Year, BR.StateAbbreviation] r, realToFrac $ F.rgetField @Incumbency r)
      cdIncPair r =  (F.rcast @[BR.Year, BR.StateAbbreviation, BR.CongressionalDistrict] r, realToFrac $ F.rgetField @Incumbency r)
      presIncMap = FL.fold (FL.premap stIncPair FL.map) $ F.filterFrame ((== ET.President) . F.rgetField @ET.Office) stElexRows
      houseIncMap = FL.fold (FL.premap cdIncPair FL.map) $ F.filterFrame ((== ET.House) . F.rgetField @ET.Office) cdElexRows
      ccesIncF o r = case o of
        ET.President -> fromMaybe 0 $ M.lookup (F.rcast r) presIncMap
        ET.House -> fromMaybe 0 $ M.lookup (F.rcast r) houseIncMap
        _ -> 0
      dmPrefType = if votesFrom model == Set.fromList [ET.President] then DMPresOnlyPref else DMPref
      officesNamePart :: Text = mconcat $ fmap (T.take 1 . show) $ Set.toList $ votesFrom model
      modelName = "LegDistricts_" <> modelLabel model <> "_HierAlpha_Inc_" <> officesNamePart
      jsonDataName = "DM_" <> dataLabel model <> "_Inc_" <> officesNamePart <> "_" <> show datYear
      psDataSetName' = psDataSetName <> "_"  <> printDensityTransform (densityTransform model) <> "_" <> officesNamePart
      dataAndCodeBuilder :: MRP.BuilderM CCESAndCPSEM (F.FrameRec rs) ()
      dataAndCodeBuilder = do
        let (dmColIndexT, dmColExprT) = DM.designMatrixColDimBinding $ designMatrixRowCCES densityMatrixRowPart DMTurnout (const 0)
            meanTurnout = 0.6
            logit x = Numeric.log (x / (1 - x))
            logitMeanTurnout = logit meanTurnout
            normal m sd = SB.normal (Just $ SB.scalar $ show m) (SB.scalar $ show sd)
            cauchy m s = SB.cauchy (Just $ SB.scalar $ show m) (SB.scalar $ show s)
        elexTData <- SB.dataSetTag @(F.Record ElectionWithDemographicsR) SC.ModelData "ElectionsT"
        alphaT <- SB.useDataSetForBindings elexTData $ do
          muAlphaT <- SMP.addParameter "muAlphaT" SB.StanReal "" (SB.UnVectorized $ normal logitMeanTurnout 1)
          sigmaAlphaT <- SMP.addParameter "sigmaAlphaT" SB.StanReal "<lower=0>"  (SB.UnVectorized $ normal 0 1)
          alphaTNonCenterF <- SMP.scalarNonCenteredF muAlphaT sigmaAlphaT
          SMP.addHierarchicalScalar "alphaT" stateGroup (SMP.NonCentered alphaTNonCenterF) $ normal 0 1
--          pure muAlphaT
        thetaT <- SB.useDataSetForBindings elexTData $ do
          SB.addDeclBinding' dmColIndexT dmColExprT
          SB.addUseBinding' dmColIndexT dmColExprT
          muThetaT <- SMP.addParameter "muThetaT" (SB.StanVector $ SB.NamedDim dmColIndexT) "" (SB.Vectorized (one dmColIndexT) (normal 0 1))
--          tauThetaT <- SMP.addParameter "tauThetaT" (SB.StanVector $ SB.NamedDim dmColIndexT) "<lower=0>" (SB.Vectorized (one dmColIndexT) (normal 0 1))
--          corrThetaT <- SMP.lkjCorrelationMatrixParameter "corrT" dmColIndexT 4
--          thetaTNonCenteredF <- SMP.vectorNonCenteredF (SB.taggedGroupName stateGroup) muThetaT tauThetaT corrThetaT
--          SMP.addHierarchicalVector "thetaT" dmColIndexT stateGroup (SMP.NonCentered thetaTNonCenteredF) (normal 0 1)
          pure muThetaT
        (centerTF, llSetT1) <- addModelForDataSet "ElexT" includePP (setupElexTData densityMatrixRowPart) NoDataSetAlpha Nothing alphaT thetaT SB.emptyLLSet
        (_, llSetT2) <- addModelForDataSet "CPST" includePP (setupCPSData densityMatrixRowPart) DataSetAlpha (Just centerTF) alphaT thetaT llSetT1
        (_, llSetT) <- addModelForDataSet "CCEST" includePP (setupCCESTData densityMatrixRowPart) DataSetAlpha Nothing alphaT thetaT llSetT2

        elexPData <- SB.dataSetTag @(F.Record ElectionWithDemographicsR) SC.ModelData "ElectionsP"
        let (dmColIndexP, dmColExprP) = DM.designMatrixColDimBinding $ designMatrixRowCCES densityMatrixRowPart dmPrefType (const 0)
        alphaP <- SB.useDataSetForBindings elexPData $ do
          muAlphaP <- SMP.addParameter "muAlphaP" SB.StanReal "" (SB.UnVectorized $ normal 0 1)
          sigmaAlphaP <- SMP.addParameter "sigmaAlphaP" SB.StanReal "<lower=0>"  (SB.UnVectorized $ normal 0 1)
          alphaPNonCenterF <- SMP.scalarNonCenteredF muAlphaP sigmaAlphaP
          SMP.addHierarchicalScalar "alphaP" stateGroup (SMP.NonCentered alphaPNonCenterF) (normal 0 1)
--          pure muAlphaP
        thetaP <- SB.useDataSetForBindings elexPData $ do
          SB.addDeclBinding' dmColIndexP dmColExprP
          SB.addUseBinding' dmColIndexP dmColExprP
          muThetaP <- SMP.addParameter "muThetaP" (SB.StanVector $ SB.NamedDim dmColIndexP) "" (SB.Vectorized (one dmColIndexP) (normal 0 1))
  --          tauThetaP <- SMP.addParameter "tauThetaP" (SB.StanVector $ SB.NamedDim dmColIndexP) "<lower=0>" (SB.Vectorized (one dmColIndexP) (normal 0 1))
--          corrThetaP <- SMP.lkjCorrelationMatrixParameter "corrP" dmColIndexP 4
--          thetaPNonCenteredF <- SMP.vectorNonCenteredF (SB.taggedGroupName stateGroup) muThetaP tauThetaP corrThetaP
--          SMP.addHierarchicalVector "thetaP" dmColIndexP stateGroup (SMP.NonCentered thetaPNonCenteredF) (normal 0 1)
          pure muThetaP
        (centerPF, llSetP1) <- addModelForDataSet "ElexP" includePP (setupElexPData densityMatrixRowPart dmPrefType (voteShareType model)) NoDataSetAlpha Nothing alphaP thetaP SB.emptyLLSet
        let ccesP (centerFM, llS) office = do
              (centerF, llS) <- addModelForDataSet
                                ("CCESP" <> show office)
                                includePP
                                (setupCCESPData densityMatrixRowPart dmPrefType ccesIncF office (voteShareType model))
                                DataSetAlpha
                                centerFM
                                alphaP
                                thetaP
                                llS
              return (Just centerF, llS)
            llFoldM = FL.FoldM ccesP (return (Nothing, llSetP1)) return
        (_, llSetP) <- FL.foldM llFoldM (votesFrom model)
        SB.generateLogLikelihood' $ SB.mergeLLSets llSetT llSetP

        -- post-stratification for crosstabs
        -- NB: invSamples does not matter for expectations
        let  dmThetaE ci dmE thetaE = SB.vectorizedOne ci $ SB.function "dot_product" (dmE :| [thetaE])
             muE ci aE dmE thetaE = SB.function "inv_logit" $ one $ aE `SB.plus` dmThetaE ci dmE thetaE
             mu ci alpha dm theta =  muE ci (SB.var alpha) (SB.var dm) (SB.var theta)
             betaA ci alpha dm theta = mu ci alpha dm theta
             betaB ci alpha dm theta =  SB.paren (SB.scalar "1.0" `SB.minus` mu ci alpha dm theta)
             betaAT dm = betaA dmColIndexT alphaT dm thetaT
             betaBT dm = betaB dmColIndexT alphaT dm thetaT
             betaAP dm = betaA dmColIndexP alphaP dm thetaP
             betaBP dm = betaB dmColIndexP alphaP dm thetaP
        psData <- SB.dataSetTag @(F.Record rs) SC.GQData "PSData"
        dmPS_T' <- DM.addDesignMatrix psData (designMatrixRowPS densityMatrixRowPart DMTurnout)
        dmPS_P' <- DM.addDesignMatrix psData (designMatrixRowPS densityMatrixRowPart dmPrefType)
        dmPS_T <- centerTF SC.GQData dmPS_T' (Just "T")
        dmPS_P <- centerPF SC.GQData dmPS_P' (Just "P")
        let psTPrecompute dm dat  = do
              betaA' <- SB.vectorizeExpr "psBetaAT" (betaAT dm) (SB.dataSetName dat)
              betaB' <- SB.vectorizeExpr "psBetaBT" (betaBT dm) (SB.dataSetName dat)
              return (betaA', betaB')
            psTExpr :: (SB.StanVar, SB.StanVar) -> SB.StanBuilderM md gq SB.StanExpr
            psTExpr (bA, bB) =  pure $ SB.betaMu (SB.var bA) (SB.var bB)
            psPPrecompute dm dat = do
              betaA' <- SB.vectorizeExpr "psBetaAP" (betaAP dm) (SB.dataSetName dat)
              betaB' <- SB.vectorizeExpr "psBetaBP" (betaBP dm) (SB.dataSetName dat)
              return (betaA', betaB')
            psPExpr :: (SB.StanVar, SB.StanVar) -> SB.StanBuilderM md gq SB.StanExpr
            psPExpr (bA, bB) =  pure $ SB.betaMu (SB.var bA) (SB.var bB)
            psDVotePreCompute dmT dmP dat = do
              betaTs <- psTPrecompute dmT dat
              betaPs <- psPPrecompute dmP dat
              return (betaTs, betaPs)
            psDVoteExpr :: ((SB.StanVar, SB.StanVar), (SB.StanVar, SB.StanVar))  -> SB.StanBuilderM md gq SB.StanExpr
            psDVoteExpr ((bAT, bBT), (bAP, bBP)) = do
              pT <- SB.stanDeclareRHS "pT" SB.StanReal "" $ SB.betaMu (SB.var bAT) (SB.var bBT)
              pD <- SB.stanDeclareRHS "pD" SB.StanReal "" $ SB.betaMu (SB.var bAP) (SB.var bBP)
              pure $ SB.var pT `SB.times` SB.var pD
            psBetaATPreCompute dm dat = SB.vectorizeExpr "psBetaAT" (betaAT dm) (SB.dataSetName dat)
            psBetaATExpr bA = pure $ SB.var bA
            turnoutPS = ((psTPrecompute dmPS_T psData, psTExpr), Nothing)
            prefPS = ((psPPrecompute dmPS_P psData, psPExpr), Nothing)
            dVotePS = ((psDVotePreCompute dmPS_T dmPS_P psData, psDVoteExpr), Just $ SB.name "pT")
            betaATPS = ((psBetaATPreCompute dmPS_T psData, psBetaATExpr), Nothing)
            postStratify :: (Typeable md, Typeable gq, Ord k)
                         => Text
                         -> ((SB.StanBuilderM md gq x, x -> SB.StanBuilderM md gq SB.StanExpr), Maybe SB.StanExpr)
                         -> SB.GroupTypeTag k -> SB.StanBuilderM md gq SB.StanVar
            postStratify name psCalcs grp =
              MRP.addPostStratification
              (fst psCalcs)
              (Just name)
              elexTData
              psData
              (realToFrac . F.rgetField @Census.Count)
              (MRP.PSShare $ snd psCalcs)
              (Just grp)
        postStratify "BetaAT" betaATPS psGroup
        postStratify "Turnout" turnoutPS psGroup
        postStratify "Pref" prefPS psGroup
        postStratify "DVote" dVotePS psGroup
        pure ()

      extractResults :: K.KnitEffects r
                     => SC.ResultAction r md gq SB.DataSetGroupIntMaps () (FS.SFrameRec (ModelResultsR ks))
      extractResults = SC.UseSummary f where
        f summary _ modelDataAndIndex_C mGQDataAndIndex_C = do
          gqIndexes_C <- K.knitMaybe "StanMRP.extractResults: gqDataAndIndex is Nothing" $ mGQDataAndIndex_C
          gqIndexesE <- K.ignoreCacheTime $ fmap snd gqIndexes_C
          let resultsMap :: (Typeable k, Show k, Ord k) => SB.RowTypeTag x -> SB.GroupTypeTag k -> Text -> K.Sem r (Map k [Double])
              resultsMap rtt gtt psPrefix = K.knitEither $ do
                gqIndexes <- gqIndexesE
                psIndexIM <- SB.getGroupIndex rtt gtt gqIndexes
                let parseAndIndexPctsWith idx g vn = do
                      v <- SP.getVector . fmap CS.percents <$> SP.parse1D vn (CS.paramStats summary)
                      indexStanResults idx $ Vector.map g v
                parseAndIndexPctsWith psIndexIM id $ psPrefix <> SB.taggedGroupName gtt
          let psRowTag = SB.RowTypeTag @(F.Record rs) SC.GQData "PSData"
              rmByGroup :: FI.RecVec (ks V.++ '[ModelDesc, ModeledTurnout, ModeledPref, ModeledShare])
                        => K.Sem r (ModelResults ks)
              rmByGroup = do
                turnoutRM <- resultsMap psRowTag psGroup "Turnout_PSData_"
                prefRM <- resultsMap psRowTag  psGroup "Pref_PSData_"
                dVoteRM <- resultsMap psRowTag  psGroup "DVote_PSData_"
                let rmTP = M.merge M.dropMissing M.dropMissing (M.zipWithMatched $ \_ x y -> (x,y)) turnoutRM prefRM
                    rmTPD = M.merge M.dropMissing M.dropMissing (M.zipWithMatched $ \_ (x, y) z -> (x,y,z)) rmTP dVoteRM
                    g (k, (t, p, d)) = do
                      tCI <- MT.listToCI t
                      pCI <- MT.listToCI p
                      dCI <- MT.listToCI d
                      let resRec :: F.Record [ModelDesc, ModeledTurnout, ModeledPref, ModeledShare] = dataLabel model F.&: tCI F.&: pCI F.&: dCI F.&: V.RNil
                      return $ (datYear F.&: k) F.<+> resRec
                K.knitEither $ F.toFrame <$> (traverse g $ M.toList rmTPD)

          res <- rmByGroup
          return $ FS.SFrame res
      dataWranglerAndCode :: K.ActionWithCacheTime r CCESAndCPSEM
                          -> K.ActionWithCacheTime r (F.FrameRec rs)
                          -> K.Sem r (SC.DataWrangler CCESAndCPSEM (F.FrameRec rs) SB.DataSetGroupIntMaps (), SB.StanCode)
      dataWranglerAndCode modelData_C gqData_C = do
        modelData <-  K.ignoreCacheTime modelData_C
        gqData <-  K.ignoreCacheTime gqData_C
        K.logLE K.Info
          $ "CCES has "
          <> show (FL.fold FL.length $ ccesEMRows modelData)
          <> " rows."
        K.logLE K.Info
          $ "CPSV "
          <> show (FL.fold FL.length $ cpsVEMRows modelData)
          <> " rows."
        let states = FL.fold (FL.premap (F.rgetField @BR.StateAbbreviation) FL.list) (ccesEMRows modelData)
            psKeys = FL.fold (FL.premap F.rcast FL.list) gqData
            groups = groupBuilderDM model psGroup states psKeys
        K.logLE K.Info $ show $ zip [1..] $ Set.toList $ FL.fold FL.set states
        MRP.buildDataWranglerAndCode @BR.SerializerC @BR.CacheData groups dataAndCodeBuilder modelData_C gqData_C
  let unwrapVoted = [SR.UnwrapNamed "Voted_Elections" "yElectionsVotes", SR.UnwrapNamed "Voted_CPS" "yCPSVotes", SR.UnwrapNamed "Voted_CCES" "yCCESVotes"]
  let unwrapDVotes = [SR.UnwrapNamed "DVotesInRace_Elections" "yElectionsDVotes"]
                     ++ if (Set.member ET.President $ votesFrom model) then [SR.UnwrapNamed "DVotesInRace_CCES_President" "yCCESDVotesPresident"] else []
                     ++ if (Set.member ET.House $ votesFrom model) then [SR.UnwrapNamed "DVotesInRace_CCES_House" "yCCESDVotesHouse"] else []

      gqNames = SC.GQNames ("By" <> SB.taggedGroupName psGroup) psDataSetName'
  (dw, stanCode) <- dataWranglerAndCode dat_C psDat_C
  fmap (fmap FS.unSFrame)
    $ MRP.runMRPModel
    clearCaches
    (SC.RunnerInputNames modelDir modelName (Just gqNames) jsonDataName)
    (fromMaybe (SC.StanMCParameters 4 4 (Just 1000) (Just 1000) (Just 0.9) (Just 15) Nothing) mStanParams)
    (BR.clStanParallel cmdLine)
    dw
    stanCode
    (MRP.Both $ unwrapVoted ++ unwrapDVotes)
    extractResults
    dat_C
    psDat_C

{-
data GroupModel = BaseG
                | DMG
                | PlusStateG
                | PlusSexEduG
                | PlusRaceEduG
                | PlusStateRaceG
                | PartiallyPooledStateG
                | PlusInteractionsG
                | PlusStateAndStateRaceG
                | PlusStateAndStateInteractionsG
                deriving (Show, Eq, Ord, Generic)

instance Flat.Flat GroupModel
type instance FI.VectorFor GroupModel = Vector.Vector
-}

data DensityTransform = RawDensity
                      | LogDensity
                      | BinDensity { numBins :: Int,  modeledRange :: Int }
                      | SigmoidDensity { sigmoidCenter :: Double, sigmoidSlope :: Double, modeledRange :: Int}
                      deriving (Eq, Ord, Generic)
instance Flat.Flat DensityTransform
type instance FI.VectorFor DensityTransform = Vector.Vector

printDensityTransform :: DensityTransform -> Text
printDensityTransform RawDensity = "RawDensity"
printDensityTransform LogDensity = "LogDensity"
printDensityTransform (BinDensity bins range) = "BinDensity_" <> show bins <> "_" <> show range
printDensityTransform (SigmoidDensity _ _ _) = "SigmoidDensity"


getCCESVotes :: (F.ElemOf rs HouseVotes
                , F.ElemOf rs AHHouseDVotes
                , F.ElemOf rs AHHouseRVotes
                , F.ElemOf rs PresVotes
                , F.ElemOf rs AHPresDVotes
                , F.ElemOf rs AHPresRVotes
                )
             => ET.OfficeT -> ET.VoteShareType -> (F.Record rs -> Double, F.Record rs -> Double)
getCCESVotes ET.House ET.FullShare = (realToFrac . F.rgetField @HouseVotes, F.rgetField @AHHouseDVotes)
getCCESVotes ET.House ET.TwoPartyShare = (\r -> F.rgetField @AHHouseDVotes r + F.rgetField @AHHouseRVotes r, F.rgetField @AHHouseDVotes)
getCCESVotes ET.President ET.FullShare = (realToFrac . F.rgetField @PresVotes, F.rgetField @AHPresDVotes)
getCCESVotes ET.President ET.TwoPartyShare = (\r ->  F.rgetField @AHPresDVotes r + F.rgetField @AHPresRVotes r, F.rgetField @AHPresDVotes)
getCCESVotes _ _ = (const 0, const 0) -- we shouldn't call this

zeroCCESVotes ::  (F.ElemOf rs HouseVotes
                  , F.ElemOf rs PresVotes
                  , F.ElemOf rs AHHouseDVotes
                  , F.ElemOf rs AHHouseRVotes
                  , F.ElemOf rs AHPresDVotes
                  , F.ElemOf rs AHPresRVotes
                  )
              => ET.OfficeT -> ET.VoteShareType -> F.Record rs -> Bool
zeroCCESVotes office vst r = votesInRace r == 0 where
  (votesInRace, _) = getCCESVotes office vst

countCCESZeroVoteRows :: (F.ElemOf rs HouseVotes
                         , F.ElemOf rs PresVotes
                         , F.ElemOf rs AHHouseDVotes
                         , F.ElemOf rs AHHouseRVotes
                         , F.ElemOf rs AHPresDVotes
                         , F.ElemOf rs AHPresRVotes
                         , Foldable f
                         )
                      => ET.OfficeT -> ET.VoteShareType -> f (F.Record rs) -> Int
countCCESZeroVoteRows office vst = FL.fold (FL.prefilter (zeroCCESVotes office vst) FL.length)

data Model = Model { voteShareType :: ET.VoteShareType
                   , votesFrom :: Set ET.OfficeT
                   , densityTransform :: DensityTransform
                   }  deriving (Generic)

--instance Flat.Flat (Model tr pr)
--type instance FI.VectorFor (Model tr pr) = Vector.Vector

modelLabel :: Model  -> Text
modelLabel m = show (voteShareType m) <> "_" <> T.intercalate "+" (show <$> Set.toList (votesFrom m)) <> "_" <> printDensityTransform (densityTransform m)

dataLabel :: Model -> Text
dataLabel = modelLabel

type SLDLocation = (Text, ET.DistrictType, Int)

sldLocationToRec :: SLDLocation -> F.Record [BR.StateAbbreviation, ET.DistrictTypeC, ET.DistrictNumber]
sldLocationToRec (sa, dt, dn) = sa F.&: dt F.&: dn F.&: V.RNil

type ModeledShare = "ModeledShare" F.:-> MT.ConfidenceInterval
type ModeledTurnout = "ModeledTurnout" F.:-> MT.ConfidenceInterval
type ModeledPref = "ModeledPref" F.:-> MT.ConfidenceInterval
type ModelDesc = "ModelDescription" F.:-> Text

type ModelResultsR ks  = '[BR.Year] V.++ ks V.++ '[ModelDesc, ModeledTurnout, ModeledPref, ModeledShare]
type ModelResults ks = F.FrameRec (ModelResultsR ks)
--type CrossTabFrame k  = F.FrameRec [BR.Year, k, ModelDesc, ModeledTurnout, ModeledPref, ModeledShare]

data ModelCrossTabs = ModelCrossTabs
  {
    bySex :: ModelResults '[DT.SexC]
  , byEducation :: ModelResults '[DT.CollegeGradC]
  , byRace :: ModelResults '[DT.Race5C]
  , byState :: ModelResults '[BR.StateAbbreviation]
  }

instance Flat.Flat ModelCrossTabs where
  size (ModelCrossTabs s e r st) n = Flat.size (FS.SFrame s, FS.SFrame e, FS.SFrame r, FS.SFrame st) n
  encode (ModelCrossTabs s e r st) = Flat.encode (FS.SFrame s, FS.SFrame e, FS.SFrame r, FS.SFrame st)
  decode = (\(s, e, r, st) -> ModelCrossTabs (FS.unSFrame s) (FS.unSFrame e) (FS.unSFrame r) (FS.unSFrame st)) <$> Flat.decode


cdGroup :: SB.GroupTypeTag Text
cdGroup = SB.GroupTypeTag "CD"

stateGroup :: SB.GroupTypeTag Text
stateGroup = SB.GroupTypeTag "State"

ageGroup :: SB.GroupTypeTag DT.SimpleAge
ageGroup = SB.GroupTypeTag "Age"

sexGroup :: SB.GroupTypeTag DT.Sex
sexGroup = SB.GroupTypeTag "Sex"

educationGroup :: SB.GroupTypeTag DT.CollegeGrad
educationGroup = SB.GroupTypeTag "Education"

raceGroup :: SB.GroupTypeTag DT.Race5
raceGroup = SB.GroupTypeTag "Race"

hispanicGroup :: SB.GroupTypeTag DT.Hisp
hispanicGroup = SB.GroupTypeTag "Hispanic"

wnhGroup :: SB.GroupTypeTag Bool
wnhGroup = SB.GroupTypeTag "WNH"

wngGroup :: SB.GroupTypeTag Bool
wngGroup = SB.GroupTypeTag "WNG"

{-
race5FromCPS :: F.Record CPSVByCDR -> DT.Race5
race5FromCPS r =
  let race4A = F.rgetField @DT.RaceAlone4C r
      hisp = F.rgetField @DT.HispC r
  in DT.race5FromRaceAlone4AndHisp True race4A hisp

race5FromCensus :: F.Record CPSVByCDR -> DT.Race5
race5FromCensus r =
  let race4A = F.rgetField @DT.RaceAlone4C r
      hisp = F.rgetField @DT.HispC r
  in DT.race5FromRaceAlone4AndHisp True race4A hisp
-}

-- many many people who identify as hispanic also identify as white. So we need to choose.
-- Better to model using both
mergeRace5AndHispanic r =
  let r5 = F.rgetField @DT.Race5C r
      h = F.rgetField @DT.HispC r
  in if (h == DT.Hispanic) then DT.R5_Hispanic else r5

--sldKey r = F.rgetField @BR.StateAbbreviation r <> "-" <> show (F.rgetField @ET.DistrictTypeC r) <> "-" <> show (F.rgetField @ET.DistrictNumber r)
sldKey :: (F.ElemOf rs BR.StateAbbreviation
          ,F.ElemOf rs ET.DistrictTypeC
          ,F.ElemOf rs ET.DistrictNumber)
       => F.Record rs -> SLDLocation
sldKey r = (F.rgetField @BR.StateAbbreviation r
           , F.rgetField @ET.DistrictTypeC r
           , F.rgetField @ET.DistrictNumber r
           )
districtKey r = F.rgetField @BR.StateAbbreviation r <> "-" <> show (F.rgetField @BR.CongressionalDistrict r)
wnh r = (F.rgetField @DT.RaceAlone4C r == DT.RA4_White) && (F.rgetField @DT.HispC r == DT.NonHispanic)
wnhNonGrad r = wnh r && (F.rgetField @DT.CollegeGradC r == DT.NonGrad)
wnhCCES r = (F.rgetField @DT.Race5C r == DT.R5_WhiteNonHispanic) && (F.rgetField @DT.HispC r == DT.NonHispanic)
wnhNonGradCCES r = wnhCCES r && (F.rgetField @DT.CollegeGradC r == DT.NonGrad)


densityMatrixRowPartFromData :: forall rs rs'.(F.ElemOf rs DT.PopPerSqMile, F.ElemOf rs' DT.PopPerSqMile)
                         => DensityTransform
                         -> F.FrameRec rs'
                         -> DM.DesignMatrixRowPart (F.Record rs)
densityMatrixRowPartFromData RawDensity _ = (DM.DesignMatrixRowPart "Density" 1 f)
  where
   f = VU.fromList . pure . F.rgetField @DT.PopPerSqMile
densityMatrixRowPartFromData LogDensity _ =
  (DM.DesignMatrixRowPart "Density" 1 logDensityPredictor)
densityMatrixRowPartFromData (BinDensity bins range) dat = DM.DesignMatrixRowPart "Density" 1 f where
  sortedData = List.sort $ FL.fold (FL.premap (F.rgetField @DT.PopPerSqMile) FL.list) dat
  quantileSize = List.length sortedData `div` bins
  quantilesExtra = List.length sortedData `rem` bins
  quantileMaxIndex k = quantilesExtra + k * quantileSize - 1 -- puts extra in 1st bucket
  quantileBreaks = fmap (\k -> sortedData List.!! quantileMaxIndex k) $ [1..bins]
  indexedBreaks = zip quantileBreaks $ fmap (\k -> (realToFrac $ k * range)/(realToFrac bins)) [1..bins] -- should this be 0 centered??
  go x [] = realToFrac range
  go x ((y, k): xs) = if x < y then k else go x xs
  quantileF x = go x indexedBreaks
  g x = VU.fromList [quantileF x]
  f = g . F.rgetField @DT.PopPerSqMile
densityMatrixRowPartFromData (SigmoidDensity c s range) _ = DM.DesignMatrixRowPart "Density" 1 f where
  d = F.rgetField @DT.PopPerSqMile
  y r = c / (d r)
  yk r = (y r) ** s
  f r = VU.singleton $ (realToFrac range / 2) * (1 - yk r) / (1 + yk r)
--  SB.MatrixRowFromData "Density" 1


--densityRowFromData = SB.MatrixRowFromData "Density" 1 densityPredictor
logDensityPredictor :: F.ElemOf rs DT.PopPerSqMile => F.Record rs -> VU.Vector Double
logDensityPredictor = safeLogV . F.rgetField @DT.PopPerSqMile
safeLogV x =  VU.singleton $ if x < 1e-12 then 0 else Numeric.log x -- won't matter because Pop will be 0 here

raceAlone4FromRace5 :: DT.Race5 -> DT.RaceAlone4
raceAlone4FromRace5 DT.R5_Other = DT.RA4_Other
raceAlone4FromRace5 DT.R5_Black = DT.RA4_Black
raceAlone4FromRace5 DT.R5_Hispanic = DT.RA4_Other
raceAlone4FromRace5 DT.R5_Asian = DT.RA4_Asian
raceAlone4FromRace5 DT.R5_WhiteNonHispanic = DT.RA4_White

indexStanResults :: (Show k, Ord k)
                 => IM.IntMap k
                 -> Vector.Vector a
                 -> Either Text (Map k a)
indexStanResults im v = do
  when (IM.size im /= Vector.length v)
    $ Left $
    "Mismatched sizes in indexStanResults. Result vector has " <> show (Vector.length v) <> " results and IntMap = " <> show im
  return $ M.fromList $ zip (IM.elems im) (Vector.toList v)

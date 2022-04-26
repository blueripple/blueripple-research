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
{-# LANGUAGE QuasiQuotes #-}
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
import Data.String.Here (here)
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
import qualified Stan.ModelBuilder.StanFunctionBuilder as SFB

import BlueRipple.Model.Election.DataPrep

groupBuilderDM :: forall rs ks k.
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
               => Model k
               -> SB.GroupTypeTag (F.Record ks)
               -> [Text]
               -> [Text]
               -> [F.Record ks]
               -> SB.StanGroupBuilderM CCESAndCPSEM (F.FrameRec rs) ()
groupBuilderDM model psGroup states cds psKeys = do
  let loadCPSTurnoutData :: SB.StanGroupBuilderM CCESAndCPSEM (F.FrameRec rs) () = do
        cpsData <- SB.addModelDataToGroupBuilder "CPS" (SB.ToFoldable $ F.filterFrame ((/=0) . F.rgetField @BRCF.Count) . cpsVEMRows)
        SB.addGroupIndexForData stateGroup cpsData $ SB.makeIndexFromFoldable show (F.rgetField @BR.StateAbbreviation) states
      officeFilterF x r = F.rgetField @ET.Office r == x
      -- filter out anything with no votes at all or where someone was running unopposed
      zeroVoteFilterF r = F.rgetField @TVotes r /= 0 && F.rgetField @DVotes r /= 0 && F.rgetField @RVotes r /= 0
      elexFilter x =  F.filterFrame (\r -> officeFilterF x r && zeroVoteFilterF r && F.rgetField @ET.Unopposed r == False)

      loadElexData :: ET.OfficeT -> SB.StanGroupBuilderM CCESAndCPSEM (F.FrameRec rs) ()
      loadElexData office = case office of
          ET.President -> do
            elexData <- SB.addModelDataToGroupBuilder "Elections_President" (SB.ToFoldable $ elexFilter ET.President . stateElectionRows)
            SB.addGroupIndexForData stateGroup elexData $ SB.makeIndexFromFoldable show (F.rgetField @BR.StateAbbreviation) states
          ET.Senate -> do
            elexData <- SB.addModelDataToGroupBuilder "Elections_Senate" (SB.ToFoldable $ elexFilter ET.Senate . stateElectionRows)
            SB.addGroupIndexForData stateGroup elexData $ SB.makeIndexFromFoldable show (F.rgetField @BR.StateAbbreviation) states
          ET.House -> do
            elexData <- SB.addModelDataToGroupBuilder "Elections_House" (SB.ToFoldable $ elexFilter ET.House. cdElectionRows)
            SB.addGroupIndexForData stateGroup elexData $ SB.makeIndexFromFoldable show (F.rgetField @BR.StateAbbreviation) states
            SB.addGroupIndexForData cdGroup elexData $ SB.makeIndexFromFoldable show districtKey cds
      loadCCESTurnoutData :: SB.StanGroupBuilderM CCESAndCPSEM (F.FrameRec rs) () = do
        ccesTurnoutData <- SB.addModelDataToGroupBuilder "CCEST" (SB.ToFoldable $ F.filterFrame ((/= 0) . F.rgetField @Surveyed) . ccesEMRows)
        SB.addGroupIndexForData stateGroup ccesTurnoutData $ SB.makeIndexFromFoldable show (F.rgetField @BR.StateAbbreviation) states
      loadCCESPrefData :: ET.OfficeT -> ET.VoteShareType -> SB.StanGroupBuilderM CCESAndCPSEM (F.FrameRec rs) ()
      loadCCESPrefData office vst = do
        ccesPrefData <- SB.addModelDataToGroupBuilder ("CCESP_" <> show office)
                        (SB.ToFoldable $ F.filterFrame (not . zeroCCESVotes office vst) . ccesEMRows)
        SB.addGroupIndexForData stateGroup ccesPrefData $ SB.makeIndexFromFoldable show (F.rgetField @BR.StateAbbreviation) states
      loadACSData :: SB.StanGroupBuilderM CCESAndCPSEM (F.FrameRec rs) () = do
        acsData <- SB.addModelDataToGroupBuilder "ACS" (SB.ToFoldable $ acsRows)
        SB.addGroupIndexForData stateGroup acsData $ SB.makeIndexFromFoldable show (F.rgetField @BR.StateAbbreviation) states
        SB.addGroupIndexForData cdGroup acsData $ SB.makeIndexFromFoldable show districtKey cds

  loadACSData
  traverse_ loadElexData $ Set.toList $ votesFrom model
  loadCPSTurnoutData
  loadCCESTurnoutData
  when (Set.member ET.President $ votesFrom model) $ loadCCESPrefData ET.President (voteShareType model)
  when (Set.member ET.House $ votesFrom model) $ loadCCESPrefData ET.House (voteShareType model)

  psData <- SB.addGQDataToGroupBuilder "PSData" (SB.ToFoldable $ F.filterFrame ((/=0) . F.rgetField @Census.Count))
  SB.addGroupIndexForData stateGroup psData $ SB.makeIndexFromFoldable show (F.rgetField @BR.StateAbbreviation) states
  SB.addGroupIndexForData psGroup psData $ SB.makeIndexFromFoldable show F.rcast psKeys

-- If only presidential eleciton is included, incumbency is not useful since it's a constant
data DMType = DMTurnout | DMPref | DMPresOnlyPref deriving (Show, Eq)

dmName :: DMType -> Text
dmName DMTurnout = "DMTurnout"
dmName DMPref = "DMPref"
dmName DMPresOnlyPref = "DMPref"

data DMComponents = DMDensity | DMInc | DMSex | DMEduc | DMRace | DMWNG deriving (Show, Eq, Ord)

printComponents :: Set DMComponents -> Text
printComponents = mconcat . fmap eachToText . Set.toList
  where eachToText DMDensity = "D"
        eachToText DMInc = "I"
        eachToText DMSex = "S"
        eachToText DMEduc = "E"
        eachToText DMRace = "R"
        eachToText DMWNG = "we"

dmSubset :: Set DMComponents -> Map DMComponents (DM.DesignMatrixRowPart a) -> [DM.DesignMatrixRowPart a]
dmSubset include = M.elems . M.filterWithKey (\k _ -> Set.member k include)

dmSubset' :: DMType -> Set DMComponents -> Map DMComponents (DM.DesignMatrixRowPart a) -> [DM.DesignMatrixRowPart a]
dmSubset' dmType include all = case dmType of
                                 DMTurnout -> dmSubset (Set.delete DMInc include) all
                                 DMPref -> dmSubset include all
                                 DMPresOnlyPref -> dmSubset (Set.delete DMInc include) all

designMatrixRowPS' :: forall rs.(F.ElemOf rs DT.CollegeGradC
                                , F.ElemOf rs DT.SexC
                                , F.ElemOf rs DT.Race5C
                                , F.ElemOf rs DT.PopPerSqMile
                                )
                   => Set DMComponents
                   -> DM.DesignMatrixRowPart (F.Record rs)
                   -> DMType
                   -> (F.Record rs -> Double)
                   -> DM.DesignMatrixRow (F.Record rs)
designMatrixRowPS' include densRP dmType incF = DM.DesignMatrixRow (dmName dmType) (dmSubset' dmType include all)
  where
    incRP = DM.rowPartFromFunctions "Incumbency" [incF]
    sexRP = DM.boundedEnumRowPart Nothing "Sex" (F.rgetField @DT.SexC)
    eduRP = DM.boundedEnumRowPart Nothing "Education" (F.rgetField @DT.CollegeGradC)
    raceRP = DM.boundedEnumRowPart (Just DT.R5_WhiteNonHispanic) "Race" (F.rgetField @DT.Race5C)
    wngRP = DM.boundedEnumRowPart Nothing "WhiteNonGrad" wnhNonGradCCES
    all = M.fromList[(DMDensity, densRP), (DMInc, incRP), (DMSex, sexRP), (DMEduc, eduRP), (DMRace, raceRP), (DMWNG, wngRP)]

designMatrixRowPS :: forall rs.(F.ElemOf rs DT.CollegeGradC
                                , F.ElemOf rs DT.SexC
                                , F.ElemOf rs DT.Race5C
                                , F.ElemOf rs DT.PopPerSqMile
                                )
                   => Set DMComponents
                   -> DM.DesignMatrixRowPart (F.Record rs)
                   -> DMType
                   -> DM.DesignMatrixRow (F.Record rs)
designMatrixRowPS include densRP dmType = designMatrixRowPS' include densRP dmType (const 0)


designMatrixRowCCES :: Set DMComponents
                    -> DM.DesignMatrixRowPart (F.Record CCESWithDensityEM)
                    -> DMType
                    -> (F.Record  CCESWithDensityEM -> Double)
                    -> DM.DesignMatrixRow (F.Record CCESWithDensityEM)
designMatrixRowCCES include densRP dmType incF = DM.DesignMatrixRow (dmName dmType) (dmSubset' dmType include all)
  where
    incRP = DM.rowPartFromFunctions "Incumbency" [incF]
    sexRP = DM.boundedEnumRowPart Nothing "Sex" (F.rgetField @DT.SexC)
    eduRP = DM.boundedEnumRowPart Nothing "Education" (F.rgetField @DT.CollegeGradC)
    raceRP = DM.boundedEnumRowPart (Just DT.R5_WhiteNonHispanic) "Race" (F.rgetField @DT.Race5C)
    wngRP = DM.boundedEnumRowPart Nothing "WhiteNonGrad" wnhNonGradCCES
    all = M.fromList[(DMDensity, densRP), (DMInc, incRP), (DMSex, sexRP), (DMEduc, eduRP), (DMRace, raceRP), (DMWNG, wngRP)]


designMatrixRowCPS :: Set DMComponents -> DM.DesignMatrixRowPart (F.Record CPSVWithDensityEM) -> DM.DesignMatrixRow (F.Record CPSVWithDensityEM)
designMatrixRowCPS include densRP = DM.DesignMatrixRow (dmName DMTurnout) $ dmSubset include all
 where
    sexRP = DM.boundedEnumRowPart Nothing "Sex" (F.rgetField @DT.SexC)
    eduRP = DM.boundedEnumRowPart Nothing "Education" (F.rgetField @DT.CollegeGradC)
    raceRP = DM.boundedEnumRowPart (Just DT.R5_WhiteNonHispanic) "Race" race5Census
    wngRP = DM.boundedEnumRowPart Nothing "WhiteNonGrad" wnhNonGradCensus
    all = M.fromList[(DMDensity, densRP), (DMSex, sexRP), (DMEduc, eduRP), (DMRace, raceRP), (DMWNG, wngRP)]

race5Census r = DT.race5FromRaceAlone4AndHisp True (F.rgetField @DT.RaceAlone4C r) (F.rgetField @DT.HispC r)
wnhCensus r = F.rgetField @DT.RaceAlone4C r == DT.RA4_White && F.rgetField @DT.HispC r == DT.NonHispanic
wnhNonGradCensus r = wnhCensus r && F.rgetField @DT.CollegeGradC r == DT.NonGrad

setupCCESTData :: (Typeable md, Typeable gq)
               => Set DMComponents
               -> DM.DesignMatrixRowPart (F.Record CCESWithDensityEM)
               -> SB.StanBuilderM md gq (SB.RowTypeTag (F.Record CCESWithDensityEM)
                                        , DM.DesignMatrixRow (F.Record CCESWithDensityEM)
                                        , SB.StanVar, SB.StanVar, SB.StanVar)
setupCCESTData include densRP = do
  ccesTData <- SB.dataSetTag @(F.Record CCESWithDensityEM) SC.ModelData "CCEST"
  dmCCES <- DM.addDesignMatrix ccesTData (designMatrixRowCCES include densRP DMTurnout (const 0))
  cvapCCES <- SB.addCountData ccesTData "CVAP_CCES" (F.rgetField @Surveyed)
  votedCCES <- SB.addCountData ccesTData "Voted_CCES" (F.rgetField @Voted) --(round . F.rgetField @AHVoted)
  return (ccesTData, designMatrixRowCCES include densRP DMTurnout (const 0), cvapCCES, votedCCES, dmCCES)


setupCPSData ::  (Typeable md, Typeable gq)
             => Set DMComponents
             -> DM.DesignMatrixRowPart (F.Record CPSVWithDensityEM)
             -> SB.StanBuilderM md gq (SB.RowTypeTag (F.Record CPSVWithDensityEM)
                                      , DM.DesignMatrixRow (F.Record CPSVWithDensityEM)
                                      , SB.StanVar, SB.StanVar, SB.StanVar)
setupCPSData include densRP = do
  cpsData <- SB.dataSetTag @(F.Record CPSVWithDensityEM) SC.ModelData "CPS"
  cvapCPS <- SB.addCountData cpsData "CVAP_CPS" (F.rgetField @BRCF.Count)
  votedCPS <- SB.addCountData cpsData "Voted_CPS"  (F.rgetField @BRCF.Successes)
  dmCPS <- DM.addDesignMatrix cpsData (designMatrixRowCPS include densRP)
  return (cpsData, designMatrixRowCPS include densRP, cvapCPS, votedCPS, dmCPS)

setupElexData :: forall rs md gq.
                 (Typeable md, Typeable gq, Typeable rs
                 , F.ElemOf rs PUMS.Citizens
                 , F.ElemOf rs TVotes
                 , F.ElemOf rs DVotes
                 , F.ElemOf rs RVotes)
              => ET.VoteShareType
              -> Int
              -> ET.OfficeT
              -> SB.StanBuilderM md gq (SB.RowTypeTag (F.Record rs)
                                        , SB.StanVar
                                        , SB.StanVar
                                        , SB.StanVar
                                        , SB.StanVar
                                        )
setupElexData vst eScale office = do
  let rescale x = x `div` eScale
  elexData <- SB.dataSetTag @(F.Record rs) SC.ModelData ("Elections_" <> show office)
  cvapElex <- SB.addCountData elexData ("CVAP_Elex_" <> officeText office) (rescale . F.rgetField @PUMS.Citizens)
  votedElex <- SB.addCountData elexData ("Voted_Elex" <> officeText office) (rescale . F.rgetField @TVotes)
  votesInRace <- SB.addCountData elexData ("VotesInRace_Elex_" <> officeText office)
                 $ case vst of
                     ET.TwoPartyShare -> (\r -> rescale $ F.rgetField @DVotes r + F.rgetField @RVotes r)
                     ET.FullShare -> rescale . F.rgetField @TVotes
  dVotesInRace <- SB.addCountData elexData ("DVotesInRace_Elex_" <> officeText office) (rescale . F.rgetField @DVotes)
  return (elexData, cvapElex, votedElex, votesInRace, dVotesInRace)

getElexData :: forall rs md gq. (Typeable md
                                , Typeable gq
                                , Typeable rs
                                , F.ElemOf rs PUMS.Citizens
                                , F.ElemOf rs TVotes
                                , F.ElemOf rs DVotes
                                , F.ElemOf rs RVotes)
            => ElectionRow rs
            -> ET.VoteShareType
            -> Int
            -> SB.StanBuilderM md gq (SB.RowTypeTag (F.Record rs), SB.StanVar, SB.StanVar, SB.StanVar, SB.StanVar)
getElexData x vst eScale = setupElexData @rs vst eScale $ officeFromElectionRow x

setupACSPSRows :: (Typeable md, Typeable gq)
               => Set DMComponents
               -> DM.DesignMatrixRowPart (F.Record ACSWithDensityEM)
               -> DMType
               -> (F.Record ACSWithDensityEM -> Double)
               -> SB.StanBuilderM md gq (SB.RowTypeTag (F.Record ACSWithDensityEM)
                                         , SB.StanVar
                                         , SB.StanVar
                                         , SB.StanVar)
setupACSPSRows include densRP dmType incF = do
  let dmRowT = designMatrixRowPS include densRP DMTurnout
      dmRowP = designMatrixRowPS' include densRP dmType incF
  acsPSData <- SB.dataSetTag @(F.Record ACSWithDensityEM) SC.ModelData "ACS"
  acsWgts <- SB.addCountData acsPSData "ACS_CVAP_WGT" (F.rgetField @PUMS.Citizens)
  dmACST <- DM.addDesignMatrix acsPSData dmRowT
  dmACSP <- DM.addDesignMatrix acsPSData dmRowP
  return (acsPSData, acsWgts, dmACST, dmACSP)

setupCCESPData :: (Typeable md, Typeable gq)
               => Set DMComponents
               -> DM.DesignMatrixRowPart (F.Record CCESWithDensityEM)
               -> DMType
               -> (ET.OfficeT -> F.Record CCESWithDensityEM -> Double)
               -> ET.OfficeT
               -> ET.VoteShareType
               -> SB.StanBuilderM md gq (SB.RowTypeTag (F.Record CCESWithDensityEM)
                                        , DM.DesignMatrixRow (F.Record CCESWithDensityEM)
                                        , SB.StanVar, SB.StanVar, SB.StanVar)
setupCCESPData include densRP dmPrefType incF office vst = do
  let (votesF, dVotesF) = getCCESVotes office vst
  ccesPData <- SB.dataSetTag @(F.Record CCESWithDensityEM) SC.ModelData ("CCESP_" <> show office)
  dmCCESP <- DM.addDesignMatrix ccesPData (designMatrixRowCCES include densRP dmPrefType (incF office))
  raceVotesCCES <- SB.addCountData ccesPData ("VotesInRace_CCES_" <> show office) votesF
  dVotesInRaceCCES <- SB.addCountData ccesPData ("DVotesInRace_CCES_" <> show office) dVotesF
  return (ccesPData, designMatrixRowCCES include densRP DMPref (incF office), raceVotesCCES, dVotesInRaceCCES, dmCCESP)

data DataSetAlpha = DataSetAlpha | NoDataSetAlpha deriving (Show, Eq)

colIndex :: SB.StanVar -> SB.StanBuilderM md gq SB.IndexKey
colIndex = SB.namedMatrixColIndex

dsAlphaBeta :: Text -> SB.IndexKey -> SB.StanExpr -> SB.StanExpr -> SB.StanBuilderM md gq (SB.StanVar, SB.StanVar)
dsAlphaBeta dsName ik alphaPrior betaPrior = do
    dsAlpha <- SMP.addParameter ("alpha_" <> dsName) SB.StanReal "" (SB.UnVectorized alphaPrior) -- $ SB.normal Nothing (SB.scalar "0.5"))
    dsBeta <- SMP.addParameter ("beta_" <> dsName) (SB.StanVector $ SB.NamedDim ik) "" (SB.Vectorized (one ik) betaPrior) -- $ SB.normal Nothing (SB.scalar "0.5"))
    return (dsAlpha, dsBeta)

dsAlphaGroup :: Text -> SB.GroupTypeTag k -> SB.StanExpr -> SB.StanExpr -> SB.StanBuilderM md gq SB.StanVar
dsAlphaGroup dsName gtt muPrior  sigmaPrior = do
    let normal x y = SB.normal (Just $ SB.scalar $ show x) (SB.scalar $ show y)
    muDSAlphaP <- SMP.addParameter ("muAlpha_" <> dsName) SB.StanReal "" (SB.UnVectorized muPrior) -- $ normal 0 0.5)
    sigmaDSAlphaP <- SMP.addParameter ("sigmaAlpha_" <> dsName) SB.StanReal "<lower=0>"  (SB.UnVectorized sigmaPrior) -- $ normal 0 0.4)
    dsAlphaNonCenterF <- SMP.scalarNonCenteredF muDSAlphaP sigmaDSAlphaP
    SMP.addHierarchicalScalar ("alpha_" <>  dsName) gtt (SMP.NonCentered dsAlphaNonCenterF) (normal 0 1)

dsSpecific :: Text -> DSSpecificWithPriors k -> SB.IndexKey -> SB.StanBuilderM md gq (Maybe SB.StanVar, Maybe SB.StanVar)
dsSpecific dsLabel dsp ik =
  case dsp of
    DSPNone -> return (Nothing, Nothing)
    DSPAlpha ap -> do
      dsAlpha <- SMP.addParameter ("alpha_" <> dsLabel) SB.StanReal "" (SB.UnVectorized ap)
      return (Just dsAlpha, Nothing)
    DSPAlphaGroup gtt muPrior sigmaPrior -> do
      dsAlpha <- dsAlphaGroup dsLabel gtt muPrior sigmaPrior
      return (Just dsAlpha, Nothing)
    DSPAlphaBeta alphaPrior betaPrior -> do
      (a, b) <- dsAlphaBeta dsLabel ik alphaPrior betaPrior
      return (Just a, Just b)

centerIf :: (Typeable md, Typeable gq)
         => SB.StanVar -> Maybe (SC.InputDataType -> SB.StanVar -> Maybe Text -> SB.StanBuilderM md gq SB.StanVar)
         -> SB.StanBuilderM md gq (SB.StanVar
                                  , SC.InputDataType -> SB.StanVar -> Maybe Text -> SB.StanBuilderM md gq SB.StanVar)
centerIf m centerM =  case centerM of
  Nothing -> DM.centerDataMatrix m Nothing
  Just f -> do
    mC <- f SC.ModelData m Nothing --(Just dataSetLabel)
    return (mC, f)

mBetaE :: SB.StanVar -> SB.StanBuilderM md gq (SB.StanExpr -> SB.StanExpr)
mBetaE dm = do
  dmColIndex <- colIndex dm
  return $ \betaE -> SB.vectorizedOne dmColIndex $ SB.function "dot_product" (SB.var dm :| [betaE])

muE :: SB.StanVar -> SB.StanBuilderM md gq (SB.StanExpr -> SB.StanExpr -> SB.StanExpr)
muE dm = do
  dmBetaE <- mBetaE dm
  return $ \aE betaE -> aE `SB.plus` dmBetaE betaE

addIf :: Maybe SB.StanVar -> SB.StanVar -> SB.StanExpr
addIf mv v = case mv of
  Nothing -> SB.var v
  Just v' -> SB.var v' `SB.plus` SB.var v


addIfM :: Maybe SB.StanVar -> SB.StanVar -> SB.StanBuilderM md gq SB.StanExpr
addIfM mv v@(SB.StanVar _ t) = case mv of
  Nothing -> return $ SB.var v
  Just v'@(SB.StanVar _ t') -> if (t == t')
    then return $ SB.var v' `SB.plus` SB.var v
    else case t of
           SB.StanVector (SB.NamedDim ik) -> if (t' == SB.StanReal) || (t' == SB.StanInt)
                                             then return $ SB.function "rep_vector" (SB.varNameE v' :| [SB.indexSize ik]) `SB.plus` SB.var v
                                             else SB.stanBuildError ("addIf: mismatched types! lhs=" <> show t' <> "; rhs=" <> show t)
           SB.StanMatrix (SB.NamedDim rk, SB.NamedDim ck) -> if (t' == SB.StanVector (SB.NamedDim rk)) || (t' == SB.StanArray [SB.NamedDim rk] SB.StanReal)
                                                          then return $ SB.function "rep_matrix" (SB.varNameE v' :| [SB.indexSize ck]) `SB.plus` SB.var v
                                                          else SB.stanBuildError ("addIf: mismatched types! lhs=" <> show t' <> "; rhs=" <> show t)
           _ -> SB.stanBuildError ("addIf: bad rhs type when lhs /= rhs. lhs=" <> show t' <> "; rhs=" <> show t)



indexedMuE :: SB.StanVar -> SB.StanBuilderM md gq (Maybe SB.StanVar -> SB.StanVar -> SB.StanVar -> SB.StanExpr)
indexedMuE dm = do
  muE' <- muE dm
  return $ \ixM alpha beta -> muE' (addIf ixM alpha) (SB.var beta)

indexedMuE2 :: SB.StanVar -> SB.StanBuilderM md gq (Maybe (SB.StanVar, SB.StanVar) -> SB.StanVar -> SB.StanVar -> SB.StanExpr)
indexedMuE2 dm = do
  muE' <- muE dm
  let f dsM alpha beta = case dsM of
        Nothing -> muE' (SB.var alpha) (SB.var beta)
        Just (aV, bV) -> muE' (SB.var aV `SB.plus` SB.var alpha) (SB.var bV `SB.plus` SB.var beta)
  return f

indexedMuE3' :: SB.StanVar -> SB.StanBuilderM md gq (Maybe SB.StanVar -> Maybe SB.StanVar -> SB.StanVar -> SB.StanVar -> SB.StanBuilderM md gq SB.StanExpr)
indexedMuE3' dm = do
  muE' <- muE dm
  let f dsAlphaM dsBetaM alpha beta = do
        let alpha' = addIf dsAlphaM alpha
        beta' <- addIfM dsBetaM beta
        return $ muE' alpha' beta'
  return f

indexedMuE3 :: SB.StanVar -> SB.StanBuilderM md gq (Maybe SB.StanVar -> Maybe SB.StanVar -> SB.StanVar -> SB.StanVar -> SB.StanExpr)
indexedMuE3 dm = do
  muE' <- muE dm
  let f dsAlphaM dsBetaM alpha beta = muE' alpha' beta'
        where
          alpha' = addIf dsAlphaM alpha
          beta' = addIf dsBetaM beta
  return f


modelVar :: SB.RowTypeTag r -> SB.StanDist a -> SB.StanVar -> SB.StanBuilderM md gq a -> SB.StanBuilderM md gq ()
modelVar rtt dist y eM = do
  let f e = SB.sampleDistV rtt dist e y
  SB.inBlock SB.SBModel
    $ SB.useDataSetForBindings rtt
    $ (eM >>= f)

updateLLSet ::  SB.RowTypeTag r -> SB.StanDist a -> SB.StanVar -> SB.StanBuilderM md gq a -> SB.LLSet md gq -> SB.LLSet md gq
updateLLSet rtt dist y eM llSet =  SB.addToLLSet rtt llDetails llSet where
  llDetails = SB.LLDetails dist eM y


addPosteriorPredictiveCheck :: Text -> SB.RowTypeTag r -> SB.StanDist a -> SB.StanBuilderM md gq a ->  SB.StanBuilderM md gq ()
addPosteriorPredictiveCheck ppVarName rtt dist eM = do
  let pp = SB.StanVar ppVarName (SB.StanVector $ SB.NamedDim $ SB.dataSetName rtt)
  eM >>= SB.useDataSetForBindings rtt
    . SB.generatePosteriorPrediction rtt pp dist
  pure ()

invLogit x = SB.function "inv_logit" (one x)

data QR = NoQR | DoQR | WithQR SB.StanVar SB.StanVar

handleQR :: SB.RowTypeTag r -> QR -> SB.StanVar -> SB.StanVar -> SB.StanBuilderM md gq (SB.StanVar, QR)
handleQR rtt qr m theta =  case qr of
  NoQR -> return (m, NoQR)
  WithQR rI beta -> do
    let qName = "Q_" <> SB.varName m
    colIndex <- SB.namedMatrixColIndex m
    let vec = SB.vectorized $ Set.fromList [SB.dataSetName rtt, colIndex]
    alreadyDeclared <- SB.isDeclared qName
    q <- case alreadyDeclared of
      True -> return $ SB.StanVar qName (SB.varType m)
      False -> do
        SB.inBlock SB.SBTransformedData
          $ SB.useDataSetForBindings rtt
          $ SB.stanDeclareRHS ("Q_" <> SB.varName m) (SB.varType m) ""
          $ vec
          $ m `SB.matMult` rI
    return (q, WithQR rI beta)
  DoQR -> do
    (q, _, rI, mBeta) <- DM.thinQR m (Just $ (theta, "ri_" <> SB.varName theta))
    beta <- case mBeta of
      Just b -> return b
      Nothing -> SB.stanBuildError "handleQR: thinQR returned nothing for beta!"
    return $ (q, WithQR rI beta)

applyQR :: QR -> (SB.StanVar -> SB.StanBuilderM md gq SB.StanExpr) -> SB.StanVar -> SB.StanBuilderM md gq SB.StanExpr
applyQR NoQR f m = f m
applyQR (WithQR rI _) f m = SB.inBlock SB.SBTransformedDataGQ $ do
  rowIndex <- SB.namedMatrixRowIndex m
  colIndex <- SB.namedMatrixColIndex rI
  let vec = SB.vectorized $ Set.fromList [rowIndex, colIndex]
  mQ <- SB.stanDeclareRHS (SB.varName m <> "_Q") (SB.varType m) "" $ vec $ m `SB.matMult` rI
  f mQ
applyQR DoQR _ _ = SB.stanBuildError "applyQR: DoQR given as QR argument."

addBLModelForDataSet :: (Typeable md, Typeable gq)
                     => Text
                     -> Bool
                     -> SB.StanBuilderM md gq (SB.RowTypeTag r, DM.DesignMatrixRow r, SB.StanVar, SB.StanVar, SB.StanVar)
                     -> DSSpecificWithPriors k
                     -> Maybe (SC.InputDataType -> SB.StanVar -> Maybe Text -> SB.StanBuilderM md gq SB.StanVar)
                     -> QR
                     -> SB.StanVar
                     -> SB.StanVar
                     -> SB.LLSet md gq
                     -> SB.StanBuilderM md gq (SC.InputDataType -> SB.StanVar -> Maybe Text -> SB.StanBuilderM md gq SB.StanVar
                                              , QR
                                              , SB.LLSet md gq
                                              , SB.StanVar -> SB.StanBuilderM md gq SB.StanExpr
                                              )
addBLModelForDataSet dataSetLabel includePP dataSetupM dsSp centerM qr alpha beta llSet = do
  let addLabel x = x <> "_" <> dataSetLabel
  (rtt, designMatrixRow, counts, successes, dm) <- dataSetupM
  colIndexKey <- colIndex dm
  (dsAlphaM, dsBetaM) <- dsSpecific dataSetLabel dsSp colIndexKey
  (dmC, centerF) <- centerIf dm centerM
  (dmQR, retQR) <- handleQR rtt qr dmC beta
  muF <- indexedMuE3 dmQR
  let muE = muF dsAlphaM dsBetaM alpha beta
      dist = SB.binomialLogitDist counts
      vecMu = SB.vectorizeExpr (addLabel "mu") muE (SB.dataSetName rtt)
  modelVar rtt dist successes (SB.var <$> vecMu)
  let llSet' = updateLLSet rtt dist successes (pure muE) llSet
  when includePP $ addPosteriorPredictiveCheck (addLabel "PP") rtt dist (pure muE)
  let prob = applyQR retQR $ fmap (\mu' -> invLogit $ mu' dsAlphaM dsBetaM alpha beta) . indexedMuE3
  return (centerF, retQR, llSet', prob)

addBBLModelForDataSet :: (Typeable md, Typeable gq)
                      => Text
                      -> Bool
                      -> SB.StanBuilderM md gq (SB.RowTypeTag r, DM.DesignMatrixRow r, SB.StanVar, SB.StanVar, SB.StanVar)
                      -> DSSpecificWithPriors k
                      -> Maybe (SC.InputDataType -> SB.StanVar -> Maybe Text -> SB.StanBuilderM md gq SB.StanVar)
                      -> QR
                      -> Bool
                      -> SB.StanVar
                      -> SB.StanVar
                      -> SB.StanVar
                      -> SB.LLSet md gq
                      -> SB.StanBuilderM md gq (SC.InputDataType -> SB.StanVar -> Maybe Text -> SB.StanBuilderM md gq SB.StanVar
                                               , QR
                                               , SB.LLSet md gq
                                               , SB.StanVar -> SB.StanBuilderM md gq SB.StanExpr
                                                )
addBBLModelForDataSet dataSetLabel includePP dataSetupM dsSp centerM qr countScaled alpha beta betaWidth llSet = do
  let addLabel x = x <> "_" <> dataSetLabel
  (rtt, designMatrixRow, counts, successes, dm) <- dataSetupM
  colIndexKey <- colIndex dm
  (dsAlphaM, dsBetaM) <- dsSpecific dataSetLabel dsSp colIndexKey
  (dmC, centerF) <- centerIf dm centerM
  (dmQR, retQR) <- handleQR rtt qr dmC beta
  muF <- indexedMuE3 dmQR
  let muE = invLogit $ muF dsAlphaM dsBetaM alpha beta
      bA = SB.var betaWidth `SB.times` SB.paren muE
      bB = SB.var betaWidth `SB.times` SB.paren (SB.scalar "1" `SB.minus` muE)
      dist = (if countScaled then SB.countScaledBetaBinomialDist else SB.betaBinomialDist) True counts
      vecA = SB.vectorizeExpr (addLabel "bAT") bA (SB.dataSetName rtt)
      vecB = SB.vectorizeExpr (addLabel "bBT") bB (SB.dataSetName rtt)
  modelVar rtt dist successes $ do
    a <- vecA
    b <- vecB
    return (SB.var a, SB.var b)
  let llSet' = updateLLSet rtt dist successes (pure (bA, bB)) llSet
  when includePP $ addPosteriorPredictiveCheck (addLabel "PP") rtt dist (pure (bA, bB))
  let prob = applyQR retQR $ fmap (\mu' -> invLogit $ mu' dsAlphaM dsBetaM alpha beta) . indexedMuE3
  return (centerF, retQR, llSet', prob)

officeText :: ET.OfficeT -> Text
officeText office = case office of
  ET.President -> "Pr"
  ET.Senate -> "Se"
  ET.House -> "Ho"

elexDSSp :: Set ET.OfficeT -> ET.OfficeT -> DSSpecificWithPriors k -> DSSpecificWithPriors k
elexDSSp offices o dsSp
  | ET.House `Set.member` offices = if o == ET.House then DSPNone else dsSp
  | ET.President `Set.member` offices = if o == ET.President then DSPNone else dsSp
  | otherwise = DSPNone

type ElectionC rs = (F.ElemOf rs PUMS.Citizens
                   , F.ElemOf rs TVotes
                   , F.ElemOf rs DVotes
                   , F.ElemOf rs RVotes)

data ElectionRow rs where
  PresidentRow :: SB.GroupTypeTag Text -> ElectionRow StateElectionR
  SenateRow :: SB.GroupTypeTag Text -> ElectionRow StateElectionR
  HouseRow :: SB.GroupTypeTag Text -> ElectionRow CDElectionR

electionRowGroup :: ElectionRow rs -> SB.GroupTypeTag Text
electionRowGroup (PresidentRow x) = x
electionRowGroup (SenateRow x) = x
electionRowGroup (HouseRow x) = x

officeFromElectionRow :: ElectionRow x -> ET.OfficeT
officeFromElectionRow (PresidentRow _) = ET.President
officeFromElectionRow (SenateRow _) = ET.Senate
officeFromElectionRow (HouseRow _) = ET.House

elexPSFunctionText :: Text
elexPSFunctionText =
  [here|
matrix elexPSFunction(array[] int gIdx, vector psWgts, vector aT, matrix bT, matrix dT, vector aP, matrix bP, matrix dP, int N_elex, array[] int elexIdx) {
  int N_ps = size(gIdx);
  int N_grp = size(aT);
  matrix[N_grp, 2] p = rep_matrix(0, N_grp, 2);
  matrix[N_grp, 2] wgts = rep_matrix(0, N_grp, 2);
  for (k in 1:N_ps) {
    real pT = inv_logit(aT[gIdx[k]] + dot_product(dT[k], bT[,gIdx[k]]));
    real pP = inv_logit(aP[gIdx[k]] + dot_product(dP[k], bP[,gIdx[k]]));
    real wPT = psWgts[k] * pT;
    p[gIdx[k], 1] += wPT;
    p[gIdx[k], 2] += wPT * pP;
    wgts[gIdx[k], 1] += psWgts[k];
    wgts[gIdx[k], 2] += wPT;
  }
  p ./= wgts;
  matrix[N_elex, 2] q;
  q[,1] = p[elexIdx,1];
  q[,2] = p[elexIdx,2];
  return q;
}
|]

elexPSFunction :: Text
               -> SB.RowTypeTag r -- ps rows
               -> SB.RowTypeTag r' -- election rows
               -> SB.GroupTypeTag k
               -> SB.StanVar -- psWgts
               -> SB.StanExpr -- alphaT
               -> SB.StanExpr -- betaT
               -> SB.StanVar -- dmT
               -> SB.StanExpr -- alphaP
               -> SB.StanExpr -- betaP
               -> SB.StanVar -- dmP
               -> SB.StanBuilderM md gq (SB.StanVar, SB.StanVar)
elexPSFunction varNameSuffix rttPS rttElex gtt psWgtsV alphaTE betaTE dmTV alphaPE betaPE dmPV = do
  SB.addFunctionsOnce "elexPSFunction" $ SB.declareStanFunction' elexPSFunctionText
  psIndexKey <- SB.named1dArrayIndex psWgtsV
  let elexIK = SB.dataSetName rttElex
  let grpIndexKey = SB.taggedGroupName gtt
  dmColIdxT <- SB.namedMatrixColIndex dmTV
  dmColIdxP <- SB.namedMatrixColIndex dmPV
  let vecPS = SB.vectorizedOne psIndexKey
      vecT = SB.vectorized $ Set.fromList [grpIndexKey, psIndexKey, dmColIdxT]
      vecP = SB.vectorized $ Set.fromList [grpIndexKey, psIndexKey, dmColIdxP]
  SB.useDataSetForBindings rttPS $ SB.addDataSetBindings rttElex $ do
    ps <- SB.stanDeclareRHS ("elexProbs_" <> varNameSuffix) (SB.StanMatrix (SB.NamedDim elexIK, SB.GivenDim 2)) ""
          $ SB.function "elexPSFunction"
          $ vecPS (SB.index grpIndexKey)
          :| [SB.function "to_vector" (one $ SB.varNameE psWgtsV)
             , vecT alphaTE, vecT betaTE, vecT (SB.var dmTV)
             , vecP alphaPE, vecP betaPE, vecP (SB.var dmPV)
             , SB.indexSize elexIK, SB.bare (SB.groupIndexVarName rttElex gtt)
             ]

    pT <- SB.stanDeclareRHS ("elexPT_" <> varNameSuffix) (SB.StanVector $ SB.NamedDim elexIK) ""
          $ SB.function "col" $ (SB.varNameE ps :| [SB.scalar "1"])
    pP <- SB.stanDeclareRHS ("elexPP_" <> varNameSuffix) (SB.StanVector $ SB.NamedDim elexIK) ""
          $ SB.function "col" $ (SB.varNameE ps :| [SB.scalar "2"])
    return (pT, pP)

addBLModelsForElex' :: forall rs r k md gq. (Typeable md, Typeable gq, Typeable rs, ElectionC rs)
                    => Bool
                    -> ET.VoteShareType
                    -> Int
                    -> ElectionRow rs
                    -> Maybe (SC.InputDataType -> SB.StanVar -> Maybe Text -> SB.StanBuilderM md gq SB.StanVar)
                    -> Maybe (SC.InputDataType -> SB.StanVar -> Maybe Text -> SB.StanBuilderM md gq SB.StanVar)
                    -> QR
                    -> QR
                    -> DSSpecificWithPriors k
                    -> DSSpecificWithPriors k
                    -> (SB.RowTypeTag r, SB.StanVar, SB.StanVar, SB.StanVar) -- ps data-set, wgt var, design-matrixes (turnout, share)
                    -> SB.StanVar
                    -> SB.StanVar
                    -> SB.StanVar
                    -> SB.StanVar
                    -> SB.LLSet md gq
                    -> SB.StanBuilderM md gq (SC.InputDataType -> SB.StanVar -> Maybe Text -> SB.StanBuilderM md gq SB.StanVar
                                             , SC.InputDataType -> SB.StanVar -> Maybe Text -> SB.StanBuilderM md gq SB.StanVar
                                             , QR
                                             , QR
                                             , SB.LLSet md gq
                                             , SB.StanVar -> SB.StanBuilderM md gq SB.StanExpr
                                             , SB.StanVar -> SB.StanBuilderM md gq SB.StanExpr
                                             )
addBLModelsForElex' includePP vst eScale officeRow centerTM centerPM qrT qrP dsSpT dsSpP (rttPS, wgtsV, dmPST, dmPSP) alphaT betaT alphaP betaP llSet = do
  let office = officeFromElectionRow officeRow
      dsLabel = "Elex_" <> officeText office
  let addLabel x = x <> "_" <> dsLabel
  (rttElex, cvap, votes, votesInRace, dVotesInRace) <- getElexData officeRow vst eScale
  colTIndexKey <- colIndex dmPST
  colPIndexKey <- colIndex dmPSP
  (dsTAlphaM, dsTBetaM) <- dsSpecific ("T_" <> dsLabel) dsSpT colTIndexKey
  (dsPAlphaM, dsPBetaM) <- dsSpecific ("P_" <> dsLabel) dsSpP colPIndexKey
  (dmTC, centerTF) <- centerIf dmPST centerTM
  (dmTQR, retQRT) <- handleQR rttElex qrT dmTC betaT
  (dmPC, centerPF) <- centerIf dmPSP centerPM
  (dmPQR, retQRP) <- handleQR rttElex qrP dmPC betaP
  betaT' <- addIfM dsTBetaM betaT
  betaP' <- addIfM dsPBetaM betaP
  (pTByElex, pSByElex) <- SB.inBlock SB.SBTransformedParameters
                          $ elexPSFunction (officeText office)
                          rttPS rttElex stateGroup wgtsV
                          (addIf dsTAlphaM alphaT) betaT' dmTQR
                          (addIf dsPAlphaM alphaP) betaP' dmPQR
  let distT = SB.normallyApproximatedBinomial cvap
      distS = SB.normallyApproximatedBinomial votesInRace
  modelVar rttElex distT votes (pure $ SB.var pTByElex)
  modelVar rttElex distS dVotesInRace (pure $ SB.var pSByElex)
  let llSet' = updateLLSet rttElex distT votes (pure $ SB.var pTByElex)
               $ updateLLSet rttElex distS dVotesInRace (pure $ SB.var pSByElex) llSet
  when includePP $ do
    addPosteriorPredictiveCheck ("PP_Election_" <> officeText office <> "_Votes") rttElex distT (pure $ SB.var pTByElex)
    addPosteriorPredictiveCheck ("PP_Election_" <> officeText office <> "DvotesInRace") rttElex distS (pure $ SB.var pSByElex)
  let probT = applyQR retQRT $ fmap (\mu' -> invLogit $ mu' dsTAlphaM dsTBetaM alphaT betaT) . indexedMuE3
      probP = applyQR retQRP $ fmap (\mu' -> invLogit $ mu' dsPAlphaM dsPBetaM alphaP betaP) . indexedMuE3
  return (centerTF, centerPF, retQRT, retQRP, llSet', probT, probP)

addBLModelsForElex offices includePP vst eScale office centerTM centerPM qrT qrP dsSpT dsSpP (rttPS, wgtsV, dmPST, dmPSP) alphaT betaT alphaP betaP llSet =
  case office of
    ET.President -> addBLModelsForElex' includePP vst eScale (PresidentRow stateGroup)
                    centerTM centerPM qrT qrP dsSpT dsSpP (rttPS, wgtsV, dmPST, dmPSP)
                    alphaT betaT alphaP betaP llSet
    ET.Senate -> addBLModelsForElex' includePP vst eScale (SenateRow stateGroup)
                 centerTM centerPM qrT qrP dsSpT dsSpP (rttPS, wgtsV, dmPST, dmPSP)
                 alphaT betaT alphaP betaP llSet
    ET.House -> addBLModelsForElex' includePP vst eScale (HouseRow cdGroup)
                centerTM centerPM qrT qrP dsSpT dsSpP (rttPS, wgtsV, dmPST, dmPSP)
                alphaT betaT alphaP betaP llSet

addBBLModelsForElex' :: forall rs r k md gq. (Typeable md, Typeable gq, Typeable rs, ElectionC rs)
                     => Bool
                     -> ET.VoteShareType
                     -> Int
                     -> ElectionRow rs
                     -> Maybe (SC.InputDataType -> SB.StanVar -> Maybe Text -> SB.StanBuilderM md gq SB.StanVar)
                     -> Maybe (SC.InputDataType -> SB.StanVar -> Maybe Text -> SB.StanBuilderM md gq SB.StanVar)
                     -> QR
                     -> QR
                     -> DSSpecificWithPriors k
                     -> DSSpecificWithPriors k
                     -> (SB.RowTypeTag r, SB.StanVar, SB.StanVar, SB.StanVar) -- ps data-set, wgt var, design-matrixes (turnout, share)
                     -> Bool
                     -> SB.StanVar
                     -> SB.StanVar
                     -> SB.StanVar
                     -> SB.StanVar
                     -> SB.StanVar
                     -> SB.StanVar
                     -> SB.LLSet md gq
                     -> SB.StanBuilderM md gq (SC.InputDataType -> SB.StanVar -> Maybe Text -> SB.StanBuilderM md gq SB.StanVar
                                              , SC.InputDataType -> SB.StanVar -> Maybe Text -> SB.StanBuilderM md gq SB.StanVar
                                              , QR
                                              , QR
                                              , SB.LLSet md gq
                                              , SB.StanVar -> SB.StanBuilderM md gq SB.StanExpr
                                              , SB.StanVar -> SB.StanBuilderM md gq SB.StanExpr
                                              )
addBBLModelsForElex' includePP vst eScale officeRow centerTM centerPM qrT qrP dsSpT dsSpP (rttPS, wgtsV, dmPST, dmPSP) countScaled alphaT betaT scaleT alphaP betaP scaleP llSet = do
  let office = officeFromElectionRow officeRow
      dsLabel = "Elex_" <> officeText office
      addLabel x = x <> "_" <> dsLabel
  (rttElex, cvap, votes, votesInRace, dVotesInRace) <- getElexData officeRow vst eScale
  colIndexT <- colIndex dmPST
  colIndexP <- colIndex dmPSP
  (dsTAlphaM, dsTBetaM) <- dsSpecific ("T_" <> dsLabel) dsSpT colIndexT
  (dsPAlphaM, dsPBetaM) <- dsSpecific ("P_" <> dsLabel) dsSpP colIndexP
  (dmTC, centerTF) <- centerIf dmPST centerTM
  (dmTQR, retQRT) <- handleQR rttElex qrT dmTC betaT
  (dmPC, centerPF) <- centerIf dmPSP centerPM
  (dmPQR, retQRP) <- handleQR rttElex qrP dmPC betaP
  betaT' <- addIfM dsTBetaM betaT
  betaP' <- addIfM dsPBetaM betaP
  (pTByElex, pSByElex) <- SB.inBlock SB.SBTransformedParameters
                          $ elexPSFunction (officeText office)
                          rttPS rttElex stateGroup wgtsV
                          (addIf dsTAlphaM alphaT) betaT' dmTQR
                          (addIf dsPAlphaM alphaP) betaP' dmPQR
  let f x w = if countScaled then x `SB.divide` SB.var w else x `SB.times` SB.var w
      bA mu w = f (SB.paren (SB.var mu)) w
      bB mu w = f (SB.paren (SB.scalar "1" `SB.minus` SB.var mu)) w
      bAT pt = bA pt scaleT
      bBT pt = bB pt scaleT
      bAP pp = bA pp scaleP
      bBP pp = bB pp scaleP
  let distT = (if countScaled then SB.countScaledBetaBinomialDist else SB.betaBinomialDist) True cvap
      distS = (if countScaled then SB.countScaledBetaBinomialDist else SB.betaBinomialDist) True votesInRace
  modelVar rttElex distT votes (pure (bAT pTByElex, bBT pTByElex))
  modelVar rttElex distS dVotesInRace (pure (bAP pSByElex, bBP pSByElex))
  let llSet' = updateLLSet rttElex distT votes (pure (bAT pTByElex, bBT pTByElex))
               $ updateLLSet rttElex distS dVotesInRace (pure (bAP pSByElex, bBP pSByElex)) llSet
  when includePP $ do
    addPosteriorPredictiveCheck ("PP_Election_" <> officeText office <> "_Votes") rttElex distT (pure (bAT pTByElex, bBT pTByElex))
    addPosteriorPredictiveCheck ("PP_Election_" <> officeText office <> "DvotesInRace") rttElex distS (pure (bAP pSByElex, bBP pSByElex))
  let probT = applyQR retQRT $ fmap (\mu' -> invLogit $ mu' dsTAlphaM dsTBetaM alphaT betaT) . indexedMuE3
      probP = applyQR retQRP $ fmap (\mu' -> invLogit $ mu' dsPAlphaM dsPBetaM alphaP betaP) . indexedMuE3
  return (centerTF, centerPF, retQRT, retQRP, llSet', probT, probP)

addBBLModelsForElex offices includePP vst eScale office centerTM centerPM qrT qrP dsSpT dsSpP (rttPS, wgtsV, dmPST, dmPSP) countScaled alphaT betaT scaleT alphaP betaP scaleP llSet = case office of
       ET.President -> addBBLModelsForElex' includePP vst eScale (PresidentRow stateGroup)
                       centerTM centerPM qrT qrP dsSpT dsSpP (rttPS, wgtsV, dmPST, dmPSP)
                       countScaled alphaT betaT scaleT alphaP betaP scaleP llSet
       ET.Senate -> addBBLModelsForElex' includePP vst eScale (SenateRow stateGroup)
                    centerTM centerPM qrP qrT dsSpT dsSpP (rttPS, wgtsV, dmPST, dmPSP)
                    countScaled alphaT betaT scaleT alphaP betaP scaleP llSet
       ET.House -> addBBLModelsForElex' includePP vst eScale (HouseRow cdGroup)
                   centerTM centerPM qrP qrT dsSpT dsSpP (rttPS, wgtsV, dmPST, dmPSP)
                   countScaled alphaT betaT scaleT alphaP betaP scaleP llSet

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

electionModelDM :: forall rs ks r mk.
                   (K.KnitEffects r
                   , BR.CacheEffects r
                   , ModelKeyC ks
                   , F.ElemOf rs BR.StateAbbreviation
                   , F.ElemOf rs DT.CollegeGradC
                   , F.ElemOf rs DT.SexC
                   , F.ElemOf rs DT.Race5C
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
                -> Model mk
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
      compInclude = modelComponents model
  reportZeroRows
  stElexRows <- K.ignoreCacheTime $ fmap stateElectionRows dat_C
  cdElexRows <- K.ignoreCacheTime $ fmap cdElectionRows dat_C
  let stIncPair r = (F.rcast @[BR.Year, BR.StateAbbreviation] r, realToFrac $ F.rgetField @Incumbency r)
      cdIncPair r =  (F.rcast @[BR.Year, BR.StateAbbreviation, BR.CongressionalDistrict] r, realToFrac $ F.rgetField @Incumbency r)
      presIncMap = FL.fold (FL.premap stIncPair FL.map) $ F.filterFrame ((== ET.President) . F.rgetField @ET.Office) stElexRows
      houseIncMap = FL.fold (FL.premap cdIncPair FL.map) $ F.filterFrame ((== ET.House) . F.rgetField @ET.Office) cdElexRows
      incF :: forall as.([BR.Year, BR.StateAbbreviation] F.⊆ as, [BR.Year, BR.StateAbbreviation, BR.CongressionalDistrict] F.⊆ as)
           => ET.OfficeT -> F.Record as -> Double
      incF o r = case o of
        ET.President -> fromMaybe 0 $ M.lookup (F.rcast r) presIncMap
        ET.House -> fromMaybe 0 $ M.lookup (F.rcast r) houseIncMap
        _ -> 0
      dmPrefType = if votesFrom model == Set.fromList [ET.President] then DMPresOnlyPref else DMPref
      modelName = "DM_" <> modelLabel model
      jsonDataName = "DD_" <> dataLabel model <> "_" <> show datYear
      dataAndCodeBuilder :: MRP.BuilderM CCESAndCPSEM (F.FrameRec rs) ()
      dataAndCodeBuilder = do
        let (dmColIndexT, dmColExprT) = DM.designMatrixColDimBinding $ designMatrixRowCCES compInclude densityMatrixRowPart DMTurnout (const 0)
            (dmColIndexP, dmColExprP) = DM.designMatrixColDimBinding $ designMatrixRowCCES compInclude densityMatrixRowPart dmPrefType (const 0)
            centerMatrices = False
            initialCenterFM = if centerMatrices then Nothing else (Just $ \_ v _ -> pure v)
            initialQR = NoQR
            meanTurnout = 0.6
            logit x = Numeric.log (x / (1 - x))
            logitMeanTurnout = logit meanTurnout
            normal m sd = SB.normal (Just $ SB.scalar $ show m) (SB.scalar $ show sd)
            cauchy m s = SB.cauchy (Just $ SB.scalar $ show m) (SB.scalar $ show s)
        elexData <- SB.dataSetTag @(F.Record StateElectionR) SC.ModelData "Elections_President"
        alphaT <- SB.useDataSetForBindings elexData $ do
          muAlphaT <- SMP.addParameter "muAlphaT" SB.StanReal "" (SB.UnVectorized $ normal logitMeanTurnout 1)
          sigmaAlphaT <- SMP.addParameter "sigmaAlphaT" SB.StanReal "<lower=0>"  (SB.UnVectorized $ normal 0 0.4)
          alphaTNonCenterF <- SMP.scalarNonCenteredF muAlphaT sigmaAlphaT
          SMP.addHierarchicalScalar "alphaT" stateGroup (SMP.NonCentered alphaTNonCenterF) $ normal 0 1
--          SMP.addHierarchicalScalar "alphaT" stateGroup SMP.Centered $ SB.normal (Just $ SB.var muAlphaT)  (SB.var sigmaAlphaT)
--          pure muAlphaT
        thetaT <- SB.useDataSetForBindings elexData $ do
          SB.addDeclBinding' dmColIndexT dmColExprT
          SB.addUseBinding' dmColIndexT dmColExprT
          muThetaT <- SMP.addParameter "muThetaT" (SB.StanVector $ SB.NamedDim dmColIndexT) "" (SB.Vectorized (one dmColIndexT) (normal 0 1))
          case betaType model of
            HierarchicalBeta -> do
              tauThetaT <- SMP.addParameter "tauThetaT" (SB.StanVector $ SB.NamedDim dmColIndexT) "<lower=0>" (SB.Vectorized (one dmColIndexT) (normal 0 0.4))
              corrThetaT <- SMP.lkjCorrelationMatrixParameter "corrT" dmColIndexT 1
              thetaTNonCenteredF <- SMP.vectorNonCenteredF (SB.taggedGroupName stateGroup) muThetaT tauThetaT corrThetaT
              SMP.addHierarchicalVector "thetaT" dmColIndexT stateGroup (SMP.NonCentered thetaTNonCenteredF) (normal 0 0.4)
            SingleBeta -> pure muThetaT
        alphaP <- SB.useDataSetForBindings elexData $ do
          muAlphaP <- SMP.addParameter "muAlphaP" SB.StanReal "" (SB.UnVectorized $ normal 0 1)
          sigmaAlphaP <- SMP.addParameter "sigmaAlphaP" SB.StanReal "<lower=0>"  (SB.UnVectorized $ normal 0 0.4)
          alphaPNonCenterF <- SMP.scalarNonCenteredF muAlphaP sigmaAlphaP
          SMP.addHierarchicalScalar "alphaP" stateGroup (SMP.NonCentered alphaPNonCenterF) (normal 0 1)
--          SMP.addHierarchicalScalar "alphaP" stateGroup SMP.Centered $ SB.normal (Just $ SB.var muAlphaP)  (SB.var sigmaAlphaP)
--          pure muAlphaP
        thetaP <- SB.useDataSetForBindings elexData $ do
          SB.addDeclBinding' dmColIndexP dmColExprP
          SB.addUseBinding' dmColIndexP dmColExprP
          muThetaP <- SMP.addParameter "muThetaP" (SB.StanVector $ SB.NamedDim dmColIndexP) "" (SB.Vectorized (one dmColIndexP) (normal 0 1))
          case betaType model of
            HierarchicalBeta -> do
              tauThetaP <- SMP.addParameter "tauThetaP" (SB.StanVector $ SB.NamedDim dmColIndexP) "<lower=0>" (SB.Vectorized (one dmColIndexP) (normal 0 0.4))
              corrThetaP <- SMP.lkjCorrelationMatrixParameter "corrP" dmColIndexP 1
              thetaPNonCenteredF <- SMP.vectorNonCenteredF (SB.taggedGroupName stateGroup) muThetaP tauThetaP corrThetaP
              SMP.addHierarchicalVector "thetaP" dmColIndexP stateGroup (SMP.NonCentered thetaPNonCenteredF) (normal 0 0.4)
            SingleBeta -> pure muThetaP
        (dssWPT, dssWPP) <- case dataSetSpecific model of
          DSNone -> return (DSPNone, DSPNone)
          DSAlpha -> let x = DSPAlpha (normal 0 0.5) in return (x, x)
          DSAlphaGroup gtt -> let x = DSPAlphaGroup gtt (normal 0 0.5) (normal 0 0.4) in return (x, x)
          DSAlphaBeta -> SB.useDataSetForBindings elexData $ do
            SB.addDeclBinding' dmColIndexT dmColExprT
            SB.addDeclBinding' dmColIndexP dmColExprP
--            SB.addUseBinding' dmColIndexT dmColExprT
            sigmaAlphaTDS <- SMP.addParameter "sigmaAlphaT_DS" SB.StanReal "<lower=0>" (SB.UnVectorized $ normal 0 0.5)
            sigmaBetaTDS <- SMP.addParameter "sigmaThetaT_DS" (SB.StanVector $ SB.NamedDim dmColIndexT) "<lower=0>" (SB.Vectorized (one dmColIndexT) (normal 0 0.4))
            let wpt = DSPAlphaBeta (SB.normal Nothing $ SB.var sigmaAlphaTDS) (SB.normal Nothing $ SB.var sigmaBetaTDS)
            sigmaAlphaPDS <- SMP.addParameter "sigmaAlphaP_DS" SB.StanReal "<lower=0>" (SB.UnVectorized $ normal 0 0.5)
            sigmaBetaPDS <- SMP.addParameter "sigmaThetaP_DS" (SB.StanVector $ SB.NamedDim dmColIndexP) "<lower=0>" (SB.Vectorized (one dmColIndexP) (normal 0 0.4))
            let wpp = DSPAlphaBeta (SB.normal Nothing $ SB.var sigmaAlphaPDS) (SB.normal Nothing $ SB.var sigmaBetaPDS)
            return (wpt, wpp)
{-
          DSAlphaBeta -> do
            let ap = normal 0 0.5
                bp = normal 0 0.5
            return $ (DSPAlphaBeta ap bp, DSPAlphaBeta ap bp)
-}
        (acsRowTag, acsPSWgts, acsDMT, acsDMP) <- setupACSPSRows compInclude densityMatrixRowPart dmPrefType (const 0) -- incF but for which office?
        (elexModelF, cpsTF, ccesTF, ccesPF) <- case distribution model of
              Binomial -> do
                let eF (centerTFM, centerPFM, qrT, qrP, llS) office
                      = addBLModelsForElex (votesFrom model) includePP (voteShareType model) (electionScale model)
                        office centerTFM centerPFM qrT qrP dssWPT dssWPP --(elexDSSp (votesFrom model) office (dataSetSpecific model))
                        (acsRowTag, acsPSWgts, acsDMT, acsDMP) alphaT thetaT alphaP thetaP llS
                    cpsTF' centerTFM qrT llS = addBLModelForDataSet "CPST" includePP (setupCPSData compInclude densityMatrixRowPart)
                                               dssWPT centerTFM qrT alphaT thetaT llS
                    ccesTF' centerTFM qrT llS = addBLModelForDataSet "CCEST" includePP (setupCCESTData compInclude densityMatrixRowPart)
                                                dssWPT centerTFM qrT alphaT thetaT llS
                    ccesPF' (centerPFM, qrP, llS) office
                      =  addBLModelForDataSet ("CCESP" <> show office) includePP
                         (setupCCESPData compInclude densityMatrixRowPart dmPrefType incF office (voteShareType model))
                         dssWPP centerPFM qrP alphaP thetaP llS
                return (eF, cpsTF', ccesTF', ccesPF')
              BetaBinomial n -> do
                let lnPriorMu = 2 * (round $ Numeric.log $ realToFrac n)
                    betaWidthPrior = SB.UnVectorized $ SB.function "lognormal"  (SB.scalar (show lnPriorMu) :| [SB.scalar "1"])
                betaWidthT <- SMP.addParameter "betaWidthT" SB.StanReal ("<lower=" <> show n <> ">") betaWidthPrior
                betaWidthP <- SMP.addParameter "betaWidthP" SB.StanReal ("<lower=" <> show n <> ">") betaWidthPrior
                let eF (centerTFM, centerPFM, qrT, qrP, llS) office
                      = addBBLModelsForElex (votesFrom model) includePP (voteShareType model) (electionScale model)
                        office centerTFM centerPFM qrT qrP dssWPT dssWPP --(elexDSSp (votesFrom model) office (dataSetSpecific model))
                        (acsRowTag, acsPSWgts, acsDMT, acsDMP)
                        False alphaT thetaT betaWidthT alphaP thetaP betaWidthP llS
                    cpsTF' centerTFM qrT llS = addBBLModelForDataSet "CPST" includePP (setupCPSData compInclude densityMatrixRowPart)
                                               dssWPT centerTFM qrT False alphaT thetaT betaWidthT llS
                    ccesTF' centerTFM qrT llS = addBBLModelForDataSet "CCEST" includePP (setupCCESTData compInclude densityMatrixRowPart)
                                                dssWPT centerTFM qrT False alphaT thetaT betaWidthT llS
                    ccesPF' (centerPFM, qrP, llS) office
                      =  addBBLModelForDataSet ("CCESP" <> show office) includePP
                         (setupCCESPData compInclude densityMatrixRowPart dmPrefType incF office (voteShareType model))
                         dssWPP centerPFM qrP False alphaP thetaP betaWidthP llS
                return (eF, cpsTF', ccesTF', ccesPF')
              CountScaledBB n -> do
                let lnPriorMu = (round $ Numeric.log $ realToFrac n) `div` 2
                    scalePrior = SB.UnVectorized $ SB.function "lognormal"  (SB.scalar (show lnPriorMu) :| [SB.scalar "1"])
                scaleT <- SMP.addParameter "betaScaleT" SB.StanReal ("<lower=1, upper=" <> show n <> ">") scalePrior
                scaleP <- SMP.addParameter "betaScaleP" SB.StanReal ("<lower=1, upper=" <> show n <> ">") scalePrior
                let eF (centerTFM, centerPFM, qrT, qrP, llS) office
                      = addBBLModelsForElex (votesFrom model) includePP (voteShareType model) (electionScale model)
                        office centerTFM centerPFM qrT qrP dssWPT dssWPP --(elexDSSp (votesFrom model) office (dataSetSpecific model))
                        (acsRowTag, acsPSWgts, acsDMT, acsDMP) True alphaT thetaT scaleT alphaP thetaP scaleP llS
                    cpsTF' centerTFM qrT llS = addBLModelForDataSet "CPST" includePP (setupCPSData compInclude densityMatrixRowPart)
                                               dssWPT centerTFM qrT alphaT thetaT llS
                    ccesTF' centerTFM qrT llS = addBLModelForDataSet "CCEST" includePP (setupCCESTData compInclude densityMatrixRowPart)
                                                dssWPT centerTFM qrT alphaT thetaT llS
                    ccesPF' (centerPFM, qrP, llS) office
                      =  addBLModelForDataSet ("CCESP" <> show office) includePP
                         (setupCCESPData compInclude densityMatrixRowPart dmPrefType incF office (voteShareType model))
                         dssWPP centerPFM qrP alphaP thetaP llS
                return (eF, cpsTF', ccesTF', ccesPF')

        let elexModels (centerTFM, centerPFM, qrT, qrP, llS, probsM) office = do
              (centerTF, centerPF, qrT', qrP', llS', probT, probP) <- elexModelF (centerTFM, centerPFM, qrT, qrP, llS) office
              let probs' = case probsM of
                    Nothing -> Just (office, probT, probP)
                    Just (o, pT, pP) -> Just $ case o of
                                            ET.Senate -> (office, probT, probP)
                                            ET.President -> if office == ET.House then (office, probT, probP) else (o, pT, pP)
                                            ET.House -> (o, pT, pP)
              return (Just centerTF, Just centerPF, qrT', qrP', llS', probs')
            extract (cfM1, cfM2, qrT, qrP, x, pM) = do
              cf1 <- SB.stanBuildMaybe "elexModels fold returned a Nothing for turnout centering function!" cfM1
              cf2 <- SB.stanBuildMaybe "elexModels fold returned a Nothing for pref centering function!" cfM2
              (_, pT, pP) <- SB.stanBuildMaybe "elexModels fold returned a Nothing for ps probs!" pM
              return (cf1, cf2, qrT, qrP, x, pT, pP)
            elexFoldM = FL.FoldM
                        elexModels
                        (return (initialCenterFM, initialCenterFM, initialQR, initialQR, SB.emptyLLSet, Nothing))
                        extract
        (centerTF, centerPF, qrT, qrP, llSet1, probTF, probPF) <- FL.foldM elexFoldM (votesFrom model)
        (_, _, llSet2, _) <- ccesTF (Just centerTF) qrT llSet1
        (_, _, llSet3, _) <- cpsTF (Just centerTF) qrT llSet2

        let (dmColIndexP, dmColExprP) = DM.designMatrixColDimBinding
                                        $ designMatrixRowCCES compInclude densityMatrixRowPart dmPrefType (const 0)
        let ccesP (centerFM, qrP, llS) office = do
              (centerF, qrP', llS, _) <- ccesPF (centerFM, qrP, llS) office
              return (Just centerF, qrP', llS)
            llFoldM = FL.FoldM ccesP (return (Just centerPF, qrP, llSet3)) return
        (_, _, llSet4) <- FL.foldM llFoldM (Set.delete ET.Senate $ votesFrom model)

        SB.generateLogLikelihood' $ llSet4 --SB.mergeLLSets llSetT llSetP
        psData <- SB.dataSetTag @(F.Record rs) SC.GQData "PSData"
        dmPS_T' <- DM.addDesignMatrix psData (designMatrixRowPS compInclude densityMatrixRowPart DMTurnout)
        dmPS_P' <- DM.addDesignMatrix psData (designMatrixRowPS compInclude densityMatrixRowPart dmPrefType)
        dmPS_T <- centerTF SC.GQData dmPS_T' (Just "T")
        dmPS_P <- centerPF SC.GQData dmPS_P' (Just "P")
        probT <- SB.useDataSetForBindings psData $ probTF dmPS_T
        probP <- SB.useDataSetForBindings psData $ probPF dmPS_P

        let psTPrecompute dat  = SB.vectorizeExpr "pT" probT (SB.dataSetName dat)
            psTExpr :: SB.StanVar -> SB.StanBuilderM md gq SB.StanExpr
            psTExpr p =  pure $ SB.var p
            psPPrecompute dat = SB.vectorizeExpr "pD" probP (SB.dataSetName dat)
            psPExpr :: SB.StanVar -> SB.StanBuilderM md gq SB.StanExpr
            psPExpr p =  pure $ SB.var p
            psDVotePreCompute dat = (,) <$> psTPrecompute dat <*> psPPrecompute dat
            psDVoteExpr :: (SB.StanVar, SB.StanVar) -> SB.StanBuilderM md gq SB.StanExpr
            psDVoteExpr (pT, pD) = pure $ SB.var pT `SB.times` SB.var pD
            turnoutPS = ((psTPrecompute psData, psTExpr), Nothing)
            prefPS = ((psPPrecompute psData, psPExpr), Nothing)
            dVotePS = ((psDVotePreCompute psData, psDVoteExpr), Just $ SB.var . fst)
            postStratify :: (Typeable md, Typeable gq, Ord k)
                         => Text
                         -> ((SB.StanBuilderM md gq x, x -> SB.StanBuilderM md gq SB.StanExpr), Maybe (x -> SB.StanExpr))
                         -> SB.GroupTypeTag k -> SB.StanBuilderM md gq SB.StanVar
            postStratify name psCalcs grp =
              MRP.addPostStratification
              (fst psCalcs)
              (Just name)
              elexData
              psData
              (realToFrac . F.rgetField @Census.Count)
              (MRP.PSShare $ snd psCalcs)
              (Just grp)
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
            cds = FL.fold (FL.premap districtKey FL.list) (ccesEMRows modelData)
            psKeys = FL.fold (FL.premap F.rcast FL.list) gqData
            groups = groupBuilderDM model psGroup states cds psKeys
        K.logLE K.Info $ show $ zip [1..] $ Set.toList $ FL.fold FL.set states
        MRP.buildDataWranglerAndCode @BR.SerializerC @BR.CacheData groups dataAndCodeBuilder modelData_C gqData_C
  let addUnwrapIf o varName unwrapName = if (Set.member o $ votesFrom model) then [SR.UnwrapNamed varName unwrapName] else []
  let unwrapVoted = [SR.UnwrapNamed "Voted_CPS" "yCPSVotes", SR.UnwrapNamed "Voted_CCES" "yCCESVotes"]
                    ++ addUnwrapIf ET.President "Voted_ElexPr" "yVoted_Elections_Pr"
                    ++ addUnwrapIf ET.Senate "Voted_ElexSe" "yVoted_Elections_Se"
                    ++ addUnwrapIf ET.House "Voted_ElexHo" "yVoted_Elections_Ho"
  let unwrapDVotes = []
                     ++ addUnwrapIf ET.President "DVotesInRace_CCES_President" "yCCESDVotesPresident"
                     ++ addUnwrapIf ET.House "DVotesInRace_CCES_House" "yCCESDVotesHouse"
                     ++ addUnwrapIf ET.President "DVotesInRace_Elex_Pr" "yElectionDVotes_Pr"
                     ++ addUnwrapIf ET.Senate "DVotesInRace_Elex_Se" "yElectionsDVotes_Se"
                     ++ addUnwrapIf ET.House "DVotesInRace_Elex_Ho" "yElectionsDVotes_Ho"

      gqNames = SC.GQNames ("By" <> SB.taggedGroupName psGroup) (psDataLabel model psDataSetName)
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

data DensityTransform = RawDensity
                      | LogDensity
                      | BinDensity { numBins :: Int,  modeledRange :: Int }
                      | SigmoidDensity { sigmoidCenter :: Int, sigmoidSlope :: Int, modeledRange :: Int}
                      deriving (Eq, Ord, Generic)
instance Flat.Flat DensityTransform
type instance FI.VectorFor DensityTransform = Vector.Vector

printDensityTransform :: DensityTransform -> Text
printDensityTransform RawDensity = "RawDensity"
printDensityTransform LogDensity = "LogDensity"
printDensityTransform (BinDensity bins range) = "BinDensity_" <> show bins <> "_" <> show range
printDensityTransform (SigmoidDensity c s r) = "SigmoidDensity_" <> show c <> "_" <> show s <> "_" <> show r

type GetCCESVotesC rs = (F.ElemOf rs HouseVotes
                        , F.ElemOf rs HouseDVotes
                        , F.ElemOf rs HouseRVotes
                        , F.ElemOf rs PresVotes
                        , F.ElemOf rs PresDVotes
                        , F.ElemOf rs PresRVotes
                        )

getCCESVotes :: GetCCESVotesC rs
             => ET.OfficeT -> ET.VoteShareType -> (F.Record rs -> Int, F.Record rs -> Int)
getCCESVotes ET.House ET.FullShare = (F.rgetField @HouseVotes, F.rgetField @HouseDVotes)
getCCESVotes ET.House ET.TwoPartyShare = (\r -> F.rgetField @HouseDVotes r + F.rgetField @HouseRVotes r, F.rgetField @HouseDVotes)
getCCESVotes ET.President ET.FullShare = (F.rgetField @PresVotes, F.rgetField @PresDVotes)
getCCESVotes ET.President ET.TwoPartyShare = (\r ->  F.rgetField @PresDVotes r + F.rgetField @PresRVotes r, F.rgetField @PresDVotes)
getCCESVotes _ _ = (const 0, const 0) -- we shouldn't call this

zeroCCESVotes :: GetCCESVotesC rs
              => ET.OfficeT -> ET.VoteShareType -> F.Record rs -> Bool
zeroCCESVotes office vst r = votesInRace r == 0 where
  (votesInRace, _) = getCCESVotes office vst

countCCESZeroVoteRows :: (GetCCESVotesC rs
                         , Foldable f
                         )
                      => ET.OfficeT -> ET.VoteShareType -> f (F.Record rs) -> Int
countCCESZeroVoteRows office vst = FL.fold (FL.prefilter (zeroCCESVotes office vst) FL.length)

data BetaType = HierarchicalBeta | SingleBeta deriving (Show,Eq)

printBetaType :: BetaType -> Text
printBetaType HierarchicalBeta = "Hab"
printBetaType SingleBeta = "Ha"

data Distribution = Binomial | BetaBinomial Int | CountScaledBB Int deriving (Show)
printDistribution :: Distribution -> Text
printDistribution Binomial = "Binomial"
printDistribution (BetaBinomial n) = "BetaBinomial" <> show n
printDistribution (CountScaledBB n) = "CountScaledBB" <> show n

data DataSetSpecific k = DSNone | DSAlpha | DSAlphaGroup (SB.GroupTypeTag k) | DSAlphaBeta

printDataSetSpecific :: DataSetSpecific k -> Text
printDataSetSpecific DSNone = "DSn"
printDataSetSpecific DSAlpha = "DSa"
printDataSetSpecific (DSAlphaGroup gtt) = "DSa" <> (SB.taggedGroupName gtt)
printDataSetSpecific DSAlphaBeta = "DSab"

data DSSpecificWithPriors k = DSPNone
                            | DSPAlpha SB.StanExpr
                            | DSPAlphaGroup (SB.GroupTypeTag k) SB.StanExpr SB.StanExpr
                            | DSPAlphaBeta SB.StanExpr SB.StanExpr


printVotesFrom :: Set ET.OfficeT -> Text
printVotesFrom = mconcat . fmap (T.take 1 . show) . Set.toList

data Model k = Model { voteShareType :: ET.VoteShareType
                     , votesFrom :: Set ET.OfficeT
                     , densityTransform :: DensityTransform
                     , modelComponents :: Set DMComponents
                     , distribution :: Distribution
                     , dataSetSpecific :: DataSetSpecific k
                     , betaType :: BetaType
                     , electionScale :: Int
                     }  deriving (Generic)


-- NB each of these records what's relevant to the named thing

-- Stan code
modelLabel :: Model k -> Text
modelLabel m = printVotesFrom (votesFrom m)
               <> "_" <> printDistribution (distribution m)
               <> "_" <> printDataSetSpecific (dataSetSpecific m)
               <> "_" <> printBetaType (betaType m) --"HierAlpha" <> (if betaType m == HierarchicalBeta then "Beta" else "")

-- model data
dataLabel :: Model k -> Text
dataLabel m = show (voteShareType m)
              <> "_" <> printVotesFrom (votesFrom m)
              <> "_" <> printDensityTransform (densityTransform m)
              <> "_" <> printComponents (modelComponents m)
              <> "_EScale" <> show (electionScale m)

-- post-strat gq data
psDataLabel :: Model k -> Text -> Text
psDataLabel m psName = psName
                       <> "_"  <> printDensityTransform (densityTransform m)
                       <> "_" <> printComponents (modelComponents m)

type SLDLocation = (Text, ET.DistrictType, Text)

sldLocationToRec :: SLDLocation -> F.Record [BR.StateAbbreviation, ET.DistrictTypeC, ET.DistrictName]
sldLocationToRec (sa, dt, dn) = sa F.&: dt F.&: dn F.&: V.RNil

type ModeledShare = "ModeledShare" F.:-> MT.ConfidenceInterval
type ModeledTurnout = "ModeledTurnout" F.:-> MT.ConfidenceInterval
type ModeledPref = "ModeledPref" F.:-> MT.ConfidenceInterval
type ModelDesc = "ModelDescription" F.:-> Text

type ModelResultsR ks  = '[BR.Year] V.++ ks V.++ '[ModelDesc, ModeledTurnout, ModeledPref, ModeledShare]
type ModelResults ks = F.FrameRec (ModelResultsR ks)

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
          ,F.ElemOf rs ET.DistrictName)
       => F.Record rs -> SLDLocation
sldKey r = (F.rgetField @BR.StateAbbreviation r
           , F.rgetField @ET.DistrictTypeC r
           , F.rgetField @ET.DistrictName r
           )
districtKey r = F.rgetField @BR.StateAbbreviation r <> "-" <> show (F.rgetField @BR.CongressionalDistrict r)
wnh r = (F.rgetField @DT.RaceAlone4C r == DT.RA4_White) && (F.rgetField @DT.HispC r == DT.NonHispanic)
wnhNonGrad r = wnh r && (F.rgetField @DT.CollegeGradC r == DT.NonGrad)
wnhCCES r = (F.rgetField @DT.Race5C r == DT.R5_WhiteNonHispanic)
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
  y r = realToFrac c / (d r)
  yk r = (y r) ** realToFrac s
  f r = VU.singleton $ (realToFrac range / 2) * (1 - yk r) / (1 + yk r)


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


{-
addBBModelForDataSet :: (Typeable md, Typeable gq)
                     => Text
                     -> Bool
                     -> SB.StanBuilderM md gq (SB.RowTypeTag r, DM.DesignMatrixRow r, SB.StanVar, SB.StanVar, SB.StanVar)
                     -> DataSetAlpha
                     -> Maybe (SC.InputDataType -> SB.StanVar -> Maybe Text -> SB.StanBuilderM md gq SB.StanVar)
                     -> SB.StanVar
                     -> SB.StanVar
                     -> SB.LLSet md gq
                     -> SB.StanBuilderM md gq (SC.InputDataType -> SB.StanVar -> Maybe Text -> SB.StanBuilderM md gq SB.StanVar, SB.LLSet md gq)
addBBModelForDataSet dataSetLabel includePP dataSetupM dataSetAlpha centerM alpha beta llSet = do
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

addBBModelForDataSet' :: (Typeable md, Typeable gq)
                     => Text
                     -> Bool
                     -> SB.StanBuilderM md gq (SB.RowTypeTag r, DM.DesignMatrixRow r, SB.StanVar, SB.StanVar, SB.StanVar)
                     -> DataSetAlpha
                     -> Maybe (SC.InputDataType -> SB.StanVar -> Maybe Text -> SB.StanBuilderM md gq SB.StanVar)
                     -> Int
                     -> SB.StanVar
                     -> SB.StanVar
                     -> SB.LLSet md gq
                     -> SB.StanBuilderM md gq (SC.InputDataType -> SB.StanVar -> Maybe Text -> SB.StanBuilderM md gq SB.StanVar, SB.LLSet md gq)
addBBModelForDataSet' dataSetLabel includePP dataSetupM dataSetAlpha centerM minKappa alpha beta llSet = do
  let addLabel x = x <> "_" <> dataSetLabel
  (rtt, designMatrixRow, counts, successes, dm) <- dataSetupM
  dmColIndex <- case dm of
    (SB.StanVar _ (SB.StanMatrix (_, SB.NamedDim ik))) -> return ik
    (SB.StanVar m _) -> SB.stanBuildError $ "addModelForData: dm is not a matrix with named row index"
  let minKappaConstraint = "<lower=" <> show minKappa <> ">"
      kappaPrior = let lmk = round $ Numeric.log $ realToFrac minKappa
                   in SB.function "lognormal" $ (SB.scalar (show $ lmk + 1) :| [SB.scalar (show $ lmk + 3)])
  kappa <- SMP.addParameter (addLabel "kappa") SB.StanReal minKappaConstraint (SB.UnVectorized kappaPrior)
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
      betaA ixM k dm = SB.var k `SB.times` muT ixM dm
      betaB ixM k dm = SB.var k `SB.times` SB.paren (SB.scalar "1.0" `SB.minus` muT ixM dm)
      vecBetaA = SB.vectorizeExpr (addLabel "betaA") (betaA dsIxM kappa dmC) (SB.dataSetName rtt)
      vecBetaB = SB.vectorizeExpr (addLabel "betaB") (betaB dsIxM kappa dmC) (SB.dataSetName rtt)
  SB.inBlock SB.SBModel $ do
    SB.useDataSetForBindings rtt $ do
      betaA <- vecBetaA
      betaB <- vecBetaB
      SB.sampleDistV rtt dist (SB.var betaA, SB.var betaB) successes
  let llDetails =  SB.LLDetails dist (pure (betaA dsIxM kappa dmC, betaB dsIxM kappa dmC)) successes
      llSet' = SB.addToLLSet rtt llDetails llSet
      pp = SB.StanVar (addLabel "PP") (SB.StanVector $ SB.NamedDim $ SB.dataSetName rtt)
  when includePP $ do
    SB.useDataSetForBindings rtt
      $ SB.generatePosteriorPrediction rtt pp dist (betaA dsIxM kappa dmC, betaB dsIxM kappa dmC)
    pure ()
  return (centerF, llSet')


addBBModelForDataSet'' :: (Typeable md, Typeable gq)
                       => Text
                       -> Bool
                       -> SB.StanBuilderM md gq (SB.RowTypeTag r, DM.DesignMatrixRow r, SB.StanVar, SB.StanVar, SB.StanVar)
                       -> DataSetAlpha
                       -> Maybe (SC.InputDataType -> SB.StanVar -> Maybe Text -> SB.StanBuilderM md gq SB.StanVar)
                       -> Double
                       -> SB.StanVar
                       -> SB.StanVar
                       -> SB.LLSet md gq
                       -> SB.StanBuilderM md gq (SC.InputDataType -> SB.StanVar -> Maybe Text -> SB.StanBuilderM md gq SB.StanVar, SB.LLSet md gq)
addBBModelForDataSet'' dataSetLabel includePP dataSetupM dataSetAlpha centerM minKappa alpha beta llSet = do
  let addLabel x = x <> "_" <> dataSetLabel
  (rtt, designMatrixRow, counts, successes, dm) <- dataSetupM
  dmColIndex <- case dm of
    (SB.StanVar _ (SB.StanMatrix (_, SB.NamedDim ik))) -> return ik
    (SB.StanVar m _) -> SB.stanBuildError $ "addModelForData: dm is not a matrix with named row index"
  let minKappaConstraint = "<lower=" <> show minKappa <> ", upper=1>"
      kappaPrior = SB.function "lognormal" $ (SB.scalar "-5" :| [SB.scalar "5"])
  kappa <- SMP.addParameter (addLabel "kappa") SB.StanReal minKappaConstraint (SB.UnVectorized kappaPrior)
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
  let dist = SB.countScaledBetaBinomialDist True counts
      dmBetaE dmE betaE = SB.vectorizedOne dmColIndex $ SB.function "dot_product" (dmE :| [betaE])
      muE aE dmE betaE = SB.function "inv_logit" $ one $ aE `SB.plus` dmBetaE dmE betaE
      muT ixM dm = case ixM of
        Nothing -> muE (SB.var alpha) (SB.var dm) (SB.var beta)
        Just ixV -> muE (SB.var ixV `SB.plus` SB.var alpha) (SB.var dm) (SB.var beta)
      betaA ixM k dm = SB.var k `SB.times` muT ixM dm
      betaB ixM k dm = SB.var k `SB.times` SB.paren (SB.scalar "1.0" `SB.minus` muT ixM dm)
      vecBetaA = SB.vectorizeExpr (addLabel "betaA") (betaA dsIxM kappa dmC) (SB.dataSetName rtt)
      vecBetaB = SB.vectorizeExpr (addLabel "betaB") (betaB dsIxM kappa dmC) (SB.dataSetName rtt)
  SB.inBlock SB.SBModel $ do
    SB.useDataSetForBindings rtt $ do
      betaA <- vecBetaA
      betaB <- vecBetaB
      SB.sampleDistV rtt dist (SB.var betaA, SB.var betaB) successes
  let llDetails =  SB.LLDetails dist (pure (betaA dsIxM kappa dmC, betaB dsIxM kappa dmC)) successes
      llSet' = SB.addToLLSet rtt llDetails llSet
      pp = SB.StanVar (addLabel "PP") (SB.StanVector $ SB.NamedDim $ SB.dataSetName rtt)
  when includePP $ do
    SB.useDataSetForBindings rtt
      $ SB.generatePosteriorPrediction rtt pp dist (betaA dsIxM kappa dmC, betaB dsIxM kappa dmC)
    pure ()
  return (centerF, llSet')

addNormalModelForDataSet :: (Typeable md, Typeable gq)
                         => Text
                         -> Bool
                         -> SB.StanBuilderM md gq (SB.RowTypeTag r, DM.DesignMatrixRow r, SB.StanVar, SB.StanVar, SB.StanVar)
                         -> DataSetAlpha
                         -> Maybe (SC.InputDataType -> SB.StanVar -> Maybe Text -> SB.StanBuilderM md gq SB.StanVar)
                         -> SB.StanVar
                         -> SB.StanVar
                         -> SB.LLSet md gq
                         -> SB.StanBuilderM md gq (SC.InputDataType -> SB.StanVar -> Maybe Text -> SB.StanBuilderM md gq SB.StanVar, SB.LLSet md gq)
addNormalModelForDataSet dataSetLabel includePP dataSetupM dataSetAlpha centerM alpha beta llSet = do
  let addLabel x = x <> "_" <> dataSetLabel
  (rtt, _, counts, successes, dm) <- dataSetupM
  dmColIndex <- case dm of
    (SB.StanVar _ (SB.StanMatrix (_, SB.NamedDim ik))) -> return ik
    (SB.StanVar m _) -> SB.stanBuildError $ "addModelForData: dm is not a matrix with named row index"
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
  let dist = SB.normalDist
      dmBetaE dmE betaE = SB.vectorizedOne dmColIndex $ SB.function "dot_product" (dmE :| [betaE])
      muE aE dmE betaE = SB.var counts `SB.times` (SB.function "inv_logit" $ one $ aE `SB.plus` dmBetaE dmE betaE)
      muT ixM dm = case ixM of
        Nothing -> muE (SB.var alpha) (SB.var dm) (SB.var beta)
        Just ixV -> muE (SB.var ixV `SB.plus` SB.var alpha) (SB.var dm) (SB.var beta)
      sdT ixM dm = SB.scalar "0.01" `SB.times` muT ixM dm
      vecMu = SB.vectorizeExpr (addLabel "mu") (muT dsIxM dmC) (SB.dataSetName rtt)
      vecSD = SB.vectorizeExpr (addLabel "sd") (sdT dsIxM dmC) (SB.dataSetName rtt)
  SB.inBlock SB.SBModel $ do
    SB.useDataSetForBindings rtt $ do
      mu <- vecMu
      sd <- vecSD
      SB.sampleDistV rtt dist (SB.var mu, SB.var sd) successes
  let llDetails =  SB.LLDetails dist (pure (muT dsIxM dmC, sdT dsIxM dmC)) successes
      llSet' = SB.addToLLSet rtt llDetails llSet
      pp = SB.StanVar (addLabel "PP") (SB.StanVector $ SB.NamedDim $ SB.dataSetName rtt)
  when includePP $ do
    SB.useDataSetForBindings rtt
      $ SB.generatePosteriorPrediction rtt pp dist (muT dsIxM dmC, sdT dsIxM dmC)
    pure ()
  return (centerF, llSet')

-}
{-
  let (SB.StanVar _ psWgtsType) = psWgts
  psIndex <- case psWgtsType of
    SB.StanArray [SB.NamedDim ik] SB.StanInt -> return ik
    _ -> SB.stanBuildError "psStanFunction: psWgts not an int array"
  let (SB.StanVar _ dmTType) = dmT
  dmTColIndex <- case dmTType of
    SB.StanMatrix (_, SB.NamedDim ik) -> return ik
    _ -> SB.stanBuildError "psStanFunction: dmT not a matrix with named column dim."
  let (SB.StanVar _ dmPType) = dmP
  dmPColIndex <- case dmPType of
    SB.StanMatrix (_, SB.NamedDim ik) -> return ik
    _ -> SB.stanBuildError "psStanFunction: dmP not a matrix with named column dim."
  let (SB.StanVar _ alphaTType) = alphaT
  case alphaTType of
    SB.Stan
      declBindings = Map.fromList [(gik,SB.bare grpN)]
  SB.addScopedDeclBindings declBindings $ do
    p <- SB.stanDeclare "p" (SB.StanMatrix (SB.NamedDim gik, SB.GivenDim 2)) ""
  let pCol = SB.StanVar "p" (SB.StanVector $ SB.NamedDim gik)
  wgt <- SB.stanDeclare "wgt" (SB.StanMatrix (SB.NamedDim gik, SB.GivenDim 2)) ""
  let wgtCol = SB.StanVar "wgt" (SB.StanVector $ SB.NamedDim gik)
  let grpIndexed x = SB.indexBy x (SB.varName grpIndex)
  ptv <- SB.vectorizeExpr
         "pT"
         (invLogit
          $ SB.var alphaT `SB.plus` SB.vectorizedOne dmTColIndex (SB.function "dot_product" (SB.var dmT :| [SB.var betaT]))
         )
         psIndex
  ppv <- SB.vectorizeExpr
         "pP"
         (invLogit
          $ SB.var alphaP `SB.plus` SB.vectorizedOne dmPColIndex (SB.function "dot_product" (SB.var dmP :| [SB.var betaP]))
         )
         psIndex
  SB.stanForLoopB "k" Nothing psIndex $ do
    ps <- SB.stanDeclareRHS "ps" (SB.StanVector $ SB.ExprDim $ SB.scalar "2") ""
      $ SB.group "[" "]" $ SB.csExprs
      $ (SB.var psWgts `SB.times` grpIndexed (SB.var ptv))
      :| [SB.var psWgts `SB.times` grpIndexed (SB.var ptv) `SB.times` grpIndexed (SB.var ptv)]
    SB.addExprLine "psStanFunction" $ SB.var p `SB.eq` SB.var ps
    wgts <- SB.stanDeclareRHS "wgts" (SB.StanVector $ SB.ExprDim $ SB.scalar "2") ""
      $ SB.group "[" "]" $ SB.csExprs
      $ (SB.var psWgts) :| [SB.var psWgts `SB.times` grpIndexed (SB.var ptv)]
    SB.addExprLine "psStanFunction" $ SB.var wgt `SB.eq` SB.var wgts
  SB.addExprLine "psStanFunction" $ SB.binOp "./=" (SB.var p) (SB.var wgt)
  return $ SB.var p

-}
{-
makePSVarsF :: (Typeable md, Typeable gq)
            => SB.StanBlock
            -> SB.RowTypeTag r
            -> SB.RowTypeTag r'
            -> SB.GroupTypeTag k
            -> ET.OfficeT
            -> SB.StanExpr
            -> SB.StanExpr
            -> SB.StanVar
            -> SB.StanBuilderM md gq (SB.StanVar, SB.StanVar)
makePSVarsF block rttPS rttElex grp office ptE ppE wgtsV = SB.inBlock block  $ do
  asVars <- SB.useDataSetForBindings rttPS
            $ SB.vectorizeExprT [("pT_" <> officeText office, ptE)
                                , ("pP_" <> officeText office, ppE)
                                , ("sWgts_" <> officeText office, SB.var wgtsV `SB.times` ptE)] (SB.dataSetName rttPS)

  (ptV, ppV, sWgtsV) <- case asVars of
    [x, y, z] -> return (x ,y, z)
    _ -> SB.stanBuildError "makePSVars: vectorizeExprT returned wrong number of vars!"
  pTByElex <- SB.postStratifiedParameterF False block (Just $ "ElexT_" <> officeText office <> "_ps") rttPS grp wgtsV ptV (Just rttElex)
  pSByElex <- SB.postStratifiedParameterF False block (Just $ "ElexS_" <> officeText office <> "_ps") rttPS grp sWgtsV ppV (Just rttElex)
  return (pTByElex, pSByElex)


makePSVars :: (Typeable md, Typeable gq)
           => SB.RowTypeTag r
           -> SB.RowTypeTag r'
           -> SB.GroupTypeTag k
           -> ET.OfficeT
           -> SB.StanExpr
           -> SB.StanExpr
           -> SB.StanVar
           -> SB.StanBuilderM md gq (SB.StanVar, SB.StanVar)
makePSVars rttPS rttElex grp office ptE ppE wgtsV = do
  let wgtsE = SB.var wgtsV `SB.times` ptE
  pTByElex <- SB.postStratifiedParameter False (Just $ "ElexT_" <> officeText office <> "_ps") rttPS grp (SB.var wgtsV) ptE (Just rttElex)
  pSByElex <- SB.postStratifiedParameter False (Just $ "ElexS_" <> officeText office <> "_ps") rttPS grp wgtsE ppE (Just rttElex)
  return (pTByElex, pSByElex)
-}

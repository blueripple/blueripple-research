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
      elexFilter x =  F.filterFrame (\r -> officeFilterF x r && zeroVoteFilterF r)
      loadElexData :: ET.OfficeT -> SB.StanGroupBuilderM CCESAndCPSEM (F.FrameRec rs) ()
      loadElexData office = case office of
          ET.President -> do
            elexData <- SB.addModelDataToGroupBuilder "Elections_Pres" (SB.ToFoldable $ elexFilter ET.President . stateElectionRows)
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
designMatrixRowPS' include densRP dmType incF = DM.DesignMatrixRow (show dmType) (dmSubset' dmType include all)
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
designMatrixRowCCES include densRP dmType incF = DM.DesignMatrixRow (show dmType) (dmSubset' dmType include all)
  where
    incRP = DM.rowPartFromFunctions "Incumbency" [incF]
    sexRP = DM.boundedEnumRowPart Nothing "Sex" (F.rgetField @DT.SexC)
    eduRP = DM.boundedEnumRowPart Nothing "Education" (F.rgetField @DT.CollegeGradC)
    raceRP = DM.boundedEnumRowPart (Just DT.R5_WhiteNonHispanic) "Race" (F.rgetField @DT.Race5C)
    wngRP = DM.boundedEnumRowPart Nothing "WhiteNonGrad" wnhNonGradCCES
    all = M.fromList[(DMDensity, densRP), (DMInc, incRP), (DMSex, sexRP), (DMEduc, eduRP), (DMRace, raceRP), (DMWNG, wngRP)]


designMatrixRowCPS :: Set DMComponents -> DM.DesignMatrixRowPart (F.Record CPSVWithDensityEM) -> DM.DesignMatrixRow (F.Record CPSVWithDensityEM)
designMatrixRowCPS include densRP = DM.DesignMatrixRow (show DMTurnout) $ dmSubset include all
 where
    sexRP = DM.boundedEnumRowPart Nothing "Sex" (F.rgetField @DT.SexC)
    eduRP = DM.boundedEnumRowPart Nothing "Education" (F.rgetField @DT.CollegeGradC)
    raceRP = DM.boundedEnumRowPart (Just DT.R5_WhiteNonHispanic) "Race" race5Census
    wngRP = DM.boundedEnumRowPart Nothing "WhiteNonGrad" wnhNonGradCensus
    all = M.fromList[(DMDensity, densRP), (DMSex, sexRP), (DMEduc, eduRP), (DMRace, raceRP), (DMWNG, wngRP)]

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

rescaleElectionData :: Int -> Int
rescaleElectionData x = x `div` 1000

setupElexTData :: (Typeable md, Typeable gq)
               => ET.OfficeT
               -> SB.StanBuilderM md gq (SB.RowTypeTag (F.Record StateElectionR)
                                        , SB.StanVar
                                        , SB.StanVar)
setupElexTData office = do
  let rescale x = x `div` 100
  elexTData <- SB.dataSetTag @(F.Record StateElectionR) SC.ModelData ("Elections_" <> show office)
  cvapElex <- SB.addCountData elexTData ("CVAP_Elections_" <> show office) (rescale . F.rgetField @PUMS.Citizens)
  votedElex <- SB.addCountData elexTData ("Voted_Elections" <> show office) (rescale . F.rgetField @TVotes)
  return (elexTData, cvapElex, votedElex)

setupElexSData :: (Typeable md, Typeable gq)
               => ET.OfficeT
               -> ET.VoteShareType
               -> SB.StanBuilderM md gq (SB.RowTypeTag (F.Record StateElectionR)
                                        , SB.StanVar
                                        , SB.StanVar)
setupElexSData office vst = do
  let rescale x = x `div` 100
  elexSData <- SB.dataSetTag @(F.Record StateElectionR) SC.ModelData ("ElectionsS_" <> show office)
  votesInRace <- SB.addCountData elexSData ("VotesInRace_Elections_" <> show office)
                 $ case vst of
                     ET.TwoPartyShare -> (\r -> rescale $ F.rgetField @DVotes r + F.rgetField @RVotes r)
                     ET.FullShare -> rescale . F.rgetField @TVotes
  dVotesInRace <- SB.addCountData elexSData ("DVotesInRace_Elections_" <> show office) (rescale . F.rgetField @DVotes)
  return (elexSData, votesInRace, dVotesInRace)


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
colIndex m =  case m of
  (SB.StanVar _ (SB.StanMatrix (_, SB.NamedDim ik))) -> return ik
  (SB.StanVar x _) -> SB.stanBuildError $ "colIndex: " <> x <> " is not a matrix with named column index"

ixM :: SB.StanName -> DataSetAlpha -> SB.StanBuilderM md gq (Maybe SB.StanVar)
ixM varName ds = case ds of
  NoDataSetAlpha -> return Nothing
  DataSetAlpha -> Just <$> SMP.addParameter varName SB.StanReal "" (SB.UnVectorized SB.stdNormal)

centerIf :: SB.StanVar -> Maybe (SC.InputDataType -> SB.StanVar -> Maybe Text -> SB.StanBuilderM md gq SB.StanVar)
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

indexedMuE :: SB.StanVar -> SB.StanBuilderM md gq (Maybe SB.StanVar -> SB.StanVar -> SB.StanVar -> SB.StanExpr)
indexedMuE dm = do
  muE' <- muE dm
  let f ixM alpha beta = case ixM of
        Nothing -> muE' (SB.var alpha) (SB.var beta)
        Just ixV -> muE' (SB.var ixV `SB.plus` SB.var alpha) (SB.var beta)
  return f

modelVar :: SB.RowTypeTag r -> SB.StanDist SB.StanExpr -> SB.StanVar -> SB.StanBuilderM md gq SB.StanExpr -> SB.StanBuilderM md gq ()
modelVar rtt dist y eM = do
  let f e = SB.sampleDistV rtt dist e y
  SB.inBlock SB.SBModel
    $ SB.useDataSetForBindings rtt
    $ (eM >>= f)

updateLLSet ::  SB.RowTypeTag r -> SB.StanDist SB.StanExpr -> SB.StanVar -> SB.StanBuilderM md gq SB.StanExpr -> SB.LLSet md gq -> SB.LLSet md gq
updateLLSet rtt dist y eM llSet =  SB.addToLLSet rtt llDetails llSet where
  llDetails = SB.LLDetails dist eM y


addPosteriorPredictiveCheck :: Text -> SB.RowTypeTag r -> SB.StanDist SB.StanExpr -> SB.StanBuilderM md gq SB.StanExpr ->  SB.StanBuilderM md gq ()
addPosteriorPredictiveCheck ppVarName rtt dist eM = do
  let pp = SB.StanVar ppVarName (SB.StanVector $ SB.NamedDim $ SB.dataSetName rtt)
  eM >>= SB.useDataSetForBindings rtt
    . SB.generatePosteriorPrediction rtt pp dist
  pure ()

addBLModelForDataSet :: (Typeable md, Typeable gq)
                     => Text
                     -> Bool
                     -> SB.StanBuilderM md gq (SB.RowTypeTag r, DM.DesignMatrixRow r, SB.StanVar, SB.StanVar, SB.StanVar)
                     -> DataSetAlpha
                     -> Maybe (SC.InputDataType -> SB.StanVar -> Maybe Text -> SB.StanBuilderM md gq SB.StanVar)
                     -> SB.StanVar
                     -> SB.StanVar
                     -> SB.LLSet md gq
                     -> SB.StanBuilderM md gq (SC.InputDataType -> SB.StanVar -> Maybe Text -> SB.StanBuilderM md gq SB.StanVar, SB.LLSet md gq)
addBLModelForDataSet dataSetLabel includePP dataSetupM dataSetAlpha centerM alpha beta llSet = do
  let addLabel x = x <> "_" <> dataSetLabel
  (rtt, designMatrixRow, counts, successes, dm) <- dataSetupM
  dsIxM <- ixM (addLabel "ix") dataSetAlpha
  (dmC, centerF) <- centerIf dm centerM
  muT <- indexedMuE dmC
  let dist = SB.binomialLogitDist counts
      vecMu = SB.vectorizeExpr (addLabel "mu") (muT dsIxM alpha beta) (SB.dataSetName rtt)
  modelVar rtt dist successes (SB.var <$> vecMu)
  let llSet' = updateLLSet rtt dist successes (pure $ muT dsIxM alpha beta) llSet
  when includePP $ addPosteriorPredictiveCheck (addLabel "PP") rtt dist (pure $ muT dsIxM alpha beta)
  return (centerF, llSet')

addBLModelsForElex :: (Typeable md, Typeable gq)
                   => Bool
                   -> ET.OfficeT
                   -> SB.StanBuilderM md gq (SB.RowTypeTag r', SB.StanVar, SB.StanVar, SB.StanVar, SB.StanVar)
                   -> Maybe (SC.InputDataType -> SB.StanVar -> Maybe Text -> SB.StanBuilderM md gq SB.StanVar)
                   -> Maybe (SC.InputDataType -> SB.StanVar -> Maybe Text -> SB.StanBuilderM md gq SB.StanVar)
                   -> (SB.RowTypeTag r, SB.StanVar, SB.StanVar, SB.StanVar) -- ps data-set, wgt var, design-matrixes (turnout, share)
                   -> SB.StanVar
                   -> SB.StanVar
                   -> SB.StanVar
                   -> SB.StanVar
                   -> SB.LLSet md gq
                   -> SB.StanBuilderM md gq (SC.InputDataType -> SB.StanVar -> Maybe Text -> SB.StanBuilderM md gq SB.StanVar
                                             , SC.InputDataType -> SB.StanVar -> Maybe Text -> SB.StanBuilderM md gq SB.StanVar
                                             , SB.LLSet md gq
                                             )
addBLModelForPrElex includePP office dataSetupM centerTM centerSM (rttPS, wgtsV, dmPST, dmPSP) alphaT betaT alphaP betaP llSet = do
  let officeText = case office of
                     ET.President -> "Pr"
                     ET.Senate -> "Se"
                     ET.House -> "Ho"
      addLabel x = x <> "_Elex_" <> officeText
  (rttElex, cvap, votes, votesInRace, dVotesInRace) <- dataSetupM
{-
  rttElex <- SB.dataSetTag @(F.Record StateElectionR) SC.ModelData ("Elections_President")
  cvap <- SB.addCountData elexTData "CVAP_Elections_Pr" (rescaleElectionData . F.rgetField @PUMS.Citizens)
  votes <- SB.addCountData elexTData "Voted_Elections_Pr" (rescaleElectionData . F.rgetField @TVotes)
  votesInRace <- SB.addCountData elexData ("VotesInRace_Elections_Pr")
                 $ case vst of
                     ET.TwoPartyShare -> (\r -> rescale $ F.rgetField @DVotes r + F.rgetField @RVotes r)
                     ET.FullShare -> rescale . F.rgetField @TVotes
  dVotesInRace <- SB.addCountData elexSData ("DVotesInRace_Elections_Pr") (rescale . F.rgetField @DVotes)
-}
  presShareIx <- ixM (addLabel "ixS") DataSetAlpha
  colIndexT <- colIndex dmPST
  colIndexP <- colIndex dmPSP
  (dmTC, centerTF) <- centerIf centerTM dmPST
  (dmPC, centerPF) <- centerIf centerSM dmPSP
  muT <- indexedMuE dmTC
  muP <- indexedMuE dmTP
  let ptE = SB.vectorizedOne colIndexT $ SB.function "inv_logit" (one $ muT Nothing (SB.var alphaT) (SB.var betaT))
      ppE = SB.vectorizedOne colIndexP $ SB.function "inv_logit" (one $ muP presShareIx (SB.var alphaP) (SB.var betaP))
      sWgtsE = SB.var wgtsV `SB.times` ptE
  pTByElex <- SB.postStratifiedParameter False (Just $ "ElexT_Pr_ps") rttPS stateGroup (SB.var wgtsV) ptE (Just rttElex)
  pSByElex <- SB.postStratifiedParameter False (Just $ "ElexS_Pr_ps") rttPS stateGroup sWgtsE ppE (Just rttElexS)
  let distT = SB.binomialDist' True cvap
      distS = SB.binomialDist' True votesInRace
  modelVar rttElex distT votes (SB.var <$> pTByElex)
  modelVar rttElex distS dVotesInRace (SB.var <$> pSByElex)
  let llSet' = updateLLSet rttElex distT votes (pure $ SB.var pTByElex)
        $ updateLLSet rttElex distS dVotesInRace (pure $ SB.var pSByElex)
  when includePP $ do
    addPosteriorPredictiveCheck "PP_Election_Pr_Votes" rttElex distT (pure $ SB.var pTByElex)
    addPosteriorPredictiveCheck "PP_Election_Pr_DvotesInRace" rttElex distS (pure $ SB.var pSByElex)
  return (centerTF, centerPF, llSet')


{-
addBLModelForElexT :: (Typeable md, Typeable gq)
                   => Bool
                   -> ET.OfficeT
                   -> Maybe (SC.InputDataType -> SB.StanVar -> Maybe Text -> SB.StanBuilderM md gq SB.StanVar)
                   -> SB.GroupTypeTag k
                   -> (SB.RowTypeTag r, SB.StanVar, SB.StanVar) -- ps data-set, wgt var, design-matrix
                   -> SB.StanVar
                   -> SB.StanVar
                   -> SB.LLSet md gq
                   -> SB.StanBuilderM md gq (SC.InputDataType -> SB.StanVar -> Maybe Text -> SB.StanBuilderM md gq SB.StanVar, SB.LLSet md gq)
addBLModelForElexT includePP office centerM groupByGTT (rttPS, wgtsV, dmPS) alphaT betaT llSet = do
  let addLabel x = x <> "_ElexT_" <> show office
  (rttElexT, cvap, votes) <- setupElexTData office
  dmColIndex <- case dmPS of
    (SB.StanVar _ (SB.StanMatrix (_, SB.NamedDim ik))) -> return ik
    (SB.StanVar m _) -> SB.stanBuildError $ "addModelForData: dm is not a matrix with named row index"
  (dmC, centerF) <- case centerM of
    Nothing -> DM.centerDataMatrix dmPS Nothing
    Just f -> do
      dmC' <- f SC.ModelData dmPS Nothing --(Just dataSetLabel)
      return (dmC', f)
  let dmBetaE dmE betaE = SB.vectorizedOne dmColIndex $ SB.function "dot_product" (dmE :| [betaE])
      muE aE dmE betaE = aE `SB.plus` dmBetaE dmE betaE
      ptE = SB.function "inv_logit" (one $ muE (SB.var alphaT) (SB.var dmC) (SB.var betaT))
      wgtsE = SB.var wgtsV
  pTByElex <- SB.postStratifiedParameter False (Just $ "ElexT_" <> show office <> "_ps") rttPS groupByGTT wgtsE ptE (Just rttElexT)
  let dist = SB.binomialDist' True cvap
  SB.inBlock SB.SBModel $ SB.sampleDistV rttElexT dist (SB.var pTByElex) votes
  let llDetails =  SB.LLDetails dist (pure $ SB.var pTByElex) votes
      llSet' = SB.addToLLSet rttElexT llDetails llSet
      pp = SB.StanVar (addLabel "PP") (SB.StanVector $ SB.NamedDim $ SB.dataSetName rttElexT)
  when includePP $ do
    SB.useDataSetForBindings rttElexT
      $ SB.generatePosteriorPrediction rttElexT pp dist (SB.var pTByElex)
    pure ()
  return (centerF, llSet')




addBLModelForElexS :: (Typeable md, Typeable gq)
                   => Bool
                   -> ET.OfficeT
                   -> ET.VoteShareType
                   -> Maybe (SC.InputDataType -> SB.StanVar -> Maybe Text -> SB.StanBuilderM md gq SB.StanVar)
                   -> SB.GroupTypeTag k
                   -> (SB.RowTypeTag r, SB.StanVar, SB.StanVar, SB.StanVar)
                   -> DataSetAlpha
                   -> SB.StanVar
                   -> SB.StanVar
                   -> SB.StanVar
                   -> SB.StanVar
                   -> SB.LLSet md gq
                   -> SB.StanBuilderM md gq (SC.InputDataType -> SB.StanVar -> Maybe Text -> SB.StanBuilderM md gq SB.StanVar, SB.LLSet md gq)
addBLModelForElexS includePP office vst centerM groupByGTT (rttPS, wgtsV, dmTC, dmS) dataSetAlpha alphaT betaT alphaP betaP llSet = do
  let addLabel x = x <> "_ElexS_" <> show office
  (rttElexS, votesInRace, dVotesInRace) <- setupElexSData office vst
  dmTColIndex <- case dmTC of
    (SB.StanVar _ (SB.StanMatrix (_, SB.NamedDim ik))) -> return ik
    (SB.StanVar m _) -> SB.stanBuildError $ "addBLModelForElexS: dmPST is not a matrix with named row index"
  dmSColIndex <- case dmS of
    (SB.StanVar _ (SB.StanMatrix (_, SB.NamedDim ik))) -> return ik
    (SB.StanVar m _) -> SB.stanBuildError $ "addBLModelForElexS: dmPSS is not a matrix with named row index"
  (dmSC, centerF) <- case centerM of
    Nothing -> DM.centerDataMatrix dmS Nothing
    Just f -> do
      dmC' <- f SC.ModelData dmS Nothing --(Just dataSetLabel)
      return (dmC', f)
  dsIxM <-  case dataSetAlpha of
              NoDataSetAlpha -> return Nothing
              DataSetAlpha -> do
                ix <- SMP.addParameter (addLabel "ix") SB.StanReal "" (SB.UnVectorized SB.stdNormal)
                return $ Just ix
  let
      dmBetaE dmE betaE = SB.function "dot_product" (dmE :| [betaE])
      muE ixM aE dmE betaE = case ixM of
        Nothing -> aE `SB.plus` dmBetaE dmE betaE
        Just ix -> SB.var ix `SB.plus` aE `SB.plus`dmBetaE dmE betaE
      ptE = SB.vectorizedOne dmTColIndex $ SB.function "inv_logit" (one $ muE Nothing (SB.var alphaT) (SB.var dmTC) (SB.var betaT))
      ppE = SB.vectorizedOne dmSColIndex $ SB.function "inv_logit" (one $ muE dsIxM (SB.var alphaP) (SB.var dmSC) (SB.var betaP))
      wgtsE = SB.var wgtsV `SB.times` ptE
  pSByElex <- SB.postStratifiedParameter False (Just $ "ElexS_" <> show office <> "_ps") rttPS groupByGTT wgtsE ppE (Just rttElexS)
  let dist = SB.binomialDist' True votesInRace
  SB.inBlock SB.SBModel $ SB.sampleDistV rttElexS dist (SB.var pSByElex) dVotesInRace
  let llDetails =  SB.LLDetails dist (pure $ SB.var pSByElex) dVotesInRace
      llSet' = SB.addToLLSet rttElexS llDetails llSet
      pp = SB.StanVar (addLabel "PP") (SB.StanVector $ SB.NamedDim $ SB.dataSetName rttElexS)
  when includePP $ do
    SB.useDataSetForBindings rttElexS
      $ SB.generatePosteriorPrediction rttElexS pp dist (SB.var pSByElex)
    pure ()
  return (centerF, llSet')


-}

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
      officesNamePart :: Text = mconcat $ fmap (T.take 1 . show) $ Set.toList $ votesFrom model
      modelName = "LegDistricts_" <> modelLabel model <> "_" <> officesNamePart
      jsonDataName = "DM_" <> dataLabel model <> "_Inc_" <> officesNamePart <> "_" <> show datYear
      psDataSetName' = psDataSetName
                       <> "_"  <> printDensityTransform (densityTransform model)
                       <> "_" <> officesNamePart
                       <> "_" <> printComponents (modelComponents model)
      dataAndCodeBuilder :: MRP.BuilderM CCESAndCPSEM (F.FrameRec rs) ()
      dataAndCodeBuilder = do
        let (dmColIndexT, dmColExprT) = DM.designMatrixColDimBinding $ designMatrixRowCCES compInclude densityMatrixRowPart DMTurnout (const 0)
            centerMatrices = False
            initialCenterFM = if centerMatrices then Nothing else (Just $ \_ v _ -> pure v)
            meanTurnout = 0.6
            logit x = Numeric.log (x / (1 - x))
            logitMeanTurnout = logit meanTurnout
            normal m sd = SB.normal (Just $ SB.scalar $ show m) (SB.scalar $ show sd)
            cauchy m s = SB.cauchy (Just $ SB.scalar $ show m) (SB.scalar $ show s)
        elexTData <- SB.dataSetTag @(F.Record StateElectionR) SC.ModelData "ElectionsT"
        alphaT <- SB.useDataSetForBindings elexTData $ do
          muAlphaT <- SMP.addParameter "muAlphaT" SB.StanReal "" (SB.UnVectorized $ normal logitMeanTurnout 1)
          sigmaAlphaT <- SMP.addParameter "sigmaAlphaT" SB.StanReal "<lower=0>"  (SB.UnVectorized $ normal 0 0.4)
          alphaTNonCenterF <- SMP.scalarNonCenteredF muAlphaT sigmaAlphaT
          SMP.addHierarchicalScalar "alphaT" stateGroup (SMP.NonCentered alphaTNonCenterF) $ normal 0 1
--          SMP.addHierarchicalScalar "alphaT" stateGroup SMP.Centered $ SB.normal (Just $ SB.var muAlphaT)  (SB.var sigmaAlphaT)
--          pure muAlphaT
        thetaT <- SB.useDataSetForBindings elexTData $ do
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
        (acsRowTag, acsPSWgts, acsDMT, acsDMP) <- setupACSPSRows compInclude densityMatrixRowPart dmPrefType (const 0) -- incF but for which office?
        (centerTF, llSetT1) <- addBLModelForElexT includePP ET.President initialCenterFM stateGroup (acsRowTag, acsPSWgts, acsDMT) alphaT thetaT SB.emptyLLSet
        (_, llSetT2) <- addBLModelForDataSet "CCEST" includePP (setupCCESTData compInclude densityMatrixRowPart) DataSetAlpha (Just centerTF)  alphaT thetaT llSetT1
        (_, llSetT) <- addBLModelForDataSet "CPST" includePP (setupCPSData compInclude densityMatrixRowPart) DataSetAlpha (Just centerTF) alphaT thetaT llSetT2

        elexPData <- SB.dataSetTag @(F.Record StateElectionR) SC.ModelData "ElectionsS"
        let (dmColIndexP, dmColExprP) = DM.designMatrixColDimBinding $ designMatrixRowCCES compInclude densityMatrixRowPart dmPrefType (const 0)
        alphaP <- SB.useDataSetForBindings elexPData $ do
          muAlphaP <- SMP.addParameter "muAlphaP" SB.StanReal "" (SB.UnVectorized $ normal 0 1)
          sigmaAlphaP <- SMP.addParameter "sigmaAlphaP" SB.StanReal "<lower=0>"  (SB.UnVectorized $ normal 0 0.4)
          alphaPNonCenterF <- SMP.scalarNonCenteredF muAlphaP sigmaAlphaP
          SMP.addHierarchicalScalar "alphaP" stateGroup (SMP.NonCentered alphaPNonCenterF) (normal 0 1)
--          SMP.addHierarchicalScalar "alphaP" stateGroup SMP.Centered $ SB.normal (Just $ SB.var muAlphaP)  (SB.var sigmaAlphaP)
--          pure muAlphaP
        thetaP <- SB.useDataSetForBindings elexPData $ do
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
        acsDMTC <- centerTF SC.ModelData acsDMT (Just "T")
        (centerPF, llSetP1) <- addBLModelForElexS includePP ET.President (voteShareType model)
                               initialCenterFM stateGroup (acsRowTag, acsPSWgts, acsDMTC, acsDMP)
                               NoDataSetAlpha alphaT thetaT alphaP thetaP SB.emptyLLSet
        let ccesP (centerFM, llS) office = do
              (centerF, llS) <- addBLModelForDataSet
                                ("CCESP" <> show office)
                                includePP
                                (setupCCESPData compInclude densityMatrixRowPart dmPrefType incF office (voteShareType model))
                                DataSetAlpha
                                centerFM
                                alphaP
                                thetaP
                                llS
              return (Just centerF, llS)
            llFoldM = FL.FoldM ccesP (return (Just centerPF, llSetP1)) return
        (_, llSetP) <- FL.foldM llFoldM (votesFrom model)

        SB.generateLogLikelihood' $ SB.mergeLLSets llSetT llSetP

        let  dmThetaE ci dmE thetaE = SB.vectorizedOne ci $ SB.function "dot_product" (dmE :| [thetaE])
             probE ci aE dmE thetaE = SB.function "inv_logit" $ one $ aE `SB.plus` dmThetaE ci dmE thetaE
             prob ci alpha dm theta =  probE ci (SB.var alpha) (SB.var dm) (SB.var theta)
             probT dm = prob dmColIndexT alphaT dm thetaT
             probP dm = prob dmColIndexP alphaP dm thetaP
        psData <- SB.dataSetTag @(F.Record rs) SC.GQData "PSData"
        dmPS_T' <- DM.addDesignMatrix psData (designMatrixRowPS compInclude densityMatrixRowPart DMTurnout)
        dmPS_P' <- DM.addDesignMatrix psData (designMatrixRowPS compInclude densityMatrixRowPart dmPrefType)
        dmPS_T <- centerTF SC.GQData dmPS_T' (Just "T")
        dmPS_P <- centerPF SC.GQData dmPS_P' (Just "P")
        let psTPrecompute dm dat  = SB.vectorizeExpr "pT" (probT dm) (SB.dataSetName dat)
            psTExpr :: SB.StanVar -> SB.StanBuilderM md gq SB.StanExpr
            psTExpr p =  pure $ SB.var p
            psPPrecompute dm dat = SB.vectorizeExpr "pD" (probP dm) (SB.dataSetName dat)
            psPExpr :: SB.StanVar -> SB.StanBuilderM md gq SB.StanExpr
            psPExpr p =  pure $ SB.var p
            psDVotePreCompute dmT dmP dat = (,) <$> psTPrecompute dmT dat <*> psPPrecompute dmP dat
            psDVoteExpr :: (SB.StanVar, SB.StanVar) -> SB.StanBuilderM md gq SB.StanExpr
            psDVoteExpr (pT, pD) = pure $ SB.var pT `SB.times` SB.var pD
            turnoutPS = ((psTPrecompute dmPS_T psData, psTExpr), Nothing)
            prefPS = ((psPPrecompute dmPS_P psData, psPExpr), Nothing)
            dVotePS = ((psDVotePreCompute dmPS_T dmPS_P psData, psDVoteExpr), Just $ SB.var . fst)
            postStratify :: (Typeable md, Typeable gq, Ord k)
                         => Text
                         -> ((SB.StanBuilderM md gq x, x -> SB.StanBuilderM md gq SB.StanExpr), Maybe (x -> SB.StanExpr))
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

{-
type GetCCESVotesC rs = (F.ElemOf rs HouseVotes
                        , F.ElemOf rs AHHouseDVotes
                        , F.ElemOf rs AHHouseRVotes
                        , F.ElemOf rs PresVotes
                        , F.ElemOf rs AHPresDVotes
                        , F.ElemOf rs AHPresRVotes
                        )

getCCESVotes :: GetCCCESVotesC rs
             => ET.OfficeT -> ET.VoteShareType -> (F.Record rs -> Double, F.Record rs -> Double)
getCCESVotes ET.House ET.FullShare = (realToFrac . F.rgetField @HouseVotes, F.rgetField @AHHouseDVotes)
getCCESVotes ET.House ET.TwoPartyShare = (\r -> F.rgetField @AHHouseDVotes r + F.rgetField @AHHouseRVotes r, F.rgetField @AHHouseDVotes)
getCCESVotes ET.President ET.FullShare = (realToFrac . F.rgetField @PresVotes, F.rgetField @AHPresDVotes)
getCCESVotes ET.President ET.TwoPartyShare = (\r ->  F.rgetField @AHPresDVotes r + F.rgetField @AHPresRVotes r, F.rgetField @AHPresDVotes)
getCCESVotes _ _ = (const 0, const 0) -- we shouldn't call this

-}
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

data Model = Model { voteShareType :: ET.VoteShareType
                   , votesFrom :: Set ET.OfficeT
                   , densityTransform :: DensityTransform
                   , modelComponents :: Set DMComponents
                   , betaType :: BetaType
                   }  deriving (Generic)

modelLabel :: Model  -> Text
modelLabel m = show (voteShareType m)
               <> "_" <> T.intercalate "_" (show <$> Set.toList (votesFrom m))
               <> "_" <> printDensityTransform (densityTransform m)
               <> "_" <> printComponents (modelComponents m)
               <> "_HierAlpha" <> if betaType m == HierarchicalBeta then "Beta" else ""

dataLabel :: Model -> Text
dataLabel = modelLabel

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

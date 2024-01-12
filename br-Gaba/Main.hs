{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE QuantifiedConstraints #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UnicodeSyntax #-}
{-# LANGUAGE StrictData #-}

module Main
  (main)
where

import qualified BlueRipple.Model.Election2.DataPrep as DP
import qualified BlueRipple.Model.Election2.ModelCommon as MC
import BlueRipple.Model.Election2.ModelCommon (ModelConfig)
import qualified BlueRipple.Model.Election2.ModelCommon2 as MC2
import qualified BlueRipple.Model.Election2.ModelRunner as MR
import qualified BlueRipple.Model.Demographic.DataPrep as DDP
import qualified BlueRipple.Model.Demographic.EnrichCensus as DMC
import qualified BlueRipple.Model.Demographic.TableProducts as DTP
import qualified BlueRipple.Data.Loaders as BRL
import qualified BlueRipple.Model.CategorizeElection as CE

import qualified BlueRipple.Configuration as BR
import qualified BlueRipple.Utilities.KnitUtils as BRK
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.GeographicTypes as GT
import qualified BlueRipple.Data.ElectionTypes as ET
import qualified BlueRipple.Data.ModelingTypes as MT
import qualified BlueRipple.Data.CCES as CCES
import qualified BlueRipple.Data.ACS_PUMS as ACS
import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Data.CensusLoaders as BRC
import qualified BlueRipple.Data.CensusTables as BRC
import qualified BlueRipple.Data.Loaders.Redistricting as BLR
import qualified BlueRipple.Data.DistrictOverlaps as DO
import qualified BlueRipple.Utilities.FramesUtils as BRF
import qualified BlueRipple.Utilities.KnitUtils as BR

import qualified Knit.Report as K
import qualified Knit.Effect.AtomicCache as KC
import qualified Text.Pandoc.Error as Pandoc
import qualified System.Console.CmdArgs as CmdArgs
import qualified Colonnade as C

import qualified Stan.ModelBuilder as SMB
import qualified Stan.ModelRunner as SMR
import qualified Stan.ModelBuilder.TypedExpressions.Types as TE
import qualified Stan.ModelBuilder.TypedExpressions.Statements as TE
import qualified Stan.Parameters as SP
import qualified Stan.ModelConfig as SC
import qualified Stan.RScriptBuilder as SR
import qualified Stan.ModelBuilder.BuildingBlocks as SBB
import qualified Stan.ModelBuilder.BuildingBlocks.GroupAlpha as SG
import qualified Stan.ModelBuilder.DesignMatrix as DM
import qualified CmdStan as CS

import qualified Frames as F
import qualified Frames.Melt as F
import qualified Frames.MapReduce as FMR
import qualified Frames.Transform as FT
import qualified Frames.SimpleJoins as FJ
import qualified Frames.Constraints as FC
import qualified Frames.Streamly.TH as FS
import qualified Frames.Streamly.InCore as FI
import qualified Frames.Streamly.CSV as FCSV
import qualified Frames.Streamly.TH as FTH
import qualified Frames.Streamly.OrMissing as FOM

--import qualified Frames.Streamly.Transform as FST

import Frames.Streamly.Streaming.Streamly (StreamlyStream, Stream)
import qualified Frames.Serialize as FS

import qualified Control.Foldl as FL
import qualified Control.Foldl.Statistics as FLS
import Control.Lens (view, (^.))

import qualified Flat
import qualified Data.IntMap.Strict as IM
import qualified Data.List as List
import qualified Data.Map.Strict as M
import qualified Data.Set as S
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V
import qualified Data.Vinyl.Functor as V

import qualified Text.Printf as PF

type OrMissingInt = FOM.OrMissing Int
type OrMissingDouble = FOM.OrMissing Double

FTH.declareColumn "CD" ''OrMissingInt
FTH.declareColumn "CDPPL" ''OrMissingDouble


templateVars ∷ Map String String
templateVars =
  M.fromList
    [ ("lang", "English")
    , ("site-title", "Blue Ripple Politics")
    , ("home-url", "https://www.blueripplepolitics.org")
    --  , ("author"   , T.unpack yamlAuthor)
    ]

pandocTemplate ∷ K.TemplatePath
pandocTemplate = K.FullySpecifiedTemplatePath "pandoc-templates/blueripple_basic.html"

dmr ::  DM.DesignMatrixRow (F.Record DP.LPredictorsR)
dmr = MC.tDesignMatrixRow_d

survey :: MC.TurnoutSurvey (F.Record DP.CESByCDR)
survey = MC.CESSurvey

aggregation :: MC.SurveyAggregation TE.ECVec
aggregation = MC.WeightedAggregation MC.ContinuousBinomial

alphaModel :: MC.Alphas
alphaModel = MC.St_A_S_E_R_AE_AR_ER_StR

type SLDKeyR = '[GT.StateAbbreviation] V.++ BRC.LDLocationR
type ModeledR = SLDKeyR V.++ '[MR.ModelCI]


main :: IO ()
main = do
  cmdLine ← CmdArgs.cmdArgsRun BR.commandLine
  pandocWriterConfig ←
    K.mkPandocWriterConfig
    pandocTemplate
    templateVars
    (BRK.brWriterOptionsF . K.mindocOptionsF)
  let cacheDir = ".flat-kh-cache"
      knitConfig ∷ K.KnitConfig BRK.SerializerC BRK.CacheData Text =
        (K.defaultKnitConfig $ Just cacheDir)
          { K.outerLogPrefix = Just "Gaba"
          , K.logIf = BR.knitLogSeverity $ BR.logLevel cmdLine -- K.logDiagnostic
          , K.pandocWriterConfig = pandocWriterConfig
          , K.serializeDict = BRK.flatSerializeDict
          , K.persistCache = KC.persistStrictByteString (\t → toString (cacheDir <> "/" <> t))
          }
  resE ← K.knitHtmls knitConfig $ do
    K.logLE K.Info $ "Command Line: " <> show cmdLine
    let postInfo = BR.PostInfo (BR.postStage cmdLine) (BR.PubTimes BR.Unpublished Nothing)
    allStatesL <- filter (\sa -> (sa `notElem` ["DC","NH"]))
                  . fmap (view GT.stateAbbreviation)
                  . filter ((< 60) . view GT.stateFIPS)
                  . FL.fold FL.list
                  <$> (K.ignoreCacheTimeM $ BRL.stateAbbrCrosswalkLoader)
    upperOnlyMap <- BRL.stateUpperOnlyMap
    singleCDMap <- BRL.stateSingleCDMap
    let  turnoutConfig = MC.TurnoutConfig survey (MC.ModelConfig aggregation alphaModel (contramap F.rcast dmr))
         prefConfig = MC.PrefConfig (MC.ModelConfig aggregation alphaModel (contramap F.rcast dmr))
         analyzeOne s = K.logLE K.Info ("Working on " <> s) >> analyzeState cmdLine turnoutConfig Nothing prefConfig Nothing upperOnlyMap singleCDMap s
    allStateAnalysis_C <- fmap (fmap mconcat . sequenceA) $ traverse analyzeOne allStatesL
    K.ignoreCacheTime allStateAnalysis_C >>= writeModeled "modeled" . fmap F.rcast

  case resE of
    Right namedDocs →
      K.writeAllPandocResultsWithInfoAsHtml "" namedDocs
    Left err → putTextLn $ "Pandoc Error: " <> Pandoc.renderError err

stateUpperOnlyMap :: (K.KnitEffects r, BRK.CacheEffects r) => K.Sem r (Map Text Bool)
stateUpperOnlyMap = FL.fold (FL.premap (\r -> (r ^. GT.stateAbbreviation, r ^. BR.sLDUpperOnly)) FL.map)
                    <$> K.ignoreCacheTimeM BRL.stateAbbrCrosswalkLoader


type AnalyzeStateR = (FJ.JoinResult [GT.StateAbbreviation, GT.DistrictTypeC, GT.DistrictName]
                      (SLDKeyR V.++ '[MR.ModelCI])
                      BLR.DRAnalysisR)
                     V.++ [CE.DistCategory, DO.Overlap, CD, CDPPL]

analyzeState :: (K.KnitEffects r, BRK.CacheEffects r)
             => BR.CommandLine
             -> MC.TurnoutConfig a b
             -> Maybe (MR.Scenario DP.PredictorsR)
             -> MC.PrefConfig b
             -> Maybe (MR.Scenario DP.PredictorsR)
             -> Map Text Bool
             -> Map Text Bool
             -> Text
             -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec AnalyzeStateR))
analyzeState cmdLine tc tScenarioM pc pScenarioM upperOnlyMap singleCDMap state = do
  let cacheStructure state' psName = MR.CacheStructure (Right "model/election2/stan/") (Right "model/election2")
                                    psName "AllCells" state'
  let psDataForState :: Text -> DP.PSData SLDKeyR -> DP.PSData SLDKeyR
      psDataForState sa = DP.PSData . F.filterFrame ((== sa) . view GT.stateAbbreviation) . DP.unPSData
      modelMid = MT.ciMid . view MR.modelCI
  modeledACSBySLDPSData_C <- modeledACSBySLD cmdLine
  let stateSLDs_C = fmap (psDataForState state) modeledACSBySLDPSData_C
  presidentialElections_C <- BRL.presidentialElectionsWithIncumbency
  draShareOverrides_C <- DP.loadOverrides "data/DRA_Shares/DRA_Share.csv" "DRA 2016-2021"
  let dVSPres2020 = DP.ElexTargetConfig "PresWO" draShareOverrides_C 2020 presidentialElections_C
      dVSModel psName
        = MR.runFullModelAH @SLDKeyR 2020 (cacheStructure state psName) cmdLine tc tScenarioM pc pScenarioM dVSPres2020
  modeled_C <- fmap modeledMapToFrame <$> dVSModel (state <> "_SLD") stateSLDs_C
  draSLD_C <- BLR.allPassedSLD 2024 BRC.TY2021
  draCD_C <- BLR.allPassedCongressional 2024 BRC.TY2021
  let deps = (,,) <$> modeled_C <*> draSLD_C <*> draCD_C
  BRK.retrieveOrMakeFrame ("gaba/" <> state <> "_analysis.bin") deps $ \(modeled, draSLD, draCD) -> do
    let draSLD_forState = F.filterFrame ((== state) . view GT.stateAbbreviation) draSLD
        draCDPPLMap = FL.fold (FL.premap (\r -> (r ^. GT.districtName, r ^. ET.demShare )) FL.map)
                      $ F.filterFrame ((== state) . view GT.stateAbbreviation) draCD
        (modeledAndDRA, missingModelDRA)
          = FJ.leftJoinWithMissing @[GT.StateAbbreviation, GT.DistrictTypeC, GT.DistrictName] modeled draSLD_forState
    when (not $ null missingModelDRA) $ K.knitError $ "br-Gaba: Missing keys in modeledDVs/dra join: " <> show missingModelDRA
    let (safe, lean, tilt) = (0.1, 0.05, 0.02)
        lrF = CE.leanRating safe lean tilt
        compText :: (FC.ElemsOf rs [ET.DemShare, MR.ModelCI]) => F.Record rs -> Text
        compText r =
          let lrPPL = lrF (r ^. ET.demShare)
              lrDPL = lrF $ modelMid r
          in CE.pPLAndDPL lrPPL lrDPL
        compareOn f x y = compare (f x) (f y)
        compareRows x y = compareOn (view GT.stateAbbreviation) x y
                          <> compareOn (view GT.districtTypeC) x y
                          <> GT.districtNameCompare (x ^. GT.districtName) (y ^. GT.districtName)
        competitivePPL r = let x = r ^. ET.demShare in x >= 0.4 && x <= 0.6
        sortAndFilter = F.toFrame . sortBy compareRows . filter competitivePPL . FL.fold FL.list
        withModelComparison = fmap (\r -> r F.<+> FT.recordSingleton @CE.DistCategory (compText r)) $ sortAndFilter modeledAndDRA
    maxOverlapsM <- fmap (>>= DO.maxSLD_CDOverlaps) $ DO.sldCDOverlaps upperOnlyMap singleCDMap 2024 BRC.TY2021 state
--    maxOverlaps <- K.knitMaybe "Failed to find max overlaps" maxOverlapsM
    case maxOverlapsM of
      Just maxOverlaps -> do
        let (withOverlaps, missingOverlaps)
              = FJ.leftJoinWithMissing @[GT.DistrictTypeC, GT.DistrictName] withModelComparison maxOverlaps
        when (not $ null missingOverlaps) $ K.knitError $ "br-Gaba: Missing keys in modeledDVs+dra/overlaps join: " <> show missingOverlaps
        let cd r = r ^. GT.congressionalDistrict
            omCD = FT.recordSingleton @CD . FOM.Present . cd
            omCDPPL r = FT.recordSingleton @CDPPL $ FOM.toOrMissing $ M.lookup (show $ cd r) draCDPPLMap
            addBoth r = FT.mutate (\r -> omCD r F.<+> omCDPPL r) r
        pure $ fmap (F.rcast . addBoth) withOverlaps
      Nothing -> do
        let overlapCols :: F.Record [DO.Overlap, CD, CDPPL] = 1 F.&: FOM.Missing F.&: FOM.Missing F.&: V.RNil
            withOverlaps = FT.mutate (const overlapCols) <$> withModelComparison
        pure withOverlaps


modeledMapToFrame :: MC.PSMap SLDKeyR MT.ConfidenceInterval -> F.FrameRec ModeledR
modeledMapToFrame = F.toFrame . fmap (\(k, ci) -> k F.<+> FT.recordSingleton @MR.ModelCI ci) . M.toList . MC.unPSMap

writeModeled :: (K.KnitEffects r)
             => Text
             -> F.FrameRec [GT.StateAbbreviation, GT.DistrictTypeC, GT.DistrictName,ET.DemShare, MR.ModelCI, CE.DistCategory, CD, DO.Overlap, CDPPL]
             -> K.Sem r ()
writeModeled csvName modeledEv = do
  let wText = FCSV.formatTextAsIs
      printNum n m = PF.printf ("%" <> show n <> "." <> show m <> "g")
      wPrintf' :: (V.KnownField t, V.Snd t ~ Double) => Int -> Int -> (Double -> Double) -> V.Lift (->) V.ElField (V.Const Text) t
      wPrintf' n m f = FCSV.liftFieldFormatter $ toText @String . printNum n m . f
      wDPL :: (V.KnownField t, V.Snd t ~ MT.ConfidenceInterval) => Int -> Int -> V.Lift (->) V.ElField (V.Const Text) t
      wDPL n m = FCSV.liftFieldFormatter
                 $ toText @String . \ci -> printNum n m (100 * MT.ciMid ci)
      wModeled :: (V.KnownField t, V.Snd t ~ MT.ConfidenceInterval) => Int -> Int -> V.Lift (->) V.ElField (V.Const Text) t
      wModeled n m = FCSV.liftFieldFormatter
                     $ toText @String . \ci -> printNum n m (100 * MT.ciMid ci) <> ","
      wOrMissing :: (V.KnownField t, V.Snd t ~ FOM.OrMissing a) => (a -> Text) -> Text -> V.Lift (->) V.ElField (V.Const Text) t
      wOrMissing ifPresent ifMissing =  FCSV.liftFieldFormatter $ \case
        FOM.Present a -> ifPresent a
        FOM.Missing -> ifMissing

      formatModeled = FCSV.formatTextAsIs
                       V.:& formatDistrictType
                       V.:& FCSV.formatTextAsIs
                       V.:& wPrintf' 2 0 (* 100)
                       V.:& wDPL 2 0
                       V.:& FCSV.formatTextAsIs
                       V.:& wOrMissing show "At Large"--FCSV.formatWithShow
                       V.:& wPrintf' 2 0 (* 100)
                       V.:& wOrMissing (toText @String . printNum 2 0 . (* 100)) "N/A" --wPrintf' 2 0 (* 100)
                       V.:& V.RNil
      newHeaderMap = M.fromList [("StateAbbreviation", "State")
                                , ("DistrictTypeC", "District Type")
                                , ("DistrictName", "District Name")
                                , ("DemShare", "Partisan Lean (Dave's Redistricting)")
                                , ("ModelCI", "Model (BlueRipple)")
                                , ("DistCategory", "BlueRipple Comment")
                                , ("CongressionalDistrict","CD")
                                , ("Overlap", "Overlap (%)")
                                , ("CongressionalPPL", "CD Partisan Lean (Dave's Redistricting)")
                                ]
  K.liftKnit @IO
    $ FCSV.writeLines (toString $ "../forGaba/" <> csvName <> ".csv")
    $ FCSV.streamSV' @_ @(StreamlyStream Stream) newHeaderMap formatModeled ","
    $ FCSV.foldableToStream modeledEv

formatDistrictType :: (V.KnownField t, V.Snd t ~ GT.DistrictType) => V.Lift (->) V.ElField (V.Const Text) t
formatDistrictType = FCSV.liftFieldFormatter $ \case
  GT.StateUpper -> "Upper House"
  GT.StateLower -> "Lower House"
  GT.Congressional -> "Error: Congressional District!"

modeledACSBySLD :: (K.KnitEffects r, BRK.CacheEffects r) => BR.CommandLine -> K.Sem r (K.ActionWithCacheTime r (DP.PSData SLDKeyR))
modeledACSBySLD cmdLine = do
  (jointFromMarginalPredictorCSR_ASR_C, _) <- DDP.cachedACSa5ByPUMA  ACS.acs1Yr2012_21 2021 -- most recent available
                                                 >>= DMC.predictorModel3 @'[DT.CitizenC] @'[DT.Age5C] @DMC.SRCA @DMC.SR
                                                 (Right "CSR_ASR_ByPUMA")
                                                 (Right "model/demographic/csr_asr_PUMA")
                                                 False -- use model not just mean
                                                 cmdLine Nothing Nothing . fmap (fmap F.rcast)
  (jointFromMarginalPredictorCASR_ASE_C, _) <- DDP.cachedACSa5ByPUMA ACS.acs1Yr2012_21 2021 -- most recent available
                                                  >>= DMC.predictorModel3 @[DT.CitizenC, DT.Race5C] @'[DT.Education4C] @DMC.ASCRE @DMC.AS
                                                  (Right "CASR_SER_ByPUMA")
                                                  (Right "model/demographic/casr_ase_PUMA")
                                                  False -- use model, not just mean
                                                  cmdLine Nothing Nothing . fmap (fmap F.rcast)
  (acsCASERBySLD, _products) <- BRC.censusTablesForSLDs 2024 BRC.TY2022
                                >>= DMC.predictedCensusCASER' (DTP.viaNearestOnSimplex) (Right "model/election2/sldDemographics")
                                jointFromMarginalPredictorCSR_ASR_C
                                jointFromMarginalPredictorCASR_ASE_C
  BRK.retrieveOrMakeD "model/election2/data/sld2024_ACS2022_PSData.bin" acsCASERBySLD
    $ \x -> DP.PSData . fmap F.rcast <$> (BRL.addStateAbbrUsingFIPS $ F.filterFrame ((== DT.Citizen) . view DT.citizenC) x)

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

import qualified BlueRipple.Configuration as BR
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.ElectionTypes as ET
import qualified BlueRipple.Data.GeographicTypes as GT
import qualified BlueRipple.Data.ModelingTypes as MT
import qualified BlueRipple.Data.ACS_PUMS as ACS
import qualified BlueRipple.Data.Loaders.Redistricting as DRA
import qualified BlueRipple.Data.DistrictOverlaps as DO
import qualified BlueRipple.Data.CensusLoaders as BRC
import qualified BlueRipple.Data.CensusTables as BRC
import qualified BlueRipple.Data.DataFrames as BRDF
--import qualified BlueRipple.Data.Keyed as Keyed
import qualified BlueRipple.Data.Loaders as BRL
import qualified BlueRipple.Utilities.TableUtils as BR
import qualified BlueRipple.Data.Visualizations.DemoCompChart as DCC
import qualified Text.Blaze.Html5.Attributes   as BHA

import qualified BlueRipple.Utilities.KnitUtils as BRK
import qualified BlueRipple.Model.Demographic.DataPrep as DDP
import qualified BlueRipple.Model.Election2.DataPrep as DP
import qualified BlueRipple.Model.Election2.ModelCommon as MC
--import qualified BlueRipple.Model.Election2.ModelCommon2 as MC2
import qualified BlueRipple.Model.Election2.ModelRunner as MR
import qualified BlueRipple.Model.Demographic.EnrichCensus as DMC
import qualified BlueRipple.Model.Demographic.DataPrep as CDDP
import qualified BlueRipple.Model.Demographic.TableProducts as DTP
import qualified BlueRipple.Model.CategorizeElection as CE

import qualified Knit.Report as K
import qualified Knit.Effect.AtomicCache as KC
import qualified Text.Pandoc.Error as Pandoc
import qualified System.Console.CmdArgs as CmdArgs
import qualified Colonnade as C

--import GHC.TypeLits (Symbol)
import qualified Control.Foldl as FL
import Control.Lens (view, (^.))

import qualified Frames as F
import qualified Data.Text as T
import qualified Text.Read as TR
import qualified Data.Discrimination.Grouping as Grouping
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V
import qualified Data.Vinyl.Class.Method as V
import qualified Data.Vinyl.Functor as V
import qualified Data.Vector as Vector
import qualified Data.Map.Merge.Strict as MM
import qualified Data.Set as Set
import qualified Data.Time.Calendar as Time
import qualified Frames.Melt as F
import qualified Frames.MapReduce as FMR
import qualified Frames.Folds as FF
import qualified Frames.Transform as FT
import qualified Frames.Constraints as FC
import qualified Frames.SimpleJoins as FJ
import qualified Frames.Streamly.InCore as FSI
import qualified Frames.Streamly.TH as FTH
import qualified Frames.Streamly.Transform as FST
import qualified Frames.Serialize as FS

import qualified Text.Blaze.Html as HTML

import qualified Frames.Serialize as FS

import Path (Dir, Rel)
import qualified Path
import qualified Numeric
import qualified Stan.ModelBuilder.DesignMatrix as DM
import qualified Stan.ModelBuilder.TypedExpressions.Types as TE

import qualified Data.Map.Strict as M

import qualified Text.Printf as PF
import qualified Graphics.Vega.VegaLite as GV
import qualified Graphics.Vega.VegaLite.Compat as FV
import qualified Graphics.Vega.VegaLite.Configuration as FV
import qualified Graphics.Vega.VegaLite.JSON as VJ

import GHC.TypeLits (Symbol)

import qualified ModelNotes as MN


--import qualified Data.Discrimination.Grouping  as G

--import Data.Monoid (Sum(getSum))
FTH.declareColumn "StateModelT" ''Double
FTH.declareColumn "StateModelP" ''Double
FTH.declareColumn "Share" ''Double
FTH.declareColumn "StateShare" ''Double
FTH.declareColumn "Overlap" ''Double
FTH.declareColumn "CongressionalPPL" ''Double


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

type SLDKeyR = '[GT.StateAbbreviation] V.++ BRC.LDLocationR
type ModeledR = SLDKeyR V.++ '[MR.ModelCI]

dlccVA = [(GT.StateLower,"41","Lily Franklin")
         , (GT.StateUpper,"31", "Russet Perry")
         , (GT.StateUpper, "24", "Monty Mason")
         , (GT.StateUpper, "30", "Danica Roem")
         , (GT.StateUpper, "22", "Aaron Rouse")
         , (GT.StateUpper, "16", "Schuyler VanValkenburg")
         , (GT.StateLower, "94", "Phil Hernandez")
         , (GT.StateLower, "84", "Nadarius Clark")
         , (GT.StateLower, "82", "Kimberly Pope Adams")
         , (GT.StateLower, "71", "Jessica Anderson")
         , (GT.StateLower, "65", "Joshua Cole")
         , (GT.StateLower, "58", "Rodney Willet")
         , (GT.StateLower, "57", "Susanna Gibson")
         , (GT.StateLower, "22", "Joshua Thomas")
         , (GT.StateLower, "89", "Karen Jenkins")
         , (GT.StateLower, "97", "Michael Feggans")
         ]

dlccNJ = [(GT.StateUpper, "38", "Joseph Lagana/Lisa Swain/Chris Tully")
         ,(GT.StateUpper, "11", "Vin Gopal/Margie Donlon/Luanne Peterpaul")
         ,(GT.StateUpper, "16", "Andrew Zwicker/Mitchelle Drulis/Roy Freiman")
         ]

dlccMS = [(GT.StateLower,"86","Annita Bonner")
          ,(GT.StateLower,"24","David Olds")
          ,(GT.StateUpper,"2","Pam McKelvy Hammer")
          ,(GT.StateUpper,"10","Andre DeBerry")
          , (GT.StateLower,"12","Donna Niewiaroski")
         ]

dlccNH = [(GT.StateLower,"CO1","Cathleen Fountain")
         ,(GT.StateLower,"CO6","Edith Tucker")
         ]

dlccMap = M.fromList [("LA",[]), ("MS",dlccMS), ("NJ", dlccNJ), ("VA", dlccVA)]

stateUpperOnlyMap :: (K.KnitEffects r, BRK.CacheEffects r) => K.Sem r (Map Text Bool)
stateUpperOnlyMap = FL.fold (FL.premap (\r -> (r ^. GT.stateAbbreviation, r ^. BRDF.sLDUpperOnly)) FL.map)
                    <$> K.ignoreCacheTimeM BRL.stateAbbrCrosswalkLoader

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
          { K.outerLogPrefix = Just "2023-StateLeg"
          , K.logIf = BR.knitLogSeverity $ BR.logLevel cmdLine -- K.logDiagnostic
          , K.pandocWriterConfig = pandocWriterConfig
          , K.serializeDict = BRK.flatSerializeDict
          , K.persistCache = KC.persistStrictByteString (\t → toString (cacheDir <> "/" <> t))
          }
  resE ← K.knitHtmls knitConfig $ do
    K.logLE K.Info $ "Command Line: " <> show cmdLine
--    modelNotesPost cmdLine

    let postInfo = BR.PostInfo (BR.postStage cmdLine) (BR.PubTimes BR.Unpublished Nothing)
        histCompetitive :: F.Record AnalyzeStateR -> Bool
        histCompetitive r = let x = r ^. ET.demShare in (x > 0.40 && x < 0.60)
        rural ::  F.Record AnalyzeStateR -> Bool
        rural r = r ^. DT.pWPopPerSqMile <= 100
    stateUpperOnlyM <- stateUpperOnlyMap
    let postsToDo = [("WI", histCompetitive)
                    , ("VA", histCompetitive)
                    , ("AZ", histCompetitive)
                    , ("KS", histCompetitive)
                    , ("MI", histCompetitive)
                    , ("MS", histCompetitive)
                    , ("NV", histCompetitive)
                    , ("KS", histCompetitive)
--                    , ("NH", histCompetitive)
                    , ("PA", histCompetitive)
                    ]
    traverse_ (uncurry $ analyzeStatePost cmdLine postInfo stateUpperOnlyM dlccMap) postsToDo

  case resE of
    Right namedDocs →
      K.writeAllPandocResultsWithInfoAsHtml "" namedDocs
    Left err → putTextLn $ "Pandoc Error: " <> Pandoc.renderError err

recDivide :: V.AllConstrained Fractional rs => V.Rec V.Identity rs -> V.Rec V.Identity rs -> V.Rec V.Identity rs
recDivide V.RNil V.RNil = V.RNil
recDivide (a V.:& as) (b V.:& bs) = V.Identity (V.getIdentity a / V.getIdentity b) V.:& recDivide as bs

recSubtract :: V.AllConstrained Fractional rs => V.Rec V.Identity rs -> V.Rec V.Identity rs -> V.Rec V.Identity rs
recSubtract V.RNil V.RNil = V.RNil
recSubtract (a V.:& as) (b V.:& bs) = V.Identity (V.getIdentity a - V.getIdentity b) V.:& recSubtract as bs

--type AnalyzeStateR = SLDKeyR V.++ '[MR.ModelCI, DRA.PlanName] V.++ F.RecordColumns DRA.DRAnalysisRaw V.++ DP.SummaryR
type AnalyzeStateR = FJ.JoinResult3 [GT.StateAbbreviation, GT.DistrictTypeC, GT.DistrictName]
                     (SLDKeyR V.++ '[MR.ModelCI])
                     DRA.DRAnalysisR
                     ([GT.StateAbbreviation, GT.DistrictTypeC, GT.DistrictName] V.++ DP.SummaryR)

analyzeState :: (K.KnitEffects r, BRK.CacheEffects r)
             => BR.CommandLine
             → MC.TurnoutConfig a b
             -> Maybe (MR.Scenario DP.PredictorsR)
             → MC.PrefConfig b
             -> Maybe (MR.Scenario DP.PredictorsR)
             -> Map Text Bool
             -> Map Text [(GT.DistrictType, Text, Text)]
             -> Text
             -> K.Sem r (F.FrameRec AnalyzeStateR)
analyzeState cmdLine tc tScenarioM pc pScenarioM stateUpperOnlyMap dlccMap state = do
  let cacheStructure state' psName = MR.CacheStructure (Right "model/election2/stan/") (Right "model/election2")
                                    psName "AllCells" state'
  let psDataForState :: Text -> DP.PSData SLDKeyR -> DP.PSData SLDKeyR
      psDataForState sa = DP.PSData . F.filterFrame ((== sa) . view GT.stateAbbreviation) . DP.unPSData

  modeledACSBySLDPSData_C <- modeledACSBySLD cmdLine
  let stateSLDs_C = fmap (psDataForState state) modeledACSBySLDPSData_C
  sldDemographicSummary <- FL.fold (DP.summarizeASER_Fld @[GT.StateAbbreviation, GT.DistrictTypeC, GT.DistrictName]) . DP.unPSData <$> K.ignoreCacheTime stateSLDs_C
  presidentialElections_C <- BRL.presidentialElectionsWithIncumbency
  houseElections_C <- BRL.houseElectionsWithIncumbency
  draShareOverrides_C <- DP.loadOverrides "data/DRA_Shares/DRA_Share.csv" "DRA 2016-2021"
  let dVSPres2020 = DP.ElexTargetConfig "PresWO" draShareOverrides_C 2020 presidentialElections_C
      dVSHouse2022 = DP.ElexTargetConfig "HouseWO" draShareOverrides_C 2022 houseElections_C
      dVSModel psName
        = MR.runFullModelAH @SLDKeyR 2020 (cacheStructure state psName) cmdLine tc tScenarioM pc pScenarioM dVSPres2020
  modeledDVSMap <- K.ignoreCacheTimeM $ dVSModel (state <> "_SLD") stateSLDs_C
  allPlansMap <- DRA.allPassedSLDPlans
  upperOnly <- K.knitMaybe ("analyzeStatePost: " <> state <> " missing from stateUpperOnlyMap") $ M.lookup state stateUpperOnlyMap
  let modeledDVs = modeledMapToFrame modeledDVSMap
  dra <- do
    upper <- K.ignoreCacheTimeM $ DRA.lookupAndLoadRedistrictingPlanAnalysis allPlansMap (DRA.redistrictingPlanId state "Passed" GT.StateUpper)
    if upperOnly then pure upper
      else (do
               lower <- K.ignoreCacheTimeM $ DRA.lookupAndLoadRedistrictingPlanAnalysis allPlansMap (DRA.redistrictingPlanId state "Passed" GT.StateLower)
               pure $ upper <> lower
           )
  let (modeledAndDRA, missingModelDRA, missingSummary)
        = FJ.leftJoin3WithMissing @[GT.StateAbbreviation, GT.DistrictTypeC, GT.DistrictName] modeledDVs dra sldDemographicSummary
  when (not $ null missingModelDRA) $ K.knitError $ "br-2023-StateLeg: Missing keys in modeledDVs/dra join: " <> show missingModelDRA
  when (not $ null missingSummary) $ K.knitError $ "br-2023-StateLeg: Missing keys in modeledDVs+dra/sumamry join: " <> show missingSummary
  pure modeledAndDRA


-- for each SLD, provide the CD, if any, with overlap over 50%, along with the PPL of that CD and the Overlap
sldCDOverlaps :: (K.KnitEffects r, BRK.CacheEffects r) =>  Map Text Bool -> Text
              -> K.Sem r (F.FrameRec [GT.DistrictTypeC, GT.DistrictName, Overlap, GT.CongressionalDistrict, CongressionalPPL])
sldCDOverlaps stateUpperOnlyMap sa = do
  upperOnly <- K.knitMaybe ("sldCDOverlaps: " <> sa <> " missing from stateUpperOnlyMap") $ M.lookup sa stateUpperOnlyMap
  allSLDPlansMap <- DRA.allPassedSLDPlans
  allCDPlansMap <- DRA.allPassedCongressionalPlans
{-  draSLD <- do
    upper <- K.ignoreCacheTimeM $ DRA.lookupAndLoadRedistrictingPlanAnalysis allSLDPlansMap (DRA.redistrictingPlanId sa "Passed" GT.StateUpper)
    if upperOnly then pure upper
      else (do
               lower <- K.ignoreCacheTimeM $ DRA.lookupAndLoadRedistrictingPlanAnalysis allSLDPlansMap (DRA.redistrictingPlanId sa "Passed" GT.StateLower)
               pure $ upper <> lower
           )
-}
  draCD <- K.ignoreCacheTimeM $ DRA.lookupAndLoadRedistrictingPlanAnalysis allCDPlansMap (DRA.redistrictingPlanId sa "Passed" GT.Congressional)
  let cdRow r = not $ (r ^. GT.districtName) `elem` ["Summary", "Un"]
--      sLDs = F.filterFrame nonSummary draSLD
      cDs = F.filterFrame cdRow draCD
  overlapsL ← DO.overlapCollection (Set.singleton sa) (\x → toString $ "data/districtOverlaps/" <> x <> "_SLDL" <> "_CD.csv") GT.StateLower GT.Congressional
  overlapsU ← DO.overlapCollection (Set.singleton sa) (\x → toString $ "data/districtOverlaps/" <> x <> "_SLDU" <> "_CD.csv") GT.StateUpper GT.Congressional
  let overlappingSLDs' cdn overlaps' = do
        let
          pwo = Vector.zip (DO.populations overlaps') (DO.overlaps overlaps')
          nameByRow = fmap fst $ sortOn snd $ M.toList $ DO.rowByName overlaps'
          intDiv x y = realToFrac x / realToFrac y
          olFracM (pop, ols) = fmap (\x → intDiv x pop) $ M.lookup cdn ols
          olFracsM = traverse olFracM pwo
        olFracs ← K.knitMaybe ("sldCDOverlaps: Missing CD in overlap data for " <> cdn) olFracsM
        let namedOlFracs = zip nameByRow (Vector.toList olFracs)
        pure namedOlFracs

      overlappingSLDs cdn = do
        overlapsU ← K.knitMaybe ("sldCDOverlap: Failed to find overlap data for upper chamber of " <> sa) $ M.lookup sa overlapsU
        namedOverU ← overlappingSLDs' cdn overlapsU
        let upperRes =  fmap (\(n, x) → (GT.StateUpper, n, x)) namedOverU
        case upperOnly of
          True -> pure upperRes
          False -> do
            overlapsL ← K.knitMaybe ("sldCDOverlap: Failed to find overlap data for lower chamber of " <> sa) $ M.lookup sa overlapsL
            namedOverL ← overlappingSLDs' cdn overlapsL
            pure $ upperRes <> fmap (\(n, x) → (GT.StateLower, n, x)) namedOverL
      stAbbr = view GT.stateAbbreviation
      dName = view GT.districtName
      sld r = (r ^. GT.districtTypeC, dName r)
--      compSLDSet = FL.fold (FL.premap sld FL.set) competitiveSLDs
--      isCompetitive (dt, dn, _) = (dt, dn) `Set.member` compSLDSet
      f r = fmap (\ols -> (dName r, r ^. ET.demShare, ols)) $ overlappingSLDs (dName r)
  overlapsByCD ← traverse f $ FL.fold FL.list cDs
  let toRecInner :: (GT.DistrictType, Text, Double) -> F.Record [GT.DistrictTypeC, GT.DistrictName, Overlap]
      toRecInner (dt, dn, ol) = dt F.&: dn F.&: ol F.&: V.RNil
      toRec :: Double -> F.Record '[GT.CongressionalDistrict] -> F.Record [GT.DistrictTypeC, GT.DistrictName, Overlap]
            -> F.Record [GT.DistrictTypeC, GT.DistrictName, Overlap, GT.CongressionalDistrict, CongressionalPPL]
      toRec cdPPL cdRec ri = ri F.<+> cdRec F.<+> FT.recordSingleton @CongressionalPPL cdPPL
      toRecs :: (Text, Double, [(GT.DistrictType, Text, Double)])
             -> Either Text [F.Record [GT.DistrictTypeC, GT.DistrictName, Overlap, GT.CongressionalDistrict, CongressionalPPL]]
      toRecs (cdT, cdPPL, slds) = fmap (\cdRec ->  fmap (toRec cdPPL cdRec) $ fmap toRecInner slds) $ cdNameToCDRec cdT
  F.toFrame . mconcat <$> (K.knitEither $ traverse toRecs overlapsByCD)

cdNameToCDRec :: Text -> Either Text (F.Record '[GT.CongressionalDistrict])
cdNameToCDRec t =
  fmap (FT.recordSingleton @GT.CongressionalDistrict)
  $ maybe (Left $ "cdNameTOCDRec: Failed to parse " <> t <> " as an Int") Right
  $ TR.readMaybe $ toString t

rbStyle :: (PF.PrintfArg a, RealFrac a) => Text -> a -> (HTML.Html, Text)
rbStyle = BR.numberToStyledHtmlFull False (BR.cellStyle BR.CellBackground . BR.PartColor . BR.numColorHiGrayLo 0.7 30 70 10 240)

bordered :: Text -> BR.CellStyles
bordered c = BR.solidBorderedCell c 3

mwoColonnade :: forall rs . (FC.ElemsOf rs [GT.DistrictTypeC, GT.DistrictName, ET.DemShare, GT.CongressionalDistrict, Overlap, CongressionalPPL])
                     => BR.CellStyleF (F.Record rs) [Char] -> C.Colonnade C.Headed (F.Record rs) K.Cell
mwoColonnade cas =
  let state = F.rgetField @GT.StateAbbreviation
      competitive r = let x = r ^. congressionalPPL in x >= 0.45 && x <= 0.55
      cas' = cas <> BR.filledCell "DarkSeaGreen" `BR.cellStyleIf` \r h -> (h == "District") && competitive r
      olStyle = BR.numberToStyledHtmlFull False (BR.cellStyle BR.CellBackground . BR.PartColor . BR.numColorWhiteUntil 50 100 300)--BR.numberToStyledHtml' False "black" 75 "green"
  in C.headed "State District" (BR.toCell cas' "District" "District" (BR.textToStyledHtml . fullDNameText))
     <> C.headed "State District PPL" (BR.toCell cas' "D. PPL" "D. PPL" (rbStyle "%2.1f" . (100*) . view ET.demShare))
     <> C.headed "CD" (BR.toCell cas' "CD" "CD" (BR.textToStyledHtml . show . view GT.congressionalDistrict))
     <> C.headed "CD PPL" (BR.toCell cas' "CD PPL" "CD PPL" (rbStyle "%2.1f" . (100*) . view congressionalPPL))
     <> C.headed "Overlap" (BR.toCell cas' "Overlap" "Overlap" (BR.numberToStyledHtml "%2.0f" . (100*) . view overlap))

dmr ::  DM.DesignMatrixRow (F.Record DP.LPredictorsR)
dmr = MC.tDesignMatrixRow_d

survey :: MC.TurnoutSurvey (F.Record DP.CESByCDR)
survey = MC.CESSurvey

aggregation :: MC.SurveyAggregation TE.ECVec
aggregation = MC.WeightedAggregation MC.ContinuousBinomial

alphaModel :: MC.Alphas
alphaModel =  MC.St_A_S_E_R_AE_AR_ER_StR --MC.St_A_S_E_R_ER_StR_StER
              --      alphaModel =  MC.St_A_S_E_R_ER_StR_StER
psDataForState :: Text -> DP.PSData SLDKeyR -> DP.PSData SLDKeyR
psDataForState sa = DP.PSData . F.filterFrame ((== sa) . view GT.stateAbbreviation) . DP.unPSData

modelNotesPost :: (K.KnitMany r, BRK.CacheEffects r)
               => BR.CommandLine
               -> K.Sem r ()
modelNotesPost cmdLine = do
  modelNotesPostPaths <- postPaths "ModelNotes" cmdLine
  let postInfo = BR.PostInfo (BR.postStage cmdLine)
                 (BR.PubTimes (BR.Published $ Time.fromGregorian 2023 12 6) Nothing)
  BRK.brNewPost modelNotesPostPaths postInfo "ModelNotes" $ do
    let  turnoutConfig agg am = MC.TurnoutConfig survey (MC.ModelConfig agg am (contramap F.rcast dmr))
         prefConfig agg am = MC.PrefConfig (MC.ModelConfig agg am (contramap F.rcast dmr))
         lowerOnly r = r ^. GT.districtTypeC == GT.StateLower
--         upperOnly r = r ^. GT.districtTypeC == GT.StateUpper
         dName = view GT.districtName
         byDistrict r1 r2 = compare (r1 ^. GT.districtTypeC) (r2 ^. GT.districtTypeC)
                            <> GT.districtNameCompare (r1 ^. GT.districtName) (r2 ^. GT.districtName)
         scenarioByDistrict (r1, _) (r2, _) = byDistrict r1 r2

    upperOnlyMap <- stateUpperOnlyMap
    modeledAndDRA_VA <- analyzeState cmdLine (turnoutConfig aggregation alphaModel) Nothing (prefConfig aggregation alphaModel) Nothing upperOnlyMap dlccMap "VA"
    BRK.brAddMarkDown MN.part1
    let ppl = view ET.demShare
    geoCompChart modelNotesPostPaths postInfo ("VA_geoPPL") "VA Past Partisan Lean"
      (FV.fixedSizeVC 400 250 10) "VA" GT.StateLower dName ("PPL", (* 100) . ppl, Just "redblue", Just (0, 100))
      (F.filterFrame lowerOnly modeledAndDRA_VA)
      >>= K.addHvega Nothing Nothing
    BRK.brAddMarkDown MN.part2
    let dpl r = MT.ciMid $ r ^. MR.modelCI
    geoCompChart modelNotesPostPaths postInfo ("VA_geoDPL") "VA Demographic Partisan Lean"
      (FV.fixedSizeVC 400 200 10) "VA" GT.StateLower dName ("DPL", (* 100) . dpl, Just "redblue", Just (0, 100))
      (F.filterFrame lowerOnly modeledAndDRA_VA)
      >>= K.addHvega Nothing Nothing
    BRK.brAddMarkDown MN.part3
    let delta r = 100 * (dpl r -  ppl r)
    geoCompChart modelNotesPostPaths postInfo ("VA_geoDelta") "VA: DPL-PPL"
      (FV.fixedSizeVC 400 150 10) "VA" GT.StateLower dName ("DPL-PPL", delta, Just "redblue", Just (-50, 50))
      (F.filterFrame lowerOnly modeledAndDRA_VA)
      >>= K.addHvega Nothing Nothing
    BRK.brAddMarkDown MN.part3b
    let flippable r = ppl r < 0.45 && dpl r >= 0.50
        vulnerable r = ppl r > 0.55 && dpl r <= 0.5
        compColonnade = distCompColonnade mempty {- $ leansCellStyle "PPL" ppl <> leansCellStyle "DPL" dpl -}
    BR.brAddRawHtmlTable (Just "Flippable/Vulnerable ?") (BHA.class_ "brTable") compColonnade
      $ F.filterFrame (\r -> lowerOnly r && (flippable r || vulnerable r)) modeledAndDRA_VA
    BRK.brAddMarkDown MN.part4
    let dobbsTurnoutF r = if (r ^. DT.sexC == DT.Female) then MR.adjustP 0.05 else id
        dobbsTurnoutS = MR.SimpleScenario "DobbsT" dobbsTurnoutF
        dobbsPrefS = MR.SimpleScenario "DobbsP" dobbsTurnoutF
        youthBoostF r = if (r ^. DT.age5C <= DT.A5_25To34) then MR.adjustP 0.05 else id
        youthBoostTurnoutS = MR.SimpleScenario "YouthBoost" youthBoostF
    modeledAndDRA_VA_Dobbs <- analyzeState cmdLine (turnoutConfig aggregation alphaModel) (Just dobbsTurnoutS)
                              (prefConfig aggregation alphaModel) (Just dobbsPrefS) upperOnlyMap dlccMap "VA"
    let sortScByClose = sortOn (abs . (+ negate 0.5) . view ET.demShare . fst . snd)
        sortScByPPL = sortOn (view ET.demShare . fst . snd)
        mergeKey :: F.Record AnalyzeStateR -> F.Record [GT.StateAbbreviation, GT.DistrictTypeC, GT.DistrictName]
        mergeKey = F.rcast
        mergeVal r1 r2 = (F.rcast @[ET.DemShare, MR.ModelCI] r1, MT.ciMid (r1 ^. MR.modelCI) - MT.ciMid (r2 ^. MR.modelCI))
    mergedDobbs <- K.knitEither $ mergeAnalyses mergeKey mergeVal modeledAndDRA_VA_Dobbs modeledAndDRA_VA
    let closeForTable n = sortScByPPL . take n . sortScByClose
                          . filter ((== GT.StateLower) . view GT.districtTypeC . fst) .  M.toList
        scPpl = view ET.demShare . fst . snd
        scenarioDelta = snd . snd
        pplPlus x = scPpl x + scenarioDelta x
    let scenarioColonnade = scenarioCompColonnade mempty -- $ leansCellStyle "PPL" scPpl <> leansCellStyle "+Scenario" pplPlus
    BR.brAddRawHtmlTable (Just "Dobbs Scenario") (BHA.class_ "brTable") scenarioColonnade $ sortBy scenarioByDistrict $ closeForTable 20 mergedDobbs
    BRK.brAddMarkDown MN.part4b
    let dobbs2F r = if (r ^. DT.sexC == DT.Female && r ^. DT.education4C == DT.E4_CollegeGrad) then MR.adjustP 0.05 else id
        dobbsTurnout2S = MR.SimpleScenario "Dobbs2T" dobbs2F
        dobbsPref2S = MR.SimpleScenario "Dobbs2P" dobbs2F
    modeledAndDRA_VA_Dobbs2 <- analyzeState cmdLine (turnoutConfig aggregation alphaModel) (Just dobbsTurnout2S)
                               (prefConfig aggregation alphaModel) (Just dobbsPref2S) upperOnlyMap dlccMap "VA"
    mergedDobbs2 <- K.knitEither $ mergeAnalyses mergeKey mergeVal modeledAndDRA_VA_Dobbs2 modeledAndDRA_VA
    BR.brAddRawHtmlTable (Just "Dobbs Scenario") (BHA.class_ "brTable") scenarioColonnade $ sortBy scenarioByDistrict $ closeForTable 20 mergedDobbs2
    BRK.brAddMarkDown MN.part4c
    modeledAndDRA_VA_YouthBoost <- analyzeState cmdLine (turnoutConfig aggregation alphaModel) (Just youthBoostTurnoutS)
                                   (prefConfig aggregation alphaModel) Nothing upperOnlyMap dlccMap "VA"
    mergedYouthBoost <- K.knitEither $ mergeAnalyses mergeKey mergeVal modeledAndDRA_VA_YouthBoost modeledAndDRA_VA
    BR.brAddRawHtmlTable (Just "Youth Enthusiasm Scenario") (BHA.class_ "brTable") scenarioColonnade $ sortBy scenarioByDistrict $ closeForTable 20 mergedYouthBoost
-- geographic overlaps
    BRK.brAddMarkDown MN.part5
    modeledAndDRA_WI <- analyzeState cmdLine (turnoutConfig aggregation alphaModel) Nothing (prefConfig aggregation alphaModel) Nothing upperOnlyMap dlccMap "WI"
    overlaps_WI <- sldCDOverlaps upperOnlyMap "WI"
    let (modeledWOverlaps, mwoMissing) = FJ.leftJoinWithMissing @[GT.DistrictTypeC, GT.DistrictName] modeledAndDRA_WI overlaps_WI
    when (not $ null mwoMissing) $ K.knitError $ "modelNotesPost: missing overlaps in model+DRA/overlap join: " <> show mwoMissing
    let interestingOverlap r =
          let c x = x >= 0.45 && x <= 0.55
          in c (r ^. ET.demShare) && (r ^. overlap > 0.5)
    BR.brAddRawHtmlTable (Just "WI SLD/CD Overlaps") (BHA.class_ "brTable") (mwoColonnade mempty)
      $ sortBy byDistrict $ FL.fold FL.list $ F.filterFrame interestingOverlap modeledWOverlaps
    BRK.brAddMarkDown MN.part6
    pure ()


leanRating :: Double -> CE.LeanRating
leanRating = CE.leanRating 0.10 0.05 0.01

distCompColonnade :: forall rs . (FC.ElemsOf rs [GT.StateAbbreviation, GT.DistrictTypeC, GT.DistrictName, MR.ModelCI, ET.DemShare])
                     => BR.CellStyleF (F.Record rs) [Char] -> C.Colonnade C.Headed (F.Record rs) K.Cell
distCompColonnade cas =
  let state = F.rgetField @GT.StateAbbreviation
      ppl = view ET.demShare
      dpl = MT.ciMid . view MR.modelCI
  in C.headed "State" (BR.toCell cas "State" "State" (BR.textToStyledHtml . state))
     <> C.headed "District" (BR.toCell cas "District" "District" (BR.textToStyledHtml . fullDNameText))
     <> C.headed "PPL" (BR.toCell cas "PPL" "PPL" (rbStyle "%2.1f" . (100*) . ppl ))
     <> C.headed "DPL" (BR.toCell cas "DPL" "DPL" (rbStyle "%2.1f" . (100*) . dpl))
     <> C.headed "Assessment" (BR.toCell cas "Type" "Type" (\r -> BR.textToStyledHtml $ CE.pPLAndDPL (leanRating $ ppl r) (leanRating $ dpl r)))

mergeAnalyses :: (Show k, Ord k)
              => (F.Record AnalyzeStateR -> k)
              -> (F.Record AnalyzeStateR -> F.Record AnalyzeStateR -> a)
              -> F.FrameRec AnalyzeStateR
              -> F.FrameRec AnalyzeStateR
              -> Either Text (Map k a)
mergeAnalyses key mergeData f1 f2 =
  let keyVal r = (key r, r)
      m1 = FL.fold (FL.premap keyVal FL.map) f1
      m2 = FL.fold (FL.premap keyVal FL.map) f2
      whenMatched = MM.zipWithAMatched (\_ r1 r2 -> Right $ mergeData r1 r2)
      whenMissing t = MM.traverseMissing (\k _ -> Left $ "mergeAnalyses: " <> t <> " missing key=" <> show k)
  in MM.mergeA (whenMissing "RHS") (whenMissing "LHS") whenMatched m1 m2

scenarioCompColonnade :: forall rs qs . (FC.ElemsOf rs [GT.StateAbbreviation, GT.DistrictTypeC, GT.DistrictName]
                                      , FC.ElemsOf qs [ET.DemShare, MR.ModelCI]
                                     )
                      => BR.CellStyleF (F.Record rs, (F.Record qs, Double)) [Char]
                      -> C.Colonnade C.Headed (F.Record rs, (F.Record qs, Double)) K.Cell
scenarioCompColonnade cas =
  let state = F.rgetField @GT.StateAbbreviation
      hpl = view ET.demShare . fst . snd
      scenarioDelta = snd . snd
      hplPlus x = hpl x + scenarioDelta x
      assessmentText r = CE.ratingChangeText (leanRating $ hpl r) (leanRating $ hplPlus r)
      cas' = cas <> BR.filledCell "DarkSeaGreen" `BR.cellStyleIf` \r h -> (h == "Change") && (assessmentText r /= "No Change")
  in C.headed "State" (BR.toCell cas "State" "State" (BR.textToStyledHtml . state . fst))
     <> C.headed "District" (BR.toCell cas "District" "District" (BR.textToStyledHtml . fullDNameText . fst))
     <> C.headed "PPL" (BR.toCell cas "PPL" "PPL" (rbStyle "%2.1f" . (100*) . hpl ))
--     <> C.headed "DPL" (BR.toCell cas "DPL" "DPL" (BR.numberToStyledHtml "%2.1f" . (100*) . MT.ciMid . view MR.modelCI . fst . snd))
     <> C.headed "PPL + Scenario" (BR.toCell cas "+Scenario" "+Scenario" (rbStyle "%2.1f" . (100*) . hplPlus))
     <> C.headed "Rating Change?" (BR.toCell cas' "Change" "Change" (BR.textToStyledHtml . assessmentText))

analyzeStatePost :: (K.KnitMany r, BRK.CacheEffects r)
                 => BR.CommandLine
                 -> BR.PostInfo
                 -> Map Text Bool
                 -> Map Text [(GT.DistrictType, Text, Text)]
                 -> Text
                 -> (F.Record AnalyzeStateR -> Bool)
                 -> K.Sem r ()
analyzeStatePost cmdLine postInfo stateUpperOnlyMap dlccMap state include = do
  modelPostPaths <- postPaths state cmdLine
  let cacheStructure state psName = MR.CacheStructure (Right "model/election2/stan/") (Right "model/election2")
                                    psName "AllCells" state

  BRK.brNewPost modelPostPaths postInfo state $ do
    let  turnoutConfig agg am = MC.TurnoutConfig survey (MC.ModelConfig agg am (contramap F.rcast dmr))
         prefConfig agg am = MC.PrefConfig (MC.ModelConfig agg am (contramap F.rcast dmr))
    modeledAndDRA <- analyzeState cmdLine (turnoutConfig aggregation alphaModel) Nothing (prefConfig aggregation alphaModel) Nothing stateUpperOnlyMap dlccMap state
    upperOnly <- K.knitMaybe ("analyzeStatePost: " <> state <> " missing from stateUpperOnlyMap") $ M.lookup state stateUpperOnlyMap

    modeledACSBySLDPSData_C <- modeledACSBySLD cmdLine
    let stateSLDs_C = fmap (psDataForState state) modeledACSBySLDPSData_C
    let toLogDensity = FT.fieldEndo @DT.PWPopPerSqMile (\x -> if x > 1 then Numeric.log x else 0)
        summaryLD = fmap toLogDensity modeledAndDRA
        (summaryMeans, summarySDs) = FL.fold DP.summaryMeanStd $ fmap F.rcast summaryLD
        f :: F.Record DP.SummaryR -> F.Record DP.SummaryR
        f r = F.withNames $ (r'  `recSubtract` m') `recDivide` s' where
          r' = F.stripNames r
          m' = F.stripNames summaryMeans
          s' = F.stripNames summarySDs

        normalizeSummary r = F.rcast @[GT.StateAbbreviation, GT.DistrictTypeC, GT.DistrictName, ET.DemShare, MR.ModelCI] r
                             F.<+> f (F.rcast @DP.SummaryR r)
        csSummary = fmap normalizeSummary summaryLD

    compChart <- modelDRAComparisonChart modelPostPaths postInfo (state <> "comp") (state <> ": model vs historical (DRA)")
                 (FV.fixedSizeVC 500 500 10) modeledAndDRA
    _ <- K.addHvega Nothing Nothing compChart
    let delta r = 100 * ((MT.ciMid $ r ^. MR.modelCI) -  r ^. ET.demShare)
        detailChamber = if upperOnly then GT.StateUpper else GT.StateLower
        cOnly c r  = r ^. GT.districtTypeC == c
        uOnly = cOnly GT.StateUpper
        lOnly = cOnly GT.StateLower
        dOnly = cOnly detailChamber
        dName = view GT.districtName
--        chamberName uo dt =
    when (not upperOnly) $ do
      void $ geoCompChart modelPostPaths postInfo (state <> "_geoDeltaL") (state <> ": model - historical")
        (FV.fixedSizeVC 500 300 10) state GT.StateLower dName ("delta", delta, Nothing, Nothing)
        (F.filterFrame lOnly modeledAndDRA)
        >>= K.addHvega Nothing Nothing
    geoCompChart modelPostPaths postInfo (state <> "_geoDeltaU") (state <> ": model - historical")
      (FV.fixedSizeVC 500 300 10) state GT.StateUpper dName ("delta", delta, Nothing, Nothing)
      (F.filterFrame uOnly modeledAndDRA)
      >>= K.addHvega Nothing Nothing
    geoCompChart modelPostPaths postInfo (state <> "_geoDensityL") (state <> ": log density")
      (FV.fixedSizeVC 500 300 10) state detailChamber dName ("log density", Numeric.log . view DT.pWPopPerSqMile, Nothing, Nothing)
      (F.filterFrame dOnly modeledAndDRA)
      >>= K.addHvega Nothing Nothing
    geoCompChart modelPostPaths postInfo (state <> "_geoAgeL") (state <> ": % Over 45")
      (FV.fixedSizeVC 500 300 10) state detailChamber dName ("Over 45 (%)", \r -> 100 * (r ^. DP.frac45To64 + r ^. DP.frac65plus), Nothing, Nothing)
      (F.filterFrame dOnly modeledAndDRA)
      >>= K.addHvega Nothing Nothing
    geoCompChart modelPostPaths postInfo (state <> "_geoCGradL") (state <> ": % College Grad")
      (FV.fixedSizeVC 500 300 10) state detailChamber dName ("College Grad (%)", \r -> 100 * (r ^. DP.fracCollegeGrad), Nothing, Nothing)
      (F.filterFrame dOnly modeledAndDRA)
      >>= K.addHvega Nothing Nothing
    let draCompetitive r = let x = r ^. ET.demShare in (x > 0.40 && x < 0.60)
    BR.brAddRawHtmlTable (Just $ state <> " model (2020 data): Upper House") (BHA.class_ "brTable") (sldColonnade mempty {- $ sldTableCellStyle state -})
      $ sortBy byDistrictName $ FL.fold FL.list
      $ F.filterFrame include
      $ F.filterFrame ((== GT.StateUpper) . view GT.districtTypeC) modeledAndDRA
    when (not upperOnly)
      $ BR.brAddRawHtmlTable (Just $ state <> " model (2020 data): Lower House") (BHA.class_ "brTable") (sldColonnade mempty {-$ sldTableCellStyle state -})
      $ sortBy byDistrictName $ FL.fold FL.list
      $ F.filterFrame include
      $ F.filterFrame ((== GT.StateLower) . view GT.districtTypeC) modeledAndDRA
    let tc = turnoutConfig aggregation alphaModel
        pc = prefConfig aggregation alphaModel
        distsForCompare = FL.fold (FL.prefilter include $ FL.premap (\r -> (r ^. GT.districtTypeC, r ^. GT.districtName)) FL.set) modeledAndDRA
    allDistrictDetails @'[DT.Race5C] cmdLine modelPostPaths postInfo
      (MR.modelCacheStructure $ cacheStructure state "") tc pc (Just distsForCompare) (show . view DT.race5C) "ByRace" state stateSLDs_C csSummary
    pure ()
  pure ()

type DistSummaryR = [DT.PWPopPerSqMile, DP.Frac45AndOver, DP.FracBlack, DP.FracWhite, DP.FracHispanic, DP.FracAAPI, DP.FracCollegeGrad, DP.FracGradOfWhite]

--type DistDetailsPSKeyR = SLDKeyR V.++


--type SLDKeyAnd as = SLDKeyR V.++ as
type SLDKeyAnd2 as bs = SLDKeyR V.++ as V.++ bs
type AndSLDKey as = as V.++ SLDKeyR
type AndSLDKey2 as bs = as V.++ SLDKeyR V.++ bs

type ModelCompR = [MR.ModelT, MR.ModelP, Share, StateModelT, StateModelP, StateShare, DT.PopCount, DT.PWPopPerSqMile]

data TotalOrGroup g = Total | Group g

textTorG :: (g -> Text) -> TotalOrGroup g -> Text
textTorG _ Total = "Total"
textTorG f (Group g) = f g

showTorG :: Show g => TotalOrGroup g -> Text
showTorG = textTorG show

type TotalOrGroupC g = "TotalOrGroup" F.:-> TotalOrGroup g

allDistrictDetails
    ∷ ∀ (ks :: [(Symbol, Type)]) r rs a b
     . ( K.KnitOne r
       , BRK.CacheEffects r
       , FC.ElemsOf rs [GT.StateAbbreviation, GT.DistrictTypeC, GT.DistrictName, MR.ModelCI, ET.DemShare]
       , FC.ElemsOf rs DistSummaryR
       , FSI.RecVec rs
       , Ord (F.Record ks)
       , Typeable ks
       , Show (F.Record ks)
       , FS.RecFlat ks
       , V.RMap ks
       , Grouping.Grouping (F.Record ks)
       , FS.RecFlat (ks V.++ ModelCompR)
       , V.RMap (ks V.++ ModelCompR)
       , FSI.RecVec (ks V.++ ModelCompR)
       , CombineDistrictPSMapsC ks
       , MR.PSTypeC ks SLDKeyR '[MR.ModelPr]
       , MR.PSTypeC ks SLDKeyR '[MR.ModelPr, MR.ModelT]
       , MR.PSTypeC (AndSLDKey ks) SLDKeyR '[MR.ModelPr]
       , MR.PSTypeC (AndSLDKey ks) SLDKeyR '[MR.ModelPr, MR.ModelT]
       , FC.ElemsOf (ks V.++ ModelCompR) ModelCompR
       , ks F.⊆ (SLDKeyR V.++ ks V.++ ModelCompR)
       )
    ⇒ BR.CommandLine
    → BR.PostPaths Path.Abs
    → BR.PostInfo
    -> MR.CacheStructure () ()
    → MC.TurnoutConfig a b
    → MC.PrefConfig b
    → Maybe (Set (GT.DistrictType, Text))
    -> (F.Record ks -> Text)
    -> Text
    → Text
    → K.ActionWithCacheTime r (DP.PSData SLDKeyR)
    → F.FrameRec rs
    → K.Sem r ()
allDistrictDetails cmdLine pp pi cacheStructure' tc pc districtsM catText cacheSuffix state psData_C summaryRows = do
    draShareOverrides_C ← DP.loadOverrides "data/DRA_Shares/DRA_Share.csv" "DRA 2016-2021"
    presidentialElections_C ← BRL.presidentialElectionsWithIncumbency
    houseElections_C ← BRL.houseElectionsWithIncumbency
    let cacheStructureState psSuffix = MR.CacheStructure (MR.csModelDirE cacheStructure') (MR.csProjectCacheDirE cacheStructure')
                                       (state <> "_" <> psSuffix) "AllCells" state
    let cacheStructureSLD psSuffix = MR.CacheStructure (MR.csModelDirE cacheStructure') (MR.csProjectCacheDirE cacheStructure')
                                       (state <> "_SLD_" <> psSuffix) "AllCells" state
        dVSPres2020 = DP.ElexTargetConfig "PresWO" draShareOverrides_C 2020 presidentialElections_C
        dVSHouse2022 = DP.ElexTargetConfig "HouseWO" draShareOverrides_C 2022 houseElections_C
    stateTurnoutByCat_C ← MR.runTurnoutModelAH @ks 2020 (cacheStructureState cacheSuffix) cmdLine tc Nothing psData_C
    statePrefByCat_C :: K.ActionWithCacheTime r (MC.PSMap ks MT.ConfidenceInterval) ←
      MR.runPrefModelAH @ks 2020 (cacheStructureState cacheSuffix) cmdLine tc Nothing pc Nothing dVSPres2020  psData_C
    districtTurnoutByCat_C :: K.ActionWithCacheTime r (MC.PSMap (AndSLDKey ks) MT.ConfidenceInterval) ←
      MR.runTurnoutModelAH @(AndSLDKey ks) 2020 (cacheStructureSLD cacheSuffix) cmdLine tc Nothing psData_C
    districtPrefByCat_C ← MR.runPrefModelAH @(AndSLDKey ks) 2020 (cacheStructureSLD cacheSuffix) cmdLine tc Nothing pc Nothing dVSPres2020  psData_C
--    K.ignoreCacheTime stateTurnoutByCat_C >>= K.logLE K.Info . show . MC.unPSMap
--    K.ignoreCacheTime statePrefByCat_C >>= K.logLE K.Info . show . MC.unPSMap
    let comboByCatDeps = (,,,,) <$> stateTurnoutByCat_C <*> districtTurnoutByCat_C <*> statePrefByCat_C <*> districtPrefByCat_C <*> psData_C
    comboByCatCacheKey <- BRK.cacheFromDirE (MR.csProjectCacheDirE $ cacheStructureState "") ("State-Leg/dd_" <> state <> "_" <> cacheSuffix <> ".bin")
    comboByCat <- K.ignoreCacheTimeM
                  $ BRK.retrieveOrMakeFrame comboByCatCacheKey comboByCatDeps $ \(st, dt, sp, dp, ps) -> combineDistrictPSMaps @ks st dt sp dp ps
    psData <- K.ignoreCacheTime psData_C
    let isUpper = (== GT.StateUpper) . view GT.districtTypeC
        isLower = (== GT.StateLower) . view GT.districtTypeC
        upper_districts = let aud = FL.fold (FL.prefilter isUpper $ FL.premap ((GT.StateUpper,) . view GT.districtName) FL.set) (DP.unPSData psData)
                          in maybe aud (aud `Set.intersection`) districtsM
        lower_districts = let lud = FL.fold (FL.prefilter isLower $ FL.premap ((GT.StateLower,) . view GT.districtName) FL.set) (DP.unPSData psData)
                          in maybe lud (lud `Set.intersection`) districtsM
    ccSetup <- categoryChartSetup summaryRows
    let includeDistrict dt dn = fromMaybe True $ fmap (Set.member (dt, dn)) districtsM
        includedSummary = F.filterFrame (\r → includeDistrict (r ^. GT.districtTypeC) (r ^. GT.districtName)) summaryRows
        oneDistrictDetails (dt, dn) = do

          let onlyDistrictB :: FC.ElemsOf qs [GT.DistrictTypeC, GT.DistrictName] => F.Record qs -> Bool
              onlyDistrictB r = r ^. GT.districtTypeC == dt && r ^. GT.districtName == dn
--              onlyDistrict = F.filterFrame onlyDistrictB
--              onlyDistrictComboByCat = onlyDistrict comboByCat
              rfPair (n, m) = (realToFrac n, realToFrac m)
          distRec <- K.knitMaybe ("allDistrictDetails: empty onlyDistrict for type=" <> show dt <> " and name=" <> dn)
                     (viaNonEmpty head $ FL.fold FL.list $ F.filterFrame onlyDistrictB summaryRows)
          sbcs <- K.knitEither $  DCC.sbcComparison (DCC.ccQuantileFunctions ccSetup) (DCC.ccPartyMedians ccSetup) distRec
          _ <- K.addHvega Nothing Nothing
            $ DCC.sbcChart DCC.SBCState (DCC.ccNumQuantiles ccSetup) 10 (FV.fixedSizeVC 300 80 5) (Just $ DCC.ccNames ccSetup)
            (fmap rfPair <<$>> DCC.ccPartyLoHis ccSetup) (one (fullDNameText distRec, True, sbcs))
          BR.brAddRawHtmlTable (Just "Demographic Summary") (BHA.class_ "brTable") (distSummaryColonnade mempty) (F.filterFrame onlyDistrictB includedSummary)
          let totalRow = FL.fold  (FL.premap F.rcast modelVStatetotalsFld) $ F.filterFrame onlyDistrictB comboByCat
              nt = realToFrac (totalRow ^. DT.popCount) * totalRow ^. MR.modelT
              snt = realToFrac (totalRow ^. DT.popCount) * totalRow ^. stateModelT
          BR.brAddRawHtmlTable (Just "Group Details") (BHA.class_ "brTable") (distDetailsColonnade catText mempty nt snt)
            (FL.fold FL.list (fmap (GroupRow @ks) $ F.filterFrame onlyDistrictB comboByCat)
             <> [TotalRow state dt dn totalRow]
            )
    traverse_ oneDistrictDetails $ FL.fold FL.list upper_districts
    traverse_ oneDistrictDetails $ FL.fold FL.list lower_districts

categoryChartSetup :: forall rs r .
                   (FC.ElemsOf rs [DT.PWPopPerSqMile, DP.FracGradOfWhite, DP.FracWhite, MR.ModelCI, ET.DemShare]
                   , FSI.RecVec rs
                   , K.KnitEffects r
                   )
                   => F.FrameRec rs -> K.Sem r (DCC.CategoryChartSetup Double rs)
categoryChartSetup summaryRows = do
  let categoryNames = ["Density", "%Grad-Among_White", "%Voters-Of-Color"]
      categoryFunctions :: [DCC.SBCFunctionSpec rs Double] =
        zipWith
          DCC.SBCCategoryData
          categoryNames
          [ view DT.pWPopPerSqMile
          , view DP.fracGradOfWhite
          , \r → 1 - r ^. DP.fracWhite
          ]
      quantileBreaks = DCC.sbcQuantileBreaks 20 categoryFunctions summaryRows
      quantileFunctionsE = DCC.sbcQuantileFunctions quantileBreaks
      quantileFunctionsDblE = (fmap (realToFrac @Int @Double) .) <<$>> quantileFunctionsE
      shareF = view ET.demShare {- MT.ciMid . view MR.modelCI -}
      partyFilters = DCC.SBCPartyData ((>= 0.5) . shareF) ((< 0.5) . shareF)
      partyMediansE = DCC.partyMedians partyFilters quantileFunctionsDblE summaryRows
      partyRanksE = DCC.partyRanks partyFilters quantileFunctionsDblE summaryRows
      partyLoHisE = DCC.partyLoHis <$> partyRanksE
  partyLoHis ← K.knitEither partyLoHisE
  partyMedians ← K.knitEither partyMediansE
  pure $ DCC.CategoryChartSetup categoryNames partyLoHis partyMedians 20 quantileFunctionsDblE

data DistDetailsRow ks where
  GroupRow :: (ks F.⊆ SLDKeyAnd2 ks ModelCompR, ModelCompR F.⊆ SLDKeyAnd2 ks ModelCompR)  => F.Record (SLDKeyAnd2 ks ModelCompR) -> DistDetailsRow ks
  TotalRow :: Text -> GT.DistrictType -> Text -> F.Record ModelCompR -> DistDetailsRow ks

ddState :: DistDetailsRow ks -> Text
ddState (GroupRow r) = r ^. GT.stateAbbreviation
ddState (TotalRow s _ _ _) = s

ddDType :: DistDetailsRow ks -> GT.DistrictType
ddDType (GroupRow r) = r ^. GT.districtTypeC
ddDType (TotalRow _ dt _ _) = dt

ddDName :: DistDetailsRow ks -> Text
ddDName (GroupRow r) = r ^. GT.districtName
ddDName (TotalRow _ _ dn _) = dn

ddGroup :: (F.Record ks -> Text) -> DistDetailsRow ks -> Text
ddGroup f (GroupRow r) = f $ F.rcast r
ddGroup _ (TotalRow _ _ _ _) = "Total"

toDetailsRec :: DistDetailsRow ks -> F.Record ModelCompR
toDetailsRec (GroupRow r) = F.rcast r
toDetailsRec (TotalRow _ _ _ r) = r

distDetailsColonnade :: (F.Record ks -> Text) -> BR.CellStyleF (DistDetailsRow ks) [Char] -> Double -> Double -> C.Colonnade C.Headed (DistDetailsRow ks) K.Cell
distDetailsColonnade kText cas nt stateNT =
  let state' = ddState
      fullDName ddr = dTypeText' (ddDType ddr) <> "-" <> ddDName ddr
  in C.headed "State" (BR.toCell cas "State" "State" (BR.textToStyledHtml . state'))
     <> C.headed "District" (BR.toCell cas "District" "District" (BR.textToStyledHtml . fullDName))
     <> C.headed "Group" (BR.toCell cas "Group" "Group" (BR.textToStyledHtml . ddGroup kText))
     <> distDetailsColonnadeDC cas nt stateNT

distDetailsColonnadeDC :: BR.CellStyleF (DistDetailsRow ks) [Char] -> Double -> Double -> C.Colonnade C.Headed (DistDetailsRow ks) K.Cell
distDetailsColonnadeDC cas nt stateNT =
  let popR = realToFrac . view DT.popCount . toDetailsRec
      pctElectorate r = popR r * toDetailsRec r ^. MR.modelT / nt
      pctElectorateS r = popR r * toDetailsRec r ^. stateModelT / stateNT
  in C.headed "# People" (BR.toCell cas "N" "N" (BR.numTextColor "black" "%2d" . view DT.popCount . toDetailsRec))
  <> C.headed "Density (ppl/sq mile)"  (BR.toCell cas "Density" "Density" (BR.numTextColor "black" "%2.1f" . view DT.pWPopPerSqMile . toDetailsRec))
  <> C.headed "Turnout %" (BR.toCell cas "T" "T" (BR.numTextColor "black" "%2.1f" . (100*) . view MR.modelT . toDetailsRec))
  <> C.headed "% Electorate" (BR.toCell cas "% Electorate" "% Electorate" (BR.numTextColor "black" "%2.1f" . (100*) . pctElectorate))
--  <> C.headed "State T" (BR.toCell cas "State T" "State T" (BR.numSimple "black" "%2.1f" . (100*) . view stateModelT . toDetailsRec))
--  <> C.headed "State % Electorate" (BR.toCell cas "State % Elec" "State % Elec" (BR.numSimple "black" "%2.1f" . (100*) . pctElectorateS))
  <> C.headed "D Pref %" (BR.toCell cas "P" "P" (rbStyle "%2.1f" . (100*) . view MR.modelP . toDetailsRec))
--  <> C.headed "State P" (BR.toCell cas "State P" "State P" (rbStyle "%2.1f" . (100*) . view stateModelP . toDetailsRec))
  <> C.headed "D Share" (BR.toCell cas "Share" "Share" (BR.numTextColor "black" "%2.1f" . (100*) . view share . toDetailsRec))
--  <> C.headed "State Share" (BR.toCell cas "State Share" "State Share" (BR.numSimple "black" "%2.1f" . (100*) . view stateShare . toDetailsRec))


type CombineDistrictPSMapsC ks =
  (FJ.CanLeftJoinWithMissing ks (ks V.++ '[StateModelT]) (ks V.++ '[StateModelP])
  , FJ.CanLeftJoinWithMissing (AndSLDKey ks) (AndSLDKey2 ks '[MR.ModelT]) (AndSLDKey2 ks '[MR.ModelP])
  , FJ.CanLeftJoinWithMissing ks
    (FJ.JoinResult  (AndSLDKey ks) (AndSLDKey2 ks '[MR.ModelT]) (AndSLDKey2 ks '[MR.ModelP]))
    (FJ.JoinResult ks (ks V.++ '[StateModelT]) (ks V.++ '[StateModelP]))
  , FJ.CanLeftJoinWithMissing
    (AndSLDKey ks)
    (FJ.JoinResult ks
    (FJ.JoinResult  (AndSLDKey ks) (AndSLDKey2 ks '[MR.ModelT]) (AndSLDKey2 ks '[MR.ModelP]))
    (FJ.JoinResult ks (ks V.++ '[StateModelT]) (ks V.++ '[StateModelP])))
    (AndSLDKey2 ks '[DT.PopCount, DT.PWPopPerSqMile])
  , FSI.RecVec (ks V.++ '[StateModelT])
  , FSI.RecVec (ks V.++ '[StateModelP])
  , FSI.RecVec (AndSLDKey2 ks '[MR.ModelT])
  , FSI.RecVec (AndSLDKey2 ks '[MR.ModelP])
  , Show (F.Record ks)
  , Show (F.Record (AndSLDKey ks))
  , (ks V.++ [MR.ModelT, MR.ModelP, StateModelT, StateModelP, DT.PopCount, DT.PWPopPerSqMile]) F.⊆
    ((FJ.JoinResult (AndSLDKey ks)
     (FJ.JoinResult ks
      (FJ.JoinResult  (AndSLDKey ks) (AndSLDKey2 ks '[MR.ModelT]) (AndSLDKey2 ks '[MR.ModelP]))
      (FJ.JoinResult ks (ks V.++ '[StateModelT]) (ks V.++ '[StateModelP])))
     (AndSLDKey2 ks '[DT.PopCount, DT.PWPopPerSqMile])))
  , FC.ElemsOf (FJ.JoinResult (AndSLDKey ks)
                (FJ.JoinResult ks
                 (FJ.JoinResult  (AndSLDKey ks) (AndSLDKey2 ks '[MR.ModelT]) (AndSLDKey2 ks '[MR.ModelP]))
                 (FJ.JoinResult ks (ks V.++ '[StateModelT]) (ks V.++ '[StateModelP])))
                (AndSLDKey2 ks '[DT.PopCount, DT.PWPopPerSqMile]))
    (SLDKeyAnd2 ks [MR.ModelT, MR.ModelP, StateModelT, StateModelP, DT.PopCount])
  , Ord (F.Record (AndSLDKey ks))
  , AndSLDKey ks F.⊆ DP.PSDataR SLDKeyR
  , FSI.RecVec (AndSLDKey2 ks [DT.PopCount, DT.PWPopPerSqMile])
  , Ord (F.Record ks)
  , (ks V.++ [MR.ModelT, MR.ModelP, StateModelT, StateModelP, DT.PopCount, DT.PWPopPerSqMile]) F.⊆
    (SLDKeyAnd2 ks [MR.ModelT, MR.ModelP, StateModelT, StateModelP, DT.PopCount, DT.PWPopPerSqMile])
  , (ks V.++ ModelCompR) F.⊆ (ks V.++ [MR.ModelT, MR.ModelP, StateModelT, StateModelP, DT.PopCount, DT.PWPopPerSqMile] V.++ [Share, StateShare])
  , FC.ElemsOf (ks V.++ [MR.ModelT, MR.ModelP, StateModelT, StateModelP, DT.PopCount, DT.PWPopPerSqMile])
    [MR.ModelT, MR.ModelP, StateModelT, StateModelP, DT.PopCount, DT.PWPopPerSqMile]
  , FSI.RecVec (ks V.++ ModelCompR)
  )

combineDistrictPSMaps :: forall ks r . (K.KnitEffects r, CombineDistrictPSMapsC ks)
                      => MC.PSMap ks MT.ConfidenceInterval
                      -> MC.PSMap (AndSLDKey ks) MT.ConfidenceInterval
                      -> MC.PSMap ks MT.ConfidenceInterval
                      -> MC.PSMap (AndSLDKey ks) MT.ConfidenceInterval
                      -> DP.PSData SLDKeyR
                      -> K.Sem r (F.FrameRec (SLDKeyAnd2 ks ModelCompR))
combineDistrictPSMaps stateT dT stateP dP psData = do
  let stateTF = MC.psMapToFrame @StateModelT $ fmap MT.ciMid stateT
      statePF = MC.psMapToFrame @StateModelP $ fmap MT.ciMid stateP
      distTF :: F.FrameRec (AndSLDKey2 ks '[MR.ModelT]) =  MC.psMapToFrame @MR.ModelT $ fmap MT.ciMid dT
      distPF :: F.FrameRec (AndSLDKey2 ks '[MR.ModelP]) =  MC.psMapToFrame @MR.ModelP $ fmap MT.ciMid dP
      (stateBoth, sbMissing) = FJ.leftJoinWithMissing @ks stateTF statePF
      (distBoth, dbMissing) = FJ.leftJoinWithMissing @(AndSLDKey ks) @(AndSLDKey2 ks '[MR.ModelT]) @(AndSLDKey2 ks '[MR.ModelP]) distTF distPF
      (allModel, allModelMissing) = FJ.leftJoinWithMissing @ks distBoth stateBoth
      (all, allMissing) = FJ.leftJoinWithMissing @(AndSLDKey ks) allModel (aggregatePS @ks psData)
  when (not $ null sbMissing) $ K.knitError $ "combineDistrictPSMaps: Missing keys in stateT/stateP join=" <> show sbMissing
  when (not $ null dbMissing) $ K.knitError $ "combineDistrictPSMaps: Missing keys in distT/distP join=" <> show dbMissing
  when (not $ null allModelMissing) $ K.knitError $ "combineDistrictPSMaps: Missing keys in dist/state join=" <> show allModelMissing
  when (not $ null allMissing) $ K.knitError $ "combineDistrictPSMaps: Missing keys in model/psData join=" <> show allMissing
  pure $ FL.fold (addSharesFld @ks) $ fmap F.rcast all

addSharesFld :: forall ks .
                (Ord (F.Record ks)
                , (ks V.++ [MR.ModelT, MR.ModelP, StateModelT, StateModelP, DT.PopCount, DT.PWPopPerSqMile]) F.⊆
                  (SLDKeyAnd2 ks [MR.ModelT, MR.ModelP, StateModelT, StateModelP, DT.PopCount, DT.PWPopPerSqMile])
                , (ks V.++ ModelCompR) F.⊆ (ks V.++ [MR.ModelT, MR.ModelP, StateModelT, StateModelP, DT.PopCount, DT.PWPopPerSqMile] V.++ [Share, StateShare])
                , FC.ElemsOf (ks V.++ [MR.ModelT, MR.ModelP, StateModelT, StateModelP, DT.PopCount, DT.PWPopPerSqMile])
                  [MR.ModelT, MR.ModelP, StateModelT, StateModelP, DT.PopCount, DT.PWPopPerSqMile]
                , FSI.RecVec (ks V.++ ModelCompR)
                )
             => FL.Fold (F.Record (SLDKeyAnd2 ks [MR.ModelT, MR.ModelP, StateModelT, StateModelP, DT.PopCount, DT.PWPopPerSqMile]))
                (F.FrameRec (SLDKeyAnd2 ks ModelCompR))
addSharesFld = FMR.concatFold
               $ FMR.mapReduceFold
               FMR.noUnpack
               (FMR.assignKeysAndData @SLDKeyR @(ks V.++ [MR.ModelT, MR.ModelP, StateModelT, StateModelP, DT.PopCount, DT.PWPopPerSqMile]))
               (FMR.makeRecsWithKey id $ FMR.ReduceFold (const innerFld))
  where
    rPop = realToFrac . view DT.popCount
    ntF r = rPop r * r ^. MR.modelT
    sntF r = rPop r * r ^. stateModelT
    ntFld = FL.premap ntF FL.sum
    sntFld = FL.premap sntF FL.sum
    shareF x r = rPop r * r ^. MR.modelT * r ^. MR.modelP / x
    stateShareF x r = rPop r * r ^. stateModelT * r ^. stateModelP / x
    f :: Double -> Double -> F.Record (ks V.++ [MR.ModelT, MR.ModelP, StateModelT, StateModelP, DT.PopCount, DT.PWPopPerSqMile]) -> F.Record (ks V.++ ModelCompR)
    f nt snt r =  F.rcast $ r F.<+> FT.recordSingleton @Share (shareF nt r) F.<+> FT.recordSingleton @StateShare (stateShareF snt r)
    innerFld :: FL.Fold (F.Record (ks V.++ [MR.ModelT, MR.ModelP, StateModelT, StateModelP, DT.PopCount, DT.PWPopPerSqMile]))
             (F.FrameRec (ks V.++ ModelCompR))
    innerFld = (\nt snt l -> F.toFrame (fmap (f nt snt) l)) <$> ntFld <*> sntFld <*> FL.list

modelVStatetotalsFld :: FL.Fold (F.Record ModelCompR)
                        (F.Record ModelCompR)
modelVStatetotalsFld =
  let popFld = FL.premap (view DT.popCount) FL.sum
      posDiv x y = if y /= 0 then x / realToFrac y else 0
      wgtd f r = realToFrac (r ^. DT.popCount) * f r
      popWgtdFld f = posDiv <$> FL.premap (wgtd f) FL.sum <*> popFld
      modelT = popWgtdFld (view MR.modelT)
      modelP = popWgtdFld (view MR.modelP)
      shr = FL.premap (view share) FL.sum
      sModelT = popWgtdFld (view stateModelT)
      sModelP = popWgtdFld (view stateModelP)
      sshr = FL.premap (view stateShare) FL.sum
      dens = popWgtdFld (view DT.pWPopPerSqMile)
  in (\t p s st sp ss n d -> t F.&: p F.&: s F.&: st F.&: sp F.&: ss F.&: n F.&: d F.&: V.RNil)
     <$> modelT <*> modelP <*> shr <*> sModelT <*> sModelP <*> sshr <*> popFld <*> dens

aggregatePS :: forall ks . (Ord (F.Record (AndSLDKey ks))
                           , AndSLDKey ks F.⊆ DP.PSDataR SLDKeyR
                           , FSI.RecVec (AndSLDKey2 ks [DT.PopCount, DT.PWPopPerSqMile]))
            => DP.PSData SLDKeyR -> F.FrameRec (AndSLDKey2 ks '[DT.PopCount, DT.PWPopPerSqMile])
aggregatePS = FL.fold f . DP.unPSData
  where
    f :: FL.Fold (F.Record (DP.PSDataR SLDKeyR)) (F.FrameRec (AndSLDKey2 ks '[DT.PopCount, DT.PWPopPerSqMile]))
    f = FMR.concatFold
        $ FMR.mapReduceFold FMR.noUnpack (FMR.assignKeysAndData @(AndSLDKey ks) @[DT.PopCount, DT.PWPopPerSqMile])
        (FMR.foldAndAddKey DT.pwDensityAndPopFldRec)

distSummaryColonnade :: (FC.ElemsOf rs DistSummaryR, FC.ElemsOf rs [GT.StateAbbreviation, GT.DistrictTypeC, GT.DistrictName, ET.DemShare, MR.ModelCI])
                     => BR.CellStyleF (F.Record rs) [Char] -> C.Colonnade C.Headed (F.Record rs) K.Cell
distSummaryColonnade cas =
  let state = F.rgetField @GT.StateAbbreviation
--      frac45AndOver r = r ^. DP.frac45To64 + r ^. DP.frac65plus
      share50 = MT.ciMid . F.rgetField @MR.ModelCI
  in C.headed "State" (BR.toCell cas "State" "State" (BR.textToStyledHtml . state))
     <> C.headed "District" (BR.toCell cas "District" "District" (BR.textToStyledHtml . fullDNameText))
     <> C.headed "Historical" (BR.toCell cas "Historical" "Historical" (rbStyle "%2.1f" . (100*) . F.rgetField @ET.DemShare))
     <> C.headed "Model" (BR.toCell cas "50%" "50%" (rbStyle "%2.1f" . (100*) . share50))
     <> C.headed "PW Density"  (BR.toCell cas "Density" "Density" (BR.numberToStyledHtml "%2.1f" . view DT.pWPopPerSqMile))
     <> C.headed "% Over 45"  (BR.toCell cas "% Over 45" "% Over 45" (BR.numberToStyledHtml "%2.1f" . view DP.frac45AndOver))
     <> C.headed "% College Grad"  (BR.toCell cas "% Grad" "% Grad" (BR.numberToStyledHtml "%2.1f" . view DP.fracCollegeGrad))
     <> C.headed "% NH White"  (BR.toCell cas "% NH White" "% NH White" (BR.numberToStyledHtml "%2.1f" . view DP.fracWhite))
     <> C.headed "% Black"  (BR.toCell cas "% Black" "% Black" (BR.numberToStyledHtml "%2.1f" . view DP.fracBlack))
     <> C.headed "% Hispanic"  (BR.toCell cas "% Hispanic" "% Hispanic" (BR.numberToStyledHtml "%2.1f" . view DP.fracHispanic))
     <> C.headed "% AAPI"  (BR.toCell cas "% AAPI" "% AAPI" (BR.numberToStyledHtml "%2.1f" . view DP.fracAAPI))
     <> C.headed "% Grad Of White"  (BR.toCell cas "% Grad of White" "% Grad of White" (BR.numberToStyledHtml "%2.1f" . view DP.fracGradOfWhite))

sldTableCellStyle :: FC.ElemsOf rs '[GT.StateAbbreviation, GT.DistrictTypeC, GT.DistrictName, MR.ModelCI, ET.DemShare]
                  => Text -> BR.CellStyleF (F.Record rs) String
sldTableCellStyle state =
  let dlcc = fromMaybe [] $ M.lookup state dlccMap
      toDistrict (x, y, _) = (x, y)
      isDLCC r = (r ^. GT.districtTypeC, r ^. GT.districtName) `elem` fmap toDistrict dlcc
      dlccChosenCS = bordered "purple" `BR.cellStyleIf` \r h -> (isDLCC r && h == "District")
      longShot ci = MT.ciUpper ci < 0.48
      leanR ci = MT.ciMid ci < 0.5 && MT.ciUpper ci >= 0.48
      leanD ci = MT.ciMid ci >= 0.5 && MT.ciLower ci <= 0.52
      safeD ci = MT.ciLower ci > 0.52
      mi = F.rgetField @MR.ModelCI
      eRes = F.rgetField @ET.DemShare
      longShotCS = bordered "red" `BR.cellStyleIf` \r h -> longShot (mi r) && h == "95%"
      leanRCS = bordered "pink" `BR.cellStyleIf` \r h -> leanR (mi r) && h `elem` ["95%", "50%"]
      leanDCS = bordered "skyblue" `BR.cellStyleIf` \r h -> leanD (mi r) && h `elem` ["5%", "50%"]
      safeDCS = bordered "blue" `BR.cellStyleIf` \r h -> safeD (mi r) && h == "5%"
      resLongShotCS = bordered "red" `BR.cellStyleIf` \r h -> eRes r < 0.48 && T.isPrefixOf "2019" h
      resLeanRCS = bordered "pink" `BR.cellStyleIf` \r h -> eRes r >= 0.48 && eRes r < 0.5 && T.isPrefixOf "2019" h
      resLeanDCS = bordered "skyblue" `BR.cellStyleIf` \r h -> eRes r >= 0.5 && eRes r <= 0.52 && T.isPrefixOf "2019" h
      resSafeDCS = bordered "blue" `BR.cellStyleIf` \r h -> eRes r > 0.52 && T.isPrefixOf "2019" h
  in mconcat [dlccChosenCS, longShotCS, leanRCS, leanDCS, safeDCS]

leansCellStyle :: String -> (row -> Double) -> BR.CellStyleF row String
leansCellStyle col pl =
  let longShot x = x < 0.47
      leanR x = x < 0.5 && x >= 0.47
      leanD x = x >= 0.5 && x <= 0.53
      safeD x = x > 0.53
      longShotCS = bordered "red" `BR.cellStyleIf` \r h -> longShot (pl r) && h == col
      leanRCS = bordered "pink" `BR.cellStyleIf` \r h -> leanR (pl r) && h == col
      leanDCS = bordered "skyblue" `BR.cellStyleIf` \r h -> leanD (pl r) && h == col
      safeDCS = bordered "blue" `BR.cellStyleIf` \r h -> safeD (pl r) && h == col
  in mconcat [longShotCS, leanRCS, leanDCS, safeDCS]

sldColonnade :: (FC.ElemsOf rs [GT.StateAbbreviation, GT.DistrictTypeC, GT.DistrictName, MR.ModelCI, ET.DemShare])
             => BR.CellStyleF (F.Record rs) [Char] -> C.Colonnade C.Headed (F.Record rs) K.Cell
sldColonnade cas =
  let state = F.rgetField @GT.StateAbbreviation
      share5 = MT.ciLower . F.rgetField @MR.ModelCI
      share50 = MT.ciMid . F.rgetField @MR.ModelCI
      share95 = MT.ciUpper . F.rgetField @MR.ModelCI
  in C.headed "State" (BR.toCell cas "State" "State" (BR.textToStyledHtml . state))
     <> C.headed "District" (BR.toCell cas "District" "District" (BR.textToStyledHtml . fullDNameText))
     <> C.headed "Historical" (BR.toCell cas "Historical" "Historical" (rbStyle "%2.1f" . (100*) . F.rgetField @ET.DemShare))
     <> C.headed "5%" (BR.toCell cas "5%" "5%" (rbStyle "%2.1f" . (100*) . share5))
     <> C.headed "50%" (BR.toCell cas "50%" "50%" (rbStyle "%2.1f" . (100*) . share50))
     <> C.headed "95%" (BR.toCell cas "95%" "95%" (rbStyle "%2.1f" . (100*) . share95))


dTypeText' :: GT.DistrictType -> Text
dTypeText' dt = case dt of
  GT.StateUpper -> "Upper"
  GT.StateLower -> "Lower"
  _ -> "Not State Leg!"

dTypeText :: F.ElemOf rs GT.DistrictTypeC => F.Record rs -> Text
dTypeText r = dTypeText' (r ^. GT.districtTypeC)

fullDNameText :: FC.ElemsOf rs [GT.DistrictTypeC, GT.DistrictName] => F.Record rs -> Text
fullDNameText r = dTypeText r <> "-" <> r ^. GT.districtName

byDistrictName :: FC.ElemsOf rs [GT.DistrictTypeC, GT.DistrictName] => F.Record rs -> F.Record rs -> Ordering
byDistrictName r1 r2 = GT.districtNameCompare (r1 ^. GT.districtName) (r2 ^. GT.districtName)

modelDRAComparisonChart :: (K.KnitEffects r
                           , FC.ElemsOf rs [GT.StateAbbreviation, MR.ModelCI, ET.DemShare, GT.DistrictName, GT.DistrictTypeC]
                           )
                        => BR.PostPaths Path.Abs -> BR.PostInfo -> Text -> Text -> FV.ViewConfig -> F.FrameRec rs -> K.Sem r GV.VegaLite
modelDRAComparisonChart pp pi chartID title vc rows = do
  let colData r = [("State", GV.Str $ r ^. GT.stateAbbreviation)
                  ,("District", GV.Str $ show (r ^. GT.districtTypeC) <> "-" <> r ^. GT.districtName)
                  ,("Model_Lo" , GV.Number $ MT.ciLower $ r ^. MR.modelCI)
                  ,("Model" , GV.Number $ MT.ciMid $ r ^. MR.modelCI)
                  ,("Model_Hi" , GV.Number $ MT.ciUpper $ r ^. MR.modelCI)
                  ,("Historical", GV.Number $ r ^. ET.demShare)
                  ]
      jsonRows = FL.fold (VJ.rowsToJSON colData [] Nothing) rows
  jsonFilePrefix <- K.getNextUnusedId $ ("2023-StateLeg_" <> chartID)
  jsonUrl <-  BRK.brAddJSON pp pi jsonFilePrefix jsonRows
  let vlData = GV.dataFromUrl jsonUrl [GV.JSON "values"]
      encHistorical = GV.position GV.X [GV.PName "Historical", GV.PmType GV.Quantitative,  GV.PScale [GV.SZero False]]
      encModel = GV.position GV.Y [GV.PName "Model", GV.PmType GV.Quantitative,  GV.PScale [GV.SZero False]]
      markMid = GV.mark GV.Circle [GV.MTooltip GV.TTData]
      midSpec = GV.asSpec [(GV.encoding . encHistorical . encModel) [], markMid]
      encModelLo = GV.position GV.Y [GV.PName "Model_Lo", GV.PmType GV.Quantitative,  GV.PScale [GV.SZero False]]
      encModelHi = GV.position GV.Y2 [GV.PName "Model_Hi", GV.PmType GV.Quantitative,  GV.PScale [GV.SZero False]]
      markError = GV.mark GV.ErrorBar [GV.MTooltip GV.TTEncoding]
      errorSpec = GV.asSpec [(GV.encoding . encHistorical . encModelLo . encModelHi) [], markError]
      encHistoricalY = GV.position GV.Y [GV.PName "Historical", GV.PmType GV.Quantitative,  GV.PScale [GV.SZero False]]
      lineSpec = GV.asSpec [(GV.encoding . encHistorical . encHistoricalY) [], GV.mark GV.Line []]
      layers = GV.layer [midSpec, errorSpec, lineSpec]
  pure $  FV.configuredVegaLite vc [FV.title title
                                  , layers
                                  , vlData
                                  ]

geoCompChart :: (K.KnitEffects r, Foldable f)
              => BR.PostPaths Path.Abs
              -> BR.PostInfo
              -> Text
              -> Text
              -> FV.ViewConfig
              -> Text
              -> GT.DistrictType
              -> (row -> Text)
              -> (Text, row -> Double, Maybe Text, Maybe (Double, Double))
              -> f row
              -> K.Sem r GV.VegaLite
geoCompChart pp pi chartID title vc sa dType districtName (label, f, cSchemeM, extentM) rows = do
  let colData r = [ ("District", GV.Str $ districtName r)
--                  , ("Model" , GV.Number $ MT.ciMid $ r ^. MR.modelCI)
--                  , ("Historical", GV.Number $ r ^. ET.demShare)
                  , (label, GV.Number $ f r) --100 * ((MT.ciMid $ r ^. MR.modelCI) -  r ^. ET.demShare))
                  ]
      jsonRows = FL.fold (VJ.rowsToJSON colData [] Nothing) rows
      schemeExtentM = do
        (lo, hi) <- extentM
        let (minM, maxM) = FL.fold ((,) <$> FL.premap f FL.minimum <*> FL.premap f FL.maximum) rows
        datMin <- minM
        datMax <- maxM
        pure ((datMin - lo) / (hi - lo), 1 - ((hi - datMax) / (hi - lo)))
      schemeExtent = maybe [] (\(l, h) -> [l, h]) schemeExtentM
  jsonFilePrefix <- K.getNextUnusedId $ ("2023-StateLeg_" <> chartID)
  jsonDataUrl <-  BRK.brAddJSON pp pi jsonFilePrefix jsonRows
  (geoJsonPrefix, titleSuffix) <- case dType of
    GT.StateUpper -> pure $ (sa <> "_2022_sldu", " (Upper Chamber)")
    GT.StateLower -> pure $ (sa <> "_2022_sldl", " (Lower Chamber)")
    _ -> K.knitError $ "modelGeoChart: Bad district type (" <> show dType <> ") specified"

  geoJsonSrc <- K.liftKnit @IO $ Path.parseAbsFile $ toString $ "/Users/adam/BlueRipple/GeoData/input_data/StateLegDistricts/" <> sa <> "/" <> geoJsonPrefix <> "_topo.json"
  jsonGeoUrl <- BRK.brCopyDataForPost pp pi BRK.LeaveExisting geoJsonSrc Nothing
  let rowData = GV.dataFromUrl jsonDataUrl [GV.JSON "values"]
      geoData = GV.dataFromUrl jsonGeoUrl [GV.TopojsonFeature geoJsonPrefix]
      encQty = GV.color $ [GV.MName label, GV.MmType GV.Quantitative] <> maybe [] (\s -> [GV.MScale [GV.SScheme s schemeExtent]]) cSchemeM
      encTooltips = GV.tooltips [[GV.TName "District", GV.TmType GV.Nominal]
                                , [GV.TName label, GV.TmType GV.Quantitative]
                                ]
      encoding = (GV.encoding . encQty . encTooltips) []
      tLookup = (GV.transform . GV.lookup "properties.NAME" rowData "District" (GV.LuFields ["District",label])) []
      mark = GV.mark GV.Geoshape []
      projection = GV.projection [GV.PrType GV.Identity, GV.PrReflectY True]
  pure $ BR.brConfiguredVegaLite vc [FV.title (title <> titleSuffix), geoData, tLookup, projection, encoding, mark]

modeledMapToFrame :: MC.PSMap SLDKeyR MT.ConfidenceInterval -> F.FrameRec ModeledR
modeledMapToFrame = F.toFrame . fmap (\(k, ci) -> k F.<+> FT.recordSingleton @MR.ModelCI ci) . M.toList . MC.unPSMap

modeledACSBySLD :: (K.KnitEffects r, BRK.CacheEffects r) => BR.CommandLine -> K.Sem r (K.ActionWithCacheTime r (DP.PSData SLDKeyR))
modeledACSBySLD cmdLine = do
  (jointFromMarginalPredictorCSR_ASR_C, _) <- CDDP.cachedACSa5ByPUMA  ACS.acs1Yr2012_21 2021 -- most recent available
                                                 >>= DMC.predictorModel3 @'[DT.CitizenC] @'[DT.Age5C] @DMC.SRCA @DMC.SR
                                                 (Right "CSR_ASR_ByPUMA")
                                                 (Right "model/demographic/csr_asr_PUMA")
                                                 False -- use model not just mean
                                                 cmdLine Nothing Nothing . fmap (fmap F.rcast)
  (jointFromMarginalPredictorCASR_ASE_C, _) <- CDDP.cachedACSa5ByPUMA ACS.acs1Yr2012_21 2021 -- most recent available
                                                  >>= DMC.predictorModel3 @[DT.CitizenC, DT.Race5C] @'[DT.Education4C] @DMC.ASCRE @DMC.AS
                                                  (Right "CASR_SER_ByPUMA")
                                                  (Right "model/demographic/casr_ase_PUMA")
                                                  False -- use model, not just mean
                                                  cmdLine Nothing Nothing . fmap (fmap F.rcast)
  (acsCASERBySLD, _products) <- BRC.censusTablesFor2022SLD_ACS2021
                                >>= DMC.predictedCensusCASER' (DTP.viaNearestOnSimplex) (Right "model/election2/sldDemographics")
                                jointFromMarginalPredictorCSR_ASR_C
                                jointFromMarginalPredictorCASR_ASE_C
  BRK.retrieveOrMakeD "model/election2/data/sldPSData.bin" acsCASERBySLD
    $ \x -> DP.PSData . fmap F.rcast <$> (BRL.addStateAbbrUsingFIPS $ F.filterFrame ((== DT.Citizen) . view DT.citizenC) x)
{-
modeledACSBySLD' :: (K.KnitEffects r, BRK.CacheEffects r) => BR.CommandLine -> K.Sem r (K.ActionWithCacheTime r (DP.PSData SLDKeyR))
modeledACSBySLD' cmdLine = do
  (jointFromMarginalPredictorCSR_ASR_C, _, _) <- CDDP.cachedACSa5ByPUMA  ACS.acs1Yr2012_21 2021 -- most recent available
                                                 >>= DMC.predictorModel3 @'[DT.CitizenC] @'[DT.Age5C] @DMC.SRCA @DMC.SR
                                                 (Right "model/demographic/csr_asr_PUMA") "CSR_ASR_ByPUMA"
                                                 cmdLine . fmap (fmap F.rcast)
  (jointFromMarginalPredictorCASR_ASE_C, _, _) <- CDDP.cachedACSa5ByPUMA ACS.acs1Yr2012_21 2021 -- most recent available
                                                  >>= DMC.predictorModel3 @[DT.CitizenC, DT.Race5C] @'[DT.Education4C] @DMC.ASCRE @DMC.AS
                                                  (Right "model/demographic/casr_ase_PUMA") "CASR_SER_ByPUMA"
                                                  cmdLine . fmap (fmap F.rcast)
  (acsCASERBySLD, _products) <- BRC.censusTablesFor2022SLD_ACS2021
                                >>= DMC.predictedCensusCASER' (DTP.viaNearestOnSimplex) True "test"
                                jointFromMarginalPredictorCSR_ASR_C
                                jointFromMarginalPredictorCASR_ASE_C
  BRK.retrieveOrMakeD "test/data/sldPSData.bin" acsCASERBySLD
    $ \x -> DP.PSData . fmap F.rcast <$> (BRL.addStateAbbrUsingFIPS $ F.filterFrame ((== DT.Citizen) . view DT.citizenC) x)

testCensusPredictions :: (K.KnitEffects r, BRK.CacheEffects r) => BR.CommandLine -> K.Sem r ()
testCensusPredictions cmdLine = do
  let densityFilter r = let x = r ^. DT.pWPopPerSqMile in x <= 0 || x > 1e6
  inputFilteredASE <- F.filterFrame densityFilter . BRC.ageSexEducation <$> K.ignoreCacheTimeM BRC.censusTablesFor2022SLD_ACS2021
  K.logLE K.Info "Weird densities in Input?"
  K.logLE K.Info "CSR"
  (F.filterFrame densityFilter . BRC.citizenshipSexRace <$> K.ignoreCacheTimeM BRC.censusTablesFor2022SLD_ACS2021) >>= BRK.logFrame
  K.logLE K.Info "ASR"
  (F.filterFrame densityFilter . BRC.ageSexRace <$> K.ignoreCacheTimeM BRC.censusTablesFor2022SLD_ACS2021) >>= BRK.logFrame
  K.logLE K.Info "ASE"
  (F.filterFrame densityFilter . BRC.ageSexEducation <$> K.ignoreCacheTimeM BRC.censusTablesFor2022SLD_ACS2021) >>= BRK.logFrame
  result <- K.ignoreCacheTimeM $  modeledACSBySLD' cmdLine
--  K.logLE K.Info "Result"
--  BRK.logFrame $ F.filterFrame densityFilter $ DP.unPSData result
  pure ()
-}

postDir ∷ Path.Path Rel Dir
postDir = [Path.reldir|br-2023-StateLeg/posts|]

postLocalDraft
  ∷ Path.Path Rel Dir
  → Maybe (Path.Path Rel Dir)
  → Path.Path Rel Dir
postLocalDraft p mRSD = case mRSD of
  Nothing → postDir BR.</> p BR.</> [Path.reldir|draft|]
  Just rsd → postDir BR.</> p BR.</> rsd

postInputs ∷ Path.Path Rel Dir → Path.Path Rel Dir
postInputs p = postDir BR.</> p BR.</> [Path.reldir|inputs|]

sharedInputs ∷ Path.Path Rel Dir
sharedInputs = postDir BR.</> [Path.reldir|Shared|] BR.</> [Path.reldir|inputs|]

postOnline ∷ Path.Path Rel t → Path.Path Rel t
postOnline p = [Path.reldir|research/Election|] BR.</> p

postPaths
  ∷ (K.KnitEffects r)
  ⇒ Text
  → BR.CommandLine
  → K.Sem r (BR.PostPaths BR.Abs)
postPaths t cmdLine = do
  let mRelSubDir = case cmdLine of
        BR.CLLocalDraft _ _ mS _ → maybe Nothing BR.parseRelDir $ fmap toString mS
        _ → Nothing
  postSpecificP ← K.knitEither $ first show $ Path.parseRelDir $ toString t
  BR.postPaths
    BR.defaultLocalRoot
    sharedInputs
    (postInputs postSpecificP)
    (postLocalDraft postSpecificP mRelSubDir)
    (postOnline postSpecificP)

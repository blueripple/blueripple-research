{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
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
import qualified BlueRipple.Data.CensusLoaders as BRC
import qualified BlueRipple.Data.CensusTables as BRC
import qualified BlueRipple.Data.DataFrames as BRDF
import qualified BlueRipple.Data.Keyed as Keyed
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
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V
import qualified Data.Vinyl.Class.Method as V
import qualified Data.Vinyl.Functor as V
import qualified Data.Map.Merge.Strict as MM
import qualified Data.Set as Set
import qualified Frames.Melt as F
import qualified Frames.MapReduce as FMR
import qualified Frames.Folds as FF
import qualified Frames.Transform as FT
import qualified Frames.Constraints as FC
import qualified Frames.SimpleJoins as FJ
import qualified Frames.Streamly.InCore as FSI
import qualified Frames.Streamly.TH as FTH
import qualified Frames.Serialize as FS

import Path (Dir, Rel)
import qualified Path
import qualified Numeric
import qualified Stan.ModelBuilder.DesignMatrix as DM

import qualified Data.Map.Strict as M

import qualified Text.Printf as PF
import qualified Graphics.Vega.VegaLite as GV
import qualified Graphics.Vega.VegaLite.Compat as FV
import qualified Graphics.Vega.VegaLite.Configuration as FV
import qualified Graphics.Vega.VegaLite.JSON as VJ

import qualified Data.Discrimination.Grouping  as G

--import Data.Monoid (Sum(getSum))
FTH.declareColumn "StateModelT" ''Double
FTH.declareColumn "StateModelP" ''Double

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

dlccMap = M.fromList [("LA",[]), ("MS",dlccMS), ("NJ", dlccNJ), ("VA", dlccVA)]

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
    let postInfo = BR.PostInfo (BR.postStage cmdLine) (BR.PubTimes BR.Unpublished Nothing)
    stateUpperOnlyMap <- FL.fold (FL.premap (\r -> (r ^. GT.stateAbbreviation, r ^. BRDF.sLDUpperOnly)) FL.map)
                         <$> K.ignoreCacheTimeM BRL.stateAbbrCrosswalkLoader

    traverse_ (analyzeStatePost cmdLine postInfo stateUpperOnlyMap dlccMap) ["LA", "MS", "NJ", "VA"]
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

analyzeStatePost :: (K.KnitMany r, BRK.CacheEffects r)
                 => BR.CommandLine
                 -> BR.PostInfo
                 -> Map Text Bool
                 -> Map Text [(GT.DistrictType, Text, Text)]
                 -> Text
                 -> K.Sem r ()
analyzeStatePost cmdLine postInfo stateUpperOnlyMap dlccMap state = do
  modelPostPaths <- postPaths state cmdLine
  let modelDirE = Right "model/election2/stan/"
      cacheDirE = Right "model/election2/"
      cacheStructure state psName = MR.CacheStructure (Right "model/election2/stan/") (Right "model/election2")
                                    psName "AllCells" state
      dmr = MC.tDesignMatrixRow_d
      survey = MC.CESSurvey
      aggregation = MC.WeightedAggregation MC.ContinuousBinomial
      alphaModel =  MC.St_A_S_E_R_AE_AR_ER_StR --MC.St_A_S_E_R_ER_StR_StER
--      alphaModel =  MC.St_A_S_E_R_ER_StR_StER
      psDataForState :: Text -> DP.PSData SLDKeyR -> DP.PSData SLDKeyR
      psDataForState sa = DP.PSData . F.filterFrame ((== sa) . view GT.stateAbbreviation) . DP.unPSData

  BRK.brNewPost modelPostPaths postInfo state $ do
--    presidentialElections_C <- BRL.presidentialByStateFrame

    modeledACSBySLDPSData_C <- modeledACSBySLD cmdLine
    let stateSLDs_C = fmap (psDataForState state) modeledACSBySLDPSData_C
    sldDemographicSummary <- FL.fold (DP.summarizeASER_Fld @[GT.StateAbbreviation, GT.DistrictTypeC, GT.DistrictName]) . DP.unPSData <$> K.ignoreCacheTime stateSLDs_C
    presidentialElections_C <- BRL.presidentialElectionsWithIncumbency
    houseElections_C <- BRL.houseElectionsWithIncumbency
    draShareOverrides_C <- DP.loadOverrides "data/DRA_Shares/DRA_Share.csv" "DRA 2016-2021"
--    K.ignoreCacheTime draShareOverrides_C >>= BRK.logFrame
    let dVSPres2020 = DP.ElexTargetConfig "PresWO" draShareOverrides_C 2020 presidentialElections_C
        dVSHouse2022 = DP.ElexTargetConfig "HouseWO" draShareOverrides_C 2022 houseElections_C
        turnoutConfig agg am = MC.TurnoutConfig survey (MC.ModelConfig agg am (contramap F.rcast dmr))
        prefConfig agg am = MC.PrefConfig (MC.ModelConfig agg am (contramap F.rcast dmr))
        dVSModel psName agg am
          = MR.runFullModelAH @SLDKeyR 2020 (cacheStructure state psName) cmdLine (turnoutConfig agg am) (prefConfig agg am) dVSPres2020
    modeledDVSMap <- K.ignoreCacheTimeM $ dVSModel (state <> "_SLD") aggregation alphaModel stateSLDs_C
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
                 (FV.ViewConfig 500 500 10) modeledAndDRA
    _ <- K.addHvega Nothing Nothing compChart
    let delta r = 100 * ((MT.ciMid $ r ^. MR.modelCI) -  r ^. ET.demShare)
        detailChamber = if upperOnly then GT.StateUpper else GT.StateLower
--        chamberName uo dt =
    when (not upperOnly) $ do
      void $ geoCompChart modelPostPaths postInfo (state <> "_geoDeltaL") (state <> ": model - historical")
        (FV.ViewConfig 500 300 10) state GT.StateLower ("delta", delta) modeledAndDRA
        >>= K.addHvega Nothing Nothing
    geoCompChart modelPostPaths postInfo (state <> "_geoDeltaU") (state <> ": model - historical")
      (FV.ViewConfig 500 300 10) state GT.StateUpper ("delta", delta) modeledAndDRA
      >>= K.addHvega Nothing Nothing
    geoCompChart modelPostPaths postInfo (state <> "_geoDensityL") (state <> ": log density")
      (FV.ViewConfig 500 300 10) state detailChamber ("log density", Numeric.log . view DT.pWPopPerSqMile) modeledAndDRA
      >>= K.addHvega Nothing Nothing
    geoCompChart modelPostPaths postInfo (state <> "_geoAgeL") (state <> ": % Over 45")
      (FV.ViewConfig 500 300 10) state detailChamber ("Over 45 (%)", \r -> 100 * (r ^. DP.frac45To64 + r ^. DP.frac65plus)) modeledAndDRA
      >>= K.addHvega Nothing Nothing
    geoCompChart modelPostPaths postInfo (state <> "_geoCGradL") (state <> ": % College Grad")
      (FV.ViewConfig 500 300 10) state detailChamber ("College Grad (%)", \r -> 100 * (r ^. DP.fracCollegeGrad)) modeledAndDRA
      >>= K.addHvega Nothing Nothing
    let draCompetitive r = let x = r ^. ET.demShare in (x > 0.42 && x < 0.58)
    BR.brAddRawHtmlTable (state <> " model (2020 data): Upper House") (BHA.class_ "brTable") (sldColonnade $ sldTableCellStyle state)
      $ sortBy byDistrictName $ FL.fold FL.list
      $ F.filterFrame draCompetitive
      $ F.filterFrame ((== GT.StateUpper) . view GT.districtTypeC) modeledAndDRA
    when (not upperOnly)
      $ BR.brAddRawHtmlTable (state <> " model (2020 data): Lower House") (BHA.class_ "brTable") (sldColonnade $ sldTableCellStyle state)
      $ sortBy byDistrictName $ FL.fold FL.list
      $ F.filterFrame draCompetitive
      $ F.filterFrame ((== GT.StateLower) . view GT.districtTypeC) modeledAndDRA
    let tc = turnoutConfig aggregation alphaModel
        pc = prefConfig aggregation alphaModel
        draCompDists = FL.fold (FL.prefilter draCompetitive $ FL.premap (\r -> (r ^. GT.districtTypeC, r ^. GT.districtName)) FL.set) modeledAndDRA
    allDistrictDetails cmdLine modelPostPaths postInfo (MR.modelCacheStructure $ cacheStructure state "") tc pc (Just draCompDists) state stateSLDs_C csSummary
    pure ()
  pure ()

type DistSummaryR = [DT.PWPopPerSqMile, DP.Frac45AndOver, DP.FracBlack, DP.FracWhite, DP.FracHispanic, DP.FracAAPI, DP.FracCollegeGrad, DP.FracGradOfWhite]

--type DistDetailsPSKeyR = SLDKeyR V.++


--type SLDKeyAnd as = SLDKeyR V.++ as
type SLDKeyAnd2 as bs = SLDKeyR V.++ as V.++ bs
type AndSLDKey as = as V.++ SLDKeyR
type AndSLDKey2 as bs = as V.++ SLDKeyR V.++ bs

allDistrictDetails
    ∷ ∀ r rs a b
     . ( K.KnitOne r
       , BRK.CacheEffects r
       , FC.ElemsOf rs [GT.StateAbbreviation, GT.DistrictTypeC, GT.DistrictName, MR.ModelCI, ET.DemShare]
       , FC.ElemsOf rs DistSummaryR
       , FSI.RecVec rs
       )
    ⇒ BR.CommandLine
    → BR.PostPaths Path.Abs
    → BR.PostInfo
    -> MR.CacheStructure () ()
    → MC.TurnoutConfig a b
    → MC.PrefConfig b
    → Maybe (Set (GT.DistrictType, Text))
    → Text
    → K.ActionWithCacheTime r (DP.PSData SLDKeyR)
    → F.FrameRec rs
    → K.Sem r ()
allDistrictDetails cmdLine pp pi cacheStructure' tc pc districtsM state psData_C summaryRows = do
    draShareOverrides_C ← DP.loadOverrides "data/DRA_Shares/DRA_Share.csv" "DRA 2016-2021"
    presidentialElections_C ← BRL.presidentialElectionsWithIncumbency
    houseElections_C ← BRL.houseElectionsWithIncumbency
    let cacheStructureState psSuffix = MR.CacheStructure (MR.csModelDirE cacheStructure') (MR.csProjectCacheDirE cacheStructure')
                                       (state <> "_" <> psSuffix) "AllCells" state
    let cacheStructureSLD psSuffix = MR.CacheStructure (MR.csModelDirE cacheStructure') (MR.csProjectCacheDirE cacheStructure')
                                       (state <> "_SLD_" <> psSuffix) "AllCells" state
        dVSPres2020 = DP.ElexTargetConfig "PresWO" draShareOverrides_C 2020 presidentialElections_C
        dVSHouse2022 = DP.ElexTargetConfig "HouseWO" draShareOverrides_C 2022 houseElections_C
    stateTurnoutByRace_C ← MR.runTurnoutModelAH @('[DT.Race5C]) 2020 (cacheStructureState "ByRace") cmdLine tc psData_C
    statePrefByRace_C :: K.ActionWithCacheTime r (MC.PSMap '[DT.Race5C] MT.ConfidenceInterval) ←
      MR.runPrefModelAH @('[DT.Race5C]) 2020 (cacheStructureState "ByRace") cmdLine tc pc dVSPres2020  psData_C
    districtTurnoutByRace_C :: K.ActionWithCacheTime r (MC.PSMap (AndSLDKey '[DT.Race5C]) MT.ConfidenceInterval) ←
      MR.runTurnoutModelAH @(AndSLDKey '[DT.Race5C]) 2020 (cacheStructureSLD "ByRace") cmdLine tc psData_C
    districtPrefByRace_C ← MR.runPrefModelAH @(AndSLDKey '[DT.Race5C]) 2020 (cacheStructureSLD "ByRace") cmdLine tc pc dVSPres2020  psData_C
    K.ignoreCacheTime stateTurnoutByRace_C >>= K.logLE K.Info . show . MC.unPSMap
    K.ignoreCacheTime statePrefByRace_C >>= K.logLE K.Info . show . MC.unPSMap
    let comboByRaceDeps = (,,,,) <$> stateTurnoutByRace_C <*> districtTurnoutByRace_C <*> statePrefByRace_C <*> districtPrefByRace_C <*> psData_C
    comboByRaceCacheKey <- BRK.cacheFromDirE (MR.csProjectCacheDirE $ cacheStructureState "") "State-Leg/dd_ByRace.bin"
    comboByRace <- K.ignoreCacheTimeM
                   $ BRK.retrieveOrMakeFrame comboByRaceCacheKey comboByRaceDeps $ \(st, dt, sp, dp, ps) -> combineDistrictPSMaps @'[DT.Race5C] st dt sp dp ps
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

          let f r = r ^. GT.districtTypeC == dt && r ^. GT.districtName == dn
              onlyDistrict = F.filterFrame f
              rfPair (n, m) = (realToFrac n, realToFrac m)
          distRec <- K.knitMaybe ("allDistrictDetails: empty onlyDistrict for type=" <> show dt <> " and name=" <> dn)
                     (viaNonEmpty head $ FL.fold FL.list $ onlyDistrict summaryRows)
          sbcs <- K.knitEither $  DCC.sbcComparison (DCC.ccQuantileFunctions ccSetup) (DCC.ccPartyMedians ccSetup) distRec
          _ <- K.addHvega Nothing Nothing
            $ DCC.sbcChart DCC.SBCState (DCC.ccNumQuantiles ccSetup) 10 (FV.ViewConfig 300 80 5) (Just $ DCC.ccNames ccSetup)
            (fmap rfPair <<$>> DCC.ccPartyLoHis ccSetup) (one (fullDNameText distRec, True, sbcs))
          BR.brAddRawHtmlTable "Demographic Summary" (BHA.class_ "brTable") (distSummaryColonnade mempty) (onlyDistrict includedSummary)
          BR.brAddRawHtmlTable "Group Details" (BHA.class_ "brTable") (distDetailsColonnade @'[DT.Race5C] (show . view DT.race5C) mempty) (onlyDistrict comboByRace)
    traverse_ oneDistrictDetails $ FL.fold FL.list upper_districts
    traverse_ oneDistrictDetails $ FL.fold FL.list lower_districts

categoryChartSetup :: forall a rs r .
                   (FC.ElemsOf rs [DT.PWPopPerSqMile, DP.FracGradOfWhite, DP.FracWhite, MR.ModelCI]
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
      partyFilters = let f = MT.ciMid . view MR.modelCI in DCC.SBCPartyData ((>= 0.5) . f) ((< 0.5) . f)
      partyMediansE = DCC.partyMedians partyFilters quantileFunctionsDblE summaryRows
      partyRanksE = DCC.partyRanks partyFilters quantileFunctionsDblE summaryRows
      partyLoHisE = DCC.partyLoHis <$> partyRanksE
  partyLoHis ← K.knitEither partyLoHisE
  partyMedians ← K.knitEither partyMediansE
  pure $ DCC.CategoryChartSetup categoryNames partyLoHis partyMedians 20 quantileFunctionsDblE



distDetailsColonnade :: forall ks rs . (FC.ElemsOf rs [GT.StateAbbreviation, GT.DistrictTypeC, GT.DistrictName, MR.ModelT, MR.ModelP
                                                      , StateModelT, StateModelP, DT.PopCount, DT.PWPopPerSqMile]
                                       , ks F.⊆ rs
                                       )
                     => (F.Record ks -> Text) -> BR.CellStyle (F.Record rs) [Char] -> C.Colonnade C.Headed (F.Record rs) K.Cell
distDetailsColonnade kText cas =
  let state = F.rgetField @GT.StateAbbreviation
  in C.headed "State" (BR.toCell cas "State" "State" (BR.textToStyledHtml . state))
     <> C.headed "District" (BR.toCell cas "District" "District" (BR.textToStyledHtml . fullDNameText))
     <> C.headed "Group" (BR.toCell cas "Group" "Group" (BR.textToStyledHtml . kText . F.rcast))
     <> C.headed "N" (BR.toCell cas "N" "N" (BR.numberToStyledHtml "%2d" . view DT.popCount))
     <> C.headed "PW Density"  (BR.toCell cas "Density" "Density" (BR.numberToStyledHtml "%2.1f" . view DT.pWPopPerSqMile))
     <> C.headed "T" (BR.toCell cas "T" "T" (BR.numberToStyledHtml "%2.2f" . (100*) . view MR.modelT))
     <> C.headed "State T" (BR.toCell cas "State T" "State T" (BR.numberToStyledHtml "%2.2f" . (100*) . view stateModelT))
     <> C.headed "P" (BR.toCell cas "P" "P" (BR.numberToStyledHtml "%2.2f" . (100*) . view MR.modelP))
     <> C.headed "State P" (BR.toCell cas "State P" "State P" (BR.numberToStyledHtml "%2.2f" . (100*) . view stateModelP))


combineDistrictPSMaps :: forall ks r rSM rDM rAllM rAll .
                         (K.KnitEffects r
                         , FJ.CanLeftJoinWithMissing ks (ks V.++ '[StateModelT]) (ks V.++ '[StateModelP])
                         , rSM ~ FJ.JoinResult ks (ks V.++ '[StateModelT]) (ks V.++ '[StateModelP])
                         , FJ.CanLeftJoinWithMissing (AndSLDKey ks) (AndSLDKey2 ks '[MR.ModelT]) (AndSLDKey2 ks '[MR.ModelP])
                         , rDM ~ FJ.JoinResult  (AndSLDKey ks) (AndSLDKey2 ks '[MR.ModelT]) (AndSLDKey2 ks '[MR.ModelP])
                         , FJ.CanLeftJoinWithMissing ks rDM rSM
                         , rAllM ~ FJ.JoinResult ks rDM rSM
                         , FJ.CanLeftJoinWithMissing (AndSLDKey ks) rAllM (AndSLDKey2 ks '[DT.PopCount, DT.PWPopPerSqMile])
                         , rAll ~ FJ.JoinResult (AndSLDKey ks) rAllM (AndSLDKey2 ks '[DT.PopCount, DT.PWPopPerSqMile])
                         , FSI.RecVec (ks V.++ '[StateModelT])
                         , FSI.RecVec (ks V.++ '[StateModelP])
                         , FSI.RecVec (AndSLDKey2 ks '[MR.ModelT])
                         , FSI.RecVec (AndSLDKey2 ks '[MR.ModelP])
                         , Show (F.Record ks)
                         , Show (F.Record (AndSLDKey ks))
                         , SLDKeyAnd2 ks [MR.ModelT, MR.ModelP, StateModelT, StateModelP, DT.PopCount, DT.PWPopPerSqMile] F.⊆ rAll
                         , Ord (F.Record (AndSLDKey ks))
                         , AndSLDKey ks F.⊆ DP.PSDataR SLDKeyR
                         , FSI.RecVec (AndSLDKey2 ks [DT.PopCount, DT.PWPopPerSqMile])
                         )
                      => MC.PSMap ks MT.ConfidenceInterval
                      -> MC.PSMap (AndSLDKey ks) MT.ConfidenceInterval
                      -> MC.PSMap ks MT.ConfidenceInterval
                      -> MC.PSMap (AndSLDKey ks) MT.ConfidenceInterval
                      -> DP.PSData SLDKeyR
--                      -> K.Sem r (F.FrameRec (FJ.JoinResult (AndSLDKey ks) (AndSLDKey2 ks '[MR.ModelT]) (AndSLDKey2 ks '[MR.ModelP])))
                      -> K.Sem r (F.FrameRec (SLDKeyAnd2 ks '[MR.ModelT, MR.ModelP, StateModelT, StateModelP, DT.PopCount, DT.PWPopPerSqMile]))
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
  pure $ fmap F.rcast all


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
                     => BR.CellStyle (F.Record rs) [Char] -> C.Colonnade C.Headed (F.Record rs) K.Cell
distSummaryColonnade cas =
  let state = F.rgetField @GT.StateAbbreviation
--      frac45AndOver r = r ^. DP.frac45To64 + r ^. DP.frac65plus
      share50 = MT.ciMid . F.rgetField @MR.ModelCI
  in C.headed "State" (BR.toCell cas "State" "State" (BR.textToStyledHtml . state))
     <> C.headed "District" (BR.toCell cas "District" "District" (BR.textToStyledHtml . fullDNameText))
     <> C.headed "Historical" (BR.toCell cas "Historical" "Historical" (BR.numberToStyledHtml "%2.2f" . (100*) . F.rgetField @ET.DemShare))
     <> C.headed "Model" (BR.toCell cas "50%" "50%" (BR.numberToStyledHtml "%2.2f" . (100*) . share50))
     <> C.headed "PW Density"  (BR.toCell cas "Density" "Density" (BR.numberToStyledHtml "%2.1f" . view DT.pWPopPerSqMile))
     <> C.headed "% Over 45"  (BR.toCell cas "% Over 45" "% Over 45" (BR.numberToStyledHtml "%2.1f" . view DP.frac45AndOver))
     <> C.headed "% College Grad"  (BR.toCell cas "% Grad" "% Grad" (BR.numberToStyledHtml "%2.1f" . view DP.fracCollegeGrad))
     <> C.headed "% NH White"  (BR.toCell cas "% NH White" "% NH White" (BR.numberToStyledHtml "%2.1f" . view DP.fracWhite))
     <> C.headed "% Black"  (BR.toCell cas "% Black" "% Black" (BR.numberToStyledHtml "%2.1f" . view DP.fracBlack))
     <> C.headed "% Hispanic"  (BR.toCell cas "% Hispanic" "% Hispanic" (BR.numberToStyledHtml "%2.1f" . view DP.fracHispanic))
     <> C.headed "% AAPI"  (BR.toCell cas "% AAPI" "% AAPI" (BR.numberToStyledHtml "%2.1f" . view DP.fracAAPI))
     <> C.headed "% Grad Of NWH"  (BR.toCell cas "% Grad of NWH" "% Grad of NWH" (BR.numberToStyledHtml "%2.1f" . view DP.fracGradOfWhite))



sldTableCellStyle :: FC.ElemsOf rs '[GT.StateAbbreviation, GT.DistrictTypeC, GT.DistrictName, MR.ModelCI, ET.DemShare]
                  => Text -> BR.CellStyle (F.Record rs) String
sldTableCellStyle state =
  let dlcc = fromMaybe [] $ M.lookup state dlccMap
      toDistrict (x, y, _) = (x, y)
      isDLCC r = (r ^. GT.districtTypeC, r ^. GT.districtName) `elem` fmap toDistrict dlcc
      bordered c = "border: 3px solid " <> c
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

sldColonnade :: (FC.ElemsOf rs [GT.StateAbbreviation, GT.DistrictTypeC, GT.DistrictName, MR.ModelCI, ET.DemShare])
             => BR.CellStyle (F.Record rs) [Char] -> C.Colonnade C.Headed (F.Record rs) K.Cell
sldColonnade cas =
  let state = F.rgetField @GT.StateAbbreviation
      share5 = MT.ciLower . F.rgetField @MR.ModelCI
      share50 = MT.ciMid . F.rgetField @MR.ModelCI
      share95 = MT.ciUpper . F.rgetField @MR.ModelCI
  in C.headed "State" (BR.toCell cas "State" "State" (BR.textToStyledHtml . state))
     <> C.headed "District" (BR.toCell cas "District" "District" (BR.textToStyledHtml . fullDNameText))
     <> C.headed "Historical" (BR.toCell cas "Historical" "Historical" (BR.numberToStyledHtml "%2.2f" . (100*) . F.rgetField @ET.DemShare))
     <> C.headed "5%" (BR.toCell cas "5%" "5%" (BR.numberToStyledHtml "%2.2f" . (100*) . share5))
     <> C.headed "50%" (BR.toCell cas "50%" "50%" (BR.numberToStyledHtml "%2.2f" . (100*) . share50))
     <> C.headed "95%" (BR.toCell cas "95%" "95%" (BR.numberToStyledHtml "%2.2f" . (100*) . share95))

dTypeText :: F.ElemOf rs GT.DistrictTypeC => F.Record rs -> Text
dTypeText r = case (r ^. GT.districtTypeC) of
        GT.StateUpper -> "Upper"
        GT.StateLower -> "Lower"
        _ -> "Not State Leg!"

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

geoCompChart :: (K.KnitEffects r
                , FC.ElemsOf rs [MR.ModelCI, ET.DemShare, GT.DistrictName, GT.DistrictTypeC]
                , FSI.RecVec rs
                 )
              => BR.PostPaths Path.Abs
              -> BR.PostInfo
              -> Text
              -> Text
              -> FV.ViewConfig
              -> Text
              -> GT.DistrictType
              -> (Text, F.Record rs -> Double)
              -> F.FrameRec rs
              -> K.Sem r GV.VegaLite
geoCompChart pp pi chartID title vc sa dType (label, f) rows = do
  let colData r = [ ("District", GV.Str $ r ^. GT.districtName)
                  , ("Model" , GV.Number $ MT.ciMid $ r ^. MR.modelCI)
                  , ("Historical", GV.Number $ r ^. ET.demShare)
                  , (label, GV.Number $ f r) --100 * ((MT.ciMid $ r ^. MR.modelCI) -  r ^. ET.demShare))
                  ]
      jsonRows = FL.fold (VJ.rowsToJSON colData [] Nothing) $ F.filterFrame ((== dType) . view GT.districtTypeC)rows
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
      encQty = GV.color [GV.MName label, GV.MmType GV.Quantitative]
      encoding = (GV.encoding . encQty) []
      tLookup = (GV.transform . GV.lookup "properties.NAME" rowData "District" (GV.LuFields [label])) []
      mark = GV.mark GV.Geoshape [GV.MTooltip GV.TTData]
      projection = GV.projection [GV.PrType GV.Identity, GV.PrReflectY True]
  pure $ FV.configuredVegaLite vc [FV.title (title <> titleSuffix), geoData, tLookup, projection, encoding, mark]

modeledMapToFrame :: MC.PSMap SLDKeyR MT.ConfidenceInterval -> F.FrameRec ModeledR
modeledMapToFrame = F.toFrame . fmap (\(k, ci) -> k F.<+> FT.recordSingleton @MR.ModelCI ci) . M.toList . MC.unPSMap

modeledACSBySLD :: (K.KnitEffects r, BRK.CacheEffects r) => BR.CommandLine -> K.Sem r (K.ActionWithCacheTime r (DP.PSData SLDKeyR))
modeledACSBySLD cmdLine = do
  (jointFromMarginalPredictorCSR_ASR_C, _, _) <- CDDP.cachedACSa5ByPUMA  ACS.acs1Yr2012_21 2021 -- most recent available
                                                 >>= DMC.predictorModel3 @'[DT.CitizenC] @'[DT.Age5C] @DMC.SRCA @DMC.SR
                                                 (Right "model/demographic/csr_asr_PUMA") "CSR_ASR_ByPUMA"
                                                 cmdLine . fmap (fmap F.rcast)
  (jointFromMarginalPredictorCASR_ASE_C, _, _) <- CDDP.cachedACSa5ByPUMA ACS.acs1Yr2012_21 2021 -- most recent available
                                                  >>= DMC.predictorModel3 @[DT.CitizenC, DT.Race5C] @'[DT.Education4C] @DMC.ASCRE @DMC.AS
                                                  (Right "model/demographic/casr_ase_PUMA") "CASR_SER_ByPUMA"
                                                  cmdLine . fmap (fmap F.rcast)
  (acsCASERBySLD, _products) <- BRC.censusTablesFor2022SLD_ACS2021
                                >>= DMC.predictedCensusCASER' (DTP.viaNearestOnSimplex) False "model/election2/sldDemographics"
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

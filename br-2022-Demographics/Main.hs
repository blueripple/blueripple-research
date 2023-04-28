{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UnicodeSyntax #-}
{-# LANGUAGE StrictData #-}

module Main
  (main)
where

import qualified BlueRipple.Configuration as BR
import qualified BlueRipple.Model.Demographic.StanModels as SM
import qualified BlueRipple.Model.Demographic.DataPrep as DDP
import qualified BlueRipple.Model.Demographic.EnrichData as DED
import qualified BlueRipple.Model.Demographic.TableProducts as DTP
import qualified BlueRipple.Model.Demographic.TPModel1 as DTM1
import qualified BlueRipple.Model.Demographic.TPModel2 as DTM2
import qualified BlueRipple.Model.Demographic.MarginalStructure as DMS
import qualified BlueRipple.Model.Demographic.BLCorrModel as DBLC
import qualified BlueRipple.Data.Keyed as Keyed

--import qualified BlueRipple.Data.ACS_PUMS as PUMS
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.GeographicTypes as GT
import qualified BlueRipple.Data.DataFrames as BRDF
import qualified BlueRipple.Utilities.KnitUtils as BRK

import qualified Stan.ModelBuilder.DesignMatrix as DM
import qualified Stan.ModelConfig as SC
import qualified Stan.ModelRunner as SMR
import qualified Stan.RScriptBuilder as SR

import qualified Knit.Report as K
import qualified Knit.Effect.AtomicCache as KC
import qualified Text.Pandoc.Error as Pandoc
import qualified System.Console.CmdArgs as CmdArgs
import qualified Colonnade as C

import qualified Data.Map.Strict as M
import qualified Data.Map.Merge.Strict as MM
import qualified Data.Set as S
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Storable as VS
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V
import qualified Numeric
import qualified Numeric.LinearAlgebra as LA
import qualified Control.Foldl as FL
import qualified Frames as F
import qualified Frames.Melt as F
import qualified Frames.Streamly.InCore as FSI
import qualified Frames.Transform as FT
import qualified Frames.MapReduce as FMR
import qualified Frames.Folds as FF


import Control.Lens (view, (^.))

import Path (Dir, Rel)
import qualified Path

--import qualified Frames.Visualization.VegaLite.Data as FVD
import qualified Text.Printf as PF
import qualified Graphics.Vega.VegaLite as GV
import qualified Graphics.Vega.VegaLite.Compat as FV
import qualified Graphics.Vega.VegaLite.Configuration as FV
import qualified Graphics.Vega.VegaLite.JSON as VJ
--import Data.Monoid (Sum(getSum))

templateVars ∷ M.Map String String
templateVars =
  M.fromList
    [ ("lang", "English")
    , ("site-title", "Blue Ripple Politics")
    , ("home-url", "https://www.blueripplepolitics.org")
    --  , ("author"   , T.unpack yamlAuthor)
    ]

pandocTemplate ∷ K.TemplatePath
pandocTemplate = K.FullySpecifiedTemplatePath "pandoc-templates/blueripple_basic.html"

data CountWithDensity = CountWithDensity { cwdN :: Int, cwdD ::  Double} deriving stock (Show, Eq)

instance Semigroup CountWithDensity where
  (CountWithDensity n1 d1) <> (CountWithDensity n2 d2) = CountWithDensity sumN avgDens
    where
      sumN = n1 + n2
      avgDens = (realToFrac n1 * d1 + realToFrac n2 * d2) / realToFrac sumN

instance Monoid CountWithDensity where
  mempty = CountWithDensity 0 0

recToCWD :: F.Record [DT.PopCount, DT.PWPopPerSqMile] -> CountWithDensity
recToCWD r = CountWithDensity (r ^. DT.popCount) (r ^. DT.pWPopPerSqMile)

cwdToRec :: CountWithDensity -> F.Record [DT.PopCount, DT.PWPopPerSqMile]
cwdToRec (CountWithDensity n d) = n F.&: d F.&: V.RNil

updateCWDCount :: Int -> CountWithDensity -> CountWithDensity
updateCWDCount n (CountWithDensity _ d) = CountWithDensity n d


--type InputFrame ks = F.FrameRec (ks V.++ '[DT.PopCount, DT.PWPopPerSqMile])
type ResultFrame ks = F.FrameRec (ks V.++ '[DT.PopCount])


data MethodResults ks =
  MethodResults
  { mrActual :: ResultFrame ks
  , mrProduct :: Maybe (ResultFrame ks)
  , mrProductWithNS :: Maybe (ResultFrame ks)
  , mrModel :: Maybe (ResultFrame ks)
  , mrModelWithCO :: Maybe (ResultFrame ks)
  }


compareResults :: forall ks key colKey rowKey r .
                  (K.KnitOne r
                  , Keyed.FiniteSet colKey
                  , Show colKey
                  , Ord colKey
                  , Keyed.FiniteSet rowKey
                  , Show rowKey
                  , Ord rowKey
                  , Ord (F.Record ks)
                  , V.RecordToList ks
                  , ks F.⊆ (ks V.++ '[DT.PopCount])
                  , V.ReifyConstraint Show F.ElField ks
                  , V.RMap ks
                  , F.ElemOf (ks V.++ '[DT.PopCount]) DT.PopCount
                  , F.ElemOf ks GT.StateAbbreviation
                  , F.ElemOf ks  GT.CongressionalDistrict
                  , FSI.RecVec (ks V.++ '[DT.PopCount])
                  , F.ElemOf (ks V.++ '[DT.PopCount]) GT.StateAbbreviation
                  )
               => BR.PostPaths Path.Abs
               -> BR.PostInfo
               -> Map Text Int -- populations
               -> Maybe Text
               -> (F.Record (ks V.++ '[DT.PopCount]) -> key)
               -> (F.Record (ks V.++ '[DT.PopCount]) -> rowKey)
               -> (F.Record (ks V.++ '[DT.PopCount]) -> colKey)
               -> (F.Record ks -> Text)
               -> Maybe (Text, F.Record ks -> Text, Maybe [Text])
               -> Maybe (Text, F.Record ks -> Text, Maybe [Text])
               -> MethodResults ks
               -> K.Sem r ()
compareResults pp pi' cdPopMap exampleStateM catKey rowKey colKey showCellKey colorM shapeM results = do
  let tableText d = toText (C.ascii (fmap toString $ mapColonnade)
                             $ FL.fold (fmap DED.totaledTable
                                         $ DED.rowMajorMapFldInt
                                         (view DT.popCount)
                                         rowKey
                                         colKey
                                       )
                             d
                           )
      allCDs = M.keys cdPopMap
      numRows = length $ Keyed.elements @rowKey
      numCols = length $ Keyed.elements @colKey
      facetB = True
      asDiffB = False
      aspect = if asDiffB then 1.33 else 1
      hvHeight = if facetB then 100 else 500
      hvWidth = aspect * hvHeight
      hvPad = if facetB then 1 else 5
  let compChartM ff pctPop ls t l fM = do
        let logF = if pctPop then Numeric.log else Numeric.logBase 10
            title = case (ls, pctPop) of
              (False, False) -> t
              (True, False) -> t <> " (Log scale)"
              (False, True) -> t <> " (% Pop)"
              (True, True) -> t <> " (% Pop, Log scale)"
        case fM of
          Just f -> do
            vl <- distCompareChart pp pi'
                  (FV.ViewConfig hvWidth hvHeight hvPad)
                  title
                  (F.rcast @ks)
                  showCellKey
                  facetB
                  colorM
                  shapeM
                  asDiffB
                  ((if ls then logF else id) . realToFrac . view DT.popCount)
                  (if pctPop then (Just (DDP.districtKeyT, cdPopMap)) else Nothing)
                  ("Actual ACS", ff $ mrActual results)
                  (l, ff f)
            _ <- K.addHvega Nothing Nothing vl
            pure ()
          Nothing -> pure ()


--      compChartMF f pp ls
  let title x = if asDiffB then (x <> " - Actual vs " <> x) else (x <> " vs Actual" )
  case exampleStateM of
    Nothing -> pure ()
    Just sa -> do
      let ff = F.filterFrame (\r -> r ^. GT.stateAbbreviation == sa)
          tableTextMF t rfM = whenJust rfM $ \rf -> K.logLE K.Info $ t <> "\n" <> tableText (ff rf)
          stTitle x = title x <> ": " <> sa
      K.logLE K.Info $ "Example State=" <> sa
      tableTextMF "Actual" (Just $ mrActual results)
      tableTextMF "Product" (mrProduct results)
      tableTextMF "Product + NS" (mrProductWithNS results)
      tableTextMF "ModelOnly" (mrModel results)
      tableTextMF "Model + CO" (mrModelWithCO results)
      K.logLE K.Info "Building example state charts."
      compChartM ff False False (stTitle "Marginal Product") "Marginal Product" $ mrProduct results
      compChartM ff False False (stTitle "MP_NSM") "MP_NSM" $ mrProductWithNS results
      compChartM ff False False (stTitle "ModelOnly") "ModelOnly" $ mrModel results
      compChartM ff False False (stTitle "ModelWithCO") "ModelWithCO" $ mrModelWithCO results
      compChartM ff False True (stTitle "Marginal Product") "Marginal Product" $ mrProduct results
      compChartM ff False True (stTitle "MP_NSM") "MP_NSM" $ mrProductWithNS results
      compChartM ff False True (stTitle "ModelOnly") "ModelOnly" $ mrModel results
      compChartM ff False True (stTitle "ModelWithCO") "ModelWithCO" $ mrModelWithCO results
      pure ()

  -- compute KL divergences
{-
  let toVecF = VS.fromList . fmap snd . M.toList . FL.fold (FL.premap (\r -> (catKey r, realToFrac (r ^. DT.popCount))) FL.map)
      computeKL d cd = DED.klDiv' acsV dV where
        acsV = toVecF $ F.filterFrame ((== cd) . DDP.districtKeyT) (mrActual results)
        dV = toVecF $ F.filterFrame ((== cd) . DDP.districtKeyT) d
  let prodKLs = computeKL (mrProduct results) <$> allCDs
      prodNSKLs = computeKL (mrProductWithNS results) <$> allCDs
      modelKLs = computeKL (mrModel results) <$> allCDs
      modelCOKLs = computeKL (mrModelWithCO results) <$> allCDs
      forKLsTable = L.zip5 allCDs prodKLs prodNSKLs modelKLs modelCOKLs
  K.logLE K.Info "Divergence Table:"
  K.logLE K.Info $ "\n" <> toText (C.ascii (fmap toString $ klColonnade) forKLsTable)
-}
--  _ <- compChart True "Actual" $ mrActual results
  K.logLE K.Info "Building national charts."
  compChartM id False False (title "Marginal Product") "Marginal Product" $ mrProduct results
  compChartM id False False (title "MP_NSM") "MP_NSM" $ mrProductWithNS results
  compChartM id False False (title "ModelOnly") "ModelOnly" $ mrModel results
  compChartM id False False (title "ModelWithCO") "ModelWithCO" $ mrModelWithCO results
  compChartM id False True (title "Marginal Product") "Marginal Product" $ mrProduct results
  compChartM id False True (title "MP_NSM") "MP_NSM" $ mrProductWithNS results
  compChartM id False True (title "ModelOnly") "ModelOnly" $ mrModel results
  compChartM id False True (title "ModelWithCO") "ModelWithCO" $ mrModelWithCO results

  pure ()

type SER = [DT.SexC, DT.Education4C, DT.Race5C]
type ASR = [DT.Age4C, DT.SexC, DT.Race5C]
type ASER = [DT.Age4C, DT.SexC, DT.Education4C, DT.Race5C]

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
          { K.outerLogPrefix = Just "2022-Demographics"
          , K.logIf = BR.knitLogSeverity $ BR.logLevel cmdLine -- K.logDiagnostic
          , K.pandocWriterConfig = pandocWriterConfig
          , K.serializeDict = BRK.flatSerializeDict
          , K.persistCache = KC.persistStrictByteString (\t → toString (cacheDir <> "/" <> t))
          }
  resE ← K.knitHtmls knitConfig $ do
    K.logLE K.Info $ "Command Line: " <> show cmdLine
--    eduModelResult <- runEduModel False cmdLine (SM.ModelConfig () False SM.HCentered False) $ SM.designMatrixRowEdu
    K.logLE K.Info $ "Loading ACS data for each PUMA" <> show cmdLine
    acsByPUMA_C <- DDP.cachedACSByPUMA
    acsByPUMA <-  K.ignoreCacheTime acsByPUMA_C
    let msSER_ASR = DMS.reKeyMarginalStructure
                    (F.rcast @[DT.SexC, DT.Race5C, DT.Education4C, DT.Age4C])
                    (F.rcast @ASER)
                    $ DMS.combineMarginalStructuresF @'[DT.SexC, DT.Race5C] @'[DT.Education4C] @'[DT.Age4C]
                    DMS.identityMarginalStructure DMS.identityMarginalStructure
        marginalStructure = msSER_ASR
--    K.logLE K.Info $ "stencils=" <> show (DMS.msStencils marginalStructure)

    let numBLModelCats = S.size $ Keyed.elements @DT.Age4
        blcModelConfig :: DBLC.ModelConfig (F.Record '[GT.StateAbbreviation, GT.PUMA, DT.SexC, DT.Education4C, DT.Race5C, DT.PWPopPerSqMile]) (Const ())
        blcModelConfig = DBLC.ModelConfig numBLModelCats
                         (contramap F.rcast DBLC.designMatrixRow_1_S_E_R)
                         (DBLC.BetaHierNonCentered (DBLC.DiagonalCovariance))
                         True True
    _ <- DBLC.runProjModel False  cmdLine (DBLC.RunConfig (Just numBLModelCats) True)
         blcModelConfig
         (F.rcast @'[DT.Age4C])
         (F.rcast @'[GT.StateAbbreviation, GT.PUMA, DT.SexC, DT.Education4C, DT.Race5C])
         (const $ Const ())
    K.knitError "STOP after blCorrModel run"
    let projCovariancesFld =
          DTP.diffCovarianceFldMS
          (F.rcast @[GT.StateAbbreviation, GT.PUMA])
          (F.rcast @ASER)
          (realToFrac . view DT.popCount)
          marginalStructure
    K.logLE K.Info $ "Computing covariance matrix of projected differences."
    let (projMeans, projCovariances) = FL.fold projCovariancesFld acsByPUMA
        (eigVals, eigVecs) = LA.eigSH projCovariances
--    K.logLE K.Info $ "mean=" <> toText (DED.prettyVector projMeans)
--    K.logLE K.Info $ "cov=" <> toText (LA.disps 3 $ LA.unSym $ projCovariances)
    K.logLE K.Info $ "covariance eigenValues: " <> DED.prettyVector eigVals
    let nullSpaceVectors = DTP.nullSpaceVectorsMS marginalStructure
--    K.logLE K.Info $ "nullSpaceVectors=" <> toText (LA.dispf 4 nullSpaceVectors)
    let nvProjections = DTP.uncorrelatedNullVecsMS marginalStructure projCovariances
        testProjections  = nvProjections --DTP.baseNullVectorProjections marginalStructure
        cMatrix = DED.mMatrix (DMS.msNumCategories marginalStructure) (DMS.msStencils marginalStructure)
--    K.logLE K.Info $ "nvpProj=" <> toText (LA.disps 3 $ DTP.nvpProj nvProjections) <> "\nnvpRotl=" <> toText (LA.disps 3 $ DTP.nvpRot nvProjections)
    acsByCD_C <- DDP.cachedACSByCD
    acsByCD <-  K.ignoreCacheTime acsByCD_C
--    BRK.logFrame acsByCD
--    K.knitError "STOP"
    let cdPopMap = FL.fold (FL.premap (\r -> (DDP.districtKeyT r, r ^. DT.popCount)) $ FL.foldByKeyMap FL.sum) acsByCD
    let cdModelData = FL.fold
                      (DTM1.nullVecProjectionsModelDataFldCheck
                        marginalStructure
                        testProjections
                        (F.rcast @'[BRDF.Year, GT.StateAbbreviation, GT.CongressionalDistrict])
                        (F.rcast @ASER)
                        (realToFrac . view DT.popCount)
                        DTM1.aserModelDatFld
                      )
                      acsByCD
    K.logLE K.Info $ "Running model, if necessary."
    let modelConfig = DTM1.ModelConfig testProjections True
                      DTM1.designMatrixRowASER DTM1.AlphaHierNonCentered DTM1.NormalDist DTM1.aserModelFuncs
    res_C <- DTM1.runProjModel @ASER False cmdLine (DTM1.RunConfig False False) modelConfig marginalStructure DTM1.aserModelDatFld
    modelRes <- K.ignoreCacheTime res_C

    K.logLE K.Info $ "Running model2, if necessary."
    let model2Config = DTM2.ModelConfig testProjections True
                       DTM2.designMatrixRow_1 DTM2.designMatrixRow0 DTM2.AlphaSimple DTM2.NormalDist
    res2_C <- DTM2.runProjModel False (Just 100) cmdLine (DTM2.RunConfig False False) model2Config marginalStructure (const DTM2.PModel0)

    K.knitError "STOP"
    forM_ cdModelData $ \(sar, md, nVpsActual, pV, nV) -> do
      let keyT = DDP.districtKeyT sar
          n = VS.sum pV

      when (BR.logLevel cmdLine >= BR.LogDebugMinimal) $ do
--        K.logLE K.Info $ keyT <> " nvps (actual) =" <> DED.prettyVector nVpsActual
        K.logLE K.Info $ keyT <> " actual  counts=" <> DED.prettyVector nV <> " (" <> show (VS.sum nV) <> ")"
        K.logLE K.Info $ keyT <> " prod    counts=" <> DED.prettyVector pV <> " (" <> show (VS.sum pV) <> ")"
        K.logLE K.Info $ keyT <> " nvps counts   =" <> DED.prettyVector (DTP.applyNSPWeights testProjections (VS.map (* n) nVpsActual) pV)
        K.logLE K.Info $ keyT <> " C * (actual - prod) =" <> DED.prettyVector (cMatrix LA.#> (nV - pV))
        K.logLE K.Info $ keyT <> " predictors: " <> show md
        nvpsModeled <- VS.fromList <$> (K.knitEither $ DTM1.modelResultNVPs DTM1.aserModelFuncs modelRes (sar ^. GT.stateAbbreviation) md)
        K.logLE K.Info $ keyT <> " modeled  =" <> DED.prettyVector nvpsModeled
        let simplexFull = DTP.projectToSimplex $ DTP.applyNSPWeights testProjections nvpsModeled (VS.map (/ VS.sum pV) pV)
            simplexNVPs = DTP.fullToProj testProjections simplexFull
--        nvpsOptimal <- DED.mapPE $ DTP.optimalWeights testProjections nvpsModeled (VS.map (/ n) pV)
        K.logLE K.Info $ keyT <> " onSimplex=" <> DED.prettyVector simplexNVPs
        K.logLE K.Info $ keyT <> " modeled counts=" <> DED.prettyVector (VS.map (* VS.sum pV) simplexFull)

    let vecToFrame ok ks v = fmap (\(k, c) -> ok F.<+> k F.<+> FT.recordSingleton @DT.PopCount (round c)) $ zip ks (VS.toList v)
        smcRowToProdAndModeled (ok, md, _, pV, _) = do
          let n = VS.sum pV --realToFrac $ DMS.msNumCategories marginalStructure
          nvpsModeled <-  VS.fromList <$> (K.knitEither $ DTM1.modelResultNVPs DTM1.aserModelFuncs modelRes (view GT.stateAbbreviation ok) md)
          let simplexFull = VS.map (* n) $ DTP.projectToSimplex $ DTP.applyNSPWeights testProjections nvpsModeled (VS.map (/ n) pV)
--            simplexNVPs = DTP.fullToProj testProjections simplexFull
--          nvpsOptimal <- DED.mapPE $ DTP.optimalWeights testProjections nvpsModeled (VS.map (/ n) pV)
          let mV = simplexFull --DTP.applyNSPWeights testProjections (VS.map (* n) nvpsOptimal) pV
          pure (ok, pV, mV)
    prodAndModeled <- traverse smcRowToProdAndModeled cdModelData
    let prodAndModeledToFrames ks (ok, pv, mv) = (vecToFrame ok ks pv, vecToFrame ok ks mv)
        (product_SER_ASR, productNS_SER_ASR) =
          first (F.toFrame . concat)
          $ second (F.toFrame . concat)
          $ unzip
          $ fmap (prodAndModeledToFrames (S.toList $ Keyed.elements @(F.Record ASER))) prodAndModeled
        addSimpleAge r = FT.recordSingleton @DT.SimpleAgeC (DT.age4ToSimple $ r ^. DT.age4C) F.<+> r
        aggregateAge = FMR.concatFold
                       $ FMR.mapReduceFold
                       (FMR.Unpack $ \r -> [addSimpleAge r])
                       (FMR.assignKeysAndData @[BRDF.Year, GT.StateAbbreviation, GT.CongressionalDistrict, DT.SimpleAgeC, DT.SexC, DT.Education4C, DT.Race5C]
                         @'[DT.PopCount])
                       (FMR.foldAndAddKey $ FF.foldAllConstrained @Num FL.sum)
        product_SER_A2SR = FL.fold aggregateAge product_SER_ASR
        productNS_SER_A2SR = FL.fold aggregateAge productNS_SER_ASR
--      smcToProdFrame ks (ok, _, _, pv, nv) = zip
--      productFrame = F.toFrame $ fmap (

--    K.ignoreCacheTime res_C >>= \r -> K.logLE K.Info ("result=" <> show r)


--    K.knitEither $ Left "Stopping before products, etc."

    K.logLE K.Info $ "Loading ACS data and building marginal distributions (SER, ASR, CSR) for each CD" <> show cmdLine
    let zc :: F.Record '[DT.PopCount, DT.PWPopPerSqMile] = 0 F.&: 0 F.&: V.RNil
        acsSampleWZ_C = fmap (FL.fold
                              (FMR.concatFold
                               $ FMR.mapReduceFold
                               FMR.noUnpack
                               (FMR.assignKeysAndData @[BRDF.Year, GT.StateAbbreviation, GT.CongressionalDistrict]
                                @[DT.CitizenC, DT.Age4C, DT.SexC, DT.Education4C, DT.Race5C, DT.PopCount, DT.PWPopPerSqMile])
                               (FMR.foldAndLabel
                                (Keyed.addDefaultRec @[DT.CitizenC, DT.Age4C, DT.SexC, DT.Education4C, DT.Race5C] zc)
                                (\k r -> fmap (k F.<+>) r)
                               )
                              )
                             )
                      $ acsByCD_C
    acsSampleWZ <- K.ignoreCacheTime acsSampleWZ_C
    let allStates = S.toList $ FL.fold (FL.premap (view GT.stateAbbreviation) FL.set) acsByCD
        allCDs = S.toList $ FL.fold (FL.premap DDP.districtKey FL.set) acsByCD
        emptyUnless x y = if x then y else mempty
        logText t k = Nothing --Just $ t <> ": Joint distribution matching for " <> show k
        zeroPopAndDens :: F.Record [DT.PopCount, DT.PWPopPerSqMile] = 0 F.&: 0 F.&: V.RNil
        tableMatchingDataFunctions = DED.TableMatchingDataFunctions zeroPopAndDens recToCWD cwdToRec cwdN updateCWDCount
--        exampleState = "KY"
        rerunMatches = False
    -- SER to A2SER
    let toOutputRow :: ([BRDF.Year, GT.StateAbbreviation, GT.CongressionalDistrict, DT.Age4C, DT.SexC, DT.Education4C, DT.Race5C, DT.PopCount]  F.⊆ rs)
                    => F.Record rs -> F.Record [BRDF.Year, GT.StateAbbreviation, GT.CongressionalDistrict, DT.Age4C, DT.SexC, DT.Education4C, DT.Race5C, DT.PopCount]
        toOutputRow = F.rcast
    let toOutputRow2 :: ([BRDF.Year, GT.StateAbbreviation, GT.CongressionalDistrict, DT.SimpleAgeC, DT.SexC, DT.Education4C, DT.Race5C, DT.PopCount]  F.⊆ rs)
                    => F.Record rs -> F.Record [BRDF.Year, GT.StateAbbreviation, GT.CongressionalDistrict, DT.SimpleAgeC, DT.SexC, DT.Education4C, DT.Race5C, DT.PopCount]
        toOutputRow2 = F.rcast
    -- actual
    let acsSampleASER_C = F.toFrame
                         . FL.fold (FMR.concatFold
                                    $ FMR.mapReduceFold
                                    FMR.noUnpack
                                    (FMR.assignKeysAndData @[BRDF.Year, GT.StateAbbreviation, GT.CongressionalDistrict, DT.Age4C, DT.SexC, DT.Education4C, DT.Race5C]
                                     @'[DT.PopCount, DT.PWPopPerSqMile])
                                    (FMR.foldAndAddKey DDP.aggregatePeopleAndDensityF)
                                   )
                         <$> acsSampleWZ_C
    acsSampleASER <- K.ignoreCacheTime acsSampleASER_C

    let acsSampleA2SER_C = F.toFrame
                           . FL.fold (FMR.concatFold
                                      $ FMR.mapReduceFold
                                      (FMR.Unpack $ \r -> [FT.recordSingleton @DT.SimpleAgeC (DT.age4ToSimple $ r ^. DT.age4C) F.<+> r])
                                      (FMR.assignKeysAndData @[BRDF.Year, GT.StateAbbreviation, GT.CongressionalDistrict, DT.SimpleAgeC, DT.SexC, DT.Education4C, DT.Race5C]
                                       @'[DT.PopCount, DT.PWPopPerSqMile])
                                      (FMR.foldAndAddKey DDP.aggregatePeopleAndDensityF)
                                     )
                           <$> acsSampleWZ_C
    acsSampleA2SER <- K.ignoreCacheTime acsSampleA2SER_C

    -- marginals, SER and CSR
    let acsSampleSER_C = F.toFrame
                         . FL.fold (FMR.concatFold
                                    $ FMR.mapReduceFold
                                    FMR.noUnpack
                                    (FMR.assignKeysAndData @[BRDF.Year, GT.StateAbbreviation, GT.CongressionalDistrict, DT.SexC, DT.Education4C, DT.Race5C]
                                     @'[DT.PopCount, DT.PWPopPerSqMile])
                                    (FMR.foldAndAddKey DDP.aggregatePeopleAndDensityF)
                                   )
                         <$> acsSampleWZ_C
    acsSampleSER <- K.ignoreCacheTime acsSampleSER_C
{-
    let acsSampleCSR_C = F.toFrame
                         . FL.fold (FMR.concatFold
                                    $ FMR.mapReduceFold
                                    FMR.noUnpack
                                    (FMR.assignKeysAndData @[BRDF.Year, GT.StateAbbreviation, GT.CongressionalDistrict, DT.CitizenC, DT.SexC, DT.Race5C]
                                     @[DT.PopCount, DT.PWPopPerSqMile])
                                    (FMR.foldAndAddKey DDP.aggregatePeopleAndDensityF)
                                   )
                         <$> acsSampleWZ_C
    acsSampleCSR <- K.ignoreCacheTime acsSampleCSR_C
-}
    let acsSampleASR_C = F.toFrame
                         . FL.fold (FMR.concatFold
                                    $ FMR.mapReduceFold
                                    FMR.noUnpack
                                    (FMR.assignKeysAndData @[BRDF.Year, GT.StateAbbreviation, GT.CongressionalDistrict, DT.Age4C, DT.SexC, DT.Race5C]
                                     @[DT.PopCount, DT.PWPopPerSqMile])
                                    (FMR.foldAndAddKey DDP.aggregatePeopleAndDensityF)
                                   )
                         <$> acsSampleWZ_C
    acsSampleASR <- K.ignoreCacheTime acsSampleASR_C

    -- model & match pipeline
    a2FromSER_C <- SM.runModel
                   False cmdLine (SM.ModelConfig () False SM.HCentered True)
                   ("Age", DT.age4ToSimple . view DT.age4C)
                   ("SER", F.rcast @[DT.SexC, DT.Education4C, DT.Race5C], SM.dmrS_ER)
{-
    cFromSER_C <- SM.runModel
                  False cmdLine (SM.ModelConfig () False SM.HCentered True)
                  ("Cit", view DT.citizenC)
                  ("SER", F.rcast @[DT.SexC, DT.Education4C, DT.Race5C], SM.dmrS_ER)
-}
    let a2srRec :: (DT.SimpleAge, DT.Sex, DT.Race5) -> F.Record [DT.SimpleAgeC, DT.SexC, DT.Race5C]
        a2srRec (c, s, r) = c F.&: s F.&: r F.&: V.RNil
        a2srDSFld =  DED.desiredSumsFld
                    (Sum . F.rgetField @DT.PopCount)
                    (F.rcast @'[BRDF.Year, GT.StateAbbreviation, GT.CongressionalDistrict])
                    (\r -> a2srRec (DT.age4ToSimple (r ^. DT.age4C), r ^. DT.sexC, r ^. DT.race5C))
        a2srDS = getSum <<$>> FL.fold a2srDSFld acsByCD

        serRec :: (DT.Sex, DT.Education4, DT.Race5) -> F.Record [DT.SexC, DT.Education4C, DT.Race5C]
        serRec (s, e, r) = s F.&: e F.&: r F.&: V.RNil
        serDSFld =  DED.desiredSumsFld
                    (Sum . F.rgetField @DT.PopCount)
                    (F.rcast @'[BRDF.Year, GT.StateAbbreviation, GT.CongressionalDistrict])
                    (\r -> serRec (r ^. DT.sexC, r ^. DT.education4C, r ^. DT.race5C))
        serDS = getSum <<$>> FL.fold serDSFld acsByCD

    let serToA2SER_PC eu = fmap
          (\m -> DED.pipelineStep @[DT.SexC, DT.Education4C, DT.Race5C] @'[DT.SimpleAgeC] @_ @_ @_ @DT.PopCount
                 tableMatchingDataFunctions
                 (DED.minConstrained DED.klPObjF)
                 (DED.enrichFrameFromBinaryModelF @DT.SimpleAgeC @DT.PopCount m (view GT.stateAbbreviation) DT.Under DT.EqualOrOver)
                 (logText "SER -> A2SER")
                 (emptyUnless eu
                   $ DED.desiredSumMapToLookup @[DT.SimpleAgeC, DT.SexC, DT.Education4C, DT.Race5C] serDS
                   <> DED.desiredSumMapToLookup @[DT.SimpleAgeC, DT.SexC, DT.Education4C, DT.Race5C] a2srDS)
          )
          a2FromSER_C

    modelA2SERFromSER <- fmap (fmap toOutputRow2)
                         $ BRK.clearIf' rerunMatches  "model/synthJoint/serToA2SER_MO.bin" >>=
                         \ck -> K.ignoreCacheTimeM $ BRK.retrieveOrMakeFrame ck
                                ((,) <$> serToA2SER_PC False <*> acsSampleSER_C)
                                $ \(p, d) -> DED.mapPE $ p d

    modelCO_A2SERFromSER <- fmap (fmap toOutputRow2)
                            $ BRK.clearIf' rerunMatches  "model/synthJoint/serToA2SER.bin" >>=
                            \ck -> K.ignoreCacheTimeM $ BRK.retrieveOrMakeFrame ck
                                   ((,) <$> serToA2SER_PC True <*> acsSampleSER_C)
                                   $ \(p, d) -> DED.mapPE $ p d

    let mResultsA2 :: MethodResults [BRDF.Year, GT.StateAbbreviation, GT.CongressionalDistrict, DT.SimpleAgeC, DT.SexC, DT.Education4C, DT.Race5C]
        mResultsA2 = MethodResults
                     (fmap toOutputRow2 acsSampleA2SER)
                     (Just product_SER_A2SR)
                     (Just productNS_SER_A2SR)
                     (Just modelA2SERFromSER)
                     (Just modelCO_A2SERFromSER)

    let mResults :: MethodResults [BRDF.Year, GT.StateAbbreviation, GT.CongressionalDistrict, DT.Age4C, DT.SexC, DT.Education4C, DT.Race5C]
        mResults = MethodResults (fmap toOutputRow acsSampleASER) (Just product_SER_ASR) (Just productNS_SER_ASR) Nothing Nothing

    let postInfo = BR.PostInfo (BR.postStage cmdLine) (BR.PubTimes BR.Unpublished Nothing)
    synthModelPaths <- postPaths "SynthModel" cmdLine
    BRK.brNewPost synthModelPaths postInfo "SynthModel" $ do
      let showCellKeyA2 r = show (r ^. GT.stateAbbreviation, r ^. DT.simpleAgeC, r ^. DT.sexC, r ^. DT.education4C, r ^. DT.race5C)
          edOrder =  show <$> S.toList (Keyed.elements @DT.Education4)
          ageOrder = show <$> S.toList (Keyed.elements @DT.Age4)
          simpleAgeOrder = show <$> S.toList (Keyed.elements @DT.SimpleAge)
      compareResults synthModelPaths postInfo cdPopMap (Just "NY")
        (F.rcast @[DT.SimpleAgeC, DT.SexC, DT.Education4C, DT.Race5C])
        (\r -> (r ^. DT.sexC, r ^. DT.race5C))
        (\r -> (r ^. DT.simpleAgeC, r ^. DT.education4C))
        showCellKeyA2
        (Just ("Education", show . view DT.education4C, Just edOrder))
        (Just ("SimpleAge", show . view DT.simpleAgeC, Just simpleAgeOrder))
        mResultsA2

      let showCellKey r = show (r ^. GT.stateAbbreviation, r ^. DT.age4C, r ^. DT.sexC, r ^. DT.education4C, r ^. DT.race5C)
      compareResults synthModelPaths postInfo cdPopMap (Just "NY")
        (F.rcast @[DT.Age4C, DT.SexC, DT.Education4C, DT.Race5C])
        (\r -> (r ^. DT.sexC, r ^. DT.race5C))
        (\r -> (r ^. DT.age4C, r ^. DT.education4C))
        showCellKey
        (Just ("Education", show . view DT.education4C, Just edOrder))
        (Just ("Age", show . view DT.age4C, Just ageOrder))
        mResults


    pure ()
  case resE of
    Right namedDocs →
      K.writeAllPandocResultsWithInfoAsHtml "" namedDocs
    Left err → putTextLn $ "Pandoc Error: " <> Pandoc.renderError err

klColonnade :: C.Colonnade C.Headed (Text, Double, Double, Double, Double) Text
klColonnade =
  let sa (x, _, _, _, _) = x
      serProd (_, x, _, _, _) = x
      serProdNS (_, _, x, _, _) = x
      serModel (_, _, _, x, _) = x
      serModelCO (_, _, _, _, x) = x
      fmtX :: Double -> Text
      fmtX x = toText @String $ PF.printf "%2.2e" x
  in C.headed "State" sa
     <> C.headed "Product" (fmtX . serProd)
     <> C.headed "Error (%)" (toText @String . PF.printf "%2.0g" . (100 *) . sqrt . (2 *) . serProd)
     <> C.headed "Product + NS" (fmtX . serProdNS)
     <> C.headed "Error (%)" (toText @String . PF.printf "%2.0g" . (100 *) . sqrt . (2 *) . serProdNS)
     <> C.headed "Model Only" (fmtX . serModel)
     <> C.headed "Error (%)" (toText @String . PF.printf "%2.0g" . (100 *) . sqrt . (2 *) . serModel)
     <> C.headed "Model + Matching" (fmtX . serModelCO)
     <> C.headed "Error (%)" (toText @String . PF.printf "%2.0g" . (100 *) . sqrt . (2 *) . serModelCO)


mapColonnade :: (Show a, Show b, Ord b, Keyed.FiniteSet b) => C.Colonnade C.Headed (a, Map b Int) Text
mapColonnade = C.headed "" (show . fst)
               <> mconcat (fmap (\b -> C.headed (show b) (fixMaybe . M.lookup b . snd)) (S.toAscList $ Keyed.elements))
               <>  C.headed "Total" (fixMaybe . sumM) where
  fixMaybe = maybe "0" show
  sumM x = fmap (FL.fold FL.sum) <$> traverse (\b -> M.lookup b (snd x)) $ S.toList $ Keyed.elements


-- emptyRel = [Path.reldir||]
postDir ∷ Path.Path Rel Dir
postDir = [Path.reldir|br-2022-Demographics/posts|]

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
postOnline p = [Path.reldir|research/Demographics|] BR.</> p

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

logLengthC :: (K.KnitEffects r, Foldable f) => K.ActionWithCacheTime r (f a) -> Text -> K.Sem r ()
logLengthC xC t = K.ignoreCacheTime xC >>= \x -> K.logLE K.Info $ t <> " has " <> show (FL.fold FL.length x) <> " rows."

distCompareChart :: (Ord k, Show k, Show a, Ord a, K.KnitEffects r)
                 => BR.PostPaths Path.Abs
                 -> BR.PostInfo
                 -> FV.ViewConfig
                 -> Text
                 -> (F.Record rs -> k) -- key for map
                 -> (k -> Text) -- description for tooltip
                 -> Bool
                 -> Maybe (Text, k -> Text, Maybe [Text]) -- category for color
                 -> Maybe (Text, k -> Text, Maybe [Text]) -- category for shape
                 -> Bool
                 -> (F.Record rs -> Double)
                 -> Maybe (k -> a, Map a Int) -- pop counts to divide by
                 -> (Text, F.FrameRec rs)
                 -> (Text, F.FrameRec rs)
                 -> K.Sem r GV.VegaLite
distCompareChart pp pi' vc title key keyText facetB cat1M cat2M asDiffB count scalesM (actualLabel, actualRows) (synthLabel, synthRows) = do
  let assoc r = (key r, count r)
      toMap = FL.fold (FL.premap assoc FL.map)
      whenMatchedF _ aCount sCount = Right (aCount, sCount)
      whenMatched = MM.zipWithAMatched whenMatchedF
      whenMissingF t k _ = Left $ "Missing key=" <> show k <> " in " <> t <> " rows."
      whenMissingFromX = MM.traverseMissing (whenMissingF actualLabel)
      whenMissingFromY = MM.traverseMissing (whenMissingF synthLabel)
  mergedMap <- K.knitEither $ MM.mergeA whenMissingFromY whenMissingFromX whenMatched (toMap actualRows) (toMap synthRows)
--  let mergedList = (\(r1, r2) -> (key r1, (count r1, count r2))) <$> zip (FL.fold FL.list xRows) (FL.fold FL.list yRows)
  let diffLabel = actualLabel <> "-" <> synthLabel
      scaleErr :: Show a => a -> Text
      scaleErr a = "distCompareChart: Missing key=" <> show a <> " in scale lookup."
      scaleF = fmap realToFrac <$> case scalesM of
        Nothing -> const $ pure 1
        Just (keyF, scaleMap) -> \k -> let a = keyF k in maybeToRight (scaleErr a) $ M.lookup a scaleMap
  let rowToDataM (k, (xCount, yCount)) = do
        scale <- scaleF k
        pure $
          [ (actualLabel, GV.Number $ xCount / scale)
          , (synthLabel, GV.Number $ yCount / scale)
          , (diffLabel,  GV.Number $ (xCount - yCount) / scale)
          , ("Description", GV.Str $ keyText k)
          ]
          <> maybe [] (\(l, f, _) -> [(l, GV.Str $ f k)]) cat1M
          <> maybe [] (\(l, f, _) -> [(l, GV.Str $ f k)]) cat2M
  jsonRows <- K.knitEither $ FL.foldM (VJ.rowsToJSONM rowToDataM [] Nothing) (M.toList mergedMap)
  jsonFilePrefix <- K.getNextUnusedId "distCompareChart"
  jsonUrl <- BRK.brAddJSON pp pi' jsonFilePrefix jsonRows
--      toVLDataRowsM x = GV.dataRow <$> rowToDataM x <*> pure []
--  vlData <- GV.dataFromRows [] . concat <$> (traverse toVLDataRowsM $ M.toList mergedMap)
  let xLabel = if asDiffB then synthLabel else actualLabel
      yLabel =  if asDiffB then diffLabel else synthLabel
  let vlData = GV.dataFromUrl jsonUrl [GV.JSON "values"]
      encX = GV.position GV.X [GV.PName xLabel, GV.PmType GV.Quantitative, GV.PNoTitle]
      encY = GV.position GV.Y [GV.PName yLabel, GV.PmType GV.Quantitative, GV.PNoTitle]
      encXYLineY = GV.position GV.Y [GV.PName actualLabel, GV.PmType GV.Quantitative, GV.PNoTitle]
      orderIf sortT = maybe [] (pure . sortT . pure . GV.CustomSort . GV.Strings)
      encColor = maybe id (\(l, _, mOrder) -> GV.color ([GV.MName l, GV.MmType GV.Nominal] <> orderIf GV.MSort mOrder)) cat1M
      encShape = maybe id (\(l, _, mOrder) -> GV.shape ([GV.MName l, GV.MmType GV.Nominal] <> orderIf GV.MSort mOrder)) cat2M
      encTooltips = GV.tooltips $ [ [GV.TName xLabel, GV.TmType GV.Quantitative]
                                  , [GV.TName yLabel, GV.TmType GV.Quantitative]
                                  , maybe [] (\(l, _, _) -> [GV.TName l, GV.TmType GV.Nominal]) cat1M
                                  , maybe [] (\(l, _, _) -> [GV.TName l, GV.TmType GV.Nominal]) cat2M
                                  , [GV.TName "Description", GV.TmType GV.Nominal]
                                  ]
      mark = GV.mark GV.Point [GV.MSize 1]
      enc = GV.encoding . encX . encY . if facetB then id else encColor . encShape . encTooltips
      markXYLine = GV.mark GV.Line [GV.MColor "black", GV.MStrokeDash [5,3]]
      dataSpec = GV.asSpec [enc [], mark]
      xySpec = GV.asSpec [(GV.encoding . encX . encXYLineY) [], markXYLine]
      layers = GV.layer $ [dataSpec] <> if asDiffB then [] else [xySpec]
      facets = GV.facet $ (maybe [] (\(l, _, mOrder) -> [GV.RowBy ([GV.FName l, GV.FmType GV.Nominal] <> orderIf GV.FSort mOrder)]) cat1M)
                           <> (maybe [] (\(l, _, mOrder) -> [GV.ColumnBy ([GV.FName l, GV.FmType GV.Nominal] <> orderIf GV.FSort mOrder)]) cat2M)

  pure $ if facetB
         then FV.configuredVegaLite vc [FV.title title, facets, GV.specification (GV.asSpec [layers]), vlData]
         else FV.configuredVegaLite vc [FV.title title, layers, vlData]

{-
runCitizenModel :: (K.KnitEffects r, BRK.CacheEffects r)
                => Bool
                -> BR.CommandLine
                -> SM.ModelConfig ()
                -> DM.DesignMatrixRow (F.Record [DT.SexC, DT.Education4C, DT.Race5C])
                -> K.Sem r (SM.ModelResult Text [DT.SexC, DT.Education4C, DT.Race5C])
runCitizenModel clearCaches cmdLine mc dmr = do
  let cacheDirE = let k = "model/demographic/citizen/" in if clearCaches then Left k else Right k
      dataName = "acsCitizen_" <> DM.dmName dmr <> SM.modelConfigSuffix mc
      runnerInputNames = SC.RunnerInputNames
                         "br-2022-Demographics/stanCitizen"
                         ("normalSER_" <> DM.dmName dmr <> SM.modelConfigSuffix mc)
                         (Just $ SC.GQNames "pp" dataName)
                         dataName
      only2020 r = F.rgetField @BRDF.Year r == 2020
      _postInfo = BR.PostInfo (BR.postStage cmdLine) (BR.PubTimes BR.Unpublished Nothing)
  _ageModelPaths <- postPaths "CitizenModel" cmdLine
  acs_C <- DDP.cachedACSByState
--  K.ignoreCacheTime acs_C >>= BRK.logFrame
  logLengthC acs_C "acsByState"
  let acsMN_C = fmap DDP.acsByStateCitizenMN acs_C
      mcWithId = "normal" <$ mc
--  K.ignoreCacheTime acsMN_C >>= print
  logLengthC acsMN_C "acsByStateMNCit"
  states <- FL.fold (FL.premap (view GT.stateAbbreviation . fst) FL.set) <$> K.ignoreCacheTime acsMN_C
  (dw, code) <- SMR.dataWranglerAndCode acsMN_C (pure ())
                (SM.groupBuilderState (S.toList states))
                (SM.normalModel (contramap F.rcast dmr) mc)
  res <- do
    K.ignoreCacheTimeM
      $ SMR.runModel' @BRK.SerializerC @BRK.CacheData
      cacheDirE
      (Right runnerInputNames)
      dw
      code
      (SM.stateModelResultAction mcWithId dmr)
      (SMR.Both [SR.UnwrapNamed "successes" "yObserved"])
      acsMN_C
      (pure ())
  K.logLE K.Info "citizenModel run complete."
  pure res


runAgeModel :: (K.KnitEffects r, BRK.CacheEffects r)
            => Bool
            -> BR.CommandLine
            -> SM.ModelConfig ()
            -> DM.DesignMatrixRow (F.Record [DT.CitizenC, DT.SexC, DT.Education4C, DT.Race5C])
            -> K.Sem r (SM.ModelResult Text [DT.CitizenC, DT.SexC, DT.Education4C, DT.Race5C])
runAgeModel clearCaches cmdLine mc dmr = do
  let cacheDirE = let k = "model/demographic/age/" in if clearCaches then Left k else Right k
      dataName = "acsAge_" <> DM.dmName dmr <> SM.modelConfigSuffix mc
      runnerInputNames = SC.RunnerInputNames
                         "br-2022-Demographics/stanAge"
                         ("normalSER_" <> DM.dmName dmr <> SM.modelConfigSuffix mc)
                         (Just $ SC.GQNames "pp" dataName)
                         dataName
      only2020 r = F.rgetField @BRDF.Year r == 2020
      _postInfo = BR.PostInfo (BR.postStage cmdLine) (BR.PubTimes BR.Unpublished Nothing)
  _ageModelPaths <- postPaths "AgeModel" cmdLine
  acs_C <- DDP.cachedACSByState
--  K.ignoreCacheTime acs_C >>= BRK.logFrame
  logLengthC acs_C "acsByState"
  let acsMN_C = fmap DDP.acsByStateAgeMN acs_C
      mcWithId = "normal" <$ mc
--  K.ignoreCacheTime acsMN_C >>= print
  logLengthC acsMN_C "acsByStateMNAge"
  states <- FL.fold (FL.premap (view GT.stateAbbreviation . fst) FL.set) <$> K.ignoreCacheTime acsMN_C
  (dw, code) <- SMR.dataWranglerAndCode acsMN_C (pure ())
                (SM.groupBuilderState (S.toList states))
                (SM.normalModel (contramap F.rcast dmr) mc) --(SM.designMatrixRowAge SM.logDensityDMRP))
  res <- do
    K.ignoreCacheTimeM
      $ SMR.runModel' @BRK.SerializerC @BRK.CacheData
      cacheDirE
      (Right runnerInputNames)
      dw
      code
      (SM.stateModelResultAction mcWithId dmr)
      (SMR.Both [SR.UnwrapNamed "successes" "yObserved"])
      acsMN_C
      (pure ())
  K.logLE K.Info "ageModel run complete."
  pure res

runEduModel :: (K.KnitMany r, BRK.CacheEffects r)
            => Bool
            -> BR.CommandLine
            → SM.ModelConfig ()
            -> DM.DesignMatrixRow (F.Record [DT.CitizenC, DT.SexC, DT.Age4C, DT.Race5C])
            -> K.Sem r (SM.ModelResult Text [DT.CitizenC, DT.SexC, DT.Age4C, DT.Race5C])
runEduModel clearCaches cmdLine mc dmr = do
  let cacheDirE = let k = "model/demographic/edu/" in if clearCaches then Left k else Right k
      dataName = "acsEdu_" <> DM.dmName dmr <> SM.modelConfigSuffix mc
      runnerInputNames = SC.RunnerInputNames
                         "br-2022-Demographics/stanEdu"
                         ("normalSAR_" <> DM.dmName dmr <> SM.modelConfigSuffix mc)
                         (Just $ SC.GQNames "pp" dataName)
                         dataName
      only2020 r = F.rgetField @BRDF.Year r == 2020
      postInfo = BR.PostInfo (BR.postStage cmdLine) (BR.PubTimes BR.Unpublished Nothing)
  eduModelPaths <- postPaths "EduModel" cmdLine
  acs_C <- DDP.cachedACSByState
  logLengthC acs_C "acsByState"
  let acsMN_C = fmap DDP.acsByStateEduMN acs_C
  logLengthC acsMN_C "acsByStateMNEdu"
  states <- FL.fold (FL.premap (view GT.stateAbbreviation . fst) FL.set) <$> K.ignoreCacheTime acsMN_C
  (dw, code) <- SMR.dataWranglerAndCode acsMN_C (pure ())
                (SM.groupBuilderState (S.toList states))
                (SM.normalModel (contramap F.rcast dmr) mc) -- (Just SM.logDensityDMRP)
  let mcWithId = "normal" <$ mc
  acsMN <- K.ignoreCacheTime acsMN_C
  BRK.brNewPost eduModelPaths postInfo "EduModel" $ do
    _ <- K.addHvega Nothing Nothing $ chart (FV.ViewConfig 100 500 5) acsMN
    pure ()
  res <- do
    K.logLE K.Info "here"
    K.ignoreCacheTimeM
      $ SMR.runModel' @BRK.SerializerC @BRK.CacheData
      cacheDirE
      (Right runnerInputNames)
      dw
      code
      (SM.stateModelResultAction mcWithId dmr)
      (SMR.Both [SR.UnwrapNamed "successes" "yObserved"])
      acsMN_C
      (pure ())
  K.logLE K.Info "eduModel run complete."
--  K.logLE K.Info $ "result: " <> show res
  pure res
-}

chart :: Foldable f => FV.ViewConfig -> f DDP.ACSByStateEduMN -> GV.VegaLite
chart vc rows =
  let total v = v VU.! 0 + v VU.! 1
      grads v = v VU.! 1
      rowToData (r, v) = [("Sex", GV.Str $ show $ F.rgetField @DT.SexC r)
                         , ("State", GV.Str $ F.rgetField @GT.StateAbbreviation r)
                         , ("Age", GV.Str $ show $ F.rgetField @DT.Age4C r)
                         , ("Race", GV.Str $ show (F.rgetField @DT.Race5C r))
                         , ("Total", GV.Number $ realToFrac $ total v)
                         , ("Grads", GV.Number $ realToFrac $ grads v)
                         , ("FracGrad", GV.Number $ realToFrac (grads v) / realToFrac (total v))
                         ]
      toVLDataRows x = GV.dataRow (rowToData x) []
      vlData = GV.dataFromRows [] $ concat $ fmap toVLDataRows $ FL.fold FL.list rows
      encAge = GV.position GV.X [GV.PName "Age", GV.PmType GV.Nominal]
      encSex = GV.color [GV.MName "Sex", GV.MmType GV.Nominal]
      _encState = GV.row [GV.FName "State", GV.FmType GV.Nominal]
      encRace = GV.column [GV.FName "Race", GV.FmType GV.Nominal]
      encFracGrad = GV.position GV.Y [GV.PName "FracGrad", GV.PmType GV.Quantitative]
      encTotal = GV.size [GV.MName "Total", GV.MmType GV.Quantitative]
      mark = GV.mark GV.Circle []
      enc = (GV.encoding . encAge . encSex . encFracGrad . encRace . encTotal)
  in FV.configuredVegaLite vc [FV.title "FracGrad v Age", enc [], mark, vlData]


{-
applyMethods :: forall outerKs startKs addKs r .
                  (K.KnitEffects r
                  )
             -> Either Text Text
             -> (Bool -> K.ActionWithCacheTime r
                 (InputFrame (outerKs V.++ startKs) -> K.Sem r (ResultFrame (outerKs V.++ startKs V.++ addKs))) -- model/model+match pipeline
             -> K.ActionWithCacheTime r (InputFrame outerKs startKs addKs)
             -> K.Sem r (MethodResults outerKs startKs addKs)
applyMethods cacheKeyE mmPipelineF_C actual_C = do
  -- make sure we have zeroes
  let zc :: F.Record '[DT.PopCount, DT.PWPopPerSqMile] = 0 F.&: 0 F.&: V.RNil
      actualWZ_C =  fmap (FL.fold
                           (FMR.concatFold
                            $ FMR.mapReduceFold
                            (FMR.noUnpack)
                            (FMR.assignKeysAndData @outerKs @(startKs V.++ addKs V.++ [DT.PopCount, DT.PWPerSqMile])
                              (FMR.foldAndLabel
                                (Keyed.addDefaultRec @(startKs V.++ addKs) zc)
                                (\k r -> fmap (k F.<+>) r)
                              )
                            )
                           )
                         )
                    actual_C
  (rerun, cKey) <- case cacheKeyE of
    Left k -> (True, k)
    Right k -> (False, k)
  let modelOnlyDeps = (,) <$> mmPipelineF_C False, <*> actualWZ_C
      modelMatchDeps = (,) <$> mmPipelineF_C True, <*> actualWZ_C

  modelOnly_C <- BRK.clearIf' rerun cKey >>=
                 \ck -> BRK.retrieveOrMakeFrame ck modelOnlyDeps $ \(pF, inputData) -> DED.mapPE $ pF inputData
  modelMatch_C <- BRK.clearIf' rerun cKey >>=
                  \ck -> BRK.retrieveOrMakeFrame ck modelMatchDeps $ \(pF, inputData) -> DED.mapPE $ pF inputData
  product <-  DED.mapPE
              $ DTP.frameTableProduct @[BRDF.Year, GT.StateAbbreviation, DT.SexC, DT.Race5C] @'[DT.Education4C] @'[DT.CitizenC] @DT.PopCount
              (fmap F.rcast acsSampleSER) (fmap F.rcast acsSampleCSR)

-}

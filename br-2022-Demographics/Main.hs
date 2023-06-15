{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE RankNTypes #-}
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
import qualified BlueRipple.Model.Demographic.TPModel3 as DTM3
import qualified BlueRipple.Model.Demographic.EnrichCensus as DMC
import qualified BlueRipple.Model.Demographic.MarginalStructure as DMS
import qualified BlueRipple.Data.Keyed as Keyed

import qualified BlueRipple.Data.CensusLoaders as BRC
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.GeographicTypes as GT
import qualified BlueRipple.Data.DataFrames as BRDF
import qualified BlueRipple.Utilities.KnitUtils as BRK

import qualified Knit.Report as K
import qualified Knit.Effect.AtomicCache as KC
import qualified Text.Pandoc.Error as Pandoc
import qualified System.Console.CmdArgs as CmdArgs
import qualified Colonnade as C

import GHC.TypeLits (Symbol)

import qualified Data.List as List
import qualified Data.Map.Strict as M
import qualified Data.Map.Merge.Strict as MM
import Data.Type.Equality (type (~))
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
import qualified Frames.Serialize as FS

import Control.Lens (view, (^.), _2)

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

data MethodResult ks = MethodResult { mrResult :: ResultFrame ks, mrTableTitle :: Maybe Text, mrChartTitle :: Maybe Text, mrKLHeader :: Maybe Text}

compareResults :: forall ks key colKey rowKey r f .
                  (K.KnitOne r
                  , Traversable f
                  , Ord key
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
               -> Text
               -> Map Text Int -- populations
               -> (F.Record ks -> Text) -- region label
               -> Maybe Text
               -> (F.Record (ks V.++ '[DT.PopCount]) -> key)
               -> (F.Record (ks V.++ '[DT.PopCount]) -> rowKey)
               -> (F.Record (ks V.++ '[DT.PopCount]) -> colKey)
               -> (F.Record ks -> Text)
               -> Maybe (Text, F.Record ks -> Text, Maybe [Text])
               -> Maybe (Text, F.Record ks -> Text, Maybe [Text])
               -> ResultFrame ks
               -> f (MethodResult ks)
               -> K.Sem r ()
compareResults pp pi' chartDataPrefix regionPopMap regionKey exampleStateM catKey rowKey colKey showCellKey colorM shapeM actual results = do
  let tableText d = toText (C.ascii (fmap toString $ mapColonnade)
                             $ FL.fold (fmap DED.totaledTable
                                         $ DED.rowMajorMapFldInt
                                         (view DT.popCount)
                                         rowKey
                                         colKey
                                       )
                             d
                           )
      allRegions = M.keys regionPopMap
      facetB = True
      asDiffB = True
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
            vl <- distCompareChart pp pi' chartDataPrefix
                  (FV.ViewConfig hvWidth hvHeight hvPad)
                  title
                  (F.rcast @ks)
                  showCellKey
                  facetB
                  colorM
                  shapeM
                  asDiffB
                  ((if ls then logF else id) . realToFrac . view DT.popCount)
                  (if pctPop then (Just (regionKey, regionPopMap)) else Nothing)
                  ("Actual ACS", ff actual)
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
      tableTextMF "Actual Joint" (Just actual)
      traverse (\mr -> maybe (pure ()) (\t -> tableTextMF t (Just $ mr.mrResult)) $ mr.mrChartTitle) results
      K.logLE K.Info "Building example state charts."
      traverse (\mr -> maybe (pure ()) (\t -> compChartM ff False False (stTitle t) t $ Just mr.mrResult) $ mr.mrChartTitle) results
      traverse (\mr -> maybe (pure ()) (\t -> compChartM ff False True (stTitle t) t $ Just mr.mrResult) $ mr.mrChartTitle) results
      pure ()

  -- compute KL divergences
  K.logLE K.Info "Computing KL Divergences:"
  let toVecF = VS.fromList . fmap snd . M.toList . FL.fold (FL.premap (\r -> (catKey r, realToFrac (r ^. DT.popCount))) FL.map)
      computeKL d rk = (DED.klDiv' acsV dV, absFracWrong acsV dV) where
        acsV = toVecF $ F.filterFrame ((== rk) . regionKey . F.rcast) actual
        dV = toVecF $ F.filterFrame ((== rk) . regionKey . F.rcast) d
        absFracWrong acsV dV = FL.fold ((/) <$> FL.premap fst FL.sum <*> FL.premap snd FL.sum) $ zipWith (\acs d -> (abs (acs - d), acs)) (VS.toList acsV) (VS.toList dV)
      klCol mr = case mr.mrKLHeader of
        Just h -> Just $ (h, computeKL mr.mrResult <$> allRegions)
        Nothing -> Nothing
      klCols = catMaybes $ FL.fold FL.list $ fmap klCol results
      klColHeaders = fst <$> klCols
      klTableRows = zipWith (\region kls -> KLTableRow region kls) allRegions (transp $ fmap snd klCols)

  K.logLE K.Info "Divergence Table:"
  K.logLE K.Info $ "\n" <> toText (C.ascii (fmap toString $ klColonnadeFlex klColHeaders "Region") klTableRows)
--  _ <- compChart True "Actual" $ mrActual results
  K.logLE K.Info "Building national charts."
  traverse (\mr -> maybe (pure ()) (\t -> compChartM id False False (title t) t $ Just mr.mrResult) $ mr.mrChartTitle) results
  traverse (\mr -> maybe (pure ()) (\t -> compChartM id False True (title t) t $ Just mr.mrResult) $ mr.mrChartTitle) results
  pure ()

transp :: [[a]] -> [[a]]
transp = go [] where
  go x [] = fmap reverse x
  go [] (r : rs) = go (fmap (: []) r) rs
  go x (r :rs) = go (List.zipWith (:) r x) rs


type SER = [DT.SexC, DT.Education4C, DT.Race5C]
type ASR = [DT.Age4C, DT.SexC, DT.Race5C]
type ASER = [DT.Age4C, DT.SexC, DT.Education4C, DT.Race5C]

type A5SR = [DT.Age5FC, DT.SexC, DT.Race5C]
type A5SER = [DT.Age5FC, DT.SexC, DT.Education4C, DT.Race5C]

type CDRow ks = [BRDF.Year, GT.StateAbbreviation, GT.StateFIPS, GT.CongressionalDistrict] V.++ DMC.KeysWD ks

-- NB: as ks - first component and bs is ks - second
-- E.g. if you are combining ASR and SER to make ASER ks=ASER, as=E, bs=A
testProductNS_CDs :: forall  ks (as :: [(Symbol, Type)]) (bs :: [(Symbol, Type)]) qs r .
                     (K.KnitEffects r, BRK.CacheEffects r
                     , qs ~ F.RDeleteAll (as V.++ bs) ks
                     , qs V.++ (as V.++ bs) ~ (qs V.++ as) V.++ bs
                     , Ord (F.Record ks)
                     , Keyed.FiniteSet (F.Record ks)
                     , ((qs V.++ as) V.++ bs) F.⊆ ks
                     , ks F.⊆ ((qs V.++ as) V.++ bs)
                     , Ord (F.Record qs)
                     , Ord (F.Record as)
                     , Ord (F.Record bs)
                     , Ord (F.Record (qs V.++ as))
                     , Ord (F.Record (qs V.++ bs))
                     , Ord (F.Record ((qs V.++ as) V.++ bs))
                     , Keyed.FiniteSet (F.Record qs)
                     , Keyed.FiniteSet (F.Record as)
                     , Keyed.FiniteSet (F.Record bs)
                     , Keyed.FiniteSet (F.Record (qs V.++ as))
                     , Keyed.FiniteSet (F.Record (qs V.++ bs))
                     , Keyed.FiniteSet (F.Record ((qs V.++ as) V.++ bs))
                     , as F.⊆ (qs V.++ as)
                     , bs F.⊆ (qs V.++ bs)
                     , qs F.⊆ (qs V.++ as)
                     , qs F.⊆ (qs V.++ bs)
                     , (qs V.++ as) F.⊆ ((qs V.++ as) V.++ bs)
                     , (qs V.++ bs) F.⊆ ((qs V.++ as) V.++ bs)
                     , qs V.++ as F.⊆ DMC.PUMARowR ks
                     , qs V.++ bs F.⊆ DMC.PUMARowR ks
                     , ks F.⊆ DMC.PUMARowR ks
                     , V.RMap as
                     , V.ReifyConstraint Show F.ElField as
                     , V.RecordToList as
                     , V.RMap bs
                     , V.ReifyConstraint Show F.ElField bs
                     , V.RecordToList bs
                     , F.ElemOf (DMC.KeysWD ks) DT.PopCount
                     , F.ElemOf (DMC.KeysWD ks) DT.PWPopPerSqMile
                     , ks F.⊆ CDRow ks
                     , qs V.++ as F.⊆ CDRow ks
                     , qs V.++ bs F.⊆ CDRow ks
                     , FSI.RecVec (DMC.KeysWD ks)
                     , V.RMap (DMC.KeysWD ks)
                     , V.ReifyConstraint Show F.ElField (DMC.KeysWD ks)
                     , V.RecordToList (DMC.KeysWD ks)
                     , FS.RecFlat (DMC.KeysWD ks)
                     )
                  => (forall k . DTP.NullVectorProjections k -> VS.Vector Double -> VS.Vector Double -> K.Sem r (VS.Vector Double))
                  -> Bool
                  -> Bool
                  -> Text
                  -> Text
                  -> BR.CommandLine
                  -> K.ActionWithCacheTime r (F.FrameRec (DMC.PUMARowR ks))
                  -> K.ActionWithCacheTime r (F.FrameRec (CDRow ks))
                  -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec (CDRow ks), F.FrameRec (CDRow ks)))
testProductNS_CDs onSimplexM rerunModel clearCaches cachePrefix modelId cmdLine byPUMA_C byCD_C = do
  let productFrameCacheKey = cachePrefix <> "_productFrame.bin"
  when clearCaches $ traverse_ BRK.clearIfPresentD [productFrameCacheKey]
  let modelCacheDirE = (if rerunModel then Left else Right) cachePrefix
  (predictor_C, nvps_C, ms) <- DMC.predictorModel3 @as @bs @ks @qs modelCacheDirE modelId cmdLine byPUMA_C
  byCD <- K.ignoreCacheTime byCD_C
  nvps <- K.ignoreCacheTime nvps_C
  let cdModelData = FL.fold
        (DTM3.nullVecProjectionsModelDataFldCheck
         DMS.cwdWgtLens
         ms
         nvps
         (F.rcast @'[BRDF.Year, GT.StateAbbreviation, GT.StateFIPS, GT.CongressionalDistrict])
         (F.rcast @ks)
         DTM3.cwdF
         (DMC.innerFoldWD @(qs V.++ bs) @(qs V.++ as) @(CDRow ks) (F.rcast @(qs V.++ bs )) (F.rcast @(qs V.++ as)))
        )
        byCD
      productFrameDeps = (,) <$> nvps_C <*> predictor_C
  BRK.retrieveOrMake2Frames productFrameCacheKey productFrameDeps $ \(nvps', predictor) -> do
    let vecToFrame ok kws = fmap (\(k, w) -> ok F.<+> k F.<+> DMS.cwdToRec w) kws
        prodAndModeledToFrames (ok, pKWs, mKWs) = (vecToFrame ok pKWs, vecToFrame ok mKWs)

        smcRowToProdAndModeled (ok, covariates, _, pKWs, _) = do
--          let pV = VS.fromList $ fmap (view (_2 . DMS.cwdWgtLens)) pKWs
          mKWs <- DTM3.predictedJoint onSimplexM DMS.cwdWgtLens predictor (ok ^. GT.stateAbbreviation) covariates pKWs
          pure (ok, pKWs, mKWs)

    let cMatrix = DED.mMatrix (DMS.msNumCategories ms) (DMS.msStencils ms)

    forM_ cdModelData $ \(sar, md, nVpsActual, pKWs, oKWs) -> do
      let keyT = DDP.districtKeyT sar
          pV = VS.fromList $ fmap (view (_2 . DMS.cwdWgtLens)) pKWs
          nV = VS.fromList $ fmap (view (_2 . DMS.cwdWgtLens)) oKWs
          n = VS.sum pV

      when (BR.logLevel cmdLine >= BR.LogDebugMinimal) $ do
        let showSum v = " (" <> show (VS.sum v) <> ")"
        K.logLE K.Info $ keyT <> " actual  counts=" <> DED.prettyVector nV <> showSum nV
        K.logLE K.Info $ keyT <> " prod counts=" <> DED.prettyVector pV <> showSum pV
        let nvpsV = DTP.applyNSPWeights nvps' (VS.map (* n) nVpsActual) pV
        K.logLE K.Info $ keyT <> "NS projections=" <> DED.prettyVector nVpsActual
        K.logLE K.Info $ "sumSq(projections)=" <> show (VS.sum $ VS.zipWith (*) nVpsActual nVpsActual)
        let cCheckV = cMatrix LA.#> (nV - pV)
        K.logLE K.Info $ keyT <> " C * (actual - prod) =" <> DED.prettyVector cCheckV <> showSum cCheckV
        K.logLE K.Info $ keyT <> " predictors: " <> DED.prettyVector md
        nvpsModeled <- VS.fromList <$> (K.knitEither $ DTM3.modelResultNVPs predictor (view GT.stateAbbreviation sar) md)
        K.logLE K.Info $ keyT <> " modeled  =" <> DED.prettyVector nvpsModeled <> showSum nvpsModeled
        let dNVPs = VS.zipWith (-) nVpsActual nvpsModeled
        K.logLE K.Info $ "dNVPS=" <> DED.prettyVector dNVPs
        K.logLE K.Info $ "sumSq(dNVPS)=" <>  show (VS.sum $ VS.zipWith (*) dNVPs dNVPs)
        simplexFull <- onSimplexM nvps' nvpsModeled $ VS.map (/ n) pV
--        let simplexFull = DTP.projectToSimplex $ DTP.applyNSPWeights nvps' nvpsModeled (VS.map (/ n) pV)
        let simplexNVPs = DTP.fullToProj nvps' simplexFull
            dSimplexNVPs = VS.zipWith (-) simplexNVPs nVpsActual
        K.logLE K.Info $ "dSimplexNVPS=" <> DED.prettyVector dSimplexNVPs
        K.logLE K.Info $ "sumSq(dSimplexNVPS)=" <>  show (VS.sum $ VS.zipWith (*) dSimplexNVPs dSimplexNVPs)
        K.logLE K.Info $ keyT <> " onSimplex=" <> DED.prettyVector simplexNVPs <> showSum simplexNVPs
        let modeledCountsV = VS.map (* n) simplexFull
        K.logLE K.Info $ keyT <> " modeled counts=" <> DED.prettyVector modeledCountsV <> showSum modeledCountsV

    prodAndModeled <- traverse smcRowToProdAndModeled cdModelData
    pure $ bimap (F.toFrame . concat) (F.toFrame . concat)
         $ unzip
         $ fmap prodAndModeledToFrames prodAndModeled


cdWithZeros :: forall ks .
             (
               (ks V.++ [DT.PopCount, DT.PWPopPerSqMile]) F.⊆ CDRow ks
             , ks F.⊆ (ks V.++ [DT.PopCount, DT.PWPopPerSqMile])
             , FSI.RecVec (ks V.++ [DT.PopCount, DT.PWPopPerSqMile])
             , Keyed.FiniteSet (F.Record ks)
             , F.ElemOf (ks V.++ [DT.PopCount, DT.PWPopPerSqMile]) DT.PWPopPerSqMile
             , F.ElemOf (ks V.++ [DT.PopCount, DT.PWPopPerSqMile]) DT.PopCount
             )
          => F.FrameRec (CDRow ks)
          -> F.FrameRec (CDRow ks)
cdWithZeros frame = FL.fold (FMR.concatFold
                           $ FMR.mapReduceFold
                           FMR.noUnpack
                           (FMR.assignKeysAndData @[BRDF.Year, GT.StateAbbreviation, GT.StateFIPS, GT.CongressionalDistrict]
                            @(ks V.++ [DT.PopCount, DT.PWPopPerSqMile]))
                           (FMR.foldAndLabel
                            (fmap F.toFrame $ Keyed.addDefaultRec @ks zc)
                            (\k r -> fmap (k F.<+>) r)
                           )
                          )
                  frame
  where
       zc :: F.Record '[DT.PopCount, DT.PWPopPerSqMile] = 0 F.&: 0 F.&: V.RNil


aggregateAndZeroFillTables :: forall ls ks .
                              (
                                (ls V.++ ks) V.++ [DT.PopCount, DT.PWPopPerSqMile] ~ ls V.++ (ks V.++ [DT.PopCount, DT.PWPopPerSqMile])
                              , Ord (F.Record ls)
                              , ls F.⊆ ((ls V.++ ks) V.++ [DT.PopCount, DT.PWPopPerSqMile])
                              , ks F.⊆ (ks V.++ [DT.PopCount, DT.PWPopPerSqMile])
                              , ks V.++ [DT.PopCount, DT.PWPopPerSqMile] F.⊆ ((ls V.++ ks) V.++ [DT.PopCount, DT.PWPopPerSqMile])
                              , FSI.RecVec (ks V.++ [DT.PopCount, DT.PWPopPerSqMile])
                              , F.ElemOf (ks V.++ [DT.PopCount, DT.PWPopPerSqMile]) DT.PopCount
                              , F.ElemOf (ks V.++ [DT.PopCount, DT.PWPopPerSqMile]) DT.PWPopPerSqMile
                              , Keyed.FiniteSet (F.Record ks)
                              , Ord (F.Record ks)
                              )
                           => F.FrameRec (ls V.++ ks V.++ [DT.PopCount, DT.PWPopPerSqMile])
                           -> F.FrameRec (ls V.++ ks V.++ [DT.PopCount, DT.PWPopPerSqMile])
aggregateAndZeroFillTables frame =FL.fold
                                  (FMR.concatFold
                                   $ FMR.mapReduceFold
                                   FMR.noUnpack
                                   (FMR.assignKeysAndData @ls @(ks V.++ [DT.PopCount, DT.PWPopPerSqMile]))
                                   (FMR.foldAndLabel innerFld (\k r -> fmap (k F.<+>) r))
                                  )
                                  frame
  where
    toKW r = (F.rcast @ks r, DTM3.cwdF r)
    toRec (k, w) = k F.<+> DMS.cwdToRec w
    innerFld :: FL.Fold (F.Record (ks V.++ [DT.PopCount, DT.PWPopPerSqMile])) (F.FrameRec (ks V.++ [DT.PopCount, DT.PWPopPerSqMile]))
    innerFld = fmap (F.toFrame . fmap toRec . M.toList) $ FL.premap toKW DMS.zeroFillSummedMapFld





compareCSR_ASR :: (K.KnitMany r, K.KnitEffects r, BRK.CacheEffects r) => BR.CommandLine -> BR.PostInfo -> K.Sem r ()
compareCSR_ASR cmdLine postInfo = do
    K.logLE K.Info "Building test CD-level products for CSR x ASR -> CASR"
    byPUMA_C <- fmap (aggregateAndZeroFillTables @DDP.ACSByPUMAGeoR @DMC.CASR . fmap F.rcast)
                <$> DDP.cachedACSa5ByPUMA
    byCD_C <- fmap (aggregateAndZeroFillTables @DDP.ACSByCDGeoR @DMC.CASR . fmap F.rcast)
              <$> DDP.cachedACSa5ByCD
    byCD <- K.ignoreCacheTime byCD_C
    (product_CSR_ASR, modeled_CSR_ASR) <- K.ignoreCacheTimeM
                                          $ testProductNS_CDs @DMC.CASR @'[DT.CitizenC] @'[DT.Age5C]
                                          DTP.viaOptimalWeights False True "model/demographic/csr_asr" "CSR_ASR" cmdLine byPUMA_C byCD_C
    let raceOrder = show <$> S.toList (Keyed.elements @DT.Race5)
        ageOrder = show <$> S.toList (Keyed.elements @DT.Age5)
        cdPopMap = FL.fold (FL.premap (\r -> (DDP.districtKeyT r, r ^. DT.popCount)) $ FL.foldByKeyMap FL.sum) byCD
        statePopMap = FL.fold (FL.premap (\r -> (view GT.stateAbbreviation r, r ^. DT.popCount)) $ FL.foldByKeyMap FL.sum) byCD
        showCellKey r =  show (r ^. GT.stateAbbreviation, r ^. DT.citizenC, r ^. DT.age5C, r ^. DT.sexC, r ^. DT.race5C)
    synthModelPaths <- postPaths "Model_CSR_ASR" cmdLine
    BRK.brNewPost synthModelPaths postInfo "Model_CSR_ASR" $ do
      compareResults @([BRDF.Year, GT.StateAbbreviation, GT.CongressionalDistrict] V.++ DMC.CASR)
        synthModelPaths postInfo "CSR_ASR" cdPopMap DDP.districtKeyT (Just "NY")
        (F.rcast @DMC.CASR)
        (\r -> (r ^. DT.sexC, r ^. DT.race5C))
        (\r -> (r ^. DT.citizenC, r ^. DT.age5C))
        showCellKey
        (Just ("Race", show . view DT.race5C, Just raceOrder))
        (Just ("Age", show . view DT.age5C, Just ageOrder))
        (fmap F.rcast $ cdWithZeros @DMC.CASR byCD)
        [MethodResult (fmap F.rcast $ cdWithZeros @DMC.CASR byCD) (Just "Actual") Nothing Nothing
        ,MethodResult (fmap F.rcast product_CSR_ASR) (Just "Product") (Just "Marginal Product") (Just "Product")
        ,MethodResult (fmap F.rcast modeled_CSR_ASR) (Just "NS Model") (Just "NS Model") (Just "NS Model")
        ]
      compareResults @([BRDF.Year, GT.StateAbbreviation, GT.CongressionalDistrict] V.++ DMC.CASR)
        synthModelPaths postInfo "CSR_ASR" statePopMap (view GT.stateAbbreviation) (Just "PA")
        (F.rcast @DMC.CASR)
        (\r -> (r ^. DT.sexC, r ^. DT.race5C))
        (\r -> (r ^. DT.citizenC, r ^. DT.age5C))
        showCellKey
        (Just ("Race", show . view DT.race5C, Just raceOrder))
        (Just ("Age", show . view DT.age5C, Just ageOrder))
        (fmap F.rcast $ cdWithZeros @DMC.CASR byCD)
        [MethodResult (fmap F.rcast $ cdWithZeros @DMC.CASR byCD) (Just "Actual") Nothing Nothing
        ,MethodResult (fmap F.rcast product_CSR_ASR) (Just "Product") (Just "Marginal Product") (Just "Product")
        ,MethodResult (fmap F.rcast modeled_CSR_ASR) (Just "NS Model") (Just "NS Model") (Just "NS Model")
        ]
    pure ()


compareSER_ASR :: (K.KnitMany r, K.KnitEffects r, BRK.CacheEffects r) => BR.CommandLine -> BR.PostInfo -> K.Sem r ()
compareSER_ASR cmdLine postInfo = do
    K.logLE K.Info "Building test CD-level products for SER x A6SR -> A6SER"
    byPUMA_C <- fmap (aggregateAndZeroFillTables @DDP.ACSByPUMAGeoR @DMC.ASER . fmap F.rcast)
                <$> DDP.cachedACSa5ByPUMA
    byCD_C <- fmap (aggregateAndZeroFillTables @DDP.ACSByCDGeoR @DMC.ASER . fmap F.rcast)
              <$> DDP.cachedACSa5ByCD
    byCD <- K.ignoreCacheTime byCD_C
    (product_SER_ASR, modeled_SER_ASR) <- K.ignoreCacheTimeM
                                            $ testProductNS_CDs @DMC.ASER @'[DT.Age5C] @'[DT.Education4C]
                                            DTP.viaOptimalWeights False True "model/demographic/ser_asr" "SER_ASR" cmdLine byPUMA_C byCD_C
    let raceOrder = show <$> S.toList (Keyed.elements @DT.Race5)
        ageOrder = show <$> S.toList (Keyed.elements @DT.Age5)
        cdPopMap = FL.fold (FL.premap (\r -> (DDP.districtKeyT r, r ^. DT.popCount)) $ FL.foldByKeyMap FL.sum) byCD
        statePopMap = FL.fold (FL.premap (\r -> (view GT.stateAbbreviation r, r ^. DT.popCount)) $ FL.foldByKeyMap FL.sum) byCD
        showCellKey r =  show (r ^. GT.stateAbbreviation, r ^. DT.age5C, r ^. DT.sexC, r ^. DT.education4C, r ^. DT.race5C)
    synthModelPaths <- postPaths "Model_SER_A6SR" cmdLine
    BRK.brNewPost synthModelPaths postInfo "Model_SER_A6SR" $ do
      compareResults @([BRDF.Year, GT.StateAbbreviation, GT.CongressionalDistrict] V.++ DMC.ASER)
        synthModelPaths postInfo "SER_ASR" cdPopMap DDP.districtKeyT (Just "NY")
        (F.rcast @DMC.ASER)
        (\r -> (r ^. DT.sexC, r ^. DT.race5C))
        (\r -> (r ^. DT.education4C, r ^. DT.age5C))
        showCellKey
        (Just ("Race", show . view DT.race5C, Just raceOrder))
        (Just ("Age", show . view DT.age5C, Just ageOrder))
        (fmap F.rcast $ cdWithZeros @DMC.ASER byCD)
        [MethodResult (fmap F.rcast $ cdWithZeros @DMC.ASER byCD) (Just "Actual") Nothing Nothing
        ,MethodResult (fmap F.rcast product_SER_ASR) (Just "Product") (Just "Marginal Product") (Just "Product")
        ,MethodResult (fmap F.rcast modeled_SER_ASR) (Just "NS Model") (Just "NS Model") (Just "NS Model")
        ]
      compareResults @([BRDF.Year, GT.StateAbbreviation, GT.CongressionalDistrict] V.++ DMC.ASER)
        synthModelPaths postInfo "SER_ASR" statePopMap (view GT.stateAbbreviation) (Just "PA")
        (F.rcast @DMC.ASER)
        (\r -> (r ^. DT.sexC, r ^. DT.race5C))
        (\r -> (r ^. DT.education4C, r ^. DT.age5C))
        showCellKey
        (Just ("Race", show . view DT.race5C, Just raceOrder))
        (Just ("Age", show . view DT.age5C, Just ageOrder))
        (fmap F.rcast $ cdWithZeros @DMC.ASER byCD)
        [MethodResult (fmap F.rcast $ cdWithZeros @DMC.ASER byCD) (Just "Actual") Nothing Nothing
        ,MethodResult (fmap F.rcast product_SER_ASR) (Just "Product") (Just "Marginal Product") (Just "Product")
        ,MethodResult (fmap F.rcast modeled_SER_ASR) (Just "NS Model") (Just "NS Model") (Just "NS Model")
        ]
    pure ()




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
    let postInfo = BR.PostInfo (BR.postStage cmdLine) (BR.PubTimes BR.Unpublished Nothing)
    byPUMA_C <-  fmap (aggregateAndZeroFillTables @DDP.ACSByPUMAGeoR @DMC.CASR . fmap F.rcast)
                <$> DDP.cachedACSa5ByPUMA
    (predictor_C, _, _) <- DMC.predictorModel3 @'[DT.CitizenC] @'[DT.Age5C] @DMC.CASR @DMC.SR (Right "model/demographic/csr_asr") "CSR_ASR" cmdLine byPUMA_C

    sld2022CensusTables_C <- BRC.censusTablesFor2022SLDs
--    sldCensusTablesMA <- BRC.filterCensusTables ((== 25) . view GT.stateFIPS) <$> K.ignoreCacheTime sld2022CensusTables_C
--    BRK.logFrame $ BRC.ageSexEducation sldCensusTablesMA
    let censusTableCond r = r ^. GT.stateFIPS == 9 && r ^. GT.districtTypeC == GT.StateUpper && r ^. GT.districtName == "10"
        filteredCensusTables_C = fmap (BRC.filterCensusTables censusTableCond) sld2022CensusTables_C
    predictedSLDDemographics_C <- fmap fst $ DMC.predictedCensusCASR DTP.viaNearestOnSimplex True "model/demographic/test/casrTest" predictor_C filteredCensusTables_C
    DMC.checkCensusTables filteredCensusTables_C
    compareCSR_ASR cmdLine postInfo
    compareSER_ASR cmdLine postInfo
  case resE of
    Right namedDocs →
      K.writeAllPandocResultsWithInfoAsHtml "" namedDocs
    Left err → putTextLn $ "Pandoc Error: " <> Pandoc.renderError err
{-
originalPost ::  (K.KnitMany r, K.KnitEffects r, BRK.CacheEffects r) => BR.CommandLine -> BR.PostInfo -> K.Sem r ()
originalPost cmdLine postInfo = do
    K.logLE K.Info $ "Loading ACS data for each PUMA" <> show cmdLine
    acsA5ByPUMA_C <- DDP.cachedACSa5ByPUMA

    let msSER_A5SR_sd = DMS.reKeyMarginalStructure
                        (F.rcast @[DT.SexC, DT.Race5C, DT.Education4C, DT.Age5FC])
                        (F.rcast @A5SER)
                        $ DMS.combineMarginalStructuresF  @'[DT.SexC, DT.Race5C] @'[DT.Education4C] @'[DT.Age5FC]
                        DTP.sumLens DMS.innerProductSum
                        (DMS.identityMarginalStructure DTP.sumLens)
                        (DMS.identityMarginalStructure DTP.sumLens)
{-
        msSER_A5SR_sd' = DMC.marginalStructure @DMC.A5SER @'[DT.Age5FC] @'[DT.Education4C] DTP.sumLens DMS.innerProductSum
    let eqStencils = DMS.msStencils msSER_A5SR_sd == DMS.msStencils msSER_A5SR_sd'
    K.logLE K.Info $ "Stencils are == ?" <> show eqStencils
    when (not eqStencils) $ K.logLE K.Info $ "orig=" <> show (DMS.msStencils msSER_A5SR_sd) <> "\nnew=" <> show (DMS.msStencils msSER_A5SR_sd')
    K.knitError "STOP"
-}
    {-
    K.logLE K.Info "Building district-level ASER joint distributions"
    predictor_C <- DMC.predictorModel3 False cmdLine
    sld2022CensusTables_C <- BRC.censusTablesFor2022SLDs
    let censusTableCond r = r ^. GT.stateFIPS == 9 && r ^. GT.districtTypeC == GT.StateUpper -- && r ^. GT.districtName == "10"
        filteredCensusTables_C = fmap (BRC.filterCensusTables censusTableCond) sld2022CensusTables_C
    predictedSLDDemographics_C <- fmap fst $ DMC.predictedCensusASER True "model/demographic/test/svdBug" predictor_C filteredCensusTables_C
-}
    acsA5ByCD_C <- DDP.cachedACSa5ByCD
    acsA5ByCD <- K.ignoreCacheTime acsA5ByCD_C
    productNS3_SER_A5SR_C <- fmap snd <$>
                             testProductNS_CDs @DMC.A5SER @'[DT.Age5FC] @'[DT.Education4C] False True "model/demographic/ser_a5sr" "SER_A5SR" cmdLine
                             (fmap (fmap F.rcast) acsA5ByPUMA_C)
                             (fmap (fmap F.rcast) acsA5ByCD_C)
    productNS3_SER_A5SR <- fmap (fmap F.rcast) $ K.ignoreCacheTime productNS3_SER_A5SR_C

    let vecToFrame ok ks v = fmap (\(k, c) -> ok F.<+> k F.<+> FT.recordSingleton @DT.PopCount (round c)) $ zip ks (VS.toList v)
        prodAndModeledToFrames ks (ok, pv, mv) = (vecToFrame ok ks pv, vecToFrame ok ks mv)


    let ms_SER_A5SR = DMC.marginalStructure @DMC.A5SER @'[DT.Age5FC] @'[DT.Education4C] DMS.cwdWgtLens DMS.innerProductCWD'
    nvps_SER_A5SR_C <- DMC.cachedNVProjections "model/demographic/ser_a5sr_NVPs.bin" ms_SER_A5SR acsA5ByPUMA_C
    nvps_SER_A5SR <- K.ignoreCacheTime nvps_SER_A5SR_C
    let cMatrix_SER_A5SR = DED.mMatrix (DMS.msNumCategories ms_SER_A5SR) (DMS.msStencils ms_SER_A5SR)
        cdPopMap = FL.fold (FL.premap (\r -> (DDP.districtKeyT r, r ^. DT.popCount)) $ FL.foldByKeyMap FL.sum) acsA5ByCD
        statePopMap = FL.fold (FL.premap (\r -> (view GT.stateAbbreviation r, r ^. DT.popCount)) $ FL.foldByKeyMap FL.sum) acsA5ByCD
        cdModelData1 = FL.fold
                       (DTM1.nullVecProjectionsModelDataFldCheck
                         msSER_A5SR_sd
                         nvps_SER_A5SR
                         (F.rcast @'[BRDF.Year, GT.StateAbbreviation, GT.CongressionalDistrict])
                         (F.rcast @A5SER)
                         (realToFrac . view DT.popCount)
                         DTM1.a5serModelDatFld
                       )
                       acsA5ByCD

    K.logLE K.Info $ "Running model summary stats as covariates model, if necessary."
    let modelConfig = DTM1.ModelConfig nvps_SER_A5SR True
                      DTM1.designMatrixRowASER DTM1.AlphaHierNonCentered DTM1.NormalDist DTM1.aserModelFuncs
    res_C <- DTM1.runProjModel @A5SER False cmdLine (DTM1.RunConfig False False) modelConfig msSER_A5SR_sd DTM1.a5serModelDatFld
    modelRes <- K.ignoreCacheTime res_C

    forM_ cdModelData1 $ \(sar, md, nVpsActual, pV, nV) -> do
      let keyT = DDP.districtKeyT sar
          n = VS.sum pV

      when (BR.logLevel cmdLine >= BR.LogDebugMinimal) $ do
--        K.logLE K.Info $ keyT <> " nvps (actual) =" <> DED.prettyVector nVpsActual
        K.logLE K.Info $ keyT <> " actual  counts=" <> DED.prettyVector nV <> " (" <> show (VS.sum nV) <> ")"
        K.logLE K.Info $ keyT <> " prod    counts=" <> DED.prettyVector pV <> " (" <> show (VS.sum pV) <> ")"
        K.logLE K.Info $ keyT <> " nvps counts   =" <> DED.prettyVector (DTP.applyNSPWeights nvps_SER_A5SR (VS.map (* n) nVpsActual) pV)
        K.logLE K.Info $ keyT <> " C * (actual - prod) =" <> DED.prettyVector (cMatrix_SER_A5SR LA.#> (nV - pV))
        K.logLE K.Info $ keyT <> " predictors: " <> show md
        nvpsModeled <- VS.fromList <$> (K.knitEither $ DTM1.modelResultNVPs DTM1.aserModelFuncs modelRes (sar ^. GT.stateAbbreviation) md)
        K.logLE K.Info $ keyT <> " modeled  =" <> DED.prettyVector nvpsModeled
        let simplexFull = DTP.projectToSimplex $ DTP.applyNSPWeights nvps_SER_A5SR nvpsModeled (VS.map (/ VS.sum pV) pV)
            simplexNVPs = DTP.fullToProj nvps_SER_A5SR simplexFull
--        nvpsOptimal <- DED.mapPE $ DTP.optimalWeights testProjections nvpsModeled (VS.map (/ n) pV)
        K.logLE K.Info $ keyT <> " onSimplex=" <> DED.prettyVector simplexNVPs
        K.logLE K.Info $ keyT <> " modeled counts=" <> DED.prettyVector (VS.map (* VS.sum pV) simplexFull)
--    K.knitError "STOP"

    let smcRowToProdAndModeled1 (ok, md, _, pV, _) = do
          let n = VS.sum pV --realToFrac $ DMS.msNumCategories marginalStructure
          nvpsModeled <-  VS.fromList <$> (K.knitEither $ DTM1.modelResultNVPs DTM1.aserModelFuncs modelRes (view GT.stateAbbreviation ok) md)
          let simplexFull = VS.map (* n) $ DTP.projectToSimplex $ DTP.applyNSPWeights nvps_SER_A5SR nvpsModeled (VS.map (/ n) pV)
--            simplexNVPs = DTP.fullToProj testProjections simplexFull
--          nvpsOptimal <- DED.mapPE $ DTP.optimalWeights testProjections nvpsModeled (VS.map (/ n) pV)
          let mV = simplexFull --DTP.applyNSPWeights testProjections (VS.map (* n) nvpsOptimal) pV
          pure (ok, pV, mV)
    prodAndModeled1 <- traverse smcRowToProdAndModeled1 cdModelData1
    let (product_SER_A5SR, productNS1_SER_A5SR) =
          first (F.toFrame . concat)
          $ second (F.toFrame . concat)
          $ unzip
          $ fmap (prodAndModeledToFrames (S.toList $ Keyed.elements @(F.Record A5SER))) prodAndModeled1
        addSimpleAge4 r = FT.recordSingleton @DT.SimpleAgeC (DT.age4ToSimple $ r ^. DT.age4C) F.<+> r
        addSimpleAge5 r = FT.recordSingleton @DT.SimpleAgeC (DT.age5FToSimple $ r ^. DT.age5FC) F.<+> r
        aggregateAge = FMR.concatFold
                       $ FMR.mapReduceFold
                       (FMR.Unpack $ \r -> [addSimpleAge5 r])
                       (FMR.assignKeysAndData @[BRDF.Year, GT.StateAbbreviation, GT.CongressionalDistrict, DT.SimpleAgeC, DT.SexC, DT.Education4C, DT.Race5C]
                         @'[DT.PopCount])
                       (FMR.foldAndAddKey $ FF.foldAllConstrained @Num FL.sum)
        product_SER_A2SR = FL.fold aggregateAge product_SER_A5SR
        productNS1_SER_A2SR = FL.fold aggregateAge productNS1_SER_A5SR
        productNS3_SER_A2SR = FL.fold aggregateAge productNS3_SER_A5SR

    K.logLE K.Info $ "Loading ACS data and building marginal distributions (SER, ASR, CSR) for each CD" <> show cmdLine
    let zc :: F.Record '[DT.PopCount, DT.PWPopPerSqMile] = 0 F.&: 0 F.&: V.RNil
    let acsA5SampleWZ_C = fmap (FL.fold
                                (FMR.concatFold
                                 $ FMR.mapReduceFold
                                 FMR.noUnpack
                                 (FMR.assignKeysAndData @[BRDF.Year, GT.StateAbbreviation, GT.CongressionalDistrict]
                                  @[DT.CitizenC, DT.Age5FC, DT.SexC, DT.Education4C, DT.Race5C, DT.PopCount, DT.PWPopPerSqMile])
                                 (FMR.foldAndLabel
                                  (Keyed.addDefaultRec @[DT.CitizenC, DT.Age5FC, DT.SexC, DT.Education4C, DT.Race5C] zc)
                                  (\k r -> fmap (k F.<+>) r)
                                 )
                                )
                               )
                          $ acsA5ByCD_C
    let allStates = S.toList $ FL.fold (FL.premap (view GT.stateAbbreviation) FL.set) acsA5ByCD
        allCDs = S.toList $ FL.fold (FL.premap DDP.districtKey FL.set) acsA5ByCD
        emptyUnless x y = if x then y else mempty
        logText t k = Nothing --Just $ t <> ": Joint distribution matching for " <> show k
        zeroPopAndDens :: F.Record [DT.PopCount, DT.PWPopPerSqMile] = 0 F.&: 0 F.&: V.RNil
        tableMatchingDataFunctions = DED.TableMatchingDataFunctions zeroPopAndDens recToCWD cwdToRec cwdN updateCWDCount
--        exampleState = "KY"
        rerunMatches = True
    -- SER to A2SER
    let toOutputRow :: ([BRDF.Year, GT.StateAbbreviation, GT.CongressionalDistrict, DT.Age5FC, DT.SexC, DT.Education4C, DT.Race5C, DT.PopCount]  F.⊆ rs)
                    => F.Record rs -> F.Record [BRDF.Year, GT.StateAbbreviation, GT.CongressionalDistrict, DT.Age5FC, DT.SexC, DT.Education4C, DT.Race5C, DT.PopCount]
        toOutputRow = F.rcast
    let toOutputRow2 :: ([BRDF.Year, GT.StateAbbreviation, GT.CongressionalDistrict, DT.SimpleAgeC, DT.SexC, DT.Education4C, DT.Race5C, DT.PopCount]  F.⊆ rs)
                    => F.Record rs -> F.Record [BRDF.Year, GT.StateAbbreviation, GT.CongressionalDistrict, DT.SimpleAgeC, DT.SexC, DT.Education4C, DT.Race5C, DT.PopCount]
        toOutputRow2 = F.rcast
    -- actual
    let acsSampleA5SER_C = F.toFrame
                           . FL.fold (FMR.concatFold
                                      $ FMR.mapReduceFold
                                      FMR.noUnpack
                                      (FMR.assignKeysAndData @[BRDF.Year, GT.StateAbbreviation, GT.CongressionalDistrict, DT.Age5FC, DT.SexC, DT.Education4C, DT.Race5C]
                                       @'[DT.PopCount, DT.PWPopPerSqMile])
                                      (FMR.foldAndAddKey DDP.aggregatePeopleAndDensityF)
                                     )
                           <$> acsA5SampleWZ_C
    acsSampleA5SER <- K.ignoreCacheTime acsSampleA5SER_C

    let acsSampleA2SER_C = F.toFrame
                           . FL.fold (FMR.concatFold
                                      $ FMR.mapReduceFold
                                      (FMR.Unpack $ \r -> [FT.recordSingleton @DT.SimpleAgeC (DT.age5FToSimple $ r ^. DT.age5FC) F.<+> r])
                                      (FMR.assignKeysAndData @[BRDF.Year, GT.StateAbbreviation, GT.CongressionalDistrict, DT.SimpleAgeC, DT.SexC, DT.Education4C, DT.Race5C]
                                       @'[DT.PopCount, DT.PWPopPerSqMile])
                                      (FMR.foldAndAddKey DDP.aggregatePeopleAndDensityF)
                                     )
                           <$> acsA5SampleWZ_C
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
                         <$> acsA5SampleWZ_C
    let acsSampleASR_C = F.toFrame
                         . FL.fold (FMR.concatFold
                                    $ FMR.mapReduceFold
                                    FMR.noUnpack
                                    (FMR.assignKeysAndData @[BRDF.Year, GT.StateAbbreviation, GT.CongressionalDistrict, DT.Age5FC, DT.SexC, DT.Race5C]
                                      @[DT.PopCount, DT.PWPopPerSqMile])
                                    (FMR.foldAndAddKey DDP.aggregatePeopleAndDensityF)
                                   )
                         <$> acsA5SampleWZ_C

    -- model & match pipeline
    a2FromSER_C <- SM.runModel
                   True cmdLine (SM.ModelConfig () False SM.HCentered True)
                   ("Age", DT.age5FToSimple . view DT.age5FC)
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
                    (\r -> a2srRec (DT.age5FToSimple (r ^. DT.age5FC), r ^. DT.sexC, r ^. DT.race5C))
        a2srDS = getSum <<$>> FL.fold a2srDSFld acsA5ByCD

        serRec :: (DT.Sex, DT.Education4, DT.Race5) -> F.Record [DT.SexC, DT.Education4C, DT.Race5C]
        serRec (s, e, r) = s F.&: e F.&: r F.&: V.RNil
        serDSFld =  DED.desiredSumsFld
                    (Sum . F.rgetField @DT.PopCount)
                    (F.rcast @'[BRDF.Year, GT.StateAbbreviation, GT.CongressionalDistrict])
                    (\r -> serRec (r ^. DT.sexC, r ^. DT.education4C, r ^. DT.race5C))
        serDS = getSum <<$>> FL.fold serDSFld acsA5ByCD

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

    synthModelPaths <- postPaths "SynthModel" cmdLine
    BRK.brNewPost synthModelPaths postInfo "SynthModel" $ do
      let showCellKeyA2 r = show (r ^. GT.stateAbbreviation, r ^. DT.simpleAgeC, r ^. DT.sexC, r ^. DT.education4C, r ^. DT.race5C)
          edOrder =  show <$> S.toList (Keyed.elements @DT.Education4)
--          age4Order = show <$> S.toList (Keyed.elements @DT.Age4)
          age5Order = show <$> S.toList (Keyed.elements @DT.Age5F)
          simpleAgeOrder = show <$> S.toList (Keyed.elements @DT.SimpleAge)
      compareResults @[BRDF.Year, GT.StateAbbreviation, GT.CongressionalDistrict, DT.SimpleAgeC, DT.SexC, DT.Education4C, DT.Race5C]
        synthModelPaths postInfo cdPopMap DDP.districtKeyT (Just "NY")
        (F.rcast @[DT.SimpleAgeC, DT.SexC, DT.Education4C, DT.Race5C])
        (\r -> (r ^. DT.sexC, r ^. DT.race5C))
        (\r -> (r ^. DT.simpleAgeC, r ^. DT.education4C))
        showCellKeyA2
        (Just ("Education", show . view DT.education4C, Just edOrder))
        (Just ("SimpleAge", show . view DT.simpleAgeC, Just simpleAgeOrder))
        (fmap toOutputRow2 acsSampleA2SER)
        [MethodResult (fmap toOutputRow2 acsSampleA2SER) (Just "Actual") Nothing Nothing
        ,MethodResult product_SER_A2SR (Just "Product") (Just "Marginal Product") (Just "Product")
        ,MethodResult productNS1_SER_A2SR (Just "Null-Space_v1") (Just "Null-Space_v1") (Just "Null-space_v1")
        ,MethodResult productNS3_SER_A2SR (Just "Null-Space_v3") (Just "Null-Space_v3") (Just "Null-space_v3")
        ,MethodResult modelA2SERFromSER (Just "Model Only") (Just "Model Only") (Just "Model Only")
        ,MethodResult modelCO_A2SERFromSER (Just "Model+CO") (Just "Model+CO") (Just "Model+CO")
        ]

      let showCellKey r = show (r ^. GT.stateAbbreviation, r ^. DT.age5FC, r ^. DT.sexC, r ^. DT.education4C, r ^. DT.race5C)
      compareResults @[BRDF.Year, GT.StateAbbreviation, GT.CongressionalDistrict, DT.Age5FC, DT.SexC, DT.Education4C, DT.Race5C]
        synthModelPaths postInfo statePopMap (view GT.stateAbbreviation) (Just "NY")
        (F.rcast @[DT.Age5FC, DT.SexC, DT.Education4C, DT.Race5C])
        (\r -> (r ^. DT.sexC, r ^. DT.race5C))
        (\r -> (r ^. DT.age5FC, r ^. DT.education4C))
        showCellKey
        (Just ("Education", show . view DT.education4C, Just edOrder))
        (Just ("Age", show . view DT.age5FC, Just age5Order))
        (fmap toOutputRow acsSampleA5SER)
        [MethodResult (fmap toOutputRow acsSampleA5SER) (Just "Actual") Nothing Nothing
        ,MethodResult product_SER_A5SR (Just "Product") (Just "Marginal Product") (Just "Product")
        ,MethodResult productNS1_SER_A5SR (Just "Null-Space_v1") (Just "Null-Space_v1") (Just "Null-Space_v1")
        ,MethodResult productNS3_SER_A5SR (Just "Null-Space_v3") (Just "Null-Space_v3") (Just "Null-Space_v3")
        ]
    pure ()
-}

data KLTableRow = KLTableRow { kltrLabel :: Text, kltrKLs :: [(Double, Double)] }

-- This function expects the same length list in the table row as is provided in the col headders argument
klColonnadeFlex :: Foldable f => f Text -> Text -> C.Colonnade C.Headed KLTableRow Text
klColonnadeFlex colHeaders regionName =
  C.headed regionName kltrLabel
  <> mconcat (fmap cols [0..(length chList - 1)])
  where
    chList = FL.fold FL.list colHeaders
    klT :: Double -> Text
    klT x = toText @String $ PF.printf "%2.2e" x
    errT :: Double -> Text
    errT x =  toText @String . PF.printf "%2.0g" . (100 *) . sqrt $ 2 * x
    klCol n = C.headed (chList List.!! n) (klT . fst . (List.!! n) . kltrKLs)
    errCol n =  C.headed "Error (%)" (errT . fst . (List.!! n) . kltrKLs)
    fwCol n = C.headed "Abs Wrong (%)" (errT . snd . (List.!! n) . kltrKLs)
    cols n = klCol n <> errCol n <> fwCol n

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
                 -> Text
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
distCompareChart pp pi' chartDataPrefix vc title key keyText facetB cat1M cat2M asDiffB count scalesM (actualLabel, actualRows) (synthLabel, synthRows) = do
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
  jsonFilePrefix <- K.getNextUnusedId $ "distCompareChart_" <> chartDataPrefix
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
                         , ("Age", GV.Str $ show $ F.rgetField @DT.Age6C r)
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

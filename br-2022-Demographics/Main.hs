{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UnicodeSyntax #-}
--{-# LANGUAGE NoStrictData #-}

module Main
  (main)
where

import qualified BlueRipple.Configuration as BR
import qualified BlueRipple.Model.Demographic.StanModels as SM
import qualified BlueRipple.Model.Demographic.DataPrep as DDP
import qualified BlueRipple.Model.Demographic.EnrichData as DED
import qualified BlueRipple.Model.Demographic.TableProducts as DTP
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
import qualified Knit.Utilities.Streamly as KS
import qualified Text.Pandoc.Error as Pandoc
import qualified System.Console.CmdArgs as CmdArgs
import qualified Colonnade as C

import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.Map.Merge.Strict as MM
import qualified Data.Set as S
import qualified Data.Text as T
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
import qualified BlueRipple.Model.Demographic.DataPrep as DED
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
  , mrProduct :: ResultFrame ks
  , mrProductWithNS :: ResultFrame ks
  , mrModel :: ResultFrame ks
  , mrModelWithCO :: ResultFrame ks
  }


compareResults :: (K.KnitEffects r
--                  , K.KnitOne r
                  , Keyed.FiniteSet colKey
                  , Show colKey
                  , Ord colKey
                  , Keyed.FiniteSet rowKey
                  , Show rowKey
                  , Ord rowKey
                  , F.ElemOf (ks V.++ '[DT.PopCount]) DT.PopCount
                  , FSI.RecVec (ks V.++ '[DT.PopCount])
                  , Ord key
                  , F.ElemOf (ks V.++ '[DT.PopCount]) GT.StateAbbreviation
                  )
               => [Text] -- state abbreviations
               -> Maybe Text
               -> (F.Record (ks V.++ '[DT.PopCount]) -> key)
               -> (F.Record (ks V.++ '[DT.PopCount]) -> rowKey)
               -> (F.Record (ks V.++ '[DT.PopCount]) -> colKey)
               -> MethodResults ks
               -> K.Sem r ()
compareResults allStates exampleStateM catKey rowKey colKey results = do
  let tableText d = toText (C.ascii (fmap toString $ mapColonnade)
                             $ FL.fold (fmap DED.totaledTable
                                         $ DED.rowMajorMapFldInt
                                         (view DT.popCount)
                                         rowKey
                                         colKey
                                       )
                             d
                           )
  case exampleStateM of
    Nothing -> pure ()
    Just sa -> do
      let ff = F.filterFrame ((== sa) . view GT.stateAbbreviation)
      K.logLE K.Info $ "Actual" <> "\n" <> tableText (ff $ mrActual results)
      K.logLE K.Info $ "Product" <> "\n" <> tableText (ff $ mrProduct results)
      K.logLE K.Info $ "Product + NS" <> "\n" <> tableText (ff $ mrProductWithNS results)
      K.logLE K.Info $ "Model" <> "\n" <> tableText (ff $ mrModel results)
      K.logLE K.Info $ "Model + CO" <> "\n" <> tableText (ff $ mrModelWithCO results)
  -- compute KL divergences
  let toVecF = VS.fromList . fmap snd . M.toList . FL.fold (FL.premap (\r -> (catKey r, realToFrac (r ^. DT.popCount))) FL.map)
      computeKL d sa = DED.klDiv' acsV dV where
        acsV = toVecF $ F.filterFrame ((== sa) . view GT.stateAbbreviation) (mrActual results)
        dV = toVecF $ F.filterFrame ((== sa) . view GT.stateAbbreviation) d
  let prodKLs = computeKL (mrProduct results) <$> allStates
      prodNSKLs = computeKL (mrProductWithNS results) <$> allStates
      modelKLs = computeKL (mrModel results) <$> allStates
      modelCOKLs = computeKL (mrModelWithCO results) <$> allStates
      forKLsTable = L.zip5 allStates prodKLs prodNSKLs modelKLs modelCOKLs
  K.logLE K.Info "Divergence Table:"
  K.logLE K.Info $ "\n" <> toText (C.ascii (fmap toString $ klColonnade) forKLsTable)



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
    let seInSERStencil = DTP.stencils @(F.Record [DT.SexC, DT.Education4C]) @(F.Record [DT.SexC, DT.Education4C, DT.Race5C]) F.rcast
        srInSERStencil = DTP.stencils @(F.Record [DT.SexC, DT.Race5C]) @(F.Record [DT.SexC, DT.Education4C, DT.Race5C]) F.rcast
        testStencils = seInSERStencil <> srInSERStencil
        exampleV = DTP.labeledRowsToVec (F.rcast @[DT.SexC, DT.Education4C, DT.Race5C]) (realToFrac . view DT.popCount)
                   $ F.filterFrame ((== "NY") . view GT.stateAbbreviation) acsByPUMA
        mMatrix = DED.mMatrix (VS.length exampleV) seInSERStencil
--        pi = DTP.psuedoInverseSVD mMatrix
    K.logLE K.Info $ "Example keys (in order)\n" <> show (Keyed.elements @(F.Record  [DT.SexC, DT.Education4C, DT.Race5C]))
    K.logLE K.Info $ "mMatrix=\n" <> toText (LA.dispf 2 mMatrix)
    K.logLE K.Info $ "NY       =" <> DED.prettyVector exampleV
    K.logLE K.Info $ "NY (Prod)=" <> (DED.prettyVector $ LA.pinv mMatrix LA.#> (mMatrix LA.#> exampleV))
--    K.knitEither $ Left "Stopping before covariance fold, etc."
    let projCovariancesFld =
          DTP.diffCovarianceFld
          (F.rcast @[GT.StateAbbreviation, GT.PUMA])
          (F.rcast @[DT.SexC, DT.Education4C, DT.Race5C])
          (realToFrac . view DT.popCount)
          testStencils
    K.logLE K.Info $ "Computing covariance matrix of projected differences."
    let projCovariances = FL.fold projCovariancesFld acsByPUMA
    K.logLE K.Info $ "C = " <> toText (LA.dispf 4 $ LA.unSym projCovariances)
    let (eVals, eVecs) = LA.eigSH projCovariances
    K.logLE K.Info $ "eVals=" <> show eVals
    K.logLE K.Info $ "eVecs=" <> toText (LA.dispf 4 eVecs)
    let sigNullVecs = DTP.significantNullVecs 0.95 40 testStencils projCovariances
    K.logLE K.Info $ "snVecs=" <> toText (LA.dispf 4 sigNullVecs)
    let modelData = FL.fold
                    (DTP.nullVecProjectionsModelDataFld
                      sigNullVecs
                      testStencils
                      (F.rcast @[GT.StateAbbreviation, GT.PUMA])
                      (F.rcast @[DT.SexC, DT.Education4C, DT.Race5C])
                      (realToFrac . view DT.popCount)
                      DTP.model1DatFld
                    )
                    acsByPUMA
--    K.logLE K.Info $ "ModelData: " <> T.intercalate "\n" (show <$> modelData)
    K.logLE K.Info $ "Running model, if necessary."
    let modelConfig = DTP.ModelConfig (fst $ LA.size sigNullVecs) True DTP.designMatrixRow1 DTP.AlphaHierNonCentered DTP.NormalDist
    _ <- DTP.runProjModel @[DT.SexC, DT.Education4C, DT.Race5C] False cmdLine modelConfig sigNullVecs testStencils DTP.model1DatFld


    K.knitEither $ Left "Stopping before products, etc."

    K.logLE K.Info $ "Loading ACS data and building marginal distributions (SER, ASR, CSR) for each state" <> show cmdLine
    acsSample_C <- DDP.cachedACSByState'
    acsSample <-  K.ignoreCacheTime acsSample_C
    let zc :: F.Record '[DT.PopCount, DT.PWPopPerSqMile] = 0 F.&: 0 F.&: V.RNil
        acsSampleWZ_C = fmap (FL.fold
                              (FMR.concatFold
                               $ FMR.mapReduceFold
                               (FMR.noUnpack)
                               (FMR.assignKeysAndData @[BRDF.Year, GT.StateAbbreviation] @[DT.CitizenC, DT.Age4C, DT.SexC, DT.Education4C, DT.Race5C, DT.PopCount, DT.PWPopPerSqMile])
                               (FMR.foldAndLabel
                                (Keyed.addDefaultRec @[DT.CitizenC, DT.Age4C, DT.SexC, DT.Education4C, DT.Race5C] zc)
                                (\k r -> fmap (k F.<+>) r)
                               )
                              )
                             )
                      $ acsSample_C
    acsSampleWZ <- K.ignoreCacheTime acsSampleWZ_C
    let allStates = S.toList $ FL.fold (FL.premap (view GT.stateAbbreviation) FL.set) acsSample
        emptyUnless x y = if x then y else mempty
        logText t k = Just $ t <> ": Joint distribution matching for " <> show k
        zeroPopAndDens :: F.Record [DT.PopCount, DT.PWPopPerSqMile] = 0 F.&: 0 F.&: V.RNil
        tableMatchingDataFunctions = DED.TableMatchingDataFunctions zeroPopAndDens recToCWD cwdToRec cwdN updateCWDCount
        exampleState = "KY"
        rerunMatches = False
    -- SER to CSER
    let toOutputRow :: ([BRDF.Year, GT.StateAbbreviation, DT.CitizenC, DT.SexC, DT.Education4C, DT.Race5C, DT.PopCount]  F.⊆ rs)
                    => F.Record rs -> F.Record [BRDF.Year, GT.StateAbbreviation, DT.CitizenC, DT.SexC, DT.Education4C, DT.Race5C, DT.PopCount]
        toOutputRow = F.rcast
    -- actual
    let acsSampleCSER_C = F.toFrame
                         . FL.fold (FMR.concatFold
                                    $ FMR.mapReduceFold
                                    FMR.noUnpack
                                    (FMR.assignKeysAndData @[BRDF.Year, GT.StateAbbreviation, DT.CitizenC, DT.SexC, DT.Education4C, DT.Race5C] @'[DT.PopCount, DT.PWPopPerSqMile])
                                    (FMR.foldAndAddKey DDP.aggregatePeopleAndDensityF)
                                   )
                         <$> acsSampleWZ_C
    acsSampleCSER <- K.ignoreCacheTime acsSampleCSER_C
    -- marginals, SER and CSR
    let acsSampleSER_C = F.toFrame
                         . FL.fold (FMR.concatFold
                                    $ FMR.mapReduceFold
                                    FMR.noUnpack
                                    (FMR.assignKeysAndData @[BRDF.Year, GT.StateAbbreviation, DT.SexC, DT.Education4C, DT.Race5C] @'[DT.PopCount, DT.PWPopPerSqMile])
                                    (FMR.foldAndAddKey DDP.aggregatePeopleAndDensityF)
                                   )
                         <$> acsSampleWZ_C
    acsSampleSER <- K.ignoreCacheTime acsSampleSER_C
    let acsSampleCSR_C = F.toFrame
                         . FL.fold (FMR.concatFold
                                    $ FMR.mapReduceFold
                                    FMR.noUnpack
                                    (FMR.assignKeysAndData @[BRDF.Year, GT.StateAbbreviation, DT.CitizenC, DT.SexC, DT.Race5C] @[DT.PopCount, DT.PWPopPerSqMile])
                                    (FMR.foldAndAddKey DDP.aggregatePeopleAndDensityF)
                                   )
                         <$> acsSampleWZ_C
    acsSampleCSR <- K.ignoreCacheTime acsSampleCSR_C
    -- product
    product_SER_CSR <- DED.mapPE
                       $ fmap toOutputRow
                       <$> DTP.frameTableProduct @[BRDF.Year, GT.StateAbbreviation, DT.SexC, DT.Race5C] @'[DT.Education4C] @'[DT.CitizenC] @DT.PopCount
                       (fmap F.rcast acsSampleSER) (fmap F.rcast acsSampleCSR)

  -- create stencils
    let serInCSERStencil = DTP.stencils @(F.Record [DT.SexC, DT.Education4C, DT.Race5C]) @(F.Record [DT.CitizenC, DT.SexC, DT.Education4C, DT.Race5C]) F.rcast
        csrInCSERStencil = DTP.stencils @(F.Record [DT.CitizenC, DT.SexC, DT.Race5C]) @(F.Record [DT.CitizenC, DT.SexC, DT.Education4C, DT.Race5C]) F.rcast
        nullSpaceVectors = DTP.nullSpaceVectors
                           (S.size $ Keyed.elements @(F.Record [DT.CitizenC, DT.SexC, DT.Education4C, DT.Race5C]))
                           (serInCSERStencil <> csrInCSERStencil)

--    K.logLE K.Info $ "nullSpaceVectors for SER, CSR in CSER" <> show nullSpaceVectors
    nullSpaceProjections <- DED.mapPE
                            $ DTP.nullSpaceProjections
                            nullSpaceVectors
                            (F.rcast @[BRDF.Year, GT.StateAbbreviation])
                            (F.rcast @[DT.CitizenC, DT.SexC, DT.Education4C, DT.Race5C])
                            (view DT.popCount)
                            (F.rcast @[BRDF.Year, GT.StateAbbreviation, DT.CitizenC, DT.SexC, DT.Education4C, DT.Race5C, DT.PopCount] <$> acsSampleCSER)
                            (F.rcast @[BRDF.Year, GT.StateAbbreviation, DT.CitizenC, DT.SexC, DT.Education4C, DT.Race5C, DT.PopCount] <$> product_SER_CSR)


    let avgNSPUW = DTP.avgNullSpaceProjections (const 1) nullSpaceProjections
        avgNSPW = DTP.avgNullSpaceProjections (id) nullSpaceProjections

        fixedProjections ps = const $ pure ps
        pureM :: Monad m => m b -> FL.FoldM m a b
        pureM x = FL.FoldM (\_ _ -> x) x (const x)

        -- test to make sure when we project and then apply these directly we get our original thing back
        stateProjections ok = pureM $ K.knitMaybe ("NS projections for ok=" <> show ok <> " not found!") $ fmap snd $ M.lookup ok nullSpaceProjections

    K.logLE K.Info $ "uw nsvs=" <> DED.prettyVector avgNSPUW <> "\nw nsvs=" <> DED.prettyVector avgNSPW

    -- product with null-space adjustments
    productNS_SER_CSR <- DED.mapPE
                         $ FL.foldM (DTP.applyNSPWeightsFld
                                     @[BRDF.Year, GT.StateAbbreviation]
                                     @[DT.CitizenC, DT.SexC, DT.Education4C, DT.Race5C]
                                     @DT.PopCount
                                     (Just . show)
                                     nullSpaceVectors
                                     stateProjections
                                    )
                         product_SER_CSR

    -- model & match pipeline
    cFromSER_C <- SM.runModel
                  False cmdLine (SM.ModelConfig () False SM.HCentered True)
                  ("Cit", view DT.citizenC)
                  ("SER", F.rcast @[DT.SexC, DT.Education4C, DT.Race5C], SM.dmrS_ER)

    let csrRec :: (DT.Citizen, DT.Sex, DT.Race5) -> F.Record [DT.CitizenC, DT.SexC, DT.Race5C]
        csrRec (c, s, r) = c F.&: s F.&: r F.&: V.RNil
        csrDSFld =  DED.desiredSumsFld
                    (Sum . F.rgetField @DT.PopCount)
                    (F.rcast @'[BRDF.Year, GT.StateAbbreviation])
                    (\r -> csrRec (r ^. DT.citizenC, r ^. DT.sexC, r ^. DT.race5C))
        csrDS = getSum <<$>> FL.fold csrDSFld acsSample
        serRec :: (DT.Sex, DT.Education4, DT.Race5) -> F.Record [DT.SexC, DT.Education4C, DT.Race5C]
        serRec (s, e, r) = s F.&: e F.&: r F.&: V.RNil
        serDSFld =  DED.desiredSumsFld
                    (Sum . F.rgetField @DT.PopCount)
                    (F.rcast @'[BRDF.Year, GT.StateAbbreviation])
                    (\r -> serRec (r ^. DT.sexC, r ^. DT.education4C, r ^. DT.race5C))
        serDS = getSum <<$>> FL.fold serDSFld acsSample

    let serToCSER_PC eu = fmap
          (\m -> DED.pipelineStep @[DT.SexC, DT.Education4C, DT.Race5C] @'[DT.CitizenC] @_ @_ @_ @DT.PopCount
                 tableMatchingDataFunctions
                 (DED.minConstrained DED.klPObjF)
                 (DED.enrichFrameFromBinaryModelF @DT.CitizenC @DT.PopCount m (view GT.stateAbbreviation) DT.Citizen DT.NonCitizen)
                 (logText "SER -> CSER")
                 (emptyUnless eu
                   $ DED.desiredSumMapToLookup @[DT.CitizenC, DT.SexC, DT.Education4C, DT.Race5C] serDS
                   <> DED.desiredSumMapToLookup @[DT.CitizenC, DT.SexC, DT.Education4C, DT.Race5C] csrDS)
          )
          cFromSER_C

    modelCSERFromCSR <- fmap (fmap toOutputRow)
                        $ BRK.clearIf' rerunMatches  "model/synthJoint/serToCSER_MO.bin" >>=
                        \ck -> K.ignoreCacheTimeM $ BRK.retrieveOrMakeFrame ck
                               ((,) <$> serToCSER_PC False <*> acsSampleSER_C)
                               $ \(p, d) -> DED.mapPE $ p d

    modelCO_CSERFromCSR <- fmap (fmap toOutputRow)
                           $ BRK.clearIf' rerunMatches  "model/synthJoint/serToCSER.bin" >>=
                           \ck -> K.ignoreCacheTimeM $ BRK.retrieveOrMakeFrame ck
                                  ((,) <$> serToCSER_PC True <*> acsSampleSER_C)
                                  $ \(p, d) -> DED.mapPE $ p d

    let mResults :: MethodResults [BRDF.Year, GT.StateAbbreviation, DT.CitizenC, DT.SexC, DT.Education4C, DT.Race5C]
        mResults = (MethodResults (fmap toOutputRow acsSampleCSER) product_SER_CSR productNS_SER_CSR modelCSERFromCSR modelCO_CSERFromCSR)
    compareResults allStates (Just "AK")
      (F.rcast @[DT.CitizenC, DT.SexC, DT.Education4C, DT.Race5C])
      (\r -> (r ^. DT.sexC, r ^. DT.race5C))
      (\r -> (r ^. DT.citizenC, r ^. DT.education4C))
      mResults

{-
    let acsSampleSE2R = F.toFrame
                        $ FL.fold
                        (FMR.concatFold
                         $ FMR.mapReduceFold
                          FMR.noUnpack
                          (FMR.assignKeysAndData @[BRDF.Year, GT.StateAbbreviation, DT.SexC, DT.Race5C] @[DT.Education4C, DT.PopCount])
                          (FMR.ReduceFold $ \k -> fmap (k F.<+>) <$> DED.simplifyFieldFld @DT.Education4C @DT.CollegeGradC DT.education4ToCollegeGrad)
                        )
                        acsSampleSER
    let acsSampleASR_C =  F.toFrame
                         . FL.fold (FMR.concatFold
                                    $ FMR.mapReduceFold
                                    FMR.noUnpack
                                    (FMR.assignKeysAndData @[BRDF.Year, GT.StateAbbreviation, DT.Age4C, DT.SexC, DT.Race5C] @[DT.PopCount, DT.PWPopPerSqMile])
                                    (FMR.foldAndAddKey DDP.aggregatePeopleAndDensityF)
                                   )
                         <$> acsSampleWZ_C
    acsSampleASR <- K.ignoreCacheTime acsSampleASR_C
    let acsSampleA2SR = F.toFrame
                        $ FL.fold
                        (FMR.concatFold
                         $ FMR.mapReduceFold
                          FMR.noUnpack
                          (FMR.assignKeysAndData @[BRDF.Year, GT.StateAbbreviation, DT.SexC, DT.Race5C] @[DT.Age4C, DT.PopCount])
                          (FMR.ReduceFold $ \k -> fmap (k F.<+>) <$> DED.simplifyFieldFld @DT.Age4C @DT.SimpleAgeC DT.age4ToSimple)
                        )
                        acsSampleASR


    serToCSER_Prod <- DED.mapPE
                      $ DTP.frameTableProduct @[BRDF.Year, GT.StateAbbreviation, DT.SexC, DT.Race5C] @'[DT.Education4C] @'[DT.CitizenC] @DT.PopCount
                      (fmap F.rcast acsSampleSER) (fmap F.rcast acsSampleCSR)
    serToCASER_Prod <- DED.mapPE
                       $ DTP.frameTableProduct @[BRDF.Year, GT.StateAbbreviation, DT.SexC, DT.Race5C] @'[DT.CitizenC, DT.Education4C] @'[DT.Age4C] @DT.PopCount
                       (fmap F.rcast serToCSER_Prod) (fmap F.rcast acsSampleASR)
    K.logLE K.Info $ "\n" <> toText (C.ascii (fmap toString $ mapColonnade)
                                      $ FL.fold (fmap DED.totaledTable
                                                  $ DED.rowMajorMapFldInt
                                                  (view DT.popCount)
                                                  (\r -> (r ^. DT.sexC, r ^. DT.race5C))
                                                  (\r -> (r ^. DT.citizenC, r ^. DT.education4C))
                                                )
                                      $ fmap (F.rcast @[DT.CitizenC, DT.SexC, DT.Education4C, DT.Race5C, DT.PopCount])
                                      $ F.filterFrame ((== exampleState) . (^. GT.stateAbbreviation)) serToCSER_Prod
                                    )
    K.logLE K.Info $ "SER -> CASER (Table Product)"
    K.logLE K.Info $ "\n" <> toText (C.ascii (fmap toString $ mapColonnade)
                                      $ FL.fold (fmap DED.totaledTable
                                                  $ DED.rowMajorMapFldInt
                                                  (view DT.popCount)
                                                  (\r -> ( r ^. DT.age4C, r ^. DT.sexC, r ^. DT.race5C))
                                                  (\r -> (r ^. DT.citizenC, r ^. DT.education4C))
                                                )
                                      $ fmap (F.rcast @[DT.Age4C, DT.CitizenC, DT.SexC, DT.Education4C, DT.Race5C, DT.PopCount])
                                      $ F.filterFrame ((== exampleState) . (^. GT.stateAbbreviation)) serToCASER_Prod
                                    )


    -- create stencils
    let serInCSERStencil :: [DED.Stencil Int]
        serInCSERStencil = DTP.stencils @[DT.SexC, DT.Education4C, DT.Race5C] @[DT.CitizenC, DT.SexC, DT.Education4C, DT.Race5C]
    K.logLE K.Info $ "serInCSER stencils" <> show serInCSERStencil

    let csrInCSERStencil = DTP.stencils @[DT.CitizenC, DT.SexC, DT.Race5C] @[DT.CitizenC, DT.SexC, DT.Education4C, DT.Race5C]
        nullSpaceVectors = DTP.nullSpaceVectors
                           (S.size $ Keyed.elements @(F.Record [DT.CitizenC, DT.SexC, DT.Education4C, DT.Race5C]))
                           (serInCSERStencil <> csrInCSERStencil)

--    K.logLE K.Info $ "nullSpaceVectors for SER, CSR in CSER" <> show nullSpaceVectors
    avgNSP <- DED.mapPE
              $ DTP.averageNullSpaceProjections
              nullSpaceVectors
              (F.rcast @[BRDF.Year, GT.StateAbbreviation])
              (F.rcast @[DT.CitizenC, DT.SexC, DT.Education4C, DT.Race5C])
              (view DT.popCount)
              (F.rcast @[BRDF.Year, GT.StateAbbreviation, DT.CitizenC, DT.SexC, DT.Education4C, DT.Race5C, DT.PopCount] <$> acsSampleCSER)
              (F.rcast @[BRDF.Year, GT.StateAbbreviation, DT.CitizenC, DT.SexC, DT.Education4C, DT.Race5C, DT.PopCount] <$> serToCSER_Prod)
    K.logLE K.Info $ "Average NSPs=" <> DED.prettyVector avgNSP


-}

    K.knitEither $ Left "Stopping before models, etc."
{-
  let  a2srRec :: (DT.SimpleAge, DT.Sex, DT.Race5) -> F.Record [DT.SimpleAgeC, DT.SexC, DT.Race5C]
       a2srRec (a, s, r) = a F.&: s F.&: r F.&: V.RNil
       a2srDSFld =  DED.desiredSumsFld
                    (Sum . F.rgetField @DT.PopCount)
                    (F.rcast @'[GT.StateAbbreviation])
                    (\r -> a2srRec (DT.age4ToSimple $ r ^. DT.age4C, r ^. DT.sexC, r ^. DT.race5C))
       a2srDS = getSum <<$>> FL.fold a2srDSFld acsSample

       asrRec :: (DT.Age4, DT.Sex, DT.Race5) -> F.Record [DT.Age4C, DT.SexC, DT.Race5C]
       asrRec (a, s, r) = a F.&: s F.&: r F.&: V.RNil
       asrDSFld =  DED.desiredSumsFld
                   (Sum . F.rgetField @DT.PopCount)
                   (F.rcast @'[GT.StateAbbreviation])
                   (\r -> asrRec (r ^. DT.age4C, r ^. DT.sexC, r ^. DT.race5C))
       asrDS = getSum <<$>> FL.fold asrDSFld acsSample


       sgrRec :: (DT.Sex, DT.CollegeGrad, DT.Race5) -> F.Record [DT.SexC, DT.CollegeGradC, DT.Race5C]
       sgrRec (s, e, r) = s F.&: e F.&: r F.&: V.RNil
       sgrDSFld =  DED.desiredSumsFld
                   (Sum . F.rgetField @DT.PopCount)
                   (F.rcast @'[GT.StateAbbreviation])
                   (\r -> sgrRec (r ^. DT.sexC, DT.education4ToCollegeGrad $ r ^. DT.education4C, r ^. DT.race5C))
       sgrDS = getSum <<$>> FL.fold sgrDSFld acsSample



    K.logLE K.Info $ "Building/rebuilding necessary models " <> show cmdLine
    a2FromCSR_C <- SM.runModel
                   False cmdLine (SM.ModelConfig () False SM.HCentered True)
                   ("Age", DT.age4ToSimple . view DT.age4C)
                   ("CSR", F.rcast @[DT.CitizenC, DT.SexC, DT.Race5C], SM.dmrS_CR)
    let ca2srKey :: F.Record DDP.ACSByStateR -> F.Record [DT.CitizenC, DT.SimpleAgeC, DT.SexC, DT.Race5C]
        ca2srKey r = r ^. DT.citizenC F.&: DT.age4ToSimple (r ^. DT.age4C) F.&: r ^. DT.sexC F.&: r ^. DT.race5C F.&: V.RNil
    e2FromCA2SR_C <- SM.runModel
                    False cmdLine (SM.ModelConfig () False SM.HCentered True)
                    ("Edu", DT.education4ToCollegeGrad . view DT.education4C)
                    ("CASR", ca2srKey, SM.dmrC_S_A2R)

    a2FromCSER_C <- SM.runModel
                   False cmdLine (SM.ModelConfig () False SM.HCentered True)
                   ("Age", DT.age4ToSimple . view DT.age4C)
                   ("CSER", F.rcast  @[DT.CitizenC, DT.SexC, DT.Education4C, DT.Race5C], SM.dmrC_S_ER)

    cFromASR_C <- SM.runModel
                  False cmdLine (SM.ModelConfig () False SM.HCentered True)
                  ("Cit", view DT.citizenC)
                  ("ASR", F.rcast @[DT.Age4C, DT.SexC, DT.Race5C], SM.dmrS_AR)

    e2FromCASR_C <- SM.runModel
                   False cmdLine (SM.ModelConfig () False SM.HCentered True)
                   ("Grad", DT.education4ToCollegeGrad . view DT.education4C)
                   ("CASR", F.rcast @[DT.CitizenC, DT.Age4C, DT.SexC, DT.Race5C], SM.dmrS_C_AR)

    -- target tables

    let toCA2SER :: F.Record [BRDF.Year, GT.StateAbbreviation, DT.CitizenC, DT.Age4C, DT.SexC, DT.Education4C, DT.Race5C, DT.PopCount, DT.PWPopPerSqMile]
                 -> F.Record [GT.StateAbbreviation, DT.CitizenC, DT.SimpleAgeC, DT.SexC, DT.Education4C, DT.Race5C]
        toCA2SER  r = r ^. GT.stateAbbreviation
                      F.&: r ^. DT.citizenC
                      F.&: DT.age4ToSimple (r ^. DT.age4C)
                      F.&: r ^. DT.sexC
                      F.&: r ^. DT.education4C
                      F.&: r ^. DT.race5C
                      F.&: V.RNil
        acsCA2SER = FL.fold (FMR.concatFold
                    $ FMR.mapReduceFold
                             (FMR.noUnpack)
                             (FMR.assign toCA2SER (F.rcast @'[DT.PopCount, DT.PWPopPerSqMile]))
                             (FMR.foldAndAddKey $ DED.aggregatePeopleAndDensityF)
                            )
                    acsSampleWZ

    let


        cserToCA2SER_PC eu = fmap
          (\m -> DED.pipelineStep @[DT.CitizenC, DT.SexC, DT.Education4C, DT.Race5C] @'[DT.SimpleAgeC] @_ @_ @_ @DT.PopCount
                 tableMatchingDataFunctions
                 (DED.minConstrained DED.klPObjF)
                 (DED.enrichFrameFromBinaryModelF @DT.SimpleAgeC @DT.PopCount m (view GT.stateAbbreviation) DT.EqualOrOver DT.Under)
                 (logText "CSER -> CASER")
                 (emptyUnless eu
                   $ DED.desiredSumMapToLookup @[DT.SimpleAgeC, DT.CitizenC, DT.SexC, DT.Education4C, DT.Race5C] serDS
                   <> DED.desiredSumMapToLookup @[DT.SimpleAgeC, DT.CitizenC, DT.SexC, DT.Education4C, DT.Race5C] csrDS
                   <>  DED.desiredSumMapToLookup @[DT.SimpleAgeC, DT.CitizenC, DT.SexC, DT.Education4C, DT.Race5C] a2srDS
                 )
          )
          a2FromCSER_C

        asrToCASR_PC eu =  fmap
          (\m -> DED.pipelineStep @[DT.Age4C, DT.SexC, DT.Race5C] @'[DT.CitizenC] @_ @_ @_ @DT.PopCount
                 tableMatchingDataFunctions
                 (DED.minConstrained DED.klPObjF)
                 (DED.enrichFrameFromBinaryModelF @DT.CitizenC @DT.PopCount m (view GT.stateAbbreviation) DT.Citizen DT.NonCitizen)
                 (logText "ASR -> CASR")
                 (emptyUnless eu
                  $ DED.desiredSumMapToLookup @[DT.CitizenC, DT.Age4C, DT.SexC, DT.Race5C] asrDS
                  <> DED.desiredSumMapToLookup @[DT.CitizenC, DT.Age4C, DT.SexC, DT.Race5C] csrDS
                 )
          )
          cFromASR_C

        casrToCASE2R_PC eu =  fmap
          (\m -> DED.pipelineStep @[DT.CitizenC, DT.Age4C, DT.SexC, DT.Race5C] @'[DT.CollegeGradC] @_ @_ @_ @DT.PopCount
                 tableMatchingDataFunctions
                 (DED.minConstrained DED.klPObjF)
                 (DED.enrichFrameFromBinaryModelF @DT.CollegeGradC @DT.PopCount m (view GT.stateAbbreviation) DT.Grad DT.NonGrad)
                 (logText "CASR -> CASGR")
                 (emptyUnless eu
                   $ DED.desiredSumMapToLookup @[DT.CollegeGradC, DT.CitizenC, DT.Age4C, DT.SexC, DT.Race5C] csrDS
                   <> DED.desiredSumMapToLookup @[DT.CollegeGradC, DT.CitizenC, DT.Age4C, DT.SexC, DT.Race5C] asrDS
                   <> DED.desiredSumMapToLookup @[DT.CollegeGradC, DT.CitizenC, DT.Age4C, DT.SexC, DT.Race5C] sgrDS
                 )
          )
          e2FromCASR_C

        csrToCA2SR_PC eu =  fmap
          (\m -> DED.pipelineStep @[DT.CitizenC, DT.SexC, DT.Race5C] @'[DT.SimpleAgeC] @_ @_ @_ @DT.PopCount
                 tableMatchingDataFunctions
                 (DED.minConstrained DED.klPObjF)
                 (DED.enrichFrameFromBinaryModelF @DT.SimpleAgeC @DT.PopCount m (view GT.stateAbbreviation) DT.EqualOrOver DT.Under)
                 (logText "CSR -> CA2SR")
                 (emptyUnless eu
                   $ DED.desiredSumMapToLookup @[DT.SimpleAgeC, DT.CitizenC, DT.SexC, DT.Race5C] csrDS
                   <> DED.desiredSumMapToLookup @[DT.SimpleAgeC, DT.CitizenC, DT.SexC, DT.Race5C] a2srDS
                 )
          )
          a2FromCSR_C

        ca2srToCA2SE2R_PC eu =  fmap
          (\m -> DED.pipelineStep @[DT.CitizenC, DT.SimpleAgeC, DT.SexC, DT.Race5C] @'[DT.CollegeGradC] @_ @_ @_ @DT.PopCount
                 tableMatchingDataFunctions
                 (DED.minConstrained DED.klPObjF)
                 (DED.enrichFrameFromBinaryModelF @DT.CollegeGradC @DT.PopCount m (view GT.stateAbbreviation) DT.Grad DT.NonGrad)
                 (logText "CA2SR -> CA2SGR")
                 (emptyUnless eu
                   $ DED.desiredSumMapToLookup @[DT.CollegeGradC, DT.CitizenC, DT.SimpleAgeC, DT.SexC, DT.Race5C] csrDS
                   <> DED.desiredSumMapToLookup @[DT.CollegeGradC, DT.CitizenC, DT.SimpleAgeC, DT.SexC, DT.Race5C] a2srDS
                   <> DED.desiredSumMapToLookup @[DT.CollegeGradC, DT.CitizenC, DT.SimpleAgeC, DT.SexC, DT.Race5C] sgrDS
                 )
          )
          e2FromCA2SR_C

    K.logLE K.Info "sample ACS data, ages simplified"
    let acsCA2SE2RSampleKey :: F.Record DDP.ACSByStateR -> F.Record [DT.CitizenC, DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.Race5C]
        acsCA2SE2RSampleKey r = r ^. DT.citizenC
                         F.&: DT.age4ToSimple (r ^. DT.age4C)
                         F.&: r ^. DT.sexC
                         F.&: DT.education4ToCollegeGrad (r ^. DT.education4C)
                         F.&: r ^. DT.race5C
                         F.&: V.RNil
        acsCA2SE2RSampleVecF t = FL.fold (DED.vecFld (realToFrac . getSum) (Sum . view DT.popCount) acsCA2SE2RSampleKey) t
        acsCA2SE2RSampleVec = acsCA2SE2RSampleVecF $ F.filterFrame ((== exampleState) . view GT.stateAbbreviation) acsSample
        acsCASERSampleVecF t = FL.fold
                               (DED.vecFld (realToFrac . getSum)
                                 (Sum . view DT.popCount) (F.rcast @[DT.CitizenC, DT.Age4C, DT.SexC, DT.Education4C, DT.Race5C])) t
        acsCASERSampleVec = acsCASERSampleVecF $ F.filterFrame ((== exampleState) . view GT.stateAbbreviation) acsSample
    let table af ef sa dat = FL.fold (fmap DED.totaledTable
                                      $ DED.rowMajorMapFldInt
                                      (F.rgetField @DT.PopCount)
                                      (\r -> (af r, F.rgetField @DT.SexC r, F.rgetField @DT.Race5C r))
                                      (\r -> (F.rgetField @DT.CitizenC r, ef r))
                                     )
                             $ fmap (F.rcast @[DT.CitizenC, DT.SimpleAgeC, DT.SexC, DT.Education4C, DT.Race5C, DT.PopCount])
                             $ F.filterFrame ((== sa) . (^. GT.stateAbbreviation)) dat
    K.logLE K.Info $ "\n" <> toText (C.ascii (fmap toString $ mapColonnade)
                                      $ FL.fold (fmap DED.totaledTable
                                                  $ DED.rowMajorMapFldInt
                                                  (view DT.popCount)
                                                  (\r -> (DT.age4ToSimple (r ^. DT.age4C), r ^. DT.sexC, r ^. DT.race5C))
                                                  (\r -> (r ^. DT.citizenC, DT.education4ToCollegeGrad (r ^. DT.education4C)))
                                                 )
                                      $ fmap (F.rcast @[DT.CitizenC, DT.Age4C, DT.SexC, DT.Education4C, DT.Race5C, DT.PopCount])
                                      $ F.filterFrame ((== exampleState) . (^. GT.stateAbbreviation)) acsSample
                                    )


    let computeKL acsV f d sa = DED.klDiv acsV dV where
--          acsV = acsSampleVecF $ F.filterFrame ((== sa) . view GT.stateAbbreviation) acsSample
          dV = f $ fmap F.rcast $ F.filterFrame ((== sa) . view GT.stateAbbreviation) d


    K.logLE K.Info $ "ACS Input to SER -> CASER"
    K.logLE K.Info $ "\n" <> toText (C.ascii (fmap toString $ mapColonnade)
                                      $ FL.fold (fmap DED.totaledTable
                                                  $ DED.rowMajorMapFldInt
                                                  (view DT.popCount)
                                                  (\r -> (r ^. DT.sexC, r ^. DT.race5C))
                                                  (\r -> r ^. DT.education4C)
                                                 )
                                      $ fmap (F.rcast @[DT.SexC, DT.Education4C, DT.Race5C, DT.PopCount])
                                      $ F.filterFrame ((== exampleState) . (^. GT.stateAbbreviation)) acsSampleSER
                                    )
    K.logLE K.Info $ "SER -> CSER (Model Only)"
    serToCSER_MO <-  BRK.clearIf' rerunMatches  "model/synthJoint/serToCSER_MO.bin" >>=
                     \ck -> K.ignoreCacheTimeM $ BRK.retrieveOrMakeFrame ck
                            ((,) <$> serToCSER_PC False <*> acsSampleSER_C)
                            $ \(p, d) -> DED.mapPE $ p d
    K.logLE K.Info $ "\n" <> toText (C.ascii (fmap toString $ mapColonnade)
                                      $ FL.fold (fmap DED.totaledTable
                                                  $ DED.rowMajorMapFldInt
                                                  (view DT.popCount)
                                                  (\r -> (r ^. DT.sexC, r ^. DT.race5C))
                                                  (\r -> (r ^. DT.citizenC, r ^. DT.education4C))
                                                )
                                      $ fmap (F.rcast @[DT.CitizenC, DT.SexC, DT.Education4C, DT.Race5C, DT.PopCount])
                                      $ F.filterFrame ((== exampleState) . (^. GT.stateAbbreviation)) serToCSER_MO
                                    )
    K.logLE K.Info $ "SER -> CSER (Table Product)"

    K.logLE K.Info $ "SER -> CSER"
    serToCSER <-  BRK.clearIf' rerunMatches  "model/synthJoint/serToCSER.bin" >>=
                  \ck -> K.ignoreCacheTimeM $ BRK.retrieveOrMakeFrame ck
                         ((,) <$> serToCSER_PC True <*> acsSampleSER_C)
                         $ \(p, d) -> DED.mapPE $ p d

    K.logLE K.Info $ "\n" <> toText (C.ascii (fmap toString $ mapColonnade)
                                      $ FL.fold (fmap DED.totaledTable
                                                  $ DED.rowMajorMapFldInt
                                                  (view DT.popCount)
                                                  (\r -> (r ^. DT.sexC, r ^. DT.race5C))
                                                  (\r -> (r ^. DT.citizenC, r ^. DT.education4C))
                                                )
                                      $ fmap (F.rcast @[DT.CitizenC, DT.SexC, DT.Education4C, DT.Race5C, DT.PopCount])
                                      $ F.filterFrame ((== exampleState) . (^. GT.stateAbbreviation)) serToCSER
                                    )
    K.logLE K.Info $ "SER -> CASER (2nd step, model only)"
    serToCA2SER_M1 <- BRK.clearIf' rerunMatches  "model/synthJoint/serToCASER_M1.bin" >>=
                      \ck -> K.ignoreCacheTimeM $ BRK.retrieveOrMakeFrame ck
                             ((,,) <$> serToCSER_PC True <*> cserToCA2SER_PC False <*> acsSampleSER_C)
                             $ \(p1, p2, d) -> DED.mapPE $ (p1 >=> p2) d

    K.logLE K.Info $ "\n" <> toText (C.ascii (fmap toString $ mapColonnade)
                                      $ FL.fold (fmap DED.totaledTable
                                                  $ DED.rowMajorMapFldInt
                                                  (view DT.popCount)
                                                  (\r -> ( r ^. DT.simpleAgeC, r ^. DT.sexC, r ^. DT.race5C))
                                                  (\r -> (r ^. DT.citizenC, r ^. DT.education4C))
                                                )
                                      $ fmap (F.rcast @[DT.SimpleAgeC, DT.CitizenC, DT.SexC, DT.Education4C, DT.Race5C, DT.PopCount])
                                      $ F.filterFrame ((== exampleState) . (^. GT.stateAbbreviation)) serToCA2SER_M1
                                    )

    K.logLE K.Info "SER -> CSER -> CASER pipeline."
    serToCA2SE2R <-  BRK.clearIf' rerunMatches  "model/synthJoint/serToCASER.bin" >>=
                   \ck -> K.ignoreCacheTimeM $ BRK.retrieveOrMakeFrame ck
                          ((,,) <$> serToCSER_PC True <*> cserToCA2SER_PC True <*> acsSampleSER_C)
                          $ \(p1, p2, d) -> DED.mapPE $ (p1 >=> p2) d

    let serToCA2SE2RKey :: F.Record [GT.StateAbbreviation, DT.CitizenC, DT.SimpleAgeC, DT.SexC, DT.Education4C, DT.Race5C, DT.PopCount]
                      -> F.Record [DT.CitizenC, DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.Race5C]
        serToCA2SE2RKey r = r ^. DT.citizenC
                         F.&: r ^. DT.simpleAgeC
                         F.&: r ^. DT.sexC
                         F.&: DT.education4ToCollegeGrad (r ^. DT.education4C)
                         F.&: r ^. DT.race5C
                         F.&: V.RNil
        serToCA2SE2RVecF t = FL.fold (DED.vecFld (realToFrac . getSum) (Sum . view DT.popCount) serToCA2SE2RKey) t
        serToCASERKey :: F.Record [GT.StateAbbreviation, DT.CitizenC, DT.Age4C, DT.SexC, DT.Education4C, DT.Race5C, DT.PopCount]
                      -> F.Record [DT.CitizenC, DT.Age4C, DT.SexC, DT.Education4C, DT.Race5C]
        serToCASERKey = F.rcast
        serToCASERVecF t = FL.fold (DED.vecFld (realToFrac . getSum) (Sum . view DT.popCount) serToCASERKey) t

        serToCA2SE2RKL = computeKL acsCA2SE2RSampleVec serToCA2SE2RVecF serToCA2SE2R exampleState
        serToCA2SE2R_KLs = computeKL acsCA2SE2RSampleVec serToCA2SE2RVecF serToCA2SE2R <$> allStates
--    K.logLE K.Info $ "KL divergences (SER -> CASER, model only)" <> show (zip allStates serToCASER_MO_KLs)
    K.logLE K.Info $ "KL divergence =" <> show serToCA2SE2RKL
    K.logLE K.Info $ "\n" <> toText (C.ascii (fmap toString $ mapColonnade)
                                      $ table (view DT.simpleAgeC) (view DT.education4C) exampleState serToCA2SE2R)

    serToCA2SE2R_MO <- BRK.clearIf' rerunMatches  "model/synthJoint/serToCASER_MO.bin" >>=
                     \ck -> K.ignoreCacheTimeM $ BRK.retrieveOrMakeFrame ck
                            ((,,) <$> serToCSER_PC False <*> cserToCA2SER_PC False <*> acsSampleSER_C)
                            $ \(p1, p2, d) -> DED.mapPE $ (p1 >=> p2) d

    let serToCA2SE2R_MO_KLs = computeKL acsCA2SE2RSampleVec serToCA2SE2RVecF serToCA2SE2R_MO <$> allStates
        serToCASER_Prod_KLs = computeKL acsCASERSampleVec serToCASERVecF serToCASER_Prod <$> allStates
    K.logLE K.Info $ "Model only\n" <> toText (C.ascii (fmap toString $ mapColonnade)
                                      $ table (view DT.simpleAgeC) (DT.education4ToCollegeGrad . view DT.education4C) exampleState serToCA2SE2R_MO)

    K.logLE K.Info "Running ASR -> CASR -> CASE2R pipeline (Model Only)"
    asrToCASGR_MO <-  BRK.clearIf' rerunMatches  "model/synthJoint/asrToCASE2R_MO.bin" >>=
                      \ck -> K.ignoreCacheTimeM $ BRK.retrieveOrMakeFrame ck
                      ((,,) <$> asrToCASR_PC False <*> casrToCASE2R_PC False <*> acsSampleASR_C)
                      $ \(p1, p2, d) -> DED.mapPE $ (p1 >=> p2) d
    asrToCASR_Prod <- DED.mapPE
                      $ DTP.frameTableProduct @[BRDF.Year, GT.StateAbbreviation, DT.SexC, DT.Race5C] @'[DT.Age4C] @'[DT.CitizenC] @DT.PopCount
                      (fmap F.rcast acsSampleASR) (fmap F.rcast acsSampleCSR)
    asrToCASE2R_Prod <- DED.mapPE
                        $ DTP.frameTableProduct @[BRDF.Year, GT.StateAbbreviation, DT.SexC, DT.Race5C] @'[DT.CitizenC, DT.Age4C] @'[DT.CollegeGradC] @DT.PopCount
                        (fmap F.rcast asrToCASR_Prod) (fmap F.rcast acsSampleSE2R)
    K.logLE K.Info "Running ASR -> CASR -> CASE2R pipeline"
    when rerunMatches $ BRK.clearIfPresentD  "model/synthJoint/asrToCASE2R.bin"
    asrToCASGR <- K.ignoreCacheTimeM $ BRK.retrieveOrMakeFrame "model/synthJoint/asrToCASE2R.bin"
                  ((,,) <$> asrToCASR_PC True <*> casrToCASE2R_PC True <*> acsSampleASR_C)
                  $ \(p1, p2, d) -> DED.mapPE $ (p1 >=> p2) d

    let asrToCASGRKey :: F.Record [GT.StateAbbreviation, DT.CitizenC, DT.Age4C, DT.SexC, DT.CollegeGradC, DT.Race5C, DT.PopCount]
                      -> F.Record [DT.CitizenC, DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.Race5C]
        asrToCASGRKey r = r ^. DT.citizenC
                         F.&: DT.age4ToSimple (r ^. DT.age4C)
                         F.&: r ^. DT.sexC
                         F.&: r ^. DT.collegeGradC
                         F.&: r ^. DT.race5C
                         F.&: V.RNil
        asrToCASGRVecF t = FL.fold (DED.vecFld (realToFrac . getSum) (Sum . view DT.popCount) asrToCASGRKey) t
        asrToCASGRVec = asrToCASGRVecF $ fmap F.rcast $ F.filterFrame ((== exampleState) . view GT.stateAbbreviation) asrToCASGR
        asrToCASGRKL = DED.klDiv acsCA2SE2RSampleVec asrToCASGRVec
        asrToCASGR_MO_KLs = computeKL acsCA2SE2RSampleVec asrToCASGRVecF asrToCASGR_MO <$> allStates
        asrToCASGR_KLs = computeKL acsCA2SE2RSampleVec asrToCASGRVecF asrToCASGR <$> allStates
        asrToCASGR_Prod_KLs = computeKL acsCA2SE2RSampleVec asrToCASGRVecF asrToCASE2R_Prod <$> allStates
    K.logLE K.Info $ "KL divergence =" <> show asrToCASGRKL
    K.logLE K.Info $ "\n" <> toText (C.ascii (fmap toString $ mapColonnade)
                                      $ FL.fold (fmap DED.totaledTable
                                                  $ DED.rowMajorMapFldInt
                                                  (view DT.popCount)
                                                  (\r -> (DT.age4ToSimple (r ^. DT.age4C), r ^. DT.sexC, r ^. DT.race5C))
                                                  (\r -> (r ^. DT.citizenC, r ^. DT.collegeGradC))
                                                 )
                                      $ fmap (F.rcast @[DT.CitizenC, DT.Age4C, DT.SexC, DT.CollegeGradC, DT.Race5C, DT.PopCount])
                                      $ F.filterFrame ((== exampleState) . (^. GT.stateAbbreviation)) asrToCASGR
                                    )


    K.logLE K.Info "Running CSR -> CA2SR -> CA2SE2R pipeline (Model Only)."
    csrToCASER_MO <-  BRK.clearIf' rerunMatches  "model/synthJoint/csrToCA2SE2R_MO.bin" >>=
                      \ck -> K.ignoreCacheTimeM $ BRK.retrieveOrMakeFrame ck
                      ((,,) <$> csrToCA2SR_PC False <*> ca2srToCA2SE2R_PC False <*> acsSampleCSR_C)
                      $ \(p1, p2, d) -> DED.mapPE $ (p1 >=> p2) d
    csrToCA2SR_Prod <- DED.mapPE
                      $ DTP.frameTableProduct @[BRDF.Year, GT.StateAbbreviation, DT.SexC, DT.Race5C] @'[DT.CitizenC] @'[DT.SimpleAgeC] @DT.PopCount
                      (fmap F.rcast acsSampleCSR) (fmap F.rcast acsSampleA2SR)
    csrToCA2SE2R_Prod <- DED.mapPE
                        $ DTP.frameTableProduct @[BRDF.Year, GT.StateAbbreviation, DT.SexC, DT.Race5C] @'[DT.CitizenC, DT.SimpleAgeC] @'[DT.CollegeGradC] @DT.PopCount
                        (fmap F.rcast csrToCA2SR_Prod) (fmap F.rcast acsSampleSE2R)
    K.logLE K.Info "Running CSR -> CA2SR -> CA2SE2R pipeline."
    csrToCASER <- BRK.clearIf' rerunMatches "model/synthJoint/csrToCA2SE2R.bin" >>=
                  \ck -> K.ignoreCacheTimeM $ BRK.retrieveOrMakeFrame ck
                  ((,,) <$> csrToCA2SR_PC True <*> ca2srToCA2SE2R_PC True <*> acsSampleCSR_C)
                  $ \(p1, p2, d) -> DED.mapPE $ (p1 >=> p2) d
    let csrToCASERKey :: F.Record [GT.StateAbbreviation, DT.CitizenC, DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.Race5C, DT.PopCount]
                      -> F.Record [DT.CitizenC, DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.Race5C]
        csrToCASERKey r = r ^. DT.citizenC
                         F.&: r ^. DT.simpleAgeC
                         F.&: r ^. DT.sexC
                         F.&: r ^. DT.collegeGradC
                         F.&: r ^. DT.race5C
                         F.&: V.RNil
        csrToCASERVecF t = FL.fold (DED.vecFld (realToFrac . getSum) (Sum . view DT.popCount) csrToCASERKey) t
        csrToCASERVec = csrToCASERVecF $ fmap F.rcast $ F.filterFrame ((== exampleState) . view GT.stateAbbreviation) csrToCASER
        csrToCASERKL = DED.klDiv acsCA2SE2RSampleVec csrToCASERVec
        csrToCASER_MO_KLs = computeKL acsCA2SE2RSampleVec csrToCASERVecF csrToCASER_MO <$> allStates
        csrToCASER_KLs = computeKL acsCA2SE2RSampleVec csrToCASERVecF csrToCASER <$> allStates
        csrToCASER_Prod_KLs = computeKL acsCA2SE2RSampleVec csrToCASERVecF csrToCA2SE2R_Prod <$> allStates
    K.logLE K.Info $ "KL divergence =" <> show csrToCASERKL
    K.logLE K.Info $ "\n" <> toText (C.ascii (fmap toString $ mapColonnade)
                                      $ FL.fold (fmap DED.totaledTable
                                                  $ DED.rowMajorMapFldInt
                                                  (view DT.popCount)
                                                  (\r -> (r ^. DT.simpleAgeC, r ^. DT.sexC, r ^. DT.race5C))
                                                  (\r -> (r ^. DT.citizenC, r ^. DT.collegeGradC))
                                                 )
                                      $ fmap (F.rcast @[DT.CitizenC, DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.Race5C, DT.PopCount])
                                      $ F.filterFrame ((== exampleState) . (^. GT.stateAbbreviation)) csrToCASER
                                    )


    let allKLs = L.zip4 allStates serToCA2SE2R_MO_KLs serToCA2SE2R_KLs serToCASER_Prod_KLs --asrToCASGR_MO_KLs asrToCASGR_KLs csrToCASER_MO_KLs csrToCASER_KLs
--    K.logLE K.Info "Divergence Table:"
--    K.logLE K.Info $ "\n" <> toText (C.ascii (fmap toString $ klColonnade) allKLs)
    let postInfo = BR.PostInfo (BR.postStage cmdLine) (BR.PubTimes BR.Unpublished Nothing)
    synthModelPaths <- postPaths "SynthModel" cmdLine
    BRK.brNewPost synthModelPaths postInfo "SynthModel" $ do
      let cellKey r = (r ^. GT.stateAbbreviation, r ^. DT.citizenC, r ^. DT.simpleAgeC, r ^. DT.sexC, r ^. DT.education4C, r ^. DT.race5C)
      modelVL <- K.knitEither
                 $ distCompareChart
                 (FV.ViewConfig 500 500 5)
                 "Model Only"
                 (F.rcast @[GT.StateAbbreviation, DT.CitizenC, DT.SimpleAgeC, DT.SexC, DT.Education4C, DT.Race5C])
                 (show . cellKey)
                 (Just ("Race/Ethnicity", show . view DT.race5C))
                 (Just ("2-way Age 45", show . view DT.simpleAgeC))
                 (realToFrac . view DT.popCount)
                 ("Actual ACS", (F.rcast @[GT.StateAbbreviation, DT.CitizenC, DT.SimpleAgeC, DT.SexC, DT.Education4C, DT.Race5C, DT.PopCount]) <$> acsCA2SER)
                 ("SER -> CASER (Model Only)", (F.rcast @[GT.StateAbbreviation, DT.CitizenC, DT.SimpleAgeC, DT.SexC, DT.Education4C, DT.Race5C, DT.PopCount]) <$> serToCA2SE2R_MO)
      _ <- K.addHvega Nothing Nothing modelVL
      modelAndMatchVL <- K.knitEither
                 $ distCompareChart
                 (FV.ViewConfig 500 500 5)
                 "Model & Match via Constrained Optimization"
                 (F.rcast @[GT.StateAbbreviation, DT.CitizenC, DT.SimpleAgeC, DT.SexC, DT.Education4C, DT.Race5C])
                 (show . cellKey)
                 (Just ("Race/Ethnicity", show . view DT.race5C))
                 (Just ("2-Way Age 45", show . view DT.simpleAgeC))
                 (realToFrac . view DT.popCount)
                 ("Actual ACS", (F.rcast @[GT.StateAbbreviation, DT.CitizenC, DT.SimpleAgeC, DT.SexC, DT.Education4C, DT.Race5C, DT.PopCount]) <$> acsCA2SER)
                 ("SER -> CASER", (F.rcast @[GT.StateAbbreviation, DT.CitizenC, DT.SimpleAgeC, DT.SexC, DT.Education4C, DT.Race5C, DT.PopCount]) <$> serToCA2SE2R)
      _ <- K.addHvega Nothing Nothing modelAndMatchVL
      let log10 = Numeric.logBase 10
      logModelVL <- K.knitEither
                 $ distCompareChart
                 (FV.ViewConfig 500 500 5)
                 "Model Only (log scale)"
                 (F.rcast @[GT.StateAbbreviation, DT.CitizenC, DT.SimpleAgeC, DT.SexC, DT.Education4C, DT.Race5C])
                 (show . cellKey)
                 (Just ("Race/Ethnicity", show . view DT.race5C))
                 (Just ("2-Way Age 45", show . view DT.simpleAgeC))
                 (log10 . realToFrac . view DT.popCount)
                 ("Actual ACS", (F.rcast @[GT.StateAbbreviation, DT.CitizenC, DT.SimpleAgeC, DT.SexC, DT.Education4C, DT.Race5C, DT.PopCount]) <$> acsCA2SER)
                 ("SER -> CASER (Model Only)", (F.rcast @[GT.StateAbbreviation, DT.CitizenC, DT.SimpleAgeC, DT.SexC, DT.Education4C, DT.Race5C, DT.PopCount]) <$> serToCA2SE2R_MO)
      _ <- K.addHvega Nothing Nothing logModelVL
      logModelAndMatchVL <- K.knitEither
                 $ distCompareChart
                 (FV.ViewConfig 500 500 5)
                 "Model & Match via Constrained Optimization (log scale)"
                 (F.rcast @[GT.StateAbbreviation, DT.CitizenC, DT.SimpleAgeC, DT.SexC, DT.Education4C, DT.Race5C])
                 (show . cellKey)
                 (Just ("Race/Ethnicity", show . view DT.race5C))
                 (Just ("2-Way Age 45", show . view DT.simpleAgeC))
                 (log10 . realToFrac . view DT.popCount)
                 ("Actual ACS", (F.rcast @[GT.StateAbbreviation, DT.CitizenC, DT.SimpleAgeC, DT.SexC, DT.Education4C, DT.Race5C, DT.PopCount]) <$> acsCA2SER)
                 ("SER -> CASER", (F.rcast @[GT.StateAbbreviation, DT.CitizenC, DT.SimpleAgeC, DT.SexC, DT.Education4C, DT.Race5C, DT.PopCount]) <$> serToCA2SE2R)
      _ <- K.addHvega Nothing Nothing logModelAndMatchVL


      pure ()
--    BRK.logFrame $ fmap (F.rcast @[BRDF.StateAbbreviation,  DT.SexC, DT.Education4C, DT.RaceAlone4C, DT.HispC, DT.SimpleAgeC, PUMS.Citizens]) nearestEnrichedAge
--    K.logLE K.Info $ show allEdus
{-
    K.logLE K.Info "Some Examples!"
--    modelResult <- K.ignoreCacheTime res_C
    let exRec :: DT.Sex -> DT.Age4 -> DT.RaceAlone4 -> DT.Hisp -> Double
              -> F.Record [DT.SexC, DT.Age4C, DT.RaceAlone4C, DT.HispC, DT.PWPopPerSqMile]
        exRec s a r h d = s F.&: a F.&: r F.&: h F.&: d F.&: V.RNil
        exR1 = exRec DT.Female DT.A4_18To24 DT.RA4_Asian DT.NonHispanic 25000
        exR2 = exRec DT.Male DT.A4_18To24 DT.RA4_Asian DT.NonHispanic 25000
        exR3 = exRec DT.Male DT.A4_65AndOver DT.RA4_White DT.NonHispanic 2000
        exR4 = exRec DT.Female DT.A4_25To44 DT.RA4_Black DT.NonHispanic 8000
        exR5 = exRec DT.Male DT.A4_45To64 DT.RA4_White DT.Hispanic 10000
        exRs = [exR1, exR2, exR3, exR4, exR5]
        showExs sa y = K.logLE K.Info $ sa <> ": " <> show y <> "=" <> show (SM.applyModelResult modelResult sa y)
    _ <- traverse (showExs "TX") exRs
    _ <- traverse (showExs "CT") exRs
-}
-}
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
  acs_C <- DDP.cachedACSByState'
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
  acs_C <- DDP.cachedACSByState'
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
  acs_C <- DDP.cachedACSByState'
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

distCompareChart :: (Ord k, Show k)
                 => FV.ViewConfig
                 -> Text
                 -> (F.Record rs -> k) -- key for map
                 -> (k -> Text) -- description for tooltip
                 -> Maybe (Text, k -> Text) -- category for color
                 -> Maybe (Text, k -> Text) -- category for shape
                 -> (F.Record rs -> Double)
                 -> (Text, F.FrameRec rs)
                 -> (Text, F.FrameRec rs)
                 -> Either Text GV.VegaLite
distCompareChart vc title key keyText colorM shapeM count (xLabel, xRows) (yLabel, yRows) = do
  let assoc r = (key r, count r)
      toMap = FL.fold (FL.premap assoc FL.map)
      whenMatchedF _ xCount yCount = Right (xCount, yCount)
      whenMatched = MM.zipWithAMatched whenMatchedF
      whenMissingF t k _ = Left $ "Missing key=" <> show k <> " in " <> t <> " rows."
      whenMissingFromX = MM.traverseMissing (whenMissingF xLabel)
      whenMissingFromY = MM.traverseMissing (whenMissingF yLabel)
  mergedMap <- MM.mergeA whenMissingFromY whenMissingFromX whenMatched (toMap xRows) (toMap yRows)
  let rowToData (k, (xCount, yCount)) =
        [ (xLabel, GV.Number xCount)
        , (yLabel, GV.Number yCount)
        , ("Description", GV.Str $ keyText k)
        ]
        <> maybe [] (\(l, f) -> [(l, GV.Str $ f k)]) colorM
        <> maybe [] (\(l, f) -> [(l, GV.Str $ f k)]) shapeM
      toVLDataRows x = GV.dataRow (rowToData x) []
      vlData = GV.dataFromRows []
               $ concatMap toVLDataRows
               $ M.toList mergedMap
      encX = GV.position GV.X [GV.PName xLabel, GV.PmType GV.Quantitative]
      encY = GV.position GV.Y [GV.PName yLabel, GV.PmType GV.Quantitative]
      encColor = maybe id (\(l, _) -> GV.color [GV.MName l, GV.MmType GV.Nominal]) colorM
      encShape = maybe id (\(l, _) -> GV.shape [GV.MName l, GV.MmType GV.Nominal]) shapeM
      encTooltips = GV.tooltips $ [ [GV.TName xLabel, GV.TmType GV.Quantitative]
                                  , [GV.TName yLabel, GV.TmType GV.Quantitative]
                                  , maybe [] (\(l, _) -> [GV.TName l, GV.TmType GV.Nominal]) colorM
                                  , maybe [] (\(l, _) -> [GV.TName l, GV.TmType GV.Nominal]) shapeM
                                  , [GV.TName "Description", GV.TmType GV.Nominal]
                                  ]
      mark = GV.mark GV.Point []
      enc = (GV.encoding . encX . encY . encColor . encShape . encTooltips)
  pure $ FV.configuredVegaLite vc [FV.title title, enc [], mark, vlData]

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

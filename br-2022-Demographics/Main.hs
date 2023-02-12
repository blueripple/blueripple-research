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
import qualified Data.Set as S
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V
import qualified Control.Foldl as FL
import qualified Frames as F
import qualified Frames.Transform as FT

import Control.Lens (view, (^.))

import Path (Dir, Rel)
import qualified Path

--import qualified Frames.Visualization.VegaLite.Data as FVD
import qualified Text.Printf as PF
import qualified Graphics.Vega.VegaLite as GV
import qualified Graphics.Vega.VegaLite.Compat as FV
import qualified Graphics.Vega.VegaLite.Configuration as FV
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
    K.logLE K.Info $ "Building/rebuilding necessary models " <> show cmdLine
    aFromCSR <- SM.runModel
                False cmdLine (SM.ModelConfig () False SM.HCentered True)
                ("Age", DT.age4ToSimple . view DT.age4C)
                ("CSR", F.rcast @[DT.CitizenC, DT.SexC, DT.Race5C], SM.dmrS_CR)
    let ca2srKey :: F.Record DDP.ACSByStateR -> F.Record [DT.CitizenC, DT.SimpleAgeC, DT.SexC, DT.Race5C]
        ca2srKey r = r ^. DT.citizenC F.&: DT.age4ToSimple (r ^. DT.age4C) F.&: r ^. DT.sexC F.&: r ^. DT.race5C F.&: V.RNil
    eFromCA2SR <- SM.runModel
                 False cmdLine (SM.ModelConfig () False SM.HCentered True)
                 ("Edu", DT.education4ToCollegeGrad . view DT.education4C)
                 ("CASR", ca2srKey, SM.dmrC_S_A2R)
    cFromSER <- SM.runModel
                False cmdLine (SM.ModelConfig () False SM.HCentered True)
                ("Cit", view DT.citizenC)
                ("SER", F.rcast @[DT.SexC, DT.Education4C, DT.Race5C], SM.dmrS_ER)
    aFromCSER <- SM.runModel
                 False cmdLine (SM.ModelConfig () False SM.HCentered True)
                 ("Age", DT.age4ToSimple . view DT.age4C)
                 ("CSER", F.rcast  @[DT.CitizenC, DT.SexC, DT.Education4C, DT.Race5C], SM.dmrC_S_ER)

    cFromASR <- SM.runModel
                False cmdLine (SM.ModelConfig () False SM.HCentered True)
                ("Cit", view DT.citizenC)
                ("ASR", F.rcast @[DT.Age4C, DT.SexC, DT.Race5C], SM.dmrS_AR)

    gFromCASR <- SM.runModel
                 False cmdLine (SM.ModelConfig () False SM.HCentered True)
                 ("Grad", DT.education4ToCollegeGrad . view DT.education4C)
                 ("CASR", F.rcast @[DT.CitizenC, DT.Age4C, DT.SexC, DT.Race5C], SM.dmrS_C_AR)

    -- target tables
    let exampleState = "NY"
    acsSample <- {- F.filterFrame ((== exampleState) . view GT.stateAbbreviation) <$> -} K.ignoreCacheTimeM DDP.cachedACSByState'
    let allStates = S.toList $ FL.fold (FL.premap (view GT.stateAbbreviation) FL.set) acsSample
    let csrRec :: (DT.Citizen, DT.Sex, DT.Race5) -> F.Record [DT.CitizenC, DT.SexC, DT.Race5C]
        csrRec (c, s, r) = c F.&: s F.&: r F.&: V.RNil
    let csrDSFld =  DED.desiredSumsFld
                    (Sum . F.rgetField @DT.PopCount)
                    (F.rcast @'[GT.StateAbbreviation])
                    (\r -> csrRec (r ^. DT.citizenC, r ^. DT.sexC, r ^. DT.race5C))
        csrDS = getSum <<$>> FL.fold csrDSFld acsSample
        serRec :: (DT.Sex, DT.Education4, DT.Race5) -> F.Record [DT.SexC, DT.Education4C, DT.Race5C]
        serRec (s, e, r) = s F.&: e F.&: r F.&: V.RNil
        serDSFld =  DED.desiredSumsFld
                    (Sum . F.rgetField @DT.PopCount)
                    (F.rcast @'[GT.StateAbbreviation])
                    (\r -> serRec (r ^. DT.sexC, r ^. DT.education4C, r ^. DT.race5C))
        serDS = getSum <<$>> FL.fold serDSFld acsSample
        a2srRec :: (DT.SimpleAge, DT.Sex, DT.Race5) -> F.Record [DT.SimpleAgeC, DT.SexC, DT.Race5C]
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

    let zeroPopAndDens :: F.Record [DT.PopCount, DT.PWPopPerSqMile] = 0 F.&: 0 F.&: V.RNil
        emptyUnless x y = if x then y else mempty
        tableMatchingDataFunctions = DED.TableMatchingDataFunctions zeroPopAndDens recToCWD cwdToRec cwdN updateCWDCount
        serToCSER_P eu = DED.pipelineStep @[DT.SexC, DT.Education4C, DT.Race5C] @'[DT.CitizenC] @_ @_ @_ @DT.PopCount
                       tableMatchingDataFunctions
                       (DED.minConstrained DED.klPObjF)
                       (DED.enrichFrameFromBinaryModelF @DT.CitizenC @DT.PopCount cFromSER (view GT.stateAbbreviation) DT.Citizen DT.NonCitizen)
                       (const Nothing)
                       (emptyUnless eu
                         $ DED.desiredSumMapToLookup @[DT.CitizenC, DT.SexC, DT.Education4C, DT.Race5C] serDS
                         <> DED.desiredSumMapToLookup @[DT.CitizenC, DT.SexC, DT.Education4C, DT.Race5C] csrDS)

        cserToCASER_P eu = DED.pipelineStep @[DT.CitizenC, DT.SexC, DT.Education4C, DT.Race5C] @'[DT.SimpleAgeC] @_ @_ @_ @DT.PopCount
                         tableMatchingDataFunctions
                         (DED.minConstrained DED.klPObjF)
                         (DED.enrichFrameFromBinaryModelF @DT.SimpleAgeC @DT.PopCount aFromCSER (view GT.stateAbbreviation) DT.EqualOrOver DT.Under)
                         (const Nothing)
                         (emptyUnless eu
                           $ {- DED.desiredSumMapToLookup @[DT.SimpleAgeC, DT.CitizenC, DT.SexC, DT.Education4C, DT.Race5C] serDS
                           <> DED.desiredSumMapToLookup @[DT.SimpleAgeC, DT.CitizenC, DT.SexC, DT.Education4C, DT.Race5C] csrDS
                           <> -} DED.desiredSumMapToLookup @[DT.SimpleAgeC, DT.CitizenC, DT.SexC, DT.Education4C, DT.Race5C] a2srDS
                         )

        asrToCASR_P eu =  DED.pipelineStep @[DT.Age4C, DT.SexC, DT.Race5C] @'[DT.CitizenC] @_ @_ @_ @DT.PopCount
                        tableMatchingDataFunctions
                        (DED.minConstrained DED.klPObjF)
                        (DED.enrichFrameFromBinaryModelF @DT.CitizenC @DT.PopCount cFromASR (view GT.stateAbbreviation) DT.Citizen DT.NonCitizen)
                        (const Nothing)
                        (emptyUnless eu
                          $ DED.desiredSumMapToLookup @[DT.CitizenC, DT.Age4C, DT.SexC, DT.Race5C] asrDS
                          <> DED.desiredSumMapToLookup @[DT.CitizenC, DT.Age4C, DT.SexC, DT.Race5C] csrDS
                        )

        casrToCASGR_P eu =  DED.pipelineStep @[DT.CitizenC, DT.Age4C, DT.SexC, DT.Race5C] @'[DT.CollegeGradC] @_ @_ @_ @DT.PopCount
                          tableMatchingDataFunctions
                          (DED.minConstrained DED.klPObjF)
                          (DED.enrichFrameFromBinaryModelF @DT.CollegeGradC @DT.PopCount gFromCASR (view GT.stateAbbreviation) DT.Grad DT.NonGrad)
                          (const Nothing)
                          (emptyUnless eu
                            $ DED.desiredSumMapToLookup @[DT.CollegeGradC, DT.CitizenC, DT.Age4C, DT.SexC, DT.Race5C] csrDS
                            <> DED.desiredSumMapToLookup @[DT.CollegeGradC, DT.CitizenC, DT.Age4C, DT.SexC, DT.Race5C] asrDS
                            <> DED.desiredSumMapToLookup @[DT.CollegeGradC, DT.CitizenC, DT.Age4C, DT.SexC, DT.Race5C] sgrDS
                          )

        csrToCA2SR_P eu =  DED.pipelineStep @[DT.CitizenC, DT.SexC, DT.Race5C] @'[DT.SimpleAgeC] @_ @_ @_ @DT.PopCount
                         tableMatchingDataFunctions
                         (DED.minConstrained DED.klPObjF)
                         (DED.enrichFrameFromBinaryModelF @DT.SimpleAgeC @DT.PopCount aFromCSR (view GT.stateAbbreviation) DT.EqualOrOver DT.Under)
                         (const Nothing)
                         (emptyUnless eu
                           $ DED.desiredSumMapToLookup @[DT.SimpleAgeC, DT.CitizenC, DT.SexC, DT.Race5C] csrDS
                           <> DED.desiredSumMapToLookup @[DT.SimpleAgeC, DT.CitizenC, DT.SexC, DT.Race5C] a2srDS
                         )

        ca2srToCA2SGR_P eu =  DED.pipelineStep @[DT.CitizenC, DT.SimpleAgeC, DT.SexC, DT.Race5C] @'[DT.CollegeGradC] @_ @_ @_ @DT.PopCount
                            tableMatchingDataFunctions
                            (DED.minConstrained DED.klPObjF)
                            (DED.enrichFrameFromBinaryModelF @DT.CollegeGradC @DT.PopCount eFromCA2SR (view GT.stateAbbreviation) DT.Grad DT.NonGrad)
                            (const Nothing)
                            (emptyUnless eu
                             $ DED.desiredSumMapToLookup @[DT.CollegeGradC, DT.CitizenC, DT.SimpleAgeC, DT.SexC, DT.Race5C] csrDS
                             <> DED.desiredSumMapToLookup @[DT.CollegeGradC, DT.CitizenC, DT.SimpleAgeC, DT.SexC, DT.Race5C] a2srDS
                             <> DED.desiredSumMapToLookup @[DT.CollegeGradC, DT.CitizenC, DT.SimpleAgeC, DT.SexC, DT.Race5C] sgrDS
                            )

    K.logLE K.Info "sample ACS data, ages simplified"
    let acsSampleKey :: F.Record DDP.ACSByStateR -> F.Record [DT.CitizenC, DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.Race5C]
        acsSampleKey r = r ^. DT.citizenC
                         F.&: DT.age4ToSimple (r ^. DT.age4C)
                         F.&: r ^. DT.sexC
                         F.&: DT.education4ToCollegeGrad (r ^. DT.education4C)
                         F.&: r ^. DT.race5C
                         F.&: V.RNil
        acsSampleVecF t = FL.fold (DED.vecFld (realToFrac . getSum) (Sum . view DT.popCount) acsSampleKey) t
        acsSampleVec = acsSampleVecF $ F.filterFrame ((== exampleState) . view GT.stateAbbreviation) acsSample
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

--                                      $ table (DT.age4ToSimple . view DT.age4C) (view DT.education4C) exampleState acsSample)

    let computeKL f d sa = DED.klDiv acsV dV where
          acsV = acsSampleVecF $ F.filterFrame ((== sa) . view GT.stateAbbreviation) acsSample
          dV = f $ fmap F.rcast $ F.filterFrame ((== sa) . view GT.stateAbbreviation) d
    let acsSampleNoAgeCit = F.toFrame $ (\(r, v) ->  FT.recordSingleton @DT.PopCount (VU.sum v) F.<+> r) <$> DDP.acsByStateCitizenMN acsSample
    K.logLE K.Info $ "ACS Input to SER -> CASER"
    K.logLE K.Info $ "\n" <> toText (C.ascii (fmap toString $ mapColonnade)
                                      $ FL.fold (fmap DED.totaledTable
                                                  $ DED.rowMajorMapFldInt
                                                  (view DT.popCount)
                                                  (\r -> (r ^. DT.sexC, r ^. DT.race5C))
                                                  (\r -> r ^. DT.education4C)
                                                 )
                                      $ fmap (F.rcast @[DT.SexC, DT.Education4C, DT.Race5C, DT.PopCount])
                                      $ F.filterFrame ((== exampleState) . (^. GT.stateAbbreviation)) acsSampleNoAgeCit
                                    )
    serToCSER_MO <-  DED.mapPE $ serToCSER_P False acsSampleNoAgeCit
    K.logLE K.Info $ "SER -> CSER (Model Only)"
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
    serToCSER <-   DED.mapPE $ serToCSER_P True acsSampleNoAgeCit
    K.logLE K.Info $ "SER -> CSER"
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
    serToCSER' <-   DED.mapPE $ (serToCSER_P True >=> cserToCASER_P False) acsSampleNoAgeCit
    K.logLE K.Info $ "SER -> CASER (2nd step, model only)"
    K.logLE K.Info $ "\n" <> toText (C.ascii (fmap toString $ mapColonnade)
                                      $ FL.fold (fmap DED.totaledTable
                                                  $ DED.rowMajorMapFldInt
                                                  (view DT.popCount)
                                                  (\r -> ( r ^. DT.simpleAgeC, r ^. DT.sexC, r ^. DT.race5C))
                                                  (\r -> (r ^. DT.citizenC, r ^. DT.education4C))
                                                )
                                      $ fmap (F.rcast @[DT.SimpleAgeC, DT.CitizenC, DT.SexC, DT.Education4C, DT.Race5C, DT.PopCount])
                                      $ F.filterFrame ((== exampleState) . (^. GT.stateAbbreviation)) serToCSER'
                                    )
    K.logLE K.Info "SER -> CSER -> CASER pipeline."
    serToCASER <-  DED.mapPE $ (serToCSER_P True >=> cserToCASER_P True) acsSampleNoAgeCit
    let serToCASERKey :: F.Record [GT.StateAbbreviation, DT.CitizenC, DT.SimpleAgeC, DT.SexC, DT.Education4C, DT.Race5C, DT.PopCount, DT.PWPopPerSqMile]
                      -> F.Record [DT.CitizenC, DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.Race5C]
        serToCASERKey r = r ^. DT.citizenC
                         F.&: r ^. DT.simpleAgeC
                         F.&: r ^. DT.sexC
                         F.&: DT.education4ToCollegeGrad (r ^. DT.education4C)
                         F.&: r ^. DT.race5C
                         F.&: V.RNil
        serToCASERVecF t = FL.fold (DED.vecFld (realToFrac . getSum) (Sum . view DT.popCount) serToCASERKey) t
        serToCASERKL = computeKL serToCASERVecF serToCASER exampleState
        serToCASER_KLs = computeKL serToCASERVecF serToCASER <$> allStates
--    K.logLE K.Info $ "KL divergences (SER -> CASER, model only)" <> show (zip allStates serToCASER_MO_KLs)
    K.logLE K.Info $ "KL divergence =" <> show serToCASERKL
    K.logLE K.Info $ "\n" <> toText (C.ascii (fmap toString $ mapColonnade)
                                      $ table (view DT.simpleAgeC) (view DT.education4C) exampleState serToCASER)
    serToCASER_MO <-  DED.mapPE $ (serToCSER_P False >=> cserToCASER_P False) acsSampleNoAgeCit
    let serToCASER_MO_KLs = computeKL serToCASERVecF serToCASER_MO <$> allStates
    K.logLE K.Info $ "Model only\n" <> toText (C.ascii (fmap toString $ mapColonnade)
                                      $ table (view DT.simpleAgeC) (DT.education4ToCollegeGrad . view DT.education4C) exampleState serToCASER_MO)


    let acsSampleNoEduCit = F.toFrame $ (\(r, v) ->  FT.recordSingleton @DT.PopCount (VU.sum v) F.<+> r)
                            <$> DDP.acsByStateMN (F.rcast @[DT.Age4C, DT.SexC, DT.Race5C]) (view DT.citizenC) acsSample

    K.logLE K.Info "Running ASR -> CASR -> CASGR pipeline."
    asrToCASGR_MO <-  DED.mapPE $ (asrToCASR_P False >=> casrToCASGR_P False) acsSampleNoEduCit
    asrToCASGR <-  DED.mapPE $ (asrToCASR_P True >=> casrToCASGR_P True) acsSampleNoEduCit
    let asrToCASGRKey :: F.Record [GT.StateAbbreviation, DT.CitizenC, DT.Age4C, DT.SexC, DT.CollegeGradC, DT.Race5C, DT.PopCount, DT.PWPopPerSqMile]
                      -> F.Record [DT.CitizenC, DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.Race5C]
        asrToCASGRKey r = r ^. DT.citizenC
                         F.&: DT.age4ToSimple (r ^. DT.age4C)
                         F.&: r ^. DT.sexC
                         F.&: r ^. DT.collegeGradC
                         F.&: r ^. DT.race5C
                         F.&: V.RNil
        asrToCASGRVecF t = FL.fold (DED.vecFld (realToFrac . getSum) (Sum . view DT.popCount) asrToCASGRKey) t
        asrToCASGRVec = asrToCASGRVecF $ fmap F.rcast $ F.filterFrame ((== exampleState) . view GT.stateAbbreviation) asrToCASGR
        asrToCASGRKL = DED.klDiv acsSampleVec asrToCASGRVec
        asrToCASGR_MO_KLs = computeKL asrToCASGRVecF asrToCASGR_MO <$> allStates
        asrToCASGR_KLs = computeKL asrToCASGRVecF asrToCASGR <$> allStates
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

    let acsSampleNoEduAge = F.toFrame $ (\(r, v) ->  FT.recordSingleton @DT.PopCount (VU.sum v) F.<+> r)
                            <$> DDP.acsByStateMN (F.rcast @[DT.CitizenC, DT.SexC, DT.Race5C]) (view DT.age4C) acsSample
    K.logLE K.Info "Running CSR -> CA2SR -> CA2SGR pipeline."
    csrToCASER_MO <-  DED.mapPE $ (csrToCA2SR_P False >=> ca2srToCA2SGR_P False) acsSampleNoEduAge
    csrToCASER <-  DED.mapPE $ (csrToCA2SR_P True >=> ca2srToCA2SGR_P True) acsSampleNoEduAge
    let csrToCASERKey :: F.Record [GT.StateAbbreviation, DT.CitizenC, DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.Race5C, DT.PopCount, DT.PWPopPerSqMile]
                      -> F.Record [DT.CitizenC, DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.Race5C]
        csrToCASERKey r = r ^. DT.citizenC
                         F.&: r ^. DT.simpleAgeC
                         F.&: r ^. DT.sexC
                         F.&: r ^. DT.collegeGradC
                         F.&: r ^. DT.race5C
                         F.&: V.RNil
        csrToCASERVecF t = FL.fold (DED.vecFld (realToFrac . getSum) (Sum . view DT.popCount) csrToCASERKey) t
        csrToCASERVec = csrToCASERVecF $ fmap F.rcast $ F.filterFrame ((== exampleState) . view GT.stateAbbreviation) csrToCASER
        csrToCASERKL = DED.klDiv acsSampleVec csrToCASERVec
        csrToCASER_MO_KLs = computeKL csrToCASERVecF csrToCASER_MO <$> allStates
        csrToCASER_KLs = computeKL csrToCASERVecF csrToCASER <$> allStates
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


    let allKLs = L.zip7 allStates serToCASER_MO_KLs serToCASER_KLs asrToCASGR_MO_KLs asrToCASGR_KLs csrToCASER_MO_KLs csrToCASER_KLs
    K.logLE K.Info "Divergence Table:"
    K.logLE K.Info $ "\n" <> toText (C.ascii (fmap toString $ klColonnade) allKLs)

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

    pure ()
  case resE of
    Right namedDocs →
      K.writeAllPandocResultsWithInfoAsHtml "" namedDocs
    Left err → putTextLn $ "Pandoc Error: " <> Pandoc.renderError err

klColonnade :: C.Colonnade C.Headed (Text, Double, Double, Double, Double, Double, Double) Text
klColonnade =
  let sa (x, _, _, _, _, _, _) = x
      serMO (_, x, _, _, _, _, _) = x
      ser (_, _, x, _, _, _, _) = x
      asrMO (_, _, _, x, _, _, _) = x
      asr (_, _, _, _, x, _, _) = x
      csrMO (_, _, _, _, _, x, _) = x
      csr (_, _, _, _, _, _, x) = x
      fmtX :: Double -> Text
      fmtX x = toText @String $ PF.printf "%2.2e" x
  in C.headed "State" sa
     <> C.headed "Model Only" (fmtX . serMO)
     <> C.headed "Model + Matching" (fmtX . ser)
     <> C.headed "Error Improvement" (toText @String . PF.printf "%1.1g" . \x -> sqrt (serMO x / ser x))
     <> C.headed "Approx Error (%)" (toText @String . PF.printf "%2.0g" . (100 *) . sqrt . (2 *) . ser)
--     <> C.headed "AxGXRE -> CxAxGxExRE (Model Only)" (show . asrMO)
--     <> C.headed "AxGxRE -> CxAxGxExRE" (show . asr)
--     <> C.headed "CxGxRE -> CxGxExRE (Model Only)" (show . csrMO)
--     <> C.headed "CxGxRE -> CxAxGxExRE" (show . csr)



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

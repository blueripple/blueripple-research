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

import qualified Data.Map as M
import qualified Data.Set as S
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vinyl as V
import qualified Control.Foldl as FL
import qualified Frames as F
import qualified Frames.Transform as FT

import Control.Lens (view, (^.))

import Path (Dir, Rel)
import qualified Path

--import qualified Frames.Visualization.VegaLite.Data as FVD
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

recToCWD :: F.Record [DT.PopCount, DT.PopPerSqMile] -> CountWithDensity
recToCWD r = CountWithDensity (r ^. DT.popCount) (r ^. DT.popPerSqMile)

cwdToRec :: CountWithDensity -> F.Record [DT.PopCount, DT.PopPerSqMile]
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
    citModelResult <- runCitizenModel False cmdLine (SM.ModelConfig () False SM.HCentered False) SM.designMatrixRowCitizen
    ageModelResult <- runAgeModel False cmdLine (SM.ModelConfig () False SM.HCentered False) SM.designMatrixRowAge
    -- create test frame
    let onlyYear y r = F.rgetField @BRDF.Year r == y
        onlyState sa r = F.rgetField @GT.StateAbbreviation r == sa
        only sa y r = onlyState sa r && onlyYear y r
        onlyGender g r = F.rgetField @DT.SexC r == g
        onlyRE race eth r = F.rgetField @DT.RaceAlone4C r == race && F.rgetField @DT.HispC r == eth
        exampleFilter r = onlyState "AZ" r
    acsSample <- K.ignoreCacheTimeM DDP.cachedACSByState'
    let acsSampleNoAgeCit = F.toFrame $ (\(r, v) ->  FT.recordSingleton @DT.PopCount (VU.sum v) F.<+> r) <$> DDP.acsByStateCitizenMN acsSample
        acsSampleNoAge = F.toFrame $ (\(r, v) ->  FT.recordSingleton @DT.PopCount (VU.sum v) F.<+> r) <$> DDP.acsByStateAgeMN acsSample
    K.logLE K.Info "sample ACS data"
    let allSimpleAge = Keyed.elements @DT.SimpleAge
        allSex = Keyed.elements @DT.Sex
        allCitizen = Keyed.elements @DT.Citizen
        allAge = Keyed.elements @DT.SimpleAge
        allEdu = Keyed.elements @DT.Education4
        allRace = Keyed.elements @DT.Race5
        all2 as bs = S.fromList [(a, b) | a <- S.toList as, b <- S.toList bs]
        all3 as bs cs = S.fromList [(a, b, c) | a <- S.toList as, b <- S.toList bs, c <- S.toList cs]
        allCE = all2 allCitizen allEdu
        allSR = all2 allCitizen allRace
--        allSexRace = all2 allSex allRace
--        allEduSex = all2 allEdus allSex --S.fromList $ [(e, s) | e <- S.toList allEdus, s <- [DT.Female, DT.Male]]
        allASR = all3 allAge allSex allRace
        allCSR = all3 allCitizen allSex allRace
--        allSimpleAgesR = S.map (FT.recordSingleton @DT.SimpleAgeC) allSimpleAges
--        allAgesR = S.map (FT.recordSingleton @DT.Age4C) allAges
--        allEdusR = S.map (FT.recordSingleton @DT.Education4C) allEdus
    K.logLE K.Info "sample ACS data, aggregated by ages"
    let aggAgeTable =  FL.fold (fmap DED.totaledTable
                                 $ DED.rowMajorMapFldInt
                                 (F.rgetField @DT.PopCount)
                                 (\r -> (F.rgetField @DT.CitizenC r, F.rgetField @DT.SexC r, F.rgetField @DT.Race5C r))
                                 (F.rgetField @DT.Education4C)
                                 allCSR
                                 allEdu
                               )
          $ fmap (F.rcast @[DT.CitizenC, DT.SexC, DT.Education4C, DT.Race5C, DT.PopCount])
          $ F.filterFrame exampleFilter acsSampleNoAge
    K.logLE K.Info $ "\n" <> toText (C.ascii (fmap toString $ mapColonnade allEdu) aggAgeTable)
    K.logLE K.Info "sample ACS data, aggregated by ages, and citizenship, then split by citizen model"
    enrichedCit <- KS.streamlyToKnit
                   $ DED.enrichFrameFromBinaryModel @DT.CitizenC @DT.PopCount
                   citModelResult
                   (F.rgetField @GT.StateAbbreviation)
                   DT.Citizen
                   DT.NonCitizen
                   acsSampleNoAgeCit
    let addCitTable =  FL.fold (fmap DED.totaledTable
                                 $ DED.rowMajorMapFldInt
                                 (F.rgetField @DT.PopCount)
                                 (\r -> (F.rgetField @DT.CitizenC r, F.rgetField @DT.SexC r, F.rgetField @DT.Race5C r))
                                 (F.rgetField @DT.Education4C)
                                 allCSR
                                 allEdu
                               )
          $ fmap (F.rcast @[DT.CitizenC, DT.SexC, DT.Education4C, DT.Race5C, DT.PopCount])
          $ F.filterFrame exampleFilter enrichedCit
    K.logLE K.Info $ "\n" <> toText (C.ascii (fmap toString $ mapColonnade allEdu) addCitTable)
    let ncFrameIFldCSR_E = DED.nearestCountsFrameIFld @[DT.CitizenC, DT.SexC, DT.Race5C] @'[DT.Education4C]
                           DED.nearestCountsKL_RC
                           recToCWD
                           cwdToRec
                           cwdN
                           updateCWDCount
        csrRec :: (DT.Citizen, DT.Sex, DT.Race5) -> F.Record [DT.CitizenC, DT.SexC, DT.Race5C]
        csrRec (c, s, r) = c F.&: s F.&: r F.&: V.RNil
        eRec :: DT.Education4 -> F.Record '[DT.Education4C]
        eRec e = e F.&: V.RNil
        addCitDSFld =  DED.desiredRowSumsFld
                       (F.rgetField @DT.PopCount)
                       (F.rcast @'[GT.StateAbbreviation])
                       (\r -> csrRec (r ^. DT.citizenC, r ^. DT.sexC, r ^. DT.race5C))
                       (S.map csrRec allCSR)
        desiredCSRRowSumMap = FL.fold addCitDSFld acsSample
        desiredCSRRowSumLookup k = maybeToRight (show k <> " not found in desired CSR sum row map") $ M.lookup k desiredCSRRowSumMap
        nMatchCSRFld = DED.nearestCountsFrameFld @'[DT.CitizenC, DT.SexC, DT.Race5C] @'[DT.Education4C]
                       ncFrameIFldCSR_E desiredCSRRowSumLookup (S.map eRec allEdu)
    nMatchCSR <- K.knitEither $ FL.foldM nMatchCSRFld enrichedCit
--    BRK.logFrame $ F.filterFrame exampleFilter enrichedCit
--    BRK.logFrame $ F.filterFrame exampleFilter nMatchCSR
    let nMatchCSRTable = FL.fold (fmap DED.totaledTable
                                  $ DED.rowMajorMapFldInt
                                   (F.rgetField @DT.PopCount)
                                   (\r -> (F.rgetField @DT.CitizenC r, F.rgetField @DT.SexC r, F.rgetField @DT.Race5C r))
                                   (F.rgetField @DT.Education4C)
                                   allCSR
                                   allEdu
                                 )
          $ fmap (F.rcast @[DT.CitizenC, DT.SexC, DT.Education4C, DT.Race5C, DT.PopCount])
          $ F.filterFrame exampleFilter nMatchCSR
    K.logLE K.Info "sample ACS data, aggregated by ages, and citizenship, then split by citizen model and row matched to actual CSR data"
    K.logLE K.Info $ "\n" <> toText (C.ascii (fmap toString $ mapColonnade allEdu) nMatchCSRTable)

    enrichedAge <- KS.streamlyToKnit
                   $ DED.enrichFrameFromBinaryModel @DT.SimpleAgeC @DT.PopCount
                   ageModelResult
                   (F.rgetField @GT.StateAbbreviation)
                   DT.EqualOrOver
                   DT.Under
                   nMatchCSR
    let enrichedAgeTable = FL.fold (fmap DED.totaledTable
                                    $ DED.rowMajorMapFldInt
                                    (F.rgetField @DT.PopCount)
                                    (\r -> (F.rgetField @DT.SimpleAgeC r, F.rgetField @DT.SexC r, F.rgetField @DT.Race5C r))
                                    (\r -> (F.rgetField @DT.CitizenC r, F.rgetField @DT.Education4C r))
                                    allASR
                                    allCE
                                   )
          $ fmap (F.rcast @[DT.CitizenC, DT.SimpleAgeC, DT.SexC, DT.Education4C, DT.Race5C, DT.PopCount])
          $ F.filterFrame exampleFilter enrichedAge
    K.logLE K.Info "previous row-matched, then split by age model"
    K.logLE K.Info $ "\n" <> toText (C.ascii (fmap toString $ mapColonnade allCE) enrichedAgeTable)
--    BRK.logFrame $ fmap fixRows2 enrichedAge
    let ncFrameIFldASR_CE = DED.nearestCountsFrameIFld @[DT.SimpleAgeC, DT.SexC, DT.Race5C] @'[DT.CitizenC, DT.Education4C]
                            DED.nearestCountsKL_RC
                            recToCWD
                            cwdToRec
                            cwdN
                            updateCWDCount
    let asrRec :: (DT.SimpleAge, DT.Sex, DT.Race5) -> F.Record [DT.SimpleAgeC, DT.SexC, DT.Race5C]
        asrRec (a, s, r) = a F.&: s F.&: r F.&: V.RNil
        ceRec :: (DT.Citizen, DT.Education4) -> F.Record [DT.CitizenC, DT.Education4C]
        ceRec (c, e) = c F.&: e F.&: V.RNil
        addAgeDSFld = DED.desiredRowSumsFld
                      (F.rgetField @DT.PopCount)
                      (F.rcast @'[GT.StateAbbreviation])
                      (\r -> asrRec (DT.age4ToSimple $ r ^. DT.age4C, r ^. DT.sexC, r ^. DT.race5C))
                      (S.map asrRec allASR)
        desiredASRRowSumMap = FL.fold addAgeDSFld acsSample --NoEdu
        desiredASRRowSumLookup k = maybeToRight (show k <> " not found in desired sum row map") $ M.lookup k desiredASRRowSumMap
--        allEduSexR :: [F.Record '[DT.Education4C, DT.SexC]]= [e F.&: s F.&: V.RNil | e <- S.toList allEdus, s <- [DT.Female, DT.Male]]
        nMatchASRFldM = DED.nearestCountsFrameFld @[DT.SimpleAgeC, DT.SexC, DT.Race5C] @[DT.CitizenC, DT.Education4C]
                        ncFrameIFldASR_CE desiredASRRowSumLookup (S.map ceRec allCE)
    nMatchASR <- K.knitEither $ FL.foldM nMatchASRFldM enrichedAge
    let nMatchASRTable = FL.fold (fmap DED.totaledTable
                                $ DED.rowMajorMapFldInt
                                 (F.rgetField @DT.PopCount)
                                 (\r -> (F.rgetField @DT.SimpleAgeC r, F.rgetField @DT.SexC r, F.rgetField @DT.Race5C r))
                                 (\r -> (F.rgetField @DT.CitizenC r, F.rgetField @DT.Education4C r))
                                 allASR
                                 allCE
                               )
          $ fmap (F.rcast @[DT.CitizenC, DT.SimpleAgeC, DT.SexC, DT.Education4C, DT.Race5C, DT.PopCount])
          $ F.filterFrame exampleFilter nMatchASR
    K.logLE K.Info "previous, row-matched to ASR table."
    K.logLE K.Info $ "\n" <> toText (C.ascii (fmap toString $ mapColonnade allCE) nMatchASRTable)
    let acsTable = FL.fold (fmap DED.totaledTable
                             $ DED.rowMajorMapFldInt
                             (F.rgetField @DT.PopCount)
                             (\r -> (DT.age4ToSimple $ F.rgetField @DT.Age4C r, F.rgetField @DT.SexC r, F.rgetField @DT.Race5C r))
                             (\r -> (F.rgetField @DT.CitizenC r, F.rgetField @DT.Education4C r))
                             allASR
                             allCE
                           )
          $ fmap (F.rcast @[DT.CitizenC, DT.Age4C, DT.SexC, DT.Education4C, DT.Race5C, DT.PopCount])
          $ F.filterFrame exampleFilter acsSample
    K.logLE K.Info "Original ACS data."
    K.logLE K.Info $ "\n" <> toText (C.ascii (fmap toString $ mapColonnade allCE) acsTable)

--    BRK.logFrame $ fmap (F.rcast @[BRDF.StateAbbreviation,  DT.SexC, DT.Education4C, DT.RaceAlone4C, DT.HispC, DT.SimpleAgeC, PUMS.Citizens]) nearestEnrichedAge
--    K.logLE K.Info $ show allEdus
{-
    K.logLE K.Info "Some Examples!"
--    modelResult <- K.ignoreCacheTime res_C
    let exRec :: DT.Sex -> DT.Age4 -> DT.RaceAlone4 -> DT.Hisp -> Double
              -> F.Record [DT.SexC, DT.Age4C, DT.RaceAlone4C, DT.HispC, DT.PopPerSqMile]
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


mapColonnade :: (Show a, Show b, Ord b) => Set b -> C.Colonnade C.Headed (a, Map b Int) Text
mapColonnade bs = C.headed "" (show . fst)
                  <> mconcat (fmap (\b -> C.headed (show b) (fixMaybe . M.lookup b . snd)) (S.toAscList bs))
                  <>  C.headed "Total" (fixMaybe . sumM) where
  fixMaybe = maybe "0" show
  sumM x = fmap (FL.fold FL.sum) <$> traverse (\b -> M.lookup b (snd x)) $ S.toList bs


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

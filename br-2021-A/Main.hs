{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE OverloadedLabels #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-# OPTIONS_GHC -O0 #-}


module Main where

import qualified BlueRipple.Configuration as BR

import qualified BlueRipple.Data.ACS_PUMS as PUMS
import qualified BlueRipple.Data.CPSVoterPUMS as CPS
--import qualified BlueRipple.Data.CCES as CCES
import qualified BlueRipple.Data.CountFolds as BRCF
import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.ElectionTypes as ET
import qualified BlueRipple.Data.Keyed as BRK
import qualified BlueRipple.Data.Loaders as BR
import qualified BlueRipple.Model.House.ElectionResult as BRE
import qualified BlueRipple.Model.StanMRP as MRP
import qualified BlueRipple.Utilities.KnitUtils as BR
import qualified BlueRipple.Utilities.Heidi as BR

import qualified Control.Foldl as FL
import qualified Data.IntMap as IM
import qualified Data.List as List
import qualified Data.Map.Strict as M
import qualified Data.Map.Merge.Strict as M
import qualified Data.MapRow as MapRow

import qualified Data.Monoid as Monoid
import qualified Data.Set as S
import Data.String.Here (here, i)
import qualified Data.Text as T
import qualified Text.Read as T
import qualified Text.Printf as Printf
import qualified Data.Text.IO as T
import qualified Data.Time.Calendar            as Time
import qualified Data.Time.Clock               as Time
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V
import qualified Data.Vector as Vector
import qualified Flat
import qualified Frames as F
import qualified Frames.InCore as FI
import qualified Frames.MapReduce as FMR
import qualified Frames.Folds as FF
import qualified Frames.Heidi as FH
import qualified Frames.SimpleJoins as FJ
import qualified Frames.Transform  as FT
import qualified Graphics.Vega.VegaLite as GV
import qualified Graphics.Vega.VegaLite.Compat as FV

import qualified Heidi
import Lens.Micro.Platform ((^?))

import qualified Graphics.Vega.VegaLite.Configuration as FV
import qualified Graphics.Vega.VegaLite.Heidi as HV

import qualified Knit.Report as K
import qualified Knit.Effect.AtomicCache as KC
import qualified Numeric
import qualified Path
import Path (Rel, Abs, Dir, File)
import qualified Polysemy

import qualified Stan.ModelConfig as SC
import qualified Stan.ModelBuilder as SB
import qualified Stan.ModelBuilder.BuildingBlocks as SB
import qualified Stan.ModelBuilder.SumToZero as SB
import qualified Stan.Parameters as SP
import qualified CmdStan as CS


yamlAuthor :: T.Text
yamlAuthor =
  [here|
- name: Adam Conner-Sax
- name: Frank David
|]

templateVars :: M.Map String String
templateVars =
  M.fromList
    [ ("lang", "English"),
      ("site-title", "Blue Ripple Politics"),
      ("home-url", "https://www.blueripplepolitics.org")
      --  , ("author"   , T.unpack yamlAuthor)
    ]

{-
cpsStateRaceIntroRST :: Text
cpsStateRaceIntroRST = [here|
.. include:: br-2021-A/RST/cpsStateRace/intro.RST
|]
-}

pandocTemplate = K.FullySpecifiedTemplatePath "pandoc-templates/blueripple_basic.html"

main :: IO ()
main = do
  pandocWriterConfig <-
    K.mkPandocWriterConfig
      pandocTemplate
      templateVars
      K.mindocOptionsF
  let cacheDir = ".flat-kh-cache"
      knitConfig :: K.KnitConfig BR.SerializerC BR.CacheData Text =
        (K.defaultKnitConfig $ Just cacheDir)
          { K.outerLogPrefix = Just "HouseModel"
          , K.logIf = K.logDiagnostic
          , K.pandocWriterConfig = pandocWriterConfig
          , K.serializeDict = BR.flatSerializeDict
          , K.persistCache = KC.persistStrictByteString (\t -> toString (cacheDir <> "/" <> t))
          }
--  let pureMTseed = PureMT.pureMT 1
  --
--  resE <- K.knitHtmls knitConfig testHouseModel
  resE <- K.knitHtmls knitConfig cpsVAnalysis

  case resE of
    Right namedDocs ->
      K.writeAllPandocResultsWithInfoAsHtml "" namedDocs
    Left err -> putStrLn $ "Pandoc Error: " ++ show err

type PctTurnout = "PctTurnout" F.:-> Double
type DShare = "DShare" F.:-> Double

--districtKey :: F.Record BRE.CCESByCD -> Text
districtKey r = F.rgetField @BR.StateAbbreviation r <> "-" <> show (F.rgetField @BR.CongressionalDistrict r)

districtPredictors r = Vector.fromList $ [Numeric.log (F.rgetField @DT.PopPerSqMile r)
                                         , realToFrac $F.rgetField @BRE.Incumbency r]

densityPredictor r = Vector.fromList $ [Numeric.log (F.rgetField @DT.PopPerSqMile r)]

--acsWhite :: F.Record BRE.PUMSByCDR -> Bool
ra4White = (== DT.RA4_White) . F.rgetField @DT.RaceAlone4C

wnh r = (F.rgetField @DT.RaceAlone4C r == DT.RA4_White) && (F.rgetField @DT.HispC r == DT.NonHispanic)

wnhCCES r = (F.rgetField @DT.Race5C r == DT.R5_WhiteNonLatinx) && (F.rgetField @DT.HispC r == DT.NonHispanic)

ra4WhiteNonGrad r = ra4White r && (F.rgetField @DT.CollegeGradC r == DT.NonGrad)
wnhNonGrad r = wnh r && (F.rgetField @DT.CollegeGradC r == DT.NonGrad)
wnhNonGradCCES r = wnhCCES r && (F.rgetField @DT.CollegeGradC r == DT.NonGrad)

ccesGroupBuilder :: SB.StanGroupBuilderM (F.Record BRE.CCESByCDR) ()
ccesGroupBuilder = do
  SB.addGroup "CD" $ SB.makeIndexByCounting show districtKey
  SB.addGroup "State" $ SB.makeIndexByCounting show $ F.rgetField @BR.StateAbbreviation
  SB.addGroup "Race" $ SB.makeIndexFromEnum (F.rgetField @DT.Race5C)
  SB.addGroup "Sex" $ SB.makeIndexFromEnum (F.rgetField @DT.SexC)
  SB.addGroup "Education" $ SB.makeIndexFromEnum (F.rgetField @DT.CollegeGradC)
  SB.addGroup "Age" $ SB.makeIndexFromEnum (F.rgetField @DT.SimpleAgeC)

race5FromPUMS :: F.Record BRE.PUMSByCDR -> DT.Race5
race5FromPUMS r =
  let race4A = F.rgetField @DT.RaceAlone4C r
      hisp = F.rgetField @DT.HispC r
  in DT.race5FromRaceAlone4AndHisp True race4A hisp

race5FromCPS :: F.Record BRE.CPSVByCDR -> DT.Race5
race5FromCPS r =
  let race4A = F.rgetField @DT.RaceAlone4C r
      hisp = F.rgetField @DT.HispC r
  in DT.race5FromRaceAlone4AndHisp True race4A hisp

catsPSGroupRowMap :: SB.GroupRowMap (F.Record BRE.AllCatR)
catsPSGroupRowMap = SB.addRowMap @Text "State" (const "NY-21")
                    $ SB.addRowMap "Sex" (F.rgetField @DT.SexC)
                    $ SB.addRowMap "Education" (F.rgetField @DT.CollegeGradC) SB.emptyGroupRowMap

indexStanResults :: (Show k, Ord k) => IM.IntMap k -> Vector.Vector a -> Either Text (Map k a)
indexStanResults im v = do
  when (IM.size im /= Vector.length v)
    $ Left $
    "Mismatched sizes in indexStanResults. Result vector has " <> show (Vector.length v) <> " result and IntMap = " <> show im
  return $ M.fromList $ zip (IM.elems im) (Vector.toList v)

expandInterval :: Text -> (T.Text, [Double]) -> Either T.Text (MapRow.MapRow GV.DataValue)
expandInterval label (t, vals)  = do
        case vals of
          [lo, mid, hi] -> Right $ M.fromList [(label, GV.Str t), ("lo", GV.Number $ (lo - mid)), ("mid", GV.Number $ mid), ("hi", GV.Number $ (hi - mid))]
          _ -> Left "Wrong length list in what should be a (lo, mid, hi) interval"

subSampleCCES :: K.KnitEffects r => Word32 -> Int -> BRE.CCESAndPUMS -> K.Sem r BRE.CCESAndPUMS
subSampleCCES seed samples (BRE.CCESAndPUMS cces cps pums dist) = do
  subSampledCCES <- K.liftKnit @IO $ BR.sampleFrame seed samples cces
  return $ BRE.CCESAndPUMS subSampledCCES cps pums dist

countInCategory :: (Eq a, Num b) => (F.Record rs -> b) -> (F.Record rs -> a) -> [a] -> FL.Fold (F.Record rs) [(a, b)]
countInCategory count key as =
  let countF a = fmap (a,) $ FL.prefilter ((== a) . key) $ FL.premap count FL.sum
  in traverse countF as

postDir = [Path.reldir|br-2021-A/posts|]
postInputs p = postDir BR.</> p BR.</> [Path.reldir|inputs|]
postLocalDraft p = postDir BR.</> p BR.</> [Path.reldir|draft|]
postOnline p =  [Path.reldir|research/Turnout|] BR.</> p

postPaths :: (K.KnitEffects r, MonadIO (K.Sem r))
          => Text
          -> K.Sem r (BR.PostPaths BR.Abs)
postPaths t = do
  postSpecificP <- K.knitEither $ first show $ Path.parseRelDir $ toString t
  BR.postPaths
    BR.defaultLocalRoot
    (postInputs postSpecificP)
    (postLocalDraft postSpecificP)
    (postOnline postSpecificP)


cpsVAnalysis :: forall r. (K.KnitMany r, BR.CacheEffects r) => K.Sem r ()
cpsVAnalysis = do
  K.logLE K.Info "Data prep..."
  data_C <- BRE.prepCCESAndPums False

  let cpsSS1PostInfo = BR.PostInfo BR.LocalDraft (BR.PubTimes (BR.Published $ Time.fromGregorian 2021 6 3) Nothing)
  cpsSS1Paths <- postPaths "StateSpecific1"
  BR.brNewPost cpsSS1Paths cpsSS1PostInfo "State-Specific VOC/WHNV Turnout Gaps"
    $ cpsStateRace False cpsSS1Paths cpsSS1PostInfo $ K.liftActionWithCacheTime data_C

{-
  let cpsSS2PostInfo = BR.PostInfo BR.LocalDraft (BR.PubTimes BR.Unpublished Nothing)
  cpsSS2Paths <- postPaths "StateSpecific2"
  BR.brNewPost cpsSS2Paths cpsSS2PostInfo "State-Specific Turnout Gaps Over Time"
    $ gapsOverTime False cpsSS2Paths cpsSS2PostInfo $ K.liftActionWithCacheTime data_C

  let cpsSS3PostInfo = BR.PostInfo BR.LocalDraft (BR.PubTimes BR.Unpublished Nothing)
  cpsSS3Paths <- postPaths "StateSpecific3"
  BR.brNewPost cpsSS3Paths cpsSS3PostInfo "District-Specific Turnout Gaps Over Time"
    $ districtSpecificTurnout False cpsSS3Paths cpsSS3PostInfo $ K.liftActionWithCacheTime data_C
-}

{-
  let cpsMTPostInfo = PostInfo BRC.LocalDraft (BRC.PubTimes BRC.Unpublished Nothing)
      cpsMTPaths = postPaths "ModelTest"
  BR.brNewPost cpsMTPaths cpsMTPostInfo "ModelTest"
    $ cpsModelTest False cpsMTPaths cpsMTPostInfo $ K.liftActionWithCacheTime data_C
-}

type VOC = "T_VOC" F.:-> Double
type WHNV = "T_WHNV" F.:-> Double
data VoterType = VOC | WNHV deriving (Eq, Ord, Show, Generic)
instance Flat.Flat VoterType
type instance FI.VectorFor VoterType = Vector.Vector

type VoterTypeC = "VoterType" F.:-> VoterType

type VEP = "VEP" F.:-> Double
type Voted = "Voted" F.:-> Double

type RawTurnout = [BR.Year, BR.StateAbbreviation, VoterTypeC, ET.ElectoralWeight]
type RawTurnoutR = F.Record RawTurnout
type CPSRawTurnoutJoinCols = [BR.Year, BR.StateAbbreviation, BR.CongressionalDistrict] V.++ BRE.CPSPredictorR

rawCPSTurnout :: (K.KnitEffects r, BR.CacheEffects r)
              => Bool
              -> K.ActionWithCacheTime r BRE.CCESAndPUMS
              -> K.Sem r (K.ActionWithCacheTime r (F.Frame RawTurnoutR))
rawCPSTurnout clearCache dat_C = do
  let cacheKey = "model/turnout/cpsRaw.bin"
  when clearCache $ BR.clearIfPresentD cacheKey
  let getEW r = F.rgetField @BRCF.WeightedSuccesses r / F.rgetField @BRCF.WeightedCount r
      ewC = FT.recordSingleton @ET.ElectoralWeight . getEW
      vtC r = FT.recordSingleton @VoterTypeC $ if (wnh r) then WNHV else VOC
      newCols r = ewC r `V.rappend` vtC r
      ew = F.rgetField @ET.ElectoralWeight
      cit = realToFrac . F.rgetField @PUMS.Citizens
      rawProbFld :: FL.Fold (F.Record [PUMS.Citizens, ET.ElectoralWeight]) (F.Record '[ET.ElectoralWeight])
      rawProbFld = (\x y -> FT.recordSingleton @ET.ElectoralWeight $ x/y)
                   <$> FL.premap (\r -> ew r * cit r) FL.sum
                   <*> FL.premap cit FL.sum
      fld = FMR.concatFold
            $ FMR.mapReduceFold
            (FMR.simpleUnpack $ FT.mutate newCols)
            (FMR.assignKeysAndData @[BR.Year, BR.StateAbbreviation, VoterTypeC])
            (FMR.foldAndAddKey rawProbFld)
  BR.retrieveOrMakeFrame cacheKey dat_C $ \dat -> do
    let (joined, missing) = FJ.leftJoinWithMissing @CPSRawTurnoutJoinCols (BRE.cpsVRows dat) (BRE.pumsRows dat)
    when (not $ null missing) $ K.knitError "rawCPSTurnout: pumsData is missing keys from cpsData"
    return $ FL.fold fld joined


rawCPSToHeidiFrame :: F.FrameRec RawTurnout -> Heidi.Frame (Heidi.Row [Heidi.TC] Heidi.VP)
rawCPSToHeidiFrame rs = Heidi.frameFromList $ fmap f $ FL.fold FL.list $ FL.fold outerFld rs where
  innerFld :: FL.Fold (F.Record [VoterTypeC, ET.ElectoralWeight]) (F.Record ['("gap", Double), '("sigma", Double)])
  innerFld =
    let p = F.rgetField @ET.ElectoralWeight
        sp r = let x = p r in x * (1 - x)
        vp r = sp r * sp r
        sgn r = if F.rgetField @VoterTypeC r == VOC then 1 else -1
        gFld = FL.premap (\r -> sgn r * p r) FL.sum
        sFld = sqrt <$> FL.premap vp FL.sum
    in  (\x y -> x F.&: y F.&: V.RNil) <$> gFld <*> sFld
  outerFld :: FL.Fold RawTurnoutR (F.FrameRec [BR.Year, BR.StateAbbreviation, '("gap", Double), '("sigma", Double)])
  outerFld = FMR.concatFold
             $ FMR.mapReduceFold
             FMR.noUnpack
             (FMR.assignKeysAndData @[BR.Year, BR.StateAbbreviation])
             (FMR.foldAndAddKey innerFld)
  gap = F.rgetField @'("gap", Double)
  sigma = F.rgetField @'("sigma", Double)
  f r = Heidi.rowFromList
        $ concat [(valToLabeledKV "State" $ F.rgetField @BR.StateAbbreviation r)
                 , (valToLabeledKV "Year" $ show @Text $ F.rgetField @BR.Year r)
                 , (valToLabeledKV "Type" ("CPS Binomial" :: Text))
                 , (valToLabeledKV "lo" $ negate $ 100 * 0.88 * sigma r)
                 , (valToLabeledKV "mid" $ 100 * gap r)
                 , (valToLabeledKV "hi" $ 100 * 0.88 * sigma r)
                 ]

type CCESRawTurnoutJoinCols = [BR.Year, BR.StateAbbreviation, BR.CongressionalDistrict]
                              V.++  [DT.SimpleAgeC, DT.SexC, DT.CollegeGradC, DT.Race5C]

rawCCESTurnout :: (K.KnitEffects r, BR.CacheEffects r)
              => Bool
              -> K.ActionWithCacheTime r BRE.CCESAndPUMS
              -> K.Sem r (K.ActionWithCacheTime r (F.Frame RawTurnoutR))
rawCCESTurnout clearCache dat_C = do
  let cacheKey = "model/turnout/ccesRaw.bin"
  when clearCache $ BR.clearIfPresentD cacheKey
  let getEW r = realToFrac (F.rgetField @BRE.TVotes r) / realToFrac (F.rgetField @BRE.Surveyed r)
      ewC = FT.recordSingleton @ET.ElectoralWeight . getEW
      vtC r = FT.recordSingleton @VoterTypeC $ if (wnh r) then WNHV else VOC
      newCols r = ewC r `V.rappend` vtC r
      ew = F.rgetField @ET.ElectoralWeight
      cit = realToFrac . F.rgetField @PUMS.Citizens
      rawProbFld :: FL.Fold (F.Record [PUMS.Citizens, ET.ElectoralWeight]) (F.Record '[ET.ElectoralWeight])
      rawProbFld = (\x y -> FT.recordSingleton @ET.ElectoralWeight $ x/y)
                   <$> FL.premap (\r -> ew r * cit r) FL.sum
                   <*> FL.premap cit FL.sum
      fld = FMR.concatFold
            $ FMR.mapReduceFold
            (FMR.simpleUnpack $ FT.mutate newCols)
            (FMR.assignKeysAndData @[BR.Year, BR.StateAbbreviation, VoterTypeC])
            (FMR.foldAndAddKey rawProbFld)
  BR.retrieveOrMakeFrame cacheKey dat_C $ \dat -> do
    let addR5ToPUMS r = FT.recordSingleton @DT.Race5C
                        $ DT.race5FromRaceAlone4AndHisp True (F.rgetField @DT.RaceAlone4C r) (F.rgetField @DT.HispC r)
        pumsWithRace5 = fmap (FT.mutate addR5ToPUMS) $ BRE.pumsRows dat
        electionYears = [2014, 2016, 2018, 2020]
        isElectionYear r = F.rgetField @BR.Year r `elem` electionYears
        eYearCCES = F.filterFrame isElectionYear $ BRE.ccesRows dat
    let (joined, missing) = FJ.leftJoinWithMissing @CCESRawTurnoutJoinCols eYearCCES pumsWithRace5
    unless (null missing) $ K.knitError $ "rawCCESTurnout: pumsData is missing keys from ccesData: " <> show missing
    return $ FL.fold fld joined


data SSTData = SSTD_CPS | SSTD_CCES deriving (Eq, Ord, Show)
nameSSTData :: SSTData -> Text
nameSSTData SSTD_CPS = "cpsV"
nameSSTData SSTD_CCES = "cces"


valToLabeledKV l = FH.labelAndFlatten l . Heidi.toVal

modelToHeidiFrame :: Text -> Text -> Map Text [Double] -> Heidi.Frame (Heidi.Row [Heidi.TC] Heidi.VP)
modelToHeidiFrame y t m = Heidi.frameFromList $ fmap f $ M.toList m where
  f :: (Text, [Double]) -> Heidi.Row [Heidi.TC] Heidi.VP
  f (s, [lo, mid, hi]) = Heidi.rowFromList
                         $ concat [(valToLabeledKV "State" s)
                                  , (valToLabeledKV "Year" y)
                                  , (valToLabeledKV "Type" t)
                                  , (valToLabeledKV "lo" $ 100 * (lo - mid))
                                  , (valToLabeledKV "mid" $ 100 * mid)
                                  , (valToLabeledKV "hi" $ 100 * (hi - mid))
                                  ]

rankToHeidiFrame :: Text -> Text -> Map Text [Double] -> Heidi.Frame (Heidi.Row [Heidi.TC] Heidi.VP)
rankToHeidiFrame y t m = Heidi.frameFromList $ fmap f $ M.toList m where
  f :: (Text, [Double]) -> Heidi.Row [Heidi.TC] Heidi.VP
  f (s, [lo, mid, hi]) = Heidi.rowFromList
                         $ concat [(valToLabeledKV "State" s)
                                  , (valToLabeledKV "Year" y)
                                  , (valToLabeledKV "Type" t)
                                  , (valToLabeledKV "lo" $ (lo - mid))
                                  , (valToLabeledKV "mid" $ mid)
                                  , (valToLabeledKV "hi" $ (hi - mid))
                                  ]


modelToHeidiFrame' :: Int -> Text -> Map Text [Double] -> Heidi.Frame (Heidi.Row [Heidi.TC] Heidi.VP)
modelToHeidiFrame' y t m = Heidi.frameFromList $ fmap f $ M.toList m where
  f :: (Text, [Double]) -> Heidi.Row [Heidi.TC] Heidi.VP
  f (s, [lo, mid, hi]) = Heidi.rowFromList
                         $ concat [(valToLabeledKV "State" s)
                                  , (valToLabeledKV "Year" y)
                                  , (valToLabeledKV "Type" t)
                                  , (valToLabeledKV "lo" $ 100 * (lo - mid))
                                  , (valToLabeledKV "mid" $ 100 * mid)
                                  , (valToLabeledKV "hi" $ 100 * (hi - mid))
                                  ]

modelHeidiToVLData = HV.rowsToVLData [] [HV.asStr "State"
                                        ,HV.asStr "Year"
                                        ,HV.asStr "Type"
                                        ,HV.asNumber "lo"
                                        ,HV.asNumber "mid"
                                        ,HV.asNumber "hi"
                                        ]

modelHeidiToVLData' = HV.rowsToVLData [] [HV.asStr "State"
                                        ,HV.asNumber "Year"
                                        ,HV.asStr "Type"
                                        ,HV.asNumber "lo"
                                        ,HV.asNumber "mid"
                                        ,HV.asNumber "hi"
                                        ]

sortedStates x = fst <$> (sortOn (\(_,[_,x,_]) -> -x) $ M.toList x)
revSortedStates x = fst <$> (sortOn (\(_,[_,x,_]) -> x) $ M.toList x)
sortedDistricts x = fst <$> (sortOn (\(_,[_,x,_]) -> -x) $ M.toList x)

stateAndCD :: Text -> Either Text (Text, Int)
stateAndCD t = do
  let state = T.take 2 t
  cd <- case T.readMaybe $ toString $ T.drop 3 t of
    Nothing -> Left $ "Failed to parse " <> t <> " as state and CD"
    Just n -> pure n
  return (state, cd)

districtModelToHeidiFrame :: Text -> Text -> Map Text [Double] -> Either Text (Heidi.Frame (Heidi.Row [Heidi.TC] Heidi.VP))
districtModelToHeidiFrame y t m = Heidi.frameFromList <$> (traverse f $ M.toList m) where
  f :: (Text, [Double]) -> Either Text (Heidi.Row [Heidi.TC] Heidi.VP)
  f (s, [lo, mid, hi]) = do
    (state, nCD) <- stateAndCD s
    return $ Heidi.rowFromList
      $ concat [(valToLabeledKV "CD" s)
               , (valToLabeledKV "State" state)
               , (valToLabeledKV "district" nCD)
               , (valToLabeledKV "Year" y)
               , (valToLabeledKV "Type" t)
               , (valToLabeledKV "lo" $ 100 * (lo - mid))
               , (valToLabeledKV "mid" $ 100 * mid)
               , (valToLabeledKV "hi" $ 100 * (hi - mid))
               ]

districtModelHeidiToVLData = HV.rowsToVLData [] [HV.asStr "CD"
                                                ,HV.asStr "Year"
                                                ,HV.asStr "Type"
                                                ,HV.asNumber "lo"
                                                ,HV.asNumber "mid"
                                                ,HV.asNumber "hi"
                                        ]


districtSpecificTurnout :: (K.KnitMany r, K.KnitOne r, BR.CacheEffects r)
                        => Bool
                        -> BR.PostPaths BR.Abs
                        -> BR.PostInfo
                        -> K.ActionWithCacheTime r BRE.CCESAndPUMS
                        -> K.Sem r ()
districtSpecificTurnout clearCaches postPaths postInfo dataAllYears_C = K.wrapPrefix "districtSpecificTurnout" $ do
  K.logLE K.Info $ "Re-building districtSpecificTurnout post"
  BR.brAddPostMarkDownFromFile postPaths "_intro"
  modeledTurnout_C <- districtSpecificTurnoutModel clearCaches True SSTD_CPS [2020] dataAllYears_C
  (fullGap, stateGap, demographicGap, districtSpecificGap, districtOnlyGap) <- K.ignoreCacheTime modeledTurnout_C
  K.logLE K.Info $ "District result has " <> show (M.size districtSpecificGap) <> " rows."
  hDistrictSpecificGap <- K.knitEither $  districtModelToHeidiFrame "2020" "District-Specific" districtSpecificGap
  hDistrictOnlyGap <- K.knitEither $  districtModelToHeidiFrame "2020" "District-Specific" districtOnlyGap
  let inState :: Text -> Heidi.Row [Heidi.TC] Heidi.VP -> Bool
      inState s r = maybe False (== s) $ r ^? Heidi.text (BR.heidiColKey "State")
  let gaDistrictSG = Heidi.filter (inState "GA")  hDistrictSpecificGap
      gaDistrictOG = Heidi.filter (inState "GA")  hDistrictOnlyGap
      txDistrictSG = Heidi.filter (inState "TX")  hDistrictSpecificGap
      txDistrictOG = Heidi.filter (inState "TX")  hDistrictOnlyGap
  _ <- K.knitEither (districtModelHeidiToVLData gaDistrictSG) >>=
       K.addHvega Nothing
       (Just "GA District-specific turnout gaps")
       . turnoutChart
       "GA District-Specific Contribution to Turnout Gap (2020)"
       "CD"
       (sortedDistricts districtSpecificGap)
       (TurnoutChartOptions False True ColorIsType Nothing Nothing False)
       (FV.ViewConfig 500 250 5)

  _ <- K.knitEither (districtModelHeidiToVLData gaDistrictOG) >>=
       K.addHvega Nothing
       (Just "GA District-Only turnout gaps")
       . turnoutChart
       "GA District-Only Contribution to Turnout Gap (2020)"
       "CD"
       (sortedDistricts districtOnlyGap)
       (TurnoutChartOptions False True ColorIsType Nothing Nothing False)
       (FV.ViewConfig 600 250 5)
  _ <- K.knitEither (districtModelHeidiToVLData txDistrictSG) >>=
       K.addHvega Nothing
       (Just "TX District-specific turnout gaps")
       . turnoutChart
       "TX District-Specific Contribution to Turnout Gap (2020)"
       "CD"
       (sortedDistricts districtSpecificGap)
       (TurnoutChartOptions False True ColorIsType Nothing Nothing False)
       (FV.ViewConfig 600 500 5)
  _ <- K.knitEither (districtModelHeidiToVLData txDistrictOG) >>=
       K.addHvega Nothing
       (Just "TX District-Only turnout gaps")
       . turnoutChart
       "TX District-Only Contribution to Turnout Gap (2020)"
       "CD"
       (sortedDistricts districtOnlyGap)
       (TurnoutChartOptions False True ColorIsType Nothing Nothing False)
       (FV.ViewConfig 600 500 5)
  return ()

gapsOverTime :: (K.KnitMany r, K.KnitOne r, BR.CacheEffects r)
             => Bool
             -> BR.PostPaths BR.Abs
             -> BR.PostInfo
             -> K.ActionWithCacheTime r BRE.CCESAndPUMS
             -> K.Sem r ()
gapsOverTime clearCaches postPaths postInfo dataAllYears_C = K.wrapPrefix "gapsOverTime" $ do
  K.logLE K.Info $ "Re-building gapsOverTime post"
  let doYear (y, t) = do
        res_C <- stateSpecificTurnoutModel clearCaches True SSTD_CPS [y] dataAllYears_C
        ((_, _, rtDiffI, _, _, _, _), _, _) <- K.ignoreCacheTime res_C
        return $ modelToHeidiFrame' y t rtDiffI
  diffsByYear <- traverse doYear [(2012, "Presidential")
                                 ,(2014, "Mid-term")
                                 ,(2016, "Presidential")
                                 ,(2018, "Mid-term")
                                 ,(2020, "Presidential")
                                 ]
  BR.brAddPostMarkDownFromFile postPaths "_intro"
  _ <- K.knitEither (modelHeidiToVLData' $ mconcat diffsByYear) >>=
       K.addHvega
       Nothing
       (Just "State-Specific Turnout Gaps over Time (NB: Each state has its own turnout scale)")
       . gapsOverTimeChart
       ("State-Specific VOC/WNH Turnout Gap (2012 - 2020)")
       Nothing
       (FV.ViewConfig 120 100 5)
  return ()

gapsOverTimeChart :: Text -> Maybe [Text] -> FV.ViewConfig -> GV.Data -> GV.VegaLite
gapsOverTimeChart title mSortedStates vc dat =
  let stateSort = maybe [] (\x -> [GV.FSort [GV.CustomSort $ GV.Strings x]]) mSortedStates
      tScale = [] --[GV.PScale [GV.SDomain $ GV.DNumbers [-30,40]]]
      encYear = GV.position GV.X [GV.PName "Year", GV.PmType GV.Quantitative, GV.PAxis [GV.AxFormatAsNum, GV.AxFormat "d"]]
      encTLo = GV.position GV.YError $ [GV.PName "lo", GV.PmType GV.Quantitative] ++ tScale
      encTMid = GV.position GV.Y $ [GV.PName "mid", GV.PmType GV.Quantitative] ++  tScale
      encTHi = GV.position GV.YError2 $ [GV.PName "hi", GV.PmType GV.Quantitative] ++ tScale
      encState = GV.facetFlow ([GV.FName "State", GV.FmType GV.Nominal] ++ stateSort)
      encoding = GV.encoding . encYear . encTLo . encTMid . encTHi
      markBand = GV.mark GV.ErrorBand []
      markMid = GV.mark GV.Line [GV.MSize 1, GV.MColor "orange"]
      encZeroY = GV.position GV.Y [GV.PDatum $ GV.Number 0]
      resolve = GV.resolve . GV.resolution (GV.RScale [(GV.ChY, GV.Independent)])
      stateBandSpec = GV.asSpec [(GV.encoding . encYear . encTLo . encTMid . encTHi) [], markBand]
      stateLineSpec = GV.asSpec [(GV.encoding . encYear . encTMid) [], markMid]
      zeroSpec = GV.asSpec [(GV.encoding . encZeroY . encYear) [], GV.mark GV.Line []]
      stateSpec = GV.asSpec $ [GV.layer [stateBandSpec, zeroSpec, stateLineSpec]]
  in FV.configuredVegaLite vc [FV.title title
                              , GV.specification stateSpec
                              , encState
--                              , resolve []
                              , GV.columns 4
                              , dat]

cpsStateRace :: (K.KnitMany r, K.KnitOne r, BR.CacheEffects r)
             => Bool
             -> BR.PostPaths BR.Abs
             -> BR.PostInfo
             -> K.ActionWithCacheTime r BRE.CCESAndPUMS
             -> K.Sem r ()
cpsStateRace clearCaches postPaths postInfo dataAllYears_C = K.wrapPrefix "cpsStateRace" $ do

  res2020_C <- stateSpecificTurnoutModel clearCaches True SSTD_CPS [2020] dataAllYears_C
  ((rtDiffWI_2020, rtDiffNI_2020, rtDiffI_2020, rtNWNH_2020, rtWNH_2020, dNWNH_2020, dWNH_2020), gapVar, gapRange) <- K.ignoreCacheTime res2020_C

  res2020_NI_C <- stateSpecificTurnoutModel clearCaches False SSTD_CPS [2020] dataAllYears_C
  ((rtDiffWI_2020_NI, rtDiffNI_2020_NI, rtDiffI_2020_NI, rtNWNH_2020_NI, rtWNH_2020_NI, dNWNH_2020_NI, dWNH_2020_NI), _, _) <- K.ignoreCacheTime res2020_NI_C

  let hfToVLDataPEI = HV.rowsToVLData [] [HV.asStr "State"
                                         ,HV.asStr "Year"
                                         ,HV.asStr "Type"
                                         ,HV.asNumber "lo"
                                         ,HV.asNumber "mid"
                                         ,HV.asNumber "hi"
                                         ,HV.asNumber "ratingstate"
                                         ,HV.asNumber "votingi"

                                      ]

      dNWNH_h_2020 = modelToHeidiFrame "2020" "VOC" dNWNH_2020
      dWNH_h_2020 = modelToHeidiFrame "2020" "WHNV" dWNH_2020
      rtDiffNI_h_2020 = modelToHeidiFrame "2020" "Demographic Turnout Gap" rtDiffNI_2020
      rtNWNH_h_2020 = modelToHeidiFrame "2020" "VOC Turnout" rtNWNH_2020
      rtWNH_h_2020 = modelToHeidiFrame "2020" "WNHV Turnout" rtWNH_2020
  data2020 <- K.ignoreCacheTime $ fmap (BRE.ccesAndPUMSForYears [2020]) dataAllYears_C
  rawCPST_C <- rawCPSTurnout False dataAllYears_C
  rawCPST <- K.ignoreCacheTime rawCPST_C
  let rawCPS2020_h = rawCPSToHeidiFrame $ F.filterFrame (\r -> F.rgetField @BR.Year r == 2020) rawCPST
  BR.logFrame rawCPST
  let wnhFld b = fmap (Heidi.frameFromList . fmap FH.recordToHeidiRow . FL.fold FL.list)
                 $ FMR.concatFold
                 $ FMR.mapReduceFold
                 (FMR.unpackFilterRow $ \r -> if b then wnh r else not $ wnh r)
                 (FMR.assignKeysAndData @'[BR.StateAbbreviation] @'[PUMS.Citizens])
                 (FMR.foldAndAddKey $ FF.foldAllConstrained @Num FL.sum)
      (nwnhCitByState, wnhCitByState) = FL.fold ((,) <$> wnhFld False <*> wnhFld True) $ BRE.pumsRows data2020

  citByState <- K.knitEither $ do
    let k = [Heidi.mkTyN "state_abbreviation"]
    wnh <- traverse (BR.rekeyCol [Heidi.mkTyN "Citizens"] [Heidi.mkTyN "WNH VEP"]) wnhCitByState
    nwnh <- traverse (BR.rekeyCol [Heidi.mkTyN "Citizens"] [Heidi.mkTyN "NWNH VEP"]) nwnhCitByState
    traverse (BR.rekeyCol [Heidi.mkTyN "state_abbreviation"] [Heidi.mkTyN "State"]) $ Heidi.leftOuterJoin k k nwnh wnh

  let
      addCols l y m = M.fromList [("Label", GV.Str l), ("Year", GV.Str y)] <> m
      filterState states =  K.knitMaybe "row missing State col" . Heidi.filterA (fmap (`elem` states) . Heidi.txt [Heidi.mkTyN "State"])
  electionIntegrity <- K.ignoreCacheTimeM BR.electionIntegrityByState2018
  let electionIntegrityh = Heidi.frameFromList
                           $ fmap FH.recordToHeidiRow
                           $ FL.fold FL.list electionIntegrity
      dNWNH_PEI_h_2020  = Heidi.leftOuterJoin
                          [Heidi.mkTyN "State"]
                          [Heidi.mkTyN "state_abbreviation"]
                          dNWNH_h_2020
                          electionIntegrityh
--  K.logLE K.Info $ show $ FL.fold FL.length electionIntegrityh
  let rtDiffNIh_2020 = modelToHeidiFrame "2020" "Demographic Turnout Gap" rtDiffNI_2020
      rtDiffNIh_2020_NI  = modelToHeidiFrame "2020" "Demographic Turnout Gap (NI model)" rtDiffNI_2020_NI
      rtDiffWIh_2020 = modelToHeidiFrame "2020" "Full Turnout Gap" rtDiffWI_2020
      rtDiffIh_2020 = modelToHeidiFrame "2020" "State-Specific Turnout Gap" rtDiffI_2020
  let chartW = 650
  let gapNoteName = BR.Unused "gaps"
  _ <- BR.brNewNote postPaths postInfo gapNoteName "Modeled VOC/WNH Turnout Gaps" $ do
    BR.brAddNoteMarkDownFromFile postPaths gapNoteName "1"
    _ <- K.knitEither (modelHeidiToVLData rtDiffNIh_2020) >>=
         K.addHvega Nothing Nothing
         . turnoutChart
         ("VOC/WNH Turnout Gap: Demographics Only")
         "State"
         (sortedStates rtDiffNI_2020)
         (TurnoutChartOptions True True ColorIsType (Just 22) (Just "Turnout Gap (%)") False)
         (FV.ViewConfig chartW 1000 5)
    BR.brAddNoteMarkDownFromFile postPaths gapNoteName "2"
    _ <- K.knitEither (modelHeidiToVLData rtDiffWIh_2020) >>=
         K.addHvega Nothing Nothing
         . turnoutChart
         ("VOC/WNH Turnout Gaps: Demographics & State-Specific Effects")
         "State"
         (sortedStates rtDiffWI_2020)
         (TurnoutChartOptions True True ColorIsType (Just 35) (Just "Turnout Gap (%)") False)
         (FV.ViewConfig chartW 1000 5)
    BR.brAddNoteMarkDownFromFile postPaths gapNoteName "3"
    _ <- K.knitEither (modelHeidiToVLData rtDiffIh_2020) >>=
         K.addHvega Nothing Nothing
         . turnoutChart
         ("VOC/WNH State-Specific Turnout Gap")
         "State"
         (sortedStates rtDiffI_2020)
         (TurnoutChartOptions False True ColorIsType (Just 25) (Just "State-Specific Turnout Gap (%)") False)
         (FV.ViewConfig chartW 1000 5)
    return ()
  -- NoteUrl <- K.knitMaybe "gap Note Url is Nothing" $ mGapNoteUrl
  --  let gapNoteRef = "[gapNote_link]: " <> gapNoteUrl
  let naiveModelNoteName = BR.Used "Naive_Model"
  mNaiveModelUrl <- BR.brNewNote postPaths postInfo naiveModelNoteName "Models of VOC/WNH Turnout Gaps" $ do
    BR.brAddNoteMarkDownFromFile postPaths naiveModelNoteName "_intro"
    _ <- K.knitEither (modelHeidiToVLData (rtDiffWIh_2020 <> rawCPS2020_h)) >>=
           K.addHvega
           (Just "figure_naiveCompare")
           (Just "Comparison of post-stratified simple Binomial model to MRP.")
           . turnoutChart
           ("VOC/WNH Turnout Gaps")
           "State"
           (sortedStates rtDiffWI_2020)
           (TurnoutChartOptions True True ColorIsType Nothing (Just "Turnout Gap (%)") False)
           (FV.ViewConfig chartW 1000 5)
    return ()
  naiveModelNoteUrl <- K.knitMaybe "naive Model Note Url is Nothing" $ mNaiveModelUrl
  let naiveModelRef = "[niNaiveModel_link]: " <> naiveModelNoteUrl
  BR.brAddPostMarkDownFromFileWith postPaths "_intro" (Just naiveModelRef)
  _ <- K.knitEither (modelHeidiToVLData rtDiffWIh_2020) >>=
       K.addHvega
       (Just "figure_fullGap")
       (Just "Figure 1: Modeled VOC/WHNV turnout gaps in the 2020 general election.")
       . turnoutChart
       ("VOC/WNH Turnout Gaps")
       "State"
       (sortedStates rtDiffWI_2020)
       (TurnoutChartOptions True True ColorIsType (Just 35) (Just "Turnout Gap (%)") False)
       (FV.ViewConfig chartW 1000 5)
  let niComparisonNoteName = BR.Used "NI_Comparison"
  mNIComparisonNoteUrl <- BR.brNewNote postPaths postInfo niComparisonNoteName "Comparison of Models with no State/Race Interactions" $ do
    BR.brAddNoteMarkDownFromFile postPaths niComparisonNoteName "_intro"
    full <- K.knitMaybe "Error changing to model types for demographic only comparison note"
      $ traverse (Heidi.at (BR.heidiColKey "Type") $ const (Just $ Just $ Heidi.VPText "Full Model")) rtDiffNIh_2020
    noSRI <- K.knitMaybe "Error changing to model types for demographic only comparison note"
      $ traverse (Heidi.at (BR.heidiColKey "Type") $ const (Just $ Just $ Heidi.VPText "No SRI Model")) rtDiffNIh_2020_NI
    _ <- K.knitEither (modelHeidiToVLData (full <> noSRI)) >>=
         K.addHvega Nothing Nothing
         . turnoutChart
         ("VOC/WNH Turnout Gap: Demographics Only")
         "State"
         (sortedStates rtDiffNI_2020)
         (TurnoutChartOptions True True ColorIsType Nothing (Just "Turnout Gap (%)") False)
         (FV.ViewConfig chartW 1000 5)
    BR.brAddNoteMarkDownFromFile postPaths niComparisonNoteName "_analysis"
    return ()
  niComparisonNoteUrl <- K.knitMaybe "NI comparison Note Url is Nothing" $ mNIComparisonNoteUrl
  let niComparisonNoteRef = "[niComparison_link]: " <> niComparisonNoteUrl
  BR.brAddPostMarkDownFromFileWith postPaths "_afterFullGaps" (Just niComparisonNoteRef)
  _ <- K.knitEither (modelHeidiToVLData rtDiffNIh_2020) >>=
       K.addHvega
       (Just "figure_demographicOnly")
       (Just "Figure 2: Modeled demographic-only VOC/WHNV turnout gaps in the 2020 general election.")
       . turnoutChart
       ("VOC/WNH Turnout Gap: Demographics Only")
       "State"
       (sortedStates rtDiffNI_2020)
       (TurnoutChartOptions True True ColorIsType (Just 35) (Just "Turnout Gap (%)") False)
       (FV.ViewConfig chartW 1000 5)
  BR.brAddPostMarkDownFromFile postPaths "_afterDemographicOnly"
  _ <- K.knitEither (hfToVLDataPEI rtDiffIh_2020) >>=
       K.addHvega Nothing
       (Just "Figure 3: Modeled state-specific contribution (= total gap - demographic gap) to turnout gaps in the 2020 general election.")
       . turnoutChart
       ("State-Specific Contribution to Turnout Gap (2020)")
       "State"
       (sortedStates rtDiffI_2020)
       (TurnoutChartOptions False True ColorIsType (Just 35) Nothing False)
       (FV.ViewConfig chartW 1000 5)
  K.logLE K.Diagnostic $ "gapVar=" <> show gapVar
  K.logLE K.Diagnostic $ "gapRange=" <> show gapRange
  let rankNoteName = BR.Used "RankStatistics"
  mRankNoteUrl <- BR.brNewNote postPaths postInfo rankNoteName "Modeled Ranks of State-Specific Gaps" $ do
    BR.brAddNoteMarkDownFromFile postPaths rankNoteName "_intro"
    BR.brAddMarkDown $ "Doing this gives a standard-deviation of "
      <> toText @String (Printf.printf "%.1f" (100 * sqrt (gapVar List.!! 1)))
      <> "% with a 90% confidence interval of "
      <> toText @String (Printf.printf "%.1f" (100 * sqrt (gapVar List.!! 0)))
      <> "% to "
      <> toText @String (Printf.printf "%.1f" (100 * sqrt (gapVar List.!! 2)))
      <> "%."
    BR.brAddMarkDown $ "And range of "
      <> toText @String (Printf.printf "%.1f" (100 * sqrt (gapRange List.!! 1)))
      <> "% with a 90% confidence interval of "
      <> toText @String (Printf.printf "%.1f" (100 * sqrt (gapRange List.!! 0)))
      <> "% to "
      <> toText @String (Printf.printf "%.1f" (100 * sqrt (gapRange List.!! 2)))
      <> "%."
    return ()
  rankNoteUrl <- K.knitMaybe "Rank Note Url is Nothing" $ mRankNoteUrl
  let rankNoteRef = "[rankNote_link]: " <> rankNoteUrl
  BR.brAddPostMarkDownFromFileWith postPaths "_afterStateSpecific" (Just rankNoteRef)
  let sig lo hi = lo * hi > 0
      sigStates2020 = M.keys $ M.filter (\[lo, _, hi] -> sig lo hi) rtDiffI_2020
  rtNWNH_sig <- filterState sigStates2020 rtDiffIh_2020
  _ <- K.knitEither (hfToVLDataPEI rtNWNH_sig) >>=
       K.addHvega Nothing
       (Just "Figure 4: Modeled state-specific contribution to turnout gaps in the 2020 general election. Clearly non-zero only.")
       . turnoutChart
       ("Significant State-Specific Contribution to VOC/WHNV Turnout Gaps (2020)")
       "State"
       (sortedStates rtDiffI_2020)
       (TurnoutChartOptions False True ColorIsType (Just 23) Nothing False)
       (FV.ViewConfig chartW 400 5)
  BR.brAddPostMarkDownFromFile postPaths "_afterSigStates"
{-
  -- zoom in on components of gaps
  _ <- K.knitEither (modelHeidiToVLData (dNWNH_h_2020 <> dWNH_h_2020)) >>=
       K.addHvega Nothing
       (Just "Figure 5: VOC/WHNV components of significant gaps.")
       . ssGapComponentsChart
       ("VOC/WNHV components of Significant Gaps")
       (Just $ sortedStates rtDiffI_2020)
       (FV.ViewConfig 100 30 5)
-}

  let integrityNoteName = BR.Unused "ElectionIntegrity"
  _ <- BR.brNewNote postPaths postInfo integrityNoteName "Election Integrity & State-Specific Turnout Effects" $ do
    BR.brAddNoteMarkDownFromFile postPaths integrityNoteName "_intro"
--    addMarkDownFromFile $ mdDir ++ "Integrity1.md"
    _ <- K.knitEither (hfToVLDataPEI  dNWNH_PEI_h_2020) >>=
         K.addHvega Nothing Nothing
         .peiScatterChart
         ("State-Specific VOC Turnout vs. Voting Integrity")
         (FV.ViewConfig 400 400 5)
    BR.brAddNoteMarkDownFromFile postPaths integrityNoteName "_afterScatter"
  let componentNoteName = BR.Unused "Components"
  _ <- BR.brNewNote postPaths postInfo componentNoteName "Components of State-Specific Turnout" $ do
    dNWNH_renamed <- traverse (K.knitEither . BR.rekeyCol [Heidi.mkTyN "mid"] [Heidi.mkTyN "State-Specific"])  dNWNH_PEI_h_2020
    rtNWNH_renamed <- traverse (K.knitEither . BR.rekeyCol [Heidi.mkTyN "mid"] [Heidi.mkTyN "VOC Total"]) rtNWNH_h_2020
    rtWNH_renamed <-  traverse (K.knitEither . BR.rekeyCol [Heidi.mkTyN "mid"] [Heidi.mkTyN "WNH Total"]) rtWNH_h_2020
    let k = BR.heidiColKey "State"
        nwnh_sig' = Heidi.leftOuterJoin k k citByState
                    $ Heidi.leftOuterJoin k k dNWNH_renamed
                    $ Heidi.leftOuterJoin k k rtNWNH_renamed rtWNH_renamed
        add_nwnh_dem r = K.knitMaybe "Missing column when computing nwnh_dem" $ do
          vocT <- r ^? Heidi.double (BR.heidiColKey "VOC Total")
          wnhT <- r ^? Heidi.double (BR.heidiColKey "WNH Total")
          voc <- r ^? Heidi.int (BR.heidiColKey "NWNH VEP")
          wnh  <- r ^? Heidi.int (BR.heidiColKey "WNH VEP")
          vocSST <- r ^? Heidi.double (BR.heidiColKey "State-Specific")
          let turnout = ((vocT * realToFrac voc) + (wnhT * realToFrac wnh))/realToFrac (voc + wnh)
              voc_dem = vocT - wnhT - vocSST
          return
            $ Heidi.insert (BR.heidiColKey "Demographic") (Heidi.VPDouble voc_dem)
            $ Heidi.insert (BR.heidiColKey "VOC - WNH") (Heidi.VPDouble $ voc_dem + vocSST) r
    nwnh_sig <- traverse add_nwnh_dem nwnh_sig'
    let nwnh_sig_long = Heidi.gatherWith
                        BR.tcKeyToTextValue
                        (BR.gatherSet [] ["State-Specific", "Demographic", "VOC - WNH"])
                        (BR.heidiColKey "VOC Turnout Component")
                        (BR.heidiColKey "Turnout")
                        nwnh_sig
    let hfToVLDataBreakdown = HV.rowsToVLData [] [HV.asStr "State"
                                                 ,HV.asStr "VOC Turnout Component"
                                                 ,HV.asNumber "Turnout"
                                                 ]
    BR.brAddNoteMarkDownFromFile postPaths componentNoteName "_intro"
    _ <- filterState sigStates2020 nwnh_sig_long
         >>= K.knitEither . hfToVLDataBreakdown  >>=
         K.addHvega Nothing Nothing
         . gapComponentsChart
         ("VOC/WNH Turnout Gap Components (2020)")
         (Just $ sortedStates dNWNH_2020)
         (FV.ViewConfig 200 40 5)
    BR.brAddNoteMarkDownFromFile postPaths componentNoteName "_afterSigComponents"
--    addMarkDownFromFileWithRefs note2Ref $ mdDir ++ "P4.md"
--  K.newPandoc
--    (K.PandocInfo (notesPath "2")
--     $ BR.brAddDates False pubDate curDate
--     $ one ("pagetitle","VOC/WNH Turnout Components for all states")) $ do
    _ <- K.knitEither (hfToVLDataBreakdown nwnh_sig_long) >>=
      K.addHvega Nothing Nothing
      . gapComponentsChart
      ("VOC Turnout Components (2020)")
      Nothing
      (FV.ViewConfig 200 40 5)
    return ()
{-
  K.newPandoc
    (K.PandocInfo (notesPath "3")
     $ BR.brAddDates False pubDate curDate
     $ one ("pagetitle","State-Specific gaps, 2012 & Both stuff")) $ do
    res2012_C <- Polysemy.raise $ runModel [2012]
    (_, _, _, _, _, dNWNH_2012, _)  <- Polysemy.raise $ K.ignoreCacheTime res2012_C
    let dNWNH_h_2012 = toHeidiFrame "2012" dNWNH_2012
        dNWNH_PEI_h_2012  = Heidi.leftOuterJoin
                            [Heidi.mkTyN "State"]
                            [Heidi.mkTyN "state_abbreviation"]
                            dNWNH_h_2012
                            electionIntegrityh
        dNWNH_h_2012_2020 = dNWNH_PEI_h_2012 <> dNWNH_PEI_h_2020
    _ <- K.knitEither (hfToVLDataPEI dNWNH_PEI_h_2012) >>=
         K.addHvega Nothing Nothing
         . coefficientChart
         ("State=Specific VOC Turnout (2012)")
         (sortedStates dNWNH_2012)
         True
         True
         (FV.ViewConfig 500 1000 5)

    let oneSig [loA,_ , hiA] [loB,_ , hiB] = if sig loA hiA || sig loB hiB then Just () else Nothing
        oneSigStates =  M.keys
                        $ M.merge M.dropMissing M.dropMissing (M.zipWithMaybeMatched (const oneSig)) dNWNH_2012 dNWNH_2020
    combinedOneSig_h <- filterState oneSigStates dNWNH_h_2012_2020
    addMarkDownFromFile $ mdDir ++ "N3a.md"
    _ <- K.knitEither (hfToVLDataPEI combinedOneSig_h) >>=
         K.addHvega' Nothing Nothing True
         . turnoutGapScatter
         ("State-Specific VOC Turnout: 2012 vs. 2020")
         (FV.ViewConfig 500 500 5)
    addMarkDownFromFile $ mdDir ++ "N3b.md"
    let sigBoth [loA,_ , hiA] [loB,_ , hiB] = if sig loA hiA && sig loB hiB && loA * loB > 0 then Just () else Nothing
        sigMove [loA,_ , hiA] [loB,_ , hiB] = if hiA < loB || hiB < loA then Just () else Nothing
        significantPersistent = M.keys
                                $ M.merge M.dropMissing M.dropMissing (M.zipWithMaybeMatched (const sigBoth)) dNWNH_2012 dNWNH_2016
        significantMove = M.keys
                          $ M.merge M.dropMissing M.dropMissing (M.zipWithMaybeMatched (const sigMove)) dNWNH_2012 dNWNH_2016
    significantMove_h <- filterState significantMove dNWNH_h_2012_2016
    signifcantPersistent_h <- filterState significantPersistent dNWNH_h_2012_2016
    _ <-  K.knitEither (hfToVLData  signifcantPersistent_h) >>=
          K.addHvega Nothing Nothing
          . coefficientChart
          ("State-Specific VOC Turnout: significant *and* persistent effects in 2012 and 2016")
          (sortedStates dNWNH_2012)
          False
          False
          (FV.ViewConfig 400 400 5)
    _ <- K.knitEither (hfToVLData significantMove_h) >>=
         K.addHvega Nothing Nothing
         . coefficientChart
         ("State-Specific Turnout: significant *changes* 2012 to 2016")
         (sortedStates dNWNH_2012)
         False
         False         (FV.ViewConfig 400 400 5)
    return ()
-}
{-
  res2012_2016_C <- runModel [2012, 2016]
  (_, _, rtDiffI_2012_2016, _, _, dNWNH_2012_2016, _) <- K.ignoreCacheTime res2012_2016_C
  let dNWNH_h_2012_2016 = toHeidiFrame "2012 & 2016" dNWNH_2012_2016
--  let diffIMR_2012_2016 = MapRow.joinKeyedMapRows rtDiffIMR_2012 $ stateDiffFromAvgMRs 2016
  addMarkDownFromFile $ mdDir ++ "N4.md"
  _ <-  K.knitEither (hfToVLData  dNWNH_h_2012_2016) >>=
        K.addHvega Nothing Nothing
        . coefficientChart
        ("State NWNH Turnout Effect (2012 & 2016)")
        (sortedStates dNWNH_2012_2016)
        True
        False
        (FV.ViewConfig 500 1000 5)
-}
  return ()



peiScatterChart :: Text -> FV.ViewConfig -> GV.Data -> GV.VegaLite
peiScatterChart title vc vlData =
  let encVOCTurnout = GV.position GV.Y [GV.PName "mid"
                                       , GV.PmType GV.Quantitative
                                       , GV.PAxis [GV.AxTitle "State-Specific VOC Turnout Shift (% Eligible VOC)"]]
      encEI = GV.position GV.X [GV.PName "votingi"
                               , GV.PmType GV.Quantitative
                               , GV.PScale [GV.SZero False], GV.PAxis [GV.AxTitle "Ease/Fairness of Voting (out of 100)"]]
      enc = GV.encoding . encVOCTurnout . encEI
      mark = GV.mark GV.Circle [GV.MTooltip GV.TTData]
  in FV.configuredVegaLite vc [FV.title title, enc [], mark, vlData]

ssGapComponentsChart :: Text -> Maybe [Text] -> FV.ViewConfig -> GV.Data -> GV.VegaLite
ssGapComponentsChart title mSortedStates vc@(FV.ViewConfig w h _) vlData =
  let compSort = GV.CustomSort $ GV.Strings ["VOC", "WNHV"]
      stateSort = maybe [] (\x -> [GV.FSort [GV.CustomSort $ GV.Strings x]]) $ mSortedStates
      encComp = GV.position GV.Y [GV.PName "Type"
                                 , GV.PmType GV.Nominal
                                 , GV.PNoTitle
                                 , GV.PSort [compSort]
                                 , GV.PAxis [GV.AxLabels False, GV.AxTicks False]
                                 ]
                . GV.color [GV.MName "Type", GV.MmType GV.Nominal
                           , GV.MSort [compSort]
                           , GV.MLegend [GV.LOrient GV.LOBottom]]

      encTurnout o = GV.position GV.X [GV.PName "mid"
                                      , GV.PmType GV.Quantitative
                                      , GV.PAxis [GV.AxTitle "Turnout Change (% Eligible)", GV.AxOrient o]
--                                      , GV.PScale [GV.SDomain $ GV.DNumbers [negate 26, 26]]
                                      ]
      encState = GV.facetFlow ([GV.FName "State", GV.FmType GV.Nominal]  ++ stateSort)

      enc = GV.encoding . encComp . encTurnout GV.SBottom -- . encState
      labelSpec x y l = GV.asSpec [GV.encoding . (GV.position GV.X [GV.PNumber $ x * w]) . (GV.position GV.Y [GV.PNumber $ y * h]) $ []
                                  ,GV.mark GV.Text [GV.MText l, GV.MFont "Verdana" ]
                                  ]
      labelSpecs = [labelSpec 0.1 0.05 "Lower Turnout"
                   , labelSpec 0.1 0.95 "Higher Turnout"
                   ]

      barMark = GV.mark GV.Bar [GV.MTooltip GV.TTEncoding]
      barSpec = GV.asSpec [enc [], barMark]
      res = GV.resolve . GV.resolution (GV.RAxis [(GV.ChX, GV.Independent)
                                                  ,(GV.ChY, GV.Independent)]
                                       )
  in FV.configuredVegaLite vc [FV.title title, GV.specification barSpec, encState, GV.columns 3, vlData]



gapComponentsChart :: Text -> Maybe [Text] -> FV.ViewConfig -> GV.Data -> GV.VegaLite
gapComponentsChart title mSortedStates vc@(FV.ViewConfig w h _) vlData =
  let compSort = GV.CustomSort $ GV.Strings ["State-Specific", "Demographic", "VOC - WNH"]
      stateSort = maybe [] (\x -> [GV.FSort [GV.CustomSort $ GV.Strings x]]) $ mSortedStates
      encComp = GV.position GV.Y [GV.PName "VOC Turnout Component"
                                 , GV.PmType GV.Nominal
                                 , GV.PNoTitle
                                 , GV.PSort [compSort]
                                 , GV.PAxis [GV.AxLabels False, GV.AxTicks False]
                                 ]
                . GV.color [GV.MName "VOC Turnout Component", GV.MmType GV.Nominal
                           , GV.MSort [compSort]
                           , GV.MLegend [GV.LOrient GV.LOBottom]]

      encTurnout o = GV.position GV.X [GV.PName "Turnout"
                                      , GV.PmType GV.Quantitative
                                      , GV.PAxis [GV.AxTitle "Turnout Change (% Eligible VOC)", GV.AxOrient o]
                                      , GV.PScale [GV.SDomain $ GV.DNumbers [negate 26, 26]]]
      encState = GV.facetFlow ([GV.FName "State", GV.FmType GV.Nominal]  ++ stateSort)

      enc = GV.encoding . encComp . encTurnout GV.SBottom -- . encState
      labelSpec x y l = GV.asSpec [GV.encoding . (GV.position GV.X [GV.PNumber $ x * w]) . (GV.position GV.Y [GV.PNumber $ y * h]) $ []
                                  ,GV.mark GV.Text [GV.MText l, GV.MFont "Verdana" ]
                                  ]
      labelSpecs = [labelSpec 0.1 0.05 "Lower Turnout"
                   , labelSpec 0.1 0.95 "Higher Turnout"
                   ]

      barMark = GV.mark GV.Bar [GV.MTooltip GV.TTEncoding]
      barSpec = GV.asSpec [enc [], barMark]
      res = GV.resolve . GV.resolution (GV.RAxis [(GV.ChX, GV.Independent)
                                                  ,(GV.ChY, GV.Independent)]
                                       )
  in FV.configuredVegaLite vc [FV.title title, GV.specification barSpec, encState, GV.columns 3, vlData]

data TurnoutChartColor = ColorIsYear | ColorIsVotingIntegrity | ColorIsType

data TurnoutChartOptions = TurnoutChartOptions { showMean :: Bool
                                               , showZero :: Bool
                                               , colorIs :: TurnoutChartColor
                                               , symmetricX :: Maybe Double
                                               , mXLabel :: Maybe Text
                                               , legend :: Bool
                                               }

turnoutChart :: Text
             -> Text
             -> [Text]
             -> TurnoutChartOptions
             -> FV.ViewConfig
             -> GV.Data
             -> GV.VegaLite
turnoutChart title yName sortedY chartOptions vc vlData =
  let --vlData = MapRow.toVLData M.toList [] rows --[GV.Parse [("Year", GV.FoDate "%Y")]] rows
      encY = GV.position GV.Y [GV.PName yName, GV.PmType GV.Nominal, GV.PSort [GV.CustomSort $ GV.Strings sortedY]]
      xLabel = fromMaybe "Turnout Gap (%)" $ mXLabel chartOptions
      xScale = case symmetricX chartOptions of
        Nothing -> []
        Just x -> [GV.PScale [GV.SDomain $ GV.DNumbers [negate x, x]]]
      encX = GV.position GV.X $ [GV.PName "mid", GV.PmType GV.Quantitative, GV.PAxis [GV.AxTitle xLabel]] ++ xScale
      leg = if legend chartOptions then [] else [GV.MLegend []]
      encColor = case colorIs chartOptions of
                   ColorIsVotingIntegrity -> GV.color $ [GV.MName "ratingstate", GV.MmType GV.Quantitative, GV.MTitle "Election Integrity Rating"] ++ leg
                   ColorIsYear -> GV.color $ [GV.MName "Year", GV.MmType GV.Nominal] ++ leg
                   ColorIsType -> GV.color $ [GV.MName "Type", GV.MmType GV.Nominal] ++ leg
      encXLo = GV.position GV.XError [GV.PName "lo"]
      encXHi = GV.position GV.XError2 [GV.PName "hi"]
      encL = GV.encoding . encX . encY  . encColor
      encB = GV.encoding . encX . encXLo . encY . encXHi . encColor
      markBar = GV.mark GV.ErrorBar [GV.MTooltip GV.TTData, GV.MTicks []]
      markLine = GV.mark GV.Line []
      markPoint = GV.mark GV.Point [GV.MTooltip GV.TTData]
      specBar = GV.asSpec [encB [], markBar]
      specLine = GV.asSpec [encL [], markLine]
      specPoint = GV.asSpec [encL [], markPoint]
      encXZero = GV.position GV.X [GV.PDatum (GV.Number 0)]
      encXMean = GV.position GV.X [GV.PName "mid", GV.PAggregate GV.Mean]
      specZero = GV.asSpec [(GV.encoding . encXZero) [], GV.mark GV.Rule [GV.MColor "skyblue"]]
      specMean = GV.asSpec [(GV.encoding . encXMean) [], GV.mark GV.Rule [GV.MColor "orange"]]
      layers = GV.layer $ [specBar, specPoint]
               ++ (if showMean chartOptions then [specMean] else [])
               ++ (if showZero chartOptions then [specZero] else [])
      spec = GV.asSpec [layers]
  in FV.configuredVegaLite vc [FV.title title, layers , vlData]


-- 2 x 2
turnoutGapScatter ::  --(Functor f, Foldable f)
                  Text
                  -> FV.ViewConfig
                  -> GV.Data
--                  -> f (MapRow.MapRow GV.DataValue)
                  -> GV.VegaLite
turnoutGapScatter title vc@(FV.ViewConfig w h _) vlData =
  let --vlData = MapRow.toVLData M.toList [] vlData -- [GV.Parse [("Year", GV.FoDate "%Y")]] rows
      foldMids = GV.pivot "Year" "mid" [GV.PiGroupBy ["State","ratingstate"]]
      gapScale = GV.PScale [GV.SDomain $ GV.DNumbers [-0.11, 0.12]]
      encY = GV.position GV.Y [GV.PName "2016", GV.PmType GV.Quantitative]
      encX = GV.position GV.X [GV.PName "2012", GV.PmType GV.Quantitative]
      encColor = GV.color [GV.MName "ratingstate", GV.MmType GV.Quantitative, GV.MTitle "Election Integrity Rating"]
      label = GV.text [GV.TName "State", GV.TmType GV.Nominal]
      enc = GV.encoding . encX . encY . encColor

--      enc45_X = GV.position GV.X [GV.PName "2016", GV.PmType GV.Quantitative, gapScale, GV.PAxis [GV.AxNoTitle]]
--      mark45 = GV.mark GV.Line
--      spec45 = GV.asSpec [(GV.encoding . enc45_X . encY) [], mark45 []]
      specXaxis = GV.asSpec [GV.encoding . (GV.position GV.Y [GV.PDatum $ GV.Number 0]) $ [],  GV.mark GV.Rule []]
      specYaxis = GV.asSpec [GV.encoding . (GV.position GV.X [GV.PDatum $ GV.Number 0]) $ [],  GV.mark GV.Rule []]
      stateLabelSpec = GV.asSpec [(enc . label) [], GV.mark GV.Text [GV.MTooltip GV.TTData]]
      labelSpec x y l = GV.asSpec [GV.encoding . (GV.position GV.X [GV.PNumber $ x * w]) . (GV.position GV.Y [GV.PNumber $ y * h]) $ []
                              ,GV.mark GV.Text [GV.MText l, GV.MFont "Verdana" ]
                              ]
      labelSpecs = [labelSpec 0.1 0.05 "Bad -> Good"
                   , labelSpec 0.1 0.95 "Both Bad"
                   , labelSpec 0.9 0.95 "Good -> Bad"
                   , labelSpec 0.9 0.05 "Both Good"
                   ]
      mark = GV.mark GV.Point [GV.MTooltip GV.TTData]
      transform = GV.transform . foldMids
      specPts = GV.asSpec [enc [], mark]
  in FV.configuredVegaLiteSchema
     (GV.vlSchema 5 (Just 1) Nothing Nothing)
     vc
     [FV.title title, GV.layer ([{-specPts ,-} stateLabelSpec, specXaxis, specYaxis]), transform [], vlData]



--
districtSpecificTurnoutModel :: (K.KnitEffects r, BR.CacheEffects r)
                          => Bool -- clear cache
                          -> Bool -- include state/race interaction term?
                          -> SSTData
                          -> [Int]
                          -> K.ActionWithCacheTime r BRE.CCESAndPUMS
                          -> K.Sem r (K.ActionWithCacheTime r (Map Text [Double]
                                                              , Map Text [Double]
                                                              , Map Text [Double]
                                                              , Map Text [Double]
                                                              , Map Text [Double]
                                                              )
                                     )
districtSpecificTurnoutModel clearCaches withSDRace dataSource years dataAllYears_C =
  K.wrapPrefix "districtSpecificTurnoutModel" $ do
  let modelDir = "br-2021-A/stan/" <> nameSSTData dataSource
      jsonDataName = "districtXrace_" <> nameSSTData dataSource <> "_" <> (T.intercalate "_" $ fmap show years)

      cpsVGroupBuilder :: [Text] -> [Text] -> SB.StanGroupBuilderM (F.Record BRE.CPSVByCDR) ()
      cpsVGroupBuilder districts states = do
        SB.addGroup "CD" $ SB.makeIndexFromFoldable show districtKey districts
        SB.addGroup "State" $ SB.makeIndexFromFoldable show (F.rgetField @BR.StateAbbreviation) states
        SB.addGroup "Race" $ SB.makeIndexFromEnum (DT.race4FromRace5 . race5FromCPS)
        SB.addGroup "WNH" $ SB.makeIndexFromEnum wnh
        SB.addGroup "Sex" $ SB.makeIndexFromEnum (F.rgetField @DT.SexC)
        SB.addGroup "Education" $ SB.makeIndexFromEnum (F.rgetField @DT.CollegeGradC)
        SB.addGroup "WhiteNonGrad" $ SB.makeIndexFromEnum wnhNonGrad
        SB.addGroup "Age" $ SB.makeIndexFromEnum (F.rgetField @DT.SimpleAgeC)

      ccesGroupBuilder :: [Text] -> [Text] -> SB.StanGroupBuilderM (F.Record BRE.CCESByCDR) ()
      ccesGroupBuilder districts states = do
        SB.addGroup "CD" $ SB.makeIndexFromFoldable show districtKey districts
        SB.addGroup "State" $ SB.makeIndexFromFoldable show (F.rgetField @BR.StateAbbreviation) states
        SB.addGroup "Race" $ SB.makeIndexFromEnum (DT.race4FromRace5 . F.rgetField @DT.Race5C)
        SB.addGroup "WNH" $ SB.makeIndexFromEnum wnhCCES
        SB.addGroup "Sex" $ SB.makeIndexFromEnum (F.rgetField @DT.SexC)
        SB.addGroup "Education" $ SB.makeIndexFromEnum (F.rgetField @DT.CollegeGradC)
        SB.addGroup "WhiteNonGrad" $ SB.makeIndexFromEnum wnhNonGradCCES
        SB.addGroup "Age" $ SB.makeIndexFromEnum (F.rgetField @DT.SimpleAgeC)

      pumsPSGroupRowMap :: SB.GroupRowMap (F.Record BRE.PUMSByCDR)
      pumsPSGroupRowMap = SB.addRowMap "CD" districtKey
        $ SB.addRowMap "State" (F.rgetField @BR.StateAbbreviation)
        $ SB.addRowMap "Sex" (F.rgetField @DT.SexC)
        $ SB.addRowMap "WNH"  wnh
        $ SB.addRowMap "Race" (DT.race4FromRace5 . race5FromPUMS) --(F.rgetField @DT.RaceAlone4C)
        $ SB.addRowMap "Age" (F.rgetField @DT.SimpleAgeC)
        $ SB.addRowMap "Education" (F.rgetField @DT.CollegeGradC)
        $ SB.addRowMap "WhiteNonGrad" wnhNonGrad
        $ SB.emptyGroupRowMap

      dataAndCodeBuilder :: Typeable modelRow
                         => (modelRow -> Int)
                         -> (modelRow -> Int)
                         -> Bool
                         -> MRP.BuilderM modelRow BRE.CCESAndPUMS ()
      dataAndCodeBuilder totalF succF withSDRace = do
        -- data & model
        cdDataRT <- SB.addIndexedDataSet "CD" (SB.ToFoldable BRE.districtRows) districtKey
        vTotal <- SB.addCountData "T" totalF
        vSucc <- SB.addCountData "S" succF
        let normal x = SB.normal Nothing $ SB.scalar $ show x
            binaryPrior = normal 2
            sigmaPrior = normal 2
            fePrior = normal 2
            sumToZeroPrior = normal 0.01
        alphaE <- SB.intercept "alpha" (normal 2)
        (feCDE, xBetaE, betaE) <- MRP.addFixedEffects @(F.Record BRE.DistrictDemDataR)
                                  True
                                  fePrior
                                  cdDataRT
                                  (MRP.FixedEffects 1 densityPredictor)
        gSexE <- MRP.addMRGroup binaryPrior sigmaPrior SB.STZNone "Sex"
        gRaceE <- MRP.addMRGroup binaryPrior sigmaPrior SB.STZNone "Race"
        gAgeE <- MRP.addMRGroup binaryPrior sigmaPrior SB.STZNone "Age"
        gEduE <- MRP.addMRGroup binaryPrior sigmaPrior SB.STZNone "Education"
        gWNGE <- MRP.addMRGroup binaryPrior sigmaPrior SB.STZNone "WhiteNonGrad"
        gStateE <- MRP.addMRGroup binaryPrior sigmaPrior SB.STZNone "State"
        gCDE <- MRP.addMRGroup binaryPrior sigmaPrior SB.STZNone "CD"
        let dist = SB.binomialLogitDist vSucc vTotal
        (gWNHStateV, gWNHState) <- MRP.addNestedMRGroup sigmaPrior SB.STZNone "WNH" "State"
        (logitPE_sample, logitPE) <- case withSDRace of
          True -> do
            (gWNHCDV, gWNHCD) <- MRP.addNestedMRGroup sigmaPrior SB.STZNone "WNH" "CD"
            return $ (SB.multiOp "+" $ alphaE :| [feCDE, gSexE, gRaceE, gAgeE, gEduE, gWNGE, gStateE, gCDE, gWNHStateV, gWNHCDV]
                     , SB.multiOp "+" $ alphaE :| [feCDE, gSexE, gRaceE, gAgeE, gEduE, gWNGE, gStateE, gCDE, gWNHState, gWNHCD])
          False ->
            return $ (SB.multiOp "+" $ alphaE :| [feCDE, gSexE, gRaceE, gAgeE, gEduE, gWNGE, gStateE, gCDE]
                     , SB.multiOp "+" $ alphaE :| [feCDE, gSexE, gRaceE, gAgeE, gEduE, gWNGE, gStateE, gCDE])

        let logitPE_NI = SB.multiOp "+" $ alphaE :| [feCDE, gSexE, gRaceE, gAgeE, gEduE, gWNGE, gStateE, gCDE]
            logitPE_SI = SB.multiOp "+" $ alphaE :| [feCDE, gSexE, gRaceE, gAgeE, gEduE, gWNGE, gStateE, gWNHState, gCDE]
        SB.sampleDistV dist logitPE_sample

        -- generated quantities
--        SB.generatePosteriorPrediction (SB.StanVar "SPred" $ SB.StanArray [SB.NamedDim "N"] SB.StanInt) dist logitPE
        SB.generateLogLikelihood dist logitPE

        acsData_W <- SB.addUnIndexedDataSet "ACS_WNH" (SB.ToFoldable $ F.filterFrame wnh . BRE.pumsRows)
        SB.addDataSetIndexes acsData_W pumsPSGroupRowMap

        acsData_NW <- SB.addUnIndexedDataSet "ACS_NWNH" (SB.ToFoldable $ F.filterFrame (not . wnh) . BRE.pumsRows)
        SB.addDataSetIndexes acsData_NW pumsPSGroupRowMap

        let postStratByState nameHead modelExp dataSet =
              MRP.addPostStratification
                dist
                modelExp
                (Just nameHead)
                dataSet
                pumsPSGroupRowMap
                (S.fromList ["CD", "Sex", "Race", "WNH", "Age", "Education", "WhiteNonGrad", "State"])
                (realToFrac . F.rgetField @PUMS.Citizens)
                MRP.PSShare
                (Just $ SB.GroupTypeTag @Text "CD")

        (SB.StanVar whiteWI psType) <- postStratByState "WI" logitPE acsData_W
        (SB.StanVar nonWhiteWI _) <- postStratByState "WI" logitPE acsData_NW
        (SB.StanVar whiteNI _) <- postStratByState "NI" logitPE_NI acsData_W
        (SB.StanVar nonWhiteNI _) <- postStratByState "NI" logitPE_NI acsData_NW
        (SB.StanVar whiteSI _) <- postStratByState "SI" logitPE_SI acsData_W
        (SB.StanVar nonWhiteSI _) <- postStratByState "SI" logitPE_SI acsData_NW

        _ <- SB.inBlock SB.SBGeneratedQuantities $ do
          SB.stanDeclareRHS "fullGap" psType "" $ SB.name nonWhiteWI `SB.minus` SB.name whiteWI
          SB.stanDeclareRHS "stateGap" psType "" $ SB.name nonWhiteSI `SB.minus` SB.name whiteSI
          SB.stanDeclareRHS "demographicGap" psType "" $ SB.name nonWhiteNI `SB.minus` SB.name whiteNI
          SB.stanDeclareRHS "districtSpecificGap" psType "" $ SB.name "fullGap" `SB.minus` SB.name "demographicGap"
          SB.stanDeclareRHS "districtOnlyGap" psType "" $ SB.name "fullGap" `SB.minus` SB.name "stateGap"
        return ()

      extractTestResults :: K.KnitEffects r
                         => SC.ResultAction r d SB.DataSetGroupIntMaps () (Map Text [Double]
                                                                          , Map Text [Double]
                                                                          , Map Text [Double]
                                                                          , Map Text [Double]
                                                                          , Map Text [Double]
                                                                          )
      extractTestResults = SC.UseSummary f where
        f summary _ aAndEb_C = do
          let eb_C = fmap snd aAndEb_C
          eb <- K.ignoreCacheTime eb_C
          K.knitEither $ do
            groupIndexes <- eb
            psIndexIM <- SB.getGroupIndex
                         (SB.RowTypeTag @(F.Record BRE.PUMSByCDR) "ACS_WNH")
                         (SB.GroupTypeTag @Text "CD")
                         groupIndexes
{-            stateIndexIM <- SB.getGroupIndex
                            (SB.ModeledRowTag @(F.Record BRE.CPSVByCDR))
                            (SB.GroupTypeTag @Text "State")
                            groupIndexes -}
            let parseAndIndexPctsWith idx f vn = do
                  v <- SP.getVector . fmap CS.percents <$> SP.parse1D vn (CS.paramStats summary)
                  indexStanResults idx $ Vector.map f v

            fullGap <- parseAndIndexPctsWith psIndexIM id "fullGap"
            stateGap <- parseAndIndexPctsWith psIndexIM id "stateGap"
            demographicGap <- parseAndIndexPctsWith psIndexIM id "demographicGap"
            districtSpecificGap <- parseAndIndexPctsWith psIndexIM id "districtSpecificGap"
            districtOnlyGap <- parseAndIndexPctsWith psIndexIM id "districtOnlyGap"
            return (fullGap, stateGap, demographicGap, districtSpecificGap, districtOnlyGap)

  K.logLE K.Info "Building json data wrangler and model code..."
  let dataWranglerAndCode data_C years withSDRace = do
        dat <- K.ignoreCacheTime data_C
        let (districts, states) = FL.fold
                                  ((,)
                                   <$> (FL.premap districtKey FL.list)
                                   <*> (FL.premap (F.rgetField @BR.StateAbbreviation) FL.list)
                                  )
                                  $ BRE.districtRows dat
        K.knitEither
          $ case dataSource of
              SSTD_CPS -> do
                let codeBuilder = dataAndCodeBuilder
                                  (round . F.rgetField @BRCF.WeightedCount)
                                  (round . F.rgetField @BRCF.WeightedSuccesses)
                    groups = cpsVGroupBuilder districts states
                    dataRows = BRE.cpsVRows
                MRP.buildDataWranglerAndCode groups () (codeBuilder withSDRace) dat (SB.ToFoldable dataRows)
              SSTD_CCES -> do
                let codeBuilder =  dataAndCodeBuilder
                                   (F.rgetField @BRE.Surveyed)
                                   (F.rgetField @BRE.TVotes)
                    groups = ccesGroupBuilder districts states
                    dataRows = BRE.ccesRows
                MRP.buildDataWranglerAndCode groups () (codeBuilder withSDRace) dat (SB.ToFoldable dataRows)

      data_C = fmap (BRE.ccesAndPUMSForYears years) dataAllYears_C
  (dw, stanCode) <- dataWranglerAndCode data_C years withSDRace
  MRP.runMRPModel
    clearCaches
    (Just modelDir)
    ("districtXrace" <> if withSDRace then "" else "_NI")
    jsonDataName
    dw
    stanCode
    "S"
    extractTestResults
    data_C
    (Just 1000)
    (Just 0.99)
    (Just 15)



stateSpecificTurnoutModel :: (K.KnitEffects r, BR.CacheEffects r)
                          => Bool -- clear cache
                          -> Bool -- include state/race interaction term?
                          -> SSTData
                          -> [Int]
                          -> K.ActionWithCacheTime r BRE.CCESAndPUMS
                          -> K.Sem r (K.ActionWithCacheTime r ((Map Text [Double]
                                                              , Map Text [Double]
                                                              , Map Text [Double]
                                                              , Map Text [Double]
                                                              , Map Text [Double]
                                                              , Map Text [Double]
                                                              , Map Text [Double])
                                                              , [Double]
                                                              , [Double]
                                                              , Map Text (Map Text [Double])
                                                              )
                                     )
stateSpecificTurnoutModel clearCaches withStateRace dataSource years dataAllYears_C =  K.wrapPrefix "stateSpecificTurnoutModel" $ do
  let modelDir = "br-2021-A/stan/" <> nameSSTData dataSource
      jsonDataName = "stateXrace_" <> nameSSTData dataSource <> "_" <> (T.intercalate "_" $ fmap show years)

      cpsVGroupBuilder :: [Text] -> [Text] -> SB.StanGroupBuilderM (F.Record BRE.CPSVByCDR) ()
      cpsVGroupBuilder districts states = do
        SB.addGroup "CD" $ SB.makeIndexFromFoldable show districtKey districts
        SB.addGroup "State" $ SB.makeIndexFromFoldable show (F.rgetField @BR.StateAbbreviation) states
        SB.addGroup "Race" $ SB.makeIndexFromEnum (DT.race4FromRace5 . race5FromCPS)
        SB.addGroup "WNH" $ SB.makeIndexFromEnum wnh
        SB.addGroup "Sex" $ SB.makeIndexFromEnum (F.rgetField @DT.SexC)
        SB.addGroup "Education" $ SB.makeIndexFromEnum (F.rgetField @DT.CollegeGradC)
        SB.addGroup "WhiteNonGrad" $ SB.makeIndexFromEnum wnhNonGrad
        SB.addGroup "Age" $ SB.makeIndexFromEnum (F.rgetField @DT.SimpleAgeC)

      ccesGroupBuilder :: [Text] -> [Text] -> SB.StanGroupBuilderM (F.Record BRE.CCESByCDR) ()
      ccesGroupBuilder districts states = do
        SB.addGroup "CD" $ SB.makeIndexFromFoldable show districtKey districts
        SB.addGroup "State" $ SB.makeIndexFromFoldable show (F.rgetField @BR.StateAbbreviation) states
        SB.addGroup "Race" $ SB.makeIndexFromEnum (DT.race4FromRace5 . F.rgetField @DT.Race5C)
        SB.addGroup "WNH" $ SB.makeIndexFromEnum wnhCCES
        SB.addGroup "Sex" $ SB.makeIndexFromEnum (F.rgetField @DT.SexC)
        SB.addGroup "Education" $ SB.makeIndexFromEnum (F.rgetField @DT.CollegeGradC)
        SB.addGroup "WhiteNonGrad" $ SB.makeIndexFromEnum wnhNonGradCCES
        SB.addGroup "Age" $ SB.makeIndexFromEnum (F.rgetField @DT.SimpleAgeC)

      pumsPSGroupRowMap :: SB.GroupRowMap (F.Record BRE.PUMSByCDR)
      pumsPSGroupRowMap = SB.addRowMap "CD" districtKey
        $ SB.addRowMap "State" (F.rgetField @BR.StateAbbreviation)
        $ SB.addRowMap "Sex" (F.rgetField @DT.SexC)
        $ SB.addRowMap "WNH"  wnh
        $ SB.addRowMap "Race" (DT.race4FromRace5 . race5FromPUMS) --(F.rgetField @DT.RaceAlone4C)
        $ SB.addRowMap "Age" (F.rgetField @DT.SimpleAgeC)
        $ SB.addRowMap "Education" (F.rgetField @DT.CollegeGradC)
        $ SB.addRowMap "WhiteNonGrad" wnhNonGrad
        $ SB.emptyGroupRowMap

      dataAndCodeBuilder :: Typeable modelRow
                         => (modelRow -> Int)
                         -> (modelRow -> Int)
                         -> Bool
                         -> MRP.BuilderM modelRow BRE.CCESAndPUMS ()
      dataAndCodeBuilder totalF succF withStateRace = do
        -- data & model
        cdDataRT <- SB.addIndexedDataSet "CD" (SB.ToFoldable BRE.districtRows) districtKey
        vTotal <- SB.addCountData "T" totalF
        vSucc <- SB.addCountData "S" succF
        let normal x = SB.normal Nothing $ SB.scalar $ show x
            binaryPrior = normal 2
            sigmaPrior = normal 2
            fePrior = normal 2
            sumToZeroPrior = normal 0.01
        alphaE <- SB.intercept "alpha" (normal 2)
        (feCDE, xBetaE, betaE) <- MRP.addFixedEffects @(F.Record BRE.DistrictDemDataR)
                                  True
                                  fePrior
                                  cdDataRT
                                  (MRP.FixedEffects 1 densityPredictor)
        gSexE <- MRP.addMRGroup binaryPrior sigmaPrior SB.STZNone "Sex"
        gRaceE <- MRP.addMRGroup binaryPrior sigmaPrior SB.STZNone "Race"
        gAgeE <- MRP.addMRGroup binaryPrior sigmaPrior SB.STZNone "Age"
        gEduE <- MRP.addMRGroup binaryPrior sigmaPrior SB.STZNone "Education"
        gWNGE <- MRP.addMRGroup binaryPrior sigmaPrior SB.STZNone "WhiteNonGrad"
        gStateE <- MRP.addMRGroup binaryPrior sigmaPrior SB.STZNone "State"
        let dist = SB.binomialLogitDist vSucc vTotal
        (logitPE_sample, logitPE) <- case withStateRace of
          True -> do
            (gWNHStateV, gWNHState) <- MRP.addNestedMRGroup sigmaPrior SB.STZNone "WNH" "State"
            return $ (SB.multiOp "+" $ alphaE :| [feCDE, gSexE, gRaceE, gAgeE, gEduE, gWNGE, gStateE, gWNHStateV]
                     , SB.multiOp "+" $ alphaE :| [feCDE, gSexE, gRaceE, gAgeE, gEduE, gWNGE, gStateE, gWNHState])
          False ->
            return $ (SB.multiOp "+" $ alphaE :| [feCDE, gSexE, gRaceE, gAgeE, gEduE, gWNGE, gStateE]
                     , SB.multiOp "+" $ alphaE :| [feCDE, gSexE, gRaceE, gAgeE, gEduE, gWNGE, gStateE])

        let logitPE' = SB.multiOp "+" $ alphaE :| [feCDE, gSexE, gRaceE, gAgeE, gEduE, gWNGE, gStateE]
        SB.sampleDistV dist logitPE_sample

        -- generated quantities
--        SB.generatePosteriorPrediction (SB.StanVar "SPred" $ SB.StanArray [SB.NamedDim "N"] SB.StanInt) dist logitPE
        SB.generateLogLikelihood dist logitPE

        acsData_W <- SB.addUnIndexedDataSet "ACS_WNH" (SB.ToFoldable $ F.filterFrame wnh . BRE.pumsRows)
        SB.addDataSetIndexes acsData_W pumsPSGroupRowMap

        acsData_NW <- SB.addUnIndexedDataSet "ACS_NWNH" (SB.ToFoldable $ F.filterFrame (not . wnh) . BRE.pumsRows)
        SB.addDataSetIndexes acsData_NW pumsPSGroupRowMap

        let postStratByState nameHead modelExp dataSet =
              MRP.addPostStratification
                dist
                modelExp
                (Just nameHead)
                dataSet
                pumsPSGroupRowMap
                (S.fromList ["CD", "Sex", "Race", "WNH", "Age", "Education", "WhiteNonGrad", "State"])
                (realToFrac . F.rgetField @PUMS.Citizens)
                MRP.PSShare
                (Just $ SB.GroupTypeTag @Text "State")

        (SB.StanVar whiteWI psType) <- postStratByState "WI" logitPE acsData_W
        (SB.StanVar nonWhiteWI _) <- postStratByState "WI" logitPE acsData_NW
        (SB.StanVar whiteNI _) <- postStratByState "NI" logitPE' acsData_W
        (SB.StanVar nonWhiteNI _) <- postStratByState "NI" logitPE' acsData_NW

        _ <- SB.inBlock SB.SBGeneratedQuantities $ do
          SB.stanDeclareRHS "rtDiffWI" psType "" $ SB.name nonWhiteWI `SB.minus` SB.name whiteWI
          SB.stanDeclareRHS "rtDiffNI" psType "" $ SB.name nonWhiteNI `SB.minus` SB.name whiteNI
          SB.stanDeclareRHS "rtDiffI" psType "" $ SB.name "rtDiffWI" `SB.minus` SB.name "rtDiffNI"
          SB.stanDeclareRHS "dNWNH" psType "" $ SB.name nonWhiteWI `SB.minus` SB.name nonWhiteNI
          SB.stanDeclareRHS "dWNH" psType "" $ SB.name whiteWI `SB.minus` SB.name whiteNI
          let rtDiffArg = SB.name "rtDiffI" :| []
          SB.stanDeclareRHS "varGap" SB.StanReal "<lower=0>" $ SB.function "variance" rtDiffArg
          SB.stanDeclareRHS "rangeGap" SB.StanReal "<lower=0>" $ SB.function "max" rtDiffArg `SB.minus` SB.function "min" rtDiffArg
          stanPairwiseDifferenceMatrix
          SB.stanDeclareRHS "gapPairwiseDiffs" (SB.StanMatrix (SB.NamedDim "State", SB.NamedDim "State")) ""
            $ SB.function "pairwiseDifferenceMatrix" (rtDiffArg)
        return ()

      extractTestResults :: K.KnitEffects r
                         => SC.ResultAction r d SB.DataSetGroupIntMaps () ((Map Text [Double]
                                                                          , Map Text [Double]
                                                                          , Map Text [Double]
                                                                          , Map Text [Double]
                                                                          , Map Text [Double]
                                                                          , Map Text [Double]
                                                                          , Map Text [Double])
                                                                          , [Double]
                                                                          , [Double]
                                                                          , Map Text (Map Text [Double])
                                                                          )
      extractTestResults = SC.UseSummary f where
        f summary _ aAndEb_C = do
          let eb_C = fmap snd aAndEb_C
          eb <- K.ignoreCacheTime eb_C
          K.knitEither $ do
            groupIndexes <- eb
            psIndexIM <- SB.getGroupIndex
                         (SB.RowTypeTag @(F.Record BRE.PUMSByCDR) "ACS_WNH")
                         (SB.GroupTypeTag @Text "State")
                         groupIndexes
            let parseAndIndexPctsWith f vn = do
                  v <- SP.getVector . fmap CS.percents <$> SP.parse1D vn (CS.paramStats summary)
                  indexStanResults psIndexIM $ Vector.map f v

            rtDiffWI <- parseAndIndexPctsWith id "rtDiffWI"
            rtDiffNI <- parseAndIndexPctsWith id "rtDiffNI"
            rtDiffI <- parseAndIndexPctsWith id "rtDiffI"
            rtNWNH_WI <- parseAndIndexPctsWith id "WI_ACS_NWNH_State"
            rtWNH_WI <- parseAndIndexPctsWith id "WI_ACS_WNH_State"
            dNWNH <- parseAndIndexPctsWith id "dNWNH"
            dWNH <- parseAndIndexPctsWith id "dWNH"
            gapVariance <- CS.percents . SP.getScalar <$> SP.parseScalar "varGap"  (CS.paramStats summary)
            gapRange <- CS.percents . SP.getScalar <$> SP.parseScalar "rangeGap"  (CS.paramStats summary)
            gapDiffsParsed <- SP.parse2D "gapPairwiseDiffs" (CS.paramStats summary)


            return ((rtDiffWI, rtDiffNI, rtDiffI, rtNWNH_WI, rtWNH_WI, dNWNH, dWNH), gapVariance, gapRange)

  K.logLE K.Info "Building json data wrangler and model code..."
  let dataWranglerAndCode data_C years withStateRace = do
        dat <- K.ignoreCacheTime data_C
        let (districts, states) = FL.fold
                                  ((,)
                                   <$> (FL.premap districtKey FL.list)
                                   <*> (FL.premap (F.rgetField @BR.StateAbbreviation) FL.list)
                                  )
                                  $ BRE.districtRows dat
        K.knitEither
          $ case dataSource of
              SSTD_CPS -> do
                let codeBuilder = dataAndCodeBuilder
                                  (round . F.rgetField @BRCF.WeightedCount)
                                  (round . F.rgetField @BRCF.WeightedSuccesses)
                    groups = cpsVGroupBuilder districts states
                    dataRows = BRE.cpsVRows
                MRP.buildDataWranglerAndCode groups () (codeBuilder withStateRace) dat (SB.ToFoldable dataRows)
              SSTD_CCES -> do
                let codeBuilder =  dataAndCodeBuilder
                                   (F.rgetField @BRE.Surveyed)
                                   (F.rgetField @BRE.TVotes)
                    groups = ccesGroupBuilder districts states
                    dataRows = BRE.ccesRows
                MRP.buildDataWranglerAndCode groups () (codeBuilder withStateRace) dat (SB.ToFoldable dataRows)

      data_C = fmap (BRE.ccesAndPUMSForYears years) dataAllYears_C
  (dw, stanCode) <- dataWranglerAndCode data_C years withStateRace
  MRP.runMRPModel
    clearCaches
    (Just modelDir)
    ("stateXrace" <> if withStateRace then "" else "_NI")
    jsonDataName
    dw
    stanCode
    "S"
    extractTestResults
    data_C
    (Just 1000)
    (Just 0.99)
    (Just 15)

--

stanPairwiseDifferenceMatrix :: SB.StanBuilderM env d r ()
stanPairwiseDifferenceMatrix = SB.addFunctionsOnce "differenceMatrix" $ do
  SB.declareStanFunction "matrix pairwiseDifferenceMatrix(vector x)" $ do
    SB.addStanLine "int N = num_elements(x)"
    SB.addStanLine "matrix[N,N] diffs"
    SB.stanForLoop "k" Nothing "N" $ const $ do
      SB.stanForLoop "l" Nothing "k" $ const $ do
        SB.addStanLine "diffs[k, l] = x[k] - x[l]"
        SB.addStanLine "diffs[l, k] = x[l] - x[k]"
    SB.addStanLine "return diffs"

cpsModelTest :: (K.KnitOne r, BR.CacheEffects r) => Bool -> BR.PostPaths BR.Abs -> BR.PostInfo -> K.ActionWithCacheTime r BRE.CCESAndPUMS -> K.Sem r ()
cpsModelTest clearCaches postPaths postInfo dataAllYears_C = K.wrapPrefix "cpsStateRace" $ do
  let year = 2018
      data_C = fmap (BRE.ccesAndPUMSForYear year) dataAllYears_C
      cpsVGroupBuilder :: [Text] -> [Text] -> SB.StanGroupBuilderM (F.Record BRE.CPSVByCDR) ()
      cpsVGroupBuilder districts states = do
        let age = F.rgetField @DT.SimpleAgeC
            sex = F.rgetField @DT.SexC
            edu = F.rgetField @DT.CollegeGradC
            race = DT.race4FromRace5 . race5FromCPS
            ageSex r = (age r, sex r)
            ageEdu r = (age r, edu r)
            ageRace r = (age r, race r)
            sexEdu r = (sex r, edu r)
            sexRace r = (sex r, race r)
            eduRace r = (edu r, race r)
        SB.addGroup "CD" $ SB.makeIndexFromFoldable show districtKey districts
        SB.addGroup "Age" $ SB.makeIndexFromEnum age
        SB.addGroup "Sex" $ SB.makeIndexFromEnum sex
        SB.addGroup "Education" $ SB.makeIndexFromEnum edu
        SB.addGroup "Race" $ SB.makeIndexFromEnum (DT.race4FromRace5 . race5FromCPS)
        SB.addGroup "AgeSex" $ SB.makeIndexFromFoldable show ageSex BRK.elements
        SB.addGroup "AgeEdu" $ SB.makeIndexFromFoldable show ageEdu BRK.elements
        SB.addGroup "AgeRace" $ SB.makeIndexFromFoldable show ageRace BRK.elements
        SB.addGroup "SexEdu" $ SB.makeIndexFromFoldable show sexEdu BRK.elements
        SB.addGroup "SexRace" $ SB.makeIndexFromFoldable show sexRace BRK.elements
        SB.addGroup "EduRace" $ SB.makeIndexFromFoldable show eduRace BRK.elements


--        SB.addGroup "State" $ SB.SupplementalIndex states --SB.makeIndexFromFoldable show (F.rgetField @BR.StateAbbreviation) states
--        SB.addGroup "Ethnicity" $ SB.makeIndexFromEnum (F.rgetField @DT.HispC)
--        SB.addGroup "WNH" $ SB.makeIndexFromEnum wnh

      pumsPSGroupRowMap :: SB.GroupRowMap (F.Record BRE.PUMSByCDR)
      pumsPSGroupRowMap = SB.addRowMap "CD" districtKey
        $ SB.addRowMap "State" (F.rgetField @BR.StateAbbreviation)
        $ SB.addRowMap "Race" (DT.race4FromRace5 . race5FromPUMS)
        $ SB.addRowMap "Sex" (F.rgetField @DT.SexC)
--        $ SB.addRowMap "Education" (F.rgetField @DT.CollegeGradC)
--        $ SB.addRowMap "Age" (F.rgetField @DT.SimpleAgeC)
        $ SB.emptyGroupRowMap

      dataAndCodeBuilder :: Typeable modelRow
                         => (modelRow -> Int)
                         -> (modelRow -> Int)
                         -> MRP.BuilderM modelRow BRE.CCESAndPUMS ()
      dataAndCodeBuilder totalF succF = do
        cdDataRT <- SB.addIndexedDataSet "CD" (SB.ToFoldable BRE.districtRows) districtKey
        vTotal <- SB.addCountData "T" totalF
        vSucc <- SB.addCountData "S"  succF
        let normal x = SB.normal Nothing $ SB.scalar $ show x
            binaryPrior = normal 1
            sigmaPrior = normal 1
            fePrior = normal 1
            stzPrior = normal 0.01
        alphaE <- SB.intercept "alpha" (normal 2)

        (feCDE, xBetaE, betaE) <- MRP.addFixedEffects @(F.Record BRE.DistrictDemDataR)
                                  True
                                  fePrior
                                  cdDataRT
                                  (MRP.FixedEffects 1 densityPredictor)

        gAgeE <- MRP.addMRGroup binaryPrior sigmaPrior SB.STZNone "Age"
        gSexE <- MRP.addMRGroup binaryPrior sigmaPrior SB.STZNone "Sex"
        gEduE <- MRP.addMRGroup binaryPrior sigmaPrior SB.STZNone "Education"
        gRaceE <- MRP.addMRGroup binaryPrior sigmaPrior SB.STZQR "Race"
        gAgeSexE <- MRP.addMRGroup binaryPrior sigmaPrior SB.STZQR "AgeSex"
        gAgeEduE <- MRP.addMRGroup binaryPrior sigmaPrior SB.STZQR "AgeEdu"
        gAgeRaceE <- MRP.addMRGroup binaryPrior sigmaPrior SB.STZQR "AgeRace"
        gSexEduE <- MRP.addMRGroup binaryPrior sigmaPrior SB.STZQR "SexEdu"
        gSexRaceE <- MRP.addMRGroup binaryPrior sigmaPrior SB.STZQR "SexRace"
        gEduRaceE <- MRP.addMRGroup binaryPrior sigmaPrior SB.STZQR "EduRace"

--        gWNHE <- MRP.addMRGroup binaryPrior sigmaPrior SB.STZNone "WNH"
--        gStateE <- MRP.addMRGroup binaryPrior sigmaPrior SB.STZNone "State"
--        (gWNHStateEV, gWNHStateE) <- MRP.addNestedMRGroup sigmaPrior SB.STZNone "WNH" "State"
--        gEthE <- MRP.addMRGroup binaryPrior nonBinaryPrior SB.STZNone "Ethnicity"
--        (gRaceStateEV, gRaceStateE) <- MRP.addNestedMRGroup sigmaPrior SB.STZNone "Race" "State"
        let dist = SB.binomialLogitDist vSucc vTotal
            logitPE = SB.multiOp "+" $ alphaE :| [feCDE, gAgeE, gSexE, gEduE, gRaceE, gAgeSexE, gAgeEduE, gAgeRaceE, gSexEduE, gSexRaceE, gEduRaceE]
        SB.sampleDistV dist logitPE
{-
        acsData <- SB.addUnIndexedDataSet "ACS" (SB.ToFoldable BRE.pumsRows)
        SB.addDataSetIndexes acsData pumsPSGroupRowMap

        MRP.addPostStratification
          dist
          logitPE
          Nothing
          acsData
          pumsPSGroupRowMap
          (S.fromList ["CD", "Sex", "Race"])
          (realToFrac . F.rgetField @PUMS.Citizens)
          MRP.PSShare
          (Just $ SB.GroupTypeTag @Text "State")
-}
--        SB.generateLogLikelihood dist logitPE
--        SB.generatePosteriorPrediction (SB.StanVar "SPred" $ SB.StanArray [SB.NamedDim SB.modeledDataIndexName] SB.StanInt) dist logitPE
        return ()

      extractTestResults :: K.KnitEffects r => SC.ResultAction r d SB.DataSetGroupIntMaps () ()
      extractTestResults = SC.UseSummary f where
        f summary _ aAndEb_C = return ()

  K.logLE K.Info "Building json data wrangler and model code..."
  dat <- K.ignoreCacheTime data_C
  let cpsVBuilder = dataAndCodeBuilder
                    (round . F.rgetField @BRCF.WeightedCount)
                    (round . F.rgetField @BRCF.WeightedSuccesses)
      (districts, states) = FL.fold
                            ((,)
                              <$> (FL.premap districtKey FL.list)
                              <*> (FL.premap (F.rgetField @BR.StateAbbreviation) FL.list)
                            )
                            $ BRE.districtRows dat
      cpsVGroups = cpsVGroupBuilder districts states
  (dw, stanCode) <- K.knitEither
    $ MRP.buildDataWranglerAndCode cpsVGroups () cpsVBuilder dat (SB.ToFoldable BRE.cpsVRows)
  res_C <- MRP.runMRPModel
    False
    (Just "br-2021-A/stan/cpsV")
    ("test")
    ("test" <> show year)
    dw
    stanCode
    "S"
    extractTestResults
    data_C
    (Just 1000)
    (Just 0.99)
    (Just 15)
  return ()

---

stateRaceCPSvsCCES :: (K.KnitMany r, K.KnitOne r, BR.CacheEffects r)
                   => Bool
                   -> BR.PostPaths BR.Abs
                   -> BR.PostInfo
                   -> K.ActionWithCacheTime r BRE.CCESAndPUMS
                   -> K.Sem r ()
stateRaceCPSvsCCES clearCaches postPaths postInfo dataAllYears_C = K.wrapPrefix "stateRaceCPSvsCCES" $ do
  rawCCEST_C <- rawCCESTurnout False dataAllYears_C
  K.ignoreCacheTime rawCCEST_C >>= BR.logFrame
  cps_C <- stateSpecificTurnoutModel clearCaches True SSTD_CPS [2016] dataAllYears_C
  ((_, _, cpsDiffI, _, _, _, _), _, _) <- K.ignoreCacheTime cps_C
  cces_C <- stateSpecificTurnoutModel clearCaches True SSTD_CCES [2016] dataAllYears_C
  ((_, _, ccesDiffI, _, _, _, _), _, _) <- K.ignoreCacheTime cces_C
  let cpsDiffI_h = modelToHeidiFrame "2016" "CPS" cpsDiffI
      ccesDiffI_h = modelToHeidiFrame "2016" "CCES" ccesDiffI
  _ <- K.knitEither (modelHeidiToVLData (cpsDiffI_h <> ccesDiffI_h)) >>=
       K.addHvega Nothing Nothing
       . cpsVsCcesScatter
       ("Turnout Gap: CPS vs CCES (2016)")
       (FV.ViewConfig 600 600 5)
  return ()

-- 2 x 2
cpsVsCcesScatter ::  --(Functor f, Foldable f)
  Text
  -> FV.ViewConfig
  -> GV.Data
  -> GV.VegaLite
cpsVsCcesScatter title vc@(FV.ViewConfig w h _) vlData =
  let --vlData = MapRow.toVLData M.toList [] vlData -- [GV.Parse [("Year", GV.FoDate "%Y")]] rows
      foldMids = GV.pivot "Type" "mid" [GV.PiGroupBy ["State"]]
--      gapScale = GV.PScale [GV.SDomain $ GV.DNumbers [-0.11, 0.12]]
      encY = GV.position GV.Y [GV.PName "CPS", GV.PmType GV.Quantitative]
      encX = GV.position GV.X [GV.PName "CCES", GV.PmType GV.Quantitative]
      label = GV.text [GV.TName "State", GV.TmType GV.Nominal]
      enc = GV.encoding . encX . encY
      specXaxis = GV.asSpec [GV.encoding . (GV.position GV.Y [GV.PDatum $ GV.Number 0]) $ [],  GV.mark GV.Rule []]
      specYaxis = GV.asSpec [GV.encoding . (GV.position GV.X [GV.PDatum $ GV.Number 0]) $ [],  GV.mark GV.Rule []]
      stateLabelSpec = GV.asSpec [(enc . label) [], GV.mark GV.Text [GV.MTooltip GV.TTData]]
      mark = GV.mark GV.Point [GV.MTooltip GV.TTData]
      transform = GV.transform . foldMids
      specPts = GV.asSpec [enc [], mark]
  in FV.configuredVegaLiteSchema
     (GV.vlSchema 5 (Just 1) Nothing Nothing)
     vc
     [FV.title title, GV.layer ([specPts, stateLabelSpec, specXaxis, specYaxis]), transform [], vlData]


--

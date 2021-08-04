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

import qualified Data.Massiv.Array

import qualified BlueRipple.Configuration as BR

import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.ElectionTypes as ET
import qualified BlueRipple.Data.Loaders as BR
import qualified BlueRipple.Utilities.KnitUtils as BR
import qualified BlueRipple.Utilities.Heidi as BR
import qualified BlueRipple.Model.House.ElectionResult as BRE
import qualified BlueRipple.Data.CensusLoaders as BRC
import qualified BlueRipple.Model.StanMRP as MRP

import qualified Control.Foldl as FL
import qualified Data.List as List
import qualified Data.Map.Strict as M
import Data.String.Here (here, i)
import qualified Data.Text as T
import qualified Data.Time.Calendar            as Time
--import qualified Data.Time.Clock               as Time
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
import qualified Frames.Serialize as FS
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
--import qualified Polysemy


import qualified Stan.ModelConfig as SC
import qualified Stan.ModelBuilder as SB
import qualified Stan.ModelBuilder.BuildingBlocks as SB
import qualified Stan.ModelBuilder.SumToZero as SB
import qualified Stan.Parameters as SP
import qualified Stan.Parameters.Massiv as SPM
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
          { K.outerLogPrefix = Just "2021-VA"
          , K.logIf = K.logDiagnostic
          , K.pandocWriterConfig = pandocWriterConfig
          , K.serializeDict = BR.flatSerializeDict
          , K.persistCache = KC.persistStrictByteString (\t -> toString (cacheDir <> "/" <> t))
          }
  resE <- K.knitHtmls knitConfig vaAnalysis

  case resE of
    Right namedDocs ->
      K.writeAllPandocResultsWithInfoAsHtml "" namedDocs
    Left err -> putStrLn $ "Pandoc Error: " ++ show err


postDir = [Path.reldir|br-2021-VA/posts|]
postInputs p = postDir BR.</> p BR.</> [Path.reldir|inputs|]
postLocalDraft p = postDir BR.</> p BR.</> [Path.reldir|draft|]
postOnline p =  [Path.reldir|research/StateLeg|] BR.</> p

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

-- data
data SLDModelData = SLDModelData
  {
    ccesRows :: F.FrameRec BRE.CCESByCDR
  , cpsVRows :: F.FrameRec BRE.CPSVByCDR
  , sldTables :: BRC.LoadedCensusTablesBySLD
  } deriving (Generic)

instance Flat.Flat SLDModelData where
  size (SLDModelData cces cps sld) n = Flat.size (FS.SFrame cces, FS.SFrame cps, sld) n
  encode (SLDModelData cces cps sld) = Flat.encode (FS.SFrame cces, FS.SFrame cps, sld)
  decode = (\(ccesSF, cpsSF, sld) -> SLDModelData (FS.unSFrame ccesSF) (FS.unSFrame cpsSF) sld) <$> Flat.decode

prepSLDModelData :: (K.KnitEffects r, BR.CacheEffects r)
                 => Bool
                 -> K.Sem r (K.ActionWithCacheTime r SLDModelData)
prepSLDModelData clearCaches = do
  ccesAndCPS_C <- BRE.prepCCESAndPums clearCaches
  sld_C <- BRC.censusTablesBySLD
  let rearrangeCached (BRE.CCESAndPUMS cces cps _ _) x = SLDModelData cces cps x
  return $ rearrangeCached <$> ccesAndCPS_C <*> sld_C


vaAnalysis :: forall r. (K.KnitMany r, BR.CacheEffects r) => K.Sem r ()
vaAnalysis = do
  K.logLE K.Info "Data prep..."
  data_C <- prepSLDModelData False
  let va1PostInfo = BR.PostInfo BR.LocalDraft (BR.PubTimes BR.Unpublished Nothing)
  va1Paths <- postPaths "VA1"
  BR.brNewPost va1Paths va1PostInfo "Virginia Lower House"
    $ vaLower False va1Paths va1PostInfo $ K.liftActionWithCacheTime data_C

vaLower :: (K.KnitMany r, K.KnitOne r, BR.CacheEffects r)
        => Bool
        -> BR.PostPaths BR.Abs
        -> BR.PostInfo
        -> K.ActionWithCacheTime r SLDModelData
        -> K.Sem r ()
vaLower clearCaches postPaths postInfo sldDat_C = K.wrapPrefix "vaLower" $ do
  K.logLE K.Info $ "Re-building VA Lower post"
  BR.brAddPostMarkDownFromFile postPaths "_intro"

stateLegModel :: (K.KnitEffects r, BR.CacheEffects r) => Bool -> K.ActionWithCacheTime r SLDModelData -> K.Sem r ()
stateLegModel clearCaches dat_C = K.wrapPrefix "stateLegModel" $ do
  let modelDir = "br-2021-VA/stan/"
      jsonDataName = "stateLeg_ASR"
      cpsVGroupBuilder :: [Text] -> [Text] -> SB.StanGroupBuilderM (F.Record BRE.CPSVByCDR) ()
      cpsVGroupBuilder districts states = do
        SB.addGroup "CD" $ SB.makeIndexFromFoldable show districtKey districts
        SB.addGroup "State" $ SB.makeIndexFromFoldable show (F.rgetField @BR.StateAbbreviation) states
        SB.addGroup "Race" $ SB.makeIndexFromEnum (DT.race4FromRace5 . race5FromCPS)
        SB.addGroup "WNH" $ SB.makeIndexFromEnum wnh
        SB.addGroup "Sex" $ SB.makeIndexFromEnum (F.rgetField @DT.SexC)
        SB.addGroup "Education" $ SB.makeIndexFromEnum (F.rgetField @DT.CollegeGradC)
        SB.addGroup "WhiteNonGrad" $ SB.makeIndexFromEnum wnhNonGrad

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

      dataAndCodeBuilder :: Typeable modelRow => (modelRow -> Int) -> (modelRow -> Int) -> Bool -> MRP.BuilderM modelRow SLDModelData ()
      dataAndCodeBuilder = undefined

  return ()


race5FromCPS :: F.Record BRE.CPSVByCDR -> DT.Race5
race5FromCPS r =
  let race4A = F.rgetField @DT.RaceAlone4C r
      hisp = F.rgetField @DT.HispC r
  in DT.race5FromRaceAlone4AndHisp True race4A hisp

districtKey r = F.rgetField @BR.StateAbbreviation r <> "-" <> show (F.rgetField @BR.CongressionalDistrict r)
wnh r = (F.rgetField @DT.RaceAlone4C r == DT.RA4_White) && (F.rgetField @DT.HispC r == DT.NonHispanic)
wnhNonGrad r = wnh r && (F.rgetField @DT.CollegeGradC r == DT.NonGrad)
wnhCCES r = (F.rgetField @DT.Race5C r == DT.R5_WhiteNonLatinx) && (F.rgetField @DT.HispC r == DT.NonHispanic)
wnhNonGradCCES r = wnhCCES r && (F.rgetField @DT.CollegeGradC r == DT.NonGrad)

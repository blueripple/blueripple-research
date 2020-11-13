{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-# OPTIONS_GHC -fplugin=Polysemy.Plugin #-}
module Main where

import qualified GA_DataFrames as GA
import qualified Data as GA
import qualified Models as GA

import qualified Control.Foldl as FL
import Control.Monad (when)
import qualified Numeric.Foldl as NFL
import qualified Data.IntMap.Strict as IM
import qualified Data.Map as M
import qualified Data.Maybe as Maybe

import qualified Data.ByteString.Lazy as BL
import qualified Data.Random.Source.PureMT     as PureMT
import qualified Data.List as L
import qualified Data.Text as T
import qualified Text.Printf as Printf
import qualified Data.Text.IO as T

import qualified Frames as F
import qualified Frames.Streamly.CSV as FS
import qualified Frames.MapReduce as FMR
import qualified Frames.Folds as FF
import qualified Frames.SimpleJoins as FJ
import qualified Frames.Transform as FT
import qualified Data.Text.Encoding as TE
import qualified Data.Text.Encoding.Error as TE
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V
import qualified Data.Vinyl.Functor            as V
import qualified Data.Vector as Vec

import qualified Data.Csv as CSV

import qualified Frames.MapReduce as FMR

import qualified Graphics.Vega.VegaLite        as GV
import           Graphics.Vega.VegaLite.Configuration as FV
import qualified Graphics.Vega.VegaLite.Compat as FV
import qualified Frames.Visualization.VegaLite.Data
                                               as FV

import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Data.LoadersCore as BR
import qualified BlueRipple.Data.Loaders as BR
import qualified BlueRipple.Utilities.KnitUtils as BR
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.ElectionTypes as ET
import qualified BlueRipple.Model.MRP as BR
import qualified BlueRipple.Data.CCES as CCES
import qualified BlueRipple.Model.CCES_MRP_Analysis as CCES
import qualified BlueRipple.Data.Keyed as BK
import qualified BlueRipple.Utilities.KnitUtils as BR

import qualified BlueRipple.Model.House.ElectionResult as HEM
import qualified BlueRipple.Model.CachedModels as BRC
import qualified BlueRipple.Model.StanCCES as BRS
import qualified BlueRipple.Data.ACS_PUMS as PUMS

import qualified Streamly.Prelude as Streamly
{-
import qualified CmdStan as CS
import qualified CmdStan.Types as CS
import qualified Stan.JSON as SJ
import qualified Stan.Parameters as SP
import qualified Stan.ModelRunner as SM
import qualified System.Environment as Env
import qualified System.Directory              as System
-}

import qualified Knit.Report as K
import           Polysemy.RandomFu              (RandomFu, runRandomIO, runRandomIOPureMT)
import Data.String.Here (here)


yamlAuthor :: T.Text
yamlAuthor = [here|
- name: Adam Conner-Sax
- name: Frank David
|]

templateVars =
  M.fromList [("lang", "English")
             , ("site-title", "Blue Ripple Politics")
             , ("home-url", "https://www.blueripplepolitics.org")             
--  , ("author"   , T.unpack yamlAuthor)
             ]


pandocTemplate = K.FullySpecifiedTemplatePath "pandoc-templates/blueripple_basic.html"


main :: IO ()
main= do
  pandocWriterConfig <- K.mkPandocWriterConfig pandocTemplate
                                               templateVars
                                               K.mindocOptionsF
  let  knitConfig = (K.defaultKnitConfig Nothing)
        { K.outerLogPrefix = Just "stan.Main"
        , K.logIf = K.logAll
        , K.pandocWriterConfig = pandocWriterConfig
        }
  let pureMTseed = PureMT.pureMT 1
  resE <- K.knitHtml knitConfig $ gaAnalysis
  case resE of
    Right htmlAsText ->
      K.writeAndMakePathLT "../Georgia/GA.html" htmlAsText
    Left err -> putStrLn $ "Pandoc Error: " ++ show err


gaAnalysis :: forall r.(K.KnitOne r,  K.CacheEffectsD r) => K.Sem r ()
gaAnalysis = do
  senateByCounty_C <- GA.gaSenateToModel
  senateByCounty <- K.ignoreCacheTime senateByCounty_C
  _ <- K.addHvega Nothing Nothing $ vlSenate1
       "Dem Votes and Turnout in the Ossoff race"
       (FV.ViewConfig 600 1500 10)
       (fmap F.rcast senateByCounty)
  modeled_C <- gaRunSBCModel
  K.ignoreCacheTime modeled_C >>= BR.logFrame
  return ()

gaRunSBCModel :: (K.KnitOne r,  K.CacheEffectsD r)
              => K.Sem r (K.ActionWithCacheTime r (F.FrameRec ('[GA.County] V.++ GA.Modeled)))
gaRunSBCModel = do
  sbc_C <- GA.gaSenateToModel 
  GA.runSenateModel GA.gaSenateModelDataWrangler ("model_v1", GA.model_v1) sbc_C

gaSenateAnalysis :: (K.KnitOne r,  K.CacheEffectsD r) => K.Sem r ()
gaSenateAnalysis = do
  gaSenate1_C <- GA.gaSenate1Loader
  gaSenate2_C <- GA.gaSenate2Loader
  gaSenate1 <- K.ignoreCacheTime gaSenate1_C
  let senate1PM = FL.fold GA.votesByMethodAndPartyF $ F.filterFrame ((/= "Total") . F.rgetField @GA.Method) gaSenate1
  _ <- K.addHvega Nothing Nothing $ vlVotingMethod
    "Voting Method by County for Democratic votes in the Ossoff/Perdue race"
    (FV.ViewConfig 600 1500 10)
    (fmap F.rcast $ F.filterFrame ((== "D") . F.rgetField @GA.Party) senate1PM)
  _ <- K.addHvega Nothing Nothing $ vlVotingMethod
    "Voting Method by County for Republican votes in the Ossoff/Perdue race"
    (FV.ViewConfig 600 1500 10)
    (fmap F.rcast $ F.filterFrame ((== "R") . F.rgetField @GA.Party) senate1PM)    
--  gaProcessElex
  return ()


gaPUMATopoJSONUrl =  "https://raw.githubusercontent.com/blueripple/Georgia/main/topojson/ga_PUMAs.json"
gaCountyTopoJSONUrl = "https://raw.githubusercontent.com/blueripple/Georgia/main/topojson/ga-counties.json"

padTo :: Int -> Int -> T.Text
padTo width n =
  let nText = T.pack $ show n
  in T.replicate (width - T.length nText) "0" <> nText 


vlDensityByPUMA :: Foldable f
                => T.Text
                -> FV.ViewConfig
                -> f (F.Record GA.X)
                -> GV.VegaLite
vlDensityByPUMA title vc rows = 
  let toVLDataRec = FV.textAsVLStr "State"
                    V.:& FV.asVLData (GV.Str . padTo 5) "PUMA"
                    V.:& FV.useColName FV.asVLNumber
                    V.:& FV.useColName FV.asVLNumber
                    V.:& FV.asVLStrViaShow "Age45"
                    V.:& FV.asVLStrViaShow "Sex"
                    V.:& FV.asVLStrViaShow "Education"
                    V.:& FV.asVLStrViaShow "Race"
                    V.:& FV.useColName FV.asVLNumber
                    V.:& V.RNil
      dat = FV.recordsToData toVLDataRec rows
      datGeo = GV.dataFromUrl gaPUMATopoJSONUrl [GV.TopojsonFeature "ga_PUMAs"]
      filter = GV.filter (GV.FExpr $ "datum.Education == 'Grad' && datum.Age45 == 'Under'")
      projection = GV.projection [GV.PrType GV.AlbersUsa]
      transform = GV.transform . GV.lookup "PUMA" datGeo "properties.PUMA" (GV.LuAs "geo") . filter
      mark = GV.mark GV.Geoshape []
      colorEnc = GV.color [GV.MName "Citizens", GV.MmType GV.Quantitative]
      shapeEnc = GV.shape [GV.MName "geo", GV.MmType GV.GeoFeature]
      enc = GV.encoding . colorEnc . shapeEnc
  in FV.configuredVegaLite vc [FV.title title, transform [], enc [], mark, projection, dat]


vlSenate1 :: Foldable f
          => T.Text
          -> FV.ViewConfig
          -> f (F.Record [GA.County, GA.DVotes1, GA.TVotes1, GA.DVotes2, GA.TVotes2, PUMS.Citizens])
          -> GV.VegaLite
vlSenate1 title vc rows =
  let toVLDataRec = FV.useColName FV.textAsVLStr
                    V.:& FV.asVLNumber "D_Votes_Ossoff"
                    V.:& FV.asVLNumber "Total_Votes_Ossoff"
                    V.:& FV.asVLNumber "D_Votes_Warnock"
                    V.:& FV.asVLNumber "Total_Votes_Warnock"
                    V.:& FV.asVLNumber "Voting_Age_Citizens"
                    V.:& V.RNil
      dat = FV.recordsToData toVLDataRec rows
      encX = GV.position GV.X [GV.PName "D_Votes_Ossoff", GV.PmType GV.Quantitative]
      encY = GV.position GV.Y [GV.PName "County", GV.PmType GV.Nominal, GV.PSort [GV.Descending, GV.ByChannel GV.ChX]]
      calcTurnout = GV.calculateAs ("100 * datum.D_Votes_Ossoff / datum.Voting_Age_Citizens") "D Turnout %"
      encColor = GV.color [GV.MName "D Turnout", GV.MmType GV.Quantitative]
      enc = GV.encoding . encX . encY . encColor 
      mark = GV.mark GV.Bar []
      transform = GV.transform . calcTurnout 
  in FV.configuredVegaLite vc [FV.title title, enc [], mark, transform [], dat]
      
                    
vlVotingMethod :: Foldable f
               => T.Text
               -> FV.ViewConfig
               -> f (F.Record [GA.County, GA.Party, GA.Method, GA.Votes])
               -> GV.VegaLite
vlVotingMethod title vc rows =
  let toVLDataRec = FV.useColName FV.textAsVLStr
                    V.:& FV.useColName FV.textAsVLStr
                    V.:& FV.useColName FV.textAsVLStr
                    V.:& FV.useColName FV.asVLNumber
                    V.:& V.RNil
      dat = FV.recordsToData toVLDataRec rows
      encY = GV.position GV.Y [GV.PName "County", GV.PmType GV.Nominal]
      encX = GV.position GV.X [GV.PName "Votes", GV.PmType GV.Quantitative]
      encColor = GV.color [GV.MName "Method", GV.MmType GV.Nominal]
      enc = GV.encoding . encX . encY . encColor
      mark = GV.mark GV.Bar []
  in FV.configuredVegaLite vc [FV.title title, enc [], mark, dat]






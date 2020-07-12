{-# LANGUAGE AllowAmbiguousTypes       #-}
{-# LANGUAGE DataKinds                 #-}
{-# LANGUAGE DeriveDataTypeable        #-}
{-# LANGUAGE DeriveGeneric             #-}
{-# LANGUAGE PolyKinds                 #-}
{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE FlexibleInstances         #-}
{-# LANGUAGE GADTs                     #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE TypeApplications          #-}
{-# LANGUAGE TypeFamilies              #-}
{-# LANGUAGE TypeOperators             #-}
{-# LANGUAGE QuasiQuotes               #-}
{-# LANGUAGE StandaloneDeriving        #-}
{-# LANGUAGE TupleSections             #-}
{-# OPTIONS_GHC  -fplugin=Polysemy.Plugin  #-}

module MRP.DistrictClusters where

import qualified Control.Foldl                 as FL
import           Data.Discrimination            ( Grouping )
import qualified Data.Map as M
import           Data.Maybe (catMaybes)
import qualified Data.Text                     as T
import qualified Data.Serialize                as Serialize
import qualified Data.Vector                   as Vec
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V

import GHC.Generics (Generic)

import           Graphics.Vega.VegaLite.Configuration as FV
import qualified Graphics.Vega.VegaLite.Compat as FV
import qualified Frames as F
import qualified Frames.Melt as F
import qualified Frames.InCore                 as FI
import qualified Polysemy.Error as P
import qualified Polysemy.ConstraintAbsorber.MonadRandom as PMR

import qualified Control.MapReduce             as MR
import qualified Frames.Transform              as FT
import qualified Frames.Folds                  as FF
import qualified Frames.MapReduce              as FMR
import qualified Frames.SimpleJoins            as FJ
import qualified Frames.Misc                   as FM
import qualified Frames.Serialize              as FS
import qualified Frames.KMeans                 as FK
import qualified Math.Rescale                  as MR

import qualified Frames.Visualization.VegaLite.Data
                                               as FV

import qualified Graphics.Vega.VegaLite        as GV
import qualified Knit.Report                   as K

  
import           Data.String.Here               ( i, here )

import qualified Text.Blaze.Html               as BH
import qualified Text.Blaze.Html5.Attributes   as BHA

import           BlueRipple.Configuration 
import           BlueRipple.Utilities.KnitUtils 
import qualified BlueRipple.Utilities.TableUtils as BR

import qualified Numeric.GLM.ProblemTypes      as GLM
import qualified Numeric.GLM.Bootstrap            as GLM
import qualified Numeric.GLM.MixedModel            as GLM

import qualified Data.Time.Calendar            as Time
import qualified Data.Time.Clock               as Time

import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Data.Loaders as BR
import qualified BlueRipple.Data.ACS_PUMS as PUMS
import qualified BlueRipple.Data.CPSVoterPUMS as CPS
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Data.ElectionTypes as ET

import qualified BlueRipple.Model.MRP as BR
import qualified BlueRipple.Model.Turnout_MRP as BR

import qualified BlueRipple.Data.UsefulDataJoins as BR
import qualified MRP.CCES_MRP_Analysis as BR
import qualified MRP.CachedModels as BR
import qualified BlueRipple.Utilities.KnitUtils as BR
import qualified BlueRipple.Data.Keyed         as Keyed
import MRP.Common
import MRP.CCES
import qualified MRP.CCES as CCES

text1 :: T.Text
text1 = [i|

|]

text2 :: T.Text = [here|

|]
  

type ASER5CD as = ('[BR.Year, BR.StateAbbreviation, BR.StateFIPS, BR.CongressionalDistrict] V.++ DT.CatColsASER5) V.++ as

addPUMSZerosF :: FL.Fold (F.Record (ASER5CD '[PUMS.Citizens, PUMS.NonCitizens])) (F.FrameRec (ASER5CD '[PUMS.Citizens, PUMS.NonCitizens]))
addPUMSZerosF =
  let zeroPop ::  F.Record '[PUMS.Citizens, PUMS.NonCitizens]
      zeroPop = 0 F.&: 0 F.&: V.RNil
  in FMR.concatFold
     $ FMR.mapReduceFold
     FMR.noUnpack
     (FMR.assignKeysAndData @'[BR.Year, BR.StateAbbreviation, BR.StateFIPS, BR.CongressionalDistrict])
     (FMR.makeRecsWithKey id
       $ FMR.ReduceFold
       $ const
       $ Keyed.addDefaultRec @DT.CatColsASER5 zeroPop)
                                                                  
type PctWWC = "PctWWC" F.:-> Double
type PctBlack = "PctBlack" F.:-> Double
type W = "w" F.:-> Double


districtToWWCBlack :: FL.Fold
                        (F.Record (PUMS.CDCounts DT.CatColsASER5))
                        (F.FrameRec '[BR.Year, BR.StateAbbreviation, BR.CongressionalDistrict, PctWWC, PctBlack, W])
districtToWWCBlack =
  let districtToXYW :: FL.Fold (F.Record (DT.CatColsASER5 V.++ '[PUMS.Citizens])) (F.Record [PctWWC, PctBlack, W])
      districtToXYW =
        let isWWC r = F.rgetField @DT.Race5C r == DT.R5_WhiteNonLatinx && F.rgetField @DT.CollegeGradC r == DT.Grad
            isBlack r =  F.rgetField @DT.Race5C r == DT.R5_Black
            citizens = realToFrac . F.rgetField @PUMS.Citizens        
            citizensF = FL.premap citizens FL.sum
            pctWWCF = (/) <$> FL.prefilter isWWC (FL.premap citizens FL.sum) <*> citizensF
            pctBlackF = (/) <$> FL.prefilter isBlack (FL.premap citizens FL.sum) <*> citizensF
        in (\x y w -> x F.&: y F.&: w F.&: V.RNil) <$> pctWWCF <*> pctBlackF <*> citizensF
  in FMR.concatFold
     $ FMR.mapReduceFold
     FMR.noUnpack
     (FMR.assignKeysAndData @'[BR.Year, BR.StateAbbreviation, BR.CongressionalDistrict] @(DT.CatColsASER5 V.++ '[PUMS.Citizens]))
     (FMR.foldAndAddKey districtToXYW)

votesToVoteShareF :: FL.Fold (F.Record [ET.Party, ET.Votes]) (F.Record '[ET.PrefType, BR.DemPref])
votesToVoteShareF =
  let
    party = F.rgetField @ET.Party
    votes = F.rgetField @ET.Votes
    demVotesF = FL.prefilter (\r -> party r == ET.Democratic) $ FL.premap votes FL.sum
    demRepVotesF = FL.prefilter (\r -> let p = party r in (p == ET.Democratic || p == ET.Republican)) $ FL.premap votes FL.sum
    demPref d dr = if dr > 0 then realToFrac d/realToFrac dr else 0
    demPrefF = demPref <$> demVotesF <*> demRepVotesF
  in fmap (\x -> FT.recordSingleton ET.VoteShare `V.rappend` FT.recordSingleton @BR.DemPref x) demPrefF
     
post :: forall r.(K.KnitMany r, K.CacheEffectsD r, K.Member GLM.RandomFu r) => Bool -> K.Sem r ()
post updated = P.mapError BR.glmErrorToPandocError $ K.wrapPrefix "BidenVsWWC" $ do

  let clusterRowsToS (cs, rs) = (fmap FS.toS cs, fmap FS.toS rs)
      clusterRowsFromS (cs', rs') = (fmap FS.fromS cs', fmap FS.fromS rs')

  pums2018ByCD_C <- do
    demo_C <- PUMS.pumsLoader
    BR.retrieveOrMakeFrame "mrp/DistrictClusters/pums2018ByCD.bin" demo_C $ \pumsRaw -> do
      let pums2018Raw = F.filterFrame ((== 2018) . F.rgetField @BR.Year) pumsRaw
      pumsCDRollup <- PUMS.pumsCDRollup (PUMS.pumsKeysToASER5 True . F.rcast) pums2018Raw
      return $ FL.fold addPUMSZerosF pumsCDRollup

  K.clearIfPresent "mrp/DistrictClusters/clusteredDistricts.bin"
  clusteredDistricts <- K.ignoreCacheTimeM $ do
    houseResults_C <- BR.houseElectionsLoader
    let cachedDeps = (,) <$> pums2018ByCD_C <*> houseResults_C
    K.retrieveOrMakeTransformed
      clusterRowsToS
      clusterRowsFromS
      "mrp/DistrictClusters/clusteredDistricts.bin"
      cachedDeps $ \(pumsByCD, houseResults) -> do
        let labelCD r = F.rgetField @BR.StateAbbreviation r <> "-" <> (T.pack $ show $ F.rgetField @BR.CongressionalDistrict r)
            filterHR r = F.rgetField @BR.Year r == 2018
                         && F.rgetField @BR.Stage r == "gen"
                         && F.rgetField @BR.Runoff r == False
                         && F.rgetField @BR.Special r == False
                         && (F.rgetField @ET.Party r == ET.Democratic || F.rgetField @ET.Party r == ET.Republican)
            houseResults2018 = fmap (F.rcast @[BR.Year, BR.StateAbbreviation, BR.CongressionalDistrict, ET.Party, ET.Votes, ET.TotalVotes])
                               $ F.filterFrame filterHR houseResults
            houseResults2018F = FMR.concatFold $ FMR.mapReduceFold
                                FMR.noUnpack
                                (FMR.assignKeysAndData @[BR.Year, BR.StateAbbreviation, BR.CongressionalDistrict])
                                (FMR.foldAndAddKey votesToVoteShareF)
          
            forClustering = F.toFrame
                            $ catMaybes
                            $ fmap F.recMaybe
                            $ F.leftJoin @[BR.Year, BR.StateAbbreviation, BR.CongressionalDistrict]
                            (FL.fold districtToWWCBlack pumsByCD)
                            (FL.fold houseResults2018F houseResults2018)
        let initialCentroidsF n 
              = FMR.functionToFoldM $ \hx -> PMR.absorbMonadRandom $ FK.kMeansPPCentroids @PctWWC @PctBlack @W FK.euclidSq n hx            
        rawClusters <- FK.kMeansOneWithClusters @PctWWC @PctBlack @W
                       (FL.premap (\r -> (F.rgetField @PctWWC r, F.rgetField @W r)) $ MR.weightedScaleAndUnscale MR.RescaleNone MR.RescaleNone id)
                       (FL.premap (\r -> (F.rgetField @PctBlack r, F.rgetField @W r)) $ MR.weightedScaleAndUnscale MR.RescaleNone MR.RescaleNone id)
                       20
                       10
                       initialCentroidsF
                       (FK.weighted2DRecord @PctWWC @PctBlack @W)
                       FK.euclidSq
                       forClustering
        return $ FK.clusteredRowsFull @PctWWC @PctBlack @W labelCD $ M.fromList [((2018 F.&: V.RNil) :: F.Record '[BR.Year], rawClusters)]

  logFrame $ F.toFrame $ fst clusteredDistricts
  logFrame $ F.toFrame $ snd clusteredDistricts
  curDate <-  (\(Time.UTCTime d _) -> d) <$> K.getCurrentTime
  let pubDateDistrictClusters =  Time.fromGregorian 2020 7 25
  K.newPandoc
    (K.PandocInfo ((postRoute PostDistrictClusters) <> "main")
      (brAddDates updated pubDateDistrictClusters curDate
       $ M.fromList [("pagetitle", "Looking for Flippable House Districts")
                    ,("title","Looking For Flippable House Districts")
                    ]
      ))
      $ do        
        brAddMarkDown text1
        _ <- K.addHvega Nothing Nothing
             $ clusterVL @PctWWC @PctBlack
             "2018 House Districts Clustered By %WWC and %Black"
             (FV.ViewConfig 800 800 10)
             (fmap F.rcast $ fst clusteredDistricts)
             (fmap F.rcast $ snd clusteredDistricts)
        brAddMarkDown brReadMore


clusterVL :: forall x y f.
             (Foldable f
             , FV.ToVLDataValue (F.ElField x)
             , FV.ToVLDataValue (F.ElField y)
             , F.ColumnHeaders '[x]
             , F.ColumnHeaders '[y]
             )
          => T.Text
          -> FV.ViewConfig
          -> f (F.Record [BR.Year, FK.ClusterId, x, y, W])
          -> f (F.Record ([BR.Year, FK.ClusterId, FK.MarkLabel, x, y, W, ET.DemPref]))
          -> GV.VegaLite
clusterVL title vc centroidRows districtRows =
  let datCentroids = FV.recordsToVLData id FV.defaultParse centroidRows
      datDistricts = FV.recordsToVLData id FV.defaultParse districtRows
      makeShare = GV.calculateAs "datum.DemPref - 0.5" "Dem Vote Share"
      encX = GV.position GV.X [FV.pName @x, GV.PmType GV.Quantitative]
      encY = GV.position GV.Y [FV.pName @y, GV.PmType GV.Quantitative]
      encColorC = GV.color [FV.mName @W, GV.MmType GV.Quantitative {- , GV.MScale [GV.SScheme "accent" []]-}]
      encColorD = GV.color [GV.MName "Dem Vote Share", GV.MmType GV.Quantitative, GV.MScale [GV.SScheme "redblue" []]]
      encSizeC = GV.size [FV.mName @W, GV.MmType GV.Quantitative]
      clusterSpec = GV.asSpec [(GV.encoding . encX . encY . encSizeC) [], GV.mark GV.Circle [], datCentroids]
      districtSpec = GV.asSpec [(GV.encoding . encX . encY . encColorD) [], GV.mark GV.Circle [], (GV.transform . makeShare) [], datDistricts]
  in FV.configuredVegaLite  vc [FV.title title, GV.layer [clusterSpec, districtSpec]]
  


--clusterVL :: T.Text -> [(Double, Double, Double)

{-
vlRallyWWC :: Foldable f
                 => T.Text
                 -> FV.ViewConfig
                 -> f (F.Record [BR.StateAbbreviation, PollMargin, ExcessWWCPer, ExcessBasePer])
                 -> GV.VegaLite
vlRallyWWC title vc rows =
  let dat = FV.recordsToVLData id FV.defaultParse rows
      makeWWC = GV.calculateAs "datum.PollMargin * datum.ExcessWWCPer" "Excess WWC %"
      makeBase = GV.calculateAs "datum.PollMargin * datum.ExcessBasePer" "Excess Base %"
      renameMargin = GV.calculateAs "datum.PollMargin" "Poll Margin %"
      renameSA = GV.calculateAs "datum.state_abbreviation" "State"      
      doFold = GV.foldAs ["Excess WWC %", "Excess Base %"] "Type" "Pct"
      encY = GV.position GV.Y [GV.PName "Type", GV.PmType GV.Nominal, GV.PAxis [GV.AxNoTitle]
                              , GV.PSort [GV.CustomSort $ GV.Strings ["Excess WWC %", "Excess Base %"]]
                              ]
      encX = GV.position GV.X [GV.PName "Pct"
                              , GV.PmType GV.Quantitative
                              ]
      encFacet = GV.row [GV.FName "State", GV.FmType GV.Nominal]
      encColor = GV.color [GV.MName "Type"
                          , GV.MmType GV.Nominal
                          , GV.MSort [GV.CustomSort $ GV.Strings ["Excess WWC %", "Excess Base %"]]
                          ]
      enc = GV.encoding . encX . encY . encColor . encFacet
      transform = GV.transform .  makeWWC . makeBase . renameSA . renameMargin . doFold
  in FV.configuredVegaLite vc [FV.title title, enc [], transform [], GV.mark GV.Bar [], dat]
-}

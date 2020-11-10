{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
module Main where

import qualified Control.Foldl as FL
import qualified Numeric.Foldl as NFL
import qualified Data.IntMap.Strict as IM
import qualified Data.Map as M
import qualified Data.Maybe as Maybe

import qualified Data.ByteString.Lazy as BL
import qualified Data.Random.Source.PureMT     as PureMT
import qualified Data.Text as T
import qualified Text.Printf as Printf
import qualified Data.Text.IO as T
import qualified Frames as F
import qualified Frames.Streamly.CSV as FS
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

import qualified CmdStan as CS
import qualified CmdStan.Types as CS
import qualified Stan.JSON as SJ
import qualified Stan.Parameters as SP
import qualified Stan.ModelRunner as SM
import qualified System.Environment as Env

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
  resE <- K.knitHtml knitConfig $ runRandomIOPureMT pureMTseed $ gaPUMAs
  case resE of
    Right htmlAsText ->
      K.writeAndMakePathLT "stan.html" htmlAsText
    Left err -> putStrLn $ "Pandoc Error: " ++ show err

type X = [BR.StateAbbreviation
         , BR.PUMA
         , PUMS.Citizens         
         , DT.PopPerSqMile
         , DT.SimpleAgeC
         , DT.SexC
         , DT.CollegeGradC
         , DT.Race5C
         , DT.AvgIncome
         ]
         
formatPct :: (V.KnownField t, Printf.PrintfArg (V.Snd t), Num (V.Snd t)) => V.Lift (->) V.ElField (V.Const T.Text) t
formatPct = FS.liftFieldFormatter (T.pack . Printf.printf "%.1f")

formatWholeNumber :: (V.KnownField t, Printf.PrintfArg (V.Snd t), Num (V.Snd t)) => V.Lift (->) V.ElField (V.Const T.Text) t
formatWholeNumber = FS.liftFieldFormatter (T.pack . Printf.printf "%.0f")



gaPUMAs :: forall r.(K.KnitOne r,  K.CacheEffectsD r, K.Member RandomFu r) => K.Sem r ()
gaPUMAs = do
  let testList :: [(Double, Int)] = [(100, 10), (100, 20), (100, 30)]
      testMedian = FL.fold (NFL.weightedMedianF fst snd) testList
  K.logLE K.Info $ "median=" <> (T.pack $ show testMedian)
  let f r = F.rgetField @BR.StateAbbreviation r == "GA"
            && F.rgetField @BR.Year r == 2018
  gaPUMAs_C <- fmap (F.filterFrame f) <$> PUMS.pumsLoaderAdults
  gaPUMAsRolled_C <- BR.retrieveOrMakeFrame "georgia/gaPUMAs.bin" gaPUMAs_C $ \gaPUMAs_Raw -> do
    let rolledUp = fmap (F.rcast @X) $ FL.fold (PUMS.pumsRollupF (const True) (PUMS.pumsKeysToASER5 True)) gaPUMAs_Raw
        formatRec =
          FS.formatTextAsIs
          V.:& FS.formatWithShow
          V.:& FS.formatWithShow
          V.:& formatPct
          V.:& FS.formatWithShow
          V.:& FS.formatWithShow
          V.:& FS.formatWithShow
          V.:& FS.formatWithShow
          V.:& formatWholeNumber
          V.:&  V.RNil
    BR.logFrame rolledUp
    K.liftKnit @IO $ FS.writeLines "gaPUMAs.csv" $ FS.streamSV' formatRec "," $ Streamly.fromFoldable rolledUp
    return rolledUp
  gaPUMAs <- K.ignoreCacheTime gaPUMAsRolled_C  
  _ <- K.addHvega Nothing Nothing $ vlDensityByPUMA
    "Young & College Educated by PUMA"
    (FV.ViewConfig 600 600 10)
    gaPUMAs
  gaProcessElex
  return ()


gaProcessElex :: (K.KnitEffects r) => K.Sem r ()
gaProcessElex = do
  let s1CSV = "../Georgia/data/election/Senate1.csv"
      s2CSV = "../Georgia/data/election/Senate2.csv"
      processOne :: (T.Text, T.Text) -> Maybe [T.Text]
      processOne (h, n) = 
        let hParts = T.splitOn "_" h
        in if length hParts < 3
           then Nothing
           else let (p : (c : ms)) = hParts in Just [p, c, mconcat ms, n]

      processLine :: M.Map T.Text T.Text -> Maybe [T.Text]
      processLine m = do
        stateFIPS <- M.lookup "State_FIPS" m
        countyFIPS <- M.lookup "County_FIPS" m
        county <- M.lookup "County" m
        let common = [stateFIPS, countyFIPS, county]
        let m' = M.delete "State_FIPS" $ M.delete "County_FIPS" $ M.delete "County" $ M.delete "Total" m
        processed <- traverse processOne $ M.toList m'
        let prefixed = fmap (common <>) processed
        return $ fmap (T.intercalate ",") prefixed

      processFile :: Vec.Vector (M.Map T.Text T.Text) -> Maybe T.Text
      processFile v = do
        let h :: T.Text = "StateFIPS,CountyFIPS,County,Party,Candidate,Method,Votes"
        rows <- mconcat . Vec.toList <$> traverse processLine v
        return $ T.intercalate "\n" (h : rows)
        
  csv1 <- loadCSVToMaps s1CSV
  res1 <- K.knitMaybe "csv loading error" $ processFile csv1
  K.liftKnit $ T.writeFile "../Georgia/data/election/Senate1_long.csv" res1

  csv2 <- loadCSVToMaps s2CSV
  res2 <- K.knitMaybe "csv loading error" $ processFile csv2
  K.liftKnit $ T.writeFile "../Georgia/data/election/Senate2_long.csv" res2
          
        
loadCSVToMaps :: (K.KnitEffects r) => FilePath -> K.Sem r (Vec.Vector (M.Map T.Text T.Text))
loadCSVToMaps fp = do
  csvData <- K.liftKnit $ BL.readFile fp
  case CSV.decodeByName csvData of
    Left err -> K.knitError $ "CSV parsing error: " <> (T.pack err)
    Right (_, rowsV) -> return rowsV
    
{-    do
      let headerV = Vec.map (TE.decodeUtf8With TE.strictDecode) headerV'
      return $ fmap (M.fromList . Vec.toList . Vec.zip headerV) rowsV
-}

padTo :: Int -> Int -> T.Text
padTo width n =
  let nText = T.pack $ show n
  in T.replicate (width - T.length nText) "0" <> nText 
  
gaPUMATopoJSONUrl =  "https://raw.githubusercontent.com/blueripple/Georgia/main/topojson/ga_PUMAs.json"

vlDensityByPUMA :: Foldable f
                => T.Text
                -> FV.ViewConfig
                -> f (F.Record X)
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
              
  


  
--  (demographics, elex) <- K.ignoreCacheTimeM $ HEM.prepCachedData
--  BR.logFrame demographics
--  BR.logFrame elex
--  return ()
  
testCCESPref :: forall r.(K.KnitOne r,  K.CacheEffectsD r, K.Member RandomFu r) => K.Sem r ()
testCCESPref = do
  K.logLE K.Info "Stan model fit for 2016 presidential votes:"
  stan_allBuckets <- K.ignoreCacheTimeM
                     $ BRS.prefASER5_MR
                     ("v1", BRS.ccesDataWrangler)
                     ("binomial_allBuckets", BRS.model_BinomialAllBuckets)
                     ET.President
                     2016
                     
  stan_sepFixedWithStates <- K.ignoreCacheTimeM
                             $ BRS.prefASER5_MR
                             ("v2", BRS.ccesDataWrangler2)
                             ("binomial_sepFixedWithStates", BRS.model_v5)
                             ET.President
                             2016                     

  stan_sepFixedWithStates3 <- K.ignoreCacheTimeM
                              $ BRS.prefASER5_MR
                              ("v2", BRS.ccesDataWrangler2)
                              ("binomial_sepFixedWithStates3", BRS.model_v7)
                              ET.President
                              2016                     


  K.logLE K.Info $ "allBuckets vs sepFixedWithStates3"
  let compList = zip (FL.fold FL.list stan_allBuckets) $ fmap (F.rgetField @ET.DemPref) $ FL.fold FL.list stan_sepFixedWithStates3
  K.logLE K.Info $ T.intercalate "\n" . fmap (T.pack . show) $ compList
  
  BRS.prefASER5_MR_Loo ("v1", BRS.ccesDataWrangler) ("binomial_allBuckets", BRS.model_BinomialAllBuckets) ET.President 2016
  BRS.prefASER5_MR_Loo ("v1", BRS.ccesDataWrangler) ("binomial_bucketFixedStateIntcpt", BRS.model_v2) ET.President 2016
  BRS.prefASER5_MR_Loo ("v1", BRS.ccesDataWrangler) ("binomial_bucketFixedOnly", BRS.model_v3) ET.President 2016
  BRS.prefASER5_MR_Loo ("v2", BRS.ccesDataWrangler2) ("binomial_sepFixedOnly", BRS.model_v4) ET.President 2016
  BRS.prefASER5_MR_Loo ("v2", BRS.ccesDataWrangler2) ("binomial_sepFixedWithStates", BRS.model_v5) ET.President 2016
  BRS.prefASER5_MR_Loo ("v2", BRS.ccesDataWrangler2) ("binomial_sepFixedWithStates2", BRS.model_v6) ET.President 2016
  BRS.prefASER5_MR_Loo ("v2", BRS.ccesDataWrangler2) ("binomial_sepFixedWithStates3", BRS.model_v7) ET.President 2016
  
--  BR.logFrame stan
{-
  K.logLE K.Info "glm-haskell model fit for 2016 presidential votes:"
  let g r = (F.rgetField @BR.Year r == 2016) && (F.rgetField @ET.Office r == ET.President)
  glmHaskell <- F.filterFrame g <$> (K.ignoreCacheTimeM $ BRC.ccesPreferencesASER5_MRP)
  BR.logFrame glmHaskell
-}

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
import qualified Data.IntMap.Strict as IM
import qualified Data.Map as M
import qualified Data.Maybe as Maybe

import qualified Data.Random.Source.PureMT     as PureMT
import qualified Data.Text as T
import qualified Frames as F
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V

import qualified Frames.MapReduce as FMR

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
  resE <- K.knitHtml knitConfig $ runRandomIOPureMT pureMTseed $ testCCESPref
  case resE of
    Right htmlAsText ->
      K.writeAndMakePathLT "stan.html" htmlAsText
    Left err -> putStrLn $ "Pandoc Error: " ++ show err



testHouseModel :: forall r.(K.KnitOne r,  K.CacheEffectsD r, K.Member RandomFu r) => K.Sem r ()
testHouseModel = do
  (demographics, elex) <- K.ignoreCacheTimeM $ HEM.prepCachedData

  BR.logFrame demographics
  BR.logFrame elex
  return ()
  
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

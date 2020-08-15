{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import qualified Data.Text as T
import qualified Data.Map as M
import qualified Knit.Report as K
import qualified Control.Foldl                 as FL
import qualified BlueRipple.Data.ACS_PUMS as PUMS
import           Data.String.Here               ( i, here )
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
        { K.outerLogPrefix = Just "Testbed.Main"
        , K.logIf = K.logDiagnostic
        , K.pandocWriterConfig = pandocWriterConfig
        }      
  resE <- K.knitHtml knitConfig makeDoc
  case resE of
    Right htmlAsText ->
      K.writeAndMakePathLT "docs/simple_example.html" htmlAsText
    Left err -> putStrLn $ "Pandoc Error: " ++ show err

makeDoc :: (K.KnitOne r,  K.CacheEffectsD r) => K.Sem r ()
makeDoc = do
  K.logLE K.Info "Loading PUMS data..."
  pumsAge5F_C <- PUMS.pumsLoader "testbed/data/acs1YrPUMS_Age5F.sbin" Nothing
  pumsAge5F <- K.ignoreCacheTime pumsAge5F_C
  K.logLE K.Info $ "PUMS data has " <> (T.pack $ show $ FL.fold FL.length pumsAge5F) <> " rows."
  

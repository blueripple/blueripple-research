{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module Main where

import qualified Data.Text as T
import qualified Data.Map as M
import qualified Knit.Report as K
import qualified Knit.Utilities.Streamly as K
import qualified Polysemy as P
import qualified Control.Foldl                 as FL
import qualified BlueRipple.Data.ACS_PUMS as PUMS
import qualified BlueRipple.Data.ACS_PUMS_Loader.ACS_PUMS_Frame as PUMS
import           Data.String.Here               ( i, here )
import qualified Frames.Streamly as FStreamly
import qualified BlueRipple.Data.LoadersCore as Loaders
import qualified Frames.CSV                     as Frames 
import qualified Streamly.Data.Fold            as Streamly.Fold
import qualified Streamly.Prelude              as Streamly
import qualified Streamly.Internal.Prelude              as Streamly
import qualified Streamly              as Streamly
import qualified Streamly.Internal.FileSystem.File
                                               as Streamly.File
                                               
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
--  testsInIO 
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


makeDoc :: forall r. (K.KnitOne r,  K.CacheEffectsD r) => K.Sem r ()
makeDoc = do
  let pumsCSV = "testbed/medPUMS.csv"
      dataPath = (Loaders.LocalData $ T.pack $ pumsCSV)
  K.logLE K.Info "Testing File.toBytes..."
  let rawBytesS =  Streamly.File.toBytes pumsCSV
  rawBytes <-  K.streamlyToKnit $ Streamly.fold Streamly.Fold.length rawBytesS
  K.logLE K.Info $ "raw PUMS data has " <> (T.pack $ show rawBytes) <> " bytes."
{-
  K.logLE K.Info "Testing streamTable..."
  let pumsRowsRawS :: Streamly.SerialT K.StreamlyM PUMS.PUMS_Raw
        = FStreamly.streamTable Frames.defaultParser "bigData/IPUMS/acsSelected2006To2018.csv"
  rawRows <-  K.streamlyToKnit $ Streamly.fold Streamly.Fold.length pumsRowsRawS
  K.logLE K.Info $ "raw PUMS data has " <> (T.pack $ show rawRows) <> " rows."


  K.logLE K.Info "Testing pumsRowsLoader..."
  let pumsRowsFixedS = PUMS.pumsRowsLoader dataPath Nothing 
  fixedRows <- K.streamlyToKnit $ Streamly.fold Streamly.Fold.length pumsRowsFixedS    
  K.logLE K.Info $ "fixed PUMS data has " <> (T.pack $ show fixedRows) <> " rows."
-}
  K.logLE K.Info "Testing pumsLoader..."
--  K.clearIfPresent (T.pack "testbed/data/test.sbin")
  pumsAge5F_C <- PUMS.pumsLoader' dataPath "testbed/data/test.sbin" Nothing
  pumsAge5F <- K.ignoreCacheTime pumsAge5F_C
  K.logLE K.Info $ "PUMS data has " <> (T.pack $ show $ FL.fold FL.length pumsAge5F) <> " rows."
  
testsInIO :: IO ()
testsInIO = do
  let pumsCSV = "testbed/medPUMS.csv"
  putStrLn "Tests in IO"
  putStrLn $ T.unpack "Testing File.toBytes..."
  let rawBytesS = Streamly.File.toBytes pumsCSV
  rawBytes <- Streamly.fold Streamly.Fold.length rawBytesS
  putStrLn $ T.unpack $ "raw PUMS data has " <> (T.pack $ show rawBytes) <> " bytes."
  putStrLn $ T.unpack "Testing streamTable..."
  let pumsRowsRawS :: Streamly.SerialT IO PUMS.PUMS_Raw
        = FStreamly.streamTable Frames.defaultParser pumsCSV
  rawRows <- Streamly.fold Streamly.Fold.length pumsRowsRawS
  putStrLn $ T.unpack $ "raw PUMS data has " <> (T.pack $ show rawRows) <> " rows."
  let --pumsRowsFixedS :: Streamly.SerialT IO PUMS.PUMS
      pumsRowsFixedS = Loaders.recStreamLoader (Loaders.LocalData $ T.pack $ pumsCSV) Nothing Nothing PUMS.transformPUMSRow
  fixedRows <- Streamly.fold Streamly.Fold.length pumsRowsFixedS
  putStrLn $ T.unpack $ "fixed PUMS data has " <> (T.pack $ show fixedRows) <> " rows."

  

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
import qualified Control.Foldl                 as FL
import qualified BlueRipple.Data.ACS_PUMS as PUMS
import qualified BlueRipple.Data.ACS_PUMS_Loader.ACS_PUMS_Frame as PUMS
import           Data.String.Here               ( i, here )
import qualified Frames.Streamly as FStreamly
import qualified BlueRipple.Data.LoadersCore as Loaders
import qualified Frames.CSV                     as Frames 
import qualified Streamly.Data.Fold            as Streamly.Fold
import qualified Streamly.Prelude              as Streamly
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
  K.logLE K.Info "Testing File.toBytes..."
  let rawBytesS = Streamly.File.toBytes "bigData/IPUMS/acsSelected2006To2018.csv"
  rawBytes <- runStreamlyInSem (Streamly.fold Streamly.Fold.length) rawBytesS
  K.logLE K.Info $ "raw PUMS data has " <> (T.pack $ show rawBytes) <> " bytes."
  K.logLE K.Info "Testing streamTable..."
  let pumsRowsRawS :: Streamly.SerialT (K.Sem r) PUMS.PUMS_Raw
        = Loaders.fixMonadCatch $ FStreamly.streamTable Frames.defaultParser "bigData/IPUMS/acsSelected2006To2018.csv"
  rawRows <- Streamly.fold Streamly.Fold.length pumsRowsRawS
  K.logLE K.Info $ "raw PUMS data has " <> (T.pack $ show rawRows) <> " rows."
  K.logLE K.Info "Testing pumsLoader..."
  pumsAge5F_C <- PUMS.pumsLoader "testbed/data/acs1YrPUMS_Age5F.sbin" Nothing
  pumsAge5F <- K.ignoreCacheTime pumsAge5F_C
  K.logLE K.Info $ "PUMS data has " <> (T.pack $ show $ FL.fold FL.length pumsAge5F) <> " rows."
  
testsInIO :: IO ()
testsInIO = do
  putStrLn "Tests in IO (also without fixMonadCatch)"
  putStrLn $ T.unpack "Testing File.toBytes..."
  let rawBytesS = Streamly.File.toBytes "bigData/IPUMS/acsSelected2006To2018.csv"
  rawBytes <- Streamly.fold Streamly.Fold.length rawBytesS
  putStrLn $ T.unpack $ "raw PUMS data has " <> (T.pack $ show rawBytes) <> " bytes."
  putStrLn $ T.unpack "Testing streamTable..."
  let pumsRowsRawS :: Streamly.SerialT IO PUMS.PUMS_Raw
        = FStreamly.streamTable Frames.defaultParser "bigData/IPUMS/acsSelected2006To2018.csv"
  rawRows <- Streamly.fold Streamly.Fold.length pumsRowsRawS
  putStrLn $ T.unpack $ "raw PUMS data has " <> (T.pack $ show rawRows) <> " rows."


runStreamlyInSem :: (K.KnitEffects r, Streamly.IsStream t) => (t K.StreamlyM a -> K.StreamlyM b) -> t K.StreamlyM a -> K.Sem r b
runStreamlyInSem runWith = K.streamlyToKnit . runWith

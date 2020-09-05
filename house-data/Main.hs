{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
module Main where

import qualified Data.Map as M
import qualified Data.Text as T

import qualified Frames as F
import qualified Frames.Transform as FT

import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Data.LoadersCore as BR
import qualified BlueRipple.Utilities.KnitUtils as BR
import qualified BlueRipple.Data.ElectionTypes as ET

import qualified Knit.Report as K
--import qualified Knit.Report.Cache as K
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
        { K.outerLogPrefix = Just "house-data.Main"
        , K.logIf = K.logDiagnostic
        , K.pandocWriterConfig = pandocWriterConfig
        }      
  resE <- K.knitHtml knitConfig makeDoc
  case resE of
    Right htmlAsText ->
      K.writeAndMakePathLT "housedata.html" htmlAsText
    Left err -> putStrLn $ "Pandoc Error: " ++ show err

makeDoc :: forall r.(K.KnitOne r,  K.CacheEffectsD r) => K.Sem r ()
makeDoc = do
  pollData_C :: K.ActionWithCacheTime r (F.FrameRec HousePollRow) <-
    BR.cachedMaybeFrameLoader @(F.RecordColumns BR.HousePolls2020) @HousePollRow' @HousePollRow
    (BR.DataSets $ T.pack BR.housePolls2020CSV)
    Nothing
    Nothing
    id
    fixHousePollRow
    Nothing
    "housePolls2020.bin"
  K.ignoreCacheTime pollData_C >>= BR.logFrame
  fecData_C :: K.ActionWithCacheTime r (F.FrameRec FECRow) <- BR.cachedFrameLoader (BR.DataSets $ T.pack BR.allMoney2020CSV) Nothing Nothing fixFECRow Nothing "allMoney2020.bin"
--  K.ignoreCacheTime fecData_C >>= BR.logFrame
  return ()


type HousePollRow' = '[BR.State, BR.FteGrade, BR.CongressionalDistrict, BR.StartDate, BR.EndDate, BR.Internal, BR.CandidateName, BR.CandidateParty]
type HousePollRow = '[BR.State, BR.FteGrade, BR.CongressionalDistrict, BR.StartDate, BR.EndDate, BR.Internal, BR.CandidateName, ET.Party]

parseHousePollParty :: T.Text -> ET.PartyT
parseHousePollParty t
  | T.isInfixOf "DEM" t = ET.Democratic
  | T.isInfixOf "REP" t = ET.Republican
  | otherwise = ET.Other

fixHousePollRow :: F.Record HousePollRow' -> F.Record HousePollRow
fixHousePollRow = F.rcast . addCols where
  addCols = FT.addOneFromOne @BR.CandidateParty @ET.Party parseHousePollParty

type FECRaw = [BR.CandidateId, BR.CandidateName, BR.StateAbbreviation, BR.CongressionalDistrict, BR.Party, BR.CashOnHand, BR.Disbursements, BR.Receipts, BR.IndSupport, BR.IndOppose, BR.PartyExpenditures]
type FECRow =  [BR.CandidateName, BR.StateAbbreviation, BR.CongressionalDistrict, ET.Party, BR.CashOnHand, BR.Disbursements, BR.Receipts, BR.IndSupport, BR.IndOppose, BR.PartyExpenditures]


parseFECParty :: T.Text -> ET.PartyT
parseFECParty t
  | T.isInfixOf "Democrat" t = ET.Democratic
  | T.isInfixOf "Republican" t = ET.Republican
  | otherwise = ET.Other


fixFECRow :: F.Record FECRaw -> F.Record FECRow
fixFECRow = F.rcast . addCols where
  addCols = FT.addOneFromOne @BR.Party @ET.Party parseFECParty

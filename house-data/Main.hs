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


import qualified Control.Foldl as FL
import qualified Data.Map as M
import           Data.Maybe (fromJust)
import qualified Data.Ord as Ord
import qualified Data.Text as T
import qualified Data.Vinyl as V

import qualified Frames as F
import qualified Frames.Transform as FT
import qualified Frames.SimpleJoins as FJ
import qualified Frames.MapReduce as FMR

import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Data.Loaders as BR
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
  K.clearIfPresent "data/housePolls2020.bin" 
  pollData_C <- housePolls2020Loader
  fecData_C <- fec2020Loader
  houseElections_C <- BR.houseElectionsLoader
  stateAbbr_C <- BR.stateAbbrCrosswalkLoader

  let houseRaceDeps = (,,,) <$> pollData_C <*> stateAbbr_C <*> houseElections_C <*> fecData_C
  K.clearIfPresent "house-data/houseRaceInfo.bin" -- until we have it right
  houseRaceInfo_C <- BR.retrieveOrMakeFrame "house-data/houseRaceInfo.bin" houseRaceDeps $ \(pollsF, abbrF, electionsF, fecF) -> do
    -- add state abbreviations to pollData
    pollsWithAbbr <- K.knitEither
                     $ either (Left . T.pack . show) Right
                     $ FJ.leftJoinE @'[BR.StateName] pollsF abbrF
    -- make each poll a row
    let flattenPollsF = FMR.concatFold
                        $ FMR.mapReduceFold
                        (FMR.unpackFilterOnField @ET.Party (`elem` [ET.Democratic, ET.Republican]))
                        (FMR.assignKeysAndData @'[BR.StateAbbreviation, BR.Pollster, BR.FteGrade, BR.CongressionalDistrict, BR.EndDate] @'[ET.Party, BR.Pct])
                        (FMR.foldAndAddKey toVoteShareF)
    let flattenedPollsF = FL.fold flattenPollsF pollsWithAbbr
    let grades = ["A+", "A", "A-", "A/B", "B+", "B", "B-", "B/C", "C+", "C", "C-", "C/D", "D+", "D", "D-", "D/F", "F"]
        acceptableGrade = ["A+", "A", "A-", "A/B", "B+", "B"]
        comparePolls :: F.Record '[BR.FteGrade, BR.EndDate] -> F.Record '[BR.FteGrade, BR.EndDate] -> Ord.Ordering
        comparePolls p1 p2 =
          let p1Grade = F.rgetField @BR.FteGrade p1
              p2Grade = F.rgetField @BR.FteGrade p2
              p1End = F.rgetField @BR.EndDate p1
              p2End = F.rgetField @BR.EndDate p2
              p1Valid = p1Grade `elem` acceptableGrade
              p2Valid = p2Grade `elem` acceptableGrade
          in case (p1Valid, p2Valid) of
            (True, True) -> compare p1End p2End
            (True, False) -> GT
            (False, True) -> LT
            (False, False) -> compare p1End p2End
        choosePollF = FMR.concatFold
                      $ FMR.mapReduceFold
                      FMR.noUnpack
                      (FMR.assignKeysAndData @[BR.StateAbbreviation, BR.CongressionalDistrict] @[BR.Pollster, BR.FteGrade, BR.EndDate, BR.DemPref])
                      (FMR.foldAndAddKey (fmap fromJust $ FL.maximumBy (\p1 p2 -> comparePolls (F.rcast p1) (F.rcast p2))))
        chosenFrame = FL.fold choosePollF flattenedPollsF
    return chosenFrame
  K.ignoreCacheTime houseRaceInfo_C >>= BR.logFrame  
  return ()

toVoteShareF :: FL.Fold (F.Record [ET.Party, BR.Pct]) (F.Record '[ET.PrefType, ET.DemPref])
toVoteShareF =
  let
    party = F.rgetField @ET.Party
    pct = F.rgetField @BR.Pct
    demVotesF = FL.prefilter (\r -> party r == ET.Democratic) $ FL.premap pct FL.sum
    demRepVotesF = FL.prefilter (\r -> let p = party r in (p == ET.Democratic || p == ET.Republican)) $ FL.premap pct FL.sum
    demPref d dr = if dr > 0 then d/dr else 0
    demPrefF = demPref <$> demVotesF <*> demRepVotesF
  in fmap (\x -> FT.recordSingleton ET.VoteShare `V.rappend` FT.recordSingleton @BR.DemPref x) demPrefF


type HousePollRow' = '[BR.State, BR.Pollster, BR.FteGrade, BR.CongressionalDistrict, BR.StartDate, BR.EndDate, BR.Internal, BR.CandidateName, BR.CandidateParty, BR.Pct]
type HousePollRow = '[BR.StateName, BR.Pollster, BR.FteGrade, BR.CongressionalDistrict, BR.StartDate, BR.EndDate, BR.Internal, BR.CandidateName, ET.Party, BR.Pct]

housePolls2020Loader :: (K.KnitOne r,  K.CacheEffectsD r) => K.Sem r (K.ActionWithCacheTime r (F.FrameRec HousePollRow))
housePolls2020Loader =  BR.cachedMaybeFrameLoader @(F.RecordColumns BR.HousePolls2020) @HousePollRow' @HousePollRow
                        (BR.DataSets $ T.pack BR.housePolls2020CSV)
                        Nothing
                        Nothing
                        id
                        fixHousePollRow
                        Nothing
                        "housePolls2020.bin"


parseHousePollParty :: T.Text -> ET.PartyT
parseHousePollParty t
  | T.isInfixOf "DEM" t = ET.Democratic
  | T.isInfixOf "REP" t = ET.Republican
  | otherwise = ET.Other

fixHousePollRow :: F.Record HousePollRow' -> F.Record HousePollRow
fixHousePollRow = F.rcast . addCols where
  addCols = FT.addName @BR.State @BR.StateName
            . FT.addOneFromOne @BR.CandidateParty @ET.Party parseHousePollParty


type FECRaw = [BR.CandidateId, BR.CandidateName, BR.StateAbbreviation, BR.CongressionalDistrict, BR.Party, BR.CashOnHand, BR.Disbursements, BR.Receipts, BR.IndSupport, BR.IndOppose, BR.PartyExpenditures]
type FECRow =  [BR.CandidateName, BR.StateAbbreviation, BR.CongressionalDistrict, ET.Party, BR.CashOnHand, BR.Disbursements, BR.Receipts, BR.IndSupport, BR.IndOppose, BR.PartyExpenditures]


fec2020Loader ::  (K.KnitOne r,  K.CacheEffectsD r) => K.Sem r (K.ActionWithCacheTime r (F.FrameRec FECRow))
fec2020Loader = BR.cachedFrameLoader (BR.DataSets $ T.pack BR.allMoney2020CSV) Nothing Nothing fixFECRow Nothing "allMoney2020.bin"

parseFECParty :: T.Text -> ET.PartyT
parseFECParty t
  | T.isInfixOf "Democrat" t = ET.Democratic
  | T.isInfixOf "Republican" t = ET.Republican
  | otherwise = ET.Other


fixFECRow :: F.Record FECRaw -> F.Record FECRow
fixFECRow = F.rcast . addCols where
  addCols = FT.addOneFromOne @BR.Party @ET.Party parseFECParty

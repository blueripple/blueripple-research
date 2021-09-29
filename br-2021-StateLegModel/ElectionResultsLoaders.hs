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

module ElectionResultsLoaders where

import qualified BlueRipple.Data.DataFrames as BR
import qualified BlueRipple.Data.ElectionTypes as ET
import qualified BlueRipple.Utilities.KnitUtils as BR
import qualified Text.Megaparsec as MP
import qualified Text.Megaparsec.Char as MP
import qualified Text.Megaparsec.Char.Lexer as MPL
import qualified Control.Foldl as FL
import qualified Data.Aeson as A
import qualified Data.Aeson.Lens as A
import qualified Data.Csv as CSV hiding (decode)
import qualified Data.Csv.Streaming as CSV
import Data.Csv ((.!))
import qualified Data.Text as T
import qualified Text.Read  as T
import qualified Data.Vinyl as V
import qualified Frames as F
import qualified Frames.MapReduce as FMR

import Control.Lens.Operators

import qualified Knit.Report as K


dlccDistricts :: Map Text [(ET.DistrictType, Int)]
dlccDistricts =
  fromList
  [
    ("VA", (ET.StateLower,) <$> [2,10,12,13,21,27,28,31,40,42,50,51,63,66,68,72,73,75,81,83,84,85,91,93,94,100])
  ]

type VotePct = "VotePct" F.:-> Text
type SLDResultR = [BR.Year, BR.StateAbbreviation, ET.DistrictTypeC, ET.DistrictNumber, BR.Candidate, ET.Party, ET.Votes, VotePct]

type Parser = MP.Parsec Void Text

parseParty :: Text -> ET.PartyT
parseParty "Democrat" = ET.Democratic
parseParty "Democratic" = ET.Democratic
parseParty "Republican" = ET.Republican
parseParty "DEM" = ET.Democratic
parseParty "REP" = ET.Republican
parseParty "Dem" = ET.Democratic
parseParty "Rep" = ET.Republican
parseParty "D" = ET.Democratic
parseParty "R" = ET.Republican
parseParty _ = ET.Other


newtype NVResult = NVResult (F.Record SLDResultR)
instance CSV.FromRecord NVResult where
  parseRecord v = if length v == 9 then NVResult <$> parseNVResult v else fail ("(NVResult.parseRecord) != 9 cols in " <> show v)

parseNVDistrictType :: Text -> CSV.Parser ET.DistrictType
parseNVDistrictType = mpToCsv parse
  where
    parse = MP.choice [ET.StateLower <$ MP.string "StateLower"
                      , ET.StateUpper <$  MP.string "StateUpper"]

parseNVResult :: CSV.Record -> CSV.Parser (F.Record SLDResultR)
parseNVResult v =
  let mkRec yr sa dt dn cn cp vts vp = yr F.&: sa F.&: dt F.&: dn F.&: cn F.&: cp F.&: vts F.&: vp F.&: V.RNil
      yr = v .! 0
      sa = v .! 1
      dt = (v .! 2) >>= parseNVDistrictType
      dn = v .! 3
      cn = v .! 4
      cp = parseParty <$> (v .! 5)
      vts = v .! 6
      vp = v .! 7
  in mkRec <$> yr <*> sa <*> dt <*> dn <*> cn <*> cp <*> vts <*> vp

getNVResults :: (K.KnitEffects r, BR.CacheEffects r) => K.Sem r (K.ActionWithCacheTime r (F.FrameRec SLDRaceResultR))
getNVResults = do
  let fp = "data/forPosts/NV_Lower_2020_general.csv"
  fileDep <- K.fileDependency fp
--  BR.clearIfPresentD "data/SLD/NV_Lower_2020_General.bin"
  BR.retrieveOrMakeFrame "data/SLD/NV_Lower_2020_General.bin" fileDep $ \_ -> do
    K.logLE K.Info "Rebuilding Nevada 2020 lower-state-house general election results."
    fileBS <- K.liftKnit $ readFileLBS @IO fp
    let records = CSV.decode CSV.HasHeader fileBS
--    K.logLE K.Diagnostic $ "Parse Errors: " <> T.intercalate "\n" (parseErrors records)
    return $ FL.fold candidatesToRaces $ fmap (\(NVResult x) -> x) $ records


newtype OHResult = OHResult (F.Record SLDResultR)
instance CSV.FromRecord OHResult where
  parseRecord v = if length v == 6 then OHResult <$> parseOHResult v else fail ("(OHResult.parseRecord) != 6 cols in " <> show v)

parseOHRace :: Text -> CSV.Parser (ET.DistrictType, Int)
parseOHRace t = mpToCsv parseRace t where
  parseOffice = MP.choice [ET.StateUpper <$ MP.string "Senator", ET.StateLower <$ MP.string "Representative"]
  parseRace = do
    MP.string "State"
    MP.space
    o <- parseOffice
    MP.space
    void $ MP.char '-'
    MP.space
    MP.string "District"
    MP.space
    d <- MPL.decimal
    return (o, d)

parseOHCandAndParty :: Text -> CSV.Parser (Text, ET.PartyT)
parseOHCandAndParty t = mpToCsv parseCP t
  where
    parseP :: Parser ET.PartyT = parseParty . toText <$> (MP.between (MP.char '(') (MP.char ')') (MP.some MP.letterChar))
    parseCP :: Parser (Text, ET.PartyT) = first toText <$> MP.someTill_ (MP.asciiChar <|> MP.spaceChar) parseP

parseOHResult :: CSV.Record -> CSV.Parser (F.Record SLDResultR)
parseOHResult v =
  let mkRec yr sa dt dn c cp vts vpct =  yr F.&: sa F.&: dt F.&: dn F.&: c F.&: cp F.&: vts F.&: vpct F.&: V.RNil
      yr = v .! 0
      sa = v .! 1
      dist = (v .! 2) >>= parseOHRace
      dt = fst <$> dist
      dn = snd <$> dist
      candp = (v .! 3) >>= parseOHCandAndParty
      cn = fst <$> candp
      cp = snd <$> candp
      vts = v .! 4
      vpct = v .! 5
  in mkRec <$> yr <*> sa <*> dt <*> dn <*> cn <*> cp <*> vts <*> vpct

getOHResults :: (K.KnitEffects r, BR.CacheEffects r) => K.Sem r (K.ActionWithCacheTime r (F.FrameRec SLDRaceResultR))
getOHResults = do
  let fp = "data/forPosts/OH_2020_general.csv"
  fileDep <- K.fileDependency fp
--  BR.clearIfPresentD "data/SLD/OH_2020_General.bin"
  BR.retrieveOrMakeFrame "data/SLD/OH_2020_General.bin" fileDep $ \_ -> do
    K.logLE K.Info "Rebuilding Ohio 2020 state-house general election results."
    fileBS <- K.liftKnit $ readFileLBS @IO fp
    let records = CSV.decode CSV.HasHeader fileBS
--    K.logLE K.Diagnostic $ "Parse Errors: " <> T.intercalate "\n" (parseErrors records)
    return $ FL.fold candidatesToRaces $ fmap (\(OHResult x) -> x) $ records


newtype TXResult = TXResult (F.Record SLDResultR)

mpToCsv :: Parser a -> Text -> CSV.Parser a
mpToCsv p t = toCSVParser $ first (MP.errorBundlePretty @_ @Void) $ MP.runParser p "mpToCsv" t
  where
    toCSVParser e = case e of
      Left msg -> fail $ toString ("MegaParsec: " <> toText msg)
      Right x -> return x

parseTXRace :: Text -> CSV.Parser (ET.DistrictType, Int)
parseTXRace t = mpToCsv parseRace t
  where
    parseOffice = MP.choice [ET.StateLower <$ MP.string "STATE REPRESENTATIVE DISTRICT"
                            , ET.StateUpper <$  MP.string "STATE SENATOR, DISTRICT"]
    parseRace = do
      o <- parseOffice
      MP.space
      d <- MPL.decimal
      return (o, d)

parseTXResult :: CSV.Record -> CSV.Parser (F.Record SLDResultR)
parseTXResult v =
  let mkRec yr sa dt dn c p vts vpct = yr F.&: sa F.&: dt F.&: dn F.&: c F.&: p F.&: vts F.&: vpct F.&: V.RNil
      yr = v .! 0
      sa = v .! 1
      dist = (v .! 2) >>= parseTXRace
      dt = fst <$> dist
      dn = snd <$> dist
      cand = v .! 3
      p = parseParty <$> (v .! 4)
      vts = v .! 5
      vpct = v .! 6
  in mkRec <$> yr <*> sa <*> dt <*> dn <*> cand <*> p <*> vts <*> vpct

instance CSV.FromRecord TXResult where
  parseRecord v = if length v == 9 then TXResult <$> parseTXResult v else fail ("(TXResult.parseRecord) != 9 cols in " <> show v)

parseErrors :: CSV.Records a -> [Text]
parseErrors rs = reverse $ go rs [] where
  go (CSV.Nil ms _) es = maybe es (\s -> toText s : es) ms
  go (CSV.Cons e rs') es = go rs' es' where
    es' = case e of
      Left e -> toText e : es
      Right _ -> es

getTXResults :: (K.KnitEffects r, BR.CacheEffects r) => K.Sem r (K.ActionWithCacheTime r (F.FrameRec SLDRaceResultR))
getTXResults = do
  let fp = "data/forPosts/TX_2020_general.csv"
  fileDep <- K.fileDependency fp
--  BR.clearIfPresentD "data/SLD/TX_2020_General.bin"
  BR.retrieveOrMakeFrame "data/SLD/TX_2020_General.bin" fileDep $ \_ -> do
    K.logLE K.Info "Rebuilding Texas 2020 state-house general election results."
    fileBS <- K.liftKnit $ readFileLBS @IO fp
    let records = CSV.decode CSV.HasHeader fileBS
--    K.logLE K.Diagnostic $ "Parse Errors: " <> T.intercalate "\n" (parseErrors records)
    return $ FL.fold candidatesToRaces $ fmap (\(TXResult x) -> x) $ records

newtype GAResult = GAResult (F.Record SLDResultR)
instance CSV.FromRecord GAResult where
  parseRecord v = if length v == 10 then GAResult <$> parseGAResult 2020 v else fail ("(GAResult.parseRecord) != 10 cols in " <> show v)

parseGARace :: Text -> CSV.Parser (ET.DistrictType, Int)
parseGARace t = mpToCsv parseRace t
  where
    parseOffice = MP.choice [ET.StateLower <$ MP.string "State House District"
                            ,ET.StateLower <$ MP.string "State House Dist"
                            ,ET.StateUpper <$ MP.string "State Senate District"
                            ,ET.StateUpper <$ MP.string "State Senate Dist"
                            ]
    parseRace = do
      o <- parseOffice
      MP.space
      d <- MPL.decimal
      return (o, d)

parseGACandAndParty :: Text -> CSV.Parser (Text, ET.PartyT)
parseGACandAndParty t = mpToCsv parseCP t
  where
    atEnd p = p >>= \res -> do { MP.eof; return res}
    parseP :: Parser ET.PartyT = parseParty . toText <$> (MP.between (MP.char '(') (MP.char ')') (MP.some MP.letterChar))
    parseCP :: Parser (Text, ET.PartyT) = first toText <$> MP.someTill_ (MP.asciiChar <|> MP.spaceChar) (MP.try $ atEnd parseP)


parseGAResult :: Int -> CSV.Record -> CSV.Parser (F.Record SLDResultR)
parseGAResult yr v =
  let mkRec dt dn c p vts vpct = yr F.&: "GA" F.&: dt F.&: dn F.&: c F.&: p F.&: vts F.&: vpct F.&: V.RNil
      dist = (v .! 1) >>= parseGARace
      dt = fst <$> dist
      dn = snd <$> dist
      cAndP = (v .! 2) >>= parseGACandAndParty
      cand = fst <$> cAndP
      p = snd <$> cAndP
      vts = v .! 4
      vpct = v .! 5
  in mkRec <$> dt <*> dn <*> cand <*> p <*> vts <*> vpct

getGAResults :: (K.KnitEffects r, BR.CacheEffects r) => K.Sem r (K.ActionWithCacheTime r (F.FrameRec SLDRaceResultR))
getGAResults = do
  let fp = "data/forPosts/GA_2020_general.csv"
  fileDep <- K.fileDependency fp
--  BR.clearIfPresentD "data/SLD/GA_2020_General.bin"
  BR.retrieveOrMakeFrame "data/SLD/GA_2020_General.bin" fileDep $ \_ -> do
    K.logLE K.Info "Rebuilding Georgia 2020 state-house general election results."
    fileBS <- K.liftKnit $ readFileLBS @IO fp
    let records = CSV.decode CSV.HasHeader fileBS
    K.logLE K.Diagnostic $ "Parse Errors: " <> T.intercalate "\n" (parseErrors records)
    return $ FL.fold candidatesToRaces $ fmap (\(GAResult x) -> x) $ records


parseVARace :: Text -> Maybe (F.Record [BR.Year, BR.StateAbbreviation, ET.DistrictTypeC, ET.DistrictNumber])
parseVARace raceDesc = do
  let isLower = T.isInfixOf "Delegates" raceDesc
      dType = if isLower then ET.StateLower else ET.StateUpper
  let numText = T.dropWhileEnd (\x -> x /= ')') $  T.dropWhile (\x -> x /= '(') raceDesc
  dNum :: Int <- T.readMaybe $ toString numText
  return $ 2019 F.&: "VA" F.&: dType F.&: dNum F.&: V.RNil


getVAResults :: (K.KnitEffects r, BR.CacheEffects r) => K.Sem r (K.ActionWithCacheTime r (F.FrameRec SLDRaceResultR))
getVAResults = do
  let fp = "data/forPosts/VA_2019_General.json"
  fileDep <-  K.fileDependency fp
  BR.clearIfPresentD "data/SLD/VA_2019_General.bin"
  BR.retrieveOrMakeFrame "data/SLD/VA_2019_General.bin" fileDep $ \_ -> do
    mJSON :: Maybe A.Value <- K.liftKnit $ A.decodeFileStrict' fp
    races <- case mJSON of
      Nothing -> K.knitError "Error loading VA general json"
      Just json -> K.knitMaybe "Error Decoding VA General json" $ do
        racesA <- json ^? A.key "Races"
        let listOfRaces = racesA ^.. A.values
            g :: A.Value -> Maybe (F.Record [BR.Candidate, ET.Party, ET.Votes, VotePct])
            g cO = do
              name <- cO ^?  A.key "BallotName" . A._String
              party <- fmap parseParty (cO ^? A.key "PoliticalParty" . A._String)
              votes <- cO ^? A.key "Votes" . A._Integer
              pct <- cO ^? A.key "Percentage" . A._String
              return $ name F.&: party F.&: fromInteger votes F.&: pct F.&: V.RNil
            f raceO = do
              raceRec <- raceO ^? A.key "RaceName" . A._String >>= parseVARace
              candidatesA <- raceO ^? A.key "Candidates"
              let candidates = catMaybes $ g <$> (candidatesA ^.. A.values)
              return $ fmap (raceRec F.<+>) candidates
        races <- traverse f listOfRaces
        return $ FL.fold candidatesToRaces $ concat races
    return races

type DVotes = "DVotes" F.:-> Int
type RVotes = "RVotes" F.:-> Int
type DShare = "DShare" F.:-> Double
type Contested = "Contested" F.:-> Bool

type SLDRaceResultR = [BR.Year, BR.StateAbbreviation, ET.DistrictTypeC, ET.DistrictNumber, Contested, DVotes, RVotes, DShare]

candidatesToRaces :: FL.Fold (F.Record SLDResultR) (F.FrameRec SLDRaceResultR)
candidatesToRaces = FMR.concatFold
                    $ FMR.mapReduceFold
                    FMR.noUnpack
                    (FMR.assignKeysAndData @[BR.Year, BR.StateAbbreviation, ET.DistrictTypeC, ET.DistrictNumber])
                    (FMR.foldAndAddKey f) where
  f :: FL.Fold (F.Record [BR.Candidate, ET.Party, ET.Votes, VotePct]) (F.Record [Contested, DVotes, RVotes, DShare])
  f =
    let party = F.rgetField @ET.Party
        votes = F.rgetField @ET.Votes
        dVotes = FL.prefilter ((== ET.Democratic) . party) $ FL.premap votes FL.sum
        rVotes = FL.prefilter ((== ET.Republican) . party) $ FL.premap votes FL.sum
        ndVotes = FL.prefilter ((/= ET.Democratic) . party) $ FL.premap votes FL.sum
        contested x y = x /= 0 && y /= 0
    in (\d r nd -> contested d r F.&: d F.&: r F.&: realToFrac d/ realToFrac (d + nd) F.&: V.RNil) <$> dVotes <*> rVotes <*> ndVotes

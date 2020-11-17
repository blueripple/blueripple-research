{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
module Parsers
  (
    module Parsers
  , module Text.Megaparsec
  )
where

import qualified Text.Megaparsec as P
import Text.Megaparsec (parseTest)
import qualified Text.Megaparsec.Char as P
import qualified Text.Megaparsec.Char.Lexer as L
import qualified Control.Monad as Monad
import qualified Control.Monad.Fail as Fail

import Data.List as L
import Data.Set as Set
import qualified Data.Text as T
import Data.Void (Void)
--import qualified Data.Csv as CSV

import qualified Frames as F

import qualified GA_DataFrames as GA
import qualified BlueRipple.Data.DataFrames as BR


type ElexCols = [BR.StateAbbreviation, BR.StateFIPS, BR.CountyFIPS, BR.CountyName, GA.Party, GA.Candidate, GA.Method, GA.Votes]
type ElexRow = F.Record ElexCols

type Text = T.Text -- strict for now but could change

type Parsec a = P.Parsec Void Text a

{-
County, Precinct, Registered Voters, Candidate (Party)_Method, ...
c, p, v, n, ...
-}
-- NB: this one eats the trailing ','
parseWideHeader :: Parsec [Text]
parseWideHeader = do
  let parseParty = P.try $ do
        Monad.void $ P.char '('
        P.manyTill P.letterChar (P.string ")_")
  (cand, party) <- P.manyTill_ P.printChar parseParty
  method <- P.manyTill P.printChar (P.char ',')
  return $ fmap T.pack [cand, party, method]                

wideReturns :: [Text] -> [Text] -> Parsec [Text] -> Parsec [[Text]]
wideReturns fixedColHeaders meltColHeaders parseWideHeader = do
  parsedHeaders <- parseWideHeaders (L.length fixedColHeaders) parseWideHeader
  P.eol
  dataRows <- uniformSVLines (L.length parsedHeaders + L.length fixedColHeaders + 1) ','
  Monad.when (L.null dataRows) $ Fail.fail $ "csv parsing returned nothing"
  let newHeader = fixedColHeaders <> meltColHeaders <> ["Votes"]
      anchorLength = length fixedColHeaders
  let processRow r = newRows where
        anchor = L.take anchorLength r
        dat = L.drop anchorLength r
        newRows = fmap (\(phs, n) -> anchor <> phs <> pure n) $ zip parsedHeaders dat
  return $ newHeader : mconcat (fmap processRow dataRows)


parseWideHeaders :: Int -> Parsec [Text] -> Parsec [[Text]]
parseWideHeaders skipCells parseWideHeader = do
  let pEnd = P.string "Total Votes"
  _ <- P.count skipCells (cell ',' >> P.char ',')
  P.manyTill parseWideHeader pEnd

uniformSVLines :: Int -> Char -> Parsec [[Text]]
uniformSVLines n s = do
  sv <- svLines s
  let isUniform = checkSameLengths n sv
  Monad.when (not isUniform) $ Fail.fail $ ("sv text does not have " ++ show n ++ " columns: " ++ show sv)
  return sv

svLines :: Char -> Parsec [[Text]]
svLines s = P.endBy (svLine s) P.eol

svLine :: Char -> Parsec [Text]
svLine s = P.sepBy (cell s) (P.char s)

cell :: Char -> Parsec Text
cell s = fmap T.pack $ P.many (P.noneOf @[] (s : "\n"))

checkSameLengths :: Int -> [[a]] -> Bool
checkSameLengths n = (==0) . L.length . L.filter (/= n) . fmap L.length 

{-
-- skip just spaces.  No comment lines
sc :: Parsec ()
sc = L.space P.space1 P.empty P.empty

parseElexByCounty :: Text -> Parsec (F.Frame ElexRow)
parseElexByCounty = undefined

parseSepRow :: Char -> Text -> Parsec [Text]
parseSepRow s = P.manyTill (untilSep s) P.eof

untilSep :: Char -> Parser Text
untilSep = fmap T.pack . P.manyTill L.charLiteral (P.char ',')
-}


  

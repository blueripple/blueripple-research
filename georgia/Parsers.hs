{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
module Parsers where

import qualified Text.Megaparsec as P
import qualified Text.Megaparsec.Char as P
import qualified Text.Megaparsec.Char.Lexer as L

import qualified Data.Text as T
import Data.Void (Void)

import qualified Frames as F

import qualified GA_DataFrames as GA
import qualified BlueRipple.Data.DataFrames as BR

type ElexCols = [BR.StateAbbreviation, BR.StateFIPS, BR.CountyFIPS, BR.CountyName, GA.Party, GA.Candidate, GA.Method, GA.Votes]
type ElexRow = F.Record ElexCols

type Text = T.Text -- strict for now but could change

type Parsec a = P.Parsec Void Text a

svFile :: Char -> Parsec [[Text]]
svFile s = P.endBy (svLine s) P.eol

svLine :: Char -> Parsec [Text]
svLine s = P.sepBy cell (P.char s)

cell :: Parsec Text
cell = fmap T.pack $ P.many (P.noneOf @[] ",\n")

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


  

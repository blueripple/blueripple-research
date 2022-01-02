{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE OverloadedStrings #-}
module Stan.SamplerCSV where

import Prelude hiding (many, some)
import Text.Megaparsec
import Text.Megaparsec.Char
import qualified Text.Megaparsec.Char.Lexer as L

import qualified Data.Massiv.Vector as MV
import qualified Data.Massiv.Array as M

import qualified Data.Text as T

--import Control.Applicative.Combinators

-- parse stan csv formats.
-- For each keep comment sections as a whole
-- Parse csv into vectors (headers and samples)

-- transform vector of headers and vectors of results into named
-- and ordered columns so we can replace them with columns from
-- generate_quantities runs.

-- then re-write sampler style file

type Parser  = Parsec Void Text

commentLine :: Parser Text
commentLine = (\c t e -> toText ([c] ++ t) <> e) <$> char '#' <*> many alphaNumChar <*> eol

commentSection :: Parser Text
commentSection = fmap mconcat $ some commentLine

untilComma :: Parser Text
untilComma = takeWhileP (Just "non-comma") (/= ',')

--commaFirst :: Parser a -> Parser a
--commaFirst p = flip const <$> char ',' <*> p

csvLine :: Parser a -> Parser [a]
csvLine p = sepEndBy1 p (void (char ',') <|> void eol)

headerLine :: Parser [Text]
headerLine = csvLine untilComma

dataLine :: Parser [Double]
dataLine = csvLine L.float

type Samples = M.Array M.U M.Ix2 Double

samples :: Parser Samples
samples = M.fromLists' M.Seq <$> many dataLine

data SamplerCSV = SamplerCSV
  { samplerDetails :: Text
  , adaptDetails :: Text
  , timeDetails :: Text
  , samplerHeader :: [Text]
  , samplerSamples :: Samples
  }

samplerCSV :: Parser SamplerCSV
samplerCSV = do
  sDetails <- commentSection
  header <- headerLine
  aDetails <- commentSection
  sSamples <- samples
  tDetails <- commentSection
  eol
  return $ SamplerCSV sDetails aDetails tDetails header sSamples

data GQCSV = GQCSV
  {
    gqDetails :: Text
  , gqHeader :: [Text]
  , gqSamples :: Samples
  }

gqCSV :: Parser GQCSV
gqCSV = do
  details <- commentSection
  header <- headerLine
  gqSamples <- samples
  return $ GQCSV details header gqSamples

headerText :: [Text] -> Text
headerText = T.intercalate ","

samplesText :: Samples -> [Text]
samplesText = undefined
